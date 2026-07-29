"""재처리(Reprocessing) DAG — 구현 스켈레톤.

설계 문서: pipeline/reprocessing-dag-design.md (Airflow 3.2.2)

append DAG 조회 범위(최근 1일)에서 밀려난 WAIT/FAILED를 전날+그저께 범위에서
회수하고, 적재분에 대해 기존 Compaction DAG을 trigger한다.
테이블별로 기존 ConvertFileTaskGroup을 재사용하며 조회 범위(reprocess_cfg)만 넘긴다.

기존 구현과 연결할 지점은 `TODO(연결)` 주석으로 표시했다.
"""

import json
from enum import Enum
from operator import itemgetter
from pathlib import Path

import pendulum
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, chain, dag, task

# TODO(연결): 기존 append DAG 공통 모듈에서 import
# from <공통 모듈>.taskgroups import ConvertFileTaskGroup
# from <공통 모듈>.iceberg import HourlyIcebergTable, DailyIcebergTable

KST = pendulum.timezone("Asia/Seoul")
DAG_ID = Path(__file__).stem  # 조직 컨벤션: dag_id는 파일명에서 파생 (단일 소스)

# Oracle DB 2개(a/b)에 동일 스키마의 Job History가 있어 같은 쿼리를 DB별로 반복한다.
# TODO(연결): append DAG이 쓰는 conn_list와 동일 소스 사용
ORACLE_CONN_IDS = ["oracle_a", "oracle_b"]

ROW_LIMIT = 1000     # 테이블당·DB당 조회 상한 — 한 회차 물량은 이것만으로 통제한다
                     # (설계 5.4 — 러프 설정, 운영 데이터로 재검증 필요)
MAX_LOOP = 10        # 자기 재trigger 상한 (설계 5.5)
ZOMBIE_HOURS = 2     # 좀비 IN_PROGRESS 판정 임계 (설계 8.2)

DAILY_COMPACTION_DAG_ID = "daily_compaction_dag"    # TODO(연결): 실제 DAG id
HOURLY_COMPACTION_DAG_ID = "hourly_compaction_dag"  # TODO(연결): 실제 DAG id


# --- 테이블 Enum (자리표시자 — 실제 iceberg.py 정의로 교체) -----------------

class HourlyIcebergTable(str, Enum):
    """첫 파티션이 hour인 테이블 그룹."""

    # TABLE_A = "table_a"

    def get_name(self) -> str:
        return self.value  # value는 alias — 실제 Enum의 get_name()으로 대체


class DailyIcebergTable(str, Enum):
    """첫 파티션이 day인 테이블 그룹."""

    # TABLE_B = "table_b"

    def get_name(self) -> str:
        return self.value


ALL_TABLES = [*HourlyIcebergTable, *DailyIcebergTable]


# --- 시간 유틸 --------------------------------------------------------------

def ts_str(dt: pendulum.DateTime) -> str:
    """pendulum datetime → Job History ts 형식 'YYYYMMDDHHmmSSsss'."""
    return dt.format("YYYYMMDDHHmmss") + "000"


def param_to_ts(value: str) -> str:
    """date-time param → ts 문자열.

    `tz=KST`는 offset 없는 문자열에만 적용되므로 `in_timezone`이 반드시 필요하다.
    빼면 '2026-07-01T15:00:00Z'가 KST 00:00이 아닌 15:00으로 읽혀 경계가 어긋난다.
    """
    return ts_str(pendulum.parse(value, tz=KST).in_timezone(KST))


def dates_between(ts_min: str, ts_max: str) -> list[str]:
    """ts_min~ts_max 구간이 걸치는 모든 날짜(YYYYMMDD) 목록.

    daily Compaction은 날짜 단위로 trigger하므로 양 끝 날짜만 쓰면 3일 이상
    범위에서 중간 날짜가 누락된다.
    """
    day = pendulum.from_format(ts_min[:8], "YYYYMMDD", tz=KST)
    last = pendulum.from_format(ts_max[:8], "YYYYMMDD", tz=KST)
    days = []
    while day <= last:
        days.append(day.format("YYYYMMDD"))
        day = day.add(days=1)
    return days


# --- Oracle -----------------------------------------------------------------
#
# 상태 UPDATE(_update_jobs)는 부모 ConvertFileTaskGroup에 이미 있다. 이 모듈은
# 재처리 고유 조회만 담당하고, UPDATE는 부모 함수를 호출한다 (예시 파일 참조).

# 복합키 4개 (ts도 그중 하나). XCom에 올릴 때는 이 순서의 값 tuple로 만든다.
# TODO(연결): 실제 컬럼명으로 교체 — 아래 SQL 2개의 컬럼명도 함께 고칠 것
PK_COLUMNS = ("k_1", "k_2", "k_3", "ts")

#
# stat_desc(CLOB)를 그냥 조회하면 LOB 객체로 와 문자열 비교·set 연산이 안 되므로
# VARCHAR2로 변환해 받는다 (batch_id는 짧아 4000바이트로 충분).
# 컬럼명이 곧 row의 키가 되므로 변환 컬럼에는 `AS stat_desc` 별칭이 반드시 필요하다.
SELECT_TARGETS_SQL = """
SELECT * FROM (
    SELECT k_1, k_2, k_3, ts, base_path, param, status,
           DBMS_LOB.SUBSTR(stat_desc, 4000, 1) AS stat_desc
      FROM JOB_HISTORY
     WHERE table_name = :tbl
       AND ts >= :ts_from AND ts < :ts_to
       AND ( status = 'FAILED'
             OR (status = 'WAIT' AND ts < :wait_bound) )
     ORDER BY ts ASC
) WHERE ROWNUM <= :row_limit
"""

# TODO(연결): ① 갱신 시각 컬럼명 확인(updated_at 가정) ② ts 범위 조건이 없어
#             파티션 전체 스캔 — status 인덱스 유무 확인 필요
ZOMBIE_SQL = """
SELECT table_name, k_1, k_2, k_3, ts, updated_at
  FROM JOB_HISTORY
 WHERE status = 'IN_PROGRESS'
   AND updated_at < SYSTIMESTAMP - NUMTODSINTERVAL(:h, 'HOUR')
"""


def select_rows(conn_id: str, sql: str, binds: dict) -> list[dict]:
    """조회 결과를 컬럼명 dict로 매핑.

    컬럼명은 커서에서 그대로 받는다 — SELECT와 컬럼 목록을 따로 맞출 필요가 없다
    (Oracle이 대문자로 주므로 소문자로 통일).
    """
    with OracleHook(oracle_conn_id=conn_id).get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, binds)
        columns = [d[0].lower() for d in cur.description]
        return [dict(zip(columns, row)) for row in cur]


def pk_of(row: dict) -> tuple:
    """row → 복합키 값 tuple. 부모 `_update_jobs` 인자이자 XCom에 올릴 형태다."""
    return tuple(row[c] for c in PK_COLUMNS)


def param_files(param: str) -> list[dict]:
    """param(VARCHAR2 JSON) → 파일 목록. row 1건에 파일이 여러 개일 수 있다.

    형태: {"files": [{"file_path": ..., "size": ...}, ...]}
    목록을 그대로 부모의 파일 목록 함수에 넘기므로 여기서 가공하지 않는다.
    TODO(연결): 키 이름을 부모 파싱 함수가 기대하는 형태와 대조할 것.
    """
    return json.loads(param)["files"]


# --- 기존 구현 연결 스텁 ------------------------------------------------------

def committed_batch_ids(table_name: str, batch_ids: set[str]) -> set[str]:
    """영수증 확인 (설계 4.2): batch_ids 중 테이블 snapshot에 실제로 있는 것만 반환.

    batch당 1회씩 조회하지 않고 IN 조건으로 한 번에 대조한다. 반환도 넘긴 값의
    부분집합이어야 한다 — snapshot 전체를 긁어오면 안 된다.
    TODO(연결): 기존 Trino/Spark 조회 경로 재사용.
      SELECT element_at(summary, 'batch_id')
        FROM <catalog>.<db>.<table>.snapshots
       WHERE element_at(summary, 'batch_id') IN (:batch_ids)
    """
    raise NotImplementedError


def send_alert(message: str, detail=None) -> None:
    """TODO(연결): 기존 알림 채널 재사용."""
    raise NotImplementedError


# --- 재처리 조회 로직 (부모 __init__의 재처리 분기가 task로 감싼다) ----------

def reprocess_get_jobs(cfg: dict, *, table, run_id, ti) -> tuple[list[dict], dict]:
    """재처리 대상 조회 — append get_jobs와 조회 범위·영수증 확인만 다르다.

    반환 `(files, marks)`:
      files — 적재할 파일 목록. 부모의 파일 목록 함수에 그대로 넘긴다
              (S3 텍스트 파일 업로드 + executor 개수 산정). 빈 목록 = 처리 대상 없음
      marks — 부모 `_update_jobs`로 내보낼 상태 변경 지시
              `{"batch_id", "keys", "done_keys"}`, keys는 `{conn_id: [복합키 tuple]}`

    Oracle 상태 UPDATE와 파일 처리는 부모에 이미 있으므로 여기서 하지 않는다.
    호출 예시: pipeline/examples/convert_file_taskgroup_example.py
    """
    if not cfg:
        raise ValueError("prepare_run 결과 없음 — 선행 task 실패")

    empty = ([], {"batch_id": None, "keys": {}, "done_keys": {}})
    table_name = table.get_name()
    if table_name not in cfg["tables"]:
        return empty   # 수동 실행에서 미선택 → skip

    # 조회는 DB별로 실행하고 결과도 conn_id를 키로 보관한다 — 상태 UPDATE가
    # 이 키로 원천 DB를 찾아가므로 row 태깅·재그룹핑이 필요 없다
    # (복합키 값은 DB 간 유일 보장 없음). ROW_LIMIT은 DB당 적용.
    binds = {"tbl": table_name, "row_limit": ROW_LIMIT, "ts_from": cfg["ts_from"],
             "ts_to": cfg["ts_to"], "wait_bound": cfg["wait_bound"]}
    jobs_by_conn = {conn_id: select_rows(conn_id, SELECT_TARGETS_SQL, binds)
                    for conn_id in ORACLE_CONN_IDS}
    # 조회 상한을 꽉 채웠다면 그 DB에 더 남아 있다는 뜻이다. 아래 필터를 거치면
    # 건수가 줄어 이 신호를 알 수 없으므로 지금 기록해 둔다 (loop 판단에 사용)
    fetched_full = any(len(rows) >= ROW_LIMIT for rows in jobs_by_conn.values())

    # 영수증 확인 (설계 4): status는 커밋 여부의 증거가 아니다. Airflow가 실패로
    # 판정했든(FAILED), 커밋 후 상태 갱신이 실패했든(WAIT로 잔류) Spark 커밋은
    # 성공했을 수 있다. batch_id가 snapshot에 있으면 그 데이터는 이미 Iceberg에
    # 있으므로 재적재하면 중복이다 → status와 무관하게 DONE 정정 후 대상에서 뺀다.
    batch_ids = {r["stat_desc"] for rows in jobs_by_conn.values() for r in rows
                 if r["stat_desc"]}
    committed = committed_batch_ids(table_name, batch_ids) if batch_ids else set()
    done_keys: dict[str, list[tuple]] = {}
    if committed:
        for conn_id, rows in jobs_by_conn.items():
            done = [r for r in rows if r["stat_desc"] in committed]
            if done:
                done_keys[conn_id] = [pk_of(r) for r in done]
            jobs_by_conn[conn_id] = [r for r in rows if r not in done]

    # DB별 결과를 하나로 합쳐 오래된 것부터 처리한다 (append와 같은 순서).
    # 항목은 (ts, row, conn_id) — conn_id를 함께 드는 이유는 합치고 나면 어느 DB에서
    # 왔는지 잃어버리는데, 상태 UPDATE는 원천 DB로 나가야 하기 때문이다.
    candidates = [(row["ts"], row, conn_id)
                  for conn_id, rows in jobs_by_conn.items() for row in rows]
    candidates.sort(key=itemgetter(0))   # ts 오름차순. DB별로 정렬돼 있어도 합치면 깨진다

    files = []                            # 부모 파일 목록 함수에 그대로 넘길 목록
    keys: dict[str, list[tuple]] = {}     # {conn_id: [복합키 tuple]} — 원천 DB별
    for _, row, conn_id in candidates:
        files += param_files(row["param"])   # row 1건에 파일이 여러 개일 수 있다
        keys.setdefault(conn_id, []).append(pk_of(row))

    if not files:
        return ([], {"batch_id": None, "keys": {}, "done_keys": done_keys})

    batch_id = f"{run_id}_{table_name}"   # 배치당 1개 (Spark 커밋 1회 = 영수증 1개)

    # 재처리 DAG 자신의 마무리 task 2개만 쓰는 값 (부모와 무관).
    #   ts_min/ts_max = 이번에 적재한 데이터의 시간 범위. compaction_targets가
    #     이 범위만 Compaction하도록 기존 Compaction DAG에 넘긴다 (설계 6.3).
    #   has_more = 조회 상한에 걸려 못 가져온 대상이 DB에 남았는가.
    #     next_loop이 이 값으로 DAG을 한 번 더 trigger할지 정한다 (설계 5.5).
    ti.xcom_push(key="reprocess", value={
        "ts_min": candidates[0][0],    # candidates는 ts 오름차순, 전부 담았다
        "ts_max": candidates[-1][0],
        "has_more": fetched_full,
    })

    return files, {"batch_id": batch_id, "keys": keys, "done_keys": done_keys}


# TODO(연결): 기존 ConvertFileTaskGroup에 reprocess_cfg 인자와 조회 분기를 추가한다.
#             변경 예시: pipeline/examples/convert_file_taskgroup_example.py


def collect_metas(ti, sources: list[dict]) -> list[dict]:
    """이번 run에서 실제로 적재한 테이블들의 재처리 meta 수집 (설계 6.3 / 5.5).

    Airflow 3 worker는 메타데이터 DB 접근이 불가하므로 task 상태 조회 대신 XCom만 쓴다.
    XCom은 push한 task의 task_id로만 꺼낼 수 있는데, 조회 task는 테이블별
    TaskGroup 안에 있어 task_id가 `{group_id}.get_jobs`다. 그 값을 여기서 다시
    조립하면 group_id 규칙이 바뀔 때 조용히 어긋나므로(pull이 None을 돌려줄 뿐
    에러가 나지 않는다), DAG 조립 시점에 실제 TaskGroup에서 뽑아 sources로 받는다.
    """
    metas = []
    for s in sources:
        meta = ti.xcom_pull(task_ids=s["task_id"], key="reprocess")
        if meta:   # 대상 0건이라 skip된 테이블은 XCom이 없다
            metas.append({**meta, "table": s["table"], "group": s["group"]})
    return metas


# ---------------------------------------------------------------------------
# DAG 정의
# ---------------------------------------------------------------------------

@dag(
    dag_id=DAG_ID,
    schedule="0 1 * * *",   # 01:00 KST — 전날 데이터 안정화 버퍼 (설계 5.1)
    start_date=pendulum.datetime(2026, 7, 1, tz=KST),
    catchup=False,
    max_active_runs=1,      # loop 회차 순차 실행 보장
    params={
        # 선택지·기본값 모두 iceberg.py Enum에서 생성 (설계 5.1)
        "tables": Param(
            default=[t.get_name() for t in ALL_TABLES],
            type="array",
            items={"type": "string", "enum": [t.get_name() for t in ALL_TABLES]},
        ),
        # 수동 실행 조회 범위 (둘 다 함께 지정, prepare_run이 검증).
        # 미지정 시 정기 범위(그저께 00:00 ~ 전날 끝).
        "start_time": Param(None, type=["null", "string"], format="date-time"),
        "end_time": Param(None, type=["null", "string"], format="date-time"),
    },
    tags=["iceberg", "reprocess"],
)
def dag():  # 함수명 dag() 고정 — DAG 정체성은 파일명(dag_id)이 담당

    @task
    def check_zombie_jobs():
        """좀비 IN_PROGRESS 탐지 → 알림만 (설계 8.2).

        독립 실행이라 알림 실패가 본류를 막지 않는다. conn_id를 키로 담아
        알림에 그대로 넘긴다 (수동 정정 시 대상 DB 식별용).
        """
        zombies_by_conn = {
            conn_id: select_rows(conn_id, ZOMBIE_SQL, {"h": ZOMBIE_HOURS})
            for conn_id in ORACLE_CONN_IDS
        }
        total = sum(len(rows) for rows in zombies_by_conn.values())
        if total:
            send_alert(f"좀비 IN_PROGRESS {total}건 — 영수증 확인 후 수동 판정 필요",
                       zombies_by_conn)

    @task
    def prepare_run(params=None, dag_run=None) -> dict:
        """params 검증·정규화 1회 (append DAG의 get_time 패턴). 이후 task는 XCom만 소비."""
        conf = dag_run.conf or {}

        # loop 재trigger 회차: conf가 곧 직전 회차의 반환값이므로 그대로 승계 (설계 5.5)
        if conf.get("ts_from"):
            return {**conf, "loop_count": int(conf.get("loop_count", 0))}

        base = pendulum.now(KST).start_of("day")  # 오늘 00:00 (오늘=4일 01:00 실행이면 4일 00:00)
        st, et = params.get("start_time"), params.get("end_time")

        if st and et:
            # 수동: start/end가 조회 범위를 직접 정의 (설계 5.1).
            # end_time ≤ 전날 00:00 이어야 append 조회 범위와 겹치지 않는다
            ts_from, ts_to = param_to_ts(st), param_to_ts(et)
            if not ts_from < ts_to <= ts_str(base.subtract(days=1)):
                raise ValueError("start < end 이고 end_time ≤ 전날 00:00 이어야 한다")
            wait_bound = ts_to  # 범위 전체가 append 범위 밖 → WAIT 전 구간 허용
        elif st or et:
            raise ValueError("start_time과 end_time은 함께 지정해야 한다")
        else:
            # 정기 실행 경계 (설계 2.1). 오늘=4일 기준:
            #   ts_from    그저께(2일) 00:00 — 하룻밤 실패분 자동 회수
            #   ts_to      오늘(4일) 00:00
            #   wait_bound 전날(3일) 01:00 — 이후 WAIT는 append 조회 범위 안이라 제외
            #                                (FAILED는 append가 안 보므로 상한 없음)
            ts_from = ts_str(base.subtract(days=2))
            ts_to = ts_str(base)
            wait_bound = ts_str(base.subtract(days=1).add(hours=1))

        return {
            "tables": list(params["tables"]),
            "ts_from": ts_from,
            "ts_to": ts_to,
            "wait_bound": wait_bound,
            "loop_count": 0,
        }

    @task(trigger_rule="all_done")
    def compaction_targets(sources: list[dict], ti=None) -> list[dict]:
        """적재 결과 집계 → TriggerDagRunOperator kwargs 목록 (설계 6.3).
        적재분 전부 trigger — tables 필터로 비용 최소, 중복은 no-op.
        TODO(연결): conf 날짜/시간 형식을 기존 Compaction DAG UI params와 일치시킬 것."""
        metas = collect_metas(ti, sources)
        daily = [m for m in metas if m["group"] == "daily"]
        hourly = [m for m in metas if m["group"] == "hourly"]

        # daily: meta는 테이블 단위지만 Compaction DAG은 날짜(target_dt) 단위라
        # "테이블 → 적재 범위"를 "날짜 → 테이블 목록"으로 뒤집는다
        by_date: dict[str, list[str]] = {}
        for m in daily:
            for date in dates_between(m["ts_min"], m["ts_max"]):
                by_date.setdefault(date, []).append(m["table"])
        targets = [
            {"trigger_dag_id": DAILY_COMPACTION_DAG_ID,
             "conf": {"target_dt": date, "tables": tables}}
            for date, tables in by_date.items()
        ]

        # hourly: 전체 min~max 합집합으로 1회만 trigger.
        # 사이에 빈 시간대가 껴도 Compaction은 no-op이라 무해
        if hourly:
            targets.append({
                "trigger_dag_id": HOURLY_COMPACTION_DAG_ID,
                "conf": {
                    "start_time": min(m["ts_min"] for m in hourly),
                    "end_time": max(m["ts_max"] for m in hourly),
                    "tables": [m["table"] for m in hourly],
                },
            })
        return targets  # 빈 목록이면 mapped operator는 skip

    @task(trigger_rule="all_done")
    def next_loop(cfg: dict, sources: list[dict], ti=None) -> list[dict]:
        """재trigger 판단 (설계 5.5) → TriggerDagRunOperator kwargs 0/1건.
        상한에 걸려 못 담은 대상이 남은 테이블이 하나라도 있으면 한 번 더 돈다.
        지속 실패도 has_more + MAX_LOOP 상한으로 유한하게 종료된다."""
        # 종료 ①: prepare_run 실패(cfg 없음) 또는 남은 대상 없음
        if not cfg or not any(m["has_more"] for m in collect_metas(ti, sources)):
            return []
        # 종료 ②: 회차 상한 — 자동으로 다 못 푸는 물량 → 알림 후 수동 (설계 8.1)
        if cfg["loop_count"] >= MAX_LOOP:
            send_alert(f"재처리 loop 상한({MAX_LOOP}회) 도달 — 수동 처리 필요")
            return []
        # 첫 회차가 확정한 조회 범위·tables를 conf로 승계 (자정을 넘겨도 경계 고정)
        return [{"trigger_dag_id": DAG_ID,
                 "conf": {**cfg, "loop_count": cfg["loop_count"] + 1}}]

    # 좀비 점검은 독립 실행 — 알림 실패가 재처리 본류를 막지 않음 (설계 8.2)
    check_zombie_jobs()

    # 본류: params 정규화 → 테이블별 그룹 순차 → 집계(Compaction/loop) → trigger
    run_cfg = prepare_run()
    groups = [
        ConvertFileTaskGroup(  # noqa: F821  TODO(연결): 부모 import + 시그니처 확인
            t,
            group_id=f"reprocess_{t.get_name()}",
            reprocess_cfg=run_cfg,   # 조회 범위만 전달 — 조회 task는 부모가 생성
        )
        for t in ALL_TABLES
    ]
    # Spark job이 동시에 뜨지 않도록 테이블 그룹을 순차 연결 (설계 5.2).
    # 각 그룹 첫 task가 trigger_rule="all_done"이라 앞 테이블 실패에도 계속 진행
    chain(run_cfg, *groups)
    tail = groups[-1] if groups else run_cfg  # 빈 Enum 상태에서도 파싱 가능

    # 집계 task가 XCom을 꺼낼 대상. task_id는 실제로 만들어진 TaskGroup에서 뽑는다
    # — 문자열을 따로 조립하면 group_id 규칙이 바뀔 때 조용히 어긋난다.
    # TODO(연결): 조회 task 이름("get_jobs")은 부모가 정하므로 대조할 것
    sources = [
        {"table": t.get_name(),
         "group": "hourly" if isinstance(t, HourlyIcebergTable) else "daily",
         "task_id": f"{g.group_id}.get_jobs"}
        for t, g in zip(ALL_TABLES, groups)
    ]

    comp = compaction_targets(sources)
    nxt = next_loop(run_cfg, sources)
    tail >> [comp, nxt]
    # trigger 건수가 가변이라 dynamic task mapping.
    # TriggerDagRunOperator는 wait_for_completion 기본 False (설계 6.3)
    TriggerDagRunOperator.partial(task_id="trigger_compaction").expand_kwargs(comp)
    TriggerDagRunOperator.partial(task_id="retrigger_self").expand_kwargs(nxt)


dag()
