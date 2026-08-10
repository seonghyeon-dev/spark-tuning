"""재처리(Reprocessing) DAG — 구현 스켈레톤.

설계 문서: pipeline/reprocessing-dag-design.md (Airflow 3.2.2)

무엇을 하는가
  append DAG은 최근 1일치 WAIT만 조회하므로, 그 범위에서 밀려난 WAIT와
  아무도 다시 집지 않는 FAILED가 Job History에 영구히 남는다.
  이 DAG이 하루 1회(04:00 KST) 전날+그저께 범위를 훑어 회수하고,
  적재한 시간 범위에 대해 기존 Compaction DAG을 trigger한다.

DAG 구조
  check_zombie_jobs                     # 좀비 IN_PROGRESS 알림 (독립 실행)

  prepare_run                           # 조회 범위 확정 (params/conf → XCom)
      │
      ├─ TaskGroup: 테이블 A            # 기존 ConvertFileTaskGroup 재사용.
      ├─ TaskGroup: 테이블 B            # 조회 범위만 넘기면 나머지는 부모가 한다.
      └─ ... (테이블 수만큼 순차)         # Spark job이 동시에 뜨지 않도록 순차
      │
      ├─ compaction_targets → trigger_compaction   # 적재 범위만 Compaction
      └─ next_loop          → retrigger_self       # 남았으면 한 번 더

역할 분담
  이 파일(DAG)               조회 범위 계산, 테이블 그룹 배치, Compaction 연계,
                             loop 판단, 좀비 탐지
  ConvertFileTaskGroup(기존)  재처리 조회 task, 상태 UPDATE, 파일 목록 처리,
                             Spark 실행 — **기존 파일을 고쳐서 쓴다**

  재처리 조회 task는 부모 __init__ 안에서 만들어진다(지역 함수 _update_jobs 등을
  써야 하므로). 그래서 조회 로직도 부모 파일에 둔다 — 이 DAG 파일에 두면
  공통 모듈이 DAG을 import해야 해서 성립하지 않는다.
  부모 변경 내용 전체: pipeline/examples/convert_file_taskgroup_example.py

기존 구현과 연결할 지점은 `TODO(연결)` 주석으로 표시했다.
"""

from enum import Enum
from pathlib import Path

import pendulum
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, chain, dag, task

# TODO(연결): 기존 append DAG 공통 모듈에서 import
# from <공통 모듈>.taskgroups import ConvertFileTaskGroup
# from <공통 모듈>.iceberg import HourlyIcebergTable, DailyIcebergTable

# Job History가 Oracle DB 2개에 동일 스키마로 있어 좀비 조회도 DB별로 반복한다.
# TODO(연결): append DAG이 쓰는 conn_list와 동일 소스 사용
ORACLE_CONN_IDS = ["oracle_a", "oracle_b"]

KST = pendulum.timezone("Asia/Seoul")
DAG_ID = Path(__file__).stem  # 조직 컨벤션: dag_id는 파일명에서 파생 (단일 소스)

# 실행 시각 — schedule과 wait_bound가 함께 써야 하는 값이라 상수로 묶는다.
# wait_bound는 "append 조회 하한 = 실행시각 - 24h"에서 나오므로 둘이 어긋나면
# 아무도 안 보는 시간대가 생긴다 (설계 2.1 / 6.2)
RUN_HOUR = 4         # 04:00 KST — expire snapshots(03:00) 종료 후 (설계 6.2)

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
ALL_TABLE_NAMES = [t.get_name() for t in ALL_TABLES]


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


def ts_to_hour_param(ts: str, add_hours: int = 0) -> str:
    """ts 문자열 → hourly Compaction DAG의 date-time 형식, **시 단위 내림**.

    hourly Compaction DAG의 `start_time`/`end_time`이 `format="date-time"`이라
    ts를 그대로 넘기면 params 검증에서 걸린다.

    분·초를 살려 보내지 않는 이유: 대상 테이블이 `hour(ts)` 히든 파티셔닝이라
    Compaction 단위가 1시간 통이다. 통 중간을 가리키는 값을 주면 그 통을
    반쪽만 지정하게 된다. `ts[:10]`(YYYYMMDDHH)만 파싱해 자연스럽게 내림한다.

    `end_time`은 `add_hours=1`로 부른다 — ts_max가 속한 통까지 포함해야 하는데
    내림한 값 그대로면 그 통이 범위 밖으로 떨어진다.
    """
    return pendulum.from_format(ts[:10], "YYYYMMDDHH", tz=KST).add(hours=add_hours).isoformat()


def dates_between(ts_min: str, ts_max: str) -> list[str]:
    """ts_min~ts_max 구간이 걸치는 모든 날짜 목록 — daily의 `target_dt` 형식.

    `target_dt`가 `format="date"`라 **YYYY-MM-DD**로 넘겨야 한다. ts 형식 그대로
    주면 API 서버가 거부한다:
        Invalid input for param target_dt: '20260728' is not a 'date'

    양 끝 날짜만 쓰면 3일 이상 범위에서 중간 날짜가 누락되므로 전부 만든다.
    """
    day = pendulum.from_format(ts_min[:8], "YYYYMMDD", tz=KST)
    last = pendulum.from_format(ts_max[:8], "YYYYMMDD", tz=KST)
    days = []
    while day <= last:
        days.append(day.format("YYYY-MM-DD"))
        day = day.add(days=1)
    return days


# dt는 파티션 키(VARCHAR2 'YYYYMMDDHHmmSSsss')다. 고정 폭 zero-padded라 사전순
# 비교가 시간순과 일치하고, dt를 함수로 감싸지 않으므로 pruning이 살아 있다.
# 임계값도 SYSTIMESTAMP를 쓰므로 dt까지 DB 시계로 통일한다.
# ⚠ 하루로 좁히면 하루 넘게 IN_PROGRESS로 굳은 row는 dt가 범위 밖이라 영영 안 잡힌다.
#   잔류 알림(설계 8.1)도 WAIT_SCHEDULING/FAILURE만 보므로 사각지대가 된다.
#   재처리 조회 범위(그저께~전날)에 맞춰 -2로 넓히는 것도 방법.
# TODO(연결): 갱신 시각 컬럼명 확인(updated_at 가정)
ZOMBIE_SQL = """
SELECT table_name, k_1, k_2, k_3, ts, updated_at
  FROM JOB_HISTORY
 WHERE status = 'IN_PROGRESS'
   AND dt >= TO_CHAR(SYSDATE - 1, 'YYYYMMDDHH24MISS') || '000'
   AND updated_at < SYSTIMESTAMP - NUMTODSINTERVAL(:h, 'HOUR')
"""


def send_alert(message: str, detail=None) -> None:
    """TODO(연결): 기존 알림 채널 재사용."""
    raise NotImplementedError


def collect_metas(ti, table_tasks: list[dict]) -> list[dict]:
    """이번 run에서 실제로 적재한 테이블들의 재처리 meta 수집 (설계 6.3 / 5.5).

    Airflow 3 worker는 메타데이터 DB 접근이 불가하므로 task 상태 조회 대신 XCom만 쓴다.
    XCom은 push한 task의 task_id로만 꺼낼 수 있는데, 조회 task는 테이블별
    TaskGroup 안에 있어 task_id가 `{group_id}.get_jobs`다. 그 값을 여기서 다시
    조립하면 group_id 규칙이 바뀔 때 조용히 어긋나므로(pull이 None을 돌려줄 뿐
    에러가 나지 않는다), DAG 조립 시점에 실제 TaskGroup에서 뽑아 인자로 받는다.
    """
    metas = []
    for t in table_tasks:
        meta = ti.xcom_pull(task_ids=t["task_id"], key="reprocess")
        if meta:   # 대상 0건이라 skip된 테이블은 XCom이 없다
            metas.append({**meta, "table": t["table"], "group": t["group"]})
    return metas


# ---------------------------------------------------------------------------
# DAG 정의
# ---------------------------------------------------------------------------

@dag(
    dag_id=DAG_ID,
    schedule=f"0 {RUN_HOUR} * * *",   # 설계 5.1 / 6.2
    start_date=pendulum.datetime(2026, 7, 1, tz=KST),
    catchup=False,
    max_active_runs=1,      # loop 회차 순차 실행 보장
    params={
        # 선택지·기본값 모두 iceberg.py Enum에서 생성 (설계 5.1).
        # default는 실제 값 — 정기 실행은 UI를 안 거치므로 이 값이 전체 테이블 처리를 뜻한다.
        # examples는 값에 관여하지 않고 multi-select UI만 만든다 (`items`로는 안 나온다).
        "tables": Param(default=ALL_TABLE_NAMES, type="array", examples=ALL_TABLE_NAMES),
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
        row는 ZOMBIE_SQL의 SELECT 순서 그대로인 tuple이다 — 알림에 실어 보낼 뿐
        개별 컬럼을 꺼내 쓰지 않으므로 dict로 만들지 않는다.
        """
        zombies_by_conn = {
            conn_id: OracleHook(oracle_conn_id=conn_id).get_records(
                ZOMBIE_SQL, parameters={"h": ZOMBIE_HOURS})
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

        base = pendulum.now(KST).start_of("day")  # 오늘 00:00 (오늘=4일 04:00 실행이면 4일 00:00)
        st, et = params.get("start_time"), params.get("end_time")

        if st and et:
            # 수동: start/end가 조회 범위를 직접 정의 (설계 5.1).
            # end_time ≤ 전날 00:00 이어야 append 조회 범위와 겹치지 않는다
            ts_from, ts_to = param_to_ts(st), param_to_ts(et)
            if not ts_from < ts_to <= ts_str(base.subtract(days=1)):
                raise ValueError("start < end 이고 end_time ≤ 전날 00:00 이어야 한다")
            wait_bound = ts_to  # 범위 전체가 append 범위 밖 → WAIT_SCHEDULING 전 구간 허용
        elif st or et:
            raise ValueError("start_time과 end_time은 함께 지정해야 한다")
        else:
            # 정기 실행 경계 (설계 2.1). 오늘=4일, RUN_HOUR=4 기준:
            #   ts_from    그저께(2일) 00:00 — 하룻밤 실패분 자동 회수
            #   ts_to      오늘(4일) 00:00
            #   wait_bound 전날(3일) 04:00 — 이후 WAIT는 append 조회 범위 안이라 제외
            #                                (FAILED는 append가 안 보므로 상한 없음)
            # wait_bound가 RUN_HOUR를 따라가야 한다. 실행만 늦추고 이 값을 두면
            # 그 사이 구간을 append도 재처리도 안 봐서 회수가 하루 밀린다
            ts_from = ts_str(base.subtract(days=2))
            ts_to = ts_str(base)
            wait_bound = ts_str(base.subtract(days=1).add(hours=RUN_HOUR))

        return {
            "tables": list(params["tables"]),
            "ts_from": ts_from,
            "ts_to": ts_to,
            "wait_bound": wait_bound,
            "loop_count": 0,
            # loop 전체를 관통하는 고정 식별자 — retrigger_self의 trigger_run_id 재료.
            # 매 회차 자기 run_id를 쓰면 이름이 회차마다 중첩되어 길어진다
            "chain_id": dag_run.run_id,
        }

    @task(trigger_rule="all_done")
    def compaction_targets(table_tasks: list[dict], ti=None, run_id=None) -> list[dict]:
        """적재 결과 집계 → TriggerDagRunOperator kwargs 목록 (설계 6.3).
        적재분 전부 trigger — tables 필터로 비용 최소, 중복은 no-op.
        conf 값은 대상 DAG params 형식에 맞춰 넘긴다 (daily=date, hourly=date-time).

        `trigger_run_id`를 직접 짓는 이유: 비워 두면 Airflow가 시도마다 새 run_id를
        만들어, trigger task가 재시도될 때 DagRun이 하나 더 생긴다 (DagRun 생성이
        상태 보고보다 먼저라 '일은 됐는데 실패로 기록'되면 재시도가 일을 또 한다).
        run_id는 이 DagRun 안에서 고정이므로 재시도해도 같은 이름이 나온다."""
        metas = collect_metas(ti, table_tasks)
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
             "trigger_run_id": f"reprocess_{run_id}_d{date}",
             "conf": {"target_dt": date, "tables": tables}}
            for date, tables in by_date.items()
        ]

        # hourly: 전체 min~max 합집합으로 1회만 trigger.
        # 사이에 빈 시간대가 껴도 Compaction은 no-op이라 무해.
        # 범위는 시 단위로 정렬한다 — 대상이 hour 히든 파티셔닝이라 1시간 통 단위로
        # 처리해야 하고, end_time은 ts_max가 속한 통을 포함하도록 +1시간 한다
        if hourly:
            targets.append({
                "trigger_dag_id": HOURLY_COMPACTION_DAG_ID,
                "trigger_run_id": f"reprocess_{run_id}_h",
                "conf": {
                    "start_time": ts_to_hour_param(min(m["ts_min"] for m in hourly)),
                    "end_time": ts_to_hour_param(max(m["ts_max"] for m in hourly), add_hours=1),
                    "tables": [m["table"] for m in hourly],
                },
            })
        return targets  # 빈 목록이면 mapped operator는 skip

    @task(trigger_rule="all_done")
    def next_loop(cfg: dict, table_tasks: list[dict], ti=None) -> list[dict]:
        """재trigger 판단 (설계 5.5) → TriggerDagRunOperator kwargs 0/1건.
        상한에 걸려 못 담은 대상이 남은 테이블이 하나라도 있으면 한 번 더 돈다.
        지속 실패도 has_more + MAX_LOOP 상한으로 유한하게 종료된다."""
        # 종료 ①: prepare_run 실패(cfg 없음) 또는 남은 대상 없음
        if not cfg or not any(m["has_more"] for m in collect_metas(ti, table_tasks)):
            return []
        # 종료 ②: 회차 상한 — 자동으로 다 못 푸는 물량 → 알림 후 수동 (설계 8.1)
        if cfg["loop_count"] >= MAX_LOOP:
            send_alert(f"재처리 loop 상한({MAX_LOOP}회) 도달 — 수동 처리 필요")
            return []
        # 첫 회차가 확정한 조회 범위·tables를 conf로 승계 (자정을 넘겨도 경계 고정).
        # trigger_run_id는 chain_id + 회차로 짓는다 — 자기 run_id를 쓰면 회차마다
        # 이름이 중첩되어 길어진다 (재시도 중복 방지 이유는 compaction_targets 참조)
        nxt = cfg["loop_count"] + 1
        return [{"trigger_dag_id": DAG_ID,
                 "trigger_run_id": f"reprocess_loop{nxt}_{cfg['chain_id']}",
                 "conf": {**cfg, "loop_count": nxt}}]

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

    # 집계 task 2개는 TaskGroup 밖에 있어 조회 task의 XCom을 task_id로 꺼내야 한다.
    # 그 task_id를 여기서 문자열로 다시 조립하면 group_id 규칙이 바뀔 때 조용히
    # 어긋나므로(xcom_pull은 에러 없이 None을 준다) 실제 TaskGroup에서 뽑는다.
    # TODO(연결): 조회 task 이름("get_jobs")은 부모가 정하므로 대조할 것
    table_tasks = [
        {"table": t.get_name(),
         "group": "hourly" if isinstance(t, HourlyIcebergTable) else "daily",
         "task_id": f"{g.group_id}.get_jobs"}
        for t, g in zip(ALL_TABLES, groups, strict=True)
    ]

    comp = compaction_targets(table_tasks)
    nxt = next_loop(run_cfg, table_tasks)
    tail >> [comp, nxt]
    # trigger 건수가 가변이라 dynamic task mapping.
    # TriggerDagRunOperator는 wait_for_completion 기본 False (설계 6.3)
    # map_index_template: UI map index를 숫자 대신 읽을 수 있는 값으로.
    # daily는 날짜, hourly는 시작 시각이 인스턴스를 유일하게 구분한다.
    # ⚠ 테이블 목록을 라벨에 넣지 말 것 — rendered_map_index 컬럼이 250자라
    #   테이블이 늘면 성공 상태 보고가 DB 에러로 실패하고, task는 이미 DagRun을
    #   만든 뒤라 재시도마다 중복 trigger된다. 실행 전 렌더링 실패는 debug로
    #   삼켜지므로 징후 없이 마지막에만 터진다
    # skip_when_already_exists: 재시도가 같은 trigger_run_id로 들어오면 이미 만든
    # DagRun을 발견하고 task를 SKIPPED로 끝낸다 (기본값은 예외 → task 실패)
    TriggerDagRunOperator.partial(
        task_id="trigger_compaction",
        skip_when_already_exists=True,
        map_index_template="{{ task.conf.get('target_dt') or task.conf['start_time'] }}",
    ).expand_kwargs(comp)
    TriggerDagRunOperator.partial(
        task_id="retrigger_self",
        skip_when_already_exists=True,
        map_index_template="loop {{ task.conf['loop_count'] }}",
    ).expand_kwargs(nxt)


dag()
