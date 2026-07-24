"""재처리(Reprocessing) DAG — 구현 스켈레톤.

설계 문서: pipeline/reprocessing-dag-design.md
환경: Airflow 3.2.2

역할: append DAG 조회 기간(최근 1일)에서 밀려난 WAIT 데이터와 FAILED 데이터를
      전날+그저께 범위에서 회수하고, 적재분에 대해 기존 Compaction DAG을 trigger한다.

구조: 단일 DAG, 테이블별로 기존 append DAG의 ConvertFileTaskGroup 템플릿을 재사용
      (재처리용 조회 로직만 주입). params는 prepare_run에서 1회 검증·정규화.
      잔여분이 남으면 자기 자신을 재trigger (loop, 상한 10회).

── 기존 구현 연결 지점 (grep "TODO(연결)") ────────────────────────
  1. iceberg.py의 hourly/daily Enum import (자리표시자 2개 교체)
  2. ConvertFileTaskGroup import + 상속 방식 (ReprocessTaskGroup 주석 참조)
  3. Oracle conn id (OracleHook)
  4. 영수증 snapshot 조회 — snapshot_exists
  5. avro 경로 목록 S3 업로드 — upload_path_list_to_s3
  6. 알림 채널 — send_alert
  7. Compaction DAG id 상수 + conf 날짜/시간 형식 (기존 Compaction UI params와 일치)
────────────────────────────────────────────────────────────────────
"""

import math
from enum import Enum
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

ORACLE_CONN_ID = "oracle_default"  # TODO(연결): 실제 conn id (append DAG과 동일)

ROW_LIMIT = 1000              # 테이블당 조회 상한 (설계 5.4 — 러프 설정, 재검증 필요)
SIZE_LIMIT_MB = 16 * 1024     # 테이블당 크기 상한 16GB (설계 5.4 — 러프 설정, 재검증 필요)
MAX_EXECUTORS = 24            # 벤치마크 검증 상한 (spark-tuning-guide.md 2.2.3)
MAX_LOOP = 10                 # 자기 재trigger 상한 (설계 5.5)
ZOMBIE_HOURS = 2              # 좀비 IN_PROGRESS 판정 임계 (설계 8.2)

DAILY_COMPACTION_DAG_ID = "daily_compaction_dag"    # TODO(연결): 실제 DAG id
HOURLY_COMPACTION_DAG_ID = "hourly_compaction_dag"  # TODO(연결): 실제 DAG id


# --- 자리표시자 Enum (실제 iceberg.py 정의로 교체) -------------------------
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


def compaction_group(table) -> str:
    """Compaction 그룹: 소속 Enum 클래스가 곧 분류다."""
    return "hourly" if isinstance(table, HourlyIcebergTable) else "daily"


def ts_str(dt: pendulum.DateTime) -> str:
    """pendulum datetime → Job History ts 형식 'YYYYMMDDHHmmSSsss'."""
    return dt.format("YYYYMMDDHHmmss") + "000"


def dates_between(ts_min: str, ts_max: str) -> list[str]:
    """ts_min~ts_max가 걸친 날짜(YYYYMMDD) 목록 (수동 범위가 여러 날에 걸칠 때 대비)."""
    day = pendulum.from_format(ts_min[:8], "YYYYMMDD", tz=KST)
    last = pendulum.from_format(ts_max[:8], "YYYYMMDD", tz=KST)
    out = []
    while day <= last:
        out.append(day.format("YYYYMMDD"))
        day = day.add(days=1)
    return out


# --- Oracle (OracleHook 사용) ----------------------------------------------

def _rows(sql: str, binds: dict, columns: tuple[str, ...]) -> list[dict]:
    """OracleHook.get_records로 조회 → 컬럼명 dict 매핑. columns는 SELECT 컬럼 순서와 일치."""
    records = OracleHook(oracle_conn_id=ORACLE_CONN_ID).get_records(sql, parameters=binds)
    return [dict(zip(columns, r)) for r in records]


def _in_binds(job_ids: list[str]) -> tuple[str, dict]:
    """Oracle IN은 list를 바로 못 바인딩 → placeholder 동적 확장."""
    binds = {f"id{i}": v for i, v in enumerate(job_ids)}
    return ", ".join(f":{k}" for k in binds), binds


def _update_status(job_ids: list[str], set_clause: str, extra: dict, where_status: str) -> None:
    """job_id 목록 대상 상태 UPDATE. stat_desc(CLOB)는 WHERE 조건 금지 (설계 4.2)."""
    if not job_ids:
        return
    clause, binds = _in_binds(job_ids)
    OracleHook(oracle_conn_id=ORACLE_CONN_ID).run(
        f"UPDATE JOB_HISTORY SET {set_clause} "
        f"WHERE job_id IN ({clause}) AND status IN ({where_status})",
        parameters={**extra, **binds},
    )


def snapshot_exists(table: str, batch_id: str) -> bool:
    """영수증 확인 (설계 4.2): 테이블 snapshot summary에 batch_id 존재 여부.
    TODO(연결): 기존 Trino/Spark 조회 경로 재사용.
      SELECT 1 FROM <catalog>.<db>.<table>.snapshots
       WHERE element_at(summary, 'batch_id') = :batch_id
    """
    raise NotImplementedError


def upload_path_list_to_s3(table: str, jobs: list[dict], batch_id: str) -> None:
    """TODO(연결): 기존 get_jobs의 avro 경로 목록 S3 업로드 로직 재사용."""
    raise NotImplementedError


def send_alert(message: str, detail=None) -> None:
    """TODO(연결): 기존 알림 채널 재사용."""
    raise NotImplementedError


# --- 재처리용 조회 로직 (ConvertFileTaskGroup에 주입) -----------------------

def reprocess_get_jobs(cfg: dict, *, table_name: str, group: str, run_id, ti) -> bool:
    """append get_jobs의 재처리 버전 (조회 범위·상한·영수증 확인만 다름).

    ConvertFileTaskGroup이 자기 그룹 컨텍스트에서 이 함수를
    @task.short_circuit(trigger_rule="all_done", ignore_downstream_trigger_rules=False)로
    래핑해 사용한다 (설계 5.2). 반환 False = 처리 대상 없음 → 그룹 내 하류 skip.
    meta는 key="meta"로 push (append get_jobs와 동일 계약: 하류 Spark·update가 소비).
    """
    if not cfg:
        raise ValueError("prepare_run 결과 없음 — 선행 task 실패")
    if table_name not in cfg["tables"]:
        return False  # 수동 실행에서 미선택 → skip

    cols = ("job_id", "status", "stat_desc", "ts", "avro_path", "file_size_mb")
    jobs = _rows(
        """
        SELECT job_id, status, stat_desc, ts, avro_path, file_size_mb FROM (
            SELECT job_id, status, stat_desc, ts, avro_path, file_size_mb
              FROM JOB_HISTORY
             WHERE table_name = :tbl
               AND ts >= :ts_from AND ts < :ts_to
               AND ( status = 'FAILED'
                     OR (status = 'WAIT' AND ts < :wait_bound) )
             ORDER BY ts ASC
        ) WHERE ROWNUM <= :row_limit
        """,
        {"tbl": table_name, "ts_from": cfg["ts_from"], "ts_to": cfg["ts_to"],
         "wait_bound": cfg["wait_bound"], "row_limit": ROW_LIMIT},
        cols,
    )
    fetched_full = len(jobs) >= ROW_LIMIT  # 잔여분 판정은 필터 전 건수 (설계 5.3)

    # 영수증 확인: 거짓 실패(커밋됐는데 FAILED)는 DONE 정정 후 제외 (설계 4)
    failed_batches = {j["stat_desc"] for j in jobs
                      if j["status"] == "FAILED" and j["stat_desc"]}
    committed = {b for b in failed_batches if snapshot_exists(table_name, b)}
    if committed:
        _update_status(
            [j["job_id"] for j in jobs if j["stat_desc"] in committed],
            "status = 'DONE'", {}, "'IN_PROGRESS', 'FAILED'",
        )
        jobs = [j for j in jobs if j["stat_desc"] not in committed]

    # 크기 상한: 초과분 이월 (loop 회차 또는 다음날 회수, 설계 5.4)
    picked, total_mb = [], 0
    for j in jobs:
        if total_mb + j["file_size_mb"] > SIZE_LIMIT_MB:
            break
        picked.append(j)
        total_mb += j["file_size_mb"]
    if not picked:
        return False
    leftover = fetched_full or len(picked) < len(jobs)

    # 원자적 IN_PROGRESS 전환 + batch_id 기록 (설계 5.3)
    batch_id = f"{run_id}_{table_name}"
    _update_status(
        [j["job_id"] for j in picked],
        "status = 'IN_PROGRESS', stat_desc = :batch_id",
        {"batch_id": batch_id}, "'WAIT', 'FAILED'",
    )

    # 마킹 직후 meta 기록 (이후 단계 실패 시 update_failure가 job_ids로 회수, 설계 5.3)
    ti.xcom_push(key="meta", value={
        "table": table_name,
        "group": group,
        "batch_id": batch_id,
        "job_ids": [j["job_id"] for j in picked],
        "leftover": leftover,
        "ts_min": picked[0]["ts"], "ts_max": picked[-1]["ts"],
        "num_executors": min(max(math.ceil(total_mb / 128 * 1.5 / 4), 1), MAX_EXECUTORS),
    })
    upload_path_list_to_s3(table_name, picked, batch_id)
    return True


# --- 기존 ConvertFileTaskGroup 재사용 --------------------------------------
#
# append DAG의 ConvertFileTaskGroup(TaskGroup 서브클래스)은 대략 이 흐름을 담고 있다:
#   get_jobs → spark append → [update_success, update_failure]
# 재처리는 이 중 get_jobs(조회)만 다르고 나머지는 동일하다. 그래서 새로 만들지 않고
# 상속해서 get_jobs 생성 부분만 override 한다 (append용 조회 → 재처리용 조회).
#
# ┌ 부모(ConvertFileTaskGroup)가 아래 둘 중 하나면 그대로 재사용된다:
# │  (a) get_jobs를 별도 메서드로 분리해 뒀다  → 그 메서드만 override (아래 예시)
# │  (b) get_jobs를 인자로 받는다              → 생성자에 넘기면 됨
# └ 만약 __init__에 인라인으로 박혀 있으면, 부모에서 그 부분만 메서드로 추출
#    (템플릿 1곳 소규모 리팩토링)하면 (a)가 된다.
#
# TODO(연결): 아래 부모 클래스명·생성자 인자·override 메서드명을 실제 정의에 맞출 것.

class ReprocessTaskGroup(ConvertFileTaskGroup):  # noqa: F821  TODO(연결): 부모 import
    """ConvertFileTaskGroup 재사용 — get_jobs만 재처리 조회로 교체.
    Spark append / update_success / update_failure는 부모 것을 그대로 사용한다.
    """

    def __init__(self, table, run_cfg, **kwargs):
        self._table = table
        self._run_cfg = run_cfg
        super().__init__(table, **kwargs)  # TODO(연결): 부모 __init__ 시그니처에 맞출 것

    def build_get_jobs(self):
        """부모의 get_jobs 생성 메서드를 override (메서드명은 부모 정의에 맞출 것).
        조회 범위·상한·영수증 확인만 다르고, 반환 계약(False=대상없음→skip, meta push)은 동일.
        """
        table_name = self._table.get_name()
        group = compaction_group(self._table)

        @task.short_circuit(
            task_id="get_jobs",
            trigger_rule="all_done",                # 앞 테이블 실패에도 실행 (순차 그룹)
            ignore_downstream_trigger_rules=False,  # skip을 그룹 내로 한정 (설계 5.2)
        )
        def get_jobs(cfg: dict, run_id=None, ti=None):
            return reprocess_get_jobs(cfg, table_name=table_name, group=group,
                                      run_id=run_id, ti=ti)

        return get_jobs(self._run_cfg)


def collect_metas(ti) -> list[dict]:
    """처리 대상이 있던 테이블들의 get_jobs meta 수집 (설계 6.3 / 5.5).

    get_jobs가 처리 대상을 선점하면(True) meta를 push하므로, meta 존재 = 그 테이블이
    이번 회차에 적재를 시도했다는 뜻. Compaction은 이 목록으로 trigger하고, loop는
    leftover 플래그로 판단한다. (Airflow 3 worker는 메타데이터 DB 접근 불가 —
    task 상태 조회 대신 XCom만 사용)
    """
    metas = []
    for t in ALL_TABLES:
        meta = ti.xcom_pull(task_ids=f"reprocess_{t.get_name()}.get_jobs", key="meta")
        if meta:
            metas.append(meta)
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
        # multi-select: 선택지·기본값 모두 iceberg.py Enum에서 생성 (설계 5.1)
        "tables": Param(
            default=[t.get_name() for t in ALL_TABLES],
            type="array",
            items={"type": "string", "enum": [t.get_name() for t in ALL_TABLES]},
        ),
        # 수동 실행 조회 범위 (둘 다 함께, end ≤ 전날 00:00 — prepare_run 검증).
        # 미지정 시 정기 범위(그저께 00:00 ~ 전날 끝). date-time → ts 문자열 변환.
        "start_time": Param(None, type=["null", "string"], format="date-time"),
        "end_time": Param(None, type=["null", "string"], format="date-time"),
    },
    tags=["iceberg", "reprocess"],
)
def dag():  # 함수명 dag() 고정 — DAG 정체성은 파일명(dag_id)이 담당

    @task
    def check_zombie_jobs():
        """좀비 IN_PROGRESS 탐지 → 알림만 (설계 8.2). 독립 실행 — 본류와 의존 없음."""
        zombies = _rows(
            "SELECT table_name, job_id, updated_at FROM JOB_HISTORY "
            "WHERE status = 'IN_PROGRESS' "
            "  AND updated_at < SYSTIMESTAMP - NUMTODSINTERVAL(:h, 'HOUR')",
            {"h": ZOMBIE_HOURS},
            ("table_name", "job_id", "updated_at"),
        )
        if zombies:
            send_alert(f"좀비 IN_PROGRESS {len(zombies)}건 — 영수증 확인 후 수동 판정 필요", zombies)

    @task
    def prepare_run(params=None, dag_run=None) -> dict:
        """params 검증·정규화 1회 (append DAG의 get_time 패턴). 이후 task는 XCom만 소비.
        loop 회차는 첫 회차가 확정한 값을 conf로 승계 (설계 5.5)."""
        conf = dag_run.conf or {}
        if conf.get("ts_from"):  # loop 재trigger 회차 — 승계 (재계산 안 함)
            return {k: conf[k] for k in ("tables", "ts_from", "ts_to", "wait_bound")} | {
                "loop_count": int(conf.get("loop_count", 0)),
            }

        base = pendulum.now(KST).start_of("day")
        st, et = params.get("start_time"), params.get("end_time")

        if st and et:  # 수동: 조회 범위 직접 정의. end ≤ 전날 00:00 (append 겹침 방지)
            ts_from = ts_str(pendulum.parse(st, tz=KST).in_timezone(KST))
            ts_to = ts_str(pendulum.parse(et, tz=KST).in_timezone(KST))
            if not ts_from < ts_to <= ts_str(base.subtract(days=1)):
                raise ValueError("start < end 이고 end_time ≤ 전날 00:00 이어야 한다")
            wait_bound = ts_to  # 범위 전체가 append 범위 밖 → WAIT 전 구간 허용
        elif st or et:
            raise ValueError("start_time과 end_time은 함께 지정해야 한다")
        else:  # 정기: 그저께 00:00 ~ 전날 끝, WAIT는 전날 01:00 이전만 (설계 2.1)
            ts_from = ts_str(base.subtract(days=2))
            ts_to = ts_str(base)
            wait_bound = ts_str(base.subtract(days=1).add(hours=1))

        return {
            "tables": list(params["tables"]),
            "ts_from": ts_from, "ts_to": ts_to, "wait_bound": wait_bound,
            "loop_count": 0,
        }

    def build_table_group(table, run_cfg):
        """테이블별 그룹 생성 — 기존 ConvertFileTaskGroup을 상속해 조회 task만 교체."""
        return ReprocessTaskGroup(table, run_cfg,
                                  group_id=f"reprocess_{table.get_name()}")

    @task(trigger_rule="all_done")
    def compaction_targets(ti=None) -> list[dict]:
        """적재 결과 집계 → TriggerDagRunOperator kwargs 목록 (설계 6.3).
        적재분 전부 trigger — tables 필터로 비용 최소, 중복은 no-op.
        TODO(연결): conf 날짜/시간 형식을 기존 Compaction DAG UI params와 일치시킬 것."""
        daily: dict[str, list[str]] = {}   # 날짜(YYYYMMDD) → 테이블 목록
        hourly_tables, hourly_min, hourly_max = [], None, None
        for m in collect_metas(ti):
            if m["group"] == "daily":
                for date in dates_between(m["ts_min"], m["ts_max"]):
                    daily.setdefault(date, []).append(m["table"])
            else:
                hourly_tables.append(m["table"])
                hourly_min = min(hourly_min or m["ts_min"], m["ts_min"])
                hourly_max = max(hourly_max or m["ts_max"], m["ts_max"])

        targets = [
            {"trigger_dag_id": DAILY_COMPACTION_DAG_ID,
             "conf": {"target_dt": dt, "tables": tables}}
            for dt, tables in daily.items()
        ]
        if hourly_tables:
            targets.append({
                "trigger_dag_id": HOURLY_COMPACTION_DAG_ID,
                "conf": {"start_time": hourly_min, "end_time": hourly_max,
                         "tables": hourly_tables},
            })
        return targets  # 빈 목록이면 mapped operator는 skip

    @task(trigger_rule="all_done")
    def next_loop(cfg: dict, ti=None) -> list[dict]:
        """재trigger 판단 (설계 5.5) → TriggerDagRunOperator kwargs 0/1건.
        잔여분(상한 초과 이월)이 있는 테이블이 하나라도 있으면 재trigger.
        지속 실패도 leftover 플래그 + MAX_LOOP 상한으로 유한하게 종료된다."""
        if not cfg or not any(m["leftover"] for m in collect_metas(ti)):
            return []
        if cfg["loop_count"] >= MAX_LOOP:
            send_alert(f"재처리 loop 상한({MAX_LOOP}회) 도달 — 수동 처리 필요 (설계 8.1)")
            return []
        # 첫 회차가 확정한 조회 범위·tables를 conf로 승계 (수동 선택값·경계 유지)
        return [{"trigger_dag_id": DAG_ID,
                 "conf": {**cfg, "loop_count": cfg["loop_count"] + 1}}]

    # check_zombie_jobs는 독립 실행 (알림 실패가 재처리 본류를 막지 않음).
    check_zombie_jobs()

    run_cfg = prepare_run()
    groups = [build_table_group(t, run_cfg) for t in ALL_TABLES]
    chain(run_cfg, *groups)                       # 테이블 순차 실행
    tail = groups[-1] if groups else run_cfg       # 모든 그룹 완료 후 집계 (빈 목록 안전)

    # 적재분 Compaction / 잔여분 loop — 개수 가변이라 dynamic task mapping으로 trigger.
    # (TriggerDagRunOperator는 wait_for_completion 기본 False → 설계 6.3 대기 없음)
    comp, nxt = compaction_targets(), next_loop(run_cfg)
    tail >> comp
    tail >> nxt
    TriggerDagRunOperator.partial(task_id="trigger_compaction").expand_kwargs(comp)
    TriggerDagRunOperator.partial(task_id="retrigger_self").expand_kwargs(nxt)


dag()
