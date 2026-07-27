"""재처리(Reprocessing) DAG — 구현 스켈레톤.

설계 문서: pipeline/reprocessing-dag-design.md
환경: Airflow 3.2.2

역할: append DAG 조회 기간(최근 1일)에서 밀려난 WAIT/FAILED 데이터를 전날+그저께
      범위에서 회수하고, 적재분에 대해 기존 Compaction DAG을 trigger한다.

구조: 단일 DAG. 테이블별로 기존 ConvertFileTaskGroup을 상속(get_jobs만 override)해
      순차 실행. params는 prepare_run에서 1회 검증·정규화.
      잔여분이 남으면 자기 자신을 재trigger (loop, 상한 10회).

── 기존 구현 연결 지점 (grep "TODO(연결)") ────────────────────────
  1. iceberg.py의 hourly/daily Enum import (자리표시자 2개 교체)
  2. ConvertFileTaskGroup import + 상속 방식 (ReprocessTaskGroup 주석 참조)
  3. Oracle conn id
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


def compaction_group(table) -> str:
    """Compaction 그룹: 소속 Enum 클래스가 곧 분류다."""
    return "hourly" if isinstance(table, HourlyIcebergTable) else "daily"


# --- 시간 유틸 --------------------------------------------------------------

def ts_str(dt: pendulum.DateTime) -> str:
    """pendulum datetime → Job History ts 형식 'YYYYMMDDHHmmSSsss'."""
    return dt.format("YYYYMMDDHHmmss") + "000"


def param_to_ts(value: str) -> str:
    """date-time param → ts 문자열. offset이 와도 KST로 통일."""
    return ts_str(pendulum.parse(value, tz=KST).in_timezone(KST))


def dates_between(ts_min: str, ts_max: str) -> list[str]:
    """ts_min~ts_max 구간이 걸치는 모든 날짜(YYYYMMDD) 목록.

    daily Compaction은 날짜 단위로 trigger하므로 적재 범위가 걸친 날짜를 빠짐없이
    뽑아야 한다. 양 끝 날짜만 쓰면 3일 이상 범위에서 중간 날짜가 누락된다.

    예) ts_min='20260705213000000', ts_max='20260707081500000'
        → ts 앞 8자리(날짜)만 취해 5일부터 7일까지 하루씩 전진
        → ['20260705', '20260706', '20260707']
    """
    day = pendulum.from_format(ts_min[:8], "YYYYMMDD", tz=KST)
    last = pendulum.from_format(ts_max[:8], "YYYYMMDD", tz=KST)
    days = []
    while day <= last:
        days.append(day.format("YYYYMMDD"))
        day = day.add(days=1)
    return days


# --- Oracle -----------------------------------------------------------------

JOB_COLUMNS = ("job_id", "status", "stat_desc", "ts", "avro_path", "file_size_mb")

SELECT_TARGETS_SQL = f"""
SELECT * FROM (
    SELECT {", ".join(JOB_COLUMNS)}
      FROM JOB_HISTORY
     WHERE table_name = :tbl
       AND ts >= :ts_from AND ts < :ts_to
       AND ( status = 'FAILED'
             OR (status = 'WAIT' AND ts < :wait_bound) )
     ORDER BY ts ASC
) WHERE ROWNUM <= :row_limit
"""

ZOMBIE_SQL = """
SELECT table_name, job_id, updated_at
  FROM JOB_HISTORY
 WHERE status = 'IN_PROGRESS'
   AND updated_at < SYSTIMESTAMP - NUMTODSINTERVAL(:h, 'HOUR')
"""


def _rows(sql: str, binds: dict, columns: tuple[str, ...]) -> list[dict]:
    """OracleHook.get_records → 컬럼명 dict 매핑. columns는 SELECT 순서와 일치해야 한다."""
    records = OracleHook(oracle_conn_id=ORACLE_CONN_ID).get_records(sql, parameters=binds)
    return [dict(zip(columns, r)) for r in records]


def _in_binds(values: list) -> tuple[str, dict]:
    """Oracle IN절용 placeholder 동적 확장.

    Oracle은 IN (:ids)에 파이썬 list를 통째로 바인딩할 수 없어서,
    값 개수만큼 개별 placeholder를 만들어 바인딩한다.

    예) values=['J1', 'J2', 'J3']
        → (":id0, :id1, :id2", {"id0": "J1", "id1": "J2", "id2": "J3"})
        사용: f"... WHERE job_id IN ({clause})" + parameters=binds
    """
    binds = {f"id{i}": v for i, v in enumerate(values)}
    return ", ".join(f":{k}" for k in binds), binds


def _mark_done(job_ids: list[str]) -> None:
    """영수증으로 커밋이 확인된 FAILED row를 DONE으로 정정 (설계 4.2)."""
    clause, binds = _in_binds(job_ids)
    OracleHook(oracle_conn_id=ORACLE_CONN_ID).run(
        f"UPDATE JOB_HISTORY SET status = 'DONE' "
        f"WHERE job_id IN ({clause}) AND status = 'FAILED'",
        parameters=binds,
    )


def _claim_jobs(job_ids: list[str], batch_id: str) -> None:
    """원자적 IN_PROGRESS 전환 + batch_id(영수증) 기록 (설계 5.3).
    stat_desc(CLOB)는 값 기록만 — WHERE 조건 사용 금지 (설계 4.2)."""
    clause, binds = _in_binds(job_ids)
    OracleHook(oracle_conn_id=ORACLE_CONN_ID).run(
        f"UPDATE JOB_HISTORY SET status = 'IN_PROGRESS', stat_desc = :batch_id "
        f"WHERE job_id IN ({clause}) AND status IN ('WAIT', 'FAILED')",
        parameters={"batch_id": batch_id, **binds},
    )


# --- 기존 구현 연결 스텁 ------------------------------------------------------

def snapshot_exists(table_name: str, batch_id: str) -> bool:
    """영수증 확인 (설계 4.2): 테이블 snapshot summary에 batch_id 존재 여부.
    TODO(연결): 기존 Trino/Spark 조회 경로 재사용.
      SELECT 1 FROM <catalog>.<db>.<table>.snapshots
       WHERE element_at(summary, 'batch_id') = :batch_id
    """
    raise NotImplementedError


def upload_path_list_to_s3(table_name: str, jobs: list[dict], batch_id: str) -> None:
    """TODO(연결): 기존 get_jobs의 avro 경로 목록 S3 업로드 로직 재사용."""
    raise NotImplementedError


def send_alert(message: str, detail=None) -> None:
    """TODO(연결): 기존 알림 채널 재사용."""
    raise NotImplementedError


# --- 재처리 조회 로직 ---------------------------------------------------------

def reprocess_get_jobs(cfg: dict, *, table, run_id, ti) -> bool:
    """append get_jobs의 재처리 버전 — 조회 범위·상한·영수증 확인만 다르다.

    반환 False = 처리 대상 없음 (short_circuit → 그룹 내 하류 skip).
    meta는 key="meta"로 push — 하류 Spark(num_executors)·update(job_ids)·
    집계 task(Compaction/loop)가 소비한다.
    """
    if not cfg:
        raise ValueError("prepare_run 결과 없음 — 선행 task 실패")

    table_name = table.get_name()
    if table_name not in cfg["tables"]:
        return False  # 수동 실행에서 미선택 → skip

    jobs = _rows(
        SELECT_TARGETS_SQL,
        {"tbl": table_name, "ts_from": cfg["ts_from"], "ts_to": cfg["ts_to"],
         "wait_bound": cfg["wait_bound"], "row_limit": ROW_LIMIT},
        JOB_COLUMNS,
    )
    # 조회가 ROW_LIMIT을 꽉 채웠다 = DB에 더 남아 있다는 신호.
    # 아래 영수증/크기 필터로 jobs가 줄어든 "후"의 건수로 판단하면 이 신호를
    # 놓치므로, 반드시 필터 적용 "전"에 기록해 둔다 (설계 5.3)
    fetched_full = len(jobs) >= ROW_LIMIT

    # ── 영수증 확인 (설계 4): "거짓 실패" 걸러내기 ──────────────────────────
    # Airflow가 실패로 판정했어도 Iceberg 커밋은 성공했을 수 있다 (커밋 직후
    # Pod 통신 오류 등). 그대로 재적재하면 중복이 되므로:
    #   1) FAILED row들이 달고 있는 batch_id(stat_desc)를 set으로 수집
    #      — 같은 batch의 row가 수백 건이어도 snapshot 조회는 batch당 1회
    #   2) batch_id별로 해당 테이블 snapshot에 영수증이 있는지 확인
    #   3) 있으면 = 이미 적재 완료 → row들을 DONE으로 정정하고 이번 대상에서 제외
    failed_batches = {j["stat_desc"] for j in jobs
                      if j["status"] == "FAILED" and j["stat_desc"]}
    committed = {b for b in failed_batches if snapshot_exists(table_name, b)}
    if committed:
        _mark_done([j["job_id"] for j in jobs if j["stat_desc"] in committed])
        jobs = [j for j in jobs if j["stat_desc"] not in committed]

    # ── 크기 상한 적용 (설계 5.4) ───────────────────────────────────────────
    # jobs는 ts ASC 정렬 상태이므로, 앞(가장 오래된 것)부터 누적 크기가
    # 16GB를 넘기 직전까지만 picked에 담고 멈춘다.
    # 잘린 뒤쪽은 상태를 건드리지 않고 이월 → loop 회차 또는 다음날 회수
    picked, total_mb = [], 0
    for j in jobs:
        if total_mb + j["file_size_mb"] > SIZE_LIMIT_MB:
            break
        picked.append(j)
        total_mb += j["file_size_mb"]
    if not picked:
        return False

    # 잔여분(leftover) 판정 — 둘 중 하나라도 참이면 "아직 남았다":
    #   fetched_full           : 조회가 1,000건을 꽉 채움 → DB에 더 있음
    #   len(picked) < len(jobs): 크기 상한으로 뒤쪽이 잘림 → 이번에 못 담은 게 있음
    # (영수증으로 제외된 건은 '처리 완료 정정'이라 잔여분이 아님 —
    #  이미 jobs에서 빠진 뒤라 이 비교에 영향을 주지 않는다)
    leftover = fetched_full or len(picked) < len(jobs)

    batch_id = f"{run_id}_{table_name}"
    _claim_jobs([j["job_id"] for j in picked], batch_id)

    # 마킹 직후 meta 기록 — 이후 단계 실패 시에도 update_failure가 job_ids로 회수 (설계 5.3)
    ti.xcom_push(key="meta", value={
        "table": table_name,
        "group": compaction_group(table),
        "batch_id": batch_id,
        "job_ids": [j["job_id"] for j in picked],
        "leftover": leftover,
        "ts_min": picked[0]["ts"], "ts_max": picked[-1]["ts"],
        # append DAG과 동일 산정식: ceil(총크기/128MB × 1.5 / executor-cores 4)
        "num_executors": min(max(math.ceil(total_mb / 128 * 1.5 / 4), 1), MAX_EXECUTORS),
    })
    upload_path_list_to_s3(table_name, picked, batch_id)
    return True


# --- 기존 ConvertFileTaskGroup 재사용 (상속 + get_jobs override) -------------
#
# ConvertFileTaskGroup: get_jobs → spark append → [update_success, update_failure]
# 재처리는 get_jobs(조회)만 다르다.
#
# 전제 — 부모 1회 추출 리팩토링 (동작 동일, 섹션 7):
#   현재 get_jobs는 __init__ 안에 @task(task_group=self)로 인라인 정의되어 있고,
#   __init__ 지역 함수 _update_jobs를 closure로 호출한다. 따라서
#   ① get_jobs 블록만 메서드로 이동 ② _update_jobs는 __init__에 그대로 두고
#   메서드 인자로 전달한다 (closure 참조 → 인자 호출로만 교체):
#
#     class ConvertFileTaskGroup(TaskGroup):
#         def __init__(self, table, group_id, ..., **kwargs):
#             super().__init__(group_id=group_id, **kwargs)
#             self.table = table
#
#             def _update_jobs(...):                     # 그대로 __init__에 둠
#                 ...
#
#             jobs = self._build_get_jobs(_update_jobs)  # ← 헬퍼를 인자로 전달
#             spark = ...
#             jobs >> spark >> [update_success, update_failure]
#
#         def _build_get_jobs(self, update_jobs):
#             @task(task_group=self)
#             def get_jobs(ti=None):
#                 ... 기존 코드 그대로 (update_jobs(...) 호출 포함) ...
#             return get_jobs()
#
# 부모 __init__이 self._build_get_jobs(...)를 호출할 때 자식 override가 실행되므로
# (파이썬 메서드 디스패치), 재처리는 아래처럼 override만 하면 된다.
#
# TODO(연결): 부모 import + __init__ 시그니처·추출 메서드명(_build_get_jobs)·
#             _update_jobs 시그니처를 실제 정의에 맞출 것.

class ReprocessTaskGroup(ConvertFileTaskGroup):  # noqa: F821  TODO(연결): 부모 import
    """ConvertFileTaskGroup 상속 — get_jobs만 재처리 조회로 교체.
    Spark append / update_success / update_failure는 부모 것을 그대로 사용한다.
    """

    def __init__(self, table, run_cfg, **kwargs):
        # 부모 __init__이 _build_get_jobs()를 호출하므로, override가 쓰는 값은
        # 반드시 super().__init__() 호출 전에 self에 넣어야 한다.
        self._run_cfg = run_cfg
        super().__init__(table, **kwargs)  # TODO(연결): 부모 __init__ 시그니처에 맞출 것

    def _build_get_jobs(self, update_jobs):
        """부모의 get_jobs 생성 메서드 override.

        update_jobs: 부모 __init__의 상태 update 헬퍼.
        TODO(연결): 시그니처가 맞으면 _mark_done/_claim_jobs 대신 이 헬퍼 사용 가능.
        """
        table = self.table  # 부모가 저장한 Enum 그대로 사용

        @task.short_circuit(
            task_group=self,                        # 부모와 동일 — with self: 없이 생성되므로 필수.
            task_id="get_jobs",                     # 누락 시 dag 레벨 생성 → task_id 충돌
            trigger_rule="all_done",                # 앞 테이블 실패에도 실행 (순차 그룹)
            ignore_downstream_trigger_rules=False,  # skip을 그룹 내로 한정 (설계 5.2)
        )
        def get_jobs(cfg: dict, run_id=None, ti=None):
            return reprocess_get_jobs(cfg, table=table, run_id=run_id, ti=ti)

        return get_jobs(self._run_cfg)


def collect_metas(ti) -> list[dict]:
    """처리 대상이 있던 테이블들의 get_jobs meta 수집 (설계 6.3 / 5.5).

    meta 존재 = 그 테이블이 이번 회차에 처리 대상을 선점했다는 뜻.
    (Airflow 3 worker는 메타데이터 DB 접근 불가 — task 상태 조회 대신 XCom만 사용)
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
        # 미지정 시 정기 범위(그저께 00:00 ~ 전날 끝).
        "start_time": Param(None, type=["null", "string"], format="date-time"),
        "end_time": Param(None, type=["null", "string"], format="date-time"),
    },
    tags=["iceberg", "reprocess"],
)
def dag():  # 함수명 dag() 고정 — DAG 정체성은 파일명(dag_id)이 담당

    @task
    def check_zombie_jobs():
        """좀비 IN_PROGRESS 탐지 → 알림만 (설계 8.2). 독립 실행 — 본류와 의존 없음."""
        zombies = _rows(ZOMBIE_SQL, {"h": ZOMBIE_HOURS},
                        ("table_name", "job_id", "updated_at"))
        if zombies:
            send_alert(f"좀비 IN_PROGRESS {len(zombies)}건 — 영수증 확인 후 수동 판정 필요",
                       zombies)

    @task
    def prepare_run(params=None, dag_run=None) -> dict:
        """params 검증·정규화 1회 (append DAG의 get_time 패턴). 이후 task는 XCom만 소비."""
        conf = dag_run.conf or {}

        # loop 재trigger 회차: 첫 회차가 확정한 값을 그대로 승계 (설계 5.5)
        if conf.get("ts_from"):
            return {
                "tables": conf["tables"],
                "ts_from": conf["ts_from"],
                "ts_to": conf["ts_to"],
                "wait_bound": conf["wait_bound"],
                "loop_count": int(conf.get("loop_count", 0)),
            }

        base = pendulum.now(KST).start_of("day")  # 오늘 00:00 (10일 01:00 실행이면 10일 00:00)
        st, et = params.get("start_time"), params.get("end_time")

        if st and et:
            # 수동: start/end가 조회 범위를 직접 정의 (설계 5.1).
            # append DAG은 "실행 시각-24시간 이후"를 계속 조회하므로, 전날 00:00
            # 이후를 허용하면 두 DAG이 같은 WAIT를 집을 수 있다
            # → end_time ≤ 전날 00:00 검증으로 원천 차단
            ts_from, ts_to = param_to_ts(st), param_to_ts(et)
            if not ts_from < ts_to <= ts_str(base.subtract(days=1)):
                raise ValueError("start < end 이고 end_time ≤ 전날 00:00 이어야 한다")
            wait_bound = ts_to  # 범위 전체가 append 범위 밖 → WAIT 전 구간 허용
        elif st or et:
            raise ValueError("start_time과 end_time은 함께 지정해야 한다")
        else:
            # 정기 실행 경계 (설계 2.1). base=10일 00:00 기준 예시:
            #   ts_from    = 8일 00:00  — 그저께 시작. 재처리가 하룻밤 실패해도
            #                             다음날 이 안전망 범위로 자동 회수된다
            #   ts_to      = 10일 00:00 — 전날 끝
            #   wait_bound = 9일 01:00  — WAIT 상한. 이 시각 이후의 WAIT는 아직
            #                             append 조회 범위(실행시각-24h) 안이라
            #                             건드리면 경합 → 재처리 대상에서 제외.
            #                             FAILED는 append가 안 보므로 상한 없음
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
    def compaction_targets(ti=None) -> list[dict]:
        """적재 결과 집계 → TriggerDagRunOperator kwargs 목록 (설계 6.3).
        적재분 전부 trigger — tables 필터로 비용 최소, 중복은 no-op.
        TODO(연결): conf 날짜/시간 형식을 기존 Compaction DAG UI params와 일치시킬 것."""
        metas = collect_metas(ti)
        daily = [m for m in metas if m["group"] == "daily"]
        hourly = [m for m in metas if m["group"] == "hourly"]

        # ── daily 그룹: "테이블 → 적재 범위"를 "날짜 → 테이블 목록"으로 뒤집기 ──
        # meta는 테이블 단위(테이블당 적재 ts 범위)인데, daily Compaction DAG은
        # 날짜 단위(target_dt)로 실행되므로 집계 축을 바꿔야 한다.
        #   바깥 for: 테이블별 meta 순회
        #   안쪽 for: 그 테이블의 적재 범위가 걸친 날짜들 순회 (dates_between)
        # 예) TABLE_A가 8~9일, TABLE_B가 9일만 적재했다면
        #     by_date = {'20260708': [A], '20260709': [A, B]}
        #     → trigger 2건: (8일, [A]), (9일, [A, B])
        by_date: dict[str, list[str]] = {}
        for m in daily:
            for date in dates_between(m["ts_min"], m["ts_max"]):
                by_date.setdefault(date, []).append(m["table"])
        targets = [
            {"trigger_dag_id": DAILY_COMPACTION_DAG_ID,
             "conf": {"target_dt": date, "tables": tables}}
            for date, tables in by_date.items()
        ]

        # ── hourly 그룹: 시간 범위 합집합으로 1회 trigger ────────────────────
        # 테이블별 범위를 전체 min~전체 max 하나로 합쳐 tables와 함께 넘긴다.
        # 테이블 간 범위 차이로 사이에 빈 시간대가 포함될 수 있지만, Compaction은
        # 합칠 파일이 없는 구간에서 no-op이라 무해 — trigger 횟수를 1회로 줄이는
        # 쪽이 이득 (Compaction DAG은 max_active_runs=1이라 run이 곧 대기열)
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
    def next_loop(cfg: dict, ti=None) -> list[dict]:
        """재trigger 판단 (설계 5.5) → TriggerDagRunOperator kwargs 0/1건.
        잔여분(상한 초과 이월) 있는 테이블이 하나라도 있으면 재trigger.
        지속 실패도 leftover + MAX_LOOP 상한으로 유한하게 종료된다."""
        # 종료 ①: prepare_run 실패(cfg 없음) 또는 잔여분 있는 테이블이 하나도 없음
        if not cfg or not any(m["leftover"] for m in collect_metas(ti)):
            return []
        # 종료 ②: 회차 상한 도달 — 이 정도 물량은 자동으로 다 못 푼다는 신호 → 알림 후 수동
        if cfg["loop_count"] >= MAX_LOOP:
            send_alert(f"재처리 loop 상한({MAX_LOOP}회) 도달 — 수동 처리 필요 (설계 8.1)")
            return []
        # 재trigger: 첫 회차가 확정한 조회 범위·tables를 conf 그대로 승계.
        # 다음 회차의 prepare_run은 conf에 ts_from이 있으면 재계산하지 않으므로,
        # 수동 선택값이 유실되지 않고 회차가 자정을 넘겨도 경계가 흔들리지 않는다
        return [{"trigger_dag_id": DAG_ID,
                 "conf": {**cfg, "loop_count": cfg["loop_count"] + 1}}]

    # 좀비 점검은 독립 실행 — 알림 실패가 재처리 본류를 막지 않음 (설계 8.2)
    check_zombie_jobs()

    # 본류: params 정규화 → 테이블별 그룹 순차 → 집계(Compaction/loop) → trigger
    run_cfg = prepare_run()
    groups = [ReprocessTaskGroup(t, run_cfg, group_id=f"reprocess_{t.get_name()}")
              for t in ALL_TABLES]
    # chain(run_cfg, g1, g2, ..., gN) = prepare_run → 그룹1 → 그룹2 → ... 순차 연결.
    # Spark job이 동시에 여러 개 뜨지 않도록 순차 (K8S 리소스, 설계 5.2).
    # 각 그룹 첫 task(get_jobs)가 trigger_rule="all_done"이라 앞 테이블이
    # 실패/skip해도 다음 테이블은 계속 진행된다
    chain(run_cfg, *groups)
    tail = groups[-1] if groups else run_cfg  # 자리표시자(빈 Enum) 상태에서도 파싱 가능

    comp, nxt = compaction_targets(), next_loop(run_cfg)
    tail >> comp
    tail >> nxt
    # trigger 대상 개수가 가변(Compaction 여러 건, loop 0/1건)이라 dynamic task mapping 사용.
    # TriggerDagRunOperator는 wait_for_completion 기본 False (설계 6.3 — 대기 없음)
    TriggerDagRunOperator.partial(task_id="trigger_compaction").expand_kwargs(comp)
    TriggerDagRunOperator.partial(task_id="retrigger_self").expand_kwargs(nxt)


dag()
