"""재처리(Reprocessing) DAG — 구현 스켈레톤.

설계 문서: pipeline/reprocessing-dag-design.md
환경: Airflow 3.2.2

역할: append DAG 조회 기간(최근 1일)에서 밀려난 WAIT 데이터와 FAILED 데이터를
      전날+그저께 범위에서 회수하고, 적재분에 대해 기존 Compaction DAG을 trigger한다.

구조: 단일 DAG, 테이블별로 기존 append DAG의 ConvertFileTaskGroup 템플릿을 재사용
      (조회 task만 재처리용으로 주입). params는 prepare_run에서 1회 검증·정규화.
      잔여분이 남으면 자기 자신을 재trigger (loop, 상한 10회).

── 기존 구현 연결 지점 ─────────────────────────────────────────────
grep "TODO(연결)" 으로 전체 확인.

  1. iceberg.py의 hourly/daily Enum import (자리표시자 2개 교체)
  2. ConvertFileTaskGroup import + 생성자 시그니처 (조회 task 주입 방식)
  3. Oracle conn id (OracleHook)
  4. 영수증 snapshot 조회 — snapshot_exists
  5. avro 경로 목록 S3 업로드 — upload_path_list_to_s3
  6. 알림 채널 — send_alert
  7. Spark 실행 — ConvertFileTaskGroup이 제공 (Spark 코드에 batch_id snapshot property)
  8. Compaction DAG id 상수 + conf 날짜/시간 형식 (기존 Compaction UI params와 일치)
     — trigger 자체는 TriggerDagRunOperator로 구현 완료
────────────────────────────────────────────────────────────────────
"""

import math
from enum import Enum
from pathlib import Path

import pendulum
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, chain, dag, task

# TODO(연결): 기존 append DAG의 ConvertFileTaskGroup 재사용
# from <공통 모듈>.taskgroups import ConvertFileTaskGroup
# TODO(연결): iceberg.py의 hourly/daily Enum
# from <공통 모듈>.iceberg import HourlyIcebergTable, DailyIcebergTable

KST = pendulum.timezone("Asia/Seoul")
DAG_ID = Path(__file__).stem  # 조직 컨벤션: dag_id는 파일명에서 파생 (단일 소스)

ORACLE_CONN_ID = "oracle_default"  # TODO(연결): 실제 conn id (append DAG과 동일)


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


def table_group(table) -> str:
    """Compaction 그룹: 소속 Enum 클래스가 곧 분류다."""
    return "hourly" if isinstance(table, HourlyIcebergTable) else "daily"


ROW_LIMIT = 1000              # 테이블당 조회 상한 (설계 5.4 — 러프 설정, 재검증 필요)
SIZE_LIMIT_MB = 16 * 1024     # 테이블당 크기 상한 16GB (설계 5.4 — 러프 설정, 재검증 필요)
MAX_EXECUTORS = 24            # 벤치마크 검증 상한 (spark-tuning-guide.md 2.2.3)
MAX_LOOP = 10                 # 자기 재trigger 상한 (설계 5.5)
ZOMBIE_HOURS = 2              # 좀비 IN_PROGRESS 판정 임계 (설계 8.2)

DAILY_COMPACTION_DAG_ID = "daily_compaction_dag"    # TODO(연결): 실제 DAG id
HOURLY_COMPACTION_DAG_ID = "hourly_compaction_dag"  # TODO(연결): 실제 DAG id


def ts_str(dt: pendulum.DateTime) -> str:
    """pendulum datetime → Job History ts 형식 'YYYYMMDDHHmmSSsss'."""
    return dt.format("YYYYMMDDHHmmss") + "000"


# --- Oracle (OracleHook 직접 사용) -----------------------------------------

def _select_jobs(sql: str, binds: dict) -> list[dict]:
    """SELECT → dict row 목록. OracleHook 사용 (append DAG과 동일 conn)."""
    hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, binds)
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _in_clause(job_ids: list[str]) -> tuple[str, dict]:
    """Oracle IN은 list를 바로 못 바인딩 → placeholder 동적 확장."""
    ids = {f"id{i}": v for i, v in enumerate(job_ids)}
    return ", ".join(f":{k}" for k in ids), ids


def _mark_status(job_ids: list[str], status: str) -> None:
    """job_id 목록 → 상태 UPDATE. stat_desc(CLOB)는 WHERE 조건 금지 (설계 4.2)."""
    if not job_ids:
        return
    clause, ids = _in_clause(job_ids)
    OracleHook(oracle_conn_id=ORACLE_CONN_ID).run(
        f"UPDATE JOB_HISTORY SET status = :status "
        f"WHERE job_id IN ({clause}) AND status = 'IN_PROGRESS'",
        parameters={"status": status, **ids},
    )


def _mark_in_progress(jobs: list[dict], batch_id: str) -> None:
    """원자적 IN_PROGRESS 전환 + batch_id 기록 (설계 5.3). stat_desc는 값 기록만."""
    clause, ids = _in_clause([j["job_id"] for j in jobs])
    OracleHook(oracle_conn_id=ORACLE_CONN_ID).run(
        f"UPDATE JOB_HISTORY SET status = 'IN_PROGRESS', stat_desc = :batch_id "
        f"WHERE job_id IN ({clause}) AND status IN ('WAIT', 'FAILED')",
        parameters={"batch_id": batch_id, **ids},
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
        zombies = _select_jobs(
            "SELECT table_name, job_id, updated_at FROM JOB_HISTORY "
            "WHERE status = 'IN_PROGRESS' "
            "  AND updated_at < SYSTIMESTAMP - NUMTODSINTERVAL(:h, 'HOUR')",
            {"h": ZOMBIE_HOURS},
        )
        if zombies:
            send_alert(f"좀비 IN_PROGRESS {len(zombies)}건 — 영수증 확인 후 수동 판정 필요", zombies)

    @task
    def prepare_run(params=None, dag_run=None) -> dict:
        """params 검증·정규화 1회 (append DAG의 get_time 패턴). 이후 task는 XCom만 소비.
        loop 회차는 첫 회차가 확정한 값을 conf로 승계 (설계 5.5)."""
        conf = dag_run.conf or {}
        if conf.get("ts_from"):  # loop 재trigger 회차 — 승계
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

    def build_get_jobs(table, run_cfg):
        """재처리용 조회 task 생성 — ConvertFileTaskGroup에 주입한다 (append의 get_jobs 대체).

        조회 범위·상한·영수증 확인만 append과 다르고, 이후(Spark·update_success/failure)는
        템플릿이 그대로 담당한다. meta XCom 형태는 append get_jobs와 동일하게 맞춘다.
        """
        tbl = table.get_name()

        @task.short_circuit(
            task_id="get_jobs",
            trigger_rule="all_done",                # 앞 테이블 실패에도 실행
            ignore_downstream_trigger_rules=False,  # skip을 그룹 내로 한정 (설계 5.2)
        )
        def get_jobs(cfg: dict, run_id=None, ti=None):
            if not cfg:
                raise ValueError("prepare_run 결과 없음 — 선행 task 실패")
            if tbl not in cfg["tables"]:
                return False  # 미선택 → skip

            jobs = _select_jobs(
                """
                SELECT * FROM (
                    SELECT job_id, status, stat_desc, ts, avro_path, file_size_mb
                      FROM JOB_HISTORY
                     WHERE table_name = :tbl
                       AND ts >= :ts_from AND ts < :ts_to
                       AND ( status = 'FAILED'
                             OR (status = 'WAIT' AND ts < :wait_bound) )
                     ORDER BY ts ASC
                ) WHERE ROWNUM <= :row_limit
                """,
                {"tbl": tbl, "ts_from": cfg["ts_from"], "ts_to": cfg["ts_to"],
                 "wait_bound": cfg["wait_bound"], "row_limit": ROW_LIMIT},
            )
            fetched_full = len(jobs) >= ROW_LIMIT  # 잔여분 판정 (필터 전 건수, 설계 5.3)

            # 영수증 확인: 거짓 실패(커밋됐는데 FAILED)는 DONE 정정 후 제외 (설계 4)
            committed = {
                b for b in {j["stat_desc"] for j in jobs
                            if j["status"] == "FAILED" and j["stat_desc"]}
                if snapshot_exists(tbl, b)
            }
            if committed:
                _mark_status([j["job_id"] for j in jobs if j["stat_desc"] in committed], "DONE")
                jobs = [j for j in jobs if j["stat_desc"] not in committed]

            # 크기 상한: 초과분 이월 (loop 회차 또는 다음날 회수, 설계 5.4)
            picked, total_mb = [], 0
            for j in jobs:
                if total_mb + j["file_size_mb"] > SIZE_LIMIT_MB:
                    break
                picked.append(j)
                total_mb += j["file_size_mb"]
            leftover = fetched_full or len(picked) < len(jobs)
            if not picked:
                return False

            batch_id = f"{run_id}_{tbl}"
            _mark_in_progress(picked, batch_id)  # 원자적 전환 + batch_id 기록

            # 마킹 직후 meta 기록 (이후 단계 실패 시 update_failure가 job_ids로 회수, 설계 5.3)
            ti.xcom_push(key="meta", value={
                "table": tbl,
                "group": table_group(table),
                "batch_id": batch_id,
                "job_ids": [j["job_id"] for j in picked],
                "leftover": leftover,
                "ts_min": picked[0]["ts"], "ts_max": picked[-1]["ts"],
                "num_executors": min(max(math.ceil(total_mb / 128 * 1.5 / 4), 1), MAX_EXECUTORS),
            })
            upload_path_list_to_s3(tbl, picked, batch_id)
            return True

        return get_jobs(run_cfg)

    def build_table_group(table, run_cfg):
        """기존 ConvertFileTaskGroup 재사용 — 조회 task만 재처리용으로 주입 (설계 5.2).

        TODO(연결): ConvertFileTaskGroup의 실제 생성자 시그니처에 맞출 것.
          - 조회 task 주입 방식(콜백/서브클래스 등)과 파라미터명
          - Spark task는 batch_id를 snapshot property로 커밋 + task retry 시
            자기 batch_id snapshot 확인 후 즉시 성공 종료 (중복 적재 방어, 설계 4.2)
          - update_success/update_failure는 get_jobs meta의 job_ids로 상태 처리
          - 성공 테이블 판별을 위해 group이 meta를 노출(pull 가능)해야 함 (_collect_metas)
        """
        return ConvertFileTaskGroup(  # noqa: F821  TODO(연결)
            group_id=f"reprocess_{table.get_name()}",
            get_jobs=build_get_jobs(table, run_cfg),
        )

    def _collect_metas(ti) -> list[dict]:
        """Spark 성공 테이블의 meta 수집.
        TODO(연결): task_ids는 ConvertFileTaskGroup이 성공 신호로 노출하는 XCom 경로에 맞출 것.
        (Airflow 3 worker는 메타데이터 DB 접근 불가 — dag_run.get_task_instance 사용 금지)
        """
        metas = []
        for t in ALL_TABLES:
            meta = ti.xcom_pull(task_ids=f"reprocess_{t.get_name()}.update_success")
            if meta:
                metas.append(meta)
        return metas

    @task(trigger_rule="all_done")
    def compaction_targets(ti=None) -> list[dict]:
        """적재 결과 집계 → TriggerDagRunOperator에 넘길 kwargs 목록 생성 (설계 6.3).
        적재분 전부 trigger — tables 필터로 비용 최소, 중복은 no-op.
        TODO(연결): conf의 날짜/시간 형식을 기존 Compaction DAG UI params와 일치시킬 것."""
        daily: dict[str, list[str]] = {}
        hourly_tables, hourly_min, hourly_max = [], None, None
        for m in _collect_metas(ti):
            if m["group"] == "daily":
                d = pendulum.from_format(m["ts_min"][:8], "YYYYMMDD", tz=KST)
                d_end = pendulum.from_format(m["ts_max"][:8], "YYYYMMDD", tz=KST)
                while d <= d_end:  # 수동 범위는 여러 날짜에 걸칠 수 있음 — 구간 전체
                    daily.setdefault(d.format("YYYYMMDD"), []).append(m["table"])
                    d = d.add(days=1)
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
        """재trigger 여부 판단 (설계 5.5) → TriggerDagRunOperator kwargs 0/1건.
        조건: 잔여분 존재 AND 해당 테이블 Spark 성공. 실패 테이블은 제외
        (같은 밤에 깨진 데이터 반복 재시도 안 함 — 다음날 회수)."""
        if not cfg or not [m for m in _collect_metas(ti) if m["leftover"]]:
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
    chain(run_cfg, *groups)  # 테이블 순차 실행

    # 적재분 Compaction / 잔여분 loop — 개수 가변이라 dynamic task mapping으로 trigger.
    # (TriggerDagRunOperator는 wait_for_completion 기본 False → 설계 6.3 대기 없음)
    comp = compaction_targets()
    nxt = next_loop(run_cfg)
    groups[-1] >> comp
    groups[-1] >> nxt
    TriggerDagRunOperator.partial(task_id="trigger_compaction").expand_kwargs(comp)
    TriggerDagRunOperator.partial(task_id="retrigger_self").expand_kwargs(nxt)


dag()
