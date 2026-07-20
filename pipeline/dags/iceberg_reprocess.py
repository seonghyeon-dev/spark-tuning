"""재처리(Reprocessing) DAG — 구현 스켈레톤.

설계 문서: pipeline/reprocessing-dag-design.md

역할: append DAG 조회 기간(최근 1일)에서 밀려난 WAIT 데이터와 FAILED 데이터를
      전날+그저께 범위에서 회수하고, 적재분에 대해 기존 Compaction DAG을 trigger한다.

구조: 단일 DAG, 테이블별 TaskGroup 순차 실행 (Compaction DAG과 동일 패턴).
      params는 prepare_run task에서 1회 검증·정규화 후 XCom으로만 소비한다.
      잔여분이 남으면 자기 자신을 재trigger (loop, 상한 10회).

── 기존 구현 연결 지점 ─────────────────────────────────────────────
이미 구현되어 있는 것들이므로 연결만 하면 된다. 코드 내 `TODO(연결):` 태그와
1:1로 대응한다 (grep "TODO(연결)" 으로 전체 확인 가능).

  1. iceberg.py의 hourly/daily Enum import (자리표시자 클래스 2개 교체)
  2. Compaction DAG id 상수 2건
  3. Oracle 조회/실행     — oracle_fetch / oracle_execute
  4. 영수증 snapshot 조회 — snapshot_exists
  5. avro 경로 목록 S3 업로드 — upload_path_list_to_s3
  6. 알림 채널            — send_alert
  7. Spark 실행           — append_data의 SparkKubernetesOperator 템플릿
  8. Compaction trigger   — trigger_dag_run 호출 + conf 날짜/시간 형식 확인
  9. TaskGroup 템플릿     — 기존 ConvertFileTaskGroup 확장/재사용 (build_table_group)
────────────────────────────────────────────────────────────────────
"""

import math
from enum import Enum
from pathlib import Path

import pendulum
from airflow.models.param import Param
from airflow.sdk import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

KST = pendulum.timezone("Asia/Seoul")

DAG_ID = Path(__file__).stem  # 조직 컨벤션: dag_id는 파일명에서 파생 (단일 소스)


# TODO(연결): append DAG이 사용하는 iceberg.py의 hourly/daily Enum 클래스를 그대로 import해서
#       아래 자리표시자를 제거 (클래스명은 iceberg.py의 실제 정의에 맞출 것 — 단일 소스).
# from <공통 모듈>.iceberg import HourlyIcebergTable, DailyIcebergTable
class HourlyIcebergTable(str, Enum):
    """자리표시자 — 첫 파티션이 hour인 테이블 그룹 (실제 iceberg.py 정의로 교체)."""

    # TABLE_A = "table_a"

    def get_name(self) -> str:
        """실제 테이블명 반환. value는 alias이므로 테이블명은 반드시 get_name() 사용."""
        return self.value  # 자리표시자 구현 — 실제 Enum의 get_name()으로 대체됨


class DailyIcebergTable(str, Enum):
    """자리표시자 — 첫 파티션이 day인 테이블 그룹 (실제 iceberg.py 정의로 교체)."""

    # TABLE_B = "table_b"

    def get_name(self) -> str:
        """실제 테이블명 반환. value는 alias이므로 테이블명은 반드시 get_name() 사용."""
        return self.value  # 자리표시자 구현 — 실제 Enum의 get_name()으로 대체됨


# append DAG과 동일한 순회 방식. hourly/daily 분류는 소속 Enum 클래스로 결정된다.
ALL_TABLES = [*HourlyIcebergTable, *DailyIcebergTable]


def table_group(table) -> str:
    """테이블의 Compaction 그룹: 소속 Enum 클래스가 곧 분류다."""
    return "hourly" if isinstance(table, HourlyIcebergTable) else "daily"


ROW_LIMIT = 1000              # 테이블당 조회 상한 (설계 5.4 — 러프 설정, 재검증 필요)
SIZE_LIMIT_MB = 16 * 1024     # 테이블당 크기 상한 16GB (설계 5.4 — 러프 설정, 재검증 필요)
MAX_EXECUTORS = 24            # 벤치마크 검증 상한 (spark-tuning-guide.md 2.2.3)
MAX_LOOP = 10                 # 자기 재trigger 상한 (설계 5.5)
ZOMBIE_HOURS = 2              # 좀비 IN_PROGRESS 판정 임계 (설계 8.2)

DAILY_COMPACTION_DAG_ID = "daily_compaction_dag"    # TODO(연결): 실제 DAG id
HOURLY_COMPACTION_DAG_ID = "hourly_compaction_dag"  # TODO(연결): 실제 DAG id


def ts_str(dt: pendulum.DateTime) -> str:
    """pendulum datetime → Job History ts 형식 'YYYYMMDDHHmmSSsss' (밀리세컨즈 3자리)."""
    return dt.format("YYYYMMDDHHmmss") + "000"


# ---------------------------------------------------------------------------
# TODO(연결): 아래 헬퍼들은 기존 append DAG의 구현을 재사용해서 연결한다.
# ---------------------------------------------------------------------------

def oracle_fetch(sql: str, **binds) -> list[dict]:
    """TODO(연결): 기존 Oracle 커넥션/Hook 재사용."""
    raise NotImplementedError


def oracle_execute(sql: str, **binds) -> None:
    """TODO(연결): 기존 Oracle 커넥션/Hook 재사용 (autocommit 또는 명시 commit)."""
    raise NotImplementedError


def snapshot_exists(table: str, batch_id: str) -> bool:
    """영수증 확인 (설계 4.2): 해당 테이블 snapshot summary에 batch_id 존재 여부.

    TODO(연결): Trino/Spark 중 기존 조회 경로 재사용.
      SELECT snapshot_id FROM <catalog>.<db>.<table>.snapshots
       WHERE element_at(summary, 'batch_id') = :batch_id
    """
    raise NotImplementedError


def upload_path_list_to_s3(table: str, jobs: list[dict], batch_id: str) -> None:
    """TODO(연결): 기존 get_jobs의 avro 경로 목록 텍스트 파일 S3 업로드 로직 재사용."""
    raise NotImplementedError


def send_alert(message: str, detail=None) -> None:
    """TODO(연결): 기존 알림 채널 재사용."""
    raise NotImplementedError


def mark_status_by_job_ids(job_ids: list[str], status: str) -> None:
    """job_id 목록으로 상태 UPDATE. stat_desc(CLOB)는 WHERE 조건 사용 금지 (설계 4.2).

    TODO(연결): Oracle은 목록을 IN에 직접 바인드할 수 없다 — 기존 Hook의
    목록 확장 방식(IN (:1,:2,...) 동적 생성 등)을 그대로 사용할 것.
    """
    oracle_execute(
        """
        UPDATE JOB_HISTORY SET status = :status
         WHERE job_id IN :ids AND status = 'IN_PROGRESS'
        """,
        status=status, ids=job_ids,
    )


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
        # UI 수동 실행: 1개/여러 개/전체 multi-select (설계 5.1)
        # 선택지·기본값 모두 iceberg.py Enum에서 생성 — 하드코딩 목록 없음
        "tables": Param(
            default=[t.get_name() for t in ALL_TABLES],
            type="array",
            items={"type": "string", "enum": [t.get_name() for t in ALL_TABLES]},
        ),
        # 수동 실행: 조회 범위를 직접 정의 (둘 다 함께 지정, end_time ≤ 전날 00:00 —
        # prepare_run에서 검증). 미지정 시 정기 범위(그저께 00:00 ~ 전날 끝).
        # 기존 DAG과 동일한 date-time 형식 → prepare_run에서 ts 문자열로 변환
        "start_time": Param(None, type=["null", "string"], format="date-time"),
        "end_time": Param(None, type=["null", "string"], format="date-time"),
    },
    tags=["iceberg", "reprocess"],
)
def dag():  # 조직 컨벤션: 함수명 dag() 고정 — DAG 정체성은 파일명(dag_id)이 담당

    @task
    def check_zombie_jobs():
        """임계 시간 초과 IN_PROGRESS 탐지 → 알림만, 자동 복구 안 함 (설계 8.2)."""
        zombies = oracle_fetch(
            """
            SELECT table_name, job_id, updated_at FROM JOB_HISTORY
             WHERE status = 'IN_PROGRESS'
               AND updated_at < SYSTIMESTAMP - NUMTODSINTERVAL(:h, 'HOUR')
            """,
            h=ZOMBIE_HOURS,
        )
        if zombies:
            send_alert(
                f"좀비 IN_PROGRESS {len(zombies)}건 — 영수증 확인 후 수동 판정 필요 (설계 8.2)",
                zombies,
            )

    @task
    def prepare_run(params=None, dag_run=None) -> dict:
        """params 검증·정규화 (1회) — 이후 task들은 params를 직접 읽지 않는다.

        기존 append DAG의 get_time 패턴과 동일. 형식 오류·잘못된 범위는 여기서
        즉시 실패하고, 하류 task들은 정규화된 XCom 값만 소비한다.
        loop 재trigger 회차는 첫 회차가 확정한 값을 conf로 승계받는다 (설계 5.5).
        """
        conf = dag_run.conf or {}

        # loop 재trigger 회차: 첫 회차가 정규화한 조회 범위·tables를 그대로 승계.
        # 수동 실행의 선택 값이 회차에서 유실되지 않고, 회차가 자정을 넘겨도 경계가 안 흔들림
        if conf.get("ts_from"):
            return {
                "tables": conf["tables"],
                "ts_from": conf["ts_from"],
                "ts_to": conf["ts_to"],
                "wait_bound": conf["wait_bound"],
                "loop_count": int(conf.get("loop_count", 0)),
            }

        base = pendulum.now(KST).start_of("day")
        start_time, end_time = params.get("start_time"), params.get("end_time")

        if start_time or end_time:
            # 수동 실행: start_time/end_time이 조회 범위를 직접 정의한다 (설계 5.1)
            # in_timezone(KST): UI가 offset 포함 문자열을 보내도 KST 기준으로 통일
            if not (start_time and end_time):
                raise ValueError("start_time과 end_time은 함께 지정해야 한다")
            ts_from = ts_str(pendulum.parse(start_time, tz=KST).in_timezone(KST))
            ts_to = ts_str(pendulum.parse(end_time, tz=KST).in_timezone(KST))
            if ts_to <= ts_from:
                raise ValueError("end_time은 start_time 이후여야 한다")
            # 전날 00:00 이후는 append 조회 범위(최근 24시간)와 겹칠 수 있어 거부
            if ts_to > ts_str(base.subtract(days=1)):
                raise ValueError("end_time은 전날 00:00 이전만 허용 — append 조회 범위와 겹침")
            wait_bound = ts_to  # 범위 전체가 append 범위 밖 → WAIT 전 구간 허용
        else:
            # 정기: 그저께 00:00 ~ 전날 끝. WAIT는 전날 01:00 이전만 (설계 2.1)
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

    # TODO(연결): 기존 append DAG의 ConvertFileTaskGroup 템플릿 확장/재사용.
    #   구조(조회 → Spark → update_success/update_failure)가 동일하므로 조회 task만
    #   재처리용(get_table_jobs)으로 교체 가능하게 템플릿을 파라미터화하면 재사용 가능.
    #   단, 그룹 첫 task에 trigger_rule=all_done + short_circuit 옵션 노출 필요 (설계 5.2).
    def build_table_group(table, run_cfg) -> TaskGroup:
        tbl = table.get_name()               # 테이블명은 get_name() 사용 (value는 alias)
        with TaskGroup(group_id=f"reprocess_{tbl}") as group:

            @task.short_circuit(
                task_id="get_table_jobs",
                trigger_rule=TriggerRule.ALL_DONE,      # 앞 테이블 실패에도 실행
                ignore_downstream_trigger_rules=False,  # skip을 그룹 내로 한정 (설계 5.2 — 필수)
            )
            def get_table_jobs(cfg: dict, run_id=None, ti=None):
                if not cfg:
                    raise ValueError("prepare_run 결과 없음 — 선행 task 실패")
                if tbl not in cfg["tables"]:
                    return False  # 수동 실행에서 미선택 → skip

                jobs = oracle_fetch(
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
                    tbl=tbl, ts_from=cfg["ts_from"], ts_to=cfg["ts_to"],
                    wait_bound=cfg["wait_bound"], row_limit=ROW_LIMIT,
                )
                # 잔여분 판정은 필터 적용 전 조회 건수 기준 (설계 5.3)
                fetched_full = len(jobs) >= ROW_LIMIT

                # 영수증 확인: 거짓 실패(커밋됐는데 FAILED) 건은 DONE 정정 후 제외 (설계 4)
                failed_batch_ids = {
                    j["stat_desc"] for j in jobs
                    if j["status"] == "FAILED" and j["stat_desc"]
                }
                committed = {b for b in failed_batch_ids if snapshot_exists(tbl, b)}
                if committed:
                    done_ids = [j["job_id"] for j in jobs if j["stat_desc"] in committed]
                    mark_status_by_job_ids(done_ids, "DONE")
                    jobs = [j for j in jobs if j["stat_desc"] not in committed]

                # 크기 상한: 초과분은 이월. 잘린 것도 잔여분 (설계 5.4)
                picked, total_mb = [], 0
                for j in jobs:
                    if total_mb + j["file_size_mb"] > SIZE_LIMIT_MB:
                        break
                    picked.append(j)
                    total_mb += j["file_size_mb"]
                leftover = fetched_full or len(picked) < len(jobs)
                if not picked:
                    return False

                # 원자적 IN_PROGRESS 전환 + batch_id 기록 (설계 5.3)
                batch_id = f"{run_id}_{tbl}"
                oracle_execute(
                    """
                    UPDATE JOB_HISTORY
                       SET status = 'IN_PROGRESS', stat_desc = :batch_id
                     WHERE job_id IN :ids AND status IN ('WAIT', 'FAILED')
                    """,
                    batch_id=batch_id, ids=[j["job_id"] for j in picked],
                )

                # 마킹 직후 XCom 먼저 기록 — 이후 단계 실패 시에도 update_failure가
                # job_ids로 회수 가능 (설계 5.3)
                ti.xcom_push(
                    key="meta",
                    value={
                        "table": tbl,                  # XCom은 JSON 직렬화 — 문자열 값 저장
                        "group": table_group(table),
                        "batch_id": batch_id,
                        "job_ids": [j["job_id"] for j in picked],  # 상태 update용 (stat_desc 조건 금지)
                        "leftover": leftover,                      # loop 판단용
                        "ts_min": picked[0]["ts"],                 # Compaction 범위
                        "ts_max": picked[-1]["ts"],
                        # append DAG과 동일 산정식: ceil(총크기/128MB × 1.5 / executor-cores 4)
                        "num_executors": min(
                            max(math.ceil(total_mb / 128 * 1.5 / 4), 1), MAX_EXECUTORS
                        ),
                    },
                )
                upload_path_list_to_s3(tbl, picked, batch_id)
                return True

            # TODO(연결): 기존 append DAG의 SparkKubernetesOperator 템플릿 재사용.
            #       Spark 코드에는 .option("snapshot-property.batch_id", batch_id) 적용 (설계 4.2)
            #       권장: Spark job 시작 시 자기 batch_id의 snapshot 존재 확인 → 있으면 즉시 성공 종료
            #       (task retry가 거짓 실패를 같은 batch_id로 재실행할 때의 중복 적재 방어, 설계 4.2)
            append_data = SparkKubernetesOperator(  # noqa: F821  TODO(연결): import/템플릿 연결
                task_id="append_data",
                retries=2,                          # 일시적 오류 1차 방어 (설계 3.3)
                retry_delay=pendulum.duration(minutes=5),
                # application_file=..., num_executors는 XCom meta 참조
            )

            @task(task_id="update_success", trigger_rule=TriggerRule.ALL_SUCCESS)
            def update_success(ti=None):
                meta = ti.xcom_pull(task_ids=f"reprocess_{tbl}.get_table_jobs", key="meta")
                mark_status_by_job_ids(meta["job_ids"], "DONE")
                # Spark 성공 테이블의 meta를 후속 집계(Compaction/loop)용으로 반환.
                # 이 task 자체가 Spark 성공 시에만 실행되므로, 집계 task는 task 상태를
                # DB 조회할 필요 없이 이 반환값의 존재 여부만 보면 된다 (Airflow 3
                # worker에서는 메타데이터 DB 접근 불가 — dag_run.get_task_instance 사용 금지)
                return meta

            @task(task_id="update_failure", trigger_rule=TriggerRule.ALL_FAILED)
            def update_failure(ti=None):
                meta = ti.xcom_pull(task_ids=f"reprocess_{tbl}.get_table_jobs", key="meta")
                if meta:  # XCom 기록 전에 실패했으면 좀비 → check_zombie_jobs가 탐지 (설계 8.2)
                    mark_status_by_job_ids(meta["job_ids"], "FAILED")

            get_table_jobs(run_cfg) >> append_data >> [update_success(), update_failure()]
        return group

    def _collect_metas(ti) -> list[dict]:
        """Spark가 성공한 테이블의 meta만 수집.

        update_success는 해당 테이블 Spark가 성공했을 때만 실행되는 task이므로,
        그 반환값(XCom)의 존재 = Spark 성공. task 상태 DB 조회가 필요 없다.
        """
        metas = []
        for t in ALL_TABLES:
            meta = ti.xcom_pull(task_ids=f"reprocess_{t.get_name()}.update_success")
            if meta:
                metas.append(meta)
        return metas

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def trigger_compaction(ti=None):
        """적재 결과 집계 → 기존 Compaction DAG trigger (설계 6.3).

        조건 없이 적재분 전부 trigger — tables 필터로 비용 최소, 중복은 no-op.
        daily: 적재 날짜별로 테이블을 묶어 target_dt+tables 전달.
        hourly: 적재 ts 범위(min~max)를 모아 start/end+tables 전달.
        TODO(연결): 날짜/시간 형식은 기존 Compaction DAG의 UI params 형식과 일치시킬 것.
        """
        metas = _collect_metas(ti)

        daily_targets: dict[str, list[str]] = {}   # 날짜(YYYYMMDD) → 테이블 목록
        hourly_tables: list[str] = []
        hourly_min, hourly_max = None, None
        for m in metas:
            if m["group"] == "daily":
                # 수동 실행 범위는 여러 날짜에 걸칠 수 있음 — 구간 내 날짜 전부 포함
                d = pendulum.from_format(m["ts_min"][:8], "YYYYMMDD", tz=KST)
                d_end = pendulum.from_format(m["ts_max"][:8], "YYYYMMDD", tz=KST)
                while d <= d_end:
                    daily_targets.setdefault(d.format("YYYYMMDD"), []).append(m["table"])
                    d = d.add(days=1)
            else:
                hourly_tables.append(m["table"])
                hourly_min = min(hourly_min or m["ts_min"], m["ts_min"])
                hourly_max = max(hourly_max or m["ts_max"], m["ts_max"])

        for target_dt, tables in daily_targets.items():
            trigger_dag_run(  # noqa: F821  TODO(연결): TriggerDagRunOperator 또는 API 호출로 구현
                DAILY_COMPACTION_DAG_ID,
                conf={"target_dt": target_dt, "tables": tables},
            )
        if hourly_tables:
            trigger_dag_run(  # noqa: F821  TODO(연결)
                HOURLY_COMPACTION_DAG_ID,
                conf={"start_time": hourly_min, "end_time": hourly_max,
                      "tables": hourly_tables},
            )

    @task.short_circuit(
        trigger_rule=TriggerRule.ALL_DONE,
        ignore_downstream_trigger_rules=False,
    )
    def check_loop(cfg: dict, ti=None):
        """재trigger 조건: 잔여분 존재 AND 해당 테이블 Spark 성공 (설계 5.5).

        실패 테이블은 제외 — 같은 밤에 깨진 데이터를 반복 재시도하지 않음 (다음날 회수).
        """
        if not cfg:
            return False  # prepare_run 실패 → loop 없이 종료
        pending = [m for m in _collect_metas(ti) if m["leftover"]]
        if not pending:
            return False
        if cfg["loop_count"] >= MAX_LOOP:
            send_alert(f"재처리 loop 상한({MAX_LOOP}회) 도달 — 수동 처리 필요 (설계 8.1)")
            return False
        return True

    @task
    def retrigger_self(cfg: dict):
        """자기 자신 재trigger (설계 5.5) — 첫 회차가 확정한 조회 범위·tables를
        conf로 승계해서, 수동 선택 값 유실과 자정 넘김에 의한 경계 변동을 막는다."""
        trigger_dag_run(  # noqa: F821  TODO(연결): Compaction trigger와 동일 헬퍼 사용
            DAG_ID,
            conf={**cfg, "loop_count": cfg["loop_count"] + 1},
        )

    # 흐름: 좀비 점검 → params 정규화 → 테이블별 그룹 순차 → Compaction → loop 판단
    # (앞 테이블 실패에도 다음 그룹은 all_done으로 계속)
    run_cfg = prepare_run()
    check_zombie_jobs() >> run_cfg

    prev = run_cfg
    for _table in ALL_TABLES:
        g = build_table_group(_table, run_cfg)
        prev >> g
        prev = g

    prev >> trigger_compaction() >> check_loop(run_cfg) >> retrigger_self(run_cfg)


dag()
