"""재처리(Reprocessing) DAG — 구현 스켈레톤.

설계 문서: pipeline/reprocessing-dag-design.md

역할: append DAG 조회 기간(최근 1일)에서 밀려난 WAIT 데이터와 FAILED 데이터를
      전날+그저께 범위에서 회수하고, 적재분에 대해 기존 Compaction DAG을 trigger한다.

구조: 단일 DAG, 테이블별 TaskGroup 순차 실행 (Compaction DAG과 동일 패턴).
      잔여분이 남으면 자기 자신을 재trigger (loop, 상한 10회).

기존 append DAG 인프라를 재사용해야 하는 지점은 전부 `TODO:`로 표시했다.
"""

import math

import pendulum
from airflow.models.param import Param
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

KST = pendulum.timezone("Asia/Seoul")

# TODO: append DAG과 동일한 테이블 설정 소스를 사용할 것.
#       group은 Compaction DAG 분류와 일치해야 한다 (첫 파티션 hour → hourly, day → daily).
TABLES = {
    # "TABLE_A": {"group": "hourly"},
    # "TABLE_B": {"group": "daily"},
}

ROW_LIMIT = 1000              # 테이블당 조회 상한 (설계 5.4 — 러프 설정, 재검증 필요)
SIZE_LIMIT_MB = 16 * 1024     # 테이블당 크기 상한 16GB (설계 5.4 — 러프 설정, 재검증 필요)
MAX_EXECUTORS = 24            # 벤치마크 검증 상한 (spark-tuning-guide.md 2.2.3)
MAX_LOOP = 10                 # 자기 재trigger 상한 (설계 5.5)
ZOMBIE_HOURS = 2              # 좀비 IN_PROGRESS 판정 임계 (설계 8.2)

DAILY_COMPACTION_DAG_ID = "daily_compaction_dag"    # TODO: 실제 DAG id
HOURLY_COMPACTION_DAG_ID = "hourly_compaction_dag"  # TODO: 실제 DAG id


def ts_str(dt: pendulum.DateTime) -> str:
    """pendulum datetime → Job History ts 형식 'YYYYMMDDHHmmSSsss' (밀리세컨즈 3자리)."""
    return dt.format("YYYYMMDDHHmmss") + "000"


# ---------------------------------------------------------------------------
# TODO: 아래 헬퍼들은 기존 append DAG의 구현을 재사용해서 연결한다.
# ---------------------------------------------------------------------------

def oracle_fetch(sql: str, **binds) -> list[dict]:
    """TODO: 기존 Oracle 커넥션/Hook 재사용."""
    raise NotImplementedError


def oracle_execute(sql: str, **binds) -> None:
    """TODO: 기존 Oracle 커넥션/Hook 재사용 (autocommit 또는 명시 commit)."""
    raise NotImplementedError


def snapshot_exists(table: str, batch_id: str) -> bool:
    """영수증 확인 (설계 4.2): 해당 테이블 snapshot summary에 batch_id 존재 여부.

    TODO: Trino/Spark 중 기존 조회 경로 재사용.
      SELECT snapshot_id FROM <catalog>.<db>.<table>.snapshots
       WHERE element_at(summary, 'batch_id') = :batch_id
    """
    raise NotImplementedError


def upload_path_list_to_s3(table: str, jobs: list[dict], batch_id: str) -> None:
    """TODO: 기존 get_jobs의 avro 경로 목록 텍스트 파일 S3 업로드 로직 재사용."""
    raise NotImplementedError


def send_alert(message: str, detail=None) -> None:
    """TODO: 기존 알림 채널 재사용."""
    raise NotImplementedError


def mark_status_by_job_ids(job_ids: list[str], status: str) -> None:
    """job_id 목록으로 상태 UPDATE. stat_desc(CLOB)는 WHERE 조건 사용 금지 (설계 4.2)."""
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
    dag_id="iceberg_reprocess",
    schedule="0 1 * * *",   # 01:00 KST — 전날 데이터 안정화 버퍼 (설계 5.1)
    start_date=pendulum.datetime(2026, 7, 1, tz=KST),
    catchup=False,
    max_active_runs=1,      # loop 회차 순차 실행 보장
    params={
        # UI 수동 실행: 1개/여러 개/전체 선택 (설계 5.1)
        "tables": Param(list(TABLES), type="array"),
        # 수동: 대상 날짜(YYYYMMDD). 그저께 이전만 허용 — task에서 검증
        "target_dt": Param(None, type=["null", "string"]),
        # 수동: ts 범위 축소 (YYYYMMDDHHmmSSsss)
        "start_time": Param(None, type=["null", "string"]),
        "end_time": Param(None, type=["null", "string"]),
    },
    tags=["iceberg", "reprocess"],
)
def iceberg_reprocess():

    @task
    def check_zombie_jobs():
        """임계 시간 초과 IN_PROGRESS 탐지 → 알림만, 자동 복구 안 함 (설계 8.2)."""
        zombies = oracle_fetch(
            """
            SELECT table_name, job_id, updated_at FROM JOB_HISTORY
             WHERE status = 'IN_PROGRESS'
               AND updated_at < SYSTIMESTAMP - INTERVAL :h HOUR
            """,
            h=ZOMBIE_HOURS,
        )
        if zombies:
            send_alert(
                f"좀비 IN_PROGRESS {len(zombies)}건 — 영수증 확인 후 수동 판정 필요 (설계 8.2)",
                zombies,
            )

    def build_table_group(tbl: str) -> TaskGroup:
        with TaskGroup(group_id=f"reprocess_{tbl}") as group:

            @task.short_circuit(
                task_id="get_table_jobs",
                trigger_rule=TriggerRule.ALL_DONE,      # 앞 테이블 실패에도 실행
                ignore_downstream_trigger_rules=False,  # skip을 그룹 내로 한정 (설계 5.2 — 필수)
            )
            def get_table_jobs(**context):
                params = context["params"]
                run_id = context["run_id"]
                conf = context["dag_run"].conf or {}

                if tbl not in params["tables"]:
                    return False  # 수동 실행에서 미선택 → skip

                # 경계 기준일: loop 재trigger가 자정을 넘겨도 흔들리지 않게 conf로 고정 (설계 5.5)
                base = (
                    pendulum.parse(conf["base_date"], tz=KST)
                    if conf.get("base_date")
                    else pendulum.now(KST).start_of("day")
                )

                if params.get("target_dt"):
                    # 수동: 지정 날짜 전체 (WAIT+FAILED). 그저께 이전만 허용 (설계 5.1)
                    d = pendulum.from_format(params["target_dt"], "YYYYMMDD", tz=KST)
                    if d >= base.subtract(days=2):
                        raise ValueError(
                            "target_dt는 그저께 이전 날짜만 허용 — 전날/당일은 append 조회 범위와 겹침"
                        )
                    d2_start, d1_end = ts_str(d), ts_str(d.add(days=1))
                    wait_bound = d1_end  # 그저께 이전은 append 범위 밖
                else:
                    # 정기: 그저께 00:00 ~ 전날 끝. WAIT는 전날 01:00 이전만 (설계 2.1)
                    d2_start = ts_str(base.subtract(days=2))
                    d1_end = ts_str(base)
                    wait_bound = ts_str(base.subtract(days=1).add(hours=1))

                s = params.get("start_time") or d2_start
                e = params.get("end_time") or d1_end

                jobs = oracle_fetch(
                    """
                    SELECT * FROM (
                        SELECT job_id, status, stat_desc, ts, avro_path, file_size_mb
                          FROM JOB_HISTORY
                         WHERE table_name = :tbl
                           AND ts >= :s AND ts < :e
                           AND ( status = 'FAILED'
                                 OR (status = 'WAIT' AND ts < :wait_bound) )
                         ORDER BY ts ASC
                    ) WHERE ROWNUM <= :row_limit
                    """,
                    tbl=tbl, s=s, e=e, wait_bound=wait_bound, row_limit=ROW_LIMIT,
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
                context["ti"].xcom_push(
                    key="meta",
                    value={
                        "table": tbl,
                        "group": TABLES[tbl]["group"],
                        "batch_id": batch_id,
                        "job_ids": [j["job_id"] for j in picked],
                        "leftover": leftover,
                        "ts_min": picked[0]["ts"],
                        "ts_max": picked[-1]["ts"],
                        # append DAG과 동일 산정식: ceil(총크기/128MB × 1.5 / executor-cores 4)
                        "num_executors": min(
                            max(math.ceil(total_mb / 128 * 1.5 / 4), 1), MAX_EXECUTORS
                        ),
                    },
                )
                upload_path_list_to_s3(tbl, picked, batch_id)
                return True

            # TODO: 기존 append DAG의 SparkKubernetesOperator 템플릿 재사용.
            #       Spark 코드에는 .option("snapshot-property.batch_id", batch_id) 적용 (설계 4.2)
            append_data = SparkKubernetesOperator(  # noqa: F821  TODO: import/템플릿 연결
                task_id="append_data",
                retries=2,                          # 일시적 오류 1차 방어 (설계 3.3)
                retry_delay=pendulum.duration(minutes=5),
                # application_file=..., num_executors는 XCom meta 참조
            )

            @task(task_id="update_success", trigger_rule=TriggerRule.ALL_SUCCESS)
            def update_success(**context):
                meta = context["ti"].xcom_pull(task_ids=f"reprocess_{tbl}.get_table_jobs", key="meta")
                mark_status_by_job_ids(meta["job_ids"], "DONE")

            @task(task_id="update_failure", trigger_rule=TriggerRule.ALL_FAILED)
            def update_failure(**context):
                meta = context["ti"].xcom_pull(task_ids=f"reprocess_{tbl}.get_table_jobs", key="meta")
                if meta:  # XCom 기록 전에 실패했으면 좀비 → check_zombie_jobs가 탐지 (설계 8.2)
                    mark_status_by_job_ids(meta["job_ids"], "FAILED")

            get_table_jobs() >> append_data >> [update_success(), update_failure()]
        return group

    def _collect_metas(context) -> list[dict]:
        """Spark가 성공한 테이블의 meta만 수집."""
        dag_run = context["dag_run"]
        metas = []
        for tbl in TABLES:
            ti = dag_run.get_task_instance(f"reprocess_{tbl}.append_data")
            if ti and ti.state == "success":
                meta = context["ti"].xcom_pull(
                    task_ids=f"reprocess_{tbl}.get_table_jobs", key="meta"
                )
                if meta:
                    metas.append(meta)
        return metas

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def trigger_compaction(**context):
        """적재 결과 집계 → 기존 Compaction DAG trigger (설계 6.3).

        조건 없이 적재분 전부 trigger — tables 필터로 비용 최소, 중복은 no-op.
        daily: 적재 날짜별로 테이블을 묶어 target_dt+tables 전달.
        hourly: 적재 ts 범위(min~max)를 테이블별로 모아 start/end+tables 전달.
        TODO: 날짜/시간 형식은 기존 Compaction DAG의 UI params 형식과 일치시킬 것.
        """
        metas = _collect_metas(context)

        daily_targets: dict[str, list[str]] = {}   # 날짜(YYYYMMDD) → 테이블 목록
        hourly_tables: list[str] = []
        hourly_min, hourly_max = None, None
        for m in metas:
            if m["group"] == "daily":
                for d in {m["ts_min"][:8], m["ts_max"][:8]}:
                    daily_targets.setdefault(d, []).append(m["table"])
            else:
                hourly_tables.append(m["table"])
                hourly_min = min(hourly_min or m["ts_min"], m["ts_min"])
                hourly_max = max(hourly_max or m["ts_max"], m["ts_max"])

        for target_dt, tables in daily_targets.items():
            trigger_dag_run(  # noqa: F821  TODO: TriggerDagRunOperator 또는 API 호출로 구현
                DAILY_COMPACTION_DAG_ID,
                conf={"target_dt": target_dt, "tables": tables},
            )
        if hourly_tables:
            trigger_dag_run(  # noqa: F821
                HOURLY_COMPACTION_DAG_ID,
                conf={"start_time": hourly_min, "end_time": hourly_max,
                      "tables": hourly_tables},
            )

    @task.short_circuit(
        trigger_rule=TriggerRule.ALL_DONE,
        ignore_downstream_trigger_rules=False,
    )
    def check_loop(**context):
        """재trigger 조건: 잔여분 존재 AND 해당 테이블 Spark 성공 (설계 5.5).

        실패 테이블은 제외 — 같은 밤에 깨진 데이터를 반복 재시도하지 않음 (다음날 회수).
        """
        pending = [m for m in _collect_metas(context) if m["leftover"]]
        loop_count = int((context["dag_run"].conf or {}).get("loop_count", 0))
        if not pending:
            return False
        if loop_count >= MAX_LOOP:
            send_alert(f"재처리 loop 상한({MAX_LOOP}회) 도달 — 수동 처리 필요 (설계 8.1)")
            return False
        return True

    retrigger = TriggerDagRunOperator(
        task_id="retrigger_self",
        trigger_dag_id="iceberg_reprocess",
        conf={
            "loop_count": "{{ (dag_run.conf.get('loop_count', 0) | int) + 1 }}",
            # 경계 기준일 고정 전달 — 회차가 자정을 넘겨도 ts 경계 유지 (설계 5.5)
            "base_date": "{{ dag_run.conf.get('base_date', ds) }}",
        },
    )

    # 테이블별 그룹 순차 연결 (앞 테이블 실패에도 다음 그룹은 all_done으로 계속)
    prev = check_zombie_jobs()
    for _tbl in TABLES:
        g = build_table_group(_tbl)
        prev >> g
        prev = g
    prev >> trigger_compaction() >> check_loop() >> retrigger


iceberg_reprocess()
