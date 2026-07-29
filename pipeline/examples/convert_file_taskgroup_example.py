"""ConvertFileTaskGroup에 재처리 분기를 추가하는 방법 예시.

⚠ 배포용이 아니라 **변경 방법을 보여주는 참고 예시**다.
   실제 소스에 `# ★ 추가` 표시된 곳만 반영하면 된다.

변경점은 두 곳:
  ① __init__ 인자에 reprocess_cfg=None 추가
  ② get_jobs 생성부를 if/else로 감싸고, else에 재처리 조회 task를 둔다

핵심은 **재처리 조회 task도 __init__ 안에 둔다**는 점이다. 조회 로직은 __init__
지역 함수·설정값(_update_jobs, logger, config …)을 써야 하는데, task를 부모 밖으로
빼면(상속 override / builder 주입 / 헬퍼 파라미터 전달) 그것들을 일일이 넘겨야
하고 헬퍼가 늘 때마다 시그니처가 깨진다. 같은 스코프에 두면 그냥 호출하면 되고,
밖에서 넘길 것은 조회 범위(reprocess_cfg) 하나뿐이다.

기존 append 경로는 코드가 if 안으로 들여쓰기만 되며 closure 포함 동작이 완전히
동일하다 (reprocess_cfg 미지정 → if 분기).

재처리 조회 본문은 pipeline/dags/iceberg_reprocess.py의 reprocess_get_jobs 참조.
"""

import logging

from airflow.sdk import TaskGroup, task

# 재처리 DAG이 제공하는 순수 조회 로직 (스코프와 무관한 부분만 분리되어 있음)
# from <재처리 모듈>.iceberg_reprocess import reprocess_get_jobs


class ConvertFileTaskGroup(TaskGroup):
    """avro → Iceberg append 공통 TaskGroup (get_jobs → spark → update_success/failure).

    본문의 `... 기존 코드 ...`는 실제 구현이 그대로 있는 자리다.
    """

    def __init__(
        self,
        table,                    # 기존 그대로: 대상 테이블 Enum
        group_id: str,            # 기존 그대로
        # ... 기존에 받던 나머지 인자들 그대로 ...
        reprocess_cfg=None,       # ★ 추가 ①: 재처리 조회 범위(prepare_run XCom).
        **kwargs,                 #            미지정이면 기존 append 동작 그대로
    ):
        super().__init__(group_id=group_id, **kwargs)

        # ── 기존 __init__ 지역값들: 위치 이동 없음 ─────────────────────────
        logger = logging.getLogger(__name__)
        config = ...              # 기존 설정값 로딩 그대로

        def _update_jobs(conn_id, keys, status, batch_id=None):
            """Job History 상태 update — **기존 지역 헬퍼 그대로**.
               재처리도 이 함수를 쓴다 (자체 UPDATE 구현 없음)."""
            ...                   # 기존 구현

        def _upload_file_list(files):
            """param에서 뽑은 파일 목록([{file_name, size}, ...])을 받아
               ① avro 경로 텍스트 파일을 S3에 저장하고
               ② size 총합으로 executor 개수를 산정해 **반환**한다
               → **기존 함수 그대로**. 재처리도 이 함수를 그대로 쓴다."""
            ...                   # 기존 구현
            return num_executors  # noqa: F821

        def _other_helper(*args):  # 다른 지역 함수들도 그대로
            ...

        # ── ★ 추가 ②: get_jobs 생성부를 분기로 감싼다 ─────────────────────
        if reprocess_cfg is None:
            # [기존 경로 — append DAG] 인라인 코드 그대로, if 안으로 들여쓰기만.
            # logger/config/_update_jobs 등 closure 참조 전부 그대로 동작한다.
            @task(task_group=self)
            def get_jobs(ti=None):
                logger.info("get_jobs start: %s", table.get_name())
                ...               # 기존 조회/선점/XCom 로직 그대로
                _update_jobs(..., "IN_PROGRESS")
                ...

            jobs = get_jobs()
        else:
            # [재처리 경로] 같은 스코프이므로 지역 함수를 그냥 쓴다 —
            # 넘겨받은 건 조회 범위(reprocess_cfg) 하나뿐이다.
            @task.short_circuit(
                task_group=self,
                task_id="get_jobs",
                trigger_rule="all_done",                # 앞 테이블 실패에도 실행
                ignore_downstream_trigger_rules=False,  # skip을 그룹 내로 한정
            )
            def get_jobs(cfg, run_id=None, ti=None):
                logger.info("reprocess get_jobs: %s", table.get_name())
                # 재처리 모듈이 하는 일은 "무엇을 적재할지" 정하는 것뿐이다 —
                # 조회 범위(전날+그저께, WAIT 상한)와 영수증 확인(중복 적재 방지).
                # S3 업로드·executor 산정·상태 UPDATE는 아래 기존 함수들이 한다.
                files, marks = reprocess_get_jobs(  # noqa: F821
                    cfg, table=table, run_id=run_id, ti=ti,
                )

                # 영수증으로 커밋이 확인된 건 → 재적재하지 않고 DONE 정정.
                # batch_id를 넘기지 않아 row의 기존 stat_desc(영수증)가 유지된다
                for conn_id, keys in marks["done_keys"].items():
                    _update_jobs(conn_id, keys, "DONE")

                if not files:
                    return False              # 대상 없음 → 그룹 내 하류 skip

                # append 경로와 동일: 파일 목록 → S3 텍스트 파일 + executor 산정.
                # 반환된 개수를 XCom에 올려야 Spark operator가 pull해서 쓴다
                ti.xcom_push(key="num_executors", value=_upload_file_list(files))

                # 마킹은 XCom push 뒤에 (설계 5.3) — 마킹 도중 실패해도
                # update_failure가 XCom으로 대상을 되찾을 수 있다
                ti.xcom_push(key="meta", value=marks)   # batch_id + conn별 복합키
                for conn_id, keys in marks["keys"].items():
                    _update_jobs(conn_id, keys, "IN_PROGRESS", marks["batch_id"])
                return True

            jobs = get_jobs(reprocess_cfg)

        # ── 이하 기존 코드 그대로 (Spark / update task / 연결) ─────────────
        spark = ...               # 기존 SparkKubernetesOperator 확장 operator 그대로
                                  # (task_id="append_data", task_group=self, ...)

        @task(task_id="update_success", task_group=self, trigger_rule="all_success")
        def update_success(ti=None):
            ...                   # 기존 구현 (meta의 job_ids로 DONE 처리)

        @task(task_id="update_failure", task_group=self, trigger_rule="all_failed")
        def update_failure(ti=None):
            ...                   # 기존 구현 (meta의 job_ids로 FAILED 처리)

        jobs >> spark >> [update_success(), update_failure()]


# ── 사용 예 ──────────────────────────────────────────────────────────────
#
# 1) append DAG — 기존 호출 그대로 (reprocess_cfg 미지정 → 기존 경로, 동작 동일)
#
#     ConvertFileTaskGroup(table, group_id=f"convert_{table.get_name()}")
#
# 2) 재처리 DAG (iceberg_reprocess.py) — 조회 범위만 전달
#
#     ConvertFileTaskGroup(
#         table,
#         group_id=f"reprocess_{table.get_name()}",
#         reprocess_cfg=run_cfg,      # prepare_run의 XCom (ts 경계·tables·loop_count)
#     )
