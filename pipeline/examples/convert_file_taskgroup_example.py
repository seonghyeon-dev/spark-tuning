"""ConvertFileTaskGroup에 get_jobs_builder 옵션 인자를 추가하는 방법 예시.

⚠ 이 파일은 배포용이 아니라 **변경 방법을 보여주는 참고 예시**다.
   실제 ConvertFileTaskGroup 소스에 `# ★ 추가` 표시된 두 곳만 반영하면 된다.

변경점은 딱 두 곳:
  ① __init__ 인자에 get_jobs_builder=None 추가
  ② get_jobs 생성부를 if/else 분기로 감싸기

기존 코드(설정값·logger·_update_jobs·get_jobs 본문·Spark·update task)는
한 줄도 옮기지 않는다 — get_jobs 블록이 if 안으로 들여쓰기만 된다.
따라서 append DAG(인자 미지정)은 closure를 포함해 완전히 동일하게 동작한다.

재처리 쪽 사용법은 pipeline/dags/iceberg_reprocess.py의
build_reprocess_get_jobs 참조.
"""

import logging

from airflow.sdk import TaskGroup, task


class ConvertFileTaskGroup(TaskGroup):
    """avro → Iceberg append 공통 TaskGroup (get_jobs → spark → update_success/failure).

    아래 본문에서 `... 기존 코드 ...` 표시는 실제 구현이 그대로 있는 자리다.
    """

    def __init__(
        self,
        table,                    # 기존 그대로: 대상 테이블 Enum
        group_id: str,            # 기존 그대로
        # ... 기존에 받던 나머지 인자들 그대로 ...
        get_jobs_builder=None,    # ★ 추가 ①: 외부 조회 task 주입용 (기본 None = 기존 동작)
        **kwargs,
    ):
        super().__init__(group_id=group_id, **kwargs)

        # ── 기존 __init__ 지역값들: 위치 이동 없음 ─────────────────────────
        logger = logging.getLogger(__name__)
        config = ...              # 기존 설정값 로딩 그대로

        def _update_jobs(job_ids, status):
            """Job History 상태 update — 기존 지역 헬퍼 그대로 (이동 없음)."""
            ...                   # 기존 구현

        # ── ★ 추가 ②: get_jobs 생성부만 분기로 감싼다 ─────────────────────
        if get_jobs_builder is None:
            # [기존 경로 — append DAG]
            # 아래 블록은 기존 인라인 코드 그대로이며 if 안으로 들여쓰기만 된다.
            # logger/config/_update_jobs 등 closure 참조 전부 그대로 동작한다.
            @task(task_group=self)
            def get_jobs(ti=None):
                logger.info("get_jobs start: %s", table.get_name())
                ...               # 기존 조회/선점/XCom 로직 그대로
                _update_jobs(..., "IN_PROGRESS")   # 기존 closure 호출 그대로
                ...

            jobs = get_jobs()
        else:
            # [주입 경로 — 재처리 DAG]
            # builder가 이 그룹 안에 자기 조회 task를 만들어 반환한다.
            # 공용 헬퍼(_update_jobs)를 인자로 넘겨 재처리도 재사용할 수 있게 한다.
            jobs = get_jobs_builder(self, _update_jobs)

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
# 1) append DAG — 기존 호출 그대로 (인자 미지정 → if 분기 → 동작 완전 동일)
#
#     ConvertFileTaskGroup(table, group_id=f"convert_{table.get_name()}")
#
# 2) 재처리 DAG (iceberg_reprocess.py) — builder 주입 (else 분기)
#
#     ConvertFileTaskGroup(
#         table,
#         group_id=f"reprocess_{table.get_name()}",
#         get_jobs_builder=build_reprocess_get_jobs(table, run_cfg),
#     )
#
# builder 계약: builder(group, update_jobs) 형태로 호출되며, 그룹 안에
# 조회 task를 생성해 반환해야 한다. 반환값(jobs)은 그대로 spark의 상류가 된다.
