"""ConvertFileTaskGroup에 get_jobs_builder 옵션 인자를 추가하는 방법 예시.

⚠ 이 파일은 배포용이 아니라 **변경 방법을 보여주는 참고 예시**다.
   실제 ConvertFileTaskGroup 소스에 `# ★ 추가` 표시된 세 곳만 반영하면 된다.

변경점은 딱 세 줄:
  ① __init__ 인자에 get_jobs_builder=None 추가
  ② __init__ 지역 함수/설정값을 self.ctx로 노출 (한 줄)
  ③ get_jobs 생성부를 if/else 분기로 감싸기

②가 핵심이다. builder가 부모의 지역 함수(_update_jobs 등)를 쓰려면 접근 경로가
필요한데, 헬퍼를 파라미터로 하나씩 넘기면 헬퍼가 늘 때마다 builder 시그니처가
깨진다. self.ctx에 묶어 노출하면 builder는 group.ctx.update_jobs(...) 로 골라
쓰고, 헬퍼가 추가돼도 ②의 한 줄만 늘어난다.

기존 코드(설정값·logger·_update_jobs·get_jobs 본문·Spark·update task)는
한 줄도 옮기지 않는다 — get_jobs 블록이 if 안으로 들여쓰기만 된다.
따라서 append DAG(인자 미지정)은 closure를 포함해 완전히 동일하게 동작한다.

재처리 쪽 사용법은 pipeline/dags/iceberg_reprocess.py의
build_reprocess_get_jobs 참조.
"""

import logging
from types import SimpleNamespace

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

        def _other_helper(*args):   # 다른 지역 함수도 그대로
            ...

        # ── ★ 추가 ②: 지역 함수/설정값을 self.ctx로 노출 (한 줄) ───────────
        # 기존 인라인 get_jobs는 여전히 closure로 직접 쓰므로 영향 없다.
        # 헬퍼가 늘면 여기에 항목만 추가하면 되고 builder 시그니처는 그대로다.
        self.ctx = SimpleNamespace(
            update_jobs=_update_jobs,
            other=_other_helper,
            logger=logger,
            config=config,
        )

        # ── ★ 추가 ③: get_jobs 생성부만 분기로 감싼다 ─────────────────────
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
            # 인자는 그룹 하나뿐 — 부모 헬퍼는 builder가 group.ctx로 접근한다.
            jobs = get_jobs_builder(self)

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
# builder 계약: builder(group) 형태로 호출되며, 그룹 안에 조회 task를 생성해
# 반환해야 한다. 반환값(jobs)은 그대로 spark의 상류가 된다.
# 부모의 지역 함수·설정은 group.ctx로 접근한다:
#
#     def builder(group):
#         @task.short_circuit(task_group=group, task_id="get_jobs", ...)
#         def get_jobs(cfg, run_id=None, ti=None):
#             group.ctx.logger.info(...)          # 부모 logger 사용
#             ...
#             group.ctx.update_jobs(ids, "IN_PROGRESS")   # 부모 헬퍼 호출
#         return get_jobs(run_cfg)
