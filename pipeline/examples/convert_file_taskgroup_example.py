"""ConvertFileTaskGroup에 재처리 분기를 추가하는 방법 — **기존 파일 변경 예시**.

⚠ 배포용 파일이 아니다. 여기 있는 내용을 **기존 ConvertFileTaskGroup 파일에**
   반영하면 된다. 새로 만드는 파일은 DAG(`pipeline/dags/iceberg_reprocess.py`)
   하나뿐이다.

왜 조회 로직까지 이 파일에 두는가
  재처리 조회 task는 `__init__` 안에서 만들어야 한다. 조회 뒤 처리(상태 UPDATE,
  파일 목록 → S3 + executor 산정)가 전부 `__init__` 지역 함수라, task를 밖으로
  빼면 그것들을 일일이 넘겨야 하고 헬퍼가 늘 때마다 시그니처가 깨진다.
  조회 로직을 DAG 파일에 두는 것도 안 된다 — 공통 모듈인 이 파일이 DAG 파일을
  import해야 해서 성립하지 않는다.

변경점은 세 곳
  ① 모듈 상단에 재처리 조회 SQL·함수 추가 (아래 A 블록)
  ② __init__ 인자에 reprocess_cfg=None 추가
  ③ get_jobs 생성부를 if/else로 감싸고, else에 재처리 조회 task를 둔다

기존 append 경로는 코드가 if 안으로 들여쓰기만 되며 closure 포함 동작이 완전히
동일하다 (reprocess_cfg 미지정 → if 분기).
"""

import json
import logging
from collections import namedtuple

from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.sdk import TaskGroup, task

# 기존 파일에 이미 있는 것 (재사용)
#   ORACLE_CONN_IDS = conn_list      # Job History가 있는 Oracle DB 2개


# ══════════════════════════════════════════════════════════════════════════
# ★ 추가 ① — 재처리 조회 (모듈 상단, 클래스 밖)
#   부모 지역 함수와 무관한 순수 조회 로직이라 클래스 밖에 둔다.
# ══════════════════════════════════════════════════════════════════════════

ROW_LIMIT = 1000     # 테이블당·DB당 조회 상한 — 한 회차 물량은 이것만으로 통제한다
                     # (설계 5.4 — 러프 설정, 운영 데이터로 재검증 필요)

# 조회 결과 1건. `get_records`는 tuple을 돌려주므로 이 이름표를 씌워서 쓴다.
# **필드 순서는 아래 SELECT 순서와 반드시 같아야 한다.** 어긋나면 Job(*row)가
# 개수 불일치로 즉시 실패하므로, 값이 밀린 채 조용히 흘러가지 않는다.
# 복합키를 앞에 몰아두어 job[:PK_LEN]이 곧 상태 UPDATE에 넘길 키가 된다.
# TODO(연결): 실제 복합키 컬럼명으로 교체 — 아래 SQL도 함께 고칠 것
Job = namedtuple("Job", "k_1 k_2 k_3 ts base_path param stat_desc")
PK_LEN = 4           # Job 앞 4개가 복합키

# status는 조회 조건으로만 쓰고 결과로는 받지 않는다 — 영수증 확인은 status가 아니라
# batch_id로 판단하므로(설계 4.2), 받아두면 잘못된 필터가 다시 생길 여지만 남는다.
# stat_desc(CLOB)는 그냥 조회하면 LOB 객체로 와 문자열 비교·set 연산이 안 되므로
# VARCHAR2로 변환해 받는다 (batch_id는 짧아 4000바이트로 충분).
SELECT_TARGETS_SQL = """
SELECT * FROM (
    SELECT k_1, k_2, k_3, ts, base_path, param,
           DBMS_LOB.SUBSTR(stat_desc, 4000, 1) AS stat_desc
      FROM JOB_HISTORY
     WHERE table_name = :tbl
       AND ts >= :ts_from AND ts < :ts_to
       AND ( status = 'FAILURE'
             OR (status = 'WAIT_SCHEDULING' AND ts < :wait_bound) )
     ORDER BY ts ASC
) WHERE ROWNUM <= :row_limit
"""


def committed_batch_ids(table_name, batch_ids):
    """영수증 확인 (설계 4.2): batch_ids 중 테이블 snapshot에 실제로 있는 것만 반환.

    batch당 1회씩 조회하지 않고 IN 조건으로 한 번에 대조한다. 반환도 넘긴 값의
    부분집합이어야 한다 — snapshot 전체를 긁어오면 안 된다.
    TODO(연결): 기존 Trino/Spark 조회 경로 재사용.
      SELECT element_at(summary, 'batch_id')
        FROM <catalog>.<db>.<table>.snapshots
       WHERE element_at(summary, 'batch_id') IN (:batch_ids)
    """
    raise NotImplementedError


def reprocess_select_jobs(cfg, table, run_id):
    """재처리 대상 조회. append get_jobs와 조회 범위·영수증 확인만 다르다.

      ① DB 2개에서 대상 조회 (전날+그저께, WAIT_SCHEDULING은 append 범위 밖만)
      ② 영수증 확인 — 이미 Iceberg에 커밋된 건은 재적재하면 중복이므로 골라낸다
      ③ 남은 것을 ts 오름차순으로 세워 파일 목록과 복합키 목록을 만든다

    반환 dict — 호출부가 각 항목을 기존 함수에 넘긴다 (아래 ③ 블록 참조)
      to_done   이미 커밋 확인된 대상. 재적재하지 않고 상태만 정정
      files     적재할 avro 파일 목록. 비었으면 이번 회차에 적재할 것이 없다
      to_mark   이번에 적재할 대상
      batch_id  이번 배치의 영수증 값
      ts_min/ts_max/has_more   재처리 DAG의 마무리 task 2개가 XCom으로 가져간다

    to_done/to_mark는 `{conn_id: [복합키 값 tuple, ...]}` 형태다. 복합키 값은
    DB 간 유일 보장이 없어 어느 DB에서 온 row인지가 UPDATE 대상을 결정한다.
    """
    if not cfg:
        raise ValueError("prepare_run 결과 없음 — 선행 task 실패")

    table_name = table.get_name()
    result = {"files": [], "to_done": {}, "to_mark": {}, "batch_id": None,
              "ts_min": None, "ts_max": None, "has_more": False}
    if table_name not in cfg["tables"]:
        return result   # 수동 실행에서 미선택 → skip

    # ── ① 조회 ────────────────────────────────────────────────────────────
    # DB별로 실행하고 결과도 conn_id를 키로 보관한다. 상태 UPDATE가 이 키로
    # 원천 DB를 찾아가므로 row에 출처를 따로 붙일 필요가 없다. ROW_LIMIT은 DB당.
    binds = {"tbl": table_name, "row_limit": ROW_LIMIT, "ts_from": cfg["ts_from"],
             "ts_to": cfg["ts_to"], "wait_bound": cfg["wait_bound"]}
    jobs_by_conn = {}
    for conn_id in ORACLE_CONN_IDS:  # noqa: F821
        rows = OracleHook(oracle_conn_id=conn_id).get_records(
            SELECT_TARGETS_SQL, parameters=binds)
        jobs_by_conn[conn_id] = [Job(*row) for row in rows]

    # 상한을 꽉 채운 DB가 있으면 거기에 더 남았다는 뜻이다. ②에서 건수가 줄면
    # 알 수 없게 되므로 지금 기록해 둔다 (loop를 한 번 더 돌지 판단하는 근거)
    result["has_more"] = any(len(jobs) >= ROW_LIMIT for jobs in jobs_by_conn.values())

    # ── ② 영수증 확인 (설계 4) ────────────────────────────────────────────
    # status는 커밋 여부의 증거가 아니다. Airflow가 실패로 판정했든(FAILURE),
    # 커밋 뒤 상태 갱신만 실패했든(WAIT_SCHEDULING으로 잔류) Spark 커밋은 성공했을
    # 수 있다. batch_id가 snapshot에 있으면 이미 Iceberg에 있다 → 재적재 금지.
    batch_ids = {j.stat_desc for jobs in jobs_by_conn.values() for j in jobs
                 if j.stat_desc}
    committed = committed_batch_ids(table_name, batch_ids) if batch_ids else set()
    if committed:
        for conn_id, jobs in jobs_by_conn.items():
            done = [j for j in jobs if j.stat_desc in committed]
            if done:
                result["to_done"][conn_id] = [j[:PK_LEN] for j in done]
                jobs_by_conn[conn_id] = [j for j in jobs
                                         if j.stat_desc not in committed]

    # ── ③ 적재 대상 구성 ──────────────────────────────────────────────────
    # DB별 결과를 합쳐 오래된 것부터 세운다 (append와 같은 순서). 항목에 conn_id를
    # 함께 두는 이유는 합치고 나면 어느 DB에서 왔는지 잃어버리기 때문이다.
    candidates = [(j.ts, j, conn_id)
                  for conn_id, jobs in jobs_by_conn.items() for j in jobs]
    candidates.sort(key=lambda c: c[0])   # DB별로 정렬돼 있어도 합치면 깨진다

    for _, job, conn_id in candidates:
        result["files"] += json.loads(job.param)["files"]  # row당 파일 여러 개 가능
        result["to_mark"].setdefault(conn_id, []).append(job[:PK_LEN])

    if not result["files"]:
        # 조회가 비었거나 전부 영수증 정정으로 빠진 경우.
        # 후자에서 has_more가 살아 있어도 loop를 돌지 않는데, 정정된 건은 SUCCESS로
        # 확정돼 다시 조회되지 않으므로 다음날 실행이 나머지를 이어받는다.
        return result

    # ts 오름차순이고 전부 담았으므로 양 끝이 곧 이번 적재의 시간 범위다
    result["ts_min"], result["ts_max"] = candidates[0][0], candidates[-1][0]
    result["batch_id"] = f"{run_id}_{table_name}"  # 배치당 1개 = 커밋 1회 = 영수증 1개
    return result


# ══════════════════════════════════════════════════════════════════════════
# ★ 변경 — 쿼리 클래스: 상태 UPDATE를 batch_id 유무로 분기
#
#   SQL 문 하나로 합쳐 `SET stat_desc = :batch_id`를 항상 두면, batch_id 없이
#   부르는 update_success / update_failure / 영수증 정정이 stat_desc를 NULL로
#   덮어써 영수증이 지워진다 (설계 5.3-6).
# ══════════════════════════════════════════════════════════════════════════

class JobHistoryQuery:
    """Job History SQL 모음 — 각 함수가 SQL 문자열을 돌려주는 기존 구조 그대로."""

    # 두 문장을 조립하지 않고 그대로 적는다. WHERE 한 줄이 겹치지만,
    # SQL을 읽어서 무엇이 나가는지 바로 알 수 있는 편이 낫다.
    _UPDATE_STATUS = """
UPDATE JOB_HISTORY
   SET status = :status
 WHERE k_1 = :k_1 AND k_2 = :k_2 AND k_3 = :k_3 AND ts = :ts
"""

    _UPDATE_STATUS_WITH_RECEIPT = """
UPDATE JOB_HISTORY
   SET status = :status, stat_desc = :batch_id
 WHERE k_1 = :k_1 AND k_2 = :k_2 AND k_3 = :k_3 AND ts = :ts
"""

    @classmethod
    def update_status(cls, batch_id=None):
        """상태 UPDATE SQL.

        batch_id를 주면 영수증(stat_desc)까지 기록하고, 주지 않으면 status만
        바꿔 **기존 영수증을 보존한다.** 빈 문자열이 아니라 `None` 여부로
        판단한다 — "값이 참인가"가 아니라 "인자를 주었는가"가 기준이다.
        """
        return cls._UPDATE_STATUS if batch_id is None else cls._UPDATE_STATUS_WITH_RECEIPT


def update_jobs_sample(conn_id, keys, status, batch_id=None):
    """호출부 예시 — SQL과 바인드를 **같은 조건으로** 갈라야 한다.

    SQL에 `:batch_id`가 없는데 바인드에 넣으면 Oracle이 바인드 불일치로 실패하고,
    반대로 SQL에만 있고 바인드에 없으면 바인드 누락으로 실패한다. 그래서 두 분기가
    한 함수 안에 붙어 있어야 어긋나지 않는다.
    """
    if not keys:
        return
    sql = JobHistoryQuery.update_status(batch_id)

    binds = []
    for k_1, k_2, k_3, ts in keys:          # keys = [복합키 값 tuple, ...]
        bind = {"status": status, "k_1": k_1, "k_2": k_2, "k_3": k_3, "ts": ts}
        if batch_id is not None:            # ← SQL 분기와 같은 조건
            bind["batch_id"] = batch_id
        binds.append(bind)

    hook = OracleHook(oracle_conn_id=conn_id)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, binds)         # 한 호출 안에서는 바인드 키가 동일하다
        conn.commit()


# 분기를 한 번만 두고 싶다면 SQL과 바인드를 함께 돌려주는 형태도 된다.
# 어긋날 여지가 아예 없어지지만, "쿼리 함수는 쿼리만 돌려준다"는 기존 규칙에서는
# 벗어난다.
#
#     @classmethod
#     def update_status(cls, batch_id=None):
#         if batch_id is None:
#             return cls._UPDATE_STATUS, {}
#         return cls._UPDATE_STATUS_WITH_RECEIPT, {"batch_id": batch_id}
#
#     sql, extra = JobHistoryQuery.update_status(batch_id)
#     bind = {"status": status, **extra, "k_1": k_1, ...}


# ══════════════════════════════════════════════════════════════════════════
# 기존 클래스 — ★ 추가 ②③ 만 반영하면 된다
# ══════════════════════════════════════════════════════════════════════════

class ConvertFileTaskGroup(TaskGroup):
    """avro → Iceberg append 공통 TaskGroup (get_jobs → spark → update_success/failure).

    본문의 `... 기존 코드 ...`는 실제 구현이 그대로 있는 자리다.
    """

    def __init__(
        self,
        table,                    # 기존 그대로: 대상 테이블 Enum
        group_id: str,            # 기존 그대로
        # ... 기존에 받던 나머지 인자들 그대로 ...
        reprocess_cfg=None,       # ★ 추가 ②: 재처리 조회 범위(prepare_run XCom).
        **kwargs,                 #            미지정이면 기존 append 동작 그대로
    ):
        super().__init__(group_id=group_id, **kwargs)

        # ── 기존 __init__ 지역값들: 위치 이동 없음 ─────────────────────────
        logger = logging.getLogger(__name__)
        config = ...              # noqa: F841  기존 설정값 로딩 그대로

        def _update_jobs(conn_id, keys, status, batch_id=None):
            """Job History 상태 update — 기존 지역 헬퍼.

            ★ 변경 필요: **batch_id를 준 호출에서만 stat_desc를 건드린다.**
              UPDATE 문 하나로 합쳐 `SET stat_desc = :batch_id`를 항상 두면,
              batch_id 없이 부르는 update_success / update_failure / 영수증 정정이
              stat_desc를 NULL로 덮어써 **영수증이 지워진다.** 특히 update_failure는
              다음날 재처리가 영수증을 확인해야 할 바로 그 row를 지우므로,
              거짓 실패(커밋 성공 + Airflow 실패) 건이 그대로 재적재된다.

                  batch_id 있음:  SET status = :status, stat_desc = :batch_id
                  batch_id 없음:  SET status = :status          (stat_desc 보존)

              `COALESCE(:batch_id, stat_desc)`로 한 문장에 담는 방법은 권하지 않는다 —
              stat_desc가 CLOB이라 VARCHAR2 바인드와 섞으면 암시적 변환에 의존하고,
              executemany에서 batch_id가 전 건 None이면 바인드 타입 추론이 안 된다.

            영수증은 **마킹 때 한 번 쓰고 이후 상태 변경에서는 건드리지 않는다.**
            SUCCESS 이후에도 남겨야 좀비 수동 판정(설계 8.2)에서 대조할 수 있다.
            """
            ...                   # 기존 구현 + 위 분기

        def _make_file_list(files):
            """param에서 뽑은 파일 목록을 받아
               ① avro 경로 텍스트 파일을 S3에 저장하고
               ② size 총합으로 executor 개수를 산정해 반환한다
               → **기존에 있는 함수**. 아래 이름은 임의로 붙인 것이다.
               TODO(연결): 실제 함수명·시그니처로 교체."""
            ...                   # 기존 구현
            return num_executors  # noqa: F821

        def _other_helper(*args):  # 다른 지역 함수들도 그대로
            ...

        # ── ★ 추가 ③: get_jobs 생성부를 분기로 감싼다 ─────────────────────
        if reprocess_cfg is None:
            # [기존 경로 — append DAG] 인라인 코드 그대로, if 안으로 들여쓰기만.
            # logger/config/_update_jobs 등 closure 참조 전부 그대로 동작한다.
            @task(task_group=self)
            def get_jobs(ti=None):
                logger.info("get_jobs start: %s", table.get_name())
                ...               # 기존 조회/선점/XCom 로직 그대로

            jobs = get_jobs()
        else:
            # [재처리 경로] 조회 대상만 다르고, 이후 처리는 기존 함수를 그대로 쓴다.
            @task.short_circuit(
                task_group=self,
                task_id="get_jobs",
                trigger_rule="all_done",                # 앞 테이블 실패에도 실행
                ignore_downstream_trigger_rules=False,  # skip을 그룹 내로 한정
            )
            def get_jobs(cfg, run_id=None, ti=None):
                logger.info("reprocess get_jobs: %s", table.get_name())
                r = reprocess_select_jobs(cfg, table, run_id)

                # 이미 Iceberg에 커밋된 건 → 재적재 없이 SUCCESS 정정.
                # batch_id를 넘기지 않아 기존 stat_desc(영수증)가 유지된다.
                # files가 비어도 이건 처리해야 하므로 아래 return보다 위에 둔다
                for conn_id, pks in r["to_done"].items():
                    _update_jobs(conn_id, pks, "SUCCESS")

                if not r["files"]:
                    return False              # 대상 없음 → 그룹 내 하류 skip

                # 파일 목록 → S3 텍스트 파일 + executor 개수 (append와 동일).
                # 반환된 개수를 XCom에 올려야 Spark operator가 pull해서 쓴다
                ti.xcom_push(key="num_executors", value=_make_file_list(r["files"]))

                # 마킹 대상을 XCom에 먼저 남긴다 (설계 5.3) — 마킹 도중 실패해도
                # update_failure가 이 값으로 대상을 되찾을 수 있다
                ti.xcom_push(key="meta", value={"batch_id": r["batch_id"],
                                                "keys": r["to_mark"]})
                # 재처리 DAG의 마무리 task 2개가 가져갈 값 (Compaction 범위·loop 판단)
                ti.xcom_push(key="reprocess", value={
                    "ts_min": r["ts_min"], "ts_max": r["ts_max"],
                    "has_more": r["has_more"]})

                for conn_id, pks in r["to_mark"].items():
                    _update_jobs(conn_id, pks, "IN_PROGRESS", r["batch_id"])
                return True

            jobs = get_jobs(reprocess_cfg)

        # ── 이하 기존 코드 그대로 (Spark / update task / 연결) ─────────────
        # 그룹 안의 XCom 주고받기는 손댈 것이 없다 — group_id가 __init__ 인자라
        # 재처리(group_id만 다름)에서도 기존 조립이 그대로 맞는다.
        spark = ...               # 기존 SparkKubernetesOperator 확장 operator 그대로
                                  # (task_id="append_data", task_group=self, ...)
                                  # ★ Spark 쓰기에 영수증 옵션 추가 (설계 4.2):
                                  #   option("snapshot-property.batch_id", batch_id)
                                  #   — meta의 batch_id를 그대로 쓴다

        @task(task_id="update_success", task_group=self, trigger_rule="all_success")
        def update_success(ti=None):
            ...                   # 기존 구현 (meta의 키 목록으로 SUCCESS 처리)

        @task(task_id="update_failure", task_group=self, trigger_rule="all_failed")
        def update_failure(ti=None):
            ...                   # 기존 구현 (meta의 키 목록으로 FAILURE 처리)

        jobs >> spark >> [update_success(), update_failure()]


# ── 사용 예 ──────────────────────────────────────────────────────────────
#
# 1) append DAG — 기존 호출 그대로 (reprocess_cfg 미지정 → 기존 경로, 동작 동일)
#
#     ConvertFileTaskGroup(table, group_id=f"convert_{table.get_name()}")
#
# 2) 재처리 DAG (pipeline/dags/iceberg_reprocess.py) — 조회 범위만 전달
#
#     ConvertFileTaskGroup(
#         table,
#         group_id=f"reprocess_{table.get_name()}",
#         reprocess_cfg=run_cfg,      # prepare_run의 XCom (ts 경계·tables·loop_count)
#     )
