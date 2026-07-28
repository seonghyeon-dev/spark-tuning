"""재처리(Reprocessing) DAG — 구현 스켈레톤.

설계 문서: pipeline/reprocessing-dag-design.md
환경: Airflow 3.2.2

역할: append DAG 조회 기간(최근 1일)에서 밀려난 WAIT/FAILED 데이터를 전날+그저께
      범위에서 회수하고, 적재분에 대해 기존 Compaction DAG을 trigger한다.

구조: 단일 DAG. 테이블별로 기존 ConvertFileTaskGroup을 재사용하며, 조회 범위만
      reprocess_cfg 인자로 넘겨 부모 __init__의 재처리 분기가 조회 task를 만든다.
      params는 prepare_run에서 1회 검증·정규화.
      잔여분이 남으면 자기 자신을 재trigger (loop, 상한 10회).

── 기존 구현 연결 지점 (grep "TODO(연결)") ────────────────────────
  1. iceberg.py의 hourly/daily Enum import (자리표시자 2개 교체)
  2. ConvertFileTaskGroup import + __init__에 reprocess_cfg 옵션 인자·분기 추가
     (아래 '기존 ConvertFileTaskGroup 재사용' 주석 참조)
  3. Oracle conn 목록 — append DAG의 conn_list와 동일 소스 (DB 2개 동일 스키마)
  4. 영수증 snapshot 조회 — snapshot_exists
  5. avro 경로 목록 S3 업로드 — upload_path_list_to_s3
  6. 알림 채널 — send_alert
  7. Compaction DAG id 상수 + conf 날짜/시간 형식 (기존 Compaction UI params와 일치)
────────────────────────────────────────────────────────────────────
"""

import json
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

# Oracle DB 2개(a/b)에 동일 스키마의 Job History가 있어 같은 쿼리를 DB별로 반복한다.
# TODO(연결): append DAG이 쓰는 conn_list와 동일 소스 사용
ORACLE_CONN_IDS = ["oracle_a", "oracle_b"]

ROW_LIMIT = 1000              # 테이블당·DB당 조회 상한 (설계 5.4 — 러프 설정, 재검증 필요)
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

    예) ts_min='20260701213000000', ts_max='20260703081500000'
        → ts 앞 8자리(날짜)만 취해 1일부터 3일까지 하루씩 전진
        → ['20260701', '20260702', '20260703']
    """
    day = pendulum.from_format(ts_min[:8], "YYYYMMDD", tz=KST)
    last = pendulum.from_format(ts_max[:8], "YYYYMMDD", tz=KST)
    days = []
    while day <= last:
        days.append(day.format("YYYYMMDD"))
        day = day.add(days=1)
    return days


# --- Oracle -----------------------------------------------------------------

# Job History의 키는 단일 컬럼이 아니라 복합키 4개다. ts도 그중 하나이며
# 조회 범위 조건·정렬·Compaction 범위 산출에 함께 쓰인다.
# TODO(연결): 실제 복합키 컬럼명으로 교체 (순서 무관, 이름만 맞추면 된다)
KEY_COLUMNS = ("k_1", "k_2", "k_3", "ts")

# 조회 컬럼 = 복합키 4개 + base_path + param(JSON: 파일명·크기)
#            + status(WAIT/FAILED 필터) + stat_desc(영수증 확인용)
JOB_COLUMNS = (*KEY_COLUMNS, "base_path", "param", "status", "stat_desc")

# stat_desc만 CLOB이라 드라이버가 LOB 객체로 돌려준다 — 문자열 비교·set 연산이
# 되지 않아 영수증 확인이 오작동하므로, 조회 시점에 VARCHAR2로 변환해서 받는다
# (batch_id는 짧은 값이라 4000바이트로 충분). 나머지 컬럼은 변환 불필요.
JOB_SELECT_EXPRS = (
    *KEY_COLUMNS,
    "base_path",
    "param",
    "status",
    "DBMS_LOB.SUBSTR(stat_desc, 4000, 1) AS stat_desc",
)

SELECT_TARGETS_SQL = f"""
SELECT * FROM (
    SELECT {", ".join(JOB_SELECT_EXPRS)}
      FROM JOB_HISTORY
     WHERE table_name = :tbl
       AND ts >= :ts_from AND ts < :ts_to
       AND ( status = 'FAILED'
             OR (status = 'WAIT' AND ts < :wait_bound) )
     ORDER BY ts ASC
) WHERE ROWNUM <= :row_limit
"""

# 복합키 전체를 AND로 묶은 조건 (executemany 바인딩용 — 건수와 무관하게 고정 SQL)
KEY_WHERE = " AND ".join(f"{k} = :{k}" for k in KEY_COLUMNS)

# TODO(연결): ① 갱신 시각 컬럼명 확인(updated_at 가정) ② 이 쿼리는 조회 조건에
#             ts 범위가 없어 파티션 전체를 스캔한다. status 인덱스가 없다면
#             (IN_PROGRESS는 소수라 인덱스가 효과적) 인덱스 추가나 ts 하한
#             추가를 검토할 것 — 하루 1회 실행이지만 테이블이 크면 부담이 된다.
ZOMBIE_COLUMNS = ("table_name", *KEY_COLUMNS, "updated_at")

ZOMBIE_SQL = f"""
SELECT {", ".join(ZOMBIE_COLUMNS)}
  FROM JOB_HISTORY
 WHERE status = 'IN_PROGRESS'
   AND updated_at < SYSTIMESTAMP - NUMTODSINTERVAL(:h, 'HOUR')
"""


def _rows(conn_id: str, sql: str, binds: dict, columns: tuple[str, ...]) -> list[dict]:
    """OracleHook.get_records → 컬럼명 dict 매핑. columns는 SELECT 순서와 일치해야 한다."""
    records = OracleHook(oracle_conn_id=conn_id).get_records(sql, parameters=binds)
    return [dict(zip(columns, r)) for r in records]


def _execute_many(conn_id: str, sql: str, rows: list[dict]) -> None:
    """고정 SQL을 바인딩 배열로 일괄 실행한다.

    IN 절에 placeholder를 건수만큼 펼치는 방식 대신 executemany를 쓰는 이유:
      - SQL이 건수와 무관하게 고정 → Oracle이 parse 1회 후 커서를 재사용
        (동적 IN 절은 건수가 바뀔 때마다 다른 SQL이 되어 hard parse가 반복됨)
      - IN 리스트 1,000개 제한(ORA-01795)에 걸리지 않음 — ROW_LIMIT을 올려도 안전
      - 바인딩 배열을 한 번에 보내므로 라운드트립 1회

    OracleHook.run()은 executemany를 노출하지 않으므로 커서를 직접 쓴다.
    get_conn()은 autocommit이 아니므로 commit도 명시한다.
    """
    if not rows:
        return
    hook = OracleHook(oracle_conn_id=conn_id)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, rows)
        conn.commit()


MARK_DONE_SQL = f"""
UPDATE JOB_HISTORY SET status = 'DONE'
 WHERE {KEY_WHERE} AND status = 'FAILED'
"""

CLAIM_SQL = f"""
UPDATE JOB_HISTORY SET status = 'IN_PROGRESS', stat_desc = :batch_id
 WHERE {KEY_WHERE} AND status IN ('WAIT', 'FAILED')
"""


def key_of(row: dict) -> dict:
    """row에서 복합키만 뽑아낸다 — UPDATE 바인딩과 XCom 전달에 그대로 쓴다."""
    return {k: row[k] for k in KEY_COLUMNS}


def _mark_done(conn_id: str, keys: list[dict]) -> None:
    """영수증으로 커밋이 확인된 FAILED row를 DONE으로 정정 (설계 4.2)."""
    _execute_many(conn_id, MARK_DONE_SQL, keys)


def _claim_jobs(conn_id: str, keys: list[dict], batch_id: str) -> None:
    """IN_PROGRESS 전환 + batch_id(영수증) 기록 (설계 5.3).

    `status IN ('WAIT','FAILED')` 조건은 만약의 이중 실행에 대한 방어선이다
    (설계상 조회 범위가 겹치지 않아 발생하지 않는다 — 설계 2.1).
    stat_desc(CLOB)는 값 기록만 — WHERE 조건 사용 금지 (설계 4.2).
    """
    _execute_many(conn_id, CLAIM_SQL, [{"batch_id": batch_id, **k} for k in keys])


def parse_param(param) -> tuple[str, float]:
    """param(VARCHAR2, JSON 문자열) → (파일명, 파일 크기 MB).

    TODO(연결): 실제 JSON 키 이름과 크기 단위를 append DAG의 파싱 로직과 맞출 것
                (아래는 {"file_name": ..., "file_size": <bytes>} 가정).
                base_path와의 결합 규칙(구분자·prefix)도 append와 동일해야 한다.
    """
    data = json.loads(param)
    return data["file_name"], float(data["file_size"]) / 1024 / 1024


# --- 기존 구현 연결 스텁 ------------------------------------------------------

def snapshot_exists(table_name: str, batch_id: str) -> bool:
    """영수증 확인 (설계 4.2): 테이블 snapshot summary에 batch_id 존재 여부.
    TODO(연결): 기존 Trino/Spark 조회 경로 재사용.
      SELECT 1 FROM <catalog>.<db>.<table>.snapshots
       WHERE element_at(summary, 'batch_id') = :batch_id
    """
    raise NotImplementedError


def upload_path_list_to_s3(table_name: str, paths: list[str], batch_id: str) -> None:
    """경로 문자열 배열을 텍스트 파일로 만들어 S3에 업로드 (Spark 입력 목록).
    TODO(연결): 기존 get_jobs의 업로드 로직 재사용 (파일 경로 규칙 포함)."""
    raise NotImplementedError


def send_alert(message: str, detail=None) -> None:
    """TODO(연결): 기존 알림 채널 재사용."""
    raise NotImplementedError


# --- 재처리 조회 로직 (ConvertFileTaskGroup.__init__ 안에서 호출됨) -----------
#
# 부모 __init__의 재처리 분기가 이 함수를 감싸는 task를 만든다. 부모의 지역 함수·
# 설정값(_update_jobs, logger 등)은 그 분기가 같은 스코프에 있으므로 그냥 쓰면 되고,
# 이 함수에는 스코프와 무관한 순수 로직(조회 범위·상한·영수증 확인)만 둔다.

def reprocess_get_jobs(cfg: dict, *, table, run_id, ti) -> bool:
    """append get_jobs의 재처리 버전 — 조회 범위·상한·영수증 확인만 다르다.

    반환 False = 처리 대상 없음 (short_circuit → 그룹 내 하류 skip).
    meta는 key="meta"로 push — 하류 Spark(num_executors)·update(keys)·
    집계 task(Compaction/loop)가 소비한다.
    """
    if not cfg:
        raise ValueError("prepare_run 결과 없음 — 선행 task 실패")

    table_name = table.get_name()
    if table_name not in cfg["tables"]:
        return False  # 수동 실행에서 미선택 → skip

    # ── 조회: DB별로 같은 쿼리 실행 (append의 conn_list 패턴) ───────────────
    # 결과를 conn_id를 키로 하는 dict에 그대로 담는다. 이후 상태 UPDATE는
    # 이 키로 자기 DB를 찾아가므로, row에 출처를 태깅하거나 나중에 다시
    # 그룹핑할 필요가 없다 (복합키는 DB 간 유일 보장 없음).
    # ROW_LIMIT은 append(DB당 200)와 동일하게 DB당 적용된다.
    jobs_by_conn = {
        conn_id: _rows(
            conn_id, SELECT_TARGETS_SQL,
            {"tbl": table_name, "ts_from": cfg["ts_from"], "ts_to": cfg["ts_to"],
             "wait_bound": cfg["wait_bound"], "row_limit": ROW_LIMIT},
            JOB_COLUMNS,
        )
        for conn_id in ORACLE_CONN_IDS
    }
    # 어느 한쪽 DB라도 상한을 꽉 채웠으면 = 그 DB에 더 남아 있다는 신호.
    # 아래 영수증/크기 필터로 줄어든 "후"의 건수로 판단하면 신호를 놓치므로
    # 반드시 필터 적용 "전"에 기록해 둔다 (설계 5.3)
    fetched_full = any(len(rows) >= ROW_LIMIT for rows in jobs_by_conn.values())

    # ── 영수증 확인 (설계 4): "거짓 실패" 걸러내기 ──────────────────────────
    # Airflow가 실패로 판정했어도 Iceberg 커밋은 성공했을 수 있다 (커밋 직후
    # Pod 통신 오류 등). 그대로 재적재하면 중복이 되므로:
    #   1) FAILED row들이 달고 있는 batch_id(stat_desc)를 set으로 수집
    #      — 같은 batch의 row가 수백 건이어도 snapshot 조회는 batch당 1회
    #   2) batch_id별로 해당 테이블 snapshot에 영수증이 있는지 확인
    #   3) 있으면 = 이미 적재 완료 → 원천 DB별로 DONE 정정하고 대상에서 제외
    failed_batches = {r["stat_desc"] for rows in jobs_by_conn.values() for r in rows
                      if r["status"] == "FAILED" and r["stat_desc"]}
    committed = {b for b in failed_batches if snapshot_exists(table_name, b)}
    if committed:
        for conn_id, rows in jobs_by_conn.items():
            _mark_done(conn_id, [key_of(r) for r in rows
                                 if r["stat_desc"] in committed])
        jobs_by_conn = {
            conn_id: [r for r in rows if r["stat_desc"] not in committed]
            for conn_id, rows in jobs_by_conn.items()
        }

    # ── 크기 상한 적용 (설계 5.4) ───────────────────────────────────────────
    # DB별 결과를 (row, conn_id) 쌍으로 펼쳐 전체 ts 오름차순으로 정렬한 뒤,
    # 앞(가장 오래된 것)부터 누적 크기가 16GB를 넘기 직전까지만 담는다.
    # 잘린 뒤쪽은 상태를 건드리지 않고 이월 → loop 회차 또는 다음날 회수
    candidates = sorted(
        ((r, conn_id) for conn_id, rows in jobs_by_conn.items() for r in rows),
        key=lambda pair: pair[0]["ts"],
    )
    picked_ts = []                            # ts 오름차순 (Compaction 범위 산출용)
    picked_paths = []                         # base_path+파일명 결합 문자열 (S3 목록)
    picked_keys: dict[str, list[dict]] = {}   # {conn_id: [복합키 dict, ...]} — 상태 UPDATE용
    total_mb = 0.0                            # 크기는 param JSON에서 꺼내 float으로 누적
    for row, conn_id in candidates:
        file_name, size_mb = parse_param(row["param"])
        if total_mb + size_mb > SIZE_LIMIT_MB:
            break
        picked_ts.append(row["ts"])
        picked_paths.append(f"{row['base_path'].rstrip('/')}/{file_name}")
        picked_keys.setdefault(conn_id, []).append(key_of(row))
        total_mb += size_mb

    if not picked_paths:
        # 정상 케이스는 "처리할 게 없어서 빈 것"(조회도 비고 상한 미달)뿐이다.
        # 그 외는 조용히 skip하면 안 되는 비정상 신호라 알림으로 노출한다:
        #   candidates 있음 → 선두 job 하나가 크기 상한(16GB) 초과 (매일 반복될 데이터)
        #   fetched_full   → 조회분이 전부 영수증 정정으로 소진 — DB에 더 남아
        #                     있는데 meta가 없어 loop의 잔여분 신호가 유실됨
        if candidates or fetched_full:
            send_alert(
                f"재처리 {table_name}: 처리 대상 구성 불가 — 수동 확인 필요 "
                f"(조회 상한 도달={fetched_full}, 크기 상한 초과 잔여 {len(candidates)}건)"
            )
        return False

    # 잔여분(leftover) 판정 — 둘 중 하나라도 참이면 "아직 남았다":
    #   fetched_full                        : 어느 한 DB라도 조회 상한(1,000)을 꽉 채움
    #   len(picked_paths) < len(candidates) : 크기 상한으로 뒤쪽이 잘림
    # (영수증으로 제외된 건은 '처리 완료 정정'이라 잔여분이 아님 — 이미 candidates에서 빠짐)
    leftover = fetched_full or len(picked_paths) < len(candidates)

    # batch_id는 배치당 1개 — 두 DB에서 온 row들이 하나의 Spark 커밋으로 적재되므로
    # 양쪽 DB의 row 모두 같은 batch_id를 달고, 영수증 확인도 snapshot 1곳에서 끝난다
    batch_id = f"{run_id}_{table_name}"

    # meta를 마킹보다 "먼저" 기록한다 (설계 5.3).
    # 마킹은 DB 수만큼 UPDATE가 나가므로 중간에 실패할 수 있는데, meta가 없으면
    # 이미 마킹된 row를 update_failure가 회수하지 못해 좀비 IN_PROGRESS가 된다.
    # 반대로 meta가 먼저 있으면: 마킹 안 된 row는 상태가 WAIT/FAILED라
    # update task의 UPDATE(WHERE status='IN_PROGRESS')에서 자동으로 빠지고
    # 다음 회차에 정상 회수된다 — 어느 쪽으로 실패해도 안전하다.
    # TODO(연결): meta의 key/필드명은 append get_jobs가 push하는 스키마와 필드 단위로
    #             일치시킬 것 — 부모의 Spark(num_executors)·update(keys) task가
    #             append과 같은 방식으로 이 XCom을 소비한다.
    #             keys는 {conn_id: [복합키 dict, ...]} — update task도 conn_list loop로
    #             각 DB에 복합키 AND 조건으로 UPDATE하는 append 방식과 동일해야 한다
    ti.xcom_push(key="meta", value={
        "table": table_name,
        "group": compaction_group(table),
        "batch_id": batch_id,
        "keys": picked_keys,               # {conn_id: [복합키 dict, ...]} — 원천 DB별
        "leftover": leftover,
        "ts_min": picked_ts[0], "ts_max": picked_ts[-1],
        # append DAG과 동일 산정식: ceil(총크기/128MB × 1.5 / executor-cores 4)
        "num_executors": min(max(math.ceil(total_mb / 128 * 1.5 / 4), 1), MAX_EXECUTORS),
    })

    for conn_id, keys in picked_keys.items():  # 마킹은 원천 DB별로
        _claim_jobs(conn_id, keys, batch_id)

    upload_path_list_to_s3(table_name, picked_paths, batch_id)
    return True


# --- 기존 ConvertFileTaskGroup 재사용 (__init__에 재처리 분기 추가) ----------
#
# ConvertFileTaskGroup: get_jobs → spark append → [update_success, update_failure]
# 재처리는 get_jobs(조회)만 다르고 나머지는 동일하다.
#
# 조회 task를 부모 밖으로 빼는 방식(상속 override / builder 주입 / 헬퍼 전달)은
# 전부 기각 — 조회 로직은 __init__ 지역 함수·설정값(_update_jobs, logger, config …)을
# 써야 하는데, 밖으로 빼면 그것들을 일일이 넘겨야 하고 헬퍼가 늘 때마다 깨진다.
#
# 채택: 재처리 조회 task도 __init__ 안에 둔다. 같은 스코프이므로 지역 함수를
#       그냥 호출하면 되고, 넘길 것은 조회 범위(prepare_run XCom) 하나뿐이다.
#
#     class ConvertFileTaskGroup(TaskGroup):
#         def __init__(self, table, group_id, ..., reprocess_cfg=None, **kwargs):
#             super().__init__(group_id=group_id, **kwargs)
#             ... 기존 그대로: logger, config, def _update_jobs(...) ...
#
#             if reprocess_cfg is None:
#                 @task(task_group=self)              # 기존 append 조회 — 그대로
#                 def get_jobs(ti=None):
#                     ... _update_jobs(...) / logger / config 그대로 ...
#                 jobs = get_jobs()
#             else:
#                 # 재처리 조회 — 같은 스코프라 지역 함수를 그냥 쓴다
#                 @task.short_circuit(
#                     task_group=self, task_id="get_jobs",
#                     trigger_rule="all_done",                # 앞 테이블 실패에도 실행
#                     ignore_downstream_trigger_rules=False,  # skip을 그룹 내로 한정
#                 )
#                 def get_jobs(cfg, run_id=None, ti=None):
#                     logger.info("reprocess get_jobs: %s", table.get_name())
#                     return reprocess_get_jobs(cfg, table=table, run_id=run_id, ti=ti)
#                 jobs = get_jobs(reprocess_cfg)
#
#             spark = ...
#             jobs >> spark >> [update_success, update_failure]
#
# append DAG은 reprocess_cfg를 안 넘기므로 동작이 완전히 동일하다.
#
# TODO(연결): 부모 import + __init__에 reprocess_cfg 옵션 인자와 위 else 분기 추가.
#             재처리 마킹을 부모 _update_jobs로 대체할지는 그 시그니처를 보고 결정
#             (대체 시 이 파일의 _mark_done/_claim_jobs 제거).
#             변경 예시 전체: pipeline/examples/convert_file_taskgroup_example.py


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
        """좀비 IN_PROGRESS 탐지 → 알림만 (설계 8.2). 독립 실행 — 본류와 의존 없음.
        Oracle 2개 모두 조회한다 (어느 DB에서 발견됐는지 알림에 포함)."""
        # conn_id를 키로 담아 그대로 알림에 넘긴다 (어느 DB의 row인지 구분됨)
        zombies_by_conn = {
            conn_id: _rows(conn_id, ZOMBIE_SQL, {"h": ZOMBIE_HOURS}, ZOMBIE_COLUMNS)
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

        # loop 재trigger 회차: 첫 회차가 확정한 값을 그대로 승계 (설계 5.5)
        if conf.get("ts_from"):
            return {
                "tables": conf["tables"],
                "ts_from": conf["ts_from"],
                "ts_to": conf["ts_to"],
                "wait_bound": conf["wait_bound"],
                "loop_count": int(conf.get("loop_count", 0)),
            }

        base = pendulum.now(KST).start_of("day")  # 오늘 00:00 (오늘=4일 01:00 실행이면 4일 00:00)
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
            # 정기 실행 경계 (설계 2.1). 오늘=4일 (base=4일 00:00) 기준 예시:
            #   ts_from    = 그저께(2일) 00:00 — 재처리가 하룻밤 실패해도
            #                             다음날 이 안전망 범위로 자동 회수된다
            #   ts_to      = 오늘(4일) 00:00  — 전날 끝
            #   wait_bound = 전날(3일) 01:00  — WAIT 상한. 이 시각 이후의 WAIT는 아직
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
        # 예) TABLE_A가 그저께~전날(2~3일), TABLE_B가 전날(3일)만 적재했다면
        #     by_date = {'20260702': [A], '20260703': [A, B]}
        #     → trigger 2건: (2일, [A]), (3일, [A, B])
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
    groups = [
        ConvertFileTaskGroup(  # noqa: F821  TODO(연결): 부모 import + 시그니처 확인
            t,
            group_id=f"reprocess_{t.get_name()}",
            reprocess_cfg=run_cfg,   # 조회 범위만 전달 — 조회 task는 부모가 생성
        )
        for t in ALL_TABLES
    ]
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
