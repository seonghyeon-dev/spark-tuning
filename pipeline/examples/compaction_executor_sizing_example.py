"""hourly Compaction의 `num-executors`를 데이터 양에 따라 산정하는 방법 — **기존 파일 변경 예시**.

⚠ 배포용 파일이 아니다. 여기 있는 내용을 **기존 hourly Compaction DAG 파일에**
   반영한다. `compaction_dag_example.py`가 만든 `compaction_specs` task의
   `instances` 값 한 줄을 계산값으로 바꾸는 것이 전부다.

⚠ **이 파일은 설계 3안 중 C안(사전 산정)의 스켈레톤이며, 도입 우선순위가 가장 낮다.**
   `pipeline/compaction-executor-sizing-design.md` §6의 권고는 다음 순서다.
     A안 정적 12 유지 (현재)
       → 55~60GB 도달 시 B안 Spark Dynamic Allocation 먼저 시도 (설정 4줄)
         → B안이 실행 창을 못 지키면 그때 C안(이 파일)

   B를 먼저 두는 이유는 구현 비용 차이다. B는 Spark 설정 4줄이고 롤백이 한 줄인데,
   C는 Trino 연결·fallback·정상 범위 검증에 외부 의존성이 하나 늘어난다.
   목적(데이터 증가 흡수)이 같으므로 싼 것부터 시도한다.

   C가 필요해지는 경우: B의 executor 확보 지연(10~20초)이 90초 job에서 11~22%라,
   실행 창 여유가 없을 때는 처음부터 맞는 수로 시작하는 C가 유리하다.

설계 문서
  `pipeline/compaction-executor-sizing-design.md` — 3안 비교, 산정 위치·조회 경로
  대안 비교, 실패 모드, 검증 계획. 이 파일은 그 설계 C안의 구현 스켈레톤이다.

왜 필요한가
  정적 executor 수는 데이터가 늘면 duration이 비례해 늘고, hourly Compaction의
  실행 창 제약(`M ≤ 60 − duration − 여유` = `:45`, reprocessing-dag-design.md
  §6.2)이 조용히 깨진다. 동적 산정은 증가분을 executor 수로 흡수해 duration을
  일정하게 유지한다.

동적화 대상은 `num-executors` 하나다
  9회 측정 결과 나머지 값은 데이터 양과 무관하거나 크게 고정하는 것이 우월하다
  (compaction-tuning-guide.md §6.1). executor cpu/memory는 task 하나가 처리하는
  단위가 512MB로 고정이라 데이터 양이 늘면 task 수만 늘고 크기는 그대로다.

선행 조건
  Compaction DAG의 mapped task 전환 (`compaction_dag_example.py`). 이 산정 코드가
  `compaction_specs` 안에 들어가므로 그 구조가 먼저 필요하다.

적용 범위
  **hourly 전용이다.** daily Compaction은 아직 튜닝하지 않았고 `rewrite-all` 낭비
  의심이 남아 있어(guide §8.1) 계수 C를 그대로 쓸 수 없다.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime

logger = logging.getLogger(__name__)

# 기존 파일에 이미 있는 것 (재사용)
#   IcebergTable                테이블 Enum. get_name(), config 제공
#   table.config.com_num_executor   기존 정적 상수 → fallback으로 유지한다

# --- 산정 상수 ----------------------------------------------------------------

EXECUTOR_CORES = 4

# 9회 측정으로 확정 (guide §6.4). 37.3GB에서 executor 12개가 dcu 최저점.
#   16개(C=0.42) dcu/GB 0.00251  →  12개(C=0.32) 0.00219  →  8개(C=0.22) 0.00247
#   8개에서 반등하므로 12가 하한이다.
SIZING_COEFFICIENT = 0.32

MIN_EXECUTORS = 4

# TODO(연결) K8S namespace quota 확인 후 확정.
#   append 벤치마크에서 32개 이상은 오히려 느려졌다(shuffle 통신, pod 스케줄링 경합,
#   S3 부하 — spark-tuning-guide.md §2.2.3). append가 batch당 약 10 executor를
#   5분 주기로 상시 점유하므로 그만큼을 남겨야 한다.
MAX_EXECUTORS = 32

# 산정값이 이 범위를 벗어나면 조회 결과를 신뢰하지 않고 fallback한다.
# 현재 데이터는 시간당 36~42GB이므로 상한 500GB는 명백한 이상값만 걸러낸다.
SANE_SIZE_RANGE_GB = (0.1, 500.0)

_EPOCH = datetime(1970, 1, 1)


# --- 대상 파티션 범위 ---------------------------------------------------------

def to_partition_hour(dt: datetime) -> int:
    """datetime을 Iceberg `hour(ts)` 파티션 값(epoch 기준 시간 수)으로 변환한다.

    `ts`가 `timestamp_ntz`이므로 **naive datetime으로 계산해야 한다.** timezone을
    붙여 `timestamp()`를 쓰면 Iceberg가 저장한 값과 어긋나 엉뚱한 시간대를 조회한다.

    검증: 2026-08-11 13:00 → 496237.
      Spark UI가 출력한 `PartitionData{ts_hour=496237, col_a=D}`와 일치한다.
    """
    if dt.tzinfo is not None:
        raise ValueError(f"naive datetime이 필요하다 (timestamp_ntz): {dt!r}")
    return int((dt - _EPOCH).total_seconds() // 3600)


# --- 입력 크기 조회 -----------------------------------------------------------

# `.files`가 아니라 `.partitions`를 조회한다.
#   .files 는 데이터 파일 1개당 1행이고, 그 행에 컬럼 19개 전부의 통계
#   (column_sizes, value_counts, null_value_counts, lower_bounds, upper_bounds)가
#   들어간다. 총 크기 하나만 필요한데 전부 끌고 온다.
#   .partitions 는 파티션당 1행으로 이미 집계되어 있다 (보관 30일 가정 시
#   .files 약 54,000행 vs .partitions 필터 후 4행).
#
# TODO(확인) Trino의 `$partitions`에서 `partition.ts_hour`가 INTEGER로 노출되는지
#   확인한다. 아래 쿼리를 DBeaver에서 1회 실행해 컬럼명과 타입을 맞춘다.
#     SELECT * FROM "<schema>.<table>$partitions" LIMIT 5;
#   Spark SQL로 조회할 경우 컬럼명이 `total_data_file_size_in_bytes`로 다르다.
#
# TODO(확인) manifest pruning이 걸리는지 확인한다. 아래 두 쿼리의 Physical input을
#   비교해 필터 쪽이 확연히 작으면 걸린 것이다. 걸리지 않으면 manifest 전체를
#   읽으므로(최악 30~90MB, 수 초) 비용을 재검토한다.
#     SELECT count(*) FROM "<schema>.<table>$partitions";
#     SELECT count(*) FROM "<schema>.<table>$partitions" WHERE partition.ts_hour = <값>;

_SIZE_QUERY = """
SELECT coalesce(sum(total_size), 0) AS total_bytes
FROM "{schema}.{table}$partitions"
WHERE partition.ts_hour >= {from_hour}
  AND partition.ts_hour <  {until_hour}
"""


def query_size_bytes(table_name: str, from_hour: int, until_hour: int) -> int:
    """대상 시간 범위의 데이터 총 크기(byte)를 조회한다.

    범위 조회인 이유: 정기 실행은 1시간이지만 재처리 DAG이 trigger할 때는
    `start_time`/`end_time`이 여러 시간에 걸친다 (reprocessing-dag-design.md §6.3).
    """
    from airflow.providers.trino.hooks.trino import TrinoHook

    sql = _SIZE_QUERY.format(
        schema=...,      # TODO(연결) Iceberg schema 이름
        table=table_name,
        from_hour=from_hour,
        until_hour=until_hour,
    )
    hook = TrinoHook(trino_conn_id=...)   # TODO(연결) 기존 Trino connection id
    row = hook.get_first(sql)
    return int(row[0]) if row and row[0] is not None else 0


# --- 산정 -------------------------------------------------------------------

def num_executors_for(table, from_hour: int, until_hour: int) -> int:
    """대상 범위의 데이터 크기로 executor 수를 산정한다. 실패 시 기존 상수로 fallback.

    조회는 외부 의존성(Trino)이므로 **어떤 실패에도 Compaction 자체는 실행되어야
    한다.** 기존 `com_num_executor` 상수를 지우지 않고 fallback으로 남기는 이유다.

    조회가 성공했는데 값이 0인 경우도 막아야 한다. 파티션 조건이 틀려 0이 오면
    executor가 MIN_EXECUTORS로 떨어져 Job이 한없이 느려진다.
    """
    fallback = int(table.config.com_num_executor)

    try:
        total_bytes = query_size_bytes(table.get_name(), from_hour, until_hour)
        total_gb = total_bytes / (1024 ** 3)

        low, high = SANE_SIZE_RANGE_GB
        if not low <= total_gb <= high:
            raise ValueError(f"크기가 정상 범위를 벗어남: {total_gb:.1f}GB")

        sized = math.ceil(total_gb * SIZING_COEFFICIENT)
        clamped = min(max(sized, MIN_EXECUTORS), MAX_EXECUTORS)

        if clamped == MAX_EXECUTORS and sized > MAX_EXECUTORS:
            # 상한에 걸리는 것은 데이터가 설계 범위를 넘었다는 신호다.
            # 파티션 재설계나 K8S 슬롯 확대를 검토해야 한다.
            logger.warning(
                "[%s] executor 산정값 %d가 상한 %d를 초과 (%.1fGB). "
                "파티션 재설계 또는 quota 확대 검토 필요",
                table.get_name(), sized, MAX_EXECUTORS, total_gb,
            )

        logger.info(
            "[%s] %.1fGB → executor %d (C=%.2f, 정적값 %d)",
            table.get_name(), total_gb, clamped, SIZING_COEFFICIENT, fallback,
        )
        return clamped

    except Exception as exc:
        logger.warning(
            "[%s] 크기 조회 실패, 정적값 %d 사용: %s",
            table.get_name(), fallback, exc,
        )
        return fallback


# --- compaction_specs에 연결 --------------------------------------------------
#
# `compaction_dag_example.py`의 compaction_specs에서 `instances` 한 줄만 바꾼다.
# 나머지(arguments, tables 필터, mapped task 구조)는 그대로다.

def compaction_specs_fragment(params):
    """변경 지점만 발췌. 실제로는 compaction_dag_example.py의 @task 안에 들어간다."""
    # hourly는 start_time/end_time을 params로 받는다 (정각 정렬 — guide §3.4)
    start_time = params.get("start_time") or ...   # TODO(연결) 기존 기본값 로직
    end_time = params.get("end_time") or ...       # TODO(연결) 기존 기본값 로직

    from_hour = to_partition_hour(start_time)
    until_hour = to_partition_hour(end_time)

    selected = set(params["tables"])
    return [
        {
            "arguments": [
                table.get_name(),
                str(COM_TARGET_FILE_SIZE_BYTES),                    # noqa: F821
                start_time.strftime("%Y-%m-%d %H:%M:%S"),           # TODO(연결) 기존 포맷
                end_time.strftime("%Y-%m-%d %H:%M:%S"),             # TODO(연결) 기존 포맷
                str(table.config.com_max_concurrent_file_group),
            ],
            # 변경: 정적 상수 → 계산값
            #   before: "instances": str(table.config.com_num_executor),
            "instances": str(num_executors_for(table, from_hour, until_hour)),
        }
        for table in IcebergTable                                    # noqa: F821
        if table.get_name() in selected
    ]


# --- 도입 전 확인 -------------------------------------------------------------
#
# ① 지금 도입할 필요가 있는가
#    설계 §3의 결론은 아니다. 정적 12개로 테이블당 74.7GB까지 실행 창에 들어가고
#    현재 최대는 42.3GB다(여유 1.77배). 산정값도 12~14로 정적값과 거의 같다.
#    55~60GB 도달 시 도입한다. 그때까지는 `com_num_executor`를 12로 고정한다.
#
# ② com_num_executor 상수를 지우지 않았는가
#    fallback 경로가 이 값을 쓴다. 지우면 Trino 장애가 곧 Compaction 실패가 된다.
#
# ③ 조회 횟수
#    compaction_specs는 DAG run당 1회 실행되고 그 안에서 테이블 수만큼 조회한다.
#    hourly 테이블 4개 × 24시간 = 96회/일.
#
# ④ MAX_EXECUTORS를 확정했는가
#    현재 32는 append 벤치마크에서 온 잠정값이다. K8S quota를 확인해 append가
#    5분 주기로 점유하는 몫(batch당 약 10 executor)을 뺀 값으로 맞춘다.
#
# ⑤ daily에 그대로 쓰지 말 것
#    C=0.32은 hourly 측정값이다. daily는 rewrite-all 낭비 의심이 남아 있어
#    (guide §8.1) 그 확인이 끝난 뒤에 별도로 계수를 잡아야 한다.
