"""append Job executor 수 산정: get_jobs 변경 전/후 (작업 1, 2026-10-14 확정)

입력은 get_jobs가 JOB_HISTORY param JSON에서 이미 뽑는 파일 목록이다
({"files": [{"name": ..., "size": "2262129"}, ...]}, size는 바이트 문자열).

변경 전: avro 총 바이트만 보고 executor 수를 정한다.
변경 후: Spark가 avro 읽기 task를 나누는 규칙 그대로 읽기 task 수를 센 뒤,
         그 task를 몇 차례(READ_WAVES)에 처리할지로 총 core를 정하고 executor 수로 바꾼다.
         파일 수가 많고 크기가 작은 테이블(2번)이 바이트만 볼 때보다 core를 더 받는다.

상수 중 사용자 결정값은 READ_WAVES, EXECUTOR_CORES, MAX_TOTAL_CORES 세 개다.
나머지는 Spark 3.5.8 기본값이다. 근거: tuning/append-tuning-test.md §8
"""
import math

MiB = 1024 * 1024

# ── 변경 전 ────────────────────────────────────────────────────────────
MAX_PARTITION_BYTES_MB = 128
EXECUTOR_CORES_BEFORE = 4
PARALLELISM_FACTOR = 1.5


def num_executors_before(file_sizes: list[int]) -> int:
    total_mb = sum(file_sizes) / MiB
    total_partitions = math.ceil(total_mb / MAX_PARTITION_BYTES_MB)
    return max(math.ceil(total_partitions * PARALLELISM_FACTOR / EXECUTOR_CORES_BEFORE), 1)


# ── 변경 후 ────────────────────────────────────────────────────────────
MAX_PARTITION_BYTES = 128 * MiB   # spark.sql.files.maxPartitionBytes 기본값
OPEN_COST_BYTES = 4 * MiB         # spark.sql.files.openCostInBytes 기본값
READ_WAVES = 2                    # 읽기 task를 몇 차례에 처리할지. 2 = 속도 우선 (사용자 선택)
EXECUTOR_CORES = 8                # executor당 core. memory 6g, memoryOverhead 2g와 한 묶음
MAX_TOTAL_CORES = 128             # 재처리·지연 같은 큰 batch 상한


def spark_read_tasks(file_sizes: list[int]) -> int:
    """Spark 3.5 FilePartition.getFilePartitions 규칙과 같은 avro 읽기 task 수.

    - 큰 파일부터 채운다
    - 넣을지 판단할 때는 파일의 실제 크기만 더해 128MiB를 넘는지 본다
    - 넣은 뒤 누적할 때는 실제 크기 + 4MiB(openCostInBytes)를 더한다
    로컬 Spark 3.5.8에서 파일 2,690개로 확인: 이 함수 114 = Spark 실제 114
    전제: 총 core < 읽기 task 수 (그보다 core가 많으면 Spark가 128MiB보다 작게 쪼갠다)
    """
    tasks, current = 0, 0
    for size in sorted(file_sizes, reverse=True):
        if current + size > MAX_PARTITION_BYTES and current > 0:
            tasks += 1
            current = 0
        current += size + OPEN_COST_BYTES
    return tasks + (1 if current > 0 else 0)


def num_executors_after(file_sizes: list[int]) -> int:
    read_tasks = spark_read_tasks(file_sizes)
    total_cores = min(math.ceil(read_tasks / READ_WAVES), MAX_TOTAL_CORES)
    return max(math.ceil(total_cores / EXECUTOR_CORES), 1)


if __name__ == "__main__":
    # 1번 테이블 테스트 batch: 파일 2,690개, 4,180,959,271 bytes (평균 1.48MiB)
    sizes = [4_180_959_271 // 2690] * 2690
    print("before:", num_executors_before(sizes), "x", EXECUTOR_CORES_BEFORE, "core")   # 12 x 4 = 48
    print("read tasks:", spark_read_tasks(sizes))                                        # 113 (파일 크기가 제각각이면 ±1)
    print("after :", num_executors_after(sizes), "x", EXECUTOR_CORES, "core")           # 8 x 8 = 64
