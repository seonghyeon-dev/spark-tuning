# Spark Job 설정 튜닝 가이드

## 문서 정보

| 항목 | 내용 |
|------|------|
| 작성 목적 | Spark Job의 리소스 및 성능 설정에 대한 근거 기반 가이드 |
| 대상 독자 | 데이터 엔지니어, 운영팀 |
| 환경 | Kubernetes 클러스터, S3(MinIO), Spark 4.1.1, Iceberg 1.10.1, Airflow 3.1.7 |
| 최종 수정일 | 2026-03-09 |

---

## 1. 개요

### 1.1 워크플로우 요약

본 시스템은 Airflow DAG을 통해 avro 파일을 Iceberg 테이블에 append하는 배치 파이프라인이다.

```
Airflow DAG
    │
    ├── Task 1: get_jobs
    │   ├── Job History 테이블(Oracle DB)에서 처리 대상 조회 (상태: 대기 → in_progress)
    │   ├── num_executor 산정: ceil(총 크기 / 128MB × 1.5 / executor-cores)
    │   ├── avro 파일 경로 리스트를 텍스트 파일로 생성 → S3 업로드
    │   └── xcom에 메타데이터 저장 (콜백 참조용)
    │
    └── Task 2: append_data (SparkKubernetesOperator)
        ├── K8S에서 Spark Driver Pod 생성
        ├── Spark Job 내부 동작:
        │   ├── S3에서 avro 경로 목록 파일 읽기
        │   ├── 해당 경로의 avro 파일들을 S3에서 read
        │   └── Iceberg 테이블에 append
        └── 완료 후 콜백
            ├── on_success_callback: Job History 테이블 완료 처리
            └── on_failure_callback: Job History 테이블 실패 처리
```

**Spark Job 실행 환경**
- Spark Driver와 Executor가 각각 Kubernetes Pod으로 실행됨
- 데이터 저장소: S3(MinIO), Iceberg 테이블 형식
- Spark Operator: SparkKubernetesOperator (kubeflow)

### 1.2 이 문서의 범위

**다루는 것**
- Spark Job의 리소스 설정 (`driver-cores`, `driver-memory`, `executor-cores`, `executor-memory`, `num-executors`)
- Spark SQL 성능 설정 (`spark.sql.shuffle.partitions`, `spark.sql.adaptive.coalescePartitions.parallelismFirst`)
- 각 설정의 공식 문서 기반 설명, 기본값, 권장값, 근거
- SparkKubernetesOperator / spark-submit 설정 예시

**다루지 않는 것**
- Airflow DAG 로직 및 get_jobs Task 구현 상세
- Iceberg 테이블 파티셔닝/정렬 전략의 설계 근거 (별도 결정 예정)
- 애플리케이션 코드 (avro read / Iceberg write 로직)

### 1.3 대상 Iceberg 테이블 스키마

> **참고**: 테이블명과 컬럼명은 도메인 정보이므로 익명화된 이름을 사용한다.

**테이블: TABLE_A**

| 항목 | 값 |
|------|----|
| 컬럼 수 | 약 19개 |
| 데이터 타입 | string, double, integer, array, timestamp_ntz 등 |

**파티션 설정 (3개)**

| 파티션 | 타입 | 설명 |
|--------|------|------|
| `day(ts)` | timestamp_ntz | 일 단위 시간 파티션 (Iceberg hidden partition) |
| `column_a` | string | 비즈니스 키 파티션 |
| `column_b` | string | 비즈니스 키 파티션 |

**Write Ordering 설정 (3개, ALTER TABLE로 적용)**

```sql
ALTER TABLE TABLE_A WRITE ORDERED BY
  column_c ASC NULLS FIRST,
  column_d ASC NULLS FIRST,
  column_e ASC NULLS FIRST;
```

**Shuffle과의 관계**

이 파티션 설정과 write ordering이 **9.2GiB shuffle의 직접적 원인**이다:
- Iceberg의 `write.distribution-mode`(기본값: `hash`)에 의해, 쓰기 전 3개 파티션 키 기준으로 데이터가 재분배(shuffle)된다
- Write ordering에 의해 각 파티션 내에서 정렬이 수행된다

> ⚠️ **변동 가능성**
> 파티션 설정과 write ordering은 현재 확정된 값이 아니며, 다음 기준으로 최종 결정할 예정이다:
> 1. **하루치 데이터 적재 후 컴팩션 결과**: 데이터 파일 크기 분포 확인
> 2. **실제 조회 패턴**: 조건절(WHERE)에 사용되는 컬럼 기준으로 파티션/정렬 최적화
>
> 파티션/정렬 변경 시 shuffle 패턴이 달라지므로 벤치마크 재검증이 필요하다.

---

## 2. 리소스 설정

### 2.1 Driver 설정

#### 2.1.1 driver-cores

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.driver.cores` (spark-submit: `--driver-cores`) |
| **기본값** | `1` |
| **권장값** | `1` |
| 근거 수준 | 📘 일반적 관행 |

Driver는 쿼리 플래닝, 태스크 스케줄링, Iceberg 커밋만 수행하므로 CPU 집약적이지 않다. 1코어로 충분하다.

---

#### 2.1.2 driver-memory

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.driver.memory` (spark-submit: `--driver-memory`) |
| **기본값** | `1g` |
| **권장값** | `2g` |
| 근거 수준 | 📘 일반적 관행 |

사용자 코드에서 `collect()`로 전체 데이터를 Driver로 가져오는 작업은 없다. Iceberg 커밋 시 각 Executor가 작성한 파일 메타데이터(경로, 크기, 레코드 수 등)를 수집하는 내부 `collect()`가 발생하지만 (Spark History UI에 `collect at SparkWrite.java`로 표시), 이는 파일당 수백 바이트의 경량 데이터이므로 2g면 충분하다.

> **K8S 메모리 계산**
> Driver Pod 실제 메모리 = `driver-memory` + `memoryOverhead`
> `memoryOverhead` 기본값 = `driver-memory × 0.10` (최소 384MB)
> 예) 2g 설정 시 → Pod 메모리 ≈ 2g + 384MB ≈ 2.4g

---

### 2.2 Executor 설정

#### 2.2.1 executor-cores

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.executor.cores` (spark-submit: `--executor-cores`) |
| **기본값** | `1` (K8S/YARN), 전체 가용 코어 (Standalone) |
| **권장값** | `4` |
| 근거 수준 | 📘 일반적 관행 (Cloudera / Databricks 커뮤니티 가이드라인) |

| 코어 수 | 특성 |
|---------|------|
| 1코어 | 멀티스레딩 이점 없음. JVM 오버헤드 대비 처리량 낮음 |
| 2~4코어 | **권장 구간**. I/O throughput과 GC 오버헤드의 균형 |
| 5코어 초과 | S3/HDFS I/O 경합 증가. GC pause 시간 증가 |

4코어는 S3 기반 I/O 중심 워크로드에서 throughput과 GC 오버헤드의 균형점으로 널리 사용된다.

> **K8S 참고**: `spark.executor.cores`는 Spark 태스크 병렬성을 결정하고, `spark.kubernetes.executor.request.cores`는 Pod CPU request를 결정한다. 두 값은 동일하게 맞추는 것이 권장된다.

---

#### 2.2.2 executor-memory

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.executor.memory` (spark-submit: `--executor-memory`) |
| **기본값** | `1g` |
| **권장값** | `8g` (executor-cores=4 기준) |
| 근거 수준 | 📘 일반적 관행 |

일반적으로 **코어당 2~4GB** 할당이 권장된다:
- 4코어 × 2GB = 8g (I/O 중심, 캐싱 불필요)
- 4코어 × 4GB = 16g (복잡한 집계, join, 캐싱이 있는 경우)

avro → Iceberg append는 데이터를 메모리에 장기 보관하지 않으므로 8g로 시작하고 OOM 발생 시 늘린다.

> **K8S 메모리 계산**
> Executor Pod 실제 메모리 = `executor-memory` + `memoryOverhead`
> `memoryOverhead` 기본값 = `executor-memory × 0.10` (최소 384MB)
> 예) 8g 설정 시 → Pod 메모리 ≈ 8g + 819MB ≈ 8.8g

---

#### 2.2.3 num-executors

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.executor.instances` (spark-submit: `--num-executors`) |
| **기본값** | `2` |
| **권장값** | `ceil(총 크기/128MB × 1.5 / executor-cores)` (동적 산정) |
| 근거 수준 | ✅ 벤치마크 검증 완료 |

**산정식 (벤치마크 검증 완료)**

```python
import math

MAX_PARTITION_BYTES_MB = 128  # spark.sql.files.maxPartitionBytes 기본값
EXECUTOR_CORES = 4
PARALLELISM_FACTOR = 1.5      # 벤치마크 결과 최적값 (여유 50%)

total_partitions = math.ceil(total_avro_size_mb / MAX_PARTITION_BYTES_MB)
num_executors = math.ceil(total_partitions * PARALLELISM_FACTOR / EXECUTOR_CORES)
num_executors = max(num_executors, 1)
```

산정식의 의미:
1. `총 데이터 / 128MB` = Spark가 생성하는 읽기 파티션 수 (= 태스크 수)
2. `× PARALLELISM_FACTOR(1.5)` = 파이프라이닝 여유 50% 확보
3. `/ executor-cores(4)` = Executor 1개가 동시 처리하는 태스크 수 반영

> **참고: PARALLELISM_FACTOR란?**
> Spark 옵션이 아니라, num-executors를 계산하기 위해 만든 **산정식의 여유 계수(배수)**이다. 최소 Executor 수에 곱하여 파티션간 처리 시간 편차·S3 읽기 지연·shuffle 대기 등의 여유분을 확보한다. Airflow `get_jobs` task에서 사용하는 코드 상수이다.

**벤치마크 결과 (10분 주기 배치, ~8GB avro)**

| 케이스 | num-executors | 총 코어 수 | 소요시간 | 비고 |
|--------|---------------|-----------|---------|------|
| A | 16 | 64 | 55초 | 코어 부족으로 태스크 대기 발생 |
| **B** | **24** | **96** | **44초** | **✅ 최적** |
| C | 32 | 128 | 51초 | 오버헤드 발생 (scheduler 경합, shuffle 통신 증가) |
| D | 60 | 240 | 51초 | 과다 할당. 24개 대비 2.5배 리소스에도 성능 저하 |

> **결론**: 24개(PARALLELISM_FACTOR=1.5)가 최적. 32개 이상에서 성능이 나빠지는 이유는 executor 간 shuffle 통신 오버헤드, K8S Pod 스케줄링 경합, S3 동시 접속 부하 증가 때문이다.

---

## 3. 성능 설정

> **읽기 파티션 vs 셔플 파티션**
>
> | 구분 | 설정 | 기본값 | 적용 시점 |
> |------|------|--------|----------|
> | **읽기 파티션** | `spark.sql.files.maxPartitionBytes` | 128MB | `spark.read` 단계. 소파일들을 묶어 하나의 파티션으로 생성 |
> | **셔플 파티션** | `spark.sql.shuffle.partitions` | 200 | shuffle 발생 시 파티션 수 결정 |
>
> 이 워크플로우에서는 Iceberg의 `write.distribution-mode`에 의해 파티션 키 기준 데이터 재분배가 발생하며, 벤치마크에서 **9.2GiB 규모의 shuffle이 확인**되었다 (Stage 5→7, 1.3절 참고).

### 3.1 spark.sql.shuffle.partitions

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.sql.shuffle.partitions` |
| **기본값** | `200` |
| **권장값** | `200` (기본값 유지, 별도 설정 불필요) |
| 근거 수준 | ✅ 벤치마크 검증 완료 |

**AQE와의 관계**

Spark 4.1.1에서 AQE는 기본 활성화되어 있다. AQE의 `coalescePartitions` 기능이 shuffle 후 작은 파티션들을 자동 병합하므로, 기본값 200을 유지하고 AQE가 조정하도록 두는 것이 권장된다.

**벤치마크 결과**

| 케이스 | 값 | 소요시간 | 비고 |
|--------|-----|---------|------|
| A | 200 (기본값, 설정하지 않음) | **44초** | **✅ 최적** |
| B | 70 (명시적 설정) | 45~46초 | 1~2초 느림 |

기본값(200)이 더 빠른 이유: AQE가 200개에서 시작해 런타임에 자동 병합한다. 수동으로 70을 지정하면 AQE의 조정 폭이 제한되어, 데이터 크기가 변동되는 배치에서 불리하다.

> **참고: 기존 설정(70)의 배경**
> Hadoop 환경에서 워커 노드 수 기반으로 산정한 값이었다. K8S 환경에서는 워커 노드가 고정되지 않고 Pod 단위로 Executor가 동적 생성되므로, 워커 수 기반 산정은 의미가 없다. AQE에 맡기는 것이 K8S 환경에 적합하다.

**Stage별 Shuffle 분석:**

| Stage | 역할 | Input | Shuffle Write | Shuffle Read | Output |
|-------|------|-------|---------------|--------------|--------|
| 0 | collect (메타데이터) | 580.6KiB | 163.8KiB | - | - |
| 1 | collect (skipped) | - | - | - | - |
| 2 | collect (메타데이터) | - | - | 163.9KiB | - |
| 3 | 파일 목록 조회 (5355 paths) | - | - | - | - |
| 4 | append (avro 읽기) | 7.9GiB | - | - | - |
| 5 | append (shuffle 준비) | 7.9GiB | **9.2GiB** | - | - |
| 7 | append (Iceberg 쓰기) | - | - | **9.2GiB** | 6.5GiB |

---

### 3.2 spark.sql.adaptive.coalescePartitions.parallelismFirst

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.sql.adaptive.coalescePartitions.parallelismFirst` |
| **기본값** | `true` |
| **권장값** | `true` (기본값 유지, 별도 설정 불필요) |
| 근거 수준 | ✅ 벤치마크 검증 완료 |

**true vs false 동작 비교**

| 설정값 | 동작 | 결과 |
|-------|------|------|
| `true` | `minPartitionSize`(1MB)만 기준으로 병합 최소화 → 병렬성 최대화 | 소파일 다수 생성 가능 |
| `false` | `advisoryPartitionSizeInBytes`(64MB) 기준으로 적극 병합 | 파티션당 ~64MB로 최적화 |

**공식 문서 권장사항**

> *"It's recommended to set this config to false and respect the target size specified by `spark.sql.adaptive.advisoryPartitionSizeInBytes`."*
> — Spark 4.1.1 공식 문서

공식 문서는 `false`를 권장한다. `false`는 불필요하게 많은 소규모 태스크를 줄이고, shuffle 후 파티션 크기를 적정(64MB)으로 만들어 전체 효율을 높이는 것이 목적이다.

**벤치마크 결과**

| 케이스 | 값 | 소요시간 | 비고 |
|--------|-----|---------|------|
| A | `true` (기본값, 설정하지 않음) | **44초** | **✅ 최적 성능** |
| B | `false` (공식 권장) | 53~57초 | 약 10초 느림 (+23%) |

`false`가 느린 이유: avro → Iceberg append는 CPU 연산보다 **S3 I/O가 병목**인 워크로드이다. `false`로 파티션을 병합하면 태스크 수가 줄어 S3 동시 읽기/쓰기 병렬성이 저하된다. 공식 권장은 JOIN/집계 등 CPU 집약적 워크로드 기준이며, I/O 중심 append 워크로드에는 병렬성 유지가 더 유리하다.

**`true` 유지 결정 근거**

| 항목 | 판단 |
|------|------|
| 성능 차이 | 23% 저하(44초→55초)는 10분 주기 배치에서 유의미 |
| 소파일 문제 | Iceberg `rewrite_data_files` 컴팩션으로 해결 (시간당/일당 주기 운영 예정) |
| 업계 관행 | Iceberg/Delta + 컴팩션 운영 환경에서는 쓰기 성능 우선(`true`)이 일반적 |

> **참고: 일반적 관행**
>
> | 워크로드 유형 | 일반적 설정 | 이유 |
> |-------------|-----------|------|
> | 스트리밍/마이크로배치 append | `true` | 쓰기 성능 우선, 컴팩션으로 후처리 |
> | 대규모 ETL (JOIN/집계) | `false` | 태스크 효율 우선, shuffle 최적화 |
> | Iceberg/Delta + 컴팩션 운영 | `true` | 쓰기 레이턴시 우선, 읽기는 컴팩션이 보장 |

**관련 AQE 설정**

| 설정 | 기본값 | 설명 |
|------|--------|------|
| `spark.sql.adaptive.enabled` | `true` | AQE 전체 활성화 |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | 작은 파티션 자동 병합 |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | `true` | 병렬성 우선 여부 |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64MB` | 파티션 병합 시 목표 크기 |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | `1MB` | 병합 후 최소 파티션 크기 |

---

## 4. 종합 설정 예시

### 4.1 SparkKubernetesOperator 설정 예시

```python
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


def calculate_num_executors(total_avro_size_mb: float) -> int:
    """
    avro 파일들의 총 크기를 기반으로 num-executors를 산정한다.

    산정식: ceil(총 크기 / 128MB × 1.5 / executor-cores)
    ✅ 벤치마크 검증 완료 (24개 최적, PARALLELISM_FACTOR=1.5)
    """
    import math
    MAX_PARTITION_BYTES_MB = 128  # spark.sql.files.maxPartitionBytes 기본값
    EXECUTOR_CORES = 4
    PARALLELISM_FACTOR = 1.5     # 벤치마크 결과 최적값 (여유 50%)

    total_partitions = math.ceil(total_avro_size_mb / MAX_PARTITION_BYTES_MB)
    num_executors = math.ceil(total_partitions * PARALLELISM_FACTOR / EXECUTOR_CORES)
    return max(num_executors, 1)


SPARK_APPLICATION_TEMPLATE = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: avro-to-iceberg-{{ task_instance.task_id }}
  namespace: spark-jobs
spec:
  type: Python
  mode: cluster
  image: "your-registry/spark:4.1.1-iceberg-1.10.1"
  imagePullPolicy: Always
  mainApplicationFile: "s3a://your-bucket/spark-apps/avro_to_iceberg.py"

  arguments:
    - "{{ params.iceberg_table }}"
    - "{{ params.avro_list_s3_path }}"

  sparkConf:
    # ── 성능 설정 ──
    # shuffle.partitions(200), parallelismFirst(true): 기본값 유지 — 별도 설정 불필요
    # ✅ 벤치마크 결과: 기본값이 최적 성능 (44초)

    # ── Iceberg 카탈로그 설정 ──
    spark.sql.catalog.iceberg_catalog: "org.apache.iceberg.spark.SparkCatalog"
    spark.sql.catalog.iceberg_catalog.type: "hadoop"
    spark.sql.catalog.iceberg_catalog.warehouse: "s3a://your-bucket/warehouse"

    # ── S3 (MinIO) 설정 ──
    spark.hadoop.fs.s3a.endpoint: "http://minio:9000"
    spark.hadoop.fs.s3a.access.key: "{{ var.value.MINIO_ACCESS_KEY }}"
    spark.hadoop.fs.s3a.secret.key: "{{ var.value.MINIO_SECRET_KEY }}"
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"

  # ── Driver 설정 ──
  driver:
    cores: 1          # 📘 일반적 관행: 1코어
    memory: "2g"      # 📘 일반적 관행: 2GB
    serviceAccount: spark-sa
    labels:
      app: avro-to-iceberg

  # ── Executor 설정 ──
  executor:
    cores: 4          # 📘 일반적 관행: 4코어
    memory: "8g"      # 📘 일반적 관행: 코어당 ~2GB
    instances: "{{ params.num_executors }}"   # ✅ 동적 산정: ceil(총 크기/128MB × 1.5 / cores)
    labels:
      app: avro-to-iceberg

  restartPolicy:
    type: Never
"""


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="avro_to_iceberg_append",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["spark", "iceberg"],
) as dag:

    # Task 1: get_jobs (PythonOperator - 별도 구현)
    # - Oracle DB 조회, in_progress 상태 업데이트
    # - num_executors 산정 후 xcom 저장
    # - avro 경로 리스트 파일 S3 업로드

    # Task 2: append_data
    append_data = SparkKubernetesOperator(
        task_id="append_data",
        application_file=SPARK_APPLICATION_TEMPLATE,
        namespace="spark-jobs",
        kubernetes_conn_id="kubernetes_default",
        params={
            "iceberg_table": "{{ ti.xcom_pull(task_ids='get_jobs', key='iceberg_table') }}",
            "avro_list_s3_path": "{{ ti.xcom_pull(task_ids='get_jobs', key='avro_list_s3_path') }}",
            "num_executors": "{{ ti.xcom_pull(task_ids='get_jobs', key='num_executors') }}",
        },
        on_success_callback=on_success_handler,
        on_failure_callback=on_failure_handler,
    )
```

### 4.2 spark-submit 명령어 예시

```bash
spark-submit \
  --master k8s://https://<k8s-api-server>:6443 \
  --deploy-mode cluster \
  --name avro-to-iceberg \
  \
  # ── Driver 설정 ──
  --driver-cores 1 \
  --driver-memory 2g \
  \
  # ── Executor 설정 ──
  --executor-cores 4 \
  --executor-memory 8g \
  --num-executors 24 \
  \
  # ── 성능 설정: 기본값 유지 (별도 설정 불필요) ──
  # shuffle.partitions=200, parallelismFirst=true
  \
  # ── Iceberg 카탈로그 ──
  --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg_catalog.type=hadoop \
  --conf spark.sql.catalog.iceberg_catalog.warehouse=s3a://your-bucket/warehouse \
  \
  # ── S3 (MinIO) ──
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  \
  # ── K8S ──
  --conf spark.kubernetes.container.image=your-registry/spark:4.1.1-iceberg-1.10.1 \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa \
  \
  s3a://your-bucket/spark-apps/avro_to_iceberg.py \
  <iceberg_table_name> \
  <avro_list_s3_path>
```

---

## 5. 설정 근거 요약 및 벤치마크 결과

### 5.1 설정 요약

| 옵션 | 값 | 기본값 | 근거 수준 | 비고 |
|------|-----|--------|-----------|------|
| `driver-cores` | `1` | `1` | 📘 일반적 관행 | 플래닝·커밋 전용 |
| `driver-memory` | `2g` | `1g` | 📘 일반적 관행 | 경량 메타데이터 collect만 발생 |
| `executor-cores` | `4` | `1` | 📘 일반적 관행 | S3 I/O throughput과 GC 균형 |
| `executor-memory` | `8g` | `1g` | 📘 일반적 관행 | 코어당 2GB |
| `num-executors` | `ceil(총크기/128MB×1.5/cores)` | `2` | ✅ 벤치마크 검증 | 24개 최적 |
| `shuffle.partitions` | `200` (기본값) | `200` | ✅ 벤치마크 검증 | AQE 자동 조정에 맡김 |
| `parallelismFirst` | `true` (기본값) | `true` | ✅ 벤치마크 검증 | I/O 중심 워크로드에 적합. 소파일은 컴팩션으로 해결 |

### 5.2 벤치마크 테스트 환경

**테스트 데이터 (10분 주기 배치 기준)**

| 항목 | 값 |
|------|----|
| Avro 파일 수 | 5,355개 (파일당 평균 ~1.58MB) |
| Avro 총 크기 | ~8.08GB |
| Parquet 총 크기 (Iceberg 적재 후) | ~6.54GB (압축률 ~82.9%) |
| 총 Row 수 | 628,699건 |
| 추정 읽기 파티션 수 | ~63개 (`8,080MB / 128MB`) |

**테스트 조건**: 모든 벤치마크에서 `shuffle.partitions`와 `parallelismFirst`는 별도 설정하지 않고 Spark 기본값(200, true)으로 진행.

### 5.3 벤치마크 결과 요약

| 테스트 | 결과 | 결론 |
|--------|------|------|
| num-executors (16/24/32/60) | 24개에서 44초 최적 | `PARALLELISM_FACTOR=1.5` 확정 |
| shuffle.partitions (200 vs 70) | 200(기본값)이 1~2초 빠름 | 기본값 유지. AQE 자동 조정이 수동 설정보다 유리 |
| parallelismFirst (true vs false) | true(기본값)가 10초 빠름 | 기본값 유지. 소파일은 컴팩션으로 해결 |

### 5.4 향후 재검증 필요 사항

```
⚠️ 파티션/write ordering 최종 확정 후:
   → shuffle 패턴이 달라지므로 num-executors·shuffle.partitions 재검증
```

### 5.5 모니터링 지표 체크리스트

| 지표 | 확인 위치 | 목표 |
|------|----------|------|
| GC 시간 비율 | Spark UI → Executors 탭 | 전체 실행 시간의 5% 미만 |
| Shuffle Read/Write 크기 | Spark UI → Stages 탭 | 파티션당 100~200MB 내외 |
| Task 소요시간 분포 | Spark UI → Stages 탭 | 최대/최소 편차 3배 이내 (스큐 없음) |
| Executor 메모리 사용률 | Spark UI → Executors 탭 | 80% 미만 (OOM 여유) |
| Pod CPU 사용률 | K8S Metrics (Grafana 등) | request 대비 70~90% |

---

## 6. 참고 자료

- [Spark 4.1.1 Configuration](https://spark.apache.org/docs/4.1.1/configuration.html)
- [Spark 4.1.1 SQL Performance Tuning (AQE 포함)](https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html)
- [Spark on Kubernetes](https://spark.apache.org/docs/4.1.1/running-on-kubernetes.html)
- [Iceberg 1.10.1 Spark Configuration](https://iceberg.apache.org/docs/1.10.1/spark-configuration/)
- Cloudera / Databricks 커뮤니티: executor-cores 4~5 권장 가이드라인
