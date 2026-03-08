# Spark Job 설정 튜닝 가이드

## 문서 정보

| 항목 | 내용 |
|------|------|
| 작성 목적 | Spark Job의 리소스 및 성능 설정에 대한 근거 기반 가이드 |
| 대상 독자 | 데이터 엔지니어, 운영팀 |
| 환경 | Kubernetes 클러스터, S3(MinIO), Spark 4.1.1, Iceberg 1.10.1, Airflow 3.1.7 |
| 최종 수정일 | 2026-03-08 |

---

## 1. 개요

### 1.1 워크플로우 요약

본 시스템은 Airflow DAG을 통해 avro 파일을 Iceberg 테이블에 append하는 배치 파이프라인이다.

```
Oracle DB (처리 대상 관리)
    │
    ▼
Airflow DAG
    │
    ├── Task 1: get_jobs
    │   ├── Oracle DB에서 처리 대상 조회 (상태: 대기 → in_progress)
    │   ├── num_executor 산정 (avro 파일 총 크기 / 70MB + 1)
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
            ├── on_success: Oracle DB 완료 처리
            └── on_fail: Oracle DB 실패 처리
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
- Iceberg 테이블 스키마 및 파티셔닝 전략
- 애플리케이션 코드 (avro read / Iceberg write 로직)

---

## 2. 리소스 설정

### 2.1 Driver 설정

#### 2.1.1 driver-cores

**설명**

Driver 프로세스에 할당할 CPU 코어 수. Cluster 모드에서만 적용되며, 쿼리 플래닝, 태스크 스케줄링, 실행 조율을 담당한다.

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.driver.cores` (spark-submit: `--driver-cores`) |
| **기본값** | `1` |
| **권장값** | `1` |
| 근거 수준 | 📘 일반적 관행 |

**왜 이 값인가?**

Driver는 데이터를 직접 처리하지 않고 다음 역할만 수행한다:
- 쿼리 실행 계획(DAG) 생성
- Executor 태스크 스케줄링
- Iceberg 테이블 커밋 (manifest 파일 작성 등)

이 작업들은 CPU 집약적이지 않으므로 1코어로 충분하다. 다만 Iceberg 커밋 시 대량의 파일 메타데이터를 처리하거나 동시 DAG 실행이 잦은 경우 2코어로 늘리는 것을 검토할 수 있다.

**우리 워크플로우에서의 의미**

avro → Iceberg append 작업에서 Driver는 avro 파일 목록을 읽어 쿼리 플랜을 생성하고 Iceberg 커밋을 수행한다. 데이터 처리 자체는 Executor가 담당하므로 Driver는 1코어면 충분하다.

---

#### 2.1.2 driver-memory

**설명**

Driver 프로세스에 할당할 메모리. SparkContext가 초기화되는 곳으로, Spark 설정 파일이나 `--driver-memory` 옵션으로만 지정 가능하다 (프로그래밍적으로 SparkConf에서 설정 불가).

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.driver.memory` (spark-submit: `--driver-memory`) |
| **기본값** | `1g` |
| **권장값** | `2g` |
| 근거 수준 | 📘 일반적 관행 |

**왜 이 값인가?**

`collect()`, `broadcast join` 등 대용량 데이터를 Driver 메모리로 가져오는 작업이 있으면 수십 GB가 필요하다. 그러나 단순 ETL(avro → Iceberg append)에서는 Driver에 데이터가 집중되지 않으므로 2g면 충분하다.

> **K8S 메모리 계산**
> Driver Pod 실제 메모리 = `driver-memory` + `memoryOverhead`
> `memoryOverhead` 기본값 = `driver-memory × 0.10` (최소 384MB)
> 예) 2g 설정 시 → Pod 메모리 ≈ 2g + 384MB ≈ 2.4g

**우리 워크플로우에서의 의미**

`collect()` 없이 append만 수행하므로 Driver가 대용량 데이터를 메모리에 올릴 일이 없다. Iceberg 커밋 시 manifest 목록을 메모리에 보관하지만, 파일 수가 수만 개 이상인 극단적인 배치가 아니면 2g로 충분하다.

---

### 2.2 Executor 설정

#### 2.2.1 executor-cores

**설명**

각 Executor에 할당할 CPU 코어 수. 이 값이 곧 Executor 1개가 동시에 처리할 수 있는 태스크(파티션) 수이다.

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.executor.cores` (spark-submit: `--executor-cores`) |
| **기본값** | `1` (K8S/YARN), 전체 가용 코어 (Standalone) |
| **권장값** | `4` |
| 근거 수준 | 📘 일반적 관행 (Cloudera / Databricks 커뮤니티 가이드라인) |

**왜 이 값인가?**

Executor 코어 수에 따른 트레이드오프:

| 코어 수 | 문제점 |
|---------|--------|
| 1코어 | 멀티스레딩 이점 없음. Executor JVM 오버헤드 대비 처리량 낮음 |
| 2~4코어 | **권장 구간**. I/O throughput과 GC 오버헤드의 균형 |
| 5코어 초과 | S3/HDFS 동시 I/O 경합 증가. JVM GC pause 시간 증가 (힙이 커질수록) |

4코어는 S3 기반 I/O 중심 워크로드에서 throughput과 GC 오버헤드의 균형점으로 널리 사용된다.

> **K8S 참고**: `spark.executor.cores`는 Spark 태스크 병렬성을 결정하고, `spark.kubernetes.executor.request.cores`는 Pod CPU request를 결정한다. 두 값은 동일하게 맞추는 것이 권장된다.

**우리 워크플로우에서의 의미**

avro read → Iceberg write는 I/O 중심 작업이다. 4코어 설정 시 Executor 1개가 4개의 avro 파티션을 동시 처리하여 S3 throughput을 최대화한다.

---

#### 2.2.2 executor-memory

**설명**

각 Executor 프로세스에 할당할 메모리. 설정한 메모리 중 일정 비율이 실행/저장 영역으로 사용된다.

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.executor.memory` (spark-submit: `--executor-memory`) |
| **기본값** | `1g` |
| **권장값** | `8g` (executor-cores=4 기준) |
| 근거 수준 | 📘 일반적 관행 |

**왜 이 값인가?**

일반적으로 **코어당 2~4GB** 할당이 권장된다:
- 4코어 × 2GB = 8g (I/O 중심 작업, 메모리 캐싱 불필요한 경우)
- 4코어 × 4GB = 16g (복잡한 집계, join, 캐싱이 있는 경우)

avro → Iceberg append는 데이터를 메모리에 장기 보관하지 않는 스트리밍성 처리이므로 8g로 시작하고 OOM 발생 시 늘리는 전략을 권장한다.

> **K8S 메모리 계산**
> Executor Pod 실제 메모리 = `executor-memory` + `memoryOverhead`
> `memoryOverhead` 기본값 = `executor-memory × 0.10` (최소 384MB)
> 예) 8g 설정 시 → Pod 메모리 ≈ 8g + 819MB ≈ 8.8g
>
> **Spark 내부 메모리 구조**
> - `spark.memory.fraction` (기본 0.6): executor-memory 중 실행/저장에 사용하는 비율
> - `spark.memory.storageFraction` (기본 0.5): 위 비율 중 캐시 저장에 사용하는 비율

**우리 워크플로우에서의 의미**

avro 파일 하나의 크기(약 수십~수백 MB)와 executor-cores(4)를 곱한 크기가 동시 처리 데이터양이다. avro는 압축되어 있어 읽을 때 메모리에서 압축 해제되므로 실제 크기보다 여유 있게 설정한다.

---

#### 2.2.3 num-executors

**설명**

Static Allocation 시 실행할 Executor의 수. 이 값이 클러스터에서 동시에 작동하는 병렬 처리 단위 수를 결정한다.

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.executor.instances` (spark-submit: `--num-executors`) |
| **기본값** | `2` |
| **현재 설정값** | `avro 파일 총 크기(MB) / 70 + 1` (동적 산정) |
| 근거 수준 | ⚠️ 테스트 기반 (개선 필요) |

**현재 산정 로직**

```python
PARTITION_SIZE_MB = 70  # ⚠️ 테스트 기반 값
num_executors = int(total_avro_size_mb / PARTITION_SIZE_MB) + 1
```

예) avro 총 350MB → num_executors = 350/70 + 1 = **6**

**일반적인 num-executors 산정 방법론**

| 방법 | 공식 | 특징 |
|------|------|------|
| 데이터 기반 | `총 데이터 크기 / 타겟 파티션 크기 / executor-cores` | 데이터 크기에 비례. 변동성 큼 |
| 클러스터 기반 | `(총 클러스터 코어 - 예약) / executor-cores` | 클러스터 자원 최대 활용 |
| 경험적 | `파티션 수 = executor 수 × executor-cores × 2~3` | 태스크 수가 코어 총합의 2~3배 |

현재 방식(70MB 기준)은 Spark 기본 파티션 크기(`spark.sql.files.maxPartitionBytes` = 128MB)보다 작은 기준을 사용하고 있으며, 이론적 근거가 명확하지 않다.

> ⚠️ **개선 필요**
> 70MB 분할 기준은 테스트를 통해 적절한 성능이 확인되어 결정한 값이며, 이론적/실험적 근거가 부족합니다.
>
> **개선 방향**:
> - **벤치마크 방법**: 동일 데이터셋에 대해 70MB / 100MB / 128MB 기준으로 각각 실행 후 소요 시간, Executor 활용률 비교
> - **고려 변수**: avro 파일 평균 크기, 압축률(압축 해제 후 실제 크기), executor-cores, K8S 노드 가용 자원
> - **참고 기준**: `spark.sql.files.maxPartitionBytes`(기본 128MB)에 맞춰 `avro 총 크기 / 128MB + 1`로 변경 검토
> - **장기적**: Dynamic Allocation(`spark.dynamicAllocation.enabled=true`) 도입으로 워크로드에 따른 자동 조정 고려

---

## 3. 성능 설정

### 3.1 spark.sql.shuffle.partitions

**설명**

JOIN이나 집계(aggregation) 등 shuffle 연산 시 생성할 파티션 수. AQE(Adaptive Query Execution)가 활성화된 경우 이 값은 초기값으로만 사용되며 런타임에 자동 조정된다.

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.sql.shuffle.partitions` |
| **기본값** | `200` |
| **현재 설정값** | `70` |
| 근거 수준 | ⚠️ 테스트 기반 (개선 필요) |

**기본값 200과의 비교**

| 파티션 수 | 특징 | 적합한 상황 |
|----------|------|-----------|
| 너무 적음 | 파티션당 데이터 과다 → OOM, shuffle spill 위험 | - |
| **적정** | 코어 총합의 2~3배. 파티션당 100~200MB | 일반 상황 |
| 너무 많음 | 소규모 태스크 오버헤드 증가 | - |

**AQE와의 관계**

Spark 3.2+ (Spark 4.1.1 포함)에서 AQE는 기본 활성화(`spark.sql.adaptive.enabled=true`)되어 있다. AQE의 `coalescePartitions` 기능이 shuffle 후 작은 파티션들을 자동으로 병합하므로, shuffle.partitions를 충분히 크게 설정하고 AQE가 조정하도록 두는 방법도 권장된다.

> ⚠️ **개선 필요**
> 현재 70으로 설정되어 있으나 명확한 근거가 부족합니다. avro → Iceberg append에서 shuffle이 발생하는 시점(Iceberg 파티셔닝 컬럼 기준 데이터 분배 시)과 실제 shuffle 데이터 크기를 측정한 후 적절한 값을 도출해야 합니다.
>
> **개선 방향**:
> - **측정**: Spark UI의 Stages 탭에서 실제 shuffle read/write 크기 확인
> - **산정 기준**: `총 shuffle 데이터 크기(MB) / 100~200 = 적정 파티션 수` 공식 적용
> - **executor와의 관계**: `executor 수(N) × executor-cores(4) × 2 = 적정 파티션 수` 기준으로도 검토
> - **AQE 활용**: `spark.sql.adaptive.enabled=true` 상태에서 기본값 200을 유지하고 AQE 자동 조정 허용 검토

---

### 3.2 spark.sql.adaptive.coalescePartitions.parallelismFirst

**설명**

AQE 파티션 병합 시 병렬성을 우선할지 파티션 크기 최적화를 우선할지 결정하는 설정이다.

| 항목 | 값 |
|------|----|
| 설정 키 | `spark.sql.adaptive.coalescePartitions.parallelismFirst` |
| **기본값** | `true` |
| **현재 설정값** | `true` |
| 근거 수준 | ✅ 공식 권장 방향 확인 필요 (아래 설명 참고) |

**true vs false 동작 비교**

| 설정값 | 동작 | 파티션 수 | 파일 크기 |
|-------|------|---------|---------|
| `true` (현재) | `advisoryPartitionSizeInBytes`(64MB)를 무시. `minPartitionSize`(1MB)만 기준으로 병합 최소화 → 병렬성 최대화 | 많이 유지됨 | 작은 파일 다수 생성 가능 |
| `false` | `advisoryPartitionSizeInBytes`(64MB) 기준으로 파티션 병합 | 줄어듦 | 파티션당 ~64MB로 최적화 |

**공식 문서 권장사항**

> *"It's recommended to set this config to false and respect the target size specified by `spark.sql.adaptive.advisoryPartitionSizeInBytes`."*
> — Spark 4.1.1 공식 문서

공식 문서는 `false` 사용을 권장한다. 현재 `true`로 설정된 것은 성능 회귀(regression) 방지 목적으로 초기값을 유지한 것으로 보인다.

**관련 AQE 설정 전체**

| 설정 | 기본값 | 설명 |
|------|--------|------|
| `spark.sql.adaptive.enabled` | `true` | AQE 전체 활성화 (Spark 3.2+ 기본값) |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | 작은 파티션 자동 병합 활성화 |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | `true` | 병렬성 우선 여부 |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64MB` | 파티션 병합 시 목표 크기 |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | `1MB` | 병합 후 최소 파티션 크기 |

**우리 워크플로우에서의 의미**

`parallelismFirst=true`(현재)로 유지하면 Iceberg에 쓰이는 파일 수가 늘어나 **소파일(small file) 문제**가 발생할 수 있다. Iceberg는 소파일이 많아질수록 읽기 성능이 저하되므로, `false`로 변경하여 파티션당 ~64MB 크기의 파일로 병합되도록 하는 것이 Iceberg 테이블 품질 측면에서 유리하다.

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

    현재 기준: total_size / 70MB + 1
    ⚠️ 70MB 기준은 테스트 기반 값. 향후 벤치마크로 재검증 필요.
    """
    PARTITION_SIZE_MB = 70  # ⚠️ 테스트 기반 값. 개선 필요.
    num_executors = int(total_avro_size_mb / PARTITION_SIZE_MB) + 1
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
    spark.sql.shuffle.partitions: "70"                                     # ⚠️ 테스트 기반. 개선 필요
    spark.sql.adaptive.enabled: "true"                                     # AQE 활성화 (기본값 유지)
    spark.sql.adaptive.coalescePartitions.enabled: "true"                  # AQE 파티션 병합 활성화 (기본값 유지)
    spark.sql.adaptive.coalescePartitions.parallelismFirst: "true"         # 병렬성 우선 (공식 권장: false)

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
    cores: 1          # 📘 일반적 관행: 1코어 (쿼리 플래닝, Iceberg 커밋 담당)
    memory: "2g"      # 📘 일반적 관행: 2GB (collect 없는 ETL에 충분)
    serviceAccount: spark-sa
    labels:
      app: avro-to-iceberg

  # ── Executor 설정 ──
  executor:
    cores: 4          # 📘 일반적 관행: 4코어 (S3 I/O throughput과 GC 균형)
    memory: "8g"      # 📘 일반적 관행: 코어당 ~2GB (= 4 × 2GB)
    instances: "{{ params.num_executors }}"   # ⚠️ 동적 산정: avro 총 크기 / 70MB + 1
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
  --num-executors 5 \
  \
  # ── 성능 설정 ──
  --conf spark.sql.shuffle.partitions=70 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=true \
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

## 5. 설정 근거 요약 및 개선 로드맵

### 5.1 현재 설정 근거 수준 요약

| 옵션 | 현재 값 | 기본값 | 근거 수준 | 비고 |
|------|---------|--------|-----------|------|
| `driver-cores` | `1` | `1` | 📘 일반적 관행 | Driver 역할(플래닝·커밋)에 충분 |
| `driver-memory` | `2g` | `1g` | 📘 일반적 관행 | collect 없는 ETL에 적합 |
| `executor-cores` | `4` | `1` | 📘 일반적 관행 | S3 I/O throughput과 GC 균형점 |
| `executor-memory` | `8g` | `1g` | 📘 일반적 관행 | 코어당 2GB 기준 |
| `num-executors` | `총 크기/70MB+1` | `2` | ⚠️ 테스트 기반 | 70MB 기준 이론적 근거 부족 |
| `spark.sql.shuffle.partitions` | `70` | `200` | ⚠️ 테스트 기반 | 기본값 대비 낮게 설정된 근거 부족 |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | `true` | `true` | ✅ 공식 문서 확인 | 공식 권장은 `false`. 현재 기본값 유지 중 |

### 5.2 개선 로드맵

#### P1 (높음): num-executors 분할 기준 재검증

**목표**: 70MB 기준을 이론적으로 근거 있는 값으로 대체

**방법**:
1. 동일 avro 데이터셋(예: 350MB)에 대해 분할 기준별 비교 실행
   - 70MB 기준 (현재): num_executors = 6
   - 100MB 기준: num_executors = 4
   - 128MB 기준 (`maxPartitionBytes` 기본값): num_executors = 3
2. 각 케이스별 측정 지표: 총 소요 시간, Executor CPU 활용률, S3 I/O 처리량
3. Spark UI → Stages 탭에서 태스크 분포 및 스큐 확인

**참고**: `spark.sql.files.maxPartitionBytes`(기본 128MB)를 기준으로 맞추는 것이 이론적으로 가장 자연스러운 출발점이다.

---

#### P2 (중간): shuffle.partitions 최적값 검증

**목표**: 70의 근거를 확보하거나 더 적절한 값으로 변경

**방법**:
1. Spark UI → Stages 탭에서 실제 shuffle read 크기 측정
2. 공식: `총 shuffle 데이터 크기(MB) / 100~200 = 적정 파티션 수`
3. 또는 AQE에 위임: `parallelismFirst=false`로 전환하고 AQE가 자동 조정하도록 설정
4. `executor 수 × executor-cores × 2` = 적정 파티션 수 기준으로도 검토

---

#### P3 (낮음): parallelismFirst=false 전환 검토

**목표**: 공식 권장에 따라 Iceberg 소파일 문제 개선

**방법**:
1. `parallelismFirst=false`로 변경 후 동일 배치 실행
2. Iceberg 테이블에 생성된 파일 수와 평균 파일 크기 비교
3. 이후 쿼리 성능 비교 (소파일 감소 효과 측정)

**기대 효과**: Iceberg 테이블 쓰기 시 파티션당 ~64MB(`advisoryPartitionSizeInBytes` 기본값)로 병합되어 소파일 문제 감소

---

#### 모니터링 지표 체크리스트

Spark UI 및 K8S 메트릭에서 아래 항목을 확인한다:

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
- Cloudera / Databricks 커뮤니티: executor-cores 4~5 권장 가이드라인 (executor-cores 관행 근거)
