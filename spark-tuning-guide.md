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
Oracle DB (처리 대상 관리)
    │
    ▼
Airflow DAG
    │
    ├── Task 1: get_jobs
    │   ├── Oracle DB에서 처리 대상 조회 (상태: 대기 → in_progress)
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

`collect()`, `broadcast join` 등 대용량 데이터를 Driver 메모리로 가져오는 작업이 있으면 수십 GB가 필요하다. 단순 ETL(avro → Iceberg append)에서는 사용자 코드 수준의 `collect()`는 없으나, Iceberg 커밋 시 각 Executor의 파일 메타데이터를 수집하는 내부 `collect()`가 발생한다. 이는 경량 데이터이므로 Driver 메모리 부담이 거의 없어 2g면 충분하다.

> **K8S 메모리 계산**
> Driver Pod 실제 메모리 = `driver-memory` + `memoryOverhead`
> `memoryOverhead` 기본값 = `driver-memory × 0.10` (최소 384MB)
> 예) 2g 설정 시 → Pod 메모리 ≈ 2g + 384MB ≈ 2.4g

**우리 워크플로우에서의 의미**

사용자 코드에서 `collect()`로 전체 데이터를 Driver로 가져오는 작업은 없다. 다만 Iceberg 커밋 과정에서 각 Executor가 작성한 **파일 메타데이터**(경로, 크기, 레코드 수 등)를 Driver로 수집하는 내부 `collect()`가 발생한다 (Spark History UI에 `collect at SparkWrite.java`로 표시됨). 이는 실제 데이터가 아닌 경량 메타데이터(파일당 수백 바이트)이므로 Driver 메모리에 큰 부담이 없으며, 파일 수가 수만 개 이상인 극단적인 배치가 아니면 2g로 충분하다.

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
| **현재 설정값** | `ceil(총 크기/128MB × 1.5 / executor-cores)` (동적 산정) |
| 근거 수준 | ✅ 벤치마크 검증 완료 |

**현재 산정 로직 (기존)**

```python
PARTITION_SIZE_MB = 70  # ⚠️ 테스트 기반 값
num_executors = int(total_avro_size_mb / PARTITION_SIZE_MB) + 1
```

예) avro 총 350MB → num_executors = 350/70 + 1 = **6**

**기존 산정식의 문제점**

현재 방식은 다음 두 가지 문제가 있다:
1. **70MB 기준의 근거 부족**: Spark 기본 읽기 파티션 크기(`spark.sql.files.maxPartitionBytes` = 128MB)보다 작은 기준을 사용하며, 이론적 근거가 명확하지 않다.
2. **executor-cores 미반영**: Executor 1개가 4코어로 동시 4개 태스크를 처리한다는 점이 반영되지 않아, Executor를 과다 할당하게 된다.

**실제 테스트 결과 (10분 주기 배치 기준)**

| 항목 | 값 |
|------|----|
| Job 수 | 234개 |
| Avro 파일 수 | 5,355개 (파일당 평균 ~1.58MB) |
| Avro 총 크기 | ~8.08GB (8,474,665,536 bytes) |
| Parquet 총 크기 (Iceberg 적재 후) | ~6.54GB (7,023,859,378 bytes) |
| 압축률 (Parquet/Avro) | ~82.9% |
| 총 Row 수 | 628,699건 |
| 테스트 Executor 수 | 60개 (임의 설정) |
| 소요 시간 | 1.1분 |

> **참고: 소파일 특성**
> Avro 파일당 평균 1.58MB로 매우 작다. Spark는 `maxPartitionBytes`(128MB) 기준으로 소파일들을 하나의 파티션에 묶어 읽으므로, 파일 수(5,355개)가 곧 읽기 파티션 수는 아니다. 실제 읽기 파티션 수는 `8,080MB / 128MB ≈ 63개`로 추정된다.

기존 산정식 적용 시:
```
num_executors = 8,080 / 70 + 1 = 116  → 실제 필요량(~16~32) 대비 과다 할당
```

**개선 산정식 (벤치마크 검증 완료)**

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
2. `× PARALLELISM_FACTOR(1.5)` = 코어당 여유 50%를 두어 파이프라이닝 확보
3. `/ executor-cores(4)` = Executor 1개가 동시 처리하는 태스크 수 반영

> **참고: PARALLELISM_FACTOR란?**
> Spark 옵션이 아니라, num-executors를 계산하기 위해 만든 **산정식의 여유 계수(배수)**이다. 최소 Executor 수(= 읽기 파티션 / executor-cores)에 곱하여, 파티션간 처리 시간 편차·S3 읽기 지연·shuffle 대기 등의 여유분을 확보한다. Airflow `get_jobs` task에서 num-executors를 계산할 때 사용하는 코드 상수이다.

테스트 데이터 적용:
```
읽기 파티션 = ceil(8080 / 128) = 64
num_executors = ceil(64 × 1.5 / 4) = 24
```

| 산정 방식 | 결과 | 총 코어 수 | 비고 |
|----------|------|-----------|------|
| 기존 (70MB) | 116 | 464 | 과다 할당 |
| **개선 (128MB + cores + 1.5배)** | **24** | **96** | **✅ 벤치마크 최적값** |
| 최소 (1라운드 처리) | 16 | 64 | 태스크 수 = 코어 수 |

**벤치마크 결과 (10분 주기 배치, ~8GB avro)**

| 케이스 | num-executors | 총 코어 수 | 소요시간 | 비고 |
|--------|---------------|-----------|---------|------|
| A | 16 | 64 | 55초 | 코어 부족으로 태스크 대기 발생 |
| **B** | **24** | **96** | **44초** | **✅ 최적 (비용 대비 최고 성능)** |
| C | 32 | 128 | 51초 | 오버헤드 발생 (scheduler 경합, shuffle 통신 증가) |
| D | 60 | 240 | 51초 | 과다 할당 확인. 24개 대비 2.5배 리소스 사용하나 성능 저하 |

> **결론**: 24개(PARALLELISM_FACTOR=1.5)가 최적. 60개 → 24개로 줄이면 리소스 60% 절감 + 성능 13% 향상(51초→44초). 32개 이상에서 성능이 나빠지는 이유는 executor 간 shuffle 통신 오버헤드, K8S Pod 스케줄링 경합, S3 동시 접속 부하 증가 때문이다.
> - **장기적**: Dynamic Allocation(`spark.dynamicAllocation.enabled=true`) 도입으로 워크로드에 따른 자동 조정 고려

---

## 3. 성능 설정

> **읽기 파티션 vs 셔플 파티션**
> Spark에서 "파티션"은 문맥에 따라 다른 의미를 가진다:
>
> | 구분 | 설정 | 기본값 | 적용 시점 |
> |------|------|--------|----------|
> | **읽기 파티션** | `spark.sql.files.maxPartitionBytes` | 128MB | `spark.read` 단계. 소파일들을 묶어 하나의 파티션으로 생성 |
> | **셔플 파티션** | `spark.sql.shuffle.partitions` | 200 | JOIN, 집계, repartition 등 shuffle 발생 시 파티션 수 결정 |
>
> avro → Iceberg append 워크플로우에서 **읽기 파티션**은 항상 적용된다. **셔플 파티션**은 Iceberg의 `write.distribution-mode`에 의해 파티션 키 기준 데이터 재분배가 발생할 때 적용되며, 벤치마크에서 **9.2GiB 규모의 shuffle이 확인**되었다 (Stage 5→7).

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

> **✅ Shuffle 발생 확인됨 (벤치마크 결과)**
>
> Spark UI Stages 탭에서 **9.2GiB 규모의 shuffle**이 확인되었다. 따라서 이 옵션은 성능에 실질적 영향을 미친다.
>
> **Stage별 Shuffle 분석:**
>
> | Stage | 역할 | Input | Shuffle Write | Shuffle Read | Output |
> |-------|------|-------|---------------|--------------|--------|
> | 0 | collect (메타데이터) | 580.6KiB | 163.8KiB | - | - |
> | 1 | collect (skipped) | - | - | - | - |
> | 2 | collect (메타데이터) | - | - | 163.9KiB | - |
> | 3 | 파일 목록 조회 (5355 paths) | - | - | - | - |
> | 4 | append (avro 읽기) | 7.9GiB | - | - | - |
> | 5 | append (shuffle 준비) | 7.9GiB | **9.2GiB** | - | - |
> | 7 | append (Iceberg 쓰기) | - | - | **9.2GiB** | 6.5GiB |
>
> - Stage 0/2의 소량 shuffle(163.8KiB)은 Iceberg `collect()` 메타데이터 수집 관련으로 무시 가능
> - **Stage 5→7의 9.2GiB shuffle이 핵심**: Iceberg의 `write.distribution-mode`에 의해 파티션 키 기준으로 데이터 재분배가 발생
>
> ⚠️ **개선 필요 (벤치마크 대기)**
> 현재 70으로 설정되어 있으나 명확한 근거가 부족합니다. shuffle이 9.2GiB 규모로 발생하는 것이 확인되었으므로 아래 벤치마크가 필요합니다.
>
> **벤치마크 테스트 케이스**:
> | 케이스 | 값 | 비고 |
> |--------|-----|------|
> | A | 70 (현재) | 비교 기준선 |
> | B | 200 (기본값) | AQE가 자동 조정하도록 위임 |
>
> **산정 기준 참고**:
> - `총 shuffle 데이터 크기(MB) / 100~200 = 적정 파티션 수` → `9,420MB / 100~200 = 47~94`
> - `executor 수(24) × executor-cores(4) × 2 = 192`
> - 현재 70은 산정 기준 범위(47~94) 안에 있으나, AQE 자동 조정(200)과 비교 벤치마크 필요

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
    spark.sql.shuffle.partitions: "70"                                     # ⚠️ 테스트 기반. 9.2GiB shuffle 확인됨. 벤치마크 대기
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
    memory: "2g"      # 📘 일반적 관행: 2GB (Iceberg 내부 collect만 발생, 경량 메타데이터이므로 충분)
    serviceAccount: spark-sa
    labels:
      app: avro-to-iceberg

  # ── Executor 설정 ──
  executor:
    cores: 4          # 📘 일반적 관행: 4코어 (S3 I/O throughput과 GC 균형)
    memory: "8g"      # 📘 일반적 관행: 코어당 ~2GB (= 4 × 2GB)
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
| `driver-memory` | `2g` | `1g` | 📘 일반적 관행 | Iceberg 내부 collect(경량 메타데이터)만 발생. ETL에 적합 |
| `executor-cores` | `4` | `1` | 📘 일반적 관행 | S3 I/O throughput과 GC 균형점 |
| `executor-memory` | `8g` | `1g` | 📘 일반적 관행 | 코어당 2GB 기준 |
| `num-executors` | `ceil(총 크기/128MB×1.5/cores)` | `2` | ✅ 벤치마크 검증 완료 | 24개 최적 (기존 60개 대비 리소스 60% 절감, 성능 13% 향상) |
| `spark.sql.shuffle.partitions` | `70` | `200` | ⚠️ 테스트 기반 | 기본값 대비 낮게 설정된 근거 부족 |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | `true` | `true` | ✅ 공식 문서 확인 | 공식 권장은 `false`. 현재 기본값 유지 중 |

### 5.2 테스트 결과 데이터 프로파일

10분 주기 배치 기준 실측 데이터:

| 항목 | 값 |
|------|----|
| Job 수 | 234개 |
| Avro 파일 수 | 5,355개 (파일당 평균 ~1.58MB) |
| Avro 총 크기 | ~8.08GB (8,474,665,536 bytes) |
| Parquet 총 크기 (Iceberg 적재 후) | ~6.54GB (7,023,859,378 bytes) |
| 압축률 (Parquet/Avro) | ~82.9% |
| 총 Row 수 | 628,699건 (Row당 평균 ~13.5KB, Avro 기준) |
| 추정 읽기 파티션 수 | ~63개 (`8,080MB / 128MB`) |
| 테스트 Executor 수 | 60개 (임의 설정) |
| 소요 시간 | 1.1분 |

### 5.3 개선 로드맵

#### 테스트 1 (완료): num-executors 최적값 벤치마크 ✅

**목표**: 개선 산정식의 적정성 검증

**고정 옵션**:

| 옵션 | 값 | 근거 |
|------|-----|------|
| `driver-cores` | `1` | 플래닝/커밋 전용 |
| `driver-memory` | `2g` | 경량 메타데이터 collect만 발생 |
| `executor-cores` | `4` | I/O throughput과 GC 균형 |
| `executor-memory` | `8g` | 코어당 2GB 기준 |

**벤치마크 결과**:

| 케이스 | num-executors | 총 코어 수 | 소요시간 | 결과 |
|--------|---------------|-----------|---------|------|
| A | 16 | 64 | 55초 | 코어 부족 |
| **B** | **24** | **96** | **44초** | **✅ 최적** |
| C | 32 | 128 | 51초 | 오버헤드 발생 |
| D | 60 | 240 | 51초 | 과다 할당 확인 |

**결론**: `PARALLELISM_FACTOR=1.5`(24개)가 최적. 기존 60개 대비 리소스 60% 절감, 성능 13% 향상.

---

#### 테스트 2 (대기): shuffle.partitions 벤치마크

**선행 확인 완료**: Spark UI Stages 탭에서 **9.2GiB 규모의 shuffle 발생 확인** → 이 옵션은 성능에 영향을 미침

**테스트 케이스**:

| 케이스 | 값 | 비고 |
|--------|-----|------|
| A | 70 (현재) | 비교 기준선 |
| B | 200 (기본값) | AQE가 자동 조정하도록 위임 |

---

#### 테스트 3 (낮음): parallelismFirst=false 전환 검토

**목표**: 공식 권장에 따라 Iceberg 소파일 문제 개선

**테스트 케이스**:

| 케이스 | 값 | 효과 |
|--------|-----|------|
| A | `true` (현재) | 병렬성 우선. 소파일 다수 생성 가능 |
| B | `false` (공식 권장) | 파티션당 ~64MB로 병합. 소파일 감소 |

**확인 사항**: Iceberg 테이블에 생성된 parquet 파일 수와 평균 크기 비교

**기대 효과**: Iceberg 테이블 쓰기 시 파티션당 ~64MB(`advisoryPartitionSizeInBytes` 기본값)로 병합되어 소파일 문제 감소

---

#### 권장 테스트 순서

```
✅ 완료: Spark UI에서 shuffle 발생 확인 (9.2GiB)
✅ 완료: num-executors 테스트 → 24개 최적 (PARALLELISM_FACTOR=1.5)

다음 단계:
1단계: shuffle.partitions 벤치마크 (70 vs 200)
       → num-executors=24 고정, shuffle.partitions만 변경하여 비교

2단계: parallelismFirst 테스트 (true vs false)
       → Iceberg 파일 품질 비교
```

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
