# Spark Job 설정 튜닝 가이드

## 문서 정보

| 항목 | 내용 |
|------|------|
| 작성 목적 | Spark Job의 리소스 및 성능 설정에 대한 근거 기반 가이드 |
| 대상 독자 | 데이터 엔지니어, 운영팀 |
| 환경 | Kubernetes 클러스터, S3(MinIO), Spark 4.1.1, Iceberg 1.10.1, Airflow 3.1.7 |
| 최종 수정일 | 2026-03-16 |

### 근거 수준 라벨

| 라벨 | 의미 |
|------|------|
| ✅ 벤치마크 검증 | 실제 환경에서 벤치마크로 검증된 값 |
| 📘 일반적 관행 | 커뮤니티에서 널리 사용되는 값. 공식 문서 또는 업계 관행 기반 |
| ⚠️ 개선 필요 | 근거 부족. 벤치마크/프로파일링으로 재검증 필요 |

### 목차

- [1. 개요](#1-개요) — 워크플로우, 문서 범위, 테이블 스키마
- [2. 리소스 설정](#2-리소스-설정) — driver-cores/memory, executor-cores/memory, num-executors
- [3. 성능 설정](#3-성능-설정) — shuffle.partitions, parallelismFirst
- [4. 설정 근거 요약 및 벤치마크 결과](#4-설정-근거-요약-및-벤치마크-결과) — 요약표, 벤치마크, 모니터링, 트러블슈팅
- [5. 용어집](#5-용어집)
- [6. 참고 자료](#6-참고-자료)

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
- K8S 환경 리소스 요구사항

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
| `write.distribution-mode` | `range` |

**파티션 설정 (3개)**

| 파티션 | 타입 | 설명 |
|--------|------|------|
| `day(ts)` | timestamp_ntz | 일 단위 시간 파티션 (Iceberg hidden partition) |
| `par_a` | string | 비즈니스 키 파티션 |
| `par_b` | string | 비즈니스 키 파티션 |

**Write Ordering 설정 (3개, ALTER TABLE로 적용)**

```sql
ALTER TABLE TABLE_A WRITE ORDERED BY
  sort_a ASC NULLS FIRST,
  sort_b ASC NULLS FIRST,
  sort_c ASC NULLS FIRST;
```

**Shuffle과의 관계**

이 파티션 설정과 write ordering이 **9.2GiB shuffle의 직접적 원인**이다:
- Iceberg의 `write.distribution-mode`(`range`로 설정)에 의해, 쓰기 전 파티션 키 + write ordering 기준으로 데이터가 범위 기반 재분배(shuffle)된다
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
| 근거 수준 | 📘 일반적 관행 |

| 코어 수 | 특성 |
|---------|------|
| 1코어 | 멀티스레딩 이점 없음. JVM 오버헤드 대비 처리량 낮음 |
| 2~4코어 | **권장 구간**. I/O throughput과 GC 오버헤드의 균형 |
| 5코어 초과 | JVM GC pause 시간 증가 (힙이 커짐). S3 HTTP 커넥션 풀 경합 가능 |

4코어는 S3 기반 I/O 중심 워크로드에서 태스크 병렬성과 JVM GC 부담의 균형점으로 널리 사용된다.

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
> | **읽기 파티션** | `spark.sql.files.maxPartitionBytes` | 128MB | `spark.read` 단계. small file들을 묶어 하나의 파티션으로 생성 |
> | **셔플 파티션** | `spark.sql.shuffle.partitions` | 200 | shuffle 발생 시 파티션 수 결정 |
>
> 이 워크플로우에서는 Iceberg의 `write.distribution-mode=range`에 의해 파티션 키 + write ordering 기준 범위 기반 데이터 재분배가 발생하며, 벤치마크에서 **9.2GiB 규모의 shuffle이 확인**되었다 (Stage 5→7, 1.3절 참고).

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

벤치마크에서 기본값(200)이 최적으로 확인되었다. AQE가 200개에서 시작해 런타임에 자동 병합하므로, 별도 설정 없이 기본값을 유지하는 것이 가장 유리하다.

**Stage별 Shuffle 분석:**

| Stage | 역할 | Input | Shuffle Write | Shuffle Read | Output |
|-------|------|-------|---------------|--------------|--------|
| 0 | collect (메타데이터) | 580.6KiB | 163.8KiB | - | - |
| 1 | collect (skipped) | - | - | - | - |
| 2 | collect (메타데이터) | - | - | 163.9KiB | - |
| 3 | 파일 목록 조회 (5355 paths) | - | - | - | - |
| 4 | append (avro 읽기) | 7.9GiB | - | - | - |
| 5 | append (shuffle 준비) | 7.9GiB | **9.2GiB** | - | - |
| 6 | append (skipped) | - | - | - | - |
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
| `true` | `minPartitionSize`(1MB)만 기준으로 병합 최소화 → 병렬성 최대화 | small file 다수 생성 가능 |
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

**`false`가 느린 이유: AQE 병합에 의한 태스크 수 급감**

`false` 설정 시 Spark UI에서 확인된 동작:
- Stage 5 (shuffle 준비): **228개 태스크**
- Stage 7 (Iceberg 쓰기): **28개 태스크**로 감소, 태스크당 shuffle read 200~364MiB

`advisoryPartitionSizeInBytes`(64MB)는 인접한 작은 shuffle 파티션을 **병합할 때의 목표 크기**이다. 단, AQE는 이미 64MB를 초과하는 파티션을 **분할할 수는 없다**.

이 워크로드에서 Iceberg range 분배는 파티션 키 + write ordering 기준으로 데이터를 배치하므로, 키 조합 수(~28개)만큼의 shuffle 파티션에 데이터가 집중된다. 나머지 파티션은 비어 있다:

```
200개 shuffle 파티션:
[330MB] [빈] [빈] ... [280MB] [빈] ... [350MB] [빈] ...
  ↓ AQE coalesce (64MB 목표)
[330MB]                [280MB]          [350MB]
→ 이미 64MB 초과이므로 분할 불가 → 28개 태스크, 태스크당 200~364MB
```

결과적으로 228개 → 28개로 태스크가 줄면서 S3 동시 I/O 병렬성이 크게 저하된다. 이 워크로드는 CPU보다 S3 I/O가 병목이므로, 병렬성 감소가 곧 성능 저하로 이어진다.

**`true` 유지 결정 근거**

| 항목 | 판단 |
|------|------|
| 성능 차이 | 23% 저하(44초→55초)는 10분 주기 배치에서 유의미 |
| small file 문제 | Iceberg `rewrite_data_files` 컴팩션으로 해결 (시간당/일당 주기 운영 예정) |
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

## 4. 설정 근거 요약 및 벤치마크 결과

### 4.1 설정 요약

| 옵션 | 값 | 기본값 | 근거 수준 | 비고 |
|------|-----|--------|-----------|------|
| `driver-cores` | `1` | `1` | 📘 일반적 관행 | 플래닝·커밋 전용 |
| `driver-memory` | `2g` | `1g` | 📘 일반적 관행 | 경량 메타데이터 collect만 발생 |
| `executor-cores` | `4` | `1` | 📘 일반적 관행 | S3 I/O throughput과 GC 균형 |
| `executor-memory` | `8g` | `1g` | 📘 일반적 관행 | 코어당 2GB |
| `num-executors` | `ceil(총크기/128MB×1.5/cores)` | `2` | ✅ 벤치마크 검증 | 24개 최적 |
| `shuffle.partitions` | `200` (기본값) | `200` | ✅ 벤치마크 검증 | AQE 자동 조정에 맡김 |
| `parallelismFirst` | `true` (기본값) | `true` | ✅ 벤치마크 검증 | I/O 중심 워크로드에 적합. small file은 컴팩션으로 해결 |

### 4.2 벤치마크 테스트 환경

**테스트 데이터 (10분 주기 배치 기준)**

| 항목 | 값 |
|------|----|
| Avro 파일 수 | 5,355개 (파일당 평균 ~1.58MB) |
| Avro 총 크기 | ~8.08GB |
| Parquet 총 크기 (Iceberg 적재 후) | ~6.54GB (압축률 ~82.9%) |
| 총 Row 수 | 628,699건 |
| 추정 읽기 파티션 수 | ~63개 (`8,080MB / 128MB`) |

**테스트 조건**: 모든 벤치마크에서 `shuffle.partitions`와 `parallelismFirst`는 별도 설정하지 않고 Spark 기본값(200, true)으로 진행.

### 4.3 벤치마크 결과 요약

| 테스트 | 결과 | 결론 |
|--------|------|------|
| num-executors (16/24/32/60) | 24개에서 44초 최적 | `PARALLELISM_FACTOR=1.5` 확정 |
| shuffle.partitions | 기본값(200) 최적 | 기본값 유지. AQE 자동 조정에 맡김 |
| parallelismFirst (true vs false) | true(기본값)가 10초 빠름 | 기본값 유지. small file은 컴팩션으로 해결 |

### 4.4 K8S 클러스터 리소스 요구사항

벤치마크 최적 설정(num-executors=24) 기준, Spark Job 실행 시 필요한 K8S 리소스:

| 구성 요소 | 수량 | CPU (request) | 메모리 (request) |
|----------|------|--------------|-----------------|
| Driver Pod | 1개 | 1 core | 2g + 384MB = **2.4g** |
| Executor Pod | 24개 | 4 core × 24 = **96 core** | 8.8g × 24 = **211.2g** |
| **합계** | **25 Pod** | **97 core** | **≈ 213.6g** |

> **참고**: `memoryOverhead` 기본값(executor-memory × 0.10, 최소 384MB)이 포함된 값이다. K8S 노드 풀에 위 리소스를 수용할 수 있는 여유가 있어야 한다.

### 4.5 향후 재검증 필요 사항

```
⚠️ 파티션/write ordering 최종 확정 후:
   → shuffle 패턴이 달라지므로 num-executors·shuffle.partitions 재검증
```

### 4.6 모니터링 지표 체크리스트

| 지표 | 확인 위치 | 목표 |
|------|----------|------|
| GC 시간 비율 | Spark UI → Executors 탭 | 전체 실행 시간의 5% 미만 |
| Shuffle Read/Write 크기 | Spark UI → Stages 탭 | 파티션당 100~200MB 내외 |
| Task 소요시간 분포 | Spark UI → Stages 탭 | 최대/최소 편차 3배 이내 (스큐 없음) |
| Executor 메모리 사용률 | Spark UI → Executors 탭 | 80% 미만 (OOM 여유) |
| Pod CPU 사용률 | K8S Metrics (Grafana 등) | request 대비 70~90% |

### 4.7 트러블슈팅

| 증상 | 원인 | 대응 |
|------|------|------|
| Executor OOM (`OutOfMemoryError`) | executor-memory 부족 또는 데이터 스큐 | executor-memory를 8g → 12g → 16g 순으로 증가. Spark UI에서 태스크별 메모리 사용량 확인 |
| Driver OOM | Iceberg 메타데이터 collect 과다 (파일 수 급증) | driver-memory를 2g → 4g로 증가 |
| Shuffle 단계 느림 | 파티션 스큐 (특정 키에 데이터 집중) | Spark UI → Stages → shuffle read 크기 편차 확인. 파티션 키 재설계 검토 |
| S3 타임아웃 / 연결 오류 | MinIO 과부하 또는 동시 접속 초과 | `fs.s3a.connection.maximum` 확인 (기본 96). num-executors 줄여 동시 접속 감소 |
| Pod Pending (스케줄링 지연) | K8S 노드 리소스 부족 | 4.4절 리소스 요구사항 대비 노드 풀 용량 확인. num-executors 축소 검토 |
| 전체 소요시간 급증 | 데이터 볼륨 증가 | num-executors 산정식이 자동 반영. 산정 결과가 32 이상이면 executor-cores/memory 조합 재검토 |

---

## 5. 용어집

| 용어 | 정의 |
|------|------|
| **AQE** | Adaptive Query Execution. Spark 4.1.1 기본 활성화. 런타임에 shuffle 파티션 병합, 조인 전략 변경 등을 자동 수행 |
| **PARALLELISM_FACTOR** | num-executors 산정식의 여유 계수. Spark 옵션이 아닌 Airflow `get_jobs` task의 코드 상수 (현재 1.5) |
| **coalescePartitions** | AQE의 기능. shuffle 후 작은 파티션들을 자동으로 병합하여 태스크 수를 줄임 |
| **advisoryPartitionSizeInBytes** | AQE 파티션 병합 시 목표 크기 (기본 64MB). 이미 초과한 파티션은 분할 불가 |
| **write.distribution-mode** | Iceberg 쓰기 전 데이터 재분배 방식. `range`는 파티션 키 + write ordering 기준 범위 분배 |
| **memoryOverhead** | JVM 외부 메모리 (off-heap, 네이티브 라이브러리 등). K8S Pod 메모리 = executor-memory + memoryOverhead |
| **컴팩션** | Iceberg `rewrite_data_files` 프로시저. small file을 병합하여 읽기 성능 최적화 |

---

## 6. 참고 자료

- [Spark 4.1.1 Configuration](https://spark.apache.org/docs/4.1.1/configuration.html)
- [Spark 4.1.1 SQL Performance Tuning (AQE 포함)](https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html)
- [Spark on Kubernetes](https://spark.apache.org/docs/4.1.1/running-on-kubernetes.html)
- [Iceberg 1.10.1 Spark Configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)
