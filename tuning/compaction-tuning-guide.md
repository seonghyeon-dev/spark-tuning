# Iceberg Compaction 튜닝 가이드 (hourly)

## 문서 정보

| 항목 | 내용 |
|------|------|
| 작성 목적 | hourly Compaction Job의 Iceberg 옵션 및 Spark 리소스 설정에 대한 근거 기반 가이드 |
| 대상 독자 | 데이터 엔지니어, 운영팀 |
| 환경 | Kubernetes 클러스터, S3(MinIO), Spark 4.1.1, Iceberg 1.10.1, Airflow 3.2.2 |
| 대상 범위 | **hourly Compaction만.** daily Compaction은 별도 (섹션 8.1) |
| 최종 수정일 | 2026-08-13 |

### 근거 수준 라벨

| 라벨 | 의미 |
|------|------|
| ✅ 벤치마크 검증 | 실제 환경에서 벤치마크로 검증된 값 |
| 📘 일반적 관행 | 커뮤니티에서 널리 사용되는 값. 공식 문서 또는 업계 관행 기반 |
| ⚠️ 개선 필요 | 근거 부족. 벤치마크/프로파일링으로 재검증 필요 |

### 목차

- [1. 개요](#1-개요) — 범위, 대상 테이블, 초기 상태
- [2. Compaction 동작 원리](#2-compaction-동작-원리) — file group, 출력 파일 수 결정 방식
- [3. 옵션 및 설정 설명](#3-옵션-및-설정-설명) — Iceberg 옵션, Spark 설정, 리소스, filter
- [4. 최적화 과정](#4-최적화-과정) — 병목 분석, 테스트 결과 9회, min_size 원인, num-executors 캘리브레이션
- [5. 확정 설정](#5-확정-설정) — 최종 설정과 근거 요약
- [6. 동적 리소스 산정](#6-동적-리소스-산정) — num-executors 산정식
- [7. 모니터링 지표](#7-모니터링-지표) — DataFlint / Iceberg 메타데이터 / Spark UI
- [8. 미확정 및 후속 과제](#8-미확정-및-후속-과제)
- [9. 용어집](#9-용어집)
- [10. 참고 자료](#10-참고-자료)

---

## 1. 개요

### 1.1 이 문서의 범위

hourly Compaction Job(`rewrite_data_files`)의 **Iceberg 옵션**(`target-file-size-bytes`, `rewrite-all`, `max-concurrent-file-group-rewrites`, `max-file-group-size-bytes`), **Spark 설정**(`advisory-partition-size`, `coalescePartitions.parallelismFirst`), **리소스 설정**(driver/executor cpu·memory, num-executors)의 역할, 근거, 실측 결과를 다룬다.

append Job의 설정은 `tuning/spark-tuning-guide.md`에서 다룬다. 두 Job은 목적이 달라 일부 설정이 정반대로 잡히며, 그 이유는 섹션 3.2에서 설명한다.

### 1.2 대상 테이블

| 항목 | 값 |
|------|-----|
| 파티션 | `hour(ts)`, `col_a` |
| Sort Order | `col_b`, `col_c` (`WRITE ORDERED BY`) |
| `write.distribution-mode` | `range` |
| `write.target-file-size-bytes` | 512MB |
| hourly 그룹 테이블 수 | 4개 (파티션·Sort 설정 동일) |

Compaction 시 **`sort` 전략**을 사용한다 (테이블 Sort Order 설정 여부에 따라 분기 처리, 위 4개 테이블은 모두 sort 적용).

**`sort` 전략은 필수다.** `schema/read-performance-test.md` §5.4의 실측에서 Sort Order 미설정 시 조회 소요시간이 650ms → 1.1s로 **40% 증가**한다. `binpack` 전략은 shuffle과 정렬을 생략해 Compaction Job 자체는 빨라지지만, 조회 성능을 희생하므로 채택하지 않는다.

### 1.3 초기 상태 (튜닝 전)

측정 대상: 2026-08-11 13~14시 (1시간치)

**입력 (Compaction 전)**

| 항목 | 값 |
|------|-----|
| object 수 | 703개 |
| 총 크기 | 37.0GB |
| records | 3,387,634 |
| 파일 크기 | min 0.6MB / avg 53.9MB / **max 72.3MB** |

**입력 파일 703개가 전부 small file이다.** small file 기준은 `target-file-size-bytes × MIN_FILE_SIZE_DEFAULT_RATIO(0.75)` = **384MB**이며, 최대 파일이 72.3MB이므로 예외가 없다. 이 사실이 `rewrite-all` 설정 판단의 근거가 된다 (섹션 3.1).

**출력 (Compaction 후)**

| 항목 | 값 |
|------|-----|
| object 수 | 75개 (9.4배 감소) |
| 총 크기 | 37.0GB |
| 파일 크기 | min 408.4MB / avg 505.5MB / max 585.3MB |

이론 최솟값이 `37.0GB ÷ 512MB` = 74개이므로 **75개는 사실상 최적**이며, min 408.4MB > 384MB로 **잔여 small file이 없다.** 즉 **출력 품질은 튜닝 전에도 정상이며, 개선 여지는 소요시간과 리소스 효율에 있다.**

**파티션별 크기** (Iceberg `.files` 메타데이터 조회)

| col_a | 크기 | 비중 |
|-------|------|------|
| C | 21.4GB | 57% |
| B | 12.0GB | 31% |
| A | 2.8GB | 7.5% |
| D | 0.9GB | 2.4% |
| 합계 | 37.1GB | |

> 이 비율은 크게 변하지 않지만 유동적이다. 섹션 6의 산정식과 섹션 5의 `max-file-group-size-bytes` 값 선택에 영향을 준다.

**입력 파일이 703개인 이유**

append Job이 `spark.sql.adaptive.coalescePartitions.parallelismFirst=true`(기본값)를 사용하기 때문이다. `tuning/spark-tuning-guide.md` §3.2에 벤치마크 근거가 기록되어 있다 — `true` 44초 vs `false` 53~57초로 **쓰기 성능을 우선한 의도적 선택**이며, small file은 Compaction으로 해소하는 것을 전제한다.

```
5분 주기 × 12회/시간 = 12 batch
batch당 데이터 = 37,888MB ÷ 12 = 3,157MB
batch당 파일   = 703 ÷ 12 = 58.6개
파일당 크기    = 3,157 ÷ 58.6 = 53.9MB   ← 실측 avg와 일치
```

`range` 분배가 파티션 키 + Sort Order 기준으로 데이터를 배치하면 큰 파티션 하나가 여러 shuffle partition에 걸치고, **각 shuffle partition이 파일 1개를 쓴다.** `write.target-file-size-bytes=512MB`는 상한일 뿐이며, task 하나가 53.9MB만 갖고 있으면 512MB에 도달할 수 없다.

> **Compaction 쪽에서 입력 파일 수를 줄일 방법은 없다.** append 설정 변경은 이미 벤치마크로 기각된 선택지이므로, 703개는 주어진 조건으로 받아들인다.

---

## 2. Compaction 동작 원리

섹션 3의 옵션 설명에 필요한 선행 지식이다.

### 2.1 file group

Compaction은 파일을 무작위로 합치지 않는다. **파티션별로 파일을 모아 file group을 만들고, group 단위로 처리한다.**

```
(14시, col_a=A) 의 파일들  → file group
(14시, col_a=B) 의 파일들  → file group
(14시, col_a=C) 의 파일들  → file group
(14시, col_a=D) 의 파일들  → file group
```

`max-file-group-size-bytes`를 넘는 파티션은 **여러 group으로 분할**된다. `max-concurrent-file-group-rewrites`는 **동시에 처리할 group 수**를 정한다.

**file group 수 계산식**

```
file group 수 = Σ(파티션별) ceil(파티션 크기 ÷ max-file-group-size-bytes)
```

초기 설정(`max-file-group-size-bytes` = 10GB)에서의 검증:

| 파티션 | 크기 | `ceil(크기÷10GB)` | Spark UI 실측 |
|--------|------|------------------|--------------|
| col_a=C | 21.4GB | 3 | C (1/3), (2/3), (3/3) ✅ |
| col_a=B | 12.0GB | 2 | B (1/2), (2/2) ✅ |
| col_a=A | 2.8GB | 1 | A (1/1) ✅ |
| col_a=D | 0.9GB | 1 | D (1/1) ✅ |
| | | **7** | **7/7** ✅ |

### 2.2 file group당 Spark Job 2개

Spark UI에서 file group 하나당 job이 2개 생성된다 (7 group → 14 job).

| job | 역할 | 특징 |
|-----|------|------|
| 1번째 | **정렬 범위 샘플링** — `repartitionByRange`가 범위 경계를 정하기 위해 전체 데이터를 읽음 | 짧음 (3~9초) |
| 2번째 | 실제 정렬 + 쓰기 | 김 (16~26초) |

**증거**: DataFlint `input: 74.06GiB` = `output: 37.03GiB × 2`. `sort` 전략은 **데이터를 2번 읽는다.**

### 2.3 출력 파일 수 결정 방식 ✅

```
group당 출력 파일 수 = ceil(group 크기 ÷ target-file-size-bytes)
```

5회 실측 전부 일치한다:

| 총 크기 | `ceil(크기÷512MB)` | 실측 file_count |
|---------|------------------|----------------|
| 37.0GB | 74 | 75 |
| 42.3GB | 85 | 86 |
| 40.7GB | 82 | 82 |
| 39.6GB | 80 | 80 |
| 38.3GB | 77 | 76 |

**즉 출력 파일 크기를 조절하는 손잡이는 `target-file-size-bytes`이다.** 이 사실이 `advisory-partition-size`가 무효라는 판정의 근거가 된다 (섹션 3.2).

---

## 3. 옵션 및 설정 설명

### 3.1 Iceberg `rewrite_data_files` 옵션

#### `target-file-size-bytes` = 536870912 (512MB) ✅

출력 파일 하나의 목표 크기이며, 동시에 **rewrite 대상 판정 기준의 원점**이다.

| 파생 기준 | 계수 | 값 | 의미 |
|----------|------|-----|------|
| `MIN_FILE_SIZE_DEFAULT_RATIO` | × 0.75 | **384MB** | 미만이면 small file → rewrite 대상 |
| `MAX_FILE_SIZE_RATIO` | × 1.80 | 922MB | 초과하면 too large → rewrite 대상 |

512MB 선택 근거는 `schema/iceberg-schema-design-guide.md` §6.5에 있다.

#### `rewrite-all` = true ✅ 유지

조건을 무시하고 필터 범위의 파일을 전부 rewrite한다. 무력화되는 조건은 4개다.

| 조건 | 기본값 | 의미 |
|------|--------|------|
| `min-file-size-ratio` | 0.75 | 384MB 미만인 파일만 대상 |
| `min-input-files` | 5 | group에 파일이 5개 미만이면 건너뜀 |
| `max-file-size-ratio` | 1.80 | 922MB 초과 파일도 대상 |
| `delete-file-threshold` | 매우 큼 | delete file이 많으면 대상 |

**유지 근거**: 입력 파일이 최대 72.3MB로 **703개 전부 384MB 미만**이므로, 조건을 보든 안 보든 처리 대상이 동일하다. 즉 `false`로 바꿔도 I/O 절감 효과가 없다. 반면 `false`는 `min-input-files=5`에 걸려 파일 수가 적은 파티션(col_a=D)이 조용히 건너뛰어질 위험만 추가한다.

> **전제**: 이 판단은 "입력이 전부 small file"에 의존한다. append 설정이 바뀌어 큰 파일이 생기면 재검토가 필요하다.

#### `max-concurrent-file-group-rewrites` = 10 ✅

동시에 처리하는 file group 수. **실제 병렬성 상한이며, 소요시간을 결정하는 핵심 값이다.**

Iceberg 기본값은 5, 초기 설정은 2다. 2에서 7개 group을 처리하면 `2+2+2+1`로 **4회차**에 나뉜다 (섹션 4.1).

**10을 쓰는 근거**: `max-file-group-size-bytes`를 100GB로 두면 group 수는 파티션 수(col_a distinct 값 수 = 4개)와 같아지므로, 기본값 5로도 1회차로 처리된다. 그럼에도 10을 쓰는 이유는 **높게 잡는 비용이 0**이라는 점이다 — group이 4개면 Iceberg는 4개만 실행한다. hourly 테이블 4개의 col_a 카디널리티가 다를 수 있고 값이 늘어날 수도 있으므로, 여유를 둔다.

> 무한정 높이지는 않는다. group 수가 실제로 커지면 driver가 그만큼의 동시 Spark job을 관리해야 한다. 10은 현재 4개의 2.5배 수준이다.

#### `max-file-group-size-bytes` = 기본값 100GB ✅

파티션이 이 크기를 넘으면 여러 group으로 **분할**한다. 초기 설정은 10GB로 기본값의 1/10이다.

**분할이 문제의 원인이므로 상한은 높을수록 안전하다** (섹션 4.3).

| 설정 | 최대 파티션 C(21.4GB) | group 수 | 재분할까지 여유 |
|------|---------------------|---------|---------------|
| 10GB | 3개로 분할 | 7 | — (이미 분할됨) |
| 30GB | 분할 안 함 | 4 | 1.4배 |
| **100GB (기본)** | 분할 안 함 | 4 | **4.7배** |

30GB와 100GB는 현재 데이터에서 **완전히 동일하게 동작**한다. 파티션 비율이 유동적이므로 여유가 큰 기본값을 쓴다.

> **향후 확인 필요** ⚠️: group 하나의 shuffle 데이터는 executor local disk에 쌓인다. 현재 최대 group 21.4GB → shuffle 약 30GB ÷ 16 executor ≈ 1.9GB/executor. 파티션이 100GB에 근접하면 약 9GB/executor가 되어 K8S emptyDir의 ephemeral storage 한도를 넘을 수 있다. 현재 데이터의 4.7배 규모이므로 당장 문제는 없다.

#### `partial-progress.enabled` = 기본값 false ✅ 유지

`false`면 모든 file group의 결과가 **맨 마지막에 한 번만 커밋**된다. 하나라도 실패하면 전체가 롤백된다.

**유지 근거**:
- `true`로 바꾸면 run당 snapshot이 여러 개 생겨(기본 최대 10) snapshot 3일 보존 정책과 재처리 DAG의 batch_id 확인 로직(`.snapshots` 조회, `pipeline/reprocessing-dag-design.md` §4)에 영향을 준다
- 얻는 것이 없다 — task error rate 0.00%, 소요시간 1~2분

> **참고**: `false`이므로 `max-concurrent-file-group-rewrites`를 올려도 **실패 시 손실이 커지지 않는다.** 2개씩 처리할 때도 4회차에서 실패하면 1~3회차 작업이 전부 버려진다.

### 3.2 Spark 설정

#### `advisory-partition-size` = 768MB → **삭제** ✅

`spark.sql.adaptive.advisoryPartitionSizeInBytes`. AQE가 shuffle partition을 병합할 때의 목표 크기다.

**무효 판정 근거 2건**:

1. **출력 파일 수가 512MB 기준과 일치한다** (섹션 2.3). 768MB가 적용된다면 `37.0GB ÷ 768MB` = 49개가 나와야 하나 실측은 75개다
2. **삭제 전후 결과가 동일하다** (T3). avg 507.9MB → 507.0MB, file_count는 양쪽 모두 `ceil(총 크기 ÷ 512MB)`

Iceberg의 shuffling rewriter가 이 값을 자체 계산으로 덮어쓴다. 남겨두면 혼란을 유발하므로 삭제한다.

#### `coalescePartitions.parallelismFirst` = **무효 확정, 삭제 가능** ✅

`false`면 AQE가 advisory 목표 크기를 존중하고, `true`(기본값)면 병렬성을 우선해 더 작은 크기로 계산한다. AQE coalesce는 **병합만 하고 분할은 못 한다.**

`true`로 전환한 T8이 이 항목을 확정한다.

| 회차 | `parallelismFirst` | 데이터 | file_count | **파일당 크기** | avg_size |
|------|-------------------|--------|-----------|--------------|---------|
| T6 | **false** | 37.3GB | 75 | **509MB** | 509.1MB |
| T8 | **true** | 38.4GB | 77 | **510MB** | 510.4MB |

`true`면 AQE가 파티션을 잘게 유지해 파일이 더 많고 작게 나와야 하는데 변화가 없다. **Iceberg가 shuffle partition 수를 직접 정하고 있어 AQE가 병합할 대상이 없다.**

소요시간 차이(초/GB 2.41 → 2.50, dcu/GB 0.00219 → 0.00242)는 노이즈 기준선(15%) 안이다.

**설정에서 제거해도 된다.** 남겨두어도 무해하다.

> ⚠️ Iceberg 버전 업그레이드로 shuffle partition 결정 방식이 바뀌면 이 판정이 뒤집힐 수 있다. 섹션 8.3의 재검증 트리거에 포함되어 있다.

> **append Job은 `true`(기본값)를 쓴다** (`spark-tuning-guide.md` §3.2, 벤치마크로 결정). append에서는 이 설정이 실제로 동작하며 쓰기 성능에 23% 영향을 준다. Compaction에서만 무효다.

### 3.3 리소스 설정

| 설정 | 값 | 근거 |
|------|-----|------|
| `driver cpu` | **2** | 동시 job 여러 개를 조율. 1에서 2로 상향. 효과는 노이즈 범위 안이었으나 값이 저렴해 유지 (섹션 4.2) |
| `driver memory` | 4GB | driver는 manifest를 읽어 file group 계획만 세운다. 파일 703개는 가볍다 📘 |
| `executor cpu` | **4** | S3 I/O throughput과 JVM GC 부담의 균형점. append 가이드 §2.2.1과 동일 📘 |
| `executor memory` | **16GB** | 코어당 4GB. `memory usage 84~94%` + `spill to disk 0b` — 정확히 맞는 크기 ✅ |
| `num-executors` | **16** (동적화 대상) | 총 64 core. 섹션 6 |

**`executor memory`를 줄이지 않는 이유**: `memory usage`가 84~94%로 높지만 `spill to disk`가 계속 0이다. 이는 "낭비 없이 맞게 쓰고 있다"는 뜻이며, 줄이면 disk spill이 시작되어 소요시간이 크게 악화된다.

### 3.4 filter

```java
.filter(Expressions.and(
    Expressions.greaterThanOrEqual("ts", from),
    Expressions.lessThan("ts", until)))
```

`[from, until)` 반열림 구간이므로 **경계 중복이 없다.** 파티션은 `hour(ts)`이고 필터는 원본 컬럼 `ts`에 걸었으며, Iceberg의 **Hidden Partitioning**이 이를 파티션 술어로 변환한다.

**from/until은 정각이어야 한다.** 시 파티션 경계와 정확히 일치해 부분 매칭 파티션이 생기지 않게 하는 것이 `rewrite-all: true`의 전제 조건이다. 정렬이 깨지면 `rewrite-all`이 조건을 보지 않으므로 **범위 밖 파일까지 rewrite한다.**

> ⚠️ **확인 필요**: `ts`가 `timestamp_ntz`이므로 Airflow가 전달하는 from/until의 timezone 처리가 어긋나면 엉뚱한 시간대를 Compaction한다.

---

## 4. 최적화 과정

### 4.1 초기 병목: file group 7개를 2개씩 처리해 4회차로 분할

`max-concurrent-file-group-rewrites=2`, `max-file-group-size-bytes=10GB` 상태의 Spark UI Jobs 탭 분석 (2026-08-11 13~14시).

| 회차 | file group | 파티션 | 입력 파일 | 크기 | 샘플링 job | 쓰기 job | 합계 |
|------|-----------|--------|---------|------|-----------|---------|------|
| **1회** | 1/7 | col_a=D | 27 | 1.4GB | 4s | 16s | 20s |
| | 2/7 | col_a=A | 53 | 2.8GB | 6s | 19s | **25s** |
| **2회** | 3/7 | col_a=B (1/2) | 180 | 9.5GB | 7s | 25s | **32s** |
| | 4/7 | col_a=B (2/2) | 40 | 2.1GB | 3s | 23s | 26s |
| **3회** | 5/7 | col_a=C (1/3) | 184 | 9.7GB | 7s | 26s | 33s |
| | 6/7 | col_a=C (2/3) | 190 | 10.0GB | 9s | 26s | **35s** |
| **4회** | 7/7 | col_a=C (3/3) | 29 | 1.5GB | 3s | 17s | **20s** |
| | | | **703** ✅ | **37.0GB** | | | |

각 회차는 느린 쪽이 끝날 때까지 대기하므로 `25 + 32 + 35 + 20` = **112초**이며, 실측 duration 2.0분(120초)과 일치한다. 차이 8초는 pod 기동과 커밋이다.

**낭비 지점**

| 회차 | 데이터 비중 | 소요시간 비중 | 원인 |
|------|-----------|-------------|------|
| 1회 (D + A) | 11% | **21%** (25s) | 작은 파티션끼리 짝지어짐 |
| 4회 (C 3/3) | 4% | **17%** (20s) | group이 7개(홀수)라 짝이 없어 단독 실행 |

**1회 + 4회 = 45초(전체의 37%)를 데이터 15%에 소비한다.** DataFlint의 `idle cores 58.22%`가 이를 다르게 표현한 값이다 — 64 core를 확보했으나 동시에 2 group만 처리하므로 절반 이상이 유휴 상태다.

> **DataFlint alert의 처방은 오진이다.** alert는 "executor를 줄여라"를 제안하나 원인은 `max-concurrent-file-group-rewrites=2`다. 제안대로 처리하면 소요시간은 120초 그대로인 채 리소스만 줄어든다. DataFlint는 Spark 지표만 보고 Iceberg 옵션을 인식하지 못한다 (섹션 7.2).

### 4.2 테스트 결과 ✅

측정 대상: 2026-08-11 ~ 08-12, 매 회차 다른 시간대. **이미 Compaction한 데이터는 이전 상태로 되돌릴 수 없어 같은 데이터로 A/B 비교가 불가능하다.** 데이터 크기가 다르므로 정규화해 비교한다.

**정규화 지표**

| 지표 | 계산 | 용도 |
|------|------|------|
| 초/GB | `duration ÷ sum_size` | 소요시간 |
| **dcu/GB** | `dcu ÷ sum_size` | **리소스 비용. 판정의 주 지표** |
| core·초/GB | `(executors × 4 × duration) ÷ sum_size` | dcu 검산용 |

`dcu`는 `cores × duration`에 정확히 비례한다 (9회 전부 비율 49,000~53,500, ±5%). `duration`은 0.1분(6초) 단위로 반올림되어 해상도가 낮으므로 **`dcu/GB`를 주 지표로 쓴다.**

| 회차 | 대상 | 누적 변경 | 데이터 | exec | 시간 | 초/GB | **dcu/GB** | idle | memory | spill | group |
|------|------|----------|--------|------|------|-------|-----------|------|--------|-------|-------|
| baseline | 08-11 13~14 | — | 37.0GB | 16 | 120s | 3.24 | **0.00416** | 58.22% | 89.31% | 0b | 7 |
| T1 | 08-12 07~08 | `max-concurrent` 2→10 | 42.3GB | 16 | 96s | 2.27 | 0.00271 | 31.61% | 89.36% | 0b | 7 |
| T2 | 08-12 08~09 | + `driver cpu` 1→2 | 40.7GB | 16 | 84s | 2.06 | 0.00261 | 29.44% | 94.38% | 0b | 7 |
| T3 | 08-12 09~10 | + `advisory-partition-size` 삭제 | 39.6GB | 16 | 96s | 2.42 | 0.00303 | 25.62% | 91.51% | 0b | 7 |
| T4 | 08-12 10~11 | + `max-file-group-size` 10→30GB | 38.3GB | 16 | 72s | 1.88 | 0.00235 | 24.90% | 84.73% | 0b | 4 |
| T5 | 08-12 11~12 | `max-file-group-size` 100GB | 38.5GB | 16 | 84s | 2.18 | 0.00268 | 28.61% | 91.66% | 0b | 4 |
| **T6** | 08-12 12~13 | + `num-executors` 16→**12** | 37.3GB | 12 | 90s | 2.41 | **0.00219** | 16.73% | 90.33% | 0b | 4 |
| T7 | 08-12 13~14 | + `num-executors` 16→**8** | 36.6GB | 8 | 138s | 3.77 | 0.00247 | 15.35% | 91.12% | 0b | 4 |
| T8 | 08-12 14~15 | exec 12, `parallelismFirst` **true** | 38.4GB | 12 | 96s | 2.50 | 0.00242 | 23.20% | 93.28% | 0b | 4 |

baseline을 제외한 8회 전부에서 DataFlint alert가 사라지며, `spill to disk`는 9회 전부 0b다.

**노이즈 기준선 (해석의 전제)**

T4와 T5는 `max-file-group-size-bytes`만 다르지만 **둘 다 group 4개로 기능적으로 동일한 설정**이다. 그런데 1.88 vs 2.18(16% 차이)이 나왔다. 데이터 크기도 38.3 vs 38.5GB로 거의 같다.

> **따라서 15% 미만의 차이는 회차 간 변동과 구별할 수 없다.** 아래 판정은 모두 이 기준을 적용한다.

**출력 품질**

| 회차 | file_count | min_size | avg_size | max_size | group |
|------|-----------|---------|---------|---------|-------|
| baseline | 75 | 408.4MB | 505.5MB | 585.3MB | 7 |
| T1 | 86 | 288.9MB | 503.2MB | 604.9MB | 7 |
| T2 | 82 | 312.5MB | 507.9MB | 620.5MB | 7 |
| T3 | 80 | 362.4MB | 507.0MB | 670.4MB | 7 |
| T4 | 76 | 414.8MB | 516.3MB | 596.7MB | 4 |
| T5 | 78 | 335.5MB | 504.9MB | 651.9MB | 4 |
| T6 | 75 | 311.5MB | 509.1MB | 607.5MB | 4 |
| T7 | 73 | 374.4MB | 514.0MB | 626.8MB | 4 |
| T8 | 77 | 377.4MB | 510.4MB | 618.9MB | 4 |

`min_size`가 회차마다 288~415MB로 흔들리며 group 수와 무관하다. 원인은 섹션 4.3에서 다룬다.

**변경별 판정**

| 변경 | 효과 | 판정 |
|------|------|------|
| `max-concurrent-file-group-rewrites` 2→10 | 초/GB **−30%**, dcu/GB **−35%** | ✅ **확실.** 노이즈 기준선을 크게 넘고 `idle cores` 58%→32%가 뒷받침 |
| `driver cpu` 1→2 | 측정 불가 | ⚪ 노이즈 범위. 저렴하고 동시 job 조율에 필요해 유지 |
| `advisory-partition-size` 삭제 | 없음 | ✅ **무효 확정.** 삭제 전후 avg 507.9 → 507.0MB |
| `max-file-group-size-bytes` 10GB→30/100GB | 초/GB −10% (group 7→4) | ⚪ **노이즈와 구별 불가.** 유지 근거는 속도가 아니라 정렬 구간 (아래) |
| **`num-executors` 16→12** | 초/GB +19%, **dcu/GB −13%** | ✅ **채택.** 리소스 절감 (섹션 4.4) |
| `num-executors` 16→8 | 초/GB +86%, dcu/GB −2% | ❌ **기각.** 12 대비 dcu가 +13% 반등 |
| `parallelismFirst` false→true | 없음 | ✅ **무효 확정.** 파일당 크기 509 → 510MB (섹션 3.2) |

**`max-file-group-size-bytes`를 100GB로 유지하는 근거**

속도 이득(−10%)은 노이즈와 구별되지 않는다. 유지 근거는 **정렬 구간**이다. group 4개면 파티션당 정렬 구간이 1개인데, 10GB 상한에서 col_a=C가 3분할되면 정렬 구간이 3개가 되어 파일 min/max 범위가 겹친다. `read-performance-test.md` §5.4의 "Sort Order 있으면 조회 40% 빠름"이라는 이득이 그만큼 깎인다. **속도 손해가 없으므로 유지가 이득이다.**

**최종 (baseline → T6)**: 초/GB 3.24 → 2.41(**−26%**), dcu/GB 0.00416 → 0.00219(**−47%**). hourly Compaction DAG 전체(테이블 4개 순차)는 4 × 2.41 × 38GB ≈ **6.1분**으로, `:45`~`:57` 창(12분)에 절반의 여유로 들어간다.

### 4.3 min_size가 384MB 미달인 원인: col_a=D 파티션 ✅

Compaction 출력의 `min_size`가 회차마다 288~415MB로 흔들리며, 절반 이상이 small file 기준(384MB)에 미달한다.

**원인은 `col_a=D` 파티션이다.** group 수와 무관하다는 것이 증거다 — T5~T8은 group이 4개(분할 없음)인데도 335.5 / 311.5 / 374.4 / 377.4MB가 나왔다.

`min_size × 2`를 보면 9회 전부 한 구간으로 모인다.

| 회차 | min_size | ×2 | group 수 |
|------|---------|-----|---------|
| baseline | 408.4MB | 817MB | 7 |
| T1 | 288.9MB | 578MB | 7 |
| T2 | 312.5MB | 625MB | 7 |
| T3 | 362.4MB | 725MB | 7 |
| T4 | 414.8MB | 830MB | 4 |
| T5 | 335.5MB | 671MB | 4 |
| T6 | 311.5MB | 623MB | 4 |
| T7 | 374.4MB | 749MB | 4 |
| T8 | 377.4MB | 755MB | 4 |

**578~830MB 크기의 group 하나가 파일 2개로 갈린 결과다.** 그 group은 `col_a=D` 파티션이며, 측정값 0.9GB를 중심으로 시간대별로 578~830MB 사이에서 변동한다.

출력 파일 수는 `ceil(group 크기 ÷ 512MB)`이므로(섹션 2.3):

```
D = 623MB → ceil(623 ÷ 512) = 2개 → 311.5MB씩   (small file)
D = 830MB → ceil(830 ÷ 512) = 2개 → 415.0MB씩   (정상)
→ D가 768MB 미만인 시간대는 반드시 small file이 나온다
```

`col_a=A`(2.8GB)는 6개로 갈려 478MB씩이므로 원인이 될 수 없다. 10GB 상한이 만드는 자투리 group도 정상 크기다 — C의 자투리 1.4GB는 3개로 갈려 467MB, B의 자투리 2.0GB는 4개로 갈려 500MB다.

> **T4의 min 414.8MB는 설정 효과가 아니라 그 시간 D가 830MB인 결과다.** 초기 분석은 이를 `max-file-group-size-bytes` 변경의 성과로 해석했으나, group 4개인 T5~T8에서도 small file이 발생하므로 정정한다.

#### 대응: 조치하지 않는다

| 항목 | 값 |
|------|-----|
| 영향 범위 | 파일 73~86개 중 **2개** |
| 데이터 비중 | 37GB 중 **0.9GB (2.4%)** |
| 조회 영향 | 무시 가능 |
| daily Compaction 영향 | `rewrite-all=true`라 어차피 재작성 |

해소하려면 `target-file-size-bytes`를 830MB 이상으로 올려 D를 파일 1개로 만들어야 하는데, 그러면 **모든 파티션의 파일이 830MB가 되어** `iceberg-schema-design-guide.md` §6.5의 512MB 결정을 뒤집는다. 2.4% 데이터를 위해 치를 비용이 아니다.

**이는 파티션 설계에서 오는 구조적 특성이며 Compaction 옵션으로 해소할 수 없다.** `min_size`가 300MB대인 것은 정상 범위로 판단한다.

> **모니터링 판정 기준 조정**: `min_size < 384MB` 자체를 이상으로 보지 않는다. 대신 **`384MB 미만 파일이 3개 이상`**이면 D 이외의 파티션에서 발생한 것이므로 조사한다.

### 4.4 num-executors: 12가 하한이다 ✅

`max-file-group-size-bytes=100GB`, `max-concurrent-file-group-rewrites=10` 상태에서 executor 수만 변경한 결과.

| exec | core | 초/GB | **dcu/GB** | idle cores | spill |
|------|------|-------|-----------|-----------|-------|
| 16 (T4·T5 평균) | 64 | 2.03 | 0.00251 | 24.9~28.6% | 0b |
| **12 (T6)** | 48 | 2.41 | **0.00219 (−13%)** | **16.73%** | 0b |
| 8 (T7) | 32 | 3.77 | 0.00247 (−2%) | 15.35% | 0b |

**8에서 dcu가 반등한다.** 12 대비 +13%로, 느려지면서 비싸지기까지 한다.

원인은 8에서 이미 core가 병목이라는 점이다. `idle cores`가 16.73% → 15.35%로 거의 줄지 않는 반면 소요시간은 +56% 증가한다. core 33% 감소 대비 시간 56% 증가이므로 순손실이다.

**12가 리소스 절감의 하한이다.** `spill to disk`는 세 지점 모두 0b다.

> **소요시간 증가는 실패가 아니다.** DAG 전체가 4 × 2.41 × 38GB ≈ 6.1분으로 `:45`~`:57` 창(12분)에 여유 있게 들어가므로, 시간을 리소스와 맞바꾸는 것이 이득이다. 16 executor는 시간이 −42%로 더 좋지만 리소스 절감이 −44%에 그친다.

---

## 5. 확정 설정

| 구분 | 설정 | 값 | 근거 수준 | 비고 |
|------|------|-----|----------|------|
| Iceberg | `target-file-size-bytes` | 536870912 (512MB) | ✅ | 스키마 설계에서 확정 |
| Iceberg | `rewrite-all` | true | ✅ | 입력이 전부 small file |
| Iceberg | `max-concurrent-file-group-rewrites` | **10** | ✅ | −30%. 유일하게 명확한 개선 |
| Iceberg | `max-file-group-size-bytes` | **기본값 100GB** (설정 제거) | 📘 | 속도는 중립. 정렬 구간 1개 유지 |
| Iceberg | `partial-progress.enabled` | 기본값 false | 📘 | 변경 이득 없음 |
| Spark | `advisory-partition-size` | **삭제** | ✅ | 무효 확정 |
| Spark | `coalescePartitions.parallelismFirst` | **삭제 가능** | ✅ | 무효 확정 (T8) |
| 리소스 | `driver cpu` / `memory` | **2** / 4GB | 📘 | 효과는 노이즈 범위, 저렴해서 유지 |
| 리소스 | `executor cpu` / `memory` | 4 / 16GB | ✅ | spill 0 유지 |
| 리소스 | `num-executors` | **12** (→ 동적 산정) | ✅ | dcu 최저점. 섹션 4.4, 6 |
| 전략 | rewrite 전략 | `sort` | ✅ | 미적용 시 조회 40% 저하 |

**변경 전후 요약** (baseline → T6)

| 항목 | 변경 전 | 변경 후 |
|------|--------|--------|
| 초/GB | 3.24 | **2.41 (−26%)** |
| **dcu/GB (리소스 비용)** | 0.00416 | **0.00219 (−47%)** |
| idle cores | 58.22% | **16.73%** |
| file group 수 | 7 | 4 |
| 처리 회차 | 4 | **1** |
| num-executors | 16 | **12** |
| DAG 전체 (테이블 4개) | 10~12분 | **약 6분** |

시간 −26%, 리소스 −47%다. 스케줄 창에 여유가 있으므로 리소스 절감을 우선한 결과다.

---

## 6. 동적 리소스 산정

> **자원 할당 방식은 `pipeline/compaction-executor-sizing-design.md`에서 확정했다.** 후보 3개(정적 유지 / Dynamic Allocation / 사전 산정) 중 **Dynamic Allocation + `executorAllocationRatio=0.13`을 채택**했으며 7회 실측으로 검증했다(39GB→12대, 82GB→24대). 이 섹션의 계수 `C=0.32`는 그 ratio 값을 도출하는 근거로 쓰인다. 사전 산정(Trino 조회) 방식은 보류 상태다.

### 6.1 배경과 동적화 대상 축소

당초 목표는 데이터 증가에 맞춰 driver/executor의 cpu·memory·개수를 모두 동적으로 산정하는 것이다. **테스트 결과 동적화가 필요한 값은 `num-executors` 하나다.**

| 설정 | 당초 계획 | 판정 | 이유 |
|------|----------|------|------|
| `num-executors` | 동적 | ✅ **동적 — 유일한 대상** | 데이터 양에 비례하는 유일한 값 |
| `executor memory` | 동적 | ❌ 고정 16GB | task 하나의 처리 단위가 512MB로 고정. 데이터가 2배가 되면 task 수가 2배 되고 task 크기는 그대로 → 메모리는 데이터 양과 무관 |
| `executor cpu` | 동적 | ❌ 고정 4 | 동일 |
| `driver cpu` / `memory` | 동적 | ❌ 고정 2 / 4GB | file group 수(4개)에 비례하나 변동 폭이 작음 |
| `max-concurrent-file-group-rewrites` | (계획 외) | ❌ 크게 고정 (10) | group 수보다 크면 남는 값은 사용되지 않음 → 동적화가 무의미 |
| `max-file-group-size-bytes` | (계획 외) | ❌ 크게 고정 (100GB) | 분할하지 않는 것이 목표 → 크게 두면 충족 |

**동적화 대상이 6개에서 1개로 축소된다.** 나머지 5개는 손댈 필요가 없음이 실측으로 확인되므로 구현·유지보수 범위가 그만큼 줄어든다.

### 6.2 duration을 일정하게 유지해야 하는 이유

`pipeline/reprocessing-dag-design.md` §6.2는 실측 duration을 근거로 maintenance 스케줄을 배치한다.

- hourly Compaction 시작 분: `M ≤ 60 − duration − 여유` = `60 − 12 − 3` = **`:45`**
- 설계에 명시: *"hourly duration이 15분을 넘으면 이 식으로 재계산해야 한다"*

정적 리소스는 데이터 증가 시 duration이 늘어 이 전제를 조용히 깨뜨린다. 동적 산정은 증가분을 리소스로 흡수해 duration을 상수에 가깝게 유지한다.

### 6.3 입력 측정

hourly는 입력 파일이 **전부** small file이므로(최대 72.3MB) 크기 필터가 불필요하다. 총 크기만 측정하면 된다.

#### `.files`가 아니라 `.partitions`를 사용한다

`.files`는 **데이터 파일 1개당 1행**을 만들고, 그 행에 `column_sizes`·`value_counts`·`null_value_counts`·`lower_bounds`·`upper_bounds`가 **컬럼 19개 전부**에 대해 들어간다. `sum(file_size_in_bytes)` 하나만 필요한데 전부 끌고 온다.

`.partitions`는 필요한 값이 **파티션당 1행으로 이미 집계**되어 있다.

| 조회 | 반환 행 수 (30일 보관 가정) | 행 하나 크기 |
|------|------------------------|------------|
| `.files` (필터 없음) | 약 54,000 | 무거움 (컬럼 19개 통계) |
| `.files` (파티션 필터) | 약 75 | 무거움 |
| **`.partitions` (파티션 필터)** | **4** | 가벼움 |

**Trino** (Airflow에서 JDBC 조회):

```sql
SELECT sum(total_size) AS total_bytes,
       sum(file_count) AS file_count
FROM "db.table_a$partitions"
WHERE partition.ts_hour = <대상 시간>
```

**Spark SQL** (컬럼명이 다름):

```sql
SELECT sum(total_data_file_size_in_bytes), sum(file_count)
FROM db.table_a.partitions
WHERE partition.ts_hour = <대상 시간>
```

> ⚠️ 컬럼명은 엔진·버전에 따라 다르다 (`total_size` vs `total_data_file_size_in_bytes`). 실제 조회로 확인한다.

#### 부하 특성과 manifest pruning ⚠️

메타데이터 조회 비용은 두 갈래이며, **행 수가 아니라 manifest 수가 지배한다.**

| 비용 | 무엇에 비례 | 규모 |
|------|-----------|------|
| manifest 읽기 (S3 GET + avro 파싱) | **manifest 파일 수** | append 5분 주기 = 288 commit/일. `rewrite_manifests` 3일 주기 사이 수백~1,000개 누적 가능 |
| 행 materialize | 데이터 파일 수 | `.partitions`를 쓰면 사실상 사라짐 |

파티션 필터를 걸면 Iceberg가 **해당 시간과 겹치지 않는 manifest를 건너뛸 수 있다** (manifest list에 manifest별 파티션 범위 요약이 있다). 이 pruning이 걸리면 manifest 수가 늘어도 비용이 거의 늘지 않는다.

**이 테이블은 pruning에 유리한 조건이다:**

| 조건 | 효과 |
|------|------|
| 파티션이 `hour(ts)` — 시간순 | 특정 시간의 파일이 최근 manifest 소수에 모여 있음 |
| `rewrite_manifests` 3일마다 실행 (`reprocessing-dag-design.md` §6.2) | manifest를 파티션 기준으로 정리 → pruning 정밀도 향상 |

즉 이미 운영 중인 `rewrite_manifests`가 이 조회의 비용을 관리해 주는 구조다.

**pruning 동작 확인 방법** (⚠️ metadata table에서 실제로 걸리는지 미검증):

```sql
-- A: 전체
SELECT count(*) FROM "db.table_a$partitions";
-- B: 파티션 필터
SELECT count(*) FROM "db.table_a$partitions" WHERE partition.ts_hour = <대상 시간>;
```

B가 A보다 확연히 빠르고 Trino 쿼리 통계의 **Physical input**이 작으면 pruning이 걸린 것이다. 비슷하면 manifest 전체를 읽고 있다.

**최악의 경우 추정**: manifest 1,000개 × 30~100KB ≈ 30~90MB 읽기 → 수 초. hourly Compaction 자체가 1~2분 걸리는 job이므로 수 초는 무시할 수준이다. 호출 빈도는 테이블 4개 × 24시간 = 96회/일.

#### fallback 필수

조회를 도입하면 **외부 의존성과 실패 모드가 하나 늘어난다.** 지금은 상수라 이 실패 모드가 없다.

```python
try:
    total_gb = query_partitions_size(table, target_hour)   # Trino
    if total_gb <= 0:
        raise ValueError(f"비정상 크기: {total_gb}")
    num_executors = clamp(ceil(total_gb * C), MIN_EXECUTORS, MAX_EXECUTORS)
except Exception as e:
    logger.warning("메타데이터 조회 실패, 기본값 사용: %s", e)
    num_executors = table.config.com_num_executor   # 기존 상수를 fallback으로 유지
```

**기존 `com_num_executor` 상수를 지우지 않고 fallback으로 남긴다.** 조회가 실패해도 Compaction은 실행되어야 한다.

**결과 검증도 필요하다.** 조회는 성공했으나 파티션 조건이 틀려 0이 반환되면 executor가 `MIN_EXECUTORS`로 떨어져 Job이 한없이 느려진다. 위 코드의 `total_gb <= 0` 검사가 그 방어선이다.

#### 검토했으나 채택하지 않은 대안: 직전 회차 값 캐싱

시간대별 데이터 양이 완만하게 변하므로(실측 37 → 42.3 → 40.7 → 39.6 → 38.3GB, 시간당 10% 내외) 직전 회차의 실측 크기를 저장해 재사용하는 방식이 대안이 된다. 매 회차 갱신되므로 값이 고정되지는 않는다.

**채택하지 않은 이유**: "실제 크기"를 Spark pod에서 Airflow로 되돌리는 배관이 필요하다. 로그 파싱은 깨지기 쉽고, Spark job이 별도 저장소에 기록하는 방식은 코드 변경 범위가 커진다. Compaction 후 `.partitions`를 조회하는 방식은 결국 조회를 하는 것이라 목적을 달성하지 못한다. **조회를 피하려다 구현이 더 늘어난다.**

pruning 측정 결과 조회 비용이 과하게 나올 때만 재검토한다.

### 6.4 산정식 — C = 0.32 ✅

```python
num_executors = ceil(total_size_gb * 0.32)
num_executors = min(max(num_executors, MIN_EXECUTORS), MAX_EXECUTORS)
```

**계수 C 캘리브레이션 결과** (섹션 4.4)

| num-executors | 총 크기 | C | dcu/GB | idle cores | spill | 판정 |
|---|---|---|---|---|---|---|
| 16 | 38.3~38.5GB | 0.42 | 0.00251 | 24.9~28.6% | 0b | 리소스 과다 |
| **12** | 37.3GB | **0.32** | **0.00219** | 16.73% | 0b | ✅ **채택** |
| 8 | 36.6GB | 0.22 | 0.00247 | 15.35% | 0b | dcu 반등, 기각 |

검산: 37.3GB × 0.32 = 11.9 → **12** / 38.5GB → 13 / 42.3GB → 14

**판정 기준** — `spill to disk = 0b` 유지가 절대 조건이고, 그 안에서 아래 파생 지표로 비교한다.

| 지표 | 계산 | 의미 |
|------|------|------|
| 초/GB | `duration ÷ sum_size` | 소요시간. **executor를 줄이면 커지는 것이 정상** |
| **dcu/GB** | `dcu ÷ sum_size` | **리소스 비용. 주 지표** — 줄어야 축소가 성공 |
| core·초/GB | `(num_executors × 4 × duration) ÷ sum_size` | dcu 검산용 |
| DAG 전체 | `초/GB × 평균 크기 × 4 테이블` | `:45`~`:57` 창(12분) 안에 여유롭게 들어가야 함 |

`duration`만 보면 "느려졌다"로 오판한다. **소요시간 증가와 리소스 절감을 맞바꾸는 것이 목적**이며, DAG 전체가 창 안에 들어가는 한 절감이 이득이다.

`dcu`는 `cores × duration`에 비례한다 (9회 측정 비율 49,000~53,500, ±5%). `duration`보다 해상도가 좋아 주 지표로 적합하다.

**상한·하한**

| 항목 | 값 | 근거 |
|------|-----|------|
| `MAX_EXECUTORS` | ⚠️ 미확정 | append 벤치마크에서 32개 이상은 오히려 느려졌다(shuffle 통신, K8S pod 스케줄링 경합, S3 부하 — `tuning/spark-tuning-guide.md` §2.2.3). K8S namespace quota도 확인 필요. **상한에 걸리면 알림을 발생시켜 파티션 재설계 검토 신호로 사용** |
| `MIN_EXECUTORS` | 4 (안) 📘 | 데이터가 적은 시간대에 과도하게 축소되는 것 방지 |

**도입 시점 — 현재는 정적 12가 충분하다**

고정 core에서는 duration이 데이터에 비례하고 초/GB가 일정하다. core가 데이터에 비례하면 duration이 일정해진다. 두 방식의 실행 창(12분) 한계를 비교하면:

| 데이터 (테이블당) | 정적 12 DAG 전체 | 동적 산정 DAG 전체 |
|------------------|-----------------|------------------|
| 42.3GB (현재 최대) | 6.8분 | 6.0분 |
| 60GB | 9.6분 | 6.0분 |
| **74.7GB** | **12.0분 (창 초과)** | 6.0분 |
| 100GB | 16.1분 | 6.0분 (`MAX_EXECUTORS` 도달) |

- 정적 12의 창 초과 지점: 테이블당 **74.7GB**. 현재 최대 42.3GB 대비 **여유 1.77배**
- 도입 시점: 테이블당 **55~60GB** 도달 시 (창 여유가 2~3분으로 줄어드는 구간)
- 그때까지는 `com_num_executor`를 12로 고정한다. 산정값도 12~14로 정적값과 거의 같다

**실제 채택안은 Dynamic Allocation이다.** 위 `C=0.32`는 DA의 `executorAllocationRatio` 값을 도출하는 데 쓰인다.

```
desired = (데이터GB × 9 ÷ 4) × ratio = 데이터GB × 2.25 × ratio
목표    = 데이터GB × 0.32
→ ratio = 0.32 ÷ 2.25 = 0.142   (실측 검증값 0.13)
```

양변에서 `데이터GB`가 소거되므로 **ratio는 테이블 크기와 무관하다.** 실측에서 같은 0.13으로 39GB → 12대, 82GB → 24대가 나왔다. 상세는 **`pipeline/compaction-executor-sizing-design.md`** §4~5를 참조한다.

### 6.5 구현

**변경 예시: `pipeline/examples/compaction_executor_sizing_example.py`**

`compaction_dag_example.py`가 만드는 `compaction_specs` task에서 `instances` 한 줄만 바꾼다.

```python
# before
"instances": str(table.config.com_num_executor),
# after
"instances": str(num_executors_for(table, from_hour, until_hour)),
```

예시 파일이 담고 있는 것:

| 항목 | 내용 |
|------|------|
| `to_partition_hour()` | datetime → `hour(ts)` 파티션 값 변환. **naive datetime으로 계산** (`ts`가 `timestamp_ntz`이므로 timezone을 붙이면 값이 어긋난다). 2026-08-11 13:00 → 496237로 Spark UI 실측값과 일치 검증 |
| `query_size_bytes()` | `.partitions` 범위 조회. 정기 실행은 1시간이지만 재처리 DAG trigger 시 여러 시간에 걸치므로(`reprocessing-dag-design.md` §6.3) 범위 조회다 |
| `num_executors_for()` | 산정 + clamp + fallback. 조회 실패, 0 반환, 비정상 크기를 모두 `com_num_executor`로 fallback |
| 상한 경고 | `MAX_EXECUTORS`에 걸리면 warning 로그. 데이터가 설계 범위를 넘었다는 신호 |

**신규 구현이 필요한 부분**: Trino connection(`TrinoHook`)과 Iceberg schema 이름. 예시 파일에 `TODO(연결)`로 표시되어 있다.

**도입 전 확인이 필요한 부분** (예시 파일에 `TODO(확인)`으로 표시):

1. Trino `$partitions`에서 `partition.ts_hour`가 INTEGER로 노출되는지 (Spark SQL은 컬럼명이 `total_data_file_size_in_bytes`로 다르다)
2. manifest pruning이 걸리는지 (섹션 6.3의 두 쿼리 비교)

**조회 횟수**: `compaction_specs`는 DAG run당 1회 실행되고 그 안에서 테이블 수만큼 조회한다. hourly 테이블 4개 × 24시간 = 96회/일.

**daily에 그대로 쓸 수 없다.** `C=0.32`은 hourly 측정값이며, daily는 `rewrite-all` 낭비 의심(섹션 8.1) 확인 후 별도로 계수를 잡아야 한다.

---

## 7. 모니터링 지표

### 7.1 매 테스트/운영 시 기록할 지표

```
DataFlint : duration, dcu, idle_cores, spill_to_disk, memory_usage
Iceberg   : sum_size, file_count, min_size, avg_size      ← min_size가 384MB를 넘는지
Spark UI  : file group 수 (Jobs 탭 job 제목의 "file group N/M")
Airflow   : task duration (pod 기동 시간 역산용)
```

### 7.2 DataFlint 지표 해석

| 지표 | 의미 | 판정 기준 및 활용 |
|------|------|-----------------|
| **idle cores** | 확보한 core 중 유휴 비율 | 20% 이하면 양호. **원인이 2가지이고 처방이 반대다** (아래) |
| **spill to disk** | 메모리가 넘쳐 디스크에 쓴 양 | **가장 중요한 안전선.** 0이 아니면 메모리 부족. memory나 executor를 줄일 때 반드시 확인 |
| **memory usage** | executor 메모리 최고 사용률 | 높은 것이 나쁜 것이 아니다. `spill 0 + 89%`는 낭비 없이 사용 중이라는 뜻. **항상 spill과 짝으로 판정** — 90%↑ & spill 발생 → 증설 / 60%↓ & spill 0 → 감축 여지 |
| **duration** | Spark 앱 실행 시간 | 데이터 크기가 매번 다르므로 **반드시 `초/GB`로 정규화.** 해상도 0.1분(6초) → 노이즈 ±7% |
| **dcu** | 리소스 × 시간 기반 비용 대리 지표 | **executor 축소 테스트의 핵심 지표.** duration은 늘어도 dcu가 줄면 축소 성공. duration만 보면 오판한다 |
| **input / output** | 읽은 양 / 쓴 양 | `sort` 전략은 **2.0배**가 정상(샘플링 + 쓰기, 섹션 2.2). 벗어나면 무언가 변한 것 |
| **shuffle read / write** | shuffle 데이터량 | 데이터 크기의 약 1.4배가 현재 수준. executor 축소 시 executor당 부담 증가를 함께 확인 |
| **task error rate** | task 실패/재시도 비율 | 0이 아니면 OOM 또는 S3 타임아웃. `partial-progress=false`라 실패가 전체 롤백으로 이어져 중요 |

**`idle cores`가 높을 때 — 원인 2가지와 반대되는 처방**

| 원인 | 증상 | 처방 |
|------|------|------|
| 리소스 과다 | task는 전부 도는데 core가 남음 | executor 축소 |
| **병렬성 제약** | 일을 나눠줄 단위가 막혀 있음 | **제약 해제** |

이번 사례의 원인은 후자다(`max-concurrent-file-group-rewrites=2`). **DataFlint alert는 전자만 제안하므로 처방을 그대로 따르면 안 된다** — Iceberg 옵션을 인식하지 못한다.

### 7.3 DataFlint에 없어 별도로 확인할 것

| 항목 | 확인 위치 | 필요 이유 |
|------|----------|----------|
| **min_size / max_size / file_count** | Iceberg `.files` 메타데이터 | **Compaction 품질의 핵심 지표인데 DataFlint에 없다.** duration이 개선되어도 출력이 나빠지면 실패다. 단 `min_size < 384MB`는 col_a=D에서 상시 발생하므로 이상이 아니다 — **384MB 미만 파일이 3개 이상**일 때 조사한다 (섹션 4.3) |
| file group 수와 구성 | Spark UI Jobs 탭 | 파티션이 몇 개로 분할됐는지 |
| pod 기동 시간 | Airflow task duration − Spark 앱 duration | 1~2분짜리 job에서 기동이 20~30초면 비중이 크다 |

---

## 8. 미확정 및 후속 과제

### 8.1 daily Compaction — rewrite-all 낭비 의심 ⚠️

hourly와 daily의 소요시간이 데이터 양에 거의 선형이다.

```
hourly:  37GB  → 2.0분 (튜닝 전)
daily:  888GB  → 30~60분        ← 약 24배 선형
```

hourly가 매시간 출력을 75개 × 505MB(대부분 384MB 이상)로 정리한다면, daily는 **합칠 small file이 거의 없어 사실상 no-op에 가까워야 한다.** 그런데 데이터 양에 선형으로 소요된다.

**가설**: daily도 `rewrite-all: true`로 888GB 전체를 다시 쓰고 있다. hourly와 달리 daily에서는 `rewrite-all: false`가 큰 이득일 수 있다.

daily 단계에서 최우선으로 확인할 항목이다.

### 8.2 남은 확인 항목

| 항목 | 내용 | 우선순위 |
|------|------|---------|
| `MAX_EXECUTORS` 확정 | K8S namespace quota 확인. append(batch당 약 10 executor)와 동시 실행됨. **산정식 완성의 마지막 조각** | 높음 |
| 확정 설정 운영 검증 | 여러 시간대에서 `spill 0`, `384MB 미만 파일 ≤ 2개`, DAG 전체 6분대 유지 확인 | 높음 |
| metadata table manifest pruning | `.partitions` 파티션 필터가 manifest를 실제로 pruning하는지 (섹션 6.3). 조회 비용 규모 결정 | 중간 |
| `ts` timezone 검증 | Airflow가 전달하는 from/until의 `timestamp_ntz` 처리 (섹션 3.4) | 중간 |
| executor local disk 한도 | 파티션이 커질 때 shuffle 저장 공간 (섹션 3.1) | 낮음 |
| 다른 hourly 테이블 3개 검증 | col_a 카디널리티가 다르면 file group 수가 달라져 `max-concurrent` 여유(10 − 4)를 재확인해야 한다 | 중간 |

**완료된 항목**: `max-file-group-size-bytes` 100GB 검증(T5), `num-executors` C 캘리브레이션(T6·T7 → C=0.32), `parallelismFirst` 판정(T8 → 무효 확정).

### 8.3 재검증 트리거

```
⚠️ 다음 상황에서 이 문서의 설정을 재검증해야 한다:

1. append Job의 parallelismFirst 또는 shuffle 설정 변경
   → 입력 파일 크기 분포가 바뀌어 rewrite-all 판단(섹션 3.1)의 전제가 깨진다

2. 파티션 또는 Sort Order 변경
   → shuffle 패턴과 file group 구성이 달라진다

3. col_a 카디널리티 증가
   → file group 수가 늘어 max-concurrent-file-group-rewrites 여유(현재 10 − 4)를 재확인해야 한다

4. 최대 파티션 크기가 100GB에 근접
   → max-file-group-size-bytes 분할이 재발하고 executor local disk 한도에 걸린다

5. hourly duration이 15분 초과 (DAG 전체)
   → reprocessing-dag-design.md §6.2의 M ≤ 60 − duration − 여유 재계산 필요

6. Iceberg 버전 업그레이드
   → shuffle partition 결정 방식이 바뀌면 advisory-partition-size와
     parallelismFirst의 "무효" 판정(섹션 3.2)이 뒤집힐 수 있다.
     file_count가 ceil(총 크기 ÷ 512MB) 수준을 유지하는지 확인한다

7. col_a=D 파티션 비중 변화
   → min_size가 D 크기 ÷ 2로 정해진다(섹션 4.3). D가 커지면 문제가
     사라지고, 512MB 미만으로 줄면 파일 1개가 되어 역시 사라진다
```

---

## 9. 용어집

| 용어 | 정의 |
|------|------|
| **file group** | Compaction의 처리 단위. 파티션별로 파일을 모아 구성하며, `max-file-group-size-bytes`를 넘으면 분할된다 |
| **small file** | `target-file-size-bytes × 0.75` 미만인 파일. 512MB 기준 **384MB 미만** |
| **`MIN_FILE_SIZE_DEFAULT_RATIO`** | Iceberg `SizeBasedFileRewriter`의 small file 판정 계수 (0.75) |
| **`MAX_FILE_SIZE_RATIO`** | too large 판정 계수 (1.80). 512MB 기준 922MB 초과 |
| **sort 전략** | `rewrite_data_files`가 테이블 Sort Order로 재정렬하며 합치는 방식. `binpack`과 달리 shuffle과 샘플링 pass가 필요하다 |
| **샘플링 job** | `repartitionByRange`가 정렬 범위 경계를 정하기 위해 데이터를 한 번 더 읽는 Spark job. file group당 1개 |
| **AQE coalescePartitions** | shuffle 후 작은 파티션을 병합하는 AQE 기능. **병합만 하고 분할은 못 한다** |
| **idle cores** | DataFlint 지표. 확보한 core 중 유휴 비율. 원인이 리소스 과다 또는 병렬성 제약 두 가지다 |
| **dcu** | DataFlint의 리소스 × 시간 기반 비용 대리 지표 |
| **spill to disk** | 메모리 부족으로 디스크에 쓴 데이터량. 0 유지가 목표 |

---

## 10. 참고 자료

- [Iceberg Spark Procedures — rewrite_data_files](https://iceberg.apache.org/docs/latest/spark-procedures/#rewrite_data_files)
- [Iceberg SizeBasedFileRewriter Javadoc](https://iceberg.apache.org/javadoc/1.4.1/org/apache/iceberg/actions/SizeBasedFileRewriter.html)
- [Iceberg Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [Spark 4.1.1 SQL Performance Tuning (AQE)](https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html)
- [Spark on Kubernetes](https://spark.apache.org/docs/4.1.1/running-on-kubernetes.html)
- 프로젝트 내부: `tuning/spark-tuning-guide.md` (append Job 설정)
- 프로젝트 내부: `schema/iceberg-schema-design-guide.md` (파티션·Sort Order·target-file-size 근거)
- 프로젝트 내부: `schema/read-performance-test.md` §5 (Sort Order 읽기 성능 실측)
- 프로젝트 내부: `pipeline/reprocessing-dag-design.md` §6.2 (maintenance 스케줄 배치)
