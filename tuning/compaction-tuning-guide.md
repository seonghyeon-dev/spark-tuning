# Iceberg Compaction 튜닝 가이드 (hourly)

## 문서 정보

| 항목 | 내용 |
|------|------|
| 작성 목적 | hourly Compaction Job의 Iceberg 옵션 및 Spark 리소스 설정에 대한 근거 기반 가이드 |
| 대상 독자 | 데이터 엔지니어, 운영팀 |
| 환경 | Kubernetes 클러스터, S3(MinIO), Spark 4.1.1, Iceberg 1.10.1, Airflow 3.2.2 |
| 대상 범위 | **hourly Compaction만.** daily Compaction은 별도 (섹션 8.1) |
| 최종 수정일 | 2026-08-12 |

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
- [4. 최적화 과정](#4-최적화-과정) — 병목 분석, 테스트 결과, small file 발생 메커니즘
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

**`sort` 전략은 필수다.** `schema/read-performance-test.md` §5.4에서 Sort Order 미설정 시 조회 소요시간이 650ms → 1.1s로 **40% 증가**하는 것이 실측되었다. `binpack` 전략은 shuffle과 정렬을 생략해 Compaction Job 자체는 빨라지지만, 조회 성능을 희생하므로 채택하지 않는다.

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

이론 최솟값이 `37.0GB ÷ 512MB` = 74개이므로 **75개는 사실상 최적**이며, min 408.4MB > 384MB로 **잔여 small file이 없다.** 즉 **출력 품질은 튜닝 전에도 문제가 없었고, 개선 여지는 소요시간과 리소스 효율에 있었다.**

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

Iceberg 기본값은 5, 초기 설정은 2였다. 2에서 7개 group을 처리하면 `2+2+2+1`로 **4회차**에 나뉜다 (섹션 4.1).

**10을 쓰는 근거**: `max-file-group-size-bytes`를 100GB로 두면 group 수는 파티션 수(col_a distinct 값 수 = 4개)와 같아지므로, 기본값 5로도 1회차로 처리된다. 그럼에도 10을 쓰는 이유는 **높게 잡는 비용이 0**이라는 점이다 — group이 4개면 Iceberg는 4개만 실행한다. hourly 테이블 4개의 col_a 카디널리티가 다를 수 있고 값이 늘어날 수도 있으므로, 여유를 둔다.

> 무한정 높이지는 않는다. group 수가 실제로 커지면 driver가 그만큼의 동시 Spark job을 관리해야 한다. 10은 현재 4개의 2.5배 수준이다.

#### `max-file-group-size-bytes` = 기본값 100GB ✅

파티션이 이 크기를 넘으면 여러 group으로 **분할**한다. 초기 설정은 10GB(기본값의 1/10)였다.

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

1. **출력 파일 수가 512MB 기준과 일치한다** (섹션 2.3). 768MB가 적용됐다면 `37.0GB ÷ 768MB` = 49개가 나와야 하는데 실측은 75개다
2. **삭제 전후 결과가 동일하다** (T3). avg 507.9MB → 507.0MB, file_count는 양쪽 모두 `ceil(총 크기 ÷ 512MB)`

Iceberg의 shuffling rewriter가 이 값을 자체 계산으로 덮어쓴다. 남겨두면 혼란을 유발하므로 삭제한다.

#### `coalescePartitions.parallelismFirst` = false ✅ 유지 (⚠️ 근거 미확정)

`false`면 AQE가 위 advisory 목표 크기를 존중하고, `true`(기본값)면 병렬성을 우선해 더 작은 크기로 계산한다.

**`advisory-partition-size`와 상황이 다르다.** 무효라는 실측 근거가 없다. AQE coalesce는 **병합만 하고 분할은 못 하므로** 두 가능성이 존재한다.

| 가능성 | `false`의 역할 |
|--------|--------------|
| Iceberg가 shuffle partition 수를 512MB 기준으로 직접 지정 | 무의미 (병합할 것이 없음) |
| Iceberg가 partition을 많이 두고 AQE 병합에 의존 | **필수** (없으면 작고 많은 파일 생성) |

`advisory-partition-size` 삭제로는 이 둘을 구별할 수 없다. 어느 쪽이든 Iceberg가 자기 값으로 덮으므로 사용자 지정값 제거는 변화를 만들지 않는다.

**위험이 비대칭이므로 유지한다**: `false` 유지는 최악의 경우 무의미하지만(손해 없음), 제거는 필수였을 경우 섹션 4.3의 small file 문제를 재발시킨다.

**판정 방법** (후속): 1시간치를 제거 상태로 실행해 `file_count`를 확인한다. `ceil(총 크기 ÷ 512MB)` 수준을 유지하면 무의미 확정, 늘어나면 필수 확정.

> **append Job은 `true`(기본값)를 쓴다.** 반대로 설정된 것이 맞다 — append는 쓰기 레이턴시가 목적이고 Compaction은 파일 크기가 목적이므로 갈라지는 것이 정상이다.

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

**1회 + 4회 = 45초(전체의 37%)를 데이터 8%에 소비한다.** DataFlint의 `idle cores 58.22%`가 이를 다르게 표현한 값이다 — 64 core를 확보했으나 동시에 2 group만 처리하므로 절반 이상이 유휴 상태다.

> **DataFlint alert의 처방은 오진이었다.** "executor를 줄여라"라고 제안했으나 원인은 `max-concurrent-file-group-rewrites=2`였다. 제안을 따랐다면 소요시간은 120초 그대로인 채 리소스만 줄었을 것이다. DataFlint는 Iceberg 옵션을 모른다 (섹션 7.2).

### 4.2 테스트 결과 ✅

측정 대상: 2026-08-12, 매시간 다른 데이터. **이미 Compaction한 데이터는 이전 상태로 되돌릴 수 없어 각 회차가 서로 다른 시간대를 대상으로 한다.** 데이터 크기가 다르므로 `초/GB`로 정규화해 비교한다.

| 회차 | 대상 | 누적 변경 | 데이터 | 시간 | **초/GB** | 개선 | idle cores | memory | spill |
|------|------|----------|--------|------|----------|------|-----------|--------|-------|
| baseline | 08-11 13~14 | — | 37.0GB | 120s | **3.24** | — | 58.22% | 89.31% | 0b |
| T1 | 08-12 07~08 | `max-concurrent` 2→10 | 42.3GB | 96s | **2.27** | **−30%** | 31.61% | 89.36% | 0b |
| T2 | 08-12 08~09 | + `driver cpu` 1→2 | 40.7GB | 84s | **2.06** | −36% | 29.44% | 94.38% | 0b |
| T3 | 08-12 09~10 | + `advisory-partition-size` 삭제 | 39.6GB | 96s | **2.42** | −25% | 25.62% | 91.51% | 0b |
| T4 | 08-12 10~11 | + `max-file-group-size` 10→30GB | 38.3GB | 72s | **1.88** | **−42%** | 24.90% | 84.73% | 0b |

T1~T4 전부에서 DataFlint alert가 사라졌다.

**출력 품질**

| 회차 | file_count | min_size | avg_size | max_size |
|------|-----------|---------|---------|---------|
| baseline | 75 | 408.4MB | 505.5MB | 585.3MB |
| T1 | 86 | **288.9MB** ❌ | 503.2MB | 604.9MB |
| T2 | 82 | **312.5MB** ❌ | 507.9MB | 620.5MB |
| T3 | 80 | **362.4MB** ❌ | 507.0MB | 670.4MB |
| T4 | 76 | **414.8MB** ✅ | 516.3MB | 596.7MB |

**변경별 판정** (duration 해상도가 0.1분=6초이므로 노이즈 폭 ±7%)

| 변경 | 효과 | 판정 |
|------|------|------|
| `max-concurrent-file-group-rewrites` 2→10 | **−30%** | ✅ **확실.** `idle cores` 58%→32%가 뒷받침 |
| `driver cpu` 1→2 | 측정 불가 | ⚪ T1·T2·T3이 2.06~2.42로 노이즈 범위에서 섞임. 값이 저렴하고 동시 job 조율에 필요해 유지 |
| `advisory-partition-size` 삭제 | 없음 (예상대로) | ✅ **무효 확인.** 삭제 전후 avg 507.9 → 507.0MB |
| `max-file-group-size-bytes` 10→30GB | **추가 −16%** | ✅ **확실.** T1~T3 평균 2.25 → 1.88. **min_size 문제 해결이 더 큰 소득** |

**최종: 3.24 → 1.88 초/GB (−42%).** hourly Compaction DAG 전체(테이블 4개 순차)는 10~12분 → 6~7분이 예상된다.

### 4.3 small file 발생 메커니즘 ⚠️

**소요시간보다 중요한 발견이다.** T1~T3의 `min_size`가 288.9 / 312.5 / 362.4MB로 **전부 small file 기준(384MB) 미달**이다. Compaction의 목적이 small file 제거인데 결과물에 small file을 남겼다.

**원인: 작은 file group은 적정 크기의 파일을 만들 수 없다.**

출력 파일 수는 `ceil(group 크기 ÷ 512MB)`로 정해지고(섹션 2.3), 이 나눗셈이 거칠다.

| group 크기 | 나누는 개수 | 파일 하나 크기 | 판정 |
|-----------|-----------|--------------|------|
| 578MB | 2 | **289MB** | ❌ |
| 625MB | 2 | **313MB** | ❌ |
| 725MB | 2 | **362MB** | ❌ |
| 850MB | 2 | 425MB | ✅ |
| 10GB | 20 | 512MB | ✅ |
| 21GB | 42 | 512MB | ✅ |

**512MB~768MB 크기의 group은 반드시 small file 2개를 만든다.** 512MB 하나로 만들 수도, 512MB짜리 2개로 만들 수도 없다.

관측값이 이 패턴과 정확히 일치한다:

```
T1: 288.9 × 2 = 577.8MB
T2: 312.5 × 2 = 625.0MB
T3: 362.4 × 2 = 724.8MB
```

`max-file-group-size-bytes=10GB`가 파티션을 분할할 때 **자투리 group**이 생기고, 그 크기가 512~768MB에 걸리면 small file이 발생한다. 자투리 크기는 그 시간의 데이터 양으로 정해지므로 **매 시간 운에 맡기는 구조**였다.

T4는 분할하지 않아 자투리가 없다. T4의 414.8MB는 col_a=D 파티션(약 0.83GB) 자체가 작아서 나온 값이며(`830 ÷ 2 = 415`), 384MB를 넘으므로 문제없다. 이는 파티션 설계에서 오는 값이라 Compaction 옵션으로 해소할 수 없다.

> **판정 상태**: 메커니즘 추론과 관측값이 일치하나, `max-file-group-size-bytes=100GB`로 여러 시간치를 검증해 `min_size`가 계속 384MB를 넘는 것을 확인해야 확정된다.

**부수 효과**: T4의 `memory usage`가 84.73%로 가장 낮다(T2는 94.38%). group이 7개 → 4개로 줄어 동시 실행되는 shuffle이 줄어든 결과다.

---

## 5. 확정 설정

| 구분 | 설정 | 값 | 근거 수준 | 비고 |
|------|------|-----|----------|------|
| Iceberg | `target-file-size-bytes` | 536870912 (512MB) | ✅ | 스키마 설계에서 확정 |
| Iceberg | `rewrite-all` | true | ✅ | 입력이 전부 small file |
| Iceberg | `max-concurrent-file-group-rewrites` | **10** | ✅ | −30%. 여유 포함 |
| Iceberg | `max-file-group-size-bytes` | **기본값 100GB** (설정 제거) | ✅ | −16% + small file 제거 |
| Iceberg | `partial-progress.enabled` | 기본값 false | 📘 | 변경 이득 없음 |
| Spark | `advisory-partition-size` | **삭제** | ✅ | 무효 확인 |
| Spark | `coalescePartitions.parallelismFirst` | false | ⚠️ | 근거 미확정, 위험 비대칭으로 유지 |
| 리소스 | `driver cpu` / `memory` | **2** / 4GB | 📘 | |
| 리소스 | `executor cpu` / `memory` | 4 / 16GB | ✅ | spill 0 유지 |
| 리소스 | `num-executors` | 16 → **동적 산정** | ⚠️ | 섹션 6 |
| 전략 | rewrite 전략 | `sort` | ✅ | 미적용 시 조회 40% 저하 |

**변경 전후 요약**

| 항목 | 변경 전 | 변경 후 |
|------|--------|--------|
| 초/GB | 3.24 | **1.88 (−42%)** |
| idle cores | 58.22% | **24.90%** |
| file group 수 | 7 | **4** |
| 처리 회차 | 4 | **1** |
| min_size | 408.4MB (운에 의존) | **414.8MB (구조적으로 보장)** |
| DAG 전체 (테이블 4개) | 10~12분 | **6~7분 예상** |

---

## 6. 동적 리소스 산정

### 6.1 배경과 동적화 대상 축소

당초 목표는 데이터 증가에 맞춰 driver/executor의 cpu·memory·개수를 모두 동적으로 산정하는 것이었다. **테스트 결과 동적화가 필요한 값은 `num-executors` 하나로 좁혀졌다.**

| 설정 | 당초 계획 | 판정 | 이유 |
|------|----------|------|------|
| `num-executors` | 동적 | ✅ **동적 — 유일한 대상** | 데이터 양에 비례하는 유일한 값 |
| `executor memory` | 동적 | ❌ 고정 16GB | task 하나의 처리 단위가 512MB로 고정. 데이터가 2배가 되면 task 수가 2배 되고 task 크기는 그대로 → 메모리는 데이터 양과 무관 |
| `executor cpu` | 동적 | ❌ 고정 4 | 동일 |
| `driver cpu` / `memory` | 동적 | ❌ 고정 2 / 4GB | file group 수(4개)에 비례하나 변동 폭이 작음 |
| `max-concurrent-file-group-rewrites` | (계획 외) | ❌ 크게 고정 (10) | group 수보다 크면 남는 값은 사용되지 않음 → 동적화가 무의미 |
| `max-file-group-size-bytes` | (계획 외) | ❌ 크게 고정 (100GB) | 분할하지 않는 것이 목표 → 크게 두면 충족 |

**동적화 대상이 6개에서 1개로 줄어든 것은 성과다.** 나머지 5개는 손댈 필요가 없음이 실측으로 확인되어, 구현·유지보수 범위가 크게 줄었다.

### 6.2 duration을 일정하게 유지해야 하는 이유

`pipeline/reprocessing-dag-design.md` §6.2가 실측 duration을 근거로 maintenance 스케줄을 배치했다.

- hourly Compaction 시작 분: `M ≤ 60 − duration − 여유` = `60 − 12 − 3` = **`:45`**
- 설계에 명시: *"hourly duration이 15분을 넘으면 이 식으로 재계산해야 한다"*

정적 리소스는 데이터 증가 시 duration이 늘어 이 전제를 조용히 깨뜨린다. 동적 산정은 증가분을 리소스로 흡수해 duration을 상수에 가깝게 유지한다.

### 6.3 입력 측정

```sql
SELECT sum(file_size_in_bytes)
FROM <db>.<table>.files
WHERE <대상 시간 파티션>
```

hourly는 입력 파일이 **전부** small file이므로(최대 72.3MB) 크기 필터가 불필요하다. 총 크기만 측정하면 된다.

`.files`는 manifest만 읽으므로 S3 ListObjects가 필요 없다. Airflow task에서 Trino JDBC로 조회하는 것이 현실적이다.

### 6.4 산정식 (⚠️ 계수 미확정)

```python
num_executors = ceil(total_size_gb * C)
num_executors = min(max(num_executors, MIN_EXECUTORS), MAX_EXECUTORS)
```

**계수 C 캘리브레이션 상태**

| | num-executors | 총 크기 | C | idle cores | spill | 판정 |
|---|---|---|---|---|---|---|
| 검증됨 | 16 | 37GB | **0.43** | 24.90% | 0b | ✅ 동작 확인 |
| 미측정 | 12 | — | 0.32 | ? | ? | 측정 예정 |
| 미측정 | 8 | — | 0.22 | ? | ? | 측정 예정 |

`spill to disk = 0b`를 유지하면서 `idle cores`가 가장 낮은 지점의 C를 채택한다.

**상한·하한**

| 항목 | 값 | 근거 |
|------|-----|------|
| `MAX_EXECUTORS` | ⚠️ 미확정 | append 벤치마크에서 32개 이상은 오히려 느려졌다(shuffle 통신, K8S pod 스케줄링 경합, S3 부하 — `tuning/spark-tuning-guide.md` §2.2.3). K8S namespace quota도 확인 필요. **상한에 걸리면 알림을 발생시켜 파티션 재설계 검토 신호로 사용** |
| `MIN_EXECUTORS` | 4 (안) 📘 | 데이터가 적은 시간대에 과도하게 축소되는 것 방지 |

### 6.5 구현 위치

`pipeline/examples/compaction_dag_example.py`의 `compaction_specs` task에 자리가 있다.

```python
"instances": str(table.config.com_num_executor),   # ← 계산값으로 교체
```

`.files` 조회를 위한 Trino 연결부만 신규 구현이 필요하다.

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

이번 사례는 후자였다(`max-concurrent-file-group-rewrites=2`). **DataFlint alert는 전자만 제안하므로 처방을 그대로 따르면 안 된다** — Iceberg 옵션을 인식하지 못한다.

### 7.3 DataFlint에 없어 별도로 확인할 것

| 항목 | 확인 위치 | 필요 이유 |
|------|----------|----------|
| **min_size / max_size / file_count** | Iceberg `.files` 메타데이터 | **Compaction 품질의 핵심 지표인데 DataFlint에 없다.** T1~T3의 small file 문제는 이것만 드러낸다. duration이 개선되어도 min_size가 384MB 미달이면 실패다 |
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

hourly가 매시간 출력을 75개 × 505MB(전부 384MB 이상)로 정리한다면, daily는 **합칠 small file이 없어 사실상 no-op이어야 한다.** 그런데 데이터 양에 선형으로 소요된다.

**가설**: daily도 `rewrite-all: true`로 888GB 전체를 다시 쓰고 있다. hourly와 달리 daily에서는 `rewrite-all: false`가 큰 이득일 수 있다.

daily 단계에서 최우선으로 확인할 항목이다.

### 8.2 남은 확인 항목

| 항목 | 내용 | 우선순위 |
|------|------|---------|
| `max-file-group-size-bytes` 100GB 검증 | 30GB와 동일하게 동작하는지. **min_size > 384MB, duration 1.2~1.4분** 확인 | 높음 |
| `num-executors` 축소 (C 캘리브레이션) | 12, 8 두 지점 측정. `spill 0` 유지가 조건 (섹션 6.4) | 높음 |
| `MAX_EXECUTORS` 확정 | K8S namespace quota 확인. append(batch당 약 10 executor)와 동시 실행됨 | 중간 |
| `parallelismFirst` 판정 | 제거 후 `file_count` 확인 (섹션 3.2) | 낮음 |
| `ts` timezone 검증 | Airflow가 전달하는 from/until의 `timestamp_ntz` 처리 (섹션 3.4) | 중간 |
| executor local disk 한도 | 파티션이 커질 때 shuffle 저장 공간 (섹션 3.1) | 낮음 |

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
