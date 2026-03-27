# Iceberg 파티션 전략별 읽기 성능 비교 테스트

### 목차

- [1. 테스트 설계](#1-테스트-설계) — 테이블 구성, 공통 조건
- [2. Compaction 후 파티션 분포](#2-compaction-후-파티션-분포) — 전략별 파일/파티션 분포 비교
- [3. 테스트 결과](#3-테스트-결과) — 쿼리별 EXPLAIN ANALYZE, Web UI 캡처
- [4. 종합 분석](#4-종합-분석) — 결과 요약, 결론

---

## 1. 테스트 설계

**테스트 테이블**

| 항목 | Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|------|-----------------|-----------------|-----|-----|-----|
| 포맷 | Hive (TextFile/CSV) | Hive (ORC) | Iceberg (Parquet) | Iceberg (Parquet) | Iceberg (Parquet) |
| 파티션 | DT(날짜), sort_b | DT(날짜) | `day(ts)`, `par_a`, `par_b` | `hour(ts)`, `par_a` | `day(ts)`, `par_a`, `bucket(16, par_b)` |
| Sort Order | — | — | `sort_a`, `sort_b`, `sort_c` | `par_b`, `sort_a`, `sort_b`, `sort_c` | `sort_a`, `sort_b`, `sort_c` |
| distribution-mode | — | — | `range` | `range` | `range` |

**공통 조건**

- 쿼리당 **5회** 실행, 전체 평균으로 비교
- 소요시간: Trino Web UI Overview의 **Elapsed Time** 기준

**테스트 쿼리**: 2개 쿼리 × 2개 조건 = 4개 테스트 케이스

- **ts 필터 기준**: A안과 C안은 `day(ts)` 파티션에 맞춰 day 단위, B안은 `hour(ts)` 파티션에 맞춰 hour 단위로 고정
- **조건 A / 조건 B**: 서로 다른 데이터를 조회하기 위한 조건 변경 (par_a, par_b 등 필터값 변경)

**Web UI 캡처 대상**: 각 쿼리 실행 후 아래 3개 탭을 각 테이블별로 캡처

| 캡처 대상 | 비교 포인트 |
|----------|-----------|
| Resource Utilization | Physical Input, CPU Time, Scheduled Time |
| Stage Performance | Operator별 throughput, CPU, Wall Time |
| Timeline | Parallelism, Input rows/s, Physical Input Bytes |

**Trino Web UI Overview 주요 메트릭 해석** ([Web UI](https://trino.io/docs/current/admin/web-interface.html) · [EXPLAIN ANALYZE](https://trino.io/docs/current/sql/explain-analyze.html) · [Query Management Properties](https://trino.io/docs/current/admin/properties-query-management.html))

| 메트릭 | 설명 | 성능 비교 시 해석 |
|--------|------|-----------------|
| **Elapsed Time** | 쿼리 제출부터 완료까지 총 소요시간 (wall-clock). Planning + Execution + 대기시간 포함 | 사용자 체감 성능. 최종 비교 기준 |
| **Planning Time** | 쿼리 계획 수립 소요시간. [`query.max-planning-time`](https://trino.io/docs/current/admin/properties-query-management.html) 참조 | 메타데이터 크기(파일 수)에 비례. 파일 수가 많으면 증가 |
| **Execution Time** | Planning 이후 실제 실행 시간. 분석·계획·대기시간 제외 ([`query.max-execution-time`](https://trino.io/docs/current/admin/properties-query-management.html) 참조) | I/O + 연산 시간 |
| **CPU Time** | 전체 Worker에서 사용한 누적 CPU 시간 ([EXPLAIN ANALYZE](https://trino.io/docs/current/sql/explain-analyze.html)의 Fragment별 CPU time 합산) | 연산 작업량 지표. 스캔 데이터가 많으면 증가 |
| **Scheduled Time** | 태스크가 프로세서에 스케줄된 총 시간. CPU 사용 + context switch 대기 포함 ([EXPLAIN ANALYZE](https://trino.io/docs/current/sql/explain-analyze.html)의 scheduled time) | CPU Time / Scheduled Time = CPU 활용률 |
| **Blocked Time** | Stage 간 데이터 전송 대기 시간 (Input/Output blocking 합산, 전체 스레드 누적) | Blocked >> CPU면 I/O 병목, Blocked << CPU면 연산 병목 |
| **Physical Input Rows** | 스토리지에서 실제 읽은 행 수 ([EXPLAIN ANALYZE](https://trino.io/docs/current/sql/explain-analyze.html)의 Physical input rows) | 파티션 프루닝·Data Skipping 효과 직접 반영. **핵심 비교 지표** |
| **Physical Input Data** | 스토리지에서 전송된 바이트 수 (압축 상태) | 실제 I/O 양. 적을수록 프루닝이 잘 동작한 것 |
| **Input Rows / Data** | Connector 최적화 후 처리된 논리적 행 수 / 크기 | Physical과 차이가 크면 압축·인코딩 효율이 높은 것 |
| **Output Rows / Data** | 최종 결과 행 수 / 크기 | 모든 전략에서 동일해야 함 (같은 쿼리이므로) |
| **Peak User Memory** | 단일 Worker에서 최대 사용 메모리 ([Memory Management](https://trino.io/docs/current/admin/properties-memory-management.html)의 `query.max-memory-per-node` 제한) | 스캔 범위가 클수록 증가. 메모리 부담 비교 |
| **Cumulative User Memory** | 메모리 사용량 × 시간 적분 (GB·s). AVG(usage) × duration으로 근사 산출 | 지속적 메모리 부하 지표 |

> **성능 비교 핵심**: 동일 쿼리 결과(Output Rows 동일)를 얻기 위해 **Physical Input Rows/Data가 얼마나 적은지**가 파티션 전략의 효과를 직접 반영한다. Physical Input이 적으면 프루닝이 잘 된 것이고, 이에 비례하여 CPU Time과 Elapsed Time도 감소한다.

**EXPLAIN ANALYZE 결과 해석 가이드** ([공식 문서](https://trino.io/docs/current/sql/explain-analyze.html))

EXPLAIN ANALYZE는 쿼리를 실제 실행한 뒤 분산 실행 계획과 각 단계별 비용을 출력한다. 출력 구조는 다음과 같다:

```
Trino version: 4xx
Queued: 374.17us, Analysis: 190.96ms, Planning: 179.03ms, Execution: 3.06s
                                        ↑                    ↑              ↑
                                   쿼리 분석 시간      실행 계획 수립     실제 실행 시간

Fragment 1 [SINGLE]
    CPU: 22.58ms, Scheduled: 96.72ms, Blocked: 46.21s (Input: 23.06s, Output: 0.00ns)
    Input: 1000 rows (37.11kB); per task: avg 1000.00 std.dev. 0.00
        ↓
    - Output[...] => [column_list]
        - Aggregate(FINAL)[...] => [column_list]
            CPU: 8.00ms (3.51%), Scheduled: 63.00ms (15.11%), ...
            - LocalExchange => [column_list]
                - RemoteSource[2] => [column_list]

Fragment 2 [HASH]
    CPU: ..., Scheduled: ..., Blocked: ...
    Input: 818058 rows (...)
        - Aggregate(PARTIAL)[...] => [column_list]
            - ScanFilterProject[table = ..., filterPredicate = ...]
                CPU: ..., Scheduled: ..., ...
                Input avg.: 818058.00 rows, Input std.dev.: 0.00%
                Filtered: 45.46%
                Physical input: 4.51MB
```

**헤더 (Queued / Analysis / Planning / Execution)**

| 항목 | 설명 |
|------|------|
| Queued | 실행 큐 대기 시간 |
| Analysis | SQL 구문 분석 및 의미 검증 시간 |
| Planning | 실행 계획 수립 시간 (메타데이터 읽기 포함) |
| Execution | 실제 실행 시간 (Planning 이후) |

**Fragment 헤더**

| 항목 | 설명 | 해석 |
|------|------|------|
| Fragment N [TYPE] | 분산 실행 단계. 번호가 클수록 먼저 실행됨 (데이터 소스가 마지막 Fragment) | [SINGLE]: 1개 노드, [HASH]: 해시 분배, [SOURCE]: 데이터 소스 |
| CPU | 해당 Fragment에서 사용한 실제 CPU 시간 | 연산 비용 |
| Scheduled | 프로세서에 스케줄된 총 시간 (CPU + context switch 대기) | CPU 대비 크면 자원 경합 발생 |
| Blocked | 데이터 전송 대기 시간 (Input: 수신 대기, Output: 송신 대기) | Blocked >> CPU면 I/O 병목 |
| Input | Fragment에 입력된 행 수 / 크기 | 스캔 범위 지표 |
| per task: avg / std.dev. | 태스크당 평균 입력 행 수와 표준편차 | std.dev.가 크면 데이터 Skew |

**주요 Operator**

| Operator | 설명 | 성능 비교 시 주목점 |
|----------|------|-------------------|
| **ScanFilterProject** | 테이블 스캔 + 필터 + 프로젝션을 합친 연산자 | `Physical input`: 스토리지에서 읽은 실제 데이터 크기, `Filtered`: 필터로 제거된 행 비율 |
| **Aggregate (PARTIAL/FINAL)** | 부분/최종 집계 연산 | PARTIAL은 Worker별, FINAL은 Coordinator에서 실행 |
| **LocalExchange** | Worker 내에서 데이터 재분배 | 로컬 셔플 비용 |
| **RemoteSource** | 다른 Fragment에서 데이터 수신 | Fragment 간 네트워크 전송 |

**ScanFilterProject 핵심 지표**

| 지표 | 설명 | 파티션 전략 비교 시 해석 |
|------|------|----------------------|
| **Physical input** | 스토리지에서 실제 읽은 바이트 수 | **핵심 지표**. 파티션 프루닝/Data Skipping이 잘 되면 감소 |
| **Filtered** | 스캔 후 필터로 제거된 행 비율 (%) | 높으면 불필요한 데이터를 많이 읽은 것. 프루닝이 잘 되면 낮아짐 |
| **Input avg. / std.dev.** | 태스크당 평균 입력 행 수와 표준편차 | std.dev.가 높으면 데이터 Skew. 파티션 간 데이터 편차 확인 |
| **CPU / Scheduled (%)** | 전체 쿼리 대비 이 연산자의 비용 비율 | 스캔이 차지하는 비율이 높으면 I/O 바운드 |

> **비교 방법**: 동일 쿼리를 각 전략 테이블에서 실행한 뒤, ScanFilterProject의 `Physical input`과 `Filtered` 비율을 비교한다. Physical input이 적고 Filtered가 낮을수록 해당 전략의 파티션 프루닝/Data Skipping이 효과적이다. `Input std.dev.`가 높은 전략은 Skew 문제가 있음을 의미한다.

---

## 2. 파티션 분포

1일치 데이터(2026-03-18) 기준. Iceberg는 `spark.sql.iceberg.advisory-partition-size = 768MB`로 Compaction 후 측정.

> **데이터 보관 현황**: Hive-raw는 10일치, Hive-orc는 3개월치 운영 중. A안/B안/C안은 1일치 테스트 데이터만 존재. 아래 통계는 모두 **1일치 기준**.

| 항목 | Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|------|-----------------|-----------------|-----|-----|-----|
| 스토리지 | HDFS (블록 128MB) | HDFS (블록 128MB) | S3 (MinIO) | S3 (MinIO) | S3 (MinIO) |
| 파일 포맷 | TextFile (CSV) | ORC | Parquet | Parquet | Parquet |
| 파티션 | DT(날짜), sort_b | DT(날짜) | day(ts), par_a, par_b | hour(ts), par_a | day(ts), par_a, bucket(16, par_b) |
| 총 파일 수 | 758,856 | 1,009 | 1,985 | 1,823 | 1,847 |
| 총 크기 | 15.3TB | 4.9TB | 912.6GB | 912.7GB | 912.6GB |
| 파일 크기 avg | 21.2MB | 5.0GB | 172.9MB | 497.9MB | 396.7MB |
| 파일 크기 min | 5.0MB | 2.2GB | 0.6MB | 153.5MB | 0.8MB |
| 파일 크기 max | 7,408.9MB | 8.9GB | 718.2MB | 691.8MB | 719.9MB |
| 파티션당 파일 수 (max/min) | 125 / 1 | — | 449 / 1 | 42 / 1 | 488 / 1 |
| 파일 1개 파티션 수 | — | — | 190 | 22 | 22 |
| 그 중 384MB 미만 | — | — | 182 | 2 | 18 |

> **small file 기준**: Iceberg [`SizeBasedFileRewriter`](https://iceberg.apache.org/javadoc/1.4.1/org/apache/iceberg/actions/SizeBasedFileRewriter.html)의 `MIN_FILE_SIZE_DEFAULT_RATIO = 0.75` 기준, **384MB 미만이 Compaction 대상(small file)**.
>
> - **Hive-raw**: TextFile(CSV), 압축 없음. sort_b 파티션(Cardinality 25,820)으로 파일이 극도로 세분화되어 1일 758,856개. 10일치 보관
> - **Hive-orc**: ORC 압축으로 4.9TB. 수직분할 4개 테이블의 합산 데이터이므로 Iceberg(TABLE_A 1개)와 직접적인 크기 비교 불가. 파티션이 날짜 1개뿐이라 세부 필터 시 전체 스캔 필요. 3개월치 보관
> - **A안**: 구조적 Skew 심각. 파일 1개 파티션 190개 중 182개가 384MB 미만 — 데이터가 적은 par_b 파티션이 원인이며, Compaction으로도 해결 불가
> - **B안**: avg 497.9MB로 목표에 근접하고 min 153.5MB로 전 전략 중 가장 균등. 384MB 미만 파일은 단 2개로 실질적 small file 문제 없음
> - **C안**: bucket(16)으로 par_b Skew를 분산하지만, bucket 내 데이터 편차로 min 0.8MB 발생. 384MB 미만 파일이 18개로 A안(182개)보다 대폭 개선되었으나 B안(2개)보다는 많음

---

## 3. 테스트 결과

### 3.1 쿼리 1

#### 조건 A

**Hive-raw / Hive-orc 쿼리** (동일 쿼리, FROM 테이블만 다름)

```sql
-- FROM: hive_raw.db.TABLE_A / hive_orc.db.TABLE_A
-- (실제 쿼리 붙여넣기)
```

**A안 / C안 쿼리** (동일 쿼리, FROM 테이블만 다름 — day 필터)

```sql
-- FROM: iceberg.db.TABLE_A (A안) / iceberg.db.TABLE_A (C안)
-- (실제 쿼리 붙여넣기)
```

**B안 쿼리** (hour 필터)

```sql
-- FROM: iceberg.db.TABLE_A (B안)
-- (실제 쿼리 붙여넣기)
```

**EXPLAIN ANALYZE**

Hive-raw:
```
(결과 붙여넣기)
```

Hive-orc:
```
(결과 붙여넣기)
```

A안:
```
(결과 붙여넣기)
```

B안:
```
(결과 붙여넣기)
```

C안:
```
(결과 붙여넣기)
```

**Web UI — Resource Utilization**

| Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-----------------|-----------------|-----|-----|-----|
| (캡처) | (캡처) | (캡처) | (캡처) | (캡처) |

**Web UI — Stage Performance**

| Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-----------------|-----------------|-----|-----|-----|
| (캡처) | (캡처) | (캡처) | (캡처) | (캡처) |

**Web UI — Timeline**

| Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-----------------|-----------------|-----|-----|-----|
| (캡처) | (캡처) | (캡처) | (캡처) | (캡처) |

**Elapsed Time**

| 회차 | Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|------|-----------------|-----------------|-----|-----|-----|
| 1회 | | | | | |
| 2회 | | | | | |
| 3회 | | | | | |
| 4회 | | | | | |
| 5회 | | | | | |
| **평균** | | | | | |

**분석**: (결과 기반 분석)

---

#### 조건 B

**Hive-raw / Hive-orc 쿼리** (동일 쿼리, FROM 테이블만 다름)

```sql
-- FROM: hive_raw.db.TABLE_A / hive_orc.db.TABLE_A
-- (실제 쿼리 붙여넣기)
```

**A안 / C안 쿼리** (동일 쿼리, FROM 테이블만 다름 — day 필터)

```sql
-- FROM: iceberg.db.TABLE_A (A안) / iceberg.db.TABLE_A (C안)
-- (실제 쿼리 붙여넣기)
```

**B안 쿼리** (hour 필터)

```sql
-- FROM: iceberg.db.TABLE_A (B안)
-- (실제 쿼리 붙여넣기)
```

**EXPLAIN ANALYZE**

Hive-raw:
```
(결과 붙여넣기)
```

Hive-orc:
```
(결과 붙여넣기)
```

A안:
```
(결과 붙여넣기)
```

B안:
```
(결과 붙여넣기)
```

C안:
```
(결과 붙여넣기)
```

**Web UI — Resource Utilization**

| Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-----------------|-----------------|-----|-----|-----|
| (캡처) | (캡처) | (캡처) | (캡처) | (캡처) |

**Web UI — Stage Performance**

| Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-----------------|-----------------|-----|-----|-----|
| (캡처) | (캡처) | (캡처) | (캡처) | (캡처) |

**Web UI — Timeline**

| Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-----------------|-----------------|-----|-----|-----|
| (캡처) | (캡처) | (캡처) | (캡처) | (캡처) |

**Elapsed Time**

| 회차 | Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|------|-----------------|-----------------|-----|-----|-----|
| 1회 | | | | | |
| 2회 | | | | | |
| 3회 | | | | | |
| 4회 | | | | | |
| 5회 | | | | | |
| **평균** | | | | | |

**분석**: (결과 기반 분석)

---

### 3.2 쿼리 2

#### 조건 A

**Hive-raw / Hive-orc 쿼리** (동일 쿼리, FROM 테이블만 다름)

```sql
-- FROM: hive_raw.db.TABLE_A / hive_orc.db.TABLE_A
-- (실제 쿼리 붙여넣기)
```

**A안 / C안 쿼리** (동일 쿼리, FROM 테이블만 다름 — day 필터)

```sql
-- FROM: iceberg.db.TABLE_A (A안) / iceberg.db.TABLE_A (C안)
-- (실제 쿼리 붙여넣기)
```

**B안 쿼리** (hour 필터)

```sql
-- FROM: iceberg.db.TABLE_A (B안)
-- (실제 쿼리 붙여넣기)
```

**EXPLAIN ANALYZE**

Hive-raw:
```
(결과 붙여넣기)
```

Hive-orc:
```
(결과 붙여넣기)
```

A안:
```
(결과 붙여넣기)
```

B안:
```
(결과 붙여넣기)
```

C안:
```
(결과 붙여넣기)
```

**Web UI — Resource Utilization**

| Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-----------------|-----------------|-----|-----|-----|
| (캡처) | (캡처) | (캡처) | (캡처) | (캡처) |

**Web UI — Stage Performance**

| Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-----------------|-----------------|-----|-----|-----|
| (캡처) | (캡처) | (캡처) | (캡처) | (캡처) |

**Web UI — Timeline**

| Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-----------------|-----------------|-----|-----|-----|
| (캡처) | (캡처) | (캡처) | (캡처) | (캡처) |

**Elapsed Time**

| 회차 | Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|------|-----------------|-----------------|-----|-----|-----|
| 1회 | | | | | |
| 2회 | | | | | |
| 3회 | | | | | |
| 4회 | | | | | |
| 5회 | | | | | |
| **평균** | | | | | |

**분석**: (결과 기반 분석)

---

#### 조건 B

**Hive-raw / Hive-orc 쿼리** (동일 쿼리, FROM 테이블만 다름)

```sql
-- FROM: hive_raw.db.TABLE_A / hive_orc.db.TABLE_A
-- (실제 쿼리 붙여넣기)
```

**A안 / C안 쿼리** (동일 쿼리, FROM 테이블만 다름 — day 필터)

```sql
-- FROM: iceberg.db.TABLE_A (A안) / iceberg.db.TABLE_A (C안)
-- (실제 쿼리 붙여넣기)
```

**B안 쿼리** (hour 필터)

```sql
-- FROM: iceberg.db.TABLE_A (B안)
-- (실제 쿼리 붙여넣기)
```

**EXPLAIN ANALYZE**

Hive-raw:
```
(결과 붙여넣기)
```

Hive-orc:
```
(결과 붙여넣기)
```

A안:
```
(결과 붙여넣기)
```

B안:
```
(결과 붙여넣기)
```

C안:
```
(결과 붙여넣기)
```

**Web UI — Resource Utilization**

| Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-----------------|-----------------|-----|-----|-----|
| (캡처) | (캡처) | (캡처) | (캡처) | (캡처) |

**Web UI — Stage Performance**

| Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-----------------|-----------------|-----|-----|-----|
| (캡처) | (캡처) | (캡처) | (캡처) | (캡처) |

**Web UI — Timeline**

| Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-----------------|-----------------|-----|-----|-----|
| (캡처) | (캡처) | (캡처) | (캡처) | (캡처) |

**Elapsed Time**

| 회차 | Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|------|-----------------|-----------------|-----|-----|-----|
| 1회 | | | | | |
| 2회 | | | | | |
| 3회 | | | | | |
| 4회 | | | | | |
| 5회 | | | | | |
| **평균** | | | | | |

**분석**: (결과 기반 분석)

---

## 4. 종합 분석

### Elapsed Time 평균 비교

| 테스트 케이스 | Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|-------------|-----------------|-----------------|-----|-----|-----|
| 쿼리 1 — 조건 A | | | | | |
| 쿼리 1 — 조건 B | | | | | |
| 쿼리 2 — 조건 A | | | | | |
| 쿼리 2 — 조건 B | | | | | |

### 결론

(테스트 결과 기반 최종 판단)
