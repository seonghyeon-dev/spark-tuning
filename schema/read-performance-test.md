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
| 포맷 | | Hive | Iceberg | Iceberg | Iceberg |
| 파티션 | | (현행 설정) | `day(ts)`, `par_a`, `par_b` | `hour(ts)`, `par_a` | `day(ts)`, `par_a`, `bucket(16, par_b)` |
| Sort Order | | — | `sort_a`, `sort_b`, `sort_c` | `par_b`, `sort_a`, `sort_b`, `sort_c` | `sort_a`, `sort_b`, `sort_c` |
| distribution-mode | | — | `range` | `range` | `range` |

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

---

## 2. Compaction 후 파티션 분포

1일치 데이터(2026-03-18) 기준, Compaction 후 측정 결과.

| 항목 | Hive-raw (as-is) | Hive-orc (as-is) | A안 | B안 | C안 |
|------|-----------------|-----------------|-----|-----|-----|
| 스토리지 | | HDFS (블록 128MB) | S3 (MinIO) | S3 (MinIO) | |
| 파일 포맷 | | ORC | Parquet | Parquet | |
| 파티션 수 | | 1 (dt=날짜) | 253 | 96 | |
| 총 파일 수 | | 1,008 | 1,987 | 1,833 | |
| 총 크기 | | 4.9TB | 912.6GB | 912.7GB | |
| 파일 크기 avg | | 5.0GB | 172.9MB | 495.7MB | |
| 파일 크기 min | | 2.2GB | 0.6MB | 10.8MB | |
| 파일 크기 max | | 8.9GB | 716.5MB | 721MB | |
| 128MB 미만 파일 수 | | — | 161개 (8.1%) | 2개 (0.1%) | |

> **small file 기준**: Iceberg [`SizeBasedFileRewriter`](https://iceberg.apache.org/javadoc/1.4.1/org/apache/iceberg/actions/SizeBasedFileRewriter.html)의 `MIN_FILE_SIZE_DEFAULT_RATIO = 0.75` 기준, target 512MB의 75%인 **384MB 미만이 Compaction 대상(small file)**. 아래 분석은 128MB 미만 파일 수를 참고 지표로 함께 기재.
>
> - **Hive-orc 총 크기가 큰 이유**: Hive-orc 테이블은 수직분할 4개 테이블의 합산 데이터. Iceberg는 그 중 1개 테이블(TABLE_A)만 대상이므로 직접적인 크기 비교는 불가
> - **A안 small file 문제**: avg 172.9MB로 target 512MB의 34% 수준. 128MB 미만 파일만 161개(8.1%). 데이터가 적은 par_b 파티션의 구조적 Skew로 Compaction으로도 해결 불가
> - **B안 파일 크기 균등**: avg 495.7MB로 목표(512MB)에 근접. 128MB 미만 파일은 10.8MB, 107.2MB 단 2개(0.1%)로 실질적 small file 문제 없음
> - **Hive-orc 파일 크기**: 파일당 2.2~8.9GB. 파티션이 날짜 1개뿐이라 par_b 등 세부 필터 시 전체 스캔 필요

---

## 3. 테스트 결과

### 3.1 쿼리 1

#### 조건 A

```sql
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

```sql
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

```sql
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

```sql
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
