# Hive vs A안 vs D안 읽기 성능 비교 테스트

### 목차

- [1. 테스트 설계](#1-테스트-설계) — 테이블 구성, 공통 조건
- [2. 테스트 시 주의사항](#2-테스트-시-주의사항) — Web UI 캡처 가이드
- [3. 테스트 결과](#3-테스트-결과) — 쿼리별 EXPLAIN ANALYZE, Web UI 캡처
- [4. 종합 분석](#4-종합-분석) — 결과 요약, 결론

---

## 1. 테스트 설계

Hive 테이블(현행)과 Iceberg A안/D안의 읽기 성능을 실측 비교하여 최종 파티션 전략을 결정한다.

**테스트 테이블**

| 항목 | Hive (as-is) | A안 | D안 |
|------|-------------|-----|-----|
| 포맷 | Hive | Iceberg | Iceberg |
| 파티션 | (현행 설정) | `day(ts)`, `par_a`, `par_b` | `hour(ts)`, `par_a` |
| Sort Order | — | `sort_a`, `sort_b`, `sort_c` | `par_b`, `sort_a`, `sort_b`, `sort_c` |
| distribution-mode | — | `range` | `range` |

**공통 조건**

- 동일한 1일치 데이터 (~851GB, 144배치)
- 쿼리당 **5회** 실행, 전체 평균으로 비교
- Trino는 데이터 캐싱이 없으므로(S3에서 매번 새로 읽음) cold/warm 구분 불필요
- 소요시간: Trino Web UI Overview의 **Elapsed Time** 기준

**테스트 쿼리**

| 쿼리 | 패턴 | 목적 |
|------|------|------|
| 쿼리 1 | 날짜 + 전체 필터 | 가장 빈번한 패턴. par_b 파티션 프루닝 효과 비교 |
| 쿼리 2 | 시간 + 전체 필터 | D안 hour 파티션 프루닝 효과 비교 |

각 쿼리는 WHERE 조건을 2가지로 변경하여 테스트한다 (조건 A, 조건 B).

---

## 2. 테스트 시 주의사항

- **반복 실행**: 5회 실행 전체 평균을 사용
- **Compaction 전후 둘 다 측정**: A안은 Compaction 전후 차이가 크므로 양쪽 다 기록. D안도 Compaction 전후를 측정하여 "Compaction 선택적"이 실제로 유효한지 확인
- **다양한 par_b 값으로 테스트**: Skew가 큰 상위 par_b 값(데이터 많음)과 하위 par_b 값(데이터 적음) 모두 테스트

**Web UI 캡처 가이드**

각 쿼리 실행 후 Trino Web UI에서 아래 3개 탭을 Hive/A안/D안 각각 캡처한다.

| 캡처 대상 | 탭 위치 | 비교 포인트 |
|----------|---------|-----------|
| Resource Utilization | Overview → Resource Utilization Summary | 핵심 수치 — Physical Input, CPU Time, Scheduled Time |
| Stage Performance | Stage Performance 탭 | 병목 구간 — Operator별 throughput, CPU, Wall Time |
| Timeline | Timeline 탭 | 처리 패턴 — Parallelism, Input rows/s, Physical Input Bytes |

---

## 3. 테스트 결과

### 3.1 쿼리 1: 날짜 + 전체 필터

#### 조건 A

```sql
-- (실제 쿼리 붙여넣기)
```

**EXPLAIN ANALYZE**

Hive:
```
(결과 붙여넣기)
```

A안:
```
(결과 붙여넣기)
```

D안:
```
(결과 붙여넣기)
```

**Web UI — Resource Utilization**

| Hive | A안 | D안 |
|------|-----|-----|
| (캡처) | (캡처) | (캡처) |

**Web UI — Stage Performance**

| Hive | A안 | D안 |
|------|-----|-----|
| (캡처) | (캡처) | (캡처) |

**Web UI — Timeline**

| Hive | A안 | D안 |
|------|-----|-----|
| (캡처) | (캡처) | (캡처) |

**Elapsed Time**

| 회차 | Hive | A안 | D안 |
|------|------|-----|-----|
| 1회 | | | |
| 2회 | | | |
| 3회 | | | |
| 4회 | | | |
| 5회 | | | |
| **평균** | | | |

**분석**: (결과 기반 분석)

---

#### 조건 B

```sql
-- (실제 쿼리 붙여넣기)
```

**EXPLAIN ANALYZE**

Hive:
```
(결과 붙여넣기)
```

A안:
```
(결과 붙여넣기)
```

D안:
```
(결과 붙여넣기)
```

**Web UI — Resource Utilization**

| Hive | A안 | D안 |
|------|-----|-----|
| (캡처) | (캡처) | (캡처) |

**Web UI — Stage Performance**

| Hive | A안 | D안 |
|------|-----|-----|
| (캡처) | (캡처) | (캡처) |

**Web UI — Timeline**

| Hive | A안 | D안 |
|------|-----|-----|
| (캡처) | (캡처) | (캡처) |

**Elapsed Time**

| 회차 | Hive | A안 | D안 |
|------|------|-----|-----|
| 1회 | | | |
| 2회 | | | |
| 3회 | | | |
| 4회 | | | |
| 5회 | | | |
| **평균** | | | |

**분석**: (결과 기반 분석)

---

### 3.2 쿼리 2: 시간 + 전체 필터

#### 조건 A

```sql
-- (실제 쿼리 붙여넣기)
```

**EXPLAIN ANALYZE**

Hive:
```
(결과 붙여넣기)
```

A안:
```
(결과 붙여넣기)
```

D안:
```
(결과 붙여넣기)
```

**Web UI — Resource Utilization**

| Hive | A안 | D안 |
|------|-----|-----|
| (캡처) | (캡처) | (캡처) |

**Web UI — Stage Performance**

| Hive | A안 | D안 |
|------|-----|-----|
| (캡처) | (캡처) | (캡처) |

**Web UI — Timeline**

| Hive | A안 | D안 |
|------|-----|-----|
| (캡처) | (캡처) | (캡처) |

**Elapsed Time**

| 회차 | Hive | A안 | D안 |
|------|------|-----|-----|
| 1회 | | | |
| 2회 | | | |
| 3회 | | | |
| 4회 | | | |
| 5회 | | | |
| **평균** | | | |

**분석**: (결과 기반 분석)

---

#### 조건 B

```sql
-- (실제 쿼리 붙여넣기)
```

**EXPLAIN ANALYZE**

Hive:
```
(결과 붙여넣기)
```

A안:
```
(결과 붙여넣기)
```

D안:
```
(결과 붙여넣기)
```

**Web UI — Resource Utilization**

| Hive | A안 | D안 |
|------|-----|-----|
| (캡처) | (캡처) | (캡처) |

**Web UI — Stage Performance**

| Hive | A안 | D안 |
|------|-----|-----|
| (캡처) | (캡처) | (캡처) |

**Web UI — Timeline**

| Hive | A안 | D안 |
|------|-----|-----|
| (캡처) | (캡처) | (캡처) |

**Elapsed Time**

| 회차 | Hive | A안 | D안 |
|------|------|-----|-----|
| 1회 | | | |
| 2회 | | | |
| 3회 | | | |
| 4회 | | | |
| 5회 | | | |
| **평균** | | | |

**분석**: (결과 기반 분석)

---

## 4. 종합 분석

### Elapsed Time 평균 비교

| 테스트 케이스 | Hive (as-is) | A안 | D안 |
|-------------|-------------|-----|-----|
| 쿼리 1 — 조건 A | | | |
| 쿼리 1 — 조건 B | | | |
| 쿼리 2 — 조건 A | | | |
| 쿼리 2 — 조건 B | | | |

### 결론

(테스트 결과 기반 최종 판단)
