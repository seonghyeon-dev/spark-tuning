# TABLE_A Trino 쿼리 가이드

### 목차

- [1. 이 문서의 목적](#1-이-문서의-목적) — 대상 독자, 범위
- [2. TABLE_A 구조 요약](#2-table_a-구조-요약) — WHERE 컬럼 역할, 3단계 필터링 원리
- [3. ts 컬럼 필터링 방법](#3-ts-컬럼-필터링-방법) — 날짜/시간 필터 패턴, 등가/범위 조건
- [4. WHERE 조건별 성능 효과](#4-where-조건별-성능-효과) — 필수 컬럼별 최적화 효과
- [5. 쿼리 패턴 가이드](#5-쿼리-패턴-가이드) — 올바른 쿼리 템플릿, 실전 패턴
- [6. 잘못된 쿼리 패턴](#6-잘못된-쿼리-패턴) — 흔한 실수와 교정
- [7. 성능 확인 방법](#7-성능-확인-방법) — EXPLAIN ANALYZE 간이 가이드
- [8. 용어집](#8-용어집)

> **테이블 설계 상세**: [iceberg-schema-design-guide.md](iceberg-schema-design-guide.md) 참조

---

## 1. 이 문서의 목적

이 문서는 **Trino를 통해 TABLE_A를 조회하는 사용자**가 올바른 쿼리를 작성할 수 있도록 안내한다.

WHERE 조건을 어떻게 작성하느냐에 따라 읽기 성능이 크게 달라진다. 조건을 잘못 쓰면 **결과가 조회되지 않거나**, 불필요한 데이터를 전부 읽어 **쿼리가 수 배 느려진다.**

**다루는 것**

- WHERE 절에 어떤 컬럼을 어떻게 필터링해야 하는지
- 각 조건이 성능에 미치는 효과
- ts 컬럼의 날짜/시간 필터링 방법
- 잘못된 쿼리 패턴과 교정 방법

**다루지 않는 것**

- 테이블 설계 근거 (파티션, Sort Order 선택 이유)
- Compaction, 적재 파이프라인 등 운영 관련 사항

---

## 2. TABLE_A 구조 요약

### 2.1 WHERE 컬럼 역할

| 컬럼 | 타입 | 성능 최적화 역할 | WHERE 필수 |
|------|------|----------------|-----------|
| ts | timestamp_ntz | Partition Pruning (hour 단위) | **필수** |
| par_a | string | Partition Pruning (identity) | **필수** |
| sort_a | string | Data Skipping (Sort Order 1순위) | **필수** |
| sort_c | string | Data Skipping (Sort Order 2순위) | **필수** |
| par_b | string | Row-level Filter | 선택 |
| sort_b | string | Row-level Filter | 선택 |

- **WHERE 필수**: Partition Pruning(파티션 단위 건너뛰기) 또는 Data Skipping(파일 단위 건너뛰기)에 해당하는 컬럼. 빠뜨리면 성능이 크게 저하된다
- **선택**: 결과 필터링에 사용되지만, 파티션/파일 단위 최적화에는 영향 없음

### 2.2 3단계 필터링 원리

쿼리가 실행되면 데이터는 3단계로 걸러진다. 앞 단계에서 많이 걸러낼수록 빠르다.

```
1단계: Partition Pruning (파티션 단위)
  ── ts 조건으로 해당 시간대의 파티션만 선택
  ── par_a 조건으로 해당 값의 파티션만 선택
  ── 나머지 파티션은 아예 읽지 않음

2단계: Data Skipping (파일 단위)
  ── 선택된 파티션 내에서 sort_a, sort_c 조건으로
     파일의 min/max 통계를 확인하여 불필요한 파일 건너뛰기

3단계: Row-level Filter (행 단위)
  ── 읽은 파일 내에서 모든 WHERE 조건으로 행 단위 필터링
  ── par_b, sort_b 등 나머지 조건은 이 단계에서 작동
```

> **Partition Pruning**은 Iceberg의 Hidden Partitioning 기능으로 자동 작동한다. 사용자는 원본 컬럼(`ts`, `par_a`)으로 WHERE 조건만 작성하면 되고, 내부 파티션 구조를 알 필요가 없다.

---

## 3. ts 컬럼 필터링 방법

ts 컬럼은 `timestamp_ntz`(시간대 정보 없는 타임스탬프) 타입이다. 이 타입의 특성상 **필터링 방법에 주의가 필요**하다.

### 3.1 주의사항

```sql
-- ❌ 결과가 조회되지 않을 수 있음
WHERE ts = timestamp '2026-03-11'
```

ts 컬럼에 timestamp 리터럴을 직접 `=` 비교하면 결과가 나오지 않을 수 있다. 반드시 아래 방법 중 하나를 사용해야 한다.

### 3.2 날짜 필터 — 등가 조건

시간 정보 없이 **날짜만 알 때** 사용한다.

```sql
-- 방법 1: date() 함수 (권장 — 가장 간결)
WHERE date(ts) = DATE '2026-03-11'

-- 방법 2: CAST
WHERE CAST(ts AS DATE) = DATE '2026-03-11'

-- 방법 3: date_trunc
WHERE date_trunc('day', ts) = TIMESTAMP '2026-03-11 00:00:00'
```

세 방식 모두 Trino가 내부적으로 timestamp 범위 조건으로 변환하여 **Partition Pruning이 동일하게 작동**한다 ([참고: Trino 블로그](https://trino.io/blog/2023/04/11/date-predicates.html)). 성능 차이는 없으므로 가장 간결한 `date(ts)`를 권장한다.

> 날짜 조건은 해당일의 24개 시간 파티션을 모두 스캔한다. 이는 정상 동작이며 Partition Pruning이 작동하는 것이다 (전체 날짜를 스캔하는 것이 아니라 해당일만 스캔).

### 3.3 날짜 필터 — 범위 조건

여러 날을 조회할 때 사용한다.

```sql
-- 범위 (2일간)
WHERE date(ts) >= DATE '2026-03-11'
  AND date(ts) <= DATE '2026-03-12'

-- BETWEEN (위와 동일)
WHERE date(ts) BETWEEN DATE '2026-03-11' AND DATE '2026-03-12'

-- IN (비연속 날짜)
WHERE date(ts) IN (DATE '2026-03-11', DATE '2026-03-15')
```

모두 Partition Pruning이 작동한다. 범위에 포함된 날짜의 시간 파티션만 스캔한다.

### 3.4 시간 필터 — 등가 조건

**특정 시간대**를 정확히 조회할 때 사용한다. 1개 시간 파티션만 스캔하므로 **가장 효율적**이다.

```sql
-- 방법 1: date_trunc (권장 — 간결)
WHERE date_trunc('hour', ts) = TIMESTAMP '2026-03-11 10:00:00'

-- 방법 2: 범위 조건 (동일 효과)
WHERE ts >= TIMESTAMP '2026-03-11 10:00:00'
  AND ts <  TIMESTAMP '2026-03-11 11:00:00'
```

### 3.5 시간 필터 — 범위 조건

여러 시간대를 조회할 때 사용한다.

```sql
-- 08시~12시 (5개 시간 파티션 스캔)
WHERE ts >= TIMESTAMP '2026-03-11 08:00:00'
  AND ts <  TIMESTAMP '2026-03-11 13:00:00'

-- BETWEEN (주의: 양쪽 끝 포함)
WHERE ts BETWEEN TIMESTAMP '2026-03-11 08:00:00'
              AND TIMESTAMP '2026-03-11 12:59:59'
```

### 3.6 ts 필터 패턴 요약

| 상황 | 권장 패턴 | Partition Pruning | 스캔 범위 |
|------|----------|-------------------|----------|
| 날짜 1일 | `date(ts) = DATE '...'` | ✅ | 24개 시간 파티션 |
| 날짜 N일 | `date(ts) BETWEEN DATE '...' AND DATE '...'` | ✅ | N×24개 시간 파티션 |
| 비연속 날짜 | `date(ts) IN (DATE '...', ...)` | ✅ | 해당 날짜의 시간 파티션 |
| 시간 1시간 | `date_trunc('hour', ts) = TIMESTAMP '...'` | ✅ | **1개** 시간 파티션 |
| 시간 범위 | `ts >= TIMESTAMP '...' AND ts < TIMESTAMP '...'` | ✅ | 해당 시간 파티션 |
| ts 직접 `=` 비교 | `ts = TIMESTAMP '...'` | ❌ | **결과 미조회 가능** |

---

## 4. WHERE 조건별 성능 효과

### 4.1 필수 컬럼 (Partition Pruning / Data Skipping)

| 컬럼 | 최적화 단계 | 포함 시 효과 | 빠뜨릴 때 영향 |
|------|-----------|------------|--------------|
| ts | Partition Pruning | 해당 시간/날짜의 파티션만 스캔 | **전체 날짜 스캔** — 데이터 전체를 읽음 |
| par_a | Partition Pruning | 해당 값의 파티션만 스캔 (1/4로 축소) | **4배 스캔** — 모든 par_a 파티션 읽음 |
| sort_a | Data Skipping | 파일의 min/max 통계로 불필요한 파일 건너뛰기 | **Data Skipping 무효화** — 파티션 내 모든 파일 읽음 |
| sort_c | Data Skipping | 추가 파일 건너뛰기 | Data Skipping 효과 감소 |

### 4.2 선택 컬럼 (Row-level Filter)

| 컬럼 | 최적화 단계 | 효과 |
|------|-----------|------|
| par_b | Row-level Filter | 파일을 읽은 후 행 단위로 필터링. 파티션/파일 단위 최적화에는 영향 없음 |
| sort_b | Row-level Filter | 동일 |

> par_b, sort_b는 WHERE에 포함해도 파티션/파일을 건너뛰는 효과는 없다. 다만 결과 행을 필터링하는 데 사용된다.

---

## 5. 쿼리 패턴 가이드

### 5.1 기본 쿼리 — 날짜 조건

```sql
SELECT *
FROM TABLE_A
WHERE date(ts) = DATE '2026-03-11'
  AND par_a = 'A'
  AND sort_a = 'value1'
  AND sort_c = 'value3';
```

### 5.2 기본 쿼리 — 시간 조건

시간 정보를 알고 있다면 시간 조건이 **가장 효율적**이다.

```sql
SELECT *
FROM TABLE_A
WHERE date_trunc('hour', ts) = TIMESTAMP '2026-03-11 10:00:00'
  AND par_a = 'A'
  AND sort_a = 'value1'
  AND sort_c = 'value3';
```

### 5.3 IN 절

```sql
-- par_a 복수 값
SELECT *
FROM TABLE_A
WHERE date(ts) = DATE '2026-03-11'
  AND par_a IN ('A', 'B')
  AND sort_a = 'value1'
  AND sort_c = 'value3';
```

Partition Pruning: IN 절의 각 값에 대해 해당 파티션만 읽는다.

### 5.4 날짜 범위

```sql
-- 3일간 조회
SELECT *
FROM TABLE_A
WHERE date(ts) BETWEEN DATE '2026-03-11' AND DATE '2026-03-13'
  AND par_a = 'A'
  AND sort_a = 'value1'
  AND sort_c = 'value3';
```

### 5.5 시간 범위

```sql
-- 08시~12시 조회
SELECT *
FROM TABLE_A
WHERE ts >= TIMESTAMP '2026-03-11 08:00:00'
  AND ts <  TIMESTAMP '2026-03-11 13:00:00'
  AND par_a = 'A'
  AND sort_a = 'value1'
  AND sort_c = 'value3';
```

### 5.6 SELECT 절 권장사항

```sql
-- 권장: 필요한 컬럼만 명시
SELECT ts, par_a, sort_a, val_1
FROM TABLE_A
WHERE ...;

-- 비권장: 전체 컬럼
SELECT *
FROM TABLE_A
WHERE ...;
```

TABLE_A는 Parquet 포맷으로 저장되어 있어 **컬럼 단위로 데이터를 읽는다**. 필요한 컬럼만 SELECT하면 읽는 데이터량이 줄어든다.

---

## 6. 잘못된 쿼리 패턴

### 6.1 ts 직접 등가 비교 — 결과 미조회

```sql
-- ❌ 잘못된 패턴: 결과가 조회되지 않을 수 있음
WHERE ts = TIMESTAMP '2026-03-11'

-- ✅ 올바른 패턴
WHERE date(ts) = DATE '2026-03-11'
```

> ts는 `timestamp_ntz` 타입이다. timestamp 리터럴과 직접 `=` 비교하면 타입 불일치로 결과가 나오지 않을 수 있다.

### 6.2 ts 조건 누락 — 전체 날짜 스캔

```sql
-- ❌ 잘못된 패턴: ts 조건 없음 → 모든 날짜 데이터를 읽음
WHERE par_a = 'A'
  AND sort_a = 'value1'
  AND sort_c = 'value3';

-- ✅ 올바른 패턴
WHERE date(ts) = DATE '2026-03-11'
  AND par_a = 'A'
  AND sort_a = 'value1'
  AND sort_c = 'value3';
```

### 6.3 par_a 조건 누락 — 4배 스캔

```sql
-- ❌ 잘못된 패턴: par_a 조건 없음 → 모든 par_a 파티션을 읽음
WHERE date(ts) = DATE '2026-03-11'
  AND sort_a = 'value1'
  AND sort_c = 'value3';

-- ✅ 올바른 패턴
WHERE date(ts) = DATE '2026-03-11'
  AND par_a = 'A'
  AND sort_a = 'value1'
  AND sort_c = 'value3';
```

### 6.4 sort_a/sort_c 조건 누락 — Data Skipping 무효화

```sql
-- ❌ 잘못된 패턴: sort_a, sort_c 없음 → 파티션 내 모든 파일을 읽음
WHERE date(ts) = DATE '2026-03-11'
  AND par_a = 'A';

-- ✅ 올바른 패턴
WHERE date(ts) = DATE '2026-03-11'
  AND par_a = 'A'
  AND sort_a = 'value1'
  AND sort_c = 'value3';
```

### 6.5 컬럼에 함수 적용 — Partition Pruning 무효화

```sql
-- ❌ 잘못된 패턴: par_a에 함수 적용 → Partition Pruning 작동 안 함
WHERE UPPER(par_a) = 'A'

-- ✅ 올바른 패턴: 원본 값으로 비교
WHERE par_a = 'A'
```

> 파티션 컬럼(`par_a`)에 함수를 적용하면 Iceberg가 파티션 값과 매칭할 수 없어 Partition Pruning이 무효화된다. 원본 값으로 비교해야 한다. 단, `ts` 컬럼의 `date()`, `date_trunc()`는 Trino가 자동으로 범위 조건으로 변환하므로 **예외적으로 Partition Pruning이 작동**한다.

---

## 7. 성능 확인 방법

쿼리 앞에 `EXPLAIN ANALYZE`를 붙이면 실제 실행 후 성능 지표를 확인할 수 있다.

```sql
EXPLAIN ANALYZE
SELECT *
FROM TABLE_A
WHERE date(ts) = DATE '2026-03-11'
  AND par_a = 'A'
  AND sort_a = 'value1'
  AND sort_c = 'value3';
```

**확인할 지표 2가지**

| 지표 | 의미 | 좋은 상태 |
|------|------|----------|
| **Physical input** | 스토리지에서 실제 읽은 데이터 크기 | 적을수록 좋음 — Partition Pruning/Data Skipping이 잘 작동한 것 |
| **Filtered** | 읽은 후 버린 행의 비율 (%) | 낮을수록 좋음 — 불필요한 데이터를 적게 읽은 것 |

`Physical input`이 크고 `Filtered`가 높다면 WHERE 조건에서 필수 컬럼(ts, par_a, sort_a, sort_c)이 빠져있을 가능성이 높다.

> **상세 해석 방법**: [read-performance-test.md](read-performance-test.md)의 "EXPLAIN ANALYZE 결과 해석 가이드" 참조

---

## 8. 용어집

| 용어 | 설명 |
|------|------|
| **Partition Pruning** | 쿼리 조건과 무관한 파티션(데이터 그룹)을 읽지 않고 건너뛰는 최적화. WHERE 절의 파티션 컬럼 조건(`ts`, `par_a`)으로 작동한다 |
| **Data Skipping** | 파일의 min/max 통계를 보고, 조건에 해당하지 않는 파일을 건너뛰는 최적화. Sort Order(`sort_a`, `sort_c`)로 데이터가 정렬되어 있을 때 효과가 높다 |
| **Sort Order** | 데이터 파일을 쓸 때 지정된 컬럼 순서로 정렬하는 설정. 정렬된 파일은 min/max 범위가 좁아져 Data Skipping 효과가 높아진다 |
| **Hidden Partitioning** | Iceberg의 파티셔닝 방식. 사용자는 원본 컬럼(`ts`)으로 쿼리하면 되고, 내부적으로 파티션(`hour(ts)`)을 이용해 Partition Pruning이 자동 수행된다 |
| **timestamp_ntz** | 시간대(timezone) 정보가 없는 타임스탬프 타입. 저장된 값 그대로 비교된다. timestamp 리터럴과 직접 `=` 비교 시 결과가 조회되지 않을 수 있다 |
| **Row Group** | Parquet 파일 내부의 행 묶음 단위. min/max 통계가 Row Group별로 저장되어 Data Skipping에 활용된다 |
| **Compaction** | 여러 개의 작은 파일을 적정 크기의 큰 파일로 병합하는 운영 작업. 정기적으로 수행된다 |
