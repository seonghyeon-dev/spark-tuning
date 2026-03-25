# A안 vs D안 실측 비교 테스트

### 목차

- [1. 테스트 설계](#1-테스트-설계) — 테이블 구성, 공통 조건
- [2. 측정 항목](#2-측정-항목) — 쓰기, Compaction, 파일 분포, 읽기, 메타데이터
- [3. 비교 기준](#3-비교-기준) — 항목별 A안/D안 유리 조건
- [4. 테스트 시 주의사항](#4-테스트-시-주의사항)
- [5. 읽기 성능 테스트 결과](#5-읽기-성능-테스트-결과) — EXPLAIN ANALYZE, Web UI 캡처, 분석

---

## 1. 테스트 설계

A안과 D안의 실측 비교를 통해 최종 파티션 전략을 결정한다.

**테스트 테이블**

| 항목 | A안 테이블 | D안 테이블 |
|------|----------|----------|
| 파티션 | `day(ts)`, `par_a`, `par_b` | `hour(ts)`, `par_a` |
| Sort Order | `sort_a`, `sort_b`, `sort_c` | `par_b`, `sort_a`, `sort_b`, `sort_c` |
| distribution-mode | `range` | `range` |

**공통 조건**

- 동일한 1일치 데이터 (~851GB, 144배치)
- 동일 Spark 클러스터 설정 (executor 24개, 4core, 8GB)
- Compaction 전후 **모두** 측정

> **참고**: 파티션, Sort Order 등의 테이블 설정은 테스트 결과에 따라 수정할 수 있다.

**실행 횟수**: 쿼리당 **5회** 실행, 전체 평균으로 비교. Trino는 데이터 캐싱이 없으므로(S3에서 매번 새로 읽음) cold/warm 구분 불필요.

---

## 2. 측정 항목

**1) 쓰기 성능**

| 측정 항목 | 확인 방법 | 목적 |
|----------|----------|------|
| 배치당 쓰기 소요시간 | Spark UI → Job Duration | D안은 파티션 수 감소로 shuffle 패턴 변경. 쓰기 성능 차이 확인 |
| Shuffle Read/Write 크기 | Spark UI → Stages | Sort Order 컬럼 수 변경(3→4)에 따른 shuffle 부하 차이 |
| 배치당 생성 파일 수 | `files` 메타데이터 테이블 | Compaction 전 파일 수 — 운영 부담의 직접 지표 |

**2) Compaction 성능**

| 측정 항목 | 확인 방법 | 목적 |
|----------|----------|------|
| Compaction 소요시간 | `rewrite_data_files` 실행 시간 | A안은 필수, D안은 선택적. 비용 차이 확인 |
| Compaction 전후 파일 수 변화 | `files` 메타데이터 테이블 | D안이 Compaction 없이도 안전 구간인지 확인 |

**3) 파일/파티션 분포**

```sql
SELECT partition,
       COUNT(*) AS file_count,
       SUM(file_size_in_bytes) / 1024 / 1024 AS size_mb,
       AVG(file_size_in_bytes) / 1024 / 1024 AS avg_file_mb,
       MIN(file_size_in_bytes) / 1024 / 1024 AS min_file_mb,
       MAX(file_size_in_bytes) / 1024 / 1024 AS max_file_mb
FROM catalog.db.TABLE_X.files
GROUP BY partition
ORDER BY size_mb DESC;
```

| 측정 항목 | 비교 기준 |
|----------|----------|
| 파티션별 데이터 크기 분포 | Skew 정도 — 최대/최소 파티션 크기 비율 |
| 파일 크기 분포 | 목표 크기(512MB) 대비 편차 |
| 소형 파일 비율 | 128MB 미만 파일의 비율 |

**4) 읽기 성능 (핵심)**

쿼리 패턴별로 나눠서 측정한다. 각 쿼리는 5회 실행하여 평균을 사용한다.

```sql
-- 쿼리 1: 날짜 + 전체 필터 (가장 빈번한 패턴)
SELECT * FROM TABLE_X
WHERE date(ts) = timestamp '2026-03-11'
  AND par_a = 'A' AND par_b = 'value0'
  AND sort_a = 'value1' AND sort_b = 'value2' AND sort_c = 'value3';

-- 쿼리 2: 시간 + 전체 필터 (D안 hour 파티션 프루닝 효과 확인)
SELECT * FROM TABLE_X
WHERE ts >= timestamp '2026-03-11 10:00:00' AND ts < timestamp '2026-03-11 11:00:00'
  AND par_a = 'A' AND par_b = 'value0'
  AND sort_a = 'value1' AND sort_b = 'value2' AND sort_c = 'value3';

-- 쿼리 3: 날짜 + IN 조건 (다중 값)
SELECT * FROM TABLE_X
WHERE date(ts) = timestamp '2026-03-11'
  AND par_a IN ('A', 'B') AND par_b IN ('value0', 'value1')
  AND sort_a = 'value1' AND sort_b = 'value2' AND sort_c = 'value3';
```

**5) 메타데이터 오버헤드**

| 측정 항목 | 확인 방법 | 목적 |
|----------|----------|------|
| 매니페스트 파일 수 | `SELECT COUNT(*) FROM table.manifests` | D안의 시간 파티션이 매니페스트 수에 미치는 영향 |

---

## 3. 비교 기준

| 구분 | 비교 항목 | A안 유리 조건 | D안 유리 조건 |
|------|----------|-------------|-------------|
| 쓰기 | 배치 쓰기 시간 | — | 파티션 수 감소로 shuffle 경량화 |
| 쓰기 | Compaction 시간 | — | Compaction 선택적/불필요 |
| 읽기 | 쿼리 1 (날짜+전체) | par_b 파티션 프루닝 1/248 | par_b Data Skipping (Sort Order 1순위) |
| 읽기 | 쿼리 2 (시간+전체) | — | hour(ts) 파티션 프루닝 추가 효과 |
| 파일 | Skew | — | 균등 분포, 소형 파일 없음 |
| 파일 | Compaction 전 파일 구조 | — | 처음부터 적정 크기 파일 |
| 운영 | 복잡도 | 검증 완료 (현행) | Compaction 부담 경감 |

---

## 4. 테스트 시 주의사항

- **반복 실행**: Trino는 데이터 캐싱이 없으므로(S3에서 매번 새로 읽음) 5회 실행 전체 평균을 사용
- **Compaction 전후 둘 다 측정**: A안은 Compaction 전후 차이가 크므로 양쪽 다 기록. D안도 Compaction 전후를 측정하여 "Compaction 선택적"이 실제로 유효한지 확인
- **다양한 par_b 값으로 테스트**: Skew가 큰 상위 par_b 값(데이터 많음)과 하위 par_b 값(데이터 적음) 모두 테스트. A안에서 하위 파티션(소형 파일)의 읽기 성능 확인

**Web UI 캡처 가이드**

각 쿼리 실행 후 Trino Web UI에서 아래 3개 탭을 A안/D안 각각 캡처한다.

| 캡처 대상 | 탭 위치 | 캡처 내용 | 비교 포인트 |
|----------|---------|----------|-----------|
| Resource Utilization | Overview → Resource Utilization Summary | Physical Input Rows/Data, CPU Time, Scheduled Time 등 전체 요약 | 핵심 수치 비교 — 실제 I/O 양, CPU 소비 |
| Stage Performance | Stage Performance 탭 | Operator별 throughput, CPU Time, Wall Time, Blocked | 병목 구간 식별 — 어떤 Operator에서 시간 소비 |
| Timeline | Timeline 탭 | Parallelism, Input rows/s, Physical Input Bytes 그래프 | 시간축 기반 처리 패턴 — I/O 집중 구간, 병렬 처리 양상 |

---

## 5. 읽기 성능 테스트 결과

### 5.1 쿼리 1: 날짜 + 전체 필터

**쿼리**

```sql
SELECT * FROM TABLE_X
WHERE date(ts) = timestamp '2026-03-11'
  AND par_a = 'A' AND par_b = 'value0'
  AND sort_a = 'value1' AND sort_b = 'value2' AND sort_c = 'value3';
```

#### EXPLAIN ANALYZE

**A안**

```
(결과 붙여넣기)
```

**D안**

```
(결과 붙여넣기)
```

#### Web UI 캡처

**Resource Utilization**

| A안 | D안 |
|-----|-----|
| (A안 캡처 이미지) | (D안 캡처 이미지) |

**Stage Performance**

| A안 | D안 |
|-----|-----|
| (A안 캡처 이미지) | (D안 캡처 이미지) |

**Timeline**

| A안 | D안 |
|-----|-----|
| (A안 캡처 이미지) | (D안 캡처 이미지) |

#### 실행 결과 요약

| 지표 | A안 | D안 | 차이 |
|------|-----|-----|------|
| Physical Input Rows | | | |
| Physical Input Data | | | |
| Physical Input Read Time | | | |
| CPU Time | | | |
| Scheduled Time | | | |
| Input Rows | | | |
| Output Rows | | | |

| 회차 | A안 소요시간 | D안 소요시간 |
|------|------------|------------|
| 1회 | | |
| 2회 | | |
| 3회 | | |
| 4회 | | |
| 5회 | | |
| **평균** | | |

#### 분석

(결과 기반 분석)

---

### 5.2 쿼리 2: 시간 + 전체 필터

**쿼리**

```sql
SELECT * FROM TABLE_X
WHERE ts >= timestamp '2026-03-11 10:00:00' AND ts < timestamp '2026-03-11 11:00:00'
  AND par_a = 'A' AND par_b = 'value0'
  AND sort_a = 'value1' AND sort_b = 'value2' AND sort_c = 'value3';
```

#### EXPLAIN ANALYZE

**A안**

```
(결과 붙여넣기)
```

**D안**

```
(결과 붙여넣기)
```

#### Web UI 캡처

**Resource Utilization**

| A안 | D안 |
|-----|-----|
| (A안 캡처 이미지) | (D안 캡처 이미지) |

**Stage Performance**

| A안 | D안 |
|-----|-----|
| (A안 캡처 이미지) | (D안 캡처 이미지) |

**Timeline**

| A안 | D안 |
|-----|-----|
| (A안 캡처 이미지) | (D안 캡처 이미지) |

#### 실행 결과 요약

| 지표 | A안 | D안 | 차이 |
|------|-----|-----|------|
| Physical Input Rows | | | |
| Physical Input Data | | | |
| Physical Input Read Time | | | |
| CPU Time | | | |
| Scheduled Time | | | |
| Input Rows | | | |
| Output Rows | | | |

| 회차 | A안 소요시간 | D안 소요시간 |
|------|------------|------------|
| 1회 | | |
| 2회 | | |
| 3회 | | |
| 4회 | | |
| 5회 | | |
| **평균** | | |

#### 분석

(결과 기반 분석)

---

### 5.3 쿼리 3: 날짜 + IN 조건

**쿼리**

```sql
SELECT * FROM TABLE_X
WHERE date(ts) = timestamp '2026-03-11'
  AND par_a IN ('A', 'B') AND par_b IN ('value0', 'value1')
  AND sort_a = 'value1' AND sort_b = 'value2' AND sort_c = 'value3';
```

#### EXPLAIN ANALYZE

**A안**

```
(결과 붙여넣기)
```

**D안**

```
(결과 붙여넣기)
```

#### Web UI 캡처

**Resource Utilization**

| A안 | D안 |
|-----|-----|
| (A안 캡처 이미지) | (D안 캡처 이미지) |

**Stage Performance**

| A안 | D안 |
|-----|-----|
| (A안 캡처 이미지) | (D안 캡처 이미지) |

**Timeline**

| A안 | D안 |
|-----|-----|
| (A안 캡처 이미지) | (D안 캡처 이미지) |

#### 실행 결과 요약

| 지표 | A안 | D안 | 차이 |
|------|-----|-----|------|
| Physical Input Rows | | | |
| Physical Input Data | | | |
| Physical Input Read Time | | | |
| CPU Time | | | |
| Scheduled Time | | | |
| Input Rows | | | |
| Output Rows | | | |

| 회차 | A안 소요시간 | D안 소요시간 |
|------|------------|------------|
| 1회 | | |
| 2회 | | |
| 3회 | | |
| 4회 | | |
| 5회 | | |
| **평균** | | |

#### 분석

(결과 기반 분석)

---

## 6. 종합 분석

### 결과 요약

| 쿼리 | A안 평균 소요시간 | D안 평균 소요시간 | Physical Input Data (A안/D안) | 판정 |
|------|----------------|----------------|------------------------------|------|
| 쿼리 1 (날짜+전체) | | | | |
| 쿼리 2 (시간+전체) | | | | |
| 쿼리 3 (IN 조건) | | | | |

### 결론

(테스트 결과 기반 최종 판단)
