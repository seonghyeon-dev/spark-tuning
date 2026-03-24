# A안 vs D안 실측 비교 테스트

### 목차

- [1. 테스트 설계](#1-테스트-설계) — 테이블 구성, 공통 조건
- [2. 측정 항목](#2-측정-항목) — 쓰기, Compaction, 파일 분포, 읽기, 메타데이터
- [3. 비교 기준](#3-비교-기준) — 항목별 A안/D안 유리 조건
- [4. 테스트 시 주의사항](#4-테스트-시-주의사항)
- [5. 테스트 결과](#5-테스트-결과) — 읽기 성능 실측

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

쿼리 패턴별로 나눠서 측정한다. 각 쿼리는 3회 이상 실행하여 평균을 사용한다.

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

| 측정 항목 | 확인 방법 | 비교 기준 |
|----------|----------|----------|
| 쿼리 소요시간 | Trino 쿼리 실행 시간 (3회 이상 평균) | 직접적인 성능 지표 |
| Scan 파일 수 | Trino EXPLAIN ANALYZE 또는 쿼리 통계 | 파티션 프루닝 + Data Skipping 효과 |
| Scan 데이터 크기 | Trino 쿼리 통계 (Physical Input) | 실제 I/O 부하 |

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

- **캐시 무효화**: Trino 쿼리 성능 측정 시 캐시 영향 제거. 첫 실행(cold)과 이후 실행(warm)을 구분하여 기록
- **Compaction 전후 둘 다 측정**: A안은 Compaction 전후 차이가 크므로 양쪽 다 기록. D안도 Compaction 전후를 측정하여 "Compaction 선택적"이 실제로 유효한지 확인
- **다양한 par_b 값으로 테스트**: Skew가 큰 상위 par_b 값(데이터 많음)과 하위 par_b 값(데이터 적음) 모두 테스트. A안에서 하위 파티션(소형 파일)의 읽기 성능 확인

---

## 5. 테스트 결과

### 5.1 읽기 성능: 쿼리 2 (시간 + 전체 필터)

**테스트 환경**: Compaction 전, 1일치 데이터 적재 상태

**쿼리 조건**: ts(날짜/시간) + par_a + par_b + sort_a + sort_b + sort_c — 6개 필수 조건 모두 포함

#### Trino Resource Utilization 비교

| 지표 | A안 (date 기준) | D안 (hour 기준) | 차이 |
|------|----------------|----------------|------|
| **Physical Input Rows** | 41.1K | 41.1K | 동일 |
| **Physical Input Data** | 10.2MB | 10.2MB | 동일 |
| **Physical Input Read Time** | 69ms | 67ms | 2ms |
| CPU Time | 339ms | 266ms | A안 73ms 더 많음 |
| Planning CPU Time | 14.63ms | 13.96ms | 거의 동일 |
| Scheduled Time | 477ms | 410ms | A안 67ms 더 많음 |
| Input Rows | 41.1K | 41.1K | 동일 |
| Input Data (논리적) | 72.3MB | 72.4MB | 동일 |
| Internal Network Rows | 59.7K | 59.7K | 동일 |
| Internal Network Data | 5.62MB | 5.62MB | 동일 |
| Peak User Memory | 2.20MB | 3.14MB | D안 0.94MB 더 많음 |
| Output Rows | 29.9K | 29.9K | 동일 |
| Output Data | 3.25MB | 3.25MB | 동일 |

#### Trino Stage Performance 비교

| Operator | 지표 | A안 (date) | D안 (hour) | 차이 |
|----------|------|-----------|-----------|------|
| MergeOperator | Throughput | 88.8K rows/s (10.8MB/s) | 89.0K rows/s (10.8MB/s) | 동일 |
| MergeOperator | Output | 29.9K rows (3.62MB) | 29.9K rows (3.62MB) | 동일 |
| MergeOperator | CPU Time | 16.1ms | 14.6ms | 1.5ms |
| MergeOperator | Wall Time | 336ms | 335ms | 1ms |
| MergeOperator | Blocked | 320ms | 321ms | 1ms |
| FilterAndProject | Throughput | 61.4M rows/s (7.27GB/s) | 85.6M rows/s (10.1GB/s) | D안 39% 높음 |
| FilterAndProject | CPU Time | 0.44ms | 0.35ms | 0.09ms |
| FilterAndProject | Wall Time | 0.49ms | 0.35ms | 0.14ms |
| TaskOutput | Throughput | 14.9M rows/s (1.45GB/s) | 14.9M rows/s (1.45GB/s) | 동일 |
| TaskOutput | CPU Time | 1.98ms | 1.99ms | 동일 |

> MergeOperator의 Wall Time(336ms) 중 Blocked(320ms)가 95%를 차지 — 실제 연산이 아닌 I/O 대기가 대부분이다. FilterAndProject의 throughput 차이(61.4M vs 85.6M)는 0.1ms 미만 구간의 측정 오차 수준.

#### 분석

**결과: 유의미한 차이 없음**

1. **Physical Input이 완전히 동일** — 양쪽 모두 41.1K rows / 10.2MB를 S3에서 읽었다. 파티션 프루닝 + Data Skipping 결과 동일한 양의 파일을 스캔한 것으로, 이 쿼리 패턴에서는 파티션 전략 차이가 I/O에 영향을 주지 않았다.

2. **CPU Time 차이(339ms vs 266ms, 27%)는 오차 범위** — 10.2MB 수준의 소량 데이터에서는 JVM warmup, 캐시 상태 등에 의해 쉽게 뒤집힐 수 있다. 반복 실행으로 평균을 내야 유의미한 판단 가능.

3. **Input Rows(41.1K) → Output Rows(29.9K)** — 약 11.2K rows가 필터에서 탈락. Parquet Row Group 단위 읽기 특성상, 조건에 맞지 않는 행도 함께 읽힌 후 필터링된 것이다.

4. **차이가 나지 않는 이유** — 6개 필수 조건(ts, par_a, par_b, sort_a, sort_b, sort_c)이 모두 걸려있어, 파티션 프루닝 이후 Data Skipping이 대부분의 필터링을 수행한다. 최종 스캔 대상 데이터(10.2MB)가 매우 작아 파티션 전략 차이가 체감되지 않는다.

> **향후 확인**: 데이터 규모가 커지면 차이가 벌어지는지는 쿼리 패턴에 따라 다르다. 시간 단위 조회에서는 D안이 hour 프루닝으로 유리할 수 있으나, 현재 조회 패턴(6개 필수 조건)에서는 Data Skipping이 이미 충분히 효과적이어서 차이가 크지 않을 가능성이 높다.
