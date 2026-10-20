# 빅테이블 array 컬럼 제거 용량 산정

| 항목 | 내용 |
|------|------|
| 대상 | 빅테이블 4개 (수직분할 1~4번) |
| 제거 컬럼 | array&lt;double&gt; value 컬럼. 1·2번 1개, 3·4번 2개 (파티션·Sort Order 컬럼 아님) |
| 기준 | 운영 `$files` 하루치 1일 (daily Compaction 완료일), Trino 482 조회 |
| 보관일수 | 현재 90일, 검토안 180일 |
| 단위 | TiB (1024⁴ B) |

---

## 1. 결론

| 구분 | 4개 합계 | 현재 대비 |
|------|---------|----------|
| 현재 (90일) | 301.6 TiB | |
| 컬럼 제거 후 90일 | 195.1 TiB | −106.5 TiB (−35.3%) |
| 컬럼 제거 후 180일 | 390.3 TiB | +88.6 TiB (+29.4%) |

- 하루 용량 3.351 TiB 중 1.183 TiB(35.3%)가 제거 대상 컬럼이다
- 보관일수를 180일로 늘리면 현재보다 88.6 TiB가 더 필요하다. 지금 용량(301.6 TiB) 안에서는 **139일**까지 보관할 수 있다
- 2번 테이블은 제거 컬럼이 하루 용량의 50.1%라서, 180일로 늘려도 현재 90일과 같은 용량이다(51.3 → 51.2 TiB)
- **180일 연장은 컬럼 제거 90일 뒤에 적용한다.** 제거와 동시에 늘리면 90일째에 496.8 TiB까지 올라간다 (§3)

---

## 2. 테이블별 용량

![테이블별 용량](images/column-removal-by-table.svg)

### 2.1 하루치

| 테이블 | 제거 컬럼 | 파일 수 | 현재 | 제거 컬럼 | 제거 후 | 절감률 |
|--------|----------|--------|------|----------|--------|--------|
| 1번 | 1개 | 2,622 | 0.936 | 0.305 | 0.631 | 32.5% |
| 2번 | 1개 | 1,619 | 0.569 | 0.285 | 0.284 | 50.1% |
| 3번 | 2개 | 1,931 | 0.937 | 0.305 | 0.632 | 32.6% |
| 4번 | 2개 | 1,868 | 0.909 | 0.288 | 0.621 | 31.7% |
| **합계** | | 8,040 | **3.351** | **1.183** | **2.168** | **35.3%** |

1번은 컬럼 1개, 3번은 컬럼 2개를 빼지만 절감량은 같다(각 약 312 GiB). 1번의 제거 컬럼 1개가 3번의 2개만큼 크다.

### 2.2 보관일수 적용

| 테이블 | 현재 (90일) | 제거 후 90일 | 차이 | 제거 후 180일 | 차이 | 현재 용량으로 보관 가능 |
|--------|-----------|------------|------|-------------|------|----------------------|
| 1번 | 84.2 | 56.8 | −27.4 | 113.6 | +29.4 | 133일 |
| 2번 | 51.3 | 25.6 | −25.7 | 51.2 | −0.1 | 180일 |
| 3번 | 84.4 | 56.9 | −27.5 | 113.8 | +29.4 | 133일 |
| 4번 | 81.8 | 55.8 | −25.9 | 111.7 | +29.9 | 131일 |
| **합계** | **301.6** | **195.1** | **−106.5** | **390.3** | **+88.6** | **139일** |

1번으로 계산 과정을 보이면 다음과 같다.

1. 하루치 0.936 TiB 중 제거 컬럼이 0.305 TiB → 제거 후 하루치 0.631 TiB
2. 현재 90일: 0.936 × 90 = 84.2 TiB
3. 제거 후 90일: 0.631 × 90 = 56.8 TiB (−27.4)
4. 제거 후 180일: 0.631 × 180 = 113.6 TiB (현재 대비 +29.4)
5. 현재 용량으로 보관 가능한 일수: 84.2 ÷ 0.631 = 133일

---

## 3. 180일 연장 적용 시점

![전환 기간 전체 용량](images/column-removal-transition.svg)

컬럼 제거(`DROP COLUMN`)는 메타데이터만 바꾼다. 새로 쓰는 파일부터 작아지고, 제거 시점에 이미 있는 90일치(301.6 TiB)는 보관일수가 지나 삭제될 때까지 그대로 남는다.

| 컬럼 제거 후 경과일 | 0일 | 90일 | 139일 | 180일 |
|-------------------|-----|------|-------|-------|
| 제거와 동시에 180일로 연장 | 301.6 | **496.8 (최대)** | 438.8 | 390.3 |
| 제거 90일 후 180일로 연장 | 301.6 | 195.1 (최저) | 301.4 | 390.3 |

**제거와 동시에 연장하면** 기존 90일치가 180일 기준으로 바뀌어 90일 더 남는다.

- 90일째: 기존 데이터 301.6 + 새 데이터 90일 × 2.168 = 195.1 → **496.8 TiB**
- 이후 기존 데이터가 하루씩 삭제되며 180일째 390.3 TiB로 내려온다

**제거 90일 후 연장하면** 기존 데이터가 90일 기준으로 모두 삭제된 뒤 늘어난다.

- 90일째: 새 데이터만 남아 195.1 TiB
- 이후 하루 2.168 TiB씩 늘어 139일째(301.6 ÷ 2.168 = 139.1)에 현재 용량에 닿고, 180일째 390.3 TiB에서 멈춘다
- 최대치가 최종값 390.3 TiB와 같다. 과거 데이터를 다시 쓰는(rewrite) 작업도 필요 없다

> 보관일수는 `ts` 기준 N일이 지난 데이터를 삭제하는 방식으로 가정했다.

---

## 4. 산정 방법과 검증

### 4.1 원리

Iceberg는 데이터 파일을 쓸 때 컬럼별 크기를 메타데이터(`$files`의 `column_sizes`, `readable_metrics`)에 기록한다.

- `column_size` = 데이터 파일 안에서 그 컬럼이 차지하는 **압축 후(zstd) 바이트**. 값 데이터, dictionary, array 구조 정보(repetition/definition level)를 포함한다 (Iceberg 1.10.1 `ParquetMetrics`, Parquet `ColumnChunkMetaData.getTotalSize()`)
- 파일 크기 = 전체 컬럼의 `column_size` 합 + footer + page index
- Parquet은 컬럼별로 따로 압축하므로, 컬럼을 빼도 나머지 컬럼의 크기는 변하지 않는다. 줄어드는 양 = 제거 컬럼의 `column_size` 합

> ⚠️ `write.metadata.metrics.column.val_arr_1 = 'none'`은 실제 값이 들어 있는 하위 컬럼 `val_arr_1.element`에 적용되지 않는다. 그래서 `none`을 설정한 array 컬럼도 `column_size`가 기록된다. 키를 `val_arr_1.element`로 바꾸면 `column_size`가 null이 되어 이 방법으로 산정할 수 없다.

### 4.2 검증

**① 로컬 테스트**: 컬럼을 제거하고 다시 쓴 실제 크기가 예측과 0.02% 차이

- 환경: Claude Code 클라우드 세션 컨테이너. PySpark 3.5.8 + `iceberg-spark-runtime-3.5_2.12-1.10.1`, `local[2]`, HadoopCatalog, 로컬 디스크
- 테이블: 파티션 `hours(ts)`, `par_a`, Sort Order, zstd, array&lt;double&gt; 3개(운영과 같은 `none` 설정 포함), 20만 row
- 절차: 컬럼 1개 `DROP COLUMN` → sort `rewrite_data_files`(rewrite-all) → 크기 비교

| 항목 | 값 |
|------|-----|
| 제거 전 전체 크기 | 91,902,234 B |
| 제거 컬럼 `column_size` 합 | 30,039,786 B |
| 예측 (전체 − 제거 컬럼) | 61,862,448 B |
| 다시 쓴 실제 크기 | 61,850,566 B |
| 차이 | 11,882 B (0.02%, 파일 2개 → 1개로 줄어든 footer 몫) |

`DROP COLUMN` 직후 rewrite 전 크기는 91,902,234 B 그대로였다 (§3의 전제).

**② 운영 gap 확인**: 파일별 `file_size_in_bytes − column_sizes 합`

| 테이블 | gap 최소 | gap 최대 |
|--------|---------|---------|
| 1번 | 5,817 B | 87,953 B |
| 2번 | 5,467 B | 101,258 B |
| 3번 | 6,288 B | 91,557 B |
| 4번 | 6,408 B | 92,901 B |

최대 101,258 B는 512 MB 파일 기준 0.02%로, footer와 page index 크기다. array 컬럼 크기가 빠진 파일이 있었다면 gap이 MB 단위로 나온다.

**③ 운영 컬럼 수 확인**: 파일별 `cardinality(column_sizes)`가 테이블 컬럼 수와 같음

### 4.3 운영 조회 결과 (원본)

| 테이블 | files | total_bytes | drop_bytes |
|--------|-------|-------------|------------|
| 1번 | 2,622 | 1,028,949,472,691 | 334,909,162,505 |
| 2번 | 1,619 | 626,161,707,715 | 313,709,343,663 |
| 3번 | 1,931 | 1,030,754,375,464 | 335,558,471,727 |
| 4번 | 1,868 | 999,083,778,664 | 316,812,871,027 |

### 4.4 쿼리 (Trino 482)

`partition.ts_hour` 타입이 미확인이라 날짜를 파일 경로로 거른다. 날짜는 daily Compaction이 끝난 날로 한다.

**산정 (1·2번, 제거 컬럼 1개)**

```sql
SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS total_bytes,
       sum(CAST(json_extract_scalar(readable_metrics, '$["<컬럼1>.element"].column_size') AS bigint)) AS drop_bytes
FROM iceberg.<스키마>."<테이블>$files"
WHERE file_path LIKE '%/ts_hour=<yyyy-MM-dd>-%'
```

**산정 (3·4번, 제거 컬럼 2개)**

```sql
SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS total_bytes,
       sum(CAST(json_extract_scalar(readable_metrics, '$["<컬럼1>.element"].column_size') AS bigint))
     + sum(CAST(json_extract_scalar(readable_metrics, '$["<컬럼2>.element"].column_size') AS bigint)) AS drop_bytes
FROM iceberg.<스키마>."<테이블>$files"
WHERE file_path LIKE '%/ts_hour=<yyyy-MM-dd>-%'
```

**검증 (gap, 컬럼 수)**

```sql
SELECT min(file_size_in_bytes - reduce(map_values(column_sizes), BIGINT '0', (s, x) -> s + x, s -> s)) AS gap_min,
       max(file_size_in_bytes - reduce(map_values(column_sizes), BIGINT '0', (s, x) -> s + x, s -> s)) AS gap_max,
       min(cardinality(column_sizes)) AS min_cols,
       max(cardinality(column_sizes)) AS max_cols
FROM iceberg.<스키마>."<테이블>$files"
WHERE file_path LIKE '%/ts_hour=<yyyy-MM-dd>-%'
```

---

## 5. 산정에 포함하지 않은 것

| 항목 | 내용 |
|------|------|
| snapshot 보존분 | Compaction으로 대체된 파일이 snapshot 보존 3일 동안 남는 몫. `$files`는 현재 snapshot 파일만 보여준다 |
| 스토리지 복제 배수 | MinIO erasure coding 등 물리 저장 배수. 이 문서는 파일 크기(논리 용량) 기준 |
| 일별 변동 | 하루치 1일 기준이다. 데이터 증가는 반영하지 않았다 |

## 6. 적용 전 확인 사항

- **append 앱**: 입력 avro에는 제거한 컬럼이 그대로 있다. 앱이 테이블에 없는 컬럼을 넘기면 쓰기가 실패할 수 있다
- **조회 쿼리**: 제거한 컬럼을 SELECT하는 Trino 쿼리는 실패한다
- **튜닝값 재검증**: 파일이 작아지면 hourly Compaction의 ratio 0.13·`C=0.32`와 append executor 수 산정의 입력이 바뀐다 (`tuning/compaction-tuning-guide.md`, `tuning/append-tuning-test.md`)
