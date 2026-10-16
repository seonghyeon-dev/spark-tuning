# append Job 튜닝 테스트 절차 (빅테이블)

| 항목 | 내용 |
|------|------|
| 대상 | 빅테이블 4개 append Job (1번 테이블 먼저) |
| 방식 | SparkApplication CRD 직접 apply, 테스트용 복제 테이블, 운영 5분치 고정 입력 |
| 환경 | Spark 3.5.8, Iceberg 1.10.1 (운영) |
| 결과 반영 | `tuning/spark-tuning-guide.md` §2.2·§4.1, `pipeline/examples/append_executor_sizing_example.py` |
| 작성일 | 2026-09-30 (1번 테이블 결과·확정값 반영 2026-10-14) |

---

## 1. 결론: executor 수가 출력 파일 수를 정한다

현재 설정에서는 executor 수를 바꾸면 append 출력 파일의 수와 크기가 같이 바뀐다. 리소스만 튜닝해도 hourly Compaction의 입력이 달라진다. 그래서 리소스보다 먼저 `parallelismFirst`를 비교한다.

**용어.** advisory partition size(이하 advisory)는 AQE가 shuffle partition을 합칠 때 목표로 삼는 partition 크기다. Spark 설정 `spark.sql.adaptive.advisoryPartitionSizeInBytes`의 기본값은 64MB다. Iceberg append에서는 Iceberg가 계산한 값이 대신 쓰인다.

**Iceberg advisory = 384MB.**
- 계산: Iceberg 목표 파일 128MB × 3.0 = 384MB
- 3.0은 같은 데이터가 shuffle 단계에서 parquet 파일보다 몇 배 큰지에 대한 Iceberg의 추정치다.
- shuffle 압축(`spark.io.compression.codec`)이 기본값 lz4이고 parquet이 zstd일 때 3.0이 된다(`SparkCompressionUtil`).

**`parallelismFirst=true`(기본값)일 때 목표 크기** = `min(advisory, 전체 shuffle ÷ 총 executor core)`

**1번 테이블 batch 1회로 계산**

| 단계 | 값 |
|------|----|
| parquet 출력 | 3,157MB |
| shuffle 크기 | 3,157MB × 1.41 = 약 4.45GB (1.41 = 2026-03 벤치마크의 shuffle 9.2GiB ÷ 출력 6.5GiB, A안 스키마 값. **실측(§7.1): shuffle 4.59GiB ÷ 출력 3.2GiB = 1.43**) |
| 총 core | 운영 14대 × 4 = 56 |
| 목표 크기 | 4.45GB ÷ 56 = 약 79MB. 384MB보다 작으므로 79MB가 쓰인다 |
| 결과 | 쓰기 task 약 56개 → 파일 약 58개, 파일당 79MB ÷ 1.41 = 약 56MB |
| 실측 (2026-08-11) | batch당 58.6개, 파일당 53.9MB |

- 파일 수(58.6)가 총 core 수(56)와 거의 같다. 단 56은 최근 10회 최대 executor 수 기준이고, 58.6은 2026-08-11 batch 12개의 평균이다. 당시 batch별 executor 수는 기록이 없으므로 §5.3 로그로 확인한다. 나머지 2~3개는 파티션(`hour(ts)`, `par_a`) 경계에 걸친 task가 파일을 하나 더 쓴 것이다.
- `false`로 바꾸면 목표가 advisory 384MB로 고정된다. 4.45GB ÷ 384MB로 쓰기 task는 약 12개, 경계를 더해 batch당 파일은 약 12~16개가 된다. 이때 파일 크기는 advisory가 정하고 executor 수와는 무관해진다.
- `spark.sql.shuffle.partitions`(200)는 range 분배의 초기 구간 수일 뿐이다. AQE가 위 목표 크기로 다시 합치거나 쪼개므로 파일 수에 영향이 없다. 기본값을 유지한다.
- `write.distribution-mode=range`는 고정이다(비교 대상 아님). range는 경계를 정하는 샘플링 job이 avro 전체를 한 번 더 읽는다. stage별 시간을 볼 때 이 stage를 따로 적는다.

출처: Spark v3.5.8 `ShufflePartitionsUtil.scala`·`CoalesceShufflePartitions.scala`, Iceberg 1.10.1 `SparkWriteConf.java`·`SparkCompressionUtil.java`

---

## 2. 현재 운영 설정 (빅테이블 4개 공통, 2026-09-30)

| 구분 | 값 |
|------|----|
| driver | cpu 1, memory 2g, memoryOverhead 기본값 |
| executor | cpu 4, memory 8g, memoryOverhead 4g |
| executor 수 | Airflow `get_jobs`가 batch마다 `ceil(avro 총크기 ÷ 128MB × 1.5 ÷ 4)`로 계산한다. Spark dynamic allocation은 쓰지 않는다 |
| 최근 10회 최대 executor 수 | 1번 14 · 2번 8 · 3번 12 · 4번 12 |
| sparkConf | 카탈로그 설정(`type=hive`, `io-impl=S3FileIO`, `s3.delete.batch-size=1000`, `s3.delete.num-threads=8`, `client.region`) 외에는 기본값 |
| TBLPROPERTIES | `write.parquet.compression-codec=zstd`, array 컬럼 `write.metadata.metrics.column.*=none`, `write.distribution-mode=range`, target-file-size 기본값 512MB |
| 파티션 / Sort Order | `hour(ts)`, `par_a` / `sort_a`, `sort_b` |
| 앱 인자 | `tableName`, `inputFileName`(avro 경로 목록 텍스트 파일의 S3 경로), `batchId` |
| 앱 쓰기 옵션 | `snapshot-property.batch_id`만 지정. 나머지 옵션이 없으므로 비교 후보는 sparkConf만으로 바꿀 수 있다 |

- Oracle Job History 상태는 Airflow(`get_jobs`, `_update_jobs`)가 갱신한다. CRD로 직접 실행하면 Oracle에 영향이 없다.

---

## 3. 테스트 준비

### 3.1 테스트 테이블

1. 운영 DDL을 조회한다.

```sql
SHOW CREATE TABLE iceberg.<db>.<테이블1>;
```

2. 결과를 복사해 아래 세 가지를 고친 뒤 CREATE한다.
   - 테이블 이름을 `<테이블1>_tune`으로 바꾼다.
   - **`LOCATION` 줄을 삭제한다.** 남겨 두면 테스트 테이블이 운영 테이블과 같은 경로에 파일을 쓴다.
   - `'sort-order'` 줄은 CREATE가 무시하므로 지워도 된다(3번에서 따로 지정한다).
3. Sort Order를 지정한다.

```sql
ALTER TABLE iceberg.<db>.<테이블1>_tune WRITE ORDERED BY sort_a, sort_b;
```

4. 운영과 대조한다. 파티션, Sort Order, TBLPROPERTIES가 같고 `Location`은 달라야 한다.

```sql
DESCRIBE TABLE EXTENDED iceberg.<db>.<테이블1>_tune;
SHOW TBLPROPERTIES iceberg.<db>.<테이블1>_tune;
```

### 3.2 입력 규모 산정 (JOB_HISTORY, Oracle)

고정 입력을 고르기 전에 5분 구간별 JOB_HISTORY row 수, avro 파일 수, 크기를 잰다. `get_jobs`의 조회 상한 200 row(변동 가능)는 1회 조회의 상한일 뿐이고 5분치 규모가 아니다.

- 가정: 시간 컬럼 `dt`(문자열 `YYYYMMDDHH24MISSFF3`, 예 `20260930132700886`), 파일 목록 JSON 컬럼 `param`(`{"table": ..., "files": [{"name": ..., "size": "2262129"}, ...]}`, `size`는 바이트)
- 맨 위 `p`의 값 3개만 바꿔서 실행한다. JSON 컬럼 이름이 `param`이 아니면 `j.param` 한 곳을 바꾼다.
- DB 2개에서 각각 실행한다. 같은 `bucket_5m`끼리 두 DB 값을 더한 것이 그 5분의 전체 입력이다.
- `bucket_5m`이 `202609291325`이면 13:25:00.000~13:29:59.999 구간이다.
- `JSON_TABLE`은 Oracle 12.1.0.2 이상에서 동작한다.

**① 요약: 5분 구간별 min / avg / max**

```sql
WITH p AS (                                   -- 여기만 수정
  SELECT 'TABLE_1'            AS tbl,         -- 대상 테이블명 (table_name 값)
         '20260929000000000'  AS dt_from,     -- 조회 시작 (포함)
         '20260930000000000'  AS dt_to        -- 조회 끝 (미포함)
    FROM dual
),
per_row AS (                                  -- JOB_HISTORY row 1개당 파일 수·크기
  SELECT j.ROWID                    AS rid,
         MIN(j.dt)                  AS dt,
         COUNT(*)                   AS n_files,
         SUM(TO_NUMBER(f.fsize))    AS size_bytes
    FROM p, JOB_HISTORY j,
         JSON_TABLE(j.param, '$.files[*]'
                    COLUMNS (fsize VARCHAR2(20) PATH '$.size')) f
   WHERE j.table_name = p.tbl
     AND j.dt >= p.dt_from
     AND j.dt <  p.dt_to
   GROUP BY j.ROWID
),
per_5m AS (                                   -- dt의 분을 5로 내림해 5분 구간으로 묶음
  SELECT SUBSTR(dt, 1, 10) || LPAD(FLOOR(TO_NUMBER(SUBSTR(dt, 11, 2)) / 5) * 5, 2, '0') AS bucket_5m,
         COUNT(*)         AS n_rows,
         SUM(n_files)     AS n_files,
         SUM(size_bytes)  AS size_bytes
    FROM per_row
   GROUP BY SUBSTR(dt, 1, 10) || LPAD(FLOOR(TO_NUMBER(SUBSTR(dt, 11, 2)) / 5) * 5, 2, '0')
)
SELECT COUNT(*)                                   AS buckets,
       MIN(n_rows)   AS rows_min,  ROUND(AVG(n_rows))   AS rows_avg,  MAX(n_rows)   AS rows_max,
       MIN(n_files)  AS files_min, ROUND(AVG(n_files))  AS files_avg, MAX(n_files)  AS files_max,
       ROUND(MIN(size_bytes) / POWER(1024, 3), 2) AS gb_min,
       ROUND(AVG(size_bytes) / POWER(1024, 3), 2) AS gb_avg,
       ROUND(MAX(size_bytes) / POWER(1024, 3), 2) AS gb_max
  FROM per_5m;
```

**② 구간별 목록 (고정 입력 batch를 고를 때)**

①의 마지막 `SELECT ... FROM per_5m;`만 아래로 바꾼다.

```sql
SELECT bucket_5m, n_rows, n_files,
       ROUND(size_bytes / POWER(1024, 3), 2) AS size_gb
  FROM per_5m
 ORDER BY bucket_5m;
```

- 고정 입력은 ①의 avg에 가까운 구간을 고른다. min·max 구간은 이후 여러 batch 크기 테스트에 쓴다.

**실측 (2026-09-30, 하루치 5분 구간, min / avg / max)**

| 테이블 | DB | 구간 수 | row | avro 파일 | 크기 (GiB) |
|------|----|------|-----|-----------|-----------|
| 1번 | DB1 | 288 | 99 / 118 / 138 | 2,270 / 2,655 / 3,135 | 3.33 / 3.98 / 4.65 |
| | DB2 | 284 | 1 / 4 / 11 | 1 / 36 / 113 | 0 / 0.06 / 0.21 |
| 2번 | DB1 | 288 | 93 / 114 / 133 | 2,121 / 2,543 / 3,012 | 2.09 / 2.52 / 2.94 |
| | DB2 | 284 | 1 / 4 / 11 | 1 / 36 / 113 | 0 / 0.04 / 0.13 |
| 3번 | DB1 | 288 | 93 / 114 / 133 | 2,121 / 2,543 / 3,012 | 3.22 / 3.90 / 4.54 |
| | DB2 | 284 | 1 / 4 / 11 | 1 / 36 / 113 | 0 / 0.06 / 0.21 |
| 4번 | DB1 | 288 | 93 / 114 / 133 | 2,121 / 2,543 / 3,012 | 3.17 / 3.82 / 4.46 |
| | DB2 | 284 | 1 / 4 / 11 | 1 / 36 / 113 | 0 / 0.06 / 0.20 |

- 구간 수: 조회 기간 안에서 데이터가 1건 이상 있는 5분 구간 개수. 하루는 288개
- 1번은 2·3·4번 데이터에 다른 종류 데이터가 더해진 테이블이라 row·파일 수가 더 많다. 2·3·4번은 row·파일 수가 같고 컬럼 폭만 다르다
- DB2 크기 min 0은 0.005GiB 미만이 반올림된 값

**테스트 입력 기준값**

| 테이블 | 두 DB 평균 합: row / avro / GiB | 현재 공식 executor 수 (평균 구간) | 현재 공식 executor 수 (DB1 max 구간) |
|------|------|------|------|
| 1번 | 122 / 2,691 / 4.04 | 13 | 14 |
| 2번 | 118 / 2,579 / 2.56 | 8 | 9 |
| 3번 | 118 / 2,579 / 3.96 | 12 | 14 |
| 4번 | 118 / 2,579 / 3.88 | 12 | 14 |

- 현재 공식: `ceil(avro 총크기 ÷ 128MB × 1.5 ÷ 4)`. 1번 평균 예: 4.04GiB = 4,137MiB → 4,137 ÷ 128 × 1.5 ÷ 4 = 12.1 → 13
- 두 DB 평균 합은 DB별 평균을 더한 근삿값이다. 정확한 구간별 합은 ②에서 같은 `bucket_5m`끼리 더한다
- 5분치 row는 DB1 최대 138로, `get_jobs` 조회 상한 200(DB당 1회 조회)에 걸리지 않는다
- avro 1개 평균 약 1.5MiB (1번: 3.98GiB ÷ 2,655), JOB_HISTORY row당 avro 약 22개

### 3.3 고정 입력

- 운영 1시간치(5분 batch 12개)의 `inputFileName` 목록 파일을 테스트용 S3 경로에 복사한다. 목록 형식은 바꾸지 않는다.
- 2단계(후보 비교)는 batch 1개(`t1_batch01`)만 반복해서 쓴다. 12개 전체는 Compaction 합계 검증(§8)에서 쓴다.
- 복사 전에 목록 속 원천 avro가 테스트 기간 동안 지워지지 않는지 확인한다.
- 그 batch의 운영 executor 수(Airflow XCom `num_executors`)를 기록해 둔다. 이 값이 L0의 `instances`다.

### 3.4 SparkApplication YAML

운영 YAML을 복사해서 아래 표시한 곳만 바꾼다. image, mainClass, jar, 볼륨, DataFlint 등 나머지는 운영 그대로 둔다.

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: append-tune-t1-l0-r1                  # 회차마다 바꾼다
  namespace: <운영 namespace>
spec:
  # ... 운영 YAML 그대로 ...
  arguments:
    - "<운영과 같은 형식>.<테이블1>_tune"                        # tableName
    - "s3a://<테스트 경로>/append-tune/input/t1_batch01.txt"   # inputFileName
    - "tune-t1-l0-r1"                                         # batchId (snapshot summary의 batch_id)
  sparkConf:
    # ... 운영 sparkConf 그대로 ...
    # L1, L2만 추가
    "spark.sql.adaptive.coalescePartitions.parallelismFirst": "false"
    # L2만 추가 (바이트 단위)
    "spark.sql.iceberg.advisory-partition-size": "201326592"
  driver:
    cores: 1
    memory: "2g"
  executor:
    cores: 4
    instances: 14                             # §3.3에서 기록한 운영 값
    memory: "8g"
    memoryOverhead: "4g"
```

- 회차마다 `metadata.name`과 `batchId`를 바꾼다(`t<테이블>-l<후보>-r<회차>`). 이름을 재사용하면 driver 로그와 DataFlint 기록이 이전 회차와 섞인다.
- 매시 45분~정각은 hourly Compaction과 겹치므로 피한다.

---

## 4. 비교 후보 (리소스 고정)

| 후보 | `parallelismFirst` | advisory | 1번 예상 (batch당) |
|------|------|------|------|
| **L0** (현재) | 미지정 (= `true`) | 미지정 (= 384MB) | 약 58개 × 54MB, 쓰기 task 약 56개 |
| **L1** | `false` | 미지정 (= 384MB) | 약 12~16개 × 약 270MB, 쓰기 task 약 12개 |
| **L2** | `false` | `201326592` (192MB) | 약 24~28개 × 약 136MB, 쓰기 task 약 24개 |

- 가설 계산(배수 1.41 기준, 실측 1.43): L1 파일 크기 384MB ÷ 1.41 = 약 272MB. L2는 4.45GB ÷ 192MB = 약 24 task, 파일당 192MB ÷ 1.41 = 약 136MB
- L1이 필수 조건을 통과하고 dcu/GB가 L0보다 낮으면 L2는 생략한다. L1의 쓰기 stage가 길어 필수 조건을 못 맞추면 L2를 돌린다.
- 실행 순서: L0 → L1 → L0 → L1 ... 순으로 번갈아 5회씩 돌린다. 시간대별 클러스터 부하가 한 후보에만 몰리지 않게 하기 위해서다.
- L0 5회의 편차 (최댓값 − 최솟값) ÷ 평균을 이번 테스트의 노이즈 기준선으로 쓴다.

**결과 (2026-10-06, 1번 테이블, instances 12)**: L0 유지. L2는 돌리지 않았다. 상세는 §7.1

| | L0 | L1 |
|---|---|---|
| duration 중앙값 (10회) | 2.6분 (1.6~3.5) | 2.85분 (2.2~4.2) |
| dcu 중앙값 (5회) | 0.129 | 0.157 |
| 파일 (batch당) | 59개 × 55.5MB | 17개 × 192.7MB |
| advisory / actual target size | 402653184 / 107578488 | 402653184 / 402653184 |

- duration 중앙값 +10%, dcu 중앙값 +22%지만, L0 안의 흔들림(duration 1.6~3.5분, dcu 0.080~0.179)이 그보다 크다. job 비용은 같은 수준으로 보고, 확실한 차이는 파일 수뿐이다.
- 쓰기 속도 우선(사용자 결정 2026-10-06)이라 L0를 고른다. L1의 이점(Compaction 입력 59 → 17개)은 hourly Compaction이 지금 성능으로 충분해서 쓰지 않는다.
- 구조상 L0가 같거나 빠르다. 쓰기 task가 L0는 48개(= 총 core), L1은 13개라 L1은 쓰기 stage에서 35 core가 논다. 측정에서 차이가 묻힌 건 쓰기 stage 비중이 작아서다.

---

## 5. 측정 방법

### 5.1 DataFlint

- duration, dcu, idle cores, spill, task error rate, executor 유실
- stage별 시간. 샘플링 stage는 input이 avro 크기와 같고 shuffle write가 없는 stage다(2026-03 벤치마크의 Stage 4).

### 5.2 append job별 row 수·파일 수·크기 (Trino)

append job 1회 = snapshot 1개다. job은 `batchId`로 넣은 `snapshot-property.batch_id`(snapshot summary의 `batch_id`)로 구분한다. `$files`에는 snapshot 연결 컬럼이 없어서 job별 집계에 쓸 수 없다.

**① job별 합계** (`$snapshots`)

```sql
SELECT element_at(summary, 'batch_id')                                     AS batch_id,
       snapshot_id,
       CAST(element_at(summary, 'added-records') AS BIGINT)                AS row_cnt,
       CAST(element_at(summary, 'added-data-files') AS INTEGER)            AS files,
       ROUND(CAST(element_at(summary, 'added-files-size') AS BIGINT) / POWER(1024, 3), 3) AS gb,
       ROUND(CAST(element_at(summary, 'added-files-size') AS BIGINT)
             / CAST(element_at(summary, 'added-data-files') AS DOUBLE) / POWER(1024, 2), 1) AS avg_mb
FROM iceberg.<db>."<테이블1>_tune$snapshots"
WHERE element_at(summary, 'batch_id') LIKE 'tune-t1-%'
ORDER BY committed_at;
```

- `row_cnt`: 같은 고정 입력이면 모든 회차에서 같아야 한다. 다르면 입력이 바뀐 것이므로 그 회차는 비교에서 뺀다.
- dcu/GB의 GB는 `gb`(이번 job의 parquet 출력 크기)를 쓴다.
- `summary['batch_id']`로 쓰면 안 된다. Trino는 map에 없는 key를 `[]`로 읽으면 `Key not present in map` 오류로 쿼리 전체가 실패한다. batch_id가 없는 snapshot(Compaction, 수동 INSERT 등)이 하나라도 있으면 실패하므로 `element_at`을 쓴다.

**② job별 파일 크기 분포** (`$entries`)

```sql
SELECT element_at(s.summary, 'batch_id')                                   AS batch_id,
       COUNT(*)                                                            AS files,
       SUM(e.data_file.record_count)                                       AS row_cnt,
       ROUND(MIN(e.data_file.file_size_in_bytes) / POWER(1024, 2), 1)      AS min_mb,
       ROUND(AVG(e.data_file.file_size_in_bytes) / POWER(1024, 2), 1)      AS avg_mb,
       ROUND(MAX(e.data_file.file_size_in_bytes) / POWER(1024, 2), 1)      AS max_mb
FROM iceberg.<db>."<테이블1>_tune$entries" e
JOIN iceberg.<db>."<테이블1>_tune$snapshots" s
  ON e.snapshot_id = s.snapshot_id
WHERE e.status <> 2
  AND element_at(s.summary, 'batch_id') LIKE 'tune-t1-%'
GROUP BY element_at(s.summary, 'batch_id')
ORDER BY 1;
```

- 조건은 `status <> 2`(삭제 제외)다. `status = 1`(ADDED)로 쓰면 append가 쌓여 manifest가 합쳐진 뒤 예전 job이 빠진다. 합쳐진 manifest에서는 예전 파일이 `EXISTING`(0)으로 바뀌고, 파일을 추가한 job의 `snapshot_id`는 그대로 남는다.
- ②의 `files`·`row_cnt`는 ①과 같아야 한다.
- `$entries`는 현재 snapshot에 살아 있는 파일만 보여 준다. Compaction으로 다시 쓰인 파일은 빠진다. 테스트 테이블은 Compaction을 돌리기 전까지 해당 없다.

**검증** (2026-09-30, Trino 482)

- 로컬 Spark 3.5.8 + Iceberg 1.10.1로 파티션 `hours(ts)`·`par_a`, Sort Order `sort_a`·`sort_b`, `range` 테이블을 만들고 batch_id를 붙여 6회 append했다(row 2만~12만, job당 파일 4개). `commit.manifest.min-count-to-merge=3`으로 manifest 합치기를 일으켰다.
- 이 테이블을 Trino 482에 등록하고 위 SQL을 그대로 실행했다. ①·② 모두 6개 job의 row 수·파일 수가 실제 넣은 값과 일치했다.
- 같은 상태에서 `status = 1`은 6개 중 4개 job을 놓쳤다.
- batch_id 없는 snapshot을 하나 추가하자 `summary['batch_id']` 방식은 `Key not present in map: batch_id`로 실패했고, `element_at` 방식은 6개 job을 그대로 반환했다.

### 5.3 driver 로그: AQE 목표 크기

driver pod에는 컨테이너가 2개(`spark-kubernetes-driver`, `monitoring`)라서 `-c`로 driver 컨테이너를 지정한다.

```bash
kubectl logs -n <ns> append-tune-t1-l0-r1-driver -c spark-kubernetes-driver | grep "actual target size"
```

- 출력 형식: `INFO ShufflePartitionsUtil: For shuffle(0), advisory target size: 402653184, actual target size 83886080, minimum partition size: 1048576`
- L0에서 `actual target size`가 advisory(402653184 = 384MB)보다 작으면 core 수가 목표를 줄였다는 뜻이다. §1의 계산이 맞는지 이 값으로 확인한다.
- L1에서는 두 값이 같아야 한다.
- INFO 레벨 로그다. 아무것도 안 나오면 driver의 log4j 레벨이 WARN 이상인지 확인한다.
- 검증 (2026-10-02, 로컬 Spark 3.5.8 + Iceberg 1.10.1): 같은 구조의 테이블에 append 1회 실행 시 위 형식으로 출력됨을 확인했다(advisory 402653184).

### 5.4 기동 시간

```bash
# driver pod 생성 → driver 컨테이너 시작
kubectl get pod -n <ns> append-tune-t1-l0-r1-driver \
  -o jsonpath='{.metadata.creationTimestamp}{"  "}{.status.containerStatuses[?(@.name=="spark-kubernetes-driver")].state.terminated.startedAt}{"\n"}'

# SparkContext 시작 → 마지막 executor 등록
kubectl logs -n <ns> append-tune-t1-l0-r1-driver -c spark-kubernetes-driver \
  | grep -E "Running Spark version|Registered executor" | sed -n '1p;$p'
```

- `containerStatuses`는 컨테이너 이름으로 고른다. 순서(`[0]`)로 고르면 `monitoring` 컨테이너 값이 나올 수 있다.
- 두 구간의 합을 기동 시간으로 기록한다.
- 현재 `spark.kubernetes.allocation.batch.size`는 기본값 5다. 14대면 5·5·4대로 1초 간격 3라운드에 나눠 요청한다.

---

## 6. 판정 기준

| 구분 | 기준 |
|------|------|
| 필수 | 기동 포함 5분 안에 종료, spill 0, task error rate 0%, executor 유실 0 |
| 주 지표 | dcu/GB (낮을수록 좋음). 차이가 노이즈 기준선보다 작으면 같은 것으로 본다 |
| 보조 | batch당 파일 수·평균 크기 (Compaction 입력 변화 확인용) |

- duration만으로 판정하지 않는다. 쓰기 task가 줄면 duration은 늘지만 dcu는 줄 수 있다(작업 5와 같은 원칙).
- **memory usage %는 판정에 쓰지 않는다.** JVM은 받은 메모리만큼 채워 쓰므로 memory를 8g → 6g → 4g로 줄여도 사용률은 60~89%로 비슷했다(§7.4). 메모리 부족은 spill로만 드러난다.
- 설정마다 보는 지표가 다르다. memory·overhead는 spill·task error·retry(통과/실패), 비용은 dcu, duration은 구조가 바뀌는 변경(parallelismFirst, core 수)에서만 본다.

---

## 7. 측정 결과 (1번 테이블, 2026-10-06 ~ 10-13)

입력 고정: 09/29 15:25 구간, JOB_HISTORY 123 row, avro 2,690개, 4,180,959,271 bytes(3.89GiB). 모든 회차 row_cnt 302,908 동일. 값 순서는 duration / dcu / 그 외.

### 7.1 L0 vs L1 (instances 12, memory 8g, overhead 4g, 각 5회)

| 회차 | L0 duration / dcu / idle / 샘플링 stage | L1 duration / dcu / idle / 샘플링 stage |
|---|---|---|
| 1 | 2.6m / 0.1304 / 28.8% / 66s | 4.2m / 0.2111 / 25.3% / 108s |
| 2 | 2.6m / 0.1286 / 25.7% / 52s | 3.5m / 0.1794 / 32.8% / 90s |
| 3 | 1.6m / 0.0795 / 33.3% / 29s | 3.1m / 0.1570 / 30.6% / 72s |
| 4 | 3.5m / 0.1791 / 23.5% / 84s | 2.5m / 0.1244 / 27.2% / 56s |
| 5 | 2.0m / 0.0983 / 42.8% / 43s | 2.2m / 0.1086 / 30.7% / 46s |
| duration만 5회 추가 | 2.3 / 2.5 / 2.9 / 2.7 / 2.9m | 3.0 / 3.4 / 2.7 / 2.3 / 2.5m |

- 공통: spill 0, task error 0%, retry 없음, 기동 11~13초. 파일 L0 59개(avg 55.5 / min 0.6 / max 71MB), L1 17개(192.7 / 3.2 / 261MB)
- 첫 회차(instances 14): 1.8m / 0.0995, 파일 71개 × 46.1MB, actual target size 92,210,132. DataFlint input 7.83GiB(avro 3.9GiB × 2회 읽기), output 3.2GiB, shuffle read/write 4.59GiB → shuffle 배수 1.43
- actual target size로 §1 가설 확인: 107,578,488 × 48 core = 4.81GiB(AQE 통계값, 실측 4.59GiB와 5% 차이), 쓰기 task 48개 + 파티션 경계 파일 11개 = 59개

### 7.2 executor 수 (L0, memory 8g, overhead 4g, 각 5회)

| instances | 총 core | duration 중앙값 / 최대 | dcu 중앙값 | 샘플링 stage 중앙값 | 파일 | memory usage |
|---|---|---|---|---|---|---|
| 12 | 48 | 2.8 / 4.0분 | 0.140 | 72초 | 59개 | 59~72% |
| 8 | 32 | 4.3 / **5.7분** | 0.147 | 90초 | 39개 | 61~63% |
| 6 | 24 | 4.2 / **5.7분** | 0.107 | 108초 | 29개 | 61~87% |

- 8·6대는 5분을 넘긴 회차가 있어 탈락. core를 줄인 만큼 duration이 늘어 dcu는 거의 같다(dcu ≈ instances × 분 × 0.0042).
- 샘플링 stage(avro 읽기만 하는 stage)가 core 수에 반비례한다. 읽기 task 약 114개를 48 core로 돌리면 48·48·18개씩 3차례, 32 core면 4차례, 24 core면 5차례가 되기 때문이다(§8.2).

### 7.3 memoryOverhead (instances 12, memory 8g, 각 5회)

| overhead | duration 중앙값 | dcu 중앙값 | 분당 dcu | task error / retry |
|---|---|---|---|---|
| 4g | 2.8분 | 0.140 | 0.050 | 0% / 없음 |
| 2g | 1.9분 | 0.089 | 0.047 | 0% / 없음 |
| 1g | 2.1분 | 0.095 | 0.045 | 0% / 없음 |

- duration 차이는 회차 시점의 부하 차이다. overhead는 처리 속도를 바꾸지 않는다. 효과는 분당 dcu(메모리 할당량)에만 나타난다.
- 2g 채택. 1g도 통과했지만 Compaction에서 overhead 부족이 executor 유실로 늦게 드러난 전례가 있어 한 단계 여유를 둔다.

### 7.4 memory (instances 12, overhead 2g, 각 5회)

| memory | core당 heap | duration 중앙값 | spill | memory usage |
|---|---|---|---|---|
| 8g | 2.0g | 1.9분 | 0 | 67~73% |
| 6g | 1.5g | 2.5분 | 0 | 60~78% |
| 4g | 1.0g | 2.3분 | 0 | 60~89% |
| 3g | 0.75g | 2.0분 | 0 | |
| 2g | 0.5g | 1.9분 | **5회 중 2회 (80.65MiB, 86.67MiB)** | |

- spill이 처음 생긴 지점이 2g이므로 한 단계 위인 **core당 0.75g**(4c × 3g)로 정한다.
- memory usage는 8g → 4g 사이에서 거의 같은 범위였다. 받은 메모리만큼 채워 쓰기 때문이며, 필요한 메모리의 근거가 되지 못한다.

### 7.5 executor 모양·기타 (사용자 실행, 수치 미기록)

- 8대 × 6c × 5g + 2g, 4대 × 12c × 8g + 2g: 모두 spill·오류 없이 정상 처리
- `spark.sql.files.maxPartitionBytes` 확대: 효과 없음, 오히려 느려진 회차 있음 → 기본값 유지

---

## 8. 확정값과 공식 (1번 테이블, 2026-10-14)

### 8.1 확정값

| 항목 | 값 | 근거 |
|---|---|---|
| `parallelismFirst`, `shuffle.partitions`, `maxPartitionBytes` | 기본값 유지 | §7.1, §7.5 |
| `write.distribution-mode` | `range` 유지 | 사용자 결정 |
| executor core | **8** | core당 heap 0.75g(§7.4), task당 overhead 256MB(= 4c × 1g, §7.3)가 모두 실측 통과 범위. 12c는 task당 overhead 171MB로 §7.5에서 1회만 정상 확인(5회 검증 안 함)이고, K8s 기본 80% 등록 조건에서 4대 중 4대가 떠야 시작한다 |
| executor memory / memoryOverhead | **6g / 2g** | 8c × 0.75g = 6g |
| executor 수 | `ceil(총 core ÷ 8)`, 총 core = `ceil(읽기 task ÷ 2)` | §8.2 |
| `spark.kubernetes.allocation.batch.size` | **20** | 3.5.8 기본값 5(jar 확인), 4.0~4.1은 10, 4.2는 20. 20이면 8대를 한 번에 요청하고 4.x에서도 그대로 쓴다 |

- 공식 문서 조사(2026-10-13): "executor당 5 core" 권장의 근거는 HDFS client 처리량(Cloudera 2015)이라 MinIO에는 해당 없음. Spark 공식 문서에는 core 권장값이 없고, Databricks는 노드당 1 executor, Dataproc은 4·8·16, EMR Serverless는 최대 32 core를 쓴다. GC 경고는 heap 32~64GB 이상, S3A 연결 pool은 executor당 96개(Hadoop 3.3.4)라 두 모양 모두 한계에 걸리지 않는다.

### 8.2 executor 수 산정 공식

Spark는 avro 읽기 task를 "파일별 실제 크기 + 4MiB(`openCostInBytes`)"를 128MiB(`maxPartitionBytes`)까지 묶어 만든다. 1번 테스트 batch는 파일 2,690개(평균 1.48MiB)라 데이터 3,987MiB보다 파일 수 몫(2,690 × 4 = 10,760MiB)이 커서 읽기 task가 약 114개가 된다. 바이트만 보는 기존 공식(`ceil(바이트 ÷ 128MB × 1.5 ÷ 4)`)은 이 몫을 보지 못한다.

읽기 stage 시간은 "읽기 task ÷ 총 core"(차례 수)에 비례한다(§7.2). 그래서 차례 수를 정하면 core가 정해진다.

```python
import math

MiB = 1024 * 1024
MAX_PARTITION_BYTES = 128 * MiB   # spark.sql.files.maxPartitionBytes 기본값
OPEN_COST_BYTES     = 4 * MiB     # spark.sql.files.openCostInBytes 기본값
READ_WAVES          = 2           # 읽기 task를 몇 차례에 처리할지 (사용자 선택: 속도 우선)
EXECUTOR_CORES      = 8
MAX_TOTAL_CORES     = 128         # 재처리 등 큰 batch 상한

def spark_read_tasks(file_sizes):              # Spark 3.5 FilePartition 규칙 그대로
    tasks, current = 0, 0
    for size in sorted(file_sizes, reverse=True):          # 큰 파일부터
        if current + size > MAX_PARTITION_BYTES and current > 0:   # 판단: 실제 크기만
            tasks += 1; current = 0
        current += size + OPEN_COST_BYTES                          # 누적: +4MiB
    return tasks + (1 if current > 0 else 0)

def num_executors(file_sizes):
    read_tasks  = spark_read_tasks(file_sizes)
    total_cores = min(math.ceil(read_tasks / READ_WAVES), MAX_TOTAL_CORES)
    return max(math.ceil(total_cores / EXECUTOR_CORES), 1)
```

- 1번 테스트 batch: 읽기 task 113~114 → 57 core → **8대 × 8c = 64 core**. 지금(48 core)보다 core 33% 증가, 읽기 차례 비율 2.4 → 1.8(정수로 3 → 2). 예상 duration −20%(읽기 stage 2개가 duration의 약 60%, 차례 3 → 2로 −33%), dcu +7%(64 ÷ 48 × 0.8)
- 검증: 로컬 Spark 3.5.8에 파일 2,690개(크기 ÷ 4MiB 비율 0.37, 운영과 동일)를 만들어 읽은 결과 Spark 실제 114 = 이 함수 114. 단순 나눗셈 `(바이트 + 파일 수 × 4MiB) ÷ 128MiB`는 116으로 2개 많다. Spark가 "넣을지 판단"에는 4MiB를 안 더하기 때문이다
- 차례 수 선택지: 2(64 core, 속도) / 3(5대 × 8c = 40 core, dcu −17%, 최악 duration 약 4.6분으로 5분 여유 적음)
- 테이블마다 계수를 따로 잡을 필요는 없다. 읽기 task 1개의 일감은 테이블과 무관하게 같고(파일 약 24개, 128MiB), 테이블 차이는 읽기 task 수(파일 수)와 메모리(spill 확인)에 각각 반영된다
- 전제: 총 core < 읽기 task 수. 128 상한에서도 성립한다. 4MiB가 MinIO에서 실제 파일 여는 비용과 같은지는 확인하지 않았다
- 변경 전/후 코드: `pipeline/examples/append_executor_sizing_example.py`

---

## 9. 남은 단계

1. **1번 확정 모양 확인 (사용자)**: 8대 × 8c × 6g + 2g, `allocation.batch.size=20`으로 3회. stage 4 task 수(약 113~114), 샘플링 stage 초(72 → 약 50 기대), spill 0, task error 0%, retry 없음. 노드 allocatable에 8 CPU + 8Gi pod 8개가 Pending 없이 뜨는지
2. **2·3·4번 테이블**: 같은 모양으로 테이블당 2~3회 spill 0·task error 0%·retry 없음 확인. 2번은 row 폭이 좁아 같은 MiB에 row가 많으므로 spill을 특히 본다. 공식으로 2번은 읽기 task 약 101 → 51 core → 7대
3. **운영 반영**: `get_jobs` 공식 교체(§8.2), SparkApplication executor 설정, `allocation.batch.size`
4. **운영 적용 후**: Airflow duration을 `job_durations.py`(`pipeline/airflow-job-duration.md`)로 집계. hourly Compaction 입력 파일 수(총 core가 바뀌면 L0에서 파일 수도 바뀐다: 64 core면 batch당 약 75개)와 ratio 0.13 수렴 대수를 설계서 §8.3 체크표로 확인
5. **이후 (사용자 계획)**: DA로 여러 batch 크기 테스트. 상한 128 core에 걸리는 batch에서 task당 처리량이 늘어나므로 spill을 다시 본다
