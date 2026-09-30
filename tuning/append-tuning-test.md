# append Job 튜닝 테스트 절차 (빅테이블)

| 항목 | 내용 |
|------|------|
| 대상 | 빅테이블 4개 append Job (1번 테이블 먼저) |
| 방식 | SparkApplication CRD 직접 apply, 테스트용 복제 테이블, 운영 5분치 고정 입력 |
| 환경 | Spark 3.5.8, Iceberg 1.10.1 (운영) |
| 결과 반영 | `tuning/spark-tuning-guide.md`, 보고용 요약은 측정 완료 후 작성 |
| 작성일 | 2026-09-30 |

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
| shuffle 크기 | 3,157MB × 1.41 = 약 4.45GB (1.41 = 2026-03 벤치마크의 shuffle 9.2GiB ÷ 출력 6.5GiB, A안 스키마 값. 현행 값은 L0 실행의 shuffle write ÷ 출력으로 다시 잰다) |
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

- L1 파일 크기: 384MB ÷ 1.41 = 약 272MB. L2 값 계산: 4.45GB ÷ 192MB = 약 24 task, 파일당 192MB ÷ 1.41 = 약 136MB
- L1이 필수 조건을 통과하고 dcu/GB가 L0보다 낮으면 L2는 생략한다. L1의 쓰기 stage가 길어 필수 조건을 못 맞추면 L2를 돌린다.
- 실행 순서: L0 → L1 → L0 → L1 ... 순으로 번갈아 5회씩 돌린다. 시간대별 클러스터 부하가 한 후보에만 몰리지 않게 하기 위해서다.
- L0 5회의 편차 (최댓값 − 최솟값) ÷ 평균을 이번 테스트의 노이즈 기준선으로 쓴다.

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

```bash
kubectl logs -n <ns> append-tune-t1-l0-r1-driver | grep "actual target size"
```

- 출력 예: `advisory target size: 402653184, actual target size 83886080, ...`
- L0에서 `actual target size`가 advisory(402653184 = 384MB)보다 작으면 core 수가 목표를 줄였다는 뜻이다. §1의 계산이 맞는지 이 값으로 확인한다.
- L1에서는 두 값이 같아야 한다.

### 5.4 기동 시간

```bash
# driver pod 생성 → driver 컨테이너 시작
kubectl get pod -n <ns> append-tune-t1-l0-r1-driver \
  -o jsonpath='{.metadata.creationTimestamp}{"  "}{.status.containerStatuses[0].state.terminated.startedAt}{"\n"}'

# SparkContext 시작 → 마지막 executor 등록
kubectl logs -n <ns> append-tune-t1-l0-r1-driver \
  | grep -E "Running Spark version|Registered executor" | sed -n '1p;$p'
```

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

---

## 7. 기록표

| 후보 | 회차 | instances | duration | dcu | row_cnt | gb | dcu/GB | idle cores | spill | task error | 유실 | files | avg_mb | min_mb | max_mb | actual target size | 샘플링 stage | 기동 |
|------|------|-----------|----------|-----|---------|----|--------|-----------|-------|-----------|------|-------|--------|--------|--------|-------------------|-------------|------|
| L0 | 1 | | | | | | | | | | | | | | | | | |
| L1 | 1 | | | | | | | | | | | | | | | | | |

---

## 8. 이후 단계

1. **리소스 비교**: §4에서 고른 설정으로 비교한다. 순서는 executor 수(3지점) → memory(spill 0인 최솟값) → memoryOverhead(4g → 3g → 2g, task error rate로 판정) → `spark.kubernetes.allocation.batch.size`
   - 채택한 executor 수는 `get_jobs` 공식의 계수(현재 1.5)로 환산해 운영에 반영한다.
   - 계산 예시(가상 값): avro 4,700MB batch는 현재 공식으로 ceil(4,700 ÷ 128 × 1.5 ÷ 4) = 14대다. 이 batch에서 10대가 최적이면 계수 = 10 × 4 ÷ (4,700 ÷ 128) = 약 1.09
2. **시간당 합계 검증**: batch 12개를 적재한 뒤 테스트 테이블에 hourly Compaction(운영 설정)을 1회 돌린다.
   - 비교 지표: append dcu × 12 + Compaction dcu
   - 입력 파일이 커지면 ratio 0.13의 전제(GB당 task 약 9개)가 바뀐다. 파일이 커지는 방향은 영향이 작다고 추정돼 있다(설계서 §4.5·§9). 실측 대수를 설계서 §4.4 역산 방법으로 확인한다.
3. **2·3·4번 테이블 확장**: 1번 결과로 시작값을 잡고 테이블당 2~3회 확인한다. memory는 테이블별로 정한다.
4. **운영 적용 후**: Airflow duration은 `job_durations.py`(`pipeline/airflow-job-duration.md`)로 집계한다.
