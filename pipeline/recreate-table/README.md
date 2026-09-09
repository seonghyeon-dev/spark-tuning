# Iceberg 테이블 재생성 + `tmp_id`(NOT NULL) 추가

Iceberg 테이블에 `tmp_id STRING NOT NULL` 컬럼을 원하는 위치에 추가한다. 값은 Oracle을 2개 키로 조인해 채우고, 매칭이 없으면 `''`를 넣는다.

## 1. 왜 재생성인가

| 시도 | 결과 |
|------|------|
| Spark SQL `ALTER TABLE ... ADD COLUMN tmp_id STRING NOT NULL` | ❌ Iceberg는 비어 있지 않은 테이블에 required 컬럼 추가를 거부 |
| `ALTER COLUMN tmp_id SET NOT NULL` | ❌ 미지원 |
| `ADD COLUMN ... DEFAULT ''` | ❌ Iceberg 1.11.0 기준 Spark SQL 미지원 |
| Trino `ALTER TABLE ... ADD COLUMN ... NOT NULL` | ❌ 비어 있지 않은 테이블 불가. `AFTER` 문법 없음, `SET NOT NULL` 미지원 |

→ **임시 테이블로 복사 → `DROP ... PURGE` → `CREATE` → 조인해서 `INSERT`** 가 유일한 방법이다. 새 테이블은 snapshot 이력과 table UUID가 초기화된다.

## 2. 역할 분담

| 구간 | 누가 | 내용 |
|------|------|------|
| `backup` | **Spark 앱** | 기존 테이블 → 임시 테이블 CTAS, 건수 일치 확인. `SHOW CREATE TABLE`을 로그에 남긴다 |
| `check` | **Spark 앱** (쓰기 없음) | Oracle 읽기 → 키 중복 0건 확인 → 임시 테이블 대비 unmatched 집계 |
| DROP / CREATE | **수동 (spark-sql)** | `manual-ddl.sql` — 원본 `DROP ... PURGE`, `tmp_id`를 끼운 DDL로 재생성, `WRITE ORDERED BY` |
| `load` | **Spark 앱** | 임시 테이블 `LEFT JOIN` Oracle → 신규 테이블 `INSERT` → 건수·`tmp_id` 검증 |

Iceberg 접근은 기존 Spark Job이 이미 되는 상태이므로 그 SparkApplication 스펙을 그대로 쓴다. **신규는 Oracle 접근뿐**이며 ojdbc8이 fat jar에 들어 있어 이미지나 `--jars` 변경이 없다. Oracle에는 SELECT만 나간다 — `spark.read.format("jdbc")`는 SELECT를 실행하고, `createOrReplaceTempView("ora")`는 Spark 세션 안의 이름 등록일 뿐이다.

## 3. 구성

```
pipeline/recreate-table/
├── build.sbt                          # Scala 2.12.18 / Spark 3.5.8(provided) / Iceberg 1.10.1(provided) / ojdbc8(fat jar 포함)
├── project/{build.properties, plugins.sbt}   # sbt 1.10.7, sbt-assembly 2.3.1
├── src/main/scala/RecreateTable.scala # backup / check / load. 상단 설정 블록만 채운다
├── manual-ddl.sql                     # 수동 DROP / CREATE / WRITE ORDERED BY 템플릿
└── k8s/recreate-table-sparkapp.yaml   # 기존 Job 스펙에서 바꿀 항목(★)만 표시한 SparkApplication
```

버전은 운영 스택 실측(Spark 3.5.8 / Scala 2.12 / Iceberg 1.10.1, `s3fileio-migration-guide.md` §5.0.2)에 맞췄다. 기존 앱(Maven)에 코드를 합칠 경우 `pom.xml`에 다음만 추가하면 된다.

```xml
<dependency>
  <groupId>com.oracle.database.jdbc</groupId>
  <artifactId>ojdbc8</artifactId>
  <version>23.9.0.25.07</version>
</dependency>
```

## 4. 설정 블록 (`RecreateTable.scala` 상단)

전부 자리표시자이며 이름만 바꾸면 된다. **컬럼 목록은 설정하지 않는다** — `load`가 신규 테이블 스키마를 읽어 그 순서대로 SELECT를 만들고, `tmp_id` 자리에만 `COALESCE(CAST(o.tmp_id AS STRING), '')`를 넣는다.

| 항목 | 자리표시자 | 설명 |
|------|-----------|------|
| `Catalog` / `Database` / `Table` | `iceberg` / `db` / `table_a` | 재생성 대상. `backup`의 원본이자 `load`의 목적지 (같은 이름으로 다시 만든다) |
| `TmpTable` | `table_a_tmp` | 임시(백업) 테이블 |
| `OracleUrl` / `OracleUser` / `OraclePassword` | — | 1회성이라 하드코딩. **커밋하지 말 것** |
| `OracleQuery` | `(SELECT key1, key2, tmp_id FROM ORA_SCHEMA.ORA_TABLE) t` | 키 타입을 Iceberg 쪽과 맞춘다. 컬럼명이 다르면 `AS`로 |
| `JoinKeys` | `key1 -> key1`, `key2 -> key2` | Iceberg 컬럼 → `ora` 컬럼 |
| `TmpIdCol` / `TmpIdDefault` | `tmp_id` / `''` | 신규 컬럼과 unmatched 기본값 |

Oracle이 반환하는 컬럼명은 대문자지만 Spark SQL은 기본 대소문자 무시라 소문자로 참조해도 된다. Oracle `NUMBER`는 `decimal`로 오며 Iceberg `int`와 암묵 CAST로 조인된다. `string` vs 숫자처럼 계열이 다르면 조인이 조용히 비어 unmatched로만 드러나므로, `check` 로그의 unmatched 비율이 예상과 다르면 타입 경고(`⚠`)부터 본다.

## 5. 절차

```bash
# 0. Airflow 쓰기 DAG(append) 일시 중지. Compaction/maintenance DAG 도 대상 테이블 건은 멈춘다

# 1. 빌드 → jar 배치 (매니페스트 mainApplicationFile 과 일치)
cd pipeline/recreate-table && sbt assembly     # → target/scala-2.12/recreate-table-assembly.jar
mc cp target/scala-2.12/recreate-table-assembly.jar minio/<bucket>/jars/

# 2. backup
#    arguments: ["backup"]
kubectl apply -f k8s/recreate-table-sparkapp.yaml
kubectl logs -f recreate-table-tmp-id-driver -n <ns> | grep RecreateTable
#    → "[backup 완료] ... N 건". 로그의 SHOW CREATE TABLE 출력을 manual-ddl.sql 에 옮긴다
kubectl delete -f k8s/recreate-table-sparkapp.yaml

# 3. check — 아무것도 쓰지 않는다
#    arguments: ["check"]
#    → 키 중복 0건 통과, unmatched 건수·비율 납득 가능, 키 타입 경고 없음
kubectl delete -f k8s/recreate-table-sparkapp.yaml

# 4. (수동, spark-sql) manual-ddl.sql — DROP ... PURGE → CREATE (tmp_id 포함) → WRITE ORDERED BY → SHOW CREATE TABLE 로 확인

# 5. load
#    arguments: ["load"]
#    → "[load 완료] ... 건수 N, tmp_id='' M 건"
kubectl delete -f k8s/recreate-table-sparkapp.yaml

# 6. Trino 확인 (HMS 경유라 별도 조치 없음)
#    SHOW CREATE TABLE ...;  SELECT count(*) ...;  SELECT count(*) ... WHERE tmp_id = '';

# 7. Airflow 재개. 며칠 뒤 임시 테이블 정리 — PURGE 없이 DROP 하면 데이터 파일이 남는다
#    DROP TABLE iceberg.db.table_a_tmp PURGE;
```

### 각 모드의 `require` (실패 시 즉시 종료)

| 모드 | 검사 |
|------|------|
| `backup` | 원본 존재 · 임시 없음 · 임시 COUNT == 원본 COUNT |
| `check` | `ora` 0건 아님 · 키·`tmp_id` 컬럼 존재 · **`ora` 키 중복 == 0** (unmatched는 기록만) |
| `load` | 임시 존재 · 신규 존재 · **신규 COUNT == 0** (원본을 안 지웠거나 이미 load 된 경우 차단) · 신규 `tmp_id` 존재·`string`·NOT NULL · 신규 컬럼(tmp_id 제외) == 임시 컬럼 · `ora` 키 중복 == 0 · INSERT 후 신규 COUNT == 임시 COUNT · `tmp_id = ''` == unmatched · `tmp_id IS NULL` == 0 |

## 6. 실패 시

| 어디서 | 상태 | 조치 |
|--------|------|------|
| `backup` | 임시 테이블이 없거나 건수 불일치 | 임시 `DROP ... PURGE` 후 재실행 |
| `check` | 아무것도 안 바뀜 | Oracle 쿼리·키 수정 후 재실행 |
| 수동 CREATE | 원본 없음, 임시 있음 | DDL 수정해 다시 CREATE (부분 생성분은 `DROP ... PURGE`) |
| `load` INSERT | 신규 테이블 비어 있음 (Iceberg 단일 커밋) | 원인 수정 후 `load` 재실행 |
| `load` 사후 검증 | 신규 테이블에 데이터 있음 | 원인 확인. 재적재는 신규 `DROP ... PURGE` → CREATE → `load` |

원래 스키마로 되돌리려면: `manual-ddl.sql`에서 `tmp_id` 줄을 뺀 DDL로 CREATE 하고, `INSERT INTO ... SELECT * FROM <임시>`. 임시 테이블은 파티션·Sort Order가 없는 CTAS 결과이므로 rename 해서 그대로 쓰지 않는다.

## 7. 주의

- `restartPolicy: Never` — 각 모드가 멱등이 아니므로 operator 재시도가 있으면 안 된다 (`backup`은 임시 존재로, `load`는 신규 COUNT > 0으로 막히긴 한다)
- 새 테이블은 snapshot 이력·UUID가 초기화된다. 재처리 DAG의 `batch_id` 영수증(`.snapshots` summary)도 사라지므로, 재생성 직전 구간에 FAILURE 재적재 대기 건이 있으면 재처리 DAG 쪽 확인이 필요하다
- `DROP TABLE ... PURGE`는 `gc.enabled=false`면 거부된다 (`manual-ddl.sql` 0-1)
- JDBC 읽기는 executor에서 실행된다 — Oracle 방화벽은 driver·executor 양쪽에 열려 있어야 한다
- 임시 테이블은 파티션·Sort Order가 없는 평면 테이블이다. `load` 시 신규 테이블의 `write.distribution-mode=range` + Sort Order에 따라 다시 정렬·분배되므로 무관하다
