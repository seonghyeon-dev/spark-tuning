# Iceberg 테이블 재생성 + `tmp_id`(NOT NULL) 추가

1회성 Scala Spark 앱. Iceberg 테이블에 `tmp_id STRING NOT NULL` 컬럼을 원하는 위치에 추가하고, 값은 Oracle DB를 2개 키 컬럼으로 조인해 채운다.

## 1. 왜 재생성인가

| 시도 | 결과 |
|------|------|
| Spark SQL `ALTER TABLE ... ADD COLUMN tmp_id STRING NOT NULL` | ❌ Iceberg는 비어 있지 않은 테이블에 required 컬럼 추가를 거부 |
| `ALTER COLUMN tmp_id SET NOT NULL` | ❌ 미지원 |
| `ADD COLUMN ... DEFAULT ''` | ❌ Iceberg 1.11.0 기준 Spark SQL 미지원 |
| Trino `ALTER TABLE ... ADD COLUMN ... NOT NULL` | ❌ 비어 있지 않은 테이블 불가. `AFTER` 문법 없음, `SET NOT NULL` 미지원 |

→ **백업 → `DROP ... PURGE` → `CREATE` → `INSERT`** 가 유일한 방법이다. 새 테이블은 snapshot 이력과 table UUID가 초기화된다.

Oracle에는 아무것도 만들지 않는다. `spark.read.format("jdbc")`는 Oracle에 SELECT만 실행하고, `createOrReplaceTempView("ora")`는 Spark 세션 안의 이름 등록일 뿐이다.

## 2. 구성

```
pipeline/recreate-table/
├── build.sbt                          # Scala 2.12.18 / Spark 3.5.8(provided) / Iceberg 1.10.1(provided) / ojdbc8(fat jar 포함)
├── project/{build.properties, plugins.sbt}   # sbt 1.10.7, sbt-assembly 2.3.1
├── src/main/scala/RecreateTable.scala # 본체. 상단 설정 블록만 채운다
└── k8s/recreate-table-sparkapp.yaml   # spark-operator SparkApplication (카탈로그·S3A conf 포함)
```

버전은 **운영 스택 실측**(Spark 3.5.8 / Scala 2.12 / Iceberg 1.10.1, `s3fileio-migration-guide.md` §5.0.2)에 맞췄다. Spark·Iceberg runtime은 이미지가 제공하므로 `provided`, `ojdbc8`은 이 앱만 쓰므로 fat jar에 넣는다 (라이브러리 배치 원칙: 같은 가이드 §5.0.5). 이미지 변경도 `--jars`도 필요 없다.

## 3. 실행 모드

| 인자 | 하는 일 | 쓰기 |
|------|---------|------|
| `check` (기본) | Oracle 읽기 → 키 중복 / unmatched 집계 → `CreateDdl` ↔ INSERT SELECT 이름·순서·NOT NULL 대조 | **없음** |
| `run` | 1 백업 → 2 Oracle → 3 검증 → 4 DROP PURGE / CREATE → 5 INSERT → 6 검증 | 있음 |
| `resume` | 4·5단계 실패 후 재개. 백업이 있고 대상 테이블이 **없어야** 한다 (부분 생성분은 수동 `DROP ... PURGE`) | 있음 |

### `run` 절차와 안전장치

| 단계 | 동작 | `require` (실패 시 즉시 종료) |
|------|------|------------------------------|
| 0 | 사전 검사 | 대상 존재 · 백업 없음 · `gc.enabled≠false` · DDL/SELECT 정합성(아래) |
| 1 | `CREATE TABLE {bak} USING iceberg AS SELECT * FROM {tbl}` | 백업 COUNT == 원본 COUNT |
| 2 | Oracle jdbc(`fetchsize 10000`) → `cache` → temp view `ora` | 0건 아님, 키·`tmp_id` 컬럼 존재 |
| 3 | `ora` 키 중복 검사, `{bak} LEFT JOIN ora ... WHERE o.tmp_id IS NULL` 집계 | **키 중복 == 0** (unmatched는 기록만) |
| 4 | `DROP TABLE {tbl} PURGE` → `CreateDdl` → `postCreateDdls`(Sort Order) | DROP 후 부재, CREATE 후 존재, `tmp_id` NOT NULL, 새 테이블 컬럼 == SELECT 컬럼 |
| 5 | `INSERT INTO {tbl} SELECT ..., COALESCE(o.tmp_id, '') ... FROM {bak} t LEFT JOIN ora o` | (단일 커밋 — 실패하면 새 테이블은 비어 있다) |
| 6 | 검증 | 새 COUNT == 백업 COUNT · `tmp_id = ''` 건수 == unmatched · `tmp_id IS NULL` == 0 |

**4단계부터는 롤백이 없다.** 0~3단계의 `require`가 실질적 안전장치이므로 `run` 전에 `check`를 반드시 통과시킨다.

**0단계의 DDL/SELECT 정합성 검사**: `CreateDdl`을 파싱해 스키마를 뽑고 ① `tmp_id`가 있고 `NOT NULL`인지 ② INSERT SELECT의 컬럼 이름·순서가 DDL과 같은지 ③ 원본 컬럼이 DDL에서 빠지거나 없는 컬럼이 들어가지 않았는지를 DROP 전에 확인한다. 타입은 나란히 출력하므로(`⚠` 표시) 로그에서 눈으로 대조한다.

## 4. 설정 블록 채우기 (`RecreateTable.scala` 상단)

| 항목 | 값 |
|------|-----|
| `Catalog` / `Database` / `Table` | 매니페스트의 `spark.sql.catalog.<catalog>`와 카탈로그 이름이 같아야 한다 |
| `Bak` | 백업 테이블명. 기본 `{Table}_bak_<날짜>` |
| `OracleUrl` / `OracleUser` / `OraclePassword` | 1회성이라 하드코딩. **커밋하지 말 것** (`git update-index --skip-worktree` 또는 실행 후 되돌리기) |
| `OracleQuery` | `(SELECT key1, key2, tmp_id FROM SCHEMA.TABLE) t` 형태. 키 타입을 Iceberg 쪽과 맞춘다 |
| `JoinKeys` | Iceberg 컬럼 → `ora` 컬럼 쌍. 조인 조건과 검증 쿼리를 여기서 만든다 |
| `CreateDdl` | 기존 `SHOW CREATE TABLE` 출력을 붙여 넣고 `tmp_id STRING NOT NULL`을 원하는 위치에 끼운다. 파티션·`TBLPROPERTIES` 그대로 복사 |
| `postCreateDdls` | Sort Order는 CREATE에 못 쓰므로 `ALTER TABLE ... WRITE ORDERED BY ...`로 |
| `insertSelectSql` | 컬럼 순서를 `CreateDdl`과 동일하게. `tmp_id` 자리에 `COALESCE(CAST(o.tmp_id AS STRING), '')` |

Oracle이 반환하는 컬럼명은 대문자(`KEY1`)지만 Spark SQL은 기본 대소문자 무시라 소문자로 참조해도 된다. Oracle `NUMBER`는 `decimal`로 오며 Iceberg `int`와 암묵 CAST로 조인된다 — `string` vs 숫자처럼 아예 다른 계열이면 조인이 조용히 비어 unmatched로만 드러나므로, `check` 로그의 unmatched 비율이 예상과 다르면 타입부터 본다.

## 5. 실행 절차

```bash
# 0. Airflow 쓰기 DAG(append) 일시 중지. Compaction/maintenance DAG도 대상 테이블 건은 멈춘다

# 1. 빌드 (JDK 17/21, sbt 1.10.7)
cd pipeline/recreate-table
sbt assembly                       # → target/scala-2.12/recreate-table-assembly.jar

# 2. jar 배치 — 매니페스트 mainApplicationFile 과 일치시킨다
mc cp target/scala-2.12/recreate-table-assembly.jar minio/<bucket>/jars/

# 3. check — 아무것도 쓰지 않는다
kubectl apply -f k8s/recreate-table-sparkapp.yaml          # arguments: ["check"]
kubectl logs -f recreate-table-tmp-id-driver -n <ns> | grep RecreateTable
#   확인: [3] unmatched 건수와 비율이 납득 가능한가 / [검증] 타입 대조에 ⚠ 가 없는가 / SHOW CREATE TABLE 이 CreateDdl 과 맞는가
kubectl delete -f k8s/recreate-table-sparkapp.yaml

# 4. run
#    arguments 를 ["run"] 으로 바꿔서
kubectl apply -f k8s/recreate-table-sparkapp.yaml
kubectl logs -f recreate-table-tmp-id-driver -n <ns> | grep RecreateTable
#   기대 마지막 줄: [완료] ... 재생성. 건수 N, tmp_id='' M 건

# 5. Trino 확인 (HMS 경유라 별도 조치 없음)
#    SHOW CREATE TABLE ...;  → tmp_id 위치·NOT NULL
#    SELECT count(*) FROM ... ;  SELECT count(*) FROM ... WHERE tmp_id = '';

# 6. Airflow 재개

# 7. 며칠 뒤 백업 정리 — PURGE 없이 DROP 하면 데이터 파일이 남는다
#    DROP TABLE <catalog>.<db>.<table>_bak_<날짜> PURGE;
```

## 6. 실패 시

| 어디서 | 상태 | 조치 |
|--------|------|------|
| 0~3단계 | 아무것도 안 바뀜 (백업만 생겼을 수 있음) | 원인 수정 → 백업 `DROP ... PURGE` → 다시 `run` |
| 4단계 CREATE / `postCreateDdls` | 원본 없음, 백업 있음, 새 테이블 없거나 부분 생성 | 부분 생성분 `DROP ... PURGE` → DDL 수정 → **`resume`** |
| 5단계 INSERT | 새 테이블이 비어 있음 (Iceberg 단일 커밋) | 새 테이블 `DROP ... PURGE` → 원인 수정 → **`resume`** |
| 6단계 검증 | 새 테이블에 데이터 있음 | 건수 차이의 원인 확인. 재적재하려면 새 테이블 `DROP ... PURGE` → `resume` |

원래 스키마로 되돌리려면(포기 시): `CreateDdl`에서 `tmp_id` 줄을 빼고 `insertSelectSql`에서도 빼서 `resume`. 백업은 파티션·Sort Order가 없는 CTAS 결과이므로 백업을 그대로 rename 해서 쓰지 않는다.

## 7. 주의

- **`restartPolicy: Never`** — DROP 이후 실패한 Job을 operator가 재시도하면 안 된다
- 새 테이블은 snapshot 이력·UUID가 초기화된다. 재처리 DAG의 `batch_id` 영수증(`.snapshots` summary)도 사라지므로, 재생성 직전 구간에 FAILURE 재적재 대기 건이 있으면 재처리 DAG 쪽 확인이 필요하다
- `DROP TABLE ... PURGE`는 `gc.enabled=false`면 거부된다 (0단계에서 검사)
- JDBC 읽기는 executor에서 실행된다 — Oracle 방화벽은 driver·executor 양쪽에 열려 있어야 한다
- 백업 CTAS는 파티션·Sort Order가 없는 평면 테이블이다. INSERT 시 새 테이블의 `write.distribution-mode=range` + Sort Order에 따라 다시 정렬·분배되므로 무관하다
- 매니페스트는 `io-impl`을 지정하지 않아 append Job과 같은 S3A 경로로 쓴다. S3FileIO로 쓰려면 `iceberg-aws-bundle`이 이미지에 있어야 한다 (`s3fileio-migration-guide.md` §5)
