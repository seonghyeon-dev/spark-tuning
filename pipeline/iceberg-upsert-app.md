# 신규 테이블 upsert 앱 (Spark append Job 변형)

| 항목 | 내용 |
|------|------|
| 대상 | 테이블1 (키 `tmp_id1`), 테이블2 (키 `tmp_id1`, `tmp_id2`) |
| 파티션 | 두 테이블 동일: `days(ts)`, `tmp_id1`(identity) |
| 보관 | 3개월 |
| 환경 | 운영 Spark 3.5.8 / Scala 2.12 / Iceberg 1.10.1. 목표 Spark 4.1.1 / Scala 2.13 / Iceberg 1.11.0 |
| 범위 | Spark 앱 코드만. Airflow DAG·executor 수 공식은 다루지 않는다 |
| 검증 | 로컬에서 3개 조합 컴파일·실행 (§6) |

## 1. 결론

- 기존 append 앱에서 바뀌는 곳은 2곳이다. 키별 최신 row 선택(3단계)이 추가되고, `append()`가 MERGE(5단계)로 바뀐다. avro read(1·2단계)와 컬럼명 변환(4단계)은 그대로 쓴다.
- 코드는 2벌이다. Spark 3.5.8은 SQL `MERGE INTO`(§3.1), Spark 4.x는 `Dataset.mergeInto` API(§3.2)를 쓴다. 3.5.8에는 MERGE용 Scala API가 없다.
- SQL 버전은 Spark 4.1.1에서도 그대로 동작한다. 업그레이드 때 코드를 바꾸지 않아도 되고, API 버전으로 바꾸면 SQL 문자열과 escape 처리가 없어진다.
- MERGE에 `tmp_id1 IN (이번 batch 값)` 조건을 넣는다. 넣지 않으면 `tmp_id1`이 파티션 컬럼이어도 대상 테이블 전체를 읽는다. Spark 3.5.8과 4.1.1 모두 같았다(로컬 실측 21개 → 1개).
- `s.tmp_key > t.tmp_key`일 때만 update한다. 재처리 DAG이 다음날 올리는 과거 데이터가 최신 row를 덮어쓰지 않고, 같은 batch를 다시 실행해도 결과가 바뀌지 않는다.
- 재처리 DAG이 쓰는 batch_id 영수증은 `CommitMetadata.withCommitProperties`로 snapshot summary에 남긴다. 두 버전 모두 같다.
- 테이블은 `write.merge.mode=merge-on-read`로 만든다.

## 2. 처리 흐름

| 단계 | 내용 | 기존 대비 |
|------|------|-----------|
| 1 | S3의 avro 경로 목록 파일 read | 동일 |
| 2 | 목록의 avro 파일 read → DataFrame | 동일 |
| 4 | 컬럼명 변환 | 동일 (3단계 앞으로 이동) |
| 3 | 키별로 `tmp_key`가 가장 큰 row 1개만 남김 | 신규 |
| 5 | Iceberg MERGE (upsert) | `append()` 대체 |

컬럼명 변환을 중복 제거 앞으로 옮겼다. 중복 제거는 컬럼 이름과 무관해서 결과가 같고, 이렇게 하면 키 컬럼 이름을 변환 후 이름 하나로만 관리한다. 원래 순서를 지키려면 `latestByKey`에는 원천 이름을, `upsert`의 `UpsertSpec`에는 변환 후 이름을 넣는다.

## 3. 코드

두 버전 공통:

- `<테이블1>`, `<테이블2>`는 앱 인자 `tableName`으로 넘어오는 실제 이름으로 바꾼다.
- `run`의 `???` 두 줄은 기존 코드 자리다.
- `UpsertSpec`, `latestByKey`, `run`은 두 버전이 같고 `upsert`만 다르다.

### 3.1 Spark 3.5.8 (운영): SQL `MERGE INTO`

```scala
import org.apache.iceberg.spark.CommitMetadata
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object UpsertJob {

  /** 테이블별 upsert 키. 컬럼명은 변환 후 이름. pruneCol = 키 중 파티션 컬럼 */
  case class UpsertSpec(keyCols: Seq[String], orderCol: String, pruneCol: String)

  val upsertSpecs: Map[String, UpsertSpec] = Map(
    "<테이블1>" -> UpsertSpec(Seq("tmp_id1"), "tmp_key", "tmp_id1"),
    "<테이블2>" -> UpsertSpec(Seq("tmp_id1", "tmp_id2"), "tmp_key", "tmp_id1")
  )

  /** 키별로 orderCol 값이 가장 큰 row 1개만 남긴다. 동률이면 아무 row나 1개 */
  def latestByKey(df: DataFrame, keyCols: Seq[String], orderCol: String): DataFrame = {
    val w = Window.partitionBy(keyCols.map(col): _*).orderBy(col(orderCol).desc)
    df.withColumn("_rn", row_number().over(w))
      .where(col("_rn") === 1)
      .drop("_rn")
  }

  /** MERGE INTO로 upsert한다. 커밋 1회 = snapshot 1개, summary에 batch_id 기록 */
  def upsert(spark: SparkSession, src: DataFrame, tableName: String,
             spec: UpsertSpec, batchId: String): Unit = {
    // 키 값 수집과 MERGE가 avro를 두 번 읽지 않도록 캐시한다
    src.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      // 대상 테이블에서 읽을 파티션을 이번 batch의 pruneCol 값으로 한정한다
      val pruneVals = src.select(spec.pruneCol).distinct().collect().map(_.getString(0))
      if (pruneVals.isEmpty) return

      val inList = pruneVals
        .map(v => "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'")
        .mkString(", ")

      val view = "upsert_src"
      src.createOrReplaceTempView(view)

      val on = (spec.keyCols.map(k => s"t.$k = s.$k") :+
        s"t.${spec.pruneCol} IN ($inList)").mkString(" AND ")
      val mergeSql =
        s"""MERGE INTO $tableName t
           |USING $view s
           |ON $on
           |WHEN MATCHED AND s.${spec.orderCol} > t.${spec.orderCol} THEN UPDATE SET *
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin

      CommitMetadata.withCommitProperties(
        java.util.Collections.singletonMap("batch_id", batchId),
        new java.util.concurrent.Callable[Unit] { def call(): Unit = spark.sql(mergeSql) },
        classOf[RuntimeException])
    } finally {
      src.unpersist()
    }
  }

  /** 메인 흐름. 1·2·4는 기존 코드 그대로 */
  def run(spark: SparkSession, tableName: String, inputFileName: String, batchId: String): Unit = {
    val spec = upsertSpecs(tableName)

    val raw: DataFrame = ???      // 1, 2. 목록 파일 → avro read (기존과 동일)
    val renamed: DataFrame = ???  // 4. 컬럼명 변환 (기존 함수)

    val latest = latestByKey(renamed, spec.keyCols, spec.orderCol)   // 3
    upsert(spark, latest, tableName, spec, batchId)                  // 5
  }
}
```

### 3.2 Spark 4.x (업그레이드 후): `mergeInto` API

```scala
import org.apache.iceberg.spark.CommitMetadata
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object UpsertJob {

  /** 테이블별 upsert 키. 컬럼명은 변환 후 이름. pruneCol = 키 중 파티션 컬럼 */
  case class UpsertSpec(keyCols: Seq[String], orderCol: String, pruneCol: String)

  val upsertSpecs: Map[String, UpsertSpec] = Map(
    "<테이블1>" -> UpsertSpec(Seq("tmp_id1"), "tmp_key", "tmp_id1"),
    "<테이블2>" -> UpsertSpec(Seq("tmp_id1", "tmp_id2"), "tmp_key", "tmp_id1")
  )

  /** 키별로 orderCol 값이 가장 큰 row 1개만 남긴다. 동률이면 아무 row나 1개 */
  def latestByKey(df: DataFrame, keyCols: Seq[String], orderCol: String): DataFrame = {
    val w = Window.partitionBy(keyCols.map(col): _*).orderBy(col(orderCol).desc)
    df.withColumn("_rn", row_number().over(w))
      .where(col("_rn") === 1)
      .drop("_rn")
  }

  /** mergeInto API로 upsert한다 (Spark 4.0 이상). 커밋 1회 = snapshot 1개, summary에 batch_id 기록 */
  def upsert(spark: SparkSession, src: DataFrame, tableName: String,
             spec: UpsertSpec, batchId: String): Unit = {
    // 키 값 수집과 MERGE가 avro를 두 번 읽지 않도록 캐시한다
    src.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      // 대상 테이블에서 읽을 파티션을 이번 batch의 pruneCol 값으로 한정한다
      val pruneVals = src.select(spec.pruneCol).distinct().collect().map(_.getString(0))
      if (pruneVals.isEmpty) return

      // 대상 테이블 컬럼은 테이블 이름으로, source 컬럼은 별칭 s로 가리킨다
      def t(c: String): Column = col(s"$tableName.$c")
      def s(c: String): Column = col(s"s.$c")

      val on = spec.keyCols.map(k => t(k) === s(k)).reduce(_ && _) &&
        t(spec.pruneCol).isin(pruneVals.toSeq: _*)

      CommitMetadata.withCommitProperties(
        java.util.Collections.singletonMap("batch_id", batchId),
        new java.util.concurrent.Callable[Unit] {
          def call(): Unit = src.as("s")
            .mergeInto(tableName, on)
            .whenMatched(s(spec.orderCol) > t(spec.orderCol)).updateAll()
            .whenNotMatched().insertAll()
            .merge()
        },
        classOf[RuntimeException])
    } finally {
      src.unpersist()
    }
  }

  /** 메인 흐름. 1·2·4는 기존 코드 그대로 */
  def run(spark: SparkSession, tableName: String, inputFileName: String, batchId: String): Unit = {
    val spec = upsertSpecs(tableName)

    val raw: DataFrame = ???      // 1, 2. 목록 파일 → avro read (기존과 동일)
    val renamed: DataFrame = ???  // 4. 컬럼명 변환 (기존 함수)

    val latest = latestByKey(renamed, spec.keyCols, spec.orderCol)   // 3
    upsert(spark, latest, tableName, spec, batchId)                  // 5
  }
}
```

## 4. 코드 설명

### 4.1 키별 최신 row 선택 (`latestByKey`)

키가 같은 row끼리 묶어 `tmp_key` 내림차순으로 번호를 매기고 1번만 남긴다.

| 입력 row | `tmp_id1` | `tmp_key` | 번호 | 결과 |
|---------|-----------|-----------|------|------|
| 1 | X | `20261014225900000` | 2 | 버림 |
| 2 | X | `20261014235900000` | 1 | 채택 |

**`tmp_key`를 문자열로 비교해도 시간 순서와 같다.** `tmp_key`는 `YYYYMMDDHHmmSSSSS` 17자리 고정 길이이고, 큰 단위(연)부터 작은 단위(밀리초) 순으로 놓여 있다. 문자열 비교는 앞자리부터 한 글자씩 비교하므로 처음 다른 자리가 곧 시간의 대소다.

| 비교 | 처음 다른 자리 | 결과 |
|------|----------------|------|
| `20261014235900000` vs `20261015000100000` | 8번째 자리 (일 `4` < `5`) | 10-15 00:01이 최신 |
| `20261015000100000` vs `20261015000100001` | 17번째 자리 (밀리초 `0` < `1`) | 1밀리초 뒤가 최신 |

자리수가 17자리가 아닌 값이 섞이면 이 성질이 깨진다(예: `202610150001` 12자리). 형식은 고정이라 변환 없이 문자열로 비교한다.

- `tmp_key`까지 같은 row가 오면 둘 중 하나를 남긴다(사용자 확인: 발생하지 않고, 발생해도 아무거나 무방).
- 이 단계는 생략할 수 없다. 같은 키가 source에 2개 있으면 MERGE가 에러로 실패한다.

### 4.2 MERGE 조건

```sql
MERGE INTO <테이블> t
USING upsert_src s
ON t.tmp_id1 = s.tmp_id1 AND t.tmp_id1 IN ('X', ...)
WHEN MATCHED AND s.tmp_key > t.tmp_key THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Spark 4 API 버전도 같은 조건을 `Column`으로 만든다 (§4.4).

**`s.tmp_key > t.tmp_key`: 과거 데이터가 최신을 덮어쓰지 않게 한다**

1. 10-14 23:59 버전 X가 batch 실패로 FAILURE가 된다.
2. 10-15 00:01 버전 X가 다음 batch에서 적재된다. 테이블에는 00:01 버전이 있다.
3. 다음날 04:00 재처리 DAG이 23:59 버전을 다시 올린다.

조건이 없으면 3에서 23:59 버전이 00:01 버전을 덮어쓴다. 조건이 있으면 `20261014235900000 > 20261015000100000`이 거짓이라 무시된다. 같은 batch를 두 번 실행해도 두 번째는 `tmp_key`가 같아 update가 0건이므로 Airflow retry로 인한 중복도 생기지 않는다.

**`t.tmp_id1 IN (...)`: 대상 테이블 읽기 범위를 줄인다**

MERGE는 source와 대상 테이블을 join한다. ON이 `t.tmp_id1 = s.tmp_id1`뿐이면 Spark가 대상 파티션을 미리 고르지 못해 테이블 전체를 읽는다. 앱이 이번 batch의 `tmp_id1` 값을 먼저 모아 `IN` 조건으로 넣으면 Iceberg가 그 값의 파티션만 연다.

| 테이블1, 대상 데이터 파일 21개 | Spark 3.5.8 | Spark 4.1.1 |
|------|------|------|
| `t.tmp_id1 = s.tmp_id1`만 | 21개 읽음 | 21개 읽음 |
| `IN` 조건 추가 | 1개 읽음 | 1개 읽음 |

- 하한 조건은 두지 않는다. 같은 키면 보관 중인 전체 데이터에서 최신 `tmp_key`로 upsert한다(사용자 확인). 그래서 `tmp_id1` 값 하나당 읽는 범위는 그 값의 `days(ts)` 파티션 전체이고, 보관이 3개월이므로 최대 약 90일치다.
- 값 수집과 MERGE가 avro를 두 번 읽지 않도록 source를 `persist`한다. 캐시 크기는 중복 제거 후 batch 데이터 크기다.
- SQL 버전은 값에 `'`나 `\`가 있어도 깨지지 않도록 escape한다. API 버전은 값이 SQL 문자열을 거치지 않아 escape가 필요 없다. 두 버전 모두 `Q'1\x` 키로 update가 정상 동작함을 확인했다.
- batch가 0 row면 MERGE를 하지 않는다. snapshot이 생기지 않으므로 재처리 DAG이 이 batch를 다시 올리는데, 0 row라 결과는 같다.

**`ts`는 ON에 넣지 않는다**

`tmp_key`는 `ts`와 같은 시간 값이므로 같은 키의 새 버전은 `ts`도 바뀐다. 10-14 23:59 버전과 10-15 00:01 버전은 `days(ts)` 파티션이 다르다. ON에 `ts` 조건을 넣으면 10-14 row를 찾지 못해 10-15 row가 새로 insert되고 X가 2건이 된다. 지금 코드는 10-14 row를 지우고 10-15 row를 10-15 파티션에 넣는다 (§6.2 b2).

### 4.3 batch_id 영수증

기존 앱은 `.option("snapshot-property.batch_id", batchId)`로 영수증을 남긴다. MERGE에는 이 옵션을 붙일 곳이 없다(SQL 버전은 문장, API 버전은 `MergeIntoWriter`에 option이 없음). `CommitMetadata.withCommitProperties`로 MERGE를 감싸면 그 안의 커밋 snapshot summary에 `batch_id`가 남는다. 재처리 DAG의 영수증 확인 쿼리(`element_at(summary, 'batch_id')`)는 바꿀 필요가 없다.

update·insert가 0건인 MERGE(같은 batch 재실행)도 snapshot이 생기고 `batch_id`가 남는다 (§6.2 b3).

### 4.4 SQL 버전과 API 버전의 차이

| 항목 | SQL (§3.1) | API (§3.2) |
|------|-----------|-----------|
| 사용 가능 버전 | Spark 3.5.8, 4.1.1 (둘 다 검증) | Spark 4.0 이상 (4.1.1 검증) |
| source 전달 | temp view `upsert_src` | `src.as("s")` |
| 대상 컬럼 참조 | 별칭 `t.<컬럼>` | 테이블 이름 `<tableName>.<컬럼>` (API에 대상 별칭이 없음) |
| `IN` 값 | SQL 문자열에 넣으므로 escape 필요 | `isin(...)`으로 값 전달, escape 불필요 |
| MERGE 실행 | `spark.sql(mergeSql)` | `.mergeInto(...).whenMatched(...).updateAll().whenNotMatched().insertAll().merge()` |

API 버전은 대상 컬럼을 테이블 이름으로 가리킨다. `tableName`이 `db.table`이든 `catalog.db.table`이든 그대로 쓰면 된다. source 별칭 `s`가 테이블 이름과 같으면 안 된다.

## 5. 테이블 설정

```sql
CREATE TABLE <테이블> (
  tmp_id1 string NOT NULL,
  tmp_id2 string NOT NULL,
  tmp_key string,
  ts      timestamp_ntz,
  ...
)
USING iceberg
PARTITIONED BY (days(ts), tmp_id1)
TBLPROPERTIES (
  'format-version'   = '2',
  'write.merge.mode' = 'merge-on-read'
);
```

- `NOT NULL`: `tmp_id1`, `tmp_id2`는 필수 값이다. NULL 키는 ON의 `=`가 항상 거짓이라 batch마다 새 row로 쌓인다. 테이블에서 막아 두면 쓰기 시점에 실패한다.
- `merge-on-read`: 기본값 copy-on-write는 update가 1건이어도 그 row가 든 파일 전체를 다시 쓴다. 로컬에서는 같은 batch를 재실행해 변경이 0건일 때도 파일을 다시 썼다. merge-on-read는 기존 파일은 두고 바뀐 row만 position delete 파일로 표시한다.

## 6. 로컬 검증

### 6.1 환경

| 조합 | Spark / Scala | Iceberg | 코드 |
|------|---------------|---------|------|
| A | 3.5.8 / 2.12.18 | `iceberg-spark-runtime-3.5_2.12-1.10.1` | §3.1 SQL |
| B | 4.1.1 / 2.13.17 | `iceberg-spark-runtime-4.1_2.13-1.11.0` | §3.1 SQL |
| C | 4.1.1 / 2.13.17 | `iceberg-spark-runtime-4.1_2.13-1.11.0` | §3.2 API |

공통: `local[2]`, Hadoop 카탈로그(운영은 HMS), §5 테이블 설정. §3 코드 블록을 그대로 컴파일해 실행했다. 읽은 파일 수는 Iceberg scan metric `resultDataFiles`로 쟀다.

### 6.2 결과 (A·B·C 모두 동일)

테이블1 (키 `tmp_id1`):

| batch | 입력 | 결과 |
|-------|------|------|
| b0 | 배경 데이터 20개 키 × 3일 | 키별 최신 1건씩 20건 insert |
| b1 | X 3건 (10-14 22:59 1건, 23:59 2건) | 23:59 버전 1건만 insert |
| b2 | X 10-15 00:01 | 10-14 row 삭제 표시(position delete 1건), 00:01 row를 10-15 파티션에 insert. X는 1건. 대상 1개 파일만 읽음 |
| b3 | b2 재실행 | 변경 0건, snapshot summary에 `batch_id=b3` 기록 |
| bq, bq2 | `Q'1\x` 키 10-14 23:59 → 10-15 00:01 | update 정상, 중복 없음 |

테이블2 (키 `tmp_id1`, `tmp_id2`)도 같은 시나리오에서 결과가 같았다. 추가로 X/2의 하루 전 버전(`stale`)이 기존 row를 덮어쓰지 않았다. `IN` 조건이 없을 때 대상 데이터 파일을 전부(61개 중 61개) 읽었고, 있을 때 1개만 읽었다.

## 7. 미확인·후속

- **executor 수 공식**: MERGE는 avro 외에 대상 테이블도 읽는다. 기존 공식(avro 크기 기준)이 맞는지 추후 검토(사용자 2026-10-14).
- **동시 MERGE 충돌 (미실측)**: append는 동시 커밋이 안전하지만 MERGE는 Iceberg가 충돌을 검사한다. 04:00 재처리와 append가 같은 테이블에 겹치면 한쪽이 `ValidationException`으로 실패할 수 있다. 데이터 유실은 아니고 FAILURE로 남아 다음날 회수된다.
- **delete 파일 정리**: merge-on-read는 position delete 파일을 남긴다. Compaction에서 delete 파일 정리를 함께 다뤄야 한다.
- **운영 카탈로그(HMS)·S3FileIO 환경 실행**: 로컬은 Hadoop 카탈로그다.
