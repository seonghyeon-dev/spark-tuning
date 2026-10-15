# 신규 테이블 upsert 앱 (Spark append Job 변형)

| 항목 | 내용 |
|------|------|
| 대상 | 테이블1 (키 `tmp_id1`, `tmp_id2` 컬럼 없음), 테이블2 (키 `tmp_id1`, `tmp_id2`) |
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
- 최신 판단은 `ts`로 한다. `tmp_key`와 같은 시각이라 결과가 같고, 성능은 같거나 낫다(§4.5).
- update는 키를 뺀 나머지 컬럼 전부를 source 값으로 바꾼다(테이블1은 `tmp_id1`, 테이블2는 `tmp_id1`·`tmp_id2` 제외). `UPDATE SET *`로 충족한다. match된 row는 키 값이 source와 같아서 키 컬럼은 바뀌지 않는다(§4.2).
- `s.ts > t.ts`일 때만 update한다. 재처리 DAG이 다음날 올리는 과거 데이터가 최신 row를 덮어쓰지 않고, 같은 batch를 다시 실행해도 결과가 바뀌지 않는다.
- 재처리 DAG이 쓰는 batch_id 영수증은 `CommitMetadata.withCommitProperties`로 snapshot summary에 남긴다. 두 버전 모두 같다.
- 테이블은 `write.merge.mode=merge-on-read`로 만든다.

## 2. 처리 흐름

| 단계 | 내용 | 기존 대비 |
|------|------|-----------|
| 1 | S3의 avro 경로 목록 파일 read | 동일 |
| 2 | 목록의 avro 파일 read → DataFrame | 동일 |
| 4 | 컬럼명 변환 | 동일 (3단계 앞으로 이동) |
| 3 | 키별로 `ts`가 가장 큰 row 1개만 남김 | 신규 |
| 5 | Iceberg MERGE (upsert) | `append()` 대체 |

컬럼명 변환을 중복 제거 앞으로 옮겼다. 중복 제거는 컬럼 이름과 무관해서 결과가 같고, 이렇게 하면 키 컬럼 이름을 변환 후 이름 하나로만 관리한다. 원래 순서를 지키려면 `latestByKey`에는 원천 이름을, `upsert`의 `UpsertSpec`에는 변환 후 이름을 넣는다.

## 3. 코드

두 버전 공통:

- `<테이블1>`, `<테이블2>`는 앱 인자 `tableName`으로 넘어오는 실제 이름으로 바꾼다.
- `renameColumns`의 `???`는 기존 컬럼명 변환 함수 호출로 바꾼다. `readAvro`는 목록 파일을 한 줄에 경로 1개로 가정한 구현이다. 기존 구현이 있으면 그것으로 바꾼다.
- `upsert`만 두 버전이 다르고 나머지는 같다.

| 함수 | 역할 |
|------|------|
| `main` | 앱 진입점. 인자(`tableName`, `inputFileName`, `batchId`)를 받아 `run` 호출 |
| `run` | 1~5단계 전체. `prepare` 결과를 `upsert`에 넘긴다 |
| `prepare` | 1~4단계. Iceberg 적재 직전의 source DataFrame을 만든다. 로컬 테스트 대상(§8) |
| `readAvro` / `renameColumns` / `latestByKey` | 1·2단계 / 4단계 / 3단계 |
| `upsert` | 5단계 MERGE |

Spark 앱은 `mainClass`로 지정한 object의 `main`에서 시작한다. 기존 앱의 `main`에 인자 파싱·SparkSession 설정이 있으면 그대로 두고 마지막에 `run`만 호출해도 된다. `run`이 SparkSession을 인자로 받는 것은 테스트에서 로컬 SparkSession을 넘기기 위해서다.

### 3.1 Spark 3.5.8 (운영): SQL `MERGE INTO`

```scala
import org.apache.iceberg.spark.CommitMetadata
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object UpsertJob {

  /** 테이블별 upsert 키. 컬럼명은 변환 후 이름. orderCol = 최신 판단 컬럼, pruneCol = 키 중 파티션 컬럼 */
  case class UpsertSpec(keyCols: Seq[String], orderCol: String, pruneCol: String)

  val upsertSpecs: Map[String, UpsertSpec] = Map(
    "<테이블1>" -> UpsertSpec(Seq("tmp_id1"), "ts", "tmp_id1"),
    "<테이블2>" -> UpsertSpec(Seq("tmp_id1", "tmp_id2"), "ts", "tmp_id1")
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
      // update는 키를 뺀 나머지 컬럼 전부를 source 값으로 바꾼다.
      // 전 컬럼 갱신(*)을 써도 키 컬럼은 ON 조건상 source와 값이 같아 바뀌지 않는다
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

  /** 1·2. 목록 파일(한 줄에 avro 경로 1개) → avro read. 기존 구현이 있으면 그것으로 교체 */
  def readAvro(spark: SparkSession, inputFileName: String): DataFrame = {
    val paths = spark.read.textFile(inputFileName).collect().map(_.trim).filter(_.nonEmpty)
    spark.read.format("avro").load(paths.toSeq: _*)
  }

  /** 4. 컬럼명 변환. 기존 함수 호출로 교체 */
  def renameColumns(df: DataFrame): DataFrame = ???

  /** 1~4단계. Iceberg 적재 직전의 source DataFrame을 만든다 (로컬 테스트 대상) */
  def prepare(spark: SparkSession, inputFileName: String, spec: UpsertSpec): DataFrame = {
    val raw = readAvro(spark, inputFileName)              // 1, 2
    val renamed = renameColumns(raw)                       // 4
    latestByKey(renamed, spec.keyCols, spec.orderCol)      // 3
  }

  /** 1~5단계 */
  def run(spark: SparkSession, tableName: String, inputFileName: String, batchId: String): Unit = {
    val spec = upsertSpecs(tableName)
    upsert(spark, prepare(spark, inputFileName, spec), tableName, spec, batchId)
  }

  /** 앱 진입점. 인자 파싱·SparkSession 생성은 기존 main을 그대로 쓰고 마지막에 run만 호출해도 된다 */
  def main(args: Array[String]): Unit = {
    val Array(tableName, inputFileName, batchId) = args
    val spark = SparkSession.builder().getOrCreate()
    try run(spark, tableName, inputFileName, batchId)
    finally spark.stop()
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

  /** 테이블별 upsert 키. 컬럼명은 변환 후 이름. orderCol = 최신 판단 컬럼, pruneCol = 키 중 파티션 컬럼 */
  case class UpsertSpec(keyCols: Seq[String], orderCol: String, pruneCol: String)

  val upsertSpecs: Map[String, UpsertSpec] = Map(
    "<테이블1>" -> UpsertSpec(Seq("tmp_id1"), "ts", "tmp_id1"),
    "<테이블2>" -> UpsertSpec(Seq("tmp_id1", "tmp_id2"), "ts", "tmp_id1")
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

      // update는 키를 뺀 나머지 컬럼 전부를 source 값으로 바꾼다.
      // 전 컬럼 갱신(*)을 써도 키 컬럼은 ON 조건상 source와 값이 같아 바뀌지 않는다
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

  /** 1·2. 목록 파일(한 줄에 avro 경로 1개) → avro read. 기존 구현이 있으면 그것으로 교체 */
  def readAvro(spark: SparkSession, inputFileName: String): DataFrame = {
    val paths = spark.read.textFile(inputFileName).collect().map(_.trim).filter(_.nonEmpty)
    spark.read.format("avro").load(paths.toSeq: _*)
  }

  /** 4. 컬럼명 변환. 기존 함수 호출로 교체 */
  def renameColumns(df: DataFrame): DataFrame = ???

  /** 1~4단계. Iceberg 적재 직전의 source DataFrame을 만든다 (로컬 테스트 대상) */
  def prepare(spark: SparkSession, inputFileName: String, spec: UpsertSpec): DataFrame = {
    val raw = readAvro(spark, inputFileName)              // 1, 2
    val renamed = renameColumns(raw)                       // 4
    latestByKey(renamed, spec.keyCols, spec.orderCol)      // 3
  }

  /** 1~5단계 */
  def run(spark: SparkSession, tableName: String, inputFileName: String, batchId: String): Unit = {
    val spec = upsertSpecs(tableName)
    upsert(spark, prepare(spark, inputFileName, spec), tableName, spec, batchId)
  }

  /** 앱 진입점. 인자 파싱·SparkSession 생성은 기존 main을 그대로 쓰고 마지막에 run만 호출해도 된다 */
  def main(args: Array[String]): Unit = {
    val Array(tableName, inputFileName, batchId) = args
    val spark = SparkSession.builder().getOrCreate()
    try run(spark, tableName, inputFileName, batchId)
    finally spark.stop()
  }
}
```

## 4. 코드 설명

### 4.1 키별 최신 row 선택 (`latestByKey`)

키가 같은 row끼리 묶어 `ts` 내림차순으로 번호를 매기고 1번만 남긴다. 번호는 임시 컬럼 `_rn`(row number)에 담고, 1번만 거른 뒤 `drop("_rn")`으로 지운다. 결과 컬럼은 입력과 같다. 이름 앞의 `_`는 실제 컬럼과 겹치지 않게 붙였다.

| 입력 row | `tmp_id1` | `ts` | 번호 | 결과 |
|---------|-----------|------|------|------|
| 1 | X | `2026-10-14 22:59:00.000` | 2 | 버림 |
| 2 | X | `2026-10-14 23:59:00.000` | 1 | 채택 |

**최신 판단에 `tmp_key` 대신 `ts`를 쓴다.** `tmp_key`(`YYYYMMDDHHmmSSSSS` 문자열)와 `ts`(timestamp_ntz)는 타입만 다르고 같은 시각을 가리킨다(사용자 확인 2026-10-14). 어느 쪽으로 판단해도 고르는 row가 같다. `ts`를 쓰면 `tmp_key`가 17자리 고정 형식이어야 한다는 전제가 필요 없고, 성능은 같거나 낫다(§4.5).

- `ts`까지 같은 row가 오면 둘 중 하나를 남긴다(사용자 확인: 발생하지 않고, 발생해도 아무거나 무방).
- 이 단계는 생략할 수 없다. 같은 키가 source에 2개 있으면 MERGE가 에러로 실패한다.

### 4.2 MERGE 조건

```sql
MERGE INTO <테이블> t
USING upsert_src s
ON t.tmp_id1 = s.tmp_id1 AND t.tmp_id1 IN ('X', ...)
WHEN MATCHED AND s.ts > t.ts THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Spark 4 API 버전도 같은 조건을 `Column`으로 만든다 (§4.4).

**`s.ts > t.ts`: 과거 데이터가 최신을 덮어쓰지 않게 한다**

1. 10-14 23:59 버전 X가 batch 실패로 FAILURE가 된다.
2. 10-15 00:01 버전 X가 다음 batch에서 적재된다. 테이블에는 00:01 버전이 있다.
3. 다음날 04:00 재처리 DAG이 23:59 버전을 다시 올린다.

조건이 없으면 3에서 23:59 버전이 00:01 버전을 덮어쓴다. 조건이 있으면 `2026-10-14 23:59 > 2026-10-15 00:01`이 거짓이라 무시된다. 같은 batch를 두 번 실행해도 두 번째는 `ts`가 같아 update가 0건이므로 Airflow retry로 인한 중복도 생기지 않는다.

**update 컬럼: 키를 뺀 나머지 전부 (`UPDATE SET *`)**

| 테이블 | 키 (갱신 안 됨) | 갱신 컬럼 |
|--------|----------------|-----------|
| 테이블1 | `tmp_id1` | 나머지 전부 (`tmp_key`, `ts`, ...) |
| 테이블2 | `tmp_id1`, `tmp_id2` | 나머지 전부 (`tmp_key`, `ts`, ...) |

`UPDATE SET *`(API는 `updateAll()`)는 모든 컬럼을 source 값으로 바꾼다. 그래도 키 컬럼은 바뀌지 않는다. ON 조건이 키 컬럼끼리 같음(`t.tmp_id1 = s.tmp_id1`, 테이블2는 `t.tmp_id2 = s.tmp_id2`도)이므로 match된 row는 키 값이 source와 이미 같기 때문이다. 결과적으로 키를 뺀 나머지 컬럼만 바뀐다. 갱신할 컬럼을 하나씩 나열할 필요가 없고, 컬럼이 추가돼도 코드를 고치지 않는다.

예: 테이블1의 Y가 10-14 23:59 / `y-old`로 있고 10-15 00:01 / `y-new`가 들어오면, `tmp_id1`=Y는 그대로이고 `tmp_key`, `ts`, `v`가 새 값으로 바뀐다(§6.2 by1·by2). `ts`가 바뀌므로 row는 10-15 파티션으로 옮겨진다.

**`t.tmp_id1 IN (...)`: 대상 테이블 읽기 범위를 줄인다**

MERGE는 source와 대상 테이블을 join한다. ON이 `t.tmp_id1 = s.tmp_id1`뿐이면 Spark가 대상 파티션을 미리 고르지 못해 테이블 전체를 읽는다. 앱이 이번 batch의 `tmp_id1` 값을 먼저 모아 `IN` 조건으로 넣으면 Iceberg가 그 값의 파티션만 연다.

| 테이블1, 대상 데이터 파일 21개 | Spark 3.5.8 | Spark 4.1.1 |
|------|------|------|
| `t.tmp_id1 = s.tmp_id1`만 | 21개 읽음 | 21개 읽음 |
| `IN` 조건 추가 | 1개 읽음 | 1개 읽음 |

- 하한 조건은 두지 않는다. 같은 키면 보관 중인 전체 데이터에서 최신 `ts`의 row로 upsert한다(사용자 확인). 그래서 `tmp_id1` 값 하나당 읽는 범위는 그 값의 `days(ts)` 파티션 전체이고, 보관이 3개월이므로 최대 약 90일치다.
- 값 수집과 MERGE가 avro를 두 번 읽지 않도록 source를 `persist`한다. 캐시 크기는 중복 제거 후 batch 데이터 크기다. `finally`의 `unpersist()`는 이 캐시를 executor 메모리·디스크에서 지운다. MERGE가 실패해도 지우도록 `finally`에 뒀다.
- SQL 버전은 값에 `'`나 `\`가 있어도 깨지지 않도록 escape한다. API 버전은 값이 SQL 문자열을 거치지 않아 escape가 필요 없다. 두 버전 모두 `Q'1\x` 키로 update가 정상 동작함을 확인했다.
- batch가 0 row면 MERGE를 하지 않는다. snapshot이 생기지 않으므로 재처리 DAG이 이 batch를 다시 올리는데, 0 row라 결과는 같다.

**`ts`는 ON에 넣지 않는다**

`ts`는 최신 판단(`WHEN MATCHED` 조건)에만 쓰고, 같은 row를 찾는 ON 조건에는 넣지 않는다. 같은 키의 새 버전은 `ts`가 바뀐다. 10-14 23:59 버전과 10-15 00:01 버전은 `days(ts)` 파티션이 다르다. ON에 `ts` 조건을 넣으면 10-14 row를 찾지 못해 10-15 row가 새로 insert되고 X가 2건이 된다. 지금 코드는 10-14 row를 지우고 10-15 row를 10-15 파티션에 넣는다 (§6.2 b2).

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

### 4.5 `ts`와 `tmp_key` 비교 (성능)

결과는 같고, 성능은 `ts`가 같거나 낫다. 차이가 나는 곳은 중복 제거 정렬이고, 그것도 batch 안에 같은 키의 row가 많을 때만이다.

| 항목 | `tmp_key` (string 17자) | `ts` (timestamp_ntz) | 차이 |
|------|------------------------|----------------------|------|
| 최신 판단 결과 (500만 row) | | | 차이 0건 |
| 값 1개 비교 | 17바이트 문자열 비교 | 8바이트 정수 비교 | `ts`가 가벼움 |
| 중복 제거 시간, 키당 row 5,000개 | 7.3~8.1초 | 4.8~5.9초 | `ts` 약 30% 빠름 (평균 7.7초 → 5.2초) |
| 중복 제거 시간, 키당 row 2개 | 7.3~7.8초 | 7.5~8.8초 | 차이 없음 (반복 간 편차 범위) |
| 파일 저장 크기 (zstd) | 26.8MB | 26.5MB | 거의 같음 |

측정: 로컬 `local[2]`, Spark 3.5.8 + Iceberg 1.10.1, 500만 row(3일 × `tmp_id1` 20종, `ts`는 하루 안 임의 밀리초). 중복 제거는 warm-up 1회 뒤 3회 반복했다.

**중복 제거: 키당 row 수에 따라 차이가 갈린다**

`latestByKey`는 row를 키로 먼저 정렬하고, 키가 같은 row끼리만 최신 판단 컬럼을 비교한다.

- 키당 row가 2개면 최신 판단 컬럼을 비교할 일이 거의 없다. 그래서 컬럼 타입을 바꿔도 시간이 같았다.
- 키당 row가 5,000개면 정렬 비교 대부분이 최신 판단 컬럼에서 일어난다. 17바이트 문자열 비교가 8바이트 정수 비교로 바뀌어 약 30% 빨라졌다.

실제 5분 batch에서 같은 키가 몇 번 겹치는지는 측정하지 않았다. 겹침이 적으면 차이도 작다.

**MERGE: 대상 테이블에서 읽는 컬럼이 바뀌지만 크기가 거의 같다**

MERGE는 대상 테이블에서 키 컬럼과 조건 컬럼만 읽는다(테이블2 로컬 실행 계획: `tmp_id1`, `tmp_id2`, 조건 컬럼, `_file`, `_pos`). 조건을 `ts`로 바꾸면 `tmp_key` 대신 `ts`를 읽는다. 두 컬럼의 파일 저장 크기는 500만 row에서 26.8MB와 26.5MB로 row당 약 5.3바이트다. 밀리초 단위 값은 압축 후 크기가 비슷해서, 읽는 양에는 차이가 없다.

## 5. 테이블 설정

```sql
CREATE TABLE <테이블> (
  tmp_id1 string NOT NULL,
  tmp_id2 string NOT NULL,   -- 테이블2만
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

- `NOT NULL`: 키 컬럼(`tmp_id1`, 테이블2는 `tmp_id2`도)은 필수 값이다. NULL 키는 ON의 `=`가 항상 거짓이라 batch마다 새 row로 쌓인다. 테이블에서 막아 두면 쓰기 시점에 실패한다.
- `merge-on-read`: 기본값 copy-on-write는 update가 1건이어도 그 row가 든 파일 전체를 다시 쓴다. 로컬에서는 같은 batch를 재실행해 변경이 0건일 때도 파일을 다시 썼다. merge-on-read는 기존 파일은 두고 바뀐 row만 position delete 파일로 표시한다.

## 6. 로컬 검증

### 6.1 환경

| 조합 | Spark / Scala | Iceberg | 코드 |
|------|---------------|---------|------|
| A | 3.5.8 / 2.12.18 | `iceberg-spark-runtime-3.5_2.12-1.10.1` | §3.1 SQL |
| B | 4.1.1 / 2.13.17 | `iceberg-spark-runtime-4.1_2.13-1.11.0` | §3.1 SQL |
| C | 4.1.1 / 2.13.17 | `iceberg-spark-runtime-4.1_2.13-1.11.0` | §3.2 API |

공통: `local[2]`, Hadoop 카탈로그(운영은 HMS), §5 테이블 설정(테이블1은 `tmp_id2` 컬럼 없음). §3 코드 블록을 그대로 컴파일해 실행했다(`renameColumns`만 입력을 그대로 돌려주는 스텁). 읽은 파일 수는 Iceberg scan metric `resultDataFiles`로 쟀다.

### 6.2 결과 (A·B·C 모두 동일)

테이블1 (키 `tmp_id1`):

| batch | 입력 | 결과 |
|-------|------|------|
| b0 | 배경 데이터 20개 키 × 3일 | 키별 최신 1건씩 20건 insert |
| b1 | X 3건 (10-14 22:59 1건, 23:59 2건) | 23:59 버전 1건만 insert |
| b2 | X 10-15 00:01 | 10-14 row 삭제 표시(position delete 1건), 00:01 row를 10-15 파티션에 insert. X는 1건. 대상 1개 파일만 읽음 |
| b3 | b2 재실행 | 변경 0건, snapshot summary에 `batch_id=b3` 기록 |
| by1, by2 | Y: 10-14 23:59 / `y-old` → 10-15 00:01 / `y-new` | 키(`tmp_id1`)를 뺀 `tmp_key`, `ts`, `v`가 새 값으로 바뀜. 10-15 파티션으로 이동 |
| bq, bq2 | `Q'1\x` 키 10-14 23:59 → 10-15 00:01 | update 정상, 중복 없음 |

테이블2 (키 `tmp_id1`, `tmp_id2`)도 같은 시나리오에서 결과가 같았다. by1·by2는 `tmp_id2`를 1과 9로 다르게 넣어, 키가 다른 Y/1, Y/9 2건이 됐다. 추가로 X/2의 하루 전 버전(`stale`)이 기존 row를 덮어쓰지 않았다. `IN` 조건이 없을 때 대상 데이터 파일을 전부(61개 중 61개) 읽었고, 있을 때 1개만 읽었다.

## 7. 미확인·후속

- **executor 수 공식**: MERGE는 avro 외에 대상 테이블도 읽는다. 기존 공식(avro 크기 기준)이 맞는지 추후 검토(사용자 2026-10-14).
- **동시 MERGE 충돌 (미실측)**: append는 동시 커밋이 안전하지만 MERGE는 Iceberg가 충돌을 검사한다. 04:00 재처리와 append가 같은 테이블에 겹치면 한쪽이 `ValidationException`으로 실패할 수 있다. 데이터 유실은 아니고 FAILURE로 남아 다음날 회수된다.
- **delete 파일 정리**: merge-on-read는 position delete 파일을 남긴다. Compaction에서 delete 파일 정리를 함께 다뤄야 한다.
- **운영 카탈로그(HMS)·S3FileIO 환경 실행**: 로컬은 Hadoop 카탈로그다.

## 8. 로컬 테스트 (avro → 중복 제거까지)

Iceberg 적재(5단계) 직전까지, 즉 `prepare`의 결과를 실제 avro 파일로 검증한다. Iceberg·S3 없이 로컬 Spark만으로 돈다.

### 8.1 파일 위치

```
src/
├── main/scala/UpsertJob.scala              # §3 코드
└── test/
    ├── resources/avro/*.avro               # 테스트할 avro 파일 (같은 테이블 것)
    └── scala/UpsertPrepareTest.scala       # 아래 §8.3 코드
```

- avro 파일은 한 테이블 것만 넣고, 테스트 코드의 `tableName`을 그 테이블로 맞춘다.
- 테스트가 avro 경로 목록 파일(운영의 `inputFileName`과 같은 형식)을 임시로 만들어 `prepare`에 넘긴다. 1단계(목록 read)부터 검증된다.
- `renameColumns`가 실제 변환 함수를 호출해야 4단계까지 검증된다.

### 8.2 빌드 설정

ScalaTest와 JVM 옵션이 필요하다. Java 17 이상에서 Spark를 테스트 JVM으로 띄우면 `--add-opens` 없이 `cannot access class sun.nio.ch.DirectBuffer`로 실패한다(로컬에서 확인). `spark-avro`는 `provided`여도 테스트 classpath에 들어간다.

**sbt**

```scala
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test

Test / fork := true
Test / javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)
```

실행: `sbt "testOnly UpsertPrepareTest"`

**Maven**

```xml
<dependency>
  <groupId>org.scalatest</groupId>
  <artifactId>scalatest_2.12</artifactId>
  <version>3.2.19</version>
  <scope>test</scope>
</dependency>

<!-- build/plugins: surefire 대신 scalatest-maven-plugin으로 실행 -->
<plugin>
  <groupId>org.scalatest</groupId>
  <artifactId>scalatest-maven-plugin</artifactId>
  <version>2.2.0</version>
  <configuration>
    <argLine>--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED</argLine>
  </configuration>
  <executions>
    <execution><id>test</id><goals><goal>test</goal></goals></execution>
  </executions>
</plugin>
```

실행: `mvn test -Dsuites=UpsertPrepareTest`

IntelliJ에서 바로 돌릴 때는 Run Configuration의 VM options에 같은 `--add-opens` 5개를 넣는다. Spark 4 / Scala 2.13 프로젝트는 `scalatest_2.13`을 쓴다.

### 8.3 테스트 코드

```scala
import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** src/test/resources/avro 의 avro 파일로 1~4단계(Iceberg 적재 직전)까지 검증한다 */
class UpsertPrepareTest extends AnyFunSuite with BeforeAndAfterAll {

  // 테스트할 테이블. avro 파일이 어느 테이블 것인지에 맞춰 바꾼다
  private val tableName = "<테이블2>"
  private val spec = UpsertJob.upsertSpecs(tableName)

  private var spark: SparkSession = _
  private var avroFiles: Seq[String] = _
  private var readFiles: Seq[String] = _  // Spark가 실제로 읽은 avro 파일
  private var input: DataFrame = _     // 1·2·4단계 결과 (중복 제거 전)
  private var prepared: DataFrame = _  // 1~4단계 결과 (MERGE에 넘길 source)

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("upsert-prepare-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.session.timeZone", "UTC")  // 운영 Pod TZ와 맞춘다
      .getOrCreate()

    val dir = new File(getClass.getResource("/avro").toURI)
    avroFiles = dir.listFiles().filter(_.getName.endsWith(".avro")).map(_.toURI.toString).sorted.toSeq
    require(avroFiles.nonEmpty, s"avro 파일이 없다: $dir")

    // 운영의 inputFileName과 같은 형식(한 줄에 경로 1개)의 목록 파일을 만든다
    val listFile = Files.createTempFile("avro-list", ".txt")
    Files.write(listFile, avroFiles.asJava, UTF_8)
    val inputFileName = listFile.toUri.toString

    val read = UpsertJob.readAvro(spark, inputFileName)
    readFiles = read.inputFiles.toSeq      // cache() 뒤에는 비어 있으므로 먼저 받아 둔다
    input = UpsertJob.renameColumns(read).cache()
    prepared = UpsertJob.prepare(spark, inputFileName, spec).cache()

    val in = input.count()
    val out = prepared.count()
    println(s"[요약] avro ${avroFiles.size}개, 입력 $in row → 중복 제거 후 $out row (제거 ${in - out})")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def keys = spec.keyCols.map(col)

  test("1·2단계: 목록 파일의 avro를 모두 읽는다") {
    assert(readFiles.size == avroFiles.size, s"읽은 파일: $readFiles")
    assert(input.count() > 0)
  }

  test("4단계: 컬럼명 변환 후 키·최신 판단 컬럼이 있다") {
    val missing = (spec.keyCols :+ spec.orderCol).filterNot(input.columns.contains)
    assert(missing.isEmpty, s"없는 컬럼: $missing")
  }

  test("키 컬럼에 NULL이 없다") {
    val nulls = input.where(spec.keyCols.map(c => col(c).isNull).reduce(_ || _)).count()
    assert(nulls == 0, s"키 NULL row $nulls 건")
  }

  test("3단계: 키별로 1건만 남는다") {
    val dup = prepared.groupBy(keys: _*).count().where(col("count") > 1).count()
    assert(dup == 0, s"중복 키 $dup 개")
  }

  test("3단계: 남은 건수 = 입력의 고유 키 수") {
    assert(prepared.count() == input.select(keys: _*).distinct().count())
  }

  test("3단계: 남은 row의 최신 판단 컬럼 값이 키별 최댓값이다") {
    val latest = input.groupBy(keys: _*).agg(max(spec.orderCol).as("_max"))
    val wrong = prepared.join(latest, spec.keyCols)
      .where(!(col(spec.orderCol) <=> col("_max"))).count()
    assert(wrong == 0, s"최신이 아닌 row $wrong 건")
  }

  test("3단계: 컬럼 구성이 입력과 같다 (임시 컬럼 _rn 없음)") {
    assert(prepared.columns.toSeq == input.columns.toSeq)
  }

  test("ts와 tmp_key가 같은 시각이다 (최신 판단을 ts로 하는 전제)") {
    assume(input.columns.contains("tmp_key"), "tmp_key 컬럼 없음")
    val diff = input.where(date_format(col("ts"), "yyyyMMddHHmmssSSS") =!= col("tmp_key"))
    val n = diff.count()
    if (n > 0) diff.select("ts", "tmp_key").show(5, truncate = false)
    assert(n == 0, s"ts와 tmp_key가 다른 row $n 건")
  }
}
```

### 8.4 검사 항목

| 테스트 | 확인 내용 | 실패하면 |
|--------|-----------|----------|
| 1·2단계 | 목록의 avro 파일을 빠짐없이 읽고 row가 있다 | 목록 파일 형식·경로 문제 |
| 4단계 | 변환 후 키 컬럼과 `ts`가 있다 | `renameColumns` 결과 이름이 `UpsertSpec`과 다름 |
| 키 NULL | 키 컬럼에 NULL이 없다 | NULL 키는 MERGE에서 batch마다 새 row로 쌓인다 |
| 키별 1건 | 같은 키가 2건 이상 남지 않는다 | MERGE가 에러로 실패한다 |
| 남은 건수 | 남은 row 수 = 입력의 고유 키 수 | 키가 빠졌거나 덜 걸러짐 |
| 최신 row | 남은 row의 `ts` = 키별 최댓값 | 과거 버전이 적재된다 |
| 컬럼 구성 | 입력과 컬럼이 같다 (`_rn` 없음) | `INSERT *`가 컬럼 불일치로 실패한다 |
| `ts` = `tmp_key` | `ts`를 `yyyyMMddHHmmssSSS`로 바꾼 값이 `tmp_key`와 같다 | `ts`로 최신 판단하는 전제가 깨진다. 다른 row 5건을 출력한다 |

`beforeAll`에서 요약을 한 줄 출력한다. 예: `[요약] avro 3개, 입력 8 row → 중복 제거 후 5 row (제거 3)`.

### 8.5 로컬 검증

Spark 3.5.8 / Scala 2.12.18 / ScalaTest 3.2.19, Java 21. ScalaTest Runner로 직접 실행했다. §8.2의 sbt·Maven 설정으로 실행해 보지는 않았다.

| 입력 | 결과 |
|------|------|
| 테스트용 avro 3개, 8 row (같은 키가 파일 사이·파일 안에 중복, 늦게 도착한 과거 버전 포함) | 8개 통과, 8 row → 5 row |
| 위와 같고 `prepare`에서 중복 제거를 고의로 뺌 | 키별 1건·남은 건수·최신 row 등 4개 실패 |
