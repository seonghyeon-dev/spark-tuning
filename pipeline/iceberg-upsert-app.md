# 신규 테이블 upsert 앱 (Spark append Job 변형)

| 항목 | 내용 |
|------|------|
| 대상 | 테이블1 (키 `tmp_id1`), 테이블2 (키 `tmp_id1`, `tmp_id2`). 파티션 키 `ts`, `tmp_id1` |
| 환경 | 운영 Spark 3.5.8 / Scala 2.12 / Iceberg 1.10.1, 카탈로그 HMS |
| 범위 | Spark 앱 코드만. Airflow DAG·executor 수 공식은 다루지 않는다 |
| 검증 | 로컬 Spark 3.5.8 + Iceberg 1.10.1에서 컴파일·실행 (§6) |

## 1. 결론

- 기존 append 앱에서 바뀌는 곳은 2곳이다. 키별 최신 row 선택(3단계)이 추가되고, `append()`가 `MERGE INTO`(5단계)로 바뀐다. avro read(1·2단계)와 컬럼명 변환(4단계)은 그대로 쓴다.
- MERGE는 SQL 문자열로 실행한다. Spark 3.5.8에는 MERGE용 Scala API가 없다. `Dataset.mergeInto`는 Spark 4.0부터 있다 (§4.4).
- MERGE에 `tmp_id1 IN (이번 batch 값)` 조건을 넣는다. 넣지 않으면 키가 파티션 컬럼이어도 대상 테이블 전체를 읽는다. 로컬 실측에서 읽은 파일이 21개에서 1개로 줄었다.
- `s.tmp_key > t.tmp_key`일 때만 update한다. 재처리 DAG이 다음날 올리는 과거 데이터가 최신 row를 덮어쓰지 않고, 같은 batch를 다시 실행해도 결과가 바뀌지 않는다.
- 재처리 DAG이 쓰는 batch_id 영수증은 `CommitMetadata.withCommitProperties`로 snapshot summary에 남긴다.
- 테이블은 `write.merge.mode=merge-on-read`로 만든다.

## 2. 처리 흐름

| 단계 | 내용 | 기존 대비 |
|------|------|-----------|
| 1 | S3의 avro 경로 목록 파일 read | 동일 |
| 2 | 목록의 avro 파일 read → DataFrame | 동일 |
| 4 | 컬럼명 변환 | 동일 (3단계 앞으로 이동) |
| 3 | 키별로 `tmp_key`가 가장 큰 row 1개만 남김 | 신규 |
| 5 | Iceberg `MERGE INTO` (upsert) | `append()` 대체 |

컬럼명 변환을 중복 제거 앞으로 옮겼다. 중복 제거는 컬럼 이름과 무관해서 결과가 같고, 이렇게 하면 키 컬럼 이름을 변환 후 이름 하나로만 관리한다. 원래 순서를 지키려면 `latestByKey`에는 원천 이름을, `upsert`의 `UpsertSpec`에는 변환 후 이름을 넣는다.

## 3. 코드

`<테이블1>`, `<테이블2>`는 앱 인자 `tableName`으로 넘어오는 실제 이름으로 바꾼다. `run`의 `???` 두 줄은 기존 코드 자리다.

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

## 4. 코드 설명

### 4.1 키별 최신 row 선택 (`latestByKey`)

키가 같은 row끼리 묶어 `tmp_key` 내림차순으로 번호를 매기고 1번만 남긴다.

| 입력 row | `tmp_id1` | `tmp_key` | 번호 | 결과 |
|---------|-----------|-----------|------|------|
| 1 | X | `20261014100000000` | 2 | 버림 |
| 2 | X | `20261014105900000` | 1 | 채택 |

- `tmp_key`는 `YYYYMMDDHHmmSSSSS` 17자리 고정 길이 문자열이라 문자열 비교 순서가 시간 순서와 같다.
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

**`s.tmp_key > t.tmp_key`: 과거 데이터가 최신을 덮어쓰지 않게 한다**

1. 10:59 버전 X가 batch 실패로 FAILURE가 된다.
2. 11:01 버전 X가 다음 batch에서 적재된다. 테이블에는 11:01 버전이 있다.
3. 다음날 04:00 재처리 DAG이 10:59 버전을 다시 올린다.

조건이 없으면 3에서 10:59 버전이 11:01 버전을 덮어쓴다. 조건이 있으면 `10:59 > 11:01`이 거짓이라 무시된다. 같은 batch를 두 번 실행해도 두 번째는 `tmp_key`가 같아 update가 0건이므로 Airflow retry로 인한 중복도 생기지 않는다.

**`t.tmp_id1 IN (...)`: 대상 테이블 읽기 범위를 줄인다**

MERGE는 source와 대상 테이블을 join한다. ON이 `t.tmp_id1 = s.tmp_id1`뿐이면 `tmp_id1`이 파티션 컬럼이어도 Spark가 대상 파티션을 미리 고르지 못해 테이블 전체를 읽는다. 로컬 실측(§6.2)에서 데이터 파일 21개 중 21개를 읽었다. 앱이 이번 batch의 `tmp_id1` 값을 먼저 모아 `IN` 조건으로 넣으면 Iceberg가 그 값의 파티션만 연다. 같은 실측에서 1개만 읽었다.

- 값 수집과 MERGE가 avro를 두 번 읽지 않도록 source를 `persist`한다. 캐시 크기는 중복 제거 후 batch 데이터 크기다.
- 값에 `'`나 `\`가 있어도 깨지지 않도록 escape한다. `Q'1\x` 키로 update가 정상 동작함을 확인했다.
- batch가 0 row면 MERGE를 하지 않는다. snapshot이 생기지 않으므로 재처리 DAG이 이 batch를 다시 올리는데, 0 row라 결과는 같다.

**`ts`는 ON에 넣지 않는다**

`tmp_key`는 `ts`와 같은 시간 값이다. 같은 키의 새 버전은 `ts`도 바뀐다. 10:59 버전과 11:01 버전은 `hour(ts)` 파티션이 다르다. ON에 `t.ts = s.ts`나 같은 시간대 조건을 넣으면 10:59 row를 찾지 못해 11:01 row가 새로 insert되고 X가 2건이 된다. 지금 코드는 `ts`를 조건에 쓰지 않으므로 10:59 row를 지우고 11:01 row를 11시 파티션에 넣는다 (§6.1 b2).

### 4.3 batch_id 영수증

기존 앱은 `.option("snapshot-property.batch_id", batchId)`로 영수증을 남긴다. MERGE는 SQL이라 이 옵션을 붙일 곳이 없다. `CommitMetadata.withCommitProperties`로 MERGE를 감싸면 그 안의 커밋 snapshot summary에 `batch_id`가 남는다. 재처리 DAG의 영수증 확인 쿼리(`element_at(summary, 'batch_id')`)는 바꿀 필요가 없다.

update·insert가 0건인 MERGE(같은 batch 재실행)도 snapshot이 생기고 `batch_id`가 남는다 (§6.1 b3).

### 4.4 Scala API로 MERGE를 할 수 있는가

| Spark | API | 비고 |
|-------|-----|------|
| 3.5.8 (운영) | 없음. SQL `MERGE INTO`만 가능 | `spark-sql_2.12-3.5.8.jar`에 `MergeIntoWriter` 클래스 없음 |
| 4.0 이상 (목표 4.1.1) | `df.mergeInto(table, 조건).whenMatched(...).updateAll().whenNotMatched().insertAll().merge()` | `spark-sql-api_2.13-4.1.1.jar`에서 확인. Iceberg와의 동작은 미검증 |

운영 버전에서는 SQL을 쓴다. source DataFrame은 temp view로 넘기므로 SQL에 들어가는 것은 MERGE 문장뿐이다.

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
PARTITIONED BY (hours(ts), tmp_id1)
TBLPROPERTIES (
  'format-version'   = '2',
  'write.merge.mode' = 'merge-on-read'
);
```

- `NOT NULL`: `tmp_id1`, `tmp_id2`는 필수 값이다. NULL 키는 ON의 `=`가 항상 거짓이라 batch마다 새 row로 쌓인다. 테이블에서 막아 두면 쓰기 시점에 실패한다.
- `merge-on-read`: 기본값 copy-on-write는 update가 1건이어도 그 row가 든 파일 전체를 다시 쓴다. 로컬에서는 같은 batch를 재실행해 변경이 0건일 때도 파일을 다시 썼다(§6.1 b3). merge-on-read는 기존 파일은 두고 바뀐 row만 position delete 파일로 표시한다.
- 파티션 transform은 `hours(ts)`, identity `tmp_id1`로 가정했다. 실제 transform이 다르면 알려 달라.

## 6. 로컬 검증

환경: `local[2]`, Spark 3.5.8 + Iceberg 1.10.1, Hadoop 카탈로그(운영은 HMS), §5 테이블 설정. 위 §3 코드를 그대로 컴파일해 실행했다.

### 6.1 결과 (테이블1, 키 `tmp_id1`)

| batch | 입력 | 결과 |
|-------|------|------|
| b0 | 배경 데이터 20개 키 | 20건 insert |
| b1 | X 3건 (10:00 1건, 10:59 2건) | 10:59 버전 1건만 insert |
| b2 | X 11:01 | 10:59 row 삭제 표시(position delete 1건), 11:01 row를 11시 파티션에 insert. X는 1건 |
| b3 | b2 재실행 | 변경 0건, snapshot summary에 `batch_id=b3` 기록 |
| bq, bq2 | `Q'1\x` 키 10:59 → 11:01 | escape된 `IN` 조건으로 update 정상, 중복 없음 |

테이블2(키 `tmp_id1`, `tmp_id2`)도 같은 시나리오에서 결과가 같았다. 추가로 X/2의 1시간 전 버전(`stale`)이 기존 row를 덮어쓰지 않았다.

### 6.2 대상 테이블 읽기 범위 (b2, merge-on-read)

| ON 조건 | 대상 데이터 파일 | 읽은 파일 |
|---------|------------------|-----------|
| `t.tmp_id1 = s.tmp_id1` | 21 | 21 |
| 위 + `t.tmp_id1 IN ('X')` | 21 | 1 |

읽은 파일 수는 Iceberg scan metric `resultDataFiles`로 쟀다. 테이블2(대상 61개)도 61 → 1이었다.

## 7. 미확인·후속

- **executor 수 공식**: MERGE는 avro 외에 대상 테이블도 읽는다. 기존 공식(avro 크기 기준)이 맞는지 추후 검토(사용자 2026-10-14).
- **대상 읽기 범위의 증가**: `IN` 조건은 `tmp_id1` 값으로만 줄인다. 그 값의 모든 시간 파티션을 읽으므로 이력이 쌓일수록 늘어난다. 같은 키의 이전 버전이 존재할 수 있는 최대 기간을 알면 `t.ts >= <하한>` 조건을 추가할 수 있다.
- **동시 MERGE 충돌 (미실측)**: append는 동시 커밋이 안전하지만 MERGE는 Iceberg가 충돌을 검사한다. 04:00 재처리와 append가 같은 테이블에 겹치면 한쪽이 `ValidationException`으로 실패할 수 있다. 데이터 유실은 아니고 FAILURE로 남아 다음날 회수된다.
- **delete 파일 정리**: merge-on-read는 position delete 파일을 남긴다. Compaction에서 delete 파일 정리를 함께 다뤄야 한다.
- **Spark 4.1 `mergeInto` + Iceberg 1.11**: 동작 미검증.
