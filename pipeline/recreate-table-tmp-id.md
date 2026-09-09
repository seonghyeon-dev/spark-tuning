# Iceberg 테이블 재생성 + `tmp_id`(NOT NULL) 추가 절차서

| 항목 | 내용 |
|------|------|
| 목적 | Iceberg 테이블에 `tmp_id STRING NOT NULL` 컬럼을 원하는 위치에 추가 |
| 값 출처 | Oracle 테이블을 키 2개로 LEFT JOIN. 매칭 없으면 `''` |
| 방법 | 임시 테이블 복사 → `DROP ... PURGE` → `CREATE` → 조인해서 `INSERT` (1회성) |
| 데이터 | 약 1.5GB |
| 신규 요소 | **Oracle JDBC 접근뿐.** Iceberg 접근은 기존 Spark Job 설정 그대로 |
| 이름 | 이 문서의 `iceberg.db.table_a`, `table_a_tmp`, `key1`/`key2`, `ORA_SCHEMA.ORA_TABLE`은 전부 **자리표시자** — 실제 값으로 바꿔 쓴다 |

---

## 1. 왜 재생성인가

| 시도 | 결과 |
|------|------|
| Spark SQL `ALTER TABLE ... ADD COLUMN tmp_id STRING NOT NULL` | ❌ Iceberg는 비어 있지 않은 테이블에 required 컬럼 추가를 거부 |
| `ALTER TABLE ... ALTER COLUMN tmp_id SET NOT NULL` | ❌ 미지원 |
| `ADD COLUMN ... DEFAULT ''` | ❌ Iceberg 1.11.0 기준 Spark SQL 미지원 |
| Trino `ALTER TABLE ... ADD COLUMN ... NOT NULL` | ❌ 비어 있지 않은 테이블 불가. `AFTER` 문법 없음, `SET NOT NULL` 미지원 |

→ 테이블을 다시 만드는 것이 유일한 방법이다. **새 테이블은 snapshot 이력과 table UUID가 초기화된다.**

Oracle에는 아무것도 만들지 않는다. `spark.read.format("jdbc")`는 Oracle에 SELECT만 실행하고, `createOrReplaceTempView("ora")`는 Spark 세션 안의 이름 등록일 뿐이다.

---

## 2. 역할 분담과 흐름

DROP / CREATE는 앱에 맡기지 않고 **spark-sql로 수동** 실행한다. 앱은 그 앞뒤만 담당한다.

```
  0  Airflow 쓰기 DAG 일시 중지
  │
  1  [앱] backup      원본 → 임시 테이블 CTAS, 건수 일치 확인
  │                   SHOW CREATE TABLE 을 로그에 남긴다 (3단계 DDL 의 출처)
  │
  2  [앱] check       Oracle 읽기 → 키 중복 0건 확인 → 임시 테이블 대비 unmatched 집계
  │                   ※ 아무것도 쓰지 않는다. 결과가 납득될 때까지 반복해도 된다
  │
  3  [수동] spark-sql  DROP TABLE ... PURGE          ★ 여기부터 롤백 없음. 임시 테이블이 유일한 사본
  │                   CREATE TABLE ... (tmp_id STRING NOT NULL 을 원하는 위치에)
  │                   ALTER TABLE ... WRITE ORDERED BY ...
  │
  4  [앱] load        임시 LEFT JOIN Oracle → 신규 테이블 INSERT
  │                   건수 일치 · tmp_id='' 건수 == unmatched · NULL 0건 확인
  │
  5  Trino 확인 → Airflow 재개 → 며칠 뒤 임시 테이블 DROP ... PURGE
```

| 모드 | 쓰기 | 통과 조건 (`require` — 실패 시 즉시 종료) |
|------|------|------------------------------------------|
| `backup` | 임시 테이블 생성 | 원본 존재 · 임시 없음 · 임시 COUNT == 원본 COUNT |
| `check` | **없음** | `ora` 0건 아님 · 키·`tmp_id` 컬럼 존재 · **`ora` 키 중복 == 0** (unmatched는 기록만) |
| `load` | 신규 테이블 INSERT | 임시 존재 · 신규 존재 · **신규 COUNT == 0** · 신규 `tmp_id` 존재·`string`·NOT NULL · 신규 컬럼(tmp_id 제외) == 임시 컬럼 · `ora` 키 중복 == 0 → INSERT 후 신규 COUNT == 임시 COUNT · `tmp_id = ''` == unmatched · `tmp_id IS NULL` == 0 |

---

## 3. 사전 준비 — 빌드

### 3.1 버전

운영 스택 실측(Spark 3.5.8 / Scala 2.12 / Iceberg 1.10.1 — `s3fileio-migration-guide.md` §5.0.2)에 맞춘다. Spark·Iceberg runtime은 이미지의 `$SPARK_HOME/jars`가 제공하므로 `provided`, **ojdbc8만 fat jar에 넣는다.** 이미지 변경도 `--jars`도 필요 없다 (라이브러리 배치 원칙: 같은 가이드 §5.0.5).

### 3.2 sbt 프로젝트 (신규 프로젝트로 빌드할 때)

```
recreate-table/
├── build.sbt
├── project/build.properties
├── project/plugins.sbt
└── src/main/scala/RecreateTable.scala     ← 4절
```

```scala title="build.sbt"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "lakehouse"
ThisBuild / version      := "0.1.0"

val sparkVersion   = "3.5.8"
val icebergVersion = "1.10.1"
val ojdbcVersion   = "23.9.0.25.07"   // ojdbc8: JDK 8/11/17/21 호환

lazy val root = (project in file("."))
  .settings(
    name := "recreate-table",
    libraryDependencies ++= Seq(
      "org.apache.spark"         %% "spark-sql"                      % sparkVersion   % Provided,
      "org.apache.iceberg"       %  "iceberg-spark-runtime-3.5_2.12" % icebergVersion % Provided,
      "com.oracle.database.jdbc" %  "ojdbc8"                         % ojdbcVersion
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-Xlint"),
    assembly / assemblyJarName := "recreate-table-assembly.jar",
    assembly / mainClass := Some("RecreateTable"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF")       => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.exists(_.endsWith(".SF")) ||
                                            xs.exists(_.endsWith(".DSA")) ||
                                            xs.exists(_.endsWith(".RSA")) => MergeStrategy.discard
      case PathList("META-INF", "services", _*)      => MergeStrategy.concat
      case PathList("META-INF", _*)                  => MergeStrategy.first
      case "module-info.class"                       => MergeStrategy.discard
      case _                                         => MergeStrategy.first
    },
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false)
  )
```

```properties title="project/build.properties"
sbt.version=1.10.7
```

```scala title="project/plugins.sbt"
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")
```

```bash
sbt assembly
# → target/scala-2.12/recreate-table-assembly.jar  (약 7.4MB = 앱 + ojdbc8. Spark/Scala/Iceberg 클래스 없음)
```

### 3.3 기존 앱(Maven)에 합칠 때

`RecreateTable.scala`를 기존 소스에 넣고 `pom.xml`에 다음만 추가한다. 나머지는 이미 있다.

```xml
<dependency>
  <groupId>com.oracle.database.jdbc</groupId>
  <artifactId>ojdbc8</artifactId>
  <version>23.9.0.25.07</version>
</dependency>
```

---

## 4. 앱 코드 — `RecreateTable.scala`

설정 블록의 값만 바꾼다. **컬럼 목록은 설정하지 않는다** — `load`가 신규 테이블 스키마를 읽어 그 순서대로 SELECT를 만들고, `tmp_id` 자리에만 `COALESCE(CAST(o.tmp_id AS STRING), '')`를 넣는다.

| 설정 | 자리표시자 | 설명 |
|------|-----------|------|
| `Catalog` / `Database` / `Table` | `iceberg` / `db` / `table_a` | 재생성 대상. `backup`의 원본이자 `load`의 목적지 (같은 이름으로 다시 만든다) |
| `TmpTable` | `table_a_tmp` | 임시(백업) 테이블 |
| `OracleUrl` / `OracleUser` / `OraclePassword` | — | 1회성이라 하드코딩. **커밋하지 말 것** |
| `OracleQuery` | `(SELECT key1, key2, tmp_id FROM ORA_SCHEMA.ORA_TABLE) t` | 키 타입을 Iceberg 쪽과 맞춘다. 컬럼명이 다르면 `AS`로 |
| `JoinKeys` | `key1 -> key1`, `key2 -> key2` | Iceberg 컬럼 → `ora` 컬럼 |
| `TmpIdCol` / `TmpIdDefault` | `tmp_id` / `''` | 신규 컬럼과 unmatched 기본값 |

```scala title="RecreateTable.scala"
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Iceberg 테이블 재생성 + `tmp_id STRING NOT NULL` 추가 — Spark 앱이 맡는 부분.
 * DROP / CREATE 는 spark-sql 로 수동 실행한다.
 *
 *   backup — 기존 테이블 → 임시 테이블 CTAS, 건수 일치 require. SHOW CREATE TABLE 을 로그에 남긴다
 *   check  — 쓰기 없음. Oracle 읽기 → 키 중복 require → 임시 테이블 대비 unmatched 집계
 *   load   — 임시 테이블 LEFT JOIN Oracle → 신규 테이블 INSERT → 건수·tmp_id 검증
 */
object RecreateTable {

  // ═══════════════════════════════════════════════════════════════════════
  // 설정 — 실행 전 이 블록만 채운다 (값은 전부 자리표시자)
  // ═══════════════════════════════════════════════════════════════════════

  // ── 테이블 ──────────────────────────────────────────────────────────────
  val Catalog  = "iceberg"      // spark.sql.catalog.<이름>
  val Database = "db"
  val Table    = "table_a"      // 재생성 대상. backup 의 원본이자 load 의 목적지 (같은 이름으로 다시 만든다)
  val TmpTable = "table_a_tmp"  // 임시(백업) 테이블. 작업 후 며칠 뒤 수동 DROP ... PURGE

  val Tbl = s"$Catalog.$Database.$Table"
  val Tmp = s"$Catalog.$Database.$TmpTable"

  // ── Oracle (1회성이므로 하드코딩, 커밋 금지) ─────────────────────────────
  val OracleUrl      = "jdbc:oracle:thin:@//oracle-host:1521/SERVICE"
  val OracleUser     = "ora_user"
  val OraclePassword = "ora_password"
  // Oracle 에는 이 SELECT 만 실행된다. 키 컬럼 타입은 Iceberg 쪽과 맞춰 둘 것
  // (계열이 다르면 — 예: string vs NUMBER — 조인이 조용히 비어 unmatched 로만 드러난다).
  // Oracle 이 반환하는 컬럼명은 대문자지만 Spark SQL 은 기본 대소문자 무시라 소문자로 참조해도 된다.
  val OracleQuery     = "(SELECT key1, key2, tmp_id FROM ORA_SCHEMA.ORA_TABLE) t"
  val OracleFetchSize = "10000"

  // ── 조인 키: Iceberg 컬럼 -> ora 뷰 컬럼 ─────────────────────────────────
  val JoinKeys: Seq[(String, String)] = Seq("key1" -> "key1", "key2" -> "key2")
  val TmpIdCol     = "tmp_id"   // 신규 컬럼 (STRING NOT NULL). Oracle 쪽 컬럼명이 다르면 OracleQuery 에서 AS 로 맞춘다
  val TmpIdDefault = ""         // unmatched 행에 넣는 값. NOT NULL 이므로 NULL 불가

  // ═══════════════════════════════════════════════════════════════════════
  // 이하 로직
  // ═══════════════════════════════════════════════════════════════════════

  private var spark: SparkSession = _

  def main(args: Array[String]): Unit = {
    val mode = args.headOption.getOrElse("check")
    require(Set("backup", "check", "load").contains(mode), s"알 수 없는 모드: $mode (backup | check | load)")

    spark = SparkSession.builder().appName(s"RecreateTable[$mode] $Tbl").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log(s"모드 = $mode, 대상 = $Tbl, 임시 = $Tmp")

    mode match {
      case "backup" => backup()
      case "check"  => check()
      case "load"   => load()
    }
    spark.stop()
  }

  // ─────────────────────────────────────────────────────────────────────
  // backup: 기존 테이블 → 임시 테이블
  // ─────────────────────────────────────────────────────────────────────
  def backup(): Unit = {
    require(tableExists(Tbl), s"[backup] 원본 테이블이 없다: $Tbl")
    require(!tableExists(Tmp), s"[backup] 임시 테이블이 이미 있다: $Tmp — 이름을 바꾸거나 DROP ... PURGE 후 재실행")

    logShowCreate(Tbl)   // 수동 CREATE DDL 의 출처. tmp_id 만 끼워 넣어 쓴다
    val src = count(Tbl)
    log(s"[backup] CREATE TABLE $Tmp AS SELECT * FROM $Tbl (원본 $src 건)")
    sql(s"CREATE TABLE $Tmp USING iceberg AS SELECT * FROM $Tbl")

    val dst = count(Tmp)
    require(dst == src, s"[backup] 건수 불일치: 원본 $src vs 임시 $dst")
    log(s"[backup 완료] $Tmp = $dst 건. 다음: check → (수동) DROP/CREATE → load")
  }

  // ─────────────────────────────────────────────────────────────────────
  // check: 쓰기 없이 Oracle 검증
  // ─────────────────────────────────────────────────────────────────────
  def check(): Unit = {
    // 임시 테이블이 있으면 그것을, 없으면(backup 전) 원본을 대조군으로
    val source = if (tableExists(Tmp)) Tmp else Tbl
    require(tableExists(source), s"[check] 대조할 테이블이 없다: $Tmp / $Tbl")
    val total = count(source)
    log(s"[check] 대조군 = $source ($total 건)")

    loadOracle()
    val unmatched = verifyOracle(source)
    log(s"[check 완료] $total 건 중 unmatched $unmatched 건 (${pct(unmatched, total)}) 이 '$TmpIdDefault' 로 채워질 예정")
  }

  // ─────────────────────────────────────────────────────────────────────
  // load: 임시 LEFT JOIN ora → 신규 테이블 INSERT → 검증
  // ─────────────────────────────────────────────────────────────────────
  def load(): Unit = {
    // [0] 사전 검사 — 수동 DROP/CREATE 가 의도대로 됐는지
    require(tableExists(Tmp), s"[load] 임시 테이블이 없다: $Tmp — backup 먼저")
    require(tableExists(Tbl), s"[load] 신규 테이블이 없다: $Tbl — 수동 CREATE 먼저")
    val newSchema = spark.table(Tbl).schema
    val tmpSchema = spark.table(Tmp).schema
    requireTmpIdRequired(newSchema)
    requireColumnsCovered(newSchema, tmpSchema)
    val existing = count(Tbl)
    require(existing == 0L, s"[load] 신규 테이블이 비어 있지 않다: $Tbl = $existing 건 — 원본을 DROP 하지 않았거나 이미 load 됐다")
    val tmpCount = count(Tmp)
    log(s"[load] 신규 테이블 스키마 확인 완료 (${newSchema.size} 컬럼, $TmpIdCol NOT NULL). 임시 $tmpCount 건")
    logShowCreate(Tbl)

    // [1] Oracle → ora, 키 중복 require, unmatched 집계
    loadOracle()
    val unmatched = verifyOracle(Tmp)

    // [2] INSERT — 단일 커밋. 실패하면 신규 테이블은 그대로 비어 있다
    val insert = s"INSERT INTO $Tbl\n${insertSelectSql(newSchema, Tmp)}"
    sql(insert)

    // [3] 검증
    val newCount = count(Tbl)
    require(newCount == tmpCount, s"[load] 건수 불일치: 임시 $tmpCount vs 신규 $newCount")
    val defaulted = scalarLong(s"SELECT COUNT(*) FROM $Tbl WHERE $TmpIdCol = '$TmpIdDefault'")
    require(defaulted == unmatched, s"[load] '$TmpIdDefault' 건수 불일치: 예상(unmatched) $unmatched vs 실제 $defaulted")
    val nulls = scalarLong(s"SELECT COUNT(*) FROM $Tbl WHERE $TmpIdCol IS NULL")
    require(nulls == 0L, s"[load] $TmpIdCol NULL 이 $nulls 건")

    log(s"[load 완료] $Tbl = $newCount 건, $TmpIdCol='$TmpIdDefault' $defaulted 건 (${pct(defaulted, newCount)})")
    log(s"[후속] ① Trino 에서 스키마·건수 확인  ② Airflow 쓰기 DAG 재개  ③ 며칠 뒤 DROP TABLE $Tmp PURGE")
  }

  // ─────────────────────────────────────────────────────────────────────
  // Oracle 읽기 → cache → temp view `ora`
  // ─────────────────────────────────────────────────────────────────────
  def loadOracle(): Unit = {
    log(s"[ora] Oracle 읽기: $OracleQuery")
    val ora: DataFrame = spark.read
      .format("jdbc")
      .option("url", OracleUrl)
      .option("user", OracleUser)
      .option("password", OraclePassword)
      .option("driver", "oracle.jdbc.OracleDriver")
      .option("dbtable", OracleQuery)
      .option("fetchsize", OracleFetchSize)
      // 커넥션 1개로 읽는다. 수천만 건 이상이면 partitionColumn/lowerBound/upperBound/numPartitions 고려
      .load()
      .cache()
    ora.createOrReplaceTempView("ora")
    val n = ora.count()   // cache 실체화
    log(s"[ora] $n 건, 스키마: ${ora.schema.map(f => s"${f.name}:${f.dataType.simpleString}").mkString(", ")}")
    require(n > 0L, "[ora] Oracle 조회 결과가 0건")
    JoinKeys.foreach { case (_, o) => require(hasColumn(ora.schema, o), s"[ora] 조인 키 컬럼이 없다: $o") }
    require(hasColumn(ora.schema, TmpIdCol), s"[ora] $TmpIdCol 컬럼이 없다 — OracleQuery 에서 AS $TmpIdCol 로 맞춰라")
  }

  // ─────────────────────────────────────────────────────────────────────
  // Oracle 검증: 키 중복 0 require, unmatched 집계 (source 는 Tmp 또는 Tbl)
  // ─────────────────────────────────────────────────────────────────────
  def verifyOracle(source: String): Long = {
    val oraKeys = JoinKeys.map(_._2).mkString(", ")
    val dup = scalarLong(s"SELECT COUNT(*) FROM (SELECT $oraKeys FROM ora GROUP BY $oraKeys HAVING COUNT(*) > 1)")
    require(dup == 0L, s"[ora] 키 중복 $dup 건 — 조인 시 행이 불어난다. OracleQuery 를 좁혀라")

    val srcSchema = spark.table(source).schema
    JoinKeys.foreach { case (t, _) => require(hasColumn(srcSchema, t), s"[ora] $source 에 조인 키 컬럼이 없다: $t") }

    val oraNullKey = scalarLong(s"SELECT COUNT(*) FROM ora WHERE ${JoinKeys.map(k => s"${k._2} IS NULL").mkString(" OR ")}")
    val srcNullKey = scalarLong(s"SELECT COUNT(*) FROM $source WHERE ${JoinKeys.map(k => s"${k._1} IS NULL").mkString(" OR ")}")
    log(s"[ora] 키 NULL — ora $oraNullKey 건, $source $srcNullKey 건 (NULL 키는 절대 매칭되지 않는다)")

    // 키 타입 대조 — 경고만. Oracle NUMBER 는 decimal 로 와서 int 와 달라도 조인은 된다
    val oraSchema = spark.table("ora").schema
    JoinKeys.foreach { case (t, o) =>
      val st = typeOf(srcSchema, t); val ot = typeOf(oraSchema, o)
      if (st != ot) log(s"[ora] ⚠ 키 타입 다름: $source.$t=$st vs ora.$o=$ot — 암묵 CAST 로 조인. unmatched 가 비정상적으로 크면 여기부터 의심")
    }

    val total = count(source)
    val unmatched = scalarLong(s"SELECT COUNT(*) FROM $source t LEFT JOIN ora o ON $joinCondition WHERE o.$TmpIdCol IS NULL")
    log(s"[ora] unmatched = $unmatched / $total (${pct(unmatched, total)}) → '$TmpIdDefault' 로 채워진다")
    unmatched
  }

  // ─────────────────────────────────────────────────────────────────────
  // INSERT SELECT — 신규 테이블 스키마 순서대로. tmp_id 만 ora 에서, 나머지는 임시 테이블에서
  // ─────────────────────────────────────────────────────────────────────
  def insertSelectSql(newSchema: StructType, source: String): String = {
    val cols = newSchema.fieldNames.map { c =>
      if (c.equalsIgnoreCase(TmpIdCol)) s"COALESCE(CAST(o.$TmpIdCol AS STRING), '$TmpIdDefault') AS $TmpIdCol"
      else s"t.$c"
    }
    s"""SELECT
       |  ${cols.mkString(",\n  ")}
       |FROM $source t
       |LEFT JOIN ora o ON $joinCondition""".stripMargin
  }

  def joinCondition: String = JoinKeys.map { case (t, o) => s"t.$t = o.$o" }.mkString(" AND ")

  // 신규 테이블에 tmp_id 가 있고 string · NOT NULL 인지
  def requireTmpIdRequired(schema: StructType): Unit = {
    val f = schema.find(_.name.equalsIgnoreCase(TmpIdCol))
    require(f.isDefined, s"$Tbl 에 $TmpIdCol 이 없다 — CREATE DDL 확인")
    require(!f.get.nullable, s"$Tbl.$TmpIdCol 이 NOT NULL 이 아니다 — CREATE DDL 에 NOT NULL 을 넣어라")
    require(f.get.dataType.simpleString == "string", s"$Tbl.$TmpIdCol 타입이 string 이 아니다: ${f.get.dataType.simpleString}")
  }

  // 신규 테이블 컬럼(tmp_id 제외) == 임시 테이블 컬럼. 빠지거나 더해진 게 있으면 중단
  def requireColumnsCovered(newSchema: StructType, tmpSchema: StructType): Unit = {
    val newCols = newSchema.fieldNames.map(_.toLowerCase).filterNot(_ == TmpIdCol.toLowerCase).toSet
    val tmpCols = tmpSchema.fieldNames.map(_.toLowerCase).toSet
    val missing = newCols.diff(tmpCols); val dropped = tmpCols.diff(newCols)
    require(missing.isEmpty, s"$Tbl 에 $Tmp 에 없는 컬럼이 있다: ${missing.mkString(", ")}")
    require(dropped.isEmpty, s"$Tmp 의 컬럼이 $Tbl 에서 빠졌다: ${dropped.mkString(", ")}")
    // 타입은 나란히 출력 — 임시는 원본 CTAS 라 같아야 정상
    log("[load] 컬럼 타입 대조 (신규 vs 임시):\n" + newSchema.map { f =>
      val tt = if (f.name.equalsIgnoreCase(TmpIdCol)) "(ora)" else typeOf(tmpSchema, f.name)
      val mark = if (tt == "(ora)" || tt == f.dataType.simpleString) "  " else "⚠ "
      f"  $mark${f.name}%-24s ${f.dataType.simpleString}%-20s $tt"
    }.mkString("\n"))
  }

  // ─────────────────────────────────────────────────────────────────────
  // 유틸
  // ─────────────────────────────────────────────────────────────────────
  def sql(q: String): DataFrame = { log(s"SQL> ${q.linesIterator.map(_.trim).filter(_.nonEmpty).mkString(" ")}"); spark.sql(q) }
  def scalarLong(q: String): Long = spark.sql(q).first().getLong(0)
  def count(t: String): Long = scalarLong(s"SELECT COUNT(*) FROM $t")
  def tableExists(t: String): Boolean = spark.catalog.tableExists(t)
  def hasColumn(s: StructType, c: String): Boolean = s.fieldNames.exists(_.equalsIgnoreCase(c))
  def typeOf(s: StructType, c: String): String = s.find(_.name.equalsIgnoreCase(c)).map(_.dataType.simpleString).getOrElse("<없음>")
  def logShowCreate(t: String): Unit = log(s"SHOW CREATE TABLE $t ↓\n${spark.sql(s"SHOW CREATE TABLE $t").first().getString(0)}")
  def pct(n: Long, total: Long): String = if (total == 0L) "n/a" else f"${n * 100.0 / total}%.2f%%"
  def log(msg: String): Unit = println(s"[RecreateTable] $msg")
}
```

> **컴파일 확인 완료** — Scala 2.12.18 / Spark 3.5.8 / Iceberg 1.10.1 조합으로 `sbt assembly` 통과 (경고 0건).

---

## 5. 실행 매니페스트 — SparkApplication

Iceberg 접근(카탈로그·S3A·eventLog·serviceAccount·이미지)은 기존 append/Compaction Job이 이미 되는 상태이므로 **그 스펙을 복사하고 ★ 항목만 바꾼다.**

```yaml title="recreate-table-sparkapp.yaml"
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: recreate-table-tmp-id                       # ★
  namespace: <기존 Job 과 동일>
spec:
  type: Scala
  mode: cluster
  image: <기존 Job 과 동일 — Spark 3.5.8 / Scala 2.12 / Iceberg 1.10.1 runtime 포함 이미지>
  sparkVersion: 3.5.8

  mainApplicationFile: s3a://<bucket>/jars/recreate-table-assembly.jar   # ★ sbt assembly 산출물
  mainClass: RecreateTable                                               # ★
  arguments:                                                             # ★ backup | check | load
    - "check"

  restartPolicy:
    type: Never              # ★ 실패 시 operator 재시도 금지 — 각 모드가 멱등이 아니다
  timeToLiveSeconds: 86400   # 완료 후 하루 동안 Pod/로그 보존

  sparkConf:
    # ── 기존 Job 의 sparkConf 그대로 (spark.sql.extensions, spark.sql.catalog.*, fs.s3a.*, eventLog ...) ──
    # spark.sql.catalog.<catalog> 의 이름이 RecreateTable.scala 의 Catalog 와 같아야 한다.

    # ── 이 Job 전용 ──
    spark.sql.shuffle.partitions: "32"        # 데이터 ~1.5GB. range 분배 shuffle 이라 낮게
    spark.sql.adaptive.enabled: "true"
    spark.dynamicAllocation.enabled: "false"  # 1회성이라 instances 고정

  driver:
    cores: 2
    coreLimit: "2"
    memory: 4g
    serviceAccount: <기존 Job 과 동일>
    env:
      # 기존 Job 의 env 그대로 (MinIO 자격증명 등). Oracle 접속 정보는 코드에 있어 env 불필요
      - name: AWS_REGION
        value: us-east-1
  executor:
    instances: 2
    cores: 2
    coreLimit: "2"
    memory: 4g
```

> **Oracle 방화벽은 driver·executor 양쪽.** JDBC 읽기는 executor에서 실행된다 — driver만 열어 두면 `[ora]` 단계에서 connection refused.

---

## 6. 절차

### 6.0 Airflow 쓰기 DAG 일시 중지

append DAG을 멈춘다. Compaction·maintenance DAG도 대상 테이블 건은 멈춘다.

### 6.1 빌드 → jar 배치

```bash
sbt assembly
mc cp target/scala-2.12/recreate-table-assembly.jar minio/<bucket>/jars/   # 매니페스트 mainApplicationFile 과 일치
```

### 6.2 `backup` — 원본 → 임시 테이블

```bash
# arguments: ["backup"]
kubectl apply -f recreate-table-sparkapp.yaml
kubectl logs -f recreate-table-tmp-id-driver -n <ns> | grep RecreateTable
kubectl delete -f recreate-table-sparkapp.yaml
```

기대 로그:

```
[RecreateTable] SHOW CREATE TABLE iceberg.db.table_a ↓
CREATE TABLE iceberg.db.table_a ( ... ) USING iceberg PARTITIONED BY (...) TBLPROPERTIES (...)
[RecreateTable] [backup] CREATE TABLE iceberg.db.table_a_tmp AS SELECT * FROM iceberg.db.table_a (원본 N 건)
[RecreateTable] [backup 완료] iceberg.db.table_a_tmp = N 건. 다음: check → (수동) DROP/CREATE → load
```

**`SHOW CREATE TABLE` 출력을 복사해 둔다.** 6.4의 CREATE DDL은 이것을 그대로 쓰고 `tmp_id` 한 줄만 끼운다.

### 6.3 `check` — Oracle 검증 (쓰기 없음)

```bash
# arguments: ["check"]
kubectl apply -f recreate-table-sparkapp.yaml
kubectl logs -f recreate-table-tmp-id-driver -n <ns> | grep RecreateTable
kubectl delete -f recreate-table-sparkapp.yaml
```

기대 로그와 판정:

```
[RecreateTable] [ora] N 건, 스키마: KEY1:string, KEY2:string, TMP_ID:string
[RecreateTable] [ora] 키 NULL — ora 0 건, iceberg.db.table_a_tmp 0 건
[RecreateTable] [ora] unmatched = M / N (x.xx%) → '' 로 채워진다
[RecreateTable] [check 완료] N 건 중 unmatched M 건 (x.xx%) 이 '' 로 채워질 예정
```

| 확인 | 판정 |
|------|------|
| `키 중복` require 통과 | 안 되면 `OracleQuery`를 좁힌다 (조인 시 행이 불어난다) |
| `⚠ 키 타입 다름` 경고 | 없어야 정상. Oracle `NUMBER` ↔ Iceberg `int`(decimal vs int)는 조인되지만, `string` ↔ 숫자는 조인이 조용히 비어 unmatched로만 드러난다 |
| unmatched 비율 | 예상 범위인지. 비정상적으로 크면 타입·키 컬럼부터 의심 |

납득될 때까지 `OracleQuery`·`JoinKeys`를 고쳐 반복해도 된다 — 아무것도 쓰지 않는다.

### 6.4 (수동) spark-sql — DROP / CREATE

> **★ `DROP` 이후 롤백 없음.** 임시 테이블이 유일한 사본이다. 6.2·6.3이 통과한 뒤에만 진행한다.

```sql
-- 0. 확인: 임시 == 원본 (backup 로그와 대조)
SELECT COUNT(*) FROM iceberg.db.table_a;
SELECT COUNT(*) FROM iceberg.db.table_a_tmp;

-- 0-1. gc.enabled=false 면 PURGE 가 거부된다
SHOW TBLPROPERTIES iceberg.db.table_a;
-- ALTER TABLE iceberg.db.table_a SET TBLPROPERTIES ('gc.enabled' = 'true');

-- 1. 원본 DROP. PURGE 가 없으면 데이터 파일이 MinIO 에 남는다
DROP TABLE iceberg.db.table_a PURGE;

-- 2. 같은 이름으로 재생성. backup 로그의 SHOW CREATE TABLE 출력을 붙여 넣고 tmp_id 한 줄만 원하는 위치에 끼운다.
--    파티션·TBLPROPERTIES 는 그대로 복사한다.
CREATE TABLE iceberg.db.table_a (
  ts      TIMESTAMP_NTZ,
  par_a   STRING,
  key1    STRING,
  key2    STRING,
  tmp_id  STRING NOT NULL,          -- ★ 신규 컬럼
  sort_a  STRING,
  sort_b  STRING,
  col_a   STRING,
  col_b   STRING
)
USING iceberg
PARTITIONED BY (hours(ts), par_a)
TBLPROPERTIES (
  'format-version' = '2',
  'write.distribution-mode' = 'range'
  -- write.target-file-size-bytes, write.metadata.metrics.column.* 등 기존 속성 그대로
);

-- 3. Sort Order 는 CREATE 에 못 쓴다 — 기존과 동일하게
ALTER TABLE iceberg.db.table_a WRITE ORDERED BY sort_a, sort_b;

-- 4. 확인: tmp_id 위치·NOT NULL, 파티션, Sort Order
SHOW CREATE TABLE iceberg.db.table_a;
```

### 6.5 `load` — 조인해서 INSERT

```bash
# arguments: ["load"]
kubectl apply -f recreate-table-sparkapp.yaml
kubectl logs -f recreate-table-tmp-id-driver -n <ns> | grep RecreateTable
kubectl delete -f recreate-table-sparkapp.yaml
```

기대 로그:

```
[RecreateTable] [load] 신규 테이블 스키마 확인 완료 (K 컬럼, tmp_id NOT NULL). 임시 N 건
[RecreateTable] [load] 컬럼 타입 대조 (신규 vs 임시):
    ts                       timestamp_ntz        timestamp_ntz
    ...
    tmp_id                   string               (ora)
[RecreateTable] [ora] unmatched = M / N (x.xx%) → '' 로 채워진다
[RecreateTable] SQL> INSERT INTO iceberg.db.table_a SELECT t.ts, ..., COALESCE(CAST(o.tmp_id AS STRING), '') AS tmp_id, ... FROM iceberg.db.table_a_tmp t LEFT JOIN ora o ON t.key1 = o.key1 AND t.key2 = o.key2
[RecreateTable] [load 완료] iceberg.db.table_a = N 건, tmp_id='' M 건 (x.xx%)
```

`load` 시작 시 신규 테이블 건수가 0이 아니면(원본을 안 지웠거나 이미 load된 경우) 즉시 중단된다. INSERT는 Iceberg 단일 커밋이라 중간에 실패해도 신규 테이블은 비어 있다.

### 6.6 Trino 확인 → Airflow 재개 → 임시 정리

HMS 경유라 Trino 쪽 별도 조치는 없다.

```sql
-- Trino
SHOW CREATE TABLE iceberg.db.table_a;               -- tmp_id 위치 · NOT NULL
SELECT count(*) FROM iceberg.db.table_a;            -- == N
SELECT count(*) FROM iceberg.db.table_a WHERE tmp_id = '';   -- == M
```

Airflow 쓰기 DAG을 재개한다. 며칠 뒤 임시 테이블을 정리한다 — **PURGE 없이 DROP 하면 데이터 파일이 남는다.**

```sql
DROP TABLE iceberg.db.table_a_tmp PURGE;
```

---

## 7. 실패 시

| 어디서 | 상태 | 조치 |
|--------|------|------|
| `backup` | 임시 테이블이 없거나 건수 불일치 | 임시 `DROP ... PURGE` 후 재실행 |
| `check` | 아무것도 안 바뀜 | `OracleQuery`·`JoinKeys` 수정 후 재실행 |
| 수동 CREATE | 원본 없음, 임시 있음 | DDL 수정해 다시 CREATE (부분 생성분은 `DROP ... PURGE`) |
| `load` INSERT 중 | 신규 테이블 비어 있음 (단일 커밋) | 원인 수정 후 `load` 재실행 |
| `load` 사후 검증 | 신규 테이블에 데이터 있음 | 원인 확인. 재적재는 신규 `DROP ... PURGE` → CREATE → `load` |

**원래 스키마로 되돌리려면**: 6.4의 DDL에서 `tmp_id` 줄을 뺀 채 CREATE 하고 `INSERT INTO iceberg.db.table_a SELECT * FROM iceberg.db.table_a_tmp`. 임시 테이블은 파티션·Sort Order가 없는 CTAS 결과이므로 rename 해서 그대로 쓰지 않는다.

---

## 8. 주의

- **`restartPolicy: Never`** — 각 모드가 멱등이 아니므로 operator 재시도가 있으면 안 된다 (`backup`은 임시 존재로, `load`는 신규 COUNT > 0으로 막히긴 한다)
- **snapshot 이력·UUID 초기화** — 재처리 DAG의 `batch_id` 영수증(`.snapshots` summary)도 사라진다. 재생성 직전 구간에 FAILURE 재적재 대기 건이 있으면 재처리 DAG 쪽 확인이 필요하다
- **`DROP TABLE ... PURGE`는 `gc.enabled=false`면 거부된다** (6.4의 0-1)
- **임시 테이블은 파티션·Sort Order 없는 평면 테이블**이다. `load` 시 신규 테이블의 `write.distribution-mode=range` + Sort Order에 따라 다시 정렬·분배되므로 무관하다
- **Oracle 접속정보를 커밋하지 않는다** — 1회성이라 코드에 두되 실행 후 되돌린다
- Oracle이 반환하는 컬럼명은 대문자(`KEY1`)지만 Spark SQL은 기본 대소문자 무시라 소문자로 참조해도 된다
