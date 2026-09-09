import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Iceberg 테이블 재생성 + `tmp_id STRING NOT NULL` 추가 — Spark 앱이 맡는 부분.
 *
 * 전체 절차 중 DROP / CREATE 는 spark-sql 로 수동 실행한다 (k8s/../manual-ddl.sql 참조).
 * 이 앱은 그 앞뒤만 담당한다:
 *
 *   backup — 기존 테이블 → 임시 테이블 CTAS, 건수 일치 require. SHOW CREATE TABLE 을 로그에 남긴다 (수동 DDL 작성용)
 *   check  — 쓰기 없음. Oracle 읽기 → 키 중복 require → 임시 테이블 대비 unmatched 집계
 *   load   — 임시 테이블 LEFT JOIN Oracle → 신규 테이블 INSERT → 건수·tmp_id 검증
 *
 *            (수동) DROP TABLE ... PURGE  →  CREATE TABLE ... (tmp_id STRING NOT NULL 포함)  →  WRITE ORDERED BY
 *
 * INSERT 컬럼 목록은 신규 테이블 스키마에서 읽어 만든다. tmp_id 자리에는
 * COALESCE(CAST(o.tmp_id AS STRING), '') 가 들어가고 나머지는 임시 테이블의 같은 이름 컬럼이다.
 * 따라서 설정할 것은 테이블 이름·Oracle 접속·조인 키뿐이다.
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
  val TmpIdCol     = "tmp_id"   // 신규 컬럼 (STRING NOT NULL). Oracle 쪽 컬럼도 같은 이름이라고 가정 — 다르면 OracleQuery 에서 AS 로 맞춘다
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

  // 신규 테이블에 tmp_id 가 있고 NOT NULL 인지
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
