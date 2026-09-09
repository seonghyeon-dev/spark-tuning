import org.apache.spark.sql.catalyst.plans.logical.CreateTable
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Iceberg 테이블 재생성 + `tmp_id STRING NOT NULL` 컬럼 추가 (1회성).
 *
 * Iceberg 는 비어 있지 않은 테이블에 required(NOT NULL) 컬럼을 추가할 수 없으므로
 * 백업 → DROP PURGE → CREATE → INSERT 로 재생성한다. tmp_id 값은 Oracle 을
 * 2개 키 컬럼으로 LEFT JOIN 해 채우고, 매칭이 없으면 `TmpIdDefault`('') 를 넣는다.
 *
 * 실행 모드 (첫 번째 인자, 생략 시 check):
 *   check  — 아무것도 쓰지 않는다. Oracle 읽기 + 키 중복 / unmatched 집계 + DDL·INSERT 정합성 검증.
 *            운영 테이블을 그대로 두고 3단계까지의 require 를 미리 통과시켜 보는 용도.
 *   run    — 전체 절차 (1 백업 → 2 Oracle → 3 검증 → 4 DROP/CREATE → 5 INSERT → 6 검증).
 *   resume — 4·5단계에서 실패해 백업만 남은 상태의 재개. 백업이 존재하고 대상 테이블이
 *            없어야 한다 (부분 생성된 테이블은 수동으로 DROP ... PURGE 한 뒤 실행).
 *
 * 4단계(DROP) 이후는 롤백이 없다. 3단계까지의 require 가 실질적 안전장치이므로
 * `run` 전에 반드시 `check` 를 한 번 통과시킨다.
 */
object RecreateTable {

  // ═══════════════════════════════════════════════════════════════════════
  // 설정 — 실행 전 이 블록만 채운다
  // ═══════════════════════════════════════════════════════════════════════

  // ── 대상 테이블 ─────────────────────────────────────────────────────────
  val Catalog  = "iceberg"                 // TODO: spark.sql.catalog.<이름> 과 동일
  val Database = "db"                      // TODO
  val Table    = "TABLE_A"                 // TODO
  val Tbl      = s"$Catalog.$Database.$Table"
  val Bak      = s"$Catalog.$Database.${Table}_bak_20260909"   // 며칠 후 수동 DROP ... PURGE

  // ── Oracle (1회성이므로 하드코딩) ────────────────────────────────────────
  val OracleUrl      = "jdbc:oracle:thin:@//<host>:1521/<service>"   // TODO
  val OracleUser     = "<user>"                                        // TODO
  val OraclePassword = "<password>"                                    // TODO
  // SELECT 만 실행된다. 키 컬럼 타입은 Iceberg 쪽과 맞춰 둘 것 (다르면 조인이 조용히 비어 unmatched 로 잡힌다).
  // Oracle 이 반환하는 컬럼명은 대문자(KEY1, ...)지만 Spark SQL 은 기본 대소문자 무시라 소문자로 참조해도 된다.
  val OracleQuery    = "(SELECT key1, key2, tmp_id FROM SCHEMA.TABLE) t"   // TODO
  val OracleFetchSize = "10000"

  // ── 조인 키: Iceberg 컬럼 -> ora 뷰 컬럼 ─────────────────────────────────
  val JoinKeys: Seq[(String, String)] = Seq("key1" -> "key1", "key2" -> "key2")   // TODO
  val TmpIdCol     = "tmp_id"
  val TmpIdDefault = ""      // unmatched 행에 넣는 값. NOT NULL 이므로 NULL 은 불가

  // ── 새 테이블 DDL ───────────────────────────────────────────────────────
  // 기존 `SHOW CREATE TABLE` 출력을 붙여 넣고 tmp_id 만 원하는 위치에 끼워 넣는다.
  // 파티션·TBLPROPERTIES 는 기존 값을 그대로 복사한다. Sort Order 는 CREATE 에 못 쓰므로 PostCreateDdls 로.
  val CreateDdl: String =
    s"""CREATE TABLE $Tbl (
       |  ts        TIMESTAMP_NTZ,
       |  par_a     STRING,
       |  key1      STRING,                 -- TODO: 실제 컬럼 목록으로 교체
       |  key2      STRING,
       |  $TmpIdCol STRING NOT NULL,         -- ★ 추가 컬럼 (원하는 위치)
       |  sort_a    STRING,
       |  sort_b    STRING,
       |  col_a     STRING,
       |  col_b     STRING
       |)
       |USING iceberg
       |PARTITIONED BY (hours(ts), par_a)
       |TBLPROPERTIES (
       |  'format-version' = '2',
       |  'write.distribution-mode' = 'range'
       |  -- TODO: write.target-file-size-bytes, write.metadata.metrics.column.* 등 기존 속성 복사
       |)""".stripMargin

  def postCreateDdls: Seq[String] = Seq(
    s"ALTER TABLE $Tbl WRITE ORDERED BY sort_a, sort_b"   // TODO: 기존 Sort Order 와 동일하게
  )

  // ── INSERT 의 SELECT 부분 ──────────────────────────────────────────────
  // 컬럼 순서는 CreateDdl 과 동일해야 한다 (실행 전 이름·순서를 자동 대조한다).
  // source 는 run/resume 에선 백업, check 에선 운영 테이블이 들어간다.
  def insertSelectSql(source: String): String =
    s"""SELECT
       |  t.ts,
       |  t.par_a,
       |  t.key1,
       |  t.key2,
       |  COALESCE(CAST(o.$TmpIdCol AS STRING), '$TmpIdDefault') AS $TmpIdCol,
       |  t.sort_a,
       |  t.sort_b,
       |  t.col_a,
       |  t.col_b
       |FROM $source t
       |LEFT JOIN ora o ON $joinCondition""".stripMargin

  // ═══════════════════════════════════════════════════════════════════════
  // 이하 로직 — 설정만 바꿔 쓰는 것이 원칙
  // ═══════════════════════════════════════════════════════════════════════

  def joinCondition: String =
    JoinKeys.map { case (t, o) => s"t.$t = o.$o" }.mkString(" AND ")

  private var spark: SparkSession = _

  def main(args: Array[String]): Unit = {
    val mode = args.headOption.getOrElse("check")
    require(Set("check", "run", "resume").contains(mode), s"알 수 없는 모드: $mode (check | run | resume)")

    spark = SparkSession.builder().appName(s"RecreateTable[$mode] $Tbl").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    log(s"모드 = $mode, 대상 = $Tbl, 백업 = $Bak")
    mode match {
      case "check"  => check()
      case "run"    => run(useExistingBackup = false)
      case "resume" => run(useExistingBackup = true)
    }
    spark.stop()
  }

  // ─────────────────────────────────────────────────────────────────────
  // check: 쓰기 없이 3단계까지 + DDL/INSERT 정합성
  // ─────────────────────────────────────────────────────────────────────
  def check(): Unit = {
    require(tableExists(Tbl), s"대상 테이블이 없다: $Tbl")
    require(!tableExists(Bak), s"백업 테이블이 이미 있다: $Bak — 이름을 바꾸거나 DROP 후 재실행")
    checkGcEnabled()
    validateDdlAndSelect(source = Tbl)

    val total = count(Tbl)
    log(s"[0] 현재 건수 = $total")
    logShowCreate(Tbl)

    loadOracle()
    val unmatched = verifyOracle(source = Tbl)
    log(s"[check 완료] 총 $total 건 중 unmatched $unmatched 건 (${pct(unmatched, total)}) 이 '$TmpIdDefault' 로 채워질 예정")
  }

  // ─────────────────────────────────────────────────────────────────────
  // run / resume: 전체 절차
  // ─────────────────────────────────────────────────────────────────────
  def run(useExistingBackup: Boolean): Unit = {
    // [0] 사전 검사 — 여기서 실패하면 아무것도 바뀌지 않는다
    val bakCount: Long =
      if (!useExistingBackup) {
        require(tableExists(Tbl), s"대상 테이블이 없다: $Tbl")
        require(!tableExists(Bak), s"백업 테이블이 이미 있다: $Bak")
        checkGcEnabled()
        validateDdlAndSelect(source = Tbl)
        logShowCreate(Tbl)

        // [1] 백업 CTAS + 건수 일치
        val srcCount = count(Tbl)
        log(s"[1] 백업 생성: $Bak (원본 $srcCount 건)")
        sql(s"CREATE TABLE $Bak USING iceberg AS SELECT * FROM $Tbl")
        val c = count(Bak)
        require(c == srcCount, s"[1] 백업 건수 불일치: 원본 $srcCount vs 백업 $c")
        log(s"[1] 백업 완료: $c 건")
        c
      } else {
        require(tableExists(Bak), s"resume: 백업 테이블이 없다: $Bak")
        require(!tableExists(Tbl), s"resume: 대상 테이블이 아직 존재한다: $Tbl — 부분 생성분은 수동 DROP ... PURGE 후 재실행")
        validateDdlAndSelect(source = Bak)
        val c = count(Bak)
        log(s"[1] resume: 기존 백업 사용 $Bak ($c 건)")
        c
      }

    // [2] Oracle → cache → temp view `ora`
    loadOracle()

    // [3] DROP 전 검증 — 마지막 안전장치
    val unmatched = verifyOracle(source = Bak)

    // [4] DROP PURGE → CREATE → 후속 DDL   ★ 이 지점부터 롤백 없음
    if (!useExistingBackup) {
      log(s"[4] DROP TABLE $Tbl PURGE")
      sql(s"DROP TABLE $Tbl PURGE")
      require(!tableExists(Tbl), s"[4] DROP 후에도 테이블이 남아 있다: $Tbl")
    }
    log(s"[4] CREATE TABLE $Tbl")
    sql(CreateDdl)
    postCreateDdls.foreach { ddl => log(s"[4] $ddl"); sql(ddl) }
    require(tableExists(Tbl), s"[4] CREATE 후 테이블이 없다: $Tbl")
    requireTmpIdRequired(spark.table(Tbl).schema)
    requireSameColumns(spark.table(Tbl).schema, spark.sql(insertSelectSql(Bak)).schema)

    // [5] INSERT (단일 커밋 — 실패 시 새 테이블은 비어 있다)
    log(s"[5] INSERT INTO $Tbl ... FROM $Bak LEFT JOIN ora")
    sql(s"INSERT INTO $Tbl\n${insertSelectSql(Bak)}")

    // [6] 검증
    val newCount = count(Tbl)
    require(newCount == bakCount, s"[6] 건수 불일치: 백업 $bakCount vs 새 테이블 $newCount")
    val defaulted = scalarLong(s"SELECT COUNT(*) FROM $Tbl WHERE $TmpIdCol = '$TmpIdDefault'")
    require(defaulted == unmatched, s"[6] '$TmpIdDefault' 건수 불일치: 예상(unmatched) $unmatched vs 실제 $defaulted")
    val nulls = scalarLong(s"SELECT COUNT(*) FROM $Tbl WHERE $TmpIdCol IS NULL")
    require(nulls == 0L, s"[6] $TmpIdCol NULL 이 $nulls 건 존재")
    logShowCreate(Tbl)

    log(s"[완료] $Tbl 재생성. 건수 $newCount, $TmpIdCol='$TmpIdDefault' $defaulted 건 (${pct(defaulted, newCount)})")
    log(s"[후속] ① Airflow 쓰기 DAG 재개  ② 며칠 뒤 `DROP TABLE $Bak PURGE`  ③ Trino 에서 스키마·조회 확인")
  }

  // ─────────────────────────────────────────────────────────────────────
  // [2] Oracle 읽기
  // ─────────────────────────────────────────────────────────────────────
  def loadOracle(): Unit = {
    log(s"[2] Oracle 읽기: $OracleQuery")
    val ora: DataFrame = spark.read
      .format("jdbc")
      .option("url", OracleUrl)
      .option("user", OracleUser)
      .option("password", OraclePassword)
      .option("driver", "oracle.jdbc.OracleDriver")
      .option("dbtable", OracleQuery)
      .option("fetchsize", OracleFetchSize)
      // 한 커넥션으로 읽는다. 수천만 건 이상이면 partitionColumn/lowerBound/upperBound/numPartitions 고려
      .load()
      .cache()
    ora.createOrReplaceTempView("ora")
    val n = ora.count()   // cache 실체화
    log(s"[2] ora = $n 건, 스키마: ${ora.schema.map(f => s"${f.name}:${f.dataType.simpleString}").mkString(", ")}")
    require(n > 0L, "[2] Oracle 조회 결과가 0건")
    JoinKeys.foreach { case (_, o) =>
      require(ora.columns.exists(_.equalsIgnoreCase(o)), s"[2] ora 에 조인 키 컬럼이 없다: $o")
    }
    require(ora.columns.exists(_.equalsIgnoreCase(TmpIdCol)), s"[2] ora 에 $TmpIdCol 컬럼이 없다")
  }

  // ─────────────────────────────────────────────────────────────────────
  // [3] DROP 전 검증: ora 키 중복 0 건 require, unmatched 집계
  // ─────────────────────────────────────────────────────────────────────
  def verifyOracle(source: String): Long = {
    val oraKeys = JoinKeys.map(_._2).mkString(", ")
    val dup = scalarLong(
      s"SELECT COUNT(*) FROM (SELECT $oraKeys FROM ora GROUP BY $oraKeys HAVING COUNT(*) > 1)")
    require(dup == 0L, s"[3] ora 키 중복 $dup 건 — 조인 시 행이 불어난다. Oracle 쿼리를 좁혀라")

    val oraNullKey = scalarLong(
      s"SELECT COUNT(*) FROM ora WHERE ${JoinKeys.map(k => s"${k._2} IS NULL").mkString(" OR ")}")
    val srcNullKey = scalarLong(
      s"SELECT COUNT(*) FROM $source WHERE ${JoinKeys.map(k => s"${k._1} IS NULL").mkString(" OR ")}")
    log(s"[3] 키 NULL — ora $oraNullKey 건, $source $srcNullKey 건 (NULL 키는 절대 매칭되지 않는다)")

    // 키 타입 대조 — 다르면 경고만. Oracle NUMBER 는 decimal 로 오므로 int 와 달라도 조인은 된다
    val srcSchema = spark.table(source).schema
    val oraSchema = spark.table("ora").schema
    JoinKeys.foreach { case (t, o) =>
      val st = srcSchema.find(_.name.equalsIgnoreCase(t)).map(_.dataType.simpleString).getOrElse("<없음>")
      val ot = oraSchema.find(_.name.equalsIgnoreCase(o)).map(_.dataType.simpleString).getOrElse("<없음>")
      require(st != "<없음>", s"[3] $source 에 조인 키 컬럼이 없다: $t")
      if (st != ot) log(s"[3] ⚠ 키 타입 다름: $source.$t=$st vs ora.$o=$ot — 암묵 CAST 로 조인됨. unmatched 가 비정상적으로 크면 여기부터 의심")
    }

    val total = count(source)
    val unmatched = scalarLong(
      s"SELECT COUNT(*) FROM $source t LEFT JOIN ora o ON $joinCondition WHERE o.$TmpIdCol IS NULL")
    log(s"[3] unmatched = $unmatched / $total (${pct(unmatched, total)}) → '$TmpIdDefault' 로 채워진다")
    unmatched
  }

  // ─────────────────────────────────────────────────────────────────────
  // DDL / INSERT SELECT 정합성 — DROP 전에 잡는다
  // ─────────────────────────────────────────────────────────────────────
  def validateDdlAndSelect(source: String): Unit = {
    val plan = spark.sessionState.sqlParser.parsePlan(CreateDdl)
    val ddlSchema: StructType = plan match {
      case c: CreateTable => c.tableSchema
      case other => throw new IllegalArgumentException(s"CreateDdl 이 CREATE TABLE 이 아니다: ${other.getClass.getSimpleName}")
    }
    requireTmpIdRequired(ddlSchema)

    // ora 는 아직 없을 수 있으므로 빈 뷰로 대체해 SELECT 를 분석만 한다 (실행 없음)
    if (!spark.catalog.tableExists("ora")) {
      val oraCols = (JoinKeys.map(_._2) :+ TmpIdCol).map(c => s"CAST(NULL AS STRING) AS $c").mkString(", ")
      spark.sql(s"SELECT $oraCols WHERE 1 = 0").createOrReplaceTempView("ora")
    }
    val selectSchema = spark.sql(insertSelectSql(source)).schema
    requireSameColumns(ddlSchema, selectSchema)

    val srcCols = spark.table(source).schema.fieldNames.map(_.toLowerCase).toSet
    val missing = ddlSchema.fieldNames.map(_.toLowerCase).filterNot(c => c == TmpIdCol.toLowerCase || srcCols.contains(c))
    require(missing.isEmpty, s"CreateDdl 에 $source 에 없는 컬럼이 있다: ${missing.mkString(", ")}")
    val dropped = srcCols.diff(ddlSchema.fieldNames.map(_.toLowerCase).toSet)
    require(dropped.isEmpty, s"$source 의 컬럼이 CreateDdl 에서 빠졌다: ${dropped.mkString(", ")}")

    log(s"[검증] CreateDdl(${ddlSchema.size}컬럼) ↔ INSERT SELECT 이름·순서 일치, $TmpIdCol NOT NULL 확인")
    log("[검증] 컬럼 타입 대조 (DDL vs SELECT):\n" +
      ddlSchema.zip(selectSchema).map { case (d, s) =>
        val mark = if (d.dataType == s.dataType) "  " else "⚠ "
        f"  $mark${d.name}%-24s ${d.dataType.simpleString}%-20s ${s.dataType.simpleString}"
      }.mkString("\n"))
  }

  def requireTmpIdRequired(schema: StructType): Unit = {
    val f: Option[StructField] = schema.find(_.name.equalsIgnoreCase(TmpIdCol))
    require(f.isDefined, s"스키마에 $TmpIdCol 이 없다")
    require(!f.get.nullable, s"$TmpIdCol 이 NOT NULL 이 아니다 — CreateDdl 에 NOT NULL 을 넣어라")
  }

  def requireSameColumns(expected: StructType, actual: StructType): Unit = {
    val e = expected.fieldNames.map(_.toLowerCase).toSeq
    val a = actual.fieldNames.map(_.toLowerCase).toSeq
    require(e == a,
      s"컬럼 이름·순서 불일치\n  DDL   : ${e.mkString(", ")}\n  SELECT: ${a.mkString(", ")}")
  }

  // DROP ... PURGE 는 gc.enabled=false 인 테이블에서 거부된다
  def checkGcEnabled(): Unit = {
    val gc = spark.sql(s"SHOW TBLPROPERTIES $Tbl").collect()
      .find(_.getString(0) == "gc.enabled").map(_.getString(1))
    require(!gc.contains("false"), s"$Tbl 은 gc.enabled=false — DROP PURGE 가 거부된다. 먼저 ALTER TABLE ... SET TBLPROPERTIES ('gc.enabled'='true')")
  }

  // ─────────────────────────────────────────────────────────────────────
  // 유틸
  // ─────────────────────────────────────────────────────────────────────
  def sql(q: String): DataFrame = { log(s"SQL> ${q.linesIterator.map(_.trim).filter(_.nonEmpty).mkString(" ")}"); spark.sql(q) }
  def scalarLong(q: String): Long = spark.sql(q).first().getLong(0)
  def count(t: String): Long = scalarLong(s"SELECT COUNT(*) FROM $t")
  def tableExists(t: String): Boolean = spark.catalog.tableExists(t)
  def logShowCreate(t: String): Unit =
    log(s"SHOW CREATE TABLE $t ↓\n${spark.sql(s"SHOW CREATE TABLE $t").first().getString(0)}")
  def pct(n: Long, total: Long): String = if (total == 0L) "n/a" else f"${n * 100.0 / total}%.2f%%"
  def log(msg: String): Unit = println(s"[RecreateTable] $msg")
}
