# Iceberg 테이블 재생성 + `tmp_id`(NOT NULL) 추가

Iceberg는 비어 있지 않은 테이블에 NOT NULL 컬럼을 추가할 수 없다 (Spark `ADD COLUMN ... NOT NULL`, `SET NOT NULL`, `DEFAULT`, Trino 전부 불가). 그래서 **임시 테이블로 복사 → DROP → CREATE → 조인해서 INSERT** 로 다시 만든다.

이름은 전부 자리표시자다: `iceberg.db`, `table_a`, `key1`/`key2`, `ORA_SCHEMA.ORA_TABLE`. 테이블명은 실행 인자로 받으므로 여러 테이블에 같은 코드를 쓴다.

## 절차

| 순서 | 누가 | 내용 |
|------|------|------|
| 1 | — | Airflow 쓰기 DAG 일시 중지 |
| 2 | 앱 `backup` | 원본 → 임시 테이블 복사 |
| 3 | spark-sql 수동 | 원본 `DROP ... PURGE` → `tmp_id`를 넣은 DDL로 재생성 |
| 4 | 앱 `load` | 임시 테이블 LEFT JOIN Oracle → 신규 테이블 INSERT |
| 5 | — | Trino로 확인 → DAG 재개 → 며칠 뒤 임시 테이블 `DROP ... PURGE` |

## 코드

기존 Spark 앱에 클래스 하나를 추가하고 `pom.xml`에 ojdbc8만 넣는다. Iceberg 접근은 기존 설정 그대로다.

```xml
<dependency>
  <groupId>com.oracle.database.jdbc</groupId>
  <artifactId>ojdbc8</artifactId>
  <version>23.9.0.25.07</version>
</dependency>
```

```scala title="RecreateTable.scala"
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.SparkSession

object RecreateTable {
  val Db = "iceberg.db"                        // 카탈로그.DB — 테이블명은 args 로 받는다
  val Where = "ts >= TIMESTAMP '1970-01-01'"   // Iceberg 읽기엔 파티션 키 조건 필수. 전수 대상이라 전체 범위

  val OracleUrl      = "jdbc:oracle:thin:@//oracle-host:1521/SERVICE"
  val OracleUser     = "ora_user"
  val OraclePassword = "ora_password"
  val OracleTable    = "(SELECT key1, key2, tmp_id, dt FROM ORA_SCHEMA.ORA_TABLE) t"
  val JoinOn         = "t.key1 = o.key1 AND t.key2 = o.key2"

  // dt('YYYYMMDD...') 범위를 ChunkDays 단위로 잘라 chunk 마다 커넥션 하나로 병렬 조회
  val DtFrom = "20260512"
  val DtTo   = "20260909"
  val ChunkDays = 7

  // 실행: RecreateTable <backup|load> <테이블명>
  def main(args: Array[String]): Unit = {
    val mode = args(0)
    val Tbl  = s"$Db.${args(1)}"               // 재생성 대상 (backup 의 원본, load 의 목적지)
    val Tmp  = s"${Tbl}_tmp"                   // 임시 테이블

    val spark = SparkSession.builder().getOrCreate()
    def count(q: String) = spark.sql(q).first().getLong(0)

    // 검수: 두 쿼리 결과가 row 단위로 같은가 — 건수 일치 + 차집합(EXCEPT ALL) 0 이면 중복까지 포함해 동일
    def assertSame(name: String, a: String, b: String): Unit = {
      val (ca, cb, diff) = (count(s"SELECT COUNT(*) FROM ($a)"), count(s"SELECT COUNT(*) FROM ($b)"), count(s"SELECT COUNT(*) FROM ($a EXCEPT ALL $b)"))
      println(s"[$name] $ca 건 vs $cb 건, 차이 $diff 건")
      require(ca == cb && diff == 0, s"[$name] 정합성 실패")
    }
    def assertZero(name: String, q: String): Unit = {
      val n = count(q); println(s"[$name] $n 건"); require(n == 0, s"[$name] 정합성 실패")
    }

    mode match {
      case "backup" =>
        spark.sql(s"CREATE TABLE $Tmp USING iceberg AS SELECT * FROM $Tbl WHERE $Where")
        assertSame("backup 원본 vs 임시", s"SELECT * FROM $Tbl WHERE $Where", s"SELECT * FROM $Tmp WHERE $Where")

      case "load" =>
        val fmt = DateTimeFormatter.ofPattern("yyyyMMdd")
        val chunks = Iterator.iterate(LocalDate.parse(DtFrom, fmt))(_.plusDays(ChunkDays))
          .takeWhile(!_.isAfter(LocalDate.parse(DtTo, fmt)))
          .map(d => s"dt >= '${d.format(fmt)}' AND dt < '${d.plusDays(ChunkDays).format(fmt)}'").toArray

        val props = new java.util.Properties()
        props.setProperty("user", OracleUser)
        props.setProperty("password", OraclePassword)
        props.setProperty("driver", "oracle.jdbc.OracleDriver")
        props.setProperty("fetchsize", "10000")
        spark.read.jdbc(OracleUrl, OracleTable, chunks, props).cache().createOrReplaceTempView("ora")

        // 사전 검수 ①: Oracle 에 같은 키가 2건 이상이면 조인으로 row 가 불어난다 → 0 이어야 한다
        assertZero("ora 키 중복", "SELECT COUNT(*) FROM (SELECT key1, key2 FROM ora GROUP BY key1, key2 HAVING COUNT(*) > 1)")
        // 사전 확인 ②: Oracle 에 키가 없는 row 는 tmp_id 를 못 받으므로 '' 가 들어간다. 그 건수를 INSERT 전에 보여 준다
        println(s"[Oracle 에 키 없는 row] ${count(s"SELECT COUNT(*) FROM $Tmp t LEFT JOIN ora o ON $JoinOn WHERE $Where AND o.tmp_id IS NULL")} 건 → tmp_id = ''")

        // 신규 테이블 컬럼 순서대로 SELECT 생성. tmp_id 만 ora 에서, 나머지는 임시 테이블에서
        val cols = spark.table(Tbl).columns.map {
          case "tmp_id" => "COALESCE(o.tmp_id, '') AS tmp_id"
          case c        => s"t.$c"
        }.mkString(", ")
        spark.sql(s"INSERT INTO $Tbl SELECT $cols FROM $Tmp t LEFT JOIN ora o ON $JoinOn WHERE $Where")

        // 사후 검수 ①: 신규 테이블에서 tmp_id 를 뺀 나머지 컬럼만 보면 임시 테이블과 완전히 같아야 한다
        //             (조인·INSERT 가 기존 데이터를 바꾸거나 누락·중복시키지 않았는지)
        // 사후 검수 ②: tmp_id 는 Oracle 에 키가 있는 row 면 Oracle 값, 없는 row 면 '' 여야 한다
        val orig = spark.table(Tmp).columns.mkString(", ")
        assertSame("load 신규 vs 임시", s"SELECT $orig FROM $Tbl WHERE $Where", s"SELECT $orig FROM $Tmp WHERE $Where")
        assertZero("tmp_id 불일치", s"SELECT COUNT(*) FROM $Tbl t LEFT JOIN ora o ON $JoinOn WHERE $Where AND NOT (t.tmp_id <=> COALESCE(o.tmp_id, ''))")
    }
    spark.stop()
  }
}
```

- **검수는 `EXCEPT ALL` 전수 비교다.** `A EXCEPT ALL B`는 A에는 있고 B에는 없는 row를 중복 개수까지 세어 돌려준다. 건수가 같고 차집합이 0이면 두 테이블은 row 단위로 완전히 같다. 양쪽을 전부 읽어 모든 컬럼으로 대조하므로 전수조사이며, array 컬럼과 NaN도 정확히 비교된다. 조건이 하나라도 어긋나면 `require`로 즉시 실패한다
- Iceberg 읽기에는 파티션 키 조건이 필수라 모든 조회에 `Where`(`ts` 하한)를 붙인다. 전수 대상이므로 전체 범위다
- Oracle에는 SELECT만 나간다. `createOrReplaceTempView`는 Spark 세션 안의 이름 등록일 뿐이다
- Oracle 조회는 `dt` 범위를 `ChunkDays` 단위로 잘라 chunk마다 커넥션 하나로 병렬 조회한다 (동시 커넥션 수 = executor 코어 합계). `dt`가 `YYYYMMDD`에 밀리초까지 붙은 문자열이라 `>=`/`<`로 잘라야 경계가 빠지지 않는다. 7월 이전은 파티션이 없어 chunk마다 같은 구간을 다시 훑으므로 느리지만 결과는 같다
- Oracle 컬럼명은 대문자로 오지만 Spark SQL은 대소문자를 구분하지 않는다. `tmp_id`가 VARCHAR2가 아니면 `OracleTable`에서 `TO_CHAR(...) AS tmp_id`
- 실행은 기존 SparkApplication에서 `mainClass: RecreateTable`, `arguments: ["backup", "table_a"]` / `["load", "table_a"]`만 바꾼다. 테이블명은 인자로 받고 임시 테이블은 `<테이블명>_tmp`다 — 다른 테이블도 같은 코드로 처리한다. `restartPolicy`는 `Never`로 (재시도되면 안 된다)
- Oracle 방화벽은 driver·executor 양쪽에 열려 있어야 한다 (JDBC 읽기는 executor에서 실행된다)

## 실행

### 1. `backup`

```
[backup 원본 vs 임시] N 건 vs N 건, 차이 0 건      ← 건수 일치 + 차집합 0. 아니면 앱이 실패한다
```

### 2. spark-sql 수동

```sql
SHOW CREATE TABLE iceberg.db.table_a;      -- 출력을 복사해 둔다

DROP TABLE iceberg.db.table_a PURGE;       -- PURGE 없으면 데이터 파일이 남는다. 여기부터 롤백 없음

-- 복사해 둔 DDL에 tmp_id 한 줄만 원하는 위치에 끼운다. 파티션·TBLPROPERTIES는 그대로
CREATE TABLE iceberg.db.table_a (
  ts      TIMESTAMP_NTZ,
  par_a   STRING,
  key1    STRING,
  key2    STRING,
  tmp_id  STRING NOT NULL,                 -- ★ 신규
  ...
)
USING iceberg
PARTITIONED BY (hours(ts), par_a)
TBLPROPERTIES ( ... );

ALTER TABLE iceberg.db.table_a WRITE ORDERED BY sort_a, sort_b;   -- Sort Order 는 CREATE 에 못 쓴다
```

### 3. `load`

```
[ora 키 중복] 0 건                              ← 0 이 아니면 실패. Oracle 쿼리를 좁히고 다시
[Oracle 에 키 없는 row] U 건 → tmp_id = ''       ← Oracle 에 키가 없어 '' 가 들어갈 row 수. 납득되는 수인지 본다 (INSERT 전 출력)
[load 신규 vs 임시] N 건 vs N 건, 차이 0 건      ← tmp_id 를 뺀 나머지 컬럼은 임시 테이블과 완전히 같다 (누락·중복·값 변경 없음)
[tmp_id 불일치] 0 건                            ← tmp_id 는 Oracle 에 키가 있는 row 면 Oracle 값, 없는 row 면 '' 이다
```

INSERT는 Iceberg 단일 커밋이라 중간에 실패해도 신규 테이블은 비어 있다. 다시 돌리면 된다. 사후 검수에서 실패하면 신규 테이블을 `DROP ... PURGE` 하고 CREATE부터 다시 한다.

### 4. 마무리

```sql
-- Trino
SHOW CREATE TABLE iceberg.db.table_a;                          -- tmp_id 위치·NOT NULL
SELECT count(*) FROM iceberg.db.table_a WHERE ts >= TIMESTAMP '1970-01-01' AND tmp_id = '';   -- == U

-- 며칠 뒤
DROP TABLE iceberg.db.table_a_tmp PURGE;
```

## 주의

- 새 테이블은 snapshot 이력·UUID가 초기화된다. 재처리 DAG의 `.snapshots` batch_id 영수증도 사라진다
- `gc.enabled=false`인 테이블은 `DROP ... PURGE`가 거부된다 — `SHOW TBLPROPERTIES`로 확인
- 임시 테이블은 파티션·Sort Order 없는 평면 CTAS다. `load` 시 신규 테이블 설정으로 다시 분배되므로 무관하지만, rename 해서 그대로 쓰지는 않는다
- Oracle 접속정보는 커밋하지 않는다
