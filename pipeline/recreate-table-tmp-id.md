# Iceberg 테이블 재생성 + `tmp_id`(NOT NULL) 추가

Iceberg는 비어 있지 않은 테이블에 NOT NULL 컬럼을 추가할 수 없다 (Spark `ADD COLUMN ... NOT NULL`, `SET NOT NULL`, `DEFAULT`, Trino 전부 불가). 그래서 **임시 테이블로 복사 → DROP → CREATE → 조인해서 INSERT** 로 다시 만든다.

이름은 전부 자리표시자다: `iceberg.db.table_a`, `table_a_tmp`, `key1`/`key2`, `ORA_SCHEMA.ORA_TABLE`.

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
import org.apache.spark.sql.SparkSession

object RecreateTable {
  val Tbl = "iceberg.db.table_a"        // 재생성 대상 (backup 의 원본, load 의 목적지)
  val Tmp = "iceberg.db.table_a_tmp"    // 임시 테이블

  val OracleUrl      = "jdbc:oracle:thin:@//oracle-host:1521/SERVICE"
  val OracleUser     = "ora_user"
  val OraclePassword = "ora_password"
  val OracleQuery    = "(SELECT key1, key2, tmp_id FROM ORA_SCHEMA.ORA_TABLE) t"
  val JoinOn         = "t.key1 = o.key1 AND t.key2 = o.key2"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    def count(q: String) = spark.sql(q).first().getLong(0)

    args(0) match {
      case "backup" =>
        spark.sql(s"CREATE TABLE $Tmp USING iceberg AS SELECT * FROM $Tbl")
        println(s"원본 ${count(s"SELECT COUNT(*) FROM $Tbl")} 건 / 임시 ${count(s"SELECT COUNT(*) FROM $Tmp")} 건")

      case "load" =>
        spark.read.format("jdbc")
          .option("url", OracleUrl).option("user", OracleUser).option("password", OraclePassword)
          .option("driver", "oracle.jdbc.OracleDriver").option("dbtable", OracleQuery).option("fetchsize", "10000")
          .load().cache().createOrReplaceTempView("ora")

        // 사전 확인 — 키 중복이 있으면 조인으로 행이 불어난다. unmatched 는 '' 로 채워진다
        println(s"ora ${count("SELECT COUNT(*) FROM ora")} 건 / 키 distinct ${count("SELECT COUNT(*) FROM (SELECT DISTINCT key1, key2 FROM ora)")} 건")
        println(s"unmatched ${count(s"SELECT COUNT(*) FROM $Tmp t LEFT JOIN ora o ON $JoinOn WHERE o.tmp_id IS NULL")} 건")

        // 신규 테이블 컬럼 순서대로 SELECT 생성. tmp_id 만 ora 에서, 나머지는 임시 테이블에서
        val cols = spark.table(Tbl).columns.map {
          case "tmp_id" => "COALESCE(o.tmp_id, '') AS tmp_id"
          case c        => s"t.$c"
        }.mkString(", ")
        spark.sql(s"INSERT INTO $Tbl SELECT $cols FROM $Tmp t LEFT JOIN ora o ON $JoinOn")

        println(s"임시 ${count(s"SELECT COUNT(*) FROM $Tmp")} 건 / 신규 ${count(s"SELECT COUNT(*) FROM $Tbl")} 건 / tmp_id='' ${count(s"SELECT COUNT(*) FROM $Tbl WHERE tmp_id = ''")} 건")
    }
    spark.stop()
  }
}
```

- Oracle에는 SELECT만 나간다. `createOrReplaceTempView`는 Spark 세션 안의 이름 등록일 뿐이다
- Oracle 컬럼명은 대문자로 오지만 Spark SQL은 대소문자를 구분하지 않는다. `tmp_id`가 VARCHAR2가 아니면 `OracleQuery`에서 `TO_CHAR(...) AS tmp_id`
- 실행은 기존 SparkApplication에서 `mainClass: RecreateTable`, `arguments: ["backup"]` / `["load"]`만 바꾼다. `restartPolicy`는 `Never`로 (재시도되면 안 된다)
- Oracle 방화벽은 driver·executor 양쪽에 열려 있어야 한다 (JDBC 읽기는 executor에서 실행된다)

## 실행

### 1. `backup`

```
원본 N 건 / 임시 N 건      ← 같아야 한다
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
ora M 건 / 키 distinct M 건          ← 같아야 한다 (다르면 Oracle 쿼리를 좁히고 다시)
unmatched U 건                       ← '' 로 채워질 건수. 납득되면 진행 (INSERT 전 출력)
임시 N 건 / 신규 N 건 / tmp_id='' U 건  ← N 일치, U 일치
```

INSERT는 Iceberg 단일 커밋이라 중간에 실패해도 신규 테이블은 비어 있다. 다시 돌리면 된다.

### 4. 마무리

```sql
-- Trino
SHOW CREATE TABLE iceberg.db.table_a;                          -- tmp_id 위치·NOT NULL
SELECT count(*) FROM iceberg.db.table_a WHERE tmp_id = '';     -- == U

-- 며칠 뒤
DROP TABLE iceberg.db.table_a_tmp PURGE;
```

## 주의

- 새 테이블은 snapshot 이력·UUID가 초기화된다. 재처리 DAG의 `.snapshots` batch_id 영수증도 사라진다
- `gc.enabled=false`인 테이블은 `DROP ... PURGE`가 거부된다 — `SHOW TBLPROPERTIES`로 확인
- 임시 테이블은 파티션·Sort Order 없는 평면 CTAS다. `load` 시 신규 테이블 설정으로 다시 분배되므로 무관하지만, rename 해서 그대로 쓰지는 않는다
- Oracle 접속정보는 커밋하지 않는다
