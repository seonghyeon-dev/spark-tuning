-- ─────────────────────────────────────────────────────────────────────────────
-- 수동 실행 구간 (spark-sql). 앱의 backup → check 가 끝난 뒤, load 전에 실행한다.
-- 이름·컬럼은 자리표시자 — backup 모드가 로그에 남긴 SHOW CREATE TABLE 출력을 기준으로 채운다.
-- ─────────────────────────────────────────────────────────────────────────────

-- 0. 확인: 임시 테이블 건수 == 원본 건수 (backup 로그와 대조)
SELECT COUNT(*) FROM iceberg.db.table_a;
SELECT COUNT(*) FROM iceberg.db.table_a_tmp;

-- 0-1. gc.enabled=false 면 PURGE 가 거부된다
SHOW TBLPROPERTIES iceberg.db.table_a;
-- ALTER TABLE iceberg.db.table_a SET TBLPROPERTIES ('gc.enabled' = 'true');

-- 1. 원본 DROP. PURGE 가 없으면 데이터 파일이 MinIO 에 남는다.  ★ 여기부터 롤백 없음 (임시 테이블이 유일한 사본)
DROP TABLE iceberg.db.table_a PURGE;

-- 2. 같은 이름으로 재생성. SHOW CREATE TABLE 출력을 붙여 넣고 tmp_id 한 줄만 원하는 위치에 끼운다.
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

-- 4. 확인: tmp_id 위치·NOT NULL, 파티션, Sort Order. 이어서 앱 load 모드 실행
SHOW CREATE TABLE iceberg.db.table_a;

-- ─────────────────────────────────────────────────────────────────────────────
-- load 완료·Trino 확인·DAG 재개 후 며칠 뒤
-- ─────────────────────────────────────────────────────────────────────────────
-- DROP TABLE iceberg.db.table_a_tmp PURGE;
