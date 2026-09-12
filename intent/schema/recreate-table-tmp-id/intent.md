---
title: Iceberg 테이블 재생성 + tmp_id(NOT NULL) 추가
status: approved
category: 스키마
originated: 2026-09-09
written: 2026-09-11
approved: 2026-09-12
record: retrospective        # 착수 후 작성. 2026-09-09~11 세션 기록과 절차서에서 복원
author: Seonghyeon Lee
drafted-by: Claude
---

# Intent: Iceberg 테이블 재생성 + `tmp_id`(NOT NULL) 추가

## Problem statement

- Oracle 원천의 `tmp_id`를 Iceberg 테이블(TABLE_A 및 같은 구조의 테이블들)에 **NOT NULL 컬럼**으로 추가해야 한다.
- Iceberg는 비어 있지 않은 테이블에 required 컬럼을 추가하지 못한다. Spark `ADD COLUMN ... NOT NULL`, `SET NOT NULL`, `DEFAULT`, Trino 모두 거부한다. 그래서 컬럼 추가가 아니라 **테이블 재생성**이 된다.
- 기존 row에는 `tmp_id` 값이 없으므로 Oracle에서 조인해 채워야 하고, Oracle에 키가 없는 row는 `''`로 채운다.
- 현재 상태(2026-09-11): 절차서와 코드(약 40줄)는 완성. 개발 클러스터에서 `backup` 통과, `load`는 `AnalysisException UNRESOLVED_COLUMN t.tmp_id`로 실패 — 원인 미확정. 운영 미실행.

## Proposed outcome

- 대상 테이블이 `tmp_id STRING NOT NULL`을 가진 채 기존 데이터를 전부 보존한다. `backup` 원본 vs 임시, `load` 신규(원본 컬럼만) vs 임시가 각각 건수 일치 + `EXCEPT ALL` 차집합 0. `tmp_id`는 Oracle 키가 있으면 Oracle 값, 없으면 `''`(불일치 0건).
- 파티션(`hour(ts)`, `par_a`)·Sort Order(`sort_a`, `sort_b`)·TBLPROPERTIES는 재생성 전과 같다.
- append DAG이 재개돼 정상 적재한다. Trino `SHOW CREATE TABLE`에 `tmp_id`가 보이고, `tmp_id = ''` 건수가 Oracle에 키 없는 row 수(U)와 같다.
- 같은 코드·절차로 다른 테이블도 처리한다 (테이블명은 실행 인자).

## Affected users and systems

| 대상 | 영향 |
|------|------|
| Iceberg 대상 테이블 + 임시 `<table>_tmp` | DROP PURGE 후 재생성. snapshot 이력·UUID 초기화 |
| Airflow append DAG (대상 테이블) | 절차 동안 중지 → 완료 후 재개 |
| Spark 앱 + SparkApplication | 클래스 1개(`RecreateTable`) + pom ojdbc8. `mainClass`/`arguments`/`restartPolicy: Never`만 변경 |
| Oracle | SELECT만. driver·executor 양쪽에서 JDBC 접근 |
| Trino 조회 사용자 | DROP부터 `load` 커밋까지 테이블이 없거나 비어 있음 (절차서에 안내 없음) |
| 재처리 DAG (운영·maintenance 작업) | `.snapshots` batch_id 영수증 소실. 아직 미배포라 지금은 영향 없음 |
| 문서 | `pipeline/recreate-table-tmp-id.md` |

## Constraints

사용자 결정 (2026-09-09~11):

- **짧게 유지한다.** 1회성 작업이라 코드·설명이 많으면 확인이 어렵다. 검증 로직·모드·매니페스트를 늘리지 말 것
- 검수는 별도 모드로 빼지 않고 `backup`/`load` 안에 둔다
- 코드 변경은 "변경 전/후" 대비로 정리해서 전달한다
- Oracle 조회는 `dt` 범위를 주 단위 균일 chunk로 병렬 조회한다. 7월 이전(파티션 없음)은 느리지만 별도 처리·hash 분할·PARALLEL 힌트 없이 그대로 둔다
- Iceberg 읽기에는 파티션 키 WHERE 필수 → 모든 조회에 `ts` 하한(전체 범위)

기술·운영 제약:

- 스택은 Spark 3.5.8 / Scala 2.12.18 / Iceberg 1.10.1. `SELECT * EXCEPT (col)`은 Spark 4.0 문법이라 불가, 컬럼 목록은 임시 테이블 스키마에서 뽑는다
- 신규 요소는 Oracle JDBC뿐. Iceberg 접근·SparkApplication은 기존 것 그대로
- DROP/CREATE는 spark-sql 수동. 앱은 `backup` / `load` 두 모드
- Oracle 접속정보는 코드 하드코딩, **커밋 금지**
- `restartPolicy: Never` — 재시도되면 안 된다
- 임시 테이블은 파티션·Sort Order 없는 평면 CTAS. rename 재사용 금지
- DROP PURGE부터 롤백 없음. `gc.enabled=false`면 PURGE 거부
- Sort Order는 CREATE에 못 쓰므로 `ALTER TABLE ... WRITE ORDERED BY` 별도 실행

## Open questions

- **개발 `load` 실패 원인.** 유력 가설: 개발 신규 테이블이 `tmp_id` DDL로 재생성되지 않았다. 확인: 에러 직전 `[load 신규 vs 임시]` 로그 유무, `DESCRIBE`. 방어 2건은 PR #64에 반영됨
- **`tmp_id`가 필요한 이유** — 어떤 조회·조인에 쓰는가. 기록 없음
- **대상 테이블 목록과 순서**
- **자리표시자 실제값**: 테이블명, 조인 키 `key1`/`key2`(19컬럼 중 어느 것), Oracle 원천, `DtFrom`/`DtTo`/`ChunkDays`가 실제값인지·테이블별로 바뀌는지, `tmp_id`의 Oracle 타입(VARCHAR2 아니면 `TO_CHAR`), `tmp_id` 컬럼 위치
- **재생성 후 보존 확인 기준.** DDL이 `SHOW CREATE TABLE` 복사에 의존하는데, 보존해야 할 TBLPROPERTIES 목록(`write.distribution-mode=range`, `write.target-file-size-bytes`, `write.parquet.compression-codec`, array metrics `none` 8개, `gc.enabled`)이 절차서에 없고 운영 테이블의 현재 값도 기록에 없다
- **Airflow 중지 범위**: append DAG만인지, Compaction·expire·orphan·rewrite manifests DAG도 포함인지
- **재생성 소요 시간과 Trino 조회 불가 구간** — 사용자 안내가 필요한지
- Oracle 방화벽이 driver·executor 양쪽에 열려 있는지
- ojdbc8·`RecreateTable`이 실제 앱 저장소·이미지에 반영됐는지 (컴파일 검증은 scratchpad sbt 환경이었고 세션 종료로 사라짐)
- 재처리 DAG 배포와의 순서 (재생성이 먼저면 영수증 소실은 무관)
- 스키마 설계에서 미결인 항목(`ts`/`par_a`/`col_a` NOT NULL, 필터 컬럼 metrics `full`)을 이번 재생성에 같이 넣을지 — "짧게" 제약과 충돌
- 임시 테이블 삭제 시점("며칠 뒤")
- `hours(ts)` vs `hour(ts)` 표기 — `SHOW CREATE TABLE` 출력을 그대로 쓰면 무관
