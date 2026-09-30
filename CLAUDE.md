# Spark + Iceberg 파이프라인 가이드

이 파일은 **규칙·공통 맥락·작업별 확정값과 결정**만 담는다. 경위·근거·실측 상세는 각 작업의 문서에 있다 (작업 현황 표의 문서 열).

## Agents

서브에이전트는 별도 컨텍스트에서 실행되고 결과 보고서만 반환한다. 여러 문서를 훑는 조사·검증은 에이전트에 위임하는 것이 기본이다.

| Agent | Purpose | 도구 |
|-------|---------|------|
| `verify-column-naming` | TABLE_A 컬럼 명명 규칙 위반과 폐기된 옛 표기(`col_c`/`col_d`/`par_b`/`sort_c`) 잔존을 검사 | 읽기 전용 |
| `verify-doc-consistency` | 확정값(설정값·cron·버전·실측 수치)이 `CLAUDE.md` ↔ 상세 가이드 ↔ 보고용 요약 사이에서 일치하는지, 목표/운영 버전 구분, 미확인 마커 동기화를 검사 | 읽기 전용 |

1. **읽기는 병렬, 쓰기는 직렬.** 수정 적용은 메인 세션에서 한다
2. **에이전트는 이 대화의 맥락을 상속하지 않는다.** 세션에서 새로 정한 규칙은 호출 프롬프트에 직접 싣는다
3. **실측값 판정과 미확인 항목 확정은 에이전트에 맡기지 않는다.** 사실 수집은 에이전트, 판정은 메인 세션
4. **보고서를 무검증으로 신뢰하지 않는다.** 예외 0건, 위반 급증, 출처 없는 수치는 원문을 직접 확인한다

## Skills

| Skill | Purpose |
|-------|---------|
| `verify-implementation` | 검증 에이전트를 병렬 실행하고 verify 스킬을 순차 실행하여 통합 검증 보고서를 생성합니다 |
| `manage-skills` | 세션 변경사항을 분석하고, 검증 에이전트/스킬을 생성·업데이트하며, CLAUDE.md를 관리합니다 |

## 작성 규칙

- 커밋 메시지·결과값·설명은 한글. 기술 용어는 영어 원어 (Compaction, Bucketing, small file 등, 음차·번역 금지)
- Confluence 호환 마크다운 (표, 코드블록, 헤더, 인용블록)
- **설명 방식 (사용자 요청 2026-09-17)**: 결론 먼저, 그다음 **실측 숫자 하나를 잡아 그 숫자로 단계별 설명**. 지표는 무엇의 크기인지 먼저 정의하고, 상수(0.32 등)는 유도 과정을 보인다. 모범: `compaction-tuning-guide.md` §2.4, 설계서 §4.4 "쉽게 말하면", §8.1, §8.4
- **간결하게, AI 티 없이 (사용자 2026-09-29)**: 보고서체. 장황한 설명·같은 말 반복·줄표(—) 남발 금지. 사용자가 이해 못 한 개념은 용어를 바꾸지 말고 **구체적인 숫자 예시 하나**로 다시 설명한다(예: "1,440개 값"처럼 맥락 없는 수를 먼저 던지지 말 것)
- **담당자 설명용 문서에 비유 금지** (식당·선반·복도 등, 사용자 2026-09-21). 용도를 문장으로 나열한다
- 사용자가 정정한 사실은 즉시 이 파일에 반영한다 (예: 작업 10의 as-is/to-be 정의)

## 작업 진행 방식

- 저장소 반영은 **PR 생성 → squash merge까지** 한다. 사용자는 머지된 main의 문서·스크립트를 가져다 쓴다 (사용자 2026-09-28 "머지를 해야 내가 가져다 쓴다")
- 문서에 넣는 스크립트는 복사해서 바로 실행하는 용도다. 수정 후 문서의 코드 블록과 실제 실행본이 같은지 확인한다
- 접속정보·비밀번호·사용자가 비공개로 둔 이름(예: `TRIGGER_TABLE` 값)은 커밋하지 않는다

## 공통 컨텍스트

### 기술 스택

- **운영: Spark 3.5.8 / Scala 2.12 / Hadoop 3.3.4** (Spark 4에서 maintenance Scala 코드 오류로 임시 다운그레이드). **목표: Spark 4.1.1** — 문서의 "Spark 4.1.1"은 목표 버전이다 (작업 6 §5.0.2, 작업 7)
- Iceberg 1.10.1, Airflow 3.2.2, Trino 482 (DBeaver JDBC)
- Kubernetes (SparkKubernetesOperator, kubeflow), S3 = MinIO, 카탈로그 **HMS**
- Oracle DB — Job History `status`: `WAIT_SCHEDULING` → `IN_PROGRESS` → `SUCCESS` / `FAILURE`. **DB 2개에 같은 스키마**, 복합키 값은 DB 간 유일 보장 없음(상태 UPDATE는 원천 DB로)
- 운영 Pod TZ = UTC (`ENV TZ` 추가 금지)

### 기존 시스템 (as-is)

- Hive 테이블 (ORC, HDFS 블록 128MB), 파티션 `dt` 1개
- **수직분할 4개 테이블 = '빅테이블'** (사용자 명명 2026-09-29). Iceberg 대상 TABLE_A는 그중 1개. hourly Compaction 튜닝 대상 1~4번 테이블과 같다. 같은 원천이라 시간당 row 수(약 340만)가 같고 컬럼 폭만 다르다. 단 append 입력 기준 1번은 2·3·4번 데이터에 다른 종류 데이터가 더해져 row·avro 수가 약 4% 많다(사용자 2026-09-30)

### 대상 테이블 (TABLE_A)

- 컬럼 19개 (timestamp_ntz, string, double, integer, array<integer>, array<double>, array<string>)
- 파티션 `hour(ts)`, `par_a` (B안) · **Sort Order `sort_a`, `sort_b` 확정** · Bloom Filter 불필요 · array 컬럼 8개 `write.metadata.metrics.column.*` = `none` · `write.distribution-mode` = `range`
- **명명 규칙 — 접두어가 역할이다**: `par_*` = 파티션, `sort_*` = Sort Order, `col_*` = 성능 최적화 역할 없음

| 컬럼 | 역할 | Pruning 단계 |
|------|------|--------------|
| `ts` | 파티션 `hour(ts)` | Partition Pruning |
| `par_a` | 파티션 identity | Partition Pruning |
| `sort_a` | **Sort Order 1순위**. `ts`의 문자열 사본 (`2026-08-19 16:21:12.466` → `'20260819162112466'`) | Data Skipping |
| `sort_b` | **Sort Order 2순위** | Data Skipping |
| `col_a` | 없음 | Row-level Filter |
| `col_b` | 없음 | Row-level Filter |

- 조회 패턴: 클라이언트가 6개 컬럼(ts, par_a, sort_a, sort_b, col_a, col_b)을 **항상 전부 WHERE에 넣는다**
- **⚠️ 2026-09-05에 전 문서 명명을 통일했다.** 그 이전 자료(회의 캡처·커밋 메시지)는 이름이 다르다:

| 예전 | 현재 |
|------|------|
| `col_a`/`col_b`/`col_c`/`col_d` (`tuning/` 계열) | `par_a`/`sort_a`/`sort_b`/`col_b` |
| `par_b`/`sort_c` (`schema/` 계열) | `col_a`/`col_b` |

  **`col_a`는 예전에 파티션 컬럼이었고 지금은 역할 없는 컬럼이다.** 예전 자료에서 `col_a`가 값 A/B/C/D로 나오면 현재의 `par_a`다
- `par_a` 분포 (2026-03-18): B 43.4%, C 43.1%, A 12.4%, D 1.0%

### 워크플로우

- Airflow DAG → avro read → Iceberg append (약 5분 주기. 벤치마크는 10분 주기 ~8GB). `get_jobs`의 JOB_HISTORY 조회 상한 200 row(변동 가능)는 1회 조회 상한일 뿐 5분치 row 수가 아니다(사용자 정정 2026-09-30). 5분치 규모는 `tuning/append-tuning-test.md` §3.2 SQL로 잰다. JOB_HISTORY row당 avro 약 25개(사용자)
- Compaction: hourly `45 * * * *`(직전 1시간치) + daily `0 1 * * *`(전일치), 모든 전략에서 필수

### 참고 공식 문서

- Spark 4.1.1 [Configuration](https://spark.apache.org/docs/4.1.1/configuration.html) · [SQL Performance Tuning](https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html) · [Running on Kubernetes](https://spark.apache.org/docs/4.1.1/running-on-kubernetes.html)
- [Iceberg Spark Configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)

## 작업 현황

| # | 작업 | 상태 | 문서 |
|---|------|------|------|
| 1 | Spark 튜닝 (append Job) | 재검증 중 (빅테이블 4개) | `tuning/spark-tuning-guide.md`, `tuning/append-tuning-test.md` |
| 2 | Iceberg 스키마 설계 | 확정 | `schema/iceberg-schema-design-guide.md`, `schema/read-performance-test.md` |
| 3 | Trino 쿼리 가이드 | 완료 | `schema/trino-query-guide.md` |
| 4 | 재처리 DAG | 운영 배포 | `pipeline/reprocessing-dag-design.md`, `pipeline/reprocess-flow.md`, `pipeline/dags/iceberg_reprocess.py` |
| 5 | hourly Compaction 튜닝 (빅테이블) | 운영 반영 | `tuning/compaction-tuning-guide.md`, `tuning/compaction-tuning-report.md`, `pipeline/compaction-executor-sizing-design.md` |
| 6 | FileIO 전환 (S3A → S3FileIO) | 운영 전환, 후속 대기 | `pipeline/s3fileio-migration-guide.md`, `pipeline/s3fileio-migration-report.md` |
| 7 | Iceberg 1.11.0 / Spark 4.1 업그레이드 검토 | 분석 완료 | `pipeline/s3fileio-migration-guide.md` §9 |
| 8 | Trino Partition Pruning 검증 | 완료 | `tuning/trino-iceberg-partition-pruning.md` |
| 9 | 테이블 재생성 + `tmp_id` 추가 | 운영 적용 중 | `pipeline/recreate-table-tmp-id.md`, `intent/schema/recreate-table-tmp-id/intent.md` |
| 10 | 일일 리소스 사용량 시각화 | 완료 | `pipeline/airflow-job-duration.md`, `pipeline/resource-timeline.md` |

## 작업 1: Spark 튜닝 가이드 (append Job)

- 7개 설정 확정 (가이드 §4.1: 3개는 벤치마크 검증, 4개는 일반 관행)
- **재개 (2026-09-30, 빅테이블 4개)**: 테스트는 SparkApplication CRD 직접 apply + 테스트용 복제 테이블 + 운영 5분치 고정 입력. duration은 DataFlint UI, Airflow 집계는 운영 적용 후. 여러 batch 크기는 DA로 추후 테스트(사용자)
- ⚠️ `parallelismFirst=true`(기본값)면 AQE 목표 크기 = `min(advisory 384MB, shuffle ÷ 총 core)` → **executor 수가 append 출력 파일 수를 정한다**. 리소스만 바꿔도 Compaction 입력이 바뀐다
- ⚠️ 가이드 §3.2의 parallelismFirst 메커니즘 설명(1MB/64MB 기준, 분할 불가)은 틀렸다(정정 박스 추가). range 분배는 샘플링 job 때문에 avro를 두 번 읽는다. `shuffle.partitions` 200은 파일 수에 영향 없음
- **현재 운영 설정 (사용자 2026-09-30, 빅테이블 4개 공통)**: driver 1 core/2g, executor 4 core/8g/**overhead 4g**. executor 수 = `get_jobs`의 `ceil(avro 총크기 ÷ 128MB × 1.5 ÷ 4)`(Spark DA 아님), 최근 10회 최대 1번 14·2번 8·3번 12·4번 12. `parallelismFirst`·`shuffle.partitions`·advisory·`spark.io.compression.codec` 기본값. 앱 인자 `tableName`, `inputFileName`(목록 텍스트 파일 경로), `batchId`. 앱 쓰기 옵션은 `snapshot-property.batch_id`뿐
- **`write.distribution-mode=range` 고정** (사용자 2026-09-30). 비교 후보는 L0(현재) / L1(`parallelismFirst=false`) / L2(false + advisory 192MB)
- 1번 가설 대조: 14대 × 4 = 56 core ↔ 실측 batch당 파일 58.6개. 튜닝 결과는 `get_jobs` 계수(1.5)로 환산해 반영
- **5분 입력 규모 실측 (2026-09-30, 두 DB 평균 합)**: 1번 avro 2,691개·4.04GiB(공식 13대), 2번 2,579개·2.56GiB(8대), 3번 3.96GiB(12대), 4번 3.88GiB(12대). DB2는 5분당 row 4개 수준. 상세 `append-tuning-test.md` §3.2
- Sort Order 없는 테이블은 `hash`가 유리(소스 확인, 미실측). 가이드 §3.3

## 작업 2: Iceberg 스키마 설계

- **확정**: 파티션 B안(`hour(ts)`, `par_a`) + Sort Order `sort_a`, `sort_b`, Bloom Filter 불필요
- 5개 전략 비교에서 **B안이 4개 케이스 전부 1위** (A안 대비 5~31% 빠름). 결과 수치는 스키마 설계 가이드에 있고 `read-performance-test.md` §3·§4는 빈 템플릿
- §5: Sort Order 4개 조합(B/B-1/B-2/B-3안) 성능 동일, Bloom Filter 효과 없음. 확정 Sort Order는 4개 조합 중 어느 것도 아니며 성능 외 기준으로 정했다 (§5 도입부 경고 반영)

## 작업 3: Trino 쿼리 가이드

- 대상 독자: Trino 쿼리 사용자(Pruning 비전문가). ts 필터링 방법, WHERE 필수 컬럼, 잘못된 패턴
- 작업 8 결과 반영 완료 (2026-09-05, 2026-09-07 재정정). 근거 계층은 작업 8 문서 — 이 가이드가 "무엇을 쓰라", 작업 8이 "왜 그런가와 경계"
- `ts =` 등가: 날짜 조회 목적 ❌ / 정확한 시각 지정 ✅. "비용이 Planning에 쌓인다"는 서술은 **철회**(실측 근거 없음), `ts`는 sort 조건이 빠질 때의 안전장치로 필요

## 작업 4: 재처리 DAG — 운영 배포 (2026-09-14 확인)

- 저장소의 `iceberg_reprocess.py`는 설계 시점 스켈레톤이며 운영 코드와 다를 수 있다
- **확정값**: DAG 1개, 1일 1회 **04:00 KST**(`RUN_HOUR`), 테이블별 TaskGroup 순차. 조회 범위 FAILED = 전날+그저께 전체, WAIT = 전날 04:00 이전. 상한 테이블당·DB당 row 1,000(재검증 필요), 초과 시 자기 재trigger 최대 10회(`max_active_runs=1`). 좀비 `IN_PROGRESS` 2시간 초과 = 탐지·알림만
- **maintenance 스케줄**: hourly Compaction `45 * * * *`, daily Compaction 01:00, expire snapshots 03:00, 재처리 04:00, remove orphan files 05:00, rewrite manifests 06:00(3일마다). 실측 duration hourly 10~12분, daily 30~60분, expire 6~12분, orphan 5~9분, manifests 2~3분
- ⚠️ `wait_bound`는 `RUN_HOUR`를 따라가야 한다 (실행 시각만 옮기면 빈 구간 발생)
- ⚠️ batch_id는 `stat_desc` CLOB 재사용 — **WHERE 조건 사용 금지**. 상태 UPDATE는 batch_id를 준 호출에서만 `stat_desc` 갱신
- ⚠️ Compaction DAG `tables` params는 선언만으로 동작 안 함 → mapped task 전환 필요 (`pipeline/examples/compaction_dag_example.py`)
- ⚠️ 전제: snapshot 보존 3일 > 재처리 조회 2일. `remove_orphan_files`의 `older_than` 기본 3일 확인. `HadoopCatalog`로 바꾸면 동시 커밋 안전 전제가 깨진다
- 후속: daily 계열 maintenance를 DAG 1개의 순차 task로 통합

## 작업 5: hourly Compaction 튜닝 (빅테이블) — 운영 반영 완료

- **DAG 반영 완료** (사용자 확인 2026-09-29, 반영 시점 미기록). 반영 후 Airflow 이력이 작업 10의 to-be다
- **확정 설정** (설계서 §5.5 "DAG 반영용 최종 설정" 표가 원본)

| 구분 | 설정 | 값 |
|---|---|---|
| Iceberg | rewrite 전략 | `sort` (미적용 시 조회 40% 저하, 필수) |
| | `max-concurrent-file-group-rewrites` | **12** (재처리 시간 수 × 4, 상한 16) |
| | `max-file-group-size-bytes` | 기본값 100GB |
| | `target-file-size-bytes` | 512MB (Compaction 출력 파일 크기를 정하는 유일한 설정) |
| | `rewrite-all` / `partial-progress` | `true` / `false` |
| | `advisory-partition-size` / `parallelismFirst` | 삭제 / 삭제 가능 (무효) |
| Spark | driver cpu | 2 |
| | executor core | 4 |
| | `dynamicAllocation.enabled` / `executorAllocationRatio` | `true` / **0.13** (전 테이블 공통) |
| | `spark.executor.instances` = `initialExecutors` = `minExecutors` | `ceil(시간당 GB × 0.32)` = **1번 12, 2번 8, 3번 12, 4번 12** |
| | `maxExecutors` | **36** |
| | executor memory | **1번 16g, 2번 20g, 3·4번 18g** |
| | executor `memoryOverhead` | **3g** (4개 공통), driver overhead는 기본값 |

- **실측 효과** (1번 테이블 9회): 초/GB 3.24 → 2.41(−26%), dcu/GB 0.00416 → 0.00219(−47%), idle cores 58% → 17%, DAG 10~12분 → 약 6분
- **판정 원칙**: 같은 테이블 안에서 spill 0 + dcu 최저 + task error 0% + executor 유실 0. **job 성공 여부로 판정하지 말 것**, duration만 보면 executor 축소를 "느려졌다"로 오판한다. 노이즈 기준선 15%
- ⚠️ `spark.executor.instances`가 남아 있으면 그 값이 시작 대수의 바닥이 된다 (`max(initial, min, instances)`)
- ⚠️ `initialExecutors` 기본값 0 — 반드시 명시
- ⚠️ ratio는 파일 크기에 의존한다. append 설정이 바뀌면 에러 없이 어긋난다 (재검증 조건 1순위)
- ⚠️ `memoryOverhead`는 메모리 지표로 못 잰다. 시행착오 + task error rate가 유일한 판정 (1g 실패, 2g executor 유실, 3g 정상)
- ⚠️ **daily Compaction은 이 작업의 대상이 아니다.** 예전 "888GB·rewrite-all 낭비 의심"은 환산 오류로 폐기(2026-09-16). 재처리 설계서를 근거로 Compaction 대상을 추론하지 말 것. `C=0.32`·ratio 0.13은 hourly 전용
- 보류: `spark.memory.fraction` 0.8 실험(2번 16g 가능성). 안 함: shuffle codec zstd, `partial-progress=true`, target-file-size·core 변경
- 미확인: executor `spark-local-dir-1` 사용량(예상 5GiB/executor, 운영 첫 실행에서 1회 확인), 메모리 사용률 92.6~97.8%(spill 0이면 조치 안 함), 2번 spill 압축 팽창 요인, 3번 row당 비용이 높은 컬럼, Trino `$partitions`의 `partition.ts_hour` 타입
- 보류안: C안(Trino 사전 산정, `pipeline/examples/compaction_executor_sizing_example.py`)

## 작업 6: FileIO 전환 (S3AFileSystem → S3FileIO) — 운영 전환 완료 (2026-08-27)

- **실측** (가이드 §6.5): `deleteObject` 481 → 17.4 req/s(−96.4%), `listObjectV2` 680 → 281 req/s(−58.7%), expire snapshots duration 13.5분 → 3.8분(−72%), dcu 0.2002 → 0.0550(−72.5%), DataFlint alert 18 → 6. MinIO checksum 문제 없음
- **jar**: `iceberg-aws-bundle-1.10.1.jar` **단일 jar**를 이미지 `$SPARK_HOME/jars/`에 추가. 기존 `aws-java-sdk-bundle`(v1)은 **유지**(S3A가 사용). 개별 `awssdk:*` jar와 섞지 말 것
- **결정**: `s3.staging-dir` 미명시, `s3.multipart.part-size-bytes` 미명시(기본 32MB), `coreLimit=1` 적용
- ⚠️ **`fs.s3a.*`는 지우면 안 된다** — `spark.eventLog.dir`이 `s3a://`라 SparkContext 생성 시 죽고(실측), 원천 avro read도 S3A다. `s3://` 스킴으로 바꿔도 안 된다. 두 설정 공존은 구조적이다
- ⚠️ 이미지에 Hadoop 3.4.x 교체·`ENV TZ` 추가 금지. 베이스는 현재 운영 태그
- ⚠️ `fs.s3a.acl.default=PublicReadWrite`는 `s3.*`로 옮기지 말 것. `client.region`은 필수
- ⚠️ 계측 순서: baseline 계측을 `coreLimit` 변경보다 먼저
- 미확인: 실제 삭제 파일 수, Compaction/append 읽기·쓰기 성능 영향, `fs.s3a.acl.default`의 MinIO 실제 효력
- 후속(전부 보류, 서로 독립): ① maintenance Job 리소스 축소(idle cores 90%, executor 4 → 2) ② 자격증명 통합(§1.0.1 Q6) ③ `acl.default` 제거 검토 ④ `remove_orphan_files` `prefix_listing` ⑤ Compaction/append dcu/GB 비교 ⑥ 작업 7

## 작업 7: Iceberg 1.11.0 / Spark 4.1 업그레이드 검토

- **결론: 찬성.** 단 FileIO 전환 측정 완료 후 진행
- Iceberg 1.11.0이 Spark 4.1을 정식 지원한다 (1.10.1은 `spark/v4.0`까지 → 기존 Spark 4 오류의 유력한 원인). 테이블 포맷 영향 없음
- **목표 jar**: `iceberg-spark-runtime-4.1_2.13-1.11.0` + `iceberg-aws-bundle-1.11.0`
- 위험: Hadoop 3.4.2로 S3A SDK v1 → v2(MinIO checksum이 avro read까지 확산), `Remove deprecations`(#14059, Scala 코드의 Iceberg API 직접 참조 여부가 공수 결정), Trino 호환성, 작업 1·5 튜닝값 재검증, Scala 2.12/2.13 확인
- 권장 순서: ① 현 스택 Phase 1 측정 ② 결과 확정 ③ Scala 코드 API 참조 조사 ④ 1.11.0 + Spark 4.1.1 업그레이드 ⑤ Trino·벤치마크 회귀 검증

## 작업 8: Trino Partition Pruning 검증 (Trino 482)

- 일 → 시 단위 splits 4,938 → 205 (24.1배, 시간 파티션 24개와 일치)
- `EXPLAIN`의 Domain 표시는 Pruning 여부 지표가 아니다. Issue #19266 워크어라운드 불필요(469에서 종결)
- 운영 쿼리 패턴(6개 컬럼 전부 WHERE)은 `ts` 유무와 무관하게 16.79MB / 15 splits
- manifest 단계 Pruning은 `ts` 전용 (29개 중 1~2개만 열림, `sort_a`만으로는 전부). `ts`는 sort 조건이 빠질 때의 안전장치: 98,458 파일/13.65GB → 1,059 파일/264MB
- ⚠️ `sort_a`가 `ts` 사본이라서 생기는 관측이다 — 다른 테이블에 일반화 금지
- ⚠️ `date_trunc('week'|'quarter')`는 482에서 Pruning 안 됨(484에서 해결) — 우회 코드를 쿼리에 영구히 박지 말 것
- ⚠️ 파티션 필터 강제(`iceberg.query-partition-filter-required`)는 `ts` 누락을 못 막는다 (`par_a`만으로 통과)
- ⚠️ `read.split.target-size`(128MB)를 쓰기 512MB에 맞춰 올리지 말 것 (병렬성 4~5배 하락)
- splits ÷ 4 환산 폐기, `dataFiles`를 직접 인용
- 미확인: `par_a` 분포 순위 차이 원인, `iceberg.query-partition-filter-required` 실제 설정값, `$partitions` 대조, 규모 증가 시 manifest 전수 조회 비용

## 작업 9: Iceberg 테이블 재생성 + `tmp_id`(NOT NULL) 추가 — 운영 적용 중

- **대상은 빅테이블 4개와 다른 테이블이다** (사용자 정정 2026-09-29). intent의 "Sort Order 미적용"은 이 테이블 얘기이고, 빅테이블 4개는 파티션 2개·Sort Order 2개·`range` 적용 상태다
- `intent/`는 사이트 게시 제외(`mkdocs.yml`)라 링크 대신 경로 텍스트로 적는다
- 절차서는 **의도적으로 짧게 유지**한다 (1회성, 사용자 2026-09-09). 검증 로직·모드·매니페스트를 늘리지 말 것. 코드 변경은 "변경 전/후" 대비로 전달
- 앱: `RecreateTable <backup|load> <테이블명>`, 임시 = `<테이블명>_tmp`. DROP/CREATE는 spark-sql 수동. Scala 2.12.18 / Spark 3.5.8 / Iceberg 1.10.1 컴파일 검증
- **사용자 결정 (유지)**: 검수(`EXCEPT ALL` 전수 + `require`)는 `backup`/`load` 안에 둔다 · `[Oracle 에 키 없는 row]`에 상한 `require` 없음 · Oracle 조회는 주 단위 균일 chunk · 별도 모드·함수 분리 안 함(PR #68 revert) · Oracle 접속 변경은 상수 수동 편집
- ⚠️ Oracle 접속정보는 코드 하드코딩 — **커밋 금지**
- ⚠️ Iceberg 읽기에는 파티션 키 WHERE 필수 (모든 조회에 `ts` 하한)
- ⚠️ 재처리 DAG의 `.snapshots` batch_id 영수증이 재생성으로 사라진다 → 운영 전 최근 2일 `FAILURE`·`IN_PROGRESS` 0건 확인
- **현재 (2026-09-15)**: 운영 `backup`·`load` 완료, 일부 `tmp_id`가 `''`로 남아 **UPDATE 버전 `load` 재실행 대기**. 경위·다음 단계·재현 환경은 intent의 "진행 기록"

## 작업 10: 일일 리소스 사용량 시각화 — 완료

- **as-is / to-be (사용자 정정 2026-09-29)**: 튜닝은 **빅테이블 hourly Compaction DAG만** 했고 **운영 적용 완료**. as-is = 튜닝 전 기간의 Airflow 이력, to-be = 튜닝 적용 후부터 조회 시점까지의 Airflow 이력. **둘 다 실측**이다(테스트값·예상값 아님). **append cron도 바뀌었다**(사용자 2026-09-29): as-is는 일부 5분, 나머지 10·15·20분 주기 → to-be는 append 전부 5분. append의 리소스 설정은 같다. rewrite manifests(rw_mani) 운영 cron = `0 6 */3 * *` (작업 4 설계와 일치)
- **집계** `job_durations.py` (Airflow REST API v2만, SQL 방법 삭제): Duration(task 시작~끝), Start Offset(cron 예정 시각 → 실제 시작), `scheduled` 실행만(재처리 trigger 제외). 예외 `TRIGGER_TABLE`·`TRIGGER_PARENT_DAG`: 수직분할 append 종료 후 trigger되는 테이블 1개의 `append_data` task만 부모 cron 기준으로 집계 (값은 사용자가 비공개로 입력, 저장소는 빈 값). rewrite manifests dag_id = `iceberg_rewrite_manifests`
- **시각화** `resource_timeline.py`: 작업 엑셀 `AS-IS`·`TO-BE` 시트(같은 파일의 `AS-IS(x)`·`DIFF`는 무시) → `<원본>_resource_diff.xlsx`·`.html`. 열: A job_type(병합) · B cron · C app name · K 토탈 cpu · L 토탈 메모리 · M~S = CSV F~L · T 기능 요약(병합)
- **지표**: 평균 사용량 = 실행 횟수 × 1회 실행 시간(분) × 코어 ÷ 1,440분 (분당 평균 사용 코어, 튜닝 효과 판단 기준) · 최대 사용량 = 동시 사용 최댓값(클러스터 확보 기준). 동시 사용량은 6초 간격(입력 정밀도 0.1분 = 6초)으로 재고, 그래프는 5분 단위 최댓값
- 보고는 HTML 기준, 엑셀은 근거 자료로 함께 보관
- 결과 문구 (사용자 2026-09-29): 보고서체. '붐빈다'(→ 최대 사용), 초보 설명('파랑이 회색보다 낮은 만큼'), '순간의 값을 이었다', 쓸모없는 안내·생성 도구 문구 금지
- 사용자 작업 엑셀은 사내 DRM → Windows Python + xlwings로 엑셀 경유 읽기 (WSL 불가). 실제 엑셀 경유 읽기는 사용자 첫 실행으로만 검증 가능

## 파일 구조

```
├── CLAUDE.md
├── README.md                          # 문서 사이트 홈 (MkDocs index)
├── mkdocs.yml                         # MkDocs Material 설정 (docs_dir = 저장소 루트)
├── requirements-docs.txt              # 문서 사이트 빌드 의존성
├── assets/extra.css                   # 문서 사이트 스타일 보정 (한글 줄바꿈, 표 폭)
├── assets/dracula.css                 # Dracula 색상 스킴 (기본 테마, 토글로 라이트 전환)
├── .github/workflows/docs.yml         # main push 시 GitHub Pages 배포
├── .claude/
│   ├── agents/
│   │   ├── verify-column-naming.md    # 컬럼 명명 검증 (읽기 전용)
│   │   └── verify-doc-consistency.md  # 문서 간 확정값 동기화 검증 (읽기 전용)
│   └── skills/
│       ├── verify-implementation/     # 통합 검증 (에이전트 병렬 + 스킬 순차)
│       └── manage-skills/             # 검증 항목 유지보수
├── intent/                            # 작업별 intent 확정본 (사이트 게시 제외)
│   └── schema/recreate-table-tmp-id/intent.md
├── tuning/
│   ├── spark-tuning-guide.md          # Spark 튜닝 가이드 (append Job)
│   ├── append-tuning-test.md          # append Job 튜닝 테스트 절차 (빅테이블, CRD 직접 apply)
│   ├── compaction-tuning-guide.md     # Compaction 튜닝 가이드 (hourly, 상세)
│   ├── compaction-tuning-report.md    # Compaction 튜닝 결과 (보고용 요약)
│   └── trino-iceberg-partition-pruning.md  # Trino Partition Pruning 검증 (조회 경로 근거)
├── schema/
│   ├── iceberg-schema-design-guide.md  # Iceberg 스키마 설계 가이드
│   ├── read-performance-test.md        # 파티션 전략별 읽기 성능 비교 테스트
│   ├── spark-query-metrics-guide.md    # Spark 쿼리 메트릭 가이드
│   └── trino-query-guide.md            # Trino 쿼리 가이드 (사용자용)
└── pipeline/
    ├── s3fileio-migration-guide.md      # FileIO 전환 가이드 (S3A → S3FileIO, 상세)
    ├── s3fileio-migration-report.md     # FileIO 전환 결과 (보고용 요약)
    ├── images/                          # 보고용 Grafana 캡처
    ├── reprocessing-dag-design.md      # 재처리 DAG 설계 가이드
    ├── reprocess-flow.md               # 재처리 DAG 처리 흐름 (보고용 요약)
    ├── compaction-executor-sizing-design.md  # Compaction executor 자원 할당 설계 (DA+ratio)
    ├── dags/
    │   └── iceberg_reprocess.py        # 재처리 DAG 정의 (신규 파일은 이것 하나)
    ├── recreate-table-tmp-id.md        # 테이블 재생성 + tmp_id(NOT NULL) 추가 절차서 (작업 9)
    ├── airflow-job-duration.md         # job별 Duration·Start Offset 집계 (작업 10)
    ├── resource-timeline.md            # 하루 리소스 사용량 as-is/to-be 비교 시각화 (엑셀 + HTML, 작업 10)
    └── examples/
        ├── convert_file_taskgroup_example.py  # ConvertFileTaskGroup 변경(builder 인자) 예시
        ├── compaction_dag_example.py          # Compaction DAG 변경(tables 필터 = mapped task) 예시
        └── compaction_executor_sizing_example.py  # Compaction 사전 산정 예시 (보류안)
```
