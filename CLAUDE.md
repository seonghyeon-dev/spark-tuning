# Spark + Iceberg 파이프라인 가이드

## Skills

| Skill | Purpose |
|-------|---------|
| `verify-implementation` | 프로젝트의 모든 verify 스킬을 순차 실행하여 통합 검증 보고서를 생성합니다 |
| `manage-skills` | 세션 변경사항을 분석하고, 검증 스킬을 생성/업데이트하며, CLAUDE.md를 관리합니다 |

## Code Style Rules

- 커밋 메시지는 한글로 작성
- 결과값과 설명은 무조건 한글로 작성
- 기술 용어는 영어 원어 사용 (Compaction, Bucketing, small file 등 — 한글 음차/번역 금지)
- Confluence 호환 마크다운 (표, 코드블록, 헤더, 인용블록 등)

## 공통 컨텍스트

### 기술 스택

- Spark 4.1.1, Iceberg 1.10.1, Airflow 3.2.2
- Kubernetes 클러스터 (Spark Pod 실행 환경)
- S3 (MinIO) 스토리지 — Iceberg 테이블 (카탈로그: **HMS**)
- Trino — 조회 엔진 (DBeaver JDBC 드라이버로 실행)
- Oracle DB (처리 대상 상태 관리)
  - Job History `status` 값: `WAIT_SCHEDULING` → `IN_PROGRESS` → `SUCCESS` / `FAILURE`
- SparkKubernetesOperator (kubeflow)

### 기존 시스템 (as-is)

- Hive 테이블 (ORC, HDFS 블록 128MB)
- 수직분할 4개 테이블 — Iceberg 대상(TABLE_A)은 그 중 1개
- 파티션: 날짜 1개 (dt=날짜)

### 대상 테이블 (TABLE_A)

- 컬럼 수: 19개 (timestamp_ntz, string, double, integer, array<integer>, array<double>, array<string>)
- 파티션: `hour(ts)`, `par_a` (B안 — 읽기 성능 테스트 최우수)
- Sort Order: 미확정 (테스트 결과 조합 간 성능 차이 없음 — 섹션 5 참조)
- Bloom Filter: 효과 없음, 설정 불필요 (테스트 확인)
- array 타입 컬럼 8개: `write.metadata.metrics.column.*` = `none`
- `write.distribution-mode`: `range`
- 조회 패턴: 클라이언트에서 6개 컬럼(ts, par_a, par_b, sort_b, sort_a, sort_c) **전부 WHERE에 항상 포함**. 성능 최적화 역할(Partition Pruning/Data Skipping/Row-level Filter)은 Sort Order 확정 후 결정
- par_a 분포 (실측, 2026-03-18 기준): B 43.4%, C 43.1%, A 12.4%, D 1.0% — 균등 분포 아님

### 워크플로우

Airflow DAG → avro read → Iceberg append (현재 약 5분 주기, 5분치 ≈ Job History 200 rows. 벤치마크는 10분 주기 ~8GB 기준)
Compaction: 1시간(`35 * * * *` → `45 * * * *`, 직전 1시간치) + 1일(`35 0 * * *` → `0 1 * * *`, 전일치) — 모든 전략에서 필수

### 참고 공식 문서

- Spark 4.1.1 Configuration: https://spark.apache.org/docs/4.1.1/configuration.html
- Spark 4.1.1 SQL Performance Tuning: https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html
- Spark on Kubernetes: https://spark.apache.org/docs/4.1.1/running-on-kubernetes.html
- Iceberg Spark Configuration: https://iceberg.apache.org/docs/latest/spark-configuration/

## 작업 1: Spark 튜닝 가이드 — 완료

- **산출물**: `tuning/spark-tuning-guide.md`
- **상태**: 7개 설정 확정, 벤치마크 검증 완료
- **대기**: 파티션/Sort Order 최종 확정 후 벤치마크 재검증

## 작업 2: Iceberg 스키마 설계 — B안 확정, Sort Order/Bloom Filter 테스트 완료

- **산출물**: `schema/iceberg-schema-design-guide.md`
- **상태**: B안 확정 (`hour(ts)`, `par_a`), Sort Order/Bloom Filter 테스트 완료
- **읽기 성능 테스트 결과** (`schema/read-performance-test.md`):
  - 섹션 1~4: Hive-raw, Hive-orc, A안, B안, C안 5개 전략 비교 완료. **B안이 4개 테스트 케이스 전부 1위** (A안 대비 5~31% 빠름)
  - 섹션 5: Sort Order/Bloom Filter 설정별 비교. 4개 조합 모두 동일 성능, Bloom Filter 효과 없음
- **Sort Order**: 미확정 (조합 간 성능 차이 없음)
- **Bloom Filter**: 설정 불필요 (테스트 확인)

## 작업 3: Trino 쿼리 가이드 — 완료

- **산출물**: `schema/trino-query-guide.md`
- **상태**: 완료
- **대상 독자**: Trino 쿼리 사용자 (Partition Pruning/Data Skipping 비전문가)
- **핵심 내용**: ts 필터링 방법(date, date_trunc, 범위 조건), WHERE 필수 컬럼, 잘못된 쿼리 패턴

## 작업 4: 재처리(Reprocessing) DAG 설계 — 설계 완료

- **산출물**: `pipeline/reprocessing-dag-design.md` (설계), `pipeline/reprocess-flow.md` (보고용 흐름 요약), `pipeline/dags/iceberg_reprocess.py` (구현 스켈레톤 — 기존 인프라 연결 지점은 TODO 표시)
- **상태**: 설계 확정, 구현 스켈레톤 작성 (기존 인프라 연결 대기)
- **배경**: append DAG의 Oracle 조회 기간(최근 1일 rolling — Job History `ts` 날짜 키 파티셔닝 제약)에서 밀려난 WAIT_SCHEDULING 데이터와, `get_jobs`가 조회하지 않는 FAILURE 데이터가 영구 잔류하는 문제
- **시스템 구조**: Iceberg 테이블 20개+ (hourly/daily 그룹), append DAG은 py 1개에서 테이블별 동적 생성(약 5분 주기, `ts` string `YYYYMMDDHHmmSSsss` 기준 ORDER BY ASC, ROWNUM 200), Compaction DAG은 hourly/daily 각 1개(내부 테이블별 task 순차). **Job History는 Oracle DB 2개에 동일 스키마로 존재 — conn_list loop로 DB별 동일 쿼리 실행, `job_id`는 DB 간 유일 보장 없음(상태 UPDATE는 원천 DB로)**
- **핵심 설계**:
  - 재처리 DAG **1개** (1일 주기, 04:00 KST — `RUN_HOUR` 상수), 테이블별 TaskGroup 순차 실행 (Compaction DAG 패턴)
  - 조회 범위 경계로 경합 원천 차단: FAILED는 전날+그저께 전체, WAIT는 전날 04:00 이전만 (append 하한 = 실행시각-24h ≥ 전날 04:00이므로 절대 안 겹침). **`wait_bound`는 `RUN_HOUR`를 따라가야 한다** — 실행 시각만 옮기면 그 사이 구간을 아무도 안 본다. 잠금/선점/pool 불필요
  - 상한: 테이블당 row 1,000 / 16GB (러프 설정, 재검증 필요). 초과 시 자기 자신 재trigger loop (상한 10회, `max_active_runs=1`로 순차)
  - 중복 적재 방지: snapshot summary에 batch_id 기록(영수증), FAILURE 재적재 전 `.snapshots` 확인 → 커밋된 건 SUCCESS 정정. batch_id는 `stat_desc` CLOB 재사용 — **WHERE 조건 사용 금지** (값 기록/읽기만). **상태 UPDATE는 batch_id를 준 호출에서만 stat_desc를 갱신** — 합쳐 두면 update_success/update_failure가 NULL로 지워 중복 방지가 무력화된다
  - Compaction: 기존 DAG trigger — daily `target_dt`, hourly `start_time`/`end_time` + 양쪽 `tables` multi-select params. **maintenance 스케줄 재배치** (설계 6.2): hourly Compaction `45 * * * *`(`M ≤ 60−duration−여유`), daily Compaction 01:00(2시간 슬롯), expire snapshots 03:00, 재처리 04:00, remove orphan files 05:00, rewrite manifests 06:00(3일마다) — 정각 시작으로 매시 `:45` hourly 창을 피한다. 실측 duration: hourly 10~12분, daily 30~60분, expire 6~12분, orphan 5~9분, manifests 2~3분. 기존 간격이 duration보다 짧아 실제로 겹쳤고 orphan이 hourly와 :35에서 충돌했다. **`remove_orphan_files`의 `older_than`은 스케줄로 못 막는다 — 기본 3일 확인 필수**. **`tables` params 선언만으로는 필터가 동작하지 않는다** — Enum loop + `chain()`을 mapped task(`partial`/`expand_kwargs`/`.map()`)로 전환해야 한다 (설계 6.1, `pipeline/examples/compaction_dag_example.py`)
  - 수동 실행: `tables`(multi-select) + `start_time`/`end_time` params (조회 범위 직접 정의, `end_time ≤ 전날 00:00`만 허용)
  - 좀비 IN_PROGRESS(2시간 초과): 탐지 + 알림만, 자동 복구 안 함
  - append DAG과 동일 테이블에 동시 append 커밋 가능 — HMS의 compare-and-swap + Iceberg 재시도로 안전 (둘 다 반영, 유실·중복 없음). `HadoopCatalog`로 전환 시 이 전제가 깨진다 (설계 2.2)
- **전제**: Iceberg snapshot 보존 3일 > 재처리 조회 범위 2일 유지 필수. maintenance 스케줄 재배치와 Compaction DAG 변경(tables params + mapped task)은 재처리 DAG 배포 전 적용
- **후속 과제**: daily 계열 maintenance를 DAG 1개의 순차 task로 통합 (시계 기반 간격은 duration이 늘면 조용히 깨짐)

## 작업 5: Compaction 튜닝 (hourly) — 설정 확정, MAX_EXECUTORS만 미확정

- **산출물**: `tuning/compaction-tuning-guide.md` (상세), `tuning/compaction-tuning-report.md` (회의 보고용 요약)
- **상태**: 9회 측정으로 설정 확정. 초/GB **3.24 → 2.41(−26%)**, dcu/GB **0.00416 → 0.00219(−47%)**, idle cores 58%→17%. DAG 전체 10~12분 → 약 6분
- **대상**: hourly 테이블 4개 (파티션 `hour(ts)`/`col_a`, sort `col_b`/`col_c`, `range` 모드 동일). rewrite 전략은 `sort` — 미적용 시 조회 40% 저하(`read-performance-test.md` §5.4)라 필수
- **확정 설정**: `max-concurrent-file-group-rewrites` 2→**10**(−30%, 유일하게 명확한 개선), `max-file-group-size-bytes` 10GB→**기본값 100GB**, `num-executors` 16→**12**(dcu −13%), `driver cpu` 1→**2**, `advisory-partition-size` **삭제**, `parallelismFirst` **삭제 가능**. `rewrite-all=true`·`partial-progress=false`·executor 4core/16GB는 유지
- **핵심 발견**:
  - **file group이 처리 단위다.** `file group 수 = Σ ceil(파티션 크기 ÷ max-file-group-size-bytes)`. 초기엔 7개를 2개씩 처리해 **4회차**로 나뉘고, 1·4회차가 데이터 15%에 시간 37%를 썼다 (`idle cores 58%`)
  - **`dcu`가 판정의 주 지표다.** `cores × duration`에 비례(9회 검증, ±5%)하고 `duration`(0.1분 반올림)보다 해상도가 좋다. **`duration`만 보면 executor 축소를 "느려졌다"로 오판한다**
  - **노이즈 기준선 15%.** T4·T5가 기능적으로 동일한 설정인데 1.88 vs 2.18(16%). 이보다 작은 차이는 판정 불가 — `driver cpu`, `max-file-group-size`의 속도 이득이 여기 묻혔다
  - **`num-executors`는 12가 하한.** 8에서 dcu가 +13% 반등(CPU 33% 감소 vs 시간 56% 증가). 16→12는 dcu −13%
  - **min_size 300MB대는 정상이다** — 원인은 `col_a=D` 파티션(시간당 600~830MB)이 `ceil(÷512MB)`로 2개로 갈리는 것. **group 분할과 무관**(group 4개에서도 발생). 파일 75개 중 2개, 데이터 2.4%라 조치 안 함. 모니터링 기준은 `min_size<384MB`가 아니라 **`384MB 미만 파일 3개 이상`**
  - **출력 파일 크기의 손잡이는 `target-file-size-bytes` 하나다.** `advisory-partition-size`와 `parallelismFirst` 모두 무효 확정 (Iceberg가 shuffle partition 수를 직접 정함)
  - **`sort` 전략은 데이터를 2번 읽는다** (정렬 범위 샘플링 + 실제 쓰기). DataFlint `input = output × 2.0`이 정상값
  - **DataFlint alert 처방을 그대로 따르면 안 된다.** `idle cores` 원인은 리소스 과다(→executor 축소)와 병렬성 제약(→제약 해제) 두 가지이고, 이번 사례의 원인은 후자다. alert는 전자만 제안한다
  - **`memory usage` 84~94%는 `spill to disk 0b`와 짝으로 읽는다** — 낭비 없이 맞게 쓰는 중이라는 뜻이며 줄이면 spill이 시작된다
- **동적 산정 (`num-executors`) — 설계 완료, 도입 보류**: 설계 `pipeline/compaction-executor-sizing-design.md`, 구현 스켈레톤 `pipeline/examples/compaction_executor_sizing_example.py`
  - **지금 도입할 필요 없다.** 고정 core는 duration이 데이터에 비례하므로 정적 12로 테이블당 **74.7GB까지 창(12분) 내 처리**. 현재 최대 42.3GB → **여유 1.77배**. 도입 시점은 **55~60GB 도달 시**. 즉시 할 일은 `com_num_executor`를 12로 바꾸는 것뿐
  - 당초 6개 값을 동적화하려 했으나 **`num-executors` 하나로 좁혀졌다** (나머지는 데이터 양과 무관하거나 크게 고정이 우월)
  - `num_executors = clamp(ceil(총 크기GB × 0.32), 4, MAX)`, **C=0.32 확정**. `MAX_EXECUTORS`만 미확정(K8S quota 필요, 잠정 32). **상한에 걸리면 조치 신호** — K8S에 여유가 없으면 executor를 늘려도 pod Pending으로 duration이 늘어 동적 산정이 무의미해진다
  - 산정 위치는 `compaction_specs` 내부(테이블별 try/except로 실패 격리). 조회 전용 task 분리·mapped task 실행 직전 조회는 미채택 (설계 §4)
  - **선행 조건: Compaction DAG의 mapped task 전환** (`compaction_dag_example.py`)
  - 입력 측정은 **`.files`가 아니라 `.partitions`** (파티션당 1행 집계, `.files`는 컬럼 19개 통계를 전부 끌고 옴). **범위 조회**여야 한다 — 재처리 DAG trigger 시 여러 시간에 걸친다
  - **파티션 값 변환은 naive datetime으로** — `ts`가 `timestamp_ntz`라 timezone을 붙이면 엉뚱한 시간대를 조회한다. `int((dt − 1970-01-01).total_seconds() // 3600)`, 2026-08-11 13:00 → 496237 (Spark UI 실측 일치)
  - 기존 `com_num_executor` 상수는 **fallback으로 유지** (조회 실패·0 반환·비정상 크기 전부). 지우면 Trino 장애가 곧 Compaction 실패가 된다
  - 미확인: Trino `$partitions`의 `partition.ts_hour` 타입, manifest pruning 동작 여부
  - 현재 데이터(36~42GB)에서 산정값이 12~14로 좁아 **정적 12로 운영하며 동적화를 미루는 선택도 가능**. `C=0.32`은 hourly 전용 — daily는 별도 측정 필요
- **후속 과제**: **daily Compaction의 `rewrite-all` 낭비 의심** — hourly가 정리한 뒤라 no-op이어야 하는데 888GB에 30~60분(데이터 양에 선형). daily 단계 최우선 확인 항목

## 작업 6: FileIO 전환 (S3AFileSystem → S3FileIO) — 분석 완료, 적용 대기

- **산출물**: `pipeline/s3fileio-migration-guide.md`
- **상태**: **운영환경 전환 완료·효과 검증 완료(2026-08-27)**. append/expire/orphan/rewrite manifests/Compaction 전부 정상. MinIO checksum 문제 없음
- **실측 결과** (가이드 §6.5): `deleteObject` **481 → 17.4 req/s(−96.4%)**, `listObjectV2` **680 → 281 req/s(−58.7%)** (peak 기준). expire snapshots **duration 13.5분 → 3.8분(−72%)**, **dcu 0.2002 → 0.0550(−72.5%)**, DataFlint alert 18 → 6
  - **개선은 Spark stage가 아니라 driver 삭제 구간에서 났다** — `input`이 오히려 +18%인데 duration이 −72%. shuffle 지표는 같은 자릿수 유지. 예측한 `Job duration − stage 합계 = 삭제 시간` 구조와 일치
  - **`idle cores` 90%는 그대로 → executor 축소가 다음 조치.** 삭제 구간이 사라진 지금도 90%면 순수 과다 할당(16코어)
  - ⚠️ MinIO 지표는 클러스터 전체일 수 있음(append 5분 주기·Compaction 동시 실행). 잔존 요청의 상당 부분이 다른 Job의 것일 가능성. **`DeleteObjects`(복수형) 지표 확인이 bulk 사용의 직접 증거**
- **⚠️ `fs.s3a.*`는 지우면 안 된다 (실측 확인)**: 지우고 테스트했더니 실패. **`io-impl`은 Iceberg 테이블에만 적용**되고 원천 avro는 Spark DataSource가 Hadoop `FileSystem`을 직접 호출하므로 S3A가 담당한다. **두 설정 공존은 과도기적 중복이 아니라 구조적**이다 (가이드 §1.0)
- **회의 대응 FAQ (가이드 §1.0.1)**:
  - **SDK v1/v2 차이는 시점 문제다** — S3A는 2010년대 초에 만들어져 당시 유일했던 SDK v1 위에 구현됐고, S3FileIO는 2021년경이라 처음부터 v2다. Hadoop은 **3.4.0에서야 v2로 전환**했으므로 우리 3.3.4가 v1인 것 (`hadoop-aws` pom 대조 확인). Spark 4.1(Hadoop 3.4.2) 가면 이 차이는 사라진다
  - **AWS SDK는 FileIO에 포함돼 있지 않다** — `iceberg-aws`는 SDK를 참조만 한다. `iceberg-spark-runtime`(SDK 없음) / `iceberg-aws-bundle`(SDK v2) / `aws-java-sdk-bundle`(SDK v1) 3종 구분
  - **`HadoopFileIO` ≠ `S3AFileSystem`** — 전자는 Iceberg의 FileIO 구현체(전환 후 미사용), 후자는 Hadoop의 FileSystem 구현체(계속 사용). avro read에 필요한 건 후자다
  - **Job별 `fs.s3a.*` 필요 여부**: append **필요**(avro read) / **`remove_orphan_files` 필요** — `usePrefixListing` 기본값이 `false`라 목록 조회를 `listDirRecursivelyWithHadoop`으로 한다(`DeleteOrphanFilesSparkAction.java:118,124,329`) / `expire_snapshots` 불필요 — `hadoopConf`·`FileSystem` 참조가 소스에 **0건** / Compaction 미검증. **그래도 Job별로 갈라놓지 말 것** — 미사용 설정은 비용이 0(`S3AFileSystem`은 `s3a://` 접근 시에만 인스턴스화)인데 템플릿 분기는 장애 여지를 만든다
- **관리 통합 여지(선택, A/B 후)**: Hadoop 3.3.4의 `fs.s3a.aws.credentials.provider` 기본 체인에 `EnvironmentVariableCredentialsProvider`가 포함되므로, 이 설정을 **제거해 기본값으로 되돌리면** S3A도 `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`를 읽어 **Secret 하나로 양쪽 커버 가능**. endpoint는 여전히 두 벌 필요
- **배경**: 1일 배치 삭제 후 `expire_snapshots`가 MinIO에 과도한 `listObject`/`deleteObject`를 발생시켜 부하 유발
- **핵심 발견**:
  - **`HadoopFileIO`의 bulk delete는 가짜다.** `SupportsBulkOperations`를 구현해 Iceberg는 bulk 분기를 타지만, 내부 `deleteFiles()`가 `Tasks.foreach(...).run(this::deleteFile)`로 단건 삭제를 흩뿌린다 → `DeleteObjects` 요청이 0건
  - **S3A는 파일 1개 삭제에 요청 3개를 쓴다.** `HeadObject` + `DeleteObject` + 부모 디렉터리 확인용 `ListObjectsV2`(+ 조건부 마커 `PutObject`). **관측된 listObject의 정체가 이것** — 삭제와 무관한, 디렉터리 시맨틱 흉내용 요청이다
  - **삭제는 driver 단일 지점에서 나간다** (`collectAsList()` 후 driver JVM). 요청은 많은데 동시성은 driver 코어에 묶여 낮다
  - S3FileIO 전환 시 **요청 수 −99.6%** (250개당 `DeleteObjects` 1건, `s3.delete.batch-size` 최대 1000)
  - **`max_concurrent_deletes`는 이미 무시되고 있다** — `HadoopFileIO`가 `SupportsBulkOperations`라고 자기 신고하는 탓에 WARN만 남기고 버려진다
  - **기존 `s3a://` 경로는 그대로 동작한다.** `S3URI`가 scheme을 검증하지 않는다(소스 확인) → **메타데이터 rewrite·테이블 재생성 불필요, 설정 제거만으로 롤백**
  - `remove_orphan_files`도 동일한 삭제 분기를 쓰므로 같이 개선된다. `prefix_listing => true`는 추가 옵션이나 **기존 S3A 디렉터리 마커 오탐 위험**이 있어 별도 검증 후 도입
- **최대 위험**: **AWS SDK v2 BOM 2.33.0의 checksum 기본 활성화 ↔ MinIO 버전 궁합**. 구버전 MinIO는 `501`/`XAmzContentChecksumMismatch`로 거부한다. MinIO 업그레이드 또는 `AWS_REQUEST_CHECKSUM_CALCULATION=when_required`로 대응
- **주의**: `S3FileIO`는 `fs.s3a.*`를 읽지 않는다. **원천 avro 읽기·경로 목록 파일은 여전히 S3A**이므로 `fs.s3a.*`와 `s3.*` 설정이 **공존**해야 한다
- **전환 순서**: maintenance Job(expire/orphan) → Compaction → append. FileIO는 세션 단위 설정이라 Job별 혼용이 안전하다(같은 테이블도 무방). **append는 삭제가 거의 없어 이득이 없으므로 S3A로 남겨도 된다**
- **기존 `fs.s3a.*` 설정 처리** (가이드 §5.1.1): `connection.ssl.enabled=false`와 `aws.credentials.provider=SimpleAWSCredentialsProvider`는 **대응 설정 불필요**(전자는 `s3.endpoint`에 `http://` 포함으로 해결, 후자는 Iceberg 자체 순서를 따름). **`acl.default=PublicReadWrite`는 옮기지 말 것** — 익명 읽기/쓰기를 여는 값이고 MinIO에서 무효일 가능성이 높다(확인 필요). `client.region`은 `s3.` 접두어가 아니며 **실질적으로 필수** — S3A는 못 찾으면 US_EAST_2로 폴백하지만 Iceberg는 폴백이 없어 클라이언트 생성 시 죽는다
- **`s3.staging-dir`은 업로드 전 파트 파일 로컬 버퍼** = `fs.s3a.buffer.dir`과 동일 개념. **명시하지 않는 것이 현상 유지다** — 마운트한 hostPath는 `spark-local-dir-1`(shuffle 전용)이고, `LocalDirsFeatureStep`이 `java.io.tmpdir`을 건드리지 않아 현재 `fs.s3a.buffer.dir`(`/tmp/hadoop-<user>/s3a`)도 전환 후 `s3.staging-dir`(`/tmp`)도 둘 다 `/tmp`다. **Phase 1(maintenance)은 데이터 파일을 안 쓰므로 아예 무관**
- **multipart 기본값이 S3A와 다르다** (§5.1.3): S3A `multipart.size=64M`/`threshold=128M` vs Iceberg `part-size-bytes=32MB`/`threshold=1.5`(→48MB). Phase 2 진입 시 **`s3.multipart.part-size-bytes=67108864`로 맞추는 것이 현상 유지**. 파트는 32MB 채워질 때마다 비동기 업로드되고 완료 즉시 삭제되므로 디스크 점유는 `동시 파트 수 × 파트 크기`
- **maintenance Job 리소스** (현재 driver 1core/1g, executor 4core/4g×4): manifest 스캔은 executor 분산, **삭제는 driver 단독**. **executor 0은 불가**. **driver cores는 1로 둬도 된다** — 삭제는 IO bound라 `s3.delete.num-threads`만 명시하면 충분(전환 후 삭제는 10초 안쪽). executor 축소는 **전환과 동시에 하지 말 것**(A/B 교란). **driver 삭제 구간은 Spark UI에 stage로 안 잡힌다** — `Job duration − stage 합계`가 삭제 시간
- **⚠️ `availableProcessors()` 함정 (확인 완료)**: `driver cores=1`은 K8s **request**라 `coreLimit` 미설정 시 JVM이 노드 전체 코어를 본다. **Compaction은 `coreLimit=1`이 설정돼 있으나 expire snapshots는 미설정** — `iceberg.hadoop.delete-file-parallelism`(= `코어×4`)이 100+ 스레드가 되어 **MinIO 부하 급증의 원인 후보**다. `coreLimit=1` 적용 예정
- **⚠️ 계측 순서**: `coreLimit=1`만 넣어도 삭제 스레드가 128→4로 줄어 MinIO 순간 RPS가 크게 바뀐다(총 요청 수는 동일, duration은 오히려 증가). **baseline 계측을 `coreLimit` 변경보다 먼저** 해야 전환 효과가 과소평가되지 않는다. `coreLimit=1` 이후에는 `s3.delete.num-threads` 기본값이 1이 되므로 명시가 더 중요해진다
- **결정 사항**: `s3.staging-dir` 미명시(현상 유지), `s3.multipart.part-size-bytes` 미명시(기본 32MB로 테스트 후 판단), `coreLimit=1` 적용
- **⚠️ 실제 운영 스택 = Spark 3.5.8 / Scala 2.12 / Hadoop 3.3.4 (§5.0.2)**: Spark 4에서 Scala 코드로 maintenance 함수 실행 시 오류가 나 **임시 다운그레이드** 상태이며 추후 Spark 4 복귀 예정. 즉 문서들의 "Spark 4.1.1"은 **목표 버전이지 현재 값이 아니다** — 구분 표기 필요. **전환 분석에는 영향 없음** — Iceberg `spark/v3.5`·`spark/v4.0` 모듈의 삭제 로직과 Hadoop 3.3.4·3.4.1의 S3A delete 경로, `fs.s3a.*` 기본값이 모두 동일함을 소스 대조로 확인. **⚠️ Iceberg 1.10.1은 Spark 4.1 미지원**(`spark/v4.0`까지만 존재) — 당시 오류의 원인 후보이며 Spark 4.0.x 재시도 검토 가치 있음
- **jar 조치 (§5.0.3)**: `iceberg-spark-runtime`은 `iceberg-aws`를 포함하지만 **AWS SDK는 미포함**(`spark/v3.5/build.gradle:241`). `iceberg-aws-bundle-1.10.1.jar`(약 60MB, **Scala 접미사·Spark 버전 의존성 없음**)를 **추가**하면 되고, **기존 `aws-java-sdk-bundle`(v1)은 제거하지 말 것** — S3A가 쓴다. **v1(`com.amazonaws.*`)과 v2(`software.amazon.awssdk.*`)는 패키지가 달라 공존 가능**
- **이미지 구성 (§5.0.4)**: 운영 이미지가 타 팀 소유라 파생 빌드 필요. **추가하는 것은 `iceberg-aws-bundle-1.10.1.jar` 하나뿐이어야 한다.** 베이스는 `apache/spark:*`가 아니라 **현재 운영 태그 그대로**(3.5.8, 타 팀 커스터마이징 보존). 공식 이미지는 `USER spark`로 끝나므로 `USER root` → 설치 → 복귀. **⚠️ 예전 테스트 Dockerfile의 Hadoop 3.3.4→3.4.1 교체는 절대 가져오지 말 것** — ①S3A가 SDK v1→v2로 바뀌어 원천 avro 읽기 경로까지 변경 → A/B 불가 ②MinIO checksum 리스크를 avro 읽기로 확산 ③shaded `hadoop-client-*`와 unshaded `hadoop-common` 클래스 중복. **`S3FileIO`는 Hadoop 버전과 무관하다 — 3.3.4 위에서 그대로 동작한다.** `ENV TZ`도 추가 금지 — **운영 Pod TZ는 UTC로 확인됐고 그 상태로 정상 동작 중**이다(`timestamp_ntz`/`hour(ts)` 영향)
- **이미지 배포와 설정 전환은 분리**: jar 추가만으로는 아무 일도 안 일어난다(`io-impl` 미설정 시 여전히 `HadoopFileIO`). ①이미지 교체(무해) → 기존 Job 정상 확인 → ②maintenance Job에만 `io-impl` 설정. 각각 독립 롤백
- **라이브러리 배치 원칙 (§5.0.5)**: Maven scope(빌드 시점) / fat jar / Spark classpath는 **다른 축**이다. **Spark 버전에 묶인 것과 인프라 공통은 이미지 + `provided`, 이 앱만 쓰는 비즈니스 라이브러리는 fat jar + `compile`.** 실행 성능은 위치와 무관하고 **기동 시간만** 달라진다. **같은 라이브러리를 이미지와 fat jar 양쪽에 두지 말 것**(버전 다르면 `NoSuchMethodError`)
- **`spark-avro`는 Spark 배포판에 없다**(공식 문서 확인) — 현재 fat jar/이미지/`--packages` 중 어디서 오는지 확인 필요. **권장은 이미지**(Spark 버전과 짝이어야 하는데 fat jar에 두면 드리프트 — 예전 Dockerfile의 `3.5.6` vs 런타임 `3.5.8`이 실례). **단 지금 옮기지 말 것** — A/B 진행 중이며 Spark 4 전환 때 `_2.13-4.1.1`로 바꾸며 함께 정리하는 것이 자연스럽다
- **배치 위치는 이미지의 `$SPARK_HOME/jars/`** — pom.xml fat jar는 ①60MB 매 submit 전송 ②bundle이 이미 relocate한 `org.apache.http`/`io.netty`를 shade가 다시 건드려 깨질 위험 ③`iceberg-spark-runtime`의 `iceberg-aws` 클래스와 중복 때문에 비권장. SQL 프로시저(`CALL ... expire_snapshots`)만 호출한다면 **pom에는 아무것도 추가할 필요 없다**(런타임 classpath 문제). Spark 4 복귀 시 `iceberg-spark-runtime`만 `4.0_2.13`으로 교체하고 **`iceberg-aws-bundle`은 그대로**
- **⚠️ jar 구성 — `iceberg-aws-bundle` 단일 jar 필수** (가이드 §5.0.1, 실제 발생): `NoClassDefFoundError: software/amazon/awssdk/services/kms/...`는 개별 SDK jar 조합의 증상이다. `S3FileIO.initialize()` → `S3FileIOAwsClientFactories.initialize()` → `AwsClientFactories.from()`이 반환하는 `DefaultAwsClientFactory`가 **`AwsClientFactory` 인터페이스의 `KmsClient kms()`/`GlueClient glue()`/`DynamoDbClient dynamo()` 시그니처** 때문에 KMS·Glue·DynamoDB 클래스를 링크 시점에 요구한다 — **S3만 써도 예외 없음**. bundle은 이 모듈들을 전부 포함하므로(`aws-bundle/build.gradle:27-42`) bundle 하나로 통일할 것. bundle은 `org.apache.http`/`io.netty`를 relocate하므로 **개별 `awssdk:*` jar와 혼재시키면 중복 클래스 충돌**
- **미확인**: 실제 삭제 파일 수(추정치 사용), 읽기/쓰기 성능 영향(`dcu/GB` A/B 필요, 노이즈 기준선 ±15%), `fs.s3a.acl.default`의 MinIO 실제 효력, driver Pod `limits.cpu` 유무

## 작업 7: Iceberg 1.11.0 / Spark 4.1 업그레이드 검토 — 분석 완료, 순서 확정

- **산출물**: `pipeline/s3fileio-migration-guide.md` §9 (부록)
- **결론**: **업그레이드 찬성.** 단 **FileIO 전환(Phase 1) 완료 후에** 진행 — 동시 진행 시 A/B 측정 불가
- **핵심**: **Iceberg 1.11.0이 Spark 4.1을 정식 지원한다** (`Support Spark 4.1 #14155`, `spark/v4.1` 모듈, 빌드 대상 `spark41 = 4.1.1`). `iceberg-spark-runtime-4.1_2.13`은 **1.11.0에만 존재**. 1.10.1에는 `spark/v4.0`까지만 있어 **기존 Spark 4 오류의 유력한 원인**이며, "해결된 버전"이 이미 나와 있다 (1.11.0 = 2026-05-19 릴리스)
- **JDK/Scala**: Spark 4.1.1은 `java.version=17`, `scala.version=2.13.17` — 현재 JDK 17 / Scala 2.13으로 충족
- **테이블 사이드 이펙트: 사실상 없다** — `DEFAULT_TABLE_FORMAT_VERSION = 2`, `SUPPORTED_TABLE_FORMAT_VERSION = 4`가 **1.10.1과 1.11.0 동일**. 기존 테이블 자동 업그레이드 없음(명시적 `ALTER TABLE`만 가능), 새 테이블도 v2, Parquet 포맷·snapshot·파티션 스펙 전부 무영향
- **진짜 위험은 스택 쪽**:
  - **⚠️ Hadoop 3.3.4 → 3.4.2로 S3A의 AWS SDK가 v1 → v2가 된다.** ①한 JVM에 SDK v2가 두 벌(Hadoop 번들 vs `iceberg-aws-bundle` 2.44.4) → 클래스패스 충돌 확인 필요 ②**MinIO checksum 이슈가 원천 avro 읽기까지 번진다** — 즉 checksum 확인은 FileIO 전환과 Spark 4 업그레이드 **양쪽의 게이트**
  - **⚠️ `Remove deprecations for 1.11.0` (#14059)** — deprecated API 제거. maintenance Scala 코드가 Iceberg API를 직접 참조하면 실패 가능. **업그레이드 공수를 결정하는 최우선 확인 항목** (SQL 프로시저만 쓰면 무관)
  - **⚠️ Trino 호환성** — 커넥터 버전 확인 + 업그레이드 후 조회 회귀 테스트 필수
  - **⚠️ 튜닝값 재검증** — 작업 1/5의 확정값은 Iceberg 1.10.1 + Spark 3.5 실측치. `Fix BinPackRewriteFilePlanner ... max-files-to-rewrite`(#15576) 등 Compaction 계획 로직 변경 있음
  - **⚠️ Scala 불일치** — 현재 런타임 jar는 `3.5_**2.12**`인데 앱은 2.13이라고 함. 이미지 빌드가 어느 쪽인지 확인 필요
- **업그레이드로 얻는 것 (maintenance 직결)**: **`Refresh table in ListMetadataFiles to prevent incorrect orphan file deletion` (#16324) — orphan 오삭제 방지 수정(데이터 안전성)**, `stream-results` for orphan(#14278), `cleanupMode` in expire(#14287/#14695), BinPack 출력 파일 수 버그 수정(#15576)
- **jar 교체**: `iceberg-spark-runtime-4.1_2.13-1.11.0`(★교체) + `iceberg-aws-bundle-1.11.0`(★버전만). `aws-java-sdk-bundle`(v1)은 Spark 배포판의 SDK v2 bundle로 대체됨
- **권장 순서**: ①현 스택에 `iceberg-aws-bundle-1.10.1` 추가 → Phase 1 측정 ②결과 확정 ③Scala 코드 API 참조 범위 조사 ④1.11.0 + Spark 4.1.1 업그레이드 ⑤Trino·벤치마크 회귀 검증

## 파일 구조

```
├── CLAUDE.md
├── tuning/
│   ├── spark-tuning-guide.md          # Spark 튜닝 가이드 (append Job)
│   ├── compaction-tuning-guide.md     # Compaction 튜닝 가이드 (hourly, 상세)
│   └── compaction-tuning-report.md    # Compaction 튜닝 결과 (보고용 요약)
├── schema/
│   ├── iceberg-schema-design-guide.md  # Iceberg 스키마 설계 가이드
│   ├── read-performance-test.md        # 파티션 전략별 읽기 성능 비교 테스트
│   ├── spark-query-metrics-guide.md    # Spark 쿼리 메트릭 가이드
│   └── trino-query-guide.md            # Trino 쿼리 가이드 (사용자용)
└── pipeline/
    ├── s3fileio-migration-guide.md      # FileIO 전환 가이드 (S3A → S3FileIO)
    ├── reprocessing-dag-design.md      # 재처리 DAG 설계 가이드
    ├── reprocess-flow.md               # 재처리 DAG 처리 흐름 (보고용 요약)
    ├── compaction-executor-sizing-design.md  # Compaction executor 동적 산정 설계
    ├── dags/
    │   └── iceberg_reprocess.py        # 재처리 DAG 정의 (신규 파일은 이것 하나)
    └── examples/
        ├── convert_file_taskgroup_example.py  # ConvertFileTaskGroup 변경(builder 인자) 예시
        ├── compaction_dag_example.py          # Compaction DAG 변경(tables 필터 = mapped task) 예시
        └── compaction_executor_sizing_example.py  # Compaction num-executors 동적 산정 예시
```
