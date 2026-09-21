# Spark + Iceberg 파이프라인 가이드

## Agents

서브에이전트는 **별도 컨텍스트**에서 실행되고 결과 보고서만 반환한다. 문서가 8,000줄이 넘으므로 **여러 문서를 훑는 조사·검증은 에이전트에 위임**하는 것이 기본이다.

| Agent | Purpose | 도구 |
|-------|---------|------|
| `verify-column-naming` | TABLE_A 컬럼 명명 규칙 위반과 폐기된 옛 표기(`col_c`/`col_d`/`par_b`/`sort_c`) 잔존을 검사 | 읽기 전용 |
| `verify-doc-consistency` | 확정값(설정값·cron·버전·실측 수치)이 `CLAUDE.md` ↔ 상세 가이드 ↔ 보고용 요약 사이에서 일치하는지, 목표/운영 버전 구분, 미확인 마커 동기화를 검사 | 읽기 전용 |

**사용 원칙:**

1. **읽기는 병렬, 쓰기는 직렬.** 같은 문서를 여러 에이전트가 동시에 고치면 충돌한다. 수정 적용은 메인 세션에서 한다
2. **에이전트는 이 대화의 맥락을 상속하지 않는다.** `CLAUDE.md`는 읽지만 세션에서 새로 정한 규칙은 모른다 → 호출 프롬프트에 직접 실을 것
3. **실측값 판정과 미확인 항목 확정은 에이전트에 맡기지 않는다.** 에이전트는 사용자에게 되물을 수 없어 빈칸을 추정치로 채울 위험이 있다. 사실 수집은 에이전트, 판정은 메인 세션
4. **보고서를 무검증으로 신뢰하지 않는다.** 예외 처리 0건, 위반 급증, 출처 없는 수치는 원문을 직접 확인할 신호다

## Skills

| Skill | Purpose |
|-------|---------|
| `verify-implementation` | 검증 에이전트를 병렬 실행하고 verify 스킬을 순차 실행하여 통합 검증 보고서를 생성합니다 |
| `manage-skills` | 세션 변경사항을 분석하고, 검증 에이전트/스킬을 생성·업데이트하며, CLAUDE.md를 관리합니다 |

## Code Style Rules

- 커밋 메시지는 한글로 작성
- 결과값과 설명은 무조건 한글로 작성
- 기술 용어는 영어 원어 사용 (Compaction, Bucketing, small file 등 — 한글 음차/번역 금지)
- Confluence 호환 마크다운 (표, 코드블록, 헤더, 인용블록 등)
- **설명 방식 (사용자 요청 2026-09-17)**: 결론 먼저, 그다음 **실측 숫자 하나를 잡아 그 숫자로 단계별 설명**. 추상 용어 나열 금지 — 비유(카드 76묶음, 통에 나눠 넣기)와 "왜 그런가"를 한 문장씩. 지표는 무엇의 크기인지(예: dcu/GB의 GB = Compaction 후 파일 합계) 먼저 정의. 상수(0.32 등)는 어디서 나온 숫자인지 유도 과정을 보여 줄 것. 모범: `compaction-tuning-guide.md` §2.4, 설계서 §4.4 "쉽게 말하면", §8.1, §8.4

## 공통 컨텍스트

### 기술 스택

- Spark 3.5.8 (운영, 임시 다운그레이드 상태 — 목표 4.1.1, 작업 6 §5.0.2), Iceberg 1.10.1, Airflow 3.2.2
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
- **Sort Order: `sort_a`, `sort_b` — 확정.** 읽기 성능 테스트의 4개 조합(B/B-1/B-2/B-3안) 어느 것도 아닌 조합으로 결정됐다 (조합 간 성능 차이가 없었으므로 — `read-performance-test.md` §5)
- Bloom Filter: 효과 없음, 설정 불필요 (테스트 확인)
- array 타입 컬럼 8개: `write.metadata.metrics.column.*` = `none`
- `write.distribution-mode`: `range`
- **명명 규칙 — 접두어가 역할이다**: `par_*` = 파티션, `sort_*` = Sort Order, `col_*` = **성능 최적화 역할 없음**

| 컬럼 | 역할 | Pruning 단계 |
|------|------|--------------|
| `ts` | 파티션 `hour(ts)` | Partition Pruning |
| `par_a` | 파티션 identity | Partition Pruning |
| `sort_a` | **Sort Order 1순위**. `ts`의 문자열 사본 (`2026-08-19 16:21:12.466` → `'20260819162112466'`) | Data Skipping |
| `sort_b` | **Sort Order 2순위** | Data Skipping |
| `col_a` | 없음 | Row-level Filter |
| `col_b` | 없음 | Row-level Filter |

- 조회 패턴: 클라이언트에서 6개 컬럼(ts, par_a, sort_a, sort_b, col_a, col_b) **전부 WHERE에 항상 포함**
- **⚠️ 2026-09-05에 전 문서 명명을 통일했다.** 그 이전 자료(회의 캡처·커밋 메시지·`tuning/` 옛 표기)는 이름이 다르다:

| 예전 | 현재 |
|------|------|
| `col_a`/`col_b`/`col_c`/`col_d` (`tuning/` 계열) | `par_a`/`sort_a`/`sort_b`/`col_b` |
| `par_b`/`sort_c` (`schema/` 계열) | `col_a`/`col_b` |

  **`col_a`는 예전에 파티션 컬럼을 뜻했고 지금은 역할 없는 컬럼이다.** 예전 자료의 숫자를 이름 그대로 옮기면 다른 컬럼 이야기가 된다 — 예전 자료에서 `col_a`가 값 A/B/C/D로 등장하면 그것은 현재의 `par_a`다
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

## 작업 2: Iceberg 스키마 설계 — 스키마 확정 완료

- **산출물**: `schema/iceberg-schema-design-guide.md`
- **상태**: **스키마 확정.** 파티션 B안(`hour(ts)`, `par_a`) + **Sort Order `sort_a`, `sort_b`**
- **읽기 성능 테스트 결과** (`schema/read-performance-test.md`):
  - 섹션 1~4: Hive-raw, Hive-orc, A안, B안, C안 5개 전략 비교 완료. **B안이 4개 테스트 케이스 전부 1위** (A안 대비 5~31% 빠름)
  - 섹션 5: Sort Order/Bloom Filter 설정별 비교. 4개 조합 모두 동일 성능, Bloom Filter 효과 없음
- **Sort Order**: **`sort_a`, `sort_b` 확정.** §5의 4개 조합(B/B-1/B-2/B-3안) 어느 것도 아니다 — 조합 간 성능 차이가 없었으므로 다른 기준으로 선택됐다. §5 도입부에 이 취지의 경고 반영 완료 (`read-performance-test.md` §5)
- **Bloom Filter**: 설정 불필요 (테스트 확인)

## 작업 3: Trino 쿼리 가이드 — 완료, 작업 8 결과 반영 완료

- **산출물**: `schema/trino-query-guide.md`
- **상태**: 완료. **작업 8 검증 결과 반영 완료 (2026-09-05)** — 상세 내역은 `tuning/trino-iceberg-partition-pruning.md` §8.1
  - **컬럼 역할 정정**: 가이드가 정렬 2순위와 일반 컬럼을 **뒤바꿔** 기술하고 있었다 (옛 이름 기준 `sort_c`를 Sort Order 2순위로, `sort_b`를 일반 컬럼으로) → 스키마 확정에 맞춰 교환하고 명명도 통일
  - **`ts =` 등가**: "❌ 결과 없음" → **날짜 조회 목적 ❌ / 정확한 시각 지정 ✅(가장 빠름)** 로 분리
  - **§6.2**: "모든 날짜의 **데이터**를 읽는다" → "전체 기간의 **파일 목록을 훑는다**". **2026-09-07 재정정**: "비용이 `Planning`에 쌓인다"는 서술은 **철회** — 실측으로 확인되지 않았고 판정 지표도 없다. 근거를 **"sort 조건이 빠질 때 `ts`가 유일한 안전장치"**(98,458 → 1,059 파일)로 교체. §7에 `EXPLAIN ANALYZE VERBOSE`의 `dataFiles`/`skippedDataManifests` 안내 추가
  - **추가**: 파티션 필터 강제의 한계(§6.2.1), `ts` 함수별 지원 표(§6.5), Domain 표시 주의(§7)
- **대상 독자**: Trino 쿼리 사용자 (Partition Pruning/Data Skipping 비전문가)
- **핵심 내용**: ts 필터링 방법(date, date_trunc, 범위 조건), WHERE 필수 컬럼, 잘못된 쿼리 패턴
- **근거 계층**: `tuning/trino-iceberg-partition-pruning.md` (작업 8) — 이 가이드가 "무엇을 쓰라"면, 그쪽은 "왜 그렇게 되는지와 그 경계"

## 작업 4: 재처리(Reprocessing) DAG 설계 — 설계 완료, 운영 배포 완료

- **산출물**: `pipeline/reprocessing-dag-design.md` (설계), `pipeline/reprocess-flow.md` (보고용 흐름 요약), `pipeline/dags/iceberg_reprocess.py` (구현 스켈레톤 — 기존 인프라 연결 지점은 TODO 표시)
- **상태**: **운영 배포 완료** (사용자 확인 2026-09-14, 배포 시점 미기록). 저장소의 `iceberg_reprocess.py`는 설계 시점 스켈레톤이며 운영 코드와 다를 수 있다
- **배경**: append DAG의 Oracle 조회 기간(최근 1일 rolling — Job History `ts` 날짜 키 파티셔닝 제약)에서 밀려난 WAIT_SCHEDULING 데이터와, `get_jobs`가 조회하지 않는 FAILURE 데이터가 영구 잔류하는 문제
- **시스템 구조**: Iceberg 테이블 20개+ (hourly/daily 그룹), append DAG은 py 1개에서 테이블별 동적 생성(약 5분 주기, `ts` string `YYYYMMDDHHmmSSsss` 기준 ORDER BY ASC, ROWNUM 200), Compaction DAG은 hourly/daily 각 1개(내부 테이블별 task 순차). **Job History는 Oracle DB 2개에 동일 스키마로 존재 — conn_list loop로 DB별 동일 쿼리 실행, `job_id`는 DB 간 유일 보장 없음(상태 UPDATE는 원천 DB로)**
- **핵심 설계**:
  - 재처리 DAG **1개** (1일 주기, 04:00 KST — `RUN_HOUR` 상수), 테이블별 TaskGroup 순차 실행 (Compaction DAG 패턴)
  - 조회 범위 경계로 경합 원천 차단: FAILED는 전날+그저께 전체, WAIT는 전날 04:00 이전만 (append 하한 = 실행시각-24h ≥ 전날 04:00이므로 절대 안 겹침). **`wait_bound`는 `RUN_HOUR`를 따라가야 한다** — 실행 시각만 옮기면 그 사이 구간을 아무도 안 본다. 잠금/선점/pool 불필요
  - 상한: 테이블당 row 1,000 (러프 설정, 재검증 필요). **크기(GB) 상한은 두지 않는다** — 통제 수단이 둘이면 둘 사이가 어긋난다 (설계 "크기 기준 상한을 두지 않는 이유"). 초과 시 자기 자신 재trigger loop (상한 10회, `max_active_runs=1`로 순차)
  - 중복 적재 방지: snapshot summary에 batch_id 기록(영수증), FAILURE 재적재 전 `.snapshots` 확인 → 커밋된 건 SUCCESS 정정. batch_id는 `stat_desc` CLOB 재사용 — **WHERE 조건 사용 금지** (값 기록/읽기만). **상태 UPDATE는 batch_id를 준 호출에서만 stat_desc를 갱신** — 합쳐 두면 update_success/update_failure가 NULL로 지워 중복 방지가 무력화된다
  - Compaction: 기존 DAG trigger — daily `target_dt`, hourly `start_time`/`end_time` + 양쪽 `tables` multi-select params. **maintenance 스케줄 재배치** (설계 6.2): hourly Compaction `45 * * * *`(`M ≤ 60−duration−여유`), daily Compaction 01:00(2시간 슬롯), expire snapshots 03:00, 재처리 04:00, remove orphan files 05:00, rewrite manifests 06:00(3일마다) — 정각 시작으로 매시 `:45` hourly 창을 피한다. 실측 duration: hourly 10~12분, daily 30~60분, expire 6~12분, orphan 5~9분, manifests 2~3분. 기존 간격이 duration보다 짧아 실제로 겹쳤고 orphan이 hourly와 :35에서 충돌했다. **`remove_orphan_files`의 `older_than`은 스케줄로 못 막는다 — 기본 3일 확인 필수**. **`tables` params 선언만으로는 필터가 동작하지 않는다** — Enum loop + `chain()`을 mapped task(`partial`/`expand_kwargs`/`.map()`)로 전환해야 한다 (설계 6.1, `pipeline/examples/compaction_dag_example.py`)
  - 수동 실행: `tables`(multi-select) + `start_time`/`end_time` params (조회 범위 직접 정의, `end_time ≤ 전날 00:00`만 허용)
  - 좀비 IN_PROGRESS(2시간 초과): 탐지 + 알림만, 자동 복구 안 함
  - append DAG과 동일 테이블에 동시 append 커밋 가능 — HMS의 compare-and-swap + Iceberg 재시도로 안전 (둘 다 반영, 유실·중복 없음). `HadoopCatalog`로 전환 시 이 전제가 깨진다 (설계 2.2)
- **전제**: Iceberg snapshot 보존 3일 > 재처리 조회 범위 2일 유지 필수. maintenance 스케줄 재배치와 Compaction DAG 변경(tables params + mapped task)은 재처리 DAG 배포 전 적용
- **후속 과제**: daily 계열 maintenance를 DAG 1개의 순차 task로 통합 (시계 기반 간격은 duration이 늘면 조용히 깨짐)

## 작업 5: Compaction 튜닝 (hourly) — 4개 테이블 heap·memoryOverhead 전부 확정 (3·4번 18g), DAG 일괄 반영 대기

- **산출물**: `tuning/compaction-tuning-guide.md` (상세), `tuning/compaction-tuning-report.md` (회의 보고용 요약)
- **상태**: 9회 측정으로 설정 확정. 초/GB **3.24 → 2.41(−26%)**, dcu/GB **0.00416 → 0.00219(−47%)**, idle cores 58%→17%. DAG 전체 10~12분 → 약 6분
- **대상**: hourly 테이블 4개 (파티션 `hour(ts)`/`par_a`, sort `sort_a`/`sort_b`, `range` 모드 동일). **같은 원천을 수직분할한 테이블이라 시간당 row 수(약 340만)가 같고 컬럼 폭만 다르다** — 하루치 1번 945GB, 2번 576GB. rewrite 전략은 `sort` — 미적용 시 조회 40% 저하(`read-performance-test.md` §5.4)라 필수
- **⚠️ daily Compaction은 이 작업의 대상이 아니다.** daily DAG은 `day` 파티션 테이블들을 돌며, 그 테이블들은 튜닝한 적 없고 크기도 공유되지 않았다. 예전 기록의 "888GB·rewrite-all 낭비 의심"은 37GB × 24 환산값에서 나온 오류로 **2026-09-16 폐기**. 재처리 DAG 설계서를 근거로 Compaction 대상을 추론하지 말 것
- **확정 설정**: `max-concurrent-file-group-rewrites` 2→**10**→**12**(−30%, 유일하게 명확한 개선. 12는 2026-09-21 재처리 3시간치 = 파티션 12개를 한 번에. 규칙 재처리 시간 수 × 4, 상한 16 — 큰 파티션 7개면 executor 천장 36에 닿아 그 위는 디스크만 증가), `max-file-group-size-bytes` 10GB→**기본값 100GB**, `num-executors` 16→**12**(dcu −13%), `driver cpu` 1→**2**, `advisory-partition-size` **삭제**, `parallelismFirst` **삭제 가능**. `rewrite-all=true`·`partial-progress=false`·executor 4core/16GB는 유지
- **핵심 발견**:
  - **file group이 처리 단위다.** `file group 수 = Σ ceil(파티션 크기 ÷ max-file-group-size-bytes)`. 초기엔 7개를 2개씩 처리해 **4회차**로 나뉘고, 1·4회차가 데이터 15%에 시간 37%를 썼다 (`idle cores 58%`)
  - **`dcu`가 판정의 주 지표다.** `cores × duration`에 비례(9회 검증, ±5%)하고 `duration`(0.1분 반올림)보다 해상도가 좋다. **`duration`만 보면 executor 축소를 "느려졌다"로 오판한다**
  - **노이즈 기준선 15%.** T4·T5가 기능적으로 동일한 설정인데 1.88 vs 2.18(16%). 이보다 작은 차이는 판정 불가 — `driver cpu`, `max-file-group-size`의 속도 이득이 여기 묻혔다
  - **`num-executors`는 12가 하한.** 8에서 dcu가 +13% 반등(CPU 33% 감소 vs 시간 56% 증가). 16→12는 dcu −13%
  - **min_size 300MB대는 정상이다** — 원인은 `par_a=D` 파티션(시간당 600~830MB)이 `ceil(÷512MB)`로 2개로 갈리는 것. **group 분할과 무관**(group 4개에서도 발생). 파일 75개 중 2개, 데이터 2.4%라 조치 안 함. 모니터링 기준은 `min_size<384MB`가 아니라 **`384MB 미만 파일 3개 이상`**
  - **출력 파일 크기의 손잡이는 `target-file-size-bytes` 하나다.** `advisory-partition-size`와 `parallelismFirst` 모두 무효 확정 (Iceberg가 shuffle partition 수를 직접 정함)
  - **`sort` 전략은 데이터를 2번 읽는다** (정렬 범위 샘플링 + 실제 쓰기). DataFlint `input = output × 2.0`이 정상값
  - **DataFlint alert 처방을 그대로 따르면 안 된다.** `idle cores` 원인은 리소스 과다(→executor 축소)와 병렬성 제약(→제약 해제) 두 가지이고, 이번 사례의 원인은 후자다. alert는 전자만 제안한다
  - **`memory usage` 84~94%는 `spill to disk 0b`와 짝으로 읽는다** — 낭비 없이 맞게 쓰는 중이라는 뜻이며 줄이면 spill이 시작된다
- **executor 자원 할당 — Dynamic Allocation + ratio 채택 (1번 9회 + 2번 12회 + 3번 9회 + 4번 14회 실측 검증)**: 설계 `pipeline/compaction-executor-sizing-design.md`, 보류된 C안 스켈레톤 `pipeline/examples/compaction_executor_sizing_example.py`
  - **확정 설정 (4개 테이블, 설계서 §5.5)**: `dynamicAllocation.enabled=true`, **`executorAllocationRatio=0.13`(전 테이블 공통)**, **`spark.executor.instances` = `initialExecutors` = `minExecutors` = `ceil(시간당 GB × 0.32)`(1번 12, 2번 8, 3번 12, 4번 12)**, executor memory **1번 16g, 2번 20g, 3·4번 18g**, `maxExecutors`=**36 고정**(quota 확인 불가·리소스 넉넉·천장은 무해)
  - **⚠️ `spark.executor.instances`가 남아 있으면 그 값이 시작 대수의 바닥이 된다** (`max(initial, min, instances)`, 반납 없음 → 끝까지 유지). 2번 테이블에서 init 6·8, ratio 0.08~0.13을 어떻게 바꿔도 12대로 돈 원인. 확인은 driver 로그 `Using initial executors = N, max of ...` 줄. 설계서 §4.8
  - **2번 테이블 9회 검증 (2026-09-15~16, 설계서 §5.2)**: `instances` 제거 후 ratio 0.13이 **8대로 수렴** (24GB × 0.32 = 7.7). 12대 대비 duration +27%(1.9분)이나 dcu **−10~16%** — 1번의 16 → 12 축소와 같은 모양. **16g에서 spill 1.6~10.4GiB(시간대별 변동, 시간 비용은 안 보임) → 20g에서 0.** 확정: `instances`=init=min **8**, executor memory **20g**, 나머지 1번과 동일. **확정 설정 그대로 돌린 test9: 1.7분, dcu 0.0711, spill 0, idle 16.8%** (init 8이라 warm-up 없어 test8 1.9분보다 짧음, 1번 idle 16.7%와 동급). **18g 재검증 3회(2026-09-21, test10~12): 1시간치 18g spill 0·16g spill 6.28GiB, 18g 20시간치(480GiB, 23대, 10.9분) spill 936MiB → 규칙상 20g 유지.** 1번도 16g + 3g로 1시간치 13대·9시간치 36대 재검증 완료(spill·error 0)
  - **3번 테이블 9회 검증 (2026-09-16~21, 설계서 §5.3)**: 시간당 37~40GB로 1번과 같은 급. ratio 0.13이 8 → **12로 수렴**(39 × 0.32 = 12.5). 16g에서 spill 2/2회(899MiB·1.77GiB), 20g에서 0 4/4회, **18g + 3g에서 0 3/3회(1·2·22시간치)**. 확정: `instances`=init=min **12**, executor memory **18g**. dcu/100만 row 0.030~0.032로 1번보다 30% 높은데 **테이블 성질**(parquet 폭은 같은데 메모리 팽창·spill 성향 → 컬럼 타입 차이) — 설정으로 못 줄인다
  - **4번 테이블 14회 검증 (2026-09-17~21, 설계서 §5.4)**: 3번과 같은 급·같은 결과. 16g spill 1.76GiB → 20g 0 → **18g + 3g 5회(1~6시간치) spill 0·task error 0**. 확정 12대 + **18g**. 사용률 92.6~97.8%와 DataFlint "Executor memory under-provisioned" alert(기준선 92.6~95.2% 사이)는 spill 0이면 무시. **idle cores 17~25%는 구조적 값** — 12대(48 slot)에 쓰기 task 74~77개라 둘째 회차에 slot이 남는다. 1번 16.7%와 같은 원인, 판정 기준 아님(DataFlint 경고 20%는 참고). 20대는 dcu 상승, 10대는 이득이 노이즈 안이라 쫓지 않는다
  - **dcu에는 메모리도 들어간다** — duration 같은 16g↔20g 쌍에서 dcu 일관 +5~6% (2번·3번 3쌍). "dcu ∝ cores × duration"은 메모리 고정 시 관측. **메모리는 spill 0이 되는 최소값만**
  - **판정 원칙 (설계서 §8.3)**: 같은 테이블 안에서 spill 0 + dcu 최저. 테이블 사이 dcu/GB는 착시(row 폭), dcu/100만 row는 참고만(컬럼 타입에 따라 row당 비용이 다름 — 1번 0.024, 2번 0.021, 3번 0.031). GB = Compaction 후 파일 합계(DataFlint `output`)
  - **executor `memoryOverhead` 3g 확정 (2026-09-18, 설계서 §8.4)** — 4번 테이블 시행착오 9회: **1g job 실패, 2g는 job 성공했으나 executor 유실(task error 2.4%/0.9%/3.1%, `MetadataFetchFailedException`·`internal_error_network` = shuffle 통 들고 있던 executor 사망 → 앞 단계 재실행), 3g 2회 task error 0**. "2g 성공"은 job 성공만 본 오판이었다(정정). **판정 지표 = spill 0 + dcu 최저 + task error 0% + executor 유실 0** — job 성공 여부로 판정하지 말 것. **메모리 지표로는 못 잰다**: Java 지표(OffHeap 135MiB)는 netty·네이티브·page cache가 빠지고, 커널 지표 `container_memory_max_usage_bytes`는 page cache 때문에 **항상 pod 한도 + 4~7MiB에 붙는다**(5회 전부) → 시행착오 + task error rate가 유일한 실측. **4개 테이블 모두 3g 명시**(기본값 10%는 2g/1.8g/1.6g라 부족). **고정값 — 동적 조정 불필요**: executor당 shuffle 4.7GB·task 4개·버퍼 크기가 데이터 양과 무관, pod spec이라 실행 중 변경 불가, 실패 비용 큼. 재검토는 cores·target-file-size·shuffle codec·`maxSizeInFlight`·Spark 버전 변경 시(설계서 §9-6). **pod 점유 = heap + overhead** — 1번 19g × 12 = 228g, 2번 23g × 8 = 184g, 3·4번 21g × 12 = 252g(18g + 3g), 합계 916g. driver `memoryOverhead`는 기본값(10% ≈ 410MB) 유지. 컨테이너 이름 executor `spark-kubernetes-executor`. **역할(담당자 설명용, 설계서 §8.4)**: pod 한도 = heap + overhead. heap = 데이터 row를 올려놓고 정렬하는 공간(부족하면 spill, 안 죽음). overhead = 프로그램이 데이터 말고 돌아가는 데 쓰는 메모리(부족하면 OOMKilled) — ①executor 간 shuffle 통신 버퍼(가장 크고 변동, 보내기 버퍼가 요청 executor 수만큼) ②JVM 자체(코드·스레드·GC 장부 — 10% 규칙의 출처) ③압축·해제 작업 공간 ④row·정렬 데이터·shuffle 파일은 절대 아님. **10%(1.8g)가 아니라 3g인 이유 = 필요량이 heap과 무관** — heap 20g → 18g로 줄여도 2g에서 똑같이 유실. 확인 흔적 `exit code 137`/`OOMKilled`. **비유(식당·선반·복도) 금지 — 담당자에게 안 통함(사용자 2026-09-21). 용도를 문장으로 나열할 것**
  - **남은 조절 여지 없음 — 3·4번 18g 확정 (2026-09-21, 설계서 §7)**: 18g + 3g = 21g로 3번 1·2·22시간치, 4번 1~6시간치 spill 0·task error 0. 두 테이블 합계 48g 절감. 2번(16g spill 10GiB)·1번(16g spill 0)은 그대로. executor 수·core 4·driver·Iceberg 옵션은 더 조절하지 않는다(core 변경은 ratio·0.32 전부 재측정, 대수 변경은 노이즈 안)
  - **여러 시간치 재처리 검증 (2026-09-21, 설계서 §5.5)**: 1번 9시간치 350GiB → 36대·4.0분, 2번 20시간치 480GiB → **23대**·10.9분(spill 936MiB), 3번 22시간치 875GiB → 36대·10.4분, 4번 6시간치 → 33대·3.1분. 전부 task error 0. **같은 메모리로 되는 이유(사용자 정리) = 12대·18g는 파티션 1개(시간 × `par_a`, 약 17GB)를 512MB 묶음으로 정렬할 때 비용이 최저인 값이고, 데이터가 늘면 파티션 크기가 아니라 개수만 는다.** 늘어나는 것은 duration(천장 36대에서 **1시간치당 약 0.5분**)뿐. dcu/1시간치는 0.108 → 0.081로 25% 내려감(고정 비용 묻힘) → 재처리는 여러 시간 한 번에 trigger가 싸다. 데이터 = DataFlint input ÷ 2
  - **executor 수는 총 데이터가 아니라 "동시에 도는 파티션의 데이터 합 × 0.3"이다 (2026-09-21 원인 확정, 설계서 §5.5)**: Compaction은 파티션마다 별도 Spark job을 `max-concurrent-file-group-rewrites`개까지만 동시에 돌리고, DA는 지금 도는 stage의 task만 보므로 22시간치를 넣어도 DA 눈에는 동시 파티션 10~12개분만 보인다. 2번은 파티션이 작아(B·C 10.5GB) 23대, 3·4번은 17GB라 31~36대. 천장 36 ÷ 0.3 = 120GB = 큰 파티션 7개면 꽉 참 → 그 위로 동시 파티션을 늘려도 시간 안 줄고 디스크만 증가. 재처리 속도의 진짜 손잡이는 `maxExecutors`(72로 올리고 동시 파티션 20이면 시간 절반, 비용 동일). ~~천장 아래 31·33대 미도달~~ 해소
  - **executor 디스크 `spark-local-dir-1` (hourly 기준)**: executor당 shuffle ≈ 1시간치 shuffle 60GiB ÷ 12대 = **5GiB, 권장 10GiB × 노드당 executor 수**. 재처리 n시간치는 최악 n × 5GiB(끝난 파티션 통을 driver GC 전까지 안 지울 때)~동시 파티션 12개분(6~7GiB). 22시간치가 현 디스크로 성공했으니 용량은 충분 — 운영 첫 실행에서 사용량 1회 확인. 디스크 부족은 executor 사망이 아니라 task 실패(`No space left on device`). OOMKilled된 executor는 자기 디렉터리를 못 지우므로 2g 실험 잔여 `spark-*`·`blockmgr-*` 확인. **job 총 메모리 = driver.memory + driver overhead + (executor.memory + overhead) × 대수**(request = limit), 대수는 평소 시작 대수·재처리 수렴값 두 값
  - **남은 손잡이 판정 (2026-09-21, 가이드 §8.4·설계서 §7)**: `spark.memory.fraction` 0.6 → 0.8 **보류**(캐시 0이라 올릴 수 있음. 16g + 0.8 = task당 3.2GB > 20g + 0.6의 3.0GB → 2번 16g 가능성. 위험 = Parquet 쓰기 버퍼 부족 시 executor Java OOM. 보통은 0.6 그대로. 하려면 2번 16g + 0.8 2~3회) · shuffle codec zstd **안 함**(parquet zstd와 무관, CPU 병목이라 dcu 증가) · `partial-progress` **false 유지**(`rewrite-all=true`라 재실행 시 커밋된 파티션도 다시 씀 → 재실행 비용 안 줄고 snapshot만 증가. `rewrite-all=false`는 재처리 파티션에서 정렬 묶음 두 벌) · `target-file-size` 512MB·core 4 **안 건드림**. 설정으로 못 없애는 비용 = `sort` 전략의 2회 읽기·shuffle 1.5배(조회 성능의 값). 튜닝 방향 판정: 창 60분에 2분이라 duration 대신 비용(dcu) 기준이 옳음
  - **DA 동작 (설계서 §4.4 "쉽게 말하면")**: 목표 대수 = ceil((실행 중 + 대기 task) × 0.13 ÷ 4). 1초 backlog 후 1·2·4·8로 증원, 상한 36, 시작 대수 아래로는 안 내려감. **input이 많으면 늘어난다** — 평소 1시간치는 시작 대수 그대로, 재처리 2~3시간치에서만 증원
  - **Compaction 데이터 흐름 (가이드 §2.4, 카드 76묶음 비유)**: ①경계 정하기(첫 읽기) → ②shuffle write(둘째 읽기, executor 디스크의 묶음별 통) → ③shuffle read(통 모으기, write와 같은 양이 정상) → ④정렬·쓰기. input = output × 2는 ①②가 같은 파일을 각각 읽어서. write가 먼저인 이유는 ③이 ②의 전부를 기다려야 해서
  - **shuffle·메모리 산정 규칙 (설계서 §8.2)**: shuffle 총량 ≈ 데이터 × 1.5(실측 1.41·1.57), task당 shuffle ≈ 512MB × 1.5 = 0.8GB로 데이터 양과 무관, executor당 디스크 ≈ 1.5 ÷ 0.32 ≈ 4.7GB 일정. task당 정렬 메모리 = `(executor memory − 300MiB) × 0.6 ÷ cores`(16g 2.4GB, 20g 3.0GB — Spark `tuning.md`·`ExecutionMemoryPool.scala` 1/N 규칙). **2번이 16g에서 spill 나는 이유는 task당 row 수** — 512MB 파일에 7KB row가 7.2만 개(1번 11KB row 4.5만 개). spill은 시간대 데이터 양이 아니라 테이블 row 모양이 정한다
  - **0.32는 비례식이다** — "37GB에 12대가 dcu 최저"를 12 ÷ 37.3으로 환산한 것. 방법(최저점 실측 → 비례 확장)은 일반적, 값은 이 job(4core·512MB·sort) 전용
  - **새 hourly 테이블 절차** (4번까지 적용 완료): `instances`=init=min=`ceil(시간당 GB × 0.32)`, ratio 0.13, max 36, **18g + 3g로 1회**(4개 중 3개가 16g에서 spill, 그 중 3·4번은 18g로 충분). spill이 나면 20g. 판정: 수렴 대수, spill 0, duration 2분 이내, 384MB 미만 파일 3개 미만 (설계서 §8.3)
  - **DAG 미반영** — 4개 테이블 heap·`memoryOverhead` 확정 완료(2026-09-21, 3·4번 18g 포함). 설계서 §5.5 "DAG 반영용 최종 설정" 표로 일괄 적용 (사용자 결정 2026-09-15)
  - **`initialExecutors` 기본값이 0이라 반드시 명시.** 생략하면 0대에서 시작해 warm-up 20~40초 낭비
  - **ratio 도출**: `desired = 데이터GB × 2.25 × ratio`, 목표 `데이터GB × 0.32` → `ratio = 0.32/2.25 = 0.142`. **양변에서 데이터GB가 소거되므로 ratio는 테이블 크기와 무관 → 공통값 사용 가능**. 실측: 39GB→12대, 82GB→24대
  - **`instances`/`initial`/`min`은 테이블별이어야 한다** — 비율이 아니라 절대 개수라 크기에 비례. **`ceil(시간당 GB × 0.32)`로 산정하며 기존 `com_num_executor`와 다를 수 있다** (2번: 기존 12 → 8. 12로 두면 바닥이 되어 dcu +10~16%). 역할 분담: `instances/initial/min`=평소 대수 바닥, `ratio`=많은 시간대에 얼마나 더 부를지
  - **`maxExecutors`는 예약이 아니라 천장** — max 36으로 두고 실행해도 실제 24대(실측). 실사용량은 ratio가 정하므로 넉넉히 둬도 자원 선점 없음. **K8S quota 확정이 긴급하지 않은 이유**
  - **반납은 일어나지 않는다.** 공식 문서: *"an executor should not be idle if there are still pending tasks"* — 일감이 725~1,450개 상시 대기라 제거 조건 자체가 성립 안 함. `minExecutors=4`로 낮춰도 12대 유지(실측). 즉 DA의 대표 기능(반납)은 안 쓰고 **요청량 조절만** 사용
  - **유일한 실질적 약점: ratio가 파일 크기에 의존.** 일감 수가 파일 크기로 정해지므로 append 설정이 바뀌면 **에러 없이 조용히 어긋난다**. 재검증 조건 1순위
  - **C안(Trino 사전 산정) 보류.** 같은 목적을 B안이 설정 4줄로 달성. `num_executors = clamp(ceil(총 크기GB × 0.32), 4, MAX)`, `.partitions` 범위 조회, naive datetime 변환(2026-08-11 13:00 → 496237), `com_num_executor` fallback 유지 — 필요 시 예시 파일 참조
  - **daily는 판단이 다르다** — 30~60분 job이라 `executorIdleTimeout` 60초가 전체의 2~3%에 불과해 반납이 실제로 일어날 수 있다. ratio 0.13도 hourly 전용. daily 튜닝 후 별도 판단
  - 미확인: 메모리 사용률 92.6~97.8%(18g에서도 동일, DataFlint under-provisioned alert 포함 — spill 0인 동안 조치 안 함), executor `spark-local-dir-1` 사용량(예상 hourly 5GiB/executor, 운영 첫 실행에서 1회 확인), 2번 spill의 압축 팽창 요인(row 수 차이는 확정), 3번 row당 비용이 높은 컬럼. ~~`maxExecutors`~~ 36 고정, ~~ratio 0.066 로그~~ `instances` 바닥으로 해소, ~~운영 duration~~ test9 1.7분으로 확인
  - 입력 측정은 **`.files`가 아니라 `.partitions`** (파티션당 1행 집계, `.files`는 컬럼 19개 통계를 전부 끌고 옴). **범위 조회**여야 한다 — 재처리 DAG trigger 시 여러 시간에 걸친다
  - **파티션 값 변환은 naive datetime으로** — `ts`가 `timestamp_ntz`라 timezone을 붙이면 엉뚱한 시간대를 조회한다. `int((dt − 1970-01-01).total_seconds() // 3600)`, 2026-08-11 13:00 → 496237 (Spark UI 실측 일치)
  - 기존 `com_num_executor` 상수는 **fallback으로 유지** (조회 실패·0 반환·비정상 크기 전부). 지우면 Trino 장애가 곧 Compaction 실패가 된다
  - 미확인: Trino `$partitions`의 `partition.ts_hour` 타입, manifest pruning 동작 여부
  - 현재 데이터(36~42GB)에서 산정값이 12~14로 좁아 **정적 12로 운영하며 동적화를 미루는 선택도 가능**. `C=0.32`은 hourly 전용 — daily는 별도 측정 필요
- **후속 과제**: DAG 일괄 반영(설계서 §5.5 표: 1번 12대 16g, 2번 8대 20g, 3·4번 12대 18g, overhead 3g 공통, `max-concurrent-file-group-rewrites` 12) → 운영 첫 실행에서 duration·task error 0·`spark-local-dir-1` 사용량 확인 → (보류, 시간 될 때) `spark.memory.fraction` 0.8 실험. daily Compaction은 별건(대상 테이블 크기·구성 공유 후 시작)

## 작업 6: FileIO 전환 (S3AFileSystem → S3FileIO) — 전환 완료, 후속 작업 대기

- **산출물**: `pipeline/s3fileio-migration-guide.md` (상세), `pipeline/s3fileio-migration-report.md` (보고용 요약 — Grafana 패널 이미지 첨부 필요)
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
  - **⚠️ `fs.s3a.*`는 Job 종류와 무관하게 전부 필요하다 (실측 원인 확정)**: 제거 시 `ERROR SparkContext: Error initializing SparkContext` / `AccessDeniedException: s3a://bucket/logs/spark` / `NoAuthWithAWSException`. 원인은 Iceberg가 아니라 **`spark.eventLog.dir`이 `s3a://`**라는 것 — `SparkContext.scala:627-633`이 생성자 안에서 `EventLoggingListener.start()`를 호출해 로그 파일을 만든다. **Iceberg 코드 실행 전에 죽으므로 Job별 필요 여부 판단 자체가 무의미하다.** (`spark.history.fs.logDirectory`는 History Server가 읽는 별도 프로세스 설정이라 직접 원인은 아니나 같은 위치)
  - 참고로 Iceberg 계층만 보면: append 필요(avro read) / `remove_orphan_files` 필요(`usePrefixListing` 기본 `false` → `listDirRecursivelyWithHadoop`, `DeleteOrphanFilesSparkAction.java:118,124,329`) / `expire_snapshots` 불필요(`hadoopConf`·`FileSystem` 참조 **0건**) / Compaction 미검증. **그래도 Job별로 갈라놓지 말 것**
  - **Iceberg manifest도 avro지만 `fs.s3a.*`·`spark-avro`와 무관하다** — manifest는 Spark DataSource가 아니라 **Iceberg 자체 reader가 `FileIO`를 통해** 읽는다(`BaseSparkAction.java:419-429`, `ManifestFiles.read(manifest, io, specs)`). Avro 라이브러리는 `iceberg-spark-runtime`에 이미 포함. 포맷만 같을 뿐 접근 경로가 다르다
- **관리 통합 여지(선택, A/B 후)**: Hadoop 3.3.4의 `fs.s3a.aws.credentials.provider` 기본 체인에 `EnvironmentVariableCredentialsProvider`가 포함되므로, 이 설정을 **제거해 기본값으로 되돌리면** S3A도 `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`를 읽어 **Secret 하나로 양쪽 커버 가능**. 정리하면 **자격증명 1곳 + 설정 5줄, 중복은 endpoint 1줄뿐**이 된다 (§1.0.1 Q6)
- **⚠️ S3A는 제거 불가 — `s3://` 스킴으로 바꿔도 안 된다 (§1.0.1 Q6)**: `s3.*`(Iceberg 카탈로그 프로퍼티)와 `s3://`(URI 스킴)은 **무관하다**. Spark의 eventLog는 Hadoop `FileSystem`만 알고 `FileIO` 개념 자체를 모른다. 게다가 **Hadoop 3.x에는 `s3://` 구현체가 없다**(`core-default.xml`에 `fs.s3a.impl`만 존재, `s3`/`s3n`은 3.0에서 제거) → `No FileSystem for scheme "s3"`. `fs.s3.impl`을 S3A로 매핑해봐야 되돌아올 뿐이다. **결정적인 것은 원천 avro read** — Spark DataSource는 Hadoop `FileSystem`만 쓰며 `S3FileIO`를 끼워 넣을 방법이 없다. Spark 4.1로 가도 구조는 동일(SDK만 v2로 통일)
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
- **⚠️ 실제 운영 스택 = Spark 3.5.8 / Scala 2.12 / Hadoop 3.3.4 (§5.0.2)**: Spark 4에서 Scala 코드로 maintenance 함수 실행 시 오류가 나 **임시 다운그레이드** 상태이며 추후 Spark 4 복귀 예정. 즉 문서들의 "Spark 4.1.1"은 **목표 버전이지 현재 값이 아니다** — 구분 표기 완료(2026-09-05, 공통 컨텍스트·가이드 환경 표 4곳). **전환 분석에는 영향 없음** — Iceberg `spark/v3.5`·`spark/v4.0` 모듈의 삭제 로직과 Hadoop 3.3.4·3.4.1의 S3A delete 경로, `fs.s3a.*` 기본값이 모두 동일함을 소스 대조로 확인. **⚠️ Iceberg 1.10.1은 Spark 4.1 미지원**(`spark/v4.0`까지만 존재) — 당시 오류의 원인 후보이며 Spark 4.0.x 재시도 검토 가치 있음
- **jar 조치 (§5.0.3)**: `iceberg-spark-runtime`은 `iceberg-aws`를 포함하지만 **AWS SDK는 미포함**(`spark/v3.5/build.gradle:241`). `iceberg-aws-bundle-1.10.1.jar`(약 60MB, **Scala 접미사·Spark 버전 의존성 없음**)를 **추가**하면 되고, **기존 `aws-java-sdk-bundle`(v1)은 제거하지 말 것** — S3A가 쓴다. **v1(`com.amazonaws.*`)과 v2(`software.amazon.awssdk.*`)는 패키지가 달라 공존 가능**
- **이미지 구성 (§5.0.4)**: 운영 이미지가 타 팀 소유라 파생 빌드 필요. **추가하는 것은 `iceberg-aws-bundle-1.10.1.jar` 하나뿐이어야 한다.** 베이스는 `apache/spark:*`가 아니라 **현재 운영 태그 그대로**(3.5.8, 타 팀 커스터마이징 보존). 공식 이미지는 `USER spark`로 끝나므로 `USER root` → 설치 → 복귀. **⚠️ 예전 테스트 Dockerfile의 Hadoop 3.3.4→3.4.1 교체는 절대 가져오지 말 것** — ①S3A가 SDK v1→v2로 바뀌어 원천 avro 읽기 경로까지 변경 → A/B 불가 ②MinIO checksum 리스크를 avro 읽기로 확산 ③shaded `hadoop-client-*`와 unshaded `hadoop-common` 클래스 중복. **`S3FileIO`는 Hadoop 버전과 무관하다 — 3.3.4 위에서 그대로 동작한다.** `ENV TZ`도 추가 금지 — **운영 Pod TZ는 UTC로 확인됐고 그 상태로 정상 동작 중**이다(`timestamp_ntz`/`hour(ts)` 영향)
- **이미지 배포와 설정 전환은 분리**: jar 추가만으로는 아무 일도 안 일어난다(`io-impl` 미설정 시 여전히 `HadoopFileIO`). ①이미지 교체(무해) → 기존 Job 정상 확인 → ②maintenance Job에만 `io-impl` 설정. 각각 독립 롤백
- **라이브러리 배치 원칙 (§5.0.5)**: Maven scope(빌드 시점) / fat jar / Spark classpath는 **다른 축**이다. **Spark 버전에 묶인 것과 인프라 공통은 이미지 + `provided`, 이 앱만 쓰는 비즈니스 라이브러리는 fat jar + `compile`.** 실행 성능은 위치와 무관하고 **기동 시간만** 달라진다. **같은 라이브러리를 이미지와 fat jar 양쪽에 두지 말 것**(버전 다르면 `NoSuchMethodError`)
- **`spark-avro`는 Spark 배포판에 없다**(공식 문서 확인) — 현재 fat jar/이미지/`--packages` 중 어디서 오는지 확인 필요. **권장은 이미지**(Spark 버전과 짝이어야 하는데 fat jar에 두면 드리프트 — 예전 Dockerfile의 `3.5.6` vs 런타임 `3.5.8`이 실례). **단 지금 옮기지 말 것** — A/B 진행 중이며 Spark 4 전환 때 `_2.13-4.1.1`로 바꾸며 함께 정리하는 것이 자연스럽다
- **배치 위치는 이미지의 `$SPARK_HOME/jars/`** — pom.xml fat jar는 ①60MB 매 submit 전송 ②bundle이 이미 relocate한 `org.apache.http`/`io.netty`를 shade가 다시 건드려 깨질 위험 ③`iceberg-spark-runtime`의 `iceberg-aws` 클래스와 중복 때문에 비권장. SQL 프로시저(`CALL ... expire_snapshots`)만 호출한다면 **pom에는 아무것도 추가할 필요 없다**(런타임 classpath 문제). Spark 4 복귀 시 `iceberg-spark-runtime`만 `4.0_2.13`으로 교체하고 **`iceberg-aws-bundle`은 그대로**
- **⚠️ jar 구성 — `iceberg-aws-bundle` 단일 jar 필수** (가이드 §5.0.1, 실제 발생): `NoClassDefFoundError: software/amazon/awssdk/services/kms/...`는 개별 SDK jar 조합의 증상이다. `S3FileIO.initialize()` → `S3FileIOAwsClientFactories.initialize()` → `AwsClientFactories.from()`이 반환하는 `DefaultAwsClientFactory`가 **`AwsClientFactory` 인터페이스의 `KmsClient kms()`/`GlueClient glue()`/`DynamoDbClient dynamo()` 시그니처** 때문에 KMS·Glue·DynamoDB 클래스를 링크 시점에 요구한다 — **S3만 써도 예외 없음**. bundle은 이 모듈들을 전부 포함하므로(`aws-bundle/build.gradle:27-42`) bundle 하나로 통일할 것. bundle은 `org.apache.http`/`io.netty`를 relocate하므로 **개별 `awssdk:*` jar와 혼재시키면 중복 클래스 충돌**
- **미확인**: 실제 삭제 파일 수(추정치 사용), Compaction/append의 읽기·쓰기 성능 영향(`dcu/GB` 비교 필요, 노이즈 기준선 ±15%), `fs.s3a.acl.default`의 MinIO 실제 효력
- **남은 후속 작업** (전부 보류 상태, 서로 독립):
  1. **maintenance Job 리소스 축소** — `idle cores` 90%가 전환 후에도 그대로다. executor `instances` 4 → 2 검토 (§5.1.2). `coreLimit=1` 적용도 여기 포함
  2. **자격증명 통합** — K8s Secret → 환경변수로 일원화, `fs.s3a.aws.credentials.provider` 제거 (§1.0.1 Q6). 절차와 manifest 예시까지 정리됨. 성능과 무관하므로 언제 해도 되나 **단독 배포로** 적용
  3. **`fs.s3a.acl.default=PublicReadWrite` 제거 검토** — 보안 항목 (§5.1.1)
  4. **`remove_orphan_files`의 `prefix_listing => true`** — LIST 추가 감소 여지. 기존 S3A 디렉터리 마커 오탐 검증 필요 (§2.2, §4.8)
  5. **Compaction/append `dcu/GB` 비교** — 리소스 튜닝과 함께 진행
  6. **Iceberg 1.11.0 + Spark 4.1 업그레이드** — 작업 7 참조

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

## 작업 8: Trino Partition Pruning 검증 — 조사 완료, `EXPLAIN ANALYZE VERBOSE` 실측 완료(2026-09-07), 가이드 반영 완료

- **산출물**: `tuning/trino-iceberg-partition-pruning.md`
- **환경**: **Trino 482** (`read-performance-test.md` §5의 Bloom Filter 측정은 475 기준 — 버전 구분 필요)
- **위치**: 작업 3(`schema/trino-query-guide.md`, 사용자용 안내)의 **근거 계층**. 안내가 성립하지 않는 경계 조건을 밝히는 문서
- **핵심 발견**:
  - **일 → 시 단위에서 splits가 4,938 → 205로 정확히 24.1배 감소** — 하루의 시간 파티션 수 24와 일치. `hour(ts)` Pruning이 설계대로 동작한다는 직접 증거이며, `read-performance-test.md`의 "B안이 4개 케이스 전부 1위"라는 **결과에 메커니즘을 채워 넣는다**
  - **`EXPLAIN`의 Domain 표시는 Pruning 여부의 지표가 아니다.** `ts = <시점>`은 표시가 없는데도 15 splits로 프루닝된다. 업스트림이 명시한 함정 — PR #24740(milestone 469)이 *"partition pruning done at the Iceberg metadata layer"*가 EXPLAIN에 pushdown이 안 보여도 일어난다는 테스트를 추가했다
  - **⚠️ Issue #19266의 워크어라운드는 필요 없다 (조사 원본 정정).** "파티션 경계에 안 맞는 범위 조건은 Pruning 실패"라는 제보는 **EXPLAIN 표시를 오독한 것**이었고 PR #24740(469)으로 종결됐다. 우리는 482라 포함. 실측도 일치 — `ts >= 16:00 AND ts < 17:00`이 205 splits로 해당 시간 파티션 하나만 읽었다
  - **`sort_a`는 `ts`의 문자열 사본이다** (`2026-08-19 16:21:12.466` → `'20260819162112466'`). 기존 프로젝트 문서 어디에도 없던 사실이며, **이것이 관측 전체를 설명한다** — Sort Order 1순위 + ts 사본 조합이라 `sort_a` 등가조건 하나가 밀리초 단위 시각 조건으로 작동해 파일 단위까지 걸러낸다. **다른 테이블에 일반화 금지**
  - **운영 쿼리 패턴(6개 컬럼 전부 WHERE)에서는 `ts` 조건을 넣든 빼든 16.79MB / 15 splits로 동일하다.** `read-performance-test.md` §5.3에서 Sort Order 4개 조합이 전부 8.56k rows / 55.2MB였던 것과 **같은 현상** — 이미 파일 단위까지 좁혀져 그 위에서 뭘 바꾸든 차이가 안 나는 영역
  - **manifest 단계 Pruning은 `ts` 전용이다 (`EXPLAIN ANALYZE VERBOSE` 실측, §3.4).** `ts` 조건이 있으면 manifest 29개 중 27~28개를 건너뛰고 1~2개만 열지만, `sort_a`만으로는 29개를 전부 연다. `par_a`도 identity 파티션 컬럼인데 0개 — 모든 manifest에 A~D가 다 들어 있어서다. 소스 근거: manifest 필터는 WHERE를 **파티션 스펙에 투영한 식**만 쓴다(`ManifestGroup.java:252-256`), 비파티션 컬럼 술어는 `alwaysTrue`가 된다
  - **그럼에도 `ts` 조건은 필요하다 — 단 근거가 바뀌었다.** 기존 "manifest 전수 조회 비용이 Planning에 쌓인다"는 서술은 **철회**. 경로 차이(29개 vs 1~2개)는 실측됐으나 시간 비용으로는 확인되지 않았고, `scanPlanningDuration`은 split 생성 전체의 벽시계 시간이라(`SnapshotScan.java:136-141`, iterable close까지 측정) 판정 지표가 못 된다. **실제 이유는 sort 조건이 빠질 때의 안전장치** — sort 없는 쿼리에서 `ts` 하나가 **98,458 파일/13.65GB → 1,059 파일/264MB(93배)**를 가른다. `ts`와 `sort_a`는 서로의 안전장치이므로 "둘 다 넣으라"는 안내는 그대로
  - **파일 단계 카운터는 파티션·통계를 분리하지 않는다** — 엔트리 필터가 `evaluator.eval(partition) && metricsEvaluator.eval(file)` 한 술어·한 카운터(`ManifestReader.java:240-251`)이고, **Trino 482는 `skippedDataFiles`를 노출조차 안 한다**. 분리하려면 통계로는 못 걸리는 술어(하루·한 시간 범위)를 대조군으로 두고 `dataFiles`를 비교한다. 그 방법으로 `date(ts)` 1,059 파일(파티션만) → `ts =` 3 파일(`ts` min/max 통계) 분해 확인
  - **splits ÷ 4 환산 폐기.** splits/파일 비율이 2.94~5.0으로 불안정(미Compaction 구간 작은 파일, row group 경계). `dataFiles`를 직접 인용한다
  - manifest 총수는 스냅샷마다 다르다(29~31, append가 늘리고 `rewrite_manifests`가 줄임). 조건 간 비교는 한 스냅샷 안에서 연속 측정, 엄밀히는 `FOR VERSION AS OF`로 고정
  - **`date_trunc('week'|'quarter')`는 482에서 Pruning 안 된다** — PR #30197이 milestone **484**. day/month/year·`year()`·`date()`·범위 비교는 전부 동작. **일시적 제약이므로 우회 코드를 쿼리에 영구히 박지 말 것**
  - **파티션 필터 강제(`iceberg.query-partition-filter-required`)는 `ts` 누락을 못 막는다** — 파티션 컬럼 **하나라도** 있으면 통과하므로 `par_a`만으로 3개월 전체 조회가 에러 없이 실행된다 (기준선 측정이 그 증거)
  - **`read.split.target-size`(읽기, 기본 128MB)를 `write.target-file-size-bytes`(쓰기, 512MB)에 맞춰 올리지 말 것** — 파일 1개 = split 1개가 되어 병렬성 4~5배 하락. 두 값이 다른 것은 불일치가 아니라 설계
- **`trino-query-guide.md` 반영 완료** — 상세는 작업 3 및 문서 §8.1
- **컬럼 명명 통일 완료 (2026-09-05)**: 전 문서에 `par_*`/`sort_*`/`col_*` 규칙 적용. 변환표는 공통 컨텍스트의 대상 테이블 절과 작업 8 문서 §1.3
- **미확인**: `par_a` 분포가 `schema/` 문서(2026-03-18)와 순위가 다른 원인, `iceberg.query-partition-filter-required` 실제 설정값, `$partitions` 대조(`date(ts)` 1,059 파일 = 해당 날 파티션 `file_count` 합계인지 — `col_b` 통계 개입 배제용, 낮음), 규모 증가 시 manifest 전수 조회가 비용으로 드러나는지(판정 지표 부재로 보류)

## 작업 9: Iceberg 테이블 재생성 + `tmp_id`(NOT NULL) 추가 — 개발 검증 완료(2026-09-14), 운영 적용 대기. 코드 변경 없음

- **intent**: `intent/schema/recreate-table-tmp-id/intent.md` (2026-09-12 승인, 사후 기록). 미결 항목은 이 문서의 Open questions 참조. `intent/`는 사이트 게시 제외(`mkdocs.yml`)라 링크가 아니라 경로 텍스트로만 적는다
- **산출물**: `pipeline/recreate-table-tmp-id.md` — 단일 절차서. 코드(약 40줄)·수동 DDL·실행 순서·기대 출력을 한 문서에. **의도적으로 짧게 유지한다** (2026-09-09 사용자 요청: 1회성 작업이라 코드·설명이 많으면 확인이 어렵다 — 검증 로직·모드·매니페스트를 늘리지 말 것)
- **배경**: Iceberg는 비어 있지 않은 테이블에 required 컬럼 추가를 거부한다 (Spark `ADD COLUMN ... NOT NULL`·`SET NOT NULL`·`DEFAULT`, Trino 전부 불가) → 임시 테이블 복사 → `DROP ... PURGE` → `CREATE` → 조인 `INSERT`. 새 테이블은 snapshot 이력·UUID 초기화
- **Oracle 조회는 `dt`(varchar2 `YYYYMMDD…`) 범위를 주 단위 chunk로 잘라 `spark.read.jdbc(predicates)`로 병렬 조회** (2026-09-09). Oracle 테이블의 `dt` 파티션은 7월부터라 그 이전 chunk는 같은 파티션을 반복 스캔해 느리지만, 사용자 결정으로 **단순한 균일 chunk 유지** (7월 이전 별도 처리·hash 분할·PARALLEL 힌트 안 씀)
- **검수는 앱 안에서 `EXCEPT ALL` 전수 비교 + `require`** (2026-09-09, 개발 클러스터 backup 테스트 후 건수만으로는 부족하다는 판단): backup 후 원본 vs 임시, load 후 신규(원본 컬럼만) vs 임시 — 건수 일치 + 차집합 0. tmp_id는 `NOT (t.tmp_id <=> COALESCE(o.tmp_id, ''))` 0건. **`SELECT * EXCEPT (col)`은 Spark 4.0 문법이라 3.5에서 ParseException** — 컬럼 목록은 임시 테이블 스키마에서 뽑는다 (로컬 Spark 3.5.8로 확인). **Iceberg 읽기에는 파티션 키 WHERE 필수**(사용자 규칙) — 모든 조회에 `Where`(`ts` 하한, 전체 범위) 부착
- **테이블명은 실행 인자** (`RecreateTable <backup|load> <테이블명>`, 임시 = `<테이블명>_tmp`) — 여러 테이블에 같은 코드 재사용 (2026-09-09). Oracle 원천·조인 키는 상수 유지
- **역할 분담**: DROP/CREATE는 spark-sql 수동. 앱은 `backup`(원본 → 임시 CTAS) / `load`(임시 LEFT JOIN Oracle → 신규 INSERT) 두 모드. INSERT 컬럼 목록은 신규 테이블 스키마에서 자동 생성 — 설정은 테이블 이름·Oracle 접속/쿼리·조인 조건뿐
- **신규 요소는 Oracle JDBC뿐** — 기존 앱에 클래스 1개 + pom에 ojdbc8 의존성 1개. Iceberg 접근·SparkApplication은 기존 것 그대로(mainClass/arguments/`restartPolicy: Never`만). Oracle에는 SELECT만. 접속정보는 코드 하드코딩 — **커밋 금지**
- **상태**: 코드는 Scala 2.12.18 / Spark 3.5.8 / Iceberg 1.10.1로 컴파일 검증 완료. 이름은 전부 자리표시자(`iceberg.db.table_a`, `table_a_tmp`, `key1`/`key2`, `ORA_SCHEMA.ORA_TABLE`). 신규 컬럼은 `STRING NOT NULL`만 확정
- **주의**: `gc.enabled=false`면 PURGE 거부 · JDBC 읽기는 executor에서 실행되므로 Oracle 방화벽은 driver·executor 양쪽 · 임시 테이블은 파티션·Sort Order 없는 CTAS(rename 재사용 금지) · Sort Order는 `WRITE ORDERED BY` 별도 · **재처리 DAG(운영 배포됨)의 `.snapshots` batch_id 영수증 소실** — 재생성 전 커밋의 영수증이 전부 사라지므로, 최근 2일(재처리 자동 범위)에 대상 테이블의 `FAILURE`·`IN_PROGRESS` row가 있고 그 데이터가 실제로는 커밋됐다면 재처리가 영수증 없이 재적재해 중복이 난다. **운영 전 Oracle Job History에서 해당 건 0건 확인**, 있으면 먼저 정리
- **진행 상태 (2026-09-14)**: 개발 클러스터에서 `backup` 통과(건수 일치). `load`는 `[Oracle 에 키 없는 row]` 로그 직후 `AnalysisException UNRESOLVED_COLUMN t.tmp_id`(driver 로그 `'Project [..., 't.tmp_id, ...]`)로 실패. **원인 확정(사용자 진단 + 로컬 재현 일치)**: 개발에 배포된 코드의 컬럼 매칭이 `case "TMP_ID" => ...` 문자열 완전 일치였고 신규 테이블 컬럼은 소문자 `tmp_id`라 매칭에 실패 → 그 컬럼이 일반 컬럼처럼 `t.tmp_id`로 생성돼 INSERT 분석 단계에서 죽은 것. 신규 테이블에 `tmp_id`는 확실히 존재했다 (`RelationV2[..., tmp_id, ...]`). Scala 문자열 match는 대소문자를 구분한다. **2026-09-13에 기록했던 "신규 테이블에 `tmp_id` 없음" 원인은 오진**이었다 — 그 경우는 INSERT가 통과하고 마지막 `tmp_id 불일치` 쿼리에서 실패하므로 로그 위치가 다르다. **조치**: 절차서의 현재 코드(PR #64, `equalsIgnoreCase`)가 이 문제를 이미 막는다 — 개발 배포 코드를 절차서 버전으로 교체하고 `load` 재실행. INSERT가 분석 단계에서 실패했으므로 신규 테이블은 비어 있고 DROP/CREATE 재실행 불필요
- **현 테이블은 Sort Order 미적용** (사용자 확인, 2026-09-14). 절차서의 `WRITE ORDERED BY`·`SHOW CREATE TABLE` 확인 줄은 "기존에 있었으면"으로 조건부 유지
- **로컬 재현으로 확인 (2026-09-13~14, Spark 3.5.8 / Iceberg 1.10.1, Oracle은 Derby 인메모리 대역)**: ①정상 경로 전 구간 통과 — NOT NULL 컬럼에 `COALESCE(o.tmp_id, '')` INSERT 허용, 완전 중복 row·NULL array·NaN 모두 `EXCEPT ALL` 검수 통과 ②패턴/컬럼 대소문자 불일치는 어느 방향이든 INSERT 단계 `UNRESOLVED_COLUMN t.<컬럼명>`으로 실패, 현재 코드는 양방향 통과 ③`SHOW CREATE TABLE` 출력을 그대로 CREATE에 쓰면 `'sort-order'`·`'current-snapshot-id'` 줄은 에러 없이 무시된다 — DDL을 그렇게 만들 계획은 없었으나 참고로 유지 ④`[Oracle 에 키 없는 row]`에는 `DtFrom`~`DtTo` 밖의 Oracle row도 들어간다. 재현 환경은 scratchpad라 세션 종료 시 사라짐: Spark 배포판 + `iceberg-spark-runtime-3.5_2.12-1.10.1.jar`, JDK 17(apt), 컴파일은 배포판의 `scala-compiler` jar(`java -cp "jars/*" scala.tools.nsc.Main -usejavacp`), Oracle 대역은 배포판 내장 Derby(`jdbc:derby:memory:`)
- **사용자 결정 (유지할 것)**: 검수는 별도 모드로 빼지 않고 `backup`/`load` 안에 둔다(제안했으나 "그냥 냅둬") · `[Oracle 에 키 없는 row]`에 상한 `require`를 넣지 않는다 — 로그만 보고 사람이 판단 (2026-09-14, 제안했으나 거절) · 절차서는 짧게, 코드 변경은 "변경 전/후" 대비로 정리해서 전달 · 코드 컴파일 확인은 scratchpad에 sbt 런처(Maven Central `sbt-launch-1.10.7.jar`) + `spark-sql`/`iceberg-spark-runtime-3.5_2.12` provided로 했으며 세션 종료 시 사라지므로 새 세션에서는 재구성 필요
- **운영 적용 (2026-09-15)**: `backup` CTAS 후 append가 이어져 추가분은 Trino `INSERT INTO ... WHERE ts > <임시 max(ts)>`로 보충(서브쿼리는 파티션 필터 강제에 걸려 리터럴 사용). `load` 완료. 단 `DtTo`를 `20260910`으로 두고 실행해 그 이후 Oracle row와 다른 Oracle에만 있는 row가 `''`로 남음 → **`load`의 INSERT 줄을 UPDATE로, 사후 검수 ②를 `tmp_id 미반영`으로 바꿔 재실행**(절차서 실행 3 하단, 로컬 검증 완료 — 이미 채워진 row 불변, 반복 실행 가능). 별도 모드·함수 분리는 하지 않는다(사용자 결정, PR #68 revert). Oracle 접속 변경은 상수 수동 편집
- **다음 단계**: 운영에서 UPDATE 버전 `load` 실행(①`DtTo` 수정 ②다른 Oracle 접속) → Trino 확인 → DAG 재개 → 며칠 뒤 임시 `DROP ... PURGE`. (이전) 개발 `backup`→DDL→`load` 전 구간 통과(2026-09-14, 코드 교체 후) — 사전 확인: `DtFrom`/`DtTo`가 운영 데이터 전체 기간을 덮는지, `gc.enabled=false` 여부, Airflow 중지 범위(append 외 Compaction·expire·orphan·재처리 포함), 대상 테이블 최근 2일 `FAILURE`·`IN_PROGRESS` 0건. 운영 적용 (Airflow 중지 → backup → DDL → load → Trino 확인 → 재개 → 며칠 뒤 임시 `DROP ... PURGE`) → 다른 테이블에 같은 절차 반복

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
    ├── recreate-table-tmp-id.md        # 테이블 재생성 + tmp_id(NOT NULL) 추가 절차서 (코드·DDL 포함, 작업 9)
    └── examples/
        ├── convert_file_taskgroup_example.py  # ConvertFileTaskGroup 변경(builder 인자) 예시
        ├── compaction_dag_example.py          # Compaction DAG 변경(tables 필터 = mapped task) 예시
        └── compaction_executor_sizing_example.py  # Compaction 사전 산정 예시 (보류안)
```
