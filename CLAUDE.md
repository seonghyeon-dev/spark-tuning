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

## 작업 5: Compaction 튜닝 (hourly) — 4개 설정 확정, num-executors 동적 산정 대기

- **산출물**: `tuning/compaction-tuning-guide.md`
- **상태**: hourly Compaction 설정 확정 (초/GB **3.24 → 1.88, −42%**), `num-executors` 동적 산정 계수 미확정
- **대상**: hourly 테이블 4개 (파티션 `hour(ts)`/`col_a`, sort `col_b`/`col_c`, `range` 모드 동일). rewrite 전략은 `sort` — 미적용 시 조회 40% 저하(`read-performance-test.md` §5.4)라 필수
- **확정 설정**: `max-concurrent-file-group-rewrites` 2→**10**(−30%), `max-file-group-size-bytes` 10GB→**기본값 100GB**(−16% + small file 제거), `driver cpu` 1→**2**, `advisory-partition-size` **삭제**(무효 확인). `rewrite-all=true`·`parallelismFirst=false`·`partial-progress=false`·executor 4core/16GB는 유지
- **핵심 발견**:
  - **file group이 처리 단위다.** `file group 수 = Σ ceil(파티션 크기 ÷ max-file-group-size-bytes)`. 초기엔 7개를 2개씩 처리해 **4회차**로 나뉘고, 그중 2회차가 데이터 8%에 시간 37%를 썼다 (`idle cores 58%`)
  - **작은 file group은 small file을 만든다.** 출력 파일 수 = `ceil(group 크기 ÷ 512MB)`이므로 **512~768MB 자투리 group은 반드시 384MB 미만 파일 2개**를 만든다 (실측 288.9/312.5/362.4MB = 각 577.8/625.0/724.8MB ÷ 2). 분할을 없애는 것이 해결책이므로 **상한은 높을수록 안전** — 그래서 30GB가 아니라 기본값 100GB
  - **출력 파일 크기의 손잡이는 `target-file-size-bytes` 하나다.** `advisory-partition-size`는 Iceberg가 덮어써서 무효 (5회 실측 전부 `ceil(총 크기÷512MB)`와 일치)
  - **`sort` 전략은 데이터를 2번 읽는다** (정렬 범위 샘플링 + 실제 쓰기). DataFlint `input = output × 2.0`이 정상값
  - **DataFlint alert 처방을 그대로 따르면 안 된다.** `idle cores` 원인은 리소스 과다(→executor 축소)와 병렬성 제약(→제약 해제) 두 가지이고, 이번은 후자였다. alert는 전자만 제안한다
  - **`memory usage` 84~94%는 `spill to disk 0b`와 짝으로 읽는다** — 낭비 없이 맞게 쓰는 중이라는 뜻이며 줄이면 spill이 시작된다
- **동적 산정**: 당초 6개 값을 동적화하려 했으나 **`num-executors` 하나로 좁혀졌다** (나머지는 데이터 양과 무관하거나 크게 고정하는 것이 우월). `num_executors = ceil(총 크기GB × C)`, 현재 검증된 C=0.43(16 executor/37GB, idle 24.9%). **12·8 두 지점 측정으로 C 확정 예정 — 조건은 `spill 0` 유지**. 입력 측정은 `.files`의 `sum(file_size_in_bytes)`만으로 충분(입력이 전부 small file). 구현 위치는 `compaction_dag_example.py`의 `compaction_specs` → `instances`
- **후속 과제**: **daily Compaction의 `rewrite-all` 낭비 의심** — hourly가 정리한 뒤라 no-op이어야 하는데 888GB에 30~60분(데이터 양에 선형). daily 단계 최우선 확인 항목

## 파일 구조

```
├── CLAUDE.md
├── tuning/
│   ├── spark-tuning-guide.md          # Spark 튜닝 가이드 (append Job)
│   └── compaction-tuning-guide.md     # Compaction 튜닝 가이드 (hourly)
├── schema/
│   ├── iceberg-schema-design-guide.md  # Iceberg 스키마 설계 가이드
│   ├── read-performance-test.md        # 파티션 전략별 읽기 성능 비교 테스트
│   ├── spark-query-metrics-guide.md    # Spark 쿼리 메트릭 가이드
│   └── trino-query-guide.md            # Trino 쿼리 가이드 (사용자용)
└── pipeline/
    ├── reprocessing-dag-design.md      # 재처리 DAG 설계 가이드
    ├── reprocess-flow.md               # 재처리 DAG 처리 흐름 (보고용 요약)
    ├── dags/
    │   └── iceberg_reprocess.py        # 재처리 DAG 정의 (신규 파일은 이것 하나)
    └── examples/
        ├── convert_file_taskgroup_example.py  # ConvertFileTaskGroup 변경(builder 인자) 예시
        └── compaction_dag_example.py          # Compaction DAG 변경(tables 필터 = mapped task) 예시
```
