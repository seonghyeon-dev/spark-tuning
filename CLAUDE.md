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

- Spark 4.1.1, Iceberg 1.10.1, Airflow 3.1.7
- Kubernetes 클러스터 (Spark Pod 실행 환경)
- S3 (MinIO) 스토리지 — Iceberg 테이블
- Trino — 조회 엔진 (DBeaver JDBC 드라이버로 실행)
- Oracle DB (처리 대상 상태 관리)
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
Compaction: 1시간(`15 * * * *`, 직전 1시간치) + 1일(`35 0 * * *`, 전일치) — 모든 전략에서 필수

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

- **산출물**: `pipeline/reprocessing-dag-design.md` (설계), `pipeline/dags/iceberg_reprocess.py` (구현 스켈레톤 — 기존 인프라 연결 지점은 TODO 표시)
- **상태**: 설계 확정, 구현 스켈레톤 작성 (기존 인프라 연결 대기)
- **배경**: append DAG의 Oracle 조회 기간(최근 1일 rolling — Job History `ts` 날짜 키 파티셔닝 제약)에서 밀려난 WAIT 데이터와, `get_jobs`가 조회하지 않는 FAILED 데이터가 영구 잔류하는 문제
- **시스템 구조**: Iceberg 테이블 20개+ (hourly/daily 그룹), append DAG은 py 1개에서 테이블별 동적 생성(약 5분 주기, `ts` string `YYYYMMDDHHmmSSsss` 기준 ORDER BY ASC, ROWNUM 200), Compaction DAG은 hourly/daily 각 1개(내부 테이블별 task 순차)
- **핵심 설계**:
  - 재처리 DAG **1개** (1일 주기, 01:00 KST), 테이블별 TaskGroup 순차 실행 (Compaction DAG 패턴)
  - 조회 범위 경계로 경합 원천 차단: FAILED는 전날+그저께 전체, WAIT는 전날 01:00 이전만 (append 하한 = 실행시각-24h ≥ 전날 01:00이므로 절대 안 겹침). 잠금/선점/pool 불필요
  - 상한: 테이블당 row 1,000 / 16GB (러프 설정, 재검증 필요). 초과 시 자기 자신 재trigger loop (상한 10회, `max_active_runs=1`로 순차)
  - 중복 적재 방지: snapshot summary에 batch_id 기록(영수증), FAILED 재적재 전 `.snapshots` 확인 → 커밋된 건 DONE 정정. batch_id는 `stat_desc` CLOB 재사용 — **WHERE 조건 사용 금지** (값 기록/읽기만)
  - Compaction: 기존 DAG trigger — daily `target_dt`, hourly `start_time`/`end_time` + 양쪽 `tables` multi-select params. daily 스케줄 00:35 → **02:00 이동** (재처리 적재 전날분 자연 커버 + 자정 지연 적재분 구멍 해소), hourly `15 * * * *` 유지
  - 수동 실행: `tables`(multi-select) + `target_dt` + `start_time`/`end_time` params
  - 좀비 IN_PROGRESS(2시간 초과): 탐지 + 알림만, 자동 복구 안 함
- **전제**: Iceberg snapshot 보존 3일 > 재처리 조회 범위 2일 유지 필수. Compaction DAG 변경(02:00, tables params)은 재처리 DAG 배포 전 적용

## 파일 구조

```
├── CLAUDE.md
├── tuning/
│   └── spark-tuning-guide.md          # Spark 튜닝 가이드
├── schema/
│   ├── iceberg-schema-design-guide.md  # Iceberg 스키마 설계 가이드
│   ├── read-performance-test.md        # 파티션 전략별 읽기 성능 비교 테스트
│   └── trino-query-guide.md            # Trino 쿼리 가이드 (사용자용)
└── pipeline/
    ├── reprocessing-dag-design.md      # 재처리 DAG 설계 가이드
    └── dags/
        └── iceberg_reprocess.py        # 재처리 DAG 구현 스켈레톤
```
