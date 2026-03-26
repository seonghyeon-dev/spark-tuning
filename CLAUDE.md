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
- 파티션 / Sort Order: 테스트 결과에 따라 변동 가능
- array 타입 컬럼 8개: `write.metadata.metrics.column.*` = `none`
- `write.distribution-mode`: `range`
- 조회 패턴: ts, par_a, par_b, sort_a, sort_b, sort_c — **6개 모두 필수**

### 워크플로우

Airflow DAG → avro read → Iceberg append (10분 주기 배치, ~8GB)

### 참고 공식 문서

- Spark 4.1.1 Configuration: https://spark.apache.org/docs/4.1.1/configuration.html
- Spark 4.1.1 SQL Performance Tuning: https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html
- Spark on Kubernetes: https://spark.apache.org/docs/4.1.1/running-on-kubernetes.html
- Iceberg Spark Configuration: https://iceberg.apache.org/docs/latest/spark-configuration/

## 작업 1: Spark 튜닝 가이드 — 완료

- **산출물**: `tuning/spark-tuning-guide.md`
- **상태**: 7개 설정 확정, 벤치마크 검증 완료
- **대기**: 파티션/Sort Order 최종 확정 후 벤치마크 재검증

## 작업 2: Iceberg 스키마 설계 — 테스트 진행 중

- **산출물**: `schema/iceberg-schema-design-guide.md`
- **상태**: 5개 전략(A~E안) 분석 완료, 읽기 성능 비교 테스트 진행 중
- **테스트 현황** (`schema/read-performance-test.md`):
  - Hive-orc(as-is) vs A안 vs B안 비교 중, 다른 전략도 추가 예정
  - 초기 테스트 결과: A안 조회 성능 우위 (par_b 파티션 프루닝 > B안 Data Skipping)
  - B안은 파일 크기 균등 (avg 495.7MB), A안은 Compaction 후에도 Skew (min 0.6MB)

## 파일 구조

```
├── CLAUDE.md
├── tuning/
│   └── spark-tuning-guide.md          # Spark 튜닝 가이드
└── schema/
    ├── iceberg-schema-design-guide.md  # Iceberg 스키마 설계 가이드
    └── read-performance-test.md        # 파티션 전략별 읽기 성능 비교 테스트
```
