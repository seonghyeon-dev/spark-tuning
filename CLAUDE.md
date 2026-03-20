# Spark + Iceberg 파이프라인 가이드

## Skills

커스텀 검증 및 유지보수 스킬은 `.claude/skills/`에 정의되어 있습니다.

| Skill | Purpose |
|-------|---------|
| `verify-implementation` | 프로젝트의 모든 verify 스킬을 순차 실행하여 통합 검증 보고서를 생성합니다 |
| `manage-skills` | 세션 변경사항을 분석하고, 검증 스킬을 생성/업데이트하며, CLAUDE.md를 관리합니다 |

## Code Style Rules

- 커밋 메시지는 한글로 작성
- 결과값과 설명은 무조건 한글로 작성
- Confluence 호환 마크다운 (표, 코드블록, 헤더, 인용블록 등)

## 공통 컨텍스트

### 기술 스택

- Spark 4.1.1
- Iceberg 1.10.1
- Airflow 3.1.7
- Kubernetes 클러스터 (Spark Pod 실행 환경)
- S3 (MinIO) 스토리지
- Oracle DB (처리 대상 상태 관리)
- SparkKubernetesOperator (kubeflow)

### 대상 테이블 (TABLE_A)

- 컬럼 수: 19개 (timestamp_ntz, string, double, integer, array<integer>, array<double>, array<string>)
- 파티션 (3개): `day(ts)`, `par_a`, `par_b` — 변동 가능
- Sort Order (3개): `sort_a`, `sort_b`, `sort_c` ASC NULLS FIRST — 변동 가능
- array 타입 컬럼 8개 (val_arr_1~4, yn_arr, tval_1~3): `write.metadata.metrics.column.*` = `none`
- `write.distribution-mode`: `range`

### 워크플로우

Airflow DAG → avro read → Iceberg append (10분 주기 배치, ~8GB)

### 참고 공식 문서

- Spark 4.1.1 Configuration: https://spark.apache.org/docs/4.1.1/configuration.html
- Spark 4.1.1 SQL Performance Tuning: https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html
- Spark on Kubernetes: https://spark.apache.org/docs/4.1.1/running-on-kubernetes.html
- Iceberg 1.10.1 Spark Configuration: https://iceberg.apache.org/docs/latest/spark-configuration/

## 작업 1: Spark 튜닝 가이드 ✅ 완료

- **산출물**: `tuning/spark-tuning-guide.md` (Confluence 복사/붙여넣기용)
- **상태**: 7개 설정 확정, 벤치마크 검증 완료
- **대기**: 파티션/Sort Order 최종 확정 후 벤치마크 재검증

## 작업 2: Iceberg 스키마 설계 가이드 — 테스트 대기

- **산출물**: `schema/iceberg-schema-design-guide.md` (Confluence 복사/붙여넣기용)
- **상태**: 5개 전략(A~E안) 분석 완료, A안 vs D안 실측 비교 테스트 대기
- **조회 패턴 (확정)**:
  - ts(날짜 또는 시간), par_a, par_b, sort_a, sort_b, sort_c — **모두 필수**
  - 테이블 설정(파티션, Sort Order 등)은 테스트 결과에 따라 수정 가능
- **Compaction 실측**: 23,789 → 1,834 파일 (92% 감소), 850,955 → 850,695 MB
- **전략 비교** (7.1절):
  - A안: 현행 identity 유지 + Compaction 필수. 프루닝 1/248, Skew 있음
  - B안: truncate(3, par_b) — 파일 수 46% 감소, 프루닝 1/135
  - C안: bucket(16, par_b) — 파일 수 74% 감소, 프루닝 1/16
  - D안: hour(ts) + par_a — 파티션 구조 변경, par_b Sort Order 이동, Compaction 선택적
  - E안: bucket(N, hash_val) — 멀티 컬럼 해시 버킷, Spark-Trino 해시 호환성 검증 필요
- **다음 작업**: A안 vs D안 테스트 실행 (8절 테스트 계획 참조)

## 파일 구조

```
├── CLAUDE.md                      # 이 파일
├── tuning/
│   └── spark-tuning-guide.md      # ✅ Spark 튜닝 가이드 완성본
└── schema/
    └── iceberg-schema-design-guide.md  # ✅ Iceberg 스키마 설계 가이드 완성본
```
