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

## 작업 2: Iceberg 스키마 설계 가이드 ✅ 완료

- **산출물**: `schema/iceberg-schema-design-guide.md` (Confluence 복사/붙여넣기용)
- **상태**: 실데이터 기반 분석 완료, 부록 본문 통합, 용어 통일 완료
- **확정 사항**:
  - 파티션: `day(ts)` ✅, `par_a`(identity, Cardinality 4) ✅, `par_b`(identity, 조합 248개) ✅
  - 파티션 프루닝: `day(ts)` + `par_a` + `par_b` 모두 유효 (WHERE 절 항상 포함 확정)
  - Sort Order: `sort_a, sort_b, sort_c` (sort_b ≠ par_b, 별개 컬럼)
  - Bucketing 불필요, Z-ordering은 2단계 검토
- **Compaction 실측**: 23,789 → 1,834 파일 (92% 감소), 850,955 → 850,695 MB
- **추천 전략** (7.1절):
  - A안 (권장): 현행 identity 유지, Compaction 실측 검증 완료. 프루닝 1/248 최대
  - B안 (차선): truncate(3, par_b) — 파일 수 46% 감소, 프루닝 1/135 유지
  - C안 (절충): bucket(16, par_b) — 파일 수 74% 감소, 프루닝 1/16
- **잔여 작업**:
  - sort_b, sort_c 단독 필터 빈도 추가 확인 → Z-ordering 전환 여부 결정

## 파일 구조

```
├── CLAUDE.md                      # 이 파일
├── tuning/
│   └── spark-tuning-guide.md      # ✅ Spark 튜닝 가이드 완성본
└── schema/
    └── iceberg-schema-design-guide.md  # ✅ Iceberg 스키마 설계 가이드 완성본
```
