# Handoff: Spark Job 설정 튜닝 가이드

## 현재 상태
- **주요 작업 완료**: `spark-tuning-guide.md` 작성 완료 (Confluence 복사/붙여넣기 가능)
- **다음 작업**: 벤치마크 수행 후 가이드 문서 내 개선 필요 항목 업데이트

## 파일 구조

```
├── CLAUDE.md                  # 작업 지시서 (목표, 옵션별 요구사항, 포맷 등)
├── project-context.md         # 아키텍처, 워크플로우, 현재 설정값 상세
├── benchmarking-plan.md       # num-executors / shuffle.partitions 등 벤치마크 계획서
├── spark-tuning-guide.md      # ✅ 최종 산출물 (완성)
└── handoff.md                 # 이 파일
```

## 완료된 작업

- 7개 Spark 옵션 상세 설명 작성 (공식 문서 기반 기본값, 권장값, 근거 수준 라벨)
- SparkKubernetesOperator Python 코드 예시 포함
- spark-submit 명령어 예시 포함
- 설정 근거 요약 표 포함
- 개선 로드맵(P1/P2/P3) 포함

## 다음에 해야 할 작업

`benchmarking-plan.md`의 계획에 따라 벤치마크를 수행하고, 결과를 바탕으로 `spark-tuning-guide.md`의 아래 항목을 업데이트한다.

| 우선순위 | 항목 | 현재 상태 |
|---------|------|-----------|
| P1 | `num-executors` 분할 기준 (70MB) | ⚠️ 테스트 기반. 벤치마크로 재검증 필요 |
| P2 | `spark.sql.shuffle.partitions` (70) | ⚠️ 테스트 기반. 실제 shuffle 크기 측정 후 재산정 필요 |
| P3 | `spark.sql.adaptive.coalescePartitions.parallelismFirst` | 공식 권장은 `false`. 전환 시 Iceberg 소파일 개선 효과 검증 필요 |

## 핵심 맥락 요약

- **워크플로우**: Airflow DAG → get_jobs → append_data(SparkKubernetesOperator) → avro read → Iceberg append
- **환경**: Spark 4.1.1, Iceberg 1.10.1, Airflow 3.1.7, K8S, S3(MinIO)
- **현재 권장 설정**: driver-cores=1, driver-memory=2g, executor-cores=4, executor-memory=8g
