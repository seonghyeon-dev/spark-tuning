# Handoff: Spark Job 설정 튜닝 가이드

## 현재 상태
- **주요 작업 완료**: `spark-tuning-guide.md` 작성 완료 (Confluence 복사/붙여넣기 가능)
- **테스트 결과 반영**: 10분 주기 배치 실측 데이터 기반 분석 완료
- **개선 산정식 제안**: num-executors 산정식을 `maxPartitionBytes`(128MB)와 `executor-cores` 반영 방식으로 개선
- **다음 작업**: 벤치마크 수행 (num-executors 최적값 검증, shuffle 발생 여부 확인, parallelismFirst 전환)

## 파일 구조

```
├── CLAUDE.md                  # 작업 지시서 (목표, 옵션별 요구사항, 포맷 등)
├── project-context.md         # 아키텍처, 워크플로우, 현재 설정값 상세
├── benchmarking-plan.md       # num-executors / shuffle.partitions 등 벤치마크 계획서
├── spark-tuning-guide.md      # ✅ 최종 산출물 (테스트 결과 반영 완료)
└── handoff.md                 # 이 파일
```

## 완료된 작업

- 7개 Spark 옵션 상세 설명 작성 (공식 문서 기반 기본값, 권장값, 근거 수준 라벨)
- SparkKubernetesOperator Python 코드 예시 포함
- spark-submit 명령어 예시 포함
- 설정 근거 요약 표 포함
- Iceberg 내부 collect() 동작 분석 및 문서 반영 (Spark History UI에서 보이는 collect at SparkWrite.java 원인)
- 10분 주기 배치 실측 테스트 결과 분석 (8GB avro, 5,355파일, Executor 60개, 1.1분)
- 읽기 파티션 vs 셔플 파티션 개념 구분 설명 추가
- num-executors 개선 산정식 제안: `ceil(총 크기 / 128MB × PARALLELISM_FACTOR / executor-cores)`
- 벤치마크 테스트 케이스 구체화 (테스트 1/2/3 순서 및 확인 사항)

## 다음에 해야 할 작업

`spark-tuning-guide.md`의 개선 로드맵(5.3절)에 따라 벤치마크를 수행한다.

| 순서 | 항목 | 현재 상태 |
|------|------|-----------|
| 테스트 1 | `num-executors` 최적값 (16/24/32/60 비교) | ⚠️ 벤치마크 대기 |
| 테스트 2 | `spark.sql.shuffle.partitions` (shuffle 발생 여부 확인 선행) | ⚠️ 벤치마크 대기 |
| 테스트 3 | `parallelismFirst` false 전환 (Iceberg 소파일 개선) | ⚠️ 벤치마크 대기 |

## 핵심 맥락 요약

- **워크플로우**: Airflow DAG → get_jobs → append_data(SparkKubernetesOperator) → avro read → Iceberg append
- **환경**: Spark 4.1.1, Iceberg 1.10.1, Airflow 3.1.7, K8S, S3(MinIO)
- **현재 권장 설정**: driver-cores=1, driver-memory=2g, executor-cores=4, executor-memory=8g
- **데이터 특성**: avro 파일당 평균 1.58MB (소파일), 10분 주기 배치 기준 총 ~8GB
- **핵심 발견**: 기존 num-executors 산정식(70MB 기준)은 과다 할당. 개선 산정식은 32개 (기존 대비 ~1/3.6)
