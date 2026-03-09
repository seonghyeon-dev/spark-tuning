# Handoff: Spark Job 설정 튜닝 가이드

## 현재 상태
- **주요 작업 완료**: `spark-tuning-guide.md` 작성 완료 (Confluence 복사/붙여넣기 가능)
- **num-executors 벤치마크 완료**: 24개 최적 확인 (`PARALLELISM_FACTOR=1.5`), 기존 60개 대비 리소스 60% 절감·성능 13% 향상
- **Shuffle 발생 확인**: 9.2GiB 규모 shuffle 확인 (Iceberg `write.distribution-mode`에 의한 파티션 키 기준 재분배)
- **다음 작업**: shuffle.partitions 벤치마크 (70 vs 200), parallelismFirst 전환 테스트

## 파일 구조

```
├── CLAUDE.md                  # 작업 지시서 (목표, 옵션별 요구사항, 포맷 등)
├── project-context.md         # 아키텍처, 워크플로우, 현재 설정값 상세
├── benchmarking-plan.md       # 벤치마크 계획서 (num-executors 결과 반영 완료)
├── spark-tuning-guide.md      # ✅ 최종 산출물 (벤치마크 결과 반영 완료)
└── handoff.md                 # 이 파일
```

## 완료된 작업

- 7개 Spark 옵션 상세 설명 작성 (공식 문서 기반 기본값, 권장값, 근거 수준 라벨)
- SparkKubernetesOperator Python 코드 예시 포함
- spark-submit 명령어 예시 포함
- 설정 근거 요약 표 포함
- Iceberg 내부 collect() 동작 분석 및 문서 반영
- 10분 주기 배치 실측 테스트 결과 분석
- 읽기 파티션 vs 셔플 파티션 개념 구분 설명 추가
- num-executors 개선 산정식: `ceil(총 크기 / 128MB × 1.5 / executor-cores)`
- **num-executors 벤치마크 완료**: 16/24/32/60개 비교 → 24개 최적 (44초)
- **Shuffle 발생 확인**: Stage 분석 (9.2GiB shuffle, Stage 5→7)
- PARALLELISM_FACTOR 설명 추가 (Spark 옵션이 아닌 산정식의 여유 계수)

## 다음에 해야 할 작업

`spark-tuning-guide.md`의 개선 로드맵(5.3절)에 따라 벤치마크를 수행한다.

| 순서 | 항목 | 현재 상태 |
|------|------|-----------|
| 테스트 1 | `num-executors` 최적값 (16/24/32/60 비교) | ✅ 완료 (24개 최적) |
| 테스트 2 | `spark.sql.shuffle.partitions` (70 vs 200 비교) | ⚠️ 벤치마크 대기 |
| 테스트 3 | `parallelismFirst` false 전환 (Iceberg 소파일 개선) | ⚠️ 벤치마크 대기 |

## 핵심 맥락 요약

- **워크플로우**: Airflow DAG → get_jobs → append_data(SparkKubernetesOperator) → avro read → Iceberg append
- **환경**: Spark 4.1.1, Iceberg 1.10.1, Airflow 3.1.7, K8S, S3(MinIO)
- **확정 설정**: driver-cores=1, driver-memory=2g, executor-cores=4, executor-memory=8g, **num-executors=24 (벤치마크 검증 완료)**
- **데이터 특성**: avro 파일당 평균 1.58MB (소파일), 10분 주기 배치 기준 총 ~8GB
- **핵심 발견**:
  - 기존 num-executors 산정식(70MB 기준, 116개)은 과다 할당. 개선 산정식(128MB+1.5배, 24개)이 최적
  - Shuffle 9.2GiB 발생 확인 → shuffle.partitions 설정이 성능에 영향을 미침
