# Handoff: Spark Job 설정 튜닝 가이드

## 현재 상태

- `spark-tuning-guide.md` 작성 완료 (Confluence 복사/붙여넣기 가능)
- 7개 Spark 옵션 벤치마크 검증 완료
- **다음 작업**: Iceberg 테이블 파티션/write ordering 최종 확정 후 벤치마크 재검증

## 파일 구조

```
├── CLAUDE.md                  # 작업 지시서
├── project-context.md         # 아키텍처, 워크플로우, 현재 설정값
├── benchmarking-plan.md       # 벤치마크 계획서 및 결과
├── spark-tuning-guide.md      # ✅ 최종 산출물
└── handoff.md                 # 이 파일
```

## 확정 설정

| 옵션 | 값 | 근거 |
|------|-----|------|
| driver-cores | 1 | 📘 일반적 관행 |
| driver-memory | 2g | 📘 일반적 관행 |
| executor-cores | 4 | 📘 일반적 관행 |
| executor-memory | 8g | 📘 일반적 관행 |
| num-executors | ceil(총크기/128MB×1.5/cores) | ✅ 벤치마크 (24개 최적, 44초) |
| shuffle.partitions | 200 (기본값, 설정 불필요) | ✅ 벤치마크 (70 설정 시 1~2초 느림) |
| parallelismFirst | true (기본값, 설정 불필요) | ✅ 벤치마크 (false 시 23% 느림) |

## 핵심 맥락

- **워크플로우**: Airflow DAG → avro read → Iceberg append (10분 주기 배치, ~8GB)
- **환경**: Spark 4.1.1, Iceberg 1.10.1, Airflow 3.1.7, K8S, S3(MinIO)
- **테이블 (TABLE_A)**: 컬럼 19개, 파티션 3개(`day(ts)`, `column_a`, `column_b`), write ordering 3개
- **Shuffle**: 9.2GiB 발생 (파티션 키 + write ordering에 의한 재분배)
- **파티션/정렬 변동 가능**: 하루치 데이터 적재 + 컴팩션 후 최종 결정 → 변경 시 재검증 필요
- **컴팩션 운영 예정**: 시간당/일당 주기 → parallelismFirst=true 유지 근거
