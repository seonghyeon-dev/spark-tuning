# Spark + Iceberg 파이프라인 가이드

Spark → Iceberg(S3/MinIO, HMS) → Trino 로 이어지는 배치 파이프라인의 **튜닝·스키마 설계·운영 가이드** 모음이다.
각 문서는 실측 근거와 결정 사유를 함께 담고 있으며, 전체 작업 현황과 확정값은 [프로젝트 현황](CLAUDE.md)에 정리돼 있다.

## 기술 스택

| 구성 | 값 |
|------|-----|
| Spark | 3.5.8 (운영, 임시 다운그레이드 — 목표 4.1.1) |
| Iceberg | 1.10.1 (카탈로그: HMS) |
| Airflow | 3.2.2 (SparkKubernetesOperator) |
| 스토리지 | S3 (MinIO) |
| 조회 엔진 | Trino 482 |
| 상태 관리 | Oracle DB (Job History) |

## 문서 안내

### 튜닝

| 문서 | 내용 |
|------|------|
| [Spark 튜닝 가이드](tuning/spark-tuning-guide.md) | append Job 설정 7개 확정, 벤치마크 검증 |
| [Compaction 튜닝 가이드](tuning/compaction-tuning-guide.md) | hourly Compaction 9회 측정, 설정 확정 (상세) |
| [Compaction 튜닝 결과](tuning/compaction-tuning-report.md) | 회의 보고용 요약 |
| [Trino Partition Pruning 검증](tuning/trino-iceberg-partition-pruning.md) | `hour(ts)` Pruning 실측, Trino 쿼리 가이드의 근거 계층 |

### 스키마

| 문서 | 내용 |
|------|------|
| [Iceberg 스키마 설계 가이드](schema/iceberg-schema-design-guide.md) | 파티션 `hour(ts)`, `par_a` + Sort Order `sort_a`, `sort_b` 확정 근거 |
| [읽기 성능 비교 테스트](schema/read-performance-test.md) | 파티션 전략 5개 비교, Sort Order/Bloom Filter 조합 비교 |
| [Trino 쿼리 가이드](schema/trino-query-guide.md) | Trino 사용자용 — `ts` 필터링 방법, WHERE 필수 컬럼, 잘못된 패턴 |
| [Spark 쿼리 메트릭 가이드](schema/spark-query-metrics-guide.md) | Spark History Server SQL 탭의 Iceberg Scan 메트릭 해설 |

### 파이프라인

| 문서 | 내용 |
|------|------|
| [FileIO 전환 가이드](pipeline/s3fileio-migration-guide.md) | S3AFileSystem → S3FileIO 전환 (상세), Iceberg 1.11 / Spark 4.1 업그레이드 검토 부록 |
| [FileIO 전환 결과](pipeline/s3fileio-migration-report.md) | MinIO API 호출 감소 실측 (보고용) |
| [재처리 DAG 설계](pipeline/reprocessing-dag-design.md) | 잔류 WAIT_SCHEDULING / FAILURE 데이터 재처리 DAG 설계 |
| [재처리 DAG 처리 흐름](pipeline/reprocess-flow.md) | 보고용 흐름 요약 |
| [Compaction executor 자원 할당 설계](pipeline/compaction-executor-sizing-design.md) | Dynamic Allocation + ratio 채택 근거 |

## 문서 사이트 로컬 실행

이 저장소는 [MkDocs Material](https://squidfunk.github.io/mkdocs-material/)로 GitHub Pages에 배포된다. 로컬에서 미리 보려면:

```bash
pip install -r requirements-docs.txt
mkdocs serve
```

`main` 브랜치에 push 되면 `.github/workflows/docs.yml` 워크플로우가 사이트를 빌드해 GitHub Pages로 배포한다.
