# 벤치마크 계획서: Spark Job 설정 최적화

## 목적
현재 테스트 기반으로 결정된 설정값들에 대해 체계적인 벤치마크를 수행하여, 근거 있는 최적 설정값을 도출한다.

---

## 1. 검증 대상

### 1.1 num-executors 분할 기준 (우선순위: P1)

**현재 상태**: `avro 파일 총 크기 / 70MB + 1`
**문제점**: 70MB 기준에 대한 이론적 근거 없음

#### 검증 방법
1. 고정 변수: executor-cores=4, executor-memory=8g
2. 변경 변수: 분할 기준을 변경하며 테스트

| 테스트 ID | 분할 기준 | 비고 |
|-----------|-----------|------|
| NE-01 | 32MB | Spark 기본 파티션 크기의 1/4 |
| NE-02 | 64MB | AQE advisoryPartitionSizeInBytes 기본값 |
| NE-03 | 70MB | 현재 설정 (baseline) |
| NE-04 | 128MB | spark.sql.files.maxPartitionBytes 기본값 |
| NE-05 | 256MB | 대용량 파티션 테스트 |

3. 각 테스트에 사용할 데이터셋

| 데이터셋 | 총 크기 | 파일 수 | 파일당 평균 크기 |
|----------|---------|---------|------------------|
| Small | ~500MB | ~50개 | ~10MB |
| Medium | ~2GB | ~100개 | ~20MB |
| Large | ~10GB | ~200개 | ~50MB |
| XLarge | ~50GB | ~500개 | ~100MB |

4. 측정 지표
   - 총 실행 시간 (wall clock)
   - 각 Stage 소요 시간
   - Task 소요 시간 중앙값 / p95 / max
   - Executor 메모리 사용률 (peak)
   - GC 시간 비율
   - S3 read throughput
   - Shuffle read/write 크기
   - 생성된 Iceberg 파일 수 및 크기 분포

5. 기대 결과
   - 분할 기준과 실행 시간의 관계 그래프
   - 최적 분할 기준값 도출 (또는 데이터 크기별 동적 기준)

### 1.2 spark.sql.shuffle.partitions (우선순위: P2)

**현재 상태**: 70
**문제점**: 기본값 200 대비 낮게 설정했으나 근거 없음

#### 검증 방법
1. 고정 변수: executor-cores=4, executor-memory=8g, num-executors=NE-01~05 최적값 사용
2. 변경 변수:

| 테스트 ID | 파티션 수 | 비고 |
|-----------|-----------|------|
| SP-01 | 50 | executor 수 × cores에 근접 |
| SP-02 | 70 | 현재 설정 (baseline) |
| SP-03 | 100 | |
| SP-04 | 200 | Spark 기본값 |
| SP-05 | AQE 자동 | shuffle.partitions=200 + AQE에 맡김 |

3. 측정 지표: 1.1과 동일 + 셔플 파티션당 데이터 크기 분포

4. 추가 확인사항
   - avro → Iceberg append에서 실제로 shuffle이 발생하는지 확인 (Spark UI의 Stage DAG 확인)
   - shuffle이 없다면 이 설정의 영향도는 낮음
   - Iceberg의 write.distribution-mode에 따라 shuffle 발생 여부가 달라짐

### 1.3 Executor 리소스 (우선순위: P3)

**현재 상태**: 일반적 값 사용 (명확한 근거 없음)

#### 검증 방법
1. num-executors를 동일 총 코어 수가 되도록 조정하여 비교

| 테스트 ID | cores | memory | executors | 총 코어 | 총 메모리 |
|-----------|-------|--------|-----------|---------|-----------|
| EX-01 | 2 | 4g | 8 | 16 | 32g |
| EX-02 | 4 | 8g | 4 | 16 | 32g |
| EX-03 | 4 | 16g | 4 | 16 | 64g |
| EX-04 | 5 | 10g | 3 | 15 | 30g |
| EX-05 | 8 | 16g | 2 | 16 | 32g |

2. 측정 지표: 1.1과 동일 + K8S Pod 스케줄링 대기 시간

---

## 2. 벤치마크 실행 환경

### 사전 조건
- 벤치마크 전용 K8S namespace 사용 (다른 워크로드 영향 차단)
- 동일한 K8S 노드 풀에서 실행
- MinIO 서버 부하가 낮은 시간대에 수행 (새벽 권장)
- 각 테스트 최소 3회 반복 (중앙값 사용)

### 데이터 수집 방법
- **Spark UI**: History Server에서 각 Job의 Stage/Task 메트릭 확인
- **K8S 메트릭**: `kubectl top pod` 또는 Prometheus/Grafana로 CPU/메모리 사용률
- **실행 시간**: Airflow Task 시작~종료 시간 (xcom 또는 로그)
- **Iceberg 메타데이터**: 생성된 데이터 파일 수, 크기 분포 확인
  ```sql
  SELECT file_path, file_size_in_bytes, record_count
  FROM iceberg_catalog.<table>.files
  ```

---

## 3. 결과 정리 템플릿

### 테스트 결과 기록

| 테스트 ID | 실행시간(s) | Task 중앙값(s) | Task p95(s) | GC 비율(%) | 메모리 peak(%) | 셔플 크기 | 파일 수 |
|-----------|------------|----------------|-------------|------------|---------------|-----------|---------|
| | | | | | | | |

### 결론 도출
- 각 검증 대상별 최적값과 근거
- 데이터 크기별 동적 설정이 필요한지 여부
- 설정 변경 전후 비교 (현재 설정 vs 최적 설정)

---

## 4. 일정 (제안)

| 단계 | 작업 | 소요 기간 |
|------|------|-----------|
| 준비 | 벤치마크 데이터셋 준비, 스크립트 작성 | 2일 |
| P1 | num-executors 분할 기준 벤치마크 (5케이스 × 4데이터셋 × 3회) | 3일 |
| P2 | shuffle.partitions 벤치마크 (5케이스 × 3데이터셋 × 3회) | 2일 |
| P3 | Executor 리소스 벤치마크 (5케이스 × 2데이터셋 × 3회) | 2일 |
| 정리 | 결과 분석, 가이드 문서 업데이트 | 2일 |
| **합계** | | **약 11일** |

---

## 5. 참고: 벤치마크 자동화 스크립트 구조 (제안)

```
benchmark/
├── run_benchmark.py          # 벤치마크 실행 스크립트 (Airflow API 호출)
├── configs/
│   ├── ne_tests.yaml         # num-executors 테스트 설정들
│   ├── sp_tests.yaml         # shuffle.partitions 테스트 설정들
│   └── ex_tests.yaml         # executor 리소스 테스트 설정들
├── datasets/
│   └── prepare_datasets.py   # 테스트용 데이터셋 생성
├── collect_metrics.py        # Spark History Server에서 메트릭 수집
└── report/
    └── generate_report.py    # 결과 그래프 및 리포트 생성
```
