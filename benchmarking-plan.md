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

| 테스트 ID | num-executors | 산정 근거 | 총 코어 수 |
|-----------|---------------|----------|-----------|
| NE-01 | 16 | 최소 1라운드 처리 (`ceil(8080/128/4)`) | 64 |
| NE-02 | 24 | 16 × 1.5 (여유 50%) | 96 |
| NE-03 | 32 | 개선 산정식 (`PARALLELISM_FACTOR=2`) | 128 |
| NE-04 | 60 | 기존 테스트값 (비교 기준선, 1.1분 소요) | 240 |

> **산정식 변경 배경**: 기존 "분할 기준(70MB) 변경" 방식에서, executor-cores를 반영한 "Executor 수 직접 산정" 방식으로 테스트 방향을 변경함.
> 기존 산정식 `total_size / 70MB + 1 = 116`은 실제 필요량 대비 과다 할당이 확인됨.

3. 실측 데이터셋 (10분 주기 배치 기준)

| 항목 | 값 |
|------|----|
| Job 수 | 234개 |
| Avro 파일 수 | 5,355개 |
| Avro 파일당 평균 크기 | ~1.58MB (소파일) |
| Avro 총 크기 | ~8.08GB (8,474,665,536 bytes) |
| Parquet 총 크기 (Iceberg 적재 후) | ~6.54GB (7,023,859,378 bytes) |
| 압축률 (Parquet/Avro) | ~82.9% |
| 총 Row 수 | 628,699건 |
| 추정 읽기 파티션 수 | ~63개 (`8,080MB / 128MB`) |

> **참고**: 기존 계획의 데이터셋(Small~XLarge)은 가상 데이터셋이었으나, 실측 결과 10분 주기 배치가 ~8GB 규모로 확인됨. 벤치마크는 실측 데이터셋 기준으로 수행한다.

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

#### 벤치마크 결과 ✅

| 테스트 ID | num-executors | 총 코어 수 | 소요시간 | 결과 |
|-----------|---------------|-----------|---------|------|
| NE-01 | 16 | 64 | 55초 | 코어 부족 |
| **NE-02** | **24** | **96** | **44초** | **✅ 최적** |
| NE-03 | 32 | 128 | 51초 | 오버헤드 발생 |
| NE-04 | 60 | 240 | 51초 | 과다 할당 확인 |

**결론**: `PARALLELISM_FACTOR=1.5`(24개)가 최적. 산정식: `ceil(총 크기 / 128MB × 1.5 / executor-cores)`

#### Shuffle 발생 확인 (Stage 분석) ✅

Spark UI Stages 탭에서 확인한 결과, **shuffle이 발생한다** (9.2GiB 규모).

| Stage | 역할 | Input | Shuffle Write | Shuffle Read | Output |
|-------|------|-------|---------------|--------------|--------|
| 0 | collect (메타데이터) | 580.6KiB | 163.8KiB | - | - |
| 1 | collect (skipped) | - | - | - | - |
| 2 | collect (메타데이터) | - | - | 163.9KiB | - |
| 3 | 파일 목록 조회 (5355 paths) | - | - | - | - |
| 4 | append (avro 읽기) | 7.9GiB | - | - | - |
| 5 | append (shuffle 준비) | 7.9GiB | **9.2GiB** | - | - |
| 7 | append (Iceberg 쓰기) | - | - | **9.2GiB** | 6.5GiB |

- Stage 5→7에서 Iceberg `write.distribution-mode`에 의한 대규모 shuffle 발생
- Stage 0/2의 소량 shuffle(163.8KiB)은 Iceberg 메타데이터 collect 관련으로 무시 가능

---

### 1.2 spark.sql.shuffle.partitions (우선순위: P2)

**현재 상태**: 70
**문제점**: 기본값 200 대비 낮게 설정했으나 근거 없음
**선행 확인 완료**: shuffle 9.2GiB 발생 확인 → 이 옵션은 성능에 영향을 미침

#### 검증 방법
1. 고정 변수: executor-cores=4, executor-memory=8g, num-executors=24 (NE 최적값)
2. 변경 변수:

| 테스트 ID | 파티션 수 | 비고 |
|-----------|-----------|------|
| SP-01 | 70 | 현재 설정 (baseline) |
| SP-02 | 200 | Spark 기본값. AQE가 자동 조정하도록 위임 |

3. 측정 지표: 1.1과 동일 + 셔플 파티션당 데이터 크기 분포

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

### num-executors 결과 (✅ 완료)

| 테스트 ID | num-executors | 실행시간(s) | 비고 |
|-----------|---------------|------------|------|
| NE-01 | 16 | 55 | 코어 부족 |
| **NE-02** | **24** | **44** | **✅ 최적** |
| NE-03 | 32 | 51 | 오버헤드 발생 |
| NE-04 | 60 | 51 | 과다 할당 |

### shuffle.partitions / parallelismFirst 결과 (⚠️ 대기)

| 테스트 ID | 실행시간(s) | Task 중앙값(s) | Task p95(s) | GC 비율(%) | 메모리 peak(%) | 셔플 크기 | 파일 수 |
|-----------|------------|----------------|-------------|------------|---------------|-----------|---------|
| | | | | | | | |

### 결론 도출
- **num-executors**: 24개 최적 (`PARALLELISM_FACTOR=1.5`). 기존 60개 대비 리소스 60% 절감, 성능 13% 향상
- shuffle.partitions, parallelismFirst: 벤치마크 대기

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
