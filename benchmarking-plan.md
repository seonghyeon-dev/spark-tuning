# 벤치마크 계획서: Spark Job 설정 최적화

## 목적
설정값들에 대해 체계적인 벤치마크를 수행하여, 근거 있는 최적 설정값을 도출한다.

---

## 1. 테스트 환경

### 1.1 대상 테이블 (TABLE_A)
- 컬럼 수: 약 19개 (string, double, integer, array, timestamp_ntz 등)
- **파티션 (3개)**: `day(ts)`, `column_a`, `column_b` → shuffle 발생 원인
- **Write Ordering (3개)**: `column_c`, `column_d`, `column_e` ASC NULLS FIRST
- ⚠️ 파티션/write ordering은 변동 가능 (하루치 데이터 적재 후 최종 결정 예정)

### 1.2 테스트 데이터 (10분 주기 배치)

| 항목 | 값 |
|------|----|
| Avro 파일 수 | 5,355개 (파일당 평균 ~1.58MB) |
| Avro 총 크기 | ~8.08GB |
| Parquet 총 크기 (Iceberg 적재 후) | ~6.54GB (압축률 ~82.9%) |
| 총 Row 수 | 628,699건 |
| 추정 읽기 파티션 수 | ~63개 (`8,080MB / 128MB`) |

### 1.3 고정 옵션
- driver-cores=1, driver-memory=2g, executor-cores=4, executor-memory=8g
- shuffle.partitions, parallelismFirst: 별도 설정하지 않음 (Spark 기본값)

### 1.4 사전 조건
- 벤치마크 전용 K8S namespace 사용
- 동일한 K8S 노드 풀에서 실행
- MinIO 서버 부하가 낮은 시간대에 수행

---

## 2. 벤치마크 결과

### 2.1 num-executors ✅

| 테스트 ID | num-executors | 총 코어 수 | 소요시간 | 결과 |
|-----------|---------------|-----------|---------|------|
| NE-01 | 16 | 64 | 55초 | 코어 부족 |
| **NE-02** | **24** | **96** | **44초** | **✅ 최적** |
| NE-03 | 32 | 128 | 51초 | 오버헤드 발생 |
| NE-04 | 60 | 240 | 51초 | 과다 할당 |

**결론**: `PARALLELISM_FACTOR=1.5`(24개)가 최적. 산정식: `ceil(총 크기 / 128MB × 1.5 / executor-cores)`

### 2.2 Shuffle 발생 확인 ✅

| Stage | 역할 | Input | Shuffle Write | Shuffle Read | Output |
|-------|------|-------|---------------|--------------|--------|
| 0 | collect (메타데이터) | 580.6KiB | 163.8KiB | - | - |
| 1 | collect (skipped) | - | - | - | - |
| 2 | collect (메타데이터) | - | - | 163.9KiB | - |
| 3 | 파일 목록 조회 (5355 paths) | - | - | - | - |
| 4 | append (avro 읽기) | 7.9GiB | - | - | - |
| 5 | append (shuffle 준비) | 7.9GiB | **9.2GiB** | - | - |
| 6 | append (skipped) | - | - | - | - |
| 7 | append (Iceberg 쓰기) | - | - | **9.2GiB** | 6.5GiB |

Stage 5→7에서 Iceberg `write.distribution-mode=range`에 의한 9.2GiB shuffle 발생.

### 2.3 shuffle.partitions ✅

고정: num-executors=24

**결론**: 기본값(200) 최적. AQE 자동 조정에 맡김.

### 2.4 parallelismFirst ✅

고정: num-executors=24

| 테스트 ID | 값 | 소요시간 | 결과 |
|-----------|-----|---------|------|
| PF-01 | `true` (기본값) | **44초** | **✅ 최적** |
| PF-02 | `false` (공식 권장) | 53~57초 | +23% 느림 |

**결론**: `true`(기본값) 유지. I/O 중심 워크로드에서 병렬성 우선이 유리. 소파일은 컴팩션으로 해결.

---

## 3. 최종 결론

- **num-executors**: 24개 최적. 리소스 60% 절감, 성능 13% 향상
- **shuffle.partitions**: 기본값(200) 최적. AQE 자동 조정에 맡김
- **parallelismFirst**: 기본값(`true`) 최적. 소파일은 컴팩션(시간당/일당)으로 해결
- **성능 설정 2개 옵션 모두 별도 설정 불필요 (Spark 기본값이 최적)**

### 향후 재검증

파티션/write ordering 최종 확정 후 shuffle 패턴이 달라지므로 num-executors·shuffle.partitions 재검증 필요.

---

## 4. 미검증 항목 (P3): Executor 리소스

| 테스트 ID | cores | memory | executors | 총 코어 | 총 메모리 |
|-----------|-------|--------|-----------|---------|-----------|
| EX-01 | 2 | 4g | 8 | 16 | 32g |
| EX-02 | 4 | 8g | 4 | 16 | 32g |
| EX-03 | 4 | 16g | 4 | 16 | 64g |
| EX-04 | 5 | 10g | 3 | 15 | 30g |
| EX-05 | 8 | 16g | 2 | 16 | 32g |
