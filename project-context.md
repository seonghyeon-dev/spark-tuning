# 프로젝트 컨텍스트

## 아키텍처 개요

```
Oracle DB (처리대상 관리)
    │
    ▼
Airflow DAG (Task 2개)
    │
    ├── Task 1: get_jobs
    │   ├── Oracle DB에서 처리 대상 조회
    │   ├── 해당 row를 in_progress 상태로 업데이트
    │   ├── xcom에 데이터 저장 (on_fail/on_success 콜백에서 참조용)
    │   ├── num_executor 산정 (avro 파일 총 크기 / 70MB + 1)
    │   ├── avro 파일 경로 리스트를 텍스트 파일로 생성
    │   └── 해당 텍스트 파일을 S3(MinIO)에 업로드
    │
    └── Task 2: append_data
        ├── SparkKubernetesOperator로 K8S에서 Spark Job 실행
        │   ├── argument: Iceberg 테이블명, S3 텍스트파일 경로
        │   └── Spark Job 내부 동작:
        │       ├── S3에서 텍스트 파일 읽기 (avro 파일 경로 목록)
        │       ├── 해당 경로의 avro 파일들 read
        │       └── Iceberg 테이블에 append
        │
        └── 완료 후 콜백
            ├── on_success: xcom 데이터 참조 → Oracle DB에 완료 처리
            └── on_fail: xcom 데이터 참조 → Oracle DB에 실패 처리
```

## 환경 상세

### 인프라
- **Kubernetes 클러스터**: Spark Job이 Pod으로 실행됨
- **S3 (MinIO)**: avro 원본 파일, 경로 리스트 텍스트 파일 저장소
- **Oracle DB**: 처리 대상 관리 (상태: 대기 → in_progress → 완료/실패)

### 버전
- **Spark**: 4.1.1
- **Iceberg**: 1.10.1
- **Airflow**: 3.1.7
- **Spark Operator**: SparkKubernetesOperator (kubeflow)

## 현재 Spark 설정 상세

### Resource 설정

| 옵션 | 현재 값 | 근거 |
|------|---------|------|
| `driver-cores` | 미확정 (일반적 값 사용) | 명확한 근거 없음 |
| `driver-memory` | 미확정 (일반적 값 사용) | 명확한 근거 없음 |
| `executor-cores` | 미확정 (일반적 값 사용) | 명확한 근거 없음 |
| `executor-memory` | 미확정 (일반적 값 사용) | 명확한 근거 없음 |
| `num-executors` | avro 파일 총 크기 / 70MB + 1 | 테스트 기반. 명확한 근거 없음 |

### SQL 설정

| 옵션 | 현재 값 | 근거 |
|------|---------|------|
| `spark.sql.shuffle.partitions` | 70 | 명확한 근거 없음 |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | true | AQE 관련 설정 |

### num-executors 산정 로직
```
num_executors = (avro 파일들의 총 크기 합산) / 70MB + 1
```
- 70MB 기준은 정확한 이론적 근거가 없음
- 테스트를 통해 적절한 성능이 나와서 결정한 값
- 향후 더 근거 있는 기준을 정하고 설정할 필요 있음

### 개선 필요 사항
1. **num-executors**: 70MB 분할 기준에 대한 이론적/실험적 근거 확보 필요
2. **기타 Spark 옵션**: 모든 설정값에 대해 명확한 근거 기준을 정해서 튜닝 필요
3. **벤치마크**: 다양한 데이터 크기에 대한 성능 프로파일링 필요

## Spark Job 데이터 흐름

```
1. Airflow get_jobs Task
   └── Oracle DB 조회 → avro 경로 리스트 → 텍스트 파일 → S3 업로드

2. Airflow append_data Task (SparkKubernetesOperator)
   └── Spark Driver (K8S Pod)
       ├── S3에서 텍스트 파일 다운로드
       ├── 텍스트 파일에 기재된 avro 경로들 파싱
       └── Spark Executors (K8S Pods)
           ├── 각 avro 파일을 S3에서 read
           └── Iceberg 테이블에 append

3. 콜백 (on_success / on_fail)
   └── xcom 데이터 참조 → Oracle DB 상태 업데이트
```

## SparkKubernetesOperator 사용 패턴

Airflow에서 `SparkKubernetesOperator`를 사용하여 Spark Job을 K8S에 제출한다.
주요 전달 인자:
- **Iceberg 테이블명**: append 대상 테이블
- **S3 텍스트파일 경로**: avro 파일 경로 목록이 담긴 파일의 S3 경로
