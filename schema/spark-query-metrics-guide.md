# Spark History Server — SQL/DataFrame 탭 쿼리 메트릭 가이드

Iceberg 테이블에 대한 쿼리를 Spark로 실행하면, Spark History Server의 **SQL/DataFrame 탭 → Details for Query**에서 Iceberg 전용 Scan 메트릭을 확인할 수 있다.

이 문서는 각 메트릭의 의미와 쿼리 실행 흐름에서의 역할을 설명한다.

## 메트릭 설명

### 1. skipped data files

테이블의 전체 파일 중에서 **아예 열어보지도 않고 건너뛴 파일 수**이다.

Iceberg는 각 데이터 파일의 메타데이터(파티션 값, 컬럼별 min/max)를 manifest에 기록해 둔다. 쿼리가 들어오면 WHERE 조건과 이 메타데이터를 대조하여, "이 파일에는 내가 찾는 데이터가 절대 없다"고 판단되면 해당 파일을 스킵한다.

**이 값이 클수록 좋다.** 불필요한 I/O를 하지 않았다는 뜻이다.

### 2. result data files

스킵되지 않고 **실제로 읽어야 한다고 판정된 파일 수**이다.

다음 관계가 항상 성립한다:

```
skipped data files + result data files = 총 파일 수
```

**이 값이 작을수록 좋다.** 읽을 파일이 적다는 것은 쿼리가 처리할 작업량이 적다는 뜻이다.

### 3. skip rate (계산값)

전체 파일 대비 스킵된 파일의 비율이다. UI에 직접 표시되지는 않으며, 다음과 같이 계산한다:

```
skip rate = skipped data files / 총 파일 수 × 100
```

skip rate가 이미 높은 구간(99%+)에서는 0.3%p 차이라도 result data files에서는 큰 차이로 나타날 수 있다. 예를 들어 99.5% → 99.8%로 올라가면 읽는 파일 수는 절반 이하로 줄어든다.

### 4. total data file size

result data files(읽기로 판정된 파일들)의 **파일 크기를 전부 합산한 값**이다.

주의할 점: 이 값은 해당 파일들의 **전체 크기**이다. 실제로 Parquet 포맷은 필요한 컬럼만 선택적으로 읽으므로(Column Projection), 스토리지에서 실제로 전송된 바이트 수는 이보다 적을 수 있다. 하지만 **전략 간 상대 비교**에는 유효하다.

### 5. file splits read

Spark가 result data files를 **병렬 처리를 위해 분할한 단위 수**이다.

Spark는 하나의 큰 Parquet 파일을 여러 개의 split으로 나누어 여러 Task가 동시에 처리하게 한다. split 수는 파일 크기와 `spark.sql.files.maxPartitionBytes`(기본 128MB) 설정에 따라 결정된다.

**이 값이 작을수록 전체 작업량이 적다는 뜻이다.** 다만 이 값 자체가 병렬성을 결정하므로, 너무 적으면(예: 1~2개) 클러스터 리소스를 제대로 활용하지 못할 수 있다.

## 쿼리 실행 흐름

```
전체 파일
  ↓ Partition Pruning + min/max 통계 대조
  ↓ N개 스킵 (skipped data files)
  ↓
읽을 파일 (result data files)
  ↓ 파일 크기 합산 = total data file size
  ↓ split 분할
  ↓
M개 Task가 병렬로 읽기 (file splits read)
  ↓
최종 결과 반환
```

## Iceberg Scan 메트릭 전체 목록

위 5개 외에도 Spark History Server에서 확인할 수 있는 Iceberg 전용 메트릭이 있다:

| 메트릭 | 설명 |
|--------|------|
| total planning duration | Iceberg가 manifest를 읽고 어떤 파일을 읽을지 결정하는 데 걸린 시간 |
| total data manifests | 테이블에 존재하는 전체 데이터 manifest 파일 수 |
| scanned data manifests | 실제로 열어서 내용을 확인한 manifest 수 |
| skipped data manifests | manifest 자체의 파티션 범위 정보로 아예 열지도 않고 스킵한 manifest 수 |
| result delete files | 적용해야 할 delete 파일 수 |
| skipped delete files | 스킵된 delete 파일 수 |
| indexed delete files | 인덱싱된 delete 파일 수 |
| equality delete files | equality delete 파일 수 |
| positional delete files | positional delete 파일 수 |

## Trino Web UI와의 차이

| 정보 | Spark History Server | Trino Web UI |
|------|---------------------|-------------|
| skipped data files | O (직접 수치 확인) | X (기본 UI 미노출) |
| manifest 레벨 통계 | O (scanned/skipped manifests) | X |
| total planning duration | O | X (전체 쿼리 시간에 포함) |
| Physical Input vs Input | X | O (압축 효율 파악 가능) |
| Operator별 CPU/Wall Time | X | O |
| Peak Memory | X | O |

**Data Skipping 효과를 정량적으로 비교하려면 Spark History Server가, 실행 엔진 레벨의 리소스 사용 상세를 보려면 Trino Web UI가 적합하다.**
