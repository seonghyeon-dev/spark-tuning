# Compaction executor 자원 할당 설계

| 항목 | 내용 |
|------|------|
| 대상 | hourly Compaction DAG |
| 목적 | 데이터 증가 시 실행 창(12분) 제약을 넘지 않도록 executor 자원을 조절 |
| 전제 | 튜닝 결과 확정 (`tuning/compaction-tuning-guide.md`) |
| 결론 | **현재 정적 12개 유지.** 증가 시 Dynamic Allocation 우선 시도, 부족하면 사전 산정 (섹션 6) |

---

## 1. 배경 및 판단 기준

hourly Compaction은 매시 `:45`에 시작해 정각까지 종료해야 한다. 시작 분 M은 `M ≤ 60 − duration − 여유`로 정해지며 현재 `60 − 12 − 3 = 45`다 (`reprocessing-dag-design.md` §6.2).

정적 executor 수에서는 데이터가 늘면 duration이 비례해 늘고 이 제약이 조용히 깨진다.

**판단 기준** (순서대로 적용)

| 순위 | 기준 | 조건 |
|------|------|------|
| 1 | 실행 창 | DAG 전체(테이블 4개 순차)가 12분 이내 |
| 2 | disk spill | 0 유지 |
| 3 | dcu | 낮을수록 좋음 |
| 4 | append 경합 | K8S 점유가 예측 가능해야 함 (append 5분 주기, batch당 약 10 executor) |
| 5 | 구현·운영 비용 | 신규 의존성과 실패 모드의 수 |

1·2는 필수 조건이고 3~5는 그 안에서 고르는 기준이다.

---

## 2. 후보

| 안 | 방식 | 조절 시점 |
|----|------|----------|
| **A** | 정적 유지 (`num-executors` 12 고정) | 없음 |
| **B** | Spark Dynamic Allocation | Job 실행 중 Spark이 자동 |
| **C** | 사전 산정 (Airflow가 데이터 양 조회 후 결정) | Job 시작 전 1회 |

A와 B, A와 C는 배타적이지 않다. A로 운영하다 한계에 도달하면 B 또는 C로 전환한다.

---

## 3. A안: 정적 유지 — 어디까지 버티는가

고정 core에서는 duration이 데이터에 비례하고 초/GB가 일정하다.

| 데이터 (테이블당) | DAG 전체 | 실행 창 |
|------------------|---------|--------|
| 42.3GB (현재 최대) | 6.8분 | 통과 |
| 60GB | 9.6분 | 통과 |
| **74.7GB** | **12.0분** | **초과** |
| 84GB | 13.5분 | 초과 |

- 창 초과 지점: 테이블당 **74.7GB**
- 현재 최대 42.3GB → **여유 1.77배**

**데이터가 1.8배가 되기 전까지는 B·C 모두 불필요하다.** 도입을 검토할 시점은 55~60GB(창 여유가 2~3분으로 줄어드는 구간)다.

---

## 4. B안: Spark Dynamic Allocation

### 4.1 Kubernetes 전제조건

K8S에는 external shuffle service가 없으므로 `shuffleTracking.enabled=true`가 필수다.

**확인한 기본값**

| 설정 | 기본값 |
|------|--------|
| `spark.dynamicAllocation.enabled` | false |
| `spark.dynamicAllocation.executorIdleTimeout` | **60초** |
| `spark.dynamicAllocation.shuffleTracking.enabled` | false (K8S에서는 true 필요) |
| `spark.dynamicAllocation.shuffleTracking.timeout` | **무한대** (Int 최대값 ms) |
| `spark.dynamicAllocation.initialExecutors` | `minExecutors` 값 |
| `spark.dynamicAllocation.minExecutors` | 0 |
| `spark.dynamicAllocation.maxExecutors` | 무한대 |
| `spark.dynamicAllocation.schedulerBacklogTimeout` | 1초 |
| `spark.dynamicAllocation.sustainedSchedulerBacklogTimeout` | `schedulerBacklogTimeout` 값 |

> 위 값은 서드파티 레퍼런스에서 확인한 것이다. `spark.apache.org` 접근이 막혀 공식 문서로 대조하지 못했으므로 Spark 4.1.1 실제 기본값은 배포 전 재확인이 필요하다 (섹션 8).

### 4.2 반납(scale-down)은 일어나지 않는다

세 가지 이유가 겹친다.

**(1) shuffle 데이터를 가진 executor는 반납되지 않는다**

`shuffleTracking.timeout`이 무한대이므로 shuffle 파일을 보유한 executor는 회수 대상에서 제외된다. Compaction은 `sort` 전략이라 shuffle read/write가 각 52.25GiB이고, 사실상 모든 executor가 shuffle 데이터를 들고 있다.

`shuffleTracking.timeout`을 유한값으로 낮추면 강제 회수는 가능하나 shuffle 재계산이 발생해 duration이 늘어난다.

**(2) idle timeout 60초 > job 전체 90초**

executor가 60초 연속으로 완전히 놀아야 반납된다. Job 전체가 90초이므로 **job의 67% 동안 노는 executor**가 있어야 한다는 뜻이다.

**(3) 유휴가 executor 단위가 아니라 부분 유휴다**

현재 설정(12 executor = 48 core, file group 4개)의 task 구성:

| group | 크기 | 읽기 task | 쓰기 task |
|-------|------|----------|----------|
| C | 21.4GB | 201 | 43 |
| B | 12.0GB | 110 | 24 |
| A | 2.8GB | 26 | 6 |
| D | 0.9GB | 13 | 2 |
| 합계 | 37GB | 350 | **75** (실측 `file_count` 75와 일치) |

종료 직전 구간에는 가장 큰 C의 쓰기 task만 남는다.

```
남은 task 43개 / core 48개
executor 12개에 분산 → executor당 3.6 task (slot 4개 중)
→ 완전히 노는 executor가 없다. 전부 부분 유휴다.
```

DA는 **executor 단위로만** 조절하므로 이 형태의 유휴(`idle cores 17%`)는 회수할 수 없다.

> (3)은 추론이다. Spark UI Executors 탭에서 executor별 active task 수 분포를 보면 확인된다 (섹션 7).

### 4.3 확보(scale-up)는 유효하다

반납과 달리 **executor를 늘리는 쪽은 동작한다.** 이것이 DA를 후보에서 배제하지 않는 이유다.

데이터가 늘면 task backlog가 쌓이고 DA가 executor를 추가한다.

```
schedulerBacklogTimeout 1초 후 요청 시작
이후 1초마다 요청 수가 배로 증가 (1, 2, 4, 8, ...)
→ 12개 추가 요청 완료까지 4라운드 = 4초
→ pod 생성/등록 5~15초
→ 총 10~20초
```

90초 job에서 10~20초는 11~22%의 지연이지만, 이후 70초 이상을 늘어난 executor로 처리한다. **데이터 증가 대응이라는 목적은 달성된다.**

### 4.4 `initialExecutors`를 반드시 지정해야 한다

기본값은 `minExecutors`이고 그 기본값은 0이다. 지정하지 않으면 **0에서 시작해 backlog를 보고 올라간다.** 90초 job에서 warm-up에만 20~40초를 쓰게 된다.

```
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.shuffleTracking.enabled=true
spark.dynamicAllocation.initialExecutors=12    ← 현재 정적값
spark.dynamicAllocation.minExecutors=12        ← 반납이 안 되므로 initial과 같게 두는 것이 명확하다
spark.dynamicAllocation.maxExecutors=?         ← K8S quota 기준 (섹션 8)
```

**여기서 `initialExecutors` 값을 정하는 문제가 곧 C안의 산정 문제다.** 다만 정적값 12를 그대로 쓰면 되고, 데이터 증가분만 DA가 흡수하므로 조회는 불필요하다.

---

## 5. C안: 사전 산정

### 5.1 산정식

```python
num_executors = clamp(ceil(total_size_gb * 0.32), MIN_EXECUTORS, MAX_EXECUTORS)
```

계수 C=0.32은 9회 측정으로 확정한 값이다 (`compaction-tuning-guide.md` §4.4).

| executor | 총 크기 | C | dcu/GB | idle | 판정 |
|---|---|---|---|---|---|
| 16 | 38.3~38.5GB | 0.42 | 0.00251 | 24.9~28.6% | 리소스 과다 |
| **12** | 37.3GB | **0.32** | **0.00219** | 16.73% | 채택 |
| 8 | 36.6GB | 0.22 | 0.00247 | 15.35% | dcu 반등, 기각 |

### 5.2 산정 위치

`compaction_dag_example.py`가 만드는 `compaction_specs` task 내부에서 테이블별로 산정한다.

| 안 | 방식 | 판정 |
|----|------|------|
| 가 | `compaction_specs` 내부 | **채택** |
| 나 | 조회 전용 task 분리 → XCom 병합 | 미채택 |
| 다 | mapped task 실행 직전 조회 | 미채택 |

- **가 채택** — 이미 params를 읽고 테이블을 loop한다. 테이블별 `try/except`로 감싸면 한 테이블의 조회 실패가 다른 테이블에 전파되지 않아 나의 실패 격리 이득이 성립한다. 산정값은 XCom을 거치므로 원시 타입이어야 하는데 기존 구조가 이미 `"instances": str(...)`이라 변경이 한 줄이다
- **나 미채택** — `expand_kwargs`에 넘길 list[dict]를 한 곳에서 만드는 것이 단순하다. 재시도 단위 분리 이득은 fallback이 있어 실질적이지 않다
- **다 미채택** — operator 인자는 `expand_kwargs` 시점에 정해져야 하므로 task가 추가로 필요하다. 대상이 이미 닫힌 과거 1시간치라 조회를 늦춰 얻는 정확도 이득이 작다

### 5.3 입력 조회

Trino JDBC로 Iceberg `.partitions`를 조회한다.

| 경로 | 판정 | 이유 |
|------|------|------|
| **Trino JDBC** | 채택 | Airflow provider 존재. pod 기동 없음 |
| Spark job | 미채택 | pod 기동 20~30초. 산정 목적에 과함 |
| HMS 직접 조회 | 미채택 | manifest 직접 파싱. 구현 비용 큼 |
| append DAG이 크기 기록 | 미채택 | DAG 간 결합 증가. avro → Parquet 변환 계수 필요 |
| 직전 회차 값 캐싱 | 미채택 | 실측 크기를 Spark pod에서 되돌리는 배관이 조회보다 복잡 |

**`.files`가 아니라 `.partitions`를 쓴다.** `.files`는 데이터 파일 1개당 1행이고 그 행에 컬럼 19개 전부의 통계가 들어간다. `.partitions`는 파티션당 1행으로 집계되어 있다 (보관 30일 가정 시 54,000행 대 4행).

**범위 조회여야 한다.** 재처리 DAG이 trigger할 때 `start_time`/`end_time`이 여러 시간에 걸친다 (`reprocessing-dag-design.md` §6.3).

**파티션 값 변환은 naive datetime으로 한다.** `ts`가 `timestamp_ntz`이므로 timezone을 붙이면 Iceberg 저장값과 어긋난다.

```
hour = int((dt − 1970-01-01).total_seconds() // 3600)
검증: 2026-08-11 13:00 → 496237 (Spark UI 출력값과 일치)
```

### 5.4 실패 모드

| 실패 모드 | 증상 | 대응 |
|----------|------|------|
| Trino 장애 | 예외 | 정적값 fallback + warning |
| 파티션 조건 불일치 | 크기 0 | 정상 범위 검사 후 fallback |
| 조회 결과 이상값 | 비정상 크기 | 정상 범위(0.1~500GB) 검사 후 fallback |
| 산정값 상한 초과 | clamp | 상한 적용 + warning (섹션 8) |
| Trino 응답 지연 | DAG 시작 지연 | timeout 설정 필요 |

**기존 `com_num_executor` 상수를 fallback으로 유지한다.** 지우면 Trino 장애가 곧 Compaction 실패가 된다.

**크기 0 검사가 별도로 필요하다.** 조회는 성공했는데 파티션 조건이 틀려 0이 오면 예외가 나지 않아 fallback 경로를 타지 않고, executor가 `MIN_EXECUTORS`로 떨어져 Job이 한없이 느려진다.

---

## 6. 비교 및 권고

| 기준 | A: 정적 | B: Dynamic Allocation | C: 사전 산정 |
|------|--------|---------------------|-------------|
| 데이터 증가 대응 | 74.7GB까지 | max까지 (10~20초 지연 후) | max까지 (지연 없음) |
| 시간대별 편차 대응 | 없음 | 없음 (반납 불가) | 가능 |
| 유휴 자원 회수 | 없음 | **불가** (섹션 4.2) | 없음 |
| 외부 의존성 | 없음 | 없음 | **Trino** |
| 신규 실패 모드 | 없음 | 없음 | 5종 (섹션 5.4) |
| K8S 점유 예측 | 명확 | 불확실 (변동) | 명확 |
| 구현 비용 | 0 | **설정 4줄** | Trino 배관 + fallback + 검증 |
| 롤백 비용 | — | 설정 1줄 | 코드 되돌리기 |
| 상한 설정 필요 | 불필요 | `maxExecutors` | `MAX_EXECUTORS` (동일) |

**권고**

| 시점 | 조치 |
|------|------|
| 현재 | **A안.** `com_num_executor`를 12로 고정 |
| 테이블당 55~60GB 도달 | **B안 먼저 시도.** 설정 4줄이고 롤백이 1줄이다 |
| B안이 창 제약을 못 지키면 | **C안.** ramp-up 10~20초가 문제가 될 때 |

**B를 C보다 먼저 두는 이유** — 구현 비용 차이가 압도적이다. B는 설정 4줄이고 실패해도 롤백이 한 줄이다. C는 Trino 연결, fallback, 정상 범위 검증, 5종 실패 모드 대응이 필요하고 외부 의존성이 하나 늘어난다. 두 방식의 목적(데이터 증가 흡수)이 같으므로 싼 것부터 시도한다.

**C가 여전히 필요할 수 있는 경우** — B의 ramp-up 10~20초가 90초 job에서 11~22%다. 창 여유가 얼마 없는 상황에서는 이 지연이 문제가 될 수 있고, 그때는 처음부터 맞는 수로 시작하는 C가 유리하다.

**시간대별 편차는 어느 쪽도 해결하지 못한다.** B는 반납이 안 되고, C는 조회로 대응 가능하나 그 이득이 측정되지 않았다. 새벽 시간대 데이터 양 측정이 선행되어야 한다 (섹션 8).

---

## 7. 검증 계획

B안을 도입하기 전에 **1시간치로 실제 동작을 확인한다.** 섹션 4의 분석은 문서 기본값과 task 구성 추론에 기반하므로 실측으로 확정해야 한다.

**설정**

```
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.shuffleTracking.enabled=true
spark.dynamicAllocation.initialExecutors=12
spark.dynamicAllocation.minExecutors=4          ← 반납이 되는지 보기 위해 낮게
spark.dynamicAllocation.maxExecutors=24
```

**확인 항목**

| 항목 | 확인 위치 | 판정 |
|------|----------|------|
| executor 수 증감 | driver 로그 `ExecutorAllocationManager` | 12에서 안 움직이면 섹션 4.2 확정 |
| 최소/최대 executor 수 | Spark UI Executors 탭 | 4까지 내려가면 반납이 되는 것 |
| executor별 active task 분포 | Spark UI Executors 탭 | 완전히 노는 executor가 있는지 (섹션 4.2 (3) 검증) |
| duration, dcu | DataFlint | 정적 12(2.41 초/GB, dcu/GB 0.00219) 대비 |
| spill | DataFlint | 0 유지 |

**판정 기준**

- executor 수가 12에서 움직이지 않으면 → 반납 불가 확정. 데이터 증가 시에만 의미가 있으므로 그때까지 A안 유지
- 4까지 내려가고 dcu가 줄면 → 섹션 4.2가 틀린 것. B안을 즉시 채택
- duration이 정적 대비 15%(노이즈 기준선) 이상 늘면 → shuffle 재계산이나 warm-up 비용. 설정 재검토

---

## 8. 미확인 항목

| 항목 | 내용 | 필요 시점 |
|------|------|----------|
| Spark 4.1.1 DA 기본값 | `spark.apache.org` 차단으로 서드파티 레퍼런스로 확인. 실제 배포 버전에서 재확인 | B안 도입 전 |
| `maxExecutors` / `MAX_EXECUTORS` | K8S namespace quota. append가 5분 주기로 batch당 약 10 executor를 점유하므로 그만큼 제외 | B·C 공통 |
| Trino `$partitions` 컬럼·타입 | `partition.ts_hour`가 INTEGER인지. Spark SQL은 컬럼명이 `total_data_file_size_in_bytes`로 다름 | C안 도입 전 |
| manifest pruning 동작 | 전체 조회와 파티션 필터 조회의 Physical input 비교 | C안 도입 전 |
| 시간대별 데이터 양 편차 | 새벽 시간대 크기. 편차가 크면 C안의 이득이 커진다 | 도입 시점 판단 |

**상한의 의미** — `MAX_EXECUTORS`는 성능 상한이자 K8S 자원 상한이다. append 벤치마크에서 32개 이상은 오히려 느려진다 (`spark-tuning-guide.md` §2.2.3). **K8S에 여유가 없으면 executor를 늘려도 pod Pending으로 duration이 오히려 늘어난다.** 상한에 걸리는 것은 데이터가 설계 범위를 넘었다는 조치 신호이므로 warning을 남긴다.

---

## 9. daily와의 분리

`C=0.32`과 위 분석을 daily에 그대로 쓸 수 없다.

- 계수는 hourly 측정값이다
- daily는 `rewrite-all` 낭비 의심이 남아 있다 — hourly가 정리한 뒤라 할 일이 거의 없어야 하는데 888GB에 30~60분이 걸리고 소요시간이 데이터 양에 비례한다 (`compaction-tuning-guide.md` §8.1)
- daily는 30~60분 job이므로 **DA의 판단 근거가 완전히 다르다.** idle timeout 60초가 전체의 2~3%에 불과해 반납이 실제로 일어날 수 있다

daily 튜닝 후 별도로 판단한다.

---

## 10. 적용 순서

| 순서 | 항목 | 상태 |
|------|------|------|
| 1 | Compaction DAG의 `tables` params + mapped task 전환 | 재처리 DAG 배포 전 적용 예정 (`reprocessing-dag-design.md` §6.1) |
| 2 | `com_num_executor`를 12로 변경 | 즉시 적용 가능 |
| 3 | K8S quota 확인 → 상한값 확정 | 대기 |
| 4 | 데이터 증가 모니터링 | 55~60GB 도달 감시 |
| 5 | B안 검증 (섹션 7) | 55~60GB 도달 시 |
| 6 | B안이 부족하면 C안 | 조건부 |

2번만으로 튜닝 결과의 리소스 절감(dcu −47%)을 확보한다. 5·6번은 데이터 증가 대응이다.

**1번은 C안의 선행 조건이다.** 산정 코드가 `compaction_specs` 안에 들어가므로 mapped task 전환이 먼저 필요하다. B안은 Spark 설정이므로 1번과 무관하다.

---

## 11. 구현

| 파일 | 내용 |
|------|------|
| `pipeline/examples/compaction_executor_sizing_example.py` | C안 구현 스켈레톤 |
| `pipeline/examples/compaction_dag_example.py` | C안 선행 조건인 mapped task 전환 예시 |
| `tuning/compaction-tuning-guide.md` §4.4, §6 | 계수 근거, 측정값 |

B안은 별도 구현 파일이 없다. 섹션 4.4의 Spark 설정 4줄을 기존 Compaction Job 제출 설정에 추가한다.
