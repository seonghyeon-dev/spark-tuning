# Compaction executor 자원 할당 설계

| 항목 | 내용 |
|------|------|
| 대상 | hourly Compaction DAG |
| 목적 | 데이터 증가·시간대별 편차에 맞춰 executor 수를 자동 조절 |
| 전제 | 튜닝 결과 확정 (`tuning/compaction-tuning-guide.md`) |
| 결론 | **Spark Dynamic Allocation + `executorAllocationRatio` 채택.** 7회 실측 검증 완료 (섹션 5) |

---

## 1. 배경 및 판단 기준

hourly Compaction은 매시 `:45`에 시작해 정각까지 종료해야 한다. 시작 분 M은 `M ≤ 60 − duration − 여유`로 정해지며 현재 `60 − 12 − 3 = 45`다 (`reprocessing-dag-design.md` §6.2).

정적 executor 수에서는 데이터가 늘면 duration이 비례해 늘고 이 제약이 깨진다. 그때마다 사람이 대수를 고쳐야 한다.

**판단 기준** (순서대로 적용)

| 순위 | 기준 | 조건 |
|------|------|------|
| 1 | 실행 창 | DAG 전체(테이블 4개 순차)가 12분 이내 |
| 2 | disk spill | 0 유지 |
| 3 | dcu | 낮을수록 좋음 |
| 4 | 구현·운영 비용 | 신규 의존성과 실패 모드의 수 |

---

## 2. 후보

| 안 | 방식 | 조절 주체 |
|----|------|----------|
| A | 정적 유지 (`num-executors` 고정) | 없음 |
| **B** | **Dynamic Allocation + ratio** | **Spark이 실행 중 자동** |
| C | 사전 산정 (Airflow가 데이터 양 조회) | Airflow가 시작 전 |

---

## 3. A안: 정적 유지 — 한계

고정 core에서는 duration이 데이터에 비례한다.

| 데이터 (테이블당) | DAG 전체 | 실행 창 |
|------------------|---------|--------|
| 42.3GB (현재 최대) | 6.8분 | 통과 |
| 60GB | 9.6분 | 통과 |
| **74.7GB** | **12.0분** | **초과** |

여유는 1.77배지만, 시간대별 편차에 대응하지 못하고 증가 시 수동 변경이 필요하다.

---

## 4. B안: Dynamic Allocation + executorAllocationRatio (채택)

### 4.1 Kubernetes 전제조건

K8S에는 external shuffle service가 없다. Spark 3.0부터 `shuffleTracking.enabled` 기본값이 **true**이므로 별도 설정 없이 동작한다.

| 설정 | 기본값 |
|------|--------|
| `spark.dynamicAllocation.enabled` | false |
| `spark.dynamicAllocation.minExecutors` | 0 |
| `spark.dynamicAllocation.maxExecutors` | 무한대 |
| `spark.dynamicAllocation.initialExecutors` | `minExecutors` 값 |
| `spark.dynamicAllocation.executorIdleTimeout` | 60초 |
| `spark.dynamicAllocation.schedulerBacklogTimeout` | 1초 |
| `spark.dynamicAllocation.executorAllocationRatio` | 1.0 |
| `spark.dynamicAllocation.shuffleTracking.enabled` | **true** (3.0.0부터) |
| `spark.dynamicAllocation.shuffleTracking.timeout` | 무한대 |

### 4.2 반납은 일어나지 않는다 (실측 확인)

공식 문서가 직접 설명한다.

> *"under most circumstances, this condition is **mutually exclusive** with the request condition, in that **an executor should not be idle if there are still pending tasks** to be scheduled."*

**밀린 일감이 있으면 노는 executor가 없으므로 제거 조건이 성립하지 않는다.** Compaction은 일감이 725~1,450개 계속 밀려 있어 반납이 발생할 상황 자체가 없다.

**실측**: `minExecutors=4`로 낮춰 실행해도 12대에서 내려가지 않았다.

> `shuffleTracking.timeout` 무한대는 "영원히 반납 안 함"이 아니라 **shuffle 데이터가 GC로 정리되면 그때 반납 가능**하다는 뜻이다. 다만 그 전에 job이 끝난다.

### 4.3 확보는 동작한다 (실측 확인)

데이터가 늘면 일감이 늘고 DA가 executor를 추가한다.

```
schedulerBacklogTimeout 1초 후 요청 시작
이후 1초마다 요청 수가 배로 증가 (1, 2, 4, 8, ...)  → 12대 추가 요청까지 4초
pod 생성/등록 5~15초
합계 10~20초
```

**실측**: 2시간 범위(82GB)로 실행 시 12대 → **24대**로 증가한다.

### 4.4 `executorAllocationRatio`로 요청량을 조절한다

**그냥 켜면 안 되는 이유** — DA는 기본적으로 밀린 일감을 전부 동시에 처리할 만큼 요청한다.

```
82GB → 순간 일감 약 738개 → 738 ÷ 4(executor당 slot) = 185대 요청
```

`executorAllocationRatio`가 이 값을 비율로 줄인다. 공식 문서에 명시된 용도다.

> *"with small tasks this setting can waste a lot of resources... This setting allows to **set a ratio that will be used to reduce the number of executors** w.r.t. full parallelism."*

**계산**

```
desired = (순간 일감 ÷ executor당 slot) × ratio
        = (데이터GB × 9 ÷ 4) × ratio
        = 데이터GB × 2.25 × ratio
```

`순간 일감 ≈ 데이터GB × 9`는 실측 역산값이다 (전체 일감의 약 절반이 동시에 큐에 존재).

**목표 대수**(`데이터GB × 0.32`, `compaction-tuning-guide.md` §4.4)와 같게 놓으면:

```
ratio = 0.32 ÷ 2.25 = 0.142  →  실측 검증값 0.13
```

### 4.5 ratio는 테이블 크기와 무관하다

위 식의 양변에 `데이터GB`가 들어가 소거된다. **비율이므로 데이터가 커지면 대수도 비례해 커진다.**

| 데이터 | 순간 일감 | 최대 병렬 | × 0.13 | 실측 |
|--------|----------|----------|--------|------|
| 39GB | 약 351개 | 88대 | 11.4대 | **12대** |
| 82GB | 약 738개 | 185대 | 24.0대 | **24대** |

데이터 2.1배에 대수도 2배다.

**단 ratio는 파일 크기에 의존한다.** 일감 수가 파일 크기로 정해지기 때문이다 (섹션 9).

| 파일 개수 (총 37GB) | 파일 크기 | 읽기 일감 | 대수 |
|-------------------|----------|----------|------|
| 703개 (현재) | 53.9MB | 352 | 12.6 |
| 28개 (1/25) | 1,353MB | 308 | 11.2 |
| 17,575개 (25배) | 2.2MB | 550 | 19.1 |

파일이 커지는 방향은 영향이 작다(현재 2개를 묶어 108MB로 처리 중이라 이미 목표 128MB에 근접). **작아지는 방향은 영향이 크다** — 파일 여는 비용 하한 4MB가 지배해 일감이 급증한다.

### 4.6 `maxExecutors`는 예약이 아니라 천장이다

**실측으로 확인했다.**

| 회차 | 데이터 | max 설정 | 실제 사용 |
|------|-------|---------|----------|
| 1시간 | 39GB | 24 | **12대** |
| 2시간 | 82GB | 24 | 24대 |
| 2시간 | 82GB | **36** | **24대** |

max를 36으로 올려도 24대에서 멈췄다. **실제 사용량은 ratio가 정하고 max는 상한일 뿐이므로, 넉넉히 두어도 자원을 미리 점유하지 않는다.**

> K8S에 자리가 없어 pod이 안 뜨면 DA는 **있는 대수로 계속 진행**한다. 정적 설정보다 degradation이 완만하다.

### 4.7 설정

```
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.executorAllocationRatio=0.13        ← 전 테이블 공통
spark.dynamicAllocation.initialExecutors=<테이블별 평소 대수>  ← 기존 com_num_executor
spark.dynamicAllocation.minExecutors=<initialExecutors와 동일>
spark.dynamicAllocation.maxExecutors=<K8S 여유 범위>
```

**`initialExecutors`를 반드시 지정한다.** 기본값이 `minExecutors`(0)이므로, 생략하면 0대에서 시작해 warm-up에만 20~40초를 쓴다.

---

## 5. 실측 결과

측정 대상: 2026-08-12 ~ 08-13. `output`이 실제 데이터 크기다.

| 회차 | 설정 | 데이터 | 대수 | 초/GB | dcu/GB | idle | memory | spill |
|------|------|--------|------|-------|--------|------|--------|-------|
| 정적 12 (기준) | — | 37.30GB | 12 | 2.413 | 0.00219 | 16.7% | 90.3% | 0 |
| 19~20 | ratio 0.066 | 40.02GB | 12 | 2.549 | 0.00240 | 24.6% | 84.6% | 0 |
| 21~22 | ratio 0.066 | 39.28GB | 12 | 2.444 | 0.00221 | 18.0% | 94.1% | 0 |
| 22~23 | ratio 0.13 | 39.79GB | 12 | 2.413 | 0.00221 | 16.4% | 92.5% | 0 |
| 23~00 | ratio 0.13 | 38.45GB | 12 | 2.653 | 0.00249 | 21.9% | 91.1% | 0 |
| 00~01 | ratio 0.13 | 39.38GB | 12 | 2.438 | 0.00226 | 17.8% | 87.6% | 0 |
| **01~03** | ratio 0.13, max 24 | **82.13GB** | **24** | 1.388 | 0.00238 | 24.4% | 91.6% | 0 |
| **03~05** | ratio 0.13, **max 36** | **82.51GB** | **24** | 1.309 | 0.00225 | 19.3% | 97.6% | 0 |

**판정**

| 항목 | 결과 |
|------|------|
| 1시간 데이터에서 정적 12와 동등한가 | ✅ 초/GB +3.7%, dcu/GB +5.9% (노이즈 기준선 15% 이내) |
| 데이터가 늘면 대수가 늘어나는가 | ✅ 39GB 12대 → 82GB 24대 |
| max가 아니라 ratio가 대수를 정하는가 | ✅ max 36에서도 24대 |
| spill 0을 유지하는가 | ✅ 7회 전부 0 |

**ratio 0.066 → 0.13 교정 근거**: 0.066에서 driver 로그의 `desired total`이 5~6으로 찍혔다. 역산하면 순간 일감 303~364개이며, 이는 **전체 일감(725개)이 아니라 그 순간 큐에 있는 수**다. 처음 계산에서 전체 일감을 썼던 것이 원인이며, 0.13으로 교정 후 요청 로그가 사라졌다(= 12대로 충분하다고 계산).

**2시간 실행의 한계**: 입력이 이미 compaction된 데이터(505MB 파일)라 실제 데이터 증가 상황과 파일 구성이 다르다. 실제 증가 시에도 desired는 25대로 계산되어 **대수 결정 동작은 동일**하나, 소요시간은 더 걸린다.

---

## 6. C안: 사전 산정 (보류)

Airflow가 Trino로 `.partitions`를 조회해 데이터 양을 파악하고 executor 수를 결정하는 방식이다. 구현 스켈레톤은 `pipeline/examples/compaction_executor_sizing_example.py`에 있다.

**B안이 같은 목적을 달성하므로 보류한다.**

| | B안 | C안 |
|---|-----|-----|
| 구현 | Spark 설정 4줄 | Trino 연결 + fallback + 검증 |
| 외부 의존성 | 없음 | Trino |
| 신규 실패 모드 | 없음 | 5종 |
| 롤백 | 설정 1줄 | 코드 되돌리기 |
| 대수 결정 시점 | 실행 중 (10~20초 지연) | 시작 전 (지연 없음) |

**C안이 필요해지는 경우**: B안의 확보 지연 10~20초가 실행 창을 압박할 때. 현재 DAG 전체가 12분 창에 6~7분이므로 해당하지 않는다.

C안 상세(산정 위치 대안 비교, 조회 경로 대안 비교, 실패 모드)는 이 문서의 이전 개정판과 예시 파일 주석에 남아 있다.

---

## 7. 권고

| 항목 | 조치 |
|------|------|
| **hourly Compaction** | **B안 적용** (섹션 4.7 설정) |
| `initialExecutors` / `minExecutors` | 테이블별 기존 `com_num_executor` 값 사용 |
| `executorAllocationRatio` | 전 테이블 **0.13** |
| `maxExecutors` | K8S 여유 범위. 넉넉히 두어도 무해 |
| C안 (사전 산정) | 보류. 예시 파일은 유지 |

---

## 8. 테이블별 적용

**설정을 두 종류로 나눠 본다.**

| 설정 | 성격 | 테이블별 |
|------|------|---------|
| `executorAllocationRatio` | **비율(%)** | ❌ 공통 |
| `initialExecutors` / `minExecutors` | **개수(대)** | ✅ 테이블별 |
| `maxExecutors` | 개수(대) | 공통 가능 |

**ratio가 공통인 이유**: 데이터가 작으면 Spark이 요청하려는 대수도 작아지므로, 같은 비율을 곱해도 알아서 작은 값이 나온다.

**`initialExecutors`가 테이블별인 이유**: 절대 개수이므로 10GB 테이블에 12를 넣으면 4배 과다이고, 반납이 안 되므로 그대로 유지된다. **기존 `com_num_executor` 상수를 그대로 쓰면 되며 새로 튜닝할 필요가 없다.**

**역할 분담**

| 설정 | 하는 일 |
|------|--------|
| `initialExecutors` / `minExecutors` | 평소 데이터량에서 쓸 대수를 바닥으로 깐다 |
| `ratio` | 평소보다 많은 시간대에 얼마나 더 부를지 정한다 |

**다른 테이블 확인 항목** (테이블당 1회 실행)

| 확인 | 기준 |
|------|------|
| 수렴 대수 | `데이터GB × 0.32`와 유사한가 |
| `dcu/GB` | 0.0022 근처인가 |
| `spill` | 0인가 |

어긋난다면 원인은 대개 **파일 크기 차이**다 (섹션 4.5).

---

## 9. 재검증 조건

```
1. append Job의 shuffle 설정 변경 또는 입력 파일 크기 변화
   → 일감 수가 달라져 ratio 0.13이 조용히 어긋난다.
     에러가 나지 않고 대수만 틀어지므로 알아채기 어렵다.
     이것이 정적 설정 대비 이 방식의 유일한 실질적 단점이다.

2. executor cores 변경 (현재 4)
   → desired 계산의 분모가 바뀐다.

3. target-file-size-bytes 변경 (현재 512MB)
   → 쓰기 일감 수가 바뀐다.

4. Iceberg 또는 Spark 버전 업그레이드
   → shuffleTracking 기본값과 DA 계산식 재확인.

5. hourly duration이 15분 초과 (DAG 전체)
   → reprocessing-dag-design.md §6.2의 M 재계산.
```

---

## 10. 미확인 항목

| 항목 | 내용 |
|------|------|
| `maxExecutors` 확정값 | K8S namespace quota 확인. **실제 사용량은 ratio가 통제하므로 긴급하지 않다** |
| ratio 0.066에서의 요청 로그 | 12대가 이미 떠 있는데 `desired total 5~6`을 "새로 요청"한 이유가 로그만으로는 설명되지 않는다. 현재 결론에는 영향 없음 |
| 메모리 97.62% | 7회 중 최고값이며 DataFlint가 `executor.memory` 19.2g를 권고한다. **spill이 0인 동안은 조치하지 않는다** — Spark의 정렬은 가용 메모리를 최대한 쓰다가 부족하면 디스크로 넘기므로, 90%대는 한계 임박이 아니라 정상 동작이다. 감시 기준은 `spill ≠ 0` |
| 다른 hourly 테이블 3개 | 섹션 8의 확인 항목 |

---

## 11. daily와의 분리

daily Compaction에 이 설계를 그대로 적용할 수 없다.

- ratio 0.13은 hourly의 파일 구성·계수(C=0.32) 기준이다
- daily는 30~60분 job이라 `executorIdleTimeout` 60초가 전체의 2~3%에 불과해 **반납이 실제로 일어날 수 있다.** 반납이 되면 판단 근거가 달라진다
- daily는 `rewrite-all` 낭비 의심이 남아 있다 (`compaction-tuning-guide.md` §8.1)

daily 튜닝 후 별도로 판단한다.
