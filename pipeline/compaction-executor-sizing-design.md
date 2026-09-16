# Compaction executor 자원 할당 설계

| 항목 | 내용 |
|------|------|
| 대상 | hourly Compaction DAG |
| 목적 | 데이터 증가·시간대별 편차에 맞춰 executor 수를 자동 조절 |
| 전제 | 튜닝 결과 확정 (`tuning/compaction-tuning-guide.md`) |
| 결론 | **Spark Dynamic Allocation + `executorAllocationRatio` 채택.** 1번 테이블 7회(섹션 5.1) + 2번 테이블 9회(섹션 5.2) 실측 검증. **`spark.executor.instances` = `initialExecutors` = `minExecutors` 필수** (섹션 4.8) |
| 진행 | 1번·2번 테이블 설정 확정, 3·4번 대기. **DAG 미반영** — 4개 완료 후 일괄 적용 |

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
spark.dynamicAllocation.maxExecutors=36
spark.executor.instances=<initialExecutors와 동일>                 ← 섹션 4.8. 다르면 이 값이 바닥이 된다
```

**`initialExecutors`를 반드시 지정한다.** 기본값이 `minExecutors`(0)이므로, 생략하면 0대에서 시작해 warm-up에만 20~40초를 쓴다.

---

### 4.8 `spark.executor.instances`는 반드시 `initialExecutors`와 같게 둔다 (2번 테이블 실측)

DA를 켜도 `spark.executor.instances`가 남아 있으면 **그 값이 시작 대수의 바닥이 된다.** Spark은 시작 대수를 아래 셋 중 최댓값으로 잡는다.

```
시작 대수 = max(initialExecutors, minExecutors, spark.executor.instances)
```

반납이 일어나지 않으므로(섹션 4.2) 이 바닥은 끝까지 유지된다. 2번 테이블에서 `initialExecutors`를 6·8로 바꾸고 ratio를 0.08·0.10·0.13으로 바꿔도 **여섯 번 전부 12대**로 돈 원인이 이것이었다. SparkApplication의 `executor.instances`가 기존 `com_num_executor` 12로 남아 있었다.

1번 테이블은 `instances` 12 = `initialExecutors` 12라 우연히 문제가 보이지 않았다. 섹션 10에 미확인으로 남겨 두었던 "ratio 0.066에서 `desired total` 5~6인데 12대가 유지된 이유"도 같은 원인이다 — DA는 5~6대면 충분하다고 계산했지만 `instances` 12가 바닥이었고 내릴 방법이 없었다.

**규칙**: `spark.executor.instances` = `initialExecutors` = `minExecutors`. 세 값을 한 곳에서 관리한다. 현재 operator 구조에서 `instances`를 비울 수 없다면 같은 값을 넣는다.

**확인 방법**: driver 로그 시작부의 `Using initial executors = N, max of spark.dynamicAllocation.initialExecutors, spark.dynamicAllocation.minExecutors and spark.executor.instances` 줄. N이 의도한 `initialExecutors`와 다르면 바닥이 걸린 것이다.

---

## 5. 실측 결과

### 5.1 1번 테이블 (하루 945GB, 시간당 약 39GB)

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

> 1시간 테스트 6회는 전부 `spark.executor.instances` 12가 바닥으로 깔린 상태였다(섹션 4.8). 따라서 이 6회가 보여 준 것은 "12대에서 정적과 동등하다"까지이고, **ratio가 대수를 정한다는 증거는 82GB 2회(12 → 24대)뿐이다.** 결론은 바뀌지 않는다 — 1번 테이블은 데이터 37~40GB에 12대가 적정 대수(`tuning/compaction-tuning-guide.md` §4.4)이므로 `instances` 12 = `initialExecutors` 12로 두면 되고, 늘어나는 쪽은 ratio가 검증됐다.

**ratio 0.066 → 0.13 교정 근거**: 0.066에서 driver 로그의 `desired total`이 5~6으로 찍혔다. 역산하면 순간 일감 303~364개이며, 이는 **전체 일감(725개)이 아니라 그 순간 큐에 있는 수**다. 처음 계산에서 전체 일감을 썼던 것이 원인이며, 0.13으로 교정 후 요청 로그가 사라졌다(= 12대로 충분하다고 계산).

**2시간 실행의 한계**: 입력이 이미 compaction된 데이터(505MB 파일)라 실제 데이터 증가 상황과 파일 구성이 다르다. 실제 증가 시에도 desired는 25대로 계산되어 **대수 결정 동작은 동일**하나, 소요시간은 더 걸린다.

### 5.2 2번 테이블 (하루 576GB, 시간당 약 24GB) — 9회 측정, 8대 + 20g 확정

측정 대상: 2026-08-12 10~16시 데이터, 2026-09-15 ~ 09-16 실행. 1번과 같은 원천을 수직분할한 테이블이라 **시간당 row 수는 1번과 같고(약 340만) row 폭만 좁다.** 공통 설정: executor 4core, ratio 0.13(test4·5 제외), `maxExecutors` 36, driver 2core/4g.

| 회차 | 시간 | 데이터 | `instances` | init / min | ratio | e.mem | 대수 | duration | dcu | dcu/GB | dcu/100만 row | idle | memory | spill |
|------|------|--------|-------------|-----------|-------|-------|------|----------|-----|--------|--------------|------|--------|-------|
| test1 | — | 24.1GiB | 12 | 12 / 12 | 0.13 | 16g | 12 | 1.5m | 0.0831 | 0.00345 | — | 26.3% | 84.1% | 10.43GiB |
| test2 | 10시 | 23.3GB | 12 | 6 / 6 | 0.13 | 16g | 12 | 1.6m | 0.0927 | 0.00398 | 0.0279 | 26.5% | 89.2% | 1.61GiB |
| test3 | 11시 | 23.4GB | 12 | 8 / 6 | 0.13 | 16g | 12 | 1.6m | 0.0885 | 0.00378 | 0.0263 | 29.2% | 95.8% | 4.66GiB |
| test4 | 12시 | 22.7GB | 12 | 8 / 8 | 0.08 | 16g | 12 | 1.5m | 0.0837 | 0.00369 | 0.0259 | 25.6% | 87.7% | 4.72GiB |
| test5 | 13시 | 22.3GB | 12 | 8 / 8 | 0.10 | 16g | 12 | 1.4m | 0.0790 | 0.00354 | 0.0245 | 28.5% | 88.1% | 4.74GiB |
| test6 | 14시 | 23.4GB | 12 | 8 / 8 | 0.10 | **20g** | 12 | 1.4m | 0.0827 | 0.00353 | 0.0249 | 27.0% | 94.4% | **0** |
| test7 | 15시 | 24.0GB | **= init** | 6 / 6 | 0.13 | 16g | **8** | 1.9m | 0.0705 | 0.00294 | **0.0208** | 19.9% | 84.7% | 6.41GiB |
| test8 | 16시 | 24.3GB | = init | 6 / 6 | 0.13 | **20g** | **8** | 1.9m | 0.0747 | 0.00307 | 0.0219 | 13.0% | 92.4% | **0** |
| **test9 (확정 설정)** | 17시 | 24.5GB | **8** | **8 / 8** | 0.13 | **20g** | **8** | **1.7m** | **0.0711** | 0.00290 | **0.0206** | **16.8%** | 93.2% | **0** |

출력 파일 품질은 9회 전부 정상 — 45~49개, avg 500~512MB, max 550~591MB. min은 test2의 275.8MB 한 건을 빼면 386~435MB.

**test1~6에서 배운 것 — 12대는 ratio가 아니라 `instances`가 만든 값이었다**

init을 6으로 낮춰도, ratio를 0.08로 낮춰도 12대였다. ratio 0.08·0.10·0.13이 우연히 전부 12를 낼 수는 없으므로 다른 바닥을 의심했고, `spark.executor.instances` 12가 원인이었다(섹션 4.8). 이 여섯 번의 dcu가 0.079~0.093으로 전부 같은 값인 이유도 전부 "12대 × 1.5분"이었기 때문이다.

**test7·8 — `instances`를 걷어내자 ratio 0.13이 8대로 수렴했다**

23~24GB × 0.32 = 7.7 → 8대. 1번 테이블에서 잡은 계수가 그대로 맞았다. 입력 파일 구성이 1번과 같아서다 — 2번 460개 / avg 50.6MB / GB당 20개, 1번 703개 / avg 54MB / GB당 19개. **ratio 0.13 공통은 유지된다.**

| | 12대 (test4~6) | 8대 (test7·8) | 변화 |
|---|---|---|---|
| duration | 1.4~1.5m | 1.9m | +27~36% |
| cores × duration | 12 × 1.5 = 18 | 8 × 1.9 = 15.2 | **−16%** |
| dcu | 0.079~0.084 | 0.071~0.075 | **−10~16%** |
| idle cores | 25~29% | 13~20% | 개선 |

1번 테이블에서 16 → 12대로 줄였을 때와 같은 모양이다. 코어를 33% 빼면 시간은 늘지만 코어 × 시간은 줄고, **duration만 보면 "느려졌다"로 오판하는** 바로 그 경우다. 실행 창은 1번 1.5분 + 2번 1.9분으로 12분에 한참 여유가 있다.

**dcu/GB로는 1번과 비교할 수 없다 — dcu/100만 row로 본다**

8대에서 dcu/GB는 0.0029~0.0031로 1번 확정값 0.0022보다 35% 높다. 그러나 두 테이블은 수직분할이라 **시간당 row 수가 같다**(1번 339만, 2번 323~341만). 정렬과 shuffle의 비용은 byte가 아니라 row 수에 크게 좌우되므로, 같은 row를 더 적은 byte로 처리하는 2번은 GB당 비용이 높아 **보일** 뿐이다. row 기준으로 놓으면 1번 12대 0.0241, 2번 8대 0.0208~0.0219로 **같은 수준**이다. 2번 12대는 0.0245~0.0279로 1번보다 비쌌다. 8이 맞는 크기라는 것이 여기서 확정된다. 판정 기준은 섹션 8.3.

**spill — 16g에서는 시간대마다 나고, 20g에서는 0**

16g에서 spill이 1.6 → 4.7 → 10.4GiB로 시간대별로 6배 움직였고, memory usage와도 맞지 않았다(84%에서 최대 spill). dcu와 duration은 spill 양과 무관하게 같았으므로 **시간 비용으로는 드러나지 않았지만**, 판단 기준 2(spill 0)에 어긋난다. 20g로 올리면 두 번(test6·test8) 모두 0이고 duration은 같다. 20g의 실제 효과는 idle cores 19.9 → 13.0%다 — spill하는 task가 꼬리를 끌어 다른 코어를 놀게 하던 것이 사라졌다. 원인과 계산은 섹션 8.2.

**test9 — 확정 설정 그대로 돌린 최종 검증**

`instances` = init = min = 8로 시작하니 6 → 8 warm-up이 사라져 duration이 1.9 → **1.7분**으로 줄었다. dcu 0.0711(test8 대비 −5%, 노이즈 안), spill 0, idle cores 16.8%로 **1번 테이블 확정값(16.7%)과 같은 수준**이다. dcu/100만 row 0.0206은 9회 중 최저다. 이 설정으로 fix한다.

**확정 설정 (2번 테이블)** — 1번과 다른 것만

| 설정 | 1번 | 2번 |
|------|-----|-----|
| `spark.executor.instances` = `initialExecutors` = `minExecutors` | 12 | **8** |
| executor memory | 16g | **20g** |

ratio 0.13, `maxExecutors` 36, executor 4core, `memoryOverhead` 4g, driver 2core/4g, Iceberg 옵션은 전부 동일하다.

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
| **hourly Compaction** | **B안 적용** (섹션 4.7 설정 + 섹션 4.8 규칙) |
| `spark.executor.instances` / `initialExecutors` / `minExecutors` | **세 값 동일.** 테이블별 `시간당 데이터GB × 0.32`. 1번 12, 2번 8, 3·4번은 측정 후 |
| `executorAllocationRatio` | 전 테이블 **0.13** (2번 테이블에서 재검증) |
| `maxExecutors` | **36 고정.** K8S quota는 확인 불가하나 리소스가 넉넉하고, 실사용량은 ratio가 정하므로 천장은 넉넉히 둬도 무해(섹션 4.6). 3시간치(약 110GB)까지 덮는다 |
| executor memory | 테이블별. 16g에서 spill이 나면 20g (섹션 8.2). 1번 16g, 2번 20g |
| 적용 시점 | **DAG 미반영.** 4개 테이블 테스트가 끝난 뒤 일괄 적용 |
| C안 (사전 산정) | 보류. 예시 파일은 유지 |

---

## 8. 테이블별 적용

**설정을 두 종류로 나눠 본다.**

| 설정 | 성격 | 테이블별 |
|------|------|---------|
| `executorAllocationRatio` | **비율(%)** | ❌ 공통 (0.13) |
| `spark.executor.instances` / `initialExecutors` / `minExecutors` | **개수(대)** | ✅ 테이블별, 세 값 동일 |
| `maxExecutors` | 개수(대) | 공통 (36) |
| executor memory | 크기 | ✅ 테이블별 (16g 또는 20g) |

**ratio가 공통인 이유**: 데이터가 작으면 Spark이 요청하려는 대수도 작아지므로, 같은 비율을 곱해도 알아서 작은 값이 나온다. 단 이것은 입력 파일 크기가 같은 테이블끼리 성립한다(섹션 4.5). hourly 4개는 append 설정이 같아 파일 구성이 같고, 2번 테이블에서 확인했다(GB당 20개 vs 19개).

**`initialExecutors`가 테이블별인 이유**: 절대 개수이므로 10GB 테이블에 12를 넣으면 4배 과다이고, 반납이 안 되므로 그대로 유지된다. 2번 테이블 test1~6이 정확히 이 상황이었다 — 24GB에 12대가 바닥으로 깔려 dcu가 8대 대비 10~16% 비쌌다.

**역할 분담**

| 설정 | 하는 일 |
|------|--------|
| `instances` / `initialExecutors` / `minExecutors` | 평소 데이터량에서 쓸 대수를 바닥으로 깐다 |
| `ratio` | 평소보다 많은 시간대에 얼마나 더 부를지 정한다 |

### 8.1 대수 — 계수 0.32는 비례식이다

1번 테이블에서 실측으로 알아낸 사실은 "37GB를 처리할 때 12대가 가장 싸다"는 것이다(16대는 노는 코어가 많아 코어 × 시간이 크고, 8대는 시간이 너무 늘어 다시 커졌다. `tuning/compaction-tuning-guide.md` §4.4). 이것을 다른 크기의 테이블에 옮기기 위해 12 ÷ 37.3 = **0.32, 즉 "1GB당 0.32대"**로 바꿔 둔 것이 계수 C다. 숫자 자체에 의미는 없고 "37GB에 12대"를 환산하는 비례식이다.

```
대수 = ceil(시간당 데이터GB × 0.32)

1번  39GB × 0.32 = 12.5 → 12   (실측 최저점 12)
2번  24GB × 0.32 =  7.7 →  8   (ratio 0.13이 8로 수렴, test7·8)
```

몇 가지 크기로 돌려 비용 최저점을 찾고 데이터 양에 비례해 옮기는 것은 Spark 리소스 right-sizing의 일반적 절차다. 단 **0.32라는 값은 이 job 전용**이다 — executor 4core, 512MB 출력 파일, `sort` 전략이라는 조건에서 나온 것이며 다른 job에는 맞지 않는다.

### 8.2 메모리 — shuffle과 spill의 산정 규칙

**shuffle 총량 ≈ 데이터 크기 × 1.5.** parquet은 컬럼 단위로 압축돼 작고, shuffle은 row를 하나씩 직렬화해 보내니 덜 압축된다.

| | 데이터(output) | shuffle | 비율 |
|---|---|---|---|
| 1번 | 41.4GiB | 58.5GiB | 1.41 |
| 2번 | 24.1GiB | 37.8GiB | 1.57 |

"shuffle은 입력의 몇 배"로 어림하는 것은 흔한 방식이나 **배수는 데이터마다 달라 job별로 실측해 정하는 것이 관행**이다. 위 값도 두 테이블 실측이다.

**task 하나의 shuffle 몫은 항상 약 0.8GB다.** 출력 파일 하나가 512MB로 고정이므로 task 수 = 데이터 ÷ 512MB이고, task당 shuffle = 512MB × 1.5 ≈ 0.8GB로 데이터 양과 무관하다(실측 1번 0.78GiB, 2번 0.80GiB). 데이터가 늘면 task 수가 늘 뿐 task 하나는 커지지 않는다. **따라서 spill 여부는 그 시간대 데이터 양이 아니라 테이블의 row 모양이 정한다** — 1번은 어느 시간대에도 0, 2번은 16g에서 어느 시간대에도 발생.

**executor 하나의 로컬 디스크도 거의 일정하다.** shuffle 총량 ÷ 대수인데 대수가 데이터 × 0.32로 따라가므로 1.5 ÷ 0.32 ≈ **executor당 약 4.7GB**다(2번 8대 37.8 ÷ 8 = 4.7GiB, 1번 82GB 24대 약 4.8GiB). 데이터가 늘어도 executor당 디스크는 늘지 않는다.

**task 하나가 정렬에 쓸 수 있는 메모리 — Spark 공식 규칙**

Spark은 heap 전체를 정렬에 내주지 않는다. Spark 3.5.8 문서 `docs/tuning.md` 133~136행: 정렬·shuffle·캐시가 쓰는 영역 M은 `(JVM heap − 300MiB) × spark.memory.fraction(기본 0.6)`이고, 나머지 40%는 사용자 자료구조·Spark 내부 메타데이터·비정상적으로 큰 record 대비용으로 남긴다. 그 M을 동시 task가 나누는 규칙은 `core/.../memory/ExecutionMemoryPool.scala` 32~33행: **task N개가 돌면 각 task는 최대 1/N, 최소 1/2N**을 받고, 그 이상 필요하면 spill한다.

```
task당 정렬 메모리(최대) = (executor memory − 300MiB) × 0.6 ÷ cores(4)

16g → (16 − 0.3) × 0.6 ÷ 4 = 2.4GB
20g → (20 − 0.3) × 0.6 ÷ 4 = 3.0GB   (+25%)
24g → (24 − 0.3) × 0.6 ÷ 4 = 3.6GB
```

`memoryOverhead`는 heap 바깥이라 이 계산과 무관하다.

**왜 1번은 16g에서 spill이 없고 2번은 났는가 — task당 row 수**

task 하나가 정렬하는 **byte**는 두 테이블이 같지만(0.8GB), **row 수**가 다르다. 출력 파일을 512MB 단위로 자르므로 row가 좁은 2번은 파일 하나에 row가 더 많이 들어간다.

| | 시간당 데이터 | 시간당 row | row 1개 (parquet) | task 수 | task당 row |
|---|---|---|---|---|---|
| 1번 | 37GB | 339만 | 약 11KB | 75 | 약 4.5만 |
| 2번 | 24GB | 340만 | 약 7KB | 47 | 약 7.2만 |

정렬 중 메모리는 압축된 byte가 아니라 압축 풀린 row들의 크기로 정해지고, row마다 붙는 고정 비용(필드별 offset, 정렬용 포인터·prefix)이 있어 row가 60% 많으면 메모리도 그만큼 더 든다. 실측으로 역산하면 2번 task의 필요량은 2.4~3.0GB 사이(16g에서 spill, 20g에서 0)이고 1번은 2.4GB 미만이다. 시간대별로 spill 양이 달랐던 것은 정렬 구간 경계가 샘플링으로 정해져 task 간 row 수가 매번 조금씩 다르기 때문이다.

> 2번 컬럼들이 parquet 압축이 더 잘 되는 타입이라면 풀었을 때 더 커지는 효과도 있을 수 있으나, 컬럼 구성을 대조하지 않아 확인된 것은 아니다. 확실한 것은 row 수 차이다.

**절차**: 새 테이블은 16g로 1회 돌려 spill이 나면 20g, 그래도 나면 24g. Spark UI task별 `Peak Execution Memory`를 받아 두면 실제 필요량이 바로 나와 이후 테이블은 계산으로 정할 수 있다.

### 8.3 판정 — 테이블 간 비교는 dcu/GB가 아니라 dcu/100만 row

hourly 4개는 같은 원천을 수직분할한 테이블이라 **시간당 row 수가 같다.** 컬럼 폭만 달라 GB가 다르므로, dcu를 GB로 나누면 row가 좁은 테이블이 비효율적으로 **보인다**(2번 8대: dcu/GB는 1번보다 +35%인데 dcu/100만 row는 −9~14%). 같은 테이블 안에서 회차를 비교할 때는 dcu/GB로 충분하지만, **테이블 사이를 비교할 때는 row로 나눈다.**

**다른 테이블 확인 항목** (테이블당 1~2회 실행)

| 확인 | 기준 | 어긋날 때 |
|------|------|----------|
| 시작 대수 | driver 로그 `Using initial executors = N`이 의도한 값인가 | `spark.executor.instances` 확인 (섹션 4.8) |
| 수렴 대수 | `시간당 데이터GB × 0.32`와 유사한가 | 입력 파일 크기 확인 (섹션 4.5) |
| `spill` | 0인가 | executor memory 16g → 20g → 24g (섹션 8.2) |
| `dcu/100만 row` | **0.021~0.024** (1번 0.0241, 2번 0.0219) | 대수 재검토 |
| duration | 2분 이내 (DAG 전체 12분 창) | — |
| 출력 파일 | avg 500MB대, **384MB 미만 파일 3개 미만** | `tuning/compaction-tuning-guide.md` §4.3 |

**3·4번 테이블 절차**: `instances` = init = min = `ceil(시간당 GB × 0.32)`, ratio 0.13, max 36, 16g로 1회. spill이 나면 20g로 1회 더. 위 표로 판정한다.

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
| `maxExecutors` 확정값 | **36으로 고정 (2026-09-16).** K8S namespace quota는 확인할 수 없으나 리소스가 넉넉하고, 실사용량은 ratio가 통제하므로 천장은 넉넉히 둬도 무해하다(섹션 4.6) |
| ~~ratio 0.066에서의 요청 로그~~ | **해소 (2026-09-16).** 12대가 이미 떠 있는데 `desired total 5~6`이 찍힌 것은 `spark.executor.instances` 12가 시작 대수의 바닥이었기 때문이다(섹션 4.8). DA의 계산은 정확했고 내릴 수단이 없었을 뿐이다 |
| 메모리 97.62% | 7회 중 최고값이며 DataFlint가 `executor.memory` 19.2g를 권고한다. **spill이 0인 동안은 조치하지 않는다** — Spark의 정렬은 가용 메모리를 최대한 쓰다가 부족하면 디스크로 넘기므로, 90%대는 한계 임박이 아니라 정상 동작이다. 감시 기준은 `spill ≠ 0` |
| 다른 hourly 테이블 | **2번 완료(섹션 5.2). 3·4번 남음** — 섹션 8.3의 절차와 기준으로 |
| 2번 테이블 spill의 압축 요인 | task당 row 수 차이는 확정, 컬럼 타입에 따른 압축 해제 팽창은 미확인(섹션 8.2). Spark UI `Peak Execution Memory`로 확인 가능 |
| ~~운영 적용 후 duration~~ | **해소.** 확정 설정(init 8)으로 돌린 test9가 1.7분 — warm-up이 빠져 test8(init 6) 1.9분보다 짧다. 예상과 일치 |

---

## 11. daily와의 분리

daily Compaction에 이 설계를 그대로 적용할 수 없다.

- ratio 0.13은 hourly의 파일 구성·계수(C=0.32) 기준이다
- daily는 30~60분 job이라 `executorIdleTimeout` 60초가 전체의 2~3%에 불과해 **반납이 실제로 일어날 수 있다.** 반납이 되면 판단 근거가 달라진다
- daily Compaction의 대상은 `day` 파티션 테이블들이며 크기·파일 구성이 공유되지 않아 아무것도 산정할 수 없다 (`compaction-tuning-guide.md` §8.1)

daily 튜닝 후 별도로 판단한다.
