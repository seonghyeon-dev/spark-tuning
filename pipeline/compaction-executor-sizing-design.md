# Compaction executor 자원 할당 설계

| 항목 | 내용 |
|------|------|
| 대상 | hourly Compaction DAG |
| 목적 | 데이터 증가·시간대별 편차에 맞춰 executor 수를 자동 조절 |
| 전제 | 튜닝 결과 확정 (`tuning/compaction-tuning-guide.md`) |
| 결론 | **Spark Dynamic Allocation + `executorAllocationRatio` 채택.** 1번 9회 + 2번 12회 + 3번 9회 + 4번 14회 실측 검증(섹션 5.1~5.4, 요약 5.5). **`spark.executor.instances` = `initialExecutors` = `minExecutors` 필수** (섹션 4.8) |
| 진행 | **4개 테이블 heap·`memoryOverhead` 전부 확정 (2026-09-21, 3·4번 18g + 3g). DAG 일괄 반영 대기** — 최종 설정은 섹션 5.5. 여러 시간치 재처리(1번 9시간치, 2번 20시간치, 3번 22시간치, 4번 6시간치)도 같은 설정으로 task error 0 확인, executor 수 수렴 원인(동시 파티션 수) 확정, `max-concurrent-file-group-rewrites` 10 → 12(섹션 5.5) |

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

**쉽게 말하면 — DA는 무엇을 보고 올리나**

driver 안의 관리자가 0.1초마다 "지금 돌고 있는 task + 줄 서 있는 task"를 센다. 목표 대수는 이것이다.

```
목표 대수 = ceil( (실행 중 task + 대기 task) × ratio(0.13) ÷ executor당 slot(4) )
```

대기 task가 1초(`schedulerBacklogTimeout`) 이상 계속 남아 있으면 요청을 시작하고, 이후 1초마다 요청량을 1, 2, 4, 8…로 배로 늘리며 목표까지 채운다. 목표는 `maxExecutors`(36)를 넘지 못하고, 시작 대수 `max(initial, min, instances)` 아래로는 내려가지 않는다. 3번 테이블 test1·2에서 8 → 12가 된 것이 이 계산이다 — 39GB면 순간 대기 task가 300개대라 300 × 0.13 ÷ 4 ≈ 10~12가 나온다.

**결국 input이 많으면 더 늘어난다는 뜻이다.** task 수는 데이터 양을 따라가므로(읽을 파일·만들 출력 파일이 늘어남), 데이터가 많으면 목표가 커져 executor를 더 부른다. 실제로는 평소 1시간치는 목표가 시작 대수와 같아서 아무 일도 안 일어나고, 재처리 trigger로 2~3시간치가 한 번에 들어올 때만 늘어난다(1번 82GB → 24대). 도중에 줄어드는 일은 없다(섹션 4.2). 즉 DA의 역할은 "평소보다 큰 범위가 왔을 때 자동 증원" 하나다.

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

### 5.1 1번 테이블 (하루 945GB, 시간당 약 39GB) — 9회 측정, 12대 + 16g 확정

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

**16g + `memoryOverhead` 3g 최종 검증 (test2·3, 2026-09-21)** — 1번은 처음부터 16g에서 spill 0이었고, overhead 3g(섹션 8.4)를 1번에서도 확인한 것이다. 데이터 = DataFlint input ÷ 2.

| 회차 | 처리량 | 데이터 | 대수 | duration | dcu | dcu/1시간치 | dcu/GB | shuffle ÷ 데이터 | idle | spill | task error |
|------|--------|--------|------|----------|-----|------------|--------|-----------------|------|-------|-----------|
| test2 | 1시간 | 40.9GiB | 12 → 13 | 1.4m | 0.0874 | 0.0874 | 0.00214 | 1.41 | 19.5% | 0 | 0 |
| test3 | 9시간 | 349.6GiB | 12 → 36 | 4.0m | 0.6508 | 0.0723 | 0.00186 | 1.41 | 11.1% | 0 | 0 |

**16g + 3g 확정.** dcu/GB 0.00214는 정적 12대 기준값 0.00219와 같다. 9시간치는 천장 36대에서 4.0분(1시간치당 0.44분), dcu/1시간치는 17% 싸다. 이로써 `memoryOverhead` 3g는 4개 테이블 전부에서 실측됐다.

### 5.2 2번 테이블 (하루 576GB, 시간당 약 24GB) — 12회 측정, 8대 + 20g 확정

측정 대상: 2026-08-12 10~16시 데이터, 2026-09-15 ~ 09-16 실행. 1번과 같은 원천을 수직분할한 테이블이라 **시간당 row 수는 1번과 같고(약 340만) row 폭만 좁다.** 공통 설정: executor 4core, ratio 0.13(test4·5 제외), `maxExecutors` 36, driver 2core/4g.

| 회차 | 시간 | 데이터 | `instances` | init / min | ratio | e.mem | 대수 | duration | dcu | dcu/GB | dcu/100만 row | idle | memory | spill |
|------|------|--------|-------------|-----------|-------|-------|------|----------|-----|--------|--------------|------|--------|-------|
| test1 | — | 24.1GB* | 12 | 12 / 12 | 0.13 | 16g | 12 | 1.5m | 0.0831 | 0.00345 | — | 26.3% | 84.1% | 10.43GiB |
| test2 | 10시 | 23.3GB | 12 | 6 / 6 | 0.13 | 16g | 12 | 1.6m | 0.0927 | 0.00398 | 0.0279 | 26.5% | 89.2% | 1.61GiB |
| test3 | 11시 | 23.4GB | 12 | 8 / 6 | 0.13 | 16g | 12 | 1.6m | 0.0885 | 0.00378 | 0.0263 | 29.2% | 95.8% | 4.66GiB |
| test4 | 12시 | 22.7GB | 12 | 8 / 8 | 0.08 | 16g | 12 | 1.5m | 0.0837 | 0.00369 | 0.0259 | 25.6% | 87.7% | 4.72GiB |
| test5 | 13시 | 22.3GB | 12 | 8 / 8 | 0.10 | 16g | 12 | 1.4m | 0.0790 | 0.00354 | 0.0245 | 28.5% | 88.1% | 4.74GiB |
| test6 | 14시 | 23.4GB | 12 | 8 / 8 | 0.10 | **20g** | 12 | 1.4m | 0.0827 | 0.00353 | 0.0249 | 27.0% | 94.4% | **0** |
| test7 | 15시 | 24.0GB | **= init** | 6 / 6 | 0.13 | 16g | **8** | 1.9m | 0.0705 | 0.00294 | **0.0208** | 19.9% | 84.7% | 6.41GiB |
| test8 | 16시 | 24.3GB | = init | 6 / 6 | 0.13 | **20g** | **8** | 1.9m | 0.0747 | 0.00307 | 0.0219 | 13.0% | 92.4% | **0** |
| **test9 (확정 설정)** | 17시 | 24.5GB | **8** | **8 / 8** | 0.13 | **20g** | **8** | **1.7m** | **0.0711** | 0.00290 | **0.0206** | **16.8%** | 93.2% | **0** |

\* test1의 데이터 크기는 `.files` 집계 전이라 DataFlint `output`(24.12GiB 표기)이고, test2 이후는 `.files` 합계다. 표는 도구 표기값을 그대로 쓰고 비율도 표기값으로 계산했다 — 단위 환산 차이(약 7%)는 노이즈 기준선 15% 안이다. test1은 시간·row 수를 기록하지 않아 dcu/100만 row가 없다.

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

**18g 검증 (test10~12, 2026-09-21) — 20g 유지**

3·4번이 18g로 확정된 뒤(섹션 5.3·5.4) 2번도 18g가 되는지 봤다. `memoryOverhead`는 3g.

| 회차 | 처리량 | heap | 데이터 | 대수 | duration | dcu | dcu/1시간치 | shuffle ÷ 데이터 | idle | spill |
|------|--------|------|--------|------|----------|-----|------------|-----------------|------|-------|
| test10 | 1시간 | 18g | 24.5GiB | 8 | 1.7m | 0.0672 | 0.0672 | 1.58 | 13.2% | 0 |
| test11 | 1시간 | 16g | 24.2GiB | 8 | 1.9m | 0.0720 | 0.0720 | 1.58 | 21.2% | **6.28GiB** |
| test12 | 20시간 | 18g | 479.8GiB | 8 → 23 | 10.9m | 1.25 | 0.0625 | 1.58 | 3.6% | **936MiB** |

**20g 유지.** 18g는 1시간치 1회에서 0이었지만 20시간치(20개 시간대를 전부 훑는 더 엄한 시험)에서 936MiB가 났다. 데이터의 0.2%라 시간 비용은 없지만(10.9분 = 1시간치당 0.55분, 규칙대로) 운영에서도 어떤 시간대엔 18g가 spill을 낸다는 뜻이고, 판정 규칙은 "spill 0인 최소값"이다(섹션 8.3). 2번은 row가 좁아(7KB) 512MB 묶음에 row가 7.2만 개라 task당 정렬 메모리가 4개 테이블 중 가장 크다 — 필요량이 18g(2.66GB)와 20g(3.0GB) 사이에 있다. 18g로 가면 절감이 2g × 8 = 16g뿐이라 규칙에 예외를 만들 값이 아니다. test10(18g + 3g) dcu 0.0672가 test9(20g + 4g) 0.0711보다 5.5% 낮은 것은 메모리 몫이다(섹션 5.3 "dcu에는 메모리도 들어간다"). 23대에서 멈춘 이유는 섹션 5.5.

**확정 설정 (2번 테이블)** — 1번과 다른 것만

| 설정 | 1번 | 2번 |
|------|-----|-----|
| `spark.executor.instances` = `initialExecutors` = `minExecutors` | 12 | **8** |
| executor memory | 16g | **20g** |

ratio 0.13, `maxExecutors` 36, executor 4core, `memoryOverhead` 3g(섹션 8.4), driver 2core/4g, Iceberg 옵션은 전부 동일하다.

---

### 5.3 3번 테이블 (시간당 약 37~40GB) — 9회 측정, 12대 + 18g 확정

측정 대상: 2026-08-12 09~14시 데이터, 2026-09-16 ~ 09-17 실행. 크기·row 수·row 폭(약 11.5KB)이 1번과 거의 같다. 공통 설정: executor 4core, ratio 0.13, `maxExecutors` 36, `memoryOverhead` 4g(test7~9는 3g), driver 2core/4g. test1·2는 `instances` = init = min = 8, test3~6은 12.

| 회차 | 시간 | 데이터 | 시작 | e.mem | 대수 | duration | dcu | dcu/GB | dcu/100만 row | idle | memory | spill | 파일 |
|------|------|--------|------|-------|------|----------|-----|--------|--------------|------|--------|-------|------|
| test1 | 09시 | 39.7GB | 8 | 16g | 8 → 12 | 1.7m | 0.0928 | 0.00234 | 0.0269 | 11.0% | 88.5% | 899MiB | 79개, min 380MB |
| test2 | 10시 | 38.4GB | 8 | 20g | 8 → 12 | 1.8m | 0.1075 | 0.00280 | 0.0324 | 22.6% | 88.4% | 0 | 76개, min 420MB |
| test3 | 11시 | 38.6GB | 12 | 20g | 12 | 1.8m | 0.1067 | 0.00276 | 0.0317 | 19.6% | 94.3% | 0 | 77개, min 344MB |
| test4 | 12시 | 37.4GB | 12 | **16g** | 12 | 1.8m | 0.1011 | 0.00270 | 0.0313 | 17.4% | 97.1% | **1.77GiB** | 75개, min 318MB |
| test5 | 13시 | 36.7GB | 12 | 20g | 12 | 1.6m | 0.0955 | 0.00260 | 0.0295 | 17.8% | 97.6% | 0 | 73개, min 378MB |
| **test6** | 14시 | 38.5GB | 12 | 20g | 12 | 1.7m | 0.1066 | 0.00277 | 0.0321 | 25.1% | 96.2% | 0 | 77개, min 365MB |

test2 입력: 704개 / avg 55.8MB / 38.4GB — 1번(703개, 54MB)과 같은 파일 구성. DataFlint input 76.29GiB / output 38.38GiB / shuffle 58.04GiB (input ÷ output 1.99, shuffle ÷ output 1.51).

**대수**: 8로 시작한 두 번 모두 ratio 0.13이 12로 올렸고, 산정식 39 × 0.32 = 12.5도 12~13이다. 2번(8)·3번(12) 두 테이블에서 ratio 0.13이 각각 다른 값으로 수렴했으므로 **공통 ratio는 확정**이다. 운영값 `instances` = init = min = **12**.

**메모리**: 16g에서 두 번 다 spill(899MiB, 1.77GiB), 20g에서 네 번 다 0. 20g의 dcu 비용은 test4 대비 +5.5%다. 이어서 **18g + `memoryOverhead` 3g**로 3회 더 돌렸다(2026-09-20~21, test7~9). 4번 test5에서 18g가 spill 0이었던 신호(섹션 8.4)를 3번에서 확인한 것이다.

**18g + 3g 검증 (test7~9)** — 데이터 = DataFlint input ÷ 2 (sort 전략은 데이터를 2번 읽는다, `tuning/compaction-tuning-guide.md` §2.4)

| 회차 | 처리량 | 데이터 | 대수 | duration | dcu | dcu/1시간치 | dcu/GB | shuffle ÷ 데이터 | idle | spill | task error |
|------|--------|--------|------|----------|-----|------------|--------|-----------------|------|-------|-----------|
| test7 | 1시간 | 39.4GiB | 12 | 1.6m | 0.0936 | 0.0936 | 0.00238 | 1.52 | 15.2% | 0 | 0 |
| test8 | 2시간 | 80.3GiB | 12 → 24 | 1.8m | 0.1959 | 0.0980 | 0.00244 | 1.51 | 20.4% | 0 | 0 |
| **test9** | **22시간** | **875.5GiB** | 12 → 36 | 10.4m | 1.77 | 0.0805 | 0.00202 | 1.51 | 3.8% | **0** | **0** |

**18g 확정.** 1시간치뿐 아니라 22시간치(하루 거의 전부, 천장 36대)에서도 spill 0·task error 0이다. 22시간치가 같은 메모리로 되는 이유는 섹션 5.5 "여러 시간치 재처리"에 정리했다. 16g와의 차이는 task당 정렬 메모리 2.4GB → 2.66GB(`(18g − 300MiB) × 0.6 ÷ 4`)로 10%뿐인데 spill이 0이 됐으므로, 3번 task의 실제 필요량은 2.4~2.66GB 사이다.

**dcu에는 메모리도 들어간다.** duration이 같은 16g ↔ 20g 쌍에서 dcu가 일관되게 +5~6%였다(2번 test5→6, test7→8, 3번 test4→test3·6). `tuning/compaction-tuning-guide.md` §6.4의 "dcu는 cores × duration에 비례"는 메모리를 16g로 고정했을 때의 관측이며, 메모리를 25% 올리면 그 몫의 비용이 붙는다. 따라서 **메모리 증설은 spill을 없애는 데 필요한 만큼만** 한다.

**dcu/100만 row가 1번보다 30% 높다 — 테이블 성질이다.** 4회 일관(0.0295~0.0324 vs 1번 0.0241)이라 노이즈가 아니다. parquet 폭은 1번과 같은데 16g에서 spill이 나고(1번은 7회 0) shuffle ÷ output도 1.51로 1번(1.41)보다 크다. 같은 byte를 메모리에 올렸을 때 더 커지는 컬럼 구성이라는 뜻이며, 정렬·직렬화 비용이 그만큼 붙는다. 설정으로 줄이는 값이 아니다. 판정 기준은 섹션 8.3에서 정정했다.

**확정 설정 (3번 테이블)** — 1번과 다른 것은 executor memory **18g**뿐. `instances` = init = min = 12는 1번과 같다.

---

### 5.4 4번 테이블 (시간당 약 37~39GB) — 14회 측정, 12대 + 18g 확정

측정 대상: 2026-08-12 09~10시 데이터, 2026-09-17 실행. 크기·row 수·입력 파일 구성(723개, avg 54.5MB)이 1번·3번과 같다. 공통 설정(test1·2): executor 4core, ratio 0.13, `maxExecutors` 36, `memoryOverhead` 4g, driver 2core/4g. test3~9는 `memoryOverhead` 시행착오(섹션 8.4), test10~14는 18g + 3g 검증(아래).

| 회차 | 시간 | 데이터 | 시작 | e.mem | 대수 | duration | dcu | dcu/GB | dcu/100만 row | idle | memory | spill | 파일 |
|------|------|--------|------|-------|------|----------|-----|--------|--------------|------|--------|-------|------|
| test1 | 09시 | 38.5GB | 8 | 16g | 8 → 12 | 2.1m | 0.1198 | 0.00311 | 0.0347 | 18.4% | 87.4% | **1.76GiB** | 77개, min 335MB |
| **test2** | 10시 | 37.2GB | 12 | **20g** | 12 | 1.7m | 0.1042 | 0.00280 | 0.0314 | 20.5% | 94.8% | **0** | 74개, min 401MB |

3번과 같은 결과다. ratio 0.13이 12로 수렴(38 × 0.32 = 12.3), 16g에서 spill, 20g에서 0, row당 비용은 3번(0.030~0.032)과 같은 수준.

**18g + 3g 검증 (test10~14, 2026-09-18~21)** — `memoryOverhead` 시행착오(test3~9, 섹션 8.4) 중 test5(18g + 2g)에서 spill 0이 나와 18g를 검증했다. 처리량을 1시간치에서 6시간치까지 늘렸다.

| 회차 | 처리량 | 데이터 | 공식 목표 | 대수 | duration | dcu | dcu/1시간치 | dcu/GB | shuffle ÷ 데이터 | idle | memory | spill | task error |
|------|--------|--------|----------|------|----------|-----|------------|--------|-----------------|------|--------|-------|-----------|
| test10 | 1시간 | 39.4GiB | 11.5 → 바닥 12 | 12 | 1.9m | 0.1077 | 0.1077 | 0.00273 | 1.54 | 19.0% | 92.6% | 0 | 0 |
| test11 | 2시간 | 78.1GiB | 22.8 → 23 | 12 → 24 | 2.0m | 0.2165 | 0.1083 | 0.00277 | 1.54 | 19.1% | 97.7% | 0 | 0 |
| test12 | 3시간 | 114.0GiB | 33.3 → 34 | 12 → 31 | 2.0m | 0.2777 | 0.0926 | 0.00244 | 1.54 | 17.4% | 95.2% | 0 | 0 |
| test13 | 5시간 | 197.8GiB | 57.9 → 천장 36 | 12 → 36 | 2.7m | 0.4510 | 0.0902 | 0.00228 | 1.55 | 13.3% | 97.8% | 0 | 0 |
| test14 | 6시간 | 231.6GiB | 67.7 → 천장 36 | 12 → 33 | 3.1m | 0.4888 | 0.0815 | 0.00211 | 1.54 | 9.0% | 미기록 | 0 | 0 |

공식 목표 = 데이터GiB × 9(task/GB) × 0.13 ÷ 4 = 데이터GiB × 0.29 (섹션 4.4). 데이터 = DataFlint input ÷ 2.

**18g 확정.** 5회 전부 spill 0·task error 0. 4번 task의 필요량도 3번과 같이 2.4~2.66GB 사이다.

**DataFlint "Executor memory under-provisioned" alert는 무시한다.** test11~13에서 떴다. 이 alert는 executor의 peak 메모리 사용률 %가 DataFlint 기준선을 넘었다는 뜻이고, test10(92.6%)에는 안 뜨고 test12(95.2%)부터 떴으므로 기준선은 92.6~95.2% 사이다. Java는 heap이 찰 때까지 GC를 미루므로 데이터만 충분하면 필요량과 무관하게 90% 후반까지 찬다 — 20g일 때도 97.6%였다(섹션 10). 부족의 신호는 사용률이 아니라 spill이고, 그것이 0이다.

**확정 설정 (4번 테이블)** — 3번과 동일. `instances` = init = min = **12**, executor memory **18g**, `memoryOverhead` 3g.

### 5.5 4개 테이블 확정 설정 요약

| 테이블 | 시간당 데이터 | `instances` = init = min | executor memory | dcu/100만 row (확정 설정) | 검증 횟수 |
|--------|--------------|-------------------------|-----------------|--------------------------|----------|
| 1번 | 37~40GB | 12 | 16g | 0.0241 | 9회 (섹션 5.1) |
| 2번 | 23~25GB | 8 | 20g | 0.0206~0.0219 | 12회 (섹션 5.2) |
| 3번 | 37~40GB | 12 | 18g | 0.0295~0.0324 (20g 측정) | 9회 (섹션 5.3) |
| 4번 | 37~39GB | 12 | 18g | 0.0314 (20g 측정) | 14회 (섹션 5.4) |

공통: `dynamicAllocation.enabled=true`, `executorAllocationRatio=0.13`, `maxExecutors=36`, executor 4core, **executor `memoryOverhead` 3g**(4개 테이블 명시, 섹션 8.4), driver 2core/4g(`memoryOverhead` 기본값), Iceberg 옵션은 `tuning/compaction-tuning-guide.md` §5.

**DAG 반영용 최종 설정 (2026-09-21)**

| 테이블 | `instances` = init = min | executor memory | executor `memoryOverhead` | pod 메모리 합계 |
|--------|-------------------------|-----------------|--------------------------|----------------|
| 1번 | 12 | 16g | 3g | 19g × 12 = 228g |
| 2번 | 8 | 20g | 3g | 23g × 8 = 184g |
| 3번 | 12 | 18g | 3g | 21g × 12 = 252g |
| 4번 | 12 | 18g | 3g | 21g × 12 = 252g |

4개 합계 916g. 2026-09-18 표(3·4번 20g, 964g) 대비 48g 감소. `memoryOverhead` 2g는 executor 유실이 나서 쓰지 않는다(섹션 8.4).

**판정 지표에 task error rate·executor 유실을 포함한다.** job 성공만 보면 executor가 죽고 재실행된 것을 놓친다(`memoryOverhead` 2g 사례). DataFlint `task error rate` 0%, failed stage 0건이 spill 0과 함께 필수 조건이다.

**idle cores 17~25%는 구조적인 값이다.** 12대 × 4core = 48 slot인데 정렬·쓰기 task는 출력 파일 수만큼(74~77개)이라 첫 회차 48개 뒤 둘째 회차에 26~29개만 남아 slot 19~22개가 논다. 이 구간의 idle이 40%대이고 앞 단계까지 평균 내면 job 전체 17~25%다. 1번 확정값 16.7%도 같은 구조이며, 17~25%의 흔들림은 둘째 회차 꼬리와 duration 반올림(0.1분 = 6%)이다. 3번 test1(8 → 12) 18%와 test2(12 시작) 20%가 같으므로 warm-up과는 무관하다. 줄이려면 slot을 task 수에 맞춰야 하는데 20대(80 slot, 1회차)는 코어가 늘어 dcu가 오르고(1번에서 16대가 +15%), 10대(40 slot, 40 + 37)는 이득이 노이즈 15% 안일 가능성이 커 쫓지 않는다. `tuning/compaction-tuning-guide.md` §7.2의 "20% 이하면 양호"는 DataFlint 경고 기준이지 판정 기준이 아니다.

**여러 시간치 재처리에서도 메모리 설정은 그대로다 (3번 22시간치·4번 6시간치 실측, 2026-09-21)**

3번 test9는 22시간치 875GiB를 36대·18g + 3g로 10.4분에 처리했고 spill 0·task error 0이었다. 1시간치와 같은 메모리로 22배의 데이터가 되는 이유는 **Spark가 데이터를 한 번에 메모리에 올리지 않기 때문**이다. `tuning/compaction-tuning-guide.md` §2.4의 카드 묶음으로 말하면:

| | 1시간치 (test7) | 22시간치 (test9) | 비고 |
|---|---|---|---|
| 데이터 | 39.4GiB | 875.5GiB | 22배 |
| 묶음(쓰기 task) 수 | 약 77개 | 약 1,750개 | 22배 — 512MB로 자르므로 **개수만** 는다 |
| 묶음 1개 크기 | 512MB | 512MB | 같다 |
| executor가 동시에 드는 묶음 | 4개 (task 4개) | 4개 | 같다 → **heap 필요량 같다** |
| executor가 동시에 주고받는 조각 | task 4개분 | task 4개분 | 같다 → **overhead 필요량 같다** |
| executor 디스크의 shuffle 통 | 60GiB ÷ 12 = 5GiB | 동시에 도는 파티션 10~12개분 (아래) | 총량이 아니라 **동시 파티션 수**가 정한다 |
| duration | 1.6m | 10.4m | 묶음 수 ÷ slot 수 |

heap은 "지금 정렬 중인 묶음 4개"만 담고, overhead는 "지금 네트워크로 오가는 조각"만 담는다. 나머지 묶음은 S3의 parquet와 executor 디스크의 shuffle 통에 있다. 데이터가 늘면 그 통이 커지고 회차가 늘 뿐이다.

**비용은 클수록 싸진다.** dcu/1시간치가 1시간 0.094~0.108 → 22시간 0.081로 25% 내려갔다(4번 6시간치도 0.082). 계획·warm-up·마지막 회차의 빈 slot 같은 고정 비용이 묻히기 때문이며, idle cores도 19% → 3.8%로 떨어진다. 재처리 DAG가 여러 시간을 한 번에 trigger하는 것은 시간별로 도는 것보다 싸다.

**duration은 천장 36대에서 1시간치당 약 0.5분이다.** test9 10.4 ÷ 22 = 0.47, test14 3.1 ÷ 6 = 0.52. 재처리 범위가 n시간이면 약 0.5n분으로 잡으면 된다(3시간치까지는 대수가 비례해 늘어 2분 안팎, 그 위는 천장이라 비례 증가).

**executor 디스크(`spark-local-dir-1`)는 hourly 기준 executor당 약 5GiB다.** shuffle 통은 executor 디스크에 쓰이고, 1시간치 shuffle 60GiB ÷ 12대 = 5GiB(1번 4.8, 2번 4.8, 3·4번 5.0). **권장 10GiB × 노드당 executor 수.** 재처리로 n시간치를 한 번에 돌리면 최악 n × 5GiB(끝난 파티션의 통을 driver GC 전까지 안 지울 때)이고, 동시 파티션 12개분(약 6~7GiB)에 그칠 수도 있다 — 22시간치(test9)가 현 디스크로 성공했으니 지금 용량은 충분하며, 정확한 값은 운영 첫 실행에서 executor pod의 `spark-local-dir-1` 사용량을 한 번 보면 된다(섹션 10). 디스크가 차면 shuffle write가 `No space left on device`로 task 실패 → 재시도 → job 실패다(executor가 죽는 게 아니라 task가 죽는다). 참고로 `memoryOverhead` 2g 실험에서 OOMKilled로 죽은 executor는 종료 훅이 안 돌아 자기 디렉터리를 못 지우므로, hostPath에 `spark-*`·`blockmgr-*` 잔여 디렉터리가 남아 있을 수 있다.

**job 하나의 K8s 메모리 = driver.memory + driver `memoryOverhead` + (executor.memory + executor `memoryOverhead`) × executor 수.** Spark on K8s는 request = limit로 낸다. executor 수는 평소 = 시작 대수(12), 재처리 = 아래 수렴값(2번 23, 나머지 36)으로 두 값을 잡는다 — 3번 재처리 peak는 4g + 0.4g + 21g × 36 = 760g. `spark.memory.offHeap.size`·pyspark memory는 안 쓰므로 더할 것 없고, 디스크는 이 식에 없다.

**executor 수는 총 데이터가 아니라 "동시에 돌고 있는 파티션들의 데이터 합"으로 정해진다 (2026-09-21 원인 확정).** 4번 3시간치 31대·6시간치 33대, 2번 20시간치 **23대**(공식 140 → 천장 36인데)가 천장에 못 미친 이유다. 2번의 23 = 8 + 1 + 2 + 4 + 8, DA 증원이 4단계에서 멈춘 수다.

1. Compaction은 파티션(시간 1개 × `par_a` 값 1개 = file group)마다 별도 Spark job을 띄우고, **`max-concurrent-file-group-rewrites`개까지만 동시에** 돌린다. 1시간치는 파티션 4개라 전부 동시에 돌고, 22시간치는 88개를 10개씩 돌린다
2. DA의 목표 대수는 **지금 돌고 있는 stage의 task 수**로 계산한다(섹션 4.4). job 전체가 아니다
3. 그러니 DA 눈에는 늘 "동시 파티션 10개분"만 보인다. 1번 튜닝에서 39GB에 12대가 최적이었으니 **GB당 0.3대**, 이걸 동시에 도는 데이터에 곱한 것이 대수이고 최대 36이다

| 처리량 | 파티션 수 | 동시에 도는 파티션 | 그 데이터 합 | 대수 = 합 × 0.3 | 실측 (4번) |
|---|---|---|---|---|---|
| 1시간 | 4 | 4 (전부) | 39GB | 12 | 12 |
| 2시간 | 8 | 8 (전부) | 78GB | 23 | 24 |
| 3시간 | 12 | 10 | 약 95GB (작은 A·D 포함) | 28 | 31 |
| 5시간 | 20 | 10 | 약 170GB (큰 B·C 위주) | 49 → 천장 36 | 36 |
| 6시간 | 24 | 10 | 약 150GB | 45 → 천장 36 | 33 |

3시간과 5시간이 다른 이유는 파티션 크기가 다 달라서다(B·C 각 17GB, A 5GB, D 0.4GB). 작은 A·D는 몇 초 만에 끝나 자리를 비우고 오래 걸리는 B·C가 10자리를 채우므로, 오래 돌수록 동시 10개가 큰 것들로 차서 36에 닿는다. 2번은 파티션이 작아(B·C 각 10.5GB) 10개 = 60~80GB → 23대에서 멈춘다. 확인은 Spark UI Jobs 탭에서 동시 실행 job이 설정값을 안 넘는 것 하나면 된다.

**이것이 "22시간치를 executor 12 × 22대 없이 처리하는 이유"다.** 12대·18g는 "1시간에 다 처리하려고" 정한 값이 아니라 **파티션 1개(약 17GB)를 512MB 묶음으로 나눠 정렬할 때 비용이 가장 싼 값**이다. 파티션 크기는 시간당 데이터로 한정돼 있고, 22시간치라고 파티션이 커지는 게 아니라 개수가 88개로 늘 뿐이다. 그래서 88개를 10개씩 순서대로 돌리면 시간만 22배 가까이 늘고(1.6분 → 10.4분) 메모리·대수는 그대로다.

**`max-concurrent-file-group-rewrites` 10 → 12 (2026-09-21).** 재처리 3시간치가 파티션 12개라 한 번에 돌리는 값으로 맞췄다. hourly는 4개라 무관하고, 3시간치는 동시 데이터 95 → 114GB로 대수 31 → 34, 2.0분 → 약 1.8분, executor당 shuffle 디스크 7 → 8.5GiB, driver 동시 job 10 → 12개 — 손실은 이것뿐이다. **상한은 16.** 36 ÷ 0.3 = 120GB, 즉 큰 파티션 7개(17 × 7 = 119GB)가 동시에 돌면 executor는 이미 36대로 꽉 차고, 그 위로는 동시 파티션을 늘려도 executor가 안 늘어 시간이 안 줄며 디스크만 파티션 수에 비례해 는다. 규칙: **재처리 시간 수 × 4, 상한 16.** 2번만 파티션이 작아 23대에서 멈추는데, 2번 재처리를 빠르게 하려면 15로 올리면 36대가 된다(20시간치 10.9분 → 약 7분, 비용 동일). 재처리 속도의 진짜 손잡이는 이 값이 아니라 `maxExecutors` 36이다 — 72로 올리고 동시 파티션도 20으로 올리면 시간 절반, 비용 동일. 재처리 속도가 문제 될 때 올린다.

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
| `spark.executor.instances` / `initialExecutors` / `minExecutors` | **세 값 동일.** 테이블별 `시간당 데이터GB × 0.32`. 1번 12, 2번 8, 3번 12, 4번 12 (섹션 5.5) |
| `executorAllocationRatio` | 전 테이블 **0.13** (2번 8대·3번 12대로 각각 수렴 — 재검증 완료) |
| `maxExecutors` | **36 고정.** K8S quota는 확인 불가하나 리소스가 넉넉하고, 실사용량은 ratio가 정하므로 천장은 넉넉히 둬도 무해(섹션 4.6). 3시간치(약 110GB)까지는 공식대로 늘고 그 위는 천장. 천장에서도 22시간치가 10.4분에 끝난다(섹션 5.5) |
| executor memory | 테이블별. spill이 0이 되는 최소값 (섹션 8.2). 1번 16g, 2번 20g, 3·4번 18g. **메모리도 dcu에 반영되므로(+5~6%/4g) 필요한 만큼만** |
| executor `memoryOverhead` | **3g, 4개 테이블 명시.** 4번에서 1g job 실패·2g executor 유실(task error 1~3%)·3g 깨끗(섹션 8.4). 고정값 — 데이터 양과 무관 |
| driver `memoryOverhead` | 기본값(heap의 10%, 약 410MB) 유지 |
| 적용 시점 | **DAG 미반영.** 4개 테이블 heap·`memoryOverhead` 확정 완료(2026-09-21, 3·4번 18g 포함) — 섹션 5.5 표로 일괄 적용 |
| C안 (사전 산정) | 보류. 예시 파일은 유지 |

**더 조절할 여지가 있는가 (2026-09-21 판단)**

| 항목 | 판단 | 남은 여지 |
|------|------|----------|
| executor 수 | 1번에서 16·12·8 실측으로 12가 비용 최저, 2·3·4번은 ratio가 그 비율대로 수렴. 확정 | 3·4번 10대는 이득이 있어도 노이즈 15% 안이라 측정으로 구분 불가. 안 한다 |
| executor core 4 | 한 번도 안 바꿈. 2나 8로 바꾸면 ratio 0.13·계수 0.32가 전부 무효가 되어 10회 이상 재측정 | 4가 통상 최적점. 손대지 않는다 |
| executor memory | 2g 단위로 spill 0인 최소값. 1번 16g, 2번 20g, 3·4번 18g | **없음 (2026-09-21 확정).** 18g + 3g = 21g로 3번 1·2·22시간치, 4번 1~6시간치를 돌려 spill 0·task error 0(섹션 5.3·5.4). 두 테이블 합계 48g 절감. 2번은 16g에서 10GiB까지 났으므로 20g 유지, 1번은 16g에서 이미 0. 17g는 이득 12g에 시행착오 비용이 더 크다 |
| `memoryOverhead` 3g | 1g job 실패·2g executor 유실·3g 깨끗 | 없음 |
| driver 2core/4g + 기본 overhead | 효과가 노이즈 안 | 없음 |
| Iceberg 옵션 | 1번에서 확정. `max-concurrent-file-group-rewrites`만 10 → 12 (재처리 3시간치 = 파티션 12개, 섹션 5.5) | 없음 |
| `spark.memory.fraction` 0.6 → 0.8 | heap 중 정렬에 쓸 수 있는 비율. Compaction은 캐시가 0이라 올릴 수 있다. 16g + 0.8이면 task당 정렬 메모리 (16,384 − 300) × 0.8 ÷ 4 = 3.2GB로 20g + 0.6의 3.0GB보다 크다 → 2번 16g 가능성, pod 합계 −32g 이상 | **보류 (2026-09-21).** 위험은 남은 20%로 Parquet 쓰기 버퍼(task당 200~300MB)가 모자라면 executor가 Java OOM으로 죽는 것. 보통은 기본값 0.6 그대로 둔다. 하려면 2번 16g + 0.8로 2~3회 |
| shuffle 압축 codec (lz4 → zstd) | shuffle 임시 파일이 20~30% 작아지는 대신 CPU 증가. 테이블의 `write.parquet.compression-codec=zstd`(S3 결과 파일 압축)와는 **무관** | **안 한다.** 디스크(executor당 5GiB)·네트워크가 병목이 아니고 CPU가 병목(idle cores 17%)이라 dcu가 는다 |
| `partial-progress.enabled` | 파티션 몇 개마다 중간 commit | **false 유지.** `rewrite-all=true`라 재실행하면 커밋된 파티션도 다시 쓰므로 재실행 비용을 안 줄이고 snapshot만 는다. `rewrite-all=false`는 재처리(늦게 온 데이터가 섞인 파티션)에서 작은 파일만 골라 써 정렬 묶음이 두 벌이 되므로 못 쓴다 |
| `target-file-size-bytes` 512MB | 묶음 크기 = task 크기·파일 수·Trino split 수 | **안 건드린다.** 읽기 성능 테스트가 512MB 기준이라 바꾸면 스키마 설계부터 재검증 |

---

## 8. 테이블별 적용

**설정을 두 종류로 나눠 본다.**

| 설정 | 성격 | 테이블별 |
|------|------|---------|
| `executorAllocationRatio` | **비율(%)** | ❌ 공통 (0.13) |
| `spark.executor.instances` / `initialExecutors` / `minExecutors` | **개수(대)** | ✅ 테이블별, 세 값 동일 |
| `maxExecutors` | 개수(대) | 공통 (36) |
| executor memory | 크기 | ✅ 테이블별 (16g·18g·20g) |

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
| 3번 | 38.4GiB | 58.0GiB | 1.51 |

> 1번 값은 섹션 5.1 표와 별도 회차의 DataFlint 지표(12대, input 82.9GiB / output 41.4GiB / shuffle 58.5GiB)다. 2번은 test1, 3번은 test2(섹션 5.3)의 값.

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

**4g 단위는 core 4개와 무관하다.** 16 → 20은 "25% 올려 보자"고 고른 폭일 뿐이며 18g든 22g든 된다. core 4개가 들어가는 자리는 위 식의 "÷ 4" 하나다.

**절차**: 새 테이블은 18g로 1회 돌려 spill이 나면 20g, 그래도 나면 24g(섹션 8.3). Spark UI task별 `Peak Execution Memory`를 받아 두면 실제 필요량이 바로 나와 이후 테이블은 계산으로 정할 수 있다.

### 8.3 판정 — 테이블 안에서는 dcu, 테이블 사이는 dcu/100만 row를 참고만

**판정 원칙: 같은 테이블 안에서 spill 0을 지키면서 dcu가 가장 낮은 설정을 고른다.** 대수는 ratio 0.13이 수렴하는 값(= `시간당 GB × 0.32`)이고, 메모리는 spill이 0이 되는 최소값이다.

**테이블 사이 비교는 참고 지표다.** hourly 4개는 같은 원천을 수직분할한 테이블이라 시간당 row 수가 같고 컬럼 폭만 다르다. dcu를 GB로 나누면 row가 좁은 테이블이 비효율적으로 **보이고**(2번 8대: dcu/GB는 1번 +35%, dcu/100만 row는 −9~14%), row로 나누면 그 착시는 사라진다. 그러나 row로 나눠도 **컬럼 타입이 다르면 row당 비용이 다르다** — 3번은 parquet 폭이 1번과 같은데 dcu/100만 row가 30% 높고, 그 원인(메모리 팽창·spill 성향)은 설정으로 바꿀 수 없다. 즉 dcu/100만 row가 1번과 다르다는 것만으로 "설정이 틀렸다"고 판정하지 않는다.

| 테이블 | 대수 | 메모리 | dcu/100만 row |
|--------|------|--------|---------------|
| 1번 | 12 | 16g | 0.0241 |
| 2번 | 8 | 20g | 0.0206~0.0219 |
| 3번 | 12 | 18g | 0.0295~0.0324 (20g 측정) |
| 4번 | 12 | 18g | 0.0314 (20g 측정) |

**다른 테이블 확인 항목** (테이블당 1~2회 실행)

| 확인 | 기준 | 어긋날 때 |
|------|------|----------|
| 시작 대수 | driver 로그 `Using initial executors = N`이 의도한 값인가 | `spark.executor.instances` 확인 (섹션 4.8) |
| 수렴 대수 | `시간당 데이터GB × 0.32`와 유사한가 | 입력 파일 크기 확인 (섹션 4.5) |
| `spill` | 0인가 | executor memory 16g → 20g → 24g (섹션 8.2) |
| `dcu` | 같은 테이블의 다른 설정보다 낮은가. 메모리를 올리면 +5~6%가 정상 | 대수 재검토 |
| duration | 2분 이내 (DAG 전체 12분 창) | — |
| 출력 파일 | avg 500MB대, **384MB 미만 파일 3개 미만** | `tuning/compaction-tuning-guide.md` §4.3 |

**새 hourly 테이블 절차** (4번까지 적용 완료): `instances` = init = min = `ceil(시간당 GB × 0.32)`, ratio 0.13, max 36. 4개 중 3개가 16g에서 spill이 났고 그 중 둘(3·4번)은 18g로 충분했으므로 **18g + `memoryOverhead` 3g로 시작**해 1회. spill 0이면 확정, 나면 20g.

### 8.4 `memoryOverhead` — 3g 확정 (실측 2026-09-17~18)

executor pod 하나의 메모리 한도 = `spark.executor.memory`(heap) + `memoryOverhead`. 3·4번이면 18g + 3g = **21g**다.

| | heap (`spark.executor.memory` 18g) | overhead (`memoryOverhead` 3g) |
|---|---|---|
| 무엇 | Spark가 **데이터 row를 올려놓고 정렬하는 공간** | Java 프로그램이 **데이터 말고 돌아가는 데 쓰는 메모리 전부** |
| 부족하면 | 디스크로 내려보낸다(**spill**). 느려지지만 죽지 않는다 | pod가 한도를 넘어 K8s가 executor를 죽인다(**OOMKilled**) |
| 지표 | DataFlint `spill` | DataFlint `task error rate`, driver 로그 `exit code 137` |

이 설정은 무언가를 할당하지 않는다. Spark가 heap에 더해 pod 한도로 제출하는 값일 뿐이고, 실제 사용량보다 작으면 pod가 죽고 크면 예약만 하고 안 쓴다. Spark 기본값은 heap의 10%(최소 384MB), 튜닝 전 설정은 4g였다.

**overhead가 실제로 쓰이는 곳 — 네 가지**

1. **executor 간 통신 버퍼 (가장 크고, 유일하게 변동하는 항목).** shuffle 단계에서 executor들이 서로 데이터 조각을 주고받는데, 네트워크로 보내고 받는 조각을 잠시 담아 두는 버퍼가 heap 바깥에 있다. 받는 쪽은 task 1개당 최대 48MB(`spark.reducer.maxSizeInFlight`)를 미리 당겨 오므로 task 4개면 약 200MB이고, 보내는 쪽은 **요청해 오는 executor 수만큼** 버퍼가 열린다 — 36대가 동시에 달려들면 한 executor에 보내기 버퍼가 몰린다. 2g에서 executor 유실이 난 시점이 shuffle read 중이었고 오류가 `MetadataFetchFailedException`·`internal_error_network`였던 것이 이 항목이 넘쳤다는 증거다
2. **Java 프로그램 자체가 돌아가는 데 필요한 메모리 (거의 고정).** 프로그램 코드를 올려 두는 공간(100~300MB), 스레드 하나마다 딸린 작은 작업 공간(100개 이상 × 약 1MB), 메모리 청소(GC)를 위한 장부(heap의 몇 %). **Spark 기본값 10%는 이 항목만 보고 잡은 값**이다
3. **압축·해제 작업 공간.** shuffle 조각은 디스크에 압축해서 쓰고 읽을 때 풀며, Parquet 파일을 쓸 때도 압축한다. 이 작업은 Java 바깥의 C 라이브러리가 하고 그 작업 공간이 overhead다. 하나하나는 작지만 task 4개 × 동시에 여는 파일 수만큼 열린다
4. **반대로, overhead에 절대 안 들어가는 것.** 데이터 row 자체, 정렬 중인 데이터, 디스크에 쓴 shuffle 파일. row는 heap, shuffle 파일은 디스크다. 이것이 **데이터 양이 늘어도 overhead가 안 늘어나는 이유**다 — 1은 "동시에 오가는 조각 수", 2·3은 "task 수·스레드 수"로 크기가 정해지고 셋 다 데이터 총량과 무관하다. 3번 22시간치 875GiB·36대에서도 3g로 task error 0(섹션 5.3)

**왜 10%(1.8g)가 아니라 3g인가 — 필요량이 heap 크기를 따라가지 않는다.** 10% 규칙은 항목 2(heap이 크면 GC 장부도 조금 커진다)에서 나온 경험칙이고, 공식 문서도 "executor 크기에 따라 커지는 경향, 보통 6~10%"라고 적는다. 우리 job은 그 위에 항목 1이 얹혀 있고, 그것은 heap과 무관하다. 실측이 그대로다 — heap을 20g에서 18g로 **줄였는데도** 2g에서 똑같이 executor를 잃었다:

| overhead | heap | 결과 |
|----------|------|------|
| 1g | 20g | job 실패 |
| 2g | **18g** (test5) | job 성공, executor 유실(task error 2.4%) |
| 2g | 20g (test6·7) | job 성공, executor 유실(task error 0.9%·3.1%) |
| 3g | 18g·20g | task error 0 |

즉 우리 job의 필요량은 heap과 무관한 **2g 조금 넘는 고정값**이고, 10% 규칙이 그 값을 덮으려면 heap이 30g는 돼야 한다. 2.5g는 재지 않았다 — 절감 0.5g × 12 = 6g이고 틀리면 운영에서 executor 유실이 난다.

**pod가 점유하는 메모리는 두 값의 합이다.** 3번 테이블은 20g + 4g일 때 executor당 24g × 12 = 288g이었고, 18g + 3g로 확정한 뒤에는 21g × 12 = **252g**다.

**실측 — 4번 테이블, 시행착오 9회**

| `memoryOverhead` | 실행 | 결과 |
|------------------|------|------|
| 1g | 1회 | executor pod 사망 → **job 실패** |
| 2g | 4회 (최초 1회 + test5·6·7, heap 18g/20g/20g) | job은 성공했으나 **task error rate 2.4% / 0.9% / 3.1%**(test5·6·7), failed stage 1~4건 (`MetadataFetchFailedException`, `internal_error_network`). 최초 1회는 task error를 확인하지 않았다 |
| **3g** | 2회 (test8·9) + 18g 검증 8회 (4번 test10~14, 3번 test7~9 — 최대 36대·22시간치) | **전부 task error 0%, failed stage 0** |
| 4g | 2회 (섹션 5.4 test1·2) | task error 0% |

`MetadataFetchFailedException`은 "shuffle 통을 가지러 갔는데 그 통을 들고 있던 executor가 사라졌다"는 뜻이고, `internal_error_network`는 죽은 executor와의 연결 단절이다. 즉 **2g에서는 시간대에 따라 executor 한두 대가 한도를 넘어 죽고, Spark이 앞 단계를 재실행해 job을 살린 것**이다. 숨은 비용은 재실행만큼의 duration·dcu이고, 진짜 위험은 재실행까지 실패하면 그 시간 Compaction 전체가 실패한다는 것(`partial-progress=false`). 그래서 **job 성공 여부가 아니라 task error rate 0%·executor 유실 0건이 판정 기준**이다. 처음에 "2g 성공"으로 판정했던 것은 job 성공만 보고 task 실패를 안 본 오판이었다(2026-09-18 정정). 확인은 driver 로그 또는 pod 상태다:

```
# driver 로그
ExecutorLostFailure (executor 7 exited caused by one of the running tasks)
Reason: The executor with id 7 exited with exit code 137
# kubectl describe pod <executor-pod>
Last State: Terminated   Reason: OOMKilled   Exit Code: 137
```

137 = 메모리 한도 초과로 강제 종료. heap 부족(spill)은 이 줄이 절대 안 나온다 — "spill은 느려지고, overhead 부족은 죽는다"로 둘을 가른다.

**메모리 지표로는 필요량을 못 잰다 — 두 가지 이유**

1. **Java 지표**(Spark UI Peak JVM Memory OffHeap 132~135MiB, `peakMemoryMetrics`)는 Java가 스스로 세는 값이라 netty direct 버퍼·네이티브 버퍼·page cache가 빠진다. 1g에서 죽는데 135MiB로 찍히는 이유
2. **커널 지표 `container_memory_max_usage_bytes`는 항상 pod 한도에 붙는다.** 4번 테이블 5회 전부 한도 + 4~7MiB였다(20g 한도 21,474,836,480 → 21,482,352,640, 22g → 23,629,045,760, 23g → 24,700,178,432). 커널이 shuffle 파일 4.7GB와 읽은 parquet를 남는 메모리에 page cache로 채워 두고 한도에 닿으면 비우기 때문이다. 이 지표는 "얼마나 필요했나"가 아니라 "한도가 얼마였나"를 보여 준다. 3g에서도 한도에 붙었지만 죽지 않은 것은 비울 수 있는 캐시가 충분했기 때문이고, 2g에서는 비울 수 없는 몫(netty·네이티브 버퍼, 아직 디스크에 안 쓰인 dirty page)이 한도를 넘는 순간이 있었던 것이다. `container_memory_rss`(page cache 제외) 최고값 − heap이 그나마 가깝지만 dirty page 몫은 역시 안 잡힌다

따라서 **값을 바꿔 돌려 보고 task error rate·executor 유실로 판정하는 시행착오가 유일한 실측 방법**이다. `partial-progress=false`라 운영 전 테스트에서만 한다. 컨테이너 이름은 executor `spark-kubernetes-executor`, driver `spark-kubernetes-driver`.

**4개 테이블 모두 3g를 명시한다.** 기본값(heap의 10%)은 20g면 2g, 18g면 1.8g, 16g면 1.6g로 전부 부족하다. 테이블별로 다 잴 필요는 없다 — non-heap 사용량은 row 모양이 아니라 shuffle 전송량·동시 task 수·로컬 shuffle 파일 크기에 좌우되고, 이것들은 4개 테이블에서 같다(executor당 shuffle 약 4.7GB, task 4개).

**고정값으로 두는 이유 — 동적 조정 불필요**

1. **필요량이 데이터 양을 따라 늘지 않는다.** executor 하나의 shuffle 몫은 ratio 산정 덕에 데이터가 늘어도 약 4.7GB로 일정하다(섹션 8.2). 동시 task 4개, 버퍼 크기(`spark.reducer.maxSizeInFlight` 등)도 설정값으로 고정이다. **실측: 3번 22시간치(875GiB, 36대)·4번 6시간치(232GiB, 33대)에서 3g로 task error 0**(섹션 5.3·5.4)
2. **바꿀 수단이 없다.** pod spec에 박히는 값이라 실행 중에는 못 바꾸고, DA가 executor를 더 부를 때도 같은 spec이다
3. **실패 비용이 크다.** 모자라면 executor가 죽고 재실행이 나며, 재실행도 실패하면 job이 실패한다. 이런 값은 여유를 둔 고정값이 맞고, 재검토는 섹션 9의 조건이 바뀔 때만 한다

**driver `memoryOverhead`**: `spark.driver.memoryOverhead`, 기본값 heap의 10%(최소 384MB) → driver 4g면 약 410MB, pod 4.4g. Compaction에서 driver는 파일 700개를 group 4개로 나누고, job 4개를 띄워 결과를 받고, commit하는 일만 한다 — shuffle 파일도 데이터도 없어 복도에 들어갈 것은 JVM 자체 몫 150MB 정도다. 줄여 봐야 pod 1개에서 200MB이고 실패하면 job 전체가 죽으므로 **기본값이 최선이다.**

**heap 18g 확정 (2026-09-21)**: test5(18g + 2g)에서 spill 0이었던 신호를 18g + 3g로 3번 3회(1·2·22시간치)·4번 5회(1~6시간치) 돌려 spill 0·task error 0을 확인했다(섹션 5.3·5.4). 3·4번은 21g pod로 확정 — 20g + 3g 대비 executor당 2g, 두 테이블 합계 48g 절감.

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

6. executor memoryOverhead 3g의 전제 변경
   → executor cores(4), target-file-size-bytes(512MB), shuffle 압축 codec,
     spark.reducer.maxSizeInFlight, Spark 버전 중 하나라도 바뀌면
     4번 테이블에서 1회 재실측(섹션 8.4). 데이터 양 증가만으로는 재검토 불필요.
```

---

## 10. 미확인 항목

| 항목 | 내용 |
|------|------|
| `maxExecutors` 확정값 | **36으로 고정 (2026-09-16).** K8S namespace quota는 확인할 수 없으나 리소스가 넉넉하고, 실사용량은 ratio가 통제하므로 천장은 넉넉히 둬도 무해하다(섹션 4.6) |
| ~~ratio 0.066에서의 요청 로그~~ | **해소 (2026-09-16).** 12대가 이미 떠 있는데 `desired total 5~6`이 찍힌 것은 `spark.executor.instances` 12가 시작 대수의 바닥이었기 때문이다(섹션 4.8). DA의 계산은 정확했고 내릴 수단이 없었을 뿐이다 |
| 메모리 97.62% | 7회 중 최고값이며 DataFlint가 `executor.memory` 19.2g를 권고한다. **spill이 0인 동안은 조치하지 않는다** — Spark의 정렬은 가용 메모리를 최대한 쓰다가 부족하면 디스크로 넘기므로, 90%대는 한계 임박이 아니라 정상 동작이다. 감시 기준은 `spill ≠ 0`. 18g에서도 92.6~97.8%(4번 test10~13)이고 DataFlint "Executor memory under-provisioned" alert가 그 사이(92.6~95.2%)부터 뜬다 — 같은 이유로 무시(섹션 5.4) |
| ~~다른 hourly 테이블~~ | **4개 전부 완료 (섹션 5.5)** |
| ~~`memoryOverhead` 필요량~~ | **3g 확정 (2026-09-18).** 4번에서 1g job 실패, 2g는 job 성공했으나 executor 유실(task error 1~3%), 3g 2회 깨끗. Java 지표(OffHeap 135MiB)도 커널 지표(`container_memory_max_usage_bytes`, 항상 한도에 붙음)도 필요량을 못 재므로 task error rate로 판정 (섹션 8.4) |
| executor 로컬 디스크 사용량 | hourly 예상 executor당 5GiB(권장 10GiB × 노드당 executor 수). 재처리 n시간치는 n × 5GiB와 동시 파티션 12개분(6~7GiB) 사이 — 22시간치가 현 디스크로 성공했으므로 용량은 충분. 운영 첫 실행에서 `spark-local-dir-1` 사용량 1회 확인(섹션 5.5) |
| ~~천장 아래 미도달 (31·33대)~~ | **해소 (2026-09-21).** DA는 동시에 도는 파티션(`max-concurrent-file-group-rewrites`개)의 일감만 보므로 대수는 총 데이터가 아니라 동시 파티션의 데이터 합 × 0.3이다. 2번 20시간치 23대도 같은 원리(섹션 5.5) |
| `spark.memory.fraction` 0.8 | 보류. 2번 16g 가능성(섹션 7). 시간 될 때 2~3회 |
| 3번 row당 비용이 높은 원인 | 메모리 팽창·spill 성향으로 보아 컬럼 타입 차이. 어느 컬럼인지는 미확인 — 설정과 무관하므로 우선순위 낮음 |
| 2번 테이블 spill의 압축 요인 | task당 row 수 차이는 확정, 컬럼 타입에 따른 압축 해제 팽창은 미확인(섹션 8.2). Spark UI `Peak Execution Memory`로 확인 가능 |
| ~~운영 적용 후 duration~~ | **해소.** 확정 설정(init 8)으로 돌린 test9가 1.7분 — warm-up이 빠져 test8(init 6) 1.9분보다 짧다. 예상과 일치 |

---

## 11. daily와의 분리

daily Compaction에 이 설계를 그대로 적용할 수 없다.

- ratio 0.13은 hourly의 파일 구성·계수(C=0.32) 기준이다
- daily는 30~60분 job이라 `executorIdleTimeout` 60초가 전체의 2~3%에 불과해 **반납이 실제로 일어날 수 있다.** 반납이 되면 판단 근거가 달라진다
- daily Compaction의 대상은 `day` 파티션 테이블들이며 크기·파일 구성이 공유되지 않아 아무것도 산정할 수 없다 (`compaction-tuning-guide.md` §8.1)

daily 튜닝 후 별도로 판단한다.
