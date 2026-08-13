# Compaction executor 동적 산정 설계

| 항목 | 내용 |
|------|------|
| 대상 | hourly Compaction DAG |
| 목적 | 데이터 증가 시 실행 창 제약을 넘지 않도록 `num-executors`를 데이터 양에 따라 산정 |
| 전제 | 튜닝 결과 확정 (`tuning/compaction-tuning-guide.md`), 계수 C=0.32 |
| 결론 | **현재 도입 불필요.** 데이터 1.8배 시점까지 정적 12개로 충분 (섹션 3) |

---

## 1. 배경

hourly Compaction은 매시 `:45`에 시작해 정각까지 종료해야 한다. 시작 분 M은 `M ≤ 60 − duration − 여유`로 정해지며 현재 `60 − 12 − 3 = 45`다 (`reprocessing-dag-design.md` §6.2).

정적 executor 수에서는 데이터가 늘면 duration이 비례해 늘고, 이 제약이 조용히 깨진다. 동적 산정은 증가분을 executor 수로 흡수해 duration을 일정하게 유지한다.

---

## 2. 산정 대상

당초 driver/executor의 cpu·memory·개수를 모두 동적화하려 했으나 **`num-executors` 하나로 축소된다.**

| 설정 | 판정 | 이유 |
|------|------|------|
| `num-executors` | 동적 | 데이터 양에 비례하는 유일한 값 |
| `executor cpu` / `memory` | 고정 | task 하나의 처리 단위가 512MB로 고정. 데이터가 늘면 task 수만 늘고 크기는 그대로 |
| `driver cpu` / `memory` | 고정 | file group 수(4개)에 비례하나 변동 폭이 작음 |
| `max-concurrent-file-group-rewrites` | 고정(크게) | group 수보다 크면 남는 값은 미사용. 동적화가 무의미 |
| `max-file-group-size-bytes` | 고정(크게) | 분할하지 않는 것이 목표 |

근거는 `tuning/compaction-tuning-guide.md` §6.1.

---

## 3. 도입 시점 판단

**정적 12개와 동적 산정의 스케줄 한계를 비교하면 현재는 도입 이득이 없다.**

고정 core에서는 duration이 데이터에 비례하고 초/GB는 일정하다. core가 데이터에 비례하면 duration이 일정하고 초/GB가 감소한다.

| 데이터 (테이블당) | 정적 12개 DAG 전체 | 동적 산정 DAG 전체 |
|------------------|------------------|------------------|
| 42.3GB (현재 최대) | 6.8분 | 6.0분 |
| 60GB | 9.6분 | 6.0분 |
| **74.7GB** | **12.0분 (창 초과)** | 6.0분 |
| 100GB | 16.1분 | 6.0분 (상한 도달) |
| 100GB 초과 | — | 정적과 동일 기울기 |

- 정적 12개의 창 초과 지점: **테이블당 74.7GB**
- 현재 최대 실측 42.3GB → **여유 1.77배**
- 동적 산정은 `MAX_EXECUTORS`(잠정 32) 도달 지점인 100GB까지 duration을 유지

**판단**

| 시점 | 조치 |
|------|------|
| 현재 | `com_num_executor`를 12로 고정. 동적 산정 미도입 |
| 테이블당 55~60GB 도달 | 도입. 창 여유가 2~3분으로 줄어드는 구간 |
| 테이블당 100GB 접근 | 동적 산정으로도 부족. 파티션 재설계 또는 K8S 슬롯 확대 |

데이터 증가 추이를 모니터링해 55GB 도달 전에 도입하면 된다. 현재 도입하면 산정값이 12~14개로 정적값과 거의 같아 Trino 연결 구현 공수만 발생한다.

**미측정 이득** — 데이터가 적은 시간대(새벽 등)의 리소스 절감. 예컨대 20GB 시간대는 산정값이 7개로 정적 12개보다 작다. 시간대별 데이터 양 편차를 측정하면 이 이득이 도입 시점을 앞당길 수 있다. 현재 측정 구간(07~15시)은 36~42GB로 편차가 작다.

---

## 4. 산정 위치

`compaction_dag_example.py`가 만드는 `compaction_specs` task 내부에서 산정한다.

**검토한 대안**

| 안 | 방식 | 판정 |
|----|------|------|
| **A** | `compaction_specs` 내부에서 테이블별 조회 | **채택** |
| B | 조회 전용 task 분리 → XCom 병합 | 미채택 |
| C | mapped task 실행 직전에 테이블별 조회 | 미채택 |

**A 채택 근거**

- `compaction_specs`가 이미 params를 읽고 테이블을 loop한다. task 추가가 불필요하다
- 테이블별 `try/except`로 감싸면 **한 테이블의 조회 실패가 다른 테이블에 전파되지 않는다.** B의 장점인 실패 격리가 A에서도 성립한다
- 산정값은 XCom을 거치므로 원시 타입이어야 한다. 기존 구조가 `"instances": str(...)`로 문자열을 담고 `.map()`에서 `DriverAndExecutor`를 만들므로 변경이 한 줄이다

**B 미채택** — task를 분리하면 `compaction_specs`가 XCom 2개를 병합해야 한다. mapped task 구조상 `expand_kwargs`에 넘길 list[dict]를 한 곳에서 만드는 것이 단순하다. 재시도 단위 분리 이득은 fallback이 있어 실질적이지 않다.

**C 미채택** — operator 인자는 `expand_kwargs` 시점에 정해져야 하므로, 실행 직전 조회를 하려면 operator를 감싸는 task가 추가로 필요하다. 대상이 이미 닫힌 과거 1시간치라 조회 시점을 늦춰 얻는 정확도 이득이 작다 (섹션 6).

---

## 5. 입력 조회 경로

Trino JDBC로 Iceberg `.partitions` 메타데이터를 조회한다.

**검토한 대안**

| 경로 | 판정 | 이유 |
|------|------|------|
| **Trino JDBC** | **채택** | Airflow provider 존재. pod 기동 없음 |
| Spark job | 미채택 | pod 기동 20~30초. 산정 목적에 과함 |
| HMS 직접 조회 | 미채택 | manifest를 직접 파싱해야 함. 구현 비용 큼 |
| append DAG이 크기 기록 | 미채택 | DAG 간 결합 증가. avro → Parquet 크기 변환 계수 필요 |
| 직전 회차 값 캐싱 | 미채택 | 실측 크기를 Spark pod에서 Airflow로 되돌리는 배관이 조회보다 복잡 |

**`.files`가 아니라 `.partitions`를 쓴다**

| 조회 | 반환 행 수 (보관 30일 가정) | 행 하나 크기 |
|------|------------------------|------------|
| `.files` (필터 없음) | 약 54,000 | 컬럼 19개 통계 전부 포함 |
| `.files` (파티션 필터) | 약 75 | 동일 |
| `.partitions` (파티션 필터) | 4 | 집계값만 |

**부하 특성** — 비용은 행 수가 아니라 manifest 수가 지배한다. append 5분 주기로 288 commit/일이 발생하고 `rewrite_manifests` 3일 주기 사이에 수백~1,000개가 누적된다. 파티션 필터로 manifest pruning이 걸리면 비용이 거의 늘지 않는다. 걸리지 않는 최악의 경우 30~90MB 읽기로 수 초다.

`hour(ts)` 파티션은 시간순이고 `rewrite_manifests`가 파티션 기준으로 정리하므로 pruning에 유리하다. **다만 metadata table에서 실제로 걸리는지는 미확인이다** (섹션 12).

---

## 6. 조회 시점과 정확도

`compaction_specs`는 DAG run 시작 시 1회 실행되고, Spark job은 테이블 순차 실행이므로 마지막 테이블은 약 5분 뒤에 시작한다. 그 사이에도 append는 5분 주기로 커밋한다.

**정확도 영향은 무시할 수 있다.**

- 대상은 직전 1시간치로 이미 닫힌 구간이다
- 그 구간에 추가로 들어오는 것은 지연 적재분뿐이며 GB 단위 미만이다
- 산정식이 `ceil(크기 × 0.32)`이므로 executor 1개가 바뀌려면 3.1GB 변동이 필요하다

---

## 7. 실패 모드와 대응

조회는 외부 의존성이므로 **어떤 실패에도 Compaction 자체는 실행되어야 한다.** 기존 `com_num_executor` 상수를 지우지 않고 fallback으로 유지하는 이유다.

| 실패 모드 | 증상 | 대응 |
|----------|------|------|
| Trino 장애 / 연결 실패 | 예외 발생 | 정적값 fallback + warning 로그 |
| 파티션 조건 불일치 | 조회 성공, 크기 0 | 정상 범위 검사로 차단 후 fallback |
| 조회 결과 이상값 | 비정상적으로 큰 값 | 정상 범위(0.1~500GB) 검사 후 fallback |
| 산정값이 상한 초과 | clamp 발생 | 상한 적용 + warning 로그 (섹션 8) |
| Trino 응답 지연 | DAG 시작 지연 | 조회 timeout 설정 필요 (미정) |

**크기 0 검사가 필요한 이유** — 조회가 성공했는데 파티션 조건이 틀려 0이 반환되면 executor가 `MIN_EXECUTORS`로 떨어져 Job이 한없이 느려진다. 예외가 나지 않으므로 fallback 경로를 타지 않는다.

---

## 8. 상한의 의미

`MAX_EXECUTORS`는 성능 상한이자 K8S 자원 상한이다.

- append 벤치마크에서 32개 이상은 오히려 느려진다 (shuffle 통신, pod 스케줄링 경합, S3 부하 — `tuning/spark-tuning-guide.md` §2.2.3)
- append가 batch당 약 10 executor를 5분 주기로 상시 점유하므로 그만큼을 남겨야 한다
- **K8S에 여유가 없으면 executor를 늘려도 pod Pending으로 duration이 오히려 늘어난다.** 동적 산정이 목적을 달성하지 못하는 구간이다

**상한에 걸리는 것은 조치 신호다.** 데이터가 설계 범위를 넘었다는 뜻이며 파티션 재설계나 슬롯 확대를 검토해야 한다. 조용히 clamp하지 않고 warning 로그를 남긴다.

값은 K8S namespace quota 확인 후 확정한다 (잠정 32).

---

## 9. 재처리 DAG과의 상호작용

재처리 DAG이 Compaction DAG을 trigger할 때 `start_time`/`end_time`이 여러 시간에 걸친다 (`reprocessing-dag-design.md` §6.3).

**조회는 단일 시간이 아니라 범위여야 한다.** 단일 시간으로 조회하면 여러 시간 범위에서 크기를 과소 산정해 executor가 부족해진다.

```
WHERE partition.ts_hour >= <from>
  AND partition.ts_hour <  <until>
```

**파티션 값 변환은 naive datetime으로 계산한다.** `ts`가 `timestamp_ntz`이므로 timezone을 붙여 `timestamp()`를 쓰면 Iceberg 저장값과 어긋나 엉뚱한 시간대를 조회한다.

```
hour = int((dt − 1970-01-01).total_seconds() // 3600)
```

검증: `2026-08-11 13:00` → `496237`. Spark UI 출력 `PartitionData{ts_hour=496237, col_a=D}`와 일치한다.

---

## 10. daily와의 분리

**`C=0.32`을 daily에 그대로 쓸 수 없다.**

- 계수는 hourly 측정값이다
- daily는 `rewrite-all` 낭비 의심이 남아 있다 — hourly가 정리한 뒤라 할 일이 거의 없어야 하는데 888GB에 30~60분이 걸리고 소요시간이 데이터 양에 비례한다 (`tuning/compaction-tuning-guide.md` §8.1)
- 그 확인이 끝나면 처리량 특성이 달라지므로 계수를 새로 측정해야 한다

산정 코드는 공유하되 계수와 상한은 hourly/daily를 분리한다.

---

## 11. 적용 순서

| 순서 | 항목 | 상태 |
|------|------|------|
| 1 | Compaction DAG의 `tables` params + mapped task 전환 | 재처리 DAG 배포 전 적용 예정 (`reprocessing-dag-design.md` §6.1) |
| 2 | `com_num_executor`를 12로 변경 | 즉시 적용 가능 |
| 3 | K8S quota 확인 → `MAX_EXECUTORS` 확정 | 대기 |
| 4 | Trino `$partitions` 컬럼·타입 확인, manifest pruning 측정 | 대기 |
| 5 | 동적 산정 도입 | **데이터 55~60GB 도달 시** (섹션 3) |

**1번이 선행 조건이다.** 동적 산정은 `compaction_specs` 안에 들어가므로 mapped task 전환이 먼저 필요하다.

2번만으로 튜닝 결과의 리소스 절감(dcu −47%)을 확보한다. 5번은 데이터 증가 대응이다.

---

## 12. 도입 전 확인 항목

| 항목 | 확인 방법 |
|------|----------|
| Trino `$partitions`의 `partition.ts_hour` 타입 | `SELECT * FROM "<schema>.<table>$partitions" LIMIT 5` |
| manifest pruning 동작 여부 | 전체 조회와 파티션 필터 조회의 Physical input 비교 |
| 조회 소요시간 | 실측. 수 초를 넘으면 DAG 시작 지연을 고려해 timeout 설정 |
| Trino connection id, Iceberg schema 이름 | 기존 설정 확인 |
| 시간대별 데이터 양 편차 | 새벽 시간대 크기 측정. 편차가 크면 도입 시점을 앞당길 근거가 된다 |

Spark SQL로 조회할 경우 컬럼명이 `total_size` 대신 `total_data_file_size_in_bytes`다.

---

## 13. 구현

| 파일 | 내용 |
|------|------|
| `pipeline/examples/compaction_executor_sizing_example.py` | 구현 스켈레톤. `to_partition_hour()`, `query_size_bytes()`, `num_executors_for()`와 `compaction_specs` 연결 지점 |
| `pipeline/examples/compaction_dag_example.py` | 선행 조건인 mapped task 전환 예시 |
| `tuning/compaction-tuning-guide.md` §6 | 계수 근거, 측정값 |

배포용 파일이 아니며 기존 hourly Compaction DAG에 반영한다. 신규 구현이 필요한 부분은 Trino 연결부뿐이고 `TODO(연결)`로 표시되어 있다.
