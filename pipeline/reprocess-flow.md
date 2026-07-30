# 재처리 DAG 처리 흐름

| 항목 | 내용 |
|------|------|
| DAG | `iceberg_reprocess` |
| 주기 | 매일 01:00 KST, 동시 실행 1개 |
| 목적 | append DAG 조회 범위에서 밀려난 데이터 회수 |

append DAG은 최근 1일치만 조회하므로, 그 범위를 벗어난 `WAIT_SCHEDULING`과 실패로 확정된 `FAILURE`가 계속 남는다. 이 DAG이 하루 한 번 훑어 회수한다.

---

## 처리 흐름

```
[독립]  check_zombie_jobs
        2시간 넘게 IN_PROGRESS인 건을 알림. 자동 복구는 하지 않는다.
        본류와 의존이 없어 알림 실패가 회수 작업을 막지 않는다.


  1  prepare_run — 조회 범위 확정
  │    FAILURE          그저께 00:00 ~ 전날 끝
  │    WAIT_SCHEDULING  전날 01:00 이전만 (이후는 append 담당)
  │    수동 실행 시 테이블·시간 직접 지정
  │
  2  테이블별 처리 — 순차, 테이블 20개+
  │    기존 append 처리 묶음을 그대로 재사용한다.
  │    Spark이 동시에 뜨지 않도록 테이블을 하나씩 처리한다.
  │
  │    ┌─────────────────────────────────────────────────────┐
  │    │  get_jobs         대상 조회 후 선점                   │
  │    │                   DB 2개, 테이블당 1,000건            │
  │    │                   이미 적재된 건은 제외               │
  │    │      ↓                                              │
  │    │  append_data      Spark으로 Iceberg 적재              │
  │    │      ↓                                              │
  │    │  update_success   결과에 따라 상태 확정               │
  │    │  update_failure   실패 건은 다음날 다시 대상이 된다    │
  │    └─────────────────────────────────────────────────────┘
  │
  3  마무리
       compaction_targets  →  적재한 시간 범위만 기존 Compaction DAG 실행
       next_loop           →  남은 대상이 있으면 재실행 (최대 10회)
```

---

## 상태 흐름

```
WAIT_SCHEDULING  ┐                    ┌→  SUCCESS
                 ├→  IN_PROGRESS  →  ┤
FAILURE          ┘                    └→  FAILURE  (다음날 재시도)
```

회수 대상은 `WAIT_SCHEDULING`과 `FAILURE` 두 가지다. `WAIT_SCHEDULING`은 append가 아직 조회하는 구간을 제외하고, `FAILURE`는 append가 조회하지 않으므로 전 구간을 잡는다.

---

## 중복 적재 방지

Spark이 적재를 마쳤으나 직후 통신 오류 등으로 Airflow만 실패로 판정하는 경우가 있다. 이를 그대로 재적재하면 같은 데이터가 두 번 들어간다.

적재 시 `batch_id`를 Iceberg 스냅샷에 함께 기록해 두고, 재처리는 대상을 집기 전에 이 값의 존재를 확인한다. 있으면 적재하지 않고 `SUCCESS`로 정정한다. 상태값과 무관하게 이 기록만을 판단 기준으로 삼는다.
