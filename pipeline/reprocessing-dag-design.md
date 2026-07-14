# 재처리(Reprocessing) DAG 설계 가이드

## 문서 정보

| 항목 | 내용 |
|------|------|
| 작성 목적 | append DAG 조회 기간(최근 1일)에서 밀려난 WAIT 데이터와 FAILED 데이터를 회수하는 재처리 DAG 설계 |
| 대상 독자 | 데이터 엔지니어, 운영팀 |
| 환경 | Kubernetes 클러스터, S3(MinIO), Spark 4.1.1, Iceberg 1.10.1, Airflow 3.1.7, Oracle DB |
| 시간대 기준 | **KST (Asia/Seoul)** — 모든 날짜/시간 계산에 적용 |
| 최종 수정일 | 2026-07-14 |

### 목차

- [1. 배경 및 문제 정의](#1-배경-및-문제-정의)
- [2. 아키텍처: 역할 분담 구조](#2-아키텍처-역할-분담-구조)
- [3. Job History 상태 모델](#3-job-history-상태-모델)
- [4. 중복 적재 방지: batch_id 영수증 방식](#4-중복-적재-방지-batch_id-영수증-방식)
- [5. 재처리 DAG 상세 설계](#5-재처리-dag-상세-설계)
- [6. Compaction 연계](#6-compaction-연계)
- [7. append DAG 변경 사항](#7-append-dag-변경-사항)
- [8. 모니터링 및 수동 처리 절차](#8-모니터링-및-수동-처리-절차)
- [9. 운영 전제 조건 체크리스트](#9-운영-전제-조건-체크리스트)
- [10. DAG 샘플 코드](#10-dag-샘플-코드)

---

## 1. 배경 및 문제 정의

### 1.1 현재 구조와 제약

append DAG(10분 주기)의 `get_jobs` task는 Oracle Job History 테이블에서 처리 대상을 조회한다. Job History 테이블은 **날짜 키로 파티셔닝**되어 있어 Partition Pruning을 위해 조회 기간을 **최근 1일**로 제한한다. 조회 기간을 늘리면 조회 성능이 크게 저하되므로 이 제약은 유지해야 한다.

### 1.2 문제: 영구 잔류 데이터

이 구조에서 다음 두 종류의 데이터가 영구히 처리되지 않고 남을 수 있다.

| 케이스 | 발생 경로 |
|--------|----------|
| **WAIT 잔류** | append 처리 또는 DB 조회 지연으로 밀린 데이터가 최근 1일 조회 기간을 벗어남 → `get_jobs`가 다시는 조회하지 않음 |
| **FAILED 잔류** | `get_jobs`는 WAIT만 조회하므로, `update_failure`가 FAILED로 확정한 데이터는 재시도 주체가 없음 |

### 1.3 설계 방향

- append DAG의 조회 기간(최근 1일)을 건드리지 않는다 — Partition Pruning 유지, 최신 데이터 freshness 유지
- 오래된 데이터를 처리하느라 최신 데이터가 밀리는 상황(예: 현재 13일 15시인데 10일 15시 데이터를 처리)을 만들지 않는다
- 잔류 데이터 회수는 **별도의 재처리 DAG(1일 주기)** 이 전담한다
- 자동 재처리 범위는 **최근 2일(D-1~D-2)** 로 제한하고, 그 이상 밀린 데이터는 알림 후 **수동 처리**한다 (매일 모니터링 운영 전제)

---

## 2. 아키텍처: 역할 분담 구조

```
┌─────────────────────────────────────────────────────────────┐
│ append DAG (10분 주기) — 신규 데이터 전담                      │
│   조회: 최근 1일 파티션, status = 'WAIT'                      │
│   재시도 없음. 실패 건은 FAILED 확정 후 재처리 DAG에 위임        │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│ 재처리 DAG (1일 주기, 00:00 KST) — 잔류 데이터 회수 전담        │
│   조회: D-1 파티션의 FAILED + D-2 파티션의 WAIT/FAILED         │
│   처리 후 기존 Compaction DAG trigger                         │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│ 수동 처리 (매일 모니터링) — 비정상 상황 전담                    │
│   D-3 이상 잔류 건: 알림 수신 → 원인 확인 → UI에서             │
│   재처리 DAG을 target_dt 지정 수동 실행                        │
└─────────────────────────────────────────────────────────────┘
```

### 2.1 조회 범위 경계 (겹침 방지)

**D-N 표기**: 재처리 DAG 실행일 기준의 파티션 날짜. 오늘이 14일이면 D-1 = 13일(어제) 파티션, D-2 = 12일(그저께) 파티션.

**전제**: append DAG의 최근 1일 조회는 "실행 시각부터 24시간 전까지"이다. 14일 10시에 실행되면 13일 10시~14일 10시를 조회한다. 즉 어제 파티션의 데이터는 오늘 중까지도 계속 append DAG의 조회 대상이다.

재처리 DAG이 도는 14일 0시 기준으로 파티션별 담당을 정하면:

| 파티션 | 상태 | 담당 | 이유 |
|--------|------|------|------|
| 13일 (D-1) | WAIT | append DAG | append가 아직 보고 있음 (14일 중까지 조회 대상). 재처리가 집으면 같은 row를 두 DAG이 잡음 |
| 13일 (D-1) | FAILED | **재처리 DAG** | append는 FAILED를 애초에 조회하지 않음 |
| 12일 (D-2) | WAIT | **재처리 DAG** | append의 24시간 조회 범위를 완전히 벗어남 — 다시는 조회되지 않음 |
| 12일 (D-2) | FAILED | **재처리 DAG** | 어제 재처리가 실패했거나, 재처리 후 또 실패한 건 |
| 14일 (D-0) | — | 조회 안 함 | 0시에는 오늘 파티션이 방금 시작되어 비어 있음. 오늘 발생할 FAILED는 내일 0시에 D-1로 회수됨 |
| 11일 이전 (D-3~) | WAIT/FAILED | 수동 | **알림 → 수동 처리** (섹션 8) |

정리하면 재처리 DAG의 조회 대상은 **어제 파티션의 FAILED + 그저께 파티션의 WAIT/FAILED** 두 가지다.

> **경합 방어선**: 만약의 경합에 대비해 IN_PROGRESS 전환 UPDATE는 반드시 `WHERE status = 'WAIT'`(또는 `'FAILED'`) 조건을 포함한 원자적 전환으로 수행한다. 두 DAG이 동시에 시도해도 한쪽만 성공한다.

**WAIT가 재처리로 회수되기까지의 흐름** — 13일 08:00에 생성된 WAIT row가 계속 처리되지 못하는 경우:

```
13일 08:00   생성. append DAG이 매 10분 조회 (24시간 동안)
14일 00:00   재처리 실행 → 13일 WAIT는 아직 append 담당이므로 제외
14일 08:00   생성 후 24시간 경과 → append 조회 범위에서 이탈
15일 00:00   재처리 실행 → 13일은 이제 그저께(D-2) → WAIT 회수 ✅
```

생성부터 재처리 회수까지 최대 약 2일 걸리지만 영구 누락 경로는 없다. 이것이 재처리 lookback을 2일로 잡은 이유다.

### 2.2 검토했으나 채택하지 않은 대안

| 대안 | 미채택 사유 |
|------|------------|
| append DAG 조회를 상태 기준(시간 조건 제거)으로 변경 | 날짜 파티셔닝된 Job History에서 전체 파티션 스캔 발생 → 조회 성능 저하 |
| append DAG 조회 기간 확장 (1일 → 7일) | 조회 성능 저하 + 밀린 과거 데이터가 최신 데이터 처리를 지연시킴 |
| 재처리 DAG에서 chunk 단위 loop 처리 (7일치) | 부분 실패 시 상태 update 누락 위험, 구조 복잡. 2일 초과 잔류는 원인 파악이 필요한 비정상 상황이므로 수동 처리가 운영상 적합 |
| 재처리 DAG에서 직접 `rewrite_data_files` 실행 | 기존 Compaction DAG과 동시 실행 시 Iceberg commit 충돌. 기존 DAG trigger 방식으로 `max_active_runs=1` 직렬화 활용 (섹션 6) |

---

## 3. Job History 상태 모델

### 3.1 상태 전이도

```
WAIT ──get_jobs(append DAG)──▶ IN_PROGRESS ──▶ update_success ──▶ DONE
                                    │
                                    └──▶ update_failure ──▶ FAILED
                                                              │
WAIT(D-2 잔류) ──┐                                            │
                 ├──get_reprocess_jobs(재처리 DAG)◀───────────┘
FAILED(D-1~D-2)──┘        │
                          ├──(영수증 확인: 이미 커밋됨)──▶ DONE 정정
                          └──▶ IN_PROGRESS ──▶ DONE / FAILED(다음날 재시도)
```

### 3.2 상태별 처리 주체

| 상태 | 생성 주체 | 소비 주체 |
|------|----------|----------|
| WAIT | 원천 시스템 | append DAG (최근 1일) / 재처리 DAG (D-2) / 수동 (D-3 이상) |
| IN_PROGRESS | get_jobs / get_reprocess_jobs | update_success/update_failure. 임계 시간 초과 시 좀비 탐지 알림 (섹션 8.2) |
| DONE | update_success, 영수증 정정 | 최종 상태 |
| FAILED | update_failure | 재처리 DAG (D-1~D-2) / 수동 (D-3 이상) |

### 3.3 재시도 정책

- **task 레벨 재시도**: Spark task의 Airflow `retries`(권장 2회, `retry_delay` 5분)가 일시적 오류(S3 순단 등)를 1차 방어
- **배치 레벨 재시도**: task 재시도 소진 후 FAILED 확정 → 다음날 00:00 재처리 DAG이 재시도. 별도 retry 카운트는 DB에 저장하지 않음
- **무한 재시도 방지**: 재처리에서도 반복 실패하는 건은 D-3 이상 잔류 알림(섹션 8.1)으로 사람에게 노출됨

> **FAILED 격리 효과**: append DAG이 FAILED를 재조회하지 않으므로, 깨진 파일이 섞인 배치가 10분마다 반복 실패하며 정상 신규 데이터까지 물고 늘어지는 상황이 구조적으로 발생하지 않는다.

---

## 4. 중복 적재 방지: batch_id 영수증 방식

### 4.1 왜 필요한가

Airflow의 "task 실패" 판정이 항상 "데이터 미적재"를 의미하지 않는다. Iceberg commit은 성공했으나 직후 Driver Pod 종료 오류, Operator-Pod 통신 단절, task timeout 등으로 Airflow가 실패로 판정하는 경우(**거짓 실패**)가 있다. 이 상태에서 FAILED 건을 기계적으로 재적재하면 **같은 데이터가 두 번 들어간다.**

```
13일 09:00  Spark job이 Iceberg commit 성공 (데이터 적재됨)
            → 직후 Pod 통신 오류 → Airflow는 실패 판정 → FAILED 기록
14일 00:00  재처리 DAG이 FAILED를 재적재 → 중복!
```

### 4.2 동작 방식

Iceberg는 append 커밋마다 snapshot을 생성하고, snapshot summary(key-value 메타데이터)에 커스텀 값을 심을 수 있다. 여기에 배치 식별자를 기록해 "영수증"으로 사용한다. Iceberg 공식 WAP(Write-Audit-Publish)의 `wap.id`, Kafka Connect Iceberg Sink의 offset 기록과 동일한 확립된 패턴이다.

```
[적재 시 — 영수증 찍기]
① 배치 식별자 생성: Airflow run_id 사용
   → IN_PROGRESS 마킹 UPDATE 시 stat_desc 컬럼에 batch_id 함께 기록
② Spark append 시 write option 추가:
   df.writeTo("db.TABLE_A")
     .option("snapshot-property.batch_id", batch_id)
     .append()
   → commit 성공 시 snapshot summary에 batch_id가 남음

[재처리 시 — 영수증 확인]
③ FAILED row를 집기 전, row의 stat_desc(batch_id)로 확인:
   SELECT snapshot_id FROM db.TABLE_A.snapshots
    WHERE summary['batch_id'] = :batch_id
   → 결과 있음: 이미 커밋됨 → 재적재하지 않고 DONE으로 정정
   → 결과 없음: 진짜 실패 → 정상 재적재
```

### 4.3 구현 비용 및 성능

| 항목 | 내용 |
|------|------|
| Oracle 변경 | 없음 (기존 `stat_desc` CLOB 컬럼 재사용 — 과거 Airflow log URL 용도, 현재 미사용). Oracle에서 batch_id로 검색할 일이 없으므로 CLOB의 인덱스/등호 비교 제약이 걸리지 않음 |
| Spark 변경 | write option 1줄 |
| `.snapshots` 조회 부하 | 없음. snapshot 목록은 metadata.json 파일 1개에 포함 — S3 GET 1회, manifest/데이터 파일 접근 없음. 3일 보존 기준 snapshot ~500개, 실행 빈도는 하루 1회(FAILED 존재 시에만) |

### 4.4 제약: snapshot 보존 기간

- snapshot 보존 정책: **3일** (`expire_snapshots`)
- 자동 재처리 lookback(2일) < 보존(3일) → 자동 경로에서는 영수증 확인이 항상 가능
- **3일을 넘긴 FAILED 건은 영수증이 expire되어 확인 불가** → 수동 처리 시 별도 검증 필요 (섹션 8.3)
- snapshot 보존 정책을 단축할 경우 반드시 `보존 기간 > 재처리 lookback` 유지

---

## 5. 재처리 DAG 상세 설계

### 5.1 DAG 기본 설정

| 항목 | 값 | 근거 |
|------|-----|------|
| schedule | `0 0 * * *` (KST) | 일일 Compaction(00:35) 이전 완료 목표. 시간당 Compaction(00:15)과의 간격은 실측 후 조정 |
| timezone | `Asia/Seoul` (pendulum) | logical_date UTC 혼선 차단. D-N 계산 전부 KST 기준 |
| max_active_runs | 1 | 중복 실행 방지 |
| catchup | False | 과거 스케줄 재실행 불필요 (수동 실행은 params로) |
| params | `target_dt` (기본값 없음) | UI 수동 실행 시 특정 날짜 지정 (섹션 8.3) |

### 5.2 Task 구성

```
check_zombie_jobs                    # 좀비 IN_PROGRESS 탐지 → 알림 (처리 차단 안 함)
      │
get_reprocess_jobs                   # 대상 조회 + 영수증 확인 + 상한 적용 + IN_PROGRESS 마킹
      │                              # 대상 0건이면 후속 전부 skip (ShortCircuit)
append_data                          # SparkKubernetesOperator (append DAG의 Spark job 재사용)
      │
      ├── update_success  [trigger_rule=all_success]   # DONE 처리
      └── update_failure  [trigger_rule=all_failed]    # FAILED 처리 (다음날 재시도)
      │
trigger_compaction                   # update_success 하류. 기존 Compaction DAG trigger (섹션 6)
```

> Spark job이 **1개**이므로 append DAG과 동일한 update_success/update_failure + trigger_rule 구조를 그대로 사용한다. chunk 분할(Spark job 여러 개) 구조는 일부 성공/일부 실패 시 `all_success`/`all_failed`가 모두 불충족되어 상태 update가 누락되는 문제가 있어 채택하지 않았다.

### 5.3 get_reprocess_jobs 처리 순서

1. **대상 조회** — 날짜 파티션별 등호 조회로 Partition Pruning 유지 (범위 조회 금지):
   - D-1 파티션: `status = 'FAILED'`
   - D-2 파티션: `status IN ('WAIT', 'FAILED')`
   - 수동 실행(`target_dt` 지정) 시: 해당 파티션의 `WAIT` + `FAILED`
2. **영수증 확인** — FAILED 건을 batch_id(stat_desc)별로 그룹핑 → `.snapshots` 조회 → 이미 커밋된 batch의 row는 DONE 정정 후 대상에서 제외
3. **크기 상한 적용** — 대상 총 avro 크기가 상한(16GB)을 초과하면 초과분을 잘라내고 알림 발송. 잘린 건은 상태 유지 → 다음날 재시도 또는 수동 처리
4. **IN_PROGRESS 마킹** — 원자적 UPDATE (`WHERE status = ...` 조건 포함) + `stat_desc = 새 batch_id(run_id)` 기록
5. **Spark 입력 준비** — avro 경로 목록 S3 업로드, num_executors 산정 (append DAG과 동일 산정식: `ceil(총크기/128MB × 1.5 / 4)`)

### 5.4 크기 상한과 리소스

| 항목 | 값 | 근거 |
|------|-----|------|
| 1회 처리 총 크기 상한 | 16GB | 벤치마크 검증 범위(~8GB, num-executors 24) 기준 2배 이내. 초과는 비정상 상황 → 알림 후 수동 판단 |
| num_executors 상한 | 24 | 벤치마크에서 32 이상은 성능 저하 확인 (spark-tuning-guide.md 2.2.3) |

> **K8S 리소스 경합 주의**: 재처리 Spark job(최대 24 executor, 96 core, ~213GB)이 도는 동안에도 10분 주기 append job(동일 규모)이 뜬다. 동시 실행 시 최대 **~192 core, ~427GB**가 필요하다. 클러스터 여유가 부족하면 재처리 job의 executor 상한을 낮춘다(예: 12 — 지연 데이터이므로 처리 속도의 우선순위가 낮음).

---

## 6. Compaction 연계

### 6.1 방식: 기존 Compaction DAG trigger

재처리 DAG은 `rewrite_data_files`를 직접 실행하지 않고 **기존 Compaction DAG을 trigger**한다.

| 이점 | 설명 |
|------|------|
| 동시 실행 방지 | 기존 Compaction DAG의 `max_active_runs=1`이 trigger run과 스케줄 run을 자동 직렬화 → 같은 테이블에 rewrite 2개가 동시 실행되어 Iceberg commit 충돌하는 상황 차단 |
| 로직 단일화 | rewrite 옵션, 대상 테이블, 실패 알림이 기존 DAG 한 곳에 유지 |
| 검증된 경로 | UI 수동 실행용 params(`target_dt` / `start_time`,`end_time`)를 그대로 사용 — `TriggerDagRunOperator`의 `conf`가 선언된 params를 덮어쓰므로 수동 실행과 동일 경로 |

### 6.2 왜 필요한가

Compaction은 시간당(직전 1시간치)·일일(전일치) 범위로 동작하므로, 재처리가 적재하는 과거 `hour(ts)` 파티션은 **이미 Compaction이 지나간 구간**이다. trigger 없이는 재처리분 small file이 과거 파티션에 영구히 남는다.

### 6.3 trigger 구성

```python
# daily 그룹 테이블 (params: target_dt)
TriggerDagRunOperator(
    trigger_dag_id="daily_compaction_dag",
    conf={"target_dt": "2026-07-12"},          # 재처리가 적재한 날짜별 1회씩
    wait_for_completion=False,
)

# hourly 그룹 테이블 (params: start_time, end_time)
TriggerDagRunOperator(
    trigger_dag_id="hourly_compaction_dag",
    conf={"start_time": "2026-07-12 00:00:00",  # 적재 데이터의 ts 최소~최대 범위 (KST)
          "end_time": "2026-07-13 00:00:00"},
    wait_for_completion=False,
)
```

- **날짜별 개별 trigger**: 여러 날짜에 걸쳐 적재한 경우 날짜당 1회씩 trigger — `max_active_runs=1`이 순차 실행 보장
- **D-1도 trigger 대상에 포함**: "재처리가 00:35 전에 끝나면 스케줄 run이 커버"라는 가정에 의존하지 않는다. 같은 날짜를 두 번 Compaction해도 두 번째는 합칠 파일이 없어 사실상 no-op이라 무해
- **wait_for_completion=False**: Compaction 실패 알림은 Compaction DAG이 담당. 대기하면 `max_active_runs=1` 대기열 때문에 재처리 DAG 실행 시간만 늘어남
- **날짜/시간 형식**: 기존 Compaction DAG의 UI params 형식과 동일하게 맞출 것 (구현 시 확인)

---

## 7. append DAG 변경 사항

재처리 DAG 도입에 따른 append DAG 변경은 **batch_id 기록 2건**뿐이다.

| 위치 | 변경 |
|------|------|
| `get_jobs` | IN_PROGRESS 마킹 UPDATE에 `stat_desc = :batch_id` 추가 (batch_id = Airflow run_id) |
| Spark job | `option("snapshot-property.batch_id", batch_id)` 추가 |

이미 적용된 사항 (변경 불필요):

- `get_jobs`는 `WAIT`만 조회 (FAILED 재시도는 재처리 DAG 전담)
- DB 상태 처리는 callback이 아닌 `update_success`(`all_success`) / `update_failure`(`all_failed`) task — callback 방식은 DB update 지연 시 작업이 kill되는 문제가 있었음. task 방식은 Airflow의 retries/로그/알림 관리 대상이 됨

---

## 8. 모니터링 및 수동 처리 절차

### 8.1 잔류 데이터 알림 (일 1회)

| 대상 | 조건 | 대응 |
|------|------|------|
| 자동 재처리 범위 초과 | D-3 ~ D-7 파티션에 `WAIT` 또는 `FAILED` 존재 | 원인 확인 → 재처리 DAG 수동 실행 (8.3) |
| 크기 상한 초과 | 재처리 대상 총 크기 > 16GB | append DAG 장애 여부 확인 → 수동 판단 |

> 잔류 알림 쿼리도 날짜 파티션별 등호 조회로 수행한다. 잔류 건이 매일 꾸준히 증가한다면 스케줄링 문제가 아니라 **처리량 부족(capacity)** 신호이므로 리소스 증설/주기 조정을 검토한다.

### 8.2 좀비 IN_PROGRESS 탐지

`get_jobs`가 IN_PROGRESS로 전환한 후 DAG run이 증발하면(scheduler 장애, worker 강제 종료 — update task 2개 모두 미실행) 해당 row는 어느 DAG도 집지 않는다.

- 재처리 DAG 선행 task(`check_zombie_jobs`)가 임계 시간(2시간, 정상 처리 ~1분 대비 충분한 여유) 초과 IN_PROGRESS를 탐지해 **알림만** 발송한다
- 자동 복구는 하지 않는다 — 판정은 사람이 영수증 확인으로 수행:
  - snapshot에 해당 batch_id **있음** → 데이터 적재 완료 → DONE으로 수동 정정
  - **없음** → 미적재 → WAIT로 수동 복구 (다음 주기에 자동 처리됨)

### 8.3 수동 재처리 절차 (D-3 이상 잔류 건)

1. 알림 수신 → 원인 확인 (append DAG 장애 이력, 깨진 파일 여부 등)
2. Airflow UI에서 재처리 DAG을 `target_dt` 지정 후 수동 실행 (Compaction DAG 수동 실행과 동일한 패턴)
3. 실행이 완료되면 Compaction trigger까지 자동으로 이어짐
4. **3일 초과 건 주의**: snapshot 보존(3일)을 넘긴 FAILED는 영수증 확인이 불가능하다. 재적재 전 중복 여부를 별도 검증할 것 — 예: 해당 ts 범위의 Iceberg row count와 원본(avro) 건수 대조

---

## 9. 운영 전제 조건 체크리스트

| # | 항목 | 기준 |
|---|------|------|
| 1 | Oracle Job History 파티션 보존 기간 | ≥ 7일 (잔류 알림 조회 범위) |
| 2 | Iceberg snapshot 보존 기간 | 3일. **항상 재처리 lookback(2일)보다 길게 유지** |
| 3 | K8S 클러스터 여유 용량 | append + 재처리 동시 실행 시 최대 ~192 core / ~427GB. 부족 시 재처리 executor 상한 하향 |
| 4 | Compaction DAG params 형식 | daily: `target_dt`, hourly: `start_time`/`end_time` — conf 전달 형식 일치 확인 |
| 5 | Spark task retries | `retries=2`, `retry_delay=5분` 권장 (일시적 오류 1차 방어) |
| 6 | 시간대 | 모든 DAG `Asia/Seoul` timezone 명시. 파티션 키 날짜 계산 KST 기준 |
| 7 | stat_desc 컬럼 | batch_id 용도로 전환됨을 팀 내 공유 (과거: Airflow log URL) |

---

## 10. DAG 샘플 코드

> 실제 구현 시 커넥션/알림/Spark 템플릿은 기존 append DAG 것을 재사용한다. 아래는 구조와 핵심 로직 중심의 샘플이다.

```python
import math
import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

KST = pendulum.timezone("Asia/Seoul")

SIZE_LIMIT_MB = 16 * 1024        # 1회 처리 총 크기 상한 (16GB)
MAX_EXECUTORS = 24               # 벤치마크 검증 상한
ZOMBIE_THRESHOLD_HOURS = 2       # 좀비 IN_PROGRESS 판정 임계
ALERT_LOOKBACK_DAYS = 7          # 잔류 알림 조회 범위


@dag(
    dag_id="iceberg_reprocess",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 7, 1, tz=KST),
    catchup=False,
    max_active_runs=1,
    params={"target_dt": None},   # UI 수동 실행: 특정 날짜 지정 (YYYYMMDD)
)
def iceberg_reprocess():

    @task
    def check_zombie_jobs():
        """임계 시간 초과 IN_PROGRESS 탐지 → 알림만 (자동 복구 안 함, 섹션 8.2)"""
        zombies = oracle.fetch("""
            SELECT job_id, stat_desc, updated_at
              FROM JOB_HISTORY
             WHERE status = 'IN_PROGRESS'
               AND updated_at < SYSTIMESTAMP - INTERVAL '2' HOUR
        """)
        if zombies:
            send_alert(f"좀비 IN_PROGRESS {len(zombies)}건 — 영수증 확인 후 수동 판정 필요", zombies)

    @task.short_circuit
    def get_reprocess_jobs(**context):
        run_id = context["run_id"]
        target_dt = context["params"].get("target_dt")
        today = pendulum.now(KST).date()

        # 1) 대상 조회 — 날짜 파티션 등호 조회 (Partition Pruning 유지)
        if target_dt:
            # 수동 실행: 지정 날짜의 WAIT + FAILED
            targets = [(target_dt, ("WAIT", "FAILED"))]
        else:
            targets = [
                (fmt(today.subtract(days=1)),      ("FAILED",)),           # D-1 (WAIT는 append DAG 담당)
                (fmt(today.subtract(days=2)),      ("WAIT", "FAILED")),    # D-2
            ]
        jobs = []
        for part_dt, statuses in targets:
            jobs += oracle.fetch("""
                SELECT job_id, status, stat_desc, avro_path, file_size_mb
                  FROM JOB_HISTORY
                 WHERE part_dt = :part_dt AND status IN :statuses
            """, part_dt=part_dt, statuses=statuses)

        # 2) 영수증 확인 — 거짓 실패(커밋됐는데 FAILED) 건은 DONE 정정 후 제외 (섹션 4)
        failed_batches = {j["stat_desc"] for j in jobs if j["status"] == "FAILED" and j["stat_desc"]}
        committed = {
            b for b in failed_batches
            if spark_sql(f"SELECT 1 FROM db.TABLE_A.snapshots WHERE summary['batch_id'] = '{b}'")
        }
        if committed:
            mark_done_by_batch(committed)   # 재적재 없이 DONE 정정
            jobs = [j for j in jobs if j["stat_desc"] not in committed]

        # 3) 크기 상한 — 초과분은 잘라내고 알림 (상태 유지 → 다음날/수동)
        jobs.sort(key=lambda j: j["job_id"])
        picked, total_mb = [], 0
        for j in jobs:
            if total_mb + j["file_size_mb"] > SIZE_LIMIT_MB:
                send_alert(f"재처리 크기 상한 초과 — {len(jobs) - len(picked)}건 이월", jobs)
                break
            picked.append(j)
            total_mb += j["file_size_mb"]
        if not picked:
            return False   # ShortCircuit: 대상 없음 → 후속 task 전부 skip

        # 4) 원자적 IN_PROGRESS 전환 + batch_id(run_id) 기록
        oracle.execute("""
            UPDATE JOB_HISTORY
               SET status = 'IN_PROGRESS', stat_desc = :batch_id
             WHERE job_id IN :ids AND status IN ('WAIT', 'FAILED')
        """, batch_id=run_id, ids=[j["job_id"] for j in picked])

        # 5) Spark 입력 준비 — append DAG과 동일 (경로 목록 S3 업로드, executor 산정식 재사용)
        upload_path_list_to_s3(picked, run_id)
        num_executors = min(
            max(math.ceil(total_mb / 128 * 1.5 / 4), 1),
            MAX_EXECUTORS,
        )
        context["ti"].xcom_push(key="meta", value={
            "batch_id": run_id,
            "num_executors": num_executors,
            "part_dts": sorted({j["part_dt"] for j in picked}),
        })
        return True

    # append DAG과 동일한 Spark job 재사용.
    # Spark 코드에는 .option("snapshot-property.batch_id", batch_id) 적용 (섹션 4.2)
    append_data = SparkKubernetesOperator(
        task_id="append_data",
        retries=2,                              # 일시적 오류 1차 방어
        retry_delay=pendulum.duration(minutes=5),
        # ... application_file 등 기존 append DAG 설정 재사용
    )

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def update_success(**context):
        mark_done_by_batch({context["ti"].xcom_pull(key="meta")["batch_id"]})

    @task(trigger_rule=TriggerRule.ALL_FAILED)
    def update_failure(**context):
        oracle.execute("""
            UPDATE JOB_HISTORY SET status = 'FAILED'
             WHERE stat_desc = :batch_id AND status = 'IN_PROGRESS'
        """, batch_id=context["ti"].xcom_pull(key="meta")["batch_id"])

    @task
    def trigger_compaction(**context):
        """적재한 날짜별로 기존 Compaction DAG trigger (섹션 6.3)
        daily 그룹: conf={"target_dt": ...} / hourly 그룹: conf={"start_time": ..., "end_time": ...}
        max_active_runs=1이 스케줄 run과의 동시 실행을 직렬화한다."""
        for part_dt in context["ti"].xcom_pull(key="meta")["part_dts"]:
            trigger_dag_run("daily_compaction_dag", conf={"target_dt": part_dt})

    picked = get_reprocess_jobs()
    check_zombie_jobs() >> picked >> append_data
    append_data >> [update_success(), update_failure()]
    update_success() >> trigger_compaction()


iceberg_reprocess()
```

### 잔류 데이터 알림 쿼리 (별도 모니터링, 섹션 8.1)

```sql
-- D-3 ~ D-7 파티션별 등호 조회 (loop)
SELECT part_dt, status, COUNT(*) AS cnt, SUM(file_size_mb) AS total_mb
  FROM JOB_HISTORY
 WHERE part_dt = :part_dt              -- D-3 ... D-7 각각
   AND status IN ('WAIT', 'FAILED')
 GROUP BY part_dt, status;
-- 결과 존재 시: 알림 → 수동 재처리 절차(8.3)
```
