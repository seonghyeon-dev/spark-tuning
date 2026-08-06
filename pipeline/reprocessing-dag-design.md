# 재처리(Reprocessing) DAG 설계 가이드

## 문서 정보

| 항목 | 내용 |
|------|------|
| 작성 목적 | append DAG 조회 기간(최근 1일)에서 밀려난 WAIT_SCHEDULING 데이터와 FAILURE 데이터를 회수하는 재처리 DAG 설계 |
| 대상 독자 | 데이터 엔지니어, 운영팀 |
| 환경 | Kubernetes 클러스터, S3(MinIO), Spark 4.1.1, Iceberg 1.10.1(**카탈로그: HMS**), Airflow 3.2.2, Oracle DB |
| 시간대 기준 | **KST (Asia/Seoul)** — 모든 날짜/시간 계산에 적용 |
| 최종 수정일 | 2026-07-27 |

### 목차

- [1. 시스템 구조 및 문제 정의](#1-시스템-구조-및-문제-정의)
- [2. 아키텍처: 역할 분담과 조회 범위 경계](#2-아키텍처-역할-분담과-조회-범위-경계)
- [3. Job History 상태 모델](#3-job-history-상태-모델)
- [4. 중복 적재 방지: batch_id 영수증 방식](#4-중복-적재-방지-batch_id-영수증-방식)
- [5. 재처리 DAG 상세 설계](#5-재처리-dag-상세-설계)
- [6. Compaction 연계](#6-compaction-연계)
- [7. 기존 DAG 변경 사항](#7-기존-dag-변경-사항)
- [8. 모니터링 및 수동 처리 절차](#8-모니터링-및-수동-처리-절차)
- [9. 운영 전제 조건 체크리스트](#9-운영-전제-조건-체크리스트)
- [10. DAG 구현 파일](#10-dag-구현-파일)

---

## 1. 시스템 구조 및 문제 정의

### 1.1 현재 시스템 구조

| 구성 요소 | 내용 |
|----------|------|
| Iceberg 테이블 | **20개 이상**. 첫 파티션 기준 hourly 그룹(`hour` hidden partition)과 daily 그룹(`day` hidden partition)으로 분류 |
| Job History (Oracle) | 처리 대상 상태 관리 테이블. **Oracle DB 2개(a/b)에 동일 스키마로 존재** — append DAG은 conn_list(conn_id 2개) loop로 같은 쿼리를 DB별로 실행. **키는 복합키 4개**(예: `k_1`, `k_2`, `k_3`, `ts`) — `ts`(string, `YYYYMMDDHHmmSSsss` 밀리세컨즈)도 그중 하나이며 날짜 파티셔닝 키이자 조회 기준이다. 그 외 컬럼: `table_name`(대상 테이블), `base_path`, `param`(JSON — `{"files": [{"file_path", "size"}, ...]}`, row당 파일 여러 개 가능), `status`, `stat_desc`(CLOB, 현재 미사용). 복합키 값은 DB 간 유일 보장 없음 |
| append DAG | py 파일 1개에서 loop로 **테이블별 DAG 동적 생성** (테이블당 1개 실행). 약 5분 주기. `get_jobs`가 conn_list의 **DB별로** `table_name` 조건 + `ts` 최근 1일 범위 + `status='WAIT_SCHEDULING'`을 `ORDER BY ts ASC`, `ROWNUM <= 200`으로 조회 |
| Compaction DAG | **hourly DAG 1개**(`15 * * * *`, 직전 1시간치) + **daily DAG 1개**(현재 `35 0 * * *`, 전일치). 각 DAG 내부에서 소속 테이블 task가 순차 실행. `max_active_runs=1`. UI 수동 실행용 params: daily는 `target_dt`, hourly는 `start_time`/`end_time` |
| DB 상태 처리 | callback이 아닌 `update_success`(`trigger_rule=all_success`) / `update_failure`(`all_failed`) task 방식 — callback은 DB update 지연 시 작업이 kill되는 문제가 있었음 |

### 1.2 문제: 영구 잔류 데이터

append DAG의 조회 기간은 **실행 시각 기준 최근 1일**(rolling)이다. Job History가 `ts` 기반 날짜 키로 파티셔닝되어 있어 조회 기간을 늘리면 성능이 크게 저하되므로 이 제약은 유지해야 한다. 이 구조에서 두 종류의 데이터가 영구히 처리되지 않고 남는다.

| 케이스 | 발생 경로 |
|--------|----------|
| **WAIT_SCHEDULING 잔류** | append 처리 또는 DB 조회 지연으로 밀린 데이터가 최근 1일 조회 범위를 벗어남 → `get_jobs`가 다시는 조회하지 않음 |
| **FAILURE 잔류** | `get_jobs`는 WAIT만 조회하므로, `update_failure`가 FAILED로 확정한 데이터는 재시도 주체가 없음 |

### 1.3 설계 방향

- append DAG의 조회 기간(최근 1일)과 `ORDER BY ts ASC` 순차 처리를 건드리지 않는다 — Partition Pruning과 최신 데이터 freshness 유지
- 잔류 데이터 회수는 **재처리 DAG 1개**(1일 주기, 01:00 KST)가 전담한다. 테이블별 task로 구성 (Compaction DAG과 동일 패턴)
- append DAG과 재처리 DAG의 조회 범위를 **애초에 겹치지 않게 설계**해서 같은 row를 두 DAG이 집는 경합을 원천 차단한다 (잠금·선점 로직 불필요)
- 자동 재처리 범위는 **전날 + 그저께**로 제한하고, 그 이상 밀린 데이터는 알림 후 **수동 처리**한다 (매일 모니터링 운영 전제)

---

## 2. 아키텍처: 역할 분담과 조회 범위 경계

```
┌──────────────────────────────────────────────────────────────┐
│ append DAG × 테이블 수 (약 5분 주기) — 신규 데이터 전담         │
│   조회: ts ≥ 실행시각-24h, status='WAIT_SCHEDULING', DB당 ROWNUM ≤ 200   │
│   재시도 없음. 실패 건은 FAILURE 확정 후 재처리 DAG에 위임        │
└──────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│ 재처리 DAG 1개 (1일 주기, 01:00 KST) — 잔류 데이터 회수 전담    │
│   테이블별 task 순차 실행 (Compaction DAG과 동일 패턴)          │
│   조회: FAILURE(전날+그저께) + WAIT_SCHEDULING(append 범위 밖만), DB 2개    │
│   테이블당·DB당 ROWNUM ≤ 1,000. 초과 시 자기 재trigger(loop)   │
│   처리 후 기존 Compaction DAG trigger                          │
└──────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│ 수동 처리 (매일 모니터링) — 비정상 상황 전담                     │
│   그저께 이전 잔류·loop 상한 초과: 알림 → 원인 확인 →            │
│   UI에서 재처리 DAG을 테이블·시간 범위 지정 수동 실행            │
└──────────────────────────────────────────────────────────────┘
```

### 2.1 조회 범위 경계 — 겹침이 원천적으로 불가능한 이유

**핵심 규칙**: append는 항상 "실행 시각-24시간 이후"만 조회한다. 재처리의 WAIT_SCHEDULING 조회 상한을 **전날 01:00**(= 재처리 스케줄 시각 - 24시간)으로 잡으면, 재처리가 01:00 이후에 도는 한 append의 조회 하한은 항상 전날 01:00 이상이므로 **두 범위는 절대 겹치지 않는다.** FAILED는 append가 아예 조회하지 않으므로 겹침 걱정 없이 전날·그저께 전체를 잡는다.

**오늘 = 4일** (전날 = 3일, 그저께 = 2일)로 두고, 오늘 4일 01:00 실행 기준 (`ts`는 `YYYYMMDDHHmmSSsss` 문자열 비교):

| 대상 | ts 범위 | 이유 |
|------|---------|------|
| FAILURE | `20260702000000000` ≤ ts < `20260704000000000` (그저께 00:00 ~ 전날 끝) | append는 FAILED를 조회하지 않으므로 전 구간 안전 |
| WAIT_SCHEDULING | `20260702000000000` ≤ ts < `20260703010000000` (그저께 00:00 ~ 전날 01:00) | 전날 01:00 이후는 append가 아직 조회 가능한 영역 |
| 그저께 이전 | 조회 안 함 | **알림 → 수동 처리** (섹션 8) |

> **경계값은 실행 시각이 아니라 그 날의 01:00 고정값으로 계산한다.** 재실행이나 loop 회차가 늦게 돌아도 경계가 append 조회 하한보다 항상 과거이므로 안전이 유지된다.
>
> **그저께까지 조회하는 이유(안전망)**: 재처리가 하룻밤 통째로 실패하거나 조회 상한으로 이월이 생겨도, 다음날 실행이 그저께 범위로 자동 회수한다. 이틀 연속 실패부터 수동 영역이다.
>
> 겹침이 없다는 것이 이 설계의 유일한 경합 방지 수단이다. 선점·잠금 로직은 두지 않는다.

**잔류 데이터 회수 타임라인**:

```
[FAILURE] 전날(3일) 15:00 배치 실패 → 오늘(4일) 01:00 재처리가 회수 (최대 하루 지연)

[WAIT_SCHEDULING]   전날(3일) 08:00 생성 후 계속 미처리
         4일 01:00  재처리: WAIT_SCHEDULING 상한이 3일 01:00 → 대상 아님 (아직 append 담당)
         4일 08:00  생성 24시간 경과 → append 조회 범위 이탈
         다음날(5일) 01:00  재처리: WAIT_SCHEDULING 범위 [3일 00:00 ~ 4일 01:00) → 회수 ✅ (최대 약 이틀 지연)
```

append가 `ORDER BY ts ASC`(오래된 것부터)로 소화하므로, 조회 범위 안의 WAIT가 하루 종일 안 집히는 상황 자체가 append 처리량 이상 신호다 — 이 경우는 잔류 알림(섹션 8.1)으로 드러난다.

### 2.2 Iceberg 동시 append — 커밋 레벨에서 안전한 이유

2.1이 막는 것은 **같은 row를 두 DAG이 집는 것**이다. 그와 별개로, 두 DAG의 Spark job이
**같은 테이블에 동시에 append 커밋**을 시도하는 상황은 실제로 발생한다 (재처리는 01:00에
수 분간 돌고, append는 그 동안에도 약 5분 주기로 뜬다). Airflow `max_active_runs`는
DAG 단위 설정이라 서로 다른 DAG 사이에는 아무 제약이 되지 않는다.

**결론: 데이터 유실·중복 없이 둘 다 반영된다.** 근거는 아래와 같다.

**테이블의 현재 상태는 HMS의 포인터 하나가 결정한다**

```
HMS:  TABLE_A  →  s3://.../metadata/00042-....metadata.json
```

S3에 데이터 파일이 있어도 이 포인터가 가리키는 메타데이터에 없으면 테이블의 일부가 아니다.
따라서 append job은 다음 3단계로 동작하며, 테이블에 반영되는 시점은 ③뿐이다.

```
① 읽기   HMS에서 현재 포인터를 읽는다 → metadata_42
② 쓰기   S3에 데이터 파일을 쓴다        ← 오래 걸리지만 테이블에는 영향 없음
③ 커밋   HMS에 "42면 43으로 바꿔줘" 요청 (compare-and-swap)
```

**동시 실행 시 타임라인**

```
시각   append job (A)                   재처리 job (R)
──────────────────────────────────────────────────────────────
 t0    HMS 읽기 → 42                    HMS 읽기 → 42
 t1    S3에 a1 쓰기                      S3에 r1 쓰기
       └─ 파일명이 UUID 기반이라 겹치지 않음. 이 단계에는 충돌 자체가 없다
 t2    커밋 "42→43"  HMS: 승인 ✅
 t3                                     커밋 "42→43"  HMS: 현재 43 → 거절 ❌
 t4                                     재시도: HMS 재조회 → 43, "43→44" 승인 ✅
```

최종 결과는 `metadata_44 = 원본 + a1 + r1`으로 **둘 다 반영된다.**

**재시도가 안전한 이유 (세 가지)**

| 근거 | 내용 |
|------|------|
| 데이터를 다시 쓰지 않는다 | t1에서 쓴 `r1`은 S3에 그대로 있다. 재시도는 **메타데이터에 r1을 추가하는 작업만** 반복한다 |
| append는 base와 무관하게 유효하다 | "r1을 추가한다"는 요청은 base가 42든 43이든 의미가 같다. 새 base 위에 그대로 다시 얹으면 된다 |
| 커밋 실패 = 아무 일도 없음 | HMS가 거절하면 포인터는 그대로다. 절반만 반영되는 중간 상태가 존재하지 않는다 |

> **Compaction은 이 성질이 없다.** `rewrite_data_files`는 "이 파일들을 지우고 저 파일로 교체"라
> 지우려던 파일이 이미 사라졌을 수 있어 검증이 필요하고, 그래서 서로 충돌한다. 재처리가
> rewrite를 직접 실행하지 않고 기존 Compaction DAG을 trigger하는 이유가 이것이다 (섹션 6).

**HMS가 보장하는 것 — 이 설계의 전제**

HMS는 자기 RDB 트랜잭션 안에서 테이블 파라미터를 읽고·비교하고·쓴다. 요청에 담긴 "내가 본 값"과
현재 값이 다르면 예외를 던지므로, **두 요청이 동시에 와도 하나는 반드시 지고 진 쪽은 그 사실을
안다.** 둘 다 성공했다고 믿는 상황이 생기지 않는다.

이 보장은 카탈로그 구현에 달려 있다. `HadoopCatalog`(파일시스템 기반)는 원자적 rename에
의존하는데 **S3에는 원자적 rename이 없어 동시 커밋에서 스냅샷이 유실될 수 있다.**
현재 구성(HMS)에서는 해당되지 않으나, 카탈로그를 바꾸면 이 전제가 깨진다 (섹션 9-9).

**job 실행 시간은 위험도와 무관하다**

충돌 구간은 ③ 커밋 순간(초 단위)이지 job 길이가 아니다. 재처리가 데이터 쓰기에 10분을 쓰고
그동안 append가 여러 번 커밋해도, 재처리는 마지막에 **한 번 거절당하고 최신 base를 다시 읽어
얹으면 끝난다** — 재시도 시 항상 그 시점의 최신 상태를 읽기 때문이다.

**재시도가 소진되는 경우**

`commit.retry.num-retries`(기본 4회)를 다 쓰면 `CommitFailedException`으로 job이 실패한다.
쓰기 주체가 2개이고 커밋이 초 단위라 실제로는 거의 발생하지 않으며, 발생하더라도
**job 실패 → `FAILURE` 기록 → 다음날 재처리가 회수 → 영수증 확인으로 중복 방지** 경로로
흡수된다. 별도 대응이 필요 없다.

### 2.3 검토했으나 채택하지 않은 대안

| 대안 | 미채택 사유 |
|------|------------|
| append DAG 조회를 상태 기준(시간 조건 제거)으로 변경 | `ts` 날짜 키 파티셔닝에서 전체 파티션 스캔 발생 → 조회 성능 저하 |
| append DAG 조회 기간 확장 (1일 → 7일) | 조회 성능 저하 + 밀린 과거 데이터가 최신 데이터 처리를 지연시킴 |
| 재처리 DAG을 테이블별로 동적 생성 (append 패턴) | 재처리는 하루 1회 청소 배치로 대부분 테이블이 no-op — DAG 20개+가 매일 빈 run을 쌓는 관리 소음. 단일 DAG + 테이블별 task(Compaction 패턴)가 관리에 유리하고, Compaction trigger도 한곳에서 날짜별로 묶어 1회씩 실행 가능 |
| 전날 WAIT_SCHEDULING 전체를 재처리가 가져가기 (UPDATE 선점 또는 Airflow pool로 경합 제어) | UPDATE 선점 후 재조회는 stat_desc(CLOB) 조건이 필요해 성능 문제. pool은 테이블 20개+ 구조에서 테이블별 pool 난립 또는 전역 병목. 조회 범위를 겹치지 않게 하는 것이 가장 단순 (전날 01:00 이후 WAIT는 다음날 회수 — 하루 지연 허용) |
| 한 DAG run 안에서 Spark task 여러 개로 분할 처리 | 일부 성공/일부 실패 시 `all_success`/`all_failed`가 모두 불충족되어 상태 update 누락. loop는 DAG 재trigger 방식으로 해결 (섹션 5.5) |
| 재처리 DAG에서 직접 `rewrite_data_files` 실행 | 기존 Compaction DAG과 동시 실행 시 Iceberg commit 충돌. 기존 DAG trigger로 `max_active_runs=1` 직렬화 활용 (섹션 6) |

---

## 3. Job History 상태 모델

### 3.1 상태 전이도

```
WAIT_SCHEDULING ──get_jobs(append DAG)──▶ IN_PROGRESS ──▶ update_success ──▶ SUCCESS
                                    │
                                    └──▶ update_failure ──▶ FAILURE
                                                              │
WAIT_SCHEDULING(append 범위 이탈분) ──┐                                   │
                          ├──재처리 DAG(전날+그저께)◀──────────┘
FAILURE ───────────────────┘        │
                                   ├──(영수증 확인: 이미 커밋됨)──▶ SUCCESS 정정
                                   └──▶ IN_PROGRESS ──▶ SUCCESS / FAILURE(다음날 재시도)
```

### 3.2 상태별 처리 주체

| 상태 | 생성 주체 | 소비 주체 |
|------|----------|----------|
| WAIT_SCHEDULING | 원천 시스템 | append DAG (최근 1일) / 재처리 DAG (append 범위 이탈분) / 수동 (그저께 이전) |
| IN_PROGRESS | get_jobs / 재처리 조회 task | update_success/update_failure. 임계 시간 초과 시 좀비 탐지 알림 (섹션 8.2) |
| SUCCESS | update_success, 영수증 정정 | 최종 상태 |
| FAILURE | update_failure | 재처리 DAG (전날+그저께) / 수동 (그저께 이전) |

### 3.3 재시도 정책

- **task 레벨 재시도**: Spark task의 Airflow `retries`(권장 2회, `retry_delay` 5분)가 일시적 오류(S3 순단 등)를 1차 방어
- **배치 레벨 재시도**: task 재시도 소진 후 FAILURE 확정 → 다음날 01:00 재처리 DAG이 재시도. 별도 retry 카운트는 DB에 저장하지 않음 (재시도는 task가, 배치 재시도는 상태 재조회가 담당)
- **무한 재시도 방지**: 재처리에서도 반복 실패하는 건은 그저께 이전 잔류 알림(섹션 8.1)으로 사람에게 노출됨

> **FAILURE 격리 효과**: append DAG이 FAILED를 재조회하지 않으므로, 깨진 파일이 섞인 배치가 5분마다 반복 실패하며 정상 신규 데이터까지 물고 늘어지는 상황이 구조적으로 발생하지 않는다.

---

## 4. 중복 적재 방지: batch_id 영수증 방식

### 4.1 왜 필요한가

Airflow의 "task 실패" 판정이 항상 "데이터 미적재"를 의미하지 않는다. Iceberg commit은 성공했으나 직후 Driver Pod 종료 오류, Operator-Pod 통신 단절, task timeout 등으로 Airflow가 실패로 판정하는 경우(**거짓 실패**)가 있다. 이 상태에서 FAILURE 건을 기계적으로 재적재하면 **같은 데이터가 두 번 들어간다.**

```
전날(3일) 09:00  Spark job이 Iceberg commit 성공 (데이터 적재됨)
            → 직후 Pod 통신 오류 → Airflow는 실패 판정 → FAILURE 기록
오늘(4일) 01:00  재처리 DAG이 FAILED를 재적재 → 중복!
```

### 4.2 동작 방식

Iceberg는 append 커밋마다 snapshot을 생성하고, snapshot summary(key-value 메타데이터)에 커스텀 값을 심을 수 있다. 여기에 배치 식별자를 기록해 "영수증"으로 사용한다. Iceberg 공식 WAP(Write-Audit-Publish)의 `wap.id`, Kafka Connect Iceberg Sink의 offset 기록과 동일한 확립된 패턴이다.

```
[적재 시 — 영수증 찍기]
① 배치 식별자 생성: Airflow run_id (+ 테이블명) 사용
   → IN_PROGRESS 마킹 UPDATE 시 stat_desc 컬럼에 batch_id 함께 기록
② Spark append 시 write option 추가:
   df.writeTo("db.TABLE_X")
     .option("snapshot-property.batch_id", batch_id)
     .append()
   → commit 성공 시 해당 테이블 snapshot summary에 batch_id가 남음

[재처리 시 — 영수증 확인]
③ 조회한 row를 집기 전, row에서 읽어온 stat_desc 값(batch_id)으로 확인:
   SELECT element_at(summary, 'batch_id')
     FROM db.TABLE_X.snapshots
    WHERE element_at(summary, 'batch_id') IN (:batch_ids)   -- 대조 대상만
   → 결과에 있음: 이미 커밋됨 → 재적재하지 않고 DONE으로 정정
   → 없음:        진짜 미적재 → 정상 재적재
```

> batch_id를 **한 번에 대조한다** — 건별로 질의하면 조회 row 수만큼 질의가 늘어난다.
> 반대로 `WHERE` 없이 snapshot 전체를 긁어오는 것도 안 된다. 보존 기간(3일) 안의
> 모든 batch_id가 반환되며, 이는 대조에 필요한 양보다 훨씬 크다 (append 약 5분 주기
> → 테이블당 수백~수천 snapshot). 넘긴 `batch_ids`의 부분집합만 받아야 한다.

> **영수증의 수명**: 마킹 시 한 번 기록하고 그 뒤로는 지우지 않는다. 상태가
> `IN_PROGRESS` → `SUCCESS`/`FAILURE`로 바뀌어도 `stat_desc`는 그대로 두어야 한다
> (섹션 5.3-6). 재처리가 다시 집을 때 판단 근거가 되는 값이므로, 상태 갱신 과정에서
> 지워지면 중복 적재 방지가 작동하지 않는다.
>
> **확인 대상은 status와 무관하게 batch_id를 가진 row 전부다.** `status`는 커밋
> 여부의 증거가 아니다. Airflow가 실패로 판정해 FAILED가 된 경우뿐 아니라,
> **Spark 커밋은 성공했는데 그 뒤 상태 갱신이 실패해 row가 WAIT로 남는 경우**도
> 있다. 판단 기준은 오직 영수증이며, snapshot에 batch_id가 있으면 status가
> 무엇이든 그 데이터는 이미 Iceberg에 있다 — 재적재하면 중복이다.

> **stat_desc 사용 제약 (중요)**: stat_desc는 CLOB이므로 **Oracle WHERE 조건으로 사용하는 것은 금지**한다 (등호 비교·인덱스 불가). 허용되는 사용은 두 가지뿐이다 — ① UPDATE 시 값 기록 ② SELECT 결과에서 개별 row의 값 읽기. 읽을 때는 **`DBMS_LOB.SUBSTR(stat_desc, 4000, 1)`로 VARCHAR2 변환해서 조회**해야 한다 — 그냥 조회하면 드라이버가 LOB 객체를 돌려주어 문자열 비교·set 연산이 되지 않고 영수증 확인이 오작동한다. 영수증 확인은 "row에서 batch_id를 읽어 → Iceberg `.snapshots`를 조회"하는 방향이므로 이 제약에 걸리지 않는다.
>
> **판단 근거와 전제**: Iceberg commit은 원자적(all-or-nothing)이므로 snapshot에 batch_id가 존재한다 = 그 배치의 커밋이 완전히 성공했다는 뜻이며, "일부만 적재된" 중간 상태는 존재하지 않는다. 단 이 판단은 **배치 전체가 Spark job 1회의 단일 append 커밋**일 때만 성립한다 (현재 append job 구조는 충족 — job 내부 다중 커밋 구조로 변경 시 성립하지 않음).
>
> **task retry 중복 방어 (권장)**: Spark task의 Airflow `retries`는 같은 batch_id로 재실행된다. attempt 1이 커밋 성공 후 거짓 실패하면 attempt 2가 같은 데이터를 중복 append할 수 있다. 방어책으로 **Spark job 시작 시 자기 batch_id의 snapshot 존재를 확인하고, 있으면 즉시 성공 종료**하는 로직을 권장한다 (조회 1회 비용).

### 4.3 구현 비용 및 성능

| 항목 | 내용 |
|------|------|
| Oracle 변경 | 없음 (기존 `stat_desc` CLOB 컬럼 재사용 — 과거 Airflow log URL 용도, 현재 미사용) |
| Spark 변경 | write option 1줄 |
| `.snapshots` 조회 부하 | 없음. snapshot 목록은 테이블 metadata.json 파일 1개에 포함 — S3 GET 1회, manifest/데이터 파일 접근 없음. **테이블당 1회**(batch_id를 IN으로 묶어 대조), 실행 빈도는 하루 1회(batch_id를 가진 row가 있는 테이블만) |

### 4.4 제약: snapshot 보존 기간

- snapshot 보존 정책: **3일** (`expire_snapshots`)
- 자동 재처리 범위(전날+그저께 = 2일) < 보존(3일) → 자동 경로에서는 영수증 확인이 항상 가능
- **3일을 넘긴 FAILURE 건은 영수증이 expire되어 확인 불가** → 수동 처리 시 별도 검증 필요 (섹션 8.3)
- snapshot 보존 정책을 단축할 경우 반드시 `보존 기간 > 재처리 조회 범위(2일)` 유지

---

## 5. 재처리 DAG 상세 설계

### 5.1 DAG 기본 설정

| 항목 | 값 | 근거 |
|------|-----|------|
| DAG 수 | **1개** (테이블별 task 순차 실행) | Compaction DAG과 동일 패턴. 재처리는 대부분 테이블이 no-op인 청소 배치라 테이블별 DAG 분리는 관리 소음 |
| schedule | `0 1 * * *` (KST) | `ts`가 전날 23:59대인 데이터가 자정을 넘겨 Job History에 적재될 수 있어, 전날 데이터가 안정된 후 조회하도록 1시간 버퍼 |
| timezone | `Asia/Seoul` (pendulum) | logical_date UTC 혼선 차단. 날짜 계산 전부 KST 기준 |
| max_active_runs | 1 | 중복 실행 방지 + loop 재trigger 순차 실행 보장 |
| catchup | False | 과거 스케줄 재실행 불필요 (수동 실행은 params로) |

**params (UI 수동 실행용)**

| param | 기본값 | 설명 |
|-------|--------|------|
| `tables` | 전체 테이블 | 처리 대상 테이블 multi-select — 1개/여러 개/전체 선택 가능. 정기 실행은 기본값(전체). 선택지·기본값은 append DAG과 동일한 **`iceberg.py`의 hourly/daily Enum 클래스**에서 생성 (`Param(type="array", examples=[...])` — multi-select UI는 `examples`가 만든다, 6.3 참조) — hourly/daily 분류는 소속 Enum 클래스로 결정되고, 테이블 추가/제거 시 Enum 한 곳만 수정하면 append/재처리가 함께 반영되는 단일 소스 |
| `start_time` / `end_time` | 없음 | **수동 실행 시 조회 범위를 직접 정의** (WAIT_SCHEDULING+FAILURE 전체). 둘 다 함께 지정해야 하며, `end_time ≤ 전날 00:00`만 허용 — 전날/당일은 append 조회 범위와 겹치므로 prepare_run이 검증 후 거부. 기존 DAG과 동일한 date-time 형식 → 내부에서 ts 문자열(`YYYYMMDDHHmmSSsss`)로 변환. 미지정 시 정기 범위(그저께 00:00 ~ 전날 끝) |

### 5.2 Task 구성

```
check_zombie_jobs                          # 좀비 IN_PROGRESS 탐지 → 알림 (독립 실행)
                                           # 관측용 — 본 파이프라인과 의존 없음.
                                           # 알림 실패가 재처리 본류를 막지 않도록 분리

prepare_run                                # params 검증·정규화 1회 → XCom (get_time 패턴)
      │                                    # ts 경계 계산, 수동 범위 검증, date-time → ts 변환
┌─ ConvertFileTaskGroup: TABLE_A (기존 append DAG 템플릿 재사용) ─┐
│  get_jobs            # ← __init__ 재처리 분기가 생성           │
│      │               # (범위·상한·영수증 확인. 대상 0건→skip)  │
│  append_data         # 템플릿 제공 (Spark append)              │
│      ├── update_success  [all_success]  # 템플릿 제공          │
│      └── update_failure  [all_failed]   # 템플릿 제공          │
└────────────────────────────────────────────────────────────────┘
      │  (다음 그룹 첫 task는 trigger_rule=all_done —
      │   앞 테이블 실패가 뒤 테이블 처리를 막지 않음)
┌─ ConvertFileTaskGroup: TABLE_B ─┐ ... (테이블 수만큼 순차)
└─────────────────────────────────┘
      │
compaction_targets [all_done] → trigger_compaction  # 집계 → TriggerDagRunOperator.expand (섹션 6)
next_loop          [all_done] → retrigger_self       # 잔여분 판단 → 자기 재trigger (0/1건, 5.5)
```

> Compaction/재trigger는 대상 개수가 가변(Compaction 여러 건, loop 0/1건)이므로, 집계 task가 `TriggerDagRunOperator` kwargs 목록을 만들고 **dynamic task mapping(`expand_kwargs`)** 으로 trigger한다. 빈 목록이면 mapped operator는 skip된다.

- **기존 ConvertFileTaskGroup 재사용 (`__init__`에 재처리 분기 추가)**: append DAG의 TaskGroup 템플릿(get_jobs → Spark append → update_success/update_failure)을 그대로 쓰고, **재처리 조회 task도 부모 `__init__` 안에 둔다**. 재처리 DAG이 넘기는 것은 조회 범위(`reprocess_cfg` = prepare_run XCom) 하나뿐이다.
  - 기각한 대안들 — 조회 task를 부모 밖으로 빼는 방식 전부: ① 메서드 추출 후 상속 override ② builder 주입 ③ 필요한 헬퍼를 파라미터로 전달. 공통 사유: 조회 로직은 `__init__` 지역 함수·설정값(`_update_jobs`, logger, config …)을 사용해야 하는데, 밖으로 빼면 그것들을 일일이 전달해야 하고 헬퍼가 늘 때마다 시그니처가 깨진다
  - **채택**: `__init__(..., reprocess_cfg=None)` 인자와 분기만 추가한다. 미지정이면 기존 인라인 경로(append: 코드·closure 전부 그대로, 동작 동일), 지정이면 같은 `__init__` 스코프에서 재처리 조회 task를 만든다 — **지역 함수를 그냥 호출**하면 되므로 전달 인자가 늘지 않는다 (섹션 7)
  - 재처리 조회 task 옵션: `trigger_rule="all_done"`(앞 테이블 실패에도 실행) + `ignore_downstream_trigger_rules=False`(skip을 그룹 내로 한정)
  - **`cfg` 없으면 즉시 skip (구현 주의)**: `all_done`은 앞 테이블뿐 아니라 `prepare_run` 실패도 통과시킨다. `prepare_run`은 특정 테이블이 아니라 **모든 테이블의 공통 전제(조회 범위)** 이므로, 실패하면 `cfg`가 `None`으로 내려와 테이블 수만큼 실패가 쌓인다. 조회 task 첫 줄에서 `if not cfg: return False`로 막아 원인 1건에 알림 1건이 되게 한다
- **테이블별 순차 실행**: Spark job(최대 24 executor)이 테이블 수만큼 동시에 뜨면 K8S가 감당하지 못한다. Compaction DAG과 동일하게 순차 — 잔여분 없는 테이블은 조회 후 즉시 skip이라 빠르다
- **상태 update는 그룹 내부에서만**: `all_success`/`all_failed`가 각 테이블 자신의 Spark task에만 걸리므로, 테이블 간 부분 실패로 상태 update가 누락되는 구멍이 없다
- **skip 전파 차단 (구현 주의)**: ShortCircuit의 기본 동작은 trigger_rule을 무시하고 **모든 하류 task를 재귀적으로 skip**시킨다. 기본값 그대로면 잔여분 없는 첫 테이블이 skip되는 순간 뒤 테이블 그룹 전체가 skip된다. 반드시 `ignore_downstream_trigger_rules=False`로 설정해 skip을 그룹 내 직계 하류로 한정한다 (`trigger_rule=all_done`인 다음 그룹/집계 task는 정상 실행)
- **상태 update의 대상 식별은 XCom의 복합키 목록으로만**: `stat_desc`(batch_id)는 CLOB이라 WHERE 조건 사용 금지 (섹션 4.2 제약). update_success/update_failure는 get_jobs가 XCom(`meta.keys`)에 남긴 복합키 목록으로 UPDATE한다
- **모든 상태 UPDATE는 row의 원천 DB로 나간다**: 복합키 값은 DB 간 유일 보장이 없으므로 어느 DB에서 온 row인지가 UPDATE 대상을 결정한다. 조회 결과를 담은 `{conn_id: rows}` 구조를 그대로 이어받아 `meta.keys`를 **`{conn_id: [복합키 값 tuple, ...]}`** 형태로 만들고, update task는 conn별 loop로 부모 `_update_jobs`를 호출한다
- **params는 prepare_run에서만 읽는다**: 기존 append DAG의 get_time 패턴과 동일. 검증·형식 변환(`ts` 경계 계산, 수동 범위 검증, date-time → ts 문자열 변환)을 첫 task에서 1회 수행하고, 이후 task들은 정규화된 XCom 값만 소비한다. 잘못된 입력은 파이프라인 중간이 아닌 첫 task에서 즉시 실패하고, TaskGroup 템플릿이 DAG params에 결합되지 않아 재사용이 가능해진다

### 5.3 get_jobs(재처리 조회) 처리 순서 (테이블별)

선행 task `prepare_run`이 params 검증과 `ts` 경계 계산을 1회 수행해 XCom으로 내려보내며(수동 범위 검증, date-time → ts 문자열 변환 포함), 재처리 조회 로직은 정규화된 값만 사용한다.

1. **실행 대상 확인** — 정규화된 `tables` 목록에 자기 테이블이 없으면 즉시 skip
2. **대상 조회** — **Oracle DB 2개에 동일 쿼리를 반복**(append의 conn_list loop 패턴)하고 결과를 `{conn_id: rows}` dict로 보관한다 (상태 UPDATE가 이 키로 원천 DB를 찾아가므로 row에 출처를 태깅할 필요가 없다). 이후 `(ts, row, conn_id)`로 펼쳐 전체 `ts` 오름차순 정렬한다 (append와 같이 오래된 것부터 처리). `ts` 범위 조건(파티션 키 → Partition Pruning 유지) + row 수 상한(**DB당** 적용):

```sql
-- conn_list의 DB 2개에 각각 실행 (결과는 {conn_id: rows}로 보관)
-- 조회 컬럼 = 복합키 4개 + base_path + param(JSON) + stat_desc(영수증용)
-- status는 조건으로만 쓰고 결과로 받지 않는다 (아래 주의 참조)
SELECT * FROM (
    SELECT k_1, k_2, k_3, ts, base_path, param,
           DBMS_LOB.SUBSTR(stat_desc, 4000, 1) AS stat_desc  -- CLOB → VARCHAR2 (아래 주의)
      FROM JOB_HISTORY
     WHERE table_name = :tbl
       AND ts >= :ts_from           -- 그저께 00:00  '20260702000000000'
       AND ts <  :ts_to             -- 전날 끝       '20260704000000000'
       AND ( status = 'FAILURE'                                   -- FAILURE: 전 구간
             OR (status = 'WAIT_SCHEDULING' AND ts < :wait_bound)           -- WAIT_SCHEDULING: 전날 01:00 이전만
           )
     ORDER BY ts ASC                -- append와 동일, 오래된 것부터
) WHERE ROWNUM <= :row_limit        -- 1,000 (ts는 meta의 적재 범위 기록용으로 함께 조회)
```
   - 조회는 **append와 동일하게 `OracleHook.get_records`** 를 쓴다. 반환은 tuple 목록이므로 `namedtuple`(`Job`)로 감싸 이름으로 접근한다. **필드 순서를 SELECT와 맞춰야 하며**, 어긋나면 `Job(*row)`가 개수 불일치로 **즉시 실패**한다 — 컬럼 목록 상수를 따로 두고 `zip`하면 값이 밀려도 조회는 성공해 런타임까지 드러나지 않는다
   - 복합키를 SELECT 앞에 몰아두어 **`job[:4]`가 곧 상태 UPDATE에 넘길 키**가 된다 (별도 추출 함수 불필요)
   - **`status`는 조건으로만 쓰고 결과로 받지 않는다.** 영수증 확인은 status가 아니라 batch_id로 판단하므로(섹션 4.2), 결과에 두면 잘못된 필터가 다시 생길 여지만 남는다
   - 수동 실행 시: prepare_run이 검증한 `start_time`~`end_time` 범위의 WAIT_SCHEDULING+FAILURE 전체
   - 조회 직후 **영수증 필터를 적용하기 전 건수로 잔여분 여부를 기록** (**어느 한 DB라도** 상한 1,000건을 채웠으면 그 DB에 더 남았다는 뜻 — loop 판단은 이 시점 값 기준. 필터로 줄어든 후의 건수로 판단하면 잔여분을 놓친다)
3. **영수증 확인** — 조회 row에서 읽은 stat_desc(batch_id)를 모아 `.snapshots`와 한 번에 대조 → 커밋이 확인된 batch의 row는 SUCCESS 정정(원천 DB별) 후 대상에서 제외 (섹션 4). **status로 거르지 않는다** — WAIT라도 커밋 후 상태 갱신만 실패한 것일 수 있고, 그 경우 재적재하면 중복이다
4. **파일 목록 수집** — 조회한 row 전부의 `param.files`를 순서대로 모은다. 별도 크기 상한은 두지 않는다 — 한 회차 물량은 `ROW_LIMIT`으로만 통제한다

> **크기 기준 상한을 두지 않는 이유**: 처리량 통제 수단이 둘이면 둘 사이가 어긋난다. `ROW_LIMIT`이 만드는 최대 물량은 약 22GB로 추정되는데(아래 환산), 여기에 16GB 같은 크기 상한을 겹치면 크기 쪽이 항상 먼저 걸려 `ROW_LIMIT` 값이 무의미해진다. 상한을 하나로 두고 그 값을 실측으로 조정하는 편이 낫다.
>
> | 근거 | 값 |
> |------|-----|
> | 벤치마크 (CLAUDE.md) | 10분치 ≈ 8GB |
> | 유입량 | 5분치 ≈ 200 rows → 10분치 ≈ 400 rows |
> | ⇒ row 1건 | ≈ 20MB |
> | DB별 물량 | DB2는 DB1의 1/10 미만 → 최대 ≈ 1,100 rows |
> | ⇒ 한 회차 최대 | ≈ **22GB** ≈ Spark 2분 (처리량 8GB/44초 선형 외삽) |
>
> 백로그가 크게 쌓인 날에만 이 규모에 도달하며, 그때의 Spark job 크기를 실측해 `ROW_LIMIT`을 조정한다 (섹션 9-8).
5. **XCom 기록 (2건)** — **마킹보다 먼저** 남긴다. 마킹은 DB 수만큼 UPDATE가 나가 중간 실패 가능성이 있는데, XCom이 없으면 이미 마킹된 row를 update_failure가 회수하지 못해 좀비가 된다. 순서가 반대면 마킹되지 않은 row는 상태가 WAIT_SCHEDULING/FAILED라 update task 조건에서 자동으로 빠지고 다음 회차에 정상 회수되므로, 어느 쪽으로 실패해도 안전하다

| key | 내용 | 소비자 |
|-----|------|--------|
| `meta` | `batch_id`, `keys`, `done_keys` — keys는 **`{conn_id: [복합키 값 tuple]}`** | **부모** update_success / update_failure |
| `reprocess` | `ts_min`, `ts_max`, `has_more` | **재처리 DAG** compaction_targets / next_loop |
| `num_executors` | 파일 목록 함수가 산정해 반환한 값 | **Spark operator** (pull) |

   - `ts_min`/`ts_max`는 **이번에 적재한 데이터의 시간 범위**다. 이 범위만 Compaction하도록 기존 Compaction DAG에 넘긴다 (섹션 6.3)
   - `has_more`는 **상한에 걸려 못 담은 대상이 남았는지** 여부다. 남았으면 DAG을 한 번 더 trigger한다 (5.5)
   - 테이블명·Compaction 그룹은 XCom에 넣지 않는다 — 수집 측이 Enum을 순회하므로 그 자리에서 붙이면 된다

> **XCom pull은 push한 task의 task_id로만 가능하다.** 조회 task는 테이블별 TaskGroup 안에 있으므로 task_id가 `{group_id}.get_jobs`이며(append DAG과 동일한 구분 방식), **TaskGroup 밖에 있는** 집계 task(`compaction_targets`·`next_loop`)는 이 값을 알아야 한다.
>
> 이 문자열을 집계 쪽에서 다시 조립하면 안 된다 — group_id 규칙이 바뀌어도 `xcom_pull`은 예외 없이 `None`을 돌려주므로, 해당 테이블이 Compaction 대상과 loop 판단에서 **조용히 빠진다**. DAG 조립 시점에 실제로 만들어진 TaskGroup의 `group_id`에서 뽑아 집계 task 인자로 넘긴다.
>
> 그룹 **안쪽**은 해당 없다. 부모 TaskGroup은 `group_id`를 `__init__` 인자로 받아 조립하므로(append도 테이블마다 다른 값을 넘긴다) 재처리 경로에서도 그대로 맞는다.
   - `keys`는 **복합키 값 tuple**로 올린다. row마다 컬럼명을 풀어 담으면 같은 이름 4개가 건수만큼 반복돼 XCom만 커진다
6. **IN_PROGRESS 마킹 — 기존 함수에 위임** — 상태 UPDATE는 ConvertFileTaskGroup의 `_update_jobs`가 이미 수행한다. 재처리는 `{conn_id: [복합키 tuple]}`과 `batch_id`를 만들어 넘길 뿐, UPDATE 문을 따로 갖지 않는다 (영수증 정정도 같은 함수 사용)

> **영수증은 마킹 때 한 번 쓰고, 이후 상태 변경에서는 건드리지 않는다.**
> `_update_jobs`의 UPDATE 문을 하나로 합쳐 `SET stat_desc = :batch_id`를 항상 두면,
> batch_id 없이 부르는 경로가 stat_desc를 **NULL로 덮어써 영수증이 지워진다.**
>
> | 호출 | batch_id | 합쳐 둘 경우 결과 |
> |------|----------|------------------|
> | IN_PROGRESS 마킹 | 있음 | 영수증 기록 (정상) |
> | update_success | 없음 | NULL로 덮어씀 |
> | **update_failure** | 없음 | **NULL로 덮어씀** |
> | 영수증 확인 후 SUCCESS 정정 | 없음 | 방금 확인한 영수증을 지움 |
>
> `update_failure`가 특히 문제다. `FAILURE`는 다음날 재처리가 집는 대상이고 그때
> 영수증으로 커밋 여부를 판단해야 하는데, 그 값을 지워버리므로 **거짓 실패 건이
> 판단 근거 없이 재적재된다.** 중복 적재 방지(섹션 4)가 통째로 무력화된다.
>
> 따라서 **batch_id를 준 호출에서만 stat_desc를 갱신**한다. 이때 **SQL과 바인드를
> 한 함수에서 함께 돌려준다** — SQL에 없는 바인드를 넘기면 드라이버가 거부하므로
> (`no bind placeholder named ":batch_id" was found in the SQL text`), SQL 모양과
> 바인드 구성은 따로 결정할 수 없는 한 가지 사안이다.
>
> ```python
> receipt_set, receipt_bind = ((", stat_desc = :batch_id", {"batch_id": batch_id})
>                             if batch_id is not None else ("", {}))
> return f"UPDATE JOB_HISTORY SET status = :status{receipt_set} WHERE <복합키>", receipt_bind
>
> # 호출부 — 받은 두 값을 그대로 쓴다 (분기 없음)
> sql, receipt = JobHistoryQuery.update_status(batch_id)
> bind = {"status": status, **receipt, "k_1": ..., "k_2": ..., "k_3": ..., "ts": ...}
> ```
>
> `COALESCE(:batch_id, stat_desc)`로 조건 없이 한 문장에 담는 방법은 권하지 않는다 —
> stat_desc가 CLOB이라 VARCHAR2 바인드와 섞으면 암시적 변환에 의존하고
> (`ORA-00932` 가능), `executemany`에서 batch_id가 전 건 NULL이면 바인드 타입
> 추론이 되지 않는다.
>
> SUCCESS 이후에도 영수증을 남기면 좀비 수동 판정(섹션 8.2)에서 snapshot과 대조할 수 있다.

7. **Spark 입력 준비 — 기존 함수에 위임** — 재처리 조회 로직은 담기로 한 파일 목록을 반환하는 데서 끝난다. avro 경로 텍스트 파일 S3 업로드와 size 총합 XCom push(Spark operator가 pull)는 **ConvertFileTaskGroup에 이미 있는 함수가 그대로 수행**하므로 재구현하지 않는다

> **`param` 구조**: `{"files": [{"file_path": ..., "size": ...}, ...]}` — **row 1건에 파일이 여러 개**일 수 있다. 재처리가 이 목록을 가공 없이 모아 부모 함수에 넘기므로, 경로 조합 규칙·size 단위 해석이 append와 자동으로 일치한다. 재처리는 이 목록의 내용을 들여다보지 않는다 — `size`도 부모 함수가 합산한다.

### 5.4 처리 상한

| 항목 | 값 | 보호 대상 | 근거 수준 |
|------|-----|----------|----------|
| 테이블당·**DB당** 조회 row 수 | 1,000 (ROWNUM) | Oracle SELECT 성능, XCom 크기, avro 경로 목록 파일 크기. DB 2개이므로 테이블당 최대 2,000건이 병합될 수 있음 | ⚠️ 러프 설정 — 재검증 필요 |
| num_executors | — | 재처리가 산정하지 않는다. size 총합을 XCom에 올리는 기존 함수를 그대로 쓰므로 Spark operator의 기존 산정 경로를 탄다 | — |

**규모 감각**: append는 약 5분 주기에 조회 상한 200 rows(DB당). 재처리 상한 1,000 rows(DB당) ≈ 약 25분치 물량. 정상 운영의 하루 잔여분은 이보다 훨씬 적을 것으로 예상하지만, 상한값은 검증된 값이 아니므로 운영 데이터로 재조정한다.

> **K8S 리소스 경합 주의**: 재처리 Spark job(최대 24 executor, 96 core, ~213GB)이 도는 동안에도 약 5분 주기 append job이 뜬다. 동시 실행 시 최대 **~192 core, ~427GB**. 클러스터 여유가 부족하면 재처리 job의 executor 상한을 낮춘다(예: 12 — 지연 데이터이므로 처리 속도의 우선순위가 낮음).

### 5.5 잔여분 loop: 자기 자신 재trigger

한 DAG run 안에서 Spark task를 여러 번 도는 대신, **잔여분이 남았으면 같은 DAG을 한 번 더 trigger**한다.

```
run N: 테이블별 최대 1,000건(DB당) 처리
       → 조회 상한을 채운(= DB에 더 남은) 테이블이 하나라도 있으면 자기 자신을 trigger
         (첫 회차가 확정한 조회 범위·tables·loop_count를 conf로 승계)
run N+1: 동일 파이프라인 반복. 남은 게 없는 테이블은 조회 후 즉시 skip
종료: 잔여분 있는 테이블 없음 또는 loop_count 상한 도달 → trigger 안 함
```

- **재trigger 조건 = 조회 상한을 채운 테이블 존재.** **어느 한 DB라도 필터 전 조회 건수가 상한(1,000)에 도달**하면 그 DB에 더 남았다는 뜻이다 (섹션 5.3). 상한을 채우지 못한 테이블(실패 포함)은 loop를 유발하지 않는다
- **지속 실패의 유한 종료**: 상한을 채운 테이블의 Spark가 계속 실패하면 다음 회차가 같은 row를 다시 집을 수 있으나, `loop_count` 상한 10회로 그날 밤 안에 종료되고 알림 후 수동 전환된다. 대부분의 실패는 상한 미달이라 애초에 loop를 만들지 않는다
- 집계용 meta는 **get_jobs가 push한 XCom**에서 읽는다 (Airflow 3 worker는 task 상태 DB 조회 불가). meta 존재 = 그 테이블이 이번 회차에 처리 대상을 선점했다는 뜻
- `max_active_runs=1`이므로 회차는 자동으로 순차 실행된다
- 매 회차가 동일한 단순 파이프라인 — Spark task당 자기 상태 update가 붙어 있어 부분 실패 문제가 없다
- **첫 회차가 확정한 조회 범위·tables를 conf로 승계**: prepare_run은 conf에 범위가 있으면 재계산하지 않고 그대로 사용한다. 수동 실행의 선택 값이 회차에서 유실되지 않고, 회차가 자정을 넘겨 실행되어도 ts 경계가 첫 회차 기준으로 유지된다
- **폭주 방지**: `loop_count` 상한 10회 (≈ 테이블당 최대 1만 건/DB, DB 2개 기준 2만 건/일). 도달 시 알림 → 수동 전환

---

## 6. Compaction 연계

### 6.1 방식: 기존 Compaction DAG trigger

재처리 DAG은 `rewrite_data_files`를 직접 실행하지 않고 **기존 Compaction DAG을 trigger**한다.

| 이점 | 설명 |
|------|------|
| 동시 실행 방지 | Compaction DAG의 `max_active_runs=1`이 trigger run과 스케줄 run을 자동 직렬화 → 같은 테이블에 rewrite 2개 동시 실행으로 인한 Iceberg commit 충돌 차단 |
| 로직 단일화 | rewrite 옵션, 테이블 목록, 실패 알림이 기존 DAG 한 곳에 유지 |
| 검증된 경로 | UI 수동 실행용 params를 그대로 사용 — `TriggerDagRunOperator`의 `conf`가 선언된 params를 덮어쓰므로 수동 실행과 동일 경로 |

**Compaction DAG params** (양쪽 공통으로 `tables` multi-select 추가 — 기본 전체, 수동/trigger 시 일부 선택. 선택지는 재처리 DAG과 동일하게 `iceberg.py`의 hourly/daily Enum에서 생성 권장 — 각 DAG은 자기 그룹 Enum만 사용):

| DAG | params | 형식 |
|-----|--------|------|
| daily | `target_dt` + `tables` | `target_dt`는 **`format="date"`** → `2026-07-28` |
| hourly | `start_time`, `end_time` + `tables` | 둘 다 **`format="date-time"`** → `2026-07-28T08:15:00+09:00` |

> **conf 값을 이 형식으로 변환해서 넘겨야 한다.** Job History의 `ts`(`YYYYMMDDHHmmSSsss`)를
> 그대로 넣으면 DagRun 생성이 params 검증에서 실패한다. 이때 Airflow API 서버는 422가
> 아니라 **500**을 돌려주고, `TriggerDagRunOperator`는 그 실패를 처리하지 못한 채
> `UnboundLocalError: cannot access local variable 'state'`로 터진다 — task 로그만 보면
> 원인을 알 수 없으므로 **API 서버 로그**를 봐야 한다.
>
> ```
> ValueError: Invalid input for param target_dt: '20260728' is not a 'date'
> ```
>
> 변환은 `dates_between`(→ `YYYY-MM-DD`)과 `ts_to_hour_param`(→ ISO 8601)이 담당한다.
>
> hourly 범위는 **시 단위로 내림**해서 넘긴다. 대상 테이블이 `hour(ts)` 히든 파티셔닝이라
> Compaction 단위가 1시간 통이고, 통 중간을 가리키는 값을 주면 그 통을 반쪽만 지정하게
> 된다. `end_time`은 여기에 **+1시간** 한다 — 내림한 값 그대로면 `ts_max`가 속한 통이
> 범위 밖으로 떨어져 정작 적재한 시간대가 Compaction되지 않는다.

**`tables` param 선언** (양쪽 DAG에 추가, 각자 자기 그룹 Enum만 사용):

```python
DAILY_TABLE_NAMES = [t.get_name() for t in DailyIcebergTable]   # hourly는 HourlyIcebergTable

"tables": Param(
    default=DAILY_TABLE_NAMES,      # 실제 값 — 정기 실행은 UI를 안 거치므로 이 값이 곧 전체 처리
    type="array",                   # format은 쓰지 않는다
    examples=DAILY_TABLE_NAMES,     # 값에 관여하지 않고 multi-select UI만 만든다
)
```

> **multi-select UI를 만드는 속성은 `examples`다.** `type="array"`만 선언하면 여러 줄
> 텍스트 필드가 나오고 **한 줄에 한 값**으로 입력해야 한다(콤마 구분 아님). `items`는
> 렌더링에 관여하지 않는다 — `items.type`이 `"string"`이 아닐 때 JSON 입력창으로 바뀔 뿐이고,
> `items.enum`은 jsonschema 검증에만 쓰인다. 표시 라벨을 값과 다르게 하려면
> `values_display={"table_a": "테이블 A"}`를 함께 준다.
> (근거: Airflow `core-concepts/params.rst` — array 항목 "If you add the attribute
> `examples` with a list, a multi-value select option will be generated instead of a
> free text field.")

`default`가 전체여야 conf 없이 도는 **정기 스케줄 실행의 대상 범위가 그대로 유지**된다.

#### params 선언만으로는 필터가 동작하지 않는다

현재 Compaction DAG은 테이블 Enum을 파싱 시점에 loop해서 `SparkKubernetesOperator`를 테이블 수만큼 만들고 `chain()`으로 직렬 연결한다. `params`는 DagRun이 생겨야 값이 정해지므로, 이 구조에서는 어떤 조건문을 넣어도 선택 결과를 반영할 수 없다 — **task 자체를 런타임에 만들어야 한다.**

```python
TABLE_NAMES = [t.get_name() for t in IcebergTable]


@task
def compaction_specs(params=None) -> list[dict]:
    """선택된 테이블의 operator 인자를 만든다. dict 1개 = 복사본 1개.

    반환값은 XCom을 거치므로 원시 타입만 담는다.
    """
    target_time = params.get("target_dt") or ...     # 기존 get_time의 기본값 로직
    selected = set(params["tables"])
    return [
        {
            # arguments[0]은 테이블명 — map_index_template이 이 위치를 참조한다
            "arguments": [table.get_name(), str(COM_TARGET_FILE_SIZE_BYTES),
                          target_time, str(table.config.com_max_concurrent_file_group)],
            "instances": str(table.config.com_num_executor),
        }
        for table in IcebergTable
        if table.get_name() in selected
    ]


SparkKubernetesOperator.partial(
    task_id="compact",
    max_active_tis_per_dagrun=1,                    # chain()이 하던 직렬 실행을 대신한다
    map_index_template="{{ task.arguments[0] }}",    # UI map index를 테이블명으로
    # ...기존 루프에서 table이 등장하지 않던 인자 전부 그대로...
).expand_kwargs(
    # map()은 XCom을 건넌 뒤 실행된다 — 커스텀 객체는 여기서만 만들 수 있다
    compaction_specs().map(
        lambda spec: {
            "arguments": spec["arguments"],
            "executor": DriverAndExecutor(instances=spec["instances"]),
        }
    )
)
```

**변경 예시 전문: `pipeline/examples/compaction_dag_example.py`** (daily 기준. hourly는 `target_dt` 대신 `start_time`/`end_time`을 담고 구조는 동일)

| 주의 | 내용 |
|------|------|
| **직렬 실행 유실** | mapped task instance는 기본이 **병렬**이다. `max_active_tis_per_dagrun=1`을 빼면 Spark job이 테이블 수만큼 동시에 뜬다 — 이 전환에서 가장 위험한 지점 |
| dict 키 = operator 인자명 | 인자를 더 넘겨야 하면 `compaction_specs`가 반환하는 dict에 키만 추가한다. 단 `task_id`, `queue`, `pool`은 확장 대상이 아니라 `partial`에만 둘 수 있다 — 테이블별 `task_id`는 포기하고 `map_index_template`으로 대신한다 |
| **커스텀 객체는 XCom을 못 건넌다** | `compaction_specs`의 반환값은 XCom을 거치므로 `DriverAndExecutor` 인스턴스를 담을 수 없다. 원시값만 반환하고 객체 생성은 XCom 이후에 도는 `.map()`이 맡는다. `.map()`에 넘기는 것은 `@task`가 아니어야 한다 |
| **expand된 값의 Jinja는 렌더링되지 않는다** | `template_fields`에 있어도 마찬가지다. XCom에서 resolve된 값은 `id()`가 `seen_oids`에 등록되고(`expandinput.py`), 렌더러가 `if id(value) in oids: return value`로 건너뛴다(`templater.py`). 기존 `'{{ ti.xcom_pull(task_ids="get_time") }}'`는 실제 값으로 대체해야 한다 |
| get_time 흡수 | `get_time`은 params만 읽어 날짜를 포맷하는 일만 하므로 `compaction_specs`에 합친다. task 하나와 XCom 왕복 한 번이 줄고, 다른 task가 그 XCom을 참조하지 않는지만 확인하면 된다 |
| task_id 고정 | 테이블별 task 이름이 사라지고 `compact` 노드 1개 + map index로 바뀐다. `map_index_template`으로 라벨을 테이블명으로 되돌릴 수 있고, 실행 전에도 렌더링되므로 running·failed 상태에서도 보인다 |
| 선택 0개 | mapped task는 `skipped` 처리된다 |

static task를 유지하고 테이블마다 gate task를 다는 대안은, 선형 chain에서 skip이 하위 전체로 전파되어 모든 task에 `trigger_rule="all_done"`을 달고 gate를 테이블 수만큼 더 만들어야 하므로 채택하지 않는다.

재처리가 왜 필요하게 만드는가: 정기 Compaction은 시간당(직전 1시간)·일일(전일치) 범위만 보므로, 재처리가 적재하는 **과거 시간대/과거 날짜**는 정기 run이 다시 방문하지 않는 구간이다. trigger 없이는 재처리분 small file이 과거 파티션에 영구히 남는다.

### 6.2 daily Compaction 스케줄 변경: 00:35 → 02:00

| 항목 | 내용 |
|------|------|
| 변경 | daily Compaction `35 0 * * *` → **`0 2 * * *`** |
| 효과 ① | 재처리(01:00)가 적재한 **전날 데이터**가 전일치 정기 run에 자연 포함 → 전날분은 trigger 불필요 |
| 효과 ② | 기존 00:35의 숨은 구멍 해소 — 자정 넘어 늦게 도착한 전날 데이터를 append가 00:35 이후에 적재하면 정기 Compaction을 영영 놓쳤음 |
| hourly | **`15 * * * *` 유지** — 정기 run은 직전 1시간만 보므로 스케줄을 옮겨도 과거 시간대는 커버 불가. 과거분은 어차피 trigger로 처리 |

### 6.3 trigger 규칙

재처리 DAG의 `trigger_compaction` task가 이번 run에서 **실제 적재된 (테이블, 날짜/시간 범위)를 집계**해서, 같은 날짜/범위는 테이블 목록으로 묶어 **1회씩** trigger한다. 단일 DAG이므로 전체 적재 결과를 한곳에서 알 수 있다.

| 재처리 적재분 | Compaction 처리 |
|--------------|----------------|
| daily 그룹 테이블 | 적재한 날짜별로 daily DAG trigger: `conf={"target_dt": 날짜, "tables": [해당 테이블들]}` |
| hourly 그룹 테이블 | hourly DAG trigger: `conf={"start_time": ..., "end_time": ..., "tables": [해당 테이블들]}` (적재 데이터 ts 최소~최대 범위) |

- **조건 없이 적재분 전부 trigger한다.** "전날 daily분은 02:00 정기 run이 커버하니 생략" 같은 조건부 생략을 두지 않는 이유: loop 회차가 02:00을 넘겨 전날 데이터를 적재하면 정기 run은 이미 지나갔는데 trigger도 생략되어 Compaction이 영영 누락된다. `tables` 필터 덕분에 trigger run은 해당 테이블만 처리하므로 중복 비용이 작고, 이미 Compaction된 범위의 중복 실행은 합칠 파일이 없어 사실상 no-op이다
- `wait_for_completion=False` — Compaction 실패 알림은 Compaction DAG이 담당. 대기하면 재처리 DAG 실행 시간만 늘어남
- loop 회차마다 자기 회차 적재분을 trigger하면 되므로 loop와의 상호작용 없음

---

## 7. 기존 DAG 변경 사항

| 대상 | 변경 | 내용 |
|------|------|------|
| append DAG (테이블별 공통 py) | batch_id 기록 3건 | ① `get_jobs`의 IN_PROGRESS 마킹 UPDATE에 `stat_desc = :batch_id` 추가 ② Spark 쓰기에 `option("snapshot-property.batch_id", batch_id)` 추가 ③ **`_update_jobs`가 batch_id를 준 호출에서만 stat_desc를 갱신하도록 분기** — 안 그러면 update_success/update_failure가 영수증을 NULL로 지운다 (섹션 5.3-6) |
| ConvertFileTaskGroup | `reprocess_cfg` 옵션 인자 + 재처리 분기 추가 | ① `__init__(..., reprocess_cfg=None)` 인자 추가 ② get_jobs 생성부를 if/else로 감싸고 else에 재처리 조회 task를 둔다 — 미지정이면 기존 인라인 경로(**코드·closure 전부 그대로, append 동작 완전 동일**). 재처리 task가 같은 `__init__` 스코프에 있으므로 `_update_jobs`·logger·config를 그냥 호출한다 (전달 인자 없음). **변경 예시: `pipeline/examples/convert_file_taskgroup_example.py`** |
| daily Compaction DAG | 스케줄 + params + task 생성 방식 | `35 0 * * *` → `0 2 * * *`. params에 `tables` multi-select 추가 (기본 전체). **Enum loop + `chain()`을 mapped task로 전환** — 안 그러면 params가 선언만 되고 필터가 동작하지 않는다 (6.1). **변경 예시: `pipeline/examples/compaction_dag_example.py`** |
| hourly Compaction DAG | params + task 생성 방식 | 위와 동일. 스케줄 변경 없음. `target_dt` 대신 `start_time`/`end_time`을 담는다 |

append DAG의 조회 로직(최근 1일, WAIT만, ts ASC, DB당 ROWNUM 200, conn_list loop)과 update task 구조는 **변경 없음**.

---

## 8. 모니터링 및 수동 처리 절차

### 8.1 잔류 데이터 알림 (일 1회)

| 대상 | 조건 | 대응 |
|------|------|------|
| 자동 재처리 범위 초과 | 그저께 이전(3~7일 전) `ts` 범위에 `WAIT_SCHEDULING` 또는 `FAILURE` 존재 | 원인 확인 → 재처리 DAG 수동 실행 (8.3) |
| loop 상한 도달 | `loop_count` 10회 초과 | append DAG 장애 등 대량 밀림 상황 → 원인 확인 후 수동 판단 |

> 잔류 알림 쿼리도 하루 단위 `ts` 범위 조회를 날짜별로 반복한다 (Partition Pruning 유지). 잔류가 매일 꾸준히 증가하면 스케줄링 문제가 아니라 **처리량 부족(capacity)** 신호 — append 조회 상한(DB당 200/5분)이 유입량과 같은 수준이므로 리소스 증설/주기/상한 조정을 검토한다.

### 8.2 좀비 IN_PROGRESS 탐지

get_jobs가 IN_PROGRESS로 전환한 후 DAG run이 증발하면(scheduler 장애, worker 강제 종료 — update task 2개 모두 미실행) 해당 row는 어느 DAG도 집지 않는다.

- 재처리 DAG의 독립 task(`check_zombie_jobs`)가 임계 시간(2시간, 정상 처리 수 분 대비 충분한 여유) 초과 IN_PROGRESS를 **양쪽 DB의** 전체 테이블 대상으로 탐지해 **알림만** 발송한다 (알림에 발견된 `conn_id` 포함 — 수동 정정 시 대상 DB 식별용)
- 자동 복구는 하지 않는다 — 판정은 사람이 영수증 확인으로 수행:
  - 해당 테이블 snapshot에 그 batch_id **있음** → 적재 완료 → DONE으로 수동 정정
  - **없음** → 미적재 → WAIT로 수동 복구 (다음 주기에 자동 처리됨)

### 8.3 수동 재처리 절차

1. 알림 수신 → 원인 확인 (append DAG 장애 이력, 깨진 파일 여부 등)
2. Airflow UI에서 재처리 DAG을 params 지정 후 수동 실행:
   - `tables`: 대상 테이블 선택 (1개/여러 개/전체)
   - `start_time`/`end_time`: 대상 `ts` 범위 지정 — **`end_time ≤ 전날 00:00`만 허용** (전날/당일은 append 조회 범위와 겹쳐 거부. 전날 잔여분은 다음날 정기 실행이 자동 회수). 잔류량이 상한을 넘는 날은 범위를 쪼개서 여러 번 실행
3. 실행이 완료되면 Compaction trigger까지 자동으로 이어짐
4. **3일 초과 건 주의**: snapshot 보존(3일)을 넘긴 FAILED는 영수증 확인이 불가능하다. 재적재 전 중복 여부를 별도 검증할 것 — 예: 해당 ts 범위의 Iceberg row count와 원본(avro) 건수 대조

---

## 9. 운영 전제 조건 체크리스트

| # | 항목 | 기준 |
|---|------|------|
| 1 | Oracle Job History 파티션 보존 기간 | ≥ 7일 (잔류 알림 조회 범위) |
| 2 | Iceberg snapshot 보존 기간 | 3일. **항상 재처리 조회 범위(2일)보다 길게 유지** |
| 3 | K8S 클러스터 여유 용량 | append + 재처리 동시 실행 시 최대 ~192 core / ~427GB. 부족 시 재처리 executor 상한 하향 |
| 4 | Compaction DAG 사전 변경 | daily 스케줄 02:00 이동, 양쪽 `tables` params 추가 — 재처리 DAG 배포 **전에** 적용 |
| 5 | Spark task retries | `retries=2`, `retry_delay=5분` 권장 (일시적 오류 1차 방어) |
| 6 | 시간대 | 모든 DAG `Asia/Seoul` timezone 명시. `ts` 경계 계산 KST 기준 |
| 7 | stat_desc 컬럼 | batch_id 용도 전환 공유. **WHERE 조건 사용 금지** (CLOB — 값 기록/읽기만) |
| 7-1 | Oracle conn 목록 | append DAG의 conn_list와 동일 소스 사용. 조회·상태 UPDATE·좀비 탐지 모두 DB 2개 대상 |
| 8 | 처리 상한 재검증 | 테이블당·DB당 row 1,000 / loop 10회는 러프 설정 — 운영 데이터로 재조정. 한 회차 최대 물량은 ≈22GB(Spark 약 2분)로 **추정**일 뿐이므로, 백로그가 크게 쌓인 날의 실제 크기·소요시간을 측정해 판단한다 (섹션 5.4) |
| 9 | **Iceberg 카탈로그 = HMS** | append 동시 커밋의 원자성이 HMS의 compare-and-swap에 의존한다 (섹션 2.2). `HadoopCatalog`로 전환하면 S3에 원자적 rename이 없어 동시 커밋에서 스냅샷이 유실될 수 있다 |

---

## 10. DAG 구현 파일

**새로 만드는 파일은 DAG 하나뿐이다.** 재처리 조회 로직은 기존 ConvertFileTaskGroup 파일에 들어간다.

| 파일 | 구분 | 역할 |
|------|------|------|
| `pipeline/dags/iceberg_reprocess.py` | **신규** | DAG 정의. 조회 범위 계산(`prepare_run`), 테이블 그룹 배치, Compaction 연계, loop 판단, 좀비 탐지 |
| ConvertFileTaskGroup 파일 | **기존 수정** | 재처리 조회 SQL·함수 + `reprocess_cfg` 분기. 변경 내용 전체: `pipeline/examples/convert_file_taskgroup_example.py` |

> **조회 로직을 왜 부모 파일에 두는가**: 재처리 조회 task는 `__init__` 안에서 만들어야 한다(조회 뒤 처리가 전부 `__init__` 지역 함수라, 밖으로 빼면 그것들을 일일이 넘겨야 하고 헬퍼가 늘 때마다 시그니처가 깨진다). 그렇다고 조회 로직을 DAG 파일에 두면 **공통 모듈이 DAG 파일을 import해야 해서 성립하지 않는다.** 별도 공통 모듈로 빼는 것도 파일만 늘 뿐 이득이 없다.

**`reprocess_select_jobs` 반환값과 처리 방법**

| 항목 | 무엇인가 | 호출부가 할 일 |
|------|---------|---------------|
| `to_done` | 영수증 확인으로 **이미 커밋이 확인된** 대상 | `_update_jobs(conn_id, pks, "SUCCESS")` — 재적재하지 않는다. `batch_id`는 넘기지 않아 기존 `stat_desc`(영수증)를 유지한다. **`files`가 비어 있어도 먼저 처리해야 한다** |
| `files` | 이번에 적재할 avro 파일 목록 | 기존 파일 목록 함수에 넘기고, **반환된 executor 개수를 XCom push** (Spark operator가 pull). **비었으면 `False` 반환 → short_circuit으로 하류 skip** |
| `to_mark` | 이번에 적재할 대상 | XCom에 먼저 남긴 뒤(update_failure 회수용) `_update_jobs(conn_id, pks, "IN_PROGRESS", batch_id)` |
| `batch_id` | 이번 배치의 영수증 값 | 위 마킹에 쓰고, Spark 쓰기 옵션 `option("snapshot-property.batch_id", batch_id)`에도 같은 값 |
| `ts_min`/`ts_max`/`has_more` | 적재 시간 범위와 잔여 여부 | `reprocess` 키로 XCom push — 재처리 DAG의 `compaction_targets`·`next_loop`가 가져간다 |

`to_done`/`to_mark`는 `{conn_id: [복합키 값 tuple, ...]}` 형태다 — 복합키 값은 DB 간 유일 보장이 없어 어느 DB에서 온 row인지가 UPDATE 대상을 결정한다.

기존 인프라(Oracle 커넥션/Hook, SparkKubernetesOperator 템플릿, 알림 채널, 테이블 설정 소스)에 연결해야 하는 지점은 파일 내 `TODO(연결):` 주석으로 표시되어 있다 (`grep "TODO(연결)"`). 구현 시 반드시 지켜야 하는 핵심 포인트:

| # | 포인트 | 설계 근거 |
|---|--------|----------|
| 1 | ShortCircuit task는 `ignore_downstream_trigger_rules=False` 필수 — 기본값이면 첫 skip에서 뒤 테이블 그룹 전체가 skip됨 | 5.2 |
| 2 | 조회 결과는 `{conn_id: rows}` dict로 보관 (row 태깅·재그룹핑 없음). 정렬 시에만 `(ts, row, conn_id)`로 펼친다 | 5.3 |
| 2-1 | 상태 UPDATE·파일 목록 처리는 **부모 ConvertFileTaskGroup의 기존 함수**를 쓴다. 재처리는 `{conn_id: [복합키 tuple]}`·`batch_id`·파일 목록을 만들어 넘길 뿐이다. `stat_desc`(CLOB)는 WHERE 조건 사용 금지 | 4.2 / 5.2 |
| 3 | 잔여분 판정은 영수증 필터 적용 **전** 조회 건수(ROW_LIMIT 도달) 기준 | 5.3 |
| 4 | XCom(meta) 기록 → IN_PROGRESS 마킹 → S3 업로드 순서 (마킹 중간 실패 시 좀비 방지) | 5.3 |
| 5 | loop 재trigger 조건 = 조회 상한을 채운 테이블 존재. 지속 실패는 MAX_LOOP(10회) 상한으로 유한 종료. 첫 회차가 확정한 조회 범위·tables를 conf로 승계 | 5.5 |
| 6 | 수동 `start_time`/`end_time`은 함께 지정 + `end_time ≤ 전날 00:00` (전날/당일 거부) — prepare_run에서 검증 | 5.1 |
| 7 | Compaction trigger는 적재분 전부, `tables` 필터 포함. conf 값은 대상 params 형식으로 변환해 넘긴다 — daily `date`, hourly `date-time` | 6.3 |


### 잔류 데이터 알림 쿼리 (별도 모니터링, 섹션 8.1)

```sql
-- DB 2개 각각에 실행. 3~7일 전을 하루 단위 ts 범위로 반복 조회 (Partition Pruning 유지)
SELECT table_name, status, COUNT(*) AS cnt
  FROM JOB_HISTORY
 WHERE ts >= :day_start AND ts < :day_end      -- 예: '20260701000000000' ~ '20260702000000000'
   AND status IN ('WAIT_SCHEDULING', 'FAILURE')
 GROUP BY table_name, status;
-- 파일 크기는 param(VARCHAR2 JSON) 안에 있어 단순 SUM이 불가하다.
-- 총 크기까지 보려면 JSON_VALUE(param, '$.<크기 키>' RETURNING NUMBER)로 꺼내 집계한다
-- (키 이름은 append DAG 파싱 로직과 맞출 것).
-- 결과 존재 시: 알림 → 수동 재처리 절차(8.3)
```
