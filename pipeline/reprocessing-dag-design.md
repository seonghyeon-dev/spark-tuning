# 재처리(Reprocessing) DAG 설계 가이드

## 문서 정보

| 항목 | 내용 |
|------|------|
| 작성 목적 | append DAG 조회 기간(최근 1일)에서 밀려난 WAIT 데이터와 FAILED 데이터를 회수하는 재처리 DAG 설계 |
| 대상 독자 | 데이터 엔지니어, 운영팀 |
| 환경 | Kubernetes 클러스터, S3(MinIO), Spark 4.1.1, Iceberg 1.10.1, Airflow 3.2.2, Oracle DB |
| 시간대 기준 | **KST (Asia/Seoul)** — 모든 날짜/시간 계산에 적용 |
| 최종 수정일 | 2026-07-14 |

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
| Job History (Oracle) | 처리 대상 상태 관리 테이블. `table_name`(대상 테이블), `ts`(string, `YYYYMMDDHHmmSSsss` 밀리세컨즈 — 날짜 파티셔닝 키이자 조회 기준), `status`, `stat_desc`(CLOB, 현재 미사용) 등 |
| append DAG | py 파일 1개에서 loop로 **테이블별 DAG 동적 생성** (테이블당 1개 실행). 약 5분 주기. `get_jobs`가 `table_name` 조건 + `ts` 최근 1일 범위 + `status='WAIT'`을 `ORDER BY ts ASC`, `ROWNUM <= 200`으로 조회 |
| Compaction DAG | **hourly DAG 1개**(`15 * * * *`, 직전 1시간치) + **daily DAG 1개**(현재 `35 0 * * *`, 전일치). 각 DAG 내부에서 소속 테이블 task가 순차 실행. `max_active_runs=1`. UI 수동 실행용 params: daily는 `target_dt`, hourly는 `start_time`/`end_time` |
| DB 상태 처리 | callback이 아닌 `update_success`(`trigger_rule=all_success`) / `update_failure`(`all_failed`) task 방식 — callback은 DB update 지연 시 작업이 kill되는 문제가 있었음 |

### 1.2 문제: 영구 잔류 데이터

append DAG의 조회 기간은 **실행 시각 기준 최근 1일**(rolling)이다. Job History가 `ts` 기반 날짜 키로 파티셔닝되어 있어 조회 기간을 늘리면 성능이 크게 저하되므로 이 제약은 유지해야 한다. 이 구조에서 두 종류의 데이터가 영구히 처리되지 않고 남는다.

| 케이스 | 발생 경로 |
|--------|----------|
| **WAIT 잔류** | append 처리 또는 DB 조회 지연으로 밀린 데이터가 최근 1일 조회 범위를 벗어남 → `get_jobs`가 다시는 조회하지 않음 |
| **FAILED 잔류** | `get_jobs`는 WAIT만 조회하므로, `update_failure`가 FAILED로 확정한 데이터는 재시도 주체가 없음 |

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
│   조회: ts ≥ 실행시각-24h, status='WAIT', ROWNUM ≤ 200        │
│   재시도 없음. 실패 건은 FAILED 확정 후 재처리 DAG에 위임        │
└──────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│ 재처리 DAG 1개 (1일 주기, 01:00 KST) — 잔류 데이터 회수 전담    │
│   테이블별 task 순차 실행 (Compaction DAG과 동일 패턴)          │
│   조회: FAILED(전날+그저께) + WAIT(append 범위 밖만)            │
│   테이블당 ROWNUM ≤ 1,000. 초과 시 자기 자신 재trigger(loop)    │
│   처리 후 기존 Compaction DAG trigger                          │
└──────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│ 수동 처리 (매일 모니터링) — 비정상 상황 전담                     │
│   그저께 이전 잔류·loop 상한 초과: 알림 → 원인 확인 →            │
│   UI에서 재처리 DAG을 테이블·시간 범위 지정 수동 실행            │
└──────────────────────────────────────────────────────────────┘
```

### 2.1 조회 범위 경계 — 겹침이 원천적으로 불가능한 이유

**핵심 규칙**: append는 항상 "실행 시각-24시간 이후"만 조회한다. 재처리의 WAIT 조회 상한을 **전날 01:00**(= 재처리 스케줄 시각 - 24시간)으로 잡으면, 재처리가 01:00 이후에 도는 한 append의 조회 하한은 항상 전날 01:00 이상이므로 **두 범위는 절대 겹치지 않는다.** FAILED는 append가 아예 조회하지 않으므로 겹침 걱정 없이 전날·그저께 전체를 잡는다.

14일 01:00 실행 기준 (`ts`는 `YYYYMMDDHHmmSSsss` 문자열 비교):

| 대상 | ts 범위 | 이유 |
|------|---------|------|
| FAILED | `20260712000000000` ≤ ts < `20260714000000000` (그저께 00:00 ~ 전날 끝) | append는 FAILED를 조회하지 않으므로 전 구간 안전 |
| WAIT | `20260712000000000` ≤ ts < `20260713010000000` (그저께 00:00 ~ 전날 01:00) | 전날 01:00 이후는 append가 아직 조회 가능한 영역 |
| 그저께 이전 | 조회 안 함 | **알림 → 수동 처리** (섹션 8) |

> **경계값은 실행 시각이 아니라 그 날의 01:00 고정값으로 계산한다.** 재실행이나 loop 회차가 늦게 돌아도 경계가 append 조회 하한보다 항상 과거이므로 안전이 유지된다.
>
> **그저께까지 조회하는 이유(안전망)**: 재처리가 하룻밤 통째로 실패하거나 상한으로 이월이 생겨도, 다음날 실행이 그저께 범위로 자동 회수한다. 이틀 연속 실패부터 수동 영역이다.
>
> **방어선**: 설계상 겹침이 없더라도, IN_PROGRESS 전환 UPDATE는 `WHERE status = ...` 조건을 포함한 원자적 전환으로 수행한다 (만약의 이중 실행에서도 한쪽만 성공).

**잔류 데이터 회수 타임라인**:

```
[FAILED] 13일 15:00 배치 실패 → 14일 01:00 재처리가 회수 (최대 하루 지연)

[WAIT]   13일 08:00 생성 후 계속 미처리
         14일 01:00  재처리: WAIT 상한이 13일 01:00 → 대상 아님 (아직 append 담당)
         14일 08:00  생성 24시간 경과 → append 조회 범위 이탈
         15일 01:00  재처리: WAIT 범위 [13일 00:00 ~ 14일 01:00) → 회수 ✅ (최대 약 2일 지연)
```

append가 `ORDER BY ts ASC`(오래된 것부터)로 소화하므로, 조회 범위 안의 WAIT가 하루 종일 안 집히는 상황 자체가 append 처리량 이상 신호다 — 이 경우는 잔류 알림(섹션 8.1)으로 드러난다.

### 2.2 검토했으나 채택하지 않은 대안

| 대안 | 미채택 사유 |
|------|------------|
| append DAG 조회를 상태 기준(시간 조건 제거)으로 변경 | `ts` 날짜 키 파티셔닝에서 전체 파티션 스캔 발생 → 조회 성능 저하 |
| append DAG 조회 기간 확장 (1일 → 7일) | 조회 성능 저하 + 밀린 과거 데이터가 최신 데이터 처리를 지연시킴 |
| 재처리 DAG을 테이블별로 동적 생성 (append 패턴) | 재처리는 하루 1회 청소 배치로 대부분 테이블이 no-op — DAG 20개+가 매일 빈 run을 쌓는 관리 소음. 단일 DAG + 테이블별 task(Compaction 패턴)가 관리에 유리하고, Compaction trigger도 한곳에서 날짜별로 묶어 1회씩 실행 가능 |
| 전날 WAIT 전체를 재처리가 가져가기 (UPDATE 선점 또는 Airflow pool로 경합 제어) | UPDATE 선점 후 재조회는 stat_desc(CLOB) 조건이 필요해 성능 문제. pool은 테이블 20개+ 구조에서 테이블별 pool 난립 또는 전역 병목. 조회 범위를 겹치지 않게 하는 것이 가장 단순 (전날 01:00 이후 WAIT는 다음날 회수 — 하루 지연 허용) |
| 한 DAG run 안에서 Spark task 여러 개로 분할 처리 | 일부 성공/일부 실패 시 `all_success`/`all_failed`가 모두 불충족되어 상태 update 누락. loop는 DAG 재trigger 방식으로 해결 (섹션 5.5) |
| 재처리 DAG에서 직접 `rewrite_data_files` 실행 | 기존 Compaction DAG과 동시 실행 시 Iceberg commit 충돌. 기존 DAG trigger로 `max_active_runs=1` 직렬화 활용 (섹션 6) |

---

## 3. Job History 상태 모델

### 3.1 상태 전이도

```
WAIT ──get_jobs(append DAG)──▶ IN_PROGRESS ──▶ update_success ──▶ DONE
                                    │
                                    └──▶ update_failure ──▶ FAILED
                                                              │
WAIT(append 범위 이탈분) ──┐                                   │
                          ├──재처리 DAG(전날+그저께)◀──────────┘
FAILED ───────────────────┘        │
                                   ├──(영수증 확인: 이미 커밋됨)──▶ DONE 정정
                                   └──▶ IN_PROGRESS ──▶ DONE / FAILED(다음날 재시도)
```

### 3.2 상태별 처리 주체

| 상태 | 생성 주체 | 소비 주체 |
|------|----------|----------|
| WAIT | 원천 시스템 | append DAG (최근 1일) / 재처리 DAG (append 범위 이탈분) / 수동 (그저께 이전) |
| IN_PROGRESS | get_jobs / 재처리 조회 task | update_success/update_failure. 임계 시간 초과 시 좀비 탐지 알림 (섹션 8.2) |
| DONE | update_success, 영수증 정정 | 최종 상태 |
| FAILED | update_failure | 재처리 DAG (전날+그저께) / 수동 (그저께 이전) |

### 3.3 재시도 정책

- **task 레벨 재시도**: Spark task의 Airflow `retries`(권장 2회, `retry_delay` 5분)가 일시적 오류(S3 순단 등)를 1차 방어
- **배치 레벨 재시도**: task 재시도 소진 후 FAILED 확정 → 다음날 01:00 재처리 DAG이 재시도. 별도 retry 카운트는 DB에 저장하지 않음 (재시도는 task가, 배치 재시도는 상태 재조회가 담당)
- **무한 재시도 방지**: 재처리에서도 반복 실패하는 건은 그저께 이전 잔류 알림(섹션 8.1)으로 사람에게 노출됨

> **FAILED 격리 효과**: append DAG이 FAILED를 재조회하지 않으므로, 깨진 파일이 섞인 배치가 5분마다 반복 실패하며 정상 신규 데이터까지 물고 늘어지는 상황이 구조적으로 발생하지 않는다.

---

## 4. 중복 적재 방지: batch_id 영수증 방식

### 4.1 왜 필요한가

Airflow의 "task 실패" 판정이 항상 "데이터 미적재"를 의미하지 않는다. Iceberg commit은 성공했으나 직후 Driver Pod 종료 오류, Operator-Pod 통신 단절, task timeout 등으로 Airflow가 실패로 판정하는 경우(**거짓 실패**)가 있다. 이 상태에서 FAILED 건을 기계적으로 재적재하면 **같은 데이터가 두 번 들어간다.**

```
13일 09:00  Spark job이 Iceberg commit 성공 (데이터 적재됨)
            → 직후 Pod 통신 오류 → Airflow는 실패 판정 → FAILED 기록
14일 01:00  재처리 DAG이 FAILED를 재적재 → 중복!
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
③ FAILED row를 집기 전, row에서 읽어온 stat_desc 값(batch_id)으로 확인:
   SELECT snapshot_id FROM db.TABLE_X.snapshots
    WHERE summary['batch_id'] = :batch_id
   → 결과 있음: 이미 커밋됨 → 재적재하지 않고 DONE으로 정정
   → 결과 없음: 진짜 실패 → 정상 재적재
```

> **stat_desc 사용 제약 (중요)**: stat_desc는 CLOB이므로 **Oracle WHERE 조건으로 사용하는 것은 금지**한다 (등호 비교·인덱스 불가). 허용되는 사용은 두 가지뿐이다 — ① UPDATE 시 값 기록 ② SELECT 결과에서 개별 row의 값 읽기. 영수증 확인은 "row에서 batch_id를 읽어 → Iceberg `.snapshots`를 조회"하는 방향이므로 이 제약에 걸리지 않는다.
>
> **판단 근거와 전제**: Iceberg commit은 원자적(all-or-nothing)이므로 snapshot에 batch_id가 존재한다 = 그 배치의 커밋이 완전히 성공했다는 뜻이며, "일부만 적재된" 중간 상태는 존재하지 않는다. 단 이 판단은 **배치 전체가 Spark job 1회의 단일 append 커밋**일 때만 성립한다 (현재 append job 구조는 충족 — job 내부 다중 커밋 구조로 변경 시 성립하지 않음).
>
> **task retry 중복 방어 (권장)**: Spark task의 Airflow `retries`는 같은 batch_id로 재실행된다. attempt 1이 커밋 성공 후 거짓 실패하면 attempt 2가 같은 데이터를 중복 append할 수 있다. 방어책으로 **Spark job 시작 시 자기 batch_id의 snapshot 존재를 확인하고, 있으면 즉시 성공 종료**하는 로직을 권장한다 (조회 1회 비용).

### 4.3 구현 비용 및 성능

| 항목 | 내용 |
|------|------|
| Oracle 변경 | 없음 (기존 `stat_desc` CLOB 컬럼 재사용 — 과거 Airflow log URL 용도, 현재 미사용) |
| Spark 변경 | write option 1줄 |
| `.snapshots` 조회 부하 | 없음. snapshot 목록은 테이블 metadata.json 파일 1개에 포함 — S3 GET 1회, manifest/데이터 파일 접근 없음. 3일 보존 기준 테이블당 snapshot 수백 개 수준, 실행 빈도는 하루 1회(FAILED 존재 테이블만) |

### 4.4 제약: snapshot 보존 기간

- snapshot 보존 정책: **3일** (`expire_snapshots`)
- 자동 재처리 범위(전날+그저께 = 2일) < 보존(3일) → 자동 경로에서는 영수증 확인이 항상 가능
- **3일을 넘긴 FAILED 건은 영수증이 expire되어 확인 불가** → 수동 처리 시 별도 검증 필요 (섹션 8.3)
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
| `tables` | 전체 테이블 | 처리 대상 테이블 multi-select — 1개/여러 개/전체 선택 가능. 정기 실행은 기본값(전체). 선택지·기본값은 append DAG과 동일한 **`iceberg.py`의 hourly/daily Enum 클래스**에서 생성 (`Param(type="array", items={"enum": [...]})`) — hourly/daily 분류는 소속 Enum 클래스로 결정되고, 테이블 추가/제거 시 Enum 한 곳만 수정하면 append/재처리가 함께 반영되는 단일 소스 |
| `start_time` / `end_time` | 없음 | **수동 실행 시 조회 범위를 직접 정의** (WAIT+FAILED 전체). 둘 다 함께 지정해야 하며, `end_time ≤ 전날 00:00`만 허용 — 전날/당일은 append 조회 범위와 겹치므로 prepare_run이 검증 후 거부. 기존 DAG과 동일한 date-time 형식 → 내부에서 ts 문자열(`YYYYMMDDHHmmSSsss`)로 변환. 미지정 시 정기 범위(그저께 00:00 ~ 전날 끝) |

### 5.2 Task 구성

```
check_zombie_jobs                          # 좀비 IN_PROGRESS 탐지 → 알림 (독립 실행)
                                           # 관측용 — 본 파이프라인과 의존 없음.
                                           # 알림 실패가 재처리 본류를 막지 않도록 분리

prepare_run                                # params 검증·정규화 1회 → XCom (get_time 패턴)
      │                                    # ts 경계 계산, 수동 범위 검증, date-time → ts 변환
┌─ ConvertFileTaskGroup: TABLE_A (기존 append DAG 템플릿 재사용) ─┐
│  get_jobs            # ← 재처리용 조회 task만 주입             │
│      │               # (범위·상한·영수증 확인. 대상 0건→skip)  │
│  append_data         # 템플릿 제공 (Spark append)              │
│      ├── update_success  [all_success]  # 템플릿 제공          │
│      └── update_failure  [all_failed]   # 템플릿 제공          │
└────────────────────────────────────────────────────────────────┘
      │  (다음 그룹 첫 task는 trigger_rule=all_done —
      │   앞 테이블 실패가 뒤 테이블 처리를 막지 않음)
┌─ TaskGroup: TABLE_B ─┐ ... (테이블 수만큼 순차)
└──────────────────────┘
      │
compaction_targets [all_done] → trigger_compaction  # 집계 → TriggerDagRunOperator.expand (섹션 6)
next_loop          [all_done] → retrigger_self       # 잔여분 판단 → 자기 재trigger (0/1건, 5.5)
```

> Compaction/재trigger는 대상 개수가 가변(Compaction 여러 건, loop 0/1건)이므로, 집계 task가 `TriggerDagRunOperator` kwargs 목록을 만들고 **dynamic task mapping(`expand_kwargs`)** 으로 trigger한다. 빈 목록이면 mapped operator는 skip된다.

- **기존 ConvertFileTaskGroup 재사용**: append DAG이 쓰는 TaskGroup 템플릿(get_jobs → Spark append → update_success/update_failure)을 그대로 재사용하고, **조회 task(get_jobs)만 재처리용으로 주입**한다. 템플릿에 재처리 로직을 추가하지 않는다 — 재처리와 append의 차이는 조회 범위·상한·영수증 확인뿐이고 이후 단계는 동일하기 때문
- **테이블별 순차 실행**: Spark job(최대 24 executor)이 테이블 수만큼 동시에 뜨면 K8S가 감당하지 못한다. Compaction DAG과 동일하게 순차 — 잔여분 없는 테이블은 조회 후 즉시 skip이라 빠르다
- **상태 update는 그룹 내부에서만**: `all_success`/`all_failed`가 각 테이블 자신의 Spark task에만 걸리므로, 테이블 간 부분 실패로 상태 update가 누락되는 구멍이 없다
- **skip 전파 차단 (구현 주의)**: ShortCircuit의 기본 동작은 trigger_rule을 무시하고 **모든 하류 task를 재귀적으로 skip**시킨다. 기본값 그대로면 잔여분 없는 첫 테이블이 skip되는 순간 뒤 테이블 그룹 전체가 skip된다. 반드시 `ignore_downstream_trigger_rules=False`로 설정해 skip을 그룹 내 직계 하류로 한정한다 (`trigger_rule=all_done`인 다음 그룹/집계 task는 정상 실행)
- **상태 update의 대상 식별은 XCom의 job_id 목록으로만**: `stat_desc`(batch_id)는 CLOB이라 WHERE 조건 사용 금지 (섹션 4.2 제약). update_success/update_failure는 get_table_jobs가 XCom에 남긴 job_id 목록으로 UPDATE한다
- **params는 prepare_run에서만 읽는다**: 기존 append DAG의 get_time 패턴과 동일. 검증·형식 변환(`ts` 경계 계산, 수동 범위 검증, date-time → ts 문자열 변환)을 첫 task에서 1회 수행하고, 이후 task들은 정규화된 XCom 값만 소비한다. 잘못된 입력은 파이프라인 중간이 아닌 첫 task에서 즉시 실패하고, TaskGroup 템플릿이 DAG params에 결합되지 않아 재사용이 가능해진다

### 5.3 get_table_jobs 처리 순서 (테이블별)

선행 task `prepare_run`이 params 검증과 `ts` 경계 계산을 1회 수행해 XCom으로 내려보내며(수동 범위 검증, date-time → ts 문자열 변환 포함), get_table_jobs는 정규화된 값만 사용한다.

1. **실행 대상 확인** — 정규화된 `tables` 목록에 자기 테이블이 없으면 즉시 skip
2. **대상 조회** — `ts` 범위 조건(파티션 키 → Partition Pruning 유지) + row 수 상한:

```sql
SELECT * FROM (
    SELECT job_id, status, stat_desc, avro_path, file_size_mb
      FROM JOB_HISTORY
     WHERE table_name = :tbl
       AND ts >= :d2_start          -- 그저께 00:00  '20260712000000000'
       AND ts <  :d1_end            -- 전날 끝       '20260714000000000'
       AND ( status = 'FAILED'                                   -- FAILED: 전 구간
             OR (status = 'WAIT' AND ts < :wait_bound)           -- WAIT: 전날 01:00 이전만
           )
     ORDER BY ts ASC                -- append와 동일, 오래된 것부터
) WHERE ROWNUM <= 1000
```
   - 수동 실행 시: prepare_run이 검증한 `start_time`~`end_time` 범위의 WAIT+FAILED 전체
   - 조회 직후 **필터 적용 전 조회 건수로 잔여분 여부를 기록** (1,000건을 채웠으면 더 남았다는 뜻 — loop 판단은 이 시점 값 기준. 영수증/크기 상한으로 줄어든 후의 건수로 판단하면 잔여분을 놓친다)
3. **영수증 확인** — FAILED row에서 읽은 stat_desc(batch_id)별로 `.snapshots` 조회 → 이미 커밋된 batch의 row는 DONE 정정 후 대상에서 제외 (섹션 4)
4. **크기 상한 적용** — 총 avro 크기 16GB 초과분은 잘라냄. 잘린 건도 잔여분으로 기록 (loop 회차 또는 다음날 회수)
5. **IN_PROGRESS 마킹** — `WHERE job_id IN (...) AND status IN ('WAIT','FAILED')` 원자적 UPDATE + `stat_desc = 새 batch_id` 기록
6. **XCom 기록 (마킹 직후, S3 업로드 전)** — job_id 목록·batch_id·잔여분 여부·적재 ts 범위를 먼저 XCom에 남긴다. 이후 단계(S3 업로드 등)에서 task가 실패해도 update_failure가 XCom의 job_id 목록으로 마킹된 row를 FAILED로 회수할 수 있어 좀비 IN_PROGRESS 발생 경로가 줄어든다
7. **Spark 입력 준비** — avro 경로 목록 S3 업로드, num_executors 산정 (append DAG과 동일 산정식: `ceil(총크기/128MB × 1.5 / 4)`, 상한 24)

### 5.4 처리 상한

| 항목 | 값 | 보호 대상 | 근거 수준 |
|------|-----|----------|----------|
| 테이블당 조회 row 수 | 1,000 (ROWNUM) | Oracle SELECT 성능, XCom 크기, avro 경로 목록 파일 크기 | ⚠️ 러프 설정 — 재검증 필요 |
| 테이블당 처리 총 크기 | 16GB | Spark 리소스, 처리 소요시간. 벤치마크 검증 범위(~8GB, 24 executors)의 2배 이내 | ⚠️ 러프 설정 — 재검증 필요 |
| num_executors | 24 | 벤치마크에서 32 이상은 성능 저하 확인 (spark-tuning-guide.md 2.2.3) | ✅ 벤치마크 검증 |

**규모 감각**: append는 약 5분 주기에 조회 상한 200 rows(5분치 유입 ≈ 200 rows). 재처리 상한 1,000 rows ≈ 약 25분치 물량. 정상 운영의 하루 잔여분은 이보다 훨씬 적을 것으로 예상하지만, 상한값은 검증된 값이 아니므로 운영 데이터로 재조정한다.

> **K8S 리소스 경합 주의**: 재처리 Spark job(최대 24 executor, 96 core, ~213GB)이 도는 동안에도 약 5분 주기 append job이 뜬다. 동시 실행 시 최대 **~192 core, ~427GB**. 클러스터 여유가 부족하면 재처리 job의 executor 상한을 낮춘다(예: 12 — 지연 데이터이므로 처리 속도의 우선순위가 낮음).

### 5.5 잔여분 loop: 자기 자신 재trigger

한 DAG run 안에서 Spark task를 여러 번 도는 대신, **잔여분이 남았으면 같은 DAG을 한 번 더 trigger**한다.

```
run N: 테이블별 최대 1,000건 처리
       → 잔여분이 있는 테이블 중 이번 회차 Spark가 성공한 테이블이 하나라도 있으면
         자기 자신을 trigger (첫 회차가 확정한 조회 범위·tables·loop_count를 conf로 승계)
run N+1: 동일 파이프라인 반복. 남은 게 없는 테이블은 조회 후 즉시 skip
종료: 재trigger 조건을 만족하는 테이블 없음 → trigger 안 함
```

- **재trigger 조건 = 잔여분 존재 AND 해당 테이블의 이번 회차 Spark 성공.** 실패한 테이블은 조건에서 제외한다 — 실패한 row는 FAILED로 돌아가 있고 `ORDER BY ts ASC`라 다음 회차가 같은 row를 다시 집게 되므로, 성공 조건 없이 돌리면 깨진 데이터를 같은 밤에 loop 상한까지 반복 재시도하게 된다. 실패분은 다음날 정기 실행이 회수한다
- 잔여분 판정은 **필터 전 조회 건수(1,000건 도달) 또는 크기 상한 이월 발생** 기준 (섹션 5.3)
- `max_active_runs=1`이므로 회차는 자동으로 순차 실행된다
- 매 회차가 동일한 단순 파이프라인 — Spark task당 자기 상태 update가 붙어 있어 부분 실패 문제가 없다
- **첫 회차가 확정한 조회 범위·tables를 conf로 승계**: prepare_run은 conf에 범위가 있으면 재계산하지 않고 그대로 사용한다. 수동 실행의 선택 값이 회차에서 유실되지 않고, 회차가 자정을 넘겨 실행되어도 ts 경계가 첫 회차 기준으로 유지된다
- **폭주 방지**: `loop_count` 상한 10회 (≈ 테이블당 최대 1만 건/일). 도달 시 알림 → 수동 전환

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

| DAG | params |
|-----|--------|
| daily | `target_dt` + `tables` |
| hourly | `start_time`, `end_time` + `tables` |

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
| append DAG (테이블별 공통 py) | batch_id 기록 2건 | ① `get_jobs`의 IN_PROGRESS 마킹 UPDATE에 `stat_desc = :batch_id` 추가 ② Spark 쓰기에 `option("snapshot-property.batch_id", batch_id)` 추가 |
| daily Compaction DAG | 스케줄 + params | `35 0 * * *` → `0 2 * * *`. params에 `tables` multi-select 추가 (기본 전체) |
| hourly Compaction DAG | params | params에 `tables` multi-select 추가 (기본 전체). 스케줄 변경 없음 |

append DAG의 조회 로직(최근 1일, WAIT만, ts ASC, ROWNUM 200)과 update task 구조는 **변경 없음**.

---

## 8. 모니터링 및 수동 처리 절차

### 8.1 잔류 데이터 알림 (일 1회)

| 대상 | 조건 | 대응 |
|------|------|------|
| 자동 재처리 범위 초과 | 그저께 이전(3~7일 전) `ts` 범위에 `WAIT` 또는 `FAILED` 존재 | 원인 확인 → 재처리 DAG 수동 실행 (8.3) |
| loop 상한 도달 | `loop_count` 10회 초과 | append DAG 장애 등 대량 밀림 상황 → 원인 확인 후 수동 판단 |

> 잔류 알림 쿼리도 하루 단위 `ts` 범위 조회를 날짜별로 반복한다 (Partition Pruning 유지). 잔류가 매일 꾸준히 증가하면 스케줄링 문제가 아니라 **처리량 부족(capacity)** 신호 — append 조회 상한(200/5분)이 유입량과 같은 수준이므로 리소스 증설/주기/상한 조정을 검토한다.

### 8.2 좀비 IN_PROGRESS 탐지

get_jobs가 IN_PROGRESS로 전환한 후 DAG run이 증발하면(scheduler 장애, worker 강제 종료 — update task 2개 모두 미실행) 해당 row는 어느 DAG도 집지 않는다.

- 재처리 DAG 선행 task(`check_zombie_jobs`)가 임계 시간(2시간, 정상 처리 수 분 대비 충분한 여유) 초과 IN_PROGRESS를 전체 테이블 대상으로 탐지해 **알림만** 발송한다
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
| 8 | 처리 상한 재검증 | 테이블당 row 1,000 / 16GB / loop 10회는 러프 설정 — 운영 데이터로 재조정 |

---

## 10. DAG 구현 파일

구현 스켈레톤: **`pipeline/dags/iceberg_reprocess.py`**

기존 인프라(Oracle 커넥션/Hook, SparkKubernetesOperator 템플릿, 알림 채널, 테이블 설정 소스)에 연결해야 하는 지점은 파일 내 `TODO:` 주석으로 표시되어 있다. 구현 시 반드시 지켜야 하는 핵심 포인트:

| # | 포인트 | 설계 근거 |
|---|--------|----------|
| 1 | ShortCircuit task는 `ignore_downstream_trigger_rules=False` 필수 — 기본값이면 첫 skip에서 뒤 테이블 그룹 전체가 skip됨 | 5.2 |
| 2 | 상태 UPDATE는 XCom의 `job_ids` 목록으로만. `stat_desc`(CLOB)는 WHERE 조건 사용 금지 | 4.2 |
| 3 | 잔여분 판정은 필터 적용 전 조회 건수(ROW_LIMIT 도달) + 크기 상한 이월 기준 | 5.3 |
| 4 | IN_PROGRESS 마킹 직후 XCom 먼저 기록, S3 업로드는 그 다음 | 5.3 |
| 5 | loop 재trigger 조건 = 잔여분 존재 AND 해당 테이블 Spark 성공. 첫 회차가 확정한 조회 범위·tables를 conf로 승계 | 5.5 |
| 6 | 수동 `start_time`/`end_time`은 함께 지정 + `end_time ≤ 전날 00:00` (전날/당일 거부) — prepare_run에서 검증 | 5.1 |
| 7 | Compaction trigger는 적재분 전부, `tables` 필터 포함. 날짜/시간 형식은 기존 Compaction DAG params 형식과 일치 확인 | 6.3 |


### 잔류 데이터 알림 쿼리 (별도 모니터링, 섹션 8.1)

```sql
-- 3~7일 전을 하루 단위 ts 범위로 반복 조회 (Partition Pruning 유지)
SELECT table_name, status, COUNT(*) AS cnt, SUM(file_size_mb) AS total_mb
  FROM JOB_HISTORY
 WHERE ts >= :day_start AND ts < :day_end      -- 예: '20260711000000000' ~ '20260712000000000'
   AND status IN ('WAIT', 'FAILED')
 GROUP BY table_name, status;
-- 결과 존재 시: 알림 → 수동 재처리 절차(8.3)
```
