# Iceberg 스키마 설계 가이드

## 문서 정보

| 항목 | 내용 |
|------|------|
| 작성 목적 | Iceberg 테이블의 파티션, 정렬, 스키마 설계에 대한 근거 기반 가이드 |
| 대상 독자 | 데이터 엔지니어, 운영팀 |
| 환경 | Kubernetes 클러스터, S3(MinIO), Spark 4.1.1, Iceberg 1.10.1, Airflow 3.1.7 |
| 최종 수정일 | 2026-03-17 |

### 근거 수준 라벨

| 라벨 | 의미 |
|------|------|
| ✅ 벤치마크 검증 | 실제 환경에서 벤치마크로 검증된 값 |
| 📘 일반적 관행 | 커뮤니티에서 널리 사용되는 값. 공식 문서 또는 업계 관행 기반 |
| ⚠️ 개선 필요 | 근거 부족. 벤치마크/프로파일링으로 재검증 필요 |

### 목차

- [1. 개요](#1-개요) — 대상 테이블, 조회 패턴
- [2. 파티션 설계](#2-파티션-설계) — Hidden Partitioning, 트랜스폼, 설계 기준
- [3. Write Ordering 설계](#3-write-ordering-설계) — 정렬 효과, distribution-mode, 트레이드오프
- [4. 버킷팅](#4-버킷팅) — bucket 트랜스폼, 적용 시나리오
- [5. Z-ordering](#5-z-ordering) — 다차원 정렬, 비교, 적용 검토
- [6. 추가 스키마 설계 고려사항](#6-추가-스키마-설계-고려사항) — 데이터 타입, Nullable, Column Metrics, Bloom Filter, 테이블 속성
- [7. 설계 결정 요약](#7-설계-결정-요약) — 의사결정 매트릭스, DDL 예시
- [8. 용어집](#8-용어집)
- [9. 참고 자료](#9-참고-자료)
- [부록 A. 설계 검증용 쿼리 및 실제 데이터 분석](#부록-a-설계-검증용-쿼리-및-실제-데이터-분석) — 카디널리티 확인, 조회 패턴 분석, 판단 매트릭스

---

## 1. 개요

### 1.1 대상 테이블 스키마

> **참고**: 테이블명과 컬럼명은 도메인 정보이므로 익명화된 이름을 사용한다.

**테이블: TABLE_A**

| 항목 | 값 |
|------|----|
| 컬럼 수 | 약 19개 |
| 데이터 타입 | string, double, integer, array, timestamp_ntz 등 |
| 배치 주기 | 10분 |
| 1회 적재량 | ~8GB (avro), ~6.5GB (Parquet/Iceberg) |
| 일 적재량 | ~144회 × 6.5GB ≈ ~936GB |

**현재 설정**

| 항목 | 설정 |
|------|------|
| 파티션 | `day(ts)`, `par_a`, `par_b` |
| Write Ordering | `sort_a`, `sort_b`, `sort_c` ASC NULLS FIRST |
| `write.distribution-mode` | `range` |

**컬럼 관계 매핑**

> 마스킹된 컬럼명 중 일부는 동일 컬럼이 파티션 키와 Write Ordering에 중복 사용된다.

| 마스킹명 | 용도 | 비고 |
|---------|------|------|
| par_b | 파티션 키 | = sort_b (동일 컬럼) |
| sort_a | Write Ordering 1순위 | WHERE 절 등가 조건 빈번 |
| sort_b | Write Ordering 2순위 (기존) | = par_b, 파티션 프루닝으로 대체 → Write Ordering에서 제외 검토 |
| sort_c | Write Ordering 3순위 | WHERE 절 등가 조건 |

### 1.2 조회 패턴

TABLE_A의 주요 조회는 **다차원 필터** 패턴이다:

```sql
-- 패턴 1: 시간 + 비즈니스 키 조합
SELECT * FROM TABLE_A
WHERE ts >= '2026-03-15' AND ts < '2026-03-16'
  AND par_a = 'value1'
  AND par_b = 'value2';

-- 패턴 2: 시간 + 비즈니스 키 + 추가 필터
SELECT * FROM TABLE_A
WHERE ts >= '2026-03-15' AND ts < '2026-03-16'
  AND par_a = 'value1'
  AND sort_a = 'filter_value';
```

스키마 설계의 핵심 목표는 이러한 다차원 필터 조회에서 **불필요한 파일 읽기를 최소화**(data skipping)하는 것이다.

---

## 2. 파티션 설계

### 2.1 Iceberg Hidden Partitioning 개념

Iceberg는 **Hidden Partitioning**을 사용한다. Hive 파티셔닝과의 핵심 차이:

| 구분 | Hive 파티셔닝 | Iceberg Hidden Partitioning |
|------|--------------|---------------------------|
| 파티션 컬럼 | 별도 컬럼으로 스키마에 노출 | 원본 컬럼에 트랜스폼 적용, 스키마 변경 없음 |
| 쿼리 작성 | 파티션 컬럼을 직접 지정해야 함 (`dt='2026-03-15'`) | 원본 컬럼으로 쿼리 (`ts >= '2026-03-15'`), Iceberg가 자동 변환 |
| 파티션 변경 | 테이블 재생성 + 데이터 재적재 필요 | **Partition Evolution**: 메타데이터만 변경, 기존 데이터 유지 |
| 사용자 실수 | 파티션 컬럼 누락 시 full scan | 자동 파티션 프루닝, 실수 방지 |

**핵심 장점**: 사용자는 `ts` 컬럼으로 쿼리하면 되고, Iceberg가 내부적으로 `day(ts)` 파티션을 이용해 불필요한 파일을 건너뛴다.

```sql
-- Hive: 사용자가 파티션 컬럼(dt)을 알고 직접 지정해야 함
SELECT * FROM table WHERE dt = '2026-03-15';

-- Iceberg: 원본 컬럼(ts)으로 쿼리, 자동 파티션 프루닝
SELECT * FROM table WHERE ts >= '2026-03-15' AND ts < '2026-03-16';
```

### 2.2 파티션 트랜스폼 종류 및 선택 기준

Iceberg가 제공하는 파티션 트랜스폼:

| 트랜스폼 | 입력 타입 | 결과 | 설명 | 적합한 경우 |
|----------|----------|------|------|-----------|
| `identity` | 모든 타입 | 원본 값 그대로 | 원본 값 = 파티션 값 | 카디널리티가 낮은 컬럼 (수십 이하) |
| `day(col)` | timestamp, date | 날짜 (정수) | 일 단위 파티션 | 일 단위 시계열 조회 |
| `month(col)` | timestamp, date | 월 (정수) | 월 단위 파티션 | 월 단위 조회, 일 단위 대비 파티션 수 감소 |
| `year(col)` | timestamp, date | 연도 (정수) | 연 단위 파티션 | 장기 보관 데이터, 연 단위 조회 |
| `hour(col)` | timestamp | 시간 (정수) | 시 단위 파티션 | 실시간/스트리밍, 시간 단위 세밀 조회 |
| `bucket(N, col)` | 모든 타입 | 0~N-1 해시값 | N개 버킷으로 해시 분배 | 고카디널리티 + 등가 조건 조회 |
| `truncate(W, col)` | string, int, long, decimal | 잘린 값 | 문자열은 앞 W자, 숫자는 W 단위 절삭 | 접두사/범위 기반 조회 |
| `void` | 모든 타입 | 항상 null | 파티션 비활성화 | Partition Evolution 시 기존 파티션 제거 |

### 2.3 파티션 설계 판단 기준

파티션 설계 시 고려해야 할 3가지 핵심 기준:

**기준 1: 파티션당 데이터 크기** 📘 일반적 관행

| 항목 | 권장 |
|------|------|
| 파티션당 최소 데이터 | 100MB 이상 |
| 파티션당 최적 데이터 | 수백 MB ~ 수 GB |
| small 파일 위험 기준 | 파티션당 100MB 미만이 다수 발생 |

파티션이 지나치게 세분화되면 파일 수가 급증하여 **small 파일 문제**가 발생한다. 메타데이터 오버헤드 증가, 파일 열기/닫기 비용 증가로 읽기·쓰기 성능이 모두 저하된다.

**기준 1-1: 배치 빈도와 파티션 조합의 곱셈 효과** 📘 일반적 관행

10분 배치 환경에서는 파티션 수가 파일 수에 **배치 횟수만큼 곱해진다**:

```
일일 파일 수 = 배치 횟수/일 × 배치당 파티션 조합 수 × 파티션당 파일 수

TABLE_A 예시 (컴팩션 전):
  배치 횟수: 144회/일
  파티션 조합: day(1) × par_a(?) × par_b(?) = ?개
  ─────────────────────────────────────────────────
  par_a=5, par_b=3 → 15 조합 → 144 × 15 = 2,160 파일/일
  par_a=20, par_b=10 → 200 조합 → 144 × 200 = 28,800 파일/일 ← ⚠️ 위험
  par_a=50, par_b=20 → 1,000 조합 → 144 × 1,000 = 144,000 파일/일 ← ❌ 운영 불가
```

**파일 수 임계값 기준** (S3 + Iceberg 메타데이터 기준):

| 일일 파일 수 | 수준 | 영향 |
|-------------|------|------|
| ~5,000 이하 | ✅ 안전 | 메타데이터 부하 미미 |
| 5,000~30,000 | ⚠️ 주의 | 시간당 컴팩션 권장. 쿼리 플래닝 시간 증가 시작 |
| 30,000 이상 | ❌ 위험 | 필수 컴팩션 + 파티션 키 재설계 검토. S3 ListObjects 비용 급증 |

**기준 2: 쿼리 패턴과의 정합성**

| 조건 | 파티션 효과 |
|------|-----------|
| WHERE 절에 파티션 컬럼 포함 | ✅ 파티션 프루닝으로 scan 범위 축소 |
| WHERE 절에 파티션 컬럼 미포함 | ❌ 프루닝 불가, 전체 파티션 scan |

**파티션 키는 WHERE 절에 가장 자주 등장하는 컬럼**이어야 한다.

**기준 3: 카디널리티**

| 카디널리티 | 권장 트랜스폼 | 예시 |
|-----------|-------------|------|
| 매우 낮음 (2~10) | `identity` | status, region |
| 중간 (수십~수백) | `identity` 또는 `bucket` | category, department |
| 높음 (수천 이상) | `bucket(N)` 또는 `truncate(W)` | user_id, order_id |
| 시계열 | `day`/`month`/`hour` | timestamp 컬럼 |

### 2.4 TABLE_A 적용: day(ts), par_a, par_b 근거

| 파티션 | 트랜스폼 | 근거 수준 | 근거 |
|--------|---------|-----------|------|
| `day(ts)` | 시간 트랜스폼 | 📘 일반적 관행 | 모든 조회에 시간 범위 조건 포함. 10분 배치로 일 적재량 ~936GB이므로 `day` 단위가 적정 |
| `par_a` | identity | ⚠️ 개선 필요 | 대부분의 조회에서 등가 조건으로 사용. 카디널리티에 따라 `bucket` 검토 필요 |
| `par_b` | identity | ⚠️ 개선 필요 | par_a와 조합하여 조회. 카디널리티에 따라 `bucket` 검토 필요 |

**day(ts) 선택 이유**

```
hour(ts) → 파티션 수: 24개/일 × 365일 = 8,760개/년 → 과다
day(ts)  → 파티션 수: 365개/년 → ✅ 적정
month(ts)→ 파티션 수: 12개/년 → 월 적재량 ~28TB, 프루닝 효과 부족
```

`day(ts)`는 일 단위 조회 패턴에 정합하며, 파티션당 데이터 크기도 적정하다.

**par_a, par_b: 확인 필요 사항**

| 확인 항목 | 판단 기준 |
|----------|----------|
| 카디널리티 | 수십 이하 → `identity` 유지, 수백 이상 → `bucket(N)` 검토 |
| 파티션당 데이터 크기 | `day × par_a × par_b` 조합별 100MB 이상인지 확인 |
| 빈 파티션 비율 | 조합 중 데이터가 없는 파티션이 많으면 파티션 키 축소 검토 |

> ⚠️ **검증 필요**
> 하루치 데이터 적재 후 `day × par_a × par_b` 조합별 파일 수와 크기 분포를 확인하여 small 파일 문제 여부를 판단해야 한다.

---

## 3. Write Ordering 설계

### 3.0 Write Ordering이 필요한가?

Write Ordering은 쓰기 비용(shuffle)을 지불하고 읽기 성능(data skipping)을 얻는 트레이드오프이다. **모든 테이블에 필요한 것은 아니다.**

| 질문 | 예 → | 아니오 → |
|------|------|---------|
| 파티션 프루닝만으로 충분한가? (파티션 조건으로 대상 파일이 충분히 줄어드는가) | Write Ordering 불필요 | 다음 질문으로 |
| 파티션 프루닝 후에도 파일 수가 많고, WHERE 절에 파티션 외 컬럼이 자주 사용되는가? | **Write Ordering 필요** | Write Ordering 불필요 |
| 쓰기 후 조회가 거의 없는가? (적재 전용, 조회는 다른 시스템에서) | Write Ordering 불필요 | **Write Ordering 필요** |

**TABLE_A 판단**: 파티션 프루닝(`day(ts)` + `par_a` + `par_b`) 후에도 다차원 필터(`sort_a`, `sort_b`, `sort_c`)로 추가 필터링이 필요하며, 반복 조회가 발생한다. → **Write Ordering 필요** ✅

> **Write Ordering 미사용 시**: `write.distribution-mode`는 `hash`(파티션 격리만) 또는 `none`(shuffle 제거)으로 설정한다. 이 경우 shuffle 비용(9.2GiB)이 사라져 쓰기가 빨라지지만, 파티션 내 파일 간 데이터가 무작위 분포하여 data skipping이 사실상 불가능하다.

### 3.1 개념 및 효과

Write Ordering은 Iceberg가 데이터 파일을 쓸 때 **파일 내 데이터를 정렬**하는 기능이다. 정렬의 핵심 효과는 **Data Skipping**이다.

```
정렬되지 않은 파일:           정렬된 파일:
┌──────────────────┐         ┌──────────────────┐
│ sort_a: A,C,B,A│         │ sort_a: A,A,B,C│
│ min=A, max=C     │         │ min=A, max=C     │
└──────────────────┘         └──────────────────┘
┌──────────────────┐         ┌──────────────────┐
│ sort_a: B,A,C,B│         │ sort_a: D,D,E,F│
│ min=A, max=C     │         │ min=D, max=F     │
└──────────────────┘         └──────────────────┘

WHERE sort_a = 'D':
정렬 안 됨 → 2개 파일 모두 읽음 (min/max 범위 겹침)
정렬 됨   → 1개 파일만 읽음 (첫 번째 파일 skip)
```

Iceberg는 각 데이터 파일의 컬럼별 min/max 통계를 메타데이터에 저장한다. 쿼리 시 이 통계를 이용해 조건에 해당하지 않는 파일을 건너뛴다. 데이터가 정렬되어 있으면 min/max 범위가 좁아져 **skipping 효과가 극대화**된다.

### 3.2 write.distribution-mode 관계

Write Ordering과 `write.distribution-mode`는 함께 동작한다:

| 모드 | 동작 | Shuffle | 적합한 경우 |
|------|------|---------|-----------|
| `none` | 재분배 없이 각 태스크가 받은 데이터를 그대로 씀 | 없음 | 정렬 불필요, 쓰기 성능 최우선 |
| `hash` | 파티션 키 기준 해시 분배. 같은 파티션 데이터가 같은 태스크로 | 있음 (경량) | 파티션 프루닝만 필요, 정렬 불필요 |
| `range` | 파티션 키 + write ordering 기준 범위 분배 + 정렬 | 있음 (무거움) | Data skipping 극대화. 읽기 성능 중시 |

**hash vs range의 핵심 차이: 글로벌 정렬 보장 여부**

`hash` 모드에서도 write ordering을 설정하면 각 태스크가 자기 데이터를 정렬한다. 그러나 **파일 간 값 범위가 겹칠 수 있다**:

```
hash 모드 + WRITE ORDERED BY sort_a:
  Task 1 → 파일 1: sort_a = [A, C, F, H]  (min=A, max=H)
  Task 2 → 파일 2: sort_a = [B, D, G, I]  (min=B, max=I)
  → 파일 내 정렬은 됨, 파일 간 min/max 범위 겹침 → data skipping 효과 제한

range 모드 + WRITE ORDERED BY sort_a:
  Task 1 → 파일 1: sort_a = [A, B, C, D]  (min=A, max=D)
  Task 2 → 파일 2: sort_a = [E, F, G, H]  (min=E, max=H)
  → 파일 간 범위가 겹치지 않음 → WHERE sort_a = 'F' 시 파일 1 완전 skip
```

| 항목 | `hash` + ordering | `range` + ordering |
|------|-------------------|-------------------|
| 파일 내 정렬 | ✅ 보장 | ✅ 보장 |
| **파일 간 범위 분리** | ❌ 보장 안 됨 | ✅ 보장 (글로벌 정렬) |
| Data Skipping 효과 | 부분적 (범위 겹침으로 감소) | **최대** (범위 분리로 극대화) |
| Shuffle 비용 | 낮음 (해시 분배만) | 높음 (범위 샘플링 + 정렬) |

> **실무 판단**: Write Ordering을 설정했다면 대부분 `range`를 사용해야 의미가 있다. `hash` + ordering 조합은 파일 내 정렬만 보장하므로 data skipping 효과가 크게 줄어든다.

**Shuffle 비용 비교**

```
none  → Shuffle 없음. 쓰기 가장 빠름. 파일 내 정렬 보장 안 됨
hash  → 파티션 키 기준 shuffle. 파일 내 정렬되나 파일 간 범위 겹침
range → 파티션 키 + sort 키 기준 range shuffle. 가장 무거움 (TABLE_A: 9.2GiB)
        범위 샘플링(reservoir sampling) 단계가 추가됨
```

### 3.3 설계 판단 기준: 쓰기 성능 vs 읽기 성능

| 항목 | 쓰기 최적화 (`none`) | 읽기 최적화 (`range` + ordering) |
|------|---------------------|-------------------------------|
| Shuffle 비용 | 없음 | 높음 (데이터 크기에 비례) |
| 파일 내 정렬 | 보장 안 됨 | 보장됨 |
| Data Skipping | 효과 낮음 | 효과 높음 |
| small 파일 문제 | 태스크 수만큼 파일 생성 가능 | 파티션별 파일 수 제어 가능 |
| 적합한 워크로드 | 쓰기 빈도 높고 읽기 적음 | 쓰기 후 반복 조회가 많음 |

**판단 기준**: 컴팩션 운영 여부와 조회 빈도에 따라 전략이 달라진다.

| 상황 | 권장 | 이유 |
|------|------|------|
| 쓰기만 하고 거의 안 읽음 | `none` | shuffle 비용 제거. 정렬 불필요 |
| 컴팩션 운영 예정 + 쓰기 레이턴시 중요 | `hash` | 파티션 격리만 보장. 정렬은 컴팩션에서 |
| 컴팩션 미운영 + 읽기 성능 중요 | `range` + ordering | 쓰기 단계에서 글로벌 정렬 보장 |
| 쓰기·읽기 모두 중요 (TABLE_A) | `range` + ordering + 컴팩션 | 쓰기 시 기본 정렬 + 컴팩션 시 small 파일 병합·재정렬 |

**TABLE_A 워크로드 분석** 📘 일반적 관행

TABLE_A는 다음 특성 때문에 `range` 모드가 적합하다:

| 특성 | TABLE_A | distribution-mode 선택 영향 |
|------|---------|--------------------------|
| 쓰기 빈도 | 10분 주기 (144회/일) | 빈번 → shuffle 비용 반복 발생 |
| 1회 쓰기 크기 | ~8GB | 중간 규모 → range shuffle 부담 감수 가능 (44초) |
| 조회 빈도 | 반복 조회 (다차원 필터) | 높음 → data skipping 효과가 누적 |
| **쓰기 비용 vs 읽기 이득** | shuffle 44초 × 144회 = **1.76시간/일** | 읽기 성능 향상이 이를 상쇄하는지가 판단 기준 |

> **대안 검토**: 쓰기 레이턴시가 SLA에 민감하다면, `hash` + 시간당 컴팩션 조합도 고려할 수 있다. 이 경우 쓰기는 빨라지지만 컴팩션 전까지 data skipping 효과가 감소한다.

### 3.4 TABLE_A 적용: sort_a, sort_b, sort_c + range 모드 근거

| 설정 | 값 | 근거 수준 | 근거 |
|------|-----|-----------|------|
| `write.distribution-mode` | `range` | 📘 일반적 관행 | 다차원 필터 조회에서 data skipping 효과 극대화. 쓰기 비용(9.2GiB shuffle)은 24 executor로 44초에 처리 |
| Write Ordering | `sort_a, sort_b, sort_c ASC NULLS FIRST` | ⚠️ 개선 필요 | 조회 패턴의 필터 컬럼 기준. 실제 조회 빈도 데이터로 재검증 필요 |

**Write Ordering 컬럼 순서의 중요성**

```sql
WRITE ORDERED BY sort_a, sort_b, sort_c
```

정렬은 **왼쪽 컬럼부터 우선 적용**된다. 따라서:
- `sort_a` 단독 필터: ✅ data skipping 효과 높음
- `sort_a + sort_b` 필터: ✅ 효과 높음
- `sort_b` 단독 필터 (sort_a 없이): ⚠️ 효과 제한적

**Write Ordering 컬럼 선택 기준**

| 순위 | 기준 | 이유 |
|------|------|------|
| 1순위 | WHERE 절에 가장 자주 등장하는 컬럼 | 1순위 정렬 컬럼이 data skipping 효과 최대 |
| 2순위 | 카디널리티가 적당한 컬럼 | 아래 표 참조 |
| 3순위 | 범위 조회 (BETWEEN, >, <)에 사용되는 컬럼 | 선형 정렬이 범위 조회에 유리 |

**카디널리티와 Write Ordering 효과의 관계**

| 카디널리티 | Data Skipping 효과 | 이유 |
|-----------|-------------------|------|
| 매우 낮음 (2~5) | ⚠️ 제한적 | 대부분의 파일에 모든 값이 포함 → min/max 범위가 넓어 skip 불가 |
| 중간 (수십~수백) | ✅ **최적** | 정렬 시 값 범위가 파일별로 명확히 분리 → skip 효과 극대화 |
| 매우 높음 (수만 이상) | ⚠️ 제한적 | 정렬 비용 대비 2순위 이하 컬럼의 skipping 효과 급감 |

> **실무 팁**: 1순위 정렬 컬럼은 카디널리티보다 **조회 빈도**를 우선한다. 2~3순위 컬럼은 카디널리티가 중간인 컬럼이 가장 효과적이다.

> ⚠️ **검증 필요**
> 실제 조회 로그를 분석하여 WHERE 절에 가장 빈번한 컬럼을 확인하고, write ordering 컬럼 순서를 재검토해야 한다.

---

## 4. 버킷팅 (Bucketing)

### 4.0 Bucketing이 필요한가?

Bucketing은 **identity 파티션의 대안**이다. 별도의 최적화 기능이 아니라, 파티션 트랜스폼 선택의 문제이다.

| 질문 | 예 → | 아니오 → |
|------|------|---------|
| 파티션 키로 사용하려는 컬럼의 카디널리티가 수백 이상인가? | **Bucket 검토** | identity 유지 |
| 해당 컬럼이 등가 조건(`=`)으로만 조회되는가? | **Bucket 적합** | 범위 조회 → identity 또는 truncate |
| `day × 파티션 키` 조합별 데이터가 100MB 미만으로 small 파일 문제가 있는가? | **Bucket으로 파티션 수 축소** | identity 유지 |

**TABLE_A 판단**: `par_a`, `par_b`의 카디널리티가 확인되지 않은 상태. → **카디널리티 확인 후 결정** ⚠️

### 4.1 개념

`bucket(N, col)` 트랜스폼은 컬럼 값을 해시 함수로 N개 버킷에 분배한다.

```sql
-- bucket(16, par_a): par_a 값을 16개 버킷으로 분배
PARTITIONED BY (day(ts), bucket(16, par_a))
```

Iceberg는 Murmur3 해시를 사용하며, 결과값은 `0 ~ N-1` 범위의 정수이다.

### 4.2 적용 시나리오

| 상황 | identity 파티션 | bucket 파티션 |
|------|----------------|--------------|
| 카디널리티 낮음 (수십 이하) | ✅ 적합 | ❌ 불필요한 복잡성 |
| 카디널리티 높음 (수백~수천) | ❌ 파티션 폭증, small 파일 | ✅ N개로 제어 가능 |
| 등가 조건 조회 (`= 'value'`) | ✅ 정확한 프루닝 | ✅ 해시 기반 프루닝 |
| 범위 조건 조회 (`BETWEEN`) | ✅ 프루닝 가능 | ❌ 해시 분배이므로 프루닝 불가 |

**버킷 수(N) 선택 기준** 📘 일반적 관행

```
N = ceil(파티션당 예상 데이터 크기 / 목표 파일 크기)

예) day 파티션당 936GB, 목표 파일 크기 512MB
    N = ceil(936GB / 512MB) ≈ 1,875 → 과다
    → bucket은 보조 파티션으로, 전체를 나누는 것이 아님

실질적 기준: 카디널리티가 수백 이상일 때 16~64개가 일반적
```

### 4.3 TABLE_A 적용 검토

| 파티션 키 | 현재 | bucket 대안 | 판단 |
|----------|------|------------|------|
| `par_a` | identity | `bucket(N, par_a)` | 카디널리티 확인 후 결정 |
| `par_b` | identity | `bucket(N, par_b)` | 카디널리티 확인 후 결정 |

**identity → bucket 전환이 필요한 신호**

| 신호 | 설명 |
|------|------|
| `day × par_a × par_b` 조합이 수천 개 이상 | 파티션 수 폭증, 메타데이터 부하 |
| 파티션당 데이터가 수 MB 이하 | small 파일 문제 |
| 특정 조합에 데이터 쏠림 | 파티션 스큐, 처리 불균형 |

> **참고**: Iceberg의 **Partition Evolution**으로 `identity` → `bucket` 전환 시 기존 데이터 재적재 없이 메타데이터만 변경하면 된다. 단, 전환 이전 데이터는 이전 파티션 구조를 유지한다.

---

## 5. Z-ordering

### 5.0 Z-ordering이 필요한가?

Z-ordering은 컴팩션 시 적용하는 **추가 최적화**이다. Write Ordering으로 충분하면 불필요하다.

| 질문 | 예 → | 아니오 → |
|------|------|---------|
| Write Ordering의 2~3순위 컬럼이 단독 필터로 자주 사용되는가? | **Z-ordering 후보** | Write Ordering으로 충분 |
| 조회 패턴에서 필터 컬럼 조합이 고정되지 않고 다양한가? | **Z-ordering 후보** | Write Ordering으로 충분 |
| 컴팩션 Job을 운영할 인프라/자원이 있는가? | 다음 질문으로 | Z-ordering 불가 (컴팩션 필수) |
| Write Ordering 1순위 컬럼의 data skipping 비율이 이미 90% 이상인가? | 추가 효과 미미 → 불필요 | **Z-ordering 검토** |

**TABLE_A 판단**: 조회 패턴이 다차원 필터이며 sort_b/sort_c 단독 필터 빈도가 불명확. → **조회 로그 확보 후 결정** ⚠️

### 5.1 개념 및 원리

Z-ordering은 **Z-curve(모튼 코드)**를 이용해 다차원 데이터를 1차원으로 매핑하여 정렬하는 기법이다. 일반 정렬(Write Ordering)이 첫 번째 컬럼을 우선하는 반면, Z-ordering은 **여러 컬럼을 균등하게** 고려한다.

```
일반 정렬 (sort_a, sort_b):        Z-ordering (sort_a, sort_b):
sort_b                               sort_b
  │                                      │
4 │ 4  8  12 16                        4 │ 3  4  11 12
3 │ 3  7  11 15                        3 │ 1  2  9  10
2 │ 2  6  10 14                        2 │ 7  8  15 16
1 │ 1  5  9  13                        1 │ 5  6  13 14
  └──────────── sort_a                 └──────────── sort_a
    1  2  3  4                             1  2  3  4

→ sort_a 단독 필터: 정렬이 유리      → sort_a, sort_b 개별 필터:
→ sort_b 단독 필터: 효과 없음           Z-ordering이 균등하게 유리
```

### 5.2 Write Ordering vs Z-ordering vs Bucketing 비교

| 항목 | Write Ordering | Z-ordering | Bucketing |
|------|---------------|------------|-----------|
| 정렬 방식 | 사전식 (첫 컬럼 우선) | Z-curve (균등 다차원) | 해시 분배 |
| 적용 시점 | 쓰기 시 (`WRITE ORDERED BY`) | 컴팩션 시 (`rewrite_data_files`) | 파티션 정의 시 |
| Data Skipping | 첫 번째 컬럼에 편중 | 모든 지정 컬럼에 균등 | 등가 조건만 |
| 범위 조회 | 첫 번째 컬럼만 효과적 | 모든 컬럼에 부분적 효과 | 효과 없음 |
| 쓰기 비용 | range 모드 시 shuffle | 컴팩션 비용 (별도 Job) | 없음 (파티션 분배만) |
| 적합한 패턴 | 단일/순차 필터 우선 | 다차원 필터, 컬럼 조합 다양 | 고카디널리티 등가 조건 |

### 5.3 다차원 필터 조회에서의 효과

TABLE_A의 조회 패턴이 **다차원 필터**(ts + par_a/par_b + sort_a/sort_b/sort_c)인 경우, Z-ordering은 Write Ordering 대비 이점이 있다:

| 조회 패턴 | Write Ordering 효과 | Z-ordering 효과 |
|----------|-------------------|----------------|
| `sort_a = 'x'` (단독) | ✅ 높음 (1순위 정렬 컬럼) | ✅ 높음 |
| `sort_b = 'y'` (단독) | ⚠️ 제한적 (2순위) | ✅ 높음 |
| `sort_c = 'z'` (단독) | ⚠️ 매우 제한적 (3순위) | ✅ 높음 |
| `sort_a = 'x' AND sort_b = 'y'` | ✅ 높음 | ✅ 높음 |
| `sort_b = 'y' AND sort_c = 'z'` | ⚠️ 제한적 | ✅ 높음 |

### 5.4 TABLE_A 적용 검토

**Z-ordering 적용 방법** (Iceberg 1.10.1 / Spark)

```sql
-- Z-ordering은 컴팩션(rewrite_data_files) 시 적용
CALL catalog.system.rewrite_data_files(
  table => 'db.TABLE_A',
  strategy => 'sort',
  sort_order => 'zorder(sort_a, sort_b, sort_c)',
  where => 'ts >= timestamp ''2026-03-15'' AND ts < timestamp ''2026-03-16'''
);
```

**TABLE_A에서의 적용 판단** ⚠️ 개선 필요

| 판단 기준 | 현재 상황 |
|----------|----------|
| 조회 패턴 다양성 | 다차원 필터 → Z-ordering 후보 |
| Write Ordering과의 관계 | 쓰기 시 Write Ordering, 컴팩션 시 Z-ordering으로 전환 가능 |
| 컴팩션 운영 여부 | 별도 문서에서 결정 예정 |

**실무 판단 프레임워크: Write Ordering vs Z-ordering 선택** 📘 일반적 관행

| 판단 질문 | Write Ordering 유리 | Z-ordering 유리 |
|----------|-------------------|-----------------|
| 조회 필터 컬럼이 몇 개? | 1~2개 (주로 고정) | 3개 이상 (조합 다양) |
| 필터 컬럼 중 "항상 포함"되는 컬럼이 있는가? | ✅ 있음 → 1순위 정렬로 배치 | ❌ 없음 → 균등 분배 필요 |
| 범위 조회(>, <, BETWEEN)가 주요한가? | ✅ 범위 조회 → 선형 정렬 유리 | 등가 조건 위주 → Z-order 유리 |
| 컴팩션 운영이 가능한가? | 운영 불가 → 쓰기 시 정렬 (Write Ordering) | 운영 가능 → 컴팩션 시 Z-ordering |

**Z-ordering 적용 시 주의사항**

| 항목 | 설명 |
|------|------|
| 컬럼 수 제한 | 실무에서 **2~4개**가 효과적. 5개 이상은 차원의 저주(curse of dimensionality)로 skipping 효과 급감 |
| 연산 비용 | Z-curve 계산(비트 인터리빙) + 전체 데이터 정렬 → Write Ordering 대비 컴팩션 시간 20~50% 증가 |
| 카디널리티 불균형 | 컬럼 간 카디널리티 차이가 클수록 Z-ordering 효과 감소. 가능하면 비슷한 카디널리티 컬럼끼리 조합 |
| Parquet row group | Z-ordering 효과는 row group 단위 min/max에 의존. `write.parquet.row-group-size-bytes`가 너무 크면 효과 감소 |

**권장 전략**

```
1단계 (현재): Write Ordering (sort_a, sort_b, sort_c)으로 쓰기
  → 기본 data skipping 확보. 추가 인프라 비용 없음

2단계 (조회 로그 확보 후): 조회 패턴 분석
  → sort_a 단독 필터가 70% 이상 → Write Ordering 유지 (현재 최적)
  → sort_b/sort_c 단독 필터가 30% 이상 → 컴팩션 시 Z-ordering 전환 검토

3단계 (Z-ordering 적용 시): 컴팩션 대상 컬럼 선정
  → 파티션 키(par_a, b)는 이미 프루닝되므로 Z-ordering 대상에서 제외
  → Z-ordering 대상: sort_a, sort_b (+ 필요 시 sort_c)
```

> **참고**: Z-ordering은 컴팩션 프로시저에서 적용하므로, 운영 상세(주기, 대상 파티션 선정, 비용)는 컴팩션 운영 가이드에서 다룬다.

---

## 6. 추가 스키마 설계 고려사항

### 6.1 데이터 타입 선택

| 타입 선택 | 권장 | 근거 수준 | 이유 |
|----------|------|-----------|------|
| timestamp_ntz vs timestamp | `timestamp_ntz` | 📘 일반적 관행 | 타임존 변환 오버헤드 없음. UTC 기준 처리 시 적합 |
| string vs fixed-length | `string` | 📘 일반적 관행 | Parquet는 내부적으로 dictionary encoding 적용. 고정 길이 이점 없음 |
| double vs float | 정밀도 요구사항에 따라 | - | float(4B)은 메모리 절약, double(8B)은 정밀도 보장 |
| integer vs long | 값 범위에 맞게 | - | integer(4B)로 충분하면 long(8B) 불필요 |
| array 타입 | 필요 시 사용 | - | Parquet nested 구조로 저장. 필터 pushdown 제한적 |

**TABLE_A 적용**

TABLE_A는 `timestamp_ntz`를 사용 중이며, 이는 적절한 선택이다. 시계열 데이터의 `day()` 트랜스폼과 호환된다.

### 6.2 Nullable 설계

| 항목 | 영향 |
|------|------|
| NOT NULL 제약 | Parquet 인코딩 시 null bitmap 생략 → 약간의 저장 효율 향상 |
| Predicate Pushdown | null이 없는 컬럼은 `IS NOT NULL` 조건 불필요 → 쿼리 단순화 |
| Write Ordering | `NULLS FIRST/LAST` 지정 필요. null이 없으면 무관 |

📘 **일반적 관행**: 비즈니스 로직상 null이 발생하지 않는 컬럼은 `NOT NULL`로 정의한다. 다만 Iceberg에서 NOT NULL 제약은 쓰기 시 검증 비용이 추가되므로, 성능 영향은 미미하다.

### 6.3 Column-level Metrics Mode

Iceberg는 데이터 파일 작성 시 컬럼별 통계(min/max, null count 등)를 메타데이터에 저장한다. 이 통계가 **data skipping의 기반**이다. 통계가 수집되지 않는 컬럼은 WHERE 절에 사용해도 파일을 skip할 수 없다.

**통계 수집 수준** 📘 일반적 관행

| 모드 | 수집 내용 | 메타데이터 크기 | 적합한 컬럼 |
|------|----------|---------------|-----------|
| `none` | 수집 안 함 | 최소 | data skipping 불필요한 컬럼 (array, 대용량 string 등) |
| `counts` | null count, value count | 작음 | 집계 쿼리용 |
| `truncate(N)` | min/max (N바이트로 잘림), counts | 보통 | **string 컬럼 기본값 (N=16)** |
| `full` | min/max (전체 값), counts | 클 수 있음 | 숫자, 날짜 등 값이 짧은 컬럼 |

**설정 방법**

```sql
-- 테이블 전체 기본 모드
ALTER TABLE catalog.db.TABLE_A
SET TBLPROPERTIES ('write.metadata.metrics.default' = 'truncate(16)');

-- 특정 컬럼 모드 지정
ALTER TABLE catalog.db.TABLE_A
SET TBLPROPERTIES (
  'write.metadata.metrics.column.sort_a' = 'full',
  'write.metadata.metrics.column.sort_b' = 'full',
  'write.metadata.metrics.column.sort_c' = 'full',
  -- array 타입 8개 컬럼은 min/max 통계 의미 없음
  'write.metadata.metrics.column.arr_col_1' = 'none',
  'write.metadata.metrics.column.arr_col_2' = 'none',
  'write.metadata.metrics.column.arr_col_3' = 'none',
  'write.metadata.metrics.column.arr_col_4' = 'none',
  'write.metadata.metrics.column.arr_col_5' = 'none',
  'write.metadata.metrics.column.arr_col_6' = 'none',
  'write.metadata.metrics.column.arr_col_7' = 'none',
  'write.metadata.metrics.column.arr_col_8' = 'none'
);
```

**TABLE_A 적용** 📘 일반적 관행

| 컬럼 유형 | 권장 모드 | 이유 |
|----------|----------|------|
| `ts`, `col_int_1`, `col_double_*` | `full` | 숫자/날짜 값은 크기가 작아 full 저장 부담 없음. data skipping 효과 최대 |
| `par_a`~`sort_c` (string, 필터용) | `truncate(16)` (기본값) 또는 `full` | 필터 대상 컬럼은 통계 필수. 값 길이가 16자 이하면 `full`이 더 정확 |
| `arr_col_1` ~ `arr_col_8` (array × 8개) | `none` ✅ 현재 적용됨 | array 타입은 min/max 통계 의미 없음. 메타데이터 절약 |

> **확인 필요**: Iceberg의 기본 `write.metadata.metrics.default`는 `truncate(16)`이다. 대부분의 경우 기본값으로 충분하지만, data skipping이 기대만큼 동작하지 않으면 해당 컬럼의 metrics mode를 `full`로 변경하고 통계 수집 여부를 확인해야 한다.

### 6.4 Parquet Bloom Filter

Bloom Filter는 **등가 조건(`=`)에 특화된 확률적 인덱스**로, min/max 통계로 skip하지 못하는 파일을 추가로 skip할 수 있다.

**min/max 통계 vs Bloom Filter**

```
파일 내 sort_a: [A, B, C, D, E, F, G, H]  (min=A, max=H)

WHERE sort_a = 'Z':
  min/max → A ≤ Z ≤ H? → 아니오 → ✅ skip (min/max로 충분)

WHERE sort_a = 'C':
  min/max → A ≤ C ≤ H? → 예 → ❌ skip 불가 (범위 안에 있음)
  Bloom Filter → 'C'가 이 파일에 있는가? → 있음 → 읽어야 함

WHERE sort_a = 'D2':
  min/max → A ≤ D2 ≤ H? → 예 → ❌ skip 불가
  Bloom Filter → 'D2'가 이 파일에 있는가? → 없음 → ✅ skip!
```

Bloom Filter는 min/max 범위 안에 있지만 실제로는 파일에 없는 값을 걸러낸다. **Write Ordering으로 min/max 범위를 좁힌 뒤에도 남는 false positive를 추가로 제거**하는 보완 역할이다.

**적용 시나리오** 📘 일반적 관행

| 상황 | Bloom Filter 효과 |
|------|------------------|
| Write Ordering 1순위 컬럼 (min/max 범위 좁음) | ⚠️ 제한적. min/max로 이미 대부분 skip |
| Write Ordering 2~3순위 컬럼 (min/max 범위 넓음) | ✅ **효과적**. min/max로 못 거른 파일을 추가 skip |
| 파티션 키가 아닌 고카디널리티 등가 필터 컬럼 | ✅ **가장 효과적**. 정렬/파티셔닝과 무관하게 동작 |
| 범위 조건 조회 (>, <, BETWEEN) | ❌ 효과 없음. Bloom Filter는 등가 조건 전용 |

**설정 방법**

```sql
ALTER TABLE catalog.db.TABLE_A
SET TBLPROPERTIES (
  'write.parquet.bloom-filter-enabled.column.sort_a' = 'true',
  'write.parquet.bloom-filter-enabled.column.sort_b' = 'true',
  'write.parquet.bloom-filter-max-bytes' = '1048576'
);
```

| 속성 | 기본값 | 설명 |
|------|--------|------|
| `write.parquet.bloom-filter-enabled.column.*` | `false` | 컬럼별 Bloom Filter 활성화 |
| `write.parquet.bloom-filter-max-bytes` | `1048576` (1MB) | Bloom Filter 최대 크기. 클수록 false positive 감소, 파일 크기 증가 |

**TABLE_A 적용 검토** ⚠️ 개선 필요

| 컬럼 | Bloom Filter 후보 | 이유 |
|------|------------------|------|
| `sort_a` | 검토 (2단계) | Write Ordering 1순위이므로 min/max로 대부분 skip. 추가 효과 제한적일 수 있음 |
| `sort_b`, `sort_c` | **검토 권장** | Write Ordering 2~3순위. min/max 범위가 넓어 등가 필터 시 Bloom Filter 효과 기대 |
| `par_a`, `par_b` | ❌ 불필요 | 파티션 키로 이미 프루닝됨 |

> **주의**: Bloom Filter는 파일 크기를 증가시킨다 (컬럼당 ~1MB). 19개 컬럼 전체에 켜면 파일당 ~19MB 증가하므로, **필터 대상 컬럼에만 선택적으로 적용**해야 한다.

### 6.5 테이블 속성

**쓰기 및 파일 속성**

| 속성 | 기본값 | 권장값 | 근거 수준 | 설명 |
|------|--------|--------|-----------|------|
| `write.parquet.compression-codec` | `zstd` (Iceberg 1.10.1) | `zstd` | 📘 일반적 관행 | 압축률·속도 균형 최적. Spark 4.x + Iceberg 최신 기본값 |
| `write.target-file-size-bytes` | `536870912` (512MB) | `536870912` (512MB) | 📘 일반적 관행 | S3 기반 워크로드에서 512MB가 I/O 효율과 병렬성의 균형점 |
| `write.distribution-mode` | `hash` | `range` | 📘 일반적 관행 | Write Ordering 사용 시 `range` 필수 (3.2절 참조) |
| `write.metadata.compression-codec` | `gzip` | `gzip` | 📘 일반적 관행 | 메타데이터 파일 압축. 기본값 유지 |
| `read.split.target-size` | `134217728` (128MB) | 기본값 유지 | 📘 일반적 관행 | Spark의 `maxPartitionBytes`와 정합 |

**Parquet 내부 구조 속성** 📘 일반적 관행

Parquet 파일은 `Row Group → Column Chunk → Page` 계층 구조를 가진다. 각 계층의 크기 설정이 data skipping과 인코딩 효율에 영향을 준다:

| 속성 | 기본값 | 권장값 | 설명 |
|------|--------|--------|------|
| `write.parquet.row-group-size-bytes` | `134217728` (128MB) | 기본값 유지 | Row Group 크기. min/max 통계가 Row Group 단위로 저장됨 |
| `write.parquet.page-size-bytes` | `1048576` (1MB) | 기본값 유지 | Page 크기. Dictionary Encoding 단위 |
| `write.parquet.dict-size-bytes` | `2097152` (2MB) | 기본값 유지 | Dictionary 크기 임계값. 초과 시 Plain Encoding 전환 |

> **Row Group과 Data Skipping의 관계**
>
> Data Skipping의 min/max 통계는 **Row Group 단위**로 저장된다. Row Group이 작을수록 min/max 범위가 좁아져 skipping 효과가 높아지지만, 메타데이터 크기가 증가한다.
>
> ```
> 512MB 파일 기준:
>   Row Group 128MB → 4개 Row Group → 4개 min/max 통계 (기본, 적정)
>   Row Group 32MB  → 16개 Row Group → 16개 min/max 통계 (세밀, 메타데이터 증가)
> ```
>
> 대부분의 워크로드에서 기본값(128MB)이 적정하다. Write Ordering + `range` 모드 조합이면 Row Group 내 데이터도 정렬되어 있으므로 추가 조정 불필요.

**메타데이터 관리 속성** 📘 일반적 관행

10분 배치(144 커밋/일)는 메타데이터 파일을 빠르게 누적시킨다:

| 속성 | 기본값 | 권장값 | 설명 |
|------|--------|--------|------|
| `write.metadata.previous-versions-max` | `100` | `100` | 유지할 이전 메타데이터 파일 수. 144커밋/일이면 ~17시간분 |
| `commit.manifest-merge.enabled` | `true` | `true` | 매니페스트 파일 자동 병합. 비활성화 시 매니페스트 누적 |
| `commit.manifest.target-size-bytes` | `8388608` (8MB) | 기본값 유지 | 매니페스트 파일 목표 크기 |

**compression-codec 비교**

| 코덱 | 압축률 | 압축 속도 | 해제 속도 | 적합한 경우 |
|------|--------|----------|----------|-----------|
| `zstd` | ✅ 높음 | 보통 | 빠름 | **범용 권장**. 압축률·속도 균형 |
| `snappy` | 보통 | 빠름 | 빠름 | 쓰기 속도 최우선 |
| `gzip` | 높음 | 느림 | 보통 | 저장 비용 최소화 |
| `lz4` | 낮음 | 매우 빠름 | 매우 빠름 | 실시간 처리 |

**target-file-size 선택 기준** 📘 일반적 관행

| 크기 | 장점 | 단점 |
|------|------|------|
| 128MB | 높은 병렬성, 빠른 태스크 시작 | 파일 수 증가, 메타데이터 부하 |
| **512MB** | **I/O 효율과 병렬성 균형** | - |
| 1GB | 파일 수 최소화, S3 ListObject 비용 절감 | 병렬성 저하, 태스크 실패 시 재처리 비용 |

---

## 7. 설계 결정 요약

### 7.1 의사결정 매트릭스

| 설계 항목 | 선택지 | TABLE_A 적용 | 근거 수준 | 판단 기준 |
|----------|--------|-------------|-----------|----------|
| 시간 파티션 | `hour`/`day`/`month` | `day(ts)` | 📘 일반적 관행 | 일 단위 조회 패턴, 일 적재량 ~936GB |
| 비즈니스 키 파티션 | `identity`/`bucket` | `identity` (par_a, b) | ⚠️ 개선 필요 | 카디널리티 확인 후 최종 결정 |
| Write Ordering | 사용/미사용, 컬럼 선택 | `sort_a, b, c` (사용) | ⚠️ 개선 필요 | 파티션 프루닝 후 추가 필터 필요 → 사용. 컬럼 순서는 조회 로그 분석 후 재검증 |
| Distribution Mode | `none`/`hash`/`range` | `range` | 📘 일반적 관행 | Write Ordering 사용 시 range 필수 |
| Z-ordering | 미적용/컴팩션 시 적용 | 미적용 (2단계 검토) | ⚠️ 개선 필요 | 2~3순위 컬럼 단독 필터 빈도 확인 후 결정 |
| Column Metrics | `none`/`truncate`/`full` | 기본값 유지 + array는 `none` | 📘 일반적 관행 | 필터 대상 컬럼 통계 수집 필수 |
| Bloom Filter | 컬럼별 on/off | sort_b, sort_c 검토 | ⚠️ 개선 필요 | Write Ordering 2~3순위 컬럼의 등가 필터 보완 |
| 압축 코덱 | `zstd`/`snappy`/`gzip` | `zstd` (기본값) | 📘 일반적 관행 | 압축률·속도 균형 |
| 파일 크기 | 128MB/512MB/1GB | 512MB (기본값) | 📘 일반적 관행 | S3 I/O 효율 |

### 7.2 TABLE_A 최종 DDL 예시

**CREATE TABLE**

```sql
CREATE TABLE catalog.db.TABLE_A (
    ts            timestamp_ntz  NOT NULL,
    par_a         string         NOT NULL,
    par_b         string         NOT NULL,
    sort_a        string,
    sort_b        string,
    sort_c        string,
    col_double_1  double,
    col_double_2  double,
    col_int_1     integer,
    arr_col_1     array<string>,
    arr_col_2     array<string>,
    arr_col_3     array<string>,
    arr_col_4     array<string>,
    arr_col_5     array<string>,
    arr_col_6     array<string>,
    arr_col_7     array<string>,
    arr_col_8     array<string>
    -- ... 나머지 컬럼 생략 (총 약 19개)
)
USING iceberg
PARTITIONED BY (day(ts), par_a, par_b)
TBLPROPERTIES (
    'write.distribution-mode' = 'range',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '536870912',
    -- array 타입 8개 컬럼: min/max 통계 의미 없으므로 none
    'write.metadata.metrics.column.arr_col_1' = 'none',
    'write.metadata.metrics.column.arr_col_2' = 'none',
    'write.metadata.metrics.column.arr_col_3' = 'none',
    'write.metadata.metrics.column.arr_col_4' = 'none',
    'write.metadata.metrics.column.arr_col_5' = 'none',
    'write.metadata.metrics.column.arr_col_6' = 'none',
    'write.metadata.metrics.column.arr_col_7' = 'none',
    'write.metadata.metrics.column.arr_col_8' = 'none'
);
```

**ALTER TABLE — Write Ordering 적용**

```sql
ALTER TABLE catalog.db.TABLE_A
WRITE ORDERED BY
    sort_a ASC NULLS FIRST,
    sort_b ASC NULLS FIRST,
    sort_c ASC NULLS FIRST;
```

**ALTER TABLE — Bloom Filter 적용 (검토 후)**

```sql
-- Write Ordering 2~3순위 컬럼에 Bloom Filter 적용
ALTER TABLE catalog.db.TABLE_A
SET TBLPROPERTIES (
    'write.parquet.bloom-filter-enabled.column.sort_b' = 'true',
    'write.parquet.bloom-filter-enabled.column.sort_c' = 'true'
);
```

**ALTER TABLE — Partition Evolution 예시 (필요 시)**

```sql
-- identity → bucket 전환 예시
ALTER TABLE catalog.db.TABLE_A
DROP PARTITION FIELD par_a;

ALTER TABLE catalog.db.TABLE_A
ADD PARTITION FIELD bucket(16, par_a);
```

> **참고**: Partition Evolution은 메타데이터만 변경한다. 기존 데이터 파일은 이전 파티션 구조를 유지하며, 이후 적재되는 데이터부터 새 파티션 구조가 적용된다. 기존 데이터도 새 구조로 맞추려면 `rewrite_data_files` 컴팩션이 필요하다.

---

## 8. 용어집

| 용어 | 정의 |
|------|------|
| **Hidden Partitioning** | Iceberg의 파티션 방식. 파티션 컬럼을 스키마에 노출하지 않고, 원본 컬럼에 트랜스폼을 적용하여 파티션을 구성 |
| **Partition Evolution** | 기존 데이터를 재적재하지 않고 파티션 구조를 변경하는 Iceberg 기능. 메타데이터만 변경 |
| **Data Skipping** | 쿼리 조건에 해당하지 않는 데이터 파일을 읽지 않고 건너뛰는 최적화. min/max 통계 기반 |
| **Write Ordering** | Iceberg 쓰기 시 파일 내 데이터를 지정된 컬럼 기준으로 정렬. min/max 통계의 효과를 극대화 |
| **Z-ordering** | Z-curve(모튼 코드)를 이용한 다차원 정렬. 여러 컬럼을 균등하게 고려하여 data skipping 효과를 분배 |
| **write.distribution-mode** | Iceberg 쓰기 전 데이터 재분배 방식. `none`(재분배 없음), `hash`(해시 분배), `range`(범위 분배+정렬) |
| **Bucket 트랜스폼** | Murmur3 해시로 컬럼 값을 N개 버킷에 분배하는 파티션 트랜스폼. 고카디널리티 컬럼에 적합 |
| **카디널리티** | 컬럼의 고유값(distinct value) 수. 파티션 트랜스폼 선택의 핵심 기준 |
| **컴팩션** | Iceberg `rewrite_data_files` 프로시저. small 파일 병합, 정렬(Z-ordering 포함) 적용 |
| **small 파일 문제** | 파티션이 과다하거나 배치 빈도가 높아 작은 파일이 다수 생성되는 현상. 읽기 성능 저하의 주요 원인 |
| **글로벌 정렬** | `range` 모드에서 파일 간 값 범위가 겹치지 않도록 보장하는 정렬. `hash` 모드의 로컬 정렬(파일 내 정렬만)과 구분 |
| **Row Group** | Parquet 파일의 데이터 블록 단위. min/max 통계가 Row Group 단위로 저장되며, data skipping의 기본 단위 |
| **매니페스트 파일** | Iceberg의 데이터 파일 목록을 관리하는 메타데이터 파일. 파일 경로, 파티션 정보, 통계를 포함 |
| **Column Metrics** | 컬럼별 통계 수집 수준 (`none`/`counts`/`truncate(N)`/`full`). Data skipping의 기반이 되는 min/max 통계 제어 |
| **Bloom Filter** | 확률적 자료구조 기반 인덱스. 등가 조건(`=`) 필터에서 min/max로 skip 못하는 파일을 추가로 걸러냄. false positive 있으나 false negative 없음 |

---

## 9. 참고 자료

- [Iceberg 1.10.1 Partitioning](https://iceberg.apache.org/docs/1.10.1/partitioning/)
- [Iceberg 1.10.1 Configuration (Table Properties)](https://iceberg.apache.org/docs/1.10.1/configuration/)
- [Iceberg 1.10.1 Spark DDL](https://iceberg.apache.org/docs/1.10.1/spark-ddl/)
- [Iceberg 1.10.1 Spark Writes](https://iceberg.apache.org/docs/1.10.1/spark-writes/)
- [Iceberg 1.10.1 Spark Procedures](https://iceberg.apache.org/docs/1.10.1/spark-procedures/)
- [Iceberg Spec — Partition Transforms](https://iceberg.apache.org/spec/#partition-transforms)
- [Spark 4.1.1 SQL Performance Tuning](https://spark.apache.org/docs/4.1.1/sql-performance-tuning.html)

---

## 부록 A. 설계 검증용 쿼리 및 실제 데이터 분석

본 문서에서 ⚠️ 개선 필요로 표시된 항목의 최종 결정을 위해 아래 데이터를 확인하고, 실제 결과를 반영하였다:

1. **파티션 현황 및 카디널리티** → 파티션 트랜스폼 결정 (identity vs bucket)
2. **조회 로그 (WHERE 절 컬럼별 빈도)** → Write Ordering 순서, Z-ordering, Bloom Filter 결정

### A.1 파티션 현황 및 카디널리티 확인

아래 쿼리는 Spark SQL 또는 Trino에서 실행 가능하다.

> **핵심**: Iceberg의 복합 파티션(`day(ts)`, `par_a`, `par_b`)은 계층 구조가 아닌 **평면 복합 키**이다. 실제 파티션 수 = `DISTINCT (par_a, par_b)` 조합 수이므로, 개별 카디널리티보다 **조합 수**를 먼저 확인해야 한다.

**1) 파티션 조합별 현황 (핵심 — Iceberg 메타데이터)**

```sql
-- files 메타데이터 테이블로 파티션별 파일 수·레코드 수·크기 한 번에 확인
SELECT
    partition,
    COUNT(*) AS file_count,
    SUM(record_count) AS total_records,
    SUM(file_size_in_bytes) / 1024 / 1024 AS total_size_mb
FROM catalog.db.TABLE_A.files
GROUP BY partition
ORDER BY total_size_mb DESC;
```

**2) par_a별 후보 컬럼 카디널리티 비교**

```sql
-- par_b, par_c, par_d를 한 번에 비교하여 3번째 파티션 키 후보 판단
SELECT
    par_a,
    COUNT(DISTINCT par_b) AS par_b_card,
    COUNT(DISTINCT par_c) AS par_c_card,
    COUNT(DISTINCT par_d) AS par_d_card,
    COUNT(*) AS total_rows
FROM catalog.db.TABLE_A
WHERE date(ts) = '2026-03-15'
GROUP BY par_a
ORDER BY par_a;
```

**실제 결과: files 메타데이터 (248개 파티션 조합, day × par_a × par_b)**

| 순위 범위 | 크기 범위 | 파일 수 | 특징 |
|----------|----------|--------|------|
| 1~50위 | 1~203GB | 다수 | ✅ 충분한 크기, 대부분의 데이터 차지 |
| 50~60위 | 0.001~1GB | 소수 | ⚠️ target-file-size(512MB) 미만 포함 |
| 60위 이후 | 0.001GB 이하 | 전부 1개 | ❌ small file, 컴팩션 필수 |

> **진단**: 248개 조합 → 기준 2의 "수백" 범위로 ⚠️ 주의 구간이다. 상위 50개가 대부분의 데이터를 차지하여 **스큐가 존재**하며, 하위 ~190개 조합이 1GB 미만으로 **small file 주의** (컴팩션 필수) 상태이다.

**실제 결과: par_a별 카디널리티**

| par_a | par_b 카디널리티 | par_c 카디널리티 | par_d 카디널리티 |
|-------|-----------------|-----------------|-----------------|
| A | 42 | 677 | 179 |
| B | 72 | 11,918 | 704 |
| C | 70 | 8,245 | 523 |
| D | 64 | 5,102 | 381 |

- **par_a**: 4개 (A, B, C, D) — 매우 낮음, identity 최적
- **par_b**: par_a별 42~72개 (전체 150개) — 중간, identity 가능하나 조합 수 주의
- **par_c**: par_a별 677~11,918개 — 고카디널리티, identity 불가
- **par_d**: par_a별 179~704개 — 중카디널리티, identity 불가

**3번째 파티션 키 후보 비교표**

| 후보 | par_a별 카디널리티 | day당 총 조합 수 추정 | 파티션 트랜스폼 | 판단 |
|------|-------------------|---------------------|---------------|------|
| par_b | 42~72 | ~248 (확인됨) | identity | ⚠️ 조합 수 많으나 상위 50개가 대부분의 데이터 차지 |
| par_c | 677~11,918 | 수만 | bucket 필수 | ❌ identity 불가, small file 문제 |
| par_d | 179~704 | 수천 | bucket 필수 | ❌ identity 불가 |

> **결론**: 3번째 파티션 키로 par_b identity가 현실적인 유일한 선택지이다. par_c, par_d는 identity 사용이 불가능하며, bucket 적용 시에도 조합 수가 par_b보다 줄어들지 않는다. 다만 par_b의 248개 조합에서 하위 ~190개의 small file은 **정기 컴팩션으로 관리**해야 한다.

**small 파일 판단 기준**

| 조합별 데이터 크기 | 판단 | 조치 |
|------------------|------|------|
| 100MB 이상 | ✅ 정상 | 파티션 설계 유지 |
| 10~100MB | ⚠️ 주의 | 컴팩션 주기를 짧게 (시간당) 운영 |
| 10MB 미만 다수 | ❌ small 파일 문제 | 파티션 키 축소 또는 bucket 전환 검토 |

### A.2 조회 패턴 분석 (WHERE 절 컬럼 빈도)

실제 조회 로그를 분석하여 WHERE 절에 가장 빈번한 컬럼을 확인한다. 이 결과는 Write Ordering 컬럼 순서, Z-ordering 적용 여부, Bloom Filter 대상 컬럼 결정에 사용된다.

**Trino 쿼리 로그 확인**

```sql
-- Trino: system.runtime.queries에서 TABLE_A 관련 쿼리 추출
SELECT
    query_id,
    query,
    created
FROM system.runtime.queries
WHERE query LIKE '%TABLE_A%'
  AND state = 'FINISHED'
  AND query NOT LIKE '%system.runtime%'  -- 메타 쿼리 제외
ORDER BY created DESC
LIMIT 100;
```

> **참고**: `system.runtime.queries`는 Trino 코디네이터 메모리에 보관되며, 재시작 시 초기화된다. 장기 분석이 필요하면 Trino의 Event Listener를 설정하여 쿼리 로그를 별도 저장소에 적재해야 한다.

**실제 분석 결과**

| 절 | 사용 컬럼 | 빈도 | 조건 유형 | 비고 |
|----|----------|------|----------|------|
| WHERE | ts | 항상 | `date(ts) = ...` | 모든 쿼리에 포함, `day(ts)` 파티션으로 프루닝 |
| WHERE | sort_a | 높음 | 등가 (`=`, `IN`) | Write Ordering 1순위 후보 |
| WHERE | sort_b (=par_b) | 높음 | 등가 (`=`, `IN`) | 파티션 키(`par_b`)로 이미 프루닝됨 → Write Ordering 불필요 |
| WHERE | sort_c | 있음 | 등가 (`=`) | Write Ordering 2순위 후보 |
| GROUP BY | (확인 필요) | — | — | 추가 로그 확보 후 분석 |
| ORDER BY | (확인 필요) | — | — | 추가 로그 확보 후 분석 |

> **핵심 발견**: `sort_b = par_b`는 동일 컬럼이다 (1.1절 컬럼 관계 매핑 참조). 파티션 키(`par_b`)로 이미 파티션 프루닝이 수행되므로, Write Ordering에서 sort_b를 별도로 정렬하는 것은 **중복 비용**이다.

### A.3 결과 기반 판단 매트릭스

실제 데이터를 반영한 최종 판단이다.

**파티션 설계 (A.1 결과 기반)**

| 파티션 | 트랜스폼 | 근거 수준 | 판단 |
|--------|---------|-----------|------|
| `day(ts)` | 시간 트랜스폼 | 📘 일반적 관행 | ✅ 유지 — 모든 조회에 시간 조건 포함 |
| `par_a` | identity | ✅ 벤치마크 검증 | ✅ 유지 — 카디널리티 4, identity 최적 |
| `par_b` | identity | ⚠️ 개선 필요 | ⚠️ 유지 — 248 조합, 스큐 있으나 상위 50개 충분한 크기. 정기 컴팩션 필수 |

파티션 조합 수(`day × par_a × par_b` = ~248)와 조합별 데이터 크기를 기준으로 판단:

| 파티션 조합 수 | 조합별 크기 | 결정 |
|--------------|-----------|------|
| ≤30 | ≥100MB | `identity(par_a)`, `identity(par_b)` 유지 |
| ≤30 | <100MB | 컴팩션 주기 단축 또는 파티션 키 축소 검토 |
| 30~200 | ≥100MB | identity 유지 가능, 일일 파일 수 모니터링 |
| 30~200 | <100MB | 고카디널리티 쪽을 `bucket(N)` 전환 |
| >200 | 무관 | `bucket(N, par_a)` 및/또는 `bucket(N, par_b)` 전환 |
| 어느 쪽이든 | ≤10MB 다수 | 파티션 키 하나 제거 검토 (par_b → write ordering으로 이동) |

> **TABLE_A 현재 위치**: 248개 조합으로 ">200" 구간에 해당하나, 상위 50개 조합이 대부분의 데이터를 차지하고 par_b 외의 대안(par_c, par_d)은 모두 더 많은 조합을 생성하므로, **par_b identity를 유지하면서 정기 컴팩션으로 small file을 관리**하는 전략이 현실적이다.

**Write Ordering (A.2 결과 기반)**

| 기존 | 변경 후 (권장) | 근거 |
|------|-------------|------|
| `sort_a, sort_b, sort_c` | `sort_a, sort_c` | sort_b = par_b이므로 파티션 프루닝으로 대체됨, Write Ordering에서 제외 |

- **sort_a** (1순위): WHERE 절 등가 조건 빈도 높음 → data skipping 효과 최대
- **sort_b 제외**: par_b와 동일 컬럼, 파티션 키로 이미 프루닝됨 → 정렬 비용만 발생, 추가 skipping 효과 없음
- **sort_c** (2순위): WHERE 절 등가 조건 있음 → sort_a 이후 보조 skipping

**Z-ordering 적용 결정 (A.2 결과 기반)**

| 조건 | 결정 |
|------|------|
| sort_a 단독 필터 비율 70% 이상 | Write Ordering 유지, Z-ordering 불필요 |
| sort_c 단독 필터 비율 30% 이상 | 컴팩션 시 Z-ordering 적용 검토 |
| 필터 컬럼 조합이 고정되지 않고 다양 | Z-ordering 적극 검토 |

> **현재 판단**: sort_a, sort_c 2개 컬럼 체제에서 sort_c 단독 필터 빈도가 충분히 확인되면 Z-ordering 전환을 검토한다. 2개 컬럼은 Z-ordering의 최적 범위(2~4개)에 해당한다.

**Bloom Filter 적용 결정 (A.2 결과 기반)**

| 조건 | 결정 |
|------|------|
| sort_c가 등가 조건(`=`)으로 자주 사용됨 | Bloom Filter 활성화 권장 |
| sort_a가 등가 조건으로 사용되지만 Write Ordering 1순위 | Bloom Filter 효과 제한적 (min/max로 충분) |
| sort_b (=par_b) | ❌ 불필요 — 파티션 키로 이미 프루닝됨 |
