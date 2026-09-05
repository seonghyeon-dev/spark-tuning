# Trino Partition Pruning 검증 (hourly 테이블)

## 문서 정보

| 항목 | 내용 |
|------|------|
| 작성 목적 | `WHERE` 절의 함수·비교 형태가 Partition Pruning / Data Skipping에 미치는 영향을 실측·근거 기반으로 확정 |
| 대상 독자 | 데이터 엔지니어, 운영팀 |
| 환경 | **Trino 482**, Iceberg, S3(MinIO), Kubernetes |
| 대상 범위 | hourly 테이블(파티션 `hour(ts)`, `par_a`)의 **조회 경로**. Compaction·append Job 설정은 `tuning/compaction-tuning-guide.md`, `tuning/spark-tuning-guide.md` |
| 최종 수정일 | 2026-09-05 |

> **Trino 버전 주의**: `schema/read-performance-test.md` §5.4의 Bloom Filter 측정은 **Trino 475** 기준이다. 이 문서는 **482** 기준이며, 그 사이에 unwrap 룰이 추가된 항목이 있다 (섹션 2.3). 버전이 다른 측정을 나란히 비교할 때 주의한다.

### 근거 수준 라벨

| 라벨 | 의미 |
|------|------|
| ✅ 실측 확인 | 이 환경에서 직접 측정한 결과 |
| 📘 공식 확인 | Trino/Iceberg 공식 문서·소스·머지된 PR로 확인 |
| ⚠️ 추론 | 근거는 있으나 미검증. 그대로 신뢰하지 말 것 |

### 목차

- [1. 개요](#1-개요) — 대상 테이블, 확정 스키마와 컬럼 명명, 파티션 필터 강제
- [2. 술어가 Pruning으로 바뀌는 경로](#2-술어가-pruning으로-바뀌는-경로) — unwrap 룰, 함수별 지원 여부
- [3. 실측 결과](#3-실측-결과) — ts 조건별 스캔량, sort 컬럼이 ts 효과를 가리는 현상
- [4. Domain 표시로 Pruning을 판단하지 말 것](#4-domain-표시로-pruning을-판단하지-말-것) — 업스트림이 명시한 함정
- [5. Pruning 검증 방법](#5-pruning-검증-방법) — VERBOSE, IO 플랜, 메타데이터 테이블
- [6. splits와 파일 수](#6-splits와-파일-수) — `read.split.target-size`, 쓰기 설정과의 구분
- [7. 이 조사가 확인해 준 설계 판단](#7-이-조사가-확인해-준-설계-판단) — hour 파티션, Sort Order, par_a 분포
- [8. 기존 문서 반영 사항](#8-기존-문서-반영-사항) — `trino-query-guide.md` 정정 내역, 남은 대상
- [9. 미확정 및 후속 과제](#9-미확정-및-후속-과제)
- [10. 참고 자료](#10-참고-자료)

---

## 1. 개요

### 1.1 조사 배경

`schema/trino-query-guide.md`는 사용자에게 "`date(ts)`를 쓰라, `ts =`는 쓰지 말라"고 안내하지만, **왜 그렇게 되는지의 근거와 그 경계**는 담고 있지 않다. 이 문서는 그 근거를 Trino 옵티마이저 동작과 실측으로 확정하고, 안내가 성립하지 않는 경계 조건을 밝힌다.

핵심 질문 세 가지:

1. `WHERE` 좌측에 함수를 쓰면 Pruning이 깨지는가? → **함수마다 다르다** (섹션 2.3)
2. `EXPLAIN`에 술어가 안 보이면 Pruning이 안 된 것인가? → **아니다** (섹션 4)
3. 운영 쿼리 패턴(6개 컬럼 전부 등가 조건)에서 `ts` 조건은 실제로 얼마나 기여하는가? → **바이트 기준으로는 거의 기여하지 않는다. 그럼에도 필요하다** (섹션 3.2, 7.1)

### 1.2 대상 테이블

| 항목 | 값 |
|------|-----|
| 파티션 | `hour(ts)`, `par_a` |
| Sort Order | `sort_a`, `sort_b` (`WRITE ORDERED BY`) |
| `ts` 타입 | `timestamp(6)` — **NTZ(without time zone)** |
| `sort_a` 타입 | `string`. **`ts`와 동일한 값**을 문자열로 보유 (`2026-08-19 16:21:12.466` → `'20260819162112466'`) |
| `write.target-file-size-bytes` | 512MB (Compaction 후 실측 400후반~600초반MB) |
| 보관 주기 | 3개월 |
| 시간당 파티션/파일 | `par_a` 4종 → 파티션 4개, 파일 **51 / 25 / 4 / 1** |
| 조회 제약 | 파티션 컬럼 조건이 하나도 없으면 **쿼리 에러** (섹션 1.4) |

`ts`가 NTZ이므로 **세션 timezone 이슈는 발생하지 않는다.** `with time zone`이었다면 `date(ts)`가 KST 기준으로 잘려 UTC 파티션과 9시간 어긋났을 것이다.

> 이 성질은 Compaction 쪽 판단과 같은 뿌리다 — `tuning/compaction-tuning-guide.md`와 `pipeline/compaction-executor-sizing-design.md`에서 `.partitions` 조회 시 **naive datetime으로 변환**해야 하는 이유가 동일하게 `ts`의 NTZ 타입이다.

### 1.3 확정된 스키마와 컬럼 명명

**스키마가 확정됐다.** Sort Order는 `schema/read-performance-test.md` §5에서 비교한 4개 조합 중 어느 것도 아닌 **`sort_a`, `sort_b`** 조합으로 결정됐다.

**명명 규칙**: 접두어가 역할을 나타낸다 — `par_*`는 파티션 컬럼, `sort_*`는 Sort Order 컬럼, `col_*`는 **성능 최적화 역할이 없는 컬럼**이다.

| 컬럼 | 역할 | Pruning 단계 |
|------|------|--------------|
| `ts` | 파티션 (`hour(ts)`) | Partition Pruning |
| `par_a` | 파티션 (identity) | Partition Pruning |
| `sort_a` | **Sort Order 1순위** | Data Skipping (파일 min/max) |
| `sort_b` | **Sort Order 2순위** | Data Skipping (파일 min/max) |
| `col_a` | 없음 | Row-level Filter |
| `col_b` | 없음 | Row-level Filter |

> **`sort_a`는 `ts`의 문자열 사본이다** (`2026-08-19 16:21:12.466` → `'20260819162112466'`). 기존 프로젝트 문서 어디에도 없던 사실이며, **섹션 3.2의 관측 전체를 설명한다.**

**예전 자료를 읽을 때 주의** ⚠️

2026-09-05 이전에 작성된 문서·커밋·회의 자료는 다른 이름을 쓴다. 대응은 다음과 같다.

| 계열 | 예전 표기 | 현재 |
|------|-----------|------|
| `tuning/` (Compaction 문서) | 첫 번째 `col` 컬럼 (identity 파티션) | `par_a` |
| `tuning/` | 두 번째 `col` 컬럼 (정렬 1순위) | `sort_a` |
| `tuning/` | 세 번째 `col` 컬럼 (정렬 2순위) | `sort_b` |
| `tuning/` | 네 번째 `col` 컬럼 | `col_b` |
| `schema/` (설계·쿼리 문서) | 두 번째 `par` 컬럼 | `col_a` |
| `schema/` | 세 번째 `sort` 컬럼 | `col_b` |

> ⚠️ **`col_a`·`col_b`는 예전과 지금이 다른 컬럼을 가리킨다.** 예전 `tuning/` 문서에서 `col_a`는 **파티션 컬럼**(현 `par_a`)이었고, 지금의 `col_a`는 **역할이 없는 컬럼**(구 `par_b`)이다. 예전 자료의 숫자를 인용할 때 이름을 그대로 옮기면 안 된다.
>
> 판별법: 예전 자료에서 `col_a`가 **파티션 값(A/B/C/D)** 으로 등장하면 그것은 현재의 `par_a`다.

### 1.4 파티션 필터 강제

파티션 컬럼 조건 없이 조회하면 쿼리가 실패한다. Trino Iceberg 커넥터의 [`iceberg.query-partition-filter-required`](https://trino.io/docs/current/connector/iceberg.html) 설정이다 (기본값 `false`, 세션 프로퍼티 `query_partition_filter_required`, 적용 스키마는 `iceberg.query-partition-filter-required-schemas`로 한정 가능). 📘

**중요한 경계**: 이 설정은 **파티션 컬럼 중 하나라도** 조건이 있으면 통과시킨다. 파티션 컬럼은 `ts`와 `par_a` 두 개이므로,

```sql
-- ✅ 에러 없이 통과. ts 조건이 없어도 par_a가 파티션 컬럼이므로 요구조건 충족
WHERE par_a = 'C' AND col_b = 'x'
```

이 쿼리는 **에러 없이 3개월 전체를 대상으로 실행된다.** 섹션 3.1의 기준선 측정이 그 증거다. 즉 **이 설정은 `ts` 조건 누락을 막아주지 못한다.** ✅

---

## 2. 술어가 Pruning으로 바뀌는 경로

### 2.1 `date(ts) = DATE '...'` — 범위 술어로 되돌려진다 📘

`date(x)`는 [`CAST(x AS date)`의 별칭](https://trino.io/docs/current/functions/datetime.html)이고, 옵티마이저가 이를 범위 술어로 되돌려 커넥터에 전달한다.

```
date(ts) = DATE '2026-08-19'
  → CanonicalizeExpressionRewriter → CAST(ts AS date) = DATE '2026-08-19'
  → UnwrapCastInComparison        → ts >= TIMESTAMP '2026-08-19 00:00:00'
                                     AND ts <  TIMESTAMP '2026-08-20 00:00:00'
  → TupleDomain 으로 Iceberg pushdown → Partition Pruning
```

Trino 소스 `CanonicalizeExpressionRewriter.rewriteFunctionCall`에 다음 주석이 있다:

> `prefer CAST(x as DATE) to date(x), see e.g. UnwrapCastInComparison`

즉 이 정규화의 **목적 자체가** unwrap 룰을 태우기 위함이다. `schema/trino-query-guide.md` §3.2가 "`date()`와 `CAST`는 완전히 동일하다"고 안내하는 근거가 이것이다.

### 2.2 `ts = <정확한 시점>` — 시간 단위 + 파일 단위까지 Pruning ✅

`date_parse(...)`, `TIMESTAMP '...466'`, `TIMESTAMP '...466000'` **세 형태 모두 결과가 동일**하다.

- `date_parse`는 플래닝 단계에서 **상수 폴딩**되므로 리터럴과 차이가 없다
- `timestamp(3)` → `timestamp(6)` 확대 변환은 무손실이라 unwrap을 막지 않는다
- 플랜에 `ts = timestamp(6) '2026-08-19 16:21:12.466000'`으로 표시되는 것은 정상이다

> ⚠️ **`schema/trino-query-guide.md` §3.1·§3.7과 충돌하는 것처럼 보인다.** 기존 가이드는 "`ts =` 등가 비교 → 결과 없음 ❌"이라고 안내한다. 둘 다 맞으며, 조건이 다르다 — 섹션 8.1에서 정리한다.

### 2.3 함수별 pushdown 지원 (Trino 482 기준)

| 표현식 | pushdown | 근거 |
|--------|----------|------|
| `CAST(ts AS date)` / `date(ts)` | ✅ | 섹션 2.1 |
| `date_trunc('hour'\|'day', ts)` | ✅ | [PR #14161](https://github.com/trinodb/trino/pull/14161), [PR #14011](https://github.com/trinodb/trino/pull/14011) |
| `date_trunc('month'\|'year', ts)` | ✅ | [Issue #30192](https://github.com/trinodb/trino/issues/30192) 본문 (day/month/year 정상 동작 명시) |
| `year(ts)` | ✅ | [Issue #14078](https://github.com/trinodb/trino/issues/14078) → PR #16106으로 해결 |
| `ts` 범위 비교 (`>=`, `<`, `BETWEEN`) | ✅ | 섹션 2.4 |
| `date_trunc('week'\|'quarter', ts)` | ❌ → **484부터 ✅** | [PR #30197](https://github.com/trinodb/trino/pull/30197) 머지(2026-08-10), **milestone 484**. 우리는 482이므로 **아직 안 된다** |
| `lower()`, `substr()`, `format_datetime()`, UDF 등 | ❌ | 풀스캔 |

> **전 함수를 커버하는 일반 메커니즘은 없다.** unwrap 룰은 함수별로 개별 구현되며, 위 목록에 없으면 풀스캔이다. 📘
>
> `week`/`quarter`는 **482에서만 안 되는 일시적 제약**이다. 484 이상으로 올라가면 해소되므로, 우회 코드를 쿼리에 영구히 박아 넣지 말 것.

### 2.4 범위 조건 — 예전 경고는 유효하지 않다 (정정) 📘

이전 조사 기록은 [Issue #19266](https://github.com/trinodb/trino/issues/19266)을 근거로 "파티션 경계에 맞지 않는 범위 조건은 Pruning이 안 되므로 `CAST(ts AS date) >= ...`를 함께 걸어 유도하라"고 적고 있었다. **이 워크어라운드는 필요 없다.**

- #19266은 [**PR #24740**](https://github.com/trinodb/trino/pull/24740)으로 **종결**됐다 (milestone **469**, 2025-01-20 머지)
- 그 PR이 한 일은 동작 변경이 아니라 **테스트 추가**다 — *"partition pruning done at the Iceberg metadata layer"*가 **`EXPLAIN`에 pushdown이 표시되지 않는 경우에도 일어난다**는 것을 명시했다
- 즉 원래 제보는 **`EXPLAIN` 표시를 Pruning 여부로 오독한 것**이었고, Pruning은 처음부터 동작하고 있었다

우리 환경(482 > 469)에는 이 확인이 포함된다. 실측도 일치한다 — 대조군 `ts >= 16:00 AND ts < 17:00`이 **약 205 splits**로, 해당 시간 파티션(51파일) 정확히 하나만큼만 읽었다 (섹션 3.1). ✅

> 이 정정은 **섹션 4와 같은 사실의 다른 얼굴**이다. `EXPLAIN`의 Domain 표시는 Pruning 여부의 지표가 아니다.

---

## 3. 실측 결과

동일 `SELECT`에 조건절만 바꿔 측정했다.

### 3.1 `ts` 조건의 효과 (sort 컬럼 제외) ✅

`sort_a`/`sort_b`를 조건에서 뺀 상태다. 이 둘을 넣으면 `ts`의 효과가 가려지기 때문이다 (섹션 3.2).

| 조건 | rows | physical input | splits |
|------|------|----------------|--------|
| 기준선 (`ts` 조건 없음, `par_a` + `col_b`) | 105,453 | 13.05 GB | 276,570 |
| `date(ts) = DATE '2026-08-19'` | 12,932 | 264.02 MB | 4,938 |
| `ts = <2026-08-19 16:21:12.466>` | 12,932 | 33.07 MB | **15** |
| 대조군 `ts >= 16:00 AND ts < 17:00` | — | — | ~205 |

**단계별 감소**

| 구간 | 배수 | 해석 |
|------|------|------|
| 기준선 → 일 단위 | **56배** | `ts` 조건 유무의 차이. 3개월 → 1일 |
| 일 단위 → 시 단위 | **24.1배** (4,938 → 205) | **하루의 시간 파티션 수 24와 정확히 일치.** `hour(ts)` 파티션이 설계대로 동작한다는 직접 증거 ✅ |
| 시 단위 → 시점 지정 | **13.7배** (205 → 15) | 파티션 안에서 **파일 단위 min/max로 추가 Pruning**. 섹션 3.3 |

> **대조군 205의 검산**: `512MB × 51파일 ÷ 128MB` = 204. 시간 파티션(파일 51개) 전체와 일치한다. splits ↔ 파일 수 환산의 유일한 실측 대조점이다 (섹션 6).

### 3.2 sort 컬럼을 넣으면 `ts` 효과가 사라진다 ✅

운영 쿼리는 6개 컬럼을 전부 `WHERE`에 넣는다. 그 조건에서 다시 측정하면:

| 조건 | physical input | splits |
|------|----------------|--------|
| `ts` 제거 (`par_a` + `sort_a` + `sort_b`) | 16.79 MB | 15 |
| 위 + `date(ts)` 추가 | 16.79 MB | 15 |
| 위 + `ts =` 추가 | 16.79 MB | 15 |

**`ts` 조건을 넣든 빼든 읽는 양이 같다.** 원인은 섹션 1.2의 테이블 성질이다:

- `sort_a`는 **`ts`와 동일한 값**을 문자열로 갖는다
- `sort_a`는 **Sort Order 1순위**라 파일이 사실상 시간순으로 물리 정렬된다
- 따라서 `sort_a` 등가조건 하나가 **밀리초 단위의 시각 조건**으로 작동해, `ts` 조건보다 촘촘하게 파일을 걸러낸다

> ⚠️ **이 현상은 이 테이블 고유의 성질에 의존한다.** `sort_a`가 `ts`의 사본이 아니었다면 성립하지 않는다. 다른 테이블에 일반화하지 말 것.

**`schema/read-performance-test.md` §5와 같은 현상이다.** 그 테스트에서 Sort Order 4개 조합이 전부 8.56k rows / 55.2MB로 동일했던 이유도 같다 — 6개 컬럼 등가 조건이 이미 파일 단위까지 좁혀버려, 그 위에서 무엇을 바꾸든 차이가 안 나는 영역에 들어간 것이다. 반대로 §5.4에서 **Sort Order를 아예 빼면 40% 느려진** 것은, 이 좁히기 자체가 사라지기 때문이다.

### 3.3 rows는 같고 스캔량만 다르다

세 조건 모두 rows가 12,932로 같다. **같은 결과를 조회하므로 최종 행 수가 같은 것이 정상**이며, 차이는 **그 행에 도달하기 위해 읽은 양**(splits, physical input)에 나타난다.

`ts = <시점>`이 205 → 15 splits로 더 줄어든 것은 **파티션 단계가 아니라 파일 단계**의 효과다. `ts`는 Sort Order 컬럼이 아니지만 `sort_a`(= `ts`의 사본)가 1순위 정렬 키이므로, 파일별 `ts` lower/upper bound가 좁고 서로 겹치지 않는다.

> **파일 단계 Pruning의 효과는 물리 정렬 상태에 좌우된다.** 도착 순서대로 쓰이면 파일 min/max가 전 구간을 덮어 아무 파일도 걸러지지 않는다. Compaction의 `sort` 전략이 이 정렬 상태를 유지하는 장치이며, `tuning/compaction-tuning-guide.md` §1.2가 `binpack`을 기각한 이유가 여기에 닿는다.

---

## 4. Domain 표시로 Pruning을 판단하지 말 것

**`EXPLAIN ANALYZE`의 `:: [[...]]`(Domain) 표시 유무는 Pruning 여부의 지표가 아니다.** 📘

실측에서 갈렸다: ✅

| 조건 | Domain 표시 | 실제 Pruning |
|------|-------------|--------------|
| `date(ts)` (범위) | 표시됨 | 됨 |
| `ts = <시점>` (단일값) | **표시 안 됨** | **됨** (15 splits) |

이것이 개별 환경의 특이 현상이 아니라는 근거가 업스트림에 있다. PR #24740이 추가한 테스트가 *"partition pruning done at the Iceberg metadata layer"*는 **`EXPLAIN`이 filter pushdown을 보여주지 않아도 수행된다**는 것을 명시한다 (섹션 2.4). 즉 **Trino가 공식적으로 인정한 표시상의 함정**이다.

`::`로 표시되는 Domain 자체의 정의는 [`EXPLAIN (TYPE IO)` 문서](https://trino.io/docs/current/sql/explain.html)에 있다: `inputTableColumnInfos` → `columnConstraints` → `domain` → `nullsAllowed` + `ranges(low/high)`. 즉 **커넥터에 전달된 "이 컬럼이 가질 수 있는 값의 제약"**이다.

> ⚠️ 커넥터가 술어를 enforced / unenforced로 나누고 그것이 표시를 가른다는 설명은 **추론이며 소스 미확인**이다. 다만 "표시가 없어도 Pruning은 된다"는 결론 자체는 위 PR로 확정됐으므로, 실무 판단에는 영향이 없다.

**따라서 Pruning 검증은 섹션 5의 방법으로 해야 한다.**

---

## 5. Pruning 검증 방법

### 5.1 권장: `EXPLAIN ANALYZE VERBOSE` ⚠️ 미실행

Trino **476**부터 split 생성 과정의 상세 메트릭이 `EXPLAIN ANALYZE VERBOSE`에 표시된다 ([PR #25770](https://trino.io/docs/current/release/release-476.html)). Iceberg 스캔 리포트 데이터를 포함하며 **482에서 사용 가능**하다.

```sql
EXPLAIN ANALYZE VERBOSE
SELECT ...
WHERE ts = date_parse('20260819162112466', '%Y%m%d%H%i%s%f')
  AND par_a = 'a' AND col_b = 'd';
```

Iceberg ScanReport 기준 확인할 메트릭:

| 메트릭 | 의미 |
|--------|------|
| `totalDataManifests` | 전체 manifest 수 |
| `scannedDataManifests` | 실제로 연 manifest |
| `skippedDataManifests` | **Partition Pruning으로 건너뛴 manifest** |
| `resultDataFiles` | 최종 선택된 파일 |
| `skippedDataFiles` | **파일 통계로 건너뛴 파일** |
| `totalPlanningDuration` | 플래닝 소요 시간 |

**파티션 단계와 파일 단계가 분리되어 나오므로 어디서 얼마나 걸러졌는지 정확히 확인 가능하다.** 섹션 3의 "205 → 15가 파일 단계 효과"라는 해석도 이것으로 직접 확인할 수 있다.

> ⚠️ Trino가 실제로 어느 메트릭까지 노출하는지, 표시 이름이 무엇인지는 **미확인이다. 직접 실행 필요** (섹션 9).

### 5.2 `EXPLAIN (TYPE IO, FORMAT JSON)`

쿼리를 실행하지 않고 커넥터로 내려간 제약만 확인한다. 비교 대상이 필요 없다.

```sql
EXPLAIN (TYPE IO, FORMAT JSON) SELECT ... ;
```

`columnConstraints`에 해당 컬럼이 low/high bound와 함께 나오면 pushdown이 확인된다. **단, 섹션 4의 사유로 안 나와도 Pruning이 없다는 뜻은 아니다.**

### 5.3 Iceberg 메타데이터 테이블

```sql
-- 파일별 통계 (정렬 상태 = 파일 min/max 범위가 겹치지 않는지 확인)
SELECT file_path, record_count, file_size_in_bytes, readable_metrics
FROM iceberg.<schema>."<table>$entries";

-- 파티션 현황
SELECT partition, record_count, file_count, total_size
FROM iceberg.<schema>."<table>$partitions" ORDER BY 1;
```

> **`$files`가 아니라 `$partitions`를 쓴다.** `$files`는 컬럼 19개의 통계를 전부 끌고 오므로 조회 비용이 크다. 같은 판단이 `pipeline/compaction-executor-sizing-design.md`의 입력 크기 측정에도 적용되어 있다.
>
> `$files` / `$partitions`에는 **일반 컬럼 조건을 걸 수 없다.** 파티션 단위까지만 필터링된다.

---

## 6. splits와 파일 수

### 6.1 환산 관계 ⚠️

Iceberg [`read.split.target-size`](https://iceberg.apache.org/docs/latest/configuration/) 기본값이 **128MB**("데이터 입력 split을 결합할 때의 목표 크기")이므로:

```
512MB 파일 ≈ split 4개   →   splits ÷ 4 ≈ 파일 수
```

- 15 splits ≈ 파일 3~4개
- 205 splits ≈ 파일 51개 ← **섹션 3.1에서 실측 대조 완료** ✅

관련 속성: `read.split.open-file-cost` 4MB, `read.split.planning-lookback` 10.

> ⚠️ 환산식 자체는 128MB 기준의 계산이며 실측 대조점이 1건뿐이다. 파일 크기가 균일하지 않은 파티션에서는 어긋날 수 있다.

### 6.2 ⚠️ `read.split.target-size`를 512MB로 올리지 말 것

`write.target-file-size-bytes`(512MB)와 **목적이 완전히 다른 설정**이다.

| 설정 | 계층 | 역할 | 현재 값 |
|------|------|------|---------|
| `write.target-file-size-bytes` | 쓰기 | Compaction 출력 **파일** 크기 | 512MB (`compaction-tuning-guide.md` §5) |
| `read.split.target-size` | 읽기 | 조회 시 **작업 분배 단위** 크기 | 128MB (기본값) |

파일 크기에 맞춰 512MB로 올리면 **파일 1개 = split 1개**가 되어 **병렬성이 4~5배 하락한다.** 두 값이 다른 것은 불일치가 아니라 의도된 설계다.

> Trino가 이 속성을 쿼리에서 전달받지 못하고 Iceberg 기본값을 쓴다는 [Issue #10874](https://github.com/trinodb/trino/issues/10874)(2022)가 있으나 **482에서의 현황은 미확인**이다.

### 6.3 성능 측정 시 볼 지표

| 지표 | 의미 |
|------|------|
| `splits` | 작업 분배 단위. **파일 1개 = split 1개가 아니다** (6.1) |
| `physical input` | S3에서 실제 내려받은 압축 바이트. row group skip이 반영됨 |
| `Planning:` | 메타데이터 탐색 비용. **파티션 컬럼과 정렬 컬럼의 차이가 여기서만 드러난다** (섹션 7.1) |

세 개면 충분하다. CPU / 네트워크 / 워커 편중은 반복 측정 평균으로 상쇄된다.

**측정 시 주의**

- 캐시 영향 제거: `1번 → 2번 → 1번 → 2번` 순서로 교차 반복
- [공식 문서](https://trino.io/docs/current/sql/explain-analyze.html)상 통계는 **특히 빨리 끝나는 쿼리에서 정확하지 않을 수 있다**

> 지표 전반의 해석은 `schema/read-performance-test.md` §1의 "Trino Web UI Overview 주요 메트릭 해석" / "EXPLAIN ANALYZE 결과 해석 가이드"를 따른다. 이 문서는 Pruning 판정에 필요한 최소 지표만 다룬다.

---

## 7. 이 조사가 확인해 준 설계 판단

### 7.1 `hour(ts)` 파티션은 유효하다 — 근거가 보강됐다

`schema/read-performance-test.md`는 B안(`hour(ts)`, `par_a`)이 4개 테스트 케이스 전부 1위(A안 대비 5~31% 빠름)라는 **결과**를 기록했다. 이 조사는 그 **메커니즘**을 보여준다: 일 단위 → 시 단위에서 splits가 **4,938 → 205로 정확히 24.1배** 줄었다 (섹션 3.1). 하루의 시간 파티션 수 24와 일치하므로, `hour(ts)` Pruning이 설계대로 동작한다는 직접 증거다. ✅

**그런데 섹션 3.2에서 `ts` 조건을 빼도 읽는 양이 같았다. 그러면 `hour(ts)` 파티션은 필요 없는가? 아니다.**

| 단계 | `ts` (파티션 컬럼) | `sort_a` (정렬 컬럼) |
|------|---------------------|----------------------|
| manifest list | 파티션 경계로 manifest를 **통째로 스킵** | 못 거름 → **manifest 전수 조회** |
| manifest | 살아남은 것만 파일 통계 확인 | 모든 manifest의 파일 엔트리 대조 |
| 최종 결과 | 동일 | 동일 |
| 비용 | 낮음 | **플래닝 시간 / 메타데이터 I/O 높음** |

즉 두 컬럼은 **읽는 데이터 양은 같게 만들지만, 거기 도달하는 메타데이터 비용이 다르다.** 3개월 × 24시간 × `par_a` 4종 ≈ **8,640 파티션**이므로 데이터가 쌓일수록 격차가 벌어진다.

> ⚠️ **이 비용 차이는 아직 측정되지 않았다.** `Planning:` 시간 비교가 섹션 9의 최우선 항목인 이유다. 측정 전까지 "`ts` 없이도 된다"고 결론 내리면 안 된다.

파티션이 필요한 다른 이유도 그대로다 — 기간 범위 조회, 시간별 집계, **파티션 단위 재처리**(`pipeline/reprocessing-dag-design.md`), **만료 데이터 정리**, 그리고 **Compaction의 file group 단위 자체**(`compaction-tuning-guide.md` §2.1)가 파티션이다.

### 7.2 `sort_a`를 유지하는 것이 맞다

`sort_a`는 외부 시스템 호환 목적으로 존재하지만, 결과적으로 **Sort Order 1순위 + `ts`의 사본**이라는 조합이 파일 단위 Pruning을 강하게 만들고 있다 (섹션 3.2·3.3). 현행 유지가 맞다.

### 7.3 `par_a` 분포 — Compaction 측정과 교차 확인 ✅

| 출처 | 시점 | C | B | A | D |
|------|------|---|---|---|---|
| `compaction-tuning-guide.md` §1.3 (크기 비중) | 2026-08-11 | 57% | 31% | 7.5% | 2.4% |
| 이 조사 (시간당 파일 수 51/25/4/1) | 2026-08-19 | 63% | 31% | 4.9% | 1.2% |

**순위와 대략적 규모가 일치한다.** 총 파일 81개 × 약 500MB ≈ **40GB/시간**으로, CLAUDE.md에 기록된 "현재 데이터 36~42GB"와도 맞는다.

두 가지 주의:

- ⚠️ 파일 수 비중은 크기 비중의 근사일 뿐이다. A·D의 차이(7.5%→4.9%, 2.4%→1.2%)가 실제 분포 변화인지 시간대 변동인지는 이 데이터로 판별할 수 없다
- `par_a=D`가 파일 **1개**로 나온 것은 `compaction-tuning-guide.md` §4.3의 "D는 600~830MB라 512MB 기준으로 2개로 갈린다"와 다르다. **512MB 미만으로 떨어진 시간대**로 보이며, 같은 문서가 예측한 대로 그 경우 파일이 1개가 되어 `min_size` 문제가 사라진다. 모순이 아니라 예측대로의 동작이다
- `schema/` 문서의 `par_a` 분포(2026-03-18: B 43.4%, C 43.1%, A 12.4%, D 1.0%)와는 **순위가 다르다.** 5개월 사이의 분포 변화이거나 대상 테이블이 다른 것이며, 어느 쪽인지 확인이 필요하다 ⚠️

---

## 8. 기존 문서 반영 사항

### 8.1 `schema/trino-query-guide.md` — 반영 완료 (2026-09-05)

**① 컬럼 역할 정정 (§2.1, §4.1, §4.2, 예제 전반)** — 스키마 확정(섹션 1.3)으로 가이드의 Sort Order 기술이 실제와 달라져 있었다.

| 컬럼 | 가이드 기존 기술 | 정정 후 |
|------|------------------|---------|
| `sort_b` | Row-level Filter, 선택 | **Data Skipping (Sort Order 2순위), 필수** |
| `col_b` | Data Skipping (Sort Order 2순위), 필수 | **Row-level Filter, 선택** |

정정 당시 두 컬럼의 이름은 `sort_b`/`sort_c`로 **접두어가 실제 역할과 어긋나 있었다.** 같은 작업에서 역할 없는 컬럼을 `col_*`로 옮기는 명명 통일(섹션 1.3)을 함께 적용해 원인을 없앴다.

**② `ts =` 등가 비교 — "❌ 결과 없음"을 조건부로 분리 (§3.1, §3.7, §6.1)**

| | 기존 가이드가 상정한 경우 | 이 조사 §2.2 |
|---|---|---|
| 쓴 값 | `TIMESTAMP '2026-03-18'` (= 자정) / `DATE '2026-03-18'` | `TIMESTAMP '2026-08-19 16:21:12.466000'` (실재하는 시점) |
| 결과 | 결과 없음 | 정상 조회 + 파일 단위 Pruning (15 splits) |

**둘 다 맞다.** `ts =`는 마이크로초 단위 정확 매칭이므로 **날짜를 조회할 의도로** 쓰면 자정에 일치하는 행이 없어 결과가 비고, **정확한 시각을 알고** 쓰면 가장 강한 Pruning 수단이 된다. 기존 표현은 후자를 아는 사용자가 최선의 패턴을 피하게 만들었으므로 두 경우를 분리했다.

**③ "`ts` 없으면 모든 날짜의 데이터를 읽는다" 정정 (§6.2)**

섹션 3.2 실측상 `sort_a`/`sort_b` 등가 조건이 있으면 **`ts` 없이도 16.79MB / 15 splits**다. 읽는 것은 데이터가 아니라 **파일 목록(manifest)** 이므로 다음과 같이 고쳤다.

- "모든 날짜의 **데이터**를 읽는다" → "보관 중인 전체 기간(3개월)의 **파일 목록을 훑는다**"
- **비용이 `Physical input`이 아니라 `Planning` 시간에 쌓인다**는 점을 명시 — `Physical input`만 보고 "ts 없어도 된다"고 판단하는 것을 막는다
- 실측 숫자는 **확인된 것만** 인용했다 (sort 조건이 약할 때 264MB → 13.05GB). `Planning` 시간 자체는 미측정이므로 **배수를 쓰지 않았다** (섹션 9.1)

**④ 추가 (§6.2.1, §6.5, §7)**

| 위치 | 내용 | 출처 |
|------|------|------|
| §6.2.1 | 파티션 필터 강제 설정과 **그 한계** — 파티션 컬럼 하나면 통과하므로 `ts` 누락을 못 막는다 | 섹션 1.4 |
| §6.5 | `ts`에 쓸 수 있는/없는 함수 표. `date_trunc('week'\|'quarter')`는 **482에서만 안 됨**(484부터 지원) | 섹션 2.3 |
| §7 | **Domain 표시로 Pruning을 판단하지 말 것** | 섹션 4 |

### 8.2 남은 반영 대상

| 대상 | 내용 | 상태 |
|------|------|------|
| `schema/iceberg-schema-design-guide.md` | **`sort_a`가 `ts`의 문자열 사본**이라는 성질과 그 성능 함의 (섹션 1.3·3.2), 컬럼 역할 표 정정 | **반영 완료 (2026-09-05)** |
| `schema/read-performance-test.md` §5 | 확정 Sort Order가 테스트한 4개 조합 어디에도 없다는 점 — 성능 차이가 없어 다른 기준으로 선택됐으므로 이 절에서 확정값을 역산하면 안 된다 | **반영 완료 (2026-09-05)** |
| 전 문서 명명 통일 | 섹션 1.3의 규칙을 CLAUDE.md·`tuning/`·`schema/` 전체에 적용 | **반영 완료 (2026-09-05)** |

---

## 9. 미확정 및 후속 과제

### 9.1 남은 확인 항목

| 항목 | 내용 | 우선순위 |
|------|------|---------|
| `Planning:` 시간 비교 | `ts` 조건 있음 vs 없음(`sort_a`만). **섹션 7.1의 논리를 숫자로 확정하는 유일한 측정.** 현재 `trino-query-guide.md` §6.2는 "비용이 Planning에 쌓인다"고만 쓰고 **배수를 제시하지 못한 상태**다 | **높음** |
| `EXPLAIN ANALYZE VERBOSE` 실행 | `skippedDataManifests` / `skippedDataFiles`로 파티션 단계와 파일 단계를 분리 확인 (섹션 5.1). 메트릭 노출 여부·표시 이름 미확인 | **높음** |
| 예전 자료 인용 시 이름 변환 | 2026-09-05 이전 회의 자료·캡처·커밋 메시지는 옛 이름이다. 특히 **`col_a`는 예전에 파티션 컬럼을 뜻했다** — 섹션 1.3의 변환표를 거치지 않고 숫자를 옮기면 다른 컬럼 이야기가 된다 | 중간 |
| 파티션 필터 강제 설정값 확인 | `iceberg.query-partition-filter-required`가 실제로 `true`인지, 스키마 한정인지 (섹션 1.4) | 중간 |
| splits ↔ 파일 수 검증 | 파일 수가 다른 파티션(51/25/4/1)에 같은 쿼리를 돌려 splits가 파일 수에 비례하는지 확인 (섹션 6.1) | 중간 |
| `$entries` 정렬 상태 확인 | `readable_metrics`로 `ts` 파일별 lower/upper bound가 겹치지 않는지 육안 확인. 섹션 3.3의 전제 | 중간 |
| `par_a` 분포 불일치 | `schema/` 문서(2026-03-18)와 순위가 다른 원인 — 분포 변화인지 다른 테이블인지 (섹션 7.3) | 중간 |
| 482 릴리즈 노트 확인 | 본 문서 근거의 상당수가 396~476 시점 자료다. unwrap 룰 / split 설정 변경 여부 | 낮음 |
| Issue #10874 현황 | Trino가 `read.split.target-size`를 전달받는지 (섹션 6.2) | 낮음 |

### 9.2 재검증 트리거

```
⚠️ 다음 상황에서 이 문서의 결론을 재검증해야 한다:

1. Trino 484 이상으로 업그레이드
   → date_trunc('week'|'quarter') Pruning이 동작하기 시작한다 (섹션 2.3).
     섹션 8.3의 안내 문구도 함께 정리해야 한다

2. Sort Order 변경 — 특히 sort_a가 1순위에서 빠질 때
   → 섹션 3.2·3.3의 관측이 통째로 무효가 된다. sort_a가 ts의 사본이면서
     1순위 정렬 키라는 조합이 파일 단위 Pruning의 유일한 근거다

3. Compaction 전략을 sort → binpack으로 변경
   → 물리 정렬이 깨져 파일 min/max가 전 구간을 덮는다 (섹션 3.3)

4. write.target-file-size-bytes 변경
   → splits ÷ 4 환산이 바뀐다 (섹션 6.1). read.split.target-size는
     따라 올리지 말 것 (섹션 6.2)

5. 파티션 스펙 변경 (hour → day 등)
   → 섹션 3.1의 24.1배가 근거를 잃는다

6. Iceberg 1.11.0 업그레이드 (작업 7)
   → 메타데이터 계층 변경이 manifest pruning 동작에 영향을 줄 수 있다
```

---

## 10. 참고 자료

**핵심 근거**

- [Trino Blog — Date Predicates (2023-04-11)](https://trino.io/blog/2023/04/11/date-predicates.html) — unwrap 룰 전반
- [Trino Docs — Date/Time Functions](https://trino.io/docs/current/functions/datetime.html) — `date(x)` = `CAST(x AS date)`
- [Trino Docs — EXPLAIN / IO 플랜](https://trino.io/docs/current/sql/explain.html) — `columnConstraints` / `domain` 구조
- [Trino Docs — Iceberg 커넥터](https://trino.io/docs/current/connector/iceberg.html) — `query-partition-filter-required`, 메타데이터 테이블
- [Iceberg — 테이블 설정](https://iceberg.apache.org/docs/latest/configuration/) — `read.split.target-size`
- [Trino Release 476](https://trino.io/docs/current/release/release-476.html) — VERBOSE split 메트릭

**이슈·PR (섹션 2.3·2.4의 버전 판정 근거)**

| 번호 | 내용 | 상태 |
|------|------|------|
| [PR #14011](https://github.com/trinodb/trino/pull/14011) | `UnwrapDateTruncInComparison` | 머지 |
| [PR #14161](https://github.com/trinodb/trino/pull/14161) | `date_trunc('hour')` | 머지 |
| [Issue #14078](https://github.com/trinodb/trino/issues/14078) | `year()` pushdown | PR #16106으로 해결 |
| [Issue #19266](https://github.com/trinodb/trino/issues/19266) | 부분 구간 Pruning "실패" 제보 | **PR #24740(milestone 469)로 종결 — 오독이었음** |
| [Issue #30192](https://github.com/trinodb/trino/issues/30192) | `date_trunc('week'\|'quarter')` 미지원 | **PR #30197(milestone 484)로 해결. 482는 미포함** |
| [Issue #10874](https://github.com/trinodb/trino/issues/10874) | split size 설정 전달 불가 | 482 현황 미확인 |

**프로젝트 내부**

- `schema/trino-query-guide.md` — 사용자용 쿼리 가이드 (섹션 8의 반영 대상)
- `schema/read-performance-test.md` §5 — Sort Order / Bloom Filter 읽기 성능 실측, 지표 해석 가이드
- `schema/iceberg-schema-design-guide.md` — 파티션·Sort Order 설계 근거
- `tuning/compaction-tuning-guide.md` §1.2·§2.1·§4.3 — `sort` 전략 필수 근거, file group, `par_a=D` 파일 크기
- `pipeline/compaction-executor-sizing-design.md` — `.partitions` 조회와 `ts` NTZ 변환
- `pipeline/reprocessing-dag-design.md` — 파티션 단위 재처리
