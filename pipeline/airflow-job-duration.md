# Airflow job 실행 시간 집계 (리소스 시각화용)

| 항목 | 내용 |
|------|------|
| 목적 | 하루 동안 시각별로 실제 동시에 떠 있는 core·memory를 그리기 위해, job별 **Duration**과 **Start Offset**을 최근 100회 실측으로 구한다 |
| 대상 | append·Compaction·maintenance 등 Spark pod를 띄우는 task 전부 |
| 방법 | Airflow 3.x REST API v2 (권장, Python) 또는 메타데이터 DB SQL — 두 결과는 같다 |
| 검증 | Airflow 3.2.2 OpenAPI 명세로 endpoint·파라미터·응답 필드 확인. 운영과 같은 구조의 DAG(append 2종, summary, hourly·daily Compaction mapped task, maintenance 3종)를 가짜 Airflow API 서버와 PostgreSQL 16에 넣고 두 방법의 결과가 순서·값까지 일치함을 확인. 기간 지정(KST 하루)도 손으로 센 실행 횟수와 일치 (2026-09-28) |

---

## 1. 무엇을 뽑나

**설정값을 단순 합산하면 의미가 없다.** cron이 다른 job들을 전부 더하면 "모든 job이 동시에 떠 있는 순간"이라는 존재하지 않는 값이 나온다. 필요한 것은 **시각 t에 실제로 떠 있는 job들의 합**이고, 그러려면 job마다 "언제 시작해서 언제 끝나나"를 알아야 한다. cron은 시작 예정 시각만 주므로 두 값이 더 필요하다.

| 엑셀 열 이름 | 뜻 | 예 (가상 수치) |
|------|------|------|
| **Duration (min)** | task가 시작해서 끝날 때까지 걸린 시간. 최근 100회 평균 | hourly Compaction 1번 테이블 1.5 |
| **Start Offset (min)** | **cron 예정 시각부터 task가 실제로 시작하기까지** 걸린 시간. 최근 100회 평균 | 같은 DAG 2번 테이블 1.9 |

그래프에서는 이렇게 쓴다.

```
실제 시작 = cron 시각 + Start Offset
실제 종료 = 실제 시작 + Duration
```

**순차 실행이 Start Offset에 자동으로 들어간다.** hourly Compaction은 엑셀에 4개 테이블 모두 `45 * * * *`로 적혀 있지만 실제로는 1번이 끝나야 2번이 시작한다. 1번이 1.5분 걸리면 2번의 Start Offset은 약 1.9분이 된다(앞 task 시간 + task 사이 대기). 이 값을 실측으로 가져오므로 순서를 손으로 누적할 필요가 없다.

| 테이블 (예시) | cron | Start Offset | Duration | 실제 구간 |
|---|---|---|---|---|
| 1번 | :45 | 0.3 | 1.5 | :45.3 ~ :46.8 |
| 2번 | :45 | 1.9 | 1.8 | :46.9 ~ :48.7 |
| 3번 | :45 | 3.9 | 1.7 | :48.9 ~ :50.6 |
| 4번 | :45 | 5.8 | 1.7 | :50.8 ~ :52.5 |

> **왜 "Delay"가 아니라 "Offset"인가.** Delay는 "늦어졌다"는 뜻이라 문제처럼 읽힌다. 순차 실행에서 2번 테이블이 1.9분 뒤에 시작하는 것은 문제가 아니라 설계된 자리다. Offset은 "기준 시각에서 얼마나 떨어져 있나"라는 중립적인 말이다. 스케줄러 지연(수 초)도 이 값에 같이 들어간다.

---

## 2. 방법 선택 — REST API를 권장한다

| | REST API (Python) | 메타데이터 DB (SQL) |
|---|---|---|
| 접근 | Airflow 계정 하나. 읽기 전용 | DB 접속 정보 필요. 운영 DB에 직접 쿼리 |
| 버전 업그레이드 | API v2는 공개 규약이라 유지된다 | 테이블 구조가 바뀔 수 있다 (예: `dag_run.run_after`는 3.0에서 생긴 컬럼) |
| 편의 | 대상 task 자동 선택, CSV 바로 저장, cron까지 같이 가져옴 | 쿼리 한 번. 가장 빠름 |
| 단점 | 한 번에 100개까지만 줘서 페이지를 넘긴다 (스크립트가 처리) | 집계 대상 task 이름을 쿼리에 직접 적어야 한다 |

DB에도 붙을 수 있으면 둘 다 쓸 수 있다. **정기적으로 다시 뽑을 거라면 REST API**, 한 번 확인용이면 SQL이 빠르다.

---

## 3. DAG 구조별 집계 대상

Spark pod를 띄우는 task만 센다. 아래 task는 Spark job이 아니므로 뺀다.

- append의 `get_jobs`·`update_success`·`update_failure`: Airflow worker 안에서 도는 Python task라 클러스터 core·memory를 쓰지 않는다
- `iceberg_delete_expired_data`의 `validate_target_dt`·`del_expired_data`: Spark job은 `del_expired_snapshots`뿐이다. `del_expired_data`도 Spark job이면 `TASK_NAMES`에 넣으면 된다(operator 이름에 `Spark`가 있으면 자동으로 잡힌다)

| DAG 종류 | 집계하는 task_id | 엑셀 대상(target) 표시 |
|---|---|---|
| 수직분할 4개 테이블 append (DAG 1개, 테이블별 병렬) | `<테이블명>.append_data` | TaskGroup 이름 = **테이블명** |
| 그 외 테이블 append (테이블별 DAG) | `convert_files.append_data` | **dag_id** (DAG 하나가 테이블 하나) |
| summary (`iceberg_summary_<alias>`, 테이블별 DAG) | `summary_<alias>` (DAG 안 task 3개 중 Spark job 하나) | 접두어를 뗀 **alias** |
| hourly·daily Compaction | `compaction` (mapped task) | map index 이름 = **테이블명**. hourly·daily는 DAG의 cron으로 구분(시 자리가 `*`면 hourly) |
| rewrite manifests (`iceberg_rewrite_manifest`) | `<번호>_<테이블명>` (예: `1_table_a` ~ `25_...`) | 번호를 뗀 **테이블명** |
| orphan 파일 삭제 (`iceberg_delete_orphan_files`) | `<번호>_<테이블명>` | 번호를 뗀 **테이블명** |
| 만료 데이터·snapshot 삭제 (`iceberg_delete_expired_data`) | `<번호>_<테이블명>.del_expired_snapshots` | TaskGroup 이름에서 번호를 뗀 **테이블명** |
| 위 규칙에 안 걸리는 Spark task | operator 이름에 `Spark`가 든 task | dag_id |

> **번호가 붙은 task는 번호 순서대로 Start Offset이 쌓인다.** rewrite manifests·orphan 삭제·만료 데이터 삭제 DAG는 테이블 task가 `1_`, `2_` … `25_` 순서로 돈다. 순차 실행이면 Compaction처럼 뒤 번호일수록 Start Offset이 커지고, 병렬이면 전부 비슷하게 나온다. 어느 쪽이든 실측값 그대로 그래프에 들어간다.

> **mapped task의 테이블명.** Compaction은 테이블마다 같은 task를 복제해 돌리는 mapped task라 task_id가 `compaction` 하나다. 각 복제본은 `map_index` 번호(0, 1, 2, 3)로 구분되고, `map_index_template`을 설정했다면 그 번호 대신 테이블명(`rendered_map_index`)이 기록된다. 설정하지 않았다면 target 열에 0·1·2·3이 나오며, 순서는 `expand`에 넘긴 테이블 목록 순서다.

---

## 4. Python 스크립트 (REST API) — 권장

### 4.1 실행

```bash
pip install requests
export AIRFLOW_URL=http://<airflow-api-server>:8080   # UI 주소와 같다
export AIRFLOW_USER=<계정>
export AIRFLOW_PASSWORD=<비밀번호>
python job_durations.py                      # job별 최근 100회
python job_durations.py 20260921 20260922    # 9/21 00시 ~ 9/22 24시(KST) 실행 전부
python job_durations.py 20260921             # 9/21 하루
```

| 실행 방법 | 무엇을 평균 내나 | 결과 파일 |
|---|---|---|
| 인자 없음 | job(테이블)별 **최근 100회** | `job_durations.csv` |
| 날짜 2개 `시작일 종료일` | 그 기간의 실행 **전부**(100회 제한 없음). 종료일은 그날 24시까지 포함 | `job_durations_<시작일>_<종료일>.csv` |
| 날짜 1개 | 그 하루의 실행 전부 | `job_durations_<날짜>_<날짜>.csv` |

- 날짜는 `YYYYMMDD` 형식이고 **한국 시간(KST)** 기준이다. `20260921`은 9/21 00:00~24:00 KST이다. 시간대는 스크립트 상단 `TZ`로 바꾼다
- 기간은 **cron 예정 시각**(`run_after`)으로 자른다. 9/21 23:45에 시작 예정이던 hourly Compaction은 실제로 9/22 0시를 넘겨 끝나도 9/21에 들어간다
- 결과는 엑셀에서 바로 열린다(한글 깨짐 방지 인코딩). 정렬은 append → summary → hourly Compaction → daily Compaction → expired snapshot → delete orphan → rewrite manifest 순이고, 같은 종류 안에서는 테이블명 오름차순이다

### 4.2 고칠 수 있는 설정 (스크립트 상단)

| 설정 | 기본값 | 언제 바꾸나 |
|------|--------|------------|
| `N_RUNS` | 100 | 기간을 안 줬을 때 평균 낼 최근 실행 횟수 |
| `TZ` | KST (UTC+9) | 기간 날짜를 해석할 시간대 |
| `TASK_NAMES` | `append_data`, `compaction`, `del_expired_snapshots` | 집계할 task 이름(점 뒤 부분). 새 Spark task가 생기면 이름을 추가 |
| `TASK_PREFIXES` | `summary_` | 이 접두어로 시작하는 task를 집계. 대상 이름은 접두어를 뗀 alias |
| `NUMBERED_TASK` | `^\d+_` | `번호_테이블명` 형태 task를 잡는 규칙 (rewrite manifests·orphan 삭제). 대상 이름에서 번호를 뗀다 |
| `MATCH_SPARK_OPERATOR` | `True` | operator 이름에 `Spark`가 든 task를 자동 포함. 위 두 규칙에 안 걸리는 Spark task를 놓치지 않기 위한 안전장치 |
| `DAG_ID_PREFIX` | `None` (전체) | 특정 DAG만 볼 때 dag_id 접두어 |
| `GENERIC_GROUPS` | `convert_files` | 테이블명이 아닌 TaskGroup 이름. 이 그룹이면 target을 dag_id로 표시 |
| `VERIFY` | `True` | 사내 인증서를 쓰면 CA 파일 경로 |
| `MAX_PAGES` | 100 | task 하나당 최대 몇 페이지(100건씩)를 훑을지. 긴 기간을 줄 때 모자라면 경고가 뜬다 |
| `JOB_TYPE_ORDER` | append … rewrite manifest, other | 출력 정렬 순서. 순서를 바꾸려면 이 목록만 바꾼다 |

### 4.3 스크립트

```python
"""Airflow Spark job의 Duration·Start Offset 집계 (Airflow 3.x REST API v2).

실행:
    export AIRFLOW_URL=http://<airflow-api-server>:8080
    export AIRFLOW_USER=<계정>  AIRFLOW_PASSWORD=<비밀번호>
    python job_durations.py                      # job별 최근 100회      → job_durations.csv
    python job_durations.py 20260921 20260922    # 9/21 00시 ~ 9/22 24시 → job_durations_20260921_20260922.csv
    python job_durations.py 20260921             # 9/21 하루             → job_durations_20260921_20260921.csv
"""
import csv
import os
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

import requests

# ── 설정 ──────────────────────────────────────────────────────────────
AIRFLOW_URL = os.environ["AIRFLOW_URL"].rstrip("/")
USERNAME = os.environ["AIRFLOW_USER"]
PASSWORD = os.environ["AIRFLOW_PASSWORD"]
N_RUNS = 100                    # 기간을 안 줬을 때 job(테이블)별 최근 몇 회를 볼지
TZ = timezone(timedelta(hours=9))   # 기간 날짜(YYYYMMDD)를 해석할 시간대 = KST
OUTPUT = "job_durations.csv"
VERIFY = True                   # 사내 CA면 인증서 경로 문자열로 (예: "/etc/ssl/certs/ca.pem")

# 집계 대상 task — task_id의 마지막 부분(점 뒤) 기준
#   append_data           : append (수직분할 4개 DAG·테이블별 DAG)
#   compaction            : hourly·daily Compaction (mapped task)
#   del_expired_snapshots : iceberg_delete_expired_data 의 snapshot 삭제 Spark job
TASK_NAMES = {"append_data", "compaction", "del_expired_snapshots"}
# 이 접두어로 시작하는 task (iceberg_summary_<alias> DAG 의 summary_<alias>). 대상 이름은 접두어를 뗀 alias
TASK_PREFIXES = ("summary_",)
# "번호_테이블명" 형태의 task (iceberg_rewrite_manifest·iceberg_delete_orphan_files, 예: 1_table_a)
NUMBERED_TASK = re.compile(r"^\d+_")
# operator 이름에 "Spark"가 들어간 task도 자동 포함 (위 규칙에 안 걸리는 Spark task를 놓치지 않기 위한 안전장치)
MATCH_SPARK_OPERATOR = True
# 특정 DAG만 볼 때 접두어 (None이면 전체 DAG)
DAG_ID_PREFIX = None
# 이 TaskGroup 이름은 테이블명이 아니다 → 대상(target)을 dag_id로 표시
GENERIC_GROUPS = {"convert_files"}
# 페이지 상한 (100개 × 100 = task 하나당 최근 1만 건까지만 훑는다)
MAX_PAGES = 100
# 출력 정렬 순서 (job_type 열). 같은 job_type 안에서는 대상(테이블명) 오름차순
JOB_TYPE_ORDER = ["append", "summary", "hourly compaction", "daily compaction",
                  "expired snapshot", "delete orphan", "rewrite manifest", "other"]
# ─────────────────────────────────────────────────────────────────────

session = requests.Session()
session.verify = VERIFY


def login():
    r = session.post(f"{AIRFLOW_URL}/auth/token",
                     json={"username": USERNAME, "password": PASSWORD}, timeout=30)
    r.raise_for_status()
    session.headers["Authorization"] = f"Bearer {r.json()['access_token']}"


def get(path, **params):
    r = session.get(f"{AIRFLOW_URL}/api/v2{path}", params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def parse_time(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def parse_period(args):
    """인자 없음 → None (최근 N_RUNS회). YYYYMMDD 1~2개 → (시작, 끝) UTC. 끝 날짜는 그날 24시까지 포함."""
    if not args:
        return None
    if len(args) > 2:
        raise SystemExit("사용법: python job_durations.py [시작일 [종료일]]   예) 20260921 20260922")
    try:
        days = [datetime.strptime(a, "%Y%m%d").replace(tzinfo=TZ) for a in args]
    except ValueError:
        raise SystemExit(f"날짜는 YYYYMMDD 형식이어야 한다: {' '.join(args)}")
    start, end = days[0], days[-1] + timedelta(days=1)
    if start >= end:
        raise SystemExit("시작일이 종료일보다 늦다")
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def list_dags():
    """활성 DAG 전체 (dag_id, cron). API는 한 번에 최대 100개라 페이지를 넘긴다."""
    dags, offset = [], 0
    while True:
        params = {"limit": 100, "offset": offset, "exclude_stale": "true"}
        if DAG_ID_PREFIX:
            params["dag_id_prefix_pattern"] = DAG_ID_PREFIX
        page = get("/dags", **params)["dags"]
        dags += [(d["dag_id"], d.get("timetable_summary") or "") for d in page]
        if len(page) < 100:
            return dags
        offset += 100


def target_tasks(dag_id):
    """DAG 안에서 집계할 Spark task만 고른다."""
    picked = []
    for t in get(f"/dags/{dag_id}/tasks")["tasks"]:
        name = t["task_id"].rsplit(".", 1)[-1]
        is_spark = MATCH_SPARK_OPERATOR and "Spark" in (t.get("operator_name") or "")
        if (name in TASK_NAMES or name.startswith(TASK_PREFIXES)
                or NUMBERED_TASK.match(name) or is_spark):
            picked.append((t["task_id"], bool(t.get("is_mapped"))))
    return picked


def collect_runs(dag_id, task_id, is_mapped, period):
    """cron으로 돈(scheduled) 성공 실행만, 최신순.
    기간이 없으면 최근 N_RUNS회, 있으면 그 기간 전부. mapped task는 테이블별로 센다."""
    params = {"task_id": task_id, "state": "success",
              "run_id_prefix_pattern": "scheduled",   # 재처리·수동 trigger(manual__) 제외
              "order_by": "-run_after", "limit": 100}
    if period:
        params["run_after_gte"], params["run_after_lt"] = (
            t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in period)
    cap = None if period else N_RUNS
    by_key = defaultdict(list)
    for page_no in range(MAX_PAGES):
        page = get(f"/dags/{dag_id}/dagRuns/~/taskInstances",
                   offset=page_no * 100, **params)["task_instances"]
        for ti in page:
            if ti["duration"] is None or ti["start_date"] is None:
                continue
            key = (ti.get("rendered_map_index") or str(ti["map_index"])) if is_mapped else None
            if cap is None or len(by_key[key]) < cap:
                by_key[key].append(ti)
        full = cap is not None and bool(by_key) and all(len(v) >= cap for v in by_key.values())
        if len(page) < 100 or full:
            break
    else:
        print(f"  ⚠ {dag_id} {task_id}: 최근 {MAX_PAGES * 100}건까지만 봤다 (MAX_PAGES)")
    return by_key


def target_of(dag_id, task_id, key):
    if key is not None:                        # mapped (Compaction): 테이블명
        return key
    name = task_id.rsplit(".", 1)[-1]
    for prefix in TASK_PREFIXES:               # summary_<alias> → alias
        if name.startswith(prefix):
            return name[len(prefix):]
    if "." in task_id:                         # TaskGroup 안의 task
        group = task_id.rsplit(".", 1)[0]
        if group in GENERIC_GROUPS:            # 테이블별 append DAG: dag_id가 곧 테이블
            return dag_id
        return NUMBERED_TASK.sub("", group)    # 수직분할 append: 테이블명 / delete_expired_data: "3_table_a" → table_a
    if NUMBERED_TASK.match(task_id):           # rewrite_manifest·delete_orphan_files: "3_table_a" → table_a
        return NUMBERED_TASK.sub("", task_id)
    return dag_id


def is_hourly(dag_id, cron):
    """cron의 '시' 자리가 * 이면 매시간 도는 DAG. cron을 못 읽으면 dag_id로 판단."""
    fields = cron.split()
    if cron.strip() == "@hourly":
        return True
    if len(fields) == 5:
        return fields[1] in ("*", "*/1")
    return "hourly" in dag_id


def job_type_of(dag_id, task_id, cron):
    name = task_id.rsplit(".", 1)[-1]
    if name == "append_data":
        return "append"
    if name.startswith("summary_"):
        return "summary"
    if name == "compaction":
        return "hourly compaction" if is_hourly(dag_id, cron) else "daily compaction"
    if name == "del_expired_snapshots":
        return "expired snapshot"
    if dag_id == "iceberg_delete_orphan_files":
        return "delete orphan"
    if dag_id == "iceberg_rewrite_manifest":
        return "rewrite manifest"
    return "other"


def to_min(seconds):
    """초 → 분, 소수점 1자리 (반올림은 SQL ROUND와 같은 사사오입)."""
    return float(Decimal(str(seconds / 60)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))


def main():
    args = sys.argv[1:]
    period = parse_period(args)
    output = OUTPUT if not period else OUTPUT.replace(".csv", f"_{args[0]}_{args[-1]}.csv")
    if period:
        print(f"기간: {args[0]} 00:00 ~ {args[-1]} 24:00 (UTC {period[0]:%Y-%m-%d %H:%M} ~ {period[1]:%Y-%m-%d %H:%M}), 그 기간 실행 전부")
    else:
        print(f"기간 지정 없음: job별 최근 {N_RUNS}회")
    login()
    rows = []
    for dag_id, cron in list_dags():
        for task_id, is_mapped in target_tasks(dag_id):
            for key, tis in collect_runs(dag_id, task_id, is_mapped, period).items():
                durations = [ti["duration"] for ti in tis]
                offsets = [(parse_time(ti["start_date"]) - parse_time(ti["run_after"])).total_seconds()
                           for ti in tis]
                rows.append({
                    "job_type": job_type_of(dag_id, task_id, cron),
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "target": target_of(dag_id, task_id, key),
                    "cron": cron,
                    "runs": len(tis),
                    "Duration (min)": to_min(statistics.mean(durations)),
                    "Duration median (min)": to_min(statistics.median(durations)),
                    "Duration max (min)": to_min(max(durations)),
                    "Start Offset (min)": to_min(statistics.mean(offsets)),
                    "oldest_run": tis[-1]["run_after"],
                    "latest_run": tis[0]["run_after"],
                })
                r = rows[-1]
                print(f"{r['job_type']:18s} {dag_id:34s} {r['target']:20s} runs={len(tis):4d} "
                      f"dur={r['Duration (min)']:5.1f} offset={r['Start Offset (min)']:5.1f}")
    if not rows:
        raise SystemExit("집계 대상이 없다 — 기간·TASK_NAMES·DAG_ID_PREFIX를 확인")
    rows.sort(key=lambda r: (JOB_TYPE_ORDER.index(r["job_type"]), r["target"], r["dag_id"], r["task_id"]))
    with open(output, "w", newline="", encoding="utf-8-sig") as f:   # utf-8-sig: 엑셀 한글 깨짐 방지
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"\n{len(rows)}행 → {output}")


if __name__ == "__main__":
    main()
```

### 4.4 무엇을 거르는가

| 필터 | 이유 |
|------|------|
| `run_id_prefix_pattern=scheduled` | **cron으로 돈 실행만.** 재처리 DAG가 trigger한 Compaction(20시간치면 10분 이상)은 run_id가 `manual__`로 시작해 빠진다. 이게 섞이면 평균이 튄다 |
| `state=success` | 실패한 실행은 중간에 끊겨 duration이 짧게 잡힌다 |
| `order_by=-run_after` | 최신순. 앞에서부터 N회만 쓴다 |
| mapped task는 테이블별로 N회 | 한 DAG 실행에 테이블 4개가 있으므로 페이지를 넘기며 테이블마다 100회를 채운다 |
| `run_after_gte`·`run_after_lt` (기간을 줬을 때만) | cron 예정 시각이 그 기간 안인 실행만. 이때는 100회 제한 없이 전부 쓴다 |

### 4.5 출력 형식

아래는 가짜 데이터로 돌린 **형식 예시**다. 숫자는 실측이 아니다.

```text
job_type,dag_id,task_id,target,cron,runs,Duration (min),Duration median (min),Duration max (min),Start Offset (min),oldest_run,latest_run
append,append_vertical,table_a.append_data,table_a,*/5 * * * *,100,3.0,3.0,3.0,0.3,2026-09-27T15:45:00Z,2026-09-28T00:00:00Z
summary,iceberg_summary_alpha,summary_alpha,alpha,10 * * * *,100,2.0,2.0,2.0,0.2,2026-09-23T21:10:00Z,2026-09-28T00:10:00Z
hourly compaction,iceberg_compaction_hourly,compaction,table_1,45 * * * *,100,1.5,1.5,1.5,0.3,2026-09-23T21:45:00Z,2026-09-28T00:45:00Z
hourly compaction,iceberg_compaction_hourly,compaction,table_2,45 * * * *,100,1.8,1.8,1.8,1.9,2026-09-23T21:45:00Z,2026-09-28T00:45:00Z
daily compaction,iceberg_compaction_daily,compaction,table_1,0 1 * * *,60,30.0,30.0,30.0,0.2,2026-07-31T01:00:00Z,2026-09-28T01:00:00Z
expired snapshot,iceberg_delete_expired_data,1_table_a.del_expired_snapshots,table_a,0 3 * * *,60,1.5,1.5,1.5,1.3,2026-07-31T03:00:00Z,2026-09-28T03:00:00Z
delete orphan,iceberg_delete_orphan_files,1_table_a,table_a,0 5 * * *,60,2.0,2.0,2.0,0.1,2026-07-31T05:00:00Z,2026-09-28T05:00:00Z
rewrite manifest,iceberg_rewrite_manifest,1_table_a,table_a,0 6 */3 * *,60,0.8,0.8,0.8,0.1,2026-07-31T06:00:00Z,2026-09-28T06:00:00Z
```

| 열 | 뜻 |
|---|---|
| `job_type` | 정렬용 종류. append·summary·hourly compaction·daily compaction·expired snapshot·delete orphan·rewrite manifest, 어디에도 안 맞으면 other |
| `runs` | 실제로 평균에 쓴 횟수. 100보다 작으면 이력이 그만큼밖에 없다 (예: 하루 1회 job은 보존 기간만큼) |
| `Duration median (min)` | 중앙값. **평균과 크게 다르면 튀는 실행이 섞였다는 신호** (4.6) |
| `Duration max (min)` | 최댓값. peak를 보수적으로 그릴 때 평균 대신 쓴다 |
| `oldest_run`·`latest_run` | 평균에 쓴 실행 중 가장 오래된·최근 실행의 cron 시각 (UTC). `latest_run`이 오래됐으면 멈춘 DAG다 |

### 4.6 Duration 평균과 중앙값의 차이

- **평균(`Duration (min)`)** = 100회의 시간을 모두 더해 100으로 나눈 값
- **중앙값(`Duration median (min)`)** = 100회를 짧은 순으로 줄 세웠을 때 가운데(50·51번째) 값

평소에는 둘이 거의 같다. 다르게 나오는 것은 **유난히 긴 실행이 몇 번 섞였을 때**다. 예를 들어 99회는 2.0분이고 1회만 30분 걸렸다면:

| | 계산 | 결과 |
|---|---|---|
| 평균 | (2.0 × 99 + 30) ÷ 100 | **2.3분** |
| 중앙값 | 줄 세운 가운데 값 | **2.0분** |

긴 실행 한 번이 평균은 끌어올리지만 중앙값은 그대로 둔다. 그래서 둘의 차이가 크면 그 job에 가끔 튀는 날이 있다는 뜻이다(데이터가 몰린 시간대, 클러스터가 붐빈 시각 등).

| 그래프 용도 | 쓸 값 |
|---|---|
| 평소 하루가 어떻게 생겼나 | **중앙값** — 한두 번 튄 실행에 끌려가지 않는다 |
| 자원을 얼마나 잡아 둬야 하나 | 평균 또는 **최댓값** — 튀는 날까지 감안한다 |

---

## 5. SQL (메타데이터 DB 직접) — PostgreSQL

위 스크립트와 같은 결과를 같은 순서로 쿼리 한 번에 낸다. 대상 task는 `WHERE`의 task 조건에 직접 적는다. SQL은 operator 이름을 모르므로 `MATCH_SPARK_OPERATOR`에 해당하는 자동 포함은 없다 — 새 Spark task는 `OR ti.task_id LIKE '%<이름>'`으로 추가한다.

**기간 지정**은 맨 위 `params`의 `NULL` 두 개를 바꾼다. 비워 두면 job별 최근 100회, 채우면 그 기간 전부다. 종료는 **종료일 다음 날 0시**로 적는다(9/21~9/22면 `'2026-09-23 00:00:00+09'`).

```sql
-- Airflow 3.x 메타데이터 DB (PostgreSQL) — Spark job의 Duration·Start Offset
-- 기간 지정: 아래 params의 NULL 두 개를 바꾼다. 비워 두면 job별 최근 100회
WITH params AS (
  SELECT NULL::timestamptz AS period_from,   -- 예) TIMESTAMPTZ '2026-09-21 00:00:00+09'
         NULL::timestamptz AS period_to      -- 예) TIMESTAMPTZ '2026-09-23 00:00:00+09'  (종료일 다음 날 0시)
),
recent AS (
  SELECT ti.dag_id,
         ti.task_id,
         COALESCE(d.timetable_summary, '')                                AS cron,
         COALESCE(ti.rendered_map_index,                                  -- mapped: 테이블명
                  CASE WHEN ti.map_index >= 0 THEN ti.map_index::text END) AS map_key,
         ti.duration,                                                     -- 초
         EXTRACT(EPOCH FROM ti.start_date - dr.run_after) AS offset_sec,  -- cron 시각 → 실제 시작
         dr.run_after,
         ROW_NUMBER() OVER (
           PARTITION BY ti.dag_id, ti.task_id,
                        COALESCE(ti.rendered_map_index, ti.map_index::text)
           ORDER BY dr.run_after DESC) AS rn
  FROM task_instance ti
  JOIN dag_run dr ON dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id
  LEFT JOIN dag d ON d.dag_id = ti.dag_id
  CROSS JOIN params p
  WHERE ti.state = 'success'
    AND dr.run_type = 'scheduled'           -- 재처리·수동 trigger 제외
    AND ti.duration IS NOT NULL
    AND ti.start_date IS NOT NULL
    AND (p.period_from IS NULL OR dr.run_after >= p.period_from)
    AND (p.period_to   IS NULL OR dr.run_after <  p.period_to)
    AND (ti.task_id LIKE '%append_data'               -- append
         OR ti.task_id ~ '^summary_'                     -- iceberg_summary_<alias> 의 summary_<alias>
         OR ti.task_id LIKE '%compaction'                -- hourly·daily Compaction (mapped)
         OR ti.task_id LIKE '%del_expired_snapshots'     -- iceberg_delete_expired_data 의 snapshot 삭제
         OR ti.task_id ~ '^[0-9]+_[^.]*$')              -- rewrite_manifest·delete_orphan_files: "번호_테이블명"
),
labeled AS (
  SELECT r.*,
         CASE WHEN task_id LIKE '%append_data' THEN 'append'
              WHEN task_id ~ '^summary_' THEN 'summary'
              WHEN task_id LIKE '%compaction'
                   THEN CASE WHEN cron = '@hourly' OR split_part(cron, ' ', 2) IN ('*', '*/1')
                             THEN 'hourly compaction' ELSE 'daily compaction' END
              WHEN task_id LIKE '%del_expired_snapshots' THEN 'expired snapshot'
              WHEN dag_id = 'iceberg_delete_orphan_files' THEN 'delete orphan'
              WHEN dag_id = 'iceberg_rewrite_manifest' THEN 'rewrite manifest'
              ELSE 'other' END                                            AS job_type,
         CASE WHEN map_key IS NOT NULL THEN map_key                       -- Compaction: 테이블명
              WHEN task_id ~ '^summary_' THEN substr(task_id, 9)          -- summary_<alias> → alias
              WHEN task_id LIKE 'convert_files.%' THEN dag_id             -- 테이블별 append DAG
              WHEN task_id LIKE '%.%'                                     -- TaskGroup: 번호 떼고 테이블명
                   THEN regexp_replace(split_part(task_id, '.', 1), '^[0-9]+_', '')
              WHEN task_id ~ '^[0-9]+_' THEN regexp_replace(task_id, '^[0-9]+_', '')
              ELSE dag_id END                                             AS target
  FROM recent r CROSS JOIN params p
  WHERE r.rn <= 100 OR p.period_from IS NOT NULL   -- 기간을 주면 그 기간 전부
)
SELECT job_type,
       dag_id,
       task_id,
       target,
       cron,
       COUNT(*)                                                          AS runs,
       ROUND(AVG(duration)::numeric / 60, 1)                             AS "Duration (min)",
       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration))::numeric / 60, 1)
                                                                         AS "Duration median (min)",
       ROUND(MAX(duration)::numeric / 60, 1)                             AS "Duration max (min)",
       ROUND(AVG(offset_sec)::numeric / 60, 1)                           AS "Start Offset (min)",
       MIN(run_after)                                                    AS oldest_run,
       MAX(run_after)                                                    AS latest_run
FROM labeled
GROUP BY job_type, dag_id, task_id, target, cron
ORDER BY array_position(ARRAY['append', 'summary', 'hourly compaction', 'daily compaction',
                              'expired snapshot', 'delete orphan', 'rewrite manifest', 'other'],
                        job_type),
         target, dag_id, task_id;
```

MySQL 8이면 `PERCENTILE_CONT` 줄(중앙값)과 `::numeric`·`::text`를 빼고, `split_part`를 `SUBSTRING_INDEX(task_id, '.', 1)`로, `~ '...'`를 `REGEXP '...'`로 바꾼다(`regexp_replace`는 MySQL 8에도 있다).

---

## 6. 엑셀 반영과 시각화

CSV의 `dag_id`·`target`으로 기존 엑셀 행(cron·CPU·memory)과 맞추고, 두 열을 붙인다.

| dag_id | target | cron | CPU | Memory | **Duration (min)** | **Start Offset (min)** |
|---|---|---|---|---|---|---|

시각화 계산은 1장의 두 줄이 전부다. 하루 1,440분을 1분 칸으로 나누고, 각 job의 실행마다 `[cron 시각 + Start Offset, + Duration)` 구간의 칸에 그 job의 CPU·memory를 더하면 시각별 실제 동시 사용량이 된다.

> **Duration이 cron 간격보다 길면 앞 실행과 겹친다.** 예를 들어 `*/5` job이 평균 6분 걸리면 항상 두 실행이 동시에 떠 있는 구간이 생긴다. 칸에 더하는 방식이면 이 겹침도 자동으로 반영된다. 단 DAG에 `max_active_runs=1`이 걸려 있으면 겹치지 않고 다음 실행이 밀리며, 그 밀림은 Start Offset이 커지는 것으로 나타난다.

---

## 7. 알아둘 점

| 항목 | 내용 |
|------|------|
| 믿어도 되는 숫자인가 | 추정이 아니라 Airflow가 task마다 기록한 **실제 시작·종료 시각**을 평균 낸 것이다. 걸러낸 것은 수동·재처리 실행과 실패 실행뿐이다. 한 번 검증하려면 Airflow UI에서 job 하나를 골라 최근 실행 몇 개의 시간을 눈으로 보고 CSV 값과 비교하면 된다 |
| Airflow Duration ≠ DataFlint duration | Airflow task 시간에는 driver pod가 뜨고 spark-submit하고 끝날 때까지가 전부 들어간다. DataFlint는 Spark 앱 시간만 본다. **리소스 시각화에는 Airflow 값이 맞다** — pod가 떠 있는 동안 자원을 잡고 있기 때문이다 |
| executor 수 | Compaction은 Dynamic Allocation이라 평소 1시간치는 시작 대수(1번 12, 2번 8, 3·4번 12)로 그린다. 재처리처럼 여러 시간치를 돌 때만 최대 36대까지 늘어나므로 별도 시나리오로 그린다 (`compaction-executor-sizing-design.md` §5.5) |
| memory | executor pod 한 대 = executor memory + `memoryOverhead`. 예: 3번 테이블 18g + 3g = 21g |
| 재시도된 실행 | Airflow는 마지막 시도만 남긴다. 재시도 전 시간은 Duration·Offset에 안 들어간다 |
| 시간대 | API·DB의 시각은 UTC다. Offset은 두 시각의 차이라 시간대와 무관하다 |
| 인증 | `POST /auth/token`에 계정·비밀번호를 보내 받은 토큰(JWT)을 쓴다. Airflow 3의 기본 로그인(Simple·FAB auth manager) 기준이며, SSO 등 다른 방식이면 토큰 발급 방법이 다르다. 401이 나면 이 부분을 먼저 확인 |
