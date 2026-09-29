# Airflow job 실행 시간 집계 (리소스 시각화용)

| 항목 | 내용 |
|------|------|
| 목적 | 하루 동안 시각별로 실제 동시에 떠 있는 core·memory를 그리기 위해, job별 **Duration**과 **Start Offset**을 최근 100회 실측으로 구한다 |
| 대상 | append·Compaction·maintenance 등 Spark pod를 띄우는 task 전부 |
| 방법 | Airflow 3.x REST API v2 (Python 스크립트). 메타 DB 직접 조회(SQL)는 쓰지 않는다 (사용자 결정 2026-09-28) |
| 검증 | Airflow 3.2.2 OpenAPI 명세로 endpoint·파라미터·응답 필드 확인. 운영과 같은 구조의 DAG(append 2종, summary, hourly·daily Compaction mapped task, maintenance 3종)를 가짜 Airflow API 서버에 넣고 결과를 손으로 센 값과 대조. 기간 지정(일·시·분 단위)도 손으로 센 실행 횟수와 일치 (2026-09-28) |

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

## 2. DAG 구조별 집계 대상

Spark pod를 띄우는 task만 센다. 아래 task는 Spark job이 아니므로 뺀다.

- append의 `get_jobs`·`update_success`·`update_failure`: Airflow worker 안에서 도는 Python task라 클러스터 core·memory를 쓰지 않는다
- `iceberg_delete_expired_data`의 `validate_target_dt`·`del_expired_data`: Spark job은 `del_expired_snapshots`뿐이다. `del_expired_data`도 Spark job이면 `TASK_NAMES`에 넣으면 된다(operator 이름에 `Spark`가 있으면 자동으로 잡힌다)

| DAG 종류 | 집계하는 task_id | 엑셀 대상(target) 표시 |
|---|---|---|
| 수직분할 4개 테이블 append (DAG 1개, 테이블별 병렬) | `<테이블명>.append_data` | TaskGroup 이름 = **테이블명** |
| 그 외 테이블 append (테이블별 DAG) | `convert_files.append_data` | **dag_id** (DAG 하나가 테이블 하나) |
| summary (`iceberg_summary_<alias>`, 테이블별 DAG) | `summary_<alias>` (DAG 안 task 3개 중 Spark job 하나) | 접두어를 뗀 **alias** |
| hourly·daily Compaction | `compaction` (mapped task) | map index 이름 = **테이블명**. hourly·daily는 DAG의 cron으로 구분(시 자리가 `*`면 hourly) |
| rewrite manifests (`iceberg_rewrite_manifests`) | `<번호>_<테이블명>` (예: `1_table_a` ~ `25_...`) | 번호를 뗀 **테이블명** |
| orphan 파일 삭제 (`iceberg_delete_orphan_files`) | `<번호>_<테이블명>` | 번호를 뗀 **테이블명** |
| 만료 데이터·snapshot 삭제 (`iceberg_delete_expired_data`) | `<번호>_<테이블명>.del_expired_snapshots` | TaskGroup 이름에서 번호를 뗀 **테이블명** |
| 위 규칙에 안 걸리는 Spark task | operator 이름에 `Spark`가 든 task | dag_id |


> **번호가 붙은 task는 번호 순서대로 Start Offset이 쌓인다.** rewrite manifests·orphan 삭제·만료 데이터 삭제 DAG는 테이블 task가 `1_`, `2_` … `25_` 순서로 돈다. 순차 실행이면 Compaction처럼 뒤 번호일수록 Start Offset이 커지고, 병렬이면 전부 비슷하게 나온다. 어느 쪽이든 실측값 그대로 그래프에 들어간다.

> **mapped task의 테이블명.** Compaction은 테이블마다 같은 task를 복제해 돌리는 mapped task라 task_id가 `compaction` 하나다. 각 복제본은 `map_index` 번호(0, 1, 2, 3)로 구분되고, `map_index_template`을 설정했다면 그 번호 대신 테이블명(`rendered_map_index`)이 기록된다. 설정하지 않았다면 target 열에 0·1·2·3이 나오며, 순서는 `expand`에 넘긴 테이블 목록 순서다.

---

## 3. Python 스크립트 (REST API)

### 3.1 실행

```bash
pip install requests
export AIRFLOW_URL=http://<airflow-api-server>:8080   # UI 주소와 같다
export AIRFLOW_USER=<계정>
export AIRFLOW_PASSWORD=<비밀번호>
python job_durations.py                              # job별 최근 100회
python job_durations.py 20260921 20260922            # 기간 지정 (아래 표)
```

인자 없이 돌리면 job(테이블)별 **최근 100회**를 평균 내고 `job_durations.csv`에 저장한다. **시작·끝을 주면** 그 기간의 실행을 **전부**(100회 제한 없음) 평균 내고 `job_durations_<시작>_<끝>.csv`에 저장한다.

날짜·시각은 `YYYYMMDD`(일) / `YYYYMMDDHH`(시) / `YYYYMMDDHHMM`(분) 세 형식 중 하나로 적는다. **끝 값은 적은 단위까지 포함한다** — 날짜만 적으면 그날 23:59까지, 시까지 적으면 그 시의 59분까지다.

| 인자 | 해석되는 기간 (KST) |
|---|---|
| `20260921 20260922` | 9/21 00:00 ~ 9/22 23:59 |
| `2026092109 2026092118` | 9/21 09:00 ~ 18:59 |
| `202609210930 202609211830` | 9/21 09:30 ~ 18:30 |
| `20260921 2026092212` | 9/21 00:00 ~ 9/22 12:59 (형식을 섞어도 된다) |
| `20260921` (하나만) | 9/21 하루 |
| `2026092109` (하나만) | 9/21 09:00 ~ 09:59 |

- 실행하면 첫 줄에 `기간(KST): 2026-09-21 09:00 ~ 2026-09-21 18:59 까지 포함`처럼 **실제로 해석한 기간**을 찍는다. 의도와 다르면 여기서 바로 보인다
- 모든 날짜·시각은 **한국 시간(KST)** 기준이다. 시간대는 스크립트 상단 `TZ`로 바꾼다
- 형식이 틀리거나(`2026-09-21`), 없는 시각이거나(`2026092125`), 시작이 끝보다 늦으면 이유를 적고 멈춘다
- 기간은 **cron 예정 시각**(`run_after`)으로 자른다. 9/21 23:45에 시작 예정이던 hourly Compaction은 실제로 9/22 0시를 넘겨 끝나도 9/21에 들어간다
- 결과는 엑셀에서 바로 열린다(한글 깨짐 방지 인코딩). 정렬은 append → summary → hourly Compaction → daily Compaction → expired snapshot → delete orphan → rewrite manifest 순이다. 같은 종류 안에서는 테이블명 오름차순이고, task_id가 `1_테이블명`처럼 **번호로 시작하는 task(expired snapshot·delete orphan·rewrite manifest)는 번호 순**이다. 번호는 숫자로 비교하므로 1, 2, …, 9, 10 순서가 된다(글자 순이면 1, 10, 2가 된다)

#### 수동(trigger) append 테이블 값 넣기

스케줄이 None이고 수직분할 4개 append DAG가 끝나면 trigger되는 append 테이블이 있으면, 실행 전에 `job_durations.py` 위쪽 설정의 **두 줄**을 고친다. 값은 따옴표 안에 적는다.

```python
# 고치기 전
TRIGGER_TABLE = None
TRIGGER_PARENT_DAG = None

# 고친 후 (예: 테이블명이 table_t, 수직분할 append DAG가 append_vertical이면)
TRIGGER_TABLE = "table_t"
TRIGGER_PARENT_DAG = "append_vertical"
```

| 변수 | 넣을 값 | 어디서 보나 |
|---|---|---|
| `TRIGGER_TABLE` | 수동 테이블의 **테이블명**. 테이블명으로 안 잡히면 그 테이블 append DAG의 **dag_id** | Airflow UI DAG 목록 |
| `TRIGGER_PARENT_DAG` | 수직분할 4개 테이블을 append하는 **DAG의 dag_id** (수동 테이블을 trigger하는 쪽) | Airflow UI DAG 목록 |

- 이 두 값은 **append task(`append_data`)에만 적용**된다. 같은 테이블명을 가진 Compaction·expired snapshot·delete orphan·rewrite manifest task는 건드리지 않는다
- 제대로 잡히면 실행 화면의 그 테이블 append 줄 끝에 `(trigger 실행, Offset·cron = 부모 … 기준, 짝 100/100)`이 붙는다
- 이름이 틀려 append가 하나도 안 잡히면(또는 2개 이상 잡히면) `⚠ TRIGGER_TABLE '…'에 걸린 append가 0개다` 경고가 뜬다 → 테이블명 대신 그 append DAG의 dag_id를 넣어 본다
- 엑셀 작업 시트에서는 이 테이블 행의 B열(cron)에 **부모 DAG의 cron**(`*/5 * * * *`)을 적는다 (§3.4 아래 설명)

### 3.2 고칠 수 있는 설정 (스크립트 상단)

| 설정 | 기본값 | 언제 바꾸나 |
|------|--------|------------|
| `N_RUNS` | 100 | 기간을 안 줬을 때 평균 낼 최근 실행 횟수 |
| `TZ` | KST (UTC+9) | 기간 날짜·시각을 해석할 시간대 |
| `TASK_NAMES` | `append_data`, `compaction`, `del_expired_snapshots` | 집계할 task 이름(점 뒤 부분). 새 Spark task가 생기면 이름을 추가 |
| `TASK_PREFIXES` | `summary_` | 이 접두어로 시작하는 task를 집계. 대상 이름은 접두어를 뗀 alias |
| `NUMBERED_TASK` | `^\d+_` | `번호_테이블명` 형태 task를 잡는 규칙 (rewrite manifests·orphan 삭제). 대상 이름에서 번호를 뗀다 |
| `MATCH_SPARK_OPERATOR` | `True` | operator 이름에 `Spark`가 든 task를 자동 포함. 위 두 규칙에 안 걸리는 Spark task를 놓치지 않기 위한 안전장치 |
| `TRIGGER_TABLE`, `TRIGGER_PARENT_DAG` | `None` | **수동(trigger) append 테이블** 1개의 테이블명과, 그 테이블을 trigger하는 수직분할 4개 append DAG의 dag_id. 넣는 법은 §3.1 끝. **append task에만 적용**되고 다른 job에는 영향 없다 (§3.4) |
| `DAG_ID_PREFIX` | `None` (전체) | 특정 DAG만 볼 때 dag_id 접두어 |
| `GENERIC_GROUPS` | `convert_files` | 테이블명이 아닌 TaskGroup 이름. 이 그룹이면 target을 dag_id로 표시 |
| `VERIFY` | `True` | 사내 인증서를 쓰면 CA 파일 경로 |
| `MAX_PAGES` | 100 | task 하나당 최대 몇 페이지(100건씩)를 훑을지. 긴 기간을 줄 때 모자라면 경고가 뜬다 |
| `JOB_TYPE_ORDER` | append … rewrite manifest, other | 출력 정렬 순서. 순서를 바꾸려면 이 목록만 바꾼다. 같은 종류 안에서는 테이블명 순이고, `번호_테이블명` task는 번호 순이다 |

### 3.3 스크립트

```python
"""Airflow Spark job의 Duration·Start Offset 집계 (Airflow 3.x REST API v2).

실행:
    export AIRFLOW_URL=http://<airflow-api-server>:8080
    export AIRFLOW_USER=<계정>  AIRFLOW_PASSWORD=<비밀번호>
    python job_durations.py                              # job별 최근 100회
    python job_durations.py 20260921 20260922            # 9/21 00:00 ~ 9/22 23:59 (KST)
    python job_durations.py 2026092109 2026092118        # 9/21 09:00 ~ 18:59
    python job_durations.py 202609210930 202609211830    # 9/21 09:30 ~ 18:30
    python job_durations.py 20260921                     # 9/21 하루 (값 하나 = 그 단위 하나)
  날짜·시각은 YYYYMMDD / YYYYMMDDHH / YYYYMMDDHHMM. 끝 값은 준 단위까지 포함한다.
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
TZ = timezone(timedelta(hours=9))   # 기간 날짜·시각을 해석할 시간대 = KST
OUTPUT = "job_durations.csv"
VERIFY = True                   # 사내 CA면 인증서 경로 문자열로 (예: "/etc/ssl/certs/ca.pem")

# 집계 대상 task — task_id의 마지막 부분(점 뒤) 기준
#   append_data           : append (수직분할 4개 DAG·테이블별 DAG)
#   compaction            : hourly·daily Compaction (mapped task)
#   del_expired_snapshots : iceberg_delete_expired_data 의 snapshot 삭제 Spark job
TASK_NAMES = {"append_data", "compaction", "del_expired_snapshots"}
# 이 접두어로 시작하는 task (iceberg_summary_<alias> DAG 의 summary_<alias>). 대상 이름은 접두어를 뗀 alias
TASK_PREFIXES = ("summary_",)
# "번호_테이블명" 형태의 task (iceberg_rewrite_manifests·iceberg_delete_orphan_files, 예: 1_table_a)
NUMBERED_TASK = re.compile(r"^\d+_")
# operator 이름에 "Spark"가 들어간 task도 자동 포함 (위 규칙에 안 걸리는 Spark task를 놓치지 않기 위한 안전장치)
MATCH_SPARK_OPERATOR = True
# 수동(trigger) append 테이블 — 스케줄이 None이고, 수직분할 4개 append DAG가 끝나면 trigger되는 append 테이블 1개.
# 두 줄에 값을 넣는다 (따옴표 안에). append task(append_data)에만 적용되고 다른 job에는 영향 없다.
TRIGGER_TABLE = None        # 수동 테이블의 테이블명 (또는 그 append DAG의 dag_id)   예) TRIGGER_TABLE = "table_t"
TRIGGER_PARENT_DAG = None   # 그 테이블을 trigger하는 수직분할 4개 append DAG의 dag_id   예) TRIGGER_PARENT_DAG = "append_vertical"
# 특정 DAG만 볼 때 접두어 (None이면 전체 DAG)
DAG_ID_PREFIX = None
# 이 TaskGroup 이름은 테이블명이 아니다 → 대상(target)을 dag_id로 표시
GENERIC_GROUPS = {"convert_files"}
# 페이지 상한 (100개 × 100 = task 하나당 최근 1만 건까지만 훑는다)
MAX_PAGES = 100
# 출력 정렬 순서 (job_type 열). 같은 job_type 안에서는 테이블명 순, "번호_테이블명" task는 번호 순
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


# 입력 자릿수 → (형식, 그 값이 가리키는 단위의 길이)
PERIOD_FORMATS = {8: ("%Y%m%d", timedelta(days=1)),
                  10: ("%Y%m%d%H", timedelta(hours=1)),
                  12: ("%Y%m%d%H%M", timedelta(minutes=1))}


def parse_moment(text):
    """'20260921' / '2026092109' / '202609210930' → (그 단위의 시작 시각, 단위 길이)."""
    if not text.isdigit() or len(text) not in PERIOD_FORMATS:
        raise SystemExit(f"날짜·시각은 YYYYMMDD, YYYYMMDDHH, YYYYMMDDHHMM 중 하나여야 한다: {text}")
    fmt, unit = PERIOD_FORMATS[len(text)]
    try:
        return datetime.strptime(text, fmt).replace(tzinfo=TZ), unit
    except ValueError:
        raise SystemExit(f"없는 날짜·시각이다: {text}")


def parse_period(args):
    """인자 없음 → None (최근 N_RUNS회). 1~2개 → (시작, 끝) — 끝 값은 준 단위까지 포함한다.
    20260922 → 9/22 23:59까지, 2026092218 → 18:59까지, 202609221830 → 18:30까지."""
    if not args:
        return None
    if len(args) > 2:
        raise SystemExit("사용법: python job_durations.py [시작 [끝]]   예) 20260921 20260922, 2026092109 2026092118")
    start, _ = parse_moment(args[0])
    end_begin, end_unit = parse_moment(args[-1])
    end = end_begin + end_unit            # 끝 값이 가리키는 단위의 끝 (그 시각 '직전'까지 포함)
    if start >= end:
        raise SystemExit("시작이 끝보다 늦다")
    return start, end


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


def name_in(name, dag_id, task_id):
    """name이 dag_id·task_id 안에 단어로 들어 있나.
    앞뒤가 문자열 끝이나 . _ 이어야 한다 → table_1을 넣어도 table_10은 안 걸린다."""
    return re.search(rf"(^|[._]){re.escape(name)}($|[._])", f"{dag_id}.{task_id}") is not None


def is_trigger_append(dag_id, task_id):
    """수동 테이블의 append task인가. append_data task만 보고, 테이블명(또는 dag_id)이 dag_id·task_id 안에 단어로 있으면 해당."""
    return bool(TRIGGER_TABLE) and task_id.rsplit(".", 1)[-1] == "append_data" and name_in(TRIGGER_TABLE, dag_id, task_id)


def parent_runs(parent, tis):
    """부모 DAG의 cron 실행(scheduled) — (예정 시각, 종료 시각). 이 테이블 실행 기간 + 앞 1시간만 가져온다."""
    times = [parse_time(ti["run_after"]) for ti in tis]
    fmt = lambda t: t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    params = {"run_type": "scheduled", "order_by": "-run_after", "limit": 100,
              "run_after_gte": fmt(min(times) - timedelta(hours=1)), "run_after_lte": fmt(max(times))}
    runs = []
    for page_no in range(MAX_PAGES * 10):
        page = get(f"/dags/{parent}/dagRuns", offset=page_no * 100, **params)["dag_runs"]
        runs += [(parse_time(r["run_after"]), parse_time(r["end_date"])) for r in page if r.get("end_date")]
        if len(page) < 100:
            break
    return runs


def offset_from_parent(ti, runs):
    """이 실행을 trigger한 부모 실행 = 예정 시각이 trigger 시각 이전이고, 종료 시각이 trigger 시각에 가장 가까운 실행
    (부모의 마지막 task가 trigger하므로 부모가 끝나는 시각 ≈ trigger 시각).
    부모가 5분을 넘겨 다음 실행과 겹쳐도 '직전에 시작한 실행'이 아니라 '방금 끝난 실행'을 고른다.
    반환: 부모 cron 시각 → 실제 시작(초). 못 찾으면 None."""
    trig = parse_time(ti["run_after"])
    cands = [(abs((end - trig).total_seconds()), ra) for ra, end in runs
             if ra <= trig and trig - ra <= timedelta(hours=1)]
    if not cands:
        return None
    return (parse_time(ti["start_date"]) - min(cands)[1]).total_seconds()


def collect_runs(dag_id, task_id, is_mapped, period, all_runs=False):
    """성공 실행만, 최신순. cron으로 돈(scheduled) 실행만 세고, all_runs면 trigger 실행도 센다.
    기간이 없으면 최근 N_RUNS회, 있으면 그 기간 전부. mapped task는 테이블별로 센다."""
    params = {"task_id": task_id, "state": "success",
              "order_by": "-run_after", "limit": 100}
    if not all_runs:
        params["run_id_prefix_pattern"] = "scheduled"   # 재처리·수동 trigger(manual__) 제외
    if period:
        params["run_after_gte"], params["run_after_lt"] = (
            t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") for t in period)
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
    if dag_id == "iceberg_rewrite_manifests":
        return "rewrite manifest"
    return "other"


def sort_key(r):
    """job_type 순서 → 같은 종류 안에서는
    "번호_테이블명" task(1_table_a …)는 번호 순 (숫자로 비교: 1, 2, …, 10), 나머지는 테이블명 순."""
    order = JOB_TYPE_ORDER.index(r["job_type"])
    m = re.match(r"(\d+)_", r["task_id"])
    if m:
        return (order, 0, r["dag_id"], int(m.group(1)), r["task_id"])
    return (order, 1, r["target"], 0, r["dag_id"] + "." + r["task_id"])


def to_min(seconds):
    """초 → 분, 소수점 1자리 (사사오입. Python round()는 0.25 → 0.2처럼 짝수 쪽으로 가서 쓰지 않는다)."""
    return float(Decimal(str(seconds / 60)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))


def main():
    args = sys.argv[1:]
    period = parse_period(args)
    output = OUTPUT if not period else OUTPUT.replace(".csv", f"_{args[0]}_{args[-1]}.csv")
    if period:
        last = period[1] - timedelta(minutes=1)
        print(f"기간(KST): {period[0]:%Y-%m-%d %H:%M} ~ {last:%Y-%m-%d %H:%M} 까지 포함, 그 기간 실행 전부")
    else:
        print(f"기간 지정 없음: job별 최근 {N_RUNS}회")
    login()
    rows = []
    dags = list_dags()
    cron_of = dict(dags)
    for dag_id, cron in dags:
        for task_id, is_mapped in target_tasks(dag_id):
            all_runs = is_trigger_append(dag_id, task_id)
            parent = TRIGGER_PARENT_DAG if all_runs else None
            for key, tis in collect_runs(dag_id, task_id, is_mapped, period, all_runs).items():
                durations = [ti["duration"] for ti in tis]
                offsets = [(parse_time(ti["start_date"]) - parse_time(ti["run_after"])).total_seconds()
                           for ti in tis]
                row_cron, note = cron, "  (trigger 실행 포함)" if all_runs else ""
                if parent:
                    runs = parent_runs(parent, tis)
                    from_parent = [offset_from_parent(ti, runs) for ti in tis]
                    paired = [o for o in from_parent if o is not None]
                    if paired:                 # 부모 cron 시각 기준 Offset, cron도 부모 것
                        offsets, row_cron = paired, cron_of.get(parent, "")
                        note = f"  (trigger 실행, Offset·cron = 부모 {parent} 기준, 짝 {len(paired)}/{len(tis)})"
                    else:
                        note = f"  ⚠ 부모 {parent}의 실행을 못 찾아 Offset은 자기 trigger 시각 기준"
                rows.append({
                    "job_type": job_type_of(dag_id, task_id, cron),
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "target": target_of(dag_id, task_id, key),
                    "cron": row_cron,
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
                      f"dur={r['Duration (min)']:5.1f} offset={r['Start Offset (min)']:5.1f}"
                      + note)
    if not rows:
        raise SystemExit("집계 대상이 없다 — 기간·TASK_NAMES·DAG_ID_PREFIX를 확인")
    if TRIGGER_TABLE:
        hit = [r for r in rows if is_trigger_append(r["dag_id"], r["task_id"])]
        if len(hit) != 1:
            print(f"  ⚠ TRIGGER_TABLE '{TRIGGER_TABLE}'에 걸린 append가 {len(hit)}개다 (1개여야 한다) — "
                  f"테이블명 대신 그 append DAG의 dag_id를 넣어 본다")
    rows.sort(key=sort_key)
    with open(output, "w", newline="", encoding="utf-8-sig") as f:   # utf-8-sig: 엑셀 한글 깨짐 방지
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"\n{len(rows)}행 → {output}")


if __name__ == "__main__":
    main()
```

### 3.4 무엇을 거르는가

| 필터 | 이유 |
|------|------|
| `run_id_prefix_pattern=scheduled` | **cron으로 돈 실행만.** 재처리 DAG가 trigger한 Compaction(20시간치면 10분 이상)은 run_id가 `manual__`로 시작해 빠진다. 이게 섞이면 평균이 튄다 |
| 단, 수동 append 테이블(`TRIGGER_TABLE`)은 이 필터를 끈다 | 스케줄 없이(None) 수직분할 4개 append DAG의 trigger로만 도는 append 테이블은 실행이 전부 `manual__` 등이라 위 필터에 전부 걸려 **CSV에서 통째로 빠진다**. 그 테이블의 **append task(`append_data`)만** 실행 종류를 가리지 않는다. 같은 테이블의 Compaction·expired snapshot·delete orphan·rewrite manifest는 다른 job과 똑같이 cron 실행만 센다. 예전 코드는 테이블명을 모든 dag_id·task_id에서 찾아 delete orphan task의 수동 실행까지 섞였다(가짜 서버에서 Duration 2.0 → 16.0분으로 재현 후 수정) |
| `state=success` | 실패한 실행은 중간에 끊겨 duration이 짧게 잡힌다 |
| `order_by=-run_after` | 최신순. 앞에서부터 N회만 쓴다 |
| mapped task는 테이블별로 N회 | 한 DAG 실행에 테이블 4개가 있으므로 페이지를 넘기며 테이블마다 100회를 채운다 |
| `run_after_gte`·`run_after_lt` (기간을 줬을 때만) | cron 예정 시각이 그 기간 안인 실행만. 이때는 100회 제한 없이 전부 쓴다 |

#### trigger 테이블의 Start Offset — 부모 cron 시각 기준

수직분할 4개 append DAG(부모, `*/5`)가 정상으로 끝나면 trigger 테이블 DAG를 trigger한다. 그래서 trigger 테이블은 **5분마다 돌지만 정확히 :00·:05에 시작하지 않고, 부모가 끝나는 시각에 따라 매번 조금씩 다른 때 시작**한다.

자기 실행의 예정 시각(`run_after`)은 trigger된 순간이라 거기서 잰 Offset은 0.7분처럼 작게만 나오고, 그래프에서 "언제 시작하는지"를 알 수 없다. 그래서 부모를 지정하면 **부모의 cron 시각 → 이 테이블의 실제 시작**으로 잰다.

| 예 (가짜 서버 실측) | 값 |
|---|---|
| 부모 cron 시각 | 00:00:00 |
| 부모 끝남 = trigger | 00:03:55 (부모 task가 3.5분 걸림) |
| trigger 테이블 실제 시작 | 00:04:35 |
| **Start Offset** | **4.6분 (275초)** — 자기 trigger 시각 기준이면 0.7분 |

- **어느 부모 실행이 trigger했나**: 예정 시각이 trigger 시각보다 이전(1시간 안)이고, **종료 시각이 trigger 시각에 가장 가까운** 부모 실행이다(부모의 마지막 task가 trigger하므로 부모가 끝나는 때 ≈ trigger 시각). 부모가 한 번 7분 걸려 다음 실행(5분 뒤 시작)과 겹쳐도, "방금 시작한 다음 실행"이 아니라 "방금 끝난 그 실행"과 짝지어진다 — 가짜 서버에서 부모가 7분 걸린 11회가 8.1분(485초)으로, 나머지 89회가 4.6분으로 잡혀 평균 5.0분
- CSV의 cron 칸에는 **부모의 cron**이 들어간다. 작업 시트 B열에도 부모 cron(`*/5 * * * *`)을 적는다 → 시각화가 부모와 같은 5분 주기로, 부모 cron + Offset 자리에 그린다
- 부모가 실패하면 trigger가 없으므로 그 5분은 비지만, 평균 모양에는 거의 영향이 없다
- 실행 화면에 `(trigger 실행, Offset·cron = 부모 … 기준, 짝 100/100)`이 붙는다. 부모 실행을 하나도 못 찾으면 경고와 함께 자기 trigger 시각 기준으로 잰다

### 3.5 출력 형식

아래는 가짜 데이터로 돌린 **형식 예시**다. 숫자는 실측이 아니다.

```text
job_type,dag_id,task_id,target,cron,runs,Duration (min),Duration median (min),Duration max (min),Start Offset (min),oldest_run,latest_run
append,append_vertical,table_a.append_data,table_a,*/5 * * * *,100,3.0,3.0,3.0,0.3,2026-09-27T15:45:00Z,2026-09-28T00:00:00Z
summary,iceberg_summary_alpha,summary_alpha,alpha,10 * * * *,100,2.0,2.0,2.0,0.2,2026-09-23T21:10:00Z,2026-09-28T00:10:00Z
hourly compaction,iceberg_compaction_hourly,compaction,table_1,45 * * * *,100,1.5,1.5,1.5,0.3,2026-09-23T21:45:00Z,2026-09-28T00:45:00Z
daily compaction,iceberg_compaction_daily,compaction,table_1,0 1 * * *,60,30.0,30.0,30.0,0.2,2026-07-31T01:00:00Z,2026-09-28T01:00:00Z
expired snapshot,iceberg_delete_expired_data,1_table_a.del_expired_snapshots,table_a,0 3 * * *,60,1.5,1.5,1.5,1.3,2026-07-31T03:00:00Z,2026-09-28T03:00:00Z
delete orphan,iceberg_delete_orphan_files,1_table_a,table_a,0 5 * * *,60,2.0,2.0,2.0,0.1,2026-07-31T05:00:00Z,2026-09-28T05:00:00Z
rewrite manifest,iceberg_rewrite_manifests,1_zeta,zeta,0 6 */3 * *,60,0.8,0.8,0.8,0.1,2026-07-31T06:00:00Z,2026-09-28T06:00:00Z
rewrite manifest,iceberg_rewrite_manifests,2_alpha,alpha,0 6 */3 * *,60,0.8,0.8,0.8,1.0,2026-07-31T06:00:00Z,2026-09-28T06:00:00Z
rewrite manifest,iceberg_rewrite_manifests,10_echo,echo,0 6 */3 * *,60,0.8,0.8,0.8,8.3,2026-07-31T06:00:00Z,2026-09-28T06:00:00Z
```

| 열 | 뜻 |
|---|---|
| `job_type` | 정렬용 종류. append·summary·hourly compaction·daily compaction·expired snapshot·delete orphan·rewrite manifest, 어디에도 안 맞으면 other |
| `runs` | 실제로 평균에 쓴 횟수. 100보다 작으면 이력이 그만큼밖에 없다 (예: 하루 1회 job은 보존 기간만큼) |
| `Duration median (min)` | 중앙값. **평균과 크게 다르면 튀는 실행이 섞였다는 신호** (4.6) |
| `Duration max (min)` | 최댓값. peak를 보수적으로 그릴 때 평균 대신 쓴다 |
| `oldest_run`·`latest_run` | 평균에 쓴 실행 중 가장 오래된·최근 실행의 cron 시각 (UTC). `latest_run`이 오래됐으면 멈춘 DAG다 |

### 3.6 Duration 평균과 중앙값의 차이

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

## 4. 엑셀 반영과 시각화

CSV의 **F~L열**(runs ~ latest_run)을 기존 작업 시트(cron·CPU·memory가 있는 시트)의 M열부터 붙이고, 시각화 스크립트를 돌린다 → [하루 리소스 사용량 시각화](resource-timeline.md).

계산은 1장의 두 줄이 전부다. 각 job의 실행마다 `[cron 시각 + Start Offset, + Duration)` 구간 동안 그 job의 CPU·memory를 잡고 있다고 보고, 시각마다 떠 있는 job을 더하면 시각별 실제 동시 사용량이 된다. 순차 실행을 두 번 세지 않도록 6초 간격으로 재고, 그래프는 5분 칸의 최댓값으로 그린다 (시각화 문서 §3).

> **Duration이 cron 간격보다 길면 앞 실행과 겹친다.** 예를 들어 `*/5` job이 평균 6분 걸리면 항상 두 실행이 동시에 떠 있는 구간이 생긴다. 구간을 더하는 방식이면 이 겹침도 자동으로 반영된다. 단 DAG에 `max_active_runs=1`이 걸려 있으면 겹치지 않고 다음 실행이 밀리며, 그 밀림은 Start Offset이 커지는 것으로 나타난다.

---

## 5. 알아둘 점

| 항목 | 내용 |
|------|------|
| 믿어도 되는 숫자인가 | 추정이 아니라 Airflow가 task마다 기록한 **실제 시작·종료 시각**을 평균 낸 것이다. 걸러낸 것은 수동·재처리 실행과 실패 실행뿐이다. 한 번 검증하려면 Airflow UI에서 job 하나를 골라 최근 실행 몇 개의 시간을 눈으로 보고 CSV 값과 비교하면 된다 |
| Airflow Duration ≠ DataFlint duration | Airflow task 시간에는 driver pod가 뜨고 spark-submit하고 끝날 때까지가 전부 들어간다. DataFlint는 Spark 앱 시간만 본다. **리소스 시각화에는 Airflow 값이 맞다** — pod가 떠 있는 동안 자원을 잡고 있기 때문이다 |
| executor 수 | Compaction은 Dynamic Allocation이라 평소 1시간치는 시작 대수(1번 12, 2번 8, 3·4번 12)로 그린다. 재처리처럼 여러 시간치를 돌 때만 최대 36대까지 늘어나므로 별도 시나리오로 그린다 (`compaction-executor-sizing-design.md` §5.5) |
| memory | executor pod 한 대 = executor memory + `memoryOverhead`. 예: 3번 테이블 18g + 3g = 21g |
| 재시도된 실행 | Airflow는 마지막 시도만 남긴다. 재시도 전 시간은 Duration·Offset에 안 들어간다 |
| 시간대 | API의 시각은 UTC다. Offset은 두 시각의 차이라 시간대와 무관하다 |
| 인증 | `POST /auth/token`에 계정·비밀번호를 보내 받은 토큰(JWT)을 쓴다. Airflow 3의 기본 로그인(Simple·FAB auth manager) 기준이며, SSO 등 다른 방식이면 토큰 발급 방법이 다르다. 401이 나면 이 부분을 먼저 확인 |
