# 하루 리소스 사용량 시각화 (엑셀 그래프)

> **결론**: 작업 시트(cron·CPU·메모리)에 `job_durations.py` 결과(Duration·Start Offset)를 붙인 엑셀을 넣으면, **시각별 동시 CPU·메모리 그래프가 들어 있는 새 엑셀**이 나온다. 예시 데이터에서 설정값 단순 합산은 591코어인데 실제로 한 순간에 겹친 최대는 **132코어(22%)**였다 — 단순 합산은 "모든 job이 동시에 떠 있는 순간"이라는, 실제로는 없는 값이다.
>
> 원본 엑셀은 건드리지 않는다. 결과는 같은 폴더에 `<원본이름>_리소스시각화.xlsx`로 생긴다.

- 앞 단계: [Airflow job 실행 시간 집계](airflow-job-duration.md) — Duration·Start Offset을 뽑는 스크립트
- 필요: Python 3.9 이상, `pip install openpyxl`

---

## 1. 입력 — 작업 시트의 모양

작업 시트 한 행 = job 하나. 스크립트는 아래 열 위치를 기준으로 읽는다.

| 열 | 내용 | 스크립트가 쓰나 |
|---|---|---|
| **A** | **job_type** (`job_durations.csv`의 `job_type` 값: `append`, `summary`, `hourly compaction`, `daily compaction`, `expired snapshot`, `delete orphan`, `rewrite manifest`, `other`) | ✅ 그래프에서 쌓는 종류 |
| **B** | **cron** | ✅ trigger로만 도는 테이블은 **부모 DAG의 cron**을 적는다 (§3.4). 비어 있거나 cron 식이 아니면 그 행만 실행 간격을 추정 |
| **C** | **app name** | ✅ 결과 표의 이름 |
| D~J | 드라이버 cpu, 드라이버 메모리, 드라이버 메모리 오버헤드, 익스큐터 cpu, 익스큐터 메모리, 익스큐터 메모리 오버헤드, 익스큐터 개수 | ❌ (K·L을 만드는 재료) |
| **K** | **토탈 cpu** (코어) | ✅ |
| **L** | **토탈 메모리** (GB. `228g`처럼 단위가 붙어 있어도 읽는다) | ✅ |
| M~S | `job_durations.csv`의 **F~L열**을 그대로 붙인 것: runs, Duration (min), Duration median (min), Duration max (min), **Start Offset (min)**, oldest_run, latest_run | ✅ |
| **U** | **기능 요약** | ✅ 결과 표(`job별 실행`, 최대 순간 job 목록)에 같이 보여 준다 |

- K·L이 수식이어도 된다. 엑셀에 **저장된 계산값**을 읽는다 (엑셀에서 한 번 저장된 파일이면 값이 들어 있다)
- 제목 행·빈 행·메모 행은 알아서 건너뛴다. **N열(Duration)과 K열(토탈 cpu)이 둘 다 숫자인 행만** job으로 본다
- 열 위치가 다르면 스크립트 상단 설정(§4)의 열 문자만 바꾼다

---

## 2. 실행

```bash
python resource_timeline.py 작업엑셀.xlsx                  # 첫 번째 시트, 기준일 = 오늘(KST)
python resource_timeline.py 작업엑셀.xlsx 리소스            # 시트 이름 지정
python resource_timeline.py 작업엑셀.xlsx 리소스 20260928   # 기준일 지정
```

**기준일이 필요한 이유**: rewrite manifests처럼 3일마다 도는 job(`0 6 */3 * *`)은 날짜에 따라 그날 도는지가 다르다. 그래프에 넣고 싶은 날을 고른다.

실행 화면 예 (예시 데이터):

```text
시트 '리소스': job 32개, 제외 0개
cron 시간대: KST (latest_run 18건 중 18건이 KST 기준 cron과 일치) / 기준일 2026-09-28
최대 동시 CPU 132.0코어 / 메모리 568.0GB (단순 합산 591.0코어 / 2,006.8GB)
→ 작업엑셀_리소스시각화.xlsx
```

### 2.1 결과 엑셀의 시트 3개

| 시트 | 내용 |
|---|---|
| **요약** | 기준일·읽은 열·cron 시간대, 지표 표(단순 합산 / 실제 최대 / 최대 시각 / 하루 평균 / 최대 ÷ 단순 합산), **CPU 그래프·메모리 그래프**(job 종류별로 쌓은 면적 그래프, x축 00:00~24:00 KST), 최대 CPU 순간에 떠 있던 job 목록(기능 요약 포함), 계산에서 뺀 행 |
| **시각별 사용량** | 5분 칸 = 1행(288행). 칸마다 종류별 CPU·메모리와 합계(`SUM` 수식). 그래프의 원본 데이터 |
| **job별 실행** | job마다 종류·cron·하루 실행 횟수·Start Offset·Duration·총 CPU·총 메모리와 `CPU·분 / 일`(= 횟수 × Duration × CPU, 수식), CPU·분 비중, 기능 요약 |

`요약`의 지표와 `job별 실행`의 계산 열은 수식이라 엑셀이 열 때 계산한다.

---

## 3. 계산 방법 — 예시 숫자로

### 3.1 job 하나의 실행 구간

한 번 실행 = `[cron 시각 + Start Offset, + Duration)` 구간 동안 토탈 cpu·토탈 메모리를 잡고 있다고 본다.

예: hourly compaction 1번 테이블 — cron `45 * * * *`, Start Offset 0.5분, Duration 2.1분, 50코어
→ 매시 45:30 ~ 47:36에 50코어

### 3.2 왜 6초 간격으로 재나 — 순차 실행을 두 번 세지 않으려고

hourly compaction은 테이블을 순서대로 돈다. 1번이 47:36에 끝나고 2번이 47:48에 시작한다(Start Offset 2.8분). **1분 칸에 "그 분에 떠 있던 job을 전부 더하기"로 계산하면 47분 칸에 1번과 2번이 같이 들어가** 50 + 34 = 84코어가 된다. 실제로는 한 순간도 둘이 같이 떠 있지 않았다.

그래서 하루를 6초 간격(14,400개 시점)으로 쪼개 **각 시점에 실제로 떠 있는 job만** 더한다.

### 3.3 왜 그래프 한 칸이 5분이고, 칸의 값은 최댓값인가

예시의 cron으로 도는 append 7개(합계 29코어)는 5분마다 시작해 2.4분 돌고 꺼진다. 1분 칸 그래프로 그리면 29 → 0 → 29 → 0 톱니가 하루 288번 반복되어 다른 job이 안 보인다.

5분 칸으로 묶고 **칸 안에서 합계가 가장 큰 순간의 값**을 쓰면 append는 29코어로 평평해진다. 뜻은 "이 5분 안에 한 번은 29코어가 필요하다" — 자원을 확보하는 입장에서 필요한 값이다. 칸 폭은 설정 `BUCKET_MIN`으로 바꾼다(1로 두면 1분 칸).

- 칸마다 최댓값이므로 `하루 평균 사용량`은 실제 평균보다 조금 높게 나온다
- 최대 동시 사용량은 칸 폭과 무관하게 같다(최댓값의 최댓값)

### 3.4 cron이 없을 때 — 실행 간격 추정

B열(cron)이 비어 있거나 cron 식이 아니면 그 행만 `(latest_run − oldest_run) ÷ (runs − 1)`로 간격을 구한다. 예: 최근 100회가 99시간에 걸쳐 있으면 99시간 ÷ 99 = 60분 → 매시 실행, 시작 분은 latest_run의 분. 실패한 실행이 빠져 조금 길게 나오므로 가까운 정규 간격(5·10·15·30·60분, 1·2·3일 등)으로 맞춘다. `job별 실행` 시트의 cron 칸에 `60분마다 (추정)`으로 표시된다.

- **trigger로만 도는 테이블은 추정 대신 B열에 부모 cron을 적는 것이 정확하다.** 집계 스크립트에 부모를 지정하면(`TRIGGER_TABLES`) Start Offset이 "부모 cron 시각 → 이 테이블 실제 시작"으로 나오므로, B열 `*/5 * * * *` + 그 Offset이면 부모가 도는 5분마다, 부모가 끝나는 자리에 그려진다 ([집계 문서](airflow-job-duration.md) §4.4). 추정으로 그리면 간격은 5분으로 맞지만 시작 자리를 latest_run 한 번에 맞추므로 덜 정확하다
- 3일마다 도는 job은 cron(`*/3`, 매달 1일 기준)과 추정(최근 실행 + 3일 간격)이 월말에 어긋날 수 있다. cron이 있는 job은 B열에 적어 둔다

### 3.5 cron 시간대 판단

Airflow cron은 UTC로 적었을 수도, KST로 적었을 수도 있다. 스크립트는 latest_run(UTC 시각)이 cron과 **UTC로 맞는지, KST로 맞는지** 세어 많은 쪽을 쓴다. 매시·5분마다 도는 job은 양쪽 다 맞아서 판단에 안 쓰고, daily job들이 판단한다. 근거는 `요약` 시트에 찍힌다(예: "latest_run 18건 중 18건이 KST 기준 cron과 일치"). 그래프는 항상 KST로 그린다.

### 3.6 예시 결과 읽기

| 지표 | CPU | 뜻 |
|---|---|---|
| 설정값 단순 합산 | 591코어 | 32개 job의 토탈 cpu를 전부 더한 값 |
| 실제 최대 동시 사용량 | 132코어 | 01:50 — daily compaction 3번째 테이블(01:45:36~) + hourly compaction 3번(01:49:48~) + cron append 7개(01:50:12~) + trigger 테이블(01:45 부모가 trigger, 01:48:06~01:50:36) = 50 + 50 + 29 + 3 |
| 최대 ÷ 단순 합산 | 22% | 단순 합산의 5분의 1 정도만 실제로 동시에 필요 |

최대 순간은 **daily compaction(01:00~) 구간에 매시 45분 hourly compaction이 겹칠 때**다. 이런 겹침이 그래프에서 봉우리로 보인다.

---

## 4. 설정 (스크립트 상단)

| 설정 | 기본값 | 바꾸는 경우 |
|---|---|---|
| `COL_TOTAL_CPU`, `COL_TOTAL_MEM` | `K`, `L` | 토탈 열 위치가 다를 때 |
| `COL_RUNS` … `COL_LATEST` | `M` ~ `S` | `job_durations.csv` F~L열을 다른 곳에 붙였을 때 |
| `COL_DURATION` | `COL_DUR_AVG` (평균) | 중앙값(`COL_DUR_MEDIAN`)이나 최댓값(`COL_DUR_MAX`)으로 그리고 싶을 때. 최댓값 = 가장 오래 걸린 날 기준의 보수적 그림 |
| `COL_GROUP`, `COL_CRON`, `COL_NAME` | `A`, `B`, `C` | job_type·cron·app name 열 |
| `COL_DESC` | `U` | 기능 요약 열. 없으면 `None` |
| `CRON_TZ` | `"auto"` | 판단 근거가 없을 때(daily job이 없음) `"KST"`/`"UTC"` 지정 |
| `BUCKET_MIN` | `5` | 그래프 한 칸의 폭(분). 1440의 약수 |
| `GROUP_ORDER`, `GROUP_COLORS` | 종류 7개 + 기타 | 종류 순서 = 그래프에 아래부터 쌓이는 순서. 색은 색각 이상 검사를 통과한 팔레트라 순서째로 바꾸지 말 것 |

---

## 5. 알아둘 점

| 항목 | 내용 |
|---|---|
| Compaction executor 수 | Dynamic Allocation이라 평소 1시간치는 시작 대수(1번 12, 2번 8, 3·4번 12)로 K·L을 채운다. 재처리처럼 여러 시간치를 돌 때만 최대 36대까지 늘어나므로, 그 경우는 K·L을 36대 기준으로 바꾼 시트로 한 번 더 돌려 별도 그림으로 본다 (`compaction-executor-sizing-design.md` §5.5) |
| Duration = Airflow 기준 | pod 기동·spark-submit이 포함된 시간이라 자원 점유 시간에 맞다 ([집계 문서](airflow-job-duration.md) §7) |
| 수동 실행 | `job_durations.py`가 `scheduled` 실행만 집계하므로 재처리가 trigger한 Compaction은 그래프에 없다 |
| Duration이 cron 간격보다 길 때 | 앞 실행과 겹치는 구간이 자동으로 두 번 더해진다. DAG에 `max_active_runs=1`이 있으면 실제로는 겹치지 않고 밀리며, 그 밀림은 Start Offset에 이미 들어 있다 |
| 작업 시트를 고치면 | 그래프는 스크립트가 계산한 값이다. 작업 시트 값이 바뀌면 스크립트를 다시 돌린다 |

---

## 6. 스크립트

```python
"""하루 리소스 사용량 시각화 — 작업 시트(cron·CPU·메모리 + job_durations 결과) → 그래프 엑셀.

실행:
    python resource_timeline.py <작업엑셀.xlsx>                     # 첫 번째 시트, 기준일 = 오늘
    python resource_timeline.py <작업엑셀.xlsx> <시트이름>
    python resource_timeline.py <작업엑셀.xlsx> <시트이름> 20260928  # 기준일 지정 (3일마다 도는 job 등이 달라진다)
결과: 같은 폴더에 <작업엑셀>_리소스시각화.xlsx  (원본은 건드리지 않는다)
필요: pip install openpyxl
"""
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.chart import AreaChart, Reference
from openpyxl.chart.axis import ChartLines
from openpyxl.chart.shapes import GraphicalProperties
from openpyxl.drawing.line import LineProperties
from openpyxl.comments import Comment
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import column_index_from_string, get_column_letter

# ── 설정: 작업 시트의 열 위치 ─────────────────────────────────────────────
COL_GROUP = "A"              # job_type (append, summary, hourly compaction …)
COL_CRON = "B"               # cron. 비어 있거나 cron이 아니면(trigger로만 도는 job) 실행 간격을 추정한다
COL_NAME = "C"               # app name
COL_TOTAL_CPU = "K"          # 토탈 cpu (코어)
COL_TOTAL_MEM = "L"          # 토탈 메모리 (GB. "228g"처럼 단위가 붙어 있어도 읽는다)
# job_durations.csv의 F~L열(runs … latest_run)을 M열부터 붙인 위치
COL_RUNS, COL_DUR_AVG, COL_DUR_MEDIAN, COL_DUR_MAX, COL_OFFSET, COL_OLDEST, COL_LATEST = \
    "M", "N", "O", "P", "Q", "R", "S"
# 그래프에 쓸 Duration: 평균(COL_DUR_AVG) / 중앙값(COL_DUR_MEDIAN) / 최댓값(COL_DUR_MAX)
COL_DURATION = COL_DUR_AVG
COL_DESC = "U"               # 기능 요약 (결과 표에 같이 보여 준다. 없으면 None)
# cron을 어느 시간대로 적었나: "auto"(latest_run으로 판단) / "KST" / "UTC". 그래프는 항상 KST로 그린다
CRON_TZ = "auto"
KST = timezone(timedelta(hours=9))
# job 종류 순서와 색 (앞에서부터 고정 순서로 배정, 검증된 범주형 팔레트)
GROUP_ORDER = ["append", "summary", "hourly compaction", "daily compaction",
               "expired snapshot", "delete orphan", "rewrite manifest", "기타"]
GROUP_COLORS = ["2A78D6", "EB6834", "1BAF7A", "EDA100", "E87BA4", "008300", "4A3AA7", "E34948"]
# 그래프 한 칸의 폭(분). 칸 안에서 합계가 가장 큰 순간의 값을 그 칸의 값으로 쓴다.
# 1로 두면 5분 주기 append가 켜졌다 꺼지는 톱니가 그대로 보여 읽기 어렵다. 1440의 약수로 (1, 5, 10, 15 …)
BUCKET_MIN = 5
# ─────────────────────────────────────────────────────────────────────

FONT = "Arial"
SAMPLES_PER_MIN = 10          # 6초 간격으로 동시 사용량을 잰다

NB = 1440 // BUCKET_MIN        # 하루 칸 수

# ── cron 해석 ─────────────────────────────────────────────────────────
PRESETS = {"@hourly": "0 * * * *", "@daily": "0 0 * * *", "@midnight": "0 0 * * *",
           "@weekly": "0 0 * * 0", "@monthly": "0 0 1 * *", "@yearly": "0 0 1 1 *",
           "@annually": "0 0 1 1 *"}
MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}
DAYS = {d: i for i, d in enumerate(["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"])}
CRON_RE = re.compile(r"^(@\w+|(\S+\s+){4}\S+)$")


def _field(expr, lo, hi, names=None):
    def num(tok):
        tok = tok.upper()
        return names[tok] if names and tok in names else int(tok)
    values = set()
    for part in expr.split(","):
        step = 1
        if "/" in part:
            part, s = part.split("/")
            step = int(s)
        if part in ("*", "?"):
            a, b = lo, hi
        elif "-" in part:
            a, b = (num(x) for x in part.split("-"))
        else:
            a = num(part)
            b = hi if step > 1 else a
        values.update(range(a, b + 1, step))
    return values, expr in ("*", "?")


class Cron:
    def __init__(self, text):
        text = PRESETS.get(text.strip().lower(), text.strip())
        m, h, dom, mon, dow = text.split()
        self.minutes, _ = _field(m, 0, 59)
        self.hours, _ = _field(h, 0, 23)
        self.doms, self.dom_any = _field(dom, 1, 31)
        self.months, _ = _field(mon, 1, 12, MONTHS)
        dows, self.dow_any = _field(dow, 0, 7, DAYS)
        self.dows = {d % 7 for d in dows}

    def day_matches(self, d):
        if d.month not in self.months:
            return False
        dom_ok = d.day in self.doms
        dow_ok = (d.weekday() + 1) % 7 in self.dows      # cron: 일요일 = 0
        if not self.dom_any and not self.dow_any:        # 둘 다 지정되면 둘 중 하나만 맞아도 실행 (Vixie cron)
            return dom_ok or dow_ok
        return dom_ok and dow_ok

    def matches(self, dt):
        return dt.minute in self.minutes and dt.hour in self.hours and self.day_matches(dt.date())

    def fires_on(self, d, tz):
        if not self.day_matches(d):
            return []
        return [datetime(d.year, d.month, d.day, h, m, tzinfo=tz)
                for h in sorted(self.hours) for m in sorted(self.minutes)]


def is_cron(v):
    return isinstance(v, str) and bool(CRON_RE.match(v.strip())) and _try_cron(v)


def _try_cron(v):
    try:
        Cron(v)
        return True
    except Exception:
        return False


# ── 값 읽기 ───────────────────────────────────────────────────────────
def to_number(v):
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        m = re.match(r"^\s*([\d.]+)\s*([a-zA-Z]*)\s*$", v)
        if m:
            n, unit = float(m.group(1)), m.group(2).lower()
            if unit in ("", "g", "gb", "gi", "gib"):
                return n
            if unit in ("m", "mb", "mi", "mib"):
                return n / 1024
            if unit in ("t", "tb", "ti", "tib"):
                return n * 1024
    return None


def to_utc(v):
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, str) and v.strip():
        try:
            dt = datetime.fromisoformat(v.strip().replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def classify(text, cron):
    """구분 열이 없을 때 이름으로 job 종류를 추정한다 (job_durations.py의 job_type과 같은 규칙)."""
    t = text.lower()
    if "append" in t or "convert_file" in t:
        return "append"
    if "summary" in t:
        return "summary"
    if "compaction" in t:
        f = cron.split()
        hourly = cron.strip() == "@hourly" or (len(f) == 5 and f[1] in ("*", "*/1")) or "hourly" in t
        return "hourly compaction" if hourly else "daily compaction"
    if "expire" in t:
        return "expired snapshot"
    if "orphan" in t:
        return "delete orphan"
    if "manifest" in t:
        return "rewrite manifest"
    return "기타"


def normalize_group(v):
    s = str(v).strip().lower()
    if s == "other":                             # job_durations.py의 job_type "other"
        return "기타"
    for g in GROUP_ORDER:
        if s == g:
            return g
    return str(v).strip()


# ── 입력 읽기 ─────────────────────────────────────────────────────────
def read_jobs(ws):
    col = lambda letter: column_index_from_string(letter) - 1
    rows = [(r[0].row, [c.value for c in r]) for r in ws.iter_rows()]
    data = [(rn, vals) for rn, vals in rows
            if len(vals) > col(COL_DURATION) and to_number(vals[col(COL_DURATION)]) is not None
            and to_number(vals[col(COL_TOTAL_CPU)]) is not None]
    if not data:
        raise SystemExit(f"{COL_DURATION}열(Duration)과 {COL_TOTAL_CPU}열(토탈 cpu)에 숫자가 있는 행이 없다 — 열 설정을 확인")

    jobs, skipped = [], []
    for rn, vals in data:
        get = lambda letter: vals[col(letter)] if letter and len(vals) > col(letter) else None
        cron = get(COL_CRON)
        name = str(get(COL_NAME) or "").strip() or f"{rn}행"
        job = {
            "row": rn, "name": name, "desc": str(get(COL_DESC) or "").strip(),
            "cron_text": "", "cron": None, "every": None,
            "cpu": to_number(get(COL_TOTAL_CPU)), "mem": to_number(get(COL_TOTAL_MEM)) or 0.0,
            "duration": to_number(get(COL_DURATION)), "offset": to_number(get(COL_OFFSET)) or 0.0,
            "latest": to_utc(get(COL_LATEST)),
        }
        if is_cron(cron):
            job["cron_text"], job["cron"] = cron.strip(), Cron(cron)
        else:
            # cron이 없으면(trigger로만 도는 job) 실행 간격을 runs·oldest_run·latest_run으로 추정한다
            every = infer_every(to_number(get(COL_RUNS)), to_utc(get(COL_OLDEST)), job["latest"])
            if not every:
                skipped.append((rn, name, "cron이 없고 runs·oldest_run·latest_run으로 간격도 못 구함"))
                continue
            job["every"] = every
            job["cron_text"] = f"{every}분마다 (추정)"
        job["group"] = normalize_group(get(COL_GROUP)) if get(COL_GROUP) else classify(
            name, job["cron_text"] if job["cron"] else ("0 * * * *" if job["every"] <= 60 else "0 0 * * *"))
        jobs.append(job)
    return jobs, skipped


NICE_INTERVALS = [1, 2, 3, 5, 10, 15, 20, 30, 60, 120, 180, 240, 360, 720, 1440, 2880, 4320, 10080]


def infer_every(runs, oldest, latest):
    """(latest − oldest) ÷ (runs − 1) = 실행 간격(분). 실패 실행이 빠져 조금 길게 나오므로 가까운 정규 간격으로 맞춘다."""
    if not runs or runs < 2 or not oldest or not latest or latest <= oldest:
        return None
    raw = (latest - oldest).total_seconds() / 60 / (runs - 1)
    return min(NICE_INTERVALS, key=lambda x: abs(math.log(raw / x)))


def detect_cron_tz(jobs):
    """latest_run(UTC)이 cron과 UTC 기준으로 맞는지, KST 기준으로 맞는지 세어 본다."""
    if CRON_TZ in ("KST", "UTC"):
        return CRON_TZ, "설정값"
    votes = Counter()
    for j in jobs:
        t = j["latest"]
        if not t or not j["cron"]:
            continue
        utc_ok = j["cron"].matches(t.astimezone(timezone.utc))
        kst_ok = j["cron"].matches(t.astimezone(KST))
        if utc_ok != kst_ok:                      # 매시 도는 job은 둘 다 맞아서 판단에 못 쓴다
            votes["UTC" if utc_ok else "KST"] += 1
    if not votes:
        return "KST", "판단 근거 없음 → KST로 가정"
    tz, n = votes.most_common(1)[0]
    return tz, f"latest_run {sum(votes.values())}건 중 {n}건이 {tz} 기준 cron과 일치"


# ── 하루치 계산 ───────────────────────────────────────────────────────
def fire_times(j, day, cron_tz):
    """기준일 0시(KST) 기준 분 단위 시작 시각 목록. 전날 시작해 오늘로 넘어오는 실행도 잡으려고 앞뒤 이틀을 본다."""
    day_start = datetime(day.year, day.month, day.day, tzinfo=KST)
    out = []
    if j["cron"]:
        tz = KST if cron_tz == "KST" else timezone.utc
        for dd in range(-2, 2):
            d = (day_start.astimezone(tz) + timedelta(days=dd)).date()
            out += [(f - day_start).total_seconds() / 60 for f in j["cron"].fires_on(d, tz)]
    else:
        anchor = (j["latest"] - day_start).total_seconds() / 60
        k0 = math.floor((-2880 - anchor) / j["every"])
        k1 = math.ceil((1440 - anchor) / j["every"])
        out = [anchor + k * j["every"] for k in range(k0, k1 + 1)]
    return [(t + j["offset"], t + j["offset"] + j["duration"]) for t in out
            if -2880 <= t < 1440]


def simulate(jobs, day, cron_tz):
    n = 1440 * SAMPLES_PER_MIN
    step = 1 / SAMPLES_PER_MIN
    groups = [g for g in GROUP_ORDER if any(j["group"] == g for j in jobs)] + \
             sorted({j["group"] for j in jobs} - set(GROUP_ORDER))
    diff = {k: {g: [0.0] * (n + 1) for g in groups} for k in ("cpu", "mem")}
    runs = defaultdict(list)                     # job 번호 → 오늘 시작한 실행 [(시작분, 끝분)]
    spans = defaultdict(list)                    # job 번호 → 오늘에 걸친 실행 전부
    for idx, j in enumerate(jobs):
        for start, end in fire_times(j, day, cron_tz):
            if end <= 0 or start >= 1440 or j["duration"] <= 0:
                continue
            spans[idx].append((start, end))
            if 0 <= start < 1440:
                runs[idx].append((start, end))
            si = max(0, math.ceil((start - step / 2) / step))
            ei = min(n, math.ceil((end - step / 2) / step))
            if si < ei:
                for k, v in (("cpu", j["cpu"]), ("mem", j["mem"])):
                    diff[k][j["group"]][si] += v
                    diff[k][j["group"]][ei] -= v
    inst = {}
    for k in ("cpu", "mem"):
        inst[k] = {}
        for g in groups:
            acc, out = 0.0, []
            for x in diff[k][g][:n]:
                acc += x
                out.append(round(acc, 6))
            inst[k][g] = out
    per_minute = {}
    w = BUCKET_MIN * SAMPLES_PER_MIN
    for k in ("cpu", "mem"):
        rows = []
        for m in range(1440 // BUCKET_MIN):
            ks = range(m * w, (m + 1) * w)
            best = max(ks, key=lambda s: sum(inst[k][g][s] for g in groups))
            rows.append({g: inst[k][g][best] for g in groups} | {"_sample": best})
        per_minute[k] = rows
    return groups, per_minute, runs, spans


def active_jobs(jobs, runs_all, t_min):
    out = []
    for idx, j in enumerate(jobs):
        for s, e in runs_all.get(idx, []):
            if s <= t_min < e:
                out.append(j)
                break
    return out


# ── 엑셀 쓰기 ─────────────────────────────────────────────────────────
HEAD_FILL = PatternFill("solid", fgColor="E9EEF5")
THIN = Side(style="thin", color="C9CED6")
BORDER = Border(bottom=THIN)


def style_header(ws, row, ncol):
    for c in range(1, ncol + 1):
        cell = ws.cell(row=row, column=c)
        cell.font = Font(name=FONT, bold=True, size=10)
        cell.fill = HEAD_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = BORDER


def set_font(ws):
    for row in ws.iter_rows():
        for c in row:
            if c.value is not None and not c.font.bold:
                c.font = Font(name=FONT, size=10, color=c.font.color)


def area_chart(title, ws, groups, first_col, label_col, y_title):
    ch = AreaChart()
    ch.grouping = "stacked"
    ch.title = title
    ch.y_axis.title = y_title
    ch.x_axis.title = "시각 (KST)"
    ch.height, ch.width = 9, 26
    ch.add_data(Reference(ws, min_col=first_col, max_col=first_col + len(groups) - 1, min_row=1, max_row=NB + 1),
                titles_from_data=True)
    ch.set_categories(Reference(ws, min_col=label_col, min_row=2, max_row=NB + 1))
    for i, s in enumerate(ch.series):
        color = GROUP_COLORS[GROUP_ORDER.index(groups[i])] if groups[i] in GROUP_ORDER else "8C8C8C"
        s.graphicalProperties.solidFill = color
        s.graphicalProperties.line.noFill = True
    ch.x_axis.tickLblSkip = 60 // BUCKET_MIN            # 정시마다 눈금
    ch.x_axis.tickMarkSkip = 60 // BUCKET_MIN
    ch.x_axis.delete = False
    ch.y_axis.delete = False
    ch.y_axis.majorGridlines = ChartLines(spPr=GraphicalProperties(ln=LineProperties(solidFill="E3E6EA")))
    ch.legend.position = "b"
    return ch


def write_workbook(path, jobs, skipped, groups, per_minute, runs, spans, day, cron_tz, tz_reason, src):
    wb = Workbook()
    summary = wb.active
    summary.title = "요약"
    tl = wb.create_sheet("시각별 사용량")
    jb = wb.create_sheet("job별 실행")

    # ── 시각별 사용량: 1분 = 1행. 종류별 값 + 합계(수식)
    g = len(groups)
    cpu_c, mem_c = 2, 2 + g + 1                  # CPU 종류별 시작 열, 메모리 종류별 시작 열
    cpu_tot, mem_tot = 2 + g, 2 + 2 * g + 1
    label_col = mem_tot + 1                      # 그래프 x축용: 정시에만 글자가 있는 열
    headers = ["시각 (KST)"] + [f"CPU · {x}" for x in groups] + ["CPU 합계 (코어)"] + \
              [f"메모리 · {x}" for x in groups] + ["메모리 합계 (GB)", "그래프 눈금"]
    tl.append(headers)
    for m in range(NB):
        r = m + 2
        t = m * BUCKET_MIN
        tl.cell(row=r, column=1, value=f"{t // 60:02d}:{t % 60:02d}")
        tl.cell(row=r, column=label_col, value=f"{t // 60:02d}:00" if t % 60 == 0 else "")
        for i, x in enumerate(groups):
            tl.cell(row=r, column=cpu_c + i, value=per_minute["cpu"][m][x])
            tl.cell(row=r, column=mem_c + i, value=per_minute["mem"][m][x])
        tl.cell(row=r, column=cpu_tot,
                value=f"=SUM({get_column_letter(cpu_c)}{r}:{get_column_letter(cpu_c + g - 1)}{r})")
        tl.cell(row=r, column=mem_tot,
                value=f"=SUM({get_column_letter(mem_c)}{r}:{get_column_letter(mem_c + g - 1)}{r})")
    style_header(tl, 1, len(headers))
    tl.freeze_panes = "B2"
    tl.column_dimensions["A"].width = 11
    for c in range(2, len(headers)):
        tl.column_dimensions[get_column_letter(c)].width = 14
        for r in range(2, NB + 2):
            tl.cell(row=r, column=c).number_format = "#,##0.0"
    tl["A1"].comment = Comment("스크립트가 cron·Start Offset·Duration으로 계산한 값이다. "
                               f"6초 간격으로 잰 동시 사용량 중 그 {BUCKET_MIN}분 안에서 합계가 가장 컸던 순간의 값. "
                               "작업 시트 값이 바뀌면 스크립트를 다시 돌린다.", "resource_timeline.py")
    set_font(tl)

    # ── job별 실행: 작업 시트 값 + 하루 합계(수식)
    jh = ["작업 시트 행", "종류", "이름", "cron", "하루 실행 횟수", "Start Offset (min)", "Duration (min)",
          "총 CPU (코어)", "총 메모리 (GB)", "CPU·분 / 일", "메모리 GB·분 / 일", "CPU·분 비중", "기능 요약"]
    jb.append(jh)
    last = len(jobs) + 1
    for i, j in enumerate(jobs):
        r = i + 2
        jb.append([j["row"], j["group"], j["name"], j["cron_text"], len(runs.get(i, [])),
                   j["offset"], j["duration"], j["cpu"], j["mem"],
                   f"=E{r}*G{r}*H{r}", f"=E{r}*G{r}*I{r}", f"=IF(SUM($J$2:$J${last})=0,0,J{r}/SUM($J$2:$J${last}))", j["desc"]])
        for c, fmt in ((6, "0.0"), (7, "0.0"), (8, "#,##0.0"), (9, "#,##0.0"), (10, "#,##0"), (11, "#,##0"), (12, "0.0%")):
            jb.cell(row=r, column=c).number_format = fmt
    style_header(jb, 1, len(jh))
    jb.freeze_panes = "D2"
    for c, w in zip("ABCDEFGHIJKLM", (9, 17, 34, 15, 10, 11, 11, 11, 12, 12, 14, 10, 40)):
        jb.column_dimensions[c].width = w
    set_font(jb)

    # ── 요약
    s = summary
    s.column_dimensions["A"].width = 30
    s.column_dimensions["B"].width = 18
    s.column_dimensions["C"].width = 16
    s.column_dimensions["D"].width = 50
    s.column_dimensions["E"].width = 40
    s["A1"] = "하루 리소스 사용량"
    s["A1"].font = Font(name=FONT, bold=True, size=14)
    ct, mt = get_column_letter(cpu_tot), get_column_letter(mem_tot)
    info = [
        ("기준일 (KST)", day.isoformat(), "3일마다 도는 job 등은 기준일에 따라 포함 여부가 달라진다"),
        ("원본", src, ""),
        ("cron 시간대", cron_tz, tz_reason),
        ("Duration 기준", {COL_DUR_AVG: "평균", COL_DUR_MEDIAN: "중앙값", COL_DUR_MAX: "최댓값"}.get(COL_DURATION, COL_DURATION),
         "스크립트 상단 COL_DURATION으로 바꾼다"),
        ("읽은 열", f"job_type {COL_GROUP} · cron {COL_CRON} · app name {COL_NAME} · 토탈 {COL_TOTAL_CPU}·{COL_TOTAL_MEM} "
                   f"· Duration {COL_DURATION} · Start Offset {COL_OFFSET}", "스크립트 상단 설정"),
    ]
    for i, (k, v, note) in enumerate(info, start=3):
        s.cell(row=i, column=1, value=k).font = Font(name=FONT, bold=True, size=10)
        s.cell(row=i, column=2, value=v)
        s.cell(row=i, column=4, value=note).font = Font(name=FONT, size=9, color="6B6B6B")
        s.cell(row=i, column=4).alignment = Alignment(indent=1)

    r0 = 10
    s.cell(row=r0, column=1, value="지표")
    s.cell(row=r0, column=2, value="CPU (코어)")
    s.cell(row=r0, column=3, value="메모리 (GB)")
    s.cell(row=r0, column=4, value="뜻")
    style_header(s, r0, 4)
    rng = lambda c: f"'시각별 사용량'!${c}$2:${c}${NB + 1}"
    metrics = [
        ("설정값 단순 합산", f"=SUM('job별 실행'!H2:H{last})", f"=SUM('job별 실행'!I2:I{last})",
         "모든 job이 동시에 떠 있다고 가정한 값 — 실제로는 일어나지 않는다"),
        ("실제 최대 동시 사용량", f"=MAX({rng(ct)})", f"=MAX({rng(mt)})", "하루 중 가장 많이 겹친 순간"),
        ("최대가 나온 시각", f"=INDEX({rng('A')},MATCH(B12,{rng(ct)},0))",
         f"=INDEX({rng('A')},MATCH(C12,{rng(mt)},0))", f"{BUCKET_MIN}분 칸의 시작 시각. 같은 값이 여러 번이면 가장 이른 시각"),
        ("하루 평균 사용량", f"=AVERAGE({rng(ct)})", f"=AVERAGE({rng(mt)})",
         f"{BUCKET_MIN}분 칸 값의 평균 — 칸마다 최댓값을 쓰므로 실제 평균보다 조금 높다"),
        ("최대 ÷ 단순 합산", "=IF(B11=0,0,B12/B11)", "=IF(C11=0,0,C12/C11)", "실제 최대가 단순 합산의 몇 %인가 — 낮을수록 단순 합산이 부풀어 있다"),
    ]
    for i, (k, fc, fm, note) in enumerate(metrics, start=r0 + 1):
        s.cell(row=i, column=1, value=k).font = Font(name=FONT, bold=True, size=10)
        s.cell(row=i, column=2, value=fc)
        s.cell(row=i, column=3, value=fm)
        s.cell(row=i, column=4, value=note).font = Font(name=FONT, size=9, color="6B6B6B")
        s.cell(row=i, column=4).alignment = Alignment(indent=1)
        for c in (2, 3):
            s.cell(row=i, column=c).number_format = "0.0%" if k.startswith("최대 ÷") else "#,##0.0"
            s.cell(row=i, column=c).alignment = Alignment(horizontal="right")

    s.add_chart(area_chart("시각별 동시 CPU (코어)", tl, groups, cpu_c, label_col, "코어"), "A18")
    s.add_chart(area_chart("시각별 동시 메모리 (GB)", tl, groups, mem_c, label_col, "GB"), "A37")

    # 최대 CPU 순간에 떠 있던 job
    peak_bucket = max(range(NB), key=lambda m: sum(per_minute["cpu"][m][x] for x in groups))
    t_peak = per_minute["cpu"][peak_bucket]["_sample"] / SAMPLES_PER_MIN + 0.05
    peak_hm = f"{int(t_peak) // 60:02d}:{int(t_peak) % 60:02d}"
    act = sorted(active_jobs(jobs, spans, t_peak), key=lambda j: -j["cpu"])
    r1 = 57
    s.cell(row=r1, column=1, value=f"최대 CPU 순간({peak_hm})에 떠 있던 job — {len(act)}개")
    s.cell(row=r1, column=1).font = Font(name=FONT, bold=True, size=11)
    s.cell(row=r1 + 1, column=1, value="이름")
    s.cell(row=r1 + 1, column=2, value="종류")
    s.cell(row=r1 + 1, column=3, value="총 CPU (코어)")
    s.cell(row=r1 + 1, column=4, value="총 메모리 (GB)")
    s.cell(row=r1 + 1, column=5, value="기능 요약")
    style_header(s, r1 + 1, 5)
    for i, j in enumerate(act, start=r1 + 2):
        s.cell(row=i, column=1, value=j["name"])
        s.cell(row=i, column=2, value=j["group"])
        s.cell(row=i, column=3, value=j["cpu"]).number_format = "#,##0.0"
        s.cell(row=i, column=4, value=j["mem"]).number_format = "#,##0.0"
        s.cell(row=i, column=5, value=j["desc"] or None)
    if skipped:
        r2 = r1 + 3 + len(act)
        s.cell(row=r2, column=1, value=f"계산에서 뺀 행 — {len(skipped)}개").font = Font(name=FONT, bold=True, size=11)
        for i, (rn, name, why) in enumerate(skipped, start=r2 + 1):
            s.cell(row=i, column=1, value=f"{rn}행 {name}")
            s.cell(row=i, column=2, value=why)
    set_font(s)
    s.page_setup.orientation = "landscape"
    s.page_setup.fitToWidth, s.page_setup.fitToHeight = 1, 0
    s.sheet_properties.pageSetUpPr.fitToPage = True
    wb.calculation.fullCalcOnLoad = True          # 합계·요약 수식을 엑셀이 열 때 계산한다
    wb.save(path)


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    src = Path(sys.argv[1])
    wb = load_workbook(src, data_only=True)       # 수식 셀은 계산된 값으로 읽는다
    ws = wb[sys.argv[2]] if len(sys.argv) > 2 else wb.worksheets[0]
    day = datetime.strptime(sys.argv[3], "%Y%m%d").date() if len(sys.argv) > 3 else datetime.now(KST).date()
    jobs, skipped = read_jobs(ws)
    cron_tz, reason = detect_cron_tz(jobs)
    guessed = sum(1 for j in jobs if j["every"])
    print(f"시트 '{ws.title}': job {len(jobs)}개, 제외 {len(skipped)}개"
          + (f" / cron 없어 간격 추정 {guessed}개" if guessed else ""))
    print(f"cron 시간대: {cron_tz} ({reason}) / 기준일 {day}")
    groups, per_minute, runs, spans = simulate(jobs, day, cron_tz)
    out = src.with_name(f"{src.stem}_리소스시각화.xlsx")
    write_workbook(out, jobs, skipped, groups, per_minute, runs, spans, day, cron_tz, reason, src.name)
    peak_cpu = max(sum(per_minute["cpu"][m][g] for g in groups) for m in range(NB))
    peak_mem = max(sum(per_minute["mem"][m][g] for g in groups) for m in range(NB))
    print(f"최대 동시 CPU {peak_cpu:,.1f}코어 / 메모리 {peak_mem:,.1f}GB "
          f"(단순 합산 {sum(j['cpu'] for j in jobs):,.1f}코어 / {sum(j['mem'] for j in jobs):,.1f}GB)")
    print(f"→ {out}")


if __name__ == "__main__":
    main()
```
