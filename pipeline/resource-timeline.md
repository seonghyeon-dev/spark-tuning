# 일일 리소스 사용량 비교 (as-is / to-be)

> 작업 엑셀의 AS-IS(튜닝 전)·TO-BE(튜닝 후) 시트로 하루 동안의 CPU·메모리 사용량을 계산해 비교한다. 결과는 원본과 같은 폴더에 `<원본>_resource_diff.xlsx`, `<원본>_resource_diff.html`로 생성된다.
>
> 예시 데이터(hourly Compaction만 튜닝) 결과: CPU 평균 사용량 29.6 → 23.4코어(−21%), 감소분 전량 comp_range. CPU 최대 사용량은 147 → 132코어(−10%)로 감소 폭이 작다. 최대 사용 시점의 62%를 설정 변경이 없는 append·comp_daily가 차지하기 때문이다.

- 앞 단계: [Airflow job 실행 시간 집계](airflow-job-duration.md) (Duration·Start Offset 추출)
- 필요: Python 3.9 이상, `pip install openpyxl plotly` (plotly가 없으면 엑셀만 생성)
- DRM 파일: Windows용 Python + `pip install xlwings` (§2.1). WSL·리눅스에서는 불가

---

## 1. 입력

작업 엑셀에 같은 규격의 시트 2개(`AS-IS`, `TO-BE`)가 있어야 한다. 시트 이름은 대소문자·기호 무시하고 정확히 일치하는 것을 먼저 찾는다(`AS-IS(x)`·`DIFF` 시트가 있어도 무방). 이름이 다르면 스크립트 상단 `SHEET_ASIS`·`SHEET_TOBE`에 지정한다.

| 열 | 내용 | 비고 |
|---|---|---|
| A | job_type (`append`, `summary`, `comp_range`, `comp_daily`, `exp_snap`, `del_orphan`, `rw_mani` …) | 결과의 종류 구분. 병합 셀 가능 |
| B | cron | trigger로만 실행되는 테이블은 부모 DAG의 cron. 비어 있으면 실행 간격 추정 (§4.4) |
| C | app name | as-is·to-be 매칭 키 (종류 + app name) |
| D~J | 드라이버·익스큐터 cpu, 메모리, 오버헤드, 익스큐터 개수 | 사용 안 함 (K·L 계산용) |
| K | 토탈 cpu (코어) | |
| L | 토탈 메모리 (GB, `228g` 형식도 가능) | |
| M~S | `job_durations.csv` F~L열: runs, Duration (min), median, max, Start Offset (min), oldest_run, latest_run | |
| T | 기능 요약 | 병합 셀 가능 |

- 병합된 셀은 병합 범위의 모든 행에 같은 값을 채운다
- N열(Duration)과 K열(토탈 cpu)이 모두 숫자인 행만 계산한다. Duration이 빈 행(미실행·삭제 job)은 제외되고, 한쪽 시트에만 있는 job은 `job별 비교`에서 다른 쪽을 0으로 계산한다
- to-be Duration은 운영 반영 전이면 테스트 실측값, 반영 후에는 `job_durations.py`로 다시 추출한 값을 쓴다

---

## 2. 실행

```bash
python resource_timeline.py 작업엑셀.xlsx              # 기준일 자동 선택
python resource_timeline.py 작업엑셀.xlsx 20261001     # 기준일 지정
```

**기준일**: 그래프를 그릴 하루. 지정하지 않으면 오늘부터 31일 안에서 모든 job이 실행되는 첫날을 고른다. 3일 주기 job(rw_mani `5 2 */3 * *` = 매달 1·4·7…31일)을 포함하기 위해서다. 날짜를 지정하면 그날 실행되지 않는 job 수를 출력한다.

출력 예 (예시 데이터):

```text
as-is: 시트 'AS-IS' job 32개, 제외 0개
to-be: 시트 'TO-BE' job 32개, 제외 0개
cron 시간대: KST (latest_run 36건 중 36건이 KST 기준 cron과 일치), 기준일 2026-10-01 (전체 job 실행일, 자동 선택)
· CPU 평균 사용량 29.6 → 23.4코어 (21% 감소). 감소분 전량 comp_range (12.2 → 6.0코어).
· CPU 최대 사용량 147 → 132코어 (10% 감소, to-be 01:50~01:55). 해당 시점 사용량의 62%가 설정 변경이 없는 job(append, comp_daily)이라 평균 사용량보다 감소 폭이 작음.
· 메모리 평균 사용량 100.8 → 77.6GB (23% 감소). 감소분 전량 comp_range (53.9 → 30.6GB).
· 메모리 최대 사용량 598 → 568GB (5% 감소, to-be 01:50~01:55). 해당 시점 사용량의 55%가 설정 변경이 없는 job(append, comp_daily)이라 평균 사용량보다 감소 폭이 작음.
→ 작업엑셀_resource_diff.xlsx
→ 작업엑셀_resource_diff.html
```

`제외`는 cron이 없고 실행 간격도 추정할 수 없는 행이다(요약 시트 하단에 목록).

### 2.1 DRM 파일

xlsx가 아닌 파일(DRM·xls·CSV 등)은 Windows에서 엑셀을 통해 읽는다. 엑셀 창은 표시되지 않고, 읽기 전용으로 열었다 닫는다.

1. Windows용 Python 설치 (python.org, "Add python.exe to PATH" 체크)
2. `pip install openpyxl xlwings plotly`
3. PowerShell에서 `python resource_timeline.py "C:\경로\작업엑셀.xlsx"`

- 사전 확인: `$x = New-Object -ComObject Excel.Application; $b = $x.Workbooks.Open("C:\경로\작업엑셀.xlsx"); $b.Sheets.Item("AS-IS").Range("A1:C3").Value2; $b.Close($false); $x.Quit()`에서 셀 값이 출력되면 사용 가능
- 수식 셀은 엑셀이 계산한 값을 읽는다. 병합 셀도 병합 범위를 확인해 채운다
- 열기 암호가 걸린 파일은 암호를 먼저 해제한다. `~$`로 시작하는 파일은 엑셀 잠금 파일이다
- 결과 파일에 DRM이 자동 적용되면 HTML이 브라우저에서 열리지 않을 수 있다 (미확인)

---

## 3. 결과

### 3.1 엑셀 (`_resource_diff.xlsx`)

| 시트 | 내용 |
|---|---|
| 요약 | 주요 결과, CPU·메모리 비교 표(평균 사용량, 최대 사용량, 최대 사용 시간대, 설정값 합계), 그래프 4개 |
| 설명 | 기준일·원본·cron 시간대, 시트 목록, 용어 |
| 종류별 비교 | job 종류별 평균 사용량, 변화, 변화율 |
| job별 비교 | job별 1회 실행 시간, CPU·메모리 설정, 평균 사용량 (필터 적용) |
| 최대 사용 시점 | 최대 사용 시점에 실행 중인 job과 CPU·메모리 합계. CPU와 메모리 최대 시점이 다르면 각각 표시 |
| 종류별 누적 그래프 | 5분 단위 사용량을 job 종류별로 누적 (as-is 위, to-be 아래, 동일 축) |
| 시간대별 비교 | 5분 단위 as-is·to-be 사용량과 변화 (요약 그래프 원본) |
| as-is/to-be 시간대별 | 5분 단위 job 종류별 사용량 (누적 그래프 원본) |
| as-is/to-be job별 | 작업 시트 입력값, 하루 실행 횟수, 하루 총 실행 시간, 평균 사용량 (계산 확인용) |

- 모든 시트 상단에 설명 한 줄. 열 폭은 내용에 맞추고 긴 글은 줄바꿈한다
- 변화 값은 감소 초록, 증가 빨강으로 표시한다
- 비교 표·합계는 수식이라 엑셀이 파일을 열 때 계산한다. LibreOffice로 다시 저장하면 x축 눈금 설정이 빠진다

### 3.2 HTML (`_resource_diff.html`)

인터넷 연결 없이 브라우저로 열린다(그래프 라이브러리 포함, 약 5MB).

| 순서 | 내용 |
|---|---|
| 상단 | 기준일, 원본, cron 시간대. 지표 4개(평균·최대 CPU, 평균·최대 메모리)와 as-is·to-be 막대 비교 |
| 1 | 주요 결과 |
| 2 | 시간대별 사용량 (as-is 회색, to-be 파랑, 차이 음영, 최대값 표시) |
| 3 | 종류별 평균 사용량 (변경된 종류만 변화율 표시) |
| 4 | 종류별 누적 사용량 |
| 5 | 최대 사용 시점의 실행 job |
| 6 | job별 비교 (종류 필터, 머리글 정렬) |
| 7 | 용어 |

- CPU / 메모리 버튼으로 2~6의 그래프·표를 전환한다
- 그래프에 마우스를 올리면 해당 5분 단위 값이 표시되고, 드래그하면 확대된다(더블클릭으로 복귀)
- 그래프 우측 상단 카메라 버튼으로 PNG 저장, `인쇄 / PDF` 버튼으로 CPU·메모리 전체를 인쇄
- HTML 선은 곡선(`SMOOTH`)으로 그리며 표시 값은 계산값 그대로다. 엑셀 그래프는 곡선 옵션이 급변 구간을 실제보다 크게 휘게 그려 직선으로 둔다

---

## 4. 지표와 계산

### 4.1 지표

| 지표 | 정의 | 예 |
|---|---|---|
| 평균 사용량 | 하루(1,440분) 동안 평균적으로 사용 중인 코어(GB) 수. 실행 횟수 × 1회 실행 시간(분) × 코어 ÷ 1,440. **튜닝 효과 판단 기준** | comp_range 1번 테이블: as-is 24회 × 2.9분 × 65코어 ÷ 1,440 = 3.1코어, to-be 24회 × 1.9분 × 50코어 ÷ 1,440 = 1.6코어 |
| 최대 사용량 | 하루 중 동시 사용량의 최댓값. 클러스터 확보 기준 | 01:50에 comp_daily(50) + append 8개(32) + comp_range(50) = 132코어 |
| 동시 사용량 | 특정 시점에 실행 중인 job들의 코어(GB) 합계 | |
| 5분 단위 값 | 해당 5분 동안의 최대 동시 사용량. 그래프와 시간대별 시트의 값 | 01:50~01:55 = 01:50부터 5분 사이 최댓값 |
| 설정값 합계 | 모든 job 설정값의 단순 합계. 모든 job이 동시에 실행된다는 가정의 값 (참고) | |
| 변화 / 변화율 | to-be − as-is / 변화 ÷ as-is. 음수는 감소 | |

평균 사용량은 하루 총 사용량(코어 × 시간)을 24시간으로 나눈 값과 같다. 단위가 코어라 최대 사용량, 설정값과 바로 비교할 수 있다.

최대 사용량은 한 시점의 값이라 튜닝하지 않은 job이 그 시점을 차지하면 덜 줄어든다. 예시에서 평균 사용량은 21% 줄었지만 최대 사용량은 10%만 줄었다. `주요 결과`에 해당 시점의 설정 변경 없는 job 비중을 표시한다.

### 4.2 실행 구간

job 1회 실행 = `cron 시각 + Start Offset`부터 `1회 실행 시간` 동안 토탈 cpu·메모리를 점유한다고 본다. 예: to-be comp_range 1번 테이블(cron `45 * * * *`, Offset 0.5분, 1.9분, 50코어) → 매시 45:30~47:24에 50코어.

### 4.3 6초 간격 측정과 5분 단위

동시 사용량은 하루를 6초 간격(14,400개 시점)으로 나눠 각 시점에 실행 중인 job만 더한다. 1분 단위로 더하면 순차 실행 job(예: 47:24 종료, 47:36 시작)이 같은 분에 겹쳐 실제보다 크게 잡힌다.

그래프는 5분 단위로 묶고 그 안의 최댓값을 쓴다. 5분마다 2.4분 실행되는 append가 그래프를 톱니 모양으로 만드는 것을 막기 위해서다. 단위 폭은 `BUCKET_MIN`으로 바꾼다. 최대 사용량은 단위 폭과 무관하다.

### 4.4 cron이 없는 행

B열이 비어 있거나 cron 형식이 아니면 `(latest_run − oldest_run) ÷ (runs − 1)`로 실행 간격을 구하고 가까운 정규 간격(5·10·15·30·60분, 1·2·3일 등)으로 맞춘다. job별 시트 cron 칸에 `60분마다 (추정)`으로 표시된다.

- trigger로만 실행되는 테이블은 B열에 부모 cron을 적는 것이 정확하다. 집계 스크립트의 `TRIGGER_TABLE`·`TRIGGER_PARENT_DAG`로 Start Offset이 부모 cron 기준으로 나온다 ([집계 문서](airflow-job-duration.md) §3.4)
- 3일 주기 job은 B열에 cron을 적는다. 추정은 월말(31일 → 1일)에 cron과 어긋날 수 있다

### 4.5 cron 시간대

latest_run(UTC)이 cron과 UTC 기준으로 맞는지, KST 기준으로 맞는지 세어 많은 쪽을 쓴다. 매시·5분 주기 job은 양쪽 모두 맞아 판단에서 제외된다. 결과는 `설명` 시트에 표시되고, 그래프는 항상 KST다.

---

## 5. 설정 (스크립트 상단)

| 설정 | 기본값 | 용도 |
|---|---|---|
| `SHEET_ASIS`, `SHEET_TOBE` | `None` | 시트 이름 직접 지정 |
| `COL_GROUP`, `COL_CRON`, `COL_NAME` | `A`, `B`, `C` | job_type·cron·app name 열 |
| `COL_TOTAL_CPU`, `COL_TOTAL_MEM` | `K`, `L` | 토탈 열 |
| `COL_RUNS` … `COL_LATEST` | `M` ~ `S` | `job_durations.csv` F~L열 위치 |
| `COL_DURATION` | `COL_DUR_AVG` | 1회 실행 시간 기준 (평균 / `COL_DUR_MEDIAN` / `COL_DUR_MAX`) |
| `COL_DESC` | `T` | 기능 요약 열 (`None`이면 미사용) |
| `CRON_TZ` | `"auto"` | 판단 근거가 없을 때 `"KST"`/`"UTC"` 지정 |
| `BUCKET_MIN` | `5` | 그래프 단위 폭(분), 1440의 약수 |
| `GROUP_COLORS` | 8색 | 종류 순서대로 배정, 9번째부터 회색 |
| `FONT`, `FONT_SIZE` | `맑은 고딕`, `10` | 결과 파일 글꼴 |
| `SMOOTH` | `0.5` | HTML 선 곡선 정도 (0 = 직선) |

---

## 6. 참고

| 항목 | 내용 |
|---|---|
| Compaction executor 수 | 평소 1시간치는 시작 대수(1번 12, 2번 8, 3·4번 12)로 K·L을 채운다. 재처리처럼 여러 시간치를 처리할 때는 최대 36대까지 늘어나므로 별도 시트로 계산한다 (`compaction-executor-sizing-design.md` §5.5) |
| Duration | Airflow task 시작~종료(pod 기동·spark-submit 포함)라 리소스 점유 시간에 맞다 ([집계 문서](airflow-job-duration.md) §5) |
| 수동 실행 | `job_durations.py`는 `scheduled` 실행만 집계하므로 재처리가 trigger한 Compaction은 포함되지 않는다 |
| Duration > cron 간격 | 앞 실행과 겹치는 구간은 합산된다. `max_active_runs=1`이면 실제로는 밀려서 실행되며 그 대기는 Start Offset에 반영돼 있다 |
| 3일 주기 job | 기준일이 rw_mani 실행일이라 평균 사용량에 rw_mani 1회분이 포함된다 (예시에서 전체의 0.3%) |

---

## 7. 스크립트

```python
"""하루 리소스 사용량 as-is / to-be 비교 시각화 — 작업 엑셀(as-is 시트·to-be 시트) → 비교 그래프 엑셀.

실행:
    python resource_timeline.py <작업엑셀.xlsx>              # 기준일 = 오늘부터 모든 job이 도는 첫날 (3일마다 도는 job 포함)
    python resource_timeline.py <작업엑셀.xlsx> 20260928     # 기준일 직접 지정
  as-is·to-be 시트는 이름으로 찾는다 (as-is / asis / AS_IS, to-be / tobe …). 다르면 SHEET_ASIS·SHEET_TOBE에 이름을 넣는다
결과: 같은 폴더에 <작업엑셀>_resource_diff.xlsx, <작업엑셀>_resource_diff.html  (원본은 건드리지 않는다)
필요: pip install openpyxl plotly   (plotly가 없으면 엑셀만 만든다)
  사내 보안(DRM)이 걸린 파일은 Python이 직접 못 연다 → Windows용 Python에서 실행하면 엑셀을 통해 값을 읽는다
  (pip install xlwings 추가. WSL·리눅스에서는 엑셀을 조종할 수 없어 안 된다)
"""
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.chart import AreaChart, BarChart, LineChart, Reference
from openpyxl.chart.axis import ChartLines
from openpyxl.chart.text import RichText, Text
from openpyxl.chart.title import Title
from openpyxl.drawing.text import CharacterProperties, Font as DrawingFont, Paragraph, ParagraphProperties, RegularTextRun
from openpyxl.chart.shapes import GraphicalProperties
from openpyxl.drawing.line import LineProperties
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import column_index_from_string, get_column_letter

# ── 설정 ───────────────────────────────────────────────────────────────
# 비교할 두 시트 이름. None이면 시트 이름에서 as-is / to-be를 찾는다 (대소문자·기호 무시)
SHEET_ASIS = None
SHEET_TOBE = None
# 작업 시트의 열 위치 (as-is·to-be 같은 규격)
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
COL_DESC = "T"               # 기능 요약 (결과 표에 같이 보여 준다. 병합돼 있어도 된다. 없으면 None)
# cron을 어느 시간대로 적었나: "auto"(latest_run으로 판단) / "KST" / "UTC". 그래프는 항상 KST로 그린다
CRON_TZ = "auto"
KST = timezone(timedelta(hours=9))
# job 종류 = 작업 시트 A열(job_type) 값 그대로(append, summary, comp_range …). 순서는 AS-IS 시트에 처음 나온 순서.
# 병합된 칸(A열 job_type, T열 기능 요약 등)은 병합 범위의 모든 행에 같은 값을 채운다. 색은 종류 순서대로 배정(9번째부터 회색)
GROUP_COLORS = ["2A78D6", "EB6834", "1BAF7A", "EDA100", "E87BA4", "008300", "4A3AA7", "E34948"]
# 그래프 한 칸의 폭(분). 칸 안에서 합계가 가장 큰 순간의 값을 그 칸의 값으로 쓴다.
# 1로 두면 5분 주기 append가 켜졌다 꺼지는 톱니가 그대로 보여 읽기 어렵다. 1440의 약수로 (1, 5, 10, 15 …)
BUCKET_MIN = 5
# ─────────────────────────────────────────────────────────────────────

FONT = "맑은 고딕"                # 글꼴·크기는 결과 엑셀·HTML 전체에 하나로 통일
FONT_SIZE = 10
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


# ── 입력 읽기 ─────────────────────────────────────────────────────────
def sheet_rows(ws):
    """(행 번호, [A열부터의 값]) 목록. 병합 셀은 병합 범위의 모든 칸에 왼쪽 위 칸의 값을 채운다
    (예: A2:A9를 병합해 'append'를 한 번만 적었으면 2~9행 모두 'append')."""
    rows = {r[0].row: [c.value for c in r] for r in ws.iter_rows(min_col=1)}
    for rng in ws.merged_cells.ranges:
        v = ws.cell(row=rng.min_row, column=rng.min_col).value
        for r in range(rng.min_row, rng.max_row + 1):
            for c in range(rng.min_col, rng.max_col + 1):
                if r in rows and c - 1 < len(rows[r]):
                    rows[r][c - 1] = v
    return sorted(rows.items())


def read_jobs(ws):
    col = lambda letter: column_index_from_string(letter) - 1
    rows = sheet_rows(ws)
    data = [(rn, vals) for rn, vals in rows
            if len(vals) > col(COL_DURATION) and to_number(vals[col(COL_DURATION)]) is not None
            and to_number(vals[col(COL_TOTAL_CPU)]) is not None]
    if not data:
        raise SystemExit(f"{COL_DURATION}열(Duration)과 {COL_TOTAL_CPU}열(토탈 cpu)에 숫자가 있는 행 없음. 열 설정 확인 필요 "
                         f"({COL_TOTAL_CPU}열이 수식이면 엑셀에서 한 번 저장한 파일이어야 값이 읽힘)")
    # Duration(N열)이 빈 행(안 돌린 job·to-be에서 삭제한 job)은 여기서 빠진다

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
                skipped.append((rn, name, "cron 없음, 실행 간격 추정 불가"))
                continue
            job["every"] = every
            job["cron_text"] = f"{every}분마다 (추정)"
        job["group"] = str(get(COL_GROUP) or "").strip() or "기타"
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


def runs_on(j, day, cron_tz):
    """기준일(KST) 0~24시 안에 시작하는 실행이 있나."""
    return any(0 <= s - j["offset"] < 1440 for s, _ in fire_times(j, day, cron_tz))


def pick_day(jobs, cron_tz, start):
    """start부터 31일 안에서 모든 job이 한 번 이상 도는 첫날. 3일마다 도는 rw_mani(예: 5 2 */3 * *)까지 그림에 넣으려고.
    그런 날이 없으면 start."""
    for k in range(31):
        d = start + timedelta(days=k)
        if all(runs_on(j, d, cron_tz) for j in jobs):
            return d, True
    return start, False


def order_groups(jobs):
    """job 종류를 작업 시트에 처음 나온 순서로 (AS-IS → TO-BE 순으로 훑는다)."""
    return list(dict.fromkeys(j["group"] for j in jobs))


def group_color(groups, g):
    i = groups.index(g)
    return GROUP_COLORS[i] if i < len(GROUP_COLORS) else "8C8C8C"


def simulate(jobs, day, cron_tz, groups):
    n = 1440 * SAMPLES_PER_MIN
    step = 1 / SAMPLES_PER_MIN
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
    return per_minute, runs, spans


def active_jobs(jobs, runs_all, t_min):
    out = []
    for idx, j in enumerate(jobs):
        for s, e in runs_all.get(idx, []):
            if s <= t_min < e:
                out.append(j)
                break
    return out


# ── 지표 (엑셀·HTML 공통) ──────────────────────────────────────────────
SIDES = ("as-is", "to-be")
UNIT = {"cpu": "코어", "mem": "GB"}
KIND = {"cpu": "CPU", "mem": "메모리"}


def hhmm(t):
    t = int(round(t)) % 1441
    return f"{t // 60:02d}:{t % 60:02d}"


def range_label(b):
    """5분 단위 b의 이름: 01:50~01:55"""
    return f"{hhmm(b * BUCKET_MIN)}~{hhmm((b + 1) * BUCKET_MIN)}"


def job_avg(n, j, k):
    """job 하나의 평균 사용량 = 실행 횟수 × 1회 실행 시간(분) × 코어(GB) ÷ 1,440분"""
    return n * j["duration"] * j[k] / 1440


def side_metrics(d):
    """as-is 또는 to-be 한쪽의 지표."""
    groups, jobs = d["groups"], d["jobs"]
    n_runs = [len(d["runs"].get(i, [])) for i in range(len(jobs))]
    out = {"n_runs": n_runs}
    for k in ("cpu", "mem"):
        series = [sum(d["per_minute"][k][b][g] for g in groups) for b in range(NB)]
        peak = max(series)
        pb = series.index(peak)
        out[k] = {"series": series, "peak": peak, "peak_bucket": pb, "peak_at": range_label(pb),
                  "avg": sum(job_avg(n, j, k) for n, j in zip(n_runs, jobs)), "simple": sum(j[k] for j in jobs),
                  "group_avg": {g: sum(job_avg(n, j, k) for n, j in zip(n_runs, jobs) if j["group"] == g)
                                for g in groups},
                  "by_group": {g: [d["per_minute"][k][b][g] for b in range(NB)] for g in groups}}
        t = d["per_minute"][k][pb]["_sample"] / SAMPLES_PER_MIN + 0.05
        out[k]["peak_time"] = t
        out[k]["peak_jobs"] = sorted(active_jobs(jobs, d["spans"], t), key=lambda j: (-j[k], j["name"]))
    return out


def change_text(a, b):
    if abs(b - a) < 1e-9:
        return "변화 없음"
    if not a:
        return "신규"
    return f"{abs(b - a) / a * 100:.0f}% {'감소' if b < a else '증가'}"


def headline(M, groups):
    """요약 문장 (숫자는 계산값)."""
    A, B = M["as-is"], M["to-be"]
    lines = []
    for k, u in (("cpu", "코어"), ("mem", "GB")):
        a, b = A[k]["avg"], B[k]["avg"]
        line = f"{KIND[k]} 평균 사용량 {a:,.1f} → {b:,.1f}{u} ({change_text(a, b)})."
        diffs = {g: B[k]["group_avg"].get(g, 0) - A[k]["group_avg"].get(g, 0) for g in groups}
        if abs(b - a) > 1e-9:
            g = min(diffs, key=diffs.get) if b < a else max(diffs, key=diffs.get)
            share = diffs[g] / (b - a) * 100
            what = "감소분" if b < a else "증가분"
            src = f"{what} 전량" if share >= 99.5 else f"{what}의 {share:.0f}%가"
            line += (f" {src} {g} ({A[k]['group_avg'].get(g, 0):,.1f} → {B[k]['group_avg'].get(g, 0):,.1f}{u}).")
        lines.append(line)
        pa, pb = A[k]["peak"], B[k]["peak"]
        line = f"{KIND[k]} 최대 사용량 {pa:,.0f} → {pb:,.0f}{u} ({change_text(pa, pb)}, to-be {B[k]['peak_at']})."
        same = [g for g in groups if abs(diffs[g]) < 1e-9]
        share = sum(B[k]["by_group"][g][B[k]["peak_bucket"]] for g in same) / pb if pb else 0
        if a and pa and (pa - pb) / pa < (a - b) / a and share >= 0.5:
            used = [g for g in same if B[k]["by_group"][g][B[k]["peak_bucket"]] > 0]
            line += (f" 해당 시점 사용량의 {share * 100:.0f}%가 설정 변경이 없는 job({', '.join(used)})이라 "
                     f"평균 사용량보다 감소 폭이 작음.")
        lines.append(line)
    return lines


# ── 엑셀 쓰기 ─────────────────────────────────────────────────────────
SIDE_COLORS = {"as-is": "9AA0A6", "to-be": "2A78D6"}     # 튜닝 전 = 회색(기준), 튜닝 후 = 파랑
INK, MUTED, GRID = "1F2328", "6B6B6B", "E3E6EA"
HEAD_FILL = PatternFill("solid", fgColor="EEF2F7")
THIN = Side(style="thin", color="D5DAE1")
BORDER = Border(bottom=THIN)
NOTE_FONT = Font(name=FONT, size=FONT_SIZE, color=MUTED)
BOLD_FONT = Font(name=FONT, size=FONT_SIZE, bold=True, color=INK)
CHG = "[Red]+#,##0.0;[Color10]-#,##0.0;0.0"              # 변화: 증가 빨강, 감소 초록
CHG2 = "[Red]+#,##0.00;[Color10]-#,##0.00;0.00"
CHG_PCT = "[Red]+0.0%;[Color10]-0.0%;0.0%"
CHART_W, CHART_H, CHART_ROWS = 30, 11, 23      # 그래프 폭·높이(cm), 그래프 하나가 차지하는 행 수
LABEL_EVERY_MIN = 120                          # x축 눈금 간격(분)
HEAD = 4                                       # 표 시트: 1행 제목, 2행 설명, 4행 머리글, 5행부터 값
FIRST = HEAD + 1


def intro(ws, title, *lines):
    ws.cell(row=1, column=1, value=title).font = BOLD_FONT
    for i, line in enumerate(lines, 2):
        ws.cell(row=i, column=1, value=line).font = NOTE_FONT


def style_header(ws, row, ncol, first=1):
    for c in range(first, first + ncol):
        cell = ws.cell(row=row, column=c)
        cell.font = BOLD_FONT
        cell.fill = HEAD_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = BORDER
    ws.row_dimensions[row].height = 32


def header(ws, names, row=HEAD):
    for c, h in enumerate(names, 1):
        ws.cell(row=row, column=c, value=h)
    style_header(ws, row, len(names))


def text_width(v):
    """엑셀 열 폭 단위로 본 글자 폭 (맑은 고딕 10: 한글 1자 ≈ 1.9, 영문·숫자 ≈ 1.1)."""
    if isinstance(v, float):
        v = f"{v:,.2f}"
    return sum(1.9 if ord(ch) >= 0x1100 else 1.1 for ch in str(v))


def autofit(ws, head_row, first_row, last_row=None, cap=50, fixed=None):
    """열 폭을 내용에 맞춘다. 머리글은 두 줄로 접히므로 절반 폭만 본다. cap을 넘는 칸은 줄바꿈하고 행 높이를 늘린다."""
    last_row = last_row or ws.max_row
    width = {}
    for row in ws.iter_rows(min_row=head_row, max_row=last_row):
        for c in row:
            if c.value is None or c.coordinate in ws.merged_cells:
                continue
            w = 10 if isinstance(c.value, str) and c.value.startswith("=") else text_width(c.value)
            w = w / 2 + 1 if c.row == head_row else w
            width[c.column_letter] = max(width.get(c.column_letter, 0), w)
    for col, w in width.items():
        w = (fixed or {}).get(col, min(max(w + 2, 8), cap))
        ws.column_dimensions[col].width = w
        for row in ws.iter_rows(min_row=first_row, max_row=last_row, min_col=column_index_from_string(col),
                                max_col=column_index_from_string(col)):
            c = row[0]
            if isinstance(c.value, str) and not c.value.startswith("=") and text_width(c.value) > w - 1:
                c.alignment = Alignment(wrap_text=True, vertical="top")
                lines = math.ceil(text_width(c.value) / (w - 1))
                ws.row_dimensions[c.row].height = max(ws.row_dimensions[c.row].height or 15, lines * 15 + 3)


def set_font(ws):
    """시트의 모든 글자를 맑은 고딕 10으로 (굵기·색만 유지)."""
    for row in ws.iter_rows():
        for c in row:
            if c.value is not None:
                c.font = Font(name=FONT, size=FONT_SIZE, bold=c.font.bold,
                              color=c.font.color if c.font.color and c.font.color.rgb not in (None, "FF000000") else INK)


def nice_max(v):
    """y축 상한을 보기 좋은 숫자로 (두 그래프를 같은 눈금으로 맞출 때 쓴다)."""
    if v <= 0:
        return 1
    mag = 10 ** math.floor(math.log10(v))
    return next(m * mag for m in (1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 7, 8, 10) if v <= m * mag)


# ── 그래프 글자: 맑은 고딕 10 ──────────────────────────────────────────
def _char(bold=False, color=INK):
    return CharacterProperties(latin=DrawingFont(typeface=FONT), ea=DrawingFont(typeface=FONT),
                               sz=FONT_SIZE * 100, b=bold, solidFill=color)


def _rich(bold=False, color=INK):
    return RichText(p=[Paragraph(pPr=ParagraphProperties(defRPr=_char(bold, color)), endParaRPr=_char(bold, color))])


def _title(text, bold=True, color=INK):
    para = Paragraph(pPr=ParagraphProperties(defRPr=_char(bold, color)), r=[RegularTextRun(rPr=_char(bold, color), t=text)])
    return Title(tx=Text(rich=RichText(p=[para])), overlay=False)


def style_chart(ch, title, y_title, y_max=None, y_fmt="#,##0"):
    ch.title = _title(title)
    ch.y_axis.title = _title(y_title, bold=False, color=MUTED)
    ch.x_axis.delete = ch.y_axis.delete = False
    ch.x_axis.txPr = ch.y_axis.txPr = _rich(color=MUTED)
    ch.y_axis.numFmt = y_fmt
    ch.y_axis.majorGridlines = ChartLines(spPr=GraphicalProperties(ln=LineProperties(solidFill=GRID)))
    ch.x_axis.spPr = GraphicalProperties(ln=LineProperties(solidFill=GRID))
    ch.y_axis.spPr = GraphicalProperties(ln=LineProperties(noFill=True))
    ch.x_axis.majorTickMark = ch.y_axis.majorTickMark = "none"
    if y_max:
        ch.y_axis.scaling.min, ch.y_axis.scaling.max = 0, y_max
    ch.legend.position = "t"
    ch.legend.txPr = _rich()
    ch.graphical_properties = GraphicalProperties(ln=LineProperties(noFill=True))   # 그래프 바깥 테두리 없음
    ch.width, ch.height = CHART_W, CHART_H


def time_axis(ch):
    ch.x_axis.tickLblSkip = LABEL_EVERY_MIN // BUCKET_MIN
    ch.x_axis.tickMarkSkip = LABEL_EVERY_MIN // BUCKET_MIN
    ch.x_axis.noMultiLvlLbl = True


def area_chart(title, ws, groups, first_col, label_col, y_title, y_max):
    ch = AreaChart()
    ch.grouping = "stacked"
    ch.add_data(Reference(ws, min_col=first_col, max_col=first_col + len(groups) - 1, min_row=HEAD, max_row=HEAD + NB),
                titles_from_data=True)
    ch.set_categories(Reference(ws, min_col=label_col, min_row=FIRST, max_row=HEAD + NB))
    for g, s in zip(groups, ch.series):
        s.graphicalProperties.solidFill = group_color(groups, g)
        s.graphicalProperties.line.solidFill = "FFFFFF"      # 종류 사이 흰 경계선
        s.graphicalProperties.line.width = 6350
    time_axis(ch)
    style_chart(ch, title, y_title, y_max)
    return ch


def compare_line_chart(title, ws, cols, label_col, y_title, y_max):
    ch = LineChart()
    for c in cols:
        ch.add_data(Reference(ws, min_col=c, min_row=HEAD, max_row=HEAD + NB), titles_from_data=True)
    ch.set_categories(Reference(ws, min_col=label_col, min_row=FIRST, max_row=HEAD + NB))
    for side, s in zip(SIDES, ch.series):
        s.graphicalProperties.line.solidFill = SIDE_COLORS[side]
        s.graphicalProperties.line.width = 25400        # 2pt
        s.marker.symbol = "none"
        s.smooth = False     # 엑셀 곡선은 급변 구간에서 실제 값보다 크게 휘어 직선으로 둔다
    time_axis(ch)
    style_chart(ch, title, y_title, y_max)
    return ch


def compare_bar_chart(title, ws, last_row, cols, y_title):
    ch = BarChart()
    ch.type, ch.grouping = "col", "clustered"
    for c in cols:
        ch.add_data(Reference(ws, min_col=c, min_row=HEAD, max_row=last_row), titles_from_data=True)
    ch.set_categories(Reference(ws, min_col=1, min_row=FIRST, max_row=last_row))
    for side, s in zip(SIDES, ch.series):
        s.graphicalProperties.solidFill = SIDE_COLORS[side]
        s.graphicalProperties.line.noFill = True
    ch.gapWidth, ch.overlap = 60, -10
    style_chart(ch, title, y_title, y_fmt="#,##0.0")
    return ch


def time_label(b):
    """그래프 x축 눈금 — LABEL_EVERY_MIN마다만 적는다 (나머지는 빈칸)."""
    t = b * BUCKET_MIN
    return hhmm(t) if t % LABEL_EVERY_MIN == 0 else ""


def write_timeline(wb, side, groups, per_minute):
    """<side> 시간대별: 5분 단위 = 1행. 종류별 CPU·메모리 + 합계(수식)."""
    ws = wb.create_sheet(f"{side} 시간대별")
    intro(ws, f"{side} 시간대별 사용량", "5분 단위 최대 동시 사용량, job 종류별 (종류별 누적 그래프 원본)")
    g = len(groups)
    cpu_c, mem_c = 2, 2 + g + 1
    cpu_tot, mem_tot = 2 + g, 2 + 2 * g + 1
    label_col = mem_tot + 1
    header(ws, ["시간대 (KST)"] + [f"CPU {x} (코어)" for x in groups] + ["CPU 합계 (코어)"] +
           [f"메모리 {x} (GB)" for x in groups] + ["메모리 합계 (GB)", "그래프 눈금"])
    for b in range(NB):
        r = FIRST + b
        ws.cell(row=r, column=1, value=range_label(b))
        for i, x in enumerate(groups):
            ws.cell(row=r, column=cpu_c + i, value=per_minute["cpu"][b][x]).number_format = "#,##0.0"
            ws.cell(row=r, column=mem_c + i, value=per_minute["mem"][b][x]).number_format = "#,##0.0"
        for tot, first in ((cpu_tot, cpu_c), (mem_tot, mem_c)):
            ws.cell(row=r, column=tot,
                    value=f"=SUM({get_column_letter(first)}{r}:{get_column_letter(first + g - 1)}{r})"
                    ).number_format = "#,##0.0"
        ws.cell(row=r, column=label_col, value=time_label(b))
    ws.freeze_panes = f"B{FIRST}"
    autofit(ws, HEAD, FIRST)
    set_font(ws)
    return cpu_c, mem_c, get_column_letter(cpu_tot), get_column_letter(mem_tot), label_col


def write_jobs(wb, side, jobs, runs):
    """<side> job별: 작업 시트 값 + 평균 사용량(수식)."""
    ws = wb.create_sheet(f"{side} job별")
    intro(ws, f"{side} job별 사용량", "평균 사용량 = 하루 총 실행 시간(분) × 코어(GB) ÷ 1,440분. "
                                     "하루 실행 횟수 = 기준일 하루 동안 cron에 따른 시작 횟수")
    header(ws, ["작업 시트 행", "종류", "app name", "cron", "하루 실행 횟수", "Start Offset (분)",
                "1회 실행 시간 (분)", "하루 총 실행 시간 (분)", "CPU (코어)", "메모리 (GB)",
                "평균 CPU 사용량 (코어)", "평균 메모리 사용량 (GB)", "기능 요약"])
    for i, j in enumerate(jobs):
        r = FIRST + i
        for c, v in enumerate([j["row"], j["group"], j["name"], j["cron_text"], len(runs.get(i, [])), j["offset"],
                               j["duration"], f"=E{r}*G{r}", j["cpu"], j["mem"], f"=H{r}*I{r}/1440",
                               f"=H{r}*J{r}/1440", j["desc"] or None], 1):
            ws.cell(row=r, column=c, value=v)
        for c, fmt in ((6, "0.0"), (7, "0.0"), (8, "#,##0.0"), (9, "#,##0.0"), (10, "#,##0.0"),
                       (11, "#,##0.00"), (12, "#,##0.00")):
            ws.cell(row=r, column=c).number_format = fmt
    ws.freeze_panes = f"D{FIRST}"
    ws.auto_filter.ref = f"A{HEAD}:M{max(FIRST, FIRST + len(jobs) - 1)}"
    autofit(ws, HEAD, FIRST, cap=60)
    set_font(ws)
    return FIRST + len(jobs) - 1


def write_compare_timeline(wb, tl):
    """시간대별 비교: as-is·to-be 합계를 나란히 (각 시간대별 시트를 참조하는 수식)."""
    ws = wb.create_sheet("시간대별 비교")
    intro(ws, "시간대별 사용량 비교", "5분 단위 최대 동시 사용량 (요약 시트 그래프 원본). 변화 = to-be − as-is")
    header(ws, ["시간대 (KST)", "as-is CPU (코어)", "to-be CPU (코어)", "CPU 변화 (코어)", "as-is 메모리 (GB)",
                "to-be 메모리 (GB)", "메모리 변화 (GB)", "그래프 눈금"])
    for b in range(NB):
        r = FIRST + b
        ws.cell(row=r, column=1, value=range_label(b))
        for col, (side, key) in zip((2, 3, 5, 6), (("as-is", "ct"), ("to-be", "ct"), ("as-is", "mt"), ("to-be", "mt"))):
            ws.cell(row=r, column=col, value=f"='{side} 시간대별'!{tl[side][key]}{r}").number_format = "#,##0.0"
        ws.cell(row=r, column=4, value=f"=C{r}-B{r}").number_format = CHG
        ws.cell(row=r, column=7, value=f"=F{r}-E{r}").number_format = CHG
        ws.cell(row=r, column=8, value=time_label(b))
    ws.freeze_panes = f"B{FIRST}"
    autofit(ws, HEAD, FIRST)
    set_font(ws)
    return ws


def write_group_compare(wb, groups, last):
    """종류별 비교: 평균 사용량을 종류별로 SUMIF."""
    ws = wb.create_sheet("종류별 비교")
    intro(ws, "종류별 평균 사용량 비교", "job 종류별 평균 사용량 합계. 변화 = to-be − as-is, 변화율 = 변화 ÷ as-is")
    header(ws, ["종류", "as-is 평균 CPU (코어)", "to-be 평균 CPU (코어)", "CPU 변화 (코어)", "CPU 변화율",
                "as-is 평균 메모리 (GB)", "to-be 평균 메모리 (GB)", "메모리 변화 (GB)", "메모리 변화율"])
    rng = lambda side, col: f"'{side} job별'!${col}${FIRST}:${col}${last[side]}"
    for i, g in enumerate(groups):
        r = FIRST + i
        ws.cell(row=r, column=1, value=g)
        for col, side, src in ((2, "as-is", "K"), (3, "to-be", "K"), (6, "as-is", "L"), (7, "to-be", "L")):
            ws.cell(row=r, column=col, value=f"=SUMIF({rng(side, 'B')},$A{r},{rng(side, src)})")
    tr = FIRST + len(groups)
    ws.cell(row=tr, column=1, value="합계").font = BOLD_FONT
    for col in (2, 3, 6, 7):
        L = get_column_letter(col)
        ws.cell(row=tr, column=col, value=f"=SUM({L}{FIRST}:{L}{tr - 1})")
    for r in range(FIRST, tr + 1):
        ws.cell(row=r, column=4, value=f"=C{r}-B{r}")
        ws.cell(row=r, column=5, value=f'=IF(B{r}=0,"",D{r}/B{r})')
        ws.cell(row=r, column=8, value=f"=G{r}-F{r}")
        ws.cell(row=r, column=9, value=f'=IF(F{r}=0,"",H{r}/F{r})')
        for c, fmt in ((2, "#,##0.00"), (3, "#,##0.00"), (4, CHG2), (5, CHG_PCT),
                       (6, "#,##0.00"), (7, "#,##0.00"), (8, CHG2), (9, CHG_PCT)):
            ws.cell(row=r, column=c).number_format = fmt
        if r == tr:
            for c in range(1, 10):
                ws.cell(row=r, column=c).border = Border(top=THIN)
    autofit(ws, HEAD, FIRST)
    set_font(ws)
    return ws, tr


def job_pairs(sides_jobs, groups):
    """(종류, app name)으로 as-is·to-be job을 짝짓는다. 순서 = 종류 순서 → 처음 나온 순서."""
    keys, pos = [], {}
    for side in SIDES:
        for i, j in enumerate(sides_jobs[side]):
            k = (j["group"], j["name"])
            if k not in pos:
                pos[k] = {"desc": j["desc"]}
                keys.append(k)
            pos[k][side] = i
            pos[k]["desc"] = pos[k]["desc"] or j["desc"]
    seen = {k: n for n, k in enumerate(keys)}
    keys.sort(key=lambda k: (groups.index(k[0]), seen[k]))
    return keys, pos


def write_job_compare(wb, sides_jobs, groups):
    """job별 비교: (종류, app name)으로 as-is·to-be 행을 짝지어 나란히. 값은 각 job별 시트 참조."""
    ws = wb.create_sheet("job별 비교")
    intro(ws, "job별 비교", "종류 + app name 기준 매칭. 한쪽에만 있는 job은 다른 쪽을 0으로 계산")
    header(ws, ["종류", "app name", "기능 요약", "as-is 1회 실행 시간 (분)", "to-be 1회 실행 시간 (분)",
                "as-is CPU (코어)", "to-be CPU (코어)", "as-is 메모리 (GB)", "to-be 메모리 (GB)",
                "as-is 평균 CPU (코어)", "to-be 평균 CPU (코어)", "평균 CPU 변화 (코어)",
                "as-is 평균 메모리 (GB)", "to-be 평균 메모리 (GB)", "평균 메모리 변화 (GB)"])
    keys, pos = job_pairs(sides_jobs, groups)
    for i, k in enumerate(keys):
        r = FIRST + i
        ws.cell(row=r, column=1, value=k[0])
        ws.cell(row=r, column=2, value=k[1])
        ws.cell(row=r, column=3, value=pos[k]["desc"] or None)
        for (ca, ct), src, fmt in (((4, 5), "G", "0.0"), ((6, 7), "I", "#,##0.0"), ((8, 9), "J", "#,##0.0"),
                                   ((10, 11), "K", "#,##0.00"), ((13, 14), "L", "#,##0.00")):
            for col, side in ((ca, "as-is"), (ct, "to-be")):
                if side in pos[k]:
                    ws.cell(row=r, column=col, value=f"='{side} job별'!{src}{pos[k][side] + FIRST}").number_format = fmt
        ws.cell(row=r, column=12, value=f"=N(K{r})-N(J{r})").number_format = CHG2
        ws.cell(row=r, column=15, value=f"=N(N{r})-N(M{r})").number_format = CHG2
    ws.freeze_panes = f"D{FIRST}"
    ws.auto_filter.ref = f"A{HEAD}:O{max(FIRST, FIRST + len(keys) - 1)}"
    autofit(ws, HEAD, FIRST, cap=40)
    set_font(ws)


def write_peaks(wb, sides, M):
    """최대 사용 시점에 실행 중인 job — CPU 최대 시점과 메모리 최대 시점 (같은 시점이면 한 번만)."""
    ws = wb.create_sheet("최대 사용 시점")
    intro(ws, "최대 사용 시점의 실행 job", "최대 사용 시점에 실행 중인 job 목록. 합계 = 최대 사용량")
    r = HEAD
    titles = []
    for side in SIDES:
        blocks = [("cpu", M[side]["cpu"])]
        if abs(M[side]["mem"]["peak_time"] - M[side]["cpu"]["peak_time"]) > 1e-6:
            blocks.append(("mem", M[side]["mem"]))
        for k, m in blocks:
            act = m["peak_jobs"]
            titles.append(r)
            what = "CPU·메모리" if len(blocks) == 1 else KIND[k]
            ws.cell(row=r, column=1, value=f"{side} {what} 최대 사용 시점 {hhmm(m['peak_time'])} "
                                           f"({range_label(m['peak_bucket'])}), job {len(act)}개").font = BOLD_FONT
            header(ws, ["app name", "종류", "CPU (코어)", "메모리 (GB)", "기능 요약"], row=r + 1)
            for i, j in enumerate(act, start=r + 2):
                ws.cell(row=i, column=1, value=j["name"])
                ws.cell(row=i, column=2, value=j["group"])
                ws.cell(row=i, column=3, value=j["cpu"]).number_format = "#,##0.0"
                ws.cell(row=i, column=4, value=j["mem"]).number_format = "#,##0.0"
                ws.cell(row=i, column=5, value=j["desc"] or None)
            tr = r + 2 + len(act)
            ws.cell(row=tr, column=1, value="합계").font = BOLD_FONT
            for c in (3, 4):
                L = get_column_letter(c)
                ws.cell(row=tr, column=c, value=f"=SUM({L}{r + 2}:{L}{tr - 1})").number_format = "#,##0.0"
            for c in range(1, 6):
                ws.cell(row=tr, column=c).border = Border(top=THIN)
            r = tr + 3
    autofit(ws, HEAD + 1, HEAD + 2, fixed={"A": 40}, cap=60)
    for t in titles:                                  # 목록 제목 줄은 줄바꿈 없이 옆 칸으로 이어 쓴다
        ws.cell(row=t, column=1).alignment = Alignment()
        ws.row_dimensions[t].height = None
    set_font(ws)


GUIDE_SHEETS = [
    ("요약", "주요 결과, as-is·to-be 비교 표, 그래프 (시간대별 사용량, 종류별 평균 사용량)"),
    ("종류별 비교", "job 종류별 평균 사용량과 변화"),
    ("job별 비교", "job별 1회 실행 시간, CPU·메모리 설정, 평균 사용량 비교"),
    ("최대 사용 시점", "최대 사용 시점에 실행 중인 job 목록과 합계"),
    ("종류별 누적 그래프", "5분 단위 사용량을 job 종류별로 누적한 그래프 (as-is·to-be 동일 축)"),
    ("시간대별 비교", "5분 단위 as-is·to-be 사용량 (요약 그래프 원본)"),
    ("as-is/to-be 시간대별", "5분 단위 job 종류별 사용량 (누적 그래프 원본)"),
    ("as-is/to-be job별", "작업 시트 입력값, 하루 실행 횟수, 평균 사용량 계산"),
]
GUIDE_TERMS = [
    ("평균 사용량", "하루(1,440분) 동안 평균적으로 사용 중인 코어(GB) 수. 실행 횟수 × 1회 실행 시간(분) × 코어 ÷ 1,440. "
                "튜닝 효과 판단 기준", "매시 1회, 3분, 48코어 → 24 × 3 × 48 ÷ 1,440 = 2.4코어"),
    ("최대 사용량", "하루 중 동시 사용량의 최댓값. 클러스터 확보 기준", ""),
    ("동시 사용량", "특정 시점에 실행 중인 job들의 코어(GB) 합계", "50코어 job과 32코어 job이 함께 실행 중이면 82코어"),
    ("5분 단위 값", "해당 5분 동안의 최대 동시 사용량. 그래프와 시간대별 시트의 값", "01:50~01:55: 01:50부터 5분 사이 최댓값"),
    ("설정값 합계", "모든 job 설정값의 단순 합계. 모든 job이 동시에 실행된다고 가정한 값 (참고)", ""),
    ("변화 / 변화율", "to-be − as-is / 변화 ÷ as-is. 음수는 감소", ""),
    ("1회 실행 시간", "작업 시트 Duration. Airflow task 시작~종료 (pod 기동 포함)", ""),
    ("Start Offset", "cron 예정 시각부터 실제 시작까지의 시간. 순차 실행 job은 앞 job 대기 시간 포함", "cron 01:00, Offset 19.3분 → 01:19 시작"),
    ("기준일", "그래프 대상 날짜. 3일 주기 job(rw_mani)까지 실행되는 날을 자동 선택", ""),
]


def write_guide(wb, day, day_note, sides, cron_tz, tz_reason, src):
    ws = wb.create_sheet("설명")
    ws.sheet_view.showGridLines = False
    intro(ws, "설명", "job별로 cron 시각 + Start Offset에 시작해 1회 실행 시간 동안 설정 리소스를 점유하는 것으로 계산")
    r = HEAD
    info = [("기준일 (KST)", f"{day.isoformat()} ({day_note})"),
            ("원본", f"{src} (as-is '{sides['as-is']['sheet']}', to-be '{sides['to-be']['sheet']}')"),
            ("cron 시간대", f"{cron_tz} ({tz_reason})"),
            ("1회 실행 시간 기준", {COL_DUR_AVG: "평균", COL_DUR_MEDIAN: "중앙값", COL_DUR_MAX: "최댓값"}.get(COL_DURATION, COL_DURATION))]
    header(ws, ["항목", "내용"], row=r)
    for i, (k, v) in enumerate(info, r + 1):
        ws.cell(row=i, column=1, value=k).font = BOLD_FONT
        ws.cell(row=i, column=2, value=v)
        for c in (1, 2):
            ws.cell(row=i, column=c).border = BORDER
            ws.cell(row=i, column=c).alignment = Alignment(vertical="top", wrap_text=True)
    r += len(info) + 3
    for cols, rows in ((("시트", "내용"), GUIDE_SHEETS), (("용어", "설명", "예"), GUIDE_TERMS)):
        header(ws, cols, row=r)
        for i, row in enumerate(rows, r + 1):
            for c, v in enumerate(row, 1):
                cell = ws.cell(row=i, column=c, value=v or None)
                cell.border = BORDER
                cell.alignment = Alignment(vertical="top", wrap_text=True)
            ws.cell(row=i, column=1).font = BOLD_FONT
        r += len(rows) + 3
    autofit(ws, HEAD, HEAD + 1, fixed={"A": 22, "B": 70, "C": 50})
    set_font(ws)


def write_workbook(path, sides, groups, day, day_note, cron_tz, tz_reason, src, M):
    wb = Workbook()
    wb._fonts[0] = Font(name=FONT, size=FONT_SIZE)            # 빈 칸에 새로 입력하는 글자도 맑은 고딕 10
    wb._named_styles["Normal"].font = Font(name=FONT, size=FONT_SIZE)
    s = wb.active
    s.title = "요약"
    write_guide(wb, day, day_note, sides, cron_tz, tz_reason, src)
    last = {side: FIRST + len(sides[side]["jobs"]) - 1 for side in SIDES}
    grp_ws, grp_total = write_group_compare(wb, groups, last)
    write_job_compare(wb, {side: sides[side]["jobs"] for side in SIDES}, groups)
    write_peaks(wb, sides, M)
    stack = wb.create_sheet("종류별 누적 그래프")
    g = len(groups)                                   # 시간대별 시트의 합계 열 위치 (write_timeline과 같은 규칙)
    tl = {side: {"ct": get_column_letter(2 + g), "mt": get_column_letter(2 + 2 * g + 1)} for side in SIDES}
    cmp_ws = write_compare_timeline(wb, tl)
    for side in SIDES:
        cpu_c, mem_c, _, _, label_col = write_timeline(wb, side, groups, sides[side]["per_minute"])
        tl[side].update(cpu_c=cpu_c, mem_c=mem_c, label=label_col)
    for side in SIDES:
        write_jobs(wb, side, sides[side]["jobs"], sides[side]["runs"])

    # ── 요약
    s.sheet_view.showGridLines = False
    widths = (28, 13, 13, 13, 11, 58)
    for c, w in enumerate(widths, 1):
        s.column_dimensions[get_column_letter(c)].width = w
    intro(s, "일일 리소스 사용량 비교 (as-is / to-be)",
          f"기준일 {day.isoformat()} (KST), 원본 {src}. 계산 기준과 용어는 '설명' 시트")
    s.cell(row=4, column=1, value="주요 결과").font = BOLD_FONT
    lines = headline(M, groups)
    per_line = sum(widths) - 2
    for i, line in enumerate(lines, 5):
        s.merge_cells(start_row=i, start_column=1, end_row=i, end_column=6)
        c = s.cell(row=i, column=1, value=f"· {line}")
        c.alignment = Alignment(wrap_text=True, vertical="top")
        s.row_dimensions[i].height = math.ceil(text_width(line) / per_line) * 15 + 3
    cmp = lambda col: f"'시간대별 비교'!${col}${FIRST}:${col}${HEAD + NB}"
    job = lambda side, col: f"SUM('{side} job별'!{col}{FIRST}:{col}{last[side]})"
    r = 5 + len(lines) + 1
    for k, (ca, cb), jc, ac in (("cpu", ("B", "C"), "I", "K"), ("mem", ("E", "F"), "J", "L")):
        u = UNIT[k]
        header(s, [f"{KIND[k]} ({u})", "as-is", "to-be", "변화", "변화율", "설명"], row=r)
        rows = [
            ("평균 사용량", f"={job('as-is', ac)}", f"={job('to-be', ac)}",
             "하루 평균 사용 중인 양. 튜닝 효과 판단 기준"),
            ("최대 사용량", f"=MAX({cmp(ca)})", f"=MAX({cmp(cb)})", "동시 사용량 최댓값. 클러스터 확보 기준 ('최대 사용 시점' 시트)"),
            ("최대 사용 시간대", None, None, "최대 사용량이 발생한 5분 단위 시간대"),
            ("설정값 합계", f"={job('as-is', jc)}", f"={job('to-be', jc)}", "모든 job 설정값의 단순 합계 (참고)"),
        ]
        for i, (label, fa, fb, note) in enumerate(rows, start=r + 1):
            s.cell(row=i, column=1, value=label).font = BOLD_FONT
            if fa is None:
                for col, src_col in ((2, ca), (3, cb)):
                    s.cell(row=i, column=col, value=f"=INDEX({cmp('A')},MATCH({get_column_letter(col)}{r + 2},{cmp(src_col)},0))")
                    s.cell(row=i, column=col).alignment = Alignment(horizontal="right")
            else:
                s.cell(row=i, column=2, value=fa).number_format = "#,##0.0"
                s.cell(row=i, column=3, value=fb).number_format = "#,##0.0"
                s.cell(row=i, column=4, value=f"=C{i}-B{i}").number_format = CHG
                s.cell(row=i, column=5, value=f'=IF(B{i}=0,"",D{i}/B{i})').number_format = CHG_PCT
            s.cell(row=i, column=6, value=note).font = NOTE_FONT
            for c in range(1, 7):
                s.cell(row=i, column=c).border = BORDER
            s.row_dimensions[i].height = 20
        r += len(rows) + 2

    ymax_cpu = nice_max(max(M[x]["cpu"]["peak"] for x in SIDES))
    ymax_mem = nice_max(max(M[x]["mem"]["peak"] for x in SIDES))
    anchor = r + 1
    charts = [compare_line_chart("시간대별 CPU 사용량 (코어, 5분 단위 최댓값)", cmp_ws, (2, 3), 8, "코어", ymax_cpu),
              compare_line_chart("시간대별 메모리 사용량 (GB, 5분 단위 최댓값)", cmp_ws, (5, 6), 8, "GB", ymax_mem),
              compare_bar_chart("종류별 평균 CPU 사용량 (코어)", grp_ws, grp_total - 1, (2, 3), "코어"),
              compare_bar_chart("종류별 평균 메모리 사용량 (GB)", grp_ws, grp_total - 1, (6, 7), "GB")]
    for k, ch in enumerate(charts):
        s.add_chart(ch, f"A{anchor + k * CHART_ROWS}")
    r2 = anchor + len(charts) * CHART_ROWS
    for side in SIDES:
        sk = sides[side]["skipped"]
        if sk:
            s.cell(row=r2, column=1, value=f"{side} 계산 제외 {len(sk)}건 (cron 없음, 실행 간격 추정 불가)").font = BOLD_FONT
            for i, (rn, name, _) in enumerate(sk, start=r2 + 1):
                s.cell(row=i, column=1, value=f"{rn}행 {name}")
            r2 += len(sk) + 2
    set_font(s)

    # ── 종류별 누적: 같은 눈금으로 as-is·to-be 위아래
    stack.sheet_view.showGridLines = False
    intro(stack, "종류별 누적 사용량", "5분 단위 최대 동시 사용량을 job 종류별로 누적. as-is(위)·to-be(아래) 동일 축")
    for i, (k, key, ymax) in enumerate((("cpu", "cpu_c", ymax_cpu), ("mem", "mem_c", ymax_mem))):
        for n, side in enumerate(SIDES):
            ws_side = wb[f"{side} 시간대별"]
            stack.add_chart(area_chart(f"{side} 종류별 {KIND[k]} 사용량 ({UNIT[k]})", ws_side, groups,
                                       tl[side][key], tl[side]["label"], UNIT[k], ymax), f"A{4 + (i * 2 + n) * CHART_ROWS}")
    set_font(stack)
    for ws in (wb["설명"], s, stack):
        ws.page_setup.orientation = "landscape"
        ws.page_setup.fitToWidth, ws.page_setup.fitToHeight = 1, 0
        ws.sheet_properties.pageSetUpPr.fitToPage = True
    wb.active = 0
    wb.calculation.fullCalcOnLoad = True          # 합계·요약 수식을 엑셀이 열 때 계산한다
    wb.save(path)


# ── HTML 보고서 (plotly) ───────────────────────────────────────────────
HTML_CSS = """
:root { --ink:#1F2328; --ink2:#4B5563; --muted:#6B7280; --line:#E5E7EB; --grid:#EEF0F3; --surface:#FFFFFF;
        --page:#F3F5F8; --head:#F3F5F8; --asis:#9AA0A6; --tobe:#2A78D6; --good:#17804D; --good-bg:#E7F5ED;
        --bad:#C0392B; --bad-bg:#FCEBEA; --band:#16213A; }
* { box-sizing:border-box; }
html { font-size:10pt; }
body { margin:0; background:var(--page); color:var(--ink); font:10pt/1.6 '맑은 고딕','Malgun Gothic',sans-serif;
       -webkit-font-smoothing:antialiased; }
.band { background:var(--band); color:#fff; }
.band .in { max-width:1200px; margin:0 auto; padding:22px 16px 18px; }
.band h1 { font-size:10pt; font-weight:700; margin:0; letter-spacing:.2px; }
.band .meta { display:flex; flex-wrap:wrap; gap:6px 18px; margin-top:8px; color:#C3CBDA; }
.band .meta b { color:#fff; font-weight:700; }
main { max-width:1200px; margin:0 auto; padding:0 16px 56px; }
h2, h3 { font-size:10pt; margin:0; font-weight:700; }
h2 .no { color:var(--tobe); margin-right:6px; }
p { margin:0; }
.card { background:var(--surface); border:1px solid var(--line); border-radius:10px; padding:18px 20px; margin-top:16px; }
.sub { color:var(--muted); margin:2px 0 10px; }
ul.summary { margin:8px 0 0; padding-left:16px; }
ul.summary li { margin:3px 0; }
ul.summary li::marker { color:var(--tobe); }
.kpis { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin-top:16px; }
.kpi { background:var(--surface); border:1px solid var(--line); border-radius:10px; padding:14px 16px; }
.kpi .top { display:flex; justify-content:space-between; align-items:center; color:var(--ink2); }
.kpi .value { font-weight:700; margin-top:6px; }
.kpi .def { color:var(--muted); }
.cmp { margin-top:10px; display:grid; grid-template-columns:36px 1fr 64px; gap:4px 8px; align-items:center; color:var(--ink2); }
.cmp .trk { height:6px; background:var(--grid); border-radius:3px; overflow:hidden; }
.cmp .trk i { display:block; height:100%; border-radius:3px; }
.cmp .v { text-align:right; font-variant-numeric:tabular-nums; }
.badge { display:inline-block; border-radius:5px; padding:0 6px; font-weight:700; white-space:nowrap; }
.badge.down { color:var(--good); background:var(--good-bg); }
.badge.up { color:var(--bad); background:var(--bad-bg); }
.badge.flat { color:var(--muted); background:var(--head); }
.toolbar { position:sticky; top:0; z-index:10; display:flex; align-items:center; gap:10px; margin-top:16px;
           padding:10px 0; background:var(--page); }
.seg { display:inline-flex; background:var(--surface); border:1px solid var(--line); border-radius:8px; padding:2px; }
.seg button, .btn { font:inherit; color:var(--ink2); background:transparent; border:0; border-radius:6px; padding:3px 14px; cursor:pointer; }
.seg button[aria-pressed="true"] { background:var(--tobe); color:#fff; font-weight:700; }
.btn { margin-left:auto; background:var(--surface); border:1px solid var(--line); }
.sw { display:inline-block; width:10px; height:10px; border-radius:2px; vertical-align:-1px; margin-right:6px; }
body[data-metric="cpu"] .m-mem, body[data-metric="mem"] .m-cpu { display:none; }
.two { display:grid; grid-template-columns:1fr 1fr; gap:20px; }
.peak h3 { margin-bottom:6px; }
table { width:100%; border-collapse:collapse; }
th { background:var(--head); color:var(--ink2); font-weight:700; text-align:left; padding:7px 8px; border-bottom:1px solid var(--line);
     white-space:nowrap; }
th.num, td.num { text-align:right; }
th[data-sort] { cursor:pointer; user-select:none; }
th[data-sort]::after { content:" ↕"; color:#B6BCC6; }
th.asc::after { content:" ↑"; color:var(--ink2); } th.desc::after { content:" ↓"; color:var(--ink2); }
td { padding:7px 8px; border-bottom:1px solid var(--grid); vertical-align:top; }
td.num { font-variant-numeric:tabular-nums; white-space:nowrap; }
tr.total td { font-weight:700; border-top:1px solid var(--line); border-bottom:0; }
tbody tr:hover td { background:#FAFBFC; }
.desc { color:var(--muted); }
.arrow { color:var(--muted); padding:0 4px; }
.chg { display:flex; align-items:center; justify-content:flex-end; gap:8px; }
.bar { width:70px; height:6px; background:var(--grid); border-radius:3px; position:relative; flex:none; }
.bar i { position:absolute; top:0; bottom:0; border-radius:3px; }
.bar i.down { right:50%; background:var(--good); } .bar i.up { left:50%; background:var(--bad); }
.tablebar { display:flex; align-items:center; gap:10px; margin-bottom:10px; flex-wrap:wrap; }
select { font:inherit; color:var(--ink); border:1px solid var(--line); border-radius:6px; padding:2px 8px; background:var(--surface); }
.scroll { overflow-x:auto; }
dl.terms { display:grid; grid-template-columns:160px 1fr; gap:8px 16px; margin:10px 0 0; }
dl.terms dt { font-weight:700; } dl.terms dd { margin:0; color:var(--ink2); }
dl.terms dd span { color:var(--muted); }
@media (max-width:900px) { .kpis { grid-template-columns:repeat(2,minmax(0,1fr)); } .two { grid-template-columns:1fr; }
                           dl.terms { grid-template-columns:1fr; } }
@media print { .toolbar { display:none; } body { background:#fff; } .card, .kpi { break-inside:avoid; }
               .band { -webkit-print-color-adjust:exact; print-color-adjust:exact; } }
"""
PX10 = 13.33                                      # 10pt = 13.33px
PLOT_FONT = dict(family="맑은 고딕, Malgun Gothic, sans-serif", size=PX10, color="#1F2328")
MUTED_FONT = dict(family=PLOT_FONT["family"], size=PX10, color="#6B7280")
PLOT_CONFIG = {"displaylogo": False, "responsive": True,
               "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d", "zoomIn2d", "zoomOut2d"],
               "toImageButtonOptions": {"format": "png", "scale": 2}}   # 카메라 버튼 = PNG 저장
SMOOTH = 0.5                                      # 선 곡선 정도 (0 = 꺾은선, 1.3 = 최대). 표시 값은 그대로


def write_html(path, sides, groups, day, day_note, cron_tz, tz_reason, src, M):
    """같은 결과를 브라우저용 HTML 한 장으로. plotly가 없으면 건너뛴다 (엑셀 결과는 그대로 나온다)."""
    try:
        import plotly.graph_objects as go
        import plotly.offline
        from plotly.subplots import make_subplots
    except ImportError:
        print("HTML 보고서 생략 (pip install plotly 필요)")
        return None
    import html as h
    import json

    esc = h.escape
    xs = [range_label(b) for b in range(NB)]                     # x = 5분 단위 시간대 이름
    ticks = [range_label(b) for b in range(0, NB, LABEL_EVERY_MIN // BUCKET_MIN)]
    tick_text = [hhmm(b * BUCKET_MIN) for b in range(0, NB, LABEL_EVERY_MIN // BUCKET_MIN)]
    col = {"as-is": "#9AA0A6", "to-be": "#2A78D6"}
    figs = {}

    def base(fig, height, **kw):
        fig.update_layout(font=PLOT_FONT, height=height, margin=dict(l=8, r=16, t=36, b=8), paper_bgcolor="white",
                          plot_bgcolor="white", hoverlabel=dict(font=PLOT_FONT, bgcolor="white", bordercolor="#E5E7EB"),
                          legend=dict(orientation="h", yanchor="bottom", y=1.0, x=0, font=PLOT_FONT, itemclick="toggle"))
        fig.update_layout(**kw)
        fig.update_yaxes(gridcolor="#EEF0F3", zeroline=False, tickformat=",", tickfont=MUTED_FONT, automargin=True,
                         rangemode="tozero", showline=False, ticks="")
        fig.update_xaxes(tickfont=MUTED_FONT, automargin=True, showgrid=False, ticks="")
        return fig

    def time_x(fig, **kw):
        fig.update_xaxes(type="category", tickmode="array", tickvals=ticks, ticktext=tick_text, showline=True,
                         linecolor="#E5E7EB", showspikes=True, spikemode="across", spikesnap="cursor",
                         spikethickness=1, spikecolor="#9AA0A6", spikedash="solid", **kw)

    for k in ("cpu", "mem"):
        u = UNIT[k]
        pa, pb = M["as-is"][k], M["to-be"][k]
        # 1) 시간대별: as-is 회색 선, to-be 파랑 선, 둘 사이 음영 = 감소분
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=xs, y=pa["series"], name="as-is", mode="lines",
                                 line=dict(color=col["as-is"], width=2, shape="spline", smoothing=SMOOTH),
                                 hovertemplate=f"%{{y:,.1f}} {u}<extra>as-is</extra>"))
        fig.add_trace(go.Scatter(x=xs, y=pb["series"], name="to-be", mode="lines", fill="tonexty",
                                 fillcolor="rgba(42,120,214,0.12)",
                                 line=dict(color=col["to-be"], width=2, shape="spline", smoothing=SMOOTH),
                                 hovertemplate=f"%{{y:,.1f}} {u}<extra>to-be</extra>"))
        top = nice_max(max(pa["peak"], pb["peak"]) * 1.15)
        if abs(pa["peak_bucket"] - pb["peak_bucket"]) <= 12:          # 1시간 안이면 표시 하나로
            marks = [(pa["peak_bucket"], max(pa["peak"], pb["peak"]), f"최대 {pa['peak']:,.0f} → {pb['peak']:,.0f}{u}", 40)]
        else:                                                            # 떨어져 있으면 좌우로 나눠 겹치지 않게
            left, right = sorted([("as-is", pa), ("to-be", pb)], key=lambda x: x[1]["peak_bucket"])
            marks = [(m["peak_bucket"], m["peak"], f"{s} 최대 {m['peak']:,.0f}{u}", ax)
                     for (s, m), ax in ((left, -50), (right, 50))]
        for b, y, text, ax in marks:
            fig.add_annotation(x=xs[b], y=y, text=text, showarrow=True, arrowhead=0, arrowwidth=1, arrowcolor="#9AA0A6",
                               ax=ax, ay=-26, font=PLOT_FONT, bgcolor="white", bordercolor="#E5E7EB", borderpad=3)
        base(fig, 380, hovermode="x unified", legend_traceorder="normal")
        time_x(fig)
        fig.update_yaxes(range=[0, top], ticksuffix=f" {u}")
        figs[f"line-{k}"] = fig

        # 2) 종류별 평균 사용량 (가로 막대) + 변화율
        fig = go.Figure()
        for side in SIDES:
            fig.add_trace(go.Bar(y=groups, x=[M[side][k]["group_avg"].get(g, 0) for g in groups], name=side,
                                 orientation="h", marker=dict(color=col[side], line=dict(width=0), cornerradius=3),
                                 hovertemplate=f"%{{x:,.2f}} {u}<extra>{side}</extra>"))
        vmax = max(max(M[side][k]["group_avg"].values() or [0]) for side in SIDES) or 1
        for g in groups:
            a, b = M["as-is"][k]["group_avg"].get(g, 0), M["to-be"][k]["group_avg"].get(g, 0)
            if abs(b - a) < 1e-9:
                continue
            c = "#17804D" if b < a else "#C0392B"
            fig.add_annotation(x=max(a, b), y=g, text=f"<b>{change_text(a, b)}</b>", showarrow=False, xanchor="left",
                               xshift=8, font=dict(family=PLOT_FONT["family"], size=PX10, color=c))
        base(fig, 90 + 46 * len(groups), barmode="group", bargap=0.32, bargroupgap=0.08, hovermode="y unified")
        fig.update_yaxes(autorange="reversed", ticksuffix="  ", tickfont=PLOT_FONT, gridcolor="rgba(0,0,0,0)")
        fig.update_xaxes(range=[0, vmax * 1.22], showgrid=True, gridcolor="#EEF0F3", tickformat=",", ticksuffix=f" {u}")
        figs[f"bar-{k}"] = fig

        # 3) 종류별 누적 — as-is 위, to-be 아래, 같은 세로축
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.12, subplot_titles=["as-is", "to-be"])
        for n, side in enumerate(SIDES, 1):
            for g in groups:
                fig.add_trace(go.Scatter(x=xs, y=M[side][k]["by_group"][g], name=g, legendgroup=g, showlegend=n == 1,
                                         stackgroup=side, mode="lines", line=dict(width=0.6, color="white"),
                                         fillcolor="#" + group_color(groups, g),
                                         hovertemplate=f"%{{y:,.1f}} {u}<extra>{esc(g)}</extra>"), row=n, col=1)
        base(fig, 640, hovermode="x unified", margin=dict(l=8, r=16, t=64, b=8))
        fig.update_layout(legend=dict(y=1.06))
        fig.update_annotations(font=PLOT_FONT, yshift=2)
        time_x(fig)
        fig.update_xaxes(showticklabels=True)
        fig.update_yaxes(range=[0, nice_max(max(pa["peak"], pb["peak"]) * 1.05)], ticksuffix=f" {u}")
        figs[f"stack-{k}"] = fig

    # 지표 카드
    def badge(a, b):
        if abs(b - a) < 1e-9:
            return '<span class="badge flat">변화 없음</span>'
        if not a:
            return '<span class="badge up">신규</span>'
        return (f'<span class="badge {"down" if b < a else "up"}">{"▼" if b < a else "▲"} '
                f'{abs(b - a) / a * 100:.1f}%</span>')

    def kpi(k, key, label, d):
        a, b, u = M["as-is"][k][key], M["to-be"][k][key], UNIT[k]
        top = max(a, b) or 1
        bars = "".join(f'<span>{s}</span><span class="trk"><i style="width:{v / top * 100:.1f}%;background:{col[s]}"></i></span>'
                       f'<span class="v">{v:,.1f}</span>' for s, v in (("as-is", a), ("to-be", b)))
        return (f'<div class="kpi"><div class="top"><span>{label} ({u})</span>{badge(a, b)}</div>'
                f'<div class="value">{b:,.1f} {u}</div><div class="def">{d}</div><div class="cmp">{bars}</div></div>')

    kpis = (kpi("cpu", "avg", "평균 CPU 사용량", "하루 평균 사용 중인 코어 수") +
            kpi("cpu", "peak", "최대 CPU 사용량", "동시 사용 최댓값") +
            kpi("mem", "avg", "평균 메모리 사용량", "하루 평균 사용 중인 메모리") +
            kpi("mem", "peak", "최대 메모리 사용량", "동시 사용 최댓값"))

    # 최대 사용 시점
    def peak_table(side, k):
        m = M[side][k]
        rows = "".join(
            f'<tr><td><span class="sw" style="background:#{group_color(groups, j["group"])}"></span>{esc(j["name"])}'
            f'<div class="desc">{esc(j["group"])}{", " + esc(j["desc"]) if j["desc"] else ""}</div></td>'
            f'<td class="num">{j["cpu"]:,.1f}</td><td class="num">{j["mem"]:,.1f}</td></tr>' for j in m["peak_jobs"])
        tot_c = sum(j["cpu"] for j in m["peak_jobs"])
        tot_m = sum(j["mem"] for j in m["peak_jobs"])
        return (f'<div class="peak"><h3>{side}  {hhmm(m["peak_time"])} ({range_label(m["peak_bucket"])}), '
                f'job {len(m["peak_jobs"])}개</h3><div class="scroll"><table><thead><tr><th>app name</th>'
                f'<th class="num">CPU (코어)</th><th class="num">메모리 (GB)</th></tr></thead><tbody>{rows}'
                f'<tr class="total"><td>합계</td><td class="num">{tot_c:,.1f}</td><td class="num">{tot_m:,.1f}</td></tr>'
                f'</tbody></table></div></div>')

    peaks = "".join(f'<div class="m-{k}"><div class="two">{peak_table("as-is", k)}{peak_table("to-be", k)}</div></div>'
                    for k in ("cpu", "mem"))

    # job별 비교 표
    keys, pos = job_pairs({side: sides[side]["jobs"] for side in SIDES}, groups)
    maxd = {k: 0.0 for k in ("cpu", "mem")}
    rows_data = []
    for key in keys:
        v = {}
        for side in SIDES:
            i = pos[key].get(side)
            if i is not None:
                j, n = sides[side]["jobs"][i], M[side]["n_runs"][i]
                v[side] = {"dur": j["duration"], "cpu": j["cpu"], "mem": j["mem"],
                           "acpu": job_avg(n, j, "cpu"), "amem": job_avg(n, j, "mem")}
        d = {k: v.get("to-be", {}).get("a" + k, 0) - v.get("as-is", {}).get("a" + k, 0) for k in ("cpu", "mem")}
        for k in d:
            maxd[k] = max(maxd[k], abs(d[k]))
        rows_data.append((key, v, d))

    def pair(v, f, fmt="{:,.1f}"):
        a = fmt.format(v["as-is"][f]) if "as-is" in v else "–"
        b = fmt.format(v["to-be"][f]) if "to-be" in v else "–"
        if "as-is" in v and "to-be" in v and abs(v["as-is"][f] - v["to-be"][f]) < 1e-9:
            return f'<span class="desc">{a}</span>'
        return f'{a}<span class="arrow">→</span><b>{b}</b>'

    def chg(x, k):
        if abs(x) < 1e-9:
            return '<div class="chg"><span class="desc">–</span><span class="bar"></span></div>'
        w = abs(x) / maxd[k] * 50 if maxd[k] else 0
        return (f'<div class="chg">{x:+,.2f}<span class="bar"><i class="{"down" if x < 0 else "up"}" '
                f'style="width:{w:.1f}%"></i></span></div>')

    trs = []
    for (g, name), v, d in rows_data:
        desc = pos[(g, name)]["desc"]
        only = "to-be에만 있음" if "as-is" not in v else ("as-is에만 있음" if "to-be" not in v else "")
        sub = ", ".join(x for x in (desc, only) if x)
        trs.append(
            f'<tr data-group="{esc(g)}"><td data-v="{groups.index(g)}"><span class="sw" style="background:#{group_color(groups, g)}">'
            f'</span>{esc(g)}</td><td data-v="{esc(name)}">{esc(name)}<div class="desc">{esc(sub)}</div></td>'
            f'<td class="num">{pair(v, "dur")}</td><td class="num">{pair(v, "cpu", "{:,.0f}")}</td>'
            f'<td class="num">{pair(v, "mem", "{:,.0f}")}</td>'
            + "".join(f'<td class="num m-{k}">{pair(v, "a" + k, "{:,.2f}")}</td>'
                      f'<td class="num m-{k}" data-v="{d[k]:.6f}">{chg(d[k], k)}</td>' for k in ("cpu", "mem")) + "</tr>")
    options = "".join(f'<option value="{esc(g)}">{esc(g)}</option>' for g in groups)
    job_table = (
        f'<div class="tablebar"><label>종류 <select id="gsel"><option value="">전체</option>{options}</select></label>'
        f'<span class="desc">값: as-is → <b>to-be</b></span></div>'
        f'<div class="scroll"><table id="jobs"><thead><tr><th data-sort="n">종류</th><th data-sort="t">app name</th>'
        f'<th class="num">1회 실행 시간 (분)</th><th class="num">CPU (코어)</th><th class="num">메모리 (GB)</th>'
        f'<th class="num m-cpu">평균 CPU 사용량 (코어)</th><th class="num m-cpu" data-sort="n">변화 (코어)</th>'
        f'<th class="num m-mem">평균 메모리 사용량 (GB)</th><th class="num m-mem" data-sort="n">변화 (GB)</th>'
        f'</tr></thead><tbody>{"".join(trs)}</tbody></table></div>')

    terms = "".join(f"<dt>{esc(t)}</dt><dd>{esc(d)}" + (f"<br><span>예: {esc(e)}</span>" if e else "") + "</dd>"
                    for t, d, e in GUIDE_TERMS)
    dur = {COL_DUR_AVG: "평균", COL_DUR_MEDIAN: "중앙값", COL_DUR_MAX: "최댓값"}.get(COL_DURATION, COL_DURATION)
    fig_json = json.dumps({k: json.loads(f.to_json()) for k, f in figs.items()}, ensure_ascii=False)

    def fig_div(name):
        return "".join(f'<div class="m-{k}"><div id="{name}-{k}"></div></div>' for k in ("cpu", "mem"))

    page = f"""<!doctype html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>일일 리소스 사용량 비교</title>
<style>{HTML_CSS}</style><script>{plotly.offline.get_plotlyjs()}</script></head>
<body data-metric="cpu">
<div class="band"><div class="in"><h1>일일 리소스 사용량 비교 (as-is / to-be)</h1>
<div class="meta"><span>기준일 <b>{day.isoformat()}</b> ({esc(day_note)})</span><span>원본 <b>{esc(src)}</b></span>
<span>as-is <b>{esc(sides['as-is']['sheet'])}</b> / to-be <b>{esc(sides['to-be']['sheet'])}</b></span>
<span>cron 시간대 <b>{cron_tz}</b></span><span>1회 실행 시간 <b>{dur}</b></span></div></div></div>
<main>
<div class="kpis">{kpis}</div>
<section class="card"><h2><span class="no">1</span>주요 결과</h2><ul class="summary">{"".join(f"<li>{esc(x)}</li>" for x in headline(M, groups))}</ul></section>

<div class="toolbar"><div class="seg" role="group"><button data-m="cpu" aria-pressed="true">CPU</button>
<button data-m="mem" aria-pressed="false">메모리</button></div><button class="btn" onclick="window.print()">인쇄 / PDF</button></div>

<section class="card"><h2><span class="no">2</span>시간대별 사용량</h2>
<p class="sub">5분 단위 최대 동시 사용량. 음영은 as-is와 to-be의 차이</p>{fig_div("line")}</section>

<section class="card"><h2><span class="no">3</span>종류별 평균 사용량</h2>
<p class="sub">job 종류별 하루 평균 사용량. 우측 수치는 as-is 대비 변화율</p>{fig_div("bar")}</section>

<section class="card"><h2><span class="no">4</span>종류별 누적 사용량</h2>
<p class="sub">5분 단위 최대 동시 사용량을 job 종류별로 누적. as-is·to-be 동일 축</p>{fig_div("stack")}</section>

<section class="card"><h2><span class="no">5</span>최대 사용 시점의 실행 job</h2>
<p class="sub">최대 사용 시점에 실행 중인 job과 사용량. 합계 = 최대 사용량</p>{peaks}</section>

<section class="card"><h2><span class="no">6</span>job별 비교</h2>
<p class="sub">종류 + app name 기준 매칭. 변화 = to-be − as-is</p>{job_table}</section>

<section class="card"><h2><span class="no">7</span>용어</h2><dl class="terms">{terms}</dl></section>
</main>
<script>
const FIGS = {fig_json};
const CONFIG = {json.dumps(PLOT_CONFIG)};
for (const [id, f] of Object.entries(FIGS)) Plotly.newPlot(id, f.data, f.layout, CONFIG);
const resizeAll = () => document.querySelectorAll('.js-plotly-plot').forEach(p => Plotly.Plots.resize(p));
function setMetric(m) {{
  document.body.dataset.metric = m;
  document.querySelectorAll('.seg button').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.m === m)));
  document.querySelectorAll('.m-' + m + ' .js-plotly-plot').forEach(p => Plotly.Plots.resize(p));
  try {{ localStorage.setItem('rt-metric', m); }} catch (e) {{}}
}}
document.querySelectorAll('.seg button').forEach(b => b.addEventListener('click', () => setMetric(b.dataset.m)));
try {{ const m = localStorage.getItem('rt-metric'); if (m === 'mem') setMetric(m); }} catch (e) {{}}
let shown = 'cpu';                                  // 인쇄할 때는 CPU·메모리 둘 다
window.addEventListener('beforeprint', () => {{ shown = document.body.dataset.metric; document.body.dataset.metric = 'all'; resizeAll(); }});
window.addEventListener('afterprint', () => setMetric(shown));
const tbody = document.querySelector('#jobs tbody');
document.getElementById('gsel').addEventListener('change', e => {{
  tbody.querySelectorAll('tr').forEach(tr => tr.hidden = !!e.target.value && tr.dataset.group !== e.target.value);
}});
document.querySelectorAll('#jobs th[data-sort]').forEach(th => th.addEventListener('click', () => {{
  const idx = [...th.parentNode.children].indexOf(th), asc = !th.classList.contains('asc');
  document.querySelectorAll('#jobs th').forEach(x => x.classList.remove('asc', 'desc'));
  th.classList.add(asc ? 'asc' : 'desc');
  const val = tr => {{ const td = tr.children[idx], v = td.dataset.v ?? td.textContent;
                      return th.dataset.sort === 'n' ? parseFloat(v) : v; }};
  [...tbody.rows].sort((a, b) => {{ const x = val(a), y = val(b); return (x > y ? 1 : x < y ? -1 : 0) * (asc ? 1 : -1); }})
    .forEach(tr => tbody.appendChild(tr));
}}));
</script></body></html>"""
    path.write_text(page, encoding="utf-8")
    return path


def open_workbook(src):
    """xlsx는 zip 파일이다. zip이면 openpyxl로 직접 읽는다.
    zip이 아니면(사내 DRM·열기 암호·옛 xls·CSV 등) Windows에서는 무조건 엑셀을 통해 읽는다 — 엑셀은 이 파일들을 다 연다.
    DRM 파일은 맨 앞이 글자로 시작하기도 해서(보안 제품 표식) 첫 바이트만으로 종류를 단정하지 않는다."""
    if not src.exists():
        raise SystemExit(f"파일 없음: {src}")
    if src.name.startswith("~$"):
        raise SystemExit(f"'{src.name}'은 엑셀 잠금 파일. '~$'가 없는 원래 파일명으로 실행")
    head = src.read_bytes()[:16]
    if head[:2] == b"PK":
        return load_workbook(src, data_only=True)     # 수식 셀은 계산된 값으로 읽는다
    if sys.platform == "win32":
        return read_via_excel(src)
    shown = head.decode("latin-1").encode("unicode_escape").decode("ascii")
    raise SystemExit(f"'{src.name}'은 xlsx 형식이 아님 (파일 앞부분: {shown}). DRM·열기 암호·xls·CSV 파일은 "
                     f"Windows용 Python에서 실행 (pip install openpyxl xlwings plotly). WSL·리눅스에서는 불가")


def read_via_excel(src):
    """엑셀을 화면에 띄우지 않고 실행해 파일을 읽기 전용으로 열고, as-is·to-be 시트의 셀 값만 받아 온다.
    엑셀은 보안(DRM) 프로그램이 풀어 주므로 Python이 파일을 직접 건드리지 않아도 된다.
    받아 온 값은 메모리 안의 새 통합 문서에 같은 셀 위치로 옮긴다 → 이후 계산은 일반 xlsx와 똑같다."""
    try:
        import xlwings as xw
    except ImportError:
        raise SystemExit("DRM 파일은 엑셀을 통해 읽음. pip install xlwings 후 다시 실행")
    print(f"'{src.name}': xlsx 형식이 아니어서 엑셀을 통해 읽음")
    app = xw.App(visible=False, add_book=False)       # 새 엑셀 프로세스 — 사용자가 열어 둔 엑셀 창과 별개
    app.display_alerts = False
    try:
        book = app.books.open(str(src.resolve()), read_only=True, update_links=False)
        names = find_sheets([sh.name for sh in book.sheets])
        wb = Workbook()
        wb.remove(wb.active)
        for name in dict.fromkeys(names.values()):
            sh, ws = book.sheets[name], wb.create_sheet(name)
            used = sh.used_range
            values = used.options(ndim=2).value          # 수식 셀은 엑셀이 계산한 값
            for i, row in enumerate(values):
                for j, v in enumerate(row):
                    if v is not None:
                        ws.cell(row=used.row + i, column=used.column + j, value=v)
            copy_merged(sh, used, len(values), len(values[0]) if values else 0, ws)
        book.close()
    finally:
        app.quit()
    return wb


def copy_merged(sh, used, nrows, ncols, ws):
    """병합 셀은 엑셀이 왼쪽 위 칸에만 값을 준다 → 병합 범위의 모든 칸에 그 값을 채운다 (A열 job_type 병합 등).
    열마다 병합 여부를 한 번 묻고(False = 병합 없음), 병합이 있는 열만 칸 단위로 병합 범위를 확인한다."""
    done = set()
    for c in range(used.column, used.column + ncols):
        col = sh.range((used.row, c), (used.row + nrows - 1, c))
        if col.merge_cells is False:
            continue
        for r in range(used.row, used.row + nrows):
            if (r, c) in done:
                continue
            cell = sh.range((r, c))
            if not cell.merge_cells:
                continue
            area = cell.merge_area
            v = ws.cell(row=area.row, column=area.column).value
            for rr in range(area.row, area.row + area.shape[0]):
                for cc in range(area.column, area.column + area.shape[1]):
                    done.add((rr, cc))
                    ws.cell(row=rr, column=cc, value=v)


def find_sheets(sheetnames):
    """이름이 정확히 as-is / to-be인 시트(대소문자·기호 무시)를 먼저 찾는다.
    없을 때만 이름에 들어 있는 시트를 쓴다 — 'AS-IS(x)' 같은 시트가 앞에 있어도 'AS-IS'를 고른다."""
    key = lambda n: re.sub(r"[^a-z]", "", n.lower())

    def pick(word):
        exact = [n for n in sheetnames if key(n) == word]
        return exact[0] if exact else next((n for n in sheetnames if word in key(n)), None)
    asis = SHEET_ASIS or pick("asis")
    tobe = SHEET_TOBE or pick("tobe")
    for label, name in (("as-is", asis), ("to-be", tobe)):
        if not name or name not in sheetnames:
            raise SystemExit(f"{label} 시트 없음 (시트: {', '.join(sheetnames)}). "
                             f"스크립트 상단 SHEET_ASIS·SHEET_TOBE에 시트 이름 지정")
    return {"as-is": asis, "to-be": tobe}


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    src = Path(sys.argv[1])
    wb = open_workbook(src)
    day_arg = datetime.strptime(sys.argv[2], "%Y%m%d").date() if len(sys.argv) > 2 else None
    names = find_sheets(wb.sheetnames)
    sides = {}
    for side in SIDES:
        jobs, skipped = read_jobs(wb[names[side]])
        guessed = sum(1 for j in jobs if j["every"])
        print(f"{side}: 시트 '{names[side]}' job {len(jobs)}개, 제외 {len(skipped)}개"
              + (f", cron 없어 간격 추정 {guessed}개" if guessed else ""))
        sides[side] = {"sheet": names[side], "jobs": jobs, "skipped": skipped}
    cron_tz, reason = detect_cron_tz(sides["as-is"]["jobs"] + sides["to-be"]["jobs"])
    all_jobs = sides["as-is"]["jobs"] + sides["to-be"]["jobs"]
    if day_arg:
        day = day_arg
        missing = sorted({j["name"] for j in all_jobs if not runs_on(j, day, cron_tz)})
        day_note = "지정일" + (f", 미실행 job {len(missing)}개" if missing else "")
    else:
        day, ok = pick_day(all_jobs, cron_tz, datetime.now(KST).date())
        day_note = "전체 job 실행일, 자동 선택" if ok else "오늘, 31일 내 전체 job 실행일 없음"
    print(f"cron 시간대: {cron_tz} ({reason}), 기준일 {day} ({day_note})")
    groups = order_groups(all_jobs)
    for side in SIDES:
        per_minute, runs, spans = simulate(sides[side]["jobs"], day, cron_tz, groups)
        sides[side].update(groups=groups, per_minute=per_minute, runs=runs, spans=spans)
    M = {side: side_metrics(sides[side]) for side in SIDES}
    out = src.with_name(f"{src.stem}_resource_diff.xlsx")
    write_workbook(out, sides, groups, day, day_note, cron_tz, reason, src.name, M)
    html_out = write_html(src.with_name(f"{src.stem}_resource_diff.html"), sides, groups, day, day_note, cron_tz, reason,
                          src.name, M)
    for line in headline(M, groups):
        print("· " + line)
    print(f"→ {out}" + (f"\n→ {html_out}" if html_out else ""))


if __name__ == "__main__":
    main()
```
