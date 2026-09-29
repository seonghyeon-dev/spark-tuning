# 하루 리소스 사용량 시각화 — as-is / to-be 비교 (엑셀 + HTML)

> **결론**: 작업 엑셀의 **as-is 시트(튜닝 전)와 to-be 시트(튜닝 후)**를 넣으면, 두 설정으로 하루를 돌렸을 때 CPU·메모리를 **언제, 얼마나** 쓰는지 비교한 엑셀과 HTML이 나온다. 예시 데이터(hourly Compaction만 튜닝)에서 하루 CPU 사용량은 710 → 561 코어·시간(**−21%**)이고 줄어든 양은 전부 comp_range(294 → 145, −51%)에서 나왔다. 가장 붐빌 때 동시 CPU는 147 → 132코어(−10%)로 덜 줄어드는데, 그 순간 CPU의 62%를 설정이 그대로인 append·comp_daily가 쓰고 있기 때문이다.
>
> 원본 엑셀은 건드리지 않는다. 결과는 같은 폴더에 `<원본이름>_리소스비교.xlsx`(엑셀)와 `<원본이름>_리소스비교.html`(브라우저용)로 생긴다. 글자는 전부 **맑은 고딕 10**. 두 파일 모두 맨 앞에 **읽는 법·용어 설명**이 있다.

- 앞 단계: [Airflow job 실행 시간 집계](airflow-job-duration.md) — Duration·Start Offset을 뽑는 스크립트
- 필요: Python 3.9 이상, `pip install openpyxl plotly` (plotly는 HTML 보고서용. 없으면 엑셀만 만든다)
- **사내 보안(DRM)이 걸린 작업 엑셀**: **Windows용 Python**에서 실행하고 `pip install xlwings`를 추가한다 → 스크립트가 엑셀을 통해 값을 읽는다 (§2.3). WSL·리눅스 Python은 Windows 엑셀을 조종할 수 없어 안 된다

---

## 1. 입력 — 작업 엑셀의 모양

작업 엑셀 하나에 **같은 규격의 시트 2개**가 있다.

| 시트 | 뜻 | 스크립트가 찾는 방법 |
|---|---|---|
| **AS-IS** | 튜닝 전 설정·실행 시간 | 이름이 정확히 as-is인 시트(대소문자·기호 무시: `AS-IS`, `asis`, `as_is`) |
| **TO-BE** | 튜닝 후 설정·실행 시간 | 이름이 정확히 to-be인 시트(`TO-BE`, `tobe` …) |

- 정확히 맞는 시트가 없을 때만 이름에 들어 있는 시트를 쓴다 → `AS-IS(x)`·`DIFF` 같은 다른 시트가 있어도 `AS-IS`·`TO-BE`를 고른다
- 이름이 다르면 스크립트 상단 `SHEET_ASIS`·`SHEET_TOBE`에 시트 이름을 넣는다

시트 한 행 = job 하나. 두 시트 모두 아래 열 위치로 읽는다.

| 열 | 내용 | 스크립트가 쓰나 |
|---|---|---|
| **A** | **job_type** (`append`, `summary`, `comp_range`, `comp_daily`, `exp_snap`, `del_orphan`, `rw_mani` …) | ✅ 결과의 종류. 순서·색 = AS-IS 시트에 처음 나온 순서 |
| **B** | **cron** | ✅ trigger로만 도는 테이블은 **부모 DAG의 cron**을 적는다 (§3.4). 비어 있거나 cron 식이 아니면 그 행만 실행 간격을 추정 |
| **C** | **app name** | ✅ 결과 표의 이름. **as-is·to-be 행을 짝짓는 키**(종류 + app name) |
| D~J | 드라이버 cpu, 드라이버 메모리, 드라이버 메모리 오버헤드, 익스큐터 cpu, 익스큐터 메모리, 익스큐터 메모리 오버헤드, 익스큐터 개수 | ❌ (K·L을 만드는 재료) |
| **K** | **토탈 cpu** (코어) | ✅ |
| **L** | **토탈 메모리** (GB. `228g`처럼 단위가 붙어 있어도 읽는다) | ✅ |
| M~S | `job_durations.csv`의 **F~L열**을 그대로 붙인 것: runs, Duration (min), Duration median (min), Duration max (min), **Start Offset (min)**, oldest_run, latest_run | ✅ |
| **T** | **기능 요약** | ✅ 결과 표에 같이 보여 준다 |

- **병합된 칸은 병합 범위의 모든 행에 같은 값을 채운다** — 같은 종류 행끼리 병합한 A열(job_type), 같은 설명끼리 병합한 T열(기능 요약) 모두 그대로 두면 된다. A열이 빈 행(병합도 아님)만 `기타`가 된다
- K·L이 수식이어도 된다. 엑셀에 **저장된 계산값**을 읽는다 (DRM 파일을 엑셀을 통해 읽을 때는 엑셀이 계산한 값)
- 제목 행·빈 행·메모 행은 알아서 건너뛴다. **N열(Duration)과 K열(토탈 cpu)이 둘 다 숫자인 행만** job으로 본다

### 1.1 M~S가 비어 있는 행 — 안 돌린 job·삭제한 job

Duration(N열)이 비어 있는 행은 **그 시트의 계산에서 빠진다.**

| 경우 | 처리 | 비교 결과 |
|---|---|---|
| 의도적으로 안 돌림 (AS-IS·TO-BE 둘 다 빈칸) | 두 시트 모두에서 빠짐 | 어디에도 안 나옴 → 전후 차이 0 |
| TO-BE에서 삭제 (AS-IS에만 값) | AS-IS에만 들어감 | `job별 비교`에 TO-BE 칸이 비고, **AS-IS 사용량 전부가 감소**로 잡힌다. 예: comp_daily 한 테이블(하루 1회 × 12.5분 × 50코어 ÷ 60 = 10.4코어·시간)을 TO-BE에서 비우면 변화 −10.4 |
| TO-BE에서 새로 생김 (TO-BE에만 값) | TO-BE에만 들어감 | TO-BE 사용량 전부가 증가로 잡힌다 |

- 붙여넣다 빠뜨린 행이 없는지는 실행 화면의 `job N개`를 작업 시트의 job 수와 맞춰 본다
- **to-be 시트의 Duration**: 튜닝이 운영에 반영되기 전이면 테스트에서 잰 값(예: Compaction 튜닝 실측)을 넣는다. 반영 후에는 `job_durations.py`로 다시 뽑아 교체한다
- app name이 두 시트에서 다르면 `job별 비교`에서 짝이 안 맞아 두 줄로 나뉜다 — 같은 job은 같은 이름으로 적는다

---

## 2. 실행

```bash
python resource_timeline.py 작업엑셀.xlsx              # 기준일 자동 (오늘부터 모든 job이 도는 첫날)
python resource_timeline.py 작업엑셀.xlsx 20261001     # 기준일 직접 지정
```

DRM 파일이면 Windows PowerShell에서 실행한다 (§2.3):

```powershell
python resource_timeline.py "C:\경로\작업엑셀.xlsx"
```

**기준일 = 그래프로 그리는 하루.** 매일·매시·5분마다 도는 job은 날짜와 무관하지만, **3일마다 도는 rw_mani(`5 2 */3 * *` = 매달 1·4·7…31일 02:05)**는 날짜에 따라 들어가기도 빠지기도 한다. 그래서 날짜를 안 주면 **오늘부터 31일 안에서 모든 job이 한 번 이상 도는 첫날**을 고른다(rw_mani가 도는 날 = 가장 바쁜 종류의 날). 실행 화면과 결과 파일에 `기준일 … — 모든 job이 도는 날 (자동 선택)`으로 찍힌다. 날짜를 직접 주면 그날 안 도는 job 수를 같이 알려 준다.

**파일을 못 열 때**: xlsx는 내부가 zip 파일이다. zip이 아니면(사내 DRM·열기 암호·옛 xls·CSV 등) **Windows에서는 종류를 가리지 않고 엑셀을 통해 읽는다** (§2.3). WSL·리눅스에서는 파일 앞부분을 보여 주고 Windows에서 실행하라고 안내한다.

| 경우 | 조치 |
|---|---|
| 사내 보안(DRM)·옛 .xls·CSV | Windows용 Python + xlwings로 실행하면 엑셀을 통해 읽는다 (§2.3) |
| 열기 암호가 걸린 파일 | 엑셀이 암호를 묻다 멈추므로 암호를 먼저 지운다 |
| `~$`로 시작하는 파일 | 엑셀 잠금 파일이다. 원래 파일 이름을 넣는다 |

실행 화면 예 (예시 데이터). 마지막 네 줄이 결론이고, 결과 파일 맨 위 `한눈에 보기`와 같다:

```text
as-is: 시트 'AS-IS' job 32개, 제외 0개
to-be: 시트 'TO-BE' job 32개, 제외 0개
cron 시간대: KST (latest_run 36건 중 36건이 KST 기준 cron과 일치) / 기준일 2026-10-01 — 모든 job이 도는 날 (자동 선택)
· 하루 CPU 사용량: 710 → 561 코어·시간 (21% 감소). 줄어든 양의 100%는 comp_range에서 나왔다 (294 → 145 코어·시간).
· 가장 붐빌 때 동시 CPU: 147 → 132코어 (10% 감소). to-be에서 가장 붐비는 시간대는 01:50~01:55. 하루 사용량보다 덜 줄어든 이유: 그 순간 CPU의 62%를 설정이 그대로인 job(append, comp_daily)이 쓰고 있다.
· 하루 메모리 사용량: 2,419 → 1,862 GB·시간 (23% 감소). 줄어든 양의 100%는 comp_range에서 나왔다 (1,293 → 735 GB·시간).
· 가장 붐빌 때 동시 메모리: 598 → 568GB (5% 감소). to-be에서 가장 붐비는 시간대는 01:50~01:55. 하루 사용량보다 덜 줄어든 이유: 그 순간 메모리의 55%를 설정이 그대로인 job(append, comp_daily)이 쓰고 있다.
→ 작업엑셀_리소스비교.xlsx
→ 작업엑셀_리소스비교.html  (브라우저로 연다)
```

`제외`는 cron도 없고 실행 간격도 못 구한 행이다(요약 시트 맨 아래에 이유와 함께 나온다).

### 2.1 결과 엑셀의 시트

모든 시트 1~3행에 **이 시트가 무엇을 보여 주는지** 한두 줄 설명이 있다. 순서는 읽는 순서다.

| 시트 | 무엇을 보여주나 | 이렇게 읽는다 |
|---|---|---|
| **읽는 법** | 기준일·원본·시트 목록·용어 설명 | 처음 보는 사람은 여기부터 |
| **요약** | `한눈에 보기` 결론 문장, 비교 표(CPU·메모리 각각), 그래프 4개(시간대별 동시 CPU·메모리 선 그래프, 종류별 하루 사용량 막대) | 튜닝 효과는 `하루 사용량 (코어·시간)`의 변화율로 본다 |
| **종류별 비교** | job 종류별 하루 사용량의 as-is·to-be·변화·변화율 | 변화가 큰 음수인 종류가 줄어든 곳 |
| **job별 비교** | job마다 1회 실행 시간·CPU·메모리·하루 사용량을 as-is·to-be로 나란히 | 무엇(실행 시간인지, 코어 수인지)이 바뀌었는지 |
| **가장 붐빈 순간** | CPU(메모리)를 가장 많이 쓴 순간에 떠 있던 job과 각 job의 CPU·메모리, **CPU·메모리 합계** | 합계 = 요약의 `가장 붐빌 때` 값. CPU 최대와 메모리 최대가 다른 순간이면 두 목록 |
| **종류별 누적 그래프** | 시간대별 동시 사용량을 종류별 색으로 쌓은 그래프, as-is 위·to-be 아래(같은 눈금) | 색 띠의 두께 = 그 종류가 그 시각에 쓰는 양 |
| 시간대별 비교 | 5분 구간마다 as-is·to-be 합계와 변화 (요약 선 그래프의 원본 값) | 특정 시각의 정확한 값 |
| as-is / to-be 시간대별 | 5분 구간마다 종류별 동시 사용량 (누적 그래프의 원본 값) | 특정 시각에 어느 종류가 얼마나 |
| as-is / to-be job별 | 작업 시트 값 + 하루 실행 횟수·하루 사용량(수식) | 계산 확인용 |

비교 표·변화·합계는 수식이라 엑셀이 열 때 계산한다. 그래프는 폭 30cm·높이 11cm, x축 글자는 2시간마다, 범례는 위.

**용어** (결과 파일의 `읽는 법`·`용어 설명`과 같다):

| 용어 | 뜻 | 예 |
|---|---|---|
| 동시 사용량 | 어떤 순간에 떠 있는 job들의 CPU(메모리) 합 | job A(50코어)와 B(32코어)가 같은 순간에 돌면 82코어 |
| 5분 구간 값 | 하루를 5분씩 288구간으로 나누고, 구간마다 **그 5분 동안 가장 많이 쓴 순간의 값**을 적은 것 (§3.3) | `01:50~01:55` 구간 100코어 = 이 5분 안에 100코어가 동시에 필요한 순간이 있었다 |
| 가장 붐빌 때 동시 사용량 | 하루 중 동시 사용량이 가장 큰 순간의 값. 클러스터에 확보해 둬야 하는 양 | |
| 하루 사용량 (코어·시간) | 코어 수 × 사용 시간(시간)을 하루 동안 모두 더한 값. **실제로 쓴 양이라 튜닝 효과는 이 값으로 본다** | 50코어로 2분 = 50 × 2 ÷ 60 = 1.7코어·시간, 매시 돌면 × 24 = 40 |
| 하루 평균 동시 사용량 | 하루 사용량 ÷ 24시간 | 480코어·시간 ÷ 24 = 20코어 |
| 설정값 단순 합산 | 모든 job 설정을 그냥 더한 값. 모든 job이 한꺼번에 떠 있다는 가정이라 실제로는 일어나지 않는다 (참고용) | |
| 변화 / 변화율 | 변화 = to-be − as-is, 변화율 = 변화 ÷ as-is. 음수면 줄었다 | −21% = 21% 줄었다 |

예전 결과의 `CPU·분 / 일`(코어·분)은 **코어·시간**으로 바꿨다(÷ 60). 숫자만 작아지고 뜻은 같다.

### 2.2 HTML 보고서 — 브라우저로 보기

`_리소스비교.html`은 같은 결과를 한 장에 담은 파일이다. 더블클릭하면 브라우저(Edge·Chrome)로 열리고, **인터넷 연결 없이** 열린다(그래프 라이브러리를 파일 안에 넣었다, 약 5MB).

| 순서 | 내용 |
|---|---|
| 1 | 제목·기준일·원본 |
| 2 | **한눈에 보기** — 엑셀 요약과 같은 결론 문장 |
| 3 | 핵심 숫자 4개 — 하루 CPU·메모리 사용량, 가장 붐빌 때 CPU·메모리 (to-be 값, 변화율 배지, as-is 값, 한 줄 정의) |
| 4 | **CPU / 메모리 전환 버튼** — 아래 그래프·표 전체가 바뀐다 (스크롤해도 위에 붙어 있다) |
| 5 | 시간대별 동시 사용량 — to-be 파랑(옅은 면) vs as-is 회색, 가장 붐빈 곳에 표시 |
| 6 | 종류별 하루 사용량 — 가로 막대, 바뀐 종류에만 변화율 |
| 7 | 종류별로 쌓아 보기 — as-is 위·to-be 아래, 같은 세로축 |
| 8 | 가장 붐빈 순간에 떠 있던 job — as-is·to-be 나란히, 합계 |
| 9 | job별 비교 표 — `as-is → to-be` 한 칸, 변화 막대(초록 = 줄어듦, 빨강 = 늘어남), 종류 필터, 머리글 누르면 정렬 |
| 10 | 용어 설명 (펼치기) |

- **마우스를 올리면** 그 5분 구간(`01:50~01:55`)의 as-is·to-be 값이 한 상자에 나온다
- **드래그하면 그 구간이 확대**되고, 더블클릭하면 원래대로. 범례의 항목을 누르면 그 선·종류를 끄고 켠다
- 그래프 오른쪽 위 카메라 버튼 = **PNG 저장**(2배 해상도). PPT에 붙일 때 쓴다
- 선은 부드러운 곡선으로 그린다(`SMOOTH`). 곡선은 모양만 바꾸고, 마우스를 올렸을 때 나오는 값은 계산값 그대로다. 엑셀 그래프는 곡선 옵션이 급하게 오르내리는 곳에서 실제보다 낮게·높게 휘어(없는 값처럼 보여) 직선으로 둔다
- plotly가 없으면 `HTML 보고서는 건너뜀 — pip install plotly`가 찍히고 엑셀만 나온다

### 2.3 사내 보안(DRM) 파일 — 엑셀을 통해 읽기

DRM 파일은 암호화돼 있어 Python이 직접 못 연다. 엑셀은 보안 프로그램이 풀어 주므로, **Python이 엑셀을 조종해 셀 값만 받아 온다.** 엑셀 창은 뜨지 않고, 파일은 읽기 전용으로 열었다 닫는다(원본 불변). 이미 열어 둔 엑셀 창과는 별개로 동작한다.

**준비 (한 번만, Windows에서)**

1. Windows용 Python 설치 — python.org에서 받아 설치(관리자 권한 없이 "Install for me only" 가능, **"Add python.exe to PATH" 체크**)
2. PowerShell에서 `pip install openpyxl xlwings plotly`

**실행**: PowerShell에서 `python resource_timeline.py "C:\경로\작업엑셀.xlsx"`. 첫 줄에 `엑셀을 통해 읽는다`가 찍히면 이 경로로 읽은 것이다. 이후 출력·결과는 일반 파일과 같다.

- 먼저 되는지 확인하려면 PowerShell에서 `$x = New-Object -ComObject Excel.Application; $b = $x.Workbooks.Open("C:\경로\작업엑셀.xlsx"); $b.Sheets.Item("AS-IS").Range("A1:C3").Value2; $b.Close($false); $x.Quit()` — 셀 값이 나오면 된다. 안 나오면 DRM이 엑셀 조종까지 막는 것이라 보안 해제(반출)만 남는다
- 수식 셀(K·L 등)은 엑셀이 계산한 값을 받아 오므로 "엑셀에서 저장한 파일" 조건이 필요 없다
- 병합 셀도 엑셀에 병합 범위를 물어 같은 값으로 채운다 (A열 job_type, T열 기능 요약)
- 결과 엑셀·HTML은 Python이 새로 만든 파일이다. 보안 프로그램에 따라 저장 후 자동으로 DRM이 걸릴 수 있다 — 엑셀은 여는 데 지장이 없고, HTML이 브라우저에서 깨져 보이면 DRM이 걸린 것이다

---

## 3. 계산 방법 — 예시 숫자로

### 3.1 job 하나의 실행 구간

한 번 실행 = `[cron 시각 + Start Offset, + Duration)` 구간 동안 토탈 cpu·토탈 메모리를 잡고 있다고 본다.

예: to-be comp_range 1번 테이블 — cron `45 * * * *`, Start Offset 0.5분, Duration 1.9분, 50코어
→ 매시 45:30 ~ 47:24에 50코어. 하루 24회 × 1.9분 × 50코어 ÷ 60 = **38.0코어·시간** (as-is 24회 × 2.9분 × 65코어 ÷ 60 = 75.4)

### 3.2 왜 6초 간격으로 재나 — 순차 실행을 두 번 세지 않으려고

comp_range는 테이블을 순서대로 돈다. to-be에서 1번이 47:24에 끝나고 2번이 47:36에 시작한다(Start Offset 2.6분, 34코어). **1분 단위로 "그 분에 떠 있던 job을 전부 더하기"로 계산하면 47분에 1번과 2번이 같이 들어가** 50 + 34 = 84코어가 된다. 실제로는 한 순간도 둘이 같이 떠 있지 않았다.

그래서 하루를 6초 간격(14,400개 시점)으로 쪼개 **각 시점에 실제로 떠 있는 job만** 더한다.

### 3.3 왜 5분 구간으로 묶고, 구간의 값은 최댓값인가

append 7개(합계 29코어)는 5분마다 시작해 2.4분 돌고 꺼진다. 6초 간격 값을 그대로 그리면 29 → 0 → 29 → 0 톱니가 하루 288번 반복되어 다른 job이 안 보인다.

5분 구간으로 묶고 **구간 안에서 합계가 가장 큰 순간의 값**을 쓰면 append는 29코어로 평평해진다. 뜻은 "이 5분 안에 한 번은 29코어가 필요하다" — 자원을 확보하는 입장에서 필요한 값이다. 구간 폭은 설정 `BUCKET_MIN`으로 바꾼다.

- 가장 붐빌 때 동시 사용량은 구간 폭과 무관하게 같다(최댓값의 최댓값)
- `하루 평균 동시 사용량`은 그래프가 아니라 `하루 사용량 ÷ 24시간`으로 구한다 — 구간마다 최댓값을 쓰는 그래프로 평균을 내면 실제보다 높게 나오기 때문

### 3.4 cron이 없을 때 — 실행 간격 추정

B열(cron)이 비어 있거나 cron 식이 아니면 그 행만 `(latest_run − oldest_run) ÷ (runs − 1)`로 간격을 구한다. 예: 최근 100회가 99시간에 걸쳐 있으면 99시간 ÷ 99 = 60분 → 매시 실행, 시작 분은 latest_run의 분. 실패한 실행이 빠져 조금 길게 나오므로 가까운 정규 간격(5·10·15·30·60분, 1·2·3일 등)으로 맞춘다. `job별` 시트의 cron 칸에 `60분마다 (추정)`으로 표시된다.

- **trigger로만 도는 테이블은 추정 대신 B열에 부모 cron을 적는 것이 정확하다.** 집계 스크립트에 수동 테이블과 부모를 지정하면(`TRIGGER_TABLE`·`TRIGGER_PARENT_DAG`) Start Offset이 "부모 cron 시각 → 이 테이블 실제 시작"으로 나오므로, B열 `*/5 * * * *` + 그 Offset이면 부모가 도는 5분마다, 부모가 끝나는 자리에 그려진다 ([집계 문서](airflow-job-duration.md) §3.4)
- 3일마다 도는 job은 B열에 cron(`5 2 */3 * *`)을 적어 둔다. 추정(최근 실행 + 3일 간격)은 월말(31일 → 다음 달 1일)에 cron과 어긋날 수 있다

### 3.5 cron 시간대 판단

Airflow cron은 UTC로 적었을 수도, KST로 적었을 수도 있다. 스크립트는 두 시트의 latest_run(UTC 시각)이 cron과 **UTC로 맞는지, KST로 맞는지** 세어 많은 쪽을 쓴다. 매시·5분마다 도는 job은 양쪽 다 맞아서 판단에 안 쓰고, daily job들이 판단한다. 근거는 `읽는 법` 시트에 찍힌다. 그래프는 항상 KST로 그린다. UTC면 `5 2 */3 * *`는 KST 11:05에 도는 것으로 그려진다.

### 3.6 예시 결과 읽기 — 어떤 지표로 비교하나

예시는 hourly Compaction만 튜닝했다고 가정했다 (as-is = executor 16대·16g·overhead 기본값·driver 1코어, to-be = 확정 설정 1번 12대 16g, 2번 8대 20g, 3·4번 12대 18g, overhead 3g, driver 2코어). 나머지 job은 두 시트가 같다.

| 지표 (CPU) | as-is | to-be | 변화 | 읽는 법 |
|---|---|---|---|---|
| 하루 사용량 | 710 코어·시간 | 561 코어·시간 | **−21%** | **하루 동안 실제로 쓴 CPU의 양 = 튜닝 효과.** 줄어든 149는 전부 comp_range(294 → 145) |
| 하루 평균 동시 사용량 | 29.6코어 | 23.4코어 | −21% | 하루 사용량 ÷ 24 |
| 가장 붐빌 때 동시 사용량 | 147코어 | 132코어 | −10% | **클러스터에 확보해야 하는 양.** 01:50~01:55 — comp_daily 3번째 테이블(50) + append 8개(32) 위에 comp_range 한 테이블이 얹힌 순간. 얹힌 테이블이 65 → 50코어라 15코어만 줄었다 |
| 설정값 단순 합산 | 667코어 | 591코어 | −11% | 모든 job이 동시에 떠 있다고 가정한 값. 튜닝 효과를 재는 지표로는 쓰지 않는다 |

- **튜닝 효과는 `하루 사용량`으로 본다.** 가장 붐빌 때는 한 순간만 보는 값이라, 튜닝하지 않은 job(comp_daily·append)이 그 순간을 만들면 덜 줄어든다 — `한눈에 보기`가 이 비율(62%)을 계산해 알려 준다
- **어디서 줄었나는 `종류별 비교`로 본다**
- 선 그래프에서는 회색(as-is)이 파랑(to-be) 위로 튀어나온 부분이 줄어든 만큼이다. 예시에서는 매시 45분 봉우리가 낮아지고(97 → 82코어) 폭도 좁아진다

---

## 4. 설정 (스크립트 상단)

| 설정 | 기본값 | 바꾸는 경우 |
|---|---|---|
| `SHEET_ASIS`, `SHEET_TOBE` | `None` (이름에서 찾기) | 시트 이름에 as-is / to-be가 없을 때 시트 이름 지정 |
| `COL_GROUP`, `COL_CRON`, `COL_NAME` | `A`, `B`, `C` | job_type·cron·app name 열 |
| `COL_TOTAL_CPU`, `COL_TOTAL_MEM` | `K`, `L` | 토탈 열 위치가 다를 때 |
| `COL_RUNS` … `COL_LATEST` | `M` ~ `S` | `job_durations.csv` F~L열을 다른 곳에 붙였을 때 |
| `COL_DURATION` | `COL_DUR_AVG` (평균) | 중앙값(`COL_DUR_MEDIAN`)이나 최댓값(`COL_DUR_MAX`)으로 그리고 싶을 때. 최댓값 = 가장 오래 걸린 날 기준의 보수적 그림 |
| `COL_DESC` | `T` | 기능 요약 열. 없으면 `None` |
| `CRON_TZ` | `"auto"` | 판단 근거가 없을 때(daily job이 없음) `"KST"`/`"UTC"` 지정 |
| `BUCKET_MIN` | `5` | 구간 폭(분). 1440의 약수 |
| `GROUP_COLORS` | 8색 | 종류 순서(AS-IS 시트에 처음 나온 순서)대로 배정. 9번째 종류부터 회색. 색각 이상 검사를 통과한 팔레트라 순서째로 바꾸지 말 것 |
| `FONT`, `FONT_SIZE` | `맑은 고딕`, `10` | 결과 엑셀·HTML 전체 글꼴 |
| `SMOOTH` | `0.5` | HTML 선의 곡선 정도 (0 = 꺾은선, 1.3 = 최대) |

---

## 5. 알아둘 점

| 항목 | 내용 |
|---|---|
| Compaction executor 수 | Dynamic Allocation이라 평소 1시간치는 시작 대수(1번 12, 2번 8, 3·4번 12)로 to-be의 K·L을 채운다. 재처리처럼 여러 시간치를 돌 때만 최대 36대까지 늘어나므로, 그 경우는 K·L을 36대 기준으로 바꾼 시트로 한 번 더 돌려 별도 그림으로 본다 (`compaction-executor-sizing-design.md` §5.5) |
| Duration = Airflow 기준 | pod 기동·spark-submit이 포함된 시간이라 자원 점유 시간에 맞다 ([집계 문서](airflow-job-duration.md) §5) |
| 수동 실행 | `job_durations.py`가 `scheduled` 실행만 집계하므로 재처리가 trigger한 Compaction은 그래프에 없다 |
| Duration이 cron 간격보다 길 때 | 앞 실행과 겹치는 구간이 자동으로 두 번 더해진다. DAG에 `max_active_runs=1`이 있으면 실제로는 겹치지 않고 밀리며, 그 밀림은 Start Offset에 이미 들어 있다 |
| 3일마다 도는 job의 하루 사용량 | 기준일이 rw_mani가 도는 날이라 하루 사용량에 rw_mani 1회분이 들어 있다(3일 평균보다 그만큼 많다). 예시에서 1.9코어·시간으로 전체의 0.3% |
| 작업 엑셀을 고치면 | 그래프는 스크립트가 계산한 값이다. 작업 엑셀 값이 바뀌면 스크립트를 다시 돌린다 |
| LibreOffice로 열고 저장하면 | x축 정시 눈금 설정이 빠진다. 결과 파일은 엑셀로 연다 |

---

## 6. 스크립트

```python
"""하루 리소스 사용량 as-is / to-be 비교 시각화 — 작업 엑셀(as-is 시트·to-be 시트) → 비교 그래프 엑셀.

실행:
    python resource_timeline.py <작업엑셀.xlsx>              # 기준일 = 오늘부터 모든 job이 도는 첫날 (3일마다 도는 job 포함)
    python resource_timeline.py <작업엑셀.xlsx> 20260928     # 기준일 직접 지정
  as-is·to-be 시트는 이름으로 찾는다 (as-is / asis / AS_IS, to-be / tobe …). 다르면 SHEET_ASIS·SHEET_TOBE에 이름을 넣는다
결과: 같은 폴더에 <작업엑셀>_리소스비교.xlsx + <작업엑셀>_리소스비교.html(브라우저용)  (원본은 건드리지 않는다)
필요: pip install openpyxl plotly   (plotly가 없으면 HTML은 건너뛰고 엑셀만 만든다)
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
        raise SystemExit(f"{COL_DURATION}열(Duration)과 {COL_TOTAL_CPU}열(토탈 cpu)에 숫자가 있는 행이 없다 — 열 설정을 확인. "
                         f"{COL_TOTAL_CPU}열이 수식이면 엑셀에서 열어 저장한 파일이어야 계산값이 읽힌다")
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
                skipped.append((rn, name, "cron이 없고 runs·oldest_run·latest_run으로 간격도 못 구함"))
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
    """5분 구간 b의 이름: 01:50~01:55"""
    return f"{hhmm(b * BUCKET_MIN)}~{hhmm((b + 1) * BUCKET_MIN)}"


def side_metrics(d):
    """한쪽(as-is 또는 to-be)의 지표. 하루 사용량 = 실행 횟수 × 1회 실행 시간(분) × 코어(GB) ÷ 60 → 코어·시간(GB·시간)."""
    groups, jobs = d["groups"], d["jobs"]
    n_runs = [len(d["runs"].get(i, [])) for i in range(len(jobs))]
    out = {"n_runs": n_runs}
    for k in ("cpu", "mem"):
        series = [sum(d["per_minute"][k][b][g] for g in groups) for b in range(NB)]
        peak = max(series)
        pb = series.index(peak)
        day = sum(n * j["duration"] * j[k] for n, j in zip(n_runs, jobs)) / 60
        out[k] = {"series": series, "peak": peak, "peak_bucket": pb, "peak_at": range_label(pb),
                  "day": day, "avg": day / 24, "simple": sum(j[k] for j in jobs),
                  "group_day": {g: sum(n * j["duration"] * j[k] for n, j in zip(n_runs, jobs) if j["group"] == g) / 60
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
        return "새로 생김"
    return f"{abs(b - a) / a * 100:.0f}% {'감소' if b < a else '증가'}"


def headline(M, groups):
    """맨 위에 보여 줄 결론 문장 (숫자는 이 파일의 계산값)."""
    A, B = M["as-is"], M["to-be"]
    lines = []
    for k, u in (("cpu", "코어"), ("mem", "GB")):
        a, b = A[k]["day"], B[k]["day"]
        line = f"하루 {KIND[k]} 사용량: {a:,.0f} → {b:,.0f} {u}·시간 ({change_text(a, b)})."
        diffs = {g: B[k]["group_day"].get(g, 0) - A[k]["group_day"].get(g, 0) for g in groups}
        if b < a:
            g = min(diffs, key=diffs.get)
            line += (f" 줄어든 양의 {diffs[g] / (b - a) * 100:.0f}%는 {g}에서 나왔다 "
                     f"({A[k]['group_day'].get(g, 0):,.0f} → {B[k]['group_day'].get(g, 0):,.0f} {u}·시간).")
        lines.append(line)
        pa, pb = A[k]["peak"], B[k]["peak"]
        line = (f"가장 붐빌 때 동시 {KIND[k]}: {pa:,.0f} → {pb:,.0f}{u} ({change_text(pa, pb)}). "
                f"to-be에서 가장 붐비는 시간대는 {B[k]['peak_at']}.")
        same = [g for g in groups if abs(diffs[g]) < 1e-9]
        share = sum(B[k]["by_group"][g][B[k]["peak_bucket"]] for g in same) / pb if pb else 0
        if a and pa and (pa - pb) / pa < (a - b) / a and share >= 0.5:
            used = [g for g in same if B[k]["by_group"][g][B[k]["peak_bucket"]] > 0]
            line += (f" 하루 사용량보다 덜 줄어든 이유: 그 순간 {KIND[k]}의 {share * 100:.0f}%를 "
                     f"설정이 그대로인 job({', '.join(used)})이 쓰고 있다.")
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
SIGNED = "+#,##0.0;-#,##0.0;0.0"
SIGNED_PCT = "+0.0%;-0.0%;0.0%"
CHART_W, CHART_H, CHART_ROWS = 30, 11, 23      # 그래프 폭·높이(cm), 그래프 하나가 차지하는 행 수
LABEL_EVERY_MIN = 120                          # x축 눈금 글자 간격(분) — 2시간마다
HEAD = 4                                       # 표 시트: 1행 제목, 2~3행 설명, 4행 머리글, 5행부터 값
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
    ws.row_dimensions[row].height = 30


def header(ws, names, row=HEAD):
    for c, h in enumerate(names, 1):
        ws.cell(row=row, column=c, value=h)
    style_header(ws, row, len(names))


def widths(ws, ws_widths):
    for c, w in enumerate(ws_widths, 1):
        ws.column_dimensions[get_column_letter(c)].width = w


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
        s.smooth = False     # 엑셀의 곡선(smooth)은 급하게 오르내리는 곳에서 실제보다 낮게·높게 휘어 값이 틀려 보인다 → 직선
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
    style_chart(ch, title, y_title)
    return ch


def time_label(b):
    """그래프 x축 눈금 글자 — LABEL_EVERY_MIN마다만 (나머지 칸은 빈칸이라 글자가 겹치지 않는다)."""
    t = b * BUCKET_MIN
    return hhmm(t) if t % LABEL_EVERY_MIN == 0 else ""


def write_timeline(wb, side, groups, per_minute):
    """<side> 시간대별: 5분 구간 = 1행. 종류별 CPU·메모리 + 합계(수식)."""
    ws = wb.create_sheet(f"{side} 시간대별")
    intro(ws, f"{side} 시간대별 동시 사용량 (그래프의 원본 값)",
          "하루를 5분씩 288구간으로 나눴다. 값 = 그 5분 동안 가장 많이 쓴 순간에 job 종류별로 쓰던 CPU·메모리. "
          "예: 01:50~01:55 행 = 01:50부터 5분 사이 가장 붐빈 순간의 값.",
          "작업 시트가 바뀌면 스크립트를 다시 돌린다 (값은 스크립트가 cron·Start Offset·Duration으로 계산했다).")
    g = len(groups)
    cpu_c, mem_c = 2, 2 + g + 1
    cpu_tot, mem_tot = 2 + g, 2 + 2 * g + 1
    label_col = mem_tot + 1
    header(ws, ["시간대 (KST)"] + [f"CPU · {x} (코어)" for x in groups] + ["CPU 합계 (코어)"] +
           [f"메모리 · {x} (GB)" for x in groups] + ["메모리 합계 (GB)", "그래프 눈금"])
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
    widths(ws, [14] + [16] * (label_col - 1))
    set_font(ws)
    return cpu_c, mem_c, get_column_letter(cpu_tot), get_column_letter(mem_tot), label_col


def write_jobs(wb, side, jobs, runs):
    """<side> job별: 작업 시트 값 + 하루 사용량(수식)."""
    ws = wb.create_sheet(f"{side} job별")
    intro(ws, f"{side} job별 하루 사용량 (작업 시트 '{side}' 값 + 계산)",
          "하루 CPU 사용량(코어·시간) = 하루 실행 횟수 × 1회 실행 시간(분) × CPU(코어) ÷ 60. "
          "예: 하루 24회 × 1.9분 × 50코어 ÷ 60 = 38코어·시간 (50코어를 하루에 모두 합쳐 46분 쓴 양).",
          "하루 실행 횟수는 cron으로 기준일 하루 동안 몇 번 시작하는지 센 값이다.")
    header(ws, ["작업 시트 행", "종류", "app name", "cron", "하루 실행 횟수", "Start Offset (분)",
                "1회 실행 시간 (분)", "CPU (코어)", "메모리 (GB)", "하루 CPU 사용량 (코어·시간)",
                "하루 메모리 사용량 (GB·시간)", "기능 요약"])
    for i, j in enumerate(jobs):
        r = FIRST + i
        for c, v in enumerate([j["row"], j["group"], j["name"], j["cron_text"], len(runs.get(i, [])), j["offset"],
                               j["duration"], j["cpu"], j["mem"], f"=E{r}*G{r}*H{r}/60", f"=E{r}*G{r}*I{r}/60",
                               j["desc"] or None], 1):
            ws.cell(row=r, column=c, value=v)
        for c, fmt in ((6, "0.0"), (7, "0.0"), (8, "#,##0.0"), (9, "#,##0.0"), (10, "#,##0.0"), (11, "#,##0.0")):
            ws.cell(row=r, column=c).number_format = fmt
    ws.freeze_panes = f"D{FIRST}"
    widths(ws, (11, 14, 36, 16, 12, 13, 13, 11, 12, 16, 17, 44))
    set_font(ws)
    return FIRST + len(jobs) - 1


def write_compare_timeline(wb, tl):
    """시간대별 비교: as-is·to-be 합계를 나란히 (각 시간대별 시트를 참조하는 수식)."""
    ws = wb.create_sheet("시간대별 비교")
    intro(ws, "시간대별 동시 사용량 — as-is vs to-be (요약 시트 선 그래프의 원본 값)",
          "5분 구간마다 그 5분 동안 가장 많이 쓴 순간의 CPU·메모리 합계. 변화 = to-be − as-is (음수면 튜닝 후 줄었다).")
    header(ws, ["시간대 (KST)", "as-is CPU (코어)", "to-be CPU (코어)", "CPU 변화 (코어)", "as-is 메모리 (GB)",
                "to-be 메모리 (GB)", "메모리 변화 (GB)", "그래프 눈금"])
    for b in range(NB):
        r = FIRST + b
        ws.cell(row=r, column=1, value=range_label(b))
        for col, (side, key) in zip((2, 3, 5, 6), (("as-is", "ct"), ("to-be", "ct"), ("as-is", "mt"), ("to-be", "mt"))):
            ws.cell(row=r, column=col, value=f"='{side} 시간대별'!{tl[side][key]}{r}").number_format = "#,##0.0"
        ws.cell(row=r, column=4, value=f"=C{r}-B{r}").number_format = SIGNED
        ws.cell(row=r, column=7, value=f"=F{r}-E{r}").number_format = SIGNED
        ws.cell(row=r, column=8, value=time_label(b))
    ws.freeze_panes = f"B{FIRST}"
    widths(ws, (14, 16, 16, 15, 17, 17, 16, 12))
    set_font(ws)
    return ws


def write_group_compare(wb, groups, last):
    """종류별 비교: 하루 사용량을 종류별로 SUMIF."""
    ws = wb.create_sheet("종류별 비교")
    intro(ws, "job 종류별 하루 사용량 — 어디서 줄었나",
          "하루 CPU 사용량(코어·시간) = 그 종류 job들의 (하루 실행 횟수 × 1회 실행 시간 × CPU ÷ 60) 합. "
          "변화 = to-be − as-is, 변화율 = 변화 ÷ as-is. 음수면 튜닝 후 줄었다.")
    header(ws, ["종류", "as-is 하루 CPU (코어·시간)", "to-be 하루 CPU (코어·시간)", "CPU 변화 (코어·시간)", "CPU 변화율",
                "as-is 하루 메모리 (GB·시간)", "to-be 하루 메모리 (GB·시간)", "메모리 변화 (GB·시간)", "메모리 변화율"])
    rng = lambda side, col: f"'{side} job별'!${col}${FIRST}:${col}${last[side]}"
    for i, g in enumerate(groups):
        r = FIRST + i
        ws.cell(row=r, column=1, value=g)
        for col, side, src in ((2, "as-is", "J"), (3, "to-be", "J"), (6, "as-is", "K"), (7, "to-be", "K")):
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
        for c, fmt in ((2, "#,##0.0"), (3, "#,##0.0"), (4, SIGNED), (5, SIGNED_PCT),
                       (6, "#,##0.0"), (7, "#,##0.0"), (8, SIGNED), (9, SIGNED_PCT)):
            ws.cell(row=r, column=c).number_format = fmt
        if r == tr:
            for c in range(1, 10):
                ws.cell(row=r, column=c).border = Border(top=THIN)
    widths(ws, (16, 17, 17, 16, 11, 18, 18, 17, 12))
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
    intro(ws, "job별 비교 — 어떤 job이 얼마나 바뀌었나",
          "같은 종류 + 같은 app name을 한 줄로 짝지었다. 한쪽 시트에만 있는 job은 다른 쪽 칸이 비고, 변화는 빈칸을 0으로 본다 "
          "(TO-BE에서 없앤 job = as-is 사용량 전부가 감소).")
    header(ws, ["종류", "app name", "기능 요약", "as-is 1회 실행 시간 (분)", "to-be 1회 실행 시간 (분)",
                "as-is CPU (코어)", "to-be CPU (코어)", "as-is 메모리 (GB)", "to-be 메모리 (GB)",
                "as-is 하루 CPU (코어·시간)", "to-be 하루 CPU (코어·시간)", "CPU 변화 (코어·시간)",
                "as-is 하루 메모리 (GB·시간)", "to-be 하루 메모리 (GB·시간)", "메모리 변화 (GB·시간)"])
    keys, pos = job_pairs(sides_jobs, groups)
    for i, k in enumerate(keys):
        r = FIRST + i
        ws.cell(row=r, column=1, value=k[0])
        ws.cell(row=r, column=2, value=k[1])
        ws.cell(row=r, column=3, value=pos[k]["desc"] or None)
        for (ca, ct), src, fmt in (((4, 5), "G", "0.0"), ((6, 7), "H", "#,##0.0"), ((8, 9), "I", "#,##0.0"),
                                   ((10, 11), "J", "#,##0.0"), ((13, 14), "K", "#,##0.0")):
            for col, side in ((ca, "as-is"), (ct, "to-be")):
                if side in pos[k]:
                    ws.cell(row=r, column=col, value=f"='{side} job별'!{src}{pos[k][side] + FIRST}").number_format = fmt
        ws.cell(row=r, column=12, value=f"=N(K{r})-N(J{r})").number_format = SIGNED
        ws.cell(row=r, column=15, value=f"=N(N{r})-N(M{r})").number_format = SIGNED
    ws.freeze_panes = f"C{FIRST}"
    widths(ws, (14, 36, 32, 13, 13, 11, 11, 12, 12, 15, 15, 15, 16, 16, 16))
    set_font(ws)


def write_peaks(wb, sides, M):
    """가장 붐빈 순간에 떠 있던 job — CPU 최대 순간과 메모리 최대 순간 (같은 순간이면 한 번만)."""
    ws = wb.create_sheet("가장 붐빈 순간")
    intro(ws, "가장 붐빈 순간에 떠 있던 job — 최대 동시 사용량을 누가 만들었나",
          "그 순간 동시에 돌던 job 목록과 각 job의 CPU·메모리. 합계 = 그 순간의 동시 사용량(요약 시트 '가장 붐빌 때' 값). "
          "여기 있는 job을 줄이거나 시각을 옮겨야 최대가 내려간다.")
    r = HEAD
    for side in SIDES:
        blocks = [("cpu", M[side]["cpu"])]
        if abs(M[side]["mem"]["peak_time"] - M[side]["cpu"]["peak_time"]) > 1e-6:
            blocks.append(("mem", M[side]["mem"]))
        for k, m in blocks:
            act = m["peak_jobs"]
            what = "CPU·메모리 모두" if len(blocks) == 1 else KIND[k]
            ws.cell(row=r, column=1, value=f"{side} — {what} 최대 순간: {hhmm(m['peak_time'])} "
                                           f"({range_label(m['peak_bucket'])} 구간), job {len(act)}개").font = BOLD_FONT
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
    widths(ws, (36, 14, 13, 13, 44))
    set_font(ws)


GUIDE_SHEETS = [
    ("요약", "결론 문장과 as-is·to-be 비교 표, 그래프 4개",
     "먼저 '한눈에 보기' 문장을 읽는다. 튜닝 효과는 '하루 사용량(코어·시간)'의 변화율로 본다"),
    ("종류별 비교", "job 종류별(append, comp_range …) 하루 사용량의 as-is·to-be·변화",
     "변화가 큰 음수인 종류가 튜닝으로 줄어든 곳이다"),
    ("job별 비교", "job 하나하나의 1회 실행 시간·CPU·메모리·하루 사용량을 as-is·to-be로 나란히",
     "어떤 job의 무엇(실행 시간인지, 코어 수인지)이 바뀌었는지 본다"),
    ("가장 붐빈 순간", "하루 중 CPU(메모리)를 가장 많이 쓴 순간에 떠 있던 job 목록과 합계",
     "최대 동시 사용량을 누가 만들었는지 본다. 합계 = 요약의 '가장 붐빌 때' 값"),
    ("종류별 누적 그래프", "시간대별 동시 사용량을 job 종류별 색으로 쌓은 그래프 (as-is 위, to-be 아래, 같은 눈금)",
     "색 띠의 높이 = 그 종류가 그 시각에 쓰는 양. 위아래 그래프의 높이를 바로 비교한다"),
    ("시간대별 비교", "5분 구간마다 as-is·to-be 동시 사용량 합계 (요약 선 그래프의 원본 값)", "특정 시각의 정확한 값을 찾을 때"),
    ("as-is/to-be 시간대별", "5분 구간마다 job 종류별 동시 사용량 (누적 그래프의 원본 값)", "특정 시각에 어느 종류가 얼마나 쓰는지 찾을 때"),
    ("as-is/to-be job별", "작업 시트 값(cron·Start Offset·실행 시간·CPU·메모리)과 하루 실행 횟수·하루 사용량", "계산이 맞는지 확인할 때"),
]
GUIDE_TERMS = [
    ("as-is / to-be", "튜닝 전 설정 / 튜닝 후 설정. 작업 엑셀의 'AS-IS'·'TO-BE' 시트", ""),
    ("동시 사용량", "어떤 순간에 떠 있는 job들의 CPU(메모리)를 모두 더한 값", "job A(50코어)와 job B(32코어)가 같은 순간에 돌면 그 순간 82코어"),
    ("5분 구간 값", "하루를 5분씩 288구간으로 나누고, 구간마다 그 5분 동안 가장 많이 쓴 순간의 값을 적었다. "
                 "5분마다 켜졌다 꺼지는 job이 그래프를 톱니로 만들지 않게 하려고 묶었다",
     "01:50~01:55 구간 값 100코어 → 이 5분 안에 100코어가 동시에 필요한 순간이 있었다"),
    ("가장 붐빌 때 동시 사용량", "하루 중 동시 사용량이 가장 큰 순간의 값. 클러스터에 확보해 둬야 하는 양", ""),
    ("하루 사용량 (코어·시간)", "코어 수 × 사용 시간(시간)을 하루 동안 모두 더한 값. 실제로 하루에 쓴 CPU의 양이라 튜닝 효과는 이 값으로 본다",
     "50코어로 2분 실행 = 50 × 2 ÷ 60 = 1.7코어·시간. 매시 돌면 × 24 = 40코어·시간"),
    ("하루 평균 동시 사용량", "하루 사용량 ÷ 24시간. 하루 내내 평균 몇 코어를 쓰는 셈인지", "480코어·시간 ÷ 24 = 20코어"),
    ("설정값 단순 합산", "모든 job의 CPU(메모리) 설정을 그냥 더한 값. 모든 job이 한꺼번에 떠 있다는 가정이라 실제로는 일어나지 않는다 (참고용)", ""),
    ("변화 / 변화율", "변화 = to-be − as-is, 변화율 = 변화 ÷ as-is. 음수면 튜닝 후 줄었다", "−21% = 21% 줄었다"),
    ("1회 실행 시간", "job이 한 번 도는 시간(분) — 작업 시트 Duration. Airflow task 시작~끝이라 pod 기동 시간이 들어 있다", ""),
    ("Start Offset", "cron 예정 시각부터 실제 시작까지 걸린 시간(분). 순차 실행 job은 앞 job이 끝나길 기다린 시간이 여기 들어 있다",
     "cron 01:00, Offset 19.3분 → 01:19에 시작"),
    ("기준일", "그래프로 그린 하루. 3일마다 도는 job(rw_mani)까지 들어가도록 모든 job이 도는 날을 자동으로 고른다", ""),
]


def write_guide(wb, day, day_note, sides, cron_tz, tz_reason, src):
    ws = wb.active
    ws.title = "읽는 법"
    ws.sheet_view.showGridLines = False
    intro(ws, "이 파일 읽는 법",
          "작업 엑셀의 as-is(튜닝 전)·to-be(튜닝 후) 설정으로 하루를 돌렸을 때 CPU·메모리를 언제, 얼마나 쓰는지 계산해 비교한다.",
          "job 하나는 cron 시각 + Start Offset에 시작해 1회 실행 시간 동안 그 job의 CPU·메모리를 잡고 있다고 보고, 하루 동안 겹쳐 더했다.")
    r = 5
    info = [("기준일 (KST)", f"{day.isoformat()} — {day_note}"),
            ("원본", f"{src} (as-is = '{sides['as-is']['sheet']}' 시트, to-be = '{sides['to-be']['sheet']}' 시트)"),
            ("cron 시간대", f"{cron_tz} ({tz_reason})"),
            ("1회 실행 시간 기준", {COL_DUR_AVG: "평균", COL_DUR_MEDIAN: "중앙값", COL_DUR_MAX: "최댓값"}.get(COL_DURATION, COL_DURATION)
             + " (스크립트 상단 COL_DURATION으로 바꾼다)")]
    for k, v in info:
        ws.cell(row=r, column=1, value=k).font = BOLD_FONT
        ws.cell(row=r, column=2, value=v)
        r += 1
    r += 1
    r0 = r
    for title, cols, rows in (("시트", ("시트", "무엇을 보여주나", "이렇게 읽는다"), GUIDE_SHEETS),
                              ("용어", ("용어", "뜻", "예"), GUIDE_TERMS)):
        header(ws, cols, row=r)
        for i, row in enumerate(rows, r + 1):
            for c, v in enumerate(row, 1):
                cell = ws.cell(row=i, column=c, value=v or None)
                cell.alignment = Alignment(wrap_text=True, vertical="top")
                cell.border = BORDER
            ws.cell(row=i, column=1).font = BOLD_FONT
        r += len(rows) + 3
    widths(ws, (26, 70, 60))
    for row in ws.iter_rows(min_row=r0):
        if row[0].value is not None and row[1].value is not None:      # 줄바꿈된 칸의 행 높이 (글자 1개 ≈ 열 폭 1.15)
            lines = max(math.ceil(len(str(c.value or "")) * 1.15 / ws.column_dimensions[c.column_letter].width)
                        for c in row[:3])
            ws.row_dimensions[row[0].row].height = max(1, lines) * 15 + 4
    set_font(ws)


def write_workbook(path, sides, groups, day, day_note, cron_tz, tz_reason, src, M):
    wb = Workbook()
    wb._fonts[0] = Font(name=FONT, size=FONT_SIZE)            # 빈 칸에 새로 입력하는 글자도 맑은 고딕 10
    wb._named_styles["Normal"].font = Font(name=FONT, size=FONT_SIZE)
    write_guide(wb, day, day_note, sides, cron_tz, tz_reason, src)
    s = wb.create_sheet("요약")
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
    widths(s, (30, 14, 14, 18, 11, 72))
    intro(s, "하루 리소스 사용량 — as-is(튜닝 전) vs to-be(튜닝 후)",
          f"기준일 {day.isoformat()} (KST) 하루 동안의 CPU·메모리 사용량. 용어는 '읽는 법' 시트")
    s.cell(row=4, column=1, value="한눈에 보기").font = BOLD_FONT
    lines = headline(M, groups)
    for i, line in enumerate(lines, 5):
        s.cell(row=i, column=1, value=f"· {line}")
    cmp = lambda col: f"'시간대별 비교'!${col}${FIRST}:${col}${HEAD + NB}"
    job = lambda side, col: f"SUM('{side} job별'!{col}{FIRST}:{col}{last[side]})"
    r = 5 + len(lines) + 1
    for k, (ca, cb), jc, tc in (("cpu", ("B", "C"), "H", "J"), ("mem", ("E", "F"), "I", "K")):
        u = UNIT[k]
        header(s, [f"{KIND[k]}", "as-is", "to-be", "변화 (to-be − as-is)", "변화율", "뜻"], row=r)
        rows = [
            (f"하루 {KIND[k]} 사용량 ({u}·시간)", f"={job('as-is', tc)}", f"={job('to-be', tc)}",
             f"{u} 수 × 사용 시간을 하루 동안 모두 더한 값 — 실제로 쓴 양이라 튜닝 효과는 이 값으로 본다"),
            (f"하루 평균 동시 사용량 ({u})", f"=B{r + 1}/24", f"=C{r + 1}/24", "하루 사용량 ÷ 24시간 — 하루 내내 평균 이만큼 쓰는 셈"),
            (f"가장 붐빌 때 동시 사용량 ({u})", f"=MAX({cmp(ca)})", f"=MAX({cmp(cb)})",
             "하루 중 job이 가장 많이 겹친 순간의 합 — 클러스터에 확보해 둬야 하는 양 ('가장 붐빈 순간' 시트)"),
            ("가장 붐빈 시간대", None, None, "위 값이 나온 5분 구간 (그 5분 안의 어느 순간)"),
            (f"설정값 단순 합산 ({u})", f"={job('as-is', jc)}", f"={job('to-be', jc)}",
             "모든 job 설정을 그냥 더한 값 — 모든 job이 한꺼번에 떠 있다는 가정이라 실제로는 일어나지 않는다 (참고용)"),
        ]
        for i, (label, fa, fb, note) in enumerate(rows, start=r + 1):
            s.cell(row=i, column=1, value=label).font = BOLD_FONT
            if fa is None:
                for col, src_col in ((2, ca), (3, cb)):
                    s.cell(row=i, column=col, value=f"=INDEX({cmp('A')},MATCH({get_column_letter(col)}{r + 3},{cmp(src_col)},0))")
                    s.cell(row=i, column=col).alignment = Alignment(horizontal="right")
            else:
                s.cell(row=i, column=2, value=fa).number_format = "#,##0.0"
                s.cell(row=i, column=3, value=fb).number_format = "#,##0.0"
                s.cell(row=i, column=4, value=f"=C{i}-B{i}").number_format = SIGNED
                s.cell(row=i, column=5, value=f'=IF(B{i}=0,"",D{i}/B{i})').number_format = SIGNED_PCT
            s.cell(row=i, column=6, value=note).font = NOTE_FONT
            for c in range(1, 7):
                s.cell(row=i, column=c).border = BORDER
            s.row_dimensions[i].height = 20
        r += len(rows) + 2

    ymax_cpu = nice_max(max(M[x]["cpu"]["peak"] for x in SIDES))
    ymax_mem = nice_max(max(M[x]["mem"]["peak"] for x in SIDES))
    anchor = r + 1
    charts = [compare_line_chart("시간대별 동시 CPU (코어) — as-is vs to-be", cmp_ws, (2, 3), 8, "코어", ymax_cpu),
              compare_line_chart("시간대별 동시 메모리 (GB) — as-is vs to-be", cmp_ws, (5, 6), 8, "GB", ymax_mem),
              compare_bar_chart("종류별 하루 CPU 사용량 (코어·시간)", grp_ws, grp_total - 1, (2, 3), "코어·시간"),
              compare_bar_chart("종류별 하루 메모리 사용량 (GB·시간)", grp_ws, grp_total - 1, (6, 7), "GB·시간")]
    for k, ch in enumerate(charts):
        s.add_chart(ch, f"A{anchor + k * CHART_ROWS}")
    r2 = anchor + len(charts) * CHART_ROWS
    for side in SIDES:
        sk = sides[side]["skipped"]
        if sk:
            s.cell(row=r2, column=1, value=f"{side} 계산에서 뺀 행 — {len(sk)}개 (cron도 없고 실행 간격도 못 구함)").font = BOLD_FONT
            for i, (rn, name, why) in enumerate(sk, start=r2 + 1):
                s.cell(row=i, column=1, value=f"{rn}행 {name}")
                s.cell(row=i, column=2, value=why)
            r2 += len(sk) + 2
    set_font(s)

    # ── 종류별 누적: 같은 눈금으로 as-is·to-be 위아래
    stack.sheet_view.showGridLines = False
    intro(stack, "job 종류별로 쌓은 시간대별 동시 사용량",
          "색 띠 하나 = job 종류 하나, 띠의 두께 = 그 종류가 그 시각에 쓰는 양, 맨 위 선 = 전체 동시 사용량.",
          "as-is(위)와 to-be(아래)는 세로축 눈금이 같아 높이를 바로 비교할 수 있다.")
    for i, (k, key, ymax) in enumerate((("cpu", "cpu_c", ymax_cpu), ("mem", "mem_c", ymax_mem))):
        for n, side in enumerate(SIDES):
            ws_side = wb[f"{side} 시간대별"]
            stack.add_chart(area_chart(f"{side} — 시간대별 동시 {KIND[k]} ({UNIT[k]}), 종류별", ws_side, groups,
                                       tl[side][key], tl[side]["label"], UNIT[k], ymax), f"A{5 + (i * 2 + n) * CHART_ROWS}")
    set_font(stack)
    for ws in (wb["읽는 법"], s, stack):
        ws.page_setup.orientation = "landscape"
        ws.page_setup.fitToWidth, ws.page_setup.fitToHeight = 1, 0
        ws.sheet_properties.pageSetUpPr.fitToPage = True
    wb.active = 0
    wb.calculation.fullCalcOnLoad = True          # 합계·요약 수식을 엑셀이 열 때 계산한다
    wb.save(path)


# ── HTML 보고서 (plotly) ───────────────────────────────────────────────
HTML_CSS = """
:root { --ink:#1F2328; --ink2:#4B5563; --muted:#6B7280; --line:#E5E7EB; --grid:#EEF0F3; --surface:#FFFFFF;
        --page:#F4F6F9; --head:#F3F5F8; --asis:#9AA0A6; --tobe:#2A78D6; --good:#17804D; --good-bg:#E7F5ED;
        --bad:#C0392B; --bad-bg:#FCEBEA; --accent-bg:#EAF2FC; }
* { box-sizing:border-box; }
html { font-size:10pt; }
body { margin:0; background:var(--page); color:var(--ink); font:10pt/1.6 '맑은 고딕','Malgun Gothic',sans-serif;
       -webkit-font-smoothing:antialiased; }
main { max-width:1200px; margin:0 auto; padding:28px 16px 64px; }
h1, h2, h3 { font-size:10pt; margin:0; }
h1 { font-weight:700; }
h2 { font-weight:700; margin-bottom:2px; }
p { margin:0; }
.sub { color:var(--ink2); margin-top:4px; }
.chips { display:flex; flex-wrap:wrap; gap:6px; margin-top:12px; }
.chip { background:var(--surface); border:1px solid var(--line); border-radius:999px; padding:2px 10px; color:var(--ink2); }
.chip b { color:var(--ink); font-weight:700; }
.card { background:var(--surface); border:1px solid var(--line); border-radius:12px; padding:18px 20px; margin-top:16px;
        box-shadow:0 1px 2px rgba(16,24,40,.04); }
.how { color:var(--muted); margin-bottom:10px; }
.headline ul { margin:8px 0 0; padding-left:18px; }
.headline li { margin:4px 0; }
.headline li::marker { color:var(--tobe); }
.kpis { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin-top:16px; }
.kpi { background:var(--surface); border:1px solid var(--line); border-radius:12px; padding:14px 16px;
       box-shadow:0 1px 2px rgba(16,24,40,.04); }
.kpi .label { color:var(--ink2); }
.kpi .row { display:flex; align-items:baseline; gap:8px; margin-top:6px; flex-wrap:wrap; }
.kpi .value { font-weight:700; }
.kpi .from { color:var(--muted); margin-top:2px; }
.kpi .def { color:var(--muted); margin-top:8px; padding-top:8px; border-top:1px solid var(--grid); }
.badge { display:inline-block; border-radius:6px; padding:0 6px; font-weight:700; white-space:nowrap; }
.badge.down { color:var(--good); background:var(--good-bg); }
.badge.up { color:var(--bad); background:var(--bad-bg); }
.badge.flat { color:var(--muted); background:var(--head); }
.toolbar { position:sticky; top:0; z-index:10; display:flex; align-items:center; gap:10px; margin-top:16px;
           padding:10px 0; background:var(--page); }
.toolbar .lab { color:var(--ink2); }
.seg { display:inline-flex; background:var(--surface); border:1px solid var(--line); border-radius:8px; padding:2px; }
.seg button { font:inherit; color:var(--ink2); background:transparent; border:0; border-radius:6px; padding:3px 14px; cursor:pointer; }
.seg button[aria-pressed="true"] { background:var(--tobe); color:#fff; font-weight:700; }
.legend-inline { display:inline-flex; align-items:center; gap:6px; margin-left:auto; color:var(--ink2); }
.key { display:inline-block; width:16px; height:0; border-top:2px solid; vertical-align:middle; }
.sw { display:inline-block; width:10px; height:10px; border-radius:2px; vertical-align:-1px; margin-right:6px; }
body[data-metric="cpu"] .m-mem, body[data-metric="mem"] .m-cpu { display:none; }
.two { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
.peak h3 { font-weight:700; margin-bottom:6px; }
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
dl.terms { display:grid; grid-template-columns:200px 1fr; gap:8px 16px; margin:10px 0 0; }
dl.terms dt { font-weight:700; } dl.terms dd { margin:0; color:var(--ink2); }
dl.terms dd span { color:var(--muted); }
details summary { cursor:pointer; font-weight:700; }
.foot { color:var(--muted); margin-top:16px; }
@media (max-width:900px) { .kpis { grid-template-columns:repeat(2,minmax(0,1fr)); } .two { grid-template-columns:1fr; }
                           dl.terms { grid-template-columns:1fr; } }
@media print { .toolbar { position:static; } body { background:#fff; } .card, .kpi { box-shadow:none; } }
"""
PX10 = 13.33                                      # 10pt = 13.33px
PLOT_FONT = dict(family="맑은 고딕, Malgun Gothic, sans-serif", size=PX10, color="#1F2328")
MUTED_FONT = dict(family=PLOT_FONT["family"], size=PX10, color="#6B7280")
PLOT_CONFIG = {"displaylogo": False, "responsive": True,
               "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d", "zoomIn2d", "zoomOut2d"],
               "toImageButtonOptions": {"format": "png", "scale": 2}}   # 카메라 버튼 = PNG 저장 (PPT에 붙일 때)
SMOOTH = 0.5                                      # 선 곡선 정도 (0 = 꺾은선, 1.3 = 최대). 값(마우스 올렸을 때)은 그대로


def write_html(path, sides, groups, day, day_note, cron_tz, tz_reason, src, M):
    """같은 결과를 브라우저용 HTML 한 장으로 — 마우스를 올리면 그 시간대의 값, 드래그로 확대, 범례 클릭으로 끄고 켜기.
    plotly가 없으면 건너뛴다 (엑셀 결과는 그대로 나온다)."""
    try:
        import plotly.graph_objects as go
        import plotly.offline
        from plotly.subplots import make_subplots
    except ImportError:
        print("HTML 보고서는 건너뜀 — pip install plotly 후 다시 실행하면 같이 만든다")
        return None
    import html as h
    import json

    esc = h.escape
    xs = [range_label(b) for b in range(NB)]                     # x = 5분 구간 이름 (마우스를 올리면 그대로 보인다)
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
        # 1) 시간대별 동시 사용량: to-be = 파랑 선 + 옅은 면, as-is = 회색 선
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=xs, y=M["as-is"][k]["series"], name="as-is (튜닝 전)", mode="lines",
                                 line=dict(color=col["as-is"], width=2, shape="spline", smoothing=SMOOTH),
                                 hovertemplate=f"%{{y:,.1f}} {u}<extra>as-is</extra>"))
        fig.add_trace(go.Scatter(x=xs, y=M["to-be"][k]["series"], name="to-be (튜닝 후)", mode="lines", fill="tozeroy",
                                 fillcolor="rgba(42,120,214,0.10)",
                                 line=dict(color=col["to-be"], width=2, shape="spline", smoothing=SMOOTH),
                                 hovertemplate=f"%{{y:,.1f}} {u}<extra>to-be</extra>"))
        pa, pb = M["as-is"][k], M["to-be"][k]
        marks = [(pb["peak_bucket"], pb["peak"], f"to-be 최대 {pb['peak']:,.0f}{u}", col["to-be"])]
        if pa["peak_bucket"] == pb["peak_bucket"]:
            marks = [(pa["peak_bucket"], pa["peak"], f"가장 붐빌 때 {pa['peak']:,.0f} → {pb['peak']:,.0f}{u}", col["to-be"])]
        else:
            marks.append((pa["peak_bucket"], pa["peak"], f"as-is 최대 {pa['peak']:,.0f}{u}", col["as-is"]))
        for b, y, text, c in marks:
            fig.add_annotation(x=xs[b], y=y, text=text, showarrow=True, arrowhead=0, arrowwidth=1, arrowcolor="#9AA0A6",
                               ax=40, ay=-24, font=PLOT_FONT, bgcolor="white", bordercolor="#E5E7EB", borderpad=3)
        base(fig, 380, hovermode="x unified")
        time_x(fig)
        fig.update_yaxes(range=[0, nice_max(max(pa["peak"], pb["peak"]) * 1.12)], ticksuffix=f" {u}")
        figs[f"line-{k}"] = fig

        # 2) 종류별 하루 사용량 (가로 막대) + 변화율
        fig = go.Figure()
        for side in SIDES:
            fig.add_trace(go.Bar(y=groups, x=[M[side][k]["group_day"].get(g, 0) for g in groups], name=side,
                                 orientation="h", marker=dict(color=col[side], line=dict(width=0), cornerradius=3),
                                 hovertemplate=f"%{{x:,.1f}} {u}·시간<extra>{side}</extra>"))
        top = max(max(M[side][k]["group_day"].values() or [0]) for side in SIDES) or 1
        for g in groups:
            a, b = M["as-is"][k]["group_day"].get(g, 0), M["to-be"][k]["group_day"].get(g, 0)
            if abs(b - a) < 1e-9:
                continue                              # 바뀐 종류에만 글자를 단다
            txt = change_text(a, b)
            c = "#17804D" if b < a else ("#C0392B" if b > a else "#6B7280")
            fig.add_annotation(x=max(a, b), y=g, text=f"<b>{txt}</b>", showarrow=False, xanchor="left", xshift=8,
                               font=dict(family=PLOT_FONT["family"], size=PX10, color=c))
        base(fig, 90 + 46 * len(groups), barmode="group", bargap=0.32, bargroupgap=0.08, hovermode="y unified")
        fig.update_yaxes(autorange="reversed", ticksuffix="  ", tickfont=PLOT_FONT, gridcolor="rgba(0,0,0,0)")
        fig.update_xaxes(range=[0, top * 1.22], showgrid=True, gridcolor="#EEF0F3", tickformat=",", ticksuffix=f" {u}·시간")
        figs[f"bar-{k}"] = fig

        # 3) 종류별로 쌓기 — as-is | to-be 나란히, 같은 세로축
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.12,
                            subplot_titles=["as-is (튜닝 전)", "to-be (튜닝 후)"])
        for n, side in enumerate(SIDES, 1):
            for g in groups:
                fig.add_trace(go.Scatter(x=xs, y=M[side][k]["by_group"][g], name=g, legendgroup=g, showlegend=n == 1,
                                         stackgroup=side, mode="lines",
                                         line=dict(width=0.6, color="white"),
                                         fillcolor="#" + group_color(groups, g),
                                         hovertemplate=f"%{{y:,.1f}} {u}<extra>{esc(g)}</extra>"), row=n, col=1)
        base(fig, 640, hovermode="x unified", margin=dict(l=8, r=16, t=64, b=8))
        fig.update_layout(legend=dict(y=1.06))
        fig.update_annotations(font=PLOT_FONT, yshift=2)
        time_x(fig)
        fig.update_xaxes(showticklabels=True)
        fig.update_yaxes(range=[0, nice_max(max(pa["peak"], pb["peak"]) * 1.05)], ticksuffix=f" {u}")
        figs[f"stack-{k}"] = fig

    # KPI
    def badge(a, b):
        if abs(b - a) < 1e-9:
            return '<span class="badge flat">변화 없음</span>'
        if not a:
            return '<span class="badge up">새로 생김</span>'
        return (f'<span class="badge {"down" if b < a else "up"}">{"▼" if b < a else "▲"} '
                f'{abs(b - a) / a * 100:.1f}%</span>')

    kpi_def = [("cpu", "day", "하루 CPU 사용량", "코어·시간", "코어 수 × 사용 시간을 하루 동안 더한 값 — 튜닝 효과"),
               ("cpu", "peak", "가장 붐빌 때 CPU", "코어", "job이 가장 많이 겹친 순간의 합 — 확보해야 할 양"),
               ("mem", "day", "하루 메모리 사용량", "GB·시간", "GB × 사용 시간을 하루 동안 더한 값"),
               ("mem", "peak", "가장 붐빌 때 메모리", "GB", "job이 가장 많이 겹친 순간의 합")]
    kpis = "".join(
        f'<div class="kpi"><div class="label">{label}</div><div class="row"><span class="value">'
        f'{M["to-be"][k][key]:,.1f} {u}</span>{badge(M["as-is"][k][key], M["to-be"][k][key])}</div>'
        f'<div class="from">as-is {M["as-is"][k][key]:,.1f} {u}</div><div class="def">{d}</div></div>'
        for k, key, label, u, d in kpi_def)

    # 가장 붐빈 순간
    def peak_table(side, k):
        m = M[side][k]
        rows = "".join(
            f'<tr><td><span class="sw" style="background:#{group_color(groups, j["group"])}"></span>{esc(j["name"])}'
            f'<div class="desc">{esc(j["group"])}{" · " + esc(j["desc"]) if j["desc"] else ""}</div></td>'
            f'<td class="num">{j["cpu"]:,.1f}</td><td class="num">{j["mem"]:,.1f}</td></tr>' for j in m["peak_jobs"])
        tot_c = sum(j["cpu"] for j in m["peak_jobs"])
        tot_m = sum(j["mem"] for j in m["peak_jobs"])
        return (f'<div class="peak"><h3>{side} — {hhmm(m["peak_time"])} ({range_label(m["peak_bucket"])} 구간), '
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
                           "dcpu": n * j["duration"] * j["cpu"] / 60, "dmem": n * j["duration"] * j["mem"] / 60}
        d = {k: v.get("to-be", {}).get("d" + k, 0) - v.get("as-is", {}).get("d" + k, 0) for k in ("cpu", "mem")}
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
        w = abs(x) / maxd[k] * 50 if maxd[k] else 0
        cls = "down" if x < 0 else "up"
        bar = f'<span class="bar"><i class="{cls}" style="width:{w:.1f}%"></i></span>' if abs(x) > 1e-9 else '<span class="bar"></span>'
        if abs(x) < 1e-9:
            return '<div class="chg"><span class="desc">변화 없음</span><span class="bar"></span></div>'
        return f'<div class="chg">{x:+,.1f}{bar}</div>'

    trs = []
    for (g, name), v, d in rows_data:
        desc = pos[(g, name)]["desc"]
        only = " · to-be에만 있음" if "as-is" not in v else (" · as-is에만 있음" if "to-be" not in v else "")
        trs.append(
            f'<tr data-group="{esc(g)}"><td data-v="{groups.index(g)}"><span class="sw" style="background:#{group_color(groups, g)}">'
            f'</span>{esc(g)}</td><td data-v="{esc(name)}">{esc(name)}<div class="desc">{esc(desc or "")}{only}</div></td>'
            f'<td class="num">{pair(v, "dur")}</td><td class="num">{pair(v, "cpu", "{:,.0f}")}</td>'
            f'<td class="num">{pair(v, "mem", "{:,.0f}")}</td>'
            + "".join(f'<td class="num m-{k}">{pair(v, "d" + k)}</td><td class="num m-{k}" data-v="{d[k]:.6f}">{chg(d[k], k)}</td>'
                      for k in ("cpu", "mem")) + "</tr>")
    options = "".join(f'<option value="{esc(g)}">{esc(g)}</option>' for g in groups)
    job_table = (
        f'<div class="tablebar"><label>종류 <select id="gsel"><option value="">전체</option>{options}</select></label>'
        f'<span class="desc">머리글을 누르면 정렬 · 값은 as-is → <b>to-be</b> (같으면 회색 한 번만)</span></div>'
        f'<div class="scroll"><table id="jobs"><thead><tr><th data-sort="n">종류</th><th data-sort="t">app name</th>'
        f'<th class="num">1회 실행 시간 (분)</th><th class="num">CPU (코어)</th><th class="num">메모리 (GB)</th>'
        f'<th class="num m-cpu">하루 CPU 사용량 (코어·시간)</th><th class="num m-cpu" data-sort="n">변화 (코어·시간)</th>'
        f'<th class="num m-mem">하루 메모리 사용량 (GB·시간)</th><th class="num m-mem" data-sort="n">변화 (GB·시간)</th>'
        f'</tr></thead><tbody>{"".join(trs)}</tbody></table></div>')

    terms = "".join(f"<dt>{esc(t)}</dt><dd>{esc(d)}" + (f"<br><span>예: {esc(e)}</span>" if e else "") + "</dd>"
                    for t, d, e in GUIDE_TERMS)
    dur = {COL_DUR_AVG: "평균", COL_DUR_MEDIAN: "중앙값", COL_DUR_MAX: "최댓값"}.get(COL_DURATION, COL_DURATION)
    fig_json = json.dumps({k: json.loads(f.to_json()) for k, f in figs.items()}, ensure_ascii=False)
    def fig_div(name):
        return "".join(f'<div class="m-{k}"><div id="{name}-{k}"></div></div>' for k in ("cpu", "mem"))

    page = f"""<!doctype html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>리소스 사용량 비교</title>
<style>{HTML_CSS}</style><script>{plotly.offline.get_plotlyjs()}</script></head>
<body data-metric="cpu"><main>
<header><h1>하루 리소스 사용량 비교 — as-is (튜닝 전) vs to-be (튜닝 후)</h1>
<p class="sub">두 설정으로 기준일 하루를 돌렸을 때 CPU·메모리를 언제, 얼마나 쓰는지. job마다 cron 시각 + Start Offset에 시작해
1회 실행 시간 동안 설정한 CPU·메모리를 잡고 있다고 보고 겹쳐 더했다.</p>
<div class="chips"><span class="chip">기준일 <b>{day.isoformat()}</b> ({esc(day_note)})</span>
<span class="chip">원본 <b>{esc(src)}</b> ('{esc(sides['as-is']['sheet'])}' vs '{esc(sides['to-be']['sheet'])}')</span>
<span class="chip">cron 시간대 <b>{cron_tz}</b></span><span class="chip">1회 실행 시간 <b>{dur}</b></span></div></header>

<section class="card headline"><h2>한눈에 보기</h2><ul>{"".join(f"<li>{esc(x)}</li>" for x in headline(M, groups))}</ul></section>
<div class="kpis">{kpis}</div>

<div class="toolbar"><span class="lab">보기</span><div class="seg" role="group">
<button data-m="cpu" aria-pressed="true">CPU</button><button data-m="mem" aria-pressed="false">메모리</button></div><span class="desc">아래 그래프·표 전체가 바뀐다</span></div>

<section class="card"><h2>시간대별 동시 사용량</h2>
<p class="how">하루를 5분 구간 288개로 나눠, 구간마다 그 5분 동안 가장 많이 쓴 순간의 값을 이었다. 파랑이 회색보다 낮은 만큼 튜닝으로 줄었다.
마우스를 올리면 그 구간의 값, 드래그하면 확대(더블클릭으로 원래대로).</p>{fig_div("line")}</section>

<section class="card"><h2>종류별 하루 사용량 — 어디서 줄었나</h2>
<p class="how">job 종류마다 하루 동안 쓴 양(코어·시간 = 코어 수 × 사용 시간). 오른쪽 글자 = as-is 대비 변화 (글자가 없으면 변화 없음).</p>{fig_div("bar")}</section>

<section class="card"><h2>job 종류별로 쌓아 보기</h2>
<p class="how">색 띠 하나 = job 종류 하나, 띠의 두께 = 그 종류가 그 시각에 쓰는 양, 맨 윗선 = 전체. 위(as-is)·아래(to-be) 세로축이 같다.
범례를 누르면 그 종류를 끄고 켠다.</p>{fig_div("stack")}</section>

<section class="card"><h2>가장 붐빈 순간에 떠 있던 job</h2>
<p class="how">하루 중 동시 사용량이 가장 큰 순간에 돌고 있던 job과 각 job의 CPU·메모리. 합계 = 위 '가장 붐빌 때' 값.
이 job들을 줄이거나 시각을 옮겨야 최댓값이 내려간다.</p>{peaks}</section>

<section class="card"><h2>job별 비교</h2>
<p class="how">같은 종류 + 같은 app name을 한 줄로 짝지었다. 변화 = to-be − as-is (초록 = 줄어듦, 빨강 = 늘어남).</p>{job_table}</section>

<section class="card"><details><summary>용어 설명</summary><dl class="terms">{terms}</dl></details></section>
<p class="foot">resource_timeline.py로 만든 파일 · 작업 엑셀이 바뀌면 스크립트를 다시 돌린다 · 그래프 오른쪽 위 카메라 버튼 = PNG 저장</p>
</main>
<script>
const FIGS = {fig_json};
const CONFIG = {json.dumps(PLOT_CONFIG)};
for (const [id, f] of Object.entries(FIGS)) Plotly.newPlot(id, f.data, f.layout, CONFIG);
function setMetric(m) {{
  document.body.dataset.metric = m;
  document.querySelectorAll('.seg button').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.m === m)));
  document.querySelectorAll('.m-' + m + ' .js-plotly-plot').forEach(p => Plotly.Plots.resize(p));
  try {{ localStorage.setItem('rt-metric', m); }} catch (e) {{}}
}}
document.querySelectorAll('.seg button').forEach(b => b.addEventListener('click', () => setMetric(b.dataset.m)));
try {{ const m = localStorage.getItem('rt-metric'); if (m === 'mem') setMetric(m); }} catch (e) {{}}
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
        raise SystemExit(f"파일이 없다: {src}")
    if src.name.startswith("~$"):
        raise SystemExit(f"'{src.name}'은 엑셀이 파일을 열어 둘 때 만드는 잠금 파일이다 — '~$'가 없는 원래 파일 이름을 넣는다")
    head = src.read_bytes()[:16]
    if head[:2] == b"PK":
        return load_workbook(src, data_only=True)     # 수식 셀은 계산된 값으로 읽는다
    if sys.platform == "win32":
        return read_via_excel(src)
    shown = head.decode("latin-1").encode("unicode_escape").decode("ascii")
    raise SystemExit(f"'{src.name}'은 Python이 직접 열 수 없는 파일이다 (파일 앞부분: {shown}) — "
                     f"사내 보안(DRM)·열기 암호·옛 xls·CSV 중 하나다. Windows용 Python에서 실행하면 엑셀을 통해 읽는다: "
                     f"pip install openpyxl xlwings plotly → python resource_timeline.py <파일>. WSL·리눅스에서는 안 된다")


def read_via_excel(src):
    """엑셀을 화면에 띄우지 않고 실행해 파일을 읽기 전용으로 열고, as-is·to-be 시트의 셀 값만 받아 온다.
    엑셀은 보안(DRM) 프로그램이 풀어 주므로 Python이 파일을 직접 건드리지 않아도 된다.
    받아 온 값은 메모리 안의 새 통합 문서에 같은 셀 위치로 옮긴다 → 이후 계산은 일반 xlsx와 똑같다."""
    try:
        import xlwings as xw
    except ImportError:
        raise SystemExit("사내 보안(DRM) 파일은 엑셀을 통해 읽는다 — pip install xlwings 후 다시 실행한다")
    print(f"'{src.name}'은 Python이 직접 못 여는 파일이다 → 엑셀을 통해 읽는다 (엑셀 창은 뜨지 않는다)")
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
            raise SystemExit(f"{label} 시트를 못 찾았다 (시트: {', '.join(sheetnames)}) — "
                             f"스크립트 상단 SHEET_ASIS·SHEET_TOBE에 시트 이름을 넣는다")
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
        day_note = "지정한 날" + (f", 이날 안 도는 job {len(missing)}개" if missing else ", 모든 job이 도는 날")
    else:
        day, ok = pick_day(all_jobs, cron_tz, datetime.now(KST).date())
        day_note = "모든 job이 도는 날 (자동 선택)" if ok else "오늘 (모든 job이 도는 날을 31일 안에서 못 찾음)"
    print(f"cron 시간대: {cron_tz} ({reason}) / 기준일 {day} — {day_note}")
    groups = order_groups(all_jobs)
    for side in SIDES:
        per_minute, runs, spans = simulate(sides[side]["jobs"], day, cron_tz, groups)
        sides[side].update(groups=groups, per_minute=per_minute, runs=runs, spans=spans)
    M = {side: side_metrics(sides[side]) for side in SIDES}
    out = src.with_name(f"{src.stem}_리소스비교.xlsx")
    write_workbook(out, sides, groups, day, day_note, cron_tz, reason, src.name, M)
    html_out = write_html(src.with_name(f"{src.stem}_리소스비교.html"), sides, groups, day, day_note, cron_tz, reason,
                          src.name, M)
    for line in headline(M, groups):
        print("· " + line)
    print(f"→ {out}" + (f"\n→ {html_out}  (브라우저로 연다)" if html_out else ""))

if __name__ == "__main__":
    main()
```
