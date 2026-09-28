# 하루 리소스 사용량 시각화 — as-is / to-be 비교 (엑셀 그래프)

> **결론**: 작업 엑셀의 **as-is 시트(튜닝 전)와 to-be 시트(튜닝 후)**를 넣으면, 두 시트의 시각별 동시 CPU·메모리와 하루 총 사용량을 **나란히 비교하는 그래프 엑셀**이 나온다. 예시 데이터(hourly Compaction만 튜닝)에서 하루 CPU 총 사용량은 42,605 → 33,684코어·분(**−21%**), 그중 hourly Compaction이 17,628 → 8,707(−51%)이다. 순간 최대는 147 → 132코어(−10%)로 덜 줄어드는데, 최대 순간을 만드는 daily Compaction·append는 그대로이기 때문이다.
>
> 원본 엑셀은 건드리지 않는다. 결과는 같은 폴더에 `<원본이름>_리소스비교.xlsx`로 생긴다.

- 앞 단계: [Airflow job 실행 시간 집계](airflow-job-duration.md) — Duration·Start Offset을 뽑는 스크립트
- 필요: Python 3.9 이상, `pip install openpyxl`
- **사내 보안(DRM)이 걸린 작업 엑셀**: **Windows용 Python**에서 실행하고 `pip install xlwings`를 추가한다 → 스크립트가 엑셀을 통해 값을 읽는다 (§2.2). WSL·리눅스 Python은 Windows 엑셀을 조종할 수 없어 안 된다

---

## 1. 입력 — 작업 엑셀의 모양

작업 엑셀 하나에 **같은 규격의 시트 2개**가 있다.

| 시트 | 뜻 | 스크립트가 찾는 방법 |
|---|---|---|
| **AS-IS** | 튜닝 전 설정·실행 시간 | 시트 이름에 `as-is`가 들어 있으면 (대소문자·기호 무시: `AS-IS`, `asis`, `as_is` 모두) |
| **TO-BE** | 튜닝 후 설정·실행 시간 | 시트 이름에 `to-be`가 들어 있으면 (`TO-BE`, `tobe` …) |

- 이름이 **정확히** as-is / to-be인 시트(대소문자·기호 무시)를 먼저 고른다. 그런 시트가 없을 때만 이름에 들어 있는 시트를 쓴다 → `AS-IS(x)`·`DIFF` 같은 다른 시트가 있어도 `AS-IS`·`TO-BE`를 고른다
- 이름이 다르면 스크립트 상단 `SHEET_ASIS`·`SHEET_TOBE`에 시트 이름을 넣는다
- 실행 화면 첫 두 줄 `시트 'AS-IS'`·`시트 'TO-BE'`로 고른 시트를 확인한다

시트 한 행 = job 하나. 두 시트 모두 아래 열 위치로 읽는다.

| 열 | 내용 | 스크립트가 쓰나 |
|---|---|---|
| **A** | **job_type** (`job_durations.csv`의 `job_type` 값: `append`, `summary`, `hourly compaction`, `daily compaction`, `expired snapshot`, `delete orphan`, `rewrite manifest`, `other`) | ✅ 그래프에서 쌓는 종류 |
| **B** | **cron** | ✅ trigger로만 도는 테이블은 **부모 DAG의 cron**을 적는다 (§3.4). 비어 있거나 cron 식이 아니면 그 행만 실행 간격을 추정 |
| **C** | **app name** | ✅ 결과 표의 이름. **as-is·to-be 행을 짝짓는 키**(종류 + app name) |
| D~J | 드라이버 cpu, 드라이버 메모리, 드라이버 메모리 오버헤드, 익스큐터 cpu, 익스큐터 메모리, 익스큐터 메모리 오버헤드, 익스큐터 개수 | ❌ (K·L을 만드는 재료) |
| **K** | **토탈 cpu** (코어) | ✅ |
| **L** | **토탈 메모리** (GB. `228g`처럼 단위가 붙어 있어도 읽는다) | ✅ |
| M~S | `job_durations.csv`의 **F~L열**을 그대로 붙인 것: runs, Duration (min), Duration median (min), Duration max (min), **Start Offset (min)**, oldest_run, latest_run | ✅ |
| **U** | **기능 요약** | ✅ 결과 표에 같이 보여 준다 |

- K·L이 수식이어도 된다. 엑셀에 **저장된 계산값**을 읽는다 (엑셀에서 한 번 저장된 파일이면 값이 들어 있다)
- 제목 행·빈 행·메모 행은 알아서 건너뛴다. **N열(Duration)과 K열(토탈 cpu)이 둘 다 숫자인 행만** job으로 본다

### 1.1 M~S가 비어 있는 행 — 안 돌린 job·삭제한 job

app name(C열)은 있는데 Duration(N열)이 비어 있는 행은 **그 시트의 계산에서 빠지고, "실행 기록 없어 뺀 행" 목록에 찍힌다** (실행 화면과 요약 시트 맨 아래).

| 경우 | 처리 | 비교 결과 |
|---|---|---|
| 의도적으로 안 돌림 (AS-IS·TO-BE 둘 다 빈칸) | 두 시트 모두에서 빠짐 | 어디에도 안 나옴 → 전후 차이 0 |
| TO-BE에서 삭제 (AS-IS에만 값) | AS-IS에만 들어감 | `job별 비교`에 TO-BE 칸이 비고, **AS-IS 사용량 전부가 감소**로 잡힌다. 예: daily compaction 한 테이블(하루 1회 × 12.5분 × 50코어 = 625코어·분)을 TO-BE에서 비우면 차이 −625 |
| TO-BE에서 새로 생김 (TO-BE에만 값) | TO-BE에만 들어감 | TO-BE 사용량 전부가 증가로 잡힌다 |

- 목록은 **붙여넣다 빠뜨린 행**을 찾는 용도다. 목록에 있는 행이 모두 일부러 비운 행인지 한 번 확인한다
- Duration은 있는데 K열(토탈 cpu)이 빈 행도 같은 목록에 이유와 함께 나온다
- **to-be 시트의 Duration**: 튜닝이 운영에 반영되기 전이면 테스트에서 잰 값(예: Compaction 튜닝 실측)을 넣는다. 반영 후에는 `job_durations.py`로 다시 뽑아 교체한다
- app name이 두 시트에서 다르면 `job별 비교` 시트에서 짝이 안 맞아 두 줄로 나뉜다 — 같은 job은 같은 이름으로 적는다

---

## 2. 실행

```bash
python resource_timeline.py 작업엑셀.xlsx              # 기준일 = 오늘(KST)
python resource_timeline.py 작업엑셀.xlsx 20260928     # 기준일 지정
```

DRM 파일이면 Windows PowerShell에서 실행한다 (§2.2):

```powershell
python resource_timeline.py "C:\경로\작업엑셀.xlsx"
```

**기준일(선택)**: 그래프로 그릴 하루다. 매일·매시·5분마다 도는 job은 날짜와 무관하고, **3일마다 도는 rewrite manifests(`0 6 */3 * *`)만** 그날 도는지가 날짜에 따라 달라진다. 생략하면 오늘. rewrite manifests까지 넣은 그림을 보려면 도는 날(1·4·7…일)을 준다.

**파일을 못 열 때**: xlsx는 내부가 zip 파일이다. zip이 아니면 스크립트가 파일 첫 바이트로 원인을 알려 준다.

| 원인 | 조치 |
|---|---|
| 사내 보안(DRM) 암호화 | Windows용 Python + xlwings로 실행 → 엑셀을 통해 읽는다 (§2.2) |
| 열기 암호가 걸린 파일 / 옛 .xls 형식 | Windows에서는 엑셀을 통해 읽는다. 열기 암호는 엑셀이 암호를 묻다 멈추므로 암호를 먼저 지운다 |
| CSV 등을 확장자만 .xlsx로 바꾼 파일 | [다른 이름으로 저장 → Excel 통합 문서(*.xlsx)] |
| `~$`로 시작하는 파일 | 엑셀 잠금 파일이다. 원래 파일 이름을 넣는다 |

실행 화면 예 (예시 데이터):

```text
as-is: 시트 'AS-IS' job 32개, 실행 기록 없어 뺀 행 0개, 제외 0개
to-be: 시트 'TO-BE' job 32개, 실행 기록 없어 뺀 행 0개, 제외 0개
cron 시간대: KST (latest_run 36건 중 36건이 KST 기준 cron과 일치) / 기준일 2026-09-28
as-is: 최대 동시 CPU 147.0코어 / 메모리 597.6GB, 하루 42,605코어·분 (단순 합산 667.0코어 / 2,217.2GB)
to-be: 최대 동시 CPU 132.0코어 / 메모리 568.0GB, 하루 33,684코어·분 (단순 합산 591.0코어 / 2,006.8GB)
→ 작업엑셀_리소스비교.xlsx
```

### 2.1 결과 엑셀의 시트

| 시트 | 내용 |
|---|---|
| **요약** | 실행 기록 없어 뺀 행 수(맨 아래에 목록), 비교 표(CPU·메모리 각각: 단순 합산 / 실제 최대 / 최대 시각 / 하루 평균 / 하루 총 사용량 — as-is, to-be, 차이, 변화율)와 그래프 4개: ①시각별 동시 CPU as-is vs to-be 선 그래프 ②같은 메모리 ③종류별 하루 CPU 사용량 막대(as-is 회색, to-be 파랑) ④같은 메모리 |
| **종류별 누적 그래프** | job 종류별로 쌓은 면적 그래프 — as-is CPU, to-be CPU, as-is 메모리, to-be 메모리. **위아래 두 그래프의 세로축 눈금을 같게** 맞춰서 높이를 눈으로 바로 비교할 수 있다 |
| as-is 시각별 / to-be 시각별 | 5분 칸 = 1행(288행). 칸마다 종류별 CPU·메모리와 합계(`SUM` 수식). 그래프의 원본 데이터 |
| as-is job별 / to-be job별 | job마다 종류·cron·하루 실행 횟수·Start Offset·Duration·총 CPU·총 메모리와 `CPU·분 / 일`(= 횟수 × Duration × CPU, 수식), 기능 요약 |
| **시각별 비교** | 5분 칸마다 as-is·to-be CPU·메모리 합계와 차이 (시각별 시트를 참조하는 수식) |
| **종류별 비교** | 종류별 하루 CPU·분, 메모리 GB·분의 as-is·to-be·차이·변화율 (`SUMIF` 수식) |
| **job별 비교** | 종류 + app name으로 짝지은 job마다 Duration·총 CPU·총 메모리·하루 사용량의 as-is·to-be·차이 |
| 최대 순간 job | as-is·to-be 각각 CPU가 가장 높았던 순간에 떠 있던 job 목록 |

비교 표·차이·합계는 수식이라 엑셀이 열 때 계산한다.

### 2.2 사내 보안(DRM) 파일 — 엑셀을 통해 읽기

DRM 파일은 암호화돼 있어 Python이 직접 못 연다. 엑셀은 보안 프로그램이 풀어 주므로, **Python이 엑셀을 조종해 셀 값만 받아 온다.** 엑셀 창은 뜨지 않고, 파일은 읽기 전용으로 열었다 닫는다(원본 불변). 이미 열어 둔 엑셀 창과는 별개로 동작한다.

**준비 (한 번만, Windows에서)**

1. Windows용 Python 설치 — python.org에서 받아 설치(관리자 권한 없이 "Install for me only" 가능, **"Add python.exe to PATH" 체크**)
2. PowerShell에서 `pip install openpyxl xlwings`

**실행**: PowerShell에서 `python resource_timeline.py "C:\경로\작업엑셀.xlsx"`. 첫 줄에 `엑셀을 통해 읽는다`가 찍히면 이 경로로 읽은 것이다. 이후 출력·결과 엑셀은 일반 파일과 같다.

- 먼저 되는지 확인하려면 PowerShell에서 `$x = New-Object -ComObject Excel.Application; $b = $x.Workbooks.Open("C:\경로\작업엑셀.xlsx"); $b.Sheets.Item("AS-IS").Range("A1:C3").Value2; $b.Close($false); $x.Quit()` — 셀 값이 나오면 된다. 안 나오면 DRM이 엑셀 조종까지 막는 것이라 보안 해제(반출)만 남는다
- 수식 셀(K·L 등)은 엑셀이 계산한 값을 받아 오므로 "엑셀에서 저장한 파일" 조건이 필요 없다
- 결과 엑셀(`_리소스비교.xlsx`)은 Python이 새로 만든 파일이다. 보안 프로그램에 따라 저장 후 자동으로 DRM이 걸릴 수 있으나, 엑셀로 여는 데는 지장이 없다

---

## 3. 계산 방법 — 예시 숫자로

### 3.1 job 하나의 실행 구간

한 번 실행 = `[cron 시각 + Start Offset, + Duration)` 구간 동안 토탈 cpu·토탈 메모리를 잡고 있다고 본다.

예: to-be hourly compaction 1번 테이블 — cron `45 * * * *`, Start Offset 0.5분, Duration 1.9분, 50코어
→ 매시 45:30 ~ 47:24에 50코어

### 3.2 왜 6초 간격으로 재나 — 순차 실행을 두 번 세지 않으려고

hourly compaction은 테이블을 순서대로 돈다. to-be에서 1번이 47:24에 끝나고 2번이 47:36에 시작한다(Start Offset 2.6분, 34코어). **1분 칸에 "그 분에 떠 있던 job을 전부 더하기"로 계산하면 47분 칸에 1번과 2번이 같이 들어가** 50 + 34 = 84코어가 된다. 실제로는 한 순간도 둘이 같이 떠 있지 않았다.

그래서 하루를 6초 간격(14,400개 시점)으로 쪼개 **각 시점에 실제로 떠 있는 job만** 더한다.

### 3.3 왜 그래프 한 칸이 5분이고, 칸의 값은 최댓값인가

예시의 cron으로 도는 append 7개(합계 29코어)는 5분마다 시작해 2.4분 돌고 꺼진다. 1분 칸 그래프로 그리면 29 → 0 → 29 → 0 톱니가 하루 288번 반복되어 다른 job이 안 보인다.

5분 칸으로 묶고 **칸 안에서 합계가 가장 큰 순간의 값**을 쓰면 append는 29코어로 평평해진다. 뜻은 "이 5분 안에 한 번은 29코어가 필요하다" — 자원을 확보하는 입장에서 필요한 값이다. 칸 폭은 설정 `BUCKET_MIN`으로 바꾼다(1로 두면 1분 칸).

- 최대 동시 사용량은 칸 폭과 무관하게 같다(최댓값의 최댓값)
- `하루 평균 사용량`은 그래프가 아니라 `하루 총 사용량 ÷ 1,440분`으로 구한다 — 칸마다 최댓값을 쓰는 그래프로 평균을 내면 실제보다 높게 나오기 때문

### 3.4 cron이 없을 때 — 실행 간격 추정

B열(cron)이 비어 있거나 cron 식이 아니면 그 행만 `(latest_run − oldest_run) ÷ (runs − 1)`로 간격을 구한다. 예: 최근 100회가 99시간에 걸쳐 있으면 99시간 ÷ 99 = 60분 → 매시 실행, 시작 분은 latest_run의 분. 실패한 실행이 빠져 조금 길게 나오므로 가까운 정규 간격(5·10·15·30·60분, 1·2·3일 등)으로 맞춘다. `job별` 시트의 cron 칸에 `60분마다 (추정)`으로 표시된다.

- **trigger로만 도는 테이블은 추정 대신 B열에 부모 cron을 적는 것이 정확하다.** 집계 스크립트에 수동 테이블과 부모를 지정하면(`TRIGGER_TABLE`·`TRIGGER_PARENT_DAG`) Start Offset이 "부모 cron 시각 → 이 테이블 실제 시작"으로 나오므로, B열 `*/5 * * * *` + 그 Offset이면 부모가 도는 5분마다, 부모가 끝나는 자리에 그려진다 ([집계 문서](airflow-job-duration.md) §3.4). 추정으로 그리면 간격은 5분으로 맞지만 시작 자리를 latest_run 한 번에 맞추므로 덜 정확하다
- 3일마다 도는 job은 cron(`*/3`, 매달 1일 기준)과 추정(최근 실행 + 3일 간격)이 월말에 어긋날 수 있다. cron이 있는 job은 B열에 적어 둔다

### 3.5 cron 시간대 판단

Airflow cron은 UTC로 적었을 수도, KST로 적었을 수도 있다. 스크립트는 두 시트의 latest_run(UTC 시각)이 cron과 **UTC로 맞는지, KST로 맞는지** 세어 많은 쪽을 쓴다. 매시·5분마다 도는 job은 양쪽 다 맞아서 판단에 안 쓰고, daily job들이 판단한다. 근거는 `요약` 시트에 찍힌다(예: "latest_run 36건 중 36건이 KST 기준 cron과 일치"). 그래프는 항상 KST로 그린다.

### 3.6 예시 결과 읽기 — 어떤 지표로 비교하나

예시는 hourly Compaction만 튜닝했다고 가정했다 (as-is = executor 16대·16g·overhead 기본값·driver 1코어, to-be = 확정 설정 1번 12대 16g, 2번 8대 20g, 3·4번 12대 18g, overhead 3g, driver 2코어). 나머지 job은 두 시트가 같다.

| 지표 (CPU) | as-is | to-be | 변화 | 읽는 법 |
|---|---|---|---|---|
| 설정값 단순 합산 | 667코어 | 591코어 | −11% | 모든 job이 동시에 떠 있다고 가정한 값. 튜닝 효과를 재는 지표로는 쓰지 않는다 |
| 실제 최대 동시 사용량 | 147코어 | 132코어 | −10% | **클러스터에 확보해야 하는 양.** 01:50 — daily compaction 3번째 테이블(50) + append 8개(32) 위에 hourly compaction이 한 테이블 얹힌 순간. 얹힌 테이블이 as-is 65코어 → to-be 50코어라 15코어만 줄었다 |
| 하루 총 사용량 | 42,605코어·분 | 33,684코어·분 | **−21%** | **하루 동안 실제로 쓴 CPU의 양 = 튜닝 효과.** 1번 테이블: as-is 24회 × 2.9분 × 65코어 = 4,524 → to-be 24회 × 1.9분 × 50코어 = 2,280 |

- **튜닝 효과는 `하루 총 사용량`으로 본다.** 최대 동시 사용량은 가장 무거운 순간 하나만 보는 값이라, 튜닝하지 않은 job(daily compaction·append)이 그 순간을 만들면 덜 줄어든다
- **어디서 줄었나는 `종류별 비교`로 본다.** 예시에서는 hourly compaction만 17,628 → 8,707코어·분(−51%)이고 나머지 종류는 0이다
- 선 그래프에서는 회색(as-is)이 파랑(to-be) 위로 튀어나온 부분이 줄어든 만큼이다. 예시에서는 매시 45분 봉우리가 낮아지고(65 → 50코어) 폭도 좁아진다(45:30부터 약 12분 → 약 8.5분)

---

## 4. 설정 (스크립트 상단)

| 설정 | 기본값 | 바꾸는 경우 |
|---|---|---|
| `SHEET_ASIS`, `SHEET_TOBE` | `None` (이름에서 찾기) | 시트 이름에 as-is / to-be가 없을 때 시트 이름 지정 |
| `COL_GROUP`, `COL_CRON`, `COL_NAME` | `A`, `B`, `C` | job_type·cron·app name 열 |
| `COL_TOTAL_CPU`, `COL_TOTAL_MEM` | `K`, `L` | 토탈 열 위치가 다를 때 |
| `COL_RUNS` … `COL_LATEST` | `M` ~ `S` | `job_durations.csv` F~L열을 다른 곳에 붙였을 때 |
| `COL_DURATION` | `COL_DUR_AVG` (평균) | 중앙값(`COL_DUR_MEDIAN`)이나 최댓값(`COL_DUR_MAX`)으로 그리고 싶을 때. 최댓값 = 가장 오래 걸린 날 기준의 보수적 그림 |
| `COL_DESC` | `U` | 기능 요약 열. 없으면 `None` |
| `CRON_TZ` | `"auto"` | 판단 근거가 없을 때(daily job이 없음) `"KST"`/`"UTC"` 지정 |
| `BUCKET_MIN` | `5` | 그래프 한 칸의 폭(분). 1440의 약수 |
| `GROUP_ORDER`, `GROUP_COLORS` | 종류 7개 + 기타 | 종류 순서 = 그래프에 아래부터 쌓이는 순서. 색은 색각 이상 검사를 통과한 팔레트라 순서째로 바꾸지 말 것 |

---

## 5. 알아둘 점

| 항목 | 내용 |
|---|---|
| Compaction executor 수 | Dynamic Allocation이라 평소 1시간치는 시작 대수(1번 12, 2번 8, 3·4번 12)로 to-be의 K·L을 채운다. 재처리처럼 여러 시간치를 돌 때만 최대 36대까지 늘어나므로, 그 경우는 K·L을 36대 기준으로 바꾼 시트로 한 번 더 돌려 별도 그림으로 본다 (`compaction-executor-sizing-design.md` §5.5) |
| Duration = Airflow 기준 | pod 기동·spark-submit이 포함된 시간이라 자원 점유 시간에 맞다 ([집계 문서](airflow-job-duration.md) §5) |
| 수동 실행 | `job_durations.py`가 `scheduled` 실행만 집계하므로 재처리가 trigger한 Compaction은 그래프에 없다 |
| Duration이 cron 간격보다 길 때 | 앞 실행과 겹치는 구간이 자동으로 두 번 더해진다. DAG에 `max_active_runs=1`이 있으면 실제로는 겹치지 않고 밀리며, 그 밀림은 Start Offset에 이미 들어 있다 |
| 작업 엑셀을 고치면 | 그래프는 스크립트가 계산한 값이다. 작업 엑셀 값이 바뀌면 스크립트를 다시 돌린다 |
| LibreOffice로 열고 저장하면 | x축 정시 눈금 설정이 빠진다. 결과 파일은 엑셀로 연다 |

---

## 6. 스크립트

```python
"""하루 리소스 사용량 as-is / to-be 비교 시각화 — 작업 엑셀(as-is 시트·to-be 시트) → 비교 그래프 엑셀.

실행:
    python resource_timeline.py <작업엑셀.xlsx>              # 기준일 = 오늘
    python resource_timeline.py <작업엑셀.xlsx> 20260928     # 기준일 지정 (3일마다 도는 job 등이 달라진다)
  as-is·to-be 시트는 이름으로 찾는다 (as-is / asis / AS_IS, to-be / tobe …). 다르면 SHEET_ASIS·SHEET_TOBE에 이름을 넣는다
결과: 같은 폴더에 <작업엑셀>_리소스비교.xlsx  (원본은 건드리지 않는다)
필요: pip install openpyxl
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
from openpyxl.chart.shapes import GraphicalProperties
from openpyxl.drawing.line import LineProperties
from openpyxl.comments import Comment
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
        raise SystemExit(f"{COL_DURATION}열(Duration)과 {COL_TOTAL_CPU}열(토탈 cpu)에 숫자가 있는 행이 없다 — 열 설정을 확인. "
                         f"{COL_TOTAL_CPU}열이 수식이면 엑셀에서 열어 저장한 파일이어야 계산값이 읽힌다")
    # 실행 기록 없어 뺀 행: app name은 있는데 Duration이 비었거나, Duration은 있는데 토탈 cpu가 빈 행
    # (의도적으로 안 돌린 job·to-be에서 삭제한 job — 붙여넣다 빠뜨린 행도 여기 나오므로 목록으로 확인한다)
    in_data = {rn for rn, _ in data}
    cell = lambda vals, letter: vals[col(letter)] if len(vals) > col(letter) else None
    no_run = []
    for rn, vals in rows:
        name = cell(vals, COL_NAME)
        if rn in in_data or not isinstance(name, str) or not name.strip():
            continue
        dur = cell(vals, COL_DURATION)
        if dur is None or (isinstance(dur, str) and not dur.strip()):
            no_run.append((rn, str(cell(vals, COL_GROUP) or "").strip(), name.strip(),
                           f"{COL_RUNS}~{COL_LATEST}열(Duration) 비어 있음"))
        elif to_number(dur) is not None:
            no_run.append((rn, str(cell(vals, COL_GROUP) or "").strip(), name.strip(),
                           f"{COL_TOTAL_CPU}열(토탈 cpu) 비어 있음"))

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
    return jobs, skipped, no_run


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


def order_groups(jobs):
    """job 종류를 GROUP_ORDER 순서로. 목록에 없는 종류는 뒤에 이름순."""
    return [g for g in GROUP_ORDER if any(j["group"] == g for j in jobs)] + \
        sorted({j["group"] for j in jobs} - set(GROUP_ORDER))


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


# ── 엑셀 쓰기 ─────────────────────────────────────────────────────────
SIDES = ("as-is", "to-be")
SIDE_COLORS = {"as-is": "9AA0A6", "to-be": "2A78D6"}     # 튜닝 전 = 회색(기준), 튜닝 후 = 파랑
HEAD_FILL = PatternFill("solid", fgColor="E9EEF5")
THIN = Side(style="thin", color="C9CED6")
BORDER = Border(bottom=THIN)
NOTE_FONT = Font(name=FONT, size=9, color="6B6B6B")
SIGNED = "+#,##0.0;-#,##0.0;0.0"
SIGNED_PCT = "+0.0%;-0.0%;0.0%"


def style_header(ws, row, ncol, first=1):
    for c in range(first, first + ncol):
        cell = ws.cell(row=row, column=c)
        cell.font = Font(name=FONT, bold=True, size=10)
        cell.fill = HEAD_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = BORDER


def set_font(ws):
    for row in ws.iter_rows():
        for c in row:
            if c.value is not None and not c.font.bold:
                c.font = Font(name=FONT, size=c.font.size or 10, color=c.font.color)


def nice_max(v):
    """y축 상한을 보기 좋은 숫자로 (두 그래프를 같은 눈금으로 맞출 때 쓴다)."""
    if v <= 0:
        return 1
    mag = 10 ** math.floor(math.log10(v))
    return next(m * mag for m in (1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10) if v <= m * mag)


def style_axes(ch, x_title, y_title, y_max=None):
    ch.x_axis.title, ch.y_axis.title = x_title, y_title
    ch.x_axis.delete = ch.y_axis.delete = False
    ch.y_axis.majorGridlines = ChartLines(spPr=GraphicalProperties(ln=LineProperties(solidFill="E3E6EA")))
    if y_max:
        ch.y_axis.scaling.min, ch.y_axis.scaling.max = 0, y_max
    ch.legend.position = "b"
    ch.height, ch.width = 9, 26


def time_axis(ch):
    ch.x_axis.tickLblSkip = 60 // BUCKET_MIN            # 정시마다 눈금
    ch.x_axis.tickMarkSkip = 60 // BUCKET_MIN


def area_chart(title, ws, groups, first_col, label_col, y_title, y_max):
    ch = AreaChart()
    ch.grouping = "stacked"
    ch.title = title
    ch.add_data(Reference(ws, min_col=first_col, max_col=first_col + len(groups) - 1, min_row=1, max_row=NB + 1),
                titles_from_data=True)
    ch.set_categories(Reference(ws, min_col=label_col, min_row=2, max_row=NB + 1))
    for g, s in zip(groups, ch.series):
        color = GROUP_COLORS[GROUP_ORDER.index(g)] if g in GROUP_ORDER else "8C8C8C"
        s.graphicalProperties.solidFill = color
        s.graphicalProperties.line.noFill = True
    time_axis(ch)
    style_axes(ch, "시각 (KST)", y_title, y_max)
    return ch


def compare_line_chart(title, ws, cols, label_col, y_title, y_max):
    ch = LineChart()
    ch.title = title
    for c in cols:
        ch.add_data(Reference(ws, min_col=c, min_row=1, max_row=NB + 1), titles_from_data=True)
    ch.set_categories(Reference(ws, min_col=label_col, min_row=2, max_row=NB + 1))
    for side, s in zip(SIDES, ch.series):
        s.graphicalProperties.line.solidFill = SIDE_COLORS[side]
        s.graphicalProperties.line.width = 22860        # 1.8pt
        s.marker.symbol = "none"
        s.smooth = False
    time_axis(ch)
    style_axes(ch, "시각 (KST)", y_title, y_max)
    return ch


def compare_bar_chart(title, ws, first_row, last_row, cols, y_title):
    ch = BarChart()
    ch.type, ch.grouping, ch.title = "col", "clustered", title
    for c in cols:
        ch.add_data(Reference(ws, min_col=c, min_row=first_row - 1, max_row=last_row), titles_from_data=True)
    ch.set_categories(Reference(ws, min_col=1, min_row=first_row, max_row=last_row))
    for side, s in zip(SIDES, ch.series):
        s.graphicalProperties.solidFill = SIDE_COLORS[side]
        s.graphicalProperties.line.noFill = True
    ch.gapWidth = 80
    style_axes(ch, "job 종류", y_title)
    return ch


def write_timeline(wb, side, groups, per_minute):
    """<side> 시각별: 5분 칸 = 1행. 종류별 CPU·메모리 + 합계(수식)."""
    ws = wb.create_sheet(f"{side} 시각별")
    g = len(groups)
    cpu_c, mem_c = 2, 2 + g + 1
    cpu_tot, mem_tot = 2 + g, 2 + 2 * g + 1
    label_col = mem_tot + 1
    headers = ["시각 (KST)"] + [f"CPU · {x}" for x in groups] + ["CPU 합계 (코어)"] + \
              [f"메모리 · {x}" for x in groups] + ["메모리 합계 (GB)", "그래프 눈금"]
    ws.append(headers)
    for m in range(NB):
        r, t = m + 2, m * BUCKET_MIN
        ws.cell(row=r, column=1, value=f"{t // 60:02d}:{t % 60:02d}")
        for i, x in enumerate(groups):
            ws.cell(row=r, column=cpu_c + i, value=per_minute["cpu"][m][x]).number_format = "#,##0.0"
            ws.cell(row=r, column=mem_c + i, value=per_minute["mem"][m][x]).number_format = "#,##0.0"
        for tot, first in ((cpu_tot, cpu_c), (mem_tot, mem_c)):
            ws.cell(row=r, column=tot,
                    value=f"=SUM({get_column_letter(first)}{r}:{get_column_letter(first + g - 1)}{r})"
                    ).number_format = "#,##0.0"
        ws.cell(row=r, column=label_col, value=f"{t // 60:02d}:00" if t % 60 == 0 else "")
    style_header(ws, 1, len(headers))
    ws.freeze_panes = "B2"
    ws.column_dimensions["A"].width = 11
    for c in range(2, len(headers) + 1):
        ws.column_dimensions[get_column_letter(c)].width = 14
    ws["A1"].comment = Comment(f"스크립트가 cron·Start Offset·Duration으로 계산한 값이다. 6초 간격으로 잰 동시 사용량 중 "
                               f"그 {BUCKET_MIN}분 안에서 합계가 가장 컸던 순간의 값. 작업 시트가 바뀌면 스크립트를 다시 돌린다.",
                               "resource_timeline.py")
    set_font(ws)
    return ws, cpu_c, mem_c, get_column_letter(cpu_tot), get_column_letter(mem_tot), label_col


def write_jobs(wb, side, jobs, runs):
    """<side> job별: 작업 시트 값 + 하루 사용량(수식)."""
    ws = wb.create_sheet(f"{side} job별")
    ws.append(["작업 시트 행", "종류", "app name", "cron", "하루 실행 횟수", "Start Offset (min)", "Duration (min)",
               "총 CPU (코어)", "총 메모리 (GB)", "CPU·분 / 일", "메모리 GB·분 / 일", "기능 요약"])
    for i, j in enumerate(jobs):
        r = i + 2
        ws.append([j["row"], j["group"], j["name"], j["cron_text"], len(runs.get(i, [])), j["offset"], j["duration"],
                   j["cpu"], j["mem"], f"=E{r}*G{r}*H{r}", f"=E{r}*G{r}*I{r}", j["desc"] or None])
        for c, fmt in ((6, "0.0"), (7, "0.0"), (8, "#,##0.0"), (9, "#,##0.0"), (10, "#,##0"), (11, "#,##0")):
            ws.cell(row=r, column=c).number_format = fmt
    style_header(ws, 1, 12)
    ws.freeze_panes = "D2"
    for c, w in zip("ABCDEFGHIJKL", (9, 17, 34, 15, 10, 11, 11, 11, 12, 12, 14, 40)):
        ws.column_dimensions[c].width = w
    set_font(ws)
    return len(jobs) + 1


def write_compare_timeline(wb, tl):
    """시각별 비교: as-is·to-be 합계를 나란히 (각 시각별 시트를 참조하는 수식)."""
    ws = wb.create_sheet("시각별 비교")
    ws.append(["시각 (KST)", "as-is CPU (코어)", "to-be CPU (코어)", "CPU 차이", "as-is 메모리 (GB)",
               "to-be 메모리 (GB)", "메모리 차이", "그래프 눈금"])
    for m in range(NB):
        r, t = m + 2, m * BUCKET_MIN
        ws.cell(row=r, column=1, value=f"{t // 60:02d}:{t % 60:02d}")
        for col, (side, key) in zip((2, 3, 5, 6), (("as-is", "ct"), ("to-be", "ct"), ("as-is", "mt"), ("to-be", "mt"))):
            ws.cell(row=r, column=col, value=f"='{side} 시각별'!{tl[side][key]}{r}").number_format = "#,##0.0"
        ws.cell(row=r, column=4, value=f"=C{r}-B{r}").number_format = SIGNED
        ws.cell(row=r, column=7, value=f"=F{r}-E{r}").number_format = SIGNED
        ws.cell(row=r, column=8, value=f"{t // 60:02d}:00" if t % 60 == 0 else "")
    style_header(ws, 1, 8)
    ws.freeze_panes = "B2"
    for c, w in zip("ABCDEFGH", (11, 14, 14, 12, 15, 15, 12, 11)):
        ws.column_dimensions[c].width = w
    set_font(ws)
    return ws


def write_group_compare(wb, groups, last):
    """종류별 비교: 하루 총 사용량(CPU·분, 메모리 GB·분)을 종류별로 SUMIF."""
    ws = wb.create_sheet("종류별 비교")
    ws.append(["종류", "as-is CPU·분 / 일", "to-be CPU·분 / 일", "CPU 차이", "CPU 변화율",
               "as-is 메모리 GB·분 / 일", "to-be 메모리 GB·분 / 일", "메모리 차이", "메모리 변화율"])
    rng = lambda side, col: f"'{side} job별'!${col}$2:${col}${last[side]}"
    for i, g in enumerate(groups):
        r = i + 2
        ws.cell(row=r, column=1, value=g)
        for col, side, src in ((2, "as-is", "J"), (3, "to-be", "J"), (6, "as-is", "K"), (7, "to-be", "K")):
            ws.cell(row=r, column=col, value=f"=SUMIF({rng(side, 'B')},$A{r},{rng(side, src)})")
    tr = len(groups) + 2
    ws.cell(row=tr, column=1, value="합계").font = Font(name=FONT, bold=True, size=10)
    for col in (2, 3, 6, 7):
        L = get_column_letter(col)
        ws.cell(row=tr, column=col, value=f"=SUM({L}2:{L}{tr - 1})")
    for r in range(2, tr + 1):
        ws.cell(row=r, column=4, value=f"=C{r}-B{r}")
        ws.cell(row=r, column=5, value=f'=IF(B{r}=0,"",D{r}/B{r})')
        ws.cell(row=r, column=8, value=f"=G{r}-F{r}")
        ws.cell(row=r, column=9, value=f'=IF(F{r}=0,"",H{r}/F{r})')
        for c, fmt in ((2, "#,##0"), (3, "#,##0"), (4, "+#,##0;-#,##0;0"), (5, SIGNED_PCT),
                       (6, "#,##0"), (7, "#,##0"), (8, "+#,##0;-#,##0;0"), (9, SIGNED_PCT)):
            ws.cell(row=r, column=c).number_format = fmt
        if r == tr:
            for c in range(1, 10):
                ws.cell(row=r, column=c).border = Border(top=THIN)
    style_header(ws, 1, 9)
    for c, w in zip("ABCDEFGHI", (18, 14, 14, 12, 11, 16, 16, 13, 12)):
        ws.column_dimensions[c].width = w
    ws.cell(row=tr + 2, column=1, value="CPU·분 / 일 = 하루 실행 횟수 × Duration × 총 CPU — 하루 동안 쓴 CPU의 양 "
                                        "(10코어로 6분 = 60코어·분). 변화율이 음수면 튜닝으로 줄었다").font = NOTE_FONT
    set_font(ws)
    return ws, tr


def write_job_compare(wb, sides_jobs):
    """job별 비교: (종류, app name)으로 as-is·to-be 행을 짝지어 나란히. 값은 각 job별 시트 참조."""
    ws = wb.create_sheet("job별 비교")
    ws.append(["종류", "app name", "기능 요약", "as-is Duration (min)", "to-be Duration (min)",
               "as-is 총 CPU", "to-be 총 CPU", "as-is 총 메모리 (GB)", "to-be 총 메모리 (GB)",
               "as-is CPU·분 / 일", "to-be CPU·분 / 일", "CPU·분 차이",
               "as-is 메모리 GB·분 / 일", "to-be 메모리 GB·분 / 일", "메모리 GB·분 차이"])
    keys, pos = [], {}
    for side in SIDES:
        for i, j in enumerate(sides_jobs[side]):
            k = (j["group"], j["name"])
            if k not in pos:
                pos[k] = {"desc": j["desc"]}
                keys.append(k)
            pos[k][side] = i + 2                       # job별 시트의 행
            pos[k]["desc"] = pos[k]["desc"] or j["desc"]
    order = {g: n for n, g in enumerate(GROUP_ORDER)}
    seen = {k: n for n, k in enumerate(keys)}          # 처음 나온 순서 (as-is 순서, to-be에만 있는 job은 뒤)
    keys.sort(key=lambda k: (order.get(k[0], len(order)), seen[k]))
    for i, k in enumerate(keys):
        r = i + 2
        ws.cell(row=r, column=1, value=k[0])
        ws.cell(row=r, column=2, value=k[1])
        ws.cell(row=r, column=3, value=pos[k]["desc"] or None)
        for (ca, ct), src, fmt in (((4, 5), "G", "0.0"), ((6, 7), "H", "#,##0.0"), ((8, 9), "I", "#,##0.0"),
                                   ((10, 11), "J", "#,##0"), ((13, 14), "K", "#,##0")):
            for col, side in ((ca, "as-is"), (ct, "to-be")):
                if side in pos[k]:
                    ws.cell(row=r, column=col, value=f"='{side} job별'!{src}{pos[k][side]}").number_format = fmt
        ws.cell(row=r, column=12, value=f"=N(K{r})-N(J{r})").number_format = "+#,##0;-#,##0;0"
        ws.cell(row=r, column=15, value=f"=N(N{r})-N(M{r})").number_format = "+#,##0;-#,##0;0"
    style_header(ws, 1, 15)
    ws.freeze_panes = "C2"
    for c, w in zip("ABCDEFGHIJKLMNO", (17, 34, 30, 11, 11, 10, 10, 12, 12, 12, 12, 11, 13, 13, 12)):
        ws.column_dimensions[c].width = w
    note = len(keys) + 3
    ws.cell(row=note, column=1, value="짝짓기 기준 = 종류 + app name. 한쪽에만 있는 job은 다른 쪽 칸이 비고, 차이는 빈칸을 0으로 본다"
            ).font = NOTE_FONT
    set_font(ws)


def write_peaks(wb, sides):
    ws = wb.create_sheet("최대 순간 job")
    r = 1
    for side in SIDES:
        d = sides[side]
        pb = max(range(NB), key=lambda m: sum(d["per_minute"]["cpu"][m][x] for x in d["groups"]))
        t = d["per_minute"]["cpu"][pb]["_sample"] / SAMPLES_PER_MIN + 0.05
        act = sorted(active_jobs(d["jobs"], d["spans"], t), key=lambda j: -j["cpu"])
        ws.cell(row=r, column=1, value=f"{side} — 최대 CPU 순간({int(t) // 60:02d}:{int(t) % 60:02d})에 떠 있던 job "
                                       f"{len(act)}개, 합계 {sum(j['cpu'] for j in act):,.1f}코어"
                ).font = Font(name=FONT, bold=True, size=11)
        for c, h in enumerate(["app name", "종류", "총 CPU (코어)", "총 메모리 (GB)", "기능 요약"], 1):
            ws.cell(row=r + 1, column=c, value=h)
        style_header(ws, r + 1, 5)
        for i, j in enumerate(act, start=r + 2):
            ws.cell(row=i, column=1, value=j["name"])
            ws.cell(row=i, column=2, value=j["group"])
            ws.cell(row=i, column=3, value=j["cpu"]).number_format = "#,##0.0"
            ws.cell(row=i, column=4, value=j["mem"]).number_format = "#,##0.0"
            ws.cell(row=i, column=5, value=j["desc"] or None)
        r += len(act) + 4
    for c, w in zip("ABCDE", (34, 18, 13, 14, 40)):
        ws.column_dimensions[c].width = w
    set_font(ws)


def write_workbook(path, sides, groups, day, cron_tz, tz_reason, src):
    wb = Workbook()
    s = wb.active
    s.title = "요약"
    tl, last = {}, {}
    for side in SIDES:
        d = sides[side]
        _, cpu_c, mem_c, ct, mt, label_col = write_timeline(wb, side, groups, d["per_minute"])
        tl[side] = {"cpu_c": cpu_c, "mem_c": mem_c, "ct": ct, "mt": mt, "label": label_col}
    for side in SIDES:
        last[side] = write_jobs(wb, side, sides[side]["jobs"], sides[side]["runs"])
    cmp_ws = write_compare_timeline(wb, tl)
    grp_ws, grp_total = write_group_compare(wb, groups, last)
    write_job_compare(wb, {side: sides[side]["jobs"] for side in SIDES})
    write_peaks(wb, sides)
    stack = wb.create_sheet("종류별 누적 그래프")

    # ── 요약
    for c, w in zip("ABCDEF", (30, 14, 14, 16, 11, 52)):
        s.column_dimensions[c].width = w
    s["A1"] = "하루 리소스 사용량 — as-is / to-be 비교"
    s["A1"].font = Font(name=FONT, bold=True, size=14)
    info = [
        ("기준일 (KST)", day.isoformat(), "3일마다 도는 job 등은 기준일에 따라 포함 여부가 달라진다"),
        ("원본", src, f"as-is = '{sides['as-is']['sheet']}' 시트, to-be = '{sides['to-be']['sheet']}' 시트"),
        ("cron 시간대", cron_tz, tz_reason),
        ("Duration 기준", {COL_DUR_AVG: "평균", COL_DUR_MEDIAN: "중앙값", COL_DUR_MAX: "최댓값"}.get(COL_DURATION, COL_DURATION),
         "스크립트 상단 COL_DURATION으로 바꾼다"),
        ("실행 기록 없어 뺀 행", f"as-is {len(sides['as-is']['no_run'])}개 · to-be {len(sides['to-be']['no_run'])}개",
         "Duration이 빈 행(안 돌린 job·삭제한 job). 목록은 이 시트 맨 아래 — 붙여넣다 빠뜨린 행이 없는지 확인"),
    ]
    for i, (k, v, note) in enumerate(info, start=3):
        s.cell(row=i, column=1, value=k).font = Font(name=FONT, bold=True, size=10)
        s.cell(row=i, column=2, value=v)
        s.cell(row=i, column=6, value=note).font = NOTE_FONT

    cmp = lambda col: f"'시각별 비교'!${col}$2:${col}${NB + 1}"
    job = lambda side, col: f"SUM('{side} job별'!{col}2:{col}{last[side]})"
    r = 9
    for unit, (ca, cb), jc, tc, sc in (("CPU (코어)", ("B", "C"), "H", "J", "코어"),
                                        ("메모리 (GB)", ("E", "F"), "I", "K", "GB")):
        for c, h in enumerate([unit, "as-is", "to-be", "차이 (to-be − as-is)", "변화율", "뜻"], 1):
            s.cell(row=r, column=c, value=h)
        style_header(s, r, 6)
        rows = [
            ("설정값 단순 합산", f"={job('as-is', jc)}", f"={job('to-be', jc)}",
             "모든 job이 동시에 떠 있다고 가정한 값 — 실제로는 일어나지 않는다"),
            ("실제 최대 동시 사용량", f"=MAX({cmp(ca)})", f"=MAX({cmp(cb)})",
             "하루 중 가장 많이 겹친 순간. 클러스터에 확보해야 하는 양"),
            ("최대가 나온 시각", None, None, f"{BUCKET_MIN}분 칸의 시작 시각. 같은 값이 여러 번이면 가장 이른 시각"),
            ("하루 평균 사용량", f"={job('as-is', tc)}/1440", f"={job('to-be', tc)}/1440",
             "하루 총 사용량 ÷ 1,440분"),
            (f"하루 총 사용량 ({sc}·분)", f"={job('as-is', tc)}", f"={job('to-be', tc)}",
             f"하루 실행 횟수 × Duration × 총 {'CPU' if sc == '코어' else '메모리'}의 합 — 하루 동안 쓴 양"),
        ]
        for i, (k, fa, fb, note) in enumerate(rows, start=r + 1):
            s.cell(row=i, column=1, value=k).font = Font(name=FONT, bold=True, size=10)
            if k == "최대가 나온 시각":
                for col, src_col in ((2, ca), (3, cb)):
                    peak = f"{get_column_letter(col)}{r + 2}"
                    s.cell(row=i, column=col, value=f"=INDEX({cmp('A')},MATCH({peak},{cmp(src_col)},0))")
                    s.cell(row=i, column=col).alignment = Alignment(horizontal="right")
            else:
                s.cell(row=i, column=2, value=fa).number_format = "#,##0.0"
                s.cell(row=i, column=3, value=fb).number_format = "#,##0.0"
                s.cell(row=i, column=4, value=f"=C{i}-B{i}").number_format = SIGNED
                s.cell(row=i, column=5, value=f'=IF(B{i}=0,"",D{i}/B{i})').number_format = SIGNED_PCT
            s.cell(row=i, column=6, value=note).font = NOTE_FONT
        r += len(rows) + 2
    s.cell(row=r - 1, column=1, value="변화율이 음수면 튜닝 후 줄어든 것이다. 세부는 '종류별 비교'·'job별 비교' 시트").font = NOTE_FONT

    ymax_cpu = nice_max(max(sum(sides[x]["per_minute"]["cpu"][m][g] for g in groups) for x in SIDES for m in range(NB)))
    ymax_mem = nice_max(max(sum(sides[x]["per_minute"]["mem"][m][g] for g in groups) for x in SIDES for m in range(NB)))
    anchor = r + 1
    s.add_chart(compare_line_chart("시각별 동시 CPU — as-is vs to-be", cmp_ws, (2, 3), 8, "코어", ymax_cpu), f"A{anchor}")
    s.add_chart(compare_line_chart("시각별 동시 메모리 — as-is vs to-be", cmp_ws, (5, 6), 8, "GB", ymax_mem),
                f"A{anchor + 19}")
    s.add_chart(compare_bar_chart("종류별 하루 CPU 사용량 (코어·분)", grp_ws, 2, grp_total - 1, (2, 3), "코어·분"),
                f"A{anchor + 38}")
    s.add_chart(compare_bar_chart("종류별 하루 메모리 사용량 (GB·분)", grp_ws, 2, grp_total - 1, (6, 7), "GB·분"),
                f"A{anchor + 57}")
    r2 = anchor + 76
    for side in SIDES:
        nr = sides[side]["no_run"]
        s.cell(row=r2, column=1, value=f"{side} ('{sides[side]['sheet']}' 시트) — 실행 기록 없어 뺀 행 {len(nr)}개"
               ).font = Font(name=FONT, bold=True, size=11)
        if nr:
            for c, h in enumerate(["작업 시트 행", "종류", "app name", "이유"], 1):
                s.cell(row=r2 + 1, column=c, value=h)
            style_header(s, r2 + 1, 4)
            for i, (rn, group, name, why) in enumerate(nr, start=r2 + 2):
                s.cell(row=i, column=1, value=rn)
                s.cell(row=i, column=2, value=group or None)
                s.cell(row=i, column=3, value=name)
                s.cell(row=i, column=4, value=why)
            r2 += len(nr) + 3
        else:
            r2 += 2
    for side in SIDES:
        sk = sides[side]["skipped"]
        if sk:
            s.cell(row=r2, column=1, value=f"{side} 계산에서 뺀 행 — {len(sk)}개").font = Font(name=FONT, bold=True, size=11)
            for i, (rn, name, why) in enumerate(sk, start=r2 + 1):
                s.cell(row=i, column=1, value=f"{rn}행 {name}")
                s.cell(row=i, column=2, value=why)
            r2 += len(sk) + 2
    set_font(s)

    # ── 종류별 누적: 같은 눈금으로 as-is·to-be 위아래
    stack["A1"] = "job 종류별 누적 — 같은 세로축 눈금으로 as-is(위)·to-be(아래)를 비교한다"
    stack["A1"].font = Font(name=FONT, bold=True, size=12)
    for i, (kind, key, unit, ymax) in enumerate((("CPU", "cpu_c", "코어", ymax_cpu), ("메모리", "mem_c", "GB", ymax_mem))):
        for k, side in enumerate(SIDES):
            ws_side = wb[f"{side} 시각별"]
            stack.add_chart(area_chart(f"{side} — 시각별 동시 {kind} ({unit})", ws_side, groups, tl[side][key],
                                       tl[side]["label"], unit, ymax), f"A{3 + (i * 2 + k) * 19}")
    set_font(stack)
    for ws in (s, stack):
        ws.page_setup.orientation = "landscape"
        ws.page_setup.fitToWidth, ws.page_setup.fitToHeight = 1, 0
        ws.sheet_properties.pageSetUpPr.fitToPage = True

    wb.move_sheet("종류별 누적 그래프", offset=-(len(wb.sheetnames) - 2))
    wb.calculation.fullCalcOnLoad = True          # 합계·요약 수식을 엑셀이 열 때 계산한다
    wb.save(path)


def open_workbook(src):
    """xlsx는 zip 파일이다. zip이면 openpyxl로 직접 읽고,
    zip이 아니면(사내 DRM·열기 암호·옛 xls) Windows에서는 엑셀을 통해 읽고, 그 밖에는 원인을 알려 준다."""
    if not src.exists():
        raise SystemExit(f"파일이 없다: {src}")
    if src.name.startswith("~$"):
        raise SystemExit(f"'{src.name}'은 엑셀이 파일을 열어 둘 때 만드는 잠금 파일이다 — '~$'가 없는 원래 파일 이름을 넣는다")
    head = src.read_bytes()[:8]
    if head[:2] == b"PK":
        return load_workbook(src, data_only=True)     # 수식 셀은 계산된 값으로 읽는다
    if head[:1] == b"<" or all(32 <= b < 127 or b in (9, 10, 13) for b in head):
        raise SystemExit(f"'{src.name}'은 xlsx가 아니라 CSV·HTML 같은 텍스트 파일이다 — "
                         f"엑셀에서 열어 [다른 이름으로 저장 → Excel 통합 문서(*.xlsx)]로 저장한다")
    if sys.platform == "win32":
        return read_via_excel(src)
    raise SystemExit(f"'{src.name}'은 Python이 직접 열 수 없는 파일이다 (파일 첫 바이트 {head.hex(' ').upper()} — "
                     f"사내 보안(DRM)·열기 암호·옛 xls). Windows용 Python에서 실행하면 엑셀을 통해 읽는다: "
                     f"pip install openpyxl xlwings → python resource_timeline.py <파일>. WSL·리눅스에서는 안 된다")


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
        book.close()
    finally:
        app.quit()
    return wb


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
    day = datetime.strptime(sys.argv[2], "%Y%m%d").date() if len(sys.argv) > 2 else datetime.now(KST).date()
    names = find_sheets(wb.sheetnames)
    sides = {}
    for side in SIDES:
        jobs, skipped, no_run = read_jobs(wb[names[side]])
        guessed = sum(1 for j in jobs if j["every"])
        print(f"{side}: 시트 '{names[side]}' job {len(jobs)}개, 실행 기록 없어 뺀 행 {len(no_run)}개, 제외 {len(skipped)}개"
              + (f", cron 없어 간격 추정 {guessed}개" if guessed else ""))
        for rn, group, name, why in no_run:
            print(f"    - {rn}행 {name} ({group or '종류 없음'}) — {why}")
        sides[side] = {"sheet": names[side], "jobs": jobs, "skipped": skipped, "no_run": no_run}
    cron_tz, reason = detect_cron_tz(sides["as-is"]["jobs"] + sides["to-be"]["jobs"])
    print(f"cron 시간대: {cron_tz} ({reason}) / 기준일 {day}")
    groups = order_groups(sides["as-is"]["jobs"] + sides["to-be"]["jobs"])
    for side in SIDES:
        per_minute, runs, spans = simulate(sides[side]["jobs"], day, cron_tz, groups)
        sides[side].update(groups=groups, per_minute=per_minute, runs=runs, spans=spans)
    out = src.with_name(f"{src.stem}_리소스비교.xlsx")
    write_workbook(out, sides, groups, day, cron_tz, reason, src.name)
    for side in SIDES:
        pm, jobs, runs = sides[side]["per_minute"], sides[side]["jobs"], sides[side]["runs"]
        peak_cpu = max(sum(pm["cpu"][m][g] for g in groups) for m in range(NB))
        peak_mem = max(sum(pm["mem"][m][g] for g in groups) for m in range(NB))
        cpu_min = sum(len(runs.get(i, [])) * j["duration"] * j["cpu"] for i, j in enumerate(jobs))
        print(f"{side}: 최대 동시 CPU {peak_cpu:,.1f}코어 / 메모리 {peak_mem:,.1f}GB, 하루 {cpu_min:,.0f}코어·분 "
              f"(단순 합산 {sum(j['cpu'] for j in jobs):,.1f}코어 / {sum(j['mem'] for j in jobs):,.1f}GB)")
    print(f"→ {out}")


if __name__ == "__main__":
    main()
```
