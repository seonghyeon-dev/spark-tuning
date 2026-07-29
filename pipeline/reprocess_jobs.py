"""재처리 대상 조회 — ConvertFileTaskGroup이 import해서 쓰는 공통 모듈.

**DAG 파일이 아니라 공통 모듈에 둔다.** 재처리 조회 task는 ConvertFileTaskGroup
`__init__` 안에서 만들어지므로(부모 지역 함수 `_update_jobs`·logger·config를
써야 하기 때문), 부모가 이 모듈을 import한다. 반대로 부모(공통 모듈)가
DAG 파일을 import할 수는 없다.

    dags/iceberg_reprocess.py  ──import──▶  ConvertFileTaskGroup ──import──▶ 이 모듈

호출 방법과 반환값 처리: pipeline/examples/convert_file_taskgroup_example.py

TODO(연결): ConvertFileTaskGroup과 같은 패키지로 옮길 것.
"""

import json

from airflow.providers.oracle.hooks.oracle import OracleHook

# Oracle DB 2개(a/b)에 동일 스키마의 Job History가 있어 같은 쿼리를 DB별로 반복한다.
# TODO(연결): append DAG이 쓰는 conn_list와 동일 소스 사용
ORACLE_CONN_IDS = ["oracle_a", "oracle_b"]

ROW_LIMIT = 1000     # 테이블당·DB당 조회 상한 — 한 회차 물량은 이것만으로 통제한다
                     # (설계 5.4 — 러프 설정, 운영 데이터로 재검증 필요)

# 복합키 4개 (ts도 그중 하나). 상태 UPDATE 인자로 넘길 때는 이 순서의 값 tuple로 만든다.
# TODO(연결): 실제 컬럼명으로 교체 — 아래 SQL의 컬럼명도 함께 고칠 것
PK_COLUMNS = ("k_1", "k_2", "k_3", "ts")

# stat_desc(CLOB)를 그냥 조회하면 LOB 객체로 와 문자열 비교·set 연산이 안 되므로
# VARCHAR2로 변환해 받는다 (batch_id는 짧아 4000바이트로 충분).
# 컬럼명이 곧 row의 키가 되므로 변환 컬럼에는 `AS stat_desc` 별칭이 반드시 필요하다.
SELECT_TARGETS_SQL = """
SELECT * FROM (
    SELECT k_1, k_2, k_3, ts, base_path, param, status,
           DBMS_LOB.SUBSTR(stat_desc, 4000, 1) AS stat_desc
      FROM JOB_HISTORY
     WHERE table_name = :tbl
       AND ts >= :ts_from AND ts < :ts_to
       AND ( status = 'FAILED'
             OR (status = 'WAIT' AND ts < :wait_bound) )
     ORDER BY ts ASC
) WHERE ROWNUM <= :row_limit
"""


def select_rows(conn_id: str, sql: str, binds: dict) -> list[dict]:
    """조회 결과를 컬럼명 dict로 매핑.

    컬럼명은 커서에서 그대로 받는다 — SELECT와 컬럼 목록을 따로 맞출 필요가 없다
    (Oracle이 대문자로 주므로 소문자로 통일).
    """
    with OracleHook(oracle_conn_id=conn_id).get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, binds)
        columns = [d[0].lower() for d in cur.description]
        return [dict(zip(columns, row)) for row in cur]


def committed_batch_ids(table_name: str, batch_ids: set[str]) -> set[str]:
    """영수증 확인 (설계 4.2): batch_ids 중 테이블 snapshot에 실제로 있는 것만 반환.

    batch당 1회씩 조회하지 않고 IN 조건으로 한 번에 대조한다. 반환도 넘긴 값의
    부분집합이어야 한다 — snapshot 전체를 긁어오면 안 된다.
    TODO(연결): 기존 Trino/Spark 조회 경로 재사용.
      SELECT element_at(summary, 'batch_id')
        FROM <catalog>.<db>.<table>.snapshots
       WHERE element_at(summary, 'batch_id') IN (:batch_ids)
    """
    raise NotImplementedError


def reprocess_get_jobs(cfg: dict, *, table, run_id, ti) -> dict:
    """재처리 대상 조회. append get_jobs와 조회 범위·영수증 확인만 다르다.

    처리 순서
      ① DB 2개에서 대상 조회 (전날+그저께, WAIT는 append 범위 밖만)
      ② 영수증 확인 — 이미 Iceberg에 커밋된 건은 재적재하면 중복이므로 골라낸다
      ③ 남은 것을 ts 오름차순으로 세워 파일 목록과 복합키 목록을 만든다
      ④ 재처리 DAG 자신이 쓸 값(적재 시간 범위·잔여 여부)을 XCom에 남긴다

    ── 반환 dict를 호출부에서 이렇게 쓴다 ──────────────────────────────────
      to_done   이미 커밋이 확인된 대상. **재적재하지 않고 상태만 정정한다.**
                → `_update_jobs(conn_id, pks, "DONE")`
                  batch_id는 넘기지 않는다 (기존 stat_desc = 영수증을 유지)
                → files가 비어 있어도 이건 먼저 처리해야 한다
      files     적재할 avro 파일 목록. **비었으면 이번 회차에 적재할 것이 없다.**
                → `_upload_file_list(files)`에 그대로 넘기고, 반환된 executor
                  개수를 XCom에 push (Spark operator가 pull)
                → 비었으면 여기서 False 반환 (short_circuit → 하류 skip)
      to_mark   이번에 적재할 대상.
                → XCom에 먼저 남긴 뒤(update_failure가 회수할 수 있도록)
                  `_update_jobs(conn_id, pks, "IN_PROGRESS", batch_id)`
      batch_id  이번 배치의 영수증 값.
                → 위 마킹에 쓰고, Spark 쓰기 옵션
                  `option("snapshot-property.batch_id", batch_id)`에도 같은 값

    to_done/to_mark는 `{conn_id: [복합키 값 tuple, ...]}` 형태다. 복합키 값은 DB 간
    유일 보장이 없어 어느 DB에서 온 row인지가 UPDATE 대상을 결정하기 때문이다.

    전체 코드: pipeline/examples/convert_file_taskgroup_example.py
    """
    if not cfg:
        raise ValueError("prepare_run 결과 없음 — 선행 task 실패")

    result = {"files": [], "to_done": {}, "to_mark": {}, "batch_id": None}
    table_name = table.get_name()
    if table_name not in cfg["tables"]:
        return result   # 수동 실행에서 미선택 → skip

    # ── ① 조회 ────────────────────────────────────────────────────────────
    # DB별로 실행하고 결과도 conn_id를 키로 보관한다. 상태 UPDATE가 이 키로
    # 원천 DB를 찾아가므로 row에 출처를 따로 붙일 필요가 없다. ROW_LIMIT은 DB당 적용.
    binds = {"tbl": table_name, "row_limit": ROW_LIMIT, "ts_from": cfg["ts_from"],
             "ts_to": cfg["ts_to"], "wait_bound": cfg["wait_bound"]}
    jobs_by_conn = {conn_id: select_rows(conn_id, SELECT_TARGETS_SQL, binds)
                    for conn_id in ORACLE_CONN_IDS}

    # 상한을 꽉 채운 DB가 있으면 거기에 더 남았다는 뜻이다. ②에서 건수가 줄면
    # 알 수 없게 되므로 지금 기록해 둔다 (loop를 한 번 더 돌지 판단하는 근거)
    fetched_full = any(len(rows) >= ROW_LIMIT for rows in jobs_by_conn.values())

    # ── ② 영수증 확인 (설계 4) ────────────────────────────────────────────
    # status는 커밋 여부의 증거가 아니다. Airflow가 실패로 판정했든(FAILED),
    # 커밋 뒤 상태 갱신만 실패했든(WAIT로 잔류) Spark 커밋은 성공했을 수 있다.
    # batch_id가 snapshot에 있으면 그 데이터는 이미 Iceberg에 있다 → 재적재 금지.
    batch_ids = {r["stat_desc"] for rows in jobs_by_conn.values() for r in rows
                 if r["stat_desc"]}
    committed = committed_batch_ids(table_name, batch_ids) if batch_ids else set()
    if committed:
        for conn_id, rows in jobs_by_conn.items():
            done = [r for r in rows if r["stat_desc"] in committed]
            if done:
                result["to_done"][conn_id] = [_pk_of(r) for r in done]
                jobs_by_conn[conn_id] = [r for r in rows
                                         if r["stat_desc"] not in committed]

    # ── ③ 적재 대상 구성 ──────────────────────────────────────────────────
    # DB별 결과를 합쳐 오래된 것부터 세운다 (append와 같은 순서). 항목에 conn_id를
    # 함께 두는 이유는 합치고 나면 어느 DB에서 왔는지 잃어버리기 때문이다.
    candidates = [(row["ts"], row, conn_id)
                  for conn_id, rows in jobs_by_conn.items() for row in rows]
    candidates.sort(key=lambda c: c[0])   # DB별로 정렬돼 있어도 합치면 깨진다

    for _, row, conn_id in candidates:
        result["files"] += _param_files(row["param"])  # row 1건에 파일 여러 개 가능
        result["to_mark"].setdefault(conn_id, []).append(_pk_of(row))

    if not result["files"]:
        # 조회가 비었거나 전부 영수증 정정으로 빠진 경우.
        # 후자에서 fetched_full이 살아 있어도 loop를 돌지 않는데, 정정된 건은
        # DONE으로 확정돼 다시 조회되지 않으므로 다음날 실행이 나머지를 이어받는다.
        return result

    # ── ④ 재처리 DAG 자신이 쓸 값 (부모와 무관) ──────────────────────────
    #   ts_min/ts_max  이번에 적재한 데이터의 시간 범위. compaction_targets가
    #                  이 범위만 Compaction하도록 기존 DAG에 넘긴다 (설계 6.3)
    #   has_more       상한에 걸려 못 가져온 대상이 DB에 남았는가.
    #                  next_loop이 이 값으로 재trigger 여부를 정한다 (설계 5.5)
    ti.xcom_push(key="reprocess", value={
        "ts_min": candidates[0][0],    # ts 오름차순이고 전부 담았으므로 양 끝이 범위
        "ts_max": candidates[-1][0],
        "has_more": fetched_full,
    })

    result["batch_id"] = f"{run_id}_{table_name}"  # 배치당 1개 = 커밋 1회 = 영수증 1개
    return result


def _pk_of(row: dict) -> tuple:
    """row → 복합키 값 tuple. 상태 UPDATE 인자로 그대로 넘긴다."""
    return tuple(row[c] for c in PK_COLUMNS)


def _param_files(param: str) -> list[dict]:
    """param(VARCHAR2 JSON) → 파일 목록. row 1건에 파일이 여러 개일 수 있다.

    형태: {"files": [{"file_path": ..., "size": ...}, ...]}
    목록을 그대로 부모의 파일 목록 함수에 넘기므로 여기서 가공하지 않는다.
    TODO(연결): 키 이름을 부모 파싱 함수가 기대하는 형태와 대조할 것.
    """
    return json.loads(param)["files"]
