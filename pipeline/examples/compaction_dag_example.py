"""Compaction DAG에 `tables` 필터를 넣는 방법 — **기존 파일 변경 예시**.

⚠ 배포용 파일이 아니다. 여기 있는 내용을 **기존 daily/hourly Compaction DAG 파일에**
   반영하면 된다. 새로 만드는 파일은 DAG(`pipeline/dags/iceberg_reprocess.py`)
   하나뿐이다.

왜 params 선언만으로는 안 되는가
  기존 DAG은 테이블 Enum을 파싱 시점에 loop해서 SparkKubernetesOperator를 테이블
  수만큼 만들고 chain()으로 직렬 연결한다. `params`는 DagRun이 생겨야 값이 정해지므로
  이 구조에서는 어떤 조건문을 넣어도 선택 결과를 반영할 수 없다. task 자체를
  런타임에 만들어야 한다 — dynamic task mapping.

변경점은 세 곳
  ① params에 `tables` 추가 (multi-select UI는 `examples`가 만든다)
  ② get_time task를 compaction_specs로 흡수 — params만 읽으므로 별도 task일 이유가 없다
  ③ for 루프 + chain()을 mapped task 하나로 교체

이 파일은 daily 기준이다. hourly는 `target_dt`(`format="date"`) 대신
`start_time`/`end_time`(`format="date-time"`)을 쓰며, compaction_specs가 담는 값만
다르고 구조는 동일하다.
"""

from airflow.sdk import Param, dag, task

# 기존 파일에 이미 있는 것 (재사용)
#   IcebergTable                    테이블 Enum. get_name(), config 제공
#   SparkKubernetesOperator         커스텀 operator (arguments, executor 인자)
#   DriverAndExecutor               executor 인자에 넘기는 커스텀 객체
#   COM_TARGET_FILE_SIZE_BYTES      Compaction 대상 파일 크기

TABLE_NAMES = [t.get_name() for t in IcebergTable]  # noqa: F821


# --- DAG ----------------------------------------------------------------------

@dag(
    dag_id=...,                  # TODO(연결) 기존 dag_id 유지
    schedule="0 2 * * *",        # daily: 00:35 → 02:00 (설계 6.2). hourly는 15 * * * * 유지
    params={
        "target_dt": Param(None, type=["null", "string"], format="date"),
        # default는 실제 값 — 정기 실행은 UI를 안 거치므로 이 값이 곧 전체 처리를 뜻한다.
        # examples는 값에 관여하지 않고 multi-select UI만 만든다 (`items`로는 안 나온다).
        "tables": Param(default=TABLE_NAMES, type="array", examples=TABLE_NAMES),
    },
    # ...기존 dag 인자 그대로...
)
def dag():

    @task
    def compaction_specs(params=None) -> list[dict]:
        """선택된 테이블의 operator 인자를 만든다. dict 1개 = 복사본 1개.

        반환값은 XCom을 거치므로 원시 타입만 담는다. DriverAndExecutor는 여기서
        만들 수 없고, 아래 map()이 XCom을 건넌 뒤에 만든다.

        인자를 더 넘겨야 하면 이 dict에 키를 추가한다 — 키가 곧 operator 인자명이다.
        단 `task_id`, `queue`, `pool`은 확장 대상이 아니라 partial에만 둘 수 있다.

        기존 get_time이 하던 일(param 날짜 확인 → 없으면 기본값 → YYYY-MM-DD 포맷)을
        여기서 한다. params는 어느 task에서든 받을 수 있어 별도 task일 이유가 없다.
        """
        target_time = params.get("target_dt") or ...  # TODO(연결) 기존 get_time의 기본값 로직
        selected = set(params["tables"])
        return [
            {
                # arguments[0]은 테이블명 — map_index_template이 이 위치를 참조한다
                "arguments": [
                    table.get_name(),
                    str(COM_TARGET_FILE_SIZE_BYTES),  # noqa: F821
                    target_time,
                    str(table.config.com_max_concurrent_file_group),
                ],
                "instances": str(table.config.com_num_executor),
            }
            for table in IcebergTable  # noqa: F821
            if table.get_name() in selected
        ]

    # 기존: for table in IcebergTable → operator 생성 → tasks.append → chain(get_time(), *tasks)
    SparkKubernetesOperator.partial(  # noqa: F821
        task_id="compact",
        max_active_tis_per_dagrun=1,                   # chain()이 하던 직렬 실행 역할
        map_index_template="{{ task.arguments[0] }}",   # UI map index를 테이블명으로
        # ...기존 루프에서 table이 등장하지 않던 인자 전부 그대로...
    ).expand_kwargs(
        # map()은 XCom을 건넌 뒤 실행된다 — 커스텀 객체는 여기서만 만들 수 있다.
        # @task를 넘기면 Airflow가 거부한다.
        compaction_specs().map(
            lambda spec: {
                "arguments": spec["arguments"],
                "executor": DriverAndExecutor(instances=spec["instances"]),  # noqa: F821
            }
        )
    )


dag()


# --- 옮기기 전 확인 -----------------------------------------------------------
#
# ① get_time의 XCom을 SKO 말고 다른 task가 참조하는가
#    알림·로깅 등에서 {{ ti.xcom_pull(task_ids="get_time") }}를 쓰고 있으면 그쪽이 깨진다.
#
# ② arguments의 첫 원소가 테이블명인가
#    map_index_template이 위치로 참조한다. 순서가 다르면 인덱스를 맞춘다.
#    커스텀 operator에 테이블명 전용 인자가 있다면 {{ task.<그 인자> }}가 더 안전하다.
#
# ③ max_active_tis_per_dagrun=1을 빼지 않았는가
#    복사본은 기본이 동시 실행이다. 빼면 Spark job이 테이블 수만큼 한꺼번에 뜬다.
#
# ④ arguments 안에 Jinja를 남기지 않았는가
#    XCom에서 온 expand 값은 template_fields에 있어도 렌더링을 건너뛴다
#    (expandinput.py의 resolved_oids → templater.py의 `if id(value) in oids: return value`).
#    기존 '{{ ti.xcom_pull(task_ids="get_time") }}'는 target_time 실제 값으로 대체했다.
