"""
Stage 파일 -> Snowflake 테이블 적재 DAG (Snowpipe REST API insertFiles 방식)

  build_items ─> prepare_target (테이블별 mapped) ─> ingest_files (테이블별 mapped) ─> report

선행: pipeline_oracle_to_s3 DAG 가 스테이지에 data 파일을 올려 둔 상태여야 한다.
      Snowflake 쪽 파이프/프로시저는 snowflake/setup/snowflake_pipe_setup.sql (프로젝트 루트) 로 만든다.

1. build_items    : snowflake_load.tables 대상별 적재 항목 생성.
                    PERIOD 는 pipeline_oracle_to_s3 와 동일한 규칙(resolve_base_dates)으로 base_date 목록 계산.
2. prepare_target : 프로시저 호출로 대상 테이블 정리 (FULL: TRUNCATE, PERIOD: base_date 행 DELETE).
3. ingest_files   : 스테이지 LIST 로 이번 적재 대상 파일 목록을 만들고, REST insertFiles 로 그 파일만 파이프에 전달.
                    COPY_HISTORY 로 파일별 적재 결과가 나올 때까지 대기하며 실패 파일을 확인한다.
4. report         : 하나라도 실패하면 DAG 를 실패 처리.

REST 호출에는 snowflake-ingest 패키지와 키페어 인증이 필요하다. 키는 Airflow Connection(snowflake) extra 에 둔다.
  extra: account, private_key_content(PEM) 또는 private_key_file(경로), (암호화 키면 password 에 passphrase)
  선택: ingest_host (기본 {account}.snowflakecomputing.com)

수동 실행 시 Params (pipeline_oracle_to_s3 와 같은 값을 쓴다)
  base_date_from / base_date_to : PERIOD 테이블 범위 (YYYYMMDD). 비우면 auto-load-range 사용
"""
import re
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, task

from common import etl_utils as eu
from common.config import load_config, load_xml_config

CFG = load_config("pipeline_config.yaml")
TABLES = load_xml_config(CFG["source"]["table_config_file"])
eu.validate_table_config(TABLES)

SF = CFG["snowflake_load"]
TZ = ZoneInfo(CFG["defaults"]["timezone"])
MAX_PARALLEL = CFG["defaults"]["max_parallel"]

# 프로시저/파이프/테이블 이름은 바인드가 불가능한 곳이 있어 import 시점에 형식을 검증한다.
_IDENT = re.compile(r"^[A-Za-z0-9_$]+(\.[A-Za-z0-9_$]+){0,2}$")
_PIPE_FQN = re.compile(r"^[A-Za-z0-9_$]+\.[A-Za-z0-9_$]+\.[A-Za-z0-9_$]+$")


def _validate_snowflake_config() -> None:
    if not _IDENT.match(SF["procedure"]):
        raise ValueError(f"snowflake_load.procedure 형식 오류: {SF['procedure']!r}")
    if not 1 <= int(SF.get("ingest_batch_size", 1000)) <= 5000:
        raise ValueError("snowflake_load.ingest_batch_size 는 1~5000 이어야 합니다.")
    for tid, t in SF["tables"].items():
        if tid not in TABLES["data"]:
            raise ValueError(f"snowflake_load.tables 의 {tid} 가 oracle_table.xml data 에 없습니다.")
        if not _IDENT.match(str(t.get("target_table", ""))):
            raise ValueError(f"snowflake_load.tables.{tid}.target_table 형식 오류: {t.get('target_table')!r}")
        if not _PIPE_FQN.match(str(t.get("pipe", ""))):
            raise ValueError(
                f"snowflake_load.tables.{tid}.pipe 는 DB.SCHEMA.PIPE 형식이어야 합니다: {t.get('pipe')!r}"
            )


_validate_snowflake_config()


def _hook():
    # provider 가 없는 환경에서도 DAG import 자체는 되도록 지연 import
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=CFG["target"]["connection_id"])


def _ingest_manager(hook, pipe: str):
    """Connection extra 의 키페어로 Snowpipe REST 클라이언트(SimpleIngestManager)를 만든다."""
    # 지연 import: 패키지가 없는 환경에서도 DAG 파일 import 자체는 되도록 함수 안에서 가져온다.
    from cryptography.hazmat.primitives import serialization  # 비공개키(PEM) 읽기/변환용
    from snowflake.ingest import SimpleIngestManager  # pip: snowflake-ingest (REST 요청을 대신 보내는 SDK 클라이언트)

    # 접속 정보는 코드에 쓰지 않고 Airflow Connection 에서 읽는다 (보안 규칙: 자격증명 하드코딩 금지).
    conn = hook.get_connection(hook.snowflake_conn_id)
    extra = conn.extra_dejson  # Connection 의 extra(JSON) 를 dict 로 변환

    # extra 키가 "account" 또는 "extra__snowflake__account" 두 형태로 저장될 수 있어 둘 다 찾는다.
    def pick(key: str):
        return extra.get(key) or extra.get(f"extra__snowflake__{key}")

    # 계정 식별자는 extra 에서, 사용자 이름은 Connection 의 login 에서 가져온다.
    account, user = pick("account"), conn.login
    if not account or not user:
        raise ValueError("snowflake Connection 에 extra.account 와 login(사용자)이 필요합니다.")

    # 비공개키(.p8)를 가져온다: 내용을 직접 넣은 경우(private_key_content) 우선, 없으면 파일 경로(private_key_file).
    pem = pick("private_key_content")
    if pem:
        pem_bytes = pem.encode()
    elif pick("private_key_file"):
        with open(pick("private_key_file"), "rb") as f:
            pem_bytes = f.read()
    else:
        raise ValueError("snowflake Connection extra 에 private_key_content 또는 private_key_file 이 필요합니다.")

    # 키에 암호(passphrase)를 걸었다면 Connection 의 password 필드에 둔다. 없으면 None.
    passphrase = conn.password.encode() if conn.password else None
    key = serialization.load_pem_private_key(pem_bytes, password=passphrase)
    # SDK 는 "암호 없는 PKCS8 PEM 문자열" 을 요구하므로, 읽은 키를 그 형식으로 다시 변환한다.
    plain_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()

    # 요청을 보낼 호스트. 별도 지정(ingest_host)이 없으면 {account}.snowflakecomputing.com 을 쓴다.
    host = pick("ingest_host") or f"{account}.snowflakecomputing.com"
    # 계정/호스트/사용자/파이프/키를 담은 클라이언트를 만들어 반환한다. 이 객체로 ingest_files(), get_history() 를 호출한다.
    return SimpleIngestManager(account=account, host=host, user=user, pipe=pipe, private_key=plain_pem)


def _list_relative_files(item: dict[str, Any]) -> list[str]:
    """이번 적재 대상 스테이지 파일을 LIST 하고, 파이프 FROM 경로(table prefix) 기준 상대 경로로 반환한다."""
    t = TABLES["data"][item["table_id"]]
    table_prefix = eu.resolve_prefix(CFG, "data", "full", schema=t["schema"], table=t["name"])
    adapter = eu.create_target_adapter(CFG["target"])

    prefixes = (
        [f"{table_prefix}/{bd}" for bd in item["base_dates"]]
        if item["load_type"] == "PERIOD" else [table_prefix]
    )
    rel: list[str] = []
    for p in prefixes:
        for key in adapter.list_keys(prefix=p + "/"):
            if key.endswith(".json.gz") and key.startswith(table_prefix + "/"):
                rel.append(key[len(table_prefix) + 1:])
    return sorted(set(rel))


def _wait_loaded(hook, table: str, rel_paths: list[str]) -> list[str]:
    """COPY_HISTORY 에서 모든 파일의 결과가 나올 때까지 대기하고, 실패/미완료 목록을 반환한다."""
    timeout = int(SF.get("wait_timeout_sec", 600))
    interval = int(SF.get("poll_interval_sec", 10))
    deadline = time.monotonic() + timeout
    wanted = {p.lower(): p for p in rel_paths}

    while True:
        time.sleep(interval)
        rows = hook.get_records(
            "SELECT FILE_NAME, STATUS, FIRST_ERROR_MESSAGE "
            "FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY("
            "TABLE_NAME => %s, START_TIME => DATEADD(day, -1, CURRENT_TIMESTAMP())))",
            parameters=(table,),
        )
        status: dict[str, tuple[str, str]] = {}
        for file_name, st, err in rows:
            fn = str(file_name).lower()
            for low in wanted:
                if fn.endswith(low):
                    status[low] = (st, err)

        failed = [
            f"{wanted[low]} [{st}] {err}"
            for low, (st, err) in status.items()
            if st not in ("Loaded", "Load in progress")
        ]
        pending = [wanted[low] for low in wanted if low not in status or status[low][0] == "Load in progress"]
        if failed:
            return failed
        if not pending:
            return []
        if time.monotonic() > deadline:
            return [f"대기 시간 초과({timeout}s), 미완료 {len(pending)}건: {pending[:5]}"]


@dag(
    dag_id="pipeline_snowflake_load",
    start_date=datetime(2026, 1, 1, tzinfo=TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"base_date_from": "", "base_date_to": ""},
    tags=["oracle", "snowflake", "snowpipe"],
)
def pipeline_snowflake_load():

    @task
    def build_items(**ctx):
        now = datetime.now(TZ)
        params = ctx["params"]
        date_from = (params.get("base_date_from") or "").strip() or None
        date_to = (params.get("base_date_to") or "").strip() or None

        items = []
        for tid, sf in SF["tables"].items():
            t = TABLES["data"][tid]
            base_dates = (
                eu.resolve_base_dates(t, now, date_from, date_to)
                if t["load_type"] == "PERIOD" else []
            )
            items.append({
                "table_id": tid,
                "target_table": sf["target_table"],
                "pipe": sf["pipe"],
                "load_type": t["load_type"],
                "base_dates": base_dates,
            })
        return items

    @task(max_active_tis_per_dag=MAX_PARALLEL)
    def prepare_target(item):
        """적재 전 대상 테이블 정리. 실패해도 예외를 던지지 않고 결과로 반환한다."""
        res = {**item, "stage": "PREPARE", "status": "FAILED", "error": None}
        try:
            msg = _hook().get_first(
                f"CALL {SF['procedure']}(%s, %s, %s)",
                parameters=(item["target_table"], item["load_type"], ",".join(item["base_dates"])),
            )[0]
            res.update(status="SUCCESS", procedure_result=msg)
        except Exception as e:  # noqa: BLE001
            res["error"] = f"{type(e).__name__}: {e}"[:2000]
        return res

    @task(max_active_tis_per_dag=MAX_PARALLEL)
    def ingest_files(prepared):
        """이번 실행 파일만 REST insertFiles 로 전달하고 적재 결과를 확인한다."""
        res = {**prepared, "stage": "INGEST", "status": "FAILED", "error": None, "file_count": 0}
        if prepared["status"] != "SUCCESS":
            res["error"] = f"정리 단계 실패로 건너뜀: {prepared.get('error')}"
            return res
        try:
            files = _list_relative_files(prepared)
            res["file_count"] = len(files)
            if not files:
                raise RuntimeError("스테이지에서 적재 대상 파일을 찾지 못했습니다 (추출 DAG 실행 여부/경로 확인).")

            from snowflake.ingest import StagedFile

            hook = _hook()
            mgr = _ingest_manager(hook, prepared["pipe"])
            batch = int(SF.get("ingest_batch_size", 1000))
            for i in range(0, len(files), batch):
                resp = mgr.ingest_files([StagedFile(p, None) for p in files[i:i + batch]])
                if resp.get("responseCode") != "SUCCESS":
                    raise RuntimeError(f"insertFiles 응답 이상: {resp}")

            errors = _wait_loaded(hook, prepared["target_table"], files)
            if errors:
                raise RuntimeError(f"적재 실패 {len(errors)}건: {errors[:5]}")
            res["status"] = "SUCCESS"
        except Exception as e:  # noqa: BLE001 - 다른 테이블 진행을 막지 않고 report 에서 일괄 실패 처리
            res["error"] = f"{type(e).__name__}: {e}"[:2000]
        return res

    @task(trigger_rule="all_done")
    def report(items, results):
        done = {r["table_id"] for r in results if r}
        failed = [r for r in results if r and r["status"] != "SUCCESS"]
        failed += [
            {"table_id": i["table_id"], "error": "결과 없음: 태스크가 비정상 종료되었습니다"}
            for i in items if i["table_id"] not in done
        ]
        print(f"snowflake load: total={len(items)} failed={len(failed)}")
        for r in results:
            print(r)
        if failed:
            raise AirflowFailException(
                f"Snowflake 적재 실패 {len(failed)}건: "
                f"{[(f['table_id'], f.get('error')) for f in failed][:10]}"
            )

    items = build_items()
    prepared = prepare_target.expand(item=items)
    results = ingest_files.expand(prepared=prepared)
    report(items, results)


dag_obj = pipeline_snowflake_load()

if __name__ == "__main__":
    dag_obj.test()
