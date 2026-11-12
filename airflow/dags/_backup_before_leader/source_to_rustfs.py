"""config 기반 Oracle -> RustFS(S3) 적재 DAG.

설계: .claude/docs/CreateDag.md
- airflow/dags/config/*.xml 하나당 DAG 하나 (dag_id = {name}_to_rustfs)
- data / log / meta / translog 를 json.gz(JSON Lines)로 저장
- Spark 없이 SQL 청크 조회(split-rows 단위)로 메모리 사용을 제한
- 실제 로직은 etl_common/ 모듈에 있고, 이 파일은 DAG 구조만 정의한다.
"""
import json
import re
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Param, dag, task

from etl_common.config import load_config
from etl_common.loader import run_table
from etl_common.naming import KST, make_generation_ts, make_job_id
from etl_common.storage import ensure_bucket, get_s3

CONFIG_DIR = Path(__file__).parent / "config"

DEFAULT_ARGS = {
    "retries": 1,                               # 재실행은 삭제 후 적재라 멱등
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=1),    # 멈춘 태스크 방지
}


def build_dag(cfg):
    @dag(
        dag_id=f"{cfg['name']}_to_rustfs",
        start_date=datetime(2026, 10, 1),
        schedule=None,
        catchup=False,
        default_args=DEFAULT_ARGS,
        max_active_runs=1,         # 재실행 시 대상 경로를 삭제하므로 동시 실행 금지
        max_active_tasks=2,        # 동시 조회 수를 제한해 메모리 사용을 억제
        params={"run_date": Param("", type="string",
                                  description="실행일자 YYYYMMDD. 비우면 오늘(KST). 기준일자 계산과 meta/translog 경로에 사용")},
        tags=["lab", "config-driven"],
    )
    def source_to_rustfs():

        @task
        def prepare(run_id=None, params=None):
            """DAG 실행당 1회: JobID, Generation timestamp, 실행일자 확정 + 버킷 준비"""
            now = datetime.now(KST)
            run_date = (params or {}).get("run_date") or now.strftime("%Y%m%d")
            if not re.fullmatch(r"\d{8}", run_date):
                raise ValueError(f"run_date 형식 오류 (YYYYMMDD): {run_date}")
            ensure_bucket(get_s3(cfg["target_conn"]), cfg["bucket"])
            ctx = {"job_id": make_job_id(run_id), "gen_ts": make_generation_ts(now), "run_date": run_date}

            # 이번 실행에 쓰이는 설정과 공통값을 로그로 남긴다 (cfg 에는 Connection ID 만 있고 비밀값은 없음)
            print("cfg =", json.dumps(cfg, ensure_ascii=False, indent=2))
            print("ctx =", json.dumps(ctx, ensure_ascii=False, indent=2))
            return ctx

        @task
        def load_data(table, ctx):
            return run_table(cfg, ctx, table)

        @task
        def load_meta(table, ctx):
            return run_table(cfg, ctx, table)

        ctx = prepare()
        load_data.partial(ctx=ctx).expand(table=cfg["tables"])
        load_meta.partial(ctx=ctx).expand(table=cfg["meta_tables"])

    return source_to_rustfs()


for _path in sorted(CONFIG_DIR.glob("*.xml")):
    _cfg = load_config(_path)
    globals()[f"dag_{_cfg['name']}"] = build_dag(_cfg)
