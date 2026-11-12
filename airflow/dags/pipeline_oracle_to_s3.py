"""
Configurable SQL Source -> Object Storage Target DAG

  prepare ─┬─> build_meta_items ─> load_metadata (뷰별 mapped) ──────┐
           └─> build_data_items ─> load_table (테이블/기준일별 mapped) ─┴─> write_translog

1. 메타데이터 : <metadata>/<table> 의 SQL 실행 -> meta 경로 적재
2. 테이블 데이터/로그 : <data>/<table> 의 SQL 실행 -> data 경로 적재 + 분할 파일별 통계를 log 경로 적재
3. translog : 위 결과 전체를 DAG 실행당 1파일로 적재 (전부 성공 -> success, 1건이라도 실패 -> fail 경로)

수동 실행 시 Params
  base_date_from / base_date_to : PERIOD 테이블 재처리 범위 (YYYYMMDD). 비우면 auto-load-range 사용
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, task

from common import etl_utils as eu
from common.config import load_config, load_xml_config

CFG = load_config("pipeline_config.yaml")        # 공통 설정 (YAML)
#TABLES = load_xml_config(CFG.get("table_config_file", "oracle_table.xml"))   # 테이블/SQL (XML)
TABLES = load_xml_config(CFG["source"]["table_config_file"])   # 테이블/SQL (XML)
eu.validate_table_config(TABLES)
DAG_ID = "pipeline_oracle_to_s3"
MAX_PARALLEL = CFG["defaults"]["max_parallel"]          # 소스/타겟 동시 처리 건수 (mapped task 별)

def _target_adapter():
    """YAML의 target.type에 맞는 타겟 어댑터."""
    return eu.create_target_adapter(CFG["target"])


def _source_conn_factory():
    """YAML의 source.system_type에 맞는 DB-API 연결 팩토리."""
    return eu.create_source_connection_factory(CFG["source"])


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo(CFG["defaults"]["timezone"])),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"base_date_from": "", "base_date_to": ""},
    tags=[str(CFG["source"]["name"]).lower(), str(CFG["target"]["type"]).lower(), "etl"],
)
def pipeline_to_target():

    @task
    def prepare(**ctx):
        """실행 공통 정보. generation_ts 는 DAG_RUN 단위로 1회만 생성해서 모든 태스크가 공유한다."""
        now = datetime.now(ZoneInfo(CFG["defaults"]["timezone"]))
        return {
            "dag_id": ctx["ti"].dag_id,
            "run_id": ctx["run_id"],
            "job_id": eu.make_job_id(ctx["run_id"], CFG),
            "generation_ts": now.strftime(CFG["defaults"]["generation_timestamp"]["format"]),
            "run_date": now.strftime("%Y%m%d"),
            "now": now.isoformat(),
        }

    # .expand() 는 커스텀 키 XCom 을 입력으로 받을 수 없으므로,
    # 매핑 대상 목록은 각각 태스크의 반환값(return_value)으로 만든다.
    @task
    def build_meta_items():
        return [{"table_id": tid} for tid in TABLES["metadata"]]

    @task
    def build_data_items(run, **ctx):
        now = datetime.fromisoformat(run["now"])
        params = ctx["params"]
        date_from = (params.get("base_date_from") or "").strip() or None
        date_to = (params.get("base_date_to") or "").strip() or None

        items = []
        for tid, t in TABLES["data"].items():
            if t["load_type"] == "PERIOD":
                for base_date in eu.resolve_base_dates(t, now, date_from, date_to):
                    items.append({"table_id": tid, "base_date": base_date})
            else:
                items.append({"table_id": tid, "base_date": None})
        return items

    @task(max_active_tis_per_dag=MAX_PARALLEL)
    def load_metadata(item, run):
        return eu.run_metadata_load(
            CFG, TABLES["metadata"][item["table_id"]], run, _target_adapter(), _source_conn_factory()
        )

    @task(max_active_tis_per_dag=MAX_PARALLEL)
    def load_table(item, run):
        return eu.run_table_load(
            CFG, TABLES["data"][item["table_id"]], item["base_date"], run,
            _target_adapter(), _source_conn_factory()
        )

    @task(trigger_rule="all_done")
    def write_translog(run, meta_items, data_items, meta_results, data_results):
        if not run:
            raise AirflowFailException("prepare 태스크 실패: 실행 정보가 없습니다.")

        results = [r for r in list(meta_results or []) + list(data_results or []) if r]
        # 태스크가 비정상 종료되어 결과가 없는 건은 FAILED 로 간주해 translog 에 포함
        results += eu.find_missing_results(CFG, TABLES, meta_items, data_items, results)

        summary = eu.write_translog(_target_adapter(), CFG, run, results)
        print(f"translog: {summary}")

        if summary["failed"]:
            raise AirflowFailException(
                f"적재 실패 {summary['failed']}건 / 전체 {summary['total']}건: "
                f"{summary['failed_items']}"
            )
        return summary

    run = prepare()
    meta_items = build_meta_items()
    data_items = build_data_items(run)

    meta_res = load_metadata.partial(run=run).expand(item=meta_items)
    data_res = load_table.partial(run=run).expand(item=data_items)

    write_translog(run, meta_items, data_items, meta_res, data_res)


dag_obj = pipeline_to_target()

if __name__ == "__main__":
    # IDE 디버거 실행용 (스케줄러 없이 1회 실행). Airflow 가 DAG 로 로드할 때는 실행되지 않는다.
    dag_obj.test()