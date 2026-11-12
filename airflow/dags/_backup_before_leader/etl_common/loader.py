"""Oracle -> RustFS 적재 로직 (data / log / meta / translog)."""
from datetime import datetime

import pandas as pd
from airflow.providers.oracle.hooks.oracle import OracleHook

from etl_common.naming import KST, base_dates, part_filename
from etl_common.storage import delete_prefix, df_to_jsonl_gz, get_s3, obj_to_json_gz, put_bytes

# meta 뷰에 owner 필터를 걸 때 쓰는 컬럼 (기본은 OWNER)
META_OWNER_COLUMN = {"ALL_IND_COLUMNS": "TABLE_OWNER"}


def build_log(df, table, ctx, base_date, part_no, data_key):
    """data 파일 하나에 대응하는 검증용 통계 (건수 / 컬럼별 NULL 수 / 설정된 컬럼의 SUM)"""
    missing = [c for c in table["sum_columns"] if c not in df.columns]
    if missing:
        raise ValueError(f"sum-columns 가 조회 결과에 없습니다: {missing}")
    return {
        "job_id": ctx["job_id"],
        "schema": table["schema"],
        "table": table["name"],
        "base_date": base_date,
        "part_no": part_no,
        "data_file": data_key,
        "row_count": int(len(df)),
        "null_counts": {c: int(n) for c, n in df.isna().sum().items()},
        "sums": {c: float(pd.to_numeric(df[c], errors="coerce").sum()) for c in table["sum_columns"]},
    }


def _prefixes(cfg, ctx, table, base_date):
    """data(또는 meta) / log 저장 경로 prefix"""
    src = cfg["name"]
    if table["kind"] == "meta":
        return f"{src}/meta/{table['name']}/{ctx['run_date']}/", None
    tail = f"{table['schema']}/{table['name']}" + (f"/{base_date}" if base_date else "")
    return f"{src}/data/{tail}/", f"{src}/log/{tail}/"


def _query(cfg, table, base_date):
    """실행할 SQL 과 바인드 변수. meta 는 owner 설정이 있을 때만 필터를 붙인다."""
    sql, params = table["sql"], None
    if base_date:
        params = {"BASE_DATE": base_date}
    if table["kind"] == "meta" and cfg["meta_owner"]:
        owner_col = META_OWNER_COLUMN.get(table["name"], "OWNER")
        sql += f" WHERE {owner_col} = :OWNER"
        params = {"OWNER": cfg["meta_owner"]}
    return sql, params


def load_unit(oracle, s3, cfg, ctx, table, base_date=None):
    """테이블(+기준일자) 하나를 조회해 part 파일로 분할 저장한다."""
    bucket = cfg["bucket"]
    data_prefix, log_prefix = _prefixes(cfg, ctx, table, base_date)

    # 재실행: 기존 파일 삭제 후 적재
    delete_prefix(s3, bucket, data_prefix)
    if log_prefix:
        delete_prefix(s3, bucket, log_prefix)

    sql, params = _query(cfg, table, base_date)
    rows = parts = 0
    for part_no, df in enumerate(oracle.get_df_by_chunks(sql, parameters=params, chunksize=cfg["split_rows"]), start=1):
        df.columns = [c.lower() for c in df.columns]      # 컬럼명 소문자 통일
        fname = part_filename(table["schema"], table["name"], ctx["job_id"], ctx["gen_ts"], part_no, base_date)
        data_key = data_prefix + fname
        put_bytes(s3, bucket, data_key, df_to_jsonl_gz(df))
        if log_prefix:
            log = build_log(df, table, ctx, base_date, part_no, data_key)
            put_bytes(s3, bucket, log_prefix + fname, obj_to_json_gz(log))
        rows += len(df)
        parts = part_no
        print(f"uploaded s3://{bucket}/{data_key} ({len(df)} rows)")
    return {"base_date": base_date, "rows": rows, "parts": parts}


def write_translog(s3, cfg, ctx, table, status, started, units, error):
    """조회/적재 결과 기록. 삭제 없이 누적(append-only). 후속 작업(Snowpipe)의 시작점."""
    record = {
        "job_id": ctx["job_id"], "generation_ts": ctx["gen_ts"], "run_date": ctx["run_date"],
        "source": cfg["name"], "kind": table["kind"], "schema": table["schema"], "table": table["name"],
        "load_type": table["load_type"], "status": status,
        "started_at": started, "ended_at": datetime.now(KST).isoformat(),
        "units": units, "total_rows": sum(u["rows"] for u in units), "error": error,
    }
    key = (f"{cfg['name']}/translog/{status}/{ctx['run_date']}/"
           + part_filename(table["schema"], table["name"], ctx["job_id"], ctx["gen_ts"], 1))
    put_bytes(s3, cfg["bucket"], key, obj_to_json_gz(record))


def run_table(cfg, ctx, table):
    """테이블 하나 처리 (PERIOD 는 기준일자별 반복). 성공/실패 모두 translog 에 남긴다."""
    s3 = get_s3(cfg["target_conn"])
    oracle = OracleHook(oracle_conn_id=cfg["source_conn"])
    started, units, status, error = datetime.now(KST).isoformat(), [], "success", None
    try:
        if table["load_type"] == "PERIOD":
            for d in base_dates(ctx["run_date"], table["range_days"]):
                units.append(load_unit(oracle, s3, cfg, ctx, table, d))
        else:
            units.append(load_unit(oracle, s3, cfg, ctx, table))
    except Exception as exc:
        status, error = "fail", f"{type(exc).__name__}: {exc}"
        raise
    finally:
        try:
            write_translog(s3, cfg, ctx, table, status, started, units, error)
        except Exception as log_exc:                       # translog 실패가 원래 오류를 가리지 않게
            print(f"translog 기록 실패: {log_exc}")
    return {"table": f"{table['schema']}.{table['name']}", "units": units}
