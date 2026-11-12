"""
Source SQL -> Target Storage 적재 공통 로직

- Airflow에 의존하지 않는다. DB-API connection과 target adapter는 호출자가 주입한다.
- 경로, 파일명, 삭제 규칙은 모두 pipeline_config.yaml 의 storage 섹션을 따른다.
- 데이터 유형별 규칙 (storage.paths / filenames / delete_policy)
    data     : data/{schema}/{table}[/{base_date}]      삭제 후 적재
    log      : log/{schema}/{table}[/{base_date}]       삭제 후 적재 (분할된 data 파일별 통계)
    meta     : meta/{metadata_table}/{run_date}         삭제 후 적재
    translog : translog/{success|fail}/{run_date}       DAG 실행당 1파일, 기존 파일 유지
"""
from __future__ import annotations

import gzip
import io
import json
import logging
import os
import re
import tempfile
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Callable
from zoneinfo import ZoneInfo

LOG = logging.getLogger(__name__)

DELETE_BEFORE_LOAD = "DELETE_EXISTING_FILES_BEFORE_LOAD"
KEEP_EXISTING = "KEEP_EXISTING_FILES"


class S3TargetAdapter:
    """S3 호환 타겟을 위한 어댑터. S3Hook을 공통 ETL 인터페이스로 감싼다."""
    def __init__(self, hook: Any, bucket: str):
        self.hook = hook
        self.bucket = bucket

    def list_keys(self, *, prefix: str) -> list[str]:
        return self.hook.list_keys(bucket_name=self.bucket, prefix=prefix) or []

    def delete_keys(self, keys: list[str], prefix: str) -> None:
        if keys:
            self.hook.delete_objects(bucket=self.bucket, keys=keys)

    def put_bytes(self, key: str, body: bytes, content_type: str = "application/json",
                  content_encoding: str | None = "gzip") -> None:
        extra: dict[str, Any] = {"ContentType": content_type}
        if content_encoding:
            extra["ContentEncoding"] = content_encoding
        self.hook.get_conn().put_object(
            Bucket=self.bucket, Key=key, Body=body, **extra
        )


class SnowflakeStageTargetAdapter:
    """Snowflake 내부 Stage에 JSON.GZ 파일을 PUT하는 타겟 어댑터."""
    def __init__(self, hook: Any, stage: str):
        self.hook = hook
        self.stage = stage.rstrip("/")

    def _conn(self):
        return self.hook.get_conn()

    def list_keys(self, *, prefix: str) -> list[str]:
        stage_path = f"{self.stage}/{prefix.strip('/')}".rstrip("/")
        cur = self._conn().cursor()
        try:
            cur.execute(f"LIST {stage_path}/")
            #LOG.info(f"LIST {stage_path}/")
            rows = cur.fetchall()
            base = self.stage.lstrip("@") + "/"
            keys = []
            for row in rows:
                name = str(row[0])
                # LIST 결과는 보통 @DB.SCHEMA.STAGE/path/file 형태다.
                marker = self.stage.lstrip("@") + "/"
                idx = name.upper().find(marker.upper())
                keys.append(name[idx + len(marker):] if idx >= 0 else name.rsplit("/", 1)[-1])

            return keys
        finally:
            cur.close()

    def delete_keys(self, keys: list[str], prefix: str) -> None:
        if not keys:
            return
        # REMOVE는 prefix별 실행으로 묶지 않고 개별 파일을 제거한다.
        stage_path = f"{self.stage}/{prefix.strip('/')}".rstrip("/")

        cur = self._conn().cursor()
        try:
            #LOG.info(f"delete_keys : {self.stage}/")
            #LOG.info(f"delete_keys_stage_path : {stage_path}/")
            LOG.info(f"REMOVE {stage_path}/")
            cur.execute(f"REMOVE {stage_path}/")
            #for key in keys:
            #    LOG.info(f"REMOVE {self.stage}/{key.lstrip('/')}")
            #    cur.execute(f"REMOVE {self.stage}/{key.lstrip('/')}")
        finally:
            cur.close()

    def put_bytes(self, key: str, body: bytes, content_type: str = "application/json",
                  content_encoding: str | None = "gzip") -> None:
        # PUT은 로컬 파일을 필요로 하므로 임시 디렉터리에 파일을 만든 뒤 내부 Stage로 업로드한다.
        parent, filename = os.path.split(key.strip("/"))
        with tempfile.TemporaryDirectory(prefix="airflow_snowflake_put_") as tmpdir:
            local_path = os.path.join(tmpdir, filename)
            with open(local_path, "wb") as f:
                f.write(body)
            destination = self.stage
            if parent:
                destination += "/" + parent
            cur = self._conn().cursor()
            try:
                uri = "file://" + local_path
                cur.execute(
                    f"PUT '{uri}' {destination} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                )
            finally:
                cur.close()


def create_source_connection_factory(source_cfg: dict[str, Any]) -> Callable[[], Any]:
    """source.system_type에 따라 Airflow provider Hook을 선택하는 연결 팩토리."""
    system_type = str(source_cfg.get("system_type", source_cfg.get("type", ""))).upper()
    connection_id = source_cfg["connection_id"]

    def factory():
        if system_type in {"ORACLE", "ORACLEDB"}:
            from airflow.providers.oracle.hooks.oracle import OracleHook
            return OracleHook(oracle_conn_id=connection_id).get_conn()
        if system_type in {"POSTGRES", "POSTGRESQL"}:
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            return PostgresHook(postgres_conn_id=connection_id).get_conn()
        if system_type == "MYSQL":
            from airflow.providers.mysql.hooks.mysql import MySqlHook
            return MySqlHook(mysql_conn_id=connection_id).get_conn()
        if system_type in {"MSSQL", "SQLSERVER"}:
            from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
            return MsSqlHook(mssql_conn_id=connection_id).get_conn()
        if system_type in {"SNOWFLAKE", "SNOWFLAKE_SQL"}:
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
            return SnowflakeHook(snowflake_conn_id=connection_id).get_conn()
        raise ValueError(
            f"지원하지 않는 source.system_type={system_type!r}. "
            "ORACLE, POSTGRESQL, MYSQL, MSSQL, SNOWFLAKE 중 하나를 지정하세요."
        )
    return factory


def create_target_adapter(target_cfg: dict[str, Any]) -> Any:
    """target.type에 맞는 타겟 어댑터를 생성한다."""
    target_type = str(target_cfg.get("type", "")).upper()
    if target_type in {"S3", "S3_COMPATIBLE", "RUSTFS", "MINIO"}:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id=target_cfg["connection_id"])
        return S3TargetAdapter(hook, target_cfg["bucket"])
    if target_type in {"SNOWFLAKE_STAGE", "SNOWFLAKE"}:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id=target_cfg["connection_id"])
        stage = target_cfg.get("stage")
        if not stage or not str(stage).startswith("@"):
            raise ValueError("Snowflake 타겟에는 target.stage를 @DB.SCHEMA.STAGE 형식으로 지정해야 합니다.")
        if not re.fullmatch(r"@[A-Za-z0-9_$#.]+(?:/[A-Za-z0-9_$#./-]+)?", str(stage)):
            raise ValueError(f"안전하지 않은 Snowflake Stage 경로: {stage!r}")
        return SnowflakeStageTargetAdapter(hook, str(stage))
    raise ValueError(
        f"지원하지 않는 target.type={target_type!r}. "
        "지원 타겟: S3, S3_COMPATIBLE, RUSTFS, MINIO, SNOWFLAKE_STAGE."
    )

# True 이면 객체에 Content-Encoding: gzip 헤더를 설정한다.
# RustFS 콘솔 미리보기가 gzip을 풀어서 보여주려면 필요하다. (S3Hook.load_bytes 는 헤더 지정 불가)
SET_CONTENT_ENCODING = True

# True 이면 S3 경로(prefix)와 파일명을 모두 소문자로 만든다. (예: oracle/data/sh/promotions)
# 파일 내용(컬럼명, 통계 레코드의 schema/table 값)은 변경하지 않는다.
LOWERCASE_S3_KEY = True

# 현재 DAG가 값을 넣어 줄 수 있는 바인드 변수
SUPPORTED_BINDS = {"BASE_DATE"}

_SEGMENT = re.compile(r"^[A-Za-z0-9_$#.\-]+$")
_YMD = re.compile(r"^\d{8}$")


# ======================================================================
# 설정 검증
# ======================================================================
def validate_table_config(tables: dict[str, Any]) -> None:
    """XML에서 읽은 테이블 설정을 DAG import 시점에 검증한다."""

    for tid, t in tables["data"].items():
        unsupported = set(t["bind_variables"]) - SUPPORTED_BINDS
        if unsupported:
            raise ValueError(f"[data/{tid}] 지원하지 않는 바인드 변수: {sorted(unsupported)}")
        if t["load_type"] == "PERIOD" and (t["range_unit"] not in ("D", "M", "Y") or not t["range_value"]):
        #if t["load_type"] == "PERIOD" and (t["range_unit"] != "D" or not t["range_value"]):
            raise ValueError(
                f"[data/{tid}] PERIOD 적재는 auto-load-range가 필요합니다 (예: 3D): "
                f"{t['auto_load_range']!r}"
            )
        if not t["schema"]:
            raise ValueError(f"[data/{tid}] <schema>가 비어 있습니다.")

    for tid, t in tables["metadata"].items():
        if t["bind_variables"]:
            raise ValueError(f"[metadata/{tid}] 메타데이터 SQL은 바인드 변수를 쓸 수 없습니다.")
        if t["load_type"] != "FULL":
            raise ValueError(f"[metadata/{tid}] 메타데이터는 FULL만 지원합니다.")


# ======================================================================
# 실행 컨텍스트 / 기준일
# ======================================================================
def make_job_id(run_id: str, cfg: dict[str, Any]) -> str:
    """Airflow run_id를 파일명에 안전한 문자열로 변환한다."""

    jc = cfg["defaults"]["job_id"]
    if jc.get("source") != "AIRFLOW_RUN_ID":
        raise ValueError(f"지원하지 않는 job_id source: {jc.get('source')}")
    if not jc.get("filename_safe", True):
        return run_id
    return re.sub(r"[^A-Za-z0-9_.\-]", jc.get("replace_invalid_chars_with", "_"), run_id)


def _parse_ymd(value: str) -> date:
    if not _YMD.match(value):
        raise ValueError(f"날짜는 YYYYMMDD 형식이어야 합니다: {value!r}")
    return datetime.strptime(value, "%Y%m%d").date()

def resolve_base_dates(
    table: dict[str, Any],
    now: datetime,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[str]:
    """PERIOD 테이블의 적재 기준일(base_date) 목록을 반환한다.

    수동 적재:
      - date_from/date_to는 YYYYMMDD 문자열
      - 날짜 형식 및 유효성, 시작/종료 순서 검증
      - 테이블의 range_unit(D/M/Y)에 따라 기준일 생성

    자동 적재:
      - D: 당일을 포함한 N일 (YYYYMMDD)
      - M: 당월을 포함한 N개월 (YYYYMM)
      - Y: 당년을 포함한 N년 (YYYY)
    """

    range_unit = table.get("range_unit")

    if range_unit not in ("D", "M", "Y"):
        raise ValueError(
            f"[{table['id']}] 지원하지 않는 range_unit입니다: {range_unit}"
        )

    # 수동 적재
    if date_from is not None or date_to is not None:
        if date_from is None or date_to is None:
            raise ValueError("수동 적재 시 date_from과 date_to를 모두 입력해야 합니다.")
        
        # 입력값은 YYYYMMDD 문자열이어야 함
        for name, value in (
            ("date_from", date_from),
            ("date_to", date_to),
        ):
            if value is not None:
                if not isinstance(value, str):
                    raise ValueError(f"{name}은 YYYYMMDD 형식의 문자열이어야 합니다.")
                if not _YMD.fullmatch(value):
                    raise ValueError(
                        f"{name}은 YYYYMMDD 형식이어야 합니다: {value!r}"
                    )

        # 실제 날짜 유효성 검증 (예: 20260230은 오류)
        start = _parse_ymd(date_from or date_to)
        end = _parse_ymd(date_to or date_from)

        if start > end:
            raise ValueError(
                f"date_from({date_from})이 date_to({date_to})보다 큽니다."
            )

        # D: 시작일부터 종료일까지 날짜별 반환
        if range_unit == "D":
            return [
                (start + timedelta(days=i)).strftime("%Y%m%d")
                for i in range((end - start).days + 1)
            ]

        # M: 시작월부터 종료월까지 월별 반환
        if range_unit == "M":
            start_month = start.year * 12 + start.month - 1
            end_month = end.year * 12 + end.month - 1

            return [
                f"{month_index // 12:04d}{month_index % 12 + 1:02d}"
                for month_index in range(start_month, end_month + 1)
            ]

        # Y: 시작연도부터 종료연도까지 연도별 반환
        return [
            f"{year:04d}"
            for year in range(start.year, end.year + 1)
        ]

    # 자동 적재: 기존 기간 계산 로직 유지
    range_value = table.get("range_value")

    if not range_value or range_value < 1:
        raise ValueError(
            f"[{table['id']}] auto-load-range를 해석할 수 없습니다."
        )

    current_date = now.date()
    base_dates = []

    if range_unit == "D":
        start = current_date - timedelta(days=range_value - 1)

        base_dates = [
            (start + timedelta(days=i)).strftime("%Y%m%d")
            for i in range(range_value)
        ]

    elif range_unit == "M":
        current_month = current_date.replace(day=1)

        for i in range(range_value - 1, -1, -1):
            year = current_month.year
            month = current_month.month - i

            while month <= 0:
                year -= 1
                month += 12

            base_dates.append(f"{year:04d}{month:02d}")

    elif range_unit == "Y":
        for i in range(range_value - 1, -1, -1):
            base_dates.append(f"{current_date.year - i:04d}")

    return base_dates

# ======================================================================
# 경로 / 파일명 / 삭제 규칙
# ======================================================================
def _check_vars(values: dict[str, Any]) -> None:
    """경로/파일명에 들어갈 값이 안전한지 검증한다 (구분자, '..' 차단)."""

    for key, val in values.items():
        if val is None:
            continue
        s = str(val)
        if not _SEGMENT.match(s) or ".." in s:
            raise ValueError(f"경로/파일명에 사용할 수 없는 값: {key}={s!r}")


def fmt_part_no(n: int, cfg: dict[str, Any]) -> str:
    p = cfg["defaults"]["part_number"]
    return str(n).rjust(int(p["padding"]), str(p["fill_char"]))


def resolve_prefix(cfg: dict[str, Any], section: str, key: str, **v: Any) -> str:
    """저장 경로 템플릿을 치환해 타겟 기준 상대 경로를 반환한다."""

    _check_vars(v)
    target = cfg["target"]
    bucket = target.get("bucket", target.get("container", ""))
    template = cfg["storage"]["paths"][section][key]
    # 새 설정은 {root}를 사용한다. 기존 {bucket}/ 템플릿도 하위 호환한다.
    root = bucket if str(target.get("type", "")).upper() not in {"SNOWFLAKE", "SNOWFLAKE_STAGE"} else ""
    full = template.format(bucket=bucket, root=root, source=cfg["source"]["name"], **v)
    if "{bucket}" in template:
        head = bucket + "/"
        if not bucket or not full.startswith(head):
            raise ValueError(f"경로 템플릿은 '{{bucket}}/'로 시작해야 합니다: {template}")
        prefix = full[len(head):].strip("/")
    elif "{root}" in template and root:
        head = root.rstrip("/") + "/"
        prefix = full[len(head):].strip("/") if full.startswith(head) else full.strip("/")
    else:
        prefix = full.strip("/")
    return prefix.lower() if LOWERCASE_S3_KEY else prefix


def resolve_filename(cfg: dict[str, Any], key: str, **v: Any) -> str:
    """
    storage.filenames.<key> 템플릿을 치환해 파일명을 반환한다.
    arg. :
        cfg: dict[str, Any]: 시스템 설정 딕셔너리. 파일명 규칙(cfg["storage"]["filenames"])과 데이터 소스 이름(cfg["source"]["name"])
        key: str: 설정된 파일명 템플릿 중 어떤 규칙을 가져올지 지정하는 키입니다. (예: "raw_data", "summary_log" 등)
        **v: Any: 가변 키워드 인자(Keyword Arguments)로, 파일명 템플릿에 동적으로 채워넣을 변수들입니다. (예: date="2026-10-27", chunk_id=1 등)
    return : 
        str: 최종 완성된 파일명 문자열 반환
    """

    _check_vars(v)
    name = cfg["storage"]["filenames"][key].format(source=cfg["source"]["name"], **v)
    return name.lower() if LOWERCASE_S3_KEY else name


def apply_delete_policy(target_storage: Any, cfg: dict[str, Any], section: str, prefix: str) -> int:
    """storage.delete_policy.<section> 규칙에 따라 기존 파일을 삭제한다.

    prefix 뒤에 '/'를 붙여 조회하므로 SH/COSTS 삭제가 SH/COSTS2 를 건드리지 않는다.
    """

    policy = cfg["storage"]["delete_policy"][section]
    if policy == KEEP_EXISTING:
        return 0
    if policy != DELETE_BEFORE_LOAD:
        raise ValueError(f"알 수 없는 delete_policy.{section}: {policy}")

    bucket = cfg["target"].get("bucket", "")
    if isinstance(target_storage, (S3TargetAdapter, SnowflakeStageTargetAdapter)):
        keys = target_storage.list_keys(prefix=prefix + "/")
        #LOG.info("[%s] 삭제 대상 파일 목록!!! (%d건):\n%s", section, len(keys), "\n".join(keys))
        target_storage.delete_keys(keys, prefix=prefix + "/")
    else:  # 기존 S3Hook 직접 전달 방식과의 하위 호환
        keys = target_storage.list_keys(bucket_name=bucket, prefix=prefix + "/") or []
        if keys:
            target_storage.delete_objects(bucket=bucket, keys=keys)
    LOG.info("[%s] 기존 타겟 파일 삭제: target=%s prefix=%s (%d건)",
             section, cfg["target"].get("type"), prefix, len(keys))
    return len(keys)


# ======================================================================
# 직렬화 (NDJSON + gzip) / 통계
# ======================================================================
def _to_jsonable(v: Any) -> Any:
    """
    주석필요..
    old..
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    if hasattr(v, "read"):          # CLOB/BLOB
        return _to_jsonable(v.read())
    return v
    """
    if v is None:
        return None
    if isinstance(v, dict):
        return {str(k): _to_jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_to_jsonable(x) for x in v]
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    if hasattr(v, "read"):          # CLOB/BLOB
        return _to_jsonable(v.read())
    return v


def put_gz(target_storage: Any, bucket: str, key: str, body: bytes) -> None:
    """gzip 바이트를 S3에 적재한다. Content-Type/Content-Encoding 헤더를 함께 지정한다."""

    extra: dict[str, Any] = {"ContentType": "application/json"}
    if SET_CONTENT_ENCODING:
        extra["ContentEncoding"] = "gzip"
    if isinstance(target_storage, (S3TargetAdapter, SnowflakeStageTargetAdapter)):
        target_storage.put_bytes(key, body, content_type="application/json",
                     content_encoding="gzip" if SET_CONTENT_ENCODING else None)
    else:  # 기존 S3Hook 직접 전달 방식과의 하위 호환
        target_storage.get_conn().put_object(Bucket=bucket, Key=key, Body=body, **extra)


def _gzip_lines(lines) -> bytes:
    """
    텍스트 데이터를 메모리 상에서 Gzip 형식으로 압축하여 바이트(bytes) 데이터로 변환
    arg.:
        lines : JSON 문자열
    return : 
        최종 압축된 바이너리 데이터
    """
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        for line in lines:
            gz.write((line + "\n").encode("utf-8"))
    return buf.getvalue()


def encode_chunk(
    rows: list[tuple], columns: list[str], sum_columns: list[str]
) -> tuple[bytes, dict[str, Any]]:
    """
    행 묶음을 gzip NDJSON 으로 만들고, 파일 단위 통계를 함께 계산한다.
    arg.:
        rows: 데이터베이스 등에서 조회한 행 데이터 리스트 (각 행은 튜플 형태: [(val1, val2, ...), (val3, val4, ...)])
        columns: 각 컬럼의 이름 리스트 (['ID', 'NAME', 'AMOUNT'])
        sum_columns: 합계를 구할 대상 컬럼명 리스트 (['AMOUNT'])
    return : 
        tuple[bytes, dict[str, Any]]: 압축된 파일 바이너리 데이터(bytes)와 통계 정보가 담긴 딕셔너리(dict) 반환
    """

    upper = [c.upper() for c in columns]
    sum_idx: dict[str, int] = {}
    #. sum_column의 조회결과 중 컬럼 위치(순서)
    for c in sum_columns:
        if c.upper() not in upper:
            raise ValueError(f"sum 컬럼이 SELECT 결과에 없습니다: {c}")
        sum_idx[c] = upper.index(c.upper())

    #. 초기화 
    nulls = [0] * len(columns)
    sums = {c: Decimal(0) for c in sum_idx}

    def lines():
        for row in rows:
            rec = {}
            for i, (col, val) in enumerate(zip(columns, row)):
                if val is None:
                    nulls[i] += 1
                rec[col] = _to_jsonable(val)
            for c, i in sum_idx.items():
                if row[i] is not None:
                    sums[c] += Decimal(str(row[i]))
            yield json.dumps(rec, ensure_ascii=False)

    body = _gzip_lines(lines())
    stats = {
        "record_count": len(rows),
        "null_count": {c: n for c, n in zip(columns, nulls)},
        "sum": {c: format(s, "f") for c, s in sums.items()},   # 정밀도 유지를 위해 문자열
    }
    return body, stats


def write_json_records(
    target_storage: Any,
    cfg: dict[str, Any],
    records: list[dict[str, Any]],
    *,
    prefix: str,
    filename_key: str,
    fname_vars: dict[str, Any],
    delete_section: str,
    common: dict[str, Any] | None = None,
    array_key: str = "files",
    single_file: bool = False,
) -> list[str]:
    """공통 정보와 상세 레코드 배열을 JSON 문서 하나로 구성해 gzip 적재한다.

    single_file=False이면 log를 chunk_size 단위로 분할하고,
    single_file=True이면 translog를 chunk_size와 무관하게 한 파일로 저장한다.
    """
    apply_delete_policy(target_storage, cfg, delete_section, prefix)
    if not records:
        return []

    bucket = cfg["target"].get("bucket", "")
    chunk = len(records) if single_file else int(cfg["defaults"]["chunk_size"])
    start = int(cfg["defaults"]["part_number"]["start"])
    keys = []

    for i in range(0, len(records), chunk):
        part_no = start + i // chunk
        # 공통 필드는 별도 common 객체로 감싸지 않고 최상위 요소로 배치
        payload = dict(common or {})
        payload[array_key] = records[i:i + chunk]
        body = _gzip_lines([json.dumps(payload, ensure_ascii=False, default=str)])
        fname = resolve_filename(
            cfg, filename_key, part_no=fmt_part_no(part_no, cfg), **fname_vars
        )
        key = f"{prefix}/{fname}"
        put_gz(target_storage, bucket, key, body)
        keys.append(key)
    return keys


# ======================================================================
# SQL Source -> Target (data / meta 공통)
# ======================================================================
def export_query(
    conn: Any,
    target_storage: Any,
    cfg: dict[str, Any],
    *,
    sql: str,
    binds: dict[str, Any],
    prefix: str,
    filename_key: str,
    fname_vars: dict[str, Any],
    delete_section: str,
    sum_columns: list[str],
) -> dict[str, Any]:
    """
    정리 필요..
    쿼리를 실행해 chunk_size 단위 파일로 나눠 S3에 적재한다.

    첫 chunk까지 읽어 SQL 오류를 확인한 뒤에 기존 파일을 삭제한다.
    (SQL이 틀렸을 때 기존 적재분이 지워지지 않도록)
    """

    bucket = cfg["target"].get("bucket", "")
    chunk = int(cfg["defaults"]["chunk_size"])
    start = int(cfg["defaults"]["part_number"]["start"])

    source_user = getattr(conn, "username", None)
    LOG.info(
        "[%s] 조회 시작: source_user=%s dsn=%s binds=%s\n%s",
        delete_section, source_user, getattr(conn, "dsn", None), binds, sql,
    )

    cur = conn.cursor()
    try:
        cur.arraysize = chunk
        if str(cfg["source"].get("system_type", cfg["source"].get("type", ""))).upper() in {"SNOWFLAKE", "SNOWFLAKE_SQL"}:
            sql = re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", r"%(\1)s", sql)
        cur.execute(sql, binds)
        columns = [d[0] for d in cur.description]
        #LOG.info("Extracted Columns: %s", columns)
        #for d in cur.description:
        #    print(d[0], d[1], getattr(d[1], "name", None))
        
        rows = cur.fetchmany(chunk)

        deleted = apply_delete_policy(target_storage, cfg, delete_section, prefix)

        files: list[dict[str, Any]] = []
        part_no = start - 1
        while rows:
            part_no += 1
            body, stats = encode_chunk(rows, columns, sum_columns)
            fname = resolve_filename(
                cfg, filename_key, part_no=fmt_part_no(part_no, cfg), **fname_vars
            )
            key = f"{prefix}/{fname}"
            put_gz(target_storage, bucket, key, body)
            files.append({"part_no": part_no, "file_name": fname, "key": key, **stats})
            rows = cur.fetchmany(chunk)
    finally:
        cur.close()

    record_count = sum(f["record_count"] for f in files)
    if record_count == 0:
        LOG.warning(
            "[%s] 조회 결과가 0건입니다. 데이터 파일은 생성되지 않습니다. "
            "source_user=%s (메타데이터 뷰의 조회 권한/범위는 소스 DB 계정 설정을 확인하세요)",
            delete_section, source_user,
        )
    else:
        LOG.info("[%s] 조회 완료: %d건, 파일 %d개", delete_section, record_count, len(files))

    return {
        "files": files,
        "record_count": record_count,
        "deleted": deleted,
        "source_user": source_user,
    }


def build_log_records(
    cfg: dict[str, Any], run: dict[str, Any], base_date: str | None, files: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """분할 데이터 파일 1개당 통계 레코드 1건.

    statistics.match_data_file_by (job_id, base_date, part_no) 로 데이터 파일과 매칭한다.
    """

    st = cfg.get("statistics", {})
    records = []
    for f in files:
        rec: dict[str, Any] = {
            "job_id": run["job_id"],
            "base_date": base_date,
            "part_no": fmt_part_no(f["part_no"], cfg),
            "file_name": f["file_name"],
            "file_path": f["key"],
        }
        if st.get("record_count"):
            rec["record_count"] = f["record_count"]
        if st.get("null_count"):
            rec["null_count"] = f["null_count"]
        if f["sum"]:
            rec["sum"] = f["sum"]
        records.append(rec)
    return records


# ======================================================================
# 작업 단위 실행 (실패해도 예외를 던지지 않고 결과 dict 로 반환)
# ======================================================================


# now의 설정파일 참조 timezone 적용 : Asia/Seoul
def _now(cfg: dict[str, Any]) -> datetime:
    return datetime.now(ZoneInfo(cfg["defaults"]["timezone"]))


def _status(cfg: dict[str, Any], ok: bool) -> str:
    tl = cfg.get("translog", {})
    return tl.get("success_path_status", "SUCCESS") if ok else tl.get("fail_path_status", "FAILED")


def _new_result(category: str, t: dict[str, Any], base_date: str | None, now: datetime) -> dict[str, Any]:
    return {
        "category": category,
        "table_id": t["id"],
        "schema": t["schema"],
        "table": t["name"],
        "load_type": t["load_type"],
        "base_date": base_date,
        "status": None,
        "file_count": 0,
        "record_count": 0,
        "deleted_count": 0,
        "source_user": None,
        "data_prefix": None,
        "log_prefix": None,
        "started_at": now.isoformat(),
        "ended_at": None,
        "elapsed_sec": None,
        "error": None,
    }


def _finish(cfg: dict[str, Any], res: dict[str, Any], started: datetime, error: Exception | None) -> dict[str, Any]:
    """
    작업(Task)이나 태스크의 실행(성공/실패)의 최종 마무리 시점에 실행 결과(메타데이터 및 통계)를 정리하고 패키징하는 후처리(Teardown) 함수
    arg. : 
        cfg: dict[str, Any]: 시스템 설정 정보 딕셔너리 (타임존 계산 등을 위해 전달됨).
        res: dict[str, Any]: 작업 진행 과정에서 쌓인 결과나 통계가 담긴 딕셔너리. 이 함수 안에서 최종 종료 정보가 추가로 업데이트됩니다.
        started: datetime: 작업이 시작된 시각(datetime 객체). 소요 시간을 계산하는 기준점이 됩니다.
        error: Exception | None: 작업 중 예외(에러)가 발생했다면 해당 예외 객체가 전달되고, 정상 종료되었다면 None이 전달됩니다.
    return :
        메타데이터가 업데이트된 최종 결과 딕셔너리(res)를 반환
    """

    ended = _now(cfg)
    res["status"] = _status(cfg, error is None)
    res["ended_at"] = ended.isoformat()
    res["elapsed_sec"] = round((ended - started).total_seconds(), 3)
    if error is not None:
        res["error"] = f"{type(error).__name__}: {error}"[:2000]
    return res


def run_table_load(
    cfg: dict[str, Any],
    t: dict[str, Any],
    base_date: str | None,
    run: dict[str, Any],
    target_storage: Any,
    conn_factory: Callable[[], Any],
) -> dict[str, Any]:
    """테이블 데이터 1건(FULL 이면 테이블, PERIOD 이면 테이블+기준일) 적재 + 통계(log) 적재."""

    started = _now(cfg)
    res = _new_result("DATA", t, base_date, started)
    conn = None
    try:
        is_period = t["load_type"] == "PERIOD"
        key = "period" if is_period else "full"

        if is_period and not base_date:
            raise ValueError("PERIOD 적재에는 base_date가 필요합니다.")
        binds = {}
        if "BASE_DATE" in t["bind_variables"]:
            if not base_date:
                raise ValueError("SQL에 :BASE_DATE가 있으나 base_date가 없습니다 (FULL 적재).")
            #_parse_ymd(base_date)
            binds["BASE_DATE"] = base_date

        v: dict[str, Any] = {
            "schema": t["schema"],
            "table": t["name"],
            "generation_ts": run["generation_ts"],
            "run_date": run["run_date"],
        }
        if is_period:
            v["base_date"] = base_date

        st = cfg.get("statistics", {})
        stats_on = bool(st.get("enabled"))
        sum_cfg = st.get("sum", {})
        sum_cols = (
            t["sum_columns"]
            if stats_on and sum_cfg.get("enabled") and sum_cfg.get("columns_from_table_config")
            else []
        )

        data_prefix = resolve_prefix(cfg, "data", key, **v)
        res["data_prefix"] = data_prefix

        conn = conn_factory()
        out = export_query(
            conn, target_storage, cfg,
            sql=t["sql"], binds=binds, prefix=data_prefix,
            filename_key=key, fname_vars=v, delete_section="data",
            sum_columns=sum_cols,
        )
        res["file_count"] = len(out["files"])
        res["record_count"] = out["record_count"]
        res["deleted_count"] = out["deleted"]
        res["source_user"] = out["source_user"]

        if stats_on:
            log_prefix = resolve_prefix(cfg, "log", key, **v)
            res["log_prefix"] = log_prefix
            log_records = build_log_records(cfg, run, base_date, out["files"])
            log_common = {
                "job_id": run["job_id"],
                "source": cfg["source"]["name"],
                "schema": t["schema"],
                "table": t["name"],
                "load_type": t["load_type"],
                "base_date": base_date,
                "source_user": out["source_user"],
                "data_prefix": data_prefix,
                "log_prefix": log_prefix,
            }
            # 공통 필드는 common에만 두고 파일별 배열에서는 중복 제거
            for record in log_records:
                record.pop("job_id", None)
                record.pop("base_date", None)
            write_json_records(
                target_storage, cfg, log_records,
                prefix=log_prefix, filename_key=key, fname_vars=v, delete_section="log",
                common=log_common, array_key="files",
            )

        return _finish(cfg, res, started, None)
    except Exception as e:  # noqa: BLE001 - 실패도 translog 로 남기기 위해 결과로 반환
        LOG.exception("[DATA] %s.%s base_date=%s 적재 실패", t["schema"], t["name"], base_date)
        return _finish(cfg, res, started, e)
    finally:
        if conn is not None:
            conn.close()


def run_metadata_load(
    cfg: dict[str, Any],
    t: dict[str, Any],
    run: dict[str, Any],
    target_storage: Any,
    conn_factory: Callable[[], Any],
) -> dict[str, Any]:
    """
    메타데이터 조회 및 적재 수행
    Args:
        cfg (dict[str, Any]): 시스템 전체 설정 정보 및 환경 변수가 담긴 딕셔너리.
        t (dict[str, Any]): 현재 태스크(Task)의 상세 설정 및 메타 정보.
        run (dict[str, Any]): 현재 실행 인스턴스(DAG Run 등)의 상태 및 실행 정보.
        target_storage (Any): S3, GCS, Azure, Snowflake Stage 클라이언트 또는 스토리지 핸들러 객체.
        conn_factory (Callable[[], Any]): 데이터베이스 커넥션을 생성하여 반환하는 팩토리 함수.
    Returns:
        dict[str, Any]: 메타데이터 처리 결과 및 통계 정보가 담긴 딕셔너리.
    """

    started = _now(cfg)
    res = _new_result("METADATA", t, None, started)
    conn = None
    try:
        v = {
            "metadata_table": t["name"],
            "generation_ts": run["generation_ts"],
            "run_date": run["run_date"],
        }
        prefix = resolve_prefix(cfg, "meta", "full", **v)
        res["data_prefix"] = prefix

        conn = conn_factory()
        out = export_query(
            conn, target_storage, cfg,
            sql=t["sql"], binds={}, prefix=prefix,
            filename_key="meta", fname_vars=v, delete_section="meta",
            sum_columns=[],
        )
        res["file_count"] = len(out["files"])
        res["record_count"] = out["record_count"]
        res["deleted_count"] = out["deleted"]
        res["source_user"] = out["source_user"]
        return _finish(cfg, res, started, None)
    except Exception as e:  # noqa: BLE001
        LOG.exception("[METADATA] %s 적재 실패", t["name"])
        return _finish(cfg, res, started, e)
    finally:
        if conn is not None:
            conn.close()


# ======================================================================
# translog
# ======================================================================
def find_missing_results(
    cfg: dict[str, Any],
    tables: dict[str, Any],
    meta_items: list[dict[str, Any]],
    data_items: list[dict[str, Any]],
    results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """결과가 없는 작업(태스크 비정상 종료 등)을 FAILED 결과로 만들어 반환한다."""

    done = {(r["category"], r["table_id"], r["base_date"]) for r in results}
    missing = []
    for category, section, items in (
        ("METADATA", "metadata", meta_items or []),
        ("DATA", "data", data_items or []),
    ):
        for item in items:
            base_date = item.get("base_date")
            if (category, item["table_id"], base_date) in done:
                continue
            now = _now(cfg)
            res = _new_result(category, tables[section][item["table_id"]], base_date, now)
            res["status"] = _status(cfg, False)
            res["ended_at"] = now.isoformat()
            res["error"] = "결과 없음: 태스크가 비정상 종료되었습니다 (Airflow 태스크 로그 확인)"
            missing.append(res)
    return missing


def write_translog(
    target_storage: Any, cfg: dict[str, Any], run: dict[str, Any], results: list[dict[str, Any]]
) -> dict[str, Any]:
    """DAG 실행 1회당 translog 파일 1개를 common + items 구조로 적재한다."""

    ok_status, fail_status = _status(cfg, True), _status(cfg, False)
    failed = [r for r in results if r.get("status") != ok_status]
    run_status = fail_status if failed else ok_status

    summary: dict[str, Any] = {
        "run_status": run_status,
        "total": len(results),
        "success": len(results) - len(failed),
        "failed": len(failed),
        "failed_items": [
            f"{r.get('schema') or ''}.{r.get('table')}[{r.get('base_date')}]"
            for r in failed
        ][:20],
        "file": None,
    }
    if not cfg.get("translog", {}).get("enabled") or not results:
        return summary

    common = {
        "job_id": run["job_id"],
        "dag_id": run["dag_id"],
        "run_id": run["run_id"],
        "source": cfg["source"]["name"],
        "run_date": run["run_date"],
        "generation_ts": run["generation_ts"],
        "run_status": run_status,
        "total": len(results),
        "success": len(results) - len(failed),
        "failed": len(failed),
        "failed_items": summary["failed_items"],
    }
    prefix = resolve_prefix(
        cfg, "translog", "fail" if failed else "success", run_date=run["run_date"]
    )
    part_no = int(cfg["defaults"]["part_number"]["start"])
    keys = write_json_records(
        target_storage,
        cfg,
        records=results,
        prefix=prefix,
        filename_key="translog",
        fname_vars={
            "generation_ts": run["generation_ts"],
        },
        delete_section="translog",
        common=common,
        array_key="items",
        single_file=True,
    )
    if keys:
        summary["file"] = keys[0]
    return summary
