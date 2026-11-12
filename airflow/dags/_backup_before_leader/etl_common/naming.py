"""파일명 / 일자 규칙 (.claude/docs/CreateDag.md 4.3)."""
import re
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))


def make_job_id(run_id):
    """Airflow run_id 를 파일명에 안전한 값으로 변환 ('_' 는 구분자라 사용하지 않음)"""
    return re.sub(r"[^A-Za-z0-9]+", "-", run_id).strip("-")


def make_generation_ts(now):
    # 스펙: "KST 기반 YYYYMMDDTHHMMSS" (시간대 접미사 없음, 전 시스템 KST 통일)
    return now.astimezone(KST).strftime("%Y%m%dT%H%M%S")


def part_filename(schema, name, job_id, gen_ts, part_no, base_date=None):
    stem = f"{schema}_{name}_{job_id}_{gen_ts}"
    if base_date:
        stem += f"_{base_date}"
    return f"{stem}_{part_no:08d}.json.gz"


def base_dates(run_date, range_days):
    """실행일을 포함한 최근 range_days 개 일자 (오름차순). 예: 20261022, 3 -> 20261020, 20261021, 20261022"""
    end = datetime.strptime(run_date, "%Y%m%d")
    return [(end - timedelta(days=i)).strftime("%Y%m%d") for i in range(range_days - 1, -1, -1)]
