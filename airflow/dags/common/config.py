"""
Get Configuration (YAML / XML)
"""
from pathlib import Path
from pprint import pformat
from typing import Any
import logging
import re
import xml.etree.ElementTree as ET

import yaml

LOG = logging.getLogger(__name__)

# dags 디렉터리
DAGS_DIR = Path(__file__).resolve().parent.parent

VALID_LOAD_TYPES = {"FULL", "PERIOD"}
XML_SECTIONS = ("data", "metadata")

# 3D, 10D 같은 기간 표기 (숫자 + 단위 1글자)
_RANGE_PATTERN = re.compile(r"^(\d+)([A-Za-z])$")
# SQL 문자열 리터럴('...')은 바인드 변수 탐색에서 제외
_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")
_BIND_VAR = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")


# ----------------------------------------------------------------------
# YAML
# ----------------------------------------------------------------------
def load_config(yaml_filename: str) -> dict[str, Any]:
    """지정된 YAML 파일을 읽어 딕셔너리로 반환한다."""

    config_path = DAGS_DIR / yaml_filename

    if not config_path.exists():
        raise FileNotFoundError(
            f"설정 파일을 찾을 수 없습니다: {config_path}"
        )

    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ValueError(
            f"YAML 설정은 딕셔너리 형식이어야 합니다: {config_path}"
        )

    return config


# ----------------------------------------------------------------------
# XML
# ----------------------------------------------------------------------
def _text(elem: ET.Element, path: str) -> str | None:
    """하위 요소의 텍스트를 반환한다. 태그가 없거나 비어 있으면 None."""

    value = elem.findtext(path)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _parse_range(raw: str | None, where: str) -> tuple[int, str] | tuple[None, None]:
    """'3D' -> (3, 'D'). 비어 있으면 (None, None)."""

    if raw is None:
        return None, None

    match = _RANGE_PATTERN.match(raw)
    if not match:
        raise ValueError(
            f"{where}: auto-load-range 형식이 올바르지 않습니다 "
            f"(예: 3D): {raw!r}"
        )
    return int(match.group(1)), match.group(2).upper()


def _find_bind_variables(sql: str) -> list[str]:
    """SQL의 바인드 변수명을 중복 없이, 등장 순서대로 반환한다."""

    stripped = _STRING_LITERAL.sub("''", sql)
    seen: dict[str, None] = {}
    for name in _BIND_VAR.findall(stripped):
        seen.setdefault(name.upper(), None)
    return list(seen)


def _parse_table(elem: ET.Element, section: str, xml_name: str) -> dict[str, Any]:
    """<table> 요소 하나를 딕셔너리로 변환하고 검증한다."""

    table_id = _text(elem, "id")
    if not table_id:
        raise ValueError(f"{xml_name} [{section}]: <id>가 없는 <table>이 있습니다.")

    where = f"{xml_name} [{section}/{table_id}]"

    name = _text(elem, "name")
    if not name:
        raise ValueError(f"{where}: <name>이 비어 있습니다.")

    load_type = (_text(elem, "load-type") or "FULL").upper()
    if load_type not in VALID_LOAD_TYPES:
        raise ValueError(
            f"{where}: load-type은 {sorted(VALID_LOAD_TYPES)} 중 하나여야 합니다: "
            f"{load_type!r}"
        )

    sql = _text(elem, "sql")
    if not sql:
        raise ValueError(f"{where}: <sql>이 비어 있습니다.")
    # oracledb는 끝의 세미콜론이 있으면 ORA-00933 발생
    sql = sql.rstrip(";").strip()

    binds = _find_bind_variables(sql)
    if load_type == "PERIOD" and "BASE_DATE" not in binds:
        raise ValueError(f"{where}: PERIOD 적재인데 :BASE_DATE 바인드 변수가 없습니다.")

    auto_load_range = _text(elem, "auto-load-range")
    range_value, range_unit = _parse_range(auto_load_range, where)

    sum_columns = [
        c.text.strip()
        for c in elem.findall("statistics/sum-columns/column")
        if c.text and c.text.strip()
    ]

    return {
        "id": table_id,
        "schema": _text(elem, "schema"),          # 메타데이터 뷰는 None
        "name": name,
        "load_type": load_type,
        "auto_load_range": auto_load_range,       # 원본 문자열 (예: "3D")
        "range_value": range_value,               # 3
        "range_unit": range_unit,                 # "D"
        "sum_columns": sum_columns,
        "sql": sql,
        "bind_variables": binds,                  # 예: ["BASE_DATE"]
    }


def load_xml_config(xml_filename: str) -> dict[str, Any]:
    """지정된 XML 파일을 읽어 딕셔너리로 반환한다.

    반환 구조:
        {
          "version": "1.0",
          "data":     {"SH_COSTS": {...}, ...},          # <data>/<table>
          "metadata": {"USER_TABLES": {...}, ...},       # <metadata>/<table>
        }
    섹션 안에서는 <id>를 키로 하며 XML에 적힌 순서를 유지한다.
    """

    xml_path = DAGS_DIR / xml_filename

    if not xml_path.exists():
        raise FileNotFoundError(
            f"XML 설정 파일을 찾을 수 없습니다: {xml_path}"
        )

    try:
        root = ET.parse(xml_path).getroot()
    except ET.ParseError as e:
        raise ValueError(f"XML 문법 오류: {xml_path}: {e}") from e

    # 알 수 없는 최상위 요소는 무시하되 로그로 남긴다 (예: 빈 <view/>)
    unknown = [child.tag for child in root if child.tag not in XML_SECTIONS]
    if unknown:
        LOG.warning(
            "%s: 사용하지 않는 최상위 요소 무시: %s",
            xml_path.name,
            sorted(set(unknown)),
        )

    config: dict[str, Any] = {"version": root.get("version")}

    for section in XML_SECTIONS:
        tables: dict[str, dict[str, Any]] = {}
        section_elem = root.find(section)

        if section_elem is not None:
            for table_elem in section_elem.findall("table"):
                table = _parse_table(table_elem, section, xml_path.name)
                if table["id"] in tables:
                    raise ValueError(
                        f"{xml_path.name} [{section}]: 중복된 id: {table['id']}"
                    )
                tables[table["id"]] = table

        config[section] = tables

    return config


# ----------------------------------------------------------------------
# 출력
# ----------------------------------------------------------------------
def print_config(config: dict[str, Any]) -> None:
    """전달받은 설정 딕셔너리 전체를 Airflow 로그에 출력한다."""

    LOG.info("=" * 80)
    LOG.info("CONFIG DICTIONARY START")
    LOG.info("\n%s", pformat(config, sort_dicts=False, width=120))
    LOG.info("CONFIG DICTIONARY END")
    LOG.info("=" * 80)