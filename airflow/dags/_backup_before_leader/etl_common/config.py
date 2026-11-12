"""원천 설정 파일(XML) 파싱. 설정 형식은 config/*.xml 과 .claude/docs/CreateDag.md 참고."""
import xml.etree.ElementTree as ET


def _text(el, tag, default=""):
    child = el.find(tag) if el is not None else None
    return (child.text or "").strip() if child is not None else default


def _parse_table(el, kind):
    table = {
        "kind": kind,                                   # data | meta
        "schema": _text(el, "schema"),
        "name": _text(el, "name"),
        "load_type": _text(el, "load-type").upper(),    # FULL | PERIOD
        "range_days": int(_text(el, "load-range-day") or 0),
        "sql": _text(el, "sql").rstrip(";").strip(),
        "sum_columns": [c.strip().lower() for c in _text(el, "sum-columns").split(",") if c.strip()],
    }
    label = f"{table['schema']}.{table['name']}"
    if table["load_type"] not in ("FULL", "PERIOD"):
        raise ValueError(f"{label}: load-type 은 FULL 또는 PERIOD 여야 합니다")
    if table["load_type"] == "PERIOD":
        if table["range_days"] < 1:
            raise ValueError(f"{label}: PERIOD 는 load-range-day >= 1 이 필요합니다")
        if ":BASE_DATE" not in table["sql"]:
            raise ValueError(f"{label}: PERIOD sql 에 :BASE_DATE 바인드가 필요합니다")
    return table


def load_config(path):
    root = ET.parse(path).getroot()
    cfg = {
        "name": _text(root, "name"),
        "source_type": _text(root, "source-type").upper(),
        "source_conn": _text(root, "connection"),
        "target_type": _text(root.find("target"), "type").upper(),
        "target_conn": _text(root.find("target"), "connection"),
        "bucket": _text(root.find("target"), "bucket"),
        "split_rows": int(_text(root, "split-rows") or 10000),
        "meta_owner": _text(root.find("meta"), "owner"),
        "tables": [_parse_table(t, "data") for t in root.findall("tables/table")],
        "meta_tables": [_parse_table(t, "meta") for t in root.findall("meta/table")],
    }
    if cfg["source_type"] != "ORACLE":
        raise NotImplementedError(f"{path.name}: source-type {cfg['source_type']} 는 아직 지원하지 않습니다")
    if cfg["target_type"] != "S3":
        raise NotImplementedError(f"{path.name}: target type {cfg['target_type']} 는 아직 지원하지 않습니다")
    return cfg
