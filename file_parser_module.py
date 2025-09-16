
#!/usr/bin/env python3
"""
file_parser_module.py

Multi-format file parser that:
- Parses CSV/Excel (auto-detect delimiter and headers; Excel first sheet by default).
- Parses JSON/XML extracting leaf-level JSONPaths/XPaths.
- Validates non-empty columns, infers data types (int, decimal, date, datetime, time, boolean, nvarchar).
- Produces a mapping DataFrame compatible with json_feed_builder_mssql.py (required columns).
- Designed to be invoked directly from json_feed_builder_mssql.py or a wrapper.

Author: ChatGPT
"""
from __future__ import annotations

import csv
import io
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd

try:
    import openpyxl  # noqa: F401
except Exception:
    openpyxl = None

import xml.etree.ElementTree as ET

# ---------- Public API ----------

REQUIRED_MAPPING_COLUMNS = [
    "customer_field",
    "biqh_import_field",
    "description",
    "biqh_column_data_type",
    "biqh_table_schema",
    "biqh_parent_table_name",
    "biqh_parent_column_name",
    "biqh_relation_table_name",
    "biqh_relation_column_name",
    "biqh_relation_column_value",
    "biqh_relation_column_data_type",
    "biqh_link_table_name",
    "remarks",
]

SQL_TYPE_IDS = {
    # mirror json_feed_builder_mssql.DATA_TYPE_MAP
    "nvarchar": 1, "int": 2, "decimal": 3, "date": 4, "datetime": 5,
    "time": 6, "boolean": 7, "bit": 7, "bigint": 8, "binary": 9, "any": 10
}

DEFAULT_SCHEMA = "beta"
DEFAULT_PARENT_TABLE = "Main"

def parse_file(file_path: Union[str, Path], sheet: Optional[Union[int, str]] = None,
               max_string_len_cap: int = 255, sample_rows: int = 2000) -> pd.DataFrame:
    """
    Detect type by extension and dispatch.
    Returns a pandas.DataFrame with REQUIRED_MAPPING_COLUMNS.
    """
    p = Path(file_path)
    ext = p.suffix.lower()
    if ext in [".csv", ".txt"]:
        df = _parse_csv(p, sample_rows=sample_rows)
        return _df_to_mapping(df, source_kind="csv", source_name=p.name, max_string_len_cap=max_string_len_cap)
    if ext in [".xlsx", ".xls"]:
        df = _parse_excel(p, sheet=sheet)
        return _df_to_mapping(df, source_kind="excel", source_name=f"{p.name}:{sheet if sheet is not None else 0}",
                              max_string_len_cap=max_string_len_cap)
    if ext in [".json"]:
        paths, samples = _parse_json_paths(p, sample_rows=sample_rows)
        return _paths_to_mapping(paths, samples, source_kind="json", source_name=p.name,
                                 max_string_len_cap=max_string_len_cap)
    if ext in [".xml"]:
        paths, samples = _parse_xml_paths(p, sample_rows=sample_rows)
        return _paths_to_mapping(paths, samples, source_kind="xml", source_name=p.name,
                                 max_string_len_cap=max_string_len_cap)
    raise ValueError(f"Unsupported file extension: {ext}")

def save_mapping_csv(mapping: pd.DataFrame, out_path: Union[str, Path]) -> Path:
    out = Path(out_path)
    mapping.to_csv(out, index=False)
    return out

# ---------- CSV / Excel ----------

def _sniff_csv(path: Path, sample_bytes: int = 4096) -> Tuple[csv.Dialect, bool]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        sample = f.read(sample_bytes)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",",";","|","\t"])
    except Exception:
        # fallback to comma
        class _Simple(csv.Dialect):
            delimiter = ","
            quotechar = '"'
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        dialect = _Simple()
    try:
        has_header = csv.Sniffer().has_header(sample)
    except Exception:
        has_header = True
    return dialect, has_header

def _parse_csv(path: Path, sample_rows: int = 2000) -> pd.DataFrame:
    dialect, has_header = _sniff_csv(path)
    # Use pandas with detected delimiter
    df = pd.read_csv(path, sep=dialect.delimiter, header=0 if has_header else None, dtype=str, nrows=None)
    # If no header, synthesize
    if not has_header:
        df.columns = [f"column{i+1}" for i in range(df.shape[1])]
    # Strip whitespace
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df

def _parse_excel(path: Path, sheet: Optional[Union[int, str]] = None) -> pd.DataFrame:
    # Default first sheet
    df = pd.read_excel(path, sheet_name=sheet if sheet is not None else 0, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df

# ---------- JSON paths ----------

def _json_leaf_paths(obj: Any, base: str = "$") -> List[str]:
    paths = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            child = f"{base}.{k}"
            paths.extend(_json_leaf_paths(v, child))
    elif isinstance(obj, list):
        # index-agnostic using [*]
        for i, v in enumerate(obj[:1] or [None]):  # only one branch needed for path shape
            child = f"{base}[*]"
            paths.extend(_json_leaf_paths(v, child))
    else:
        paths.append(base)
    return paths

def _collect_json_samples(obj: Any, path: str, out: List[Any]):
    """Collect sample values for a given JSONPath-like string (with [*])."""
    # Traverse according to path segments
    def _walk(o, segs, idx=0):
        if idx >= len(segs):
            out.append(o)
            return
        seg = segs[idx]
        if isinstance(o, dict) and seg in o:
            _walk(o[seg], segs, idx+1)
        elif isinstance(o, list):
            for item in o:
                _walk(item, segs, idx)  # don't consume seg; arrays continue at same key
        # else: missing branch -> ignore
    # normalize "$.a.b[*].c" -> ["a","b","c"]
    segs = [s for s in re.split(r"\.|\[\*\]", path) if s and s != "$"]
    _walk(obj, segs, 0)

def _parse_json_paths(path: Path, sample_rows: int = 2000) -> Tuple[List[str], Dict[str, List[Any]]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    root = data
    # If top-level is an array with homogeneous objects, use first element for shape
    probe = root[0] if isinstance(root, list) and root else root
    paths = sorted(set(_json_leaf_paths(probe)))
    samples: Dict[str, List[Any]] = {p: [] for p in paths}
    # Collect samples across full data (or first N elements)
    if isinstance(root, list):
        for rec in root[:sample_rows]:
            for p in paths:
                _collect_json_samples(rec, p, samples[p])
    else:
        for p in paths:
            _collect_json_samples(root, p, samples[p])
    return paths, samples

# ---------- XML paths ----------

def _element_is_leaf(elem: ET.Element) -> bool:
    return (len(list(elem)) == 0) and ((elem.text or "").strip() != "" or len(elem.attrib) > 0)

def _xpath_for_element(elem: ET.Element, root: ET.Element) -> str:
    # Build absolute, index-agnostic path with [*] for repeated tags
    def _index_agnostic_path(e: ET.Element) -> List[str]:
        segs = []
        while e is not None and e is not root:
            parent = e.getparent() if hasattr(e, "getparent") else None
            tag = e.tag
            segs.append(tag)
            e = parent
        segs.append(root.tag)
        segs.reverse()
        return segs

    # xml.etree doesn't expose parent, so compute by traversal
    # Build a dictionary of child->parent and sibling tag counts
    parent_of = {root: None}
    order = [root]
    for cur in list(root.iter()):
        for ch in list(cur):
            parent_of[ch] = cur
            order.append(ch)

    def _segments(e: ET.Element) -> List[str]:
        segs = []
        while e is not None:
            segs.append(e.tag)
            e = parent_of.get(e)
        return list(reversed(segs))

    segs = _segments(elem)
    return "/" + "/".join(segs)

def _parse_xml_paths(path: Path, sample_rows: int = 2000) -> Tuple[List[str], Dict[str, List[Any]]]:
    tree = ET.parse(path)
    root = tree.getroot()

    paths: List[str] = []
    samples: Dict[str, List[Any]] = {}

    # Collect element text leaves
    for elem in root.iter():
        if len(list(elem)) == 0 and (elem.text or "").strip() != "":
            xp = _xpath_for_element(elem, root)
            paths.append(xp)
            samples.setdefault(xp, []).append((elem.text or "").strip())

        # Collect attributes as leaf paths
        for aname, aval in elem.attrib.items():
            xp = _xpath_for_element(elem, root) + f"/@{aname}"
            paths.append(xp)
            samples.setdefault(xp, []).append(str(aval))

    # Deduplicate and sort
    paths = sorted(set(paths))
    return paths, samples

# ---------- Validation & Type inference ----------

_DATE_PATTS = [
    "%Y-%m-%d", "%d.%m.%Y", "%m/%d/%Y", "%Y/%m/%d", "%Y%m%d",
]
_TIME_PATTS = ["%H:%M:%S", "%H:%M"]

def _is_int(s: str) -> bool:
    try:
        int(s)
        return True
    except Exception:
        return False

def _is_float(s: str) -> bool:
    try:
        float(s)
        return True
    except Exception:
        return False

def _is_bool(s: str) -> bool:
    return str(s).strip().lower() in {"true","false","0","1","yes","no"}

def _is_date(s: str) -> bool:
    for p in _DATE_PATTS:
        try:
            datetime.strptime(str(s).strip(), p)
            return True
        except Exception:
            continue
    return False

def _is_time(s: str) -> bool:
    for p in _TIME_PATTS:
        try:
            datetime.strptime(str(s).strip(), p)
            return True
        except Exception:
            continue
    return False

def _infer_type(values: List[Any], max_string_len_cap: int = 255) -> Tuple[str, Optional[int], Optional[Tuple[int,int]]]:
    """
    Return (biqh_type, nvarchar_length, decimal(precision,scale))
    """
    vals = [v for v in (values or []) if v is not None and str(v).strip() != ""]
    if not vals:
        return ("nvarchar", 1, None)
    svals = [str(v).strip() for v in vals]

    # Booleans?
    if all(_is_bool(v) for v in svals):
        return ("boolean", None, None)

    # Integers?
    if all(_is_int(v) for v in svals):
        # choose bigint if any value exceeds 32-bit
        try:
            if any(abs(int(v)) > 2147483647 for v in svals):
                return ("bigint", None, None)
        except Exception:
            pass
        return ("int", None, None)

    # Decimals?
    if all(_is_float(v) for v in svals):
        # derive precision/scale
        max_prec = 1
        max_scale = 0
        for v in svals:
            if "." in v:
                parts = v.split(".")
                scale = len(parts[1])
                ip = parts[0].lstrip("-+")
                prec = len(ip) + scale
            else:
                scale = 0
                ip = v.lstrip("-+")
                prec = len(ip)
            max_prec = max(max_prec, min(38, prec))
            max_scale = max(max_scale, min(15, scale))
        if max_prec < max_scale:
            max_prec = max_scale
        return ("decimal", None, (max(1, min(38, max_prec)), max(0, min(15, max_scale))))

    # Date/Time?
    if all(_is_date(v) for v in svals):
        return ("date", None, None)
    if all(_is_time(v) for v in svals):
        return ("time", None, None)

    # Fallback to nvarchar with observed max length
    maxlen = max(len(v) for v in svals)
    length = min(max_string_len_cap, maxlen if maxlen > 0 else 1)
    return ("nvarchar", length, None)

def _df_to_mapping(df: pd.DataFrame, source_kind: str, source_name: str,
                   max_string_len_cap: int = 255) -> pd.DataFrame:
    rows = []
    for col in df.columns.tolist():
        col_values = df[col].dropna().astype(str).tolist()
        btype, nlen, dec = _infer_type(col_values, max_string_len_cap=max_string_len_cap)
        type_str = None
        if btype == "nvarchar":
            type_str = f"nvarchar({nlen if nlen and nlen>0 else 'max'})"
        elif btype == "decimal":
            prec, scale = dec or (38, 15)
            type_str = f"decimal({prec},{scale})"
        else:
            type_str = btype

        rows.append({
            "customer_field": col,
            "biqh_import_field": col,
            "description": "",
            "biqh_column_data_type": type_str,
            "biqh_table_schema": DEFAULT_SCHEMA,
            "biqh_parent_table_name": DEFAULT_PARENT_TABLE,
            "biqh_parent_column_name": col,
            "biqh_relation_table_name": "",
            "biqh_relation_column_name": "",
            "biqh_relation_column_value": "",
            "biqh_relation_column_data_type": "",
            "biqh_link_table_name": "",
            "remarks": f"source={source_kind}:{source_name}",
        })
    return pd.DataFrame(rows, columns=REQUIRED_MAPPING_COLUMNS)

def _paths_to_mapping(paths: List[str], samples: Dict[str, List[Any]], source_kind: str, source_name: str,
                      max_string_len_cap: int = 255) -> pd.DataFrame:
    rows = []
    for idx, p in enumerate(paths):
        vals = samples.get(p, [])
        btype, nlen, dec = _infer_type(vals, max_string_len_cap=max_string_len_cap)
        if btype == "nvarchar":
            type_str = f"nvarchar({nlen if nlen and nlen>0 else 'max'})"
        elif btype == "decimal":
            prec, scale = dec or (38, 15)
            type_str = f"decimal({prec},{scale})"
        else:
            type_str = btype

        safe_name = _path_to_safe_name(p)
        rows.append({
            "customer_field": p,                     # store full path here
            "biqh_import_field": safe_name,          # normalized flat name
            "description": "",
            "biqh_column_data_type": type_str,
            "biqh_table_schema": DEFAULT_SCHEMA,
            "biqh_parent_table_name": DEFAULT_PARENT_TABLE,
            "biqh_parent_column_name": safe_name,
            "biqh_relation_table_name": "",
            "biqh_relation_column_name": "",
            "biqh_relation_column_value": "",
            "biqh_relation_column_data_type": "",
            "biqh_link_table_name": "",
            "remarks": f"source={source_kind}:{source_name}",
        })
    return pd.DataFrame(rows, columns=REQUIRED_MAPPING_COLUMNS)

_slug_pat = re.compile(r"[^A-Za-z0-9]+")

def _path_to_safe_name(p: str) -> str:
    """
    Convert JSONPath/XPath to a BIQH-friendly flat column name.
    E.g. $.orders[*].id -> orders_id ; /Root/Item/Name -> Root_Item_Name
    """
    if p.startswith("$."):
        p = p[2:]
    if p.startswith("/"):
        p = p[1:]
    p = p.replace("[*]", "")
    p = p.replace("@", "attr_")
    tokens = [t for t in re.split(r"[./]", p) if t]
    flat = "_".join(tokens)
    flat = _slug_pat.sub("_", flat).strip("_")
    if not flat:
        flat = "field"
    # ensure starts with a letter for SQL friendliness
    if not flat[0].isalpha():
        flat = "c_" + flat
    return flat[:128]  # reasonable cap

# ---------- CLI ----------

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Parse a file and emit BIQH mapping CSV")
    ap.add_argument("file", help="Path to input file (csv/xlsx/json/xml)")
    ap.add_argument("--sheet", help="Excel sheet index or name", default=None)
    ap.add_argument("--out", help="Output mapping CSV path", default=None)
    args = ap.parse_args()

    sheet = None
    if args.sheet is not None:
        try:
            sheet = int(args.sheet)
        except Exception:
            sheet = args.sheet

    mapping = parse_file(args.file, sheet=sheet)
    if args.out:
        save_mapping_csv(mapping, args.out)
        print(f"Wrote mapping: {args.out}")
    else:
        # Print first rows
        print(mapping.head().to_string(index=False))

if __name__ == "__main__":
    main()
