#!/usr/bin/env python3
"""
json_xml_feed_builder_merged_v2.py

Purpose
-------
Combine the strong **hierarchy discovery** from json_xml_feed_builder (XML/JSON vendor-driven
TableDefinitions + ColumnDefinitions) with the **full BIQH feed structure** emitted by
json_feed_builder_mssql_V2 (FileDefinitions metadata block + Map/RowMap/Authorize blocks),
while keeping mapping **optional**. If mapping is missing, we still emit a valid
FileDefinitions hierarchy; if mapping is present, we also emit Map/RowMap/Authorize blocks
(based on the mapping, with DB validation optional).

Usage (CLI)
-----------
python json_xml_feed_builder_merged_v2.py \
  --template file_structure_V24.json \
  --vendor-file vendor.xml \
  --feed-id 123 --feed-name "My XML Feed" \
  --provider-id 61 --import-frequency-id 1 \
  --mapping mtge_ref_mapping.csv  # optional \
  --out-dir ./out

Usage (in code)
---------------
from json_xml_feed_builder_merged_v2 import build_feed_json, load_template, load_mapping
feed = build_feed_json(
    template=load_template("file_structure_V24.json"),
    mapping=load_mapping("mtge_ref_mapping.csv"),   # or None / ""
    vendor_file="vendor.xml",
    feed_id=123, feed_name="My XML Feed",
    provider_id=61, import_frequency_id=1,
    schema=None, custom_table_name="CustomTable"
)

Notes
-----
- pyodbc is optional. If the connection string is omitted or pyodbc is unavailable, the
  script runs offline and skips DB validation.
- Mapping is optional. Without mapping, only FileDefinitions are populated. With mapping,
  MapDefinitions / RowMapDefinitions / AuthorizeDefinitions are also created (V2 style).
"""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

# =====================
# Defaults & ENV
# =====================
DEFAULTS = {
    "template": "file_structure_V24.json",
    "mapping": "mtge_ref_mapping.csv",  # optional
    "vendor_file": "mtge_cmbs_namr.out.20250715",
    "feed_id": 9991,
    "feed_name": "test_feed_creation_v2",
    "provider_id": 61,
    "import_frequency_id": 1,
    "schema": "beta",
    "out_dir": ".",
    # Optional SQL Server connection string to IntBIQHModel.
    # Example (Windows Auth): DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=IntBIQHModel;Trusted_Connection=yes;Encrypt=no
    # Example (SQL Login):   DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=IntBIQHModel;UID=sa;PWD=***;Encrypt=no
    "mssql_conn_str": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=intdbserver.maanlander.local;DATABASE=IntBIQHModel;UID=Ukrain;PWD=Ukrain01!;Encrypt=no",
}

ENV_MAP = {
    "template": "FEED_TEMPLATE",
    "mapping": "FEED_MAPPING",
    "vendor_file": "FEED_VENDOR_FILE",
    "feed_id": "FEED_ID",
    "feed_name": "FEED_NAME",
    "provider_id": "FEED_PROVIDER_ID",
    "import_frequency_id": "FEED_IMPORT_FREQUENCY_ID",
    "schema": "FEED_SCHEMA",
    "out_dir": "FEED_OUT_DIR",
    "mssql_conn_str": "FEED_MSSQL_CONN_STR",
}

DATA_TYPE_MAP = {
    "nvarchar": 1, "int": 2, "decimal": 3, "date": 4, "datetime": 5,
    "time": 6, "boolean": 7, "bit": 7, "bigint": 8, "binary": 9, "varbinary": 9, "any": 10,
}

# =====================
# Mapping & Template Loaders
# =====================

def load_template(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_mapping(path: str | None) -> pd.DataFrame:
    expected = ['customer_field','biqh_import_field','description','biqh_column_data_type',
                'biqh_table_schema','biqh_parent_table_name','biqh_parent_column_name',
                'biqh_relation_table_name','biqh_relation_column_name','biqh_relation_column_value',
                'biqh_relation_column_data_type','biqh_link_table_name','remarks']
    if not path or not str(path).strip() or not Path(path).exists():
        return pd.DataFrame([], columns=expected)
    df = pd.read_csv(path, dtype=str).fillna("")
    missing = set(expected) - set(df.columns)
    if missing:
        raise ValueError(f"Mapping file is missing required columns: {missing}")
    return df

# =====================
# String helpers
# =====================

def parse_biqh_type(type_str: str):
    s = (type_str or "").strip().lower()
    m = re.match(r"nvarchar\((max|\d+)\)", s)
    if m:
        val = m.group(1)
        length = -1 if val == "max" else int(val)
        return DATA_TYPE_MAP["nvarchar"], length, None
    m = re.match(r"decimal\((\d+)\s*,\s*(\d+)\)", s)
    if m:
        return DATA_TYPE_MAP["decimal"], None, (int(m.group(1)), int(m.group(2)))
    for k, v in DATA_TYPE_MAP.items():
        if s == k:
            return v, None, None
    return DATA_TYPE_MAP["nvarchar"], -1, None


def sanitize_feed_name(name: str) -> str:
    no_underscore = name.replace("_", " ")
    return re.sub(r"\s+", " ", no_underscore).strip()


def infer_schema(feed_name: str, explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    tokens = re.findall(r"[A-Za-z]+", feed_name)
    if not tokens:
        return "beta"
    first = tokens[0].lower()
    caps = "".join(t.lower() for t in tokens[1:] if t.isupper())
    schema = (first + caps)[:30]
    return schema or "beta"


def file_type_id_and_sheet(vendor_file: str):
    ext = Path(vendor_file).suffix.lower()
    if ext in [".csv", ".txt"]: return 1, None
    if ext in [".xml"]: return 2, None
    if ext in [".json"]: return 3, None
    if ext in [".xlsx", ".xls"]: return 4, 1
    return 1, None


def build_filename_regex(vendor_file: str) -> str:
    fn = Path(vendor_file).name
    base, ext = os.path.splitext(fn)
    m = re.match(r"^(\d{8})_(.+)$", base)
    if m:
        rest = re.escape(m.group(2))
        return rf"^(\d{{8}})_{rest}{re.escape(ext)}$"
    return rf"^{re.escape(base)}{re.escape(ext)}$"

# =====================
# Optional DB helpers (pyodbc)
# =====================
@dataclass
class DbMeta:
    valid_tables: Set[str]
    rowmap_allowed_tables: Set[str]
    column_meta: Dict[str, Dict[str, int]]


def try_connect(conn_str: str):
    if not conn_str:
        return None
    try:
        import pyodbc  # type: ignore
    except Exception as e:
        print(f"[WARN] pyodbc not available: {e}. Running offline (no DB validation).")
        return None
    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
        return conn
    except Exception as e:
        print(f"[WARN] Could not connect to SQL Server: {e}. Running offline.")
        return None


def db_fetch_valid_tables(conn) -> Set[str]:
    sql = (
        "SELECT DISTINCT Name FROM dbo.[Table] "
        "WHERE Name NOT LIKE '%RowAuth%' AND Name NOT LIKE '%Translation%' AND Name NOT LIKE '%RowLink%';"
    )
    try:
        df = pd.read_sql(sql, conn)
        return set(df["Name"].astype(str).str.strip())
    except Exception as e:
        print(f"[WARN] Failed to fetch valid tables: {e}")
        return set()


def db_fetch_rowmap_allowed(conn) -> Set[str]:
    sql = "SELECT Name FROM dbo.[Table] WHERE MapTableName IS NOT NULL;"
    try:
        df = pd.read_sql(sql, conn)
        return set(df["Name"].astype(str).str.strip())
    except Exception as e:
        print(f"[WARN] Failed to fetch rowmap-eligible tables: {e}")
        return set()


def db_fetch_column_meta(conn, table: str) -> Dict[str, int]:
    sql = "SELECT Name, ColumnTypeId FROM dbo.[Column] WHERE TableName = ?;"
    try:
        import pyodbc  # noqa
        df = pd.read_sql(sql, conn, params=[table])
        return {str(r["Name"]).strip(): int(r["ColumnTypeId"]) for _, r in df.iterrows()}
    except Exception as e:
        print(f"[WARN] Failed to fetch column meta for {table}: {e}")
        return {}


def load_db_meta(conn) -> DbMeta:
    valid = db_fetch_valid_tables(conn) if conn else set()
    rowmap_allowed = db_fetch_rowmap_allowed(conn) if conn else set()
    return DbMeta(valid_tables=valid, rowmap_allowed_tables=rowmap_allowed, column_meta={})

# =====================
# Vendor-driven hierarchy (XML/JSON)
# =====================
# ---- XML helpers ----

def _xpath_segments(xp: str) -> List[str]:
    xp = (xp or "").strip()
    if not xp:
        return []
    if xp.startswith("/"):
        xp = xp[1:]
    if xp.startswith("./"):
        xp = xp[2:]
    return [seg for seg in xp.split("/") if seg]


def _rel_xpath(from_abs: str, to_abs: str) -> str:
    f = _xpath_segments(from_abs)
    t = _xpath_segments(to_abs)
    i = 0
    while i < len(f) and i < len(t) and f[i] == t[i]:
        i += 1
    rel = t[i:]
    return "./" + "/".join(rel) if rel else "."


def _xml_abs_paths_from_vendor(xml_path: str) -> List[str]:
    import xml.etree.ElementTree as ET
    tree = ET.parse(xml_path)
    root = tree.getroot()
    parent_of = {root: None}
    for cur in list(root.iter()):
        for ch in list(cur):
            parent_of[ch] = cur
    def _segments(e):
        segs = []
        while e is not None:
            segs.append(e.tag)
            e = parent_of.get(e)
        return list(reversed(segs))
    paths = set()
    for elem in root.iter():
        if len(list(elem)) == 0:
            text = (elem.text or "").strip()
            if text != "":
                xp = "/" + "/".join(_segments(elem))
                paths.add(xp)
        for aname, _aval in elem.attrib.items():
            xp = "/" + "/".join(_segments(elem)) + f"/@{aname}"
            paths.add(xp)
    return sorted(paths)


def build_xml_table_definitions_from_vendor(xml_path: str, mapping: pd.DataFrame) -> List[Dict]:
    leaf_paths = _xml_abs_paths_from_vendor(xml_path)
    if not leaf_paths:
        return []

    def _parent_xpath(xp: str) -> Optional[str]:
        segs = _xpath_segments(xp)
        if not segs:
            return None
        if segs[-1].startswith("@"):
            segs = segs[:-1]
        segs = segs[:-1]
        return "/" + "/".join(segs) if segs else "/"

    parent_to_leaves: Dict[str, List[str]] = {}
    for leaf in leaf_paths:
        parent = _parent_xpath(leaf) or "/"
        parent_to_leaves.setdefault(parent, []).append(leaf)

    root_tag = _xpath_segments(leaf_paths[0])[0]
    root_path = f"/{root_tag}"
    table_nodes = set([root_path])
    for parent, leaves in parent_to_leaves.items():
        if len(leaves) >= 2:
            table_nodes.add(parent)

    def _ancestors(xp: str) -> List[str]:
        segs = _xpath_segments(xp)
        return ["/" + "/".join(segs[:i]) for i in range(1, len(segs))]

    for n in list(table_nodes):
        for a in _ancestors(n):
            table_nodes.add(a)

    def _parent_node(xp: str) -> Optional[str]:
        segs = _xpath_segments(xp)
        if len(segs) <= 1:
            return None
        return "/" + "/".join(segs[:-1])

    children: Dict[str, List[str]] = {}
    for node in sorted(table_nodes):
        par = _parent_node(node)
        if par and par in table_nodes:
            children.setdefault(par, []).append(node)
    for k in children:
        children[k].sort()

    map_by_path = {}
    if mapping is not None and not mapping.empty and "customer_field" in mapping.columns:
        for _, r in mapping.iterrows():
            p = (r.get("customer_field") or "").strip()
            if p.startswith("/"):
                map_by_path[p] = r

    def _col_name_from_leaf(leaf_abs: str) -> str:
        segs = _xpath_segments(leaf_abs)
        last = segs[-1]
        base = last[1:] if last.startswith("@") else last
        name = re.sub(r"[^A-Za-z0-9_]+", "_", base).strip("_") or "field"
        if not name[0].isalpha():
            name = "c_" + name
        return name[:128]

    def _coldefs_for_node(node_abs: str) -> List[Dict]:
        rows = []
        for leaf in parent_to_leaves.get(node_abs, []):
            rel = _rel_xpath(node_abs, leaf)
            name = _col_name_from_leaf(leaf)
            dtid, length, dec = (10, None, None)
            precision = scale = None
            r = map_by_path.get(leaf)
            if r is not None:
                name = r.get("biqh_parent_column_name") or r.get("biqh_import_field") or name
                _dtid, length, dec = parse_biqh_type(r.get("biqh_column_data_type"))
                dtid = _dtid
                if dec:
                    precision, scale = dec
            rows.append({
                "HeaderName": None, "SampleData": None,
                "Name": name, "DataTypeId": dtid, "Length": length if dtid in (1,10) else None,
                "Precision": precision if dtid == 3 else None, "Format": None, "Description": None,
                "Scale": scale if dtid == 3 else None, "AllowLeadingWhite": False, "AllowTrailingWhite": False,
                "AllowLeadingSign": False, "AllowTrailingSign": False, "AllowParentheses": False,
                "AllowDecimalPoint": dtid == 3, "AllowThousands": False, "AllowExponent": False,
                "AllowCurrencySymbol": False, "AllowPercentage": False, "CultureInfo": None,
                "ColumnTypeId": 5, "Start": None, "End": None, "ColumnNumber": 0, "Script": None,
                "UseInnerXml": False, "XPath": rel, "SourceName": None, "JsonPath": None,
                "NullAliases": None, "Multiplier": None, "TrueAliases": None, "FalseAliases": None,
                "RetrievalStatisticsEnabled": False, "Validations": []
            })
        return rows

    def _make_table(node_abs: str) -> Dict:
        tname = "_".join(_xpath_segments(node_abs)) or "Root"
        parent = _parent_node(node_abs)
        node_xpath_for_json = node_abs if (parent is None) else _rel_xpath(parent, node_abs).lstrip("./")
        tbl = {
            "TableName": tname, "NodeXPath": node_xpath_for_json if parent is not None else node_abs,
            "NodeJsonPath": None, "SqlQuery": None, "ChildTableDefinitions": [],
            "ColumnDefinitions": _coldefs_for_node(node_abs), "IndexDefinitions": []
        }
        for ch in children.get(node_abs, []):
            tbl["ChildTableDefinitions"].append(_make_table(ch))
        return tbl

    roots = [n for n in table_nodes if (_parent_node(n) not in table_nodes)]
    return [_make_table(r) for r in sorted(roots)]

# ---- JSON helpers ----

def _json_abs_paths_and_arrays(obj: Any, base: str = "$") -> Tuple[Set[str], Set[str]]:
    leafs: Set[str] = set()
    tables: Set[str] = set()
    def walk(o, path):
        if isinstance(o, dict):
            if not o:
                leafs.add(path)
            else:
                for k, v in o.items():
                    walk(v, f"{path}.{k}")
        elif isinstance(o, list):
            if any(isinstance(it, dict) for it in o):
                tables.add(f"{path}[*]")
                for it in o[:1]:
                    if isinstance(it, dict):
                        walk(it, f"{path}[*]")
            else:
                leafs.add(f"{path}[*]")
        else:
            leafs.add(path)
    walk(obj, base)
    return leafs, tables


def _json_rel(from_abs: str, to_abs: str) -> str:
    f = [t for t in re.split(r"\.|\[\*\]", from_abs) if t and t != "$"]
    t = [ti for ti in re.split(r"\.|\[\*\]", to_abs) if ti and ti != "$"]
    i = 0
    while i < len(f) and i < len(t) and f[i] == t[i]:
        i += 1
    remainder = t[i:]
    if not remainder:
        return "$"
    return "$." + ".".join(remainder)


def build_json_table_definitions_from_vendor(json_path: str, mapping: pd.DataFrame) -> List[Dict]:
    data = json.loads(Path(json_path).read_text(encoding="utf-8"))
    if isinstance(data, list) and data and isinstance(data[0], dict):
        root = {"_root": data}
    else:
        root = data
    leafs, tables = _json_abs_paths_and_arrays(root, "$")
    if not tables:
        tables = {"$"}

    def is_ancestor(a: str, b: str) -> bool:
        return b.startswith(a) and a != b

    parents: Dict[str, Optional[str]] = {}
    for t in sorted(tables, key=lambda s: (s.count("."), len(s))):
        par = None
        for cand in tables:
            if is_ancestor(cand, t):
                if par is None or len(cand) > len(par):
                    par = cand
        parents[t] = par

    table_to_leafs: Dict[str, List[str]] = {t: [] for t in tables}
    for leaf in leafs:
        anc = None
        for t in tables:
            if leaf.startswith(t):
                if anc is None or len(t) > len(anc):
                    anc = t
        if anc is None and "$" in tables:
            anc = "$"
        if anc is not None:
            table_to_leafs.setdefault(anc, []).append(leaf)

    map_by_path: Dict[str, pd.Series] = {}
    if mapping is not None and not mapping.empty and "customer_field" in mapping.columns:
        for _, r in mapping.iterrows():
            p = (r.get("customer_field") or "").strip()
            if p.startswith("$."):
                map_by_path[p] = r

    def _tname(tpath: str) -> str:
        if tpath == "$":
            return "Root"
        tokens = [t for t in re.split(r"\.|\[\*\]", tpath) if t and t != "$"]
        return "_".join(tokens)

    def _coldefs_for_table(tnode: str) -> List[Dict]:
        out = []
        for leaf in sorted(set(table_to_leafs.get(tnode, []))):
            rel = _json_rel(tnode, leaf)
            last = [t for t in re.split(r"\.|\[\*\]", leaf) if t and t != "$"][-1]
            name = re.sub(r"[^A-Za-z0-9_]+", "_", last).strip("_") or "field"
            dtid, length, dec = (10, None, None)
            precision = scale = None
            r = map_by_path.get(leaf)
            if r is not None:
                name = r.get("biqh_parent_column_name") or r.get("biqh_import_field") or name
                _dtid, length, dec = parse_biqh_type(r.get("biqh_column_data_type"))
                dtid = _dtid
                if dec:
                    precision, scale = dec
            out.append({
                "HeaderName": None, "SampleData": None,
                "Name": name, "DataTypeId": dtid, "Length": length if dtid in (1,10) else None,
                "Precision": precision if dtid == 3 else None, "Format": None, "Description": None,
                "Scale": scale if dtid == 3 else None, "AllowLeadingWhite": False, "AllowTrailingWhite": False,
                "AllowLeadingSign": False, "AllowTrailingSign": False, "AllowParentheses": False,
                "AllowDecimalPoint": dtid == 3, "AllowThousands": False, "AllowExponent": False,
                "AllowCurrencySymbol": False, "AllowPercentage": False, "CultureInfo": None,
                "ColumnTypeId": 6, "Start": None, "End": None, "ColumnNumber": 0, "Script": None,
                "UseInnerXml": False, "XPath": None, "SourceName": None, "JsonPath": rel,
                "NullAliases": None, "Multiplier": None, "TrueAliases": None, "FalseAliases": None,
                "RetrievalStatisticsEnabled": False, "Validations": []
            })
        return out

    def _rel_jsonpath(par: Optional[str], child: str) -> str:
        if par is None:
            return child
        rel = _json_rel(par, child)
        return rel.lstrip("$.")

    roots = [t for t, par in parents.items() if par is None]
    if not roots and "$" in tables:
        roots = ["$"]

    def _make_table(tpath: str) -> Dict:
        par = parents.get(tpath)
        node_json = _rel_jsonpath(par, tpath) if par is not None else tpath
        tbl = {
            "TableName": _tname(tpath), "NodeXPath": None, "NodeJsonPath": node_json, "SqlQuery": None,
            "ChildTableDefinitions": [], "ColumnDefinitions": _coldefs_for_table(tpath), "IndexDefinitions": []
        }
        for ch in [c for c, p in parents.items() if p == tpath]:
            tbl["ChildTableDefinitions"].append(_make_table(ch))
        return tbl

    return [_make_table(r) for r in roots]

# =====================
# Mapping -> ColumnDefs (flat fallback)
# =====================

def build_column_definitions_from_mapping(mapping: pd.DataFrame, filetype_id: int) -> List[Dict]:
    coldefs = []
    for idx, r in mapping.iterrows():
        src_header = r["customer_field"] or r["biqh_import_field"]
        target_name = r["biqh_parent_column_name"] or r["biqh_import_field"]
        dtid, length, dec = parse_biqh_type(r["biqh_column_data_type"])
        precision = scale = None
        if dec:
            precision, scale = dec
        coldef = {
            "HeaderName": src_header or None, "SampleData": None,
            "Name": target_name or None, "DataTypeId": dtid,
            "Length": length if dtid in (1,10) else None,
            "Precision": precision if dtid == 3 else None, "Format": None,
            "Description": (r["description"] or None) if (r["description"] or "").strip() else None,
            "Scale": scale if dtid == 3 else None,
            "AllowLeadingWhite": False, "AllowTrailingWhite": False, "AllowLeadingSign": False, "AllowTrailingSign": False,
            "AllowParentheses": False, "AllowDecimalPoint": dtid == 3, "AllowThousands": False, "AllowExponent": False,
            "AllowCurrencySymbol": False, "AllowPercentage": False, "CultureInfo": None,
            "ColumnTypeId": 6 if filetype_id == 3 else (5 if filetype_id == 2 else 2),
            "Start": None, "End": None, "ColumnNumber": idx, "Script": None, "UseInnerXml": False,
            "XPath": None if filetype_id != 2 else None, "JsonPath": None if filetype_id != 3 else None,
            "NullAliases": None, "Multiplier": None, "TrueAliases": None, "FalseAliases": None,
            "SourceName": src_header or None, "RetrievalStatisticsEnabled": False, "Validations": [],
        }
        if dtid == 1 and (length is not None) and length > 0:
            coldef["Validations"].append({"ValidationTypeId": 10, "Parameter": str(length),
                                          "Message": None, "Condition": None, "IsError": True, "IsTechnical": True})
        coldefs.append(coldef)
    return coldefs

# =====================
# Map / RowMap / Authorize blocks (from mapping)
# =====================

def choose_provider_key_source(rows: pd.DataFrame):
    candidates = ["ProviderKey","OwnProviderId","CompanyIdentifier","Identifier","Isin","Symbol","Code","Name"]
    for target in candidates:
        r = rows[rows["biqh_parent_column_name"].str.lower() == target.lower()]
        if not r.empty:
            source = r.iloc[0]["customer_field"] or r.iloc[0]["biqh_import_field"]
            return source, "ProviderKey"
    string_rows = rows[rows["biqh_column_data_type"].str.lower().str.startswith("nvarchar")]
    if not string_rows.empty:
        source = string_rows.iloc[0]["customer_field"] or string_rows.iloc[0]["biqh_import_field"]
        return source, "ProviderKey"
    return None


def infer_link_target_table(tgt_col: str, mapping_row: pd.Series) -> Optional[str]:
    link = (mapping_row.get("biqh_link_table_name") or "").strip()
    if link:
        return link
    if tgt_col.lower().endswith("id") and len(tgt_col) > 2:
        return tgt_col[:-2]
    return None


def build_map_sql_for_table(
    parent_table: str,
    rows: pd.DataFrame,
    custom_table_name: str,
    provider_key_sources: Dict[str, str],
    dbmeta: Optional[DbMeta] = None,
    conn=None,
) -> str:
    col_meta = {}
    if dbmeta is not None:
        if parent_table not in dbmeta.column_meta and conn is not None:
            dbmeta.column_meta[parent_table] = db_fetch_column_meta(conn, parent_table)
        col_meta = dbmeta.column_meta.get(parent_table, {})

    select_lines: List[str] = []
    join_lines: List[str] = []

    have_provider_key = any(rows["biqh_parent_column_name"].str.lower() == "providerkey")
    if not have_provider_key:
        inferred = choose_provider_key_source(rows)
        if inferred:
            src, _ = inferred
            select_lines.append(f"\tr.{src} AS [ProviderKey]")

    joins_by_table: Dict[str, str] = {}

    for _, r in rows.iterrows():
        src = r["customer_field"] or r["biqh_import_field"]
        tgt = r["biqh_parent_column_name"] or r["biqh_import_field"]
        if not src or not tgt:
            continue

        coltype = int(col_meta.get(tgt, 1)) if (tgt in col_meta) else 1

        if coltype == 2:
            link_table = infer_link_target_table(tgt, r)
            if not link_table:
                select_lines.append(f"\tr.{src} AS [{tgt}]")
                continue

            link_pk_src = provider_key_sources.get(link_table) or src

            alias = f"l_{re.sub('[^A-Za-z0-9]', '', link_table.lower())[:12]}"
            if link_table not in joins_by_table:
                joins_by_table[link_table] = alias
                join_lines.append(
                    f"\tINNER JOIN dbo.{link_table} {alias} "
                    f"ON {alias}.ProviderKey = r.{link_pk_src} AND {alias}.ProviderId = @ProviderId"
                )
            select_lines.append(f"\t{alias}.Id AS [{tgt}]")
        else:
            select_lines.append(f"\tr.{src} AS [{tgt}]")

    rel_rows = rows[rows["biqh_relation_table_name"].str.strip() != ""]
    seen_rel = set()
    for _, rr in rel_rows.iterrows():
        rtable = rr["biqh_relation_table_name"]
        if not rtable or rtable in seen_rel:
            continue
        seen_rel.add(rtable)
        alias = f"b_{re.sub('[^A-Za-z0-9]', '', rtable.lower())[:12]}"
        rcol = rr["biqh_relation_column_name"] or "ProviderKey"
        rval = rr["biqh_relation_column_value"]
        dtype = (rr["biqh_relation_column_data_type"] or "").lower()
        const = f"N'{rval}'" if (dtype.startswith("nvarchar") or dtype.startswith("nchar")) else (rval if rval else "NULL")
        join_lines.append(
            f"\tINNER JOIN dbo.{rtable} {alias} ON {alias}.{rcol} = {const} AND {alias}.ProviderId = @ProviderId"
        )

    if not select_lines:
        select_lines.append("\t-- TODO: add columns")

    sql = (
        "SELECT DISTINCT\n" + ",\n".join(select_lines) +
        f"\nFROM\n\t@Schema.{custom_table_name} r \n"
    )
    if join_lines:
        sql += "\n" + "\n".join(join_lines) + "\n"
    sql += (
        "\nWHERE\n"
        "\tr.Record_ImportId = @ImportId\n"
        "\tAND r.Record_HasError = 0\n"
    )
    return sql


def build_link_map_sql(
    link_table: str,
    mapping: pd.DataFrame,
    custom_table_name: str,
    provider_key_sources: Dict[str, str],
) -> Optional[str]:
    participants = [t for t, src in provider_key_sources.items() if src]
    if len(participants) < 2:
        return None

    join_lines, id_selects = [], []
    for pt in sorted(set(participants)):
        alias = f"b_{re.sub('[^A-Za-z0-9]', '', pt.lower())[:12]}"
        id_col = f"{pt}Id" if not pt.lower().endswith("id") else pt
        src_field = provider_key_sources.get(pt)
        if not src_field:
            continue
        id_selects.append(f"\t{alias}.Id AS [{id_col}]")
        join_lines.append(
            f"\tINNER JOIN dbo.{pt} {alias} ON {alias}.ProviderKey = r.{src_field} AND {alias}.ProviderId = @ProviderId"
        )

    if not id_selects:
        return None

    sql = (
        "SELECT DISTINCT\n" + ",\n".join(id_selects) +
        f"\nFROM\n\t@Schema.{custom_table_name} r \n"
    )
    sql += "\n" + "\n".join(join_lines) + "\n"
    sql += (
        "\nWHERE\n"
        "\tr.Record_ImportId = @ImportId\n"
        "\tAND r.Record_HasError = 0\n"
    )
    return sql

# =====================
# Feed build (V2-style) + vendor-driven FileDefinitions
# =====================

def build_feed_json(
    template: Dict,
    mapping: pd.DataFrame,
    vendor_file: str,
    feed_id: int,
    feed_name: str,
    provider_id: int,
    import_frequency_id: int,
    schema: Optional[str] = None,
    custom_table_name: str = "CustomTable",
    dbmeta: Optional[DbMeta] = None,
    conn=None,
) -> Dict:
    feed = json.loads(json.dumps(template))  # deep copy
    feed_obj = feed["Feed"]
    feed_obj["Id"] = int(feed_id)
    feed_obj["ProviderId"] = int(provider_id)
    feed_obj["Name"] = sanitize_feed_name(feed_name)
    feed_obj["Description"] = None
    feed_obj["ImportFrequencyId"] = int(import_frequency_id)
    feed_obj["Schema"] = infer_schema(feed_name, schema)
    feed_obj["ImportStoredProcedure"] = None
    feed_obj["IsCreatedManually"] = False
    feed_obj["ValidatorExe"] = None
    feed_obj["MapStoredProcedure"] = None
    feed_obj["UseGenericImporter"] = True
    feed_obj["UseGenericValidator"] = True
    feed_obj["UseGenericMapper"] = True
    feed_obj["IsManualValidationEnabled"] = False
    feed_obj["IsWaitForManualValidation"] = False
    feed_obj["IsAdjustingCorrectValues"] = False
    feed_obj["AlwaysIncremental"] = False
    feed_obj["UseGenericAuthorizer"] = True
    feed_obj["AuthorizeStoredProcedure"] = None

    ftype_id, sheet_num = file_type_id_and_sheet(vendor_file)
    file_name_regex = build_filename_regex(vendor_file)
    file_def = {
        "FileTypeId": ftype_id, "Name": None, "FileNameRegex": file_name_regex,
        "FileNameRegexDescription": None, "CsvDelimiterCharacter": None if ftype_id != 1 else ",",
        "HasHeader": None, "SubsetGroupNumber": None, "DynamicColumnCount": True,
        "DefaultColumnNamePrefix": None, "TrimWhiteSpaces": True, "AdvancedEscapingEnabled": True,
        "QuoteCharacter": None, "DoubleQuoteEscapingEnabled": True, "ColumnHeaderTypeSeparator": None,
        "ReadHeaders": True, "CheckHeaders": False, "CheckUnexpectedHeaders": False, "UseEmbargoDateTime": False,
        "EmbargoDateTimeComposite": None, "IgnoreColumnsWithEmptyHeader": True, "SkipEmptyLines": True,
        "SkipFirstNumberOfLines": 0, "EndOfFileRegex": None, "CheckZippedFileNameByRegex": False,
        "DefaultMicrosoftStandardTimeZoneId": None, "NumberDecimalSeparator": "." if ftype_id in (1,4) else None,
        "NumberGroupSeparator": None, "RootXPath": None, "XmlNamespaces": None, "RootJsonPath": None,
        "DetectEncoding": False, "SheetNumber": sheet_num, "SortOrder": None, "TableDefinitions": [],
    }

    # Vendor-driven hierarchy for XML/JSON
    if ftype_id == 2:
        tbls = build_xml_table_definitions_from_vendor(vendor_file, mapping)
        if tbls:
            file_def["TableDefinitions"] = tbls
    elif ftype_id == 3:
        tbls = build_json_table_definitions_from_vendor(vendor_file, mapping)
        if tbls:
            file_def["TableDefinitions"] = tbls

    # Fallback to flat columns if no hierarchy or non XML/JSON
    if not file_def["TableDefinitions"]:
        cols = build_column_definitions_from_mapping(mapping, ftype_id)
        table_def = {
            "TableName": custom_table_name, "NodeXPath": None, "NodeJsonPath": None, "SqlQuery": None,
            "ChildTableDefinitions": [], "ColumnDefinitions": cols, "IndexDefinitions": []
        }
        file_def["TableDefinitions"].append(table_def)

    feed_obj["FileDefinitions"] = [file_def]

    # Mapping dependent blocks
    if mapping is None or mapping.empty:
        # No mapping -> omit Map/RowMap/Authorize blocks
        feed_obj["MapDefinitions"] = []
        feed_obj["RowMapDefinitions"] = []
        feed_obj["AuthorizeDefinitions"] = []
        return feed

    # ProviderKey sources per parent table
    provider_key_sources: Dict[str, str] = {}
    for pt, rows in mapping.groupby("biqh_parent_table_name"):
        inf = choose_provider_key_source(rows)
        if inf:
            provider_key_sources[pt] = inf[0]

    # Prepare DB metadata if available
    if dbmeta is None and conn is not None:
        dbmeta = load_db_meta(conn)

    def is_valid_table(t: str) -> bool:
        if not t or not str(t).strip():
            return False
        if dbmeta and dbmeta.valid_tables:
            return t in dbmeta.valid_tables
        return True

    # MapDefinitions
    map_block = {"Name": None, "Description": None, "UseTransaction": True, "UseSubsetKey": False,
                 "BatchSize": 0, "BiqhTableMaps": []}

    parent_groups = dict(tuple(mapping.groupby("biqh_parent_table_name")))
    relation_tables = sorted(set([t for t in mapping["biqh_relation_table_name"].unique() if str(t).strip()]))

    sort_counter = 1
    for parent_table, rows in parent_groups.items():
        if not is_valid_table(parent_table):
            print(f"[INFO] Skip invalid/unknown table (parent): {parent_table}")
            continue
        sql = build_map_sql_for_table(parent_table, rows, custom_table_name, provider_key_sources, dbmeta, conn)
        map_block["BiqhTableMaps"].append(
            {"_allowDeletes": {"Value": None, "HasValue": True},
             "_allowDeletesPolicyId": {"Value": None, "HasValue": True},
             "BiqhTableName": parent_table, "SortOrder": sort_counter, "SqlQuery": sql,
             "UseBiql": False, "BiqlQuery": None, "IsMergeStatement": True, "UseSubsetKey": None,
             "AllowDeletesPolicyId": None, "BiqhTableColumnMaps": None, "DelayTypeMaps": []}
        )
        sort_counter += 1

    for rtable in relation_tables:
        if not is_valid_table(rtable):
            print(f"[INFO] Skip invalid/unknown table (relation): {rtable}")
            continue
        stub_sql = (
            f"-- Relation table [{rtable}] is referenced for filtering; add concrete mapping if needed.\n"
            f"SELECT DISTINCT\n\t-- TODO: map columns for [{rtable}] if needed\n"
            f"FROM\n\t@Schema.{custom_table_name} r\n"
            "WHERE\n\tr.Record_ImportId = @ImportId\n\tAND r.Record_HasError = 0\n"
        )
        map_block["BiqhTableMaps"].append(
            {"_allowDeletes": {"Value": None, "HasValue": True},
             "_allowDeletesPolicyId": {"Value": None, "HasValue": True},
             "BiqhTableName": rtable, "SortOrder": sort_counter, "SqlQuery": stub_sql,
             "UseBiql": False, "BiqlQuery": None, "IsMergeStatement": True, "UseSubsetKey": None,
             "AllowDeletesPolicyId": None, "BiqhTableColumnMaps": None, "DelayTypeMaps": []}
        )
        sort_counter += 1

    # Link tables
    link_names = sorted(set([n for n in mapping["biqh_link_table_name"].unique() if n]))
    for link_name in link_names:
        if not is_valid_table(link_name):
            print(f"[INFO] Skip invalid/unknown link table: {link_name}")
            continue
        link_sql = build_link_map_sql(link_name, mapping, custom_table_name, provider_key_sources) or (
            f"-- TODO: Provide join logic to produce Id columns for [{link_name}] using @Schema.{custom_table_name}\n"
            "SELECT DISTINCT\n\t-- <table1>.Id AS [Table1Id]\n\t-- <table2>.Id AS [Table2Id]\n"
            f"FROM\n\t@Schema.{custom_table_name} r\n"
            "WHERE\n\tr.Record_ImportId = @ImportId\n\tAND r.Record_HasError = 0\n"
        )
        map_block["BiqhTableMaps"].append(
            {"_allowDeletes": {"Value": None, "HasValue": True},
             "_allowDeletesPolicyId": {"Value": None, "HasValue": True},
             "BiqhTableName": link_name, "SortOrder": sort_counter, "SqlQuery": link_sql,
             "UseBiql": False, "BiqlQuery": None, "IsMergeStatement": True, "UseSubsetKey": None,
             "AllowDeletesPolicyId": None, "BiqhTableColumnMaps": None, "DelayTypeMaps": []}
        )
        sort_counter += 1

    feed_obj["MapDefinitions"] = [map_block]

    # RowMapDefinitions
    row_block = {"Name": None, "Description": None, "UseTransaction": True, "UseSubsetKey": False,
                 "BatchSize": 0, "BiqhTableRowMaps": []}
    sort = 1
    all_candidates = (
        list(mapping["biqh_parent_table_name"].dropna().unique()) 
        + relation_tables 
        + link_names
    )
    
    seen = set()
    unique_candidates = []
    
    for t in all_candidates:
        if t and t not in seen:
            seen.add(t)
            unique_candidates.append(t)

    for t in unique_candidates:    
        if not is_valid_table(t):
            continue
        # If DB metadata says t isn't eligible for rowmaps, skip quietly
        if (dbmeta and dbmeta.rowmap_allowed_tables and (t not in dbmeta.rowmap_allowed_tables)):
            continue
        row_block["BiqhTableRowMaps"].append(
            {"BiqhTableName": t, "SortOrder": sort, "SqlQuery": "", "Description": None,
             "IsMergeStatement": False, "UseSubsetKey": None, "AllowDeletes": None,
             "BiqhTableRowMapRowLinkSets": []}
        )
        sort += 1
    feed_obj["RowMapDefinitions"] = [row_block]

    # AuthorizeDefinitions
    auth_block = {"Name": None, "Description": None, "UseTransaction": True, "UseSubsetKey": False,
                  "BatchSize": 0, "BiqhTableAuthorizeMaps": []}
    sort = 1
    auth_targets = (
        list(mapping["biqh_parent_table_name"].dropna().unique()) + relation_tables + link_names
    )
    seen = set()
    unique_auth_targets = []
    for t in auth_targets:
        if t and t not in seen:
            seen.add(t)
            unique_auth_targets.append(t)
    
    for t in unique_auth_targets:
        if not is_valid_table(t):
            continue
        auth_block["BiqhTableAuthorizeMaps"].append(
            {"BiqhTableName": t, "SortOrder": sort, "Description": None, "IsMergeStatement": True,
             "UseSubsetKey": None, "AllowDeletes": None, "AuthorizeForProvider": True,
             "BiqhTableAuthorizeMapForTables": []}
        )
        sort += 1
    feed_obj["AuthorizeDefinitions"] = [auth_block]

    return feed

# =====================
# Runner
# =====================

def merge_config(cli: dict, env: dict, defaults: dict) -> dict:
    merged = defaults.copy()
    for k, env_name in ENV_MAP.items():
        if env_name in env and str(env[env_name]).strip():
            merged[k] = env[env_name]
    for k, v in cli.items():
        if v is not None and v != "":
            merged[k] = v
    for k in ("feed_id", "provider_id", "import_frequency_id"):
        merged[k] = int(merged[k])
    return merged


def build_feed_json_paths(
    template_path: str,
    vendor_file: str,
    feed_id: str,
    feed_name: str,
    provider_id: str,
    import_frequency_id: str,
    mapping_path: Optional[str] = None,
    out_dir: str = ".",
    schema: Optional[str] = None,
    custom_table_name: str = "CustomTable",
    mssql_conn_str: Optional[str] = None,
) -> Path:
    """Convenience wrapper so you can call this directly in code without CLI.
    Returns the output file path.
    """
    template = load_template(template_path)
    mapping = load_mapping(mapping_path)

    conn = try_connect(mssql_conn_str or "")
    dbmeta = load_db_meta(conn) if conn else None

    feed_json = build_feed_json(
        template=template,
        mapping=mapping,
        vendor_file=vendor_file,
        feed_id=int(feed_id),
        feed_name=feed_name,
        provider_id=int(provider_id),
        import_frequency_id=int(import_frequency_id),
        schema=schema,
        custom_table_name=custom_table_name,
        dbmeta=dbmeta,
        conn=conn,
    )

    out_path = Path(out_dir) / f"{sanitize_feed_name(feed_name)}_{int(feed_id)}_V1.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(feed_json, indent=2, ensure_ascii=False), encoding="utf-8")
    return out_path

if __name__ == "__main__":
    out = build_feed_json_paths(
        template_path=DEFAULTS["template"],
        vendor_file=DEFAULTS["vendor_file"],
        feed_id=DEFAULTS["feed_id"],
        feed_name=DEFAULTS["feed_name"],
        provider_id=DEFAULTS["provider_id"],
        import_frequency_id=DEFAULTS["import_frequency_id"],
        mapping_path=DEFAULTS["mapping"] or None,
        out_dir=DEFAULTS["out_dir"],
        schema=DEFAULTS.get("schema"),
        mssql_conn_str=DEFAULTS.get("mssql_conn_str"),
    )
    print("Built feed:", out)

