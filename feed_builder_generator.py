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
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from pandas.api.types import is_datetime64_any_dtype, is_integer_dtype, is_float_dtype

import pandas as pd
from datetime import datetime

# =====================
# Defaults & ENV
# =====================
DEFAULTS = {
    "template": "file_structure_V24.json",
    "mapping": "cds_corps_pricing_mapping.csv",  # optional
    "vendor_file": "Corps_Pricing_N1600-2025-09-09.csv",
    "feed_id": 401,
    "feed_name": "test",
    "provider_id": 284,
    "import_frequency_id": 1,
    "schema": "dekafdwh",
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
    "nvarchar": 1, "int": 2,"integer": 2, "decimal": 3, "date": 4, "datetime": 5,
    "time": 6, "boolean": 7, "bit": 7, "bigint": 8, "biginteger": 8, "binary": 9, "varbinary": 9, "any": 10,
    # numeric aliases
    "numeric": 3, "number": 3, "money": 3, "smallmoney": 3, "float": 3, "double": 3, "real": 3,
    # integer aliases
    "smallint": 2, "tinyint": 2,
}


_decimal_like = {"decimal","numeric","number","money","smallmoney","float","double","real"}

import json
import xml.etree.ElementTree as ET

def collect_json_samples(vendor_file: str, json_path: list[str], max_samples: int = 100):
    """Collect sample values from JSON vendor file for a given path."""
    samples = []
    with open(vendor_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    def walk(obj, path_idx=0):
        nonlocal samples
        if len(samples) >= max_samples:
            return
        if path_idx >= len(json_path):
            if obj is not None:
                samples.append(str(obj))
            return
        key = json_path[path_idx]
        if isinstance(obj, dict) and key in obj:
            walk(obj[key], path_idx + 1)
        elif isinstance(obj, list):
            for item in obj:
                walk(item, path_idx)
                if len(samples) >= max_samples:
                    break

    walk(data, 0)
    return samples


def collect_xml_samples(vendor_file: str, xpath: str, max_samples: int = 100):
    """Collect sample values from XML vendor file for a given xpath."""
    samples = []
    tree = ET.parse(vendor_file)
    root = tree.getroot()
    for elem in root.findall(xpath):
        if elem.text is not None:
            samples.append(elem.text.strip())
        if len(samples) >= max_samples:
            break
    return samples



def _val(v_id, param=None):
    """Helper to build a validation dictionary."""
    return {
        "ValidationTypeId": v_id,
        "Parameter": param,
        "Message": None,
        "Condition": None,
        "IsError": True,
        "IsTechnical": True
    }

def build_validations(coldef, samples=None):
    """
    Build a list of validation dicts for a given column definition.
    samples: optional list of sample values from vendor file.
    """
    validations = []

    name = (coldef.get("Name") or "").lower()
    header = (coldef.get("HeaderName") or "").lower()
    dtid = coldef.get("DataTypeId")
    length = coldef.get("Length")
    precision = coldef.get("Precision")
    scale = coldef.get("Scale")

    MIN_SAMPLES = 50        # need at least this many samples
    TOLERANCE = 0.02        # allow up to 2% empties
    
    if samples and len(samples) >= MIN_SAMPLES:
        empties = sum(1 for v in samples if str(v).strip().lower() in ("", "none", "nan", "null"))
        ratio = empties / len(samples)

    # mark as NotEmpty if empties are very rare
    #if coldef["DataTypeId"] != 3: # exception for decimal not to put NotEmppty; TO DO if needed
        if ratio <= TOLERANCE and coldef["DataTypeId"] != 3:
            validations.append(_val(1))
       

# # Fallback rule: force NotEmpty for key identifiers
#     if coldef["Name"] in ("ISIN", "SecurityId", "ProviderKey"):
#         if not any(v for v in validations if v["ValidationTypeId"] == 1):
#             validations.append(_val(1))




    # 2. NotNull
    if coldef.get("IsRequired") is True:
        validations.append(_val(2))

    # 3. IsIsin
    pattern = re.compile(r"(^|[_-])isin($|[_-]|code)", flags=re.IGNORECASE)
    if pattern.search(name) or pattern.search(header):
        validations.append(_val(3))

    # 4. IsCurrency (3-letter code)
    if ("currency" in name or "currency" in header) and length == 3:
        validations.append(_val(4))

    # 9/10/11. String length validations
    pattern = re.compile(r"(^|[_-])isin($|[_-]|code)", flags=re.IGNORECASE)
    if (
        dtid == 1 and length and length > 0
        and not ("currency" in name or "currency" in header)
        and not (pattern.search(name) or pattern.search(header))
):
        validations.append(_val(10, str(length)))  # MaximumLength


    # 23. IsValidDecimal
    if dtid == 3 and precision and scale is not None:
        validations.append(_val(23, f"{precision},{scale}"))

    # 26. IsCfi
    if "cfi" in name or "cfi" in header:
        validations.append(_val(26))

    # 17. EmailAddress
    if "email" in name or "mail" in header:
        validations.append(_val(17))

    # 18. CreditCard
    if "credit" in name and "card" in name:
        validations.append(_val(18))

    # 25. AnyString (explicit allowed values)
    if coldef.get("AllowedValues"):
        param = ",".join(coldef["AllowedValues"])
        validations.append(_val(25, param))

    # 27/28. NotStale / MaxDeviationPct
    # These require BIQH context and extra parameters.
    if coldef.get("NotStaleParams"):
        validations.append(_val(27, coldef["NotStaleParams"]))
    if coldef.get("MaxDeviationPct"):
        validations.append(_val(28, coldef["MaxDeviationPct"]))

    return validations


def parse_biqh_type(type_str: str):
    s = (type_str or "").strip().lower()
    # nvarchar(length)
    m = re.match(r"nvarchar\((max|\d+)\)", s)
    if m:
        val = m.group(1)
        length = -1 if val == "max" else int(val)
        return DATA_TYPE_MAP["nvarchar"], length, None
    # decimal/numeric(p,s)
    m = re.match(r"(decimal|numeric|number)\s*\(\s*(\d+)\s*[,\.]\s*(\d+)\s*\)", s)
    if m:
        p, sc = int(m.group(2)), int(m.group(3))
        return 3, None, (p, sc)
    # plain decimal-like tokens
    if s in _decimal_like:
        return 3, None, None
    if s in DATA_TYPE_MAP:
        return DATA_TYPE_MAP[s], None, None
    # fallback nvarchar(max)
    return DATA_TYPE_MAP["nvarchar"], -1, None

# -------- Inference helpers (used only when mapping type is absent) --------
_date_rx = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_datetime_rx = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$")
_time_rx = re.compile(r"^\d{2}:\d{2}(:\d{2})?$")
_int_rx = re.compile(r"^[+-]?\d+$")
_dec_rx = re.compile(r"^[+-]?\d+([.,]\d+)?$")

def infer_dtype_from_samples(samples: list[str]):
    # strip empties
    S = [s for s in (samples or []) if s not in (None, "", "null", "NULL")]
    if not S:
        return DATA_TYPE_MAP["nvarchar"], -1, None  # default nvarchar(max)
    # normalize decimals (comma → dot) for detection
    n = [s.replace(",", ".") for s in S]

    # date/time first
    if all(_date_rx.match(s) for s in n):
        return DATA_TYPE_MAP["date"], None, None
    if all(_datetime_rx.match(s) for s in n):
        return DATA_TYPE_MAP["datetime"], None, None
    if all(_time_rx.match(s) for s in n):
        return DATA_TYPE_MAP["time"], None, None

    # numbers
    if all(_int_rx.match(s) for s in n):
        # pick bigint if any value exceeds 32-bit range
        try:
            if any(abs(int(s)) > 2_147_483_647 for s in n):
                return DATA_TYPE_MAP["bigint"], None, None
        except Exception:
            pass
        return DATA_TYPE_MAP["int"], None, None
    if all(_dec_rx.match(s) for s in n):
        # decimal without explicit precision/scale
        return DATA_TYPE_MAP["decimal"], None, None

    # boolean-like?
    if all(s.lower() in ("true","false","0","1","y","n","yes","no") for s in n):
        return DATA_TYPE_MAP["boolean"], None, None

    # fallback string
    return DATA_TYPE_MAP["nvarchar"], -1, None


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
    #df = pd.read_csv(path, dtype=str).fillna("")

    df = pd.read_csv(path, dtype=str, sep=None, engine="python").fillna("")
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]
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
    m = re.match(r"decimal\((\d+)\s*[,\.]\s*(\d+)\)", s)
    #m = re.match(r"(decimal|numeric|number)\s*\(\s*(\d+)\s*[,\.]\s*(\d+)\s*\)", s)
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

def detect_decimal_separator(vendor_file, nrows=100):
    # read just a sample with auto delimiter detection
    #df = pd.read_csv(vendor_file, sep=None, engine="python", nrows=nrows, dtype=str)
    ext = Path(vendor_file).suffix.lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(vendor_file, nrows=nrows, dtype=str)
    elif ext in [".csv", ".txt"]:
        try:
            df = pd.read_csv(vendor_file, sep=None, engine="python", nrows=nrows, dtype=str, encoding="utf-8-sig")
        except UnicodeDecodeError:
            df = pd.read_csv(vendor_file, sep=None, engine="python", nrows=nrows, dtype=str, encoding="latin1")


    dot_count = 0
    comma_count = 0

    # regex for numbers with either , or .
    num_pattern = re.compile(r"^\s*[-+]?\d{1,3}([.,]\d+)?\s*$")

    for col in df.columns:
        for val in df[col].dropna().astype(str):
            if num_pattern.match(val):
                if "." in val:
                    dot_count += 1
                elif "," in val:
                    comma_count += 1

    if dot_count > comma_count:
        return "."
    elif comma_count > dot_count:
        return ","
    else:
        return None  # unknown / no decimals found
    
##date/time/datetime FORMAT detection - START BLOCK
def read_vendor_df(vendor_file: str, nrows: int = 200, xml_xpath: str | None = None) -> pd.DataFrame:
    """
    Returns a small sample DataFrame from CSV/TXT/XLS/XLSX/JSON/XML.
    For JSON: flattens objects/arrays with json_normalize.
    For XML: tries pandas.read_xml (optionally with xpath).
    """
    ext = Path(vendor_file).suffix.lower()

    # Excel - allow pandas to infer dtypes so datetime/numeric detection can work
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(vendor_file, nrows=nrows)

    # CSV/TXT with delimiter/encoding auto-detection
    if ext in (".csv", ".txt"):
        try:
            # let pandas infer types (don't force dtype=str) so date/number detection can work
            return pd.read_csv(vendor_file, sep=None, engine="python", nrows=nrows, encoding="utf-8-sig")
        except UnicodeDecodeError:
            return pd.read_csv(vendor_file, sep=None, engine="python", nrows=nrows, encoding="latin1")

    # JSON: flatten
    if ext == ".json":
        with open(vendor_file, "r", encoding="utf-8-sig") as f:
            obj = json.load(f)

        # find a list of records
        def _find_records(o):
            if isinstance(o, list):
                return o
            if isinstance(o, dict):
                # prefer first list value
                for _, v in o.items():
                    if isinstance(v, list):
                        return v
                return [o]
            return [o]

        records = _find_records(obj)
        df = pd.json_normalize(records)
        return df.head(nrows).astype(str)

    # XML
    if ext == ".xml":
        # pandas.read_xml will try to infer structure; use xpath if you know the row node
        try:
            df = pd.read_xml(vendor_file, xpath=xml_xpath) if xml_xpath else pd.read_xml(vendor_file)
            return df.head(nrows).astype(str)
        except Exception:
            # If XML is too custom, skip detection instead of failing
            return pd.DataFrame()

    raise ValueError(f"Unsupported file extension: {ext}")

def detect_datetime_format_series(series: pd.Series, force_kind: str | None = None) -> str | None:
    """
    Detects a canonical format string for a column sample.
    Handles:
      - native datetime dtype
      - Excel serials (integers/floats ~ Excel date origin)
      - dd/MM vs MM/dd disambiguation by majority vote (defaults to MM/dd if ambiguous)
      - milliseconds (HH:mm:ss.fff / .ffffff)
      - ISO 'T' and optional 'Z'
    Returns strings like: yyyy-MM-dd, dd.MM.yyyy, HHmmss, yyyy-MM-dd HH:mm:ss, yyyyMMddHHmmss, etc.
    """
    s = series.dropna()
    if s.empty:
        return None

    # 1) Native datetime dtype → pick date vs datetime by time component presence
    if is_datetime64_any_dtype(s):
        has_time = (s.dt.time != datetime.min.time()).any()
        return "yyyy-MM-dd HH:mm:ss" if has_time else "yyyy-MM-dd"

    # 2) Excel serials (numbers around Excel epoch)
    def _looks_like_excel_serial(vals) -> bool:
        try:
            vals = [float(x) for x in vals]
        except Exception:
            return False
        ok = [20000 <= v <= 60000 for v in vals]  # rough bounds 1954..2064
        return sum(ok) >= max(3, len(vals)//2)

    def _has_fraction(vals) -> bool:
        try:
            return any(abs(float(x) - int(float(x))) > 1e-9 for x in vals)
        except Exception:
            return False

    if is_integer_dtype(s) or is_float_dtype(s):
        vals = s.head(20).tolist()
        if _looks_like_excel_serial(vals):
            return "yyyy-MM-dd HH:mm:ss" if _has_fraction(vals) else "yyyy-MM-dd"

    # 3) Strings → patterns
    samples = [str(x).strip() for x in s.astype(str).unique().tolist()[:30] if str(x).strip()]

    # ISO first
    iso_dt = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d{3,6})?)?(Z)?$")
    iso_d  = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    iso_t  = re.compile(r"^\d{2}:\d{2}(:\d{2}(\.\d{3,6})?)?$")
    for v in samples:
        if iso_dt.match(v):
            msec = re.search(r"\.(\d{3,6})", v)
            if msec:
                return "yyyy-MM-dd HH:mm:ss.fff" if len(msec.group(1)) == 3 else "yyyy-MM-dd HH:mm:ss.FFFFFF"
            return "yyyy-MM-dd HH:mm:ss" if ":ss" in v or v.count(":") == 2 else "yyyy-MM-dd HH:mm"
        if iso_d.match(v):
            return "yyyy-MM-dd"
        if iso_t.match(v):
            if re.search(r"\.\d{3,6}$", v):
                return "HH:mm:ss.fff" if len(v.split(".")[-1]) == 3 else "HH:mm:ss.FFFFFF"
            return "HH:mm:ss" if v.count(":") == 2 else "HH:mm"

    # Compact digit formats
    if all(re.fullmatch(r"\d{8}", v) for v in samples):
        def _resolve_8(s8: str) -> str | None:
            y, m, d = s8[:4], s8[4:6], s8[6:]
            if 1 <= int(m) <= 12 and 1 <= int(d) <= 31:
                return "yyyyMMdd"
            d2, m2 = s8[:2], s8[2:4]
            if 1 <= int(m2) <= 12 and 1 <= int(d2) <= 31:
                return "ddMMyyyy"
            m3, d3 = s8[:2], s8[2:4]
            if 1 <= int(m3) <= 12 and 1 <= int(d3) <= 31:
                return "MMddyyyy"
            return None
        got = _resolve_8(samples[0])
        if got:
            return got

    if all(re.fullmatch(r"\d{12}", v) for v in samples):
        return "yyyyMMddHHmm"
    if all(re.fullmatch(r"\d{14}", v) for v in samples):
        return "yyyyMMddHHmmss"
    if all(re.fullmatch(r"\d{4}", v) for v in samples):
        return "HHmm"
    if all(re.fullmatch(r"\d{6}", v) for v in samples):
        return "HHmmss"

    # --- FIXED DISAMBIGUATION ---
    def _vote_day_first(vals, sep):
        dd_first = md_first = 0
        for v in vals:
            parts = v.split()[0].split(sep)
            if len(parts) != 3:
                continue
            a, b = int(parts[0]), int(parts[1])
            if a > 12: dd_first += 1
            if b > 12: md_first += 1
        if dd_first > md_first:
            return "dd"
        if md_first > dd_first:
            return "MM"
        return None  # ambiguous

    # Disambiguate dd/MM/yyyy vs MM/dd/yyyy (also -, .)
    for sep in [r"/", r"-", r"\."]:
        subset = [v for v in samples if re.match(fr"^\d{{2}}{sep}\d{{2}}{sep}\d{{4}}", v)]
        if not subset:
            continue
        vote = _vote_day_first(subset, sep)

        # 👇 default to US-style if ambiguous
        if vote is None:
            vote = "MM"

        # Normalize separator text (remove regex escaping)
        sep_chr = sep.replace("\\", "")

        has_ss   = any(re.search(r"[ T]\d{2}:\d{2}:\d{2}(?:\.\d{3,6})?$", v) for v in subset)
        has_ms3  = any(re.search(r"\.\d{3}$", v) for v in subset)
        has_ms6  = any(re.search(r"\.\d{6}$", v) for v in subset)
        time_fmt = None
        if any(re.search(r"[ T]\d{2}:\d{2}(:\d{2}(\.\d{3,6})?)?$", v) for v in subset):
            if has_ms6: time_fmt = " HH:mm:ss.FFFFFF"
            elif has_ms3: time_fmt = " HH:mm:ss.fff"
            elif has_ss:  time_fmt = " HH:mm:ss"
            else:         time_fmt = " HH:mm"

        if vote == "dd":
            date_fmt = f"dd{sep_chr}MM{sep_chr}yyyy"
        else:
            date_fmt = f"MM{sep_chr}dd{sep_chr}yyyy"

        return date_fmt + (time_fmt or "")





    return None


def find_series_for_column(df: pd.DataFrame, col_key: str, jsonpath: str | None = None) -> pd.Series | None:
    """
    Try to locate the DataFrame column for a mapping key.
    1) exact match
    2) case-insensitive match
    3) last-token match from JsonPath/XPath or key (handles flattened JSON/XML)
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None

    # 1) exact
    if col_key in df.columns:
        return df[col_key]

    # 2) case-insensitive exact
    lower_map = {c.lower(): c for c in df.columns}
    if col_key.lower() in lower_map:
        return df[lower_map[col_key.lower()]]

    # 3) last-token match (jsonpath/xpath or key)
    raw = jsonpath or col_key
    tokens = [t for t in re.split(r"[.\[\]/]+", str(raw)) if t]
    if tokens:
        last = tokens[-1].lower()
        for c in df.columns:
            if c.lower().split(".")[-1] == last:
                return df[c]

    return None
def detect_datetime_formats(
    vendor_file: str,
    column_meta: dict,
    nrows: int = 200,
    json_paths: dict | None = None,
    xml_xpath: str | None = None
) -> dict:
    """
    Detect formats for date(4)/datetime(5)/time(6) columns in any vendor file.
    column_meta: {column_name -> DataTypeId or 'date'/'datetime'/'time'}
    json_paths:  optional {column_name -> JsonPath} to help match flattened JSON
    xml_xpath:   optional row-level XPath to guide XML read
    Returns: {column_name: format_string}
    """
    df = read_vendor_df(vendor_file, nrows=nrows, xml_xpath=xml_xpath)
    if df is None or df.empty:
        return {}

    formats: dict[str, str] = {}
    for key, dtype in column_meta.items():
        # normalize dtype → kind
        kind = None
        if dtype in (4, "date"): kind = "date"
        elif dtype in (5, "datetime"): kind = "datetime"
        elif dtype in (6, "time"): kind = "time"
        else: 
            continue  # skip non-date types

        s = find_series_for_column(df, key, (json_paths or {}).get(key))
        if s is None:
            continue

        fmt = detect_datetime_format_series(s, force_kind=kind)
        if fmt:
            formats[key] = fmt

    return formats

##date/time/datetime FORMAT detection - END BLOCK

def build_filename_regex(vendor_file: str) -> str:
    """
    Build a regex from a concrete filename that:
      - keeps extension fixed
      - replaces any date/time token with a capturing group
        Supported tokens (preserving exact separators from the name):
          YYYY-MM-DD, DD-MM-YYYY, YYYY_MM_DD, DD_MM_YYYY, YYYY.MM.DD, DD.MM.YYYY
          HH:MM, HH:MM:SS, HH-MM, HH-MM-SS, HH.MM, HH.MM.SS
          contiguous: yyyyMMddHHmmss, yyyyMMddHHmm, yyyyMMdd, HHmmss
      - supports zero, one, or multiple tokens
      - leaves other characters literal (escaped)
    """
    fn = Path(vendor_file).name
    base, ext = os.path.splitext(fn)

    # token patterns (return a function that builds the capture with observed separators)
    def _ymd_builder(m: re.Match) -> str:
        sep = re.escape(m.group(1))  # not 2
        return rf"(\d{{4}}{sep}\d{{2}}{sep}\d{{2}})"

    def _dmy_builder(m: re.Match) -> str:
        sep = re.escape(m.group(1))  # not 2
        return rf"(\d{{2}}{sep}\d{{2}}{sep}\d{{4}})"

    def _time_builder(m: re.Match) -> str:
        sep = re.escape(m.group(1))  # separator
    # detect HH<sep>MM or HH<sep>MM<sep>SS
        return rf"(\d{{2}}{sep}\d{{2}}{sep}\d{{2}})" if m.group(2) else rf"(\d{{2}}{sep}\d{{2}})"

    token_specs = [
        # separated date: YYYY-sep-MM-sep-DD  (sep repeated)
        (re.compile(r"\d{4}([\-_.])\d{2}\1\d{2}"), _ymd_builder),
        # separated date: DD-sep-MM-sep-YYYY
        (re.compile(r"\d{2}([\-_.])\d{2}\1\d{4}"), _dmy_builder),
        # separated time: HH:MM[:SS]
        (re.compile(r"\d{2}([:\-_.])\d{2}(\1\d{2})?"), _time_builder),
        # contiguous datetime/date/time tokens (longest first)
        (re.compile(r"\d{14}"), lambda m: r"(\d{14})"),  # yyyyMMddHHmmss
        (re.compile(r"\d{12}"), lambda m: r"(\d{12})"),  # yyyyMMddHHmm
        (re.compile(r"\d{8}"),  lambda m: r"(\d{8})"),   # yyyyMMdd
        (re.compile(r"\d{6}"),  lambda m: r"(\d{6})"),   # HHmmss
    ]

    # collect all matches (as (start, end, replacement)) without overlapping
    intervals = []
    taken = [False] * (len(base) + 1)

    for rx, builder in token_specs:
        for m in rx.finditer(base):
            s, e = m.start(), m.end()
            if any(taken[s:e]):  # skip overlaps (earlier/longer patterns win)
                continue
            intervals.append((s, e, builder(m)))
            for i in range(s, e):
                taken[i] = True

    intervals.sort(key=lambda x: x[0])

    # stitch literal pieces + token captures
    parts = []
    last = 0
    for s, e, repl in intervals:
        parts.append(re.escape(base[last:s]))  # literal before token
        parts.append(repl)                     # capture for token
        last = e
    parts.append(re.escape(base[last:]))       # tail literal

    pattern = "".join(parts) if parts else re.escape(base)
    return rf"^{pattern}{re.escape(ext)}$"



    # If we find any 8-digit cluster in the name, generalize to a capture group
    # Otherwise allow an *optional* leading date token with optional separator
    # if re.search(r"\d{8}", base):
    #     esc = re.sub(r"\\d\{8\}", r"(\\d{8})", esc)  # if it's already escaped \d{8}
    #     esc = re.sub(r"(?:\\d){8}", r"(\\d{8})", esc)  # plain digits in the base
    # else:
    #     esc = r"(?:\d{8}[_-]?)?" + esc

    # # Let separators be flexible
    # esc = esc.replace(r"\_", r"[_-]?").replace(r"\-", r"[-_]?")

    # return rf"^{esc}{re.escape(ext)}$"

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
    
    # Build column_meta from mapping (only date/time/datetime types matter)
    column_meta = {}
    if mapping is not None and not mapping.empty:
        for _, r in mapping.iterrows():
            if (r.get("biqh_column_data_type") or "").strip():
                dtid, _, _ = parse_biqh_type(r["biqh_column_data_type"])
                if dtid in (4,5,6):  # date, datetime, time
                    key = r.get("customer_field") or r.get("biqh_import_field")
                    if key:
                        column_meta[key] = dtid
    
    fmt_by_key = detect_datetime_formats(
        vendor_file=xml_path,
        column_meta=column_meta,
        nrows=500,
        xml_xpath=root_path,   # or row-level node if you know it
    )
    def _coldefs_for_node(node_abs: str) -> List[Dict]:

        rows = []
        for leaf in parent_to_leaves.get(node_abs, []):
            rel = _rel_xpath(node_abs, leaf)
            name = _col_name_from_leaf(leaf)
            dtid, length, dec = (10, None, None)
            precision = scale = None
            r = map_by_path.get(leaf)
            header_name = None
            description = None
            dtid, length, dec = (DATA_TYPE_MAP["nvarchar"], -1, None)
            precision = scale = None

            if r is not None:
                header_name = r.get("customer_field") or r.get("biqh_import_field") or None
                name       = r.get("biqh_import_field") or r.get("customer_field") or name

                if (r.get("biqh_column_data_type") or "").strip():
                    _dtid, length, dec = parse_biqh_type(r.get("biqh_column_data_type"))
                    dtid = _dtid
                else:
                    # (optional) infer if mapping doesn't specify a type:
                    # samples = collect_xml_samples_for_xpath(xml_root, leaf, max_samples=100)
                    # dtid, length, dec = infer_dtype_from_samples(samples)
                    pass

                description = (r.get("description") or None) if (r.get("description") or "").strip() else None
            else:
                # optional inference when there's no mapping:
                # samples = collect_xml_samples_for_xpath(xml_root, leaf, max_samples=100)
                # dtid, length, dec = infer_dtype_from_samples(samples)
                pass

            if dec:
                precision, scale = dec
            if dtid in (4,5,6):  # date/time/datetime
                fmt = None
                if header_name:
                    fmt = fmt_by_key.get(header_name)
                if not fmt:
                    fmt = fmt_by_key.get(name)
                # fallback defaults
                if not fmt:
                    if dtid == 4:
                        fmt = "yyyy-MM-dd"
                    elif dtid == 5:
                        fmt = "yyyy-MM-dd HH:mm:ss"
                    else:
                        fmt = "HH:mm:ss"
            coldef = {
            "HeaderName": header_name,  # customer_field
            "SampleData": None,
            "Name": name,               # biqh_import_field
            "DataTypeId": dtid,
            "Length": length if dtid in (1,10) else None,
            "Precision": precision if dtid == 3 else None,
            "Format": fmt if dtid in (4,5,6) else None,
            "Description": description,
            "Scale": scale if dtid == 3 else None,
            "AllowLeadingWhite": False, "AllowTrailingWhite": False,
            "AllowLeadingSign": False, "AllowTrailingSign": False,
            "AllowParentheses": False, "AllowDecimalPoint": dtid == 3,
            "AllowThousands": False, "AllowExponent": False,
            "AllowCurrencySymbol": False, "AllowPercentage": False,
            "CultureInfo": None,
            "ColumnTypeId": 5, "Start": None, "End": None,
            "ColumnNumber": 0, "Script": None,
            "UseInnerXml": False,
            "XPath": rel,
            "SourceName": header_name,
            "JsonPath": None,
            "NullAliases": None, "Multiplier": None,
            "TrueAliases": None, "FalseAliases": None,
            "RetrievalStatisticsEnabled": False,
            "Validations": []
        }
        

        # 🔧 Collect samples for this XPath
            samples = collect_xml_samples(xml_path, rel, max_samples=100)

        # 🔧 Build validations using type info + samples
            coldef["Validations"] = build_validations(coldef, samples=samples)
            rows.append(coldef)

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
    root = {"_root": data} if isinstance(data, list) and data and isinstance(data[0], dict) else data

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
    column_meta = {}
    if mapping is not None and not mapping.empty:
        for _, r in mapping.iterrows():
            if (r.get("biqh_column_data_type") or "").strip():
                dtid, _, _ = parse_biqh_type(r["biqh_column_data_type"])
                if dtid in (4,5,6):  # date, datetime, time
                    key = r.get("customer_field") or r.get("biqh_import_field")
                    if key:
                        column_meta[key] = dtid

    # Detect formats using your existing helper
    fmt_by_key = detect_datetime_formats(
        vendor_file=json_path,
        column_meta=column_meta,
        nrows=200,
        json_paths={r.get("biqh_import_field") or r.get("customer_field"): r.get("customer_field")
                    for _, r in mapping.iterrows() if (r.get("customer_field") or "").startswith("$.")}
    )


    def _coldefs_for_table(tnode: str) -> List[Dict]:
        out = []
        for leaf in sorted(set(table_to_leafs.get(tnode, []))):
            rel = _json_rel(tnode, leaf)
            last = [t for t in re.split(r"\.|\[\*\]", leaf) if t and t != "$"][-1]
            name = re.sub(r"[^A-Za-z0-9_]+", "_", last).strip("_") or "field"

            dtid, length, dec = (DATA_TYPE_MAP["nvarchar"], -1, None)
            precision = scale = None

            r = map_by_path.get(leaf)
            header_name = None
            description = None

            if r is not None:
                header_name = r.get("customer_field") or r.get("biqh_import_field") or None
                name = r.get("biqh_import_field") or r.get("customer_field") or name
                if (r.get("biqh_column_data_type") or "").strip():
                    _dtid, length, dec = parse_biqh_type(r.get("biqh_column_data_type"))
                    dtid = _dtid
                description = (r.get("description") or None) if (r.get("description") or "").strip() else None

            if dec:
                precision, scale = dec
            if dtid in (4,5,6):  # date/time/datetime
                fmt = None
                if header_name:
                    fmt = fmt_by_key.get(header_name)
                if not fmt:
                    fmt = fmt_by_key.get(name)
                if not fmt:
                    fmt = fmt_by_key.get(rel)  # sometimes rel JSONPath matches
                # fallback defaults when detection didn't find a format
                if not fmt:
                    if dtid == 4:
                        fmt = "yyyy-MM-dd"
                    elif dtid == 5:
                        fmt = "yyyy-MM-dd HH:mm:ss"
                    else:
                        fmt = "HH:mm:ss"
                

                coldef = {
                "HeaderName": header_name,
                "SampleData": None,
                "Name": name,
                "DataTypeId": dtid,
                "Length": length if dtid in (1, 10) else None,
                "Precision": precision if dtid == 3 else None,
                "Format": fmt if dtid in (4,5,6) else None,
                "Description": description,
                "Scale": scale if dtid == 3 else None,
                "AllowLeadingWhite": False, "AllowTrailingWhite": False,
                "AllowLeadingSign": False, "AllowTrailingSign": False,
                "AllowParentheses": False, "AllowDecimalPoint": dtid == 3,
                "AllowThousands": False, "AllowExponent": False,
                "AllowCurrencySymbol": False, "AllowPercentage": False,
                "CultureInfo": None,
                "ColumnTypeId": 6,
                "Start": None, "End": None, "ColumnNumber": 0, "Script": None,
                "UseInnerXml": False,
                "XPath": None,
                "SourceName": header_name,
                "JsonPath": rel,
                "NullAliases": None, "Multiplier": None,
                "TrueAliases": None, "FalseAliases": None,
                "RetrievalStatisticsEnabled": False,
                "Validations": []
            }
            
        

            path_tokens = [tok for tok in re.split(r"\.|\[\*\]", leaf) if tok and tok != "$"]
            samples = collect_json_samples(json_path, path_tokens, max_samples=100)
            coldef["Validations"] = build_validations(coldef, samples=samples)

            out.append(coldef)

        return out  # ✅ return after loop, not inside

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
            "TableName": _tname(tpath),
            "NodeXPath": None,
            "NodeJsonPath": node_json,
            "SqlQuery": None,
            "ChildTableDefinitions": [],
            "ColumnDefinitions": _coldefs_for_table(tpath),
            "IndexDefinitions": []
        }
        for ch in [c for c, p in parents.items() if p == tpath]:
            tbl["ChildTableDefinitions"].append(_make_table(ch))
        return tbl

    return [_make_table(r) for r in roots]


# =====================
# Mapping -> ColumnDefs (flat fallback)
# =====================

def build_column_definitions_from_mapping(mapping: pd.DataFrame, filetype_id: int,df: pd.DataFrame | None = None,  vendor_file: str | None = None ) -> List[Dict]:
    coldefs = []
    datetime_formats = {}
    if vendor_file:
        column_meta: dict[str, int] = {}
        for _, r in mapping.iterrows():
            dtid, _, _ = parse_biqh_type(r["biqh_column_data_type"])
            if r.get("customer_field"):
                column_meta[r["customer_field"].strip()] = dtid
            if r.get("biqh_import_field"):
                column_meta[r["biqh_import_field"].strip()] = dtid

        datetime_formats = detect_datetime_formats(
            vendor_file,
            column_meta=column_meta,
            nrows=500
        )
    for idx, r in mapping.iterrows():
        # src_header = r["customer_field"] or r["biqh_import_field"]
        # target_name = r["biqh_parent_column_name"] or r["biqh_import_field"]
        # HeaderName = customer_field ; Name = biqh_import_field
        src_header = r["customer_field"] or r["biqh_import_field"]     # HeaderName
        target_name = r["biqh_import_field"] or r["customer_field"]    # Name

       # print("DEBUG biqh_column_data_type raw:", repr(r["biqh_column_data_type"]))

        dtid, length, dec = parse_biqh_type(r["biqh_column_data_type"])
        precision = scale = None
        if dec:
            precision, scale = dec
        fmt = None
        if dtid in (4, 5, 6):
            fmt = (
                datetime_formats.get(src_header)
                or datetime_formats.get(target_name)
            )
            # fallback defaults
            if not fmt:
                if dtid == 4:
                    fmt = "yyyy-MM-dd"
                elif dtid == 5:
                    fmt = "yyyy-MM-dd HH:mm:ss"
                else:
                    fmt = "HH:mm:ss"
        coldef = {
            "HeaderName": src_header or None, "SampleData": None,
            "Name": target_name or None, "DataTypeId": dtid,
            "Length": length if dtid in (1,10) else None,
            "Precision": precision if dtid == 3 else None, "Format": fmt,
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
        # if dtid == 1 and (length is not None) and length > 0:
        #     coldef["Validations"].append({"ValidationTypeId": 10, "Parameter": str(length),
        #                                   "Message": None, "Condition": None, "IsError": True, "IsTechnical": True})
        samples = None
        if src_header and src_header in df.columns:
            #samples = df[src_header].dropna().astype(str).head(100).tolist()
            samples = df[src_header].astype(str).head(100).tolist()
        coldef["Validations"] = build_validations(coldef, samples=samples)
        #coldef["Validations"] = build_validations(coldef)
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
            source = r.iloc[0]["biqh_import_field"] or r.iloc[0]["customer_field"]
            return source, "ProviderKey"
    string_rows = rows[rows["biqh_column_data_type"].str.lower().str.startswith("nvarchar")]
    if not string_rows.empty:
        source = string_rows.iloc[0]["biqh_import_field"] or string_rows.iloc[0]["customer_field"]
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
        src = r["biqh_import_field"] or r["customer_field"]
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
    df: Optional[pd.DataFrame] = None,
    ftype_id: Optional[int] = None
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
    decimal_separator = detect_decimal_separator(vendor_file)
    file_def = {
        "FileTypeId": ftype_id, "Name": None, "FileNameRegex": file_name_regex,
        "FileNameRegexDescription": None, "CsvDelimiterCharacter": None if ftype_id != 1 else ",",
        "HasHeader": None, "SubsetGroupNumber": None, "DynamicColumnCount": True,
        "DefaultColumnNamePrefix": None, "TrimWhiteSpaces": True, "AdvancedEscapingEnabled": True,
        "QuoteCharacter": None, "DoubleQuoteEscapingEnabled": True, "ColumnHeaderTypeSeparator": None,
        "ReadHeaders": True, "CheckHeaders": False, "CheckUnexpectedHeaders": False, "UseEmbargoDateTime": False,
        "EmbargoDateTimeComposite": None, "IgnoreColumnsWithEmptyHeader": True, "SkipEmptyLines": True,
        "SkipFirstNumberOfLines": 0, "EndOfFileRegex": None, "CheckZippedFileNameByRegex": False,
        "DefaultMicrosoftStandardTimeZoneId": None, "NumberDecimalSeparator": decimal_separator if ftype_id in (1,4) else None,
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
        cols = build_column_definitions_from_mapping(mapping, ftype_id, df=df,vendor_file=vendor_file)
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

    rowauth_tables = set()
    if conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT Name 
                FROM dbo.[Table] 
                WHERE Name LIKE '%RowAuth%'
                """)
            rowauth_tables = {r[0] for r in cur.fetchall()}        
    
    for t in unique_auth_targets:
        if not is_valid_table(t):
            continue
        if (t + "RowAuth") not in rowauth_tables:
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
    import os
    import pandas as pd

    # Load template and mapping
    template = load_template(template_path)
    mapping = load_mapping(mapping_path)

    # Detect file type
    ext = os.path.splitext(vendor_file)[1].lower()
    df = None
    filetype_id = None

    if ext in [".csv", ".txt"]:
        filetype_id = 1
        try:
            df = pd.read_csv(vendor_file, sep=None, engine="python", encoding="utf-8-sig")
        except UnicodeDecodeError:
            df = pd.read_csv(vendor_file, sep=None, engine="python", encoding="latin1")
        #df = pd.read_csv(vendor_file, sep=None, engine="python")  # auto-detect delimiter
    elif ext in [".xls", ".xlsx"]:
        filetype_id = 4
        df = pd.read_excel(vendor_file, sheet_name=0)  # first sheet
    elif ext in [".json"]:
        filetype_id = 3
    elif ext in [".xml"]:
        filetype_id = 2
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

    # Optional DB metadata
    conn = try_connect(mssql_conn_str or "")
    dbmeta = load_db_meta(conn) if conn else None

    # Build feed JSON (note the new df/filetype_id args)
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
        df=df,                  # pass DataFrame when available
        ftype_id=filetype_id # tell builder what type of file this is
    )

    # Save output file
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

