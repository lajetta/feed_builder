#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# =====================
# Editable built-in defaults
# (Change these once; then you can just run `python json_feed_builder.py`)
# =====================
DEFAULTS = {
    "template": "file_structure_V24.json",
    "mapping": "mtge_ref_mapping.csv",
    "vendor_file": "mtge_cmbs_namr.out.20250715",
    "feed_id": 999,
    "feed_name": "test_feed_creation",
    "provider_id": 61,
    "import_frequency_id": 1,
    "schema": "beta",
    "out_dir": "C:/Users/artem/OneDrive/Work/Data Analyst stuff/BIQH - python scripts/feed_builder",
}

# Optional environment variable names for overrides
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
}

DATA_TYPE_MAP = {
    "nvarchar": 1, "int": 2, "decimal": 3, "date": 4, "datetime": 5,
    "time": 6, "boolean": 7, "bit": 7, "bigint": 8, "binary": 9, "varbinary": 9, "any": 10,
}

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
    if not tokens: return "beta"
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

def load_template(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_mapping(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    expected = {'customer_field','biqh_import_field','description','biqh_column_data_type',
                'biqh_table_schema','biqh_parent_table_name','biqh_parent_column_name',
                'biqh_relation_table_name','biqh_relation_column_name','biqh_relation_column_value',
                'biqh_relation_column_data_type','biqh_link_table_name','remarks'}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Mapping file is missing required columns: {missing}")
    return df

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

def build_column_definitions(mapping: pd.DataFrame, filetype_id: int) -> List[Dict]:
    coldefs = []
    for idx, r in mapping.iterrows():
        src_header = r["customer_field"] or r["biqh_import_field"]
        target_name = r["biqh_parent_column_name"] or r["biqh_import_field"]
        dtid, length, dec = parse_biqh_type(r["biqh_column_data_type"])
        precision = scale = None
        if dec: precision, scale = dec
        coldef = {
            "HeaderName": src_header or None,
            "SampleData": None,
            "Name": target_name or None,
            "DataTypeId": dtid,
            "Length": length if dtid in (1,10) else None,
            "Precision": precision if dtid == 3 else None,
            "Format": None,
            "Description": (r["description"] or None) if (r["description"] or "").strip() else None,
            "Scale": scale if dtid == 3 else None,
            "AllowLeadingWhite": False, "AllowTrailingWhite": False,
            "AllowLeadingSign": False, "AllowTrailingSign": False,
            "AllowParentheses": False, "AllowDecimalPoint": dtid == 3,
            "AllowThousands": False, "AllowExponent": False,
            "AllowCurrencySymbol": False, "AllowPercentage": False,
            "CultureInfo": None,
            "ColumnTypeId": 6 if filetype_id == 3 else (5 if filetype_id == 2 else 2),
            "Start": None, "End": None,
            "ColumnNumber": idx,
            "Script": None, "UseInnerXml": False,
            "XPath": None if filetype_id != 2 else None,
            "JsonPath": None if filetype_id != 3 else None,
            "NullAliases": None, "Multiplier": None,
            "TrueAliases": None, "FalseAliases": None,
            "SourceName": src_header or None,
            "RetrievalStatisticsEnabled": False,
            "Validations": [],
        }
        if dtid == 1 and (length is not None) and length > 0:
            coldef["Validations"].append(
                {"ValidationTypeId": 10, "Parameter": str(length),
                 "Message": None, "Condition": None, "IsError": True, "IsTechnical": True}
            )
        coldefs.append(coldef)
    return coldefs

def build_map_sql_for_table(parent_table: str, rows: pd.DataFrame, custom_table_name: str, add_provider_key: bool=True) -> str:
    select_lines = []
    have_provider_key = any(rows["biqh_parent_column_name"].str.lower() == "providerkey")
    if add_provider_key and not have_provider_key:
        inferred = choose_provider_key_source(rows)
        if inferred:
            src, _ = inferred
            select_lines.append(f"\tae.{src} AS [ProviderKey]")
    for _, r in rows.iterrows():
        src = r["customer_field"] or r["biqh_import_field"]
        tgt = r["biqh_parent_column_name"] or r["biqh_import_field"]
        if not src or not tgt: continue
        select_lines.append(f"\tae.{src} AS [{tgt}]")
    if not select_lines: select_lines.append("\t-- TODO: add columns")
    join_lines = []
    rel_rows = rows[rows["biqh_relation_table_name"].str.strip() != ""]
    seen = set()
    for _, rr in rel_rows.iterrows():
        rtable = rr["biqh_relation_table_name"]
        if rtable in seen: continue
        seen.add(rtable)
        alias = f"b_{re.sub('[^A-Za-z0-9]', '', rtable.lower())[:10]}"
        rcol = rr["biqh_relation_column_name"] or "ProviderKey"
        rval = rr["biqh_relation_column_value"]
        dtype = (rr["biqh_relation_column_data_type"] or "").lower()
        const = f"N'{rval}'" if dtype.startswith("nvarchar") else (rval if rval else "NULL")
        join_lines.append(f"\tINNER JOIN dbo.{rtable} {alias} ON {alias}.{rcol} = {const} AND {alias}.ProviderId = @ProviderId")
    sql = "SELECT DISTINCT\n" + ",\n".join(select_lines) + f"\nFROM\n\t@Schema.{custom_table_name} ae \n"
    if join_lines: sql += "\n".join(join_lines) + "\n"
    sql += "\nWHERE\n\tae.Record_ImportId = @ImportId\n\tAND ae.Record_HasError = 0\n"
    return sql

def build_link_map_sql(link_table: str, mapping: pd.DataFrame, custom_table_name: str) -> Optional[str]:
    link_rows = mapping[mapping["biqh_link_table_name"].str.lower() == link_table.lower()]
    if link_rows.empty: return None
    inferred_keys = {}
    for pt in sorted(mapping["biqh_parent_table_name"].unique()):
        pt_rows = mapping[mapping["biqh_parent_table_name"].str.lower() == pt.lower()]
        inf = choose_provider_key_source(pt_rows)
        if inf: inferred_keys[pt] = inf
    if len(inferred_keys) < 2: return None
    join_lines, id_selects = [], []
    for pt, (src_field, _) in inferred_keys.items():
        alias = f"b_{re.sub('[^A-Za-z0-9]', '', pt.lower())[:10]}"
        id_col = f"{pt}Id" if not pt.lower().endswith("id") else pt
        id_selects.append(f"\t{alias}.Id AS [{id_col}]")
        join_lines.append(f"\tINNER JOIN dbo.{pt} {alias} ON {alias}.ProviderKey = ae.{src_field} AND {alias}.ProviderId = @ProviderId")
    sql = "SELECT DISTINCT\n" + ",\n".join(id_selects) + f"\nFROM\n\t@Schema.{custom_table_name} ae \n"
    sql += "\n".join(join_lines) + "\n"
    sql += "\nWHERE\n\tae.Record_ImportId = @ImportId\n\tAND ae.Record_HasError = 0\n"
    return sql

def build_feed_json(template: Dict, mapping: pd.DataFrame, vendor_file: str, feed_id: int, feed_name: str,
                    provider_id: int, import_frequency_id: int, schema: Optional[str]=None,
                    custom_table_name: str="CustomTable") -> Dict:
    feed = json.loads(json.dumps(template))
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
    table_def = {
        "TableName": custom_table_name, "NodeXPath": None, "NodeJsonPath": None, "SqlQuery": None,
        "ChildTableDefinitions": [], "ColumnDefinitions": build_column_definitions(mapping, ftype_id),
        "IndexDefinitions": [],
    }
    file_def["TableDefinitions"].append(table_def)
    feed_obj["FileDefinitions"] = [file_def]

    map_block = {"Name": None, "Description": None, "UseTransaction": True, "UseSubsetKey": False,
                 "BatchSize": 0, "BiqhTableMaps": []}
    by_parent = dict(tuple(mapping.groupby("biqh_parent_table_name")))
    sort_counter = 1
    for parent_table, rows in by_parent.items():
        if not parent_table: continue
        sql = build_map_sql_for_table(parent_table, rows, custom_table_name)
        map_block["BiqhTableMaps"].append(
            {"_allowDeletes": {"Value": None, "HasValue": True},
             "_allowDeletesPolicyId": {"Value": None, "HasValue": True},
             "BiqhTableName": parent_table, "SortOrder": sort_counter, "SqlQuery": sql,
             "UseBiql": False, "BiqlQuery": None, "IsMergeStatement": True, "UseSubsetKey": None,
             "AllowDeletesPolicyId": None, "BiqhTableColumnMaps": None, "DelayTypeMaps": []}
        )
        sort_counter += 1
    link_names = sorted(set([n for n in mapping["biqh_link_table_name"].unique() if n]))
    for link_name in link_names:
        link_sql = build_link_map_sql(link_name, mapping, custom_table_name) or (
            f"-- TODO: Provide join logic to produce Id columns for [{link_name}] using @Schema.{custom_table_name}\n"
            "SELECT DISTINCT\n\t-- <table1>.Id AS [Table1Id]\n\t-- <table2>.Id AS [Table2Id]\n"
            f"FROM\n\t@Schema.{custom_table_name} ae\n"
            "WHERE\n\tae.Record_ImportId = @ImportId\n\tAND ae.Record_HasError = 0\n"
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

    row_block = {"Name": None, "Description": None, "UseTransaction": True, "UseSubsetKey": False,
                 "BatchSize": 0, "BiqhTableRowMaps": []}
    sort = 1
    all_tables = list(by_parent.keys()) + link_names
    for t in all_tables:
        row_block["BiqhTableRowMaps"].append(
            {"BiqhTableName": t, "SortOrder": sort, "SqlQuery": "", "Description": None,
             "IsMergeStatement": False, "UseSubsetKey": None, "AllowDeletes": None,
             "BiqhTableRowMapRowLinkSets": []}
        )
        sort += 1
    feed_obj["RowMapDefinitions"] = [row_block]

    auth_block = {"Name": None, "Description": None, "UseTransaction": True, "UseSubsetKey": False,
                  "BatchSize": 0, "BiqhTableAuthorizeMaps": []}
    sort = 1
    for t in all_tables:
        auth_block["BiqhTableAuthorizeMaps"].append(
            {"BiqhTableName": t, "SortOrder": sort, "Description": None, "IsMergeStatement": True,
             "UseSubsetKey": None, "AllowDeletes": None, "AuthorizeForProvider": True,
             "BiqhTableAuthorizeMapForTables": []}
        )
        sort += 1
    feed_obj["AuthorizeDefinitions"] = [auth_block]
    return feed

def merge_config(cli: dict, env: dict, defaults: dict) -> dict:
    merged = defaults.copy()
    # env overrides
    for k, env_name in ENV_MAP.items():
        if env_name in env and str(env[env_name]).strip():
            merged[k] = env[env_name]
    # CLI overrides
    for k, v in cli.items():
        if v is not None and v != "":
            merged[k] = v
    # coerce ints
    for k in ("feed_id", "provider_id", "import_frequency_id"):
        merged[k] = int(merged[k])
    return merged

def _run(cfg: dict):
    template = load_template(cfg["template"])
    mapping = load_mapping(cfg["mapping"])
    feed_json = build_feed_json(
        template=template, mapping=mapping, vendor_file=cfg["vendor_file"],
        feed_id=cfg["feed_id"], feed_name=cfg["feed_name"], provider_id=cfg["provider_id"],
        import_frequency_id=cfg["import_frequency_id"], schema=cfg.get("schema"), custom_table_name="CustomTable",
    )
    out_dir = Path(cfg["out_dir"]); out_dir.mkdir(parents=True, exist_ok=True)
    sanitized_name = sanitize_feed_name(cfg["feed_name"])
    out_path = out_dir / f"{sanitized_name}_{cfg['feed_id']}_V1.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(feed_json, f, indent=2, ensure_ascii=False)
    print(f"Wrote: {out_path}")

def main():
    ap = argparse.ArgumentParser(description="Build Feed JSON from template + mapping.", add_help=True)
    # CLI flags are optional now
    ap.add_argument("--template", help="Path to file_structure_V24.json")
    ap.add_argument("--mapping", help="Path to mapping CSV")
    ap.add_argument("--vendor-file", help="Vendor file name (drives FileTypeId & regex)")
    ap.add_argument("--feed-id", type=int, help="Feed Id (integer)")
    ap.add_argument("--feed-name", help="Feed Name (underscores removed)")
    ap.add_argument("--provider-id", type=int, help="Provider Id")
    ap.add_argument("--import-frequency-id", type=int, help="ImportFrequencyId (1=Daily,2=Monthly,...)")
    ap.add_argument("--schema", help="Override Feed.Schema")
    ap.add_argument("--out-dir", help="Output folder")
    args, _unknown = ap.parse_known_args()

    # Merge precedence: DEFAULTS < ENV < CLI
    cfg = merge_config(vars(args), os.environ, DEFAULTS)
    _run(cfg)

if __name__ == "__main__":
    main()
