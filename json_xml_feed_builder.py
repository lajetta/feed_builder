#!/usr/bin/env python3
import json, re
from pathlib import Path
import pandas as pd
import xml.etree.ElementTree as ET

# ------------------ Mapping Loader ------------------
def load_mapping(path: str) -> pd.DataFrame:
    expected_cols = ['customer_field','biqh_import_field','description','biqh_column_data_type',
                     'biqh_table_schema','biqh_parent_table_name','biqh_parent_column_name',
                     'biqh_relation_table_name','biqh_relation_column_name','biqh_relation_column_value',
                     'biqh_relation_column_data_type','biqh_link_table_name','remarks']
    if not path or not Path(path).exists():
        return pd.DataFrame([], columns=expected_cols)
    df = pd.read_csv(path, dtype=str).fillna("")
    return df

# ------------------ Helpers ------------------
def parse_biqh_type(type_str: str):
    if not type_str: return 10, None, None
    t = type_str.lower()
    if t.startswith("int"): return 1, None, None
    if t.startswith("decimal"):
        m = re.search(r"decimal\((\d+),(\d+)\)", t)
        if m: return 3, None, (int(m.group(1)), int(m.group(2)))
        return 3, None, None
    if t.startswith("date"): return 4, None, None
    if t.startswith("datetime"): return 7, None, None
    if t.startswith("time"): return 8, None, None
    if t.startswith("bit") or t.startswith("bool"): return 2, None, None
    if t.startswith("nvarchar"):
        m = re.search(r"nvarchar\((max|\d+)\)", t)
        if m:
            length = None if m.group(1)=="max" else int(m.group(1))
            return 10, length, None
        return 10, None, None
    return 10, None, None

# ------------------ XML Parsing ------------------
def xml_abs_paths(xml_path: str):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    parent_of = {root: None}
    for cur in list(root.iter()):
        for ch in list(cur):
            parent_of[ch] = cur
    def segments(e):
        segs=[]
        while e is not None:
            segs.append(e.tag)
            e=parent_of.get(e)
        return list(reversed(segs))
    paths=set()
    for elem in root.iter():
        if len(list(elem))==0 and (elem.text or "").strip():
            paths.add("/"+"/".join(segments(elem)))
        for aname in elem.attrib:
            paths.add("/"+"/".join(segments(elem))+f"/@{aname}")
    return sorted(paths)

# ------------------ JSON Parsing ------------------
def json_paths(obj, base="$"):
    leafs=set(); tables=set()
    def walk(o,p):
        if isinstance(o,dict):
            if not o: leafs.add(p)
            else:
                for k,v in o.items(): walk(v,f"{p}.{k}")
        elif isinstance(o,list):
            if any(isinstance(it,dict) for it in o):
                tables.add(f"{p}[*]")
                for it in o[:1]:
                    if isinstance(it,dict): walk(it,f"{p}[*]")
            else: leafs.add(f"{p}[*]")
        else: leafs.add(p)
    walk(obj,base)
    return leafs,tables

# ------------------ Builder ------------------
def build_file_definitions(vendor_file: str, mapping: pd.DataFrame):
    if vendor_file.endswith(".xml"):
        leaf_paths=xml_abs_paths(vendor_file)
        column_defs=[]
        for lp in leaf_paths:
            name=re.sub(r"[^A-Za-z0-9_]+","_",lp.split("/")[-1].lstrip("@")).strip("_") or "field"
            column_defs.append({
                "Name":name,"DataTypeId":10,"Length":None,"Precision":None,"Scale":None,
                "ColumnTypeId":5,"XPath":lp,"JsonPath":None
            })
        return [default_file_def(2,column_defs)]
    elif vendor_file.endswith(".json"):
        data=json.loads(Path(vendor_file).read_text(encoding="utf-8"))
        leafs,tables=json_paths(data)
        column_defs=[]
        for lf in leafs:
            name=re.sub(r"[^A-Za-z0-9_]+","_",lf.split(".")[-1]).strip("_") or "field"
            column_defs.append({
                "Name":name,"DataTypeId":10,"Length":None,"Precision":None,"Scale":None,
                "ColumnTypeId":6,"XPath":None,"JsonPath":lf
            })
        return [default_file_def(3,column_defs)]
    else:
        # CSV/Excel fallback
        import csv
        with open(vendor_file,newline='',encoding="utf-8") as f:
            reader=csv.reader(f)
            headers=next(reader)
        column_defs=[]
        for h in headers:
            name=re.sub(r"[^A-Za-z0-9_]+","_",h).strip("_") or "field"
            column_defs.append({
                "Name":name,"DataTypeId":10,"Length":None,"Precision":None,"Scale":None,
                "ColumnTypeId":1,"XPath":None,"JsonPath":None
            })
        return [default_file_def(1,column_defs)]

# ------------------ Defaults from reference ------------------
def default_file_def(ftype_id:int, column_defs:list):
    return {
        "FileTypeId": ftype_id,
        "Name": None,
        "FileNameRegex": None,
        "FileNameRegexDescription": None,
        "CsvDelimiterCharacter": "," if ftype_id==1 else None,
        "HasHeader": True if ftype_id==1 else None,
        "SubsetGroupNumber": None,
        "DynamicColumnCount": True,
        "DefaultColumnNamePrefix": None,
        "TrimWhiteSpaces": True,
        "AdvancedEscapingEnabled": True,
        "QuoteCharacter": None,
        "DoubleQuoteEscapingEnabled": True,
        "ColumnHeaderTypeSeparator": None,
        "ReadHeaders": True,
        "CheckHeaders": False,
        "CheckUnexpectedHeaders": False,
        "UseEmbargoDateTime": False,
        "EmbargoDateTimeComposite": None,
        "IgnoreColumnsWithEmptyHeader": True,
        "SkipEmptyLines": True,
        "SkipFirstNumberOfLines": 0,
        "EndOfFileRegex": None,
        "CheckZippedFileNameByRegex": False,
        "DefaultMicrosoftStandardTimeZoneId": None,
        "NumberDecimalSeparator": ".",
        "NumberGroupSeparator": None,
        "RootXPath": None,
        "XmlNamespaces": None,
        "RootJsonPath": None,
        "DetectEncoding": False,
        "SheetNumber": 1 if ftype_id==4 else None,
        "SortOrder": None,
        "TableDefinitions": [{
            "TableName": "AutoTable",
            "NodeXPath": None,
            "NodeJsonPath": None,
            "SqlQuery": None,
            "ChildTableDefinitions": [],
            "ColumnDefinitions": column_defs,
            "IndexDefinitions": []
        }]
    }

# ------------------ Feed Builder ------------------
def build_feed_json(template_path,vendor_file,feed_id,feed_name,provider_id,import_frequency_id,mapping_path=None,out_dir="."):
    tpl=json.loads(Path(template_path).read_text(encoding="utf-8"))
    mapping=load_mapping(mapping_path) if mapping_path else load_mapping(None)
    file_defs=build_file_definitions(vendor_file,mapping)
    tpl["Feed"]["FileDefinitions"]=file_defs
    Path(out_dir).mkdir(parents=True,exist_ok=True)
    outp=Path(out_dir)/f"{feed_name}_{feed_id}_V1.json"
    outp.write_text(json.dumps(tpl,indent=2),encoding="utf-8")
    return outp

# ------------------ Config block ------------------


if __name__ == "__main__":
    out = build_feed_json(
        template_path="file_structure_V24.json",
        vendor_file="ETF-101753-ISHARES SLI (CH).json",
        feed_id="999",
        feed_name="test_feed",
        provider_id="61",
        import_frequency_id="1",
        mapping_path=None,       # or "mtge_ref_mapping.csv"
        out_dir="."
    )
    print("Built feed:", out)
