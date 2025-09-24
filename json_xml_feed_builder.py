#!/usr/bin/env python3
import argparse, json, re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import pandas as pd

# ------------------ Mapping Loader ------------------
def load_mapping(path: str) -> pd.DataFrame:
    expected_cols = ['customer_field','biqh_import_field','description','biqh_column_data_type',
                     'biqh_table_schema','biqh_parent_table_name','biqh_parent_column_name',
                     'biqh_relation_table_name','biqh_relation_column_name','biqh_relation_column_value',
                     'biqh_relation_column_data_type','biqh_link_table_name','remarks']
    if not path or not Path(path).exists():
        return pd.DataFrame([], columns=expected_cols)
    df = pd.read_csv(path, dtype=str).fillna("")
    missing = set(expected_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Mapping file is missing required columns: {missing}")
    return df

# ------------------ Helpers ------------------
def parse_biqh_type(type_str: str):
    if not type_str:
        return 10, None, None
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

# ------------------ XML Vendor-driven Builder ------------------
def _xpath_segments(xp: str) -> List[str]:
    if not xp: return []
    xp = xp.lstrip("/").lstrip("./")
    return [seg for seg in xp.split("/") if seg]

def _rel_xpath(from_abs: str, to_abs: str) -> str:
    f = _xpath_segments(from_abs)
    t = _xpath_segments(to_abs)
    i = 0
    while i < len(f) and i < len(t) and f[i] == t[i]:
        i += 1
    rel = t[i:]
    return "./"+"/".join(rel) if rel else "."

def _xml_abs_paths_from_vendor(xml_path: str) -> List[str]:
    import xml.etree.ElementTree as ET
    tree = ET.parse(xml_path)
    root = tree.getroot()
    parent_of = {root: None}
    for cur in list(root.iter()):
        for ch in list(cur):
            parent_of[ch] = cur
    def _segments(e):
        segs=[]
        while e is not None:
            segs.append(e.tag)
            e=parent_of.get(e)
        return list(reversed(segs))
    paths=set()
    for elem in root.iter():
        if len(list(elem))==0 and (elem.text or "").strip()!="":
            paths.add("/"+"/".join(_segments(elem)))
        for aname in elem.attrib:
            paths.add("/"+"/".join(_segments(elem))+f"/@{aname}")
    return sorted(paths)

def build_xml_table_definitions_from_vendor(xml_path: str, mapping: pd.DataFrame) -> List[Dict]:
    leaf_paths=_xml_abs_paths_from_vendor(xml_path)
    if not leaf_paths: return []
    def parent(xp:str):
        seg=_xpath_segments(xp)
        if not seg: return None
        if seg[-1].startswith("@"): seg=seg[:-1]
        seg=seg[:-1]
        return "/"+"/".join(seg) if seg else "/"
    parent_to_leaves={}
    for lp in leaf_paths:
        p=parent(lp) or "/"
        parent_to_leaves.setdefault(p,[]).append(lp)
    root_tag=_xpath_segments(leaf_paths[0])[0]
    root_path=f"/{root_tag}"
    table_nodes={root_path}
    for p,l in parent_to_leaves.items():
        if len(l)>=2: table_nodes.add(p)
    def ancestors(xp):
        seg=_xpath_segments(xp)
        return ["/"+"/".join(seg[:i]) for i in range(1,len(seg))]
    for n in list(table_nodes):
        for a in ancestors(n): table_nodes.add(a)
    def parent_node(xp:str):
        seg=_xpath_segments(xp)
        return "/"+"/".join(seg[:-1]) if len(seg)>1 else None
    children={}
    for n in sorted(table_nodes):
        p=parent_node(n)
        if p and p in table_nodes:
            children.setdefault(p,[]).append(n)
    for k in children: children[k].sort()
    map_by_path={}
    if not mapping.empty:
        for _,r in mapping.iterrows():
            p=(r.get("customer_field") or "").strip()
            if p.startswith("/"): map_by_path[p]=r
    def coldefs(node_abs:str):
        out=[]
        for leaf in parent_to_leaves.get(node_abs,[]):
            rel=_rel_xpath(node_abs,leaf)
            name=_xpath_segments(leaf)[-1].lstrip("@")
            name=re.sub(r"[^A-Za-z0-9_]+","_",name).strip("_") or "field"
            dtid,length,dec=10,None,None
            r=map_by_path.get(leaf)
            if r is not None:
                nm=r.get("biqh_parent_column_name") or r.get("biqh_import_field") or name
                dtid,length,dec=parse_biqh_type(r.get("biqh_column_data_type"))
                name=nm
            precision,scale=(dec if dec else (None,None))
            out.append({"Name":name,"DataTypeId":dtid,"Length":length,"Precision":precision,"Scale":scale,
                        "ColumnTypeId":5,"XPath":rel,"JsonPath":None})
        return out
    def make_table(node_abs:str):
        tname="_".join(_xpath_segments(node_abs)) or "Root"
        par=parent_node(node_abs)
        node_xpath=node_abs if par is None else _rel_xpath(par,node_abs).lstrip("./")
        tbl={"TableName":tname,"NodeXPath":node_xpath,"NodeJsonPath":None,
             "SqlQuery":None,"ChildTableDefinitions":[],"ColumnDefinitions":coldefs(node_abs),"IndexDefinitions":[]}
        for ch in children.get(node_abs,[]):
            tbl["ChildTableDefinitions"].append(make_table(ch))
        return tbl
    roots=[n for n in table_nodes if parent_node(n) not in table_nodes]
    return [make_table(r) for r in sorted(roots)]

# ------------------ JSON Vendor-driven Builder ------------------
def _json_abs_paths_and_arrays(obj: Any, base: str = "$") -> Tuple[Set[str], Set[str]]:
    leafs:set=set(); tables:set=set()
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

def build_json_table_definitions_from_vendor(json_path: str, mapping: pd.DataFrame) -> List[Dict]:
    data=json.loads(Path(json_path).read_text(encoding="utf-8"))
    root=data if not (isinstance(data,list) and data and isinstance(data[0],dict)) else {"_root":data}
    leafs,tables=_json_abs_paths_and_arrays(root,"$")
    if not tables: tables={"$"}
    def is_anc(a,b): return b.startswith(a) and a!=b
    parents={}
    for t in tables:
        par=None
        for cand in tables:
            if is_anc(cand,t):
                if par is None or len(cand)>len(par): par=cand
        parents[t]=par
    table_to_leafs={t:[] for t in tables}
    for lf in leafs:
        anc=None
        for t in tables:
            if lf.startswith(t):
                if anc is None or len(t)>len(anc): anc=t
        if anc is None and "$" in tables: anc="$"
        if anc: table_to_leafs.setdefault(anc,[]).append(lf)
    map_by_path={}
    if not mapping.empty:
        for _,r in mapping.iterrows():
            p=(r.get("customer_field") or "").strip()
            if p.startswith("$."): map_by_path[p]=r
    def coldefs(tnode:str):
        out=[]
        for lf in sorted(set(table_to_leafs.get(tnode,[]))):
            rel="$"+lf[len(tnode):] if lf.startswith(tnode) else lf
            name=re.sub(r"[^A-Za-z0-9_]+","_",lf.split(".")[-1]).strip("_") or "field"
            dtid,length,dec=10,None,None
            r=map_by_path.get(lf)
            if r is not None:
                nm=r.get("biqh_parent_column_name") or r.get("biqh_import_field") or name
                dtid,length,dec=parse_biqh_type(r.get("biqh_column_data_type"))
                name=nm
            precision,scale=(dec if dec else (None,None))
            out.append({"Name":name,"DataTypeId":dtid,"Length":length,"Precision":precision,"Scale":scale,
                        "ColumnTypeId":6,"XPath":None,"JsonPath":rel})
        return out
    def tname(tp):
        if tp=="$": return "Root"
        return "_".join([t for t in re.split(r"\.|\[\*\]",tp) if t and t!="$"])
    def make_table(tp):
        par=parents.get(tp)
        node_json=tp if par is None else "$"+tp[len(par):]
        tbl={"TableName":tname(tp),"NodeXPath":None,"NodeJsonPath":node_json,
             "SqlQuery":None,"ChildTableDefinitions":[],"ColumnDefinitions":coldefs(tp),"IndexDefinitions":[]}
        for ch in [c for c,p in parents.items() if p==tp]:
            tbl["ChildTableDefinitions"].append(make_table(ch))
        return tbl
    roots=[t for t,p in parents.items() if p is None] or ["$"]
    return [make_table(r) for r in roots]

# ------------------ Feed Builder ------------------
def build_feed_json(template_path,vendor_file,feed_id,feed_name,provider_id,import_frequency_id,mapping_path=None,out_dir="."):
    tpl=json.loads(Path(template_path).read_text(encoding="utf-8"))
    mapping=load_mapping(mapping_path) if mapping_path else load_mapping(None)
    ftype_id=2 if vendor_file.endswith(".xml") else 3 if vendor_file.endswith(".json") else 1
    file_def={"FileTypeId":ftype_id,"TableDefinitions":[]}
    if ftype_id==2:
        tbls=build_xml_table_definitions_from_vendor(vendor_file,mapping)
        if tbls: file_def["TableDefinitions"]=tbls
    elif ftype_id==3:
        tbls=build_json_table_definitions_from_vendor(vendor_file,mapping)
        if tbls: file_def["TableDefinitions"]=tbls
    tpl["Feed"]["FileDefinitions"]=[file_def]
    Path(out_dir).mkdir(parents=True,exist_ok=True)
    outp=Path(out_dir)/f"{feed_name}_{feed_id}_V1.json"
    outp.write_text(json.dumps(tpl,indent=2),encoding="utf-8")
    return outp

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
