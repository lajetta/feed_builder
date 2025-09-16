
#!/usr/bin/env python3
"""
parse_and_build.py

Example integration:
1) Use file_parser_module.parse_file(...) to generate a mapping DataFrame.
2) Save it to mapping.csv.
3) Call json_feed_builder_mssql._run(...) with a config dict, pointing to file_structure_V24.json.

Note: Adjust feed_id, feed_name, provider_id, import_frequency_id, out_dir, etc.
"""
from pathlib import Path
import os
import pandas as pd

from file_parser_module import parse_file, save_mapping_csv

# Import builder safely
import importlib.util, sys, json
spec = importlib.util.spec_from_file_location("builder", str(Path(__file__).parent / "json_feed_builder_mssql.py"))
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)  # type: ignore

def run_pipeline(vendor_file: str, template_path: str, out_dir: str,
                 feed_id: int = 900, feed_name: str = "AutoFeed", provider_id: int = 62,
                 import_frequency_id: int = 1, schema: str = "beta"):
    mapping_df = parse_file(vendor_file)
    mp_path = Path(out_dir) / "auto_mapping.csv"
    mp_path.parent.mkdir(parents=True, exist_ok=True)
    save_mapping_csv(mapping_df, mp_path)
    cfg = dict(
        template=template_path,
        mapping=str(mp_path),
        vendor_file=vendor_file,
        feed_id=feed_id,
        feed_name=feed_name,
        provider_id=provider_id,
        import_frequency_id=import_frequency_id,
        schema=schema,
        out_dir=out_dir,
        mssql_conn_str=None,
    )
    # Merge defaults & run
    cfg = builder.merge_config(cfg, os.environ, builder.DEFAULTS)
    builder._run(cfg)
    return mp_path

if __name__ == "__main__":
    # Example usage (adjust paths as needed)
    out_dir = "/mnt/data/out"
    template = "/mnt/data/file_structure_V24.json"
    vendor = "/mnt/data/ETF-101753-ISHARES SLI (CH).json"
    run_pipeline(vendor, template, out_dir)
