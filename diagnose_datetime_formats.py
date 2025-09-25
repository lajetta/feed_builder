import json
from feed_builder_generator import load_mapping, detect_datetime_formats, read_vendor_df, find_series_for_column, parse_biqh_type

mapping = load_mapping('cds_corps_pricing_mapping.csv')
column_meta = {}
for _, r in mapping.iterrows():
    dt_raw = (r.get('biqh_column_data_type') or '').strip()
    if not dt_raw:
        continue
    dtid, _, _ = parse_biqh_type(dt_raw)
    if dtid in (4,5,6):
        key = (r.get('customer_field') or r.get('biqh_import_field') or '').strip()
        if key:
            column_meta[key] = dtid

print('Detected date-like mapping entries (column_meta):')
print(json.dumps(column_meta, indent=2))

fmt = detect_datetime_formats('Corps_Pricing_N1600-2025-09-09.csv', column_meta, nrows=500)
print('\nFormats from detect_datetime_formats:')
print(json.dumps(fmt, indent=2))

# show sample values from the vendor file for each key
print('\nSample values from vendor (first 20 rows):')
df = read_vendor_df('Corps_Pricing_N1600-2025-09-09.csv', nrows=20)
for key in column_meta:
    s = find_series_for_column(df, key)
    vals = None
    if s is not None:
        vals = [str(x) for x in s.head(20).tolist()]
    print(f'\nKey: {key}\n  Detected format: {fmt.get(key)}\n  Samples: {vals}')
