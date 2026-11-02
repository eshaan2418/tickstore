#!/usr/bin/env python3
import json, os, sys
d = sys.argv[1]
T = json.load(open(os.path.join(d, "tickstore_queries.json")))
W = json.load(open(os.path.join(d, "tickstore_write.json")))
D = json.load(open(os.path.join(d, "duckdb_parquet.json")))
ing = json.load(open(os.path.join("bench/results", "ingest.json"))) if os.path.exists("bench/results/ingest.json") else None
print(f"# tickstore vs Parquet + DuckDB ({os.path.basename(d)})\n")
print(f"Data: NASDAQ ITCH 2019-01-30 -> {D['trades_rows']:,} trades (all symbols), {D['quotes_rows']:,} top-of-book quotes (20 symbols).\n")
print("## Storage\n")
print("| format | trades + quotes bytes | vs raw int64 columns |")
print("|---|---|---|")
raw = (D["trades_rows"] * 4 + D["quotes_rows"] * 5) * 8
print(f"| tickstore (delta/DoD + zstd stage) | {T['db_bytes']:,} | {raw / T['db_bytes']:.1f}x |")
for c in ["zstd", "snappy"]:
    b = D[f"trades_{c}_bytes"] + D[f"quotes_{c}_bytes"]
    print(f"| Parquet ({c}, pyarrow {D['pyarrow']}) | {b:,} | {raw / b:.1f}x |")
print("\n## Write (encode + write all trade partitions from memory)\n")
print("| writer | rows/s | bytes |\n|---|---|---|")
labels = {"raw_nosync": "tickstore, no zstd, no fsync", "zstd_nosync": "tickstore, zstd, no fsync", "zstd_fsync": "tickstore, zstd, fsync per partition (7,538 files)"}
for k, lab in labels.items():
    print(f"| {lab} | {W[k]['rows_per_sec']:,.0f} | {W[k]['bytes']:,} |")
for c in ["zstd", "snappy"]:
    print(f"| pyarrow Parquet ({c}), one file, no fsync | {D['trades_rows'] / D[f'trades_{c}_write_s']:,.0f} | {D[f'trades_{c}_bytes']:,} |")
print("\n## Queries (median of 5; AAPL; regular session)\n")
print("| query | tickstore ms | DuckDB on Parquet ms | results agree |")
print("|---|---|---|---|")
dq = {q["query"]: q for q in D["queries"]}
for q in T["queries"]:
    name = q["query"]
    base = dq.get(name.replace("_8_workers", "").replace("_pack", ""))
    r = q["result"]
    agree = "-"
    if base:
        b = base["result"][0]
        if name == "range_count_1h": agree = str(r == b[0])
        elif name == "bars_1min": agree = str(r["bars"] == b[0] and r["volume"] == b[1])
        elif name == "asof_join": agree = str(r["trades"] == b[0] and r["matched_with_quote"] == b[1] and abs(r["effective_spread_bps_volume_weighted"] - b[2]) < 1e-9 and abs(r["checksum"] - b[3]) < 1e-3)
        elif name.startswith("market_vwap"): agree = str(r["symbols"] == b[0] and r["volume"] == b[1])
    print(f"| {name} | {q['median_ms']:.2f} | {base['median_ms']:.2f} | {agree} |" if base else f"| {name} | {q['median_ms']:.2f} | - | - |")
