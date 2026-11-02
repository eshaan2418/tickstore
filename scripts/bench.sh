#!/usr/bin/env bash
# tickstore vs Parquet+DuckDB on the same ITCH-derived trades and quotes.
set -euo pipefail
DB="${DB:-$HOME/quantfactory/_data/tickstore/db-01302019}"
WORK="${WORK:-$HOME/quantfactory/_data/tickstore}"
OUT="bench/results/$(date +%Y%m%d-%H%M%S)"; mkdir -p "$OUT"
{ date -u +%FT%TZ; (sysctl -n machdep.cpu.brand_string 2>/dev/null || true); go version; uptime; } > "$OUT/machine.txt"
bin/tickstore pack -dir "$DB" > /dev/null
bin/tickstore bench -dir "$DB" -symbol AAPL -runs 5 -out "$OUT/tickstore_queries.json" > /dev/null
bin/tickstore bench-write -dir "$DB" -runs 3 -out "$OUT/tickstore_write.json" > /dev/null
bin/tickstore export -dir "$DB" -out "$WORK/csv"
uv run -q --with duckdb --with pyarrow python research/baseline_duckdb.py "$WORK/csv" "$WORK/parquet" 5 > "$OUT/duckdb_parquet.json"
python3 scripts/summarize.py "$OUT" | tee "$OUT/SUMMARY.md"
