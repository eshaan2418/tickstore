#!/usr/bin/env python3
"""Parquet + DuckDB baseline on the same trades/quotes tickstore ingested.

Writes Parquet (zstd and snappy) from an in-memory Arrow table (timed: encode + write), then runs
the same four queries as `tickstore bench` with DuckDB (median of N runs) and prints results so
they can be compared value-for-value with tickstore's.
usage: baseline_duckdb.py CSVDIR OUTDIR RUNS
"""
import json, os, statistics, sys, time
import duckdb, pyarrow as pa, pyarrow.csv as pcsv, pyarrow.parquet as pq

csvdir, outdir, runs = sys.argv[1], sys.argv[2], int(sys.argv[3])
os.makedirs(outdir, exist_ok=True)
H = 3600 * 10**9
res = {"duckdb": duckdb.__version__, "pyarrow": pa.__version__}
tables = {}
for t in ["trades", "quotes"]:
    tables[t] = pcsv.read_csv(os.path.join(csvdir, f"{t}.csv"))
    res[f"{t}_rows"] = tables[t].num_rows
for codec in ["zstd", "snappy"]:
    for t in ["trades", "quotes"]:
        p = os.path.join(outdir, f"{t}_{codec}.parquet")
        ts = []
        for _ in range(3):
            s = time.perf_counter()
            pq.write_table(tables[t], p, compression=codec)
            ts.append(time.perf_counter() - s)
        res[f"{t}_{codec}_bytes"] = os.path.getsize(p)
        res[f"{t}_{codec}_write_s"] = statistics.median(ts)
con = duckdb.connect()
con.execute("PRAGMA threads=8")
tp, qp = os.path.join(outdir, "trades_zstd.parquet"), os.path.join(outdir, "quotes_zstd.parquet")
t0, t1 = int(9.5 * H), 16 * H
queries = {
    "range_count_1h": f"SELECT count(*) FROM '{tp}' WHERE symbol='AAPL' AND ts >= {10*H} AND ts < {11*H}",
    "bars_1min": f"""SELECT count(*) AS bars, sum(vol) AS volume FROM (
        SELECT (ts - {t0}) // 60000000000 AS b, sum(size) AS vol, sum(price*size)/sum(size) AS vwap,
               arg_min(price, ts) AS o, max(price) AS h, min(price) AS l, arg_max(price, ts) AS c
        FROM '{tp}' WHERE symbol='AAPL' AND ts >= {t0} AND ts < {t1} AND (flags & 4) = 0 GROUP BY b)""",
    "asof_join": f"""SELECT count(*) AS trades, count(q.ts) AS matched,
        sum(2*abs(t.price - (q.bid+q.ask)/2.0)/((q.bid+q.ask)/2.0)*1e4*t.size)/sum(t.size) AS eff_spread_bps,
        sum((q.bid+q.ask)/2.0) AS checksum
        FROM (SELECT * FROM '{tp}' WHERE symbol='AAPL' AND ts >= {t0} AND ts < {t1} AND (flags & 4) = 0) t
        ASOF LEFT JOIN (SELECT * FROM '{qp}' WHERE symbol='AAPL') q ON t.ts >= q.ts""",
    "market_vwap_all_symbols": f"""SELECT count(*) AS symbols, sum(v) AS volume FROM (
        SELECT symbol, sum(size) AS v, sum(price*size)/sum(size) AS vwap FROM '{tp}'
        WHERE ts >= {t0} AND ts < {t1} AND (flags & 4) = 0 GROUP BY symbol)""",
}
res["queries"] = []
for name, sql in queries.items():
    ms = []
    for _ in range(runs):
        s = time.perf_counter()
        out = con.execute(sql).fetchall()
        ms.append((time.perf_counter() - s) * 1e3)
    res["queries"].append({"query": name, "runs_ms": ms, "median_ms": statistics.median(ms), "result": [list(r) for r in out]})
print(json.dumps(res, indent=2, default=str))
