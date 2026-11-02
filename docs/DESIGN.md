# tickstore design

## Data model
- **trades**: `ts, price, size, flags`. Prices are ITCH fixed point (1e-4 $). Flags record the
  aggressor side taken from the feed, whether the print is a cross (auction), and whether it was
  a non-displayed execution.
- **quotes**: `ts, bid, ask, bid_size, ask_size`, one row per top-of-book change. Derived from
  the full L3 book during ingest, for a configurable set of symbols.

Partitioning is one file per (table, symbol, day). Symbols are the dominant filter in market data
queries, so a per-symbol query touches exactly one file and needs no symbol column. The cost
shows up in cross-sectional queries: every symbol is a separate file open. See the benchmarks.

## File format
```
"TKS1" | u16 ncols | (u16 len, name)*
block*: u32 rows | i64 tmin | i64 tmax | (u32 len, u8 codec, chunk)* | u32 CRC32-C
footer: (u64 offset, u32 rows, i64 tmin, i64 tmax)* | u32 nblocks | u64 footer_offset | "TKSF"
```
- **Blocks** hold 8,192 rows, each column encoded independently. A query decodes only its
  projected columns (projection pushdown), and the footer's per-block time ranges let it skip
  blocks outside the query window (binary search, then a binary search inside the boundary
  blocks).
- **Encodings:**
  - **Timestamps: Gorilla delta-of-delta.** Inter-arrival times within a block are similar, so
    the second difference costs 1-9 bits most of the time.
  - **Prices and sizes: delta + zigzag + varint.** Consecutive trade prices differ by a few
    ticks.
  - **zstd stage:** an optional zstd pass on top of each encoded chunk, kept only when it is
    smaller. Delta coding removes trend; zstd captures the repetition (round lots, constant flags,
    repeated deltas) that varints cannot.
  - **Floats: Gorilla XOR** (`enc.EncodeFloats`) is implemented and tested for analytics columns.
    The schema uses fixed point, so it is unused by the default tables.
- **Integrity:** a CRC32-C per block (`Verify`). Readers reject a bad header, footer or offsets
  without reading out of bounds (fuzzed).
- **Atomicity:** a file is written to `path.tmp`, fsynced, then renamed, so readers never see a
  partially written partition.

## Reads
Files are memory-mapped, so the OS page cache does the caching, and cold and warm runs differ only
in page faults. `Scan(t0, t1, projection, fn)` hands the callback decoded column slices plus the
[lo, hi) row window. The slices are reused between blocks, so a scan allocates per file, not per
row.

## Queries
| query | algorithm |
|---|---|
| range scan | footer binary search + in-block binary search on ts |
| OHLCV/VWAP bars | single pass over (ts, price, size, flags), bucket = floor((ts−t0)/interval); crosses excluded |
| as-of join | merge join of two time-ordered streams (trades, quotes); quote rule at the mid, tick test fallback (Lee-Ready); effective spread 2·\|p−mid\|/mid |
| market VWAP | one VWAP per symbol file; sequential or a worker pool |

## Ingest
Ingest runs one pass over the ITCH file. It keeps an order map (ref → symbol, side, price, shares)
because E messages carry no price, and a sorted-level book per quoted symbol. It emits:
- trades for E (resting price) and C messages, printable only, per the ITCH spec;
- P (non-displayed) and Q (cross) prints, flagged;
- quotes whenever a quoted symbol's top of book changes.

Throughput is bounded by the Go map and gzip inflate, not by the storage writer, whose encode rate
is benchmarked separately.

## Not here
Multi-day partitions and a catalog; concurrent writers; streaming (append-while-query) ingest; SQL;
compression dictionaries shared across partitions; secondary indexes (e.g. by price).
