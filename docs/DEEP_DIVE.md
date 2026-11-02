# tickstore: deep-dive interview questions

### 1. Why delta-of-delta for timestamps?
Tick timestamps are monotone, with locally similar gaps. The first difference removes the
absolute level and the second removes the local rate, leaving small integers near zero. Gorilla's
buckets spend 1 bit on a zero and 9-25 bits on small values. Encoding raw nanosecond timestamps
would cost 64 bits each.

### 2. Why not Gorilla XOR for prices?
Prices here are fixed-point integers. XOR coding is designed for float64 bit patterns, where
similar values share sign, exponent and leading mantissa bits. For integers, a plain delta (a few
ticks) plus zigzag varint is simpler and smaller. XOR is implemented for float analytics columns.
Choosing fixed point also keeps aggregations exact.

### 3. Why add zstd if you already delta-encode?
They remove different redundancy. Delta coding removes trend and level. zstd removes repetition:
sizes are mostly round lots (100, 200), flags are long runs, and price deltas repeat (+1, −1
tick). Measured on this data, the zstd stage cut the database from 198 MB to 114 MB
(3.6× → 6.3× versus raw int64 columns), roughly on par with Parquet+zstd.

### 4. Your per-symbol queries beat DuckDB but the cross-sectional one doesn't. Why?
Partitioning per symbol makes "one symbol, one day" read exactly one small file, with no symbol
filter and no row-group pruning. A whole-market query then opens about 7,500 files, and each open
plus mmap plus footer parse costs tens of microseconds. DuckDB reads one Parquet file, pushes the
filter down and parallelises across row groups. A worker pool recovers part of the gap. The real
fix is a second layout for cross-sectional access: day-level files sorted by (symbol, ts), with a
symbol index, which is what Parquet effectively is here. The trade-off is inherent: choose the
layout for the dominant query.

### 5. Why trust that the results are correct?
- **Unit and property tests:** encoders round-trip exactly (including extremes, and fuzzed);
  range scans equal brute force on random queries; bars and as-of joins equal brute-force
  implementations.
- **Independent engine:** the four benchmark queries are computed independently by DuckDB on the
  same data exported to Parquet. Counts, volumes, effective spread and the as-of checksum agree
  exactly.

### 6. How good is Lee-Ready on this data?
The feed records the true aggressor (which side's resting order was executed), so classification
accuracy can be measured directly. On AAPL, Lee-Ready (quote rule, tick test at the mid) agrees
with the true side on 88.4% of trades, in line with the ~85% usually reported. The errors cluster
on trades at the midpoint and on trades whose prevailing quote is stale by microseconds.

### 7. What happens on a crash mid-write?
Partitions are written to a temp file, fsynced and renamed, so a reader sees the old file or the
complete new one, never a torn file. The per-block CRC catches media corruption, and the reader
validates every offset, so a corrupted file is rejected rather than misread (fuzzed).

### 8. How would you support streaming ingest?
Append-only "open" blocks in a write-ahead segment, sealed into compressed blocks every N rows or
T seconds. Readers merge sealed blocks with the open segment's tail. A manifest is updated
atomically, as in LSM-style time-series stores (QuestDB, InfluxDB's TSM).

### 9. Why mmap?
Zero-copy access, OS-managed caching, and trivially concurrent readers. The cost is that a SIGBUS
on a truncated file would crash the process, which the atomic-rename protocol prevents for files
this process wrote. For untrusted files I would read rather than map.

### 10. What would kdb+ do differently?
kdb+ keeps columns as flat vectors (one file per column per partition), sorted with attributes
(`s#`, `p#`), and runs vectorised q over them. It compresses with general-purpose codecs on top.
Its as-of join (`aj`) is the same merge idea. tickstore is a small, explicit version of that
design with typed encodings.
