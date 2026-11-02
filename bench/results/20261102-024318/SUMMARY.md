# tickstore vs Parquet + DuckDB (20261102-024318)

Data: NASDAQ ITCH 2019-01-30 -> 9,580,917 trades (all symbols), 10,267,547 top-of-book quotes (20 symbols).

## Storage

| format | trades + quotes bytes | vs raw int64 columns |
|---|---|---|
| tickstore (delta/DoD + zstd stage) | 114,462,602 | 6.3x |
| Parquet (zstd, pyarrow 26.0.0) | 118,472,630 | 6.1x |
| Parquet (snappy, pyarrow 26.0.0) | 165,557,145 | 4.3x |

## Write (encode + write all trade partitions from memory)

| writer | rows/s | bytes |
|---|---|---|
| tickstore, no zstd, no fsync | 5,410,714 | 94,664,744 |
| tickstore, zstd, no fsync | 4,105,089 | 61,451,488 |
| tickstore, zstd, fsync per partition (7,538 files) | 303,829 | 61,451,488 |
| pyarrow Parquet (zstd), one file, no fsync | 12,345,191 | 57,056,459 |
| pyarrow Parquet (snappy), one file, no fsync | 13,482,843 | 75,475,283 |

## Queries (median of 5; AAPL; regular session)

| query | tickstore ms | DuckDB on Parquet ms | results agree |
|---|---|---|---|
| range_count_1h | 0.88 | 5.14 | True |
| bars_1min | 4.95 | 13.38 | True |
| asof_join | 33.66 | 36.85 | True |
| market_vwap_all_symbols | 886.57 | 38.43 | True |
| market_vwap_all_symbols_8_workers | 988.04 | 38.43 | True |
| market_vwap_all_symbols_pack | 590.86 | 38.43 | True |
| market_vwap_all_symbols_pack_8_workers | 168.79 | 38.43 | True |
