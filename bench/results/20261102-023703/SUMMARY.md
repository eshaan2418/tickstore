# tickstore vs Parquet + DuckDB (20261102-023703)

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
| tickstore (no zstd) | 256,303 | 94,664,744 |
| tickstore (zstd stage) | 268,642 | 61,451,488 |
| pyarrow Parquet (zstd) | 13,030,523 | 57,056,459 |
| pyarrow Parquet (snappy) | 14,002,590 | 75,475,283 |

## Queries (median of 5; AAPL; regular session)

| query | tickstore ms | DuckDB on Parquet ms | results agree |
|---|---|---|---|
| range_count_1h | 1.60 | 4.15 | True |
| bars_1min | 9.42 | 12.43 | True |
| asof_join | 62.95 | 34.17 | True |
| market_vwap_all_symbols | 2168.19 | 36.92 | True |
| market_vwap_all_symbols_8_workers | 1982.98 | 36.92 | True |
