#!/usr/bin/env bash
set -euo pipefail
DB="${DB:-$HOME/quantfactory/_data/tickstore/db-01302019}"
echo "== AAPL 2019-01-30, first five 1-minute bars (Nasdaq prints)"
bin/tickstore bars -dir "$DB" -symbol AAPL -from 09:30 -to 09:35 | python3 scripts/demo_fmt.py bars
echo "== AAPL trades as-of joined to the prevailing quote (regular session)"
bin/tickstore asof -dir "$DB" -symbol AAPL | python3 scripts/demo_fmt.py asof
