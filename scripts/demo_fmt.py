import json, sys
d = json.load(sys.stdin)
if sys.argv[1] == "bars":
    for b in d:
        s = b["start_ns"] // 10**9
        print(f'  {s // 3600:02d}:{s % 3600 // 60:02d}  O {b["open"]/1e4:.2f}  H {b["high"]/1e4:.2f}  L {b["low"]/1e4:.2f}  C {b["close"]/1e4:.2f}  vol {b["volume"]:>8,}  vwap {b["vwap"]/1e4:.4f}')
else:
    print(f'  trades {d["trades"]:,}  Lee-Ready buys {d["lee_ready_buys"]:,}  sells {d["lee_ready_sells"]:,}  '
          f'agreement with ITCH aggressor {100*d["agreement_with_feed_aggressor"]:.1f}%  effective spread {d["effective_spread_bps_volume_weighted"]:.2f} bps')
