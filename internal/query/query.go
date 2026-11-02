// Package query implements analytics over tickstore partitions: OHLCV/VWAP bars, as-of joins of
// trades to the prevailing quote (with Lee-Ready trade classification and effective spread), and
// cross-sectional VWAP.
package query

import (
	"math"

	"github.com/eshaan2418/tickstore/internal/store"
)

// Bar is one OHLCV interval. Prices in ITCH fixed point (1e-4 $).
type Bar struct {
	Start  int64   `json:"start_ns"`
	Open   int64   `json:"open"`
	High   int64   `json:"high"`
	Low    int64   `json:"low"`
	Close  int64   `json:"close"`
	Volume int64   `json:"volume"`
	Trades int64   `json:"trades"`
	VWAP   float64 `json:"vwap"`
}

// Bars resamples trades in [t0, t1) into fixed intervals. Cross (auction) prints are excluded,
// as is conventional for intraday bars.
func Bars(r *store.Reader, t0, t1, interval int64) ([]Bar, error) {
	ts, px, sz, fl := r.Col("ts"), r.Col("price"), r.Col("size"), r.Col("flags")
	var out []Bar
	var cur *Bar
	var notional float64
	err := r.Scan(t0, t1, []int{ts, px, sz, fl}, func(b *store.Batch) error {
		T, P, S, F := b.Cols[0], b.Cols[1], b.Cols[2], b.Cols[3]
		for i := b.Lo; i < b.Hi; i++ {
			if F[i]&store.FlagCross != 0 {
				continue
			}
			start := t0 + (T[i]-t0)/interval*interval
			if cur == nil || cur.Start != start {
				if cur != nil {
					cur.VWAP = notional / float64(cur.Volume)
				}
				out = append(out, Bar{Start: start, Open: P[i], High: P[i], Low: P[i]})
				cur = &out[len(out)-1]
				notional = 0
			}
			if P[i] > cur.High {
				cur.High = P[i]
			}
			if P[i] < cur.Low {
				cur.Low = P[i]
			}
			cur.Close = P[i]
			cur.Volume += S[i]
			cur.Trades++
			notional += float64(P[i]) * float64(S[i])
		}
		return nil
	})
	if cur != nil && cur.Volume > 0 {
		cur.VWAP = notional / float64(cur.Volume)
	}
	return out, err
}

// AsOfStats summarise an as-of join of trades with the prevailing quote.
type AsOfStats struct {
	Trades          int64   `json:"trades"`
	Matched         int64   `json:"matched_with_quote"`
	LeeReadyBuys    int64   `json:"lee_ready_buys"`
	LeeReadySells   int64   `json:"lee_ready_sells"`
	AgreeWithFeed   float64 `json:"agreement_with_feed_aggressor"` // classification accuracy vs ITCH truth
	EffSpreadBps    float64 `json:"effective_spread_bps_volume_weighted"`
	QuotedSpreadBps float64 `json:"quoted_spread_bps_trade_weighted"`
	Checksum        float64 `json:"checksum"` // sum of matched mids (cross-engine comparison)
}

// AsOf joins every trade in [t0, t1) with the last quote at or before it (merge join: both inputs
// are time-ordered), classifies it with Lee-Ready (quote rule, tick test at the midpoint), and
// compares the classification with the aggressor flag recorded from the ITCH feed.
func AsOf(trades, quotes *store.Reader, t0, t1 int64) (AsOfStats, error) {
	var st AsOfStats
	// Materialise quotes in range (plus the one before t0) for the merge.
	var qt, qb, qa []int64
	if err := quotes.Scan(math.MinInt64, t1, []int{0, quotes.Col("bid"), quotes.Col("ask")}, func(b *store.Batch) error {
		qt = append(qt, b.Cols[0][b.Lo:b.Hi]...)
		qb = append(qb, b.Cols[1][b.Lo:b.Hi]...)
		qa = append(qa, b.Cols[2][b.Lo:b.Hi]...)
		return nil
	}); err != nil {
		return st, err
	}
	j := -1
	var lastPx, lastDir int64
	var agree, classified int64
	var effNum, effDen, qsNum float64
	err := trades.Scan(t0, t1, []int{0, trades.Col("price"), trades.Col("size"), trades.Col("flags")}, func(b *store.Batch) error {
		T, P, S, F := b.Cols[0], b.Cols[1], b.Cols[2], b.Cols[3]
		for i := b.Lo; i < b.Hi; i++ {
			if F[i]&store.FlagCross != 0 {
				continue
			}
			st.Trades++
			for j+1 < len(qt) && qt[j+1] <= T[i] {
				j++
			}
			dir := int64(0)
			if j >= 0 && qb[j] > 0 && qa[j] > qb[j] {
				st.Matched++
				mid := float64(qb[j]+qa[j]) / 2
				st.Checksum += mid
				switch {
				case float64(P[i]) > mid:
					dir = 1
				case float64(P[i]) < mid:
					dir = -1
				}
				effNum += 2 * math.Abs(float64(P[i])-mid) / mid * 1e4 * float64(S[i])
				effDen += float64(S[i])
				qsNum += float64(qa[j]-qb[j]) / mid * 1e4
			}
			if dir == 0 { // tick test
				switch {
				case P[i] > lastPx && lastPx > 0:
					dir = 1
				case P[i] < lastPx:
					dir = -1
				default:
					dir = lastDir
				}
			}
			lastPx, lastDir = P[i], dir
			if dir > 0 {
				st.LeeReadyBuys++
			} else if dir < 0 {
				st.LeeReadySells++
			}
			truth := int64(0)
			if F[i]&store.FlagBuyAggressor != 0 {
				truth = 1
			} else if F[i]&store.FlagSellAggressor != 0 {
				truth = -1
			}
			if truth != 0 && dir != 0 {
				classified++
				if truth == dir {
					agree++
				}
			}
		}
		return nil
	})
	if classified > 0 {
		st.AgreeWithFeed = float64(agree) / float64(classified)
	}
	if effDen > 0 {
		st.EffSpreadBps = effNum / effDen
	}
	if st.Matched > 0 {
		st.QuotedSpreadBps = qsNum / float64(st.Matched)
	}
	return st, err
}

// VWAP returns the volume-weighted average price and volume of trades in [t0, t1) (excluding
// crosses).
func VWAP(r *store.Reader, t0, t1 int64) (vwap float64, volume int64, trades int64, err error) {
	var notional float64
	err = r.Scan(t0, t1, []int{r.Col("price"), r.Col("size"), r.Col("flags")}, func(b *store.Batch) error {
		P, S, F := b.Cols[0], b.Cols[1], b.Cols[2]
		for i := b.Lo; i < b.Hi; i++ {
			if F[i]&store.FlagCross != 0 {
				continue
			}
			notional += float64(P[i]) * float64(S[i])
			volume += S[i]
			trades++
		}
		return nil
	})
	if volume > 0 {
		vwap = notional / float64(volume)
	}
	return vwap, volume, trades, err
}
