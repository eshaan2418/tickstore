package query

import (
	"math"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/eshaan2418/tickstore/internal/store"
)

func TestBarsAndAsOfMatchBruteForce(t *testing.T) {
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(7))
	tw := store.NewWriter(filepath.Join(dir, "t.tks"), store.TradeColumns, 512)
	qw := store.NewWriter(filepath.Join(dir, "q.tks"), store.QuoteColumns, 512)
	type tr struct{ ts, px, sz, fl int64 }
	type qu struct{ ts, b, a int64 }
	var trs []tr
	var qus []qu
	ts := int64(10 * 3600 * 1e9)
	mid := int64(1_000_000)
	for i := 0; i < 20000; i++ {
		ts += int64(1 + rng.Intn(50_000_000))
		if rng.Intn(2) == 0 {
			mid += int64(rng.Intn(3)-1) * 100
			q := qu{ts, mid - 100, mid + 100}
			qus = append(qus, q)
			qw.Append(q.ts, q.b, q.a, 100, 100)
		} else {
			px := mid + 100
			if rng.Intn(2) == 0 {
				px = mid - 100 // off-mid trades: the quote rule decides, so agreement must be exact
			}
			fl := int64(store.FlagBuyAggressor)
			if px < mid {
				fl = store.FlagSellAggressor
			}
			x := tr{ts, px, int64(100 * (1 + rng.Intn(3))), fl}
			trs = append(trs, x)
			tw.Append(x.ts, x.px, x.sz, x.fl)
		}
	}
	_ = tw.Close()
	_ = qw.Close()
	tr0, _ := store.Open(filepath.Join(dir, "t.tks"))
	q0, _ := store.Open(filepath.Join(dir, "q.tks"))
	t0, t1, iv := int64(10*3600*1e9), int64(16*3600*1e9), int64(60e9)
	bars, err := Bars(tr0, t0, t1, iv)
	if err != nil {
		t.Fatal(err)
	}
	// Brute-force bars.
	type agg struct {
		o, h, l, c, v, n int64
		notional         float64
	}
	want := map[int64]*agg{}
	var order []int64
	for _, x := range trs {
		if x.ts < t0 || x.ts >= t1 {
			continue
		}
		k := t0 + (x.ts-t0)/iv*iv
		a, ok := want[k]
		if !ok {
			a = &agg{o: x.px, h: x.px, l: x.px}
			want[k] = a
			order = append(order, k)
		}
		a.h = max(a.h, x.px)
		a.l = min(a.l, x.px)
		a.c = x.px
		a.v += x.sz
		a.n++
		a.notional += float64(x.px * x.sz)
	}
	if len(bars) != len(order) {
		t.Fatalf("bars %d want %d", len(bars), len(order))
	}
	for i, b := range bars {
		a := want[order[i]]
		if b.Start != order[i] || b.Open != a.o || b.High != a.h || b.Low != a.l || b.Close != a.c || b.Volume != a.v || b.Trades != a.n ||
			math.Abs(b.VWAP-a.notional/float64(a.v)) > 1e-6 {
			t.Fatalf("bar %d: %+v vs %+v", i, b, *a)
		}
	}
	// Brute-force as-of.
	st, err := AsOf(tr0, q0, t0, t1)
	if err != nil {
		t.Fatal(err)
	}
	var matched int64
	var check float64
	j := -1
	for _, x := range trs {
		for j+1 < len(qus) && qus[j+1].ts <= x.ts {
			j++
		}
		if j >= 0 {
			matched++
			check += float64(qus[j].b+qus[j].a) / 2
		}
	}
	if st.Trades != int64(len(trs)) || st.Matched != matched || math.Abs(st.Checksum-check) > 1e-3 {
		t.Fatalf("asof %+v want matched %d checksum %f", st, matched, check)
	}
	if st.AgreeWithFeed < 0.99 { // flags were generated with the quote rule, so agreement is exact
		t.Fatalf("agreement %f", st.AgreeWithFeed)
	}
}
