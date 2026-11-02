// Command tickstore ingests ITCH into columnar partitions and runs analytics on them.
//
//	tickstore ingest  -itch FILE.gz -dir DB [-quote AAPL,MSFT,...] [-max N]
//	tickstore bars    -dir DB -symbol AAPL [-interval 1m] [-from 09:30 -to 16:00]
//	tickstore asof    -dir DB -symbol AAPL
//	tickstore vwap    -dir DB            (every symbol, regular hours)
//	tickstore export  -dir DB -out CSVDIR (CSV for the Parquet/DuckDB baseline)
//	tickstore bench   -dir DB -symbol AAPL -runs 5 -out results.json
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/eshaan2418/tickstore/internal/itch"
	"github.com/eshaan2418/tickstore/internal/query"
	"github.com/eshaan2418/tickstore/internal/store"
)

func hm(s string) int64 {
	p := strings.Split(s, ":")
	h, _ := strconv.Atoi(p[0])
	m := 0
	if len(p) > 1 {
		m, _ = strconv.Atoi(p[1])
	}
	return (int64(h)*3600 + int64(m)*60) * 1e9
}

func open(dir, table, sym string) *store.Reader {
	r, err := store.Open(filepath.Join(dir, table, sym+".tks"))
	if err != nil {
		fail(err)
	}
	return r
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "tickstore:", err)
	os.Exit(1)
}

func printJSON(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

// DefaultQuoteSymbols are liquid Nasdaq names whose top of book is stored.
var DefaultQuoteSymbols = "AAPL,MSFT,AMZN,GOOGL,GOOG,FB,INTC,CSCO,NFLX,NVDA,QQQ,TQQQ,SQQQ,AMD,MU,TSLA,CMCSA,PEP,ADBE,PYPL"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: tickstore {ingest|bars|asof|vwap|export|bench} ...")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	dir := fs.String("dir", "db", "database directory")
	itchPath := fs.String("itch", "", "ITCH file (ingest)")
	quote := fs.String("quote", DefaultQuoteSymbols, "symbols whose quotes are stored (ingest)")
	maxMsgs := fs.Int64("max", 0, "limit messages (ingest)")
	sym := fs.String("symbol", "AAPL", "symbol")
	interval := fs.Duration("interval", time.Minute, "bar interval")
	from := fs.String("from", "09:30", "start (HH:MM)")
	to := fs.String("to", "16:00", "end (HH:MM)")
	out := fs.String("out", "", "output path")
	runs := fs.Int("runs", 5, "benchmark repetitions")
	cpuprof := fs.String("cpuprofile", "", "write CPU profile")
	_ = fs.Parse(os.Args[2:])
	t0, t1 := hm(*from), hm(*to)
	if *cpuprof != "" {
		f, err := os.Create(*cpuprof)
		if err != nil {
			fail(err)
		}
		_ = pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	switch cmd {
	case "ingest":
		st, err := itch.Ingest(*itchPath, *dir, strings.Split(*quote, ","), *maxMsgs)
		if err != nil {
			fail(err)
		}
		printJSON(map[string]any{"stats": st, "seconds": st.Elapsed.Seconds(),
			"messages_per_sec":               float64(st.Messages) / st.Elapsed.Seconds(),
			"compression_ratio_vs_raw_int64": float64(st.RawBytes) / float64(st.BytesOut)})
	case "bars":
		r := open(*dir, "trades", *sym)
		bars, err := query.Bars(r, t0, t1, interval.Nanoseconds())
		if err != nil {
			fail(err)
		}
		printJSON(bars)
	case "asof":
		st, err := query.AsOf(open(*dir, "trades", *sym), open(*dir, "quotes", *sym), t0, t1)
		if err != nil {
			fail(err)
		}
		printJSON(st)
	case "vwap":
		files, _ := filepath.Glob(filepath.Join(*dir, "trades", "*.tks"))
		type row struct {
			Symbol string
			VWAP   float64
			Volume int64
		}
		var rows []row
		for _, f := range files {
			r, err := store.Open(f)
			if err != nil {
				fail(err)
			}
			v, vol, _, err := query.VWAP(r, t0, t1)
			_ = r.Close()
			if err != nil {
				fail(err)
			}
			if vol > 0 {
				rows = append(rows, row{strings.TrimSuffix(filepath.Base(f), ".tks"), v / 1e4, vol})
			}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Volume > rows[j].Volume })
		if len(rows) > 20 {
			rows = rows[:20]
		}
		printJSON(rows)
	case "export":
		if err := export(*dir, *out); err != nil {
			fail(err)
		}
	case "pack":
		for _, t := range []string{"trades", "quotes"} {
			n, err := store.WritePack(filepath.Join(*dir, t), filepath.Join(*dir, t+".pack"))
			if err != nil {
				fail(err)
			}
			fmt.Printf("%s.pack: %d partitions\n", t, n)
		}
	case "bench-write":
		res := benchWrite(*dir, *runs)
		b, _ := json.MarshalIndent(res, "", "  ")
		if *out != "" {
			_ = os.WriteFile(*out, b, 0o644)
		}
		fmt.Println(string(b))
	case "bench":
		res := bench(*dir, *sym, *runs, t0, t1)
		b, _ := json.MarshalIndent(res, "", "  ")
		if *out != "" {
			_ = os.WriteFile(*out, b, 0o644)
		}
		fmt.Println(string(b))
	default:
		fail(fmt.Errorf("unknown command %q", cmd))
	}
}

// export writes trades.csv (all symbols) and quotes.csv (quoted symbols) with a symbol column.
func export(dir, out string) error {
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	for _, table := range []string{"trades", "quotes"} {
		f, err := os.Create(filepath.Join(out, table+".csv"))
		if err != nil {
			return err
		}
		w := bufio.NewWriterSize(f, 1<<20)
		cols := store.TradeColumns
		if table == "quotes" {
			cols = store.QuoteColumns
		}
		fmt.Fprintf(w, "symbol,%s\n", strings.Join(cols, ","))
		files, _ := filepath.Glob(filepath.Join(dir, table, "*.tks"))
		sort.Strings(files)
		var line []byte
		for _, p := range files {
			sym := strings.TrimSuffix(filepath.Base(p), ".tks")
			r, err := store.Open(p)
			if err != nil {
				return err
			}
			proj := make([]int, len(cols))
			for i := range proj {
				proj[i] = i
			}
			err = r.Scan(-1<<62, 1<<62, proj, func(b *store.Batch) error {
				for i := b.Lo; i < b.Hi; i++ {
					line = append(line[:0], sym...)
					for c := range proj {
						line = append(line, ',')
						line = strconv.AppendInt(line, b.Cols[c][i], 10)
					}
					line = append(line, '\n')
					if _, err := w.Write(line); err != nil {
						return err
					}
				}
				return nil
			})
			_ = r.Close()
			if err != nil {
				return err
			}
		}
		if err := w.Flush(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

type timing struct {
	Query  string    `json:"query"`
	RunsMs []float64 `json:"runs_ms"`
	Median float64   `json:"median_ms"`
	Result any       `json:"result"`
}

func timeit(name string, runs int, f func() any) timing {
	t := timing{Query: name}
	for i := 0; i < runs; i++ {
		s := time.Now()
		t.Result = f()
		t.RunsMs = append(t.RunsMs, float64(time.Since(s).Microseconds())/1e3)
	}
	c := append([]float64(nil), t.RunsMs...)
	sort.Float64s(c)
	t.Median = c[len(c)/2]
	return t
}

func bench(dir, sym string, runs int, t0, t1 int64) map[string]any {
	var res []timing
	// 1. Range count: one symbol, one hour (10:00-11:00), timestamps only.
	res = append(res, timeit("range_count_1h", runs, func() any {
		r := open(dir, "trades", sym)
		defer r.Close()
		var n int
		_ = r.Scan(hm("10:00"), hm("11:00"), []int{0}, func(b *store.Batch) error { n += b.Hi - b.Lo; return nil })
		return n
	}))
	// 2. 1-minute OHLCV + VWAP bars for the regular session.
	res = append(res, timeit("bars_1min", runs, func() any {
		r := open(dir, "trades", sym)
		defer r.Close()
		b, _ := query.Bars(r, t0, t1, int64(time.Minute))
		var vol int64
		for _, x := range b {
			vol += x.Volume
		}
		return map[string]any{"bars": len(b), "volume": vol}
	}))
	// 3. As-of join of trades to quotes (Lee-Ready).
	res = append(res, timeit("asof_join", runs, func() any {
		tr, q := open(dir, "trades", sym), open(dir, "quotes", sym)
		defer tr.Close()
		defer q.Close()
		st, _ := query.AsOf(tr, q, t0, t1)
		return st
	}))
	// 4. Whole-market VWAP per symbol for the regular session (sequential, then parallel).
	market := func(workers int) any {
		files, _ := filepath.Glob(filepath.Join(dir, "trades", "*.tks"))
		type part struct {
			n   int
			vol int64
		}
		jobs := make(chan string, len(files))
		for _, f := range files {
			jobs <- f
		}
		close(jobs)
		outs := make(chan part, workers)
		for w := 0; w < workers; w++ {
			go func() {
				var p part
				for f := range jobs {
					r, err := store.Open(f)
					if err != nil {
						continue
					}
					_, v, _, _ := query.VWAP(r, t0, t1)
					_ = r.Close()
					if v > 0 {
						p.n++
						p.vol += v
					}
				}
				outs <- p
			}()
		}
		var tot part
		for w := 0; w < workers; w++ {
			p := <-outs
			tot.n += p.n
			tot.vol += p.vol
		}
		return map[string]any{"symbols": tot.n, "volume": tot.vol}
	}
	res = append(res, timeit("market_vwap_all_symbols", runs, func() any { return market(1) }))
	res = append(res, timeit("market_vwap_all_symbols_8_workers", runs, func() any { return market(8) }))
	// The same query over the pack file (one mmap; partitions parsed in place).
	if _, err := os.Stat(filepath.Join(dir, "trades.pack")); err == nil {
		packed := func(workers int) any {
			p, err := store.OpenPack(filepath.Join(dir, "trades.pack"))
			if err != nil {
				fail(err)
			}
			defer p.Close()
			type part struct {
				n   int
				vol int64
			}
			outs := make(chan part, workers)
			for w := 0; w < workers; w++ {
				go func(w int) {
					var pt part
					for i := w; i < len(p.Names); i += workers {
						r, err := p.Reader(i)
						if err != nil {
							continue
						}
						if _, v, _, _ := query.VWAP(r, t0, t1); v > 0 {
							pt.n++
							pt.vol += v
						}
					}
					outs <- pt
				}(w)
			}
			var tot part
			for w := 0; w < workers; w++ {
				x := <-outs
				tot.n += x.n
				tot.vol += x.vol
			}
			return map[string]any{"symbols": tot.n, "volume": tot.vol}
		}
		res = append(res, timeit("market_vwap_all_symbols_pack", runs, func() any { return packed(1) }))
		res = append(res, timeit("market_vwap_all_symbols_pack_8_workers", runs, func() any { return packed(8) }))
	}
	var size int64
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		// Partitions only; .pack files duplicate them for cross-sectional access.
		if err == nil && !info.IsDir() && strings.HasSuffix(p, ".tks") {
			size += info.Size()
		}
		return nil
	})
	var packBytes int64
	for _, t := range []string{"trades", "quotes"} {
		if st, err := os.Stat(filepath.Join(dir, t+".pack")); err == nil {
			packBytes += st.Size()
		}
	}
	return map[string]any{"symbol": sym, "db_bytes": size, "pack_bytes": packBytes, "queries": res}
}

// benchWrite loads every trade into memory, then times encoding + writing all partitions (the same
// work pyarrow's write_table does for Parquet), with and without the zstd stage.
func benchWrite(dir string, runs int) map[string]any {
	type part struct {
		sym  string
		cols [][]int64
	}
	files, _ := filepath.Glob(filepath.Join(dir, "trades", "*.tks"))
	var parts []part
	var rows int64
	for _, f := range files {
		r, err := store.Open(f)
		if err != nil {
			fail(err)
		}
		p := part{sym: strings.TrimSuffix(filepath.Base(f), ".tks"), cols: make([][]int64, 4)}
		_ = r.Scan(-1<<62, 1<<62, []int{0, 1, 2, 3}, func(b *store.Batch) error {
			for c := 0; c < 4; c++ {
				p.cols[c] = append(p.cols[c], b.Cols[c][b.Lo:b.Hi]...)
			}
			return nil
		})
		rows += int64(len(p.cols[0]))
		_ = r.Close()
		parts = append(parts, p)
	}
	out := map[string]any{"rows": rows, "partitions": len(parts)}
	for _, mode := range []string{"raw_nosync", "zstd_nosync", "zstd_fsync"} {
		z := mode != "raw_nosync"
		tmp, _ := os.MkdirTemp("", "tickstore-write-")
		var secs []float64
		var bytes int64
		for i := 0; i < runs; i++ {
			s := time.Now()
			bytes = 0
			for _, p := range parts {
				w := store.NewWriter(filepath.Join(tmp, p.sym+".tks"), store.TradeColumns, 0)
				w.Zstd = z
				w.NoSync = mode != "zstd_fsync"
				for k := range p.cols[0] {
					w.Append(p.cols[0][k], p.cols[1][k], p.cols[2][k], p.cols[3][k])
				}
				if err := w.Close(); err != nil {
					fail(err)
				}
				bytes += int64(w.Size())
			}
			secs = append(secs, time.Since(s).Seconds())
		}
		_ = os.RemoveAll(tmp)
		sort.Float64s(secs)
		out[mode] = map[string]any{"median_s": secs[len(secs)/2], "rows_per_sec": float64(rows) / secs[len(secs)/2], "bytes": bytes}
	}
	return out
}
