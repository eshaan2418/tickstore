// Package itch ingests a NASDAQ TotalView-ITCH 5.0 file into tickstore partitions: trades for
// every symbol, and top-of-book quotes for a chosen set of symbols.
//
// Trade prices for E (Order Executed) messages are not in the message: they come from the resting
// order, so ingest keeps an order map (ref -> locate, side, price, shares). Quotes require a book
// per quoted symbol; each side is a sorted slice of price levels (best first).
package itch

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/gzip"

	"github.com/eshaan2418/tickstore/internal/store"
)

type order struct {
	locate uint16
	sell   bool
	price  int64
	shares int64
}

type level struct {
	price int64
	qty   int64
}

type book struct {
	bids, asks       []level // bids descending, asks ascending
	lastBid, lastAsk level
}

func (b *book) side(sell bool) *[]level {
	if sell {
		return &b.asks
	}
	return &b.bids
}

func (b *book) adjust(sell bool, px, d int64) {
	s := b.side(sell)
	v := *s
	i := sort.Search(len(v), func(i int) bool {
		if sell {
			return v[i].price >= px
		}
		return v[i].price <= px
	})
	if i < len(v) && v[i].price == px {
		v[i].qty += d
		if v[i].qty <= 0 {
			*s = append(v[:i], v[i+1:]...)
		}
		return
	}
	if d <= 0 {
		return
	}
	v = append(v, level{})
	copy(v[i+1:], v[i:])
	v[i] = level{px, d}
	*s = v
}

// Stats summarise an ingest run.
type Stats struct {
	Messages  int64         `json:"messages"`
	Trades    int64         `json:"trades"`
	Quotes    int64         `json:"quotes"`
	Symbols   int           `json:"symbols_with_trades"`
	BytesOut  int64         `json:"bytes_written"`
	RawBytes  int64         `json:"raw_row_bytes"` // rows x columns x 8 (uncompressed int64)
	Elapsed   time.Duration `json:"elapsed_ns"`
	ParseOnly bool          `json:"-"`
}

// Ingest reads an ITCH file (gzip or raw) and writes partitions under dir.
// quoteSymbols selects symbols whose BBO is stored; maxMsgs > 0 limits input.
func Ingest(path, dir string, quoteSymbols []string, maxMsgs int64) (Stats, error) {
	t0 := time.Now()
	f, err := os.Open(path)
	if err != nil {
		return Stats{}, err
	}
	defer func() { _ = f.Close() }()
	var r io.Reader = bufio.NewReaderSize(f, 4<<20)
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return Stats{}, err
		}
		defer func() { _ = gz.Close() }()
		r = bufio.NewReaderSize(gz, 4<<20)
	}
	br := r.(*bufio.Reader)
	wantQuote := map[string]bool{}
	for _, s := range quoteSymbols {
		wantQuote[strings.ToUpper(s)] = true
	}
	symbols := map[uint16]string{}
	quoted := map[uint16]*book{}
	orders := make(map[uint64]order, 1<<21)
	trades := map[uint16]*store.Writer{}
	quotes := map[uint16]*store.Writer{}
	var st Stats
	trade := func(loc uint16, ts, px, sz int64, flags int64) {
		w := trades[loc]
		if w == nil {
			sym := symbols[loc]
			if sym == "" {
				sym = fmt.Sprintf("LOC%d", loc)
			}
			w = store.NewWriter(filepath.Join(dir, "trades", sym+".tks"), store.TradeColumns, 0)
			trades[loc] = w
		}
		w.Append(ts, px, sz, flags)
		st.Trades++
	}
	quote := func(loc uint16, ts int64, b *book) {
		var bid, ask level
		if len(b.bids) > 0 {
			bid = b.bids[0]
		}
		if len(b.asks) > 0 {
			ask = b.asks[0]
		}
		if bid == b.lastBid && ask == b.lastAsk {
			return
		}
		b.lastBid, b.lastAsk = bid, ask
		w := quotes[loc]
		if w == nil {
			w = store.NewWriter(filepath.Join(dir, "quotes", symbols[loc]+".tks"), store.QuoteColumns, 0)
			quotes[loc] = w
		}
		w.Append(ts, bid.price, ask.price, bid.qty, ask.qty)
		st.Quotes++
	}
	var hdr [2]byte
	buf := make([]byte, 64)
	be64 := func(b []byte) int64 { return int64(binary.BigEndian.Uint64(b)) }
	be32 := func(b []byte) int64 { return int64(binary.BigEndian.Uint32(b)) }
	for maxMsgs <= 0 || st.Messages < maxMsgs {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return st, err
		}
		n := int(binary.BigEndian.Uint16(hdr[:]))
		if n > len(buf) {
			buf = make([]byte, n)
		}
		m := buf[:n]
		if _, err := io.ReadFull(br, m); err != nil {
			break
		}
		st.Messages++
		if n < 11 {
			continue
		}
		loc := binary.BigEndian.Uint16(m[1:])
		ts := int64(binary.BigEndian.Uint16(m[5:]))<<32 | be32(m[7:])
		switch m[0] {
		case 'R':
			sym := strings.TrimSpace(string(m[11:19]))
			symbols[loc] = sym
			if wantQuote[sym] {
				quoted[loc] = &book{}
			}
		case 'A', 'F':
			ref := uint64(be64(m[11:]))
			o := order{locate: loc, sell: m[19] == 'S', shares: be32(m[20:]), price: be32(m[32:])}
			orders[ref] = o
			if b := quoted[loc]; b != nil {
				b.adjust(o.sell, o.price, o.shares)
				quote(loc, ts, b)
			}
		case 'E', 'C', 'X', 'D':
			ref := uint64(be64(m[11:]))
			o, ok := orders[ref]
			if !ok {
				continue
			}
			var q int64
			switch m[0] {
			case 'D':
				q = o.shares
			default:
				q = be32(m[19:])
			}
			if m[0] == 'E' || m[0] == 'C' {
				px := o.price
				printable := true
				if m[0] == 'C' {
					px = be32(m[32:])
					printable = m[31] == 'Y'
				}
				if printable {
					flags := int64(store.FlagBuyAggressor)
					if !o.sell {
						flags = store.FlagSellAggressor // resting buy executed => seller aggressed
					}
					trade(loc, ts, px, q, flags)
				}
			}
			if q > o.shares {
				q = o.shares
			}
			o.shares -= q
			if o.shares == 0 {
				delete(orders, ref)
			} else {
				orders[ref] = o
			}
			if b := quoted[loc]; b != nil {
				b.adjust(o.sell, o.price, -q)
				quote(loc, ts, b)
			}
		case 'U':
			old := uint64(be64(m[11:]))
			o, ok := orders[old]
			if !ok {
				continue
			}
			delete(orders, old)
			no := order{locate: o.locate, sell: o.sell, shares: be32(m[27:]), price: be32(m[31:])}
			orders[uint64(be64(m[19:]))] = no
			if b := quoted[loc]; b != nil {
				b.adjust(o.sell, o.price, -o.shares)
				b.adjust(no.sell, no.price, no.shares)
				quote(loc, ts, b)
			}
		case 'P': // non-displayed execution; side is the resting (hidden) order's side
			flags := int64(store.FlagNonDisplayed | store.FlagBuyAggressor)
			if m[19] == 'B' {
				flags = store.FlagNonDisplayed | store.FlagSellAggressor
			}
			trade(loc, ts, be32(m[32:]), be32(m[20:]), flags)
		case 'Q': // cross (auction) trade
			if q := be64(m[11:]); q > 0 {
				trade(loc, ts, be32(m[27:]), q, store.FlagCross)
			}
		}
	}
	for _, w := range trades {
		st.RawBytes += w.Rows * int64(len(store.TradeColumns)) * 8
		if err := w.Close(); err != nil {
			return st, err
		}
		st.BytesOut += int64(w.Size())
	}
	for _, w := range quotes {
		st.RawBytes += w.Rows * int64(len(store.QuoteColumns)) * 8
		if err := w.Close(); err != nil {
			return st, err
		}
		st.BytesOut += int64(w.Size())
	}
	st.Symbols = len(trades)
	st.Elapsed = time.Since(t0)
	return st, nil
}
