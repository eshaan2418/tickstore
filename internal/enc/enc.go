// Package enc implements the column encodings used by tickstore:
//
//   - Timestamps: delta-of-delta with Gorilla-style variable-width buckets (Pelkonen et al., VLDB
//     2015). Ticks arrive at irregular but locally similar intervals, so the second difference is
//     usually tiny and costs 1-9 bits.
//   - Integers (prices in fixed point, sizes): delta + zigzag + LEB128 varint. Consecutive trade
//     prices differ by a few ticks, so most deltas fit in one byte.
//   - Float64: Gorilla XOR encoding (leading/trailing zero windows), for analytics columns.
//
// All encoders are append-style over a byte slice and allocation-free once the slice has capacity.
package enc

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

// ErrCorrupt is returned for malformed input.
var ErrCorrupt = errors.New("enc: corrupt input")

// ---- bit stream

// BitWriter appends bits MSB-first.
type BitWriter struct {
	buf  []byte
	cur  uint64
	nbit uint
}

// NewBitWriter starts writing into dst (appending).
func NewBitWriter(dst []byte) *BitWriter { return &BitWriter{buf: dst} }

// WriteBits writes the low n bits of v (n <= 64).
func (w *BitWriter) WriteBits(v uint64, n uint) {
	for n > 0 {
		space := 64 - w.nbit
		take := n
		if take > space {
			take = space
		}
		chunk := (v >> (n - take)) & (1<<take - 1)
		if take == 64 {
			chunk = v
		}
		w.cur |= chunk << (space - take)
		w.nbit += take
		n -= take
		if w.nbit == 64 {
			w.buf = binary.BigEndian.AppendUint64(w.buf, w.cur)
			w.cur, w.nbit = 0, 0
		}
	}
}

// WriteBit writes one bit.
func (w *BitWriter) WriteBit(b bool) {
	if b {
		w.WriteBits(1, 1)
	} else {
		w.WriteBits(0, 1)
	}
}

// Bytes flushes partial bytes and returns the buffer.
func (w *BitWriter) Bytes() []byte {
	for n := w.nbit; n > 0; {
		w.buf = append(w.buf, byte(w.cur>>56))
		w.cur <<= 8
		if n >= 8 {
			n -= 8
		} else {
			n = 0
		}
	}
	w.cur, w.nbit = 0, 0
	return w.buf
}

// BitReader reads bits MSB-first.
type BitReader struct {
	buf []byte
	pos uint // bit position
}

// NewBitReader reads from b.
func NewBitReader(b []byte) *BitReader { return &BitReader{buf: b} }

// ReadBits reads n bits (n <= 64).
func (r *BitReader) ReadBits(n uint) (uint64, error) {
	if r.pos+n > uint(len(r.buf))*8 {
		return 0, ErrCorrupt
	}
	var v uint64
	for n > 0 {
		byteIdx := r.pos / 8
		off := r.pos % 8
		avail := 8 - off
		take := n
		if take > avail {
			take = avail
		}
		b := uint64(r.buf[byteIdx]) >> (avail - take) & (1<<take - 1)
		v = v<<take | b
		r.pos += take
		n -= take
	}
	return v, nil
}

// ReadBit reads one bit.
func (r *BitReader) ReadBit() (bool, error) {
	v, err := r.ReadBits(1)
	return v == 1, err
}

// ---- timestamps: delta-of-delta

// EncodeTimestamps appends an encoding of ts (non-decreasing not required, but compression is
// best for monotone series): first value raw (64 bits), first delta as signed 64, then each
// delta-of-delta D in buckets:
//
//	D == 0             '0'
//	|D| < 2^6          '10'   + 7 bits
//	|D| < 2^13         '110'  + 14 bits
//	|D| < 2^20         '1110' + 21 bits
//	otherwise          '1111' + 64 bits
func EncodeTimestamps(dst []byte, ts []int64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(ts)))
	if len(ts) == 0 {
		return dst
	}
	w := NewBitWriter(dst)
	w.WriteBits(uint64(ts[0]), 64)
	if len(ts) == 1 {
		return w.Bytes()
	}
	prevDelta := ts[1] - ts[0]
	w.WriteBits(uint64(prevDelta), 64)
	for i := 2; i < len(ts); i++ {
		d := ts[i] - ts[i-1]
		dd := d - prevDelta
		prevDelta = d
		switch {
		case dd == 0:
			w.WriteBit(false)
		case dd >= -(1<<6) && dd < 1<<6:
			w.WriteBits(0b10, 2)
			w.WriteBits(uint64(dd)&(1<<7-1), 7)
		case dd >= -(1<<13) && dd < 1<<13:
			w.WriteBits(0b110, 3)
			w.WriteBits(uint64(dd)&(1<<14-1), 14)
		case dd >= -(1<<20) && dd < 1<<20:
			w.WriteBits(0b1110, 4)
			w.WriteBits(uint64(dd)&(1<<21-1), 21)
		default:
			w.WriteBits(0b1111, 4)
			w.WriteBits(uint64(dd), 64)
		}
	}
	return w.Bytes()
}

func signExtend(v uint64, n uint) int64 {
	shift := 64 - n
	return int64(v<<shift) >> shift
}

// DecodeTimestamps appends decoded values to dst; returns bytes consumed.
func DecodeTimestamps(dst []int64, b []byte) ([]int64, int, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > 1<<31 {
		return dst, 0, ErrCorrupt
	}
	if n == 0 {
		return dst, k, nil
	}
	r := NewBitReader(b[k:])
	v0, err := r.ReadBits(64)
	if err != nil {
		return dst, 0, err
	}
	dst = append(dst, int64(v0))
	if n > 1 {
		d, err := r.ReadBits(64)
		if err != nil {
			return dst, 0, err
		}
		delta := int64(d)
		cur := int64(v0) + delta
		dst = append(dst, cur)
		for i := uint64(2); i < n; i++ {
			var dd int64
			bit, err := r.ReadBit()
			if err != nil {
				return dst, 0, err
			}
			if bit {
				var width uint
				ctrl := uint(1)
				for ctrl < 4 {
					b2, err := r.ReadBit()
					if err != nil {
						return dst, 0, err
					}
					if !b2 {
						break
					}
					ctrl++
				}
				switch ctrl {
				case 1:
					width = 7
				case 2:
					width = 14
				case 3:
					width = 21
				default:
					width = 64
				}
				raw, err := r.ReadBits(width)
				if err != nil {
					return dst, 0, err
				}
				if width == 64 {
					dd = int64(raw)
				} else {
					dd = signExtend(raw, width)
				}
			}
			delta += dd
			cur += delta
			dst = append(dst, cur)
		}
	}
	return dst, k + int((r.pos+7)/8), nil
}

// ---- integers: delta + zigzag + varint

func zigzag(v int64) uint64   { return uint64(v<<1) ^ uint64(v>>63) }
func unzigzag(u uint64) int64 { return int64(u>>1) ^ -int64(u&1) }

// EncodeDeltaInts appends delta+zigzag+varint encoding of vs.
func EncodeDeltaInts(dst []byte, vs []int64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(vs)))
	var prev int64
	for _, v := range vs {
		dst = binary.AppendUvarint(dst, zigzag(v-prev))
		prev = v
	}
	return dst
}

// DecodeDeltaInts appends decoded values; returns bytes consumed.
func DecodeDeltaInts(dst []int64, b []byte) ([]int64, int, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > 1<<31 {
		return dst, 0, ErrCorrupt
	}
	off := k
	var prev int64
	for i := uint64(0); i < n; i++ {
		u, m := binary.Uvarint(b[off:])
		if m <= 0 {
			return dst, 0, ErrCorrupt
		}
		off += m
		prev += unzigzag(u)
		dst = append(dst, prev)
	}
	return dst, off, nil
}

// ---- float64: Gorilla XOR

// EncodeFloats appends the Gorilla XOR encoding of vs.
func EncodeFloats(dst []byte, vs []float64) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(vs)))
	if len(vs) == 0 {
		return dst
	}
	w := NewBitWriter(dst)
	prev := math.Float64bits(vs[0])
	w.WriteBits(prev, 64)
	prevLead, prevTrail := uint(64), uint(0)
	for _, f := range vs[1:] {
		cur := math.Float64bits(f)
		x := cur ^ prev
		prev = cur
		if x == 0 {
			w.WriteBit(false)
			continue
		}
		w.WriteBit(true)
		lead := uint(bits.LeadingZeros64(x))
		trail := uint(bits.TrailingZeros64(x))
		if lead > 31 {
			lead = 31
		}
		if prevLead != 64 && lead >= prevLead && trail >= prevTrail {
			// Fits in the previous meaningful-bit window.
			w.WriteBit(false)
			w.WriteBits(x>>prevTrail, 64-prevLead-prevTrail)
			continue
		}
		w.WriteBit(true)
		sig := 64 - lead - trail
		w.WriteBits(uint64(lead), 5)
		w.WriteBits(uint64(sig-1), 6) // sig in [1,64]
		w.WriteBits(x>>trail, sig)
		prevLead, prevTrail = lead, trail
	}
	return w.Bytes()
}

// DecodeFloats appends decoded values; returns bytes consumed.
func DecodeFloats(dst []float64, b []byte) ([]float64, int, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 || n > 1<<31 {
		return dst, 0, ErrCorrupt
	}
	if n == 0 {
		return dst, k, nil
	}
	r := NewBitReader(b[k:])
	v, err := r.ReadBits(64)
	if err != nil {
		return dst, 0, err
	}
	prev := v
	dst = append(dst, math.Float64frombits(prev))
	var lead, trail uint = 64, 0
	for i := uint64(1); i < n; i++ {
		nz, err := r.ReadBit()
		if err != nil {
			return dst, 0, err
		}
		if nz {
			ctrl, err := r.ReadBit()
			if err != nil {
				return dst, 0, err
			}
			if ctrl {
				l, err := r.ReadBits(5)
				if err != nil {
					return dst, 0, err
				}
				s, err := r.ReadBits(6)
				if err != nil {
					return dst, 0, err
				}
				lead = uint(l)
				sig := uint(s) + 1
				if lead+sig > 64 {
					return dst, 0, ErrCorrupt
				}
				trail = 64 - lead - sig
			} else if lead == 64 {
				return dst, 0, ErrCorrupt
			}
			sig := 64 - lead - trail
			m, err := r.ReadBits(sig)
			if err != nil {
				return dst, 0, err
			}
			prev ^= m << trail
		}
		dst = append(dst, math.Float64frombits(prev))
	}
	return dst, k + int((r.pos+7)/8), nil
}
