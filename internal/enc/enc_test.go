package enc

import (
	"math"
	"math/rand"
	"testing"
)

func TestTimestampsRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	cases := [][]int64{{}, {42}, {1, 2}, {5, 5, 5, 5}}
	var ts []int64
	cur := int64(34_200_000_000_000)
	for i := 0; i < 100_000; i++ {
		switch rng.Intn(4) {
		case 0:
			cur += 1
		case 1:
			cur += int64(rng.Intn(1000))
		case 2:
			cur += int64(rng.Intn(10_000_000))
		default:
			cur += rng.Int63n(1 << 40)
		}
		ts = append(ts, cur)
	}
	cases = append(cases, ts, []int64{math.MaxInt64, math.MinInt64, 0, -1, 1})
	for _, c := range cases {
		b := EncodeTimestamps(nil, c)
		got, n, err := DecodeTimestamps(nil, b)
		if err != nil || n != len(b) || len(got) != len(c) {
			t.Fatalf("len %d: err=%v n=%d/%d got %d", len(c), err, n, len(b), len(got))
		}
		for i := range c {
			if got[i] != c[i] {
				t.Fatalf("idx %d: %d != %d", i, got[i], c[i])
			}
		}
	}
}

func TestDeltaIntsAndFloatsRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	ints := []int64{0, 1, -1, math.MaxInt64, math.MinInt64}
	p := int64(1_650_000)
	for i := 0; i < 50_000; i++ {
		p += int64(rng.Intn(21) - 10)
		ints = append(ints, p)
	}
	b := EncodeDeltaInts(nil, ints)
	got, n, err := DecodeDeltaInts(nil, b)
	if err != nil || n != len(b) || len(got) != len(ints) {
		t.Fatal(err)
	}
	for i := range ints {
		if got[i] != ints[i] {
			t.Fatalf("int %d", i)
		}
	}
	fl := []float64{0, math.Copysign(0, -1), 1.5, math.Inf(1), math.NaN(), math.SmallestNonzeroFloat64, math.MaxFloat64}
	v := 165.0
	for i := 0; i < 50_000; i++ {
		v += float64(rng.Intn(5)-2) * 0.01
		fl = append(fl, v)
	}
	fb := EncodeFloats(nil, fl)
	gf, n, err := DecodeFloats(nil, fb)
	if err != nil || n != len(fb) || len(gf) != len(fl) {
		t.Fatal(err)
	}
	for i := range fl {
		if math.Float64bits(gf[i]) != math.Float64bits(fl[i]) {
			t.Fatalf("float %d: %v != %v", i, gf[i], fl[i])
		}
	}
}

func FuzzTimestamps(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17})
	f.Fuzz(func(t *testing.T, raw []byte) {
		// Encode arbitrary values derived from the input, decode, compare.
		var ts []int64
		for i := 0; i+8 <= len(raw); i += 8 {
			var v int64
			for k := 0; k < 8; k++ {
				v = v<<8 | int64(raw[i+k])
			}
			ts = append(ts, v)
		}
		got, _, err := DecodeTimestamps(nil, EncodeTimestamps(nil, ts))
		if err != nil || len(got) != len(ts) {
			t.Fatalf("round trip failed: %v", err)
		}
		for i := range ts {
			if got[i] != ts[i] {
				t.Fatalf("mismatch at %d", i)
			}
		}
		// Decoding arbitrary bytes must not panic.
		_, _, _ = DecodeTimestamps(nil, raw)
		_, _, _ = DecodeFloats(nil, raw)
		_, _, _ = DecodeDeltaInts(nil, raw)
	})
}

func BenchmarkEncodeTimestamps(b *testing.B) {
	ts := make([]int64, 8192)
	cur := int64(34_200_000_000_000)
	rng := rand.New(rand.NewSource(1))
	for i := range ts {
		cur += int64(rng.Intn(2_000_000))
		ts[i] = cur
	}
	buf := make([]byte, 0, 1<<16)
	b.SetBytes(int64(len(ts) * 8))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = EncodeTimestamps(buf[:0], ts)
	}
}
