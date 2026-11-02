package store

import (
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func genRows(n int, seed int64) [][]int64 {
	rng := rand.New(rand.NewSource(seed))
	cols := make([][]int64, 4)
	ts := int64(9 * 3600 * 1e9)
	px := int64(1_650_000)
	for i := 0; i < n; i++ {
		ts += int64(rng.Intn(3_000_000))
		px += int64(rng.Intn(5)-2) * 100
		cols[0] = append(cols[0], ts)
		cols[1] = append(cols[1], px)
		cols[2] = append(cols[2], int64(100*(1+rng.Intn(5))))
		cols[3] = append(cols[3], int64(1+rng.Intn(2)))
	}
	return cols
}

func write(t *testing.T, path string, cols [][]int64, zstd bool, block int) *Writer {
	w := NewWriter(path, TradeColumns, block)
	w.Zstd = zstd
	for i := range cols[0] {
		w.Append(cols[0][i], cols[1][i], cols[2][i], cols[3][i])
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return w
}

func TestRoundTripAndRangeScanMatchesBruteForce(t *testing.T) {
	for _, z := range []bool{false, true} {
		dir := t.TempDir()
		cols := genRows(100_000, 1)
		path := filepath.Join(dir, "X.tks")
		write(t, path, cols, z, 1000)
		r, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if r.Rows() != 100_000 {
			t.Fatalf("rows %d", r.Rows())
		}
		if err := r.Verify(); err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(2))
		for q := 0; q < 200; q++ {
			a := cols[0][rng.Intn(len(cols[0]))] - int64(rng.Intn(1e6))
			b := a + int64(rng.Intn(1e9))
			// Brute force.
			lo := sort.Search(len(cols[0]), func(i int) bool { return cols[0][i] >= a })
			hi := sort.Search(len(cols[0]), func(i int) bool { return cols[0][i] >= b })
			var want, got []int64
			want = append(want, cols[1][lo:hi]...)
			_ = r.Scan(a, b, []int{1}, func(bt *Batch) error {
				got = append(got, bt.Cols[0][bt.Lo:bt.Hi]...)
				return nil
			})
			if len(got) != len(want) {
				t.Fatalf("zstd=%v range [%d,%d): got %d rows want %d", z, a, b, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("value mismatch at %d", i)
				}
			}
		}
		_ = r.Close()
	}
}

func TestCorruptionIsDetected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "X.tks")
	write(t, path, genRows(20_000, 3), true, 4096)
	b, _ := os.ReadFile(path)
	b[200] ^= 0x55 // inside the first block's column data
	r, err := FromBytes(b)
	if err != nil {
		return // footer-level rejection is fine too
	}
	if err := r.Verify(); err == nil {
		t.Fatal("corruption not detected")
	}
	if _, err := FromBytes(b[:len(b)-3]); err == nil {
		t.Fatal("truncated file accepted")
	}
}

func TestAtomicPublish(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "X.tks")
	w := NewWriter(path, TradeColumns, 0)
	w.Append(1, 2, 3, 4)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file visible before Close")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file left behind")
	}
}

func FuzzReader(f *testing.F) {
	w := NewWriter(filepath.Join(f.TempDir(), "s.tks"), TradeColumns, 64)
	cols := genRows(300, 4)
	for i := range cols[0] {
		w.Append(cols[0][i], cols[1][i], cols[2][i], cols[3][i])
	}
	f.Add(append([]byte(nil), w.out...))
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := FromBytes(b)
		if err != nil {
			return
		}
		_ = r.Verify()
		_ = r.Scan(-1<<62, 1<<62, []int{0, 1, 2, 3}, func(*Batch) error { return nil })
	})
}

func BenchmarkScanBars(b *testing.B) {
	path := filepath.Join(b.TempDir(), "X.tks")
	cols := genRows(500_000, 5)
	w := NewWriter(path, TradeColumns, 0)
	for i := range cols[0] {
		w.Append(cols[0][i], cols[1][i], cols[2][i], cols[3][i])
	}
	_ = w.Close()
	r, _ := Open(path)
	defer r.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var s int64
		_ = r.Scan(-1<<62, 1<<62, []int{1, 2}, func(bt *Batch) error {
			for k := bt.Lo; k < bt.Hi; k++ {
				s += bt.Cols[0][k] * bt.Cols[1][k]
			}
			return nil
		})
	}
	b.SetBytes(int64(len(cols[0]) * 16))
}
