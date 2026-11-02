// Package store implements tickstore's on-disk columnar format.
//
// One file per (table, symbol, day) partition. A file is a sequence of blocks followed by a
// footer index:
//
//	header: "TKS1" | u16 ncols | (u16 len, name)*
//	block:  u32 rows | i64 tmin | i64 tmax | (u32 len, encoded column)* | u32 CRC32-C(columns)
//	footer: (u64 offset, u32 rows, i64 tmin, i64 tmax)* | u32 nblocks | u64 footerOffset | "TKSF"
//
// Column 0 is the timestamp (delta-of-delta); the others are delta+zigzag varints. Readers mmap
// the file, binary-search the footer for blocks overlapping a time range and decode only the
// columns a query asks for (projection pushdown). Writers write to a temporary file, fsync and
// rename, so a crash never leaves a half-written partition visible.
package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/klauspost/compress/zstd"

	"github.com/eshaan2418/tickstore/internal/enc"
)

// Column chunk codecs (first byte of every encoded column chunk).
const (
	codecRaw  = 0 // the type-specific encoding only
	codecZstd = 1 // type-specific encoding, then zstd
)

var (
	zenc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	zdec, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
)

// DefaultBlockRows is the block size (rows).
const DefaultBlockRows = 8192

// mmapThreshold: files smaller than this are read into memory instead of mapped.
const mmapThreshold = 256 << 10

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	// ErrFormat reports a malformed or truncated file.
	ErrFormat = errors.New("tickstore: bad file format")
)

// Schemas.
var (
	TradeColumns = []string{"ts", "price", "size", "flags"}
	QuoteColumns = []string{"ts", "bid", "ask", "bid_size", "ask_size"}
)

// Trade flags.
const (
	FlagBuyAggressor  = 1
	FlagSellAggressor = 2
	FlagCross         = 4
	FlagNonDisplayed  = 8
)

// Writer buffers rows and writes a partition on Close.
type Writer struct {
	path      string
	cols      []string
	blockRows int
	pending   [][]int64 // per column
	out       []byte    // encoded file so far
	index     []blockMeta
	scratch   []byte
	zbuf      []byte
	Rows      int64
	// Zstd applies a zstd stage on top of each column's encoding when it makes the chunk smaller.
	Zstd bool
	// NoSync skips the fsync before rename (benchmarks; the default is durable).
	NoSync bool
}

type blockMeta struct {
	off        uint64
	rows       uint32
	tmin, tmax int64
}

// NewWriter creates a writer for path with the given columns (column 0 must be the timestamp).
func NewWriter(path string, cols []string, blockRows int) *Writer {
	if blockRows <= 0 {
		blockRows = DefaultBlockRows
	}
	w := &Writer{path: path, cols: cols, blockRows: blockRows, pending: make([][]int64, len(cols)), Zstd: true}
	w.out = append(w.out, "TKS1"...)
	w.out = binary.LittleEndian.AppendUint16(w.out, uint16(len(cols)))
	for _, c := range cols {
		w.out = binary.LittleEndian.AppendUint16(w.out, uint16(len(c)))
		w.out = append(w.out, c...)
	}
	return w
}

// Append adds one row (len(row) == number of columns). Timestamps must be non-decreasing.
func (w *Writer) Append(row ...int64) {
	for i, v := range row {
		w.pending[i] = append(w.pending[i], v)
	}
	w.Rows++
	if len(w.pending[0]) >= w.blockRows {
		w.flushBlock()
	}
}

func (w *Writer) flushBlock() {
	n := len(w.pending[0])
	if n == 0 {
		return
	}
	ts := w.pending[0]
	meta := blockMeta{off: uint64(len(w.out)), rows: uint32(n), tmin: ts[0], tmax: ts[n-1]}
	w.out = binary.LittleEndian.AppendUint32(w.out, uint32(n))
	w.out = binary.LittleEndian.AppendUint64(w.out, uint64(meta.tmin))
	w.out = binary.LittleEndian.AppendUint64(w.out, uint64(meta.tmax))
	colStart := len(w.out)
	for i, col := range w.pending {
		if i == 0 {
			w.scratch = enc.EncodeTimestamps(w.scratch[:0], col)
		} else {
			w.scratch = enc.EncodeDeltaInts(w.scratch[:0], col)
		}
		chunk, codec := w.scratch, byte(codecRaw)
		if w.Zstd {
			w.zbuf = zenc.EncodeAll(w.scratch, w.zbuf[:0])
			if len(w.zbuf) < len(w.scratch) {
				chunk, codec = w.zbuf, codecZstd
			}
		}
		w.out = binary.LittleEndian.AppendUint32(w.out, uint32(len(chunk)+1))
		w.out = append(w.out, codec)
		w.out = append(w.out, chunk...)
		w.pending[i] = w.pending[i][:0]
	}
	w.out = binary.LittleEndian.AppendUint32(w.out, crc32.Checksum(w.out[colStart:], castagnoli))
	w.index = append(w.index, meta)
}

// Close writes the footer and atomically publishes the file.
func (w *Writer) Close() error {
	w.flushBlock()
	footer := uint64(len(w.out))
	for _, m := range w.index {
		w.out = binary.LittleEndian.AppendUint64(w.out, m.off)
		w.out = binary.LittleEndian.AppendUint32(w.out, m.rows)
		w.out = binary.LittleEndian.AppendUint64(w.out, uint64(m.tmin))
		w.out = binary.LittleEndian.AppendUint64(w.out, uint64(m.tmax))
	}
	w.out = binary.LittleEndian.AppendUint32(w.out, uint32(len(w.index)))
	w.out = binary.LittleEndian.AppendUint64(w.out, footer)
	w.out = append(w.out, "TKSF"...)
	if err := os.MkdirAll(filepath.Dir(w.path), 0o755); err != nil {
		return err
	}
	tmp := w.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(w.out); err != nil {
		_ = f.Close()
		return err
	}
	if !w.NoSync {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, w.path)
}

// Size returns the encoded size so far (bytes).
func (w *Writer) Size() int { return len(w.out) }

// Reader reads one partition (memory-mapped).
type Reader struct {
	data  []byte
	cols  []string
	index []blockMeta
	mmap  bool
}

// Open maps a partition file and parses its footer.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < 24 {
		return nil, ErrFormat
	}
	if st.Size() < mmapThreshold {
		// Small partitions (most symbols) are cheaper to read than to map: one read syscall instead
		// of mmap + munmap, which dominate cross-sectional scans over thousands of files.
		data := make([]byte, st.Size())
		if _, err := io.ReadFull(f, data); err != nil {
			return nil, err
		}
		r := &Reader{data: data}
		return r, r.parse()
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(st.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	r := &Reader{data: data, mmap: true}
	if err := r.parse(); err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}

// FromBytes parses an in-memory partition (tests, fuzzing).
func FromBytes(b []byte) (*Reader, error) {
	r := &Reader{data: b}
	return r, r.parse()
}

func (r *Reader) parse() error {
	d := r.data
	if len(d) < 24 || string(d[:4]) != "TKS1" || string(d[len(d)-4:]) != "TKSF" {
		return ErrFormat
	}
	nc := int(binary.LittleEndian.Uint16(d[4:]))
	off := 6
	for i := 0; i < nc; i++ {
		if off+2 > len(d) {
			return ErrFormat
		}
		l := int(binary.LittleEndian.Uint16(d[off:]))
		if off+2+l > len(d) {
			return ErrFormat
		}
		r.cols = append(r.cols, string(d[off+2:off+2+l]))
		off += 2 + l
	}
	tail := len(d) - 4
	foot := binary.LittleEndian.Uint64(d[tail-8:])
	nb := int(binary.LittleEndian.Uint32(d[tail-12:]))
	if foot > uint64(tail-12) || uint64(tail-12)-foot != uint64(nb)*28 {
		return ErrFormat
	}
	for i := 0; i < nb; i++ {
		p := int(foot) + i*28
		m := blockMeta{off: binary.LittleEndian.Uint64(d[p:]), rows: binary.LittleEndian.Uint32(d[p+8:]),
			tmin: int64(binary.LittleEndian.Uint64(d[p+12:])), tmax: int64(binary.LittleEndian.Uint64(d[p+20:]))}
		if m.off >= foot {
			return ErrFormat
		}
		r.index = append(r.index, m)
	}
	return nil
}

// Close unmaps the file.
func (r *Reader) Close() error {
	if r.mmap && r.data != nil {
		err := syscall.Munmap(r.data)
		r.data = nil
		return err
	}
	return nil
}

// Columns returns the schema.
func (r *Reader) Columns() []string { return r.cols }

// Rows returns the total row count.
func (r *Reader) Rows() int64 {
	var n int64
	for _, m := range r.index {
		n += int64(m.rows)
	}
	return n
}

// Bytes returns the file size.
func (r *Reader) Bytes() int { return len(r.data) }

// Col returns the index of a named column or -1.
func (r *Reader) Col(name string) int {
	for i, c := range r.cols {
		if c == name {
			return i
		}
	}
	return -1
}

// Batch is one decoded block restricted to [Lo, Hi) rows inside the requested time range.
type Batch struct {
	Cols   [][]int64 // indexed by position in the projection passed to Scan
	Lo, Hi int
}

// chunk returns the type-specific encoding of a column chunk, decompressing if needed.
func (r *Reader) chunk(off [2]int, buf *[]byte) ([]byte, error) {
	if off[1] <= off[0] {
		return nil, ErrFormat
	}
	b := r.data[off[0]:off[1]]
	switch b[0] {
	case codecRaw:
		return b[1:], nil
	case codecZstd:
		out, err := zdec.DecodeAll(b[1:], (*buf)[:0])
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrFormat, err)
		}
		*buf = out
		return out, nil
	}
	return nil, ErrFormat
}

// Scan calls fn for each block overlapping [t0, t1) with the projected columns decoded. Column 0
// (timestamps) is always decoded first and returned as the first projection if requested.
// The slices are reused between calls.
func (r *Reader) Scan(t0, t1 int64, proj []int, fn func(b *Batch) error) error {
	// First block whose tmax >= t0.
	start := sort.Search(len(r.index), func(i int) bool { return r.index[i].tmax >= t0 })
	b := &Batch{Cols: make([][]int64, len(proj))}
	var ts []int64
	var zb []byte
	for bi := start; bi < len(r.index); bi++ {
		m := r.index[bi]
		if m.tmin >= t1 {
			break
		}
		offs, err := r.columnOffsets(m)
		if err != nil {
			return err
		}
		raw, err := r.chunk(offs[0], &zb)
		if err != nil {
			return err
		}
		ts, _, err = enc.DecodeTimestamps(ts[:0], raw)
		if err != nil {
			return err
		}
		lo := sort.Search(len(ts), func(i int) bool { return ts[i] >= t0 })
		hi := sort.Search(len(ts), func(i int) bool { return ts[i] >= t1 })
		if lo >= hi {
			continue
		}
		for k, c := range proj {
			if c == 0 {
				b.Cols[k] = append(b.Cols[k][:0], ts...)
				continue
			}
			raw, err := r.chunk(offs[c], &zb)
			if err != nil {
				return err
			}
			b.Cols[k], _, err = enc.DecodeDeltaInts(b.Cols[k][:0], raw)
			if err != nil {
				return err
			}
		}
		b.Lo, b.Hi = lo, hi
		if err := fn(b); err != nil {
			return err
		}
	}
	return nil
}

// Verify checks every block's CRC.
func (r *Reader) Verify() error {
	for _, m := range r.index {
		offs, err := r.columnOffsets(m)
		if err != nil {
			return err
		}
		start, end := offs[0][0]-4, offs[len(offs)-1][1]
		_ = start
		colStart := int(m.off) + 20
		if end+4 > len(r.data) {
			return ErrFormat
		}
		want := binary.LittleEndian.Uint32(r.data[end:])
		if crc32.Checksum(r.data[colStart:end], castagnoli) != want {
			return fmt.Errorf("%w: CRC mismatch in block at %d", ErrFormat, m.off)
		}
	}
	return nil
}

func (r *Reader) columnOffsets(m blockMeta) ([][2]int, error) {
	p := int(m.off) + 20
	offs := make([][2]int, len(r.cols))
	for i := range r.cols {
		if p+4 > len(r.data) {
			return nil, ErrFormat
		}
		l := int(binary.LittleEndian.Uint32(r.data[p:]))
		if p+4+l > len(r.data) {
			return nil, ErrFormat
		}
		offs[i] = [2]int{p + 4, p + 4 + l}
		p += 4 + l
	}
	return offs, nil
}
