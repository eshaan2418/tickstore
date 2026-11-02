package store

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

// A pack concatenates every partition of a table into one file with a trailing index, so
// cross-sectional queries map one file instead of opening thousands (per-file open/mmap syscalls
// dominate otherwise). Each entry is a complete partition parsed in place (zero copy).
//
//	partition bytes* | (u16 len, name, u64 off, u64 size)* | u32 n | u64 index_off | "TKSP"

// WritePack packs every *.tks file in dir into out.
func WritePack(dir, out string) (int, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.tks"))
	if err != nil {
		return 0, err
	}
	sort.Strings(files)
	tmp := out + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}
	var idx []byte
	var off uint64
	for _, p := range files {
		b, err := os.ReadFile(p)
		if err != nil {
			_ = f.Close()
			return 0, err
		}
		if _, err := f.Write(b); err != nil {
			_ = f.Close()
			return 0, err
		}
		name := strings.TrimSuffix(filepath.Base(p), ".tks")
		idx = binary.LittleEndian.AppendUint16(idx, uint16(len(name)))
		idx = append(idx, name...)
		idx = binary.LittleEndian.AppendUint64(idx, off)
		idx = binary.LittleEndian.AppendUint64(idx, uint64(len(b)))
		off += uint64(len(b))
	}
	idx = binary.LittleEndian.AppendUint32(idx, uint32(len(files)))
	idx = binary.LittleEndian.AppendUint64(idx, off)
	idx = append(idx, "TKSP"...)
	if _, err := f.Write(idx); err != nil {
		_ = f.Close()
		return 0, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	return len(files), os.Rename(tmp, out)
}

// Pack is a memory-mapped pack file.
type Pack struct {
	data    []byte
	Names   []string
	entries [][2]uint64
}

// OpenPack maps a pack and parses its index.
func OpenPack(path string) (*Pack, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil || st.Size() < 16 {
		return nil, ErrFormat
	}
	d, err := syscall.Mmap(int(f.Fd()), 0, int(st.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	p := &Pack{data: d}
	if string(d[len(d)-4:]) != "TKSP" {
		_ = p.Close()
		return nil, ErrFormat
	}
	n := int(binary.LittleEndian.Uint32(d[len(d)-16:]))
	io := binary.LittleEndian.Uint64(d[len(d)-12:])
	pos := int(io)
	for i := 0; i < n; i++ {
		if pos+2 > len(d) {
			_ = p.Close()
			return nil, ErrFormat
		}
		l := int(binary.LittleEndian.Uint16(d[pos:]))
		if pos+2+l+16 > len(d) {
			_ = p.Close()
			return nil, ErrFormat
		}
		p.Names = append(p.Names, string(d[pos+2:pos+2+l]))
		off := binary.LittleEndian.Uint64(d[pos+2+l:])
		sz := binary.LittleEndian.Uint64(d[pos+10+l:])
		if off+sz > io {
			_ = p.Close()
			return nil, ErrFormat
		}
		p.entries = append(p.entries, [2]uint64{off, sz})
		pos += 2 + l + 16
	}
	return p, nil
}

// Reader returns the i-th partition (zero copy).
func (p *Pack) Reader(i int) (*Reader, error) {
	e := p.entries[i]
	return FromBytes(p.data[e[0] : e[0]+e[1]])
}

// Close unmaps the pack.
func (p *Pack) Close() error { return syscall.Munmap(p.data) }
