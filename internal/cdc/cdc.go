// Package cdc implements FastCDC content-defined chunking.
//
// Chunk boundaries depend only on local content, so an insertion or deletion
// in the middle of a file only changes the chunks around the edit. Both
// peers chunk independently and compare chunk hashes; matching chunks are
// copied from the receiver's existing file instead of being transferred.
package cdc

import (
	"errors"
	"io"
	"math/bits"
)

// Params are the chunk size bounds. Both peers must use identical params.
type Params struct {
	Min, Avg, Max int
}

// ParamsFor picks chunk sizes that keep signatures around 16-32K chunks
// regardless of file size, bounding signature transfer cost.
func ParamsFor(size int64) Params {
	avg := 16 << 10
	for avg < 1<<20 && size/int64(avg) > 1<<15 {
		avg <<= 1
	}
	return Params{Min: avg / 4, Avg: avg, Max: avg * 4}
}

// Valid reports whether p is usable.
func (p Params) Valid() bool {
	return p.Min > 0 && p.Avg >= p.Min && p.Max >= p.Avg && p.Max <= 64<<20 && p.Avg&(p.Avg-1) == 0
}

var gear [256]uint64

func init() {
	// splitmix64 with a fixed seed: deterministic on every peer.
	s := uint64(0x9E3779B97F4A7C15)
	for i := range gear {
		s += 0x9E3779B97F4A7C15
		z := s
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		gear[i] = z ^ (z >> 31)
	}
}

// masks use the high bits of the gear hash, which depend on the last 64
// bytes of input; low bits would only depend on the last few bytes.
func masks(avg int) (small, large uint64) {
	b := bits.Len(uint(avg)) - 1
	small = ^uint64(0) << (64 - (b + 2))
	large = ^uint64(0) << (64 - (b - 2))
	return
}

// cut returns the length of the next chunk in data. Normalized chunking uses
// a stricter mask before Avg and a looser one after it, tightening the chunk
// size distribution around Avg.
func cut(data []byte, p Params, ms, ml uint64) int {
	n := len(data)
	if n <= p.Min {
		return n
	}
	if n > p.Max {
		n = p.Max
	}
	normal := min(p.Avg, n)
	var h uint64
	i := p.Min
	for ; i < normal; i++ {
		h = (h << 1) + gear[data[i]]
		if h&ms == 0 {
			return i + 1
		}
	}
	for ; i < n; i++ {
		h = (h << 1) + gear[data[i]]
		if h&ml == 0 {
			return i + 1
		}
	}
	return n
}

// Split reads r to EOF and calls fn for each chunk. The data slice is only
// valid during the callback.
func Split(r io.Reader, p Params, fn func(off int64, data []byte) error) error {
	if !p.Valid() {
		return errors.New("cdc: invalid params")
	}
	ms, ml := masks(p.Avg)
	buf := make([]byte, max(p.Max*4, 1<<20))
	start, end := 0, 0
	var off int64
	eof := false
	for {
		if !eof && end-start < p.Max {
			copy(buf, buf[start:end])
			end -= start
			start = 0
			n, err := io.ReadFull(r, buf[end:])
			end += n
			switch {
			case err == io.EOF || err == io.ErrUnexpectedEOF:
				eof = true
			case err != nil:
				return err
			}
		}
		if start == end {
			return nil
		}
		n := cut(buf[start:end], p, ms, ml)
		if err := fn(off, buf[start:start+n]); err != nil {
			return err
		}
		off += int64(n)
		start += n
	}
}
