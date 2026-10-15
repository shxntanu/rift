// Package scan walks directory trees in parallel and describes their contents.
package scan

import (
	"io"
	"os"
	"strings"

	"github.com/shxntanu/rift/internal/proto"

	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"
)

// Kind is the type of a filesystem entry.
type Kind uint8

// Entry kinds.
const (
	KindFile    Kind = 1
	KindDir     Kind = 2
	KindSymlink Kind = 3
)

// Entry describes one filesystem object relative to a sync root.
type Entry struct {
	Path  string // '/'-separated, relative to the root; "" is the root itself
	Kind  Kind
	Mode  uint32 // permission bits including setuid/setgid/sticky (07777)
	Size  int64
	Mtime int64 // unix nanoseconds
	Link  string
}

// Encode appends e to enc.
func (e *Entry) Encode(enc *proto.Enc) {
	enc.Str(e.Path)
	enc.U8(uint8(e.Kind))
	enc.Uvarint(uint64(e.Mode))
	enc.Uvarint(uint64(e.Size))
	enc.Varint(e.Mtime)
	if e.Kind == KindSymlink {
		enc.Str(e.Link)
	}
}

// DecodeEntry reads an entry written by Encode.
func DecodeEntry(d *proto.Dec) Entry {
	var e Entry
	e.Path = d.Str()
	e.Kind = Kind(d.U8())
	e.Mode = uint32(d.Uvarint())
	e.Size = int64(d.Uvarint())
	e.Mtime = d.Varint()
	if e.Kind == KindSymlink {
		e.Link = d.Str()
	}
	return e
}

// fromStat fills kind/mode/size/mtime from a raw stat result. It reports
// false for object types rift does not sync (sockets, devices, fifos).
func fromStat(st *unix.Stat_t, e *Entry) bool {
	switch uint32(st.Mode) & unix.S_IFMT {
	case unix.S_IFREG:
		e.Kind = KindFile
		e.Size = st.Size
	case unix.S_IFDIR:
		e.Kind = KindDir
	case unix.S_IFLNK:
		e.Kind = KindSymlink
	default:
		return false
	}
	e.Mode = uint32(st.Mode) & 0o7777
	e.Mtime = st.Mtim.Nano()
	return true
}

// Lstat describes a single path.
func Lstat(abs, rel string) (Entry, bool, error) {
	var st unix.Stat_t
	if err := unix.Lstat(abs, &st); err != nil {
		return Entry{}, false, &os.PathError{Op: "lstat", Path: abs, Err: err}
	}
	e := Entry{Path: rel}
	if !fromStat(&st, &e) {
		return e, false, nil
	}
	if e.Kind == KindSymlink {
		l, err := os.Readlink(abs)
		if err != nil {
			return e, false, err
		}
		e.Link = l
	}
	return e, true, nil
}

// FileMode converts unix permission bits to an os.FileMode.
func FileMode(m uint32) os.FileMode {
	fm := os.FileMode(m & 0o777)
	if m&unix.S_ISUID != 0 {
		fm |= os.ModeSetuid
	}
	if m&unix.S_ISGID != 0 {
		fm |= os.ModeSetgid
	}
	if m&unix.S_ISVTX != 0 {
		fm |= os.ModeSticky
	}
	return fm
}

// TempPrefix marks rift's in-flight temp files so scans can skip them.
const TempPrefix = ".rift-tmp."

// IsTemp reports whether a base name is a rift temp file.
func IsTemp(name string) bool { return strings.HasPrefix(name, TempPrefix) }

// HashFile returns the XXH3-128 of a file's contents.
func HashFile(abs string, buf []byte) (proto.Hash, error) {
	f, err := os.Open(abs)
	if err != nil {
		return proto.Hash{}, err
	}
	defer f.Close()
	h := xxh3.New128()
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return proto.Hash{}, err
	}
	s := h.Sum128()
	return proto.Hash{Hi: s.Hi, Lo: s.Lo}, nil
}

// HashBytes returns the XXH3-128 of b.
func HashBytes(b []byte) proto.Hash {
	s := xxh3.Hash128(b)
	return proto.Hash{Hi: s.Hi, Lo: s.Lo}
}
