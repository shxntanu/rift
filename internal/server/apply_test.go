package server

import (
	"bytes"
	"fmt"
	"golang.org/x/sys/unix"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shxntanu/rift/internal/proto"
	"github.com/shxntanu/rift/internal/scan"
)

type memoryStream struct{ bytes.Buffer }

func (*memoryStream) Close() error { return nil }

func TestApplyOrderingAndFences(t *testing.T) {
	wire := &memoryStream{}
	s := &server{raw: wire, c: proto.NewConn(wire), root: t.TempDir(), session: "test", ranges: make(map[uint64]*rangeFile), buf: make([]byte, proto.ChunkSize)}
	p := newApplyPool(s)
	defer p.close()
	send := func(typ proto.Type, encode func(*proto.Enc)) {
		t.Helper()
		var e proto.Enc
		encode(&e)
		if err := p.handle(s, typ, e.B); err != nil {
			t.Fatal(err)
		}
		// Model Recv reusing its payload immediately after dispatch.
		clear(e.B)
	}
	id := uint64(0)
	put := func(path, data string) {
		id++
		send(proto.TPut, func(e *proto.Enc) {
			e.Uvarint(id)
			e.U8(0)
			e.Str(path)
			e.Uvarint(0644)
			e.Varint(123456789)
			e.B = append(e.B, data...)
		})
	}
	// Each path repeatedly crosses put/metadata operations, while different
	// paths fill all lanes. The final write must win for every file.
	for i := range 100 {
		for j := range 20 {
			put(fmt.Sprintf("f%d", j), fmt.Sprint(i))
		}
	}
	send(proto.TMeta, func(e *proto.Enc) {
		e.Uvarint(20)
		for j := range 20 {
			e.Str(fmt.Sprintf("f%d", j))
			e.U8(uint8(scan.KindFile))
			e.Uvarint(0600)
			e.Varint(987654321)
		}
	})
	send(proto.TSync, func(e *proto.Enc) { e.Uvarint(42) })
	for j := range 20 {
		path := filepath.Join(s.root, fmt.Sprintf("f%d", j))
		b, err := os.ReadFile(path)
		if err != nil || string(b) != "99" {
			t.Fatalf("unordered put: %q %v", b, err)
		}
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != 0600 || fi.ModTime().UnixNano() != 987654321 {
			t.Fatalf("metadata reordered: %v", fi)
		}
	}
	pc := proto.NewConn(wire)
	typ, b, err := pc.Recv()
	if err != nil || typ != proto.TSyncAck {
		t.Fatalf("barrier: %v %v", typ, err)
	}
	d := proto.Dec{B: b}
	if d.Uvarint() != 42 {
		t.Fatal("incorrect barrier sequence")
	}

	put("conflict", "file")
	send(proto.TDelete, func(e *proto.Enc) { e.U8(proto.DeleteRecursive); e.Uvarint(1); e.Str("conflict") })
	send(proto.TMkdirs, func(e *proto.Enc) { e.Uvarint(1); e.Str("conflict"); e.Uvarint(0755) })
	put("conflict/child", "child")
	send(proto.TSymlink, func(e *proto.Enc) { e.Uvarint(2); e.Str("link"); e.Str("f0"); e.Varint(123456789) })
	put("link", "replacement")
	if err := p.drain(); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{"conflict/child": "child", "link": "replacement"} {
		b, err := os.ReadFile(filepath.Join(s.root, path))
		if err != nil || string(b) != want {
			t.Fatalf("fence %s: %q %v", path, b, err)
		}
	}
}

func TestApplyRangesAndDelta(t *testing.T) {
	wire := &memoryStream{}
	s := &server{raw: wire, c: proto.NewConn(wire), root: t.TempDir(), session: "test", ranges: make(map[uint64]*rangeFile), buf: make([]byte, proto.ChunkSize)}
	p := newApplyPool(s)
	defer p.close()
	send := func(typ proto.Type, e proto.Enc) {
		t.Helper()
		if err := p.handle(s, typ, e.B); err != nil {
			t.Fatal(err)
		}
		clear(e.B)
	}
	for part := range 4 {
		for id := uint64(1); id <= 32; id++ {
			var e proto.Enc
			e.Uvarint(id)
			e.Str(fmt.Sprintf("r%d", id))
			e.Uvarint(4)
			e.Uvarint(uint64(part))
			e.B = append(e.B, byte(part))
			send(proto.TRangeWrite, e)
		}
	}
	for id := uint64(1); id <= 32; id++ {
		var e proto.Enc
		e.Uvarint(id)
		send(proto.TRangeClose, e)
		e = proto.Enc{}
		e.Uvarint(id)
		e.Str(fmt.Sprintf("r%d", id))
		e.Uvarint(0600)
		e.Varint(123)
		send(proto.TFinalize, e)
	}
	if err := p.drain(); err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= 32; id++ {
		b, err := os.ReadFile(filepath.Join(s.root, fmt.Sprintf("r%d", id)))
		if err != nil || !bytes.Equal(b, []byte{0, 1, 2, 3}) {
			t.Fatalf("range %d: %x %v", id, b, err)
		}
	}
	var e proto.Enc
	e.Uvarint(1)
	e.Str("r1")
	e.Uvarint(5)
	send(proto.TDeltaBegin, e)
	e = proto.Enc{}
	e.Uvarint(0)
	e.Uvarint(4)
	send(proto.TDeltaCopy, e)
	send(proto.TDeltaData, proto.Enc{B: []byte{4}})
	e = proto.Enc{}
	e.Uvarint(0600)
	e.Varint(123)
	e.Hash(scan.HashBytes([]byte{0, 1, 2, 3, 4}))
	send(proto.TDeltaEnd, e)
	b, err := os.ReadFile(filepath.Join(s.root, "r1"))
	if err != nil || !bytes.Equal(b, []byte{0, 1, 2, 3, 4}) {
		t.Fatalf("delta: %x %v", b, err)
	}
}

func TestBoundDirectRoot(t *testing.T) {
	for _, tc := range []struct {
		name, root, session string
		direct              byte
	}{
		{"root", "elsewhere", "session", 0}, {"session", "bound", "wrong", 0}, {"nested-listener", "bound", "session", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			requested := root
			if tc.root != "bound" {
				requested = filepath.Join(root, tc.root)
			}
			var e proto.Enc
			e.Uvarint(proto.Version)
			e.Str(tc.session)
			e.Str(requested)
			e.Varint(0)
			e.U8(tc.direct)
			input := &memoryStream{}
			pc := proto.NewConn(input)
			if err := pc.Send(proto.THello, e.B); err != nil {
				t.Fatal(err)
			}
			if err := pc.Flush(); err != nil {
				t.Fatal(err)
			}
			s := &server{c: proto.NewConn(input), boundRoot: root, boundSession: "session"}
			if err := s.handshake(); err == nil {
				t.Fatal("accepted unauthorized direct hello")
			}
		})
	}
}

var _ io.ReadWriteCloser = (*memoryStream)(nil)

func TestIndependentApplyAndFence(t *testing.T) {
	wire := &memoryStream{}
	s := &server{raw: wire, c: proto.NewConn(wire), root: t.TempDir(), session: "test", ranges: make(map[uint64]*rangeFile)}
	p := newApplyPool(s)
	fifo := filepath.Join(s.root, "fifo")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	// Keep the FIFO open throughout pool teardown, even on a failed assertion,
	// so a writer that has not reached open yet cannot strand cleanup.
	defer func() {
		f, err := os.OpenFile(fifo, os.O_RDWR, 0)
		if err == nil {
			defer func() { _ = f.Close() }()
		}
		p.close()
	}()
	var e proto.Enc
	e.Uvarint(1)
	e.U8(proto.PutNew)
	e.Str("fifo")
	e.Uvarint(0600)
	e.Varint(123)
	e.B = append(e.B, 'x')
	if err := p.handle(s, proto.TPut, e.B); err != nil {
		t.Fatal(err)
	}
	// Select a different hash lane from "fifo".
	hash := func(path string) uint32 {
		h := fnv.New32a()
		_, _ = h.Write([]byte(path))
		return h.Sum32() % applyWorkers
	}
	name := "other"
	for hash(name) == hash("fifo") {
		name += "x"
	}
	e = proto.Enc{}
	e.Uvarint(2)
	e.U8(proto.PutNew)
	e.Str(name)
	e.Uvarint(0600)
	e.Varint(123)
	e.B = append(e.B, 'y')
	if err := p.handle(s, proto.TPut, e.B); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if b, err := os.ReadFile(filepath.Join(s.root, name)); err == nil && string(b) == "y" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("independent path stalled behind FIFO")
		}
		time.Sleep(time.Millisecond)
	}
	fenced := make(chan error, 1)
	go func() { fenced <- p.drain() }()
	select {
	case err := <-fenced:
		t.Fatalf("fence passed blocked operation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	f, err := os.OpenFile(fifo, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(f); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-fenced:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fence did not drain")
	}
}
