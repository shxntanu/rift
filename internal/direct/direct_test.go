package direct

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/shxntanu/rift/internal/proto"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

func dialTest(t *testing.T, l *Listener, offer Offer, slot int) (*Conn, error) {
	t.Helper()
	c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(l.Offer.Port))), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Client(c, offer, slot)
	if err != nil {
		_ = c.Close()
	} else {
		t.Cleanup(func() { _ = s.Close() })
	}
	return s, err
}

func TestAuthenticationAndStreams(t *testing.T) {
	l, err := Listen(func(c *Conn) {
		_, _ = io.Copy(c, c)
		_ = c.CloseWrite()
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	bad := l.Offer
	bad.Key[0] ^= 1
	if _, err := dialTest(t, l, bad, 0); err == nil {
		t.Fatal("accepted wrong key")
	}
	// A failed authentication cannot consume a slot. Exercise all slots in
	// parallel, with messages spanning record boundaries and authenticated EOF.
	var wg sync.WaitGroup
	for slot := range Slots {
		wg.Go(func() {
			c, err := dialTest(t, l, l.Offer, slot)
			if err != nil {
				t.Error(err)
				return
			}
			_ = c.SetDeadline(time.Now().Add(10 * time.Second))
			want := bytes.Repeat([]byte{byte(slot)}, 2*recordSize+13)
			written := make(chan error, 1)
			go func() {
				_, err := c.Write(want)
				if err == nil {
					err = c.CloseWrite()
				}
				written <- err
			}()
			got, err := io.ReadAll(c)
			if err != nil || !bytes.Equal(got, want) {
				t.Errorf("slot %d: roundtrip: %v, length %d", slot, err, len(got))
			}
			if err := <-written; err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if _, err := dialTest(t, l, l.Offer, 0); err == nil {
		t.Fatal("accepted reused slot")
	}
}

// recordNet supplies deliberately corrupted ciphertext without a live peer.
type recordNet struct {
	net.Conn
	r *bytes.Reader
	w bytes.Buffer
}

func (c *recordNet) Read(b []byte) (int, error)  { return c.r.Read(b) }
func (c *recordNet) Write(b []byte) (int, error) { return c.w.Write(b) }
func (*recordNet) SetDeadline(time.Time) error   { return nil }

func TestRecordRejection(t *testing.T) {
	key := bytes.Repeat([]byte{1}, 32)
	transcript := bytes.Repeat([]byte{2}, 65)
	wire := &recordNet{}
	sender, err := newConn(wire, key, transcript, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sender.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	frame := append([]byte(nil), wire.w.Bytes()...)
	corrupt := append([]byte(nil), frame...)
	corrupt[len(corrupt)-1] ^= 1
	oversized := make([]byte, 4)
	binary.BigEndian.PutUint32(oversized, recordSize+17)
	for _, tc := range []struct {
		name   string
		data   []byte
		client bool
		seq    uint64
	}{
		{"tampered", corrupt, false, 0},
		{"oversized", oversized, false, 0},
		{"truncated", frame[:len(frame)-1], false, 0},
		{"missing-eof", frame, false, 0},
		{"replayed", append(append([]byte(nil), frame...), frame...), false, 0},
		{"wrong-direction", frame, true, 0},
		{"exhausted", frame, false, ^uint64(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := &recordNet{r: bytes.NewReader(tc.data)}
			c, err := newConn(raw, key, transcript, tc.client)
			if err != nil {
				t.Fatal(err)
			}
			c.rseq = tc.seq
			if _, err := io.ReadAll(c); err == nil || err == io.EOF {
				t.Fatal("accepted invalid record stream")
			}
			if _, err := c.Read(make([]byte, 1)); err == nil {
				t.Fatal("authentication failure was not sticky")
			}
		})
	}
}

func TestClosePendingHandshake(t *testing.T) {
	l, err := Listen(func(c *Conn) {})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(l.Offer.Port))))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	done := make(chan struct{})
	go func() { _ = l.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("listener close blocked on unauthenticated client")
	}
}

func TestTruncationThroughCompressedProtocol(t *testing.T) {
	key, transcript := make([]byte, 32), make([]byte, 65)
	wire := &recordNet{}
	writer, err := newConn(wire, key, transcript, true)
	if err != nil {
		t.Fatal(err)
	}
	out := proto.NewConn(writer)
	if err := out.EnableCompression(1); err != nil {
		t.Fatal(err)
	}
	if err := out.Send(proto.TSync, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := out.Flush(); err != nil {
		t.Fatal(err)
	}
	// The valid frame arrives, but the attacker cuts off the stream before
	// its authenticated EOF. zstd/proto must not convert this to clean EOF.
	input := &recordNet{r: bytes.NewReader(wire.w.Bytes())}
	reader, err := newConn(input, key, transcript, false)
	if err != nil {
		t.Fatal(err)
	}
	in := proto.NewConn(reader)
	if err := in.EnableCompression(1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := in.Recv(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := in.Recv(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("truncation accepted: %v", err)
	}
}
