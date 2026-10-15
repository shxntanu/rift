// Package direct implements the opt-in SSH-bootstrapped AES-256-GCM data path.
// The session secret is sent only over SSH. Each of 32 connection slots can be
// authenticated once, with fresh challenges and separate directional keys.
package direct

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	Slots      = 32
	Timeout    = 5 * time.Second
	recordSize = 64 << 10
	maxRecords = 1 << 32 // cap each directional key at 256 TiB
)

// Offer is passed over the authenticated SSH stream, never over plain TCP.
type Offer struct {
	Port uint16
	Key  [32]byte
}

// Listener lives only as long as the SSH control session that created it.
type Listener struct {
	Offer  Offer
	ln     net.Listener
	mu     sync.Mutex
	used   [Slots]bool
	active map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
	done   chan struct{}
}

// Listen opens an ephemeral port. serve runs only after mutual authentication.
func Listen(serve func(*Conn)) (*Listener, error) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	l := &Listener{ln: ln, active: make(map[net.Conn]struct{}), done: make(chan struct{})}
	l.Offer.Port = uint16(ln.Addr().(*net.TCPAddr).Port)
	if _, err := rand.Read(l.Offer.Key[:]); err != nil {
		_ = ln.Close()
		return nil, err
	}
	l.wg.Go(func() {
		// Bound unauthenticated sockets and handshake work as well as sessions.
		sem := make(chan struct{}, Slots)
		for {
			select {
			case sem <- struct{}{}:
			case <-l.done:
				return
			}
			c, err := ln.Accept()
			if err != nil {
				<-sem
				return
			}
			l.mu.Lock()
			if l.closed {
				l.mu.Unlock()
				_ = c.Close()
				<-sem
				return
			}
			l.active[c] = struct{}{}
			l.mu.Unlock()
			l.wg.Go(func() {
				defer func() {
					_ = c.Close()
					l.mu.Lock()
					delete(l.active, c)
					l.mu.Unlock()
					<-sem
				}()
				secure, err := l.authenticate(c)
				if err == nil {
					serve(secure)
				}
			})
		}
	})
	return l, nil
}

// Close stops accepts and all authenticated or pending connections, then joins.
func (l *Listener) Close() error {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		close(l.done)
		_ = l.ln.Close()
		for c := range l.active {
			_ = c.Close()
		}
	}
	l.mu.Unlock()
	l.wg.Wait()
	return nil
}

func mac(key []byte, label string, transcript []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte("rift-direct-v1/" + label + "\x00"))
	_, _ = h.Write(transcript)
	return h.Sum(nil)
}

func (l *Listener) authenticate(c net.Conn) (*Conn, error) {
	if err := c.SetDeadline(time.Now().Add(Timeout)); err != nil {
		return nil, err
	}
	// transcript = slot || server challenge || client challenge.
	var transcript [65]byte
	if _, err := io.ReadFull(c, transcript[:1]); err != nil {
		return nil, err
	}
	idx := int(transcript[0])
	if idx >= Slots {
		return nil, errors.New("invalid direct slot")
	}
	if _, err := rand.Read(transcript[1:33]); err != nil {
		return nil, err
	}
	if err := writeFull(c, transcript[1:33]); err != nil {
		return nil, err
	}
	var reply [64]byte
	if _, err := io.ReadFull(c, reply[:]); err != nil {
		return nil, err
	}
	copy(transcript[33:], reply[:32])
	if !hmac.Equal(reply[32:], mac(l.Offer.Key[:], "client-proof", transcript[:])) {
		return nil, errors.New("direct authentication failed")
	}
	l.mu.Lock()
	used := l.used[idx]
	l.used[idx] = true
	l.mu.Unlock()
	if used {
		return nil, errors.New("direct slot already used")
	}
	if err := writeFull(c, mac(l.Offer.Key[:], "server-proof", transcript[:])); err != nil {
		return nil, err
	}
	return newConn(c, l.Offer.Key[:], transcript[:], false)
}

// Client authenticates one slot on a connected TCP socket. Callers must close
// the socket on failure. The listener rejects reuse of a successfully used slot.
func Client(c net.Conn, offer Offer, slot int) (*Conn, error) {
	if slot < 0 || slot >= Slots {
		return nil, errors.New("invalid direct slot")
	}
	if err := c.SetDeadline(time.Now().Add(Timeout)); err != nil {
		return nil, err
	}
	var transcript [65]byte
	transcript[0] = byte(slot)
	if err := writeFull(c, transcript[:1]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(c, transcript[1:33]); err != nil {
		return nil, err
	}
	if _, err := rand.Read(transcript[33:]); err != nil {
		return nil, err
	}
	reply := append(append([]byte(nil), transcript[33:]...), mac(offer.Key[:], "client-proof", transcript[:])...)
	if err := writeFull(c, reply); err != nil {
		return nil, err
	}
	var proof [32]byte
	if _, err := io.ReadFull(c, proof[:]); err != nil {
		return nil, err
	}
	if !hmac.Equal(proof[:], mac(offer.Key[:], "server-proof", transcript[:])) {
		return nil, errors.New("direct server authentication failed")
	}
	return newConn(c, offer.Key[:], transcript[:], true)
}

// Conn is an authenticated record stream. A reader and a writer may operate
// concurrently. Empty authenticated records mark EOF; bare TCP EOF is an error.
type Conn struct {
	net.Conn
	r, w                cipher.AEAD
	rseq, wseq          uint64
	rbuf, pending, wbuf []byte
	rdone, wdone        bool
	rerr, werr          error
}

func newConn(c net.Conn, key, transcript []byte, client bool) (*Conn, error) {
	aead := func(label string) (cipher.AEAD, error) {
		block, err := aes.NewCipher(mac(key, label, transcript))
		if err != nil {
			return nil, err
		}
		return cipher.NewGCM(block)
	}
	c2s, err := aead("client-to-server")
	if err != nil {
		return nil, err
	}
	s2c, err := aead("server-to-client")
	if err != nil {
		return nil, err
	}
	s := &Conn{Conn: c, r: c2s, w: s2c}
	if client {
		s.r, s.w = s2c, c2s
	}
	if err := c.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	return s, nil
}

func nonce(seq uint64) []byte {
	var n [12]byte
	binary.BigEndian.PutUint64(n[4:], seq)
	return n[:]
}

func (c *Conn) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if c.rerr != nil {
		return 0, c.rerr
	}
	if len(c.pending) == 0 {
		if c.rdone {
			return 0, io.EOF
		}
		c.rerr = c.readRecord()
		if c.rerr != nil {
			return 0, c.rerr
		}
		if c.rdone {
			return 0, io.EOF
		}
	}
	n := copy(dst, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *Conn) readRecord() error {
	var hdr [4]byte
	if _, err := io.ReadFull(c.Conn, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return errors.New("direct stream ended without authenticated EOF")
		}
		return err
	}
	n := int(binary.BigEndian.Uint32(hdr[:]))
	if n < c.r.Overhead() || n > recordSize+c.r.Overhead() {
		return errors.New("invalid direct record length")
	}
	if c.rseq >= maxRecords {
		return errors.New("direct nonce exhausted")
	}
	if cap(c.rbuf) < n {
		c.rbuf = make([]byte, n)
	}
	c.rbuf = c.rbuf[:n]
	if _, err := io.ReadFull(c.Conn, c.rbuf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return errors.New("direct stream ended without authenticated EOF")
		}
		return err
	}
	plain, err := c.r.Open(c.rbuf[:0], nonce(c.rseq), c.rbuf, hdr[:])
	if err != nil {
		return fmt.Errorf("direct record authentication: %w", err)
	}
	c.rseq++
	c.pending = plain
	c.rdone = len(plain) == 0
	return nil
}

func (c *Conn) Write(src []byte) (int, error) {
	if c.werr != nil {
		return 0, c.werr
	}
	if c.wdone {
		return 0, net.ErrClosed
	}
	written := 0
	for len(src) > 0 {
		n := min(len(src), recordSize)
		c.werr = c.writeRecord(src[:n])
		if c.werr != nil {
			return written, c.werr
		}
		written += n
		src = src[n:]
	}
	return written, nil
}

func (c *Conn) writeRecord(src []byte) error {
	if c.wseq >= maxRecords {
		return errors.New("direct nonce exhausted")
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(src)+c.w.Overhead()))
	c.wbuf = append(c.wbuf[:0], hdr[:]...)
	c.wbuf = c.w.Seal(c.wbuf, nonce(c.wseq), src, hdr[:])
	c.wseq++
	return writeFull(c.Conn, c.wbuf)
}

// CloseWrite sends an authenticated EOF before half-closing TCP.
func (c *Conn) CloseWrite() error {
	if c.werr != nil {
		return c.werr
	}
	if c.wdone {
		return nil
	}
	c.wdone = true
	c.werr = c.writeRecord(nil)
	if c.werr != nil {
		return c.werr
	}
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}
