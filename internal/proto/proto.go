// Package proto implements rift's framed binary wire protocol.
//
// Every frame is a 1-byte message type followed by a 4-byte little-endian
// payload length and the payload. After the handshake both directions are
// optionally wrapped in a zstd stream; frames are flushed explicitly so many
// small frames coalesce into a single compressed block on the wire.
package proto

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Version is bumped whenever the wire format changes incompatibly.
const Version = 1

// Magic is written raw by the server as the very first bytes of a session.
const Magic = "RIFT!"

// MaxFrame bounds a single frame so a corrupt stream cannot force a huge allocation.
const MaxFrame = 64 << 20

// ChunkSize is the unit of file data carried by one frame.
const ChunkSize = 1 << 20

// Type identifies a message.
type Type uint8

// Message types. C→S are sent by the client, S→C by the server.
const (
	THello       Type = iota + 1 // C→S version, session, root, compression level
	THelloOK                     // S→C version, goos, goarch, absolute root
	TScan                        // C→S exclude patterns
	TScanEntries                 // S→C batch of entries
	TScanDone                    // S→C entry count
	THashReq                     // C→S request id, paths
	THashResp                    // S→C request id, hashes
	TMkdirs                      // C→S directories to create
	TPut                         // C→S whole small file
	TSymlink                     // C→S create/replace symlink
	TRangeWrite                  // C→S write a slice of a large file into its temp file
	TRangeClose                  // C→S done with a range on this connection
	TRangeAck                    // S→C bytes written for a closed range
	TFinalize                    // C→S rename a fully written temp file into place
	TSigReq                      // C→S request content-defined chunk signature
	TSig                         // S→C chunk signature
	TDeltaBegin                  // C→S start reconstructing a file from its basis
	TDeltaCopy                   // C→S copy a basis range
	TDeltaData                   // C→S literal bytes
	TDeltaEnd                    // C→S verify and finalize the reconstruction
	TMeta                        // C→S set mode/mtime
	TDelete                      // C→S remove paths
	TSync                        // C→S barrier
	TSyncAck                     // S→C barrier reply
	TError                       // S→C error report
)

// Error codes carried by TError.
const (
	CodeGeneric     uint8 = 1
	CodeDeltaFailed uint8 = 2 // reconstruction failed; client should resend in full
	CodeFatal       uint8 = 3
)

// Put flags.
const (
	PutNew uint8 = 1 << iota // destination did not exist; write in place without temp+rename
)

// Delete flags.
const (
	DeleteRecursive uint8 = 1 << iota
)

// Conn is a framed, optionally compressed, bidirectional message stream.
// Send/Flush and Recv may be used from two different goroutines, but each
// side must only be used by one goroutine at a time.
type Conn struct {
	raw io.ReadWriteCloser
	bw  *bufio.Writer
	zw  *zstd.Encoder
	w   io.Writer
	br  *bufio.Reader
	zr  *zstd.Decoder
	r   io.Reader

	hdr  [5]byte
	rhdr [5]byte
	rbuf []byte
}

// NewConn wraps raw in an uncompressed framed connection.
func NewConn(raw io.ReadWriteCloser) *Conn {
	c := &Conn{
		raw: raw,
		bw:  bufio.NewWriterSize(raw, 256<<10),
		br:  bufio.NewReaderSize(raw, 256<<10),
	}
	c.w = c.bw
	c.r = c.br
	return c
}

// EnableCompression switches both directions to zstd at the given level
// (1=fastest .. 4=best). It must be called at the same frame boundary on
// both peers, with no unflushed outgoing data.
func (c *Conn) EnableCompression(level int) error {
	zw, err := zstd.NewWriter(c.bw,
		zstd.WithEncoderLevel(zstd.EncoderLevel(level)),
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(1<<22),
	)
	if err != nil {
		return fmt.Errorf("zstd writer: %w", err)
	}
	zr, err := zstd.NewReader(c.br,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxWindow(1<<26),
	)
	if err != nil {
		return fmt.Errorf("zstd reader: %w", err)
	}
	c.zw, c.zr = zw, zr
	c.w, c.r = zw, zr
	return nil
}

// Send writes one frame whose payload is the concatenation of parts.
func (c *Conn) Send(t Type, parts ...[]byte) error {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	if n > MaxFrame {
		return fmt.Errorf("frame too large: %d", n)
	}
	c.hdr[0] = byte(t)
	binary.LittleEndian.PutUint32(c.hdr[1:], uint32(n))
	if _, err := c.w.Write(c.hdr[:]); err != nil {
		return err
	}
	for _, p := range parts {
		if _, err := c.w.Write(p); err != nil {
			return err
		}
	}
	return nil
}

// Flush pushes all buffered frames to the peer.
func (c *Conn) Flush() error {
	if c.zw != nil {
		if err := c.zw.Flush(); err != nil {
			return err
		}
	}
	return c.bw.Flush()
}

// Recv reads the next frame. The returned payload is only valid until the
// next call to Recv.
func (c *Conn) Recv() (Type, []byte, error) {
	if n, err := io.ReadFull(c.r, c.rhdr[:]); err != nil {
		// The peer hanging up between frames is a clean end of stream, even
		// if its compressed stream was not formally terminated.
		if n == 0 && errors.Is(err, io.ErrUnexpectedEOF) {
			err = io.EOF
		}
		return 0, nil, err
	}
	n := binary.LittleEndian.Uint32(c.rhdr[1:])
	if n > MaxFrame {
		return 0, nil, fmt.Errorf("frame too large: %d", n)
	}
	if cap(c.rbuf) < int(n) {
		c.rbuf = make([]byte, n, max(int(n), 64<<10))
	}
	buf := c.rbuf[:n]
	if _, err := io.ReadFull(c.r, buf); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return Type(c.rhdr[0]), buf, nil
}

// CloseWrite terminates the outgoing stream and half-closes the transport
// so the peer sees a clean EOF. Receiving keeps working until the peer
// hangs up.
func (c *Conn) CloseWrite() error {
	if c.zw != nil {
		if err := c.zw.Close(); err != nil {
			return err
		}
	}
	if err := c.bw.Flush(); err != nil {
		return err
	}
	if cw, ok := c.raw.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

// Close releases the connection. Recv must no longer be in use.
func (c *Conn) Close() error {
	if c.zr != nil {
		c.zr.Close()
	}
	return c.raw.Close()
}

// Enc is an append-only payload encoder.
type Enc struct{ B []byte }

// Reset clears the buffer, keeping its capacity.
func (e *Enc) Reset() { e.B = e.B[:0] }

// U8 appends a byte.
func (e *Enc) U8(v uint8) { e.B = append(e.B, v) }

// Uvarint appends an unsigned varint.
func (e *Enc) Uvarint(v uint64) { e.B = binary.AppendUvarint(e.B, v) }

// Varint appends a signed varint.
func (e *Enc) Varint(v int64) { e.B = binary.AppendVarint(e.B, v) }

// U64 appends a fixed 8-byte little-endian integer.
func (e *Enc) U64(v uint64) { e.B = binary.LittleEndian.AppendUint64(e.B, v) }

// Str appends a length-prefixed string.
func (e *Enc) Str(s string) {
	e.Uvarint(uint64(len(s)))
	e.B = append(e.B, s...)
}

// Bytes appends a length-prefixed byte slice.
func (e *Enc) Bytes(b []byte) {
	e.Uvarint(uint64(len(b)))
	e.B = append(e.B, b...)
}

// Hash appends a 128-bit hash.
func (e *Enc) Hash(h Hash) {
	e.U64(h.Hi)
	e.U64(h.Lo)
}

// Dec decodes a payload. Errors are sticky: after the first failure every
// accessor returns a zero value and Err reports the problem.
type Dec struct {
	B   []byte
	Err error
}

var errShort = errors.New("proto: truncated payload")

func (d *Dec) fail() {
	if d.Err == nil {
		d.Err = errShort
	}
	d.B = nil
}

// U8 reads a byte.
func (d *Dec) U8() uint8 {
	if len(d.B) < 1 {
		d.fail()
		return 0
	}
	v := d.B[0]
	d.B = d.B[1:]
	return v
}

// Uvarint reads an unsigned varint.
func (d *Dec) Uvarint() uint64 {
	v, n := binary.Uvarint(d.B)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.B = d.B[n:]
	return v
}

// Varint reads a signed varint.
func (d *Dec) Varint() int64 {
	v, n := binary.Varint(d.B)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.B = d.B[n:]
	return v
}

// U64 reads a fixed 8-byte integer.
func (d *Dec) U64() uint64 {
	if len(d.B) < 8 {
		d.fail()
		return 0
	}
	v := binary.LittleEndian.Uint64(d.B)
	d.B = d.B[8:]
	return v
}

// BytesRef reads a length-prefixed byte slice without copying.
func (d *Dec) BytesRef() []byte {
	n := d.Uvarint()
	if uint64(len(d.B)) < n {
		d.fail()
		return nil
	}
	b := d.B[:n]
	d.B = d.B[n:]
	return b
}

// Str reads a length-prefixed string.
func (d *Dec) Str() string { return string(d.BytesRef()) }

// Hash reads a 128-bit hash.
func (d *Dec) Hash() Hash {
	hi := d.U64()
	lo := d.U64()
	return Hash{Hi: hi, Lo: lo}
}

// Rest returns the remaining undecoded bytes.
func (d *Dec) Rest() []byte {
	b := d.B
	d.B = nil
	return b
}

// Hash is a 128-bit content hash (XXH3-128).
type Hash struct{ Hi, Lo uint64 }
