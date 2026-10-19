package transport

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/shxntanu/rift/internal/proto"
	"github.com/shxntanu/rift/internal/server"
)

// Kernel pipes provide bounded buffering and directional EOF, just like the
// subprocess transport. Unbuffered io.Pipes deadlock the pipelined handshake.
func (d *Dialer) local(ctx context.Context) (*Conn, error) {
	in, write, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	read, out, err := os.Pipe()
	if err != nil {
		_ = in.Close()
		_ = write.Close()
		return nil, err
	}
	rw := &localStream{in, out}
	done := make(chan error, 1)
	c := &Conn{stdin: write, stdout: read, d: d, wait: func() error { return <-done }}
	c.stop = context.AfterFunc(ctx, func() {
		_ = write.Close()
		_ = read.Close()
		_ = rw.Close()
	})
	go func() {
		err := server.ServeContext(ctx, rw)
		_ = rw.Close()
		done <- err
	}()
	var magic [len(proto.Magic)]byte
	if _, err := io.ReadFull(c, magic[:]); err != nil {
		_ = c.Close()
		return nil, errors.Join(err, ctx.Err())
	}
	return c, nil
}

type localStream struct {
	in  *os.File
	out *os.File
}

func (s *localStream) Read(p []byte) (int, error)  { return s.in.Read(p) }
func (s *localStream) Write(p []byte) (int, error) { return s.out.Write(p) }
func (s *localStream) CloseWrite() error           { return s.out.Close() }
func (s *localStream) Close() error                { return errors.Join(s.in.Close(), s.out.Close()) }
