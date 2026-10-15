package client

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/shxntanu/rift/internal/proto"
	"github.com/shxntanu/rift/internal/transport"
)

func handshakeSession(t *testing.T, mode string, compression int) *session {
	t.Helper()
	script := filepath.Join(t.TempDir(), "ssh")
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
	command := "#!/bin/sh\nexec " + quote(os.Args[0]) + " -test.run='^TestConnectServerProcess$' -- " + quote(mode) + "\n"
	if err := os.WriteFile(script, []byte(command), 0o700); err != nil {
		t.Fatal(err)
	}
	dst := transport.Endpoint{Host: "test", Path: t.TempDir()}
	return &session{
		id:     "testsession",
		opt:    Options{Dst: dst, Compress: compression},
		dialer: transport.NewDialer(dst, transport.Options{SSH: script}),
		sched:  newScheduler(),
	}
}

func TestConnectPipelinesScan(t *testing.T) {
	for _, compression := range []int{0, 1} {
		t.Run(fmt.Sprintf("compression=%d", compression), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s := handshakeSession(t, "rift-connect-pipeline", compression)
			rc, err := s.connect(ctx, false, 0, func() ([]string, error) {
				return []string{"- *.tmp", "+ important.tmp"}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer rc.close()
			if s.remoteRoot != s.opt.Dst.Path || s.remotePlatform != "testos/testarch" {
				t.Fatalf("remote = %q, %q", s.remoteRoot, s.remotePlatform)
			}
			if _, _, err := collectScan(rc, s.sched.abortCh); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConnectReportsRejectedHello(t *testing.T) {
	for _, size := range []int{0, 1 << 20} {
		t.Run(fmt.Sprintf("scanBytes=%d", size), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s := handshakeSession(t, "rift-connect-reject", 1)
			spec := make([]byte, size)
			if _, err := rand.Read(spec); err != nil {
				t.Fatal(err)
			}
			_, err := s.connect(ctx, false, 0, func() ([]string, error) {
				return []string{string(spec)}, nil
			})
			if err == nil || err.Error() != "server: rejected hello" {
				t.Fatalf("connect() error = %v, want server rejection", err)
			}
		})
	}
}

type handshakeStdio struct {
	io.Reader
	io.Writer
}

func (handshakeStdio) Close() error { return nil }

// This subprocess deliberately waits for TScan before replying to THello,
// proving that connect does not wait for the hello reply before sending it.
func TestConnectServerProcess(t *testing.T) {
	mode := os.Args[len(os.Args)-1]
	if mode != "rift-connect-pipeline" && mode != "rift-connect-reject" {
		return
	}
	err := serveHandshakeTest(mode)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func serveHandshakeTest(mode string) error {
	if _, err := io.WriteString(os.Stdout, proto.Magic); err != nil {
		return err
	}
	pc := proto.NewConn(handshakeStdio{os.Stdin, os.Stdout})
	t, p, err := pc.Recv()
	if err != nil || t != proto.THello {
		return fmt.Errorf("hello: %d, %v", t, err)
	}
	d := proto.Dec{B: p}
	d.Uvarint()
	d.Str()
	root := d.Str()
	compression := int(d.Varint())
	if d.Err != nil {
		return d.Err
	}
	var enc proto.Enc
	if mode == "rift-connect-reject" {
		enc.Uvarint(0)
		enc.U8(proto.CodeFatal)
		enc.Str("rejected hello")
		if err := pc.Send(proto.TError, enc.B); err != nil {
			return err
		}
		return pc.Flush()
	}
	if compression > 0 {
		if err := pc.EnableReadCompression(); err != nil {
			return err
		}
	}
	t, p, err = pc.Recv()
	if err != nil || t != proto.TScan {
		return fmt.Errorf("scan before hello reply: %d, %v", t, err)
	}
	d = proto.Dec{B: p}
	var specs []string
	for n := d.Uvarint(); n > 0; n-- {
		specs = append(specs, d.Str())
	}
	if d.Err != nil || !reflect.DeepEqual(specs, []string{"- *.tmp", "+ important.tmp"}) {
		return fmt.Errorf("scan specs = %q, %v", specs, d.Err)
	}
	enc.Uvarint(proto.Version)
	enc.Str("testos")
	enc.Str("testarch")
	enc.Str(root)
	if err := pc.Send(proto.THelloOK, enc.B); err != nil {
		return err
	}
	if compression > 0 {
		if err := pc.EnableWriteCompression(compression); err != nil {
			return err
		}
	}
	enc.Reset()
	enc.Uvarint(0)
	enc.Uvarint(0)
	if err := pc.Send(proto.TScanDone, enc.B); err != nil {
		return err
	}
	if err := pc.Flush(); err != nil {
		return err
	}
	_, _, err = pc.Recv()
	if err != io.EOF {
		return fmt.Errorf("client close: %v", err)
	}
	return nil
}
