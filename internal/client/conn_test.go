package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shxntanu/rift/internal/proto"
	"github.com/shxntanu/rift/internal/scan"
	"github.com/shxntanu/rift/internal/transport"
)

func TestWorkerJoinsBeforeShutdown(t *testing.T) {
	s := &session{sched: newScheduler()}
	rc := &rconn{s: s, pc: proto.NewConn(nil)}
	rc.startWork()
	s.sched.mu.Lock()
	workers := s.sched.workers
	s.sched.mu.Unlock()
	s.sched.close()
	s.sched.waitWorkers()
	if workers != 1 {
		t.Fatalf("worker was not registered before startWork returned: %d", workers)
	}
	// Reuse must be safe even if shutdown preceded the worker's first read.
	rc.s = &session{sched: newScheduler()}
}

func TestAcknowledgementsOnlyConfirmEarlierFrames(t *testing.T) {
	rc := &rconn{}
	rc.recordScan()
	if !rc.unconfirmed() {
		t.Fatal("scan should be unconfirmed until ScanDone")
	}
	rc.confirm(proto.TScanDone, 0)
	if rc.unconfirmed() {
		t.Fatal("completed scan should permit immediate teardown")
	}
	var enc proto.Enc
	enc.Uvarint(17)
	rc.recordFrame(proto.TSync, enc.B)
	rc.recordFrame(proto.TMeta, nil)
	rc.confirm(proto.TSyncAck, 17)
	if !rc.unconfirmed() {
		t.Fatal("barrier acknowledged metadata sent after the barrier")
	}
	enc.Reset()
	enc.Uvarint(18)
	rc.recordFrame(proto.TSync, enc.B)
	rc.confirm(proto.TSyncAck, 18)
	if rc.unconfirmed() {
		t.Fatal("latest barrier should confirm every preceding frame")
	}
	enc.Reset()
	enc.Uvarint(42)
	rc.recordFrame(proto.THashReq, enc.B)
	rc.confirm(proto.THashResp, 42)
	if rc.unconfirmed() {
		t.Fatal("hash-only no-op should permit immediate teardown")
	}
}

func TestMalformedServerError(t *testing.T) {
	rc := &rconn{s: &session{sched: newScheduler()}}
	if err := rc.dispatch(proto.TError, []byte{0, proto.CodeGeneric, 3, 'x'}); err == nil {
		t.Fatal("malformed server error was accepted")
	}
	if rc.s.errCount != 0 {
		t.Fatal("malformed server error was recorded as an ordinary file error")
	}
}

// This subprocess deliberately withholds EOF until its parent releases it.
// Running it through a tiny SSH replacement exercises real process pipes.
func TestConnCloseServerProcess(t *testing.T) {
	mode := os.Getenv("RIFT_CONN_CLOSE_TEST")
	if mode == "" {
		return
	}
	if _, err := io.WriteString(os.Stdout, proto.Magic); err != nil {
		os.Exit(2)
	}
	if mode == "run-final" {
		if err := serveFinalError(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		os.Exit(0)
	}
	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		os.Exit(2)
	}
	gate := os.Getenv("RIFT_CONN_CLOSE_GATE")
	if err := os.WriteFile(gate+".ready", nil, 0o600); err != nil {
		os.Exit(2)
	}
	for {
		if _, err := os.Stat(gate); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if mode == "truncated" {
		_, _ = os.Stdout.Write([]byte{byte(proto.TError), 10, 0, 0, 0, 1})
	} else {
		var enc proto.Enc
		enc.Uvarint(0)
		enc.U8(proto.CodeGeneric)
		enc.Str("final metadata failed")
		pc := proto.NewConn(closeTestStdio{})
		if err := pc.Send(proto.TError, enc.B); err == nil {
			_ = pc.Flush()
		}
	}
	os.Exit(0)
}

type closeTestStdio struct{}

func (closeTestStdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (closeTestStdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (closeTestStdio) Close() error                { return nil }

func serveFinalError() error {
	pc := proto.NewConn(closeTestStdio{})
	t, p, err := pc.Recv()
	if err != nil {
		return err
	}
	if t != proto.THello {
		return fmt.Errorf("expected hello, got %d", t)
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
	enc.Uvarint(proto.Version)
	enc.Str("test")
	enc.Str("test")
	enc.Str(root)
	enc.Uvarint(0)
	if err := pc.Send(proto.THelloOK, enc.B); err != nil {
		return err
	}
	if err := pc.Flush(); err != nil {
		return err
	}
	if compression > 0 {
		if err := pc.EnableCompression(compression); err != nil {
			return err
		}
	}
	if t, _, err = pc.Recv(); err != nil {
		return err
	} else if t != proto.TScan {
		return fmt.Errorf("expected scan, got %d", t)
	}
	enc.Reset()
	enc.Uvarint(1)
	remoteRoot := scan.Entry{Kind: scan.KindDir, Mode: 0o700, Mtime: 0}
	remoteRoot.Encode(&enc)
	if err := pc.Send(proto.TScanEntries, enc.B); err != nil {
		return err
	}
	enc.Reset()
	enc.Uvarint(1)
	enc.Uvarint(0)
	if err := pc.Send(proto.TScanDone, enc.B); err != nil {
		return err
	}
	if err := pc.Flush(); err != nil {
		return err
	}
	if t, _, err = pc.Recv(); err != nil {
		return err
	} else if t != proto.TMeta {
		return fmt.Errorf("expected final directory metadata, got %d", t)
	}
	// Buffer the operation error until EOF. A final barrier would deadlock
	// without this explicit rejection, which makes the regression immediate.
	enc.Reset()
	enc.Uvarint(0)
	enc.U8(proto.CodeGeneric)
	enc.Str("final directory metadata failed")
	if err := pc.Send(proto.TError, enc.B); err != nil {
		return err
	}
	if t, _, err = pc.Recv(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("expected EOF after final metadata, got message %d: %v", t, err)
	}
	return pc.Flush()
}

func closeTestSSH(t *testing.T) string {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ssh := filepath.Join(t.TempDir(), "ssh")
	script := "#!/bin/sh\nexec '" + strings.ReplaceAll(binary, "'", "'\\''") + "' -test.run '^TestConnCloseServerProcess$'\n"
	if err := os.WriteFile(ssh, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return ssh
}

func TestRunCollectsFinalErrorsAtEOF(t *testing.T) {
	for _, compression := range []int{0, 1} {
		t.Run(fmt.Sprintf("compression=%d", compression), func(t *testing.T) {
			t.Setenv("RIFT_CONN_CLOSE_TEST", "run-final")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			sum, err := Run(ctx, Options{
				Src: t.TempDir(), Dst: transport.Endpoint{Host: "test", Path: t.TempDir()},
				Compress: compression, Conns: 1, Log: io.Discard,
				Transport: transport.Options{SSH: closeTestSSH(t)},
			})
			if err != nil {
				t.Fatal(err)
			}
			if sum.ErrorCount != 1 || len(sum.Errors) != 1 || sum.Errors[0] != "remote: final directory metadata failed" {
				t.Fatalf("lost final server error: %+v", sum)
			}
		})
	}
}

func TestCloseReapsServersAndDrainsUnconfirmedFrames(t *testing.T) {
	for _, mode := range []string{"confirmed", "pending", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			ssh := closeTestSSH(t)
			gate := filepath.Join(dir, "release")
			t.Setenv("RIFT_CONN_CLOSE_TEST", mode)
			t.Setenv("RIFT_CONN_CLOSE_GATE", gate)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dialer := transport.NewDialer(transport.Endpoint{Host: "test"}, transport.Options{SSH: ssh})
			tc, err := dialer.Dial(ctx, true, 1)
			if err != nil {
				t.Fatal(err)
			}
			rc := &rconn{
				id: 1, s: &session{sched: newScheduler()}, pc: proto.NewConn(tc), tc: tc,
				dead: make(chan struct{}), stop: make(chan struct{}),
			}
			if mode != "confirmed" {
				if err := rc.send(proto.TMeta, nil); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() { done <- rc.close() }()
			deadline := time.Now().Add(time.Second)
			for {
				if _, err := os.Stat(gate + ".ready"); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("server did not receive EOF")
				}
				time.Sleep(time.Millisecond)
			}
			select {
			case err := <-done:
				t.Fatalf("connection returned before reaping server: %v", err)
			default:
			}
			if err := os.WriteFile(gate, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if mode == "truncated" {
					if !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatalf("truncated teardown error = %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				} else if mode != "confirmed" && (rc.s.errCount != 1 || rc.s.errs[0] != "remote: final metadata failed") {
					t.Fatalf("lost final server error: %v", rc.s.errs)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("connection did not close after server exit")
			}
			if err := tc.Close(); err != nil && mode != "confirmed" {
				t.Fatal(err)
			}
		})
	}
}
