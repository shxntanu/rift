package transport

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func testSSH(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ssh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRetainedDialCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A live agent must not keep dialing after its request is canceled.
	d := NewDialer(Endpoint{Host: "test"}, Options{SSH: testSSH(t, "exec sleep 60")})
	done := make(chan error, 1)
	go func() {
		_, err := d.DialRetained(ctx, context.Background(), true, 1)
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("dial error = %v, want canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled request left a pending dial")
	}
}

func TestRetainedConnectionSurvivesRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lifetime, stop := context.WithCancel(context.Background())
	defer stop()
	d := NewDialer(Endpoint{Host: "test"}, Options{SSH: testSSH(t, "printf 'RIFT!'; exec cat")})
	c, err := d.DialRetained(ctx, lifetime, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cancel()
	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := c.Read(b[:]); err != nil || b[0] != 'x' {
		t.Fatalf("retained stream closed with request: %q, %v", b, err)
	}
	stop()
	if err := c.Close(); err == nil {
		t.Fatal("lifetime cancellation did not terminate server")
	}
	if c.cmd.ProcessState == nil || !c.cmd.ProcessState.Exited() && c.cmd.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatal("server was not reaped")
	}
}

func TestCloseKillsAndReapsUnresponsiveServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	d := NewDialer(Endpoint{Host: "test"}, Options{SSH: testSSH(t, "trap '' TERM; printf 'RIFT!'; exec sleep 60")})
	c, err := d.Dial(ctx, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err == nil {
		t.Fatal("unresponsive server did not require a kill")
	}
	if c.cmd.ProcessState == nil || c.cmd.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("server was not killed and reaped: %v", c.cmd.ProcessState)
	}
}
