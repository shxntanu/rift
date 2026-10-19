package process

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCancellationKillsDescendants(t *testing.T) {
	for _, foreground := range []bool{false, true} {
		t.Run(strconv.FormatBool(foreground), func(t *testing.T) {
			testCancellationKillsDescendants(t, foreground)
		})
	}
}

func testCancellationKillsDescendants(t *testing.T, foreground bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pidFile := filepath.Join(t.TempDir(), "child")
	// The background child ignores TERM and holds the output pipes open.
	// Killing only the shell leaves it running after cmd.Wait returns.
	command := CommandContext
	if foreground {
		command = CommandContextForeground
	}
	cmd := command(ctx, "sh", "-c", `trap '' TERM; sleep 60 & echo $! > "$1"; wait`, "sh", pidFile)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	var child int
	for child == 0 {
		data, _ := os.ReadFile(pidFile)
		child, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		if time.Now().After(deadline) {
			t.Fatal("child did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled command succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not release Wait")
	}
	for {
		out, err := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(child)).Output()
		state := strings.TrimSpace(string(out))
		// An orphan may briefly be a zombie awaiting init's reap. It must
		// have exited, rather than being left sleeping with inherited pipes.
		if (err != nil && state == "") || strings.HasPrefix(state, "Z") {
			break
		}
		if time.Now().After(deadline) {
			_ = syscall.Kill(child, syscall.SIGKILL)
			t.Fatalf("descendant %d survived cancellation: %s", child, state)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCanceledCommandDoesNotStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := CommandContext(ctx, "sh", "-c", "exit 0")
	if err := cmd.Run(); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context canceled", err)
	}
	if cmd.Process != nil {
		t.Fatal("started a canceled command")
	}
	if err := Kill(cmd); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("kill unstarted command: %v", err)
	}
}

func TestNotifyContextRestoresSignals(t *testing.T) {
	if os.Getenv("RIFT_TEST_SIGNALS") == "1" {
		ctx, stop := NotifyContext(context.Background())
		defer stop()
		_ = syscall.Kill(os.Getpid(), syscall.SIGHUP)
		<-ctx.Done()
		// Calling stop explicitly also waits for signal.NotifyContext's
		// internal goroutine before sending the second signal.
		stop()
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		time.Sleep(time.Second)
		os.Exit(1)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := CommandContext(ctx, self, "-test.run=^TestNotifyContextRestoresSignals$")
	cmd.Env = append(os.Environ(), "RIFT_TEST_SIGNALS=1")
	err = cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGTERM {
		t.Fatalf("second signal did not terminate process: %v", err)
	}
}
