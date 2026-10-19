package main

import (
	"context"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/shxntanu/rift/internal/client"
	"github.com/shxntanu/rift/internal/server"
	"github.com/shxntanu/rift/internal/transport"
	"golang.org/x/sys/unix"
)

func TestMain(m *testing.M) {
	if ready := os.Getenv("RIFT_TEST_CLEANUP_STUB"); ready != "" {
		signal.Ignore(syscall.SIGTERM)
		if err := os.WriteFile(ready, nil, 0600); err != nil {
			os.Exit(1)
		}
		time.Sleep(time.Hour)
		os.Exit(0)
	}
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve":
			if err := server.Serve(stdio{}); err != nil {
				os.Exit(1)
			}
			os.Exit(0)
		case "__agent":
			if build := os.Getenv("RIFT_TEST_AGENT_BUILD"); build != "" {
				buildID = build
			}
			idle := agentIdle
			if os.Getenv("RIFT_TEST_AGENT") == "1" {
				idle = 250 * time.Millisecond
			}
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			if err := serveAgentFor(ctx, idle); err != nil {
				os.Exit(1)
			}
			os.Exit(0)
		}
	}
	os.Exit(m.Run())
}

func TestAgentStartupLockCancellation(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "rift-agent-lock-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(home)
	t.Setenv("HOME", home)
	dir, err := agentDir()
	if err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(filepath.Join(dir, "agent.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveAgentFor(ctx, agentIdle) }()
	select {
	case err := <-done:
		if err != context.DeadlineExceeded {
			t.Fatalf("lock wait returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent ignored cancellation while waiting for its startup lock")
	}
}

func TestDefaultSyncDoesNotStartAgent(t *testing.T) {
	for _, args := range [][]string{nil, {"--no-persist"}, {"--persist", "--no-persist"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			src, dst := t.TempDir(), t.TempDir()
			if err := os.WriteFile(filepath.Join(src, "file"), []byte("one shot"), 0600); err != nil {
				t.Fatal(err)
			}
			args = append(append([]string{"--quiet"}, args...), src+"/", dst)
			if code := run(args); code != 0 {
				t.Fatalf("sync exited with %d", code)
			}
			data, err := os.ReadFile(filepath.Join(dst, "file"))
			if err != nil || string(data) != "one shot" {
				t.Fatalf("copy failed: %q, %v", data, err)
			}
			entries, err := os.ReadDir(home)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("one-shot sync created persistent state: %v", entries)
			}
		})
	}
}

func TestAgentConcurrentStartupAndIdleExit(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "rift-agent-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(home)
	t.Setenv("HOME", home)
	t.Setenv("RIFT_TEST_AGENT", "1")
	src, dst := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "file"), []byte("initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	opt := client.Options{Src: src, Dst: transport.Endpoint{Path: dst}, Compress: 1, Conns: 2, Delete: true, Log: io.Discard}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			richOpt := opt
			var progress client.Progress
			richOpt.Rich = true
			richOpt.OnProgress = func(p client.Progress) { progress = p }
			sum, err := runAgentClient(ctx, richOpt)
			if err != nil {
				t.Error(err)
			} else if sum.ErrorCount != 0 {
				t.Error(sum.Errors)
			} else if progress.Phase != "Finishing" || progress.FilesDone != progress.FilesTotal || progress.BytesDone != sum.Bytes {
				t.Errorf("agent lost progress: %+v, summary: %+v", progress, sum)
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		log, _ := os.ReadFile(filepath.Join(home, ".rift/agent.log"))
		t.Log(string(log))
		return
	}
	if err := os.WriteFile(filepath.Join(src, "file"), []byte("saved"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"--persist", "--quiet", src + "/", dst}); code != 0 {
		t.Fatalf("explicit persistent sync exited with %d", code)
	}
	data, err := os.ReadFile(filepath.Join(dst, "file"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "saved" {
		t.Fatalf("stale copy: %s", data)
	}
	path := filepath.Join(home, ".rift/agent.sock")
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("socket permissions: %v", fi.Mode())
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("unix", path, 100*time.Millisecond)
		if err != nil {
			return
		}
		_ = c.Close()
		time.Sleep(1200 * time.Millisecond)
	}
	t.Fatal("agent did not exit after becoming idle")
}
