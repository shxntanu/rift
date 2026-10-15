package main

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/shxntanu/rift/internal/client"
	"github.com/shxntanu/rift/internal/server"
	"github.com/shxntanu/rift/internal/transport"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve":
			if err := server.Serve(stdio{}); err != nil {
				os.Exit(1)
			}
			os.Exit(0)
		case "__agent":
			idle := agentIdle
			if os.Getenv("RIFT_TEST_AGENT") == "1" {
				idle = 250 * time.Millisecond
			}
			if err := serveAgentFor(context.Background(), idle); err != nil {
				os.Exit(1)
			}
			os.Exit(0)
		}
	}
	os.Exit(m.Run())
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
			sum, err := runAgentClient(ctx, opt)
			if err != nil {
				t.Error(err)
			} else if sum.ErrorCount != 0 {
				t.Error(sum.Errors)
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
	sum, err := runAgentClient(ctx, opt)
	if err != nil {
		t.Fatal(err)
	}
	if sum.ErrorCount != 0 {
		t.Fatal(sum.Errors)
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
