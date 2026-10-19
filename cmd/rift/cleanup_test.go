package main

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"
)

func TestPersistentSessionSelection(t *testing.T) {
	out := `
  101 /usr/local/bin/rift __agent
  102 /old builds/rift-v0.1.0 __agent
  103 ssh: /Users/name with spaces/.rift/cm/hash-0 [mux]
  104 /usr/local/bin/rift serve
  105 /usr/local/bin/rift --persist ./ host:dest
  106 /usr/bin/something-else __agent
  107 ssh: /tmp/unrelated.sock [mux]
  108 sh -c rift __agent
  109 /usr/local/bin/rift --help
  110 ssh: /tmp/.rift/cm-elsewhere/hash-0 [mux]
  bad /usr/local/bin/rift __agent
`
	var pids []int
	for _, session := range parsePersistentSessions(out) {
		pids = append(pids, session.pid)
	}
	if !slices.Equal(pids, []int{101, 102, 103}) {
		t.Fatalf("selected unrelated processes or missed old builds: %v", pids)
	}
}

func TestCleanupStopsOlderAgents(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "rift-cleanup-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	t.Setenv("HOME", dir)
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var pids []int
	var exits []chan error
	for _, build := range []string{"old-v1", "old-v2"} {
		path := filepath.Join(dir, "rift-"+build)
		if err := os.Symlink(self, path); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(path, "__agent")
		cmd.Env = append(os.Environ(), "RIFT_TEST_AGENT_BUILD="+build, "RIFT_TEST_AGENT=0")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		pids = append(pids, cmd.Process.Pid)
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		exits = append(exits, done)
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		for {
			c, err := dialAgent(ctx, filepath.Join(dir, ".rift", "agent.sock"), build)
			if err == nil {
				c.Close()
				break
			}
			select {
			case <-done:
				t.Fatalf("agent %s exited during startup", build)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	// Both builds stay alive even though only the newer socket is reachable.
	sessions, err := persistentSessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var targets []persistentSession
	for _, session := range sessions {
		if slices.Contains(pids, session.pid) {
			targets = append(targets, session)
		}
	}
	if len(targets) != 2 {
		t.Fatalf("missed an agent with an unlinked socket: %v", targets)
	}
	// Only test children are ever passed to the signal function.
	if err := stopPersistentSessions(ctx, targets); err != nil {
		t.Fatal(err)
	}
	for _, done := range exits {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("agent did not stop gracefully: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestCleanupSocketsAndDryRun(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "rift-cleanup-sockets-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	t.Setenv("HOME", dir)
	root := filepath.Join(dir, ".rift")
	cm := filepath.Join(root, "cm")
	if err := os.MkdirAll(cm, 0700); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(root, "agent.sock"), filepath.Join(cm, "stale-master")} {
		ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		ln.SetUnlinkOnClose(false)
		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{filepath.Join(root, "agent.lock"), filepath.Join(root, "agent.log"), filepath.Join(cm, "keep-file")} {
		if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// A private ps fixture prevents this CLI test from finding real sessions.
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "ps"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if code := run([]string{"cleanup", "--dry-run", "--quiet"}); code != 0 {
		t.Fatalf("dry run exited with %d", code)
	}
	sockets, err := sessionSockets(root)
	if err != nil || len(sockets) != 2 {
		t.Fatalf("dry run removed sockets: %v, %v", sockets, err)
	}
	for range 2 {
		if code := run([]string{"cleanup", "--quiet"}); code != 0 {
			t.Fatalf("cleanup exited with %d", code)
		}
	}
	for _, path := range sockets {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("socket survived cleanup: %s, %v", path, err)
		}
	}
	if data, err := os.ReadFile(filepath.Join(cm, "keep-file")); err != nil || string(data) != "keep" {
		t.Fatalf("cleanup removed a regular file: %q, %v", data, err)
	}
}

func TestCleanupRejectsTransferArguments(t *testing.T) {
	for _, args := range [][]string{{"cleanup", "src", "dst"}, {"cleanup", "--persist"}} {
		if code := run(args); code == 0 {
			t.Fatalf("accepted conflicting arguments: %v", args)
		}
	}
}

func TestCleanupKillsUnresponsiveSession(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(t.TempDir(), "ready")
	cmd := &exec.Cmd{Path: self, Args: []string{"ssh: /tmp/.rift/cm/test-master [mux]"},
		Env: append(os.Environ(), "RIFT_TEST_CLEANUP_STUB="+ready)}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := stopPersistentSessions(ctx, []persistentSession{{pid: cmd.Process.Pid, command: cmd.Args[0]}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if exit, ok := err.(*exec.ExitError); !ok || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			t.Fatalf("expected SIGKILL for an unresponsive master, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
