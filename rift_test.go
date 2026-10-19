package rift_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shxntanu/rift"
	"golang.org/x/sys/unix"
)

func write(t *testing.T, path string, data []byte, mode fs.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func sameTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		other := filepath.Join(dst, rel)
		a, err := os.Lstat(path)
		if err != nil {
			return err
		}
		b, err := os.Lstat(other)
		if err != nil {
			return err
		}
		if a.Mode() != b.Mode() || !a.ModTime().Equal(b.ModTime()) {
			return fmt.Errorf("%s metadata: %v/%v != %v/%v", rel, a.Mode(), a.ModTime(), b.Mode(), b.ModTime())
		}
		if d.Type()&os.ModeSymlink != 0 {
			x, err := os.Readlink(path)
			if err != nil {
				return err
			}
			y, err := os.Readlink(other)
			if err != nil {
				return err
			}
			if x != y {
				return fmt.Errorf("%s symlink differs", rel)
			}
		} else if !d.IsDir() {
			x, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			y, err := os.ReadFile(other)
			if err != nil {
				return err
			}
			if !bytes.Equal(x, y) {
				return fmt.Errorf("%s contents differ", rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLocalSync(t *testing.T) {
	// No helper executable, git, or SSH should be needed for local sync.
	t.Setenv("PATH", t.TempDir())
	for _, compression := range []int{-1, 0, 4} {
		t.Run(fmt.Sprint(compression), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			src, dst := t.TempDir(), filepath.Join(t.TempDir(), "missing", "destination")
			if err := os.Mkdir(filepath.Join(src, "nested"), 0755); err != nil {
				t.Fatal(err)
			}
			write(t, filepath.Join(src, "nested", "small"), []byte("hello"), 0640)
			write(t, filepath.Join(src, "empty"), nil, 0644)
			data := make([]byte, 5<<20)
			rng := rand.New(rand.NewPCG(1, 2))
			for i := range data {
				data[i] = byte(rng.Uint32())
			}
			write(t, filepath.Join(src, "large"), data, 0644)
			if err := os.Symlink("nested/small", filepath.Join(src, "link")); err != nil {
				t.Fatal(err)
			}
			var last rift.Progress
			var logs bytes.Buffer
			opts := rift.Options{Conns: 4, CompressionLevel: compression, Delete: true, Log: &logs,
				OnProgress: func(p rift.Progress) { last = p }}
			run := func() *rift.Summary {
				t.Helper()
				sum, err := rift.Sync(ctx, src+"/", rift.Endpoint{Path: dst}, opts)
				if err != nil {
					t.Fatal(err)
				}
				sameTree(t, src, dst)
				return sum
			}
			if sum := run(); sum.Transferred != 4 || sum.WireOut == 0 {
				t.Fatalf("unexpected summary: %+v", sum)
			}
			if last.FilesDone != last.FilesTotal || last.BytesDone != int64(len(data)+5) {
				t.Fatalf("incomplete progress: %+v", last)
			}
			if sum := run(); sum.Transferred != 0 {
				t.Fatalf("not a no-op: %+v", sum)
			}
			data[2<<20] ^= 1
			write(t, filepath.Join(src, "large"), data, 0644)
			stamp := time.Unix(1700000000, 123456000)
			if err := os.Chtimes(filepath.Join(src, "large"), stamp, stamp); err != nil {
				t.Fatal(err)
			}
			write(t, filepath.Join(dst, "extra"), []byte("delete"), 0600)
			if sum := run(); sum.Deleted != 1 || sum.Matched == 0 {
				t.Fatalf("delta/deletion missing: %+v", sum)
			}
			if _, err := os.Stat(filepath.Join(dst, "extra")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if logs.Len() != 0 {
				t.Fatalf("unexpected output: %q", logs.String())
			}
		})
	}
}

func TestPathsDryRunAndFilters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	src, parent := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "keep"), []byte("hello"), 0644)
	write(t, filepath.Join(src, "skip.tmp"), []byte("excluded"), 0644)
	for _, slash := range []bool{false, true} {
		t.Run(fmt.Sprint(slash), func(t *testing.T) {
			dst := filepath.Join(parent, fmt.Sprint(slash), "missing")
			source, expected := src, filepath.Join(dst, filepath.Base(src))
			if slash {
				source, expected = src+"/", dst
			}
			opts := rift.Options{DryRun: true, Rules: []string{"- *.tmp"}}
			sum, err := rift.Sync(ctx, source, rift.Endpoint{Path: dst}, opts)
			if err != nil || sum.Transferred != 1 || sum.Bytes != 5 {
				t.Fatalf("dry run: %+v, %v", sum, err)
			}
			if _, err := os.Stat(filepath.Join(parent, fmt.Sprint(slash))); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("dry run created a destination ancestor: %v", err)
			}
			opts.DryRun = false
			if _, err := rift.Sync(ctx, source, rift.Endpoint{Path: dst}, opts); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(expected, "keep")); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(expected, "skip.tmp")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			write(t, filepath.Join(expected, "skip.tmp"), []byte("protected"), 0644)
			opts.Delete = true
			if _, err := rift.Sync(ctx, source, rift.Endpoint{Path: dst}, opts); err != nil {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(filepath.Join(expected, "skip.tmp")); err != nil || string(data) != "protected" {
				t.Fatalf("filter protection: %q, %v", data, err)
			}
		})
	}
	if err := os.Symlink(src, filepath.Join(parent, "alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(src, "missing"), filepath.Join(parent, "dangling")); err != nil {
		t.Fatal(err)
	}
	for _, dst := range []string{src, filepath.Join(src, "child"), filepath.Dir(src), filepath.Join(parent, "alias", "missing"), filepath.Join(parent, "dangling", "nested")} {
		if _, err := rift.Sync(ctx, src+"/", rift.Endpoint{Path: dst}, rift.Options{Delete: true}); err == nil {
			t.Fatalf("accepted overlapping destination %s", dst)
		}
	}
}

func TestRunnerLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r := rift.NewRunner(ctx)
	defer func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	}()
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "file"), []byte("hello"), 0644)
	for i := range 3 {
		sum, err := r.Sync(ctx, src+"/", rift.Endpoint{Path: dst}, rift.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 && sum.Transferred != 0 {
			t.Fatalf("not a no-op: %+v", sum)
		}
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if _, err := r.Sync(ctx, src+"/", rift.Endpoint{Path: dst}, rift.Options{}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	request, stop := context.WithCancel(ctx)
	_, err := r.Sync(request, src+"/", rift.Endpoint{Path: dst}, rift.Options{OnProgress: func(rift.Progress) { stop() }})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if _, err := r.Sync(ctx, src+"/", rift.Endpoint{Path: dst}, rift.Options{}); err != nil {
		t.Fatalf("reuse after cancellation: %v", err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := r.Sync(ctx, src+"/", rift.Endpoint{Path: dst}, rift.Options{OnProgress: func(rift.Progress) {
			select {
			case <-started:
			default:
				close(started)
				<-release
			}
		}})
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close hung")
	}
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, rift.ErrClosed) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Sync hung")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Sync(ctx, src+"/", rift.Endpoint{Path: dst}, rift.Options{}); !errors.Is(err, rift.ErrClosed) {
		t.Fatal(err)
	}
}

func TestPartialFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires filesystem permission enforcement")
	}
	src, dst := t.TempDir(), t.TempDir()
	for _, dir := range []string{filepath.Join(src, "locked"), filepath.Join(dst, "locked")} {
		if err := os.Mkdir(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	write(t, filepath.Join(src, "locked", "file"), []byte("cannot write"), 0644)
	if err := os.Chmod(filepath.Join(dst, "locked"), 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(dst, "locked"), 0700) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sum, err := rift.Sync(ctx, src+"/", rift.Endpoint{Path: dst}, rift.Options{})
	var partial *rift.PartialError
	if !errors.As(err, &partial) || sum == nil || sum.ErrorCount == 0 || partial.Count != sum.ErrorCount {
		t.Fatalf("partial result: %+v, %v", sum, err)
	}
}

func TestUmaskPreserved(t *testing.T) {
	if os.Getenv("RIFT_TEST_UMASK") == "" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestUmaskPreserved$", "-test.timeout=20s")
		cmd.Env = append(os.Environ(), "RIFT_TEST_UMASK=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("umask subprocess: %v\n%s", err, out)
		}
		return
	}
	previous := unix.Umask(0077)
	defer unix.Umask(previous)
	src, dst := t.TempDir(), filepath.Join(t.TempDir(), "new")
	if err := os.Mkdir(filepath.Join(src, "nested"), 0755); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(src, "nested", "file"), []byte("permissions"), 0664)
	if err := os.Chmod(filepath.Join(src, "nested"), 0550); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(filepath.Join(src, "nested"), 0750)
		_ = os.Chmod(filepath.Join(dst, "nested"), 0750)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rift.Sync(ctx, src+"/", rift.Endpoint{Path: dst}, rift.Options{}); err != nil {
		t.Fatal(err)
	}
	if got := unix.Umask(0077); got != 0077 {
		t.Fatalf("library changed umask to %#o", got)
	}
	sameTree(t, src, dst)
}

func TestRemoteDeployment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dir := t.TempDir()
	bin := filepath.Join(dir, "receiver")
	build := exec.CommandContext(ctx, "go", "build", "-race", "-o", bin, "./cmd/rift")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	ssh := filepath.Join(dir, "ssh")
	// Execute the actual deployment/bootstrap scripts locally, avoiding an SSH
	// daemon dependency while exercising platform discovery and uploaded bytes.
	write(t, ssh, []byte("#!/bin/sh\nfor arg do :; done\nexec /bin/sh -c \"$arg\"\n"), 0700)
	t.Setenv("HOME", dir)
	src, dst := t.TempDir(), filepath.Join(dir, "destination")
	write(t, filepath.Join(src, "file"), []byte("remote"), 0644)
	opts := rift.Options{SSH: rift.SSHOptions{Command: ssh, ServerBinaries: map[string]string{runtime.GOOS + "/" + runtime.GOARCH: bin}}}
	r := rift.NewRunner(ctx)
	defer func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	}()
	for i := range 2 {
		sum, err := r.Sync(ctx, src+"/", rift.Endpoint{Host: "test", Path: dst}, opts)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 && sum.Transferred != 0 {
			t.Fatalf("remote no-op: %+v", sum)
		}
		sameTree(t, src, dst)
	}
	installed, err := filepath.Glob(filepath.Join(dir, ".rift", "bin", "rift-lib-*"))
	if err != nil || len(installed) != 1 {
		t.Fatalf("deployment: %v, %v", installed, err)
	}
	actual, err := os.ReadFile(installed[0])
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(bin)
	if err != nil || !bytes.Equal(actual, want) {
		t.Fatalf("wrong deployed executable: %v", err)
	}
	// Changing the supplied artifact set must invalidate retained sessions and
	// select a new content identity, without depending on the importing binary.
	opts.SSH.ServerBinaries["other/arch"] = bin
	if _, err := r.Sync(ctx, src+"/", rift.Endpoint{Host: "test", Path: dst}, opts); err != nil {
		t.Fatal(err)
	}
	installed, err = filepath.Glob(filepath.Join(dir, ".rift", "bin", "rift-lib-*"))
	if err != nil || len(installed) != 2 {
		t.Fatalf("stale receiver identity: %v, %v", installed, err)
	}
}

func TestInvalidOptions(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	for _, tc := range []struct {
		name string
		opts rift.Options
		dst  rift.Endpoint
	}{
		{"conns", rift.Options{Conns: 33}, rift.Endpoint{Path: dst}},
		{"compression", rift.Options{CompressionLevel: 5}, rift.Endpoint{Path: dst}},
		{"delta", rift.Options{DeltaMin: -1}, rift.Endpoint{Path: dst}},
		{"window", rift.Options{ModifyWindow: -1}, rift.Endpoint{Path: dst}},
		{"host", rift.Options{}, rift.Endpoint{Host: "-oProxyCommand=bad", Path: dst}},
		{"receiver", rift.Options{}, rift.Endpoint{Host: "test", Path: dst}},
		{"filter", rift.Options{Rules: []string{"invalid rule"}}, rift.Endpoint{Path: dst}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := rift.Sync(context.Background(), src+"/", tc.dst, tc.opts); err == nil {
				t.Fatal("accepted invalid options")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := rift.Sync(ctx, src+"/", rift.Endpoint{Path: dst}, rift.Options{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestExternalConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	write(t, filepath.Join(dir, "go.mod"), []byte("module example.com/consumer\n\ngo 1.27.2\n\nrequire github.com/shxntanu/rift v0.0.0\nreplace github.com/shxntanu/rift => "+root+"\n"), 0600)
	write(t, filepath.Join(dir, "main.go"), []byte(`package main
import (
 "context"
 "os"
 "github.com/shxntanu/rift"
)
func main() {
 os.Setenv("PATH", "")
 _, err := rift.Sync(context.Background(), os.Args[1]+"/", rift.Endpoint{Path: os.Args[2]}, rift.Options{})
 if err != nil { panic(err) }
}
`), 0600)
	build := exec.CommandContext(ctx, "go", "build", "-mod=mod", "-race", "-o", filepath.Join(dir, "consumer"), ".")
	build.Dir = dir
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("external build: %v\n%s", err, out)
	}
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "hello"), []byte("external import"), 0644)
	run := exec.CommandContext(ctx, filepath.Join(dir, "consumer"), src, dst)
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("external sync: %v\n%s", err, out)
	}
	sameTree(t, src, dst)
	deps := exec.CommandContext(ctx, "go", "list", "-mod=mod", "-deps", ".")
	deps.Dir = dir
	out, err := deps.Output()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "bubbletea") || strings.Contains(string(out), "lipgloss") {
		t.Fatal("library imports terminal UI")
	}
}
