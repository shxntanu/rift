package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSyncPipelines(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "rift")
	build := exec.Command("go", "build", "-race", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	for _, tc := range []struct {
		direct          bool
		conns, compress int
	}{
		{false, 1, 0}, {false, 4, 1}, {true, 1, 1}, {true, 4, 0},
	} {
		t.Run(fmt.Sprintf("direct=%v/conns=%d/zstd=%d", tc.direct, tc.conns, tc.compress), func(t *testing.T) {
			src, dst := t.TempDir(), t.TempDir()
			if err := os.Mkdir(filepath.Join(src, "sub"), 0755); err != nil {
				t.Fatal(err)
			}
			rng := rand.New(rand.NewPCG(1, 2))
			data := make([]byte, 384<<10)
			for i := range data {
				data[i] = byte(rng.Uint32())
			}
			write := func(name string, data []byte) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(src, name), data, 0640); err != nil {
					t.Fatal(err)
				}
			}
			// Cross both the read-ahead byte budget and empty-file slot bound.
			for i := range 180 {
				write(fmt.Sprintf("sub/f%04d", i), data)
			}
			for i := range 1100 {
				write(fmt.Sprintf("empty%04d", i), nil)
			}
			large := bytes.Repeat(data, 32)
			write("large", large)
			write("deleted", []byte("gone later"))
			write("conflict", []byte("file becomes directory"))
			if err := os.Symlink("sub/f0000", filepath.Join(src, "link")); err != nil {
				t.Fatal(err)
			}
			run := func(extra ...string) string {
				t.Helper()
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				args := []string{"--no-persist", "--conns=" + strconv.Itoa(tc.conns), "--compress-level=" + strconv.Itoa(tc.compress), "--delete"}
				if tc.direct {
					args = append(args, "--direct")
				}
				args = append(args, extra...)
				args = append(args, src+"/", dst)
				cmd := exec.CommandContext(ctx, bin, args...)
				cmd.Env = append(os.Environ(), "RIFT_DEBUG=1")
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("sync: %v\n%s", err, out)
				}
				if tc.direct && !strings.Contains(string(out), "direct TCP connected") {
					t.Fatalf("direct silently fell back:\n%s", out)
				}
				return string(out)
			}
			run()
			compareTrees(t, src, dst)
			if out := run(); !strings.Contains(out, "synced 0 files") {
				t.Fatalf("no-op transferred data:\n%s", out)
			}
			// Metadata-only, deltas, recursive replacement, symlinks and deletes.
			stamp := time.Unix(1700000000, 123456000)
			if err := os.Chtimes(filepath.Join(src, "sub/f0001"), stamp, stamp); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(filepath.Join(src, "sub/f0002"), 0600); err != nil {
				t.Fatal(err)
			}
			write("sub/f0003", []byte("small replacement"))
			large[2<<20] ^= 1
			write("large", large)
			if err := os.Remove(filepath.Join(src, "deleted")); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(src, "conflict")); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(src, "conflict"), 0755); err != nil {
				t.Fatal(err)
			}
			write("conflict/child", []byte("child"))
			if err := os.Remove(filepath.Join(src, "link")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("large", filepath.Join(src, "link")); err != nil {
				t.Fatal(err)
			}
			run()
			compareTrees(t, src, dst)
		})
	}
}

func compareTrees(t *testing.T, src, dst string) {
	t.Helper()
	count := 0
	err := filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		a, err := os.Lstat(path)
		if err != nil {
			return err
		}
		b, err := os.Lstat(filepath.Join(dst, rel))
		if err != nil {
			return err
		}
		if a.Mode().Type() != b.Mode().Type() || (a.Mode()&os.ModeSymlink == 0 && a.Mode() != b.Mode()) {
			return fmt.Errorf("%s mode: %v != %v", rel, a.Mode(), b.Mode())
		}
		if !a.ModTime().Equal(b.ModTime()) {
			return fmt.Errorf("%s mtime: %v != %v", rel, a.ModTime(), b.ModTime())
		}
		if a.Mode().IsRegular() {
			hash := func(name string) ([32]byte, error) {
				var sum [32]byte
				f, err := os.Open(name)
				if err != nil {
					return sum, err
				}
				defer func() { _ = f.Close() }()
				h := sha256.New()
				if _, err := io.Copy(h, f); err != nil {
					return sum, err
				}
				copy(sum[:], h.Sum(nil))
				return sum, nil
			}
			x, err := hash(path)
			if err != nil {
				return err
			}
			y, err := hash(filepath.Join(dst, rel))
			if err != nil {
				return err
			}
			if x != y {
				return fmt.Errorf("%s contents differ", rel)
			}
		} else if a.Mode()&os.ModeSymlink != 0 {
			x, err := os.Readlink(path)
			if err != nil {
				return err
			}
			y, err := os.Readlink(filepath.Join(dst, rel))
			if err != nil {
				return err
			}
			if x != y {
				return fmt.Errorf("%s link differs", rel)
			}
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got := 0
	if err := filepath.WalkDir(dst, func(_ string, _ os.DirEntry, err error) error { got++; return err }); err != nil {
		t.Fatal(err)
	}
	if got != count {
		t.Fatalf("extra receiver entries: %d != %d", got, count)
	}
}
