package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shxntanu/rift/internal/scan"
	"github.com/shxntanu/rift/internal/transport"
)

func TestRunSync(t *testing.T) {
	for _, compression := range []int{0, 1} {
		t.Run(fmt.Sprintf("compression=%d", compression), func(t *testing.T) {
			src, dst := t.TempDir(), t.TempDir()
			write := func(rel string, data []byte) {
				t.Helper()
				abs := filepath.Join(src, rel)
				if err := os.MkdirAll(filepath.Dir(abs), 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(abs, data, 0o640); err != nil {
					t.Fatal(err)
				}
			}
			write("nested/deeper/data", []byte("new directories"))
			write("large", bytes.Repeat([]byte("range transfer data\n"), 700000))
			write("was-file", []byte("replace with directory"))
			write("was-directory/child", []byte("replace with link"))
			for i := range 300 {
				write(fmt.Sprintf("small/%03d", i), []byte("small file"))
			}
			if err := os.MkdirAll(filepath.Join(src, "empty/nested"), 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("../../large", filepath.Join(src, "nested/deeper/link")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("large", filepath.Join(src, "was-link")); err != nil {
				t.Fatal(err)
			}
			opt := Options{Src: src, Dst: transport.Endpoint{Path: dst}, Conns: 4,
				Compress: compression, Delete: true, DeltaMin: 128 << 10, Log: io.Discard}
			var progress Progress
			opt.Rich = true
			opt.OnProgress = func(p Progress) { progress = p }
			run := func() *Summary {
				t.Helper()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				sum, err := Run(ctx, opt)
				if err != nil {
					t.Fatal(err)
				}
				if sum.ErrorCount != 0 {
					t.Fatalf("remote errors: %v", sum.Errors)
				}
				if progress.Phase != "Finishing" || progress.FilesDone != progress.FilesTotal ||
					progress.BytesDone != sum.Bytes || progress.BytesTotal != sum.Bytes || progress.Failed != 0 {
					t.Fatalf("incomplete progress: %+v, summary: %+v", progress, sum)
				}
				assertSameTree(t, src, dst)
				return sum
			}
			run()
			if sum := run(); sum.Transferred != 0 || sum.MetaOnly != 0 || sum.Deleted != 0 || sum.Conns != 1 {
				t.Fatalf("no-op summary: %+v", sum)
			}

			// Type replacements must retain their barrier even with warmed workers.
			if err := os.Remove(filepath.Join(src, "was-file")); err != nil {
				t.Fatal(err)
			}
			write("was-file/deep/child", []byte("file became directory"))
			if err := os.RemoveAll(filepath.Join(src, "was-directory")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("large", filepath.Join(src, "was-directory")); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(src, "was-link")); err != nil {
				t.Fatal(err)
			}
			write("was-link", []byte("link became file"))
			if err := os.RemoveAll(filepath.Join(src, "empty")); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(src, "small/000")); err != nil {
				t.Fatal(err)
			}
			// Change a large file without changing its size, exercising delta writes.
			fh, err := os.OpenFile(filepath.Join(src, "large"), os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fh.WriteAt([]byte("edited"), 6<<20); err != nil {
				_ = fh.Close()
				t.Fatal(err)
			}
			if err := fh.Close(); err != nil {
				t.Fatal(err)
			}
			if sum := run(); sum.Deleted != 3 {
				t.Fatalf("expected three deletes, got %+v", sum)
			}
			run()
		})
	}
}

func TestRunDirectoriesOnly(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "one/two/three"), 0o750); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1600000000, 123456789)
	for _, rel := range []string{"one/two/three", "one/two", "one", ""} {
		if err := os.Chtimes(filepath.Join(src, rel), mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sum, err := Run(ctx, Options{Src: src, Dst: transport.Endpoint{Path: dst}, Compress: 1, Conns: 4, Log: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if sum.ErrorCount != 0 {
		t.Fatalf("remote errors: %v", sum.Errors)
	}
	assertSameTree(t, src, dst)
}

func assertSameTree(t *testing.T, src, dst string) {
	t.Helper()
	filter, err := scan.NewFilter(nil)
	if err != nil {
		t.Fatal(err)
	}
	local, err := scan.Walk(context.Background(), src, scan.Options{Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	remote, err := scan.Walk(context.Background(), dst, scan.Options{Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	if len(local.Warnings)+len(remote.Warnings) != 0 {
		t.Fatalf("scan warnings: %v, %v", local.Warnings, remote.Warnings)
	}
	if len(local.Entries) != len(remote.Entries) {
		t.Fatalf("entry counts: local %d, remote %d", len(local.Entries), len(remote.Entries))
	}
	rmap := make(map[string]scan.Entry, len(remote.Entries))
	for _, e := range remote.Entries {
		rmap[e.Path] = e
	}
	for _, e := range local.Entries {
		got, ok := rmap[e.Path]
		// Symlink permissions are not carried by TSymlink; on macOS their
		// creation mode also depends on the process umask.
		if e.Kind == scan.KindSymlink {
			got.Mode = e.Mode
		}
		if !ok || got != e {
			t.Fatalf("entry %q: local %+v, remote %+v", e.Path, e, got)
		}
		if e.Kind == scan.KindFile {
			want, err := os.ReadFile(filepath.Join(src, e.Path))
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(dst, e.Path))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(want, got) {
				t.Fatalf("file contents differ: %s", e.Path)
			}
		}
	}
}
