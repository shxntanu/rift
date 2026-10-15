package scan

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCacheTracksChanges(t *testing.T) {
	root := t.TempDir()
	filter, _ := NewFilter(nil)
	cache := NewCache(root)
	defer cache.Close()
	if cache.watch == nil {
		t.Skip("native watcher unavailable")
	}
	ctx := context.Background()
	check := func(t *testing.T) {
		t.Helper()
		got, err := cache.Scan(ctx, filter)
		if err != nil {
			t.Fatal(err)
		}
		want, err := Walk(ctx, root, Options{Filter: filter})
		if err != nil {
			t.Fatal(err)
		}
		entries := func(r *Result) map[string]Entry {
			m := map[string]Entry{}
			for _, e := range r.Entries {
				m[e.Path] = e
			}
			return m
		}
		if !reflect.DeepEqual(entries(got), entries(want)) {
			t.Fatalf("cached scan differs:\ngot %v\nwant %v", entries(got), entries(want))
		}
	}
	write := func(p, s string) {
		t.Helper()
		p = filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	check(t)
	for _, tc := range []struct {
		name   string
		change func()
	}{
		{"new tree", func() { write("a/b/file", "one"); write("untouched/file", "stable") }},
		{"in-place edit", func() { write("a/b/file", "two") }},
		{"atomic save", func() {
			write("a/b/tmp", "three")
			if err := os.Rename(filepath.Join(root, "a/b/tmp"), filepath.Join(root, "a/b/file")); err != nil {
				t.Fatal(err)
			}
		}},
		{"rename directory", func() {
			if err := os.Rename(filepath.Join(root, "a"), filepath.Join(root, "renamed")); err != nil {
				t.Fatal(err)
			}
		}},
		{"delete tree", func() {
			if err := os.RemoveAll(filepath.Join(root, "renamed")); err != nil {
				t.Fatal(err)
			}
		}},
		{"filter change", func() { filter, _ = NewFilter([]string{"- untouched"}) }},
		{"filter reset", func() { filter, _ = NewFilter(nil) }},
		{"uncertain cache", func() { cache.result = nil; write("fresh/file", "after overflow") }},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.change(); check(t); check(t) })
	}
}

func TestCacheFallbackAndRootReplacement(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	filter, _ := NewFilter(nil)
	cache := NewCache(root)
	defer cache.Close()
	ctx := context.Background()
	if _, err := cache.Scan(ctx, filter); err != nil {
		t.Fatal(err)
	}
	cache.Close() // Native watch limits or unavailable watcher must never reuse entries.
	if err := os.WriteFile(filepath.Join(root, "new"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := cache.Scan(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 2 {
		t.Fatal(res.Entries)
	}
	if err := os.Rename(root, filepath.Join(parent, "old")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	res, err = cache.Scan(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 1 {
		t.Fatal(res.Entries)
	}
}
