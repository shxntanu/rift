package scan

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Cache keeps directory listings only while native change tracking is healthy.
// Callers serialize Scan, Changed and Close.
type Cache struct {
	root      string
	source    string
	identity  os.FileInfo
	watch     *nativeWatch
	entries   map[string]Entry
	result    *Result
	specs     string
	pending   []string
	uncertain bool
}

// NewCache starts tracking before the first walk, closing the scan/watch gap.
func NewCache(root string) *Cache {
	root, _ = filepath.Abs(root)
	source := root
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return &Cache{root: root, source: root}
	}
	root = resolved
	w, _ := newNativeWatch(root)
	identity, _ := os.Stat(root)
	return &Cache{root: root, source: source, identity: identity, watch: w}
}

// Close releases native watches.
func (c *Cache) Close() {
	if c.watch != nil {
		c.watch.close()
		c.watch = nil
	}
}

// Changed drains native events. Unsupported or unreliable tracking always
// reports a change, so watch mode falls back to periodic full scans.
func (c *Cache) Changed() bool {
	identity, err := os.Stat(c.source)
	if err != nil || c.identity == nil || !os.SameFile(c.identity, identity) {
		c.uncertain = true
		return true
	}
	if c.watch == nil {
		c.result = nil
		return true
	}
	dirs, lost := c.watch.drain()
	if lost || len(dirs) > 0 {
		c.pending = append(c.pending, dirs...)
		c.uncertain = c.uncertain || lost
		return true
	}
	return false
}

// Scan refreshes only changed directory listings. Filters with merge files and
// git membership deliberately use a full walk: changes can affect descendants.
func (c *Cache) Scan(ctx context.Context, filter *Filter) (*Result, error) {
	identity, err := os.Stat(c.source)
	if err != nil {
		return nil, err
	}
	if c.identity == nil || !os.SameFile(c.identity, identity) {
		c.Close()
		resolved, err := filepath.EvalSymlinks(c.source)
		if err != nil {
			return nil, err
		}
		c.root = resolved
		c.identity = identity
		c.watch, _ = newNativeWatch(resolved)
		c.result = nil
	}
	specs := ""
	if filter != nil {
		specs = strings.Join(filter.Specs(), "\x00")
	}
	if specs != c.specs {
		c.result = nil
		c.specs = specs
	}
	if c.watch == nil || filter.HasMerges() {
		return Walk(ctx, c.root, Options{Filter: filter})
	}
	dirs, lost := c.watch.drain()
	dirs = append(dirs, c.pending...)
	lost = lost || c.uncertain
	c.pending = nil
	c.uncertain = false
	if c.entries == nil || c.result == nil || lost {
		c.entries = nil
		// Watches are installed before reading each directory, including new ones.
		r, err := Walk(ctx, c.root, Options{Filter: filter, watch: c.watch.add})
		if err != nil {
			c.result = nil
			return nil, err
		}
		if len(r.Warnings) != 0 {
			c.result = nil
			return r, nil
		}
		c.entries = make(map[string]Entry, len(r.Entries))
		for _, e := range r.Entries {
			c.entries[e.Path] = e
		}
		c.result = r
		return r, nil
	}
	if len(dirs) == 0 {
		return c.result, nil
	}
	// Re-read each dirty directory, recurse only into newly discovered children.
	sort.Strings(dirs)
	seen := map[string]bool{}
	for len(dirs) > 0 {
		if err := ctx.Err(); err != nil {
			c.result = nil
			return nil, err
		}
		dir := dirs[0]
		dirs = dirs[1:]
		if seen[dir] {
			continue
		}
		seen[dir] = true
		old, known := c.entries[dir]
		if !known || old.Kind != KindDir {
			continue
		}
		abs := filepath.Join(c.root, filepath.FromSlash(dir))
		e, ok, err := Lstat(abs, dir)
		if err != nil || !ok || e.Kind != KindDir {
			c.result = nil
			return c.Scan(ctx, filter)
		}
		c.entries[dir] = e
		w := &walker{root: c.root, filter: filter, watch: c.watch.add}
		children, _, err := w.readDir(dirItem{dir: dir}, nil, nil, make([]byte, 4096))
		if err != nil || len(w.warnings) != 0 {
			c.result = nil
			return c.Scan(ctx, filter)
		}
		keep := map[string]bool{}
		for _, child := range children {
			keep[child.Path] = true
		}
		for p := range c.entries {
			if p != "" && parentDir(p) == dir && !keep[p] {
				for q := range c.entries {
					if q == p || strings.HasPrefix(q, p+"/") {
						delete(c.entries, q)
					}
				}
			}
		}
		for _, child := range children {
			previous, exists := c.entries[child.Path]
			if exists && previous.Kind == KindDir && child.Kind != KindDir {
				for q := range c.entries {
					if strings.HasPrefix(q, child.Path+"/") {
						delete(c.entries, q)
					}
				}
			}
			c.entries[child.Path] = child
			if child.Kind == KindDir && (!exists || previous.Kind != KindDir) {
				dirs = append(dirs, child.Path)
			}
		}
	}
	r := &Result{Entries: make([]Entry, 0, len(c.entries))}
	r.Entries = append(r.Entries, c.entries[""])
	for p, e := range c.entries {
		if p != "" {
			r.Entries = append(r.Entries, e)
		}
	}
	c.result = r
	return r, nil
}
