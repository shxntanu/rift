package scan

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"

	"golang.org/x/sys/unix"
)

// Options controls a walk.
type Options struct {
	Filter  *Filter
	Workers int // 0 means a CPU-derived default
}

// Result is the outcome of a walk.
type Result struct {
	Entries  []Entry // Entries[0] is the root itself (Path "")
	Warnings []error // unreadable entries that were skipped
	// Ctx maps each walked directory to its merge-rule context. It is only
	// populated when the filter has per-directory merge rules.
	Ctx map[string]*MergeCtx
}

// CtxFor returns the merge context governing rel, using the nearest walked
// ancestor directory.
func (r *Result) CtxFor(rel string) *MergeCtx {
	if r == nil || r.Ctx == nil {
		return nil
	}
	for d := parentDir(rel); ; d = parentDir(d) {
		if c, ok := r.Ctx[d]; ok {
			return c
		}
		if d == "" {
			return nil
		}
	}
}

func parentDir(rel string) string {
	for i := len(rel) - 1; i >= 0; i-- {
		if rel[i] == '/' {
			return rel[:i]
		}
	}
	return ""
}

type dirItem struct {
	dir string
	ctx *MergeCtx
}

// Walk lists everything under root in parallel. Directories are distributed
// across workers through a shared LIFO stack, so idle workers immediately
// pick up subdirectories discovered by busy ones. Each directory is read
// through its own fd and children are stat'ed with fstatat, avoiding repeated
// path resolution from the root.
func Walk(ctx context.Context, root string, opt Options) (*Result, error) {
	var st unix.Stat_t
	if err := unix.Stat(root, &st); err != nil {
		return nil, &os.PathError{Op: "stat", Path: root, Err: err}
	}
	rootEntry := Entry{}
	if !fromStat(&st, &rootEntry) || rootEntry.Kind != KindDir {
		return nil, fmt.Errorf("%s: not a directory", root)
	}

	workers := opt.Workers
	if workers <= 0 {
		workers = min(max(runtime.NumCPU()*2, 4), 64)
	}
	w := &walker{root: root, filter: opt.Filter, stack: []dirItem{{dir: ""}}}
	w.cond = sync.NewCond(&w.mu)
	if opt.Filter.HasMerges() {
		w.ctxs = map[string]*MergeCtx{}
		w.mergeNames = opt.Filter.MergeFiles()
	}

	results := make([][]Entry, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = w.run(ctx)
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	n := 1
	for _, r := range results {
		n += len(r)
	}
	all := make([]Entry, 0, n)
	all = append(all, rootEntry)
	for _, r := range results {
		all = append(all, r...)
	}
	return &Result{Entries: all, Warnings: w.warnings, Ctx: w.ctxs}, nil
}

type walker struct {
	root       string
	filter     *Filter
	mergeNames []string

	mu       sync.Mutex
	cond     *sync.Cond
	stack    []dirItem
	active   int
	warnings []error
	ctxs     map[string]*MergeCtx
}

func (w *walker) run(ctx context.Context) []Entry {
	out := make([]Entry, 0, 1024)
	var subdirs []dirItem
	linkBuf := make([]byte, 4096)
	for {
		w.mu.Lock()
		for len(w.stack) == 0 && w.active > 0 {
			w.cond.Wait()
		}
		if len(w.stack) == 0 || ctx.Err() != nil {
			w.mu.Unlock()
			w.cond.Broadcast()
			return out
		}
		it := w.stack[len(w.stack)-1]
		w.stack = w.stack[:len(w.stack)-1]
		w.active++
		w.mu.Unlock()

		subdirs = subdirs[:0]
		var err error
		out, subdirs, err = w.readDir(it, out, subdirs, linkBuf)

		w.mu.Lock()
		w.active--
		w.stack = append(w.stack, subdirs...)
		if err != nil {
			w.warnings = append(w.warnings, err)
		}
		if len(subdirs) > 0 || (w.active == 0 && len(w.stack) == 0) {
			w.cond.Broadcast()
		}
		w.mu.Unlock()
	}
}

func (w *walker) readDir(it dirItem, out []Entry, subdirs []dirItem, linkBuf []byte) ([]Entry, []dirItem, error) {
	dir := it.dir
	abs := w.root
	if dir != "" {
		abs = w.root + "/" + dir
	}
	fd, err := unix.Open(abs, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return out, subdirs, &os.PathError{Op: "open", Path: abs, Err: err}
	}
	f := os.NewFile(uintptr(fd), abs)
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		return out, subdirs, err
	}

	mctx := it.ctx
	if w.mergeNames != nil {
		mctx = w.filter.EnterDir(it.ctx, dir, func(name string) ([]byte, bool) {
			if !slices.Contains(names, name) {
				return nil, false
			}
			data, err := os.ReadFile(abs + "/" + name)
			return data, err == nil
		})
		w.mu.Lock()
		w.ctxs[dir] = mctx
		w.mu.Unlock()
	}

	var st unix.Stat_t
	for _, name := range names {
		if IsTemp(name) {
			continue
		}
		rel := name
		if dir != "" {
			rel = dir + "/" + name
		}
		if err := unix.Fstatat(fd, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			if !errors.Is(err, unix.ENOENT) {
				w.addWarning(&os.PathError{Op: "lstat", Path: abs + "/" + name, Err: err})
			}
			continue
		}
		e := Entry{Path: rel}
		if !fromStat(&st, &e) {
			continue
		}
		if w.filter.Excluded(rel, e.Kind == KindDir, mctx) {
			continue
		}
		switch e.Kind {
		case KindSymlink:
			n, err := unix.Readlinkat(fd, name, linkBuf)
			if err != nil {
				w.addWarning(&os.PathError{Op: "readlink", Path: abs + "/" + name, Err: err})
				continue
			}
			e.Link = string(linkBuf[:n])
		case KindDir:
			subdirs = append(subdirs, dirItem{dir: rel, ctx: mctx})
		}
		out = append(out, e)
	}
	return out, subdirs, nil
}

func (w *walker) addWarning(err error) {
	w.mu.Lock()
	w.warnings = append(w.warnings, err)
	w.mu.Unlock()
}
