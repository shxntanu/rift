package scan

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"runtime"
	"sort"
	"strings"
	"sync"
)

// GitList lists what git considers the working tree under root: tracked
// files plus untracked files that are not ignored. This gives exact
// .gitignore semantics (nested ignores, global excludes, negations) for free.
func GitList(ctx context.Context, root string, filter *Filter) (*Result, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", root, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-files in %s: %w: %s", root, err, strings.TrimSpace(stderr.String()))
	}

	rootEntry, ok, err := Lstat(root, "")
	if err != nil {
		return nil, err
	}
	if !ok || rootEntry.Kind != KindDir {
		return nil, fmt.Errorf("%s: not a directory", root)
	}

	dirExcluded := map[string]bool{}
	var dirExcl func(d string) bool
	dirExcl = func(d string) bool {
		if d == "." || d == "" {
			return false
		}
		if v, ok := dirExcluded[d]; ok {
			return v
		}
		v := dirExcl(path.Dir(d)) || filter.Excluded(d, true, nil)
		dirExcluded[d] = v
		return v
	}

	seen := map[string]struct{}{}
	var paths []string
	for _, p := range bytes.Split(out, []byte{0}) {
		if len(p) == 0 {
			continue
		}
		s := string(p)
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		if dirExcl(path.Dir(s)) || filter.Excluded(s, false, nil) {
			continue
		}
		paths = append(paths, s)
		for d := path.Dir(s); d != "." && d != ""; d = path.Dir(d) {
			if _, ok := seen[d]; ok {
				break
			}
			seen[d] = struct{}{}
			paths = append(paths, d)
		}
	}
	sort.Strings(paths)

	entries := make([]Entry, len(paths))
	keep := make([]bool, len(paths))
	var mu sync.Mutex
	var warnings []error
	workers := min(max(runtime.NumCPU()*2, 4), 64)
	var wg sync.WaitGroup
	next := make(chan int, workers*4)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				e, ok, err := Lstat(root+"/"+paths[i], paths[i])
				if err != nil {
					if !errors.Is(err, os.ErrNotExist) {
						mu.Lock()
						warnings = append(warnings, err)
						mu.Unlock()
					}
					continue
				}
				entries[i], keep[i] = e, ok
			}
		}()
	}
	for i := range paths {
		next <- i
	}
	close(next)
	wg.Wait()

	res := &Result{Entries: make([]Entry, 0, len(paths)+1), Warnings: warnings}
	res.Entries = append(res.Entries, rootEntry)
	for i, ok := range keep {
		if ok {
			res.Entries = append(res.Entries, entries[i])
		}
	}
	return res, nil
}

// GitIgnored returns the subset of paths that git would ignore under root.
// Directory paths should carry a trailing '/'.
func GitIgnored(ctx context.Context, root string, paths []string) (map[string]bool, error) {
	ignored := map[string]bool{}
	if len(paths) == 0 {
		return ignored, nil
	}
	cmd := exec.CommandContext(ctx, "git", "-C", root, "check-ignore", "-z", "--stdin", "--no-index")
	var in bytes.Buffer
	for _, p := range paths {
		in.WriteString(p)
		in.WriteByte(0)
	}
	cmd.Stdin = &in
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != 1 {
			return nil, fmt.Errorf("git check-ignore: %w: %s", err, strings.TrimSpace(stderr.String()))
		}
	}
	for _, p := range bytes.Split(out, []byte{0}) {
		if len(p) > 0 {
			ignored[strings.TrimSuffix(string(p), "/")] = true
		}
	}
	return ignored, nil
}
