// Command rift-bench times rift against rsync by pushing a local directory
// to a scratch directory under /tmp on a remote host, across a set of
// realistic scenarios (fresh copy, no-op, touched files, edits, deletes and
// a large-file delta).
//
// The source directory is never modified: scenarios that change files work
// on a local scratch copy (cloned where the filesystem supports it). The
// remote scratch directory is removed at the end unless -keep is given.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

const usage = `rift-bench - benchmark rift against rsync

Usage:
  rift-bench [FLAGS] SRC [USER@]HOST [-- SYNC-ARGS...]

Pushes SRC into a fresh scratch directory under /tmp on HOST with both tools
and prints a timing table. SRC itself is never modified; scenarios that
change files run against a local scratch copy. Both tools always get
"-a --delete"; SYNC-ARGS (e.g. filters) are appended for both.

Scenarios (run in this order, each -runs times; median is reported):
  fresh    copy into empty destinations
  noop     sync again with nothing changed
  touch    change the mtime of -touch files (content unchanged)
  edit     append a line to -edit files
  delete   delete -delete files
  big-new  copy a new -big sized file of random data
  big      overwrite 16 random 4 KiB regions of that file (delta transfer)

By default both tools reuse warm ssh connections, so the numbers compare the
tools rather than ssh handshakes; -cold disables connection reuse for both.

Examples:
  rift-bench ~/code/myrepo me@server
  rift-bench -runs 5 -only fresh,noop,edit ~/code/myrepo server -- --filter=':- .gitignore'

Flags:
`

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "rift-bench:", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		if ctx.Err() != nil {
			err = errors.New("interrupted")
		}
		fmt.Fprintln(os.Stderr, "rift-bench:", err)
		os.Exit(1)
	}
}

type config struct {
	src, host  string
	syncArgs   []string
	riftArgs   []string
	rsyncArgs  []string
	riftBin    string
	rsyncBin   string
	ssh        []string
	remoteBase string
	runs       int
	only       map[string]bool
	touch      int
	edit       int
	del        int
	big        int64
	cold       bool
	keep       bool
	verify     bool
	markdown   bool
}

var scenarioNames = []string{"fresh", "noop", "touch", "edit", "delete", "big-new", "big"}

func parseFlags(args []string) (*config, error) {
	cfg := &config{}
	fs := flag.NewFlagSet("rift-bench", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), usage)
		fs.PrintDefaults()
	}
	fs.StringVar(&cfg.riftBin, "rift", "", "rift binary (default: rift next to rift-bench, then $PATH)")
	fs.StringVar(&cfg.rsyncBin, "rsync", "rsync", "rsync binary")
	ssh := fs.String("ssh", "", `ssh command for both tools, e.g. "ssh -p 2222"`)
	fs.StringVar(&cfg.remoteBase, "remote-dir", "/tmp", "remote parent directory for the scratch directory")
	fs.IntVar(&cfg.runs, "runs", 3, "runs per scenario")
	only := fs.String("only", strings.Join(scenarioNames, ","), "comma-separated scenarios to run")
	fs.IntVar(&cfg.touch, "touch", 1000, "files to touch in the touch scenario")
	fs.IntVar(&cfg.edit, "edit", 50, "files to edit in the edit scenario")
	fs.IntVar(&cfg.del, "delete", 100, "files to delete per run in the delete scenario")
	big := fs.String("big", "256M", "size of the large file for big-new/big (0 skips them)")
	riftArgs := fs.String("rift-args", "", `extra arguments for rift only, e.g. "-j 16"`)
	rsyncArgs := fs.String("rsync-args", "", "extra arguments for rsync only")
	fs.BoolVar(&cfg.cold, "cold", false, "no ssh connection reuse: every run pays full ssh handshakes")
	fs.BoolVar(&cfg.keep, "keep", false, "keep the remote scratch directory")
	fs.BoolVar(&cfg.verify, "verify", true, "check after each scenario that both destinations are identical")
	fs.BoolVar(&cfg.markdown, "markdown", false, "print the results as a markdown table")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	pos := fs.Args()
	for i, a := range pos {
		if a == "--" {
			cfg.syncArgs = pos[i+1:]
			pos = pos[:i]
			break
		}
	}
	if len(pos) != 2 {
		fs.Usage()
		return nil, errors.New("need SRC and HOST")
	}
	cfg.src, cfg.host = pos[0], pos[1]
	if strings.Contains(cfg.host, ":") {
		return nil, errors.New("HOST must not contain a path: rift-bench creates its own scratch directory under -remote-dir")
	}
	fi, err := os.Stat(cfg.src)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", cfg.src)
	}
	if cfg.src, err = filepath.Abs(cfg.src); err != nil {
		return nil, err
	}

	cfg.ssh = strings.Fields(*ssh)
	cfg.riftArgs = strings.Fields(*riftArgs)
	cfg.rsyncArgs = strings.Fields(*rsyncArgs)
	if cfg.runs < 1 {
		return nil, errors.New("-runs must be at least 1")
	}
	if cfg.big, err = parseSize(*big); err != nil {
		return nil, fmt.Errorf("-big: %w", err)
	}
	cfg.only = map[string]bool{}
	for _, s := range strings.Split(*only, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if !slices.Contains(scenarioNames, s) {
			return nil, fmt.Errorf("unknown scenario %q (have %s)", s, strings.Join(scenarioNames, ", "))
		}
		cfg.only[s] = true
	}
	if cfg.big == 0 {
		delete(cfg.only, "big-new")
		delete(cfg.only, "big")
	}
	if len(cfg.only) == 0 {
		return nil, errors.New("no scenarios selected")
	}
	if cfg.riftBin == "" {
		cfg.riftBin = findRift()
	}
	return cfg, nil
}

func findRift() string {
	if self, err := os.Executable(); err == nil {
		p := filepath.Join(filepath.Dir(self), "rift")
		if fi, err := os.Stat(p); err == nil && fi.Mode().IsRegular() {
			return p
		}
	}
	return "rift"
}

func parseSize(s string) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	mult := int64(1)
	if n := len(s); n > 0 {
		switch s[n-1] {
		case 'K':
			mult = 1 << 10
		case 'M':
			mult = 1 << 20
		case 'G':
			mult = 1 << 30
		}
		if mult > 1 {
			s = s[:n-1]
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("bad size %q", s)
	}
	return v * mult, nil
}
