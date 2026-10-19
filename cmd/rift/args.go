package main

import (
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
)

// config is the parsed command line.
type config struct {
	conns          int
	compress       int
	verbose        int
	quiet          bool
	rich           bool
	dryRun         bool
	checksum       bool
	del            bool
	stats          bool
	wholeFile      bool
	sizeOnly       bool
	update         bool
	ignoreExisting bool
	gitignore      bool
	noFastCipher   bool
	persist        bool
	direct         bool
	modifyWindow   int64
	deltaMin       int64
	rules          []string
	ssh            string
	pos            []string
	help           bool
	version        bool
}

type optKind uint8

const (
	optFlag optKind = iota
	optArg
)

type option struct {
	kind optKind
	set  func(c *config, v string) error
}

func flag(fn func(c *config)) option {
	return option{kind: optFlag, set: func(c *config, _ string) error { fn(c); return nil }}
}

func arg(fn func(c *config, v string) error) option {
	return option{kind: optArg, set: fn}
}

var noop = flag(func(*config) {})

func intArg(dst func(c *config) *int) option {
	return arg(func(c *config, v string) error {
		n, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		*dst(c) = n
		return nil
	})
}

func unsupported(kind optKind) option {
	return option{kind: kind, set: func(*config, string) error {
		return fmt.Errorf("not supported by rift yet")
	}}
}

// Options rsync users rely on are accepted; ones that only affect things
// rift always does (recursion, links, perms, times) are no-ops.
var longOpts = map[string]option{
	"archive":         noop,
	"recursive":       noop,
	"links":           noop,
	"perms":           noop,
	"times":           noop,
	"group":           noop,
	"owner":           noop,
	"devices":         noop,
	"specials":        noop,
	"executability":   noop,
	"omit-dir-times":  noop,
	"numeric-ids":     noop,
	"human-readable":  noop,
	"partial":         noop,
	"inplace":         noop,
	"mkpath":          noop,
	"sparse":          noop,
	"no-motd":         noop,
	"progress":        noop, // progress is shown automatically on a terminal
	"partial-dir":     arg(func(*config, string) error { return nil }),
	"timeout":         arg(func(*config, string) error { return nil }),
	"contimeout":      arg(func(*config, string) error { return nil }),
	"info":            arg(func(*config, string) error { return nil }),
	"verbose":         flag(func(c *config) { c.verbose++ }),
	"itemize-changes": flag(func(c *config) { c.verbose++ }),
	"quiet":           flag(func(c *config) { c.quiet = true }),
	"dry-run":         flag(func(c *config) { c.dryRun = true }),
	"checksum":        flag(func(c *config) { c.checksum = true }),
	"ignore-times":    flag(func(c *config) { c.checksum = true }),
	"compress":        flag(func(c *config) { c.compress = max(c.compress, 1) }),
	"no-compress":     flag(func(c *config) { c.compress = 0 }),
	"compress-level":  intArg(func(c *config) *int { return &c.compress }),
	"delete":          flag(func(c *config) { c.del = true }),
	"del":             flag(func(c *config) { c.del = true }),
	"delete-before":   flag(func(c *config) { c.del = true }),
	"delete-during":   flag(func(c *config) { c.del = true }),
	"delete-delay":    flag(func(c *config) { c.del = true }),
	"delete-after":    flag(func(c *config) { c.del = true }),
	"stats":           flag(func(c *config) { c.stats = true }),
	"whole-file":      flag(func(c *config) { c.wholeFile = true }),
	"no-whole-file":   flag(func(c *config) { c.wholeFile = false }),
	"size-only":       flag(func(c *config) { c.sizeOnly = true }),
	"update":          flag(func(c *config) { c.update = true }),
	"ignore-existing": flag(func(c *config) { c.ignoreExisting = true }),
	"modify-window": arg(func(c *config, v string) error {
		n, err := strconv.ParseInt(v, 10, 64)
		c.modifyWindow = n
		return err
	}),
	"exclude": arg(func(c *config, v string) error { c.rules = append(c.rules, "- "+v); return nil }),
	"include": arg(func(c *config, v string) error { c.rules = append(c.rules, "+ "+v); return nil }),
	"filter":  arg(func(c *config, v string) error { c.rules = append(c.rules, v); return nil }),
	"exclude-from": arg(func(c *config, v string) error {
		c.rules = append(c.rules, ".- "+v)
		return nil
	}),
	"include-from": arg(func(c *config, v string) error {
		c.rules = append(c.rules, ".+ "+v)
		return nil
	}),
	"cvs-exclude":         unsupported(optFlag),
	"rsh":                 arg(func(c *config, v string) error { c.ssh = v; return nil }),
	"help":                flag(func(c *config) { c.help = true }),
	"version":             flag(func(c *config) { c.version = true }),
	"hard-links":          unsupported(optFlag),
	"acls":                unsupported(optFlag),
	"xattrs":              unsupported(optFlag),
	"copy-links":          unsupported(optFlag),
	"relative":            unsupported(optFlag),
	"backup":              unsupported(optFlag),
	"remove-source-files": unsupported(optFlag),
	"delete-excluded":     unsupported(optFlag),
	"existing":            unsupported(optFlag),
	"bwlimit":             unsupported(optArg),

	// rift extensions
	"rich":           flag(func(c *config) { c.rich = true }),
	"direct":         flag(func(c *config) { c.direct = true }),
	"conns":          intArg(func(c *config) *int { return &c.conns }),
	"gitignore":      flag(func(c *config) { c.gitignore = true }),
	"no-fast-cipher": flag(func(c *config) { c.noFastCipher = true }),
	"persist":        flag(func(c *config) { c.persist = true }),
	"no-persist":     flag(func(c *config) { c.persist = false }),
	"delta-min": arg(func(c *config, v string) error {
		n, err := parseSize(v)
		c.deltaMin = n
		return err
	}),
}

var shortOpts = map[byte]string{
	'a': "archive", 'r': "recursive", 'l': "links", 'p': "perms", 't': "times",
	'g': "group", 'o': "owner", 'D': "devices", 'E': "executability", 'O': "omit-dir-times",
	'h': "human-readable", 'v': "verbose", 'i': "itemize-changes", 'q': "quiet",
	'n': "dry-run", 'c': "checksum", 'I': "ignore-times", 'z': "compress",
	'u': "update", 'W': "whole-file", 'S': "sparse", 'e': "rsh", 'f': "filter",
	'H': "hard-links", 'A': "acls", 'X': "xattrs", 'L': "copy-links", 'R': "relative",
	'b': "backup", 'j': "conns",
}

func parseArgs(args []string) (*config, error) {
	c := &config{conns: 32, compress: 1, deltaMin: 128 << 10, ssh: "ssh"}
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--":
			c.pos = append(c.pos, args[i+1:]...)
			i = len(args)
		case strings.HasPrefix(a, "--"):
			name, val, hasVal := strings.Cut(a[2:], "=")
			o, ok := longOpts[name]
			if !ok {
				return nil, fmt.Errorf("unknown option --%s", name)
			}
			if o.kind == optArg && !hasVal {
				if i+1 >= len(args) {
					return nil, fmt.Errorf("--%s requires a value", name)
				}
				i++
				val = args[i]
			} else if o.kind == optFlag && hasVal {
				return nil, fmt.Errorf("--%s does not take a value", name)
			}
			if err := o.set(c, val); err != nil {
				return nil, fmt.Errorf("--%s: %w", name, err)
			}
		case len(a) > 1 && a[0] == '-':
			for j := 1; j < len(a); j++ {
				ch := a[j]
				if ch == 'P' {
					continue // --partial --progress: rift always shows progress on a terminal
				}
				if ch == 'F' {
					c.rules = append(c.rules, ": .rsync-filter")
					continue
				}
				name, ok := shortOpts[ch]
				if !ok {
					return nil, fmt.Errorf("unknown option -%c", ch)
				}
				o := longOpts[name]
				if o.kind == optArg {
					val := a[j+1:]
					if val == "" {
						if i+1 >= len(args) {
							return nil, fmt.Errorf("-%c requires a value", ch)
						}
						i++
						val = args[i]
					}
					if err := o.set(c, val); err != nil {
						return nil, fmt.Errorf("-%c: %w", ch, err)
					}
					break
				}
				if err := o.set(c, ""); err != nil {
					return nil, fmt.Errorf("-%c: %w", ch, err)
				}
			}
		default:
			c.pos = append(c.pos, a)
		}
	}
	if c.conns < 1 || c.conns > 32 {
		return nil, fmt.Errorf("connections must be 1-32")
	}
	if c.compress < 0 || c.compress > 4 {
		return nil, fmt.Errorf("compress level must be 0-4")
	}
	if c.wholeFile {
		c.deltaMin = math.MaxInt64
	}
	return c, nil
}

// destRoot applies rsync's trailing-slash rule: "src/" syncs the contents
// of src into dest, while "src" syncs src itself into dest/src.
func destRoot(src, dest string) string {
	if strings.HasSuffix(src, "/") {
		return dest
	}
	base := path.Base(src)
	if base == "." || base == "/" {
		return dest
	}
	if dest == "" {
		return base
	}
	return strings.TrimRight(dest, "/") + "/" + base
}

func parseSize(s string) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "K"):
		mult, s = 1<<10, s[:len(s)-1]
	case strings.HasSuffix(s, "M"):
		mult, s = 1<<20, s[:len(s)-1]
	case strings.HasSuffix(s, "G"):
		mult, s = 1<<30, s[:len(s)-1]
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return n * mult, nil
}
