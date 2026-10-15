package scan

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
)

// Action is what a filter rule does when it matches.
type Action uint8

// Rule actions, mirroring rsync filter rules.
const (
	ActExclude  Action = iota + 1 // '-': hide on sender, protect on receiver
	ActInclude                    // '+': show on sender, risk on receiver
	ActProtect                    // 'P': receiver only, never delete
	ActRisk                       // 'R': receiver only, allow delete
	ActHide                       // 'H': sender only exclude
	ActShow                       // 'S': sender only include
	ActDirMerge                   // ':': per-directory merge file
)

type rule struct {
	act      Action
	re       *regexp.Regexp
	dirOnly  bool
	basename bool // match only the final path component
	pattern  string
	spec     string

	mergeFile string
	mergeMode byte // '-' gitignore-style lines, '+' include lines, 0 rsync rule lines
	mergeIdx  int
}

func (r *rule) match(rel string, isDir bool) bool {
	if r.dirOnly && !isDir {
		return false
	}
	if r.basename {
		return r.re.MatchString(path.Base(rel))
	}
	return r.re.MatchString(rel)
}

// Filter is an ordered rsync-style rule list where the first matching rule
// wins. Per-directory merge rules (":- .gitignore") splice the rules found in
// each directory's merge file into the list at their position, with deeper
// directories taking precedence. Gitignore-style merge files use gitignore's
// own semantics: last matching line wins, "!" re-includes, and a pattern with
// a slash is anchored to the file's directory.
type Filter struct {
	rules  []rule
	merges int
	specs  []string
}

// MergeCtx carries the per-directory merge rules in effect for a directory.
// It is immutable; entering a directory returns a new context.
type MergeCtx struct {
	heads []*mergeNode
}

type mergeNode struct {
	parent    *mergeNode
	base      string
	rules     []rule
	gitignore bool
}

// NewFilter compiles rsync filter rule strings such as "- *.o", "+ /src/",
// "P /build/***", ":- .gitignore", "exclude foo" or "dir-merge,- .gitignore".
func NewFilter(specs []string) (*Filter, error) {
	f := &Filter{}
	for _, s := range specs {
		if err := f.add(s); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// Specs returns the rule strings the filter was built from.
func (f *Filter) Specs() []string { return f.specs }

// HasMerges reports whether any per-directory merge rule is present.
func (f *Filter) HasMerges() bool { return f != nil && f.merges > 0 }

var longNames = map[string]byte{
	"exclude": '-', "include": '+', "protect": 'P', "risk": 'R',
	"hide": 'H', "show": 'S', "dir-merge": ':', "merge": '.', "clear": '!',
}

// splitRule parses "RULE[,MODS] PATTERN" or the short form "R[MODS] PATTERN".
func splitRule(s string) (kind byte, mods, pattern string, err error) {
	s = strings.TrimRight(s, "\r\n")
	s = strings.TrimLeft(s, " \t")
	if s == "" {
		return 0, "", "", nil
	}
	word := s
	if i := strings.IndexAny(s, " ,"); i >= 0 {
		word = s[:i]
	}
	if k, ok := longNames[word]; ok {
		kind = k
		s = s[len(word):]
		if strings.HasPrefix(s, ",") {
			s = s[1:]
			i := strings.IndexByte(s, ' ')
			if i < 0 {
				i = len(s)
			}
			mods, s = s[:i], s[i:]
		}
		return kind, mods, strings.TrimPrefix(s, " "), nil
	}
	kind = s[0]
	if !strings.ContainsRune("-+PRHS:.!", rune(kind)) {
		return 0, "", "", fmt.Errorf("unknown filter rule %q", s)
	}
	s = s[1:]
	i := strings.IndexAny(s, " _")
	if i < 0 {
		return kind, s, "", nil
	}
	return kind, strings.TrimPrefix(s[:i], ","), s[i+1:], nil
}

func (f *Filter) add(spec string) error {
	kind, mods, pat, err := splitRule(spec)
	if err != nil || kind == 0 {
		return err
	}
	switch kind {
	case '!':
		f.rules = nil
		f.specs = append(f.specs, spec)
		return nil
	case '.':
		data, err := os.ReadFile(pat)
		if err != nil {
			return fmt.Errorf("merge file: %w", err)
		}
		for line := range strings.Lines(string(data)) {
			line = strings.TrimRight(line, "\r\n")
			if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
				continue
			}
			if mods == "-" {
				line = "- " + line
			} else if mods == "+" {
				line = "+ " + line
			}
			if err := f.add(line); err != nil {
				return err
			}
		}
		return nil
	case ':':
		r := rule{act: ActDirMerge, mergeFile: strings.TrimPrefix(pat, "/"), mergeIdx: f.merges, spec: spec}
		for _, m := range mods {
			switch m {
			case '-', '+':
				r.mergeMode = byte(m)
			case 'e':
				defer func() { _ = f.add("- " + path.Base(r.mergeFile)) }()
			case 'n', 'w', 's', 'r', 'C':
			default:
				return fmt.Errorf("unsupported dir-merge modifier %q in %q", m, spec)
			}
		}
		if r.mergeFile == "" || strings.Contains(r.mergeFile, "/") {
			return fmt.Errorf("bad dir-merge file in %q", spec)
		}
		f.merges++
		f.rules = append(f.rules, r)
		f.specs = append(f.specs, spec)
		return nil
	}

	act := map[byte]Action{'-': ActExclude, '+': ActInclude, 'P': ActProtect, 'R': ActRisk, 'H': ActHide, 'S': ActShow}[kind]
	for _, m := range mods {
		switch m {
		case 's':
			act = map[Action]Action{ActExclude: ActHide, ActInclude: ActShow}[act]
		case 'r':
			act = map[Action]Action{ActExclude: ActProtect, ActInclude: ActRisk}[act]
		case 'p':
		default:
			return fmt.Errorf("unsupported filter modifier %q in %q", m, spec)
		}
		if act == 0 {
			return fmt.Errorf("invalid modifier combination in %q", spec)
		}
	}
	r, err := compilePattern(pat, false)
	if err != nil {
		return err
	}
	r.act = act
	r.pattern = pat
	r.spec = spec
	f.rules = append(f.rules, r)
	f.specs = append(f.specs, spec)
	return nil
}

// RemoteScanSpecs returns the rules the receiver should apply to its own
// scan. A remote path can be skipped (not even listed) when the sender will
// never transfer it and it may not be deleted: that holds for excludes, and
// for protect rules that match nothing on the sender. Sender-only rules are
// dropped, and risk rules become includes so first-match order is kept.
func (f *Filter) RemoteScanSpecs(local []Entry) []string {
	if f == nil {
		return nil
	}
	var out []string
	for i := range f.rules {
		r := &f.rules[i]
		switch r.act {
		case ActExclude, ActInclude, ActDirMerge:
			out = append(out, r.spec)
		case ActRisk:
			out = append(out, "+ "+r.pattern)
		case ActProtect:
			used := false
			for j := range local {
				if local[j].Path != "" && r.match(local[j].Path, local[j].Kind == KindDir) {
					used = true
					break
				}
			}
			if !used {
				out = append(out, "- "+r.pattern)
			}
		}
	}
	return out
}

// compilePattern turns an rsync/gitignore glob into a rule.
func compilePattern(p string, gitignore bool) (rule, error) {
	var r rule
	if p == "" {
		return r, fmt.Errorf("empty filter pattern")
	}
	if len(p) > 1 && strings.HasSuffix(p, "/") {
		r.dirOnly = true
		p = strings.TrimRight(p, "/")
	}
	anchored := strings.HasPrefix(p, "/")
	p = strings.TrimPrefix(p, "/")
	hasSlash := strings.Contains(p, "/")
	if gitignore && hasSlash {
		anchored = true
	}
	r.basename = !anchored && !hasSlash && !strings.Contains(p, "**")
	expr := globToRegexp(p)
	if !anchored && !r.basename {
		expr = "(?:.*/)?" + expr
	}
	re, err := regexp.Compile("^" + expr + "$")
	if err != nil {
		return r, fmt.Errorf("bad pattern %q: %w", p, err)
	}
	r.re = re
	return r, nil
}

func globToRegexp(p string) string {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		c := p[i]
		switch c {
		case '/':
			if strings.HasPrefix(p[i:], "/***") {
				b.WriteString("(?:/.*)?")
				i += 3
			} else {
				b.WriteByte('/')
			}
		case '*':
			if i+1 < len(p) && p[i+1] == '*' {
				for i+1 < len(p) && p[i+1] == '*' {
					i++
				}
				if i+1 < len(p) && p[i+1] == '/' {
					b.WriteString("(?:.*/)?")
					i++
				} else {
					b.WriteString(".*")
				}
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '[':
			j := strings.IndexByte(p[i+1:], ']')
			if j < 0 {
				b.WriteString(`\[`)
				continue
			}
			class := p[i+1 : i+1+j]
			if strings.HasPrefix(class, "!") {
				class = "^" + class[1:]
			}
			b.WriteString("[" + strings.ReplaceAll(class, `\`, `\\`) + "]")
			i += j + 1
		case '\\':
			if i+1 < len(p) {
				b.WriteString(regexp.QuoteMeta(p[i+1 : i+2]))
				i++
			}
		default:
			b.WriteString(regexp.QuoteMeta(p[i : i+1]))
		}
	}
	return b.String()
}

type side uint8

const (
	sender side = iota
	receiver
)

func relevant(s side, a Action) bool {
	switch a {
	case ActExclude, ActInclude:
		return true
	case ActHide, ActShow:
		return s == sender
	case ActProtect, ActRisk:
		return s == receiver
	}
	return false
}

func (f *Filter) decide(p string, isDir bool, mc *MergeCtx, s side) Action {
	if f == nil {
		return 0
	}
	for i := range f.rules {
		r := &f.rules[i]
		if r.act != ActDirMerge {
			if relevant(s, r.act) && r.match(p, isDir) {
				return r.act
			}
			continue
		}
		if mc == nil || r.mergeIdx >= len(mc.heads) {
			continue
		}
		for n := mc.heads[r.mergeIdx]; n != nil; n = n.parent {
			rel := p
			if n.base != "" {
				if !strings.HasPrefix(p, n.base+"/") {
					continue
				}
				rel = p[len(n.base)+1:]
			}
			if n.gitignore {
				for j := len(n.rules) - 1; j >= 0; j-- {
					if n.rules[j].match(rel, isDir) {
						return n.rules[j].act
					}
				}
				continue
			}
			for j := range n.rules {
				if relevant(s, n.rules[j].act) && n.rules[j].match(rel, isDir) {
					return n.rules[j].act
				}
			}
		}
	}
	return 0
}

// Excluded reports whether the sender should skip p.
func (f *Filter) Excluded(p string, isDir bool, mc *MergeCtx) bool {
	a := f.decide(p, isDir, mc, sender)
	return a == ActExclude || a == ActHide
}

// Protected reports whether the receiver must not delete p.
func (f *Filter) Protected(p string, isDir bool, mc *MergeCtx) bool {
	a := f.decide(p, isDir, mc, receiver)
	return a == ActExclude || a == ActProtect
}

// MergeFiles returns the per-directory merge file names, indexed by merge id.
func (f *Filter) MergeFiles() []string {
	if f == nil {
		return nil
	}
	out := make([]string, f.merges)
	for i := range f.rules {
		if f.rules[i].act == ActDirMerge {
			out[f.rules[i].mergeIdx] = f.rules[i].mergeFile
		}
	}
	return out
}

// EnterDir returns the merge context for dir given its parent's context.
// read returns a merge file's contents if the directory contains it.
func (f *Filter) EnterDir(parent *MergeCtx, dir string, read func(name string) ([]byte, bool)) *MergeCtx {
	if !f.HasMerges() {
		return nil
	}
	ctx := parent
	for i := range f.rules {
		r := &f.rules[i]
		if r.act != ActDirMerge {
			continue
		}
		data, ok := read(r.mergeFile)
		if !ok {
			continue
		}
		node := &mergeNode{base: dir, gitignore: r.mergeMode == '-'}
		for line := range strings.Lines(string(data)) {
			line = strings.TrimRight(line, "\r\n")
			if r.mergeMode == '-' {
				if rl, ok := gitignoreRule(line); ok {
					node.rules = append(node.rules, rl)
				}
				continue
			}
			if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
				continue
			}
			if r.mergeMode == '+' {
				line = "+ " + line
			}
			kind, mods, pat, err := splitRule(line)
			if err != nil || kind == 0 || kind == ':' || kind == '.' || kind == '!' || mods != "" {
				continue
			}
			rl, err := compilePattern(pat, false)
			if err != nil {
				continue
			}
			rl.act = map[byte]Action{'-': ActExclude, '+': ActInclude, 'P': ActProtect, 'R': ActRisk, 'H': ActHide, 'S': ActShow}[kind]
			node.rules = append(node.rules, rl)
		}
		if len(node.rules) == 0 {
			continue
		}
		next := &MergeCtx{heads: make([]*mergeNode, f.merges)}
		if ctx != nil {
			copy(next.heads, ctx.heads)
		}
		node.parent = next.heads[r.mergeIdx]
		next.heads[r.mergeIdx] = node
		ctx = next
	}
	return ctx
}

// gitignoreRule parses one .gitignore line.
func gitignoreRule(line string) (rule, bool) {
	if strings.HasPrefix(line, "#") {
		return rule{}, false
	}
	// Trailing spaces are ignored unless escaped.
	for strings.HasSuffix(line, " ") && !strings.HasSuffix(line, `\ `) {
		line = line[:len(line)-1]
	}
	if line == "" {
		return rule{}, false
	}
	act := ActExclude
	if strings.HasPrefix(line, "!") {
		act = ActInclude
		line = line[1:]
	} else if strings.HasPrefix(line, `\!`) || strings.HasPrefix(line, `\#`) {
		line = line[1:]
	}
	r, err := compilePattern(line, true)
	if err != nil {
		return rule{}, false
	}
	r.act = act
	return r, true
}
