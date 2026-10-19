package main

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
	"slices"
	"strings"
	"time"

	"github.com/shxntanu/rift/internal/client"
)

const bigName = "rift-bench-big.dat"

type tool int

const (
	rsyncTool tool = iota
	riftTool
)

func (t tool) String() string { return [...]string{"rsync", "rift"}[t] }

type result struct {
	name, desc string
	times      [2][]time.Duration
	fails      [2]int
	verify     string
}

type bench struct {
	*config
	ctx      context.Context
	tmp      string   // local scratch root
	local    string   // tree being pushed: SRC or its scratch copy
	remote   string   // remote scratch directory
	sock     string   // control socket of the helper/rsync ssh master
	sshProg  string   // ssh executable
	sshOpts  []string // options shared by every ssh invocation
	files    []string // regular files scenarios may modify, shuffled
	rng      *rand.Rand
	logN     int
	baseline bool
}

func run(ctx context.Context, cfg *config) (err error) {
	b := &bench{config: cfg, ctx: ctx, rng: rand.New(rand.NewPCG(1, 2))}
	if b.tmp, err = os.MkdirTemp("", "rift-bench-"); err != nil {
		return err
	}
	defer os.RemoveAll(b.tmp)

	rsyncVer, err := firstLine(b.rsyncBin, "--version")
	if err != nil {
		return fmt.Errorf("rsync: %w", err)
	}
	riftVer, err := firstLine(b.riftBin, "--version")
	if err != nil {
		return fmt.Errorf("rift: %w (build it with make, or pass -rift)", err)
	}

	b.sshProg, b.sshOpts = "ssh", nil
	if len(b.ssh) > 0 {
		b.sshProg = b.ssh[0]
	}
	// Same transport settings as rift for both tools, so the comparison is
	// between the tools rather than ssh configurations. Forwardings from the
	// user's ssh config are dropped: they are useless here and parallel
	// connections would fight over forwarded sockets.
	b.sshOpts = []string{"-T", "-x", "-a",
		"-o", "ClearAllForwardings=yes", "-o", "PermitLocalCommand=no",
		"-o", "Compression=no", "-o", "ServerAliveInterval=15",
		"-o", "Ciphers=^aes128-gcm@openssh.com,aes256-gcm@openssh.com"}

	if err := b.startMaster(); err != nil {
		return err
	}
	defer b.stopMaster()

	if b.remote, err = b.remoteSh(fmt.Sprintf(
		`mkdir -p %s && d=$(mktemp -d %s/rift-bench-XXXXXX) && cd "$d" && mkdir rsync rift warm-rsync warm-rift && echo "$d"`,
		shq(b.remoteBase), shq(b.remoteBase))); err != nil {
		return fmt.Errorf("creating remote scratch directory: %w", err)
	}
	b.remote = strings.TrimSpace(b.remote)
	defer func() {
		if b.keep {
			fmt.Printf("\nkept %s:%s\n", b.host, b.remote)
			return
		}
		if _, err := b.remoteSh("rm -rf " + shq(b.remote)); err != nil {
			fmt.Fprintf(os.Stderr, "rift-bench: removing %s:%s: %v\n", b.host, b.remote, err)
		}
	}()

	b.local = b.src
	if b.only["touch"] || b.only["edit"] || b.only["delete"] || b.only["big-new"] || b.only["big"] {
		fmt.Printf("copying %s to a local scratch copy...\n", b.src)
		b.local = filepath.Join(b.tmp, "src")
		if err := cloneTree(b.ctx, b.src, b.local); err != nil {
			return fmt.Errorf("local scratch copy: %w", err)
		}
	}
	nfiles, size, err := treeStats(b.local)
	if err != nil {
		return err
	}
	tracked, err := b.pickFiles()
	if err != nil {
		return err
	}

	fmt.Printf("\nsource   %s (%d files, %s)\n", b.src, nfiles, client.HumanBytes(size))
	fmt.Printf("dest     %s:%s/{rsync,rift}\n", b.host, b.remote)
	fmt.Printf("rsync    %s\n", rsyncVer)
	fmt.Printf("rift     %s\n", riftVer)
	mode := "warm (both reuse persistent ssh connections)"
	if b.cold {
		mode = "cold (new ssh connections every run)"
	}
	fmt.Printf("ssh      %s\n", mode)
	fmt.Printf("args     %s\n", strings.Join(b.commonArgs(), " "))
	if tracked {
		fmt.Printf("changes  picked from %d git-tracked files\n", len(b.files))
	}
	if strings.Contains(rsyncVer, "openrsync") || strings.Contains(rsyncVer, "version 2.") {
		fmt.Println("\nnote: this is not rsync 3.x (macOS ships openrsync or rsync 2.6.9). For a")
		fmt.Println("      representative comparison install it (brew install rsync) and pass -rsync.")
	}

	fmt.Println("\nwarming up (installs rift on the remote, opens ssh connections)...")
	if err := b.warmUp(); err != nil {
		return err
	}

	var results []result
	for _, sc := range b.scenarios() {
		if !b.only[sc.name] {
			continue
		}
		if err := b.ctx.Err(); err != nil {
			return err
		}
		if sc.name != "fresh" && !b.baseline {
			if err := b.syncBoth("baseline"); err != nil {
				return err
			}
		}
		res, err := b.runScenario(sc)
		if err != nil {
			return err
		}
		results = append(results, res)
		b.baseline = true
	}
	b.report(results)
	return nil
}

type scenario struct {
	name, desc string
	setup      func(run int) error
}

func (b *bench) scenarios() []scenario {
	n := func(v int) int { return min(v, len(b.files)) }
	// The same number of files every run, leaving part of the tree in place.
	del := min(b.del, len(b.files)/(b.runs+1))
	return []scenario{
		{"fresh", "fresh copy", func(int) error {
			_, err := b.remoteSh(fmt.Sprintf("cd %s && rm -rf rsync rift && mkdir rsync rift", shq(b.remote)))
			return err
		}},
		{"noop", "no changes", nil},
		{"touch", fmt.Sprintf("touch %d files", n(b.touch)), b.touchFiles},
		{"edit", fmt.Sprintf("edit %d files", n(b.edit)), b.editFiles},
		{"delete", fmt.Sprintf("delete %d files", del), func(int) error { return b.deleteFiles(del) }},
		{"big-new", fmt.Sprintf("new %s file", client.HumanBytes(b.big)), func(int) error {
			if err := b.ensureBig(); err != nil {
				return err
			}
			_, err := b.remoteSh(fmt.Sprintf("cd %s && rm -f rsync/%s rift/%s", shq(b.remote), bigName, bigName))
			return err
		}},
		{"big", fmt.Sprintf("16 edits in %s file", client.HumanBytes(b.big)), b.editBig},
	}
}

func (b *bench) runScenario(sc scenario) (result, error) {
	res := result{name: sc.name, desc: sc.desc}
	for run := 0; run < b.runs; run++ {
		if sc.setup != nil {
			if err := sc.setup(run); err != nil {
				return res, fmt.Errorf("%s: %w", sc.name, err)
			}
		}
		// Alternate who goes first so neither tool always gets the
		// warmer caches.
		order := []tool{rsyncTool, riftTool}
		if run%2 == 1 {
			order = []tool{riftTool, rsyncTool}
		}
		var line [2]string
		for i, t := range order {
			d, err := b.sync(t, sc.name)
			if err != nil {
				if b.ctx.Err() != nil {
					return res, b.ctx.Err()
				}
				fmt.Fprintf(os.Stderr, "\n%s failed in %s run %d: %v\n", t, sc.name, run+1, err)
				res.fails[t]++
				line[t] = "FAILED"
				continue
			}
			res.times[t] = append(res.times[t], d)
			line[t] = fmtDur(d)
			if t == riftTool && i+1 < len(order) && order[i+1] == rsyncTool {
				fmt.Printf("  rift finished in %s; starting rsync...\n", line[t])
			}
		}
		fmt.Printf("  %-26s run %d/%d   rsync %-8s rift %s\n", sc.desc, run+1, b.runs, line[rsyncTool], line[riftTool])
	}
	if b.verify {
		res.verify = b.compare()
		if res.verify != "" {
			fmt.Fprintf(os.Stderr, "\n%s: destinations differ after %s:\n%s\n", sc.name, sc.desc, res.verify)
		}
	}
	return res, nil
}

// sync pushes the local tree with one tool and returns the wall time.
func (b *bench) sync(t tool, label string) (time.Duration, error) {
	return b.syncTo(t, b.local, t.String(), label)
}

func (b *bench) syncTo(t tool, src, dir, label string) (time.Duration, error) {
	dst := fmt.Sprintf("%s:%s/%s/", b.host, b.remote, dir)
	var args []string
	switch t {
	case rsyncTool:
		args = append([]string{"-e", strings.Join(b.rsyncSSH(), " ")}, b.commonArgs()...)
		args = append(args, b.rsyncArgs...)
	case riftTool:
		if len(b.ssh) > 0 {
			args = append(args, "-e", strings.Join(b.ssh, " "))
		}
		if b.cold {
			args = append(args, "--no-persist")
		} else {
			args = append(args, "--persist")
		}
		args = append(args, b.commonArgs()...)
		args = append(args, b.riftArgs...)
	}
	args = append(args, src+"/", dst)
	bin := map[tool]string{rsyncTool: b.rsyncBin, riftTool: b.riftBin}[t]

	// Output goes to a file rather than a pipe: background ssh masters
	// inherit the descriptors, and waiting for a pipe to close would add
	// their lifetime to the measurement.
	b.logN++
	logPath := filepath.Join(b.tmp, fmt.Sprintf("%03d-%s-%s.log", b.logN, label, t))
	logf, err := os.Create(logPath)
	if err != nil {
		return 0, err
	}
	defer logf.Close()
	cmd := exec.CommandContext(b.ctx, bin, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	start := time.Now()
	err = cmd.Run()
	d := time.Since(start)
	if err != nil {
		out, _ := os.ReadFile(logPath)
		return d, fmt.Errorf("%s %s: %v\n%s", bin, strings.Join(args, " "), err, tail(out, 20))
	}
	return d, nil
}

func (b *bench) commonArgs() []string {
	return append([]string{"-a", "--delete"}, b.syncArgs...)
}

func (b *bench) syncBoth(label string) error {
	for _, t := range []tool{rsyncTool, riftTool} {
		if _, err := b.sync(t, label); err != nil {
			return fmt.Errorf("%s sync: %w", label, err)
		}
	}
	return nil
}

// warmUp deploys rift's server and opens the ssh connections both tools
// will use, outside of any measurement. The warm-up tree has enough files
// for rift to open all of its parallel connections.
func (b *bench) warmUp() error {
	dir := filepath.Join(b.tmp, "warm")
	if err := os.Mkdir(dir, 0o755); err != nil {
		return err
	}
	for i := range 2048 {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%04d", i)), nil, 0o644); err != nil {
			return err
		}
	}
	for _, t := range []tool{rsyncTool, riftTool} {
		if _, err := b.syncTo(t, dir, "warm-"+t.String(), "warmup"); err != nil {
			return fmt.Errorf("warm-up: %w", err)
		}
	}
	return nil
}

// ---- local changes ----

// pickFiles collects the regular files scenarios may change: git-tracked
// files when the tree is a repository (so filters such as ":- .gitignore"
// don't hide the changes), otherwise everything outside .git.
func (b *bench) pickFiles() (tracked bool, err error) {
	var files []string
	if out, err := exec.Command("git", "-C", b.local, "ls-files", "-z").Output(); err == nil {
		for _, p := range strings.Split(strings.TrimRight(string(out), "\x00"), "\x00") {
			if fi, err := os.Lstat(filepath.Join(b.local, p)); p != "" && err == nil && fi.Mode().IsRegular() {
				files = append(files, p)
			}
		}
		tracked = len(files) > 0
	}
	if len(files) == 0 {
		err = filepath.WalkDir(b.local, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && d.Name() == ".git" {
				return filepath.SkipDir
			}
			if d.Type().IsRegular() && d.Name() != bigName {
				rel, _ := filepath.Rel(b.local, p)
				files = append(files, rel)
			}
			return nil
		})
		if err != nil {
			return false, err
		}
	}
	if len(files) == 0 && (b.only["touch"] || b.only["edit"] || b.only["delete"]) {
		return false, errors.New("source has no regular files to change")
	}
	slices.Sort(files)
	b.rng.Shuffle(len(files), func(i, j int) { files[i], files[j] = files[j], files[i] })
	b.files = files
	return tracked, nil
}

// window returns n files, a different slice of the list for every run.
func (b *bench) window(run, n int) []string {
	n = min(n, len(b.files))
	if n == 0 {
		return nil
	}
	out := make([]string, n)
	start := run * n
	for i := range out {
		out[i] = b.files[(start+i)%len(b.files)]
	}
	return out
}

func (b *bench) touchFiles(run int) error {
	for _, p := range b.window(run, b.touch) {
		p = filepath.Join(b.local, p)
		fi, err := os.Stat(p)
		if err != nil {
			return err
		}
		t := fi.ModTime().Add(-time.Hour)
		if err := os.Chtimes(p, t, t); err != nil {
			return err
		}
	}
	return nil
}

func (b *bench) editFiles(run int) error {
	for _, p := range b.window(run, b.edit) {
		f, err := os.OpenFile(filepath.Join(b.local, p), os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(f, "\nrift-bench edit %d\n", run)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// deleteFiles removes n files from the end of the list, so they never
// overlap with the ones touch and edit use from the front.
func (b *bench) deleteFiles(n int) error {
	victims := b.files[len(b.files)-n:]
	b.files = b.files[:len(b.files)-n]
	for _, p := range victims {
		if err := os.Remove(filepath.Join(b.local, p)); err != nil {
			return err
		}
	}
	return nil
}

func (b *bench) bigPath() string { return filepath.Join(b.local, bigName) }

// ensureBig creates the large random file once.
func (b *bench) ensureBig() error {
	if _, err := os.Stat(b.bigPath()); err == nil {
		return nil
	}
	f, err := os.Create(b.bigPath())
	if err != nil {
		return err
	}
	src := rand.NewChaCha8([32]byte{1})
	buf := make([]byte, 4<<20)
	for left := b.big; left > 0; {
		n := min(int64(len(buf)), left)
		_, _ = src.Read(buf[:n])
		if _, err := f.Write(buf[:n]); err != nil {
			f.Close()
			return err
		}
		left -= n
	}
	return f.Close()
}

func (b *bench) editBig(int) error {
	if _, err := os.Stat(b.bigPath()); err != nil {
		if err := b.ensureBig(); err != nil {
			return err
		}
		if err := b.syncBoth("big-baseline"); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(b.bigPath(), os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	buf := make([]byte, 4<<10)
	for range 16 {
		for i := range buf {
			buf[i] = byte(b.rng.Uint32())
		}
		if _, err := f.WriteAt(buf, b.rng.Int64N(max(b.big-int64(len(buf)), 1))); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}

// ---- remote ----

func (b *bench) startMaster() error {
	b.sock = filepath.Join(os.TempDir(), fmt.Sprintf("rift-bench-%d.sock", os.Getpid()))
	if len(b.sock) > 100 {
		b.sock = fmt.Sprintf("/tmp/rift-bench-%d.sock", os.Getpid())
	}
	args := append(slices.Clone(b.sshOpts),
		"-o", "ControlMaster=yes", "-o", "ControlPath="+b.sock, "-o", "ControlPersist=yes", "-f", "-N")
	args = append(append(args, b.sshExtra()...), b.host)
	// Attached to the terminal so password or host-key prompts work; the
	// master detaches once authenticated.
	cmd := exec.CommandContext(b.ctx, b.sshProg, args...)
	cmd.Stdin, cmd.Stderr = os.Stdin, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ssh %s: %w", b.host, err)
	}
	return nil
}

func (b *bench) stopMaster() {
	args := append(slices.Clone(b.sshOpts), "-o", "ControlPath="+b.sock, "-O", "exit")
	args = append(append(args, b.sshExtra()...), b.host)
	cmd := exec.Command(b.sshProg, args...)
	_ = cmd.Run()
	os.Remove(b.sock)
}

func (b *bench) sshExtra() []string {
	if len(b.ssh) > 1 {
		return b.ssh[1:]
	}
	return nil
}

// rsyncSSH is rsync's -e command: the shared master when warm, a fresh
// connection per run when cold.
func (b *bench) rsyncSSH() []string {
	args := append([]string{b.sshProg}, b.sshOpts...)
	if b.cold {
		args = append(args, "-o", "ControlMaster=no", "-o", "ControlPath=none")
	} else {
		args = append(args, "-o", "ControlMaster=no", "-o", "ControlPath="+b.sock)
	}
	return append(args, b.sshExtra()...)
}

// remoteSh runs a POSIX sh script on the remote over the master connection,
// independent of the user's login shell. Cleanup still runs after Ctrl-C.
func (b *bench) remoteSh(script string) (string, error) {
	args := append(slices.Clone(b.sshOpts), "-o", "ControlMaster=no", "-o", "ControlPath="+b.sock)
	args = append(append(args, b.sshExtra()...), b.host, "sh -c "+shq(script))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	var out, errb bytes.Buffer
	cmd := exec.CommandContext(ctx, b.sshProg, args...)
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%v: %s", err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

// listingScript prints type, path, size, mode, mtime and checksum of every
// entry, in a form comparable between the two destinations.
const listingScript = `cd "$1" || exit 1
{
if stat -c %Y . >/dev/null 2>&1; then
  find . -type f -exec stat -c 'f %n %s %a %Y' {} +
  find . -type d -exec stat -c 'd %n %a %Y' {} +
else
  find . -type f -exec stat -f 'f %N %z %Lp %m' {} +
  find . -type d -exec stat -f 'd %N %Lp %m' {} +
fi
find . -type l | while IFS= read -r l; do echo "l $l -> $(readlink "$l")"; done
find . -type f -exec cksum {} + | sed 's/^/c /'
} | LC_ALL=C sort`

// compare returns a description of the differences between the two
// destinations, or "" if they are identical.
func (b *bench) compare() string {
	list := func(dir string) (string, error) {
		return b.remoteSh(fmt.Sprintf("set -- %s; %s", shq(b.remote+"/"+dir), listingScript))
	}
	a, err := list("rsync")
	if err != nil {
		return "listing failed: " + err.Error()
	}
	c, err := list("rift")
	if err != nil {
		return "listing failed: " + err.Error()
	}
	if a == c {
		return ""
	}
	al, cl := lineSet(a), lineSet(c)
	var diff []string
	for l := range al {
		if !cl[l] {
			diff = append(diff, "  rsync only: "+l)
		}
	}
	for l := range cl {
		if !al[l] {
			diff = append(diff, "  rift only:  "+l)
		}
	}
	slices.Sort(diff)
	if len(diff) > 20 {
		diff = append(diff[:20], fmt.Sprintf("  ... and %d more", len(diff)-20))
	}
	return strings.Join(diff, "\n")
}

func lineSet(s string) map[string]bool {
	m := map[string]bool{}
	for _, l := range strings.Split(strings.TrimSpace(s), "\n") {
		m[l] = true
	}
	return m
}

// ---- report ----

func (b *bench) report(results []result) {
	fmt.Println()
	if b.markdown {
		fmt.Println("| scenario | rsync | rift | speedup | identical |")
		fmt.Println("|---|---:|---:|---:|---|")
	} else {
		fmt.Printf("%-28s %10s %10s %9s  %s\n", "scenario", "rsync", "rift", "speedup", "identical")
	}
	for _, r := range results {
		rs, rf := median(r.times[rsyncTool]), median(r.times[riftTool])
		cell := func(t tool, d time.Duration) string {
			if len(r.times[t]) == 0 {
				return "FAILED"
			}
			s := fmtDur(d)
			if r.fails[t] > 0 {
				s += "*"
			}
			return s
		}
		speedup := "-"
		if rs > 0 && rf > 0 {
			speedup = fmt.Sprintf("%.1fx", rs.Seconds()/rf.Seconds())
		}
		same := "yes"
		switch {
		case !b.verify:
			same = "-"
		case r.verify != "":
			same = "NO"
		}
		if b.markdown {
			fmt.Printf("| %s | %s | %s | %s | %s |\n", r.desc, cell(rsyncTool, rs),
				cell(riftTool, rf), speedup, same)
		} else {
			fmt.Printf("%-28s %10s %10s %9s  %s\n", r.desc, cell(rsyncTool, rs),
				cell(riftTool, rf), speedup, same)
		}
	}
	fmt.Printf("\nmedian of %d runs; * = some runs failed. Speedup = rsync time / rift time.\n",
		b.runs)
}

// ---- helpers ----

// cloneTree copies src to dst preserving times and modes, using
// copy-on-write clones where the filesystem supports them.
func cloneTree(ctx context.Context, src, dst string) error {
	var tries [][]string
	if runtime.GOOS == "darwin" {
		tries = [][]string{{"-c", "-pR"}, {"-pR"}}
	} else {
		tries = [][]string{{"-a", "--reflink=auto"}, {"-pR"}}
	}
	var err error
	for _, flags := range tries {
		os.RemoveAll(dst)
		var out []byte
		out, err = exec.CommandContext(ctx, "cp", append(flags, src, dst)...).CombinedOutput()
		if err == nil {
			return nil
		}
		err = fmt.Errorf("cp %s: %v: %s", strings.Join(flags, " "), err, tail(out, 5))
	}
	return err
}

func treeStats(root string) (files int, size int64, err error) {
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			fi, err := d.Info()
			if err != nil {
				return err
			}
			files++
			size += fi.Size()
		}
		return nil
	})
	return
}

func firstLine(bin string, args ...string) (string, error) {
	out, err := exec.Command(bin, args...).CombinedOutput()
	if err != nil {
		return "", err
	}
	line, _, _ := strings.Cut(strings.TrimSpace(string(out)), "\n")
	return strings.Join(strings.Fields(line), " "), nil
}

func median(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func fmtDur(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func tail(b []byte, n int) string {
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func shq(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
