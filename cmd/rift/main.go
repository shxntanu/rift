// Command rift is a fast, parallel rsync alternative for pushing a directory
// tree to a remote host over SSH. It accepts rsync's common command line.
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"syscall"

	"github.com/shxntanu/rift/internal/client"
	"github.com/shxntanu/rift/internal/proto"
	"github.com/shxntanu/rift/internal/server"
	"github.com/shxntanu/rift/internal/transport"
	"github.com/zeebo/xxh3"
)

// Set via -ldflags at build time.
var (
	version = "dev"
	buildID string
)

const usage = `rift - fast parallel directory sync over SSH, rsync compatible

Usage:
  rift [OPTIONS] SRC [USER@]HOST:DEST
  rift [OPTIONS] SRC DEST

Like rsync, "SRC/" copies the contents of SRC into DEST while "SRC" copies
the directory itself to DEST/SRC. The remote side installs itself into
~/.rift/bin on first use; only ssh access is needed.

rsync options:
  -a, -r, -l, -p, -t, -g, -o, -D   accepted (rift always syncs recursively
                                   with links, permissions and times)
  -v, --verbose            list transferred files (-i/--itemize-changes too)
  -q, --quiet              no progress or summary
  -n, --dry-run            show what would change
  -c, --checksum           compare by content instead of size+mtime
  -I, --ignore-times       same as --checksum
  -z, --compress           zstd compression (on by default in rift)
      --compress-level=N   zstd level 1-4, 0 disables
      --no-compress        disable compression
      --delete             delete receiver files missing on the sender
                           (--del/--delete-before/-during/-after/-delay)
  -f, --filter=RULE        rsync filter rule: "- PAT", "+ PAT", "P PAT",
                           "R PAT", "H PAT", "S PAT", ":- .gitignore", ...
      --exclude=PAT        same as --filter='- PAT'
      --include=PAT        same as --filter='+ PAT'
      --exclude-from=FILE  read exclude patterns from FILE
      --include-from=FILE  read include patterns from FILE
  -F                       same as --filter=': .rsync-filter'
  -e, --rsh=CMD            ssh command, e.g. "ssh -p 2222"
  -u, --update             skip files that are newer on the receiver
      --ignore-existing    skip files that exist on the receiver
      --size-only          skip files whose size matches
      --modify-window=N    treat mtimes within N seconds as equal
  -W, --whole-file         disable delta transfer
  -P, --progress, --partial, --stats, -h   accepted

rift options:
  -j, --conns=N            parallel ssh connections (default 8)
      --gitignore          send exactly what git sees (git ls-files); never
                           deletes ignored files or .git on the receiver
      --delta-min=SIZE     delta-transfer changed files at least SIZE (128K)
      --no-fast-cipher     don't prefer AES-GCM for ssh
      --no-persist         don't keep ssh connections open for 15 minutes
                           (persisting makes the next sync skip handshakes)
`

func main() {
	// RIFT_PROFILE=dir writes CPU profiles for the client and any local
	// server processes into dir.
	if dir := os.Getenv("RIFT_PROFILE"); dir != "" {
		role := "client"
		if len(os.Args) > 1 && os.Args[1] == "serve" {
			role = "server"
		}
		if f, err := os.Create(fmt.Sprintf("%s/%s-%d.pprof", dir, role, os.Getpid())); err == nil {
			_ = pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}
	}
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		debug.SetGCPercent(400)
		if err := server.Serve(stdio{}); err != nil {
			fmt.Fprintln(os.Stderr, "rift serve:", err)
			pprof.StopCPUProfile()
			os.Exit(1)
		}
		return
	}
	code := run(os.Args[1:])
	pprof.StopCPUProfile()
	os.Exit(code)
}

type stdio struct{}

func (stdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (stdio) Close() error                { return os.Stdout.Close() }

func run(args []string) int {
	cfg, err := parseArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rift:", err)
		fmt.Fprintln(os.Stderr, "rift: see rift --help")
		return 1
	}
	if cfg.help {
		fmt.Print(usage)
		return 0
	}
	if cfg.version {
		fmt.Printf("rift %s (protocol %d, build %s)\n", version, proto.Version, getBuildID())
		return 0
	}
	if len(cfg.pos) != 2 {
		if len(cfg.pos) > 2 {
			fmt.Fprintln(os.Stderr, "rift: multiple sources are not supported yet")
		} else {
			fmt.Fprint(os.Stderr, usage)
		}
		return 1
	}
	src := cfg.pos[0]
	if fi, err := os.Stat(src); err != nil {
		fmt.Fprintln(os.Stderr, "rift:", err)
		return 1
	} else if !fi.IsDir() {
		fmt.Fprintf(os.Stderr, "rift: %s: only directory sources are supported yet\n", src)
		return 1
	}
	if strings.Contains(src, ":") && transport.ParseEndpoint(src).Host != "" {
		fmt.Fprintln(os.Stderr, "rift: pulling from a remote source is not supported yet")
		return 1
	}
	dst := transport.ParseEndpoint(cfg.pos[1])
	if strings.HasPrefix(dst.Path, ":") {
		fmt.Fprintln(os.Stderr, "rift: rsync daemon (host::module) destinations are not supported")
		return 1
	}
	dst.Path = destRoot(src, dst.Path)
	if dst.Path == "" {
		dst.Path = "."
	}

	sshFields := strings.Fields(cfg.ssh)
	if len(sshFields) == 0 {
		sshFields = []string{"ssh"}
	}
	isTTY := false
	if fi, err := os.Stderr.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
		isTTY = true
	}
	debug.SetGCPercent(400)

	opt := client.Options{
		Src:            src,
		Dst:            dst,
		Conns:          cfg.conns,
		Compress:       cfg.compress,
		Delete:         cfg.del,
		Checksum:       cfg.checksum,
		DryRun:         cfg.dryRun,
		Rules:          cfg.rules,
		GitIgnore:      cfg.gitignore,
		DeltaMin:       cfg.deltaMin,
		SizeOnly:       cfg.sizeOnly,
		Update:         cfg.update,
		IgnoreExisting: cfg.ignoreExisting,
		ModifyWindow:   cfg.modifyWindow,
		Verbose:        cfg.verbose > 0 || cfg.dryRun,
		Progress:       isTTY && !cfg.quiet,
		Transport: transport.Options{
			SSH:        sshFields[0],
			SSHArgs:    sshFields[1:],
			FastCipher: !cfg.noFastCipher,
			BuildID:    getBuildID(),
			Persist:    !cfg.noPersist,
			PersistFor: "15m",
		},
		Log: os.Stderr,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	sum, err := client.Run(ctx, opt)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rift:", err)
		return 1
	}
	for _, e := range sum.Errors {
		fmt.Fprintln(os.Stderr, "rift: error:", e)
	}
	if sum.ErrorCount > len(sum.Errors) {
		fmt.Fprintf(os.Stderr, "rift: ... and %d more errors\n", sum.ErrorCount-len(sum.Errors))
	}
	if !cfg.quiet {
		printSummary(sum, cfg.dryRun)
	}
	if sum.ErrorCount > 0 {
		return 23
	}
	return 0
}

func printSummary(s *client.Summary, dryRun bool) {
	verb := "synced"
	if dryRun {
		verb = "would sync"
	}
	fmt.Fprintf(os.Stderr, "rift: %s %d files (%s), %d metadata-only, %d deleted | scanned %d local / %d remote in %s\n",
		verb, s.Transferred, client.HumanBytes(s.Bytes), s.MetaOnly, s.Deleted,
		s.Scanned, s.RemoteScanned, s.ScanTime.Round(1e6))
	if dryRun {
		return
	}
	secs := s.Elapsed.Seconds()
	ratio := ""
	if s.WireOut > 0 && s.Literal >= 64<<10 {
		ratio = fmt.Sprintf(", %.1fx vs raw", float64(s.Literal+s.Matched)/float64(s.WireOut))
	}
	fmt.Fprintf(os.Stderr, "rift: wire %s out / %s in%s | delta reused %s | %d conns, %d steals | %.2fs (%s/s)\n",
		client.HumanBytes(s.WireOut), client.HumanBytes(s.WireIn), ratio,
		client.HumanBytes(s.Matched), s.Conns, s.Steals, secs,
		client.HumanBytes(int64(float64(s.Bytes)/max(secs, 1e-3))))
}

// getBuildID names the remote server binary. Release builds inject it;
// otherwise it is derived from this executable's contents so any rebuild
// deploys a fresh, matching server.
func getBuildID() string {
	if buildID != "" {
		return buildID
	}
	self, err := os.Executable()
	if err != nil {
		return version
	}
	f, err := os.Open(self)
	if err != nil {
		return version
	}
	defer f.Close()
	h := xxh3.New()
	if _, err := io.Copy(h, f); err != nil {
		return version
	}
	return hex.EncodeToString(h.Sum(nil))
}
