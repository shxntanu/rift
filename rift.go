// Package rift synchronizes a local directory to a local directory or an SSH
// destination using Rift's parallel transfer and content-defined delta engine.
//
// Local transfers run in-process. Remote transfers require OpenSSH and matching
// Rift receiver binaries supplied through SSHOptions.ServerBinaries; missing
// receivers are deployed over SSH. Rift does not speak the rsync wire protocol.
// Sources must be directories. Linux and macOS are supported.
package rift

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shxntanu/rift/internal/client"
	"github.com/shxntanu/rift/internal/transport"
)

// Endpoint identifies a destination. An empty Host selects a local directory;
// otherwise Host is an OpenSSH host alias or [user@]hostname.
type Endpoint struct {
	Host string
	Path string
}

// SSHOptions configures remote connections. Authentication is noninteractive
// and uses OpenSSH's normal config files and agent. No SSH masters are left
// running after the operation or runner closes.
type SSHOptions struct {
	Command string   // SSH executable; defaults to "ssh".
	Args    []string // Arguments before the destination host, e.g. {"-p", "2222"}.
	// ServerBinaries maps "goos/goarch" (e.g. "linux/amd64") to a matching
	// Rift executable for that remote platform. Use receivers built from the
	// same Rift revision as the library. No binaries are downloaded or built.
	ServerBinaries    map[string]string
	DisableFastCipher bool // Disable Rift's preference for hardware-accelerated AES-GCM.
}

// Options configures a sync. The zero value enables compression and deltas,
// automatically sizes connections, and preserves extra destination files.
// Callers must not mutate options or their slices/maps during a sync.
type Options struct {
	Delete           bool     // Remove destination entries missing from the source.
	Checksum         bool     // Compare contents even when size and mtime match.
	DryRun           bool     // Plan without modifying the destination tree.
	Rules            []string // Ordered rsync filter rules, e.g. "- *.o" or "P /build/***".
	GitIgnore        bool     // Use git file membership; explicitly invokes git.
	SizeOnly         bool
	Update           bool // Skip files newer on the receiver.
	IgnoreExisting   bool
	ModifyWindow     int64 // Mtime comparison tolerance in seconds; defaults to zero.
	Conns            int   // Maximum connections, 1-32; zero defaults to 32.
	CompressionLevel int   // 0 defaults to level 1; -1 disables; otherwise 1-4.
	DeltaMin         int64 // Minimum delta file size in bytes; zero defaults to 128 KiB.
	WholeFile        bool  // Disable delta transfers.
	SSH              SSHOptions
	Log              io.Writer // Optional diagnostic writer; nil discards output.
	// OnProgress receives serial snapshots until Sync returns. Return promptly;
	// do not synchronously call Sync or Close on the same runner from a callback.
	OnProgress func(Progress)
}

// Summary describes applied changes, or planned changes for a dry run.
// A partial failure returns both a summary and a *PartialError.
type Summary struct {
	Scanned       int
	RemoteScanned int
	Transferred   int64
	Bytes         int64 // Logical bytes of transferred files.
	Literal       int64 // File bytes sent, excluding delta matches.
	Matched       int64 // File bytes reused from the destination.
	MetaOnly      int
	Deleted       int
	WireOut       int64 // Transport bytes; local transfers count protocol bytes.
	WireIn        int64
	Conns         int
	Steals        int
	Errors        []string // Up to 50 failure messages; ErrorCount includes omitted messages.
	ErrorCount    int
	ScanTime      time.Duration
	Elapsed       time.Duration
	Platform      string
}

// FileProgress describes one file's current transfer attempt.
type FileProgress struct {
	Path             string
	BytesDone, Total int64
	State            string // "queued", "checking", "sending", "done", or "failed".
}

// Progress is a snapshot with up to 32 files, prioritizing active transfers.
type Progress struct {
	Phase                 string
	BytesDone, BytesTotal int64
	FilesDone, FilesTotal int
	Failed                int
	WireOut               int64
	Conns                 int
	Elapsed               time.Duration
	Rate                  float64 // Smoothed logical bytes per second.
	Files                 []FileProgress
}

// PartialError reports failures of individual operations after a sync completes.
// Other operations may have succeeded; a sync is not a whole-tree transaction.
type PartialError struct {
	Count  int
	Errors []string // Up to 50 messages.
}

func (e *PartialError) Error() string { return fmt.Sprintf("rift: %d operations failed", e.Count) }

// ErrClosed is returned when a runner has been closed.
var ErrClosed = errors.New("rift: runner is closed")

// Sync synchronizes src to dst and closes all owned connections before returning.
// A trailing slash on src copies its contents; otherwise its directory name is
// appended to dst.Path. Symlinks, permissions and mtimes are preserved, but file
// ownership, hard links, ACLs and extended attributes are not synchronized.
//
// Cancellation may leave applied changes and incomplete new files. DryRun does
// not modify the destination tree, but a remote receiver may still be deployed
// into ~/.rift/bin. Destination trees must not be under hostile concurrent
// mutation; the receiver is not a filesystem sandbox.
func Sync(ctx context.Context, src string, dst Endpoint, opts Options) (*Summary, error) {
	opt, err := options(ctx, src, dst, opts)
	if err != nil {
		return nil, err
	}
	sum, err := client.Run(ctx, opt)
	return result(ctx, sum, err)
}

// Runner retains connections and watched scans between syncs. Concurrent Sync
// calls on one runner are serialized; separate runners do not coordinate writes
// to overlapping destinations. Use NewRunner to create a runner and Close it
// when finished. A Runner must not be copied.
type Runner struct {
	runner *client.Runner
	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool
	once   sync.Once
}

// NewRunner creates a runner whose sessions live until Close or ctx cancellation.
func NewRunner(ctx context.Context) *Runner {
	ctx, cancel := context.WithCancel(ctx)
	return &Runner{runner: client.NewRunner(ctx), ctx: ctx, cancel: cancel}
}

// Sync synchronizes directories with the same semantics as Sync, reusing idle
// sessions. Changing destination, receiver binaries, or transport settings
// invalidates retained sessions.
func (r *Runner) Sync(ctx context.Context, src string, dst Endpoint, opts Options) (*Summary, error) {
	if r.closed.Load() {
		return nil, ErrClosed
	}
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer cancel()
	defer stop()
	opt, err := options(ctx, src, dst, opts)
	if err != nil {
		return nil, err
	}
	sum, err := r.runner.Run(ctx, opt)
	if err != nil {
		err = errors.Join(err, r.ctx.Err())
		if r.closed.Load() {
			err = errors.Join(err, ErrClosed)
		}
	}
	return result(ctx, sum, err)
}

// Close cancels active work, closes retained sessions and filesystem watchers,
// and waits for owned workers. It is safe to call concurrently and repeatedly.
func (r *Runner) Close() error {
	r.closed.Store(true)
	r.once.Do(func() { r.cancel(); r.runner.Close() })
	return nil
}

func options(ctx context.Context, src string, dst Endpoint, opts Options) (client.Options, error) {
	var opt client.Options
	if err := ctx.Err(); err != nil {
		return opt, err
	}
	if opts.Conns < 0 || opts.Conns > 32 {
		return opt, errors.New("rift: Conns must be between 0 and 32")
	}
	if opts.CompressionLevel < -1 || opts.CompressionLevel > 4 {
		return opt, errors.New("rift: CompressionLevel must be between -1 and 4")
	}
	if opts.DeltaMin < 0 || opts.ModifyWindow < 0 || opts.ModifyWindow > math.MaxInt64/int64(time.Second) {
		return opt, errors.New("rift: invalid DeltaMin or ModifyWindow")
	}
	if strings.HasPrefix(dst.Host, "-") || strings.ContainsAny(dst.Host, "\x00\r\n \t") {
		return opt, errors.New("rift: invalid SSH host")
	}
	opt = client.Options{
		Src: src, Dst: transport.Endpoint{Host: dst.Host, Path: client.DestinationRoot(src, dst.Path)},
		Conns: opts.Conns, Compress: opts.CompressionLevel, DeltaMin: opts.DeltaMin,
		Delete: opts.Delete, Checksum: opts.Checksum, DryRun: opts.DryRun,
		Rules: append([]string(nil), opts.Rules...), GitIgnore: opts.GitIgnore,
		SizeOnly: opts.SizeOnly, Update: opts.Update, IgnoreExisting: opts.IgnoreExisting,
		ModifyWindow: opts.ModifyWindow, Log: opts.Log,
		Transport: transport.Options{SSH: opts.SSH.Command, SSHArgs: append([]string(nil), opts.SSH.Args...),
			FastCipher: !opts.SSH.DisableFastCipher, NonInteractive: true},
	}
	if opt.Conns == 0 {
		opt.Conns = 32
	}
	if opt.Compress == 0 {
		opt.Compress = 1
	}
	if opt.Compress == -1 {
		opt.Compress = 0
	}
	if opt.DeltaMin == 0 {
		opt.DeltaMin = 128 << 10
	}
	if opts.WholeFile {
		opt.DeltaMin = math.MaxInt64
	}
	if opt.Log == nil {
		opt.Log = io.Discard
	}
	opt.Transport.Log = opt.Log
	if opts.OnProgress != nil {
		opt.Rich = true
		opt.OnProgress = func(p client.Progress) {
			files := make([]FileProgress, len(p.Files))
			for i, f := range p.Files {
				files[i] = FileProgress(f)
			}
			opts.OnProgress(Progress{Phase: p.Phase, BytesDone: p.BytesDone, BytesTotal: p.BytesTotal,
				FilesDone: p.FilesDone, FilesTotal: p.FilesTotal, Failed: p.Failed,
				WireOut: p.WireOut, Conns: p.Conns, Elapsed: p.Elapsed, Rate: p.Rate, Files: files})
		}
	}
	if dst.Host != "" {
		if len(opts.SSH.ServerBinaries) == 0 {
			return opt, errors.New("rift: remote sync requires SSH.ServerBinaries")
		}
		opt.Transport.ServerBinaries = make(map[string]string, len(opts.SSH.ServerBinaries))
		keys := make([]string, 0, len(opts.SSH.ServerBinaries))
		for key, path := range opts.SSH.ServerBinaries {
			opt.Transport.ServerBinaries[key] = path
			keys = append(keys, key)
		}
		sort.Strings(keys)
		h := sha256.New()
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return opt, err
			}
			path := opt.Transport.ServerBinaries[key]
			fi, err := os.Stat(path)
			if err != nil {
				return opt, fmt.Errorf("rift: receiver %s: %w", key, err)
			}
			if !fi.Mode().IsRegular() {
				return opt, fmt.Errorf("rift: receiver %s must be a regular file", key)
			}
			f, err := os.Open(path)
			if err != nil {
				return opt, fmt.Errorf("rift: receiver %s: %w", key, err)
			}
			binaryHash := sha256.New()
			_, copyErr := io.Copy(binaryHash, contextReader{ctx, f})
			closeErr := f.Close()
			if err := errors.Join(copyErr, closeErr); err != nil {
				return opt, err
			}
			// hash.Hash.Write never returns an error.
			_, _ = fmt.Fprintf(h, "%s:%x\n", key, binaryHash.Sum(nil))
		}
		opt.Transport.BuildID = fmt.Sprintf("lib-%x", h.Sum(nil))
	}
	return opt, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func result(ctx context.Context, sum *client.Summary, err error) (*Summary, error) {
	if err != nil {
		err = errors.Join(err, ctx.Err())
	}
	if sum == nil {
		return nil, err
	}
	if sum.ErrorCount > 0 {
		err = errors.Join(err, &PartialError{Count: sum.ErrorCount, Errors: append([]string(nil), sum.Errors...)})
	}
	return (*Summary)(sum), err
}
