// Package client drives a rift sync: it scans both sides, plans the minimal
// set of operations, and executes them over a pool of connections.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shxntanu/rift/internal/proto"
	"github.com/shxntanu/rift/internal/scan"
	"github.com/shxntanu/rift/internal/transport"
)

// Options configure a sync.
type Options struct {
	Src       string
	Dst       transport.Endpoint
	Conns     int
	Compress  int // zstd level 1-4; 0 disables
	Delete    bool
	Checksum  bool
	DryRun    bool
	Rules     []string // rsync filter rules, e.g. "- *.o", "P /build/***", ":- .gitignore"
	GitIgnore bool     // list files with git ls-files instead of walking
	DeltaMin  int64

	SizeOnly       bool
	Update         bool  // skip files that are newer on the receiver
	IgnoreExisting bool  // skip files that already exist on the receiver
	ModifyWindow   int64 // seconds of mtime slack when comparing
	Verbose        bool
	Progress       bool
	Rich           bool
	OnProgress     func(Progress) `json:"-"` // called serially; return promptly
	Transport      transport.Options
	Log            io.Writer `json:"-"`
}

// Summary describes a completed sync.
type Summary struct {
	Scanned       int
	RemoteScanned int
	Transferred   int64
	Bytes         int64 // logical bytes of transferred files
	Literal       int64 // file bytes actually sent
	Matched       int64 // bytes reused from the remote via delta
	MetaOnly      int
	Deleted       int
	WireOut       int64
	WireIn        int64
	Conns         int
	Steals        int
	Errors        []string
	ErrorCount    int
	ScanTime      time.Duration
	Elapsed       time.Duration
	Platform      string
}

type fileTask struct {
	id        uint64
	e         scan.Entry
	isNew     bool
	remaining atomic.Int64
	failed    atomic.Bool
	progress  atomic.Int64 // logical bytes sent in the current attempt
	state     atomic.Int32
}

type stats struct {
	bytesTotal atomic.Int64
	bytesDone  atomic.Int64
	filesTotal atomic.Int64
	filesDone  atomic.Int64
	literal    atomic.Int64
	matched    atomic.Int64
	metaOnly   atomic.Int64 // large files found identical after a delta signature
}

type session struct {
	opt    Options
	id     string
	src    string
	dialer *transport.Dialer
	runner *Runner
	ctx    context.Context
	sched  *scheduler
	st     stats
	files  []*fileTask

	rtt            time.Duration
	remoteRoot     string
	remotePlatform string

	connMu  sync.Mutex
	conns   []*rconn
	seq     atomic.Uint64
	workers []*workerDial // speculative and selected worker handshakes.

	errMu    sync.Mutex
	errs     []string
	errCount int

	logMu        sync.Mutex
	start        time.Time
	debug        bool
	progressPlan atomic.Pointer[progressPlan]
	wireStart    int64
}

func (s *session) debugf(format string, args ...any) {
	if !s.debug {
		return
	}
	s.logMu.Lock()
	fmt.Fprintf(s.opt.Log, "[%7.1fms] "+format+"\n", append([]any{float64(time.Since(s.start).Microseconds()) / 1000}, args...)...)
	s.logMu.Unlock()
}

func (s *session) file(id uint64) *fileTask {
	if id == 0 || id >= uint64(len(s.files)) {
		return nil
	}
	return s.files[id]
}

func (s *session) localPath(rel string) string {
	if rel == "" {
		return s.src
	}
	return s.src + "/" + rel
}

func (s *session) logf(format string, args ...any) {
	if !s.opt.Verbose {
		return
	}
	s.logMu.Lock()
	fmt.Fprintf(s.opt.Log, format+"\n", args...)
	s.logMu.Unlock()
}

func (s *session) recordErr(msg string) {
	s.errMu.Lock()
	s.errCount++
	if len(s.errs) < 50 {
		s.errs = append(s.errs, msg)
	}
	s.errMu.Unlock()
}

func (s *session) onServerError(id uint64, code uint8, msg string) {
	f := s.file(id)
	if f == nil {
		s.recordErr("remote: " + msg)
		return
	}
	if code == proto.CodeDeltaFailed {
		s.logf("delta failed for %s (%s), resending in full", f.e.Path, msg)
		f.isNew = false
		f.progress.Store(0)
		f.state.Store(fileSending)
		s.sched.push(nil, s.fullJobs(f), nil, s.fullPending(f))
		return
	}
	f.failed.Store(true)
	s.recordErr(fmt.Sprintf("remote %s: %s", f.e.Path, msg))
}

// fullJobs returns the jobs that send f in its entirety.
func (s *session) fullJobs(f *fileTask) []job {
	if f.e.Size <= proto.ChunkSize {
		return []job{{kind: jobPut, f: f}}
	}
	f.remaining.Store(f.e.Size)
	f.failed.Store(false)
	return []job{{kind: jobRange, f: f, rt: &rangeTask{f: f, end: f.e.Size}}}
}

func (s *session) fullPending(f *fileTask) int {
	if f.e.Size <= proto.ChunkSize {
		return 0
	}
	return 1
}

// rangeProgress records bytes acknowledged for a large file; once every
// byte is in, the file is renamed into place by a finalize job.
func (s *session) rangeProgress(f *fileTask, n int64) {
	if f.remaining.Add(-n) != 0 {
		return
	}
	if f.failed.Load() {
		s.sched.push(nil, nil, nil, -1)
		return
	}
	s.sched.push([]job{{kind: jobFinalize, f: f}}, nil, nil, -1)
}

func (s *session) addConn(rc *rconn) {
	s.connMu.Lock()
	s.conns = append(s.conns, rc)
	s.connMu.Unlock()
}

func (s *session) liveConns() []*rconn {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return append([]*rconn(nil), s.conns...)
}

// barrier waits until every connection has processed everything sent so
// far, which also guarantees every server error has been received.
func (s *session) barrier(conns []*rconn) error {
	seq := s.seq.Add(1)
	for _, rc := range conns {
		s.sched.pushOwn(rc.id, job{kind: jobFunc, fn: func(rc *rconn) error { return rc.sync(seq) }})
	}
	for _, rc := range conns {
		if err := rc.waitSync(seq, s.sched.abortCh); err != nil {
			return err
		}
	}
	return nil
}

func newSessionID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Run performs a sync.
func Run(ctx context.Context, opt Options) (*Summary, error) {
	return run(ctx, opt, nil)
}

func run(ctx context.Context, opt Options, runner *Runner) (*Summary, error) {
	start := time.Now()
	if opt.Log == nil {
		opt.Log = os.Stderr
	}
	if opt.Conns < 1 {
		opt.Conns = 1
	}
	src, err := filepath.Abs(opt.Src)
	if err != nil {
		return nil, err
	}
	s := &session{
		opt:    opt,
		id:     newSessionID(),
		src:    src,
		dialer: transport.NewDialer(opt.Dst, opt.Transport),
		sched:  newScheduler(),
		files:  []*fileTask{nil},
		start:  start,
		debug:  os.Getenv("RIFT_DEBUG") != "",
	}
	s.runner = runner
	if runner != nil && !opt.Transport.Direct {
		s.dialer = runner.dialer
	}
	wireOut, wireIn := s.dialer.WireOut.Load(), s.dialer.WireIn.Load()
	s.wireStart = wireOut
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if runner == nil || opt.Transport.Direct {
		defer func() { _ = s.dialer.Close() }()
	}
	s.ctx = ctx
	stopProgress := func() {}
	if opt.Rich && opt.OnProgress != nil && !opt.DryRun {
		s.progressPlan.Store(&progressPlan{phase: "Connecting and scanning"})
		stopProgress = s.startProgress()
	}
	defer func() { stopProgress() }()
	stopRunner := func() bool { return false }
	if runner != nil {
		stopRunner = context.AfterFunc(runner.ctx, cancel)
	}
	defer stopRunner()

	filter, err := scan.NewFilter(opt.Rules)
	if err != nil {
		return nil, err
	}

	// scan locally while SSH connects, then pipeline the scan
	// request behind the hello without waiting for its acknowledgement.
	type localScan struct {
		res *scan.Result
		err error
	}
	localCh := make(chan localScan, 1)
	localDone := make(chan struct{})
	defer func() { cancel(); <-localDone }()
	go func() {
		defer close(localDone)
		var r localScan
		if opt.GitIgnore {
			r.res, r.err = scan.GitList(ctx, src, filter)
		} else {
			if runner != nil {
				if runner.cache == nil || runner.src != src {
					if runner.cache != nil {
						runner.cache.Close()
					}
					runner.cache = scan.NewCache(src)
					runner.src = src
				}
				r.res, r.err = runner.cache.Scan(ctx, filter)
			} else {
				r.res, r.err = scan.Walk(ctx, src, scan.Options{Filter: filter})
			}
		}
		localCh <- r
	}()

	var ls localScan
	ctrl, err := s.connect(ctx, false, 0, func() ([]string, error) {
		select {
		case ls = <-localCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if ls.err != nil {
			return nil, ls.err
		}
		remoteSpecs := filter.RemoteScanSpecs(ls.res.Entries)
		if opt.GitIgnore {
			remoteSpecs = append([]string{"- /.git"}, remoteSpecs...)
			remoteSpecs = append(remoteSpecs, ":- .gitignore")
		}
		return remoteSpecs, nil
	})
	if err != nil {
		return nil, err
	}
	s.debugf("primary connection ready (%s), hello RTT %s", s.remotePlatform, s.rtt.Round(time.Microsecond))
	s.addConn(ctrl)
	stopIO := context.AfterFunc(ctx, func() {
		for _, rc := range s.liveConns() {
			_ = rc.tc.CloseRead()
			_ = rc.tc.CloseWrite()
		}
	})
	defer stopIO()
	var spawnWG sync.WaitGroup
	stopReaders := func() {}
	var shutdownOnce sync.Once
	var shutdownErr error
	// use graceful teardown to confirm the final batch before
	// building the summary. Keep it deferred as well for early-return cleanup.
	shutdown := func() error {
		shutdownOnce.Do(func() {
			s.stopWarmWorkers()
			if s.sched.failed() != nil || ctx.Err() != nil {
				// Kill the processes so workers blocked on a dead pipe return.
				cancel()
			}
			s.sched.close()
			s.sched.waitWorkers()
			stopReaders()
			conns := s.liveConns()
			s.debugf("closing %d connections", len(conns))
			var wg sync.WaitGroup
			closeErrs := make([]error, len(conns))
			for i, rc := range conns {
				wg.Add(1)
				go func() {
					defer wg.Done()
					closeErrs[i] = rc.close()
				}()
			}
			wg.Wait()
			s.debugf("connections closed")
			if runner == nil || opt.Transport.Direct {
				_ = s.dialer.Close()
			}
			stopIO()
			cancel()
			spawnWG.Wait()
			shutdownErr = errors.Join(closeErrs...)
			if shutdownErr == nil {
				shutdownErr = s.sched.failed()
			}
			s.debugf("done")
		})
		return shutdownErr
	}
	defer func() { _ = shutdown() }() // Successful runs collect teardown errors below.

	s.debugf("local scan: %d entries", len(ls.res.Entries))
	// overlap worker handshakes with the remote scan; planning
	// will retain only the connections justified by the actual transfer.
	s.warmWorkers(ctx, &spawnWG, ls.res.Entries)

	remote, remoteWarn, err := collectScan(ctrl, s.sched.abortCh)
	if err != nil {
		return nil, err
	}
	s.debugf("remote scan: %d entries", len(remote))
	for _, w := range ls.res.Warnings {
		s.recordErr(w.Error())
	}
	for _, w := range remoteWarn {
		s.recordErr("remote: " + w)
	}
	scanTime := time.Since(start)
	if opt.Rich {
		s.progressPlan.Store(&progressPlan{phase: "Planning"})
	}

	p, err := s.plan(ctx, ctrl, ls.res, remote, filter)
	if err != nil {
		return nil, err
	}
	s.debugf("plan: %d transfers, %d deltas, %d meta, %d deletes", len(p.transfers), len(p.sigs), len(p.metas), len(p.deletes))

	sum := &Summary{
		Scanned:       len(ls.res.Entries),
		RemoteScanned: len(remote),
		ScanTime:      scanTime,
		Platform:      s.remotePlatform,
		MetaOnly:      len(p.metas),
		Deleted:       len(p.deletes),
	}
	if opt.DryRun {
		p.print(opt.Log)
		sum.Transferred = int64(len(p.transfers))
		for _, f := range p.transfers {
			sum.Bytes += f.e.Size
		}
		sum.Elapsed = time.Since(start)
		sum.Conns = 1
		return sum, nil
	}

	if opt.Rich {
		s.progressPlan.Store(&progressPlan{phase: "Transferring", files: s.files[1:]})
	} else if opt.Progress && !opt.Verbose {
		stopProgress = s.startProgress()
	}

	if err := ctrl.sendDeletes(p.conflicts, proto.DeleteRecursive); err != nil {
		return nil, err
	}
	if err := ctrl.sendMkdirs(p.mkdirs); err != nil {
		return nil, err
	}
	// workers create missing parents themselves, so only type
	// conflicts need a directory-creation barrier before workers start.
	if len(p.conflicts) > 0 {
		seq := s.seq.Add(1)
		if err := ctrl.sync(seq); err != nil {
			return nil, err
		}
		if err := ctrl.waitSync(seq, s.sched.abortCh); err != nil {
			return nil, err
		}
	}

	stopReaders = s.startReaders()
	s.spawnWorkers(ctx, &spawnWG, p)
	ctrl.startWork()
	finalBatchFolded := false
	if s.pushPlan(p) {
		// Barriers make every server confirm it applied everything; delta
		// failures reported before the ack queue full resends, so repeat.
		for {
			if err := s.sched.waitIdle(); err != nil {
				return nil, err
			}
			// One stream orders file writes before directory times. With several
			// streams, wait for their acknowledgements before setting final times.
			finalBatchFolded = runner != nil && len(s.liveConns()) == 1
			if finalBatchFolded {
				s.sched.pushOwn(0, job{kind: jobFunc, fn: func(rc *rconn) error {
					if err := rc.sendDeletes(p.deletes, 0); err != nil {
						return err
					}
					return rc.sendMeta(p.dirMetas)
				}})
			}
			if err := s.barrier(s.liveConns()); err != nil {
				return nil, err
			}
			// An error before an ack can start a resend on another worker.
			// Wait for it, then repeat only if frames remain unconfirmed;
			// counting a barrier worker still returning would add an RTT.
			if err := s.sched.waitIdle(); err != nil {
				return nil, err
			}
			pending := false
			for _, rc := range s.liveConns() {
				pending = pending || rc.unconfirmed()
			}
			if !pending {
				break
			}
		}
	}

	// Send the remaining deletes and directory times after all file writes.
	// Teardown confirms them with EOF or a barrier on persistent streams.
	if !finalBatchFolded && (len(p.deletes) > 0 || len(p.dirMetas) > 0) {
		s.sched.pushOwn(0, job{kind: jobFunc, fn: func(rc *rconn) error {
			if err := rc.sendDeletes(p.deletes, 0); err != nil {
				return err
			}
			return rc.sendMeta(p.dirMetas)
		}})
		if err := s.sched.waitIdle(); err != nil {
			return nil, err
		}
	}
	if opt.Rich {
		s.progressPlan.Store(&progressPlan{phase: "Finishing", files: s.files[1:]})
	} else {
		stopProgress()
	}
	s.debugf("transfer complete")
	// Collect errors from the final batch before taking the summary.
	if err := shutdown(); err != nil {
		return nil, err
	}

	s.sched.mu.Lock()
	sum.Steals = s.sched.steals
	s.sched.mu.Unlock()
	sum.Conns = len(s.liveConns())
	sum.Transferred = s.st.filesDone.Load() - s.st.metaOnly.Load()
	sum.MetaOnly += int(s.st.metaOnly.Load())
	sum.Bytes = s.st.bytesTotal.Load()
	sum.Literal = s.st.literal.Load()
	sum.Matched = s.st.matched.Load()
	sum.WireOut = s.dialer.WireOut.Load() - wireOut
	sum.WireIn = s.dialer.WireIn.Load() - wireIn
	s.errMu.Lock()
	sum.Errors = s.errs
	sum.ErrorCount = s.errCount
	s.errMu.Unlock()
	sum.Elapsed = time.Since(start)
	return sum, nil
}

// keep speculative connections out of the active pool until
// planning confirms it is useful. Only the Run goroutine chooses workers.
type workerDial struct {
	use     chan struct{}
	cancel  context.CancelFunc
	adopted bool
}

// use the existing 4 MiB/256-file sizing for both warming
// and the actual plan; only planned deltas get the extra worker allowance.
func (s *session) workerCount(bytes int64, files, deltas int) int {
	var window int64
	if s.dialer != nil {
		window, _ = s.dialer.ChannelWindow()
	}
	return connectionCount(bytes, files, deltas, s.opt.Conns, s.rtt, window)
}

// overlap worker handshakes with the remote scan. Local file
// sizes estimate useful parallelism; planning discards unnecessary attempts.
func (s *session) warmWorkers(ctx context.Context, wg *sync.WaitGroup, entries []scan.Entry) {
	if s.opt.DryRun {
		return
	}
	var bytes int64
	var files int
	for _, e := range entries {
		if e.Kind == scan.KindFile {
			bytes += e.Size
			files++
		}
	}
	for i := 1; i < s.workerCount(bytes, files, 0); i++ {
		s.workers = append(s.workers, s.dialWorker(ctx, wg, i))
	}
}

// cancel unused handshakes, including on an early return.
// Adopted workers stay alive for graceful teardown after their final ack.
func (s *session) stopWarmWorkers() {
	for _, w := range s.workers {
		if !w.adopted {
			w.cancel()
		}
	}
}

// select warmed connections and fill any remaining slots.
// Workers join as their handshakes finish, so transfers never wait on them.
func (s *session) spawnWorkers(ctx context.Context, wg *sync.WaitGroup, p *plan) {
	// Delta files mostly send copy instructions, so they count as files,
	// not bytes.
	var bytes int64
	for _, f := range p.transfers {
		bytes += f.e.Size
	}
	files := len(p.transfers) + len(p.sigs)
	want := s.workerCount(bytes, files, len(p.sigs))
	_, info := s.dialer.ChannelWindow()
	s.debugf("connections: %d, hello RTT=%s, %s", want, s.rtt.Round(time.Microsecond), info)
	for i := 1; i < want; i++ {
		if i > len(s.workers) {
			s.workers = append(s.workers, s.dialWorker(ctx, wg, i))
		}
		w := s.workers[i-1]
		w.adopted = true
		close(w.use)
	}
	s.stopWarmWorkers()
}

func (s *session) dialWorker(ctx context.Context, wg *sync.WaitGroup, id int) *workerDial {
	ctx, cancel := context.WithCancel(ctx)
	w := &workerDial{use: make(chan struct{}), cancel: cancel}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		rc, err := s.connect(ctx, true, id, nil)
		if rc != nil {
			defer func() {
				// Optimization 5: discarded attempts are optional; teardown
				// collects the stored close error for every adopted connection.
				_ = rc.close()
			}()
		}
		select {
		case <-w.use:
		case <-ctx.Done():
			return
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if id == 1 {
				s.logf("extra connections unavailable (%v); continuing with fewer", err)
			}
			return
		}
		// join before publishing this connection. Shutdown
		// closes admission and waits for every joined worker before its snapshot.
		if !s.sched.join() {
			return
		}
		rc.dispatchMu.Lock()
		rc.optional.Store(false)
		if rc.rerr != nil {
			s.sched.abort(rc.rerr)
		}
		rc.dispatchMu.Unlock()
		rc.startRead()
		s.addConn(rc)
		s.debugf("connection %d ready", id)
		rc.workJoined()
	}()
	return w
}

// connectionCount uses a 1 GiB/s target to estimate the bandwidth-delay
// product. This is a sizing target, not a measured bandwidth claim. Planned
// work and the user's cap still limit fan-out; low-RTT/local syncs cap at 8.
func connectionCount(bytes int64, files, sigs, limit int, rtt time.Duration, window int64) int {
	work := 1 + bytes/(4<<20) + int64(files/256+sigs/4)
	ceiling := int64(8)
	if window > 0 && rtt > 0 {
		bdp := float64(1<<30) * rtt.Seconds()
		ceiling = max(ceiling, int64(min(32, math.Ceil(bdp/float64(window)))))
	}
	return int(max(1, min(work, ceiling, 32, int64(limit))))
}

func collectScan(rc *rconn, abort <-chan struct{}) ([]scan.Entry, []string, error) {
	var entries []scan.Entry
	for {
		select {
		case m := <-rc.scanCh:
			if m.done {
				return entries, m.warnings, nil
			}
			entries = append(entries, m.entries...)
		case <-rc.dead:
			if rc.rerr != nil {
				return nil, nil, rc.rerr
			}
			return nil, nil, errors.New("connection closed during scan")
		case <-abort:
			return nil, nil, rc.s.sched.failed()
		}
	}
}

type plan struct {
	conflicts []string
	mkdirs    []scan.Entry
	transfers []*fileTask
	sigs      []*fileTask
	symlinks  []*fileTask
	metas     []metaUpdate
	dirMetas  []metaUpdate
	deletes   []string
}

func (p *plan) print(w io.Writer) {
	for _, c := range p.conflicts {
		fmt.Fprintf(w, "*replace %s\n", c)
	}
	for _, d := range p.mkdirs {
		fmt.Fprintf(w, "cd+ %s/\n", d.Path)
	}
	for _, f := range p.transfers {
		fmt.Fprintf(w, ">f %s\n", f.e.Path)
	}
	for _, f := range p.sigs {
		fmt.Fprintf(w, ">d %s\n", f.e.Path)
	}
	for _, f := range p.symlinks {
		fmt.Fprintf(w, ">l %s -> %s\n", f.e.Path, f.e.Link)
	}
	for _, m := range p.metas {
		fmt.Fprintf(w, ".t %s\n", m.path)
	}
	for _, d := range p.deletes {
		fmt.Fprintf(w, "*delete %s\n", d)
	}
}

func (s *session) newTask(e scan.Entry, isNew bool) *fileTask {
	f := &fileTask{id: uint64(len(s.files)), e: e, isNew: isNew}
	s.files = append(s.files, f)
	return f
}

func (s *session) plan(ctx context.Context, ctrl *rconn, lres *scan.Result, remote []scan.Entry,
	filter *scan.Filter) (*plan, error) {
	opt := s.opt
	local := lres.Entries
	p := &plan{}
	window := opt.ModifyWindow * int64(time.Second)
	sameTime := func(a, b int64) bool {
		d := a - b
		return d == 0 || (d <= window && -d <= window)
	}
	rmap := make(map[string]*scan.Entry, len(remote))
	for i := range remote {
		rmap[remote[i].Path] = &remote[i]
	}
	lmap := make(map[string]*scan.Entry, len(local))
	for i := range local {
		lmap[local[i].Path] = &local[i]
	}
	touched := map[string]bool{}
	conflicts := map[string]bool{}
	touch := func(rel string) {
		touched[parentOf(rel)] = true
	}
	var hashCands []*scan.Entry

	transfer := func(e scan.Entry, r *scan.Entry) {
		if r != nil && e.Size >= opt.DeltaMin && r.Size > 0 && r.Kind == scan.KindFile {
			p.sigs = append(p.sigs, s.newTask(e, false))
		} else {
			p.transfers = append(p.transfers, s.newTask(e, r == nil))
		}
		touch(e.Path)
	}

	for i := range local {
		l := &local[i]
		r := rmap[l.Path]
		if r != nil && r.Kind != l.Kind && l.Path != "" {
			p.conflicts = append(p.conflicts, l.Path)
			conflicts[l.Path] = true
			touch(l.Path)
			r = nil
		}
		switch l.Kind {
		case scan.KindDir:
			if r == nil {
				p.mkdirs = append(p.mkdirs, *l)
				touched[l.Path] = true
				touch(l.Path)
			} else if r.Mode != l.Mode || r.Mtime != l.Mtime {
				touched[l.Path] = true
			}
		case scan.KindSymlink:
			if r != nil && r.Link == l.Link {
				continue
			}
			p.symlinks = append(p.symlinks, s.newTask(*l, r == nil))
			touch(l.Path)
		case scan.KindFile:
			switch {
			case r == nil:
				transfer(*l, nil)
			case opt.IgnoreExisting:
			case opt.Update && r.Mtime > l.Mtime && !sameTime(r.Mtime, l.Mtime):
			case r.Size == l.Size && (opt.SizeOnly || sameTime(r.Mtime, l.Mtime)) && !opt.Checksum:
				if r.Mode != l.Mode {
					p.metas = append(p.metas, metaUpdate{l.Path, l.Kind, l.Mode, l.Mtime})
				}
			case r.Size == l.Size && (l.Size < opt.DeltaMin || opt.Checksum):
				hashCands = append(hashCands, l)
			default:
				transfer(*l, r)
			}
		}
	}

	if len(hashCands) > 0 {
		same, err := s.compareHashes(ctx, ctrl, hashCands)
		if err != nil {
			return nil, err
		}
		for i, l := range hashCands {
			r := rmap[l.Path]
			if same[i] {
				if r.Mode != l.Mode || !sameTime(r.Mtime, l.Mtime) {
					p.metas = append(p.metas, metaUpdate{l.Path, l.Kind, l.Mode, l.Mtime})
				}
				continue
			}
			transfer(*l, r)
		}
	}

	if opt.Delete {
		var extra []string
		for i := range remote {
			r := &remote[i]
			if r.Path == "" {
				continue
			}
			if _, ok := lmap[r.Path]; ok {
				continue
			}
			// The conflict barrier already deleted these descendants. The
			// final batch must not delete through their replacement file/link.
			if hasAncestor(r.Path, conflicts) {
				continue
			}
			if filter.Protected(r.Path, r.Kind == scan.KindDir, lres.CtxFor(r.Path)) {
				continue
			}
			extra = append(extra, r.Path)
		}
		if opt.GitIgnore && len(extra) > 0 {
			query := make([]string, len(extra))
			for i, e := range extra {
				query[i] = e
				if rmap[e].Kind == scan.KindDir {
					query[i] += "/"
				}
			}
			ignored, err := scan.GitIgnored(ctx, s.src, query)
			if err != nil {
				return nil, err
			}
			kept := extra[:0]
			for _, e := range extra {
				if !ignored[e] && !hasAncestor(e, ignored) {
					kept = append(kept, e)
				}
			}
			extra = kept
		}
		// Children before parents, so directories are empty when removed.
		sort.Sort(sort.Reverse(sort.StringSlice(extra)))
		for _, e := range extra {
			p.deletes = append(p.deletes, e)
			touch(e)
		}
	}

	for d := range touched {
		if l := lmap[d]; l != nil && l.Kind == scan.KindDir {
			p.dirMetas = append(p.dirMetas, metaUpdate{l.Path, l.Kind, l.Mode, l.Mtime})
		}
	}

	sort.Slice(p.mkdirs, func(i, j int) bool { return p.mkdirs[i].Path < p.mkdirs[j].Path })
	for _, f := range p.transfers {
		s.st.bytesTotal.Add(f.e.Size)
	}
	for _, f := range p.sigs {
		s.st.bytesTotal.Add(f.e.Size)
	}
	s.st.filesTotal.Store(int64(len(p.transfers) + len(p.sigs) + len(p.symlinks)))
	return p, nil
}

func hasAncestor(p string, paths map[string]bool) bool {
	for d := path.Dir(p); d != "." && d != "/"; d = path.Dir(d) {
		if paths[d] {
			return true
		}
	}
	return false
}

func parentOf(rel string) string {
	if i := strings.LastIndexByte(rel, '/'); i >= 0 {
		return rel[:i]
	}
	return ""
}

// compareHashes hashes candidates on both sides in parallel and reports
// which ones have identical content.
func (s *session) compareHashes(ctx context.Context, ctrl *rconn, cands []*scan.Entry) ([]bool, error) {
	const batch = 2048
	nreq := 0
	for i := 0; i < len(cands); i += batch {
		end := min(i+batch, len(cands))
		ctrl.enc.Reset()
		ctrl.enc.Uvarint(uint64(i))
		ctrl.enc.Uvarint(uint64(end - i))
		for _, c := range cands[i:end] {
			ctrl.enc.Str(c.Path)
		}
		// Optimization 2: hash replies confirm these frames too, so a
		// checksum-only no-op can skip waiting for server exit.
		if err := ctrl.send(proto.THashReq, ctrl.enc.B); err != nil {
			return nil, err
		}
		nreq++
	}
	if err := ctrl.pc.Flush(); err != nil {
		return nil, err
	}

	local := make([]proto.Hash, len(cands))
	localOK := make([]bool, len(cands))
	var wg sync.WaitGroup
	next := make(chan int, 256)
	for range runtime.NumCPU() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 256<<10)
			for i := range next {
				h, err := scan.HashFile(s.localPath(cands[i].Path), buf)
				local[i], localOK[i] = h, err == nil
			}
		}()
	}
	for i := range cands {
		next <- i
	}
	close(next)
	wg.Wait()

	same := make([]bool, len(cands))
	for range nreq {
		select {
		case r := <-ctrl.hashCh:
			for k := range r.hashes {
				i := int(r.req) + k
				if i < len(cands) && r.ok[k] && localOK[i] && r.hashes[k] == local[i] {
					same[i] = true
				}
			}
		case <-ctrl.dead:
			return nil, ctrl.rerr
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return same, nil
}

// pushPlan enqueues every transfer and reports whether there was any.
// Signature requests go first so the remote starts chunking basis files
// while bulk data is already flowing.
func (s *session) pushPlan(p *plan) bool {
	var urgent, big, small []job
	pending := 0
	for _, f := range p.sigs {
		urgent = append(urgent, job{kind: jobSigReq, f: f})
		pending++
	}
	sort.Slice(p.transfers, func(i, j int) bool { return p.transfers[i].e.Size > p.transfers[j].e.Size })
	for _, f := range p.transfers {
		js := s.fullJobs(f)
		pending += s.fullPending(f)
		if js[0].kind == jobRange {
			big = append(big, js...)
		} else {
			small = append(small, js...)
		}
	}
	for _, f := range p.symlinks {
		small = append(small, job{kind: jobSymlink, f: f})
	}
	for i := 0; i < len(p.metas); i += 4096 {
		small = append(small, job{kind: jobMeta, metas: p.metas[i:min(i+4096, len(p.metas))]})
	}
	s.sched.push(urgent, big, small, pending)
	return len(urgent)+len(big)+len(small) > 0
}
