package client

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/shxntanu/rift/internal/scan"
	"github.com/shxntanu/rift/internal/transport"
)

// Runner retains handshaken servers and a watched local scan between runs.
// Close must be called after its last Run.
type Runner struct {
	gate   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	key    string
	idle   map[int]*rconn
	dialer *transport.Dialer
	cache  *scan.Cache
	src    string
}

// NewRunner creates a pool whose processes live until Close or ctx cancellation.
func NewRunner(ctx context.Context) *Runner {
	ctx, cancel := context.WithCancel(ctx)
	return &Runner{gate: make(chan struct{}, 1), ctx: ctx, cancel: cancel, idle: map[int]*rconn{}}
}

// Run queues simultaneous syncs, including overlapping destination roots.
func (r *Runner) Run(ctx context.Context, opt Options) (*Summary, error) {
	// ponytail: serialize syncs; use destination locks if concurrent throughput matters.
	select {
	case r.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}
	defer func() { <-r.gate }()
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	key, _ := json.Marshal(struct {
		Dst       transport.Endpoint
		Transport transport.Options
		Compress  int
	}{opt.Dst, opt.Transport, opt.Compress})
	if r.key != string(key) {
		r.clear()
		r.key = string(key)
		r.dialer = transport.NewDialer(opt.Dst, opt.Transport)
	}
	return run(ctx, opt, r)
}

func (r *Runner) clear() {
	r.mu.Lock()
	idle := r.idle
	r.idle = map[int]*rconn{}
	r.mu.Unlock()
	var wg sync.WaitGroup
	for _, rc := range idle {
		wg.Add(1)
		go func() { defer wg.Done(); rc.discard() }()
	}
	wg.Wait()
	if r.cache != nil {
		r.cache.Close()
		r.cache = nil
	}
}

// Close terminates retained servers and releases filesystem watches.
func (r *Runner) Close() {
	r.cancel()
	r.gate <- struct{}{}
	defer func() { <-r.gate }()
	r.clear()
}

func (r *Runner) take(id int, s *session) *rconn {
	r.mu.Lock()
	rc := r.idle[id]
	delete(r.idle, id)
	r.mu.Unlock()
	if rc == nil {
		return nil
	}
	rc.dispatchMu.Lock()
	select {
	case <-rc.dead:
		rc.dispatchMu.Unlock()
		rc.discard()
		return nil
	default:
	}
	if rc.rerr != nil {
		rc.dispatchMu.Unlock()
		rc.discard()
		return nil
	}
	rc.needsSession = false
	rc.optional.Store(id != 0)
	rc.s = s
	rc.closeOnce = sync.Once{}
	rc.closeErr = nil
	rc.dispatchMu.Unlock()
	return rc
}
func (r *Runner) put(rc *rconn) { r.mu.Lock(); r.idle[rc.id] = rc; r.mu.Unlock() }
func (rc *rconn) discard() {
	rc.stopOnce.Do(func() { close(rc.stop) })
	rc.stopRead.Store(true)
	rc.closing.Store(true)
	_ = rc.tc.CloseRead()
	rc.startRead()
	<-rc.dead
	_ = rc.pc.Close()
}
