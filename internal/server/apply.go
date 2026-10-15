package server

import (
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/shxntanu/rift/internal/proto"
	"github.com/shxntanu/rift/internal/scan"
)

const applyWorkers = 16

type applyJob struct {
	t proto.Type
	p []byte
}

// Fixed FIFO lanes serialize paths without per-path goroutines or locks.
// Hash collisions merely serialize unrelated paths. Each lane owns its range
// handles and encoder; only the framed response writer is shared.
type applyPool struct {
	lanes   [applyWorkers]chan applyJob
	servers [applyWorkers]*server
	wg      sync.WaitGroup
	workers sync.WaitGroup
	mu      sync.Mutex
	cond    *sync.Cond
	bytes   int
	count   int
	err     error
	ranges  map[uint64]string // receive goroutine only
}

func newApplyPool(s *server) *applyPool {
	p := &applyPool{ranges: make(map[uint64]string)}
	p.cond = sync.NewCond(&p.mu)
	for i := range p.lanes {
		q := make(chan applyJob, 256)
		p.lanes[i] = q
		worker := &server{c: s.c, root: s.root, session: s.session, ranges: make(map[uint64]*rangeFile)}
		p.servers[i] = worker
		p.workers.Go(func() {
			defer worker.cleanup()
			for j := range q {
				p.mu.Lock()
				failed := p.err != nil
				p.mu.Unlock()
				if !failed {
					if err := worker.handle(j.t, j.p); err != nil {
						p.mu.Lock()
						first := p.err == nil
						if first {
							p.err = err
						}
						p.mu.Unlock()
						if first {
							_ = worker.sendErr(0, proto.CodeFatal, err)
							// Unblock Recv even when the peer is waiting for a reply.
							_ = s.raw.Close()
						}
					}
				}
				p.mu.Lock()
				p.bytes -= len(j.p)
				j.p = nil
				p.count--
				p.cond.Broadcast()
				p.mu.Unlock()
				p.wg.Done()
			}
		})
	}
	return p
}

func (p *applyPool) submit(path string, t proto.Type, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.err == nil && (p.bytes+len(data) > 64<<20 || p.count == 256) {
		p.cond.Wait()
	}
	if p.err != nil {
		return p.err
	}
	p.bytes += len(data)
	p.count++
	p.wg.Add(1)
	h := fnv.New32a()
	_, _ = h.Write([]byte(path))
	p.lanes[h.Sum32()%applyWorkers] <- applyJob{t, append([]byte(nil), data...)}
	return nil
}

func (p *applyPool) drain() error {
	p.wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *applyPool) close() {
	for _, q := range p.lanes {
		close(q)
	}
	p.workers.Wait()
}

func (p *applyPool) handle(s *server, t proto.Type, data []byte) error {
	if s.delta != nil && t != proto.TDeltaCopy && t != proto.TDeltaData && t != proto.TDeltaEnd {
		return fmt.Errorf("message %d interleaved with delta sequence", t)
	}
	d := proto.Dec{B: data}
	var path string
	switch t {
	case proto.TPut, proto.TSymlink, proto.TRangeWrite, proto.TFinalize:
		id := d.Uvarint()
		if t == proto.TPut {
			d.U8()
		}
		path = d.Str()
		if t == proto.TRangeWrite {
			if old, ok := p.ranges[id]; ok && old != path {
				return fmt.Errorf("range %d changed path", id)
			}
			p.ranges[id] = path
		}
	case proto.TRangeClose:
		id := d.Uvarint()
		path = p.ranges[id]
		delete(p.ranges, id)
	case proto.TMeta:
		n := d.Uvarint()
		for range n {
			before := d.B
			path = d.Str()
			kind := scan.Kind(d.U8())
			d.Uvarint()
			d.Varint()
			if d.Err != nil {
				return d.Err
			}
			// Split batches into independent one-entry metadata messages.
			entry := append([]byte{1}, before[:len(before)-len(d.B)]...)
			if kind == scan.KindDir {
				// Directory mtimes depend on every earlier child write, even
				// when the final metadata is folded into a single connection.
				if err := p.drain(); err != nil {
					return err
				}
				if err := s.handle(t, entry); err != nil {
					return err
				}
				continue
			}
			if err := p.submit(path, t, entry); err != nil {
				return err
			}
		}
		return d.Err
	default:
		// Namespace changes, reads, barriers, and every delta instruction are
		// fences. No asynchronous operation can cross a delta sequence.
		if err := p.drain(); err != nil {
			return err
		}
		if err := s.handle(t, data); err != nil {
			return err
		}
		if t == proto.TNewSession {
			// All workers are idle at this fence; future channel sends publish
			// the new root/session before they execute another operation.
			for _, worker := range p.servers {
				worker.cleanup()
				worker.root, worker.session = s.root, s.session
			}
			clear(p.ranges)
		}
		return nil
	}
	if d.Err != nil {
		return d.Err
	}
	return p.submit(path, t, data)
}
