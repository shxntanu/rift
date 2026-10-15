package client

import (
	"errors"
	"sync"

	"github.com/shxntanu/rift/internal/proto"
)

type jobKind uint8

const (
	jobPut jobKind = iota + 1
	jobSymlink
	jobRange
	jobFinalize
	jobSigReq
	jobDelta
	jobMeta
	jobFunc
)

type job struct {
	read  *smallRead
	kind  jobKind
	f     *fileTask
	rt    *rangeTask
	sig   *signature
	metas []metaUpdate
	fn    func(*rconn) error
}

// minSteal is the smallest remaining range worth splitting; each half of a
// steal is at least half of this.
const minSteal = 4 * proto.ChunkSize

// small-file batch limits per scheduling round trip.
const (
	batchFiles = 128
	batchBytes = 8 << 20
)

// rangeTask is a contiguous byte range of one large file being sent by one
// worker. Workers claim chunk by chunk under the mutex, so a thief can
// shrink end at any moment without racing the owner's in-flight chunk.
type rangeTask struct {
	f        *fileTask
	mu       sync.Mutex
	pos, end int64
}

func (rt *rangeTask) remaining() int64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.end - rt.pos
}

func (rt *rangeTask) claim(maxN int64) (off, n int64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	n = min(maxN, rt.end-rt.pos)
	off = rt.pos
	rt.pos += n
	return off, n
}

// split hands the upper half of the unclaimed range to a new task.
func (rt *rangeTask) split() *rangeTask {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rem := rt.end - rt.pos
	if rem < minSteal {
		return nil
	}
	mid := rt.pos + rem/2
	mid = (mid + proto.ChunkSize - 1) / proto.ChunkSize * proto.ChunkSize
	if mid >= rt.end {
		return nil
	}
	nr := &rangeTask{f: rt.f, pos: mid, end: rt.end}
	rt.end = mid
	return nr
}

var errClosed = errors.New("scheduler closed")

// scheduler hands out work to connection workers.
//
// Priority order: per-connection jobs (barriers, final phase), urgent jobs
// (finalizes and deltas, which unblock completion), large files largest
// first (classic LPT scheduling to shorten the tail), then batches of small
// files. When every queue is empty, an idle worker steals the upper half of
// the largest range another worker is still sending, so the last big file is
// finished by all connections together instead of one.
type scheduler struct {
	mu      sync.Mutex
	cond    *sync.Cond
	own     map[int][]job
	urgent  []job
	big     []job
	small   []job
	active  map[*rangeTask]struct{}
	pending int // async results still expected (range acks, signatures)
	workers int
	idle    int
	err     error
	closed  bool
	abortCh chan struct{}

	readQ     chan job
	readBytes int64
	readCount int
	steals    int
}

func newScheduler() *scheduler {
	s := &scheduler{
		own:     map[int][]job{},
		active:  map[*rangeTask]struct{}{},
		abortCh: make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *scheduler) join() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.err != nil {
		return false
	}
	s.workers++
	return true
}

func (s *scheduler) leave() {
	s.mu.Lock()
	s.workers--
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *scheduler) abort(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
		close(s.abortCh)
	}
	s.cond.Broadcast()
}

func (s *scheduler) close() {
	s.mu.Lock()
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

// waitWorkers blocks until every joined worker has left. After close, no new
// worker can join, so this is a stable point for tearing connections down.
func (s *scheduler) waitWorkers() {
	s.mu.Lock()
	for s.workers > 0 {
		s.cond.Wait()
	}
	s.mu.Unlock()
}

func (s *scheduler) failed() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *scheduler) emptyLocked() bool {
	if len(s.urgent) > 0 || len(s.big) > 0 || len(s.small) > 0 {
		return false
	}
	for _, q := range s.own {
		if len(q) > 0 {
			return false
		}
	}
	return true
}

// push adds jobs and adjusts the pending counter atomically, so waitIdle can
// never observe the gap between "async result arrived" and "follow-up job
// queued".
func (s *scheduler) push(urgent, big, small []job, pendingDelta int) {
	s.mu.Lock()
	s.urgent = append(s.urgent, urgent...)
	s.big = append(s.big, big...)
	s.small = append(s.small, small...)
	s.pending += pendingDelta
	s.prefetchLocked()
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *scheduler) pushOwn(worker int, j job) {
	s.mu.Lock()
	s.own[worker] = append(s.own[worker], j)
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *scheduler) rangeDone(rt *rangeTask) {
	s.mu.Lock()
	delete(s.active, rt)
	s.mu.Unlock()
}

// next returns the next batch of jobs for worker w, blocking until work is
// available. flush is invoked (without the lock held) before blocking so
// buffered frames are not left sitting while the worker sleeps.
func (s *scheduler) next(w int, out []job, flush func() error) ([]job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	flushed := false
	for {
		if s.err != nil {
			return nil, s.err
		}
		if s.closed {
			return nil, errClosed
		}
		if q := s.own[w]; len(q) > 0 {
			s.own[w] = q[1:]
			return append(out, q[0]), nil
		}
		if len(s.urgent) > 0 && (s.urgent[0].kind != jobPut || s.urgent[0].read != nil) {
			j := s.urgent[0]
			s.urgent = s.urgent[1:]
			return append(out, j), nil
		}
		if len(s.big) > 0 && (s.big[0].kind != jobPut || s.big[0].read != nil) {
			j := s.big[0]
			s.big = s.big[1:]
			if j.rt != nil {
				s.active[j.rt] = struct{}{}
			}
			return append(out, j), nil
		}
		if len(s.small) > 0 && (s.small[0].kind != jobPut || s.small[0].read != nil) {
			var bytes int64
			n := 0
			for n < len(s.small) && n < batchFiles && bytes < batchBytes {
				if s.small[n].kind == jobPut && s.small[n].read == nil {
					break
				}
				if f := s.small[n].f; f != nil {
					bytes += f.e.Size
				}
				n++
			}
			out = append(out, s.small[:n]...)
			s.small = s.small[n:]
			return out, nil
		}
		if rt := s.stealLocked(); rt != nil {
			s.active[rt] = struct{}{}
			s.steals++
			return append(out, job{kind: jobRange, f: rt.f, rt: rt}), nil
		}
		if !flushed {
			flushed = true
			s.mu.Unlock()
			err := flush()
			s.mu.Lock()
			if err != nil {
				return nil, err
			}
			continue
		}
		s.idle++
		s.cond.Broadcast()
		s.cond.Wait()
		s.idle--
		flushed = false
	}
}

func (s *scheduler) stealLocked() *rangeTask {
	var best *rangeTask
	var bestRem int64
	for rt := range s.active {
		if rem := rt.remaining(); rem > bestRem {
			best, bestRem = rt, rem
		}
	}
	if best == nil || bestRem < minSteal {
		return nil
	}
	return best.split()
}

// waitIdle blocks until all queued work is done, no async results are
// outstanding, and every worker is waiting.
func (s *scheduler) waitIdle() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.err != nil {
			return s.err
		}
		if s.emptyLocked() && s.pending == 0 && len(s.active) == 0 && s.idle == s.workers {
			return nil
		}
		s.cond.Wait()
	}
}
