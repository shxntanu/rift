package client

import "sync"

const (
	readers    = 24
	readBudget = 64 << 20
	readSlots  = 1024
)

type smallRead struct {
	done chan struct{}
	data []byte
	err  error
	cost int64
}

// startReaders decouples filesystem latency from the connection count. The
// scheduler reserves bytes before submitting a read, including in-flight data.
func (s *session) startReaders() func() {
	q := make(chan job, readSlots)
	s.sched.mu.Lock()
	s.sched.readQ = q
	s.sched.mu.Unlock()
	var wg sync.WaitGroup
	for range readers {
		wg.Go(func() {
			for j := range q {
				if s.sched.failed() == nil {
					j.read.data, j.read.err = readSmall(s.localPath(j.f.e.Path), make([]byte, j.f.e.Size))
				}
				close(j.read.done)
			}
		})
	}
	return func() { close(q); wg.Wait() }
}

// prefetchLocked follows scheduler priority, and never waits for disk or space.
func (s *scheduler) prefetchLocked() {
	if s.readQ == nil || s.closed || s.err != nil {
		return
	}
	for _, q := range [][]job{s.urgent, s.big, s.small} {
		for i := range q {
			j := &q[i]
			if j.kind != jobPut || j.read != nil {
				continue
			}
			cost := max(j.f.e.Size, 4096)
			if s.readBytes+cost > readBudget || s.readCount == readSlots {
				return
			}
			j.read = &smallRead{done: make(chan struct{}), cost: cost}
			s.readBytes += cost
			s.readCount++
			s.readQ <- *j // outstanding jobs, including readers, are capped at readSlots
		}
	}
}

func (s *scheduler) releaseRead(r *smallRead) {
	r.data = nil
	s.mu.Lock()
	s.readBytes -= r.cost
	s.readCount--
	s.prefetchLocked()
	s.cond.Broadcast()
	s.mu.Unlock()
}
