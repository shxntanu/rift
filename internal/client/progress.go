package client

import (
	"fmt"
	"sync"
	"time"
)

// HumanBytes formats a byte count with binary units.
func HumanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func (s *session) startProgress() func() {
	done := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		start := time.Now()
		var lastBytes int64
		lastT := start
		var rate float64
		for {
			select {
			case <-done:
				fmt.Fprint(s.opt.Log, "\r\033[K")
				return
			case now := <-t.C:
				b := s.st.bytesDone.Load()
				dt := now.Sub(lastT).Seconds()
				inst := float64(b-lastBytes) / dt
				rate = 0.7*rate + 0.3*inst
				lastBytes, lastT = b, now
				total := s.st.bytesTotal.Load()
				pct := 100.0
				if total > 0 {
					pct = 100 * float64(b) / float64(total)
				}
				fmt.Fprintf(s.opt.Log, "\r\033[K%5.1f%%  %s / %s  %s/s  files %d/%d  wire %s  conns %d",
					pct, HumanBytes(b), HumanBytes(total), HumanBytes(int64(rate)),
					s.st.filesDone.Load(), s.st.filesTotal.Load(),
					HumanBytes(s.dialer.WireOut.Load()), len(s.liveConns()))
			}
		}
	}()
	return func() {
		once.Do(func() {
			close(done)
			wg.Wait()
		})
	}
}
