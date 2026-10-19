package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/shxntanu/rift/internal/scan"
)

const (
	fileQueued int32 = iota
	fileChecking
	fileSending
	fileDone
)

// FileProgress describes one file's current transfer attempt.
type FileProgress struct {
	Path             string
	BytesDone, Total int64
	State            string
}

// Progress is a live sync snapshot. Files contains up to 32 rows, prioritizing
// active and failed transfers, then queued and completed files.
type Progress struct {
	Phase                 string
	BytesDone, BytesTotal int64
	FilesDone, FilesTotal int
	Failed                int
	WireOut               int64
	Conns                 int
	Elapsed               time.Duration
	Rate                  float64
	Files                 []FileProgress
}

type progressPlan struct {
	phase string
	files []*fileTask // published after planning, then immutable
}

func (f *fileTask) finishProgress(err error) error {
	if err != nil {
		f.failed.Store(true)
	} else {
		f.progress.Store(f.e.Size)
	}
	f.state.Store(fileDone)
	return err
}

func (s *session) progressSnapshot() Progress {
	plan := s.progressPlan.Load()
	p := Progress{Phase: plan.phase, Elapsed: time.Since(s.start),
		WireOut: s.dialer.WireOut.Load() - s.wireStart, Conns: len(s.liveConns()), FilesTotal: len(plan.files)}
	var active, queued, completed []FileProgress
	for _, f := range plan.files {
		state := f.state.Load()
		row := FileProgress{Path: f.e.Path, BytesDone: min(f.progress.Load(), f.e.Size), Total: f.e.Size}
		if f.e.Kind == scan.KindFile {
			p.BytesDone += row.BytesDone
			p.BytesTotal += row.Total
		}
		switch {
		case f.failed.Load():
			row.State = "failed"
			p.Failed++
		case state == fileDone:
			row.State = "done"
			p.FilesDone++
		case state == fileChecking:
			row.State = "checking"
		case state == fileSending:
			row.State = "sending"
		default:
			row.State = "queued"
		}
		switch row.State {
		case "queued":
			if len(queued) < 32 {
				queued = append(queued, row)
			}
		case "done":
			// Keep only a bounded tail of completed files.
			if len(completed) < 32 {
				completed = append(completed, row)
			} else {
				completed[p.FilesDone%32] = row
			}
		default:
			if len(active) < 32 {
				active = append(active, row)
			}
		}
	}
	p.Files = append(active, queued[:min(len(queued), 32-len(active))]...)
	p.Files = append(p.Files, completed[:min(len(completed), 32-len(p.Files))]...)
	return p
}

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
	rich := s.opt.Rich && s.opt.OnProgress != nil
	if rich {
		s.opt.OnProgress(s.progressSnapshot())
	}
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
				if rich {
					p := s.progressSnapshot()
					p.Rate = rate
					s.opt.OnProgress(p)
				} else {
					fmt.Fprint(s.opt.Log, "\r\033[K")
				}
				return
			case now := <-t.C:
				b := s.st.bytesDone.Load()
				var p Progress
				if rich {
					p = s.progressSnapshot()
					b = p.BytesDone
				}
				dt := now.Sub(lastT).Seconds()
				inst := float64(max(0, b-lastBytes)) / dt
				rate = 0.7*rate + 0.3*inst
				lastBytes, lastT = b, now
				if rich {
					p.Rate = rate
					s.opt.OnProgress(p)
					continue
				}
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
