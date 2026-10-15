package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shxntanu/rift/internal/proto"
	"github.com/shxntanu/rift/internal/scan"
	"github.com/shxntanu/rift/internal/transport"
)

func TestConnectionCount(t *testing.T) {
	for _, tc := range []struct {
		name        string
		bytes       int64
		rtt         time.Duration
		window      int64
		limit, want int
	}{
		{"small-slow", 1024, time.Second, 2 << 20, 32, 1},
		{"lan", 1 << 30, time.Millisecond, 2 << 20, 32, 8},
		{"wan", 1 << 30, 200 * time.Millisecond, 2 << 20, 32, 32},
		{"middle", 1 << 30, 30 * time.Millisecond, 2 << 20, 32, 16},
		{"explicit-cap", 1 << 30, time.Second, 2 << 20, 3, 3},
		{"larger-window", 1 << 30, 200 * time.Millisecond, 32 << 20, 32, 8},
		{"direct", 1 << 30, 200 * time.Millisecond, 0, 32, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := connectionCount(tc.bytes, 1, 0, tc.limit, tc.rtt, tc.window); got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestReadAheadBudgetAndOrder(t *testing.T) {
	s := &session{src: t.TempDir(), sched: newScheduler(), opt: Options{Log: io.Discard}}
	stop := s.startReaders()
	defer func() { s.sched.close(); stop() }()
	var jobs []job
	for i := range 80 {
		name := fmt.Sprintf("%d", i)
		data := make([]byte, proto.ChunkSize)
		data[0] = byte(i)
		if err := os.WriteFile(filepath.Join(s.src, name), data, 0600); err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job{kind: jobPut, f: &fileTask{e: scan.Entry{Path: name, Size: int64(len(data))}}})
	}
	s.sched.push(nil, nil, jobs, 0)
	if s.sched.readCount != 64 || s.sched.readBytes != readBudget {
		t.Fatalf("unexpected admission %d/%d", s.sched.readCount, s.sched.readBytes)
	}
	for i := 0; i < len(jobs); {
		batch, err := s.sched.next(0, nil, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range batch {
			select {
			case <-j.read.done:
			case <-time.After(5 * time.Second):
				t.Fatal("reader stalled")
			}
			if j.read.err != nil || len(j.read.data) != proto.ChunkSize || j.read.data[0] != byte(i) {
				t.Fatalf("read %d: %v", i, j.read.err)
			}
			s.sched.releaseRead(j.read)
			if s.sched.readBytes > readBudget {
				t.Fatal("budget exceeded")
			}
			i++
		}
	}
	if s.sched.readBytes != 0 {
		t.Fatal("budget not released")
	}
}

func TestReadAheadCancellation(t *testing.T) {
	s := &session{src: t.TempDir(), sched: newScheduler()}
	stop := s.startReaders()
	var jobs []job
	for range 2000 {
		jobs = append(jobs, job{kind: jobPut, f: &fileTask{e: scan.Entry{Path: "missing"}}})
	}
	s.sched.push(nil, nil, jobs, 0)
	if s.sched.readCount != readSlots {
		t.Fatal("unbounded admission of empty files")
	}
	s.sched.abort(context.Canceled)
	s.sched.close()
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("readers did not stop")
	}
}

func TestRunnerWithDirectAndParallelApply(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct=%v", direct), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			r := NewRunner(ctx)
			defer r.Close()
			src, dst := t.TempDir(), t.TempDir()
			opt := Options{Src: src, Dst: transport.Endpoint{Path: dst}, Conns: 4, Compress: 1, Delete: true, DeltaMin: 1 << 62, Transport: transport.Options{Direct: direct}, Log: io.Discard}
			for round := range 3 {
				// Reuse warmed connections while changing session IDs, including
				// range writes whose temp files are shared between connections.
				data := bytes.Repeat([]byte{byte(round + 1)}, 12<<20)
				if err := os.WriteFile(filepath.Join(src, "large"), data, 0600); err != nil {
					t.Fatal(err)
				}
				for i := range 300 {
					if err := os.WriteFile(filepath.Join(src, fmt.Sprintf("small%d", i)), data[:100+round], 0600); err != nil {
						t.Fatal(err)
					}
				}
				sum, err := r.Run(ctx, opt)
				if err != nil {
					t.Fatal(err)
				}
				if sum.ErrorCount != 0 {
					t.Fatal(sum.Errors)
				}
				assertSameTree(t, src, dst)
				if direct && len(r.idle) != 0 {
					t.Fatal("direct streams retained across bootstrap sessions")
				}
			}
			sum, err := r.Run(ctx, opt)
			if err != nil {
				t.Fatal(err)
			}
			if sum.Transferred != 0 || sum.ErrorCount != 0 {
				t.Fatalf("no-op: %+v", sum)
			}
		})
	}
}
