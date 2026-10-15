package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shxntanu/rift/internal/scan"
	"github.com/shxntanu/rift/internal/server"
	"github.com/shxntanu/rift/internal/transport"
)

type testStdio struct{}

func (testStdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (testStdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (testStdio) Close() error                { return os.Stdout.Close() }

// Local transports execute the test binary with "serve", just as they do
// the rift binary. Keep the real server on the other side of the pipes.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		if err := server.Serve(testStdio{}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestWorkerCount(t *testing.T) {
	for _, tt := range []struct {
		name          string
		bytes         int64
		files, deltas int
		limit, want   int
	}{
		{name: "small", bytes: 4<<20 - 1, files: 255, limit: 8, want: 1},
		{name: "bytes", bytes: 4 << 20, limit: 8, want: 2},
		{name: "files", files: 256, limit: 8, want: 2},
		{name: "deltas", files: 4, deltas: 4, limit: 8, want: 2},
		{name: "limit", bytes: 1 << 30, files: 1024, deltas: 32, limit: 3, want: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &session{opt: Options{Conns: tt.limit}}
			if got := s.workerCount(tt.bytes, tt.files, tt.deltas); got != tt.want {
				t.Fatalf("workerCount = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestWarmWorkers(t *testing.T) {
	for _, compress := range []int{0, 1} {
		t.Run(fmt.Sprintf("compression%d", compress), func(t *testing.T) {
			for _, adopt := range []bool{false, true} {
				t.Run(fmt.Sprintf("adopt%v", adopt), func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					opt := Options{Conns: 4, Compress: compress, Log: io.Discard,
						Dst: transport.Endpoint{Path: t.TempDir()}}
					s := &session{opt: opt, id: newSessionID(), sched: newScheduler(),
						dialer: transport.NewDialer(opt.Dst, opt.Transport)}
					var wg sync.WaitGroup
					t.Cleanup(func() {
						s.stopWarmWorkers()
						s.sched.close()
						cancel()
						wg.Wait()
					})
					s.warmWorkers(ctx, &wg, []scan.Entry{{Kind: scan.KindFile, Size: 12 << 20}})
					if len(s.workers) != 3 || len(s.liveConns()) != 0 {
						t.Fatalf("warmup: %d attempts, %d active connections", len(s.workers), len(s.liveConns()))
					}
					first := s.workers[0]
					p := &plan{}
					if adopt {
						p.transfers = []*fileTask{{e: scan.Entry{Kind: scan.KindFile, Size: 4 << 20}}}
						joined := make(chan struct{})
						s.sched.pushOwn(1, job{kind: jobFunc, fn: func(*rconn) error {
							close(joined)
							return nil
						}})
						s.spawnWorkers(ctx, &wg, p)
						select {
						case <-joined:
						case <-time.After(5 * time.Second):
							t.Fatal("selected worker did not join")
						}
						if s.workers[0] != first || !first.adopted || len(s.liveConns()) != 1 {
							t.Fatal("planned worker did not reuse the speculative attempt")
						}
					} else {
						s.spawnWorkers(ctx, &wg, p)
					}
					s.sched.close()
					finished := make(chan struct{})
					go func() { wg.Wait(); close(finished) }()
					select {
					case <-finished:
					case <-time.After(5 * time.Second):
						t.Fatal("unused worker setup did not stop")
					}
					if err := s.sched.failed(); err != nil {
						t.Fatalf("discarded worker aborted the session: %v", err)
					}
				})
			}
		})
	}
}

func TestWarmWorkersSkipsUnneededDials(t *testing.T) {
	for _, tt := range []struct {
		name   string
		dryRun bool
		conns  int
		kind   scan.Kind
	}{
		{name: "dry run", dryRun: true, conns: 8, kind: scan.KindFile},
		{name: "one connection", conns: 1, kind: scan.KindFile},
		{name: "directories", conns: 8, kind: scan.KindDir},
		{name: "symlinks", conns: 8, kind: scan.KindSymlink},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &session{opt: Options{Conns: tt.conns, DryRun: tt.dryRun}}
			var wg sync.WaitGroup
			s.warmWorkers(context.Background(), &wg, []scan.Entry{{Kind: tt.kind, Size: 1 << 30}})
			if len(s.workers) != 0 {
				t.Fatal("unneeded speculative connections opened")
			}
		})
	}
}

func TestWarmWorkerFinishingAfterShutdown(t *testing.T) {
	for _, compress := range []int{0, 1} {
		t.Run(fmt.Sprintf("compression%d", compress), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			opt := Options{Conns: 2, Compress: compress, Log: io.Discard,
				Dst: transport.Endpoint{Path: t.TempDir()}}
			s := &session{opt: opt, id: newSessionID(), sched: newScheduler(),
				dialer: transport.NewDialer(opt.Dst, opt.Transport)}
			// Reproduce a handshake that completes after shutdown has closed
			// admission and taken its stable snapshot of active connections.
			s.sched.close()
			var wg sync.WaitGroup
			s.spawnWorkers(ctx, &wg, &plan{transfers: []*fileTask{{e: scan.Entry{Size: 4 << 20}}}})
			wg.Wait()
			if got := len(s.liveConns()); got != 0 {
				t.Fatalf("late handshake registered %d active connections", got)
			}
			if err := s.sched.failed(); err != nil {
				t.Fatalf("late handshake aborted the session: %v", err)
			}
			if s.dialer.WireIn.Load() == 0 {
				t.Fatal("worker never completed its handshake")
			}
		})
	}
}
