package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/shxntanu/rift/internal/transport"
)

func TestRunnerReusesSessions(t *testing.T) {
	for _, compression := range []int{0, 1} {
		t.Run(fmt.Sprint(compression), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			r := NewRunner(ctx)
			defer r.Close()
			src, dst := t.TempDir(), t.TempDir()
			opt := Options{Src: src, Dst: transport.Endpoint{Path: dst}, Compress: compression, Conns: 4, Delete: true, DeltaMin: 128 << 10, Log: io.Discard}
			write := func(rel, data string) {
				t.Helper()
				p := filepath.Join(src, rel)
				if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte(data), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			run := func() *Summary {
				t.Helper()
				sum, err := r.Run(ctx, opt)
				if err != nil {
					t.Fatal(err)
				}
				if sum.ErrorCount != 0 {
					t.Fatal(sum.Errors)
				}
				assertSameTree(t, opt.Src, opt.Dst.Path)
				return sum
			}
			write("nested/file", "one")
			run()
			first := r.idle[0]
			if first == nil {
				t.Fatal("primary was not retained")
			}
			if sum := run(); sum.Transferred != 0 || sum.Deleted != 0 || sum.MetaOnly != 0 {
				t.Fatalf("not a no-op: %+v", sum)
			}
			if r.idle[0] != first {
				t.Fatal("no-op opened a new server")
			}
			write("nested/file", "two")
			run()
			// External receiver edits must invalidate its cached scan immediately.
			if err := os.WriteFile(filepath.Join(dst, "nested/file"), []byte("external edit"), 0o600); err != nil {
				t.Fatal(err)
			}
			run()
			if err := os.RemoveAll(filepath.Join(src, "nested")); err != nil {
				t.Fatal(err)
			}
			run()
			for i := range 300 {
				write(fmt.Sprintf("many/%03d", i), "worker")
			}
			run()
			sent := map[int]uint64{}
			retained := map[int]*rconn{}
			for id, rc := range r.idle {
				sent[id] = rc.sent
				retained[id] = rc
			}
			if sum := run(); sum.Transferred != 0 || sum.MetaOnly != 0 || sum.Deleted != 0 {
				t.Fatalf("warm worker no-op: %+v", sum)
			}
			for id, n := range sent {
				want := n
				if id == 0 {
					want += 2
				}
				if rc := r.idle[id]; rc == retained[id] && rc.sent != want {
					t.Fatalf("no-op sent unnecessary frames on connection %d", id)
				}
			}
			write("many/000", "changed again")
			run()
			// A dead idle server is discarded before reuse.
			if err := first.tc.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-first.dead:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			run()
			// Two simultaneous invocations queue safely.
			var wg sync.WaitGroup
			for range 2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					sum, err := r.Run(ctx, opt)
					if err != nil {
						t.Error(err)
					} else if sum.ErrorCount != 0 {
						t.Error(sum.Errors)
					}
				}()
			}
			wg.Wait()
			// Different roots never share cached state.
			opt.Src = t.TempDir()
			opt.Dst.Path = t.TempDir()
			run()
		})
	}
}
