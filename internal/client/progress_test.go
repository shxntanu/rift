package client

import (
	"testing"
	"time"

	"github.com/shxntanu/rift/internal/proto"
	"github.com/shxntanu/rift/internal/scan"
	"github.com/shxntanu/rift/internal/transport"
)

func TestProgressTracksAttempts(t *testing.T) {
	s := &session{start: time.Now(), dialer: transport.NewDialer(transport.Endpoint{}, transport.Options{}),
		sched: newScheduler(), files: []*fileTask{nil}}
	queued := s.newTask(scan.Entry{Path: "queued", Kind: scan.KindFile, Size: 10}, true)
	active := s.newTask(scan.Entry{Path: "active", Kind: scan.KindFile, Size: 8 << 20}, false)
	active.state.Store(fileSending)
	active.progress.Store(2 << 20)
	empty := s.newTask(scan.Entry{Path: "empty", Kind: scan.KindFile}, true)
	if err := empty.finishProgress(nil); err != nil {
		t.Fatal(err)
	}
	s.progressPlan.Store(&progressPlan{phase: "Transferring", files: s.files[1:]})
	p := s.progressSnapshot()
	if p.BytesDone != 2<<20 || p.FilesDone != 1 || p.FilesTotal != 3 || p.Files[0].Path != "active" {
		t.Fatalf("missing live chunk progress or active-file priority: %+v", p)
	}
	if err := active.finishProgress(nil); err != nil {
		t.Fatal(err)
	}
	s.onServerError(active.id, proto.CodeDeltaFailed, "retry")
	p = s.progressSnapshot()
	if p.BytesDone != 0 || p.FilesDone != 1 || p.Files[0].State != "sending" {
		t.Fatalf("retry counted the previous attempt: %+v", p)
	}
	queued.failed.Store(true)
	p = s.progressSnapshot()
	if p.Failed != 1 || p.FilesDone != 1 || p.BytesTotal != (8<<20)+10 {
		t.Fatalf("failed file counted as completed: %+v", p)
	}
}
