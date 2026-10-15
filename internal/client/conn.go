package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shxntanu/rift/internal/cdc"
	"github.com/shxntanu/rift/internal/proto"
	"github.com/shxntanu/rift/internal/scan"
	"github.com/shxntanu/rift/internal/transport"

	"github.com/zeebo/xxh3"
)

// rconn is one connection to a server process. Exactly one goroutine (the
// planner during setup, then the worker loop) writes to it; a dedicated
// reader goroutine dispatches everything the server sends back.
type rconn struct {
	id  int
	s   *session
	pc  *proto.Conn
	tc  *transport.Conn
	enc proto.Enc
	buf []byte

	scanCh chan scanMsg
	hashCh chan hashResp
	syncCh chan uint64
	dead   chan struct{}
	rerr   error

	closing   atomic.Bool
	stopRead  atomic.Bool
	stop      chan struct{}
	readOnce  sync.Once
	closeOnce sync.Once
	closeErr  error

	// an ack confirms its original send watermark, so an
	// older barrier cannot hide frames sent while its reply was in flight.
	ackMu sync.Mutex
	sent  uint64
	acked uint64
	acks  map[frameAck]uint64

	curFile *fileTask
	curFH   *os.File
}

type frameAck struct {
	t   proto.Type
	seq uint64
}

// recordFrame runs before Send, since sending can flush buffered frames and
// let the reader receive an acknowledgement before Send returns.
func (rc *rconn) recordFrame(t proto.Type, p []byte) {
	rc.ackMu.Lock()
	defer rc.ackMu.Unlock()
	rc.sent++
	var ack frameAck
	switch t {
	case proto.TScan:
		ack.t = proto.TScanDone
	case proto.THashReq, proto.TSync:
		d := proto.Dec{B: p}
		ack.seq = d.Uvarint()
		if t == proto.THashReq {
			ack.t = proto.THashResp
		} else {
			ack.t = proto.TSyncAck
		}
	default:
		return
	}
	if rc.acks == nil {
		rc.acks = make(map[frameAck]uint64)
	}
	rc.acks[ack] = rc.sent
}

// recordScan accounts for the scan sent together with the hello, before the
// rconn and its reader are created.
func (rc *rconn) recordScan() { rc.recordFrame(proto.TScan, nil) }

func (rc *rconn) send(t proto.Type, parts ...[]byte) error {
	var p []byte
	if len(parts) > 0 {
		p = parts[0]
	}
	rc.recordFrame(t, p)
	return rc.pc.Send(t, parts...)
}

func (rc *rconn) confirm(t proto.Type, seq uint64) {
	rc.ackMu.Lock()
	defer rc.ackMu.Unlock()
	ack := frameAck{t, seq}
	if n, ok := rc.acks[ack]; ok {
		rc.acked = max(rc.acked, n)
		delete(rc.acks, ack)
	}
}

func (rc *rconn) unconfirmed() bool {
	rc.ackMu.Lock()
	defer rc.ackMu.Unlock()
	return rc.sent != rc.acked
}

func (rc *rconn) startRead() {
	rc.readOnce.Do(func() {
		if rc.stopRead.Load() {
			close(rc.dead)
			return
		}
		go rc.readLoop()
	})
}

type scanMsg struct {
	entries  []scan.Entry
	done     bool
	warnings []string
}

type hashResp struct {
	req    uint64
	hashes []proto.Hash
	ok     []bool
}

type signature struct {
	size   int64
	params cdc.Params
	lens   []uint32
	hashes []proto.Hash
}

// connect pipelines the primary's scan behind its hello. scanSpecs waits for
// the local scan after dialing, while worker connections pass nil.
func (s *session) connect(ctx context.Context, worker bool, id int, scanSpecs func() ([]string, error)) (*rconn, error) {
	tc, err := s.dialer.Dial(ctx, worker, id)
	if err != nil {
		return nil, err
	}
	var specs []string
	if scanSpecs != nil {
		specs, err = scanSpecs()
		if err != nil {
			_ = tc.Close()
			return nil, err
		}
	}
	pc := proto.NewConn(tc)
	var enc proto.Enc
	enc.Uvarint(proto.Version)
	enc.Str(s.id)
	enc.Str(s.opt.Dst.Path)
	enc.Varint(int64(s.opt.Compress))
	// keep the raw hello buffered while switching the write
	// half, then send the scan in the same flush without waiting for hello OK.
	if err := pc.Send(proto.THello, enc.B); err != nil {
		_ = tc.Close()
		return nil, fmt.Errorf("handshake: %w", err)
	}
	if s.opt.Compress > 0 {
		if err := pc.EnableWriteCompression(s.opt.Compress); err != nil {
			_ = pc.Close()
			return nil, err
		}
	}
	if scanSpecs != nil {
		enc.Reset()
		enc.Uvarint(uint64(len(specs)))
		for _, spec := range specs {
			enc.Str(spec)
		}
		if len(enc.B) > proto.MaxFrame {
			_ = pc.Close()
			return nil, fmt.Errorf("scan frame too large: %d", len(enc.B))
		}
		err = pc.Send(proto.TScan, enc.B)
	}
	if err == nil {
		err = pc.Flush()
	}
	writeErr := err
	// A rejecting server can close its input before the pipelined scan has
	// finished writing. Read its uncompressed TError even after a write fails.
	t, p, err := pc.Recv()
	if err != nil {
		if writeErr != nil {
			err = writeErr
		}
		_ = pc.Close()
		return nil, fmt.Errorf("handshake: %w", err)
	}
	d := proto.Dec{B: p}
	switch t {
	case proto.THelloOK:
	case proto.TError:
		d.Uvarint()
		d.U8()
		msg := d.Str()
		_ = pc.Close()
		if d.Err != nil {
			return nil, fmt.Errorf("handshake: %w", d.Err)
		}
		return nil, fmt.Errorf("server: %s", msg)
	default:
		_ = pc.Close()
		return nil, fmt.Errorf("handshake: unexpected message %d", t)
	}
	if writeErr != nil {
		_ = tc.Close()
		return nil, fmt.Errorf("handshake: %w", writeErr)
	}
	d.Uvarint()
	goos, goarch, root := d.Str(), d.Str(), d.Str()
	if d.Err != nil {
		_ = tc.Close()
		return nil, d.Err
	}
	if s.opt.Compress > 0 {
		// the read half switches only after a raw hello OK;
		// a rejected hello is always decoded as an uncompressed error above.
		if err := pc.EnableReadCompression(); err != nil {
			_ = pc.Close()
			return nil, err
		}
	}
	if id == 0 {
		s.remoteRoot = root
		s.remotePlatform = goos + "/" + goarch
	}
	rc := &rconn{
		id:     id,
		s:      s,
		pc:     pc,
		tc:     tc,
		buf:    make([]byte, proto.ChunkSize),
		scanCh: make(chan scanMsg, 16),
		hashCh: make(chan hashResp, 4),
		syncCh: make(chan uint64, 4),
		dead:   make(chan struct{}),
		stop:   make(chan struct{}),
	}
	if scanSpecs != nil {
		rc.recordScan()
	}
	rc.startRead()
	return rc, nil
}

// close ends our stream and waits for the server only when frames remain
// unconfirmed. Workers must have stopped before it is called.
func (rc *rconn) close() error {
	rc.closeOnce.Do(func() {
		pending := rc.unconfirmed()
		rc.closing.Store(true)
		if !pending {
			rc.stopRead.Store(true)
		}
		close(rc.stop)
		rc.startRead()
		if rc.curFH != nil {
			_ = rc.curFH.Close()
		}
		werr := rc.pc.CloseWrite()
		if !pending {
			// all server errors precede the last ack. Stop
			// reading and reap in the background to skip the server-exit RTT.
			_ = rc.tc.CloseRead()
			<-rc.dead
			rc.closeErr = rc.rerr
			go func() { _ = rc.pc.Close() }()
			return
		}
		// EOF replaces the final barrier for pending deletes
		// and directory metadata. Drain errors before accepting server exit.
		if werr != nil {
			rc.closeErr = fmt.Errorf("connection %d: close write: %w", rc.id, werr)
		}
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-rc.dead:
		case <-timer.C:
			rc.closeErr = errors.Join(rc.closeErr, fmt.Errorf("connection %d: timed out waiting for server exit", rc.id))
			rc.stopRead.Store(true)
			_ = rc.tc.CloseRead()
			<-rc.dead
		}
		rc.closeErr = errors.Join(rc.closeErr, rc.rerr)
		if err := rc.pc.Close(); err != nil {
			rc.closeErr = errors.Join(rc.closeErr, fmt.Errorf("connection %d: server exit: %w", rc.id, err))
		}
	})
	return rc.closeErr
}

func (rc *rconn) readLoop() {
	defer close(rc.dead)
	for {
		t, p, err := rc.pc.Recv()
		if err != nil {
			// Final-batch teardown expects clean EOF, but must retain a
			// truncated frame or any other receive error.
			cleanEOF := rc.closing.Load() && errors.Is(err, io.EOF)
			if !rc.stopRead.Load() && !cleanEOF {
				if errors.Is(err, io.EOF) {
					err = errors.New("server closed the connection")
				}
				rc.rerr = fmt.Errorf("connection %d: %w", rc.id, err)
				rc.s.sched.abort(rc.rerr)
			}
			return
		}
		if err := rc.dispatch(t, p); err != nil {
			rc.rerr = fmt.Errorf("connection %d: %w", rc.id, err)
			rc.s.sched.abort(rc.rerr)
			return
		}
	}
}

func (rc *rconn) dispatch(t proto.Type, p []byte) error {
	d := proto.Dec{B: p}
	s := rc.s
	switch t {
	case proto.TScanEntries:
		n := int(d.Uvarint())
		entries := make([]scan.Entry, 0, n)
		for range n {
			entries = append(entries, scan.DecodeEntry(&d))
		}
		if d.Err != nil {
			return d.Err
		}
		select {
		case rc.scanCh <- scanMsg{entries: entries}:
		case <-rc.stop:
		}
	case proto.TScanDone:
		d.Uvarint()
		n := int(d.Uvarint())
		var warns []string
		for range n {
			warns = append(warns, d.Str())
		}
		if d.Err != nil {
			return d.Err
		}
		rc.confirm(proto.TScanDone, 0)
		select {
		case rc.scanCh <- scanMsg{done: true, warnings: warns}:
		case <-rc.stop:
		}
	case proto.THashResp:
		r := hashResp{req: d.Uvarint()}
		n := int(d.Uvarint())
		r.hashes = make([]proto.Hash, n)
		r.ok = make([]bool, n)
		for i := range n {
			if d.U8() == 1 {
				r.ok[i] = true
				r.hashes[i] = d.Hash()
			}
		}
		if d.Err != nil {
			return d.Err
		}
		rc.confirm(proto.THashResp, r.req)
		select {
		case rc.hashCh <- r:
		case <-rc.stop:
		}
	case proto.TSig:
		id := d.Uvarint()
		f := s.file(id)
		if f == nil {
			return fmt.Errorf("signature for unknown file %d", id)
		}
		var sig *signature
		if d.U8() == 1 {
			sg := &signature{size: int64(d.Uvarint())}
			sg.params = cdc.Params{Min: int(d.Uvarint()), Avg: int(d.Uvarint()),
				Max: int(d.Uvarint())}
			n := int(d.Uvarint())
			sg.lens = make([]uint32, 0, n)
			sg.hashes = make([]proto.Hash, 0, n)
			for range n {
				sg.lens = append(sg.lens, uint32(d.Uvarint()))
				sg.hashes = append(sg.hashes, d.Hash())
			}
			if d.Err != nil {
				return d.Err
			}
			if sg.params.Valid() {
				sig = sg
			}
		}
		if sig == nil {
			s.sched.push(nil, s.fullJobs(f), nil, -1+s.fullPending(f))
		} else {
			s.sched.push([]job{{kind: jobDelta, f: f, sig: sig}}, nil, nil, -1)
		}
	case proto.TRangeAck:
		id := d.Uvarint()
		n := int64(d.Uvarint())
		f := s.file(id)
		if f == nil {
			return fmt.Errorf("range ack for unknown file %d", id)
		}
		s.rangeProgress(f, n)
	case proto.TSyncAck:
		seq := d.Uvarint()
		if d.Err != nil {
			return d.Err
		}
		rc.confirm(proto.TSyncAck, seq)
		select {
		case rc.syncCh <- seq:
		case <-rc.stop:
		}
	case proto.TError:
		id := d.Uvarint()
		code := d.U8()
		msg := d.Str()
		if d.Err != nil {
			return d.Err
		}
		if code == proto.CodeFatal {
			return fmt.Errorf("server: %s", msg)
		}
		s.onServerError(id, code, msg)
	default:
		return fmt.Errorf("unexpected message %d", t)
	}
	return nil
}

// work is the worker loop: pull jobs, execute them, repeat.
func (rc *rconn) work() {
	s := rc.s
	if !s.sched.join() {
		return
	}
	defer s.sched.leave()
	var batch []job
	for {
		jobs, err := s.sched.next(rc.id, batch[:0], rc.pc.Flush)
		if err != nil {
			if !errors.Is(err, errClosed) {
				s.sched.abort(err)
			}
			return
		}
		for i := range jobs {
			if err := rc.do(&jobs[i]); err != nil {
				s.sched.abort(fmt.Errorf("connection %d: %w", rc.id, err))
				return
			}
			jobs[i] = job{}
		}
		batch = jobs
	}
}

func (rc *rconn) do(j *job) error {
	switch j.kind {
	case jobPut:
		return rc.put(j.f)
	case jobSymlink:
		f := j.f
		rc.enc.Reset()
		rc.enc.Uvarint(f.id)
		rc.enc.Str(f.e.Path)
		rc.enc.Str(f.e.Link)
		rc.enc.Varint(f.e.Mtime)
		rc.s.logf(">l %s -> %s", f.e.Path, f.e.Link)
		rc.s.st.filesDone.Add(1)
		return rc.send(proto.TSymlink, rc.enc.B)
	case jobRange:
		return rc.sendRange(j.rt)
	case jobFinalize:
		f := j.f
		rc.enc.Reset()
		rc.enc.Uvarint(f.id)
		rc.enc.Str(f.e.Path)
		rc.enc.Uvarint(uint64(f.e.Mode))
		rc.enc.Varint(f.e.Mtime)
		rc.s.st.filesDone.Add(1)
		return rc.send(proto.TFinalize, rc.enc.B)
	case jobSigReq:
		rc.enc.Reset()
		rc.enc.Uvarint(j.f.id)
		rc.enc.Str(j.f.e.Path)
		if err := rc.send(proto.TSigReq, rc.enc.B); err != nil {
			return err
		}
		return rc.pc.Flush()
	case jobDelta:
		return rc.sendDelta(j.f, j.sig)
	case jobMeta:
		return rc.sendMeta(j.metas)
	case jobFunc:
		return j.fn(rc)
	}
	return fmt.Errorf("unknown job kind %d", j.kind)
}

func (rc *rconn) put(f *fileTask) error {
	s := rc.s
	data, err := readSmall(s.localPath(f.e.Path), rc.buf[:f.e.Size])
	if err != nil {
		s.recordErr(fmt.Sprintf("%s: %v", f.e.Path, err))
		s.st.bytesDone.Add(f.e.Size)
		return nil
	}
	var flags uint8
	if f.isNew {
		flags |= proto.PutNew
	}
	rc.enc.Reset()
	rc.enc.Uvarint(f.id)
	rc.enc.U8(flags)
	rc.enc.Str(f.e.Path)
	rc.enc.Uvarint(uint64(f.e.Mode))
	rc.enc.Varint(f.e.Mtime)
	s.logf(">f %s", f.e.Path)
	s.st.bytesDone.Add(f.e.Size)
	s.st.literal.Add(int64(len(data)))
	s.st.filesDone.Add(1)
	return rc.send(proto.TPut, rc.enc.B, data)
}

func readSmall(path string, buf []byte) ([]byte, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	n, err := io.ReadFull(fh, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:n], nil
}

func (rc *rconn) open(f *fileTask) (*os.File, error) {
	if rc.curFile == f {
		return rc.curFH, nil
	}
	if rc.curFH != nil {
		rc.curFH.Close()
		rc.curFH, rc.curFile = nil, nil
	}
	fh, err := os.Open(rc.s.localPath(f.e.Path))
	if err != nil {
		return nil, err
	}
	rc.curFH, rc.curFile = fh, f
	return fh, nil
}

func (rc *rconn) sendRange(rt *rangeTask) error {
	s := rc.s
	f := rt.f
	defer s.sched.rangeDone(rt)
	fh, err := rc.open(f)
	if err != nil {
		f.failed.Store(true)
		s.recordErr(fmt.Sprintf("%s: %v", f.e.Path, err))
		var drained int64
		for {
			_, n := rt.claim(1 << 62)
			if n == 0 {
				break
			}
			drained += n
		}
		s.st.bytesDone.Add(drained)
		s.rangeProgress(f, drained)
		return nil
	}
	if rt.pos == 0 {
		s.logf(">f %s", f.e.Path)
	}
	for {
		off, n := rt.claim(proto.ChunkSize)
		if n == 0 {
			break
		}
		buf := rc.buf[:n]
		k, err := fh.ReadAt(buf, off)
		if int64(k) < n {
			// The file shrank or failed mid-transfer: still send the claimed
			// length so byte accounting completes, but never finalize it.
			clear(buf[k:])
			if !f.failed.Swap(true) {
				if err == nil || errors.Is(err, io.EOF) {
					err = errors.New("file changed during transfer")
				}
				s.recordErr(fmt.Sprintf("%s: %v", f.e.Path, err))
			}
		}
		rc.enc.Reset()
		rc.enc.Uvarint(f.id)
		rc.enc.Str(f.e.Path)
		rc.enc.Uvarint(uint64(f.e.Size))
		rc.enc.Uvarint(uint64(off))
		if err := rc.send(proto.TRangeWrite, rc.enc.B, buf); err != nil {
			return err
		}
		s.st.bytesDone.Add(n)
		s.st.literal.Add(n)
	}
	rc.enc.Reset()
	rc.enc.Uvarint(f.id)
	if err := rc.send(proto.TRangeClose, rc.enc.B); err != nil {
		return err
	}
	return rc.pc.Flush()
}

type deltaOp struct {
	copy bool  // copy from basis at off, else literal from local file at off
	off  int64 // basis offset for copies, local offset for literals
	n    int64
}

func (rc *rconn) sendDelta(f *fileTask, sig *signature) error {
	s := rc.s
	index := make(map[proto.Hash]int64, len(sig.hashes))
	var boff int64
	for i, h := range sig.hashes {
		if _, ok := index[h]; !ok {
			index[h] = boff
		}
		boff += int64(sig.lens[i])
	}

	fh, err := rc.open(f)
	if err != nil {
		s.recordErr(fmt.Sprintf("%s: %v", f.e.Path, err))
		s.st.bytesDone.Add(f.e.Size)
		return nil
	}
	if _, err := fh.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var ops []deltaOp
	whole := xxh3.New128()
	var size int64
	err = cdc.Split(bufio.NewReaderSize(fh, 1<<20), sig.params, func(off int64, data []byte) error {
		_, _ = whole.Write(data)
		size = off + int64(len(data))
		n := int64(len(data))
		if bo, ok := index[scan.HashBytes(data)]; ok {
			if k := len(ops) - 1; k >= 0 && ops[k].copy && ops[k].off+ops[k].n == bo {
				ops[k].n += n
			} else {
				ops = append(ops, deltaOp{copy: true, off: bo, n: n})
			}
			return nil
		}
		if k := len(ops) - 1; k >= 0 && !ops[k].copy && ops[k].off+ops[k].n == off {
			ops[k].n += n
		} else {
			ops = append(ops, deltaOp{off: off, n: n})
		}
		return nil
	})
	if err != nil {
		s.recordErr(fmt.Sprintf("%s: %v", f.e.Path, err))
		s.st.bytesDone.Add(f.e.Size)
		return nil
	}

	// Identical content (typically a touched file): only metadata changes.
	if size == sig.size && (size == 0 || len(ops) == 1 && ops[0].copy && ops[0].off == 0 && ops[0].n == size) {
		s.st.bytesDone.Add(f.e.Size)
		s.st.filesDone.Add(1)
		s.st.metaOnly.Add(1)
		return rc.sendMeta([]metaUpdate{{path: f.e.Path, kind: scan.KindFile, mode: f.e.Mode, mtime: f.e.Mtime}})
	}

	rc.enc.Reset()
	rc.enc.Uvarint(f.id)
	rc.enc.Str(f.e.Path)
	rc.enc.Uvarint(uint64(size))
	if err := rc.send(proto.TDeltaBegin, rc.enc.B); err != nil {
		return err
	}
	var literal, matched int64
	for _, op := range ops {
		if op.copy {
			rc.enc.Reset()
			rc.enc.Uvarint(uint64(op.off))
			rc.enc.Uvarint(uint64(op.n))
			if err := rc.send(proto.TDeltaCopy, rc.enc.B); err != nil {
				return err
			}
			matched += op.n
			continue
		}
		for done := int64(0); done < op.n; {
			k := min(op.n-done, proto.ChunkSize)
			buf := rc.buf[:k]
			r, _ := fh.ReadAt(buf, op.off+done)
			clear(buf[r:]) // a changed file fails the server's hash check and is resent in full
			if err := rc.send(proto.TDeltaData, buf); err != nil {
				return err
			}
			done += k
		}
		literal += op.n
	}
	sum := whole.Sum128()
	rc.enc.Reset()
	rc.enc.Uvarint(uint64(f.e.Mode))
	rc.enc.Varint(f.e.Mtime)
	rc.enc.Hash(proto.Hash{Hi: sum.Hi, Lo: sum.Lo})
	if err := rc.send(proto.TDeltaEnd, rc.enc.B); err != nil {
		return err
	}
	s.logf(">d %s (%.0f%% reused)", f.e.Path, 100*float64(matched)/float64(max(size, 1)))
	s.st.literal.Add(literal)
	s.st.matched.Add(matched)
	s.st.bytesDone.Add(f.e.Size)
	s.st.filesDone.Add(1)
	return nil
}

type metaUpdate struct {
	path  string
	kind  scan.Kind
	mode  uint32
	mtime int64
}

func (rc *rconn) sendMeta(ms []metaUpdate) error {
	for len(ms) > 0 {
		n := min(len(ms), 4096)
		rc.enc.Reset()
		rc.enc.Uvarint(uint64(n))
		for _, m := range ms[:n] {
			rc.enc.Str(m.path)
			rc.enc.U8(uint8(m.kind))
			rc.enc.Uvarint(uint64(m.mode))
			rc.enc.Varint(m.mtime)
		}
		if err := rc.send(proto.TMeta, rc.enc.B); err != nil {
			return err
		}
		ms = ms[n:]
	}
	return nil
}

func (rc *rconn) sendDeletes(paths []string, flags uint8) error {
	for len(paths) > 0 {
		n := min(len(paths), 4096)
		rc.enc.Reset()
		rc.enc.U8(flags)
		rc.enc.Uvarint(uint64(n))
		for _, p := range paths[:n] {
			rc.enc.Str(p)
		}
		if err := rc.send(proto.TDelete, rc.enc.B); err != nil {
			return err
		}
		paths = paths[n:]
	}
	return nil
}

func (rc *rconn) sendMkdirs(dirs []scan.Entry) error {
	for len(dirs) > 0 {
		n := min(len(dirs), 4096)
		rc.enc.Reset()
		rc.enc.Uvarint(uint64(n))
		for _, e := range dirs[:n] {
			rc.enc.Str(e.Path)
			rc.enc.Uvarint(uint64(e.Mode))
		}
		if err := rc.send(proto.TMkdirs, rc.enc.B); err != nil {
			return err
		}
		dirs = dirs[n:]
	}
	return nil
}

// sync sends a barrier; the caller waits for the ack on syncCh.
func (rc *rconn) sync(seq uint64) error {
	rc.enc.Reset()
	rc.enc.Uvarint(seq)
	if err := rc.send(proto.TSync, rc.enc.B); err != nil {
		return err
	}
	return rc.pc.Flush()
}

func (rc *rconn) waitSync(seq uint64, abort <-chan struct{}) error {
	for {
		select {
		case got := <-rc.syncCh:
			if got == seq {
				return nil
			}
		case <-rc.dead:
			if rc.rerr != nil {
				return rc.rerr
			}
			return fmt.Errorf("connection %d closed", rc.id)
		case <-abort:
			return rc.s.sched.failed()
		}
	}
}
