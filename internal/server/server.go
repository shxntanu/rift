// Package server implements the receiving side of a rift session.
//
// Each connection owns its apply and delta state; the client coordinates
// shared files across connections. SSH launches separate server processes,
// while the optional direct listener hosts multiple connections in one process.
package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/shxntanu/rift/internal/cdc"
	"github.com/shxntanu/rift/internal/direct"
	"github.com/shxntanu/rift/internal/proto"
	"github.com/shxntanu/rift/internal/scan"

	"github.com/zeebo/xxh3"
	"golang.org/x/sys/unix"
)

type server struct {
	ctx                     context.Context
	listener                *direct.Listener
	boundRoot, boundSession string
	raw                     io.ReadWriteCloser
	c                       *proto.Conn
	root                    string
	session                 string
	enc                     proto.Enc
	ranges                  map[uint64]*rangeFile
	delta                   *deltaState
	buf                     []byte
	cache                   *scan.Cache
}

type rangeFile struct {
	f        *os.File
	tmp      string
	received int64
	err      error
}

type deltaState struct {
	id       uint64
	abs, tmp string
	basis    *os.File
	out      *os.File
	w        *bufio.Writer
	h        *xxh3.Hasher128
	size     int64
	written  int64
	err      error
}

// Serve runs one server session over rw until the client disconnects.
func Serve(rw io.ReadWriteCloser) error { return ServeContext(context.Background(), rw) }

// ServeContext runs a server session and closes its stream on cancellation.
func ServeContext(ctx context.Context, rw io.ReadWriteCloser) error {
	return serve(ctx, rw, "", "")
}

func serve(ctx context.Context, rw io.ReadWriteCloser, root, session string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = rw.Close() })
	defer stop()
	if _, err := io.WriteString(rw, proto.Magic); err != nil {
		return err
	}
	s := &server{
		ctx:       ctx,
		raw:       rw,
		boundRoot: root, boundSession: session,
		c:      proto.NewConn(rw),
		ranges: map[uint64]*rangeFile{},
		buf:    make([]byte, proto.ChunkSize),
	}
	defer func() { _ = s.c.CloseWrite(); _ = s.c.Close() }()
	defer func() {
		if s.listener != nil {
			_ = s.listener.Close()
		}
	}()
	defer s.cleanup()
	defer func() {
		if s.cache != nil {
			s.cache.Close()
		}
	}()
	if err := s.handshake(); err != nil {
		return err
	}
	pool := newApplyPool(s)
	defer pool.close()
	for {
		t, p, err := s.c.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return pool.drain()
			}
			return err
		}
		if err := pool.handle(s, t, p); err != nil {
			_ = pool.drain()
			s.enc.Reset()
			s.enc.Uvarint(0)
			s.enc.U8(proto.CodeFatal)
			s.enc.Str(err.Error())
			_ = s.c.Send(proto.TError, s.enc.B)
			_ = s.c.Flush()
			return err
		}
	}
}

func (s *server) handshake() error {
	t, p, err := s.c.Recv()
	if err != nil {
		return err
	}
	if t != proto.THello {
		return fmt.Errorf("expected hello, got %d", t)
	}
	d := proto.Dec{B: p}
	ver := d.Uvarint()
	s.session = d.Str()
	root := d.Str()
	level := int(d.Varint())
	wantDirect := d.U8() == 1
	if d.Err != nil {
		return d.Err
	}
	fail := func(err error) error {
		s.enc.Reset()
		s.enc.Uvarint(0)
		s.enc.U8(proto.CodeFatal)
		s.enc.Str(err.Error())
		_ = s.c.Send(proto.TError, s.enc.B)
		_ = s.c.Flush()
		return err
	}
	if ver != proto.Version {
		return fail(fmt.Errorf("protocol mismatch: client v%d, server v%d", ver, proto.Version))
	}
	if s.boundRoot != "" && wantDirect {
		return fail(errors.New("direct session cannot open a listener"))
	}
	if err := s.setSession(s.session, root); err != nil {
		return fail(err)
	}
	abs := s.root

	s.enc.Reset()
	s.enc.Uvarint(proto.Version)
	s.enc.Str(runtime.GOOS)
	s.enc.Str(runtime.GOARCH)
	s.enc.Str(abs)
	if wantDirect {
		// Only the SSH-authorized root and session are valid on this listener.
		root, session := s.root, s.session
		listener, listenErr := direct.Listen(func(c *direct.Conn) { _ = serve(s.ctx, c, root, session) })
		if listenErr == nil {
			s.listener = listener
		} // A blocked listener falls back to SSH.
	}
	if s.listener != nil {
		s.enc.Uvarint(uint64(s.listener.Offer.Port))
		s.enc.Bytes(s.listener.Offer.Key[:])
	} else {
		s.enc.Uvarint(0)
	}
	if err := s.c.Send(proto.THelloOK, s.enc.B); err != nil {
		return err
	}
	if err := s.c.Flush(); err != nil {
		return err
	}
	if level > 0 {
		return s.c.EnableCompression(level)
	}
	return nil
}

func (s *server) handle(t proto.Type, p []byte) error {
	d := proto.Dec{B: p}
	switch t {
	case proto.TNewSession:
		if s.boundRoot != "" || s.listener != nil {
			return errors.New("direct transport requires a fresh SSH bootstrap for each session")
		}
		session, root := d.Str(), d.Str()
		if d.Err != nil {
			return d.Err
		}
		if err := s.setSession(session, root); err != nil {
			return err
		}
		return nil
	case proto.TScan:
		return s.scan(&d)
	case proto.THashReq:
		return s.hash(&d)
	case proto.TMkdirs:
		return s.mkdirs(&d)
	case proto.TPut:
		return s.put(&d)
	case proto.TSymlink:
		return s.symlink(&d)
	case proto.TRangeWrite:
		return s.rangeWrite(&d)
	case proto.TRangeClose:
		return s.rangeClose(&d)
	case proto.TFinalize:
		return s.finalize(&d)
	case proto.TSigReq:
		return s.sig(&d)
	case proto.TDeltaBegin:
		return s.deltaBegin(&d)
	case proto.TDeltaCopy:
		return s.deltaCopy(&d)
	case proto.TDeltaData:
		return s.deltaData(&d)
	case proto.TDeltaEnd:
		return s.deltaEnd(&d)
	case proto.TMeta:
		return s.meta(&d)
	case proto.TDelete:
		return s.del(&d)
	case proto.TSync:
		seq := d.Uvarint()
		if d.Err != nil {
			return d.Err
		}
		s.enc.Reset()
		s.enc.Uvarint(seq)
		if err := s.c.Send(proto.TSyncAck, s.enc.B); err != nil {
			return err
		}
		return s.c.Flush()
	default:
		return fmt.Errorf("unknown message type %d", t)
	}
}

// abs resolves a client-supplied relative path, refusing anything that
// could escape the root.
func (s *server) abs(rel string) (string, error) {
	if rel == "" {
		return s.root, nil
	}
	if strings.HasPrefix(rel, "/") || strings.ContainsRune(rel, 0) {
		return "", fmt.Errorf("unsafe path %q", rel)
	}
	for seg := range strings.SplitSeq(rel, "/") {
		if seg == ".." || seg == "." || seg == "" {
			return "", fmt.Errorf("unsafe path %q", rel)
		}
	}
	return s.root + "/" + rel, nil
}

func (s *server) tmpPath(abs string, id uint64) string {
	return filepath.Dir(abs) + "/" + scan.TempPrefix + s.session + "." + strconv.FormatUint(id, 10)
}

func (s *server) sendErr(id uint64, code uint8, err error) error {
	s.enc.Reset()
	s.enc.Uvarint(id)
	s.enc.U8(code)
	s.enc.Str(err.Error())
	if err := s.c.Send(proto.TError, s.enc.B); err != nil {
		return err
	}
	return s.c.Flush()
}

func setMtime(path string, ns int64) error {
	ts := unix.NsecToTimespec(ns)
	return unix.UtimesNanoAt(unix.AT_FDCWD, path, []unix.Timespec{ts, ts}, unix.AT_SYMLINK_NOFOLLOW)
}

func (s *server) scan(d *proto.Dec) error {
	n := int(d.Uvarint())
	patterns := make([]string, 0, n)
	for range n {
		patterns = append(patterns, d.Str())
	}
	if d.Err != nil {
		return d.Err
	}
	filter, err := scan.NewFilter(patterns)
	if err != nil {
		return err
	}
	res := &scan.Result{}
	_, err = os.Stat(s.root)
	if err == nil {
		if s.cache == nil {
			s.cache = scan.NewCache(s.root)
		}
		res, err = s.cache.Scan(s.ctx, filter)
	} else if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		return err
	}
	s.enc.Reset()
	count := 0
	flushBatch := func() error {
		if count == 0 {
			return nil
		}
		hdr := binary.AppendUvarint(nil, uint64(count))
		err := s.c.Send(proto.TScanEntries, hdr, s.enc.B)
		s.enc.Reset()
		count = 0
		return err
	}
	for i := range res.Entries {
		res.Entries[i].Encode(&s.enc)
		count++
		if len(s.enc.B) >= 256<<10 {
			if err := flushBatch(); err != nil {
				return err
			}
		}
	}
	if err := flushBatch(); err != nil {
		return err
	}
	s.enc.Reset()
	s.enc.Uvarint(uint64(len(res.Entries)))
	warn := res.Warnings[:min(len(res.Warnings), 20)]
	s.enc.Uvarint(uint64(len(warn)))
	for _, w := range warn {
		s.enc.Str(w.Error())
	}
	if err := s.c.Send(proto.TScanDone, s.enc.B); err != nil {
		return err
	}
	return s.c.Flush()
}

func (s *server) hash(d *proto.Dec) error {
	req := d.Uvarint()
	n := int(d.Uvarint())
	paths := make([]string, n)
	for i := range paths {
		paths[i] = d.Str()
	}
	if d.Err != nil {
		return d.Err
	}
	hashes := make([]proto.Hash, n)
	ok := make([]bool, n)
	var wg sync.WaitGroup
	next := make(chan int)
	for range min(runtime.NumCPU(), max(n, 1)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 256<<10)
			for i := range next {
				abs, err := s.abs(paths[i])
				if err != nil {
					continue
				}
				h, err := scan.HashFile(abs, buf)
				hashes[i], ok[i] = h, err == nil
			}
		}()
	}
	for i := range paths {
		next <- i
	}
	close(next)
	wg.Wait()

	s.enc.Reset()
	s.enc.Uvarint(req)
	s.enc.Uvarint(uint64(n))
	for i := range hashes {
		if ok[i] {
			s.enc.U8(1)
			s.enc.Hash(hashes[i])
		} else {
			s.enc.U8(0)
		}
	}
	if err := s.c.Send(proto.THashResp, s.enc.B); err != nil {
		return err
	}
	return s.c.Flush()
}

func (s *server) mkdirs(d *proto.Dec) error {
	n := int(d.Uvarint())
	for range n {
		rel := d.Str()
		mode := uint32(d.Uvarint())
		if d.Err != nil {
			return d.Err
		}
		abs, err := s.abs(rel)
		if err != nil {
			return err
		}
		if err := mkdirAll(abs, scan.FileMode(mode)|0o700); err != nil {
			if err := s.sendErr(0, proto.CodeGeneric, err); err != nil {
				return err
			}
		}
	}
	return nil
}

// mkdirAll leaves existing directories alone and makes new directories usable
// while receiving. Final directory metadata restores source modes afterwards.
// Never change the host application's process-wide umask.
func mkdirAll(path string, mode os.FileMode) error {
	err := os.Mkdir(path, mode)
	if errors.Is(err, os.ErrNotExist) {
		if err := mkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		err = os.Mkdir(path, mode)
	}
	if err != nil {
		if fi, statErr := os.Stat(path); statErr == nil && fi.IsDir() {
			return nil
		}
		return err
	}
	return os.Chmod(path, mode)
}

func createFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|flag, perm)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		if merr := mkdirAll(filepath.Dir(path), 0o755); merr == nil {
			f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|flag, perm)
		}
	}
	return f, err
}

func (s *server) put(d *proto.Dec) error {
	id := d.Uvarint()
	flags := d.U8()
	rel := d.Str()
	mode := uint32(d.Uvarint())
	mtime := d.Varint()
	data := d.Rest()
	if d.Err != nil {
		return d.Err
	}
	abs, err := s.abs(rel)
	if err != nil {
		return err
	}
	target := abs
	if flags&proto.PutNew == 0 {
		target = s.tmpPath(abs, id)
	}
	err = func() error {
		f, err := createFile(target, os.O_TRUNC, scan.FileMode(mode))
		if err != nil {
			return err
		}
		if len(data) > 0 {
			if _, err := f.Write(data); err != nil {
				f.Close()
				return err
			}
		}
		if err := f.Chmod(scan.FileMode(mode)); err != nil {
			_ = f.Close() // Preserve the chmod error.
			return err
		}
		if err := setFileTimes(f, target, mtime); err != nil {
			f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if target != abs {
			return os.Rename(target, abs)
		}
		return nil
	}()
	if err != nil {
		if target != abs {
			_ = os.Remove(target)
		}
		return s.sendErr(id, proto.CodeGeneric, err)
	}
	return nil
}

func (s *server) symlink(d *proto.Dec) error {
	id := d.Uvarint()
	rel := d.Str()
	target := d.Str()
	mtime := d.Varint()
	if d.Err != nil {
		return d.Err
	}
	abs, err := s.abs(rel)
	if err != nil {
		return err
	}
	tmp := s.tmpPath(abs, id)
	err = os.Symlink(target, tmp)
	// workers can arrive before the primary's mkdir batch,
	// so symlinks create missing parents just as file writes already do.
	if errors.Is(err, os.ErrNotExist) {
		if err = mkdirAll(filepath.Dir(tmp), 0o755); err == nil {
			err = os.Symlink(target, tmp)
		}
	}
	if err == nil {
		_ = setMtime(tmp, mtime)
		err = os.Rename(tmp, abs)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return s.sendErr(id, proto.CodeGeneric, err)
	}
	return nil
}

func (s *server) rangeWrite(d *proto.Dec) error {
	id := d.Uvarint()
	rel := d.Str()
	size := int64(d.Uvarint())
	off := int64(d.Uvarint())
	data := d.Rest()
	if d.Err != nil {
		return d.Err
	}
	rf := s.ranges[id]
	if rf == nil {
		rf = &rangeFile{}
		s.ranges[id] = rf
		abs, err := s.abs(rel)
		if err != nil {
			return err
		}
		rf.tmp = s.tmpPath(abs, id)
		// Several server processes may open the same temp file concurrently;
		// truncating to the final size is idempotent and never discards data
		// already written by a peer.
		rf.f, rf.err = createFile(rf.tmp, 0, 0o600)
		if rf.err == nil {
			rf.err = rf.f.Truncate(size)
		}
		if rf.err != nil {
			if err := s.sendErr(id, proto.CodeGeneric, rf.err); err != nil {
				return err
			}
		}
	}
	rf.received += int64(len(data))
	if rf.err != nil {
		return nil
	}
	if _, err := rf.f.WriteAt(data, off); err != nil {
		rf.err = err
		return s.sendErr(id, proto.CodeGeneric, err)
	}
	return nil
}

func (s *server) rangeClose(d *proto.Dec) error {
	id := d.Uvarint()
	if d.Err != nil {
		return d.Err
	}
	var n int64
	if rf := s.ranges[id]; rf != nil {
		n = rf.received
		if rf.f != nil {
			if err := rf.f.Close(); err != nil && rf.err == nil {
				if err := s.sendErr(id, proto.CodeGeneric, err); err != nil {
					return err
				}
			}
		}
		delete(s.ranges, id)
	}
	s.enc.Reset()
	s.enc.Uvarint(id)
	s.enc.Uvarint(uint64(n))
	if err := s.c.Send(proto.TRangeAck, s.enc.B); err != nil {
		return err
	}
	return s.c.Flush()
}

func (s *server) finalize(d *proto.Dec) error {
	id := d.Uvarint()
	rel := d.Str()
	mode := uint32(d.Uvarint())
	mtime := d.Varint()
	if d.Err != nil {
		return d.Err
	}
	abs, err := s.abs(rel)
	if err != nil {
		return err
	}
	tmp := s.tmpPath(abs, id)
	err = os.Chmod(tmp, scan.FileMode(mode))
	if err == nil {
		err = setMtime(tmp, mtime)
	}
	if err == nil {
		err = os.Rename(tmp, abs)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return s.sendErr(id, proto.CodeGeneric, err)
	}
	return nil
}

func (s *server) sig(d *proto.Dec) error {
	id := d.Uvarint()
	rel := d.Str()
	if d.Err != nil {
		return d.Err
	}
	abs, err := s.abs(rel)
	if err != nil {
		return err
	}
	s.enc.Reset()
	s.enc.Uvarint(id)
	err = func() error {
		f, err := os.Open(abs)
		if err != nil {
			return err
		}
		defer f.Close()
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		if !fi.Mode().IsRegular() {
			return fmt.Errorf("not a regular file")
		}
		p := cdc.ParamsFor(fi.Size())
		var body proto.Enc
		count := 0
		err = cdc.Split(bufio.NewReaderSize(f, 1<<20), p, func(_ int64, data []byte) error {
			body.Uvarint(uint64(len(data)))
			body.Hash(scan.HashBytes(data))
			count++
			return nil
		})
		if err != nil {
			return err
		}
		s.enc.U8(1)
		s.enc.Uvarint(uint64(fi.Size()))
		s.enc.Uvarint(uint64(p.Min))
		s.enc.Uvarint(uint64(p.Avg))
		s.enc.Uvarint(uint64(p.Max))
		s.enc.Uvarint(uint64(count))
		s.enc.B = append(s.enc.B, body.B...)
		return nil
	}()
	if err != nil {
		s.enc.Reset()
		s.enc.Uvarint(id)
		s.enc.U8(0)
	}
	if err := s.c.Send(proto.TSig, s.enc.B); err != nil {
		return err
	}
	return s.c.Flush()
}

func (s *server) deltaBegin(d *proto.Dec) error {
	id := d.Uvarint()
	rel := d.Str()
	size := int64(d.Uvarint())
	if d.Err != nil {
		return d.Err
	}
	s.abortDelta()
	abs, err := s.abs(rel)
	if err != nil {
		return err
	}
	ds := &deltaState{id: id, abs: abs, tmp: s.tmpPath(abs, id), size: size, h: xxh3.New128()}
	s.delta = ds
	ds.basis, ds.err = os.Open(abs)
	if ds.err != nil {
		return nil
	}
	ds.out, ds.err = createFile(ds.tmp, os.O_TRUNC, 0o600)
	if ds.err != nil {
		return nil
	}
	ds.w = bufio.NewWriterSize(ds.out, 1<<20)
	return nil
}

func (s *server) deltaCopy(d *proto.Dec) error {
	off := int64(d.Uvarint())
	n := int64(d.Uvarint())
	if d.Err != nil {
		return d.Err
	}
	ds := s.delta
	if ds == nil {
		return errors.New("delta copy outside delta")
	}
	for n > 0 && ds.err == nil {
		k := min(n, int64(len(s.buf)))
		var r int
		r, ds.err = ds.basis.ReadAt(s.buf[:k], off)
		if int64(r) < k {
			if ds.err == nil || ds.err == io.EOF {
				ds.err = errors.New("basis file changed during transfer")
			}
			break
		}
		ds.err = nil
		_, _ = ds.h.Write(s.buf[:k])
		_, ds.err = ds.w.Write(s.buf[:k])
		ds.written += k
		off += k
		n -= k
	}
	return nil
}

func (s *server) deltaData(d *proto.Dec) error {
	data := d.Rest()
	ds := s.delta
	if ds == nil {
		return errors.New("delta data outside delta")
	}
	if ds.err == nil {
		_, _ = ds.h.Write(data)
		_, ds.err = ds.w.Write(data)
		ds.written += int64(len(data))
	}
	return nil
}

func (s *server) deltaEnd(d *proto.Dec) error {
	mode := uint32(d.Uvarint())
	mtime := d.Varint()
	want := d.Hash()
	if d.Err != nil {
		return d.Err
	}
	ds := s.delta
	if ds == nil {
		return errors.New("delta end outside delta")
	}
	s.delta = nil
	err := ds.err
	if err == nil {
		err = ds.w.Flush()
	}
	if err == nil {
		if got := ds.h.Sum128(); ds.written != ds.size || got.Hi != want.Hi || got.Lo != want.Lo {
			err = errors.New("reconstructed file does not match source hash")
		}
	}
	if err == nil {
		err = ds.out.Chmod(scan.FileMode(mode))
	}
	if ds.out != nil {
		if cerr := ds.out.Close(); err == nil {
			err = cerr
		}
	}
	if ds.basis != nil {
		ds.basis.Close()
	}
	if err == nil {
		err = setMtime(ds.tmp, mtime)
	}
	if err == nil {
		err = os.Rename(ds.tmp, ds.abs)
	}
	if err != nil {
		_ = os.Remove(ds.tmp)
		return s.sendErr(ds.id, proto.CodeDeltaFailed, err)
	}
	return nil
}

func (s *server) abortDelta() {
	if ds := s.delta; ds != nil {
		if ds.out != nil {
			ds.out.Close()
			_ = os.Remove(ds.tmp)
		}
		if ds.basis != nil {
			ds.basis.Close()
		}
		s.delta = nil
	}
}

func (s *server) meta(d *proto.Dec) error {
	n := int(d.Uvarint())
	for range n {
		rel := d.Str()
		kind := scan.Kind(d.U8())
		mode := uint32(d.Uvarint())
		mtime := d.Varint()
		if d.Err != nil {
			return d.Err
		}
		abs, err := s.abs(rel)
		if err != nil {
			return err
		}
		if kind != scan.KindSymlink {
			err = os.Chmod(abs, scan.FileMode(mode))
		}
		if err == nil {
			err = setMtime(abs, mtime)
		}
		if err != nil {
			if err := s.sendErr(0, proto.CodeGeneric, err); err != nil {
				return err
			}
		}
	}
	return nil
}

// del removes paths. Recursive deletes replace an entry whose type changed.
// Otherwise entries arrive children-first and directories are only removed
// once empty, so anything protected or hidden from the scan survives.
func (s *server) del(d *proto.Dec) error {
	recursive := d.U8()&proto.DeleteRecursive != 0
	n := int(d.Uvarint())
	for range n {
		rel := d.Str()
		if d.Err != nil {
			return d.Err
		}
		abs, err := s.abs(rel)
		if err != nil {
			return err
		}
		if rel == "" {
			return errors.New("refusing to delete the sync root")
		}
		if recursive {
			err = os.RemoveAll(abs)
		} else if err = unix.Unlink(abs); errors.Is(err, unix.EISDIR) || errors.Is(err, unix.EPERM) {
			err = unix.Rmdir(abs)
			if errors.Is(err, unix.ENOTEMPTY) || errors.Is(err, unix.EEXIST) {
				err = nil
			}
		}
		if err != nil && !errors.Is(err, unix.ENOENT) {
			if err := s.sendErr(0, proto.CodeGeneric, &os.PathError{Op: "delete", Path: abs,
				Err: err}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *server) cleanup() {
	s.abortDelta()
	for id, rf := range s.ranges {
		if rf.f != nil {
			rf.f.Close()
			_ = os.Remove(rf.tmp)
		}
		delete(s.ranges, id)
	}
}

func (s *server) setSession(session, root string) error {
	if session == "" {
		return errors.New("empty session id")
	}
	for _, r := range session {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9') {
			return errors.New("bad session id")
		}
	}
	if root == "~" || strings.HasPrefix(root, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		root = filepath.Join(home, root[1:])
	}
	if root == "" {
		root = "."
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if s.boundRoot != "" && (abs != s.boundRoot || session != s.boundSession) {
		return errors.New("direct session does not match SSH authorization")
	}
	s.cleanup()
	if s.root != abs && s.cache != nil {
		s.cache.Close()
		s.cache = nil
	}
	s.root = abs
	s.session = session
	return nil
}
