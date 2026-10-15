package scan

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
)

// FSEvents runs on a private dispatch queue; a same-volume event fences
// delivery before cached entries can be reused.
var fsAPI struct {
	sync.Once
	err           error
	stringCreate  func(uintptr, string, uint32) uintptr
	arrayCreate   func(uintptr, *uintptr, int64, uintptr) uintptr
	release       func(uintptr)
	create        func(uintptr, uintptr, *fsContext, uintptr, uint64, float64, uint32) uintptr
	setQueue      func(uintptr, uintptr)
	start         func(uintptr) bool
	flush         func(uintptr)
	stop          func(uintptr)
	invalidate    func(uintptr)
	streamRelease func(uintptr)
	queueCreate   func(string, uintptr) uintptr
	queueRelease  func(uintptr)
	callback      uintptr
	mu            sync.Mutex
	next          uintptr
	watches       map[uintptr]*nativeWatch
}

type fsContext struct {
	version                            int64
	info, retain, release, description uintptr
}
type nativeWatch struct {
	fenceDir             string
	fencePath            string
	fence                chan struct{}
	sequence             uint64
	mu                   sync.Mutex
	root                 string
	stream, queue, token uintptr
	dirty                map[string]bool
	lost                 bool
}

func initFS() {
	fsAPI.watches = map[uintptr]*nativeWatch{}
	cf, err := purego.Dlopen("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation", purego.RTLD_NOW|purego.RTLD_LOCAL)
	if err != nil {
		fsAPI.err = err
		return
	}
	cs, err := purego.Dlopen("/System/Library/Frameworks/CoreServices.framework/CoreServices", purego.RTLD_NOW|purego.RTLD_LOCAL)
	if err != nil {
		fsAPI.err = err
		return
	}
	dq, err := purego.Dlopen("/usr/lib/system/libdispatch.dylib", purego.RTLD_NOW|purego.RTLD_LOCAL)
	if err != nil {
		fsAPI.err = err
		return
	}
	purego.RegisterLibFunc(&fsAPI.stringCreate, cf, "CFStringCreateWithCString")
	purego.RegisterLibFunc(&fsAPI.arrayCreate, cf, "CFArrayCreate")
	purego.RegisterLibFunc(&fsAPI.release, cf, "CFRelease")
	purego.RegisterLibFunc(&fsAPI.create, cs, "FSEventStreamCreate")
	purego.RegisterLibFunc(&fsAPI.setQueue, cs, "FSEventStreamSetDispatchQueue")
	purego.RegisterLibFunc(&fsAPI.start, cs, "FSEventStreamStart")
	purego.RegisterLibFunc(&fsAPI.flush, cs, "FSEventStreamFlushSync")
	purego.RegisterLibFunc(&fsAPI.stop, cs, "FSEventStreamStop")
	purego.RegisterLibFunc(&fsAPI.invalidate, cs, "FSEventStreamInvalidate")
	purego.RegisterLibFunc(&fsAPI.streamRelease, cs, "FSEventStreamRelease")
	purego.RegisterLibFunc(&fsAPI.queueCreate, dq, "dispatch_queue_create")
	purego.RegisterLibFunc(&fsAPI.queueRelease, dq, "dispatch_release")
	fsAPI.callback = purego.NewCallback(func(_ uintptr, token uintptr, count uintptr, paths **byte, flags *uint32, _ *uint64) {
		fsAPI.mu.Lock()
		w := fsAPI.watches[token]
		fsAPI.mu.Unlock()
		if w == nil {
			return
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		ps := unsafe.Slice(paths, count)
		fs := unsafe.Slice(flags, count)
		for i, p := range ps {
			// MustScanSubDirs, dropped events, wrapped IDs, root changes, mount/unmount.
			if fs[i]&0xff != 0 {
				w.lost = true
			}
			n := 0
			for *(*byte)(unsafe.Add(unsafe.Pointer(p), n)) != 0 {
				n++
			}
			abs := string(unsafe.Slice(p, n))
			if abs == w.fencePath && w.fence != nil {
				close(w.fence)
				w.fence = nil
			}
			if abs == w.fenceDir || strings.HasPrefix(abs, w.fenceDir+"/") {
				continue
			}
			rel, err := filepath.Rel(w.root, abs)
			if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
				w.lost = true
				continue
			}
			if IsTemp(filepath.Base(rel)) {
				continue
			}
			if rel == "." {
				rel = ""
			}
			w.dirty[parentDir(rel)] = true
			if fs[i]&0x20000 != 0 {
				w.dirty[rel] = true
			}
			// Renamed directories can replace an existing cached subtree.
			if fs[i]&0x20000 != 0 && fs[i]&(0x100|0x200|0x800) != 0 {
				w.lost = true
			}
		}
	})
}
func newNativeWatch(root string) (*nativeWatch, error) {
	fsAPI.Do(initFS)
	if fsAPI.err != nil {
		return nil, fsAPI.err
	}
	fenceDir, err := os.MkdirTemp("", TempPrefix+"events-")
	if err != nil {
		return nil, err
	}
	fenceDir, err = filepath.EvalSymlinks(fenceDir)
	if err != nil {
		return nil, err
	}
	var rootStat, fenceStat unix.Stat_t
	if err := unix.Stat(root, &rootStat); err != nil {
		_ = os.RemoveAll(fenceDir)
		return nil, err
	}
	if err := unix.Stat(fenceDir, &fenceStat); err != nil {
		_ = os.RemoveAll(fenceDir)
		return nil, err
	}
	if rootStat.Dev != fenceStat.Dev {
		_ = os.RemoveAll(fenceDir)
		return nil, fmt.Errorf("FSEvents fence requires the same volume")
	}
	w := &nativeWatch{root: root, fenceDir: fenceDir, dirty: map[string]bool{}}
	fsAPI.mu.Lock()
	fsAPI.next++
	w.token = fsAPI.next
	fsAPI.watches[w.token] = w
	fsAPI.mu.Unlock()
	str := fsAPI.stringCreate(0, root, 0x08000100)
	fenceStr := fsAPI.stringCreate(0, fenceDir, 0x08000100)
	paths := [2]uintptr{str, fenceStr}
	arr := fsAPI.arrayCreate(0, &paths[0], 2, 0)
	ctx := fsContext{info: w.token}
	w.stream = fsAPI.create(0, fsAPI.callback, &ctx, arr, ^uint64(0), 0, 0x2|0x4|0x10)
	fsAPI.release(arr)
	fsAPI.release(str)
	fsAPI.release(fenceStr)
	if w.stream == 0 {
		w.close()
		return nil, fmt.Errorf("FSEventStreamCreate failed")
	}
	w.queue = fsAPI.queueCreate("rift.fsevents", 0)
	fsAPI.setQueue(w.stream, w.queue)
	if !fsAPI.start(w.stream) {
		w.close()
		return nil, fmt.Errorf("FSEventStreamStart failed")
	}
	return w, nil
}
func (*nativeWatch) add(string) {}
func (w *nativeWatch) drain() ([]string, bool) {
	// FSEvents is asynchronous even at zero latency. A unique event on the
	// same volume fences earlier edits; never trust the cache without it.
	w.mu.Lock()
	w.sequence++
	marker := filepath.Join(w.fenceDir, strconv.FormatUint(w.sequence, 10))
	fence := make(chan struct{})
	w.fencePath = marker
	w.fence = fence
	w.mu.Unlock()
	err := os.WriteFile(marker, nil, 0o600)
	confirmed := false
	if err == nil {
		timer := time.NewTimer(100 * time.Millisecond)
		ticker := time.NewTicker(time.Millisecond)
	waitFence:
		for {
			fsAPI.flush(w.stream)
			select {
			case <-fence:
				confirmed = true
				break waitFence
			case <-timer.C:
				break waitFence
			case <-ticker.C:
			}
		}
		timer.Stop()
		ticker.Stop()
		_ = os.Remove(marker)
	}
	w.mu.Lock()
	if !confirmed {
		w.lost = true
	}
	defer w.mu.Unlock()
	dirs := make([]string, 0, len(w.dirty))
	for d := range w.dirty {
		dirs = append(dirs, d)
	}
	lost := w.lost
	w.lost = false
	clear(w.dirty)
	return dirs, lost
}
func (w *nativeWatch) close() {
	defer os.RemoveAll(w.fenceDir)
	if w.stream != 0 {
		fsAPI.stop(w.stream)
		fsAPI.invalidate(w.stream)
		fsAPI.streamRelease(w.stream)
	}
	if w.queue != 0 {
		fsAPI.queueRelease(w.queue)
	}
	fsAPI.mu.Lock()
	delete(fsAPI.watches, w.token)
	fsAPI.mu.Unlock()
}
