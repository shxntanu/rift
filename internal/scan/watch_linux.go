package scan

import (
	"encoding/binary"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

type nativeWatch struct {
	mu    sync.Mutex
	root  string
	fd    int
	paths map[int]string
	lost  bool
}

func newNativeWatch(root string) (*nativeWatch, error) {
	fd, err := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	if err != nil {
		return nil, err
	}
	w := &nativeWatch{root: root, fd: fd, paths: map[int]string{}}
	w.add("")
	return w, nil
}
func (w *nativeWatch) add(dir string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	wd, err := unix.InotifyAddWatch(w.fd, filepath.Join(w.root, dir), unix.IN_CREATE|unix.IN_DELETE|unix.IN_MOVED_FROM|unix.IN_MOVED_TO|unix.IN_MODIFY|unix.IN_ATTRIB|unix.IN_DELETE_SELF|unix.IN_MOVE_SELF|unix.IN_ONLYDIR|unix.IN_DONT_FOLLOW)
	if err != nil {
		w.lost = true
		return
	}
	w.paths[wd] = dir
}
func (w *nativeWatch) drain() ([]string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	dirty := map[string]bool{}
	lost := w.lost
	w.lost = false
	var buf [65536]byte
	for {
		n, err := unix.Read(w.fd, buf[:])
		if err == unix.EAGAIN {
			break
		}
		if err != nil || n == 0 {
			lost = true
			break
		}
		for off := 0; off+16 <= n; {
			wd := int(int32(binary.NativeEndian.Uint32(buf[off:])))
			mask := binary.NativeEndian.Uint32(buf[off+4:])
			size := int(binary.NativeEndian.Uint32(buf[off+12:]))
			if off+16+size > n {
				lost = true
				break
			}
			dir, ok := w.paths[wd]
			if !ok || mask&(unix.IN_Q_OVERFLOW|unix.IN_IGNORED|unix.IN_UNMOUNT|unix.IN_DELETE_SELF|unix.IN_MOVE_SELF) != 0 {
				lost = true
			}
			if mask&unix.IN_ISDIR != 0 && mask&(unix.IN_MOVED_FROM|unix.IN_MOVED_TO) != 0 {
				lost = true
			}
			if mask&unix.IN_IGNORED != 0 {
				delete(w.paths, wd)
			}
			name := strings.TrimRight(string(buf[off+16:off+16+size]), "\x00")
			if ok && !IsTemp(name) {
				dirty[dir] = true
				dirty[parentDir(dir)] = true
			}
			off += 16 + size
		}
	}
	dirs := make([]string, 0, len(dirty))
	for d := range dirty {
		dirs = append(dirs, d)
	}
	return dirs, lost
}
func (w *nativeWatch) close() { _ = unix.Close(w.fd) }
