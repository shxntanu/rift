package server

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// setFileTimes sets atime and mtime through the open descriptor, avoiding a
// path lookup: utimensat with a NULL path operates on the fd itself.
func setFileTimes(f *os.File, _ string, ns int64) error {
	ts := [2]unix.Timespec{unix.NsecToTimespec(ns), unix.NsecToTimespec(ns)}
	rc, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var errno unix.Errno
	if cerr := rc.Control(func(fd uintptr) {
		_, _, errno = unix.Syscall6(unix.SYS_UTIMENSAT, fd, 0, uintptr(unsafe.Pointer(&ts[0])),
			0, 0, 0)
	}); cerr != nil {
		return cerr
	}
	if errno != 0 {
		return errno
	}
	return nil
}
