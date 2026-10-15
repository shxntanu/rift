//go:build !linux

package server

import "os"

func setFileTimes(_ *os.File, path string, ns int64) error {
	return setMtime(path, ns)
}
