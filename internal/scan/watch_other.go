//go:build !linux && !darwin

package scan

import "errors"

type nativeWatch struct{}

func newNativeWatch(string) (*nativeWatch, error) {
	return nil, errors.New("native watching unavailable")
}
func (*nativeWatch) add(string)              {}
func (*nativeWatch) drain() ([]string, bool) { return nil, true }
func (*nativeWatch) close()                  {}
