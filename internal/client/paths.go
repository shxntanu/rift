package client

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/shxntanu/rift/internal/transport"
)

// DestinationRoot applies the source's trailing-slash semantics before cleaning
// or absolutizing it. Both the CLI and library use this convention.
func DestinationRoot(src, dst string) string {
	if strings.HasSuffix(src, "/") {
		return dst
	}
	base := path.Base(src)
	if base == "." || base == "/" {
		return dst
	}
	if dst == "" {
		return base
	}
	return strings.TrimRight(dst, "/") + "/" + base
}

func validatePaths(src string, dst transport.Endpoint) (string, transport.Endpoint, error) {
	if src == "" {
		return "", dst, errors.New("empty source directory")
	}
	src, err := filepath.Abs(src)
	if err != nil {
		return "", dst, err
	}
	fi, err := os.Stat(src)
	if err != nil {
		return "", dst, err
	}
	if !fi.IsDir() {
		return "", dst, fmt.Errorf("%s: source must be a directory", src)
	}
	if strings.ContainsRune(dst.Path, 0) {
		return "", dst, errors.New("invalid destination path")
	}
	if dst.Path == "" {
		dst.Path = "."
	}
	if dst.Host != "" {
		return src, dst, nil
	}
	dst.Path, err = filepath.Abs(dst.Path)
	if err != nil {
		return "", dst, err
	}
	realSrc, err := filepath.EvalSymlinks(src)
	if err != nil {
		return "", dst, err
	}
	realDst, err := resolveMissing(dst.Path)
	if err != nil {
		return "", dst, err
	}
	if within(realSrc, realDst) || within(realDst, realSrc) {
		return "", dst, errors.New("source and destination directories overlap")
	}
	return src, dst, nil
}

// Resolve existing ancestors too, so a missing destination beneath a symlink
// cannot evade overlap checks. This does not promise protection against races.
func resolveMissing(p string) (string, error) {
	real, err := filepath.EvalSymlinks(p)
	if err == nil {
		return real, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if fi, statErr := os.Lstat(p); statErr == nil && fi.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(p)
		if err != nil {
			return "", err
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(p), target)
		}
		return resolveMissing(target)
	}
	parent := filepath.Dir(p)
	if parent == p {
		return "", err
	}
	real, err = resolveMissing(parent)
	if err != nil {
		return "", err
	}
	return filepath.Join(real, filepath.Base(p)), nil
}

func within(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
