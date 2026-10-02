package privatefs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

var ErrUnsafe = errors.New("unsafe private file")
var ErrDurability = errors.New("private file durability unknown")

func OpenDirectory(dir string, create bool) (*os.Root, os.FileInfo, error) {
	return openDirectory(dir, create, true)
}
func OpenServiceDirectory(dir string, create bool) (*os.Root, os.FileInfo, error) {
	return openDirectory(dir, create, false)
}
func openDirectory(dir string, create, private bool) (*os.Root, os.FileInfo, error) {
	cur := string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(dir, cur), string(filepath.Separator)) {
		if part == "" {
			continue
		}
		parent := cur
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if errors.Is(err, os.ErrNotExist) && create {
			if err = os.Mkdir(cur, 0700); err != nil && !errors.Is(err, os.ErrExist) {
				return nil, nil, ErrUnsafe
			}
			pf, e := os.Open(parent)
			if e != nil {
				return nil, nil, ErrDurability
			}
			e = pf.Sync()
			pf.Close()
			if e != nil {
				return nil, nil, ErrDurability
			}
			fi, err = os.Lstat(cur)
		}
		if err != nil {
			return nil, nil, err
		}
		if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return nil, nil, ErrUnsafe
		}
	}
	before, err := os.Lstat(dir)
	if err != nil {
		return nil, nil, err
	}
	if !directoryInfo(before, private) {
		return nil, nil, ErrUnsafe
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, nil, ErrUnsafe
	}
	f, err := root.Open(".")
	if err != nil {
		root.Close()
		return nil, nil, ErrUnsafe
	}
	after, err := f.Stat()
	f.Close()
	if err != nil || !os.SameFile(before, after) || !directoryInfo(after, private) {
		root.Close()
		return nil, nil, ErrUnsafe
	}
	return root, after, nil
}
