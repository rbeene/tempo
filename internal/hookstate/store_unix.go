//go:build darwin || linux

package hookstate

import (
	"errors"
	"os"
	"syscall"
)

func privateInfo(fi os.FileInfo, dir bool) bool {
	if fi == nil || fi.IsDir() != dir || (!dir && !fi.Mode().IsRegular()) || fi.Mode().Perm()&0077 != 0 {
		return false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && st.Uid == uint32(os.Getuid()) && (dir || st.Nlink == 1)
}
func openNoFollow(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	return root.OpenFile(name, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, mode)
}
func tryLockFile(f *os.File) (bool, error) {
	e := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(e, syscall.EWOULDBLOCK) || errors.Is(e, syscall.EAGAIN) {
		return false, nil
	}
	return e == nil, e
}
func unlockFile(f *os.File) { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }
