//go:build darwin || linux

package privatefs

import (
	"errors"
	"os"
	"syscall"
)

func PrivateInfo(fi os.FileInfo, dir bool) bool {
	if fi == nil || fi.IsDir() != dir || (!dir && !fi.Mode().IsRegular()) || fi.Mode().Perm()&0077 != 0 {
		return false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && st.Uid == uint32(os.Getuid()) && (dir || st.Nlink == 1)
}
func OpenNoFollow(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	return root.OpenFile(name, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, mode)
}
func TryLock(f *os.File) (bool, error) {
	e := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(e, syscall.EWOULDBLOCK) || errors.Is(e, syscall.EAGAIN) {
		return false, nil
	}
	return e == nil, e
}
func Unlock(f *os.File) { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }

func directoryInfo(fi os.FileInfo, private bool) bool {
	if private {
		return PrivateInfo(fi, true)
	}
	if fi == nil || !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 || fi.Mode().Perm()&0022 != 0 {
		return false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && st.Uid == uint32(os.Getuid())
}

func SocketInfo(fi os.FileInfo) bool {
	if fi == nil || fi.Mode()&os.ModeSocket == 0 || fi.Mode()&os.ModeSymlink != 0 {
		return false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && st.Uid == uint32(os.Getuid()) && st.Nlink == 1
}
