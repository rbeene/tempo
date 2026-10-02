//go:build darwin || linux

package terminal

import (
	"golang.org/x/sys/unix"
	"os"
)

func duplicateTerminal(f *os.File) (*os.File, func(), error) {
	fd := int(f.Fd())
	flags, e := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if e != nil {
		return nil, nil, e
	}
	dup, e := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if e != nil {
		return nil, nil, e
	}
	if e = unix.SetNonblock(dup, true); e != nil {
		unix.Close(dup)
		return nil, nil, e
	}
	owned := os.NewFile(uintptr(dup), "tempo-terminal")
	return owned, func() { owned.Close(); _, _ = unix.FcntlInt(uintptr(fd), unix.F_SETFL, flags) }, nil
}
