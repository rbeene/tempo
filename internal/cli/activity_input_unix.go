//go:build darwin || linux

package cli

import (
	"os"
	"syscall"
	"time"
)

// Inherited stdin is often a blocking NewFile, for which deadlines are not
// supported. A nonblocking duplicate is registered with Go's poller. Descriptor
// status flags are shared by dup, so preserve and restore the original flags.
func deadlineActivityFile(original *os.File, deadline time.Time) (*os.File, func(), error) {
	info, err := original.Stat()
	if err != nil {
		return nil, nil, err
	}
	// Regular files have finite, byte-limited input and cannot use the fd poller.
	if info.Mode().IsRegular() {
		return original, func() {}, nil
	}
	raw, err := original.SyscallConn()
	if err != nil {
		return nil, nil, err
	}
	var duplicate int
	var flags uintptr
	var operationErr error
	err = raw.Control(func(fd uintptr) {
		var errno syscall.Errno
		flags, _, errno = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
		if errno != 0 {
			operationErr = errno
			return
		}
		duplicate, operationErr = syscall.Dup(int(fd))
		if operationErr != nil {
			return
		}
		operationErr = syscall.SetNonblock(duplicate, true)
		if operationErr != nil {
			syscall.Close(duplicate)
		}
	})
	if err != nil {
		return nil, nil, err
	}
	if operationErr != nil {
		return nil, nil, operationErr
	}
	reader := os.NewFile(uintptr(duplicate), "tempo-activity-input")
	restore := func() {
		reader.Close()
		_ = raw.Control(func(fd uintptr) { _, _, _ = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_SETFL, flags) })
	}
	if err := reader.SetReadDeadline(deadline); err != nil {
		restore()
		return nil, nil, err
	}
	return reader, restore, nil
}
