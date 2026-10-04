//go:build linux && (amd64 || arm64)

package sqliteio

import "golang.org/x/sys/unix"

func rcQAKernelAccess(fd int) error {
	return unix.Faccessat2(fd, ".", unix.R_OK|unix.X_OK, unix.AT_EACCESS)
}
