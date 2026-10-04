//go:build darwin && (amd64 || arm64)

package sqliteio

import "golang.org/x/sys/unix"

func rootDirectoryAccess(fd int) error {
	// Check the actual retained directory, not a name that could select a decoy.
	return unix.Faccessat(fd, ".", unix.R_OK|unix.X_OK, unix.AT_EACCESS)
}
