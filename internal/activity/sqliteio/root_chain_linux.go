//go:build linux && (amd64 || arm64)

package sqliteio

import "golang.org/x/sys/unix"

func rootDirectoryAccess(fd int) error {
	// Faccessat's compatibility emulation checks mode bits, not all ACLs.
	// Unsupported/blocked faccessat2 therefore selects the original walker.
	return unix.Faccessat2(fd, ".", unix.R_OK|unix.X_OK, unix.AT_EACCESS)
}
