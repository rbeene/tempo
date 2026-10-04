//go:build linux && (amd64 || arm64)

package sqliteio

import (
	"modernc.org/libc"
)

func errnoAddress(tls *libc.TLS) uintptr {
	return libc.X__errno_location(tls)
}
