//go:build darwin && (amd64 || arm64)

package sqliteio

import (
	"modernc.org/libc"
)

func errnoAddress(tls *libc.TLS) uintptr {
	return libc.X__error(tls)
}
