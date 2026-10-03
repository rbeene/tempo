//go:build linux && (amd64 || arm64)

package sqliteio

import (
	"modernc.org/libc"
	"unsafe"
)

func errnoPointer(tls *libc.TLS) *int32 {
	return (*int32)(unsafe.Pointer(libc.X__errno_location(tls)))
}
