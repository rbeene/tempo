//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"unsafe"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

// These pinned ABI values contain numeric fields and foreign addresses only.
// A Go pointer, string, slice, or interface must never be copied this way.
type nativeValue interface {
	int32 | uint32 | int64 | uintptr | lib.Tsqlite3_vfs | lib.Tstat
}

// nativeLoad copies a value while the caller owns access to live foreign storage.
// GoBytes is borrowed; only the copied value leaves this function. The Go side
// stays a pointer throughout, so neither stack movement nor GC loses it.
func nativeLoad[T nativeValue](address uintptr) T {
	var value T
	n := int(unsafe.Sizeof(value))
	copy(unsafe.Slice((*byte)(unsafe.Pointer(&value)), n), libc.GoBytes(address, n))
	return value
}

// The registered VFS remains SQLite-allocated and process-lived. Copying its
// pointer-free ABI value does not transfer ownership of its foreign addresses.
func storeNativeVFS(address uintptr, value lib.Tsqlite3_vfs) {
	n := int(unsafe.Sizeof(value))
	copy(libc.GoBytes(address, n), unsafe.Slice((*byte)(unsafe.Pointer(&value)), n))
}

// C __ATOMIC_SEQ_CST, also used by the pinned libc's portable atomic tests.
const nativeSeqCst = 5
