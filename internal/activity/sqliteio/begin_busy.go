//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"time"
	"unsafe"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

const (
	beginBusyStateBytes     = 16
	beginBusyDeadlineOffset = 8
)

// Both values live for the process. The epoch supplies only monotonic elapsed
// time; it retains no connection, filesystem observation or application state.
var beginBusyEpoch = time.Now()
var beginBusyValue = beginBusy

// The existing SQLite-owned cancellation cell also holds an aligned int64
// deadline. Only the operation owner writes that deadline, before installing
// this callback; the watcher still changes only the atomic uint32 at offset 0.
func (c *Conn) beginBusyRemaining(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return safeError(BeginPhase, err)
	}
	now := time.Now()
	left := c.acquireDeadline.Sub(now)
	if deadline, ok := ctx.Deadline(); ok && deadline.Sub(now) < left {
		left = deadline.Sub(now)
	}
	if left <= 0 {
		return safeError(Admission, ErrBusy)
	}
	// Preserve busy_timeout's existing maximum for nondefault options.
	if maximum := time.Duration(2147483647) * time.Millisecond; left > maximum {
		left = maximum
	}
	elapsed := now.Sub(beginBusyEpoch)
	if maximum := time.Duration(1<<63-1) - elapsed; left > maximum {
		left = maximum
	}
	if c.cancelFlag == 0 || vfsMemory == 0 || nativeLoad[lib.Tsqlite3_vfs](vfsMemory).FxSleep == 0 {
		return safeError(Admission, ErrUnsafe)
	}
	libc.AssignPtrInt64(c.cancelFlag+beginBusyDeadlineOffset, int64(elapsed+left))
	if rc := lib.Xsqlite3_busy_handler(c.tls, c.db, functionPointer(beginBusyValue), c.cancelFlag); rc != lib.SQLITE_OK {
		c.poisoned = true
		return engineError(Admission, rc, ctx)
	}
	return nil
}

// SQLite retries its lock within the original single BEGIN step. Unlike the
// default growing sleeps, each requested wait is at most 1ms and never exceeds
// the original absolute admission deadline. The OS may still deschedule us.
// This callback invokes only the VFS sleep primitive, not SQL or application code.
func beginBusy(tls *libc.TLS, cell uintptr, _ int32) int32 {
	if cell == 0 || libc.AtomicLoadNUint32(cell, nativeSeqCst) != 0 {
		return 0
	}
	deadline := time.Duration(nativeLoad[int64](cell + beginBusyDeadlineOffset))
	left := deadline - time.Since(beginBusyEpoch)
	if left < time.Microsecond || vfsMemory == 0 {
		return 0
	}
	if left > time.Millisecond {
		left = time.Millisecond
	}
	vfs := nativeLoad[lib.Tsqlite3_vfs](vfsMemory)
	if vfs.FxSleep == 0 {
		return 0
	}
	sleep := *(*func(*libc.TLS, uintptr, int32) int32)(unsafe.Pointer(&vfs.FxSleep))
	sleep(tls, vfsMemory, int32(left/time.Microsecond))
	if libc.AtomicLoadNUint32(cell, nativeSeqCst) != 0 || time.Since(beginBusyEpoch) >= deadline {
		return 0
	}
	return 1
}
