//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"sync"
	"testing"
	"time"
	"unsafe"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

// This observer replaces only the process-private VFS sleep entry while no
// native owner exists. Every call still reaches the original real sleep with
// exactly the received TLS, VFS and microseconds and returns its exact result.
// Neither the SQLite busy callback nor its timeout state is replaced by QA.
type abqSleepTrace struct {
	forward  func(*libc.TLS, uintptr, int32) int32
	first    chan struct{}
	record   bool
	count    int
	requests [512]int32
	results  [512]int32
	overflow bool
	wrongVFS bool
}

var abqActiveSleep *abqSleepTrace
var abqSleepValue = abqForwardSleep

func abqForwardSleep(tls *libc.TLS, vfs uintptr, micros int32) int32 {
	q := abqActiveSleep
	index := -1
	if q.record {
		index = q.count
		q.count++
		if index < len(q.requests) {
			q.requests[index] = micros
		} else {
			q.overflow = true
		}
		q.wrongVFS = q.wrongVFS || vfs != vfsMemory
		if index == 0 {
			close(q.first)
		}
	}
	result := q.forward(tls, vfs, micros)
	if index >= 0 && index < len(q.results) {
		q.results[index] = result
	}
	return result
}

func abqObserveSleep(t *testing.T) *abqSleepTrace {
	t.Helper()
	abqQuiet(t)
	if err := initialize(); err != nil || vfsMemory == 0 || abqActiveSleep != nil {
		t.Fatal("private native VFS observer prerequisite", err)
	}
	original := nativeLoad[lib.Tsqlite3_vfs](vfsMemory)
	if original.FxSleep == 0 {
		t.Fatal("private native VFS has no real sleep primitive")
	}
	q := &abqSleepTrace{
		forward: *(*func(*libc.TLS, uintptr, int32) int32)(unsafe.Pointer(&original.FxSleep)),
		first:   make(chan struct{}),
	}
	abqActiveSleep = q
	observed := original
	observed.FxSleep = functionPointer(abqSleepValue)
	storeNativeVFS(vfsMemory, observed)
	// Registered before child/Conn/Tx/coordinator ownership. Their LIFO cleanup
	// must join first, even after a failed prerequisite or nonfatal old-code RED.
	t.Cleanup(func() {
		if s := statsForTest(); s.Active != 0 || s.NativeActive != 0 {
			t.Error("cannot restore VFS while a native/root owner remains", s)
			return
		}
		current := nativeLoad[lib.Tsqlite3_vfs](vfsMemory)
		if abqActiveSleep != q || current.FxSleep != observed.FxSleep {
			t.Error("private sleep observer changed before owned restoration")
			return
		}
		current.FxSleep = original.FxSleep
		storeNativeVFS(vfsMemory, current)
		abqActiveSleep = nil
		if nativeLoad[lib.Tsqlite3_vfs](vfsMemory).FxSleep != original.FxSleep {
			t.Error("original private VFS sleep pointer was not restored")
		}
		abqQuiet(t)
	})
	return q
}

func abqCheckSleeps(t *testing.T, q *abqSleepTrace, checkQuantum bool) {
	t.Helper()
	if q.count == 0 || q.overflow || q.wrongVFS {
		t.Fatal("actual bounded native sleep witness prerequisite", q.count, q.overflow, q.wrongVFS)
	}
	var max int32
	for i := 0; i < q.count; i++ {
		if q.requests[i] <= 0 || q.results[i] != q.requests[i] {
			t.Error("real native sleep request/result changed", i, q.requests[i], q.results[i])
		}
		if q.requests[i] > max {
			max = q.requests[i]
		}
	}
	t.Logf("native sleep calls=%d max_requested_us=%d", q.count, max)
	if checkQuantum && max > 1000 {
		// The old default busy handler must reach this behavioral RED. Do not
		// abort: statement/watcher/root/child cleanup checks below must execute.
		t.Error("BEGIN admission requested a native wait longer than 1000us", max)
	}
}

// The action runs only after the first actual native wait, following a fixed
// 30ms real timer. Cleanup stops a not-yet-dispatched action and always joins.
// The writer's existing 6s process context bounds its release-and-Wait action.
func abqCoordinate(t *testing.T, first <-chan struct{}, action func()) func() bool {
	t.Helper()
	stop, done := make(chan struct{}), make(chan struct{})
	acted := false
	go func() {
		defer close(done)
		select {
		case <-stop:
			return
		case <-first:
		}
		timer := time.NewTimer(30 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-stop:
			return
		case <-timer.C:
			action()
			acted = true
		}
	}()
	var once sync.Once
	join := func() bool {
		once.Do(func() { close(stop); <-done })
		return acted
	}
	t.Cleanup(func() { join() })
	return join
}

func abqQuiet(t *testing.T) {
	t.Helper()
	if s := statsForTest(); s.Active != 0 || s.Entries != 0 || s.NativeActive != 0 || s.Guards != 0 || s.SyncGuards != 0 || s.SyncEntries != 0 || s.RejectedFDs != 0 || s.Poisoned || s.Fatal {
		t.Fatal("native/root registry is not quiescent", s)
	}
}

func abqDisarmed(t *testing.T, c *Conn) {
	t.Helper()
	// Numeric foreign-memory reads use the pinned generated ABI's field
	// offsets. No PRAGMA, setter, policy replacement or callback invocation.
	base := c.db + unsafe.Offsetof(lib.Tsqlite3{}.FbusyHandler)
	handler := nativeLoad[uintptr](base + unsafe.Offsetof(lib.TBusyHandler{}.FxBusyHandler))
	argument := nativeLoad[uintptr](base + unsafe.Offsetof(lib.TBusyHandler{}.FpBusyArg))
	timeout := nativeLoad[int32](c.db + unsafe.Offsetof(lib.Tsqlite3{}.FbusyTimeout))
	if handler != 0 || argument != 0 || timeout != 0 {
		// Nonfatal on the old refusal path: all native owners still close.
		t.Error("native admission busy policy remains armed after BEGIN returned", handler != 0, argument != 0, timeout)
	}
}
