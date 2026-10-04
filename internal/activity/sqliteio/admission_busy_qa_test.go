//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"testing"
	"time"

	lib "modernc.org/sqlite/lib"
)

func TestSQLiteIOAdmissionBusyWaitQuantum(t *testing.T) {
	dir := qaDirectory(t)
	qaInitialize(t, dir, qaBasename)
	sleeps := abqObserveSleep(t)
	release := bfrQAWriter(t, dir, qaBasename)
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	deadline := time.Now().Add(250 * time.Millisecond)
	c := bfrQAOpen(t, ctx, dir, deadline)
	begins := 0
	qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
		if e.Phase == "control-before-native" && e.Operation == "begin" {
			begins++
			sleeps.record = true
		}
	}})
	start := time.Now()
	tx, err := c.Begin(ctx, Write)
	if tx != nil {
		rollback := tx.Rollback()
		t.Fatal("independent native writer did not block BEGIN", rollback)
	}
	var checked *Error
	if !errors.As(err, &checked) || checked.Phase != BeginPhase || begins != 1 || (checked.Code != lib.SQLITE_BUSY && checked.Code != lib.SQLITE_INTERRUPT) || (checked.Category != Busy && checked.Category != Canceled) {
		t.Fatal("fixture did not reach actual native busy/interrupt BEGIN", err, begins)
	}
	if ctx.Err() != nil || !c.acquireDeadline.Equal(deadline) {
		t.Fatal("original caller expired or admission deadline changed")
	}
	if checked.Category == Canceled && !errors.Is(checked.Cause, context.DeadlineExceeded) {
		t.Error("admission cancellation lost its original deadline")
	}
	if checked.Cleanup != nil {
		t.Error("failed finalized BEGIN invented cleanup uncertainty", checked.Cleanup)
	}
	t.Logf("actual BEGIN phase=%s category=%s code=%d elapsed_ns=%d caller_live=%t", checked.Phase, checked.Category, checked.Code, time.Since(start).Nanoseconds(), ctx.Err() == nil)
	abqCheckSleeps(t, sleeps, true)
	abqFailedBeginReleased(t, c)
	qaSafeError(t, err, dir, qaBasename, "BEGIN IMMEDIATE")
	release() // Checked normal child exit, including race child, inside 900ms.
	// The old millisecond timeout may finish just before the absolute deadline.
	// Consume only that original remainder before testing its expired reuse.
	if remaining := time.Until(deadline); remaining > 0 {
		timer := time.NewTimer(remaining)
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
		timer.Stop()
	}
	if ctx.Err() != nil {
		t.Fatal("original caller expired while joining the released writer")
	}
	before := sleeps.count
	again, againErr := c.Begin(ctx, Write)
	if again != nil {
		rollback := again.Rollback()
		t.Fatal("expired original admission accepted another transaction", rollback)
	}
	if againErr == nil || begins != 1 || sleeps.count != before || !c.acquireDeadline.Equal(deadline) {
		t.Error("expired admission restarted native BEGIN or its busy wait", againErr, begins, sleeps.count)
	}
	bfrQAClose(t, c)
	setSQLHooksForTest(sqlTestHooks{})
	sleeps.record = false
	if n := qaCount(t, dir, qaBasename); n != 0 {
		t.Error("failed native admission created an effect", n)
	}
	abqQuiet(t)
}

func TestSQLiteIOAdmissionBusyEarlyRelease(t *testing.T) {
	dir := qaDirectory(t)
	qaInitialize(t, dir, qaBasename)
	sleeps := abqObserveSleep(t)
	release := bfrQAWriter(t, dir, qaBasename)
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	deadline := time.Now().Add(250 * time.Millisecond)
	c := bfrQAOpen(t, ctx, dir, deadline)
	begins := 0
	qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
		if e.Phase == "control-before-native" && e.Operation == "begin" {
			begins++
			sleeps.record = true
		}
	}})
	join := abqCoordinate(t, sleeps.first, release)
	tx, err := c.Begin(ctx, Write)
	returned := time.Now()
	if tx != nil {
		t.Cleanup(func() {
			if !tx.terminal {
				if err := tx.Rollback(); err != nil {
					t.Error("owned admission transaction cleanup", err)
				}
			}
		})
	}
	acted := join()
	release()
	if !acted || err != nil || tx == nil || begins != 1 {
		t.Fatal("real released writer did not admit the single native BEGIN", acted, err, begins)
	}
	if ctx.Err() != nil || !returned.Before(deadline) || !c.acquireDeadline.Equal(deadline) {
		t.Fatal("early release changed or exceeded the original budgets")
	}
	abqCheckSleeps(t, sleeps, true)
	if lib.Xsqlite3_get_autocommit(c.tls, c.db) != 0 || lib.Xsqlite3_next_stmt(c.tls, c.db, 0) != 0 || !c.used || c.poisoned || nativeLoad[uint32](c.authMode) != authApplication {
		t.Fatal("successful admission left invalid native ownership")
	}
	abqDisarmed(t, c)
	before := sleeps.count
	qaDone(t, tx, "INSERT INTO qa_items VALUES(1,'native-early-release')")
	qaCommit(t, tx)
	abqDisarmed(t, c)
	if sleeps.count != before || begins != 1 || progress(nil, c.cancelFlag) != 0 || tx.stopInterrupt != nil || !tx.terminal || lib.Xsqlite3_next_stmt(c.tls, c.db, 0) != 0 {
		t.Error("post-BEGIN work reused admission waiting or retained native ownership")
	}
	bfrQAClose(t, c)
	setSQLHooksForTest(sqlTestHooks{})
	sleeps.record = false
	if n := qaCount(t, dir, qaBasename); n != 1 {
		t.Error("released single BEGIN did not commit exactly one row", n)
	}
	abqQuiet(t)
}

func TestSQLiteIOAdmissionBusyCallerCancellation(t *testing.T) {
	dir := qaDirectory(t)
	qaInitialize(t, dir, qaBasename)
	sleeps := abqObserveSleep(t)
	release := bfrQAWriter(t, dir, qaBasename)
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	deadline := time.Now().Add(250 * time.Millisecond)
	c := bfrQAOpen(t, ctx, dir, deadline)
	begins := 0
	qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
		if e.Phase == "control-before-native" && e.Operation == "begin" {
			begins++
			sleeps.record = true
		}
	}})
	join := abqCoordinate(t, sleeps.first, cancel)
	tx, err := c.Begin(ctx, Write)
	if tx != nil {
		rollback := tx.Rollback()
		t.Fatal("canceled held-writer admission returned transaction", rollback)
	}
	acted := join()
	release()
	var checked *Error
	if !acted || !errors.Is(ctx.Err(), context.Canceled) || !errors.As(err, &checked) || checked.Phase != BeginPhase || checked.Category != Canceled || !errors.Is(checked.Cause, context.Canceled) || begins != 1 || (checked.Code != lib.SQLITE_BUSY && checked.Code != lib.SQLITE_INTERRUPT) {
		t.Fatal("original caller cancellation did not stop actual native BEGIN", acted, err, begins)
	}
	if checked.Cleanup != nil || !c.acquireDeadline.Equal(deadline) {
		t.Error("caller cancellation invented cleanup or replaced admission deadline", checked.Cleanup)
	}
	abqCheckSleeps(t, sleeps, true)
	abqFailedBeginReleased(t, c)
	qaSafeError(t, err, dir, qaBasename, "BEGIN IMMEDIATE")
	before := sleeps.count
	again, againErr := c.Begin(ctx, Write)
	if again != nil {
		rollback := again.Rollback()
		t.Fatal("canceled caller readmitted transaction", rollback)
	}
	if againErr == nil || begins != 1 || sleeps.count != before {
		t.Error("canceled caller redispatched BEGIN", againErr, begins)
	}
	bfrQAClose(t, c)
	setSQLHooksForTest(sqlTestHooks{})
	sleeps.record = false
	if n := qaCount(t, dir, qaBasename); n != 0 {
		t.Error("canceled native admission committed an effect", n)
	}
	abqQuiet(t)
}

func abqFailedBeginReleased(t *testing.T, c *Conn) {
	t.Helper()
	if lib.Xsqlite3_get_autocommit(c.tls, c.db) == 0 || lib.Xsqlite3_next_stmt(c.tls, c.db, 0) != 0 || c.used || c.poisoned || progress(nil, c.cancelFlag) != 0 || nativeLoad[uint32](c.authMode) != authApplication {
		t.Fatal("failed BEGIN retained transaction, statement, watcher flag or authorization")
	}
	if s := statsForTest(); s.Active != 1 || s.Entries != 1 || s.NativeActive != 1 || s.Poisoned || s.Fatal {
		t.Fatal("failed BEGIN lost its single checked-close owner", s)
	}
	abqDisarmed(t, c)
}
