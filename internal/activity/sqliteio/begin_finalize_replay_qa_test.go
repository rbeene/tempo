//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	lib "modernc.org/sqlite/lib"
)

// The external writer executes a real BEGIN IMMEDIATE. No fault substitutes
// either the failed sqlite3_step or its sqlite3_finalize return value.
func TestSQLiteIOFailedBeginFinalizeReplayIsNotCleanup(t *testing.T) {
	for _, retained := range []bool{false, true} {
		name := "sole-failed-begin"
		if retained {
			name = "other-native-statement-retains-cleanup-veto"
		}
		t.Run(name, func(t *testing.T) {
			dir := qaDirectory(t)
			qaInitialize(t, dir, qaBasename)
			release := bfrQAWriter(t, dir, qaBasename)
			var acquired, released, begins atomic.Int32
			qaFSHooks(t, hooks{Observe: func(e event) {
				if e.Role == "root" && e.Op == "lease" {
					if e.Phase == "acquired" {
						acquired.Add(1)
					}
					if e.Phase == "released" {
						released.Add(1)
					}
				}
			}})
			ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
			defer cancel()
			deadline := time.Now().Add(250 * time.Millisecond)
			c := bfrQAOpen(t, ctx, dir, deadline)
			var raw uintptr
			if retained {
				var err error
				raw, err = c.prepareRaw("SELECT 1", PreparePhase)
				// Own the raw statement before any assertion can terminate this test.
				t.Cleanup(func() {
					if raw != 0 {
						if rc := lib.Xsqlite3_finalize(c.tls, raw); rc != lib.SQLITE_OK {
							t.Error("raw QA statement cleanup failed", rc)
						}
						raw = 0
					}
				})
				if err != nil || raw == 0 {
					t.Fatal("real additional statement prerequisite", err)
				}
			}
			trace := &qaSQLTrace{}
			qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
				trace.observe(e)
				if e.Phase == "control-before-native" && e.Operation == "begin" {
					begins.Add(1)
				}
			}})
			start := time.Now()
			tx, err := c.Begin(ctx, Write)
			if tx != nil {
				// Even an unexpected success retains a transaction owner before Fatal.
				rollback := tx.Rollback()
				t.Fatal("held native writer unexpectedly admitted transaction", rollback)
			}
			var checked *Error
			if !errors.As(err, &checked) || checked.Phase != BeginPhase || begins.Load() != 1 || (checked.Code != lib.SQLITE_BUSY && checked.Code != lib.SQLITE_INTERRUPT) || (checked.Category != Busy && checked.Category != Canceled) || (checked.Code == lib.SQLITE_INTERRUPT && checked.Category != Canceled) {
				t.Fatal("fixture did not reach actual busy/interrupt BEGIN", err, begins.Load())
			}
			if ctx.Err() != nil || !c.acquireDeadline.Equal(deadline) {
				t.Fatal("outer hook budget expired or original admission deadline changed")
			}
			if checked.Category == Canceled && !errors.Is(checked.Cause, context.DeadlineExceeded) {
				t.Error("local admission cancellation lost its deadline cause")
			}
			t.Logf("actual BEGIN primary phase=%s category=%s code=%d local_deadline=%t elapsed_ns=%d", checked.Phase, checked.Category, checked.Code, errors.Is(checked.Cause, context.DeadlineExceeded), time.Since(start).Nanoseconds())
			if retained {
				var cleanup *Error
				if !errors.As(checked.Cleanup, &cleanup) || cleanup.Phase != FinalizePhase || cleanup.Code != checked.Code {
					t.Error("remaining native statement incorrectly erased conservative cleanup veto")
				}
			} else if checked.Cleanup != nil {
				// Deliberately nonfatal: old-source RED must continue through actual
				// native state, watcher release, close, child join and successful retry.
				t.Error("destroyed BEGIN statement replayed its execution failure as cleanup", checked.Cleanup)
			}
			qaSafeError(t, err, dir, qaBasename, "BEGIN IMMEDIATE", "SELECT 1")
			if lib.Xsqlite3_get_autocommit(c.tls, c.db) == 0 || lib.Xsqlite3_next_stmt(c.tls, c.db, 0) != raw || c.used || c.poisoned || progress(nil, c.cancelFlag) != 0 || nativeLoad[uint32](c.authMode) != authApplication {
				t.Fatal("failed BEGIN retained transaction, statement, watcher flag or authorization")
			}
			if stats := statsForTest(); stats.Active != 1 || stats.NativeActive != 1 || acquired.Load() != 1 || released.Load() != 0 {
				t.Fatal("failed BEGIN lost or duplicated native/root ownership", stats)
			}
			if raw != 0 {
				terminal, closeErr := c.CloseChecked(ctx)
				var closeFailure *Error
				if terminal || !errors.As(closeErr, &closeFailure) || closeFailure.Phase != ClosePhase || closeFailure.Category != Busy || closeFailure.Code != lib.SQLITE_BUSY || c.db == 0 || !c.nativeCounted || released.Load() != 0 {
					t.Fatal("actual retained-statement close BUSY lost the native owner", terminal, closeErr)
				}
				if rc := lib.Xsqlite3_finalize(c.tls, raw); rc != lib.SQLITE_OK {
					t.Fatal("real QA statement finalization", rc)
				}
				raw = 0
			}
			bfrQAClose(t, c)
			if released.Load() != 1 {
				t.Error("failed attempt did not release exactly one root")
			}
			release() // Existing once-owned helper joins before a fresh attempt.
			if ctx.Err() != nil {
				t.Fatal("original caller no longer live for fresh retry")
			}
			setSQLHooksForTest(sqlTestHooks{})
			retry := bfrQAOpen(t, ctx, dir, time.Now().Add(250*time.Millisecond))
			retryTx, err := retry.Begin(ctx, Write)
			if retryTx != nil {
				t.Cleanup(func() { _ = retryTx.Rollback() })
			}
			if err != nil || retryTx == nil {
				t.Fatal("fresh native retry after independent writer joined", err)
			}
			qaDone(t, retryTx, "INSERT INTO qa_items VALUES(1,'single-retry-effect')")
			outcome, err := retryTx.Commit()
			if outcome != Committed || err != nil {
				t.Fatal("fresh retry failed to commit", outcome, err)
			}
			bfrQAClose(t, retry)
			if acquired.Load() != 2 || released.Load() != 2 {
				t.Error("fresh retry root lifetime count")
			}
			setHooksForTest(hooks{})
			if n := qaCount(t, dir, qaBasename); n != 1 {
				t.Error("failed admission or fresh retry produced wrong row count", n)
			}
			if stats := statsForTest(); stats.Active != 0 || stats.NativeActive != 0 || stats.Guards != 0 || stats.RejectedFDs != 0 || stats.Poisoned || stats.Fatal {
				t.Error("completed native attempts retained registry resources", stats)
			}
		})
	}
}

// The BEGIN-only exception must not erase actual statement finalization
// errors, nor a distinct existing finalize-after cleanup fault.
func TestSQLiteIOFailedBeginExceptionKeepsStatementFinalizeErrors(t *testing.T) {
	for _, kind := range []string{"actual-constraint-replay", "independent-finalize-fault"} {
		t.Run(kind, func(t *testing.T) {
			dir := qaDirectory(t)
			qaInitialize(t, dir, qaBasename)
			ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
			defer cancel()
			c := bfrQAOpen(t, ctx, dir, time.Now().Add(250*time.Millisecond))
			tx := qaBegin(t, c, ctx, Write)
			var stmt *Stmt
			var wantCode int32
			if kind == "actual-constraint-replay" {
				qaDone(t, tx, "INSERT INTO qa_items VALUES(1,'uncommitted')")
				stmt = qaPrepare(t, tx, "INSERT INTO qa_items VALUES(1,'duplicate')")
				row, err := stmt.Step()
				var step *Error
				if row || !errors.As(err, &step) || step.Phase != StepPhase || step.Category != Constraint || step.Code&255 != lib.SQLITE_CONSTRAINT {
					t.Fatal("actual failed statement prerequisite", err)
				}
				wantCode = step.Code
			} else {
				stmt = qaPrepare(t, tx, "SELECT 1")
				row, err := stmt.Step()
				if !row || err != nil {
					t.Fatal("actual statement prerequisite", err)
				}
				wantCode = 266
				qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
					if e.Phase == "finalize-after" && e.Operation == "statement" && e.Code == lib.SQLITE_OK {
						return &Error{Phase: FinalizePhase, Category: IO, Code: wantCode}
					}
					return nil
				}})
			}
			err := stmt.Close()
			var finalized *Error
			if !errors.As(err, &finalized) || finalized.Phase != FinalizePhase || finalized.Code != wantCode {
				t.Error("non-BEGIN finalization evidence erased", err)
			}
			if stmt.ptr != 0 || len(tx.statements) != 0 || lib.Xsqlite3_next_stmt(c.tls, c.db, 0) != 0 {
				t.Fatal("failed statement finalization retained a native statement")
			}
			setSQLHooksForTest(sqlTestHooks{})
			if err := stmt.Close(); err != nil {
				t.Error("already finalized statement redispatched", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal("checked rollback", err)
			}
			bfrQAClose(t, c)
			if count := qaCount(t, dir, qaBasename); count != 0 {
				t.Error("finalization control committed a domain effect", count)
			}
		})
	}
}

func bfrQAOpen(t *testing.T, ctx context.Context, dir string, deadline time.Time) *Conn {
	t.Helper()
	c, err := Open(ctx, dir, qaBasename, Options{AcquireDeadline: deadline})
	if c != nil {
		t.Cleanup(func() { bfrQAClose(t, c) })
	}
	if err != nil || c == nil {
		t.Fatal("real native open prerequisite", err)
	}
	return c
}

func bfrQAClose(t *testing.T, c *Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	terminal, err := c.CloseChecked(ctx)
	if err != nil || !terminal {
		t.Error("checked ordinary native close", terminal, err)
		return
	}
	if !c.closed || c.db != 0 || c.nativeCounted || c.tls != nil || c.authMode != 0 || c.cancelFlag != 0 {
		t.Error("terminal native close retained slot or callback memory")
	}
}
