//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestSQLiteSyncNowWALMaintenanceReclaimsPausedAndEmpty(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "paused-with-retained-capture", true: "enabled-empty"}[empty], func(t *testing.T) {
			q := mwQANew(t, empty)
			oldID := mwQAID(10)
			old, err, trace, _ := mwQARun(t, q.s, oldID)
			if err != nil || old.State != "complete" || old.RemainingCount != q.remaining || trace.checkpoints != 0 || mwQAWAL(t, q.f) >= mwQASoft {
				t.Fatal("SETUP below-soft public Now", err)
			}
			mwQAGrow(t, q.f)
			before, rows := mwQAAudit(t, q.f)
			size := mwQAWAL(t, q.f)
			replay, err, trace, _ := mwQARun(t, q.reopen(), oldID)
			if err != nil || !reflect.DeepEqual(replay, old) || trace.checkpoints != 0 || trace.guardAcquired != 0 || mwQAWAL(t, q.f) < size {
				t.Fatal("terminal replay changed fast path or checkpointed", err)
			}
			mwQANonceOnly(t, before, rows, q.f)
			before, rows = mwQAAudit(t, q.f)
			run, err, trace, elapsed := mwQARun(t, q.reopen(), mwQAID(11))
			if err != nil || run.State != "complete" || run.RequestID != mwQAID(11) || run.RemainingCount != q.remaining || run.AttemptedIDs == nil || len(run.AttemptedIDs) != 0 || run.ResolvedIDs == nil || len(run.ResolvedIDs) != 0 || run.BlockedIDs == nil || len(run.BlockedIDs) != 0 {
				t.Fatal("fresh paused/empty maintenance result", err)
			}
			if trace.checkpoints != 1 || trace.completed != 1 || trace.code != 0 || trace.commitsBefore != 1 || !trace.checkpointOwned || !trace.ordinaryClosed || !trace.nextBeginClosed || trace.guardAcquired != 1 || trace.guardReleased != 1 {
				t.Fatal("missing one guarded native checkpoint after clean Read COMMIT and before reserve")
			}
			t.Logf("WAL_MAINTENANCE operation_elapsed_ns=%d native_checkpoint_ns=%d", elapsed.Nanoseconds(), trace.nativeTime.Nanoseconds())
			mwQAMaintained(t, q.f, before, rows, run)
			before, rows = mwQAAudit(t, q.f)
			got, err := q.reopen().Ingest(context.Background(), q.event)
			if err != nil || !reflect.DeepEqual(got, q.receipt) {
				t.Fatal("retained capture exact replay after reclaim", err)
			}
			mwQANonceOnly(t, before, rows, q.f)
			before, rows = mwQAAudit(t, q.f)
			replay, err, trace, _ = mwQARun(t, q.reopen(), mwQAID(11))
			if err != nil || !reflect.DeepEqual(replay, run) || trace.checkpoints != 0 || trace.guardAcquired != 0 {
				t.Fatal("maintained Now historical replay", err)
			}
			mwQANonceOnly(t, before, rows, q.f)
		})
	}
}

func TestSQLiteSyncNowWALMaintenancePinnedReaderBusyThenExplicitRetry(t *testing.T) {
	q := mwQANew(t, false)
	mwQAGrow(t, q.f)
	release := mwQAReader(t, q.f)
	defer release()
	// Append after the child's actual snapshot so its reader end mark is old.
	if got, err := q.reopen().Ingest(context.Background(), q.event); err != nil || !reflect.DeepEqual(got, q.receipt) {
		t.Fatal("actual append while independent reader held", err)
	}
	before, rows := mwQAAudit(t, q.f)
	id := mwQAID(20)
	run, err, trace, elapsed := mwQARun(t, q.reopen(), id)
	var domain *Error
	if !reflect.DeepEqual(run, SyncRun{}) || !errors.As(err, &domain) || domain.Code != "state_busy" || !domain.Retryable || domain.Uncertain {
		t.Fatal("reader contention did not return zero/retryable busy", err)
	}
	if trace.checkpoints != 1 || trace.completed != 1 || trace.code&255 != 5 || trace.commitsBefore != 1 || !trace.checkpointOwned || !trace.ordinaryClosed || trace.guardReleased != 1 || trace.nativeTime >= 250*time.Millisecond || elapsed > time.Second {
		t.Fatal("real finite checkpoint BUSY/checked release witness missing")
	}
	if mwQAWAL(t, q.f) < mwQASoft {
		t.Fatal("held reader falsely reported reclaimed WAL")
	}
	after, gotRows := mwQAAudit(t, q.f)
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(rows, gotRows) {
		t.Fatal("BUSY wrote nonce, receipt or retained history")
	}
	// Both the SQL root and real sync guard must already be available, while
	// the independent reader still holds its old native snapshot.
	guard, guardErr := q.s.acquireSQLiteSyncLock(context.Background(), time.Now().Add(250*time.Millisecond))
	if guard != nil {
		t.Cleanup(func() {
			if err := guard.Close(); err != nil {
				t.Error("retained guard cleanup", err)
			}
		})
	}
	if guardErr != nil {
		t.Fatal("BUSY retained sync guard ownership", guardErr)
	}
	if err := guard.Verify(); err != nil {
		t.Fatal("released guard cannot verify", err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal("checked guard probe close", err)
	}
	release()
	retryContext, retryDiagnostic := withSQLiteSyncFailureDiagnostics(context.Background())
	run, err, trace, _ = mwQARunContext(t, retryContext, q.reopen(), id)
	if err != nil || run.State != "complete" || run.RequestID != id || run.RemainingCount != 1 || len(run.AttemptedIDs) != 0 || trace.checkpoints != 1 || trace.completed != 1 || trace.code != 0 {
		// The unchanged no-provider dependency cannot return after provider construction.
		t.Log(sqliteSyncFailureDiagnosticLog(retryDiagnostic, true, true))
		t.Fatal("explicit same-ID retry after actual reader release", err)
	}
	mwQAMaintained(t, q.f, before, rows, run)
	before, rows = mwQAAudit(t, q.f)
	if got, err := q.reopen().Ingest(context.Background(), q.event); err != nil || !reflect.DeepEqual(got, q.receipt) {
		t.Fatal("capture replay remained stuck after explicit reclaim", err)
	}
	mwQANonceOnly(t, before, rows, q.f)
}
