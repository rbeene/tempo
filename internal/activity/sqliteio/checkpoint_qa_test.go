//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
	"unsafe"

	lib "modernc.org/sqlite/lib"
)

func checkpointQAOnce(t *testing.T, dir string, mode CheckpointMode) (CheckpointResult, error) {
	t.Helper()
	c := capacityQAOpen(t, dir, false, 0)
	capacityQACleanRead(t, c)
	r, err := c.Checkpoint(capacityQAContext(t), mode)
	capacityQAApplicationAuth(t, c)
	qaClose(t, c)
	return r, err
}

func TestSQLiteCheckpointPinnedReaderPartialBusyAndExplicitRecovery(t *testing.T) {
	dir := qaDirectory(t)
	capacityQAInitialize(t, dir, 0)
	release := capacityQAReader(t, dir)
	for id := int64(2); id <= 9; id++ {
		capacityQAAppend(t, dir, id, 32<<10)
	}
	first, err := checkpointQAOnce(t, dir, Passive)
	if err != nil || !first.Attempted || first.Code != lib.SQLITE_OK || !first.BeforeValid || !first.AfterValid {
		t.Fatalf("real reader passive result=%+v error=%v", first, err)
	}
	if first.LogFrames <= 0 || first.CheckpointedFrames <= 0 || first.CheckpointedFrames >= first.LogFrames || first.After.WAL == 0 {
		t.Fatalf("held snapshot did not yield actual partial backfill: %+v", first)
	}
	second, err := checkpointQAOnce(t, dir, Passive)
	if err != nil || second.LogFrames != first.LogFrames || second.CheckpointedFrames != first.CheckpointedFrames {
		t.Fatalf("unchanged pinned WAL lost cumulative frame semantics: first=%+v second=%+v error=%v", first, second, err)
	}
	// The second call copied no newly eligible frames, but pnCkpt remains the
	// positive cumulative count. A wrapper returning per-call work fails here.
	busy, err := checkpointQAOnce(t, dir, Truncate)
	var checked *Error
	if !busy.Attempted || busy.Code&255 != lib.SQLITE_BUSY || !errors.As(err, &checked) || checked.Category != Busy || !busy.AfterValid || busy.After.WAL == 0 {
		t.Fatalf("held-reader truncate was not a finite native BUSY: %+v error=%v", busy, err)
	}
	if busy.LogFrames != first.LogFrames || busy.CheckpointedFrames != first.CheckpointedFrames {
		t.Fatalf("BUSY lost available frame outputs: %+v", busy)
	}
	release()
	clean, err := checkpointQAOnce(t, dir, Truncate)
	if err != nil || !clean.Attempted || clean.Code != lib.SQLITE_OK || !clean.AfterValid || clean.After.WAL != 0 || clean.LogFrames != 0 || clean.CheckpointedFrames != 0 {
		t.Fatalf("released-reader explicit truncate=%+v error=%v", clean, err)
	}
	if n := capacityQACount(t, dir); n != 9 {
		t.Fatalf("maintenance changed retained records: count=%d", n)
	}
	// The reclaimed/zero-WAL case must work again on a new one-operation Conn.
	again, err := checkpointQAOnce(t, dir, Passive)
	if err != nil || !again.Attempted || !again.AfterValid || again.After.WAL != 0 {
		t.Fatalf("reclaimed WAL passive result=%+v error=%v", again, err)
	}
}

func TestSQLiteCheckpointUnusedAndCleanReadAreOneShot(t *testing.T) {
	for _, readFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "unused", true: "clean-terminal-read"}[readFirst], func(t *testing.T) {
			dir := qaDirectory(t)
			capacityQAInitialize(t, dir, 0)
			c := capacityQAOpen(t, dir, false, 0)
			if readFirst {
				capacityQACleanRead(t, c)
			}
			trace := &qaSQLTrace{}
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observe})
			r, err := c.Checkpoint(capacityQAContext(t), Passive)
			if err != nil || !r.Attempted || trace.count("checkpoint-after-engine") != 1 {
				t.Fatalf("eligible single checkpoint=%+v error=%v", r, err)
			}
			capacityQAApplicationAuth(t, c)
			again, err := c.Checkpoint(capacityQAContext(t), Truncate)
			if err == nil || again.Attempted || trace.count("checkpoint-after-engine") != 1 {
				t.Fatal("checkpoint eligibility was reused")
			}
			if tx, err := c.Begin(capacityQAContext(t), Read); err == nil || tx != nil {
				if tx != nil {
					_ = tx.Rollback()
				}
				t.Fatal("administrative checkpoint enabled a second transaction")
			}
			qaClose(t, c)
		})
	}
}

func TestSQLiteCheckpointRejectsWriteRollbackAndUnknownRead(t *testing.T) {
	for _, scenario := range []string{"write-commit", "write-rollback", "read-rollback", "unknown-read"} {
		t.Run(scenario, func(t *testing.T) {
			dir := qaDirectory(t)
			capacityQAInitialize(t, dir, 0)
			c := capacityQAOpen(t, dir, false, 0)
			mode := Read
			if scenario == "write-commit" || scenario == "write-rollback" {
				mode = Write
			}
			tx := qaBegin(t, c, capacityQAContext(t), mode)
			_ = capacityQAScalar(t, tx, "SELECT count(*) FROM capacity_items")
			switch scenario {
			case "write-commit":
				qaCommit(t, tx)
			case "write-rollback", "read-rollback":
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			case "unknown-read":
				qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
					if e.Phase == "commit-after-engine" {
						return &Error{Phase: VerifyPhase, Category: IO}
					}
					return nil
				}})
				outcome, err := tx.Commit()
				setSQLHooksForTest(sqlTestHooks{})
				if outcome != Unknown || err == nil {
					t.Fatalf("read fixture did not produce terminal Unknown: %v %v", outcome, err)
				}
			}
			trace := &qaSQLTrace{}
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observe})
			r, err := c.Checkpoint(capacityQAContext(t), Passive)
			if err == nil || r.Attempted || trace.count("checkpoint-after-engine") != 0 {
				t.Fatalf("ineligible terminal %s checkpoint=%+v error=%v", scenario, r, err)
			}
			capacityQAApplicationAuth(t, c)
			qaClose(t, c)
		})
	}
}

func TestSQLiteCheckpointFailedReadCannotBecomeCleanByCommit(t *testing.T) {
	for _, scenario := range []string{"native-step", "bind-arity", "typed-getter", "foreign-key-audit"} {
		t.Run(scenario, func(t *testing.T) {
			dir := qaDirectory(t)
			capacityQAInitialize(t, dir, 0)
			if scenario == "foreign-key-audit" {
				// Seed an actual on-disk FK violation using a private fixed
				// fixture setup; the next production Open restores FK enforcement.
				seed := capacityQAOpen(t, dir, false, 0)
				if err := seed.lockContext(capacityQAContext(t), seed.acquireDeadline); err != nil {
					t.Fatal(err)
				}
				_, err := seed.control(capacityQAContext(t), "PRAGMA foreign_keys=OFF", authPragma, OpenPhase)
				seed.unlock()
				if err != nil {
					t.Fatal(err)
				}
				seedTx := qaBegin(t, seed, capacityQAContext(t), Write)
				qaDone(t, seedTx, "CREATE TABLE capacity_parent(id INTEGER PRIMARY KEY) STRICT")
				qaDone(t, seedTx, "CREATE TABLE capacity_child(id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES capacity_parent(id)) STRICT")
				qaDone(t, seedTx, "INSERT INTO capacity_child(id,parent_id) VALUES(1,999)")
				qaCommit(t, seedTx)
				qaClose(t, seed)
			}
			c := capacityQAOpen(t, dir, false, 0)
			tx := qaBegin(t, c, capacityQAContext(t), Read)
			switch scenario {
			case "native-step":
				s := qaPrepare(t, tx, "SELECT abs(-9223372036854775808)")
				row, err := s.Step()
				var checked *Error
				if row || !errors.As(err, &checked) || checked.Code&255 != lib.SQLITE_ERROR {
					_ = s.Close()
					t.Fatalf("fixture lacked actual SQLite integer-overflow read error: %v", err)
				}
				_ = s.Close() // Finalize repeats the actual failed native step.
			case "bind-arity":
				s, err := tx.Prepare("SELECT ?")
				if s != nil {
					_ = s.Close()
				}
				if err == nil {
					t.Fatal("fixture accepted missing binding")
				}
			case "typed-getter":
				s := qaPrepare(t, tx, "SELECT body FROM capacity_items WHERE id=1")
				if row, err := s.Step(); !row || err != nil {
					_ = s.Close()
					t.Fatal("typed getter fixture has no BLOB row")
				}
				if _, err := s.Int64(0); err == nil {
					_ = s.Close()
					t.Fatal("typed getter silently coerced BLOB")
				}
				if row, err := s.Step(); row || err != nil {
					_ = s.Close()
					t.Fatal("getter fixture did not finish normally")
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			case "foreign-key-audit":
				err := tx.CheckForeignKeys()
				var checked *Error
				if !errors.As(err, &checked) || checked.Category != Corrupt {
					t.Fatalf("fixed read FK audit did not report real violation: %v", err)
				}
			}
			// The failure does not manufacture a commit error. It only prevents
			// this transaction from proving a clean administrative read.
			qaCommit(t, tx)
			trace := &qaSQLTrace{}
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observe})
			r, err := c.Checkpoint(capacityQAContext(t), Passive)
			if err == nil || r.Attempted || trace.count("checkpoint-after-engine") != 0 {
				t.Fatalf("failed %s read became checkpoint-eligible: %+v %v", scenario, r, err)
			}
			qaClose(t, c)
		})
	}
}

func TestSQLiteCheckpointActiveStatementCannotEscapeReadOwner(t *testing.T) {
	dir := qaDirectory(t)
	capacityQAInitialize(t, dir, 0)
	c := capacityQAOpen(t, dir, false, 0)
	tx := qaBegin(t, c, capacityQAContext(t), Read)
	s := qaPrepare(t, tx, "SELECT body FROM capacity_items")
	trace := &qaSQLTrace{}
	qaSQLHooks(t, sqlTestHooks{Observe: trace.observe})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	r, err := c.Checkpoint(ctx, Passive)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || r.Attempted || trace.count("checkpoint-after-engine") != 0 {
		t.Fatalf("checkpoint escaped active read ownership: %+v %v", r, err)
	}
	if row, err := s.Step(); !row || err != nil {
		t.Fatal("waiting checkpoint damaged original cursor")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	qaCommit(t, tx)
	r, err = c.Checkpoint(capacityQAContext(t), Passive)
	if err != nil || !r.Attempted || trace.count("checkpoint-after-engine") != 1 {
		t.Fatalf("clean owner could not explicitly checkpoint after release: %+v %v", r, err)
	}
	qaClose(t, c)
}

func TestSQLiteCheckpointCancellationAtNativeEntryHasRealEvidence(t *testing.T) {
	dir := qaDirectory(t)
	capacityQAInitialize(t, dir, 0)
	capacityQAAppend(t, dir, 2, 1<<20)
	c := capacityQAOpen(t, dir, false, 0)
	capacityQACleanRead(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	interrupted := make(chan struct{})
	var once sync.Once
	trace := &qaSQLTrace{}
	boundary, joinedInterrupt, pragmaScoped := false, false, false
	qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
		trace.observe(e)
		if e.Phase == "interrupt-after" {
			once.Do(func() { close(interrupted) })
		}
		if e.Phase != "checkpoint-before-native" || boundary {
			return
		}
		boundary = true
		pragmaScoped = c.authMode != 0 && *(*uint32)(unsafe.Pointer(c.authMode)) == authPragma
		cancel()
		wait := time.NewTimer(200 * time.Millisecond)
		select {
		case <-interrupted:
			joinedInterrupt = true
		case <-wait.C:
		}
		wait.Stop()
	}})
	r, err := c.Checkpoint(ctx, Passive)
	if !boundary || !joinedInterrupt || !pragmaScoped {
		t.Errorf("controlled checkpoint seam incomplete: boundary=%t interrupt=%t auth=%t", boundary, joinedInterrupt, pragmaScoped)
	}
	var checked *Error
	if !errors.As(err, &checked) || !errors.Is(err, context.Canceled) || checked.Category != Canceled {
		t.Errorf("canceled checkpoint result=%+v error=%v", r, err)
	}
	nativeCalls := trace.count("checkpoint-after-engine")
	if nativeCalls != 1 || !r.Attempted || r.Code&255 != lib.SQLITE_INTERRUPT || trace.countCode("checkpoint-after-engine", lib.SQLITE_INTERRUPT) != 1 {
		t.Errorf("lost idle interrupt or fabricated cancellation: native=%d result=%+v", nativeCalls, r)
	}
	// This deliberately tests idle-entry cancellation, not a claim that a
	// particular page had already copied. The payload is finite if broken.
	setSQLHooksForTest(sqlTestHooks{})
	capacityQAApplicationAuth(t, c)
	qaClose(t, c)
	if n := capacityQACount(t, dir); n != 2 {
		t.Fatal("canceled physical maintenance lost prior acknowledgments")
	}
}

func TestSQLiteCheckpointExpiredAdmissionAndCanceledContextDoNotDispatch(t *testing.T) {
	for _, scenario := range []string{"canceled", "expired-acquisition"} {
		t.Run(scenario, func(t *testing.T) {
			dir := qaDirectory(t)
			capacityQAInitialize(t, dir, 0)
			c := capacityQAOpen(t, dir, false, 0)
			ctx := capacityQAContext(t)
			if scenario == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else {
				// Expire the same stored deadline; do not provide a replacement
				// budget to the checkpoint or depend on a scheduling-sensitive sleep.
				c.acquireDeadline = time.Now().Add(-time.Millisecond)
			}
			trace := &qaSQLTrace{}
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observe})
			r, err := c.Checkpoint(ctx, Passive)
			if err == nil || r.Attempted || trace.count("checkpoint-after-engine") != 0 {
				t.Fatalf("expired operation dispatched: %+v error=%v", r, err)
			}
			capacityQAApplicationAuth(t, c)
			qaClose(t, c)
		})
	}
}

func TestSQLiteCheckpointZeroWALAndInvalidModeRestoreAuthorizer(t *testing.T) {
	t.Run("fresh-zero-wal", func(t *testing.T) {
		dir := qaDirectory(t)
		c := capacityQAOpen(t, dir, true, 0)
		// No application transaction: exercise the exact native empty-WAL path
		// if the pinned engine needs its internal PRAGMA to initialize pWal.
		r, err := c.Checkpoint(capacityQAContext(t), Passive)
		if err != nil || !r.Attempted || r.Code != lib.SQLITE_OK || !r.BeforeValid || !r.AfterValid {
			t.Fatalf("zero-WAL native checkpoint=%+v error=%v", r, err)
		}
		capacityQAApplicationAuth(t, c)
		qaClose(t, c)
	})
	t.Run("readonly-refuses", func(t *testing.T) {
		dir := qaDirectory(t)
		capacityQAInitialize(t, dir, 0)
		c, err := Open(capacityQAContext(t), dir, capacityQAName, Options{ReadOnly: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
		if c != nil {
			t.Cleanup(func() { _ = c.Close(context.Background()) })
		}
		if err != nil {
			t.Fatal(err)
		}
		trace := &qaSQLTrace{}
		qaSQLHooks(t, sqlTestHooks{Observe: trace.observe})
		r, err := c.Checkpoint(capacityQAContext(t), Passive)
		if err == nil || r.Attempted || trace.count("checkpoint-after-engine") != 0 {
			t.Fatalf("readonly connection performed physical maintenance: %+v %v", r, err)
		}
		capacityQAApplicationAuth(t, c)
		qaClose(t, c)
	})
	t.Run("invalid-mode", func(t *testing.T) {
		dir := qaDirectory(t)
		capacityQAInitialize(t, dir, 0)
		c := capacityQAOpen(t, dir, false, 0)
		trace := &qaSQLTrace{}
		qaSQLHooks(t, sqlTestHooks{Observe: trace.observe})
		r, err := c.Checkpoint(capacityQAContext(t), CheckpointMode(255))
		var checked *Error
		if !errors.As(err, &checked) || checked.Category != Invalid || r.Attempted || trace.count("checkpoint-after-engine") != 0 {
			t.Fatalf("invalid mode reached native checkpoint: %+v error=%v", r, err)
		}
		capacityQAApplicationAuth(t, c)
		qaClose(t, c)
	})
}
