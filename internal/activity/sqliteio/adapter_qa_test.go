//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

func TestSQLiteIOTypedValuesRetainBytesAndOwnCopies(t *testing.T) {
	dir := qaDirectory(t)
	c := qaOpen(t, dir, qaBasename, true)
	tx := qaBegin(t, c, context.Background(), Write)
	qaDone(t, tx, "CREATE TABLE qa_values(lo INTEGER,hi INTEGER,n TEXT,e TEXT,b BLOB,z BLOB,t TEXT,counter BLOB,arbitrary BLOB)")
	textValue := "雪\x00after-NUL🙂"
	counter := bytes.Repeat([]byte{0xff}, 8)
	arbitrary := []byte{0, 1, 0xfe, 0xff, 0, 2}
	s := qaPrepare(t, tx, "INSERT INTO qa_values VALUES(?,?,?,?,?,?,?,?,?)", Integer(math.MinInt64), Integer(math.MaxInt64), Null(), Text(""), Blob([]byte{}), Blob(nil), Text(textValue), Blob(counter), Blob(arbitrary))
	// Prepare's TRANSIENT binding snapshot must survive caller mutation.
	counter[0] = 0
	arbitrary[1] = 99
	if row, err := s.Step(); row || err != nil {
		t.Fatalf("insert row=%t error=%v", row, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	qaCommit(t, tx)
	qaClose(t, c)
	c = qaOpen(t, dir, qaBasename, false)
	tx = qaBegin(t, c, context.Background(), Read)
	s = qaPrepare(t, tx, "SELECT lo,hi,n,e,b,z,t,counter,arbitrary FROM qa_values")
	if s.ColumnCount() != 9 {
		t.Fatalf("column metadata before ROW: %d", s.ColumnCount())
	}
	if _, err := s.Kind(0); err == nil {
		t.Fatal("value access before ROW accepted")
	}
	if row, err := s.Step(); !row || err != nil {
		t.Fatalf("read row=%t error=%v", row, err)
	}
	for i, want := range []Kind{IntegerKind, IntegerKind, NullKind, TextKind, BlobKind, BlobKind, TextKind, BlobKind, BlobKind} {
		got, err := s.Kind(i)
		if err != nil || got != want {
			t.Fatalf("column %d kind=%v want=%v error=%v", i, got, want, err)
		}
	}
	if n, err := s.Int64(0); err != nil || n != math.MinInt64 {
		t.Fatalf("min int64 %d error=%v", n, err)
	}
	if n, err := s.Int64(1); err != nil || n != math.MaxInt64 {
		t.Fatalf("max int64 %d error=%v", n, err)
	}
	if null, err := s.IsNull(2); err != nil || !null {
		t.Fatalf("SQL NULL %t error=%v", null, err)
	}
	if _, err := s.Text(2); err == nil {
		t.Fatal("NULL coerced to empty TEXT")
	}
	if _, err := s.Int64(3); err == nil {
		t.Fatal("empty TEXT coerced to integer")
	}
	if v, err := s.Text(3); err != nil || v != "" {
		t.Fatalf("empty TEXT %q error=%v", v, err)
	}
	for _, i := range []int{4, 5} {
		b, err := s.Blob(i)
		if err != nil || b == nil || len(b) != 0 {
			t.Fatalf("empty BLOB column=%d nil=%t len=%d error=%v", i, b == nil, len(b), err)
		}
		if null, err := s.IsNull(i); err != nil || null {
			t.Fatalf("empty BLOB coerced to NULL: %v", err)
		}
	}
	if v, err := s.Text(6); err != nil || v != textValue {
		t.Fatalf("NUL/Unicode TEXT %q error=%v", v, err)
	}
	b, err := s.Blob(7)
	if err != nil || !bytes.Equal(b, bytes.Repeat([]byte{0xff}, 8)) {
		t.Fatalf("uint64 upper-half BLOB %x error=%v", b, err)
	}
	b[0] = 0
	bAgain, err := s.Blob(7)
	if err != nil || len(bAgain) != 8 || bAgain[0] != 0xff {
		t.Fatal("returned BLOB aliases SQLite storage")
	}
	owned, err := s.Blob(8)
	if err != nil || !bytes.Equal(owned, []byte{0, 1, 0xfe, 0xff, 0, 2}) {
		t.Fatalf("arbitrary BLOB %x error=%v", owned, err)
	}
	for _, i := range []int{-1, 9} {
		if _, err := s.Kind(i); err == nil {
			t.Fatalf("invalid column index %d accepted", i)
		}
	}
	if row, err := s.Step(); row || err != nil {
		t.Fatalf("DONE row=%t error=%v", row, err)
	}
	if _, err := s.Blob(8); err == nil {
		t.Fatal("value access after DONE accepted")
	}
	if s.ColumnCount() != 9 {
		t.Fatal("column metadata lost after DONE")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.ColumnCount() != 0 {
		t.Fatal("finalized statement retains columns")
	}
	if _, err = s.Step(); err == nil {
		t.Fatal("finalized statement stepped")
	}
	if err = s.Close(); err != nil {
		t.Fatalf("double finalize is not idempotent: %v", err)
	}
	if !bytes.Equal(owned, []byte{0, 1, 0xfe, 0xff, 0, 2}) {
		t.Fatal("owned column bytes invalidated after DONE/finalize")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
}

func TestSQLiteIOFixedSQLBindingAndControlAuthorizer(t *testing.T) {
	dir := qaDirectory(t)
	qaInitialize(t, dir, qaBasename)
	for _, mode := range []Mode{Read, Write} {
		c := qaOpen(t, dir, qaBasename, false)
		tx := qaBegin(t, c, context.Background(), mode)
		for _, sql := range []string{
			"/* leading comment */ COMMIT", "-- leading comment\nROLLBACK", "/*x*/ BEGIN", "/*x*/ SAVEPOINT nested",
			"/*x*/ RELEASE nested", "/*x*/ ROLLBACK TO nested", "/*x*/ PRAGMA foreign_keys=OFF",
			"/*x*/ PRAGMA synchronous", "/*x*/ ATTACH DATABASE ':memory:' AS other", "/*x*/ DETACH DATABASE main",
			"SELECT 1; SELECT 2", "SELECT 1\x00; COMMIT",
		} {
			s, err := tx.Prepare(sql)
			if s != nil {
				_ = s.Close()
			}
			if err == nil {
				t.Fatalf("application control/multiple SQL accepted in mode %v: %q", mode, sql)
			}
		}
		for _, args := range [][]Value{nil, {Integer(1), Integer(2)}} {
			s, err := tx.Prepare("SELECT ?", args...)
			if s != nil {
				_ = s.Close()
			}
			if err == nil {
				t.Fatal("wrong parameter count accepted")
			}
		}
		floating := qaPrepare(t, tx, "SELECT 1.5")
		if row, err := floating.Step(); !row || err != nil {
			t.Fatal("FLOAT fixture failed")
		}
		if _, err := floating.Kind(0); err == nil {
			t.Fatal("FLOAT implicitly admitted as a supported kind")
		}
		if err := floating.Close(); err != nil {
			t.Fatal(err)
		}
		if mode == Read {
			s, err := tx.Prepare("INSERT INTO qa_items VALUES(?,?)", Integer(1), Text("read-write violation"))
			if s != nil {
				_ = s.Close()
			}
			if err == nil {
				t.Fatal("read transaction admitted DML")
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		} else {
			payload := "value'); DROP TABLE qa_items; --\x00suffix"
			qaDone(t, tx, "INSERT INTO qa_items VALUES(?,?)", Integer(1), Text(payload))
			s := qaPrepare(t, tx, "SELECT value FROM qa_items WHERE id=?", Integer(1))
			if row, err := s.Step(); !row || err != nil {
				t.Fatal("bound row missing")
			}
			got, err := s.Text(0)
			if err != nil || got != payload {
				t.Fatalf("bound payload altered: %q error=%v", got, err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			qaCommit(t, tx)
		}
		qaClose(t, c)
	}
	if n := qaCount(t, dir, qaBasename); n != 1 {
		t.Fatalf("SQL binding or control refusal changed store count=%d", n)
	}
}

func TestSQLiteIOTransactionOwnerAndCachedTerminalEvidence(t *testing.T) {
	dir := qaDirectory(t)
	qaInitialize(t, dir, qaBasename)
	c := qaOpen(t, dir, qaBasename, false)
	tx := qaBegin(t, c, context.Background(), Write)
	qaDone(t, tx, "INSERT INTO qa_items VALUES(?,?)", Integer(1), Text("first"))
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	started := time.Now()
	other, err := c.Begin(ctx, Write)
	cancel()
	if other != nil {
		_ = other.Rollback()
	}
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 300*time.Millisecond {
		t.Fatalf("competing Begin entered owner or ignored context: %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 35*time.Millisecond)
	err = c.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close freed active transaction: %v", err)
	}
	qaDone(t, tx, "INSERT INTO qa_items VALUES(?,?)", Integer(2), Text("second"))
	trace := &qaSQLTrace{}
	qaSQLHooks(t, sqlTestHooks{Observe: trace.observe})
	live := qaPrepare(t, tx, "SELECT id FROM qa_items ORDER BY id")
	outcome, err := tx.Commit()
	if outcome != NotAttempted || err == nil || trace.count("commit-before-dispatch") != 0 {
		t.Fatalf("live statement COMMIT outcome=%v error=%v dispatch=%d", outcome, err, trace.count("commit-before-dispatch"))
	}
	if err = live.Close(); err != nil {
		t.Fatal(err)
	}
	qaCommit(t, tx)
	before := trace.count("commit-before-dispatch")
	for i := 0; i < 2; i++ {
		outcome, err = tx.Commit()
		if outcome != Committed || err != nil {
			t.Fatal("terminal committed evidence changed")
		}
		if err = tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
	if before != 1 || trace.count("commit-before-dispatch") != before || trace.count("rollback-before") != 0 {
		t.Fatal("terminal calls redispatched SQL")
	}
	if subsequent, err := c.Begin(context.Background(), Read); err == nil || subsequent != nil {
		if subsequent != nil {
			_ = subsequent.Rollback()
		}
		t.Fatal("single-operation connection reused")
	}
	qaClose(t, c)
	if n := qaCount(t, dir, qaBasename); n != 2 {
		t.Fatalf("committed transaction count=%d", n)
	}
}

func TestSQLiteIOConstraintRollbackAndActualCancellation(t *testing.T) {
	for _, which := range []string{"constraint", "cancel"} {
		t.Run(which, func(t *testing.T) {
			dir := qaDirectory(t)
			qaInitialize(t, dir, qaBasename)
			c := qaOpen(t, dir, qaBasename, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tx := qaBegin(t, c, ctx, Write)
			qaDone(t, tx, "INSERT INTO qa_items VALUES(?,?)", Integer(1), Text("must roll back"))
			var s *Stmt
			var cancellationTimer *time.Timer
			stepEntered := false
			stepCode := int32(0)
			if which == "constraint" {
				s = qaPrepare(t, tx, "INSERT INTO qa_items VALUES(?,?)", Integer(1), Text("duplicate"))
			} else {
				s = qaPrepare(t, tx, "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n")
				qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
					if e.Phase == "step-before" {
						stepEntered = true
						cancellationTimer = time.AfterFunc(30*time.Millisecond, cancel)
					}
					if e.Phase == "step-after" {
						stepCode = e.Code
					}
				}})
			}
			started := time.Now()
			row, err := s.Step()
			if cancellationTimer != nil {
				cancellationTimer.Stop()
			}
			if row || err == nil {
				t.Fatalf("failure fixture unexpectedly succeeded row=%t error=%v", row, err)
			}
			var checked *Error
			if !errors.As(err, &checked) {
				t.Fatalf("statement error not checked: %v", err)
			}
			if which == "constraint" && (checked.Category != Constraint || checked.Code&255 != 19) {
				t.Fatalf("duplicate fixture failed for unrelated reason: %v", err)
			}
			qaSafeError(t, err, dir, "qa_items", "INSERT INTO", "duplicate", "must roll back")
			if which == "cancel" && (!stepEntered || stepCode&255 != 9 || checked.Code&255 != 9 || !errors.Is(err, context.Canceled) || time.Since(started) > time.Second) {
				t.Fatalf("actual SQLite interruption not witnessed/bounded/context-aware: entered=%t native=%d error=%v", stepEntered, stepCode, err)
			}
			setSQLHooksForTest(sqlTestHooks{})
			// sqlite3_finalize may return the last step code; even then the handle
			// is freed and removed from the transaction's cleanup ledger.
			_ = s.Close()
			if err = s.Close(); err != nil {
				t.Fatalf("finalized failed statement retained stale error: %v", err)
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			outcome, err := tx.Commit()
			if outcome != NotCommitted || err == nil {
				t.Fatalf("rolled-back evidence outcome=%v error=%v", outcome, err)
			}
			qaClose(t, c)
			if n := qaCount(t, dir, qaBasename); n != 0 {
				t.Fatalf("preceding insert survived rollback count=%d", n)
			}
		})
	}
}

func TestSQLiteIOAutomaticEngineRollbackRefusesFurtherTransactionWork(t *testing.T) {
	dir := qaDirectory(t)
	qaInitialize(t, dir, qaBasename)
	c := qaOpen(t, dir, qaBasename, false)
	tx, err := c.Begin(context.Background(), Write)
	if err != nil {
		t.Fatalf("real BEGIN IMMEDIATE failed: %v", err)
	}
	// This fallback only protects an unexpected fixture failure. All behavioral
	// witnesses and explicit cleanup below occur before test cleanup runs.
	t.Cleanup(func() { _ = tx.Rollback() })
	if lib.Xsqlite3_get_autocommit(c.tls, c.db) != 0 {
		t.Fatal("successful BEGIN did not establish a native transaction")
	}
	qaDone(t, tx, "INSERT INTO qa_items VALUES(?,?)", Integer(1), Text("transient-first-row"))
	// A previously prepared statement must also be stopped when the engine
	// rolls back the transaction; guarding only future Prepare is insufficient.
	followup := qaPrepare(t, tx, "INSERT INTO qa_items VALUES(?,?)", Integer(2), Text("must-never-autocommit"))
	conflict := qaPrepare(t, tx, "INSERT OR ROLLBACK INTO qa_items VALUES(?,?)", Integer(1), Text("rollback-conflict"))
	trace := &qaSQLTrace{}
	qaSQLHooks(t, sqlTestHooks{Observe: trace.observe})
	row, err := conflict.Step()
	var checked *Error
	if row || !errors.As(err, &checked) || checked.Category != Constraint || checked.Code&255 != 19 || trace.countCode("step-after", 19)+trace.countCode("step-after", 1555) != 1 {
		t.Fatalf("actual rollback-conflict fixture failed row=%t error=%v", row, err)
	}
	qaSafeError(t, err, dir, "qa_items", "rollback-conflict")
	// This is a direct read of the owned engine connection, with no new DB FD.
	// It proves SQLite already ended the transaction before any Go cleanup.
	if lib.Xsqlite3_get_autocommit(c.tls, c.db) != 1 {
		t.Fatal("INSERT OR ROLLBACK did not perform actual engine rollback")
	}
	_ = conflict.Close() // Finalize may report the preceding constraint code.
	prepareBefore, stepBefore := trace.count("prepare-before"), trace.count("step-before")
	late, prepareErr := tx.Prepare("INSERT INTO qa_items VALUES(?,?)", Integer(3), Text("late-prepare-marker"))
	if prepareErr == nil || late != nil {
		t.Error("Go transaction admitted Prepare after actual SQLite rollback")
	}
	if prepareErr != nil {
		qaSafeError(t, prepareErr, dir, "qa_items", "late-prepare-marker")
	}
	if late != nil {
		_ = late.Close()
	}
	if trace.count("prepare-before") != prepareBefore {
		t.Error("stale transaction reached native Prepare dispatch")
	}
	row, stepErr := followup.Step()
	if row || stepErr == nil {
		t.Error("preprepared statement ran outside the rolled-back transaction")
	}
	if stepErr != nil {
		qaSafeError(t, stepErr, dir, "qa_items", "must-never-autocommit")
	}
	if trace.count("step-before") != stepBefore {
		t.Error("stale transaction reached native Step dispatch")
	}
	if lib.Xsqlite3_get_autocommit(c.tls, c.db) != 1 {
		t.Error("later API changed the ended engine transaction state")
	}
	_ = followup.Close()
	// Cleanup is explicit only after the automatic rollback and dispatch
	// witnesses. An incorrectly dispatched followup INSERT is autocommitted;
	// this Rollback or Close cannot erase it and hide the durable regression.
	if err = tx.Rollback(); err != nil {
		t.Fatalf("ended transaction cleanup unsafe: %v", err)
	}
	outcome, commitErr := tx.Commit()
	if outcome != NotCommitted || commitErr == nil {
		t.Errorf("automatic rollback outcome=%v error=%v", outcome, commitErr)
	}
	if trace.count("commit-before-dispatch") != 0 || trace.count("rollback-before") != 0 {
		t.Error("cleanup dispatched control SQL after confirmed automatic rollback")
	}
	if again, againErr := tx.Commit(); again != outcome || (againErr == nil) != (commitErr == nil) || (againErr != nil && againErr.Error() != commitErr.Error()) {
		t.Error("ended transaction terminal evidence changed")
	}
	if err = tx.Rollback(); err != nil {
		t.Errorf("repeated terminal cleanup changed: %v", err)
	}
	setSQLHooksForTest(sqlTestHooks{})
	qaClose(t, c)
	if n := qaCount(t, dir, qaBasename); n != 0 {
		t.Errorf("stale Go transaction durably autocommitted a later row: fresh-open count=%d", n)
	}
}

func TestSQLiteIOOrdinaryConstraintKeepsNativeTransactionActive(t *testing.T) {
	dir := qaDirectory(t)
	qaInitialize(t, dir, qaBasename)
	c := qaOpen(t, dir, qaBasename, false)
	tx := qaBegin(t, c, context.Background(), Write)
	qaDone(t, tx, "INSERT INTO qa_items VALUES(?,?)", Integer(1), Text("first-survives-ABORT"))
	conflict := qaPrepare(t, tx, "INSERT INTO qa_items VALUES(?,?)", Integer(1), Text("ordinary-ABORT-conflict"))
	row, err := conflict.Step()
	var checked *Error
	if row || !errors.As(err, &checked) || checked.Category != Constraint || checked.Code&255 != 19 {
		t.Fatalf("ordinary ABORT fixture row=%t error=%v", row, err)
	}
	if lib.Xsqlite3_get_autocommit(c.tls, c.db) != 0 {
		t.Fatal("ordinary ABORT unexpectedly ended the native transaction")
	}
	_ = conflict.Close()
	// Ordinary ABORT cancels only its statement. A fix must distinguish this
	// active transaction from INSERT OR ROLLBACK, rather than reject all errors.
	qaDone(t, tx, "INSERT INTO qa_items VALUES(?,?)", Integer(2), Text("second-after-ABORT"))
	if lib.Xsqlite3_get_autocommit(c.tls, c.db) != 0 {
		t.Fatal("valid continuation escaped the original native transaction")
	}
	qaCommit(t, tx)
	qaClose(t, c)
	if n := qaCount(t, dir, qaBasename); n != 2 {
		t.Fatalf("ordinary ABORT continuation committed count=%d want2", n)
	}
}

func TestSQLiteIOCommitFaultPhasesKeepDurableFacts(t *testing.T) {
	for _, tc := range []struct {
		phase   string
		outcome Outcome
		rows    int64
	}{
		{"commit-before-dispatch", NotCommitted, 0},
		{"commit-after-engine", Unknown, 1},
		{"commit-before-verify", Unknown, 1},
		{"commit-after-verify", Committed, 1},
	} {
		t.Run(tc.phase, func(t *testing.T) {
			dir := qaDirectory(t)
			qaInitialize(t, dir, qaBasename)
			c := qaOpen(t, dir, qaBasename, false)
			tx := qaBegin(t, c, context.Background(), Write)
			qaDone(t, tx, "INSERT INTO qa_items VALUES(?,?)", Integer(1), Text("phase fixture"))
			trace := &qaSQLTrace{}
			fired := false
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observe, Fault: func(e sqlTestEvent) error {
				if e.Phase == tc.phase && !fired {
					fired = true
					return &Error{Phase: CommitPhase, Category: IO, Code: 10}
				}
				return nil
			}})
			outcome, err := tx.Commit()
			if !fired || outcome != tc.outcome || err == nil {
				t.Fatalf("phase fired=%t outcome=%v want=%v error=%v", fired, outcome, tc.outcome, err)
			}
			if tc.outcome == NotCommitted && trace.countCode("rollback-after", 0) != 1 {
				t.Fatal("NotCommitted lacks conclusive rollback witness before Commit returns")
			}
			dispatches := trace.count("commit-before-dispatch")
			repeated, repeatedErr := tx.Commit()
			if repeated != outcome || repeatedErr == nil || repeatedErr.Error() != err.Error() || trace.count("commit-before-dispatch") != dispatches {
				t.Fatal("terminal failure/outcome redispatched or changed")
			}
			// A cleanup result can be non-nil. It must be stable and cannot alter
			// a previously established Unknown/Committed terminal outcome.
			cleanup := tx.Rollback()
			again := tx.Rollback()
			if (cleanup == nil) != (again == nil) || (cleanup != nil && cleanup.Error() != again.Error()) {
				t.Fatal("terminal cleanup evidence changed")
			}
			setSQLHooksForTest(sqlTestHooks{})
			qaClose(t, c)
			if n := qaCount(t, dir, qaBasename); n != tc.rows {
				t.Fatalf("fresh-open actual engine facts count=%d want=%d", n, tc.rows)
			}
		})
	}
}

func TestSQLiteIOFailedRollbackPreservesCleanupAndBlocksWork(t *testing.T) {
	dir := qaDirectory(t)
	qaInitialize(t, dir, qaBasename)
	c := qaOpen(t, dir, qaBasename, false)
	tx := qaBegin(t, c, context.Background(), Write)
	qaDone(t, tx, "INSERT INTO qa_items VALUES(1,'rollback-fault marker')")
	trace := &qaSQLTrace{}
	fired := false
	qaSQLHooks(t, sqlTestHooks{Observe: trace.observe, Fault: func(e sqlTestEvent) error {
		if e.Phase == "rollback-before" && !fired {
			fired = true
			return &Error{Phase: RollbackPhase, Category: IO, Code: 10}
		}
		return nil
	}})
	err := tx.Rollback()
	if !fired || err == nil {
		t.Fatal("inconclusive rollback was hidden")
	}
	qaSafeError(t, err, dir, "qa_items", "rollback-fault marker")
	before := trace.count("rollback-before")
	if again := tx.Rollback(); again == nil || again.Error() != err.Error() || trace.count("rollback-before") != before {
		t.Fatal("failed cleanup replay redispatched or forgot evidence")
	}
	if next, beginErr := c.Begin(context.Background(), Write); beginErr == nil || next != nil {
		if next != nil {
			_ = next.Rollback()
		}
		t.Fatal("inconclusive cleanup admitted new work")
	}
	setSQLHooksForTest(sqlTestHooks{})
	qaClose(t, c)
	// Checked native close owns final rollback; no statement handle or watcher
	// survives it. This assertion is after close deliberately, unlike the
	// terminal NotCommitted witness in the pre-dispatch commit test.
	if n := qaCount(t, dir, qaBasename); n != 0 {
		t.Fatal("checked close left uncommitted inserted rows")
	}
}

func TestSQLiteIOSameDirectoryBasenamesAndAliasAdmission(t *testing.T) {
	dir := qaDirectory(t)
	aName, bName := "A.sqlite3", "B.sqlite3"
	// Default macOS volumes may fold case: use genuinely distinct physical
	// filenames here; the pure naming test independently covers case inputs.
	a := qaOpen(t, dir, aName, true)
	ta := qaBegin(t, a, context.Background(), Write)
	qaDone(t, ta, "CREATE TABLE qa_items(id INTEGER PRIMARY KEY,value TEXT NOT NULL)")
	qaDone(t, ta, "INSERT INTO qa_items VALUES(1,'A')")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	alias, err := Open(ctx, filepath.Join(dir, "."), aName, Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	if alias != nil {
		_ = alias.Close(context.Background())
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled alias admitted: %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 35*time.Millisecond)
	alias, err = Open(ctx, dir+"/.", aName, Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	cancel()
	if alias != nil {
		_ = alias.Close(context.Background())
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same-store alias did not share registry gate: %v", err)
	}
	b := qaOpen(t, dir, bName, true)
	tb := qaBegin(t, b, context.Background(), Write)
	qaDone(t, tb, "CREATE TABLE qa_items(id INTEGER PRIMARY KEY,value TEXT NOT NULL)")
	qaDone(t, tb, "INSERT INTO qa_items VALUES(1,'B'),(2,'B')")
	qaCommit(t, tb)
	qaCommit(t, ta)
	// Before closing either owner, both exact sidecar sets must coexist.
	for _, base := range []string{aName, bName} {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			info, err := os.Lstat(filepath.Join(dir, base+suffix))
			if err != nil || !info.Mode().IsRegular() {
				t.Fatalf("separate basename role missing: base=%s role=%s error=%v", base, suffix, err)
			}
		}
	}
	qaClose(t, b)
	qaClose(t, a)
	if n := qaCount(t, dir, aName); n != 1 {
		t.Fatalf("A conflated count=%d", n)
	}
	if n := qaCount(t, dir, bName); n != 2 {
		t.Fatalf("B conflated count=%d", n)
	}
	// A sidecar-safe long leaf is legal; longer leaves fail before creation.
	longName := strings.Repeat("x", 220) + ".sqlite3"
	long := qaOpen(t, dir, longName, true)
	tl := qaBegin(t, long, context.Background(), Write)
	qaDone(t, tl, "CREATE TABLE qa_items(id INTEGER PRIMARY KEY,value TEXT NOT NULL)")
	qaCommit(t, tl)
	qaClose(t, long)
	for _, name := range []string{"", ".", "..", "a/b", "a\x00b", strings.Repeat("y", 256)} {
		mainOpens := 0
		qaFSHooks(t, hooks{Observe: func(e event) {
			if e.Role == "main" && e.Op == "open" && e.Phase == "before" {
				mainOpens++
			}
		}})
		before, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		c, err := Open(context.Background(), dir, name, Options{Create: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
		if c != nil {
			_ = c.Close(context.Background())
		}
		if err == nil {
			t.Fatalf("invalid leaf admitted %q", name)
		}
		after, readErr := os.ReadDir(dir)
		if readErr != nil || len(before) != len(after) || mainOpens != 0 {
			t.Fatal("invalid basename reached main open or changed directory")
		}
		for i := range before {
			if before[i].Name() != after[i].Name() {
				t.Fatal("invalid basename altered existing directory entries")
			}
		}
	}
}

func TestSQLiteIONamespaceReplacementAfterActualEngineCommitIsUnknown(t *testing.T) {
	base := qaDirectory(t)
	dir := filepath.Join(base, "store")
	moved := filepath.Join(base, "displaced")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	qaInitialize(t, dir, qaBasename)
	c := qaOpen(t, dir, qaBasename, false)
	tx := qaBegin(t, c, context.Background(), Write)
	qaDone(t, tx, "INSERT INTO qa_items VALUES(1,'actual engine commit')")
	var swapErr error
	swapped := false
	qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
		if e.Phase == "commit-before-verify" && !swapped {
			swapped = true
			swapErr = os.Rename(dir, moved)
			if swapErr == nil {
				swapErr = os.Mkdir(dir, 0700)
			}
		}
	}})
	outcome, err := tx.Commit()
	setSQLHooksForTest(sqlTestHooks{})
	// Restore the owned namespace for checked close and a fresh-open oracle.
	// No duplicate DB descriptor is opened or closed in the owning process.
	if swapErr == nil && swapped {
		swapErr = os.Remove(dir)
		if swapErr == nil {
			swapErr = os.Rename(moved, dir)
		}
	}
	if swapErr != nil || !swapped {
		t.Fatalf("post-engine namespace fixture not exercised: %v", swapErr)
	}
	if outcome != Unknown || err == nil {
		t.Fatalf("replaced authority acknowledged outcome=%v error=%v", outcome, err)
	}
	if cached, cachedErr := tx.Commit(); cached != Unknown || cachedErr == nil {
		t.Fatal("namespace Unknown changed on terminal replay")
	}
	qaClose(t, c)
	if n := qaCount(t, dir, qaBasename); n != 1 {
		t.Fatalf("actual COMMIT evidence lost on fresh-open count=%d", n)
	}
}

func TestSQLiteIOStatementCloseBusyAndAbsentReadonlyLifetime(t *testing.T) {
	dir := qaDirectory(t)
	validatedOpens := 0
	qaFSHooks(t, hooks{Observe: func(e event) {
		if e.Role == "main" && e.Op == "open" && e.Phase == "validated" {
			validatedOpens++
		}
	}})
	c, err := Open(context.Background(), dir, qaBasename, Options{ReadOnly: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	if c != nil {
		_ = c.Close(context.Background())
	}
	if err == nil {
		t.Fatal("absent read-only database opened")
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil || len(entries) != 0 || validatedOpens != 0 {
		t.Fatal("absent read-only open created/validated storage")
	}
	setHooksForTest(hooks{})
	c = qaOpen(t, dir, qaBasename, true)
	release, err := c.holdStatementForTest("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	released := false
	t.Cleanup(func() {
		if !released {
			_ = release()
		}
	})
	err = c.Close(context.Background())
	var checked *Error
	if !errors.As(err, &checked) || checked.Code&255 != 5 {
		t.Fatalf("live native statement close did not retain BUSY: %v", err)
	}
	stats := statsForTest()
	if stats.Active != 1 || stats.Entries != 1 {
		t.Fatalf("inconclusive close released ownership: %+v", stats)
	}
	if err = release(); err != nil {
		t.Fatal(err)
	}
	released = true
	qaClose(t, c)
	stats = statsForTest()
	if stats.Active != 0 || stats.Entries != 0 || stats.RejectedFDs != 0 || stats.Poisoned || stats.Fatal {
		t.Fatalf("conclusive close retained registry/FD state: %+v", stats)
	}
}

func TestSQLiteIOInvalidAcquisitionOptionsNeverOpenMain(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options Options
	}{
		{"zero-deadline", Options{Create: true}},
		{"expired-deadline", Options{Create: true, AcquireDeadline: time.Now().Add(-time.Second)}},
		{"create-readonly", Options{Create: true, ReadOnly: true, AcquireDeadline: time.Now().Add(time.Second)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := qaDirectory(t)
			mainOpens := 0
			qaFSHooks(t, hooks{Observe: func(e event) {
				if e.Role == "main" && e.Op == "open" && e.Phase == "before" {
					mainOpens++
				}
			}})
			c, err := Open(context.Background(), dir, qaBasename, tc.options)
			if c != nil {
				_ = c.Close(context.Background())
			}
			if err == nil {
				t.Fatal("invalid acquisition options admitted")
			}
			qaSafeError(t, err, dir, qaBasename)
			entries, readErr := os.ReadDir(dir)
			if mainOpens != 0 || readErr != nil || len(entries) != 0 {
				t.Fatal("invalid options opened or created database storage")
			}
		})
	}
}

func TestSQLiteIORegistryAndWriterShareOneAcquisitionDeadline(t *testing.T) {
	dir := qaDirectory(t)
	qaInitialize(t, dir, qaBasename)
	releaseWriter := qaWriter(t, dir, qaBasename)
	blocker := qaOpen(t, dir, qaBasename, false)
	var wg sync.WaitGroup
	wg.Add(1)
	releaseErr := make(chan error, 1)
	var started time.Time
	var leaseElapsed time.Duration
	qaFSHooks(t, hooks{Observe: func(e event) {
		if e.Role == "root" && e.Op == "lease" && e.Phase == "acquired" {
			leaseElapsed = time.Since(started)
		}
	}})
	go func() {
		defer wg.Done()
		timer := time.NewTimer(150 * time.Millisecond)
		defer timer.Stop()
		<-timer.C
		releaseErr <- blocker.Close(context.Background())
	}()
	started = time.Now()
	deadline := started.Add(250 * time.Millisecond)
	c, err := Open(context.Background(), dir, qaBasename, Options{AcquireDeadline: deadline})
	wg.Wait()
	if closeErr := <-releaseErr; closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		if c != nil {
			_ = c.Close(context.Background())
		}
		t.Fatalf("registry admission failed before held-writer stage: %v", err)
	}
	beginNativeCalls := 0
	qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
		if e.Phase == "control-before-native" && e.Operation == "begin" {
			beginNativeCalls++
		}
	}})
	tx, err := c.Begin(context.Background(), Write)
	elapsed := time.Since(started)
	if tx != nil {
		_ = tx.Rollback()
	}
	qaClose(t, c)
	releaseWriter()
	if err == nil {
		t.Fatal("actual external writer did not block BEGIN IMMEDIATE")
	}
	// A restarted 250ms BEGIN wait after 150ms registry admission takes about
	// 400ms. Leave 100ms scheduler tolerance while detecting that restart.
	if leaseElapsed < 125*time.Millisecond || leaseElapsed > 225*time.Millisecond || elapsed < 200*time.Millisecond || elapsed > 350*time.Millisecond {
		t.Fatalf("shared acquisition budget lease=%v total=%v", leaseElapsed, elapsed)
	}
	var safe *Error
	if !errors.As(err, &safe) {
		t.Fatalf("actual busy BEGIN result is not a checked error: %v", err)
	}
	primary := safe.Code & 255
	// The absolute deadline can expire in the bounded busy handler (BUSY) or
	// the joined admission watcher (INTERRUPT). Both require a real held-writer
	// native BEGIN, and cancellation must retain its detectable deadline cause.
	if beginNativeCalls != 1 || (primary != 5 && primary != 9) || (safe.Category != Busy && safe.Category != Canceled) || (primary == 9 && safe.Category != Canceled) || (safe.Category == Canceled && !errors.Is(err, context.DeadlineExceeded)) {
		t.Fatalf("actual busy BEGIN lacks native dispatch/deadline evidence: native=%d error=%v", beginNativeCalls, err)
	}
	setSQLHooksForTest(sqlTestHooks{})
	setHooksForTest(hooks{})
	if n := qaCount(t, dir, qaBasename); n != 0 {
		t.Fatal("contention dispatched a domain write")
	}
}

func TestSQLiteIOCancellationBeforeNativeEntryCannotLoseInterrupt(t *testing.T) {
	dir := qaDirectory(t)
	qaInitialize(t, dir, qaBasename)
	c := qaOpen(t, dir, qaBasename, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx := qaBegin(t, c, ctx, Write)
	qaDone(t, tx, "INSERT INTO qa_items VALUES(1,'uncommitted-before-cancellation')")
	s := qaPrepare(t, tx, "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n")
	trace := &qaSQLTrace{}
	interrupted := make(chan struct{})
	var interruptOnce sync.Once
	var fallbackRan atomic.Bool
	fallbackDone := make(chan struct{})
	var fallback *time.Timer
	boundaryReached, interruptCompleted := false, false
	qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
		trace.observe(e)
		if e.Phase == "interrupt-after" {
			interruptOnce.Do(func() { close(interrupted) })
		}
		if e.Phase != "step-before-native" || boundaryReached {
			return
		}
		boundaryReached = true
		// The original cancellation watcher must finish its real interrupt
		// while no sqlite3_step is running. This exposes the no-op interrupt
		// window after the wrapper's final context check.
		cancel()
		wait := time.NewTimer(200 * time.Millisecond)
		select {
		case <-interrupted:
			interruptCompleted = true
		case <-wait.C:
		}
		wait.Stop()
		// A broken implementation must fail here without leaving a billion-row
		// recursive query for an infrastructure timeout to kill. This separate
		// TLS interrupt is cleanup only: its use is an explicit test failure,
		// and it is joined before any statement/connection memory is freed.
		fallback = time.AfterFunc(300*time.Millisecond, func() {
			defer close(fallbackDone)
			fallbackRan.Store(true)
			tls := libc.NewTLS()
			lib.Xsqlite3_interrupt(tls, c.db)
			tls.Close()
		})
	}})
	started := time.Now()
	row, err := s.Step()
	elapsed := time.Since(started)
	if fallback != nil && !fallback.Stop() {
		<-fallbackDone
	}
	if !boundaryReached || !interruptCompleted {
		t.Error("native-entry fixture did not witness the original interrupt completing before release")
	}
	if fallbackRan.Load() {
		t.Error("cancellation interrupt was lost before native entry; owned fallback had to interrupt the live query")
	}
	var checked *Error
	if row || !errors.As(err, &checked) || !errors.Is(err, context.Canceled) || checked.Category != Canceled {
		t.Errorf("canceled native entry row=%t error=%v", row, err)
	} else {
		// A context check may safely refuse entry. If native stepping did occur,
		// the engine must return actual SQLITE_INTERRUPT, not merely wrap a
		// successful query's result with a cancellation error.
		nativeCalls := trace.count("step-after")
		if !((nativeCalls == 0 && checked.Code == 0) || (nativeCalls == 1 && trace.countCode("step-after", 9) == 1 && checked.Code&255 == 9)) {
			t.Errorf("cancellation lacked safe refusal or native interrupt: native=%d code=%d", nativeCalls, checked.Code)
		}
		qaSafeError(t, err, dir, "qa_items", "uncommitted-before-cancellation")
	}
	if elapsed > 250*time.Millisecond {
		t.Errorf("canceled native entry did not complete promptly: %v", elapsed)
	}
	setSQLHooksForTest(sqlTestHooks{})
	_ = s.Close() // Finalize can repeat an actual interrupted step's code.
	if err = tx.Rollback(); err != nil {
		t.Fatalf("canceled transaction cleanup: %v", err)
	}
	outcome, commitErr := tx.Commit()
	if outcome != NotCommitted || commitErr == nil {
		t.Errorf("canceled transaction outcome=%v error=%v", outcome, commitErr)
	}
	if again, againErr := tx.Commit(); again != outcome || (againErr == nil) != (commitErr == nil) || (againErr != nil && againErr.Error() != commitErr.Error()) {
		t.Error("canceled transaction terminal evidence changed")
	}
	qaClose(t, c)
	if n := qaCount(t, dir, qaBasename); n != 0 {
		t.Errorf("prior uncommitted row survived canceled operation: count=%d", n)
	}
}

func TestSQLiteIOCommitKeepsEarlierAndLaterCheckedCleanupEvidence(t *testing.T) {
	dir := qaDirectory(t)
	qaInitialize(t, dir, qaBasename)
	c := qaOpen(t, dir, qaBasename, false)
	tx := qaBegin(t, c, context.Background(), Write)
	qaDone(t, tx, "INSERT INTO qa_items VALUES(1,'cleanup-preservation-marker')")
	earlier := &Error{Phase: FinalizePhase, Category: IO, Code: 266}
	later := &Error{Phase: RollbackPhase, Category: IO, Code: 778}
	trace := &qaSQLTrace{}
	beforeFired, afterFired := false, false
	qaSQLHooks(t, sqlTestHooks{Observe: trace.observe, Fault: func(e sqlTestEvent) error {
		if e.Phase == "commit-before-dispatch" && !beforeFired {
			beforeFired = true
			return &Error{Phase: CommitPhase, Category: IO, Code: 10, Cleanup: earlier}
		}
		if e.Phase == "rollback-after" && !afterFired {
			afterFired = true
			return later
		}
		return nil
	}})
	outcome, err := tx.Commit()
	if !beforeFired || !afterFired || trace.countCode("rollback-after", 0) != 1 || outcome != NotCommitted || err == nil {
		t.Fatalf("checked cleanup fixture before=%t after=%t rollbackOK=%d outcome=%v error=%v", beforeFired, afterFired, trace.countCode("rollback-after", 0), outcome, err)
	}
	// Traverse the public error unwrap contract, allowing either nesting or
	// joined errors. The assertion concerns retained checked evidence, not a
	// private field layout or one particular implementation of aggregation.
	var evidence func(error) (bool, bool, bool)
	evidence = func(current error) (bool, bool, bool) {
		if current == nil {
			return false, false, false
		}
		first, second, root := false, false, false
		if checked, ok := current.(*Error); ok {
			first = checked.Phase == FinalizePhase && checked.Category == IO && checked.Code == 266
			second = checked.Phase == RollbackPhase && checked.Category == IO && checked.Code == 778
			root = checked.Phase == CommitPhase && checked.Category == IO && checked.Code == 10
		}
		for _, marker := range []string{dir, "qa_items", "cleanup-preservation-marker", "INSERT INTO"} {
			if strings.Contains(current.Error(), marker) {
				t.Errorf("public recursive cleanup error disclosed synthetic marker %q", marker)
			}
		}
		var children []error
		switch wrapped := current.(type) {
		case interface{ Unwrap() []error }:
			children = wrapped.Unwrap()
		case interface{ Unwrap() error }:
			children = []error{wrapped.Unwrap()}
		}
		for _, child := range children {
			a, b, r := evidence(child)
			first, second, root = first || a, second || b, root || r
		}
		return first, second, root
	}
	first, second, root := evidence(err)
	if !first || !second || !root {
		t.Errorf("terminal error discarded checked evidence: earlier=%t later=%t primary=%t", first, second, root)
	}
	before := trace.count("commit-before-dispatch")
	rollbackBefore := trace.count("rollback-before")
	if again, againErr := tx.Commit(); again != NotCommitted || againErr == nil {
		t.Error("cleanup failure terminal outcome changed")
	} else if a, b, r := evidence(againErr); !a || !b || !r {
		t.Error("cached Commit discarded earlier or later checked cleanup")
	}
	cleanup := tx.Rollback()
	if cleanup == nil {
		t.Error("terminal Rollback forgot the checked cleanup failure")
	} else if _, retained, _ := evidence(cleanup); !retained {
		t.Error("terminal Rollback discarded its checked rollback cleanup code/phase")
	}
	if again := tx.Rollback(); again == nil || (cleanup != nil && again.Error() != cleanup.Error()) {
		t.Error("cached terminal cleanup changed")
	}
	if trace.count("commit-before-dispatch") != before || trace.count("rollback-before") != rollbackBefore {
		t.Error("terminal evidence replay redispatched control SQL")
	}
	setSQLHooksForTest(sqlTestHooks{})
	qaClose(t, c)
	if n := qaCount(t, dir, qaBasename); n != 0 {
		t.Errorf("checked rollback did not remove staged row: count=%d", n)
	}
}
