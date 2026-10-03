//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"testing"
)

// These fixtures use the real adapter and real deferred references. They call
// Commit directly: successful DML admission alone cannot acknowledge the row.
const deferredQASchema = `
CREATE TABLE qa_df_parent(id INTEGER PRIMARY KEY, value TEXT NOT NULL) STRICT;
CREATE TABLE qa_df_child(id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, value TEXT NOT NULL,
 FOREIGN KEY(parent_id) REFERENCES qa_df_parent(id) DEFERRABLE INITIALLY DEFERRED) STRICT;
CREATE TABLE qa_df_ack(id INTEGER PRIMARY KEY, child_id INTEGER NOT NULL, payload TEXT NOT NULL,
 FOREIGN KEY(child_id) REFERENCES qa_df_child(id) DEFERRABLE INITIALLY DEFERRED) STRICT;
`
const deferredQABaseline = "owned baseline receipt"
const deferredQAProposed = "owned proposed receipt must not leak"

func deferredQASeed(t *testing.T) string {
	t.Helper()
	dir := qaDirectory(t)
	c := qaOpen(t, dir, qaBasename, true)
	tx := qaBegin(t, c, context.Background(), Write)
	if err := tx.InstallSchema(deferredQASchema); err != nil {
		t.Fatal("owned deferred schema installation failed", err)
	}
	qaDone(t, tx, "INSERT INTO qa_df_parent VALUES(?,?)", Integer(1), Text("owned baseline parent"))
	qaDone(t, tx, "INSERT INTO qa_df_child VALUES(?,?,?)", Integer(1), Integer(1), Text("owned baseline child"))
	qaDone(t, tx, "INSERT INTO qa_df_ack VALUES(?,?,?)", Integer(1), Integer(1), Text(deferredQABaseline))
	qaCommit(t, tx)
	qaClose(t, c)
	return dir
}

func deferredQACounts(t *testing.T, tx *Tx, parents, children, acknowledgements int64) {
	t.Helper()
	for _, tc := range []struct {
		sql  string
		want int64
	}{
		{"SELECT count(*) FROM qa_df_parent", parents},
		{"SELECT count(*) FROM qa_df_child", children},
		{"SELECT count(*) FROM qa_df_ack", acknowledgements},
	} {
		if got := installQAScalar(t, tx, tc.sql); got != tc.want {
			t.Fatalf("owned deferred fixture count=%d want=%d", got, tc.want)
		}
	}
}

func deferredQAAck(t *testing.T, tx *Tx, id int64, found bool, payload string) {
	t.Helper()
	s := qaPrepare(t, tx, "SELECT payload FROM qa_df_ack WHERE id=?", Integer(id))
	row, err := s.Step()
	if err != nil || row != found {
		_ = s.Close()
		t.Fatal("owned receipt selection differs", err)
	}
	if row {
		got, err := s.Text(0)
		if err != nil || got != payload {
			_ = s.Close()
			t.Fatal("owned receipt payload differs", err)
		}
		if row, err = s.Step(); err != nil || row {
			_ = s.Close()
			t.Fatal("receipt did not reach singleton DONE", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal("receipt checked Close failed", err)
	}
}

func deferredQAReopen(t *testing.T, dir string, proposed bool) {
	t.Helper()
	c := qaOpen(t, dir, qaBasename, false)
	tx := qaBegin(t, c, context.Background(), Read)
	want := int64(1)
	if proposed {
		want = 2
	}
	deferredQACounts(t, tx, want, want, want)
	deferredQAAck(t, tx, 1, true, deferredQABaseline)
	deferredQAAck(t, tx, 2, proposed, deferredQAProposed)
	// Actual FK targets/joins and exact bound payloads survive cold reopen.
	if got := installQAScalar(t, tx, "SELECT count(*) FROM qa_df_ack AS a JOIN qa_df_child AS c ON c.id=a.child_id JOIN qa_df_parent AS p ON p.id=c.parent_id"); got != want {
		t.Fatal("cold receipt lacks actual child/parent dependency")
	}
	qaCommit(t, tx)
	qaClose(t, c)
}

func TestSQLiteIODeferredMissingTargetFailsActualCommitWithoutPartialReceipt(t *testing.T) {
	dir := deferredQASeed(t)
	deferredQAReopen(t, dir, false)
	c := qaOpen(t, dir, qaBasename, false)
	tx := qaBegin(t, c, context.Background(), Write)
	// Both native INSERTs reach DONE/checked Close while parent99 is absent.
	qaDone(t, tx, "INSERT INTO qa_df_child VALUES(?,?,?)", Integer(2), Integer(99), Text("owned missing-parent child"))
	qaDone(t, tx, "INSERT INTO qa_df_ack VALUES(?,?,?)", Integer(2), Integer(2), Text(deferredQAProposed))
	deferredQACounts(t, tx, 1, 2, 2)
	deferredQAAck(t, tx, 2, true, deferredQAProposed)
	outcome, commitErr := tx.Commit()
	var checked *Error
	// Current adapter conservatively retains Unknown for a non-busy native
	// COMMIT failure, then owns automatic rollback and terminal cleanup.
	if outcome != Unknown || !errors.As(commitErr, &checked) || checked.Phase != CommitPhase || checked.Category != Constraint || checked.Code != 787 {
		t.Fatal("real deferred COMMIT did not preserve native FK refusal", commitErr)
	}
	if commitErr.Error() != "SQLite commit failure (constraint, code 787)" {
		t.Fatal("COMMIT refusal did not use fixed safe diagnostic")
	}
	qaSafeError(t, commitErr, dir, qaBasename, "qa_df_parent", "qa_df_child", "qa_df_ack", deferredQAProposed, "owned missing-parent child", "INSERT INTO", "FOREIGN KEY constraint failed")
	// sqlite3_finalize can retain the failed COMMIT step's constraint code.
	// Check that evidence rather than assuming terminal Rollback must be nil.
	if checked.Cleanup != nil {
		var cleanup *Error
		if !errors.As(checked.Cleanup, &cleanup) || cleanup.Phase != FinalizePhase || cleanup.Category != Constraint || cleanup.Code != 787 {
			t.Fatal("failed COMMIT finalization lost checked cleanup evidence")
		}
		qaSafeError(t, checked.Cleanup, dir, deferredQAProposed, "qa_df_child", "FOREIGN KEY constraint failed")
	}
	if cleanup := tx.Rollback(); cleanup != checked.Cleanup {
		t.Fatal("terminal rollback changed cached checked cleanup", cleanup)
	}
	if cached, err := tx.Commit(); cached != outcome || err != commitErr {
		t.Fatal("terminal COMMIT retried or rewrote original refusal")
	}
	stmt, err := tx.Prepare("SELECT payload FROM qa_df_ack WHERE id=?", Integer(2))
	var terminal *Error
	if stmt != nil || !errors.As(err, &terminal) || terminal.Category != Closed {
		if stmt != nil {
			_ = stmt.Close()
		}
		t.Fatal("failed COMMIT left transaction usable", err)
	}
	if next, err := c.Begin(context.Background(), Write); next != nil || !errors.Is(err, ErrClosed) {
		if next != nil {
			_ = next.Rollback()
		}
		t.Fatal("failed transaction's used connection was readmitted", err)
	}
	qaClose(t, c)
	deferredQAReopen(t, dir, false)
	// Recovery is a fresh connection/transaction, not repair of terminal Tx.
	c = qaOpen(t, dir, qaBasename, false)
	tx = qaBegin(t, c, context.Background(), Write)
	qaDone(t, tx, "INSERT INTO qa_df_parent VALUES(?,?)", Integer(99), Text("owned recovered parent"))
	qaDone(t, tx, "INSERT INTO qa_df_child VALUES(?,?,?)", Integer(2), Integer(99), Text("owned recovered child"))
	qaDone(t, tx, "INSERT INTO qa_df_ack VALUES(?,?,?)", Integer(2), Integer(2), Text(deferredQAProposed))
	qaCommit(t, tx)
	qaClose(t, c)
	deferredQAReopen(t, dir, true)
}

func TestSQLiteIODeferredChildAndReceiptBeforeParentCommitAndReopen(t *testing.T) {
	dir := deferredQASeed(t)
	c := qaOpen(t, dir, qaBasename, false)
	tx := qaBegin(t, c, context.Background(), Write)
	qaDone(t, tx, "INSERT INTO qa_df_child VALUES(?,?,?)", Integer(2), Integer(99), Text("owned deferred positive child"))
	qaDone(t, tx, "INSERT INTO qa_df_ack VALUES(?,?,?)", Integer(2), Integer(2), Text(deferredQAProposed))
	deferredQACounts(t, tx, 1, 2, 2)
	deferredQAAck(t, tx, 2, true, deferredQAProposed)
	qaDone(t, tx, "INSERT INTO qa_df_parent VALUES(?,?)", Integer(99), Text("owned deferred positive parent"))
	deferredQACounts(t, tx, 2, 2, 2)
	qaCommit(t, tx)
	if err := tx.Rollback(); err != nil {
		t.Fatal("successful terminal cleanup unexpectedly failed", err)
	}
	qaClose(t, c)
	deferredQAReopen(t, dir, true)
}
