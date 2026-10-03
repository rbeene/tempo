//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

// Independent source-first QA for installer plan 7a3dac18077e7a14f18b51a9c48dbe8e3c38fd072a3a7afd2da0626f9f455ffa.
// The API is new: no compile RED or native acceptance is claimed by this source.

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func installQAScalar(t *testing.T, tx *Tx, sql string) int64 {
	t.Helper()
	s := qaPrepare(t, tx, sql)
	row, err := s.Step()
	if !row || err != nil {
		_ = s.Close()
		t.Fatalf("installer scalar row=%t error=%v", row, err)
	}
	n, err := s.Int64(0)
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	if row, err = s.Step(); row || err != nil {
		_ = s.Close()
		t.Fatal("scalar returned extra rows or failed")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	return n
}

func installQARefuseControls(t *testing.T, tx *Tx) {
	t.Helper()
	for _, sql := range []string{"/* install must not enable */ COMMIT", "/*x*/ ROLLBACK", "/*x*/ SAVEPOINT nested", "/*x*/ PRAGMA foreign_keys=OFF", "/*x*/ ATTACH DATABASE ':memory:' AS other", "/*x*/ DETACH DATABASE main"} {
		s, err := tx.Prepare(sql)
		if s != nil {
			_ = s.Close()
		}
		if err == nil {
			t.Fatalf("application control admitted after installation: %q", sql)
		}
	}
}

func TestSQLiteInstallNativeTailsTriggerBodiesAndPermanentAuthorizer(t *testing.T) {
	dir := qaDirectory(t)
	c := qaOpen(t, dir, qaBasename, true)
	tx := qaBegin(t, c, context.Background(), Write)
	installQARefuseControls(t, tx)
	trace := &qaSQLTrace{}
	qaSQLHooks(t, sqlTestHooks{Observe: trace.observe})
	const ddl = `-- leading comment; is not a statement
CREATE TABLE qa_items(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE qa_audit(value TEXT NOT NULL);
CREATE TRIGGER qa_insert AFTER INSERT ON qa_items BEGIN
  INSERT INTO qa_audit VALUES('first;inside--literal');
  INSERT INTO qa_audit VALUES(NEW.value);
END;
/* trailing comment; with another semicolon; */ -- comment-only tail
`
	if err := tx.InstallSchema(ddl); err != nil {
		t.Fatal(err)
	}
	if trace.count("prepare-before") == 0 || trace.count("step-before") == 0 {
		t.Fatal("positive installer did not expose owned native prepare/step witnesses")
	}
	if trace.count("commit-before-dispatch") != 0 {
		t.Fatal("installer committed its caller's transaction")
	}
	qaDone(t, tx, "INSERT INTO qa_items VALUES(?,?)", Integer(1), Text("second;literal"))
	if n := installQAScalar(t, tx, "SELECT count(*) FROM qa_audit WHERE value IN ('first;inside--literal','second;literal')"); n != 2 {
		t.Fatalf("trigger body or quoted semicolon truncated: count=%d", n)
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	installQARefuseControls(t, tx)
	qaCommit(t, tx)
	setSQLHooksForTest(sqlTestHooks{})
	qaClose(t, c)
	if n := qaCount(t, dir, qaBasename); n != 1 {
		t.Fatalf("caller commit lost installed rows: %d", n)
	}
}

func TestSQLiteInstallRejectsInvalidInputsBeforeNativePreparation(t *testing.T) {
	dir := qaDirectory(t)
	c := qaOpen(t, dir, qaBasename, true)
	tx := qaBegin(t, c, context.Background(), Write)
	trace := &qaSQLTrace{}
	qaSQLHooks(t, sqlTestHooks{Observe: trace.observe})
	for _, script := range []string{"", "CREATE TABLE forbidden(v TEXT);\x00", strings.Repeat(" ", (1<<20)+1)} {
		before, steps := trace.count("prepare-before"), trace.count("step-before")
		err := tx.InstallSchema(script)
		if err == nil {
			t.Fatal("empty/NUL/oversized installer source admitted")
		}
		qaSafeError(t, err, dir, "forbidden", "CREATE TABLE")
		if trace.count("prepare-before") != before || trace.count("step-before") != steps {
			t.Fatal("invalid installer source reached native SQL preparation/step")
		}
	}
	const prefix = "CREATE TABLE qa_items(id INTEGER PRIMARY KEY,value TEXT NOT NULL);"
	if err := tx.InstallSchema(prefix + strings.Repeat(" ", (1<<20)-len(prefix))); err != nil {
		t.Fatalf("exact 1MiB source boundary rejected: %v", err)
	}
	if trace.count("prepare-before") == 0 {
		t.Fatal("valid boundary source never witnessed native prepare")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	setSQLHooksForTest(sqlTestHooks{})
	qaClose(t, c)
}

func TestSQLiteInstallReadLiveStatementParametersAndRowsRefuse(t *testing.T) {
	for _, which := range []string{"read", "live", "parameters", "rows", "pragma", "transaction"} {
		t.Run(which, func(t *testing.T) {
			dir := qaDirectory(t)
			qaInitialize(t, dir, qaBasename)
			c := qaOpen(t, dir, qaBasename, false)
			mode := Write
			if which == "read" {
				mode = Read
			}
			tx := qaBegin(t, c, context.Background(), mode)
			var live *Stmt
			if which == "live" {
				live = qaPrepare(t, tx, "SELECT 1")
			}
			script := "CREATE TABLE forbidden(v TEXT);"
			switch which {
			case "parameters":
				script = "INSERT INTO qa_items(id,value) VALUES(?, 'parameter-marker');"
			case "rows":
				script = "SELECT 1;"
			case "pragma":
				script = "/*x*/ PRAGMA foreign_keys=OFF;"
			case "transaction":
				script = "/*x*/ COMMIT;"
			}
			err := tx.InstallSchema(script)
			if err == nil {
				t.Fatalf("installer admitted %s contract violation", which)
			}
			qaSafeError(t, err, dir, "forbidden", "parameter-marker")
			if live != nil {
				if err := live.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			qaClose(t, c)
			if n := qaCount(t, dir, qaBasename); n != 0 {
				t.Fatal("refused installer altered existing data")
			}
		})
	}
}

func TestSQLiteForeignKeyAuditFindsRealDeferredViolationAndRestoresAuthorizer(t *testing.T) {
	dir := qaDirectory(t)
	c := qaOpen(t, dir, qaBasename, true)
	tx := qaBegin(t, c, context.Background(), Write)
	if err := tx.InstallSchema("CREATE TABLE qa_parent(id INTEGER PRIMARY KEY); CREATE TABLE qa_items(id INTEGER PRIMARY KEY,value TEXT NOT NULL,parent INTEGER REFERENCES qa_parent(id) DEFERRABLE INITIALLY DEFERRED);"); err != nil {
		t.Fatal(err)
	}
	qaDone(t, tx, "INSERT INTO qa_items VALUES(1,'deferred',99)")
	err := tx.CheckForeignKeys()
	var checked *Error
	if !errors.As(err, &checked) || checked.Category != Corrupt {
		t.Fatalf("real FK audit error=%v", err)
	}
	qaSafeError(t, err, dir, "qa_items", "qa_parent", "deferred")
	installQARefuseControls(t, tx)
	qaDone(t, tx, "INSERT INTO qa_parent VALUES(99)")
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatalf("resolved dependency still fails fixed audit: %v", err)
	}
	qaCommit(t, tx)
	qaClose(t, c)
	if n := qaCount(t, dir, qaBasename); n != 1 {
		t.Fatal("audited deferred pair did not persist")
	}
	c = qaOpen(t, dir, qaBasename, false)
	tx = qaBegin(t, c, context.Background(), Read)
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	installQARefuseControls(t, tx)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
}
