//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

// No new producer seam: three real cold admission entry points and the existing
// native setup observer. Every returned owner is registered before assertions.
func niQAOpen(t *testing.T, ctx context.Context, dir, entry string, deadline time.Time) (*swQAOwner, LinkInspection, error) {
	t.Helper()
	var c *Conn
	var kind LinkInspection
	var err error
	switch entry {
	case "readonly-open":
		c, err = Open(ctx, dir, freshQAName, Options{ReadOnly: true, AcquireDeadline: deadline})
	case "writable-open":
		c, err = Open(ctx, dir, freshQAName, Options{AcquireDeadline: deadline})
	case "link-inspection":
		c, kind, err = InspectForLink(ctx, dir, freshQAState, freshQAName, deadline)
	default:
		t.Fatal("unknown finite admission case")
	}
	o := &swQAOwner{c: c}
	t.Cleanup(func() {
		if e := o.close(); e != nil || !o.ended {
			t.Errorf("identity QA checked owner cleanup terminal=%t error=%v", o.ended, e)
		}
	})
	return o, kind, err
}

func niQAText(tx *Tx, sql string) (value string, err error) {
	s, err := tx.Prepare(sql)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	row, err := s.Step()
	if err != nil {
		return "", err
	}
	if !row || s.ColumnCount() != 1 {
		return "", errors.New("identity SQL ROW/width")
	}
	kind, err := s.Kind(0)
	if err != nil {
		return "", err
	}
	if kind != TextKind {
		return "", errors.New("identity SQL TEXT kind")
	}
	value, err = s.Text(0)
	if err != nil {
		return "", err
	}
	row, err = s.Step()
	if err != nil {
		return "", err
	}
	if row {
		return "", errors.New("identity SQL missing DONE")
	}
	return value, nil
}

// Runs after Begin, outside the admission work measurement. SQL results use the
// public typed statement API; calibration also exercises actual control ROW/DONE.
func niQAControls(t *testing.T, o *swQAOwner, ctx context.Context) {
	t.Helper()
	var err error
	o.tx, err = o.c.Begin(ctx, Read)
	if err != nil {
		t.Fatal("identity positive Begin", errors.Join(err, o.close()))
	}
	version := lib.Xsqlite3_libversion(o.c.tls)
	sourceID := lib.Xsqlite3_sourceid(o.c.tls)
	if version == 0 || sourceID == 0 {
		t.Fatal("actual native identity pointer absent")
	}
	want := []struct{ sql, exact, native string }{
		{"SELECT sqlite_version()", "3.53.4", libc.GoString(version)},
		{"SELECT sqlite_source_id()", "2026-07-24 19:02:57 bf7c7f30031888f4e796e429ab3978879485813aaca6f641c7b33e4e09459bcc", libc.GoString(sourceID)},
	}
	for _, q := range want {
		got, e := niQAText(o.tx, q.sql)
		if e != nil || got != q.exact || q.native != q.exact || got != q.native {
			t.Fatal("actual SQL/native compiled identity differs", e)
		}
	}
	calibration := 0
	qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
		if e.Phase == "control-before-native" && e.Operation == "setup" {
			calibration++
		}
	}})
	for _, q := range want {
		got, e := o.c.control(ctx, q.sql, authPragma, VerifyPhase)
		if e != nil || got != q.exact {
			t.Fatal("actual control scalar calibration", e)
		}
	}
	setSQLHooksForTest(sqlTestHooks{})
	if calibration != 4 {
		t.Fatalf("two scalar controls did not yield four actual ROW/DONE steps: %d", calibration)
	}
	for _, q := range []struct{ sql, want string }{
		{"PRAGMA temp_store", "2"}, {"PRAGMA foreign_keys", "1"},
		{"PRAGMA synchronous", "2"}, {"PRAGMA wal_autocheckpoint", "0"}, {"PRAGMA journal_mode", "wal"},
	} {
		got, e := o.c.control(ctx, q.sql, authPragma, VerifyPhase)
		if e != nil || got != q.want {
			t.Fatal("required setup policy readback", q.sql, e)
		}
	}
	for _, q := range []struct {
		op   int32
		want bool
	}{
		{lib.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true}, {lib.SQLITE_DBCONFIG_DEFENSIVE, true},
		{lib.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, false}, {lib.SQLITE_DBCONFIG_TRUSTED_SCHEMA, false},
		{lib.SQLITE_DBCONFIG_DQS_DDL, false}, {lib.SQLITE_DBCONFIG_DQS_DML, false},
	} {
		got, e := o.c.config(q.op, -1)
		if e != nil || got != q.want {
			t.Fatal("required native configuration", q.op, e)
		}
	}
	info, err := o.tx.PageInfo()
	if err != nil || info.PageSize != PageSize || info.PageCount <= 0 || info.MaxPages < info.PageCount {
		t.Fatal("all page-policy fields remain native and checked", info, err)
	}
	if !o.c.readOnly && (info.MaxPages != MaxPages || info.JournalLimitBytes != JournalLimitBytes) {
		t.Fatal("writable limits changed", info)
	}
	linkQARawReceipt(t, o.tx)
	if err = o.tx.Rollback(); err != nil {
		t.Fatal("positive rollback", err)
	}
	if lib.Xsqlite3_next_stmt(o.c.tls, o.c.db, 0) != 0 || lib.Xsqlite3_get_autocommit(o.c.tls, o.c.db) == 0 || nativeLoad[uint32](o.c.authMode) != authApplication {
		t.Fatal("setup/control left native statement, transaction or authorizer mode")
	}
	if err = o.close(); err != nil || !o.ended {
		t.Fatal("positive terminal cleanup", err)
	}
	freshQAQuiet(t)
}

func niQASameMainWAL(t *testing.T, dir string, before map[string][]byte) {
	t.Helper()
	freshQAQuiet(t)
	after := freshQAImage(t, dir, freshQAName)
	for _, suffix := range []string{"", "-wal"} {
		b, bok := before[suffix]
		a, aok := after[suffix]
		if bok != aok || !bytes.Equal(a, b) {
			t.Fatal("cold main/WAL identity admission changed bytes", suffix)
		}
	}
	linkQANoAuthority(t, dir)
}

func niQAColdReceipt(t *testing.T, dir string) {
	t.Helper()
	o, _, err := niQAOpen(t, context.Background(), dir, "readonly-open", time.Now().Add(250*time.Millisecond))
	if err != nil {
		t.Fatal("cold receipt reopen", errors.Join(err, o.close()))
	}
	o.tx, err = o.c.Begin(context.Background(), Read)
	if err != nil {
		t.Fatal("cold receipt Begin", errors.Join(err, o.close()))
	}
	linkQARawReceipt(t, o.tx)
	if err = o.close(); err != nil || !o.ended {
		t.Fatal("cold receipt checked cleanup", err)
	}
	freshQAQuiet(t)
}

func TestSQLiteNativeIdentityGettersPreserveSetupAndReduceNativeWork(t *testing.T) {
	for _, entry := range []string{"readonly-open", "writable-open", "link-inspection"} {
		t.Run(entry, func(t *testing.T) {
			dir := qaDirectory(t)
			linkQASeedWAL(t, dir)
			before := freshQAImage(t, dir, freshQAName)
			steps := 0
			qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
				if e.Phase == "control-before-native" && e.Operation == "setup" {
					steps++
				}
			}})
			deadline := time.Now().Add(250 * time.Millisecond)
			o, kind, err := niQAOpen(t, context.Background(), dir, entry, deadline)
			setSQLHooksForTest(sqlTestHooks{})
			if err != nil {
				t.Fatal("real cold admission", errors.Join(err, o.close()))
			}
			if o.c == nil || o.c.db == 0 || o.c.closed || o.c.used || o.c.readOnly != (entry != "writable-open") || o.c.acquireDeadline != deadline {
				t.Fatal("admission changed owner, mode or original deadline")
			}
			if entry == "link-inspection" && kind != LinkWAL {
				t.Fatal("WAL observation changed", kind)
			}
			niQAControls(t, o, context.Background())
			niQASameMainWAL(t, dir, before)
			niQAColdReceipt(t, dir)
			niQASameMainWAL(t, dir, before)
			// This assertion is last: identity, calibration, policy, actual row,
			// cold bytes and all cleanup controls must pass before a work RED.
			t.Logf("actual cold setup native control steps=%d; two identity SQL controls independently calibrated to 4", steps)
			if steps != 13 {
				t.Fatalf("redundant identity SQL work: got %d setup steps, want 13 (original source predicts 17)", steps)
			}
		})
	}
}

func TestSQLiteNativeIdentityAdmissionCancellationAndBudgetRemainExact(t *testing.T) {
	for _, entry := range []string{"readonly-open", "writable-open", "link-inspection"} {
		for _, phase := range []string{"canceled-before", "expired-budget", "cancel-in-setup"} {
			t.Run(entry+"/"+phase, func(t *testing.T) {
				dir := qaDirectory(t)
				linkQASeedWAL(t, dir)
				// Positive control first on the same fixture, without the work assertion.
				control, _, err := niQAOpen(t, context.Background(), dir, entry, time.Now().Add(250*time.Millisecond))
				if err != nil {
					t.Fatal("cancellation fixture positive admission", errors.Join(err, control.close()))
				}
				niQAControls(t, control, context.Background())
				before := freshQAImage(t, dir, freshQAName)
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				deadline := time.Now().Add(250 * time.Millisecond)
				if phase == "canceled-before" {
					cancel()
				}
				if phase == "expired-budget" {
					deadline = time.Now().Add(-time.Second)
				}
				reached := 0
				qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
					if e.Phase == "control-before-native" && e.Operation == "setup" {
						reached++
						if phase == "cancel-in-setup" {
							cancel()
						}
					}
				}})
				o, kind, openErr := niQAOpen(t, ctx, dir, entry, deadline)
				setSQLHooksForTest(sqlTestHooks{})
				closeErr := o.close()
				var checked *Error
				if openErr == nil || !errors.As(openErr, &checked) || kind != 0 || o.c != nil || closeErr != nil || !o.ended {
					t.Fatal("refusal escaped checked cleanup or classification", openErr, closeErr)
				}
				if phase == "expired-budget" {
					if ctx.Err() != nil || !errors.Is(openErr, ErrBusy) || checked.Phase != Admission || checked.Category != Busy || checked.Code != 0 {
						t.Fatal("explicit budget became caller cancellation", openErr)
					}
				} else if !errors.Is(openErr, context.Canceled) || checked.Category != Canceled {
					t.Fatal("caller cancellation identity lost", openErr)
				}
				if phase == "cancel-in-setup" && reached != 1 || phase != "cancel-in-setup" && reached != 0 {
					t.Fatal("cancellation did not reach exact native boundary", reached)
				}
				qaSafeError(t, openErr, dir, freshQAName)
				niQASameMainWAL(t, dir, before)
				niQAColdReceipt(t, dir)
				niQASameMainWAL(t, dir, before)
			})
		}
	}
}
