//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	lib "modernc.org/sqlite/lib"
)

// This diagnostic compiles against the unchanged adapter. It calls the real
// configured native handle, fixed checked probe and schema-memory counter; it
// does not implement or substitute the prospective public writer prerequisite.
// All owner cleanup is registered before the first fallible native operation.
func wpQAProbe(t *testing.T, dir, name string, body func(context.Context, *Conn) error) (err error) {
	t.Helper()
	deadline := time.Now().Add(250 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	r, err := acquireRoot(ctx, dir, name, false, deadline)
	if err != nil {
		return err
	}
	o := &swQAOwner{c: newConnection(r, Options{ReadOnly: true, AcquireDeadline: deadline})}
	var stop func()
	closeOwner := func() error {
		if stop != nil {
			stop()
			stop = nil
		}
		return o.close()
	}
	t.Cleanup(func() {
		if e := closeOwner(); e != nil || !o.ended {
			t.Errorf("probe diagnostic cleanup terminal=%t error=%v", o.ended, e)
		}
	})
	defer func() { err = errors.Join(err, closeOwner()) }()
	if err = validateRoot(r); err != nil {
		return err
	}
	if err = requireAuthorityAbsent(r, freshQAState); err != nil {
		return err
	}
	if err = initialize(); err != nil {
		return err
	}
	beginInitialProbe(r)
	if err = o.c.openNative(ctx, Options{ReadOnly: true, AcquireDeadline: deadline}); err != nil {
		return err
	}
	stop = o.c.interruptWith(ctx)
	pristine, err := o.c.initialMainProfile(ctx)
	if err != nil {
		return err
	}
	if pristine {
		return errors.New("diagnostic requires actual qualified WAL main")
	}
	if err = finishInitialProbe(r); err != nil {
		return err
	}
	if err = body(ctx, o.c); err != nil {
		return err
	}
	if err = initialProbeError(r); err != nil {
		return err
	}
	if err = validateRoot(r); err != nil {
		return err
	}
	if err = requireAuthorityAbsent(r, freshQAState); err != nil {
		return err
	}
	return admissionError(ctx, deadline)
}

func wpQASchemaBytes(c *Conn) (int64, error) {
	p := lib.Xsqlite3_malloc64(c.tls, 16)
	if p == 0 {
		return 0, engineError(OpenPhase, lib.SQLITE_NOMEM, nil)
	}
	defer lib.Xsqlite3_free(c.tls, p)
	if rc := lib.Xsqlite3_db_status64(c.tls, c.db, lib.SQLITE_DBSTATUS_SCHEMA_USED, p, p+8, 0); rc != lib.SQLITE_OK {
		return 0, engineError(OpenPhase, rc, nil)
	}
	n, high := nativeLoad[int64](p), nativeLoad[int64](p+8)
	if n < 0 || high != 0 {
		return 0, errors.New("invalid native schema-memory counter")
	}
	return n, nil
}

func TestSQLiteWriterProbeDiagnosticCookieAvoidsCatalogParse(t *testing.T) {
	dir := qaDirectory(t)
	ddl, err := os.ReadFile("../sqlite_schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	o, err := rpQAOpen(t, context.Background(), dir, Options{Create: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	if err != nil {
		t.Fatal("actual schema seed open", errors.Join(err, o.close()))
	}
	o.tx, err = o.c.Begin(context.Background(), Write)
	if err == nil {
		err = o.tx.InstallSchema(string(ddl))
	}
	if err != nil {
		t.Fatal("actual embedded schema install", errors.Join(err, o.close()))
	}
	if outcome, e := o.tx.Commit(); outcome != Committed || e != nil {
		t.Fatal("actual catalog commit", outcome, errors.Join(e, o.close()))
	}
	if err = o.close(); err != nil {
		t.Fatal(err)
	}
	before := rpQAImage(t, dir)
	var opened, cookie, full, again int64
	err = wpQAProbe(t, dir, qaBasename, func(ctx context.Context, c *Conn) error {
		var e error
		opened, e = wpQASchemaBytes(c)
		if e != nil {
			return e
		}
		version, e := c.probeInteger(ctx, "PRAGMA schema_version")
		if e != nil {
			return e
		}
		if version <= 0 {
			return errors.New("actual catalog cookie absent")
		}
		cookie, e = wpQASchemaBytes(c)
		if e != nil {
			return e
		}
		mode, e := c.probeText(ctx, "PRAGMA journal_mode")
		if e != nil {
			return e
		}
		if mode != "wal" {
			return errors.New("native full-parse positive control is not WAL")
		}
		full, e = wpQASchemaBytes(c)
		if e != nil {
			return e
		}
		// The literal production schema has 32 tables, 47 explicit indexes and
		// six triggers. Implicit autoindexes are excluded by this fixed query.
		count, e := c.probeInteger(ctx, "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'")
		if e != nil {
			return e
		}
		if count != 85 {
			return fmt.Errorf("actual embedded catalog count=%d", count)
		}
		again, e = c.probeInteger(ctx, "PRAGMA schema_version")
		if e != nil {
			return e
		}
		if again != version {
			return errors.New("read-only cookie changed")
		}
		if lib.Xsqlite3_get_autocommit(c.tls, c.db) == 0 || lib.Xsqlite3_next_stmt(c.tls, c.db, 0) != 0 || nativeLoad[uint32](c.authMode) != authApplication {
			return errors.New("checked scalar left native transaction, statement or authorizer mode")
		}
		return nil
	})
	if err != nil {
		t.Fatal("real native cookie/full-parse diagnostic", err)
	}
	if after := rpQAImage(t, dir); !reflect.DeepEqual(after, before) {
		t.Fatal("read-only diagnostic changed main/WAL bytes")
	}
	linkQANoAuthority(t, dir)
	freshQAQuiet(t)
	t.Logf("actual schema_used configured_open=%d cookie=%d full_journal_mode=%d cookie_value=%d", opened, cookie, full, again)
	if cookie != opened || full <= cookie {
		t.Fatalf("candidate has not demonstrated avoiding catalog parse: open=%d cookie=%d full=%d", opened, cookie, full)
	}
}

func TestSQLiteWriterProbeDiagnosticCookieRefusesActualHotJournal(t *testing.T) {
	dir := qaDirectory(t)
	child := freshQAChildStart(t, "hot-journal", "spilled", dir, freshQAName)
	child.join(t, true)
	freshQAQuiet(t)
	initial := freshQAImage(t, dir, freshQAName)
	magic := []byte{0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7}
	if len(initial["-journal"]) <= 512 || !bytes.Equal(initial["-journal"][:8], magic) || len(initial[""]) < 4096 {
		t.Fatal("actual spilled native hot-journal premise not reached")
	}
	// The genuine journal is untouched. Qualify only the main's physical WAL
	// header so both readonly paths must reach the pager, not a DELETE-profile
	// no-sidecar shortcut. No native handle survives the checked child join.
	fd, err := unix.Open(filepath.Join(dir, freshQAName), unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	n, writeErr := unix.Pwrite(fd, []byte{2, 2}, 18)
	closeErr := unix.Close(fd)
	if n != 2 || writeErr != nil || closeErr != nil {
		t.Fatal("owned closed-main header edit", n, writeErr, closeErr)
	}
	before := freshQAImage(t, dir, freshQAName)
	wantMain := append([]byte(nil), initial[""]...)
	wantMain[18], wantMain[19] = 2, 2
	if !bytes.Equal(before[""], wantMain) || !bytes.Equal(before["-journal"], initial["-journal"]) {
		t.Fatal("hot-journal control changed more than the two physical mode bytes")
	}
	checkRefusal := func(label string, e error) {
		t.Helper()
		var native *Error
		if !errors.As(e, &native) || native.Code != lib.SQLITE_READONLY_ROLLBACK {
			t.Fatalf("%s did not reach actual read-only hot-journal refusal: %v", label, e)
		}
		// sqlite3_finalize may repeat the failed Step result. That checked
		// Finalize evidence is distinct from an inconclusive Conn close.
		if native.Cleanup != nil {
			f, ok := native.Cleanup.(*Error)
			if !ok || f.Phase != FinalizePhase || f.Code != lib.SQLITE_READONLY_ROLLBACK || f.Cleanup != nil {
				t.Fatalf("%s unexpected cleanup evidence: %v", label, e)
			}
		}
		qaSafeError(t, e, dir, freshQAName)
		freshQASameImage(t, dir, freshQAName, before)
		linkQANoAuthority(t, dir)
		freshQAQuiet(t)
	}
	c, kind, err := InspectForLink(context.Background(), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
	owner := &swQAOwner{c: c}
	t.Cleanup(func() {
		if e := owner.close(); e != nil {
			t.Error(e)
		}
	})
	if e := owner.close(); e != nil {
		t.Fatal("original inspection cleanup", e)
	}
	if c != nil || kind != 0 {
		t.Fatal("original refusal retained usable output", kind)
	}
	checkRefusal("existing full inspection", err)
	err = wpQAProbe(t, dir, freshQAName, func(ctx context.Context, c *Conn) error {
		_, e := c.probeInteger(ctx, "PRAGMA schema_version")
		return e
	})
	checkRefusal("checked cookie prerequisite", err)
}
