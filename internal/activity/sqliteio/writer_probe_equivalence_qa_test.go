//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	lib "modernc.org/sqlite/lib"
)

// This file requires the prospective InspectForCaptureWrite entry point. Its
// absence is a compile prerequisite, never evidence of a behavioral RED.
func weQAInspect(t *testing.T, ctx context.Context, dir string, deadline time.Time) (*swQAOwner, LinkInspection, error) {
	t.Helper()
	c, kind, err := InspectForCaptureWrite(ctx, dir, freshQAState, freshQAName, deadline)
	o := &swQAOwner{c: c}
	t.Cleanup(func() {
		if e := o.close(); e != nil || !o.ended {
			t.Errorf("writer probe cleanup terminal=%t error=%v", o.ended, e)
		}
	})
	return o, kind, err
}
func weQANow(t *testing.T, dir string) (*swQAOwner, LinkInspection, error) {
	return weQAInspect(t, context.Background(), dir, time.Now().Add(250*time.Millisecond))
}
func weQARefused(t *testing.T, o *swQAOwner, kind LinkInspection, err error) {
	t.Helper()
	if err == nil || kind != 0 {
		t.Fatal("refusal supplied usable observation", kind, err)
	}
	qaSafeError(t, err)
	if e := o.close(); e != nil {
		t.Fatal("refusal owner cleanup", e)
	}
	freshQAQuiet(t)
}
func weQASchemaBytes(t *testing.T, c *Conn) int64 {
	t.Helper()
	p := lib.Xsqlite3_malloc64(c.tls, 16)
	if p == 0 {
		t.Fatal("native schema counter allocation")
	}
	defer lib.Xsqlite3_free(c.tls, p)
	if rc := lib.Xsqlite3_db_status64(c.tls, c.db, lib.SQLITE_DBSTATUS_SCHEMA_USED, p, p+8, 0); rc != lib.SQLITE_OK {
		t.Fatal("native schema counter", rc)
	}
	n, high := nativeLoad[int64](p), nativeLoad[int64](p+8)
	if n < 0 || high != 0 {
		t.Fatal("native schema counter shape", n, high)
	}
	return n
}

func TestSQLiteWriterProbeAbsentPristineAndOccupiedAuthority(t *testing.T) {
	for _, profile := range []string{"absent", "absent-parent", "zero", "single"} {
		t.Run(profile, func(t *testing.T) {
			base := qaDirectory(t)
			dir := base
			want := LinkAbsent
			if profile == "absent-parent" {
				dir = filepath.Join(base, "missing", "child")
			}
			if profile == "zero" || profile == "single" {
				freshQAProfile(t, dir, profile)
				want = LinkPristine
			}
			before := freshQAImage(t, base, freshQAName)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			o, kind, err := weQANow(t, dir)
			if err != nil || o.c != nil || kind != want {
				t.Fatal("exact absent/pristine ownership", kind, err)
			}
			if err := o.close(); err != nil {
				t.Fatal(err)
			}
			linkQANoCreating(t, trace, true)
			freshQASameImage(t, base, freshQAName, before)
			linkQANoAuthority(t, base)
			freshQAQuiet(t)
			if profile == "absent-parent" {
				linkQAEmpty(t, base)
			}
		})
	}
	for _, profile := range []string{"file", "directory", "symlink"} {
		t.Run("authority-"+profile, func(t *testing.T) {
			dir := qaDirectory(t)
			path := linkQAOccupied(t, dir, profile)
			before := freshQAOccupiedSnapshot(t, path)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			o, kind, err := weQANow(t, dir)
			var e *Error
			if !errors.As(err, &e) || e.Phase != Admission || e.Category != Unsafe || !errors.Is(err, os.ErrExist) || o.c != nil {
				t.Fatal("occupied P precedence", err)
			}
			weQARefused(t, o, kind, err)
			linkQASameOccupied(t, path, before)
			if trace.count("main", "open", "validated") != 0 {
				t.Fatal("occupied P reached native main")
			}
			if _, e := os.Lstat(filepath.Join(dir, freshQAState+".lock")); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("probe created authority guard", e)
			}
		})
	}
}

func TestSQLiteWriterProbePhysicalAndNamespaceRefusals(t *testing.T) {
	for _, mutation := range []string{"short", "magic", "mixed-mode", "wrong-page", "fraction"} {
		t.Run(mutation, func(t *testing.T) {
			dir := qaDirectory(t)
			freshQAProfile(t, dir, "single")
			b := freshQAImage(t, dir, freshQAName)[""]
			if len(b) != 4096 {
				t.Fatal("physical control")
			}
			b[18], b[19] = 2, 2
			switch mutation {
			case "short":
				b = b[:100]
			case "magic":
				b[0] = 'X'
			case "mixed-mode":
				b[19] = 1
			case "wrong-page":
				b[16] = 32
			case "fraction":
				b[21] = 63
			}
			if err := os.WriteFile(filepath.Join(dir, freshQAName), b, 0600); err != nil {
				t.Fatal(err)
			}
			before := freshQAImage(t, dir, freshQAName)
			o, kind, err := weQANow(t, dir)
			weQARefused(t, o, kind, err)
			freshQASameImage(t, dir, freshQAName, before)
			linkQANoAuthority(t, dir)
		})
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		t.Run("sidecar-only"+suffix, func(t *testing.T) {
			dir := qaDirectory(t)
			if err := os.WriteFile(filepath.Join(dir, freshQAName+suffix), []byte("owned"), 0600); err != nil {
				t.Fatal(err)
			}
			before := freshQAImage(t, dir, freshQAName)
			o, kind, err := weQANow(t, dir)
			weQARefused(t, o, kind, err)
			freshQASameImage(t, dir, freshQAName, before)
		})
	}
	t.Run("ancestor-symlink", func(t *testing.T) {
		base, target := qaDirectory(t), qaDirectory(t)
		if err := os.Symlink(target, filepath.Join(base, "alias")); err != nil {
			t.Fatal(err)
		}
		o, kind, err := weQANow(t, filepath.Join(base, "alias", "missing"))
		weQARefused(t, o, kind, err)
		linkQAEmpty(t, target)
	})
}

func TestSQLiteWriterProbeWALCleanupOnlyAndNativeCloseBusy(t *testing.T) {
	dir := qaDirectory(t)
	linkQASeedWAL(t, dir)
	// Add a real committed WAL-only change after the seed's checkpoint.
	w := qaOpen(t, dir, freshQAName, false)
	tx := qaBegin(t, w, context.Background(), Write)
	qaDone(t, tx, "UPDATE link_qa_meta SET nonce=20")
	qaCommit(t, tx)
	qaClose(t, w)
	before := freshQAImage(t, dir, freshQAName)
	if len(before["-wal"]) <= 32 {
		t.Fatal("committed WAL visibility premise")
	}
	control, controlKind, controlErr := InspectForLink(context.Background(), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
	controlOwner := &swQAOwner{c: control}
	t.Cleanup(func() {
		if e := controlOwner.close(); e != nil {
			t.Error(e)
		}
	})
	if controlErr != nil || controlKind != LinkWAL || control == nil || control.used {
		t.Fatal("unchanged full inspection control", controlKind, controlErr)
	}
	fullSchema := weQASchemaBytes(t, control)
	if e := controlOwner.close(); e != nil {
		t.Fatal(e)
	}
	o, kind, err := weQANow(t, dir)
	if err != nil || kind != LinkWAL || o.c == nil || !o.c.used || !o.c.readOnly || o.c.closed || o.c.db == 0 {
		t.Fatal("cleanup-only native WAL owner", kind, err)
	}
	probeSchema := weQASchemaBytes(t, o.c)
	t.Logf("actual entrypoint schema_used probe=%d full_inspection=%d", probeSchema, fullSchema)
	if probeSchema != 0 || fullSchema <= 0 {
		t.Fatal("new entrypoint must retain zero schema bytes with positive full-inspection control", probeSchema, fullSchema)
	}
	if s := statsForTest(); s.Active != 1 || s.NativeActive != 1 {
		t.Fatal("probe owner accounting", s)
	}
	for _, mode := range []Mode{Read, Write} {
		if tx, err := o.c.Begin(context.Background(), mode); tx != nil || !errors.Is(err, ErrClosed) {
			t.Fatal("cleanup-only probe admitted Begin", mode, err)
		}
	}
	// Real unfinalized native statement makes sqlite3_close return BUSY. The
	// test does not clear used or replace a result. Existing holdStatementForTest
	// intentionally refuses used handles, so this private, serial fixture owns
	// the raw prepared statement directly and registers its finalizer first.
	stmt, err := o.c.prepareRaw("SELECT 1", PreparePhase)
	if err != nil {
		t.Fatal(err)
	}
	finalize := func() error {
		if stmt == 0 {
			return nil
		}
		rc := lib.Xsqlite3_finalize(o.c.tls, stmt)
		stmt = 0
		if rc != lib.SQLITE_OK {
			return engineError(FinalizePhase, rc, nil)
		}
		return nil
	}
	t.Cleanup(func() {
		if err := finalize(); err != nil {
			t.Error(err)
		}
	})
	terminal, err := o.c.CloseChecked(context.Background())
	var native *Error
	if terminal || !errors.As(err, &native) || native.Phase != ClosePhase || native.Code&255 != lib.SQLITE_BUSY || o.c.db == 0 || o.c.closed || statsForTest().NativeActive != 1 {
		t.Fatal("real BUSY released native owner", terminal, err)
	}
	if err := finalize(); err != nil {
		t.Fatal(err)
	}
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	if terminal, err := o.c.CloseChecked(context.Background()); !terminal || err != nil {
		t.Fatal("terminal cached close", terminal, err)
	}
	freshQAQuiet(t)
	after := freshQAImage(t, dir, freshQAName)
	if !bytes.Equal(after[""], before[""]) || !bytes.Equal(after["-wal"], before["-wal"]) {
		t.Fatal("probe changed main/WAL")
	}
	linkQAColdReceipt(t, dir, 20)
	linkQANoAuthority(t, dir)
}

func TestSQLiteWriterProbeCancellationDeadlineAndRetainedCleanup(t *testing.T) {
	for _, mode := range []string{"already-canceled", "expired-budget", "reached-budget", "reached-cancel-close-fault"} {
		t.Run(mode, func(t *testing.T) {
			dir := qaDirectory(t)
			linkQASeedWAL(t, dir)
			before := freshQAImage(t, dir, freshQAName)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			deadline := time.Now().Add(250 * time.Millisecond)
			reached, closeReached := false, false
			if mode == "already-canceled" {
				cancel()
			}
			if mode == "expired-budget" {
				deadline = time.Now().Add(-time.Second)
			}
			qaFSHooks(t, hooks{Observe: func(e event) {
				if reached || e.Role != "main" || e.Op != "open" || e.Phase != "validated" {
					return
				}
				reached = true
				if mode == "reached-cancel-close-fault" {
					cancel()
				}
				if mode == "reached-budget" {
					timer := time.NewTimer(time.Until(deadline) + time.Millisecond)
					defer timer.Stop()
					<-timer.C
				}
			}})
			qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
				if mode == "reached-cancel-close-fault" && e.Phase == "close-before" && e.Operation == "close" {
					closeReached = true
					return unix.EIO
				}
				return nil
			}})
			o, kind, err := weQAInspect(t, ctx, dir, deadline)
			// Failure cleanup must remove the injected close fault before the
			// just-registered retained-owner cleanup runs (testing cleanup is LIFO).
			t.Cleanup(func() { setHooksForTest(hooks{}); setSQLHooksForTest(sqlTestHooks{}) })
			if mode == "reached-budget" || mode == "reached-cancel-close-fault" {
				if !reached {
					t.Fatal("required real native boundary not reached")
				}
			} else if reached {
				t.Fatal("pre-admission refusal opened main")
			}
			if mode == "already-canceled" || mode == "reached-cancel-close-fault" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal("caller cancellation precedence", err)
				}
			} else if !errors.Is(err, ErrBusy) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("original budget refusal", err)
			}
			if mode == "reached-cancel-close-fault" && (!closeReached || o.c == nil || o.c.db == 0 || o.c.closed || statsForTest().NativeActive != 1) {
				t.Fatal("partial owner lost", err)
			}
			setHooksForTest(hooks{})
			setSQLHooksForTest(sqlTestHooks{})
			weQARefused(t, o, kind, err)
			freshQASameImage(t, dir, freshQAName, before)
			linkQANoAuthority(t, dir)
		})
	}
}

func TestSQLiteWriterProbeRootReplacementRefuses(t *testing.T) {
	dir := qaDirectory(t)
	linkQASeedWAL(t, dir)
	before := freshQAImage(t, dir, freshQAName)
	old := dir + "-owned-displaced"
	moved := false
	var callbackErr error
	t.Cleanup(func() {
		if err := os.RemoveAll(old); err != nil {
			t.Error(err)
		}
	})
	qaFSHooks(t, hooks{Observe: func(e event) {
		if moved || e.Role != "main" || e.Op != "open" || e.Phase != "before" {
			return
		}
		if e := os.Rename(dir, old); e != nil {
			callbackErr = e
			return
		}
		moved = true
		callbackErr = os.Mkdir(dir, 0700)
	}})
	o, kind, err := weQANow(t, dir)
	setHooksForTest(hooks{})
	if callbackErr != nil {
		t.Fatal("owned replacement fixture", callbackErr)
	}
	if !moved {
		t.Fatal("replacement boundary not reached")
	}
	weQARefused(t, o, kind, err)
	freshQASameImage(t, old, freshQAName, before)
	linkQAEmpty(t, dir)
}

func TestSQLiteWriterProbeAliasPreservesNativePOSIXLocks(t *testing.T) {
	dir, other := qaDirectory(t), qaDirectory(t)
	name := "owned-live.sqlite3"
	c, err := Open(context.Background(), other, name, Options{Create: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	owner := &swQAOwner{c: c}
	t.Cleanup(func() {
		if e := owner.close(); e != nil {
			t.Error(e)
		}
	})
	if err != nil {
		t.Fatal("live alias control open", err)
	}
	owner.tx, err = c.Begin(context.Background(), Write)
	if err != nil {
		t.Fatal(err)
	}
	qaDone(t, owner.tx, "CREATE TABLE owned_lock(id INTEGER)")
	freshQALockWitness(t, other, name, "main")
	freshQAProfile(t, dir, "single")
	target, source, displaced := filepath.Join(dir, freshQAName), filepath.Join(other, name), filepath.Join(dir, "owned-displaced-main")
	moved, quarantined := false, false
	var callbackErr error
	// Restoration runs before the original transaction's cleanup on every exit.
	t.Cleanup(func() {
		if moved {
			if e := os.Rename(target, source); e != nil {
				t.Error(e)
			}
			if e := os.Rename(displaced, target); e != nil {
				t.Error(e)
			}
			moved = false
		}
	})
	qaFSHooks(t, hooks{Observe: func(e event) {
		if !moved && e.Namespace != c.root.key && e.Role == "main" && e.Op == "open" && e.Phase == "before" {
			if e := os.Rename(target, displaced); e != nil {
				callbackErr = e
				return
			}
			if e := os.Rename(source, target); e != nil {
				callbackErr = errors.Join(e, os.Rename(displaced, target))
				return
			}
			moved = true
		}
		if e.Role == "main" && e.Op == "open" && e.Phase == "quarantined" {
			if e.FD < 0 {
				callbackErr = errors.New("missing real quarantined FD")
			}
			quarantined = true
		}
	}})
	o, kind, err := weQANow(t, dir)
	if callbackErr != nil {
		t.Fatal("owned alias fixture", callbackErr)
	}
	if !moved || !quarantined || err == nil || kind != 0 {
		t.Fatal("actual native alias refusal not reached", err)
	}
	if e := o.close(); e != nil {
		t.Fatal(e)
	}
	if s := statsForTest(); !s.Poisoned || s.RejectedFDs < 1 || s.NativeActive != 1 {
		t.Fatal("alias descriptor drained beside live owner", s)
	}
	freshQALockWitness(t, dir, freshQAName, "main")
	setHooksForTest(hooks{})
	if err := os.Rename(target, source); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(displaced, target); err != nil {
		t.Fatal(err)
	}
	moved = false
	if err := owner.close(); err != nil {
		t.Fatal(err)
	}
	freshQAQuiet(t)
}

func TestSQLiteWriterProbeHotJournalRefusesWithoutRecovery(t *testing.T) {
	dir := qaDirectory(t)
	child := freshQAChildStart(t, "hot-journal", "spilled", dir, freshQAName)
	child.join(t, true)
	freshQAQuiet(t)
	image := freshQAImage(t, dir, freshQAName)
	magic := []byte{0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7}
	if len(image[""]) < 4096 || len(image["-journal"]) <= 512 || !bytes.Equal(image["-journal"][:8], magic) {
		t.Fatal("actual spilled hot journal not reached")
	}
	main := append([]byte(nil), image[""]...)
	main[18], main[19] = 2, 2
	if err := os.WriteFile(filepath.Join(dir, freshQAName), main, 0600); err != nil {
		t.Fatal(err)
	}
	before := freshQAImage(t, dir, freshQAName)
	if !bytes.Equal(before[""], main) || !bytes.Equal(before["-journal"], image["-journal"]) {
		t.Fatal("closed header qualification altered journal")
	}
	o, kind, err := weQANow(t, dir)
	var native *Error
	if !errors.As(err, &native) || native.Code != lib.SQLITE_READONLY_ROLLBACK {
		t.Fatal("read-only pager refusal not reached", err)
	}
	weQARefused(t, o, kind, err)
	freshQASameImage(t, dir, freshQAName, before)
	linkQANoAuthority(t, dir)
}
