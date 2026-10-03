//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

// Source-only independent prerequisite QA. These fixtures depend explicitly on
// root integration of fresh_fixture_test.go (freshQAProfile, freshQARawRoot,
// freshQARawOpen, freshQARawSQL, freshQARawRelease, freshQAImage,
// freshQASameImage, freshQAOccupiedSnapshot, freshQAOwn, freshQAQuiet,
// freshQALockWitness, freshQAContext, freshQAState, freshQAName, freshQADDL,
// freshQAValue, freshQAChildStart/child.join hot-journal mode) and its accepted
// native registry producer. No substitute
// InspectForLink/CheckAuthorityAbsent implementation is supplied here.

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	lib "modernc.org/sqlite/lib"
)

func linkQAInspect(t *testing.T, dir string) (*Conn, LinkInspection, error) {
	t.Helper()
	c, kind, err := InspectForLink(freshQAContext(t), dir, freshQAState, freshQAName, time.Now().Add(time.Second))
	freshQAOwn(t, c)
	return c, kind, err
}

func linkQAEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("inspection created an owned directory entry", err)
	}
}

func linkQANoAuthority(t *testing.T, dir string) {
	t.Helper()
	for _, suffix := range []string{"", ".lock"} {
		if _, err := os.Lstat(filepath.Join(dir, freshQAState+suffix)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("inspection created/opened initialization authority", err)
		}
	}
}

func linkQARefused(t *testing.T, c *Conn, kind LinkInspection, err error) {
	t.Helper()
	if err == nil || kind != LinkInspection(0) {
		t.Fatal("error supplied usable inspection classification", kind, err)
	}
	qaSafeError(t, err)
	if c != nil {
		var checked *Error
		if !errors.As(err, &checked) || (checked.Cleanup == nil && checked.Phase != ClosePhase) || c.closed {
			t.Fatal("nonnil refusal owner lacks actual inconclusive cleanup evidence", err)
		}
		// Retain the exact owner; cleanup neither Begin nor durable ACK.
		if closeErr := c.Close(context.Background()); closeErr != nil {
			t.Fatal("inspection cleanup remained inconclusive", closeErr)
		}
	}
	freshQAQuiet(t)
}

func linkQAOccupied(t *testing.T, dir, kind string) string {
	t.Helper()
	path := filepath.Join(dir, freshQAState)
	switch kind {
	case "file":
		if err := os.WriteFile(path, []byte("owned P bytes"), 0600); err != nil {
			t.Fatal(err)
		}
	case "directory":
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "owned-sentinel"), []byte("owned child"), 0600); err != nil {
			t.Fatal(err)
		}
	case "symlink":
		if err := os.Symlink("missing-owned-target", path); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("unknown finite occupied fixture")
	}
	return path
}

func linkQASameOccupied(t *testing.T, path string, want freshQAOccupiedPath) {
	t.Helper()
	if !reflect.DeepEqual(freshQAOccupiedSnapshot(t, path), want) {
		t.Fatal("authority type/identity/content changed")
	}
}

// Raw native WAL with an exact receipt BLOB and nonce, kept independently of
// the domain ABI. Inspection grants only observation, never schema admission.
func linkQASeedWAL(t *testing.T, dir string) {
	t.Helper()
	r := freshQARawRoot(t, dir, freshQAName)
	h := freshQARawOpen(t, r)
	freshQARawSQL(t, h, "PRAGMA page_size=4096")
	freshQARawSQL(t, h, "PRAGMA journal_mode=WAL")
	freshQARawSQL(t, h, "BEGIN IMMEDIATE")
	freshQARawSQL(t, h, "CREATE TABLE link_qa_receipts(request_id TEXT PRIMARY KEY,fingerprint BLOB NOT NULL,result BLOB NOT NULL) STRICT")
	freshQARawSQL(t, h, "CREATE TABLE link_qa_meta(nonce INTEGER NOT NULL) STRICT")
	freshQARawSQL(t, h, "INSERT INTO link_qa_meta VALUES(19)")
	freshQARawSQL(t, h, "INSERT INTO link_qa_receipts VALUES('owned-request',X'00FF0100',X'7B226964223A317D')")
	freshQARawSQL(t, h, "COMMIT")
	// Give the physical main a genuine 100-byte WAL header before all native
	// owners close. NO_CKPT_ON_CLOSE otherwise leaves committed rows WAL-only.
	freshQARawSQL(t, h, "PRAGMA wal_checkpoint(TRUNCATE)")
	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	freshQARawRelease(t, r)
}

func linkQARawReceipt(t *testing.T, tx *Tx, wantNonce ...int64) {
	t.Helper()
	s := qaPrepare(t, tx, "SELECT request_id,fingerprint,result,(SELECT nonce FROM link_qa_meta) FROM link_qa_receipts")
	row, err := s.Step()
	if err != nil || !row || s.ColumnCount() != 4 {
		t.Fatal("exact receipt projection", err)
	}
	id, e1 := s.Text(0)
	fp, e2 := s.Blob(1)
	result, e3 := s.Blob(2)
	nonce, e4 := s.Int64(3)
	want := int64(19)
	if len(wantNonce) != 0 {
		want = wantNonce[0]
	}
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || id != "owned-request" ||
		!bytes.Equal(fp, []byte{0, 255, 1, 0}) || !bytes.Equal(result, []byte(`{"id":1}`)) || nonce != want {
		t.Fatal("raw receipt/meta changed")
	}
	if row, err = s.Step(); row || err != nil {
		t.Fatal("exact receipt did not reach DONE", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

func linkQAColdReceipt(t *testing.T, dir string, wantNonce ...int64) {
	t.Helper()
	c := qaOpen(t, dir, freshQAName, false)
	tx := qaBegin(t, c, freshQAContext(t), Read)
	linkQARawReceipt(t, tx, wantNonce...)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
	freshQAQuiet(t)
}

func linkQANoCreating(t *testing.T, trace *freshQATrace, deleteProfile bool) {
	t.Helper()
	for _, e := range trace.snapshot() {
		if e.Role == "guard" || e.Op == "flock" || e.Op == "exclusive" || e.Op == "unlink" || e.Op == "fsync" {
			t.Fatal("inspection reached initialization/durability effect", e.Role, e.Op, e.Phase)
		}
		if deleteProfile && (e.Role == "wal" || e.Role == "shm" || e.Role == "journal") && e.Op == "open" {
			t.Fatal("DELETE inspection opened a sidecar")
		}
		if e.Role == "main" && e.Op == "open" && e.Flags&unix.O_CREAT != 0 {
			t.Fatal("inspection requested main CREATE")
		}
		if e.Role == "main" && e.Op == "vfs-open" && e.Flags&int(lib.SQLITE_OPEN_CREATE) != 0 {
			t.Fatal("inspection requested native main CREATE")
		}
	}
}

func linkQABareENOSPC(t *testing.T, err error, phase Phase) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Phase != phase || e.Category != IO || e.Code != 0 || e.Cause != unix.ENOSPC || !errors.Is(err, unix.ENOSPC) {
		t.Fatal("known ENOSPC was not retained as bare IO/code0 evidence", err)
	}
	if e.Error() != "SQLite "+string(phase)+" failure (io, code 0)" {
		t.Fatal("public error text changed")
	}
	var path *os.PathError
	if errors.As(err, &path) {
		t.Fatal("raw wrapper retained")
	}
	return e
}

// A finite filesystem-only coordinator owns no SQL/native handle. join cancels
// waits at both the observation and timer, joins the exact child release, and
// can be called by normal, deferred and test-cleanup paths without a retry.
type linkQAAuthorityWait struct {
	cancel context.CancelFunc
	joined chan struct{}
	result chan error
}

func linkQAAuthorityCoordinator(operationCtx context.Context, dir, kind string, entered <-chan struct{}, release func()) *linkQAAuthorityWait {
	ctx, cancel := context.WithTimeout(operationCtx, time.Second)
	w := &linkQAAuthorityWait{cancel: cancel, joined: make(chan struct{}), result: make(chan error, 1)}
	go func() {
		var err error
		defer func() {
			w.result <- err
			release() // Exact already-handshaken subprocess owner is joined.
			close(w.joined)
		}()
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-entered:
		}
		timer := time.NewTimer(60 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-timer.C:
		}
		path := filepath.Join(dir, freshQAState)
		switch kind {
		case "file":
			err = os.WriteFile(path, []byte("owned P bytes"), 0600)
		case "directory":
			err = os.Mkdir(path, 0700)
			if err == nil {
				err = os.WriteFile(filepath.Join(path, "owned-sentinel"), []byte("owned child"), 0600)
			}
		case "symlink":
			err = os.Symlink("missing-owned-target", path)
		default:
			err = errors.New("invalid finite authority fixture")
		}
	}()
	return w
}

func (w *linkQAAuthorityWait) join() {
	w.cancel()
	<-w.joined
}
