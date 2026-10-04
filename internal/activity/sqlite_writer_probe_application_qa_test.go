//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Setup enters the existing public Service Link with synthetic provider data.
// Only deliberate catalog-corruption fixtures perform direct SQL/file changes.
func waQALink(t *testing.T) interopFixture {
	t.Helper()
	s, in, f := flQAService(t)
	r, err := s.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil || r.RequestID != in.RequestID || !validUUID(r.Binding.ID) {
		t.Fatal("public SQLite Link control", err)
	}
	return f
}
func waQACapture(t *testing.T, f interopFixture, mode sqliteio.Mode) (*stQAOwner, sqliteStoreMeta, bool, error) {
	t.Helper()
	a := sqliteCaptureAdmission{Directory: f.directory, StateBasename: f.authority, DatabaseBasename: f.database, AcquireDeadline: time.Now().Add(250 * time.Millisecond)}
	c, tx, m, found, err := sqliteOpenCapture(context.Background(), a, mode)
	o := &stQAOwner{t: t, c: c, tx: tx}
	var retained *sqliteCaptureCleanupError
	if c == nil && errors.As(err, &retained) {
		o.c = retained.owner
	}
	t.Cleanup(o.cleanup)
	return o, m, found, err
}
func waQARefusal(t *testing.T, o *stQAOwner, m sqliteStoreMeta, found bool, err error) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != "state_corrupt" || e.Uncertain || found || o.c != nil || o.tx != nil || !reflect.DeepEqual(m, sqliteStoreMeta{}) {
		t.Fatal("full writer refusal lost classification/zero outputs/cleanup", found, err)
	}
	stQAClose(t, o, false)
}
func waQADurableBytes(t *testing.T, f interopFixture) [2][]byte {
	t.Helper()
	var image [2][]byte
	for i, suffix := range []string{"", "-wal"} {
		b, err := os.ReadFile(filepath.Join(f.directory, f.database+suffix))
		if errors.Is(err, os.ErrNotExist) && suffix != "" {
			continue
		}
		if err != nil {
			t.Fatal("closed durable family", err)
		}
		image[i] = b
	}
	return image
}

func TestSQLiteWriterProbeApplicationWALCatalogAndAllRowsPreserved(t *testing.T) {
	f := waQALink(t)
	beforeMeta, beforeRows := flQASnapshot(t, f)
	if len(beforeRows["catalog"]) != 85 || len(beforeRows) != 33 || len(beforeRows["bindings"]) != 1 || len(beforeRows["requests"]) != 1 {
		t.Fatal("public full retained state premise")
	}
	before := waQADurableBytes(t, f)
	if len(before[1]) <= 32 {
		t.Fatal("actual committed catalog WAL premise")
	}
	for _, mode := range []sqliteio.Mode{sqliteio.Read, sqliteio.Write} {
		o, m, found, err := waQACapture(t, f, mode)
		if err != nil || !found || o.c == nil || o.tx == nil || !reflect.DeepEqual(m, beforeMeta) {
			t.Fatal("complete admitted catalog/meta", mode, found, err)
		}
		if rows := flQASnapshotTx(t, o.tx); !reflect.DeepEqual(rows, beforeRows) {
			t.Fatal("WAL-visible complete logical state differs", mode)
		}
		stQAClose(t, o, false)
	}
	if after := waQADurableBytes(t, f); !reflect.DeepEqual(after, before) {
		t.Fatal("read/rolled-back writer changed main/WAL")
	}
	flQAUnchanged(t, f, beforeMeta, beforeRows)
	if _, err := os.Lstat(filepath.Join(f.directory, f.authority)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("authority created", err)
	}
}

func TestSQLiteWriterProbeApplicationRejectsForeignCatalogAndCorruptMetadata(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"extra-table", "CREATE TABLE owned_foreign(value TEXT) STRICT"},
		{"missing-index", "DROP INDEX binding_timer"},
		{"invalid-computer", "UPDATE store_meta SET computer_id='owned-invalid-computer'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := waQALink(t)
			// Positive writer control precedes this one deliberate mutation.
			control, _, found, err := waQACapture(t, f, sqliteio.Write)
			if err != nil || !found {
				t.Fatal("positive complete writer control", err)
			}
			stQAClose(t, control, false)
			w := cefQAOpen(t, f, false, sqliteio.Write)
			interopDone(t, w.tx, tc.sql)
			stQAClose(t, w, true)
			r := cefQAOpen(t, f, false, sqliteio.Read)
			beforeRows := flQASnapshotTx(t, r.tx)
			stQAClose(t, r, false)
			before := waQADurableBytes(t, f)
			o, m, ok, err := waQACapture(t, f, sqliteio.Write)
			waQARefusal(t, o, m, ok, err)
			if after := waQADurableBytes(t, f); !reflect.DeepEqual(after, before) {
				t.Fatal("catalog refusal changed main/WAL")
			}
			r = cefQAOpen(t, f, false, sqliteio.Read)
			afterRows := flQASnapshotTx(t, r.tx)
			stQAClose(t, r, false)
			if !reflect.DeepEqual(afterRows, beforeRows) {
				t.Fatal("foreign/corrupt refusal mutated catalog or one of32tables")
			}
		})
	}
	t.Run("occupied-authority", func(t *testing.T) {
		f := waQALink(t)
		meta, rows := flQASnapshot(t, f)
		path := filepath.Join(f.directory, f.authority)
		content := []byte("owned existing selector must survive")
		if err := os.WriteFile(path, content, 0600); err != nil {
			t.Fatal(err)
		}
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		before := waQADurableBytes(t, f)
		o, m, found, err := waQACapture(t, f, sqliteio.Write)
		var domain *Error
		if !errors.As(err, &domain) || domain.Code != "state_path_in_use" || found || o.c != nil || o.tx != nil || !reflect.DeepEqual(m, sqliteStoreMeta{}) {
			t.Fatal("writer P priority/zero outputs", err)
		}
		stQAClose(t, o, false)
		afterInfo, statErr := os.Lstat(path)
		afterContent, readErr := os.ReadFile(path)
		if statErr != nil || readErr != nil || !os.SameFile(info, afterInfo) || !bytes.Equal(afterContent, content) || !reflect.DeepEqual(waQADurableBytes(t, f), before) {
			t.Fatal("occupied authority/database modified", statErr, readErr)
		}
		// Raw checked cold oracle does not adopt or remove the occupied selector.
		flQAUnchanged(t, f, meta, rows)
	})
}

func TestSQLiteWriterProbeApplicationMalformedSchemaStillRefuses(t *testing.T) {
	f := waQALink(t)
	meta, rows := flQASnapshot(t, f)
	control, _, found, err := waQACapture(t, f, sqliteio.Write)
	if err != nil || !found {
		t.Fatal("positive writer before malformed schema", err)
	}
	stQAClose(t, control, false)
	// Checkpoint only in fixture construction, through the real checked native
	// API. The tested admission must not checkpoint or repair the malformed file.
	c, err := sqliteio.Open(context.Background(), f.directory, f.database, sqliteio.Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	cp := &stQAOwner{t: t, c: c}
	t.Cleanup(cp.cleanup)
	if err != nil {
		t.Fatal("fixture checkpoint open", err)
	}
	checkpoint, err := c.Checkpoint(context.Background(), sqliteio.Truncate)
	if err != nil || !checkpoint.Attempted || checkpoint.Code != 0 || !checkpoint.AfterValid || checkpoint.After.WAL != 0 {
		t.Fatal("actual complete fixture checkpoint", checkpoint, err)
	}
	stQAClose(t, cp, false)
	before := waQADurableBytes(t, f)
	needle := []byte("CREATE INDEX binding_timer ON bindings(")
	if bytes.Count(before[0], needle) != 1 || len(before[1]) != 0 {
		t.Fatal("one physically checkpointed schema record required")
	}
	index := bytes.Index(before[0], needle)
	broken := append([]byte(nil), before[0]...)
	copy(broken[index:index+6], []byte("BROKEN"))
	path := filepath.Join(f.directory, f.database)
	if err := os.WriteFile(path, broken, 0600); err != nil {
		t.Fatal("owned closed-file malformed catalog fixture", err)
	}
	brokenImage := waQADurableBytes(t, f)
	if !bytes.Equal(brokenImage[0], broken) || len(brokenImage[1]) != 0 {
		t.Fatal("malformed fixture bytes")
	}
	// First prove the new narrow probe actually reaches its qualified-WAL
	// success. A false physical-header premise must not mask later validation.
	probe, kind, err := sqliteio.InspectForCaptureWrite(context.Background(), f.directory, f.authority, f.database, time.Now().Add(250*time.Millisecond))
	probeOwner := &stQAOwner{t: t, c: probe}
	t.Cleanup(probeOwner.cleanup)
	if err != nil || kind != sqliteio.LinkWAL || probe == nil {
		t.Fatal("malformed catalog must reach cookie prerequisite", kind, err)
	}
	stQAClose(t, probeOwner, false)
	o, m, ok, err := waQACapture(t, f, sqliteio.Write)
	waQARefusal(t, o, m, ok, err)
	if after := waQADurableBytes(t, f); !reflect.DeepEqual(after, brokenImage) {
		t.Fatal("full writer repaired or changed malformed catalog")
	}
	// Restore only the fixture's six bytes after every owner conclusively closed.
	// Exact original bytes then permit an independent all-table cold comparison.
	if err := os.WriteFile(path, before[0], 0600); err != nil {
		t.Fatal("owned fixture restoration", err)
	}
	if restored := waQADurableBytes(t, f); !reflect.DeepEqual(restored, before) {
		t.Fatal("fixture restoration not exact")
	}
	flQAUnchanged(t, f, meta, rows)
}
