//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Reuse the existing checked terminal owner, registering even a partial Open
// owner before examining the error. This fixture has no subprocess or hook.
func cefQAOpen(t *testing.T, f interopFixture, create bool, mode sqliteio.Mode) *stQAOwner {
	t.Helper()
	c, err := sqliteio.Open(context.Background(), f.directory, f.database, sqliteio.Options{Create: create, ReadOnly: mode == sqliteio.Read, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	owner := &stQAOwner{t: t, c: c}
	t.Cleanup(owner.cleanup)
	if err == nil {
		owner.tx, err = c.Begin(context.Background(), mode)
	}
	if err != nil {
		cleanup := owner.finish(false)
		owner.reported = true
		t.Fatal("catalog fixture acquisition/checked cleanup", errors.Join(err, cleanup))
	}
	return owner
}

// The measured bytes come from the actual installed catalog and production SQL.
// Independently read all four TEXT fields before accepting the aggregate as an
// allocation input. All native owners are closed before measurement begins.
func cefQANativeCatalog(t *testing.T) string {
	t.Helper()
	f := interopLocation(t)
	wantMeta := interopMeta(f)
	w := cefQAOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(w.tx); err != nil {
		t.Fatal("actual embedded catalog install", err)
	}
	if err := sqliteInsertMeta(w.tx, wantMeta); err != nil {
		t.Fatal("actual catalog metadata", err)
	}
	stQAClose(t, w, true)
	r := cefQAOpen(t, f, false, sqliteio.Read)
	rows := cefQAProjection(t, r.tx)
	if len(rows) != len(sqliteCaptureCatalog) || !reflect.DeepEqual(rows, sqliteCaptureCatalog[:]) {
		t.Fatal("actual ordered four-TEXT catalog differs from fixed expected catalog")
	}
	raw := cefQAAggregate(t, r.tx)
	var decoded [][4]string
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil || !reflect.DeepEqual(decoded, rows) {
		t.Fatal("actual native aggregate differs from independent typed projection", err)
	}
	var canonical strings.Builder
	e := json.NewEncoder(&canonical)
	e.SetEscapeHTML(false)
	if err := e.Encode(rows); err != nil || raw != strings.TrimSuffix(canonical.String(), "\n") {
		t.Fatal("native aggregate does not use the expected exact JSON spelling", err)
	}
	gotMeta, err := sqliteReadCaptureSchema(r.tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(gotMeta, wantMeta) {
		t.Fatal("actual complete schema admission control", err)
	}
	stQAClose(t, r, false)
	if t.Failed() {
		t.Fatal("native catalog control failed before checker measurement")
	}
	t.Logf("native catalog rows=%d bytes=%d; all native owners checked closed", len(rows), len(raw))
	return raw
}

func cefQAProjection(t *testing.T, tx *sqliteio.Tx) [][4]string {
	t.Helper()
	s, err := tx.Prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name")
	if err != nil {
		t.Fatal("independent native catalog projection", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error("checked native projection finalize", err)
		}
	}()
	var rows [][4]string
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal("independent native catalog ROW/DONE", err)
		}
		if !row {
			return rows
		}
		if len(rows) >= len(sqliteCaptureCatalog) || s.ColumnCount() != 4 {
			t.Fatal("independent native catalog cardinality/width")
		}
		var values [4]string
		for i := range values {
			kind, err := s.Kind(i)
			if err != nil || kind != sqliteio.TextKind {
				t.Fatal("independent native catalog TEXT kind", i, kind, err)
			}
			values[i], err = s.Text(i)
			if err != nil {
				t.Fatal("owned independent native catalog TEXT", err)
			}
		}
		rows = append(rows, values)
	}
}

func cefQAAggregate(t *testing.T, tx *sqliteio.Tx) string {
	t.Helper()
	s, err := tx.Prepare(sqliteCaptureCatalogSQL, sqliteio.Integer(int64(len(sqliteCaptureCatalog)+1)))
	if err != nil {
		t.Fatal("actual aggregate prepare", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error("checked actual aggregate finalize", err)
		}
	}()
	row, err := s.Step()
	if err != nil || !row || s.ColumnCount() != 1 {
		t.Fatal("actual aggregate ROW/width", err)
	}
	kind, err := s.Kind(0)
	if err != nil || kind != sqliteio.TextKind {
		t.Fatal("actual aggregate TEXT kind", kind, err)
	}
	raw, err := s.Text(0)
	if err != nil {
		t.Fatal("owned actual aggregate TEXT", err)
	}
	if row, err = s.Step(); err != nil || row {
		t.Fatal("actual aggregate DONE", err)
	}
	return raw
}

func TestSQLiteCatalogExactNativeAggregateHasZeroCheckerAllocations(t *testing.T) {
	raw := cefQANativeCatalog(t)
	if err := sqliteCheckCaptureCatalogJSON(raw); err != nil {
		t.Fatal("actual native catalog acceptance control", err)
	}
	var checked error
	allocs := testing.AllocsPerRun(100, func() { checked = sqliteCheckCaptureCatalogJSON(raw) })
	t.Logf("checker allocations per actual native aggregate: %.0f", allocs)
	if checked != nil || allocs != 0 {
		t.Fatalf("exact native catalog checker allocations=%v want=0 error=%v", allocs, checked)
	}
}

func TestSQLiteCatalogExactFallbackPreservesAcceptedAndRejectedBytes(t *testing.T) {
	raw := cefQANativeCatalog(t)
	var indented bytes.Buffer
	if err := json.Indent(&indented, []byte(raw), "", "  "); err != nil {
		t.Fatal(err)
	}
	escaped := strings.Replace(raw, `"index"`, `"\u0069ndex"`, 1)
	if escaped == raw || indented.String() == raw {
		t.Fatal("alternate accepted encodings were not distinct")
	}
	for _, value := range []string{raw, " \n\t" + raw + "\r\n", indented.String(), escaped} {
		if err := sqliteCheckCaptureCatalogJSON(value); err != nil {
			t.Fatal("equivalent exact catalog JSON rejected", err)
		}
	}
	encode := func(rows [][]any) string {
		b, err := json.Marshal(rows)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	copyRows := func() [][]any {
		var rows [][]any
		if err := json.Unmarshal([]byte(raw), &rows); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	changedDDL := copyRows()
	changedDDL[0][3] = changedDDL[0][3].(string) + " "
	wrongKind := copyRows()
	wrongKind[0][0] = 7
	nullCell := copyRows()
	nullCell[0][3] = nil
	wrongOrder := copyRows()
	wrongOrder[0], wrongOrder[1] = wrongOrder[1], wrongOrder[0]
	wrongWidth := copyRows()
	wrongWidth[0] = append(wrongWidth[0], "extra")
	missing := copyRows()
	missing = missing[:len(missing)-1]
	extra := copyRows()
	extra = append(extra, extra[0])
	for name, value := range map[string]string{
		"foreign DDL byte": encode(changedDDL), "non-TEXT": encode(wrongKind),
		"NULL": encode(nullCell), "wrong order": encode(wrongOrder),
		"wrong width": encode(wrongWidth), "missing row": encode(missing), "extra row": encode(extra),
		"trailing token": raw + " true", "truncated": raw[:len(raw)-1],
		"invalid UTF-8": raw + string([]byte{0xff}), "null catalog": "null", "empty": "",
	} {
		t.Run(name, func(t *testing.T) {
			var domain *Error
			if err := sqliteCheckCaptureCatalogJSON(value); !errors.As(err, &domain) || domain.Code != "state_corrupt" {
				t.Fatal("invalid catalog was accepted or lost fixed refusal", err)
			}
		})
	}
}

func TestSQLiteCatalogExactSnapshotCannotAcceptChangedExpectedArray(t *testing.T) {
	// Nonparallel and native owners already closed: exercise only the pure
	// validator's existing package-local input, restoring on every exit.
	raw := cefQANativeCatalog(t)
	original := sqliteCaptureCatalog
	defer func() { sqliteCaptureCatalog = original }()
	sqliteCaptureCatalog[0][3] += " "
	var domain *Error
	if err := sqliteCheckCaptureCatalogJSON(raw); !errors.As(err, &domain) || domain.Code != "state_corrupt" {
		t.Fatal("stale canonical bytes accepted after expected catalog changed", err)
	}
	updated, err := json.Marshal(sqliteCaptureCatalog)
	if err != nil {
		t.Fatal(err)
	}
	if err = sqliteCheckCaptureCatalogJSON(string(updated)); err != nil {
		t.Fatal("changed expected array did not take conservative decoder", err)
	}
	sqliteCaptureCatalog = original
	if err = sqliteCheckCaptureCatalogJSON(raw); err != nil {
		t.Fatal("restored expected catalog rejected", err)
	}
}
