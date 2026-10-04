//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

type rpQATrace struct {
	queries []string
	deny    string
	denied  int
}

var rpQAObservation struct {
	sync.Mutex
	trace *rpQATrace
}

// Use the existing native callback-value slot, preserving the permanent
// authorizer's decision. The observer never calls SQLite or invents a result.
func rpQAAuthorize(tls *libc.TLS, cell uintptr, action int32, a, b, c, d uintptr) int32 {
	decision := authorize(tls, cell, action, a, b, c, d)
	if decision != lib.SQLITE_OK || action != lib.SQLITE_PRAGMA || a == 0 {
		return decision
	}
	name := libc.GoString(a)
	switch name {
	case "page_size", "page_count", "max_page_count", "journal_size_limit":
	default:
		return decision
	}
	query := name
	if b != 0 {
		query += "=" + libc.GoString(b)
	}
	rpQAObservation.Lock()
	defer rpQAObservation.Unlock()
	if trace := rpQAObservation.trace; trace != nil {
		trace.queries = append(trace.queries, query)
		if b == 0 && name == trace.deny {
			trace.denied++
			return lib.SQLITE_DENY
		}
	}
	return decision
}

func rpQAWatch(t *testing.T, deny string) (*rpQATrace, func()) {
	t.Helper()
	prior := authorizerValue
	trace := &rpQATrace{deny: deny}
	rpQAObservation.Lock()
	rpQAObservation.trace = trace
	rpQAObservation.Unlock()
	authorizerValue = rpQAAuthorize
	stop := func() {
		authorizerValue = prior
		rpQAObservation.Lock()
		rpQAObservation.trace = nil
		rpQAObservation.Unlock()
	}
	t.Cleanup(stop)
	return trace, stop
}

func rpQASnapshot(trace *rpQATrace) ([]string, int) {
	rpQAObservation.Lock()
	defer rpQAObservation.Unlock()
	return append([]string(nil), trace.queries...), trace.denied
}

func rpQAOpen(t *testing.T, ctx context.Context, dir string, opts Options) (*swQAOwner, error) {
	t.Helper()
	c, err := Open(ctx, dir, qaBasename, opts)
	o := &swQAOwner{c: c}
	// Register a returned owner even when Open reports an error.
	t.Cleanup(func() {
		if e := o.close(); e != nil {
			t.Error("page-policy checked cleanup", e)
		}
	})
	return o, err
}

func rpQASeed(t *testing.T, dir string) {
	t.Helper()
	ctx := context.Background()
	o, err := rpQAOpen(t, ctx, dir, Options{Create: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	if err != nil {
		t.Fatal("native seed open", errors.Join(err, o.close()))
	}
	o.tx, err = o.c.Begin(ctx, Write)
	if err != nil {
		t.Fatal("native seed begin", errors.Join(err, o.close()))
	}
	qaDone(t, o.tx, "CREATE TABLE rp_items(id INTEGER PRIMARY KEY, body BLOB NOT NULL) STRICT")
	qaDone(t, o.tx, "INSERT INTO rp_items VALUES(1,?)", Blob(bytes.Repeat([]byte{0x71}, 16384)))
	qaCommit(t, o.tx)
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
}

// These byte reads occur only after every owned native connection is closed.
func rpQAImage(t *testing.T, dir string) [2][]byte {
	t.Helper()
	if s := statsForTest(); s.Active != 0 || s.NativeActive != 0 {
		t.Fatal("raw image requires no live native owner", s)
	}
	var image [2][]byte
	for i, suffix := range []string{"", "-wal"} {
		data, err := os.ReadFile(filepath.Join(dir, qaBasename+suffix))
		if err != nil {
			t.Fatal("closed native image", err)
		}
		image[i] = data
	}
	if len(image[1]) == 0 {
		t.Fatal("seed did not retain real committed WAL")
	}
	return image
}

func TestSQLiteReadonlyOpenReadsEachPagePolicyFieldOnce(t *testing.T) {
	for _, tc := range []struct {
		name     string
		readonly bool
		limit    int64
	}{
		{"readonly", true, 0}, {"readonly-oversize", true, 1}, {"writable-control", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := qaDirectory(t)
			rpQASeed(t, dir)
			before := rpQAImage(t, dir)
			trace, stop := rpQAWatch(t, "")
			deadline := time.Now().Add(250 * time.Millisecond)
			o, err := rpQAOpen(t, context.Background(), dir, Options{ReadOnly: tc.readonly, AcquireDeadline: deadline, testMaxPages: tc.limit})
			got, denied := rpQASnapshot(trace)
			stop()
			if err != nil {
				t.Fatal("real cold native open", errors.Join(err, o.close()))
			}
			if o.c.readOnly != tc.readonly || o.c.acquireDeadline != deadline || denied != 0 {
				t.Fatal("original admission or native decision changed", denied)
			}
			o.tx, err = o.c.Begin(context.Background(), Read)
			if err != nil {
				t.Fatal("real read admission", errors.Join(err, o.close()))
			}
			info, err := o.tx.PageInfo()
			if err != nil || info.PageSize != 4096 || info.PageCount <= 1 || info.MaxPages < info.PageCount || info.MaxPages <= 0 {
				t.Fatal("actual four-field policy control", info, err)
			}
			if o.c.oversize != (tc.limit == 1) {
				t.Fatal("actual oversize classification", o.c.oversize, info)
			}
			if !tc.readonly && (info.MaxPages != 65536 || info.JournalLimitBytes != 4194304) {
				t.Fatal("writable policy changed", info)
			}
			if n := capacityQAScalar(t, o.tx, "SELECT length(body) FROM rp_items WHERE id=1"); n != 16384 {
				t.Fatal("retained native row changed", n)
			}
			qaCommit(t, o.tx)
			capacityQAApplicationAuth(t, o.c)
			if err := o.close(); err != nil {
				t.Fatal(err)
			}
			if after := rpQAImage(t, dir); !reflect.DeepEqual(after, before) {
				t.Fatal("policy observation rewrote retained main/WAL")
			}
			want := []string{"page_size", "page_count", "max_page_count", "journal_size_limit"}
			if !tc.readonly {
				want = []string{"page_count", "page_size", "max_page_count=65536", "journal_size_limit=4194304", "page_size", "page_count", "max_page_count", "journal_size_limit"}
			}
			t.Logf("actual authorized policy controls=%v", got)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("native policy work got=%v want=%v", got, want)
			}
		})
	}
}

func TestSQLiteReadonlyPagePolicyRequiresEveryNativeField(t *testing.T) {
	for _, field := range []string{"page_size", "page_count", "max_page_count", "journal_size_limit"} {
		t.Run(field, func(t *testing.T) {
			dir := qaDirectory(t)
			rpQASeed(t, dir)
			before := rpQAImage(t, dir)
			// Successful native control precedes the fault on the same store.
			control, err := rpQAOpen(t, context.Background(), dir, Options{ReadOnly: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			if err != nil {
				t.Fatal("positive readonly control", errors.Join(err, control.close()))
			}
			if err := control.close(); err != nil {
				t.Fatal(err)
			}
			trace, stop := rpQAWatch(t, field)
			o, openErr := rpQAOpen(t, context.Background(), dir, Options{ReadOnly: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			got, denied := rpQASnapshot(trace)
			closeErr := o.close()
			stop()
			var checked *Error
			if denied != 1 || !errors.As(openErr, &checked) || checked.Phase != OpenPhase || checked.Category != Misuse || checked.Code != lib.SQLITE_AUTH || closeErr != nil {
				t.Fatal("required native field was skipped or fault was not actual AUTH", got, denied, openErr, closeErr)
			}
			qaSafeError(t, openErr, dir, qaBasename, "rp_items")
			if after := rpQAImage(t, dir); !reflect.DeepEqual(after, before) {
				t.Fatal("failed policy read changed retained main/WAL")
			}
		})
	}
}

func TestSQLiteReadonlyPagePolicyRejectsActualForeignPageSize(t *testing.T) {
	dir := qaDirectory(t)
	qaCreate8192PageFixture(t, dir, qaBasename)
	before := rpQAImage(t, dir)
	o, openErr := rpQAOpen(t, context.Background(), dir, Options{ReadOnly: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	closeErr := o.close()
	var checked *Error
	if !errors.As(openErr, &checked) || checked.Category != Unsafe || closeErr != nil {
		t.Fatal("native 8192-page readonly policy refusal", openErr, closeErr)
	}
	qaSafeError(t, openErr, dir, qaBasename, "incompatible_page_fixture")
	if after := rpQAImage(t, dir); !reflect.DeepEqual(after, before) {
		t.Fatal("foreign-page refusal rewrote retained main/WAL")
	}
}
