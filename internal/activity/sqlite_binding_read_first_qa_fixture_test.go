//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/harvest"
)

// Keep binding scopes outside the runner's repository-local TMPDIR. Every path
// and Git repository belongs to this one synthetic tree; no user path is used.
func brQAHome(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "tempo-sqlite-binding-read-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Error("owned fixture removal", err)
		}
	})
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}

func brQADirectory(t *testing.T, path string) string {
	t.Helper()
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func brQAID(n int) string { return fmt.Sprintf("bd000000-0000-4000-8000-%012d", n) }

func brQAService(t *testing.T, path string) *Service {
	t.Helper()
	return NewSQLite(Options{Path: path, Clock: ClockFunc(func() (ClockSample, error) {
		t.Fatal("binding read/link unexpectedly sampled activity clock")
		return ClockSample{}, failure("clock_unavailable")
	})})
}

func brQALocation(t *testing.T, path string) interopFixture {
	t.Helper()
	directory, authority, database, err := sqliteLocation(path)
	if err != nil {
		t.Fatal(err)
	}
	return interopFixture{directory: directory, authority: authority, database: database}
}

func brQAInput(path string, n int) LinkInput {
	return LinkInput{Path: path, AccountID: "1", ProjectID: "3", TaskID: "4", Timezone: "UTC", RequestID: brQAID(n)}
}

func brQALink(t *testing.T, s *Service, in LinkInput) BindingResult {
	t.Helper()
	r, err := s.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil || !r.Changed || !validUUID(r.Binding.ID) || r.Binding.AttachedActors == nil || len(r.Binding.AttachedActors) != 0 {
		t.Fatal("SETUP real public SQLite Link prerequisite", err)
	}
	return r
}

func brQAOffline(t *testing.T) LinkDependencies {
	return LinkDependencies{
		ResolveAccount: func(context.Context) (string, error) {
			t.Fatal("historical replay resolved an account")
			return "", nil
		},
		NewProvider: func(context.Context, string) (harvest.Provider, error) {
			t.Fatal("historical replay constructed a provider")
			return nil, nil
		},
	}
}

type brQASnapshot struct {
	meta     sqliteStoreMeta
	rows     map[string][][]string
	bindings map[string]sqliteBindingRow
	requests map[string]sqliteMutationRequestRow
}

// Use existing checked native ownership. This helper only reads real rows;
// Link is the sole writer used by these tests.
func brQARead(t *testing.T, f interopFixture, bindings []Binding, receipts []BindingResult) brQASnapshot {
	t.Helper()
	owner := stQAOpen(t, f, sqliteio.Read)
	defer owner.cleanup()
	m, err := sqliteReadMeta(owner.tx, f.authority, f.database)
	if err != nil {
		t.Fatal("cold binding meta", err)
	}
	r := brQASnapshot{meta: m, bindings: map[string]sqliteBindingRow{}, requests: map[string]sqliteMutationRequestRow{}}
	for _, want := range bindings {
		row, found, err := sqliteReadBinding(owner.tx, m.ComputerID, want.ID)
		if err != nil || !found || row.ComputerID != m.ComputerID || row.Record == nil || row.Record.Deleted || row.Record.Kind != want.Kind || row.Record.Locator != want.Locator || row.Snapshot.ID != want.ID || row.Snapshot.Revision != want.Revision || row.Snapshot.Attribution != want.Attribution {
			t.Fatal("cold typed binding differs from public write", err)
		}
		r.bindings[want.ID] = row
	}
	for _, want := range receipts {
		row, found, err := sqliteReadMutationRequestLocal(owner.tx, m.ComputerID, want.RequestID, m.Revision)
		if err != nil || !found || row.Value.Operation != "bindings.link" || !reflect.DeepEqual(row.Value.BindingResult, &want) {
			t.Fatal("cold historical Link receipt", err)
		}
		r.requests[want.RequestID] = row
	}
	var charge int64
	r.rows, charge = sgQAAudit(t, owner.tx, m)
	if len(r.rows) != 32 || charge != m.LogicalBytes {
		t.Fatal("cold literal row charge", len(r.rows), charge, m.LogicalBytes)
	}
	stQAClose(t, owner, false)
	return r
}

func brQAPrivate(t *testing.T, f interopFixture) {
	t.Helper()
	if _, err := os.Lstat(filepath.Join(f.directory, f.authority)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("read workflow created legacy authority", err)
	}
	dir, err := os.Lstat(f.directory)
	if err != nil || !dir.IsDir() || dir.Mode().Perm() != 0700 {
		t.Fatal("private state directory", err)
	}
	db, err := os.Lstat(filepath.Join(f.directory, f.database))
	if err != nil || !db.Mode().IsRegular() || db.Mode().Perm() != 0600 {
		t.Fatal("private SQLite main", err)
	}
}

func brQAList(t *testing.T, got BindingList, err error, revision string, bindings []Binding) {
	t.Helper()
	want := BindingList{ContractVersion: 1, SnapshotRevision: revision, Bindings: bindings}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("public binding read projection", err, got, want)
	}
}

func brQARefusal(t *testing.T, got BindingList, err error, code string) {
	t.Helper()
	var e *Error
	if !reflect.DeepEqual(got, BindingList{}) || !errors.As(err, &e) || e.Code != code || e.Uncertain {
		t.Fatal("public binding read refusal", code, err, got)
	}
}
