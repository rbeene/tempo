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
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func scvQAID(n int) string { return fmt.Sprintf("ca000000-0000-4000-8000-%012d", n) }
func scvQAService(t *testing.T, f interopFixture) *Service {
	t.Helper()
	// Keep the real default 250 ms admission budget. These methods must not
	// sample capture clocks.
	return NewSQLite(Options{Path: filepath.Join(f.directory, f.authority), Clock: ClockFunc(func() (ClockSample, error) {
		t.Fatal("configure/control sampled capture clock")
		return ClockSample{}, nil
	})})
}
func scvQABootstrap(t *testing.T) (*Service, interopFixture) {
	t.Helper()
	f := interopLocation(t)
	s := scvQAService(t, f)
	r, err := s.Link(context.Background(), qaLinkInput(t), qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil || r.SnapshotRevision != "1" || !r.Changed {
		t.Fatal("actual public SQLite first Link", r, err)
	}
	if _, err := os.Lstat(filepath.Join(f.directory, f.authority)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Link created legacy authority", err)
	}
	return s, f
}
func scvQAInput(n int) SyncConfigureInput {
	return SyncConfigureInput{AccountID: "1", UserID: "2", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: scvQAID(n), Confirmed: true}
}
func scvQAReader(t *testing.T, f interopFixture) (*sqliteio.Conn, *sqliteio.Tx) {
	t.Helper()
	c, err := sqliteio.Open(context.Background(), f.directory, f.database, sqliteio.Options{ReadOnly: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	var tx *sqliteio.Tx
	if c != nil {
		t.Cleanup(func() {
			if tx != nil {
				if err := tx.Rollback(); err != nil {
					t.Error("cold rollback cleanup", err)
				}
			}
			if terminal, err := c.CloseChecked(context.Background()); !terminal || err != nil {
				t.Error("cold checked close", terminal, err)
			}
		})
	}
	if err != nil || c == nil {
		t.Fatal("actual cold Open", err)
	}
	tx, err = c.Begin(context.Background(), sqliteio.Read)
	if err != nil {
		t.Fatal("actual cold Begin", err)
	}
	return c, tx
}
func scvQAEnd(t *testing.T, c *sqliteio.Conn, tx *sqliteio.Tx) {
	t.Helper()
	if err := tx.Rollback(); err != nil {
		t.Fatal("cold rollback", err)
	}
	if terminal, err := c.CloseChecked(context.Background()); !terminal || err != nil {
		t.Fatal("cold terminal close", terminal, err)
	}
}

type scvQASnapshot struct {
	meta     sqliteStoreMeta
	rows     map[string][][]string
	configs  []SyncConfiguration
	requests map[string]sqliteMutationRequestRow
}

func scvQAAudit(t *testing.T, f interopFixture, ids ...string) scvQASnapshot {
	t.Helper()
	c, tx := scvQAReader(t, f)
	m, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil {
		t.Fatal("cold metadata", err)
	}
	rows, charge := sgQAAudit(t, tx, m)
	if len(rows) != 32 || charge != m.LogicalBytes {
		t.Fatal("actual 32-table materialized charge", len(rows), charge, m.LogicalBytes)
	}
	configs, err := sqliteSyncConfigurations(tx)
	if err != nil {
		t.Fatal("cold configuration", err)
	}
	requests := map[string]sqliteMutationRequestRow{}
	for _, id := range ids {
		r, found, err := sqliteReadMutationRequestLocal(tx, m.ComputerID, id, m.Revision)
		if err != nil {
			t.Fatal("cold request", err)
		}
		if found {
			requests[id] = r
		}
	}
	scvQAEnd(t, c, tx)
	return scvQASnapshot{m, rows, configs, requests}
}
func scvQAError(t *testing.T, got any, zero any, err error, code, request string) {
	t.Helper()
	var e *Error
	if !reflect.DeepEqual(got, zero) || !errors.As(err, &e) || e.Code != code {
		t.Fatalf("expected zero result/%s: got=%+v err=%v", code, got, err)
	}
	if code == "local_write_unknown" && (!e.Uncertain || e.Details["request_id"] != request || len(e.Details) != 1) {
		t.Fatal("unknown lost safe request identity", e)
	}
}
func scvQAUnchanged(t *testing.T, before, after scvQASnapshot) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("refusal changed cold rows, nonce, charge, or result projection")
	}
}
func scvQANonceOnly(t *testing.T, before, after scvQASnapshot) {
	t.Helper()
	next, err := sqliteNextNonce(before.meta.DurabilityNonce[:])
	if err != nil || after.meta.DurabilityNonce != next {
		t.Fatal("historical replay did not make exactly one real nonce fence", err)
	}
	want := before.meta
	want.DurabilityNonce = next
	if !reflect.DeepEqual(want, after.meta) || !reflect.DeepEqual(before.configs, after.configs) || !reflect.DeepEqual(before.requests, after.requests) {
		t.Fatal("nonce replay changed semantic metadata/configuration/receipt")
	}
	for table, rows := range before.rows {
		if table != "store_meta" && !reflect.DeepEqual(rows, after.rows[table]) {
			t.Fatal("nonce replay changed semantic rows", table)
		}
	}
}
