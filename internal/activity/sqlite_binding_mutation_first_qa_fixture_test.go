//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type bmQAFixture struct {
	t                  *testing.T
	ctx                context.Context
	home, path, scope  string
	f                  interopFixture
	input              LinkInput
	linked             BindingResult
	bindings, requests []string
	actor              *ActorKey
	intervalID         string
	sample             ClockSample
	clockErr           error
	clockAllowed       bool
}

func bmQAID(n int) string { return fmt.Sprintf("bf000000-0000-4000-8000-%012d", n) }

func (q *bmQAFixture) service() *Service {
	return NewSQLite(Options{Path: q.path, LockTimeout: sqliteFlowTestLockTimeout(), Clock: ClockFunc(func() (ClockSample, error) {
		if !q.clockAllowed {
			q.t.Error("binding mutation/read/replay sampled capture clock")
			return ClockSample{}, errors.New("forbidden binding clock")
		}
		return q.sample, q.clockErr
	})})
}

func bmQANew(t *testing.T) *bmQAFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	q := &bmQAFixture{t: t, ctx: ctx, home: brQAHome(t), sample: stQAAt(0)}
	q.path = filepath.Join(q.home, "private", "activity-state.json")
	q.scope = brQADirectory(t, filepath.Join(q.home, "project"))
	q.f = brQALocation(t, q.path)
	q.input = LinkInput{Path: q.scope, AccountID: "1", ProjectID: "3", TaskID: "4", Timezone: "UTC", RequestID: bmQAID(1)}
	var err error
	q.linked, err = q.service().Link(ctx, q.input, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil || !q.linked.Changed || q.linked.SnapshotRevision != "1" || q.linked.Binding.Kind != "directory" || q.linked.Binding.Revision != "1" || !validUUID(q.linked.Binding.ID) {
		t.Fatal("SETUP public SQLite Link", err)
	}
	q.bindings, q.requests = []string{q.linked.Binding.ID}, []string{q.input.RequestID}
	return q
}

type bmQASnapshot struct {
	meta     sqliteStoreMeta
	rows     map[string][][]string
	bindings map[string]sqliteBindingRow
	requests map[string]sqliteMutationRequestRow
	actor    sqliteActorLocalRow
	item     OutboxItem
}

// Reads only: public Link/Ingest/mutations are the sole fixture writers.
func bmQARead(t *testing.T, q *bmQAFixture) bmQASnapshot {
	t.Helper()
	o := stQAOpen(t, q.f, sqliteio.Read)
	defer o.cleanup()
	m, err := sqliteReadMeta(o.tx, q.f.authority, q.f.database)
	if err != nil {
		t.Fatal("cold binding metadata", err)
	}
	r := bmQASnapshot{meta: m, bindings: map[string]sqliteBindingRow{}, requests: map[string]sqliteMutationRequestRow{}}
	for _, id := range q.bindings {
		row, found, err := sqliteReadBinding(o.tx, m.ComputerID, id)
		if err != nil || !found || row.Record == nil {
			t.Fatal("cold retained typed binding", err)
		}
		r.bindings[id] = row
	}
	for _, id := range q.requests {
		row, found, err := sqliteReadMutationRequestLocal(o.tx, m.ComputerID, id, m.Revision)
		if err != nil || !found {
			t.Fatal("cold retained typed request", err)
		}
		r.requests[id] = row
	}
	if q.actor != nil {
		var found bool
		r.actor, found, err = sqliteReadActorLocal(o.tx, m.ComputerID, *q.actor)
		if err != nil || !found {
			t.Fatal("cold captured actor", err)
		}
	}
	if q.intervalID != "" {
		root, found, err := sqliteReadOutboxLocal(o.tx, m.ComputerID, q.intervalID)
		if err != nil || !found {
			t.Fatal("cold captured outbox", err)
		}
		r.item, found, err = sqliteReadSyncItem(o.tx, m.ComputerID, root.ID, m.Revision)
		if err != nil || !found {
			t.Fatal("cold complete captured graph", err)
		}
	}
	var charge int64
	r.rows, charge = sgQAAudit(t, o.tx, m)
	if len(r.rows) != 32 || charge != m.LogicalBytes {
		t.Fatal("literal charge differs from metadata", charge, m.LogicalBytes)
	}
	stQAClose(t, o, false)
	return r
}

func bmQACapture(t *testing.T, q *bmQAFixture, state string) {
	t.Helper()
	initial := bmQARead(t, q)
	key := ActorKey{ComputerID: initial.meta.ComputerID, Source: "manual-test", SessionID: "binding-mutation", AgentID: "root"}
	e := Event{ContractVersion: 1, Actor: key, Generation: "1", Sequence: "1", EventID: "binding-mutation/1", Kind: "work", BindingID: q.linked.Binding.ID, BindingRevision: "1", CWD: q.scope}
	q.clockAllowed = true
	if r, err := q.service().Ingest(q.ctx, e); err != nil || r.Disposition != "applied" {
		t.Fatal("SETUP public work", err)
	}
	q.actor = &key
	e.BindingID, e.BindingRevision, e.CWD = "", "", ""
	if state != "working" {
		q.sample = stQAAt(10)
		e.Sequence, e.EventID, e.Kind = "2", "binding-mutation/2", "wait_user"
		if state == "finished" {
			q.sample, e.Kind = stQAAt(30), "observe_work"
		} else if state == "stale" {
			e.Kind, q.clockErr = "finish", errors.New("synthetic unavailable clock")
		}
		r, err := q.service().Ingest(q.ctx, e)
		if state == "stale" {
			qaCode(t, err, "clock_unavailable")
		} else if err != nil || r.Disposition != "applied" {
			t.Fatal("SETUP public next evidence", err)
		}
	}
	if state == "finished" {
		q.sample = stQAAt(36)
		e.Sequence, e.EventID, e.Kind = "3", "binding-mutation/3", "finish"
		if r, err := q.service().Ingest(q.ctx, e); err != nil || r.Disposition != "applied" {
			t.Fatal("SETUP public finish", err)
		}
		status, err := q.service().Status(q.ctx)
		if err != nil || len(status.ClosedIntervals) != 1 || status.ClosedIntervals[0].DurationNS != "36000000000" || len(status.Uncertainties) != 0 {
			t.Fatal("SETUP real immutable interval", err)
		}
		q.intervalID = status.ClosedIntervals[0].ID
	}
	q.clockAllowed = false
	got := bmQARead(t, q)
	wantState, wantHealth := state, "continuous"
	if state == "stale" {
		wantState, wantHealth = "working", "stale"
	}
	if got.actor.State != wantState || got.actor.Health != wantHealth || got.actor.BindingID != q.linked.Binding.ID || got.actor.BindingRevision != "1" || got.actor.Attribution != q.linked.Binding.Attribution {
		t.Fatal("SETUP actual attached/history actor", got.actor)
	}
	if state == "stale" && len(got.rows["uncertainties"]) != 1 {
		t.Fatal("SETUP stale evidence was not persisted")
	}
	if state == "finished" && (got.item.State != "queued" || got.item.Interval.DurationNS != "36000000000" || got.item.Interval.Attribution != q.linked.Binding.Attribution || len(got.item.Interval.SegmentIDs) != 1) {
		t.Fatal("SETUP complete typed immutable output", got.item)
	}
}

func bmQARefusal(t *testing.T, got, zero any, err error, code string) {
	t.Helper()
	var e *Error
	if !reflect.DeepEqual(got, zero) || !errors.As(err, &e) || e.Code != code || e.Uncertain {
		t.Fatal("public mutation refusal", code, err, got)
	}
}

func bmQAUnchanged(t *testing.T, q *bmQAFixture, before bmQASnapshot) {
	t.Helper()
	if !reflect.DeepEqual(bmQARead(t, q), before) {
		t.Fatal("refusal/read changed rows, nonce or public counters")
	}
}

func bmQAFence(t *testing.T, before, after bmQASnapshot, replay bool) {
	t.Helper()
	want := before.meta
	var err error
	want.DurabilityNonce, err = sqliteNextNonce(before.meta.DurabilityNonce[:])
	if err != nil {
		t.Fatal(err)
	}
	if !replay {
		want.Revision, want.LogicalBytes = bump(before.meta.Revision), after.meta.LogicalBytes
	}
	if !reflect.DeepEqual(want, after.meta) {
		t.Fatal("mutation/replay counter or nonce mismatch", before.meta, after.meta)
	}
	for table, rows := range before.rows {
		if table == "store_meta" || !replay && (table == "bindings" || table == "requests") {
			continue
		}
		if !reflect.DeepEqual(rows, after.rows[table]) {
			t.Fatal("operation rewrote immutable/unrelated rows", table)
		}
	}
	for id, row := range before.requests {
		if !reflect.DeepEqual(row, after.requests[id]) {
			t.Fatal("historical request changed", id)
		}
	}
	if !replay && len(after.rows["requests"]) != len(before.rows["requests"])+1 {
		t.Fatal("new mutation omitted or duplicated receipt")
	}
	if !reflect.DeepEqual(before.actor, after.actor) || !reflect.DeepEqual(before.item, after.item) {
		t.Fatal("binding operation rewrote captured identity/attribution")
	}
}

func bmQABinding(t *testing.T, got sqliteBindingRow, want Binding, deleted bool) {
	t.Helper()
	if got.Record == nil || got.Record.Deleted != deleted || got.Snapshot != (BindingSnapshot{ID: want.ID, Revision: want.Revision, Attribution: want.Attribution}) || got.Record.Kind != want.Kind || got.Record.Locator != want.Locator {
		t.Fatal("cold binding identity/locator/tombstone", got, want)
	}
}
