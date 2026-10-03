//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type snQAFixture struct {
	t             *testing.T
	f             interopFixture
	s             *Service
	sample        ClockSample
	interval      Interval
	configuration SyncConfiguration
}

func snQAID(n int) string { return fmt.Sprintf("cb000000-0000-4000-8000-%012d", n) }

func (q *snQAFixture) reopen() *Service {
	return NewSQLite(Options{Path: filepath.Join(q.f.directory, q.f.authority), Clock: ClockFunc(func() (ClockSample, error) { return q.sample, nil })})
}

// Setup is exclusively public operation composition. A failure here is a
// prerequisite failure, not a SyncNow behavioral RED or a seeded sync outcome.
func snQACaptured(t *testing.T) *snQAFixture {
	t.Helper()
	q := &snQAFixture{t: t, f: interopLocation(t), sample: stQAAt(0)}
	q.s = q.reopen()
	ctx := context.Background()
	linkInput := qaLinkInput(t)
	linked, err := q.s.Link(ctx, linkInput, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil || !linked.Changed || linked.SnapshotRevision != "1" {
		t.Fatal("SETUP public SQLite Link prerequisite", err)
	}
	initial, err := q.s.Status(ctx)
	if err != nil || initial.ComputerID == nil || !validUUID(*initial.ComputerID) {
		t.Fatal("SETUP public Status computer identity", err)
	}
	event := Event{ContractVersion: 1, Actor: ActorKey{ComputerID: *initial.ComputerID, Source: "manual-test", SessionID: "first-sync-workflow", AgentID: "root"}, Generation: "1", Sequence: "1", EventID: "first-sync/work/1", Kind: "work", BindingID: linked.Binding.ID, BindingRevision: linked.Binding.Revision, CWD: linkInput.Path}
	started, err := q.s.Ingest(ctx, event)
	if err != nil || started.Disposition != "applied" {
		t.Fatal("SETUP public normalized work", err)
	}
	event.BindingID, event.BindingRevision, event.CWD = "", "", ""
	q.sample = stQAAt(30)
	event.Sequence, event.EventID, event.Kind = "2", "first-sync/work/2", "observe_work"
	if r, err := q.s.Ingest(ctx, event); err != nil || r.Disposition != "applied" {
		t.Fatal("SETUP public normalized evidence", err)
	}
	q.sample = stQAAt(36)
	event.Sequence, event.EventID, event.Kind = "3", "first-sync/work/3", "finish"
	if r, err := q.s.Ingest(ctx, event); err != nil || r.Disposition != "applied" {
		t.Fatal("SETUP public normalized finish", err)
	}
	closed, err := q.s.Status(ctx)
	if err != nil || len(closed.ClosedIntervals) != 1 || closed.ClosedIntervals[0].DurationNS != "36000000000" || closed.Worker.QueuedCount != 1 || len(closed.Uncertainties) != 0 {
		t.Fatal("SETUP public captured interval/queued root", err)
	}
	q.interval = closed.ClosedIntervals[0]
	configInput := SyncConfigureInput{AccountID: "1", UserID: "2", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: snQAID(1), Confirmed: true}
	configured, err := q.s.SyncConfigure(ctx, configInput, qaSyncDeps(t, qaNewSyncProvider(t)))
	if err != nil || !configured.Changed || configured.Configuration.Revision != "1" {
		t.Fatal("SETUP public SQLite Configure prerequisite", err)
	}
	q.configuration = configured.Configuration
	enabled, err := q.s.SyncResume(ctx, snQAID(2))
	if err != nil || !enabled.Changed {
		t.Fatal("SETUP public SQLite Resume prerequisite", err)
	}
	before := snQARead(t, q)
	if before.item.State != "queued" || before.item.Plan != nil || before.item.RunRequestID != nil || before.item.RetryRequestID != nil || !reflect.DeepEqual(before.item.Interval, q.interval) || !before.meta.SyncEnabled {
		t.Fatal("SETUP actual unattempted queued graph")
	}
	return q
}

type snQASnapshot struct {
	meta     sqliteStoreMeta
	rows     map[string][][]string
	item     OutboxItem
	requests map[string]sqliteMutationRequestRow
}

// Existing checked owner joins rollback and at most two lifetime CloseChecked
// attempts even on Fatal. Cold reads use real typed graph and literal charge.
func snQARead(t *testing.T, q *snQAFixture, requestIDs ...string) snQASnapshot {
	t.Helper()
	owner := stQAOpen(t, q.f, sqliteio.Read)
	defer owner.cleanup()
	meta, err := sqliteReadMeta(owner.tx, q.f.authority, q.f.database)
	if err != nil {
		t.Fatal("cold meta", err)
	}
	root, found, err := sqliteReadOutboxLocal(owner.tx, meta.ComputerID, q.interval.ID)
	if err != nil || !found {
		t.Fatal("cold actual interval/root selector", err)
	}
	item, found, err := sqliteReadSyncItem(owner.tx, meta.ComputerID, root.ID, meta.Revision)
	if err != nil || !found {
		t.Fatal("cold complete typed sync item", err)
	}
	requests := map[string]sqliteMutationRequestRow{}
	selected := []string{}
	for _, id := range requestIDs {
		row, ok, err := sqliteReadMutationRequestLocal(owner.tx, meta.ComputerID, id, meta.Revision)
		if err != nil {
			t.Fatal("cold typed request", err)
		}
		if ok {
			requests[id] = row
			selected = append(selected, id)
		}
	}
	if err := sqliteValidateSelectedSync(owner.tx, meta.ComputerID, meta.Revision, []string{item.ID}, selected); err != nil {
		t.Fatal("cold selected sync closure", err)
	}
	rows, charge := sgQAAudit(t, owner.tx, meta)
	if len(rows) != 32 || charge != meta.LogicalBytes {
		t.Fatal("cold literal 32-table charge", len(rows), charge, meta.LogicalBytes)
	}
	stQAClose(t, owner, false)
	return snQASnapshot{meta: meta, rows: rows, item: item, requests: requests}
}

func snQANonceOnly(t *testing.T, before, after snQASnapshot) {
	t.Helper()
	nonce, err := sqliteNextNonce(before.meta.DurabilityNonce[:])
	if err != nil {
		t.Fatal(err)
	}
	want := before.meta
	want.DurabilityNonce = nonce
	if !reflect.DeepEqual(want, after.meta) || !reflect.DeepEqual(before.item, after.item) || !reflect.DeepEqual(before.requests, after.requests) {
		t.Fatal("replay changed semantic graph or lacked one nonce fence")
	}
	for table, rows := range before.rows {
		if table != "store_meta" && !reflect.DeepEqual(rows, after.rows[table]) {
			t.Fatal("replay changed retained table", table)
		}
	}
}

func snQAString(t *testing.T, got *string, want string) {
	t.Helper()
	if got == nil || *got != want {
		t.Fatal("typed optional value mismatch", want)
	}
}

func snQAClaim(t *testing.T, q *snQAFixture, id string) snQASnapshot {
	t.Helper()
	status, err := q.reopen().Status(context.Background())
	if err != nil || status.Worker.SubmittingCount != 1 {
		t.Fatal("POST retained SQL ownership or lacked public submitting fact", err)
	}
	claim := snQARead(t, q, id)
	item := claim.item
	if item.State != "submitting" || item.RunRequestID == nil || *item.RunRequestID != id || item.Plan == nil || len(item.Plan.Parts) != 1 || !reflect.DeepEqual(item.Interval, q.interval) {
		t.Fatal("POST without actual complete submitting claim")
	}
	part := item.Plan.Parts[0]
	if part.State != "submitting" || part.EntryID != nil || len(part.Attempts) != 1 || part.Attempts[0].State != "submitting" || part.Attempts[0].RequestID != id || part.Attempts[0].Number != "1" || !validUUID(part.Attempts[0].ID) {
		t.Fatal("POST without exact typed pending attempt")
	}
	request, found := claim.requests[id]
	if !found || request.Value.PendingSync == nil || request.Value.PendingSync.Run == nil || request.Value.PendingSync.Run.RequestID != id || request.Value.PendingSync.Run.Limit != 20 || !reflect.DeepEqual(request.Value.PendingSync.RootIDs, []string{item.ID}) || request.Value.SyncRun != nil {
		t.Fatal("POST without actual bounded pending request")
	}
	return claim
}
