//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func TestSQLiteFirstSyncStatusAbsentPublicReadDoesNotCreate(t *testing.T) {
	f := interopLocation(t)
	path := filepath.Join(f.directory, "absent", f.authority)
	clockCalls := 0
	zero := "0"
	want := SyncStatus{ContractVersion: 1, SnapshotRevision: "0", Configurations: []SyncConfiguration{}, Items: []OutboxItem{}, Accounting: []SyncAccounting{}, Worker: WorkerStatus{State: "not_installed"}, Totals: SyncTotals{ExactDurationNS: "0", PlannedDurationNS: &zero, ConfirmedDurationNS: &zero, PlannedResidualNS: &zero, TotalResidualNS: &zero}}
	for n := 0; n < 2; n++ {
		s := NewSQLite(Options{Path: path, Clock: ClockFunc(func() (ClockSample, error) {
			clockCalls++
			t.Error("SyncStatus sampled capture clock")
			return ClockSample{}, errors.New("forbidden capture clock")
		})})
		got, err := s.SyncStatus(context.Background())
		if err != nil || !reflect.DeepEqual(got, want) || clockCalls != 0 {
			t.Fatal("public absent SyncStatus must return the finite empty projection", err, got)
		}
		if _, err := os.Lstat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("absent SyncStatus created authority directory", err)
		}
	}
}

// All setup writes use public methods. These tests do not depend on SyncNow,
// import a legacy State, seed rows, or install a worker/provider service.
type ssQAFixture struct {
	t              *testing.T
	f              interopFixture
	sample         ClockSample
	clockCalls     int
	clockForbidden bool
	interval       Interval
	configuration  SyncConfiguration
	linkProvider   *qaLinkProvider
	syncProvider   *qaSyncProvider
}

const ssQAConfigureID = "ce000000-0000-4000-8000-000000000001"
const ssQAResumeID = "ce000000-0000-4000-8000-000000000002"
const ssQAPauseID = "ce000000-0000-4000-8000-000000000003"

func (q *ssQAFixture) service(observer WorkerObserver) *Service {
	return NewSQLite(Options{Path: filepath.Join(q.f.directory, q.f.authority), ObserveWorker: observer, Clock: ClockFunc(func() (ClockSample, error) {
		q.clockCalls++
		if q.clockForbidden {
			q.t.Error("SyncStatus/control sampled capture clock")
			return ClockSample{}, errors.New("forbidden capture clock")
		}
		return q.sample, nil
	})})
}

func ssQACaptured(t *testing.T) *ssQAFixture {
	t.Helper()
	q := &ssQAFixture{t: t, f: interopLocation(t), sample: stQAAt(0), linkProvider: qaNewLinkProvider(t), syncProvider: qaNewSyncProvider(t)}
	s, ctx := q.service(nil), context.Background()
	in := qaLinkInput(t)
	linked, err := s.Link(ctx, in, qaLinkDeps(t, q.linkProvider))
	if err != nil || !linked.Changed || linked.SnapshotRevision != "1" {
		t.Fatal("SETUP public SQLite Link prerequisite", err)
	}
	initial, err := s.Status(ctx)
	if err != nil || initial.ComputerID == nil || !validUUID(*initial.ComputerID) {
		t.Fatal("SETUP public Status identity prerequisite", err)
	}
	e := Event{ContractVersion: 1, Actor: ActorKey{ComputerID: *initial.ComputerID, Source: "manual-test", SessionID: "first-sync-status", AgentID: "root"}, Generation: "1", Sequence: "1", EventID: "first-sync-status/1", Kind: "work", BindingID: linked.Binding.ID, BindingRevision: linked.Binding.Revision, CWD: in.Path}
	if r, err := s.Ingest(ctx, e); err != nil || r.Disposition != "applied" || r.SnapshotRevision != "2" {
		t.Fatal("SETUP public normalized work at zero", err)
	}
	e.BindingID, e.BindingRevision, e.CWD = "", "", ""
	q.sample = stQAAt(30)
	e.Sequence, e.EventID, e.Kind = "2", "first-sync-status/2", "observe_work"
	if r, err := s.Ingest(ctx, e); err != nil || r.Disposition != "applied" || r.SnapshotRevision != "3" {
		t.Fatal("SETUP public normalized observe at thirty", err)
	}
	q.sample = stQAAt(36)
	e.Sequence, e.EventID, e.Kind = "3", "first-sync-status/3", "finish"
	if r, err := s.Ingest(ctx, e); err != nil || r.Disposition != "applied" || r.SnapshotRevision != "4" {
		t.Fatal("SETUP public normalized finish at thirty-six", err)
	}
	closed, err := s.Status(ctx)
	if err != nil || len(closed.ClosedIntervals) != 1 || closed.ClosedIntervals[0].DurationNS != "36000000000" || !closed.ClosedIntervals[0].Start.Equal(qaEpochStart) || !closed.ClosedIntervals[0].End.Equal(qaEpochStart.Add(36*time.Second)) || closed.Worker.QueuedCount != 1 || len(closed.Uncertainties) != 0 {
		t.Fatal("SETUP real public captured interval", err)
	}
	q.interval, q.clockForbidden = closed.ClosedIntervals[0], true
	configuration, err := s.SyncConfigure(ctx, SyncConfigureInput{AccountID: "1", UserID: "2", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: ssQAConfigureID, Confirmed: true}, qaSyncDeps(t, q.syncProvider))
	if err != nil || !configuration.Changed || configuration.SnapshotRevision != "5" || configuration.Configuration.Revision != "1" {
		t.Fatal("SETUP public Configure prerequisite", err)
	}
	q.configuration = configuration.Configuration
	if r, err := s.SyncResume(ctx, ssQAResumeID); err != nil || !r.Changed || r.SnapshotRevision != "6" {
		t.Fatal("SETUP public Resume prerequisite", err)
	}
	if !reflect.DeepEqual(q.linkProvider.calls, []string{"accounts", "/users/me", "/users/me/project_assignments"}) || !reflect.DeepEqual(q.syncProvider.calls, []string{"accounts", "/users/me"}) || len(q.syncProvider.posts) != 0 {
		t.Fatal("SETUP exceeded bounded read-only provider discovery")
	}
	return q
}

type ssQASnapshot struct {
	meta           sqliteStoreMeta
	rows           map[string][][]string
	item           OutboxItem
	configurations []SyncConfiguration
}

// An existing checked native owner reads the real graph and all literal rows.
// The complete metadata comparison includes the durability nonce.
func ssQARead(t *testing.T, q *ssQAFixture) ssQASnapshot {
	t.Helper()
	owner := stQAOpen(t, q.f, sqliteio.Read)
	defer owner.cleanup()
	m, err := sqliteReadMeta(owner.tx, q.f.authority, q.f.database)
	if err != nil {
		t.Fatal("cold metadata", err)
	}
	root, found, err := sqliteReadOutboxLocal(owner.tx, m.ComputerID, q.interval.ID)
	if err != nil || !found {
		t.Fatal("cold captured outbox root", err)
	}
	item, found, err := sqliteReadSyncItem(owner.tx, m.ComputerID, root.ID, m.Revision)
	if err != nil || !found {
		t.Fatal("cold complete typed sync graph", err)
	}
	configurations, err := sqliteSyncConfigurations(owner.tx)
	if err != nil {
		t.Fatal("cold saved configurations", err)
	}
	rows, charge := sgQAAudit(t, owner.tx, m)
	if len(rows) != 32 || charge != m.LogicalBytes {
		t.Fatal("cold literal row charge", len(rows), charge, m.LogicalBytes)
	}
	stQAClose(t, owner, false)
	return ssQASnapshot{meta: m, rows: rows, item: item, configurations: configurations}
}

func ssQAProjection(t *testing.T, q *ssQAFixture, got SyncStatus, cold ssQASnapshot, revision string, enabled bool) {
	t.Helper()
	if got.ContractVersion != 1 || got.SnapshotRevision != revision || got.Enabled != enabled || cold.meta.Revision != revision || cold.meta.SyncEnabled != enabled {
		t.Fatal("sync snapshot identity/revision/consent", got)
	}
	if got.Configurations == nil || !reflect.DeepEqual(got.Configurations, []SyncConfiguration{q.configuration}) || !reflect.DeepEqual(got.Configurations, cold.configurations) || got.Items == nil || !reflect.DeepEqual(got.Items, []OutboxItem{cold.item}) {
		t.Fatal("sync snapshot lost saved configuration or typed item")
	}
	item := got.Items[0]
	if !validUUID(item.ID) || item.ID == q.interval.ID || item.Revision != "1" || item.State != "queued" || item.Correlation != "tempo:"+q.interval.ID || item.Plan != nil || item.EntryID != nil || item.FailureCategory != nil || item.RunRequestID != nil || item.RetryRequestID != nil || !reflect.DeepEqual(item.Interval, q.interval) {
		t.Fatal("status invented submission, plan or immutable capture", item)
	}
	wantTotals := SyncTotals{ExactDurationNS: "36000000000"}
	if !reflect.DeepEqual(got.Totals, wantTotals) || got.Accounting == nil || len(got.Accounting) != 2 {
		t.Fatal("unplanned or unconfirmed time must remain null", got.Totals, got.Accounting)
	}
	seen := map[string]bool{}
	for _, a := range got.Accounting {
		if seen[a.Scope] || a.Attribution != q.interval.Attribution || !reflect.DeepEqual(a.Totals, wantTotals) {
			t.Fatal("project/day accounting duplicated or changed exact capture", a)
		}
		switch a.Scope {
		case "project":
			if a.Date != nil {
				t.Fatal("project accounting invented date")
			}
		case "day":
			if a.Date == nil || *a.Date != qaEpochStart.UTC().Format("2006-01-02") {
				t.Fatal("wrong UTC capture day")
			}
		default:
			t.Fatal("unexpected accounting scope", a.Scope)
		}
		seen[a.Scope] = true
	}
	if !reflect.DeepEqual(got.Worker, WorkerStatus{State: "not_installed", QueuedCount: 1, SyncEnabled: enabled}) {
		t.Fatal("worker projection disagrees with authoritative queue", got.Worker)
	}
}

func TestSQLiteFirstSyncStatusPublicCaptureReopenReadOnlyAndPause(t *testing.T) {
	q := ssQACaptured(t)
	before := ssQARead(t, q)
	clockCalls := q.clockCalls
	linkCalls, syncCalls := append([]string{}, q.linkProvider.calls...), append([]string{}, q.syncProvider.calls...)
	var ready SyncStatus
	for n := 0; n < 3; n++ {
		got, err := q.service(nil).SyncStatus(context.Background())
		if err != nil {
			t.Fatal("public cold SyncStatus", err)
		}
		ssQAProjection(t, q, got, before, "6", true)
		if n == 0 {
			ready = got
		} else if !reflect.DeepEqual(got, ready) {
			t.Fatal("cold repeated status drifted")
		}
	}
	if after := ssQARead(t, q); !reflect.DeepEqual(after, before) {
		t.Fatal("read-only status changed rows, nonce, revision or charge")
	}
	workerCalls := 0
	observed, err := q.service(func(_ context.Context, w WorkerStatus) WorkerStatus {
		workerCalls++
		if !reflect.DeepEqual(w, ready.Worker) {
			t.Fatal("observer did not receive snapshot queue")
		}
		installed := true
		w.State, w.Installed = "running", &installed
		w.QueuedCount, w.SubmittingCount, w.UnknownCount, w.SyncEnabled = 99, 98, 97, false
		return w
	}).SyncStatus(context.Background())
	wantObserved := ready
	installed := true
	wantObserved.Worker.State, wantObserved.Worker.Installed = "running", &installed
	if err != nil || workerCalls != 1 || !reflect.DeepEqual(observed, wantObserved) {
		t.Fatal("worker evidence overwrote authoritative accounting", err)
	}
	if after := ssQARead(t, q); !reflect.DeepEqual(after, before) {
		t.Fatal("worker decoration mutated durable state")
	}
	paused, err := q.service(nil).SyncPause(context.Background(), ssQAPauseID)
	if err != nil || !paused.Changed || paused.SnapshotRevision != "7" {
		t.Fatal("public Pause prerequisite", err)
	}
	pauseRows := ssQARead(t, q)
	if !reflect.DeepEqual(pauseRows.item, before.item) || !reflect.DeepEqual(pauseRows.configurations, before.configurations) {
		t.Fatal("Pause changed captured item or consent declaration")
	}
	for n := 0; n < 2; n++ {
		got, err := q.service(nil).SyncStatus(context.Background())
		if err != nil {
			t.Fatal("public paused SyncStatus", err)
		}
		ssQAProjection(t, q, got, pauseRows, "7", false)
		want := ready
		want.SnapshotRevision, want.Enabled, want.Worker.SyncEnabled = "7", false, false
		if !reflect.DeepEqual(got, want) {
			t.Fatal("paused view changed saved queue/accounting")
		}
	}
	if after := ssQARead(t, q); !reflect.DeepEqual(after, pauseRows) {
		t.Fatal("paused reads mutated durable state")
	}
	if q.clockCalls != clockCalls || !reflect.DeepEqual(q.linkProvider.calls, linkCalls) || !reflect.DeepEqual(q.syncProvider.calls, syncCalls) || len(q.syncProvider.posts) != 0 {
		t.Fatal("offline status/control sampled clock or accessed provider")
	}
	if _, err := os.Lstat(filepath.Join(q.f.directory, q.f.authority)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("public SQL workflow created JSON authority", err)
	}
}
