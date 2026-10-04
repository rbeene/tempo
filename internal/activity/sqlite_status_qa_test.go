//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent source-only behavior tests for the inactive normalized SQLite
// status port. Events here are normalized unit inputs, not host delivery proof.
import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/harvest"
)

func TestSQLiteStatusQAAbsentNoncreatingMachineSnapshot(t *testing.T) {
	f := interopLocation(t)
	path := filepath.Join(f.directory, f.authority)
	calls := 0
	s := New(Options{Path: path, Clock: ClockFunc(func() (ClockSample, error) { calls++; return stQAAt(4), nil })})
	got, err := s.statusSQLite(context.Background())
	if err != nil || got.ContractVersion != 1 || got.ComputerID != nil || got.SnapshotRevision != "0" || calls != 1 {
		t.Fatal("absent status identity/clock", err, got)
	}
	if got.Projects == nil || got.ProjectTimers == nil || got.Actors == nil || got.Uncertainties == nil || got.CaptureReviews == nil || got.ClosedIntervals == nil || len(got.Projects)+len(got.ProjectTimers)+len(got.Actors)+len(got.Uncertainties)+len(got.CaptureReviews)+len(got.ClosedIntervals) != 0 {
		t.Fatal("absent machine arrays", got)
	}
	if got.Worker.State != "not_installed" || got.Worker.SyncEnabled || got.SyncEnabled || !got.ObservedAt.Equal(stQAAt(4).WallUTC) {
		t.Fatal("absent offline snapshot fields", got)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"projects", "project_timers", "actors", "uncertainties", "capture_reviews", "closed_intervals"} {
		if string(fields[name]) != "[]" {
			t.Fatalf("%s must encode as []: %s", name, fields[name])
		}
	}
	if _, err = os.Stat(filepath.Join(f.directory, f.database)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("absent status created native database", err)
	}
}

func TestSQLiteStatusQAFreshLinkEmptyAndColdReadOnly(t *testing.T) {
	q := stQALinked(t)
	before, rows := stQAAudit(t, q.f)
	providerCalls := len(q.provider.calls)
	q.at(10)
	got := q.status()
	if got.ComputerID == nil || *got.ComputerID != q.computer || got.SnapshotRevision != before.Revision || !got.ObservedAt.Equal(q.sample.WallUTC) || len(got.Projects)+len(got.ProjectTimers)+len(got.Actors)+len(got.ClosedIntervals) != 0 {
		t.Fatal("linked empty status", got)
	}
	if len(q.provider.calls) != providerCalls {
		t.Fatal("status called external account provider")
	}
	stQAUnchanged(t, q.f, before, rows)
}

func TestSQLiteStatusQAOverlappingParentChildOneUnionAndClockOnlyRefresh(t *testing.T) {
	q := stQALinked(t)
	q.service.store.timeout = sqliteFlowTestLockTimeout() // Prepare the parent/child graph under race.
	b := q.bindings[q.first.Binding.ID]
	parent := q.event("P", "1", "work", b)
	q.ingest(0, parent)
	child := q.event("C", "1", "work", BindingSnapshot{})
	ref := ActorRef{Key: parent.Actor, Generation: "1"}
	child.Parent = &ref
	q.ingest(10, child)
	q.ingest(20, q.event("P", "2", "finish", BindingSnapshot{}))
	before, rows := stQAAudit(t, q.f)
	q.at(30)
	got := q.status()
	timer := qaProjectTimer(t, got, "1", "3")
	qaTimerDurations(t, timer, 30, 0)
	if len(got.ProjectTimers) != 1 || len(timer.ActiveActorRefs) != 1 || timer.ActiveActorRefs[0].Key.AgentID != "C" || len(got.ClosedIntervals) != 0 {
		t.Fatal("parent finish stopped child or split union", got)
	}
	if !got.ObservedAt.Equal(q.sample.WallUTC) {
		t.Fatal("status sample time")
	}
	stQAUnchanged(t, q.f, before, rows)
	q.at(40)
	later := q.status()
	qaTimerDurations(t, qaProjectTimer(t, later, "1", "3"), 40, 0)
	if later.SnapshotRevision != got.SnapshotRevision || !later.ObservedAt.After(got.ObservedAt) {
		t.Fatal("clock-only observation mutated public revision")
	}
	stQAUnchanged(t, q.f, before, rows)
}

func TestSQLiteStatusQASeparateProjectsAndStableOrder(t *testing.T) {
	q := stQALinked(t)
	b := q.bindings[q.first.Binding.ID]
	in := qaLinkInput(t)
	in.ProjectID = "9"
	in.RequestID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	p := qaNewLinkProvider(t)
	p.assignments[0]["project"] = harvest.Object{"id": json.Number("9")}
	second, err := q.service.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
	if err != nil || !second.Changed {
		t.Fatal("actual second project link", err)
	}
	other := BindingSnapshot{ID: second.Binding.ID, Revision: second.Binding.Revision, Attribution: second.Binding.Attribution}
	q.bindings[other.ID] = other
	q.ingest(0, q.event("A", "1", "work", b))
	q.ingest(10, q.event("B", "1", "work", other))
	q.at(30)
	before, rows := stQAAudit(t, q.f)
	got := q.status()
	if len(got.ProjectTimers) != 2 || got.ProjectTimers[0].ProjectID != "3" || got.ProjectTimers[1].ProjectID != "9" {
		t.Fatal("project partition/order", got.ProjectTimers)
	}
	qaTimerDurations(t, qaProjectTimer(t, got, "1", "3"), 30, 0)
	qaTimerDurations(t, qaProjectTimer(t, got, "1", "9"), 20, 0)
	stQAUnchanged(t, q.f, before, rows)
}

func TestSQLiteStatusQAWaitingAndClockDiscontinuityDoNotInventTime(t *testing.T) {
	t.Run("waiting", func(t *testing.T) {
		q := stQALinked(t)
		b := q.bindings[q.first.Binding.ID]
		q.ingest(0, q.event("A", "1", "work", b))
		q.ingest(10, q.event("A", "2", "wait_user", BindingSnapshot{}))
		q.at(20)
		before, rows := stQAAudit(t, q.f)
		first := q.status()
		q.at(1000)
		later := q.status()
		a := qaProjectTimer(t, later, "1", "3")
		if len(a.WaitingActorRefs) != 1 || len(a.ActiveActorRefs) != 0 || !reflect.DeepEqual(first.ProjectTimers, later.ProjectTimers) {
			t.Fatal("waiting actor accrued silent time", first, later)
		}
		stQAUnchanged(t, q.f, before, rows)
	})
	t.Run("discontinuity", func(t *testing.T) {
		q := stQALinked(t)
		b := q.bindings[q.first.Binding.ID]
		q.ingest(0, q.event("A", "1", "work", b))
		q.ingest(10, q.event("A", "2", "observe_work", BindingSnapshot{}))
		before, rows := stQAAudit(t, q.f)
		q.at(20)
		epoch := "other-boot"
		q.sample.Epoch = &epoch
		first := q.status()
		if len(first.Actors) != 1 || first.Actors[0].Health != "stale" || len(first.Uncertainties) != 0 {
			t.Fatal("observed discontinuity wrote health or uncertainty", first)
		}
		q.at(1000)
		q.sample.Epoch = &epoch
		later := q.status()
		if !reflect.DeepEqual(first.ProjectTimers, later.ProjectTimers) || len(later.Uncertainties) != 0 {
			t.Fatal("stale tail grew without evidence", first, later)
		}
		stQAUnchanged(t, q.f, before, rows)
	})
}

func TestSQLiteStatusQAClosedQueuedOutboxAndSyncEnabled(t *testing.T) {
	q := stQALinked(t)
	b := q.bindings[q.first.Binding.ID]
	q.ingest(0, q.event("A", "1", "work", b))
	q.ingest(10, q.event("A", "2", "finish", BindingSnapshot{}))
	owner := stQAOpen(t, q.f, sqliteio.Write)
	defer owner.cleanup()
	meta, err := sqliteReadMeta(owner.tx, q.f.authority, q.f.database)
	if err != nil {
		t.Fatal(err)
	}
	next := metaQANext(t, meta)
	next.SyncEnabled = true
	next.Revision = bump(meta.Revision)
	if err = sqliteUpdateMeta(owner.tx, meta, next); err != nil {
		t.Fatal(err)
	}
	stQAClose(t, owner, true)
	before, rows := stQAAudit(t, q.f)
	q.at(100)
	got := q.status()
	timer := qaProjectTimer(t, got, "1", "3")
	qaTimerDurations(t, timer, 0, 10)
	if len(got.ClosedIntervals) != 1 || timer.QueuedCount != 1 || got.Worker.QueuedCount != 1 || timer.SyncedCount != 0 || got.Worker.UnknownCount != 0 || !got.SyncEnabled || !got.Worker.SyncEnabled {
		t.Fatal("closed queue/sync state", got)
	}
	stQAUnchanged(t, q.f, before, rows)
}

func TestSQLiteStatusQAHistoricalAttributionAfterRealRelink(t *testing.T) {
	q := stQALinked(t)
	first := q.bindings[q.first.Binding.ID]
	q.ingest(0, q.event("old", "1", "work", first))
	q.ingest(10, q.event("old", "2", "finish", BindingSnapshot{}))
	in := q.input
	in.IfRevision = first.Revision
	in.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	in.TaskID = "5"
	in.Timezone = "America/New_York"
	q.provider.user["id"] = json.Number("99")
	q.provider.assignments[0]["task_assignments"] = []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number("5")}}}
	changed, err := q.service.linkSQLite(context.Background(), in, qaLinkDeps(t, q.provider))
	if err != nil || !changed.Changed {
		t.Fatal("real relink", err)
	}
	second := BindingSnapshot{ID: changed.Binding.ID, Revision: changed.Binding.Revision, Attribution: changed.Binding.Attribution}
	q.bindings[second.ID] = second
	q.ingest(20, q.event("new", "1", "work", second))
	q.ingest(35, q.event("new", "2", "finish", BindingSnapshot{}))
	before, rows := stQAAudit(t, q.f)
	q.at(40)
	got := q.status()
	if len(got.Projects) != 2 || len(got.ProjectTimers) != 1 || len(got.ClosedIntervals) != 2 {
		t.Fatal("relink erased or duplicated accounting history", got)
	}
	timer := qaProjectTimer(t, got, "1", "3")
	qaTimerDurations(t, timer, 0, 25)
	if got.ClosedIntervals[0].Attribution != first.Attribution || got.ClosedIntervals[1].Attribution != second.Attribution || timer.QueuedCount != 2 {
		t.Fatal("historical interval attribution changed", got)
	}
	stQAUnchanged(t, q.f, before, rows)
}

func TestSQLiteStatusQACanceledAndCheckedCloseFailureReturnZero(t *testing.T) {
	q := stQALinked(t)
	before, rows := stQAAudit(t, q.f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := q.service.statusSQLite(ctx)
	if err == nil || !reflect.DeepEqual(got, ActivitySnapshot{}) {
		t.Fatal("canceled status acknowledged partial snapshot", err, got)
	}
	stQAUnchanged(t, q.f, before, rows)
	for _, fault := range []struct{ phase, operation string }{{"finalize-after", "statement"}, {"close-before", "close"}} {
		t.Run(fault.phase, func(t *testing.T) {
			hit := false
			stQASetSQLHooks(stQASQLHooks{Fault: func(e stQASQLEvent) error {
				if e.Phase == fault.phase && e.Operation == fault.operation && !hit {
					hit = true
					return errors.New("synthetic checked read cleanup refusal")
				}
				return nil
			}})
			t.Cleanup(func() { stQASetSQLHooks(stQASQLHooks{}) })
			got, err = q.service.statusSQLite(context.Background())
			stQASetSQLHooks(stQASQLHooks{})
			if !hit || err == nil || !reflect.DeepEqual(got, ActivitySnapshot{}) {
				t.Fatal("checked read cleanup failure acknowledged snapshot", err, got)
			}
			stQAUnchanged(t, q.f, before, rows)
			if _, err = q.service.statusSQLite(context.Background()); err != nil {
				t.Fatal("read did not recover after hook removal", err)
			}
		})
	}
}

func TestSQLiteStatusQAWorkerDecorationAfterCheckedRelease(t *testing.T) {
	q := stQALinked(t)
	b := q.bindings[q.first.Binding.ID]
	q.ingest(0, q.event("A", "1", "work", b))
	q.ingest(10, q.event("A", "2", "finish", BindingSnapshot{}))
	before, rows := stQAAudit(t, q.f)
	closed := false
	calls := 0
	stQASetSQLHooks(stQASQLHooks{Observe: func(e stQASQLEvent) {
		if e.Phase == "close-after" && e.Operation == "close" {
			closed = true
		}
	}})
	t.Cleanup(func() { stQASetSQLHooks(stQASQLHooks{}) })
	q.service.observeWorker = func(_ context.Context, w WorkerStatus) WorkerStatus {
		calls++
		if !closed || w.QueuedCount != 1 {
			t.Fatal("worker callback before checked read release or missing queue", w)
		}
		owner := stQAOpen(t, q.f, sqliteio.Write)
		defer owner.cleanup()
		stQAClose(t, owner, false)
		w.State = "running"
		w.QueuedCount = 99
		return w
	}
	got := q.status()
	stQASetSQLHooks(stQASQLHooks{})
	if calls != 1 || got.Worker.State != "running" || got.Worker.QueuedCount != 1 {
		t.Fatal("worker decoration lost authoritative counts", got.Worker)
	}
	stQAUnchanged(t, q.f, before, rows)
}
