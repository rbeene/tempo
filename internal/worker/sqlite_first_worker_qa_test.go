//go:build (darwin || linux) && (amd64 || arm64)

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/harvest"
)

// Keep the existing provider's read-only identity and assignment answers. The
// sole override represents one synthetic completed POST, optionally held at a
// channel after the durable SQLite claim and before ACK.
type fwQAProvider struct {
	*qaWorkerHarvest
	posts   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (p *fwQAProvider) Create(ctx context.Context, path string, in harvest.Object) (harvest.Object, error) {
	if path != "/time_entries" {
		return nil, &harvest.Error{Code: "response", Message: "unexpected synthetic path"}
	}
	p.posts.Add(1)
	if p.entered != nil {
		select {
		case p.entered <- struct{}{}:
		default:
		}
	}
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	out := harvest.Object{}
	for key, value := range in {
		out[key] = value
	}
	out["id"] = json.Number("901")
	out["user"] = harvest.Object{"id": json.Number("2")}
	out["project"] = harvest.Object{"id": json.Number("3")}
	out["task"] = harvest.Object{"id": json.Number("4")}
	out["is_running"] = false
	out["hours"] = json.Number("0.01")
	out["rounded_hours"] = json.Number("0.25")
	return out, nil
}

type fwQAFixture struct {
	o        Options
	a        *activity.Service
	p        *fwQAProvider
	deps     activity.SyncDependencies
	interval activity.Interval
}

// Link, capture, consent, and queued graph are all produced by public SQLite
// operations. A failure here is a setup failure, not a worker result.
func fwQACaptured(t *testing.T) fwQAFixture {
	t.Helper()
	o, _ := qaWorkerOptions(t)
	epoch, elapsed := "worker-sqlite-boot", "0"
	wall := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	a := activity.NewSQLite(activity.Options{Path: o.StatePath, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: wall, Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &elapsed}, nil
	})})
	p := &fwQAProvider{qaWorkerHarvest: &qaWorkerHarvest{t: t}}
	factory := func(_ context.Context, account string) (harvest.Provider, error) {
		if account != "1" {
			t.Error("wrong synthetic account")
		}
		return p, nil
	}
	deps := activity.SyncDependencies{NewProvider: factory}
	ctx := context.Background()
	linked, err := a.Link(ctx, activity.LinkInput{ProjectID: "3", TaskID: "4", AccountID: "1", Timezone: "UTC", Path: t.TempDir(), RequestID: qaWorkerID(201)}, activity.LinkDependencies{NewProvider: factory})
	if err != nil || !linked.Changed || linked.SnapshotRevision != "1" {
		t.Fatal("SETUP public SQLite Link", err)
	}
	initial, err := a.Status(ctx)
	if err != nil || initial.ComputerID == nil {
		t.Fatal("SETUP public SQLite computer", err)
	}
	event := activity.Event{ContractVersion: 1, Actor: activity.ActorKey{ComputerID: *initial.ComputerID, Source: "manual-test", SessionID: "worker-sqlite", AgentID: "root"}, Generation: "1", Sequence: "1", EventID: "worker-sqlite/1", Kind: "work", BindingID: linked.Binding.ID, BindingRevision: linked.Binding.Revision}
	if r, err := a.Ingest(ctx, event); err != nil || r.Disposition != "applied" || r.SnapshotRevision != "2" {
		t.Fatal("SETUP public work", err)
	}
	event.BindingID, event.BindingRevision = "", ""
	wall = wall.Add(30 * time.Second)
	elapsed = "30000000000"
	event.Sequence, event.EventID, event.Kind = "2", "worker-sqlite/2", "observe_work"
	if r, err := a.Ingest(ctx, event); err != nil || r.Disposition != "applied" || r.SnapshotRevision != "3" {
		t.Fatal("SETUP public observe", err)
	}
	wall = wall.Add(6 * time.Second)
	elapsed = "36000000000"
	event.Sequence, event.EventID, event.Kind = "3", "worker-sqlite/3", "finish"
	if r, err := a.Ingest(ctx, event); err != nil || r.Disposition != "applied" || r.SnapshotRevision != "4" {
		t.Fatal("SETUP public finish", err)
	}
	closed, err := a.Status(ctx)
	if err != nil || len(closed.ClosedIntervals) != 1 || closed.ClosedIntervals[0].DurationNS != "36000000000" || closed.Worker.QueuedCount != 1 || len(closed.Uncertainties) != 0 {
		t.Fatal("SETUP actual closed queue", err)
	}
	configured, err := a.SyncConfigure(ctx, activity.SyncConfigureInput{AccountID: "1", UserID: "2", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: qaWorkerID(202), Confirmed: true}, deps)
	if err != nil || !configured.Changed || configured.SnapshotRevision != "5" {
		t.Fatal("SETUP public SQLite consent", err)
	}
	enabled, err := a.SyncResume(ctx, qaWorkerID(203))
	if err != nil || !enabled.Changed || enabled.SnapshotRevision != "6" {
		t.Fatal("SETUP public SQLite resume", err)
	}
	status, err := a.SyncStatus(ctx)
	if err != nil || !status.Enabled || len(status.Items) != 1 || status.Items[0].State != "queued" || status.Worker.QueuedCount != 1 {
		t.Fatal("SETUP authoritative pending work", err)
	}
	o.Sync, o.SyncDependencies = a, deps
	return fwQAFixture{o: o, a: a, p: p, deps: deps, interval: closed.ClosedIntervals[0]}
}

func TestSQLiteFirstWorkerCapturesUploadsOnceAndColdRestartDoesNotRepost(t *testing.T) {
	f := fwQACaptured(t)
	worker := qaWorkerNew(t, f.o)
	completed := 0
	worker.fail = func(stage string) error {
		if stage == "runtime_after_result_saved" {
			completed++
			return errors.New("bounded stop after checked worker result")
		}
		return nil
	}
	if err := qaWorkerBoundedRun(t, worker); err == nil || completed != 1 || f.p.posts.Load() != 1 {
		t.Fatal("worker did not run exactly one real SQLite/POST/ACK pass", err, completed, f.p.posts.Load())
	}
	runtime := qaWorkerReadRuntime(t, f.o)
	if runtime.Pending != nil || runtime.LastSuccess == nil {
		t.Fatal("completed pass lost runtime acknowledgement", runtime)
	}
	cold := activity.NewSQLite(activity.Options{Path: f.o.StatePath})
	status, err := cold.SyncStatus(context.Background())
	if err != nil || len(status.Items) != 1 || status.Items[0].State != "synced" || status.Worker.QueuedCount != 0 || status.Items[0].Interval.ID != f.interval.ID || status.Items[0].Plan == nil || len(status.Items[0].Plan.Parts) != 1 || len(status.Items[0].Plan.Parts[0].Attempts) != 1 {
		t.Fatal("cold status lost exact completed queue", err, status)
	}
	blockedProvider := 0
	f.o.Sync = cold
	f.o.SyncDependencies = activity.SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) {
		blockedProvider++
		return nil, errors.New("completed work must not construct provider")
	}}
	replay, err := cold.SyncNow(context.Background(), activity.SyncRunInput{RequestID: qaWorkerID(900), Limit: 20}, f.o.SyncDependencies)
	if err != nil || replay.State != "complete" || len(replay.ResolvedIDs) != 1 || blockedProvider != 0 || f.p.posts.Load() != 1 {
		t.Fatal("cold exact request replay posted again", err, replay)
	}
	f.o.NewRequestID = func() (string, error) {
		t.Error("cold idle worker minted another pass")
		return "", errors.New("unexpected pass")
	}
	clock := qaWorkerNewClock()
	f.o.Clock = clock
	cancel, done := qaWorkerStartLoop(t, qaWorkerNew(t, f.o))
	if delay := qaWorkerWaitTimer(t, clock, done); delay != 30*time.Second {
		t.Fatal("cold synced worker did not reach idle barrier", delay)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("cold worker did not stop cleanly", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cold worker did not join")
	}
	if blockedProvider != 0 || f.p.posts.Load() != 1 {
		t.Fatal("cold worker reopened remote write")
	}
}

func TestSQLiteFirstWorkerHeldPostKeepsStatusAndPauseAvailable(t *testing.T) {
	f := fwQACaptured(t)
	f.p.entered, f.p.release = make(chan struct{}, 1), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(f.p.release) }) })
	clock := qaWorkerNewClock()
	f.o.Clock = clock
	cancel, done := qaWorkerStartLoop(t, qaWorkerNew(t, f.o))
	select {
	case <-f.p.entered:
	case err := <-done:
		t.Fatal("worker left before mock POST", err)
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not reach mock POST")
	}
	ctx, end := context.WithTimeout(context.Background(), time.Second)
	status, err := f.a.SyncStatus(ctx)
	if err != nil || len(status.Items) != 1 || status.Items[0].State != "submitting" || status.Worker.SubmittingCount != 1 {
		end()
		t.Fatal("held POST blocked authoritative status", err, status)
	}
	paused, err := f.a.SyncPause(ctx, qaWorkerID(204))
	end()
	if err != nil || !paused.Changed {
		t.Fatal("held POST blocked consent pause", err, paused)
	}
	release.Do(func() { close(f.p.release) })
	if delay := qaWorkerWaitTimer(t, clock, done); delay != time.Second {
		t.Fatal("successful held pass did not finish through ACK", delay)
	}
	status, err = f.a.SyncStatus(context.Background())
	if err != nil || status.Enabled || len(status.Items) != 1 || status.Items[0].State != "synced" || status.Worker.SubmittingCount != 0 || f.p.posts.Load() != 1 {
		t.Fatal("pause or held POST changed exact one-write completion", err, status)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("held worker did not stop cleanly", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("held worker did not join")
	}
}
