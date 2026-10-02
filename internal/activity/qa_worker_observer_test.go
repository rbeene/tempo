package activity

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestQAWorkerObserverAbsentSnapshotsRemainReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "activity.json")
	calls := 0
	s := New(Options{Path: path, ObserveWorker: func(_ context.Context, w WorkerStatus) WorkerStatus {
		calls++
		if w.QueuedCount != 0 || w.SubmittingCount != 0 || w.UnknownCount != 0 || w.SyncEnabled {
			t.Fatalf("invented snapshot%+v", w)
		}
		w.State = "needs_attention"
		return w
	}})
	snap, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sync, err := s.SyncStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || snap.Worker.State != "needs_attention" || sync.Worker.State != "needs_attention" || snap.ComputerID != nil {
		t.Fatalf("observer not composed calls%d activity%+v sync%+v", calls, snap.Worker, sync.Worker)
	}
	if _, err = os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("observation initialized store%v", err)
	}
}
func TestQAWorkerObserverSeesSameQueueAndRunsAfterStoreUnlock(t *testing.T) {
	original, _, _ := qaSyncFixture(t, 15*time.Second)
	path := original.store.path
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	base, err := original.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	observed := New(Options{Path: path, Clock: original.clock, ObserveWorker: func(ctx context.Context, w WorkerStatus) WorkerStatus {
		calls++
		if w.QueuedCount != 1 || w.SubmittingCount != 0 || w.UnknownCount != 0 {
			t.Fatalf("snapshot omitted captured queue%+v", w)
		}
		lockCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		if err := original.store.update(lockCtx, func(*state) (bool, error) { return false, nil }); err != nil {
			t.Fatalf("observer called under activity lock%v", err)
		}
		w.State = "running"
		return w
	}})
	got, err := observed.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sync, err := observed.SyncStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || got.Worker.State != "running" || sync.Worker.State != "running" || got.Worker.QueuedCount != sync.Worker.QueuedCount || got.SnapshotRevision != base.SnapshotRevision || !reflect.DeepEqual(got.ClosedIntervals, base.ClosedIntervals) {
		t.Fatalf("composed snapshot mismatch calls%d activity%+v sync%+v", calls, got.Worker, sync.Worker)
	}
	after, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("read-only observer changed state%v", err)
	}
}
