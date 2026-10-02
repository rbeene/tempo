package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
)

func qaWorkerID(n int) string { return fmt.Sprintf("12000000-0000-4000-8000-%012d", n) }

type qaWorkerSync struct {
	t            *testing.T
	status       activity.SyncStatus
	reads, calls int
	run          func(context.Context, activity.SyncRunInput, activity.SyncDependencies) (activity.SyncRun, error)
}

func (a *qaWorkerSync) SyncStatus(context.Context) (activity.SyncStatus, error) {
	a.reads++
	return a.status, nil
}
func (a *qaWorkerSync) SyncNow(ctx context.Context, in activity.SyncRunInput, d activity.SyncDependencies) (activity.SyncRun, error) {
	a.calls++
	if a.run != nil {
		return a.run(ctx, in, d)
	}
	a.t.Fatal("read-only/lifecycle flow invoked sync pass")
	return activity.SyncRun{}, nil
}

type qaWorkerRunnerFunc func(context.Context, Command) (CommandResult, error)

func (f qaWorkerRunnerFunc) Run(ctx context.Context, c Command) (CommandResult, error) {
	return f(ctx, c)
}
func qaWorkerOptions(t *testing.T) (Options, *qaWorkerSync) {
	t.Helper()
	root := t.TempDir()
	exe := filepath.Join(root, "tempo")
	if err := os.WriteFile(exe, []byte("synthetic executable, never run\n"), 0700); err != nil {
		t.Fatal(err)
	}
	sync := &qaWorkerSync{t: t, status: activity.SyncStatus{ContractVersion: 1, SnapshotRevision: "0", Configurations: []activity.SyncConfiguration{}, Items: []activity.OutboxItem{}, Accounting: []activity.SyncAccounting{}, Worker: activity.WorkerStatus{State: "not_installed"}}}
	return Options{StatePath: filepath.Join(root, "state", "activity.json"), ConfigPath: filepath.Join(root, "config.json"), Executable: exe, ServiceDir: filepath.Join(root, "services"), Platform: "darwin", UID: 501, Sync: sync, Runner: qaWorkerRunnerFunc(func(context.Context, Command) (CommandResult, error) {
		t.Fatal("read-only observation invoked manager")
		return CommandResult{}, nil
	}), NewRequestID: func() (string, error) { return qaWorkerID(900), nil }, Jitter: func(d time.Duration) time.Duration { return d }}, sync
}
func qaWorkerNew(t *testing.T, o Options) *Service {
	t.Helper()
	s, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func qaWorkerCode(t *testing.T, err error, want string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != want {
		t.Fatalf("wanted%s got%#v", want, err)
	}
}
func qaWorkerAbsent(t *testing.T, o Options) {
	t.Helper()
	for _, path := range []string{filepath.Dir(o.StatePath), o.ServiceDir, o.ConfigPath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read-only operation created %s: %v", path, err)
		}
	}
}
func TestQAWorkerAbsentStatusDoesNotInitializeOrProbeManager(t *testing.T) {
	o, a := qaWorkerOptions(t)
	s := qaWorkerNew(t, o)
	qaWorkerAbsent(t, o)
	result, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.ContractVersion != 1 || result.Status.State == "running" || result.Status.Installed != nil || result.Status.InstanceMode != nil || result.Status.LastSuccess != nil || result.Status.QueuedCount != 0 || result.Status.SubmittingCount != 0 || result.Status.UnknownCount != 0 {
		t.Fatalf("invented absent health=%+v", result)
	}
	if a.reads != 1 || a.calls != 0 {
		t.Fatalf("snapshot reads%d passes%d", a.reads, a.calls)
	}
	qaWorkerAbsent(t, o)
}
func TestQAWorkerObserveNeverTrustsRunningLabelWithoutOwner(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	base := activity.WorkerStatus{State: "running", QueuedCount: 7, SubmittingCount: 2, UnknownCount: 3, SyncEnabled: true}
	got := Observe(context.Background(), o.StatePath, base)
	if got.State == "running" || got.Installed != nil || got.InstanceMode != nil {
		t.Fatalf("absent owner reported live=%+v", got)
	}
	if got.QueuedCount != 7 || got.SubmittingCount != 2 || got.UnknownCount != 3 || !got.SyncEnabled {
		t.Fatalf("observer reread/changed activity counts=%+v", got)
	}
	qaWorkerAbsent(t, o)
}
func TestQAWorkerStatusPreservesSingleSnapshotCounts(t *testing.T) {
	o, a := qaWorkerOptions(t)
	a.status.Enabled = true
	a.status.Worker = activity.WorkerStatus{State: "not_installed", QueuedCount: 7, SubmittingCount: 2, UnknownCount: 3, SyncEnabled: true}
	s := qaWorkerNew(t, o)
	first, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || a.reads != 2 || first.Status.QueuedCount != 7 || first.Status.SubmittingCount != 2 || first.Status.UnknownCount != 3 || !first.Status.SyncEnabled {
		t.Fatalf("status consistency first%+v second%+v reads%d", first, second, a.reads)
	}
	qaWorkerAbsent(t, o)
}

func TestQAWorkerObserveOwnedDefinitionDriftIsReadOnlyAttention(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "edited", true: "missing"}[remove], func(t *testing.T) {
			_, o, m, _ := qaWorkerInstallFixture(t)
			record := qaWorkerReadControl(t, o)
			var err error
			if remove {
				err = os.Remove(record.Owned.Path)
			} else {
				err = os.WriteFile(record.Owned.Path, []byte("foreign edit\n"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(o.StatePath + ".worker-control.json")
			if err != nil {
				t.Fatal(err)
			}
			calls := len(m.commands)
			got := Observe(context.Background(), o.StatePath, activity.WorkerStatus{QueuedCount: 7, UnknownCount: 2, SyncEnabled: true})
			if got.State != "needs_attention" || got.FailureCategory == nil || got.QueuedCount != 7 || got.UnknownCount != 2 || !got.SyncEnabled {
				t.Fatalf("drift observation%+v", got)
			}
			after, err := os.ReadFile(o.StatePath + ".worker-control.json")
			if err != nil || !reflect.DeepEqual(before, after) || len(m.commands) != calls {
				t.Fatalf("observer mutated/probed manager%v", err)
			}
		})
	}
}
func TestQAWorkerLiveOwnerCannotHidePendingController(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	m := &qaWorkerManager{t: t, o: o, requestID: qaWorkerID(91)}
	o.Runner = m
	s := qaWorkerNew(t, o)
	hit := 0
	s.fail = func(stage string) error {
		if stage == "control_after_reserved" {
			hit++
			return errors.New("pending controller")
		}
		return nil
	}
	_, err := s.Install(context.Background(), ControlRequest{RequestID: qaWorkerID(91), Confirmed: true})
	if err == nil || hit != 1 {
		t.Fatalf("missing pending fixture hit%d err%v", hit, err)
	}
	qaWorkerWriteRuntime(t, o, runtimeRecord{Version: 1, InstanceMode: "foreground"})
	held, err := acquire(context.Background(), s.options.StatePath+".worker")
	if err != nil {
		t.Fatal(err)
	}
	defer held.close()
	got := Observe(context.Background(), o.StatePath, activity.WorkerStatus{QueuedCount: 3, SubmittingCount: 1})
	if got.State != "needs_attention" || got.FailureCategory == nil || *got.FailureCategory != "control_pending" || got.QueuedCount != 3 || got.SubmittingCount != 1 {
		t.Fatalf("live runtime hid controller failure%+v", got)
	}
}
