package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/harvest"
)

func qaWorkerReadRuntime(t *testing.T, o Options) runtimeRecord {
	t.Helper()
	b, err := os.ReadFile(o.StatePath + ".worker-runtime.json")
	if err != nil {
		t.Fatal(err)
	}
	var r runtimeRecord
	if err = json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return r
}
func qaWorkerWriteRuntime(t *testing.T, o Options, r runtimeRecord) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(o.StatePath), 0700); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(o.StatePath+".worker-runtime.json", b, 0600); err != nil {
		t.Fatal(err)
	}
}
func qaWorkerQueued(a *qaWorkerSync) {
	a.status.Enabled = true
	a.status.Worker.SyncEnabled = true
	a.status.Worker.QueuedCount = 1
	a.status.Items = []activity.OutboxItem{{ID: qaWorkerID(700), State: "queued"}}
}
func qaWorkerBoundedRun(t *testing.T, s *Service) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.Run(ctx)
}
func TestQAWorkerPendingIsDurableBeforeAnySyncCall(t *testing.T) {
	o, a := qaWorkerOptions(t)
	qaWorkerQueued(a)
	s := qaWorkerNew(t, o)
	reached := 0
	s.fail = func(stage string) error {
		if stage == "runtime_after_pending_saved" {
			reached++
			r := qaWorkerReadRuntime(t, o)
			if r.Pending == nil || r.Pending.RequestID != qaWorkerID(900) || r.Pending.Limit != 20 {
				t.Fatalf("bad reserved pass%+v", r)
			}
			return errors.New("synthetic private failure")
		}
		return nil
	}
	err := qaWorkerBoundedRun(t, s)
	if err == nil || reached != 1 || a.calls != 0 {
		t.Fatalf("reservation barrier not respected reached%d calls%d err%v", reached, a.calls, err)
	}
	if _, err = os.Stat(o.StatePath); !os.IsNotExist(err) {
		t.Fatalf("worker minted activity identity: %v", err)
	}
}
func TestQAWorkerPendingReplayPrecedesPausedEmptyAndDoesNotStampSuccess(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(map[bool]string{false: "null", true: "known"}[known], func(t *testing.T) {
			o, a := qaWorkerOptions(t)
			pending := activity.SyncRunInput{RequestID: qaWorkerID(41), Limit: 20}
			r := runtimeRecord{Version: 1, Pending: &pending, InstanceMode: "foreground"}
			if known {
				old := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
				r.LastSuccess = &old
			}
			qaWorkerWriteRuntime(t, o, r)
			o.NewRequestID = func() (string, error) { t.Fatal("recovery replaced pending identity"); return "", nil }
			a.run = func(_ context.Context, in activity.SyncRunInput, _ activity.SyncDependencies) (activity.SyncRun, error) {
				if in != pending {
					t.Fatalf("recovery changed input%+v", in)
				}
				durable := qaWorkerReadRuntime(t, o)
				if durable.Pending == nil || *durable.Pending != pending {
					t.Fatal("pending cleared before replay")
				}
				return activity.SyncRun{ContractVersion: 1, RequestID: in.RequestID, State: "complete", ResolvedIDs: []string{qaWorkerID(700)}}, nil
			}
			s := qaWorkerNew(t, o)
			saved := 0
			s.fail = func(stage string) error {
				if stage == "runtime_after_result_saved" {
					saved++
					return errors.New("stop after durable recovery")
				}
				return nil
			}
			err := qaWorkerBoundedRun(t, s)
			if err == nil || a.calls != 1 || saved != 1 {
				t.Fatalf("paused recovery calls%d saved%d err%v", a.calls, saved, err)
			}
			after := qaWorkerReadRuntime(t, o)
			if after.Pending != nil || !reflect.DeepEqual(after.LastSuccess, r.LastSuccess) {
				t.Fatalf("receipt replay invented delivery timestamp before%+v after%+v", r, after)
			}
		})
	}
}
func TestQAWorkerLostLocalCompletionReplaysSameRequest(t *testing.T) {
	o, a := qaWorkerOptions(t)
	qaWorkerQueued(a)
	var inputs []activity.SyncRunInput
	a.run = func(_ context.Context, in activity.SyncRunInput, _ activity.SyncDependencies) (activity.SyncRun, error) {
		inputs = append(inputs, in)
		r := qaWorkerReadRuntime(t, o)
		if r.Pending == nil || *r.Pending != in {
			t.Fatal("sync without persisted identity")
		}
		return activity.SyncRun{ContractVersion: 1, RequestID: in.RequestID, State: "complete", ResolvedIDs: []string{qaWorkerID(700)}}, nil
	}
	first := qaWorkerNew(t, o)
	reached := 0
	first.fail = func(stage string) error {
		if stage == "runtime_after_sync_return" {
			reached++
			return errors.New("lost local completion")
		}
		return nil
	}
	if err := qaWorkerBoundedRun(t, first); err == nil || reached != 1 || len(inputs) != 1 {
		t.Fatalf("first pass did not reach outcome barrier reached%d inputs%v err%v", reached, inputs, err)
	}
	pending := qaWorkerReadRuntime(t, o)
	if pending.Pending == nil || *pending.Pending != inputs[0] {
		t.Fatal("uncertain completion dropped request")
	}
	a.status.Enabled = false
	a.status.Worker.SyncEnabled = false
	a.status.Worker.QueuedCount = 0
	a.status.Items = nil
	o.NewRequestID = func() (string, error) { t.Fatal("restart minted replacement request"); return "", nil }
	second := qaWorkerNew(t, o)
	saved := 0
	second.fail = func(stage string) error {
		if stage == "runtime_after_result_saved" {
			saved++
			return errors.New("stop after replay")
		}
		return nil
	}
	err := qaWorkerBoundedRun(t, second)
	if err == nil || saved != 1 || len(inputs) != 2 || inputs[0] != inputs[1] {
		t.Fatalf("restart replay mismatch inputs%v saved%d err%v", inputs, saved, err)
	}
	final := qaWorkerReadRuntime(t, o)
	if final.Pending != nil || final.LastSuccess != nil {
		t.Fatalf("restart falsely attributed old delivery%+v", final)
	}
}

func TestQAWorkerControllerCommitCannotEraseRuntimePending(t *testing.T) {
	o, a := qaWorkerOptions(t)
	qaWorkerQueued(a)
	m := &qaWorkerManager{t: t, o: o, requestID: qaWorkerID(61)}
	o.Runner = m
	runtime := qaWorkerNew(t, o)
	hit := 0
	runtime.fail = func(stage string) error {
		if stage != "runtime_after_pending_saved" {
			return nil
		}
		hit++
		before := qaWorkerReadRuntime(t, o)
		if before.Pending == nil {
			t.Fatal("missing runtime pending before concurrent control")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := qaWorkerNew(t, o).Install(ctx, ControlRequest{RequestID: qaWorkerID(61), Confirmed: true})
		if err != nil {
			t.Fatalf("independent controller blocked by runtime owner: %v", err)
		}
		after := qaWorkerReadRuntime(t, o)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("controller replaced runtime record before%+v after%+v", before, after)
		}
		return errors.New("stop runtime after independent controller commit")
	}
	err := qaWorkerBoundedRun(t, runtime)
	if err == nil || hit != 1 || a.calls != 0 {
		t.Fatalf("did not reach interleaving hit%d calls%d err%v", hit, a.calls, err)
	}
	control := qaWorkerReadControl(t, o)
	pending := qaWorkerReadRuntime(t, o)
	if control.Receipts[qaWorkerID(61)].Result == nil || pending.Pending == nil || pending.Pending.RequestID != qaWorkerID(900) {
		t.Fatalf("split writers lost IDs control%+v runtime%+v", control, pending)
	}
	before, err := os.ReadFile(o.StatePath + ".worker-control.json")
	if err != nil {
		t.Fatal(err)
	}
	a.status.Enabled = false
	a.status.Worker.SyncEnabled = false
	a.status.Items = nil
	a.status.Worker.QueuedCount = 0
	a.run = func(_ context.Context, in activity.SyncRunInput, _ activity.SyncDependencies) (activity.SyncRun, error) {
		if in != *pending.Pending {
			t.Fatalf("restart replaced pending%+v", in)
		}
		return activity.SyncRun{ContractVersion: 1, RequestID: in.RequestID, State: "complete"}, nil
	}
	restart := qaWorkerNew(t, o)
	saved := 0
	restart.fail = func(stage string) error {
		if stage == "runtime_after_result_saved" {
			saved++
			return errors.New("stop replay")
		}
		return nil
	}
	err = qaWorkerBoundedRun(t, restart)
	if err == nil || saved != 1 || a.calls != 1 {
		t.Fatalf("restart did not replay saved%d calls%d err%v", saved, a.calls, err)
	}
	after, err := os.ReadFile(o.StatePath + ".worker-control.json")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("runtime writer replaced controller receipt%v", err)
	}
}

type qaWorkerHarvest struct {
	t      *testing.T
	posts  int
	cancel context.CancelFunc
}

func (p *qaWorkerHarvest) Accounts(context.Context) ([]harvest.Object, error) {
	return []harvest.Object{{"id": json.Number("1"), "product": "harvest"}}, nil
}
func (p *qaWorkerHarvest) Get(_ context.Context, path string) (harvest.Object, error) {
	switch path {
	case "/users/me":
		return harvest.Object{"id": json.Number("2"), "is_active": true, "timezone": "UTC"}, nil
	case "/company":
		return harvest.Object{"is_active": true, "wants_timestamp_timers": false}, nil
	}
	p.t.Fatalf("unexpected fixture GET%s", path)
	return nil, nil
}
func (p *qaWorkerHarvest) List(_ context.Context, path string, _ url.Values) ([]harvest.Object, error) {
	if path != "/users/me/project_assignments" {
		p.t.Fatalf("unexpected fixture LIST%s", path)
	}
	return []harvest.Object{{"is_active": true, "project": harvest.Object{"id": json.Number("3")}, "task_assignments": []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number("4")}}}}}, nil
}
func (p *qaWorkerHarvest) Create(context.Context, string, harvest.Object) (harvest.Object, error) {
	p.posts++
	if p.cancel != nil {
		p.cancel()
	}
	return nil, &harvest.Error{Code: "network", Message: "synthetic uncertain response", Uncertain: true}
}
func (p *qaWorkerHarvest) Update(context.Context, string, harvest.Object) (harvest.Object, error) {
	p.t.Fatal("unexpected PATCH")
	return nil, nil
}
func (p *qaWorkerHarvest) Delete(context.Context, string) error {
	p.t.Fatal("unexpected DELETE")
	return nil
}
func qaWorkerRealActivity(t *testing.T, o Options) (*activity.Service, *qaWorkerHarvest, activity.SyncDependencies) {
	t.Helper()
	epoch, n := "worker-fixture-boot", "0"
	wall := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	a := activity.New(activity.Options{Path: o.StatePath, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: wall, Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}, nil
	})})
	p := &qaWorkerHarvest{t: t}
	factory := func(_ context.Context, id string) (harvest.Provider, error) {
		if id != "1" {
			t.Fatalf("wrong account%s", id)
		}
		return p, nil
	}
	deps := activity.SyncDependencies{NewProvider: factory}
	linked, err := a.Link(context.Background(), activity.LinkInput{ProjectID: "3", TaskID: "4", AccountID: "1", Timezone: "UTC", Path: t.TempDir(), RequestID: qaWorkerID(201)}, activity.LinkDependencies{NewProvider: factory})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := a.Status(context.Background())
	if err != nil || snapshot.ComputerID == nil {
		t.Fatalf("fixture identity%v", err)
	}
	event := activity.Event{ContractVersion: 1, Actor: activity.ActorKey{ComputerID: *snapshot.ComputerID, Source: "manual-test", SessionID: "worker-real", AgentID: "actor"}, Generation: "1", Sequence: "1", EventID: "worker-real/1", Kind: "work", BindingID: linked.Binding.ID, BindingRevision: linked.Binding.Revision}
	if _, err = a.Ingest(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	n = strconv.FormatInt(int64(30*time.Second), 10)
	wall = wall.Add(30 * time.Second)
	event.Sequence = "2"
	event.EventID = "worker-real/2"
	event.Kind = "finish"
	event.BindingID = ""
	event.BindingRevision = ""
	if _, err = a.Ingest(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if _, err = a.SyncConfigure(context.Background(), activity.SyncConfigureInput{AccountID: "1", Mode: "duration", DurationPolicy: "nearest-hundredth-hour", IfRevision: "0", RequestID: qaWorkerID(202), Confirmed: true}, deps); err != nil {
		t.Fatal(err)
	}
	if _, err = a.SyncResume(context.Background(), qaWorkerID(203)); err != nil {
		t.Fatal(err)
	}
	status, err := a.SyncStatus(context.Background())
	if err != nil || len(status.Items) != 1 || status.Items[0].State != "queued" || status.Items[0].Interval.DurationNS != "30000000000" {
		t.Fatalf("genuine fixture queue%+v err%v", status, err)
	}
	return a, p, deps
}
func TestQAWorkerRealPausedOrphanRecoveryNeverResubmits(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		t.Run(map[bool]string{false: "startup_submitting", true: "pending_runtime"}[persisted], func(t *testing.T) {
			o, _ := qaWorkerOptions(t)
			a, p, deps := qaWorkerRealActivity(t, o)
			ctx, cancel := context.WithCancel(context.Background())
			p.cancel = cancel
			input := activity.SyncRunInput{RequestID: qaWorkerID(204), Limit: 20}
			_, err := a.SyncNow(ctx, input, deps)
			cancel()
			if err == nil || p.posts != 1 {
				t.Fatalf("did not reach uncertain canceled submission posts%d err%v", p.posts, err)
			}
			status, err := a.SyncStatus(context.Background())
			if err != nil || status.Items[0].State != "submitting" || status.Items[0].RunRequestID == nil {
				t.Fatalf("fixture lacks genuine orphan%+v err%v", status, err)
			}
			if _, err = a.SyncPause(context.Background(), qaWorkerID(205)); err != nil {
				t.Fatal(err)
			}
			if persisted {
				qaWorkerWriteRuntime(t, o, runtimeRecord{Version: 1, Pending: &input, InstanceMode: "foreground"})
			}
			o.Sync = activity.New(activity.Options{Path: o.StatePath})
			providerCalls := 0
			o.SyncDependencies = activity.SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) {
				providerCalls++
				t.Fatal("paused orphan recovery accessed credentials/provider")
				return nil, nil
			}}
			generated := 0
			o.NewRequestID = func() (string, error) { generated++; return qaWorkerID(206), nil }
			s := qaWorkerNew(t, o)
			saved := 0
			s.fail = func(stage string) error {
				if stage == "runtime_after_result_saved" {
					saved++
					return errors.New("stop after real recovery")
				}
				return nil
			}
			err = qaWorkerBoundedRun(t, s)
			if err == nil || saved != 1 || providerCalls != 0 || p.posts != 1 {
				t.Fatalf("worker recovery effects saved%d providers%d posts%d err%v", saved, providerCalls, p.posts, err)
			}
			wantIDs := 1
			if persisted {
				wantIDs = 0
			}
			if generated != wantIDs {
				t.Fatalf("recovery IDs%d want%d", generated, wantIDs)
			}
			status, err = a.SyncStatus(context.Background())
			if err != nil || status.Enabled || status.Items[0].State != "unknown" || status.Items[0].RunRequestID != nil || len(status.Items[0].Plan.Parts[0].Attempts) != 1 {
				t.Fatalf("real recovery wrong%+v err%v", status, err)
			}
			r := qaWorkerReadRuntime(t, o)
			if r.Pending != nil || r.LastSuccess != nil {
				t.Fatalf("orphan recovery invented delivery%+v", r)
			}
		})
	}
}

func TestQAWorkerManualContentionOneHTTPPostAndCanceledUnknown(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	a, p, _ := qaWorkerRealActivity(t, o)
	entered := make(chan struct{})
	releaseHandler := make(chan struct{})
	var posts, providers atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer synthetic-worker-token" || (strings.HasPrefix(r.URL.Path, "/v2/") && r.Header.Get("Harvest-Account-Id") != "1") {
			t.Error("fixture request missing synthetic attribution")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/identity/accounts":
			rows, _ := p.Accounts(r.Context())
			_ = json.NewEncoder(w).Encode(map[string]any{"accounts": rows})
		case "/v2/users/me", "/v2/company":
			row, err := p.Get(r.Context(), strings.TrimPrefix(r.URL.Path, "/v2"))
			if err != nil {
				t.Error(err)
			}
			_ = json.NewEncoder(w).Encode(row)
		case "/v2/users/me/project_assignments":
			rows, _ := p.List(r.Context(), "/users/me/project_assignments", nil)
			_ = json.NewEncoder(w).Encode(map[string]any{"project_assignments": rows, "links": map[string]any{"next": nil}})
		case "/v2/time_entries":
			if r.Method != "POST" {
				t.Errorf("unexpected entry verb%s", r.Method)
				http.Error(w, "bad", 400)
				return
			}
			if posts.Add(1) != 1 {
				t.Error("duplicate HTTP POST")
			} else {
				close(entered)
			}
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
			case <-releaseHandler:
			}
		default:
			t.Errorf("unexpected local HTTP path%s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(releaseHandler) })
	client := harvest.NewWithHTTP("synthetic-worker-token", "1", server.URL+"/v2", server.URL+"/identity", server.Client())
	deps := activity.SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { providers.Add(1); return client, nil }}
	o.Sync = a
	o.SyncDependencies = deps
	cancel, done := qaWorkerStartLoop(t, qaWorkerNew(t, o))
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("worker exited before HTTP claim%v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("worker didn't reach HTTP POST")
	}
	manualCtx, manualCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := a.SyncNow(manualCtx, activity.SyncRunInput{RequestID: qaWorkerID(850), Limit: 20}, deps)
	manualCancel()
	var ae *activity.Error
	if !errors.As(err, &ae) || ae.Code != "state_busy" {
		t.Fatalf("manual run bypassed live #11 lock%v", err)
	}
	if posts.Load() != 1 || providers.Load() != 1 {
		t.Fatalf("manual contender made effects posts%d providers%d", posts.Load(), providers.Load())
	}
	cancel()
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("lost worker cancel cause%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker didn't cancel active HTTP pass")
	}
	pending := qaWorkerReadRuntime(t, o)
	if pending.Pending == nil {
		t.Fatal("cancelled ambiguous request identity lost")
	}
	if _, err = a.SyncPause(context.Background(), qaWorkerID(851)); err != nil {
		t.Fatal(err)
	}
	o.SyncDependencies = activity.SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) {
		t.Fatal("paused recovery retried HTTP/provider")
		return nil, nil
	}}
	restart := qaWorkerNew(t, o)
	saved := 0
	restart.fail = func(stage string) error {
		if stage == "runtime_after_result_saved" {
			saved++
			return errors.New("stop after paused HTTP recovery")
		}
		return nil
	}
	err = qaWorkerBoundedRun(t, restart)
	if err == nil || saved != 1 {
		t.Fatalf("recovery not reached saved%d err%v", saved, err)
	}
	status, err := a.SyncStatus(context.Background())
	if err != nil || len(status.Items) != 1 || status.Items[0].State != "unknown" || len(status.Items[0].Plan.Parts[0].Attempts) != 1 || posts.Load() != 1 {
		t.Fatalf("ambiguous cancellation reposted or lost audit posts%d state%+v err%v", posts.Load(), status, err)
	}
}
