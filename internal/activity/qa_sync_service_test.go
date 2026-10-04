package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/harvest"
)

func qaSyncID(n int) string { return fmt.Sprintf("90000000-0000-4000-8000-%012d", n) }

type qaSyncProvider struct {
	entryListErr           error
	beforeEntryList        func()
	userErr, assignmentErr error
	entryErr               error
	timezone               string
	entryRows              []harvest.Object
	listQueries            []url.Values
	entry                  harvest.Object
	t                      *testing.T
	company                harvest.Object
	companyErr, createErr  error
	calls                  []string
	posts                  []harvest.Object
	beforeCreate           func(harvest.Object)
	beforeUser             func()
	returnedHours          string
}

func qaNewSyncProvider(t *testing.T) *qaSyncProvider {
	return &qaSyncProvider{timezone: "UTC", t: t, company: harvest.Object{"is_active": true, "wants_timestamp_timers": false, "clock": "24h"}, returnedHours: "0.040"}
}
func (p *qaSyncProvider) Accounts(context.Context) ([]harvest.Object, error) {
	p.calls = append(p.calls, "accounts")
	return []harvest.Object{{"id": json.Number("1"), "product": "harvest"}}, nil
}
func (p *qaSyncProvider) Get(_ context.Context, path string) (harvest.Object, error) {
	p.calls = append(p.calls, path)
	switch path {
	case "/users/me":
		if p.userErr != nil {
			return nil, p.userErr
		}
		if p.beforeUser != nil {
			p.beforeUser()
		}
		return harvest.Object{"id": json.Number("2"), "is_active": true, "timezone": p.timezone}, nil
	case "/time_entries/901", "/time_entries/902":
		return p.entry, p.entryErr
	case "/company":
		return p.company, p.companyErr
	default:
		p.t.Fatalf("unexpected GET %s", path)
		return nil, nil
	}
}
func (p *qaSyncProvider) List(_ context.Context, path string, q url.Values) ([]harvest.Object, error) {
	p.calls = append(p.calls, path)
	if path == "/time_entries" {
		p.listQueries = append(p.listQueries, q)
		if p.beforeEntryList != nil {
			p.beforeEntryList()
		}
		return p.entryRows, p.entryListErr
	}
	if path != "/users/me/project_assignments" {
		p.t.Fatalf("unexpected list %s", path)
	}
	return qaNewLinkProvider(p.t).assignments, p.assignmentErr
}
func (p *qaSyncProvider) Create(_ context.Context, path string, in harvest.Object) (harvest.Object, error) {
	if path != "/time_entries" {
		p.t.Fatalf("unexpected POST %s", path)
	}
	p.posts = append(p.posts, in)
	if p.beforeCreate != nil {
		p.beforeCreate(in)
	}
	if p.createErr != nil {
		return nil, p.createErr
	}
	out := harvest.Object{}
	for k, v := range in {
		out[k] = v
	}
	out["id"] = json.Number(strconv.Itoa(900 + len(p.posts)))
	out["user"] = harvest.Object{"id": json.Number("2")}
	out["project"] = harvest.Object{"id": json.Number("3")}
	out["task"] = harvest.Object{"id": json.Number("4")}
	out["is_running"] = false
	if p.returnedHours != "" {
		out["hours"] = json.Number(p.returnedHours)
	}
	out["rounded_hours"] = json.Number("0.25")
	return out, nil
}
func (p *qaSyncProvider) Update(context.Context, string, harvest.Object) (harvest.Object, error) {
	p.t.Fatal("sync PATCH forbidden")
	return nil, nil
}
func (p *qaSyncProvider) Delete(context.Context, string) error {
	p.t.Fatal("sync DELETE forbidden")
	return nil
}
func qaSyncDeps(t *testing.T, p *qaSyncProvider) SyncDependencies {
	return SyncDependencies{NewProvider: func(_ context.Context, account string) (harvest.Provider, error) {
		if account != "1" {
			t.Fatalf("wrong persisted account %q", account)
		}
		return p, nil
	}}
}
func qaSyncNoProvider(t *testing.T) SyncDependencies {
	return SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) {
		t.Fatal("offline/replay constructed provider")
		return nil, nil
	}}
}
func qaSyncFixture(t *testing.T, duration time.Duration) (*Service, string, Interval) {
	t.Helper()
	s, path := qaLegacyLinkService(t)
	sample := ClockSample{}
	setClock := func(d time.Duration) {
		epoch, n := "sync-qa-boot", strconv.FormatInt(int64(d), 10)
		sample = ClockSample{Capability: "available", WallUTC: qaEpochStart.Add(d), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}
	}
	setClock(0)
	s.clock = ClockFunc(func() (ClockSample, error) { return sample, nil })
	b, err := s.Link(context.Background(), qaLinkInput(t), qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil {
		t.Fatal(err)
	}
	st, _, err := s.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	event := qaEvent("sync-fixture", "1", "1", "work", b.Binding.ID)
	event.Actor.ComputerID = st.ComputerID
	if _, err = s.Ingest(context.Background(), event); err != nil {
		t.Fatalf("fixture start: %v event=%+v", err, event)
	}
	event.BindingID, event.BindingRevision = "", ""
	seq := 2
	for d := 30 * time.Second; d < duration; d += 30 * time.Second {
		setClock(d)
		event.Sequence = strconv.Itoa(seq)
		event.EventID = fmt.Sprintf("sync-fixture/%d", seq)
		event.Kind = "observe_work"
		if _, err = s.Ingest(context.Background(), event); err != nil {
			t.Fatalf("fixture heartbeat: %v event=%+v", err, event)
		}
		seq++
	}
	setClock(duration)
	event.Sequence = strconv.Itoa(seq)
	event.EventID = fmt.Sprintf("sync-fixture/%d", seq)
	event.Kind = "finish"
	if _, err = s.Ingest(context.Background(), event); err != nil {
		t.Fatalf("fixture finish: %v event=%+v", err, event)
	}
	snap, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.ClosedIntervals) != 1 || snap.ClosedIntervals[0].DurationNS != strconv.FormatInt(int64(duration), 10) {
		t.Fatalf("fixture exact capture failed: %+v", snap)
	}
	return s, path, snap.ClosedIntervals[0]
}
func qaSyncConfigure(t *testing.T, s *Service, p *qaSyncProvider) SyncConfigurationResult {
	t.Helper()
	r, err := s.SyncConfigure(context.Background(), SyncConfigureInput{AccountID: "1", Mode: "duration", DurationPolicy: "nearest-hundredth-hour", IfRevision: "0", RequestID: qaSyncID(1), Confirmed: true}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func qaSyncEnable(t *testing.T, s *Service) {
	t.Helper()
	if _, err := s.SyncResume(context.Background(), qaSyncID(2)); err != nil {
		t.Fatal(err)
	}
}
func qaSyncOnlyItem(t *testing.T, s *Service) OutboxItem {
	t.Helper()
	st, err := s.SyncStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Items) != 1 {
		t.Fatalf("items=%+v", st.Items)
	}
	return st.Items[0]
}
func qaSyncString(t *testing.T, p *string, want string) {
	t.Helper()
	if p == nil || *p != want {
		t.Fatalf("value=%v want %s", p, want)
	}
}

func TestQASyncOfflineAbsentStatusAndControlsDoNotInitialize(t *testing.T) {
	s, path := qaLinkService(t)
	r, err := s.SyncStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.ContractVersion != 1 || r.SnapshotRevision != "0" || r.Enabled || r.Items == nil || r.Configurations == nil || r.Accounting == nil || r.Totals.ExactDurationNS != "0" {
		t.Fatalf("absent status=%+v", r)
	}
	for i, op := range []func(context.Context, string) (MutationResult, error){s.SyncPause, s.SyncResume} {
		_, err := op(context.Background(), qaSyncID(100+i))
		qaCode(t, err, "input_required")
	}
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("offline controls initialized state: %v", err)
	}
	_, err = s.SyncConfigure(context.Background(), SyncConfigureInput{AccountID: "1", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: qaSyncID(103), Confirmed: true}, qaSyncNoProvider(t))
	qaCode(t, err, "input_required")
	qaAbsentLinkState(t, path)
}
func TestQASyncConfigurationConsentCASAndReplay(t *testing.T) {
	s, path, interval := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	in := SyncConfigureInput{AccountID: "1", Mode: "duration", DurationPolicy: "nearest-hundredth-hour", IfRevision: "0", RequestID: qaSyncID(1), Confirmed: true}
	unconfirmed := in
	unconfirmed.Confirmed = false
	_, err := s.SyncConfigure(context.Background(), unconfirmed, qaSyncNoProvider(t))
	qaCode(t, err, "confirmation_required")
	r, err := s.SyncConfigure(context.Background(), in, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	c := r.Configuration
	if !r.Changed || c.AccountID != "1" || c.UserID != "2" || c.Revision != "1" || !c.Declared || c.Source != "user_declared" || c.PolicyVersion != "nearest-hundredth-hour-v1" || c.DeclaredAt.IsZero() {
		t.Fatalf("consent=%+v", r)
	}
	replay, err := qaLegacyNew(Options{Path: path}).SyncConfigure(context.Background(), in, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(r, replay) {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	changed := in
	changed.DurationPolicy = "exact"
	_, err = s.SyncConfigure(context.Background(), changed, qaSyncNoProvider(t))
	qaCode(t, err, "request_conflict")
	changed.RequestID = qaSyncID(4)
	_, err = s.SyncConfigure(context.Background(), changed, qaSyncDeps(t, p))
	qaCode(t, err, "revision_conflict")
	st, err := s.SyncStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Enabled || len(st.Items) != 1 || !reflect.DeepEqual(st.Items[0].Interval, interval) || len(p.posts) != 0 {
		t.Fatalf("configure changed capture/sync: %+v", st)
	}
}
func TestQASyncConsented137482MillisecondsPreservesAccounting(t *testing.T) {
	s, _, original := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	p.companyErr = &harvest.Error{Code: "forbidden", Status: 403}
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	p.beforeCreate = func(_ harvest.Object) {
		st, _, err := s.store.read(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		item := st.Outbox[original.ID]
		if item.State != "submitting" || item.RunRequestID == nil || item.Plan == nil || len(item.Plan.Parts) != 1 || item.Plan.Parts[0].State != "submitting" || len(item.Plan.Parts[0].Attempts) != 1 {
			t.Fatalf("POST without durable intent: %+v", item)
		}
	}
	run, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(3)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	item := qaSyncOnlyItem(t, s)
	if len(p.posts) != 1 || item.State != "synced" || !reflect.DeepEqual(item.Interval, original) || item.Plan == nil || item.Plan.CompanySource != "user_declared_fallback" || !reflect.DeepEqual(run.AttemptedIDs, []string{item.ID}) {
		t.Fatalf("run=%+v item=%+v posts=%d", run, item, len(p.posts))
	}
	part := item.Plan.Parts[0]
	if part.PlannedHours != "0.04" || part.PlannedDurationNS != "144000000000" || part.PlannedResidualNS != "-6518000000" {
		t.Fatalf("plan arithmetic=%+v", part)
	}
	qaSyncString(t, part.ReturnedHours, "0.040")
	qaSyncString(t, part.ConfirmedDurationNS, "144000000000")
	qaSyncString(t, part.ProviderDeltaNS, "0")
	qaSyncString(t, part.TotalResidualNS, "-6518000000")
	qaSyncString(t, part.RoundedHours, "0.25")
	status, err := s.SyncStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Totals.ExactDurationNS != "137482000000" {
		t.Fatalf("exact total=%+v", status.Totals)
	}
	qaSyncString(t, status.Totals.TotalResidualNS, "-6518000000")
}
func TestQASyncMissingConsentNeverPosts(t *testing.T) {
	s, _, original := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncEnable(t, s)
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(3)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	item := qaSyncOnlyItem(t, s)
	if len(p.posts) != 0 || item.State != "needs_attention" || !reflect.DeepEqual(item.Interval, original) {
		t.Fatalf("undeclared billing: %+v posts=%d", item, len(p.posts))
	}
}
func TestQASyncUnknownWriteReplayAndFreshRunNeverRetry(t *testing.T) {
	s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	p.createErr = &harvest.Error{Code: "timeout", Uncertain: true}
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	input := SyncRunInput{RequestID: qaSyncID(3)}
	first, err := s.SyncNow(context.Background(), input, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	item := qaSyncOnlyItem(t, s)
	if len(p.posts) != 1 || item.State != "unknown" {
		t.Fatalf("ambiguous result=%+v posts=%d", item, len(p.posts))
	}
	if item.Plan == nil || item.Plan.Parts[0].ConfirmedDurationNS != nil {
		t.Fatalf("unknown fabricated duration: %+v", item.Plan)
	}
	replay, err := qaLegacyNew(Options{Path: path}).SyncNow(context.Background(), input, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(first, replay) {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	_, err = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(4)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.posts) != 1 {
		t.Fatalf("unknown POST retried: %d", len(p.posts))
	}
}

func TestQASyncMissingConsentCanBeRemediatedByExplicitConfiguration(t *testing.T) {
	s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncEnable(t, s)
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(120)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	before := qaSyncOnlyItem(t, s)
	if before.State != "needs_attention" || len(p.posts) != 0 {
		t.Fatalf("missing consent=%+v posts%d", before, len(p.posts))
	}
	qaSyncConfigure(t, s, p)
	_, err = qaLegacyNew(Options{Path: path}).SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(121)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	after := qaSyncOnlyItem(t, s)
	if after.State != "synced" || len(p.posts) != 1 || after.ID != before.ID || !reflect.DeepEqual(after.Interval, before.Interval) {
		t.Fatalf("remediated never-attempted root stuck: %+v posts%d", after, len(p.posts))
	}
}
func TestQASyncPreflightProviderFailureDoesNotPermanentlyDisableQueuedWork(t *testing.T) {
	s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	reached := 0
	bad := SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) {
		reached++
		return nil, &harvest.Error{Code: "network"}
	}}
	_, _ = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(122)}, bad)
	first := qaSyncOnlyItem(t, s)
	if reached != 1 || len(p.posts) != 0 || first.State != "queued" {
		t.Fatalf("transient credential/provider preflight changed eligibility: %+v calls%d", first, reached)
	}
	_, err := qaLegacyNew(Options{Path: path}).SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(123)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	after := qaSyncOnlyItem(t, s)
	if after.State != "synced" || len(p.posts) != 1 {
		t.Fatalf("transient read failure permanently blocked work: %+v posts%d", after, len(p.posts))
	}
}

func TestQASyncLimitAndNormalizedRequestReplayAreBounded(t *testing.T) {
	s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	later := qaSyncAppendCapturedInterval(t, s)
	for _, limit := range []int{-1, 101} {
		_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(180), Limit: limit}, qaSyncNoProvider(t))
		qaCode(t, err, "validation")
	}
	first, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(181), Limit: 1}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	st, _, err := s.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(p.posts) != 1 || len(first.AttemptedIDs) != 1 || st.Outbox[later.ID].State != "queued" || first.RemainingCount != 1 {
		t.Fatalf("limit selection run=%+v later=%+v posts%d", first, st.Outbox[later.ID], len(p.posts))
	}
	// Same ID with changed canonical limit must conflict before provider access.
	_, err = qaLegacyNew(Options{Path: path}).SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(181), Limit: 2}, qaSyncNoProvider(t))
	qaCode(t, err, "request_conflict")
	p.returnedHours = "0.01"
	last, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(182)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := qaLegacyNew(Options{Path: path}).SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(182), Limit: 20}, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(last, replay) {
		t.Fatalf("omitted/default20 fingerprint differs: %+v err%v", replay, err)
	}
	if len(p.posts) != 2 {
		t.Fatalf("bounded root selection/replay posts%d", len(p.posts))
	}
}
