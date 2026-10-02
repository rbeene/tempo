package ui

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/harvest"
)

// These controller tests use synthetic typed callbacks exclusively, without
// opening credentials, activity files, native services or a terminal session.
type qaSyncFlow struct {
	p      *promptBridge
	done   chan error
	cancel context.CancelFunc
}

func qaSyncStart(t *testing.T, c *syncController, a *SyncActions) *qaSyncFlow {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f := &qaSyncFlow{newPromptBridge(ctx), make(chan error, 1), cancel}
	go func() { f.done <- c.run(ctx, f.p, a); close(f.done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-f.done:
		case <-time.After(time.Second):
			t.Error("sync controller did not join")
		}
	})
	return f
}
func (f *qaSyncFlow) next(t *testing.T, kind string) promptRequest {
	t.Helper()
	select {
	case r := <-f.p.requests:
		if r.kind != kind {
			t.Fatalf("prompt=%s want %s (%s)", r.kind, kind, r.title)
		}
		return r
	case e := <-f.done:
		t.Fatalf("sync skipped %s: %v", kind, e)
	case <-time.After(time.Second):
		t.Fatalf("sync stalled before %s", kind)
	}
	return promptRequest{}
}
func (f *qaSyncFlow) finish(t *testing.T) error {
	t.Helper()
	select {
	case e := <-f.done:
		return e
	case <-time.After(time.Second):
		t.Fatal("sync did not finish")
	}
	return nil
}
func qaSyncPick(t *testing.T, r promptRequest, id string) {
	t.Helper()
	for _, v := range r.choices {
		if v.ID == id {
			r.reply <- promptReply{choiceID: id}
			return
		}
	}
	t.Fatalf("missing choice %q: %+v", id, r.choices)
}
func qaSyncHas(t *testing.T, s string, parts ...string) {
	t.Helper()
	for _, v := range parts {
		if !strings.Contains(strings.ToLower(s), strings.ToLower(v)) {
			t.Errorf("visible details omit %q: %s", v, s)
		}
	}
}
func qaSyncView(t *testing.T, f *qaSyncFlow, parts ...string) {
	t.Helper()
	r := f.next(t, "view")
	qaSyncHas(t, r.title+" "+r.body, parts...)
	if strings.Contains(r.title+r.body, "RAW-SYNC-CANARY") {
		t.Error("raw errors or payloads displayed")
	}
	r.reply <- promptReply{}
}
func qaSyncBound(t *testing.T, ctx context.Context, budget time.Duration) {
	t.Helper()
	d, ok := ctx.Deadline()
	if !ok || time.Until(d) > budget+100*time.Millisecond {
		t.Error("callback lacks bounded context")
	}
}

var qaSyncUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

const qaSyncItemID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
const qaSyncOtherID = "11111111-2222-4333-8444-555555555555"

func qaSyncPointer(s string) *string { return &s }
func qaSyncSnapshot() activity.SyncStatus {
	return activity.SyncStatus{ContractVersion: 1, SnapshotRevision: "15", Enabled: false, Configurations: []activity.SyncConfiguration{{AccountID: "11", UserID: "33", Revision: "99", Mode: "duration", DurationPolicy: "exact"}, {AccountID: "11", UserID: "22", Revision: "8", Mode: "duration", DurationPolicy: "nearest-hundredth-hour"}}, Items: []activity.OutboxItem{{ID: qaSyncItemID, Revision: "7", State: "unknown", Interval: activity.Interval{ComputerID: qaSyncOtherID, Attribution: activity.Attribution{AccountID: "11", UserID: "22", ProjectID: "100", TaskID: "200", Timezone: "UTC"}, DurationNS: "137482000000"}, Plan: &activity.SyncPlan{Configuration: activity.SyncConfiguration{AccountID: "11", UserID: "22", Revision: "4", Mode: "duration", DurationPolicy: "nearest-hundredth-hour"}, Parts: []activity.SyncPart{{ID: qaSyncOtherID, State: "unknown", DurationNS: "137482000000", PlannedHours: "0.04", PlannedDurationNS: "144000000000", PlannedResidualNS: "-6518000000"}}}}}, Totals: activity.SyncTotals{ExactDurationNS: "137482000000", PlannedDurationNS: qaSyncPointer("144000000000"), PlannedResidualNS: qaSyncPointer("-6518000000")}}
}
func qaSyncActions(t *testing.T) *SyncActions {
	return &SyncActions{Accounts: func(ctx context.Context) ([]harvest.Object, error) {
		qaSyncBound(t, ctx, 2*time.Minute)
		return []harvest.Object{{"id": "11", "product": "harvest", "name": "Same"}, {"id": "44", "product": "harvest", "name": "Same"}, {"id": "66", "product": "other", "name": "Foreign"}}, nil
	}, Identity: func(ctx context.Context, account string) (activity.SyncAccountIdentity, error) {
		qaSyncBound(t, ctx, 2*time.Minute)
		return activity.SyncAccountIdentity{AccountID: account, UserID: "22"}, nil
	}, Status: func(ctx context.Context) (activity.SyncStatus, error) {
		qaSyncBound(t, ctx, 250*time.Millisecond)
		return qaSyncSnapshot(), nil
	}}
}
func qaSyncConfigureDraft(t *testing.T, f *qaSyncFlow, account, mode, policy, clock string) {
	t.Helper()
	qaSyncPick(t, f.next(t, "choose"), "configure")
	qaSyncPick(t, f.next(t, "choose"), account)
	qaSyncPick(t, f.next(t, "choose"), mode)
	qaSyncPick(t, f.next(t, "choose"), policy)
	if mode == "timestamp" {
		qaSyncPick(t, f.next(t, "choose"), clock)
	}
}
func qaSyncRunResult(id string) activity.SyncRun {
	return activity.SyncRun{ContractVersion: 1, RequestID: id, SnapshotRevision: "16", State: "interrupted", AttemptedIDs: []string{qaSyncItemID}, BlockedIDs: []string{qaSyncItemID}, RemainingCount: 2}
}

func TestQASyncControllerLazyMenuAndBack(t *testing.T) {
	a := &SyncActions{Accounts: func(context.Context) ([]harvest.Object, error) { t.Error("menu read accounts"); return nil, nil }, Status: func(context.Context) (activity.SyncStatus, error) {
		t.Error("menu read status")
		return activity.SyncStatus{}, nil
	}}
	f := qaSyncStart(t, &syncController{}, a)
	r := f.next(t, "choose")
	qaSyncHas(t, r.title, "Sync actions")
	for _, id := range []string{"status", "configure", "now", "reconcile", "resolve", "pause", "resume", "back"} {
		found := false
		for _, v := range r.choices {
			found = found || v.ID == id
		}
		if !found {
			t.Errorf("menu missing %s", id)
		}
	}
	qaSyncPick(t, r, "back")
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
}
func TestQASyncControllerStatusLocalBoundedAndTruthful(t *testing.T) {
	a := qaSyncActions(t)
	a.Accounts = func(context.Context) ([]harvest.Object, error) {
		t.Error("status read credentials/accounts")
		return nil, nil
	}
	a.Identity = func(context.Context, string) (activity.SyncAccountIdentity, error) {
		t.Error("status verified remote identity")
		return activity.SyncAccountIdentity{}, nil
	}
	f := qaSyncStart(t, &syncController{}, a)
	qaSyncPick(t, f.next(t, "choose"), "status")
	r := f.next(t, "view")
	qaSyncHas(t, r.body, "false", "137482000000", "144000000000", "-6518000000", "unknown", "reconciliation")
	if strings.Contains(strings.ToLower(r.body), "confirmed returned 0") {
		t.Error("missing confirmed accounting presented as zero")
	}
	r.reply <- promptReply{}
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
}
func TestQASyncControllerConfigureVerifiedPairAndFullConsent(t *testing.T) {
	for _, tc := range []struct{ account, mode, policy, clock, revision string }{{"11", "duration", "nearest-hundredth-hour", "", "8"}, {"44", "duration", "exact", "", "0"}, {"11", "timestamp", "exact", "12h", "8"}, {"11", "timestamp", "exact", "24h", "8"}} {
		for _, yes := range []bool{false, true} {
			t.Run(tc.account+"/"+tc.mode+tc.clock+map[bool]string{true: "accept", false: "decline"}[yes], func(t *testing.T) {
				a := qaSyncActions(t)
				var order []string
				identity := a.Identity
				status := a.Status
				a.Identity = func(ctx context.Context, account string) (activity.SyncAccountIdentity, error) {
					order = append(order, "identity")
					return identity(ctx, account)
				}
				a.Status = func(ctx context.Context) (activity.SyncStatus, error) {
					order = append(order, "status")
					return status(ctx)
				}
				calls := 0
				var submitted activity.SyncConfigureInput
				a.Configure = func(ctx context.Context, in activity.SyncConfigureInput) (activity.SyncConfigurationResult, error) {
					calls++
					submitted = in
					qaSyncBound(t, ctx, 2*time.Minute)
					if in.AccountID != tc.account || in.UserID != "22" || in.Mode != tc.mode || in.DurationPolicy != tc.policy || in.Clock != tc.clock || in.IfRevision != tc.revision || !in.Confirmed || !qaSyncUUID.MatchString(in.RequestID) {
						t.Errorf("configure replaced reviewed consent: %+v", in)
					}
					return activity.SyncConfigurationResult{ContractVersion: 1, RequestID: in.RequestID, Configuration: activity.SyncConfiguration{AccountID: in.AccountID, UserID: in.UserID, Revision: "9", Mode: in.Mode, DurationPolicy: in.DurationPolicy}}, nil
				}
				a.Resume = func(context.Context, string) (activity.MutationResult, error) {
					t.Error("configure enabled uploads")
					return activity.MutationResult{}, nil
				}
				a.Now = func(context.Context, activity.SyncRunInput) (activity.SyncRun, error) {
					t.Error("configure submitted Harvest write")
					return activity.SyncRun{}, nil
				}
				f := qaSyncStart(t, &syncController{}, a)
				qaSyncConfigureDraft(t, f, tc.account, tc.mode, tc.policy, tc.clock)
				r := f.next(t, "confirm")
				qaSyncHas(t, r.title, tc.account, "22", tc.revision, tc.mode, tc.policy, "history", "enable", "upload")
				if tc.clock != "" {
					qaSyncHas(t, r.title, tc.clock)
				}
				if tc.policy == "nearest-hundredth-hour" {
					qaSyncHas(t, r.title, "36", "18", "round")
				}
				if !reflect.DeepEqual(order, []string{"identity", "status"}) || calls != 0 {
					t.Errorf("preconsent order=%v calls%d", order, calls)
				}
				r.reply <- promptReply{confirmed: yes}
				if yes {
					view := f.next(t, "view")
					qaSyncHas(t, view.body, "request", submitted.RequestID)
					view.reply <- promptReply{}
				}
				_ = f.finish(t)
				if calls != map[bool]int{true: 1, false: 0}[yes] {
					t.Errorf("configure calls%d", calls)
				}
			})
		}
	}
}
func TestQASyncControllerConfigureIdentityFailureDoesNotMutate(t *testing.T) {
	a := qaSyncActions(t)
	a.Identity = func(context.Context, string) (activity.SyncAccountIdentity, error) {
		return activity.SyncAccountIdentity{}, errors.New("RAW-SYNC-CANARY")
	}
	a.Configure = func(context.Context, activity.SyncConfigureInput) (activity.SyncConfigurationResult, error) {
		t.Error("failed identity configured sync")
		return activity.SyncConfigurationResult{}, nil
	}
	a.Status = func(context.Context) (activity.SyncStatus, error) {
		t.Error("identity failure probed status")
		return activity.SyncStatus{}, nil
	}
	f := qaSyncStart(t, &syncController{}, a)
	qaSyncPick(t, f.next(t, "choose"), "configure")
	qaSyncPick(t, f.next(t, "choose"), "11")
	qaSyncView(t, f, "failed")
	_ = f.finish(t)
}
func TestQASyncControllerNowLimitConsentAndBlockedResult(t *testing.T) {
	for _, limit := range []string{"1", "20", "100"} {
		for _, yes := range []bool{true, false} {
			t.Run(limit+map[bool]string{true: "accept", false: "decline"}[yes], func(t *testing.T) {
				a := qaSyncActions(t)
				calls := 0
				var submitted activity.SyncRunInput
				a.Now = func(ctx context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
					calls++
					submitted = in
					qaSyncBound(t, ctx, 2*time.Minute)
					wantLimit, _ := strconv.Atoi(limit)
					if !qaSyncUUID.MatchString(in.RequestID) || in.Limit != wantLimit {
						t.Errorf("Now changed durable identity or limit: %+v", in)
					}
					return qaSyncRunResult(in.RequestID), nil
				}
				a.Identity = func(context.Context, string) (activity.SyncAccountIdentity, error) {
					t.Error("Now re-resolved queue account")
					return activity.SyncAccountIdentity{}, nil
				}
				f := qaSyncStart(t, &syncController{}, a)
				qaSyncPick(t, f.next(t, "choose"), "now")
				r := f.next(t, "text")
				if r.defaultText != "20" {
					t.Errorf("default limit=%q", r.defaultText)
				}
				r.reply <- promptReply{text: limit}
				r = f.next(t, "confirm")
				qaSyncHas(t, r.title, limit, "unknown", "retry", "attribution", "upload")
				if calls != 0 {
					t.Error("Now submitted before confirmation")
				}
				r.reply <- promptReply{confirmed: yes}
				if yes {
					view := f.next(t, "view")
					qaSyncHas(t, view.body, "interrupted", "blocked", "remaining", "2", submitted.RequestID, "137482000000", "-6518000000")
					view.reply <- promptReply{}
				}
				_ = f.finish(t)
				if calls != map[bool]int{true: 1, false: 0}[yes] {
					t.Errorf("Now calls%d", calls)
				}
			})
		}
	}
}
func TestQASyncControllerInvalidLimitNeverDispatches(t *testing.T) {
	for _, op := range []string{"now", "reconcile"} {
		for _, limit := range []string{"0", "101", "-1", "1.5", "", "RAW-SYNC-CANARY"} {
			t.Run(op+"/"+limit, func(t *testing.T) {
				a := qaSyncActions(t)
				a.Now = func(context.Context, activity.SyncRunInput) (activity.SyncRun, error) {
					t.Error("bad limit reached Now")
					return activity.SyncRun{}, nil
				}
				a.Reconcile = func(context.Context, activity.SyncReconcileInput) (activity.SyncRun, error) {
					t.Error("bad limit reached reconcile")
					return activity.SyncRun{}, nil
				}
				f := qaSyncStart(t, &syncController{}, a)
				qaSyncPick(t, f.next(t, "choose"), op)
				if op == "reconcile" {
					qaSyncPick(t, f.next(t, "choose"), "all")
				}
				f.next(t, "text").reply <- promptReply{text: limit}
				qaSyncView(t, f, "validation")
				_ = f.finish(t)
			})
		}
	}
}
func TestQASyncControllerReconcileGETOnlyExactSelection(t *testing.T) {
	for _, target := range []string{qaSyncItemID, "all"} {
		t.Run(target, func(t *testing.T) {
			a := qaSyncActions(t)
			calls := 0
			var submitted activity.SyncReconcileInput
			a.Reconcile = func(ctx context.Context, in activity.SyncReconcileInput) (activity.SyncRun, error) {
				calls++
				submitted = in
				qaSyncBound(t, ctx, 2*time.Minute)
				want := target
				if want == "all" {
					want = ""
				}
				if in.OutboxID != want || in.Limit != 20 || !qaSyncUUID.MatchString(in.RequestID) {
					t.Errorf("reconcile input=%+v", in)
				}
				return qaSyncRunResult(in.RequestID), nil
			}
			a.Now = func(context.Context, activity.SyncRunInput) (activity.SyncRun, error) {
				t.Error("reconciliation submitted another POST")
				return activity.SyncRun{}, nil
			}
			f := qaSyncStart(t, &syncController{}, a)
			qaSyncPick(t, f.next(t, "choose"), "reconcile")
			qaSyncPick(t, f.next(t, "choose"), target)
			f.next(t, "text").reply <- promptReply{text: "20"}
			view := f.next(t, "view")
			qaSyncHas(t, view.body, submitted.RequestID, "interrupted", "blocked", "remaining")
			view.reply <- promptReply{}
			if e := f.finish(t); e != nil {
				t.Fatal(e)
			}
			if calls != 1 {
				t.Errorf("reconcile calls%d", calls)
			}
		})
	}
}
func TestQASyncControllerPauseResumeConsentAndNoOtherLifecycle(t *testing.T) {
	for _, op := range []string{"pause", "resume"} {
		for _, yes := range []bool{true, false} {
			t.Run(op+map[bool]string{true: "accept", false: "decline"}[yes], func(t *testing.T) {
				a := qaSyncActions(t)
				calls := 0
				callback := func(ctx context.Context, id string) (activity.MutationResult, error) {
					calls++
					qaSyncBound(t, ctx, 2*time.Minute)
					if !qaSyncUUID.MatchString(id) {
						t.Error("control missing UUID")
					}
					return activity.MutationResult{ContractVersion: 1, RequestID: id, SnapshotRevision: "16"}, nil
				}
				if op == "pause" {
					a.Pause = callback
				} else {
					a.Resume = callback
				}
				a.Now = func(context.Context, activity.SyncRunInput) (activity.SyncRun, error) {
					t.Error("pause/resume launched sync pass")
					return activity.SyncRun{}, nil
				}
				a.Configure = func(context.Context, activity.SyncConfigureInput) (activity.SyncConfigurationResult, error) {
					t.Error("pause/resume changed consent")
					return activity.SyncConfigurationResult{}, nil
				}
				f := qaSyncStart(t, &syncController{}, a)
				qaSyncPick(t, f.next(t, "choose"), op)
				r := f.next(t, "confirm")
				qaSyncHas(t, r.title, op, "capture", "worker")
				if calls != 0 {
					t.Error("control preceded confirmation")
				}
				r.reply <- promptReply{confirmed: yes}
				if yes {
					qaSyncView(t, f, "request")
				}
				_ = f.finish(t)
				if calls != map[bool]int{true: 1, false: 0}[yes] {
					t.Errorf("control calls%d", calls)
				}
			})
		}
	}
}
func TestQASyncControllerCancellationBeforeDispatchHasNoEffect(t *testing.T) {
	a := qaSyncActions(t)
	var effects atomic.Int32
	a.Now = func(context.Context, activity.SyncRunInput) (activity.SyncRun, error) {
		effects.Add(1)
		return activity.SyncRun{}, nil
	}
	f := qaSyncStart(t, &syncController{}, a)
	qaSyncPick(t, f.next(t, "choose"), "now")
	f.next(t, "text").reply <- promptReply{text: "20"}
	_ = f.next(t, "confirm")
	f.cancel()
	if e := f.finish(t); !errors.Is(e, context.Canceled) {
		t.Errorf("cancel result=%v", e)
	}
	if effects.Load() != 0 {
		t.Fatal("cancel submitted upload")
	}
}
func TestQASyncControllerDefiniteConflictSafeAndRenewsIntent(t *testing.T) {
	for _, code := range []string{"revision_conflict", "identity_conflict", "request_conflict", "RAW-SYNC-CANARY"} {
		t.Run(code, func(t *testing.T) {
			a := qaSyncActions(t)
			var ids []string
			a.Configure = func(ctx context.Context, in activity.SyncConfigureInput) (activity.SyncConfigurationResult, error) {
				ids = append(ids, in.RequestID)
				return activity.SyncConfigurationResult{}, &activity.Error{Code: code, Message: "RAW-SYNC-CANARY", Details: map[string]any{"body": "RAW-SYNC-CANARY"}}
			}
			c := &syncController{}
			for i := 0; i < 2; i++ {
				f := qaSyncStart(t, c, a)
				qaSyncConfigureDraft(t, f, "11", "duration", "exact", "")
				f.next(t, "confirm").reply <- promptReply{confirmed: true}
				qaSyncView(t, f, "review")
				_ = f.finish(t)
				if c.pending != nil {
					t.Fatal("definite conflict retained unknown intent")
				}
			}
			if len(ids) != 2 || ids[0] == ids[1] {
				t.Errorf("fresh review reused failed ID: %v", ids)
			}
		})
	}
}
