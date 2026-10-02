package ui

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/harvest"
)

func qaSyncBegin(t *testing.T, f *qaSyncFlow, op string) {
	t.Helper()
	if op == "configure" {
		qaSyncConfigureDraft(t, f, "11", "duration", "nearest-hundredth-hour", "")
		f.next(t, "confirm").reply <- promptReply{confirmed: true}
		return
	}
	qaSyncPick(t, f.next(t, "choose"), op)
	switch op {
	case "now":
		f.next(t, "text").reply <- promptReply{text: "20"}
		f.next(t, "confirm").reply <- promptReply{confirmed: true}
	case "reconcile":
		qaSyncPick(t, f.next(t, "choose"), "all")
		f.next(t, "text").reply <- promptReply{text: "20"}
	case "resolve":
		qaSyncPick(t, f.next(t, "choose"), qaSyncItemID)
		qaSyncPick(t, f.next(t, "choose"), "attach")
		f.next(t, "text").reply <- promptReply{text: "900"}
		f.next(t, "confirm").reply <- promptReply{confirmed: true}
	case "pause", "resume":
		f.next(t, "confirm").reply <- promptReply{confirmed: true}
	}
}
func qaSyncReceipt(in any) string {
	switch v := in.(type) {
	case activity.SyncConfigureInput:
		return v.RequestID
	case activity.SyncRunInput:
		return v.RequestID
	case activity.SyncReconcileInput:
		return v.RequestID
	case activity.SyncResolveInput:
		return v.RequestID
	case string:
		return v
	}
	return ""
}
func TestQASyncControllerResolveFreezesTargetAndRequiresConsent(t *testing.T) {
	for _, op := range []string{"attach", "retry-rejected"} {
		for _, yes := range []bool{false, true} {
			t.Run(op+map[bool]string{false: "decline", true: "accept"}[yes], func(t *testing.T) {
				a := qaSyncActions(t)
				snapshot := qaSyncSnapshot()
				if op == "retry-rejected" {
					snapshot.Items[0].State = "needs_attention"
					snapshot.Items[0].Plan.Parts = append(snapshot.Items[0].Plan.Parts, activity.SyncPart{ID: "22222222-3333-4444-8555-666666666666", State: "synced", PlannedDurationNS: "36000000000"})
					snapshot.Items[0].Plan.Parts[0].State = "rejected"
				}
				a.Status = func(ctx context.Context) (activity.SyncStatus, error) {
					qaSyncBound(t, ctx, 250*time.Millisecond)
					return snapshot, nil
				}
				calls := 0
				var submitted activity.SyncResolveInput
				a.Resolve = func(ctx context.Context, in activity.SyncResolveInput) (activity.MutationResult, error) {
					calls++
					submitted = in
					qaSyncBound(t, ctx, 2*time.Minute)
					entry := "900"
					if op == "retry-rejected" {
						entry = ""
					}
					if in.OutboxID != qaSyncItemID || in.IfRevision != "7" || in.EntryID != entry || in.RetryRejected != (op == "retry-rejected") || !in.Confirmed || !qaSyncUUID.MatchString(in.RequestID) {
						t.Errorf("resolve changed immutable target: %+v", in)
					}
					return activity.MutationResult{ContractVersion: 1, RequestID: in.RequestID, SnapshotRevision: "16", AffectedIDs: []string{qaSyncItemID}}, nil
				}
				a.Now = func(context.Context, activity.SyncRunInput) (activity.SyncRun, error) {
					t.Error("Resolve launched POST pass")
					return activity.SyncRun{}, nil
				}
				f := qaSyncStart(t, &syncController{}, a)
				qaSyncPick(t, f.next(t, "choose"), "resolve")
				qaSyncPick(t, f.next(t, "choose"), qaSyncItemID)
				qaSyncPick(t, f.next(t, "choose"), op)
				if op == "attach" {
					f.next(t, "text").reply <- promptReply{text: "900"}
				}
				r := f.next(t, "confirm")
				qaSyncHas(t, r.title, qaSyncItemID, "7", "11", "22", "100", "200", "UTC", "137482000000", "144000000000", op, "POST")
				if op == "attach" {
					qaSyncHas(t, r.title, "900", "stopped")
				}
				if calls != 0 {
					t.Error("resolve dispatched before consent")
				}
				snapshot.Items[0].Revision = "100"
				snapshot.Items[0].Interval.Attribution.AccountID = "44"
				r.reply <- promptReply{confirmed: yes}
				if yes {
					view := f.next(t, "view")
					qaSyncHas(t, view.body, submitted.RequestID)
					view.reply <- promptReply{}
				}
				_ = f.finish(t)
				if calls != map[bool]int{false: 0, true: 1}[yes] {
					t.Errorf("resolve calls%d", calls)
				}
			})
		}
	}
}
func TestQASyncControllerUnknownOrSubmittingPartsCannotAuthorizeRejectedRetry(t *testing.T) {
	for _, state := range []string{"unknown", "submitting", "needs_attention"} {
		t.Run(state, func(t *testing.T) {
			a := qaSyncActions(t)
			list := qaSyncSnapshot()
			list.Items[0].State = "rejected"
			list.Items[0].Plan.Parts[0].State = state
			list.Items[0].Plan.Parts = append(list.Items[0].Plan.Parts, activity.SyncPart{State: "rejected"})
			a.Status = func(context.Context) (activity.SyncStatus, error) { return list, nil }
			a.Resolve = func(context.Context, activity.SyncResolveInput) (activity.MutationResult, error) {
				t.Error("unknown part authorized retry")
				return activity.MutationResult{}, nil
			}
			f := qaSyncStart(t, &syncController{}, a)
			qaSyncPick(t, f.next(t, "choose"), "resolve")
			qaSyncPick(t, f.next(t, "choose"), qaSyncItemID)
			r := f.next(t, "choose")
			for _, choice := range r.choices {
				if choice.ID == "retry-rejected" {
					t.Error("retry offered for ambiguous part despite rejected root label")
				}
			}
			f.cancel()
			_ = f.finish(t)
		})
	}
}
func TestQASyncControllerInvalidEntryHasNoResolveEffect(t *testing.T) {
	for _, entry := range []string{"", "0", "-1", "abc", "900\nRAW-SYNC-CANARY"} {
		t.Run(entry, func(t *testing.T) {
			a := qaSyncActions(t)
			a.Resolve = func(context.Context, activity.SyncResolveInput) (activity.MutationResult, error) {
				t.Error("invalid attachment reached service")
				return activity.MutationResult{}, nil
			}
			f := qaSyncStart(t, &syncController{}, a)
			qaSyncPick(t, f.next(t, "choose"), "resolve")
			qaSyncPick(t, f.next(t, "choose"), qaSyncItemID)
			qaSyncPick(t, f.next(t, "choose"), "attach")
			f.next(t, "text").reply <- promptReply{text: entry}
			qaSyncView(t, f, "validation")
			_ = f.finish(t)
		})
	}
}
func TestQASyncControllerLocalLedgerUnknownRetainsExactInputAcrossReopen(t *testing.T) {
	for _, op := range []string{"configure", "now", "reconcile", "resolve", "pause", "resume"} {
		t.Run(op, func(t *testing.T) {
			a := qaSyncActions(t)
			uncertain := &activity.Error{Code: "local_write_unknown", Message: "RAW-SYNC-CANARY", Uncertain: true}
			var submitted []any
			record := func(ctx context.Context, in any) bool {
				qaSyncBound(t, ctx, 2*time.Minute)
				submitted = append(submitted, in)
				return len(submitted) == 1
			}
			a.Configure = func(ctx context.Context, in activity.SyncConfigureInput) (activity.SyncConfigurationResult, error) {
				if record(ctx, in) {
					return activity.SyncConfigurationResult{}, uncertain
				}
				return activity.SyncConfigurationResult{ContractVersion: 1, RequestID: in.RequestID}, nil
			}
			a.Now = func(ctx context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
				if record(ctx, in) {
					return activity.SyncRun{}, uncertain
				}
				return qaSyncRunResult(in.RequestID), nil
			}
			a.Reconcile = func(ctx context.Context, in activity.SyncReconcileInput) (activity.SyncRun, error) {
				if record(ctx, in) {
					return activity.SyncRun{}, uncertain
				}
				return qaSyncRunResult(in.RequestID), nil
			}
			a.Resolve = func(ctx context.Context, in activity.SyncResolveInput) (activity.MutationResult, error) {
				if record(ctx, in) {
					return activity.MutationResult{}, uncertain
				}
				return activity.MutationResult{ContractVersion: 1, RequestID: in.RequestID}, nil
			}
			control := func(ctx context.Context, id string) (activity.MutationResult, error) {
				if record(ctx, id) {
					return activity.MutationResult{}, uncertain
				}
				return activity.MutationResult{ContractVersion: 1, RequestID: id}, nil
			}
			a.Pause = control
			a.Resume = control
			c := &syncController{}
			f := qaSyncStart(t, c, a)
			qaSyncBegin(t, f, op)
			r := f.next(t, "view")
			id := qaSyncReceipt(submitted[0])
			qaSyncHas(t, r.body, "local_write_unknown", id, "same", "nonapplication")
			if op == "configure" {
				qaSyncHas(t, r.body, "11", "22", "8", "nearest-hundredth-hour")
			}
			if op == "resolve" {
				qaSyncHas(t, r.body, qaSyncItemID, "7", "900")
			}
			if strings.Contains(r.body, "RAW-SYNC-CANARY") {
				t.Error("raw uncertain error disclosed")
			}
			r.reply <- promptReply{}
			qaSyncPick(t, f.next(t, "choose"), "back")
			if !errors.Is(f.finish(t), uncertain) || c.pending == nil {
				t.Fatal("unknown lost when leaving flow")
			}
			a.Accounts = func(context.Context) ([]harvest.Object, error) { t.Error("pending reread accounts"); return nil, nil }
			a.Identity = func(context.Context, string) (activity.SyncAccountIdentity, error) {
				t.Error("pending reverified identity")
				return activity.SyncAccountIdentity{}, nil
			}
			a.Status = func(ctx context.Context) (activity.SyncStatus, error) {
				qaSyncBound(t, ctx, 250*time.Millisecond)
				list := qaSyncSnapshot()
				list.Configurations[1].Revision = "90"
				list.Items[0].Revision = "80"
				return list, nil
			}
			g := qaSyncStart(t, c, a)
			qaSyncPick(t, g.next(t, "choose"), "status")
			qaSyncView(t, g, id, "read")
			if len(submitted) != 1 {
				t.Error("read-only status replayed mutation")
			}
			qaSyncPick(t, g.next(t, "choose"), "replay")
			qaSyncView(t, g, id)
			if e := g.finish(t); e != nil {
				t.Fatal(e)
			}
			if c.pending != nil || len(submitted) != 2 || !reflect.DeepEqual(submitted[0], submitted[1]) {
				t.Errorf("replay changed exact frozen input: %+v", submitted)
			}
		})
	}
}
func TestQASyncControllerRemoteUnknownNeverOffersOrRetriesPOST(t *testing.T) {
	a := qaSyncActions(t)
	remote := &harvest.Error{Code: "uncertain_write", Message: "RAW-SYNC-CANARY", Uncertain: true}
	var now []activity.SyncRunInput
	var reconciled []activity.SyncReconcileInput
	a.Now = func(ctx context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
		now = append(now, in)
		return activity.SyncRun{}, remote
	}
	a.Reconcile = func(ctx context.Context, in activity.SyncReconcileInput) (activity.SyncRun, error) {
		qaSyncBound(t, ctx, 2*time.Minute)
		reconciled = append(reconciled, in)
		return qaSyncRunResult(in.RequestID), nil
	}
	c := &syncController{}
	f := qaSyncStart(t, c, a)
	qaSyncBegin(t, f, "now")
	r := f.next(t, "view")
	qaSyncHas(t, r.body, "uncertain_write", now[0].RequestID, "reconcil", "absence")
	if strings.Contains(r.body, "RAW-SYNC-CANARY") {
		t.Error("raw transport error disclosed")
	}
	r.reply <- promptReply{}
	r = f.next(t, "choose")
	for _, v := range r.choices {
		if v.ID == "replay" || v.ID == "now" {
			t.Error("remote uncertainty offered POST replay")
		}
	}
	qaSyncPick(t, r, "reconcile")
	qaSyncPick(t, f.next(t, "choose"), "all")
	f.next(t, "text").reply <- promptReply{text: "10"}
	qaSyncView(t, f, "interrupted", "blocked", "remaining")
	qaSyncPick(t, f.next(t, "choose"), "back")
	if !errors.Is(f.finish(t), remote) || c.pending == nil {
		t.Fatal("GET result acknowledged original ambiguous POST")
	}
	if len(now) != 1 || len(reconciled) != 1 || reconciled[0].RequestID == now[0].RequestID || reconciled[0].Limit != 10 || reconciled[0].OutboxID != "" {
		t.Errorf("remote reconciliation changed writer contract: now%+v reconcile%+v", now, reconciled)
	}
	g := qaSyncStart(t, c, a)
	qaSyncPick(t, g.next(t, "choose"), "status")
	qaSyncView(t, g, now[0].RequestID, "read")
	qaSyncPick(t, g.next(t, "choose"), "back")
	if !errors.Is(g.finish(t), remote) || len(now) != 1 {
		t.Fatal("reopen/status retried or erased POST uncertainty")
	}
}
func TestQASyncControllerRemoteAndReconcileUnknownRetainBothReceipts(t *testing.T) {
	a := qaSyncActions(t)
	remote := &harvest.Error{Code: "uncertain_write", Uncertain: true}
	local := &activity.Error{Code: "local_write_unknown", Uncertain: true}
	var now activity.SyncRunInput
	var reads []activity.SyncReconcileInput
	a.Now = func(ctx context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
		now = in
		return activity.SyncRun{}, remote
	}
	a.Reconcile = func(ctx context.Context, in activity.SyncReconcileInput) (activity.SyncRun, error) {
		reads = append(reads, in)
		if len(reads) == 1 {
			return activity.SyncRun{}, local
		}
		return qaSyncRunResult(in.RequestID), nil
	}
	c := &syncController{}
	f := qaSyncStart(t, c, a)
	qaSyncBegin(t, f, "now")
	qaSyncView(t, f, "uncertain_write")
	qaSyncPick(t, f.next(t, "choose"), "reconcile")
	qaSyncPick(t, f.next(t, "choose"), qaSyncItemID)
	f.next(t, "text").reply <- promptReply{text: "20"}
	r := f.next(t, "view")
	qaSyncHas(t, r.body, "local_write_unknown", now.RequestID, reads[0].RequestID)
	r.reply <- promptReply{}
	qaSyncPick(t, f.next(t, "choose"), "back")
	if !errors.Is(f.finish(t), remote) {
		t.Fatal("nested local unknown replaced original remote uncertainty")
	}
	a.Status = func(context.Context) (activity.SyncStatus, error) {
		if len(reads) < 2 {
			t.Error("exact pending reconciliation rediscovered targets before replay")
		}
		return qaSyncSnapshot(), nil
	}
	g := qaSyncStart(t, c, a)
	qaSyncPick(t, g.next(t, "choose"), "reconcile")
	qaSyncView(t, g, "interrupted", "blocked")
	qaSyncPick(t, g.next(t, "choose"), "back")
	if !errors.Is(g.finish(t), remote) || c.pending == nil || len(reads) != 2 || !reflect.DeepEqual(reads[0], reads[1]) {
		t.Errorf("nested recovery changed identities: %+v", reads)
	}
}
func TestQASyncControllerCancelAfterDispatchRetainsLocalUnknown(t *testing.T) {
	a := qaSyncActions(t)
	var calls atomic.Int32
	started := make(chan struct{})
	a.Now = func(ctx context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		return activity.SyncRun{}, &activity.Error{Code: "local_write_unknown", Uncertain: true}
	}
	c := &syncController{}
	f := qaSyncStart(t, c, a)
	qaSyncBegin(t, f, "now")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Now did not dispatch")
	}
	f.cancel()
	if !unknownOutcome(f.finish(t)) || c.pending == nil || calls.Load() != 1 {
		t.Fatal("cancellation erased postdispatch unknown")
	}
}
func TestQASyncControllerUnavailableActionsSafe(t *testing.T) {
	for _, op := range []string{"status", "configure", "now", "reconcile", "resolve", "pause", "resume"} {
		t.Run(op, func(t *testing.T) {
			f := qaSyncStart(t, &syncController{}, &SyncActions{})
			qaSyncPick(t, f.next(t, "choose"), op)
			qaSyncView(t, f, "unavailable")
			_ = f.finish(t)
		})
	}
}
