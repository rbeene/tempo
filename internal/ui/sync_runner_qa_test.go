package ui_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
)

const qaSyncRunnerItem = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"

func qaSyncRunnerStatus() activity.SyncStatus {
	return activity.SyncStatus{SnapshotRevision: "15", Configurations: []activity.SyncConfiguration{{AccountID: "11", UserID: "33", Revision: "99"}, {AccountID: "11", UserID: "22", Revision: "8"}}, Items: []activity.OutboxItem{{ID: qaSyncRunnerItem, Revision: "7", State: "unknown", Interval: activity.Interval{Attribution: activity.Attribution{AccountID: "11", UserID: "22", ProjectID: "100", TaskID: "200", Timezone: "UTC"}, DurationNS: "137482000000"}}}, Totals: activity.SyncTotals{ExactDurationNS: "137482000000"}}
}
func qaSyncRunnerActions() *ui.SyncActions {
	return &ui.SyncActions{Accounts: func(context.Context) ([]harvest.Object, error) {
		return []harvest.Object{{"id": "11", "product": "harvest"}}, nil
	}, Identity: func(_ context.Context, id string) (activity.SyncAccountIdentity, error) {
		return activity.SyncAccountIdentity{AccountID: id, UserID: "22"}, nil
	}, Status: func(context.Context) (activity.SyncStatus, error) { return qaSyncRunnerStatus(), nil }}
}
func qaSyncRunnerRig(t *testing.T, a *ui.SyncActions, outcome func(string, string, error)) *qaRunnerRig {
	t.Helper()
	s := qaNewRunnerScreen()
	r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("10", "100"), nil }}
	x := qaStartRunnerOptions(t, s, r, ui.Options{Refresh: make(chan time.Time), Sync: a, OnRetainedOutcome: outcome})
	x.frame(t, "Project 100")
	return x
}
func qaSyncRunnerPick(t *testing.T, x *qaRunnerRig, title, id string) {
	t.Helper()
	qaLinkFrame(t, x, title)
	qaLinkKey(x, id)
	qaLinkEnter(x)
}
func qaSyncRunnerOpen(t *testing.T, x *qaRunnerRig, op string) {
	t.Helper()
	qaLinkKey(x, "s")
	qaSyncRunnerPick(t, x, "Sync actions", op)
}
func qaSyncRunnerConfirm(t *testing.T, x *qaRunnerRig) {
	t.Helper()
	qaLinkFrame(t, x, "Review scoped change")
	for i := 0; i < 70; i++ {
		x.screen.events <- terminal.Event{Kind: "down"}
	}
	qaLinkFrame(t, x, "Request ID")
	qaLinkYes(x)
}
func qaSyncRunnerDone(t *testing.T, x *qaRunnerRig, title string) {
	t.Helper()
	qaLinkFrame(t, x, title)
	qaLinkEnter(x)
	x.frame(t, "Project 100")
}
func qaSyncRunnerQuit(t *testing.T, x *qaRunnerRig) { t.Helper(); qaLinkKey(x, "q"); x.finish(t, nil) }

func TestQAUISyncRunnerLazyBackAndReadRoute(t *testing.T) {
	var reads atomic.Int32
	a := qaSyncRunnerActions()
	a.Status = func(context.Context) (activity.SyncStatus, error) { reads.Add(1); return qaSyncRunnerStatus(), nil }
	a.Accounts = func(context.Context) ([]harvest.Object, error) { t.Error("local route read accounts"); return nil, nil }
	x := qaSyncRunnerRig(t, a, nil)
	qaSyncRunnerOpen(t, x, "back")
	x.frame(t, "Project 100")
	if reads.Load() != 0 {
		t.Error("menu/Back read Sync state")
	}
	qaSyncRunnerOpen(t, x, "status")
	qaLinkFrame(t, x, "Exact captured 137482000000 ns")
	qaLinkEnter(x)
	x.frame(t, "Project 100")
	if reads.Load() != 1 {
		t.Error("status missing/duplicated read")
	}
	qaSyncRunnerQuit(t, x)
}
func TestQAUISyncRunnerConfigureFrozenPairAndConsent(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "cancel"}[cancel], func(t *testing.T) {
			a := qaSyncRunnerActions()
			var calls atomic.Int32
			status := qaSyncRunnerStatus()
			a.Status = func(context.Context) (activity.SyncStatus, error) { return status, nil }
			a.Configure = func(_ context.Context, in activity.SyncConfigureInput) (activity.SyncConfigurationResult, error) {
				calls.Add(1)
				if in.AccountID != "11" || in.UserID != "22" || in.IfRevision != "8" || in.DurationPolicy != "nearest-hundredth-hour" || !in.Confirmed || !qaUILinkUUID.MatchString(in.RequestID) {
					t.Errorf("retargeted consent: %+v", in)
				}
				return activity.SyncConfigurationResult{RequestID: in.RequestID}, nil
			}
			x := qaSyncRunnerRig(t, a, nil)
			qaSyncRunnerOpen(t, x, "configure")
			qaSyncRunnerPick(t, x, "Choose a Harvest account", "11")
			qaSyncRunnerPick(t, x, "Tracking mode", "duration")
			qaSyncRunnerPick(t, x, "Duration policy", "nearest-hundredth-hour")
			qaLinkFrame(t, x, "Review scoped change")
			if calls.Load() != 0 {
				t.Fatal("unconfirmed Configure")
			}
			status.Configurations[1].Revision = "88"
			status.Configurations[1].UserID = "44"
			if cancel {
				qaLinkEscape(x)
				x.frame(t, "Project 100")
			} else {
				for i := 0; i < 70; i++ {
					x.screen.events <- terminal.Event{Kind: "down"}
				}
				qaLinkFrame(t, x, "Request ID")
				qaLinkYes(x)
				qaSyncRunnerDone(t, x, "Sync · Local operation complete")
			}
			qaSyncRunnerQuit(t, x)
			if calls.Load() != map[bool]int32{false: 1, true: 0}[cancel] {
				t.Error("Configure effects escaped consent")
			}
		})
	}
}
func TestQAUISyncRunnerAllMutationRoutes(t *testing.T) {
	for _, op := range []string{"now", "reconcile", "resolve", "pause", "resume"} {
		t.Run(op, func(t *testing.T) {
			a := qaSyncRunnerActions()
			var calls atomic.Int32
			result := func(id string) activity.MutationResult {
				calls.Add(1)
				if !qaUILinkUUID.MatchString(id) {
					t.Error("not UUID")
				}
				return activity.MutationResult{RequestID: id}
			}
			a.Now = func(_ context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
				result(in.RequestID)
				if in.Limit != 20 {
					t.Error("limit")
				}
				return activity.SyncRun{RequestID: in.RequestID, State: "interrupted", BlockedIDs: []string{qaSyncRunnerItem}, RemainingCount: 2}, nil
			}
			a.Reconcile = func(_ context.Context, in activity.SyncReconcileInput) (activity.SyncRun, error) {
				result(in.RequestID)
				if in.OutboxID != "" || in.Limit != 20 {
					t.Error("reconcile target/limit")
				}
				return activity.SyncRun{RequestID: in.RequestID, State: "complete"}, nil
			}
			a.Resolve = func(_ context.Context, in activity.SyncResolveInput) (activity.MutationResult, error) {
				if in.OutboxID != qaSyncRunnerItem || in.IfRevision != "7" || in.EntryID != "901" || in.RetryRejected || !in.Confirmed {
					t.Errorf("resolve retargeted %+v", in)
				}
				return result(in.RequestID), nil
			}
			a.Pause = func(_ context.Context, id string) (activity.MutationResult, error) { return result(id), nil }
			a.Resume = a.Pause
			x := qaSyncRunnerRig(t, a, nil)
			qaSyncRunnerOpen(t, x, op)
			switch op {
			case "now":
				qaLinkFrame(t, x, "Bounded batch limit")
				qaLinkEnter(x)
			case "reconcile":
				qaSyncRunnerPick(t, x, "Observed outbox target", "all")
				qaLinkFrame(t, x, "Bounded batch limit")
				qaLinkEnter(x)
			case "resolve":
				qaSyncRunnerPick(t, x, "Observed outbox target", qaSyncRunnerItem)
				qaSyncRunnerPick(t, x, "Resolve outbox", "attach")
				qaLinkFrame(t, x, "Existing stopped Harvest entry ID")
				qaLinkKey(x, "901")
				qaLinkEnter(x)
			}
			if op != "reconcile" {
				qaSyncRunnerConfirm(t, x)
			}
			qaSyncRunnerDone(t, x, "Sync · Local operation complete")
			qaSyncRunnerQuit(t, x)
			if calls.Load() != 1 {
				t.Error("route missing/duplicated mutation")
			}
		})
	}
}
func TestQAUISyncRunnerExactReplaySurvivesNavigationRefresh(t *testing.T) {
	a := qaSyncRunnerActions()
	unknown := &activity.Error{Code: "local_write_unknown", Uncertain: true, Message: "RAW-SYNC-RUNNER-CANARY"}
	var ids []string
	a.Pause = func(_ context.Context, id string) (activity.MutationResult, error) {
		ids = append(ids, id)
		if len(ids) == 1 {
			return activity.MutationResult{}, unknown
		}
		return activity.MutationResult{RequestID: id}, nil
	}
	x := qaSyncRunnerRig(t, a, nil)
	qaSyncRunnerOpen(t, x, "pause")
	qaSyncRunnerConfirm(t, x)
	qaSyncRunnerUnknownBack(t, x)
	qaLinkKey(x, "r")
	x.frame(t, "Project 100")
	qaSyncRunnerRecover(t, x, "status")
	qaLinkFrame(t, x, "Sync · Read-only status")
	qaLinkEnter(x)
	qaSyncRunnerPick(t, x, "Recover submitted intent", "back")
	x.frame(t, "Project 100")
	qaSyncRunnerRecover(t, x, "replay")
	qaSyncRunnerDone(t, x, "Exact local receipt complete")
	qaSyncRunnerQuit(t, x)
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Errorf("changed pending UUID: %v", ids)
	}
}
func TestQAUISyncRunnerRemoteNestedGETIdentity(t *testing.T) {
	a := qaSyncRunnerActions()
	remote := &harvest.Error{Code: "uncertain_write", Uncertain: true, Message: "RAW-SYNC-RUNNER-CANARY"}
	local := &activity.Error{Code: "local_write_unknown", Uncertain: true}
	var posts atomic.Int32
	var original string
	var inputs []activity.SyncReconcileInput
	a.Now = func(_ context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
		posts.Add(1)
		original = in.RequestID
		return activity.SyncRun{}, remote
	}
	a.Reconcile = func(_ context.Context, in activity.SyncReconcileInput) (activity.SyncRun, error) {
		inputs = append(inputs, in)
		if len(inputs) == 1 {
			return activity.SyncRun{}, local
		}
		return activity.SyncRun{RequestID: in.RequestID, State: "complete"}, nil
	}
	var retained atomic.Int32
	x := qaSyncRunnerRig(t, a, func(f, id string, e error) {
		retained.Add(1)
		if f != "sync" || id != original || e != remote {
			t.Error("retained original POST identity lost")
		}
	})
	qaSyncRunnerOpen(t, x, "now")
	qaLinkFrame(t, x, "Bounded batch limit")
	qaLinkEnter(x)
	qaSyncRunnerConfirm(t, x)
	qaSyncRunnerUnknownBack(t, x)
	qaSyncRunnerRecover(t, x, "reconcile")
	qaSyncRunnerPick(t, x, "Observed outbox target", "all")
	qaLinkFrame(t, x, "Bounded batch limit")
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Read-only reconciliation")
	qaLinkEnter(x)
	qaSyncRunnerPick(t, x, "Recover submitted intent", "back")
	x.frame(t, "Project 100")
	qaSyncRunnerRecover(t, x, "reconcile")
	qaLinkFrame(t, x, "Read-only reconciliation")
	qaLinkEnter(x)
	qaSyncRunnerPick(t, x, "Recover submitted intent", "back")
	x.frame(t, "Project 100")
	qaLinkKey(x, "q")
	x.finish(t, remote)
	if posts.Load() != 1 || len(inputs) != 2 || !reflect.DeepEqual(inputs[0], inputs[1]) || inputs[0].RequestID == original || retained.Load() != 1 {
		t.Error("remote uncertainty retried POST or nested GET identity changed")
	}
}
func TestQAUISyncRunnerUnknownJoinsBeforeCloseAndReportsUUID(t *testing.T) {
	for _, mode := range []string{"eof", "sigint", "sigterm", "draw-failure", "close-failure"} {
		t.Run(mode, func(t *testing.T) {
			a := qaSyncRunnerActions()
			unknown := &activity.Error{Code: "local_write_unknown", Uncertain: true, Message: "RAW-SYNC-RUNNER-CANARY"}
			var active, calls, reported atomic.Int32
			entered := make(chan struct{})
			var id string
			a.Pause = func(ctx context.Context, in string) (activity.MutationResult, error) {
				active.Add(1)
				defer active.Add(-1)
				calls.Add(1)
				id = in
				close(entered)
				<-ctx.Done()
				return activity.MutationResult{}, unknown
			}
			var x *qaRunnerRig
			x = qaSyncRunnerRig(t, a, func(f, in string, e error) {
				reported.Add(1)
				if f != "sync" || in != id || e != unknown || x.screen.closes.Load() != 1 || active.Load() != 0 {
					t.Error("unknown report before Close or missing UUID")
				}
			})
			x.screen.closeCheck = func() bool { return active.Load() != 0 }
			qaSyncRunnerOpen(t, x, "pause")
			qaSyncRunnerConfirm(t, x)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("pause not reached")
			}
			switch mode {
			case "eof":
				x.screen.endErrors <- &terminal.ExitError{Code: 0}
			case "sigint":
				x.cancel(&terminal.ExitError{Code: 130})
			case "sigterm":
				x.cancel(&terminal.ExitError{Code: 143})
			case "draw-failure":
				x.screen.mu.Lock()
				x.screen.drawErr = errors.New("safe synthetic failure")
				x.screen.mu.Unlock()
				x.screen.events <- terminal.Event{Kind: "resize", Columns: 120, Rows: 24}
			case "close-failure":
				x.screen.closeErr = errors.New("safe restoration failure")
				x.cancel(&terminal.ExitError{Code: 143})
			}
			x.finish(t, unknown)
			if calls.Load() != 1 || active.Load() != 0 || reported.Load() != 1 || x.screen.closeBeforeJoin {
				t.Error("unknown escaped joined terminal lifetime")
			}
		})
	}
}

func qaSyncRunnerRecover(t *testing.T, x *qaRunnerRig, op string) {
	t.Helper()
	qaLinkKey(x, "s")
	qaSyncRunnerPick(t, x, "Recover submitted intent", op)
}
func TestQAUISyncRunnerNestedUnknownTwoReceiptsAfterClose(t *testing.T) {
	for _, mode := range []string{"eof", "sigint", "sigterm", "close-failure"} {
		t.Run(mode, func(t *testing.T) {
			a := qaSyncRunnerActions()
			remote := &harvest.Error{Code: "uncertain_write", Uncertain: true, Message: "RAW-SYNC-CANARY"}
			local := &activity.Error{Code: "local_write_unknown", Uncertain: true, Message: "RAW-SYNC-CANARY"}
			var postID, getID string
			var posts, gets atomic.Int32
			var reports []string
			var x *qaRunnerRig
			a.Now = func(_ context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
				posts.Add(1)
				postID = in.RequestID
				return activity.SyncRun{}, remote
			}
			a.Reconcile = func(_ context.Context, in activity.SyncReconcileInput) (activity.SyncRun, error) {
				gets.Add(1)
				getID = in.RequestID
				return activity.SyncRun{}, local
			}
			x = qaSyncRunnerRig(t, a, func(f, id string, e error) {
				if f != "sync" || x.screen.closes.Load() != 1 {
					t.Error("nested report before Close/wrong family")
				}
				if len(reports) == 0 {
					if id != postID || e != remote {
						t.Error("primary POST typed identity lost")
					}
				} else {
					if id != getID || e != local {
						t.Error("GET receipt typed identity lost")
					}
				}
				reports = append(reports, id)
			})
			qaSyncRunnerOpen(t, x, "now")
			qaLinkFrame(t, x, "Bounded batch limit")
			qaLinkEnter(x)
			qaSyncRunnerConfirm(t, x)
			qaSyncRunnerUnknownBack(t, x)
			qaSyncRunnerRecover(t, x, "reconcile")
			qaSyncRunnerPick(t, x, "Observed outbox target", "all")
			qaLinkFrame(t, x, "Bounded batch limit")
			qaLinkEnter(x)
			qaLinkFrame(t, x, "Read-only reconciliation")
			qaLinkEnter(x)
			qaSyncRunnerPick(t, x, "Recover submitted intent", "back")
			x.frame(t, "Project 100")
			switch mode {
			case "eof":
				x.screen.endErrors <- &terminal.ExitError{Code: 0}
			case "sigint":
				x.cancel(&terminal.ExitError{Code: 130})
			case "sigterm":
				x.cancel(&terminal.ExitError{Code: 143})
			case "close-failure":
				x.screen.closeErr = errors.New("synthetic restore failure")
				qaLinkKey(x, "q")
			}
			x.finish(t, remote)
			if posts.Load() != 1 || gets.Load() != 1 || len(reports) != 2 || postID == getID || !qaUILinkUUID.MatchString(getID) {
				t.Errorf("nested shutdown omitted identity: %v", reports)
			}
		})
	}
}

func qaSyncRunnerUnknownBack(t *testing.T, x *qaRunnerRig) {
	t.Helper()
	qaLinkFrame(t, x, "Outcome unknown")
	qaLinkEnter(x)
	qaSyncRunnerPick(t, x, "Recover submitted intent", "back")
	x.frame(t, "Project 100")
}

func TestQAUISyncRunnerRemoteReconcileCancellationKeepsOriginal(t *testing.T) {
	for _, stage := range []string{"target", "limit"} {
		t.Run(stage, func(t *testing.T) {
			a := qaSyncRunnerActions()
			remote := &harvest.Error{Code: "uncertain_write", Uncertain: true}
			var calls atomic.Int32
			var submitted string
			a.Now = func(_ context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
				submitted = in.RequestID
				return activity.SyncRun{}, remote
			}
			a.Reconcile = func(context.Context, activity.SyncReconcileInput) (activity.SyncRun, error) {
				calls.Add(1)
				return activity.SyncRun{}, nil
			}
			var reports atomic.Int32
			x := qaSyncRunnerRig(t, a, func(f, id string, e error) {
				reports.Add(1)
				if f != "sync" || id != submitted || e != remote {
					t.Error("cancel replaced original remote outcome")
				}
			})
			qaSyncRunnerOpen(t, x, "now")
			qaLinkFrame(t, x, "Bounded batch limit")
			qaLinkEnter(x)
			qaSyncRunnerConfirm(t, x)
			qaSyncRunnerUnknownBack(t, x)
			qaSyncRunnerRecover(t, x, "reconcile")
			if stage == "target" {
				qaLinkFrame(t, x, "Observed outbox target")
			} else {
				qaSyncRunnerPick(t, x, "Observed outbox target", "all")
				qaLinkFrame(t, x, "Bounded batch limit")
			}
			qaLinkEscape(x)
			x.frame(t, "Project 100")
			qaSyncRunnerRecover(t, x, "back")
			x.frame(t, "Project 100")
			qaLinkKey(x, "q")
			x.finish(t, remote)
			if calls.Load() != 0 || reports.Load() != 1 {
				t.Error("cancel dispatched GET or forgot original ambiguity")
			}
		})
	}
}
