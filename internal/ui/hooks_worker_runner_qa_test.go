package ui_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
	"github.com/rbeene/tempo/internal/worker"
)

func qaHWRunReader() *qaRunnerReader {
	return &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("1"), nil }}
}
func qaHWRunBound(t *testing.T, ctx context.Context, max time.Duration) {
	t.Helper()
	d, ok := ctx.Deadline()
	if !ok || time.Until(d) > max {
		t.Errorf("unbounded shared callback %v %t", time.Until(d), ok)
	}
}
func qaHWRunPick(t *testing.T, x *qaRunnerRig, title, id string) {
	t.Helper()
	qaLinkFrame(t, x, title)
	qaLinkKey(x, id)
	qaLinkEnter(x)
}
func qaHWRunHookDraft(t *testing.T, x *qaRunnerRig, operation string) {
	t.Helper()
	qaLinkKey(x, "h")
	qaHWRunPick(t, x, "Hooks and capture", operation)
	qaHWRunPick(t, x, "Choose hook host", "claude")
	qaHWRunPick(t, x, "Choose hook scope", "project")
	qaLinkFrame(t, x, "Absolute project context path")
	x.screen.events <- terminal.Event{Kind: "paste", Text: "/synthetic/hook-project"}
	qaLinkEnter(x)
}
func TestQAUIHookWorkerDiagnosticsMenusAreLazyAndQuitOwnsNoService(t *testing.T) {
	for _, family := range []struct{ key, title string }{{"h", "Hooks and capture"}, {"w", "Worker controls"}, {"d", "Diagnostics"}} {
		t.Run(family.key, func(t *testing.T) {
			calls := atomic.Int32{}
			o := ui.Options{Refresh: make(chan time.Time), Hooks: &ui.HookActions{Status: func(context.Context, hookstate.HookSelector) (hookstate.HookList, error) {
				calls.Add(1)
				return hookstate.HookList{}, nil
			}}, Worker: &ui.WorkerActions{Status: func(context.Context) (worker.Result, error) { calls.Add(1); return worker.Result{}, nil }, Stop: func(context.Context, worker.ControlRequest) (worker.Result, error) {
				calls.Add(1)
				return worker.Result{}, nil
			}}, Diagnostics: func(context.Context, bool) (setup.Diagnostics, error) { calls.Add(1); return setup.Diagnostics{}, nil }}
			x := qaStartRunnerOptions(t, qaNewRunnerScreen(), qaHWRunReader(), o)
			x.frame(t, "No activity")
			qaLinkKey(x, family.key)
			qaHWRunPick(t, x, family.title, "back")
			x.frame(t, "No activity")
			qaLinkKey(x, "q")
			x.finish(t, nil)
			if calls.Load() != 0 {
				t.Error("menu/back/UI quit performed an unrequested shared service action")
			}
		})
	}
}
func TestQAUIHookConfirmationFullScrollTinyInertAndFrozenIntent(t *testing.T) {
	var previews, applied atomic.Int32
	var captured hookstate.ApplyInstallInput
	actions := &ui.HookActions{Status: func(ctx context.Context, in hookstate.HookSelector) (hookstate.HookList, error) {
		qaHWRunBound(t, ctx, 250*time.Millisecond)
		return hookstate.HookList{}, nil
	}, PreviewInstall: func(ctx context.Context, in hookstate.InstallIntent) (hookstate.HookPreview, error) {
		qaHWRunBound(t, ctx, 2*time.Minute)
		previews.Add(1)
		return hookstate.HookPreview{ContractVersion: 1, Intent: in, Fingerprint: strings.Repeat("a", 64), Changes: []hookstate.HookChange{{Host: "claude", Path: "/synthetic/hook-project/.claude/settings.json", Operation: "install", SafeSummary: "Reviewed handler change"}}, ApprovalSteps: []string{strings.Repeat("Full warning about retained history and independent upload consent. ", 80) + "END-OF-HOOK-WARNING"}}, nil
	}, ApplyInstall: func(ctx context.Context, in hookstate.ApplyInstallInput) (hookstate.HookList, error) {
		qaHWRunBound(t, ctx, 2*time.Minute)
		captured = in
		applied.Add(1)
		return hookstate.HookList{ContractVersion: 1, RequestID: in.RequestID}, nil
	}}
	x := qaStartRunnerOptions(t, qaNewRunnerScreen(), qaHWRunReader(), ui.Options{Refresh: make(chan time.Time), Hooks: actions})
	x.frame(t, "No activity")
	qaHWRunHookDraft(t, x, "install")
	qaLinkFrame(t, x, "Review scoped change")
	qaLinkYes(x)
	time.Sleep(20 * time.Millisecond)
	if applied.Load() != 0 {
		t.Error("hidden warning was committed before review")
	}
	x.screen.events <- terminal.Event{Kind: "resize", Columns: 39, Rows: 7}
	qaLinkFrame(t, x, "Terminal too small")
	qaLinkYes(x)
	time.Sleep(20 * time.Millisecond)
	if applied.Load() != 0 {
		t.Error("tiny form committed hook action")
	}
	x.screen.events <- terminal.Event{Kind: "resize", Columns: 80, Rows: 24}
	qaLinkFrame(t, x, "Review scoped change")
	for n := 0; n < 100; n++ {
		x.screen.events <- terminal.Event{Kind: "down"}
	}
	qaLinkFrame(t, x, "END-OF-HOOK-WARNING")
	qaLinkYes(x)
	qaLinkFrame(t, x, "Hooks · Complete")
	qaLinkEnter(x)
	x.frame(t, "No activity")
	qaLinkKey(x, "q")
	x.finish(t, nil)
	if previews.Load() != 1 || applied.Load() != 1 || captured.Intent != (hookstate.InstallIntent{Host: "claude", Scope: "project", Path: "/synthetic/hook-project", Operation: "install"}) || captured.Fingerprint != strings.Repeat("a", 64) || !captured.Confirmed || len(captured.RequestID) != 36 {
		t.Errorf("reviewed hook intent changed: %+v preview%d apply%d", captured, previews.Load(), applied.Load())
	}
}
func TestQAUIWorkerUnknownReopenAndExactExplicitReplay(t *testing.T) {
	var calls atomic.Int32
	requests := []worker.ControlRequest{}
	unknown := &worker.Error{Code: "local_write_unknown", Message: "RAW-MANAGER-CANARY", Uncertain: true}
	actions := &ui.WorkerActions{Status: func(ctx context.Context) (worker.Result, error) {
		qaHWRunBound(t, ctx, 250*time.Millisecond)
		return worker.Result{ContractVersion: 1, Status: activity.WorkerStatus{State: "stopped", QueuedCount: 4}}, nil
	}, Stop: func(ctx context.Context, r worker.ControlRequest) (worker.Result, error) {
		qaHWRunBound(t, ctx, 2*time.Minute)
		requests = append(requests, r)
		if calls.Add(1) == 1 {
			return worker.Result{}, unknown
		}
		return worker.Result{ContractVersion: 1, Status: activity.WorkerStatus{State: "stopped"}}, nil
	}}
	x := qaStartRunnerOptions(t, qaNewRunnerScreen(), qaHWRunReader(), ui.Options{Refresh: make(chan time.Time), Worker: actions})
	x.frame(t, "No activity")
	qaLinkKey(x, "w")
	qaHWRunPick(t, x, "Worker controls", "stop")
	qaLinkFrame(t, x, "Worker · Outcome unknown")
	qaLinkEnter(x)
	qaHWRunPick(t, x, "Recover pending request", "back")
	x.frame(t, "No activity")
	qaLinkKey(x, "w")
	qaLinkFrame(t, x, "Recover pending request")
	qaLinkKey(x, "q")
	time.Sleep(20 * time.Millisecond)
	if x.screen.closes.Load() != 0 {
		t.Error("q in recovery search quit UI")
	}
	x.screen.events <- terminal.Event{Kind: "backspace"}
	qaLinkKey(x, "status")
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Worker · Read-only status")
	if calls.Load() != 1 {
		t.Error("inspection retried service")
	}
	qaLinkEnter(x)
	qaHWRunPick(t, x, "Recover pending request", "replay")
	qaLinkFrame(t, x, "Worker · Complete")
	qaLinkEnter(x)
	x.frame(t, "No activity")
	qaLinkKey(x, "q")
	x.finish(t, nil)
	if len(requests) != 2 || !reflect.DeepEqual(requests[0], requests[1]) || requests[0].Confirmed || len(requests[0].RequestID) != 36 {
		t.Errorf("worker recovery replaced exact submitted intent: %+v", requests)
	}
}
func TestQAUIWorkerSearchStartSelectsExactStartOperation(t *testing.T) {
	started := make(chan worker.ControlRequest, 1)
	var installed atomic.Int32
	actions := &ui.WorkerActions{Start: func(ctx context.Context, in worker.ControlRequest) (worker.Result, error) {
		qaHWRunBound(t, ctx, 2*time.Minute)
		started <- in
		return worker.Result{ContractVersion: 1, Status: activity.WorkerStatus{State: "running"}}, nil
	}, Install: func(context.Context, worker.ControlRequest) (worker.Result, error) {
		installed.Add(1)
		return worker.Result{}, nil
	}}
	x := qaStartRunnerOptions(t, qaNewRunnerScreen(), qaHWRunReader(), ui.Options{Refresh: make(chan time.Time), Worker: actions})
	x.frame(t, "No activity")
	qaLinkKey(x, "w")
	qaHWRunPick(t, x, "Worker controls", "start")
	select {
	case in := <-started:
		if in.Confirmed || len(in.RequestID) != 36 {
			t.Errorf("explicit Start changed control intent: %+v", in)
		}
	case <-time.After(time.Second):
		t.Fatal("searching start selected another operation instead of explicit Start")
	}
	qaLinkFrame(t, x, "Worker · Complete")
	qaLinkEnter(x)
	x.frame(t, "No activity")
	qaLinkKey(x, "q")
	x.finish(t, nil)
	if installed.Load() != 0 {
		t.Fatal("start search performed Install")
	}
}
func TestQAUIHookWorkerJoinedUnknownSurvivesEOFSignalAndCleanup(t *testing.T) {
	for _, family := range []string{"hooks", "worker"} {
		for _, mode := range []string{"eof", "signal", "escape-then-quit"} {
			t.Run(family+"/"+mode, func(t *testing.T) {
				var active atomic.Int32
				entered := make(chan struct{})
				var id string
				var unknown error
				screen := qaNewRunnerScreen()
				screen.closeErr = errors.New("RAW-RESTORATION-CANARY")
				screen.closeCheck = func() bool { return active.Load() != 0 }
				o := ui.Options{Refresh: make(chan time.Time)}
				if family == "hooks" {
					unknown = &hookstate.Error{Code: "local_write_unknown", Uncertain: true}
					o.Hooks = &ui.HookActions{Status: func(context.Context, hookstate.HookSelector) (hookstate.HookList, error) {
						return hookstate.HookList{}, nil
					}, PreviewInstall: func(_ context.Context, in hookstate.InstallIntent) (hookstate.HookPreview, error) {
						return hookstate.HookPreview{Intent: in, Fingerprint: strings.Repeat("b", 64)}, nil
					}, ApplyInstall: func(ctx context.Context, in hookstate.ApplyInstallInput) (hookstate.HookList, error) {
						id = in.RequestID
						active.Add(1)
						defer active.Add(-1)
						close(entered)
						<-ctx.Done()
						return hookstate.HookList{}, unknown
					}}
				} else {
					unknown = &worker.Error{Code: "local_write_unknown", Uncertain: true, Message: "RAW-MANAGER-CANARY"}
					o.Worker = &ui.WorkerActions{Stop: func(ctx context.Context, in worker.ControlRequest) (worker.Result, error) {
						id = in.RequestID
						active.Add(1)
						defer active.Add(-1)
						close(entered)
						<-ctx.Done()
						return worker.Result{}, unknown
					}}
				}
				reportCount, notices := 0, 0
				o.OnRetainedOutcome = func(name, requestID string, e error) {
					reportCount++
					if name != family || requestID != id || len(id) != 36 || e != unknown || active.Load() != 0 || screen.closes.Load() != 1 {
						t.Errorf("retained family/input/outcome before joined cleanup: %s %s %v", name, requestID, e)
					}
				}
				o.OnRestorationFailure = func() {
					notices++
					if active.Load() != 0 || screen.closes.Load() != 1 {
						t.Error("notice before join/Close")
					}
				}
				x := qaStartRunnerOptions(t, screen, qaHWRunReader(), o)
				x.frame(t, "No activity")
				if family == "hooks" {
					qaHWRunHookDraft(t, x, "install")
					qaLinkFrame(t, x, "Review scoped change")
					qaLinkYes(x)
				} else {
					qaLinkKey(x, "w")
					qaHWRunPick(t, x, "Worker controls", "stop")
				}
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("explicit family action never admitted")
				}
				switch mode {
				case "eof":
					screen.endErrors <- &terminal.ExitError{Code: 0}
				case "signal":
					x.cancel(&terminal.ExitError{Code: 143})
				case "escape-then-quit":
					screen.events <- terminal.Event{Kind: "escape"}
					x.frame(t, "No activity")
					qaLinkKey(x, "q")
				}
				x.finish(t, unknown)
				if reportCount != 1 || notices != 1 || screen.closeBeforeJoin {
					t.Errorf("unknown/report/cleanup lost: reports%d notices%d", reportCount, notices)
				}
			})
		}
	}
}
func TestQAUIDiagnosticsLocalAndExplicitRemoteBoundedSafeAndLazy(t *testing.T) {
	for _, remote := range []bool{false, true} {
		name := "local"
		if remote {
			name = "check"
		}
		t.Run(name, func(t *testing.T) {
			calls := 0
			action := "review independent hook status"
			read := func(ctx context.Context, check bool) (setup.Diagnostics, error) {
				calls++
				max := 250 * time.Millisecond
				if check {
					max = 2 * time.Minute
				}
				qaHWRunBound(t, ctx, max)
				if check != remote {
					t.Error("explicit doctor mode changed")
				}
				return setup.Diagnostics{ContractVersion: 1, Items: []setup.Diagnostic{{Code: "synthetic_provenance", Severity: "warning", SafeMessage: "Retained declaration does not prove real delivery", Action: &action}}}, nil
			}
			x := qaStartRunnerOptions(t, qaNewRunnerScreen(), qaHWRunReader(), ui.Options{Refresh: make(chan time.Time), Diagnostics: read})
			x.frame(t, "No activity")
			qaLinkKey(x, "d")
			qaHWRunPick(t, x, "Diagnostics", name)
			frame := x.frame(t, "synthetic_provenance")
			if !strings.Contains(strings.Join(frame, "\n"), "review independent hook status") {
				t.Error("shared diagnostic action omitted")
			}
			qaLinkEnter(x)
			x.frame(t, "No activity")
			qaLinkKey(x, "q")
			x.finish(t, nil)
			if calls != 1 {
				t.Error("doctor read repeated or skipped")
			}
		})
	}
}
