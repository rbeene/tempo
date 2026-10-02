package ui_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
)

func qaUIReadonlyLinks() activity.BindingList {
	return activity.BindingList{ContractVersion: 1, SnapshotRevision: "1", Bindings: []activity.Binding{{ID: "22222222-2222-4222-8222-222222222222", Revision: "7", Kind: "directory", Locator: "/synthetic/quiet-project", Attribution: activity.Attribution{AccountID: "1", ProjectID: "100", TaskID: "7", UserID: "2", Timezone: "UTC"}, AttachedActors: []activity.ActorRef{}}}}
}
func qaUIReadonlySync() activity.SyncStatus {
	return activity.SyncStatus{ContractVersion: 1, SnapshotRevision: "1", Configurations: []activity.SyncConfiguration{}, Items: []activity.OutboxItem{{ID: "33333333-3333-4333-8333-333333333333", Revision: "8", State: "unknown", Interval: activity.Interval{ComputerID: qaUIComputer, Attribution: activity.Attribution{AccountID: "1", ProjectID: "100", TaskID: "7", UserID: "2", Timezone: "UTC"}, DurationNS: "10000000000"}}}, Worker: activity.WorkerStatus{State: "not_installed"}, Totals: activity.SyncTotals{ExactDurationNS: "10000000000"}, Accounting: []activity.SyncAccounting{}}
}
func qaUIReadonlySetup() setup.Status {
	return setup.Status{ContractVersion: 1, Steps: []setup.Step{{Action: "hooks.status", State: "input_required", SafeMessage: "Capture remains unverified", RequiredFields: []string{}}}}
}
func qaUIViewsRig(t *testing.T, views *ui.ReadViews) (*qaRunnerRig, chan time.Time) {
	t.Helper()
	s := qaNewRunnerScreen()
	r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("10", "100"), nil }}
	ticks := make(chan time.Time, 4)
	x := qaStartRunnerOptions(t, s, r, ui.Options{Refresh: ticks, Views: views})
	x.frame(t, "Project 100")
	return x, ticks
}
func qaUIBudget(t *testing.T, ctx context.Context) {
	t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 250*time.Millisecond {
		t.Errorf("explicit local read has no <=250ms budget")
	}
}

func TestQAUIViewsExplicitReadsLazyAndNeverOnRefresh(t *testing.T) {
	for _, tc := range []struct{ key, title, content string }{{"2", "Links", "quiet-project"}, {"3", "Sync", "unknown"}, {",", "Setup", "Capture remains unverified"}} {
		t.Run(tc.title, func(t *testing.T) {
			var links, syncs, setups atomic.Int32
			views := &ui.ReadViews{Links: func(ctx context.Context) (activity.BindingList, error) {
				qaUIBudget(t, ctx)
				links.Add(1)
				return qaUIReadonlyLinks(), nil
			}, Sync: func(ctx context.Context) (activity.SyncStatus, error) {
				qaUIBudget(t, ctx)
				syncs.Add(1)
				return qaUIReadonlySync(), nil
			}, Setup: func(ctx context.Context) (setup.Status, error) {
				qaUIBudget(t, ctx)
				setups.Add(1)
				return qaUIReadonlySetup(), nil
			}}
			x, ticks := qaUIViewsRig(t, views)
			if links.Load()+syncs.Load()+setups.Load() != 0 {
				t.Fatal("opening dashboard invoked ancillary reads")
			}
			x.screen.events <- terminal.Event{Kind: "text", Text: tc.key}
			frame := x.frame(t, tc.content)
			if !strings.Contains(strings.Join(frame, "\n"), tc.title) {
				t.Errorf("shared view content lost its title %s", tc.title)
			}
			ticks <- time.Now()
			deadline := time.Now().Add(time.Second)
			for x.reader.calls.Load() < 2 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if x.reader.calls.Load() < 2 {
				t.Error("active modal blocked authoritative refresh")
			}
			if total := links.Load() + syncs.Load() + setups.Load(); total != 1 {
				t.Errorf("view read calls=%d, want exactly one lazy call", total)
			}
			x.screen.events <- terminal.Event{Kind: "escape"}
			x.frame(t, "Project 100")
			x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
			x.finish(t, nil)
		})
	}
}
func TestQAUIViewsLinkSearchQAndEscapeStayInsideModal(t *testing.T) {
	views := &ui.ReadViews{Links: func(context.Context) (activity.BindingList, error) { return qaUIReadonlyLinks(), nil }}
	x, _ := qaUIViewsRig(t, views)
	x.screen.events <- terminal.Event{Kind: "text", Text: "2"}
	x.frame(t, "quiet-project")
	x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
	x.frame(t, "quiet-project")
	x.screen.events <- terminal.Event{Kind: "enter"}
	x.frame(t, "22222222-2222-4222-8222-222222222222")
	x.screen.events <- terminal.Event{Kind: "escape"}
	x.frame(t, "Project 100")
	x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
	x.finish(t, nil)
}
func TestQAUIViewsPendingReadEscapeAndShutdownJoin(t *testing.T) {
	for _, mode := range []string{"escape", "quit", "signal"} {
		t.Run(mode, func(t *testing.T) {
			started, ended := make(chan struct{}, 1), make(chan struct{}, 1)
			var active, calls atomic.Int32
			views := &ui.ReadViews{Links: func(ctx context.Context) (activity.BindingList, error) {
				qaUIBudget(t, ctx)
				calls.Add(1)
				active.Add(1)
				defer func() { active.Add(-1); ended <- struct{}{} }()
				started <- struct{}{}
				<-ctx.Done()
				return activity.BindingList{}, context.Cause(ctx)
			}}
			s := qaNewRunnerScreen()
			s.closeCheck = func() bool { return active.Load() != 0 }
			r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("1", "100"), nil }}
			x := qaStartRunnerOptions(t, s, r, ui.Options{Refresh: make(chan time.Time), Views: views})
			x.frame(t, "Project 100")
			x.screen.events <- terminal.Event{Kind: "text", Text: "2"}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("explicit link read never started")
			}
			x.screen.events <- terminal.Event{Kind: "text", Text: "2"}
			x.screen.events <- terminal.Event{Kind: "text", Text: "3"}
			var want error
			switch mode {
			case "escape":
				x.screen.events <- terminal.Event{Kind: "escape"}
				x.frame(t, "Project 100")
			case "quit":
				x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
			case "signal":
				want = &terminal.ExitError{Code: 143}
				x.cancel(want)
			}
			select {
			case <-ended:
			case <-time.After(time.Second):
				t.Fatal("canceled explicit view read survived")
			}
			if calls.Load() != 1 {
				t.Errorf("duplicate keys dispatched multiple flows: %d", calls.Load())
			}
			if mode == "escape" {
				x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
			}
			x.finish(t, want)
		})
	}
}
func TestQAUIViewsUnavailableAndRawErrorsStaySafe(t *testing.T) {
	for _, mode := range []string{"nil-views", "nil-callback", "read-error"} {
		t.Run(mode, func(t *testing.T) {
			var views *ui.ReadViews
			if mode == "nil-callback" {
				views = &ui.ReadViews{}
			}
			if mode == "read-error" {
				views = &ui.ReadViews{Links: func(context.Context) (activity.BindingList, error) {
					return activity.BindingList{}, errors.New("PRIVATE raw transport \x1b]52;c;SECRET\a")
				}}
			}
			x, _ := qaUIViewsRig(t, views)
			x.screen.events <- terminal.Event{Kind: "text", Text: "2"}
			frame := x.frame(t, "unavailable")
			text := strings.Join(frame, "\n")
			if strings.Contains(text, "PRIVATE") || strings.Contains(text, "SECRET") {
				t.Errorf("raw view error leaked: %q", text)
			}
			x.screen.events <- terminal.Event{Kind: "escape"}
			x.frame(t, "Project 100")
			x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
			x.finish(t, nil)
		})
	}
}
func TestQAUIViewsHelpHasReachableKeyboardHintsAndNoReads(t *testing.T) {
	var calls atomic.Int32
	views := &ui.ReadViews{Links: func(context.Context) (activity.BindingList, error) { calls.Add(1); return qaUIReadonlyLinks(), nil }, Sync: func(context.Context) (activity.SyncStatus, error) { calls.Add(1); return qaUIReadonlySync(), nil }, Setup: func(context.Context) (setup.Status, error) { calls.Add(1); return qaUIReadonlySetup(), nil }}
	x, _ := qaUIViewsRig(t, views)
	x.screen.events <- terminal.Event{Kind: "text", Text: "?"}
	frame := x.frame(t, "q: quit only from dashboard")
	text := strings.Join(frame, "\n")
	for _, hint := range []string{"Links", "Sync", "Setup", "Enter", "Escape"} {
		if !strings.Contains(text, hint) {
			t.Errorf("help hides reachable control %s: %q", hint, text)
		}
	}
	x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-x.done:
		x.done <- err
		t.Fatalf("q inside readonly view quit dashboard: %v", err)
	default:
	}
	if calls.Load() != 0 {
		t.Error("Help invoked shared backend reads")
	}
	x.screen.events <- terminal.Event{Kind: "escape"}
	x.frame(t, "Project 100")
	x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
	x.finish(t, nil)
}
func TestQAUIViewsTimerDetailsFreezeCompositeTargetAndAttribution(t *testing.T) {
	old := qaUISnapshot("10", "100", "200")
	attr := activity.Attribution{AccountID: "1", ProjectID: "100", TaskID: "7", UserID: "2", Timezone: "UTC"}
	ref := activity.ActorRef{Key: activity.ActorKey{ComputerID: qaUIComputer, Source: "codex", SessionID: "session", AgentID: "original-actor"}, Generation: "1"}
	old.Projects = []activity.ProjectActivity{{ComputerID: qaUIComputer, Attribution: attr, ActiveActorRefs: []activity.ActorRef{ref}, WaitingActorRefs: []activity.ActorRef{}, UnresolvedIDs: []string{}, ProvisionalUnionNS: "10000000000", ConfirmedClosedNS: "5000000000"}}
	old.Actors = []activity.Actor{{ID: "original-actor", Revision: "7", Ref: ref, State: "working", Health: "continuous", Attribution: attr}}
	newer := qaUISnapshot("10", "200", "100")
	newer.ProjectTimers[1].ProvisionalUnionNS = "11000000000"
	var reads, viewReads atomic.Int32
	r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) {
		if reads.Add(1) == 1 {
			return old, nil
		}
		return newer, nil
	}}
	views := &ui.ReadViews{Links: func(context.Context) (activity.BindingList, error) { viewReads.Add(1); return qaUIReadonlyLinks(), nil }}
	s := qaNewRunnerScreen()
	ticks := make(chan time.Time, 1)
	x := qaStartRunnerOptions(t, s, r, ui.Options{Refresh: ticks, Views: views})
	x.frame(t, "> Project 100")
	s.events <- terminal.Event{Kind: "enter"}
	frame := x.frame(t, "original-actor")
	if !strings.Contains(strings.Join(frame, "\n"), "UTC") {
		t.Error("timer detail omitted immutable attribution epoch")
	}
	ticks <- time.Now()
	deadline := time.Now().Add(time.Second)
	for reads.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if reads.Load() < 2 {
		t.Fatal("detail modal blocked refresh")
	}
	s.events <- terminal.Event{Kind: "resize", Columns: 100, Rows: 24}
	frame = x.frame(t, "original-actor")
	if !strings.Contains(strings.Join(frame, "\n"), "100") {
		t.Error("refresh replaced frozen detail target with reordered row")
	}
	if viewReads.Load() != 0 {
		t.Error("opening snapshot timer detail invoked ancillary backend")
	}
	s.events <- terminal.Event{Kind: "escape"}
	x.frame(t, "00:00:11")
	s.events <- terminal.Event{Kind: "text", Text: "q"}
	x.finish(t, nil)
}
