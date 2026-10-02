package ui_test

import (
	"context"
	"errors"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
	"sync/atomic"
	"testing"
	"time"
)

func qaActivityRunnerActor() activity.Actor {
	return activity.Actor{ID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", Revision: "9", State: "working", Ref: activity.ActorRef{Generation: "7", Key: activity.ActorKey{ComputerID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", Source: "synthetic", SessionID: "session", AgentID: "actor"}}, Attribution: activity.Attribution{AccountID: "1", ProjectID: "100", TaskID: "7", UserID: "2", Timezone: "UTC"}}
}
func qaActivityRig(t *testing.T, a *ui.ActivityActions) *qaRunnerRig {
	t.Helper()
	s := qaNewRunnerScreen()
	r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("10", "100"), nil }}
	x := qaStartRunnerOptions(t, s, r, ui.Options{Refresh: make(chan time.Time), Activity: a})
	x.frame(t, "Project 100")
	return x
}
func qaActivityOpen(t *testing.T, x *qaRunnerRig) {
	t.Helper()
	qaLinkKey(x, "x")
	qaLinkFrame(t, x, "Activity actions")
}
func qaActivityActorFlow(t *testing.T, x *qaRunnerRig) {
	t.Helper()
	qaActivityOpen(t, x)
	qaLinkKey(x, "actors")
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Actor actions")
	qaLinkKey(x, qaActivityRunnerActor().ID)
	qaLinkEnter(x)
	qaLinkFrame(t, x, "quarant")
}
func TestQAUIActivityRunnerInterruptExplicitAndCancel(t *testing.T) {
	for _, yes := range []bool{false, true} {
		name := "cancel"
		if yes {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			var writes atomic.Int32
			a := &ui.ActivityActions{Status: func(context.Context) (activity.ActivitySnapshot, error) {
				return activity.ActivitySnapshot{Actors: []activity.Actor{qaActivityRunnerActor()}}, nil
			}, Interrupt: func(_ context.Context, in activity.InterruptInput) (activity.MutationResult, error) {
				writes.Add(1)
				if in.ActorID != qaActivityRunnerActor().ID || in.Generation != "7" || in.IfRevision != "9" || !in.Confirmed || !qaUILinkUUID.MatchString(in.RequestID) {
					t.Errorf("runner dispatched wrong target: %+v", in)
				}
				return activity.MutationResult{RequestID: in.RequestID, Changed: true, SnapshotRevision: "11"}, nil
			}}
			x := qaActivityRig(t, a)
			qaActivityActorFlow(t, x)
			if writes.Load() != 0 {
				t.Fatal("modal auto-confirmed")
			}
			if yes {
				qaLinkYes(x)
				qaLinkFrame(t, x, "Activity · Complete")
				qaLinkEnter(x)
			} else {
				qaLinkEscape(x)
			}
			x.frame(t, "Project 100")
			qaLinkKey(x, "q")
			x.finish(t, nil)
			want := int32(0)
			if yes {
				want = 1
			}
			if writes.Load() != want {
				t.Error("unexpected interrupt count")
			}
		})
	}
}
func TestQAUIActivityRunnerUnknownJoinedBeforeClose(t *testing.T) {
	for _, mode := range []string{"eof", "sigint", "sigterm", "draw-failure", "close-failure"} {
		t.Run(mode, func(t *testing.T) {
			var active, calls atomic.Int32
			entered := make(chan struct{})
			unknown := &activity.Error{Code: "local_write_unknown", Uncertain: true}
			a := &ui.ActivityActions{Status: func(context.Context) (activity.ActivitySnapshot, error) {
				return activity.ActivitySnapshot{Actors: []activity.Actor{qaActivityRunnerActor()}}, nil
			}, Interrupt: func(ctx context.Context, in activity.InterruptInput) (activity.MutationResult, error) {
				active.Add(1)
				defer active.Add(-1)
				calls.Add(1)
				unknown.Details = map[string]any{"request_id": in.RequestID}
				close(entered)
				<-ctx.Done()
				return activity.MutationResult{}, unknown
			}}
			x := qaActivityRig(t, a)
			x.screen.closeCheck = func() bool { return active.Load() != 0 }
			qaActivityActorFlow(t, x)
			qaLinkYes(x)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("interrupt never dispatched")
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
				x.screen.drawErr = errors.New("synthetic output failure")
				x.screen.mu.Unlock()
				x.screen.events <- terminal.Event{Kind: "resize"}
			case "close-failure":
				x.screen.closeErr = errors.New("synthetic restore failure")
				x.cancel(&terminal.ExitError{Code: 143})
			}
			x.finish(t, unknown)
			if calls.Load() != 1 || active.Load() != 0 || x.screen.closeBeforeJoin {
				t.Error("runner duplicated/closed before joined outcome")
			}
		})
	}
}
