package ui

import (
	"context"
	"errors"
	"fmt"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

type qaActFlow struct {
	p      *promptBridge
	done   chan error
	cancel context.CancelCauseFunc
}

func qaActStart(t *testing.T, c *activityController, a *ActivityActions) *qaActFlow {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	f := &qaActFlow{newPromptBridge(ctx), make(chan error, 1), cancel}
	go func() { f.done <- c.run(ctx, f.p, a); close(f.done) }()
	t.Cleanup(func() {
		cancel(context.Canceled)
		select {
		case <-f.done:
		case <-time.After(time.Second):
			t.Error("auth controller did not join after cancellation")
		}
	})
	return f
}
func (f *qaActFlow) next(t *testing.T, kind string) promptRequest {
	t.Helper()
	select {
	case r := <-f.p.requests:
		if r.kind != kind {
			t.Fatalf("prompt kind=%q want %q (%s)", r.kind, kind, r.title)
		}
		return r
	case err := <-f.done:
		t.Fatalf("auth skipped %s prompt: %v", kind, err)
	case <-time.After(time.Second):
		t.Fatalf("auth stalled before %s", kind)
	}
	return promptRequest{}
}
func (f *qaActFlow) finish(t *testing.T) error {
	t.Helper()
	select {
	case e := <-f.done:
		return e
	case <-time.After(time.Second):
		t.Fatal("auth controller did not complete")
		return nil
	}
}
func qaActPick(t *testing.T, r promptRequest, id string) {
	t.Helper()
	for _, c := range r.choices {
		if c.ID == id {
			r.reply <- promptReply{choiceID: id}
			return
		}
	}
	t.Fatalf("choice %q absent: %#v", id, r.choices)
}
func qaActDeadline(t *testing.T, ctx context.Context, max time.Duration) {
	t.Helper()
	d, ok := ctx.Deadline()
	if !ok || time.Until(d) > max+time.Second {
		t.Error("shared callback missing bounded deadline")
	}
}
func qaActBase() *ActivityActions { return &ActivityActions{} }
func qaActView(t *testing.T, f *qaActFlow, needles ...string) {
	t.Helper()
	r := f.next(t, "view")
	s := r.title + " " + r.body
	for _, n := range needles {
		if !strings.Contains(s, n) {
			t.Errorf("safe result omits %q: %s", n, s)
		}
	}
	r.reply <- promptReply{}
}

var qaActUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func qaActActor() activity.Actor {
	return activity.Actor{ID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", Revision: "9", State: "working", Ref: activity.ActorRef{Generation: "7", Key: activity.ActorKey{ComputerID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", Source: "synthetic", SessionID: "session", AgentID: "actor"}}, Attribution: activity.Attribution{AccountID: "1", ProjectID: "100", TaskID: "7", UserID: "2", Timezone: "UTC"}}
}
func qaActUncertainty() activity.Uncertainty {
	return activity.Uncertainty{ID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Revision: "10", Actor: qaActActor().Ref, LowerBound: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC), Attribution: qaActActor().Attribution}
}
func qaActPreview(u activity.Uncertainty, end *time.Time, discard bool) activity.RecoveryPreview {
	proposed := u.LowerBound
	if end != nil {
		proposed = *end
	}
	return activity.RecoveryPreview{ContractVersion: 1, SnapshotRevision: "20", Uncertainty: u, SegmentStart: u.LowerBound.Add(-time.Minute), ConfirmedPrefix: activity.TimeRange{Start: u.LowerBound.Add(-time.Minute), End: u.LowerBound}, ProposedEnd: proposed, DiscardedSuffix: &activity.TimeRange{Start: proposed, End: proposed.Add(time.Minute)}, AffectedUnionBefore: []activity.TimeRange{{Start: u.LowerBound.Add(-time.Minute), End: u.LowerBound}}, AffectedUnionAfter: []activity.TimeRange{{Start: u.LowerBound.Add(-time.Minute), End: proposed}}, StillBlockedIDs: []string{"dddddddd-dddd-4ddd-8ddd-dddddddddddd"}}
}
func qaActResult(id string) activity.MutationResult {
	return activity.MutationResult{ContractVersion: 1, RequestID: id, SnapshotRevision: "21", Changed: true}
}
func TestQAActivityControllerMenuLazyAndBack(t *testing.T) {
	a := &ActivityActions{Status: func(context.Context) (activity.ActivitySnapshot, error) {
		t.Error("menu read status")
		return activity.ActivitySnapshot{}, nil
	}}
	f := qaActStart(t, &activityController{}, a)
	r := f.next(t, "choose")
	if r.title != "Activity actions" {
		t.Errorf("menu title=%q", r.title)
	}
	for _, id := range []string{"actors", "review", "capture", "back"} {
		found := false
		for _, v := range r.choices {
			found = found || v.ID == id
		}
		if !found {
			t.Errorf("missing %s", id)
		}
	}
	qaActPick(t, r, "back")
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
}
func TestQAActivityControllerInterruptFrozenIdentityAndConsent(t *testing.T) {
	for _, yes := range []bool{false, true} {
		t.Run(fmt.Sprint(yes), func(t *testing.T) {
			actor := qaActActor()
			actors := []activity.Actor{actor}
			terminalActor := actor
			terminalActor.ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
			terminalActor.State = "finished"
			actors = append(actors, terminalActor)
			writes := 0
			a := &ActivityActions{Status: func(ctx context.Context) (activity.ActivitySnapshot, error) {
				qaActDeadline(t, ctx, 250*time.Millisecond)
				return activity.ActivitySnapshot{Actors: actors}, nil
			}, Interrupt: func(ctx context.Context, in activity.InterruptInput) (activity.MutationResult, error) {
				writes++
				qaActDeadline(t, ctx, 2*time.Minute)
				if in.ActorID != actor.ID || in.Generation != actor.Ref.Generation || in.IfRevision != actor.Revision || !in.Confirmed || !qaActUUID.MatchString(in.RequestID) {
					t.Errorf("interrupt changed observed intent: %+v", in)
				}
				return qaActResult(in.RequestID), nil
			}}
			f := qaActStart(t, &activityController{}, a)
			qaActPick(t, f.next(t, "choose"), "actors")
			r := f.next(t, "choose")
			if len(r.choices) != 1 {
				t.Errorf("terminal actor selectable: %#v", r.choices)
			}
			qaActPick(t, r, actor.ID)
			r = f.next(t, "confirm")
			for _, n := range []string{actor.ID, "7", "9", "100", "quarant", "child", "sync"} {
				if !strings.Contains(strings.ToLower(r.title), n) {
					t.Errorf("interrupt warning hides %s", n)
				}
			}
			actors[0].Revision = "99"
			actors[0].Ref.Generation = "99"
			r.reply <- promptReply{confirmed: yes}
			if yes {
				qaActView(t, f)
			}
			if e := f.finish(t); e != nil {
				t.Fatal(e)
			}
			want := 0
			if yes {
				want = 1
			}
			if writes != want {
				t.Errorf("writes=%d", writes)
			}
		})
	}
}
func TestQAActivityControllerRecoveryEditInvalidatesPreviewAndUsesReturnedRevision(t *testing.T) {
	u := qaActUncertainty()
	previews := 0
	var captured activity.ResolveInput
	a := &ActivityActions{Review: func(ctx context.Context, in activity.ReviewInput) (activity.ReviewList, error) {
		qaActDeadline(t, ctx, 250*time.Millisecond)
		if in != (activity.ReviewInput{}) {
			t.Error("invented review filter")
		}
		return activity.ReviewList{Uncertainties: []activity.Uncertainty{u}}, nil
	}, Preview: func(ctx context.Context, in activity.RecoveryInput) (activity.RecoveryPreview, error) {
		qaActDeadline(t, ctx, 250*time.Millisecond)
		previews++
		if in.UncertaintyID != u.ID {
			t.Error("target changed")
		}
		if previews == 1 {
			if in.End == nil || in.End.Location() != time.UTC || in.DiscardTail {
				t.Errorf("end not normalizedUTC: %+v", in)
			}
			u.Revision = "12"
		} else if !in.DiscardTail || in.End != nil {
			t.Error("editing discard retained end")
		}
		return qaActPreview(u, in.End, in.DiscardTail), nil
	}, Resolve: func(ctx context.Context, in activity.ResolveInput) (activity.MutationResult, error) {
		qaActDeadline(t, ctx, 2*time.Minute)
		captured = in
		return qaActResult(in.RequestID), nil
	}}
	f := qaActStart(t, &activityController{}, a)
	qaActPick(t, f.next(t, "choose"), "review")
	qaActPick(t, f.next(t, "choose"), u.ID)
	qaActPick(t, f.next(t, "choose"), "end")
	r := f.next(t, "text")
	if r.defaultText != u.LowerBound.Format(time.RFC3339Nano) {
		t.Error("recovery default not exact lowerbound")
	}
	r.reply <- promptReply{text: "2026-10-02T14:01:00+02:00"}
	f.next(t, "text").reply <- promptReply{text: "first reason"}
	qaActPick(t, f.next(t, "choose"), "edit")
	qaActPick(t, f.next(t, "choose"), "discard")
	f.next(t, "text").reply <- promptReply{text: "explicit discarded tail"}
	qaActPick(t, f.next(t, "choose"), "confirm")
	confirm := f.next(t, "confirm")
	for _, n := range []string{u.ID, "12", "100", "2026-10-02", "dddddddd-dddd-4ddd-8ddd-dddddddddddd", "union", "discard"} {
		if !strings.Contains(strings.ToLower(confirm.title), n) {
			t.Errorf("preview warning hides %s", n)
		}
	}
	if captured.RequestID != "" {
		t.Fatal("Resolve before consent")
	}
	confirm.reply <- promptReply{confirmed: true}
	qaActView(t, f)
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
	if previews != 2 || captured.UncertaintyID != u.ID || captured.IfRevision != "12" || captured.End != nil || !captured.DiscardTail || captured.Reason != "explicit discarded tail" || !captured.Confirmed || !qaActUUID.MatchString(captured.RequestID) {
		t.Errorf("edited intent not exact preview: %+v previews=%d", captured, previews)
	}
}
func TestQAActivityControllerCaptureReceiptNeverResolves(t *testing.T) {
	writes := 0
	r := activity.HostReceipt{ID: "ffffffff-ffff-4fff-8fff-ffffffffffff", Source: "synthetic", SessionID: "session", DiagnosticCode: "capture-gap", Origin: "unverified", ProfileBasis: "operator_declared", Fingerprint: "synthetic-fingerprint"}
	a := &ActivityActions{Status: func(context.Context) (activity.ActivitySnapshot, error) {
		return activity.ActivitySnapshot{CaptureReviews: []activity.HostReceipt{r}}, nil
	}, Resolve: func(context.Context, activity.ResolveInput) (activity.MutationResult, error) {
		writes++
		return activity.MutationResult{}, nil
	}}
	f := qaActStart(t, &activityController{}, a)
	qaActPick(t, f.next(t, "choose"), "capture")
	qaActPick(t, f.next(t, "choose"), r.ID)
	qaActView(t, f, r.ID, "capture-gap", "unverified", "operator_declared", "synthetic-fingerprint")
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
	if writes != 0 {
		t.Error("capture review offered interval resolution")
	}
}
func TestQAActivityControllerUnknownInterruptExactReplayNoNewConsent(t *testing.T) {
	actor := qaActActor()
	calls, reads := 0, 0
	var original activity.InterruptInput
	unknown := &activity.Error{Code: "local_write_unknown", Uncertain: true, Details: map[string]any{}}
	a := &ActivityActions{Status: func(context.Context) (activity.ActivitySnapshot, error) {
		reads++
		return activity.ActivitySnapshot{Actors: []activity.Actor{actor}}, nil
	}, Interrupt: func(_ context.Context, in activity.InterruptInput) (activity.MutationResult, error) {
		calls++
		if calls == 1 {
			original = in
			unknown.Details["request_id"] = in.RequestID
			return activity.MutationResult{}, unknown
		}
		if !reflect.DeepEqual(original, in) {
			t.Errorf("replay changed exact intent: %+v vs %+v", in, original)
		}
		return qaActResult(in.RequestID), nil
	}}
	c := &activityController{}
	f := qaActStart(t, c, a)
	qaActPick(t, f.next(t, "choose"), "actors")
	qaActPick(t, f.next(t, "choose"), actor.ID)
	f.next(t, "confirm").reply <- promptReply{confirmed: true}
	qaActView(t, f, "unknown")
	r := f.next(t, "choose")
	qaActPick(t, r, "back")
	if e := f.finish(t); e != unknown {
		t.Error("back lost uncertainty")
	}
	actor.Revision = "999"
	actor.Ref.Generation = "999"
	f = qaActStart(t, c, a)
	r = f.next(t, "choose")
	if !strings.Contains(r.title, original.RequestID) {
		t.Error("pending replay hides exact request UUID")
	}
	qaActPick(t, r, "replay")
	qaActView(t, f)
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
	if calls != 2 || reads != 1 || c.pending != nil {
		t.Errorf("replay rediscovered/stillpending calls%d reads%d", calls, reads)
	}
}
func TestQAActivityControllerJoinedUnknownBeatsCanceledDispatch(t *testing.T) {
	actor := qaActActor()
	entered := make(chan struct{})
	unknown := &activity.Error{Code: "local_write_unknown", Uncertain: true}
	a := &ActivityActions{Status: func(context.Context) (activity.ActivitySnapshot, error) {
		return activity.ActivitySnapshot{Actors: []activity.Actor{actor}}, nil
	}, Interrupt: func(ctx context.Context, in activity.InterruptInput) (activity.MutationResult, error) {
		unknown.Details = map[string]any{"request_id": in.RequestID}
		close(entered)
		<-ctx.Done()
		return activity.MutationResult{}, unknown
	}}
	c := &activityController{}
	f := qaActStart(t, c, a)
	qaActPick(t, f.next(t, "choose"), "actors")
	qaActPick(t, f.next(t, "choose"), actor.ID)
	f.next(t, "confirm").reply <- promptReply{confirmed: true}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("not dispatched")
	}
	f.cancel(&terminal.ExitError{Code: 143})
	if e := f.finish(t); !errors.Is(e, unknown) || c.pending == nil {
		t.Errorf("unknown lost on canceled dispatch: %v", e)
	}
}

func TestQAActivityControllerRecoveryInvalidOrCanceledDraftNeverResolves(t *testing.T) {
	for _, mode := range []string{"invalid-end", "cancel-end", "cancel-reason", "cancel-preview", "decline-confirm"} {
		t.Run(mode, func(t *testing.T) {
			u := qaActUncertainty()
			previews, writes := 0, 0
			a := &ActivityActions{Review: func(context.Context, activity.ReviewInput) (activity.ReviewList, error) {
				return activity.ReviewList{Uncertainties: []activity.Uncertainty{u}}, nil
			}, Preview: func(_ context.Context, in activity.RecoveryInput) (activity.RecoveryPreview, error) {
				previews++
				return qaActPreview(u, in.End, in.DiscardTail), nil
			}, Resolve: func(context.Context, activity.ResolveInput) (activity.MutationResult, error) {
				writes++
				return activity.MutationResult{}, nil
			}}
			f := qaActStart(t, &activityController{}, a)
			qaActPick(t, f.next(t, "choose"), "review")
			qaActPick(t, f.next(t, "choose"), u.ID)
			qaActPick(t, f.next(t, "choose"), "end")
			r := f.next(t, "text")
			end0 := &terminal.ExitError{Code: 0}
			switch mode {
			case "invalid-end":
				r.reply <- promptReply{text: "not a UTC instant"}
				qaActView(t, f)
			case "cancel-end":
				r.reply <- promptReply{err: end0}
			default:
				r.reply <- promptReply{text: "2026-10-02T12:01:00Z"}
				r = f.next(t, "text")
				if mode == "cancel-reason" {
					r.reply <- promptReply{err: end0}
					break
				}
				r.reply <- promptReply{text: "verified reason"}
				r = f.next(t, "choose")
				if mode == "cancel-preview" {
					r.reply <- promptReply{err: end0}
					break
				}
				qaActPick(t, r, "confirm")
				f.next(t, "confirm").reply <- promptReply{confirmed: false}
			}
			e := f.finish(t)
			if e != nil && !errors.Is(e, end0) {
				t.Errorf("cancellation changed: %v", e)
			}
			if writes != 0 {
				t.Error("unconfirmed recovery mutated")
			}
			if (mode == "invalid-end" || mode == "cancel-end" || mode == "cancel-reason") && previews != 0 {
				t.Error("invalid/canceled draft ran preview")
			}
		})
	}
}
func TestQAActivityControllerRetainedResolveExactReplayWithoutNewPreview(t *testing.T) {
	u := qaActUncertainty()
	end := u.LowerBound.Add(time.Minute)
	in := activity.ResolveInput{UncertaintyID: u.ID, End: &end, IfRevision: u.Revision, Reason: "frozen reason", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", Confirmed: true}
	unknown := &activity.Error{Code: "local_write_unknown", Uncertain: true, Details: map[string]any{"request_id": in.RequestID}}
	c := &activityController{pending: &activityPending{operation: "resolve", resolve: in, err: unknown}}
	calls := 0
	a := &ActivityActions{Review: func(context.Context, activity.ReviewInput) (activity.ReviewList, error) {
		t.Error("replay rediscovered uncertainty")
		return activity.ReviewList{}, nil
	}, Preview: func(context.Context, activity.RecoveryInput) (activity.RecoveryPreview, error) {
		t.Error("replay regenerated preview")
		return activity.RecoveryPreview{}, nil
	}, Resolve: func(_ context.Context, got activity.ResolveInput) (activity.MutationResult, error) {
		calls++
		if !reflect.DeepEqual(got, in) {
			t.Errorf("resolve replay replaced immutable input %+v", got)
		}
		return qaActResult(got.RequestID), nil
	}}
	f := qaActStart(t, c, a)
	r := f.next(t, "choose")
	if !strings.Contains(r.title, in.RequestID) {
		t.Error("retained resolve request hidden")
	}
	qaActPick(t, r, "replay")
	qaActView(t, f)
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
	if calls != 1 || c.pending != nil {
		t.Error("replay not acknowledged exactly once")
	}
}
func TestQAActivityControllerConflictEndsIntentAndRenewedSelectionUsesFreshRevision(t *testing.T) {
	actor := qaActActor()
	reads, writes := 0, 0
	var first string
	a := &ActivityActions{Status: func(context.Context) (activity.ActivitySnapshot, error) {
		reads++
		actor.Revision = fmt.Sprint(8 + reads)
		return activity.ActivitySnapshot{Actors: []activity.Actor{actor}}, nil
	}, Interrupt: func(_ context.Context, in activity.InterruptInput) (activity.MutationResult, error) {
		writes++
		if writes == 1 {
			first = in.RequestID
			return activity.MutationResult{}, &activity.Error{Code: "revision_conflict", Message: "RAW-REVISION-CANARY"}
		}
		if in.IfRevision != "10" || in.RequestID == first {
			t.Error("renewed intent reused stale revision/request")
		}
		return qaActResult(in.RequestID), nil
	}}
	c := &activityController{}
	for n := 0; n < 2; n++ {
		f := qaActStart(t, c, a)
		qaActPick(t, f.next(t, "choose"), "actors")
		qaActPick(t, f.next(t, "choose"), actor.ID)
		f.next(t, "confirm").reply <- promptReply{confirmed: true}
		r := f.next(t, "view")
		if n == 0 {
			body := strings.ToLower(r.title + r.body)
			if !strings.Contains(body, "revision_conflict") || !strings.Contains(body, "reload") || strings.Contains(body, "raw-revision-canary") {
				t.Errorf("conflict guidance unsafe %s", body)
			}
		}
		r.reply <- promptReply{}
		if e := f.finish(t); e != nil {
			t.Fatal(e)
		}
		if writes != n+1 {
			t.Error("automatic retry")
		}
	}
	if reads != 2 || writes != 2 {
		t.Errorf("read%d writes%d", reads, writes)
	}
}
