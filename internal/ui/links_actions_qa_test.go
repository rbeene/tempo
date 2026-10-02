package ui_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
)

var qaUILinkUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func qaLinkRig(t *testing.T, actions *ui.LinkActions, views *ui.ReadViews) *qaRunnerRig {
	t.Helper()
	if views == nil {
		views = &ui.ReadViews{Links: func(context.Context) (activity.BindingList, error) { return qaUIReadonlyLinks(), nil }}
	}
	s := qaNewRunnerScreen()
	r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("10", "100"), nil }}
	x := qaStartRunnerOptions(t, s, r, ui.Options{Refresh: make(chan time.Time), Views: views, Links: actions})
	x.frame(t, "Project 100")
	return x
}

func qaLinkKey(x *qaRunnerRig, text string) {
	x.screen.events <- terminal.Event{Kind: "text", Text: text}
}
func qaLinkEnter(x *qaRunnerRig)  { x.screen.events <- terminal.Event{Kind: "enter"} }
func qaLinkEscape(x *qaRunnerRig) { x.screen.events <- terminal.Event{Kind: "escape"} }

func qaLinkFrame(t *testing.T, x *qaRunnerRig, needle string) []string {
	t.Helper()
	deadline := time.NewTimer(1500 * time.Millisecond)
	defer deadline.Stop()
	for {
		select {
		case frame := <-x.screen.frames:
			if strings.Contains(strings.ToLower(strings.Join(frame, "\n")), strings.ToLower(needle)) {
				return frame
			}
		case err := <-x.done:
			x.done <- err
			t.Fatalf("runner returned before link frame %q: %v", needle, err)
		case <-deadline.C:
			t.Fatalf("no link frame containing %q", needle)
		}
	}
}

func qaLinkOpen(t *testing.T, x *qaRunnerRig) {
	t.Helper()
	qaLinkKey(x, "l")
	qaLinkFrame(t, x, "Create link")
}

func qaLinkSelectBindingAction(t *testing.T, x *qaRunnerRig, action string) {
	t.Helper()
	qaLinkOpen(t, x)
	qaLinkKey(x, qaUIReadonlyLinks().Bindings[0].ID)
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Binding actions")
	qaLinkKey(x, action)
	qaLinkEnter(x)
}

func qaLinkYes(x *qaRunnerRig) { qaLinkKey(x, "y"); qaLinkEnter(x) }

func qaLinkCompleteResult(in activity.LinkInput) activity.BindingResult {
	return activity.BindingResult{ContractVersion: 1, RequestID: in.RequestID, SnapshotRevision: "11", Changed: true, Binding: activity.Binding{ID: qaUIReadonlyLinks().Bindings[0].ID, Revision: "8", Kind: "directory", Locator: in.Path, Attribution: activity.Attribution{AccountID: in.AccountID, ProjectID: in.ProjectID, TaskID: in.TaskID, UserID: "2", Timezone: in.Timezone}, AttachedActors: []activity.ActorRef{}}}
}

func qaLinkPrepared(ctx context.Context, in activity.LinkInput, p terminal.Prompter) (activity.LinkInput, error) {
	yes, err := p.Confirm(ctx, "SHARED-PREPARED-WARNING account 1 project 100 task 7 timezone UTC. Confirm capture mapping; historical attribution remains retained.")
	if err != nil {
		return activity.LinkInput{}, err
	}
	if !yes {
		return activity.LinkInput{}, &terminal.ExitError{Code: 0}
	}
	in.AccountID, in.ProjectID, in.TaskID, in.Timezone, in.IfRevision = "1", "100", "7", "UTC", "9"
	return in, nil
}

func TestQAUILinksCreateUsesExactPreparedInputAndRefreshesOnSuccess(t *testing.T) {
	preparedCall, committed := make(chan activity.LinkInput, 2), make(chan activity.LinkInput, 2)
	var prepares, commits atomic.Int32
	actions := &ui.LinkActions{Prepare: func(ctx context.Context, in activity.LinkInput, p terminal.Prompter) (activity.LinkInput, error) {
		prepares.Add(1)
		preparedCall <- in
		return qaLinkPrepared(ctx, in, p)
	}, Commit: func(ctx context.Context, in activity.LinkInput) (activity.BindingResult, error) {
		commits.Add(1)
		committed <- in
		return qaLinkCompleteResult(in), nil
	}}
	x := qaLinkRig(t, actions, nil)
	reads := x.reader.calls.Load()
	qaLinkOpen(t, x)
	qaLinkKey(x, "create")
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Absolute project path")
	path := "/synthetic/qa-project"
	unclean := path + "/../project"
	// Pasted text stays a draft until explicit Enter.
	x.screen.events <- terminal.Event{Kind: "paste", Text: unclean}
	qaLinkFrame(t, x, unclean)
	if prepares.Load() != 0 {
		t.Fatal("path paste dispatched preparation")
	}
	qaLinkEnter(x)
	qaLinkFrame(t, x, "SHARED-PREPARED-WARNING")
	initial := <-preparedCall
	if !filepath.IsAbs(initial.Path) || initial.Path != filepath.Clean(unclean) || !qaUILinkUUID.MatchString(initial.RequestID) {
		t.Fatalf("UI did not freeze clean absolute path and request identity: %+v", initial)
	}
	if commits.Load() != 0 {
		t.Fatal("shared preparation committed before confirmation")
	}
	qaLinkYes(x)
	frame := qaLinkFrame(t, x, "Links · Complete")
	got := <-committed
	want := initial
	want.AccountID, want.ProjectID, want.TaskID, want.Timezone, want.IfRevision = "1", "100", "7", "UTC", "9"
	if got != want || !strings.Contains(strings.Join(frame, "\n"), got.RequestID) || prepares.Load() != 1 || commits.Load() != 1 {
		t.Fatalf("commit replaced prepared intent or lost receipt: got %+v want %+v frame %q", got, want, frame)
	}
	qaLinkEnter(x)
	x.frame(t, "Project 100")
	deadline := time.Now().Add(time.Second)
	for x.reader.calls.Load() <= reads && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if x.reader.calls.Load() <= reads {
		t.Error("successful mutation did not invalidate authoritative snapshot")
	}
	qaLinkKey(x, "q")
	x.finish(t, nil)
}

func TestQAUILinksCreateCanceledOrInvalidDraftNeverCommits(t *testing.T) {
	for _, mode := range []string{"escape-path", "empty-path", "relative-path", "escape-confirm", "negative-confirm"} {
		t.Run(mode, func(t *testing.T) {
			var prepares, commits atomic.Int32
			actions := &ui.LinkActions{Prepare: func(ctx context.Context, in activity.LinkInput, p terminal.Prompter) (activity.LinkInput, error) {
				prepares.Add(1)
				return qaLinkPrepared(ctx, in, p)
			}, Commit: func(context.Context, activity.LinkInput) (activity.BindingResult, error) {
				commits.Add(1)
				return activity.BindingResult{}, nil
			}}
			x := qaLinkRig(t, actions, nil)
			qaLinkOpen(t, x)
			qaLinkKey(x, "create")
			qaLinkEnter(x)
			qaLinkFrame(t, x, "Absolute project path")
			switch mode {
			case "escape-path":
				qaLinkKey(x, "q")
				qaLinkFrame(t, x, "q")
				qaLinkEscape(x)
			case "empty-path", "relative-path":
				if mode == "relative-path" {
					qaLinkKey(x, "./relative-project")
				}
				qaLinkEnter(x)
				qaLinkFrame(t, x, "Links · Failed")
				qaLinkEnter(x)
			default:
				qaLinkKey(x, t.TempDir())
				qaLinkEnter(x)
				qaLinkFrame(t, x, "SHARED-PREPARED-WARNING")
				if mode == "escape-confirm" {
					qaLinkEscape(x)
				} else {
					qaLinkEnter(x)
				}
			}
			x.frame(t, "Project 100")
			if commits.Load() != 0 || prepares.Load() != 0 && mode != "escape-confirm" && mode != "negative-confirm" {
				t.Fatalf("canceled/invalid draft reached mutation: prepare=%d commit=%d", prepares.Load(), commits.Load())
			}
			qaLinkKey(x, "q")
			x.finish(t, nil)
		})
	}
}

func TestQAUILinksUnlinkAndRepairFreezeObservedTargetAndRequireConsent(t *testing.T) {
	for _, op := range []string{"unlink", "repair"} {
		t.Run(op, func(t *testing.T) {
			binding := qaUIReadonlyLinks().Bindings[0]
			var calls, reads atomic.Int32
			unlinked, repaired := make(chan activity.UnlinkInput, 1), make(chan activity.RepairBindingInput, 1)
			actions := &ui.LinkActions{Unlink: func(ctx context.Context, in activity.UnlinkInput) (activity.MutationResult, error) {
				calls.Add(1)
				unlinked <- in
				return activity.MutationResult{ContractVersion: 1, RequestID: in.RequestID, SnapshotRevision: "11", Changed: true, AffectedIDs: []string{in.BindingID}}, nil
			}, Repair: func(ctx context.Context, in activity.RepairBindingInput) (activity.BindingResult, error) {
				calls.Add(1)
				repaired <- in
				return activity.BindingResult{ContractVersion: 1, RequestID: in.RequestID, SnapshotRevision: "11", Binding: binding}, nil
			}}
			views := &ui.ReadViews{Links: func(ctx context.Context) (activity.BindingList, error) {
				reads.Add(1)
				qaUIBudget(t, ctx)
				return qaUIReadonlyLinks(), nil
			}}
			x := qaLinkRig(t, actions, views)
			qaLinkSelectBindingAction(t, x, op)
			path := t.TempDir()
			if op == "repair" {
				qaLinkFrame(t, x, "Replacement absolute path")
				qaLinkKey(x, path+"/../"+filepath.Base(path))
				qaLinkEnter(x)
			}
			frame := qaLinkFrame(t, x, "histor")
			text := strings.Join(frame, "\n")
			for _, span := range []string{binding.ID, binding.Locator, binding.Attribution.AccountID, binding.Attribution.ProjectID, binding.Attribution.Timezone, binding.Revision} {
				if !strings.Contains(text, span) {
					t.Errorf("confirmation hid observed target field %q: %q", span, text)
				}
			}
			if calls.Load() != 0 {
				t.Fatal("target menu/path applied mutation before confirmation")
			}
			x.screen.events <- terminal.Event{Kind: "resize", Columns: 20, Rows: 5}
			qaLinkFrame(t, x, "too small")
			qaLinkYes(x)
			qaLinkFrame(t, x, "too small")
			if calls.Load() != 0 {
				t.Fatal("tiny confirmation committed hidden warning")
			}
			x.screen.events <- terminal.Event{Kind: "resize", Columns: 120, Rows: 24}
			qaLinkFrame(t, x, "histor")
			qaLinkYes(x)
			qaLinkFrame(t, x, "Links · Complete")
			if calls.Load() != 1 || reads.Load() != 1 {
				t.Fatalf("dispatch/read counts mutation=%d read=%d", calls.Load(), reads.Load())
			}
			if op == "unlink" {
				got := <-unlinked
				if got.BindingID != binding.ID || got.IfRevision != binding.Revision || !got.Confirmed || !qaUILinkUUID.MatchString(got.RequestID) {
					t.Fatalf("unlink changed observed intent: %+v", got)
				}
			} else {
				got := <-repaired
				if got.BindingID != binding.ID || got.IfRevision != binding.Revision || got.Path != filepath.Clean(path) || !got.Confirmed || !qaUILinkUUID.MatchString(got.RequestID) {
					t.Fatalf("repair changed observed intent: %+v", got)
				}
			}
			qaLinkEnter(x)
			x.frame(t, "Project 100")
			qaLinkKey(x, "q")
			x.finish(t, nil)
		})
	}
}

func TestQAUILinksDuplicateKeysDoNotDispatchConcurrentActions(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	actions := &ui.LinkActions{Unlink: func(ctx context.Context, in activity.UnlinkInput) (activity.MutationResult, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
		case <-ctx.Done():
			return activity.MutationResult{}, context.Cause(ctx)
		}
		return activity.MutationResult{ContractVersion: 1, RequestID: in.RequestID, SnapshotRevision: "11"}, nil
	}}
	x := qaLinkRig(t, actions, nil)
	qaLinkSelectBindingAction(t, x, "unlink")
	qaLinkFrame(t, x, "histor")
	qaLinkYes(x)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("confirmed action never dispatched")
	}
	qaLinkEnter(x)
	qaLinkEnter(x)
	qaLinkKey(x, "l")
	qaLinkKey(x, "r")
	x.screen.events <- terminal.Event{Kind: "resize", Columns: 100, Rows: 24}
	qaLinkFrame(t, x, "histor")
	if calls.Load() != 1 {
		t.Fatalf("duplicate keys dispatched %d mutations", calls.Load())
	}
	close(release)
	qaLinkFrame(t, x, "Links · Complete")
	qaLinkEnter(x)
	x.frame(t, "Project 100")
	qaLinkKey(x, "q")
	x.finish(t, nil)
}

func TestQAUILinksRefreshCannotReplaceOpenMutationTarget(t *testing.T) {
	var reads, listReads atomic.Int32
	committed := make(chan activity.UnlinkInput, 1)
	actions := &ui.LinkActions{Unlink: func(_ context.Context, in activity.UnlinkInput) (activity.MutationResult, error) {
		committed <- in
		return activity.MutationResult{ContractVersion: 1, RequestID: in.RequestID, SnapshotRevision: "12"}, nil
	}}
	views := &ui.ReadViews{Links: func(context.Context) (activity.BindingList, error) {
		list := qaUIReadonlyLinks()
		if listReads.Add(1) > 1 {
			list.Bindings[0].ID, list.Bindings[0].Revision = "44444444-4444-4444-8444-444444444444", "99"
		}
		return list, nil
	}}
	screen := qaNewRunnerScreen()
	reader := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) {
		if reads.Add(1) == 1 {
			return qaUISnapshot("10", "100", "200"), nil
		}
		return qaUISnapshot("11", "200", "100"), nil
	}}
	ticks := make(chan time.Time, 1)
	x := qaStartRunnerOptions(t, screen, reader, ui.Options{Refresh: ticks, Views: views, Links: actions})
	x.frame(t, "Project 100")
	qaLinkSelectBindingAction(t, x, "unlink")
	qaLinkFrame(t, x, "histor")
	ticks <- time.Now()
	deadline := time.Now().Add(time.Second)
	for reads.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if reads.Load() < 2 {
		t.Fatal("open confirmation blocked status refresh")
	}
	screen.events <- terminal.Event{Kind: "resize", Columns: 100, Rows: 24}
	frame := qaLinkFrame(t, x, "histor")
	if !strings.Contains(strings.Join(frame, "\n"), qaUIReadonlyLinks().Bindings[0].ID) {
		t.Fatal("refresh replaced visible immutable confirmation target")
	}
	qaLinkYes(x)
	qaLinkFrame(t, x, "Links · Complete")
	got := <-committed
	if got.BindingID != qaUIReadonlyLinks().Bindings[0].ID || got.IfRevision != "7" || listReads.Load() != 1 {
		t.Fatalf("refresh substituted target/revision or reread ancillary bindings: %+v reads=%d", got, listReads.Load())
	}
	qaLinkEnter(x)
	x.frame(t, "Project 100")
	qaLinkKey(x, "q")
	x.finish(t, nil)
}

func TestQAUILinksUnknownReplayRetainsExactPreparedIntent(t *testing.T) {
	var prepares, commits atomic.Int32
	var inputs []activity.LinkInput
	actions := &ui.LinkActions{Prepare: func(ctx context.Context, in activity.LinkInput, p terminal.Prompter) (activity.LinkInput, error) {
		prepares.Add(1)
		return qaLinkPrepared(ctx, in, p)
	}, Commit: func(_ context.Context, in activity.LinkInput) (activity.BindingResult, error) {
		inputs = append(inputs, in)
		if commits.Add(1) == 1 {
			return activity.BindingResult{}, &activity.Error{Code: "local_write_unknown", Message: "safe local uncertainty", Uncertain: true, Details: map[string]any{"request_id": in.RequestID}}
		}
		return qaLinkCompleteResult(in), nil
	}}
	x := qaLinkRig(t, actions, nil)
	qaLinkOpen(t, x)
	qaLinkKey(x, "create")
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Absolute project path")
	qaLinkKey(x, t.TempDir())
	qaLinkEnter(x)
	qaLinkFrame(t, x, "SHARED-PREPARED-WARNING")
	qaLinkYes(x)
	frame := qaLinkFrame(t, x, "Links · Outcome unknown")
	if len(inputs) != 1 || !strings.Contains(strings.Join(frame, "\n"), inputs[0].RequestID) {
		t.Fatalf("unknown frame lost exact request identity: %q", frame)
	}
	if commits.Load() != 1 {
		t.Fatal("unknown outcome automatically retried")
	}
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Recover submitted intent")
	qaLinkKey(x, "replay")
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Links · Complete")
	if prepares.Load() != 1 || commits.Load() != 2 || len(inputs) != 2 || !reflect.DeepEqual(inputs[0], inputs[1]) {
		t.Fatalf("replay changed exact input/UUID or repeated guided discovery: %+v prepare=%d commit=%d", inputs, prepares.Load(), commits.Load())
	}
	qaLinkEnter(x)
	x.frame(t, "Project 100")
	qaLinkKey(x, "q")
	x.finish(t, nil)
}

func TestQAUILinksUnlinkAndRepairUnknownReplayDoNotRenewConsent(t *testing.T) {
	for _, operation := range []string{"unlink", "repair"} {
		t.Run(operation, func(t *testing.T) {
			var reads, calls atomic.Int32
			var unlinks []activity.UnlinkInput
			var repairs []activity.RepairBindingInput
			unknown := func(request string) error {
				return &activity.Error{Code: "local_write_unknown", Message: "safe local uncertainty", Uncertain: true, Details: map[string]any{"request_id": request}}
			}
			actions := &ui.LinkActions{Unlink: func(_ context.Context, in activity.UnlinkInput) (activity.MutationResult, error) {
				unlinks = append(unlinks, in)
				if calls.Add(1) == 1 {
					return activity.MutationResult{}, unknown(in.RequestID)
				}
				return activity.MutationResult{ContractVersion: 1, RequestID: in.RequestID}, nil
			}, Repair: func(_ context.Context, in activity.RepairBindingInput) (activity.BindingResult, error) {
				repairs = append(repairs, in)
				if calls.Add(1) == 1 {
					return activity.BindingResult{}, unknown(in.RequestID)
				}
				return activity.BindingResult{ContractVersion: 1, RequestID: in.RequestID}, nil
			}}
			views := &ui.ReadViews{Links: func(context.Context) (activity.BindingList, error) { reads.Add(1); return qaUIReadonlyLinks(), nil }}
			x := qaLinkRig(t, actions, views)
			qaLinkSelectBindingAction(t, x, operation)
			if operation == "repair" {
				qaLinkFrame(t, x, "Replacement absolute path")
				qaLinkKey(x, t.TempDir())
				qaLinkEnter(x)
			}
			qaLinkFrame(t, x, "histor")
			qaLinkYes(x)
			qaLinkFrame(t, x, "Links · Outcome unknown")
			qaLinkEnter(x)
			qaLinkFrame(t, x, "Recover submitted intent")
			qaLinkKey(x, "replay")
			qaLinkEnter(x)
			qaLinkFrame(t, x, "Links · Complete")
			if calls.Load() != 2 || reads.Load() != 1 {
				t.Fatalf("typed replay rediscovered target or renewed consent: calls=%d reads=%d", calls.Load(), reads.Load())
			}
			if operation == "unlink" {
				if len(unlinks) != 2 || unlinks[0] != unlinks[1] {
					t.Fatalf("unlink replay replaced confirmed input: %+v", unlinks)
				}
			} else if len(repairs) != 2 || repairs[0] != repairs[1] {
				t.Fatalf("repair replay replaced confirmed path/revision/input: %+v", repairs)
			}
			qaLinkEnter(x)
			x.frame(t, "Project 100")
			qaLinkKey(x, "q")
			x.finish(t, nil)
		})
	}
}

func TestQAUILinksUnknownDominatesCanceledShutdownAfterJoin(t *testing.T) {
	for _, mode := range []string{"ctrlc", "sigterm", "eof", "quit", "cleanup-failure"} {
		t.Run(mode, func(t *testing.T) {
			started, joined := make(chan *activity.Error, 1), make(chan struct{}, 1)
			var active, calls atomic.Int32
			actions := &ui.LinkActions{Unlink: func(ctx context.Context, in activity.UnlinkInput) (activity.MutationResult, error) {
				calls.Add(1)
				active.Add(1)
				defer func() { active.Add(-1); joined <- struct{}{} }()
				uncertain := &activity.Error{Code: "local_write_unknown", Message: "safe local uncertainty", Uncertain: true, Details: map[string]any{"request_id": in.RequestID}}
				started <- uncertain
				<-ctx.Done()
				return activity.MutationResult{}, uncertain
			}}
			x := qaLinkRig(t, actions, nil)
			x.screen.mu.Lock()
			x.screen.closeCheck = func() bool { return active.Load() != 0 }
			x.screen.mu.Unlock()
			if mode == "cleanup-failure" {
				x.screen.mu.Lock()
				x.screen.closeErr = errors.New("synthetic restoration failure")
				x.screen.mu.Unlock()
			}
			qaLinkSelectBindingAction(t, x, "unlink")
			qaLinkFrame(t, x, "histor")
			qaLinkYes(x)
			var uncertain *activity.Error
			select {
			case uncertain = <-started:
			case <-time.After(time.Second):
				t.Fatal("confirmed mutation never dispatched")
			}
			switch mode {
			case "ctrlc":
				x.cancel(&terminal.ExitError{Code: 130})
			case "sigterm", "cleanup-failure":
				x.cancel(&terminal.ExitError{Code: 143})
			case "eof":
				x.screen.endErrors <- &terminal.ExitError{Code: 0}
			case "quit":
				qaLinkEscape(x)
				x.frame(t, "Project 100")
				qaLinkKey(x, "q")
			}
			x.finish(t, uncertain)
			select {
			case <-joined:
			default:
				t.Fatal("unknown result returned before action worker joined")
			}
			if active.Load() != 0 || calls.Load() != 1 || x.screen.closes.Load() != 1 {
				t.Fatalf("shutdown duplicated action or closed before join: active=%d calls=%d closes=%d", active.Load(), calls.Load(), x.screen.closes.Load())
			}
		})
	}
}

func TestQAUILinksUnknownStatusAndBackNeverAuthorizeNewIntent(t *testing.T) {
	var mutations, reads atomic.Int32
	var uncertain *activity.Error
	actions := &ui.LinkActions{Unlink: func(_ context.Context, in activity.UnlinkInput) (activity.MutationResult, error) {
		mutations.Add(1)
		uncertain = &activity.Error{Code: "local_write_unknown", Message: "safe local uncertainty", Uncertain: true, Details: map[string]any{"request_id": in.RequestID}}
		return activity.MutationResult{}, uncertain
	}}
	views := &ui.ReadViews{Links: func(ctx context.Context) (activity.BindingList, error) {
		qaUIBudget(t, ctx)
		reads.Add(1)
		return qaUIReadonlyLinks(), nil
	}}
	x := qaLinkRig(t, actions, views)
	qaLinkSelectBindingAction(t, x, "unlink")
	qaLinkFrame(t, x, "histor")
	qaLinkYes(x)
	qaLinkFrame(t, x, "Links · Outcome unknown")
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Recover submitted intent")
	qaLinkKey(x, "status")
	qaLinkEnter(x)
	qaLinkFrame(t, x, qaUIReadonlyLinks().Bindings[0].ID)
	if mutations.Load() != 1 || reads.Load() != 2 {
		t.Fatalf("read-only recovery status wrote or skipped local read: mutations=%d reads=%d", mutations.Load(), reads.Load())
	}
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Recover submitted intent")
	qaLinkKey(x, "back")
	qaLinkEnter(x)
	x.frame(t, "Project 100")
	qaLinkKey(x, "l")
	// Reopening must expose the retained recovery intent, not Create link.
	qaLinkFrame(t, x, "Recover submitted intent")
	if mutations.Load() != 1 || reads.Load() != 2 {
		t.Fatalf("reopening unknown intent restarted mutation/discovery: mutations=%d reads=%d", mutations.Load(), reads.Load())
	}
	qaLinkEscape(x)
	x.frame(t, "Project 100")
	qaLinkKey(x, "q")
	x.finish(t, uncertain)
}

func TestQAUILinksRevisionConflictRequiresFreshReadAndConfirmation(t *testing.T) {
	var reads, calls atomic.Int32
	inputs := make(chan activity.UnlinkInput, 2)
	views := &ui.ReadViews{Links: func(context.Context) (activity.BindingList, error) {
		list := qaUIReadonlyLinks()
		if reads.Add(1) > 1 {
			list.Bindings[0].Revision = "8"
		}
		return list, nil
	}}
	actions := &ui.LinkActions{Unlink: func(_ context.Context, in activity.UnlinkInput) (activity.MutationResult, error) {
		inputs <- in
		if calls.Add(1) == 1 {
			return activity.MutationResult{}, &activity.Error{Code: "revision_conflict", Message: "PRIVATE raw data SECRET"}
		}
		return activity.MutationResult{ContractVersion: 1, RequestID: in.RequestID, SnapshotRevision: "11"}, nil
	}}
	x := qaLinkRig(t, actions, views)
	qaLinkSelectBindingAction(t, x, "unlink")
	qaLinkFrame(t, x, "histor")
	qaLinkYes(x)
	frame := qaLinkFrame(t, x, "Links · Failed")
	text := strings.ToLower(strings.Join(frame, "\n"))
	if !strings.Contains(text, "revision_conflict") || !strings.Contains(text, "reload") || !strings.Contains(text, "confirm") || strings.Contains(text, "private") || strings.Contains(text, "secret") {
		t.Fatalf("conflict diagnostics lost renewal guidance or exposed raw error: %q", frame)
	}
	first := <-inputs
	if calls.Load() != 1 {
		t.Fatal("revision conflict automatically retried")
	}
	qaLinkEnter(x)
	x.frame(t, "Project 100")
	qaLinkSelectBindingAction(t, x, "unlink")
	qaLinkFrame(t, x, "histor")
	if calls.Load() != 1 || reads.Load() != 2 {
		t.Fatal("new observed revision skipped fresh read or confirmation")
	}
	qaLinkYes(x)
	qaLinkFrame(t, x, "Links · Complete")
	second := <-inputs
	if first.IfRevision != "7" || second.IfRevision != "8" || first.RequestID == second.RequestID || first.BindingID != second.BindingID {
		t.Fatalf("renewed intent reused stale revision/UUID: first %+v second %+v", first, second)
	}
	qaLinkEnter(x)
	x.frame(t, "Project 100")
	qaLinkKey(x, "q")
	x.finish(t, nil)
}

func TestQAUILinksLongScopeMustBeFullyReviewedBeforeConfirmation(t *testing.T) {
	list := qaUIReadonlyLinks()
	list.Bindings[0].Locator = "/" + strings.Repeat("long-visible-scope-segment/", 160) + "FINAL-SCOPE-END"
	var calls atomic.Int32
	actions := &ui.LinkActions{Unlink: func(_ context.Context, in activity.UnlinkInput) (activity.MutationResult, error) {
		calls.Add(1)
		return activity.MutationResult{ContractVersion: 1, RequestID: in.RequestID}, nil
	}}
	views := &ui.ReadViews{Links: func(context.Context) (activity.BindingList, error) { return list, nil }}
	x := qaLinkRig(t, actions, views)
	qaLinkSelectBindingAction(t, x, "unlink")
	qaLinkFrame(t, x, "Read full warning")
	qaLinkYes(x)
	qaLinkFrame(t, x, "Read full warning")
	if calls.Load() != 0 {
		t.Fatal("affirmative committed before full locator and warning were read")
	}
	for i := 0; i < 100; i++ {
		x.screen.events <- terminal.Event{Kind: "down"}
	}
	qaLinkFrame(t, x, "FINAL-SCOPE-END")
	qaLinkFrame(t, x, "histor")
	qaLinkYes(x)
	qaLinkFrame(t, x, "Links · Complete")
	if calls.Load() != 1 {
		t.Fatalf("fully reviewed explicit confirmation dispatched %d times", calls.Load())
	}
	qaLinkEnter(x)
	x.frame(t, "Project 100")
	qaLinkKey(x, "q")
	x.finish(t, nil)
}

func TestQAUILinksUnavailableActionsAreExplicitAndSafe(t *testing.T) {
	x := qaLinkRig(t, nil, nil)
	qaLinkKey(x, "l")
	qaLinkFrame(t, x, "unavailable")
	qaLinkEscape(x)
	x.frame(t, "Project 100")
	qaLinkKey(x, "q")
	x.finish(t, nil)
}
