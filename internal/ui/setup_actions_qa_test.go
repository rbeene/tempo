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
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
)

func qaSetupPartial() setup.Status {
	return setup.Status{ContractVersion: 1, Complete: false, Steps: []setup.Step{
		{Action: "auth.login", State: "complete", RequiredFields: []string{}, SafeMessage: "Credential and accessible account verified; no hook or upload readiness inferred."},
		{Action: "bindings.link", State: "input_required", RequiredFields: []string{"project_id", "task_id", "timezone"}, SafeMessage: "Link cancelled; any completed authentication step remains applied."},
		{Action: "hooks.status", State: "unsupported", RequiredFields: []string{}, SafeMessage: "Actual host delivery remains unverified."},
	}}
}
func qaSetupStart(t *testing.T, c *setupController, actions *SetupActions, views *ReadViews) *qaAuthFlow {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	f := &qaAuthFlow{newPromptBridge(ctx), make(chan error, 1), cancel}
	go func() { f.done <- c.run(ctx, f.p, actions, views); close(f.done) }()
	t.Cleanup(func() {
		cancel(context.Canceled)
		select {
		case <-f.done:
		case <-time.After(time.Second):
			t.Error("setup controller did not join canceled broker")
		}
	})
	return f
}
func qaSetupGuided(t *testing.T, f *qaAuthFlow) {
	t.Helper()
	r := f.next(t, "choose")
	if r.title != "Setup" {
		t.Errorf("Setup menu title %q", r.title)
	}
	qaAuthPick(t, r, "guided")
}

func TestQASetupControllerMenuBackIsLazyAndGuidedUsesExactBroker(t *testing.T) {
	var calls, reads atomic.Int32
	c := &setupController{}
	a := &SetupActions{Run: func(ctx context.Context, in setup.Input, p terminal.Prompter) (setup.Status, error) {
		calls.Add(1)
		qaAuthDeadline(t, ctx, 2*time.Minute)
		if in != (setup.Input{}) {
			t.Error("UI invented setup input instead of using shared guided choices")
		}
		secret, e := p.Secret(ctx, "Shared setup private secret")
		if e != nil {
			return setup.Status{}, e
		}
		defer clear(secret)
		if string(secret) != "synthetic-q-secret" {
			t.Error("broker changed private secret")
		}
		yes, e := p.Confirm(ctx, "Shared setup exact scope confirmation")
		if e != nil {
			return setup.Status{}, e
		}
		if !yes {
			return setup.Status{}, &terminal.ExitError{Code: 0}
		}
		return qaSetupPartial(), nil
	}}
	v := &ReadViews{Setup: func(context.Context) (setup.Status, error) { reads.Add(1); return setup.Status{}, nil }}
	f := qaSetupStart(t, c, a, v)
	qaAuthPick(t, f.next(t, "choose"), "back")
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 0 || reads.Load() != 0 || c.completed != nil || c.pending != nil {
		t.Fatal("opening/back caused guided/read action or retained invented result")
	}
	f = qaSetupStart(t, c, a, v)
	qaSetupGuided(t, f)
	f.next(t, "secret").reply <- promptReply{secret: []byte("synthetic-q-secret")}
	f.next(t, "confirm").reply <- promptReply{confirmed: true}
	r := f.next(t, "view")
	if !strings.Contains(r.body, "auth.login") || !strings.Contains(r.body, "complete") || !strings.Contains(r.body, "input_required") {
		t.Error("shared partial steps omitted")
	}
	r.reply <- promptReply{}
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 1 || reads.Load() != 0 || c.completed == nil || !reflect.DeepEqual(c.completed.status, qaSetupPartial()) || c.completed.err != nil {
		t.Fatal("guided broker reply changed or completion missing")
	}
}

func TestQASetupControllerRetainsExactPartialBeforeCanceledResultView(t *testing.T) {
	for _, mode := range []string{"partial-success", "known-failure", "auth-unknown"} {
		t.Run(mode, func(t *testing.T) {
			status := qaSetupPartial()
			var sharedErr error
			if mode == "known-failure" {
				sharedErr = &harvest.Error{Code: "network", Message: "RAW-SETUP-TRANSPORT-CANARY"}
			}
			if mode == "auth-unknown" {
				status.Steps[0].Action, status.Steps[0].State = "auth.status", "input_required"
				sharedErr = &auth.Error{Code: "credential_write_unknown", Message: "RAW-SETUP-CREDENTIAL-CANARY", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}
			}
			original := status
			original.Steps = append([]setup.Step(nil), status.Steps...)
			original.Steps[1].RequiredFields = append([]string(nil), status.Steps[1].RequiredFields...)
			c := &setupController{}
			a := &SetupActions{Run: func(context.Context, setup.Input, terminal.Prompter) (setup.Status, error) { return status, sharedErr }}
			f := qaSetupStart(t, c, a, nil)
			qaSetupGuided(t, f)
			r := f.next(t, "view")
			if c.completed == nil || c.completed.err != sharedErr || !reflect.DeepEqual(c.completed.status, original) {
				t.Fatal("shared Status/error not retained before View")
			}
			if strings.Contains(r.body, "RAW-") || strings.Contains(r.title, "RAW-") {
				t.Error("raw shared error leaked in Setup view")
			}
			status.Steps[0].State = "MUTATED"
			status.Steps[1].RequiredFields[0] = "MUTATED"
			if !reflect.DeepEqual(c.completed.status, original) {
				t.Fatal("retained shared Steps/RequiredFields alias caller memory")
			}
			cause := &terminal.ExitError{Code: 143}
			f.cancel(cause)
			returned := f.finish(t)
			if mode == "auth-unknown" {
				if returned != sharedErr || c.pending != sharedErr {
					t.Error("unknown dropped by canceled View")
				}
			} else if !errors.Is(returned, cause) {
				t.Errorf("known outcome replaced session cancellation %v", returned)
			}
			if c.completed == nil || c.completed.err != sharedErr || !reflect.DeepEqual(c.completed.status, original) {
				t.Fatal("canceled View erased original partial Status/error")
			}
		})
	}
}

func TestQASetupControllerJoinedReplyDuringCancellationKeepsExactSteps(t *testing.T) {
	for _, mode := range []string{"known", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			status := qaSetupPartial()
			var sharedErr error
			if mode == "unknown" {
				sharedErr = &activity.Error{Code: "local_write_unknown", Uncertain: true, Details: map[string]any{"request_id": "12345678-1234-1234-1234-123456789abc"}}
			}
			entered := make(chan struct{})
			a := &SetupActions{Run: func(ctx context.Context, _ setup.Input, _ terminal.Prompter) (setup.Status, error) {
				close(entered)
				<-ctx.Done()
				return status, sharedErr
			}}
			c := &setupController{}
			f := qaSetupStart(t, c, a, nil)
			qaSetupGuided(t, f)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("Guided not admitted")
			}
			cause := &terminal.ExitError{Code: 130}
			f.cancel(cause)
			returned := f.finish(t)
			if c.completed == nil || !reflect.DeepEqual(c.completed.status, status) || c.completed.err != sharedErr {
				t.Fatal("joined shared reply lost before canceled presentation")
			}
			if mode == "unknown" {
				if returned != sharedErr || c.pending != sharedErr {
					t.Error("joined uncertainty demoted")
				}
			} else if !errors.Is(returned, cause) {
				t.Error("known result erased session signal")
			}
		})
	}
}

func TestQASetupControllerUnknownReopenInspectionCannotPermitBlindGuidedRerun(t *testing.T) {
	unknown := &auth.Error{Code: "credential_write_unknown", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}
	status := qaSetupPartial()
	status.Steps[0].Action, status.Steps[0].State = "auth.status", "input_required"
	var calls, reads atomic.Int32
	a := &SetupActions{Run: func(context.Context, setup.Input, terminal.Prompter) (setup.Status, error) {
		calls.Add(1)
		return status, unknown
	}}
	v := &ReadViews{Setup: func(ctx context.Context) (setup.Status, error) {
		qaAuthDeadline(t, ctx, 250*time.Millisecond)
		reads.Add(1)
		return setup.Status{ContractVersion: 1, Complete: true, Steps: []setup.Step{{Action: "bindings.link", State: "complete", RequiredFields: []string{}, SafeMessage: "Current local observation only"}}}, errors.New("RAW-READINESS-CANARY")
	}}
	c := &setupController{}
	f := qaSetupStart(t, c, a, v)
	qaSetupGuided(t, f)
	f.next(t, "view").reply <- promptReply{}
	r := f.next(t, "choose")
	for _, choice := range r.choices {
		if choice.ID != "readiness" && choice.ID != "back" {
			t.Errorf("unknown enabled %s", choice.ID)
		}
	}
	qaAuthPick(t, r, "back")
	if e := f.finish(t); e != unknown {
		t.Fatal("Back dropped original uncertainty")
	}
	f = qaSetupStart(t, c, a, v)
	r = f.next(t, "choose")
	if r.title != "Setup · Outcome unknown" {
		t.Error("reopen lost pending recovery scope")
	}
	qaAuthPick(t, r, "readiness")
	r = f.next(t, "view")
	if strings.Contains(r.body, "RAW-") {
		t.Error("read-only error leaked")
	}
	r.reply <- promptReply{}
	r = f.next(t, "choose")
	qaAuthPick(t, r, "back")
	if e := f.finish(t); e != unknown {
		t.Fatal("readiness failure erased original unknown")
	}
	if calls.Load() != 1 || reads.Load() != 1 || c.pending != unknown || c.completed == nil || c.completed.err != unknown || !reflect.DeepEqual(c.completed.status, status) {
		t.Fatal("inspection/reopen reran Guided or overwrote original Status/outcome")
	}
}

func TestQASetupControllerReadinessNeverOverwritesKnownPartial(t *testing.T) {
	status := qaSetupPartial()
	sharedErr := &harvest.Error{Code: "network", Message: "RAW-SETUP-CANARY"}
	c := &setupController{completed: &setupCompletion{status: status, err: sharedErr}}
	a := &SetupActions{Run: func(context.Context, setup.Input, terminal.Prompter) (setup.Status, error) {
		t.Fatal("readiness reran guided mutation")
		return setup.Status{}, nil
	}}
	v := &ReadViews{Setup: func(ctx context.Context) (setup.Status, error) {
		qaAuthDeadline(t, ctx, 250*time.Millisecond)
		return setup.Status{ContractVersion: 1, Complete: false, Steps: []setup.Step{{Action: "auth.status", State: "input_required"}}}, nil
	}}
	f := qaSetupStart(t, c, a, v)
	qaAuthPick(t, f.next(t, "choose"), "readiness")
	f.next(t, "view").reply <- promptReply{}
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
	if c.completed == nil || c.completed.err != sharedErr || !reflect.DeepEqual(c.completed.status, status) {
		t.Fatal("readonly status erased known partial operation outcome")
	}
}
