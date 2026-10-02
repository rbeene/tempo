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
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
)

func qaSetupRunPartial() setup.Status {
	return setup.Status{ContractVersion: 1, Complete: false, Steps: []setup.Step{{Action: "auth.login", State: "complete", RequiredFields: []string{}, SafeMessage: "Credential and accessible account verified; no hook or upload readiness inferred."}, {Action: "bindings.link", State: "input_required", RequiredFields: []string{"project_id", "task_id", "timezone"}, SafeMessage: "Completed authentication remains applied."}}}
}
func qaSetupRunPick(t *testing.T, x *qaRunnerRig, id string) {
	t.Helper()
	qaLinkKey(x, ",")
	qaSetupChoice(t, x, "Setup", id)
}
func qaSetupChoice(t *testing.T, x *qaRunnerRig, title, id string) {
	t.Helper()
	deadline := time.NewTimer(1500 * time.Millisecond)
	defer deadline.Stop()
	for {
		select {
		case frame := <-x.screen.frames:
			if len(frame) > 1 && strings.TrimSpace(frame[0]) == title && strings.HasPrefix(frame[1], "Search: ") {
				qaLinkKey(x, id)
				qaLinkEnter(x)
				return
			}
		case err := <-x.done:
			x.done <- err
			t.Fatalf("Setup picker %s skipped: %v", title, err)
		case <-deadline.C:
			t.Fatalf("missing Setup picker %s", title)
		}
	}
}

func TestQAUISetupMenuBackAndReadinessStayLazy(t *testing.T) {
	var guided, read, reported atomic.Int32
	o := ui.Options{Refresh: make(chan time.Time), Setup: &ui.SetupActions{Run: func(context.Context, setup.Input, terminal.Prompter) (setup.Status, error) {
		guided.Add(1)
		return qaSetupRunPartial(), nil
	}}, Views: &ui.ReadViews{Setup: func(ctx context.Context) (setup.Status, error) {
		qaUIBudget(t, ctx)
		read.Add(1)
		return setup.Status{ContractVersion: 1, Complete: false, Steps: []setup.Step{{Action: "auth.status", State: "input_required", SafeMessage: "LOCAL-READINESS-ONLY"}}}, nil
	}}, OnSetupResult: func(setup.Status, error) { reported.Add(1) }}
	x := qaStartRunnerOptions(t, qaNewRunnerScreen(), qaHWRunReader(), o)
	x.frame(t, "No activity")
	qaSetupRunPick(t, x, "back")
	x.frame(t, "No activity")
	if guided.Load() != 0 || read.Load() != 0 {
		t.Error("opening Setup/back caused service reads or writes")
	}
	qaSetupRunPick(t, x, "readiness")
	qaLinkFrame(t, x, "LOCAL-READINESS-ONLY")
	qaLinkEscape(x)
	x.frame(t, "No activity")
	qaLinkKey(x, "q")
	x.finish(t, nil)
	if guided.Load() != 0 || read.Load() != 1 || reported.Load() != 0 {
		t.Fatal("readiness ran Guided or invented retained mutation outcome")
	}
}

func TestQAUISetupSharedBrokerPartialSurvivesBackAndReadiness(t *testing.T) {
	status := qaSetupRunPartial()
	sharedErr := &harvest.Error{Code: "network", Message: "RAW-SETUP-CANARY"}
	var calls, reads atomic.Int32
	screen := qaNewRunnerScreen()
	reports := 0
	o := ui.Options{Refresh: make(chan time.Time), Setup: &ui.SetupActions{Run: func(ctx context.Context, in setup.Input, p terminal.Prompter) (setup.Status, error) {
		calls.Add(1)
		qaHWRunBound(t, ctx, 2*time.Minute)
		if in != (setup.Input{}) {
			t.Error("UI invented wizard selections")
		}
		secret, e := p.Secret(ctx, "Shared setup private secret")
		if e != nil {
			return setup.Status{}, e
		}
		defer clear(secret)
		if string(secret) != "synthetic-q-secret" {
			t.Error("shared secret changed")
		}
		path, e := p.Text(ctx, "Shared setup path", "")
		if e != nil {
			return status, e
		}
		if path != "/synthetic/q-path" {
			t.Error("q/paste input changed")
		}
		yes, e := p.Confirm(ctx, "Shared setup full scoped confirmation")
		if e != nil || !yes {
			return status, e
		}
		return status, sharedErr
	}}, Views: &ui.ReadViews{Setup: func(ctx context.Context) (setup.Status, error) {
		qaUIBudget(t, ctx)
		reads.Add(1)
		return setup.Status{ContractVersion: 1, Complete: true, Steps: []setup.Step{{Action: "auth.status", State: "complete", SafeMessage: "READ-ONLY-CURRENT"}}}, nil
	}}, OnSetupResult: func(got setup.Status, e error) {
		reports++
		if screen.closes.Load() != 1 || !reflect.DeepEqual(got, status) || e != sharedErr {
			t.Errorf("original partial shared outcome changed or reported before Close: %+v %v", got, e)
		}
	}}
	x := qaStartRunnerOptions(t, screen, qaHWRunReader(), o)
	x.frame(t, "No activity")
	qaSetupRunPick(t, x, "guided")
	qaLinkFrame(t, x, "Shared setup private secret")
	x.screen.events <- terminal.Event{Kind: "paste", Text: "synthetic-q-secret"}
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Shared setup path")
	x.screen.events <- terminal.Event{Kind: "paste", Text: "/synthetic/q-path\n"}
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != 1 || screen.closes.Load() != 0 {
		t.Fatal("paste submitted or q text quit guided Setup")
	}
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Shared setup full scoped confirmation")
	qaLinkYes(x)
	qaLinkFrame(t, x, "auth.login")
	qaLinkEscape(x)
	x.frame(t, "No activity")
	qaSetupRunPick(t, x, "readiness")
	qaLinkFrame(t, x, "READ-ONLY-CURRENT")
	qaLinkEscape(x)
	x.frame(t, "No activity")
	qaLinkKey(x, "q")
	x.finish(t, nil)
	if calls.Load() != 1 || reads.Load() != 1 || reports != 1 {
		t.Errorf("partial outcome lost/repeated: calls%d reads%d reports%d", calls.Load(), reads.Load(), reports)
	}
}

func TestQAUISetupJoinedPartialAndUnknownSurviveShutdownAndClose(t *testing.T) {
	for _, outcome := range []string{"known-partial", "auth-unknown", "later-link-unknown"} {
		for _, mode := range []string{"eof", "sigint", "sigterm"} {
			t.Run(outcome+"/"+mode, func(t *testing.T) {
				status := qaSetupRunPartial()
				var sharedErr error
				if outcome == "auth-unknown" {
					status.Steps[0].Action, status.Steps[0].State = "auth.status", "input_required"
					sharedErr = &auth.Error{Code: "credential_write_unknown", Message: "RAW-SETUP-CANARY", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}
				}
				if outcome == "later-link-unknown" {
					sharedErr = &activity.Error{Code: "local_write_unknown", Uncertain: true, Details: map[string]any{"request_id": "12345678-1234-1234-1234-123456789abc"}}
				}
				var active atomic.Int32
				entered := make(chan struct{})
				s := qaNewRunnerScreen()
				s.closeErr = errors.New("RAW-CLOSE-CANARY")
				s.closeCheck = func() bool { return active.Load() != 0 }
				reports, notices := 0, 0
				o := ui.Options{Refresh: make(chan time.Time), Setup: &ui.SetupActions{Run: func(ctx context.Context, _ setup.Input, _ terminal.Prompter) (setup.Status, error) {
					active.Add(1)
					defer active.Add(-1)
					close(entered)
					<-ctx.Done()
					return status, sharedErr
				}}, OnSetupResult: func(got setup.Status, e error) {
					reports++
					if active.Load() != 0 || s.closes.Load() != 1 || e != sharedErr || !reflect.DeepEqual(got, status) {
						t.Error("Setup report lost original joined Status/error or preceded cleanup")
					}
				}, OnRestorationFailure: func() {
					notices++
					if active.Load() != 0 || s.closes.Load() != 1 {
						t.Error("restoration notice preceded joined Close")
					}
				}}
				x := qaStartRunnerOptions(t, s, qaHWRunReader(), o)
				x.frame(t, "No activity")
				qaSetupRunPick(t, x, "guided")
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("Guided not admitted")
				}
				var sessionErr error
				switch mode {
				case "eof":
					sessionErr = &terminal.ExitError{Code: 0}
					s.endErrors <- sessionErr
				case "sigint":
					sessionErr = &terminal.ExitError{Code: 130}
					x.cancel(sessionErr)
				case "sigterm":
					sessionErr = &terminal.ExitError{Code: 143}
					x.cancel(sessionErr)
				}
				want := sessionErr
				if mode == "eof" {
					want = s.closeErr
				}
				if sharedErr != nil {
					want = sharedErr
				}
				x.finish(t, want)
				if reports != 1 || notices != 1 {
					t.Error("original Setup outcome or restoration diagnostic missing")
				}
			})
		}
	}
}

func TestQAUISetupUnknownBlocksFreshOtherFamilyMutationsButKeepsReads(t *testing.T) {
	unknown := &auth.Error{Code: "credential_write_unknown", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}
	var guided, mutations, reads atomic.Int32
	status := qaSetupRunPartial()
	status.Steps[0].Action, status.Steps[0].State = "auth.status", "input_required"
	o := ui.Options{Refresh: make(chan time.Time), Setup: &ui.SetupActions{Run: func(context.Context, setup.Input, terminal.Prompter) (setup.Status, error) {
		guided.Add(1)
		return status, unknown
	}}, Views: &ui.ReadViews{Links: func(context.Context) (activity.BindingList, error) { reads.Add(1); return qaUIReadonlyLinks(), nil }, Setup: func(context.Context) (setup.Status, error) {
		reads.Add(1)
		return setup.Status{ContractVersion: 1, Complete: false, Steps: []setup.Step{{Action: "auth.status", State: "input_required", SafeMessage: "READINESS-DOES-NOT-PROVE-NONAPPLICATION"}}}, nil
	}}, Auth: &ui.AuthActions{CanPersist: func() bool { return true }, PrepareLogin: func(context.Context, []byte) (*auth.LoginAttempt, error) {
		mutations.Add(1)
		return nil, errors.New("forbidden")
	}, Logout: func(context.Context, bool) (auth.Result, error) { mutations.Add(1); return auth.Result{}, nil }, UseAccount: func(context.Context, string) (auth.Result, error) { mutations.Add(1); return auth.Result{}, nil }, Accounts: func(context.Context) ([]harvest.Object, error) {
		reads.Add(1)
		return []harvest.Object{{"id": "11", "product": "harvest", "name": "Synthetic"}}, nil
	}, ConfigShow: func(context.Context, string) (auth.ConfigStatus, error) {
		reads.Add(1)
		return auth.ConfigStatus{Path: "/synthetic/account-config", SavedAccountID: "11", AccountID: "11"}, nil
	}, Status: func(context.Context, bool, string) (auth.Result, error) {
		reads.Add(1)
		return auth.Result{AccountID: "11"}, nil
	}}, Links: &ui.LinkActions{Prepare: func(context.Context, activity.LinkInput, terminal.Prompter) (activity.LinkInput, error) {
		mutations.Add(1)
		return activity.LinkInput{}, nil
	}, Commit: func(context.Context, activity.LinkInput) (activity.BindingResult, error) {
		mutations.Add(1)
		return activity.BindingResult{}, nil
	}, Repair: func(context.Context, activity.RepairBindingInput) (activity.BindingResult, error) {
		mutations.Add(1)
		return activity.BindingResult{}, nil
	}, Unlink: func(context.Context, activity.UnlinkInput) (activity.MutationResult, error) {
		mutations.Add(1)
		return activity.MutationResult{}, nil
	}}, Hooks: &ui.HookActions{Status: func(context.Context, hookstate.HookSelector) (hookstate.HookList, error) {
		reads.Add(1)
		return hookstate.HookList{}, nil
	}, PreviewInstall: func(_ context.Context, in hookstate.InstallIntent) (hookstate.HookPreview, error) {
		reads.Add(1)
		return hookstate.HookPreview{Intent: in, Fingerprint: strings.Repeat("a", 64)}, nil
	}, ApplyInstall: func(context.Context, hookstate.ApplyInstallInput) (hookstate.HookList, error) {
		mutations.Add(1)
		return hookstate.HookList{}, nil
	}, ConfirmInstalled: func(context.Context, hookstate.InstalledConfirmInput) (hookstate.HookList, error) {
		mutations.Add(1)
		return hookstate.HookList{}, nil
	}, Revoke: func(context.Context, hookstate.RevokeInput) (hookstate.Profile, error) {
		mutations.Add(1)
		return hookstate.Profile{}, nil
	}}}
	x := qaStartRunnerOptions(t, qaNewRunnerScreen(), qaHWRunReader(), o)
	x.frame(t, "No activity")
	qaSetupRunPick(t, x, "guided")
	qaLinkFrame(t, x, "Setup · Outcome unknown")
	qaLinkEnter(x)
	qaSetupChoice(t, x, "Setup · Outcome unknown", "back")
	x.frame(t, "No activity")
	qaLinkKey(x, "a")
	qaHWRunPick(t, x, "Accounts and auth", "login")
	qaLinkFrame(t, x, "unavailable")
	qaLinkEscape(x)
	x.frame(t, "No activity")
	for _, op := range []string{"accounts", "logout"} {
		qaLinkKey(x, "a")
		qaHWRunPick(t, x, "Accounts and auth", op)
		qaLinkFrame(t, x, "unavailable")
		qaLinkEscape(x)
		x.frame(t, "No activity")
	}
	qaLinkKey(x, "a")
	qaHWRunPick(t, x, "Accounts and auth", "status")
	qaLinkFrame(t, x, "Auth status")
	qaLinkEscape(x)
	x.frame(t, "No activity")
	qaLinkKey(x, "a")
	qaHWRunPick(t, x, "Accounts and auth", "config")
	qaLinkFrame(t, x, "Auth config")
	qaLinkEscape(x)
	x.frame(t, "No activity")
	qaLinkKey(x, "l")
	qaHWRunPick(t, x, "Links actions", "create")
	qaLinkFrame(t, x, "unavailable")
	qaLinkEscape(x)
	x.frame(t, "No activity")
	for _, op := range []string{"repair", "unlink"} {
		qaLinkKey(x, "l")
		qaHWRunPick(t, x, "Links actions", "22222222-2222-4222-8222-222222222222")
		qaHWRunPick(t, x, "Links · Binding actions", op)
		qaLinkFrame(t, x, "unavailable")
		qaLinkEscape(x)
		x.frame(t, "No activity")
	}
	qaLinkKey(x, "2")
	qaSetupChoice(t, x, "Links", "22222222-2222-4222-8222-222222222222")
	qaLinkFrame(t, x, "Links · Details")
	qaLinkEscape(x)
	x.frame(t, "No activity")
	qaHWRunHookDraft(t, x, "install")
	qaLinkFrame(t, x, "unavailable")
	qaLinkEscape(x)
	x.frame(t, "No activity")
	for _, op := range []string{"repair", "uninstall", "confirm-profile", "revoke-profile"} {
		qaHWRunHookDraft(t, x, op)
		qaLinkFrame(t, x, "unavailable")
		qaLinkEscape(x)
		x.frame(t, "No activity")
	}
	qaHWRunHookDraft(t, x, "status")
	qaLinkFrame(t, x, "Hooks · Read-only evidence")
	qaLinkEscape(x)
	x.frame(t, "No activity")
	qaHWRunHookDraft(t, x, "preview")
	qaHWRunPick(t, x, "Choose preview operation", "install")
	qaLinkFrame(t, x, "Hooks · Read-only preview")
	qaLinkEscape(x)
	x.frame(t, "No activity")
	qaLinkKey(x, ",")
	qaSetupChoice(t, x, "Setup · Outcome unknown", "readiness")
	qaLinkFrame(t, x, "READINESS-DOES-NOT-PROVE-NONAPPLICATION")
	qaLinkEnter(x)
	qaSetupChoice(t, x, "Setup · Outcome unknown", "back")
	x.frame(t, "No activity")
	qaLinkKey(x, "q")
	x.finish(t, unknown)
	if guided.Load() != 1 || mutations.Load() != 0 || reads.Load() < 3 {
		t.Errorf("unknown cleared/bypassed across families: guided%d mutation%d reads%d", guided.Load(), mutations.Load(), reads.Load())
	}
}

func TestQAUIExistingFamilyUnknownBlocksGuidedSetupPreservingExactIntent(t *testing.T) {
	for _, family := range []string{"auth", "links", "hooks"} {
		t.Run(family, func(t *testing.T) {
			var guided atomic.Int32
			var unknown error
			var id string
			o := ui.Options{Refresh: make(chan time.Time), Setup: &ui.SetupActions{Run: func(context.Context, setup.Input, terminal.Prompter) (setup.Status, error) {
				guided.Add(1)
				return qaSetupRunPartial(), nil
			}}, Views: &ui.ReadViews{Links: func(context.Context) (activity.BindingList, error) { return activity.BindingList{}, nil }, Setup: func(context.Context) (setup.Status, error) {
				return setup.Status{ContractVersion: 1, Steps: []setup.Step{{Action: "auth.status", State: "input_required", SafeMessage: "READ-ONLY-LOCAL"}}}, nil
			}}}
			switch family {
			case "auth":
				unknown = &auth.Error{Code: "credential_write_unknown", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}
				o.Auth = &ui.AuthActions{Logout: func(context.Context, bool) (auth.Result, error) { return auth.Result{}, unknown }}
			case "links":
				unknown = &activity.Error{Code: "local_write_unknown", Uncertain: true}
				o.Links = &ui.LinkActions{Prepare: func(_ context.Context, in activity.LinkInput, _ terminal.Prompter) (activity.LinkInput, error) {
					in.AccountID, in.ProjectID, in.TaskID, in.Timezone = "11", "100", "200", "UTC"
					return in, nil
				}, Commit: func(_ context.Context, in activity.LinkInput) (activity.BindingResult, error) {
					id = in.RequestID
					return activity.BindingResult{}, unknown
				}}
			case "hooks":
				unknown = &hookstate.Error{Code: "local_write_unknown", Uncertain: true}
				o.Hooks = &ui.HookActions{Status: func(context.Context, hookstate.HookSelector) (hookstate.HookList, error) {
					return hookstate.HookList{}, nil
				}, PreviewInstall: func(_ context.Context, in hookstate.InstallIntent) (hookstate.HookPreview, error) {
					return hookstate.HookPreview{Intent: in, Fingerprint: strings.Repeat("a", 64)}, nil
				}, ApplyInstall: func(_ context.Context, in hookstate.ApplyInstallInput) (hookstate.HookList, error) {
					id = in.RequestID
					return hookstate.HookList{}, unknown
				}}
			}
			reports := 0
			o.OnRetainedOutcome = func(name, requestID string, e error) {
				reports++
				if name != family || requestID != id || e != unknown {
					t.Error("blocked Setup replaced family/error/exact retained ID")
				}
			}
			x := qaStartRunnerOptions(t, qaNewRunnerScreen(), qaHWRunReader(), o)
			x.frame(t, "No activity")
			switch family {
			case "auth":
				qaLinkKey(x, "a")
				qaHWRunPick(t, x, "Accounts and auth", "logout")
				qaLinkFrame(t, x, "Remove the saved credential")
				qaLinkYes(x)
				qaLinkFrame(t, x, "Auth outcome unknown")
				qaLinkEnter(x)
				qaHWRunPick(t, x, "Auth outcome unknown · Inspect", "back")
			case "links":
				qaLinkKey(x, "l")
				qaHWRunPick(t, x, "Links actions", "create")
				qaLinkFrame(t, x, "Absolute project path")
				x.screen.events <- terminal.Event{Kind: "paste", Text: "/synthetic/link"}
				qaLinkEnter(x)
				qaLinkFrame(t, x, "Links · Outcome unknown")
				qaLinkEnter(x)
				qaHWRunPick(t, x, "Links · Recover submitted intent", "back")
			case "hooks":
				qaHWRunHookDraft(t, x, "install")
				qaLinkFrame(t, x, "Apply this exact reviewed")
				qaLinkYes(x)
				qaLinkFrame(t, x, "Hooks · Outcome unknown")
				qaLinkEnter(x)
				qaHWRunPick(t, x, "Recover pending request", "back")
			}
			x.frame(t, "No activity")
			qaSetupRunPick(t, x, "guided")
			qaLinkFrame(t, x, "unavailable")
			qaLinkEscape(x)
			x.frame(t, "No activity")
			qaSetupRunPick(t, x, "readiness")
			qaLinkFrame(t, x, "READ-ONLY-LOCAL")
			qaLinkEscape(x)
			x.frame(t, "No activity")
			qaLinkKey(x, "q")
			x.finish(t, unknown)
			if guided.Load() != 0 || reports != 1 {
				t.Error("pending family permitted new wizard or lost original report")
			}
		})
	}
}
