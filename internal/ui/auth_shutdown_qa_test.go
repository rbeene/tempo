package ui_test

import (
	"context"
	"errors"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestQAUIAuthJoinedShutdownReportsAllRetainedFamiliesAfterRestoration(t *testing.T) {
	local := &activity.Error{Code: "local_write_unknown", Uncertain: true, Details: map[string]any{"request_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}}
	credential := &auth.Error{Code: "credential_write_unknown", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "saved"}}
	var active atomic.Int32
	entered := make(chan struct{})
	s := qaNewRunnerScreen()
	s.closeErr = errors.New("RAW-RESTORATION-SECRET")
	s.closeCheck = func() bool { return active.Load() != 0 }
	r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("10", "100"), nil }}
	reports := []string{}
	restored := 0
	actions := &ui.AuthActions{Logout: func(ctx context.Context, yes bool) (auth.Result, error) {
		active.Add(1)
		defer active.Add(-1)
		close(entered)
		<-ctx.Done()
		return auth.Result{}, credential
	}}
	x := qaStartRunnerOptions(t, s, r, ui.Options{Refresh: make(chan time.Time), Views: &ui.ReadViews{Links: func(context.Context) (activity.BindingList, error) { return qaUIReadonlyLinks(), nil }}, Links: &ui.LinkActions{Unlink: func(_ context.Context, in activity.UnlinkInput) (activity.MutationResult, error) {
		local.Details["request_id"] = in.RequestID
		return activity.MutationResult{}, local
	}}, Auth: actions, OnRetainedOutcome: func(family, requestID string, e error) {
		if s.closes.Load() != 1 || active.Load() != 0 {
			t.Error("retained outcome emitted before joined cleanup")
		}
		reports = append(reports, family)
		if family == "auth" && requestID != "" || family == "links" && requestID != local.Details["request_id"] {
			t.Error("callback lost authoritative request identity")
		}
		if family == "auth" && e != credential || family == "links" && e != local {
			t.Error("shared typed outcome changed")
		}
	}, OnAuthResult: func(string, auth.Result, error) { t.Error("unknown invented completed result") }, OnRestorationFailure: func() {
		if s.closes.Load() != 1 || active.Load() != 0 {
			t.Error("restoration notice preceded join/Close")
		}
		restored++
	}})
	x.frame(t, "Project 100")
	qaLinkSelectBindingAction(t, x, "unlink")
	qaLinkFrame(t, x, "history")
	qaLinkYes(x)
	qaLinkFrame(t, x, "Links · Outcome unknown")
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Recover submitted intent")
	qaLinkKey(x, "back")
	qaLinkEnter(x)
	x.frame(t, "Project 100")
	qaLinkKey(x, "a")
	qaLinkFrame(t, x, "Accounts and auth")
	qaLinkKey(x, "logout")
	qaLinkEnter(x)
	qaLinkFrame(t, x, "Remove the saved credential")
	qaLinkYes(x)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("auth not admitted")
	}
	x.cancel(&terminal.ExitError{Code: 143})
	x.finish(t, credential)
	if !reflect.DeepEqual(reports, []string{"auth", "links"}) || restored != 1 || s.closeBeforeJoin {
		t.Errorf("missing ordered shared outcomes/restoration notice: %v notices%d", reports, restored)
	}
}
func TestQAUIRestorationNoticeAfterCloseAndNeverOnSuccessfulClose(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			s := qaNewRunnerScreen()
			var want error
			if fail {
				want = errors.New("RAW-RESTORATION-SECRET")
				s.closeErr = want
			}
			r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("1"), nil }}
			notices := 0
			x := qaStartRunnerOptions(t, s, r, ui.Options{Refresh: make(chan time.Time), OnRestorationFailure: func() {
				notices++
				if s.closes.Load() != 1 || s.nextActive.Load() != 0 || r.active.Load() != 0 {
					t.Error("restoration notification before cleanup")
				}
			}})
			x.frame(t, "No activity")
			qaLinkKey(x, "q")
			x.finish(t, want)
			expected := 0
			if fail {
				expected = 1
			}
			if notices != expected {
				t.Errorf("notices%d want%d", notices, expected)
			}
		})
	}
}
