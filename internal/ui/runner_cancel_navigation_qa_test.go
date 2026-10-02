package ui_test

import (
	"context"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/ui"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestQAUIRunnerCanceledFlowRefreshCannotAdvertiseNavigationBeforeJoin(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseWorker := func() { once.Do(func() { close(release) }) }
	defer releaseWorker()
	unknown := &auth.Error{Code: "credential_write_unknown", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}
	s := qaNewRunnerScreen()
	r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("10", "100"), nil }}
	refresh := make(chan time.Time, 1)
	x := qaStartRunnerOptions(t, s, r, ui.Options{Refresh: refresh, Auth: &ui.AuthActions{Logout: func(ctx context.Context, _ bool) (auth.Result, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return auth.Result{}, unknown
	}}})
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
		t.Fatal("auth worker not entered")
	}
	qaLinkEscape(x)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Escape did not cancel worker")
	}
	before := r.calls.Load()
	refresh <- time.Now()
	deadline := time.Now().Add(time.Second)
	for r.calls.Load() == before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if r.calls.Load() == before {
		t.Fatal("authoritative refresh never ran while cancellation joins")
	}
	timer := time.NewTimer(80 * time.Millisecond)
	defer timer.Stop()
waiting:
	for {
		select {
		case f := <-s.frames:
			if len(f) > 0 && strings.HasPrefix(f[0], "TEMPO") {
				t.Fatalf("dashboard advertised navigation while flow worker still joining: %q", f)
			}
		case <-timer.C:
			break waiting
		}
	}
	releaseWorker()
	x.frame(t, "Project 100")
	qaLinkKey(x, "?")
	qaLinkFrame(t, x, "Help")
	qaLinkEscape(x)
	x.frame(t, "Project 100")
	qaLinkKey(x, "q")
	x.finish(t, unknown)
}
