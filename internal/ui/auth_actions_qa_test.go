package ui

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
)

type qaAuthFlow struct {
	p      *promptBridge
	done   chan error
	cancel context.CancelCauseFunc
}

func qaAuthStart(t *testing.T, c *authController, a *AuthActions) *qaAuthFlow {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	f := &qaAuthFlow{newPromptBridge(ctx), make(chan error, 1), cancel}
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
func (f *qaAuthFlow) next(t *testing.T, kind string) promptRequest {
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
func (f *qaAuthFlow) finish(t *testing.T) error {
	t.Helper()
	select {
	case e := <-f.done:
		return e
	case <-time.After(time.Second):
		t.Fatal("auth controller did not complete")
		return nil
	}
}
func qaAuthPick(t *testing.T, r promptRequest, id string) {
	t.Helper()
	for _, c := range r.choices {
		if c.ID == id {
			r.reply <- promptReply{choiceID: id}
			return
		}
	}
	t.Fatalf("choice %q absent: %#v", id, r.choices)
}
func qaAuthDeadline(t *testing.T, ctx context.Context, max time.Duration) {
	t.Helper()
	d, ok := ctx.Deadline()
	if !ok || time.Until(d) > max+time.Second {
		t.Error("shared callback missing bounded deadline")
	}
}
func qaAuthBase() *AuthActions { return &AuthActions{CanPersist: func() bool { return true }} }
func qaAuthView(t *testing.T, f *qaAuthFlow, needles ...string) {
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

type qaAuthProvider struct{ harvest.Provider }

func (qaAuthProvider) Accounts(context.Context) ([]harvest.Object, error) {
	return []harvest.Object{{"id": "11", "product": "harvest", "name": "Alpha"}, {"id": "22", "product": "harvest", "name": "Beta"}}, nil
}
func qaAuthAttempt(t *testing.T) (*auth.Service, *auth.LoginAttempt) {
	t.Helper()
	s := auth.NewService(auth.Options{ConfigPath: t.TempDir() + "/config", LockPath: t.TempDir() + "/lock", Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Error("closed attempt dispatched credential helper")
		return auth.NativeReply{}, errors.New("forbidden")
	}), NewProvider: func(string, string) harvest.Provider { return qaAuthProvider{} }})
	a, e := s.PrepareLogin(context.Background(), []byte("synthetic-secret"))
	if e != nil {
		t.Fatal(e)
	}
	return s, a
}
func qaAuthClosed(t *testing.T, s *auth.Service, a *auth.LoginAttempt) {
	t.Helper()
	_, e := s.CommitLogin(context.Background(), a, "11")
	var ae *auth.Error
	if !errors.As(e, &ae) || ae.Code != "validation" {
		t.Errorf("login attempt was not closed: %v", e)
	}
}

func TestQAAuthControllerLazyMenuAndBack(t *testing.T) {
	a := qaAuthBase()
	a.CanPersist = func() bool { t.Error("menu probed persistence"); return true }
	c := &authController{}
	f := qaAuthStart(t, c, a)
	r := f.next(t, "choose")
	if r.title != "Accounts and auth" {
		t.Errorf("menu title=%q", r.title)
	}
	for _, id := range []string{"status", "check", "config", "accounts", "login", "logout", "back"} {
		found := false
		for _, v := range r.choices {
			found = found || v.ID == id
		}
		if !found {
			t.Errorf("missing %s", id)
		}
	}
	qaAuthPick(t, r, "back")
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
}
func TestQAAuthControllerReadViewsBoundedAndSafe(t *testing.T) {
	for _, mode := range []string{"status", "check", "config"} {
		t.Run(mode, func(t *testing.T) {
			a := qaAuthBase()
			calls := 0
			a.Status = func(ctx context.Context, check bool, explicit string) (auth.Result, error) {
				calls++
				if check != (mode == "check") || explicit != "" {
					t.Errorf("wrong status arguments")
				}
				limit := 250 * time.Millisecond
				if check {
					limit = 2 * time.Minute
				}
				qaAuthDeadline(t, ctx, limit)
				return auth.Result{AccountID: "22", Source: "environment", EnvironmentOverride: true, Effects: auth.Effects{Credential: "unchanged", Config: "saved"}}, nil
			}
			a.ConfigShow = func(ctx context.Context, explicit string) (auth.ConfigStatus, error) {
				calls++
				qaAuthDeadline(t, ctx, 250*time.Millisecond)
				if explicit != "" {
					t.Error("invented account override")
				}
				return auth.ConfigStatus{Path: "/synthetic/config", SavedAccountID: "11", AccountID: "22"}, nil
			}
			f := qaAuthStart(t, &authController{}, a)
			qaAuthPick(t, f.next(t, "choose"), mode)
			if mode == "config" {
				qaAuthView(t, f, "/synthetic/config", "11", "22")
			} else {
				qaAuthView(t, f, "22", "environment", "unchanged", "saved")
			}
			if e := f.finish(t); e != nil {
				t.Fatal(e)
			}
			if calls != 1 {
				t.Errorf("read calls=%d", calls)
			}
		})
	}
}
func TestQAAuthControllerAccountConsentExactTarget(t *testing.T) {
	for _, yes := range []bool{false, true} {
		t.Run(fmt.Sprint(yes), func(t *testing.T) {
			a := qaAuthBase()
			commits := 0
			objects := []harvest.Object{{"id": "11", "name": "Same", "product": "harvest"}, {"id": "22", "name": "Same", "product": "harvest"}}
			a.Accounts = func(ctx context.Context) ([]harvest.Object, error) {
				qaAuthDeadline(t, ctx, 2*time.Minute)
				return objects, nil
			}
			a.UseAccount = func(ctx context.Context, id string) (auth.Result, error) {
				commits++
				qaAuthDeadline(t, ctx, 2*time.Minute)
				if id != "22" {
					t.Errorf("wrong committed ID %q", id)
				}
				return auth.Result{AccountID: id, Effects: auth.Effects{Credential: "unchanged", Config: "saved"}}, nil
			}
			f := qaAuthStart(t, &authController{}, a)
			qaAuthPick(t, f.next(t, "choose"), "accounts")
			r := f.next(t, "choose")
			qaAuthPick(t, r, "22")
			confirm := f.next(t, "confirm")
			if !strings.Contains(confirm.title, "22") {
				t.Error("confirmation hides exact account ID")
			}
			objects[1]["id"] = "99"
			confirm.reply <- promptReply{confirmed: yes}
			if yes {
				qaAuthView(t, f, "22", "saved")
			}
			if e := f.finish(t); e != nil {
				t.Fatal(e)
			}
			want := 0
			if yes {
				want = 1
			}
			if commits != want {
				t.Errorf("commits=%d want %d", commits, want)
			}
		})
	}
}
func TestQAAuthControllerLoginSecretLifecycleAndConsent(t *testing.T) {
	for _, stage := range []string{"choose-cancel", "decline", "commit"} {
		t.Run(stage, func(t *testing.T) {
			s, attempt := qaAuthAttempt(t)
			a := qaAuthBase()
			var private []byte
			commits := 0
			a.PrepareLogin = func(ctx context.Context, b []byte) (*auth.LoginAttempt, error) {
				qaAuthDeadline(t, ctx, 2*time.Minute)
				if string(b) != "synthetic-secret" {
					t.Error("private token changed")
				}
				private = b
				return attempt, nil
			}
			a.CommitLogin = func(ctx context.Context, got *auth.LoginAttempt, id string) (auth.Result, error) {
				commits++
				qaAuthDeadline(t, ctx, 2*time.Minute)
				if got != attempt || id != "22" {
					t.Error("prepared attempt/selected account changed")
				}
				return auth.Result{AccountID: id, Authenticated: true, Effects: auth.Effects{Credential: "applied", Config: "saved"}}, nil
			}
			f := qaAuthStart(t, &authController{}, a)
			qaAuthPick(t, f.next(t, "choose"), "login")
			secret := f.next(t, "secret")
			secret.reply <- promptReply{secret: []byte("synthetic-secret")}
			r := f.next(t, "choose")
			if len(private) == 0 {
				t.Error("preparation skipped")
			}
			for _, b := range private {
				if b != 0 {
					t.Error("input token retained across account prompt")
					break
				}
			}
			if strings.Contains(r.title, "synthetic-secret") {
				t.Error("secret leaked in modal")
			}
			if stage == "choose-cancel" {
				r.reply <- promptReply{err: &terminal.ExitError{Code: 0}}
				var e *terminal.ExitError
				if !errors.As(f.finish(t), &e) {
					t.Error("prompt cancellation lost")
				}
			} else {
				qaAuthPick(t, r, "22")
				confirm := f.next(t, "confirm")
				for _, needle := range []string{"22", "credential", "config"} {
					if !strings.Contains(strings.ToLower(confirm.title), needle) {
						t.Errorf("login warning hides %s", needle)
					}
				}
				confirm.reply <- promptReply{confirmed: stage == "commit"}
				if stage == "commit" {
					qaAuthView(t, f, "22", "applied", "saved")
				}
				if e := f.finish(t); e != nil {
					t.Fatal(e)
				}
			}
			qaAuthClosed(t, s, attempt)
			want := 0
			if stage == "commit" {
				want = 1
			}
			if commits != want {
				t.Errorf("commits=%d", commits)
			}
		})
	}
}
func TestQAAuthControllerLoginUnavailableNeverRequestsSecret(t *testing.T) {
	a := qaAuthBase()
	a.CanPersist = func() bool { return false }
	a.PrepareLogin = func(context.Context, []byte) (*auth.LoginAttempt, error) {
		t.Error("unsupported login prepared")
		return nil, nil
	}
	f := qaAuthStart(t, &authController{}, a)
	qaAuthPick(t, f.next(t, "choose"), "login")
	qaAuthView(t, f)
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
}
func TestQAAuthControllerLogoutExplicitAndEnvironmentWarning(t *testing.T) {
	for _, yes := range []bool{false, true} {
		t.Run(fmt.Sprint(yes), func(t *testing.T) {
			a := qaAuthBase()
			calls := 0
			a.Logout = func(ctx context.Context, confirmed bool) (auth.Result, error) {
				calls++
				qaAuthDeadline(t, ctx, 2*time.Minute)
				if !confirmed {
					t.Error("shared Logout lacks consent")
				}
				return auth.Result{LoggedOut: true, EnvironmentTokenPresent: true, Effects: auth.Effects{Credential: "applied", Config: "cleared"}}, nil
			}
			f := qaAuthStart(t, &authController{}, a)
			qaAuthPick(t, f.next(t, "choose"), "logout")
			r := f.next(t, "confirm")
			for _, n := range []string{"environment", "credential", "config"} {
				if !strings.Contains(strings.ToLower(r.title), n) {
					t.Errorf("logout warning omits %s", n)
				}
			}
			r.reply <- promptReply{confirmed: yes}
			if yes {
				qaAuthView(t, f, "applied", "cleared")
			}
			if e := f.finish(t); e != nil {
				t.Fatal(e)
			}
			want := 0
			if yes {
				want = 1
			}
			if calls != want {
				t.Errorf("logout writes=%d", calls)
			}
		})
	}
}
func TestQAAuthControllerUnknownRetainsEffectsNoReplayOrReplacement(t *testing.T) {
	a := qaAuthBase()
	unknown := &auth.Error{Code: "credential_write_unknown", Message: "RAW-SYNTHETIC-SECRET", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "saved"}}
	writes, reads := 0, 0
	a.Logout = func(context.Context, bool) (auth.Result, error) { writes++; return auth.Result{}, unknown }
	a.Status = func(ctx context.Context, check bool, _ string) (auth.Result, error) {
		reads++
		return auth.Result{AccountID: "22", Effects: auth.Effects{Credential: "unchanged", Config: "unchanged"}}, nil
	}
	a.ConfigShow = func(context.Context, string) (auth.ConfigStatus, error) {
		reads++
		return auth.ConfigStatus{Path: "/synthetic/config", AccountID: "22"}, nil
	}
	c := &authController{}
	f := qaAuthStart(t, c, a)
	qaAuthPick(t, f.next(t, "choose"), "logout")
	f.next(t, "confirm").reply <- promptReply{confirmed: true}
	r := f.next(t, "view")
	body := r.title + " " + r.body
	for _, n := range []string{"unknown", "saved"} {
		if !strings.Contains(body, n) {
			t.Errorf("unknown effects hidden: %s", body)
		}
	}
	if strings.Contains(body, unknown.Message) {
		t.Error("raw error leaked")
	}
	r.reply <- promptReply{}
	for _, mode := range []string{"status", "check", "config", "back"} {
		r = f.next(t, "choose")
		for _, v := range r.choices {
			if v.ID != "status" && v.ID != "check" && v.ID != "config" && v.ID != "back" {
				t.Errorf("unsafe recovery action %s", v.ID)
			}
		}
		qaAuthPick(t, r, mode)
		if mode != "back" {
			qaAuthView(t, f)
		}
	}
	if e := f.finish(t); e != unknown {
		t.Errorf("typed unknown lost: %#v", e)
	}
	if c.pending != unknown {
		t.Error("inspection cleared pending unknown")
	}
	if writes != 1 || reads != 3 {
		t.Errorf("writes=%d reads=%d", writes, reads)
	}
	reopened := qaAuthStart(t, c, a)
	menu := reopened.next(t, "choose")
	qaAuthPick(t, menu, "back")
	if e := reopened.finish(t); e != unknown {
		t.Error("reopening erased earlier uncertain outcome")
	}
}
func TestQAAuthControllerInFlightCancellationReturnsSharedUnknown(t *testing.T) {
	a := qaAuthBase()
	unknown := &auth.Error{Code: "credential_write_unknown", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}
	entered := make(chan struct{})
	a.Logout = func(ctx context.Context, _ bool) (auth.Result, error) {
		close(entered)
		<-ctx.Done()
		return auth.Result{}, unknown
	}
	c := &authController{}
	f := qaAuthStart(t, c, a)
	qaAuthPick(t, f.next(t, "choose"), "logout")
	f.next(t, "confirm").reply <- promptReply{confirmed: true}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("mutation not dispatched")
	}
	f.cancel(&terminal.ExitError{Code: 143})
	if e := f.finish(t); e != unknown {
		t.Errorf("cancellation masked shared unknown: %#v", e)
	}
	if c.pending != unknown {
		t.Error("in-flight outcome not retained")
	}
}

func TestQAAuthControllerAccountChoicesFilterAndDoNotLeakObjectFields(t *testing.T) {
	a := qaAuthBase()
	a.Accounts = func(context.Context) ([]harvest.Object, error) {
		return []harvest.Object{{"id": "11", "product": "harvest", "name": "Alpha", "token": "RAW-OBJECT-SECRET"}, {"id": "22", "product": "other", "name": "Wrong"}, {"id": "oops", "product": "harvest", "name": "Invalid"}}, nil
	}
	f := qaAuthStart(t, &authController{}, a)
	qaAuthPick(t, f.next(t, "choose"), "accounts")
	r := f.next(t, "choose")
	if len(r.choices) != 1 || r.choices[0].ID != "11" {
		t.Errorf("invalid/inaccessible account admitted: %#v", r.choices)
	}
	if strings.Contains(fmt.Sprint(r.choices), "RAW-OBJECT-SECRET") {
		t.Error("arbitrary account field leaked")
	}
	r.reply <- promptReply{err: &terminal.ExitError{Code: 0}}
	f.finish(t)
}
func TestQAAuthControllerPrepareFailureWipesTokenAndSanitizesError(t *testing.T) {
	a := qaAuthBase()
	var input []byte
	a.PrepareLogin = func(_ context.Context, b []byte) (*auth.LoginAttempt, error) {
		input = b
		return nil, errors.New("RAW-TOKEN-TRANSPORT-SECRET")
	}
	f := qaAuthStart(t, &authController{}, a)
	qaAuthPick(t, f.next(t, "choose"), "login")
	f.next(t, "secret").reply <- promptReply{secret: []byte("synthetic-secret")}
	r := f.next(t, "view")
	for _, b := range input {
		if b != 0 {
			t.Error("failed Prepare retained private input")
			break
		}
	}
	if strings.Contains(r.title+r.body, "RAW-TOKEN-TRANSPORT-SECRET") {
		t.Error("raw provider error leaked")
	}
	r.reply <- promptReply{}
	f.finish(t)
}
func TestQAAuthControllerCanceledAccountPromptClosesAttempt(t *testing.T) {
	s, attempt := qaAuthAttempt(t)
	a := qaAuthBase()
	a.PrepareLogin = func(context.Context, []byte) (*auth.LoginAttempt, error) { return attempt, nil }
	f := qaAuthStart(t, &authController{}, a)
	qaAuthPick(t, f.next(t, "choose"), "login")
	f.next(t, "secret").reply <- promptReply{secret: []byte("synthetic-secret")}
	f.next(t, "choose")
	cause := &terminal.ExitError{Code: 130}
	f.cancel(cause)
	if e := f.finish(t); e != cause {
		t.Errorf("pre-dispatch cancellation changed: %v", e)
	}
	qaAuthClosed(t, s, attempt)
}
func TestQAAuthControllerRecoveryReadFailureAndCancelRetainOriginalEffects(t *testing.T) {
	unknown := &auth.Error{Code: "credential_write_unknown", Uncertain: true, Effects: auth.Effects{Credential: "applied", Config: "unknown"}}
	c := &authController{pending: unknown}
	a := qaAuthBase()
	a.Status = func(context.Context, bool, string) (auth.Result, error) {
		return auth.Result{}, errors.New("RAW-RECOVERY-SECRET")
	}
	f := qaAuthStart(t, c, a)
	qaAuthPick(t, f.next(t, "choose"), "status")
	r := f.next(t, "view")
	if strings.Contains(r.title+r.body, "RAW-RECOVERY-SECRET") {
		t.Error("read failure leaked")
	}
	r.reply <- promptReply{}
	f.next(t, "choose")
	f.cancel(&terminal.ExitError{Code: 143})
	if e := f.finish(t); e != unknown || c.pending != unknown {
		t.Errorf("read failure/cancellation lost prior effects: %v", e)
	}
}
