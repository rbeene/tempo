package setup

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestQAFiniteReadinessAndDoctorAreReadOnly(t *testing.T) {
	for _, op := range []string{"setup", "doctor"} {
		t.Run(op, func(t *testing.T) {
			dir := t.TempDir()
			a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(string) string { return "" }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
				t.Fatal("local readiness read credentials")
				return auth.NativeReply{}, nil
			}), NewProvider: func(string, string) harvest.Provider { t.Fatal("local readiness constructed provider"); return nil }, PersistentAvailable: func() bool { return true }})
			s := New(Options{Auth: a, Activity: activity.New(activity.Options{Path: filepath.Join(dir, "activity", "state")})})
			if op == "setup" {
				r, e := s.Run(context.Background(), Input{Path: dir}, nil)
				if e != nil {
					t.Fatal(e)
				}
				if r.ContractVersion != 1 || r.Complete || len(r.Steps) == 0 {
					t.Fatalf("dishonest readiness %+v", r)
				}
			} else {
				r, e := s.Doctor(context.Background(), false)
				if e != nil {
					t.Fatal(e)
				}
				if r.ContractVersion != 1 || len(r.Items) == 0 {
					t.Fatalf("empty diagnostics %+v", r)
				}
			}
			entries, e := os.ReadDir(dir)
			if e != nil {
				t.Fatal(e)
			}
			if len(entries) != 0 {
				t.Fatalf("readiness wrote files %v", entries)
			}
		})
	}
}

type qaSetupProvider struct {
	harvest.Provider
	fail bool
}

func (p qaSetupProvider) Accounts(context.Context) ([]harvest.Object, error) {
	return []harvest.Object{{"id": "11", "product": "harvest", "name": "Account"}}, nil
}
func (p qaSetupProvider) Get(context.Context, string) (harvest.Object, error) {
	return harvest.Object{"id": "2", "is_active": true}, nil
}
func (p qaSetupProvider) List(context.Context, string, url.Values) ([]harvest.Object, error) {
	if p.fail {
		return nil, &harvest.Error{Code: "network", Message: "safe synthetic failure"}
	}
	return []harvest.Object{{"is_active": true, "project": harvest.Object{"id": "3", "name": "Project\x1b[31m"}, "client": harvest.Object{"id": "8", "name": "Client"}, "task_assignments": []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": "4", "name": "Work"}}, harvest.Object{"is_active": true, "task": harvest.Object{"id": "5", "name": "Review"}}}}}, nil
}

type qaSetupPrompt struct {
	t            *testing.T
	choices      []string
	confirm      bool
	confirmCalls int
	chooseCalls  int
	textCalls    int
	cancelAt     string
}

func (p *qaSetupPrompt) Choose(_ context.Context, label string, choices []terminal.Choice) (string, error) {
	p.chooseCalls++
	if p.cancelAt == "choose" {
		return "", &terminal.ExitError{Code: 0}
	}
	for _, c := range choices {
		if strings.ContainsAny(c.Label, "\x1b\r\n") {
			p.t.Fatal("unsanitized picker label")
		}
	}
	if len(p.choices) == 0 {
		p.t.Fatalf("unexpected picker %s %+v", label, choices)
	}
	choice := p.choices[0]
	p.choices = p.choices[1:]
	found := false
	for _, c := range choices {
		if c.ID == choice {
			found = true
		}
	}
	if !found {
		p.t.Fatalf("requested choice %s absent from %+v", choice, choices)
	}
	return choice, nil
}
func (p *qaSetupPrompt) Text(context.Context, string, string) (string, error) {
	p.textCalls++
	if p.cancelAt == "text" {
		return "", &terminal.ExitError{Code: 0}
	}
	return "UTC", nil
}
func (p *qaSetupPrompt) Secret(context.Context, string) ([]byte, error) {
	p.t.Fatal("provisioned link requested secret")
	return nil, nil
}
func (p *qaSetupPrompt) Confirm(context.Context, string) (bool, error) {
	p.confirmCalls++
	return p.confirm, nil
}
func qaGuidedLink(t *testing.T) (*Service, string, activity.LinkInput) {
	t.Helper()
	dir := t.TempDir()
	state := filepath.Join(dir, "activity", "state")
	a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "lock"), Getenv: func(k string) string {
		if k == "HARVEST_TOKEN" {
			return "synthetic-qa-secret"
		}
		if k == "HARVEST_ACCOUNT_ID" {
			return "11"
		}
		return ""
	}, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Fatal("environment guided link invoked native helper")
		return auth.NativeReply{}, nil
	}), NewProvider: func(_ string, account string) harvest.Provider {
		if account != "11" && account != "" {
			t.Fatalf("cross-account provider %s", account)
		}
		return qaSetupProvider{}
	}})
	return New(Options{Auth: a, Activity: activity.New(activity.Options{Path: state})}), state, activity.LinkInput{Path: dir, AccountID: "11", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}
}
func TestQAGuidedLinkChoosesProjectTaskTimezoneAndConfirms(t *testing.T) {
	s, _, in := qaGuidedLink(t)
	p := &qaSetupPrompt{t: t, choices: []string{"3", "5"}, confirm: true}
	r, e := s.Link(context.Background(), in, p)
	if e != nil {
		t.Fatal(e)
	}
	if r.Binding.Attribution != (activity.Attribution{AccountID: "11", UserID: "2", ProjectID: "3", TaskID: "5", Timezone: "UTC"}) || !r.Changed || p.confirmCalls != 1 || len(p.choices) != 0 {
		t.Fatalf("bad guided binding %+v prompt=%+v", r, p)
	}
}
func TestQAGuidedLinkCancellationNeverInitializesState(t *testing.T) {
	for _, stage := range []string{"choose", "text", "confirm"} {
		t.Run(stage, func(t *testing.T) {
			s, state, in := qaGuidedLink(t)
			p := &qaSetupPrompt{t: t, choices: []string{"3", "5"}, cancelAt: stage}
			_, e := s.Link(context.Background(), in, p)
			if stage == "choose" && p.chooseCalls == 0 || stage == "text" && p.textCalls == 0 || stage == "confirm" && p.confirmCalls == 0 {
				t.Fatal("cancellation stage never reached")
			}
			if e == nil {
				t.Fatal("canceled link succeeded")
			}
			if _, e = os.Stat(state); !os.IsNotExist(e) {
				t.Fatal("preconfirmation cancel created activity state")
			}
		})
	}
}
func TestQAExplicitLinkBypassesAllPickersAndReplayStaysOffline(t *testing.T) {
	s, state, in := qaGuidedLink(t)
	in.ProjectID = "3"
	in.TaskID = "4"
	in.Timezone = "UTC"
	r, e := s.Link(context.Background(), in, nil)
	if e != nil {
		t.Fatal(e)
	}
	forbidden := auth.NewService(auth.Options{ConfigPath: filepath.Join(t.TempDir(), "cfg"), LockPath: filepath.Join(t.TempDir(), "lock"), Getenv: func(string) string { t.Fatal("replay resolved auth"); return "" }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Fatal("replay accessed credentials")
		return auth.NativeReply{}, nil
	})})
	replay, e := New(Options{Auth: forbidden, Activity: activity.New(activity.Options{Path: state})}).Link(context.Background(), in, nil)
	if e != nil || !reflect.DeepEqual(r, replay) {
		t.Fatalf("offline replay changed %+v %v", replay, e)
	}
}

type qaWizardPrompt struct {
	t                                      *testing.T
	cancelStage                            string
	secretCalls, confirmCalls, chooseCalls int
}

func (p *qaWizardPrompt) Secret(context.Context, string) ([]byte, error) {
	p.secretCalls++
	if p.cancelStage == "secret" {
		return nil, &terminal.ExitError{Code: 0}
	}
	return []byte("synthetic-qa-secret"), nil
}
func (p *qaWizardPrompt) Choose(_ context.Context, _ string, c []terminal.Choice) (string, error) {
	p.chooseCalls++
	if p.cancelStage == "account" {
		return "", &terminal.ExitError{Code: 0}
	}
	for _, v := range c {
		if v.ID == "11" {
			return "11", nil
		}
	}
	p.t.Fatal("unexpected wizard picker")
	return "", nil
}
func (p *qaWizardPrompt) Text(context.Context, string, string) (string, error) {
	p.t.Fatal("explicit path caused extra input")
	return "", nil
}
func (p *qaWizardPrompt) Confirm(context.Context, string) (bool, error) {
	p.confirmCalls++
	return p.cancelStage != "confirm", nil
}

type qaWizardProvider struct{ qaSetupProvider }

func (p qaWizardProvider) Accounts(context.Context) ([]harvest.Object, error) {
	return []harvest.Object{{"id": "11", "product": "harvest"}, {"id": "22", "product": "harvest"}}, nil
}
func TestQAWizardCredentialConfirmationAndPartialCompletion(t *testing.T) {
	for _, stage := range []string{"secret", "account", "confirm", "link-failure"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			commits := 0
			tokenStored := false
			path := filepath.Join(dir, "cfg")
			a := auth.NewService(auth.Options{ConfigPath: path, LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, NewProvider: func(string, string) harvest.Provider { return qaWizardProvider{qaSetupProvider{fail: true}} }, Runner: auth.RunnerFunc(func(_ context.Context, r auth.NativeRequest, l *os.File) (auth.NativeReply, error) {
				switch r.Operation {
				case "read":
					if tokenStored {
						return auth.NativeReply{Token: []byte("synthetic-qa-secret"), Effects: auth.Effects{Credential: "unchanged", Config: "unchanged"}}, nil
					}
					return auth.NativeReply{Code: "not_found", Effects: auth.Effects{Credential: "unchanged", Config: "unchanged"}}, nil
				case "login":
					commits++
					if l == nil || r.AccountID != "11" {
						t.Fatal("wizard did not use shared validated commit")
					}
					tokenStored = true
					if e := auth.Save(path, auth.Config{Account: "11"}); e != nil {
						t.Fatal(e)
					}
					return auth.NativeReply{Effects: auth.Effects{Credential: "applied", Config: "saved"}}, nil
				default:
					t.Fatalf("unexpected wizard operation %s", r.Operation)
					return auth.NativeReply{}, nil
				}
			})})
			p := &qaWizardPrompt{t: t, cancelStage: stage}
			s := New(Options{Auth: a, Activity: activity.New(activity.Options{Path: filepath.Join(dir, "activity", "state")})})
			status, e := s.Run(context.Background(), Input{Path: dir}, p)
			if p.secretCalls != 1 {
				t.Fatal("wizard did not reach secure input")
			}
			if stage == "link-failure" {
				if commits != 1 || p.confirmCalls != 1 || e == nil {
					t.Fatalf("separate confirmed auth step lost commits=%d confirms=%d err=%v", commits, p.confirmCalls, e)
				}
				complete := false
				for _, step := range status.Steps {
					if step.Action == "auth.login" && step.State == "complete" {
						complete = true
					}
				}
				if !complete || status.Complete {
					t.Fatalf("partial auth completion not reported %+v", status)
				}
				cfg, e := auth.Load(path)
				if e != nil || cfg.Account != "11" {
					t.Fatalf("later link error undid auth %+v %v", cfg, e)
				}
			} else {
				if e == nil {
					t.Fatal("canceled wizard succeeded")
				}
				var ee *terminal.ExitError
				if !errors.As(e, &ee) || ee.Code != 0 {
					t.Fatalf("unexpected cancellation error %v", e)
				}
				if commits != 0 {
					t.Fatal("preconfirmation wizard changed credential")
				}
				if _, e := os.Stat(path); !os.IsNotExist(e) {
					t.Fatal("preconfirmation wizard changed config")
				}
				if stage == "account" && p.chooseCalls != 1 || stage == "confirm" && p.confirmCalls != 1 {
					t.Fatal("cancellation stage never reached")
				}
			}
			raw, _ := json.Marshal(status)
			if strings.Contains(string(raw), "synthetic-qa-secret") {
				t.Fatal("wizard model leaked secret")
			}
		})
	}
}
func TestQAWizardUnsupportedPersistenceBeforeSecret(t *testing.T) {
	dir := t.TempDir()
	a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return false }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Fatal("unsupported backend accessed")
		return auth.NativeReply{}, nil
	})})
	p := &qaWizardPrompt{t: t}
	status, e := New(Options{Auth: a, Activity: activity.New(activity.Options{Path: filepath.Join(dir, "activity", "state")})}).Run(context.Background(), Input{Path: dir}, p)
	if p.secretCalls != 0 || p.confirmCalls != 0 {
		t.Fatal("unsupported platform collected secret")
	}
	raw, _ := json.Marshal(status)
	text := string(raw)
	if e != nil {
		text += " " + e.Error()
	}
	if !strings.Contains(text, "HARVEST_TOKEN") {
		t.Fatalf("unsupported workflow lacks environment guidance %s", text)
	}
}

type qaChoiceProvider struct {
	qaSetupProvider
	sole bool
}

func (p qaChoiceProvider) Accounts(context.Context) ([]harvest.Object, error) {
	return []harvest.Object{{"id": "11", "product": "harvest"}, {"id": "22", "product": "harvest"}}, nil
}
func (p qaChoiceProvider) List(c context.Context, s string, v url.Values) ([]harvest.Object, error) {
	rows, e := p.qaSetupProvider.List(c, s, v)
	if p.sole {
		tasks := rows[0]["task_assignments"].([]any)
		rows[0]["task_assignments"] = tasks[:1]
	}
	return rows, e
}
func TestQAGuidedExplicitProjectAccountAndTaskResolution(t *testing.T) {
	for _, sole := range []bool{false, true} {
		t.Run(map[bool]string{false: "ambiguous-task", true: "sole-task"}[sole], func(t *testing.T) {
			dir := t.TempDir()
			a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(k string) string {
				if k == "HARVEST_TOKEN" {
					return "synthetic-qa-secret"
				}
				return ""
			}, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
				t.Fatal("environment choice invoked credential helper")
				return auth.NativeReply{}, nil
			}), NewProvider: func(_ string, account string) harvest.Provider {
				if account != "" && account != "22" {
					t.Fatalf("selected account leaked into other provider %s", account)
				}
				return qaChoiceProvider{sole: sole}
			}})
			s := New(Options{Auth: a, Activity: activity.New(activity.Options{Path: filepath.Join(dir, "activity", "state")})})
			choices := []string{"22"}
			wantTask := "4"
			if !sole {
				choices = append(choices, "5")
				wantTask = "5"
			}
			p := &qaSetupPrompt{t: t, choices: choices, confirm: true}
			r, e := s.Link(context.Background(), activity.LinkInput{ProjectID: "3", Timezone: "UTC", Path: dir, RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, p)
			if e != nil {
				t.Fatal(e)
			}
			if r.Binding.Attribution.AccountID != "22" || r.Binding.Attribution.TaskID != wantTask || len(p.choices) != 0 {
				t.Fatalf("wrong selected binding %+v", r)
			}
			if p.chooseCalls != len(choices) {
				t.Fatalf("explicit project or sole task prompted unexpectedly: %d", p.chooseCalls)
			}
			if _, e := os.Stat(filepath.Join(dir, "cfg")); !os.IsNotExist(e) {
				t.Fatal("link account choice implicitly persisted default account")
			}
		})
	}
}
func TestQAFiniteReadinessRecognizesExistingLocalBinding(t *testing.T) {
	s, _, in := qaGuidedLink(t)
	in.ProjectID = "3"
	in.TaskID = "4"
	in.Timezone = "UTC"
	if _, e := s.Link(context.Background(), in, nil); e != nil {
		t.Fatal(e)
	}
	r, e := s.Run(context.Background(), Input{Path: in.Path}, nil)
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, step := range r.Steps {
		if step.Action == "bindings.link" && step.State == "complete" {
			found = true
		}
	}
	if !found {
		t.Fatalf("existing local mapping ignored by readiness %+v", r)
	}
}
func TestQADoctorLocallyDetectsMalformedConfigAndCorruptState(t *testing.T) {
	for _, kind := range []string{"config", "state"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			configPath := filepath.Join(dir, "cfg")
			statePath := filepath.Join(dir, "activity", "state")
			if e := os.MkdirAll(filepath.Dir(statePath), 0700); e != nil {
				t.Fatal(e)
			}
			badPath := configPath
			if kind == "state" {
				badPath = statePath
			}
			before := []byte("malformed-synthetic-private-content")
			if e := os.WriteFile(badPath, before, 0600); e != nil {
				t.Fatal(e)
			}
			a := auth.NewService(auth.Options{ConfigPath: configPath, LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
				t.Fatal("local doctor read credential")
				return auth.NativeReply{}, nil
			}), NewProvider: func(string, string) harvest.Provider { t.Fatal("local doctor constructed remote provider"); return nil }})
			r, e := New(Options{Auth: a, Activity: activity.New(activity.Options{Path: statePath})}).Doctor(context.Background(), false)
			detected := e != nil
			for _, item := range r.Items {
				if item.Severity == "error" {
					detected = true
				}
			}
			if !detected {
				t.Fatalf("doctor ignored corrupt local %s: %+v", kind, r)
			}
			raw, _ := json.Marshal(r)
			if strings.Contains(string(raw), string(before)) || e != nil && strings.Contains(e.Error(), string(before)) {
				t.Fatal("raw corrupt local content leaked")
			}
			after, readErr := os.ReadFile(badPath)
			if readErr != nil || string(after) != string(before) {
				t.Fatal("read-only doctor altered corrupt evidence")
			}
		})
	}
}
func TestQAGuidedChildBindingDoesNotReuseInheritedParentRevision(t *testing.T) {
	s, _, in := qaGuidedLink(t)
	in.ProjectID = "3"
	in.TaskID = "4"
	in.Timezone = "UTC"
	parent, e := s.Link(context.Background(), in, nil)
	if e != nil {
		t.Fatal(e)
	}
	child := filepath.Join(in.Path, "child")
	if e = os.Mkdir(child, 0700); e != nil {
		t.Fatal(e)
	}
	in.Path = child
	in.TaskID = ""
	in.Timezone = ""
	in.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	p := &qaSetupPrompt{t: t, confirm: true}
	r, e := s.Link(context.Background(), in, p)
	if e != nil {
		t.Fatalf("inherited defaults could not create exact child binding: %v", e)
	}
	if r.Binding.ID == parent.Binding.ID || r.Binding.Attribution != parent.Binding.Attribution {
		t.Fatalf("child changed inherited parent identity/attribution %+v", r)
	}
	list, e := s.options.Activity.ListBindings(context.Background())
	if e != nil || len(list.Bindings) != 2 {
		t.Fatalf("parent was replaced instead of child override %+v %v", list, e)
	}
	for _, b := range list.Bindings {
		if b.ID == parent.Binding.ID && b.Revision != parent.Binding.Revision {
			t.Fatal("child confirmation revised parent binding")
		}
	}
}

type qaUnavailableBackendProvider struct {
	harvest.Provider
	failure error
}

func (p qaUnavailableBackendProvider) Accounts(context.Context) ([]harvest.Object, error) {
	return nil, p.failure
}
func TestQAUnsupportedPersistencePreservesEnvironmentAuthFailures(t *testing.T) {
	for _, code := range []string{"network", "forbidden", "keychain", "cancel"} {
		t.Run(code, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			failure := error(&harvest.Error{Code: code, Message: "safe synthetic failure"})
			if code == "cancel" {
				cancel(&terminal.ExitError{Code: 130})
				failure = context.Canceled
			}
			a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(k string) string {
				if k == "HARVEST_TOKEN" {
					return "synthetic-qa-secret"
				}
				if k == "HARVEST_ACCOUNT_ID" {
					return "11"
				}
				return ""
			}, PersistentAvailable: func() bool { return false }, NewProvider: func(string, string) harvest.Provider { return qaUnavailableBackendProvider{failure: failure} }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
				t.Fatal("environment auth invoked unsupported backend")
				return auth.NativeReply{}, nil
			})})
			p := &qaWizardPrompt{t: t}
			_, e := New(Options{Auth: a, Activity: activity.New(activity.Options{Path: filepath.Join(dir, "activity", "state")})}).Run(ctx, Input{Path: dir}, p)
			if e == nil {
				t.Fatalf("unsupported storage swallowed environment %s failure", code)
			}
			if code == "cancel" && !errors.Is(e, context.Canceled) {
				var ended *terminal.ExitError
				if !errors.As(e, &ended) || ended.Code != 130 {
					t.Fatalf("cancellation lost %v", e)
				}
			}
			if p.secretCalls+p.confirmCalls+p.chooseCalls != 0 {
				t.Fatal("failed environment authentication started secret workflow")
			}
		})
	}
}
