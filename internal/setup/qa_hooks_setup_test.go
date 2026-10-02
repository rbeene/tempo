package setup

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/worker"
)

type qaHooksSetupRunner struct{ t *testing.T }

func (r qaHooksSetupRunner) Run(context.Context, worker.Command) (worker.CommandResult, error) {
	r.t.Fatal("hook setup invoked service manager")
	return worker.CommandResult{}, nil
}
func qaHooksSetupFixture(t *testing.T) (*Service, *hookstate.Service, Input, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	project := filepath.Join(root, "project")
	if err = os.Mkdir(project, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"tempo", "runtime"} {
		if err = os.WriteFile(filepath.Join(root, name), []byte("inert fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	config := filepath.Join(root, "account.json")
	a := auth.NewService(auth.Options{ConfigPath: config, LockPath: filepath.Join(root, "credential.lock"), Getenv: func(string) string { return "" }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Fatal("hook setup accessed credentials")
		return auth.NativeReply{}, nil
	}), NewProvider: func(string, string) harvest.Provider { t.Fatal("hook setup constructed network provider"); return nil }})
	activityService := activity.New(activity.Options{Path: filepath.Join(root, "activity", "state.json")})
	hooks := hookstate.New(hookstate.Options{CodexSystemDir: filepath.Join(root, "system-codex"), ClaudeManagedDir: filepath.Join(root, "system-claude"), Path: filepath.Join(root, "metadata", "hooks.json"), HomeDir: filepath.Join(root, "home"), Executable: filepath.Join(root, "tempo"), BuildVersion: "qa-setup-15", DiscoverRuntime: func(_ context.Context, host string) (hookstate.Runtime, error) {
		version := "0.159.3"
		if host == "claude" {
			version = "2.1.286"
		}
		return hookstate.Runtime{Path: filepath.Join(root, "runtime"), Version: version, Surface: "local"}, nil
	}})
	w, err := worker.New(worker.Options{StatePath: filepath.Join(root, "activity", "state.json"), ConfigPath: config, Executable: filepath.Join(root, "tempo"), ServiceDir: filepath.Join(root, "services"), Platform: "darwin", UID: 501, Sync: activityService, Runner: qaHooksSetupRunner{t}})
	if err != nil {
		t.Fatal(err)
	}
	return New(Options{Auth: a, Activity: activityService, Hooks: hooks, Worker: w}), hooks, Input{Host: "claude", Scope: "project", Path: project}, root
}

type qaHooksSetupPrompt struct {
	t                         *testing.T
	action                    string
	yes                       bool
	chooseCount, confirmCount int
	confirmText               string
}

func (p *qaHooksSetupPrompt) Choose(_ context.Context, label string, choices []terminal.Choice) (string, error) {
	p.chooseCount++
	if p.chooseCount > 1 {
		p.t.Fatal("finite hook action unexpectedly loops")
	}
	for _, choice := range choices {
		if choice.ID == p.action {
			return choice.ID, nil
		}
	}
	p.t.Fatalf("hook action %s missing from %s %+v", p.action, label, choices)
	return "", nil
}
func (p *qaHooksSetupPrompt) Confirm(_ context.Context, label string) (bool, error) {
	p.confirmCount++
	p.confirmText += label
	return p.yes, nil
}
func (p *qaHooksSetupPrompt) Text(context.Context, string, string) (string, error) {
	p.t.Fatal("explicit hook context requested text")
	return "", nil
}
func (p *qaHooksSetupPrompt) Secret(context.Context, string) ([]byte, error) {
	p.t.Fatal("hook action requested secret")
	return nil, nil
}
func qaHooksSetupAbsent(t *testing.T, root string) {
	t.Helper()
	for _, name := range []string{"metadata", "home", "activity", "services", "credential.lock"} {
		if _, err := os.Stat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read/cancel initialized %s: %v", name, err)
		}
	}
}
func TestQAHooksSetupNilPromptUsesSharedReadOnlyStatus(t *testing.T) {
	s, hooks, in, root := qaHooksSetupFixture(t)
	want, err := hooks.Status(context.Background(), hookstate.HookSelector{Host: in.Host, Scope: in.Scope, Path: in.Path})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.ManageHooks(context.Background(), in, nil)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("setup status diverges from shared service: %+v %v", got, err)
	}
	qaHooksSetupAbsent(t, root)
}
func TestQAHooksSetupPreviewAndCancelledInstallNeverMutate(t *testing.T) {
	for _, action := range []string{"preview", "cancel", "install"} {
		t.Run(action, func(t *testing.T) {
			s, _, in, root := qaHooksSetupFixture(t)
			prompt := &qaHooksSetupPrompt{t: t, action: action, yes: false}
			_, err := s.ManageHooks(context.Background(), in, prompt)
			if err != nil {
				var exit *terminal.ExitError
				if !errors.As(err, &exit) || exit.Code != 0 {
					t.Fatal(err)
				}
			}
			if action != "cancel" && (prompt.confirmCount == 0 || !strings.Contains(prompt.confirmText, filepath.Join(root, "tempo")) || !strings.Contains(prompt.confirmText, "hook claude --input-stdin") || !strings.Contains(prompt.confirmText, filepath.Join(in.Path, ".claude", "settings.json"))) {
				t.Fatalf("approval lacks concrete reviewed command/destination: %s", prompt.confirmText)
			}
			qaHooksSetupAbsent(t, root)
			if _, err := os.Stat(filepath.Join(in.Path, ".claude")); !os.IsNotExist(err) {
				t.Fatal("unconfirmed action changed host configuration")
			}
		})
	}
}
func TestQAHooksSetupActionsKeepPolicyConfirmationSeparate(t *testing.T) {
	s, hooks, in, root := qaHooksSetupFixture(t)
	install := &qaHooksSetupPrompt{t: t, action: "install", yes: true}
	installed, err := s.ManageHooks(context.Background(), in, install)
	if err != nil {
		t.Fatal(err)
	}
	if len(installed.Hooks) != 1 || installed.Hooks[0].Profile.CaptureEligible || installed.Hooks[0].LastRealEvent != nil || install.confirmCount != 1 {
		t.Fatalf("setup install silently opted in: %+v prompt=%+v", installed, install)
	}
	before, err := os.ReadFile(filepath.Join(root, "metadata", "hooks.json"))
	if err != nil {
		t.Fatal(err)
	}
	declined := &qaHooksSetupPrompt{t: t, action: "confirm-profile", yes: false}
	_, err = s.ManageHooks(context.Background(), in, declined)
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(root, "metadata", "hooks.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("unchecked profile choice persisted consent")
	}
	confirm := &qaHooksSetupPrompt{t: t, action: "confirm-profile", yes: true}
	confirmed, err := s.ManageHooks(context.Background(), in, confirm)
	if err != nil {
		t.Fatal(err)
	}
	if len(confirmed.Hooks) != 1 || !confirmed.Hooks[0].Profile.CaptureEligible || confirmed.Hooks[0].Profile.Basis != "operator_declared" || confirmed.Hooks[0].LastRealEvent != nil {
		t.Fatalf("setup declaration not distinct from delivery: %+v", confirmed)
	}
	if confirm.confirmCount != 1 || !strings.Contains(strings.ToLower(confirm.confirmText), "inaccur") {
		t.Fatalf("declaration omitted acknowledged timing accuracy risk: %s", confirm.confirmText)
	}
	before, err = os.ReadFile(filepath.Join(root, "metadata", "hooks.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"status", "verify"} {
		_, err = s.ManageHooks(context.Background(), in, &qaHooksSetupPrompt{t: t, action: action})
		if err != nil {
			t.Fatal(err)
		}
	}
	after, err = os.ReadFile(filepath.Join(root, "metadata", "hooks.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("status/verify persisted evidence")
	}
	revoked, err := s.ManageHooks(context.Background(), in, &qaHooksSetupPrompt{t: t, action: "revoke-profile", yes: true})
	if err != nil {
		t.Fatal(err)
	}
	if revoked.Hooks[0].Profile.CaptureEligible {
		t.Fatal("setup revoke left eligible policy")
	}
	for _, action := range []string{"repair", "uninstall"} {
		_, err = s.ManageHooks(context.Background(), in, &qaHooksSetupPrompt{t: t, action: action, yes: true})
		if err != nil {
			t.Fatal(err)
		}
	}
	status, err := hooks.Status(context.Background(), hookstate.HookSelector{Host: in.Host, Scope: in.Scope, Path: in.Path})
	if err != nil {
		t.Fatal(err)
	}
	if status.Hooks[0].State != "not_installed" || status.Hooks[0].Profile.CaptureEligible {
		t.Fatalf("setup removal not reflected in shared service: %+v", status)
	}
	for _, name := range []string{"activity", "services", "credential.lock"} {
		if _, err := os.Stat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Fatalf("hook setup mutated independent %s", name)
		}
	}
}
func TestQAHooksSetupFiniteReadinessComposesHooksAndWorkerWithoutStarting(t *testing.T) {
	s, _, in, root := qaHooksSetupFixture(t)
	result, err := s.Run(context.Background(), in, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Complete {
		t.Fatal("unlinked setup claimed complete")
	}
	seen := map[string]Step{}
	for _, step := range result.Steps {
		seen[step.Action] = step
	}
	if step, ok := seen["hooks.status"]; !ok || step.State == "unsupported" {
		t.Fatalf("readiness omits available hook service: %+v", result)
	}
	if _, ok := seen["worker.status"]; !ok {
		t.Fatalf("readiness fails to expose independent worker state: %+v", result)
	}
	qaHooksSetupAbsent(t, root)
}

func TestQAHooksSetupEligibleCaptureCompletesOnlyHookStep(t *testing.T) {
	s, hooks, in, _ := qaHooksSetupFixture(t)
	intent := hookstate.InstallIntent{Host: in.Host, Scope: in.Scope, Path: in.Path, Operation: "install"}
	preview, err := hooks.PreviewInstall(context.Background(), intent)
	if err != nil {
		t.Fatal(err)
	}
	_, err = hooks.ApplyInstall(context.Background(), hookstate.ApplyInstallInput{Intent: intent, Fingerprint: preview.Fingerprint, RequestID: "15150000-0000-4000-8000-000000000121", Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	selector := hookstate.HookSelector{Host: in.Host, Scope: in.Scope, Path: in.Path}
	status, err := hooks.Status(context.Background(), selector)
	if err != nil {
		t.Fatal(err)
	}
	p := status.Hooks[0].Profile
	_, err = hooks.Confirm(context.Background(), hookstate.ConfirmInput{Context: p.Context, Fingerprint: p.Fingerprint, DeclarationVersion: p.DeclarationVersion, RequestID: "15150000-0000-4000-8000-000000000122", Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Run(context.Background(), in, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Complete {
		t.Fatal("hook eligibility hid missing independent setup prerequisites")
	}
	found := false
	for _, step := range result.Steps {
		if step.Action == "hooks.status" {
			found = true
			if step.State != "complete" {
				t.Fatalf("eligible capture blocked by absent native-delivery proof: %+v", step)
			}
			text := strings.ToLower(step.SafeMessage)
			if !strings.Contains(text, "deliver") || (!strings.Contains(text, "unverified") && !strings.Contains(text, "not verified")) {
				t.Fatalf("completed hook step omitted explicit unverified delivery: %+v", step)
			}
		}
	}
	if !found {
		t.Fatal("missing hooks readiness step")
	}
	status, err = hooks.Status(context.Background(), selector)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Hooks[0].Profile.CaptureEligible || status.Hooks[0].LastRealEvent != nil || status.Hooks[0].State == "receiving" {
		t.Fatalf("setup completion fabricated delivery: %+v", status)
	}
}
