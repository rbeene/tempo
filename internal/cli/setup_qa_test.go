package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type qaForbiddenPrompt struct{ t *testing.T }

func (p qaForbiddenPrompt) Choose(context.Context, string, []terminal.Choice) (string, error) {
	p.t.Fatal("machine mode prompted")
	return "", nil
}
func (p qaForbiddenPrompt) Text(context.Context, string, string) (string, error) {
	p.t.Fatal("machine mode prompted")
	return "", nil
}
func (p qaForbiddenPrompt) Secret(context.Context, string) ([]byte, error) {
	p.t.Fatal("machine mode read secret")
	return nil, nil
}
func (p qaForbiddenPrompt) Confirm(context.Context, string) (bool, error) {
	p.t.Fatal("machine mode confirmed")
	return false, nil
}
func TestQASetupMachineControlsOverrideInjectedTTY(t *testing.T) {
	for _, flag := range []string{"--json", "--non-interactive"} {
		t.Run(flag, func(t *testing.T) {
			dir := t.TempDir()
			a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(string) string { return "" }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
				t.Fatal("finite setup accessed credentials")
				return auth.NativeReply{}, nil
			})})
			var out, stderr bytes.Buffer
			code := cli.Run(context.Background(), []string{"setup", flag}, strings.NewReader(""), &out, &stderr, cli.Dependencies{Auth: a, Activity: activity.New(activity.Options{Path: filepath.Join(dir, "state")}), ConfigPath: filepath.Join(dir, "cfg"), Getenv: func(string) string { return "" }, Prompter: qaForbiddenPrompt{t}, TerminalEligible: func(io.Reader, io.Writer) bool { return true }})
			if code != 0 || stderr.Len() != 0 {
				t.Fatalf("setup exit=%d stderr=%s", code, stderr.String())
			}
			var result map[string]any
			if json.Unmarshal(out.Bytes(), &result) != nil || result["schema_version"] != float64(1) {
				t.Fatalf("not one machine envelope %q", out.String())
			}
			if strings.Contains(out.String(), "\x1b") {
				t.Fatal("ANSI in machine output")
			}
		})
	}
}

type qaCLIPartialPrompt struct {
	t          *testing.T
	cancelPath bool
	confirms   int
}

func (p *qaCLIPartialPrompt) Secret(context.Context, string) ([]byte, error) {
	return []byte("synthetic-qa-secret"), nil
}
func (p *qaCLIPartialPrompt) Choose(context.Context, string, []terminal.Choice) (string, error) {
	p.t.Fatal("unexpected picker after partial-stage fixture")
	return "", nil
}
func (p *qaCLIPartialPrompt) Text(context.Context, string, string) (string, error) {
	if p.cancelPath {
		return "", &terminal.ExitError{Code: 0}
	}
	p.t.Fatal("unexpected path prompt")
	return "", nil
}
func (p *qaCLIPartialPrompt) Confirm(context.Context, string) (bool, error) {
	p.confirms++
	return true, nil
}
func TestQACLISetupReportsEarlierAuthOnLaterFailureOrCancel(t *testing.T) {
	for _, cancelPath := range []bool{false, true} {
		t.Run(map[bool]string{false: "later-link-error", true: "later-path-cancel"}[cancelPath], func(t *testing.T) {
			dir := t.TempDir()
			stored := false
			commits := 0
			a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, NewProvider: func(string, string) harvest.Provider { return &fakeAPI{} }, Runner: auth.RunnerFunc(func(_ context.Context, r auth.NativeRequest, l *os.File) (auth.NativeReply, error) {
				switch r.Operation {
				case "read":
					if stored {
						return auth.NativeReply{Token: []byte("synthetic-qa-secret"), Effects: auth.Effects{Credential: "unchanged", Config: "unchanged"}}, nil
					}
					return auth.NativeReply{Code: "not_found", Effects: auth.Effects{Credential: "unchanged", Config: "unchanged"}}, nil
				case "login":
					stored = true
					commits++
					if l == nil {
						t.Fatal("no credential owner")
					}
					if e := auth.Save(filepath.Join(dir, "cfg"), auth.Config{Account: r.AccountID}); e != nil {
						t.Fatal(e)
					}
					return auth.NativeReply{Effects: auth.Effects{Credential: "applied", Config: "saved"}}, nil
				}
				t.Fatalf("unexpected native op %s", r.Operation)
				return auth.NativeReply{}, nil
			})})
			p := &qaCLIPartialPrompt{t: t, cancelPath: cancelPath}
			args := []string{"setup"}
			if !cancelPath {
				args = append(args, "--path", dir)
			}
			var out, stderr bytes.Buffer
			code := cli.Run(context.Background(), args, strings.NewReader(""), &out, &stderr, cli.Dependencies{Auth: a, Activity: activity.New(activity.Options{Path: filepath.Join(dir, "activity", "state")}), Prompter: p, TerminalEligible: func(io.Reader, io.Writer) bool { return true }, ConfigPath: filepath.Join(dir, "cfg"), Getenv: func(string) string { return "" }})
			if commits != 1 || p.confirms != 1 {
				t.Fatalf("test did not reach confirmed auth commits=%d confirms=%d stderr=%s", commits, p.confirms, stderr.String())
			}
			if cancelPath && code != 0 || !cancelPath && code == 0 {
				t.Fatalf("wrong final failure/cancel exit %d", code)
			}
			combined := out.String() + stderr.String()
			if !strings.Contains(combined, "auth.login") || !strings.Contains(strings.ToLower(combined), "complet") {
				t.Fatalf("earlier credential persistence hidden from user: exit=%d out=%s err=%s", code, out.String(), stderr.String())
			}
			if strings.Contains(combined, "synthetic-qa-secret") {
				t.Fatal("secret in partial-step output")
			}
			cfg, e := auth.Load(filepath.Join(dir, "cfg"))
			if e != nil || cfg.Account != "11" {
				t.Fatalf("later failure undid config %+v %v", cfg, e)
			}
		})
	}
}

type qaGuidedCLIProvider struct{ fakeAPI }

func (p *qaGuidedCLIProvider) Get(context.Context, string) (harvest.Object, error) {
	return harvest.Object{"id": "7", "is_active": true}, nil
}
func (p *qaGuidedCLIProvider) List(context.Context, string, url.Values) ([]harvest.Object, error) {
	return []harvest.Object{{"is_active": true, "project": harvest.Object{"id": "3", "name": "Project"}, "task_assignments": []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": "4", "name": "Work"}}}}}, nil
}

type qaCLILinkPrompt struct {
	t                 *testing.T
	choices, confirms int
}

func (p *qaCLILinkPrompt) Choose(_ context.Context, _ string, c []terminal.Choice) (string, error) {
	p.choices++
	if len(c) != 1 || c[0].ID != "3" {
		p.t.Fatalf("unexpected project choices %+v", c)
	}
	return "3", nil
}
func (p *qaCLILinkPrompt) Text(context.Context, string, string) (string, error) {
	p.t.Fatal("explicit path/timezone prompted")
	return "", nil
}
func (p *qaCLILinkPrompt) Secret(context.Context, string) ([]byte, error) {
	p.t.Fatal("environment link requested secret")
	return nil, nil
}
func (p *qaCLILinkPrompt) Confirm(context.Context, string) (bool, error) {
	p.confirms++
	return true, nil
}
func TestQACLIBareLinkUsesEligibleProjectPicker(t *testing.T) {
	dir := t.TempDir()
	api := &qaGuidedCLIProvider{}
	a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(k string) string {
		if k == "HARVEST_TOKEN" {
			return "synthetic-qa-secret"
		}
		if k == "HARVEST_ACCOUNT_ID" {
			return "11"
		}
		return ""
	}, NewProvider: func(string, string) harvest.Provider { return api }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Fatal("environment picker accessed native backend")
		return auth.NativeReply{}, nil
	})})
	act := activity.New(activity.Options{Path: filepath.Join(dir, "activity", "state")})
	p := &qaCLILinkPrompt{t: t}
	var out, stderr bytes.Buffer
	code := cli.Run(context.Background(), []string{"link", "--path", dir, "--timezone", "UTC"}, strings.NewReader(""), &out, &stderr, cli.Dependencies{Auth: a, Activity: act, ConfigPath: filepath.Join(dir, "cfg"), Getenv: func(string) string { return "" }, Prompter: p, TerminalEligible: func(io.Reader, io.Writer) bool { return true }})
	if code != 0 || p.choices != 1 || p.confirms != 1 {
		t.Fatalf("interactive project picker unreachable: exit=%d choices=%d confirms=%d err=%s", code, p.choices, p.confirms, stderr.String())
	}
	bindings, e := act.ListBindings(context.Background())
	if e != nil || len(bindings.Bindings) != 1 || bindings.Bindings[0].Attribution.ProjectID != "3" || bindings.Bindings[0].Attribution.TaskID != "4" {
		t.Fatalf("guided CLI did not persist chosen mapping %+v %v", bindings, e)
	}
}
