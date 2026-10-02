package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
			code := cli.Run(context.Background(), []string{"setup", flag}, strings.NewReader(""), &out, &stderr, cli.Dependencies{Auth: a, Activity: activity.New(activity.Options{Path: filepath.Join(dir, "activity", "state")}), ConfigPath: filepath.Join(dir, "cfg"), Getenv: func(string) string { return "" }, Prompter: qaForbiddenPrompt{t}, TerminalEligible: func(io.Reader, io.Writer) bool { return true }})
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

type qaForbiddenLegacyStore struct{ t *testing.T }

func (s qaForbiddenLegacyStore) Get() (string, error) {
	s.t.Fatal("bounded operation invoked legacy Get")
	return "", nil
}
func (s qaForbiddenLegacyStore) Set(string) error {
	s.t.Fatal("bounded operation invoked legacy Set")
	return nil
}
func (s qaForbiddenLegacyStore) Delete() error {
	s.t.Fatal("bounded operation invoked legacy Delete")
	return nil
}
func TestQACLIAuthUnknownEffectsExitEightAcrossWriters(t *testing.T) {
	for _, command := range [][]string{{"auth", "logout", "--yes"}, {"accounts", "use", "11"}, {"config", "set-account", "11"}} {
		t.Run(strings.Join(command, "-"), func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			calls := 0
			credential := "unchanged"
			if command[0] == "auth" {
				credential = "unknown"
			}
			a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(k string) string {
				if k == "HARVEST_TOKEN" {
					return "synthetic-qa-secret"
				}
				return ""
			}, PersistentAvailable: func() bool { return true }, NewProvider: func(string, string) harvest.Provider { return &fakeAPI{} }, Runner: auth.RunnerFunc(func(_ context.Context, r auth.NativeRequest, l *os.File) (auth.NativeReply, error) {
				calls++
				if l == nil {
					t.Fatal("writer omitted mutation ownership")
				}
				if command[0] != "auth" && r.Operation != "account" {
					t.Fatalf("account writer wrong operation %s", r.Operation)
				}
				cancel(&terminal.ExitError{Code: 130})
				return auth.NativeReply{}, &auth.Error{Code: "credential_write_unknown", Message: "synthetic-qa-secret raw backend message", Uncertain: true, Effects: auth.Effects{Credential: credential, Config: "unknown"}}
			})})
			var out, stderr bytes.Buffer
			code := cli.Run(ctx, append(command, "--json"), strings.NewReader(""), &out, &stderr, cli.Dependencies{Auth: a, Store: qaForbiddenLegacyStore{t}, ConfigPath: filepath.Join(dir, "cfg"), Getenv: func(string) string { return "" }})
			envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 8, "credential_write_unknown")
			if calls != 1 {
				t.Fatalf("mutation replayed %d times", calls)
			}
			var result map[string]any
			json.Unmarshal(stderr.Bytes(), &result)
			failure := result["error"].(map[string]any)
			if failure["uncertain"] != true || failure["retryable"] != false {
				t.Fatalf("unsafe replay guidance %+v", failure)
			}
			if strings.Contains(stderr.String(), "synthetic-qa-secret") {
				t.Fatal("runner error leaked secret")
			}
			if !strings.Contains(stderr.String(), `"credential":"`+credential+`"`) || !strings.Contains(stderr.String(), `"config":"unknown"`) {
				t.Fatalf("operation-specific effects lost %s", stderr.String())
			}
		})
	}
}

type qaCanceledActionProvider struct{ qaGuidedCLIProvider }

func (p *qaCanceledActionProvider) Accounts(ctx context.Context) ([]harvest.Object, error) {
	fmt.Fprintln(os.Stdout, "QA_CLI_ACTION_READY")
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestQACLIActionPTYChild(t *testing.T) {
	if os.Getenv("TEMPO_QA_CLI_ACTION_CHILD") != "1" {
		return
	}
	dir := os.Getenv("TEMPO_QA_CLI_ACTION_DIR")
	a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(k string) string {
		if k == "HARVEST_TOKEN" {
			return "synthetic-qa-secret"
		}
		if k == "HARVEST_ACCOUNT_ID" {
			return "11"
		}
		return ""
	}, NewProvider: func(string, string) harvest.Provider { return &qaCanceledActionProvider{} }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Fatal("synthetic action used native backend")
		return auth.NativeReply{}, nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	code := cli.Run(ctx, []string{"link", "3", "--task", "4", "--timezone", "UTC", "--path", dir}, os.Stdin, os.Stdout, os.Stderr, cli.Dependencies{Auth: a, Activity: activity.New(activity.Options{Path: filepath.Join(dir, "activity", "state")}), ConfigPath: filepath.Join(dir, "cfg"), Getenv: func(string) string { return "" }})
	if code != 130 {
		fmt.Fprintf(os.Stderr, "QA_BAD_EXIT_%d\n", code)
		os.Exit(51)
	}
	os.Exit(0)
}
func TestQACLIRealPTYRawCtrlCPreservesActionCancellation(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, "-c", qaCLIActionScript, os.Args[0], t.TempDir())
	out, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("CLI action cancellation: %v\n%s", e, out)
	}
}

const qaCLIActionScript = `
import os, sys, pty, subprocess, select, time
master,slave=pty.openpty()
env=dict(os.environ,TEMPO_QA_CLI_ACTION_CHILD='1',TEMPO_QA_CLI_ACTION_DIR=sys.argv[2],GORACE='atexit_sleep_ms=0')
p=subprocess.Popen([sys.argv[1],'-test.run=^TestQACLIActionPTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env)
transcript=b''
try:
 deadline=time.monotonic()+2
 while b'QA_CLI_ACTION_READY' not in transcript and time.monotonic()<deadline:
  ready,_,_=select.select([master],[],[],.03)
  if ready: transcript+=os.read(master,65536)
 if b'QA_CLI_ACTION_READY' not in transcript: raise AssertionError('action not reached '+repr(transcript))
 os.write(master,b'\x03')
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:
  ready,_,_=select.select([master],[],[],.03)
  if ready: transcript+=os.read(master,65536)
 if p.poll() is None: raise AssertionError('raw Ctrl-C did not cancel action promptly')
 if p.returncode: raise AssertionError('incorrect CLI cancellation '+repr(transcript))
finally:
 if p.poll() is None: p.kill();p.wait()
 os.close(master);os.close(slave)
`

type qaFailedPrompt struct{ qaCLILinkPrompt }

func (p *qaFailedPrompt) Choose(context.Context, string, []terminal.Choice) (string, error) {
	return "", &terminal.ExitError{Code: 1}
}
func TestQACLITerminalFailureHasSafeDiagnostic(t *testing.T) {
	dir := t.TempDir()
	a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(k string) string {
		if k == "HARVEST_TOKEN" {
			return "synthetic-qa-secret"
		}
		if k == "HARVEST_ACCOUNT_ID" {
			return "11"
		}
		return ""
	}, NewProvider: func(string, string) harvest.Provider { return &qaGuidedCLIProvider{} }})
	var out, stderr bytes.Buffer
	code := cli.Run(context.Background(), []string{"link", "--path", dir}, strings.NewReader(""), &out, &stderr, cli.Dependencies{Auth: a, Activity: activity.New(activity.Options{Path: filepath.Join(dir, "activity", "state")}), ConfigPath: filepath.Join(dir, "cfg"), Getenv: func(string) string { return "" }, Prompter: &qaFailedPrompt{qaCLILinkPrompt{t: t}}, TerminalEligible: func(io.Reader, io.Writer) bool { return true }})
	if code != 1 || stderr.Len() == 0 || out.Len() != 0 {
		t.Fatalf("terminal failure lacks safe diagnostic exit=%d out=%s err=%s", code, out.String(), stderr.String())
	}
	if strings.Contains(stderr.String(), "synthetic-qa-secret") {
		t.Fatal("terminal error leaked secret")
	}
}
