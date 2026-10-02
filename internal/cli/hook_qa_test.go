package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rbeene/tempo/internal/hookstate"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
)

const qaHookJSON = `{"hook_event_name":"UserPromptSubmit","session_id":"s","turn_id":"t","cwd":"/synthetic-unlinked","prompt":"SECRET_PROMPT"}`

func TestQAHostCLIFiniteHostJSONBypassesCredentialsAndConfig(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		flags       []string
	}{
		{"unlinked", qaHookJSON, nil},
		{"json_flag", qaHookJSON, []string{"--json"}},
		{"noninteractive", qaHookJSON, []string{"--non-interactive"}},
		{"malformed", `{"SECRET_PROMPT":`, nil},
		{"oversized", strings.Repeat("SECRET", 12000), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "state", "activity.json")
			config := filepath.Join(root, "config.json")
			if err := os.WriteFile(config, []byte("PRIVATE_CONFIG_INVALID_JSON"), 0600); err != nil {
				t.Fatal(err)
			}
			s := activity.New(activity.Options{Path: path})
			store := &fakeStore{}
			d := cli.Dependencies{Activity: s, Store: store, ConfigPath: config, Getenv: func(k string) string {
				if k == "TEMPO_HOOK_STATE" {
					return filepath.Join(root, "policy", "state.json")
				}
				return ""
			}, NewProvider: func(string, string) harvest.Provider { t.Fatal("hook accessed provider"); return nil }}
			var out, stderr bytes.Buffer
			args := append([]string{"hook", "codex", "--input-stdin"}, tc.flags...)
			code := cli.Run(context.Background(), args, strings.NewReader(tc.input), &out, &stderr, d)
			if code != 0 || strings.TrimSpace(out.String()) != "{}" {
				t.Fatalf("hook must emit only nonblocking host object: exit=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
			}
			var obj map[string]json.RawMessage
			if err := json.Unmarshal(out.Bytes(), &obj); err != nil || len(obj) != 0 {
				t.Fatalf("host output contains control/envelope fields: %q", out.String())
			}
			if store.gets+store.sets+store.deletes != 0 {
				t.Fatal("hook touched credential store")
			}
			if strings.Contains(stderr.String(), "SECRET") || strings.Contains(stderr.String(), "PRIVATE_CONFIG") || strings.ContainsRune(stderr.String(), '\x1b') || stderr.Len() > 1024 {
				t.Fatalf("unsafe diagnostic %q", stderr.String())
			}
			if tc.name == "malformed" || tc.name == "oversized" {
				if stderr.Len() == 0 || !strings.Contains(stderr.String(), "not_committed") {
					t.Fatalf("error hid durable capture outcome: %q", stderr.String())
				}
			}
			if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unlinked hook wrote state: %v", err)
			}
		})
	}
}

func TestQAHostCLIInheritedInputDeadlineIsNonblocking(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "tempo")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer buildCancel()
	build := exec.CommandContext(buildCtx, filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, "../../cmd/tempo")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, output)
	}
	for _, input := range []string{`{"hook_event_name":`, qaHookJSON} {
		t.Run(fmt.Sprintf("bytes_%d", len(input)), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			path := filepath.Join(t.TempDir(), "state", "activity.json")
			cmd := exec.CommandContext(ctx, binary, "hook", "codex", "--input-stdin")
			cmd.Env = append(os.Environ(), "TEMPO_STATE="+path, "TEMPO_HOOK_STATE="+filepath.Join(t.TempDir(), "policy", "state.json"), "TEMPO_CONFIG="+filepath.Join(root, "unused-config"), "HARVEST_TOKEN=", "HARVEST_ACCOUNT_ID=")
			var out, stderr bytes.Buffer
			cmd.Stdout = &out
			cmd.Stderr = &stderr
			pipe, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer pipe.Close()
			begin := time.Now()
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			if _, err := pipe.Write([]byte(input)); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			if ctx.Err() != nil || time.Since(begin) > 2500*time.Millisecond {
				t.Fatalf("hook input blocked beyond finite budget: %v %q", err, stderr.String())
			}
			if err != nil || strings.TrimSpace(out.String()) != "{}" || !strings.Contains(stderr.String(), "not_committed") {
				t.Fatalf("deadline host result: err=%v stdout=%q stderr=%q", err, out.String(), stderr.String())
			}
			if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("timed-out input initialized store: %v", err)
			}
		})
	}
}

func TestQAHostCLIOpaqueReaderIsRejectedWithoutReading(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent", "activity.json")
	var out, stderr bytes.Buffer
	d := cli.Dependencies{Activity: activity.New(activity.Options{Path: path}), Store: &fakeStore{}, Getenv: func(string) string { return "" }, NewProvider: func(string, string) harvest.Provider { t.Fatal("opaque reader touched provider"); return nil }}
	code := cli.Run(context.Background(), []string{"hook", "codex", "--input-stdin"}, qaNeverRead{t}, &out, &stderr, d)
	if code != 0 || strings.TrimSpace(out.String()) != "{}" || !strings.Contains(stderr.String(), "not_committed") {
		t.Fatalf("opaque reader result: exit=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
	}
}

func TestQAHostCLICommittedClockFailureReportsDurableQuarantine(t *testing.T) {
	root := t.TempDir()
	cwd := filepath.Join(root, "project")
	if err := os.Mkdir(cwd, 0700); err != nil {
		t.Fatal(err)
	}
	policies := hookstate.New(hookstate.Options{Path: filepath.Join(root, "policy", "state.json")})
	c := hookstate.Context{Host: "codex", Scope: "project", Path: cwd, RuntimeVersion: "0.159.3", Surface: "local", Conflicts: []string{}}
	for _, role := range []string{"runtime", "executable", "definitions"} {
		p := filepath.Join(cwd, role)
		if err := os.WriteFile(p, []byte("synthetic "+role), 0600); err != nil {
			t.Fatal(err)
		}
		c.Artifacts = append(c.Artifacts, hookstate.Artifact{Role: role, Path: p})
	}
	p, err := policies.Preview(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := policies.Confirm(context.Background(), hookstate.ConfirmInput{Context: p.Context, Fingerprint: p.Fingerprint, DeclarationVersion: hookstate.DeclarationVersion, RequestID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", Confirmed: true}); err != nil {
		t.Fatal(err)
	}
	seconds := int64(0)
	clockFailed := false
	base := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	s := activity.New(activity.Options{Path: filepath.Join(root, "activity", "state.json"), HookPolicies: policies, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		epoch, n := "test-boot", strconv.FormatInt(seconds*int64(time.Second), 10)
		sample := activity.ClockSample{Capability: "available", WallUTC: base.Add(time.Duration(seconds) * time.Second), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}
		if clockFailed {
			return sample, errors.New("synthetic clock unavailable")
		}
		return sample, nil
	})})
	if _, err := s.Link(context.Background(), activity.LinkInput{Path: cwd, AccountID: "11", ProjectID: "100", TaskID: "200", Timezone: "UTC", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, activity.LinkDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { return &qaCLILinkAPI{}, nil }}); err != nil {
		t.Fatal(err)
	}
	for _, e := range []activity.HostEvent{{Source: "codex", Kind: "SessionStart", SessionID: "s", CWD: cwd, SessionSource: "startup"}, {Source: "codex", Kind: "UserPromptSubmit", SessionID: "s", TurnID: "t", CWD: cwd}} {
		if _, err := s.IngestHost(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	seconds = 10
	clockFailed = true
	payload, err := json.Marshal(map[string]any{"hook_event_name": "Stop", "session_id": "s", "turn_id": "t", "cwd": cwd, "stop_hook_active": false})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{}
	var out, stderr bytes.Buffer
	code := cli.Run(context.Background(), []string{"hook", "codex", "--input-stdin"}, strings.NewReader(string(payload)), &out, &stderr, cli.Dependencies{Activity: s, Store: store, Getenv: func(string) string { return "" }, NewProvider: func(string, string) harvest.Provider { t.Fatal("hook accessed provider"); return nil }})
	if code != 0 || strings.TrimSpace(out.String()) != "{}" {
		t.Fatalf("committed error blocked native host: %d %q %q", code, out.String(), stderr.String())
	}
	clockFailed = false
	snapshot, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Uncertainties) != 1 || len(snapshot.ClosedIntervals) != 0 {
		t.Fatalf("diagnostic claimed commit without quarantined tail: %+v", snapshot)
	}
	if store.gets+store.sets+store.deletes != 0 {
		t.Fatal("capture error accessed credentials")
	}
	if !strings.Contains(stderr.String(), "clock_unavailable") || !strings.Contains(stderr.String(), "committed") || strings.Contains(stderr.String(), "not_committed") {
		t.Fatalf("durable safety mutation mislabeled: %q", stderr.String())
	}
}

func TestQAHostCLIWaitReviewUsesFiniteDiagnostic(t *testing.T) {
	for _, tc := range []struct{ kind, diagnostic string }{{"Stop", "incomplete_wait"}, {"PermissionRequest", "source_loss_while_waiting"}} {
		t.Run(tc.kind, func(t *testing.T) {
			root := t.TempDir()
			cwd := filepath.Join(root, "project")
			if err := os.Mkdir(cwd, 0700); err != nil {
				t.Fatal(err)
			}
			policies := hookstate.New(hookstate.Options{Path: filepath.Join(root, "policy", "state.json")})
			c := hookstate.Context{Host: "codex", Scope: "project", Path: cwd, RuntimeVersion: "0.159.3", Surface: "local", Conflicts: []string{}}
			for _, role := range []string{"runtime", "executable", "definitions"} {
				p := filepath.Join(cwd, role)
				if err := os.WriteFile(p, []byte("synthetic "+role), 0600); err != nil {
					t.Fatal(err)
				}
				c.Artifacts = append(c.Artifacts, hookstate.Artifact{Role: role, Path: p})
			}
			p, err := policies.Preview(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := policies.Confirm(context.Background(), hookstate.ConfirmInput{Context: p.Context, Fingerprint: p.Fingerprint, DeclarationVersion: hookstate.DeclarationVersion, RequestID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", Confirmed: true}); err != nil {
				t.Fatal(err)
			}
			seconds := int64(0)
			clockFailed := false
			base := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
			s := activity.New(activity.Options{Path: filepath.Join(root, "activity", "state.json"), HookPolicies: policies, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
				epoch, n := "test-boot", strconv.FormatInt(seconds*int64(time.Second), 10)
				sample := activity.ClockSample{Capability: "available", WallUTC: base.Add(time.Duration(seconds) * time.Second), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}
				if clockFailed {
					return sample, errors.New("synthetic clock unavailable")
				}
				return sample, nil
			})})
			if _, err := s.Link(context.Background(), activity.LinkInput{Path: cwd, AccountID: "11", ProjectID: "100", TaskID: "200", Timezone: "UTC", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, activity.LinkDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { return &qaCLILinkAPI{}, nil }}); err != nil {
				t.Fatal(err)
			}
			for _, e := range []activity.HostEvent{{Source: "codex", Kind: "SessionStart", SessionID: "s", CWD: cwd, SessionSource: "startup"}, {Source: "codex", Kind: "UserPromptSubmit", SessionID: "s", TurnID: "t", CWD: cwd}} {
				if _, err := s.IngestHost(context.Background(), e); err != nil {
					t.Fatal(err)
				}
			}

			seconds = 10
			if _, err := s.IngestHost(context.Background(), activity.HostEvent{Source: "codex", Kind: "PreToolUse", SessionID: "s", TurnID: "t", CWD: cwd, ToolID: "wait-1", ToolName: "wait_agent"}); err != nil {
				t.Fatal(err)
			}
			seconds = 20
			payload, err := json.Marshal(map[string]any{"hook_event_name": tc.kind, "session_id": "s", "turn_id": "t", "cwd": cwd, "stop_hook_active": false, "tool_name": "Bash"})
			if err != nil {
				t.Fatal(err)
			}
			var out, stderr bytes.Buffer
			code := cli.Run(context.Background(), []string{"hook", "codex", "--input-stdin"}, strings.NewReader(string(payload)), &out, &stderr, cli.Dependencies{Activity: s, Store: &fakeStore{}, Getenv: func(string) string { return "" }, NewProvider: func(string, string) harvest.Provider { t.Fatal("hook accessed provider"); return nil }})
			snapshot, err := s.Status(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.CaptureReviews) != 1 || snapshot.CaptureReviews[0].DiagnosticCode != tc.diagnostic {
				t.Fatalf("missing actual committed wait review: %+v", snapshot.CaptureReviews)
			}
			if code != 0 || strings.TrimSpace(out.String()) != "{}" || !strings.Contains(stderr.String(), tc.diagnostic) || !strings.Contains(stderr.String(), "durability=committed") || strings.Contains(stderr.String(), "internal") {
				t.Fatalf("wrong finite wait diagnostic: exit=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
			}
		})
	}
}
