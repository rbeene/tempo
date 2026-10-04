package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
)

const qaClaudeHookJSON = `{"hook_event_name":"UserPromptSubmit","session_id":"s","prompt_id":"p","cwd":"/synthetic-unlinked","prompt":"SECRET_PROMPT"}`

func qaClaudeRun(t *testing.T, d cli.Dependencies, payload string) string {
	t.Helper()
	var out, errout bytes.Buffer
	start := time.Now()
	code := cli.Run(context.Background(), []string{"hook", "claude", "--input-stdin"}, strings.NewReader(payload), &out, &errout, d)
	if code != 0 || out.Len() != 0 {
		t.Fatalf("Claude hook changed host flow: exit%d stdout%q stderr%q", code, out.String(), errout.String())
	}
	if time.Since(start) > 1500*time.Millisecond {
		t.Fatal("finite callback budget exceeded")
	}
	if strings.Contains(errout.String(), "SECRET") || errout.Len() > 1024 {
		t.Fatalf("unsafe diagnostic %q", errout.String())
	}
	return errout.String()
}
func TestQAClaudeCLIFiniteEmptyOutputAndNoCredentials(t *testing.T) {
	for _, mode := range []string{"unlinked", "malformed", "oversize", "wrong_prompt"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "absent", "activity.json")
			config := filepath.Join(root, "config.json")
			if err := os.WriteFile(config, []byte("PRIVATE_CONFIG_INVALID"), 0600); err != nil {
				t.Fatal(err)
			}
			store := &fakeStore{}
			d := cli.Dependencies{Activity: activity.New(activity.Options{Path: path}), Store: store, ConfigPath: config, Getenv: func(string) string { return "" }, NewProvider: func(string, string) harvest.Provider { t.Fatal("hook accessed provider"); return nil }}
			payload := qaClaudeHookJSON
			switch mode {
			case "malformed":
				payload = `{"SECRET":`
			case "oversize":
				payload = strings.Repeat("SECRET", 12000)
			case "wrong_prompt":
				payload = strings.Replace(payload, "prompt_id", "turn_id", 1)
			}
			diagnostic := qaClaudeRun(t, d, payload)
			if !strings.Contains(diagnostic, "not_committed") {
				t.Fatalf("capture outcome hidden %q", diagnostic)
			}
			if store.gets+store.sets+store.deletes != 0 {
				t.Fatal("hook accessed credentials")
			}
			if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("hook initialized unlinked state %v", err)
			}
		})
	}
}
func TestQAClaudeCLIHeldOpenInputDeadlineAndOpaqueReader(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "tempo")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	build := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, "../../cmd/tempo")
	if b, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %v %s", err, b)
	}
	for _, input := range []string{`{"hook_event_name":`, qaClaudeHookJSON} {
		t.Run(input[:10], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			path := filepath.Join(t.TempDir(), "absent", "activity.json")
			cmd := exec.CommandContext(ctx, binary, "hook", "claude", "--input-stdin")
			cmd.Env = append(os.Environ(), "TEMPO_STATE="+path, "TEMPO_HOOK_STATE="+filepath.Join(root, "policy.json"), "TEMPO_CONFIG="+filepath.Join(root, "config.json"), "HARVEST_TOKEN=", "HARVEST_ACCOUNT_ID=")
			var out, stderr bytes.Buffer
			cmd.Stdout = &out
			cmd.Stderr = &stderr
			pipe, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer pipe.Close()
			start := time.Now()
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			if _, err = pipe.Write([]byte(input)); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			if ctx.Err() != nil || time.Since(start) > 2500*time.Millisecond {
				t.Fatal("held input exceeded bounded host deadline")
			}
			if err != nil || out.Len() != 0 || !strings.Contains(stderr.String(), "not_committed") {
				t.Fatalf("deadline host outcome %v %q %q", err, out.String(), stderr.String())
			}
			if _, err = os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("timed out callback wrote state")
			}
		})
	}
	var out, stderr bytes.Buffer
	code := cli.Run(context.Background(), []string{"hook", "claude", "--input-stdin"}, qaNeverRead{t}, &out, &stderr, cli.Dependencies{Activity: activity.New(activity.Options{Path: filepath.Join(root, "absent", "opaque.json")}), Getenv: func(string) string { return "" }})
	if code != 0 || out.Len() != 0 || !strings.Contains(stderr.String(), "not_committed") {
		t.Fatalf("opaque reader outcome%d %q %q", code, out.String(), stderr.String())
	}
}
func qaClaudeWakeFixture(t *testing.T, path string) (cli.Dependencies, string, *int64, *bool) {
	t.Helper()
	d, cwd, seconds, failed := qaHostWakeFixture(t, path)
	// Close the fixture's independent Codex root at zero, using public ingress.
	if _, err := d.Activity.IngestHost(context.Background(), activity.HostEvent{Source: "codex", Kind: "Stop", SessionID: "s", TurnID: "t", CWD: cwd}); err != nil {
		t.Fatal(err)
	}
	policies := hookstate.New(hookstate.Options{Path: filepath.Join(filepath.Dir(path), "policy", "state.json")})
	c := hookstate.Context{Host: "claude", Scope: "project", Path: cwd, RuntimeVersion: "2.1.286", Surface: "local", Conflicts: []string{}}
	for _, role := range []string{"runtime", "executable", "definitions"} {
		c.Artifacts = append(c.Artifacts, hookstate.Artifact{Role: role, Path: filepath.Join(cwd, role)})
	}
	p, err := policies.Preview(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = policies.Confirm(context.Background(), hookstate.ConfirmInput{Context: p.Context, Fingerprint: p.Fingerprint, DeclarationVersion: hookstate.DeclarationVersion, RequestID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", Confirmed: true}); err != nil {
		t.Fatal(err)
	}
	for _, e := range []activity.HostEvent{{Source: "claude", Kind: "SessionStart", SessionID: "s", CWD: cwd, SessionSource: "startup"}, {Source: "claude", Kind: "UserPromptSubmit", SessionID: "s", TurnID: "p", CWD: cwd}} {
		if _, err = d.Activity.IngestHost(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	return d, cwd, seconds, failed
}
func qaClaudeStopPayload(t *testing.T, cwd string) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{"hook_event_name": "Stop", "session_id": "s", "prompt_id": "p", "cwd": cwd, "stop_hook_active": false, "error_details": "SECRET_CONTENT"})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
func TestQAClaudeCLICommittedCallbackWakeAndReplay(t *testing.T) {
	path, socket := qaWorkerNotificationSocket(t)
	d, cwd, seconds, _ := qaClaudeWakeFixture(t, path)
	*seconds = 10
	payload := qaClaudeStopPayload(t, cwd)
	if diagnostic := qaClaudeRun(t, d, payload); diagnostic != "" {
		t.Fatalf("healthy diagnostic %q", diagnostic)
	}
	qaWorkerNotification(t, socket, "wake")
	snapshot, err := d.Activity.Status(context.Background())
	if err != nil || len(snapshot.ClosedIntervals) != 1 || snapshot.ClosedIntervals[0].DurationNS != "10000000000" {
		t.Fatalf("callback duration %+v %v", snapshot, err)
	}
	list, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{Source: "claude", SessionID: "s"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range list.Receipts {
		if r.Kind == "Stop" {
			found = true
			if r.Durability != "committed" || r.Actor == nil || r.Origin != "unverified" {
				t.Fatalf("wrong receipt %+v", r)
			}
		}
	}
	if !found {
		t.Fatal("missing durable Claude Stop")
	}
	beforeReceipts := list
	beforeStatus := snapshot
	assertReplayUnchanged := func() {
		t.Helper()
		afterReceipts, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{Source: "claude", SessionID: "s"})
		if err != nil || !reflect.DeepEqual(beforeReceipts, afterReceipts) {
			t.Fatal("replay changed durable Claude receipts", err)
		}
		afterStatus, err := d.Activity.Status(context.Background())
		if err != nil || !reflect.DeepEqual(beforeStatus, afterStatus) {
			t.Fatal("replay changed captured activity", err)
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("Claude replay created JSON authority", err)
		}
		entries, err := os.ReadDir(filepath.Dir(path))
		if err != nil {
			t.Fatal("read private SQLite directory", err)
		}
		foundDatabase := false
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), "activity-") || entry.IsDir() {
				continue
			}
			if strings.HasSuffix(entry.Name(), ".sqlite3") {
				foundDatabase = true
			}
			data, err := os.ReadFile(filepath.Join(filepath.Dir(path), entry.Name()))
			if err != nil {
				t.Fatal("read private SQLite file", err)
			}
			if bytes.Contains(data, []byte("SECRET_CONTENT")) {
				t.Fatal("raw Claude callback content persisted")
			}
		}
		if !foundDatabase {
			t.Fatal("Claude receipt has no SQLite authority")
		}
	}
	qaClaudeRun(t, d, payload)
	qaWorkerNotification(t, socket, "wake")
	assertReplayUnchanged()
	socket.Close()
	qaClaudeRun(t, d, payload)
	assertReplayUnchanged()
}
func TestQAClaudeCLIUnacceptedCallbacksDoNotWake(t *testing.T) {
	for _, mode := range []string{"validation", "untracked", "clock_error", "permission", "stale"} {
		t.Run(mode, func(t *testing.T) {
			path, socket := qaWorkerNotificationSocket(t)
			d, cwd, seconds, failed := qaClaudeWakeFixture(t, path)
			*seconds = 10
			payload := qaClaudeStopPayload(t, cwd)
			switch mode {
			case "validation":
				payload = "{"
			case "untracked":
				d.Activity = activity.New(activity.Options{Path: path + ".absent"})
			case "clock_error":
				*failed = true
			case "permission":
				b, _ := json.Marshal(map[string]any{"hook_event_name": "PermissionRequest", "session_id": "s", "prompt_id": "p", "cwd": cwd, "tool_name": "Read"})
				payload = string(b)
			case "stale":
				payload = strings.Replace(payload, `"prompt_id":"p"`, `"prompt_id":"unknown"`, 1)
			}
			diagnostic := qaClaudeRun(t, d, payload)
			qaWorkerNotification(t, socket, "")
			if mode == "clock_error" {
				if !strings.Contains(diagnostic, "clock_unavailable") || !strings.Contains(diagnostic, "durability=committed") {
					t.Fatalf("committed safety outcome %q", diagnostic)
				}
				*failed = false
				s, err := d.Activity.Status(context.Background())
				if err != nil || len(s.Uncertainties) != 1 || len(s.ClosedIntervals) != 0 {
					t.Fatalf("clock quarantine not committed %+v %v", s, err)
				}
			}
		})
	}
}
