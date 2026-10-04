//go:build (darwin || linux) && (amd64 || arm64)

package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	_ "unsafe" // The existing private inert sqliteio fault hook, used only in the last test.

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
)

func startContextQARun(t *testing.T, d cli.Dependencies, host, input string, flags ...string) (string, string) {
	t.Helper()
	var out, diagnostic bytes.Buffer
	args := append([]string{"hook", host, "--input-stdin"}, flags...)
	if cli.Run(context.Background(), args, strings.NewReader(input), &out, &diagnostic, d) != 0 {
		t.Fatal("capture diagnostic vetoed native host")
	}
	if out.Len() > 512 || diagnostic.Len() > 256 || strings.Contains(out.String()+diagnostic.String(), "SECRET") {
		t.Fatal("capture diagnostic exceeded its fixed/redacted contract")
	}
	return out.String(), diagnostic.String()
}

func startContextQAExpect(t *testing.T, out, diagnostic, kind, code, durability string) {
	t.Helper()
	text := "tempo capture: kind=" + kind + "; code=" + code + "; durability=" + durability
	want := map[string]any{"hookSpecificOutput": map[string]string{"hookEventName": kind, "additionalContext": text}}
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatal("cannot encode fixed expected native context")
	}
	if out != string(b)+"\n" {
		t.Error("start diagnostic did not emit the exact bounded native context object")
	}
	if diagnostic != "tempo hook: "+code+"; durability="+durability+"\n" {
		t.Fatal("start diagnostic changed the fixed stderr or durability result")
	}
}

func TestQAHostStartContextUnlinkedKindsAndMachineFlags(t *testing.T) {
	for _, kind := range []string{"SessionStart", "SubagentStart"} {
		for _, mode := range []string{"plain", "json", "noninteractive"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				root := t.TempDir()
				path := filepath.Join(root, "absent", "activity.json")
				store := &fakeStore{}
				d := cli.Dependencies{Activity: activity.New(activity.Options{Path: path}), Store: store,
					Getenv:      func(string) string { return "" },
					NewProvider: func(string, string) harvest.Provider { t.Fatal("hook touched provider"); return nil }}
				payload := map[string]any{"hook_event_name": kind, "source": "startup", "session_id": "SECRET_SESSION", "turn_id": "SECRET_TURN", "agent_id": "SECRET_AGENT", "cwd": "/SECRET_UNLINKED", "prompt": "SECRET_PROMPT"}
				b, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				var flags []string
				if mode == "json" {
					flags = []string{"--json"}
				}
				if mode == "noninteractive" {
					flags = []string{"--non-interactive"}
				}
				out, diagnostic := startContextQARun(t, d, "codex", string(b), flags...)
				startContextQAExpect(t, out, diagnostic, kind, "untracked", "not_committed")
				if store.gets+store.sets+store.deletes != 0 {
					t.Fatal("hook touched credentials")
				}
				if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("unlinked diagnostic initialized state")
				}
			})
		}
	}
}

func TestQAHostStartContextDoesNotExpandOtherProtocols(t *testing.T) {
	for _, tc := range []struct{ name, host, input, output string }{
		{"malformed", "codex", `{"hook_event_name":"SessionStart","SECRET":`, "{}\n"},
		{"invalid_start", "codex", `{"hook_event_name":"SubagentStart","session_id":"s","turn_id":"t","cwd":"/SECRET"}`, "{}\n"},
		{"prompt", "codex", qaHookJSON, "{}\n"},
		{"claude", "claude", `{"hook_event_name":"SessionStart","session_id":"s","cwd":"/SECRET","source":"startup"}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := cli.Dependencies{Activity: activity.New(activity.Options{Path: filepath.Join(t.TempDir(), "absent", "activity.json")}), Getenv: func(string) string { return "" }}
			out, _ := startContextQARun(t, d, tc.host, tc.input)
			if out != tc.output {
				t.Fatal("context escaped the two validated Codex start kinds")
			}
		})
	}
}

func TestQAHostStartContextReportsActualCommittedReview(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path, err := activity.ResolveStatePath(filepath.Join(dir, "activity.json"))
	if err != nil {
		t.Fatal(err)
	}
	d, cwd, seconds, failed := qaHostWakeFixture(t, path)
	*seconds, *failed = 10, true
	b, err := json.Marshal(map[string]any{"hook_event_name": "SessionStart", "session_id": "s", "source": "resume", "cwd": cwd})
	if err != nil {
		t.Fatal(err)
	}
	out, diagnostic := startContextQARun(t, d, "codex", string(b))
	startContextQAExpect(t, out, diagnostic, "SessionStart", "clock_unavailable", "committed")
	*failed = false
	snapshot, err := d.Activity.Status(context.Background())
	if err != nil || len(snapshot.Uncertainties) != 1 || len(snapshot.ClosedIntervals) != 0 {
		t.Fatal("committed diagnostic lacked its actual safety quarantine")
	}
	rows, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{Source: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, r := range rows.Receipts {
		if r.Kind == "SessionStart" && r.DiagnosticCode == "clock_unavailable" {
			found++
			if r.Durability != "committed" || r.Disposition != "review_required" {
				t.Fatal("context misrepresented retained review")
			}
		}
	}
	if found != 1 {
		t.Fatal("actual review receipt missing or duplicated")
	}
}

// Exact existing sqliteio test ABI. No fake SQLite result or new producer seam.
type startContextQASQLEvent struct {
	Phase, Operation string
	Code             int32
}
type startContextQASQLHooks struct {
	Observe func(startContextQASQLEvent)
	Fault   func(startContextQASQLEvent) error
}

//go:linkname startContextQASetSQLHooks github.com/rbeene/tempo/internal/activity/sqliteio.setSQLHooksForTest
func startContextQASetSQLHooks(startContextQASQLHooks)

func TestQAHostStartContextUnknownThenActualCleanReplay(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path, err := activity.ResolveStatePath(filepath.Join(dir, "activity.json"))
	if err != nil {
		t.Fatal(err)
	}
	d, cwd, seconds, _ := qaHostWakeFixture(t, path)
	*seconds = 10
	b, err := json.Marshal(map[string]any{"hook_event_name": "SubagentStart", "session_id": "s", "turn_id": "child", "agent_id": "child-agent", "cwd": cwd})
	if err != nil {
		t.Fatal(err)
	}
	hits := 0
	startContextQASetSQLHooks(startContextQASQLHooks{Fault: func(e startContextQASQLEvent) error {
		if e.Phase == "durable-native-closed" && e.Code == 0 && hits == 0 {
			hits++
			return errors.New("SECRET native close detail")
		}
		return nil
	}})
	t.Cleanup(func() { startContextQASetSQLHooks(startContextQASQLHooks{}) })
	out, diagnostic := startContextQARun(t, d, "codex", string(b))
	startContextQASetSQLHooks(startContextQASQLHooks{})
	if hits != 1 {
		t.Fatal("actual durable-close fault was not reached")
	}
	startContextQAExpect(t, out, diagnostic, "SubagentStart", "local_write_unknown", "unknown")
	cold := activity.New(activity.Options{Path: path, LockTimeout: sqliteFlowTestLockTimeout()})
	before, err := cold.HostReceipts(context.Background(), activity.HostReceiptFilter{Source: "codex"})
	if err != nil || len(before.Receipts) != 3 {
		t.Fatal("fault did not preserve one actual committed child receipt")
	}
	var child activity.HostReceipt
	for _, r := range before.Receipts {
		if r.Kind == "SubagentStart" {
			child = r
		}
	}
	if child.ID == "" || child.Durability != "committed" || child.Disposition != "applied" {
		t.Fatal("cold read lost committed child outcome")
	}
	out, diagnostic = startContextQARun(t, d, "codex", string(b))
	if out != "{}\n" || diagnostic != "" {
		t.Fatal("clean exact replay emitted model context")
	}
	after, err := cold.HostReceipts(context.Background(), activity.HostReceiptFilter{Source: "codex"})
	if err != nil || len(after.Receipts) != len(before.Receipts) {
		t.Fatal("exact replay fabricated another capture receipt")
	}
	found := 0
	for _, r := range after.Receipts {
		if r.Kind == "SubagentStart" {
			found++
			if r.ID != child.ID || r.SnapshotRevision != child.SnapshotRevision {
				t.Fatal("replay changed immutable child receipt")
			}
		}
	}
	if found != 1 {
		t.Fatal("replay duplicated child receipt")
	}
}
