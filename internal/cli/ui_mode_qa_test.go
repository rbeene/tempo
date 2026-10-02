package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/worker"
)

// These tests intentionally use the existing public CLI and a real absent store.
// They require no dashboard scaffolding and never acquire a native terminal.
type qaUIModeFixture struct {
	deps         cli.Dependencies
	statusReads  int
	observations int
}

func qaNewUIModeFixture(t *testing.T) *qaUIModeFixture {
	t.Helper()
	f := &qaUIModeFixture{}
	root := t.TempDir()
	path := filepath.Join(root, "absent", "activity.json")
	config := filepath.Join(root, "malformed-config")
	const configBytes = "PRIVATE-UI-CONFIG-not-json"
	if err := os.WriteFile(config, []byte(configBytes), 0600); err != nil {
		t.Fatal(err)
	}
	getenv := func(k string) string {
		switch k {
		case "TEMPO_STATE":
			return path
		case "TEMPO_CONFIG":
			return config
		default:
			return ""
		}
	}
	provider := func(string, string) harvest.Provider {
		t.Fatal("finite local mode constructed an authenticated provider")
		return nil
	}
	fixed := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	act := activity.New(activity.Options{Path: path, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		f.statusReads++
		return activity.ClockSample{Capability: "unavailable", WallUTC: fixed}, nil
	}), ObserveWorker: func(_ context.Context, base activity.WorkerStatus) activity.WorkerStatus {
		f.observations++
		return base
	}})
	workerActions := &qaWorkerCLIActions{t: t}
	w, err := worker.New(worker.Options{StatePath: path, ConfigPath: config, Executable: filepath.Join(root, "tempo"), ServiceDir: filepath.Join(root, "services"), Platform: "darwin", UID: 501, Sync: workerActions, Runner: qaWorkerCLIRunner{t}})
	if err != nil {
		t.Fatal(err)
	}
	f.deps = cli.Dependencies{
		Activity: act, Worker: w, Store: qaForbiddenLegacyStore{t}, ConfigPath: config,
		Getenv: getenv, NewProvider: provider, Now: func() time.Time { return fixed },
		Prompter: qaForbiddenPrompt{t}, SaveConfig: func(string, auth.Config) error {
			t.Fatal("finite local mode saved account configuration")
			return nil
		},
		Auth: auth.NewService(auth.Options{ConfigPath: config, LockPath: filepath.Join(root, "auth.lock"), Getenv: getenv, NewProvider: provider,
			PersistentAvailable: func() bool { t.Fatal("finite local mode inspected credential capability"); return false },
			Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
				t.Fatal("finite local mode accessed native credentials")
				return auth.NativeReply{}, nil
			}),
		}),
	}
	t.Cleanup(func() {
		if workerActions.reads != 0 {
			t.Errorf("finite local mode called worker controller status %d times", workerActions.reads)
		}
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 1 || entries[0].Name() != "malformed-config" {
			t.Errorf("finite invocation created state/identity/lock/service files: entries=%v err=%v", entries, err)
		}
		got, err := os.ReadFile(config)
		if err != nil || string(got) != configBytes {
			t.Errorf("finite invocation changed configuration: %q err=%v", got, err)
		}
	})
	return f
}

func qaUIModeRun(t *testing.T, f *qaUIModeFixture, args []string) result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var out, stderr bytes.Buffer
	code := cli.Run(ctx, args, qaNeverRead{t}, &out, &stderr, f.deps)
	if ctx.Err() != nil {
		t.Fatal("finite invocation waited for its safety deadline instead of returning one snapshot")
	}
	if strings.ContainsRune(out.String()+stderr.String(), '\x1b') || strings.Contains(out.String()+stderr.String(), "PRIVATE-UI-CONFIG") {
		t.Fatalf("finite output contained terminal controls or private config: out=%q err=%q", out.String(), stderr.String())
	}
	return result{code: code, out: out.String(), err: stderr.String()}
}

func qaUIModeEnvelope(t *testing.T, text string) map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var got map[string]any
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("expected one JSON envelope, got %q: %v", text, err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		t.Fatalf("output was not exactly one JSON envelope: trailing=%v err=%v", extra, err)
	}
	if got["schema_version"] != json.Number("1") {
		t.Fatalf("wrong envelope version: %v", got)
	}
	return got
}

func TestQAUIModeFiniteSnapshotParity(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		forced bool
	}{
		{"default", nil, false},
		{"ui", []string{"ui"}, false},
		{"default-json", []string{"--json"}, true},
		{"default-noninteractive", []string{"--non-interactive"}, true},
		{"ui-json", []string{"ui", "--json"}, true},
		{"ui-noninteractive", []string{"ui", "--non-interactive"}, true},
		{"ui-forcing-before-command", []string{"--non-interactive", "ui"}, true},
		{"watch-redirected", []string{"activity", "status", "--watch"}, false},
		{"watch-json", []string{"activity", "status", "--watch", "--json"}, true},
		{"watch-noninteractive", []string{"activity", "status", "--watch", "--non-interactive"}, true},
		{"watch-forcing-before-command", []string{"--json", "activity", "status", "--watch"}, true},
	}
	for _, mode := range []string{"real-nonTTY-streams", "eligible-false", "eligible-true"} {
		for _, tc := range cases {
			if mode == "eligible-true" && !tc.forced {
				continue // Actual interactive terminal ownership belongs to the PTY slice.
			}
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				f := qaNewUIModeFixture(t)
				if mode != "real-nonTTY-streams" {
					f.deps.TerminalEligible = func(io.Reader, io.Writer) bool { return mode == "eligible-true" }
				}
				baseline := qaUIModeRun(t, f, []string{"activity", "status", "--json"})
				if baseline.code != 0 || baseline.err != "" {
					t.Fatalf("existing status baseline failed: %+v", baseline)
				}
				want := qaUIModeEnvelope(t, baseline.out)
				data, ok := want["data"].(map[string]any)
				if !ok || data["computer_id"] != nil || data["snapshot_revision"] != "0" || data["observed_at"] != "2026-10-02T09:00:00Z" {
					t.Fatalf("invalid independent fixed-clock empty-state baseline: %v", want)
				}
				f.statusReads, f.observations = 0, 0
				got := qaUIModeRun(t, f, tc.args)
				if got.code != 0 || got.err != "" {
					t.Fatalf("finite snapshot exit=%d stdout=%q stderr=%q", got.code, got.out, got.err)
				}
				if actual := qaUIModeEnvelope(t, got.out); !reflect.DeepEqual(actual, want) {
					t.Errorf("finite alias differs from activity status: got=%v want=%v", actual, want)
				}
				if f.statusReads != 1 || f.observations != 1 {
					t.Errorf("finite alias must project exactly once: status reads=%d worker observations=%d", f.statusReads, f.observations)
				}
			})
		}
	}
}

func TestQAUIModeExistingOfflineControls(t *testing.T) {
	for _, args := range [][]string{{"activity", "status", "--json"}, {"activity", "status", "--non-interactive"}, {"help"}, {"--help"}, {"schema"}, {"version"}, {"--version"}} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			f := qaNewUIModeFixture(t)
			f.deps.TerminalEligible = func(io.Reader, io.Writer) bool { return true }
			got := qaUIModeRun(t, f, args)
			if got.code != 0 || got.err != "" || got.out == "" {
				t.Fatalf("offline control failed: %+v", got)
			}
			wantReads := 0
			switch args[0] {
			case "activity":
				wantReads = 1
				qaUIModeEnvelope(t, got.out)
			case "schema":
				qaUIModeEnvelope(t, got.out)
			case "help", "--help":
				if !strings.Contains(got.out, "Usage: tempo") {
					t.Errorf("help lost usage: %q", got.out)
				}
			case "version", "--version":
				if !strings.HasPrefix(got.out, "tempo ") {
					t.Errorf("version lost identity: %q", got.out)
				}
			}
			if f.statusReads != wantReads || f.observations != wantReads {
				t.Errorf("offline control reads=%d observations=%d want=%d", f.statusReads, f.observations, wantReads)
			}
		})
	}
}

func TestQAUIModeUsageBeforeDependencyAccess(t *testing.T) {
	for _, args := range [][]string{
		{"status", "--json"},
		{"ui", "unexpected", "--json"},
		{"ui", "--unknown-option", "--json"},
		{"ui", "--watch", "--json"},
		{"activity", "status", "--watch=true", "--json"},
		{"activity", "status", "--watch", "--watch", "--json"},
		{"timer", "status", "--watch", "--json"},
		{"version", "--watch", "--json"},
	} {
		t.Run(fmt.Sprint(args), func(t *testing.T) {
			f := qaNewUIModeFixture(t)
			f.deps.Getenv = func(string) string { t.Fatal("invalid invocation accessed environment"); return "" }
			got := qaUIModeRun(t, f, args)
			if got.code != 2 || got.out != "" {
				t.Fatalf("usage must exit 2 with empty stdout: %+v", got)
			}
			v := qaUIModeEnvelope(t, got.err)
			e, ok := v["error"].(map[string]any)
			if !ok || e["code"] != "usage" || v["data"] != nil {
				t.Errorf("wrong usage envelope: %v", v)
			}
			if f.statusReads != 0 || f.observations != 0 {
				t.Errorf("invalid invocation accessed snapshot: %d/%d", f.statusReads, f.observations)
			}
		})
	}
}
