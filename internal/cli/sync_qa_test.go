package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
)

func qaSyncCLIDeps(t *testing.T) (cli.Dependencies, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "activity", "state.json")
	cfg := filepath.Join(dir, "malformed-config")
	if err := os.WriteFile(cfg, []byte("not configuration JSON"), 0600); err != nil {
		t.Fatal(err)
	}
	a := auth.NewService(auth.Options{ConfigPath: cfg, LockPath: filepath.Join(dir, "auth.lock"), Getenv: func(string) string { return "" }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Fatal("finite sync accessed credentials")
		return auth.NativeReply{}, nil
	})})
	return cli.Dependencies{Activity: activity.New(activity.Options{Path: path}), Auth: a, ConfigPath: cfg, Getenv: func(string) string { return "" }, Prompter: qaForbiddenPrompt{t}, TerminalEligible: func(io.Reader, io.Writer) bool { return false }, NewProvider: func(string, string) harvest.Provider {
		t.Fatal("offline/invalid sync constructed provider")
		return nil
	}}, path
}
func qaSyncCLIEnvelope(t *testing.T, args []string, d cli.Dependencies, wantExit int) map[string]any {
	t.Helper()
	var out, stderr bytes.Buffer
	code := cli.Run(context.Background(), args, strings.NewReader(""), &out, &stderr, d)
	if code != wantExit {
		t.Fatalf("args%v exit%d want%d stdout=%q stderr=%q", args, code, wantExit, out.String(), stderr.String())
	}
	raw := &out
	if wantExit != 0 {
		if out.Len() != 0 {
			t.Fatal("failure polluted stdout")
		}
		raw = &stderr
	} else if stderr.Len() != 0 {
		t.Fatal("success polluted stderr")
	}
	dec := json.NewDecoder(raw)
	var v map[string]any
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("not machine envelope: %v", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		t.Fatalf("more than one envelope: %v", err)
	}
	if v["schema_version"] != float64(1) {
		t.Fatalf("schema=%v", v)
	}
	return v
}
func TestQASyncCLIOfflineStatusAndAbsentControls(t *testing.T) {
	for _, action := range []string{"status", "pause", "resume"} {
		t.Run(action, func(t *testing.T) {
			d, path := qaSyncCLIDeps(t)
			want := 0
			if action != "status" {
				want = 2
			}
			args := []string{"sync", action, "--json"}
			if action != "status" {
				args = append(args, "--request-id", "90000000-0000-4000-8000-000000000091")
			}
			v := qaSyncCLIEnvelope(t, args, d, want)
			if action != "status" {
				e, _ := v["error"].(map[string]any)
				if e["code"] != "input_required" {
					t.Fatalf("absent mutation error=%v", v)
				}
			}
			if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
				t.Fatalf("offline command initialized store: %v", err)
			}
		})
	}
}
func TestQASyncCLIInvalidInputsStayFiniteAndMachineReadable(t *testing.T) {
	for _, args := range [][]string{
		{"sync", "now", "--limit", "0"},
		{"sync", "now", "--limit", "101"},
		{"sync", "now", "--account", "1"},
		{"sync", "now", "--request-id", "invalid"},
		{"sync", "reconcile", "--limit", "0"},
		{"sync", "resolve", "root", "--entry", "901", "--retry-rejected", "--yes"},
		{"sync", "configure", "--account", "1", "--unknown"},
		{"sync", "status", "--unknown"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			d, _ := qaSyncCLIDeps(t)
			qaSyncCLIEnvelope(t, args, d, 2)
		})
	}
}
