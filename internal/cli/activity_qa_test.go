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
	"time"

	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
)

func qaRunActivity(t *testing.T, input string, args ...string) (result, string) {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "absent", "activity.json")
	config := filepath.Join(root, "broken-config.json")
	if err := os.WriteFile(config, []byte("PRIVATE-CONFIG-DO-NOT-READ"), 0600); err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{}
	api := &fakeAPI{}
	factories := 0
	var out, stderr bytes.Buffer
	d := cli.Dependencies{Store: store, ConfigPath: config, Getenv: func(k string) string {
		if k == "TEMPO_STATE" {
			return path
		}
		return ""
	}, Now: func() time.Time { return time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC) }, NewProvider: func(string, string) harvest.Provider { factories++; return api }}
	code := cli.Run(context.Background(), args, strings.NewReader(input), &out, &stderr, d)
	r := result{code: code, out: out.String(), err: stderr.String(), store: store, api: api, factories: factories}
	if store.gets+store.sets+store.deletes+factories != 0 || len(api.calls) != 0 {
		t.Fatalf("local activity accessed credentials/provider: %+v", r)
	}
	if strings.Contains(r.out+r.err, "PRIVATE-CONFIG") || strings.ContainsRune(r.out+r.err, '\x1b') {
		t.Fatalf("unsafe local output: %q %q", r.out, r.err)
	}
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("absent local operation created state path: %v", err)
	}
	return r, path
}

func TestQAActivityCLIOfflineSnapshotFiniteEnvelope(t *testing.T) {
	for _, flag := range []string{"--json", "--non-interactive"} {
		t.Run(flag, func(t *testing.T) {
			r, _ := qaRunActivity(t, "", "activity", "status", flag)
			envelope(t, r, 0, "")
			if r.err != "" {
				t.Fatalf("success polluted stderr: %q", r.err)
			}
			var v struct {
				Data map[string]json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal([]byte(r.out), &v); err != nil {
				t.Fatal(err)
			}
			if string(v.Data["computer_id"]) != "null" || string(v.Data["snapshot_revision"]) != "\"0\"" {
				t.Fatalf("bad initial state: %s", r.out)
			}
			for _, key := range []string{"projects", "actors", "uncertainties", "closed_intervals"} {
				if string(v.Data[key]) != "[]" {
					t.Fatalf("%s must be [], got %s", key, v.Data[key])
				}
			}
		})
	}
}

const qaEventJSON = `{"contract_version":1,"actor":{"computer_id":"11111111-1111-4111-8111-111111111111","source":"manual-test","session_id":"qa-session","agent_id":"A"},"generation":"1","sequence":"1","event_id":"qa-event-1","kind":"work"}`

func TestQAActivityCLIUnlinkedEventNeverBootstrapsIdentity(t *testing.T) {
	r, _ := qaRunActivity(t, qaEventJSON, "activity", "event", "--input-stdin", "--json")
	envelope(t, r, 0, "")
	var v struct {
		Data struct {
			Disposition string `json:"disposition"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(r.out), &v); err != nil {
		t.Fatal(err)
	}
	if v.Data.Disposition != "untracked" {
		t.Fatalf("unlinked event disposition=%q", v.Data.Disposition)
	}
}

func TestQAActivityCLIStrictInputBeforeMutation(t *testing.T) {
	for _, tc := range []struct{ name, input, code string }{
		{"unknown-field", strings.TrimSuffix(qaEventJSON, "}") + `,"prompt":"PRIVATE-PAYLOAD"}`, "validation"},
		{"duplicate-key", strings.Replace(qaEventJSON, `"kind":"work"`, `"kind":"work","kind":"finish"`, 1), "validation"},
		{"case-variant", strings.Replace(qaEventJSON, `"kind"`, `"Kind"`, 1), "validation"},
		{"trailing-object", qaEventJSON + ` {}`, "validation"},
		{"invalid-utf8", strings.Replace(qaEventJSON, "qa-session", string([]byte{0xff}), 1), "validation"},
		{"future-version", strings.Replace(qaEventJSON, `"contract_version":1`, `"contract_version":2`, 1), "unsupported_contract"},
		{"noncanonical-counter", strings.Replace(qaEventJSON, `"sequence":"1"`, `"sequence":"01"`, 1), "validation"},
		{"counter-overflow", strings.Replace(qaEventJSON, `"sequence":"1"`, `"sequence":"18446744073709551616"`, 1), "validation"},
		{"oversized", strings.Replace(qaEventJSON, "qa-event-1", strings.Repeat("X", 16*1024), 1), "validation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := qaRunActivity(t, tc.input, "activity", "event", "--input-stdin", "--json")
			envelope(t, r, 2, tc.code)
			if strings.Contains(r.err, "PRIVATE-PAYLOAD") {
				t.Fatal("raw payload leaked into error")
			}
		})
	}
}

func TestQAActivityCLIHelpStillSelectsHelpAfterCatalogExpansion(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"activity", "status", "--help"}, {"time", "create", "--help"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			r, _ := qaRunActivity(t, "", args...)
			if r.code != 0 || r.err != "" || !strings.Contains(r.out, "Usage: tempo") || !strings.Contains(r.out, "activity status") || !strings.Contains(r.out, "timer status") {
				t.Fatalf("help command selection broke: exit=%d out=%q err=%q", r.code, r.out, r.err)
			}
		})
	}
}

func TestQAActivityCLIForcingDoesNotInterpretHarvestNoteAsCommand(t *testing.T) {
	r := run(t, &fakeAPI{}, "time", "create", "--project", "100", "--task", "200", "--date", "2026-10-02", "--duration", "1m", "--notes", "activity", "--non-interactive")
	if r.code != 0 {
		t.Fatalf("ordinary Harvest command failed: %s", r.err)
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal([]byte(r.out), &data); err != nil {
		t.Fatal(err)
	}
	if _, ok := data["schema_version"]; ok {
		t.Fatalf("literal note changed existing Harvest output mode: %s", r.out)
	}
	if _, ok := data["id"]; !ok {
		t.Fatalf("ordinary Harvest result missing: %s", r.out)
	}
}
