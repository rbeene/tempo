//go:build (darwin || linux) && (amd64 || arm64)

package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hooks"
	"github.com/rbeene/tempo/internal/hookstate"
)

type csQAFixture struct {
	t                                  *testing.T
	ctx                                context.Context
	cwd, path                          string
	base                               time.Time
	seconds                            atomic.Int64
	offline, captured                  atomic.Bool
	credentials, providers, nativeAuth atomic.Int64
	store                              *fakeStore
	policies                           *hookstate.Service
	auth                               *auth.Service
	server                             *httptest.Server
	mu                                 sync.Mutex
	requests                           []string
	posts, getReads                    int
	claim                              activity.SyncStatus
}

func csQAID(n int) string { return fmt.Sprintf("db000000-0000-4000-8000-%012d", n) }

func csQANew(t *testing.T) *csQAFixture {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "tempo-cli-sync-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Error("owned fixture cleanup", err)
		}
	})
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	q := &csQAFixture{t: t, ctx: ctx, cwd: filepath.Join(canonical, "project"), path: filepath.Join(canonical, "activity", "state.json"), base: time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC), store: &fakeStore{}, requests: []string{}}
	if err := os.Mkdir(q.cwd, 0700); err != nil {
		t.Fatal(err)
	}
	q.policies = hookstate.New(hookstate.Options{Path: filepath.Join(canonical, "policy", "hooks.json")})
	q.server = httptest.NewServer(http.HandlerFunc(q.http))
	t.Cleanup(q.server.Close)
	q.auth = auth.NewService(auth.Options{ConfigPath: filepath.Join(canonical, "unused-config.json"), LockPath: filepath.Join(canonical, "unused-auth.lock"), Getenv: q.getenv, NewProvider: q.provider, PersistentAvailable: func() bool { return false }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		q.nativeAuth.Add(1)
		return auth.NativeReply{}, errors.New("native credential access forbidden")
	})})
	if _, err := os.Lstat(filepath.Dir(q.path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("fixture initialized activity storage", err)
	}
	return q
}

func (q *csQAFixture) service() *activity.Service {
	return activity.NewSQLite(activity.Options{Path: q.path, LockTimeout: sqliteFlowTestLockTimeout(), HookPolicies: q.policies, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		n := q.seconds.Load()
		epoch, ns := "cli-sync-boot", strconv.FormatInt(n*int64(time.Second), 10)
		return activity.ClockSample{Capability: "available", WallUTC: q.base.Add(time.Duration(n) * time.Second), Epoch: &epoch, ElapsedNS: &ns, AwakeNS: &ns}, nil
	})})
}

func (q *csQAFixture) getenv(key string) string {
	if key == "HARVEST_TOKEN" {
		q.credentials.Add(1)
		if q.offline.Load() {
			q.t.Error("offline CLI operation accessed credentials")
		}
		return "synthetic-cli-sync-token"
	}
	return "" // No personal environment, config lookup or worker notification.
}

func (q *csQAFixture) provider(token, account string) harvest.Provider {
	q.providers.Add(1)
	if q.offline.Load() || token != "synthetic-cli-sync-token" || account != "11" {
		q.t.Error("unexpected credential/provider scope")
		return nil
	}
	client := q.server.Client()
	client.Timeout = 3 * time.Second
	return harvest.NewWithHTTP(token, account, q.server.URL+"/v2", q.server.URL+"/id", client)
}

func (q *csQAFixture) deps() cli.Dependencies {
	return cli.Dependencies{Activity: q.service(), Auth: q.auth, Store: q.store, Hooks: q.policies, Getenv: q.getenv, NewProvider: q.provider, Now: func() time.Time { return q.base.Add(time.Duration(q.seconds.Load()) * time.Second) }}
}

// Returns errors instead of Fatal so the HTTP server goroutine always returns.
// Each call constructs a new private SQL Service against the same durable path.
func (q *csQAFixture) runJSON(ctx context.Context, args []string, target any) error {
	var out, stderr bytes.Buffer
	argv := append(append([]string{}, args...), "--json", "--non-interactive")
	code := cli.Run(ctx, argv, bytes.NewReader(nil), &out, &stderr, q.deps())
	if code != 0 || stderr.Len() != 0 {
		return fmt.Errorf("CLI %s failed exit=%d (output suppressed)", strings.Join(args[:min(2, len(args))], " "), code)
	}
	var envelope struct {
		SchemaVersion int             `json:"schema_version"`
		Data          json.RawMessage `json:"data"`
	}
	dec := json.NewDecoder(&out)
	if err := dec.Decode(&envelope); err != nil || envelope.SchemaVersion != 1 || len(envelope.Data) == 0 {
		return errors.New("invalid CLI data envelope")
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("CLI emitted multiple envelopes")
	}
	if err := json.Unmarshal(envelope.Data, target); err != nil {
		return errors.New("invalid typed CLI data")
	}
	if _, err := os.Lstat(q.path); !errors.Is(err, os.ErrNotExist) {
		return errors.New("SQL CLI created/adopted JSON authority")
	}
	return nil
}

func (q *csQAFixture) json(args []string, target any) {
	q.t.Helper()
	if err := q.runJSON(q.ctx, args, target); err != nil {
		q.t.Fatal(err)
	}
}

func (q *csQAFixture) profile() {
	q.t.Helper()
	c := hookstate.Context{Host: "codex", Scope: "project", Path: q.cwd, RuntimeVersion: "0.159.3", Surface: "local", Conflicts: []string{}}
	for _, role := range []string{"runtime", "executable", "definitions"} {
		path := filepath.Join(q.cwd, role)
		if err := os.WriteFile(path, []byte("synthetic CLI sync "+role), 0600); err != nil {
			q.t.Fatal(err)
		}
		c.Artifacts = append(c.Artifacts, hookstate.Artifact{Role: role, Path: path})
	}
	p, err := q.policies.Preview(q.ctx, c)
	if err != nil {
		q.t.Fatal(err)
	}
	r, err := q.policies.Confirm(q.ctx, hookstate.ConfirmInput{Context: p.Context, Fingerprint: p.Fingerprint, DeclarationVersion: hookstate.DeclarationVersion, RequestID: csQAID(6), Confirmed: true})
	if err != nil || !r.CaptureEligible || r.Basis != "operator_declared" {
		q.t.Fatal("synthetic profile prerequisite", err)
	}
}

func (q *csQAFixture) hook(at int64, kind, turn, agent string) {
	q.t.Helper()
	q.seconds.Store(at)
	v := map[string]any{"hook_event_name": kind, "session_id": "cli-sync-session", "cwd": q.cwd}
	if kind == "SessionStart" {
		v["source"] = "startup"
	} else {
		v["turn_id"] = turn
	}
	if agent != "" {
		v["agent_id"] = agent
	}
	if kind == "Stop" || kind == "SubagentStop" {
		v["stop_hook_active"] = false
	}
	raw, err := json.Marshal(v)
	if err != nil {
		q.t.Fatal(err)
	}
	if _, err := hooks.DecodeCodex(bytes.NewReader(raw)); err != nil {
		q.t.Fatal("actual Codex decoder refused fixture", err)
	}
	var out, stderr bytes.Buffer
	code := cli.Run(q.ctx, []string{"hook", "codex", "--input-stdin"}, bytes.NewReader(raw), &out, &stderr, q.deps())
	if code != 0 || out.String() != "{}\n" || stderr.Len() != 0 {
		q.t.Fatal("CLI hook failed; host output must be the sole empty object")
	}
	if _, err := os.Lstat(q.path); !errors.Is(err, os.ErrNotExist) {
		q.t.Fatal("hook created/adopted JSON authority")
	}
}

func (q *csQAFixture) http(w http.ResponseWriter, r *http.Request) {
	refuse := func(message string) { q.t.Error(message); http.Error(w, "fixture refused", http.StatusBadRequest) }
	q.mu.Lock()
	q.requests = append(q.requests, r.Method+" "+r.URL.RequestURI())
	q.mu.Unlock()
	if q.offline.Load() || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "Bearer synthetic-cli-sync-token" {
		refuse("unexpected HTTP scope or offline access")
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v2/") && r.Header.Get("Harvest-Account-Id") != "11" {
		refuse("HTTP account was not the saved current-user account")
		return
	}
	if r.URL.Path == "/id/accounts" && r.Header.Get("Harvest-Account-Id") != "" {
		refuse("account discovery carried an account header")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet {
		var body string
		switch r.URL.Path {
		case "/id/accounts":
			body = `{"accounts":[{"id":11,"product":"harvest","is_active":true}]}`
		case "/v2/users/me":
			body = `{"id":7,"is_active":true,"timezone":"UTC"}`
		case "/v2/users/me/project_assignments":
			body = `{"project_assignments":[{"is_active":true,"project":{"id":100},"task_assignments":[{"is_active":true,"task":{"id":200}}]}],"links":{"next":null}}`
		case "/v2/company":
			body = `{"is_active":true,"wants_timestamp_timers":false}`
		default:
			refuse("unexpected HTTP discovery endpoint")
			return
		}
		// A separate public SQL read must finish while the provider GET is open.
		// Before the first Link it observes absence; later it observes real rows.
		status, err := q.service().Status(r.Context())
		if err != nil {
			refuse("provider GET retained SQL ownership")
			return
		}
		if q.captured.Load() && (len(status.ClosedIntervals) != 1 || status.ClosedIntervals[0].DurationNS != "36000000000") {
			refuse("provider GET lost the committed captured union")
			return
		}
		q.mu.Lock()
		q.getReads++
		q.mu.Unlock()
		if _, err := io.WriteString(w, body); err != nil {
			q.t.Error("mock GET response write failed")
		}
		return
	}
	if r.Method != http.MethodPost || r.URL.Path != "/v2/time_entries" || r.Header.Get("Content-Type") != "application/json" {
		refuse("unexpected HTTP mutation")
		return
	}
	q.mu.Lock()
	q.posts++
	n := q.posts
	q.mu.Unlock()
	if n != 1 {
		refuse("duplicate completed-entry POST")
		return
	}
	var payload map[string]any
	dec := json.NewDecoder(io.LimitReader(r.Body, 64*1024))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		refuse("malformed POST body")
		return
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		refuse("multiple POST bodies")
		return
	}
	var claim activity.SyncStatus
	if err := q.runJSON(r.Context(), []string{"sync", "status"}, &claim); err != nil {
		refuse("POST could not cold-read real SQL submitting plan")
		return
	}
	if !claim.Enabled || claim.Worker.SubmittingCount != 1 || len(claim.Items) != 1 {
		refuse("POST preceded the durable submitting root")
		return
	}
	item := claim.Items[0]
	if item.State != "submitting" || item.RunRequestID == nil || *item.RunRequestID != csQAID(4) || item.Plan == nil || len(item.Plan.Parts) != 1 {
		refuse("POST has no owned immutable plan")
		return
	}
	p := item.Plan.Parts[0]
	if p.State != "submitting" || p.ID == "" || p.Correlation != "tempo:v1:"+p.ID || p.Notes == "" || p.PlannedHours != "0.01" || p.DurationNS != "36000000000" || p.SpentDate != "2026-10-03" || len(p.Attempts) != 1 || p.Attempts[0].ID == "" || p.Attempts[0].RequestID != csQAID(4) || p.Attempts[0].Number != "1" || p.Attempts[0].State != "submitting" {
		refuse("POST plan/attempt facts are not frozen")
		return
	}
	want := map[string]any{"user_id": json.Number("7"), "project_id": json.Number("100"), "task_id": json.Number("200"), "spent_date": "2026-10-03", "hours": json.Number("0.01"), "notes": p.Notes, "external_reference": map[string]any{"id": p.Correlation, "group_id": item.ID, "account_id": item.Interval.ComputerID}}
	if !reflect.DeepEqual(payload, want) {
		refuse("wire payload differs from complete frozen duration plan")
		return
	}
	q.mu.Lock()
	q.claim = claim
	q.mu.Unlock()
	// A completed entry is inactive. Echo the exact correlation and amount so
	// this workflow tests HTTP/ACK composition without a rounding discrepancy.
	response := map[string]any{"id": json.Number("901"), "user": map[string]any{"id": json.Number("7")}, "project": map[string]any{"id": json.Number("100")}, "task": map[string]any{"id": json.Number("200")}, "spent_date": "2026-10-03", "hours": json.Number("0.01"), "rounded_hours": json.Number("0.01"), "notes": payload["notes"], "external_reference": payload["external_reference"], "is_running": false}
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		q.t.Error("mock completed-entry response write failed")
	}
}
