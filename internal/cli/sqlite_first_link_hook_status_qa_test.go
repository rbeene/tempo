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
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hooks"
	"github.com/rbeene/tempo/internal/hookstate"
)

// This exercises the private SQL service through the real embedded CLI. The
// payloads are native-shaped fixtures, not evidence of installed host delivery.
func TestSQLiteFirstLinkHookStatusCLIFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The coordinator's TMPDIR can be under the repository. Use the existing
	// binding QA convention so these are two independent directory locations.
	fixtureRoot, err := os.MkdirTemp("/tmp", "tempo-sqlite-first-flow-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(fixtureRoot); err != nil {
			t.Error("owned fixture cleanup", err)
		}
	})
	root, err := filepath.EvalSymlinks(fixtureRoot)
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "activity", "state.json")
	cwds := []string{filepath.Join(root, "project-a"), filepath.Join(root, "project-b")}
	for _, cwd := range cwds {
		if err := os.Mkdir(cwd, 0700); err != nil {
			t.Fatal(err)
		}
	}
	assertNoJSONAuthority := func() {
		t.Helper()
		if _, err := os.Lstat(statePath); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("SQL route created or adopted JSON authority", err)
		}
	}

	var requestMu sync.Mutex
	requests := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestMu.Lock()
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		requestMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet || r.URL.RawQuery != "" {
			t.Error("unexpected remote mutation or request scope")
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v2/") && r.Header.Get("Harvest-Account-Id") != "11" {
			t.Error("current-user discovery used the wrong account")
		}
		var body string
		switch r.URL.Path {
		case "/id/accounts":
			body = `{"accounts":[{"id":11,"product":"harvest"}]}`
		case "/v2/users/me":
			body = `{"id":7,"is_active":true}`
		case "/v2/users/me/project_assignments":
			body = `{"project_assignments":[{"is_active":true,"project":{"id":100},"task_assignments":[{"is_active":true,"task":{"id":200}}]},{"is_active":true,"project":{"id":101},"task_assignments":[{"is_active":true,"task":{"id":200}}]}],"links":{"next":null}}`
		default:
			t.Error("unexpected Harvest discovery endpoint")
			http.Error(w, "unexpected endpoint", http.StatusBadRequest)
			return
		}
		if _, err := io.WriteString(w, body); err != nil {
			t.Error("mock response write failed", err)
		}
	}))
	t.Cleanup(server.Close)
	providerCalls, nativeAuthCalls := 0, 0
	allowDiscovery := true
	getenv := func(key string) string {
		if key == "HARVEST_TOKEN" {
			if !allowDiscovery {
				t.Error("local operation or replay accessed credentials")
			}
			return "synthetic-first-flow-token"
		}
		// Empty TEMPO_STATE keeps post-commit worker hints off for this injected
		// service. Never consult personal environment/configuration.
		return ""
	}
	newProvider := func(token, account string) harvest.Provider {
		providerCalls++
		if !allowDiscovery || token != "synthetic-first-flow-token" || account != "11" {
			t.Error("unexpected provider creation")
		}
		client := server.Client()
		client.Timeout = 2 * time.Second
		return harvest.NewWithHTTP(token, account, server.URL+"/v2", server.URL+"/id", client)
	}
	authService := auth.NewService(auth.Options{
		ConfigPath: filepath.Join(root, "unused-config.json"), LockPath: filepath.Join(root, "unused-auth.lock"),
		Getenv: getenv, NewProvider: newProvider, PersistentAvailable: func() bool { return false },
		Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
			nativeAuthCalls++
			return auth.NativeReply{}, errors.New("native credential runner forbidden")
		}),
	})
	policyPath := filepath.Join(root, "policy", "hooks.json")
	policies := hookstate.New(hookstate.Options{Path: policyPath})
	base := time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC)
	seconds, clockCalls := int64(0), 0
	forbidClock := false
	newService := func() *activity.Service {
		return activity.NewSQLite(activity.Options{Path: statePath, LockTimeout: sqliteFlowTestLockTimeout(), HookPolicies: policies, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
			clockCalls++
			if forbidClock {
				return activity.ClockSample{}, errors.New("exact replay sampled a clock")
			}
			epoch, n := "fixture-boot", strconv.FormatInt(seconds*int64(time.Second), 10)
			return activity.ClockSample{Capability: "available", WallUTC: base.Add(time.Duration(seconds) * time.Second), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}, nil
		})})
	}
	d := cli.Dependencies{Activity: newService(), Auth: authService, Hooks: policies, Getenv: getenv, NewProvider: newProvider, Now: func() time.Time { return base.Add(time.Duration(seconds) * time.Second) }}
	if _, err := os.Lstat(filepath.Dir(statePath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("private constructor created storage", err)
	}
	runJSON := func(args []string, input []byte, target any) {
		t.Helper()
		var out, stderr bytes.Buffer
		code := cli.Run(ctx, append(append([]string{}, args...), "--json", "--non-interactive"), bytes.NewReader(input), &out, &stderr, d)
		if code != 0 || stderr.Len() != 0 {
			t.Fatalf("CLI %v: exit=%d stderr=%s", args, code, stderr.String())
		}
		var envelope struct {
			SchemaVersion int             `json:"schema_version"`
			Data          json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || envelope.SchemaVersion != 1 || len(envelope.Data) == 0 {
			t.Fatal("missing finite CLI data envelope", err)
		}
		if err := json.Unmarshal(envelope.Data, target); err != nil {
			t.Fatal("invalid typed CLI data", err)
		}
		assertNoJSONAuthority()
	}
	status := func() activity.ActivitySnapshot {
		t.Helper()
		var snapshot activity.ActivitySnapshot
		runJSON([]string{"activity", "status"}, nil, &snapshot)
		return snapshot
	}
	linkArgs := [][]string{}
	links := []activity.BindingResult{}
	for i, project := range []string{"100", "101"} {
		args := []string{"link", project, "--account", "11", "--task", "200", "--timezone", "UTC", "--path", cwds[i], "--request-id", fmt.Sprintf("91000000-0000-4000-8000-%012d", i+1)}
		var linked activity.BindingResult
		runJSON(args, nil, &linked)
		if !linked.Changed || linked.SnapshotRevision != strconv.Itoa(i+1) || linked.Binding.ID == "" || linked.Binding.Kind != "directory" || linked.Binding.Locator != cwds[i] || linked.Binding.Attribution != (activity.Attribution{AccountID: "11", UserID: "7", ProjectID: project, TaskID: "200", Timezone: "UTC"}) {
			t.Fatal("CLI link lost fresh identity or current-user attribution", linked)
		}
		linkArgs, links = append(linkArgs, args), append(links, linked)
	}
	wantRequests := []string{"GET /id/accounts", "GET /v2/users/me", "GET /v2/users/me/project_assignments", "GET /id/accounts", "GET /v2/users/me", "GET /v2/users/me/project_assignments"}
	requestMu.Lock()
	gotRequests := append([]string{}, requests...)
	requestMu.Unlock()
	if providerCalls != 2 || nativeAuthCalls != 0 || !reflect.DeepEqual(gotRequests, wantRequests) {
		t.Fatal("first links did not use exactly bounded read-only discovery", gotRequests)
	}
	allowDiscovery = false

	// Only the host project needs an operator-declared profile. Synthetic files
	// exercise actual artifact hashing; no installer or native delivery is claimed.
	policyContext := hookstate.Context{Host: "codex", Scope: "project", Path: cwds[0], RuntimeVersion: "0.159.3", Surface: "local", Conflicts: []string{}}
	for _, role := range []string{"runtime", "executable", "definitions"} {
		path := filepath.Join(cwds[0], role)
		if err := os.WriteFile(path, []byte("synthetic first-flow "+role), 0600); err != nil {
			t.Fatal(err)
		}
		policyContext.Artifacts = append(policyContext.Artifacts, hookstate.Artifact{Role: role, Path: path})
	}
	preview, err := policies.Preview(ctx, policyContext)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := policies.Confirm(ctx, hookstate.ConfirmInput{Context: preview.Context, Fingerprint: preview.Fingerprint, DeclarationVersion: hookstate.DeclarationVersion, RequestID: "92000000-0000-4000-8000-000000000001", Confirmed: true})
	if err != nil || !profile.CaptureEligible || profile.Basis != "operator_declared" {
		t.Fatal("declared host fixture is not eligible", err)
	}
	initial := status()
	if initial.ComputerID == nil || *initial.ComputerID == "" || initial.SnapshotRevision != "2" || len(initial.Actors) != 0 {
		t.Fatal("linked SQL identity unavailable through CLI status")
	}
	computer := *initial.ComputerID
	type savedHook struct {
		raw     []byte
		event   activity.HostEvent
		receipt activity.HostReceipt
	}
	saved := []savedHook{}
	runHook := func(raw []byte) {
		t.Helper()
		var out, stderr bytes.Buffer
		code := cli.Run(ctx, []string{"hook", "codex", "--input-stdin"}, bytes.NewReader(raw), &out, &stderr, d)
		if code != 0 || out.String() != "{}\n" || stderr.Len() != 0 {
			t.Fatalf("native-shaped hook was not captured: exit=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
		}
		assertNoJSONAuthority()
	}
	send := func(at int64, kind, turn, agent string) activity.HostReceipt {
		t.Helper()
		seconds = at
		raw := map[string]any{"hook_event_name": kind, "session_id": "flow-session", "cwd": cwds[0]}
		if kind == "SessionStart" {
			raw["source"] = "startup"
		} else {
			raw["turn_id"] = turn
		}
		if agent != "" {
			raw["agent_id"] = agent
		}
		if kind == "Stop" || kind == "SubagentStop" {
			raw["stop_hook_active"] = false
		}
		encoded, err := json.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		event, err := hooks.DecodeCodex(bytes.NewReader(encoded))
		if err != nil {
			t.Fatal("actual Codex decoder refused fixture", err)
		}
		runHook(encoded)
		// CLI host output intentionally contains no receipt. The public exact
		// replay returns its identity without minting a second operation.
		beforeCalls := clockCalls
		forbidClock = true
		receipt, err := d.Activity.IngestHost(ctx, event)
		forbidClock = false
		if err != nil || clockCalls != beforeCalls || receipt.ID == "" || receipt.Disposition != "duplicate" || receipt.Durability != "committed" || receipt.Origin != "unverified" || receipt.ProfileBasis != "operator_declared" {
			t.Fatal("exact host receipt replay failed", err, receipt)
		}
		saved = append(saved, savedHook{encoded, event, receipt})
		return receipt
	}
	send(0, "SessionStart", "", "")
	parent := send(0, "UserPromptSubmit", "parent-turn", "")
	if parent.Actor == nil || parent.Actor.Generation != "1" || parent.Actor.Key.ComputerID != computer {
		t.Fatal("parent identity was not admitted")
	}
	other := activity.Event{ContractVersion: 1, Actor: activity.ActorKey{ComputerID: computer, Source: "manual-test", SessionID: "other-project", AgentID: "root"}, Generation: "1", Sequence: "1", EventID: "other/work/1", Kind: "work", BindingID: links[1].Binding.ID, BindingRevision: links[1].Binding.Revision, CWD: cwds[1]}
	otherRaw, err := json.Marshal(other)
	if err != nil {
		t.Fatal(err)
	}
	var otherWork activity.EventResult
	runJSON([]string{"activity", "event", "--input-stdin"}, otherRaw, &otherWork)
	if otherWork.Disposition != "applied" || otherWork.Actor != (activity.ActorRef{Key: other.Actor, Generation: "1"}) {
		t.Fatal("normalized public SQL route did not apply independent project")
	}
	child := send(10, "SubagentStart", "child-turn", "child-agent")
	if child.Actor == nil || child.Actor.Key == parent.Actor.Key {
		t.Fatal("child identity collapsed into parent")
	}
	send(20, "Stop", "parent-turn", "")
	waiting := status()
	if len(waiting.Actors) != 3 || len(waiting.ClosedIntervals) != 0 || len(waiting.Uncertainties) != 0 {
		t.Fatal("parent stop finalized overlapping live work")
	}
	parentFound, childFound := false, false
	for _, a := range waiting.Actors {
		if a.Ref == *parent.Actor {
			parentFound = true
			if a.State != "wait_user" || a.Health != "continuous" {
				t.Fatal("parent did not wait independently")
			}
		}
		if a.Ref == *child.Actor {
			childFound = true
			if a.State != "working" || a.Health != "continuous" || a.Parent == nil || *a.Parent != *parent.Actor {
				t.Fatal("parent stop closed or detached child")
			}
		}
	}
	if !parentFound || !childFound {
		t.Fatal("status lost parent/child identities")
	}
	send(30, "SubagentStop", "child-turn", "child-agent")
	other.Kind, other.Sequence, other.EventID = "finish", "2", "other/finish/2"
	other.BindingID, other.BindingRevision = "", ""
	otherRaw, err = json.Marshal(other)
	if err != nil {
		t.Fatal(err)
	}
	var otherFinish activity.EventResult
	runJSON([]string{"activity", "event", "--input-stdin"}, otherRaw, &otherFinish)
	if otherFinish.Disposition != "applied" {
		t.Fatal("independent normalized finish not applied")
	}
	closed := status()
	if closed.SnapshotRevision != "9" || closed.ComputerID == nil || *closed.ComputerID != computer || len(closed.Actors) != 3 || len(closed.Projects) != 2 || len(closed.ProjectTimers) != 2 || len(closed.ClosedIntervals) != 2 || len(closed.Uncertainties) != 0 || len(closed.CaptureReviews) != 0 || closed.Worker.QueuedCount != 2 {
		t.Fatal("wrong final public SQL snapshot", closed)
	}
	seen := map[string]bool{}
	for _, interval := range closed.ClosedIntervals {
		project := interval.Attribution.ProjectID
		if seen[project] || project != "100" && project != "101" || interval.ID == "" || interval.ComputerID != computer || interval.Attribution.AccountID != "11" || interval.Attribution.UserID != "7" || interval.Attribution.TaskID != "200" || interval.Attribution.Timezone != "UTC" || !interval.Start.Equal(base) || !interval.End.Equal(base.Add(30*time.Second)) || interval.DurationNS != "30000000000" {
			t.Fatal("union boundaries, nanosecond duration or project attribution drifted", interval)
		}
		wantSupports := 1
		if project == "100" {
			wantSupports = 2
		}
		if len(interval.SegmentIDs) != wantSupports {
			t.Fatal("union lost distinct supports", interval)
		}
		seen[project] = true
	}
	projectSeen, timerSeen := map[string]bool{}, map[string]bool{}
	for _, project := range closed.Projects {
		if !seen[project.Attribution.ProjectID] || projectSeen[project.Attribution.ProjectID] || project.ComputerID != computer || project.ConfirmedClosedNS != "30000000000" || project.ProvisionalUnionNS != "0" || project.QueuedCount != 1 || len(project.ActiveActorRefs) != 0 || len(project.UnresolvedIDs) != 0 {
			t.Fatal("project totals merged independent attribution or double-counted overlap", project)
		}
		projectSeen[project.Attribution.ProjectID] = true
	}
	for _, timer := range closed.ProjectTimers {
		if !seen[timer.ProjectID] || timerSeen[timer.ProjectID] || timer.ComputerID != computer || timer.AccountID != "11" || timer.ConfirmedClosedNS != "30000000000" || timer.ProvisionalUnionNS != "0" || timer.QueuedCount != 1 {
			t.Fatal("project timer disagrees with immutable union", timer)
		}
		timerSeen[timer.ProjectID] = true
	}

	// Reconstruct both public services from their durable paths. Exact replay
	// must retain receipt IDs, timestamps, attribution, revisions and interval IDs.
	policies = hookstate.New(hookstate.Options{Path: policyPath})
	d.Activity, d.Hooks = newService(), policies
	forbidClock = true
	beforeCalls := clockCalls
	for i, args := range linkArgs {
		var replay activity.BindingResult
		runJSON(args, nil, &replay)
		if !reflect.DeepEqual(replay, links[i]) {
			t.Fatal("reopened exact CLI link replay changed identity")
		}
	}
	for _, prior := range saved {
		runHook(prior.raw)
		replay, err := d.Activity.IngestHost(ctx, prior.event)
		if err != nil || !reflect.DeepEqual(replay, prior.receipt) {
			t.Fatal("reopened exact native-shaped replay changed receipt", err)
		}
	}
	var normalizedReplay activity.EventResult
	runJSON([]string{"activity", "event", "--input-stdin"}, otherRaw, &normalizedReplay)
	if normalizedReplay.Disposition != "duplicate" {
		t.Fatal("normalized reopen did not replay")
	}
	normalizedReplay.Disposition = otherFinish.Disposition
	if !reflect.DeepEqual(normalizedReplay, otherFinish) || clockCalls != beforeCalls {
		t.Fatal("normalized exact replay changed facts or sampled clock")
	}
	forbidClock = false
	if reopened := status(); !reflect.DeepEqual(reopened, closed) {
		t.Fatal("reopen/replays changed public SQL state", reopened)
	}
	requestMu.Lock()
	finalRequests := append([]string{}, requests...)
	requestMu.Unlock()
	if providerCalls != 2 || nativeAuthCalls != 0 || !reflect.DeepEqual(finalRequests, wantRequests) {
		t.Fatal("capture/status/replay accessed remote or credential dependencies")
	}
	entries, err := os.ReadDir(filepath.Dir(statePath))
	if err != nil {
		t.Fatal(err)
	}
	databases := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "activity-") && strings.HasSuffix(entry.Name(), ".sqlite3") {
			databases++
		}
	}
	if databases != 1 {
		t.Fatal("fresh flow did not retain one SQL database")
	}
	assertNoJSONAuthority()
}
