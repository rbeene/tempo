// Package nativefixture supplies a test-only setup/read executable for the
// hosted native-hook smoke. It never delivers callbacks or edits state JSON.
package nativefixture

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
)

// TestMain's private protocol is only in a compiled test binary. The production
// Tempo command has no fixture switch, provider override, or trust shortcut.
func TestMain(m *testing.M) {
	if len(os.Args) < 2 || os.Args[1] != "fixture" {
		os.Exit(m.Run())
	}
	if err := run(os.Args[2:]); err != nil {
		// Never print underlying errors, request bodies, paths or configuration.
		fmt.Fprintln(os.Stderr, "native_fixture_operation_failed")
		os.Exit(1)
	}
	os.Exit(0)
}

func fixtureArguments(args []string) ([]string, string, string, error) {
	host, version := "codex", "0.159.3"
	if len(args) >= 2 && args[len(args)-2] == "--host" {
		if args[len(args)-1] != "claude" {
			return nil, "", "", fmt.Errorf("arguments")
		}
		host, version = "claude", "2.1.286"
		args = args[:len(args)-2]
	}
	if len(args) < 3 || !filepath.IsAbs(args[1]) || !filepath.IsAbs(args[2]) {
		return nil, "", "", fmt.Errorf("arguments")
	}
	expected := 0
	switch args[0] {
	case "link":
		if host != "codex" {
			return nil, "", "", fmt.Errorf("arguments")
		}
		expected = 4
	case "confirm":
		expected = 8
		if host == "claude" {
			expected = 7
		}
	case "read":
		expected = 3
	}
	if expected == 0 || len(args) != expected {
		return nil, "", "", fmt.Errorf("arguments")
	}
	return args, host, version, nil
}

func run(raw []string) error {
	args, host, version, err := fixtureArguments(raw)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	policy := hookstate.New(hookstate.Options{Path: args[2]})
	service := activity.New(activity.Options{Path: args[1], HookPolicies: policy})
	switch args[0] {
	case "link":
		if len(args) != 4 {
			return fmt.Errorf("arguments")
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			switch r.URL.Path {
			case "/id/accounts":
				fmt.Fprint(w, `{"accounts":[{"id":1,"product":"harvest"}]}`)
			case "/v2/users/me":
				fmt.Fprint(w, `{"id":2,"is_active":true}`)
			case "/v2/users/me/project_assignments":
				fmt.Fprint(w, `{"project_assignments":[{"is_active":true,"project":{"id":3},"task_assignments":[{"is_active":true,"task":{"id":4}}]}],"links":{"next":null}}`)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()
		_, err := service.Link(ctx, activity.LinkInput{Path: args[3], AccountID: "1", ProjectID: "3", TaskID: "4", Timezone: "UTC", RequestID: "873c9fc4-f334-4b9b-9d46-d11c1b1fa013"}, activity.LinkDependencies{
			NewProvider: func(_ context.Context, account string) (harvest.Provider, error) {
				return harvest.NewWithHTTP("synthetic-not-a-credential", account, server.URL+"/v2", server.URL+"/id", server.Client()), nil
			},
		})
		return err
	case "confirm":
		artifacts := []hookstate.Artifact{
			{Role: "runtime", Path: args[4]}, {Role: "executable", Path: args[5]},
			{Role: "definitions", Path: args[6]},
		}
		if host == "codex" {
			artifacts = append(artifacts, hookstate.Artifact{Role: "configuration", Path: args[7]})
		}
		preview, err := policy.Preview(ctx, hookstate.Context{Host: host, Scope: "project", Path: args[3], RuntimeVersion: version, Surface: "local", Conflicts: []string{}, Artifacts: artifacts})
		if err != nil {
			return err
		}
		profile, err := policy.Confirm(ctx, hookstate.ConfirmInput{Context: preview.Context, Fingerprint: preview.Fingerprint, DeclarationVersion: hookstate.DeclarationVersion, RequestID: "8565ed5c-e3b2-4fa3-b6d3-fb49e7a3ca13", Confirmed: true})
		if err != nil {
			return err
		}
		if profile.Basis != "operator_declared" || !profile.CaptureEligible {
			return fmt.Errorf("profile")
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"basis": profile.Basis, "revision": profile.Revision, "fingerprint": profile.Fingerprint})
	case "read":
		if len(args) != 3 {
			return fmt.Errorf("arguments")
		}
		receipts, err := service.HostReceipts(ctx, activity.HostReceiptFilter{Source: host})
		if err != nil {
			return err
		}
		snapshot, err := service.Status(ctx)
		if err != nil {
			return err
		}
		// Deliberate projection: no generic state serialization or file reads.
		type actor struct {
			Ref    activity.ActorRef `json:"ref"`
			State  string            `json:"state"`
			Health string            `json:"health"`
		}
		actors := []actor{}
		for _, a := range snapshot.Actors {
			actors = append(actors, actor{a.Ref, a.State, a.Health})
		}
		details, err := projectUncertainties(snapshot.Uncertainties)
		if err != nil {
			return err
		}
		captureReviews := 0
		for _, review := range snapshot.CaptureReviews {
			if review.Source == host {
				captureReviews++
			}
		}
		return json.NewEncoder(os.Stdout).Encode(struct {
			Receipts           []activity.HostReceipt `json:"receipts"`
			Actors             []actor                `json:"actors"`
			Queued             int                    `json:"queued"`
			Uncertainties      int                    `json:"uncertainties"`
			UncertaintyDetails []uncertaintyDetail    `json:"uncertainty_details"`
			CaptureReviews     int                    `json:"capture_reviews"`
		}{receipts.Receipts, actors, snapshot.Worker.QueuedCount, len(snapshot.Uncertainties), details, captureReviews})
	default:
		return fmt.Errorf("operation")
	}
}

type uncertaintyDetail struct {
	Actor             activity.ActorRef `json:"actor"`
	Reason            string            `json:"reason"`
	State             string            `json:"state"`
	Bounded           bool              `json:"bounded"`
	ResolutionPresent bool              `json:"resolution_present"`
	Discarded         bool              `json:"discarded"`
}

func projectUncertainties(values []activity.Uncertainty) ([]uncertaintyDetail, error) {
	if len(values) > 8 {
		return nil, fmt.Errorf("uncertainty_bound")
	}
	result := make([]uncertaintyDetail, 0, len(values))
	for _, u := range values {
		switch u.Reason {
		case "source_lost", "ordering_unavailable", "suspend", "clock_changed", "restart_unknown", "event_gap", "superseded":
		default:
			return nil, fmt.Errorf("uncertainty_contract")
		}
		if u.State != "unresolved" && u.State != "resolved" {
			return nil, fmt.Errorf("uncertainty_contract")
		}
		result = append(result, uncertaintyDetail{u.Actor, u.Reason, u.State, u.UpperBound != nil && !u.UpperBound.Before(u.LowerBound), u.ResolutionEnd != nil, u.Discarded})
	}
	return result, nil
}

func TestUncertaintyProjectionIsBoundedAndAllowlisted(t *testing.T) {
	lower := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	upper := lower.Add(time.Second)
	u := activity.Uncertainty{ID: "PRIVATE_CANARY", SegmentID: "PRIVATE_CANARY", Reason: "source_lost", State: "unresolved", LowerBound: lower, UpperBound: &upper}
	projected, err := projectUncertainties([]activity.Uncertainty{u})
	if err != nil || len(projected) != 1 {
		t.Fatalf("projection: %v", err)
	}
	b, err := json.Marshal(projected[0])
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 6 || fields["reason"] != "source_lost" || fields["state"] != "unresolved" || fields["bounded"] != true || fields["resolution_present"] != false || fields["discarded"] != false || fields["actor"] == nil || strings.Contains(string(b), "PRIVATE_CANARY") {
		t.Fatalf("unexpected safe projection: %s", b)
	}
	for _, invalid := range []activity.Uncertainty{{Reason: "PRIVATE_CANARY", State: "unresolved"}, {Reason: "source_lost", State: "PRIVATE_CANARY"}} {
		if _, err := projectUncertainties([]activity.Uncertainty{invalid}); err == nil {
			t.Fatal("unknown private text accepted")
		}
	}
	bounded := make([]activity.Uncertainty, 9)
	for i := range bounded {
		bounded[i] = u
	}
	if _, err := projectUncertainties(bounded[:8]); err != nil {
		t.Fatal("allowed bound rejected")
	}
	if _, err := projectUncertainties(bounded); err == nil {
		t.Fatal("unbounded projection")
	}
	u.UpperBound = nil
	projected, err = projectUncertainties([]activity.Uncertainty{u})
	if err != nil || projected[0].Bounded {
		t.Fatal("missing upper bound accepted")
	}
	before := lower.Add(-time.Second)
	u.UpperBound = &before
	projected, err = projectUncertainties([]activity.Uncertainty{u})
	if err != nil || projected[0].Bounded {
		t.Fatal("reversed bound accepted")
	}
	u.UpperBound, u.ResolutionEnd, u.Discarded = &upper, &upper, true
	projected, err = projectUncertainties([]activity.Uncertainty{u})
	if err != nil || !projected[0].ResolutionPresent || !projected[0].Discarded {
		t.Fatal("resolution flags lost")
	}
}

func TestFixtureHostSelectorRejectsBeforeAccess(t *testing.T) {
	for _, args := range [][]string{
		{"read", "/unused-state", "/unused-policy", "--host", "unknown"},
		{"read", "/unused-state", "/unused-policy", "--host"},
		{"read", "/unused-state", "/unused-policy", "--host", "claude", "--runtime-version", "2.1.286"},
		{"link", "/unused-state", "/unused-policy", "/unused-project", "--host", "claude"},
	} {
		if _, _, _, err := fixtureArguments(args); err == nil {
			t.Errorf("unexpected selector accepted")
		}
	}
	for _, tc := range []struct {
		args          []string
		host, version string
	}{
		{[]string{"read", "/unused-state", "/unused-policy"}, "codex", "0.159.3"},
		{[]string{"read", "/unused-state", "/unused-policy", "--host", "claude"}, "claude", "2.1.286"},
	} {
		args, host, version, err := fixtureArguments(tc.args)
		if err != nil || len(args) != 3 || host != tc.host || version != tc.version {
			t.Fatal("valid selector contract rejected")
		}
	}
}

func TestFixtureClaudeConfirmsThreeArtifacts(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	project := filepath.Join(root, "project")
	if err := os.Mkdir(project, 0700); err != nil {
		t.Fatal(err)
	}
	state, policyPath := filepath.Join(root, "state"), filepath.Join(root, "metadata", "policy")
	paths := []string{}
	for _, name := range []string{"runtime", "tempo", "settings"} {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte("inert fixture artifact"), 0600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	args := append([]string{"confirm", state, policyPath, project}, paths...)
	args = append(args, "--host", "claude")
	if err := run(args); err != nil {
		if problem, ok := err.(*hookstate.Error); ok {
			t.Fatalf("genuine Claude profile confirmation: %s", problem.Code)
		}
		t.Fatal("genuine Claude profile confirmation failed")
	}
	policy := hookstate.New(hookstate.Options{Path: policyPath})
	eligibility, err := policy.Eligibility(context.Background(), "claude", project)
	if err != nil || !eligibility.CaptureEligible || eligibility.Basis != "operator_declared" {
		t.Fatal("confirmed Claude policy not eligible")
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatal("profile confirmation created activity state")
	}
}
