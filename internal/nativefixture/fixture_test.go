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
	case "install", "status", "confirm":
		expected = 7
	case "read":
		expected = 3
	}
	if expected == 0 || len(args) != expected {
		return nil, "", "", fmt.Errorf("arguments")
	}
	for i := 3; i < len(args); i++ {
		if i == 6 {
			if args[i] != "user" && args[i] != "project" {
				return nil, "", "", fmt.Errorf("arguments")
			}
		} else if !filepath.IsAbs(args[i]) {
			return nil, "", "", fmt.Errorf("arguments")
		}
	}
	return args, host, version, nil
}

func run(raw []string) error {
	return runWithOptions(raw, hookstate.Options{})
}

// Options are injected only by inert unit tests. Hosted fixture calls use the
// actual unchanged home and production default managed/system roots.
func runWithOptions(raw []string, options hookstate.Options) error {
	args, host, version, err := fixtureArguments(raw)
	if err != nil {
		return err
	}
	timeout := 5 * time.Second
	if args[0] == "install" || args[0] == "status" || args[0] == "confirm" {
		// Production inspection resamples the large pinned runtime at each
		// integrity boundary, including again inside the confirmation lock.
		timeout = 15 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	options.Path = args[2]
	if args[0] == "install" || args[0] == "status" || args[0] == "confirm" {
		options.Executable = args[5]
		options.DiscoverRuntime = func(context.Context, string) (hookstate.Runtime, error) {
			// The hosted caller has already verified the official pinned archive.
			// Production installation hashes this exact path again itself.
			return hookstate.Runtime{Path: args[4], Version: version, Surface: "local"}, nil
		}
	}
	policy := hookstate.New(options)
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
	case "install", "status", "confirm":
		selector := hookstate.HookSelector{Host: host, Scope: args[6], Path: args[3]}
		if args[0] == "install" {
			preview, err := policy.PreviewInstall(ctx, hookstate.InstallIntent{Host: host, Scope: selector.Scope, Path: selector.Path, Operation: "install"})
			if err != nil {
				return err
			}
			if _, err := policy.ApplyInstall(ctx, hookstate.ApplyInstallInput{Intent: preview.Intent, Fingerprint: preview.Fingerprint, RequestID: "f354e4f2-a5f9-45a6-a657-10f7b3d98a15", Confirmed: true}); err != nil {
				return err
			}
		}
		result, err := policy.Status(ctx, selector)
		if err != nil {
			return err
		}
		if args[0] == "confirm" {
			if len(result.Hooks) != 1 || result.Hooks[0].State != "approval_required" || result.Hooks[0].Profile.Fingerprint == "" {
				return fmt.Errorf("profile")
			}
			result, err = policy.ConfirmInstalled(ctx, hookstate.InstalledConfirmInput{Selector: selector, Fingerprint: result.Hooks[0].Profile.Fingerprint, DeclarationVersion: hookstate.DeclarationVersion, RequestID: "8565ed5c-e3b2-4fa3-b6d3-fb49e7a3ca13", Confirmed: true})
			if err != nil {
				return err
			}
			if len(result.Hooks) != 1 || result.Hooks[0].State != "awaiting_real_event" || result.Hooks[0].Profile.Basis != "operator_declared" || !result.Hooks[0].Profile.CaptureEligible {
				return fmt.Errorf("profile")
			}
		}
		return json.NewEncoder(os.Stdout).Encode(result)
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
