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

func run(args []string) error {
	if len(args) < 3 || !filepath.IsAbs(args[1]) || !filepath.IsAbs(args[2]) {
		return fmt.Errorf("arguments")
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
		if len(args) != 8 {
			return fmt.Errorf("arguments")
		}
		preview, err := policy.Preview(ctx, hookstate.Context{Host: "codex", Scope: "project", Path: args[3], RuntimeVersion: "0.159.3", Surface: "local", Conflicts: []string{}, Artifacts: []hookstate.Artifact{
			{Role: "runtime", Path: args[4]}, {Role: "executable", Path: args[5]},
			{Role: "definitions", Path: args[6]}, {Role: "configuration", Path: args[7]},
		}})
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
		receipts, err := service.HostReceipts(ctx, activity.HostReceiptFilter{Source: "codex"})
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
		return json.NewEncoder(os.Stdout).Encode(struct {
			Receipts           []activity.HostReceipt `json:"receipts"`
			Actors             []actor                `json:"actors"`
			Queued             int                    `json:"queued"`
			Uncertainties      int                    `json:"uncertainties"`
			UncertaintyDetails []uncertaintyDetail    `json:"uncertainty_details"`
		}{receipts.Receipts, actors, snapshot.Worker.QueuedCount, len(snapshot.Uncertainties), details})
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
