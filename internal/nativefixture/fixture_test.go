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
		return json.NewEncoder(os.Stdout).Encode(struct {
			Receipts      []activity.HostReceipt `json:"receipts"`
			Actors        []actor                `json:"actors"`
			Queued        int                    `json:"queued"`
			Uncertainties int                    `json:"uncertainties"`
		}{receipts.Receipts, actors, snapshot.Worker.QueuedCount, len(snapshot.Uncertainties)})
	default:
		return fmt.Errorf("operation")
	}
}
