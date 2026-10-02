package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/hookstate"
)

func qaHooksCLIFixture(t *testing.T) (cli.Dependencies, string, string) {
	t.Helper()
	d, _ := qaSyncCLIDeps(t)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	project := filepath.Join(root, "project")
	if err = os.Mkdir(project, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"tempo", "runtime"} {
		if err = os.WriteFile(filepath.Join(root, name), []byte("inert synthetic binary"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	d.Hooks = hookstate.New(hookstate.Options{CodexSystemDir: filepath.Join(root, "system-codex"), ClaudeManagedDir: filepath.Join(root, "system-claude"), Path: filepath.Join(root, "metadata", "hooks.json"), HomeDir: filepath.Join(root, "home"), Executable: filepath.Join(root, "tempo"), BuildVersion: "qa-cli-15", DiscoverRuntime: func(_ context.Context, host string) (hookstate.Runtime, error) {
		version := "0.159.3"
		if host == "claude" {
			version = "2.1.286"
		}
		return hookstate.Runtime{Path: filepath.Join(root, "runtime"), Version: version, Surface: "local"}, nil
	}})
	return d, root, project
}
func qaHooksCLIArgs(action, project string) []string {
	return []string{"hooks", action, "--host", "claude", "--scope", "project", "--path", project}
}
func qaHooksCLIDecode(t *testing.T, envelope map[string]any, out any) {
	t.Helper()
	b, err := json.Marshal(envelope["data"])
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
}
func qaHooksCLIRequest(n int) string { return fmt.Sprintf("15150000-0000-4000-8000-%012d", n) }
func TestQAHooksCLIPurePreviewStatusVerifyAreFiniteOffline(t *testing.T) {
	for _, action := range []string{"preview", "status", "verify"} {
		for _, mode := range []string{"--json", "--non-interactive", "redirected"} {
			t.Run(action+"/"+mode, func(t *testing.T) {
				d, root, project := qaHooksCLIFixture(t)
				args := qaHooksCLIArgs(action, project)
				if action == "preview" {
					args = append(args, "--operation", "install")
				}
				if mode != "redirected" {
					args = append(args, mode)
				}
				result := qaSyncCLIEnvelope(t, args, d, 0)
				if result["data"] == nil {
					t.Fatal("missing finite result")
				}
				for _, name := range []string{"metadata", "home"} {
					if _, err := os.Stat(filepath.Join(root, name)); !os.IsNotExist(err) {
						t.Fatalf("pure %s initialized %s", action, name)
					}
				}
			})
		}
	}
}
func TestQAHooksCLISharedInstallProfileRepairAndUninstall(t *testing.T) {
	d, root, project := qaHooksCLIFixture(t)
	intent := hookstate.InstallIntent{Host: "claude", Scope: "project", Path: project, Operation: "install"}
	preview, err := d.Hooks.PreviewInstall(context.Background(), intent)
	if err != nil {
		t.Fatal(err)
	}
	args := append(qaHooksCLIArgs("install", project), "--fingerprint", preview.Fingerprint, "--request-id", qaHooksCLIRequest(1), "--yes", "--non-interactive")
	var installed hookstate.HookList
	qaHooksCLIDecode(t, qaSyncCLIEnvelope(t, args, d, 0), &installed)
	replayed, err := d.Hooks.ApplyInstall(context.Background(), hookstate.ApplyInstallInput{Intent: intent, Fingerprint: preview.Fingerprint, RequestID: qaHooksCLIRequest(1), Confirmed: true})
	if err != nil || !reflect.DeepEqual(installed, replayed) {
		t.Fatalf("CLI diverges from shared service replay: %+v %v", replayed, err)
	}
	var status hookstate.HookList
	qaHooksCLIDecode(t, qaSyncCLIEnvelope(t, append(qaHooksCLIArgs("status", project), "--json"), d, 0), &status)
	if len(status.Hooks) != 1 {
		t.Fatal("missing host status")
	}
	p := status.Hooks[0].Profile
	if p.CaptureEligible {
		t.Fatal("CLI install silently confirmed profile")
	}
	args = append(qaHooksCLIArgs("confirm-profile", project), "--fingerprint", p.Fingerprint, "--declaration-version", p.DeclarationVersion, "--request-id", qaHooksCLIRequest(2), "--yes", "--json")
	var confirmed hookstate.HookList
	qaHooksCLIDecode(t, qaSyncCLIEnvelope(t, args, d, 0), &confirmed)
	if len(confirmed.Hooks) != 1 || !confirmed.Hooks[0].Profile.CaptureEligible || confirmed.Hooks[0].Profile.Basis != "operator_declared" || confirmed.Hooks[0].LastRealEvent != nil {
		t.Fatalf("wrong confirmation semantics: %+v", confirmed)
	}
	args = append(qaHooksCLIArgs("revoke-profile", project), "--if-revision", confirmed.Hooks[0].Profile.Revision, "--request-id", qaHooksCLIRequest(3), "--yes", "--json")
	var revoked hookstate.HookList
	qaHooksCLIDecode(t, qaSyncCLIEnvelope(t, args, d, 0), &revoked)
	if len(revoked.Hooks) != 1 || revoked.Hooks[0].Profile.CaptureEligible {
		t.Fatal("CLI revoke retained eligibility")
	}
	for i, operation := range []string{"repair", "uninstall"} {
		intent.Operation = operation
		preview, err = d.Hooks.PreviewInstall(context.Background(), intent)
		if err != nil {
			t.Fatal(err)
		}
		args = append(qaHooksCLIArgs(operation, project), "--fingerprint", preview.Fingerprint, "--request-id", qaHooksCLIRequest(4+i), "--yes", "--json")
		qaSyncCLIEnvelope(t, args, d, 0)
	}
	b, err := os.ReadFile(filepath.Join(project, ".claude", "settings.json"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte(filepath.Join(root, "tempo"))) {
		t.Fatal("CLI uninstall left owned command")
	}
}
func TestQAHooksCLIValidationRejectsBeforeRuntimeOrCredentials(t *testing.T) {
	cases := [][]string{
		{"hooks", "preview", "--host", "other", "--scope", "project", "--operation", "install"},
		{"hooks", "preview", "--host", "codex", "--scope", "local", "--operation", "install"},
		{"hooks", "preview", "--host", "codex", "--scope", "project", "--operation", "install"},
		{"hooks", "status", "--request-id", qaHooksCLIRequest(8)},
		{"hooks", "verify", "--fingerprint", "abc"},
		{"hooks", "install", "--host", "claude", "--scope", "user", "--yes"},
		{"hooks", "install", "--host", "claude", "--scope", "user", "--fingerprint", "abc", "--request-id", "invalid", "--yes"},
		{"hooks", "confirm-profile", "--host", "both", "--scope", "project", "--path", "/synthetic", "--fingerprint", "abc", "--declaration-version", hookstate.DeclarationVersion, "--yes"},
		{"hooks", "revoke-profile", "--host", "claude", "--scope", "user", "--path", "/synthetic", "--if-revision", "01", "--yes"},
		{"hooks", "status", "--host", "codex", "--host", "claude"},
		{"hooks", "status", "--account", "1"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			d, root, _ := qaHooksCLIFixture(t)
			d.Hooks = hookstate.New(hookstate.Options{CodexSystemDir: filepath.Join(root, "system-codex"), ClaudeManagedDir: filepath.Join(root, "system-claude"), Path: filepath.Join(root, "metadata", "hooks.json"), DiscoverRuntime: func(context.Context, string) (hookstate.Runtime, error) {
				t.Fatal("invalid arguments reached runtime discovery")
				return hookstate.Runtime{}, nil
			}})
			qaSyncCLIEnvelope(t, append(args, "--non-interactive"), d, 2)
			if _, err := os.Stat(filepath.Join(root, "metadata")); !os.IsNotExist(err) {
				t.Fatal("invalid input created hooks metadata")
			}
		})
	}
}
func TestQAHooksCLIMissingConfirmationAndStalePreviewPreserveTypedErrors(t *testing.T) {
	d, _, project := qaHooksCLIFixture(t)
	preview, err := d.Hooks.PreviewInstall(context.Background(), hookstate.InstallIntent{Host: "claude", Scope: "project", Path: project, Operation: "install"})
	if err != nil {
		t.Fatal(err)
	}
	args := append(qaHooksCLIArgs("install", project), "--fingerprint", preview.Fingerprint, "--request-id", qaHooksCLIRequest(10), "--json")
	result := qaSyncCLIEnvelope(t, args, d, 6)
	detail := result["error"].(map[string]any)
	if detail["code"] != "confirmation_required" {
		t.Fatalf("missing confirmation changed category: %+v", detail)
	}
	if err = os.MkdirAll(filepath.Join(project, ".claude"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(project, ".claude", "settings.json"), []byte(`{"foreign":"PRIVATE_SETTINGS_SENTINEL"}`), 0600); err != nil {
		t.Fatal(err)
	}
	result = qaSyncCLIEnvelope(t, append(args, "--yes"), d, 6)
	detail = result["error"].(map[string]any)
	if detail["code"] != "revision_conflict" || detail["uncertain"] != false {
		t.Fatalf("stale preview typed error lost: %+v", detail)
	}
	b, _ := json.Marshal(result)
	if bytes.Contains(b, []byte("PRIVATE_SETTINGS_SENTINEL")) {
		t.Fatal("foreign config exposed")
	}
}
func TestQAHooksCLISchemaHelpOfflineAndComplete(t *testing.T) {
	d, _, _ := qaHooksCLIFixture(t)
	for _, action := range []string{"schema", "help"} {
		var out, stderr bytes.Buffer
		if code := cli.Run(context.Background(), []string{action}, strings.NewReader(""), &out, &stderr, d); code != 0 {
			t.Fatalf("offline %s failed: %s", action, stderr.String())
		}
		for _, name := range []string{"hooks preview", "hooks install", "hooks status", "hooks verify", "hooks repair", "hooks uninstall", "hooks confirm-profile", "hooks revoke-profile"} {
			if !strings.Contains(out.String(), name) {
				t.Errorf("%s omits %s", action, name)
			}
		}
	}
}

func TestQAHooksCLIOmittedRequestIDIsGeneratedAndReplayable(t *testing.T) {
	d, _, project := qaHooksCLIFixture(t)
	intent := hookstate.InstallIntent{Host: "claude", Scope: "project", Path: project, Operation: "install"}
	preview, err := d.Hooks.PreviewInstall(context.Background(), intent)
	if err != nil {
		t.Fatal(err)
	}
	args := append(qaHooksCLIArgs("install", project), "--fingerprint", preview.Fingerprint, "--yes", "--json")
	var result hookstate.HookList
	qaHooksCLIDecode(t, qaSyncCLIEnvelope(t, args, d, 0), &result)
	if len(result.RequestID) != 36 {
		t.Fatalf("generated UUID missing from mutation result: %+v", result)
	}
	var replayed hookstate.HookList
	qaHooksCLIDecode(t, qaSyncCLIEnvelope(t, append(args, "--request-id", result.RequestID), d, 0), &replayed)
	if !reflect.DeepEqual(result, replayed) {
		t.Fatalf("generated request could not exactly replay: %+v / %+v", result, replayed)
	}
	var status hookstate.HookList
	qaHooksCLIDecode(t, qaSyncCLIEnvelope(t, append(qaHooksCLIArgs("status", project), "--json"), d, 0), &status)
	if status.RequestID != "" {
		t.Fatal("read-only status generated a mutation ID")
	}
}
