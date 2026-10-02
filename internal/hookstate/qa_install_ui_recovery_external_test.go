package hookstate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
)

func qaRecoverySurface(t *testing.T) (*hookstate.Service, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	project := filepath.Join(root, "project")
	if err = os.Mkdir(project, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"tempo", "runtime"} {
		if err = os.WriteFile(filepath.Join(root, name), []byte("inert fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	h := hookstate.New(hookstate.Options{CodexSystemDir: filepath.Join(root, "system-codex"), ClaudeManagedDir: filepath.Join(root, "system-claude"), Path: filepath.Join(root, "metadata", "hooks.json"), HomeDir: filepath.Join(root, "home"), Executable: filepath.Join(root, "tempo"), BuildVersion: "qa-recovery-15", DiscoverRuntime: func(context.Context, string) (hookstate.Runtime, error) {
		return hookstate.Runtime{Path: filepath.Join(root, "runtime"), Version: "2.1.286", Surface: "local"}, nil
	}})
	return h, project
}

type qaRecoveryPrompt struct{ t *testing.T }

func (p qaRecoveryPrompt) Choose(_ context.Context, _ string, choices []terminal.Choice) (string, error) {
	for _, c := range choices {
		if c.ID == "install" {
			return c.ID, nil
		}
	}
	p.t.Fatal("install action missing")
	return "", nil
}
func (p qaRecoveryPrompt) Confirm(context.Context, string) (bool, error) { return true, nil }
func (p qaRecoveryPrompt) Text(context.Context, string, string) (string, error) {
	p.t.Fatal("unexpected text input")
	return "", nil
}
func (p qaRecoveryPrompt) Secret(context.Context, string) ([]byte, error) {
	p.t.Fatal("unexpected credential input")
	return nil, nil
}
func TestQAInstallSetupUnknownOutcomeRetainsGeneratedRecoveryIdentity(t *testing.T) {
	h, project := qaRecoverySurface(t)
	reached := false
	hookstate.QAInstallSetFault(h, func(stage string) error {
		if stage == "install_target_sync" {
			reached = true
			return errors.New("PRIVATE_SETTINGS_SENTINEL disk detail")
		}
		return nil
	})
	result, err := setup.New(setup.Options{Hooks: h}).ManageHooks(context.Background(), setup.Input{Host: "claude", Scope: "project", Path: project}, qaRecoveryPrompt{t})
	var safe *hookstate.Error
	if !reached || !errors.As(err, &safe) || safe.Code != "local_write_unknown" || len(safe.RequestID) != 36 || result.RequestID != safe.RequestID {
		t.Fatalf("UI-generated uncertain operation lost recovery identity: %+v %v", result, err)
	}
	status, err := h.Status(context.Background(), hookstate.HookSelector{Host: "claude", Scope: "project", Path: project})
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Hooks) != 1 || status.Hooks[0].Pending == nil || status.Hooks[0].Pending.RequestID != safe.RequestID {
		t.Fatalf("UI cannot recover original pending operation: %+v", status)
	}
	pending := status.Hooks[0].Pending
	hookstate.QAInstallSetFault(h, nil)
	recovered, err := h.ApplyInstall(context.Background(), hookstate.ApplyInstallInput{Intent: pending.Intent, Fingerprint: pending.Fingerprint, RequestID: pending.RequestID, Confirmed: true})
	if err != nil || recovered.RequestID != safe.RequestID {
		t.Fatalf("UI recovery cannot use status's exact original intent: %+v %v", recovered, err)
	}
}
func TestQAInstallCLIUnknownOutcomeExposesOnlySafeGeneratedRequestID(t *testing.T) {
	h, project := qaRecoverySurface(t)
	intent := hookstate.InstallIntent{Host: "claude", Scope: "project", Path: project, Operation: "install"}
	preview, err := h.PreviewInstall(context.Background(), intent)
	if err != nil {
		t.Fatal(err)
	}
	reached := false
	hookstate.QAInstallSetFault(h, func(stage string) error {
		if stage == "install_target_sync" {
			reached = true
			return errors.New("PRIVATE_SETTINGS_SENTINEL disk detail")
		}
		return nil
	})
	args := []string{"hooks", "install", "--host", "claude", "--scope", "project", "--path", project, "--fingerprint", preview.Fingerprint, "--yes", "--json"}
	var out, stderr bytes.Buffer
	code := cli.Run(context.Background(), args, strings.NewReader(""), &out, &stderr, cli.Dependencies{Hooks: h, Getenv: func(string) string { return "" }})
	if !reached || code != 8 || out.Len() != 0 || strings.Contains(stderr.String(), "PRIVATE_SETTINGS_SENTINEL") {
		t.Fatalf("CLI lost safe uncertain result: exit%d out%q err%q", code, out.String(), stderr.String())
	}
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Uncertain bool   `json:"uncertain"`
			Details   struct {
				RequestID string `json:"request_id"`
			} `json:"details"`
		} `json:"error"`
	}
	if err = json.Unmarshal(stderr.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != "local_write_unknown" || !envelope.Error.Uncertain || len(envelope.Error.Details.RequestID) != 36 {
		t.Fatalf("machine error lacks generated request identity: %s", stderr.String())
	}
	status, err := h.Status(context.Background(), hookstate.HookSelector{Host: "claude", Scope: "project", Path: project})
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Hooks) != 1 || status.Hooks[0].Pending == nil || status.Hooks[0].Pending.RequestID != envelope.Error.Details.RequestID {
		t.Fatalf("machine error and pending recovery identities diverge: %+v", status)
	}
}
