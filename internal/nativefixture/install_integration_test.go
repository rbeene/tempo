package nativefixture

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/hookstate"
)

// These fixtures are inert files. All discovery roots are synthetic; neither
// runtime version discovery nor the installer launches a host in these tests.
func installerFixture(t *testing.T, host, scope string) ([]string, hookstate.Options) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Go's TempDir leaf uses 0777 before umask; SQLite requires an existing
	// activity directory to be private. This directory is fixture-owned only.
	stateDir := filepath.Join(root, "state")
	if err := os.Mkdir(stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	project := filepath.Join(root, "project")
	if err := os.Mkdir(project, 0700); err != nil {
		t.Fatal(err)
	}
	runtime, executable := filepath.Join(root, "runtime"), filepath.Join(root, "tempo")
	for _, path := range []string{runtime, executable} {
		if err := os.WriteFile(path, []byte("inert build"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"install", filepath.Join(stateDir, "activity"), filepath.Join(root, "metadata", "hooks"), project, runtime, executable, scope}
	if host == "claude" {
		args = append(args, "--host", "claude")
	}
	options := hookstate.Options{HomeDir: filepath.Join(root, "home"), CodexSystemDir: filepath.Join(root, "system-codex"), ClaudeManagedDir: filepath.Join(root, "system-claude")}
	return args, options
}

func TestFixtureProductionInstallAndConfirmation(t *testing.T) {
	for _, tc := range []struct{ host, scope string }{{"codex", "user"}, {"claude", "project"}} {
		t.Run(tc.host, func(t *testing.T) {
			args, options := installerFixture(t, tc.host, tc.scope)
			if err := runWithOptions(args, options); err != nil {
				t.Fatalf("production install: %v", err)
			}
			base := args[3]
			if tc.scope == "user" {
				base = options.HomeDir
			}
			definitions, skill := filepath.Join(base, ".codex", "hooks.json"), filepath.Join(base, ".agents", "skills", "tempo", "SKILL.md")
			events := []string{"SessionStart", "SessionEnd", "UserPromptSubmit", "Stop", "SubagentStart", "SubagentStop", "PreToolUse", "PostToolUse", "Interrupt", "PermissionRequest", "PreCompact", "PostCompact"}
			if tc.host == "claude" {
				definitions, skill = filepath.Join(base, ".claude", "settings.json"), filepath.Join(base, ".claude", "skills", "tempo", "SKILL.md")
				events = append(events[:8], "PostToolUseFailure", "PermissionRequest", "StopFailure", "TaskCreated", "TaskCompleted")
			}
			data, err := os.ReadFile(definitions)
			if err != nil {
				t.Fatal(err)
			}
			var doc struct {
				Hooks map[string][]struct {
					Hooks []struct {
						Type, Command string
						Timeout       int
					}
				}
			}
			if err := json.Unmarshal(data, &doc); err != nil {
				t.Fatal(err)
			}
			if len(doc.Hooks) != len(events) {
				t.Fatal("full production event inventory not installed")
			}
			for _, event := range events {
				groups := doc.Hooks[event]
				if len(groups) != 1 || len(groups[0].Hooks) != 1 {
					t.Fatalf("event %s is ambiguous", event)
				}
				h := groups[0].Hooks[0]
				if h.Type != "command" || h.Timeout != 2 || h.Command != "'"+args[5]+"' hook "+tc.host+" --input-stdin" {
					t.Fatalf("event %s not exact production command", event)
				}
			}
			if data, err := os.ReadFile(skill); err != nil || string(data) != hookstate.BundledSkill {
				t.Fatal("owned bundled skill missing")
			}
			if tc.host == "codex" {
				config := filepath.Join(base, ".codex", "config.toml")
				data, err := os.ReadFile(config)
				if err != nil || !strings.Contains(string(data), "show_tooltips = false") {
					t.Fatal("production tooltip not installed")
				}
				// This emulates only the configuration bytes written after normal
				// host review, not a trust bypass or a claim of real host delivery.
				data = append(data, []byte("\n[hooks.state.\"inert-normal-review\"]\nenabled = true\ntrusted_hash = \"synthetic-hash\"\n")...)
				if err := os.WriteFile(config, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			args[0] = "status"
			if err := runWithOptions(args, options); err != nil {
				t.Fatalf("production status: %v", err)
			}
			args[0] = "confirm"
			if err := runWithOptions(args, options); err != nil {
				t.Fatalf("installed profile confirmation: %v", err)
			}
			// Reopen in the same injected host-root environment. Eligibility hashes
			// the retained runtime/executable inventory; it must not switch to HOME.
			options.Path = args[2]
			policy := hookstate.New(options)
			profile, err := policy.Eligibility(context.Background(), tc.host, args[3])
			if err != nil || !profile.CaptureEligible || profile.Basis != "operator_declared" || profile.Context.InventoryVersion != "tempo-installed-static-v1" {
				t.Fatal("genuine installed inventory not confirmed")
			}
			foundTrust := tc.host != "codex"
			for _, artifact := range profile.Context.Artifacts {
				if tc.host == "codex" && artifact.Path == filepath.Join(base, ".codex", "config.toml") && artifact.SHA256 != "absent" {
					foundTrust = true
				}
			}
			if !foundTrust {
				t.Fatal("post-review current configuration not fingerprinted")
			}
			if _, err := os.Stat(args[1]); !os.IsNotExist(err) {
				t.Fatal("install/confirmation created the activity selector")
			}
			snapshot, err := activity.New(activity.Options{Path: args[1]}).Status(context.Background())
			if err != nil {
				var local *activity.Error
				if errors.As(err, &local) {
					t.Fatalf("install/confirmation activity read failed: code=%s", local.Code)
				}
				t.Fatalf("install/confirmation activity read failed: error_type=%T", err)
			}
			if snapshot.ComputerID != nil || snapshot.SnapshotRevision != "0" {
				t.Fatalf("install/confirmation initialized SQLite activity identity: computer_id_present=%t revision=%q", snapshot.ComputerID != nil, snapshot.SnapshotRevision)
			}
			if err := os.WriteFile(skill, []byte("edited skill"), 0600); err != nil {
				t.Fatal(err)
			}
			profile, err = policy.Eligibility(context.Background(), tc.host, args[3])
			if err != nil || profile.CaptureEligible {
				t.Fatal("installed resource drift remained eligible")
			}
		})
	}
}

func TestFixtureConfirmationCannotReplaceInstallationOrRepair(t *testing.T) {
	args, options := installerFixture(t, "claude", "project")
	args[0] = "confirm"
	if err := runWithOptions(args, options); err == nil {
		t.Fatal("confirmation without installation succeeded")
	}
	if _, err := os.Stat(args[2]); !os.IsNotExist(err) {
		t.Fatal("failed absent confirmation initialized policy metadata")
	}
	args[0] = "install"
	if err := runWithOptions(args, options); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(args[2])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(args[3], ".claude", "settings.json"), []byte("{\"hooks\":{}}"), 0600); err != nil {
		t.Fatal(err)
	}
	args[0] = "confirm"
	if err := runWithOptions(args, options); err == nil {
		t.Fatal("missing installed callbacks admitted by fixture confirmation")
	}
	after, err := os.ReadFile(args[2])
	if err != nil || string(before) != string(after) {
		t.Fatal("rejected confirmation changed policy metadata")
	}
	if _, err := os.Stat(args[1]); !os.IsNotExist(err) {
		t.Fatal("fixture confirmation manufactured activity identity")
	}
}

func TestFixtureInstallProtocolRejectsBeforeAccess(t *testing.T) {
	valid := []string{"install", "/unused-state", "/unused-policy", "/unused-project", "/unused-runtime", "/unused-tempo", "user"}
	for _, action := range []string{"install", "status", "confirm"} {
		args := append([]string(nil), valid...)
		args[0] = action
		if _, _, _, err := fixtureArguments(args); err != nil {
			t.Fatalf("valid %s rejected", action)
		}
		for _, index := range []int{1, 2, 3, 4, 5, 6} {
			bad := append([]string(nil), args...)
			bad[index] = "relative-or-invalid"
			if _, _, _, err := fixtureArguments(bad); err == nil {
				t.Fatalf("%s accepted invalid field %d", action, index)
			}
		}
		if _, _, _, err := fixtureArguments(append(args, "extra")); err == nil {
			t.Fatal("extra argument accepted")
		}
	}
}
