package hookstate

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Fail-closed unsupported syntax is acceptable; quietly declaring a known
// admission/continuation conflict eligible is not. Exercise the public bridge.
func qaInstallCannotDeclare(t *testing.T, f qaInstallFixture, s *Service, selector HookSelector) {
	t.Helper()
	before := qaInstallTree(t, f.root)
	row := qaInstallStatus(t, s, selector)
	if row.Profile.CaptureEligible {
		t.Fatal("known static conflict is eligible")
	}
	if row.State != "unsupported" && row.State != "needs_repair" && len(row.Profile.Context.Conflicts) == 0 {
		t.Errorf("known static conflict was silently omitted: state=%s", row.State)
	}
	out, err := s.ConfirmInstalled(context.Background(), InstalledConfirmInput{Selector: selector, Fingerprint: row.Profile.Fingerprint, DeclarationVersion: DeclarationVersion, RequestID: qaInstallID(151), Confirmed: true})
	if err == nil {
		t.Error("explicit confirmation admitted known conflicting static configuration")
	}
	b, _ := json.Marshal(struct {
		Status HookStatus
		Result HookList
	}{row, out})
	if strings.Contains(string(b), "PRIVATE_SETTINGS_SENTINEL") || err != nil && strings.Contains(err.Error(), "PRIVATE_SETTINGS_SENTINEL") {
		t.Fatal("raw foreign configuration leaked")
	}
	if !reflect.DeepEqual(before, qaInstallTree(t, f.root)) {
		t.Error("failed declaration changed synthetic files")
	}
}

func TestQAInstallCodexInlineTOMLConflictsCannotBeDeclared(t *testing.T) {
	for name, document := range map[string]string{
		"array-tables":     "[[hooks.Stop]]\n[[hooks.Stop.hooks]]\ntype = \"command\"\ncommand = \"echo PRIVATE_SETTINGS_SENTINEL\"\n",
		"inline-array":     "[hooks]\nUserPromptSubmit = [{ hooks = [{ type = \"command\", command = \"echo PRIVATE_SETTINGS_SENTINEL\" }] }]\n",
		"quoted-component": "[\"hooks\"]\n\"Stop\" = [{ hooks = [{ type = \"command\", command = \"echo PRIVATE_SETTINGS_SENTINEL\" }] }]\n",
		"dotted-key":       "hooks.Stop = [{ hooks = [{ type = \"command\", command = \"echo PRIVATE_SETTINGS_SENTINEL\" }] }]\n",
		"inline-table":     "hooks = { Stop = [{ hooks = [{ type = \"command\", command = \"echo PRIVATE_SETTINGS_SENTINEL\" }] }] }\n",
	} {
		t.Run(name, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			s := New(f.options)
			qaInstallApply(t, s, f.intent("codex", "project"), 150)
			qaInstallWrite(t, filepath.Join(f.home, ".codex", "config.toml"), []byte(document))
			qaInstallCannotDeclare(t, f, s, HookSelector{Host: "codex", Scope: "project", Path: f.project})
		})
	}
}

func TestQAInstallCodexOrdinaryTOMLTrustStateAndUnrelatedSynchronousHookAllowed(t *testing.T) {
	for name, document := range map[string]string{
		"trust-state":    "[hooks.state.\"synthetic-hook-id\"]\ntrusted_hash = \"synthetic-content-hash\"\nenabled = true\n",
		"unrelated-sync": "[[hooks.PreCompact]]\n[[hooks.PreCompact.hooks]]\ntype = \"command\"\ncommand = \"echo PRIVATE_SETTINGS_SENTINEL\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			s := New(f.options)
			qaInstallApply(t, s, f.intent("codex", "project"), 150)
			qaInstallWrite(t, filepath.Join(f.home, ".codex", "config.toml"), []byte(document))
			profile := qaInstallConfirmFromStatus(t, s, HookSelector{Host: "codex", Scope: "project", Path: f.project}, 151)
			if profile.Basis != "operator_declared" {
				t.Fatal("ordinary trust state promoted native trust evidence")
			}
		})
	}
}

func TestQAInstallUserScopeIncludesSelectedProjectHooks(t *testing.T) {
	for _, host := range []string{"codex", "claude"} {
		t.Run(host, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			s := New(f.options)
			qaInstallApply(t, s, f.intent(host, "user"), 150)
			qaInstallWrite(t, f.target(host, "project"), []byte(`{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"echo PRIVATE_SETTINGS_SENTINEL"}]}]}}`))
			qaInstallCannotDeclare(t, f, s, HookSelector{Host: host, Scope: "user", Path: f.project})
		})
	}
}

func TestQAInstallCodexAncestorAndMainCheckoutStaticLayers(t *testing.T) {
	for _, kind := range []string{"ancestor-config", "ancestor-hooks", "main-config", "main-hooks"} {
		t.Run(kind, func(t *testing.T) {
			f, linked := qaInstallWorktree(t)
			selected := filepath.Join(linked, "nested")
			if err := os.Mkdir(selected, 0700); err != nil {
				t.Fatal(err)
			}
			s := New(f.options)
			qaInstallApply(t, s, InstallIntent{Host: "codex", Scope: "project", Path: selected, Operation: "install"}, 150)
			root := linked
			if strings.HasPrefix(kind, "main") {
				root = f.project
			}
			path := filepath.Join(root, ".codex", "config.toml")
			doc := "[hooks]\nStop = [{ hooks = [{ type=\"command\", command=\"echo PRIVATE_SETTINGS_SENTINEL\" }] }]\n"
			if strings.HasSuffix(kind, "hooks") {
				path = filepath.Join(root, ".codex", "hooks.json")
				doc = `{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"echo PRIVATE_SETTINGS_SENTINEL"}]}]}}`
			}
			qaInstallWrite(t, path, []byte(doc))
			qaInstallCannotDeclare(t, f, s, HookSelector{Host: "codex", Scope: "project", Path: selected})
		})
	}
}

func TestQAInstallInstalledProfileDoesNotWidenToUnreviewedDescendant(t *testing.T) {
	for _, host := range []string{"codex", "claude"} {
		t.Run(host, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			s := New(f.options)
			qaInstallApply(t, s, f.intent(host, "project"), 150)
			qaInstallConfirmFromStatus(t, s, HookSelector{Host: host, Scope: "project", Path: f.project}, 151)
			child := filepath.Join(f.project, "child")
			if err := os.Mkdir(child, 0700); err != nil {
				t.Fatal(err)
			}
			p, err := s.Eligibility(context.Background(), host, child)
			if err != nil {
				t.Fatal(err)
			}
			if p.CaptureEligible {
				t.Fatal("installed declaration widened to unreviewed descendant configuration context")
			}
		})
	}
}

func qaInstallAbsentProfileContract(t *testing.T, row HookStatus) {
	t.Helper()
	if row.Profile.Revision != "0" || row.Profile.Basis != "none" || row.Profile.State != "absent" || row.Profile.CaptureEligible || row.LastRealEvent != nil {
		t.Errorf("invalid absent profile: %+v", row.Profile)
	}
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Diagnostics json.RawMessage `json:"diagnostics"`
		Profile     struct {
			Context json.RawMessage `json:"context"`
		} `json:"profile"`
	}
	if err = json.Unmarshal(b, &wire); err != nil {
		t.Fatal(err)
	}
	if string(wire.Diagnostics) != "[]" {
		t.Errorf("absent diagnostics must be array: %s", wire.Diagnostics)
	}
	// A documented absent context may be null. If represented as an object,
	// collection fields must remain arrays, never ambiguous nulls.
	if string(wire.Profile.Context) != "null" {
		var c struct {
			Host      string          `json:"host"`
			Scope     string          `json:"scope"`
			Surface   string          `json:"surface"`
			Artifacts json.RawMessage `json:"artifacts"`
			Conflicts json.RawMessage `json:"conflicts"`
		}
		if err = json.Unmarshal(wire.Profile.Context, &c); err != nil {
			t.Fatal(err)
		}
		if c.Host != row.Host || c.Scope != row.Scope || c.Surface != "local" {
			t.Errorf("absent context enum fields invalid: %s", wire.Profile.Context)
		}
		if string(c.Artifacts) != "[]" || string(c.Conflicts) != "[]" {
			t.Errorf("absent context collections must be arrays: %s", wire.Profile.Context)
		}
	}
}
func TestQAInstallUninstallReturnsFiniteNotInstalledState(t *testing.T) {
	f := qaNewInstallFixture(t)
	s := New(f.options)
	selector := HookSelector{Host: "claude", Scope: "project", Path: f.project}
	qaInstallAbsentProfileContract(t, qaInstallStatus(t, s, selector))
	intent := f.intent("claude", "project")
	qaInstallApply(t, s, intent, 150)
	intent.Operation = "uninstall"
	result := qaInstallApply(t, s, intent, 151)
	if len(result.Hooks) != 1 {
		t.Fatalf("expected one uninstall result: %+v", result)
	}
	if result.Hooks[0].State != "not_installed" {
		t.Errorf("uninstall returned uncontracted state: %s", result.Hooks[0].State)
	}
	qaInstallAbsentProfileContract(t, result.Hooks[0])
	qaInstallAbsentProfileContract(t, qaInstallStatus(t, s, selector))
}

func TestQAInstallManagedStaticConflictCannotBeDeclared(t *testing.T) {
	for _, kind := range []string{"codex-config", "codex-hooks", "codex-requirements", "codex-managed-config", "claude-settings", "claude-only-managed", "claude-managed-directory"} {
		t.Run(kind, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			s := New(f.options)
			host := "codex"
			if strings.HasPrefix(kind, "claude") {
				host = "claude"
			}
			qaInstallApply(t, s, f.intent(host, "project"), 150)
			path := filepath.Join(f.options.CodexSystemDir, "config.toml")
			doc := "[hooks]\nStop = [{ hooks = [{ type=\"command\", command=\"echo PRIVATE_SETTINGS_SENTINEL\" }] }]\n"
			switch kind {
			case "codex-hooks":
				path = filepath.Join(f.options.CodexSystemDir, "hooks.json")
				doc = `{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"echo PRIVATE_SETTINGS_SENTINEL"}]}]}}`
			case "codex-requirements":
				path = filepath.Join(f.options.CodexSystemDir, "requirements.toml")
				doc = "allow_managed_hooks_only = true\n"
			case "codex-managed-config":
				path = filepath.Join(f.options.CodexSystemDir, "managed_config.toml")
			case "claude-settings":
				path = filepath.Join(f.options.ClaudeManagedDir, "managed-settings.json")
				doc = `{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"echo PRIVATE_SETTINGS_SENTINEL"}]}]}}`
			case "claude-only-managed":
				path = filepath.Join(f.options.ClaudeManagedDir, "managed-settings.json")
				doc = `{"allowManagedHooksOnly":true}`
			case "claude-managed-directory":
				path = filepath.Join(f.options.ClaudeManagedDir, "managed-settings.d", "policy.json")
				doc = `{"allowManagedHooksOnly":true}`
			}
			qaInstallWrite(t, path, []byte(doc))
			qaInstallCannotDeclare(t, f, s, HookSelector{Host: host, Scope: "project", Path: f.project})
		})
	}
}

func TestQAInstallMissingStaticSourceArrivalInvalidatesAdapterAdmission(t *testing.T) {
	for _, kind := range []string{"user-project-hooks", "ancestor-config", "ancestor-hooks", "main-config", "codex-system", "codex-requirements", "claude-system", "claude-managed-directory"} {
		t.Run(kind, func(t *testing.T) {
			f, linked := qaInstallWorktree(t)
			host, scope, selected := "codex", "project", filepath.Join(linked, "nested")
			if err := os.Mkdir(selected, 0700); err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(kind, "claude") {
				host = "claude"
			}
			if kind == "user-project-hooks" {
				scope = "user"
			}
			s := New(f.options)
			intent := InstallIntent{Host: host, Scope: scope, Operation: "install"}
			if scope == "project" {
				intent.Path = selected
			}
			qaInstallApply(t, s, intent, 150)
			selector := HookSelector{Host: host, Scope: scope, Path: selected}
			qaInstallConfirmFromStatus(t, s, selector, 151)
			path, doc := filepath.Join(selected, ".codex", "hooks.json"), "{}"
			switch kind {
			case "ancestor-config":
				path = filepath.Join(linked, ".codex", "config.toml")
				doc = "# newly observed layer\n"
			case "ancestor-hooks":
				path = filepath.Join(linked, ".codex", "hooks.json")
			case "main-config":
				path = filepath.Join(f.project, ".codex", "config.toml")
				doc = "# newly observed layer\n"
			case "codex-system":
				path = filepath.Join(f.options.CodexSystemDir, "hooks.json")
			case "codex-requirements":
				path = filepath.Join(f.options.CodexSystemDir, "requirements.toml")
				doc = "# newly observed managed constraints\n"
			case "claude-system":
				path = filepath.Join(f.options.ClaudeManagedDir, "managed-settings.json")
			case "claude-managed-directory":
				path = filepath.Join(f.options.ClaudeManagedDir, "managed-settings.d", "new-policy.json")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("fixture expected absent layer: %s %v", path, err)
			}
			qaInstallWrite(t, path, []byte(doc))
			// Admission must notice even before anyone asks for status/verify.
			p, err := New(f.options).Eligibility(context.Background(), host, selected)
			if err != nil {
				t.Fatal(err)
			}
			if p.CaptureEligible {
				t.Error("adapter admitted a policy after an absent static source appeared")
			}
			before := qaInstallTree(t, f.root)
			row := qaInstallStatus(t, New(f.options), selector)
			if row.Profile.CaptureEligible || row.Profile.State != "invalidated" {
				t.Errorf("new static source not reflected as invalidated: %+v", row.Profile)
			}
			qaInstallAssertUnchanged(t, f, before)
		})
	}
}

func TestQAInstallOwnedArtifactDamageCannotBeReconfirmed(t *testing.T) {
	for _, host := range []string{"codex", "claude"} {
		for _, kind := range []string{"empty-definitions", "missing-stop", "duplicate-stop", "edited-stop", "missing-skill", "edited-skill"} {
			t.Run(host+"/"+kind, func(t *testing.T) {
				f := qaNewInstallFixture(t)
				s := New(f.options)
				qaInstallApply(t, s, f.intent(host, "project"), 150)
				selector := HookSelector{Host: host, Scope: "project", Path: f.project}
				qaInstallConfirmFromStatus(t, s, selector, 149)
				if strings.HasSuffix(kind, "skill") {
					if kind == "missing-skill" {
						if err := os.Remove(f.skill(host, "project")); err != nil {
							t.Fatal(err)
						}
					} else {
						qaInstallWrite(t, f.skill(host, "project"), []byte("foreign replacement PRIVATE_SETTINGS_SENTINEL"))
					}
				} else {
					path := f.target(host, "project")
					var doc map[string]json.RawMessage
					if err := json.Unmarshal(qaInstallRead(t, path), &doc); err != nil {
						t.Fatal(err)
					}
					var hooks map[string][]json.RawMessage
					if err := json.Unmarshal(doc["hooks"], &hooks); err != nil {
						t.Fatal(err)
					}
					switch kind {
					case "empty-definitions":
						doc = map[string]json.RawMessage{}
					case "missing-stop":
						delete(hooks, "Stop")
					case "duplicate-stop":
						hooks["Stop"] = append(hooks["Stop"], hooks["Stop"][0])
					case "edited-stop":
						hooks["Stop"] = []json.RawMessage{json.RawMessage(`{"hooks":[{"type":"command","command":"echo PRIVATE_SETTINGS_SENTINEL"}]}`)}
					}
					if kind != "empty-definitions" {
						doc["hooks"], _ = json.Marshal(hooks)
					}
					b, _ := json.Marshal(doc)
					qaInstallWrite(t, path, b)
				}
				row := qaInstallStatus(t, s, selector)
				if row.State != "needs_repair" || row.Profile.CaptureEligible {
					t.Errorf("damaged owned artifact not repair-required: %s eligible=%v", row.State, row.Profile.CaptureEligible)
				}
				qaInstallCannotDeclare(t, f, s, selector)
			})
		}
	}
}

func TestQAInstallTooltipSubtableCollisionRejectsWithoutWrites(t *testing.T) {
	f := qaNewInstallFixture(t)
	qaInstallWrite(t, filepath.Join(f.project, ".codex", "config.toml"), []byte("[tui.show_tooltips]\nvalue = true\n"))
	before := qaInstallTree(t, f.root)
	_, err := New(f.options).PreviewInstall(context.Background(), f.intent("codex", "project"))
	if err == nil {
		t.Fatal("preview would create scalar over existing TOML table")
	}
	qaInstallAssertUnchanged(t, f, before)
}
