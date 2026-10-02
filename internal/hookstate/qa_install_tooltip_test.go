package hookstate

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func qaTooltipPath(f qaInstallFixture, scope string) string {
	root := f.home
	if scope == "project" {
		root = f.project
	}
	return filepath.Join(root, ".codex", "config.toml")
}
func TestQAInstallTooltipIsExplicitScopedOwnedAndReversible(t *testing.T) {
	for _, scope := range []string{"user", "project"} {
		for _, kind := range []string{"missing-file", "missing-field", "true", "false"} {
			t.Run(scope+"/"+kind, func(t *testing.T) {
				f := qaNewInstallFixture(t)
				path := qaTooltipPath(f, scope)
				var original []byte
				switch kind {
				case "missing-field":
					original = []byte("# PRIVATE_SETTINGS_SENTINEL\nmodel = \"ordinary-model\"\n[tui]\nnotifications = [\"agent-turn-complete\"] # keep\n")
				case "true":
					original = []byte("# PRIVATE_SETTINGS_SENTINEL\n[tui]\nshow_tooltips  = true   # user preference\nnotifications = false\n")
				case "false":
					original = []byte("# PRIVATE_SETTINGS_SENTINEL\n[tui]\nshow_tooltips = false # already chosen\n")
				}
				if original != nil {
					qaInstallWrite(t, path, original)
				}
				s := New(f.options)
				intent := f.intent("codex", scope)
				preview, err := s.PreviewInstall(context.Background(), intent)
				if err != nil {
					t.Fatal(err)
				}
				if kind != "false" {
					found := false
					for _, change := range preview.Changes {
						if change.Path == path && strings.Contains(strings.ToLower(change.SafeSummary), "tooltip") && strings.Contains(change.SafeSummary, "false") {
							found = true
						}
					}
					if !found {
						t.Fatalf("preview omitted exact selected tooltip change: %+v", preview.Changes)
					}
				}
				qaInstallApply(t, s, intent, 91)
				installed := qaInstallRead(t, path)
				if kind == "false" && !bytes.Equal(installed, original) {
					t.Fatal("existing false was needlessly rewritten/adopted")
				}
				if !bytes.Contains(installed, []byte("show_tooltips")) || !bytes.Contains(installed, []byte("false")) {
					t.Fatalf("selected config did not suppress tooltip writer: %s", installed)
				}
				if original != nil && !bytes.Contains(installed, []byte("# PRIVATE_SETTINGS_SENTINEL")) {
					t.Fatal("foreign comment was rewritten")
				}
				other := qaTooltipPath(f, "user")
				if scope == "user" {
					other = qaTooltipPath(f, "project")
				}
				if _, err := os.Stat(other); !os.IsNotExist(err) {
					t.Fatalf("tooltip changed unselected scope %s", other)
				}
				intent.Operation = "uninstall"
				qaInstallApply(t, s, intent, 92)
				if kind == "missing-file" {
					if _, err := os.Stat(path); !os.IsNotExist(err) {
						t.Fatalf("unchanged wholly owned config not removed: %v", err)
					}
				} else {
					restored := qaInstallRead(t, path)
					if !bytes.Equal(restored, original) {
						t.Fatalf("owned field not reversibly restored; got %q want %q", restored, original)
					}
				}
			})
		}
	}
}
func TestQAInstallTooltipRemovalPreservesForeignAdditions(t *testing.T) {
	for _, initial := range []string{"# original user comment\n", "[tui]\nshow_tooltips = true # previous preference\n"} {
		t.Run(initial, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			path := qaTooltipPath(f, "project")
			qaInstallWrite(t, path, []byte(initial))
			s := New(f.options)
			intent := f.intent("codex", "project")
			qaInstallApply(t, s, intent, 93)
			added := []byte("\n[future_setting]\nlabel = \"PRIVATE_SETTINGS_SENTINEL user addition\"\n")
			qaInstallWrite(t, path, append(qaInstallRead(t, path), added...))
			intent.Operation = "uninstall"
			qaInstallApply(t, s, intent, 94)
			after := qaInstallRead(t, path)
			if !bytes.Contains(after, added) {
				t.Fatal("selective tooltip removal restored backup over foreign addition")
			}
			if !bytes.Contains(after, []byte(initial)) {
				t.Fatalf("prior owned field/comment not restored: %q", after)
			}
		})
	}
}
func TestQAInstallTooltipUnfamiliarOrMalformedTOMLConflictsWithoutWrites(t *testing.T) {
	for name, content := range map[string]string{"dotted": "tui.show_tooltips = true\n", "inline": "tui = { show_tooltips = true }\n", "duplicate-table": "[tui]\nshow_tooltips = true\n[tui]\nnotifications = false\n", "duplicate-field": "[tui]\nshow_tooltips = true\nshow_tooltips = false\n", "nonboolean": "[tui]\nshow_tooltips = \"PRIVATE_SETTINGS_SENTINEL\"\n", "malformed": "[tui\nshow_tooltips = true\n", "multiline": "[tui]\nshow_tooltips = \"\"\"true\"\"\"\n"} {
		t.Run(name, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			qaInstallWrite(t, qaTooltipPath(f, "project"), []byte(content))
			before := qaInstallTree(t, f.root)
			_, err := New(f.options).PreviewInstall(context.Background(), f.intent("codex", "project"))
			if err == nil {
				t.Fatal("ambiguous/unfamiliar tooltip TOML accepted")
			}
			if strings.Contains(err.Error(), "PRIVATE_SETTINGS_SENTINEL") {
				t.Fatal("raw config diagnostic escaped")
			}
			qaInstallAssertUnchanged(t, f, before)
		})
	}
}
func TestQAInstallTooltipPreviewDriftAndUserPreferenceEditsArePreserved(t *testing.T) {
	f := qaNewInstallFixture(t)
	s := New(f.options)
	intent := f.intent("codex", "project")
	path := qaTooltipPath(f, "project")
	in := qaInstallInput(t, s, intent, 95)
	qaInstallWrite(t, path, []byte("# changed after preview\n[tui]\nshow_tooltips = true\n"))
	before := qaInstallTree(t, f.root)
	_, err := s.ApplyInstall(context.Background(), in)
	qaInstallError(t, err, "revision_conflict")
	qaInstallAssertUnchanged(t, f, before)
	qaInstallApply(t, s, intent, 96)
	current := qaInstallRead(t, path)
	changed := bytes.Replace(current, []byte("false"), []byte("true"), 1)
	if bytes.Equal(current, changed) {
		t.Fatal("fixture lacks false")
	}
	qaInstallWrite(t, path, changed)
	before = qaInstallTree(t, f.root)
	intent.Operation = "uninstall"
	preview, err := s.PreviewInstall(context.Background(), intent)
	if err == nil {
		_, err = s.ApplyInstall(context.Background(), ApplyInstallInput{Intent: intent, Fingerprint: preview.Fingerprint, RequestID: qaInstallID(97), Confirmed: true})
	}
	if err == nil {
		t.Fatal("user-edited tooltip preference was overwritten/removed")
	}
	qaInstallAssertUnchanged(t, f, before)
}
func TestQAInstallTooltipRetainsWholeConfigurationFingerprint(t *testing.T) {
	for _, change := range []string{"ordinary-counter", "admission-config"} {
		t.Run(change, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			global := qaTooltipPath(f, "user")
			original := []byte("[tui.model_availability_nux]\n\"gpt-6.1-sol\" = 1\n")
			qaInstallWrite(t, global, original)
			s := New(f.options)
			qaInstallApply(t, s, f.intent("codex", "project"), 98)
			selector := HookSelector{Host: "codex", Scope: "project", Path: f.project}
			qaInstallConfirmFromStatus(t, s, selector, 99)
			// Stable repeated reads model unchanged artifacts only, not a fabricated host restart.
			for i := 0; i < 2; i++ {
				status := qaInstallStatus(t, New(f.options), selector)
				if !status.Profile.CaptureEligible {
					t.Fatal("unchanged profile did not retain eligibility")
				}
			}
			mutated := bytes.Replace(original, []byte(" = 1"), []byte(" = 2"), 1)
			if change == "admission-config" {
				mutated = append(original, []byte("\n[hooks]\nStop = []\n")...)
			}
			qaInstallWrite(t, global, mutated)
			status := qaInstallStatus(t, New(f.options), selector)
			if status.Profile.CaptureEligible || status.Profile.State != "invalidated" {
				t.Fatalf("whole config change %s was excluded or silently reconfirmed: %+v", change, status.Profile)
			}
		})
	}
}

func TestQAInstallTooltipPreservesOrdinaryQuotedTrustTable(t *testing.T) {
	f := qaNewInstallFixture(t)
	path := qaTooltipPath(f, "user")
	foreign := []byte("# ordinary host trust data\n[projects.\"/synthetic/project with spaces\"]\ntrust_level = \"trusted\" # PRIVATE_SETTINGS_SENTINEL\n")
	qaInstallWrite(t, path, foreign)
	s := New(f.options)
	intent := f.intent("codex", "user")
	qaInstallApply(t, s, intent, 141)
	if !bytes.Contains(qaInstallRead(t, path), foreign) {
		t.Fatal("tooltip edit rewrote unrelated ordinary trust table")
	}
	intent.Operation = "uninstall"
	qaInstallApply(t, s, intent, 142)
	if !bytes.Equal(qaInstallRead(t, path), foreign) {
		t.Fatal("tooltip removal did not preserve exact original trust-table bytes")
	}
}
func TestQAInstallTooltipRejectsUnclosedForeignScalarQuote(t *testing.T) {
	f := qaNewInstallFixture(t)
	qaInstallWrite(t, qaTooltipPath(f, "project"), []byte("model = \"PRIVATE_SETTINGS_SENTINEL\n[tui]\nshow_tooltips = true\n"))
	before := qaInstallTree(t, f.root)
	_, err := New(f.options).PreviewInstall(context.Background(), f.intent("codex", "project"))
	if err == nil {
		t.Fatal("malformed TOML foreign scalar was accepted")
	}
	qaInstallAssertUnchanged(t, f, before)
}

func TestQAInstallTooltipRemovalRetainsNewTableWithForeignChildren(t *testing.T) {
	for name, addition := range map[string]string{"setting": "notifications = false # PRIVATE_SETTINGS_SENTINEL\n", "comment": "# PRIVATE_SETTINGS_SENTINEL user note in tui\n"} {
		t.Run(name, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			path := qaTooltipPath(f, "project")
			original := []byte("model = \"ordinary-model\"\n")
			qaInstallWrite(t, path, original)
			s := New(f.options)
			intent := f.intent("codex", "project")
			qaInstallApply(t, s, intent, 143)
			current := qaInstallRead(t, path)
			current = append(current, []byte(addition)...)
			qaInstallWrite(t, path, current)
			intent.Operation = "uninstall"
			qaInstallApply(t, s, intent, 144)
			after := qaInstallRead(t, path)
			table := bytes.Index(after, []byte("[tui]"))
			child := bytes.Index(after, []byte(addition))
			if table < 0 || child < table {
				t.Fatalf("uninstall removed foreign child's enclosing table: %q", after)
			}
			if bytes.Contains(after, []byte("show_tooltips")) || !bytes.Contains(after, original) {
				t.Fatalf("selective field removal damaged owned/foreign distinction: %q", after)
			}
		})
	}
}
