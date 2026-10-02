package hookstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func qaInstallID(n int) string { return fmt.Sprintf("15151515-1515-4151-8151-%012d", n) }
func qaInstallInput(t *testing.T, s *Service, in InstallIntent, n int) ApplyInstallInput {
	t.Helper()
	p, err := s.PreviewInstall(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	return ApplyInstallInput{Intent: p.Intent, Fingerprint: p.Fingerprint, RequestID: qaInstallID(n), Confirmed: true}
}
func qaInstallApply(t *testing.T, s *Service, in InstallIntent, n int) HookList {
	t.Helper()
	result, err := s.ApplyInstall(context.Background(), qaInstallInput(t, s, in, n))
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func qaInstallRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func qaInstallCheckNoPromotion(t *testing.T, result HookList) {
	t.Helper()
	if result.ContractVersion != 1 || len(result.Hooks) == 0 {
		t.Fatalf("invalid result: %+v", result)
	}
	for _, status := range result.Hooks {
		if status.State == "receiving" || status.LastRealEvent != nil || status.Profile.CaptureEligible || status.Profile.Basis == "host_observed" {
			t.Fatalf("installation silently granted evidence/eligibility: %+v", status)
		}
	}
	b, _ := json.Marshal(result)
	if bytes.Contains(b, []byte("PRIVATE_SETTINGS_SENTINEL")) {
		t.Fatal("result exposed foreign config")
	}
}

type qaNativeHandler struct {
	Type, Command string
	Async         bool
	Timeout       float64
}
type qaNativeGroup struct {
	Matcher string
	Hooks   []qaNativeHandler
}

func qaInstallNative(t *testing.T, path string) map[string][]qaNativeGroup {
	t.Helper()
	var document struct{ Hooks map[string][]qaNativeGroup }
	if err := json.Unmarshal(qaInstallRead(t, path), &document); err != nil {
		t.Fatal(err)
	}
	return document.Hooks
}
func qaInstallOwnedCommands(t *testing.T, path, executable string) []string {
	t.Helper()
	hooks := qaInstallNative(t, path)
	var result []string
	for _, event := range []string{"SessionStart", "SessionEnd", "UserPromptSubmit", "Stop", "SubagentStart", "SubagentStop", "PreToolUse", "PostToolUse"} {
		found := 0
		for _, group := range hooks[event] {
			for _, handler := range group.Hooks {
				if strings.Contains(handler.Command, executable) {
					found++
					if handler.Type != "command" || handler.Async || handler.Timeout <= 0 {
						t.Fatalf("non-synchronous/unbounded handler: %+v", handler)
					}
					result = append(result, handler.Command)
				}
			}
		}
		if found != 1 {
			t.Fatalf("want one owned %s handler; got %d in %s", event, found, path)
		}
	}
	return result
}
func TestQAInstallApplyPreservesForeignSpansAndModes(t *testing.T) {
	for _, host := range []string{"codex", "claude"} {
		for _, scope := range []string{"user", "project"} {
			t.Run(host+"/"+scope, func(t *testing.T) {
				f := qaNewInstallFixture(t)
				target := f.target(host, scope)
				foreign := `{ "matcher" : "foreign-scope", "hooks" : [ { "type" : "command", "command" : "echo PRIVATE_SETTINGS_SENTINEL", "future" : [1, 2] } ] }`
				top := `"unknown" : {"nested": "PRIVATE_SETTINGS_SENTINEL", "number": 1.2500}`
				original := []byte("{\n  " + top + ",\n  \"hooks\" : { \"Stop\" : [ " + foreign + " ], \"UnrelatedFutureEvent\" : [] },\n  \"tail\" : true\n}\n")
				qaInstallWrite(t, target, original)
				if err := os.Chmod(target, 0640); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(filepath.Dir(target), 0755); err != nil {
					t.Fatal(err)
				}
				s := New(f.options)
				result := qaInstallApply(t, s, f.intent(host, scope), 1)
				qaInstallCheckNoPromotion(t, result)
				after := qaInstallRead(t, target)
				if !bytes.Contains(after, []byte(foreign)) || !bytes.Contains(after, []byte(top)) || !bytes.Contains(after, []byte(`"tail" : true`)) {
					t.Fatalf("unrelated byte spans were rewritten: %s", after)
				}
				commands := qaInstallOwnedCommands(t, target, f.executable)
				for _, command := range commands {
					if !strings.HasSuffix(command, " hook "+host+" --input-stdin") {
						t.Fatalf("unexpected native command: %q", command)
					}
				}
				info, _ := os.Stat(target)
				parent, _ := os.Stat(filepath.Dir(target))
				if info.Mode().Perm() != 0640 || parent.Mode().Perm() != 0755 {
					t.Fatal("existing config or parent permissions changed")
				}
				skill := qaInstallRead(t, f.skill(host, scope))
				if !bytes.HasPrefix(skill, []byte("---\n")) || !bytes.Contains(skill, []byte("name: tempo")) || !bytes.Contains(skill, []byte("description:")) {
					t.Fatal("packaged skill lacks host-compatible frontmatter")
				}
				parts := bytes.SplitN(skill, []byte("---"), 3)
				if len(parts) != 3 || bytes.Contains(parts[1], []byte("hooks:")) {
					t.Fatal("packaged skill unexpectedly registers dynamic hooks")
				}
				if _, err := os.Stat(filepath.Join(f.root, "activity-state.json")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("installer created activity state")
				}
			})
		}
	}
}
func TestQAInstallApplyConfirmationAndStalePreviewDoNotMutate(t *testing.T) {
	for _, drift := range []string{"unconfirmed", "target", "executable", "runtime", "intent", "skill"} {
		t.Run(drift, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			s := New(f.options)
			in := qaInstallInput(t, s, f.intent("codex", "project"), 2)
			code := "revision_conflict"
			switch drift {
			case "unconfirmed":
				in.Confirmed = false
				code = "confirmation_required"
			case "target":
				qaInstallWrite(t, f.target("codex", "project"), []byte(`{"private":"PRIVATE_SETTINGS_SENTINEL"}`))
			case "executable":
				qaInstallWrite(t, f.executable, []byte("changed executable"))
			case "runtime":
				qaInstallWrite(t, filepath.Join(f.root, "inert-runtime"), []byte("changed runtime"))
			case "intent":
				in.Intent.Operation = "repair"
			case "skill":
				qaInstallWrite(t, f.skill("codex", "project"), []byte("PRIVATE_SETTINGS_SENTINEL user skill"))
			}
			before := qaInstallTree(t, f.root)
			_, err := s.ApplyInstall(context.Background(), in)
			qaInstallError(t, err, code)
			qaInstallAssertUnchanged(t, f, before)
		})
	}
}
func TestQAInstallApplyExactReplayAndNoOpInstall(t *testing.T) {
	f := qaNewInstallFixture(t)
	s := New(f.options)
	intent := f.intent("both", "project")
	in := qaInstallInput(t, s, intent, 3)
	first, err := s.ApplyInstall(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	qaInstallCheckNoPromotion(t, first)
	before := qaInstallTree(t, f.root)
	// Completed replay precedes current runtime discovery and does not initialize a
	// fresh installation or duplicate backups when another process retries.
	options := f.options
	options.DiscoverRuntime = func(context.Context, string) (Runtime, error) {
		t.Error("completed replay rediscovered runtime")
		return Runtime{}, errors.New("inert failure")
	}
	again, err := New(options).ApplyInstall(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, again) {
		t.Fatalf("replay changed original result: %+v / %+v", first, again)
	}
	qaInstallAssertUnchanged(t, f, before)
	changed := in
	changed.Intent.Operation = "uninstall"
	_, err = New(f.options).ApplyInstall(context.Background(), changed)
	qaInstallError(t, err, "request_conflict")
	qaInstallAssertUnchanged(t, f, before)
	targets := []string{f.target("codex", "project"), f.target("claude", "project"), f.skill("codex", "project"), f.skill("claude", "project")}
	infos := map[string]os.FileInfo{}
	for _, path := range targets {
		infos[path], err = os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	qaInstallApply(t, New(f.options), intent, 4)
	for _, path := range targets {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(infos[path], info) || !info.ModTime().Equal(infos[path].ModTime()) {
			t.Fatalf("identical reinstall churned %s", path)
		}
	}
	after := qaInstallTree(t, f.root)
	for path, value := range after {
		if path == "metadata/hooks.json" {
			continue
		}
		if old, exists := before[path]; !exists || old != value {
			t.Fatalf("identical install created/changed backup or target %s", path)
		}
	}
	for _, host := range []string{"codex", "claude"} {
		qaInstallOwnedCommands(t, f.target(host, "project"), f.executable)
	}
}
func TestQAInstallUninstallPreservesForeignContentAndOtherState(t *testing.T) {
	f := qaNewInstallFixture(t)
	target := f.target("claude", "project")
	foreign := `{"hooks":[{"type":"command","command":"echo PRIVATE_SETTINGS_SENTINEL"}]}`
	qaInstallWrite(t, target, []byte(`{"hooks":{"Stop":[`+foreign+`]}, "other": 1.20}`))
	qaInstallWrite(t, filepath.Join(f.root, "activity-state.json"), []byte("opaque activity history"))
	qaInstallWrite(t, filepath.Join(f.root, "worker-state.json"), []byte("opaque worker state"))
	s := New(f.options)
	intent := f.intent("claude", "project")
	qaInstallApply(t, s, intent, 5)
	extra := filepath.Join(filepath.Dir(f.skill("claude", "project")), "user-notes.txt")
	qaInstallWrite(t, extra, []byte("user-added sibling"))
	intent.Operation = "uninstall"
	qaInstallApply(t, s, intent, 6)
	after := qaInstallRead(t, target)
	if !bytes.Contains(after, []byte(foreign)) || !bytes.Contains(after, []byte(`"other": 1.20`)) || bytes.Contains(after, []byte(f.executable)) {
		t.Fatalf("selective uninstall damaged foreign content: %s", after)
	}
	if _, err := os.Stat(f.skill("claude", "project")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unchanged owned skill not removed: %v", err)
	}
	for path, want := range map[string]string{extra: "user-added sibling", filepath.Join(f.root, "activity-state.json"): "opaque activity history", filepath.Join(f.root, "worker-state.json"): "opaque worker state"} {
		if string(qaInstallRead(t, path)) != want {
			t.Fatalf("uninstall touched %s", path)
		}
	}
}
func TestQAInstallNeverAdoptsUnownedIdenticalDefinitions(t *testing.T) {
	f := qaNewInstallFixture(t)
	s := New(f.options)
	intent := f.intent("codex", "project")
	qaInstallApply(t, s, intent, 7)
	// New metadata domain sees an identical existing installation with no proof of
	// ownership. Matching executable text alone is insufficient to adopt it.
	options := f.options
	options.Path = filepath.Join(f.root, "other-metadata", "hooks.json")
	other := New(options)
	before := qaInstallTree(t, f.root)
	preview, err := other.PreviewInstall(context.Background(), intent)
	if err == nil {
		_, err = other.ApplyInstall(context.Background(), ApplyInstallInput{Intent: intent, Fingerprint: preview.Fingerprint, RequestID: qaInstallID(8), Confirmed: true})
	}
	if err == nil {
		t.Fatal("identical unowned hooks/skill were silently adopted")
	}
	qaInstallAssertUnchanged(t, f, before)
}
func TestQAInstallUninstallPreservesEditedOwnedResources(t *testing.T) {
	for _, edit := range []string{"handler", "skill", "duplicate-handler"} {
		t.Run(edit, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			s := New(f.options)
			intent := f.intent("codex", "project")
			qaInstallApply(t, s, intent, 9)
			target := f.target("codex", "project")
			switch edit {
			case "handler":
				b := qaInstallRead(t, target)
				if !bytes.Contains(b, []byte(f.executable)) {
					t.Fatal("fixture lacks owned command")
				}
				qaInstallWrite(t, target, bytes.Replace(b, []byte(f.executable), []byte(f.executable+"-USER-EDIT"), 1))
			case "skill":
				qaInstallWrite(t, f.skill("codex", "project"), []byte("PRIVATE_SETTINGS_SENTINEL edited user skill"))
			case "duplicate-handler":
				var root map[string]json.RawMessage
				if err := json.Unmarshal(qaInstallRead(t, target), &root); err != nil {
					t.Fatal(err)
				}
				var hooks map[string][]json.RawMessage
				if err := json.Unmarshal(root["hooks"], &hooks); err != nil {
					t.Fatal(err)
				}
				if len(hooks["Stop"]) != 1 {
					t.Fatal("fixture lacks unique Stop group")
				}
				hooks["Stop"] = append(hooks["Stop"], hooks["Stop"][0])
				root["hooks"], _ = json.Marshal(hooks)
				b, _ := json.Marshal(root)
				qaInstallWrite(t, target, b)
			}
			before := qaInstallTree(t, f.root)
			intent.Operation = "uninstall"
			preview, err := s.PreviewInstall(context.Background(), intent)
			if err == nil {
				_, err = s.ApplyInstall(context.Background(), ApplyInstallInput{Intent: intent, Fingerprint: preview.Fingerprint, RequestID: qaInstallID(10), Confirmed: true})
			}
			if err == nil {
				t.Fatal("edited or ambiguous owned resource silently removed")
			}
			qaInstallAssertUnchanged(t, f, before)
		})
	}
}
func TestQAInstallRepairMovedExecutablePreservesForeignHook(t *testing.T) {
	f := qaNewInstallFixture(t)
	foreign := `{"hooks":[{"type":"command","command":"echo PRIVATE_SETTINGS_SENTINEL"}]}`
	qaInstallWrite(t, f.target("claude", "project"), []byte(`{"hooks":{"Stop":[`+foreign+`]}}`))
	s := New(f.options)
	intent := f.intent("claude", "project")
	qaInstallApply(t, s, intent, 11)
	old := f.executable
	f.options.Executable = filepath.Join(f.root, "new tempo")
	qaInstallWrite(t, f.options.Executable, []byte("new inert binary"))
	intent.Operation = "repair"
	qaInstallApply(t, New(f.options), intent, 12)
	target := qaInstallRead(t, f.target("claude", "project"))
	if bytes.Contains(target, []byte(old+"' hook")) || bytes.Contains(target, []byte(old+" hook")) {
		t.Fatal("repair left prior executable command")
	}
	if !bytes.Contains(target, []byte(foreign)) {
		t.Fatal("repair rewrote the foreign hook")
	}
	qaInstallOwnedCommands(t, f.target("claude", "project"), f.options.Executable)
}
