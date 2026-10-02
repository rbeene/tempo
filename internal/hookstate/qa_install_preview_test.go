package hookstate

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Installer QA uses synthetic, inert runtime files and explicit temporary paths.
// Runtime discovery must never launch a host or inspect the developer's home.
type qaInstallFixture struct {
	root, home, project, state, executable string
	options                                Options
}

func qaNewInstallFixture(t *testing.T) qaInstallFixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := qaInstallFixture{root: root, home: filepath.Join(root, "synthetic-home"), project: filepath.Join(root, "checkout"), state: filepath.Join(root, "metadata", "hooks.json"), executable: filepath.Join(root, "tempo")}
	if err := os.Mkdir(f.project, 0700); err != nil {
		t.Fatal(err)
	}
	qaInstallWrite(t, f.executable, []byte("inert Tempo build"))
	runtime := filepath.Join(root, "inert-runtime")
	qaInstallWrite(t, runtime, []byte("inert host build"))
	f.options = Options{CodexSystemDir: filepath.Join(root, "system-codex"), ClaudeManagedDir: filepath.Join(root, "system-claude"), Path: f.state, HomeDir: f.home, Executable: f.executable, BuildVersion: "qa-build-15", DiscoverRuntime: func(ctx context.Context, host string) (Runtime, error) {
		if err := ctx.Err(); err != nil {
			return Runtime{}, err
		}
		version := "0.159.3"
		if host == "claude" {
			version = "2.1.286"
		}
		return Runtime{Path: runtime, Version: version, Surface: "local"}, nil
	}}
	return f
}
func qaInstallWrite(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
}
func (f qaInstallFixture) target(host, scope string) string {
	root := f.home
	if scope == "project" {
		root = f.project
	}
	if host == "codex" {
		return filepath.Join(root, ".codex", "hooks.json")
	}
	return filepath.Join(root, ".claude", "settings.json")
}
func (f qaInstallFixture) skill(host, scope string) string {
	root := f.home
	if scope == "project" {
		root = f.project
	}
	dir := ".agents"
	if host == "claude" {
		dir = ".claude"
	}
	return filepath.Join(root, dir, "skills", "tempo", "SKILL.md")
}
func (f qaInstallFixture) intent(host, scope string) InstallIntent {
	in := InstallIntent{Host: host, Scope: scope, Operation: "install"}
	if scope == "project" {
		in.Path = f.project
	}
	return in
}

type qaInstallFile struct {
	Mode fs.FileMode
	Data string
}

func qaInstallTree(t *testing.T, root string) map[string]qaInstallFile {
	t.Helper()
	tree := map[string]qaInstallFile{}
	err := filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		value := qaInstallFile{Mode: info.Mode()}
		if info.Mode().IsRegular() {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			value.Data = string(b)
		} else if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			value.Data = target
		}
		relative, _ := filepath.Rel(root, path)
		tree[relative] = value
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return tree
}
func qaInstallAssertUnchanged(t *testing.T, f qaInstallFixture, before map[string]qaInstallFile) {
	t.Helper()
	if after := qaInstallTree(t, f.root); !reflect.DeepEqual(before, after) {
		t.Fatalf("read-only operation changed files: before=%v after=%v", before, after)
	}
}
func qaInstallError(t *testing.T, err error, code string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("want safe %s error; got %T %v", code, err, err)
	}
	if strings.Contains(err.Error(), "PRIVATE_SETTINGS_SENTINEL") {
		t.Fatal("error exposed foreign settings")
	}
}
func TestQAInstallPreviewPureAndScoped(t *testing.T) {
	for _, host := range []string{"codex", "claude", "both"} {
		for _, scope := range []string{"user", "project"} {
			t.Run(host+"/"+scope, func(t *testing.T) {
				f := qaNewInstallFixture(t)
				before := qaInstallTree(t, f.root)
				in := f.intent(host, scope)
				first, err := New(f.options).PreviewInstall(context.Background(), in)
				if err != nil {
					t.Fatal(err)
				}
				second, err := New(f.options).PreviewInstall(context.Background(), in)
				if err != nil {
					t.Fatal(err)
				}
				if first.ContractVersion != 1 || len(first.Fingerprint) != 64 || !reflect.DeepEqual(first, second) {
					t.Fatalf("preview is not stable/versioned: %+v / %+v", first, second)
				}
				if first.Intent != in {
					t.Fatalf("normalized intent changed selected scope: %+v", first.Intent)
				}
				hosts := []string{host}
				if host == "both" {
					hosts = []string{"codex", "claude"}
				}
				changes := map[string]HookChange{}
				for _, change := range first.Changes {
					if _, exists := changes[change.Path]; exists {
						t.Fatalf("duplicate target %s", change.Path)
					}
					changes[change.Path] = change
				}
				for _, h := range hosts {
					for _, path := range []string{f.target(h, scope), f.skill(h, scope)} {
						change, ok := changes[path]
						if !ok || change.Host != h || change.Operation != "insert_owned" || change.SafeSummary == "" {
							t.Fatalf("missing safe insertion for %s: %+v", path, first.Changes)
						}
					}
				}
				wrongRoot := f.project
				if scope == "project" {
					wrongRoot = f.home
				}
				for _, change := range first.Changes {
					if strings.HasPrefix(change.Path, wrongRoot+string(filepath.Separator)) {
						t.Fatalf("preview touched wrong scope: %+v", change)
					}
				}
				if len(first.ApprovalSteps) == 0 {
					t.Fatal("preview omits normal host trust/reload guidance")
				}
				qaInstallAssertUnchanged(t, f, before)
			})
		}
	}
}
func TestQAInstallPreviewRequiresExplicitValidIntent(t *testing.T) {
	for _, name := range []string{"missing-project", "unknown-host", "unknown-scope", "unknown-operation", "control-path"} {
		t.Run(name, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			in := f.intent("codex", "project")
			switch name {
			case "missing-project":
				in.Path = ""
			case "unknown-host":
				in.Host = "other"
			case "unknown-scope":
				in.Scope = "local"
			case "unknown-operation":
				in.Operation = "force"
			case "control-path":
				in.Path = f.project + "\n"
			}
			before := qaInstallTree(t, f.root)
			_, err := New(f.options).PreviewInstall(context.Background(), in)
			qaInstallError(t, err, "validation")
			qaInstallAssertUnchanged(t, f, before)
		})
	}
}
func TestQAInstallPreviewFingerprintBindsReviewedEvidence(t *testing.T) {
	for _, what := range []string{"target", "executable", "build", "runtime", "intent", "other-host"} {
		t.Run(what, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			in := f.intent("both", "project")
			original, err := New(f.options).PreviewInstall(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			switch what {
			case "target":
				qaInstallWrite(t, f.target("codex", "project"), []byte(`{"private":"PRIVATE_SETTINGS_SENTINEL","hooks":{}}`))
			case "executable":
				qaInstallWrite(t, f.executable, []byte("different inert Tempo build"))
			case "build":
				f.options.BuildVersion = "qa-build-16"
			case "runtime":
				qaInstallWrite(t, filepath.Join(f.root, "inert-runtime"), []byte("different inert runtime"))
			case "intent":
				in.Operation = "repair"
			case "other-host":
				qaInstallWrite(t, f.target("claude", "project"), []byte(`{"unknown":"PRIVATE_SETTINGS_SENTINEL"}`))
			}
			before := qaInstallTree(t, f.root)
			changed, err := New(f.options).PreviewInstall(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			if original.Fingerprint == changed.Fingerprint {
				t.Fatalf("fingerprint ignored %s", what)
			}
			output, _ := json.Marshal(changed)
			if strings.Contains(string(output), "PRIVATE_SETTINGS_SENTINEL") {
				t.Fatal("preview disclosed foreign settings")
			}
			qaInstallAssertUnchanged(t, f, before)
		})
	}
}
func TestQAInstallPreviewRejectsUnsafeJSONWithoutMutation(t *testing.T) {
	inputs := map[string]string{"malformed": `{"private":"PRIVATE_SETTINGS_SENTINEL",`, "trailing": `{} {"private":"PRIVATE_SETTINGS_SENTINEL"}`, "duplicate-root": `{"hooks":{},"hooks":{}}`, "duplicate-nested": `{"hooks":{"Stop":[],"Stop":[]}}`, "root-array": `[]`, "hooks-array": `{"hooks":[]}`, "event-object": `{"hooks":{"Stop":{}}}`, "oversized": `{"private":"` + strings.Repeat("x", (8<<20)+1) + `"}`}
	for _, host := range []string{"codex", "claude"} {
		for name, input := range inputs {
			t.Run(host+"/"+name, func(t *testing.T) {
				f := qaNewInstallFixture(t)
				qaInstallWrite(t, f.target(host, "project"), []byte(input))
				before := qaInstallTree(t, f.root)
				_, err := New(f.options).PreviewInstall(context.Background(), f.intent(host, "project"))
				if err == nil {
					t.Fatal("unsafe JSON was accepted")
				}
				var safe *Error
				if !errors.As(err, &safe) {
					t.Fatalf("raw parser error escaped: %T %v", err, err)
				}
				if strings.Contains(err.Error(), "PRIVATE_SETTINGS_SENTINEL") {
					t.Fatal("error exposed foreign settings")
				}
				qaInstallAssertUnchanged(t, f, before)
			})
		}
	}
}
func TestQAInstallPreviewRejectsUnsafeDestinations(t *testing.T) {
	for _, kind := range []string{"target-symlink", "parent-symlink", "directory-target"} {
		t.Run(kind, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			target := f.target("codex", "project")
			outside := filepath.Join(f.root, "outside")
			qaInstallWrite(t, filepath.Join(outside, "hooks.json"), []byte(`{"private":"PRIVATE_SETTINGS_SENTINEL"}`))
			if kind == "parent-symlink" {
				if err := os.Symlink(outside, filepath.Dir(target)); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
					t.Fatal(err)
				}
				if kind == "target-symlink" {
					if err := os.Symlink(filepath.Join(outside, "hooks.json"), target); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := os.Mkdir(target, 0700); err != nil {
						t.Fatal(err)
					}
				}
			}
			before := qaInstallTree(t, f.root)
			_, err := New(f.options).PreviewInstall(context.Background(), f.intent("codex", "project"))
			if err == nil {
				t.Fatal("unsafe target accepted")
			}
			qaInstallAssertUnchanged(t, f, before)
		})
	}
}
func TestQAInstallPreviewCancellationIsReadOnly(t *testing.T) {
	f := qaNewInstallFixture(t)
	before := qaInstallTree(t, f.root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := New(f.options).PreviewInstall(ctx, f.intent("both", "project"))
	if err == nil {
		t.Fatal("cancelled preview succeeded")
	}
	qaInstallAssertUnchanged(t, f, before)
}

func TestQAInstallPreviewShowsExactOwnedCommandForApproval(t *testing.T) {
	for _, host := range []string{"codex", "claude"} {
		t.Run(host, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			preview, err := New(f.options).PreviewInstall(context.Background(), f.intent(host, "project"))
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, change := range preview.Changes {
				if change.Path == f.target(host, "project") && strings.Contains(change.SafeSummary, f.executable) && strings.Contains(change.SafeSummary, "hook "+host+" --input-stdin") {
					found = true
				}
			}
			if !found {
				t.Fatal("approval preview omitted exact owned executable/arguments for configuration target")
			}
		})
	}
}
