package hookstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func qaInstallOriginals(t *testing.T, f qaInstallFixture) map[string][]byte {
	t.Helper()
	originals := map[string][]byte{}
	for _, host := range []string{"codex", "claude"} {
		path := f.target(host, "project")
		data := []byte("{\n \"private\" : \"PRIVATE_SETTINGS_SENTINEL " + host + "\",\n \"hooks\" : {}\n}\n")
		qaInstallWrite(t, path, data)
		originals[path] = data
	}
	return originals
}
func qaInstallBackups(t *testing.T, f qaInstallFixture) map[string]qaInstallFile {
	t.Helper()
	tree := qaInstallTree(t, f.root)
	result := map[string]qaInstallFile{}
	for path, value := range tree {
		if strings.HasSuffix(path, ".before") {
			result[path] = value
		}
	}
	return result
}
func qaInstallChangedTarget(t *testing.T, f qaInstallFixture, originals map[string][]byte) string {
	t.Helper()
	for _, host := range []string{"codex", "claude"} {
		paths := []string{f.target(host, "project"), f.skill(host, "project")}
		if host == "codex" {
			paths = append(paths, filepath.Join(f.project, ".codex", "config.toml"))
		}
		for _, path := range paths {
			current, err := os.ReadFile(path)
			original, existed := originals[path]
			if !existed && err == nil || existed && err == nil && !bytes.Equal(current, original) {
				return path
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
	}
	return ""
}
func qaInstallBackupContains(t *testing.T, f qaInstallFixture, want []byte) {
	t.Helper()
	found := false
	for _, value := range qaInstallBackups(t, f) {
		if value.Data == string(want) {
			found = true
			if value.Mode.Perm() != 0600 {
				t.Fatalf("exact backup not private: %s", value.Mode)
			}
		}
	}
	if !found {
		t.Fatal("no exact-byte backup before replacement")
	}
	info, err := os.Stat(filepath.Dir(f.state))
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("metadata/staging directory not private: %v %v", info, err)
	}
}
func TestQAInstallTransactionPreparationFailuresPreserveTargets(t *testing.T) {
	for _, phase := range []string{"install_backup", "install_stage_sync", "install_intent", "install_target_rename"} {
		t.Run(phase, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			originals := qaInstallOriginals(t, f)
			s := New(f.options)
			in := qaInstallInput(t, s, f.intent("both", "project"), 21)
			hit := 0
			s.fail = func(at string) error {
				if at == phase {
					hit++
					return errors.New("PRIVATE_SETTINGS_SENTINEL injected disk failure")
				}
				return nil
			}
			_, err := s.ApplyInstall(context.Background(), in)
			if hit == 0 {
				t.Fatalf("required phase %s never reached", phase)
			}
			if err == nil {
				t.Fatal("failed preparation acknowledged success")
			}
			var safe *Error
			if !errors.As(err, &safe) || safe.Uncertain {
				t.Fatalf("before-first-replacement fault not a definite safe failure: %#v", err)
			}
			if strings.Contains(err.Error(), "PRIVATE_SETTINGS_SENTINEL") {
				t.Fatal("raw failure escaped")
			}
			for path, original := range originals {
				if !bytes.Equal(qaInstallRead(t, path), original) {
					t.Fatalf("preparation failure changed %s", path)
				}
			}
			_, err = New(f.options).ApplyInstall(context.Background(), in)
			if err != nil {
				t.Fatalf("exact retry cannot finish after definite preparation failure: %v", err)
			}
			for _, host := range []string{"codex", "claude"} {
				qaInstallOwnedCommands(t, f.target(host, "project"), f.executable)
			}
		})
	}
}
func TestQAInstallTransactionPartialReplayAndPrivateExactBackups(t *testing.T) {
	for _, phase := range []string{"second_rename", "install_target_sync", "install_complete"} {
		t.Run(phase, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			originals := qaInstallOriginals(t, f)
			s := New(f.options)
			in := qaInstallInput(t, s, f.intent("both", "project"), 22)
			calls := 0
			s.fail = func(at string) error {
				if phase == "second_rename" && at == "install_target_rename" {
					calls++
					if calls == 2 {
						return errors.New("injected interruption")
					}
				}
				if at == phase {
					calls++
					return errors.New("injected interruption")
				}
				return nil
			}
			_, err := s.ApplyInstall(context.Background(), in)
			qaInstallError(t, err, "local_write_unknown")
			if calls == 0 {
				t.Fatal("fault was not exercised")
			}
			for _, original := range originals {
				qaInstallBackupContains(t, f, original)
			}
			if qaInstallChangedTarget(t, f, originals) == "" {
				t.Fatal("unknown-outcome test never crossed a real destination replacement")
			}
			ledger := qaInstallRead(t, f.state)
			if bytes.Contains(ledger, []byte("PRIVATE_SETTINGS_SENTINEL")) {
				t.Fatal("journal copied raw foreign configuration")
			}
			backups := qaInstallBackups(t, f)
			result, err := New(f.options).ApplyInstall(context.Background(), in)
			if err != nil {
				t.Fatalf("same-ID restart failed to reconcile partial files: %v", err)
			}
			qaInstallCheckNoPromotion(t, result)
			for _, host := range []string{"codex", "claude"} {
				qaInstallOwnedCommands(t, f.target(host, "project"), f.executable)
			}
			if !reflect.DeepEqual(backups, qaInstallBackups(t, f)) {
				t.Fatal("partial replay replaced/duplicated original backups")
			}
			before := qaInstallTree(t, f.root)
			again, err := New(f.options).ApplyInstall(context.Background(), in)
			if err != nil || !reflect.DeepEqual(result, again) {
				t.Fatalf("completed replay changed result: %+v %v", again, err)
			}
			qaInstallAssertUnchanged(t, f, before)
		})
	}
}
func TestQAInstallTransactionReplayNeverOverwritesInterveningEdit(t *testing.T) {
	f := qaNewInstallFixture(t)
	originals := qaInstallOriginals(t, f)
	s := New(f.options)
	in := qaInstallInput(t, s, f.intent("both", "project"), 23)
	renames := 0
	s.fail = func(at string) error {
		if at == "install_target_rename" {
			renames++
			if renames == 2 {
				return errors.New("injected crash")
			}
		}
		return nil
	}
	_, err := s.ApplyInstall(context.Background(), in)
	qaInstallError(t, err, "local_write_unknown")
	changed := qaInstallChangedTarget(t, f, originals)
	if changed == "" {
		t.Fatal("fixture did not replace a target before crash")
	}
	userEdit := append(qaInstallRead(t, changed), []byte(" \n")...)
	qaInstallWrite(t, changed, userEdit)
	before := qaInstallTree(t, f.root)
	_, err = New(f.options).ApplyInstall(context.Background(), in)
	if err == nil {
		t.Fatal("partial replay silently absorbed intervening user edit")
	}
	if !bytes.Equal(qaInstallRead(t, changed), userEdit) {
		t.Fatal("partial replay overwrote edited destination")
	}
	qaInstallAssertUnchanged(t, f, before)
}

type qaInstallChildInput struct {
	OptionsHome, OptionsState, Executable, Project, Runtime string
	Input                                                   ApplyInstallInput
}
type qaInstallChildResult struct {
	Code   string
	Result HookList
}

func TestQAInstallProcessHelper(t *testing.T) {
	raw := os.Getenv("TEMPO_QA_INSTALL_CHILD")
	if raw == "" {
		return
	}
	var child qaInstallChildInput
	if err := json.Unmarshal([]byte(raw), &child); err != nil {
		t.Fatal(err)
	}
	var barrier [1]byte
	if _, err := io.ReadFull(os.Stdin, barrier[:]); err != nil {
		t.Fatal(err)
	}
	options := Options{CodexSystemDir: filepath.Join(filepath.Dir(child.OptionsHome), "system-codex"), ClaudeManagedDir: filepath.Join(filepath.Dir(child.OptionsHome), "system-claude"), Path: child.OptionsState, HomeDir: child.OptionsHome, Executable: child.Executable, BuildVersion: "qa-build-15", LockTimeout: time.Second, DiscoverRuntime: func(_ context.Context, host string) (Runtime, error) {
		version := "0.159.3"
		if host == "claude" {
			version = "2.1.286"
		}
		return Runtime{Path: child.Runtime, Version: version, Surface: "local"}, nil
	}}
	result, err := New(options).ApplyInstall(context.Background(), child.Input)
	output := qaInstallChildResult{Result: result}
	if err != nil {
		var safe *Error
		if !errors.As(err, &safe) {
			t.Fatal(err)
		}
		output.Code = safe.Code
	}
	b, _ := json.Marshal(output)
	fmt.Printf("QA_INSTALL_RESULT:%s\n", b)
}
func qaInstallConcurrentChildren(t *testing.T, f qaInstallFixture, inputs []ApplyInstallInput) []qaInstallChildResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	commands := make([]*exec.Cmd, len(inputs))
	writers := make([]io.WriteCloser, len(inputs))
	buffers := make([]bytes.Buffer, len(inputs))
	for i, in := range inputs {
		payload, _ := json.Marshal(qaInstallChildInput{OptionsHome: f.home, OptionsState: f.state, Executable: f.executable, Project: f.project, Runtime: filepath.Join(f.root, "inert-runtime"), Input: in})
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestQAInstallProcessHelper$", "-test.count=1")
		cmd.Env = append(os.Environ(), "TEMPO_QA_INSTALL_CHILD="+string(payload))
		cmd.Stdout = &buffers[i]
		cmd.Stderr = &buffers[i]
		writers[i], err = cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err = cmd.Start(); err != nil {
			t.Fatal(err)
		}
		commands[i] = cmd
	}
	for _, writer := range writers {
		if _, err = writer.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		writer.Close()
	}
	results := make([]qaInstallChildResult, len(inputs))
	for i, cmd := range commands {
		if err = cmd.Wait(); err != nil {
			t.Fatalf("synthetic installer subprocess: %v %s", err, buffers[i].String())
		}
		found := false
		for _, line := range strings.Split(buffers[i].String(), "\n") {
			if strings.HasPrefix(line, "QA_INSTALL_RESULT:") {
				if err = json.Unmarshal([]byte(strings.TrimPrefix(line, "QA_INSTALL_RESULT:")), &results[i]); err != nil {
					t.Fatal(err)
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("missing subprocess result: %s", buffers[i].String())
		}
	}
	return results
}
func TestQAInstallConcurrentProcessesSerializeSameDestination(t *testing.T) {
	f := qaNewInstallFixture(t)
	s := New(f.options)
	intent := f.intent("codex", "project")
	first := qaInstallInput(t, s, intent, 31)
	second := first
	second.RequestID = qaInstallID(32)
	results := qaInstallConcurrentChildren(t, f, []ApplyInstallInput{first, second})
	success := 0
	for _, result := range results {
		if result.Code == "" {
			success++
		} else if result.Code != "revision_conflict" {
			t.Fatalf("unexpected concurrent result: %+v", result)
		}
	}
	if success != 1 {
		t.Fatalf("want exactly one stale-preview winner: %+v", results)
	}
	qaInstallOwnedCommands(t, f.target("codex", "project"), f.executable)
}
func TestQAInstallConcurrentScopesPreserveSharedOwnershipAndReplay(t *testing.T) {
	f := qaNewInstallFixture(t)
	s := New(f.options)
	intents := []InstallIntent{f.intent("codex", "user"), f.intent("claude", "project")}
	inputs := []ApplyInstallInput{qaInstallInput(t, s, intents[0], 33), qaInstallInput(t, s, intents[1], 34)}
	results := qaInstallConcurrentChildren(t, f, inputs)
	for i, result := range results {
		if result.Code == "revision_conflict" {
			inputs[i] = qaInstallInput(t, New(f.options), intents[i], 33+i)
			var err error
			results[i].Result, err = New(f.options).ApplyInstall(context.Background(), inputs[i])
			if err != nil {
				t.Fatal(err)
			}
		} else if result.Code != "" {
			t.Fatalf("shared-ledger race: %+v", result)
		}
	}
	for _, selected := range []struct{ host, scope string }{{"codex", "user"}, {"claude", "project"}} {
		qaInstallOwnedCommands(t, f.target(selected.host, selected.scope), f.executable)
	}
	before := qaInstallTree(t, f.root)
	for i, in := range inputs {
		replayed, err := New(f.options).ApplyInstall(context.Background(), in)
		if err != nil || !reflect.DeepEqual(replayed, results[i].Result) {
			t.Fatalf("concurrent scope lost replay %d: %+v %v", i, replayed, err)
		}
	}
	qaInstallAssertUnchanged(t, f, before)
	// Both ownership records must remain actionable after the independent writers.
	for i, intent := range intents {
		intent.Operation = "uninstall"
		qaInstallApply(t, New(f.options), intent, 40+i)
	}
}

func TestQAInstallRechecksDestinationImmediatelyBeforeReplacement(t *testing.T) {
	f := qaNewInstallFixture(t)
	qaInstallOriginals(t, f)
	s := New(f.options)
	in := qaInstallInput(t, s, f.intent("both", "project"), 81)
	target := f.target("codex", "project")
	foreign := []byte(`{"private":"PRIVATE_SETTINGS_SENTINEL changed immediately before replacement"}`)
	injected := false
	s.fail = func(at string) error {
		if at == "install_target_rename" && !injected {
			injected = true
			qaInstallWrite(t, target, foreign)
		}
		return nil
	}
	_, err := s.ApplyInstall(context.Background(), in)
	if !injected {
		t.Fatal("replacement boundary not reached")
	}
	if err == nil {
		t.Fatal("foreign editor race was overwritten/acknowledged")
	}
	if !bytes.Equal(qaInstallRead(t, target), foreign) {
		t.Fatal("replacement lost intervening foreign edit")
	}
}

func TestQAInstallRejectsParentSwapAtReplacement(t *testing.T) {
	f := qaNewInstallFixture(t)
	qaInstallOriginals(t, f)
	s := New(f.options)
	in := qaInstallInput(t, s, f.intent("both", "project"), 82)
	parent := filepath.Dir(f.target("codex", "project"))
	saved := filepath.Join(f.project, "saved-original-codex")
	outside := filepath.Join(f.root, "outside-destination")
	sentinel := []byte(`{"private":"PRIVATE_SETTINGS_SENTINEL outside reviewed destination"}`)
	qaInstallWrite(t, filepath.Join(outside, "hooks.json"), sentinel)
	injected := false
	s.fail = func(at string) error {
		if at == "install_target_rename" && !injected {
			injected = true
			if err := os.Rename(parent, saved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, parent); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}
	_, err := s.ApplyInstall(context.Background(), in)
	if !injected {
		t.Fatal("replacement boundary not reached")
	}
	if err == nil {
		t.Fatal("swapped parent accepted")
	}
	if !bytes.Equal(qaInstallRead(t, filepath.Join(outside, "hooks.json")), sentinel) {
		t.Fatal("replacement escaped into unreviewed symlink destination")
	}
	if _, err := os.Stat(filepath.Join(saved, "hooks.json")); err != nil {
		t.Fatalf("original configuration lost after parent swap: %v", err)
	}
}
