package themes_test

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
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/themes"
)

func qaThemeID(n int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", n) }

func qaThemePrivateTemp(t *testing.T) string {
	t.Helper()
	// Go's testing.TempDir leaf uses Mkdir(0777) and can be 0755 under the
	// process umask. Valid preference fixtures need an explicitly private leaf.
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func qaThemeService(path string) *themes.Service {
	return themes.New(themes.Options{Path: path, Getenv: func(string) string { panic("explicit preference path read environment") }})
}

func qaThemeCode(t *testing.T, err error, code string) *themes.Error {
	t.Helper()
	var safe *themes.Error
	if !errors.As(err, &safe) || safe.Code != code {
		t.Fatalf("error = %v, want safe code %s", err, code)
	}
	return safe
}

func qaThemeMutation(t *testing.T, got activity.MutationResult, request, transaction, preference string, changed bool) {
	t.Helper()
	if got.ContractVersion != 1 || got.RequestID != request || got.SnapshotRevision != transaction || got.EntityRevision == nil || *got.EntityRevision != preference || got.Changed != changed || got.AffectedIDs == nil || len(got.AffectedIDs) != 0 {
		t.Fatalf("mutation = %+v (entity %v), want request %s transaction %s preference %s changed %v and [] affected IDs", got, got.EntityRevision, request, transaction, preference, changed)
	}
}

func qaThemeSet(t *testing.T, s *themes.Service, theme, revision string, id int) activity.MutationResult {
	t.Helper()
	result, err := s.Set(context.Background(), themes.SetInput{Theme: theme, IfRevision: revision, RequestID: qaThemeID(id)})
	if err != nil {
		t.Fatalf("Set(%s,%s,%d): %v", theme, revision, id, err)
	}
	return result
}

func qaThemeSaved(t *testing.T, path, want, revision string) {
	t.Helper()
	got, err := qaThemeService(path).Show(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if got.ContractVersion != 1 || got.PreferenceRevision != revision || got.Theme.ID != want || !got.Theme.Selected {
		t.Fatalf("saved = %+v, want %s revision %s", got, want, revision)
	}
}

func qaThemeRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestQAPreferencesAbsentReadAndPreviewAreSideEffectFree(t *testing.T) {
	root := qaThemePrivateTemp(t)
	path := filepath.Join(root, "missing", "preferences.json")
	s := qaThemeService(path)
	list, err := s.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if list.ContractVersion != 1 || list.PreferenceRevision != "0" || len(list.Themes) != 4 {
		t.Fatalf("initial list: %+v", list)
	}
	selected := 0
	for _, item := range list.Themes {
		if item.Selected {
			selected++
			if item.ID != "terminal-default" {
				t.Errorf("wrong initial selection %s", item.ID)
			}
		}
	}
	if selected != 1 {
		t.Errorf("selected count = %d", selected)
	}
	for _, id := range []string{"", "terminal-default", "tokyo-night", "gruvbox", "catppuccin"} {
		got, err := s.Show(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		want := id
		if want == "" {
			want = "terminal-default"
		}
		if got.PreferenceRevision != "0" || got.Theme.ID != want || got.Theme.Selected != (want == "terminal-default") {
			t.Errorf("initial preview %q: %+v", id, got)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("absent reads created files/directories: %v err=%v", entries, err)
	}
}

func TestQAPreferencesRevisionReplayResetAndNoOpSemantics(t *testing.T) {
	root := qaThemePrivateTemp(t)
	path := filepath.Join(root, "preferences.json")
	// Unrelated stores are sentinels, never initialized or parsed by themes.
	for _, name := range []string{"activity-state.json", "config.json", "hooks.json"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("foreign synthetic bytes"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	s := qaThemeService(path)
	initial := qaThemeSet(t, s, "terminal-default", "0", 1)
	qaThemeMutation(t, initial, qaThemeID(1), "1", "0", false)
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0600 {
		t.Fatalf("receipt store permissions: %v %v", fi, err)
	}
	changed := qaThemeSet(t, qaThemeService(path), "tokyo-night", "0", 2)
	qaThemeMutation(t, changed, qaThemeID(2), "2", "1", true)
	beforeReplay := qaThemeRead(t, path)
	replay := qaThemeSet(t, qaThemeService(path), "terminal-default", "0", 1)
	if !reflect.DeepEqual(replay, initial) {
		t.Fatalf("replay changed historical result: %+v vs %+v", replay, initial)
	}
	if !bytes.Equal(beforeReplay, qaThemeRead(t, path)) {
		t.Error("exact replay rewrote preference/receipt contents")
	}
	qaThemeSaved(t, path, "tokyo-night", "1")
	_, err := s.Set(context.Background(), themes.SetInput{Theme: "tokyo-night", IfRevision: "0", RequestID: qaThemeID(3)})
	qaThemeCode(t, err, "revision_conflict") // stale no-op must reject
	_, err = s.Set(context.Background(), themes.SetInput{Theme: "gruvbox", IfRevision: "1", RequestID: qaThemeID(1)})
	qaThemeCode(t, err, "request_conflict") // known ID intent check before revision
	reset, err := s.Reset(context.Background(), themes.ResetInput{IfRevision: "1", RequestID: qaThemeID(3)})
	if err != nil {
		t.Fatal(err)
	}
	qaThemeMutation(t, reset, qaThemeID(3), "3", "2", true)
	resetReplay := qaThemeSet(t, qaThemeService(path), "terminal-default", "1", 3)
	if !reflect.DeepEqual(resetReplay, reset) {
		t.Fatal("Reset not canonical equivalent of Set default")
	}
	noOp := qaThemeSet(t, s, "terminal-default", "2", 4)
	qaThemeMutation(t, noOp, qaThemeID(4), "4", "2", false)
	qaThemeSaved(t, path, "terminal-default", "2")
	for _, name := range []string{"activity-state.json", "config.json", "hooks.json"} {
		if string(qaThemeRead(t, filepath.Join(root, name))) != "foreign synthetic bytes" {
			t.Errorf("themes changed %s", name)
		}
	}
	for _, entry := range mustThemeDir(t, root) {
		switch entry.Name() {
		case "preferences.json", "preferences.json.lock", "activity-state.json", "config.json", "hooks.json":
		default:
			t.Errorf("unexpected side-effect file: %s", entry.Name())
		}
	}
}

func mustThemeDir(t *testing.T, path string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func TestQAPreferencesExplicitAndOmittedRevisionAreDifferentIntents(t *testing.T) {
	path := filepath.Join(qaThemePrivateTemp(t), "preferences.json")
	s := qaThemeService(path)
	first := qaThemeSet(t, s, "tokyo-night", "", 10)
	qaThemeMutation(t, first, qaThemeID(10), "1", "1", true)
	_, err := s.Set(context.Background(), themes.SetInput{Theme: "tokyo-night", IfRevision: "0", RequestID: qaThemeID(10)})
	qaThemeCode(t, err, "request_conflict")
	qaThemeSet(t, s, "gruvbox", "", 11)
	qaThemeSaved(t, path, "gruvbox", "2")
	if got := qaThemeSet(t, s, "tokyo-night", "", 10); !reflect.DeepEqual(got, first) {
		t.Error("omitted revision replay changed")
	}
	qaThemeSaved(t, path, "gruvbox", "2")
}

func TestQAPreferencesPreviewReadsCurrentSavedStateWithoutWriting(t *testing.T) {
	path := filepath.Join(qaThemePrivateTemp(t), "preferences.json")
	s := qaThemeService(path)
	qaThemeSet(t, s, "catppuccin", "0", 20)
	before := qaThemeRead(t, path)
	for _, id := range []string{"", "catppuccin", "gruvbox"} {
		got, err := s.Show(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		want := id
		if want == "" {
			want = "catppuccin"
		}
		if got.Theme.ID != want || got.Theme.Selected != (want == "catppuccin") || got.PreferenceRevision != "1" {
			t.Errorf("preview %q: %+v", id, got)
		}
	}
	list, err := s.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, theme := range list.Themes {
		if theme.Selected != (theme.ID == "catppuccin") {
			t.Errorf("selection %s=%v", theme.ID, theme.Selected)
		}
	}
	if !bytes.Equal(before, qaThemeRead(t, path)) {
		t.Error("read/preview changed persistent state")
	}
}

func TestQAPreferencesInvalidInputsFailBeforeIO(t *testing.T) {
	for _, input := range []themes.SetInput{
		{Theme: "unknown", RequestID: qaThemeID(30)}, {Theme: "tokyo-night", IfRevision: "01", RequestID: qaThemeID(30)},
		{Theme: "tokyo-night", IfRevision: "-1", RequestID: qaThemeID(30)}, {Theme: "tokyo-night", IfRevision: "18446744073709551616", RequestID: qaThemeID(30)},
		{Theme: "tokyo-night", RequestID: "not-a-uuid"}, {Theme: "tokyo-night", RequestID: "00000000-0000-4000-8000-00000000000A"},
	} {
		t.Run(fmt.Sprint(input), func(t *testing.T) {
			root := qaThemePrivateTemp(t)
			s := qaThemeService(filepath.Join(root, "missing", "preferences.json"))
			_, err := s.Set(context.Background(), input)
			qaThemeCode(t, err, "validation")
			if len(mustThemeDir(t, root)) != 0 {
				t.Fatal("invalid input performed filesystem writes")
			}
		})
	}
	for _, timeout := range []time.Duration{-time.Millisecond, time.Second + time.Nanosecond} {
		root := qaThemePrivateTemp(t)
		s := themes.New(themes.Options{Path: filepath.Join(root, "missing", "preferences.json"), LockTimeout: timeout})
		_, err := s.List(context.Background())
		qaThemeCode(t, err, "validation")
		if len(mustThemeDir(t, root)) != 0 {
			t.Fatal("invalid timeout created state")
		}
	}
}

func TestQAPreferencesCorruptFutureAndUnsafeStoresPreserved(t *testing.T) {
	for _, fixture := range []struct{ name, body, code string }{
		{"malformed", "private-payload-do-not-disclose", "state_corrupt"},
		{"future", `{"format_version":2,"snapshot_revision":"0","preference_revision":"0","selected_theme":"terminal-default","requests":{}}`, "unsupported_contract"},
		{"unknown-theme", `{"format_version":1,"snapshot_revision":"0","preference_revision":"0","selected_theme":"unknown","requests":{}}`, "state_corrupt"},
		{"leading-zero", `{"format_version":1,"snapshot_revision":"01","preference_revision":"0","selected_theme":"terminal-default","requests":{}}`, "state_corrupt"},
		{"trailing-json", `{"format_version":1} {}`, "state_corrupt"},
		{"oversized", strings.Repeat("x", (4<<20)+1), "state_corrupt"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			path := filepath.Join(qaThemePrivateTemp(t), "preferences.json")
			if err := os.WriteFile(path, []byte(fixture.body), 0600); err != nil {
				t.Fatal(err)
			}
			s := qaThemeService(path)
			for _, mutate := range []bool{false, true} {
				var err error
				if mutate {
					_, err = s.Set(context.Background(), themes.SetInput{Theme: "gruvbox", RequestID: qaThemeID(40)})
				} else {
					_, err = s.List(context.Background())
				}
				safe := qaThemeCode(t, err, fixture.code)
				if strings.Contains(safe.Error(), "private-payload") || strings.Contains(safe.Error(), path) {
					t.Error("raw private state/path leaked")
				}
				if !bytes.Equal([]byte(fixture.body), qaThemeRead(t, path)) {
					t.Error("rejected state was overwritten")
				}
			}
		})
	}
	for _, hazard := range []string{"symlink", "hardlink", "public-permissions", "directory"} {
		t.Run(hazard, func(t *testing.T) {
			root := qaThemePrivateTemp(t)
			original := filepath.Join(root, "foreign")
			path := filepath.Join(root, "preferences.json")
			if err := os.WriteFile(original, []byte("foreign bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			var err error
			switch hazard {
			case "symlink":
				err = os.Symlink(original, path)
			case "hardlink":
				err = os.Link(original, path)
			case "public-permissions":
				err = os.WriteFile(path, []byte("foreign bytes"), 0644)
			case "directory":
				err = os.Mkdir(path, 0700)
			}
			if err != nil {
				t.Fatal(err)
			}
			s := qaThemeService(path)
			_, err = s.List(context.Background())
			qaThemeCode(t, err, "state_corrupt")
			_, err = s.Set(context.Background(), themes.SetInput{Theme: "gruvbox", RequestID: qaThemeID(41)})
			qaThemeCode(t, err, "state_corrupt")
			if string(qaThemeRead(t, original)) != "foreign bytes" {
				t.Error("unsafe target modified")
			}
		})
	}
}

func TestQAPreferencesFaultBoundariesAndDurableRetry(t *testing.T) {
	for _, stage := range []string{"before_write", "file_sync", "rename"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(qaThemePrivateTemp(t), "preferences.json")
			qaThemeSet(t, qaThemeService(path), "tokyo-night", "0", 50)
			before := qaThemeRead(t, path)
			calls := 0
			s := themes.New(themes.Options{Path: path, Fault: func(got string) error {
				if got == stage {
					calls++
					return errors.New("synthetic secret fault")
				}
				return nil
			}})
			_, err := s.Set(context.Background(), themes.SetInput{Theme: "gruvbox", IfRevision: "1", RequestID: qaThemeID(51)})
			safe := qaThemeCode(t, err, "state_corrupt")
			if safe.Uncertain || strings.Contains(safe.Error(), "synthetic secret") {
				t.Errorf("unsafe definite fault result %+v", safe)
			}
			if calls != 1 || !bytes.Equal(before, qaThemeRead(t, path)) {
				t.Errorf("definite failure changed committed data or fault missed: calls%d", calls)
			}
			qaThemeSaved(t, path, "tokyo-night", "1")
			qaThemeMutation(t, qaThemeSet(t, qaThemeService(path), "gruvbox", "1", 51), qaThemeID(51), "2", "2", true)
		})
	}
	t.Run("directory-sync", func(t *testing.T) {
		path := filepath.Join(qaThemePrivateTemp(t), "preferences.json")
		calls := 0
		s := themes.New(themes.Options{Path: path, Fault: func(stage string) error {
			if stage == "directory_sync" {
				calls++
				return errors.New("private sync failure")
			}
			return nil
		}})
		input := themes.SetInput{Theme: "tokyo-night", IfRevision: "0", RequestID: qaThemeID(55)}
		_, err := s.Set(context.Background(), input)
		safe := qaThemeCode(t, err, "local_write_unknown")
		if !safe.Uncertain || safe.Details["request_id"] != qaThemeID(55) {
			t.Fatalf("uncertain result lacks retained intent ID: %+v", safe)
		}
		qaThemeSaved(t, path, "tokyo-night", "1")
		_, err = s.Set(context.Background(), input)
		qaThemeCode(t, err, "local_write_unknown")
		if calls != 2 {
			t.Errorf("same-ID replay did not retry durability, calls=%d", calls)
		}
		result, err := qaThemeService(path).Set(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		qaThemeMutation(t, result, qaThemeID(55), "1", "1", true)
		qaThemeSet(t, qaThemeService(path), "catppuccin", "1", 56)
		later, err := qaThemeService(path).Set(context.Background(), input)
		if err != nil || !reflect.DeepEqual(later, result) {
			t.Fatalf("replay after later update=%+v err=%v", later, err)
		}
		qaThemeSaved(t, path, "catppuccin", "2")
	})
}

func TestQAPreferencesReceiptIntegrityIsValidated(t *testing.T) {
	for _, field := range []string{"snapshot_revision", "preference_revision", "selected_theme", "requests"} {
		t.Run(field, func(t *testing.T) {
			path := filepath.Join(qaThemePrivateTemp(t), "preferences.json")
			qaThemeSet(t, qaThemeService(path), "tokyo-night", "0", 60)
			var state map[string]any
			if err := json.Unmarshal(qaThemeRead(t, path), &state); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "snapshot_revision":
				state[field] = "0"
			case "preference_revision":
				state[field] = "99"
			case "selected_theme":
				state[field] = "gruvbox"
			case "requests":
				state[field] = map[string]any{qaThemeID(60): map[string]any{"result": map[string]any{"changed": true}}}
			}
			body, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = qaThemeService(path).Show(context.Background(), "")
			qaThemeCode(t, err, "state_corrupt")
			if !bytes.Equal(body, qaThemeRead(t, path)) {
				t.Error("corrupt receipt evidence changed")
			}
		})
	}
}

func TestQAPreferencesInjectedPathGeneratedIDsAndCancellation(t *testing.T) {
	root := qaThemePrivateTemp(t)
	path := filepath.Join(root, "isolated", "preferences.json")
	keys := []string{}
	s := themes.New(themes.Options{Getenv: func(key string) string {
		keys = append(keys, key)
		if key == "TEMPO_PREFERENCES" {
			return path
		}
		return ""
	}})
	result, err := s.Set(context.Background(), themes.SetInput{Theme: "gruvbox", IfRevision: "0"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.RequestID) != 36 || result.RequestID[14] != '4' {
		t.Fatalf("missing generated v4 request identity: %q", result.RequestID)
	}
	qaThemeMutation(t, result, result.RequestID, "1", "1", true)
	qaThemeSaved(t, path, "gruvbox", "1")
	for _, key := range keys {
		if key != "TEMPO_PREFERENCES" {
			t.Errorf("unexpected ambient input %s", key)
		}
	}
	if len(keys) == 0 {
		t.Error("injected preference path not resolved")
	}
	if fi, err := os.Stat(filepath.Dir(path)); err != nil || fi.Mode().Perm() != 0700 {
		t.Fatalf("created preference directory not private: %v %v", fi, err)
	}
	before := qaThemeRead(t, path)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.Set(ctx, themes.SetInput{Theme: "catppuccin", IfRevision: "1", RequestID: qaThemeID(80)})
	if err == nil {
		t.Error("already-cancelled mutation succeeded")
	}
	if !bytes.Equal(before, qaThemeRead(t, path)) {
		t.Error("already-cancelled mutation changed persistent state")
	}
}

func TestQAPreferencesUnsafeParentAndLockNeverFollowed(t *testing.T) {
	for _, hazard := range []string{"parent-symlink", "parent-public-permissions", "lock-symlink", "lock-hardlink"} {
		t.Run(hazard, func(t *testing.T) {
			root := qaThemePrivateTemp(t)
			external := filepath.Join(root, "external")
			if err := os.Mkdir(external, 0700); err != nil {
				t.Fatal(err)
			}
			foreign := filepath.Join(external, "foreign")
			if err := os.WriteFile(foreign, []byte("synthetic foreign sentinel"), 0600); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "preferences.json")
			switch hazard {
			case "parent-public-permissions":
				if err := os.Chmod(root, 0755); err != nil {
					t.Fatal(err)
				}
			case "parent-symlink":
				alias := filepath.Join(root, "alias")
				if err := os.Symlink(external, alias); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(alias, "preferences.json")
			case "lock-symlink":
				if err := os.Symlink(foreign, path+".lock"); err != nil {
					t.Fatal(err)
				}
			case "lock-hardlink":
				if err := os.Link(foreign, path+".lock"); err != nil {
					t.Fatal(err)
				}
			}
			_, err := qaThemeService(path).Set(context.Background(), themes.SetInput{Theme: "gruvbox", RequestID: qaThemeID(81)})
			qaThemeCode(t, err, "state_corrupt")
			if hazard == "parent-public-permissions" {
				if fi, err := os.Stat(root); err != nil || fi.Mode().Perm() != 0755 {
					t.Errorf("unsafe existing parent permissions changed: %v %v", fi, err)
				}
			}
			if string(qaThemeRead(t, foreign)) != "synthetic foreign sentinel" {
				t.Error("followed unsafe parent/lock into external file")
			}
			if len(mustThemeDir(t, external)) != 1 {
				t.Error("unsafe target gained preference files")
			}
		})
	}
}
