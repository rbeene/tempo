package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/themes"
)

type qaThemeCLI struct {
	t                  *testing.T
	root, path, config string
	preferenceLookups  int
	deps               cli.Dependencies
}

func qaNewThemeCLI(t *testing.T) *qaThemeCLI {
	t.Helper()
	f := &qaThemeCLI{t: t, root: t.TempDir()}
	f.path = filepath.Join(f.root, "appearance", "preferences.json")
	f.config = filepath.Join(f.root, "config.json")
	if err := os.WriteFile(f.config, []byte("PRIVATE-THEME-ACCOUNT-SENTINEL"), 0600); err != nil {
		t.Fatal(err)
	}
	getenv := func(k string) string {
		switch k {
		case "TEMPO_PREFERENCES":
			f.preferenceLookups++
			return f.path
		case "TEMPO_CONFIG":
			return f.config
		case "TEMPO_STATE":
			return filepath.Join(f.root, "activity.json")
		case "TERM":
			return "xterm-256color"
		case "COLORTERM":
			return "truecolor"
		}
		return ""
	}
	provider := func(string, string) harvest.Provider { t.Fatal("theme command accessed Harvest"); return nil }
	fixed := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	f.deps = cli.Dependencies{
		Getenv: getenv, ConfigPath: f.config, Store: qaForbiddenLegacyStore{t}, Prompter: qaForbiddenPrompt{t},
		TerminalEligible: func(io.Reader, io.Writer) bool { return false }, NewProvider: provider,
		Now: func() time.Time { return fixed }, SaveConfig: func(string, auth.Config) error { t.Fatal("theme command changed account config"); return nil },
		Activity: activity.New(activity.Options{Path: filepath.Join(f.root, "activity.json"), Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
			return activity.ClockSample{Capability: "unavailable", WallUTC: fixed}, nil
		})}),
		Auth: auth.NewService(auth.Options{ConfigPath: f.config, LockPath: filepath.Join(f.root, "auth.lock"), Getenv: getenv, NewProvider: provider,
			PersistentAvailable: func() bool { t.Fatal("theme command inspected native credentials"); return false },
			Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
				t.Fatal("theme command used native credentials")
				return auth.NativeReply{}, nil
			}),
		}),
	}
	t.Cleanup(func() {
		b, err := os.ReadFile(f.config)
		if err != nil || string(b) != "PRIVATE-THEME-ACCOUNT-SENTINEL" {
			t.Errorf("account config changed: %q %v", b, err)
		}
		entries, err := os.ReadDir(f.root)
		if err != nil {
			t.Error(err)
			return
		}
		for _, e := range entries {
			if e.Name() != "appearance" && e.Name() != "config.json" {
				t.Errorf("unrelated state/identity/lock created: %s", e.Name())
			}
		}
	})
	return f
}

func (f *qaThemeCLI) run(args ...string) result {
	f.t.Helper()
	var out, stderr bytes.Buffer
	code := cli.Run(context.Background(), args, strings.NewReader(""), &out, &stderr, f.deps)
	return result{code: code, out: out.String(), err: stderr.String()}
}

func qaThemeDecode(t *testing.T, r result, target any) {
	t.Helper()
	if r.code != 0 || r.err != "" {
		t.Fatalf("success code=%d stdout=%q stderr=%q", r.code, r.out, r.err)
	}
	if strings.ContainsRune(r.out, 27) {
		t.Fatal("machine output contained terminal controls")
	}
	var env struct {
		Schema int             `json:"schema_version"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(r.out), &env); err != nil || env.Schema != 1 {
		t.Fatalf("invalid finite envelope %q: %v", r.out, err)
	}
	if err := json.Unmarshal(env.Data, target); err != nil {
		t.Fatal(err)
	}
}

func qaThemeID(n int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", n) }

func TestQAThemesCLIListAndExplicitPreviewDoNotCreateStore(t *testing.T) {
	f := qaNewThemeCLI(t)
	var list themes.ThemeList
	qaThemeDecode(t, f.run("themes", "list", "--json"), &list)
	if list.ContractVersion != 1 || list.PreferenceRevision != "0" || len(list.Themes) != 4 {
		t.Fatalf("list=%+v", list)
	}
	ids := []string{"terminal-default", "tokyo-night", "gruvbox", "catppuccin"}
	for i, item := range list.Themes {
		if item.ID != ids[i] || item.Selected != (i == 0) {
			t.Errorf("initial item=%+v", item)
		}
		var preview themes.ThemeResult
		qaThemeDecode(t, f.run("themes", "show", item.ID, "--json"), &preview)
		if preview.ContractVersion != 1 || preview.PreferenceRevision != "0" || !reflect.DeepEqual(preview.Theme, item) {
			t.Errorf("preview=%+v, item=%+v", preview, item)
		}
	}
	var current themes.ThemeResult
	qaThemeDecode(t, f.run("themes", "show", "--json"), &current)
	if current.Theme.ID != "terminal-default" || !current.Theme.Selected {
		t.Errorf("default=%+v", current)
	}
	if _, err := os.Stat(filepath.Dir(f.path)); !os.IsNotExist(err) {
		t.Errorf("reads created appearance directory: %v", err)
	}
}

func TestQAThemesCLIMutationsShareServiceRevisionAndHistoricalReplay(t *testing.T) {
	f := qaNewThemeCLI(t)
	apply := func(id, rev string, n int) activity.MutationResult {
		t.Helper()
		var m activity.MutationResult
		qaThemeDecode(t, f.run("themes", "set", id, "--if-revision", rev, "--request-id", qaThemeID(n), "--json"), &m)
		return m
	}
	first := apply("tokyo-night", "0", 1)
	if first.ContractVersion != 1 || first.SnapshotRevision != "1" || !first.Changed || first.RequestID != qaThemeID(1) || first.EntityRevision == nil || *first.EntityRevision != "1" || first.AffectedIDs == nil || len(first.AffectedIDs) != 0 {
		t.Fatalf("mutation=%+v", first)
	}
	svc := themes.New(themes.Options{Path: f.path})
	shown, err := svc.Show(context.Background(), "")
	if err != nil || shown.Theme.ID != "tokyo-night" || shown.PreferenceRevision != "1" {
		t.Fatalf("CLI→service=%+v %v", shown, err)
	}
	second, err := svc.Set(context.Background(), themes.SetInput{Theme: "catppuccin", IfRevision: "1", RequestID: qaThemeID(2)})
	if err != nil {
		t.Fatal(err)
	}
	var current themes.ThemeResult
	qaThemeDecode(t, f.run("themes", "show", "--json"), &current)
	if current.Theme.ID != "catppuccin" || current.PreferenceRevision != "2" || !current.Theme.Selected {
		t.Fatalf("service→CLI=%+v", current)
	}
	if got := apply("tokyo-night", "0", 1); !reflect.DeepEqual(got, first) {
		t.Errorf("historical replay changed %+v", got)
	}
	qaThemeDecode(t, f.run("themes", "show", "--json"), &current)
	if current.Theme.ID != "catppuccin" {
		t.Fatal("historical replay reapplied old theme")
	}
	envelope(t, f.run("themes", "set", "catppuccin", "--if-revision", "1", "--request-id", qaThemeID(3), "--json"), 6, "revision_conflict")
	envelope(t, f.run("themes", "set", "gruvbox", "--if-revision", "0", "--request-id", qaThemeID(1), "--json"), 6, "request_conflict")
	var reset activity.MutationResult
	qaThemeDecode(t, f.run("themes", "reset", "--if-revision", "2", "--request-id", qaThemeID(4), "--json"), &reset)
	if reset.SnapshotRevision != "3" || reset.EntityRevision == nil || *reset.EntityRevision != "3" || !reset.Changed {
		t.Errorf("reset=%+v second=%+v", reset, second)
	}
	if got := apply("terminal-default", "2", 4); !reflect.DeepEqual(got, reset) {
		t.Errorf("Reset and Set default differ: %+v %+v", reset, got)
	}
}

func TestQAThemesCLINoOpGeneratedRequestAndOmittedRevision(t *testing.T) {
	f := qaNewThemeCLI(t)
	// Explicit machine flags also suppress prompts on otherwise eligible streams.
	f.deps.TerminalEligible = func(io.Reader, io.Writer) bool { return true }
	var initial activity.MutationResult
	qaThemeDecode(t, f.run("themes", "reset", "--json"), &initial)
	if initial.Changed || initial.SnapshotRevision != "1" || initial.EntityRevision == nil || *initial.EntityRevision != "0" || len(initial.RequestID) != 36 || initial.AffectedIDs == nil || len(initial.AffectedIDs) != 0 {
		t.Fatalf("generated no-op result=%+v", initial)
	}
	var changed activity.MutationResult
	qaThemeDecode(t, f.run("themes", "set", "gruvbox", "--request-id", qaThemeID(8), "--json"), &changed)
	if !changed.Changed || changed.SnapshotRevision != "2" || changed.EntityRevision == nil || *changed.EntityRevision != "1" {
		t.Fatalf("omitted-revision result=%+v", changed)
	}
	// Output mode isn't canonical intent; switching it preserves the receipt.
	var replay activity.MutationResult
	qaThemeDecode(t, f.run("themes", "set", "gruvbox", "--request-id", qaThemeID(8), "--non-interactive"), &replay)
	if !reflect.DeepEqual(replay, changed) {
		t.Errorf("output flag changed request intent: %+v %+v", changed, replay)
	}
	envelope(t, f.run("themes", "set", "gruvbox", "--request-id", qaThemeID(8), "--if-revision", "0", "--json"), 6, "request_conflict")
	var current themes.ThemeResult
	qaThemeDecode(t, f.run("themes", "show", "--non-interactive"), &current)
	if current.Theme.ID != "gruvbox" || current.PreferenceRevision != "1" {
		t.Fatalf("finite current=%+v", current)
	}
}

func TestQAThemesCLIRejectsInvalidIntentBeforePreferenceIO(t *testing.T) {
	cases := []struct {
		args []string
		code string
	}{
		{[]string{"themes", "set"}, "usage"}, {[]string{"themes", "set", "tokyo-night", "gruvbox"}, "usage"},
		{[]string{"themes", "reset", "tokyo-night"}, "usage"}, {[]string{"themes", "list", "--request-id", qaThemeID(9)}, "usage"},
		{[]string{"themes", "show", "--if-revision", "0"}, "usage"}, {[]string{"themes", "set", "tokyo-night", "--yes"}, "usage"},
		{[]string{"themes", "list", "--account", "1"}, "usage"}, {[]string{"themes", "set", "tokyo-night", "--if-revision", "01"}, "validation"},
		{[]string{"themes", "set", "tokyo-night", "--request-id", "NOT-A-UUID"}, "validation"},
		{[]string{"themes", "set", "unknown"}, "validation"}, {[]string{"themes", "show", "unknown"}, "validation"},
		{[]string{"themes", "set", "tokyo-night", "--if-revision", "0", "--if-revision", "0"}, "usage"},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, "/"), func(t *testing.T) {
			f := qaNewThemeCLI(t)
			r := f.run(append(tc.args, "--json")...)
			envelope(t, r, 2, tc.code)
			if f.preferenceLookups != 0 {
				t.Errorf("invalid input resolved preferences %d times", f.preferenceLookups)
			}
			if _, err := os.Stat(filepath.Dir(f.path)); !os.IsNotExist(err) {
				t.Errorf("invalid intent created store: %v", err)
			}
		})
	}
}

func TestQAThemesCLICorruptionAndFutureVersionPreserved(t *testing.T) {
	for _, tc := range []struct {
		bytes, code string
		exit        int
	}{{"PRIVATE-BROKEN-PREF-SENTINEL", "state_corrupt", 1}, {`{"format_version":2}`, "unsupported_contract", 2}} {
		t.Run(tc.code, func(t *testing.T) {
			f := qaNewThemeCLI(t)
			if err := os.MkdirAll(filepath.Dir(f.path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.path, []byte(tc.bytes), 0600); err != nil {
				t.Fatal(err)
			}
			for _, args := range [][]string{{"themes", "list"}, {"themes", "show"}, {"themes", "set", "gruvbox"}, {"themes", "reset"}} {
				r := f.run(append(args, "--json")...)
				envelope(t, r, tc.exit, tc.code)
				if strings.Contains(r.err, "PRIVATE-") || strings.Contains(r.err, f.root) || strings.ContainsRune(r.err, 27) {
					t.Errorf("unsafe error %q", r.err)
				}
				b, err := os.ReadFile(f.path)
				if err != nil || string(b) != tc.bytes {
					t.Errorf("corruption overwritten: %q %v", b, err)
				}
			}
		})
	}
}

func TestQAThemesCLINonThemeJSONNeverReadsPreferences(t *testing.T) {
	f := qaNewThemeCLI(t)
	for _, args := range [][]string{{"activity", "status", "--json"}, {"activity", "status"}, {"ui", "--json"}, {"schema"}, {"help", "--json"}, {"version", "--json"}, {"bogus", "--json"}} {
		baseline := f.run(args...)
		for _, id := range []string{"terminal-default", "tokyo-night", "gruvbox", "catppuccin"} {
			if err := os.MkdirAll(filepath.Dir(f.path), 0700); err != nil {
				t.Fatal(err)
			}
			// A presentation lookup would fail on this intentionally corrupt file.
			if err := os.WriteFile(f.path, []byte("PRIVATE-CORRUPT-PREFERENCES-"+id), 0600); err != nil {
				t.Fatal(err)
			}
			got := f.run(args...)
			if got.code != baseline.code || got.out != baseline.out || got.err != baseline.err {
				t.Errorf("%v changed machine bytes under %s", args, id)
			}
			if strings.ContainsRune(got.out+got.err, 27) {
				t.Errorf("%v has ANSI", args)
			}
		}
	}
	if f.preferenceLookups != 0 {
		t.Errorf("machine paths resolved preferences %d times", f.preferenceLookups)
	}
}

func TestQAThemesCLIHelpSchemaExposeOfflineActions(t *testing.T) {
	f := qaNewThemeCLI(t)
	help := f.run("help")
	if help.code != 0 || help.err != "" {
		t.Fatalf("offline help=%+v", help)
	}
	// Human help may load a presentation preference. The schema must bypass it.
	f.preferenceLookups = 0
	var schema struct {
		Commands    []cli.Command `json:"commands"`
		Environment []string      `json:"environment"`
	}
	qaThemeDecode(t, f.run("schema"), &schema)
	for name, positional := range map[string]string{"themes list": "", "themes show": "[THEME]", "themes set": "THEME", "themes reset": ""} {
		if !strings.Contains(help.out, name) {
			t.Errorf("help misses %s", name)
		}
		found := false
		for _, c := range schema.Commands {
			if c.Name == name {
				found = true
				if c.Positionals != positional || c.Mutation != (name == "themes set" || name == "themes reset") {
					t.Errorf("schema command=%+v", c)
				}
				if c.Mutation && (c.Flags["if-revision"] != "counter" || c.Flags["request-id"] != "uuid") {
					t.Errorf("missing request/revision flags %+v", c)
				}
			}
		}
		if !found {
			t.Errorf("schema misses %s", name)
		}
	}
	if !strings.Contains(strings.Join(schema.Environment, ","), "TEMPO_PREFERENCES") {
		t.Error("schema omits isolated preference environment")
	}
	if f.preferenceLookups != 0 {
		t.Error("offline schema inspected saved preference")
	}
}

func TestQAThemesCLIUncertainWriteRetainsGeneratedIDAndDurableReplay(t *testing.T) {
	f := qaNewThemeCLI(t)
	fail := true
	calls := 0
	f.deps.Themes = themes.New(themes.Options{Path: f.path, Fault: func(stage string) error {
		if stage == "directory_sync" {
			calls++
			if fail {
				return errors.New("PRIVATE-SYNTHETIC-FSYNC-DETAIL")
			}
		}
		return nil
	}})
	r := f.run("themes", "set", "tokyo-night", "--if-revision", "0", "--json")
	envelope(t, r, 8, "local_write_unknown")
	var env struct {
		Error struct {
			Uncertain bool `json:"uncertain"`
			Retryable bool `json:"retryable"`
			Details   struct {
				RequestID string `json:"request_id"`
			} `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(r.err), &env); err != nil {
		t.Fatal(err)
	}
	id := env.Error.Details.RequestID
	if !env.Error.Uncertain || env.Error.Retryable || len(id) != 36 || id[14] != '4' || strings.Contains(r.err, "PRIVATE-") || strings.Contains(r.err, f.root) {
		t.Fatalf("unsafe unknown result: %q", r.err)
	}
	// A visible replacement is still unknown until the same intent is synced.
	r = f.run("themes", "set", "tokyo-night", "--if-revision", "0", "--request-id", id, "--json")
	envelope(t, r, 8, "local_write_unknown")
	if calls != 2 {
		t.Errorf("unknown replay skipped durability barrier: %d", calls)
	}
	fail = false
	var replay activity.MutationResult
	qaThemeDecode(t, f.run("themes", "set", "tokyo-night", "--if-revision", "0", "--request-id", id, "--json"), &replay)
	if replay.SnapshotRevision != "1" || replay.EntityRevision == nil || *replay.EntityRevision != "1" || replay.RequestID != id || !replay.Changed {
		t.Fatalf("durable replay=%+v", replay)
	}
	if calls != 3 {
		t.Errorf("successful replay skipped durability barrier: %d", calls)
	}
}

func TestQAThemesCLILockConflictPreservesSafeFlags(t *testing.T) {
	f := qaNewThemeCLI(t)
	if err := os.MkdirAll(filepath.Dir(f.path), 0700); err != nil {
		t.Fatal(err)
	}
	// A cancelled context deterministically exercises the same bounded lock
	// category, without adding another native lock fixture to the process suite.
	f.deps.Themes = themes.New(themes.Options{Path: f.path, LockTimeout: time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, stderr bytes.Buffer
	code := cli.Run(ctx, []string{"themes", "set", "gruvbox", "--json"}, strings.NewReader(""), &out, &stderr, f.deps)
	r := result{code: code, out: out.String(), err: stderr.String()}
	envelope(t, r, 6, "state_busy")
	var env struct {
		Error struct{ Retryable, Uncertain bool }
	}
	if err := json.Unmarshal(stderr.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if !env.Error.Retryable || env.Error.Uncertain {
		t.Errorf("busy flags=%+v", env.Error)
	}
	if _, err := os.Stat(f.path); !os.IsNotExist(err) {
		t.Errorf("busy operation wrote preferences: %v", err)
	}
}

var qaThemeSGR = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func qaThemePlain(t *testing.T, text string) string {
	t.Helper()
	plain := qaThemeSGR.ReplaceAllString(text, "")
	if strings.ContainsRune(plain, 27) {
		t.Fatalf("prose emitted non-SGR terminal sequence %q", text)
	}
	return plain
}

func (f *qaThemeCLI) runStreams(outTTY, errTTY bool, args ...string) result {
	f.t.Helper()
	var out, stderr bytes.Buffer
	d := f.deps
	d.OutputTTY = func(w io.Writer) bool {
		switch w {
		case &out:
			return outTTY
		case &stderr:
			return errTTY
		default:
			f.t.Fatal("color policy tested an unrelated output stream")
			return false
		}
	}
	code := cli.Run(context.Background(), args, strings.NewReader(""), &out, &stderr, d)
	return result{code: code, out: out.String(), err: stderr.String()}
}

func TestQAThemesCLIProseUsesSelectedPaletteAndDestinationTTY(t *testing.T) {
	for _, id := range []string{"terminal-default", "tokyo-night", "gruvbox", "catppuccin"} {
		t.Run(id, func(t *testing.T) {
			baseBG := map[string]string{"tokyo-night": "48;2;26;27;38", "gruvbox": "48;2;40;40;40", "catppuccin": "48;2;30;30;46"}[id]
			errorFG := map[string]string{"tokyo-night": "38;2;247;118;142", "gruvbox": "38;2;234;105;98", "catppuccin": "38;2;243;139;168"}[id]
			f := qaNewThemeCLI(t)
			if _, err := themes.New(themes.Options{Path: f.path}).Set(context.Background(), themes.SetInput{Theme: id, RequestID: qaThemeID(20)}); err != nil {
				t.Fatal(err)
			}
			plainHelp := f.runStreams(false, false, "help")
			plainError := f.runStreams(false, false, "unknown-command")
			for _, tty := range []struct{ out, err bool }{{true, false}, {false, true}, {true, true}, {false, false}} {
				help := f.runStreams(tty.out, tty.err, "help")
				bad := f.runStreams(tty.out, tty.err, "unknown-command")
				if help.code != 0 || help.err != "" || qaThemePlain(t, help.out) != plainHelp.out {
					t.Errorf("help changed labels/stream: %+v", help)
				}
				if bad.code != 2 || bad.out != "" || qaThemePlain(t, bad.err) != plainError.err {
					t.Errorf("prose error changed text/stream: %+v", bad)
				}
				if got, want := strings.ContainsRune(help.out, 27), tty.out && id != "terminal-default"; got != want {
					t.Errorf("help ANSI=%v want=%v stdoutTTY=%v stderrTTY=%v", got, want, tty.out, tty.err)
				}
				if got, want := strings.ContainsRune(bad.err, 27), tty.err && id != "terminal-default"; got != want {
					t.Errorf("error ANSI=%v want=%v stdoutTTY=%v stderrTTY=%v", got, want, tty.out, tty.err)
				}
				if id != "terminal-default" && tty.out && !strings.Contains(help.out, baseBG) {
					t.Errorf("help did not use %s selected palette background", id)
				}
				if id != "terminal-default" && tty.err && (!strings.Contains(bad.err, baseBG) || !strings.Contains(bad.err, errorFG)) {
					t.Errorf("error did not use selected palette/error role: %q", bad.err)
				}
				for _, output := range []string{help.out, bad.err} {
					if strings.ContainsRune(output, 27) && !strings.HasSuffix(strings.TrimSuffix(output, "\n"), "\x1b[0m") && !strings.HasSuffix(output, "\x1b[0m") {
						t.Errorf("finite prose failed final inherited-style cleanup for %s", id)
					}
				}
			}
		})
	}
}

func TestQAThemesCLIProseNoColorAndCorruptFallbackAreSafe(t *testing.T) {
	f := qaNewThemeCLI(t)
	if _, err := themes.New(themes.Options{Path: f.path}).Set(context.Background(), themes.SetInput{Theme: "tokyo-night", RequestID: qaThemeID(21)}); err != nil {
		t.Fatal(err)
	}
	getenv := f.deps.Getenv
	for _, caps := range []struct{ term, noColor string }{{"xterm-256color", "0"}, {"dumb", ""}, {"unknown", ""}, {"", ""}} {
		f.deps.Getenv = func(k string) string {
			switch k {
			case "TERM":
				return caps.term
			case "NO_COLOR":
				return caps.noColor
			}
			return getenv(k)
		}
		for _, args := range [][]string{{"help"}, {"unknown-command"}} {
			r := f.runStreams(true, true, args...)
			if strings.ContainsRune(r.out+r.err, 27) {
				t.Errorf("disabled prose emitted colors: %q %q", r.out, r.err)
			}
		}
	}
	f.deps.Getenv = getenv
	if err := os.WriteFile(f.path, []byte("PRIVATE-PROSE-PREF-CORRUPTION"), 0600); err != nil {
		t.Fatal(err)
	}
	r := f.runStreams(true, true, "help")
	if r.code != 0 || !strings.Contains(r.out, "Usage:") || strings.ContainsRune(r.out+r.err, 27) || strings.Contains(r.out+r.err, "PRIVATE-") || strings.Contains(r.out+r.err, f.root) {
		t.Errorf("unsafe fallback: %+v", r)
	}
	if !strings.Contains(strings.ToLower(r.err), "appearance") && !strings.Contains(strings.ToLower(r.err), "preference") {
		t.Errorf("corrupt presentation fallback lacked safe warning: %q", r.err)
	}
	if b, err := os.ReadFile(f.path); err != nil || string(b) != "PRIVATE-PROSE-PREF-CORRUPTION" {
		t.Errorf("fallback reset corruption: %q %v", b, err)
	}
}

func TestQAThemesCLIExplicitHumanPreviewUsesCandidateWithoutSaving(t *testing.T) {
	f := qaNewThemeCLI(t)
	f.deps.TerminalEligible = func(io.Reader, io.Writer) bool { return true }
	if _, err := themes.New(themes.Options{Path: f.path}).Set(context.Background(), themes.SetInput{Theme: "tokyo-night", RequestID: qaThemeID(22)}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ id, bg, errorFG string }{
		{"terminal-default", "", ""}, {"tokyo-night", "48;2;26;27;38", "38;2;247;118;142"},
		{"gruvbox", "48;2;40;40;40", "38;2;234;105;98"}, {"catppuccin", "48;2;30;30;46", "38;2;243;139;168"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			r := f.runStreams(true, false, "themes", "show", tc.id)
			plain := qaThemePlain(t, r.out)
			if r.code != 0 || r.err != "" || !strings.Contains(plain, "("+tc.id+")") || !strings.Contains(plain, "> Selected row [selected]") || !strings.Contains(plain, "Preference revision: 1;") {
				t.Fatalf("human preview lost theme/labels: %+v", r)
			}
			wantState := "preview; not saved"
			if tc.id == "tokyo-night" {
				wantState = "saved"
			}
			if !strings.Contains(plain, "Preference revision: 1; "+wantState+"\n") {
				t.Errorf("preview lied about saved selection: %q", plain)
			}
			if tc.id == "terminal-default" {
				if strings.ContainsRune(r.out, 27) {
					t.Error("default preview used saved named-palette styling")
				}
			} else if !strings.Contains(r.out, tc.bg) || !strings.Contains(r.out, tc.errorFG) {
				t.Errorf("preview %s did not encode its own palette roles", tc.id)
			}
			if b, err := os.ReadFile(f.path); err != nil || !bytes.Equal(b, before) {
				t.Errorf("preview changed saved preference/receipts: %v", err)
			}
		})
	}
}

type qaThemeFailingOutput struct {
	resetOnly bool
	writes    int
}

func (w *qaThemeFailingOutput) Write(b []byte) (int, error) {
	w.writes++
	if !w.resetOnly || string(b) == "\x1b[0m" {
		return 0, errors.New("PRIVATE-OUTPUT-FAILURE")
	}
	return len(b), nil
}

func TestQAThemesCLIHumanOutputAndFinalResetFailuresAreReported(t *testing.T) {
	for _, resetOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("resetOnly=%v", resetOnly), func(t *testing.T) {
			f := qaNewThemeCLI(t)
			f.deps.TerminalEligible = func(io.Reader, io.Writer) bool { return true }
			f.deps.OutputTTY = func(io.Writer) bool { return true }
			if _, err := themes.New(themes.Options{Path: f.path}).Set(context.Background(), themes.SetInput{Theme: "tokyo-night", RequestID: qaThemeID(23)}); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(f.path)
			if err != nil {
				t.Fatal(err)
			}
			out := &qaThemeFailingOutput{resetOnly: resetOnly}
			var stderr bytes.Buffer
			code := cli.Run(context.Background(), []string{"themes", "show"}, strings.NewReader(""), out, &stderr, f.deps)
			if code != 1 || out.writes == 0 {
				t.Errorf("failed finite human output reported code=%d writes=%d", code, out.writes)
			}
			if strings.Contains(stderr.String(), "PRIVATE-") {
				t.Error("raw writer failure leaked")
			}
			if after, err := os.ReadFile(f.path); err != nil || !bytes.Equal(after, before) {
				t.Errorf("failed read-only presentation changed preferences: %v", err)
			}
		})
	}
}
