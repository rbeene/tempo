package cli_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/themes"
)

func TestQAAppearanceTerminalFailureProseUsesRestoredErrorDestination(t *testing.T) {
	for _, stderrTTY := range []bool{false, true} {
		t.Run(map[bool]string{false: "redirected", true: "tty"}[stderrTTY], func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "preferences.json")
			svc := themes.New(themes.Options{Path: path})
			if _, err := svc.Set(context.Background(), themes.SetInput{Theme: "tokyo-night", RequestID: qaThemeID(71)}); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(k string) string {
				if k == "HARVEST_TOKEN" {
					return "synthetic-qa-secret"
				}
				if k == "HARVEST_ACCOUNT_ID" {
					return "11"
				}
				return ""
			}, NewProvider: func(string, string) harvest.Provider { return &qaGuidedCLIProvider{} }})
			var out, stderr bytes.Buffer
			code := cli.Run(context.Background(), []string{"link", "--path", dir}, strings.NewReader(""), &out, &stderr, cli.Dependencies{
				Auth: a, Themes: svc, Activity: activity.New(activity.Options{Path: filepath.Join(dir, "activity", "state")}), ConfigPath: filepath.Join(dir, "cfg"),
				Getenv: func(k string) string {
					if k == "TERM" {
						return "xterm-256color"
					}
					if k == "COLORTERM" {
						return "truecolor"
					}
					return ""
				},
				Prompter: &qaFailedPrompt{qaCLILinkPrompt{t: t}}, TerminalEligible: func(io.Reader, io.Writer) bool { return true },
				OutputTTY: func(w io.Writer) bool {
					if w == &stderr {
						return stderrTTY
					}
					if w == &out {
						return !stderrTTY
					}
					t.Fatal("unrelated destination probed")
					return false
				},
			})
			const plain = "tempo: terminal: terminal input or output failed\n"
			if code != 1 || out.Len() != 0 || qaThemePlain(t, stderr.String()) != plain {
				t.Fatalf("terminal result code=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
			}
			if stderrTTY {
				if !strings.Contains(stderr.String(), "38;2;247;118;142") || !strings.Contains(stderr.String(), "48;2;26;27;38") || !strings.HasSuffix(stderr.String(), "\x1b[0m") {
					t.Errorf("restored terminal error bypassed saved Tokyo error role/final reset: %q", stderr.String())
				}
			} else if strings.ContainsRune(stderr.String(), 27) {
				t.Error("redirected stderr was colored using stdout eligibility")
			}
			if after, err := os.ReadFile(path); err != nil || !bytes.Equal(after, before) {
				t.Errorf("terminal failure changed preferences: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, "activity", "state")); !os.IsNotExist(err) {
				t.Errorf("failed prompt admitted activity write: %v", err)
			}
		})
	}
}

func TestQAAppearanceUnknownProsePreservesCanonicalNonV4Identity(t *testing.T) {
	f := qaNewThemeCLI(t)
	f.deps.TerminalEligible = func(io.Reader, io.Writer) bool { return true }
	f.deps.Themes = themes.New(themes.Options{Path: f.path, Fault: func(stage string) error {
		if stage == "directory_sync" {
			return errors.New("PRIVATE synthetic durability failure")
		}
		return nil
	}})
	const id = "00000000-0000-1000-8000-000000000001"
	r := f.runStreams(false, false, "themes", "set", "gruvbox", "--if-revision", "0", "--request-id", id)
	if r.code != 8 || r.out != "" {
		t.Fatalf("unknown result=%+v", r)
	}
	for _, want := range []string{"local_write_unknown", id, "gruvbox", "--if-revision 0", "--request-id " + id} {
		if !strings.Contains(r.err, want) {
			t.Errorf("safe human recovery omitted exact %q: %q", want, r.err)
		}
	}
	if strings.Contains(r.err, "PRIVATE") {
		t.Error("raw failure leaked")
	}
	shown, err := themes.New(themes.Options{Path: f.path}).Show(context.Background(), "")
	if err != nil || shown.Theme.ID != "gruvbox" || shown.PreferenceRevision != "1" {
		t.Errorf("real fault did not cross write boundary: %+v %v", shown, err)
	}
}
