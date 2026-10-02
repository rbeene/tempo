package ui_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
	"github.com/rbeene/tempo/internal/ui"
)

var qaAppearanceRunCaps = themes.Capabilities{OutputTTY: true, Term: "xterm-256color", ColorTerm: "truecolor"}
var qaAppearanceRunSGR = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func qaAppearanceRunPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "preferences.json")
}
func qaAppearanceRunService(path string, fault func(string) error) *themes.Service {
	return themes.New(themes.Options{Path: path, Fault: fault, Getenv: func(string) string { panic("explicit Appearance path read environment") }})
}
func qaAppearanceRunSave(t *testing.T, path, theme, revision string, id int) {
	t.Helper()
	_, err := qaAppearanceRunService(path, nil).Set(context.Background(), themes.SetInput{Theme: theme, IfRevision: revision, RequestID: fmt.Sprintf("99999999-9999-4999-8999-%012d", id)})
	if err != nil {
		t.Fatal(err)
	}
}
func qaAppearanceRunBytes(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func qaAppearanceRunRig(t *testing.T, path string, fault func(string) error) *qaRunnerRig {
	t.Helper()
	s := qaNewRunnerScreen()
	r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("37", "100"), nil }}
	return qaStartRunnerOptions(t, s, r, ui.Options{Refresh: make(chan time.Time), Appearance: qaAppearanceRunService(path, fault), Capabilities: qaAppearanceRunCaps})
}
func qaAppearanceRunStyled(t *testing.T, frame []string, theme string) {
	t.Helper()
	styler, err := themes.NewStyler(theme, qaAppearanceRunCaps)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range frame {
		plain := qaAppearanceRunSGR.ReplaceAllString(line, "")
		if plain == "" {
			continue
		}
		found := false
		for _, role := range []terminal.Role{terminal.RoleText, terminal.RoleAccent, terminal.RoleSelection, terminal.RoleMuted, terminal.RoleKey, terminal.RoleInfo, terminal.RoleWarning, terminal.RoleSuccess, terminal.RoleError, terminal.RoleBorder} {
			found = found || styler.Paint(role, plain) == line
		}
		if !found {
			t.Errorf("visible row does not use current palette %s: %q", theme, line)
		}
	}
}
func qaAppearanceRunChoose(t *testing.T, x *qaRunnerRig, id string) {
	t.Helper()
	x.screen.events <- terminal.Event{Kind: "text", Text: id}
	x.frame(t, "["+id+"]")
	x.screen.events <- terminal.Event{Kind: "enter"}
	x.frame(t, "Review scoped change")
}
func qaAppearanceRunConfirm(t *testing.T, x *qaRunnerRig) {
	t.Helper()
	x.screen.events <- terminal.Event{Kind: "text", Text: "y"}
	x.frame(t, "Selected: Yes")
	x.screen.events <- terminal.Event{Kind: "enter"}
}

func TestQAAppearanceRunnerLoadsSavedPaletteAndLeavesAbsentStoresAlone(t *testing.T) {
	for _, id := range []string{"terminal-default", "tokyo-night", "gruvbox", "catppuccin"} {
		t.Run(id, func(t *testing.T) {
			path := qaAppearanceRunPath(t)
			if id != "terminal-default" {
				qaAppearanceRunSave(t, path, id, "0", 1)
			}
			x := qaAppearanceRunRig(t, path, nil)
			frame := x.frame(t, "Project 100")
			qaAppearanceRunStyled(t, frame, id)
			if !strings.Contains(strings.Join(frame, "\n"), "A Appearance") {
				t.Error("dashboard does not expose Appearance key")
			}
			x.screen.events <- terminal.Event{Kind: "text", Text: "A"}
			qaAppearanceRunStyled(t, x.frame(t, "["+id+"]"), id)
			x.screen.events <- terminal.Event{Kind: "escape"}
			qaAppearanceRunStyled(t, x.frame(t, "Project 100"), id)
			x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
			x.finish(t, nil)
			if id == "terminal-default" {
				entries, err := os.ReadDir(filepath.Dir(path))
				if err != nil || len(entries) != 0 {
					t.Errorf("absent Appearance read/preview initialized files: %v %v", entries, err)
				}
			}
		})
	}
}

func TestQAAppearanceRunnerHelpExposesKeyboardEntry(t *testing.T) {
	x := qaAppearanceRunRig(t, qaAppearanceRunPath(t), nil)
	x.frame(t, "Project 100")
	x.screen.events <- terminal.Event{Kind: "text", Text: "?"}
	x.frame(t, "A Appearance")
	x.screen.events <- terminal.Event{Kind: "escape"}
	x.frame(t, "Project 100")
	x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
	x.finish(t, nil)
}

func TestQAAppearanceRunnerCorruptStartupFallbackAndExplicitActionAreSafe(t *testing.T) {
	path := qaAppearanceRunPath(t)
	before := []byte("PRIVATE broken preferences")
	if err := os.WriteFile(path, before, 0600); err != nil {
		t.Fatal(err)
	}
	x := qaAppearanceRunRig(t, path, nil)
	frame := x.frame(t, "Project 100")
	qaAppearanceRunStyled(t, frame, "terminal-default")
	plain := strings.ToLower(strings.Join(frame, "\n"))
	if !strings.Contains(plain, "appearance") || !strings.Contains(plain, "unavailable") || strings.Contains(plain, "private") {
		t.Errorf("startup fallback lacks safe presentation warning: %s", plain)
	}
	x.screen.events <- terminal.Event{Kind: "text", Text: "A"}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case err := <-x.done:
			x.done <- err
			var safe *themes.Error
			if !errors.As(err, &safe) || safe.Code != "state_corrupt" || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("explicit Appearance did not surface safe actionable corruption: %v", err)
			}
			if !bytes.Equal(before, qaAppearanceRunBytes(t, path)) {
				t.Error("corrupt preference bytes reset")
			}
			return
		case frame := <-x.screen.frames:
			text := strings.Join(frame, "\n")
			if !strings.Contains(text, "state_corrupt") {
				continue
			}
			if strings.Contains(text, "PRIVATE") {
				t.Error("raw preference bytes leaked in explicit action")
			}
			x.screen.events <- terminal.Event{Kind: "escape"}
			x.frame(t, "Project 100")
			x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
			x.finish(t, nil)
			if !bytes.Equal(before, qaAppearanceRunBytes(t, path)) {
				t.Error("explicit Appearance rewrote corrupt preference")
			}
			return
		case <-deadline.C:
			t.Fatal("explicit Appearance silently ignored corruption")
		}
	}
}

func TestQAAppearanceRunnerPreviewCancelApplyAndResetKeepSingleOwner(t *testing.T) {
	path := qaAppearanceRunPath(t)
	qaAppearanceRunSave(t, path, "tokyo-night", "0", 1)
	for _, name := range []string{"activity-state.json", "config.json", "hooks.json"} {
		if err := os.WriteFile(filepath.Join(filepath.Dir(path), name), []byte("PRIVATE synthetic foreign bytes"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	x := qaAppearanceRunRig(t, path, nil)
	x.frame(t, "Project 100")
	x.screen.events <- terminal.Event{Kind: "text", Text: "A"}
	x.frame(t, "[tokyo-night]")
	x.screen.events <- terminal.Event{Kind: "text", Text: "gruvbox"}
	qaAppearanceRunStyled(t, x.frame(t, "[gruvbox]"), "gruvbox")
	qaAppearanceRunSave(t, path, "catppuccin", "1", 2)
	external := qaAppearanceRunBytes(t, path)
	x.screen.events <- terminal.Event{Kind: "escape"}
	qaAppearanceRunStyled(t, x.frame(t, "Project 100"), "catppuccin")
	if !bytes.Equal(external, qaAppearanceRunBytes(t, path)) {
		t.Fatal("cancel rewrote external selection")
	}
	for _, target := range []string{"gruvbox", "terminal-default"} {
		x.screen.events <- terminal.Event{Kind: "text", Text: "A"}
		x.frame(t, "[catppuccin]") // All four choices remain available after first Apply.
		qaAppearanceRunChoose(t, x, target)
		qaAppearanceRunConfirm(t, x)
		qaAppearanceRunStyled(t, x.frame(t, "Project 100"), target)
	}
	x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
	x.finish(t, nil)
	if x.screen.nextMaximum.Load() != 1 || x.screen.enters.Load() != 1 {
		t.Errorf("Appearance created second input/screen owner: Next=%d Enter=%d", x.screen.nextMaximum.Load(), x.screen.enters.Load())
	}
	for _, name := range []string{"activity-state.json", "config.json", "hooks.json"} {
		if got := qaAppearanceRunBytes(t, filepath.Join(filepath.Dir(path), name)); string(got) != "PRIVATE synthetic foreign bytes" {
			t.Errorf("Appearance touched %s", name)
		}
	}
}

func TestQAAppearanceRunnerJoinedUnknownWinsOverShutdownAndCleanup(t *testing.T) {
	for _, mode := range []string{"escape", "eof", "sigint", "sigterm", "input-error"} {
		t.Run(mode, func(t *testing.T) {
			path := qaAppearanceRunPath(t)
			qaAppearanceRunSave(t, path, "tokyo-night", "0", 1)
			crossed, release := make(chan struct{}), make(chan struct{})
			var active atomic.Int32
			x := qaAppearanceRunRig(t, path, func(stage string) error {
				if stage == "directory_sync" {
					active.Add(1)
					defer active.Add(-1)
					close(crossed)
					<-release
					return errors.New("PRIVATE durability failure")
				}
				return nil
			})
			x.screen.closeCheck = func() bool { return active.Load() != 0 }
			x.screen.closeErr = errors.New("synthetic cleanup failure")
			x.frame(t, "Project 100")
			x.screen.events <- terminal.Event{Kind: "text", Text: "A"}
			x.frame(t, "[tokyo-night]")
			qaAppearanceRunChoose(t, x, "gruvbox")
			qaAppearanceRunConfirm(t, x)
			select {
			case <-crossed:
			case <-time.After(time.Second):
				close(release)
				t.Fatal("shared Set did not reach actual durability crossing")
			}
			switch mode {
			case "escape":
				x.screen.events <- terminal.Event{Kind: "escape"}
			case "eof":
				x.screen.endErrors <- &terminal.ExitError{Code: 0}
			case "sigint":
				x.cancel(&terminal.ExitError{Code: 130})
			case "sigterm":
				x.cancel(&terminal.ExitError{Code: 143})
			case "input-error":
				x.screen.endErrors <- errors.New("synthetic terminal I/O failure")
			}
			close(release)
			if mode == "escape" {
				// Observe completion before q, so buffered navigation cannot be
				// mistaken for input to the completed confirmation modal.
				deadline := time.NewTimer(time.Second)
				for waiting := true; waiting; {
					select {
					case result := <-x.done:
						x.done <- result
						waiting = false
					case frame := <-x.screen.frames:
						if strings.Contains(strings.Join(frame, "\n"), "Project 100") {
							x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
							waiting = false
						}
					case <-deadline.C:
						t.Fatal("Escape during admitted write neither joined nor restored dashboard")
					}
				}
				deadline.Stop()
			}
			var result error
			select {
			case result = <-x.done:
				x.done <- result
			case <-time.After(time.Second):
				t.Fatal("shutdown failed to join mutation")
			}
			var safe *themes.Error
			if !errors.As(result, &safe) || safe.Code != "local_write_unknown" || !safe.Uncertain || safe.Retryable {
				t.Fatalf("joined unknown was hidden by %s/Close: %v", mode, result)
			}
			if safe.Details["theme"] != "gruvbox" || safe.Details["if_revision"] != "1" || safe.Details["request_id"] == "" {
				t.Errorf("uncertain joined outcome lost immutable replay inputs: %+v", safe.Details)
			}
			if strings.Contains(result.Error(), "PRIVATE") {
				t.Error("raw persistence failure leaked")
			}
		})
	}
}

type qaAppearanceFailDraw struct {
	*qaRunnerScreen
	failed  atomic.Bool
	failure error
}

func (s *qaAppearanceFailDraw) Draw(ctx context.Context, lines []string) error {
	if strings.Contains(strings.Join(lines, "\n"), "local_write_unknown") {
		s.failed.Store(true)
		return s.failure
	}
	return s.qaRunnerScreen.Draw(ctx, lines)
}

func TestQAAppearanceRunnerUnknownSurvivesActualDrawFailure(t *testing.T) {
	path := qaAppearanceRunPath(t)
	qaAppearanceRunSave(t, path, "tokyo-night", "0", 1)
	s := qaNewRunnerScreen()
	s.closeErr = errors.New("synthetic Close failure")
	wrapper := &qaAppearanceFailDraw{qaRunnerScreen: s, failure: errors.New("synthetic output writer failure")}
	r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("37", "100"), nil }}
	ctx, cancel := context.WithCancelCause(context.Background())
	x := &qaRunnerRig{screen: s, reader: r, done: make(chan error, 1), cancel: cancel}
	s.reader = r
	go func() {
		x.done <- ui.Run(ctx, wrapper, r, ui.Options{Refresh: make(chan time.Time), Appearance: qaAppearanceRunService(path, func(stage string) error {
			if stage == "directory_sync" {
				return errors.New("PRIVATE directory sync failure")
			}
			return nil
		}), Capabilities: qaAppearanceRunCaps})
	}()
	t.Cleanup(func() {
		cancel(context.Canceled)
		select {
		case <-x.done:
		case <-time.After(time.Second):
			t.Error("draw failure left owned work")
		}
		if s.closes.Load() != 1 || s.closeBeforeJoin {
			t.Error("draw failure did not join before exactly one Close")
		}
	})
	x.frame(t, "Project 100")
	s.events <- terminal.Event{Kind: "text", Text: "A"}
	x.frame(t, "[tokyo-night]")
	qaAppearanceRunChoose(t, x, "gruvbox")
	qaAppearanceRunConfirm(t, x)
	select {
	case err := <-x.done:
		x.done <- err
		var safe *themes.Error
		if !errors.As(err, &safe) || safe.Code != "local_write_unknown" || !safe.Uncertain || !wrapper.failed.Load() {
			t.Fatalf("Draw/Close failure hid unknown mutation: %v; actual failing Draw=%v", err, wrapper.failed.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("output failure did not terminate and join")
	}
}

func TestQAAppearanceRunnerCleanCancelDoesNotHideCloseFailure(t *testing.T) {
	path := qaAppearanceRunPath(t)
	x := qaAppearanceRunRig(t, path, nil)
	x.screen.closeErr = errors.New("synthetic cleanup error")
	x.frame(t, "Project 100")
	x.screen.events <- terminal.Event{Kind: "text", Text: "A"}
	x.frame(t, "[terminal-default]")
	x.screen.events <- terminal.Event{Kind: "escape"}
	x.frame(t, "Project 100")
	x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
	x.finish(t, x.screen.closeErr)
}
