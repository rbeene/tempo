//go:build darwin || linux

package themes_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/themes"
)

type qaThemeChildResult struct {
	Result activity.MutationResult
	Code   string
}

// Same-binary children exercise kernel locks and reopened durable state. They
// receive only explicit synthetic paths and fixture inputs, no inherited auth.
func TestQAPreferencesProcessHelper(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_THEME_MODE")
	if mode == "" {
		return
	}
	path := os.Getenv("TEMPO_QA_THEME_PATH")
	ready := os.NewFile(3, "qa-ready")
	if mode == "lock" {
		lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
			t.Fatal(err)
		}
		if _, err = ready.Write([]byte("R")); err != nil {
			t.Fatal(err)
		}
		ready.Close()
		if _, err = io.ReadFull(os.Stdin, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	s := qaThemeService(path)
	if mode == "race" {
		observed, err := s.Show(context.Background(), "")
		if err != nil {
			t.Fatal(err)
		}
		if observed.PreferenceRevision != "0" {
			t.Fatalf("race not started from revision0: %+v", observed)
		}
		if _, err = ready.Write([]byte("R")); err != nil {
			t.Fatal(err)
		}
		ready.Close()
		if _, err = io.ReadFull(os.Stdin, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
	} else {
		ready.Close()
	}
	result, err := s.Set(context.Background(), themes.SetInput{Theme: os.Getenv("TEMPO_QA_THEME_SELECTION"), IfRevision: os.Getenv("TEMPO_QA_THEME_REVISION"), RequestID: os.Getenv("TEMPO_QA_THEME_REQUEST")})
	reply := qaThemeChildResult{Result: result}
	if err != nil {
		var safe *themes.Error
		if !errors.As(err, &safe) {
			t.Fatal(err)
		}
		reply.Code = safe.Code
	}
	if err = json.NewEncoder(os.Stdout).Encode(reply); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}

type qaThemeChild struct {
	cmd                 *exec.Cmd
	ready               *os.File
	input               io.WriteCloser
	output, diagnostics bytes.Buffer
	waited              bool
}

func qaStartThemeChild(t *testing.T, path, mode, theme, revision, request string) *qaThemeChild {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	r, w, err := os.Pipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	child := &qaThemeChild{ready: r}
	child.cmd = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQAPreferencesProcessHelper$", "-test.timeout=8s")
	child.cmd.Env = []string{"TEMPO_QA_THEME_MODE=" + mode, "TEMPO_QA_THEME_PATH=" + path, "TEMPO_QA_THEME_SELECTION=" + theme, "TEMPO_QA_THEME_REVISION=" + revision, "TEMPO_QA_THEME_REQUEST=" + request, "GORACE=atexit_sleep_ms=0", "GOMAXPROCS=2"}
	child.cmd.ExtraFiles = []*os.File{w}
	child.cmd.Stdout = &child.output
	child.cmd.Stderr = &child.diagnostics
	child.input, err = child.cmd.StdinPipe()
	if err != nil {
		r.Close()
		w.Close()
		cancel()
		t.Fatal(err)
	}
	if err = child.cmd.Start(); err != nil {
		r.Close()
		w.Close()
		child.input.Close()
		cancel()
		t.Fatal(err)
	}
	w.Close()
	t.Cleanup(func() {
		child.ready.Close()
		child.input.Close()
		if !child.waited {
			child.cmd.Process.Kill()
			child.cmd.Wait()
		}
		cancel()
	})
	return child
}

func (c *qaThemeChild) awaitReady(t *testing.T) {
	t.Helper()
	if err := c.ready.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 1)
	if _, err := io.ReadFull(c.ready, b); err != nil || string(b) != "R" {
		_ = c.cmd.Process.Kill()
		_ = c.cmd.Wait()
		c.waited = true
		t.Fatalf("child readiness: %q %v stdout=%s stderr=%s", b, err, c.output.String(), c.diagnostics.String())
	}
}
func (c *qaThemeChild) release(t *testing.T) {
	t.Helper()
	if _, err := c.input.Write([]byte("G")); err != nil {
		t.Fatal(err)
	}
	c.input.Close()
}
func (c *qaThemeChild) wait(t *testing.T) qaThemeChildResult {
	t.Helper()
	err := c.cmd.Wait()
	c.waited = true
	if err != nil {
		t.Fatalf("child failed: %v stdout=%s stderr=%s", err, c.output.String(), c.diagnostics.String())
	}
	var result qaThemeChildResult
	if err = json.Unmarshal(c.output.Bytes(), &result); err != nil {
		t.Fatalf("child result=%q: %v", c.output.String(), err)
	}
	return result
}

func TestQAPreferencesTwoProcessesChangingSameRevisionHaveOneWinner(t *testing.T) {
	path := filepath.Join(qaThemePrivateTemp(t), "preferences.json")
	first := qaStartThemeChild(t, path, "race", "tokyo-night", "0", qaThemeID(70))
	second := qaStartThemeChild(t, path, "race", "gruvbox", "0", qaThemeID(71))
	first.awaitReady(t)
	second.awaitReady(t)
	first.release(t)
	second.release(t)
	a, b := first.wait(t), second.wait(t)
	winners, conflicts := 0, 0
	want := ""
	for i, result := range []qaThemeChildResult{a, b} {
		switch result.Code {
		case "":
			winners++
			if i == 0 {
				want = "tokyo-night"
			} else {
				want = "gruvbox"
			}
			qaThemeMutation(t, result.Result, qaThemeID(70+i), "1", "1", true)
		case "revision_conflict":
			conflicts++
		default:
			t.Errorf("unexpected race result %+v", result)
		}
	}
	if winners != 1 || conflicts != 1 {
		t.Fatalf("race winners%d conflicts%d: %+v %+v", winners, conflicts, a, b)
	}
	qaThemeSaved(t, path, want, "1")
}

func TestQAPreferencesRealLockTimeoutAndHolderExit(t *testing.T) {
	path := filepath.Join(qaThemePrivateTemp(t), "preferences.json")
	holder := qaStartThemeChild(t, path, "lock", "", "", "")
	holder.awaitReady(t)
	s := themes.New(themes.Options{Path: path, LockTimeout: 40 * time.Millisecond})
	start := time.Now()
	_, err := s.Set(context.Background(), themes.SetInput{Theme: "tokyo-night", IfRevision: "0", RequestID: qaThemeID(72)})
	safe := qaThemeCode(t, err, "state_busy")
	if !safe.Retryable || safe.Uncertain {
		t.Errorf("lock busy flags %+v", safe)
	}
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond || elapsed > time.Second {
		t.Errorf("lock wait not bounded around budget: %s", elapsed)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("busy contender wrote store: %v", err)
	}
	if err = holder.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = holder.cmd.Wait()
	holder.waited = true
	qaThemeMutation(t, qaThemeSet(t, qaThemeService(path), "tokyo-night", "0", 72), qaThemeID(72), "1", "1", true)
}

func TestQAPreferencesProcessRestartRetainsHistoricalReceipt(t *testing.T) {
	path := filepath.Join(qaThemePrivateTemp(t), "preferences.json")
	first := qaStartThemeChild(t, path, "set", "tokyo-night", "0", qaThemeID(73)).wait(t)
	if first.Code != "" {
		t.Fatalf("child initial set: %+v", first)
	}
	qaThemeMutation(t, first.Result, qaThemeID(73), "1", "1", true)
	qaThemeSet(t, qaThemeService(path), "catppuccin", "1", 74)
	replay := qaStartThemeChild(t, path, "set", "tokyo-night", "0", qaThemeID(73)).wait(t)
	if replay.Code != "" {
		t.Fatalf("child replay: %+v", replay)
	}
	qaThemeMutation(t, replay.Result, qaThemeID(73), "1", "1", true)
	qaThemeSaved(t, path, "catppuccin", "2")
	conflict := qaStartThemeChild(t, path, "set", "gruvbox", "0", qaThemeID(73)).wait(t)
	if conflict.Code != "request_conflict" {
		t.Fatalf("child changed intent: %+v", conflict)
	}
}
