package ui_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
)

func TestQAAppearanceRunnerReopenUnknownReplaysSameIDAndClearsOnlyConclusiveOutcome(t *testing.T) {
	path := qaAppearanceRunPath(t)
	qaAppearanceRunSave(t, path, "tokyo-night", "0", 1)
	var syncs atomic.Int32
	x := qaAppearanceRunRig(t, path, func(stage string) error {
		if stage == "directory_sync" && syncs.Add(1) == 1 {
			return errors.New("PRIVATE unknown durability")
		}
		return nil
	})
	x.frame(t, "Project 100")
	x.screen.events <- terminal.Event{Kind: "text", Text: "A"}
	x.frame(t, "[tokyo-night]")
	qaAppearanceRunChoose(t, x, "gruvbox")
	qaAppearanceRunConfirm(t, x)
	first := strings.Join(x.frame(t, "local_write_unknown"), "\n")
	var store struct{ Requests map[string]json.RawMessage }
	if err := json.Unmarshal(qaAppearanceRunBytes(t, path), &store); err != nil {
		t.Fatal(err)
	}
	var id string
	for key := range store.Requests {
		if key != "99999999-9999-4999-8999-000000000001" {
			id = key
		}
	}
	if id == "" || !strings.Contains(first, id) {
		t.Fatal("unknown first outcome lacks exact saved request ID")
	}
	qaAppearanceRunSave(t, path, "catppuccin", "2", 2)
	external := qaAppearanceRunBytes(t, path)
	x.screen.events <- terminal.Event{Kind: "escape"}
	qaAppearanceRunStyled(t, x.frame(t, "Project 100"), "catppuccin")
	if !bytes.Equal(external, qaAppearanceRunBytes(t, path)) {
		t.Fatal("uncertain cancel rolled back later writer")
	}
	x.screen.events <- terminal.Event{Kind: "text", Text: "A"}
	reopened := strings.Join(x.frame(t, "local_write_unknown"), "\n")
	if !strings.Contains(reopened, id) || !strings.Contains(reopened, "gruvbox") {
		t.Fatal("reopened Appearance replaced pending identity or draft")
	}
	x.screen.events <- terminal.Event{Kind: "enter"}
	x.frame(t, "Review scoped change")
	qaAppearanceRunConfirm(t, x)
	qaAppearanceRunStyled(t, x.frame(t, "Project 100"), "catppuccin")
	if !bytes.Equal(external, qaAppearanceRunBytes(t, path)) || syncs.Load() != 2 {
		t.Fatal("recovery reapplied old palette/new request or missed exact durability retry")
	}
	x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
	x.finish(t, nil)
	if x.screen.nextMaximum.Load() != 1 || x.screen.enters.Load() != 1 {
		t.Fatal("reopened recovery created another terminal/input owner")
	}
}

func TestQAAppearanceRunnerDuplicateInputDuringApplyCreatesOneReceipt(t *testing.T) {
	path := qaAppearanceRunPath(t)
	qaAppearanceRunSave(t, path, "tokyo-night", "0", 1)
	initial := qaAppearanceRunBytes(t, path)
	entered, release := make(chan struct{}), make(chan struct{})
	var attempts atomic.Int32
	x := qaAppearanceRunRig(t, path, func(stage string) error {
		if stage == "before_write" {
			if attempts.Add(1) == 1 {
				close(entered)
			}
			<-release
		}
		return nil
	})
	t.Cleanup(func() {
		x.cancel(context.Canceled)
		select {
		case <-release:
		default:
			close(release)
		}
	})
	x.frame(t, "Project 100")
	x.screen.events <- terminal.Event{Kind: "text", Text: "A"}
	x.frame(t, "[tokyo-night]")
	qaAppearanceRunChoose(t, x, "gruvbox")
	qaAppearanceRunConfirm(t, x)
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("Apply never reached actual shared write barrier")
	}
	x.screen.events <- terminal.Event{Kind: "enter"}
	x.screen.events <- terminal.Event{Kind: "text", Text: "A"}
	x.screen.events <- terminal.Event{Kind: "enter"}
	// A subsequent visible resize acknowledges the prior FIFO inputs through
	// the sole runner without adding sleeps or a second input consumer.
	x.screen.events <- terminal.Event{Kind: "resize", Columns: 20, Rows: 5}
	x.frame(t, "too small")
	x.screen.events <- terminal.Event{Kind: "resize", Columns: 120, Rows: 24}
	x.frame(t, "Review scoped change")
	if !bytes.Equal(initial, qaAppearanceRunBytes(t, path)) {
		close(release)
		t.Fatal("precommit barrier wrote prematurely")
	}
	close(release)
	qaAppearanceRunStyled(t, x.frame(t, "Project 100"), "gruvbox")
	var state struct {
		Requests           map[string]json.RawMessage
		PreferenceRevision string `json:"preference_revision"`
	}
	if err := json.Unmarshal(qaAppearanceRunBytes(t, path), &state); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 1 || len(state.Requests) != 2 || state.PreferenceRevision != "2" {
		t.Fatalf("duplicate input admitted another Apply: attempts%d receipts%d revision%s", attempts.Load(), len(state.Requests), state.PreferenceRevision)
	}
	x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
	x.finish(t, nil)
}

func TestQAAppearanceRunnerUnknownRereadFailureUsesSafeFallbackWithoutErasingPending(t *testing.T) {
	path := qaAppearanceRunPath(t)
	qaAppearanceRunSave(t, path, "tokyo-night", "0", 1)
	x := qaAppearanceRunRig(t, path, func(stage string) error {
		if stage == "directory_sync" {
			return errors.New("PRIVATE durability failure")
		}
		return nil
	})
	x.frame(t, "Project 100")
	x.screen.events <- terminal.Event{Kind: "text", Text: "A"}
	x.frame(t, "[tokyo-night]")
	qaAppearanceRunChoose(t, x, "gruvbox")
	qaAppearanceRunConfirm(t, x)
	x.frame(t, "local_write_unknown")
	foreign := []byte("PRIVATE corrupt intervening synthetic preferences")
	if err := os.WriteFile(path, foreign, 0600); err != nil {
		t.Fatal(err)
	}
	x.screen.events <- terminal.Event{Kind: "escape"}
	frame := x.frame(t, "Project 100")
	qaAppearanceRunStyled(t, frame, "terminal-default")
	plain := strings.ToLower(strings.Join(frame, "\n"))
	if !strings.Contains(plain, "appearance") || !strings.Contains(plain, "unavailable") || strings.Contains(plain, "private") {
		t.Errorf("uncertain reread failure lacks safe fallback warning: %s", plain)
	}
	x.screen.events <- terminal.Event{Kind: "text", Text: "q"}
	select {
	case err := <-x.done:
		x.done <- err
		var safe *themes.Error
		if !errors.As(err, &safe) || safe.Code != "local_write_unknown" || !safe.Uncertain || safe.Details["theme"] != "gruvbox" || safe.Details["if_revision"] != "1" || safe.Details["request_id"] == "" {
			t.Fatalf("reread failure erased stronger pending outcome: %v", err)
		}
		if !bytes.Equal(foreign, qaAppearanceRunBytes(t, path)) {
			t.Error("reread fallback reset or rewrote corrupt bytes")
		}
	case <-time.After(time.Second):
		t.Fatal("pending reread failure did not join on quit")
	}
}
