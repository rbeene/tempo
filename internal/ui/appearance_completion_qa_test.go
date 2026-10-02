package ui_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
	"github.com/rbeene/tempo/internal/ui"
)

func TestQAAppearanceCompletionPreWriteFailureCannotReplaceSignal(t *testing.T) {
	for _, code := range []int{130, 143} {
		t.Run(map[int]string{130: "sigint", 143: "sigterm"}[code], func(t *testing.T) {
			path := qaAppearanceRunPath(t)
			qaAppearanceRunSave(t, path, "tokyo-night", "0", 1)
			before := qaAppearanceRunBytes(t, path)
			var failed, signalled atomic.Bool
			var attempts atomic.Int32
			var x *qaRunnerRig
			cause := &terminal.ExitError{Code: code}
			svc := themes.New(themes.Options{Getenv: func(k string) string {
				if k != "TEMPO_PREFERENCES" {
					return ""
				}
				// A causal checkpoint: completion has selected flowDone and is
				// rereading presentation after the safe pre-write failure. Deliver
				// the signal here, so this is not a random select-race test.
				if failed.Load() && signalled.CompareAndSwap(false, true) {
					x.cancel(cause)
				}
				return path
			}, Fault: func(stage string) error {
				if stage == "before_write" {
					attempts.Add(1)
					failed.Store(true)
					return errors.New("PRIVATE synthetic pre-write failure")
				}
				return nil
			}})
			screen := qaNewRunnerScreen()
			reader := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("37", "100"), nil }}
			x = qaStartRunnerOptions(t, screen, reader, ui.Options{Refresh: make(chan time.Time), Appearance: svc, Capabilities: qaAppearanceRunCaps})
			x.frame(t, "Project 100")
			x.screen.events <- terminal.Event{Kind: "text", Text: "A"}
			x.frame(t, "[tokyo-night]")
			qaAppearanceRunChoose(t, x, "gruvbox")
			qaAppearanceRunConfirm(t, x)
			select {
			case result := <-x.done:
				x.done <- result
				if !errors.Is(result, cause) {
					t.Errorf("safe pre-write failure replaced signal %d: %v", code, result)
				}
			case <-time.After(time.Second):
				t.Fatal("completion failed to join after signal")
			}
			if !signalled.Load() || attempts.Load() != 1 {
				t.Fatalf("completion checkpoint not reached: signal=%v attempts=%d", signalled.Load(), attempts.Load())
			}
			if after, err := os.ReadFile(path); err != nil || !bytes.Equal(after, before) {
				t.Errorf("pre-write failure changed preferences/receipt: %v", err)
			}
		})
	}
}
