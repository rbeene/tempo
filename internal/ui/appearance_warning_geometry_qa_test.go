package ui_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
	"github.com/rbeene/tempo/internal/ui"
	"github.com/rivo/uniseg"
)

func qaAppearanceWarningDashboard(t *testing.T, x *qaRunnerRig, width, rows int, warning, content string) []string {
	t.Helper()
	deadline := time.NewTimer(1500 * time.Millisecond)
	defer deadline.Stop()
	for {
		select {
		case frame := <-x.screen.frames:
			if len(frame) == 0 || len(frame) > rows {
				continue
			}
			fits := true
			for _, line := range frame {
				fits = fits && uniseg.StringWidth(line) <= width
			}
			text := strings.Join(frame, "\n")
			dashboard := strings.Contains(text, "Computer "+qaUIComputer) && strings.Contains(text, content)
			if content == "Project 100" {
				for _, information := range []string{"Provisional union", "00:00:10", "Confirmed closed", "00:00:05"} {
					dashboard = dashboard && strings.Contains(text, information)
				}
			}
			if fits && dashboard && strings.Contains(text, warning) {
				return frame
			}
		case err := <-x.done:
			x.done <- err
			t.Fatalf("runner ended before warning dashboard: %v", err)
		case <-deadline.C:
			t.Fatal("warning dashboard did not reach requested viewport")
		}
	}
}

func TestQAAppearanceWarningGeometryKeepsActionsAndAuthoritativeActivity(t *testing.T) {
	for _, mode := range []string{"startup-corrupt", "retained-unknown"} {
		t.Run(mode, func(t *testing.T) {
			for _, width := range []int{59, 60, 71, 72, 80} {
				t.Run(fmt.Sprintf("content-%d", width), func(t *testing.T) {
					for _, rows := range []int{8, 12, 24} {
						t.Run(fmt.Sprintf("rows-%d", rows), func(t *testing.T) {
							for _, state := range []string{"empty", "active"} {
								t.Run(state, func(t *testing.T) {
									path := qaAppearanceRunPath(t)
									var writes atomic.Int32
									warning := "Appearance unavailable"
									corrupt := []byte("PRIVATE corrupt owned preferences\x1b]52;c;SECRET\a")
									if mode == "startup-corrupt" {
										if err := os.WriteFile(path, corrupt, 0600); err != nil {
											t.Fatal(err)
										}
									} else {
										qaAppearanceRunSave(t, path, "tokyo-night", "0", 1)
										warning = "Appearance outcome unknown"
									}
									snapshot := qaUISnapshot("1")
									content := "No activity yet"
									if state == "active" {
										snapshot = qaUISnapshot("1", "100")
										content = "Project 100"
									}
									reader := &qaRunnerReader{read: func(ctx context.Context) (activity.ActivitySnapshot, error) {
										qaUIBudget(t, ctx)
										return snapshot, nil
									}}
									service := qaAppearanceRunService(path, func(stage string) error {
										if stage == "directory_sync" {
											writes.Add(1)
											return errors.New("PRIVATE durability failure")
										}
										return nil
									})
									x := qaStartRunnerOptions(t, qaNewRunnerScreen(), reader, ui.Options{Refresh: make(chan time.Time), Appearance: service, Capabilities: themes.Capabilities{OutputTTY: false}})
									if mode == "retained-unknown" {
										x.frame(t, content)
										qaLinkKey(x, "A")
										x.frame(t, "[tokyo-night]")
										qaAppearanceRunChoose(t, x, "gruvbox")
										qaAppearanceRunConfirm(t, x)
										x.frame(t, "local_write_unknown")
										qaLinkEscape(x)
									}
									qaAppearanceWarningDashboard(t, x, 119, 24, warning, content)
									x.screen.events <- terminal.Event{Kind: "resize", Columns: width + 1, Rows: rows}
									frame := qaAppearanceWarningDashboard(t, x, width, rows, warning, content)
									qaUIFrame(t, frame, width+1, rows)
									text := strings.Join(frame, "\n")
									hints := []string{"A Appearance", "? Help", "q Quit"}
									if width >= 60 {
										hints = append(hints, "a Auth", "l Links", "h Hooks", "s Sync", "w Worker")
									}
									for _, hint := range hints {
										if !strings.Contains(text, hint) {
											t.Errorf("warning displaced available action %q: %q", hint, text)
										}
									}
									for _, information := range []string{"Computer " + qaUIComputer, "Sync paused"} {
										if !strings.Contains(text, information) {
											t.Errorf("warning displaced authoritative scope/status %q", information)
										}
									}
									if state == "active" {
										for _, information := range []string{"Account 1", "Provisional union", "00:00:10", "Confirmed closed", "00:00:05"} {
											if !strings.Contains(text, information) {
												t.Errorf("warning displaced authoritative timer %q", information)
											}
										}
									}
									if strings.Contains(text, "PRIVATE") || strings.Contains(text, "SECRET") || strings.Contains(text, "Complete") {
										t.Error("warning presentation leaked raw data or fabricated a result")
									}
									qaLinkKey(x, "q")
									select {
									case err := <-x.done:
										x.done <- err
										if mode == "retained-unknown" {
											var outcome *themes.Error
											if !errors.As(err, &outcome) || outcome.Code != "local_write_unknown" || !outcome.Uncertain || outcome.Details["theme"] != "gruvbox" || outcome.Details["if_revision"] != "1" || outcome.Details["request_id"] == "" || writes.Load() != 1 {
												t.Errorf("warning/resize/quit lost exact unknown or repeated write: %v writes%d", err, writes.Load())
											}
										} else if err != nil || writes.Load() != 0 || !bytes.Equal(corrupt, qaAppearanceRunBytes(t, path)) {
											t.Error("startup warning/resize rewrote corrupt state or caused action")
										}
									case <-time.After(time.Second):
										t.Fatal("warning dashboard quit did not join within1s")
									}
									if reader.calls.Load() != 1 || reader.maximum.Load() != 1 || x.screen.nextMaximum.Load() != 1 {
										t.Error("warning render/resize changed observation or input ownership")
									}
								})
							}
						})
					}
				})
			}
		})
	}
}
