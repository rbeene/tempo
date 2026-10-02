package ui_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
)

func qaSyncObservationUnknown(t *testing.T, frame []string) {
	t.Helper()
	qaUIFrame(t, frame, 120, 24)
	for _, line := range frame {
		if strings.HasPrefix(line, "Sync ") {
			state := strings.ToLower(line)
			if strings.Contains(state, "paused") || strings.Contains(state, "enabled") || !strings.Contains(state, "unknown") && !strings.Contains(state, "unavailable") {
				t.Errorf("unobserved sync was presented as authoritative: %q", line)
			}
			return
		}
	}
	t.Error("unobserved frame omitted sync availability")
}

func qaSyncObservationKnown(t *testing.T, frame []string, enabled, stale bool) {
	t.Helper()
	qaUIFrame(t, frame, 120, 24)
	want, other := "Sync paused", "Sync enabled"
	if enabled {
		want, other = other, want
	}
	text := strings.Join(frame, "\n")
	if !strings.Contains(text, want) || strings.Contains(text, other) {
		t.Errorf("sync observation not retained: want %q, frame %q", want, text)
	}
	if strings.Contains(text, "Stale") != stale {
		t.Errorf("stale marker=%t, want %t: %q", strings.Contains(text, "Stale"), stale, text)
	}
}

func TestQAUISyncObservationInitialUnknownAndAuthoritativeRecovery(t *testing.T) {
	for _, first := range []bool{false, true} {
		name := "paused"
		if first {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			m := ui.NewModel(120, 24)
			initial := m.Render(nil)
			qaSyncObservationUnknown(t, initial)
			if !strings.Contains(strings.Join(initial, "\n"), "Loading local activity") {
				t.Error("initial frame lost loading distinction")
			}
			m.MarkStale(1, "Local activity unavailable")
			failed := m.Render(nil)
			qaSyncObservationUnknown(t, failed)
			text := strings.Join(failed, "\n")
			if !strings.Contains(text, "Local activity unavailable") || strings.Contains(text, "00:00:00") {
				t.Error("failed first observation fabricated zero activity or lost availability")
			}
			snapshot := qaUISnapshot("1")
			snapshot.SyncEnabled = first
			qaUIApply(t, m, 2, snapshot)
			qaSyncObservationKnown(t, m.Render(nil), first, false)
			m.MarkStale(3, "Local activity unavailable")
			qaSyncObservationKnown(t, m.Render(nil), first, true)
			snapshot.SyncEnabled = !first
			qaUIApply(t, m, 4, snapshot)
			qaSyncObservationKnown(t, m.Render(nil), !first, false)
		})
	}
}

func TestQAUIRunnerFailedInitialSyncObservationRecoversWithoutFalseConsent(t *testing.T) {
	for _, first := range []bool{false, true} {
		name := "paused"
		if first {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			screen := qaNewRunnerScreen()
			reader := &qaRunnerReader{read: func(ctx context.Context) (activity.ActivitySnapshot, error) {
				qaUIBudget(t, ctx)
				i := calls.Add(1)
				if i <= 2 || i == 4 {
					return activity.ActivitySnapshot{}, errors.New("PRIVATE-SYNC-FAILURE\x1b]52;c;SECRET\a")
				}
				snapshot := qaUISnapshot("1")
				snapshot.SyncEnabled = first
				if i == 5 {
					snapshot.SyncEnabled = !first
				}
				return snapshot, nil
			}}
			x := qaStartRunner(t, screen, reader, make(chan time.Time))
			for i := 0; i < 2; i++ {
				frame := x.frame(t, "Local activity unavailable")
				qaSyncObservationUnknown(t, frame)
				text := strings.Join(frame, "\n")
				if strings.Contains(text, "00:00:00") || strings.Contains(text, "PRIVATE") || strings.Contains(text, "SECRET") {
					t.Error("failed initial Status fabricated duration or leaked its error")
				}
				screen.events <- terminal.Event{Kind: "text", Text: "r"}
			}
			qaSyncObservationKnown(t, x.frame(t, "No activity yet"), first, false)
			screen.events <- terminal.Event{Kind: "text", Text: "r"}
			qaSyncObservationKnown(t, x.frame(t, "Stale"), first, true)
			screen.events <- terminal.Event{Kind: "text", Text: "r"}
			qaSyncObservationKnown(t, x.frame(t, "No activity yet"), !first, false)
			screen.events <- terminal.Event{Kind: "text", Text: "q"}
			x.finish(t, nil)
			if calls.Load() != 5 || reader.maximum.Load() != 1 || screen.nextMaximum.Load() != 1 {
				t.Errorf("observation/input ownership: reads=%d status concurrency=%d input concurrency=%d", calls.Load(), reader.maximum.Load(), screen.nextMaximum.Load())
			}
		})
	}
}
