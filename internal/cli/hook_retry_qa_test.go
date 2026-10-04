//go:build (darwin || linux) && (amd64 || arm64)

package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/cli"
)

func retryQAPayload(t *testing.T, cwd, kind string) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{"hook_event_name": kind, "session_id": "s", "turn_id": "retry-child", "agent_id": "retry-agent", "source": "resume", "cwd": cwd, "prompt": "SECRET ignored prompt"})
	if err != nil {
		t.Fatal("fixture payload encoding failed")
	}
	return string(b)
}

func retryQARun(t *testing.T, ctx context.Context, d cli.Dependencies, input string) (string, string) {
	t.Helper()
	var out, diagnostic bytes.Buffer
	if cli.Run(ctx, []string{"hook", "codex", "--input-stdin"}, strings.NewReader(input), &out, &diagnostic, d) != 0 {
		t.Fatal("hook vetoed host")
	}
	if out.Len() > 512 || diagnostic.Len() > 256 || strings.Contains(out.String()+diagnostic.String(), "SECRET") {
		t.Fatal("unsafe hook output")
	}
	store := d.Store.(*fakeStore)
	if store.gets+store.sets+store.deletes != 0 {
		t.Fatal("hook touched credentials")
	}
	return out.String(), diagnostic.String()
}

// A read-only typed audit of the actual meta row; it never seeds an outcome.
// Every retained owner is registered before checking its acquisition error.
func retryQAMeta(t *testing.T, path string) ([]byte, []byte, int64) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(filepath.Dir(path), "activity-*.sqlite3"))
	if err != nil || len(files) != 1 {
		t.Fatal("expected one actual SQLite database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := sqliteio.Open(ctx, filepath.Dir(files[0]), filepath.Base(files[0]), sqliteio.Options{ReadOnly: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	if c != nil {
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			terminal, first := c.CloseChecked(clean)
			var second error
			if !terminal {
				terminal, second = c.CloseChecked(clean)
			}
			if !terminal || first != nil || second != nil {
				t.Error("meta audit checked close failed")
			}
		}()
	}
	if err != nil || c == nil {
		t.Fatal("meta audit open failed")
	}
	tx, err := c.Begin(ctx, sqliteio.Read)
	if tx != nil {
		defer func() {
			if tx.Rollback() != nil {
				t.Error("meta audit rollback failed")
			}
		}()
	}
	if err != nil || tx == nil {
		t.Fatal("meta audit begin failed")
	}
	q, err := tx.Prepare("SELECT durability_nonce,revision,logical_bytes FROM store_meta WHERE singleton=1")
	if q != nil {
		defer func() {
			if q.Close() != nil {
				t.Error("meta audit statement close failed")
			}
		}()
	}
	if err != nil || q == nil {
		t.Fatal("meta audit prepare failed")
	}
	row, err := q.Step()
	if err != nil || !row {
		t.Fatal("meta audit row missing")
	}
	nonce, e1 := q.Blob(0)
	revision, e2 := q.Blob(1)
	charge, e3 := q.Int64(2)
	row, err = q.Step()
	if e1 != nil || e2 != nil || e3 != nil || err != nil || row || len(nonce) != 16 || len(revision) != 8 || charge <= 0 {
		t.Fatal("invalid typed meta audit")
	}
	return nonce, revision, charge
}

func TestQAHostBoundedRetryTransientThenSameEventReplay(t *testing.T) {
	for _, tc := range []struct {
		name     string
		refusals int
		fault    error
	}{
		{"busy_once", 1, sqliteio.ErrBusy},
		{"busy_twice", 2, sqliteio.ErrBusy},
		// Fault plumbing represents the checked local admission deadline error;
		// this does not claim an actual elapsed 250ms or hosted performance result.
		{"local_open_deadline", 1, &sqliteio.Error{Phase: sqliteio.OpenPhase, Category: sqliteio.Canceled, Code: 9, Cause: context.DeadlineExceeded}},
		{"local_begin_canceled", 1, &sqliteio.Error{Phase: sqliteio.BeginPhase, Category: sqliteio.Canceled, Cause: context.Canceled}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, socket := qaWorkerNotificationSocket(t)
			d, cwd, seconds, _ := qaHostWakeFixture(t, path)
			*seconds = 10
			input := retryQAPayload(t, cwd, "SubagentStart")
			baseline, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{Source: "codex"})
			if err != nil {
				t.Fatal("baseline receipt read failed")
			}
			hits, durable := 0, 0
			startContextQASetSQLHooks(startContextQASQLHooks{Observe: func(e startContextQASQLEvent) {
				if e.Phase == "durable-native-closed" && e.Code == 0 {
					durable++
				}
			}, Fault: func(e startContextQASQLEvent) error {
				if e.Phase == "prepare-before" && hits < tc.refusals {
					hits++
					return tc.fault
				}
				return nil
			}})
			t.Cleanup(func() { startContextQASetSQLHooks(startContextQASQLHooks{}) })
			out, diagnostic := retryQARun(t, context.Background(), d, input)
			startContextQASetSQLHooks(startContextQASQLHooks{})
			if out != "{}\n" || diagnostic != "" {
				t.Error("known precommit busy did not recover quietly")
			}
			if hits != tc.refusals || durable != 1 {
				t.Errorf("refusals=%d durable completions=%d, want %d/1", hits, durable, tc.refusals)
			}
			before, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{Source: "codex"})
			if err != nil {
				t.Fatal("cold receipt read failed")
			}
			children := 0
			for _, r := range before.Receipts {
				if r.Kind == "SubagentStart" {
					children++
					if r.TurnID != "retry-child" || r.AgentID != "retry-agent" || r.Durability != "committed" || r.Disposition != "applied" {
						t.Error("retry changed decoded child identity/outcome")
					}
				}
			}
			if children != 1 || len(before.Receipts) != 3 {
				t.Error("retry did not retain exactly one child effect")
			}
			// Continue all state postconditions on the old producer's expected RED.
			if durable != 1 {
				startContextQAExpect(t, out, diagnostic, "SubagentStart", "state_busy", "not_committed")
				if !reflect.DeepEqual(baseline, before) {
					t.Error("known precommit refusal changed baseline receipts")
				}
				qaWorkerNotification(t, socket, "")
				return
			}
			qaWorkerNotification(t, socket, "wake")
			qaWorkerNotification(t, socket, "")
			state, err := d.Activity.Status(context.Background())
			if err != nil || len(state.Actors) != 2 {
				t.Fatal("child actor projection missing")
			}
			nonce, revision, charge := retryQAMeta(t, path)
			out, diagnostic = retryQARun(t, context.Background(), d, input)
			if out != "{}\n" || diagnostic != "" {
				t.Error("exact replay was not quiet")
			}
			after, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{Source: "codex"})
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Error("replay changed immutable receipts")
			}
			afterState, err := d.Activity.Status(context.Background())
			if err != nil || !reflect.DeepEqual(state, afterState) {
				t.Error("replay changed actor/timing state")
			}
			next, nextRevision, nextCharge := retryQAMeta(t, path)
			if bytes.Equal(nonce, next) || !bytes.Equal(revision, nextRevision) || charge != nextCharge {
				t.Error("exact replay was not a nonce-only durability fence")
			}
			qaWorkerNotification(t, socket, "wake")
			qaWorkerNotification(t, socket, "")
		})
	}
}

func TestQAHostBoundedRetryPersistentBusyStopsAtThree(t *testing.T) {
	path, socket := qaWorkerNotificationSocket(t)
	d, cwd, _, _ := qaHostWakeFixture(t, path)
	before, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{})
	if err != nil {
		t.Fatal("baseline receipt read failed")
	}
	hits := 0
	startContextQASetSQLHooks(startContextQASQLHooks{Fault: func(e startContextQASQLEvent) error {
		if e.Phase == "prepare-before" {
			hits++
			return sqliteio.ErrBusy
		}
		return nil
	}})
	t.Cleanup(func() { startContextQASetSQLHooks(startContextQASQLHooks{}) })
	out, diagnostic := retryQARun(t, context.Background(), d, retryQAPayload(t, cwd, "SubagentStart"))
	startContextQASetSQLHooks(startContextQASQLHooks{})
	if hits != 3 {
		t.Errorf("native busy attempts=%d, want three total", hits)
	}
	startContextQAExpect(t, out, diagnostic, "SubagentStart", "state_busy", "not_committed")
	after, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Error("exhausted busy changed receipts")
	}
	qaWorkerNotification(t, socket, "")
}

func TestQAHostBoundedRetryRejectsUnsafeErrorTrees(t *testing.T) {
	busy := &activity.Error{Code: "state_busy", Retryable: true}
	unknown := &activity.Error{Code: "local_write_unknown", Uncertain: true}
	uncertain := &activity.Error{Code: "state_busy", Retryable: true, Uncertain: true}
	for _, tc := range []struct {
		name   string
		cause  error
		cancel bool
	}{
		{"busy_then_unknown", errors.Join(busy, unknown), false},
		{"unknown_then_busy", errors.Join(unknown, busy), false},
		{"busy_then_uncertain", errors.Join(busy, uncertain), false},
		{"uncertain_then_busy", errors.Join(uncertain, busy), false},
		{"nonbusy_sibling", errors.Join(busy, &activity.Error{Code: "state_corrupt"}), false},
		{"not_retryable", &activity.Error{Code: "state_busy"}, false},
		{"native_cleanup", &sqliteio.Error{Phase: sqliteio.PreparePhase, Category: sqliteio.Busy, Cause: sqliteio.ErrBusy, Cleanup: &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO}}, false},
		{"close_busy", &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.Busy}, false},
		{"finalize_busy", &sqliteio.Error{Phase: sqliteio.FinalizePhase, Category: sqliteio.Busy}, false},
		{"rollback_busy", &sqliteio.Error{Phase: sqliteio.RollbackPhase, Category: sqliteio.Busy}, false},
		{"commit_busy", &sqliteio.Error{Phase: sqliteio.CommitPhase, Category: sqliteio.Busy}, false},
		{"statement_canceled", &sqliteio.Error{Phase: sqliteio.StepPhase, Category: sqliteio.Canceled}, false},
		{"bare_canceled_sibling", errors.Join(busy, context.Canceled), false},
		{"admission_does_not_authorize_canceled_sibling", errors.Join(busy, &sqliteio.Error{Phase: sqliteio.OpenPhase, Category: sqliteio.Canceled, Cause: context.DeadlineExceeded}, context.Canceled), false},
		{"caller_canceled", sqliteio.ErrBusy, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, socket := qaWorkerNotificationSocket(t)
			d, cwd, _, _ := qaHostWakeFixture(t, path)
			before, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{})
			if err != nil {
				t.Fatal("baseline receipt read failed")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hits := 0
			startContextQASetSQLHooks(startContextQASQLHooks{Fault: func(e startContextQASQLEvent) error {
				if e.Phase != "prepare-before" {
					return nil
				}
				hits++
				if tc.cancel {
					cancel()
				}
				if native, ok := tc.cause.(*sqliteio.Error); ok {
					return native
				}
				// Preserve a complete checked error tree through the existing
				// native fault boundary; no fabricated operation result.
				return &sqliteio.Error{Phase: sqliteio.PreparePhase, Category: sqliteio.Busy, Cause: tc.cause}
			}})
			t.Cleanup(func() { startContextQASetSQLHooks(startContextQASQLHooks{}) })
			_, diagnostic := retryQARun(t, ctx, d, retryQAPayload(t, cwd, "SubagentStart"))
			startContextQASetSQLHooks(startContextQASQLHooks{})
			if hits != 1 || diagnostic == "" {
				t.Errorf("unsafe refusal attempts=%d, want one with final diagnostic", hits)
			}
			after, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{})
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Error("unsafe refusal changed receipts")
			}
			qaWorkerNotification(t, socket, "")
		})
	}
}

func TestQAHostBoundedRetryNeverReplaysUnknownOrCommittedReview(t *testing.T) {
	for _, mode := range []string{"unknown_close", "committed_review"} {
		t.Run(mode, func(t *testing.T) {
			path, socket := qaWorkerNotificationSocket(t)
			d, cwd, seconds, failed := qaHostWakeFixture(t, path)
			*seconds = 10
			kind := "SubagentStart"
			if mode == "committed_review" {
				kind = "SessionStart"
				*failed = true
			}
			durable, faults := 0, 0
			startContextQASetSQLHooks(startContextQASQLHooks{Observe: func(e startContextQASQLEvent) {
				if e.Phase == "durable-native-closed" && e.Code == 0 {
					durable++
				}
			}, Fault: func(e startContextQASQLEvent) error {
				if mode == "unknown_close" && e.Phase == "durable-native-closed" && e.Code == 0 && faults == 0 {
					faults++
					return errors.New("SECRET native close detail")
				}
				return nil
			}})
			t.Cleanup(func() { startContextQASetSQLHooks(startContextQASQLHooks{}) })
			out, diagnostic := retryQARun(t, context.Background(), d, retryQAPayload(t, cwd, kind))
			startContextQASetSQLHooks(startContextQASQLHooks{})
			*failed = false
			if durable != 1 {
				t.Errorf("unsafe outcome dispatched %d durable fences, want one", durable)
			}
			if mode == "unknown_close" {
				if faults != 1 {
					t.Fatal("actual close fault was not reached")
				}
				startContextQAExpect(t, out, diagnostic, kind, "local_write_unknown", "unknown")
			} else {
				startContextQAExpect(t, out, diagnostic, kind, "clock_unavailable", "committed")
			}
			rows, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{Source: "codex"})
			if err != nil || len(rows.Receipts) != 3 {
				t.Fatal("actual safety/child receipt missing")
			}
			state, err := d.Activity.Status(context.Background())
			if err != nil {
				t.Fatal("cold status failed")
			}
			if mode == "committed_review" && (len(state.Uncertainties) != 1 || len(state.ClosedIntervals) != 0) {
				t.Error("review quarantine was not preserved")
			}
			qaWorkerNotification(t, socket, "")
		})
	}
}
