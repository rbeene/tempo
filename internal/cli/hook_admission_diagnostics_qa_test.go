//go:build (darwin || linux) && (amd64 || arm64)

package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/hookstate"
)

// Public old APIs only: QA-only compilation must succeed before the producer.
// The existing inert native fault proves phase plumbing, not hosted hash cost.
func TestQAHostAdmissionDiagnosticsFailureOnlyOptIn(t *testing.T) {
	for _, opt := range []string{"", "true", "1"} {
		t.Run("opt_"+opt, func(t *testing.T) {
			path, socket := qaWorkerNotificationSocket(t)
			d, cwd, _, _ := qaHostWakeFixture(t, path)
			// Keep fixture setup's existing calibration, but the observed action
			// itself uses the unchanged production default 250ms admission.
			d.Activity = activity.New(activity.Options{Path: path, HookPolicies: hookstate.New(hookstate.Options{Path: filepath.Join(filepath.Dir(path), "policy", "state.json")})})
			getenv := d.Getenv
			d.Getenv = func(key string) string {
				if key == "TEMPO_HOOK_DIAGNOSTICS" {
					return opt
				}
				return getenv(key)
			}
			before, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{})
			if err != nil {
				t.Fatal("receipt prerequisite", err)
			}
			payload, err := json.Marshal(map[string]string{"hook_event_name": "SessionStart", "source": "startup", "session_id": "diagnostic-new-session", "cwd": cwd})
			if err != nil {
				t.Fatal("payload prerequisite")
			}
			hits := 0
			startContextQASetSQLHooks(startContextQASQLHooks{Fault: func(e startContextQASQLEvent) error {
				if e.Operation == "capture-write-handoff" && e.Phase == "handoff-before-writer" {
					hits++
					return sqliteio.ErrBusy
				}
				return nil
			}})
			t.Cleanup(func() { startContextQASetSQLHooks(startContextQASQLHooks{}) })
			var out, diagnostic bytes.Buffer
			code := cli.Run(context.Background(), []string{"hook", "codex", "--input-stdin"}, bytes.NewReader(payload), &out, &diagnostic, d)
			startContextQASetSQLHooks(startContextQASQLHooks{})
			if code != 0 || hits != 3 || diagnostic.String() != "tempo hook: state_busy; durability=not_committed\n" {
				t.Fatal("actual three-attempt refusal prerequisite", code, hits)
			}
			if out.Len() > 4096 || strings.Contains(out.String(), cwd) || strings.Contains(out.String(), "diagnostic-new-session") {
				t.Error("diagnostic leaked identity or exceeded bound")
			}
			var response struct {
				Hook struct {
					Kind string `json:"hookEventName"`
					Text string `json:"additionalContext"`
				} `json:"hookSpecificOutput"`
			}
			if json.Unmarshal(out.Bytes(), &response) != nil {
				t.Fatal("native response was not valid JSON")
			}
			public := "tempo capture: kind=SessionStart; code=state_busy; durability=not_committed"
			const prefix = "\ntempo hook diagnostics v2: "
			if opt != "1" {
				if response.Hook.Kind != "SessionStart" || response.Hook.Text != public {
					t.Error("default public three-field diagnostic changed")
				}
			} else {
				parts := strings.Split(response.Hook.Text, prefix)
				if len(parts) != 2 || parts[0] != public {
					// Old-producer RED must still audit every durable postcondition.
					t.Error("opt-in omitted bounded admission metadata")
				} else {
					var rows []struct {
						Ordinal        int      `json:"ordinal"`
						Start          int64    `json:"start_us"`
						End            int64    `json:"end_us"`
						Deadline       int64    `json:"deadline_us"`
						CallerDeadline int64    `json:"caller_deadline_us"`
						Phase          [8]int64 `json:"phase_us"`
						Caller         string   `json:"caller"`
						Retry          bool     `json:"retry"`
						NativePhase    string   `json:"native_phase"`
						NativeCategory string   `json:"native_category"`
						NativeCode     int32    `json:"native_code"`
						NativeCleanup  bool     `json:"native_cleanup"`
						Eligibility    *struct {
							P [7]int64    `json:"p"`
							R [6][9]int64 `json:"r"`
							C [2]int64    `json:"c"`
							D bool        `json:"d"`
						} `json:"eligibility"`
					}
					if json.Unmarshal([]byte(parts[1]), &rows) != nil || len(rows) != 3 {
						t.Error("wrong bounded attempt inventory")
					} else {
						for i, row := range rows {
							if row.Eligibility == nil || row.Eligibility.D {
								t.Error("opt-in omitted complete policy cost observation", i)
							} else {
								p := row.Eligibility
								for _, phase := range []int{0, 1, 2, 4, 6} {
									if p.P[phase] < 0 {
										t.Error("reached policy phase missing", i, phase)
									}
								}
								if p.P[3] != -1 || p.P[5] != -1 {
									t.Error("unreached installed-root/invalidation phase fabricated", i)
								}
								for role, name := range []string{"runtime", "executable", "definitions"} {
									r := p.R[role]
									if r[0] != 1 || r[1] != 1 || r[2] != int64(len("synthetic "+name)) || r[3] < 1 {
										t.Error("actual role byte/read accounting missing", i, role)
									}
								}
								for role := 3; role < 6; role++ {
									if p.R[role] != [9]int64{} {
										t.Error("unvisited artifact role fabricated", i, role)
									}
								}
								if p.C[0] < -1 || p.C[1] < -1 || (p.C[0] == -1) != (p.C[1] == -1) {
									t.Error("invalid process CPU availability", i)
								}
							}
							// The deadline is created after validation/location. Compare
							// the actual two deadlines, not entry time plus invented slack.
							if row.NativePhase != "open" || row.NativeCategory != "busy" || row.NativeCode != 0 || row.NativeCleanup {
								t.Error("known checked boundary diagnostic was misclassified", i)
							}
							if row.Ordinal != i+1 || row.Start < 0 || row.End < row.Start || row.Deadline <= row.Start || row.Deadline > row.CallerDeadline || row.CallerDeadline > 900000 || row.Caller != "live" || !row.Retry {
								t.Error("attempt/deadline evidence changed", i)
							}
							for p, stamp := range row.Phase {
								if stamp < row.Start || stamp > row.End || p > 0 && stamp < row.Phase[p-1] {
									t.Error("actual read/discovery/policy/writer phase missing or reversed", i, p)
								}
							}
						}
					}
				}
			}
			after, err := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{})
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Error("diagnostics altered refused capture history")
			}
			qaWorkerNotification(t, socket, "")
			if store := d.Store.(*fakeStore); store.gets+store.sets+store.deletes != 0 {
				t.Error("diagnostics touched credentials")
			}
			// On opt-in, both clean first capture and exact replay stay quiet.
			if opt == "1" {
				for i := 0; i < 2; i++ {
					out.Reset()
					diagnostic.Reset()
					if cli.Run(context.Background(), []string{"hook", "codex", "--input-stdin"}, bytes.NewReader(payload), &out, &diagnostic, d) != 0 || out.String() != "{}\n" || diagnostic.Len() != 0 {
						t.Error("opt-in changed clean capture or replay output")
					}
					qaWorkerNotification(t, socket, "wake")
				}
				qaWorkerNotification(t, socket, "")
				last, x := d.Activity.HostReceipts(context.Background(), activity.HostReceiptFilter{})
				if x != nil || len(last.Receipts) != len(before.Receipts)+1 {
					t.Error("clean replay duplicated the session receipt")
				}
			}
		})
	}
}
