//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// These are post-implementation controls for a new private observer API.
// Missing symbols or compilation failure on old35b1 are never semantic RED.
func TestSQLiteSyncFailureDiagnosticsService(t *testing.T) {
	type caseSpec struct {
		name, code, delta, first string
		last                     int
	}
	cases := []caseSpec{
		{"success", "", "complete", "none", 10},
		{"replay", "", "replay", "none", 2},
		{"admission", "validation", "unchanged", "admission", 0},
		{"observe", "state_corrupt", "unchanged", "observe", 1},
		{"guard-acquire", "state_busy", "unchanged", "guard_acquire", 3},
		{"maintenance", "state_corrupt", "unchanged", "wal_maintenance", 5},
		{"reserve", "state_corrupt", "unchanged", "reserve", 6},
		{"barrier", "local_write_unknown", "pending", "before_complete_barrier", 8},
		{"complete", "state_corrupt", "pending", "complete", 10},
		{"cleanup-before", "local_write_unknown", "complete", "guard_cleanup", 10},
		{"cleanup-after", "local_write_unknown", "complete", "guard_cleanup", 10},
		{"primary-cleanup", "local_write_unknown", "pending", "before_complete_barrier", 8},
		{"cancel-at-barrier", "state_busy", "pending", "guard_verify_final", 9},
		{"caller-deadline", "state_busy", "unchanged", "observe", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var offResult SyncRun
			var offCode string
			for _, enabled := range []bool{false, true} {
				t.Run(map[bool]string{false: "off", true: "on"}[enabled], func(t *testing.T) {
					s, f := scvQABootstrap(t)
					in := SyncRunInput{RequestID: snQAID(3901)}
					var prior SyncRun
					if tc.name == "replay" {
						var err error
						prior, err = s.SyncNow(context.Background(), in, qaSyncNoProvider(t))
						if err != nil || prior.State != "complete" {
							t.Fatal("SETUP actual zero-root terminal receipt", err)
						}
					}
					before := scvQAAudit(t, f, in.RequestID)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var held *sqliteSyncRunGuard
					if tc.name == "guard-acquire" {
						held = sfdQAHeldGuard(t, s)
						var boundedCancel context.CancelFunc
						ctx, boundedCancel = context.WithTimeout(ctx, 75*time.Millisecond)
						defer boundedCancel()
					}
					if tc.name == "caller-deadline" {
						var expiredCancel context.CancelFunc
						ctx, expiredCancel = context.WithDeadline(ctx, time.Now().Add(-time.Millisecond))
						defer expiredCancel()
					}
					if tc.name == "admission" {
						s.store.timeout = -time.Nanosecond
					}
					originalDeadline, bounded := ctx.Deadline()
					var d *sqliteSyncFailureDiagnostics
					if enabled {
						ctx, d = withSQLiteSyncFailureDiagnostics(ctx)
						got, ok := ctx.Deadline()
						if ok != bounded || !got.Equal(originalDeadline) {
							t.Fatal("observer changed original caller deadline")
						}
					}
					faults := sfdQAInstall(t, s, tc.name, cancel)
					// Exactly one measured call. No diagnostic or assertion retries it.
					result, err := s.SyncNow(ctx, in, qaSyncNoProvider(t))
					sfdQAClear(s)
					faults.check(t)
					s.store.timeout = 0
					if held != nil {
						if err := held.Close(); err != nil {
							t.Fatal("held guard did not end", err)
						}
					}
					code := ""
					if err != nil {
						var domain *Error
						if !errors.As(err, &domain) {
							t.Fatal("original safe domain error lost")
						}
						code = domain.Code
					}
					if code != tc.code {
						t.Fatal("observer changed expected public error classification", code)
					}
					if err != nil && !reflect.DeepEqual(result, SyncRun{}) {
						t.Fatal("failed operation returned nonzero run")
					}
					if tc.name == "replay" && !reflect.DeepEqual(result, prior) {
						t.Fatal("terminal replay changed the saved result")
					}
					if !enabled {
						offResult, offCode = result, code
					} else if code != offCode || !reflect.DeepEqual(result, offResult) {
						t.Fatal("observer changed enabled/off public outcome")
					}
					if strings.HasPrefix(tc.name, "cleanup-") || tc.name == "primary-cleanup" {
						sfdQACleanupEvidence(t, err, tc.name == "primary-cleanup")
					}
					after := scvQAAudit(t, f, in.RequestID)
					sfdQADelta(t, before, after, in, tc.delta, result)
					if !enabled {
						return
					}
					r := sfdQAReadRecord(t, d)
					if r.Error != (err != nil) || r.First != tc.first {
						t.Fatal("diagnostic mislocated original failure")
					}
					for i := 0; i < 12; i++ {
						reached := i <= tc.last && i != 2
						if tc.name == "replay" {
							reached = i <= 2
						}
						if i == 11 {
							reached = tc.last >= 3 && tc.name != "replay"
						}
						if reached != (r.Phase[2*i] >= 0) || reached != (r.Phase[2*i+1] >= 0) {
							t.Fatal("phase reach disagrees with actual witnessed path", sfdQAPhases[i])
						}
					}
					wantClose := 0
					if tc.last >= 3 && tc.name != "replay" {
						wantClose = 1
					}
					if strings.HasPrefix(tc.name, "cleanup-") || tc.name == "primary-cleanup" {
						wantClose = 2
					}
					if r.CloseCalls != wantClose || r.CleanupFailed != (wantClose == 2) {
						t.Fatal("diagnostic lost original adapter cleanup calls/failure")
					}
					if (r.CompletionExpired != nil) != (tc.last >= 10 && tc.name != "replay") || (r.InitialExpired != nil) != (tc.name != "admission") {
						t.Fatal("deadline observation invented or omitted original assignment")
					}
					wantCaller := "live"
					if tc.name == "caller-deadline" {
						wantCaller = "deadline"
					}
					if tc.name == "cancel-at-barrier" {
						wantCaller = "canceled"
					}
					if tc.name == "guard-acquire" {
						// The absolute native admission timer can precede context timer
						// delivery. The separate expired-caller case pins deadline status.
						if r.Caller != "live" && r.Caller != "deadline" {
							t.Fatal("guard refusal invented caller cancellation")
						}
					} else if r.Caller != wantCaller {
						t.Fatal("final observer ran after own cancel or lost original caller")
					}
					if err != nil {
						if r.State != "none" || r.Attempted != -1 || r.Remaining != -1 {
							t.Fatal("cleanup/error observation retained pre-defer successful result")
						}
						line := sqliteSyncFailureDiagnosticLog(d, true, true)
						if line == sfdQAPrefix+"unavailable" || !strings.HasPrefix(line, sfdQAPrefix) || len(line) > 4096 || !strings.Contains(line, `"provider_posts_unchanged":true`) {
							t.Fatal("failure record missing or unbounded")
						}
					} else if sqliteSyncFailureDiagnosticLog(d, false, true) != "" || r.State != "complete" || r.Attempted != 0 || r.Remaining != 0 {
						t.Fatal("successful zero-root run emitted failure or changed result facts")
					}
				})
			}
		})
	}
}

type sfdQAPrivateKey struct{}
type sfdQAPanicStringer struct{}

func (sfdQAPanicStringer) Error() string  { panic("PRIVATE-ERROR formatted") }
func (sfdQAPanicStringer) String() string { panic("PRIVATE-ERROR formatted") }

func TestSQLiteSyncFailureDiagnosticsFixedRecord(t *testing.T) {
	t.Run("snapshot-copies-and-private-counts", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), sfdQAPrivateKey{}, sfdQAPanicStringer{})
		ctx, d := withSQLiteSyncFailureDiagnostics(ctx)
		before, err := json.Marshal(d.snapshot())
		if err != nil {
			t.Fatal(err)
		}
		var unset sfdQARecord
		if json.Unmarshal(before, &unset) != nil {
			t.Fatal("initial snapshot")
		}
		for _, stamp := range unset.Phase {
			if stamp != -1 {
				t.Fatal("unreached boundary became reached zero")
			}
		}
		base := time.Now()
		d.initialDeadline(base.Add(-time.Second))
		d.completionDeadline(base.Add(time.Second))
		d.begin(0)
		d.end(0, false)
		d.begin(7)
		d.end(7, true)
		d.begin(11)
		d.guardClose()
		d.end(11, true)
		d.finish(ctx, SyncRun{RequestID: "PRIVATE-REQUEST", State: "PRIVATE-STATE", AttemptedIDs: []string{"PRIVATE-PATH", "SELECT PRIVATE"}, RemainingCount: 101}, true)
		r := sfdQAReadRecord(t, d)
		// Signed duration-to-microsecond conversion truncates toward zero;
		// offsets on opposite sides of the origin can differ by one microsecond.
		if r.First != "submit_roots" || !r.CleanupFailed || r.Attempted != 2 || r.Remaining != 101 || r.State != "other" || r.Caller != "live" || r.InitialExpired == nil || !*r.InitialExpired || r.CompletionExpired == nil || *r.CompletionExpired || r.Completion-r.Initial < 1999999 || r.Completion-r.Initial > 2000000 {
			t.Fatal("finite copied facts or deadline interval changed")
		}
		snap := d.snapshot()
		v := reflect.ValueOf(&snap).Elem()
		mutatedArray, mutatedPointer := false, false
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if f.CanSet() && f.Kind() == reflect.Array && f.Len() == 24 && f.Index(0).Kind() == reflect.Int64 {
				f.Index(0).SetInt(999999)
				mutatedArray = true
			}
			if f.Kind() == reflect.Pointer && !f.IsNil() && f.Elem().Kind() == reflect.Bool {
				f.Elem().SetBool(!f.Elem().Bool())
				mutatedPointer = true
			}
		}
		if !mutatedArray || !mutatedPointer || !reflect.DeepEqual(r, sfdQAReadRecord(t, d)) {
			t.Fatal("snapshot shared array or optional-boolean state")
		}
		if line := sqliteSyncFailureDiagnosticLog(d, true, false); line == sfdQAPrefix+"unavailable" || len(line) > 4096 || !strings.Contains(line, `"provider_posts_unchanged":false`) {
			t.Fatal("bounded failure serialization")
		}
	})
	t.Run("duplicates-invalid-and-reuse", func(t *testing.T) {
		for _, action := range []string{"duplicate-begin", "duplicate-end", "bad-index", "unpaired-end", "duplicate-deadline", "reuse", "postfinish-marker", "close-overflow"} {
			t.Run(action, func(t *testing.T) {
				ctx, d := withSQLiteSyncFailureDiagnostics(context.Background())
				d.begin(0)
				d.end(0, false)
				old := d.snapshot()
				switch action {
				case "duplicate-begin":
					d.begin(0)
				case "duplicate-end":
					d.end(0, true)
				case "bad-index":
					d.begin(-1)
					d.end(12, true)
				case "unpaired-end":
					d.end(1, true)
				case "duplicate-deadline":
					d.initialDeadline(time.Now())
					old = d.snapshot()
					d.initialDeadline(time.Now().Add(time.Second))
				case "reuse":
					d.finish(ctx, SyncRun{State: "complete", AttemptedIDs: []string{}, RemainingCount: 101}, true)
				case "postfinish-marker":
					d.finish(ctx, SyncRun{}, true)
					d.begin(1)
				case "close-overflow":
					d.guardClose()
					d.guardClose()
					d.guardClose()
				}
				d.finish(ctx, SyncRun{}, true)
				b, _ := json.Marshal(d.snapshot())
				var r sfdQARecord
				_ = json.Unmarshal(b, &r)
				ob, _ := json.Marshal(old)
				var original sfdQARecord
				_ = json.Unmarshal(ob, &original)
				if !r.Dropped || r.Phase[0] != original.Phase[0] || r.Phase[1] != original.Phase[1] || r.Failed[0] || sqliteSyncFailureDiagnosticLog(d, true, true) != sfdQAPrefix+"unavailable" {
					t.Fatal("invalid marker overwrote first evidence or emitted trusted record")
				}
				if action == "duplicate-deadline" && r.Initial != original.Initial || action == "reuse" && (r.State != "complete" || r.Remaining != 101 || r.Attempted != 0) || action == "postfinish-marker" && r.Phase[2] != -1 || action == "close-overflow" && r.CloseCalls != 2 {
					t.Fatal("reuse or invalid count overwrote first immutable facts")
				}
			})
		}
	})
	t.Run("bounded-counts-nil-and-caller", func(t *testing.T) {
		var nilD *sqliteSyncFailureDiagnostics
		nilD.begin(0)
		nilD.end(0, true)
		nilD.guardClose()
		nilD.initialDeadline(time.Time{})
		nilD.completionDeadline(time.Time{})
		nilD.finish(context.Background(), SyncRun{}, true)
		if sqliteSyncFailureDiagnosticLog(nilD, false, true) != "" || sqliteSyncFailureDiagnosticLog(nilD, true, true) != sfdQAPrefix+"unavailable" {
			t.Fatal("nil collector path changed emission")
		}
		for _, canceled := range []bool{false, true} {
			ctx, cancel := context.WithCancel(context.Background())
			if canceled {
				cancel()
			}
			defer cancel()
			ctx, d := withSQLiteSyncFailureDiagnostics(ctx)
			d.finish(ctx, SyncRun{State: "complete", AttemptedIDs: make([]string, 101), RemainingCount: int(sfdQAMax + 1)}, true)
			b, _ := json.Marshal(d.snapshot())
			var r sfdQARecord
			_ = json.Unmarshal(b, &r)
			want := "live"
			if canceled {
				want = "canceled"
			}
			if !r.Dropped || r.Attempted != -1 || r.Remaining != -1 || r.Caller != want || sqliteSyncFailureDiagnosticLog(d, true, true) != sfdQAPrefix+"unavailable" {
				t.Fatal("overflow clamped counts or changed caller")
			}
		}
	})
	t.Run("maximum-wire-zero-versus-absent-and-malformed", func(t *testing.T) {
		f, tv := false, true
		r := sfdQARecord{Version: 1, Source: "sync_now_private_phase", First: "admission", CleanupFailed: true, CloseCalls: 2, Initial: -sfdQAMax, Completion: sfdQAMax, InitialExpired: &tv, CompletionExpired: &f, Caller: "deadline", State: "interrupted", Attempted: 100, Remaining: sfdQAMax, Error: true}
		for i := range r.Phase {
			r.Phase[i] = sfdQAMax
		}
		for i := range r.Failed {
			r.Failed[i] = true
		}
		b, err := json.Marshal(r)
		if _, ok := sfdQADecode(b); err != nil || !ok || len(b)+len(sfdQAPrefix)+40 > 4096 {
			t.Fatal("maximum valid fixed record exceeds compact bound")
		}
		for _, mutation := range []string{"unknown-field", "null-count", "bad-enum", "short-phase", "bool-timestamp", "count-overflow", "lost-cleanup", "wrong-first", "absent-with-expiry-value"} {
			var fields map[string]json.RawMessage
			if json.Unmarshal(b, &fields) != nil {
				t.Fatal("wire fixture")
			}
			switch mutation {
			case "unknown-field":
				fields["PRIVATE-PATH"] = json.RawMessage(`"PRIVATE-ERROR"`)
			case "null-count":
				fields["remaining_count"] = json.RawMessage("null")
			case "bad-enum":
				fields["caller_at_return"] = json.RawMessage(`"PRIVATE-STATE"`)
			case "short-phase":
				fields["phase_us"] = json.RawMessage("[0]")
			case "bool-timestamp":
				fields["initial_admission_deadline_us"] = json.RawMessage("true")
			case "count-overflow":
				fields["attempted_count"] = json.RawMessage("101")
			case "lost-cleanup":
				fields["guard_cleanup_failed"] = json.RawMessage("false")
			case "wrong-first":
				fields["first_failed_phase"] = json.RawMessage(`"guard_cleanup"`)
			case "absent-with-expiry-value":
				fields["initial_deadline_expired_at_return"] = json.RawMessage("null")
			}
			bad, _ := json.Marshal(fields)
			if _, ok := sfdQADecode(bad); ok {
				t.Fatal("malformed record accepted", mutation)
			}
		}
		for i := range r.Phase {
			r.Phase[i] = -1
		}
		for i := range r.Failed {
			r.Failed[i] = false
		}
		r.First, r.CleanupFailed = "none", false
		r.Phase[14], r.Phase[15] = 0, 0
		b, _ = json.Marshal(r)
		if got, ok := sfdQADecode(b); !ok || got.Phase[14] != 0 || got.Phase[12] != -1 {
			t.Fatal("zero-length reached submit span confused with absent phase")
		}
	})
}
