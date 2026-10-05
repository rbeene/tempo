//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const sodQAMax = int64(9007199254740991)

var sodQAPhases = [6]string{"inspect", "begin", "schema", "rows", "checked_cleanup", "projection"}

// Independent wire contract: no producer record type or enum helper is used.
type sodQAError struct {
	Domain    string `json:"domain_code"`
	Phase     string `json:"native_phase"`
	Category  string `json:"native_category"`
	Code      int32  `json:"native_code"`
	Cleanup   bool   `json:"native_cleanup"`
	Retained  bool   `json:"retained_cleanup"`
	Truncated bool   `json:"truncated"`
}
type sodQARecord struct {
	Version            int        `json:"schema_version"`
	Source             string     `json:"source"`
	Phase              [12]int64  `json:"phase_us"`
	Failed             [6]bool    `json:"phase_failed"`
	First              string     `json:"first_failed_phase"`
	Cleanup            bool       `json:"cleanup_failed"`
	Operation          int64      `json:"operation_deadline_us"`
	Acquisition        int64      `json:"acquisition_deadline_us"`
	OperationExpired   *bool      `json:"operation_expired_at_return"`
	AcquisitionExpired *bool      `json:"acquisition_expired_at_return"`
	Caller             string     `json:"caller_at_return"`
	Context            string     `json:"operation_at_return"`
	Error              bool       `json:"final_error_present"`
	Dropped            bool       `json:"dropped"`
	Primary            sodQAError `json:"first_error"`
	CleanupError       sodQAError `json:"cleanup_error"`
}

func sodQARecordRead(t *testing.T, d *sqliteStatusFailureDiagnostics) sodQARecord {
	t.Helper()
	b, err := json.Marshal(d.snapshot())
	var fields map[string]json.RawMessage
	if err != nil || json.Unmarshal(b, &fields) != nil || len(fields) != 16 {
		t.Fatal("fixed Status record keys")
	}
	for k, v := range fields {
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) && k != "operation_expired_at_return" && k != "acquisition_expired_at_return" {
			t.Fatal("unexpected null field", k)
		}
	}
	var r sodQARecord
	dc := json.NewDecoder(bytes.NewReader(b))
	dc.DisallowUnknownFields()
	if dc.Decode(&r) != nil || r.Version != 1 || r.Source != "status_private_phase" || r.Dropped {
		t.Fatal("invalid fixed Status record")
	}
	var phases []int64
	var failures []bool
	if json.Unmarshal(fields["phase_us"], &phases) != nil || len(phases) != 12 || json.Unmarshal(fields["phase_failed"], &failures) != nil || len(failures) != 6 {
		t.Fatal("nonfixed phase arrays")
	}
	first := "none"
	for i, name := range sodQAPhases {
		a, z := r.Phase[2*i], r.Phase[2*i+1]
		if a < -1 || z < -1 || a > sodQAMax || z > sodQAMax || (a == -1) != (z == -1) || z < a || a == -1 && r.Failed[i] {
			t.Fatal("invalid phase timestamps")
		}
		if first == "none" && r.Failed[i] {
			first = name
		}
	}
	if first != r.First || r.Cleanup != r.Failed[4] {
		t.Fatal("first failure or cleanup not preserved")
	}
	for _, v := range []int64{r.Operation, r.Acquisition} {
		if v < -sodQAMax || v > sodQAMax {
			t.Fatal("unsafe deadline integer")
		}
	}
	if r.OperationExpired == nil && r.Operation != -1 || r.AcquisitionExpired == nil && r.Acquisition != -1 {
		t.Fatal("unset deadline acquired expiry")
	}
	for _, v := range []string{r.Caller, r.Context} {
		if v != "live" && v != "canceled" && v != "deadline" {
			t.Fatal("context enum")
		}
	}
	for _, k := range []string{"first_error", "cleanup_error"} {
		var nested map[string]json.RawMessage
		if json.Unmarshal(fields[k], &nested) != nil || len(nested) != 7 {
			t.Fatal("fixed error keys")
		}
		for _, v := range nested {
			if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
				t.Fatal("null error fact")
			}
		}
	}
	for _, e := range []sodQAError{r.Primary, r.CleanupError} {
		if e.Code < 0 || !sodQAContains([]string{"none", "other", "validation", "state_busy", "state_corrupt", "state_path_in_use", "local_write_unknown"}, e.Domain) || !sodQAContains([]string{"none", "other", "admission", "open", "begin", "prepare", "bind", "step", "commit", "verify", "rollback", "finalize", "close", "checkpoint"}, e.Phase) || !sodQAContains([]string{"none", "other", "invalid", "busy", "canceled", "unsafe", "corrupt", "full", "constraint", "io", "closed", "misuse"}, e.Category) {
			t.Fatal("unsafe native error projection")
		}
	}
	for _, secret := range []string{"PRIVATE-STATUS-ERROR", "PRIVATE-CLEANUP-ERROR", "SELECT PRIVATE", "PRIVATE-PATH"} {
		if strings.Contains(string(b), secret) {
			t.Fatal("private content escaped diagnostic")
		}
	}
	if len(b) > 4096 {
		t.Fatal("fixed record exceeded bound")
	}
	return r
}

type sodQAPrivateError struct{}

func (sodQAPrivateError) Error() string { panic("observer formatted a private error") }

type sodQACycle struct{ visits int }

func (e *sodQACycle) Error() string { panic("observer formatted a cycle") }
func (e *sodQACycle) Unwrap() error { e.visits++; return e }

func TestSQLiteStatusFailureDiagnosticsFixedRecord(t *testing.T) {
	t.Run("first-failure-deadlines-and-snapshot", func(t *testing.T) {
		ctx, d := withSQLiteStatusFailureDiagnostics(context.Background())
		initial := sodQARecordRead(t, d)
		if initial.Operation != -1 || initial.Acquisition != -1 || initial.OperationExpired != nil || initial.AcquisitionExpired != nil {
			t.Fatal("absent deadline became zero or expired")
		}
		for _, v := range initial.Phase {
			if v != -1 {
				t.Fatal("unreached phase became reached zero")
			}
		}
		base := time.Now()
		d.deadline(0, base.Add(time.Second))
		d.deadline(1, base.Add(-time.Second))
		primary := &sqliteio.Error{Phase: sqliteio.OpenPhase, Category: sqliteio.Canceled, Code: 9, Cause: sodQAPrivateError{}}
		cleanup := &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO, Code: 10, Cause: sodQAPrivateError{}}
		d.begin(0)
		d.end(0, primary)
		d.begin(4)
		d.end(4, cleanup)
		d.finish(ctx, ctx, errors.Join(primary, cleanup))
		r := sodQARecordRead(t, d)
		if r.First != "inspect" || !r.Cleanup || r.Primary.Phase != "open" || r.Primary.Category != "canceled" || r.Primary.Code != 9 || r.CleanupError.Phase != "close" || r.OperationExpired == nil || *r.OperationExpired || r.AcquisitionExpired == nil || !*r.AcquisitionExpired || r.Operation-r.Acquisition < 1999999 || r.Operation-r.Acquisition > 2000000 {
			t.Fatal("first error, independent cleanup or deadline evidence changed")
		}
		snap := d.snapshot()
		v := reflect.ValueOf(&snap).Elem()
		arrays, pointers := 0, 0
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if f.CanSet() && f.Kind() == reflect.Array && f.Len() == 12 && f.Index(0).Kind() == reflect.Int64 {
				f.Index(0).SetInt(123456)
				arrays++
			}
			if f.Kind() == reflect.Pointer && !f.IsNil() && f.Elem().Kind() == reflect.Bool {
				f.Elem().SetBool(!f.Elem().Bool())
				pointers++
			}
		}
		if arrays != 1 || pointers != 2 || !reflect.DeepEqual(r, sodQARecordRead(t, d)) {
			t.Fatal("snapshot aliased collector storage")
		}
		line := sqliteStatusFailureDiagnosticLog(d, true)
		if len(line) > 4096 || !strings.Contains(line, `"native_code":9`) || strings.Contains(line, "PRIVATE") {
			t.Fatal("bounded typed native evidence lost")
		}
		if sqliteStatusFailureDiagnosticLog(d, false) != "" {
			t.Fatal("success emitted failure record")
		}
	})
	t.Run("nil-and-malformed-lifecycle", func(t *testing.T) {
		var nilD *sqliteStatusFailureDiagnostics
		nilD.begin(-1)
		nilD.end(7, sodQAPrivateError{})
		nilD.deadline(-1, time.Time{})
		nilD.finish(nil, nil, sodQAPrivateError{})
		if sqliteStatusFailureDiagnosticLog(nilD, false) != "" || !strings.HasSuffix(sqliteStatusFailureDiagnosticLog(nilD, true), "unavailable") {
			t.Fatal("nil emission")
		}
		for _, mode := range []string{"duplicate-begin", "duplicate-end", "bad-phase", "unpaired-end", "duplicate-deadline", "zero-deadline", "overflow-deadline", "reuse", "postfinish"} {
			t.Run(mode, func(t *testing.T) {
				ctx, d := withSQLiteStatusFailureDiagnostics(context.Background())
				d.begin(0)
				d.end(0, nil)
				old := d.snapshot()
				switch mode {
				case "duplicate-begin":
					d.begin(0)
				case "duplicate-end":
					d.end(0, sodQAPrivateError{})
				case "bad-phase":
					d.begin(6)
				case "unpaired-end":
					d.end(3, sodQAPrivateError{})
				case "duplicate-deadline":
					d.deadline(0, time.Now())
					old = d.snapshot()
					d.deadline(0, time.Now().Add(time.Second))
				case "zero-deadline":
					d.deadline(0, time.Time{})
				case "overflow-deadline":
					d.deadline(0, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))
				case "reuse":
					d.finish(ctx, ctx, sodQAPrivateError{})
				case "postfinish":
					d.finish(ctx, ctx, sodQAPrivateError{})
					d.begin(1)
				}
				d.finish(ctx, ctx, sodQAPrivateError{})
				b, _ := json.Marshal(d.snapshot())
				var r sodQARecord
				_ = json.Unmarshal(b, &r)
				ob, _ := json.Marshal(old)
				var prior sodQARecord
				_ = json.Unmarshal(ob, &prior)
				if !r.Dropped || r.Phase[0] != prior.Phase[0] || r.Phase[1] != prior.Phase[1] || r.Failed[0] || !strings.HasSuffix(sqliteStatusFailureDiagnosticLog(d, true), "unavailable") {
					t.Fatal("invalid marker rewrote first evidence or emitted trusted record")
				}
			})
		}
	})
	t.Run("bounded-error-projection", func(t *testing.T) {
		private := sodQAPrivateError{}
		cases := []struct {
			err             error
			phase, category string
			code            int32
			truncated       bool
		}{
			{private, "none", "none", 0, false},
			{&sqliteio.Error{Phase: sqliteio.OpenPhase, Category: sqliteio.Canceled, Code: 9, Cause: private}, "open", "canceled", 9, false},
			{&sqliteio.Error{Phase: sqliteio.Phase("PRIVATE-PATH"), Category: sqliteio.Category("SELECT PRIVATE"), Code: 2147483647}, "other", "other", 2147483647, false},
			{&sqliteio.Error{Phase: sqliteio.OpenPhase, Category: sqliteio.IO, Code: -1}, "open", "io", 0, true},
		}
		for _, tc := range cases {
			b, err := json.Marshal(sqliteFailureTypedEvidence(tc.err))
			var got sodQAError
			if err != nil || json.Unmarshal(b, &got) != nil || got.Phase != tc.phase || got.Category != tc.category || got.Code != tc.code || got.Truncated != tc.truncated || strings.Contains(string(b), "PRIVATE") {
				t.Fatal("safe typed evidence projection")
			}
		}
		cycle := &sodQACycle{}
		got := sqliteFailureTypedEvidence(cycle)
		if !got.Truncated || cycle.visits < 1 || cycle.visits > 16 {
			t.Fatal("cyclic unwrap work was not bounded")
		}
		children := make([]error, 20)
		for i := range children {
			children[i] = private
		}
		b, _ := json.Marshal(sqliteFailureTypedEvidence(errors.Join(children...)))
		var limited sodQAError
		_ = json.Unmarshal(b, &limited)
		if !limited.Truncated {
			t.Fatal("broad joined evidence was not marked truncated")
		}
		primary := &sqliteio.Error{Phase: sqliteio.BeginPhase, Category: sqliteio.Busy, Code: 5, Cause: private, Cleanup: &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO, Code: 10}}
		b, _ = json.Marshal(sqliteFailureTypedEvidence(errors.Join(failure("state_busy"), primary)))
		var joined sodQAError
		_ = json.Unmarshal(b, &joined)
		if joined.Domain != "state_busy" || joined.Phase != "begin" || joined.Code != 5 || !joined.Cleanup {
			t.Fatal("native cleanup replaced primary evidence")
		}
	})
}

func TestSQLiteStatusFailureDiagnosticsWireBounds(t *testing.T) {
	ctx, d := withSQLiteStatusFailureDiagnostics(context.Background())
	d.finish(ctx, ctx, sodQAPrivateError{})
	f, tv := false, true
	r := sodQARecord{Version: 1, Source: "status_private_phase", First: "inspect", Cleanup: true, Operation: sodQAMax, Acquisition: -sodQAMax, OperationExpired: &f, AcquisitionExpired: &tv, Caller: "deadline", Context: "deadline", Error: true, Primary: sodQAError{Domain: "local_write_unknown", Phase: "checkpoint", Category: "constraint", Code: 2147483647, Cleanup: true, Retained: true, Truncated: true}, CleanupError: sodQAError{Domain: "state_path_in_use", Phase: "finalize", Category: "canceled", Code: 2147483647, Cleanup: true, Retained: true, Truncated: true}}
	for i := range r.Phase {
		r.Phase[i] = sodQAMax
	}
	for i := range r.Failed {
		r.Failed[i] = true
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"maximum-valid", "unsafe-timestamp", "unsafe-deadline", "bad-domain", "bad-phase", "bad-category", "negative-code", "no-native-with-code", "wrong-first", "lost-cleanup", "absent-deadline-without-expiry", "bad-context", "unpaired-phase"} {
		t.Run(mutation, func(t *testing.T) {
			var fields map[string]json.RawMessage
			_ = json.Unmarshal(b, &fields)
			switch mutation {
			case "unsafe-timestamp":
				var a []int64
				_ = json.Unmarshal(fields["phase_us"], &a)
				a[0] = sodQAMax + 1
				fields["phase_us"], _ = json.Marshal(a)
			case "unsafe-deadline":
				fields["operation_deadline_us"] = json.RawMessage("9007199254740992")
			case "bad-domain", "bad-phase", "bad-category", "negative-code", "no-native-with-code":
				var e map[string]json.RawMessage
				_ = json.Unmarshal(fields["first_error"], &e)
				switch mutation {
				case "bad-domain":
					e["domain_code"] = json.RawMessage(`"PRIVATE-PATH"`)
				case "bad-phase":
					e["native_phase"] = json.RawMessage(`"SELECT PRIVATE"`)
				case "bad-category":
					e["native_category"] = json.RawMessage(`"PRIVATE-STATUS-ERROR"`)
				case "negative-code":
					e["native_code"] = json.RawMessage("-1")
				case "no-native-with-code":
					e["native_phase"] = json.RawMessage(`"none"`)
					e["native_category"] = json.RawMessage(`"none"`)
				}
				fields["first_error"], _ = json.Marshal(e)
			case "wrong-first":
				fields["first_failed_phase"] = json.RawMessage(`"rows"`)
			case "lost-cleanup":
				fields["cleanup_failed"] = json.RawMessage("false")
			case "absent-deadline-without-expiry":
				fields["operation_expired_at_return"] = json.RawMessage("null")
			case "bad-context":
				fields["operation_at_return"] = json.RawMessage(`"PRIVATE-PATH"`)
			case "unpaired-phase":
				var a []int64
				_ = json.Unmarshal(fields["phase_us"], &a)
				a[0] = -1
				fields["phase_us"], _ = json.Marshal(a)
			}
			raw, _ := json.Marshal(fields)
			record := d.snapshot()
			if json.Unmarshal(raw, &record) != nil {
				t.Fatal("wire control construction")
			}
			d.record = record
			line := sqliteStatusFailureDiagnosticLog(d, true)
			if mutation == "maximum-valid" {
				if strings.HasSuffix(line, "unavailable") || len(line) > 4096 {
					t.Fatal("maximum valid wire exceeds bound")
				}
			} else if !strings.HasSuffix(line, "unavailable") {
				t.Fatal("malformed diagnostic emitted trusted record")
			}
			if strings.Contains(line, "PRIVATE") {
				t.Fatal("private malformed field escaped formatter")
			}
		})
	}
}

func TestSQLiteCaptureFixtureFailureDiagnosticWitness(t *testing.T) {
	for _, tc := range []struct {
		name      string
		elapsed   time.Duration
		err       error
		budget    bool
		available bool
	}{
		{"native-canceled", time.Millisecond, &sqliteio.Error{Phase: sqliteio.OpenPhase, Category: sqliteio.Canceled, Code: 9, Cause: sodQAPrivateError{}}, true, true},
		{"disposition-only", 0, nil, true, true},
		{"private-error", time.Millisecond, sodQAPrivateError{}, false, true},
		{"negative-duration", -time.Nanosecond, nil, true, false},
		{"saturated-duration", time.Duration(1<<63 - 1), nil, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line := sqliteCaptureFixtureFailureLog(tc.elapsed, tc.err, tc.budget)
			if len(line) > 4096 || strings.Contains(line, "PRIVATE") {
				t.Fatal("unsafe capture witness")
			}
			if !tc.available {
				if !strings.HasSuffix(line, "unavailable") {
					t.Fatal("invalid elapsed time emitted trusted record")
				}
				return
			}
			const prefix = "tempo capture fixture failure v1: "
			if !strings.HasPrefix(line, prefix) || strings.HasSuffix(line, "unavailable") {
				t.Fatal("missing finite capture witness")
			}
			var fields map[string]json.RawMessage
			if json.Unmarshal([]byte(strings.TrimPrefix(line, prefix)), &fields) != nil || len(fields) != 6 {
				t.Fatal("capture wire key count")
			}
			var r struct {
				Version  int        `json:"schema_version"`
				Source   string     `json:"source"`
				Elapsed  int64      `json:"elapsed_us"`
				Budget   bool       `json:"default_budget"`
				Error    bool       `json:"error_present"`
				Evidence sodQAError `json:"error"`
			}
			dc := json.NewDecoder(strings.NewReader(strings.TrimPrefix(line, prefix)))
			dc.DisallowUnknownFields()
			if dc.Decode(&r) != nil || r.Version != 1 || r.Source != "public_capture_fixture" || r.Elapsed != tc.elapsed.Microseconds() || r.Budget != tc.budget || r.Error != (tc.err != nil) {
				t.Fatal("capture witness changed original scalar facts")
			}
			if tc.name == "native-canceled" && (r.Evidence.Phase != "open" || r.Evidence.Category != "canceled" || r.Evidence.Code != 9) {
				t.Fatal("capture cancellation misclassified")
			}
		})
	}
}
func sodQAContains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

// New private observation controls; missing APIs on old source are not RED.
func TestSQLiteStatusFailureDiagnosticsService(t *testing.T) {
	cases := []struct{ name, code, first string }{
		{"success", "", "none"}, {"wide-acquisition-success", "", "none"}, {"invalid-budget", "validation", "none"},
		{"caller-canceled", "state_busy", "inspect"}, {"caller-expired", "state_busy", "inspect"},
		{"held-root-short", "state_busy", "inspect"}, {"held-root-default", "state_busy", "inspect"},
		{"inspection-cancel", "state_busy", "inspect"}, {"begin-cancel", "state_busy", "begin"},
		{"schema-error", "state_corrupt", "schema"}, {"cleanup-error", "state_corrupt", "checked_cleanup"},
		{"primary-cleanup", "state_corrupt", "schema"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := sodQALinked(t)
			before := sodQARead(t, q.f)
			var off ActivitySnapshot
			for _, enabled := range []bool{false, true} {
				t.Run(map[bool]string{false: "off", true: "on"}[enabled], func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if tc.name == "caller-canceled" {
						cancel()
					}
					if tc.name == "caller-expired" {
						var stop context.CancelFunc
						ctx, stop = context.WithDeadline(ctx, time.Now().Add(-time.Second))
						defer stop()
					}
					q.service.store.timeout = 0
					if tc.name == "wide-acquisition-success" {
						q.service.store.timeout = time.Second
					}
					if tc.name == "invalid-budget" {
						q.service.store.timeout = -time.Nanosecond
					}
					if tc.name == "held-root-short" {
						q.service.store.timeout = 25 * time.Millisecond
					}
					var held *stQAOwner
					if strings.HasPrefix(tc.name, "held-root-") {
						held = mwQAOpen(t, q.f, sqliteio.Write)
						defer held.cleanup()
					}
					originalDeadline, hasDeadline := ctx.Deadline()
					var d *sqliteStatusFailureDiagnostics
					if enabled {
						ctx, d = withSQLiteStatusFailureDiagnostics(ctx)
						got, ok := ctx.Deadline()
						if ok != hasDeadline || !got.Equal(originalDeadline) {
							t.Fatal("diagnostic changed caller deadline")
						}
					}
					faults := &sodQAFaults{name: tc.name, cancel: cancel, primary: errors.New("PRIVATE-STATUS-ERROR"), cleanup: errors.New("PRIVATE-CLEANUP-ERROR")}
					faults.install(t)
					clockCalls, workerCalls := 0, 0
					q.service.clock = ClockFunc(func() (ClockSample, error) {
						clockCalls++
						if faults.roots.Load() != faults.releases.Load() {
							t.Fatal("clock called while native owner retained")
						}
						return stQAAt(4), nil
					})
					q.service.observeWorker = func(operation context.Context, w WorkerStatus) WorkerStatus {
						workerCalls++
						if deadline, ok := operation.Deadline(); !ok || time.Until(deadline) > 250*time.Millisecond {
							t.Fatal("Status callback lost fixed operation cap")
						}
						if faults.roots.Load() != faults.releases.Load() || faults.closes.Load() == 0 {
							t.Fatal("worker called before checked native release")
						}
						o := mwQAOpen(t, q.f, sqliteio.Write)
						defer o.cleanup()
						stQAClose(t, o, false)
						w.State = "running"
						w.QueuedCount = 99
						return w
					}
					result, err := q.service.Status(ctx)
					sodQAClear()
					faults.check(t, err)
					q.service.observeWorker = nil
					q.service.store.timeout = 0
					if held != nil {
						stQAClose(t, held, false)
					}
					code := ""
					if err != nil {
						var domain *Error
						if !errors.As(err, &domain) {
							t.Fatal("lost domain error")
						}
						code = domain.Code
					}
					if code != tc.code {
						t.Fatal("changed public classification", code)
					}
					if err != nil && !reflect.DeepEqual(result, ActivitySnapshot{}) {
						t.Fatal("failed read acknowledged partial snapshot")
					}
					if tc.code == "" {
						if clockCalls != 1 || workerCalls != 1 || result.ComputerID == nil || *result.ComputerID != before.meta.ComputerID || result.SnapshotRevision != before.meta.Revision || result.Worker.State != "running" || result.Worker.QueuedCount != 0 {
							t.Fatal("success projection or callbacks changed")
						}
					} else if clockCalls != 0 || workerCalls != 0 {
						t.Fatal("failed read invoked projection callbacks")
					}
					if !enabled {
						off = result
					} else if !reflect.DeepEqual(off, result) {
						t.Fatal("diagnostic changed public result")
					}
					sodQAUnchanged(t, q.f, before)
					if !enabled {
						return
					}
					r := sodQARecordRead(t, d)
					if r.First != tc.first || r.Error != (err != nil) || r.Cleanup != (tc.name == "cleanup-error" || tc.name == "primary-cleanup") {
						t.Fatal("incorrect failure attribution")
					}
					last := 5
					switch tc.name {
					case "invalid-budget":
						last = -1
					case "caller-canceled", "caller-expired", "held-root-short", "held-root-default", "inspection-cancel":
						last = 0
					case "begin-cancel":
						last = 1
					case "schema-error", "primary-cleanup":
						last = 2
					case "cleanup-error":
						last = 4
					}
					for i := 0; i < 6; i++ {
						reached := i <= last
						if i == 4 && last >= 0 {
							reached = true
						}
						if reached != (r.Phase[2*i] >= 0) || reached != (r.Phase[2*i+1] >= 0) {
							t.Fatal("phase reach disagrees with existing native boundary", sodQAPhases[i])
						}
					}
					if tc.name != "invalid-budget" && (r.OperationExpired == nil || r.AcquisitionExpired == nil || r.Acquisition > r.Operation) {
						t.Fatal("clipped acquisition or operation deadline missing")
					}
					if tc.name == "wide-acquisition-success" && r.Acquisition != r.Operation {
						t.Fatal("one-second acquisition escaped fixed Status operation cap")
					}
					if tc.name == "caller-expired" && (r.Caller != "deadline" || r.Context != "deadline" || r.Operation != r.Acquisition || r.OperationExpired == nil || !*r.OperationExpired || r.AcquisitionExpired == nil || !*r.AcquisitionExpired) {
						t.Fatal("earlier caller deadline was refreshed or lost")
					}
					if tc.name == "held-root-short" && (r.Operation-r.Acquisition < 150000 || r.Primary.Phase != "admission" || r.Primary.Category != "busy" || r.Primary.Code != 0) {
						t.Fatal("short acquisition contention confused with native cancellation")
					}
					if tc.name == "held-root-default" && (r.Primary.Phase != "admission" || !sodQAContains([]string{"busy", "canceled"}, r.Primary.Category) || r.Primary.Code != 0) {
						t.Fatal("default contention lacks admission provenance")
					}
					if tc.name == "inspection-cancel" && (r.Primary.Phase != "open" || r.Primary.Category != "canceled" || r.Context != "canceled" || r.Caller != "canceled" || !errors.Is(err, context.Canceled)) {
						t.Fatal("native inspection cancellation confused with root contention")
					}
					if tc.name == "primary-cleanup" && (r.Primary.Phase != "prepare" || r.CleanupError.Phase != "close") {
						t.Fatal("cleanup erased original native failure")
					}
					if tc.code == "" && (r.Context != "live" || r.Caller != "live") {
						t.Fatal("own defer cancel contaminated successful return")
					}
					line := sqliteStatusFailureDiagnosticLog(d, err != nil)
					if err == nil && line != "" || err != nil && (line == "" || len(line) > 4096 || strings.Contains(line, "PRIVATE")) {
						t.Fatal("failure-only bounded safe emission")
					}
				})
			}
		})
	}
}

func TestSQLiteStatusFailureDiagnosticsAbsent(t *testing.T) {
	f := interopLocation(t)
	s := NewSQLite(Options{Path: filepath.Join(f.directory, f.authority), Clock: ClockFunc(func() (ClockSample, error) { return stQAAt(4), nil })})
	var off ActivitySnapshot
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "off", true: "on"}[enabled], func(t *testing.T) {
			ctx := context.Background()
			var d *sqliteStatusFailureDiagnostics
			if enabled {
				ctx, d = withSQLiteStatusFailureDiagnostics(ctx)
			}
			got, err := s.Status(ctx)
			if err != nil || got.ContractVersion != 1 || got.ComputerID != nil || got.SnapshotRevision != "0" || got.Projects == nil || got.Actors == nil || got.ClosedIntervals == nil {
				t.Fatal("absent public snapshot changed")
			}
			for _, name := range []string{f.authority, f.database, f.database + "-wal", f.database + "-shm"} {
				if _, err := os.Lstat(filepath.Join(f.directory, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("absent status created or adopted state")
				}
			}
			if !enabled {
				off = got
				return
			}
			if !reflect.DeepEqual(off, got) {
				t.Fatal("observer changed absent result")
			}
			r := sodQARecordRead(t, d)
			if r.First != "none" || r.Error || r.Phase[0] < 0 || r.Phase[8] < 0 || r.Phase[10] < 0 || r.Phase[2] != -1 || r.Phase[4] != -1 || r.Phase[6] != -1 {
				t.Fatal("absent branch invented native transaction or omitted cleanup")
			}
		})
	}
}
