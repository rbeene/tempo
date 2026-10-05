//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"time"
)

// A single synchronous test-owned call may opt in. The nil path performs no
// clock read, allocation, callback or owner operation. This is not a timeout.
const sqliteSyncFailureDiagnosticMax = int64(9007199254740991)

type sqliteSyncFailureDiagnosticKey struct{}

type sqliteSyncFailureDiagnostic struct {
	SchemaVersion                     int       `json:"schema_version"`
	Source                            string    `json:"source"`
	PhaseUS                           [24]int64 `json:"phase_us"`
	PhaseFailed                       [12]bool  `json:"phase_failed"`
	FirstFailedPhase                  string    `json:"first_failed_phase"`
	GuardCleanupFailed                bool      `json:"guard_cleanup_failed"`
	GuardCloseCalls                   int       `json:"guard_close_calls"`
	InitialAdmissionDeadlineUS        int64     `json:"initial_admission_deadline_us"`
	CompletionAdmissionDeadlineUS     int64     `json:"completion_admission_deadline_us"`
	InitialDeadlineExpiredAtReturn    *bool     `json:"initial_deadline_expired_at_return"`
	CompletionDeadlineExpiredAtReturn *bool     `json:"completion_deadline_expired_at_return"`
	CallerAtReturn                    string    `json:"caller_at_return"`
	ResultState                       string    `json:"result_state"`
	AttemptedCount                    int64     `json:"attempted_count"`
	RemainingCount                    int64     `json:"remaining_count"`
	FinalErrorPresent                 bool      `json:"final_error_present"`
	Dropped                           bool      `json:"dropped"`
}

type sqliteSyncFailureDiagnostics struct {
	origin        time.Time
	initial       time.Time
	completion    time.Time
	initialSet    bool
	completionSet bool
	finished      bool
	record        sqliteSyncFailureDiagnostic
}

func withSQLiteSyncFailureDiagnostics(ctx context.Context) (context.Context, *sqliteSyncFailureDiagnostics) {
	d := &sqliteSyncFailureDiagnostics{origin: time.Now()}
	d.record = sqliteSyncFailureDiagnostic{
		SchemaVersion: 1, Source: "sync_now_private_phase", FirstFailedPhase: "none",
		InitialAdmissionDeadlineUS: -1, CompletionAdmissionDeadlineUS: -1,
		CallerAtReturn: "live", ResultState: "none", AttemptedCount: -1, RemainingCount: -1,
	}
	for i := range d.record.PhaseUS {
		d.record.PhaseUS[i] = -1
	}
	return context.WithValue(ctx, sqliteSyncFailureDiagnosticKey{}, d), d
}

func sqliteSyncFailureDiagnosticsFromContext(ctx context.Context) *sqliteSyncFailureDiagnostics {
	d, _ := ctx.Value(sqliteSyncFailureDiagnosticKey{}).(*sqliteSyncFailureDiagnostics)
	return d
}

func sqliteSyncFailurePhaseName(phase int) string {
	switch phase {
	case 0:
		return "admission"
	case 1:
		return "observe"
	case 2:
		return "terminal_replay"
	case 3:
		return "guard_acquire"
	case 4:
		return "guard_verify_initial"
	case 5:
		return "wal_maintenance"
	case 6:
		return "reserve"
	case 7:
		return "submit_roots"
	case 8:
		return "before_complete_barrier"
	case 9:
		return "guard_verify_final"
	case 10:
		return "complete"
	case 11:
		return "guard_cleanup"
	default:
		return "none"
	}
}

func (d *sqliteSyncFailureDiagnostics) begin(phase int) {
	if d == nil {
		return
	}
	if d.finished || phase < 0 || phase >= 12 || d.record.PhaseUS[2*phase] != -1 {
		d.record.Dropped = true
		return
	}
	elapsed := time.Since(d.origin)
	value := elapsed.Microseconds()
	if elapsed == time.Duration(1<<63-1) || value < 0 || value > sqliteSyncFailureDiagnosticMax {
		d.record.Dropped = true
		return
	}
	d.record.PhaseUS[2*phase] = value
}

func (d *sqliteSyncFailureDiagnostics) end(phase int, failed bool) {
	if d == nil {
		return
	}
	if d.finished || phase < 0 || phase >= 12 || d.record.PhaseUS[2*phase] == -1 || d.record.PhaseUS[2*phase+1] != -1 {
		d.record.Dropped = true
		return
	}
	elapsed := time.Since(d.origin)
	value := elapsed.Microseconds()
	if elapsed == time.Duration(1<<63-1) || value < d.record.PhaseUS[2*phase] || value > sqliteSyncFailureDiagnosticMax {
		d.record.Dropped = true
		return
	}
	d.record.PhaseUS[2*phase+1] = value
	d.record.PhaseFailed[phase] = failed
	if failed {
		if d.record.FirstFailedPhase == "none" {
			d.record.FirstFailedPhase = sqliteSyncFailurePhaseName(phase)
		}
		if phase == 11 {
			d.record.GuardCleanupFailed = true
		}
	}
}

func (d *sqliteSyncFailureDiagnostics) initialDeadline(value time.Time) {
	if d == nil {
		return
	}
	if d.finished || d.initialSet || value.IsZero() {
		d.record.Dropped = true
		return
	}
	d.initialSet = true
	elapsed := value.Sub(d.origin)
	offset := elapsed.Microseconds()
	if elapsed == time.Duration(1<<63-1) || elapsed == time.Duration(-1<<63) || offset < -sqliteSyncFailureDiagnosticMax || offset > sqliteSyncFailureDiagnosticMax {
		d.record.Dropped = true
		return
	}
	d.initial = value
	d.record.InitialAdmissionDeadlineUS = offset
}

func (d *sqliteSyncFailureDiagnostics) completionDeadline(value time.Time) {
	if d == nil {
		return
	}
	if d.finished || d.completionSet || value.IsZero() {
		d.record.Dropped = true
		return
	}
	d.completionSet = true
	elapsed := value.Sub(d.origin)
	offset := elapsed.Microseconds()
	if elapsed == time.Duration(1<<63-1) || elapsed == time.Duration(-1<<63) || offset < -sqliteSyncFailureDiagnosticMax || offset > sqliteSyncFailureDiagnosticMax {
		d.record.Dropped = true
		return
	}
	d.completion = value
	d.record.CompletionAdmissionDeadlineUS = offset
}

func (d *sqliteSyncFailureDiagnostics) guardClose() {
	if d == nil {
		return
	}
	if d.finished || d.record.GuardCloseCalls >= 2 {
		d.record.Dropped = true
		return
	}
	d.record.GuardCloseCalls++
}

func (d *sqliteSyncFailureDiagnostics) finish(ctx context.Context, result SyncRun, failed bool) {
	if d == nil {
		return
	}
	if d.finished {
		d.record.Dropped = true
		return
	}
	d.finished = true
	d.record.FinalErrorPresent = failed
	now := time.Now()
	if !d.initial.IsZero() {
		expired := !now.Before(d.initial)
		d.record.InitialDeadlineExpiredAtReturn = &expired
	}
	if !d.completion.IsZero() {
		expired := !now.Before(d.completion)
		d.record.CompletionDeadlineExpiredAtReturn = &expired
	}
	if ctx == nil {
		d.record.Dropped = true
	} else {
		switch ctx.Err() {
		case nil:
		case context.Canceled:
			d.record.CallerAtReturn = "canceled"
		case context.DeadlineExceeded:
			d.record.CallerAtReturn = "deadline"
		default:
			d.record.Dropped = true
		}
	}
	switch result.State {
	case "":
	case "complete", "interrupted":
		d.record.ResultState = result.State
	default:
		d.record.ResultState = "other"
	}
	if result.AttemptedIDs != nil {
		count := int64(len(result.AttemptedIDs))
		if count > 100 {
			d.record.Dropped = true
		} else {
			d.record.AttemptedCount = count
		}
	}
	if result.State != "" {
		count := int64(result.RemainingCount)
		if count < 0 || count > sqliteSyncFailureDiagnosticMax {
			d.record.Dropped = true
		} else {
			d.record.RemainingCount = count
		}
	}
}

func (d *sqliteSyncFailureDiagnostics) snapshot() sqliteSyncFailureDiagnostic {
	if d == nil {
		return sqliteSyncFailureDiagnostic{Dropped: true}
	}
	result := d.record
	if result.InitialDeadlineExpiredAtReturn != nil {
		value := *result.InitialDeadlineExpiredAtReturn
		result.InitialDeadlineExpiredAtReturn = &value
	}
	if result.CompletionDeadlineExpiredAtReturn != nil {
		value := *result.CompletionDeadlineExpiredAtReturn
		result.CompletionDeadlineExpiredAtReturn = &value
	}
	return result
}
