//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Private, synchronous, single-call observation only. A nil collector performs
// no clock read, allocation, callback or ownership operation.
const sqliteStatusFailureDiagnosticMax = int64(9007199254740991)

type sqliteStatusFailureDiagnosticKey struct{}

type sqliteFailureEvidence struct {
	DomainCode      string `json:"domain_code"`
	NativePhase     string `json:"native_phase"`
	NativeCategory  string `json:"native_category"`
	NativeCode      int32  `json:"native_code"`
	NativeCleanup   bool   `json:"native_cleanup"`
	RetainedCleanup bool   `json:"retained_cleanup"`
	Truncated       bool   `json:"truncated"`
}

func sqliteFailureDomain(code string) string {
	switch code {
	case "validation", "state_busy", "state_corrupt", "state_path_in_use", "local_write_unknown":
		return code
	default:
		return "other"
	}
}
func sqliteFailureNativePhase(phase sqliteio.Phase) string {
	switch phase {
	case sqliteio.Admission, sqliteio.OpenPhase, sqliteio.BeginPhase, sqliteio.PreparePhase,
		sqliteio.BindPhase, sqliteio.StepPhase, sqliteio.CommitPhase, sqliteio.VerifyPhase,
		sqliteio.RollbackPhase, sqliteio.FinalizePhase, sqliteio.ClosePhase, sqliteio.CheckpointPhase:
		return string(phase)
	default:
		return "other"
	}
}
func sqliteFailureNativeCategory(category sqliteio.Category) string {
	switch category {
	case sqliteio.Invalid, sqliteio.Busy, sqliteio.Canceled, sqliteio.Unsafe, sqliteio.Corrupt,
		sqliteio.Full, sqliteio.Constraint, sqliteio.IO, sqliteio.Closed, sqliteio.Misuse:
		return string(category)
	default:
		return "other"
	}
}

// Bound both visited and queued nodes. Native causes/cleanup are evidence, not
// a replacement primary classification. No error text or payload is inspected.
func sqliteFailureTypedEvidence(err error) sqliteFailureEvidence {
	r := sqliteFailureEvidence{DomainCode: "none", NativePhase: "none", NativeCategory: "none"}
	var pending [16]error
	pending[0] = err
	count, visited := 1, 0
	for count > 0 && visited < len(pending) {
		count--
		current := pending[count]
		pending[count] = nil
		visited++
		if current == nil {
			continue
		}
		switch e := current.(type) {
		case *sqliteio.Error:
			if e == nil {
				r.Truncated = true
				continue
			}
			if r.NativePhase == "none" {
				r.NativePhase, r.NativeCategory = sqliteFailureNativePhase(e.Phase), sqliteFailureNativeCategory(e.Category)
				if e.Code < 0 {
					r.Truncated = true
				} else {
					r.NativeCode = e.Code
				}
				r.NativeCleanup = e.Cleanup != nil
			}
			continue
		case *Error:
			if e == nil {
				r.Truncated = true
				continue
			}
			if r.DomainCode == "none" {
				r.DomainCode = sqliteFailureDomain(e.Code)
			}
			continue
		case *sqliteCaptureCleanupError:
			r.RetainedCleanup = r.RetainedCleanup || e != nil
			if e == nil {
				r.Truncated = true
			}
			continue
		}
		if joined, ok := current.(interface{ Unwrap() []error }); ok {
			children := joined.Unwrap()
			take := len(children)
			if take > len(pending)-count {
				take = len(pending) - count
				r.Truncated = true
			}
			for i := take - 1; i >= 0; i-- {
				pending[count] = children[i]
				count++
			}
		} else if wrapped, ok := current.(interface{ Unwrap() error }); ok {
			pending[count] = wrapped.Unwrap()
			count++
		}
	}
	if count != 0 {
		r.Truncated = true
	}
	return r
}

type sqliteStatusFailureDiagnostic struct {
	SchemaVersion              int                   `json:"schema_version"`
	Source                     string                `json:"source"`
	PhaseUS                    [12]int64             `json:"phase_us"`
	PhaseFailed                [6]bool               `json:"phase_failed"`
	FirstFailedPhase           string                `json:"first_failed_phase"`
	CleanupFailed              bool                  `json:"cleanup_failed"`
	OperationDeadlineUS        int64                 `json:"operation_deadline_us"`
	AcquisitionDeadlineUS      int64                 `json:"acquisition_deadline_us"`
	OperationExpiredAtReturn   *bool                 `json:"operation_expired_at_return"`
	AcquisitionExpiredAtReturn *bool                 `json:"acquisition_expired_at_return"`
	CallerAtReturn             string                `json:"caller_at_return"`
	OperationAtReturn          string                `json:"operation_at_return"`
	FinalErrorPresent          bool                  `json:"final_error_present"`
	FirstError                 sqliteFailureEvidence `json:"first_error"`
	CleanupError               sqliteFailureEvidence `json:"cleanup_error"`
	Dropped                    bool                  `json:"dropped"`
}
type sqliteStatusFailureDiagnostics struct {
	origin    time.Time
	deadlines [2]time.Time
	finished  bool
	record    sqliteStatusFailureDiagnostic
}

func withSQLiteStatusFailureDiagnostics(ctx context.Context) (context.Context, *sqliteStatusFailureDiagnostics) {
	d := &sqliteStatusFailureDiagnostics{origin: time.Now()}
	d.record = sqliteStatusFailureDiagnostic{SchemaVersion: 1, Source: "status_private_phase",
		FirstFailedPhase: "none", OperationDeadlineUS: -1, AcquisitionDeadlineUS: -1,
		CallerAtReturn: "live", OperationAtReturn: "live",
		FirstError: sqliteFailureTypedEvidence(nil), CleanupError: sqliteFailureTypedEvidence(nil)}
	for i := range d.record.PhaseUS {
		d.record.PhaseUS[i] = -1
	}
	return context.WithValue(ctx, sqliteStatusFailureDiagnosticKey{}, d), d
}
func sqliteStatusFailureDiagnosticsFromContext(ctx context.Context) *sqliteStatusFailureDiagnostics {
	d, _ := ctx.Value(sqliteStatusFailureDiagnosticKey{}).(*sqliteStatusFailureDiagnostics)
	return d
}
func sqliteStatusFailurePhaseName(phase int) string {
	switch phase {
	case 0:
		return "inspect"
	case 1:
		return "begin"
	case 2:
		return "schema"
	case 3:
		return "rows"
	case 4:
		return "checked_cleanup"
	case 5:
		return "projection"
	default:
		return "none"
	}
}
func (d *sqliteStatusFailureDiagnostics) begin(phase int) {
	if d == nil {
		return
	}
	if d.finished || phase < 0 || phase >= 6 || d.record.PhaseUS[2*phase] != -1 {
		d.record.Dropped = true
		return
	}
	elapsed := time.Since(d.origin)
	value := elapsed.Microseconds()
	if elapsed == time.Duration(1<<63-1) || value < 0 || value > sqliteStatusFailureDiagnosticMax {
		d.record.Dropped = true
		return
	}
	d.record.PhaseUS[2*phase] = value
}
func (d *sqliteStatusFailureDiagnostics) end(phase int, err error) {
	if d == nil {
		return
	}
	if d.finished || phase < 0 || phase >= 6 || d.record.PhaseUS[2*phase] == -1 || d.record.PhaseUS[2*phase+1] != -1 {
		d.record.Dropped = true
		return
	}
	elapsed := time.Since(d.origin)
	value := elapsed.Microseconds()
	if elapsed == time.Duration(1<<63-1) || value < d.record.PhaseUS[2*phase] || value > sqliteStatusFailureDiagnosticMax {
		d.record.Dropped = true
		return
	}
	d.record.PhaseUS[2*phase+1] = value
	d.record.PhaseFailed[phase] = err != nil
	if err != nil {
		evidence := sqliteFailureTypedEvidence(err)
		if d.record.FirstFailedPhase == "none" {
			d.record.FirstFailedPhase = sqliteStatusFailurePhaseName(phase)
			d.record.FirstError = evidence
		}
		if phase == 4 {
			d.record.CleanupFailed = true
			d.record.CleanupError = evidence
		}
	}
}

// Which 0 is the fixed operation cap; 1 is the already-clipped acquisition.
func (d *sqliteStatusFailureDiagnostics) deadline(which int, value time.Time) {
	if d == nil {
		return
	}
	if d.finished || which < 0 || which >= 2 || !d.deadlines[which].IsZero() || value.IsZero() {
		d.record.Dropped = true
		return
	}
	elapsed := value.Sub(d.origin)
	offset := elapsed.Microseconds()
	if elapsed == time.Duration(1<<63-1) || elapsed == time.Duration(-1<<63) || offset < -sqliteStatusFailureDiagnosticMax || offset > sqliteStatusFailureDiagnosticMax {
		d.record.Dropped = true
		return
	}
	d.deadlines[which] = value
	if which == 0 {
		d.record.OperationDeadlineUS = offset
	} else {
		d.record.AcquisitionDeadlineUS = offset
	}
}
func sqliteStatusFailureContext(ctx context.Context) string {
	if ctx == nil {
		return "other"
	}
	switch ctx.Err() {
	case nil:
		return "live"
	case context.Canceled:
		return "canceled"
	case context.DeadlineExceeded:
		return "deadline"
	default:
		return "other"
	}
}
func (d *sqliteStatusFailureDiagnostics) finish(caller, operation context.Context, err error) {
	if d == nil {
		return
	}
	if d.finished {
		d.record.Dropped = true
		return
	}
	d.finished = true
	d.record.FinalErrorPresent = err != nil
	d.record.CallerAtReturn = sqliteStatusFailureContext(caller)
	d.record.OperationAtReturn = sqliteStatusFailureContext(operation)
	if d.record.CallerAtReturn == "other" || d.record.OperationAtReturn == "other" {
		d.record.Dropped = true
	}
	if err != nil && d.record.FirstFailedPhase == "none" {
		d.record.FirstError = sqliteFailureTypedEvidence(err)
	}
	now := time.Now()
	if !d.deadlines[0].IsZero() {
		expired := !now.Before(d.deadlines[0])
		d.record.OperationExpiredAtReturn = &expired
	}
	if !d.deadlines[1].IsZero() {
		expired := !now.Before(d.deadlines[1])
		d.record.AcquisitionExpiredAtReturn = &expired
	}
}
func (d *sqliteStatusFailureDiagnostics) snapshot() sqliteStatusFailureDiagnostic {
	if d == nil {
		return sqliteStatusFailureDiagnostic{Dropped: true}
	}
	result := d.record
	if result.OperationExpiredAtReturn != nil {
		value := *result.OperationExpiredAtReturn
		result.OperationExpiredAtReturn = &value
	}
	if result.AcquisitionExpiredAtReturn != nil {
		value := *result.AcquisitionExpiredAtReturn
		result.AcquisitionExpiredAtReturn = &value
	}
	return result
}
