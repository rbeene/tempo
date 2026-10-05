//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"encoding/json"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const sqliteStatusFailureDiagnosticPrefix = "tempo status failure diagnostics v1: "
const sqliteCaptureFixtureFailurePrefix = "tempo capture fixture failure v1: "

func sqliteFailureEvidenceValid(e sqliteFailureEvidence) bool {
	domain := e.DomainCode == "none" || e.DomainCode == "other" || sqliteFailureDomain(e.DomainCode) == e.DomainCode
	phase := e.NativePhase == "none" || e.NativePhase == "other" || sqliteFailureNativePhase(sqliteio.Phase(e.NativePhase)) == e.NativePhase
	category := e.NativeCategory == "none" || e.NativeCategory == "other" || sqliteFailureNativeCategory(sqliteio.Category(e.NativeCategory)) == e.NativeCategory
	return domain && phase && category && e.NativeCode >= 0 && ((e.NativePhase == "none") == (e.NativeCategory == "none")) && (e.NativePhase != "none" || e.NativeCode == 0 && !e.NativeCleanup)
}

// Failure branch only. This bounded formatter accepts no raw error or payload;
// the original fixture assertion remains the authority for success/failure.
func sqliteStatusFailureDiagnosticLog(d *sqliteStatusFailureDiagnostics, failed bool) string {
	if !failed {
		return ""
	}
	unavailable := sqliteStatusFailureDiagnosticPrefix + "unavailable"
	if d == nil || !d.finished {
		return unavailable
	}
	r := d.snapshot()
	if r.Dropped || r.SchemaVersion != 1 || r.Source != "status_private_phase" || !sqliteFailureEvidenceValid(r.FirstError) || !sqliteFailureEvidenceValid(r.CleanupError) {
		return unavailable
	}
	first := "none"
	for i := range r.PhaseFailed {
		start, end := r.PhaseUS[2*i], r.PhaseUS[2*i+1]
		if start < -1 || end < -1 || start > sqliteStatusFailureDiagnosticMax || end > sqliteStatusFailureDiagnosticMax || (start == -1) != (end == -1) || end < start || start == -1 && r.PhaseFailed[i] {
			return unavailable
		}
		if r.PhaseFailed[i] && first == "none" {
			first = sqliteStatusFailurePhaseName(i)
		}
	}
	if r.FirstFailedPhase != first || r.CleanupFailed != r.PhaseFailed[4] {
		return unavailable
	}
	for _, state := range []string{r.CallerAtReturn, r.OperationAtReturn} {
		if state != "live" && state != "canceled" && state != "deadline" {
			return unavailable
		}
	}
	for _, value := range []int64{r.OperationDeadlineUS, r.AcquisitionDeadlineUS} {
		if value < -sqliteStatusFailureDiagnosticMax || value > sqliteStatusFailureDiagnosticMax {
			return unavailable
		}
	}
	if r.OperationExpiredAtReturn == nil && r.OperationDeadlineUS != -1 || r.AcquisitionExpiredAtReturn == nil && r.AcquisitionDeadlineUS != -1 {
		return unavailable
	}
	b, err := json.Marshal(r)
	if err != nil || len(b)+len(sqliteStatusFailureDiagnosticPrefix) > 4096 {
		return unavailable
	}
	return sqliteStatusFailureDiagnosticPrefix + string(b)
}

// Called only inside mwQANew's unchanged capture failure guard. The duration
// spans that one original Ingest call, without a new timeout or second attempt.
func sqliteCaptureFixtureFailureLog(elapsed time.Duration, err error, defaultBudget bool) string {
	unavailable := sqliteCaptureFixtureFailurePrefix + "unavailable"
	us := elapsed.Microseconds()
	if elapsed < 0 || elapsed == time.Duration(1<<63-1) || us > sqliteStatusFailureDiagnosticMax {
		return unavailable
	}
	r := struct {
		SchemaVersion int                   `json:"schema_version"`
		Source        string                `json:"source"`
		ElapsedUS     int64                 `json:"elapsed_us"`
		DefaultBudget bool                  `json:"default_budget"`
		ErrorPresent  bool                  `json:"error_present"`
		Error         sqliteFailureEvidence `json:"error"`
	}{1, "public_capture_fixture", us, defaultBudget, err != nil, sqliteFailureTypedEvidence(err)}
	if !sqliteFailureEvidenceValid(r.Error) {
		return unavailable
	}
	b, marshalErr := json.Marshal(r)
	if marshalErr != nil || len(b)+len(sqliteCaptureFixtureFailurePrefix) > 4096 {
		return unavailable
	}
	return sqliteCaptureFixtureFailurePrefix + string(b)
}
