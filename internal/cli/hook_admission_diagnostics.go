package cli

import (
	"encoding/json"

	"github.com/rbeene/tempo/internal/activity"
)

const hookAdmissionDiagnosticPrefix = "\ntempo hook diagnostics v3: "

func hookAdmissionDiagnosticContext(d *activity.HostCaptureDiagnostics) string {
	rows := d.Snapshot()
	if len(rows) == 0 || len(rows) > 3 {
		return ""
	}
	for _, row := range rows {
		if row.Eligibility == nil || row.Eligibility.D {
			return ""
		}
		if row.StartUS < 0 || row.EndUS < row.StartUS || row.EndUS > 120000000 || row.DeadlineUS < -1 || row.DeadlineUS > 120000000 || row.CallerDeadlineUS < -1 || row.CallerDeadlineUS > 120000000 {
			return ""
		}
		if !hookAdmissionPauseValid(row) {
			return ""
		}
		for _, stamp := range row.PhaseUS {
			if stamp < -1 || stamp > row.EndUS {
				return ""
			}
		}
	}
	b, err := json.Marshal(rows)
	if err != nil || len(b) > 3072 {
		return ""
	}
	return hookAdmissionDiagnosticPrefix + string(b)
}

// All timestamps are independently floored to microseconds. Adding the floored
// excluded duration to the original deadline can be one microsecond short.
func hookAdmissionPauseValid(row activity.HostCaptureDiagnosticAttempt) bool {
	if row.EffectiveDeadlineUS < -1 || row.EffectiveDeadlineUS > 120000000 || row.EligibilityExcludedUS < 0 || row.EligibilityExcludedUS > 120000000 {
		return false
	}
	begin, end := row.PhaseUS[4], row.PhaseUS[5]
	switch row.EligibilityOutcome {
	case "not_called":
		return begin == -1 && end == -1 && row.EffectiveDeadlineUS == row.DeadlineUS && row.EligibilityExcludedUS == 0
	case "error", "caller_stopped", "paused":
		if row.DeadlineUS < 0 || begin < row.StartUS || end < begin || end > row.EndUS {
			return false
		}
	default:
		return false
	}
	if row.EligibilityOutcome != "paused" {
		return row.EffectiveDeadlineUS == row.DeadlineUS && row.EligibilityExcludedUS == 0 &&
			(row.EligibilityOutcome != "caller_stopped" || row.Caller == "canceled" || row.Caller == "deadline")
	}
	if row.EffectiveDeadlineUS < row.DeadlineUS || row.EligibilityExcludedUS > row.EndUS-row.StartUS || end-begin < row.EligibilityExcludedUS || end-begin-row.EligibilityExcludedUS > 1 {
		return false
	}
	lower := row.DeadlineUS + row.EligibilityExcludedUS
	upper := lower + 1
	if row.CallerDeadlineUS >= 0 {
		lower = min(lower, row.CallerDeadlineUS)
		upper = min(upper, row.CallerDeadlineUS)
	}
	return lower <= row.EffectiveDeadlineUS && row.EffectiveDeadlineUS <= upper
}
