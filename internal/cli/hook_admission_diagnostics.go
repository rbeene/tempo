package cli

import (
	"encoding/json"

	"github.com/rbeene/tempo/internal/activity"
)

const hookAdmissionDiagnosticPrefix = "\ntempo hook diagnostics v1: "

func hookAdmissionDiagnosticContext(d *activity.HostCaptureDiagnostics) string {
	rows := d.Snapshot()
	if len(rows) == 0 || len(rows) > 3 {
		return ""
	}
	for _, row := range rows {
		if row.StartUS < 0 || row.EndUS < row.StartUS || row.EndUS > 120000000 || row.DeadlineUS < -1 || row.DeadlineUS > 120000000 || row.CallerDeadlineUS < -1 || row.CallerDeadlineUS > 120000000 {
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
