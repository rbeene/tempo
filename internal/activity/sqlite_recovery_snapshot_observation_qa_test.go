//go:build (darwin || linux) && (amd64 || arm64)

package activity

import "context"

// Only the later-epoch recovery test's final snapshot opts into this observer.
// The public Status call keeps its original Background caller and deadlines.
func qaRecoveryObservedSnapshot(h *qaHarness) ActivitySnapshot {
	h.t.Helper()
	ctx, diagnostic := withSQLiteStatusFailureDiagnostics(context.Background())
	s, err := h.service.Status(ctx)
	if err != nil {
		h.t.Log(sqliteStatusFailureDiagnosticLog(diagnostic, true))
		h.t.Fatalf("status: %v", err)
	}
	return s
}
