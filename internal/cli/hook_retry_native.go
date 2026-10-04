//go:build (darwin || linux) && (amd64 || arm64)

package cli

import "github.com/rbeene/tempo/internal/activity/sqliteio"

// Report known native evidence without exposing it as another public API.
// Even Busy cannot authorize retry after a cleanup/commit/terminal failure.
func hookNativeRetryNode(err error) (known, safe, admissionCancellation bool) {
	if err == sqliteio.ErrBusy {
		return true, true, false
	}
	native, ok := err.(*sqliteio.Error)
	if !ok {
		return false, false, false
	}
	if native == nil || native.Cleanup != nil {
		return true, false, false
	}
	switch native.Phase {
	case sqliteio.Admission, sqliteio.OpenPhase, sqliteio.BeginPhase, sqliteio.VerifyPhase:
		if native.Category == sqliteio.Canceled {
			// A local admission can expire while the original hook context
			// remains live. Receipt and whole-tree checks still apply.
			return true, true, true
		}
	case sqliteio.PreparePhase, sqliteio.BindPhase, sqliteio.StepPhase:
	default:
		return true, false, false
	}
	return true, native.Category == sqliteio.Busy, false
}
