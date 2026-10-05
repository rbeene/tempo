//go:build (!darwin && !linux) || (!amd64 && !arm64)

package activity

// Keep the generic recovery test buildable where the native observer is absent.
func qaRecoveryObservedSnapshot(h *qaHarness) ActivitySnapshot {
	h.t.Helper()
	return h.snapshot()
}
