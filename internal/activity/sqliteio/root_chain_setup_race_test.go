//go:build (darwin || linux) && (amd64 || arm64) && race

package sqliteio

import "time"

// Only root-chain positive setup gets this finite race-instrumentation allowance.
// Open and Read Begin consume one absolute deadline; structural subjects are unchanged.
func rcQAPositiveSetupBudget() time.Duration {
	return time.Second
}
