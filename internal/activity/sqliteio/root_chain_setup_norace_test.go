//go:build (darwin || linux) && (amd64 || arm64) && !race

package sqliteio

import "time"

// Ordinary positive setup retains the production acquisition allowance.
// Open and Read Begin consume one absolute deadline; structural subjects are unchanged.
func rcQAPositiveSetupBudget() time.Duration {
	return 250 * time.Millisecond
}
