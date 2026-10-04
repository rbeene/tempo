//go:build race

package cli_test

import "time"

// The race detector instruments the generated SQLite engine. Explicitly
// opted-in functional fixtures use this supported budget; production,
// ordinary/default-deadline acceptance, and caller deadlines are unchanged.
func sqliteFlowTestLockTimeout() time.Duration { return time.Second }
