//go:build !race

package cli_test

import "time"

// Ordinary functional flows exercise the unchanged production default: a
// zero constructor option selects the 250 ms admission budget.
func sqliteFlowTestLockTimeout() time.Duration { return 0 }
