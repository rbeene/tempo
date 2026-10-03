//go:build (darwin || linux) && (amd64 || arm64) && !race

package activity

import "time"

// Ordinary functional flows exercise the unchanged production default: a
// zero constructor option selects the 250 ms admission budget.
func sqliteFlowTestLockTimeout() time.Duration { return 0 }
