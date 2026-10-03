//go:build (darwin || linux) && (amd64 || arm64) && race

package activity

import "time"

// The race detector instruments the generated SQLite engine. Only the two
// opted-in functional flows use this supported budget; production and
// ordinary/default-deadline acceptance are unchanged.
func sqliteFlowTestLockTimeout() time.Duration { return time.Second }
