// Package identity defines the shared numeric identifier boundary for CLI and config.
package identity

import "strconv"

// Valid accepts canonical, positive signed 64-bit decimal identifiers. Canonical
// representation is required because IDs also become exact JSON numbers.
func Valid(s string) bool {
	n, err := strconv.ParseInt(s, 10, 64)
	return err == nil && n > 0 && strconv.FormatInt(n, 10) == s
}
