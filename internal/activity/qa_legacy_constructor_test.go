package activity

import "testing"

// qaLegacyNew is confined to historical JSON semantic/storage oracles. The
// production New constructor remains the SQLite default after activation.
func qaLegacyNew(o Options) *Service {
	s := New(o)
	s.store.sqliteOnly = false
	return s
}

// qaLegacyLinkService is for tests that inspect or fault the old JSON store.
// qaLinkService itself stays on the public default constructor.
func qaLegacyLinkService(t *testing.T) (*Service, string) {
	t.Helper()
	s, path := qaLinkService(t)
	s.store.sqliteOnly = false
	return s, path
}
