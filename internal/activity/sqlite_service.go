package activity

// NewSQLite constructs the fresh SQLite operation route for internal callers.
// Construction is lazy. Operations not yet ported refuse the legacy store;
// they cannot create a second authority or silently read old JSON state.
func NewSQLite(o Options) *Service {
	s := New(o)
	s.store.sqliteOnly = true
	return s
}
