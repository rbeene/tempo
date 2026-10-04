package activity

// NewSQLite is an alias for the fresh SQLite operation service constructed by
// New. Construction is lazy; existing JSON state is never adopted or modified.
func NewSQLite(o Options) *Service {
	return New(o)
}
