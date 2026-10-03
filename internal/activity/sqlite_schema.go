package activity

import _ "embed"

// This private physical schema version is distinct from stateVersion and the
// future version-2 authority marker. Embedding does not install or activate it.
const sqliteSchemaVersion = 1

//go:embed sqlite_schema.sql
var sqliteSchema string
