package activity

import (
	_ "embed"
	"encoding/json"
)

//go:embed schema.json
var schemaJSON []byte

// Schema describes only the shipped local engine types, not planned operations.
func Schema() json.RawMessage { return append(json.RawMessage(nil), schemaJSON...) }
