package activity

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
)

// sqliteLocation derives the database name from the stable state-path selector.
// Authority validation and database creation remain separate operations.
func sqliteLocation(path string) (directory, authorityBase, databaseBase string, err error) {
	resolved, err := (&fileStore{path: path}).location()
	if err != nil {
		return "", "", "", err
	}
	base := filepath.Base(resolved)
	if base == "." || base == "/" || strings.ContainsRune(resolved, 0) {
		return "", "", "", failure("validation")
	}
	digest := sha256.Sum256([]byte("tempo-sqlite-v1\x00" + base))
	return filepath.Dir(resolved), base, "activity-" + hex.EncodeToString(digest[:]) + ".sqlite3", nil
}
