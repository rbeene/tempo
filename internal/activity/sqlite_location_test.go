package activity

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Source-only QA against the authorized new naming helper. This does not open
// activity state, install a marker, or initialize either legacy or SQLite data.
func TestSQLiteLocationExactBasenameHashAndLongNames(t *testing.T) {
	parent := "/synthetic/tempo"
	seen := map[string]string{}
	for _, base := range []string{"activity-state.json", "Activity-state.json", " state.json ", "state.json-wal", ".state", "雪🙂.json", strings.Repeat("x", 250)} {
		path := filepath.Join(parent, base)
		dir, authority, database, err := sqliteLocation(path)
		if err != nil {
			t.Fatalf("legal authority basename %q: %v", base, err)
		}
		want := fmt.Sprintf("activity-%x.sqlite3", sha256.Sum256([]byte("tempo-sqlite-v1\x00"+base)))
		if dir != parent || authority != base || database != want {
			t.Fatalf("naming dir=%q authority=%q database=%q want=%q", dir, authority, database, want)
		}
		if len(database)+len("-journal") > 255 {
			t.Fatalf("derived sidecar exceeds legal leaf size: %d", len(database))
		}
		if prior, exists := seen[database]; exists {
			t.Fatalf("distinct exact basenames conflated: %q and %q", prior, base)
		}
		seen[database] = base
		otherDir, otherAuthority, otherDatabase, err := sqliteLocation(filepath.Join("/other/synthetic/tempo", base))
		if err != nil || otherDir != "/other/synthetic/tempo" || otherAuthority != base || otherDatabase != database {
			t.Fatal("derivation depends on parent path instead of exact authority basename")
		}
	}
}

func TestSQLiteLocationPreservesExistingCanonicalPathRulesWithoutIO(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(base, "does-not-exist")
	path := filepath.Join(parent, "state.json")
	dir, authority, _, err := sqliteLocation(path)
	if err != nil || dir != parent || authority != "state.json" {
		t.Fatalf("pure location result dir=%q authority=%q error=%v", dir, authority, err)
	}
	if _, err = os.Lstat(parent); !os.IsNotExist(err) {
		t.Fatal("naming helper created authority storage")
	}
	for _, path := range []string{"relative.json", "/synthetic/tempo/../state.json", "/synthetic//state.json", "/synthetic/state.json/"} {
		if _, _, _, err := sqliteLocation(path); err == nil {
			t.Fatalf("legacy noncanonical path admitted %q", path)
		}
	}
	if runtime.GOOS == "darwin" {
		for _, alias := range []string{"/tmp/tempo/state.json", "/var/tempo/state.json"} {
			dir, authority, _, err := sqliteLocation(alias)
			if err != nil || dir != "/private"+filepath.Dir(alias) || authority != "state.json" {
				t.Fatalf("Darwin legacy alias rule changed dir=%q error=%v", dir, err)
			}
		}
	}
}
