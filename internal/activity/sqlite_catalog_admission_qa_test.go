//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"reflect"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// The existing reader admits the real fixed catalog and metadata, then reports
// the exact native Step count. This must fail on the 85-row reader and pass only
// when the optimized admission retains the same positive result in <=4 Steps.
func TestSQLiteCatalogAdmissionNativeFixedCatalogBoundedSteps(t *testing.T) {
	canonical := flQAExpectedCatalog(t)
	if len(canonical) != 85 || len(sqliteCaptureCatalog) != 85 {
		t.Fatal("fresh native canonical catalog cardinality")
	}
	f := interopLocation(t)
	wantMeta := interopMeta(f)
	interopInitialize(t, f, wantMeta)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	steps := 0
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		if e.Operation == "statement" && e.Phase == "step-before-native" {
			steps++
		}
	}})
	gotMeta, err := sqliteReadCaptureSchema(tx, f.authority, f.database)
	spQASetSQLHooks(spQASQLHooks{})
	if err != nil || !reflect.DeepEqual(gotMeta, wantMeta) || steps == 0 || steps > 4 {
		t.Fatalf("fixed catalog admission metadata=%+v steps=%d error=%v", gotMeta, steps, err)
	}
	if got := flQACatalog(t, tx); !reflect.DeepEqual(got, canonical) {
		t.Fatal("fresh fixed catalog differs from independently installed native canonical DDL")
	}
	interopRollback(t, tx)
	interopClose(t, c)
}
