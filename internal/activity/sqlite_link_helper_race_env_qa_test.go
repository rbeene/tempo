//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
)

// The real existing guard/writer child checks its received environment before
// READY. Successful release still has every original scanner/Wait/close check.
// With the unchanged launcher, this is a runtime environment-propagation RED,
// never an absent API, mock process, raised budget or suppressed race detector.
func TestSQLiteLinkOwnedHelperPreservesCallerRaceOptions(t *testing.T) {
	for _, options := range []struct{ name, inherited string }{
		{"empty", ""},
		{"configured", "exitcode=23 halt_on_error=1 history_size=3 atexit_sleep_ms=10000"},
	} {
		for _, mode := range []string{"guard", "writer"} {
			t.Run(options.name+"/"+mode, func(t *testing.T) {
				_, _, f, _ := flQABootstrap(t)
				t.Setenv("GORACE", options.inherited)
				want := options.inherited
				if want != "" {
					want += " "
				}
				want += "atexit_sleep_ms=0"
				t.Setenv("TEMPO_SQLITE_LINK_QA_EXPECT_GORACE", want)
				before, present := os.LookupEnv("GORACE")
				release := flQAOwnedHelper(t, f, mode)
				release()
				after, stillPresent := os.LookupEnv("GORACE")
				if after != before || stillPresent != present {
					t.Fatal("owned helper launcher changed caller GORACE")
				}
			})
		}
	}
}

// Inert in all normal suites. Root may select this ONE child explicitly from a
// race-built binary with the exact synthetic environment below. It deliberately
// races two accesses, joins both goroutines, then takes the normal Go exit path.
// os.Exit(0) reaches racefini before testing can translate the race to test-exit1;
// the detector must print the actual DATA RACE and terminate with configured66.
// This opt-in control owns no native connection, file, socket or descendant.
func TestSQLiteLinkJoinedRaceReportControlHelper(t *testing.T) {
	if os.Getenv("TEMPO_SQLITE_LINK_RACE_CONTROL") != "joined" {
		return
	}
	if os.Getenv("GORACE") != "exitcode=66 halt_on_error=0 history_size=2 atexit_sleep_ms=0" {
		t.Fatal("race-report control requires exact synthetic child options")
	}
	var value int
	start := make(chan struct{})
	var done sync.WaitGroup
	done.Add(2)
	for n := 1; n <= 2; n++ {
		go func(n int) {
			defer done.Done()
			<-start
			value = n
		}(n)
	}
	close(start)
	done.Wait()
	runtime.KeepAlive(value)
	if _, err := fmt.Fprintln(os.Stdout, "FL_QA_RACE_WRITERS_JOINED"); err != nil {
		t.Fatal("checked race-report control marker", err)
	}
	os.Exit(0)
}
