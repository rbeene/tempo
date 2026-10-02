//go:build (darwin && cgo) || linux

package activity

import (
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"testing"
)

func TestPlatformClockComparableAcrossProcesses(t *testing.T) {
	sample := func() ClockSample {
		cmd := exec.Command(os.Args[0], "-test.run=^TestPlatformClockProcessHelper$")
		cmd.Env = append(os.Environ(), "TEMPO_QA_CLOCK_HELPER=1")
		b, err := cmd.Output()
		if err != nil {
			t.Fatalf("clock helper: %v", err)
		}
		var got ClockSample
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("decode clock: %v", err)
		}
		if got.Capability != "available" || got.Epoch == nil || got.ElapsedNS == nil || got.AwakeNS == nil || got.WallUTC.IsZero() {
			t.Fatalf("clock evidence unavailable: %+v", got)
		}
		return got
	}
	a, b := sample(), sample()
	if *a.Epoch != *b.Epoch {
		t.Fatal("clock epoch changed across short-lived processes")
	}
	for _, pair := range [][2]*string{{a.ElapsedNS, b.ElapsedNS}, {a.AwakeNS, b.AwakeNS}} {
		av, e1 := strconv.ParseUint(*pair[0], 10, 64)
		bv, e2 := strconv.ParseUint(*pair[1], 10, 64)
		if e1 != nil || e2 != nil || bv < av {
			t.Fatal("cross-process counter regression")
		}
	}
}
func TestPlatformClockProcessHelper(t *testing.T) {
	if os.Getenv("TEMPO_QA_CLOCK_HELPER") != "1" {
		return
	}
	s, err := (nativeClock{}).Sample()
	if err != nil {
		os.Exit(2)
	}
	if err := json.NewEncoder(os.Stdout).Encode(s); err != nil {
		os.Exit(3)
	}
	os.Exit(0)
}
