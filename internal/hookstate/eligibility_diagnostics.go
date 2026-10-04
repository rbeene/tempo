package hookstate

import (
	"context"
	"time"
)

// EligibilityDiagnostic contains only fixed numeric observations. P is the
// seven policy phases; R is six roles by nine counters; C is process user/system
// CPU microseconds. Unreached phases and unavailable CPU use -1.
type EligibilityDiagnostic struct {
	P [7]int64    `json:"p"`
	R [6][9]int64 `json:"r"`
	C [2]int64    `json:"c"`
	D bool        `json:"d"`
}

// EligibilityDiagnostics is owned by one synchronous, opt-in capture attempt.
// No event, profile, artifact path, digest, error, callback or native owner is kept.
type EligibilityDiagnostics struct {
	value           EligibilityDiagnostic
	started, active bool
	start           time.Time
	cpu             [2]int64
	cpuOK           bool
}
type eligibilityDiagnosticKey struct{}

func WithEligibilityDiagnostics(ctx context.Context) (context.Context, *EligibilityDiagnostics) {
	d := &EligibilityDiagnostics{}
	for i := range d.value.P {
		d.value.P[i] = -1
	}
	d.value.C = [2]int64{-1, -1}
	return context.WithValue(ctx, eligibilityDiagnosticKey{}, d), d
}

func (d *EligibilityDiagnostics) Snapshot() EligibilityDiagnostic {
	if d == nil {
		return EligibilityDiagnostic{D: true}
	}
	v := d.value
	for i, n := range v.P {
		if n < -1 || n > 120000000000 {
			v.D = true
		}
		if n >= 0 {
			v.P[i] = n / 1000
		}
	}
	for i := range v.R {
		for j, n := range v.R[i] {
			if n < 0 || j < 4 && n > 1<<40 || j >= 4 && n > 120000000000 {
				v.D = true
			}
			if j >= 4 {
				v.R[i][j] = n / 1000
			}
		}
	}
	return v
}

func beginEligibilityDiagnostic(ctx context.Context) *EligibilityDiagnostics {
	d, _ := ctx.Value(eligibilityDiagnosticKey{}).(*EligibilityDiagnostics)
	if d == nil {
		return nil
	}
	if d.started {
		d.value.D = true
		return nil
	}
	d.started, d.active, d.start = true, true, time.Now()
	d.cpu, d.cpuOK = eligibilityProcessCPU()
	return d
}
func (d *EligibilityDiagnostics) now() time.Time {
	if d == nil {
		return time.Time{}
	}
	return time.Now()
}
func (d *EligibilityDiagnostics) phase(index int, start time.Time) {
	if d == nil {
		return
	}
	if index < 0 || index >= len(d.value.P) || start.IsZero() || d.value.P[index] != -1 {
		d.value.D = true
		return
	}
	d.value.P[index] = time.Since(start).Nanoseconds()
}
func (d *EligibilityDiagnostics) finish() {
	if d == nil {
		return
	}
	after, ok := eligibilityProcessCPU()
	d.value.C = eligibilityCPUDelta(d.cpu, after, d.cpuOK && ok)
	d.phase(6, d.start)
	d.active = false
}

func eligibilityCPUDelta(before, after [2]int64, valid bool) [2]int64 {
	unavailable := [2]int64{-1, -1}
	if !valid {
		return unavailable
	}
	var delta [2]int64
	for i := range delta {
		if before[i] < 0 || after[i] < before[i] {
			return unavailable
		}
		delta[i] = after[i] - before[i]
		if delta[i] > 120000000 {
			return unavailable
		}
	}
	return delta
}

type eligibilityArtifactDiagnostic struct {
	d       *EligibilityDiagnostics
	role    int
	start   time.Time
	prelude bool
}

func beginEligibilityArtifact(ctx context.Context, role string) eligibilityArtifactDiagnostic {
	d, _ := ctx.Value(eligibilityDiagnosticKey{}).(*EligibilityDiagnostics)
	if d == nil || !d.active {
		return eligibilityArtifactDiagnostic{}
	}
	index := -1
	switch role {
	case "runtime":
		index = 0
	case "executable":
		index = 1
	case "definitions":
		index = 2
	case "skill":
		index = 3
	case "configuration":
		index = 4
	case "repository":
		index = 5
	}
	if index < 0 {
		d.value.D = true
		return eligibilityArtifactDiagnostic{}
	}
	d.value.R[index][0]++
	if d.value.R[index][0] > maxProfileArtifacts {
		d.value.D = true
	}
	return eligibilityArtifactDiagnostic{d: d, role: index, start: time.Now()}
}
func (a *eligibilityArtifactDiagnostic) now() time.Time { return a.d.now() }
func (a *eligibilityArtifactDiagnostic) opened() {
	if a.d != nil {
		a.d.value.R[a.role][1]++
	}
}
func (a *eligibilityArtifactDiagnostic) preludeEnd() {
	if a.d == nil {
		return
	}
	a.d.value.R[a.role][4] += time.Since(a.start).Nanoseconds()
	a.prelude = true
}
func (a *eligibilityArtifactDiagnostic) read(start time.Time, n int) {
	if a.d == nil {
		return
	}
	a.d.value.R[a.role][2] += int64(n)
	a.d.value.R[a.role][3]++
	a.d.value.R[a.role][5] += time.Since(start).Nanoseconds()
}
func (a *eligibilityArtifactDiagnostic) duration(index int, start time.Time) {
	if a.d != nil {
		a.d.value.R[a.role][index] += time.Since(start).Nanoseconds()
	}
}
func (a *eligibilityArtifactDiagnostic) finish() {
	if a.d == nil {
		return
	}
	if !a.prelude {
		a.preludeEnd()
	}
	a.d.value.R[a.role][8] += time.Since(a.start).Nanoseconds()
}
