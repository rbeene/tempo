package activity

import (
	"context"
	"time"
)

// HostCaptureDiagnostics is an opt-in, per-hook numeric observation. It never
// invokes a callback, writes state, changes deadlines or retains event data.
// The CLI owns it across at most three synchronous capture attempts.
type HostCaptureDiagnostics struct {
	start time.Time
	count int
	rows  [3]HostCaptureDiagnosticAttempt
}

type HostCaptureDiagnosticAttempt struct {
	Ordinal          int      `json:"ordinal"`
	StartUS          int64    `json:"start_us"`
	EndUS            int64    `json:"end_us"`
	DeadlineUS       int64    `json:"deadline_us"`
	CallerDeadlineUS int64    `json:"caller_deadline_us"`
	PhaseUS          [8]int64 `json:"phase_us"`
	Caller           string   `json:"caller"`
	Retry            bool     `json:"retry"`
	NativePhase      string   `json:"native_phase"`
	NativeCategory   string   `json:"native_category"`
	NativeCode       int32    `json:"native_code"`
	NativeCleanup    bool     `json:"native_cleanup"`
}

type hostCaptureDiagnosticKey struct{}
type hostCaptureDiagnosticSlot struct {
	owner *HostCaptureDiagnostics
	index int
}

const (
	hostTraceInitialRead = iota
	hostTraceDiscoveryBegin
	hostTraceDiscoveryEnd
	hostTraceBindingRead
	hostTracePolicyBegin
	hostTracePolicyEnd
	hostTraceWriterBegin
	hostTraceWriterEnd
)

func NewHostCaptureDiagnostics() *HostCaptureDiagnostics {
	return &HostCaptureDiagnostics{start: time.Now()}
}

func (d *HostCaptureDiagnostics) Begin(ctx context.Context) context.Context {
	if d == nil || d.count == len(d.rows) {
		return ctx
	}
	i := d.count
	d.count++
	d.rows[i] = HostCaptureDiagnosticAttempt{Ordinal: i + 1, StartUS: time.Since(d.start).Microseconds(), DeadlineUS: -1, NativePhase: "none", NativeCategory: "none"}
	d.rows[i].CallerDeadlineUS = -1
	if deadline, ok := ctx.Deadline(); ok {
		d.rows[i].CallerDeadlineUS = deadline.Sub(d.start).Microseconds()
	}
	for p := range d.rows[i].PhaseUS {
		d.rows[i].PhaseUS[p] = -1
	}
	return context.WithValue(ctx, hostCaptureDiagnosticKey{}, hostCaptureDiagnosticSlot{d, i})
}

func (d *HostCaptureDiagnostics) End(ctx context.Context, retry bool) {
	slot, ok := ctx.Value(hostCaptureDiagnosticKey{}).(hostCaptureDiagnosticSlot)
	if !ok || slot.owner != d {
		return
	}
	r := &d.rows[slot.index]
	r.EndUS, r.Retry, r.Caller = time.Since(d.start).Microseconds(), retry, "live"
	switch ctx.Err() {
	case context.Canceled:
		r.Caller = "canceled"
	case context.DeadlineExceeded:
		r.Caller = "deadline"
	}
}

func (d *HostCaptureDiagnostics) Snapshot() []HostCaptureDiagnosticAttempt {
	if d == nil {
		return nil
	}
	return append([]HostCaptureDiagnosticAttempt(nil), d.rows[:d.count]...)
}

func hostCaptureTraceMark(ctx context.Context, phase int) {
	if slot, ok := ctx.Value(hostCaptureDiagnosticKey{}).(hostCaptureDiagnosticSlot); ok && phase >= 0 && phase < 8 {
		slot.owner.rows[slot.index].PhaseUS[phase] = time.Since(slot.owner.start).Microseconds()
	}
}

func hostCaptureTraceDeadline(ctx context.Context, deadline time.Time) {
	if slot, ok := ctx.Value(hostCaptureDiagnosticKey{}).(hostCaptureDiagnosticSlot); ok {
		slot.owner.rows[slot.index].DeadlineUS = deadline.Sub(slot.owner.start).Microseconds()
	}
}

func hostCaptureTraceNative(ctx context.Context, phase, category string, code int32, cleanup bool) {
	slot, ok := ctx.Value(hostCaptureDiagnosticKey{}).(hostCaptureDiagnosticSlot)
	if !ok {
		return
	}
	// The primary native node is selected by the existing checked classifier.
	// It is descriptive evidence, never retry authority or a whole-tree summary.
	switch phase {
	case "admission", "open", "begin", "prepare", "bind", "step", "commit", "verify", "rollback", "finalize", "close", "checkpoint":
	default:
		phase = "other"
	}
	switch category {
	case "invalid", "busy", "canceled", "unsafe", "corrupt", "full", "constraint", "io", "closed", "misuse":
	default:
		category = "other"
	}
	r := &slot.owner.rows[slot.index]
	r.NativePhase, r.NativeCategory, r.NativeCode, r.NativeCleanup = phase, category, code, cleanup
}
