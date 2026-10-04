//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Post-clean-OLD-RED supplement: v3 fields require the reviewed producer.
// These cases make no claim of compiling against the original producer.
func hapQACalibrate(t *testing.T, q *habQARun, e HostEvent, budget time.Duration) (int, sqliteHostPrepared) {
	t.Helper()
	before, rows := mwQAAudit(t, q.h.f)
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	d := NewHostCaptureDiagnostics()
	ctx = d.Begin(ctx)
	var releases []int64
	q.leases.Store(0)
	ncQASetFSHooks(ncQAFSHooks{Observe: func(ev ncQAFSEvent) {
		if ev.Role != "root" || ev.Op != "lease" {
			return
		}
		if ev.Phase == "acquired" {
			q.leases.Add(1)
		}
		if ev.Phase == "released" {
			q.leases.Add(-1)
			releases = append(releases, time.Since(d.start).Microseconds())
		}
	}})
	t.Cleanup(func() { ncQASetFSHooks(ncQAFSHooks{}) })
	deadline := time.Now().Add(budget)
	if caller, ok := ctx.Deadline(); ok && caller.Before(deadline) {
		deadline = caller
	}
	hostCaptureTraceDeadline(ctx, deadline)
	p, found, err := q.h.s.sqlitePrepareHost(ctx, e, sqliteCaptureAdmission{Directory: q.h.f.directory, StateBasename: q.h.f.authority, DatabaseBasename: q.h.f.database, AcquireDeadline: deadline})
	d.End(ctx, false)
	ncQASetFSHooks(ncQAFSHooks{})
	after, got := mwQAAudit(t, q.h.f)
	if err != nil || !found || q.leases.Load() != 0 || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(rows, got) {
		t.Fatal("SETUP actual preparation calibration/checked ownership/full32", err)
	}
	r := d.Snapshot()[0]
	if !p.Policy.CaptureEligible || p.Policy.Revision != "1" || r.Eligibility == nil || r.Eligibility.D || r.Eligibility.P[4] < 0 {
		t.Fatal("SETUP real valid policy fingerprint calibration")
	}
	last := 0
	for i, stamp := range releases {
		if stamp < r.PhaseUS[4] {
			last = i + 1
		}
	}
	if last == 0 {
		t.Fatal("SETUP no checked release before actual policy call")
	}
	return last, p
}

func hapQAInvoke(t *testing.T, q *habQARun, e HostEvent, releaseOrdinal int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), q.plan.caller)
	defer cancel()
	q.d = NewHostCaptureDiagnostics()
	ctx = q.d.Begin(ctx)
	q.leases.Store(0)
	q.acquired.Store(0)
	q.released.Store(0)
	armed := false
	ncQASetFSHooks(ncQAFSHooks{Observe: func(ev ncQAFSEvent) {
		if ev.Role != "root" || ev.Op != "lease" {
			return
		}
		if ev.Phase == "acquired" {
			q.acquired.Add(1)
			q.leases.Add(1)
		}
		if ev.Phase != "released" {
			return
		}
		n := q.released.Add(1)
		q.leases.Add(-1)
		if int(n) != releaseOrdinal {
			return
		}
		if armed || q.leases.Load() != 0 {
			t.Fatal("SETUP selected read did not release ownership")
		}
		armed = true
		row := q.d.Snapshot()[0]
		habQAWait(ctx, q.d.start.Add(time.Duration(row.StartUS)*time.Microsecond+q.plan.consume))
		if ctx.Err() != nil || !time.Now().Before(q.d.start.Add(time.Duration(row.DeadlineUS)*time.Microsecond)) {
			t.Fatal("SETUP pre-policy native work exhausted original admission")
		}
		lock := habQALockPolicy(t, q.h.policyPath)
		q.coordinate(t, lock, cancel)
	}})
	t.Cleanup(func() { ncQASetFSHooks(ncQAFSHooks{}) })
	q.r, q.err = q.h.s.IngestHost(ctx, e)
	q.ended, q.callerErr = time.Now(), ctx.Err()
	q.d.End(ctx, false)
	q.row = q.d.Snapshot()[0]
	if q.done != nil {
		<-q.done
	}
	ncQASetFSHooks(ncQAFSHooks{})
	if !armed || q.lockErr != nil || q.leases.Load() != 0 || q.acquired.Load() != q.released.Load() {
		t.Fatal("checked pre-policy/terminal native and lock ownership", q.lockErr)
	}
	// Failures still get the complete cold oracle before any outcome assertion.
	m, rows := mwQAAudit(t, q.h.f)
	if q.err != nil && q.r.Durability == "not_committed" && (!reflect.DeepEqual(m, q.before) || !reflect.DeepEqual(rows, q.rows)) {
		t.Error("failed post-policy admission changed full32 activity rows")
	}
	r, d := q.row, q.row.Eligibility
	if q.callerErr != nil || r.EligibilityOutcome != "paused" || r.PhaseUS[4] < 0 || r.PhaseUS[4] >= r.DeadlineUS || r.PhaseUS[5] <= r.DeadlineUS || d == nil || d.D || d.P[0] < (q.plan.policyHold-20*time.Millisecond).Microseconds() || d.P[4] < 0 {
		// Failure-only witness from already captured values; preserve the guard.
		metadataUS, fingerprintUS := int64(-1), int64(-1)
		notDropped := false
		if d != nil {
			metadataUS, fingerprintUS, notDropped = d.P[0], d.P[4], !d.D
		}
		t.Logf("postcore failed premise caller_live=%t caller_deadline=%t caller_canceled=%t caller_other=%t paused=%t policy_start_seen=%t policy_start_before_d0=%t policy_end_after_d0=%t eligibility_present=%t eligibility_not_dropped=%t metadata_wait_ok=%t fingerprint_seen=%t original_us=%d effective_us=%d caller_us=%d return_us=%d diagnostic_end_us=%d excluded_us=%d policy_begin_us=%d policy_end_us=%d writer_begin_us=%d writer_end_us=%d metadata_wait_us=%d metadata_min_us=%d fingerprint_us=%d error_present=%t committed=%t not_committed=%t unknown=%t actor_present=%t native_open=%t native_begin=%t native_code=%d native_cleanup=%t",
			q.callerErr == nil, q.callerErr == context.DeadlineExceeded, q.callerErr == context.Canceled, q.callerErr != nil && q.callerErr != context.DeadlineExceeded && q.callerErr != context.Canceled,
			r.EligibilityOutcome == "paused", r.PhaseUS[4] >= 0, r.PhaseUS[4] < r.DeadlineUS, r.PhaseUS[5] > r.DeadlineUS, d != nil, notDropped, d != nil && metadataUS >= (q.plan.policyHold-20*time.Millisecond).Microseconds(), d != nil && fingerprintUS >= 0,
			r.DeadlineUS, r.EffectiveDeadlineUS, r.CallerDeadlineUS, q.ended.Sub(q.d.start).Microseconds(), r.EndUS, r.EligibilityExcludedUS, r.PhaseUS[4], r.PhaseUS[5], r.PhaseUS[6], r.PhaseUS[7], metadataUS, (q.plan.policyHold - 20*time.Millisecond).Microseconds(), fingerprintUS,
			q.err != nil, q.r.Durability == "committed", q.r.Durability == "not_committed", q.r.Durability == "unknown", q.r.Actor != nil, r.NativePhase == "open", r.NativePhase == "begin", r.NativeCode, r.NativeCleanup)
		t.Fatal("SETUP actual long successful policy interval/remaining caller/v3 pause")
	}
	span := r.PhaseUS[5] - r.PhaseUS[4]
	if r.EligibilityExcludedUS > span || span-r.EligibilityExcludedUS > 1 {
		t.Error("v3 does not describe exact excluded production interval")
	}
	lower, upper := r.DeadlineUS+r.EligibilityExcludedUS, r.DeadlineUS+r.EligibilityExcludedUS+1
	if r.CallerDeadlineUS >= 0 {
		lower = min(lower, r.CallerDeadlineUS)
		upper = min(upper, r.CallerDeadlineUS)
	}
	if r.EffectiveDeadlineUS < lower || r.EffectiveDeadlineUS > upper || r.EffectiveDeadlineUS <= r.DeadlineUS {
		t.Error("effective deadline did not carry original remaining allowance")
	}
	for i, role := range []string{"runtime", "executable", "definitions"} {
		info, err := os.Stat(filepath.Join(q.h.cwd[0], role))
		if err != nil || d.R[i][0] != 1 || d.R[i][1] != 1 || d.R[i][2] != info.Size() || d.R[i][3] < 2 {
			t.Fatal("real full artifact rehash after earlier successful calibration", role, err)
		}
	}
	t.Logf("postcore release=%d original_us=%d effective_us=%d excluded_us=%d policy=%d/%d writer=%d/%d leases=%d/%d", releaseOrdinal, r.DeadlineUS, r.EffectiveDeadlineUS, r.EligibilityExcludedUS, r.PhaseUS[4], r.PhaseUS[5], r.PhaseUS[6], r.PhaseUS[7], q.acquired.Load(), q.released.Load())
}

func hapQAActor(t *testing.T, q *habQARun, ref *ActorRef) sqliteActorLocalRow {
	t.Helper()
	if ref == nil {
		t.Fatal("expected concrete actor identity")
	}
	o := mwQAOpen(t, q.h.f, sqliteio.Read)
	a, found, err := sqliteReadActorLocal(o.tx, q.before.ComputerID, ref.Key)
	cleanup := o.finish(false)
	o.reported = cleanup != nil
	if err != nil || cleanup != nil || !found || a.Ref != *ref {
		t.Fatal("cold exact actor/checked close", errors.Join(err, cleanup))
	}
	return a
}
func hapQAContains(old, current [][]string) bool {
	for _, row := range old {
		found := false
		for _, candidate := range current {
			if reflect.DeepEqual(row, candidate) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestSQLiteHostEligibilityAdmissionBranches(t *testing.T) {
	for _, name := range []string{
		"new-root-prompt-default-resolver-owned-capture-path",
		"new-root-prompt-injected-resolver-generic-capture-path",
		"new-subagent-start-exact-parent-facts",
		"extended-pretool-wait-current-actor",
		"stop-hook-active-safety-read",
		"full-hash-drift-invalidates-existing-actor-and-commits-only-safety",
		"full-hash-drift-declines-new-actor-without-activity-write",
	} {
		t.Run(name, func(t *testing.T) {
			// These functional branches opt into the existing race fixture allowance.
			// The separate six core deadline tests retain their original budgets.
			budget, consume, policyHold := 250*time.Millisecond, 100*time.Millisecond, 190*time.Millisecond
			caller := 900 * time.Millisecond
			if sqliteFlowTestLockTimeout() != 0 {
				budget, consume, policyHold = 600*time.Millisecond, 400*time.Millisecond, 210*time.Millisecond
				// Only this measured functional call allows instrumented post-BEGIN work.
				caller = 2 * time.Second
			}
			q := habQANew(t, habQAPlan{consume: consume, policyHold: policyHold, caller: caller})
			h := q.h
			if budget != 250*time.Millisecond {
				// Keep the independent real policy metadata acquisition cap at250ms.
				h.s = New(Options{Path: h.path, LockTimeout: budget, HookPolicies: h.policies, Clock: h.s.clock})
			}
			t.Logf("postcore functional admission_us=%d pre_policy_us=%d policy_hold_us=%d caller_us=%d", budget.Microseconds(), consume.Microseconds(), policyHold.Microseconds(), caller.Microseconds())
			setup := func(e HostEvent) HostReceipt {
				ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
				defer cancel()
				r, err := h.s.IngestHost(ctx, e)
				if err != nil || r.Durability != "committed" {
					t.Fatal("SETUP real public host capture", err)
				}
				return r
			}
			start := h.event("SessionStart", "", "")
			start.SessionSource = "startup"
			setup(start)
			needRoot := name == "new-subagent-start-exact-parent-facts" || name == "extended-pretool-wait-current-actor" || name == "stop-hook-active-safety-read" || name == "full-hash-drift-invalidates-existing-actor-and-commits-only-safety"
			var root HostReceipt
			var oldActor sqliteActorLocalRow
			if needRoot {
				root = setup(h.event("UserPromptSubmit", "root", ""))
				oldActor = hapQAActor(t, q, root.Actor)
			}
			e := h.event("UserPromptSubmit", "new-root", "")
			switch name {
			case "new-subagent-start-exact-parent-facts":
				e = h.event("SubagentStart", "child", "child")
			case "extended-pretool-wait-current-actor":
				e = h.event("PreToolUse", "root", "")
				e.ToolID, e.ToolName = "wait-1", "wait_agent"
			case "stop-hook-active-safety-read":
				e = h.event("Stop", "root", "")
				e.StopHookActive = true
			case "full-hash-drift-invalidates-existing-actor-and-commits-only-safety":
				e = h.event("Stop", "root", "")
			}
			h.at(10)
			if name == "new-root-prompt-injected-resolver-generic-capture-path" {
				h.s.resolve = func(context.Context, Event) (BindingSnapshot, bool, error) {
					if q.leases.Load() != 0 {
						t.Fatal("resolver called with an owned activity lease")
					}
					return q.binding, true, nil
				}
			}
			h.s.clock = ClockFunc(func() (ClockSample, error) {
				if q.leases.Load() != 0 {
					t.Fatal("clock called with an owned activity lease")
				}
				h.clockCalls++
				return h.sample, nil
			})
			ordinal, p := hapQACalibrate(t, q, e, budget)
			if name == "new-root-prompt-default-resolver-owned-capture-path" && (h.s.resolve != nil || len(p.Admission.Locations) == 0 || p.Normalized == nil || p.Extended) {
				t.Fatal("SETUP owned default new-work preparation branch")
			}
			if name == "new-root-prompt-injected-resolver-generic-capture-path" && (h.s.resolve == nil || len(p.Admission.Locations) != 0 || p.Normalized == nil || p.Extended) {
				t.Fatal("SETUP injected generic preparation branch")
			}
			if name == "new-subagent-start-exact-parent-facts" && (p.Normalized == nil || !reflect.DeepEqual(p.Normalized.Parent, root.Actor) || p.Extended) {
				t.Fatal("SETUP exact owned root-turn/actor parent")
			}
			if name == "extended-pretool-wait-current-actor" && (!p.Extended || p.Normalized == nil || p.Normalized.Kind != "wait_children" || p.ToolAfter == nil) {
				t.Fatal("SETUP concrete extended wait preparation")
			}
			if name == "stop-hook-active-safety-read" && (p.Extended || p.Normalized != nil || p.Clock.Site == sqliteCaptureClockNone) {
				t.Fatal("SETUP concrete stop safety read/clock branch")
			}
			q.before, q.rows = mwQAAudit(t, h.f)
			var err error
			q.policy, err = os.ReadFile(h.policyPath)
			if err != nil {
				t.Fatal(err)
			}
			drift := name == "full-hash-drift-invalidates-existing-actor-and-commits-only-safety" || name == "full-hash-drift-declines-new-actor-without-activity-write"
			if drift {
				path := filepath.Join(h.cwd[0], "runtime")
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				body, err := os.ReadFile(path)
				if err != nil || len(body) == 0 {
					t.Fatal("SETUP real artifact bytes", err)
				}
				body[len(body)-1] ^= 1
				if err = os.WriteFile(path, body, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
				after, err := os.Stat(path)
				if err != nil || !os.SameFile(info, after) || info.Size() != after.Size() || !info.ModTime().Equal(after.ModTime()) {
					t.Fatal("SETUP same-size same-mtime same-inode content drift", err)
				}
			}
			calls := h.clockCalls
			hapQAInvoke(t, q, e, ordinal)
			after, rows := mwQAAudit(t, h.f)
			policyBytes, err := os.ReadFile(h.policyPath)
			if err != nil {
				t.Fatal(err)
			}
			profile, err := h.policies.Eligibility(context.Background(), h.source, h.cwd[0])
			if err != nil {
				t.Fatal("cold separate policy result", err)
			}
			if drift {
				if reflect.DeepEqual(q.policy, policyBytes) || profile.CaptureEligible || profile.State != "invalidated" || profile.Revision != "2" || profile.DiagnosticCode != "profile_invalidated" || q.row.Eligibility.P[5] < 0 {
					t.Error("full rehash did not durably invalidate only the actual changed profile")
				}
			} else if !reflect.DeepEqual(q.policy, policyBytes) || !profile.CaptureEligible || profile.Revision != "1" || q.row.Eligibility.P[5] != -1 {
				t.Error("eligible read altered retained policy")
			}
			declined := name == "full-hash-drift-declines-new-actor-without-activity-write"
			if declined {
				if q.err != nil || q.r.Disposition != "review_required" || q.r.Durability != "not_committed" || q.r.Actor != nil || q.r.ProfileRevision != "2" || q.r.DiagnosticCode != "profile_invalidated" || h.clockCalls != calls || q.row.PhaseUS[6] != -1 {
					t.Error("new ineligible work created capture or reached writer", q.err)
				}
				if !reflect.DeepEqual(q.before, after) || !reflect.DeepEqual(q.rows, rows) {
					t.Error("new ineligible callback changed full32 activity rows/nonce")
				}
				return
			}
			if q.err != nil || q.r.Durability != "committed" || q.r.Actor == nil {
				t.Error("post-policy branch failed actual committed capture", q.err)
				return
			}
			if q.row.PhaseUS[6] < q.row.PhaseUS[5] || q.row.PhaseUS[6] >= q.row.EffectiveDeadlineUS || q.acquired.Load() <= int64(ordinal)+1 || h.clockCalls != calls+1 {
				t.Error("post-policy read/one clock/final writer did not use carried admission")
			}
			if after.Revision != bump(q.before.Revision) || after.DurabilityNonce == q.before.DurabilityNonce || after.ComputerID != q.before.ComputerID || len(rows["host_receipts"]) != len(q.rows["host_receipts"])+1 {
				t.Error("one atomic host revision/receipt/nonce")
			}
			for _, table := range []string{"bindings", "requests", "sync_configurations", "sync_plans", "sync_parts", "sync_attempts", "pending_sync", "pending_sync_roots", "recovery_decisions"} {
				if !reflect.DeepEqual(q.rows[table], rows[table]) {
					t.Error("unrelated typed rows changed", table)
				}
			}
			for _, table := range []string{"host_receipts", "event_receipts", "event_ids", "actor_generations"} {
				if !hapQAContains(q.rows[table], rows[table]) {
					t.Error("immutable prior history changed", table)
				}
			}
			a := hapQAActor(t, q, q.r.Actor)
			if a.Attribution != q.binding.Attribution || a.BindingID != q.binding.ID {
				t.Error("captured attribution changed")
			}
			safety := name == "stop-hook-active-safety-read" || name == "full-hash-drift-invalidates-existing-actor-and-commits-only-safety"
			if safety {
				if q.r.Disposition != "review_required" || a.Health != "stale" || a.Ref != oldActor.Ref || len(rows["uncertainties"]) != 1 || len(rows["uncertainty_evidence"]) != 1 || len(rows["intervals"]) != 0 || len(rows["outbox"]) != 0 || len(rows["actors"]) != len(q.rows["actors"]) {
					t.Error("safety branch invented work or lost the original actor")
				}
				want := "ordering_unavailable"
				if drift {
					want = "profile_invalidated"
				}
				if q.r.DiagnosticCode != want || (drift && q.r.ProfileRevision != "2") {
					t.Error("safety receipt lost exact policy decision")
				}
			} else if name == "extended-pretool-wait-current-actor" {
				if q.r.Disposition != "applied" || a.Ref != oldActor.Ref || a.State != "wait_children" || a.Health != "continuous" || a.SegmentID != nil || len(rows["host_tools"]) != 1 || len(rows["intervals"]) != 1 || len(rows["outbox"]) != 1 || len(rows["uncertainties"]) != 0 {
					t.Error("actual PRE wait did not close only confirmed work")
				}
				h.intervals("3", [2]int64{0, 10})
			} else {
				if q.r.Disposition != "applied" || a.State != "working" || a.Health != "continuous" || a.SegmentID == nil || len(rows["actors"]) != len(q.rows["actors"])+1 || len(rows["uncertainties"]) != 0 || len(rows["intervals"]) != 0 || len(rows["outbox"]) != 0 {
					t.Error("new work branch actor/segment/union semantics")
				}
				if name == "new-subagent-start-exact-parent-facts" && !reflect.DeepEqual(a.Parent, root.Actor) {
					t.Error("child lost exact validated parent")
				}
			}
			list, err := h.s.HostReceipts(context.Background(), HostReceiptFilter{})
			if err != nil {
				t.Fatal(err)
			}
			matched := 0
			for _, r := range list.Receipts {
				if r.ID == q.r.ID {
					matched++
					if !reflect.DeepEqual(r, q.r) {
						t.Error("cold receipt projection changed")
					}
				}
			}
			if matched != 1 {
				t.Error("one exact cold committed host receipt")
			}
			lock := habQALockPolicy(t, h.policyPath)
			replayClockCalls := h.clockCalls
			ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
			d := NewHostCaptureDiagnostics()
			ctx = d.Begin(ctx)
			replayed, replayErr := h.s.IngestHost(ctx, e)
			d.End(ctx, false)
			cancel()
			closeErr := lock.close()
			want := q.r
			want.Disposition = "duplicate"
			if replayErr != nil || closeErr != nil || !reflect.DeepEqual(replayed, want) || d.Snapshot()[0].EligibilityOutcome != "not_called" || d.Snapshot()[0].EligibilityExcludedUS != 0 || h.clockCalls != replayClockCalls {
				t.Error("exact replay did not retain historical receipt/policy/clock bypass", errors.Join(replayErr, closeErr))
			}
			mwQANonceOnly(t, after, rows, h.f)
		})
	}
}
