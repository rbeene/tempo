//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/hookstate"
)

func TestSQLiteHostEligibilityAdmissionCore(t *testing.T) {
	t.Run("successful-policy-crosses-original-deadline", func(t *testing.T) {
		q := habQANew(t, habQAPlan{consume: 100 * time.Millisecond, policyHold: 190 * time.Millisecond})
		q.invoke(t)
		q.policySucceeded(t)
		if q.row.PhaseUS[5] <= q.row.DeadlineUS || q.callerErr != nil || q.row.CallerDeadlineUS-q.row.DeadlineUS < 500000 {
			t.Fatal("SETUP successful policy did not cross original250 inside original900")
		}
		if q.err != nil && q.beginAfterResolver != 0 {
			t.Error("old failure did not stop at expired writer admission")
		}
		// Nonfatal desired-output assertion lets old RED execute every cold check.
		q.committed(t)
	})
	t.Run("spent-remainder-real-writer-refuses", func(t *testing.T) {
		q := habQANew(t, habQAPlan{consume: 100 * time.Millisecond, policyHold: 40 * time.Millisecond, writerAfterUnlock: 200 * time.Millisecond})
		q.invoke(t)
		q.policySucceeded(t)
		policyEnd := q.d.start.Add(time.Duration(q.row.PhaseUS[5]) * time.Microsecond)
		carried := q.d.start.Add(time.Duration(q.row.DeadlineUS+q.row.PhaseUS[5]-q.row.PhaseUS[4]) * time.Microsecond)
		if !q.writerReleaseStarted.After(carried) || !q.writerReleased.Before(policyEnd.Add(250*time.Millisecond)) || !q.ended.Before(q.writerReleaseStarted) || q.callerErr != nil {
			t.Fatal("SETUP real held-writer release did not distinguish remaining from reset250")
		}
		var native *sqliteio.Error
		if ncQAErrorCode(q.err) != "state_busy" || q.r.Durability != "not_committed" || !errors.As(q.err, &native) || native.Phase != sqliteio.BeginPhase || (native.Category != sqliteio.Busy && native.Category != sqliteio.Canceled) || native.Cleanup != nil || q.beginAfterResolver != 1 {
			t.Error("remaining allowance refusal lacked genuine clean native BEGIN contention", q.err)
		}
		q.unchanged(t)
	})
	t.Run("release-inside-remainder-commits", func(t *testing.T) {
		q := habQANew(t, habQAPlan{consume: 100 * time.Millisecond, policyHold: 30 * time.Millisecond, writerAfterUnlock: 40 * time.Millisecond})
		q.invoke(t)
		q.policySucceeded(t)
		original := q.d.start.Add(time.Duration(q.row.DeadlineUS) * time.Microsecond)
		if !q.writerReleased.Before(original) || q.callerErr != nil || q.beginAfterResolver != 1 {
			t.Fatal("SETUP old positive real writer release missed original deadline")
		}
		q.committed(t)
	})
	t.Run("expired-before-policy-never-revives", func(t *testing.T) {
		q := habQANew(t, habQAPlan{expireBeforePolicy: true})
		q.invoke(t)
		if ncQAErrorCode(q.err) != "state_busy" || q.r.Durability != "not_committed" || q.row.PhaseUS[4] != -1 || q.row.PhaseUS[6] != -1 || q.row.Eligibility == nil || q.row.Eligibility.P[6] != -1 || q.beginAfterResolver != 0 || q.acquired.Load() != 1 || q.callerErr != nil {
			t.Error("expired non-policy allowance reached Eligibility or another native owner", q.err)
		}
		q.unchanged(t)
	})
	t.Run("cancel-during-real-policy-wait", func(t *testing.T) {
		q := habQANew(t, habQAPlan{consume: 40 * time.Millisecond, policyHold: 150 * time.Millisecond, cancelAfter: 40 * time.Millisecond})
		q.invoke(t)
		var policy *hookstate.Error
		if q.callerErr != context.Canceled || !q.ended.Before(q.lockReleased) || q.row.PhaseUS[4] < 0 || q.row.PhaseUS[5] < 0 || q.row.PhaseUS[6] != -1 || q.beginAfterResolver != 0 || q.acquired.Load() != 1 {
			t.Fatal("SETUP caller did not cancel inside real policy lock wait")
		}
		// Preserve the existing policy error and current activity classification;
		// policy errors are not an opportunity to grant extra activity admission.
		if !errors.As(q.err, &policy) || policy.Code != "state_busy" || ncQAErrorCode(q.err) != "state_corrupt" || q.r.Durability != "not_committed" || q.row.Eligibility == nil || q.row.Eligibility.R[0][0] != 0 {
			t.Error("policy cancellation priority/cause changed", q.err)
		}
		q.unchanged(t)
	})
	t.Run("shorter-caller-dominates", func(t *testing.T) {
		q := habQANew(t, habQAPlan{consume: 70 * time.Millisecond, policyHold: 40 * time.Millisecond, writerAfterUnlock: 200 * time.Millisecond, caller: 200 * time.Millisecond})
		q.invoke(t)
		q.policySucceeded(t)
		originalCaller := q.d.start.Add(time.Duration(q.row.CallerDeadlineUS) * time.Microsecond)
		// The absolute native admission deadline can expire before ctx.Err is
		// delivered. Checked watcher cleanup does not rewrite that saved error.
		var native *sqliteio.Error
		nodes, nativeCount, domainCount := 0, 0, 0
		canonical := failure("state_busy")
		// Walk once, with a finite node budget. Primary Busy cannot conceal
		// cleanup, uncertainty, a nil typed error or any additional cause.
		var cleanTree func(error) bool
		cleanTree = func(err error) bool {
			nodes++
			if err == nil || nodes > 16 {
				return false
			}
			switch e := err.(type) {
			case *sqliteio.Error:
				nativeCount++
				if e == nil || nativeCount != 1 {
					return false
				}
				native = e
				return e.Phase == sqliteio.BeginPhase && e.Cleanup == nil &&
					(e.Category == sqliteio.Busy && e.Code == 5 || e.Category == sqliteio.Canceled && (e.Code == 5 || e.Code == 9)) &&
					(e.Cause == nil || e.Cause == context.DeadlineExceeded)
			case *Error:
				domainCount++
				return e != nil && domainCount == 1 && e.Code == canonical.Code &&
					e.Message == canonical.Message && e.Retryable == canonical.Retryable && !e.Uncertain && e.Details == nil
			case interface{ Unwrap() []error }:
				if nodes >= 16 {
					return false
				}
				children := e.Unwrap()
				if len(children) == 0 || len(children) > 16-nodes {
					return false
				}
				for _, child := range children {
					if !cleanTree(child) {
						return false
					}
				}
				return true
			}
			return false
		}
		cleanRefusal := cleanTree(q.err) && nativeCount == 1 && domainCount == 1
		nativeFound := native != nil
		phase, category, code := sqliteio.Phase("none"), sqliteio.Category("none"), int32(0)
		causeNil, causeDeadline, cleanupNil := false, false, false
		if nativeFound {
			phase, category, code = native.Phase, native.Category, native.Code
			causeNil, causeDeadline, cleanupNil = native.Cause == nil, native.Cause == context.DeadlineExceeded, native.Cleanup == nil
		}
		t.Logf("shorter witness deadline_equal=%t end_at_or_after_caller=%t caller_allowed=%t cause_deadline=%t clean_refusal=%t one_begin=%t before_release=%t native_found=%t native_phase=%s native_category=%s native_code=%d native_cause_nil=%t native_cause_deadline=%t native_cleanup_nil=%t caller_us=%d end_us=%d release_started_us=%d", q.row.DeadlineUS == q.row.CallerDeadlineUS, !q.ended.Before(originalCaller), q.callerErr == nil || q.callerErr == context.DeadlineExceeded, causeDeadline, cleanRefusal, q.beginAfterResolver == 1, q.ended.Before(q.writerReleaseStarted), nativeFound, phase, category, code, causeNil, causeDeadline, cleanupNil, q.row.CallerDeadlineUS, q.ended.Sub(q.d.start).Microseconds(), q.writerReleaseStarted.Sub(q.d.start).Microseconds())
		if q.row.DeadlineUS != q.row.CallerDeadlineUS || q.ended.Before(originalCaller) || (q.callerErr != nil && q.callerErr != context.DeadlineExceeded) || !cleanRefusal || q.beginAfterResolver != 1 || !q.ended.Before(q.writerReleaseStarted) {
			t.Fatal("SETUP shorter original caller did not bound actual held native writer")
		}
		if ncQAErrorCode(q.err) != "state_busy" || q.r.Durability != "not_committed" {
			t.Error("short original caller was extended or falsely committed", q.err)
		}
		q.unchanged(t)
	})
}
