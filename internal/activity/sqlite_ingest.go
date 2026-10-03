//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func sqliteCaptureFingerprint(e Event) string {
	raw, _ := json.Marshal(e)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}
func sqliteCaptureOrder(meta sqliteStoreMeta, e Event, a *sqliteActorLocalRow) (EventResult, *Error, sqliteCaptureBranch, bool) {
	result := baseResult(e, meta.Revision, "applied")
	if e.Actor.ComputerID != meta.ComputerID {
		return result, failure("validation"), sqliteCaptureNoSample, false
	}
	gen, _ := counter(e.Generation)
	seq, _ := counter(e.Sequence)
	newer := a == nil
	if a != nil {
		old, _ := counter(a.Ref.Generation)
		if gen < old || gen == old && sqliteCaptureTerminal(*a) {
			result.Disposition = "stale"
			return result, nil, sqliteCaptureNoSample, false
		}
		newer = gen > old
	}
	if newer && (e.Kind != "work" || seq != 1) {
		return result, failure("event_gap"), sqliteCaptureNoSample, newer
	}
	if (e.BindingID != "" || e.Parent != nil) && (e.Kind != "work" || e.Sequence != "1") {
		return result, failure("validation"), sqliteCaptureNoSample, newer
	}
	if !newer {
		last, _ := counter(a.Sequence)
		if a.Health == "order_blocked" {
			return result, failure("event_gap"), sqliteCaptureNoSample, false
		}
		if seq <= last {
			return result, failure("event_conflict"), sqliteCaptureNoSample, false
		}
		if seq != last+1 {
			return result, nil, sqliteCaptureGapSample, false
		}
		return result, nil, sqliteCaptureExistingSample, false
	}
	return result, nil, sqliteCaptureNewSample, true
}
func sqliteCaptureSite(branch sqliteCaptureBranch) sqliteCaptureClockSite {
	if branch == sqliteCaptureGapSample {
		return sqliteCaptureClockGap
	}
	if branch == sqliteCaptureExistingSample || branch == sqliteCaptureNewSample {
		return sqliteCaptureClockReduce
	}
	return sqliteCaptureClockNone
}

// Read-only writer priority checks shared with direct reducer callers. No
// capacity refusal may hide the current computer, exact replay or raw ID conflict.
func sqliteCapturePriority(tx *sqliteio.Tx, meta sqliteStoreMeta, e Event) (sqliteEventTransition, bool, error) {
	result := sqliteEventTransition{Result: baseResult(e, meta.Revision, "applied"), Finalization: sqliteEmptyFinalizationSelection()}
	if e.Actor.ComputerID != meta.ComputerID {
		result.OperationError = failure("validation")
		return result, true, nil
	}
	key := eventKey(e)
	prior, found, err := sqliteReadEventReceipt(tx, key, meta.Revision)
	if err != nil {
		return sqliteEventTransition{}, false, err
	}
	if found {
		if prior.Value.Fingerprint != sqliteCaptureFingerprint(e) {
			result.OperationError = failure("event_conflict")
		} else {
			result.Result = prior.Value.Result
			result.Result.Disposition = "duplicate"
		}
		return result, true, nil
	}
	oldKey, found, err := sqliteReadEventID(tx, e.EventID)
	if err != nil {
		return sqliteEventTransition{}, false, err
	}
	if found && oldKey != key {
		result.OperationError = failure("event_conflict")
		return result, true, nil
	}
	return result, false, nil
}

// The standalone reducer and host composer share validation and one mutation
// owner. Validation runs against original facts before any host row is staged.
func sqliteReduceEvent(tx *sqliteio.Tx, meta sqliteStoreMeta, e Event, prepared sqliteCapturePreparation, clock *sqliteCaptureClock, guardWaitResume bool) (result sqliteEventTransition, err error) {
	exhausted := meta.Revision == "18446744073709551615"
	defer func() {
		if err == nil && exhausted && result.Changed {
			err = failure("validation")
		}
		if err != nil {
			result = sqliteEventTransition{}
		}
	}()
	early, ready, err := sqliteCheckPreparedEvent(tx, meta, e, prepared, clock)
	if err != nil {
		return result, err
	}
	if !ready {
		return early, nil
	}
	sample, clockErr := clock.take(sqliteCaptureSite(prepared.Branch))
	if clockErr != nil {
		if v, ok := clockErr.(*Error); !ok || v.Code != "clock_unavailable" {
			return result, clockErr
		}
	}
	nextRevision := meta.Revision
	if !exhausted {
		nextRevision = bump(meta.Revision)
	}
	m := sqliteCaptureMutation{tx: tx, meta: meta, nextRevision: nextRevision}
	m.transition.Finalization = sqliteEmptyFinalizationSelection()
	changed, err := m.quarantineClock(sample)
	if err != nil {
		return result, err
	}
	return m.applyPreparedEvent(e, prepared.Binding, sqliteCaptureSample{Value: sample, Unavailable: clockErr != nil}, guardWaitResume, changed)
}

func sqliteCheckPreparedEvent(tx *sqliteio.Tx, meta sqliteStoreMeta, e Event, prepared sqliteCapturePreparation, clock *sqliteCaptureClock) (result sqliteEventTransition, ready bool, err error) {
	if err = validateEvent(e); err != nil {
		return result, false, err
	}
	if !sqliteDependencyScope(meta.ComputerID, meta.Revision) || prepared.ComputerID == "" || clock == nil {
		return result, false, failure("validation")
	}
	result.Result = baseResult(e, meta.Revision, "applied")
	result.Finalization = sqliteEmptyFinalizationSelection()
	priority, terminal, err := sqliteCapturePriority(tx, meta, e)
	if err != nil {
		return result, false, err
	}
	if terminal {
		return priority, false, nil
	}
	if prepared.RefusalCode != "" {
		if prepared.RefusalCode != "state_busy" {
			return result, false, failure("validation")
		}
		return result, false, failure("state_busy")
	}
	if prepared.ComputerID != meta.ComputerID || !prepared.TargetChecked {
		return result, false, failure("state_busy")
	}
	a, err := sqliteCaptureReadActor(tx, meta.ComputerID, meta.Revision, e.Actor)
	if err != nil {
		return result, false, err
	}
	same, err := sqliteCaptureSameActor(a, prepared.Target)
	if err != nil {
		return result, false, err
	}
	if !same {
		return result, false, failure("state_busy")
	}
	base, op, branch, newer := sqliteCaptureOrder(meta, e, a)
	result.Result = base
	if branch == sqliteCaptureNoSample {
		result.OperationError = op
		return result, false, nil
	}
	if err = sqliteCaptureCheckBinding(tx, meta, e, prepared.Binding); err != nil {
		return result, false, err
	}
	if newer {
		if !prepared.Binding.Needed {
			return result, false, failure("validation")
		}
		if prepared.Binding.ErrorCode != "" {
			if prepared.Binding.ErrorCode == "attribution_conflict" && prepared.Binding.Conflict != nil && prepared.Binding.Snapshot != nil {
				result.OperationError = attributionConflict(*prepared.Binding.Conflict, prepared.Binding.Snapshot.Attribution)
			} else {
				result.OperationError = failure(prepared.Binding.ErrorCode)
			}
			return result, false, nil
		}
		if prepared.Binding.Snapshot == nil {
			result.Result.Disposition = "untracked"
			return result, false, nil
		}
	}
	if prepared.Branch != branch || clock.Site != sqliteCaptureSite(branch) {
		return result, false, failure("state_busy")
	}
	if clock.Mode == sqliteCapturePrepared {
		now, e := sqliteCaptureObserveClock(tx, meta.ComputerID, meta.Revision)
		if e != nil {
			return result, false, e
		}
		same, e := sqliteCaptureSameClock(now, prepared.ClockActors)
		if e != nil {
			return result, false, e
		}
		if !same {
			return result, false, failure("state_busy")
		}
	} else if clock.Mode != sqliteCaptureNative {
		return result, false, failure("validation")
	}
	return result, true, nil
}

func (m *sqliteCaptureMutation) applyPreparedEvent(e Event, binding sqliteCaptureBinding, preparedSample sqliteCaptureSample, guardWaitResume bool, clockChanged bool) (result sqliteEventTransition, err error) {
	defer func() {
		if err != nil {
			result = sqliteEventTransition{}
		}
	}()
	tx, meta := m.tx, m.meta
	sample := preparedSample.Value
	var clockErr error
	if preparedSample.Unavailable {
		clockErr = failure("clock_unavailable")
	}
	// Shared safety may have rewritten the target; use its actual new scalar.
	a, err := m.readActor(e.Actor)
	if err != nil {
		return result, err
	}
	base, op, branch, newer := sqliteCaptureOrder(meta, e, a)
	m.transition.Result = base
	if branch == sqliteCaptureNoSample {
		m.transition.OperationError = op
		return m.transition, nil
	}
	sequence, key, fingerprint := e.Sequence, eventKey(e), sqliteCaptureFingerprint(e)
	if branch == sqliteCaptureGapSample {
		if a == nil {
			return result, failure("state_corrupt")
		}
		if _, err = m.quarantine(*a, "event_gap", "SourceObservation", sample); err != nil {
			return result, err
		}
		a, err = m.readActor(e.Actor)
		if err != nil {
			return result, err
		}
		if a == nil {
			return result, failure("state_corrupt")
		}
		after := *a
		after.Health = "order_blocked"
		after.Revision = bump(a.Revision)
		if err = m.writeActor(a, after); err != nil {
			return result, err
		}
		if after.SegmentID != nil {
			seg, ok, e := sqliteReadSegmentLocal(tx, meta.ComputerID, *after.SegmentID)
			if e != nil {
				return result, e
			}
			if !ok {
				return result, failure("state_corrupt")
			}
			if seg.UncertaintyID != nil {
				ev, ok, e := sqliteReadUncertaintyEvidenceScalar(tx, *seg.UncertaintyID)
				if e != nil {
					return result, e
				}
				if !ok {
					return result, failure("state_corrupt")
				}
				next := ev
				next.MissingFrom = bump(after.Sequence)
				next.MissingThrough = sequence
				n, x := sqliteWriteUncertaintyEvidence(tx, *seg.UncertaintyID, &ev, next)
				if x = m.add(n, x); x != nil {
					return result, x
				}
			}
		}
		m.transition.Changed = true
		m.transition.OperationError = failure("event_gap")
		return m.transition, nil
	}
	if clockErr != nil {
		m.transition.Changed = clockChanged
		m.transition.OperationError = failure("clock_unavailable")
		return m.transition, nil
	}
	if guardWaitResume && !newer && a != nil && a.Health != "continuous" {
		m.transition.Changed = clockChanged
		m.transition.OperationError = failure("clock_conflict")
		return m.transition, nil
	}
	if newer {
		id, revision := newID(), "1"
		if a != nil {
			if _, err = m.quarantine(*a, "superseded", "SourceObservation", sample); err != nil {
				return result, err
			}
			a, err = m.readActor(e.Actor)
			if err != nil {
				return result, err
			}
			if a == nil {
				return result, failure("state_corrupt")
			}
			op, fatal := m.cap(*a, sample)
			if fatal != nil {
				return result, fatal
			}
			if op != nil {
				m.transition.Changed = true
				m.transition.OperationError = op
				return m.transition, nil
			}
			id, revision = a.ID, bump(a.Revision)
		}
		b := *binding.Snapshot
		next := sqliteActorLocalRow{ID: id, Revision: revision, Ref: base.Actor, Sequence: e.Sequence, State: "working", Health: "continuous", BindingID: b.ID, BindingRevision: b.Revision, Attribution: b.Attribution, Parent: e.Parent, LastEvidence: sample}
		op, fatal := m.open(&next, sample, key)
		if fatal != nil {
			return result, fatal
		}
		if op != nil {
			m.transition.Changed = clockChanged || a != nil && a.Health != "continuous"
			m.transition.OperationError = op
			return m.transition, nil
		}
		if err = m.writeActor(a, next); err != nil {
			return result, err
		}
		a = &next
	} else {
		if a == nil {
			return result, failure("state_corrupt")
		}
		stale := a.Health != "continuous"
		if e.Kind == "observe_work" && (a.State != "working" || stale) {
			m.transition.Changed = clockChanged
			m.transition.OperationError = failure("invalid_transition")
			return m.transition, nil
		}
		if stale {
			op, fatal := m.cap(*a, sample)
			if fatal != nil {
				return result, fatal
			}
			if op != nil {
				m.transition.Changed = true
				m.transition.OperationError = op
				return m.transition, nil
			}
		}
		next := *a
		var op *Error
		var fatal error
		switch e.Kind {
		case "observe_work":
			op, fatal = m.confirm(&next, sample, key, false)
		case "work":
			if a.State != "working" || stale {
				op, fatal = m.open(&next, sample, key)
				if fatal != nil {
					return result, fatal
				}
				if op != nil {
					m.transition.Changed = clockChanged || stale
					m.transition.OperationError = op
					return m.transition, nil
				}
				next.State = "working"
				next.Health = "continuous"
			} else {
				op, fatal = m.confirm(&next, sample, key, false)
			}
		default:
			if a.State == "working" && !stale {
				op, fatal = m.confirm(&next, sample, key, true)
			}
			if op == nil && fatal == nil {
				next.State = e.Kind
				if e.Kind == "finish" {
					next.State = "finished"
				}
				if e.Kind == "interrupt" {
					next.State = "interrupted"
				}
				next.SegmentID = nil
			}
		}
		if fatal != nil {
			return result, fatal
		}
		if op != nil {
			m.transition.Changed = clockChanged
			m.transition.OperationError = op
			return m.transition, nil
		}
		next.Sequence = e.Sequence
		next.LastEvidence = sample
		next.Revision = bump(a.Revision)
		if err = m.writeActor(a, next); err != nil {
			return result, err
		}
		a = &next
	}
	drained, err := sqliteDrainFinalization(tx, meta.ComputerID, m.nextRevision)
	if err = m.add(drained.Delta, err); err != nil {
		return result, err
	}
	sqliteCaptureMergeFinal(&m.transition.Finalization, drained.Selection)
	m.transition.Dependencies.ChangedSegmentIDs = append(m.transition.Dependencies.ChangedSegmentIDs, drained.Selection.ChangedSegmentIDs...)
	if drained.OperationError != nil {
		m.transition.Changed = clockChanged
		m.transition.OperationError = drained.OperationError
		return m.transition, nil
	}
	m.transition.Result.SnapshotRevision = m.nextRevision
	m.transition.Result.SegmentID = sqliteCaptureCopyString(a.SegmentID)
	ids, err := sqliteActorUncertaintyIDsLocal(tx, meta.ComputerID, a.Ref.Key)
	if err != nil {
		return result, err
	}
	m.transition.Result.UncertaintyIDs = append([]string{}, ids...)
	receipt := sqliteEventReceiptRow{Key: key, Value: eventReceipt{Fingerprint: fingerprint, Result: m.transition.Result}}
	n, x := sqliteInsertEventReceipt(tx, receipt, e.EventID)
	if x = m.add(n, x); x != nil {
		return result, x
	}
	m.transition.Dependencies.EventReceiptKeys = append(m.transition.Dependencies.EventReceiptKeys, key)
	m.transition.Changed = true
	return m.transition, nil
}
