//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"time"
)

func sqliteRecoveryStagedMeta(m *sqliteCaptureMutation) sqliteStoreMeta {
	meta := m.meta
	meta.Revision = m.nextRevision
	return meta
}
func sqliteRecoveryNext(value string) (string, error) {
	if value == "18446744073709551615" {
		return "", failure("validation")
	}
	return bump(value), nil
}

func sqliteRecoveryApplyResolve(m *sqliteCaptureMutation, in ResolveInput, initial sqliteRecoveryTarget, sample *ClockSample, clockErr error) (MutationResult, *Error, error) {
	if !in.DiscardTail {
		if sample == nil {
			return MutationResult{}, nil, failure("validation")
		}
		changed, err := m.quarantineClock(*sample)
		if err != nil {
			return MutationResult{}, nil, err
		}
		m.transition.Changed = changed
		if clockErr != nil {
			domain, fatal := sqliteRecoveryDomain(clockErr)
			return MutationResult{}, domain, fatal
		}
	}
	target, err := sqliteRecoveryReadTarget(m.tx, sqliteRecoveryStagedMeta(m), initial.Uncertainty.ID)
	if err != nil {
		return MutationResult{}, nil, err
	}
	input := RecoveryInput{UncertaintyID: in.UncertaintyID, End: in.End, DiscardTail: in.DiscardTail}
	end, suffix, err := sqliteRecoveryEnd(target, input, sample)
	if err != nil {
		domain, fatal := sqliteRecoveryDomain(err)
		return MutationResult{}, domain, fatal
	}
	contacts, err := sqliteRecoveryReadContacts(m.tx, sqliteRecoveryStagedMeta(m), target, end)
	if err != nil {
		return MutationResult{}, nil, err
	}
	if err = sqliteRecoveryContactError(target, end, contacts); err != nil {
		domain, fatal := sqliteRecoveryDomain(err)
		return MutationResult{}, domain, fatal
	}
	next := target.Uncertainty
	next.Revision, err = sqliteRecoveryNext(next.Revision)
	if err != nil {
		return MutationResult{}, nil, err
	}
	next.State = "resolved"
	next.ResolutionEnd = &end
	next.Discarded = in.DiscardTail
	nextSegment := target.Segment
	nextSegment.End = &end
	var nextActor *sqliteActorLocalRow
	if a := target.Actor; a != nil && a.Ref == next.Actor && a.State == "working" && a.Health != "continuous" && a.SegmentID != nil && *a.SegmentID == target.Segment.ID {
		copyActor := *a
		copyActor.Revision, err = sqliteRecoveryNext(a.Revision)
		if err != nil {
			return MutationResult{}, nil, err
		}
		copyActor.State = "interrupted"
		copyActor.SegmentID = nil
		nextActor = &copyActor
	}
	// Resolve has no partial proposal prefix: every domain conflict is checked
	// against selected supports/reservations before the first resolution write.
	conflict, err := sqliteRecoveryPlanFinalization(m.tx, sqliteRecoveryStagedMeta(m), target, nextSegment)
	if err != nil {
		return MutationResult{}, nil, err
	}
	if conflict {
		return MutationResult{}, failure("clock_conflict"), nil
	}
	evidence := target.Evidence
	if !in.DiscardTail && next.UpperBound == nil {
		bound := sqliteRecoveryCeiling(target, *sample)
		next.UpperBound = &bound
		value := sqliteCaptureCopySample(*sample)
		evidence.BoundSample = &value
	}
	if err = m.writeUncertainty(&target.Uncertainty, next); err != nil {
		return MutationResult{}, nil, err
	}
	n, e := sqliteWriteUncertaintyEvidence(m.tx, next.ID, &target.Evidence, evidence)
	if err = m.add(n, e); err != nil {
		return MutationResult{}, nil, err
	}
	decision := recoveryDecision{RequestID: in.RequestID, UncertaintyID: next.ID, PreviousRevision: target.Uncertainty.Revision, ResolutionEnd: end, Discarded: in.DiscardTail, Reason: in.Reason, DiscardedSuffix: suffix}
	if sample != nil {
		owned := sqliteCaptureCopySample(*sample)
		decision.ObservedSample = &owned
	}
	n, e = sqliteInsertRecoveryDecision(m.tx, decision)
	if err = m.add(n, e); err != nil {
		return MutationResult{}, nil, err
	}
	if err = m.writeSegment(&target.Segment, nextSegment); err != nil {
		return MutationResult{}, nil, err
	}
	affected := []string{next.ID}
	if nextActor != nil {
		if err = m.writeActor(target.Actor, *nextActor); err != nil {
			return MutationResult{}, nil, err
		}
		affected = append(affected, nextActor.ID)
	}
	drained, e := sqliteDrainFinalization(m.tx, m.meta.ComputerID, m.nextRevision)
	if e != nil {
		return MutationResult{}, nil, e
	}
	if drained.OperationError != nil {
		return MutationResult{}, nil, failure("state_corrupt")
	}
	if err = m.add(drained.Delta, nil); err != nil {
		return MutationResult{}, nil, err
	}
	sqliteCaptureMergeFinal(&m.transition.Finalization, drained.Selection)
	m.transition.Dependencies.ChangedSegmentIDs = append(m.transition.Dependencies.ChangedSegmentIDs, drained.Selection.ChangedSegmentIDs...)
	m.transition.Changed = true
	return MutationResult{Changed: true, AffectedIDs: affected, EntityRevision: &next.Revision}, nil, nil
}

// Cap preflight checks every actual membership before the shared cap writer.
// A later clock conflict cannot retain only the first part of a cap batch.
func sqliteRecoveryCapCheck(m *sqliteCaptureMutation, a sqliteActorLocalRow, sample ClockSample) error {
	bound := sample.WallUTC
	if a.SegmentID != nil {
		row, found, err := sqliteReadSegmentLocal(m.tx, m.meta.ComputerID, *a.SegmentID)
		if err != nil {
			return err
		}
		if !found {
			return failure("state_corrupt")
		}
		epoch, found, err := sqliteReadEpoch(m.tx, m.meta.ComputerID, row.EpochID)
		if err != nil {
			return err
		}
		if !found {
			return failure("state_corrupt")
		}
		if at, ok := projectSample(&epoch.Value, sample); ok {
			bound = at
		}
	}
	ids, err := sqliteActorUncertaintyIDsLocal(m.tx, m.meta.ComputerID, a.Ref.Key)
	if err != nil {
		return err
	}
	for _, id := range ids {
		u, found, e := sqliteReadUncertaintyScalar(m.tx, m.meta.ComputerID, id)
		if e != nil {
			return e
		}
		if !found {
			return failure("state_corrupt")
		}
		if u.State == "unresolved" && u.UpperBound == nil {
			if bound.Before(u.LowerBound) {
				return failure("clock_conflict")
			}
			if _, e = sqliteRecoveryNext(u.Revision); e != nil {
				return e
			}
		}
	}
	return nil
}

func sqliteRecoveryApplyInterrupt(m *sqliteCaptureMutation, in InterruptInput, initial sqliteActorLocalRow, sample *ClockSample, clockErr error) (MutationResult, *Error, error) {
	current := initial
	affected := []string{initial.ID}
	if initial.State == "working" {
		if sample == nil {
			return MutationResult{}, nil, failure("validation")
		}
		changed, err := m.quarantineClock(*sample)
		if err != nil {
			return MutationResult{}, nil, err
		}
		m.transition.Changed = changed
		if clockErr != nil {
			domain, fatal := sqliteRecoveryDomain(clockErr)
			return MutationResult{}, domain, fatal
		}
		actor, err := m.readActor(initial.Ref.Key)
		if err != nil {
			return MutationResult{}, nil, err
		}
		if actor == nil || actor.Ref != initial.Ref {
			return MutationResult{}, nil, failure("state_corrupt")
		}
		did, err := m.quarantine(*actor, "source_lost", "Interrupt", *sample)
		if err != nil {
			return MutationResult{}, nil, err
		}
		m.transition.Changed = m.transition.Changed || did
		actor, err = m.readActor(initial.Ref.Key)
		if err != nil {
			return MutationResult{}, nil, err
		}
		if actor == nil {
			return MutationResult{}, nil, failure("state_corrupt")
		}
		current = *actor
		if err = sqliteRecoveryCapCheck(m, current, *sample); err != nil {
			domain, fatal := sqliteRecoveryDomain(err)
			return MutationResult{}, domain, fatal
		}
		domain, err := m.cap(current, *sample)
		if err != nil {
			return MutationResult{}, nil, err
		}
		if domain != nil {
			return MutationResult{}, nil, failure("state_corrupt")
		}
		ids, err := sqliteActorUncertaintyIDsLocal(m.tx, m.meta.ComputerID, current.Ref.Key)
		if err != nil {
			return MutationResult{}, nil, err
		}
		for _, id := range ids {
			u, found, e := sqliteReadUncertaintyScalar(m.tx, m.meta.ComputerID, id)
			if e != nil {
				return MutationResult{}, nil, e
			}
			if !found {
				return MutationResult{}, nil, failure("state_corrupt")
			}
			if u.State == "unresolved" {
				affected = append(affected, id)
			}
		}
	}
	// The legacy waiting path records only genuine native wait provenance. The
	// wall observation does not sample the injected evidence clock or invent work.
	if current.State != "working" {
		_, err := m.quarantine(current, "source_lost", "Interrupt", ClockSample{Capability: "unavailable", WallUTC: time.Now().UTC()})
		if err != nil {
			return MutationResult{}, nil, err
		}
		actor, err := m.readActor(current.Ref.Key)
		if err != nil {
			return MutationResult{}, nil, err
		}
		if actor == nil {
			return MutationResult{}, nil, failure("state_corrupt")
		}
		current = *actor
	}
	next := current
	var err error
	next.Revision, err = sqliteRecoveryNext(current.Revision)
	if err != nil {
		return MutationResult{}, nil, err
	}
	next.State = "interrupted"
	next.SegmentID = nil
	if err = m.writeActor(&current, next); err != nil {
		return MutationResult{}, nil, err
	}
	m.transition.Changed = true
	return MutationResult{Changed: true, AffectedIDs: affected, EntityRevision: &next.Revision}, nil, nil
}
