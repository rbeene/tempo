package activity

import (
	"context"
	"sort"
	"time"
)

// A callback may return a committed safety error only when it has quarantined
// evidence. Ordinary input/conflict errors roll the entire proposal back.
func (s *Service) recoveryMutation(ctx context.Context, id, operation string, input any, apply func(*state) (MutationResult, bool, error)) (MutationResult, error) {
	if !validUUID(id) {
		return MutationResult{}, failure("validation")
	}
	fingerprint := mutationFingerprint(operation, input)
	old, ok, err := s.replayMutation(ctx, id, operation, fingerprint)
	if err != nil {
		return MutationResult{}, err
	}
	if ok {
		return recoveryReceipt(old)
	}
	_, exists, err := s.store.read(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	if !exists {
		return MutationResult{}, requiredRecovery("binding")
	}
	var result MutationResult
	var operationErr error
	err = s.store.update(ctx, func(st *state) (bool, error) {
		prior, found, e := mutationLookup(st, id, operation, fingerprint)
		if e != nil {
			return false, e
		}
		if found {
			result, operationErr = recoveryReceipt(prior)
			return false, nil
		}
		var commitError bool
		result, commitError, operationErr = apply(st)
		if operationErr != nil && !commitError {
			return false, operationErr
		}
		receipt := mutationRequest{Operation: operation, Fingerprint: fingerprint}
		if operationErr != nil {
			ae, ok := operationErr.(*Error)
			if !ok {
				return false, failure("validation")
			}
			// Keep committed failure receipts finite and safe. The same error is
			// returned now and on replay, without arbitrary diagnostic payloads.
			operationErr = failure(ae.Code)
			receipt.Error = operationErr.(*Error)
		} else {
			result.ContractVersion = 1
			result.SnapshotRevision = bump(st.Revision)
			result.RequestID = id
			if result.AffectedIDs == nil {
				result.AffectedIDs = []string{}
			}
			sort.Strings(result.AffectedIDs)
			receipt.MutationResult = &result
		}
		saveMutation(st, id, receipt)
		return true, nil
	})
	if err != nil {
		return MutationResult{}, requestError(err, id)
	}
	return result, operationErr
}
func recoveryReceipt(r mutationRequest) (MutationResult, error) {
	if r.Error != nil {
		return MutationResult{}, r.Error
	}
	if r.MutationResult == nil {
		return MutationResult{}, failure("state_corrupt")
	}
	return *r.MutationResult, nil
}

func (s *Service) Interrupt(ctx context.Context, in InterruptInput) (MutationResult, error) {
	if !validUUID(in.ActorID) {
		return MutationResult{}, failure("validation")
	}
	if in.Generation == "" {
		return MutationResult{}, requiredRecovery("generation")
	}
	if n, ok := counter(in.Generation); !ok || n == 0 {
		return MutationResult{}, failure("validation")
	}
	if err := validateRevision(in.IfRevision); err != nil {
		return MutationResult{}, err
	}
	if !in.Confirmed {
		return MutationResult{}, failure("confirmation_required")
	}
	if s.store.sqliteOnly {
		return s.interruptSQLite(ctx, in)
	}
	return s.recoveryMutation(ctx, in.RequestID, "activity.interrupt", in, func(st *state) (MutationResult, bool, error) {
		var a *Actor
		for _, v := range st.Actors {
			if v.ID == in.ActorID {
				a = v
				break
			}
		}
		if a == nil {
			return MutationResult{}, false, failure("actor_not_found")
		}
		if a.Ref.Generation != in.Generation || a.Revision != in.IfRevision {
			return MutationResult{}, false, failure("revision_conflict")
		}
		if terminal(a) {
			return MutationResult{}, false, failure("invalid_transition")
		}
		affected := []string{a.ID}
		if a.State == "working" {
			sample, clockErr := s.sample()
			changed := quarantineClock(st, sample)
			if clockErr != nil {
				return MutationResult{}, changed, clockErr
			}
			// Source interruption is not a reliable lifecycle stop. Preserve its tail.
			changed = quarantine(st, a, "source_lost", sample) || changed
			proposal := copyState(st)
			next := proposal.Actors[actorKey(a.Ref.Key)]
			if err := capUncertainties(proposal, next, sample); err != nil {
				return MutationResult{}, changed, err
			}
			*st = *proposal
			a = next
			for _, id := range a.UncertaintyIDs {
				if st.Uncertainties[id].State == "unresolved" {
					affected = append(affected, id)
				}
			}
		}
		retainHostWaitLoss(st, a, "Interrupt", time.Now().UTC())
		detach(a)
		return MutationResult{Changed: true, AffectedIDs: affected, EntityRevision: &a.Revision}, false, nil
	})
}
func (s *Service) ObserveSource(ctx context.Context, in SourceObservation) (MutationResult, error) {
	if s.store.sqliteOnly {
		return s.observeSourceSQLite(ctx, in)
	}
	if !validRef(in.Actor) {
		return MutationResult{}, failure("validation")
	}
	switch in.Reason {
	case "source_lost", "ordering_unavailable", "restart_unknown":
	default:
		return MutationResult{}, failure("validation")
	}
	return s.recoveryMutation(ctx, in.RequestID, "activity.observe_source", in, func(st *state) (MutationResult, bool, error) {
		return s.observeSourceState(st, in)
	})
}
func (s *Service) observeSourceState(st *state, in SourceObservation) (MutationResult, bool, error) {
	a := st.Actors[actorKey(in.Actor.Key)]
	if a == nil {
		return MutationResult{}, false, failure("actor_not_found")
	}
	if a.Ref != in.Actor {
		old, _ := counter(in.Actor.Generation)
		current, _ := counter(a.Ref.Generation)
		if old > current {
			return MutationResult{}, false, failure("actor_not_found")
		}
		return MutationResult{}, false, nil
	}
	if terminal(a) {
		return MutationResult{}, false, nil
	}
	before := actorRevisions(st)
	sample, _ := s.sample()
	// Preserve the positive source-loss reason on the target even if the same
	// observation also discovers computer-wide clock discontinuity.
	quarantine(st, a, in.Reason, sample)
	quarantineClock(st, sample)
	if in.Reason == "ordering_unavailable" && a.Health != "order_blocked" {
		a.Health = "order_blocked"
		a.Revision = bump(a.Revision)
	}
	ids := changedActors(st, before)
	return MutationResult{Changed: len(ids) > 0, AffectedIDs: ids}, false, nil
}

func (s *Service) ObserveClock(ctx context.Context, in ClockObservation) (MutationResult, error) {
	if s.store.sqliteOnly {
		return s.observeClockSQLite(ctx, in)
	}
	return s.recoveryMutation(ctx, in.RequestID, "activity.observe_clock", in, func(st *state) (MutationResult, bool, error) {
		before := actorRevisions(st)
		sample, _ := s.sample()
		quarantineClock(st, sample)
		ids := changedActors(st, before)
		return MutationResult{Changed: len(ids) > 0, AffectedIDs: ids}, false, nil
	})
}
func actorRevisions(st *state) map[string]string {
	m := map[string]string{}
	for _, a := range st.Actors {
		m[a.ID] = a.Revision
	}
	return m
}
func changedActors(st *state, before map[string]string) []string {
	ids := []string{}
	seen := map[string]bool{}
	for _, a := range st.Actors {
		if before[a.ID] != a.Revision {
			ids = append(ids, a.ID)
			for _, id := range a.UncertaintyIDs {
				if !seen[id] && st.Uncertainties[id].State == "unresolved" {
					seen[id] = true
					ids = append(ids, id)
				}
			}
		}
	}
	sort.Strings(ids)
	return ids
}
