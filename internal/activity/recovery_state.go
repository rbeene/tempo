package activity

import (
	"reflect"
	"time"
)

type recoveryDecision struct {
	RequestID        string       `json:"request_id"`
	UncertaintyID    string       `json:"uncertainty_id"`
	PreviousRevision string       `json:"previous_revision"`
	ResolutionEnd    time.Time    `json:"resolution_end"`
	Discarded        bool         `json:"discarded"`
	Reason           string       `json:"reason"`
	ObservedSample   *ClockSample `json:"observed_sample"`
	DiscardedSuffix  *TimeRange   `json:"discarded_suffix"`
}

func segmentEnd(seg *segment) time.Time {
	if seg.End != nil {
		return *seg.End
	}
	return seg.Confirmed
}
func validRecoveryState(st *state) bool {
	for id, u := range st.Uncertainties {
		decision, has := st.RecoveryDecisions[id]
		if u.State == "unresolved" {
			if has || u.ResolutionEnd != nil || u.Discarded {
				return false
			}
			continue
		}
		if !has || decision.UncertaintyID != id || !validUUID(decision.RequestID) || !validReason(decision.Reason) || u.ResolutionEnd == nil || !decision.ResolutionEnd.Equal(*u.ResolutionEnd) || decision.Discarded != u.Discarded {
			return false
		}
		old, ok := counter(decision.PreviousRevision)
		current, _ := counter(u.Revision)
		if !ok || old == 0 || old == ^uint64(0) || current != old+1 {
			return false
		}
		seg := st.Segments[u.SegmentID]
		if seg == nil || seg.End == nil || !seg.End.Equal(*u.ResolutionEnd) || seg.End.Before(seg.Confirmed) {
			return false
		}
		if u.Discarded && !seg.End.Equal(seg.Confirmed) {
			return false
		}
		if u.UpperBound != nil && seg.End.After(*u.UpperBound) {
			return false
		}
		if !u.Discarded {
			if decision.ObservedSample == nil || u.UpperBound == nil {
				return false
			}
			if _, _, ok := sampleValues(*decision.ObservedSample); !ok {
				return false
			}
			ceiling := recoveryCeiling(st, seg, *decision.ObservedSample)
			if ceiling.Before(*seg.End) || ceiling.Before(*u.UpperBound) {
				return false
			}
		}
		if u.UpperBound == nil {
			if decision.DiscardedSuffix != nil {
				return false
			}
		} else if decision.DiscardedSuffix == nil || !decision.DiscardedSuffix.Start.Equal(*seg.End) || !decision.DiscardedSuffix.End.Equal(*u.UpperBound) {
			return false
		}
		receipt, ok := st.Requests[decision.RequestID]
		if !ok || receipt.Operation != "activity.resolve" || receipt.MutationResult == nil || receipt.Error != nil {
			return false
		}
		found := false
		for _, affected := range receipt.MutationResult.AffectedIDs {
			if affected == id {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	for id := range st.RecoveryDecisions {
		if st.Uncertainties[id] == nil {
			return false
		}
	}
	return true
}
func recoveryOperation(op string) bool {
	switch op {
	case "activity.resolve", "activity.interrupt", "activity.observe_source", "activity.observe_clock", "activity.observe_host":
		return true
	}
	return false
}
func validRecoveryReceipt(st *state, id string, r mutationRequest, revision uint64) bool {
	if r.BindingResult != nil {
		return false
	}
	if r.Error != nil {
		if r.MutationResult != nil {
			return false
		}
		switch r.Error.Code {
		case "clock_unavailable", "clock_conflict", "recovery_bounds", "attribution_conflict":
		default:
			return false
		}
		// Only safe, fixed errors are persisted, never arbitrary error details.
		return reflect.DeepEqual(r.Error, failure(r.Error.Code))
	}
	m := r.MutationResult
	if m == nil || m.ContractVersion != 1 || m.RequestID != id || m.AffectedIDs == nil {
		return false
	}
	if n, ok := counter(m.SnapshotRevision); !ok || n == 0 || n > revision {
		return false
	}
	if m.EntityRevision != nil {
		if n, ok := counter(*m.EntityRevision); !ok || n == 0 {
			return false
		}
	}
	seen := map[string]bool{}
	for _, v := range m.AffectedIDs {
		if !validUUID(v) || seen[v] {
			return false
		}
		seen[v] = true
		found := st.Uncertainties[v] != nil
		for _, a := range st.Actors {
			if a == nil {
				return false
			}
			found = found || a.ID == v
		}
		if !found {
			return false
		}
	}
	if !m.Changed && (len(m.AffectedIDs) > 0 || m.EntityRevision != nil) {
		return false
	}
	if r.Operation == "activity.resolve" || r.Operation == "activity.interrupt" {
		return m.Changed && m.EntityRevision != nil && len(m.AffectedIDs) > 0
	}
	return m.EntityRevision == nil
}
