package activity

import (
	"context"
	"encoding/json"
	"sort"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/identity"
)

func (s *Service) Review(ctx context.Context, in ReviewInput) (ReviewList, error) {
	if in.AccountID != "" && !identity.Valid(in.AccountID) || in.ProjectID != "" && !identity.Valid(in.ProjectID) {
		return ReviewList{}, failure("validation")
	}
	if s.store.sqliteOnly {
		return s.reviewSQLite(ctx, in)
	}
	st, _, err := s.store.read(ctx)
	if err != nil {
		return ReviewList{}, err
	}
	r := ReviewList{ContractVersion: 1, SnapshotRevision: st.Revision, Uncertainties: []Uncertainty{}}
	for _, u := range st.Uncertainties {
		if u.State == "unresolved" && (in.AccountID == "" || in.AccountID == u.Attribution.AccountID) && (in.ProjectID == "" || in.ProjectID == u.Attribution.ProjectID) {
			r.Uncertainties = append(r.Uncertainties, *u)
		}
	}
	sort.Slice(r.Uncertainties, func(i, j int) bool { return r.Uncertainties[i].ID < r.Uncertainties[j].ID })
	return r, nil
}

func validReason(s string) bool {
	if len(s) > 512 || !utf8.ValidString(s) {
		return false
	}
	for _, c := range s {
		if unicode.IsControl(c) {
			return false
		}
	}
	return true
}
func validateRecovery(in RecoveryInput) error {
	if !validUUID(in.UncertaintyID) {
		return failure("validation")
	}
	if in.End == nil && !in.DiscardTail {
		return requiredRecovery("end", "discard_tail")
	}
	if in.End != nil && in.DiscardTail {
		return failure("validation")
	}
	if in.End != nil && (in.End.IsZero() || in.End.Location() != time.UTC || in.End.Year() < 1 || in.End.Year() > 9999) {
		return failure("validation")
	}
	return nil
}
func requiredRecovery(fields ...string) *Error {
	e := failure("input_required")
	e.Details = map[string]any{"required_fields": fields}
	return e
}
func validateRevision(revision string) error {
	if revision == "" {
		return requiredRecovery("if_revision")
	}
	if _, ok := counter(revision); !ok {
		return failure("validation")
	}
	return nil
}
func unresolved(st *state, id string) (*Uncertainty, error) {
	u := st.Uncertainties[id]
	if u == nil {
		return nil, failure("uncertainty_not_found")
	}
	if u.State != "unresolved" {
		return nil, failure("invalid_transition")
	}
	return u, nil
}
func copyState(st *state) *state {
	b, _ := json.Marshal(st)
	var out state
	_ = json.Unmarshal(b, &out)
	return &out
}

// projectedRecovery does not mutate state or mint identities. It uses the same
// bounds and union calculation for the read-only preview and locked resolution.
func projectedRecovery(st *state, in RecoveryInput, sample *ClockSample) (RecoveryPreview, error) {
	u, err := unresolved(st, in.UncertaintyID)
	if err != nil {
		return RecoveryPreview{}, err
	}
	seg := st.Segments[u.SegmentID]
	upper := u.UpperBound
	end := u.LowerBound
	if !in.DiscardTail {
		if sample == nil {
			return RecoveryPreview{}, failure("clock_unavailable")
		}
		now := recoveryCeiling(st, seg, *sample)
		if now.Before(u.LowerBound) {
			return RecoveryPreview{}, failure("clock_conflict")
		}
		// A trustworthy later lifecycle bound cannot be erased by a rolled-back clock.
		if ev := st.UncertaintyEvidence[u.ID]; ev.BoundSample != nil && now.Before(*u.UpperBound) {
			return RecoveryPreview{}, failure("clock_conflict")
		}
		if upper == nil || now.Before(*upper) {
			upper = &now
		}
		end = *in.End
		if end.Before(u.LowerBound) || end.After(*upper) {
			return RecoveryPreview{}, failure("recovery_bounds")
		}
		if !seg.Start.Add(end.Sub(seg.Start)).Equal(end) {
			return RecoveryPreview{}, failure("recovery_bounds")
		}
	}
	// Later work by the same actor is a hard chronological boundary even if an
	// imported/corrupt uncertainty omitted its cap. No recovery rewrites output.
	for _, other := range st.Segments {
		if other.ID != seg.ID && other.Actor.Key == seg.Actor.Key && other.Start.After(seg.Start) && end.After(other.Start) {
			return RecoveryPreview{}, failure("recovery_bounds")
		}
		if other.ID != seg.ID && timerKey(other.Actor.Key.ComputerID, other.Binding.Attribution) == timerKey(seg.Actor.Key.ComputerID, seg.Binding.Attribution) && other.Binding.Attribution != seg.Binding.Attribution {
			otherEnd := other.End
			if otherEnd == nil && other.UncertaintyID != nil {
				otherEnd = st.Uncertainties[*other.UncertaintyID].UpperBound
			}
			if !end.Before(other.Start) && (otherEnd == nil || !seg.Start.After(*otherEnd)) {
				return RecoveryPreview{}, attributionConflict(seg.Binding.Attribution, other.Binding.Attribution)
			}
		}
	}
	for _, old := range st.Intervals {
		if timerKey(old.ComputerID, old.Attribution) == timerKey(seg.Actor.Key.ComputerID, seg.Binding.Attribution) && !seg.Start.After(old.End) && !end.Before(old.Start) {
			return RecoveryPreview{}, failure("clock_conflict")
		}
	}
	p := RecoveryPreview{ContractVersion: 1, SnapshotRevision: st.Revision, Uncertainty: *u, SegmentStart: seg.Start, ConfirmedPrefix: TimeRange{seg.Start, seg.Confirmed}, ProposedEnd: end, AffectedUnionBefore: recoveryUnion(st, seg, "", time.Time{}), AffectedUnionAfter: recoveryUnion(st, seg, seg.ID, end), StillBlockedIDs: []string{}}
	if upper != nil {
		p.DiscardedSuffix = &TimeRange{end, *upper}
	}
	for _, other := range st.Uncertainties {
		if other.ID != u.ID && other.State == "unresolved" && attributionKey(other.Actor.Key.ComputerID, other.Attribution) == attributionKey(seg.Actor.Key.ComputerID, seg.Binding.Attribution) {
			p.StillBlockedIDs = append(p.StillBlockedIDs, other.ID)
		}
	}
	sort.Strings(p.StillBlockedIDs)
	return p, nil
}

func recoveryCeiling(st *state, seg *segment, sample ClockSample) time.Time {
	if t, ok := projectSample(findEpoch(st, seg.EpochID), sample); ok {
		return t
	}
	return sample.WallUTC
}
func recoveryUnion(st *state, target *segment, replace string, end time.Time) []TimeRange {
	rs := []timeRange{}
	for _, seg := range st.Segments {
		if attributionKey(seg.Actor.Key.ComputerID, seg.Binding.Attribution) != attributionKey(target.Actor.Key.ComputerID, target.Binding.Attribution) {
			continue
		}
		e := seg.Confirmed
		if seg.End != nil {
			e = *seg.End
		}
		if seg.ID == replace {
			e = end
		}
		rs = append(rs, timeRange{start: seg.Start, end: e})
	}
	out := []TimeRange{}
	for _, r := range mergeRanges(rs) {
		out = append(out, TimeRange{r.start, r.end})
	}
	return out
}
func (s *Service) Preview(ctx context.Context, in RecoveryInput) (RecoveryPreview, error) {
	if err := validateRecovery(in); err != nil {
		return RecoveryPreview{}, err
	}
	if s.store.sqliteOnly {
		return s.previewSQLite(ctx, in)
	}
	st, _, err := s.store.read(ctx)
	if err != nil {
		return RecoveryPreview{}, err
	}
	if _, err = unresolved(st, in.UncertaintyID); err != nil {
		return RecoveryPreview{}, err
	}
	var sample *ClockSample
	if !in.DiscardTail {
		v, e := s.sample()
		if e != nil {
			return RecoveryPreview{}, e
		}
		sample = &v
	}
	return projectedRecovery(st, in, sample)
}

func (s *Service) Resolve(ctx context.Context, in ResolveInput) (MutationResult, error) {
	input := RecoveryInput{UncertaintyID: in.UncertaintyID, End: in.End, DiscardTail: in.DiscardTail}
	if err := validateRecovery(input); err != nil {
		return MutationResult{}, err
	}
	if err := validateRevision(in.IfRevision); err != nil {
		return MutationResult{}, err
	}
	if !validReason(in.Reason) {
		return MutationResult{}, failure("validation")
	}
	if !in.Confirmed {
		return MutationResult{}, failure("confirmation_required")
	}
	if s.store.sqliteOnly {
		return s.resolveSQLite(ctx, in)
	}
	return s.recoveryMutation(ctx, in.RequestID, "activity.resolve", in, func(st *state) (MutationResult, bool, error) {
		u, err := unresolved(st, in.UncertaintyID)
		if err != nil {
			return MutationResult{}, false, err
		}
		if u.Revision != in.IfRevision {
			return MutationResult{}, false, failure("revision_conflict")
		}
		var sample *ClockSample
		safety := false
		if !in.DiscardTail {
			v, e := s.sample()
			sample = &v
			safety = quarantineClock(st, v)
			if e != nil {
				return MutationResult{}, safety, e
			}
		}
		p, err := projectedRecovery(st, input, sample)
		if err != nil {
			return MutationResult{}, safety, err
		}
		// Keep safety quarantine if finalization rejects the proposal, but never
		// partially commit the attempted resolution or a newly created interval.
		proposal := copyState(st)
		u = proposal.Uncertainties[in.UncertaintyID]
		seg := proposal.Segments[u.SegmentID]
		previous := u.Revision
		u.State = "resolved"
		u.Revision = bump(u.Revision)
		end := p.ProposedEnd
		u.ResolutionEnd = &end
		u.Discarded = in.DiscardTail
		seg.End = &end
		if !in.DiscardTail && u.UpperBound == nil {
			bound := recoveryCeiling(proposal, seg, *sample)
			u.UpperBound = &bound
			ev := proposal.UncertaintyEvidence[u.ID]
			ev.BoundSample = sample
			proposal.UncertaintyEvidence[u.ID] = ev
		}
		if proposal.RecoveryDecisions == nil {
			proposal.RecoveryDecisions = map[string]recoveryDecision{}
		}
		proposal.RecoveryDecisions[u.ID] = recoveryDecision{RequestID: in.RequestID, UncertaintyID: u.ID, PreviousRevision: previous, ResolutionEnd: end, Discarded: in.DiscardTail, Reason: in.Reason, ObservedSample: sample, DiscardedSuffix: p.DiscardedSuffix}
		affected := []string{u.ID}
		a := proposal.Actors[actorKey(u.Actor.Key)]
		if a != nil && a.Ref == u.Actor && a.State == "working" && a.Health != "continuous" && a.SegmentID != nil && *a.SegmentID == seg.ID {
			detach(a)
			affected = append(affected, a.ID)
		}
		if err = finalize(proposal); err != nil {
			return MutationResult{}, safety, err
		}
		*st = *proposal
		return MutationResult{Changed: true, AffectedIDs: affected, EntityRevision: &u.Revision}, false, nil
	})
}
func detach(a *Actor) { a.State = "interrupted"; a.SegmentID = nil; a.Revision = bump(a.Revision) }
