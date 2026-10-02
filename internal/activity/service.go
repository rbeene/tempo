package activity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type Service struct {
	store   *fileStore
	clock   Clock
	resolve BindingResolver
}

func New(o Options) *Service {
	c := o.Clock
	if c == nil {
		c = nativeClock{}
	}
	return &Service{store: &fileStore{path: o.Path, timeout: o.LockTimeout}, clock: c, resolve: o.ResolveBinding}
}
func (s *Service) sample() (ClockSample, error) {
	v, err := s.clock.Sample()
	if v.WallUTC.IsZero() {
		v.WallUTC = time.Now().UTC()
	}
	v.WallUTC = v.WallUTC.UTC()
	if _, _, ok := sampleValues(v); err != nil || !ok {
		v.Capability = "unavailable"
		v.Epoch = nil
		v.ElapsedNS = nil
		v.AwakeNS = nil
		return v, failure("clock_unavailable")
	}
	return v, nil
}
func baseResult(e Event, revision, disposition string) EventResult {
	return EventResult{ContractVersion: 1, SnapshotRevision: revision, Disposition: disposition, Actor: ActorRef{Key: e.Actor, Generation: e.Generation}, UncertaintyIDs: []string{}}
}
func (s *Service) Ingest(ctx context.Context, e Event) (EventResult, error) {
	if err := validateEvent(e); err != nil {
		return EventResult{}, err
	}
	// Link initialization owns identity. Viewing or unlinked ingress never creates it.
	_, exists, err := s.store.read(ctx)
	if err != nil {
		return EventResult{}, err
	}
	if !exists {
		return baseResult(e, "0", "untracked"), nil
	}
	var result EventResult
	var operationErr error
	err = s.store.update(ctx, func(st *state) (bool, error) {
		var changed bool
		result, changed, operationErr = s.reduce(ctx, st, e)
		if changed {
			return true, nil
		}
		return false, operationErr
	})
	if err != nil {
		return EventResult{}, err
	}
	return result, operationErr
}
func (s *Service) resolveInitial(ctx context.Context, st *state, e Event) (BindingSnapshot, bool, error) {
	if s.resolve != nil {
		b, ok, err := s.resolve(ctx, e)
		if err != nil {
			return BindingSnapshot{}, false, failure("binding_unavailable")
		}
		if ok {
			return b, true, nil
		}
	}
	if e.BindingID != "" || e.CWD != "" {
		return BindingSnapshot{}, false, nil
	}
	if e.Parent != nil {
		p := st.Actors[actorKey(e.Parent.Key)]
		if p != nil && p.Ref.Generation == e.Parent.Generation && !terminal(p) {
			return BindingSnapshot{ID: p.BindingID, Revision: p.BindingRevision, Attribution: p.Attribution}, true, nil
		}
	}
	return BindingSnapshot{}, false, nil
}

// reduce is an already-locked transaction primitive. Future bridges can persist
// source ordering tokens and call it in the same transaction, without reentering Ingest.
func (s *Service) reduce(ctx context.Context, st *state, e Event) (EventResult, bool, error) {
	result := baseResult(e, st.Revision, "applied")
	if e.Actor.ComputerID != st.ComputerID {
		return result, false, failure("validation")
	}
	raw, _ := json.Marshal(e)
	digest := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(digest[:])
	key := eventKey(e)
	if old, ok := st.Receipts[key]; ok {
		if old.Fingerprint != fingerprint {
			return result, false, failure("event_conflict")
		}
		r := old.Result
		r.Disposition = "duplicate"
		return r, false, nil
	}
	if old, ok := st.EventIDs[e.EventID]; ok && old != key {
		return result, false, failure("event_conflict")
	}
	a := st.Actors[actorKey(e.Actor)]
	gen, _ := counter(e.Generation)
	seq, _ := counter(e.Sequence)
	newer := a == nil
	if a != nil {
		g, _ := counter(a.Ref.Generation)
		if gen < g || gen == g && terminal(a) {
			result.Disposition = "stale"
			return result, false, nil
		}
		newer = gen > g
	}
	if newer && (e.Kind != "work" || seq != 1) {
		return result, false, failure("event_gap")
	}
	if (e.BindingID != "" || e.Parent != nil) && (e.Kind != "work" || e.Sequence != "1") {
		return result, false, failure("validation")
	}
	if !newer {
		last, _ := counter(a.Sequence)
		if a.Health == "order_blocked" {
			return result, false, failure("event_gap")
		}
		if seq <= last {
			return result, false, failure("event_conflict")
		}
		if seq != last+1 {
			sample, _ := s.sample()
			quarantine(st, a, "event_gap", sample)
			a.Health = "order_blocked"
			a.Revision = bump(a.Revision)
			if a.SegmentID != nil {
				seg := st.Segments[*a.SegmentID]
				if seg.UncertaintyID != nil {
					ev := st.UncertaintyEvidence[*seg.UncertaintyID]
					ev.MissingFrom = bump(a.Sequence)
					ev.MissingThrough = e.Sequence
					st.UncertaintyEvidence[*seg.UncertaintyID] = ev
				}
			}
			return result, true, failure("event_gap")
		}
	}
	var binding BindingSnapshot
	if newer {
		b, ok, err := s.resolveInitial(ctx, st, e)
		if err != nil {
			return result, false, err
		}
		if !ok {
			result.Disposition = "untracked"
			return result, false, nil
		}
		binding = b
		if !validUUID(b.ID) || !validAttribution(b.Attribution) {
			return result, false, failure("binding_unavailable")
		}
		if _, ok := counter(b.Revision); !ok {
			return result, false, failure("binding_unavailable")
		}
		if e.BindingID != "" && (b.ID != e.BindingID || b.Revision != e.BindingRevision) {
			return result, false, failure("binding_unavailable")
		}
		for _, other := range st.Bindings {
			if timerKey(st.ComputerID, other.Attribution) == timerKey(st.ComputerID, b.Attribution) && other.Attribution != b.Attribution {
				return result, false, failure("attribution_conflict")
			}
		}
	}
	sample, clockErr := s.sample()
	clockChanged := quarantineClock(st, sample)
	if clockErr != nil {
		return result, clockChanged, clockErr
	}
	if newer {
		var oldIDs []string
		var id string
		revision := "1"
		if a != nil {
			quarantine(st, a, "superseded", sample)
			if err := capUncertainties(st, a, sample); err != nil {
				return result, true, err
			}
			oldIDs = append([]string{}, a.UncertaintyIDs...)
			id = a.ID
			revision = bump(a.Revision)
		} else {
			id = newID()
			oldIDs = []string{}
		}
		next := &Actor{ID: id, Revision: revision, Ref: result.Actor, Sequence: e.Sequence, State: "working", Health: "continuous", BindingID: binding.ID, BindingRevision: binding.Revision, Attribution: binding.Attribution, Parent: e.Parent, LastEvidence: sample, UncertaintyIDs: oldIDs}
		if err := openSegment(st, next, sample, key); err != nil {
			return result, clockChanged || a != nil && a.Health != "continuous", err
		}
		a = next
		st.Actors[actorKey(e.Actor)] = a
	} else {
		stale := a.Health != "continuous"
		if stale {
			if err := capUncertainties(st, a, sample); err != nil {
				return result, true, err
			}
		}
		switch e.Kind {
		case "observe_work":
			if a.State != "working" || stale {
				return result, clockChanged || stale, failure("invalid_transition")
			}
			if err := confirmSegment(st, a, sample, key, false); err != nil {
				return result, clockChanged, err
			}
		case "work":
			if a.State != "working" || stale {
				if err := openSegment(st, a, sample, key); err != nil {
					return result, clockChanged || stale, err
				}
				a.State = "working"
				a.Health = "continuous"
			} else if err := confirmSegment(st, a, sample, key, false); err != nil {
				return result, clockChanged, err
			}
		default:
			if a.State == "working" && !stale {
				if err := confirmSegment(st, a, sample, key, true); err != nil {
					return result, clockChanged, err
				}
			}
			a.State = e.Kind
			if e.Kind == "finish" {
				a.State = "finished"
			}
			if e.Kind == "interrupt" {
				a.State = "interrupted"
			}
			a.SegmentID = nil
		}
		a.Sequence = e.Sequence
		a.LastEvidence = sample
		a.Revision = bump(a.Revision)
	}
	if err := finalize(st); err != nil {
		return result, clockChanged, err
	}
	result.SnapshotRevision = bump(st.Revision)
	result.SegmentID = a.SegmentID
	result.UncertaintyIDs = append([]string{}, a.UncertaintyIDs...)
	st.Receipts[key] = eventReceipt{Fingerprint: fingerprint, Result: result}
	st.EventIDs[e.EventID] = key
	return result, true, nil
}
func openSegment(st *state, a *Actor, sample ClockSample, event string) error {
	ep := selectEpoch(st, a.Ref.Key.ComputerID, a.Attribution, sample)
	start, _ := projectSample(ep, sample)
	for _, old := range st.Intervals {
		if timerKey(old.ComputerID, old.Attribution) == timerKey(a.Ref.Key.ComputerID, a.Attribution) && start.Before(old.End) {
			return failure("clock_conflict")
		}
	}
	for _, u := range st.Uncertainties {
		if u.State == "unresolved" && timerKey(u.Actor.Key.ComputerID, u.Attribution) == timerKey(a.Ref.Key.ComputerID, a.Attribution) && u.Attribution != a.Attribution && (u.UpperBound == nil || !start.After(*u.UpperBound)) {
			return failure("attribution_conflict")
		}
	}
	id := newID()
	st.Segments[id] = &segment{ID: id, Actor: a.Ref, Binding: BindingSnapshot{ID: a.BindingID, Revision: a.BindingRevision, Attribution: a.Attribution}, EpochID: ep.ID, StartSample: sample, ConfirmedSample: sample, Start: start, Confirmed: start, EventReferences: []string{event}}
	a.SegmentID = &id
	return nil
}
func confirmSegment(st *state, a *Actor, s ClockSample, event string, close bool) error {
	if a.SegmentID == nil {
		return failure("state_corrupt")
	}
	seg := st.Segments[*a.SegmentID]
	if seg == nil {
		return failure("state_corrupt")
	}
	end, ok := projectSample(findEpoch(st, seg.EpochID), s)
	if !ok || end.Before(seg.Confirmed) {
		return failure("clock_conflict")
	}
	seg.Confirmed = end
	seg.ConfirmedSample = s
	seg.EventReferences = append(seg.EventReferences, event)
	a.LastEvidence = s
	if close {
		seg.End = &end
	}
	return nil
}
