package activity

import (
	"encoding/hex"
	"reflect"
	"sort"
)

func validBinding(b BindingSnapshot) bool {
	n, ok := counter(b.Revision)
	return validUUID(b.ID) && ok && n > 0 && validAttribution(b.Attribution)
}
func validState(st *state) bool {
	if st.SchemaVersion != stateVersion || !validUUID(st.ComputerID) || !validBindingState(st) {
		return false
	}
	revision, ok := counter(st.Revision)
	if !ok || revision == 0 {
		return false
	}
	if st.Bindings == nil || st.Actors == nil || st.Uncertainties == nil || st.Intervals == nil || st.Outbox == nil || st.Segments == nil || st.Receipts == nil || st.EventIDs == nil || st.Epochs == nil || st.UncertaintyEvidence == nil {
		return false
	}
	for id, b := range st.Bindings {
		if id != b.ID || !validBinding(b) {
			return false
		}
	}
	epochs := map[string]*timelineEpoch{}
	for _, ep := range st.Epochs {
		if ep == nil || !validUUID(ep.ID) || epochs[ep.ID] != nil || ep.ComputerID != st.ComputerID || !validAttribution(ep.Attribution) {
			return false
		}
		if _, _, ok := sampleValues(ep.Anchor); !ok {
			return false
		}
		epochs[ep.ID] = ep
	}
	for id, seg := range st.Segments {
		if seg == nil || id != seg.ID || !validUUID(id) || !validRef(seg.Actor) || seg.Actor.Key.ComputerID != st.ComputerID || !validBinding(seg.Binding) || seg.EventReferences == nil {
			return false
		}
		ep := epochs[seg.EpochID]
		if ep == nil || ep.Attribution != seg.Binding.Attribution {
			return false
		}
		start, ok := projectSample(ep, seg.StartSample)
		confirmed, ok2 := projectSample(ep, seg.ConfirmedSample)
		if !ok || !ok2 || !start.Equal(seg.Start) || !confirmed.Equal(seg.Confirmed) || seg.Confirmed.Before(seg.Start) {
			return false
		}
		if seg.End != nil && !seg.End.Equal(seg.Confirmed) && (seg.UncertaintyID == nil || st.Uncertainties[*seg.UncertaintyID] == nil || st.Uncertainties[*seg.UncertaintyID].State != "resolved") {
			return false
		}
		if seg.UncertaintyID != nil {
			u := st.Uncertainties[*seg.UncertaintyID]
			if u == nil || u.SegmentID != id || u.Actor != seg.Actor || u.Attribution != seg.Binding.Attribution {
				return false
			}
		}
	}
	actorIDs := map[string]bool{}
	for key, a := range st.Actors {
		if a == nil || key != actorKey(a.Ref.Key) || !validUUID(a.ID) || actorIDs[a.ID] || !validRef(a.Ref) || a.Ref.Key.ComputerID != st.ComputerID || !validBinding(BindingSnapshot{ID: a.BindingID, Revision: a.BindingRevision, Attribution: a.Attribution}) || a.UncertaintyIDs == nil {
			return false
		}
		actorIDs[a.ID] = true
		if n, ok := counter(a.Revision); !ok || n == 0 {
			return false
		}
		if n, ok := counter(a.Sequence); !ok || n == 0 {
			return false
		}
		switch a.State {
		case "working", "wait_user", "wait_permission", "wait_children", "interrupted", "finished":
		default:
			return false
		}
		switch a.Health {
		case "continuous", "stale", "order_blocked":
		default:
			return false
		}
		if _, _, ok := sampleValues(a.LastEvidence); !ok {
			return false
		}
		if a.Parent != nil && !validRef(*a.Parent) {
			return false
		}
		if a.State == "working" {
			if a.SegmentID == nil {
				return false
			}
			seg := st.Segments[*a.SegmentID]
			if seg == nil || seg.Actor != a.Ref || seg.End != nil {
				return false
			}
		} else if a.SegmentID != nil {
			return false
		}
		for _, id := range a.UncertaintyIDs {
			u := st.Uncertainties[id]
			if u == nil || u.Actor.Key != a.Ref.Key {
				return false
			}
		}
	}
	for id, u := range st.Uncertainties {
		if u == nil || u.ID != id || !validUUID(id) || !validRef(u.Actor) || !validAttribution(u.Attribution) || u.LowerBound.IsZero() {
			return false
		}
		if n, ok := counter(u.Revision); !ok || n == 0 {
			return false
		}
		seg := st.Segments[u.SegmentID]
		if seg == nil || !u.LowerBound.Equal(seg.Confirmed) || u.Actor != seg.Actor {
			return false
		}
		if u.UpperBound != nil && u.UpperBound.Before(u.LowerBound) {
			return false
		}
		if u.State != "unresolved" && u.State != "resolved" {
			return false
		}
		switch u.Reason {
		case "source_lost", "ordering_unavailable", "suspend", "clock_changed", "restart_unknown", "event_gap", "superseded":
		default:
			return false
		}
		evidence, ok := st.UncertaintyEvidence[id]
		if !ok || evidence.Detection.WallUTC.IsZero() || !reflect.DeepEqual(evidence.LastConfirmed, seg.ConfirmedSample) {
			return false
		}
		if (evidence.BoundSample == nil) != (u.UpperBound == nil) {
			return false
		}
		if evidence.BoundSample != nil {
			if _, _, ok := sampleValues(*evidence.BoundSample); !ok {
				return false
			}
		}
	}
	intervals := map[string]Interval{}
	supportsBySegment := map[string]int{}
	rangesByTimer := map[string][]timeRange{}
	for _, in := range st.Intervals {
		if !validUUID(in.ID) || in.ComputerID != st.ComputerID || !validAttribution(in.Attribution) || !in.End.After(in.Start) || len(in.SegmentIDs) == 0 {
			return false
		}
		if _, ok := intervals[in.ID]; ok {
			return false
		}
		intervals[in.ID] = in
		rangesByTimer[timerKey(in.ComputerID, in.Attribution)] = append(rangesByTimer[timerKey(in.ComputerID, in.Attribution)], timeRange{start: in.Start, end: in.End})
		var supports []timeRange
		n, ok := counter(in.DurationNS)
		if !ok || n == 0 || n != uint64(in.End.Sub(in.Start)) {
			return false
		}
		ids := map[string]bool{}
		for _, id := range in.SegmentIDs {
			seg := st.Segments[id]
			if seg == nil || !seg.Finalized || ids[id] || seg.Binding.Attribution != in.Attribution || seg.Start.Before(in.Start) || segmentEnd(seg).After(in.End) {
				return false
			}
			ids[id] = true
			supportsBySegment[id]++
			if supportsBySegment[id] > 1 {
				return false
			}
			supports = append(supports, timeRange{start: seg.Start, end: segmentEnd(seg)})
		}
		merged := mergeRanges(supports)
		if len(merged) != 1 || !merged[0].start.Equal(in.Start) || !merged[0].end.Equal(in.End) {
			return false
		}
	}
	for id, seg := range st.Segments {
		if seg.Finalized {
			if supportsBySegment[id] != 1 {
				return false
			}
		} else if supportsBySegment[id] != 0 {
			return false
		}
	}
	for _, ranges := range rangesByTimer {
		sort.Slice(ranges, func(i, j int) bool { return ranges[i].start.Before(ranges[j].start) })
		for i := 1; i < len(ranges); i++ {
			if ranges[i].start.Before(ranges[i-1].end) {
				return false
			}
		}
	}
	if len(st.UncertaintyEvidence) != len(st.Uncertainties) {
		return false
	}
	if len(st.Outbox) != len(st.Intervals) {
		return false
	}
	for id, o := range st.Outbox {
		in, ok := intervals[id]
		if !ok || !reflect.DeepEqual(in, o.Interval) || !validUUID(o.ID) || o.Correlation != "tempo:"+id {
			return false
		}
		if n, ok := counter(o.Revision); !ok || n == 0 {
			return false
		}
		switch o.State {
		case "queued", "submitting", "synced", "rejected", "unknown", "needs_attention":
		default:
			return false
		}
	}
	if len(st.EventIDs) != len(st.Receipts) {
		return false
	}
	identities := map[string]bool{}
	for id, key := range st.EventIDs {
		if !safeIdentifier(id, 256) || identities[key] {
			return false
		}
		if _, ok := st.Receipts[key]; !ok {
			return false
		}
		identities[key] = true
	}
	for _, r := range st.Receipts {
		if len(r.Fingerprint) != 64 {
			return false
		}
		if _, err := hex.DecodeString(r.Fingerprint); err != nil {
			return false
		}
		if r.Result.ContractVersion != 1 || r.Result.Disposition != "applied" || !validRef(r.Result.Actor) || r.Result.UncertaintyIDs == nil {
			return false
		}
		n, ok := counter(r.Result.SnapshotRevision)
		if !ok || n > revision {
			return false
		}
		if r.Result.SegmentID != nil && st.Segments[*r.Result.SegmentID] == nil {
			return false
		}
	}
	return validRecoveryState(st)
}
