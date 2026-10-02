package activity

import (
	"encoding/json"
	"sort"
	"time"
)

type timeRange struct {
	start, end time.Time
	ids        []string
}

func mergeRanges(ranges []timeRange) []timeRange {
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].start.Equal(ranges[j].start) {
			return ranges[i].end.Before(ranges[j].end)
		}
		return ranges[i].start.Before(ranges[j].start)
	})
	out := []timeRange{}
	for _, r := range ranges {
		if !r.end.After(r.start) {
			continue
		}
		if len(out) == 0 || r.start.After(out[len(out)-1].end) {
			out = append(out, r)
			continue
		}
		last := &out[len(out)-1]
		if r.end.After(last.end) {
			last.end = r.end
		}
		last.ids = append(last.ids, r.ids...)
	}
	return out
}
func attributionKey(computer string, a Attribution) string {
	b, _ := json.Marshal(a)
	return computer + "/" + string(b)
}
func reserved(st *state, key string, r timeRange) bool {
	for _, a := range st.Actors {
		if a.State != "working" || a.Health != "continuous" || a.SegmentID == nil || timerKey(a.Ref.Key.ComputerID, a.Attribution) != key {
			continue
		}
		seg := st.Segments[*a.SegmentID]
		if seg != nil && !seg.Start.After(r.end) {
			return true
		}
	}
	for _, u := range st.Uncertainties {
		if u.State != "unresolved" || timerKey(u.Actor.Key.ComputerID, u.Attribution) != key {
			continue
		}
		if !u.LowerBound.After(r.end) && (u.UpperBound == nil || !u.UpperBound.Before(r.start)) {
			return true
		}
	}
	return false
}
func finalize(st *state) error {
	groups := map[string][]timeRange{}
	examples := map[string]*segment{}
	for _, seg := range st.Segments {
		if seg.Finalized {
			continue
		}
		end := seg.Confirmed
		if seg.End != nil {
			end = *seg.End
		} else if seg.UncertaintyID == nil {
			continue
		}
		k := attributionKey(seg.Actor.Key.ComputerID, seg.Binding.Attribution)
		groups[k] = append(groups[k], timeRange{seg.Start, end, []string{seg.ID}})
		examples[k] = seg
	}
	keys := []string{}
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		seg := examples[k]
		tk := timerKey(seg.Actor.Key.ComputerID, seg.Binding.Attribution)
		for _, r := range mergeRanges(groups[k]) {
			if reserved(st, tk, r) {
				continue
			}
			for _, old := range st.Intervals {
				if timerKey(old.ComputerID, old.Attribution) == tk && r.start.Before(old.End) && r.end.After(old.Start) {
					return failure("clock_conflict")
				}
			}
			sort.Strings(r.ids)
			in := Interval{ID: newID(), ComputerID: seg.Actor.Key.ComputerID, Attribution: seg.Binding.Attribution, Start: r.start, End: r.end, DurationNS: durationString(r.end.Sub(r.start)), SegmentIDs: r.ids}
			st.Intervals = append(st.Intervals, in)
			st.Outbox[in.ID] = OutboxItem{ID: newID(), Revision: "1", Interval: in, State: "queued", Correlation: "tempo:" + in.ID}
			for _, id := range r.ids {
				st.Segments[id].Finalized = true
			}
		}
	}
	return nil
}
