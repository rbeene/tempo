package activity

import (
	"context"
	"sort"
	"time"
)

func (s *Service) Status(ctx context.Context) (ActivitySnapshot, error) {
	st, exists, err := s.store.read(ctx)
	if err != nil {
		return ActivitySnapshot{}, err
	}
	sample, _ := s.sample()
	result := ActivitySnapshot{ContractVersion: 1, SnapshotRevision: st.Revision, ObservedAt: sample.WallUTC, Projects: []ProjectActivity{}, Actors: []Actor{}, Uncertainties: []Uncertainty{}, ClosedIntervals: []Interval{}, Worker: WorkerStatus{State: "not_installed"}}
	if !exists {
		return result, nil
	}
	result.ComputerID = &st.ComputerID
	result.SyncEnabled = st.SyncEnabled
	result.Worker.SyncEnabled = st.SyncEnabled
	projects := map[string]*ProjectActivity{}
	get := func(computer string, a Attribution) *ProjectActivity {
		k := attributionKey(computer, a)
		p := projects[k]
		if p == nil {
			p = &ProjectActivity{ComputerID: computer, Attribution: a, ActiveActorRefs: []ActorRef{}, WaitingActorRefs: []ActorRef{}, UnresolvedIDs: []string{}, ProvisionalUnionNS: "0", ConfirmedClosedNS: "0"}
			projects[k] = p
		}
		return p
	}
	for _, stored := range st.Actors {
		a := *stored
		if a.State == "working" && a.Health == "continuous" && actorDiscontinuity(st, &a, sample) != "" {
			a.Health = "stale"
		}
		result.Actors = append(result.Actors, a)
		p := get(a.Ref.Key.ComputerID, a.Attribution)
		if a.State == "working" && a.Health == "continuous" {
			p.ActiveActorRefs = append(p.ActiveActorRefs, a.Ref)
		} else if !terminal(&a) && a.State != "working" {
			p.WaitingActorRefs = append(p.WaitingActorRefs, a.Ref)
		}
	}
	for _, u := range st.Uncertainties {
		result.Uncertainties = append(result.Uncertainties, *u)
		p := get(u.Actor.Key.ComputerID, u.Attribution)
		if u.State == "unresolved" {
			p.UnresolvedIDs = append(p.UnresolvedIDs, u.ID)
			p.NeedsAttentionCount++
		}
	}
	ranges := map[string][]timeRange{}
	for _, seg := range st.Segments {
		if seg.Finalized {
			continue
		}
		end := seg.Confirmed
		if seg.End != nil {
			end = *seg.End
		} else if seg.UncertaintyID == nil {
			if t, ok := projectSample(findEpoch(st, seg.EpochID), sample); ok {
				end = t
			}
		}
		k := attributionKey(seg.Actor.Key.ComputerID, seg.Binding.Attribution)
		get(seg.Actor.Key.ComputerID, seg.Binding.Attribution)
		ranges[k] = append(ranges[k], timeRange{start: seg.Start, end: end})
	}
	for k, rs := range ranges {
		var sum time.Duration
		for _, r := range mergeRanges(rs) {
			sum += r.end.Sub(r.start)
		}
		projects[k].ProvisionalUnionNS = durationString(sum)
	}
	for _, in := range st.Intervals {
		result.ClosedIntervals = append(result.ClosedIntervals, in)
		p := get(in.ComputerID, in.Attribution)
		n, _ := counter(p.ConfirmedClosedNS)
		d, _ := counter(in.DurationNS)
		p.ConfirmedClosedNS = durationString(time.Duration(n + d))
	}
	for _, o := range st.Outbox {
		p := get(o.Interval.ComputerID, o.Interval.Attribution)
		switch o.State {
		case "queued":
			p.QueuedCount++
			result.Worker.QueuedCount++
		case "synced":
			p.SyncedCount++
		case "unknown":
			p.NeedsAttentionCount++
			result.Worker.UnknownCount++
		case "needs_attention", "rejected":
			p.NeedsAttentionCount++
		}
	}
	keys := []string{}
	for k := range projects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		p := projects[k]
		sort.Slice(p.ActiveActorRefs, func(i, j int) bool { return actorKey(p.ActiveActorRefs[i].Key) < actorKey(p.ActiveActorRefs[j].Key) })
		sort.Slice(p.WaitingActorRefs, func(i, j int) bool { return actorKey(p.WaitingActorRefs[i].Key) < actorKey(p.WaitingActorRefs[j].Key) })
		sort.Strings(p.UnresolvedIDs)
		result.Projects = append(result.Projects, *p)
	}
	sort.Slice(result.Actors, func(i, j int) bool { return result.Actors[i].ID < result.Actors[j].ID })
	sort.Slice(result.Uncertainties, func(i, j int) bool { return result.Uncertainties[i].ID < result.Uncertainties[j].ID })
	sort.Slice(result.ClosedIntervals, func(i, j int) bool { return result.ClosedIntervals[i].Start.Before(result.ClosedIntervals[j].Start) })
	return result, nil
}
