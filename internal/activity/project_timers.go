package activity

import (
	"sort"
	"time"
)

// projectTimers groups attribution epochs for presentation. Timing ranges come
// from the same Status sample and endpoint resolution as the attribution view;
// neither the renderer nor this grouping samples a second clock.
func projectTimers(projects []ProjectActivity, provisional map[string][]timeRange, intervals []Interval) []ProjectTimer {
	grouped := make(map[string]*ProjectTimer)
	for _, p := range projects {
		key := timerKey(p.ComputerID, p.Attribution)
		timer := grouped[key]
		if timer == nil {
			timer = &ProjectTimer{ComputerID: p.ComputerID, AccountID: p.Attribution.AccountID, ProjectID: p.Attribution.ProjectID,
				ActiveActorRefs: []ActorRef{}, WaitingActorRefs: []ActorRef{}, UnresolvedIDs: []string{}, ProvisionalUnionNS: "0", ConfirmedClosedNS: "0"}
			grouped[key] = timer
		}
		timer.ActiveActorRefs = append(timer.ActiveActorRefs, p.ActiveActorRefs...)
		timer.WaitingActorRefs = append(timer.WaitingActorRefs, p.WaitingActorRefs...)
		timer.UnresolvedIDs = append(timer.UnresolvedIDs, p.UnresolvedIDs...)
		timer.QueuedCount += p.QueuedCount
		timer.SyncedCount += p.SyncedCount
		timer.NeedsAttentionCount += p.NeedsAttentionCount
	}
	closed := make(map[string][]timeRange)
	for _, interval := range intervals {
		key := timerKey(interval.ComputerID, interval.Attribution)
		closed[key] = append(closed[key], timeRange{start: interval.Start, end: interval.End})
	}
	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]ProjectTimer, 0, len(keys))
	for _, key := range keys {
		timer := grouped[key]
		timer.ProvisionalUnionNS = statusUnionDuration(provisional[key])
		// Finalization already forbids overlapping closed intervals under timerKey.
		// Reuse range union defensively without changing immutable accounting rows.
		timer.ConfirmedClosedNS = statusUnionDuration(closed[key])
		sort.Slice(timer.ActiveActorRefs, func(i, j int) bool {
			return actorKey(timer.ActiveActorRefs[i].Key) < actorKey(timer.ActiveActorRefs[j].Key)
		})
		sort.Slice(timer.WaitingActorRefs, func(i, j int) bool {
			return actorKey(timer.WaitingActorRefs[i].Key) < actorKey(timer.WaitingActorRefs[j].Key)
		})
		sort.Strings(timer.UnresolvedIDs)
		result = append(result, *timer)
	}
	return result
}

func statusUnionDuration(ranges []timeRange) string {
	var sum time.Duration
	for _, r := range mergeRanges(ranges) {
		sum += r.end.Sub(r.start)
	}
	return durationString(sum)
}
