//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// statusSQLite is an inactive read port. Its clock and worker callbacks run
// only after the one owned native snapshot has been checked closed.
func (s *Service) statusSQLite(ctx context.Context) (ActivitySnapshot, error) {
	if ctx == nil || s == nil || s.store == nil {
		return ActivitySnapshot{}, failure("validation")
	}
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	directory, authority, database, err := sqliteLocation(s.store.path)
	if err != nil {
		return ActivitySnapshot{}, err
	}
	budget := s.store.timeout
	if budget == 0 {
		budget = 250 * time.Millisecond
	}
	if budget < 0 || budget > time.Second {
		return ActivitySnapshot{}, failure("validation")
	}
	deadline := time.Now().Add(budget)
	if operationDeadline, ok := ctx.Deadline(); ok && operationDeadline.Before(deadline) {
		deadline = operationDeadline
	}
	facts, found, err := sqliteStatusRead(ctx, directory, authority, database, deadline)
	if err != nil {
		return ActivitySnapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return ActivitySnapshot{}, sqliteCaptureError(err)
	}
	// Unavailable evidence still supplies the ordinary offline observation time.
	sample, _ := s.sample()
	result := sqliteStatusProjection(facts, found, sample)
	result.Worker = s.observedWorker(ctx, result.Worker)
	return result, nil
}

func sqliteStatusRead(ctx context.Context, directory, authority, database string, deadline time.Time) (result sqliteStatusFacts, found bool, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	defer func() {
		cleanup := sqliteCaptureCleanup(c, tx)
		if err != nil || cleanup != nil {
			err = sqliteCaptureError(errors.Join(err, cleanup))
			result, found = sqliteStatusFacts{}, false
		}
	}()
	var kind sqliteio.LinkInspection
	c, kind, err = sqliteio.InspectForLink(ctx, directory, authority, database, deadline)
	if err != nil {
		return sqliteStatusFacts{}, false, err
	}
	if kind == sqliteio.LinkAbsent || kind == sqliteio.LinkPristine {
		if c != nil {
			return sqliteStatusFacts{}, false, failure("state_corrupt")
		}
		return sqliteStatusFacts{}, false, nil
	}
	if kind != sqliteio.LinkWAL || c == nil {
		return sqliteStatusFacts{}, false, failure("state_corrupt")
	}
	tx, err = c.Begin(ctx, sqliteio.Read)
	if err != nil {
		return sqliteStatusFacts{}, false, err
	}
	// Empty catalog is uninitialized; every nonempty catalog must pass the
	// shared exact schema/meta admission used by first Link and capture.
	meta, current, err := sqliteReadLinkSchema(tx, authority, database)
	if err != nil || !current {
		return sqliteStatusFacts{}, false, err
	}
	result, err = sqliteStatusRows(tx, meta)
	if err != nil {
		return sqliteStatusFacts{}, false, err
	}
	return result, true, nil
}

// This is a display projection of complete typed observations, not a partial
// legacy State. The existing range union and project grouping remain shared.
func sqliteStatusProjection(f sqliteStatusFacts, found bool, sample ClockSample) ActivitySnapshot {
	result := ActivitySnapshot{ContractVersion: 1, SnapshotRevision: "0", ObservedAt: sample.WallUTC,
		Projects: []ProjectActivity{}, ProjectTimers: []ProjectTimer{}, Actors: []Actor{},
		Uncertainties: []Uncertainty{}, CaptureReviews: []HostReceipt{}, ClosedIntervals: []Interval{},
		Worker: WorkerStatus{State: "not_installed"}}
	if !found {
		return result
	}
	computer := f.Meta.ComputerID
	result.ComputerID, result.SnapshotRevision = &computer, f.Meta.Revision
	result.SyncEnabled, result.Worker.SyncEnabled = f.Meta.SyncEnabled, f.Meta.SyncEnabled
	result.CaptureReviews = f.Reviews
	projects := make(map[string]*ProjectActivity)
	get := func(computer string, attribution Attribution) *ProjectActivity {
		key := attributionKey(computer, attribution)
		if projects[key] == nil {
			projects[key] = &ProjectActivity{ComputerID: computer, Attribution: attribution,
				ActiveActorRefs: []ActorRef{}, WaitingActorRefs: []ActorRef{}, UnresolvedIDs: []string{},
				ProvisionalUnionNS: "0", ConfirmedClosedNS: "0"}
		}
		return projects[key]
	}
	for _, stored := range f.Actors {
		a := stored
		if a.State == "working" && a.Health == "continuous" {
			why := discontinuity(a.LastEvidence, sample)
			if why == "" && a.SegmentID != nil {
				segment := f.Segments[*a.SegmentID]
				epoch := f.Epochs[segment.EpochID]
				why = discontinuity(epoch.Anchor, sample)
			}
			if why != "" {
				a.Health = "stale"
			}
		}
		result.Actors = append(result.Actors, a)
		p := get(a.Ref.Key.ComputerID, a.Attribution)
		if a.State == "working" && a.Health == "continuous" {
			p.ActiveActorRefs = append(p.ActiveActorRefs, a.Ref)
		} else if !terminal(&a) && a.State != "working" {
			p.WaitingActorRefs = append(p.WaitingActorRefs, a.Ref)
		}
	}
	for _, u := range f.Uncertainties {
		result.Uncertainties = append(result.Uncertainties, u)
		p := get(u.Actor.Key.ComputerID, u.Attribution)
		if u.State == "unresolved" {
			p.UnresolvedIDs = append(p.UnresolvedIDs, u.ID)
			p.NeedsAttentionCount++
		}
	}
	ranges, timerRanges := make(map[string][]timeRange), make(map[string][]timeRange)
	for _, segment := range f.Segments {
		end := segment.Confirmed
		if segment.End != nil {
			end = *segment.End
		} else if segment.UncertaintyID == nil {
			epoch := f.Epochs[segment.EpochID]
			if projected, ok := projectSample(&epoch, sample); ok {
				end = projected
			}
		}
		computer, attribution := segment.Actor.Key.ComputerID, segment.Binding.Attribution
		get(computer, attribution)
		key, timer := attributionKey(computer, attribution), timerKey(computer, attribution)
		span := timeRange{start: segment.Start, end: end}
		ranges[key], timerRanges[timer] = append(ranges[key], span), append(timerRanges[timer], span)
	}
	for key, spans := range ranges {
		projects[key].ProvisionalUnionNS = statusUnionDuration(spans)
	}
	for _, interval := range f.Intervals {
		result.ClosedIntervals = append(result.ClosedIntervals, interval)
		p := get(interval.ComputerID, interval.Attribution)
		n, _ := counter(p.ConfirmedClosedNS)
		d, _ := counter(interval.DurationNS)
		p.ConfirmedClosedNS = durationString(time.Duration(n + d))
	}
	for _, root := range f.Outbox {
		p := get(computer, root.Attribution)
		switch root.State {
		case "queued":
			p.QueuedCount++
			result.Worker.QueuedCount++
		case "submitting":
			result.Worker.SubmittingCount++
		case "synced":
			p.SyncedCount++
		case "unknown":
			p.NeedsAttentionCount++
			result.Worker.UnknownCount++
		case "needs_attention", "rejected":
			p.NeedsAttentionCount++
		}
	}
	keys := make([]string, 0, len(projects))
	for key := range projects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		p := projects[key]
		sort.Slice(p.ActiveActorRefs, func(i, j int) bool { return actorKey(p.ActiveActorRefs[i].Key) < actorKey(p.ActiveActorRefs[j].Key) })
		sort.Slice(p.WaitingActorRefs, func(i, j int) bool { return actorKey(p.WaitingActorRefs[i].Key) < actorKey(p.WaitingActorRefs[j].Key) })
		sort.Strings(p.UnresolvedIDs)
		result.Projects = append(result.Projects, *p)
	}
	sort.Slice(result.Actors, func(i, j int) bool { return result.Actors[i].ID < result.Actors[j].ID })
	sort.Slice(result.Uncertainties, func(i, j int) bool { return result.Uncertainties[i].ID < result.Uncertainties[j].ID })
	sort.Slice(result.CaptureReviews, func(i, j int) bool { return result.CaptureReviews[i].ID < result.CaptureReviews[j].ID })
	sort.Slice(result.ClosedIntervals, func(i, j int) bool { return result.ClosedIntervals[i].Start.Before(result.ClosedIntervals[j].Start) })
	result.ProjectTimers = projectTimers(result.Projects, timerRanges, result.ClosedIntervals)
	return result
}
