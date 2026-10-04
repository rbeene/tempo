//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"sort"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Plan only the target's connected support closure, all reservation-invalidated
// owners and already pending work. No IDs are minted and no row is changed.
// The concrete proposed segment replaces exactly one owned support; no partial
// legacy state or reducer callback participates in this preflight.
func sqliteRecoveryPlanFinalization(tx *sqliteio.Tx, meta sqliteStoreMeta, target sqliteRecoveryTarget, after sqliteSegmentLocalRow) (bool, error) {
	computer := meta.ComputerID
	candidates := map[string]sqliteFrontierLocalRow{}
	var cursor *sqlitePendingLocalRow
	for {
		pending, found, err := sqliteNextPendingLocal(tx, computer, cursor)
		if err != nil {
			return false, err
		}
		if !found {
			break
		}
		previous := pending
		cursor = &previous
		component, err := sqliteReadFinalizationComponent(tx, computer, pending.ID, nil)
		if err != nil {
			return false, err
		}
		if component.Pending == nil || !sqliteFinalizationPendingMatches(pending, component.Row) {
			return false, failure("state_corrupt")
		}
		if err = sqliteFinalizationFrontierMaximal(tx, component.Row); err != nil {
			return false, err
		}
		candidates[pending.ID] = component.Row
	}
	released, err := sqliteFrontierReservationRowsLocal(tx, sqliteCaptureTimer(computer, target.Uncertainty.Attribution), sqliteReservationWindow{Start: target.Uncertainty.LowerBound, End: target.Uncertainty.UpperBound})
	if err != nil {
		return false, err
	}
	for _, row := range released {
		component, e := sqliteReadFinalizationComponent(tx, computer, row.ID, nil)
		if e != nil {
			return false, e
		}
		if e = sqliteFinalizationFrontierMaximal(tx, component.Row); e != nil {
			return false, e
		}
		candidates[row.ID] = component.Row
	}
	owner, owned, err := sqliteComponentForSegmentLocal(tx, computer, after.ID)
	if err != nil {
		return false, err
	}
	if owned != sqliteFinalizationEligible(target.Segment) {
		return false, failure("state_corrupt")
	}
	consumed := map[string]bool{}
	supports := []sqliteSegmentLocalRow{}
	if owned {
		old, e := sqliteReadFinalizationComponent(tx, computer, owner, nil)
		if e != nil {
			return false, e
		}
		if e = sqliteFinalizationFrontierMaximal(tx, old.Row); e != nil {
			return false, e
		}
		consumed[owner] = true
		for _, row := range old.Segments {
			if row.ID != after.ID {
				supports = append(supports, row)
			}
		}
	}
	if sqliteFinalizationEligible(after) {
		supports = append(supports, after)
	}
	builds, err := sqliteBuildFrontiers(computer, supports)
	if err != nil {
		return false, err
	}
	// Resolving extends a confirmed prefix or retains it. It cannot split its
	// old component, so every touched neighbor joins the one monotone range.
	if len(builds) > 1 {
		return false, failure("state_corrupt")
	}
	if len(builds) == 1 {
		candidate := builds[0]
		for {
			changed := false
			rows, e := sqliteFrontierReservationRowsLocal(tx, sqliteCaptureTimer(computer, candidate.Row.Attribution), sqliteReservationWindow{Start: candidate.Row.Start, End: &candidate.Row.End})
			if e != nil {
				return false, e
			}
			for _, row := range rows {
				if row.Attribution != candidate.Row.Attribution || consumed[row.ID] {
					continue
				}
				old, e := sqliteReadFinalizationComponent(tx, computer, row.ID, nil)
				if e != nil {
					return false, e
				}
				if e = sqliteFinalizationFrontierMaximal(tx, old.Row); e != nil {
					return false, e
				}
				consumed[row.ID] = true
				joined, e := sqliteBuildFrontiers(computer, append(candidate.Segments, old.Segments...))
				if e != nil {
					return false, e
				}
				if len(joined) != 1 {
					return false, failure("state_corrupt")
				}
				candidate = joined[0]
				changed = true
			}
			if !changed {
				break
			}
		}
		for id := range consumed {
			delete(candidates, id)
		}
		candidates[candidate.Row.ID] = candidate.Row
	} else {
		for id := range consumed {
			delete(candidates, id)
		}
	}
	ordered := make([]sqliteFrontierLocalRow, 0, len(candidates))
	for _, row := range candidates {
		ordered = append(ordered, row)
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		ga, gb := attributionKey(computer, a.Attribution), attributionKey(computer, b.Attribution)
		if ga != gb {
			return ga < gb
		}
		if n := sqliteCompareRangeTime(a.Start, b.Start); n != 0 {
			return n < 0
		}
		if n := sqliteCompareRangeTime(a.End, b.End); n != 0 {
			return n < 0
		}
		return a.ID < b.ID
	})
	planned := map[sqliteTimerKey][]sqliteFrontierLocalRow{}
	for _, row := range ordered {
		reserved, e := sqliteRecoveryReserved(tx, meta, row, target.Uncertainty.ID)
		if e != nil {
			return false, e
		}
		if reserved {
			continue
		}
		conflict, e := sqliteFinalizationConflicts(tx, row)
		if e != nil {
			return false, e
		}
		if conflict {
			return true, nil
		}
		timer := sqliteCaptureTimer(computer, row.Attribution)
		for _, prior := range planned[timer] {
			if row.Start.Before(prior.End) && prior.Start.Before(row.End) {
				return true, nil
			}
		}
		planned[timer] = append(planned[timer], row)
	}
	return false, nil
}

// The proposed resolution removes one unresolved reservation. Its stale target
// actor was never an active continuous reservation; any exact detach therefore
// leaves these actual working-head queries unchanged.
func sqliteRecoveryReserved(tx *sqliteio.Tx, meta sqliteStoreMeta, row sqliteFrontierLocalRow, resolvedID string) (bool, error) {
	a := row.Attribution
	stmt, err := tx.Prepare("SELECT a.actor_key,a.segment_id FROM actors AS a JOIN segments AS s ON s.segment_id=a.segment_id WHERE a.computer_id=? AND a.account_id=? AND a.project_id=? AND a.state='working' AND a.health='continuous' AND (s.start_sec,s.start_nsec)<=(?,?) LIMIT 1", sqliteio.Text(meta.ComputerID), sqliteio.Text(a.AccountID), sqliteio.Text(a.ProjectID), sqliteio.Integer(row.End.Unix()), sqliteio.Integer(int64(row.End.Nanosecond())))
	if err != nil {
		return false, err
	}
	key, id, found, err := sqliteFinalizationWorkingPair(stmt)
	if err != nil {
		return false, err
	}
	if found {
		seg, present, e := sqliteReadSegmentLocal(tx, meta.ComputerID, id)
		if e != nil {
			return false, e
		}
		if !present || actorKey(seg.Actor.Key) != key || seg.Start.After(row.End) {
			return false, failure("state_corrupt")
		}
		actor, present, e := sqliteReadActorLocal(tx, meta.ComputerID, seg.Actor.Key)
		if e != nil {
			return false, e
		}
		if !present || actor.Ref != seg.Actor || actor.State != "working" || actor.Health != "continuous" || actor.SegmentID == nil || *actor.SegmentID != id || actor.Attribution.AccountID != a.AccountID || actor.Attribution.ProjectID != a.ProjectID {
			return false, failure("state_corrupt")
		}
		if e = sqliteValidateSelectedCaptureDependencies(tx, meta.ComputerID, meta.Revision, sqliteDependencySelection{ActorKeys: []ActorKey{actor.Ref.Key}}); e != nil {
			return false, e
		}
		return true, nil
	}
	stmt, err = tx.Prepare("SELECT uncertainty_id FROM uncertainties WHERE computer_id=? AND account_id=? AND project_id=? AND state='unresolved' AND uncertainty_id<>? AND upper_bound_sec IS NULL AND (lower_bound_sec,lower_bound_nsec)<=(?,?) LIMIT 1", sqliteio.Text(meta.ComputerID), sqliteio.Text(a.AccountID), sqliteio.Text(a.ProjectID), sqliteio.Text(resolvedID), sqliteio.Integer(row.End.Unix()), sqliteio.Integer(int64(row.End.Nanosecond())))
	if err != nil {
		return false, err
	}
	id, found, err = sqliteFinalizationSelectedID(stmt, false)
	if err != nil {
		return false, err
	}
	if found {
		return sqliteFinalizationUncertaintyReservation(tx, meta.ComputerID, meta.Revision, row, id, false)
	}
	stmt, err = tx.Prepare("SELECT uncertainty_id FROM uncertainties WHERE computer_id=? AND account_id=? AND project_id=? AND state='unresolved' AND uncertainty_id<>? AND (upper_bound_sec,upper_bound_nsec)>=(?,?) AND (lower_bound_sec,lower_bound_nsec)<=(?,?) LIMIT 1", sqliteio.Text(meta.ComputerID), sqliteio.Text(a.AccountID), sqliteio.Text(a.ProjectID), sqliteio.Text(resolvedID), sqliteio.Integer(row.Start.Unix()), sqliteio.Integer(int64(row.Start.Nanosecond())), sqliteio.Integer(row.End.Unix()), sqliteio.Integer(int64(row.End.Nanosecond())))
	if err != nil {
		return false, err
	}
	id, found, err = sqliteFinalizationSelectedID(stmt, false)
	if err != nil || !found {
		return false, err
	}
	return sqliteFinalizationUncertaintyReservation(tx, meta.ComputerID, meta.Revision, row, id, true)
}
