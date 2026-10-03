//go:build (darwin || linux) && (amd64 || arm64)

package activity

import "github.com/rbeene/tempo/internal/activity/sqliteio"

type sqliteFinalizationResult struct {
	Delta          int64
	Selection      sqliteFinalizationSelection
	OperationError *Error
}

func sqliteFinalizationWorkingPair(s *sqliteio.Stmt) (key, id string, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			key = ""
			id = ""
			found = false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return "", "", false, err
	}
	if s.ColumnCount() != 2 {
		return "", "", false, failure("state_corrupt")
	}
	key, err = sqliteDependencyText(s, 0)
	if err != nil {
		return "", "", false, err
	}
	id, err = sqliteDependencyText(s, 1)
	if err != nil {
		return "", "", false, err
	}
	if key == "" || !validUUID(id) {
		return "", "", false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return "", "", false, err
	} else if present {
		return "", "", false, failure("state_corrupt")
	}
	return key, id, true, nil
}

func sqliteFinalizationReserved(tx *sqliteio.Tx, computer, revisionCeiling string, row sqliteFrontierLocalRow) (bool, error) {
	a := row.Attribution
	s, err := tx.Prepare("SELECT a.actor_key,a.segment_id FROM actors AS a JOIN segments AS s ON s.segment_id=a.segment_id WHERE a.computer_id=? AND a.account_id=? AND a.project_id=? AND a.state='working' AND a.health='continuous' AND (s.start_sec,s.start_nsec)<=(?,?) LIMIT 1",
		sqliteio.Text(computer), sqliteio.Text(a.AccountID), sqliteio.Text(a.ProjectID), sqliteio.Integer(row.End.Unix()), sqliteio.Integer(int64(row.End.Nanosecond())))
	if err != nil {
		return false, err
	}
	key, id, found, err := sqliteFinalizationWorkingPair(s)
	if err != nil {
		return false, err
	}
	if found {
		segment, present, readErr := sqliteReadSegmentLocal(tx, computer, id)
		if readErr != nil {
			return false, readErr
		}
		if !present || actorKey(segment.Actor.Key) != key || sqliteCompareRangeTime(segment.Start, row.End) > 0 {
			return false, failure("state_corrupt")
		}
		actor, present, readErr := sqliteReadActorLocal(tx, computer, segment.Actor.Key)
		if readErr != nil {
			return false, readErr
		}
		if !present || actor.Ref != segment.Actor || actor.State != "working" || actor.Health != "continuous" || actor.Attribution.AccountID != a.AccountID || actor.Attribution.ProjectID != a.ProjectID || actor.SegmentID == nil || *actor.SegmentID != id {
			return false, failure("state_corrupt")
		}
		if readErr = sqliteValidateSelectedCaptureDependencies(tx, computer, revisionCeiling, sqliteDependencySelection{ActorKeys: []ActorKey{actor.Ref.Key}, SegmentIDs: []string{id}}); readErr != nil {
			return false, readErr
		}
		return true, nil
	}
	s, err = tx.Prepare("SELECT uncertainty_id FROM uncertainties WHERE computer_id=? AND account_id=? AND project_id=? AND state='unresolved' AND upper_bound_sec IS NULL AND (lower_bound_sec,lower_bound_nsec)<=(?,?) LIMIT 1",
		sqliteio.Text(computer), sqliteio.Text(a.AccountID), sqliteio.Text(a.ProjectID), sqliteio.Integer(row.End.Unix()), sqliteio.Integer(int64(row.End.Nanosecond())))
	if err != nil {
		return false, err
	}
	id, found, err = sqliteFinalizationSelectedID(s, false)
	if err != nil {
		return false, err
	}
	if found {
		return sqliteFinalizationUncertaintyReservation(tx, computer, revisionCeiling, row, id, false)
	}
	s, err = tx.Prepare("SELECT uncertainty_id FROM uncertainties WHERE computer_id=? AND account_id=? AND project_id=? AND state='unresolved' AND (upper_bound_sec,upper_bound_nsec)>=(?,?) AND (lower_bound_sec,lower_bound_nsec)<=(?,?) LIMIT 1",
		sqliteio.Text(computer), sqliteio.Text(a.AccountID), sqliteio.Text(a.ProjectID), sqliteio.Integer(row.Start.Unix()), sqliteio.Integer(int64(row.Start.Nanosecond())), sqliteio.Integer(row.End.Unix()), sqliteio.Integer(int64(row.End.Nanosecond())))
	if err != nil {
		return false, err
	}
	id, found, err = sqliteFinalizationSelectedID(s, false)
	if err != nil || !found {
		return false, err
	}
	return sqliteFinalizationUncertaintyReservation(tx, computer, revisionCeiling, row, id, true)
}

func sqliteFinalizationUncertaintyReservation(tx *sqliteio.Tx, computer, revisionCeiling string, row sqliteFrontierLocalRow, id string, bounded bool) (bool, error) {
	uncertainty, found, err := sqliteReadUncertaintyScalar(tx, computer, id)
	if err != nil {
		return false, err
	}
	if !found || uncertainty.State != "unresolved" || uncertainty.Attribution.AccountID != row.Attribution.AccountID || uncertainty.Attribution.ProjectID != row.Attribution.ProjectID ||
		(uncertainty.UpperBound != nil) != bounded || sqliteCompareRangeTime(uncertainty.LowerBound, row.End) > 0 || bounded && sqliteCompareRangeTime(*uncertainty.UpperBound, row.Start) < 0 {
		return false, failure("state_corrupt")
	}
	if err = sqliteValidateSelectedCaptureDependencies(tx, computer, revisionCeiling, sqliteDependencySelection{UncertaintyIDs: []string{id}}); err != nil {
		return false, err
	}
	return true, nil
}

func sqliteFinalizationConflicts(tx *sqliteio.Tx, row sqliteFrontierLocalRow) (bool, error) {
	timer := sqliteTimerKey{ComputerID: row.ComputerID, AccountID: row.Attribution.AccountID, ProjectID: row.Attribution.ProjectID}
	predecessor, found, err := sqliteIntervalPredecessorLocal(tx, timer, row.Start)
	if err != nil {
		return false, err
	}
	if found && sqliteCompareRangeTime(predecessor.End, row.Start) > 0 {
		return true, nil
	}
	successor, found, err := sqliteIntervalSuccessorLocal(tx, timer, row.Start)
	if err != nil {
		return false, err
	}
	return found && sqliteCompareRangeTime(successor.Start, row.End) < 0, nil
}

// One unit consists of all retained rows, flags and derived removals. A late
// failure is fatal even when previous units completed inside this same Tx.
func sqliteSealFinalizationComponent(tx *sqliteio.Tx, computer string, component sqliteFinalizationComponent) (seal sqliteSealSelection, delta int64, err error) {
	defer func() {
		if err != nil {
			seal = sqliteSealSelection{}
			delta = 0
		}
	}()
	ordinal, err := sqliteNextIntervalOrdinalLocal(tx)
	if err != nil {
		return seal, 0, err
	}
	intervalID, rootID := newID(), newID()
	if intervalID == rootID {
		return seal, 0, failure("validation")
	}
	row := component.Row
	interval := sqliteIntervalLocalRow{ID: intervalID, ComputerID: computer, Attribution: row.Attribution, Start: row.Start, End: row.End, DurationNS: durationString(row.End.Sub(row.Start)), Ordinal: ordinal}
	n, err := sqliteInsertIntervalLocal(tx, computer, interval)
	if err = sqliteFinalizationAdd(&delta, n, err); err != nil {
		return seal, 0, err
	}
	for index, segment := range component.Segments {
		n, err = sqliteAppendIntervalSegmentLocal(tx, computer, intervalID, int64(index), segment.ID)
		if err = sqliteFinalizationAdd(&delta, n, err); err != nil {
			return seal, 0, err
		}
	}
	n, err = sqliteInsertIntervalComponent(tx, computer, intervalID, row.ID)
	if err = sqliteFinalizationAdd(&delta, n, err); err != nil {
		return seal, 0, err
	}
	n, err = sqliteInsertQueuedOutbox(tx, computer, intervalID, rootID)
	if err = sqliteFinalizationAdd(&delta, n, err); err != nil {
		return seal, 0, err
	}
	for _, before := range component.Segments {
		after := before
		after.Finalized = true
		n, err = sqliteWriteSegmentLocal(tx, computer, &before, after)
		if err = sqliteFinalizationAdd(&delta, n, err); err != nil {
			return seal, 0, err
		}
	}
	n, err = sqliteRemoveFinalizationComponent(tx, computer, component)
	if err = sqliteFinalizationAdd(&delta, n, err); err != nil {
		return seal, 0, err
	}
	return sqliteSealSelection{ComponentID: row.ID, IntervalID: intervalID, OutboxID: rootID}, delta, nil
}

// Domain conflicts occur before the failing candidate's first write. The
// returned prefix is uncommitted; only the enclosing operation may retain it.
func sqliteDrainFinalization(tx *sqliteio.Tx, computer, revisionCeiling string) (result sqliteFinalizationResult, err error) {
	defer func() {
		if err != nil {
			result = sqliteFinalizationResult{}
		}
	}()
	if !sqliteDependencyScope(computer, revisionCeiling) {
		return result, failure("validation")
	}
	result.Selection = sqliteEmptyFinalizationSelection()
	var cursor *sqlitePendingLocalRow
	for {
		pending, found, readErr := sqliteNextPendingLocal(tx, computer, cursor)
		if readErr != nil {
			return result, readErr
		}
		if !found {
			return result, nil
		}
		// This exact materialized cursor remains valid after removing its row.
		previous := pending
		cursor = &previous
		component, readErr := sqliteReadFinalizationComponent(tx, computer, pending.ID, nil)
		if readErr != nil {
			return result, readErr
		}
		if component.Pending == nil || !sqliteFinalizationPendingMatches(pending, component.Row) {
			return result, failure("state_corrupt")
		}
		if err = sqliteFinalizationFrontierMaximal(tx, component.Row); err != nil {
			return result, err
		}
		reserved, readErr := sqliteFinalizationReserved(tx, computer, revisionCeiling, component.Row)
		if readErr != nil {
			return result, readErr
		}
		if reserved {
			n, writeErr := sqliteDeletePendingLocal(tx, computer, pending)
			if err = sqliteFinalizationAdd(&result.Delta, n, writeErr); err != nil {
				return result, err
			}
			result.Selection.FrontierIDs = append(result.Selection.FrontierIDs, pending.ID)
			result.Selection.ClearedPendingIDs = append(result.Selection.ClearedPendingIDs, pending.ID)
			continue
		}
		conflict, readErr := sqliteFinalizationConflicts(tx, component.Row)
		if readErr != nil {
			return result, readErr
		}
		if conflict {
			result.OperationError = failure("clock_conflict")
			return result, nil
		}
		seal, n, writeErr := sqliteSealFinalizationComponent(tx, computer, component)
		if err = sqliteFinalizationAdd(&result.Delta, n, writeErr); err != nil {
			return result, err
		}
		result.Selection.NewSeals = append(result.Selection.NewSeals, seal)
		result.Selection.RemovedFrontierIDs = append(result.Selection.RemovedFrontierIDs, pending.ID)
		for _, segment := range component.Segments {
			result.Selection.ChangedSegmentIDs = append(result.Selection.ChangedSegmentIDs, segment.ID)
		}
	}
}
