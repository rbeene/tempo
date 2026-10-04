//go:build (darwin || linux) && (amd64 || arm64)

package activity

import "github.com/rbeene/tempo/internal/activity/sqliteio"

// These are the finite Now selectors from the selected-graph contract. Each
// cursor is completely checked and closed before any dependent graph is read.
func sqliteSyncNowRoots(tx *sqliteio.Tx, computer, ceiling string, limit int) ([]string, error) {
	if !sqliteDependencyScope(computer, ceiling) || limit < 1 || limit > 100 {
		return nil, failure("validation")
	}
	s, err := tx.Prepare("SELECT o.interval_id,o.id,i.start_sec,i.start_nsec FROM outbox AS o LEFT JOIN intervals AS i ON i.interval_id=o.interval_id WHERE o.state='queued' OR (o.state='needs_attention' AND o.failure_category IN ('configuration_required','representation') AND NOT EXISTS (SELECT 1 FROM sync_attempts AS a WHERE a.interval_id=o.interval_id)) ORDER BY i.start_sec,i.start_nsec,o.id LIMIT ?", sqliteio.Integer(int64(limit)))
	if err != nil {
		return nil, err
	}
	tuples, err := sqliteSyncNowTuples(s, limit)
	if err != nil {
		return nil, err
	}
	roots := make([]string, 0, len(tuples))
	for _, v := range tuples {
		o, found, err := sqliteReadSyncItem(tx, computer, v.root, ceiling)
		if err != nil {
			return nil, err
		}
		if !found || o.Interval.ID != v.interval || o.Interval.Start.Unix() != v.sec || int64(o.Interval.Start.Nanosecond()) != v.nsec || !syncEligible(o) {
			return nil, failure("state_corrupt")
		}
		roots = append(roots, v.root)
	}
	return roots, nil
}

type sqliteSyncNowTuple struct {
	interval, root string
	sec, nsec      int64
}

func sqliteSyncNowTuples(s *sqliteio.Stmt, limit int) (result []sqliteSyncNowTuple, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = make([]sqliteSyncNowTuple, 0)
	for {
		present, e := s.Step()
		if e != nil {
			return nil, e
		}
		if !present {
			return result, nil
		}
		if s.ColumnCount() != 4 || len(result) == limit {
			return nil, failure("state_corrupt")
		}
		var v sqliteSyncNowTuple
		if v.interval, err = sqliteDependencyText(s, 0); err != nil {
			return nil, err
		}
		if v.root, err = sqliteDependencyText(s, 1); err != nil {
			return nil, err
		}
		if v.sec, err = sqliteLocalRangeInteger(s, 2); err != nil {
			return nil, err
		}
		if v.nsec, err = sqliteLocalRangeInteger(s, 3); err != nil {
			return nil, err
		}
		if !validUUID(v.interval) || !validUUID(v.root) || v.nsec < 0 || v.nsec >= 1000000000 {
			return nil, failure("state_corrupt")
		}
		if len(result) > 0 {
			p := result[len(result)-1]
			if p.sec > v.sec || p.sec == v.sec && (p.nsec > v.nsec || p.nsec == v.nsec && p.root >= v.root) {
				return nil, failure("state_corrupt")
			}
		}
		result = append(result, v)
	}
}

func sqliteSyncPendingRequest(tx *sqliteio.Tx, computer, ceiling string) (sqliteMutationRequestRow, bool, error) {
	if !sqliteDependencyScope(computer, ceiling) {
		return sqliteMutationRequestRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT request_id FROM pending_sync WHERE singleton=1")
	if err != nil {
		return sqliteMutationRequestRow{}, false, err
	}
	id, found, err := sqliteSyncGraphPendingID(s)
	if err != nil || !found {
		return sqliteMutationRequestRow{}, false, err
	}
	row, found, err := sqliteReadMutationRequestLocal(tx, computer, id, ceiling)
	if err != nil {
		return sqliteMutationRequestRow{}, false, err
	}
	if !found || row.Value.PendingSync == nil {
		return sqliteMutationRequestRow{}, false, failure("state_corrupt")
	}
	if err = sqliteValidateSelectedSync(tx, computer, ceiling, row.Value.PendingSync.RootIDs, []string{id}); err != nil {
		return sqliteMutationRequestRow{}, false, err
	}
	return row, true, nil
}

func sqliteSyncRemaining(tx *sqliteio.Tx) (result int, err error) {
	s, err := tx.Prepare("SELECT count(*) FROM outbox WHERE state IN ('queued','submitting','rejected','unknown','needs_attention')")
	if err != nil {
		return 0, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = 0
		}
	}()
	present, err := s.Step()
	if err != nil {
		return 0, err
	}
	if !present || s.ColumnCount() != 1 {
		return 0, failure("state_corrupt")
	}
	n, err := sqliteLocalRangeInteger(s, 0)
	if err != nil {
		return 0, err
	}
	if n < 0 || uint64(n) > uint64(^uint(0)>>1) {
		return 0, failure("state_corrupt")
	}
	present, err = s.Step()
	if err != nil {
		return 0, err
	}
	if present {
		return 0, failure("state_corrupt")
	}
	return int(n), nil
}

func sqliteSyncRunResult(tx *sqliteio.Tx, computer, requestID, nextRevision, stateName string, rootIDs []string, reconcile bool) (SyncRun, error) {
	if !sqliteDependencyScope(computer, nextRevision) || !validUUID(requestID) || (stateName != "complete" && stateName != "interrupted") {
		return SyncRun{}, failure("validation")
	}
	r := SyncRun{ContractVersion: 1, RequestID: requestID, SnapshotRevision: nextRevision, State: stateName, AttemptedIDs: []string{}, ResolvedIDs: []string{}, BlockedIDs: []string{}}
	seen := make(map[string]bool)
	for _, root := range rootIDs {
		if !validUUID(root) || seen[root] {
			return SyncRun{}, failure("validation")
		}
		seen[root] = true
		o, found, err := sqliteReadSyncItem(tx, computer, root, nextRevision)
		if err != nil {
			return SyncRun{}, err
		}
		if !found {
			return SyncRun{}, failure("state_corrupt")
		}
		attempted := false
		if o.Plan != nil && !reconcile {
			for _, p := range o.Plan.Parts {
				for _, a := range p.Attempts {
					if a.RequestID == requestID {
						attempted = true
					}
				}
			}
		}
		if attempted {
			r.AttemptedIDs = append(r.AttemptedIDs, root)
		}
		if o.State == "synced" {
			r.ResolvedIDs = append(r.ResolvedIDs, root)
		} else {
			r.BlockedIDs = append(r.BlockedIDs, root)
		}
	}
	var err error
	r.RemainingCount, err = sqliteSyncRemaining(tx)
	if err != nil {
		return SyncRun{}, err
	}
	return r, nil
}
