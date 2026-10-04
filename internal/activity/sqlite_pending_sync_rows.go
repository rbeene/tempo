//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"math"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const sqlitePendingSyncColumns = "request_id,singleton,kind,effect_committed,snapshot_revision,limit_count,outbox_id,entry_id,if_revision,retry_rejected,confirmed"
const sqlitePendingSyncRootColumns = "request_id,ordinal,outbox_id"

// This is only the fixed pending11/roots3 projection. The JSON reservation
// remains the typed request outcome; selected reads require their exact equality.
type sqlitePendingSyncRow struct {
	ID, Kind, SnapshotRevision    string
	EffectCommitted               bool
	Limit                         *int64
	OutboxID, EntryID, IfRevision *string
	RetryRejected, Confirmed      *bool
	RootIDs                       []string
}

func sqliteProjectPendingSync(id string, p syncReservation) sqlitePendingSyncRow {
	row := sqlitePendingSyncRow{ID: id, SnapshotRevision: p.SnapshotRevision, EffectCommitted: p.EffectCommitted}
	row.RootIDs = make([]string, len(p.RootIDs))
	copy(row.RootIDs, p.RootIDs)
	switch {
	case p.Run != nil:
		row.Kind = "run"
		limit := int64(p.Run.Limit)
		row.Limit = &limit
	case p.Reconcile != nil:
		row.Kind = "reconcile"
		limit, outbox := int64(p.Reconcile.Limit), p.Reconcile.OutboxID
		row.Limit, row.OutboxID = &limit, &outbox
	case p.Resolve != nil:
		row.Kind = "resolve"
		outbox, entry, revision := p.Resolve.OutboxID, p.Resolve.EntryID, p.Resolve.IfRevision
		retry, confirmed := p.Resolve.RetryRejected, p.Resolve.Confirmed
		row.OutboxID, row.EntryID, row.IfRevision = &outbox, &entry, &revision
		row.RetryRejected, row.Confirmed = &retry, &confirmed
	}
	return row
}

func sqliteReadPendingSync(tx *sqliteio.Tx, id string) (sqlitePendingSyncRow, bool, error) {
	row, found, err := sqliteReadPendingSyncScalar(tx, id)
	if err != nil {
		return sqlitePendingSyncRow{}, false, err
	}
	roots, err := sqliteReadPendingSyncRoots(tx, id)
	if err != nil {
		return sqlitePendingSyncRow{}, false, err
	}
	if !found {
		if len(roots) != 0 {
			return sqlitePendingSyncRow{}, false, failure("state_corrupt")
		}
		return sqlitePendingSyncRow{}, false, nil
	}
	row.RootIDs = roots
	return row, true, nil
}

func sqliteReadPendingSyncScalar(tx *sqliteio.Tx, id string) (result sqlitePendingSyncRow, found bool, err error) {
	s, err := tx.Prepare("SELECT "+sqlitePendingSyncColumns+" FROM pending_sync WHERE request_id=?", sqliteio.Text(id))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqlitePendingSyncRow{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	if s.ColumnCount() != 11 {
		return result, false, failure("state_corrupt")
	}
	for column := 0; column < 11; column++ {
		kind, kindErr := s.Kind(column)
		if kindErr != nil {
			return result, false, errors.Join(failure("state_corrupt"), kindErr)
		}
		if kind == sqliteio.NullKind && column >= 5 {
			continue
		}
		expected := sqliteio.TextKind
		switch column {
		case 1, 3, 5, 9, 10:
			expected = sqliteio.IntegerKind
		case 4:
			expected = sqliteio.BlobKind
		}
		if kind != expected {
			return result, false, failure("state_corrupt")
		}
		switch kind {
		case sqliteio.TextKind:
			value, readErr := sqliteHostNormalizationText(s, column)
			if readErr != nil {
				return result, false, readErr
			}
			switch column {
			case 0:
				result.ID = value
			case 2:
				result.Kind = value
			case 6:
				result.OutboxID = &value
			case 7:
				result.EntryID = &value
			case 8:
				result.IfRevision = &value
			}
		case sqliteio.IntegerKind:
			value, readErr := s.Int64(column)
			if readErr != nil {
				return result, false, readErr
			}
			switch column {
			case 1:
				if value != 1 {
					return result, false, failure("state_corrupt")
				}
			case 5:
				if value < 1 || value > 100 {
					return result, false, failure("state_corrupt")
				}
				result.Limit = &value
			default:
				if value != 0 && value != 1 {
					return result, false, failure("state_corrupt")
				}
				flag := value == 1
				switch column {
				case 3:
					result.EffectCommitted = flag
				case 9:
					result.RetryRejected = &flag
				case 10:
					result.Confirmed = &flag
				}
			}
		case sqliteio.BlobKind:
			value, readErr := s.Blob(column)
			if readErr != nil {
				return result, false, readErr
			}
			result.SnapshotRevision, err = sqliteDecodeUint64(value)
			if err != nil {
				return result, false, err
			}
		}
	}
	if result.ID != id || !validUUID(result.ID) || !sqlitePositiveMutationCounter(result.SnapshotRevision) || !sqliteValidPendingProjection(result) {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteValidPendingProjection(row sqlitePendingSyncRow) bool {
	switch row.Kind {
	case "run":
		return !row.EffectCommitted && row.Limit != nil && row.OutboxID == nil && row.EntryID == nil && row.IfRevision == nil && row.RetryRejected == nil && row.Confirmed == nil
	case "reconcile":
		return row.Limit != nil && row.OutboxID != nil && (*row.OutboxID == "" || validUUID(*row.OutboxID)) && row.EntryID == nil && row.IfRevision == nil && row.RetryRejected == nil && row.Confirmed == nil
	case "resolve":
		return row.Limit == nil && row.OutboxID != nil && validUUID(*row.OutboxID) && row.EntryID != nil && row.IfRevision != nil && row.RetryRejected != nil && row.Confirmed != nil && *row.Confirmed
	}
	return false
}

func sqliteReadPendingSyncRoots(tx *sqliteio.Tx, id string) (result []string, err error) {
	s, err := tx.Prepare("SELECT "+sqlitePendingSyncRootColumns+" FROM pending_sync_roots WHERE request_id=? ORDER BY ordinal", sqliteio.Text(id))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = []string{}
	seen := make(map[string]bool)
	for {
		present, stepErr := s.Step()
		if stepErr != nil {
			return nil, stepErr
		}
		if !present {
			return result, nil
		}
		ordinal, root, readErr := sqliteReadLocalChild(s, id, true)
		if readErr != nil {
			return nil, readErr
		}
		if ordinal != int64(len(result)) || seen[root] || len(result) >= 100 {
			return nil, failure("state_corrupt")
		}
		seen[root] = true
		result = append(result, root)
	}
}

func sqlitePendingSyncValues(row sqlitePendingSyncRow) ([11]sqliteio.Value, error) {
	var values [11]sqliteio.Value
	revision, err := sqliteEncodeUint64(row.SnapshotRevision)
	if err != nil {
		return values, err
	}
	values = [11]sqliteio.Value{sqliteio.Text(row.ID), sqliteio.Integer(1), sqliteio.Text(row.Kind),
		sqliteio.Integer(0), sqliteio.Blob(revision[:]), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null()}
	if row.EffectCommitted {
		values[3] = sqliteio.Integer(1)
	}
	if row.Limit != nil {
		values[5] = sqliteio.Integer(*row.Limit)
	}
	if row.OutboxID != nil {
		values[6] = sqliteio.Text(*row.OutboxID)
	}
	if row.EntryID != nil {
		values[7] = sqliteio.Text(*row.EntryID)
	}
	if row.IfRevision != nil {
		values[8] = sqliteio.Text(*row.IfRevision)
	}
	if row.RetryRejected != nil {
		values[9] = sqliteio.Integer(0)
		if *row.RetryRejected {
			values[9] = sqliteio.Integer(1)
		}
	}
	if row.Confirmed != nil {
		values[10] = sqliteio.Integer(0)
		if *row.Confirmed {
			values[10] = sqliteio.Integer(1)
		}
	}
	return values, nil
}

func sqlitePendingSyncCharge(row sqlitePendingSyncRow) (int64, error) {
	revision, err := sqliteEncodeUint64(row.SnapshotRevision)
	if err != nil {
		return 0, err
	}
	texts := []string{row.ID, row.Kind}
	integers, nulls := 2, 0 // singleton and effect; snapshot is the one BLOB8.
	if row.Limit == nil {
		nulls++
	} else {
		integers++
	}
	for _, value := range []*string{row.OutboxID, row.EntryID, row.IfRevision} {
		if value == nil {
			nulls++
		} else {
			texts = append(texts, *value)
		}
	}
	for _, value := range []*bool{row.RetryRejected, row.Confirmed} {
		if value == nil {
			nulls++
		} else {
			integers++
		}
	}
	charge, err := sqliteRowCharge(integers, nulls, texts, [][]byte{revision[:]})
	if err != nil {
		return 0, err
	}
	for _, root := range row.RootIDs {
		n, rootErr := sqliteRowCharge(1, 0, []string{row.ID, root}, nil)
		if rootErr != nil {
			return 0, rootErr
		}
		if n > math.MaxInt64-charge {
			return 0, failure("state_corrupt")
		}
		charge += n
	}
	return charge, nil
}

func sqliteInsertPendingSync(tx *sqliteio.Tx, row sqlitePendingSyncRow) error {
	if err := sqliteInsertPendingSyncScalar(tx, row); err != nil {
		return err
	}
	for ordinal, root := range row.RootIDs {
		if err := sqliteInsertPendingSyncRoot(tx, row.ID, int64(ordinal), root); err != nil {
			return err
		}
	}
	return nil
}

func sqliteInsertPendingSyncScalar(tx *sqliteio.Tx, row sqlitePendingSyncRow) (err error) {
	values, err := sqlitePendingSyncValues(row)
	if err != nil {
		return err
	}
	s, err := tx.Prepare("INSERT INTO pending_sync("+sqlitePendingSyncColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?) RETURNING request_id", values[:]...)
	if err != nil {
		return sqliteMutationInsertError(err, true, true)
	}
	defer func() {
		err = sqliteMutationInsertError(sqliteCloseMetaStatement(s, err), true, true)
	}()
	return sqliteRecoveryReturnedID(s, row.ID)
}

func sqliteInsertPendingSyncRoot(tx *sqliteio.Tx, id string, ordinal int64, root string) (err error) {
	s, err := tx.Prepare("INSERT INTO pending_sync_roots("+sqlitePendingSyncRootColumns+") VALUES(?,?,?) RETURNING "+sqlitePendingSyncRootColumns,
		sqliteio.Text(id), sqliteio.Integer(ordinal), sqliteio.Text(root))
	if err != nil {
		return sqliteMutationInsertError(err, true, true)
	}
	defer func() {
		err = sqliteMutationInsertError(sqliteCloseMetaStatement(s, err), true, true)
	}()
	return sqlitePendingSyncReturnedRoot(s, id, ordinal, root)
}

func sqliteUpdatePendingSync(tx *sqliteio.Tx, before, after sqlitePendingSyncRow) (err error) {
	old, err := sqlitePendingSyncValues(before)
	if err != nil {
		return err
	}
	next, err := sqlitePendingSyncValues(after)
	if err != nil {
		return err
	}
	values := append([]sqliteio.Value{}, next[1:]...)
	values = append(values, old[:]...)
	s, err := tx.Prepare(`UPDATE pending_sync SET singleton=?,kind=?,effect_committed=?,snapshot_revision=?,limit_count=?,outbox_id=?,entry_id=?,if_revision=?,retry_rejected=?,confirmed=?
		WHERE request_id=? AND singleton=? AND kind=? AND effect_committed=? AND snapshot_revision=?
		AND limit_count IS ? AND outbox_id IS ? AND entry_id IS ? AND if_revision IS ? AND retry_rejected IS ? AND confirmed IS ? RETURNING request_id`, values...)
	if err != nil {
		return err
	}
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	return sqliteRecoveryReturnedID(s, after.ID)
}

func sqliteDeletePendingSync(tx *sqliteio.Tx, before sqlitePendingSyncRow) error {
	for ordinal, root := range before.RootIDs {
		if err := sqliteDeletePendingSyncRoot(tx, before.ID, int64(ordinal), root); err != nil {
			return err
		}
	}
	return sqliteDeletePendingSyncScalar(tx, before)
}

func sqliteDeletePendingSyncRoot(tx *sqliteio.Tx, id string, ordinal int64, root string) (err error) {
	s, err := tx.Prepare("DELETE FROM pending_sync_roots WHERE request_id=? AND ordinal=? AND outbox_id=? RETURNING "+sqlitePendingSyncRootColumns,
		sqliteio.Text(id), sqliteio.Integer(ordinal), sqliteio.Text(root))
	if err != nil {
		return err
	}
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	return sqlitePendingSyncReturnedRoot(s, id, ordinal, root)
}

func sqliteDeletePendingSyncScalar(tx *sqliteio.Tx, before sqlitePendingSyncRow) (err error) {
	values, err := sqlitePendingSyncValues(before)
	if err != nil {
		return err
	}
	s, err := tx.Prepare(`DELETE FROM pending_sync WHERE request_id=? AND singleton=? AND kind=? AND effect_committed=? AND snapshot_revision=?
		AND limit_count IS ? AND outbox_id IS ? AND entry_id IS ? AND if_revision IS ? AND retry_rejected IS ? AND confirmed IS ? RETURNING request_id`, values[:]...)
	if err != nil {
		return err
	}
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	return sqliteRecoveryReturnedID(s, before.ID)
}

func sqlitePendingSyncReturnedRoot(s *sqliteio.Stmt, id string, ordinal int64, root string) error {
	present, err := s.Step()
	if err != nil {
		return err
	}
	if !present {
		return failure("state_corrupt")
	}
	storedOrdinal, storedRoot, err := sqliteReadLocalChild(s, id, true)
	if err != nil {
		return err
	}
	if storedOrdinal != ordinal || storedRoot != root {
		return failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return err
	} else if present {
		return failure("state_corrupt")
	}
	return nil
}
