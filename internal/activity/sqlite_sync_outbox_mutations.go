//go:build (darwin || linux) && (amd64 || arm64)

package activity

import "github.com/rbeene/tempo/internal/activity/sqliteio"

const sqliteSyncOutboxOld = "interval_id=? AND id=? AND revision=? AND state=? AND correlation=? AND entry_id IS ? AND failure_category IS ? AND retry_request_id IS ? AND run_request_id IS ? AND plan_present=?"

func sqliteReadOutboxByIDLocal(tx *sqliteio.Tx, computer, rootID string) (sqliteOutboxLocalRow, bool, error) {
	if !validUUID(computer) || !validUUID(rootID) {
		return sqliteOutboxLocalRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT interval_id,id FROM outbox WHERE id=?", sqliteio.Text(rootID))
	if err != nil {
		return sqliteOutboxLocalRow{}, false, err
	}
	interval, found, err := sqliteSyncOutboxIndexPair(s, rootID)
	if err != nil || !found {
		return sqliteOutboxLocalRow{}, false, err
	}
	// The UNIQUE(id) pair is fully consumed and closed before the existing real
	// interval-keyed scalar reader checks its complete owner and identity scope.
	row, found, err := sqliteReadOutboxLocal(tx, computer, interval)
	if err != nil {
		return sqliteOutboxLocalRow{}, false, err
	}
	if !found || row.ID != rootID {
		return sqliteOutboxLocalRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}
func sqliteSyncOutboxIndexPair(s *sqliteio.Stmt, rootID string) (interval string, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			interval, found = "", false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return "", false, err
	}
	if s.ColumnCount() != 2 {
		return "", false, failure("state_corrupt")
	}
	interval, err = sqliteDependencyText(s, 0)
	if err != nil {
		return "", false, err
	}
	id, err := sqliteDependencyText(s, 1)
	if err != nil {
		return "", false, err
	}
	if !validUUID(interval) || id != rootID {
		return "", false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return "", false, err
	} else if present {
		return "", false, failure("state_corrupt")
	}
	return interval, true, nil
}
func sqliteUpdateOutboxLocal(tx *sqliteio.Tx, computer string, before, after sqliteOutboxLocalRow) (delta int64, err error) {
	if !validUUID(computer) || before.IntervalID != after.IntervalID || before.ID != after.ID || before.Correlation != after.Correlation {
		return 0, failure("validation")
	}
	next, charge, err := sqliteEncodeOutboxLocal(after)
	if err != nil {
		return 0, err
	}
	old, oldCharge, err := sqliteEncodeOutboxLocal(before)
	if err != nil {
		return 0, err
	}
	_, found, err := sqliteReadIntervalLocal(tx, computer, after.IntervalID)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, failure("state_corrupt")
	}
	values := make([]sqliteio.Value, 0, 19)
	values = append(values, next[1:]...)
	values = append(values, old[:]...)
	s, err := tx.Prepare("UPDATE outbox SET id=?,revision=?,state=?,correlation=?,entry_id=?,failure_category=?,retry_request_id=?,run_request_id=?,plan_present=? WHERE "+sqliteSyncOutboxOld+" RETURNING interval_id", values...)
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteLocalReturnedID(s, after.IntervalID); err != nil {
		return 0, err
	}
	return charge - oldCharge, nil
}
