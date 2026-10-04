//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"encoding/json"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqlitePendingLocalRow struct {
	ID, ComputerID string
	Attribution    Attribution
	Start, End     time.Time
}

const sqlitePendingLocalColumns = "component_id,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json"
const sqlitePendingLocalOld = "component_id=? AND group_order=? AND start_sec=? AND start_nsec=? AND start_json=? AND end_sec=? AND end_nsec=? AND end_json=?"

func sqliteEncodePendingLocal(row sqlitePendingLocalRow) ([8]sqliteio.Value, int64, error) {
	var values [8]sqliteio.Value
	if !sqliteValidComponentID(row.ID) || !validUUID(row.ComputerID) || !validAttribution(row.Attribution) {
		return values, 0, failure("validation")
	}
	start, end, err := sqliteEncodePositiveRange(row.Start, row.End)
	if err != nil {
		return values, 0, err
	}
	group := attributionKey(row.ComputerID, row.Attribution)
	values = [8]sqliteio.Value{sqliteio.Text(row.ID), sqliteio.Text(group), sqliteio.Integer(start.Seconds), sqliteio.Integer(start.Nanoseconds), sqliteio.Text(start.JSON), sqliteio.Integer(end.Seconds), sqliteio.Integer(end.Nanoseconds), sqliteio.Text(end.JSON)}
	charge, err := sqliteRowCharge(4, 0, []string{row.ID, group, start.JSON, end.JSON}, nil)
	if err != nil {
		return [8]sqliteio.Value{}, 0, err
	}
	return values, charge, nil
}

func sqlitePendingLocalCharge(row sqlitePendingLocalRow) (int64, error) {
	_, charge, err := sqliteEncodePendingLocal(row)
	return charge, err
}

func sqliteDecodePendingLocal(s *sqliteio.Stmt) (sqlitePendingLocalRow, error) {
	var row sqlitePendingLocalRow
	if s.ColumnCount() != 8 {
		return row, failure("state_corrupt")
	}
	id, err := sqliteHostNormalizationText(s, 0)
	if err != nil {
		return row, err
	}
	group, err := sqliteHostNormalizationText(s, 1)
	if err != nil {
		return row, err
	}
	if !sqliteValidComponentID(id) || len(group) < 38 || group[36] != '/' || !validUUID(group[:36]) {
		return row, failure("state_corrupt")
	}
	// Exact re-encoding enforces the one canonical concrete Attribution grammar,
	// including rejection of unknown, duplicate, omitted, or reordered members.
	row.ID, row.ComputerID = id, group[:36]
	if json.Unmarshal([]byte(group[37:]), &row.Attribution) != nil || !validAttribution(row.Attribution) || attributionKey(row.ComputerID, row.Attribution) != group {
		return sqlitePendingLocalRow{}, failure("state_corrupt")
	}
	row.Start, err = sqliteLocalRangeTime(s, 2)
	if err != nil {
		return sqlitePendingLocalRow{}, err
	}
	row.End, err = sqliteLocalRangeTime(s, 5)
	if err != nil {
		return sqlitePendingLocalRow{}, err
	}
	if !row.End.After(row.Start) {
		return sqlitePendingLocalRow{}, failure("state_corrupt")
	}
	return row, nil
}

func sqliteReadPendingLocalStatement(s *sqliteio.Stmt) (result sqlitePendingLocalRow, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqlitePendingLocalRow{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	result, err = sqliteDecodePendingLocal(s)
	if err != nil {
		return result, false, err
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteReadPendingLocal(tx *sqliteio.Tx, computer, id string) (sqlitePendingLocalRow, bool, error) {
	if !validUUID(computer) || !sqliteValidComponentID(id) {
		return sqlitePendingLocalRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqlitePendingLocalColumns+" FROM pending_finalization WHERE component_id=?", sqliteio.Text(id))
	if err != nil {
		return sqlitePendingLocalRow{}, false, err
	}
	row, found, err := sqliteReadPendingLocalStatement(s)
	if err != nil || !found {
		return row, found, err
	}
	if row.ID != id || row.ComputerID != computer {
		return sqlitePendingLocalRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

func sqliteWritePendingLocal(tx *sqliteio.Tx, computer string, before *sqlitePendingLocalRow, after sqlitePendingLocalRow) (delta int64, err error) {
	if !validUUID(computer) || after.ComputerID != computer {
		return 0, failure("validation")
	}
	next, charge, err := sqliteEncodePendingLocal(after)
	if err != nil {
		return 0, err
	}
	query := "INSERT INTO pending_finalization(" + sqlitePendingLocalColumns + ") VALUES(?,?,?,?,?,?,?,?) RETURNING component_id"
	values := next[:]
	if before != nil {
		if before.ID != after.ID || before.ComputerID != after.ComputerID || before.Attribution != after.Attribution {
			return 0, failure("validation")
		}
		old, oldCharge, encodeErr := sqliteEncodePendingLocal(*before)
		if encodeErr != nil {
			return 0, encodeErr
		}
		query = "UPDATE pending_finalization SET group_order=?,start_sec=?,start_nsec=?,start_json=?,end_sec=?,end_nsec=?,end_json=? WHERE " + sqlitePendingLocalOld + " RETURNING component_id"
		values = append(append(make([]sqliteio.Value, 0, 15), next[1:]...), old[:]...)
		charge -= oldCharge
	}
	s, err := tx.Prepare(query, values...)
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteLocalReturnedID(s, after.ID); err != nil {
		return 0, err
	}
	return charge, nil
}

func sqliteDeletePendingLocal(tx *sqliteio.Tx, computer string, before sqlitePendingLocalRow) (delta int64, err error) {
	if !validUUID(computer) || before.ComputerID != computer {
		return 0, failure("validation")
	}
	values, charge, err := sqliteEncodePendingLocal(before)
	if err != nil {
		return 0, err
	}
	s, err := tx.Prepare("DELETE FROM pending_finalization WHERE "+sqlitePendingLocalOld+" RETURNING component_id", values[:]...)
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteLocalReturnedID(s, before.ID); err != nil {
		return 0, err
	}
	return -charge, nil
}

func sqliteNextPendingLocal(tx *sqliteio.Tx, computer string, after *sqlitePendingLocalRow) (sqlitePendingLocalRow, bool, error) {
	if !validUUID(computer) {
		return sqlitePendingLocalRow{}, false, failure("validation")
	}
	query := "SELECT " + sqlitePendingLocalColumns + " FROM pending_finalization"
	var values []sqliteio.Value
	if after != nil {
		if after.ComputerID != computer {
			return sqlitePendingLocalRow{}, false, failure("validation")
		}
		encoded, _, err := sqliteEncodePendingLocal(*after)
		if err != nil {
			return sqlitePendingLocalRow{}, false, err
		}
		query += " WHERE (group_order,start_sec,start_nsec,end_sec,end_nsec,component_id)>(?,?,?,?,?,?)"
		// Continuation uses the exact persisted coordinates, unlike query points.
		values = []sqliteio.Value{encoded[1], encoded[2], encoded[3], encoded[5], encoded[6], encoded[0]}
	}
	query += " ORDER BY group_order,start_sec,start_nsec,end_sec,end_nsec,component_id LIMIT 1"
	s, err := tx.Prepare(query, values...)
	if err != nil {
		return sqlitePendingLocalRow{}, false, err
	}
	row, found, err := sqliteReadPendingLocalStatement(s)
	if err != nil || !found {
		return row, found, err
	}
	if row.ComputerID != computer {
		return sqlitePendingLocalRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}
