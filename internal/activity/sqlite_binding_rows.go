//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteBindingRow struct {
	ComputerID string
	Snapshot   BindingSnapshot
	Record     *bindingRecord
}

const sqliteBindingColumns = "binding_id,revision,account_id,user_id,project_id,task_id,timezone,computer_id,active,record_present,kind,locator,deleted"

type sqliteEncodedBinding struct {
	row    sqliteBindingRow
	values [13]sqliteio.Value
	charge int64
}

func sqliteReadBinding(tx *sqliteio.Tx, computerID, id string) (sqliteBindingRow, bool, error) {
	if !validUUID(computerID) || !validUUID(id) {
		return sqliteBindingRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteBindingColumns+" FROM bindings WHERE binding_id=?", sqliteio.Text(id))
	if err != nil {
		return sqliteBindingRow{}, false, err
	}
	return sqliteReadBindingStatement(s, computerID, id)
}

func sqliteReadBindingLocation(tx *sqliteio.Tx, computerID, kind, locator string) (sqliteBindingRow, bool, error) {
	if !validUUID(computerID) || !validStoredLocation(kind, locator) {
		return sqliteBindingRow{}, false, failure("validation")
	}
	// Lookup deliberately binds the original bytes, before persistence repair.
	s, err := tx.Prepare("SELECT "+sqliteBindingColumns+" FROM bindings WHERE kind=? AND locator=? AND record_present=1 AND deleted=0",
		sqliteio.Text(kind), sqliteio.Text(locator))
	if err != nil {
		return sqliteBindingRow{}, false, err
	}
	row, found, err := sqliteReadBindingStatement(s, computerID, "")
	if err != nil || !found {
		return row, found, err
	}
	if row.Record == nil || row.Record.Deleted || row.Record.Kind != kind || row.Record.Locator != locator {
		return sqliteBindingRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

func sqliteReadBindingStatement(s *sqliteio.Stmt, computerID, id string) (result sqliteBindingRow, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteBindingRow{}, false
		}
	}()
	row, err := s.Step()
	if err != nil || !row {
		return result, false, err
	}
	if s.ColumnCount() != 13 {
		return result, false, failure("state_corrupt")
	}
	kinds := [13]sqliteio.Kind{sqliteio.TextKind, sqliteio.BlobKind, sqliteio.TextKind, sqliteio.TextKind,
		sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind,
		sqliteio.IntegerKind, sqliteio.IntegerKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.IntegerKind}
	var active, present, deleted int64
	var recordKind, locator string
	for column, expected := range kinds {
		if column >= 10 && present == 0 {
			expected = sqliteio.NullKind
		}
		kind, kindErr := s.Kind(column)
		if kindErr != nil || kind != expected {
			return result, false, failure("state_corrupt")
		}
		if kind == sqliteio.NullKind {
			continue
		}
		if kind == sqliteio.TextKind {
			value, readErr := s.Text(column)
			if readErr != nil {
				return result, false, readErr
			}
			if !utf8.ValidString(value) {
				return result, false, failure("state_corrupt")
			}
			switch column {
			case 0:
				result.Snapshot.ID = value
			case 2:
				result.Snapshot.Attribution.AccountID = value
			case 3:
				result.Snapshot.Attribution.UserID = value
			case 4:
				result.Snapshot.Attribution.ProjectID = value
			case 5:
				result.Snapshot.Attribution.TaskID = value
			case 6:
				result.Snapshot.Attribution.Timezone = value
			case 7:
				result.ComputerID = value
			case 10:
				recordKind = value
			case 11:
				locator = value
			}
		} else if kind == sqliteio.IntegerKind {
			value, readErr := s.Int64(column)
			if readErr != nil {
				return result, false, readErr
			}
			if value != 0 && value != 1 {
				return result, false, failure("state_corrupt")
			}
			switch column {
			case 8:
				active = value
			case 9:
				present = value
			case 12:
				deleted = value
			}
		} else {
			value, readErr := s.Blob(column)
			if readErr != nil {
				return result, false, readErr
			}
			result.Snapshot.Revision, err = sqliteDecodeUint64(value)
			if err != nil {
				return result, false, err
			}
		}
	}
	if present == 1 {
		if active != 1-deleted {
			return result, false, failure("state_corrupt")
		}
		result.Record = &bindingRecord{Snapshot: result.Snapshot, Kind: recordKind, Locator: locator, Deleted: deleted == 1}
	} else if active != 1 {
		return result, false, failure("state_corrupt")
	}
	if !sqliteValidBindingRow(result) || result.ComputerID != computerID || id != "" && result.Snapshot.ID != id {
		return result, false, failure("state_corrupt")
	}
	if row, err = s.Step(); err != nil {
		return result, false, err
	} else if row {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteInsertBinding(tx *sqliteio.Tx, row sqliteBindingRow) (delta int64, err error) {
	encoded, err := sqliteEncodeBinding(row)
	if err != nil {
		return 0, err
	}
	s, err := tx.Prepare("INSERT INTO bindings("+sqliteBindingColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING binding_id", encoded.values[:]...)
	if err != nil {
		return 0, sqliteBindingWriteError(err)
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteBindingReturnedID(s, encoded.row.Snapshot.ID); err != nil {
		return 0, sqliteBindingWriteError(err)
	}
	return encoded.charge, nil
}

func sqliteUpdateBinding(tx *sqliteio.Tx, before, after sqliteBindingRow) (delta int64, err error) {
	if before.ComputerID != after.ComputerID || before.Snapshot.ID != after.Snapshot.ID {
		return 0, failure("validation")
	}
	old, err := sqliteEncodeBinding(before)
	if err != nil {
		return 0, err
	}
	next, err := sqliteEncodeBinding(after)
	if err != nil {
		return 0, err
	}
	s, err := tx.Prepare(`UPDATE bindings SET revision=?,account_id=?,user_id=?,project_id=?,task_id=?,timezone=?,
		active=?,record_present=?,kind=?,locator=?,deleted=?
		WHERE binding_id=? AND revision=? AND account_id=? AND user_id=? AND project_id=? AND task_id=?
		AND timezone=? AND computer_id=? AND active=? AND record_present=? AND kind IS ? AND locator IS ? AND deleted IS ?
		RETURNING binding_id`,
		next.values[1], next.values[2], next.values[3], next.values[4], next.values[5], next.values[6],
		next.values[8], next.values[9], next.values[10], next.values[11], next.values[12],
		old.values[0], old.values[1], old.values[2], old.values[3], old.values[4], old.values[5], old.values[6],
		old.values[7], old.values[8], old.values[9], old.values[10], old.values[11], old.values[12])
	if err != nil {
		return 0, sqliteBindingWriteError(err)
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteBindingReturnedID(s, next.row.Snapshot.ID); err != nil {
		return 0, sqliteBindingWriteError(err)
	}
	// Both charges are checked, nonnegative int64 values, so their difference
	// is within [-MaxInt64, MaxInt64] without a wrapping intermediate sum.
	return next.charge - old.charge, nil
}

func sqliteBindingReturnedID(s *sqliteio.Stmt, id string) error {
	row, err := s.Step()
	if err != nil {
		return err
	}
	if !row || s.ColumnCount() != 1 {
		return failure("state_corrupt")
	}
	kind, err := s.Kind(0)
	if err != nil || kind != sqliteio.TextKind {
		return failure("state_corrupt")
	}
	got, err := s.Text(0)
	if err != nil {
		return err
	}
	if got != id {
		return failure("state_corrupt")
	}
	if row, err = s.Step(); err != nil {
		return err
	} else if row {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteBindingCharge(row sqliteBindingRow) (int64, error) {
	encoded, err := sqliteEncodeBinding(row)
	return encoded.charge, err
}

func sqliteValidBindingRow(row sqliteBindingRow) bool {
	return validUUID(row.ComputerID) && validBinding(row.Snapshot) &&
		(row.Record == nil || row.Record.Snapshot == row.Snapshot && validStoredLocation(row.Record.Kind, row.Record.Locator))
}

func sqliteEncodeBinding(row sqliteBindingRow) (sqliteEncodedBinding, error) {
	if !sqliteValidBindingRow(row) {
		return sqliteEncodedBinding{}, failure("validation")
	}
	// This is the final persistence boundary, after raw identity decisions.
	// Clone the record so replacement never feeds back into the caller's graph.
	row.Snapshot.Attribution.Timezone = sqlitePersistClockString(row.Snapshot.Attribution.Timezone)
	if row.Record != nil {
		record := *row.Record
		record.Snapshot = row.Snapshot
		record.Locator = sqlitePersistClockString(record.Locator)
		if !validStoredLocation(record.Kind, record.Locator) {
			return sqliteEncodedBinding{}, failure("validation")
		}
		row.Record = &record
	}
	revision, err := sqliteEncodeUint64(row.Snapshot.Revision)
	if err != nil {
		return sqliteEncodedBinding{}, err
	}
	a := row.Snapshot.Attribution
	encoded := sqliteEncodedBinding{row: row, values: [13]sqliteio.Value{
		sqliteio.Text(row.Snapshot.ID), sqliteio.Blob(revision[:]), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID),
		sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Text(row.ComputerID),
		sqliteio.Integer(1), sqliteio.Integer(0), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(),
	}}
	texts := []string{row.Snapshot.ID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, row.ComputerID}
	integers, nulls := 2, 3
	if row.Record != nil {
		r := row.Record
		encoded.values[8], encoded.values[9] = sqliteMetaBool(!r.Deleted), sqliteio.Integer(1)
		encoded.values[10], encoded.values[11], encoded.values[12] = sqliteio.Text(r.Kind), sqliteio.Text(r.Locator), sqliteMetaBool(r.Deleted)
		texts = append(texts, r.Kind, r.Locator)
		integers, nulls = 3, 0
	}
	encoded.charge, err = sqliteRowCharge(integers, nulls, texts, [][]byte{revision[:]})
	if err != nil {
		return sqliteEncodedBinding{}, err
	}
	return encoded, nil
}

func sqliteBindingWriteError(err error) error {
	var native *sqliteio.Error
	if errors.As(err, &native) && native.Category == sqliteio.Constraint && (native.Code == 1555 || native.Code == 2067) {
		// Only PRIMARYKEY/UNIQUE at this fixed binding write boundary describe a
		// new identity or active-locator collision. Keep checked engine evidence.
		return errors.Join(failure("validation"), err)
	}
	return err
}
