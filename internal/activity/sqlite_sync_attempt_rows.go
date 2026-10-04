//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/identity"
)

type sqliteSyncAttemptLocalRow struct {
	IntervalID           string
	PartOrdinal, Ordinal int64
	Value                SyncAttempt
}

const sqliteSyncAttemptLocalColumns = "interval_id,part_ordinal,ordinal,request_id,id,number,state,entry_id,failure_category"
const sqliteSyncAttemptLocalOld = "interval_id=? AND part_ordinal=? AND ordinal=? AND request_id=? AND id=? AND number=? AND state=? AND entry_id IS ? AND failure_category IS ?"

func sqliteValidSyncAttemptLocal(row sqliteSyncAttemptLocalRow) bool {
	v := row.Value
	return validUUID(row.IntervalID) && row.PartOrdinal >= 0 && row.PartOrdinal < 100 && row.Ordinal >= 0 && row.Ordinal < math.MaxInt64 &&
		validUUID(v.RequestID) && validUUID(v.ID) && v.Number == strconv.FormatInt(row.Ordinal+1, 10) && validSyncPartState(v.State) && v.State != "queued" &&
		(v.EntryID == nil || identity.Valid(*v.EntryID)) && (v.FailureCategory == nil || utf8.ValidString(*v.FailureCategory))
}
func sqliteEncodeSyncAttemptLocal(row sqliteSyncAttemptLocalRow) ([9]sqliteio.Value, int64, error) {
	var values [9]sqliteio.Value
	if !sqliteValidSyncAttemptLocal(row) {
		return values, 0, failure("validation")
	}
	v := row.Value
	values = [9]sqliteio.Value{sqliteio.Text(row.IntervalID), sqliteio.Integer(row.PartOrdinal), sqliteio.Integer(row.Ordinal), sqliteio.Text(v.RequestID), sqliteio.Text(v.ID), sqliteio.Text(v.Number), sqliteio.Text(v.State), sqliteMetaOptional(v.EntryID), sqliteMetaOptional(v.FailureCategory)}
	texts := []string{row.IntervalID, v.RequestID, v.ID, v.Number, v.State}
	nulls := 0
	for _, s := range []*string{v.EntryID, v.FailureCategory} {
		if s == nil {
			nulls++
		} else {
			texts = append(texts, *s)
		}
	}
	charge, err := sqliteRowCharge(2, nulls, texts, nil)
	if err != nil {
		return [9]sqliteio.Value{}, 0, err
	}
	return values, charge, nil
}
func sqliteSyncAttemptLocalCharge(row sqliteSyncAttemptLocalRow) (int64, error) {
	_, charge, err := sqliteEncodeSyncAttemptLocal(row)
	return charge, err
}
func sqliteDecodeSyncAttemptLocal(s *sqliteio.Stmt) (sqliteSyncAttemptLocalRow, error) {
	var row sqliteSyncAttemptLocalRow
	if s.ColumnCount() != 9 {
		return row, failure("state_corrupt")
	}
	for _, field := range []struct {
		column int
		target *string
	}{{0, &row.IntervalID}, {3, &row.Value.RequestID}, {4, &row.Value.ID}, {5, &row.Value.Number}, {6, &row.Value.State}} {
		value, err := sqliteDependencyText(s, field.column)
		if err != nil {
			return sqliteSyncAttemptLocalRow{}, err
		}
		*field.target = value
	}
	var err error
	row.PartOrdinal, err = sqliteLocalRangeInteger(s, 1)
	if err != nil {
		return sqliteSyncAttemptLocalRow{}, err
	}
	row.Ordinal, err = sqliteLocalRangeInteger(s, 2)
	if err != nil {
		return sqliteSyncAttemptLocalRow{}, err
	}
	row.Value.EntryID, err = sqliteSyncOptionalText(s, 7)
	if err != nil {
		return sqliteSyncAttemptLocalRow{}, err
	}
	row.Value.FailureCategory, err = sqliteSyncOptionalText(s, 8)
	if err != nil {
		return sqliteSyncAttemptLocalRow{}, err
	}
	if !sqliteValidSyncAttemptLocal(row) {
		return sqliteSyncAttemptLocalRow{}, failure("state_corrupt")
	}
	return row, nil
}
func sqliteRequireSyncPartLocal(tx *sqliteio.Tx, computer, interval string, part int64) error {
	_, found, err := sqliteReadSyncPartLocal(tx, computer, interval, part)
	if err != nil {
		return err
	}
	if !found {
		return failure("state_corrupt")
	}
	return nil
}
func sqliteSyncAttemptsLocal(tx *sqliteio.Tx, computer, interval string, part int64) ([]sqliteSyncAttemptLocalRow, error) {
	if !validUUID(computer) || !validUUID(interval) || part < 0 || part >= 100 {
		return nil, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteSyncAttemptLocalColumns+" FROM sync_attempts WHERE interval_id=? AND part_ordinal=? ORDER BY ordinal", sqliteio.Text(interval), sqliteio.Integer(part))
	if err != nil {
		return nil, err
	}
	rows, err := sqliteSyncAttemptsStatement(s, interval, part)
	if err != nil {
		return nil, err
	}
	if err = sqliteRequireSyncPartLocal(tx, computer, interval, part); err != nil {
		return nil, err
	}
	return rows, nil
}
func sqliteSyncAttemptsStatement(s *sqliteio.Stmt, interval string, part int64) (result []sqliteSyncAttemptLocalRow, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = make([]sqliteSyncAttemptLocalRow, 0)
	for {
		present, e := s.Step()
		if e != nil {
			return nil, e
		}
		if !present {
			return result, nil
		}
		row, e := sqliteDecodeSyncAttemptLocal(s)
		if e != nil {
			return nil, e
		}
		if row.IntervalID != interval || row.PartOrdinal != part || row.Ordinal != int64(len(result)) {
			return nil, failure("state_corrupt")
		}
		result = append(result, row)
	}
}
func sqliteLastSyncAttemptStatement(s *sqliteio.Stmt, interval string, part int64) (result sqliteSyncAttemptLocalRow, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteSyncAttemptLocalRow{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	result, err = sqliteDecodeSyncAttemptLocal(s)
	if err != nil {
		return result, false, err
	}
	if result.IntervalID != interval || result.PartOrdinal != part {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}
func sqliteAppendSyncAttempt(tx *sqliteio.Tx, computer string, row sqliteSyncAttemptLocalRow) (delta int64, err error) {
	if !validUUID(computer) {
		return 0, failure("validation")
	}
	values, charge, err := sqliteEncodeSyncAttemptLocal(row)
	if err != nil {
		return 0, err
	}
	if row.Value.State != "submitting" || row.Value.EntryID != nil || row.Value.FailureCategory != nil {
		return 0, failure("validation")
	}
	if err = sqliteRequireSyncPartLocal(tx, computer, row.IntervalID, row.PartOrdinal); err != nil {
		return 0, err
	}
	s, err := tx.Prepare("SELECT "+sqliteSyncAttemptLocalColumns+" FROM sync_attempts WHERE interval_id=? AND part_ordinal=? ORDER BY ordinal DESC LIMIT 1", sqliteio.Text(row.IntervalID), sqliteio.Integer(row.PartOrdinal))
	if err != nil {
		return 0, err
	}
	last, found, err := sqliteLastSyncAttemptStatement(s, row.IntervalID, row.PartOrdinal)
	if err != nil {
		return 0, err
	}
	ordinal := int64(0)
	if found {
		if last.Ordinal == math.MaxInt64-1 {
			return 0, failure("validation")
		}
		ordinal = last.Ordinal + 1
	}
	if row.Ordinal != ordinal {
		return 0, failure("validation")
	}
	s, err = tx.Prepare("INSERT INTO sync_attempts("+sqliteSyncAttemptLocalColumns+") VALUES(?,?,?,?,?,?,?,?,?) RETURNING interval_id,part_ordinal,ordinal", values[:]...)
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteSyncAttemptReturnedKey(s, row.IntervalID, row.PartOrdinal, row.Ordinal); err != nil {
		return 0, err
	}
	return charge, nil
}
func sqliteUpdateSyncAttempt(tx *sqliteio.Tx, computer string, before, after sqliteSyncAttemptLocalRow) (delta int64, err error) {
	if !validUUID(computer) {
		return 0, failure("validation")
	}
	next, charge, err := sqliteEncodeSyncAttemptLocal(after)
	if err != nil {
		return 0, err
	}
	old, oldCharge, err := sqliteEncodeSyncAttemptLocal(before)
	if err != nil {
		return 0, err
	}
	if before.IntervalID != after.IntervalID || before.PartOrdinal != after.PartOrdinal || before.Ordinal != after.Ordinal || before.Value.RequestID != after.Value.RequestID || before.Value.ID != after.Value.ID || before.Value.Number != after.Value.Number {
		return 0, failure("validation")
	}
	if err = sqliteRequireSyncPartLocal(tx, computer, after.IntervalID, after.PartOrdinal); err != nil {
		return 0, err
	}
	s, err := tx.Prepare("SELECT "+sqliteSyncAttemptLocalColumns+" FROM sync_attempts WHERE interval_id=? AND part_ordinal=? ORDER BY ordinal DESC LIMIT 1", sqliteio.Text(after.IntervalID), sqliteio.Integer(after.PartOrdinal))
	if err != nil {
		return 0, err
	}
	last, found, err := sqliteLastSyncAttemptStatement(s, after.IntervalID, after.PartOrdinal)
	if err != nil {
		return 0, err
	}
	if !found || last.Ordinal != after.Ordinal {
		return 0, failure("state_corrupt")
	}
	values := make([]sqliteio.Value, 0, 15)
	values = append(values, next[3:]...)
	values = append(values, old[:]...)
	s, err = tx.Prepare("UPDATE sync_attempts SET request_id=?,id=?,number=?,state=?,entry_id=?,failure_category=? WHERE "+sqliteSyncAttemptLocalOld+" RETURNING interval_id,part_ordinal,ordinal", values...)
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteSyncAttemptReturnedKey(s, after.IntervalID, after.PartOrdinal, after.Ordinal); err != nil {
		return 0, err
	}
	return charge - oldCharge, nil
}
func sqliteSyncAttemptReturnedKey(s *sqliteio.Stmt, interval string, part, ordinal int64) error {
	present, err := s.Step()
	if err != nil {
		return err
	}
	if !present || s.ColumnCount() != 3 {
		return failure("state_corrupt")
	}
	owner, err := sqliteDependencyText(s, 0)
	if err != nil {
		return err
	}
	actualPart, err := sqliteLocalRangeInteger(s, 1)
	if err != nil {
		return err
	}
	actualOrdinal, err := sqliteLocalRangeInteger(s, 2)
	if err != nil {
		return err
	}
	if owner != interval || actualPart != part || actualOrdinal != ordinal {
		return failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return err
	} else if present {
		return failure("state_corrupt")
	}
	return nil
}
