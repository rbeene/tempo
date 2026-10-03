//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/identity"
)

// This is an incomplete scalar, not an OutboxItem or a validated sync plan.
type sqliteOutboxLocalRow struct {
	IntervalID, ID, Revision, State, Correlation           string
	EntryID, FailureCategory, RetryRequestID, RunRequestID *string
	PlanPresent                                            bool
}

const sqliteOutboxLocalColumns = "interval_id,id,revision,state,correlation,entry_id,failure_category,retry_request_id,run_request_id,plan_present"

func sqliteValidOutboxLocal(row sqliteOutboxLocalRow) bool {
	revision, ok := counter(row.Revision)
	if !validUUID(row.IntervalID) || !validUUID(row.ID) || row.ID == row.IntervalID || !ok || revision == 0 || row.Correlation != "tempo:"+row.IntervalID {
		return false
	}
	switch row.State {
	case "queued", "submitting", "synced", "rejected", "unknown", "needs_attention":
	default:
		return false
	}
	for _, optional := range []*string{row.EntryID, row.FailureCategory, row.RetryRequestID, row.RunRequestID} {
		if optional != nil && !utf8.ValidString(*optional) {
			return false
		}
	}
	return (row.EntryID == nil || identity.Valid(*row.EntryID)) &&
		(row.RetryRequestID == nil || validUUID(*row.RetryRequestID)) &&
		(row.RunRequestID == nil || validUUID(*row.RunRequestID))
}

func sqliteEncodeOutboxLocal(row sqliteOutboxLocalRow) ([10]sqliteio.Value, int64, error) {
	var values [10]sqliteio.Value
	if !sqliteValidOutboxLocal(row) {
		return values, 0, failure("validation")
	}
	revision, err := sqliteEncodeUint64(row.Revision)
	if err != nil {
		return values, 0, failure("validation")
	}
	values = [10]sqliteio.Value{
		sqliteio.Text(row.IntervalID), sqliteio.Text(row.ID), sqliteio.Blob(revision[:]),
		sqliteio.Text(row.State), sqliteio.Text(row.Correlation), sqliteMetaOptional(row.EntryID),
		sqliteMetaOptional(row.FailureCategory), sqliteMetaOptional(row.RetryRequestID),
		sqliteMetaOptional(row.RunRequestID), sqliteMetaBool(row.PlanPresent),
	}
	texts := []string{row.IntervalID, row.ID, row.State, row.Correlation}
	nulls := 0
	for _, optional := range []*string{row.EntryID, row.FailureCategory, row.RetryRequestID, row.RunRequestID} {
		if optional == nil {
			nulls++
		} else {
			texts = append(texts, *optional)
		}
	}
	charge, err := sqliteRowCharge(1, nulls, texts, [][]byte{revision[:]})
	if err != nil {
		return [10]sqliteio.Value{}, 0, err
	}
	return values, charge, nil
}

func sqliteOutboxLocalCharge(row sqliteOutboxLocalRow) (int64, error) {
	_, charge, err := sqliteEncodeOutboxLocal(row)
	return charge, err
}

func sqliteDecodeOutboxLocal(s *sqliteio.Stmt) (sqliteOutboxLocalRow, error) {
	var row sqliteOutboxLocalRow
	if s.ColumnCount() != 10 {
		return row, failure("state_corrupt")
	}
	for _, field := range []struct {
		column int
		target *string
	}{
		{0, &row.IntervalID}, {1, &row.ID}, {3, &row.State}, {4, &row.Correlation},
	} {
		value, err := sqliteHostNormalizationText(s, field.column)
		if err != nil {
			return sqliteOutboxLocalRow{}, err
		}
		*field.target = value
	}
	kind, err := s.Kind(2)
	if err != nil {
		return sqliteOutboxLocalRow{}, errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.BlobKind {
		return sqliteOutboxLocalRow{}, failure("state_corrupt")
	}
	blob, err := s.Blob(2)
	if err != nil {
		return sqliteOutboxLocalRow{}, err
	}
	row.Revision, err = sqliteDecodeUint64(blob)
	if err != nil {
		return sqliteOutboxLocalRow{}, err
	}
	for i, target := range []**string{&row.EntryID, &row.FailureCategory, &row.RetryRequestID, &row.RunRequestID} {
		kind, err := s.Kind(i + 5)
		if err != nil {
			return sqliteOutboxLocalRow{}, errors.Join(failure("state_corrupt"), err)
		}
		if kind == sqliteio.NullKind {
			continue
		}
		value, err := sqliteHostNormalizationText(s, i+5)
		if err != nil {
			return sqliteOutboxLocalRow{}, err
		}
		// A distinct owned string retains NULL versus present-empty semantics.
		*target = &value
	}
	plan, err := sqliteLocalRangeInteger(s, 9)
	if err != nil {
		return sqliteOutboxLocalRow{}, err
	}
	if plan != 0 && plan != 1 {
		return sqliteOutboxLocalRow{}, failure("state_corrupt")
	}
	row.PlanPresent = plan == 1
	if !sqliteValidOutboxLocal(row) {
		return sqliteOutboxLocalRow{}, failure("state_corrupt")
	}
	return row, nil
}

func sqliteReadOutboxLocalStatement(s *sqliteio.Stmt, intervalID string) (result sqliteOutboxLocalRow, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteOutboxLocalRow{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	result, err = sqliteDecodeOutboxLocal(s)
	if err != nil {
		return result, false, err
	}
	if result.IntervalID != intervalID {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

// These two callers use fixed UNIQUE(id) queries. Even a corrupt selected
// identity is decoded and closed before returning the collision refusal.
func sqliteOutboxIDAbsentStatement(s *sqliteio.Stmt, id string) (err error) {
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	present, err := s.Step()
	if err != nil || !present {
		return err
	}
	if s.ColumnCount() != 1 {
		return failure("state_corrupt")
	}
	selected, err := sqliteHostNormalizationText(s, 0)
	if err != nil {
		return err
	}
	if selected != id {
		return failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return err
	} else if present {
		return failure("state_corrupt")
	}
	return failure("state_corrupt")
}

func sqliteReadOutboxLocal(tx *sqliteio.Tx, computer, intervalID string) (sqliteOutboxLocalRow, bool, error) {
	if !validUUID(computer) || !validUUID(intervalID) {
		return sqliteOutboxLocalRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteOutboxLocalColumns+" FROM outbox WHERE interval_id=?", sqliteio.Text(intervalID))
	if err != nil {
		return sqliteOutboxLocalRow{}, false, err
	}
	row, found, err := sqliteReadOutboxLocalStatement(s, intervalID)
	if err != nil || !found {
		return row, found, err
	}
	_, ownerFound, err := sqliteReadIntervalLocal(tx, computer, intervalID)
	if err != nil {
		return sqliteOutboxLocalRow{}, false, err
	}
	if !ownerFound {
		return sqliteOutboxLocalRow{}, false, failure("state_corrupt")
	}
	s, err = tx.Prepare("SELECT id FROM sync_parts WHERE id=?", sqliteio.Text(row.ID))
	if err != nil {
		return sqliteOutboxLocalRow{}, false, err
	}
	if err = sqliteOutboxIDAbsentStatement(s, row.ID); err != nil {
		return sqliteOutboxLocalRow{}, false, err
	}
	s, err = tx.Prepare("SELECT id FROM sync_attempts WHERE id=?", sqliteio.Text(row.ID))
	if err != nil {
		return sqliteOutboxLocalRow{}, false, err
	}
	if err = sqliteOutboxIDAbsentStatement(s, row.ID); err != nil {
		return sqliteOutboxLocalRow{}, false, err
	}
	return row, true, nil
}

func sqliteInsertQueuedOutbox(tx *sqliteio.Tx, computer, intervalID, rootID string) (delta int64, err error) {
	if !validUUID(computer) {
		return 0, failure("validation")
	}
	row := sqliteOutboxLocalRow{IntervalID: intervalID, ID: rootID, Revision: "1", State: "queued", Correlation: "tempo:" + intervalID}
	values, charge, err := sqliteEncodeOutboxLocal(row)
	if err != nil {
		return 0, err
	}
	_, found, err := sqliteReadIntervalLocal(tx, computer, intervalID)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, failure("state_corrupt")
	}
	// The actual cross-family trigger remains authoritative on INSERT. Its
	// native1811 must not be preempted or mislabeled as a UNIQUE violation.
	s, err := tx.Prepare("INSERT INTO outbox("+sqliteOutboxLocalColumns+") VALUES(?,?,?,?,?,?,?,?,?,?) RETURNING interval_id,id", values[:]...)
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteHostNormalizationReturnedKeys(s, intervalID, rootID); err != nil {
		return 0, err
	}
	return charge, nil
}
