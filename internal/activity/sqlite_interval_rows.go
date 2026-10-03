//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"math"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteIntervalLocalRow struct {
	ID, ComputerID string
	Attribution    Attribution
	Start, End     time.Time
	DurationNS     string
	Ordinal        int64
}

const sqliteIntervalLocalColumns = "interval_id,computer_id,account_id,user_id,project_id,task_id,timezone,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,duration_ns,creation_ordinal"

func sqliteEncodeIntervalLocal(row sqliteIntervalLocalRow) ([16]sqliteio.Value, int64, error) {
	var values [16]sqliteio.Value
	duration, ok := counter(row.DurationNS)
	if !validUUID(row.ID) || row.Ordinal < 0 || !ok || duration == 0 || !row.End.After(row.Start) || duration != uint64(row.End.Sub(row.Start)) {
		return values, 0, failure("validation")
	}
	start, end, err := sqliteEncodePositiveRange(row.Start, row.End)
	if err != nil {
		return values, 0, err
	}
	// Sub deliberately saturates for very large positive ranges. Persistence may
	// move sub-minute offsets, but must never silently change billed duration.
	finalStart := time.Unix(start.Seconds, start.Nanoseconds)
	finalEnd := time.Unix(end.Seconds, end.Nanoseconds)
	if duration != uint64(finalEnd.Sub(finalStart)) {
		return values, 0, failure("validation")
	}
	prefix, _, err := sqliteEncodeRangeColumns(row.ID, row.ComputerID, row.Attribution, row.Start, row.End)
	if err != nil {
		return values, 0, err
	}
	copy(values[:14], prefix[:])
	encoded, err := sqliteEncodeUint64(row.DurationNS)
	if err != nil {
		return [16]sqliteio.Value{}, 0, failure("validation")
	}
	values[14], values[15] = sqliteio.Blob(encoded[:]), sqliteio.Integer(row.Ordinal)
	a := row.Attribution
	charge, err := sqliteRowCharge(5, 0, []string{row.ID, row.ComputerID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, attributionKey(row.ComputerID, a), start.JSON, end.JSON}, [][]byte{encoded[:]})
	if err != nil {
		return [16]sqliteio.Value{}, 0, err
	}
	return values, charge, nil
}

func sqliteIntervalLocalCharge(row sqliteIntervalLocalRow) (int64, error) {
	_, charge, err := sqliteEncodeIntervalLocal(row)
	return charge, err
}

func sqliteDecodeIntervalLocal(s *sqliteio.Stmt) (sqliteIntervalLocalRow, error) {
	if s.ColumnCount() != 16 {
		return sqliteIntervalLocalRow{}, failure("state_corrupt")
	}
	prefix, err := sqliteReadRangeColumns(s)
	if err != nil {
		return sqliteIntervalLocalRow{}, err
	}
	if !validUUID(prefix.ID) {
		return sqliteIntervalLocalRow{}, failure("state_corrupt")
	}
	kind, err := s.Kind(14)
	if err != nil {
		return sqliteIntervalLocalRow{}, errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.BlobKind {
		return sqliteIntervalLocalRow{}, failure("state_corrupt")
	}
	blob, err := s.Blob(14)
	if err != nil {
		return sqliteIntervalLocalRow{}, err
	}
	duration, err := sqliteDecodeUint64(blob)
	if err != nil {
		return sqliteIntervalLocalRow{}, err
	}
	ordinal, err := sqliteLocalRangeInteger(s, 15)
	if err != nil {
		return sqliteIntervalLocalRow{}, err
	}
	n, ok := counter(duration)
	if !ok || n == 0 || n != uint64(prefix.End.Sub(prefix.Start)) || ordinal < 0 {
		return sqliteIntervalLocalRow{}, failure("state_corrupt")
	}
	return sqliteIntervalLocalRow{ID: prefix.ID, ComputerID: prefix.ComputerID, Attribution: prefix.Attribution, Start: prefix.Start, End: prefix.End, DurationNS: duration, Ordinal: ordinal}, nil
}

func sqliteReadIntervalLocalStatement(s *sqliteio.Stmt) (result sqliteIntervalLocalRow, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteIntervalLocalRow{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	result, err = sqliteDecodeIntervalLocal(s)
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

func sqliteReadIntervalLocal(tx *sqliteio.Tx, computer, id string) (sqliteIntervalLocalRow, bool, error) {
	if !validUUID(computer) || !validUUID(id) {
		return sqliteIntervalLocalRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteIntervalLocalColumns+" FROM intervals WHERE interval_id=?", sqliteio.Text(id))
	if err != nil {
		return sqliteIntervalLocalRow{}, false, err
	}
	row, found, err := sqliteReadIntervalLocalStatement(s)
	if err != nil || !found {
		return row, found, err
	}
	if row.ID != id || row.ComputerID != computer {
		return sqliteIntervalLocalRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

func sqliteInsertIntervalLocal(tx *sqliteio.Tx, computer string, row sqliteIntervalLocalRow) (delta int64, err error) {
	if !validUUID(computer) || row.ComputerID != computer {
		return 0, failure("validation")
	}
	values, charge, err := sqliteEncodeIntervalLocal(row)
	if err != nil {
		return 0, err
	}
	s, err := tx.Prepare("INSERT INTO intervals("+sqliteIntervalLocalColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING interval_id", values[:]...)
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteLocalReturnedID(s, row.ID); err != nil {
		return 0, err
	}
	return charge, nil
}

func sqliteNextIntervalOrdinalLocal(tx *sqliteio.Tx) (result int64, err error) {
	s, err := tx.Prepare("SELECT creation_ordinal FROM intervals ORDER BY creation_ordinal DESC LIMIT 1")
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
	if err != nil || !present {
		return 0, err
	}
	if s.ColumnCount() != 1 {
		return 0, failure("state_corrupt")
	}
	ordinal, err := sqliteLocalRangeInteger(s, 0)
	if err != nil {
		return 0, err
	}
	if ordinal < 0 {
		return 0, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return 0, err
	} else if present {
		return 0, failure("state_corrupt")
	}
	if ordinal == math.MaxInt64 {
		return 0, failure("validation")
	}
	return ordinal + 1, nil
}

func sqliteRequireIntervalLocal(tx *sqliteio.Tx, computer, intervalID string) error {
	_, found, err := sqliteReadIntervalLocal(tx, computer, intervalID)
	if err != nil {
		return err
	}
	if !found {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteIntervalSegmentLocalCharge(intervalID string, ordinal int64, segmentID string) (int64, error) {
	if !validUUID(intervalID) || !validUUID(segmentID) || ordinal < 0 {
		return 0, failure("validation")
	}
	return sqliteRowCharge(1, 0, []string{intervalID, segmentID}, nil)
}

func sqliteIntervalSegmentIDsLocal(tx *sqliteio.Tx, computer, intervalID string) (result []string, err error) {
	if err = sqliteRequireIntervalLocal(tx, computer, intervalID); err != nil {
		return nil, err
	}
	s, err := tx.Prepare("SELECT interval_id,ordinal,segment_id FROM interval_segments WHERE interval_id=? ORDER BY ordinal ASC", sqliteio.Text(intervalID))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = make([]string, 0)
	for {
		present, stepErr := s.Step()
		if stepErr != nil {
			return nil, stepErr
		}
		if !present {
			return result, nil
		}
		ordinal, segment, decodeErr := sqliteReadLocalChild(s, intervalID, true)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if ordinal != int64(len(result)) {
			return nil, failure("state_corrupt")
		}
		result = append(result, segment)
	}
}

func sqliteNextIntervalSegmentOrdinalLocal(tx *sqliteio.Tx, computer, intervalID string) (result int64, err error) {
	if err = sqliteRequireIntervalLocal(tx, computer, intervalID); err != nil {
		return 0, err
	}
	s, err := tx.Prepare("SELECT interval_id,ordinal,segment_id FROM interval_segments WHERE interval_id=? ORDER BY ordinal DESC LIMIT 1", sqliteio.Text(intervalID))
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
	if err != nil || !present {
		return 0, err
	}
	ordinal, _, err := sqliteReadLocalChild(s, intervalID, true)
	if err != nil {
		return 0, err
	}
	if present, err = s.Step(); err != nil {
		return 0, err
	} else if present {
		return 0, failure("state_corrupt")
	}
	if ordinal == math.MaxInt64 {
		return 0, failure("validation")
	}
	return ordinal + 1, nil
}

func sqliteAppendIntervalSegmentLocal(tx *sqliteio.Tx, computer, intervalID string, ordinal int64, segmentID string) (delta int64, err error) {
	charge, err := sqliteIntervalSegmentLocalCharge(intervalID, ordinal, segmentID)
	if err != nil {
		return 0, err
	}
	next, err := sqliteNextIntervalSegmentOrdinalLocal(tx, computer, intervalID)
	if err != nil {
		return 0, err
	}
	if ordinal != next {
		return 0, failure("state_corrupt")
	}
	s, err := tx.Prepare("INSERT INTO interval_segments(interval_id,ordinal,segment_id) VALUES(?,?,?) RETURNING interval_id,ordinal", sqliteio.Text(intervalID), sqliteio.Integer(ordinal), sqliteio.Text(segmentID))
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteLocalReturnedChild(s, intervalID, ordinal); err != nil {
		return 0, err
	}
	return charge, nil
}

func sqliteIntervalPredecessorLocal(tx *sqliteio.Tx, timer sqliteTimerKey, at time.Time) (sqliteIntervalLocalRow, bool, error) {
	if !sqliteValidTimerKey(timer) {
		return sqliteIntervalLocalRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteIntervalLocalColumns+" FROM intervals WHERE computer_id=? AND account_id=? AND project_id=? AND (start_sec,start_nsec)<=(?,?) ORDER BY start_sec DESC,start_nsec DESC,end_sec DESC,end_nsec DESC LIMIT 1",
		sqliteio.Text(timer.ComputerID), sqliteio.Text(timer.AccountID), sqliteio.Text(timer.ProjectID), sqliteio.Integer(at.Unix()), sqliteio.Integer(int64(at.Nanosecond())))
	if err != nil {
		return sqliteIntervalLocalRow{}, false, err
	}
	row, found, err := sqliteReadIntervalLocalStatement(s)
	if err != nil || !found {
		return row, found, err
	}
	if row.ComputerID != timer.ComputerID || row.Attribution.AccountID != timer.AccountID || row.Attribution.ProjectID != timer.ProjectID || sqliteCompareRangeTime(row.Start, at) > 0 {
		return sqliteIntervalLocalRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

func sqliteIntervalSuccessorLocal(tx *sqliteio.Tx, timer sqliteTimerKey, after time.Time) (sqliteIntervalLocalRow, bool, error) {
	if !sqliteValidTimerKey(timer) {
		return sqliteIntervalLocalRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteIntervalLocalColumns+" FROM intervals WHERE computer_id=? AND account_id=? AND project_id=? AND (start_sec,start_nsec)>(?,?) ORDER BY start_sec ASC,start_nsec ASC,end_sec ASC,end_nsec ASC LIMIT 1",
		sqliteio.Text(timer.ComputerID), sqliteio.Text(timer.AccountID), sqliteio.Text(timer.ProjectID), sqliteio.Integer(after.Unix()), sqliteio.Integer(int64(after.Nanosecond())))
	if err != nil {
		return sqliteIntervalLocalRow{}, false, err
	}
	row, found, err := sqliteReadIntervalLocalStatement(s)
	if err != nil || !found {
		return row, found, err
	}
	if row.ComputerID != timer.ComputerID || row.Attribution.AccountID != timer.AccountID || row.Attribution.ProjectID != timer.ProjectID || sqliteCompareRangeTime(row.Start, after) <= 0 {
		return sqliteIntervalLocalRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

func sqliteLatestIntervalLocal(tx *sqliteio.Tx, timer sqliteTimerKey) (sqliteIntervalLocalRow, bool, error) {
	if !sqliteValidTimerKey(timer) {
		return sqliteIntervalLocalRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteIntervalLocalColumns+" FROM intervals WHERE computer_id=? AND account_id=? AND project_id=? ORDER BY end_sec DESC,end_nsec DESC LIMIT 1", sqliteio.Text(timer.ComputerID), sqliteio.Text(timer.AccountID), sqliteio.Text(timer.ProjectID))
	if err != nil {
		return sqliteIntervalLocalRow{}, false, err
	}
	row, found, err := sqliteReadIntervalLocalStatement(s)
	if err != nil || !found {
		return row, found, err
	}
	if row.ComputerID != timer.ComputerID || row.Attribution.AccountID != timer.AccountID || row.Attribution.ProjectID != timer.ProjectID {
		return sqliteIntervalLocalRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}
