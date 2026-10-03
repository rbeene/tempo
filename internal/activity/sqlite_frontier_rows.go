//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/identity"
)

type sqliteFrontierLocalRow struct {
	ID, ComputerID string
	Attribution    Attribution
	Start, End     time.Time
}

type sqliteTimerKey struct{ ComputerID, AccountID, ProjectID string }
type sqliteReservationWindow struct {
	Start time.Time
	End   *time.Time
}

const sqliteFrontierLocalColumns = "component_id,computer_id,account_id,user_id,project_id,task_id,timezone,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json"
const sqliteFrontierLocalOld = "component_id=? AND computer_id=? AND account_id=? AND user_id=? AND project_id=? AND task_id=? AND timezone=? AND group_order=? AND start_sec=? AND start_nsec=? AND start_json=? AND end_sec=? AND end_nsec=? AND end_json=?"

func sqliteValidComponentID(id string) bool {
	if len(id) != 68 || id[:4] != "fc1:" {
		return false
	}
	for _, c := range id[4:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// Query bounds retain their supplied instants, independently of time JSON repair.
func sqliteCompareRangeTime(a, b time.Time) int {
	if a.Unix() < b.Unix() {
		return -1
	}
	if a.Unix() > b.Unix() {
		return 1
	}
	if a.Nanosecond() < b.Nanosecond() {
		return -1
	}
	if a.Nanosecond() > b.Nanosecond() {
		return 1
	}
	return 0
}

func sqliteValidTimerKey(timer sqliteTimerKey) bool {
	return validUUID(timer.ComputerID) && identity.Valid(timer.AccountID) && identity.Valid(timer.ProjectID)
}

// Validate both the caller's range and the owned materialized range before SQL.
func sqliteEncodePositiveRange(start, end time.Time) (sqliteTimeValue, sqliteTimeValue, error) {
	if !end.After(start) {
		return sqliteTimeValue{}, sqliteTimeValue{}, failure("validation")
	}
	a, err := sqliteEncodeTime(start)
	if err != nil {
		return sqliteTimeValue{}, sqliteTimeValue{}, failure("validation")
	}
	b, err := sqliteEncodeTime(end)
	if err != nil {
		return sqliteTimeValue{}, sqliteTimeValue{}, failure("validation")
	}
	decodedStart, err := sqliteDecodeTime(a)
	if err != nil {
		return sqliteTimeValue{}, sqliteTimeValue{}, failure("validation")
	}
	decodedEnd, err := sqliteDecodeTime(b)
	if err != nil || !decodedEnd.After(decodedStart) {
		return sqliteTimeValue{}, sqliteTimeValue{}, failure("validation")
	}
	return a, b, nil
}

// Frontier and interval have this exact fourteen-column prefix. Identity shape
// belongs to the concrete owner; this helper never prepares or chooses SQL.
func sqliteEncodeRangeColumns(id, computer string, a Attribution, start, end time.Time) ([14]sqliteio.Value, int64, error) {
	var values [14]sqliteio.Value
	if !validUUID(computer) || !validAttribution(a) {
		return values, 0, failure("validation")
	}
	s, e, err := sqliteEncodePositiveRange(start, end)
	if err != nil {
		return values, 0, err
	}
	group := attributionKey(computer, a)
	values = [14]sqliteio.Value{
		sqliteio.Text(id), sqliteio.Text(computer), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID),
		sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Text(group),
		sqliteio.Integer(s.Seconds), sqliteio.Integer(s.Nanoseconds), sqliteio.Text(s.JSON),
		sqliteio.Integer(e.Seconds), sqliteio.Integer(e.Nanoseconds), sqliteio.Text(e.JSON),
	}
	charge, err := sqliteRowCharge(4, 0, []string{id, computer, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, group, s.JSON, e.JSON}, nil)
	if err != nil {
		return [14]sqliteio.Value{}, 0, err
	}
	return values, charge, nil
}

func sqliteLocalRangeInteger(s *sqliteio.Stmt, column int) (int64, error) {
	kind, err := s.Kind(column)
	if err != nil {
		return 0, errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.IntegerKind {
		return 0, failure("state_corrupt")
	}
	return s.Int64(column)
}

func sqliteLocalRangeTime(s *sqliteio.Stmt, column int) (time.Time, error) {
	seconds, err := sqliteLocalRangeInteger(s, column)
	if err != nil {
		return time.Time{}, err
	}
	nanos, err := sqliteLocalRangeInteger(s, column+1)
	if err != nil {
		return time.Time{}, err
	}
	text, err := sqliteHostNormalizationText(s, column+2)
	if err != nil {
		return time.Time{}, err
	}
	return sqliteDecodeTime(sqliteTimeValue{Seconds: seconds, Nanoseconds: nanos, JSON: text})
}

func sqliteReadRangeColumns(s *sqliteio.Stmt) (sqliteFrontierLocalRow, error) {
	var row sqliteFrontierLocalRow
	var group string
	texts := []*string{&row.ID, &row.ComputerID, &row.Attribution.AccountID, &row.Attribution.UserID, &row.Attribution.ProjectID, &row.Attribution.TaskID, &row.Attribution.Timezone, &group}
	for column, target := range texts {
		value, err := sqliteHostNormalizationText(s, column)
		if err != nil {
			return sqliteFrontierLocalRow{}, err
		}
		*target = value
	}
	var err error
	row.Start, err = sqliteLocalRangeTime(s, 8)
	if err != nil {
		return sqliteFrontierLocalRow{}, err
	}
	row.End, err = sqliteLocalRangeTime(s, 11)
	if err != nil {
		return sqliteFrontierLocalRow{}, err
	}
	if !validUUID(row.ComputerID) || !validAttribution(row.Attribution) || group != attributionKey(row.ComputerID, row.Attribution) || !row.End.After(row.Start) {
		return sqliteFrontierLocalRow{}, failure("state_corrupt")
	}
	return row, nil
}

func sqliteDecodeFrontierLocal(s *sqliteio.Stmt) (sqliteFrontierLocalRow, error) {
	if s.ColumnCount() != 14 {
		return sqliteFrontierLocalRow{}, failure("state_corrupt")
	}
	row, err := sqliteReadRangeColumns(s)
	if err != nil {
		return sqliteFrontierLocalRow{}, err
	}
	if !sqliteValidComponentID(row.ID) {
		return sqliteFrontierLocalRow{}, failure("state_corrupt")
	}
	return row, nil
}

func sqliteReadFrontierLocalStatement(s *sqliteio.Stmt) (result sqliteFrontierLocalRow, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteFrontierLocalRow{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	result, err = sqliteDecodeFrontierLocal(s)
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

func sqliteReadFrontierLocal(tx *sqliteio.Tx, computer, id string) (sqliteFrontierLocalRow, bool, error) {
	if !validUUID(computer) || !sqliteValidComponentID(id) {
		return sqliteFrontierLocalRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteFrontierLocalColumns+" FROM union_frontier WHERE component_id=?", sqliteio.Text(id))
	if err != nil {
		return sqliteFrontierLocalRow{}, false, err
	}
	row, found, err := sqliteReadFrontierLocalStatement(s)
	if err != nil || !found {
		return row, found, err
	}
	if row.ID != id || row.ComputerID != computer {
		return sqliteFrontierLocalRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

func sqliteEncodeFrontierLocal(row sqliteFrontierLocalRow) ([14]sqliteio.Value, int64, error) {
	if !sqliteValidComponentID(row.ID) {
		return [14]sqliteio.Value{}, 0, failure("validation")
	}
	return sqliteEncodeRangeColumns(row.ID, row.ComputerID, row.Attribution, row.Start, row.End)
}

func sqliteFrontierLocalCharge(row sqliteFrontierLocalRow) (int64, error) {
	_, charge, err := sqliteEncodeFrontierLocal(row)
	return charge, err
}

func sqliteWriteFrontierLocal(tx *sqliteio.Tx, computer string, before *sqliteFrontierLocalRow, after sqliteFrontierLocalRow) (delta int64, err error) {
	if !validUUID(computer) || after.ComputerID != computer {
		return 0, failure("validation")
	}
	next, charge, err := sqliteEncodeFrontierLocal(after)
	if err != nil {
		return 0, err
	}
	query := "INSERT INTO union_frontier(" + sqliteFrontierLocalColumns + ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING component_id"
	values := next[:]
	if before != nil {
		if before.ID != after.ID || before.ComputerID != after.ComputerID || before.Attribution != after.Attribution {
			return 0, failure("validation")
		}
		old, oldCharge, encodeErr := sqliteEncodeFrontierLocal(*before)
		if encodeErr != nil {
			return 0, encodeErr
		}
		query = "UPDATE union_frontier SET computer_id=?,account_id=?,user_id=?,project_id=?,task_id=?,timezone=?,group_order=?,start_sec=?,start_nsec=?,start_json=?,end_sec=?,end_nsec=?,end_json=? WHERE " + sqliteFrontierLocalOld + " RETURNING component_id"
		values = append(append(make([]sqliteio.Value, 0, 27), next[1:]...), old[:]...)
		charge -= oldCharge // Both complete charges are nonnegative int64s.
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

func sqliteDeleteFrontierLocal(tx *sqliteio.Tx, computer string, before sqliteFrontierLocalRow) (delta int64, err error) {
	if !validUUID(computer) || before.ComputerID != computer {
		return 0, failure("validation")
	}
	values, charge, err := sqliteEncodeFrontierLocal(before)
	if err != nil {
		return 0, err
	}
	s, err := tx.Prepare("DELETE FROM union_frontier WHERE "+sqliteFrontierLocalOld+" RETURNING component_id", values[:]...)
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

func sqliteComponentSegmentLocalCharge(componentID, segmentID string) (int64, error) {
	if !sqliteValidComponentID(componentID) || !validUUID(segmentID) {
		return 0, failure("validation")
	}
	return sqliteRowCharge(0, 0, []string{componentID, segmentID}, nil)
}

func sqliteRequireFrontierLocal(tx *sqliteio.Tx, computer, componentID string) error {
	_, found, err := sqliteReadFrontierLocal(tx, computer, componentID)
	if err != nil {
		return err
	}
	if !found {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteDecodeComponentMemberLocal(s *sqliteio.Stmt) (string, string, error) {
	if s.ColumnCount() != 2 {
		return "", "", failure("state_corrupt")
	}
	component, err := sqliteHostNormalizationText(s, 0)
	if err != nil {
		return "", "", err
	}
	segment, err := sqliteHostNormalizationText(s, 1)
	if err != nil {
		return "", "", err
	}
	if !sqliteValidComponentID(component) || !validUUID(segment) {
		return "", "", failure("state_corrupt")
	}
	return component, segment, nil
}

func sqliteComponentForSegmentLocal(tx *sqliteio.Tx, computer, segmentID string) (result string, found bool, err error) {
	if !validUUID(computer) || !validUUID(segmentID) {
		return "", false, failure("validation")
	}
	s, err := tx.Prepare("SELECT component_id,segment_id FROM component_segments WHERE segment_id=?", sqliteio.Text(segmentID))
	if err != nil {
		return "", false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = "", false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return "", false, err
	}
	component, selected, err := sqliteDecodeComponentMemberLocal(s)
	if err != nil {
		return "", false, err
	}
	if selected != segmentID {
		return "", false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return "", false, err
	} else if present {
		return "", false, failure("state_corrupt")
	}
	if err = sqliteRequireFrontierLocal(tx, computer, component); err != nil {
		return "", false, err
	}
	return component, true, nil
}

func sqliteComponentSegmentIDsLocal(tx *sqliteio.Tx, computer, componentID string) (result []string, err error) {
	if err = sqliteRequireFrontierLocal(tx, computer, componentID); err != nil {
		return nil, err
	}
	s, err := tx.Prepare("SELECT component_id,segment_id FROM component_segments WHERE component_id=? ORDER BY segment_id ASC", sqliteio.Text(componentID))
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
		owner, segment, decodeErr := sqliteDecodeComponentMemberLocal(s)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if owner != componentID || len(result) > 0 && segment <= result[len(result)-1] {
			return nil, failure("state_corrupt")
		}
		result = append(result, segment)
	}
}

func sqliteInsertComponentSegmentLocal(tx *sqliteio.Tx, computer, componentID, segmentID string) (delta int64, err error) {
	charge, err := sqliteComponentSegmentLocalCharge(componentID, segmentID)
	if err != nil {
		return 0, err
	}
	if err = sqliteRequireFrontierLocal(tx, computer, componentID); err != nil {
		return 0, err
	}
	s, err := tx.Prepare("INSERT INTO component_segments(component_id,segment_id) VALUES(?,?) RETURNING component_id,segment_id", sqliteio.Text(componentID), sqliteio.Text(segmentID))
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteHostNormalizationReturnedKeys(s, componentID, segmentID); err != nil {
		return 0, err
	}
	return charge, nil
}

func sqliteDeleteComponentSegmentLocal(tx *sqliteio.Tx, computer, componentID, segmentID string) (delta int64, err error) {
	charge, err := sqliteComponentSegmentLocalCharge(componentID, segmentID)
	if err != nil {
		return 0, err
	}
	if err = sqliteRequireFrontierLocal(tx, computer, componentID); err != nil {
		return 0, err
	}
	s, err := tx.Prepare("DELETE FROM component_segments WHERE component_id=? AND segment_id=? RETURNING component_id,segment_id", sqliteio.Text(componentID), sqliteio.Text(segmentID))
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteHostNormalizationReturnedKeys(s, componentID, segmentID); err != nil {
		return 0, err
	}
	return -charge, nil
}

func sqliteFrontierPredecessorLocal(tx *sqliteio.Tx, computer string, a Attribution, at time.Time) (sqliteFrontierLocalRow, bool, error) {
	if !validUUID(computer) || !validAttribution(a) {
		return sqliteFrontierLocalRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteFrontierLocalColumns+" FROM union_frontier WHERE computer_id=? AND account_id=? AND user_id=? AND project_id=? AND task_id=? AND timezone=? AND (start_sec,start_nsec)<=(?,?) ORDER BY start_sec DESC,start_nsec DESC,end_sec DESC,end_nsec DESC LIMIT 1",
		sqliteio.Text(computer), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID), sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Integer(at.Unix()), sqliteio.Integer(int64(at.Nanosecond())))
	if err != nil {
		return sqliteFrontierLocalRow{}, false, err
	}
	row, found, err := sqliteReadFrontierLocalStatement(s)
	if err != nil || !found {
		return row, found, err
	}
	if row.ComputerID != computer || row.Attribution != a || sqliteCompareRangeTime(row.Start, at) > 0 {
		return sqliteFrontierLocalRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

func sqliteFrontierSuccessorLocal(tx *sqliteio.Tx, computer string, a Attribution, after time.Time) (sqliteFrontierLocalRow, bool, error) {
	if !validUUID(computer) || !validAttribution(a) {
		return sqliteFrontierLocalRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteFrontierLocalColumns+" FROM union_frontier WHERE computer_id=? AND account_id=? AND user_id=? AND project_id=? AND task_id=? AND timezone=? AND (start_sec,start_nsec)>(?,?) ORDER BY start_sec ASC,start_nsec ASC,end_sec ASC,end_nsec ASC LIMIT 1",
		sqliteio.Text(computer), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID), sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Integer(after.Unix()), sqliteio.Integer(int64(after.Nanosecond())))
	if err != nil {
		return sqliteFrontierLocalRow{}, false, err
	}
	row, found, err := sqliteReadFrontierLocalStatement(s)
	if err != nil || !found {
		return row, found, err
	}
	if row.ComputerID != computer || row.Attribution != a || sqliteCompareRangeTime(row.Start, after) <= 0 {
		return sqliteFrontierLocalRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

func sqliteFrontierReservationRowsLocal(tx *sqliteio.Tx, timer sqliteTimerKey, window sqliteReservationWindow) (result []sqliteFrontierLocalRow, err error) {
	if !sqliteValidTimerKey(timer) || window.End != nil && sqliteCompareRangeTime(*window.End, window.Start) < 0 {
		return nil, failure("validation")
	}
	query := "SELECT " + sqliteFrontierLocalColumns + " FROM union_frontier WHERE computer_id=? AND account_id=? AND project_id=? AND (end_sec,end_nsec)>=(?,?)"
	values := []sqliteio.Value{sqliteio.Text(timer.ComputerID), sqliteio.Text(timer.AccountID), sqliteio.Text(timer.ProjectID), sqliteio.Integer(window.Start.Unix()), sqliteio.Integer(int64(window.Start.Nanosecond()))}
	if window.End != nil {
		query += " AND (start_sec,start_nsec)<=(?,?)"
		values = append(values, sqliteio.Integer(window.End.Unix()), sqliteio.Integer(int64(window.End.Nanosecond())))
	}
	query += " ORDER BY end_sec ASC,end_nsec ASC,component_id ASC"
	s, err := tx.Prepare(query, values...)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = make([]sqliteFrontierLocalRow, 0)
	for {
		present, stepErr := s.Step()
		if stepErr != nil {
			return nil, stepErr
		}
		if !present {
			return result, nil
		}
		row, decodeErr := sqliteDecodeFrontierLocal(s)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if row.ComputerID != timer.ComputerID || row.Attribution.AccountID != timer.AccountID || row.Attribution.ProjectID != timer.ProjectID || sqliteCompareRangeTime(row.End, window.Start) < 0 || window.End != nil && sqliteCompareRangeTime(row.Start, *window.End) > 0 {
			return nil, failure("state_corrupt")
		}
		result = append(result, row)
	}
}
