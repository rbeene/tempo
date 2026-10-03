//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/identity"
)

// This scalar owns no invented attempt history or complete calendar graph.
type sqliteSyncPartLocalRow struct {
	IntervalID                                            string
	Ordinal                                               int64
	ID, SpentDate, DurationNS                             string
	Start, End                                            time.Time
	PlannedHours, PlannedDurationNS, PlannedResidualNS    string
	StartedTime, EndedTime                                *string
	Correlation, Notes, State                             string
	EntryID, FailureCategory, ReturnedHours, RoundedHours *string
	ConfirmedDurationNS, ProviderDeltaNS, TotalResidualNS *string
	Attachment                                            *SyncAttachment
}

const sqliteSyncPartLocalColumns = "interval_id,ordinal,id,spent_date,duration_ns,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,planned_hours,planned_duration_ns,planned_residual_ns,started_time,ended_time,correlation,notes,state,entry_id,failure_category,returned_hours,rounded_hours,confirmed_duration_ns,provider_delta_ns,total_residual_ns,attachment_request_id,attachment_entry_id"
const sqliteSyncPartLocalOld = "interval_id=? AND ordinal=? AND id=? AND spent_date=? AND duration_ns=? AND start_sec=? AND start_nsec=? AND start_json=? AND end_sec=? AND end_nsec=? AND end_json=? AND planned_hours=? AND planned_duration_ns=? AND planned_residual_ns=? AND started_time IS ? AND ended_time IS ? AND correlation=? AND notes=? AND state=? AND entry_id IS ? AND failure_category IS ? AND returned_hours IS ? AND rounded_hours IS ? AND confirmed_duration_ns IS ? AND provider_delta_ns IS ? AND total_residual_ns IS ? AND attachment_request_id IS ? AND attachment_entry_id IS ?"

func sqliteValidSyncPartLocal(row sqliteSyncPartLocalRow) bool {
	if !validUUID(row.IntervalID) || row.Ordinal < 0 || row.Ordinal >= 100 || !validUUID(row.ID) || row.Correlation != "tempo:v1:"+row.ID || !validSyncPartState(row.State) || row.Start.Location() != time.UTC || row.End.Location() != time.UTC || !row.End.After(row.Start) {
		return false
	}
	for _, s := range []string{row.SpentDate, row.PlannedHours, row.Notes} {
		if !utf8.ValidString(s) {
			return false
		}
	}
	for _, s := range []*string{row.StartedTime, row.EndedTime, row.EntryID, row.FailureCategory, row.ReturnedHours, row.RoundedHours} {
		if s != nil && !utf8.ValidString(*s) {
			return false
		}
	}
	if row.EntryID != nil && !identity.Valid(*row.EntryID) {
		return false
	}
	if row.Attachment != nil && (!validUUID(row.Attachment.RequestID) || !identity.Valid(row.Attachment.EntryID)) {
		return false
	}
	exact, ok := syncInt(row.DurationNS)
	if !ok || exact <= 0 || exact != int64(row.End.Sub(row.Start)) {
		return false
	}
	planned, ok := syncInt(row.PlannedDurationNS)
	if !ok || planned <= 0 {
		return false
	}
	residual, ok := syncInt(row.PlannedResidualNS)
	if !ok || new(big.Int).Sub(big.NewInt(exact), big.NewInt(planned)).String() != strconv.FormatInt(residual, 10) {
		return false
	}
	_, hours := syncDecimal(json.Number(row.PlannedHours))
	if hours == nil {
		return false
	}
	rounded := syncRoundNS(hours)
	if !rounded.IsInt64() || rounded.Int64() != planned {
		return false
	}
	for _, s := range []*string{row.ConfirmedDurationNS, row.ProviderDeltaNS, row.TotalResidualNS} {
		if s != nil {
			if _, ok := syncInt(*s); !ok {
				return false
			}
		}
	}
	// validSyncAmounts is a pure scalar predicate; no incomplete item/state is
	// passed to a graph validator. Its numeric prerequisites were checked above.
	return validSyncAmounts(SyncPart{DurationNS: row.DurationNS, PlannedHours: row.PlannedHours, PlannedDurationNS: row.PlannedDurationNS, State: row.State, EntryID: row.EntryID, FailureCategory: row.FailureCategory, ReturnedHours: row.ReturnedHours, RoundedHours: row.RoundedHours, ConfirmedDurationNS: row.ConfirmedDurationNS, ProviderDeltaNS: row.ProviderDeltaNS, TotalResidualNS: row.TotalResidualNS})
}

func sqliteEncodeSyncPartLocal(row sqliteSyncPartLocalRow) ([28]sqliteio.Value, int64, error) {
	var values [28]sqliteio.Value
	if !sqliteValidSyncPartLocal(row) {
		return values, 0, failure("validation")
	}
	start, err := sqliteEncodeTime(row.Start)
	if err != nil {
		return values, 0, failure("validation")
	}
	end, err := sqliteEncodeTime(row.End)
	if err != nil {
		return values, 0, failure("validation")
	}
	materialized := row
	materialized.Start, err = sqliteDecodeTime(start)
	if err != nil {
		return values, 0, failure("validation")
	}
	materialized.End, err = sqliteDecodeTime(end)
	if err != nil || !sqliteValidSyncPartLocal(materialized) {
		return values, 0, failure("validation")
	}
	exact, _ := syncInt(row.DurationNS)
	planned, _ := syncInt(row.PlannedDurationNS)
	residual, _ := syncInt(row.PlannedResidualNS)
	values = [28]sqliteio.Value{
		sqliteio.Text(row.IntervalID), sqliteio.Integer(row.Ordinal), sqliteio.Text(row.ID), sqliteio.Text(row.SpentDate), sqliteio.Integer(exact),
		sqliteio.Integer(start.Seconds), sqliteio.Integer(start.Nanoseconds), sqliteio.Text(start.JSON), sqliteio.Integer(end.Seconds), sqliteio.Integer(end.Nanoseconds), sqliteio.Text(end.JSON),
		sqliteio.Text(row.PlannedHours), sqliteio.Integer(planned), sqliteio.Integer(residual), sqliteMetaOptional(row.StartedTime), sqliteMetaOptional(row.EndedTime),
		sqliteio.Text(row.Correlation), sqliteio.Text(row.Notes), sqliteio.Text(row.State), sqliteMetaOptional(row.EntryID), sqliteMetaOptional(row.FailureCategory), sqliteMetaOptional(row.ReturnedHours), sqliteMetaOptional(row.RoundedHours),
		sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(),
	}
	integers, nulls := 8, 0
	texts := []string{row.IntervalID, row.ID, row.SpentDate, start.JSON, end.JSON, row.PlannedHours, row.Correlation, row.Notes, row.State}
	for _, s := range []*string{row.StartedTime, row.EndedTime, row.EntryID, row.FailureCategory, row.ReturnedHours, row.RoundedHours} {
		if s == nil {
			nulls++
		} else {
			texts = append(texts, *s)
		}
	}
	for i, s := range []*string{row.ConfirmedDurationNS, row.ProviderDeltaNS, row.TotalResidualNS} {
		if s == nil {
			nulls++
		} else {
			n, _ := syncInt(*s)
			values[23+i] = sqliteio.Integer(n)
			integers++
		}
	}
	if row.Attachment == nil {
		nulls += 2
	} else {
		values[26], values[27] = sqliteio.Text(row.Attachment.RequestID), sqliteio.Text(row.Attachment.EntryID)
		texts = append(texts, row.Attachment.RequestID, row.Attachment.EntryID)
	}
	charge, err := sqliteRowCharge(integers, nulls, texts, nil)
	if err != nil {
		return [28]sqliteio.Value{}, 0, err
	}
	return values, charge, nil
}
func sqliteSyncPartLocalCharge(row sqliteSyncPartLocalRow) (int64, error) {
	_, charge, err := sqliteEncodeSyncPartLocal(row)
	return charge, err
}

func sqliteSyncOptionalText(s *sqliteio.Stmt, column int) (*string, error) {
	kind, err := s.Kind(column)
	if err != nil {
		return nil, errors.Join(failure("state_corrupt"), err)
	}
	if kind == sqliteio.NullKind {
		return nil, nil
	}
	value, err := sqliteDependencyText(s, column)
	if err != nil {
		return nil, err
	}
	return &value, nil
}
func sqliteSyncOptionalInteger(s *sqliteio.Stmt, column int) (*string, error) {
	kind, err := s.Kind(column)
	if err != nil {
		return nil, errors.Join(failure("state_corrupt"), err)
	}
	if kind == sqliteio.NullKind {
		return nil, nil
	}
	value, err := sqliteLocalRangeInteger(s, column)
	if err != nil {
		return nil, err
	}
	text := sqliteDecodeInt64(value)
	return &text, nil
}
func sqliteDecodeSyncPartLocal(s *sqliteio.Stmt) (sqliteSyncPartLocalRow, error) {
	var row sqliteSyncPartLocalRow
	if s.ColumnCount() != 28 {
		return row, failure("state_corrupt")
	}
	for _, field := range []struct {
		column int
		target *string
	}{{0, &row.IntervalID}, {2, &row.ID}, {3, &row.SpentDate}, {11, &row.PlannedHours}, {16, &row.Correlation}, {17, &row.Notes}, {18, &row.State}} {
		v, err := sqliteDependencyText(s, field.column)
		if err != nil {
			return sqliteSyncPartLocalRow{}, err
		}
		*field.target = v
	}
	var err error
	row.Ordinal, err = sqliteLocalRangeInteger(s, 1)
	if err != nil {
		return sqliteSyncPartLocalRow{}, err
	}
	for _, field := range []struct {
		column int
		target *string
	}{{4, &row.DurationNS}, {12, &row.PlannedDurationNS}, {13, &row.PlannedResidualNS}} {
		v, e := sqliteLocalRangeInteger(s, field.column)
		if e != nil {
			return sqliteSyncPartLocalRow{}, e
		}
		*field.target = sqliteDecodeInt64(v)
	}
	row.Start, err = sqliteLocalRangeTime(s, 5)
	if err != nil {
		return sqliteSyncPartLocalRow{}, err
	}
	row.End, err = sqliteLocalRangeTime(s, 8)
	if err != nil {
		return sqliteSyncPartLocalRow{}, err
	}
	for _, field := range []struct {
		column int
		target **string
	}{{14, &row.StartedTime}, {15, &row.EndedTime}, {19, &row.EntryID}, {20, &row.FailureCategory}, {21, &row.ReturnedHours}, {22, &row.RoundedHours}} {
		v, e := sqliteSyncOptionalText(s, field.column)
		if e != nil {
			return sqliteSyncPartLocalRow{}, e
		}
		*field.target = v
	}
	for i, target := range []**string{&row.ConfirmedDurationNS, &row.ProviderDeltaNS, &row.TotalResidualNS} {
		v, e := sqliteSyncOptionalInteger(s, 23+i)
		if e != nil {
			return sqliteSyncPartLocalRow{}, e
		}
		*target = v
	}
	request, err := sqliteSyncOptionalText(s, 26)
	if err != nil {
		return sqliteSyncPartLocalRow{}, err
	}
	entry, err := sqliteSyncOptionalText(s, 27)
	if err != nil {
		return sqliteSyncPartLocalRow{}, err
	}
	if (request == nil) != (entry == nil) {
		return sqliteSyncPartLocalRow{}, failure("state_corrupt")
	}
	if request != nil {
		row.Attachment = &SyncAttachment{RequestID: *request, EntryID: *entry}
	}
	if !sqliteValidSyncPartLocal(row) {
		return sqliteSyncPartLocalRow{}, failure("state_corrupt")
	}
	return row, nil
}
func sqliteReadSyncPartStatement(s *sqliteio.Stmt, interval string, ordinal *int64) (result sqliteSyncPartLocalRow, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteSyncPartLocalRow{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	result, err = sqliteDecodeSyncPartLocal(s)
	if err != nil {
		return result, false, err
	}
	if result.IntervalID != interval || ordinal != nil && result.Ordinal != *ordinal {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}
func sqliteRequireSyncPlanLocal(tx *sqliteio.Tx, computer, interval string) error {
	_, found, err := sqliteReadSyncPlanLocal(tx, computer, interval)
	if err != nil {
		return err
	}
	if !found {
		return failure("state_corrupt")
	}
	return nil
}
func sqliteReadSyncPartLocal(tx *sqliteio.Tx, computer, interval string, ordinal int64) (sqliteSyncPartLocalRow, bool, error) {
	if !validUUID(computer) || !validUUID(interval) || ordinal < 0 || ordinal >= 100 {
		return sqliteSyncPartLocalRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteSyncPartLocalColumns+" FROM sync_parts WHERE interval_id=? AND ordinal=?", sqliteio.Text(interval), sqliteio.Integer(ordinal))
	if err != nil {
		return sqliteSyncPartLocalRow{}, false, err
	}
	row, found, err := sqliteReadSyncPartStatement(s, interval, &ordinal)
	if err != nil || !found {
		return row, found, err
	}
	if err = sqliteRequireSyncPlanLocal(tx, computer, interval); err != nil {
		return sqliteSyncPartLocalRow{}, false, err
	}
	return row, true, nil
}
func sqliteSyncPartsLocal(tx *sqliteio.Tx, computer, interval string) (result []sqliteSyncPartLocalRow, err error) {
	if !validUUID(computer) || !validUUID(interval) {
		return nil, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteSyncPartLocalColumns+" FROM sync_parts WHERE interval_id=? ORDER BY ordinal", sqliteio.Text(interval))
	if err != nil {
		return nil, err
	}
	result, err = sqliteSyncPartsStatement(s, interval)
	if err != nil {
		return nil, err
	}
	if err = sqliteRequireSyncPlanLocal(tx, computer, interval); err != nil {
		return nil, err
	}
	return result, nil
}
func sqliteSyncPartsStatement(s *sqliteio.Stmt, interval string) (result []sqliteSyncPartLocalRow, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = make([]sqliteSyncPartLocalRow, 0)
	for {
		present, e := s.Step()
		if e != nil {
			return nil, e
		}
		if !present {
			return result, nil
		}
		row, e := sqliteDecodeSyncPartLocal(s)
		if e != nil {
			return nil, e
		}
		if row.IntervalID != interval || row.Ordinal != int64(len(result)) {
			return nil, failure("state_corrupt")
		}
		result = append(result, row)
	}
}
func sqliteWriteSyncPartLocal(tx *sqliteio.Tx, computer string, before *sqliteSyncPartLocalRow, after sqliteSyncPartLocalRow) (delta int64, err error) {
	if !validUUID(computer) {
		return 0, failure("validation")
	}
	next, charge, err := sqliteEncodeSyncPartLocal(after)
	if err != nil {
		return 0, err
	}
	var old [28]sqliteio.Value
	var oldCharge int64
	if before != nil {
		old, oldCharge, err = sqliteEncodeSyncPartLocal(*before)
		if err != nil {
			return 0, err
		}
		if !reflect.DeepEqual(old[:18], next[:18]) {
			return 0, failure("validation")
		}
	}
	if err = sqliteRequireSyncPlanLocal(tx, computer, after.IntervalID); err != nil {
		return 0, err
	}
	var s *sqliteio.Stmt
	if before == nil {
		s, err = tx.Prepare("SELECT "+sqliteSyncPartLocalColumns+" FROM sync_parts WHERE interval_id=? ORDER BY ordinal DESC LIMIT 1", sqliteio.Text(after.IntervalID))
		if err != nil {
			return 0, err
		}
		last, found, e := sqliteReadSyncPartStatement(s, after.IntervalID, nil)
		if e != nil {
			return 0, e
		}
		ordinal := int64(0)
		if found {
			ordinal = last.Ordinal + 1
		}
		if ordinal >= 100 || after.Ordinal != ordinal {
			return 0, failure("validation")
		}
		s, err = tx.Prepare("INSERT INTO sync_parts("+sqliteSyncPartLocalColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING interval_id,ordinal", next[:]...)
	} else {
		values := make([]sqliteio.Value, 0, 54)
		values = append(values, next[2:]...)
		values = append(values, old[:]...)
		s, err = tx.Prepare("UPDATE sync_parts SET id=?,spent_date=?,duration_ns=?,start_sec=?,start_nsec=?,start_json=?,end_sec=?,end_nsec=?,end_json=?,planned_hours=?,planned_duration_ns=?,planned_residual_ns=?,started_time=?,ended_time=?,correlation=?,notes=?,state=?,entry_id=?,failure_category=?,returned_hours=?,rounded_hours=?,confirmed_duration_ns=?,provider_delta_ns=?,total_residual_ns=?,attachment_request_id=?,attachment_entry_id=? WHERE "+sqliteSyncPartLocalOld+" RETURNING interval_id,ordinal", values...)
	}
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteLocalReturnedChild(s, after.IntervalID, after.Ordinal); err != nil {
		return 0, err
	}
	return charge - oldCharge, nil
}
func sqliteDeleteUnattemptedSyncPart(tx *sqliteio.Tx, computer string, before sqliteSyncPartLocalRow) (delta int64, err error) {
	if !validUUID(computer) {
		return 0, failure("validation")
	}
	values, charge, err := sqliteEncodeSyncPartLocal(before)
	if err != nil {
		return 0, err
	}
	if before.State != "queued" || before.EntryID != nil || before.FailureCategory != nil || before.Attachment != nil {
		return 0, failure("validation")
	}
	if err = sqliteRequireSyncPlanLocal(tx, computer, before.IntervalID); err != nil {
		return 0, err
	}
	s, err := tx.Prepare("SELECT interval_id,part_ordinal,ordinal FROM sync_attempts WHERE interval_id=? AND part_ordinal=? LIMIT 1", sqliteio.Text(before.IntervalID), sqliteio.Integer(before.Ordinal))
	if err != nil {
		return 0, err
	}
	if err = sqliteSyncPartNoAttempts(s, before.IntervalID, before.Ordinal); err != nil {
		return 0, err
	}
	s, err = tx.Prepare("DELETE FROM sync_parts WHERE "+sqliteSyncPartLocalOld+" RETURNING interval_id,ordinal", values[:]...)
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteLocalReturnedChild(s, before.IntervalID, before.Ordinal); err != nil {
		return 0, err
	}
	return -charge, nil
}
func sqliteSyncPartNoAttempts(s *sqliteio.Stmt, interval string, part int64) (err error) {
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	present, err := s.Step()
	if err != nil || !present {
		return err
	}
	if s.ColumnCount() != 3 {
		return failure("state_corrupt")
	}
	owner, err := sqliteDependencyText(s, 0)
	if err != nil {
		return err
	}
	storedPart, err := sqliteLocalRangeInteger(s, 1)
	if err != nil {
		return err
	}
	ordinal, err := sqliteLocalRangeInteger(s, 2)
	if err != nil {
		return err
	}
	if owner != interval || storedPart != part || ordinal < 0 || ordinal == math.MaxInt64 {
		return failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return err
	} else if present {
		return failure("state_corrupt")
	}
	return failure("state_corrupt")
}
