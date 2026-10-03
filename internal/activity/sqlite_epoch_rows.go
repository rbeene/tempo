//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"math"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteEpochRow struct {
	Ordinal int64
	Value   timelineEpoch
}

const sqliteEpochColumns = "epoch_id,computer_id,account_id,user_id,project_id,task_id,timezone,creation_ordinal,group_order,anchor_capability,anchor_wall_sec,anchor_wall_nsec,anchor_wall_json,anchor_epoch,anchor_elapsed_raw,anchor_awake_raw,anchor_elapsed,anchor_awake"

type sqliteEncodedEpoch struct {
	id     string
	values [18]sqliteio.Value
	charge int64
}

func sqliteReadEpoch(tx *sqliteio.Tx, computerID, id string) (sqliteEpochRow, bool, error) {
	if !validUUID(computerID) || !validUUID(id) {
		return sqliteEpochRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteEpochColumns+" FROM epochs WHERE epoch_id=?", sqliteio.Text(id))
	if err != nil {
		return sqliteEpochRow{}, false, err
	}
	row, found, err := sqliteReadEpochStatement(s)
	if err != nil || !found {
		return row, found, err
	}
	if row.Value.ID != id || row.Value.ComputerID != computerID {
		return sqliteEpochRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

func sqliteLatestEpoch(tx *sqliteio.Tx, computerID string, attribution Attribution) (sqliteEpochRow, bool, error) {
	if !validUUID(computerID) || !validAttribution(attribution) {
		return sqliteEpochRow{}, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteEpochColumns+" FROM epochs WHERE computer_id=? AND account_id=? AND user_id=? AND project_id=? AND task_id=? AND timezone=? ORDER BY creation_ordinal DESC LIMIT 1",
		sqliteio.Text(computerID), sqliteio.Text(attribution.AccountID), sqliteio.Text(attribution.UserID),
		sqliteio.Text(attribution.ProjectID), sqliteio.Text(attribution.TaskID), sqliteio.Text(attribution.Timezone))
	if err != nil {
		return sqliteEpochRow{}, false, err
	}
	row, found, err := sqliteReadEpochStatement(s)
	if err != nil || !found {
		return row, found, err
	}
	if row.Value.ComputerID != computerID || row.Value.Attribution != attribution {
		return sqliteEpochRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

func sqliteReadEpochStatement(s *sqliteio.Stmt) (result sqliteEpochRow, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteEpochRow{}, false
		}
	}()
	row, err := s.Step()
	if err != nil || !row {
		return result, false, err
	}
	if s.ColumnCount() != 18 {
		return result, false, failure("state_corrupt")
	}
	clock := sqliteClockValue{Available: true}
	var group string
	e := &result.Value
	for column := 0; column < 18; column++ {
		expected := sqliteio.TextKind
		switch column {
		case 7, 10, 11:
			expected = sqliteio.IntegerKind
		case 16, 17:
			expected = sqliteio.BlobKind
		}
		kind, kindErr := s.Kind(column)
		if kindErr != nil {
			return result, false, errors.Join(failure("state_corrupt"), kindErr)
		}
		if kind != expected {
			return result, false, failure("state_corrupt")
		}
		switch kind {
		case sqliteio.IntegerKind:
			value, readErr := s.Int64(column)
			if readErr != nil {
				return result, false, readErr
			}
			switch column {
			case 7:
				result.Ordinal = value
			case 10:
				clock.Wall.Seconds = value
			case 11:
				clock.Wall.Nanoseconds = value
			}
		case sqliteio.BlobKind:
			value, readErr := s.Blob(column)
			if readErr != nil {
				return result, false, readErr
			}
			if column == 16 {
				clock.ElapsedCounter = value
			} else {
				clock.AwakeCounter = value
			}
		case sqliteio.TextKind:
			value, readErr := s.Text(column)
			if readErr != nil {
				return result, false, readErr
			}
			if !utf8.ValidString(value) {
				return result, false, failure("state_corrupt")
			}
			switch column {
			case 0:
				e.ID = value
			case 1:
				e.ComputerID = value
			case 2:
				e.Attribution.AccountID = value
			case 3:
				e.Attribution.UserID = value
			case 4:
				e.Attribution.ProjectID = value
			case 5:
				e.Attribution.TaskID = value
			case 6:
				e.Attribution.Timezone = value
			case 8:
				group = value
			case 9:
				clock.Capability = value
			case 12:
				clock.Wall.JSON = value
			case 13:
				clock.Epoch = sqliteCopyString(&value)
			case 14:
				clock.ElapsedRaw = sqliteCopyString(&value)
			case 15:
				clock.AwakeRaw = sqliteCopyString(&value)
			}
		}
	}
	e.Anchor, err = sqliteDecodeClock(clock)
	if err != nil {
		return result, false, err
	}
	if !sqliteValidEpochRow(result) || group != attributionKey(e.ComputerID, e.Attribution) {
		return result, false, failure("state_corrupt")
	}
	if row, err = s.Step(); err != nil {
		return result, false, err
	} else if row {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteNextEpochOrdinal(tx *sqliteio.Tx) (next int64, err error) {
	s, err := tx.Prepare("SELECT creation_ordinal FROM epochs ORDER BY creation_ordinal DESC LIMIT 1")
	if err != nil {
		return 0, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			next = 0
		}
	}()
	row, err := s.Step()
	if err != nil || !row {
		return 0, err
	}
	if s.ColumnCount() != 1 {
		return 0, failure("state_corrupt")
	}
	kind, err := s.Kind(0)
	if err != nil {
		return 0, errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.IntegerKind {
		return 0, failure("state_corrupt")
	}
	ordinal, err := s.Int64(0)
	if err != nil {
		return 0, err
	}
	if ordinal < 0 {
		return 0, failure("state_corrupt")
	}
	if row, err = s.Step(); err != nil {
		return 0, err
	} else if row {
		return 0, failure("state_corrupt")
	}
	if ordinal == math.MaxInt64 {
		return 0, failure("validation")
	}
	return ordinal + 1, nil
}

func sqliteInsertEpoch(tx *sqliteio.Tx, row sqliteEpochRow) (delta int64, err error) {
	encoded, err := sqliteEncodeEpochRow(row)
	if err != nil {
		return 0, err
	}
	s, err := tx.Prepare("INSERT INTO epochs("+sqliteEpochColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING epoch_id", encoded.values[:]...)
	if err != nil {
		return 0, sqliteEpochWriteError(err)
	}
	defer func() {
		err = sqliteEpochWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	present, err := s.Step()
	if err != nil {
		return 0, err
	}
	if !present || s.ColumnCount() != 1 {
		return 0, failure("state_corrupt")
	}
	kind, err := s.Kind(0)
	if err != nil {
		return 0, errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.TextKind {
		return 0, failure("state_corrupt")
	}
	id, err := s.Text(0)
	if err != nil {
		return 0, err
	}
	if id != encoded.id {
		return 0, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return 0, err
	} else if present {
		return 0, failure("state_corrupt")
	}
	return encoded.charge, nil
}

func sqliteEpochRowCharge(row sqliteEpochRow) (int64, error) {
	encoded, err := sqliteEncodeEpochRow(row)
	return encoded.charge, err
}

func sqliteValidEpochRow(row sqliteEpochRow) bool {
	e := row.Value
	if row.Ordinal < 0 || !validUUID(e.ID) || !validUUID(e.ComputerID) || !validAttribution(e.Attribution) {
		return false
	}
	_, _, ok := sampleValues(e.Anchor)
	return ok
}

func sqliteEncodeEpochRow(row sqliteEpochRow) (sqliteEncodedEpoch, error) {
	if !sqliteValidEpochRow(row) {
		return sqliteEncodedEpoch{}, failure("validation")
	}
	e := row.Value
	clock, err := sqliteEncodeClock(e.Anchor, true)
	if err != nil {
		// Raw sample shape passed above; an unencodable explicit wall time is
		// invalid input. The stored-clock decoder retains state_corrupt instead.
		return sqliteEncodedEpoch{}, failure("validation")
	}
	a := e.Attribution
	// Compute grouping on the original attribution, before final text repair.
	texts := []string{e.ID, e.ComputerID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone,
		attributionKey(e.ComputerID, a), clock.Capability, clock.Wall.JSON,
		*clock.Epoch, *clock.ElapsedRaw, *clock.AwakeRaw}
	for i := range texts {
		texts[i] = sqlitePersistClockString(texts[i])
	}
	encoded := sqliteEncodedEpoch{id: texts[0], values: [18]sqliteio.Value{
		sqliteio.Text(texts[0]), sqliteio.Text(texts[1]), sqliteio.Text(texts[2]), sqliteio.Text(texts[3]),
		sqliteio.Text(texts[4]), sqliteio.Text(texts[5]), sqliteio.Text(texts[6]), sqliteio.Integer(row.Ordinal),
		sqliteio.Text(texts[7]), sqliteio.Text(texts[8]), sqliteio.Integer(clock.Wall.Seconds), sqliteio.Integer(clock.Wall.Nanoseconds),
		sqliteio.Text(texts[9]), sqliteio.Text(texts[10]), sqliteio.Text(texts[11]), sqliteio.Text(texts[12]),
		sqliteio.Blob(clock.ElapsedCounter), sqliteio.Blob(clock.AwakeCounter),
	}}
	encoded.charge, err = sqliteRowCharge(3, 0, texts, [][]byte{clock.ElapsedCounter, clock.AwakeCounter})
	if err != nil {
		return sqliteEncodedEpoch{}, err
	}
	return encoded, nil
}

func sqliteEpochWriteError(err error) error {
	var native *sqliteio.Error
	if errors.As(err, &native) && native.Category == sqliteio.Constraint && (native.Code == 1555 || native.Code == 2067) {
		// Only duplicate immutable ID or ordinal are identity conflicts here.
		return errors.Join(failure("validation"), err)
	}
	return err
}
