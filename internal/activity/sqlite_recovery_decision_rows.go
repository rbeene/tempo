//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const sqliteRecoveryDecisionColumns = "uncertainty_id,request_id,previous_revision,resolution_end_sec,resolution_end_nsec,resolution_end_json,discarded,reason,observed_capability,observed_wall_sec,observed_wall_nsec,observed_wall_json,observed_epoch,observed_elapsed_raw,observed_awake_raw,discarded_start_sec,discarded_start_nsec,discarded_start_json,discarded_end_sec,discarded_end_nsec,discarded_end_json"

type sqliteEncodedRecoveryDecision struct {
	id     string
	values [21]sqliteio.Value
	charge int64
}

// This local projection can be read before the request/uncertainty cycle is
// complete. Revision progression, endpoints and proof membership are later gates.
func sqliteReadRecoveryDecisionScalar(tx *sqliteio.Tx, id string) (result recoveryDecision, found bool, err error) {
	if !validUUID(id) {
		return result, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteRecoveryDecisionColumns+" FROM recovery_decisions WHERE uncertainty_id=?", sqliteio.Text(id))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = recoveryDecision{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	if s.ColumnCount() != 21 {
		return result, false, failure("state_corrupt")
	}
	var end, suffixStart, suffixEnd sqliteTimeValue
	var observed sqliteClockValue // raw7, including for nondiscarded decisions
	observedNulls, observedCoreNulls, suffixNulls := 0, 0, 0
	for column := 0; column < 21; column++ {
		expected := sqliteio.TextKind
		switch column {
		case 2:
			expected = sqliteio.BlobKind
		case 3, 4, 6, 9, 10, 15, 16, 18, 19:
			expected = sqliteio.IntegerKind
		}
		kind, kindErr := s.Kind(column)
		if kindErr != nil {
			return result, false, errors.Join(failure("state_corrupt"), kindErr)
		}
		if kind == sqliteio.NullKind {
			switch {
			case column >= 8 && column <= 14:
				observedNulls++
				if column <= 11 {
					observedCoreNulls++
				}
			case column >= 15:
				suffixNulls++
			default:
				return result, false, failure("state_corrupt")
			}
			continue
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
			case 3:
				end.Seconds = value
			case 4:
				end.Nanoseconds = value
			case 6:
				if value != 0 && value != 1 {
					return result, false, failure("state_corrupt")
				}
				result.Discarded = value == 1
			case 9:
				observed.Wall.Seconds = value
			case 10:
				observed.Wall.Nanoseconds = value
			case 15:
				suffixStart.Seconds = value
			case 16:
				suffixStart.Nanoseconds = value
			case 18:
				suffixEnd.Seconds = value
			case 19:
				suffixEnd.Nanoseconds = value
			}
		case sqliteio.BlobKind:
			value, readErr := s.Blob(column)
			if readErr != nil {
				return result, false, readErr
			}
			result.PreviousRevision, err = sqliteDecodeUint64(value)
			if err != nil {
				return result, false, err
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
				result.UncertaintyID = value
			case 1:
				result.RequestID = value
			case 5:
				end.JSON = value
			case 7:
				result.Reason = value
			case 8:
				observed.Capability = value
			case 11:
				observed.Wall.JSON = value
			case 12:
				observed.Epoch = sqliteCopyString(&value)
			case 13:
				observed.ElapsedRaw = sqliteCopyString(&value)
			case 14:
				observed.AwakeRaw = sqliteCopyString(&value)
			case 17:
				suffixStart.JSON = value
			case 20:
				suffixEnd.JSON = value
			}
		}
	}
	if result.UncertaintyID != id || observedNulls != 7 && observedCoreNulls != 0 || suffixNulls != 0 && suffixNulls != 6 {
		return result, false, failure("state_corrupt")
	}
	result.ResolutionEnd, err = sqliteDecodeTime(end)
	if err != nil {
		return result, false, err
	}
	if observedNulls != 7 {
		sample, decodeErr := sqliteDecodeClock(observed)
		if decodeErr != nil {
			return result, false, decodeErr
		}
		result.ObservedSample = &sample
	}
	if suffixNulls == 0 {
		start, decodeErr := sqliteDecodeTime(suffixStart)
		if decodeErr != nil {
			return result, false, decodeErr
		}
		end, decodeErr := sqliteDecodeTime(suffixEnd)
		if decodeErr != nil {
			return result, false, decodeErr
		}
		result.DiscardedSuffix = &TimeRange{Start: start, End: end}
	}
	if !sqliteValidRecoveryDecision(result) {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteInsertRecoveryDecision(tx *sqliteio.Tx, row recoveryDecision) (delta int64, err error) {
	encoded, err := sqliteEncodeRecoveryDecision(row)
	if err != nil {
		return 0, err
	}
	s, err := tx.Prepare("INSERT INTO recovery_decisions("+sqliteRecoveryDecisionColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING uncertainty_id", encoded.values[:]...)
	if err != nil {
		return 0, sqliteRecoveryWriteError(err)
	}
	defer func() {
		err = sqliteRecoveryWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteRecoveryReturnedID(s, encoded.id); err != nil {
		return 0, err
	}
	return encoded.charge, nil
}

func sqliteRecoveryDecisionCharge(row recoveryDecision) (int64, error) {
	encoded, err := sqliteEncodeRecoveryDecision(row)
	return encoded.charge, err
}

func sqliteValidRecoveryDecision(row recoveryDecision) bool {
	previous, ok := counter(row.PreviousRevision)
	if !validUUID(row.UncertaintyID) || !validUUID(row.RequestID) || !ok || previous == 0 || !validReason(row.Reason) {
		return false
	}
	if !row.Discarded {
		if row.ObservedSample == nil {
			return false
		}
		if _, _, ok := sampleValues(*row.ObservedSample); !ok {
			return false
		}
	}
	// MaxUint64 previous revision and endpoint agreement need the final graph;
	// no peer-row policy is inferred by this local immutable row codec.
	return true
}

func sqliteEncodeRecoveryDecision(row recoveryDecision) (sqliteEncodedRecoveryDecision, error) {
	if !sqliteValidRecoveryDecision(row) {
		return sqliteEncodedRecoveryDecision{}, failure("validation")
	}
	previous, err := sqliteEncodeUint64(row.PreviousRevision)
	if err != nil {
		return sqliteEncodedRecoveryDecision{}, err
	}
	end, err := sqliteEncodeTime(row.ResolutionEnd)
	if err != nil {
		return sqliteEncodedRecoveryDecision{}, failure("validation")
	}
	var observed sqliteClockValue
	if row.ObservedSample != nil {
		// Raw admission above precedes repair. Even an available observation is
		// stored as raw7; discarded samples acquire no available-clock policy.
		observed, err = sqliteEncodeClock(*row.ObservedSample, false)
		if err != nil {
			return sqliteEncodedRecoveryDecision{}, failure("validation")
		}
	}
	var suffixStart, suffixEnd sqliteTimeValue
	if row.DiscardedSuffix != nil {
		suffixStart, err = sqliteEncodeTime(row.DiscardedSuffix.Start)
		if err != nil {
			return sqliteEncodedRecoveryDecision{}, failure("validation")
		}
		suffixEnd, err = sqliteEncodeTime(row.DiscardedSuffix.End)
		if err != nil {
			return sqliteEncodedRecoveryDecision{}, failure("validation")
		}
	}
	discarded := int64(0)
	if row.Discarded {
		discarded = 1
	}
	encoded := sqliteEncodedRecoveryDecision{id: row.UncertaintyID, values: [21]sqliteio.Value{
		sqliteio.Text(row.UncertaintyID), sqliteio.Text(row.RequestID), sqliteio.Blob(previous[:]),
		sqliteio.Integer(end.Seconds), sqliteio.Integer(end.Nanoseconds), sqliteio.Text(end.JSON), sqliteio.Integer(discarded), sqliteio.Text(row.Reason),
		sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(),
		sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(),
	}}
	integers, nulls := 3, 13
	texts := []string{row.UncertaintyID, row.RequestID, end.JSON, row.Reason}
	if row.ObservedSample != nil {
		encoded.values[8], encoded.values[9], encoded.values[10], encoded.values[11] = sqliteio.Text(observed.Capability), sqliteio.Integer(observed.Wall.Seconds), sqliteio.Integer(observed.Wall.Nanoseconds), sqliteio.Text(observed.Wall.JSON)
		integers, nulls = integers+2, nulls-4
		texts = append(texts, observed.Capability, observed.Wall.JSON)
		for index, value := range []*string{observed.Epoch, observed.ElapsedRaw, observed.AwakeRaw} {
			if value != nil {
				encoded.values[12+index] = sqliteio.Text(*value)
				texts = append(texts, *value)
				nulls--
			}
		}
	}
	if row.DiscardedSuffix != nil {
		encoded.values[15], encoded.values[16], encoded.values[17] = sqliteio.Integer(suffixStart.Seconds), sqliteio.Integer(suffixStart.Nanoseconds), sqliteio.Text(suffixStart.JSON)
		encoded.values[18], encoded.values[19], encoded.values[20] = sqliteio.Integer(suffixEnd.Seconds), sqliteio.Integer(suffixEnd.Nanoseconds), sqliteio.Text(suffixEnd.JSON)
		integers, nulls = integers+4, nulls-6
		texts = append(texts, suffixStart.JSON, suffixEnd.JSON)
	}
	encoded.charge, err = sqliteRowCharge(integers, nulls, texts, [][]byte{previous[:]})
	if err != nil {
		return sqliteEncodedRecoveryDecision{}, err
	}
	return encoded, nil
}

// The two insert-only recovery projections return their explicit identity.
func sqliteRecoveryReturnedID(s *sqliteio.Stmt, expected string) error {
	present, err := s.Step()
	if err != nil {
		return err
	}
	if !present || s.ColumnCount() != 1 {
		return failure("state_corrupt")
	}
	kind, err := s.Kind(0)
	if err != nil {
		return errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.TextKind {
		return failure("state_corrupt")
	}
	id, err := s.Text(0)
	if err != nil {
		return err
	}
	if id != expected {
		return failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return err
	} else if present {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteRecoveryWriteError(err error) error {
	var native *sqliteio.Error
	if errors.As(err, &native) && native.Category == sqliteio.Constraint && (native.Code == 1555 || native.Code == 2067) {
		return errors.Join(failure("validation"), err)
	}
	return err
}
