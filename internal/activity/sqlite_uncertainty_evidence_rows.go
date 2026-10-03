//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const sqliteUncertaintyEvidenceColumns = "uncertainty_id,detection_capability,detection_wall_sec,detection_wall_nsec,detection_wall_json,detection_epoch,detection_elapsed_raw,detection_awake_raw,last_confirmed_capability,last_confirmed_wall_sec,last_confirmed_wall_nsec,last_confirmed_wall_json,last_confirmed_epoch,last_confirmed_elapsed_raw,last_confirmed_awake_raw,last_confirmed_elapsed,last_confirmed_awake,bound_capability,bound_wall_sec,bound_wall_nsec,bound_wall_json,bound_epoch,bound_elapsed_raw,bound_awake_raw,bound_elapsed,bound_awake,missing_from,missing_through"

type sqliteEncodedUncertaintyEvidence struct {
	id     string
	values [28]sqliteio.Value
	charge int64
}

// Peer-row agreement belongs to final dependency validation. In particular,
// bound evidence may be staged before its uncertainty's UpperBound is replaced.
func sqliteReadUncertaintyEvidenceScalar(tx *sqliteio.Tx, id string) (result uncertaintyEvidence, found bool, err error) {
	if !validUUID(id) {
		return result, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteUncertaintyEvidenceColumns+" FROM uncertainty_evidence WHERE uncertainty_id=?", sqliteio.Text(id))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = uncertaintyEvidence{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	if s.ColumnCount() != 28 {
		return result, false, failure("state_corrupt")
	}
	var storedID string
	var detection sqliteClockValue
	last, bound := sqliteClockValue{Available: true}, sqliteClockValue{Available: true}
	boundNulls := 0
	for column := 0; column < 28; column++ {
		expected := sqliteio.TextKind
		switch column {
		case 2, 3, 9, 10, 18, 19:
			expected = sqliteio.IntegerKind
		case 15, 16, 24, 25:
			expected = sqliteio.BlobKind
		}
		kind, kindErr := s.Kind(column)
		if kindErr != nil {
			return result, false, errors.Join(failure("state_corrupt"), kindErr)
		}
		if kind == sqliteio.NullKind {
			switch {
			case column >= 5 && column <= 7:
			case column >= 17 && column <= 25:
				boundNulls++
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
			case 2:
				detection.Wall.Seconds = value
			case 3:
				detection.Wall.Nanoseconds = value
			case 9:
				last.Wall.Seconds = value
			case 10:
				last.Wall.Nanoseconds = value
			case 18:
				bound.Wall.Seconds = value
			case 19:
				bound.Wall.Nanoseconds = value
			}
		case sqliteio.BlobKind:
			value, readErr := s.Blob(column)
			if readErr != nil {
				return result, false, readErr
			}
			switch column {
			case 15:
				last.ElapsedCounter = value
			case 16:
				last.AwakeCounter = value
			case 24:
				bound.ElapsedCounter = value
			case 25:
				bound.AwakeCounter = value
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
				storedID = value
			case 1:
				detection.Capability = value
			case 4:
				detection.Wall.JSON = value
			case 5:
				detection.Epoch = sqliteCopyString(&value)
			case 6:
				detection.ElapsedRaw = sqliteCopyString(&value)
			case 7:
				detection.AwakeRaw = sqliteCopyString(&value)
			case 8:
				last.Capability = value
			case 11:
				last.Wall.JSON = value
			case 12:
				last.Epoch = sqliteCopyString(&value)
			case 13:
				last.ElapsedRaw = sqliteCopyString(&value)
			case 14:
				last.AwakeRaw = sqliteCopyString(&value)
			case 17:
				bound.Capability = value
			case 20:
				bound.Wall.JSON = value
			case 21:
				bound.Epoch = sqliteCopyString(&value)
			case 22:
				bound.ElapsedRaw = sqliteCopyString(&value)
			case 23:
				bound.AwakeRaw = sqliteCopyString(&value)
			case 26:
				result.MissingFrom = value
			case 27:
				result.MissingThrough = value
			}
		}
	}
	if storedID != id || boundNulls != 0 && boundNulls != 9 {
		return result, false, failure("state_corrupt")
	}
	result.Detection, err = sqliteDecodeClock(detection)
	if err != nil {
		return result, false, err
	}
	result.LastConfirmed, err = sqliteDecodeClock(last)
	if err != nil {
		return result, false, err
	}
	if boundNulls == 0 {
		sample, decodeErr := sqliteDecodeClock(bound)
		if decodeErr != nil {
			return result, false, decodeErr
		}
		result.BoundSample = &sample
	}
	if !sqliteValidUncertaintyEvidence(result) {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteWriteUncertaintyEvidence(tx *sqliteio.Tx, id string, before *uncertaintyEvidence, after uncertaintyEvidence) (delta int64, err error) {
	next, err := sqliteEncodeUncertaintyEvidence(id, after)
	if err != nil {
		return 0, err
	}
	var old sqliteEncodedUncertaintyEvidence
	if before != nil {
		old, err = sqliteEncodeUncertaintyEvidence(id, *before)
		if err != nil {
			return 0, err
		}
	}
	var s *sqliteio.Stmt
	if before == nil {
		s, err = tx.Prepare("INSERT INTO uncertainty_evidence("+sqliteUncertaintyEvidenceColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING uncertainty_id", next.values[:]...)
	} else {
		values := make([]sqliteio.Value, 0, 55)
		values = append(values, next.values[1:]...)
		values = append(values, old.values[:]...)
		s, err = tx.Prepare(`UPDATE uncertainty_evidence SET detection_capability=?,detection_wall_sec=?,detection_wall_nsec=?,detection_wall_json=?,
			detection_epoch=?,detection_elapsed_raw=?,detection_awake_raw=?,
			last_confirmed_capability=?,last_confirmed_wall_sec=?,last_confirmed_wall_nsec=?,last_confirmed_wall_json=?,
			last_confirmed_epoch=?,last_confirmed_elapsed_raw=?,last_confirmed_awake_raw=?,last_confirmed_elapsed=?,last_confirmed_awake=?,
			bound_capability=?,bound_wall_sec=?,bound_wall_nsec=?,bound_wall_json=?,bound_epoch=?,bound_elapsed_raw=?,bound_awake_raw=?,bound_elapsed=?,bound_awake=?,
			missing_from=?,missing_through=?
			WHERE uncertainty_id=? AND detection_capability=? AND detection_wall_sec=? AND detection_wall_nsec=? AND detection_wall_json=?
			AND detection_epoch IS ? AND detection_elapsed_raw IS ? AND detection_awake_raw IS ?
			AND last_confirmed_capability=? AND last_confirmed_wall_sec=? AND last_confirmed_wall_nsec=? AND last_confirmed_wall_json=?
			AND last_confirmed_epoch=? AND last_confirmed_elapsed_raw=? AND last_confirmed_awake_raw=? AND last_confirmed_elapsed=? AND last_confirmed_awake=?
			AND bound_capability IS ? AND bound_wall_sec IS ? AND bound_wall_nsec IS ? AND bound_wall_json IS ?
			AND bound_epoch IS ? AND bound_elapsed_raw IS ? AND bound_awake_raw IS ? AND bound_elapsed IS ? AND bound_awake IS ?
			AND missing_from=? AND missing_through=? RETURNING uncertainty_id`, values...)
	}
	if err != nil {
		return 0, sqliteUncertaintyWriteError(err)
	}
	defer func() {
		err = sqliteUncertaintyWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteUncertaintyReturnedID(s, next.id); err != nil {
		return 0, err
	}
	return next.charge - old.charge, nil
}

func sqliteUncertaintyEvidenceCharge(id string, row uncertaintyEvidence) (int64, error) {
	encoded, err := sqliteEncodeUncertaintyEvidence(id, row)
	return encoded.charge, err
}

func sqliteValidUncertaintyEvidence(row uncertaintyEvidence) bool {
	if row.Detection.WallUTC.IsZero() {
		return false
	}
	if _, _, ok := sampleValues(row.LastConfirmed); !ok {
		return false
	}
	if row.BoundSample != nil {
		if _, _, ok := sampleValues(*row.BoundSample); !ok {
			return false
		}
	}
	return true
}

func sqliteEncodeUncertaintyEvidence(id string, row uncertaintyEvidence) (sqliteEncodedUncertaintyEvidence, error) {
	if !validUUID(id) || !sqliteValidUncertaintyEvidence(row) {
		return sqliteEncodedUncertaintyEvidence{}, failure("validation")
	}
	detection, err := sqliteEncodeClock(row.Detection, false)
	if err != nil {
		return sqliteEncodedUncertaintyEvidence{}, failure("validation")
	}
	last, err := sqliteEncodeClock(row.LastConfirmed, true)
	if err != nil {
		return sqliteEncodedUncertaintyEvidence{}, failure("validation")
	}
	var bound sqliteClockValue
	if row.BoundSample != nil {
		bound, err = sqliteEncodeClock(*row.BoundSample, true)
		if err != nil {
			return sqliteEncodedUncertaintyEvidence{}, failure("validation")
		}
	}
	// The clock codecs already copied/repaired their strings. Detection stays
	// raw7; no capability/counter policy or numeric projections are added to it.
	from, through := sqlitePersistClockString(row.MissingFrom), sqlitePersistClockString(row.MissingThrough)
	texts := []string{id, detection.Capability, detection.Wall.JSON, last.Capability, last.Wall.JSON, *last.Epoch, *last.ElapsedRaw, *last.AwakeRaw, from, through}
	blobs := [][]byte{last.ElapsedCounter, last.AwakeCounter}
	integers, nulls := 4, 12
	encoded := sqliteEncodedUncertaintyEvidence{id: id, values: [28]sqliteio.Value{
		sqliteio.Text(id), sqliteio.Text(detection.Capability), sqliteio.Integer(detection.Wall.Seconds), sqliteio.Integer(detection.Wall.Nanoseconds), sqliteio.Text(detection.Wall.JSON),
		sqliteio.Null(), sqliteio.Null(), sqliteio.Null(),
		sqliteio.Text(last.Capability), sqliteio.Integer(last.Wall.Seconds), sqliteio.Integer(last.Wall.Nanoseconds), sqliteio.Text(last.Wall.JSON),
		sqliteio.Text(*last.Epoch), sqliteio.Text(*last.ElapsedRaw), sqliteio.Text(*last.AwakeRaw), sqliteio.Blob(last.ElapsedCounter), sqliteio.Blob(last.AwakeCounter),
		sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(),
		sqliteio.Text(from), sqliteio.Text(through),
	}}
	for index, value := range []*string{detection.Epoch, detection.ElapsedRaw, detection.AwakeRaw} {
		if value != nil {
			encoded.values[5+index] = sqliteio.Text(*value)
			texts = append(texts, *value)
			nulls--
		}
	}
	if row.BoundSample != nil {
		encoded.values[17], encoded.values[18], encoded.values[19] = sqliteio.Text(bound.Capability), sqliteio.Integer(bound.Wall.Seconds), sqliteio.Integer(bound.Wall.Nanoseconds)
		encoded.values[20], encoded.values[21], encoded.values[22], encoded.values[23] = sqliteio.Text(bound.Wall.JSON), sqliteio.Text(*bound.Epoch), sqliteio.Text(*bound.ElapsedRaw), sqliteio.Text(*bound.AwakeRaw)
		encoded.values[24], encoded.values[25] = sqliteio.Blob(bound.ElapsedCounter), sqliteio.Blob(bound.AwakeCounter)
		integers, nulls = integers+2, nulls-9
		texts = append(texts, bound.Capability, bound.Wall.JSON, *bound.Epoch, *bound.ElapsedRaw, *bound.AwakeRaw)
		blobs = append(blobs, bound.ElapsedCounter, bound.AwakeCounter)
	}
	encoded.charge, err = sqliteRowCharge(integers, nulls, texts, blobs)
	if err != nil {
		return sqliteEncodedUncertaintyEvidence{}, err
	}
	return encoded, nil
}
