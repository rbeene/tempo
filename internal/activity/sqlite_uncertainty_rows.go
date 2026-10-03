//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const sqliteUncertaintyColumns = "uncertainty_id,revision,actor_key,actor_generation,segment_id,account_id,user_id,project_id,task_id,timezone,computer_id,lower_bound_sec,lower_bound_nsec,lower_bound_json,upper_bound_sec,upper_bound_nsec,upper_bound_json,reason,state,resolution_end_sec,resolution_end_nsec,resolution_end_json,discarded"

type sqliteEncodedUncertainty struct {
	id     string
	actor  ActorRef
	values [23]sqliteio.Value
	charge int64
}

// This selected scalar read does not validate the segment/evidence/recovery
// cycle. The operation composer must validate those relationships after staging.
func sqliteReadUncertaintyScalar(tx *sqliteio.Tx, computer, id string) (result Uncertainty, found bool, err error) {
	if !validUUID(computer) || !validUUID(id) {
		return result, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteUncertaintyColumns+" FROM uncertainties WHERE uncertainty_id=?", sqliteio.Text(id))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = Uncertainty{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	if s.ColumnCount() != 23 {
		return result, false, failure("state_corrupt")
	}
	var key, generation, storedComputer string
	var lower, upper, end sqliteTimeValue
	upperNulls, endNulls := 0, 0
	for column := 0; column < 23; column++ {
		expected := sqliteio.TextKind
		switch column {
		case 1, 3:
			expected = sqliteio.BlobKind
		case 11, 12, 14, 15, 19, 20, 22:
			expected = sqliteio.IntegerKind
		}
		kind, kindErr := s.Kind(column)
		if kindErr != nil {
			return result, false, errors.Join(failure("state_corrupt"), kindErr)
		}
		if kind == sqliteio.NullKind {
			switch {
			case column >= 14 && column <= 16:
				upperNulls++
			case column >= 19 && column <= 21:
				endNulls++
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
			case 11:
				lower.Seconds = value
			case 12:
				lower.Nanoseconds = value
			case 14:
				upper.Seconds = value
			case 15:
				upper.Nanoseconds = value
			case 19:
				end.Seconds = value
			case 20:
				end.Nanoseconds = value
			case 22:
				if value != 0 && value != 1 {
					return result, false, failure("state_corrupt")
				}
				result.Discarded = value == 1
			}
		case sqliteio.BlobKind:
			value, readErr := s.Blob(column)
			if readErr != nil {
				return result, false, readErr
			}
			decoded, decodeErr := sqliteDecodeUint64(value)
			if decodeErr != nil {
				return result, false, decodeErr
			}
			if column == 1 {
				result.Revision = decoded
			} else {
				generation = decoded
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
				result.ID = value
			case 2:
				key = value
			case 4:
				result.SegmentID = value
			case 5:
				result.Attribution.AccountID = value
			case 6:
				result.Attribution.UserID = value
			case 7:
				result.Attribution.ProjectID = value
			case 8:
				result.Attribution.TaskID = value
			case 9:
				result.Attribution.Timezone = value
			case 10:
				storedComputer = value
			case 13:
				lower.JSON = value
			case 16:
				upper.JSON = value
			case 17:
				result.Reason = value
			case 18:
				result.State = value
			case 21:
				end.JSON = value
			}
		}
	}
	if upperNulls != 0 && upperNulls != 3 || endNulls != 0 && endNulls != 3 {
		return result, false, failure("state_corrupt")
	}
	result.LowerBound, err = sqliteDecodeTime(lower)
	if err != nil {
		return result, false, err
	}
	if upperNulls == 0 {
		result.UpperBound, err = sqliteDecodeOptionalTime(&upper)
		if err != nil {
			return result, false, err
		}
	}
	if endNulls == 0 {
		result.ResolutionEnd, err = sqliteDecodeOptionalTime(&end)
		if err != nil {
			return result, false, err
		}
	}
	result.Actor, err = sqliteReadHostReceiptGeneration(tx, key, generation)
	if err != nil {
		return result, false, err
	}
	if result.ID != id || storedComputer != computer || result.Actor.Key.ComputerID != storedComputer || !sqliteValidUncertaintyScalar(result) {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteWriteUncertainty(tx *sqliteio.Tx, computer string, before *Uncertainty, after Uncertainty) (delta int64, err error) {
	if !validUUID(computer) || after.Actor.Key.ComputerID != computer || before != nil && (before.ID != after.ID || before.Actor.Key.ComputerID != computer) {
		return 0, failure("validation")
	}
	next, err := sqliteEncodeUncertainty(after)
	if err != nil {
		return 0, err
	}
	var old sqliteEncodedUncertainty
	if before != nil {
		old, err = sqliteEncodeUncertainty(*before)
		if err != nil {
			return 0, err
		}
	}
	// Generation identities are immutable and acyclic; no current head or
	// temporarily staged segment/evidence relationship is consulted here.
	ref, found, err := sqliteReadActorGeneration(tx, after.Actor)
	if err != nil {
		return 0, err
	}
	if !found || ref != next.actor {
		return 0, failure("state_corrupt")
	}
	var s *sqliteio.Stmt
	if before == nil {
		s, err = tx.Prepare("INSERT INTO uncertainties("+sqliteUncertaintyColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING uncertainty_id", next.values[:]...)
	} else {
		values := make([]sqliteio.Value, 0, 45)
		values = append(values, next.values[1:]...)
		values = append(values, old.values[:]...)
		s, err = tx.Prepare(`UPDATE uncertainties SET revision=?,actor_key=?,actor_generation=?,segment_id=?,
			account_id=?,user_id=?,project_id=?,task_id=?,timezone=?,computer_id=?,
			lower_bound_sec=?,lower_bound_nsec=?,lower_bound_json=?,upper_bound_sec=?,upper_bound_nsec=?,upper_bound_json=?,
			reason=?,state=?,resolution_end_sec=?,resolution_end_nsec=?,resolution_end_json=?,discarded=?
			WHERE uncertainty_id=? AND revision=? AND actor_key=? AND actor_generation=? AND segment_id=?
			AND account_id=? AND user_id=? AND project_id=? AND task_id=? AND timezone=? AND computer_id=?
			AND lower_bound_sec=? AND lower_bound_nsec=? AND lower_bound_json=?
			AND upper_bound_sec IS ? AND upper_bound_nsec IS ? AND upper_bound_json IS ? AND reason=? AND state=?
			AND resolution_end_sec IS ? AND resolution_end_nsec IS ? AND resolution_end_json IS ? AND discarded=? RETURNING uncertainty_id`, values...)
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

func sqliteUncertaintyCharge(row Uncertainty) (int64, error) {
	encoded, err := sqliteEncodeUncertainty(row)
	return encoded.charge, err
}

func sqliteValidUncertaintyScalar(row Uncertainty) bool {
	if !validUUID(row.ID) || !validRef(row.Actor) || !validUUID(row.SegmentID) || !validAttribution(row.Attribution) || row.LowerBound.IsZero() {
		return false
	}
	if revision, ok := counter(row.Revision); !ok || revision == 0 {
		return false
	}
	if row.UpperBound != nil && row.UpperBound.Before(row.LowerBound) {
		return false
	}
	switch row.Reason {
	case "source_lost", "ordering_unavailable", "suspend", "clock_changed", "restart_unknown", "event_gap", "superseded":
	default:
		return false
	}
	switch row.State {
	case "unresolved":
		return row.ResolutionEnd == nil && !row.Discarded
	case "resolved":
		return row.ResolutionEnd != nil
	default:
		return false
	}
}

func sqliteEncodeUncertainty(row Uncertainty) (sqliteEncodedUncertainty, error) {
	if !sqliteValidUncertaintyScalar(row) {
		return sqliteEncodedUncertainty{}, failure("validation")
	}
	actor, err := sqliteEncodeGeneration(row.Actor)
	if err != nil {
		return sqliteEncodedUncertainty{}, err
	}
	revision, err := sqliteEncodeUint64(row.Revision)
	if err != nil {
		return sqliteEncodedUncertainty{}, err
	}
	generation, err := sqliteEncodeUint64(actor.ref.Generation)
	if err != nil {
		return sqliteEncodedUncertainty{}, err
	}
	lower, err := sqliteEncodeTime(row.LowerBound)
	if err != nil {
		return sqliteEncodedUncertainty{}, failure("validation")
	}
	upper, err := sqliteEncodeOptionalTime(row.UpperBound)
	if err != nil {
		return sqliteEncodedUncertainty{}, failure("validation")
	}
	end, err := sqliteEncodeOptionalTime(row.ResolutionEnd)
	if err != nil {
		return sqliteEncodedUncertainty{}, failure("validation")
	}
	a := row.Attribution
	texts := []string{row.ID, actor.key, row.SegmentID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, actor.ref.Key.ComputerID, lower.JSON, row.Reason, row.State}
	for i := range texts {
		texts[i] = sqlitePersistClockString(texts[i])
	}
	discarded := int64(0)
	if row.Discarded {
		discarded = 1
	}
	encoded := sqliteEncodedUncertainty{id: texts[0], actor: actor.ref, values: [23]sqliteio.Value{
		sqliteio.Text(texts[0]), sqliteio.Blob(revision[:]), sqliteio.Text(texts[1]), sqliteio.Blob(generation[:]),
		sqliteio.Text(texts[2]), sqliteio.Text(texts[3]), sqliteio.Text(texts[4]), sqliteio.Text(texts[5]), sqliteio.Text(texts[6]), sqliteio.Text(texts[7]), sqliteio.Text(texts[8]),
		sqliteio.Integer(lower.Seconds), sqliteio.Integer(lower.Nanoseconds), sqliteio.Text(texts[9]),
		sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Text(texts[10]), sqliteio.Text(texts[11]),
		sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Integer(discarded),
	}}
	integers, nulls := 3, 6
	if upper != nil {
		encoded.values[14], encoded.values[15], encoded.values[16] = sqliteio.Integer(upper.Seconds), sqliteio.Integer(upper.Nanoseconds), sqliteio.Text(upper.JSON)
		integers, nulls = integers+2, nulls-3
		texts = append(texts, upper.JSON)
	}
	if end != nil {
		encoded.values[19], encoded.values[20], encoded.values[21] = sqliteio.Integer(end.Seconds), sqliteio.Integer(end.Nanoseconds), sqliteio.Text(end.JSON)
		integers, nulls = integers+2, nulls-3
		texts = append(texts, end.JSON)
	}
	encoded.charge, err = sqliteRowCharge(integers, nulls, texts, [][]byte{revision[:], generation[:]})
	if err != nil {
		return sqliteEncodedUncertainty{}, err
	}
	return encoded, nil
}

// Both local row writers return exactly their retained uncertainty identity.
func sqliteUncertaintyReturnedID(s *sqliteio.Stmt, expected string) error {
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

func sqliteUncertaintyWriteError(err error) error {
	var native *sqliteio.Error
	if errors.As(err, &native) && native.Category == sqliteio.Constraint && (native.Code == 1555 || native.Code == 2067) {
		return errors.Join(failure("validation"), err)
	}
	return err
}
