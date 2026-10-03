//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"math"
	"time"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const sqliteSegmentLocalColumns = "segment_id,actor_key,actor_generation,binding_id,binding_revision,account_id,user_id,project_id,task_id,timezone,computer_id,group_order,epoch_id,start_sample_capability,start_sample_wall_sec,start_sample_wall_nsec,start_sample_wall_json,start_sample_epoch,start_sample_elapsed_raw,start_sample_awake_raw,start_sample_elapsed,start_sample_awake,confirmed_sample_capability,confirmed_sample_wall_sec,confirmed_sample_wall_nsec,confirmed_sample_wall_json,confirmed_sample_epoch,confirmed_sample_elapsed_raw,confirmed_sample_awake_raw,confirmed_sample_elapsed,confirmed_sample_awake,start_sec,start_nsec,start_json,confirmed_sec,confirmed_nsec,confirmed_json,end_sec,end_nsec,end_json,uncertainty_id,finalized"

type sqliteSegmentLocalRow struct {
	ID                           string
	Actor                        ActorRef
	Binding                      BindingSnapshot
	EpochID                      string
	StartSample, ConfirmedSample ClockSample
	Start, Confirmed             time.Time
	End                          *time.Time
	UncertaintyID                *string
	Finalized                    bool
}

type sqliteEncodedSegmentLocal struct {
	id     string
	actor  ActorRef
	values [42]sqliteio.Value
	charge int64
}

// Epoch projection, uncertainty and working-head relationships are deliberately
// left to the operation's final dependency validation after all rows are staged.
func sqliteReadSegmentLocal(tx *sqliteio.Tx, computer, id string) (result sqliteSegmentLocalRow, found bool, err error) {
	if !validUUID(computer) || !validUUID(id) {
		return result, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteSegmentLocalColumns+" FROM segments WHERE segment_id=?", sqliteio.Text(id))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteSegmentLocalRow{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	if s.ColumnCount() != 42 {
		return result, false, failure("state_corrupt")
	}
	var key, generation, storedComputer, group string
	for column := 0; column < 13; column++ {
		expected := sqliteio.TextKind
		if column == 2 || column == 4 {
			expected = sqliteio.BlobKind
		}
		kind, kindErr := s.Kind(column)
		if kindErr != nil {
			return result, false, errors.Join(failure("state_corrupt"), kindErr)
		}
		if kind != expected {
			return result, false, failure("state_corrupt")
		}
		if kind == sqliteio.BlobKind {
			value, readErr := s.Blob(column)
			if readErr != nil {
				return result, false, readErr
			}
			decoded, decodeErr := sqliteDecodeUint64(value)
			if decodeErr != nil {
				return result, false, decodeErr
			}
			if column == 2 {
				generation = decoded
			} else {
				result.Binding.Revision = decoded
			}
			continue
		}
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
		case 1:
			key = value
		case 3:
			result.Binding.ID = value
		case 5:
			result.Binding.Attribution.AccountID = value
		case 6:
			result.Binding.Attribution.UserID = value
		case 7:
			result.Binding.Attribution.ProjectID = value
		case 8:
			result.Binding.Attribution.TaskID = value
		case 9:
			result.Binding.Attribution.Timezone = value
		case 10:
			storedComputer = value
		case 11:
			group = value
		case 12:
			result.EpochID = value
		}
	}
	result.StartSample, err = sqliteReadLocalAvailableClock(s, 13)
	if err != nil {
		return result, false, err
	}
	result.ConfirmedSample, err = sqliteReadLocalAvailableClock(s, 22)
	if err != nil {
		return result, false, err
	}
	var start, confirmed, end sqliteTimeValue
	endNulls := 0
	for column := 31; column < 42; column++ {
		expected := sqliteio.IntegerKind
		switch column {
		case 33, 36, 39, 40:
			expected = sqliteio.TextKind
		}
		kind, kindErr := s.Kind(column)
		if kindErr != nil {
			return result, false, errors.Join(failure("state_corrupt"), kindErr)
		}
		if kind == sqliteio.NullKind {
			if column >= 37 && column <= 39 {
				endNulls++
			} else if column != 40 {
				return result, false, failure("state_corrupt")
			}
			continue
		}
		if kind != expected {
			return result, false, failure("state_corrupt")
		}
		if kind == sqliteio.IntegerKind {
			value, readErr := s.Int64(column)
			if readErr != nil {
				return result, false, readErr
			}
			switch column {
			case 31:
				start.Seconds = value
			case 32:
				start.Nanoseconds = value
			case 34:
				confirmed.Seconds = value
			case 35:
				confirmed.Nanoseconds = value
			case 37:
				end.Seconds = value
			case 38:
				end.Nanoseconds = value
			case 41:
				if value != 0 && value != 1 {
					return result, false, failure("state_corrupt")
				}
				result.Finalized = value == 1
			}
			continue
		}
		value, readErr := s.Text(column)
		if readErr != nil {
			return result, false, readErr
		}
		if !utf8.ValidString(value) {
			return result, false, failure("state_corrupt")
		}
		switch column {
		case 33:
			start.JSON = value
		case 36:
			confirmed.JSON = value
		case 39:
			end.JSON = value
		case 40:
			result.UncertaintyID = &value
		}
	}
	if endNulls != 0 && endNulls != 3 {
		return result, false, failure("state_corrupt")
	}
	result.Start, err = sqliteDecodeTime(start)
	if err != nil {
		return result, false, err
	}
	result.Confirmed, err = sqliteDecodeTime(confirmed)
	if err != nil {
		return result, false, err
	}
	if endNulls == 0 {
		result.End, err = sqliteDecodeOptionalTime(&end)
		if err != nil {
			return result, false, err
		}
	}
	result.Actor, err = sqliteReadHostReceiptGeneration(tx, key, generation)
	if err != nil {
		return result, false, err
	}
	if result.ID != id || storedComputer != computer || result.Actor.Key.ComputerID != computer || group != attributionKey(computer, result.Binding.Attribution) || !sqliteValidSegmentLocal(result) {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteWriteSegmentLocal(tx *sqliteio.Tx, computer string, before *sqliteSegmentLocalRow, after sqliteSegmentLocalRow) (delta int64, err error) {
	if !validUUID(computer) || after.Actor.Key.ComputerID != computer || before != nil && (before.ID != after.ID || before.Actor.Key.ComputerID != computer) {
		return 0, failure("validation")
	}
	next, err := sqliteEncodeSegmentLocal(after)
	if err != nil {
		return 0, err
	}
	var old sqliteEncodedSegmentLocal
	if before != nil {
		old, err = sqliteEncodeSegmentLocal(*before)
		if err != nil {
			return 0, err
		}
	}
	ref, found, err := sqliteReadActorGeneration(tx, after.Actor)
	if err != nil {
		return 0, err
	}
	if !found || ref != next.actor {
		return 0, failure("state_corrupt")
	}
	var s *sqliteio.Stmt
	if before == nil {
		s, err = tx.Prepare("INSERT INTO segments("+sqliteSegmentLocalColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING segment_id", next.values[:]...)
	} else {
		values := make([]sqliteio.Value, 0, 83)
		values = append(values, next.values[1:]...)
		values = append(values, old.values[:]...)
		s, err = tx.Prepare(`UPDATE segments SET actor_key=?,actor_generation=?,binding_id=?,binding_revision=?,account_id=?,user_id=?,project_id=?,task_id=?,timezone=?,computer_id=?,group_order=?,epoch_id=?,
			start_sample_capability=?,start_sample_wall_sec=?,start_sample_wall_nsec=?,start_sample_wall_json=?,start_sample_epoch=?,start_sample_elapsed_raw=?,start_sample_awake_raw=?,start_sample_elapsed=?,start_sample_awake=?,
			confirmed_sample_capability=?,confirmed_sample_wall_sec=?,confirmed_sample_wall_nsec=?,confirmed_sample_wall_json=?,confirmed_sample_epoch=?,confirmed_sample_elapsed_raw=?,confirmed_sample_awake_raw=?,confirmed_sample_elapsed=?,confirmed_sample_awake=?,
			start_sec=?,start_nsec=?,start_json=?,confirmed_sec=?,confirmed_nsec=?,confirmed_json=?,end_sec=?,end_nsec=?,end_json=?,uncertainty_id=?,finalized=?
			WHERE segment_id=? AND actor_key=? AND actor_generation=? AND binding_id=? AND binding_revision=? AND account_id=? AND user_id=? AND project_id=? AND task_id=? AND timezone=? AND computer_id=? AND group_order=? AND epoch_id=?
			AND start_sample_capability=? AND start_sample_wall_sec=? AND start_sample_wall_nsec=? AND start_sample_wall_json=? AND start_sample_epoch=? AND start_sample_elapsed_raw=? AND start_sample_awake_raw=? AND start_sample_elapsed=? AND start_sample_awake=?
			AND confirmed_sample_capability=? AND confirmed_sample_wall_sec=? AND confirmed_sample_wall_nsec=? AND confirmed_sample_wall_json=? AND confirmed_sample_epoch=? AND confirmed_sample_elapsed_raw=? AND confirmed_sample_awake_raw=? AND confirmed_sample_elapsed=? AND confirmed_sample_awake=?
			AND start_sec=? AND start_nsec=? AND start_json=? AND confirmed_sec=? AND confirmed_nsec=? AND confirmed_json=?
			AND end_sec IS ? AND end_nsec IS ? AND end_json IS ? AND uncertainty_id IS ? AND finalized=? RETURNING segment_id`, values...)
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
	if err = sqliteLocalReturnedID(s, next.id); err != nil {
		return 0, err
	}
	return next.charge - old.charge, nil
}

func sqliteSegmentLocalCharge(row sqliteSegmentLocalRow) (int64, error) {
	encoded, err := sqliteEncodeSegmentLocal(row)
	return encoded.charge, err
}

func sqliteValidSegmentLocal(row sqliteSegmentLocalRow) bool {
	if !validUUID(row.ID) || !validRef(row.Actor) || !validBinding(row.Binding) || !validUUID(row.EpochID) || row.Confirmed.Before(row.Start) {
		return false
	}
	if row.UncertaintyID != nil && !validUUID(*row.UncertaintyID) {
		return false
	}
	if _, _, ok := sampleValues(row.StartSample); !ok {
		return false
	}
	_, _, ok := sampleValues(row.ConfirmedSample)
	return ok
}

func sqliteEncodeSegmentLocal(row sqliteSegmentLocalRow) (sqliteEncodedSegmentLocal, error) {
	if !sqliteValidSegmentLocal(row) {
		return sqliteEncodedSegmentLocal{}, failure("validation")
	}
	actor, err := sqliteEncodeGeneration(row.Actor)
	if err != nil {
		return sqliteEncodedSegmentLocal{}, err
	}
	startSample, err := sqliteEncodeClock(row.StartSample, true)
	if err != nil {
		return sqliteEncodedSegmentLocal{}, failure("validation")
	}
	confirmedSample, err := sqliteEncodeClock(row.ConfirmedSample, true)
	if err != nil {
		return sqliteEncodedSegmentLocal{}, failure("validation")
	}
	start, err := sqliteEncodeTime(row.Start)
	if err != nil {
		return sqliteEncodedSegmentLocal{}, failure("validation")
	}
	confirmed, err := sqliteEncodeTime(row.Confirmed)
	if err != nil {
		return sqliteEncodedSegmentLocal{}, failure("validation")
	}
	end, err := sqliteEncodeOptionalTime(row.End)
	if err != nil {
		return sqliteEncodedSegmentLocal{}, failure("validation")
	}
	generation, _ := sqliteEncodeUint64(actor.ref.Generation)
	bindingRevision, _ := sqliteEncodeUint64(row.Binding.Revision)
	a := row.Binding.Attribution
	texts := []string{row.ID, actor.key, row.Binding.ID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, actor.ref.Key.ComputerID, attributionKey(actor.ref.Key.ComputerID, a), row.EpochID,
		startSample.Capability, startSample.Wall.JSON, *startSample.Epoch, *startSample.ElapsedRaw, *startSample.AwakeRaw,
		confirmedSample.Capability, confirmedSample.Wall.JSON, *confirmedSample.Epoch, *confirmedSample.ElapsedRaw, *confirmedSample.AwakeRaw,
		start.JSON, confirmed.JSON}
	for i := range texts {
		texts[i] = sqlitePersistClockString(texts[i])
	}
	finalized := int64(0)
	if row.Finalized {
		finalized = 1
	}
	encoded := sqliteEncodedSegmentLocal{id: texts[0], actor: actor.ref, values: [42]sqliteio.Value{
		sqliteio.Text(texts[0]), sqliteio.Text(texts[1]), sqliteio.Blob(generation[:]), sqliteio.Text(texts[2]), sqliteio.Blob(bindingRevision[:]),
		sqliteio.Text(texts[3]), sqliteio.Text(texts[4]), sqliteio.Text(texts[5]), sqliteio.Text(texts[6]), sqliteio.Text(texts[7]), sqliteio.Text(texts[8]), sqliteio.Text(texts[9]), sqliteio.Text(texts[10]),
		sqliteio.Text(texts[11]), sqliteio.Integer(startSample.Wall.Seconds), sqliteio.Integer(startSample.Wall.Nanoseconds), sqliteio.Text(texts[12]), sqliteio.Text(texts[13]), sqliteio.Text(texts[14]), sqliteio.Text(texts[15]), sqliteio.Blob(startSample.ElapsedCounter), sqliteio.Blob(startSample.AwakeCounter),
		sqliteio.Text(texts[16]), sqliteio.Integer(confirmedSample.Wall.Seconds), sqliteio.Integer(confirmedSample.Wall.Nanoseconds), sqliteio.Text(texts[17]), sqliteio.Text(texts[18]), sqliteio.Text(texts[19]), sqliteio.Text(texts[20]), sqliteio.Blob(confirmedSample.ElapsedCounter), sqliteio.Blob(confirmedSample.AwakeCounter),
		sqliteio.Integer(start.Seconds), sqliteio.Integer(start.Nanoseconds), sqliteio.Text(texts[21]), sqliteio.Integer(confirmed.Seconds), sqliteio.Integer(confirmed.Nanoseconds), sqliteio.Text(texts[22]),
		sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Integer(finalized),
	}}
	integers, nulls := 9, 4
	if end != nil {
		encoded.values[37], encoded.values[38], encoded.values[39] = sqliteio.Integer(end.Seconds), sqliteio.Integer(end.Nanoseconds), sqliteio.Text(end.JSON)
		texts, integers, nulls = append(texts, end.JSON), integers+2, nulls-3
	}
	if row.UncertaintyID != nil {
		id := sqlitePersistClockString(*row.UncertaintyID)
		encoded.values[40] = sqliteio.Text(id)
		texts, nulls = append(texts, id), nulls-1
	}
	encoded.charge, err = sqliteRowCharge(integers, nulls, texts, [][]byte{generation[:], bindingRevision[:], startSample.ElapsedCounter, startSample.AwakeCounter, confirmedSample.ElapsedCounter, confirmedSample.AwakeCounter})
	if err != nil {
		return sqliteEncodedSegmentLocal{}, err
	}
	return encoded, nil
}

func sqliteSegmentEventsLocal(tx *sqliteio.Tx, computer, id string) (result []string, err error) {
	_, found, err := sqliteReadSegmentLocal(tx, computer, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, failure("state_corrupt")
	}
	s, err := tx.Prepare("SELECT segment_id,ordinal,event_reference FROM segment_events WHERE segment_id=? ORDER BY ordinal ASC", sqliteio.Text(id))
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
		ordinal, reference, readErr := sqliteReadLocalChild(s, id, false)
		if readErr != nil {
			return nil, readErr
		}
		if ordinal != int64(len(result)) {
			return nil, failure("state_corrupt")
		}
		result = append(result, reference)
	}
}

func sqliteNextSegmentEventOrdinalLocal(tx *sqliteio.Tx, computer, id string) (result int64, err error) {
	_, found, err := sqliteReadSegmentLocal(tx, computer, id)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, failure("state_corrupt")
	}
	s, err := tx.Prepare("SELECT segment_id,ordinal,event_reference FROM segment_events WHERE segment_id=? ORDER BY ordinal DESC LIMIT 1", sqliteio.Text(id))
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
	ordinal, _, err := sqliteReadLocalChild(s, id, false)
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

func sqliteAppendSegmentEventLocal(tx *sqliteio.Tx, computer, id string, ordinal int64, eventKey string) (delta int64, err error) {
	if ordinal < 0 {
		return 0, failure("validation")
	}
	next, err := sqliteNextSegmentEventOrdinalLocal(tx, computer, id)
	if err != nil {
		return 0, err
	}
	if ordinal != next {
		return 0, failure("state_corrupt")
	}
	// Event references are opaque legacy array members; empty and duplicate
	// entries remain distinct rows, and repair occurs only at persistence.
	reference := sqlitePersistClockString(eventKey)
	charge, err := sqliteRowCharge(1, 0, []string{id, reference}, nil)
	if err != nil {
		return 0, err
	}
	s, err := tx.Prepare("INSERT INTO segment_events(segment_id,ordinal,event_reference) VALUES(?,?,?) RETURNING segment_id,ordinal", sqliteio.Text(id), sqliteio.Integer(ordinal), sqliteio.Text(reference))
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteLocalReturnedChild(s, id, ordinal); err != nil {
		return 0, err
	}
	return charge, nil
}
