//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"math"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const sqliteActorLocalColumns = "actor_key,id,revision,generation,sequence,state,health,binding_id,binding_revision,account_id,user_id,project_id,task_id,timezone,computer_id,parent_key,parent_generation,segment_id,last_evidence_capability,last_evidence_wall_sec,last_evidence_wall_nsec,last_evidence_wall_json,last_evidence_epoch,last_evidence_elapsed_raw,last_evidence_awake_raw,last_evidence_elapsed,last_evidence_awake"

// Local rows intentionally exclude children and do not claim a valid domain graph.
type sqliteActorLocalRow struct {
	ID, Revision                                        string
	Ref                                                 ActorRef
	Sequence, State, Health, BindingID, BindingRevision string
	Attribution                                         Attribution
	Parent                                              *ActorRef
	SegmentID                                           *string
	LastEvidence                                        ClockSample
}

type sqliteEncodedActorLocal struct {
	key    string
	ref    ActorRef
	parent *ActorRef
	values [27]sqliteio.Value
	charge int64
}

func sqliteReadActorLocal(tx *sqliteio.Tx, computer string, key ActorKey) (result sqliteActorLocalRow, found bool, err error) {
	if !validUUID(computer) || !validKey(key) {
		return result, false, failure("validation")
	}
	rawKey := actorKey(key)
	s, err := tx.Prepare("SELECT "+sqliteActorLocalColumns+" FROM actors WHERE actor_key=?", sqliteio.Text(rawKey))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteActorLocalRow{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	if s.ColumnCount() != 27 {
		return result, false, failure("state_corrupt")
	}
	var storedKey, generation, storedComputer, parentKey, parentGeneration string
	parentNulls := 0
	for column := 0; column < 18; column++ {
		expected := sqliteio.TextKind
		switch column {
		case 2, 3, 4, 8, 16:
			expected = sqliteio.BlobKind
		}
		kind, kindErr := s.Kind(column)
		if kindErr != nil {
			return result, false, errors.Join(failure("state_corrupt"), kindErr)
		}
		if kind == sqliteio.NullKind {
			switch column {
			case 15, 16:
				parentNulls++
			case 17:
			default:
				return result, false, failure("state_corrupt")
			}
			continue
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
			switch column {
			case 2:
				result.Revision = decoded
			case 3:
				generation = decoded
			case 4:
				result.Sequence = decoded
			case 8:
				result.BindingRevision = decoded
			case 16:
				parentGeneration = decoded
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
			storedKey = value
		case 1:
			result.ID = value
		case 5:
			result.State = value
		case 6:
			result.Health = value
		case 7:
			result.BindingID = value
		case 9:
			result.Attribution.AccountID = value
		case 10:
			result.Attribution.UserID = value
		case 11:
			result.Attribution.ProjectID = value
		case 12:
			result.Attribution.TaskID = value
		case 13:
			result.Attribution.Timezone = value
		case 14:
			storedComputer = value
		case 15:
			parentKey = value
		case 17:
			result.SegmentID = &value
		}
	}
	if parentNulls != 0 && parentNulls != 2 {
		return result, false, failure("state_corrupt")
	}
	result.LastEvidence, err = sqliteReadLocalAvailableClock(s, 18)
	if err != nil {
		return result, false, err
	}
	result.Ref, err = sqliteReadHostReceiptGeneration(tx, storedKey, generation)
	if err != nil {
		return result, false, err
	}
	if parentNulls == 0 {
		parent, readErr := sqliteReadHostReceiptGeneration(tx, parentKey, parentGeneration)
		if readErr != nil {
			return result, false, readErr
		}
		result.Parent = &parent
	}
	materialized := key
	materialized.SessionID = sqlitePersistClockString(key.SessionID)
	materialized.AgentID = sqlitePersistClockString(key.AgentID)
	if storedKey != rawKey || result.Ref.Key != materialized || storedComputer != computer || result.Ref.Key.ComputerID != computer || !sqliteValidActorLocal(result) {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteWriteActorLocal(tx *sqliteio.Tx, computer string, before *sqliteActorLocalRow, after sqliteActorLocalRow) (delta int64, err error) {
	if !validUUID(computer) || after.Ref.Key.ComputerID != computer || before != nil && (before.ID != after.ID || before.Ref.Key.ComputerID != computer || actorKey(before.Ref.Key) != actorKey(after.Ref.Key)) {
		return 0, failure("validation")
	}
	next, err := sqliteEncodeActorLocal(after)
	if err != nil {
		return 0, err
	}
	var old sqliteEncodedActorLocal
	if before != nil {
		old, err = sqliteEncodeActorLocal(*before)
		if err != nil {
			return 0, err
		}
	}
	ref, found, err := sqliteReadActorGeneration(tx, after.Ref)
	if err != nil {
		return 0, err
	}
	if !found || ref != next.ref {
		return 0, failure("state_corrupt")
	}
	if after.Parent != nil {
		parent, present, readErr := sqliteReadActorGeneration(tx, *after.Parent)
		if readErr != nil {
			return 0, readErr
		}
		if !present || next.parent == nil || parent != *next.parent {
			return 0, failure("state_corrupt")
		}
	}
	var s *sqliteio.Stmt
	if before == nil {
		s, err = tx.Prepare("INSERT INTO actors("+sqliteActorLocalColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING actor_key", next.values[:]...)
	} else {
		values := make([]sqliteio.Value, 0, 53)
		values = append(values, next.values[1:]...)
		values = append(values, old.values[:]...)
		s, err = tx.Prepare(`UPDATE actors SET id=?,revision=?,generation=?,sequence=?,state=?,health=?,binding_id=?,binding_revision=?,
			account_id=?,user_id=?,project_id=?,task_id=?,timezone=?,computer_id=?,parent_key=?,parent_generation=?,segment_id=?,
			last_evidence_capability=?,last_evidence_wall_sec=?,last_evidence_wall_nsec=?,last_evidence_wall_json=?,last_evidence_epoch=?,
			last_evidence_elapsed_raw=?,last_evidence_awake_raw=?,last_evidence_elapsed=?,last_evidence_awake=?
			WHERE actor_key=? AND id=? AND revision=? AND generation=? AND sequence=? AND state=? AND health=? AND binding_id=? AND binding_revision=?
			AND account_id=? AND user_id=? AND project_id=? AND task_id=? AND timezone=? AND computer_id=?
			AND parent_key IS ? AND parent_generation IS ? AND segment_id IS ?
			AND last_evidence_capability=? AND last_evidence_wall_sec=? AND last_evidence_wall_nsec=? AND last_evidence_wall_json=? AND last_evidence_epoch=?
			AND last_evidence_elapsed_raw=? AND last_evidence_awake_raw=? AND last_evidence_elapsed=? AND last_evidence_awake=? RETURNING actor_key`, values...)
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
	if err = sqliteLocalReturnedID(s, next.key); err != nil {
		return 0, err
	}
	return next.charge - old.charge, nil
}

func sqliteActorLocalCharge(row sqliteActorLocalRow) (int64, error) {
	encoded, err := sqliteEncodeActorLocal(row)
	return encoded.charge, err
}

func sqliteValidActorLocal(row sqliteActorLocalRow) bool {
	if !validUUID(row.ID) || !validRef(row.Ref) || !validBinding(BindingSnapshot{ID: row.BindingID, Revision: row.BindingRevision, Attribution: row.Attribution}) {
		return false
	}
	if n, ok := counter(row.Revision); !ok || n == 0 {
		return false
	}
	if n, ok := counter(row.Sequence); !ok || n == 0 {
		return false
	}
	if row.Parent != nil && !validRef(*row.Parent) || row.SegmentID != nil && !validUUID(*row.SegmentID) {
		return false
	}
	switch row.State {
	case "working", "wait_user", "wait_permission", "wait_children", "interrupted", "finished":
	default:
		return false
	}
	switch row.Health {
	case "continuous", "stale", "order_blocked":
	default:
		return false
	}
	if (row.State == "working") != (row.SegmentID != nil) {
		return false
	}
	_, _, ok := sampleValues(row.LastEvidence)
	return ok
}

func sqliteEncodeActorLocal(row sqliteActorLocalRow) (sqliteEncodedActorLocal, error) {
	if !sqliteValidActorLocal(row) {
		return sqliteEncodedActorLocal{}, failure("validation")
	}
	actor, err := sqliteEncodeGeneration(row.Ref)
	if err != nil {
		return sqliteEncodedActorLocal{}, err
	}
	var parent sqliteEncodedGeneration
	if row.Parent != nil {
		parent, err = sqliteEncodeGeneration(*row.Parent)
		if err != nil {
			return sqliteEncodedActorLocal{}, err
		}
	}
	clock, err := sqliteEncodeClock(row.LastEvidence, true)
	if err != nil {
		return sqliteEncodedActorLocal{}, failure("validation")
	}
	revision, _ := sqliteEncodeUint64(row.Revision)
	generation, _ := sqliteEncodeUint64(row.Ref.Generation)
	sequence, _ := sqliteEncodeUint64(row.Sequence)
	bindingRevision, _ := sqliteEncodeUint64(row.BindingRevision)
	a := row.Attribution
	texts := []string{actor.key, row.ID, row.State, row.Health, row.BindingID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, actor.ref.Key.ComputerID,
		clock.Capability, clock.Wall.JSON, *clock.Epoch, *clock.ElapsedRaw, *clock.AwakeRaw}
	for i := range texts {
		texts[i] = sqlitePersistClockString(texts[i])
	}
	encoded := sqliteEncodedActorLocal{key: actor.key, ref: actor.ref, values: [27]sqliteio.Value{
		sqliteio.Text(texts[0]), sqliteio.Text(texts[1]), sqliteio.Blob(revision[:]), sqliteio.Blob(generation[:]), sqliteio.Blob(sequence[:]),
		sqliteio.Text(texts[2]), sqliteio.Text(texts[3]), sqliteio.Text(texts[4]), sqliteio.Blob(bindingRevision[:]),
		sqliteio.Text(texts[5]), sqliteio.Text(texts[6]), sqliteio.Text(texts[7]), sqliteio.Text(texts[8]), sqliteio.Text(texts[9]), sqliteio.Text(texts[10]),
		sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Text(texts[11]), sqliteio.Integer(clock.Wall.Seconds), sqliteio.Integer(clock.Wall.Nanoseconds),
		sqliteio.Text(texts[12]), sqliteio.Text(texts[13]), sqliteio.Text(texts[14]), sqliteio.Text(texts[15]), sqliteio.Blob(clock.ElapsedCounter), sqliteio.Blob(clock.AwakeCounter),
	}}
	nulls := 3
	blobs := [][]byte{revision[:], generation[:], sequence[:], bindingRevision[:], clock.ElapsedCounter, clock.AwakeCounter}
	if row.Parent != nil {
		parentGeneration, _ := sqliteEncodeUint64(parent.ref.Generation)
		encoded.parent = &parent.ref
		encoded.values[15], encoded.values[16] = sqliteio.Text(parent.key), sqliteio.Blob(parentGeneration[:])
		texts, blobs, nulls = append(texts, parent.key), append(blobs, parentGeneration[:]), nulls-2
	}
	if row.SegmentID != nil {
		id := sqlitePersistClockString(*row.SegmentID)
		encoded.values[17] = sqliteio.Text(id)
		texts, nulls = append(texts, id), nulls-1
	}
	encoded.charge, err = sqliteRowCharge(2, nulls, texts, blobs)
	if err != nil {
		return sqliteEncodedActorLocal{}, err
	}
	return encoded, nil
}

func sqliteActorUncertaintyIDsLocal(tx *sqliteio.Tx, computer string, key ActorKey) (result []string, err error) {
	_, found, err := sqliteReadActorLocal(tx, computer, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, failure("state_corrupt")
	}
	owner := actorKey(key)
	s, err := tx.Prepare("SELECT actor_key,ordinal,uncertainty_id FROM actor_uncertainties WHERE actor_key=? ORDER BY ordinal ASC", sqliteio.Text(owner))
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
		ordinal, id, readErr := sqliteReadLocalChild(s, owner, true)
		if readErr != nil {
			return nil, readErr
		}
		if ordinal != int64(len(result)) {
			return nil, failure("state_corrupt")
		}
		result = append(result, id)
	}
}

func sqliteNextActorUncertaintyOrdinalLocal(tx *sqliteio.Tx, computer string, key ActorKey) (result int64, err error) {
	_, found, err := sqliteReadActorLocal(tx, computer, key)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, failure("state_corrupt")
	}
	owner := actorKey(key)
	s, err := tx.Prepare("SELECT actor_key,ordinal,uncertainty_id FROM actor_uncertainties WHERE actor_key=? ORDER BY ordinal DESC LIMIT 1", sqliteio.Text(owner))
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
	ordinal, _, err := sqliteReadLocalChild(s, owner, true)
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

func sqliteAppendActorUncertaintyLocal(tx *sqliteio.Tx, computer string, key ActorKey, ordinal int64, id string) (delta int64, err error) {
	if ordinal < 0 || !validUUID(id) {
		return 0, failure("validation")
	}
	next, err := sqliteNextActorUncertaintyOrdinalLocal(tx, computer, key)
	if err != nil {
		return 0, err
	}
	if ordinal != next {
		return 0, failure("state_corrupt")
	}
	owner := actorKey(key)
	charge, err := sqliteRowCharge(1, 0, []string{owner, id}, nil)
	if err != nil {
		return 0, err
	}
	s, err := tx.Prepare("INSERT INTO actor_uncertainties(actor_key,ordinal,uncertainty_id) VALUES(?,?,?) RETURNING actor_key,ordinal", sqliteio.Text(owner), sqliteio.Integer(ordinal), sqliteio.Text(id))
	if err != nil {
		return 0, sqliteLocalWriteError(err)
	}
	defer func() {
		err = sqliteLocalWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteLocalReturnedChild(s, owner, ordinal); err != nil {
		return 0, err
	}
	return charge, nil
}

// The available clock is one fixed nine-column inline projection, shared by
// the two local scalar rows. It owns every returned string and BLOB value.
func sqliteReadLocalAvailableClock(s *sqliteio.Stmt, start int) (ClockSample, error) {
	value := sqliteClockValue{Available: true}
	for offset := 0; offset < 9; offset++ {
		column := start + offset
		expected := sqliteio.TextKind
		switch offset {
		case 1, 2:
			expected = sqliteio.IntegerKind
		case 7, 8:
			expected = sqliteio.BlobKind
		}
		kind, err := s.Kind(column)
		if err != nil {
			return ClockSample{}, errors.Join(failure("state_corrupt"), err)
		}
		if kind != expected {
			return ClockSample{}, failure("state_corrupt")
		}
		switch kind {
		case sqliteio.IntegerKind:
			n, err := s.Int64(column)
			if err != nil {
				return ClockSample{}, err
			}
			if offset == 1 {
				value.Wall.Seconds = n
			} else {
				value.Wall.Nanoseconds = n
			}
		case sqliteio.BlobKind:
			b, err := s.Blob(column)
			if err != nil {
				return ClockSample{}, err
			}
			if offset == 7 {
				value.ElapsedCounter = b
			} else {
				value.AwakeCounter = b
			}
		case sqliteio.TextKind:
			text, err := s.Text(column)
			if err != nil {
				return ClockSample{}, err
			}
			if !utf8.ValidString(text) {
				return ClockSample{}, failure("state_corrupt")
			}
			switch offset {
			case 0:
				value.Capability = text
			case 3:
				value.Wall.JSON = text
			case 4:
				value.Epoch = &text
			case 5:
				value.ElapsedRaw = &text
			case 6:
				value.AwakeRaw = &text
			}
		}
	}
	return sqliteDecodeClock(value)
}

// Both child tables have exactly (owner TEXT, ordinal INTEGER, reference TEXT).
// Only actor uncertainty references have the additional UUID requirement.
func sqliteReadLocalChild(s *sqliteio.Stmt, owner string, uncertainty bool) (int64, string, error) {
	if s.ColumnCount() != 3 {
		return 0, "", failure("state_corrupt")
	}
	for column := 0; column < 3; column++ {
		expected := sqliteio.TextKind
		if column == 1 {
			expected = sqliteio.IntegerKind
		}
		kind, err := s.Kind(column)
		if err != nil {
			return 0, "", errors.Join(failure("state_corrupt"), err)
		}
		if kind != expected {
			return 0, "", failure("state_corrupt")
		}
	}
	storedOwner, err := s.Text(0)
	if err != nil {
		return 0, "", err
	}
	ordinal, err := s.Int64(1)
	if err != nil {
		return 0, "", err
	}
	reference, err := s.Text(2)
	if err != nil {
		return 0, "", err
	}
	if storedOwner != owner || !utf8.ValidString(storedOwner) || ordinal < 0 || !utf8.ValidString(reference) || uncertainty && !validUUID(reference) {
		return 0, "", failure("state_corrupt")
	}
	return ordinal, reference, nil
}

func sqliteLocalReturnedChild(s *sqliteio.Stmt, owner string, ordinal int64) error {
	present, err := s.Step()
	if err != nil {
		return err
	}
	if !present || s.ColumnCount() != 2 {
		return failure("state_corrupt")
	}
	ownerKind, ownerErr := s.Kind(0)
	ordinalKind, ordinalErr := s.Kind(1)
	if ownerErr != nil || ordinalErr != nil {
		return errors.Join(failure("state_corrupt"), ownerErr, ordinalErr)
	}
	if ownerKind != sqliteio.TextKind || ordinalKind != sqliteio.IntegerKind {
		return failure("state_corrupt")
	}
	storedOwner, err := s.Text(0)
	if err != nil {
		return err
	}
	storedOrdinal, err := s.Int64(1)
	if err != nil {
		return err
	}
	if storedOwner != owner || storedOrdinal != ordinal {
		return failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return err
	} else if present {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteLocalReturnedID(s *sqliteio.Stmt, expected string) error {
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

func sqliteLocalWriteError(err error) error {
	var native *sqliteio.Error
	if errors.As(err, &native) && native.Category == sqliteio.Constraint && (native.Code == 1555 || native.Code == 2067) {
		return errors.Join(failure("validation"), err)
	}
	return err
}
