//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteHostReceiptRow struct {
	Key    string
	Record hostReceiptRecord
}

const sqliteHostReceiptColumns = "receipt_key,input_fingerprint,error_code,contract_version,snapshot_revision,receipt_id,source,native_session,turn_id,agent_id,kind,tool_id,disposition,ordering,diagnostic_code,durability,origin,profile_basis,profile_revision,policy_fingerprint,actor_key,actor_generation,observed_at_sec,observed_at_nsec,observed_at_json"

type sqliteEncodedHostReceipt struct {
	key    string
	values [25]sqliteio.Value
	charge int64
}

func sqliteReadHostReceipt(tx *sqliteio.Tx, computerID, maxRevision, key string) (sqliteHostReceiptRow, bool, error) {
	if !sqliteValidHostReceiptScope(computerID, maxRevision) || len(key) != 64 {
		return sqliteHostReceiptRow{}, false, failure("validation")
	}
	// The map lookup precedes final persistence repair, just as in the old store.
	s, err := tx.Prepare("SELECT "+sqliteHostReceiptColumns+" FROM host_receipts WHERE receipt_key=?", sqliteio.Text(key))
	if err != nil {
		return sqliteHostReceiptRow{}, false, err
	}
	row, found, err := sqliteReadHostReceiptStatement(tx, s, computerID, maxRevision)
	if err != nil || !found {
		return row, found, err
	}
	if row.Key != key {
		return sqliteHostReceiptRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

func sqliteLatestHostReceipt(tx *sqliteio.Tx, computerID, maxRevision string, ref ActorRef) (sqliteHostReceiptRow, bool, error) {
	if !sqliteValidHostReceiptScope(computerID, maxRevision) || !validRef(ref) || ref.Key.ComputerID != computerID {
		return sqliteHostReceiptRow{}, false, failure("validation")
	}
	generation, err := sqliteEncodeUint64(ref.Generation)
	if err != nil {
		return sqliteHostReceiptRow{}, false, err
	}
	key := actorKey(ref.Key)
	s, err := tx.Prepare("SELECT "+sqliteHostReceiptColumns+" FROM host_receipts WHERE actor_key=? AND actor_generation=? ORDER BY snapshot_revision DESC,receipt_id DESC LIMIT 1",
		sqliteio.Text(key), sqliteio.Blob(generation[:]))
	if err != nil {
		return sqliteHostReceiptRow{}, false, err
	}
	row, found, err := sqliteReadHostReceiptStatement(tx, s, computerID, maxRevision)
	if err != nil || !found {
		return row, found, err
	}
	actor := row.Record.Result.Actor
	if actor == nil || actorKey(actor.Key) != key || actor.Generation != ref.Generation {
		return sqliteHostReceiptRow{}, false, failure("state_corrupt")
	}
	// Receipt/dependency validation and checked close precede this comparison.
	// JSON can give a raw malformed Ref the stored Ref's key, but the legacy
	// provenance selector matches the original Ref exactly, not its repaired copy.
	if *actor != ref {
		return sqliteHostReceiptRow{}, false, nil
	}
	return row, true, nil
}

func sqliteReadHostReceiptStatement(tx *sqliteio.Tx, s *sqliteio.Stmt, computerID, maxRevision string) (result sqliteHostReceiptRow, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteHostReceiptRow{}, false
		}
	}()
	row, err := s.Step()
	if err != nil || !row {
		return result, false, err
	}
	if s.ColumnCount() != 25 {
		return result, false, failure("state_corrupt")
	}
	r := &result.Record.Result
	var wall sqliteTimeValue
	var actorKeyValue, actorGeneration string
	var actorPresent bool
	for column := 0; column < 25; column++ {
		kind, kindErr := s.Kind(column)
		if kindErr != nil {
			return result, false, errors.Join(failure("state_corrupt"), kindErr)
		}
		expected := sqliteio.TextKind
		switch column {
		case 3, 22, 23:
			expected = sqliteio.IntegerKind
		case 4, 18:
			expected = sqliteio.BlobKind
		case 20:
			actorPresent = kind != sqliteio.NullKind
			if !actorPresent {
				expected = sqliteio.NullKind
			}
		case 21:
			expected = sqliteio.NullKind
			if actorPresent {
				expected = sqliteio.BlobKind
			}
		}
		if kind != expected {
			return result, false, failure("state_corrupt")
		}
		switch kind {
		case sqliteio.NullKind:
			continue
		case sqliteio.IntegerKind:
			value, readErr := s.Int64(column)
			if readErr != nil {
				return result, false, readErr
			}
			switch column {
			case 3:
				if value != 1 {
					return result, false, failure("state_corrupt")
				}
				r.ContractVersion = 1
			case 22:
				wall.Seconds = value
			case 23:
				wall.Nanoseconds = value
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
			switch column {
			case 4:
				r.SnapshotRevision = decoded
			case 18:
				r.ProfileRevision = decoded
			case 21:
				actorGeneration = decoded
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
				result.Key = value
			case 1:
				result.Record.Fingerprint = value
			case 2:
				result.Record.ErrorCode = value
			case 5:
				r.ID = value
			case 6:
				r.Source = value
			case 7:
				r.SessionID = value
			case 8:
				r.TurnID = value
			case 9:
				r.AgentID = value
			case 10:
				r.Kind = value
			case 11:
				r.ToolID = value
			case 12:
				r.Disposition = value
			case 13:
				r.Ordering = value
			case 14:
				r.DiagnosticCode = value
			case 15:
				r.Durability = value
			case 16:
				r.Origin = value
			case 17:
				r.ProfileBasis = value
			case 19:
				r.Fingerprint = value
			case 20:
				actorKeyValue = value
			case 24:
				wall.JSON = value
			}
		}
	}
	r.ObservedAt, err = sqliteDecodeTime(wall)
	if err != nil {
		return result, false, err
	}
	if actorPresent {
		ref, readErr := sqliteReadHostReceiptGeneration(tx, actorKeyValue, actorGeneration)
		if readErr != nil {
			return result, false, readErr
		}
		r.Actor = &ref
	}
	if !sqliteValidHostReceipt(result) || !sqliteHostReceiptInScope(result, computerID, maxRevision) {
		return result, false, failure("state_corrupt")
	}
	if row, err = s.Step(); err != nil {
		return result, false, err
	} else if row {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

// A receipt stores an opaque key/generation reference, not a complete ActorRef.
// Resolve only that historical identity; never parse the key or invent a head.
func sqliteReadHostReceiptGeneration(tx *sqliteio.Tx, key, generation string) (result ActorRef, err error) {
	n, ok := counter(generation)
	if !ok || n == 0 {
		return result, failure("state_corrupt")
	}
	encoded, err := sqliteEncodeUint64(generation)
	if err != nil {
		return result, err
	}
	s, err := tx.Prepare("SELECT actor_key,generation,computer_id,source,session_id,agent_id FROM actor_generations WHERE actor_key=? AND generation=?",
		sqliteio.Text(key), sqliteio.Blob(encoded[:]))
	if err != nil {
		return result, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = ActorRef{}
		}
	}()
	row, err := s.Step()
	if err != nil {
		return result, err
	}
	if !row || s.ColumnCount() != 6 {
		return result, failure("state_corrupt")
	}
	var storedKey string
	for column := 0; column < 6; column++ {
		expected := sqliteio.TextKind
		if column == 1 {
			expected = sqliteio.BlobKind
		}
		kind, kindErr := s.Kind(column)
		if kindErr != nil {
			return result, errors.Join(failure("state_corrupt"), kindErr)
		}
		if kind != expected {
			return result, failure("state_corrupt")
		}
		if column == 1 {
			value, readErr := s.Blob(column)
			if readErr != nil {
				return result, readErr
			}
			result.Generation, err = sqliteDecodeUint64(value)
			if err != nil {
				return result, err
			}
			continue
		}
		value, readErr := s.Text(column)
		if readErr != nil {
			return result, readErr
		}
		if !utf8.ValidString(value) {
			return result, failure("state_corrupt")
		}
		switch column {
		case 0:
			storedKey = value
		case 2:
			result.Key.ComputerID = value
		case 3:
			result.Key.Source = value
		case 4:
			result.Key.SessionID = value
		case 5:
			result.Key.AgentID = value
		}
	}
	if !validRef(result) || storedKey != key || actorKey(result.Key) != key || result.Generation != generation {
		return result, failure("state_corrupt")
	}
	if row, err = s.Step(); err != nil {
		return result, err
	} else if row {
		return result, failure("state_corrupt")
	}
	return result, nil
}

func sqliteInsertHostReceipt(tx *sqliteio.Tx, computerID, maxRevision string, row sqliteHostReceiptRow) (delta int64, err error) {
	encoded, err := sqliteEncodeScopedHostReceipt(computerID, maxRevision, row)
	if err != nil {
		return 0, err
	}
	if err := sqliteCheckHostReceiptGeneration(tx, row); err != nil {
		return 0, err
	}
	s, err := tx.Prepare("INSERT INTO host_receipts("+sqliteHostReceiptColumns+") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING receipt_key", encoded.values[:]...)
	if err != nil {
		return 0, sqliteHostReceiptWriteError(err)
	}
	defer func() {
		err = sqliteHostReceiptWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteHostReceiptReturnedKey(s, encoded.key); err != nil {
		return 0, err
	}
	return encoded.charge, nil
}

func sqliteUpdateHostReceipt(tx *sqliteio.Tx, computerID, maxRevision string, before, after sqliteHostReceiptRow) (delta int64, err error) {
	if before.Key != after.Key {
		return 0, failure("validation")
	}
	old, err := sqliteEncodeScopedHostReceipt(computerID, maxRevision, before)
	if err != nil {
		return 0, err
	}
	next, err := sqliteEncodeScopedHostReceipt(computerID, maxRevision, after)
	if err != nil {
		return 0, err
	}
	if err := sqliteCheckHostReceiptGeneration(tx, before); err != nil {
		return 0, err
	}
	if err := sqliteCheckHostReceiptGeneration(tx, after); err != nil {
		return 0, err
	}
	values := make([]sqliteio.Value, 0, 49)
	values = append(values, next.values[1:]...)
	values = append(values, old.values[:]...)
	s, err := tx.Prepare(`UPDATE host_receipts SET input_fingerprint=?,error_code=?,contract_version=?,snapshot_revision=?,
		receipt_id=?,source=?,native_session=?,turn_id=?,agent_id=?,kind=?,tool_id=?,disposition=?,ordering=?,
		diagnostic_code=?,durability=?,origin=?,profile_basis=?,profile_revision=?,policy_fingerprint=?,
		actor_key=?,actor_generation=?,observed_at_sec=?,observed_at_nsec=?,observed_at_json=?
		WHERE receipt_key=? AND input_fingerprint=? AND error_code=? AND contract_version=? AND snapshot_revision=?
		AND receipt_id=? AND source=? AND native_session=? AND turn_id=? AND agent_id=? AND kind=? AND tool_id=?
		AND disposition=? AND ordering=? AND diagnostic_code=? AND durability=? AND origin=? AND profile_basis=?
		AND profile_revision=? AND policy_fingerprint=? AND actor_key IS ? AND actor_generation IS ?
		AND observed_at_sec=? AND observed_at_nsec=? AND observed_at_json=? RETURNING receipt_key`, values...)
	if err != nil {
		return 0, sqliteHostReceiptWriteError(err)
	}
	defer func() {
		err = sqliteHostReceiptWriteError(sqliteCloseMetaStatement(s, err))
		if err != nil {
			delta = 0
		}
	}()
	if err = sqliteHostReceiptReturnedKey(s, next.key); err != nil {
		return 0, err
	}
	// Both complete charges are nonnegative checked int64 values.
	return next.charge - old.charge, nil
}

func sqliteHostReceiptReturnedKey(s *sqliteio.Stmt, key string) error {
	row, err := s.Step()
	if err != nil {
		return err
	}
	if !row || s.ColumnCount() != 1 {
		return failure("state_corrupt")
	}
	kind, err := s.Kind(0)
	if err != nil {
		return errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.TextKind {
		return failure("state_corrupt")
	}
	value, err := s.Text(0)
	if err != nil {
		return err
	}
	if value != key {
		return failure("state_corrupt")
	}
	if row, err = s.Step(); err != nil {
		return err
	} else if row {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteHostReceiptCharge(row sqliteHostReceiptRow) (int64, error) {
	encoded, err := sqliteEncodeHostReceipt(row)
	return encoded.charge, err
}

func sqliteValidHostReceiptScope(computerID, maxRevision string) bool {
	n, ok := counter(maxRevision)
	return validUUID(computerID) && ok && n > 0
}

func sqliteHostReceiptInScope(row sqliteHostReceiptRow, computerID, maxRevision string) bool {
	r := row.Record.Result
	revision, ok := counter(r.SnapshotRevision)
	ceiling, ceilingOK := counter(maxRevision)
	return ok && ceilingOK && revision <= ceiling && (r.Actor == nil || r.Actor.Key.ComputerID == computerID)
}

func sqliteValidHostReceipt(row sqliteHostReceiptRow) bool {
	r := row.Record.Result
	revision, ok := counter(r.SnapshotRevision)
	if len(row.Key) != 64 || len(row.Record.Fingerprint) != 64 || r.ContractVersion != 1 || !ok || revision == 0 ||
		!validUUID(r.ID) || !hostSource(r.Source) || !safeIdentifier(r.SessionID, 256) || r.ObservedAt.IsZero() ||
		r.Durability != "committed" || r.Origin != "unverified" || r.Actor != nil && !validRef(*r.Actor) {
		return false
	}
	switch r.Disposition {
	case "applied", "stale", "review_required":
	default:
		return false
	}
	switch r.Ordering {
	case "supported", "review_required", "unavailable":
	default:
		return false
	}
	if r.ProfileBasis == "operator_declared" {
		profile, ok := counter(r.ProfileRevision)
		if !ok || profile == 0 || len(r.Fingerprint) != 64 {
			return false
		}
	} else if r.ProfileBasis != "none" || r.ProfileRevision != "0" || r.Fingerprint != "" || r.Disposition != "review_required" {
		return false
	}
	switch row.Record.ErrorCode {
	case "", "clock_unavailable", "clock_conflict", "event_gap", "event_conflict", "invalid_transition":
		return true
	default:
		return false
	}
}

func sqliteEncodeScopedHostReceipt(computerID, maxRevision string, row sqliteHostReceiptRow) (sqliteEncodedHostReceipt, error) {
	if !sqliteValidHostReceiptScope(computerID, maxRevision) || !sqliteHostReceiptInScope(row, computerID, maxRevision) {
		return sqliteEncodedHostReceipt{}, failure("validation")
	}
	return sqliteEncodeHostReceipt(row)
}

func sqliteEncodeHostReceipt(row sqliteHostReceiptRow) (sqliteEncodedHostReceipt, error) {
	if !sqliteValidHostReceipt(row) {
		return sqliteEncodedHostReceipt{}, failure("validation")
	}
	r := row.Record.Result
	snapshot, err := sqliteEncodeUint64(r.SnapshotRevision)
	if err != nil {
		return sqliteEncodedHostReceipt{}, err
	}
	profile, err := sqliteEncodeUint64(r.ProfileRevision)
	if err != nil {
		return sqliteEncodedHostReceipt{}, err
	}
	wall, err := sqliteEncodeTime(r.ObservedAt)
	if err != nil {
		return sqliteEncodedHostReceipt{}, failure("validation")
	}
	// Final text encoding follows raw validation and never mutates the caller.
	texts := []string{row.Key, row.Record.Fingerprint, row.Record.ErrorCode, r.ID, r.Source,
		r.SessionID, r.TurnID, r.AgentID, r.Kind, r.ToolID, r.Disposition, r.Ordering,
		r.DiagnosticCode, r.Durability, r.Origin, r.ProfileBasis, r.Fingerprint, wall.JSON}
	for i := range texts {
		texts[i] = sqlitePersistClockString(texts[i])
	}
	encoded := sqliteEncodedHostReceipt{key: texts[0], values: [25]sqliteio.Value{
		sqliteio.Text(texts[0]), sqliteio.Text(texts[1]), sqliteio.Text(texts[2]), sqliteio.Integer(1), sqliteio.Blob(snapshot[:]),
		sqliteio.Text(texts[3]), sqliteio.Text(texts[4]), sqliteio.Text(texts[5]), sqliteio.Text(texts[6]), sqliteio.Text(texts[7]),
		sqliteio.Text(texts[8]), sqliteio.Text(texts[9]), sqliteio.Text(texts[10]), sqliteio.Text(texts[11]), sqliteio.Text(texts[12]),
		sqliteio.Text(texts[13]), sqliteio.Text(texts[14]), sqliteio.Text(texts[15]), sqliteio.Blob(profile[:]), sqliteio.Text(texts[16]),
		sqliteio.Null(), sqliteio.Null(), sqliteio.Integer(wall.Seconds), sqliteio.Integer(wall.Nanoseconds), sqliteio.Text(texts[17]),
	}}
	blobs := [][]byte{snapshot[:], profile[:]}
	nulls := 2
	if r.Actor != nil {
		actor, actorErr := sqliteEncodeGeneration(*r.Actor)
		if actorErr != nil {
			return sqliteEncodedHostReceipt{}, actorErr
		}
		generation, genErr := sqliteEncodeUint64(actor.ref.Generation)
		if genErr != nil {
			return sqliteEncodedHostReceipt{}, genErr
		}
		encoded.values[20], encoded.values[21] = sqliteio.Text(actor.key), sqliteio.Blob(generation[:])
		texts = append(texts, actor.key)
		blobs = append(blobs, generation[:])
		nulls = 0
	}
	encoded.charge, err = sqliteRowCharge(3, nulls, texts, blobs)
	if err != nil {
		return sqliteEncodedHostReceipt{}, err
	}
	return encoded, nil
}

func sqliteCheckHostReceiptGeneration(tx *sqliteio.Tx, row sqliteHostReceiptRow) error {
	if row.Record.Result.Actor == nil {
		return nil
	}
	_, found, err := sqliteReadActorGeneration(tx, *row.Record.Result.Actor)
	if err != nil {
		return err
	}
	if !found {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteHostReceiptWriteError(err error) error {
	var native *sqliteio.Error
	if errors.As(err, &native) && native.Category == sqliteio.Constraint &&
		(native.Code == 1555 || native.Code == 2067 || native.Code == 275) {
		// These fixed writes can refuse an existing map key or raw-valid text
		// expanding past a schema byte-length CHECK. Preserve checked evidence.
		return errors.Join(failure("validation"), err)
	}
	return err
}
