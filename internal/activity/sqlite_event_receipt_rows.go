//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"encoding/hex"
	"errors"
	"math"
	"slices"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteEventReceiptRow struct {
	Key   string
	Value eventReceipt
}

const sqliteEventReceiptColumns = "event_key,fingerprint,contract_version,snapshot_revision,disposition,actor_key,actor_generation,segment_id"
const sqliteEventReceiptChildColumns = "event_key,ordinal,uncertainty_id"

type sqliteEncodedEventReceipt struct {
	row    sqliteEventReceiptRow
	id     string
	values [8]sqliteio.Value
	charge int64
}

func sqliteReadEventReceipt(tx *sqliteio.Tx, key, revisionCeiling string) (sqliteEventReceiptRow, bool, error) {
	row, found, err := sqliteReadEventReceiptData(tx, key, revisionCeiling)
	if err != nil || !found {
		return row, found, err
	}
	if _, err = sqliteProveInverseEventID(tx, key); err != nil {
		return sqliteEventReceiptRow{}, false, err
	}
	return row, true, nil
}

func sqliteValidateEventReceiptID(tx *sqliteio.Tx, key, revisionCeiling string) error {
	_, found, err := sqliteReadEventReceiptData(tx, key, revisionCeiling)
	if err != nil {
		return err
	}
	if !found {
		return failure("state_corrupt")
	}
	_, err = sqliteProveInverseEventID(tx, key)
	return err
}

// Only this decoder assembles the retained scalar and ordered result array.
// Inverse-ID proof is separate so neither public entry point recurses.
func sqliteReadEventReceiptData(tx *sqliteio.Tx, key, revisionCeiling string) (result sqliteEventReceiptRow, found bool, err error) {
	ceiling, ok := counter(revisionCeiling)
	if !ok || ceiling == 0 {
		return result, false, failure("validation")
	}
	// Raw direct lookup precedes any final persistence repair.
	s, err := tx.Prepare("SELECT "+sqliteEventReceiptColumns+" FROM event_receipts WHERE event_key=?", sqliteio.Text(key))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteEventReceiptRow{}, false
		}
	}()
	selected, err := s.Step()
	if err != nil || !selected {
		return result, false, err
	}
	if s.ColumnCount() != 8 {
		return result, false, failure("state_corrupt")
	}
	r := &result.Value.Result
	var storedActor string
	for col := 0; col < 8; col++ {
		kind, kindErr := s.Kind(col)
		if kindErr != nil {
			return result, false, errors.Join(failure("state_corrupt"), kindErr)
		}
		expected := sqliteio.TextKind
		if col == 2 {
			expected = sqliteio.IntegerKind
		}
		if col == 3 || col == 6 {
			expected = sqliteio.BlobKind
		}
		if col == 7 && kind == sqliteio.NullKind {
			continue
		}
		if kind != expected {
			return result, false, failure("state_corrupt")
		}
		switch kind {
		case sqliteio.IntegerKind:
			value, readErr := s.Int64(col)
			if readErr != nil {
				return result, false, readErr
			}
			if value != 1 {
				return result, false, failure("state_corrupt")
			}
			r.ContractVersion = 1
		case sqliteio.BlobKind:
			value, readErr := s.Blob(col)
			if readErr != nil {
				return result, false, readErr
			}
			decoded, decodeErr := sqliteDecodeUint64(value)
			if decodeErr != nil {
				return result, false, decodeErr
			}
			if col == 3 {
				r.SnapshotRevision = decoded
			} else {
				r.Actor.Generation = decoded
			}
		case sqliteio.TextKind:
			value, readErr := sqliteEventReceiptText(s, col)
			if readErr != nil {
				return result, false, readErr
			}
			switch col {
			case 0:
				result.Key = value
			case 1:
				result.Value.Fingerprint = value
			case 4:
				r.Disposition = value
			case 5:
				storedActor = value
			case 7:
				r.SegmentID = sqliteCopyString(&value)
			}
		}
	}
	if result.Key != key {
		return result, false, failure("state_corrupt")
	}
	r.Actor, err = sqliteReadHostReceiptGeneration(tx, storedActor, r.Actor.Generation)
	if err != nil {
		return result, false, err
	}
	if r.SegmentID != nil {
		if err = sqliteEventReceiptSegmentPresent(tx, *r.SegmentID); err != nil {
			return result, false, err
		}
	}
	r.UncertaintyIDs, err = sqliteReadEventReceiptChildren(tx, key)
	if err != nil {
		return result, false, err
	}
	revision, _ := counter(r.SnapshotRevision)
	if !sqliteValidEventReceipt(result) || revision > ceiling {
		return result, false, failure("state_corrupt")
	}
	if selected, err = s.Step(); err != nil {
		return result, false, err
	} else if selected {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteReadEventReceiptChildren(tx *sqliteio.Tx, key string) (result []string, err error) {
	s, err := tx.Prepare("SELECT "+sqliteEventReceiptChildColumns+" FROM event_receipt_uncertainties WHERE event_key=? ORDER BY ordinal", sqliteio.Text(key))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = []string{}
	for {
		selected, stepErr := s.Step()
		if stepErr != nil {
			return nil, stepErr
		}
		if !selected {
			return result, nil
		}
		if s.ColumnCount() != 3 {
			return nil, failure("state_corrupt")
		}
		owner, readErr := sqliteEventReceiptText(s, 0)
		if readErr != nil {
			return nil, readErr
		}
		kind, kindErr := s.Kind(1)
		if kindErr != nil {
			return nil, errors.Join(failure("state_corrupt"), kindErr)
		}
		if kind != sqliteio.IntegerKind {
			return nil, failure("state_corrupt")
		}
		ordinal, readErr := s.Int64(1)
		if readErr != nil {
			return nil, readErr
		}
		value, readErr := sqliteEventReceiptText(s, 2)
		if readErr != nil {
			return nil, readErr
		}
		if owner != key || ordinal < 0 || ordinal != int64(len(result)) {
			return nil, failure("state_corrupt")
		}
		result = append(result, value)
	}
}

func sqliteReadEventID(tx *sqliteio.Tx, eventID string) (key string, found bool, err error) {
	s, err := tx.Prepare("SELECT event_id,event_key FROM event_ids WHERE event_id=?", sqliteio.Text(eventID))
	if err != nil {
		return "", false, err
	}
	id, key, found, err := sqliteReadEventIDStatement(s)
	if err != nil || !found {
		return key, found, err
	}
	if id != eventID {
		return "", false, failure("state_corrupt")
	}
	// This lookup needs presence only, not another receipt's result array.
	s, err = tx.Prepare("SELECT event_key FROM event_receipts WHERE event_key=?", sqliteio.Text(key))
	if err != nil {
		return "", false, err
	}
	if err = sqliteEventReceiptCheckedKey(s, key); err != nil {
		return "", false, err
	}
	return key, true, nil
}

func sqliteProveInverseEventID(tx *sqliteio.Tx, key string) (string, error) {
	s, err := tx.Prepare("SELECT event_id,event_key FROM event_ids WHERE event_key=?", sqliteio.Text(key))
	if err != nil {
		return "", err
	}
	id, storedKey, found, err := sqliteReadEventIDStatement(s)
	if err != nil {
		return "", err
	}
	if !found || storedKey != key {
		return "", failure("state_corrupt")
	}
	return id, nil
}

func sqliteReadEventIDStatement(s *sqliteio.Stmt) (id, key string, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			id, key, found = "", "", false
		}
	}()
	selected, err := s.Step()
	if err != nil || !selected {
		return "", "", false, err
	}
	if s.ColumnCount() != 2 {
		return "", "", false, failure("state_corrupt")
	}
	id, err = sqliteEventReceiptText(s, 0)
	if err != nil {
		return "", "", false, err
	}
	key, err = sqliteEventReceiptText(s, 1)
	if err != nil {
		return "", "", false, err
	}
	if !safeIdentifier(id, 256) {
		return "", "", false, failure("state_corrupt")
	}
	if selected, err = s.Step(); err != nil {
		return "", "", false, err
	} else if selected {
		return "", "", false, failure("state_corrupt")
	}
	return id, key, true, nil
}

func sqliteEventReceiptSegmentPresent(tx *sqliteio.Tx, id string) error {
	s, err := tx.Prepare("SELECT segment_id FROM segments WHERE segment_id=?", sqliteio.Text(id))
	if err != nil {
		return err
	}
	return sqliteEventReceiptCheckedKey(s, id)
}

// Own a one-key statement through ROW, DONE and checked finalization. Both
// presence proofs and scalar INSERT RETURNING must yield exactly this key.
func sqliteEventReceiptCheckedKey(s *sqliteio.Stmt, key string) (err error) {
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	selected, err := s.Step()
	if err != nil {
		return err
	}
	if !selected || s.ColumnCount() != 1 {
		return failure("state_corrupt")
	}
	value, err := sqliteEventReceiptText(s, 0)
	if err != nil {
		return err
	}
	if value != key {
		return failure("state_corrupt")
	}
	if selected, err = s.Step(); err != nil {
		return err
	} else if selected {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteAssertEventReceipt(tx *sqliteio.Tx, revisionCeiling string, before sqliteEventReceiptRow, eventID string) error {
	encoded, err := sqliteEncodeEventReceipt(before, eventID)
	if err != nil {
		return err
	}
	stored, found, err := sqliteReadEventReceiptData(tx, before.Key, revisionCeiling)
	if err != nil {
		return err
	}
	if !found {
		return failure("state_corrupt")
	}
	want, got := encoded.row.Value.Result, stored.Value.Result
	if stored.Key != encoded.row.Key || stored.Value.Fingerprint != encoded.row.Value.Fingerprint ||
		got.ContractVersion != want.ContractVersion || got.SnapshotRevision != want.SnapshotRevision ||
		got.Disposition != want.Disposition || got.Actor != want.Actor ||
		(got.SegmentID == nil) != (want.SegmentID == nil) ||
		got.SegmentID != nil && *got.SegmentID != *want.SegmentID || !slices.Equal(got.UncertaintyIDs, want.UncertaintyIDs) {
		return failure("state_corrupt")
	}
	id, err := sqliteProveInverseEventID(tx, before.Key)
	if err != nil {
		return err
	}
	if id != encoded.id {
		return failure("state_corrupt")
	}
	return nil
}

// No metadata or dependency rows are inserted here. The caller must validate
// the complete staged unit with its final revision ceiling before committing.
func sqliteInsertEventReceipt(tx *sqliteio.Tx, row sqliteEventReceiptRow, eventID string) (delta int64, err error) {
	encoded, err := sqliteEncodeEventReceipt(row, eventID)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = sqliteEventReceiptWriteError(err)
		if err != nil {
			delta = 0
		}
	}()
	s, err := tx.Prepare("INSERT INTO event_receipts("+sqliteEventReceiptColumns+") VALUES(?,?,?,?,?,?,?,?) RETURNING event_key", encoded.values[:]...)
	if err != nil {
		return 0, err
	}
	if err = sqliteEventReceiptCheckedKey(s, encoded.row.Key); err != nil {
		return 0, err
	}
	for ordinal, id := range encoded.row.Value.Result.UncertaintyIDs {
		if err = sqliteInsertEventReceiptChild(tx, encoded.row.Key, int64(ordinal), id); err != nil {
			return 0, err
		}
	}
	s, err = tx.Prepare("INSERT INTO event_ids(event_id,event_key) VALUES(?,?) RETURNING event_id,event_key", sqliteio.Text(encoded.id), sqliteio.Text(encoded.row.Key))
	if err != nil {
		return 0, err
	}
	id, key, found, err := sqliteReadEventIDStatement(s)
	if err != nil {
		return 0, err
	}
	if !found || id != encoded.id || key != encoded.row.Key {
		return 0, failure("state_corrupt")
	}
	return encoded.charge, nil
}

func sqliteInsertEventReceiptChild(tx *sqliteio.Tx, key string, ordinal int64, id string) (err error) {
	s, err := tx.Prepare("INSERT INTO event_receipt_uncertainties("+sqliteEventReceiptChildColumns+") VALUES(?,?,?) RETURNING event_key,ordinal", sqliteio.Text(key), sqliteio.Integer(ordinal), sqliteio.Text(id))
	if err != nil {
		return err
	}
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	selected, err := s.Step()
	if err != nil {
		return err
	}
	if !selected || s.ColumnCount() != 2 {
		return failure("state_corrupt")
	}
	owner, err := sqliteEventReceiptText(s, 0)
	if err != nil {
		return err
	}
	kind, err := s.Kind(1)
	if err != nil {
		return errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.IntegerKind {
		return failure("state_corrupt")
	}
	stored, err := s.Int64(1)
	if err != nil {
		return err
	}
	if owner != key || stored != ordinal {
		return failure("state_corrupt")
	}
	if selected, err = s.Step(); err != nil {
		return err
	} else if selected {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteEventReceiptCharge(row sqliteEventReceiptRow, eventID string) (int64, error) {
	encoded, err := sqliteEncodeEventReceipt(row, eventID)
	return encoded.charge, err
}

func sqliteValidEventReceipt(row sqliteEventReceiptRow) bool {
	r := row.Value.Result
	if len(row.Value.Fingerprint) != 64 || r.ContractVersion != 1 || r.Disposition != "applied" ||
		!validRef(r.Actor) || r.UncertaintyIDs == nil || r.SegmentID != nil && !validUUID(*r.SegmentID) {
		return false
	}
	if _, err := hex.DecodeString(row.Value.Fingerprint); err != nil {
		return false
	}
	_, ok := counter(r.SnapshotRevision)
	return ok
}

func sqliteEncodeEventReceipt(row sqliteEventReceiptRow, eventID string) (sqliteEncodedEventReceipt, error) {
	if !sqliteValidEventReceipt(row) || !safeIdentifier(eventID, 256) {
		return sqliteEncodedEventReceipt{}, failure("validation")
	}
	id := sqlitePersistClockString(eventID)
	if !safeIdentifier(id, 256) {
		return sqliteEncodedEventReceipt{}, failure("validation")
	}
	actor, err := sqliteEncodeGeneration(row.Value.Result.Actor)
	if err != nil {
		return sqliteEncodedEventReceipt{}, err
	}
	// One owned materialized key supplies all three tables. Raw lookup and
	// fingerprint decisions are never repeated using its repaired spelling.
	row.Key = sqlitePersistClockString(row.Key)
	r := &row.Value.Result
	r.Actor = actor.ref
	r.SegmentID = sqliteCopyString(r.SegmentID)
	children := make([]string, len(r.UncertaintyIDs))
	for i, value := range r.UncertaintyIDs {
		children[i] = sqlitePersistClockString(value)
	}
	r.UncertaintyIDs = children
	snapshot, err := sqliteEncodeUint64(r.SnapshotRevision)
	if err != nil {
		return sqliteEncodedEventReceipt{}, err
	}
	generation, err := sqliteEncodeUint64(r.Actor.Generation)
	if err != nil {
		return sqliteEncodedEventReceipt{}, err
	}
	encoded := sqliteEncodedEventReceipt{row: row, id: id, values: [8]sqliteio.Value{
		sqliteio.Text(row.Key), sqliteio.Text(row.Value.Fingerprint), sqliteio.Integer(1), sqliteio.Blob(snapshot[:]),
		sqliteio.Text(r.Disposition), sqliteio.Text(actor.key), sqliteio.Blob(generation[:]), sqliteio.Null(),
	}}
	texts := []string{row.Key, row.Value.Fingerprint, r.Disposition, actor.key}
	nulls := 1
	if r.SegmentID != nil {
		encoded.values[7] = sqliteio.Text(*r.SegmentID)
		texts = append(texts, *r.SegmentID)
		nulls = 0
	}
	encoded.charge, err = sqliteRowCharge(1, nulls, texts, [][]byte{snapshot[:], generation[:]})
	if err != nil {
		return sqliteEncodedEventReceipt{}, err
	}
	for _, child := range children {
		charge, chargeErr := sqliteRowCharge(1, 0, []string{row.Key, child}, nil)
		if chargeErr != nil {
			return sqliteEncodedEventReceipt{}, chargeErr
		}
		if charge > math.MaxInt64-encoded.charge {
			return sqliteEncodedEventReceipt{}, failure("state_corrupt")
		}
		encoded.charge += charge
	}
	charge, err := sqliteRowCharge(0, 0, []string{id, row.Key}, nil)
	if err != nil {
		return sqliteEncodedEventReceipt{}, err
	}
	if charge > math.MaxInt64-encoded.charge {
		return sqliteEncodedEventReceipt{}, failure("state_corrupt")
	}
	encoded.charge += charge
	return encoded, nil
}

func sqliteEventReceiptText(s *sqliteio.Stmt, column int) (string, error) {
	kind, err := s.Kind(column)
	if err != nil {
		return "", errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.TextKind {
		return "", failure("state_corrupt")
	}
	value, err := s.Text(column)
	if err != nil {
		return "", err
	}
	if !utf8.ValidString(value) {
		return "", failure("state_corrupt")
	}
	return value, nil
}

func sqliteEventReceiptWriteError(err error) error {
	var native *sqliteio.Error
	if errors.As(err, &native) && native.Category == sqliteio.Constraint && (native.Code == 1555 || native.Code == 2067) {
		return errors.Join(failure("validation"), err)
	}
	return err
}
