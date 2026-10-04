//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteResolveRequestRow struct {
	ID, Fingerprint string
	Result          MutationResult
}

const sqliteResolveRequestColumns = "request_id,operation,fingerprint,outcome_kind,payload"

type sqliteEncodedResolveRequest struct {
	id     string
	values [5]sqliteio.Value
	charge int64
}

// This is a selected activity.resolve proof, not general request replay. The
// caller's final dependency validator owns affected-ID membership and existence.
func sqliteReadResolveRequestProof(tx *sqliteio.Tx, id, revisionCeiling string) (result sqliteResolveRequestRow, found bool, err error) {
	ceiling, ok := counter(revisionCeiling)
	if !validUUID(id) || !ok || ceiling == 0 {
		return result, false, failure("validation")
	}
	s, err := tx.Prepare("SELECT "+sqliteResolveRequestColumns+" FROM requests WHERE request_id=?", sqliteio.Text(id))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteResolveRequestRow{}, false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return result, false, err
	}
	if s.ColumnCount() != 5 {
		return result, false, failure("state_corrupt")
	}
	var fields [5]string
	for column := 0; column < 5; column++ {
		kind, kindErr := s.Kind(column)
		if kindErr != nil {
			return result, false, errors.Join(failure("state_corrupt"), kindErr)
		}
		if kind != sqliteio.TextKind {
			return result, false, failure("state_corrupt")
		}
		value, readErr := s.Text(column)
		if readErr != nil {
			return result, false, readErr
		}
		if !utf8.ValidString(value) {
			return result, false, failure("state_corrupt")
		}
		fields[column] = value
	}
	if fields[0] != id || fields[1] != "activity.resolve" || fields[3] != "mutation_result" {
		return result, false, failure("state_corrupt")
	}
	result.ID, result.Fingerprint = fields[0], fields[2]
	payload := []byte(fields[4])
	// Match the strict legacy typed boundary, including duplicate names,
	// case-sensitive allowlisted fields, UTF-8 and a single complete JSON value.
	if !strictJSON(payload) || !exactJSONFields(payload, reflect.TypeOf(MutationResult{})) {
		return result, false, failure("state_corrupt")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&result.Result) != nil || !sqliteValidResolveRequest(result) {
		return result, false, failure("state_corrupt")
	}
	revision, _ := counter(result.Result.SnapshotRevision)
	if revision > ceiling {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteInsertResolveRequest(tx *sqliteio.Tx, row sqliteResolveRequestRow) (delta int64, err error) {
	encoded, err := sqliteEncodeResolveRequest(row)
	if err != nil {
		return 0, err
	}
	s, err := tx.Prepare("INSERT INTO requests("+sqliteResolveRequestColumns+") VALUES(?,?,?,?,?) RETURNING request_id", encoded.values[:]...)
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

func sqliteResolveRequestCharge(row sqliteResolveRequestRow) (int64, error) {
	encoded, err := sqliteEncodeResolveRequest(row)
	return encoded.charge, err
}

func sqliteValidResolveRequest(row sqliteResolveRequestRow) bool {
	r := row.Result
	if !validUUID(row.ID) || len(row.Fingerprint) != 64 || r.ContractVersion != 1 || r.RequestID != row.ID ||
		!r.Changed || r.EntityRevision == nil || len(r.AffectedIDs) == 0 {
		return false
	}
	for _, c := range row.Fingerprint {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	if n, ok := counter(r.SnapshotRevision); !ok || n == 0 {
		return false
	}
	if n, ok := counter(*r.EntityRevision); !ok || n == 0 {
		return false
	}
	seen := make(map[string]bool, len(r.AffectedIDs))
	for _, id := range r.AffectedIDs {
		if !validUUID(id) || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}

func sqliteEncodeResolveRequest(row sqliteResolveRequestRow) (sqliteEncodedResolveRequest, error) {
	if !sqliteValidResolveRequest(row) {
		return sqliteEncodedResolveRequest{}, failure("validation")
	}
	row.Result.EntityRevision = sqliteCopyString(row.Result.EntityRevision)
	row.Result.AffectedIDs = append([]string{}, row.Result.AffectedIDs...)
	payload, err := json.Marshal(row.Result)
	if err != nil {
		return sqliteEncodedResolveRequest{}, failure("validation")
	}
	texts := []string{row.ID, "activity.resolve", row.Fingerprint, "mutation_result", string(payload)}
	charge, err := sqliteRowCharge(0, 0, texts, nil)
	if err != nil {
		return sqliteEncodedResolveRequest{}, err
	}
	return sqliteEncodedResolveRequest{id: row.ID, charge: charge, values: [5]sqliteio.Value{
		sqliteio.Text(texts[0]), sqliteio.Text(texts[1]), sqliteio.Text(texts[2]), sqliteio.Text(texts[3]), sqliteio.Text(texts[4]),
	}}, nil
}
