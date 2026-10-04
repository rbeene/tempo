//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteMutationRequestRow struct {
	ID      string
	Value   mutationRequest
	payload string
}

const sqliteMutationRequestColumns = "request_id,operation,fingerprint,outcome_kind,payload"

type sqliteEncodedMutationRequest struct {
	row     sqliteMutationRequestRow
	values  [5]sqliteio.Value
	pending *sqlitePendingSyncRow
	charge  int64
}

func sqliteReadMutationRequestLocal(tx *sqliteio.Tx, computer, id, revisionCeiling string) (sqliteMutationRequestRow, bool, error) {
	ceiling, ok := counter(revisionCeiling)
	if !validUUID(computer) || !validUUID(id) || !ok || ceiling == 0 {
		return sqliteMutationRequestRow{}, false, failure("validation")
	}
	row, found, err := sqliteReadMutationRequestUnit(tx, computer, id)
	if err != nil || !found {
		return sqliteMutationRequestRow{}, false, err
	}
	if revision := sqliteMutationRequestRevision(row.Value); revision != "" {
		n, _ := counter(revision)
		if n > ceiling {
			return sqliteMutationRequestRow{}, false, failure("state_corrupt")
		}
	}
	return row, true, nil
}

// The writer has no metadata/ceiling authority. Its internal observation checks
// the complete selected unit and is compared to the caller's exact before row.
func sqliteReadMutationRequestUnit(tx *sqliteio.Tx, computer, id string) (sqliteMutationRequestRow, bool, error) {
	row, found, err := sqliteReadMutationRequestScalar(tx, computer, id)
	if err != nil {
		return sqliteMutationRequestRow{}, false, err
	}
	pending, pendingFound, err := sqliteReadPendingSync(tx, id)
	if err != nil {
		return sqliteMutationRequestRow{}, false, err
	}
	if !found {
		if pendingFound {
			return sqliteMutationRequestRow{}, false, failure("state_corrupt")
		}
		return sqliteMutationRequestRow{}, false, nil
	}
	if row.Value.PendingSync == nil {
		if pendingFound {
			return sqliteMutationRequestRow{}, false, failure("state_corrupt")
		}
	} else {
		expected := sqliteProjectPendingSync(id, *row.Value.PendingSync)
		if !pendingFound || !reflect.DeepEqual(pending, expected) {
			return sqliteMutationRequestRow{}, false, failure("state_corrupt")
		}
	}
	return row, true, nil
}

func sqliteReadMutationRequestScalar(tx *sqliteio.Tx, computer, id string) (result sqliteMutationRequestRow, found bool, err error) {
	s, err := tx.Prepare("SELECT "+sqliteMutationRequestColumns+" FROM requests WHERE request_id=?", sqliteio.Text(id))
	if err != nil {
		return result, false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result, found = sqliteMutationRequestRow{}, false
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
	for column := range fields {
		fields[column], err = sqliteHostNormalizationText(s, column)
		if err != nil {
			return result, false, err
		}
	}
	if fields[0] != id {
		return result, false, failure("state_corrupt")
	}
	value, decodeErr := sqliteDecodeMutationPayload(fields[1], fields[2], fields[3], fields[4])
	if decodeErr != nil {
		return result, false, decodeErr
	}
	result = sqliteMutationRequestRow{ID: id, Value: value, payload: fields[4]}
	if !sqliteValidMutationRequest(computer, result) {
		return result, false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return result, false, err
	} else if present {
		return result, false, failure("state_corrupt")
	}
	return result, true, nil
}

func sqliteMutationRequestCharge(computer string, row sqliteMutationRequestRow) (int64, error) {
	encoded, err := sqliteEncodeMutationRequest(computer, row)
	if err != nil {
		return 0, err
	}
	return encoded.charge, nil
}

func sqliteWriteMutationRequest(tx *sqliteio.Tx, computer string, before *sqliteMutationRequestRow, after sqliteMutationRequestRow) (int64, error) {
	if after.payload != "" {
		return 0, failure("validation")
	}
	next, err := sqliteEncodeMutationRequest(computer, after)
	if err != nil {
		return 0, err
	}
	var old sqliteEncodedMutationRequest
	if before != nil {
		old, err = sqliteEncodeMutationRequest(computer, *before)
		if err != nil {
			return 0, err
		}
		if !sqliteValidMutationTransition(*before, after) {
			return 0, failure("validation")
		}
		current, found, readErr := sqliteReadMutationRequestUnit(tx, computer, before.ID)
		if readErr != nil {
			return 0, readErr
		}
		if !found || !reflect.DeepEqual(current, old.row) {
			return 0, failure("state_corrupt")
		}
	} else {
		// An insert must not bless preexisting orphan projection rows. The
		// actual request PK remains responsible for the request collision.
		_, found, readErr := sqliteReadPendingSync(tx, after.ID)
		if readErr != nil {
			return 0, readErr
		}
		if found && next.pending == nil {
			return 0, failure("state_corrupt")
		}
	}
	if err := sqliteWriteMutationRequestScalar(tx, before == nil, old, next); err != nil {
		return 0, err
	}
	if before == nil {
		if next.pending != nil {
			if err := sqliteInsertPendingSync(tx, *next.pending); err != nil {
				return 0, err
			}
		}
	} else if next.pending != nil {
		if err := sqliteUpdatePendingSync(tx, *old.pending, *next.pending); err != nil {
			return 0, err
		}
	} else {
		if err := sqliteDeletePendingSync(tx, *old.pending); err != nil {
			return 0, err
		}
	}
	return next.charge - old.charge, nil
}

func sqliteWriteMutationRequestScalar(tx *sqliteio.Tx, insert bool, before, after sqliteEncodedMutationRequest) (err error) {
	var s *sqliteio.Stmt
	if insert {
		s, err = tx.Prepare("INSERT INTO requests("+sqliteMutationRequestColumns+") VALUES(?,?,?,?,?) RETURNING request_id", after.values[:]...)
	} else {
		values := append([]sqliteio.Value{}, after.values[1:]...)
		values = append(values, before.values[:]...)
		s, err = tx.Prepare(`UPDATE requests SET operation=?,fingerprint=?,outcome_kind=?,payload=?
			WHERE request_id=? AND operation=? AND fingerprint=? AND outcome_kind=? AND payload=? RETURNING request_id`, values...)
	}
	if err != nil {
		return sqliteMutationInsertError(err, insert, false)
	}
	defer func() {
		err = sqliteMutationInsertError(sqliteCloseMetaStatement(s, err), insert, false)
	}()
	return sqliteRecoveryReturnedID(s, after.row.ID)
}

// Only uniqueness codes belonging to these fixed insert statements become
// validation. Native CHECK/FK/IO/cancellation and all cleanup evidence survive.
func sqliteMutationInsertError(err error, insert, pending bool) error {
	var native *sqliteio.Error
	if insert && errors.As(err, &native) && native.Category == sqliteio.Constraint &&
		(native.Code == 1555 || pending && native.Code == 2067) {
		return errors.Join(failure("validation"), err)
	}
	return err
}

func sqliteEncodeMutationRequest(computer string, row sqliteMutationRequestRow) (sqliteEncodedMutationRequest, error) {
	if !sqliteValidMutationRequest(computer, row) {
		return sqliteEncodedMutationRequest{}, failure("validation")
	}
	kind, value := sqliteMutationOutcome(row.Value)
	payload := row.payload
	if payload == "" {
		encoded, err := json.Marshal(value)
		if err != nil {
			return sqliteEncodedMutationRequest{}, failure("validation")
		}
		payload = string(encoded)
	}
	materialized, err := sqliteDecodeMutationPayload(row.Value.Operation, row.Value.Fingerprint, kind, payload)
	if err != nil {
		return sqliteEncodedMutationRequest{}, failure("validation")
	}
	stored := sqliteMutationRequestRow{ID: row.ID, Value: materialized, payload: payload}
	if row.payload != "" && (!sqliteValidMutationRequest(computer, stored) || !reflect.DeepEqual(materialized, row.Value)) {
		return sqliteEncodedMutationRequest{}, failure("validation")
	}
	// Validate raw admission before Marshal. Do not strengthen its historical
	// policy by revalidating a fresh, potentially JSON-repaired proposal here.
	texts := []string{row.ID, row.Value.Operation, row.Value.Fingerprint, kind, payload}
	charge, err := sqliteRowCharge(0, 0, texts, nil)
	if err != nil {
		return sqliteEncodedMutationRequest{}, err
	}
	result := sqliteEncodedMutationRequest{row: stored, charge: charge}
	for i := range result.values {
		result.values[i] = sqliteio.Text(texts[i])
	}
	if materialized.PendingSync != nil {
		pending := sqliteProjectPendingSync(row.ID, *materialized.PendingSync)
		n, chargeErr := sqlitePendingSyncCharge(pending)
		if chargeErr != nil {
			return sqliteEncodedMutationRequest{}, chargeErr
		}
		if n > math.MaxInt64-result.charge {
			return sqliteEncodedMutationRequest{}, failure("state_corrupt")
		}
		result.pending, result.charge = &pending, result.charge+n
	}
	return result, nil
}

// The decoder is a finite six-way typed codec. Reflection is confined to the
// existing exactJSONFields allowlist, as in the selected resolve-proof reader.
func sqliteDecodeMutationPayload(operation, fingerprint, kind, payload string) (mutationRequest, error) {
	result := mutationRequest{Operation: operation, Fingerprint: fingerprint}
	var target any
	switch kind {
	case "binding_result":
		result.BindingResult = &BindingResult{}
		target = result.BindingResult
	case "mutation_result":
		result.MutationResult = &MutationResult{}
		target = result.MutationResult
	case "sync_configuration_result":
		result.SyncConfigurationResult = &SyncConfigurationResult{}
		target = result.SyncConfigurationResult
	case "sync_run":
		result.SyncRun = &SyncRun{}
		target = result.SyncRun
	case "error":
		result.Error = &Error{}
		target = result.Error
	case "pending_sync":
		result.PendingSync = &syncReservation{}
		target = result.PendingSync
	default:
		return mutationRequest{}, failure("state_corrupt")
	}
	raw := []byte(payload)
	if !strictJSON(raw) || !exactJSONFields(raw, reflect.TypeOf(target)) {
		return mutationRequest{}, failure("state_corrupt")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return mutationRequest{}, failure("state_corrupt")
	}
	return result, nil
}

func sqliteMutationOutcome(r mutationRequest) (string, any) {
	switch {
	case r.BindingResult != nil:
		return "binding_result", r.BindingResult
	case r.MutationResult != nil:
		return "mutation_result", r.MutationResult
	case r.SyncConfigurationResult != nil:
		return "sync_configuration_result", r.SyncConfigurationResult
	case r.SyncRun != nil:
		return "sync_run", r.SyncRun
	case r.Error != nil:
		return "error", r.Error
	case r.PendingSync != nil:
		return "pending_sync", r.PendingSync
	}
	return "", nil
}

func sqliteMutationRequestRevision(r mutationRequest) string {
	switch {
	case r.BindingResult != nil:
		return r.BindingResult.SnapshotRevision
	case r.MutationResult != nil:
		return r.MutationResult.SnapshotRevision
	case r.SyncConfigurationResult != nil:
		return r.SyncConfigurationResult.SnapshotRevision
	case r.SyncRun != nil:
		return r.SyncRun.SnapshotRevision
	case r.PendingSync != nil:
		return r.PendingSync.SnapshotRevision
	}
	return ""
}

func sqlitePositiveMutationCounter(value string) bool {
	n, ok := counter(value)
	return ok && n > 0
}

func sqliteValidMutationIDs(ids []string, unique bool) bool {
	if ids == nil {
		return false
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !validUUID(id) || unique && seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}

func sqliteValidMutationRequest(computer string, row sqliteMutationRequestRow) bool {
	r := row.Value
	if !validUUID(computer) || !validUUID(row.ID) || len(r.Fingerprint) != 64 {
		return false
	}
	for _, c := range r.Fingerprint {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	outcomes := 0
	for _, present := range []bool{r.BindingResult != nil, r.MutationResult != nil, r.SyncConfigurationResult != nil, r.SyncRun != nil, r.Error != nil, r.PendingSync != nil} {
		if present {
			outcomes++
		}
	}
	if outcomes != 1 {
		return false
	}
	if r.Error != nil {
		if recoveryOperation(r.Operation) {
			switch r.Error.Code {
			case "clock_unavailable", "clock_conflict", "recovery_bounds", "attribution_conflict":
				return reflect.DeepEqual(r.Error, failure(r.Error.Code))
			}
			return false
		}
		return (r.Operation == "sync.resolve" || r.Operation == "sync.reconcile") &&
			r.Error.Code == "local_write_unknown" && r.Error.Uncertain && r.Error.Details == nil
	}
	if !sqlitePositiveMutationCounter(sqliteMutationRequestRevision(r)) {
		return false
	}
	if r.BindingResult != nil {
		v := r.BindingResult
		return (r.Operation == "bindings.link" || r.Operation == "bindings.repair") &&
			v.ContractVersion == 1 && v.RequestID == row.ID && validBindingView(v.Binding, computer)
	}
	if r.SyncConfigurationResult != nil {
		v := r.SyncConfigurationResult
		return r.Operation == "sync.configure" && v.ContractVersion == 1 && v.RequestID == row.ID && validSyncConfiguration(v.Configuration)
	}
	if r.SyncRun != nil {
		v := r.SyncRun
		return (r.Operation == "sync.now" || r.Operation == "sync.reconcile") && v.ContractVersion == 1 &&
			v.RequestID == row.ID && (v.State == "complete" || v.State == "interrupted") && v.RemainingCount >= 0 &&
			sqliteValidMutationIDs(v.AttemptedIDs, true) && sqliteValidMutationIDs(v.ResolvedIDs, true) && sqliteValidMutationIDs(v.BlockedIDs, true)
	}
	if r.PendingSync != nil {
		return sqliteValidPendingReservation(row.ID, r)
	}
	v := r.MutationResult
	if v == nil || v.ContractVersion != 1 || v.RequestID != row.ID {
		return false
	}
	if r.Operation == "sync.pause" || r.Operation == "sync.resume" || r.Operation == "sync.resolve" {
		// The stored sync predicate deliberately permits duplicates and opaque
		// EntityRevision, including Changed=false with nonempty affected IDs.
		return sqliteValidMutationIDs(v.AffectedIDs, false)
	}
	if !sqliteValidMutationIDs(v.AffectedIDs, true) || v.EntityRevision != nil && !sqlitePositiveMutationCounter(*v.EntityRevision) {
		return false
	}
	if r.Operation == "bindings.unlink" {
		return v.Changed && len(v.AffectedIDs) == 1 && v.EntityRevision != nil
	}
	if !recoveryOperation(r.Operation) || !v.Changed && (len(v.AffectedIDs) != 0 || v.EntityRevision != nil) {
		return false
	}
	if r.Operation == "activity.resolve" || r.Operation == "activity.interrupt" {
		return v.Changed && len(v.AffectedIDs) > 0 && v.EntityRevision != nil
	}
	return v.EntityRevision == nil
}

func sqliteValidPendingReservation(id string, r mutationRequest) bool {
	p := r.PendingSync
	if p == nil || !sqliteValidMutationIDs(p.RootIDs, true) {
		return false
	}
	inputs := 0
	if p.Run != nil {
		inputs++
		in := p.Run
		if r.Operation != "sync.now" || in.RequestID != id || in.Limit < 1 || in.Limit > 100 || len(p.RootIDs) > in.Limit ||
			p.EffectCommitted || r.Fingerprint != mutationFingerprint(r.Operation, *in) {
			return false
		}
	}
	if p.Reconcile != nil {
		inputs++
		in := p.Reconcile
		if r.Operation != "sync.reconcile" || in.RequestID != id || in.Limit < 1 || in.Limit > 100 || len(p.RootIDs) > in.Limit ||
			in.OutboxID != "" && !validUUID(in.OutboxID) || r.Fingerprint != mutationFingerprint(r.Operation, *in) {
			return false
		}
	}
	if p.Resolve != nil {
		inputs++
		in := p.Resolve
		if r.Operation != "sync.resolve" || in.RequestID != id || !in.Confirmed || !validUUID(in.OutboxID) ||
			len(p.RootIDs) != 1 || p.RootIDs[0] != in.OutboxID || r.Fingerprint != mutationFingerprint(r.Operation, *in) {
			return false
		}
	}
	return inputs == 1
}

func sqliteValidMutationTransition(before, after sqliteMutationRequestRow) bool {
	if before.Value.PendingSync == nil || before.ID != after.ID || before.Value.Operation != after.Value.Operation || before.Value.Fingerprint != after.Value.Fingerprint {
		return false
	}
	old, next := before.Value.PendingSync, after.Value.PendingSync
	if next != nil {
		if old.SnapshotRevision != next.SnapshotRevision || !reflect.DeepEqual(old.RootIDs, next.RootIDs) ||
			!reflect.DeepEqual(old.Run, next.Run) || !reflect.DeepEqual(old.Reconcile, next.Reconcile) || !reflect.DeepEqual(old.Resolve, next.Resolve) {
			return false
		}
		return old.EffectCommitted == next.EffectCommitted || !old.EffectCommitted && next.EffectCommitted && old.Reconcile != nil
	}
	switch before.Value.Operation {
	case "sync.now":
		return after.Value.SyncRun != nil
	case "sync.reconcile":
		return after.Value.SyncRun != nil || after.Value.Error != nil
	case "sync.resolve":
		return after.Value.MutationResult != nil || after.Value.Error != nil
	}
	return false
}
