//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"reflect"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func sqliteBindingMutationReceipt(tx *sqliteio.Tx, meta sqliteStoreMeta, p sqliteBindingMutation) (sqliteMutationRequestRow, bool, error) {
	row, found, err := sqliteReadMutationRequestLocal(tx, meta.ComputerID, p.ID, meta.Revision)
	if err != nil || !found {
		return sqliteMutationRequestRow{}, false, err
	}
	if row.Value.Operation != p.Operation || row.Value.Fingerprint != p.Fingerprint {
		return sqliteMutationRequestRow{}, false, failure("request_conflict")
	}
	var revision, kind string
	switch p.Operation {
	case "bindings.repair":
		v := row.Value.BindingResult
		if v == nil || v.Binding.ID != p.BindingID {
			return sqliteMutationRequestRow{}, false, failure("state_corrupt")
		}
		revision, kind = v.Binding.Revision, v.Binding.Kind
	case "bindings.unlink":
		v := row.Value.MutationResult
		if v == nil || len(v.AffectedIDs) != 1 || v.AffectedIDs[0] != p.BindingID || v.EntityRevision == nil {
			return sqliteMutationRequestRow{}, false, failure("state_corrupt")
		}
		revision = *v.EntityRevision
	default:
		return sqliteMutationRequestRow{}, false, failure("validation")
	}
	binding, found, err := sqliteReadBinding(tx, meta.ComputerID, p.BindingID)
	if err != nil {
		return sqliteMutationRequestRow{}, false, err
	}
	old, oldOK := counter(revision)
	current, currentOK := counter(binding.Snapshot.Revision)
	ceiling, _ := counter(meta.Revision)
	if !found || binding.Record == nil || !oldOK || !currentOK || old == 0 || current < old || current > ceiling ||
		kind != "" && binding.Record.Kind != kind || p.Operation == "bindings.unlink" && !binding.Record.Deleted {
		return sqliteMutationRequestRow{}, false, failure("state_corrupt")
	}
	// Historical location, attribution and attachment projections stay owned
	// by the receipt even after later repairs, relinks or deletion.
	return row, true, nil
}

func sqliteMutableBinding(tx *sqliteio.Tx, meta sqliteStoreMeta, id, revision string) (sqliteBindingRow, error) {
	row, found, err := sqliteReadBinding(tx, meta.ComputerID, id)
	if err != nil {
		return sqliteBindingRow{}, err
	}
	if !found || row.Record == nil || row.Record.Deleted {
		return sqliteBindingRow{}, failure("not_found")
	}
	current, _ := counter(row.Snapshot.Revision)
	ceiling, _ := counter(meta.Revision)
	if current > ceiling {
		return sqliteBindingRow{}, failure("state_corrupt")
	}
	if row.Snapshot.Revision != revision {
		return sqliteBindingRow{}, failure("revision_conflict")
	}
	refs, err := sqliteLinkAttached(tx, meta, row)
	if err != nil {
		return sqliteBindingRow{}, err
	}
	if len(refs) != 0 {
		return sqliteBindingRow{}, failure("binding_in_use")
	}
	if revisionExhausted(revision) {
		return sqliteBindingRow{}, failure("validation")
	}
	return row, nil
}

func sqliteFenceBindingMutation(tx *sqliteio.Tx, meta sqliteStoreMeta) error {
	if err := sqliteCaptureCapacity(tx, meta); err != nil {
		return err
	}
	after := meta
	var err error
	after.DurabilityNonce, err = sqliteNextNonce(meta.DurabilityNonce[:])
	if err == nil {
		err = sqliteUpdateMeta(tx, meta, after)
	}
	if err != nil {
		return err
	}
	got, err := sqliteReadMeta(tx, meta.StateBasename, meta.DatabaseBasename)
	if err == nil && !reflect.DeepEqual(got, after) {
		err = failure("state_corrupt")
	}
	return err
}

func sqliteApplyBindingMutation(tx *sqliteio.Tx, meta sqliteStoreMeta, p sqliteBindingMutation) (mutationRequest, error) {
	before, err := sqliteMutableBinding(tx, meta, p.BindingID, p.IfRevision)
	if err != nil {
		return mutationRequest{}, err
	}
	if p.Before != nil && !reflect.DeepEqual(before, *p.Before) {
		return mutationRequest{}, failure("state_busy")
	}
	if revisionExhausted(meta.Revision) {
		return mutationRequest{}, failure("validation")
	}
	after := before
	record := *before.Record
	after.Record = &record
	changed := true
	switch p.Operation {
	case "bindings.unlink":
		record.Deleted = true
	case "bindings.repair":
		if p.Before == nil || !validStoredLocation(p.Location.Kind, p.Location.Locator) || p.Location.Kind != record.Kind {
			return mutationRequest{}, failure("validation")
		}
		other, exists, readErr := sqliteReadBindingLocation(tx, meta.ComputerID, p.Location.Kind, p.Location.Locator)
		if readErr != nil {
			return mutationRequest{}, readErr
		}
		if exists {
			revision, _ := counter(other.Snapshot.Revision)
			ceiling, _ := counter(meta.Revision)
			if revision > ceiling {
				return mutationRequest{}, failure("state_corrupt")
			}
			if other.Snapshot.ID != before.Snapshot.ID {
				return mutationRequest{}, failure("revision_conflict")
			}
		}
		changed = record.Locator != p.Location.Locator
		record.Locator = p.Location.Locator
	default:
		return mutationRequest{}, failure("validation")
	}
	if changed {
		after.Snapshot.Revision = bump(before.Snapshot.Revision)
	}
	record.Snapshot = after.Snapshot
	encoded, err := sqliteEncodeBinding(after)
	if err != nil {
		return mutationRequest{}, err
	}
	after = encoded.row
	next := meta
	next.Revision = bump(meta.Revision)
	next.DurabilityNonce, err = sqliteNextNonce(meta.DurabilityNonce[:])
	if err != nil {
		return mutationRequest{}, err
	}
	result := mutationRequest{Operation: p.Operation, Fingerprint: p.Fingerprint}
	if p.Operation == "bindings.unlink" {
		revision := after.Snapshot.Revision
		result.MutationResult = &MutationResult{ContractVersion: 1, SnapshotRevision: next.Revision, RequestID: p.ID, Changed: true, AffectedIDs: []string{p.BindingID}, EntityRevision: &revision}
	} else {
		result.BindingResult = &BindingResult{ContractVersion: 1, SnapshotRevision: next.Revision, RequestID: p.ID, Changed: changed, Binding: Binding{ID: after.Snapshot.ID, Revision: after.Snapshot.Revision, Kind: after.Record.Kind, Locator: after.Record.Locator, Attribution: after.Snapshot.Attribution, AttachedActors: []ActorRef{}}}
	}
	request := sqliteMutationRequestRow{ID: p.ID, Value: result}
	encodedRequest, err := sqliteEncodeMutationRequest(meta.ComputerID, request)
	if err != nil {
		return mutationRequest{}, err
	}
	oldCharge, err := sqliteBindingCharge(before)
	if err != nil {
		return mutationRequest{}, err
	}
	delta := encoded.charge - oldCharge
	if !changed && delta != 0 {
		return mutationRequest{}, failure("state_corrupt")
	}
	next.LogicalBytes, err = sqliteLinkAddCharge(meta.LogicalBytes, delta)
	if err == nil {
		next.LogicalBytes, err = sqliteLinkAddCharge(next.LogicalBytes, encodedRequest.charge)
	}
	if err != nil {
		return mutationRequest{}, err
	}
	if err = sqliteMetaCapacity(meta); err != nil {
		return mutationRequest{}, err
	}
	if err = sqliteCaptureCapacity(tx, next); err != nil {
		return mutationRequest{}, err
	}
	var written int64
	if changed {
		written, err = sqliteUpdateBinding(tx, before, after)
	}
	if err != nil {
		return mutationRequest{}, err
	}
	if written != delta {
		return mutationRequest{}, failure("state_corrupt")
	}
	written, err = sqliteWriteMutationRequest(tx, meta.ComputerID, nil, request)
	if err != nil {
		return mutationRequest{}, err
	}
	if written != encodedRequest.charge {
		return mutationRequest{}, failure("state_corrupt")
	}
	if err = sqliteUpdateMeta(tx, meta, next); err != nil {
		return mutationRequest{}, err
	}
	if err = sqliteLinkReadback(tx, next, after, encodedRequest.row); err != nil {
		return mutationRequest{}, err
	}
	return encodedRequest.row.Value, nil
}
