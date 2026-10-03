//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"math"
	"reflect"
	"sort"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Prepared values are owned observations. No provider, discovery callback or
// legacy state graph is carried into the transaction.
type sqliteLinkPrepared struct {
	Input            LinkInput
	Fingerprint      string
	Location         Location
	Attribution      Attribution
	PreparationError error
}

// An empty catalog is the sole initialization branch. The shared fixed catalog
// reader admits every nonempty store; it must be integrated before this port.
func sqliteReadLinkSchema(tx *sqliteio.Tx, stateBase, databaseBase string) (sqliteStoreMeta, bool, error) {
	s, err := tx.Prepare("SELECT 1 FROM sqlite_schema LIMIT 1")
	if err != nil {
		return sqliteStoreMeta{}, false, err
	}
	present, err := s.Step()
	if err == nil && present {
		if s.ColumnCount() != 1 {
			err = failure("state_corrupt")
		} else {
			var kind sqliteio.Kind
			kind, err = s.Kind(0)
			if err == nil && kind != sqliteio.IntegerKind {
				err = failure("state_corrupt")
			}
			if err == nil {
				var value int64
				value, err = s.Int64(0)
				if err == nil && value != 1 {
					err = failure("state_corrupt")
				}
			}
		}
		if err == nil {
			var extra bool
			extra, err = s.Step()
			if err == nil && extra {
				err = failure("state_corrupt")
			}
		}
	}
	if err = sqliteCloseMetaStatement(s, err); err != nil {
		return sqliteStoreMeta{}, false, err
	}
	if !present {
		return sqliteStoreMeta{}, false, nil
	}
	meta, err := sqliteReadCaptureSchema(tx, stateBase, databaseBase)
	if err != nil {
		return sqliteStoreMeta{}, false, err
	}
	return meta, true, nil
}

// The receipt decoder supplies an owned historical value. Only its retained
// binding identity/revision is current: movement, deletion and later rebinding
// do not rewrite the result or require its historical actors to be current.
func sqliteLinkReceipt(tx *sqliteio.Tx, meta sqliteStoreMeta, requestID, fingerprint string) (BindingResult, bool, error) {
	row, found, err := sqliteReadMutationRequestLocal(tx, meta.ComputerID, requestID, meta.Revision)
	if err != nil || !found {
		return BindingResult{}, false, err
	}
	if row.Value.Operation != "bindings.link" || row.Value.Fingerprint != fingerprint {
		return BindingResult{}, false, failure("request_conflict")
	}
	if row.Value.BindingResult == nil {
		return BindingResult{}, false, failure("state_corrupt")
	}
	result := *row.Value.BindingResult
	binding, found, err := sqliteReadBinding(tx, meta.ComputerID, result.Binding.ID)
	if err != nil {
		return BindingResult{}, false, err
	}
	old, oldOK := counter(result.Binding.Revision)
	current, currentOK := counter(binding.Snapshot.Revision)
	if !found || binding.Record == nil || !oldOK || !currentOK || current < old {
		return BindingResult{}, false, failure("state_corrupt")
	}
	result.Binding.AttachedActors = append([]ActorRef{}, result.Binding.AttachedActors...)
	return result, true, nil
}

func sqliteReplayLink(tx *sqliteio.Tx, meta sqliteStoreMeta, requestID, fingerprint string) (BindingResult, bool, error) {
	result, found, err := sqliteLinkReceipt(tx, meta, requestID, fingerprint)
	if err != nil || !found {
		return BindingResult{}, false, err
	}
	// From here a known historical receipt cannot be declared uncommitted by a
	// failed new fence, including capacity or a stale full-before CAS.
	if err = sqliteLinkPressure(tx, meta.LogicalBytes, meta.LogicalBytes); err != nil {
		return BindingResult{}, false, errors.Join(failure("local_write_unknown"), err)
	}
	next := meta
	next.DurabilityNonce, err = sqliteNextNonce(meta.DurabilityNonce[:])
	if err == nil {
		err = sqliteUpdateMeta(tx, meta, next)
	}
	if err == nil {
		var observed sqliteStoreMeta
		observed, err = sqliteReadMeta(tx, meta.StateBasename, meta.DatabaseBasename)
		if err == nil && !reflect.DeepEqual(observed, next) {
			err = failure("state_corrupt")
		}
	}
	if err != nil {
		return BindingResult{}, false, errors.Join(failure("local_write_unknown"), err)
	}
	return result, true, nil
}

// Callers own Write admission, the immediate CheckAuthorityAbsent, COMMIT and
// checked closure. This unit returns only provisional, fully checked values.
func sqliteLinkTransaction(tx *sqliteio.Tx, stateBase, databaseBase string, p sqliteLinkPrepared) (BindingResult, error) {
	result, _, err := sqliteLinkUnit(tx, stateBase, databaseBase, p)
	return result, err
}

// The private coordinator also needs to remember receipt establishment across
// COMMIT. The public transaction helper deliberately exposes no ACK authority.
func sqliteLinkUnit(tx *sqliteio.Tx, stateBase, databaseBase string, p sqliteLinkPrepared) (BindingResult, bool, error) {
	meta, current, err := sqliteReadLinkSchema(tx, stateBase, databaseBase)
	if err != nil {
		return BindingResult{}, false, err
	}
	if current {
		_, found, err := sqliteLinkReceipt(tx, meta, p.Input.RequestID, p.Fingerprint)
		if err != nil {
			return BindingResult{}, false, err
		}
		if found {
			result, _, err := sqliteReplayLink(tx, meta, p.Input.RequestID, p.Fingerprint)
			return result, true, err
		}
	}
	if p.PreparationError != nil {
		return BindingResult{}, false, p.PreparationError
	}
	result, err := sqliteLinkMutate(tx, meta, current, stateBase, databaseBase, p)
	return result, false, err
}

func sqliteLinkMutate(tx *sqliteio.Tx, meta sqliteStoreMeta, current bool, stateBase, databaseBase string, p sqliteLinkPrepared) (BindingResult, error) {
	if !validUUID(p.Input.RequestID) || p.Fingerprint != mutationFingerprint("bindings.link", p.Input) {
		return BindingResult{}, failure("validation")
	}
	if p.Input.IfRevision != "" {
		if _, ok := counter(p.Input.IfRevision); !ok {
			return BindingResult{}, failure("validation")
		}
	}
	var before sqliteBindingRow
	var err error
	exists := false
	refs := []ActorRef{}
	if current {
		before, exists, err = sqliteReadBindingLocation(tx, meta.ComputerID, p.Location.Kind, p.Location.Locator)
		if err != nil {
			return BindingResult{}, err
		}
	}
	changed := !exists || before.Snapshot.Attribution != p.Attribution
	if exists {
		if p.Input.IfRevision != "" && p.Input.IfRevision != before.Snapshot.Revision || p.Input.IfRevision == "" && changed {
			return BindingResult{}, failure("revision_conflict")
		}
		refs, err = sqliteLinkAttached(tx, meta, before)
		if err != nil {
			return BindingResult{}, err
		}
		if changed && len(refs) != 0 {
			return BindingResult{}, failure("binding_in_use")
		}
	} else if p.Input.IfRevision != "" {
		return BindingResult{}, failure("revision_conflict")
	}
	if current {
		if err = sqliteLinkPeers(tx, meta.ComputerID, before.Snapshot.ID, p.Attribution); err != nil {
			return BindingResult{}, err
		}
		if revisionExhausted(meta.Revision) || exists && changed && revisionExhausted(before.Snapshot.Revision) {
			return BindingResult{}, failure("validation")
		}
	}
	if err = sqliteLinkPressure(tx, meta.LogicalBytes, meta.LogicalBytes); err != nil {
		return BindingResult{}, err
	}
	next := meta
	if current {
		next.Revision = bump(meta.Revision)
		next.DurabilityNonce, err = sqliteNextNonce(meta.DurabilityNonce[:])
		if err != nil {
			return BindingResult{}, err
		}
	} else {
		next = sqliteStoreMeta{ComputerID: newID(), Revision: "1", StateBasename: stateBase, DatabaseBasename: databaseBase}
	}
	after := before
	if !exists {
		snapshot := BindingSnapshot{ID: newID(), Revision: "1", Attribution: p.Attribution}
		after = sqliteBindingRow{ComputerID: next.ComputerID, Snapshot: snapshot,
			Record: &bindingRecord{Snapshot: snapshot, Kind: p.Location.Kind, Locator: p.Location.Locator}}
	} else if changed {
		after.Snapshot.Revision = bump(before.Snapshot.Revision)
		after.Snapshot.Attribution = p.Attribution
		record := *before.Record
		record.Snapshot = after.Snapshot
		after.Record = &record
	}
	encoded, err := sqliteEncodeBinding(after)
	if err != nil {
		return BindingResult{}, err
	}
	after = encoded.row
	result := BindingResult{ContractVersion: 1, SnapshotRevision: next.Revision, RequestID: p.Input.RequestID,
		Changed: changed, Binding: Binding{ID: after.Snapshot.ID, Revision: after.Snapshot.Revision,
			Kind: after.Record.Kind, Locator: after.Record.Locator, Attribution: after.Snapshot.Attribution,
			AttachedActors: append([]ActorRef{}, refs...)}}
	request := sqliteMutationRequestRow{ID: p.Input.RequestID,
		Value: mutationRequest{Operation: "bindings.link", Fingerprint: p.Fingerprint, BindingResult: &result}}
	encodedRequest, err := sqliteEncodeMutationRequest(next.ComputerID, request)
	if err != nil {
		return BindingResult{}, err
	}
	bindingDelta := encoded.charge
	if exists {
		oldCharge, chargeErr := sqliteBindingCharge(before)
		if chargeErr != nil {
			return BindingResult{}, chargeErr
		}
		bindingDelta -= oldCharge
		if !changed && bindingDelta != 0 {
			return BindingResult{}, failure("state_corrupt")
		}
	}
	if !current {
		next.LogicalBytes, err = sqliteMetaCharge(next)
		if err != nil {
			return BindingResult{}, err
		}
	}
	next.LogicalBytes, err = sqliteLinkAddCharge(next.LogicalBytes, bindingDelta)
	if err == nil {
		next.LogicalBytes, err = sqliteLinkAddCharge(next.LogicalBytes, encodedRequest.charge)
	}
	if err != nil {
		return BindingResult{}, err
	}
	if err = sqliteLinkPressure(tx, meta.LogicalBytes, next.LogicalBytes); err != nil {
		return BindingResult{}, err
	}
	if !current {
		if err = sqliteCreateSchema(tx); err != nil {
			return BindingResult{}, err
		}
	}
	var written int64
	if !exists {
		written, err = sqliteInsertBinding(tx, after)
	} else if changed {
		written, err = sqliteUpdateBinding(tx, before, after)
	}
	if err != nil {
		return BindingResult{}, err
	}
	if written != bindingDelta {
		return BindingResult{}, failure("state_corrupt")
	}
	written, err = sqliteWriteMutationRequest(tx, next.ComputerID, nil, request)
	if err != nil {
		return BindingResult{}, err
	}
	if written != encodedRequest.charge {
		return BindingResult{}, failure("state_corrupt")
	}
	if current {
		err = sqliteUpdateMeta(tx, meta, next)
	} else {
		err = sqliteInsertMeta(tx, next)
	}
	if err != nil {
		return BindingResult{}, err
	}
	if err = sqliteLinkReadback(tx, next, after, encodedRequest.row); err != nil {
		return BindingResult{}, err
	}
	if !current {
		if err = sqliteLinkBootstrapEmpty(tx); err != nil {
			return BindingResult{}, err
		}
		if err = tx.CheckForeignKeys(); err != nil {
			return BindingResult{}, err
		}
	}
	return *encodedRequest.row.Value.BindingResult, nil
}

func sqliteLinkReadback(tx *sqliteio.Tx, meta sqliteStoreMeta, binding sqliteBindingRow, request sqliteMutationRequestRow) error {
	gotMeta, err := sqliteReadMeta(tx, meta.StateBasename, meta.DatabaseBasename)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(gotMeta, meta) {
		return failure("state_corrupt")
	}
	gotBinding, found, err := sqliteReadBinding(tx, meta.ComputerID, binding.Snapshot.ID)
	if err != nil {
		return err
	}
	if !found || !reflect.DeepEqual(gotBinding, binding) {
		return failure("state_corrupt")
	}
	gotRequest, found, err := sqliteReadMutationRequestLocal(tx, meta.ComputerID, request.ID, meta.Revision)
	if err != nil {
		return err
	}
	if !found || !reflect.DeepEqual(gotRequest, request) {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteLinkPeers(tx *sqliteio.Tx, computer, selectedID string, attribution Attribution) error {
	s, err := tx.Prepare("SELECT binding_id FROM bindings WHERE computer_id=? AND account_id=? AND project_id=? AND active=1 ORDER BY binding_id",
		sqliteio.Text(computer), sqliteio.Text(attribution.AccountID), sqliteio.Text(attribution.ProjectID))
	if err != nil {
		return err
	}
	ids, err := sqliteDependencyUUIDRows(s)
	if err != nil {
		return err
	}
	for _, id := range ids {
		row, found, err := sqliteReadBinding(tx, computer, id)
		if err != nil {
			return err
		}
		if !found || row.Snapshot.Attribution.AccountID != attribution.AccountID || row.Snapshot.Attribution.ProjectID != attribution.ProjectID || row.Record != nil && row.Record.Deleted {
			return failure("state_corrupt")
		}
		if id != selectedID && row.Snapshot.Attribution != attribution {
			return attributionConflict(row.Snapshot.Attribution, attribution)
		}
	}
	return nil
}

func sqliteLinkAttached(tx *sqliteio.Tx, meta sqliteStoreMeta, binding sqliteBindingRow) ([]ActorRef, error) {
	s, err := tx.Prepare("SELECT actor_key,generation FROM actors WHERE binding_id=? AND state NOT IN ('finished','interrupted') ORDER BY actor_key", sqliteio.Text(binding.Snapshot.ID))
	if err != nil {
		return nil, err
	}
	type head struct{ key, generation string }
	heads := []head{}
	for {
		var present bool
		present, err = s.Step()
		if err != nil || !present {
			break
		}
		if s.ColumnCount() != 2 {
			err = failure("state_corrupt")
			break
		}
		var item head
		item.key, err = sqliteDependencyText(s, 0)
		if err != nil {
			break
		}
		var kind sqliteio.Kind
		kind, err = s.Kind(1)
		if err == nil && kind != sqliteio.BlobKind {
			err = failure("state_corrupt")
		}
		if err != nil {
			break
		}
		var encoded []byte
		encoded, err = s.Blob(1)
		if err == nil {
			item.generation, err = sqliteDecodeUint64(encoded)
		}
		if err != nil {
			break
		}
		if len(heads) != 0 && heads[len(heads)-1].key >= item.key {
			err = failure("state_corrupt")
			break
		}
		heads = append(heads, item)
	}
	if err = sqliteCloseMetaStatement(s, err); err != nil {
		return nil, err
	}
	refs := []ActorRef{}
	selection := sqliteDependencySelection{}
	for _, item := range heads {
		ref, err := sqliteReadHostReceiptGeneration(tx, item.key, item.generation)
		if err != nil {
			return nil, err
		}
		if ref.Key.ComputerID != meta.ComputerID || actorKey(ref.Key) != item.key || ref.Generation != item.generation {
			return nil, failure("state_corrupt")
		}
		actor, found, err := sqliteReadActorLocal(tx, meta.ComputerID, ref.Key)
		if err != nil {
			return nil, err
		}
		actorRevision, actorOK := counter(actor.BindingRevision)
		bindingRevision, bindingOK := counter(binding.Snapshot.Revision)
		if !found || actor.Ref != ref || actor.BindingID != binding.Snapshot.ID || actor.State == "finished" || actor.State == "interrupted" ||
			!actorOK || !bindingOK || actorRevision > bindingRevision || actor.Attribution != binding.Snapshot.Attribution {
			return nil, failure("state_corrupt")
		}
		ids, err := sqliteActorUncertaintyIDsLocal(tx, meta.ComputerID, ref.Key)
		if err != nil {
			return nil, err
		}
		for ordinal := range ids {
			selection.ActorUncertaintyEdges = append(selection.ActorUncertaintyEdges, sqliteActorUncertaintySelection{Key: ref.Key, Ordinal: int64(ordinal)})
		}
		selection.ActorKeys = append(selection.ActorKeys, ref.Key)
		refs = append(refs, ref)
	}
	if err = sqliteValidateSelectedCaptureDependencies(tx, meta.ComputerID, meta.Revision, selection); err != nil {
		return nil, err
	}
	sort.Slice(refs, func(i, j int) bool { return actorKey(refs[i].Key) < actorKey(refs[j].Key) })
	return refs, nil
}

func sqliteLinkAddCharge(total, delta int64) (int64, error) {
	if total < 0 || delta > 0 && total > math.MaxInt64-delta || delta < 0 && delta < -total {
		return 0, failure("state_corrupt")
	}
	return total + delta, nil
}

func sqliteLinkPressure(tx *sqliteio.Tx, before, after int64) error {
	if before < 0 || after < 0 {
		return failure("state_corrupt")
	}
	if before > sqliteLogicalCapacity || after > sqliteLogicalCapacity {
		return sqliteLinkCapacityError("logical_capacity")
	}
	pages, err := tx.PageInfo()
	if err != nil {
		return err
	}
	if pages.PageCount > sqliteio.MaxPages {
		return sqliteLinkCapacityError("database_capacity")
	}
	files, err := tx.Footprint()
	if err != nil {
		return err
	}
	if files.WAL >= 64<<20 || files.Total >= 320<<20 {
		return sqliteLinkCapacityError("maintenance_required")
	}
	return nil
}

func sqliteLinkCapacityError(reason string) *Error {
	return &Error{Code: "validation", Message: "local activity state capacity reached; preserve the existing state for review", Details: map[string]any{"reason": reason}}
}

// This audit is used only after creating the complete schema and exactly one
// binding/request/meta row in a previously empty writer snapshot.
func sqliteLinkBootstrapEmpty(tx *sqliteio.Tx) (err error) {
	s, err := tx.Prepare(sqliteLinkBootstrapEmptySQL)
	if err != nil {
		return err
	}
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	for _, want := range sqliteLinkBootstrapTables {
		present, err := s.Step()
		if err != nil {
			return err
		}
		if !present || s.ColumnCount() != 2 {
			return failure("state_corrupt")
		}
		name, err := sqliteDependencyText(s, 0)
		if err != nil {
			return err
		}
		kind, err := s.Kind(1)
		if err != nil {
			return err
		}
		if name != want || kind != sqliteio.IntegerKind {
			return failure("state_corrupt")
		}
		occupied, err := s.Int64(1)
		if err != nil {
			return err
		}
		if occupied != 0 {
			return failure("state_corrupt")
		}
	}
	present, err := s.Step()
	if err == nil && present {
		err = failure("state_corrupt")
	}
	return err
}

var sqliteLinkBootstrapTables = [...]string{
	"actor_generations", "actors", "actor_uncertainties", "host_sessions", "host_turns", "host_tools", "host_receipts",
	"epochs", "segments", "segment_events", "uncertainties", "uncertainty_evidence", "event_receipts", "event_receipt_uncertainties", "event_ids",
	"union_frontier", "component_segments", "pending_finalization", "intervals", "interval_segments", "interval_components", "outbox",
	"sync_configurations", "sync_plans", "sync_parts", "sync_attempts", "pending_sync", "pending_sync_roots", "recovery_decisions",
}

const sqliteLinkBootstrapEmptySQL = "SELECT 'actor_generations',EXISTS(SELECT 1 FROM actor_generations LIMIT 1) UNION ALL SELECT 'actors',EXISTS(SELECT 1 FROM actors LIMIT 1) UNION ALL SELECT 'actor_uncertainties',EXISTS(SELECT 1 FROM actor_uncertainties LIMIT 1) UNION ALL SELECT 'host_sessions',EXISTS(SELECT 1 FROM host_sessions LIMIT 1) UNION ALL SELECT 'host_turns',EXISTS(SELECT 1 FROM host_turns LIMIT 1) UNION ALL SELECT 'host_tools',EXISTS(SELECT 1 FROM host_tools LIMIT 1) UNION ALL SELECT 'host_receipts',EXISTS(SELECT 1 FROM host_receipts LIMIT 1) UNION ALL SELECT 'epochs',EXISTS(SELECT 1 FROM epochs LIMIT 1) UNION ALL SELECT 'segments',EXISTS(SELECT 1 FROM segments LIMIT 1) UNION ALL SELECT 'segment_events',EXISTS(SELECT 1 FROM segment_events LIMIT 1) UNION ALL SELECT 'uncertainties',EXISTS(SELECT 1 FROM uncertainties LIMIT 1) UNION ALL SELECT 'uncertainty_evidence',EXISTS(SELECT 1 FROM uncertainty_evidence LIMIT 1) UNION ALL SELECT 'event_receipts',EXISTS(SELECT 1 FROM event_receipts LIMIT 1) UNION ALL SELECT 'event_receipt_uncertainties',EXISTS(SELECT 1 FROM event_receipt_uncertainties LIMIT 1) UNION ALL SELECT 'event_ids',EXISTS(SELECT 1 FROM event_ids LIMIT 1) UNION ALL SELECT 'union_frontier',EXISTS(SELECT 1 FROM union_frontier LIMIT 1) UNION ALL SELECT 'component_segments',EXISTS(SELECT 1 FROM component_segments LIMIT 1) UNION ALL SELECT 'pending_finalization',EXISTS(SELECT 1 FROM pending_finalization LIMIT 1) UNION ALL SELECT 'intervals',EXISTS(SELECT 1 FROM intervals LIMIT 1) UNION ALL SELECT 'interval_segments',EXISTS(SELECT 1 FROM interval_segments LIMIT 1) UNION ALL SELECT 'interval_components',EXISTS(SELECT 1 FROM interval_components LIMIT 1) UNION ALL SELECT 'outbox',EXISTS(SELECT 1 FROM outbox LIMIT 1) UNION ALL SELECT 'sync_configurations',EXISTS(SELECT 1 FROM sync_configurations LIMIT 1) UNION ALL SELECT 'sync_plans',EXISTS(SELECT 1 FROM sync_plans LIMIT 1) UNION ALL SELECT 'sync_parts',EXISTS(SELECT 1 FROM sync_parts LIMIT 1) UNION ALL SELECT 'sync_attempts',EXISTS(SELECT 1 FROM sync_attempts LIMIT 1) UNION ALL SELECT 'pending_sync',EXISTS(SELECT 1 FROM pending_sync LIMIT 1) UNION ALL SELECT 'pending_sync_roots',EXISTS(SELECT 1 FROM pending_sync_roots LIMIT 1) UNION ALL SELECT 'recovery_decisions',EXISTS(SELECT 1 FROM recovery_decisions LIMIT 1)"
