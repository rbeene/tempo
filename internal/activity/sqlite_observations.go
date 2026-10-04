//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"reflect"
	"sort"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// These three finite observation intents share one receipt and safety-write
// boundary. No provider, binding discovery or host policy participates.
type sqliteObservationInput struct {
	ID, Operation, Fingerprint, Reason string
	Actor                              ActorRef
	Host                               *HostObservation
}

func (s *Service) observeSourceSQLite(ctx context.Context, in SourceObservation) (MutationResult, error) {
	if !validRef(in.Actor) || !sqliteObservationReason(in.Reason) {
		return MutationResult{}, failure("validation")
	}
	return s.observeSQLite(ctx, sqliteObservationInput{ID: in.RequestID, Operation: "activity.observe_source", Fingerprint: mutationFingerprint("activity.observe_source", in), Reason: in.Reason, Actor: in.Actor})
}

func (s *Service) observeClockSQLite(ctx context.Context, in ClockObservation) (MutationResult, error) {
	return s.observeSQLite(ctx, sqliteObservationInput{ID: in.RequestID, Operation: "activity.observe_clock", Fingerprint: mutationFingerprint("activity.observe_clock", in)})
}

func (s *Service) observeHostSQLite(ctx context.Context, in HostObservation) (MutationResult, error) {
	if !hostSource(in.Source) || !safeIdentifier(in.SessionID, 256) || !safeIdentifier(in.TurnID, 256) || in.AgentID != "" && !safeIdentifier(in.AgentID, 128) || !sqliteObservationReason(in.Reason) {
		return MutationResult{}, failure("validation")
	}
	return s.observeSQLite(ctx, sqliteObservationInput{ID: in.RequestID, Operation: "activity.observe_host", Fingerprint: mutationFingerprint("activity.observe_host", in), Reason: in.Reason, Host: &in})
}

func sqliteObservationReason(reason string) bool {
	return reason == "source_lost" || reason == "ordering_unavailable" || reason == "restart_unknown"
}

func (s *Service) observeSQLite(ctx context.Context, in sqliteObservationInput) (MutationResult, error) {
	if !validUUID(in.ID) {
		return MutationResult{}, failure("validation")
	}
	a, _, err := s.sqliteSyncConfigAdmission(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	facts, known, found, err := sqliteObservationRead(ctx, a, in)
	if err != nil {
		return MutationResult{}, err
	}
	if !found {
		return MutationResult{}, requiredRecovery("binding")
	}
	var sample ClockSample
	if !known && facts.Sample {
		if err = sqliteCaptureDeadline(ctx, a); err != nil {
			return MutationResult{}, err
		}
		// Both injected and native samples are taken after checked ordinary
		// release. Unavailable evidence still belongs to a successful observation.
		value, _ := s.sample()
		sample = sqliteCaptureCopySample(value)
	}
	receipt, err := sqliteObservationWrite(ctx, a, in, facts, known, sample)
	if err != nil {
		return MutationResult{}, err
	}
	// A historical safe error is returned only after its nonce fence and owner
	// cleanup succeeded; it must not be reclassified as a new storage failure.
	return recoveryReceipt(receipt)
}

// Every returned owner, including an inspection failure owner, belongs to the
// caller's immediate checked-cleanup defer. This read never initializes a store.
func sqliteOpenObservationRead(ctx context.Context, a sqliteCaptureAdmission) (c *sqliteio.Conn, tx *sqliteio.Tx, meta sqliteStoreMeta, found bool, err error) {
	if err = sqliteCaptureDeadline(ctx, a); err != nil {
		return
	}
	var kind sqliteio.LinkInspection
	c, kind, err = sqliteio.InspectForLink(ctx, a.Directory, a.StateBasename, a.DatabaseBasename, a.AcquireDeadline)
	if err != nil {
		return
	}
	if kind == sqliteio.LinkAbsent || kind == sqliteio.LinkPristine {
		if c != nil {
			err = failure("state_corrupt")
		}
		return
	}
	if kind != sqliteio.LinkWAL || c == nil {
		err = failure("state_corrupt")
		return
	}
	tx, err = c.Begin(ctx, sqliteio.Read)
	if err == nil {
		meta, found, err = sqliteReadLinkSchema(tx, a.StateBasename, a.DatabaseBasename)
	}
	return
}

func sqliteObservationReceipt(tx *sqliteio.Tx, meta sqliteStoreMeta, in sqliteObservationInput) (mutationRequest, bool, error) {
	row, found, err := sqliteReadMutationRequestLocal(tx, meta.ComputerID, in.ID, meta.Revision)
	if err != nil || !found {
		return mutationRequest{}, false, err
	}
	if row.Value.Operation != in.Operation || row.Value.Fingerprint != in.Fingerprint {
		return mutationRequest{}, false, failure("request_conflict")
	}
	if row.Value.MutationResult == nil && row.Value.Error == nil {
		return mutationRequest{}, false, failure("state_corrupt")
	}
	return row.Value, true, nil
}

func sqliteObservationRead(ctx context.Context, a sqliteCaptureAdmission, in sqliteObservationInput) (facts sqliteObservationFacts, known, found bool, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, false, in.ID, nil)
			facts, known, found = sqliteObservationFacts{}, false, false
		}
	}()
	var meta sqliteStoreMeta
	c, tx, meta, found, err = sqliteOpenObservationRead(ctx, a)
	if err != nil || !found {
		return
	}
	_, known, err = sqliteObservationReceipt(tx, meta, in)
	if err == nil && !known {
		facts, err = sqliteReadObservationFacts(tx, meta, in)
	}
	return
}

func sqliteObservationWrite(ctx context.Context, a sqliteCaptureAdmission, in sqliteObservationInput, prepared sqliteObservationFacts, known bool, sample ClockSample) (receipt mutationRequest, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	uncertain := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, uncertain, in.ID, nil)
			receipt = mutationRequest{}
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return receipt, err
	}
	if !found {
		return receipt, failure("state_busy")
	}
	// A concurrent exact receipt wins before current facts, pressure or a
	// prepared refusal. A known historical receipt cannot silently disappear.
	receipt, found, err = sqliteObservationReceipt(tx, meta, in)
	if err != nil {
		return receipt, err
	}
	if known && !found {
		return receipt, failure("state_corrupt")
	}
	known = known || found
	var facts sqliteObservationFacts
	if !found {
		facts, err = sqliteReadObservationFacts(tx, meta, in)
		if err != nil {
			return receipt, err
		}
		same, compareErr := sqliteSameObservationFacts(facts, prepared)
		if compareErr != nil {
			return receipt, compareErr
		}
		if !same {
			return receipt, failure("state_busy")
		}
		if facts.RefusalCode != "" {
			return receipt, failure(facts.RefusalCode)
		}
	}
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return receipt, err
	}
	after := meta
	if !found {
		if meta.Revision == "18446744073709551615" {
			return receipt, failure("validation")
		}
		after.Revision = bump(meta.Revision)
		m := sqliteCaptureMutation{tx: tx, meta: meta, nextRevision: after.Revision}
		m.transition.Finalization = sqliteEmptyFinalizationSelection()
		ids, applyErr := sqliteApplyObservation(&m, in, facts, sample)
		if applyErr != nil {
			return receipt, applyErr
		}
		if err = sqliteValidateSelectedCaptureDependencies(tx, meta.ComputerID, after.Revision, m.transition.Dependencies); err != nil {
			return receipt, err
		}
		if err = sqliteValidateSelectedFinalization(tx, meta.ComputerID, after.Revision, m.transition.Finalization); err != nil {
			return receipt, err
		}
		if err = sqliteFinalizationAdd(&after.LogicalBytes, m.transition.Delta, nil); err != nil {
			return receipt, err
		}
		result := MutationResult{ContractVersion: 1, SnapshotRevision: after.Revision, RequestID: in.ID, Changed: len(ids) > 0, AffectedIDs: ids}
		receipt = mutationRequest{Operation: in.Operation, Fingerprint: in.Fingerprint, MutationResult: &result}
		delta, writeErr := sqliteWriteMutationRequest(tx, meta.ComputerID, nil, sqliteMutationRequestRow{ID: in.ID, Value: receipt})
		if err = sqliteFinalizationAdd(&after.LogicalBytes, delta, writeErr); err != nil {
			return receipt, err
		}
	}
	after.DurabilityNonce, err = sqliteNextNonce(meta.DurabilityNonce[:])
	if err == nil {
		err = sqliteUpdateMeta(tx, meta, after)
	}
	if err != nil {
		return receipt, err
	}
	observed, err := sqliteReadMeta(tx, a.StateBasename, a.DatabaseBasename)
	if err != nil {
		return receipt, err
	}
	if !reflect.DeepEqual(observed, after) {
		return receipt, failure("state_corrupt")
	}
	readback, found, err := sqliteObservationReceipt(tx, after, in)
	if err != nil {
		return receipt, err
	}
	if !found || !reflect.DeepEqual(readback, receipt) {
		return receipt, failure("state_corrupt")
	}
	receipt = readback
	outcome, commitErr := tx.Commit()
	if commitErr != nil || outcome != sqliteio.Committed {
		uncertain = outcome == sqliteio.Unknown || outcome == sqliteio.Committed
		if commitErr == nil {
			commitErr = failure("state_corrupt")
		}
		return receipt, commitErr
	}
	if err = c.CloseDurably(ctx); err != nil {
		uncertain = true
		return receipt, err
	}
	return receipt, nil
}

func (s *Service) hostReceiptsSQLite(ctx context.Context, filter HostReceiptFilter) (result HostReceiptList, err error) {
	if filter.Source != "" && !hostSource(filter.Source) || filter.SessionID != "" && !safeIdentifier(filter.SessionID, 256) {
		return result, failure("validation")
	}
	a, _, err := s.sqliteSyncConfigAdmission(ctx)
	if err != nil {
		return result, err
	}
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if cleanup != nil || owner != nil {
			// Reuse the existing ordinary-read owner wrapper: this operation has
			// no mutation request whose durability could be acknowledged.
			err = &sqliteBindingReadCleanupError{public: failure("state_corrupt"), evidence: errors.Join(err, cleanup), owner: owner}
			result = HostReceiptList{}
		} else if err != nil {
			err = sqliteCaptureError(err)
			result = HostReceiptList{}
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenObservationRead(ctx, a)
	if err != nil {
		return result, err
	}
	result = HostReceiptList{ContractVersion: 1, SnapshotRevision: "0", Receipts: []HostReceipt{}}
	if !found {
		return result, nil
	}
	result.SnapshotRevision = meta.Revision
	// Finish the cursor before materializing each complete, owned typed receipt.
	stmt, err := tx.Prepare("SELECT receipt_key FROM host_receipts WHERE (?='' OR source=?) AND (?='' OR native_session=?) ORDER BY receipt_key", sqliteio.Text(filter.Source), sqliteio.Text(filter.Source), sqliteio.Text(filter.SessionID), sqliteio.Text(filter.SessionID))
	if err != nil {
		return result, err
	}
	keys, err := sqliteCaptureIDs(stmt, true)
	if err != nil {
		return result, err
	}
	for _, key := range keys {
		row, exists, readErr := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, key)
		if readErr != nil {
			return result, readErr
		}
		value := row.Record.Result
		if !exists || filter.Source != "" && value.Source != filter.Source || filter.SessionID != "" && value.SessionID != filter.SessionID {
			return result, failure("state_corrupt")
		}
		result.Receipts = append(result.Receipts, value)
	}
	sort.Slice(result.Receipts, func(i, j int) bool {
		a, b := result.Receipts[i], result.Receipts[j]
		x, _ := counter(a.SnapshotRevision)
		y, _ := counter(b.SnapshotRevision)
		if x != y {
			return x < y
		}
		return a.ID < b.ID
	})
	return result, nil
}
