//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"reflect"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Two concrete recovery intents share the existing typed receipt and commit
// boundary. There are no provider callbacks or State projections in this file.
type sqliteSyncRecoveryIntent struct {
	ID, Operation, Fingerprint string
	Reconcile                  *SyncReconcileInput
	Resolve                    *SyncResolveInput
}

func sqliteSyncRecoveryReceipt(tx *sqliteio.Tx, meta sqliteStoreMeta, in sqliteSyncRecoveryIntent) (sqliteMutationRequestRow, bool, error) {
	row, found, err := sqliteReadMutationRequestLocal(tx, meta.ComputerID, in.ID, meta.Revision)
	if err != nil || !found {
		return sqliteMutationRequestRow{}, false, err
	}
	if row.Value.Operation != in.Operation || row.Value.Fingerprint != in.Fingerprint {
		return sqliteMutationRequestRow{}, false, failure("request_conflict")
	}
	if row.Value.Error != nil {
		return row, true, nil
	}
	if in.Reconcile != nil {
		if row.Value.SyncRun == nil && (row.Value.PendingSync == nil || row.Value.PendingSync.Reconcile == nil) {
			return sqliteMutationRequestRow{}, false, failure("state_corrupt")
		}
	} else if row.Value.MutationResult == nil && (row.Value.PendingSync == nil || row.Value.PendingSync.Resolve == nil) {
		return sqliteMutationRequestRow{}, false, failure("state_corrupt")
	}
	return row, true, nil
}

// An empty root is the initial receipt-only observation. A nonempty root is a
// selected reconcile snapshot owned by the matching pending request.
func sqliteSyncRecoveryRead(ctx context.Context, a sqliteCaptureAdmission, in sqliteSyncRecoveryIntent, root string) (terminal bool, item OutboxItem, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	known := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, false, in.ID, nil)
			terminal, item = false, OutboxItem{}
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Read)
	if err != nil {
		return false, item, err
	}
	if !found {
		return false, item, syncRequired("binding")
	}
	row, found, err := sqliteSyncRecoveryReceipt(tx, meta, in)
	if err != nil {
		return false, item, err
	}
	known = found
	if root == "" {
		return found && row.Value.PendingSync == nil, item, nil
	}
	if !found || in.Reconcile == nil || row.Value.PendingSync == nil || !sqliteSyncGraphContains(row.Value.PendingSync.RootIDs, root) {
		return false, item, failure("state_corrupt")
	}
	item, found, err = sqliteReadSyncItem(tx, meta.ComputerID, root, meta.Revision)
	if err != nil {
		return false, item, err
	}
	if !found || item.Plan == nil {
		return false, item, failure("state_corrupt")
	}
	return false, item, nil
}

func sqliteSyncRecoveryReplay(ctx context.Context, a sqliteCaptureAdmission, in sqliteSyncRecoveryIntent) (result mutationRequest, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	uncertain := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, true, uncertain, in.ID, nil)
			result = mutationRequest{}
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("state_corrupt")
	}
	row, found, err := sqliteSyncRecoveryReceipt(tx, meta, in)
	if err != nil {
		return result, err
	}
	if !found || row.Value.PendingSync != nil {
		return result, failure("state_corrupt")
	}
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return result, err
	}
	uncertain, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, meta, nil, []string{in.ID})
	if err != nil {
		return result, err
	}
	return row.Value, nil
}

func sqliteSyncReconcileRoots(tx *sqliteio.Tx, computer, ceiling string, in SyncReconcileInput) ([]string, error) {
	query := "SELECT o.interval_id,o.id,i.start_sec,i.start_nsec FROM outbox AS o LEFT JOIN intervals AS i ON i.interval_id=o.interval_id WHERE o.plan_present=1 AND o.state IN ('unknown','needs_attention') ORDER BY i.start_sec,i.start_nsec,o.id LIMIT ?"
	args := []sqliteio.Value{sqliteio.Integer(int64(in.Limit))}
	if in.OutboxID != "" {
		query = "SELECT o.interval_id,o.id,i.start_sec,i.start_nsec FROM outbox AS o LEFT JOIN intervals AS i ON i.interval_id=o.interval_id WHERE o.id=? AND o.plan_present=1 AND o.state IN ('unknown','needs_attention') ORDER BY i.start_sec,i.start_nsec,o.id LIMIT ?"
		args = []sqliteio.Value{sqliteio.Text(in.OutboxID), sqliteio.Integer(int64(in.Limit))}
	}
	s, err := tx.Prepare(query, args...)
	if err != nil {
		return nil, err
	}
	tuples, err := sqliteSyncNowTuples(s, in.Limit)
	if err != nil {
		return nil, err
	}
	roots := make([]string, 0, len(tuples))
	for _, tuple := range tuples {
		o, found, err := sqliteReadSyncItem(tx, computer, tuple.root, ceiling)
		if err != nil {
			return nil, err
		}
		if !found || o.Interval.ID != tuple.interval || o.Interval.Start.Unix() != tuple.sec || int64(o.Interval.Start.Nanosecond()) != tuple.nsec || o.Plan == nil || o.State != "unknown" && o.State != "needs_attention" || in.OutboxID != "" && o.ID != in.OutboxID {
			return nil, failure("state_corrupt")
		}
		roots = append(roots, o.ID)
	}
	return roots, nil
}

// Only the run-guard owner enters reservation/recovery. Terminal replay wins
// before orphan recovery and current root revision. A recovered receipt retains
// its original intent; a new request cannot silently broaden old targets.
func sqliteSyncRecoveryReserve(ctx context.Context, a sqliteCaptureAdmission, in sqliteSyncRecoveryIntent) (roots []string, item OutboxItem, replay *mutationRequest, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	known, uncertain := false, false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, known, uncertain, in.ID, nil)
			roots, item, replay = nil, OutboxItem{}, nil
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return nil, item, nil, err
	}
	if !found {
		return nil, item, nil, syncRequired("binding")
	}
	prior, found, err := sqliteSyncRecoveryReceipt(tx, meta, in)
	if err != nil {
		return nil, item, nil, err
	}
	known = found
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return nil, item, nil, err
	}
	if found && prior.Value.PendingSync == nil {
		uncertain, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, meta, nil, []string{in.ID})
		if err != nil {
			return nil, item, nil, err
		}
		return []string{}, item, &prior.Value, nil
	}
	if meta.Revision == "18446744073709551615" {
		return nil, item, nil, failure("validation")
	}
	after := meta
	after.Revision = bump(meta.Revision)
	changedRoots, requests := []string{}, []string{in.ID}
	if err = sqliteSyncRecoverPending(tx, meta, &after, &changedRoots, &requests); err != nil {
		return nil, item, nil, err
	}
	prior, found, err = sqliteSyncRecoveryReceipt(tx, after, in)
	if err != nil {
		return nil, item, nil, err
	}
	if found {
		known = true
		if prior.Value.PendingSync != nil {
			return nil, item, nil, failure("state_corrupt")
		}
		replay = &prior.Value
	} else {
		roots = []string{}
		next := sqliteMutationRequestRow{ID: in.ID, Value: mutationRequest{Operation: in.Operation, Fingerprint: in.Fingerprint}}
		if in.Reconcile != nil {
			if in.Reconcile.OutboxID != "" {
				_, exists, readErr := sqliteReadSyncItem(tx, meta.ComputerID, in.Reconcile.OutboxID, after.Revision)
				if readErr != nil {
					return nil, item, nil, readErr
				}
				if !exists {
					return nil, item, nil, failure("not_found")
				}
			}
			roots, err = sqliteSyncReconcileRoots(tx, meta.ComputerID, after.Revision, *in.Reconcile)
			if err != nil {
				return nil, item, nil, err
			}
			next.Value.PendingSync = &syncReservation{Reconcile: in.Reconcile, RootIDs: roots, SnapshotRevision: after.Revision}
		} else {
			item, found, err = sqliteReadSyncItem(tx, meta.ComputerID, in.Resolve.OutboxID, after.Revision)
			if err != nil {
				return nil, item, nil, err
			}
			if !found {
				return nil, item, nil, failure("not_found")
			}
			if item.Revision != in.Resolve.IfRevision {
				return nil, item, nil, failure("revision_conflict")
			}
			if item.Plan == nil {
				return nil, item, nil, failure("invalid_transition")
			}
			roots = []string{item.ID}
			if in.Resolve.RetryRejected {
				rejected := false
				for _, part := range item.Plan.Parts {
					switch part.State {
					case "synced", "queued":
					case "rejected":
						rejected = true
					default:
						return nil, item, nil, failure("invalid_transition")
					}
				}
				if !rejected {
					return nil, item, nil, failure("invalid_transition")
				}
				if item.Revision == "18446744073709551615" {
					return nil, item, nil, failure("validation")
				}
				n := sqliteSyncNowCopyItem(item)
				n.State, n.RetryRequestID, n.FailureCategory, n.Revision = "queued", syncString(in.ID), nil, bump(item.Revision)
				if err = sqliteSyncNowWriteItem(tx, meta.ComputerID, item, n, &after.LogicalBytes, &requests); err != nil {
					return nil, item, nil, err
				}
				item = n
				result := MutationResult{ContractVersion: 1, SnapshotRevision: after.Revision, RequestID: in.ID, Changed: true, AffectedIDs: roots, EntityRevision: syncString(item.Revision)}
				next.Value.MutationResult = &result
				replay = &next.Value
			} else {
				next.Value.PendingSync = &syncReservation{Resolve: in.Resolve, RootIDs: roots, SnapshotRevision: after.Revision}
			}
		}
		delta, writeErr := sqliteWriteMutationRequest(tx, meta.ComputerID, nil, next)
		if err = sqliteFinalizationAdd(&after.LogicalBytes, delta, writeErr); err != nil {
			return nil, item, nil, err
		}
		changedRoots = append(changedRoots, roots...)
	}
	uncertain, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, after, changedRoots, requests)
	return roots, item, replay, err
}

// Persist one checked reconcile acknowledgement/block or one manual attachment.
// Provider data is already materialized into the selected part; the complete
// before item and reservation are re-read before any staged write.
func sqliteSyncRecoverySavePart(ctx context.Context, a sqliteCaptureAdmission, in sqliteSyncRecoveryIntent, expected OutboxItem, index int, acknowledged SyncPart, block string) (item OutboxItem, result *MutationResult, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	uncertain := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, false, uncertain, in.ID, nil)
			item, result = OutboxItem{}, nil
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return item, nil, err
	}
	if !found {
		return item, nil, failure("state_corrupt")
	}
	old, found, err := sqliteSyncRecoveryReceipt(tx, meta, in)
	if err != nil {
		return item, nil, err
	}
	if !found || old.Value.PendingSync == nil || !sqliteSyncGraphContains(old.Value.PendingSync.RootIDs, expected.ID) {
		return item, nil, failure("state_corrupt")
	}
	current, found, err := sqliteReadSyncItem(tx, meta.ComputerID, expected.ID, meta.Revision)
	if err != nil {
		return item, nil, err
	}
	if !found {
		return item, nil, failure("state_corrupt")
	}
	if in.Resolve != nil && current.Revision != in.Resolve.IfRevision {
		return item, nil, failure("revision_conflict")
	}
	if !reflect.DeepEqual(current, expected) {
		return item, nil, failure("revision_conflict")
	}
	if current.Plan == nil || index < 0 || index >= len(current.Plan.Parts) || block != "" && (in.Reconcile == nil || block != "correlation_collision" && block != "response_mismatch") {
		return item, nil, failure("state_corrupt")
	}
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return item, nil, err
	}
	if meta.Revision == "18446744073709551615" || current.Revision == "18446744073709551615" {
		return item, nil, failure("validation")
	}
	after := meta
	after.Revision = bump(meta.Revision)
	item = sqliteSyncNowCopyItem(current)
	if block != "" {
		item.State, item.FailureCategory, item.Revision = "needs_attention", syncString(block), bump(current.Revision)
	} else {
		part := acknowledged
		part.Attempts = append([]SyncAttempt{}, acknowledged.Attempts...)
		if in.Resolve != nil {
			part.Attachment = &SyncAttachment{RequestID: in.ID, EntryID: in.Resolve.EntryID}
		} else if len(part.Attempts) == 0 {
			return OutboxItem{}, nil, failure("state_corrupt")
		}
		if len(part.Attempts) > 0 {
			attempt := &part.Attempts[len(part.Attempts)-1]
			attempt.State, attempt.EntryID, attempt.FailureCategory = part.State, part.EntryID, part.FailureCategory
		}
		item.Plan.Parts[index] = part
		syncFinishItem(&item)
	}
	requests := []string{in.ID}
	if err = sqliteSyncNowWriteItem(tx, meta.ComputerID, current, item, &after.LogicalBytes, &requests); err != nil {
		return item, nil, err
	}
	next := sqliteMutationRequestRow{ID: in.ID, Value: mutationRequest{Operation: in.Operation, Fingerprint: in.Fingerprint}}
	if in.Reconcile != nil {
		pending := *old.Value.PendingSync
		pending.EffectCommitted = true
		next.Value.PendingSync = &pending
	} else {
		result = &MutationResult{ContractVersion: 1, SnapshotRevision: after.Revision, RequestID: in.ID, Changed: true, AffectedIDs: []string{item.ID}, EntityRevision: syncString(item.Revision)}
		next.Value.MutationResult = result
	}
	delta, writeErr := sqliteWriteMutationRequest(tx, meta.ComputerID, &old, next)
	if err = sqliteFinalizationAdd(&after.LogicalBytes, delta, writeErr); err != nil {
		return item, nil, err
	}
	uncertain, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, after, []string{item.ID}, requests)
	return item, result, err
}

func sqliteSyncReconcileComplete(ctx context.Context, a sqliteCaptureAdmission, in sqliteSyncRecoveryIntent, roots []string) (result SyncRun, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	uncertain := false
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, false, uncertain, in.ID, nil)
			result = SyncRun{}
		}
	}()
	var meta sqliteStoreMeta
	var found bool
	c, tx, meta, found, err = sqliteOpenCapture(ctx, a, sqliteio.Write)
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("state_corrupt")
	}
	old, found, err := sqliteSyncRecoveryReceipt(tx, meta, in)
	if err != nil {
		return result, err
	}
	if !found || old.Value.PendingSync == nil || !reflect.DeepEqual(old.Value.PendingSync.RootIDs, roots) {
		return result, failure("state_corrupt")
	}
	if err = sqliteValidateSelectedSync(tx, meta.ComputerID, meta.Revision, roots, []string{in.ID}); err != nil {
		return result, err
	}
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return result, err
	}
	if meta.Revision == "18446744073709551615" {
		return result, failure("validation")
	}
	after := meta
	after.Revision = bump(meta.Revision)
	result, err = sqliteSyncRunResult(tx, meta.ComputerID, in.ID, after.Revision, "complete", roots, true)
	if err != nil {
		return result, err
	}
	next := sqliteMutationRequestRow{ID: in.ID, Value: mutationRequest{Operation: in.Operation, Fingerprint: in.Fingerprint, SyncRun: &result}}
	delta, writeErr := sqliteWriteMutationRequest(tx, meta.ComputerID, &old, next)
	if err = sqliteFinalizationAdd(&after.LogicalBytes, delta, writeErr); err != nil {
		return result, err
	}
	uncertain, err = sqliteSyncNowCommit(ctx, a, c, tx, meta, after, roots, []string{in.ID})
	return result, err
}
