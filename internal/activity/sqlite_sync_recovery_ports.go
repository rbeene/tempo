//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"net/url"
	"time"

	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/identity"
)

func (s *Service) syncReconcileSQLite(ctx context.Context, in SyncReconcileInput, d SyncDependencies) (result SyncRun, err error) {
	if ctx == nil || !validUUID(in.RequestID) || in.OutboxID != "" && !validUUID(in.OutboxID) || in.Limit < 0 || in.Limit > 100 {
		return result, failure("validation")
	}
	if in.Limit == 0 {
		in.Limit = 20
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	a, budget, err := s.sqliteSyncConfigAdmission(ctx)
	if err != nil {
		return result, err
	}
	change := sqliteSyncRecoveryIntent{ID: in.RequestID, Operation: "sync.reconcile", Fingerprint: mutationFingerprint("sync.reconcile", in), Reconcile: &in}
	terminal, _, err := sqliteSyncRecoveryRead(ctx, a, change, "")
	if err != nil {
		return result, err
	}
	if terminal {
		receipt, e := sqliteSyncRecoveryReplay(ctx, a, change)
		if e != nil {
			return result, e
		}
		return sqliteSyncReconcileResult(receipt, in.RequestID)
	}
	var guard *sqliteSyncRunGuard
	effect := false
	defer func() {
		if closeErr := guard.Close(); closeErr != nil {
			retryErr := guard.Close()
			err = &sqliteSyncNowGuardError{public: requestError(failure("local_write_unknown"), in.RequestID), evidence: errors.Join(err, closeErr, retryErr), owner: guard}
		} else if err != nil && effect {
			err = sqliteLinkFailure(err, nil, nil, false, true, in.RequestID, nil)
		}
		if err != nil {
			result = SyncRun{}
		}
	}()
	guard, err = s.acquireSQLiteSyncLock(ctx, a.AcquireDeadline)
	if err != nil {
		return result, sqliteLinkFailure(err, nil, nil, false, false, in.RequestID, nil)
	}
	if err = guard.Verify(); err != nil {
		return result, sqliteLinkFailure(err, nil, nil, false, false, in.RequestID, nil)
	}
	roots, _, replay, err := sqliteSyncRecoveryReserve(ctx, a, change)
	if err != nil {
		return result, err
	}
	if replay != nil {
		return sqliteSyncReconcileResult(*replay, in.RequestID)
	}
	for _, root := range roots {
		a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
		_, item, readErr := sqliteSyncRecoveryRead(ctx, a, change, root)
		if readErr != nil {
			return result, readErr
		}
		if err = guard.Verify(); err != nil {
			return result, sqliteLinkFailure(err, nil, nil, false, effect, in.RequestID, nil)
		}
		// The checked snapshot is closed before provider construction and every
		// remote read. This port never calls Create, Update or Delete.
		p, providerErr := syncProvider(ctx, d, item.Interval.Attribution.AccountID)
		if providerErr != nil {
			continue
		}
		user, identityErr := syncIdentity(ctx, p, item.Interval.Attribution.AccountID)
		if identityErr != nil || bindingID(user) != item.Interval.Attribution.UserID {
			continue
		}
		for index := range item.Plan.Parts {
			part := item.Plan.Parts[index]
			if part.State == "synced" || len(part.Attempts) == 0 {
				continue
			}
			rows, listErr := p.List(ctx, "/time_entries", url.Values{"user_id": {item.Interval.Attribution.UserID}, "external_reference_id": {part.Correlation}})
			if listErr != nil || len(rows) == 0 {
				continue
			}
			category := ""
			if len(rows) > 1 {
				category = "correlation_collision"
			} else {
				syncApplyResponse(item, &part, rows[0])
				if part.State == "unknown" {
					category = "response_mismatch"
				}
			}
			if err = guard.Verify(); err != nil {
				return result, sqliteLinkFailure(err, nil, nil, false, effect, in.RequestID, nil)
			}
			a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
			item, _, err = sqliteSyncRecoverySavePart(ctx, a, change, item, index, part, category)
			if err != nil {
				return result, err
			}
			effect = true
		}
	}
	if err = s.syncBarrier("sync_before_complete", in.RequestID); err != nil {
		return result, err
	}
	if err = guard.Verify(); err != nil {
		return result, sqliteLinkFailure(err, nil, nil, false, effect, in.RequestID, nil)
	}
	a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
	return sqliteSyncReconcileComplete(ctx, a, change, roots)
}

func sqliteSyncReconcileResult(receipt mutationRequest, id string) (SyncRun, error) {
	if receipt.Error != nil {
		return SyncRun{}, requestError(receipt.Error, id)
	}
	if receipt.SyncRun == nil {
		return SyncRun{}, failure("state_corrupt")
	}
	return *receipt.SyncRun, nil
}

func sqliteSyncResolveResult(receipt mutationRequest, id string) (MutationResult, error) {
	if receipt.Error != nil {
		return MutationResult{}, requestError(receipt.Error, id)
	}
	if receipt.MutationResult == nil {
		return MutationResult{}, failure("state_corrupt")
	}
	return *receipt.MutationResult, nil
}

func (s *Service) syncResolveSQLite(ctx context.Context, in SyncResolveInput, d SyncDependencies) (result MutationResult, err error) {
	if ctx == nil || !validUUID(in.RequestID) || !validUUID(in.OutboxID) || in.EntryID != "" && !identity.Valid(in.EntryID) {
		return result, failure("validation")
	}
	if !in.Confirmed {
		return result, failure("confirmation_required")
	}
	if in.IfRevision == "" {
		return result, syncRequired("if_revision")
	}
	if _, ok := counter(in.IfRevision); !ok || (in.EntryID != "") == in.RetryRejected {
		return result, failure("validation")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	a, budget, err := s.sqliteSyncConfigAdmission(ctx)
	if err != nil {
		return result, err
	}
	change := sqliteSyncRecoveryIntent{ID: in.RequestID, Operation: "sync.resolve", Fingerprint: mutationFingerprint("sync.resolve", in), Resolve: &in}
	terminal, _, err := sqliteSyncRecoveryRead(ctx, a, change, "")
	if err != nil {
		return result, err
	}
	if terminal {
		receipt, e := sqliteSyncRecoveryReplay(ctx, a, change)
		if e != nil {
			return result, e
		}
		return sqliteSyncResolveResult(receipt, in.RequestID)
	}
	var guard *sqliteSyncRunGuard
	defer func() {
		if closeErr := guard.Close(); closeErr != nil {
			retryErr := guard.Close()
			err = &sqliteSyncNowGuardError{public: requestError(failure("local_write_unknown"), in.RequestID), evidence: errors.Join(err, closeErr, retryErr), owner: guard}
		}
		if err != nil {
			result = MutationResult{}
		}
	}()
	guard, err = s.acquireSQLiteSyncLock(ctx, a.AcquireDeadline)
	if err != nil {
		return result, sqliteLinkFailure(err, nil, nil, false, false, in.RequestID, nil)
	}
	if err = guard.Verify(); err != nil {
		return result, sqliteLinkFailure(err, nil, nil, false, false, in.RequestID, nil)
	}
	_, item, replay, err := sqliteSyncRecoveryReserve(ctx, a, change)
	if err != nil {
		return result, err
	}
	if replay != nil {
		return sqliteSyncResolveResult(*replay, in.RequestID)
	}
	if err = guard.Verify(); err != nil {
		return result, sqliteLinkFailure(err, nil, nil, false, false, in.RequestID, nil)
	}
	// Resolve's pending reservation and complete item are durable and released.
	// Its only remote effects are current-user identity and complete GETs.
	p, err := syncProvider(ctx, d, item.Interval.Attribution.AccountID)
	if err != nil {
		return result, err
	}
	user, err := syncIdentity(ctx, p, item.Interval.Attribution.AccountID)
	if err != nil {
		return result, err
	}
	if bindingID(user) != item.Interval.Attribution.UserID {
		e := failure("attribution_conflict")
		e.Details = map[string]any{"fields": []string{"user_id"}}
		return result, e
	}
	row, err := p.Get(ctx, "/time_entries/"+in.EntryID)
	if err != nil {
		return result, syncSafeError(err)
	}
	if bindingID(row) != in.EntryID {
		return result, failure("response")
	}
	index := -1
	for i, part := range item.Plan.Parts {
		if row["notes"] == part.Notes && part.State != "synced" {
			index = i
			break
		}
	}
	if index < 0 {
		return result, failure("conflict")
	}
	rows, err := p.List(ctx, "/time_entries", url.Values{"user_id": {item.Interval.Attribution.UserID}})
	if err != nil {
		return result, syncSafeError(err)
	}
	var match harvest.Object
	matches := 0
	for _, candidate := range rows {
		if candidate["notes"] == item.Plan.Parts[index].Notes || syncObject(candidate["external_reference"])["id"] == item.Plan.Parts[index].Correlation {
			matches++
			match = candidate
		}
	}
	if matches != 1 || bindingID(match) != in.EntryID {
		return result, failure("conflict")
	}
	part := item.Plan.Parts[index]
	for _, candidate := range []harvest.Object{row, match} {
		owned := harvest.Object{}
		for key, value := range candidate {
			owned[key] = value
		}
		if owned["external_reference"] == nil {
			owned["external_reference"] = harvest.Object{"id": part.Correlation, "group_id": item.ID, "account_id": item.Interval.ComputerID}
		}
		syncApplyResponse(item, &part, owned)
		if part.State == "unknown" {
			return result, failure("conflict")
		}
	}
	if err = guard.Verify(); err != nil {
		return result, sqliteLinkFailure(err, nil, nil, false, false, in.RequestID, nil)
	}
	a.AcquireDeadline = sqliteLinkDeadline(ctx, budget)
	_, receipt, err := sqliteSyncRecoverySavePart(ctx, a, change, item, index, part, "")
	if err != nil {
		return result, err
	}
	if receipt == nil {
		return result, failure("state_corrupt")
	}
	return *receipt, nil
}
