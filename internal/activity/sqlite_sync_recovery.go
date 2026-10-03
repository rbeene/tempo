//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"reflect"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Recovery is called only by the verified run-guard owner. The old complete
// graph is certified before any pending projection or submitting fact changes.
func sqliteSyncRecoverPending(tx *sqliteio.Tx, meta sqliteStoreMeta, after *sqliteStoreMeta, roots, requests *[]string) error {
	old, found, err := sqliteSyncPendingRequest(tx, meta.ComputerID, meta.Revision)
	if err != nil || !found {
		return err
	}
	pending := old.Value.PendingSync
	*roots = append(*roots, pending.RootIDs...)
	*requests = append(*requests, old.ID)
	next := sqliteMutationRequestRow{ID: old.ID, Value: mutationRequest{Operation: old.Value.Operation, Fingerprint: old.Value.Fingerprint}}
	if pending.Resolve != nil || pending.Reconcile != nil && !pending.EffectCommitted {
		next.Value.Error = failure("local_write_unknown")
	} else {
		if pending.Run == nil && pending.Reconcile == nil {
			return failure("state_corrupt")
		}
		for _, root := range pending.RootIDs {
			o, found, err := sqliteReadSyncItem(tx, meta.ComputerID, root, meta.Revision)
			if err != nil {
				return err
			}
			if !found {
				return failure("state_corrupt")
			}
			if o.RunRequestID == nil {
				continue
			}
			if *o.RunRequestID != old.ID || o.Plan == nil {
				return failure("state_corrupt")
			}
			if o.Revision == "18446744073709551615" {
				return failure("validation")
			}
			n := sqliteSyncNowCopyItem(o)
			for i := range n.Plan.Parts {
				p := &n.Plan.Parts[i]
				if p.State != "submitting" {
					continue
				}
				if len(p.Attempts) == 0 {
					return failure("state_corrupt")
				}
				p.State, p.FailureCategory = "unknown", syncString("interrupted_submission")
				a := &p.Attempts[len(p.Attempts)-1]
				a.State, a.FailureCategory = p.State, p.FailureCategory
			}
			syncFinishItem(&n)
			if err = sqliteSyncNowWriteItem(tx, meta.ComputerID, o, n, &after.LogicalBytes, requests); err != nil {
				return err
			}
		}
		result, err := sqliteSyncRunResult(tx, meta.ComputerID, old.ID, after.Revision, "interrupted", pending.RootIDs, pending.Reconcile != nil)
		if err != nil {
			return err
		}
		next.Value.SyncRun = &result
	}
	delta, err := sqliteWriteMutationRequest(tx, meta.ComputerID, &old, next)
	return sqliteFinalizationAdd(&after.LogicalBytes, delta, err)
}

func sqliteSyncNowCopyItem(o OutboxItem) OutboxItem {
	if o.Plan == nil {
		return o
	}
	p := *o.Plan
	p.Parts = append([]SyncPart{}, o.Plan.Parts...)
	for i := range p.Parts {
		p.Parts[i].Attempts = append([]SyncAttempt{}, p.Parts[i].Attempts...)
	}
	o.Plan = &p
	return o
}

func sqliteSyncNowOutbox(o OutboxItem) sqliteOutboxLocalRow {
	return sqliteOutboxLocalRow{IntervalID: o.Interval.ID, ID: o.ID, Revision: o.Revision, State: o.State, Correlation: o.Correlation, EntryID: o.EntryID, FailureCategory: o.FailureCategory, RetryRequestID: o.RetryRequestID, RunRequestID: o.RunRequestID, PlanPresent: o.Plan != nil}
}

func sqliteSyncNowPart(interval string, index int, p SyncPart) sqliteSyncPartLocalRow {
	return sqliteSyncPartLocalRow{IntervalID: interval, Ordinal: int64(index), ID: p.ID, SpentDate: p.SpentDate, DurationNS: p.DurationNS, Start: p.Start, End: p.End, PlannedHours: p.PlannedHours, PlannedDurationNS: p.PlannedDurationNS, PlannedResidualNS: p.PlannedResidualNS, StartedTime: p.StartedTime, EndedTime: p.EndedTime, Correlation: p.Correlation, Notes: p.Notes, State: p.State, EntryID: p.EntryID, FailureCategory: p.FailureCategory, ReturnedHours: p.ReturnedHours, RoundedHours: p.RoundedHours, ConfirmedDurationNS: p.ConfirmedDurationNS, ProviderDeltaNS: p.ProviderDeltaNS, TotalResidualNS: p.TotalResidualNS, Attachment: p.Attachment}
}

// Persist only changed literal rows of two complete owned items. Plan creation
// and replacement have their own explicit unattempted-child recipe.
func sqliteSyncNowWriteItem(tx *sqliteio.Tx, computer string, before, after OutboxItem, logical *int64, requests *[]string) error {
	if before.ID != after.ID || !reflect.DeepEqual(before.Interval, after.Interval) || (before.Plan == nil) != (after.Plan == nil) {
		return failure("state_corrupt")
	}
	for _, id := range []*string{before.RunRequestID, before.RetryRequestID, after.RunRequestID, after.RetryRequestID} {
		if id != nil {
			*requests = append(*requests, *id)
		}
	}
	if before.Plan != nil {
		if !reflect.DeepEqual(before.Plan.Configuration, after.Plan.Configuration) || before.Plan.CompanySource != after.Plan.CompanySource || len(before.Plan.Parts) != len(after.Plan.Parts) {
			return failure("state_corrupt")
		}
		for i, p := range before.Plan.Parts {
			n := after.Plan.Parts[i]
			oldPart, newPart := sqliteSyncNowPart(before.Interval.ID, i, p), sqliteSyncNowPart(after.Interval.ID, i, n)
			if !reflect.DeepEqual(oldPart, newPart) {
				for _, a := range []*SyncAttachment{p.Attachment, n.Attachment} {
					if a != nil {
						*requests = append(*requests, a.RequestID)
					}
				}
				delta, err := sqliteWriteSyncPartLocal(tx, computer, &oldPart, newPart)
				if err = sqliteFinalizationAdd(logical, delta, err); err != nil {
					return err
				}
			}
			if len(n.Attempts) < len(p.Attempts) || len(n.Attempts) > len(p.Attempts)+1 {
				return failure("state_corrupt")
			}
			for j, a := range p.Attempts {
				if reflect.DeepEqual(a, n.Attempts[j]) {
					continue
				}
				if j != len(p.Attempts)-1 || len(n.Attempts) != len(p.Attempts) {
					return failure("state_corrupt")
				}
				*requests = append(*requests, a.RequestID, n.Attempts[j].RequestID)
				old := sqliteSyncAttemptLocalRow{IntervalID: before.Interval.ID, PartOrdinal: int64(i), Ordinal: int64(j), Value: a}
				next := old
				next.Value = n.Attempts[j]
				delta, err := sqliteUpdateSyncAttempt(tx, computer, old, next)
				if err = sqliteFinalizationAdd(logical, delta, err); err != nil {
					return err
				}
			}
			if len(n.Attempts) > len(p.Attempts) {
				j := len(p.Attempts)
				a := n.Attempts[j]
				*requests = append(*requests, a.RequestID)
				delta, err := sqliteAppendSyncAttempt(tx, computer, sqliteSyncAttemptLocalRow{IntervalID: before.Interval.ID, PartOrdinal: int64(i), Ordinal: int64(j), Value: a})
				if err = sqliteFinalizationAdd(logical, delta, err); err != nil {
					return err
				}
			}
		}
	}
	old, next := sqliteSyncNowOutbox(before), sqliteSyncNowOutbox(after)
	if reflect.DeepEqual(old, next) {
		return nil
	}
	delta, err := sqliteUpdateOutboxLocal(tx, computer, old, next)
	return sqliteFinalizationAdd(logical, delta, err)
}

// Every Now writer uses one metadata CAS and the existing durable-close proof.
// This helper accepts fixed owned rows, never a callback or arbitrary SQL.
func sqliteSyncNowCommit(ctx context.Context, a sqliteCaptureAdmission, c *sqliteio.Conn, tx *sqliteio.Tx, before, after sqliteStoreMeta, roots, requests []string) (uncertain bool, err error) {
	if err = sqliteValidateSelectedSync(tx, before.ComputerID, after.Revision, roots, requests); err != nil {
		return false, err
	}
	after.DurabilityNonce, err = sqliteNextNonce(before.DurabilityNonce[:])
	if err == nil {
		err = sqliteUpdateMeta(tx, before, after)
	}
	if err != nil {
		return false, err
	}
	observed, err := sqliteReadMeta(tx, a.StateBasename, a.DatabaseBasename)
	if err != nil {
		return false, err
	}
	if !reflect.DeepEqual(observed, after) {
		return false, failure("state_corrupt")
	}
	outcome, err := tx.Commit()
	if err != nil || outcome != sqliteio.Committed {
		if err == nil {
			err = failure("state_corrupt")
		}
		return outcome == sqliteio.Unknown || outcome == sqliteio.Committed, err
	}
	if err = c.CloseDurably(ctx); err != nil {
		return true, err
	}
	return false, nil
}
