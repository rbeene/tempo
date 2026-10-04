package activity

import (
	"context"
	"net/url"
	"time"
)

func (s *Service) SyncReconcile(ctx context.Context, in SyncReconcileInput, d SyncDependencies) (SyncRun, error) {
	if s.store.sqliteOnly {
		return s.syncReconcileSQLite(ctx, in, d)
	}
	if !validUUID(in.RequestID) || (in.OutboxID != "" && !validUUID(in.OutboxID)) || in.Limit < 0 || in.Limit > 100 {
		return SyncRun{}, failure("validation")
	}
	if in.Limit == 0 {
		in.Limit = 20
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	fp := mutationFingerprint("sync.reconcile", in)
	prior, ok, e := s.replayMutation(ctx, in.RequestID, "sync.reconcile", fp)
	if e != nil {
		return SyncRun{}, e
	}
	if ok && prior.Error != nil {
		return SyncRun{}, requestError(prior.Error, in.RequestID)
	}
	if ok && prior.SyncRun != nil {
		return *prior.SyncRun, nil
	}
	_, exists, e := s.store.read(ctx)
	if e != nil {
		return SyncRun{}, e
	}
	if !exists {
		return SyncRun{}, syncRequired("binding")
	}
	lock, e := s.acquireSyncLock(ctx)
	if e != nil {
		return SyncRun{}, e
	}
	defer lock.close()
	roots := []string{}
	var replay *SyncRun
	var replayErr *Error
	e = s.store.update(ctx, func(st *state) (bool, error) {
		if e := syncRecoverPending(st); e != nil {
			return false, e
		}
		r, ok, e := mutationLookup(st, in.RequestID, "sync.reconcile", fp)
		if e != nil {
			return false, e
		}
		if ok {
			replay = r.SyncRun
			replayErr = r.Error
			return true, nil
		}
		if in.OutboxID != "" && syncItem(st, in.OutboxID).ID == "" {
			return false, failure("not_found")
		}
		for _, o := range syncSortedItems(st) {
			if in.OutboxID != "" && o.ID != in.OutboxID {
				continue
			}
			if o.Plan != nil && (o.State == "unknown" || o.State == "needs_attention") {
				roots = append(roots, o.ID)
				if len(roots) == in.Limit {
					break
				}
			}
		}
		saveMutation(st, in.RequestID, mutationRequest{Operation: "sync.reconcile", Fingerprint: fp, PendingSync: &syncReservation{Reconcile: &in, RootIDs: roots, SnapshotRevision: bump(st.Revision)}})
		return true, nil
	})
	if e != nil {
		return SyncRun{}, requestError(e, in.RequestID)
	}
	if replayErr != nil {
		return SyncRun{}, requestError(replayErr, in.RequestID)
	}
	if replay != nil {
		return *replay, nil
	}
	for _, root := range roots {
		st, _, e := s.store.read(ctx)
		if e != nil {
			return SyncRun{}, e
		}
		o := syncItem(st, root)
		p, e := syncProvider(ctx, d, o.Interval.Attribution.AccountID)
		if e != nil {
			continue
		}
		u, e := syncIdentity(ctx, p, o.Interval.Attribution.AccountID)
		if e != nil || bindingID(u) != o.Interval.Attribution.UserID {
			continue
		}
		for index, part := range o.Plan.Parts {
			if part.State == "synced" || len(part.Attempts) == 0 {
				continue
			}
			rows, e := p.List(ctx, "/time_entries", url.Values{"user_id": {o.Interval.Attribution.UserID}, "external_reference_id": {part.Correlation}})
			if e != nil {
				continue
			}
			// Any extra candidate, including a conflicting row, blocks resolution.
			if len(rows) != 1 {
				if len(rows) > 1 {
					if e = s.syncReconcileBlock(ctx, in.RequestID, root, "correlation_collision"); e != nil {
						return SyncRun{}, requestError(e, in.RequestID)
					}
				}
				continue
			}
			result := part
			syncApplyResponse(o, &result, rows[0])
			if result.State == "unknown" {
				if e = s.syncReconcileBlock(ctx, in.RequestID, root, "response_mismatch"); e != nil {
					return SyncRun{}, requestError(e, in.RequestID)
				}
				continue
			}
			if e = lock.verify(); e != nil {
				return SyncRun{}, e
			}
			e = s.store.update(ctx, func(st *state) (bool, error) {
				current := syncItem(st, root)
				a := &result.Attempts[len(result.Attempts)-1]
				a.State = result.State
				a.EntryID = result.EntryID
				a.FailureCategory = result.FailureCategory
				current.Plan.Parts[index] = result
				syncFinishItem(&current)
				st.Outbox[current.Interval.ID] = current
				receipt := st.Requests[in.RequestID]
				receipt.PendingSync.EffectCommitted = true
				st.Requests[in.RequestID] = receipt
				return true, nil
			})
			if e != nil {
				return SyncRun{}, requestError(e, in.RequestID)
			}
		}
	}
	if e = s.syncBarrier("sync_before_complete", in.RequestID); e != nil {
		return SyncRun{}, e
	}
	var result SyncRun
	e = s.store.update(ctx, func(st *state) (bool, error) {
		result = syncRunResult(st, in.RequestID, "complete", roots)
		result.AttemptedIDs = []string{}
		saveMutation(st, in.RequestID, mutationRequest{Operation: "sync.reconcile", Fingerprint: fp, SyncRun: &result})
		return true, nil
	})
	if e != nil {
		return SyncRun{}, requestError(e, in.RequestID)
	}
	return result, nil
}

func (s *Service) syncReconcileBlock(ctx context.Context, request, root, category string) error {
	return s.store.update(ctx, func(st *state) (bool, error) {
		o := syncItem(st, root)
		o.State = "needs_attention"
		o.FailureCategory = syncString(category)
		o.Revision = bump(o.Revision)
		st.Outbox[o.Interval.ID] = o
		r := st.Requests[request]
		r.PendingSync.EffectCommitted = true
		st.Requests[request] = r
		return true, nil
	})
}
