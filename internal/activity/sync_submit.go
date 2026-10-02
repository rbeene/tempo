package activity

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/rbeene/tempo/internal/harvest"
)

func syncRunResult(st *state, id, stateName string, roots []string) SyncRun {
	r := SyncRun{ContractVersion: 1, RequestID: id, SnapshotRevision: bump(st.Revision), State: stateName, AttemptedIDs: []string{}, ResolvedIDs: []string{}, BlockedIDs: []string{}}
	for _, root := range roots {
		o := syncItem(st, root)
		attempted := false
		if o.Plan != nil {
			for _, p := range o.Plan.Parts {
				for _, attempt := range p.Attempts {
					if attempt.RequestID == id {
						attempted = true
					}
				}
			}
		}
		if attempted {
			r.AttemptedIDs = append(r.AttemptedIDs, root)
		}
		if o.State == "synced" {
			r.ResolvedIDs = append(r.ResolvedIDs, root)
		} else {
			r.BlockedIDs = append(r.BlockedIDs, root)
		}
	}
	for _, o := range st.Outbox {
		if o.State != "synced" {
			r.RemainingCount++
		}
	}
	return r
}
func syncFinishItem(o *OutboxItem) {
	o.RunRequestID = nil
	o.RetryRequestID = nil
	o.EntryID = nil
	o.FailureCategory = nil
	if o.Plan == nil {
		return
	}
	o.State = "synced"
	for _, p := range o.Plan.Parts {
		if p.EntryID != nil && len(o.Plan.Parts) == 1 {
			o.EntryID = p.EntryID
		}
		if p.State != "synced" {
			o.State = p.State
			o.FailureCategory = p.FailureCategory
		}
	}
	if len(o.Plan.Parts) > 1 && o.State != "synced" {
		o.State = "needs_attention"
		o.FailureCategory = syncString("partial_submission_interrupted")
	}
	o.Revision = bump(o.Revision)
}
func (s *Service) syncBarrier(stage, id string) error {
	if s.store.fault(stage) != nil {
		return requestError(failure("local_write_unknown"), id)
	}
	return nil
}
func (s *Service) SyncNow(ctx context.Context, in SyncRunInput, d SyncDependencies) (SyncRun, error) {
	if !validUUID(in.RequestID) || in.Limit < 0 || in.Limit > 100 {
		return SyncRun{}, failure("validation")
	}
	if in.Limit == 0 {
		in.Limit = 20
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	fp := mutationFingerprint("sync.now", in)
	old, ok, e := s.replayMutation(ctx, in.RequestID, "sync.now", fp)
	if e != nil {
		return SyncRun{}, e
	}
	if ok && old.SyncRun != nil {
		return *old.SyncRun, nil
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
	e = s.store.update(ctx, func(st *state) (bool, error) {
		if e := syncRecoverPending(st); e != nil {
			return false, e
		}
		prior, found, e := mutationLookup(st, in.RequestID, "sync.now", fp)
		if e != nil {
			return false, e
		}
		if found {
			replay = prior.SyncRun
			return true, nil
		}
		if st.SyncEnabled {
			for _, o := range syncSortedItems(st) {
				if syncEligible(o) {
					roots = append(roots, o.ID)
					if len(roots) == in.Limit {
						break
					}
				}
			}
		}
		saveMutation(st, in.RequestID, mutationRequest{Operation: "sync.now", Fingerprint: fp, PendingSync: &syncReservation{Run: &in, RootIDs: roots, SnapshotRevision: bump(st.Revision)}})
		return true, nil
	})
	if e != nil {
		return SyncRun{}, requestError(e, in.RequestID)
	}
	if replay != nil {
		return *replay, nil
	}
	for _, root := range roots {
		if e = s.syncSubmitRoot(ctx, lock, in.RequestID, root, d); e != nil {
			return SyncRun{}, requestError(e, in.RequestID)
		}
	}
	if e = s.syncBarrier("sync_before_complete", in.RequestID); e != nil {
		return SyncRun{}, e
	}
	var result SyncRun
	e = s.store.update(ctx, func(st *state) (bool, error) {
		for _, root := range roots {
			o := syncItem(st, root)
			if o.RunRequestID != nil && *o.RunRequestID == in.RequestID {
				syncFinishItem(&o)
				st.Outbox[o.Interval.ID] = o
			}
		}
		result = syncRunResult(st, in.RequestID, "complete", roots)
		saveMutation(st, in.RequestID, mutationRequest{Operation: "sync.now", Fingerprint: fp, SyncRun: &result})
		return true, nil
	})
	if e != nil {
		return SyncRun{}, requestError(e, in.RequestID)
	}
	return result, nil
}
func (s *Service) syncBlock(ctx context.Context, root, category string) error {
	return s.store.update(ctx, func(st *state) (bool, error) {
		o := syncItem(st, root)
		o.State = "needs_attention"
		switch category {
		case "network", "auth", "keychain", "rate_limit", "api":
			o.State = "queued"
		}
		if o.RetryRequestID != nil {
			o.State = "queued"
		}
		o.FailureCategory = syncString(category)
		o.Revision = bump(o.Revision)
		st.Outbox[o.Interval.ID] = o
		return true, nil
	})
}
func (s *Service) syncSubmitRoot(ctx context.Context, lock *syncRunLock, request, root string, d SyncDependencies) error {
	st, _, e := s.store.read(ctx)
	if e != nil {
		return e
	}
	o := syncItem(st, root)
	if !st.SyncEnabled {
		return nil
	}

	cfg, ok := st.SyncConfigurations[syncConfigKey(o.Interval.Attribution.AccountID, o.Interval.Attribution.UserID)]
	frozen := o.RetryRequestID != nil && o.Plan != nil
	if frozen {
		cfg = o.Plan.Configuration
		ok = true
	}
	if !ok {
		return s.syncBlock(ctx, root, "configuration_required")
	}
	p, e := syncProvider(ctx, d, o.Interval.Attribution.AccountID)
	if e != nil {
		return s.syncBlock(ctx, root, syncSafeError(e).Code)
	}
	source, e := syncPreflight(ctx, p, o.Interval.Attribution, cfg)
	if e != nil {
		return s.syncBlock(ctx, root, syncSafeError(e).Code)
	}
	plan := o.Plan
	if !frozen {
		plan, e = syncBuildPlan(o, cfg, source)
		if e != nil {
			return s.syncBlock(ctx, root, "representation")
		}
		e = s.store.update(ctx, func(st *state) (bool, error) {
			o = syncItem(st, root)
			current := st.SyncConfigurations[syncConfigKey(cfg.AccountID, cfg.UserID)]
			if current.Revision != cfg.Revision {
				return false, failure("revision_conflict")
			}
			o.Plan = plan
			o.Revision = bump(o.Revision)
			st.Outbox[o.Interval.ID] = o
			return true, nil
		})
		if e != nil {
			return e
		}
	}
	for index := range plan.Parts {
		if plan.Parts[index].State == "synced" {
			continue
		}
		if e = lock.verify(); e != nil {
			return e
		}
		claimed := false
		e = s.store.update(ctx, func(st *state) (bool, error) {
			o = syncItem(st, root)
			if !st.SyncEnabled {
				return false, nil
			}
			current := st.SyncConfigurations[syncConfigKey(cfg.AccountID, cfg.UserID)]
			if !frozen && index == 0 && current.Revision != cfg.Revision {
				return false, failure("revision_conflict")
			}
			part := &o.Plan.Parts[index]
			if !(part.State == "queued" && len(part.Attempts) == 0) && !(frozen && part.State == "rejected") {
				return false, failure("state_corrupt")
			}
			part.State = "submitting"
			part.FailureCategory = nil
			part.Attempts = append(part.Attempts, SyncAttempt{ID: newID(), RequestID: request, Number: strconv.Itoa(len(part.Attempts) + 1), State: "submitting"})
			o.State = "submitting"
			o.RunRequestID = syncString(request)
			o.Revision = bump(o.Revision)
			st.Outbox[o.Interval.ID] = o
			claimed = true
			return true, nil
		})
		if e != nil {
			return e
		}
		if !claimed {
			return nil
		}
		if e = s.syncBarrier("sync_after_claim", request); e != nil {
			return e
		}
		if ctx.Err() != nil {
			return failure("local_write_unknown")
		}
		row, writeErr := p.Create(ctx, "/time_entries", syncPayload(o, o.Plan.Parts[index]))
		// Failure to save an acknowledgement leaves the durable submitting claim.
		// Its future recovery is unknown; it is never automatically submitted again.
		e = s.store.update(ctx, func(st *state) (bool, error) {
			o = syncItem(st, root)
			part := &o.Plan.Parts[index]
			if writeErr != nil {
				part.State = "unknown"
				var he *harvest.Error
				if errors.As(writeErr, &he) && !he.Uncertain {
					switch he.Status {
					case 400, 401, 403, 404, 422, 429:
						part.State = "rejected"
					}
				}
				part.FailureCategory = syncString("remote_write")
			} else {
				syncApplyResponse(o, part, row)
			}
			a := &part.Attempts[len(part.Attempts)-1]
			a.State = part.State
			a.EntryID = part.EntryID
			a.FailureCategory = part.FailureCategory
			o.Revision = bump(o.Revision)
			st.Outbox[o.Interval.ID] = o
			return true, nil
		})
		if e != nil {
			return failure("local_write_unknown")
		}
		if e = s.syncBarrier("sync_after_part_saved", request); e != nil {
			return e
		}
		if o.Plan.Parts[index].State != "synced" {
			break
		}
	}
	return nil
}

func syncItem(st *state, id string) OutboxItem {
	for _, o := range st.Outbox {
		if o.ID == id {
			return o
		}
	}
	return OutboxItem{}
}

func syncEligible(o OutboxItem) bool {
	if o.State == "queued" {
		return true
	}
	if o.State != "needs_attention" || o.FailureCategory == nil || (*o.FailureCategory != "configuration_required" && *o.FailureCategory != "representation") {
		return false
	}
	if o.Plan != nil {
		for _, p := range o.Plan.Parts {
			if len(p.Attempts) > 0 {
				return false
			}
		}
	}
	return true
}
