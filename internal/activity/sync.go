package activity

import (
	"context"
	"net/url"
	"time"

	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/identity"
)

func (s *Service) SyncResolve(ctx context.Context, in SyncResolveInput, d SyncDependencies) (MutationResult, error) {
	if !validUUID(in.RequestID) || !validUUID(in.OutboxID) || (in.EntryID != "" && !identity.Valid(in.EntryID)) {
		return MutationResult{}, failure("validation")
	}
	if !in.Confirmed {
		return MutationResult{}, failure("confirmation_required")
	}
	if in.IfRevision == "" {
		return MutationResult{}, syncRequired("if_revision")
	}
	if _, ok := counter(in.IfRevision); !ok || (in.EntryID != "") == in.RetryRejected {
		return MutationResult{}, failure("validation")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	fp := mutationFingerprint("sync.resolve", in)
	prior, ok, e := s.replayMutation(ctx, in.RequestID, "sync.resolve", fp)
	if e != nil {
		return MutationResult{}, e
	}
	if ok && prior.MutationResult != nil {
		return *prior.MutationResult, nil
	}
	if ok && prior.Error != nil {
		return MutationResult{}, requestError(prior.Error, in.RequestID)
	}
	_, exists, e := s.store.read(ctx)
	if e != nil {
		return MutationResult{}, e
	}
	if !exists {
		return MutationResult{}, syncRequired("binding")
	}
	lock, e := s.acquireSyncLock(ctx)
	if e != nil {
		return MutationResult{}, e
	}
	defer lock.close()
	var item OutboxItem
	var replay *MutationResult
	var replayErr *Error
	e = s.store.update(ctx, func(st *state) (bool, error) {
		if e := syncRecoverPending(st); e != nil {
			return false, e
		}
		prior, ok, e := mutationLookup(st, in.RequestID, "sync.resolve", fp)
		if e != nil {
			return false, e
		}
		if ok {
			if prior.Error != nil {
				replayErr = prior.Error
				return true, nil
			}
			replay = prior.MutationResult
			return true, nil
		}
		item = syncItem(st, in.OutboxID)
		if item.ID == "" {
			return false, failure("not_found")
		}
		if item.Revision != in.IfRevision {
			return false, failure("revision_conflict")
		}
		if item.Plan == nil {
			return false, failure("invalid_transition")
		}
		if in.RetryRejected {
			rejected := false
			for _, part := range item.Plan.Parts {
				switch part.State {
				case "synced", "queued":
				case "rejected":
					rejected = true
				default:
					return false, failure("invalid_transition")
				}
			}
			if !rejected {
				return false, failure("invalid_transition")
			}
			item.State = "queued"
			item.RetryRequestID = syncString(in.RequestID)
			item.FailureCategory = nil
			item.Revision = bump(item.Revision)
			st.Outbox[item.Interval.ID] = item
			result := MutationResult{ContractVersion: 1, SnapshotRevision: bump(st.Revision), RequestID: in.RequestID, Changed: true, AffectedIDs: []string{item.ID}, EntityRevision: syncString(item.Revision)}
			saveMutation(st, in.RequestID, mutationRequest{Operation: "sync.resolve", Fingerprint: fp, MutationResult: &result})
			replay = &result
			return true, nil
		}
		saveMutation(st, in.RequestID, mutationRequest{Operation: "sync.resolve", Fingerprint: fp, PendingSync: &syncReservation{Resolve: &in, RootIDs: []string{item.ID}, SnapshotRevision: bump(st.Revision)}})
		return true, nil
	})
	if e != nil {
		return MutationResult{}, requestError(e, in.RequestID)
	}
	if replayErr != nil {
		return MutationResult{}, requestError(replayErr, in.RequestID)
	}
	if replay != nil {
		return *replay, nil
	}
	p, e := syncProvider(ctx, d, item.Interval.Attribution.AccountID)
	if e != nil {
		return MutationResult{}, e
	}
	u, e := syncIdentity(ctx, p, item.Interval.Attribution.AccountID)
	if e != nil {
		return MutationResult{}, e
	}
	if bindingID(u) != item.Interval.Attribution.UserID {
		e := failure("attribution_conflict")
		e.Details = map[string]any{"fields": []string{"user_id"}}
		return MutationResult{}, e
	}
	row, e := p.Get(ctx, "/time_entries/"+in.EntryID)
	if e != nil {
		return MutationResult{}, syncSafeError(e)
	}
	if bindingID(row) != in.EntryID {
		return MutationResult{}, failure("response")
	}
	index := -1
	for i, part := range item.Plan.Parts {
		if row["notes"] == part.Notes && part.State != "synced" {
			index = i
			break
		}
	}
	if index < 0 {
		return MutationResult{}, failure("conflict")
	}
	// Manual notes-only attachment requires a complete current-user scan, even
	// when the selected entry itself has the expected reference.
	rows, e := p.List(ctx, "/time_entries", url.Values{"user_id": {item.Interval.Attribution.UserID}})
	if e != nil {
		return MutationResult{}, syncSafeError(e)
	}
	matches := []harvest.Object{}
	for _, candidate := range rows {
		if candidate["notes"] == item.Plan.Parts[index].Notes || syncObject(candidate["external_reference"])["id"] == item.Plan.Parts[index].Correlation {
			matches = append(matches, candidate)
		}
	}
	if len(matches) != 1 || bindingID(matches[0]) != in.EntryID {
		return MutationResult{}, failure("conflict")
	}
	resultPart := item.Plan.Parts[index]
	for _, candidate := range []harvest.Object{row, matches[0]} {
		copy := harvest.Object{}
		for k, v := range candidate {
			copy[k] = v
		}
		if copy["external_reference"] == nil {
			copy["external_reference"] = harvest.Object{"id": resultPart.Correlation, "group_id": item.ID, "account_id": item.Interval.ComputerID}
		}
		syncApplyResponse(item, &resultPart, copy)
		if resultPart.State == "unknown" {
			return MutationResult{}, failure("conflict")
		}
	}
	if e = lock.verify(); e != nil {
		return MutationResult{}, e
	}
	var result MutationResult
	e = s.store.update(ctx, func(st *state) (bool, error) {
		o := syncItem(st, item.ID)
		if o.Revision != in.IfRevision {
			return false, failure("revision_conflict")
		}
		resultPart.Attachment = &SyncAttachment{RequestID: in.RequestID, EntryID: in.EntryID}
		if len(resultPart.Attempts) > 0 {
			a := &resultPart.Attempts[len(resultPart.Attempts)-1]
			a.State = resultPart.State
			a.EntryID = resultPart.EntryID
			a.FailureCategory = resultPart.FailureCategory
		}

		o.Plan.Parts[index] = resultPart
		syncFinishItem(&o)
		st.Outbox[o.Interval.ID] = o
		result = MutationResult{ContractVersion: 1, SnapshotRevision: bump(st.Revision), RequestID: in.RequestID, Changed: true, AffectedIDs: []string{o.ID}, EntityRevision: syncString(o.Revision)}
		saveMutation(st, in.RequestID, mutationRequest{Operation: "sync.resolve", Fingerprint: fp, MutationResult: &result})
		return true, nil
	})
	if e != nil {
		return MutationResult{}, requestError(e, in.RequestID)
	}
	return result, nil
}
