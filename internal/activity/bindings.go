package activity

import (
	"context"
	"sort"
)

func (s *Service) Link(ctx context.Context, in LinkInput, d LinkDependencies) (BindingResult, error) {
	if s.store.sqliteOnly {
		return s.linkSQLite(ctx, in, d)
	}
	if err := validateLinkInput(in); err != nil {
		return BindingResult{}, err
	}
	path, err := lexicalPath(in.Path)
	if err != nil {
		return BindingResult{}, err
	}
	in.Path = path
	fingerprint := mutationFingerprint("bindings.link", in)
	if old, ok, err := s.replayMutation(ctx, in.RequestID, "bindings.link", fingerprint); err != nil {
		return BindingResult{}, err
	} else if ok {
		return *old.BindingResult, nil
	}
	loc, err := DiscoverLocation(ctx, in.Path)
	if err != nil {
		return BindingResult{}, err
	}
	before, _, err := s.store.read(ctx)
	if err != nil {
		return BindingResult{}, err
	}
	var saved *BindingSnapshot
	if r, ok := exactRecord(before, loc); ok {
		if in.IfRevision != "" && in.IfRevision != r.Snapshot.Revision {
			return BindingResult{}, failure("revision_conflict")
		}
		saved = &r.Snapshot
	} else if in.IfRevision != "" {
		return BindingResult{}, failure("revision_conflict")
	}
	attr, err := assignedAttribution(ctx, in, d, saved)
	if err != nil {
		return BindingResult{}, err
	}
	if err := checkLocation(ctx, in.Path, loc); err != nil {
		return BindingResult{}, err
	}
	var result BindingResult
	err = s.store.update(ctx, func(st *state) (bool, error) {
		if old, ok, e := mutationLookup(st, in.RequestID, "bindings.link", fingerprint); e != nil {
			return false, e
		} else if ok {
			result = *old.BindingResult
			return false, nil
		}
		if err := checkLocation(ctx, in.Path, loc); err != nil {
			return false, err
		}
		r, exists := exactRecord(st, loc)
		changed := !exists || r.Snapshot.Attribution != attr
		if exists {
			if in.IfRevision != "" && in.IfRevision != r.Snapshot.Revision || in.IfRevision == "" && changed {
				return false, failure("revision_conflict")
			}
			if changed && len(bindingView(st, r).AttachedActors) > 0 {
				return false, failure("binding_in_use")
			}
		} else if in.IfRevision != "" {
			return false, failure("revision_conflict")
		}
		for id, b := range st.Bindings {
			if exists && id == r.Snapshot.ID {
				continue
			}
			if b.Attribution.AccountID == attr.AccountID && b.Attribution.ProjectID == attr.ProjectID && b.Attribution != attr {
				return false, attributionConflict(b.Attribution, attr)
			}
		}
		if st.ComputerID == "" {
			st.ComputerID = newID()
		}
		if st.BindingRecords == nil {
			st.BindingRecords = map[string]bindingRecord{}
		}
		if !exists {
			r = bindingRecord{Snapshot: BindingSnapshot{ID: newID(), Revision: "1", Attribution: attr}, Kind: loc.Kind, Locator: loc.Locator}
		} else if changed {
			if revisionExhausted(r.Snapshot.Revision) {
				return false, failure("validation")
			}
			r.Snapshot.Revision = bump(r.Snapshot.Revision)
			r.Snapshot.Attribution = attr
		}
		st.Bindings[r.Snapshot.ID] = r.Snapshot
		st.BindingRecords[r.Snapshot.ID] = r
		result = BindingResult{ContractVersion: 1, SnapshotRevision: bump(st.Revision), RequestID: in.RequestID, Changed: changed, Binding: bindingView(st, r)}
		saveMutation(st, in.RequestID, mutationRequest{Operation: "bindings.link", Fingerprint: fingerprint, BindingResult: &result})
		return true, nil
	})
	if err != nil {
		return BindingResult{}, requestError(err, in.RequestID)
	}
	return result, nil
}
func checkLocation(ctx context.Context, path string, expected Location) error {
	now, err := DiscoverLocation(ctx, path)
	if err != nil {
		return err
	}
	if now != expected {
		return failure("binding_unavailable")
	}
	return nil
}
func revisionExhausted(revision string) bool {
	n, ok := counter(revision)
	return !ok || n == ^uint64(0)
}
func (s *Service) ListBindings(ctx context.Context) (BindingList, error) {
	if s.store.sqliteOnly {
		return s.listBindingsSQLite(ctx)
	}
	st, _, err := s.store.read(ctx)
	if err != nil {
		return BindingList{}, err
	}
	result := BindingList{ContractVersion: 1, SnapshotRevision: st.Revision, Bindings: []Binding{}}
	for _, r := range st.BindingRecords {
		if !r.Deleted {
			result.Bindings = append(result.Bindings, bindingView(st, r))
		}
	}
	sort.Slice(result.Bindings, func(i, j int) bool { return result.Bindings[i].ID < result.Bindings[j].ID })
	return result, nil
}
func (s *Service) ShowBinding(ctx context.Context, in ShowBindingInput) (BindingList, error) {
	if in.BindingID != "" && (!validUUID(in.BindingID) || in.Path != "") {
		return BindingList{}, failure("validation")
	}
	if s.store.sqliteOnly {
		return s.showBindingSQLite(ctx, in)
	}
	st, _, err := s.store.read(ctx)
	if err != nil {
		return BindingList{}, err
	}
	var r bindingRecord
	var ok bool
	if in.BindingID != "" {
		r, ok = st.BindingRecords[in.BindingID]
		ok = ok && !r.Deleted
		if ok {
			err = bindingAvailable(ctx, r)
		}
	} else {
		var loc Location
		loc, err = DiscoverLocation(ctx, in.Path)
		if err == nil {
			r, ok = recordForLocation(st, loc)
		}
	}
	if err != nil {
		return BindingList{}, err
	}
	if !ok {
		return BindingList{}, failure("not_found")
	}
	return BindingList{ContractVersion: 1, SnapshotRevision: st.Revision, Bindings: []Binding{bindingView(st, r)}}, nil
}
func validateBindingMutation(id, revision, request string, confirmed bool) error {
	if !validUUID(id) || !validUUID(request) {
		return failure("validation")
	}
	if n, ok := counter(revision); !ok || n == 0 {
		return failure("validation")
	}
	if !confirmed {
		return failure("confirmation_required")
	}
	return nil
}
func mutableRecord(st *state, id, revision string) (bindingRecord, error) {
	r, ok := st.BindingRecords[id]
	if !ok || r.Deleted {
		return r, failure("not_found")
	}
	if r.Snapshot.Revision != revision {
		return r, failure("revision_conflict")
	}
	if len(bindingView(st, r).AttachedActors) > 0 {
		return r, failure("binding_in_use")
	}
	if revisionExhausted(revision) {
		return r, failure("validation")
	}
	return r, nil
}
func (s *Service) Unlink(ctx context.Context, in UnlinkInput) (MutationResult, error) {
	if err := validateBindingMutation(in.BindingID, in.IfRevision, in.RequestID, in.Confirmed); err != nil {
		return MutationResult{}, err
	}
	fp := mutationFingerprint("bindings.unlink", in)
	if old, ok, err := s.replayMutation(ctx, in.RequestID, "bindings.unlink", fp); err != nil {
		return MutationResult{}, err
	} else if ok {
		return *old.MutationResult, nil
	}
	// Validate the absent/missing case before acquiring a creating transaction.
	before, _, err := s.store.read(ctx)
	if err != nil {
		return MutationResult{}, err
	}
	if _, err = mutableRecord(before, in.BindingID, in.IfRevision); err != nil {
		return MutationResult{}, err
	}
	var result MutationResult
	err = s.store.update(ctx, func(st *state) (bool, error) {
		if old, ok, e := mutationLookup(st, in.RequestID, "bindings.unlink", fp); e != nil {
			return false, e
		} else if ok {
			result = *old.MutationResult
			return false, nil
		}
		r, err := mutableRecord(st, in.BindingID, in.IfRevision)
		if err != nil {
			return false, err
		}
		r.Deleted = true
		r.Snapshot.Revision = bump(r.Snapshot.Revision)
		st.BindingRecords[in.BindingID] = r
		delete(st.Bindings, in.BindingID)
		revision := r.Snapshot.Revision
		result = MutationResult{ContractVersion: 1, SnapshotRevision: bump(st.Revision), RequestID: in.RequestID, Changed: true, AffectedIDs: []string{in.BindingID}, EntityRevision: &revision}
		saveMutation(st, in.RequestID, mutationRequest{Operation: "bindings.unlink", Fingerprint: fp, MutationResult: &result})
		return true, nil
	})
	if err != nil {
		return MutationResult{}, requestError(err, in.RequestID)
	}
	return result, nil
}
func (s *Service) RepairBinding(ctx context.Context, in RepairBindingInput) (BindingResult, error) {
	if err := validateBindingMutation(in.BindingID, in.IfRevision, in.RequestID, in.Confirmed); err != nil {
		return BindingResult{}, err
	}
	if in.Path == "" {
		return BindingResult{}, required("path")
	}
	path, err := lexicalPath(in.Path)
	if err != nil {
		return BindingResult{}, err
	}
	in.Path = path
	fp := mutationFingerprint("bindings.repair", in)
	if old, ok, err := s.replayMutation(ctx, in.RequestID, "bindings.repair", fp); err != nil {
		return BindingResult{}, err
	} else if ok {
		return *old.BindingResult, nil
	}
	before, _, err := s.store.read(ctx)
	if err != nil {
		return BindingResult{}, err
	}
	r, err := mutableRecord(before, in.BindingID, in.IfRevision)
	if err != nil {
		return BindingResult{}, err
	}
	loc, err := DiscoverLocation(ctx, in.Path)
	if err != nil {
		return BindingResult{}, err
	}
	if loc.Kind != r.Kind {
		return BindingResult{}, failure("validation")
	}
	var result BindingResult
	err = s.store.update(ctx, func(st *state) (bool, error) {
		if old, ok, e := mutationLookup(st, in.RequestID, "bindings.repair", fp); e != nil {
			return false, e
		} else if ok {
			result = *old.BindingResult
			return false, nil
		}
		r, err := mutableRecord(st, in.BindingID, in.IfRevision)
		if err != nil {
			return false, err
		}
		if err := checkLocation(ctx, in.Path, loc); err != nil {
			return false, err
		}
		if loc.Kind != r.Kind {
			return false, failure("validation")
		}
		if other, ok := exactRecord(st, loc); ok && other.Snapshot.ID != r.Snapshot.ID {
			return false, failure("revision_conflict")
		}
		changed := r.Locator != loc.Locator
		if changed {
			r.Locator = loc.Locator
			r.Snapshot.Revision = bump(r.Snapshot.Revision)
		}
		st.BindingRecords[in.BindingID] = r
		st.Bindings[in.BindingID] = r.Snapshot
		result = BindingResult{ContractVersion: 1, SnapshotRevision: bump(st.Revision), RequestID: in.RequestID, Changed: changed, Binding: bindingView(st, r)}
		saveMutation(st, in.RequestID, mutationRequest{Operation: "bindings.repair", Fingerprint: fp, BindingResult: &result})
		return true, nil
	})
	if err != nil {
		return BindingResult{}, requestError(err, in.RequestID)
	}
	return result, nil
}
