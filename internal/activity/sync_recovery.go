package activity

// Only the holder of the separate run lock may call this function. A process
// cannot infer orphanhood from status or a state-lock snapshot alone.
func syncRecoverPending(st *state) error {
	for id, r := range st.Requests {
		if r.PendingSync == nil {
			continue
		}
		pending := r.PendingSync
		if pending.Resolve != nil || (pending.Reconcile != nil && !pending.EffectCommitted) {
			saveMutation(st, id, mutationRequest{Operation: r.Operation, Fingerprint: r.Fingerprint, Error: failure("local_write_unknown")})
			continue
		}
		if pending.Run == nil && pending.Reconcile == nil {
			return failure("state_corrupt")
		}
		for _, root := range pending.RootIDs {
			o := syncItem(st, root)
			if o.RunRequestID == nil {
				continue
			}
			if *o.RunRequestID != id || o.Plan == nil {
				return failure("state_corrupt")
			}
			for i := range o.Plan.Parts {
				part := &o.Plan.Parts[i]
				if part.State == "submitting" {
					part.State = "unknown"
					part.FailureCategory = syncString("interrupted_submission")
					if len(part.Attempts) == 0 {
						return failure("state_corrupt")
					}
					a := &part.Attempts[len(part.Attempts)-1]
					a.State = "unknown"
					a.FailureCategory = part.FailureCategory
				}
			}
			syncFinishItem(&o)
			st.Outbox[o.Interval.ID] = o
		}
		result := syncRunResult(st, id, "interrupted", pending.RootIDs)
		if pending.Reconcile != nil {
			result.AttemptedIDs = []string{}
		}
		saveMutation(st, id, mutationRequest{Operation: r.Operation, Fingerprint: r.Fingerprint, SyncRun: &result})
	}
	return nil
}
