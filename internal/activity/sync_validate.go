package activity

import (
	"encoding/json"
	"github.com/rbeene/tempo/internal/identity"
	"math/big"
	"reflect"
	"strconv"
	"time"
)

func validSyncConfiguration(c SyncConfiguration) bool {
	if !identity.Valid(c.AccountID) || !identity.Valid(c.UserID) || !c.Declared || c.Source != "user_declared" || c.DeclaredAt.IsZero() || c.DeclaredAt.Location() != time.UTC {
		return false
	}
	n, ok := counter(c.Revision)
	if !ok || n == 0 {
		return false
	}
	clock := ""
	if c.Clock != nil {
		clock = *c.Clock
	}
	in := SyncConfigureInput{AccountID: c.AccountID, Mode: c.Mode, DurationPolicy: c.DurationPolicy, Clock: clock, IfRevision: c.Revision, RequestID: "00000000-0000-4000-8000-000000000000", Confirmed: true}
	return validateSyncConfig(in) == nil && c.PolicyVersion == c.DurationPolicy+"-v1"
}
func syncOperation(op string) bool {
	switch op {
	case "sync.configure", "sync.pause", "sync.resume", "sync.now", "sync.reconcile", "sync.resolve":
		return true
	}
	return false
}
func validSyncReceipt(st *state, id string, r mutationRequest, revision uint64) bool {
	if r.BindingResult != nil {
		return false
	}
	validRev := func(s string) bool { n, ok := counter(s); return ok && n > 0 && n <= revision }
	if r.Error != nil {
		return (r.Operation == "sync.resolve" || r.Operation == "sync.reconcile") && r.Error.Code == "local_write_unknown" && r.Error.Uncertain && r.Error.Details == nil
	}
	if r.PendingSync != nil {
		p := r.PendingSync
		if !validRev(p.SnapshotRevision) || p.RootIDs == nil {
			return false
		}
		count := 0
		if p.Run != nil {
			count++
			if r.Operation != "sync.now" || p.Run.RequestID != id || p.Run.Limit < 1 || p.Run.Limit > 100 || len(p.RootIDs) > p.Run.Limit || r.Fingerprint != mutationFingerprint(r.Operation, *p.Run) {
				return false
			}
		}
		if p.Reconcile != nil {
			count++
			if r.Operation != "sync.reconcile" || p.Reconcile.RequestID != id || p.Reconcile.Limit < 1 || p.Reconcile.Limit > 100 || len(p.RootIDs) > p.Reconcile.Limit || (p.Reconcile.OutboxID != "" && !validUUID(p.Reconcile.OutboxID)) || r.Fingerprint != mutationFingerprint(r.Operation, *p.Reconcile) {
				return false
			}
		}
		if p.Resolve != nil {
			count++
			if r.Operation != "sync.resolve" || p.Resolve.RequestID != id || !p.Resolve.Confirmed || !validUUID(p.Resolve.OutboxID) || len(p.RootIDs) != 1 || p.RootIDs[0] != p.Resolve.OutboxID || r.Fingerprint != mutationFingerprint(r.Operation, *p.Resolve) {
				return false
			}
		}
		if count != 1 {
			return false
		}
		seen := map[string]bool{}
		for _, rid := range p.RootIDs {
			if !validUUID(rid) || seen[rid] {
				return false
			}
			seen[rid] = true
			found := false
			for _, o := range st.Outbox {
				if o.ID == rid {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
	if r.SyncConfigurationResult != nil {
		v := r.SyncConfigurationResult
		if r.Operation != "sync.configure" || v.ContractVersion != 1 || v.RequestID != id || !validRev(v.SnapshotRevision) || !validSyncConfiguration(v.Configuration) {
			return false
		}
		current, ok := st.SyncConfigurations[syncConfigKey(v.Configuration.AccountID, v.Configuration.UserID)]
		a, _ := counter(current.Revision)
		b, _ := counter(v.Configuration.Revision)
		return ok && a >= b
	}
	if r.SyncRun != nil {
		v := r.SyncRun
		if r.Operation != "sync.now" && r.Operation != "sync.reconcile" {
			return false
		}
		if v.ContractVersion != 1 || v.RequestID != id || !validRev(v.SnapshotRevision) || (v.State != "complete" && v.State != "interrupted") || v.RemainingCount < 0 || v.AttemptedIDs == nil || v.ResolvedIDs == nil || v.BlockedIDs == nil {
			return false
		}
		for _, ids := range [][]string{v.AttemptedIDs, v.ResolvedIDs, v.BlockedIDs} {
			seen := map[string]bool{}
			for _, v := range ids {
				if !validUUID(v) || seen[v] || syncItem(st, v).ID == "" {
					return false
				}
				seen[v] = true
			}
		}
		return true
	}
	v := r.MutationResult
	if v == nil || (r.Operation != "sync.pause" && r.Operation != "sync.resume" && r.Operation != "sync.resolve") || v.ContractVersion != 1 || v.RequestID != id || !validRev(v.SnapshotRevision) || v.AffectedIDs == nil {
		return false
	}
	for _, id := range v.AffectedIDs {
		if !validUUID(id) {
			return false
		}
	}
	return true
}
func validSyncState(st *state) bool {
	pendingCount := 0
	for _, r := range st.Requests {
		if r.PendingSync != nil {
			pendingCount++
			if r.PendingSync.Run != nil && r.PendingSync.EffectCommitted {
				return false
			}
		}
	}
	if pendingCount > 1 {
		return false
	}
	for key, c := range st.SyncConfigurations {
		if key != syncConfigKey(c.AccountID, c.UserID) || !validSyncConfiguration(c) {
			return false
		}
	}
	ids := map[string]bool{}
	for _, o := range st.Outbox {
		if ids[o.ID] {
			return false
		}
		ids[o.ID] = true
		if o.EntryID != nil && !identity.Valid(*o.EntryID) {
			return false
		}
		if o.RetryRequestID != nil {
			r, ok := st.Requests[*o.RetryRequestID]
			if !validUUID(*o.RetryRequestID) || !ok || r.Operation != "sync.resolve" || r.MutationResult == nil || len(r.MutationResult.AffectedIDs) != 1 || r.MutationResult.AffectedIDs[0] != o.ID || (o.State != "queued" && o.State != "submitting") {
				return false
			}
		}
		if o.RunRequestID != nil {
			if !validUUID(*o.RunRequestID) || o.State != "submitting" || o.Plan == nil {
				return false
			}
			r, ok := st.Requests[*o.RunRequestID]
			if !ok || r.PendingSync == nil || r.PendingSync.Run == nil {
				return false
			}
			found := false
			for _, id := range r.PendingSync.RootIDs {
				if id == o.ID {
					found = true
				}
			}
			if !found {
				return false
			}
		} else if o.State == "submitting" {
			return false
		}
		if o.Plan == nil {
			if o.EntryID != nil || o.State == "synced" || o.State == "rejected" || o.State == "unknown" {
				return false
			}
			continue
		}
		plan := o.Plan
		if !validSyncConfiguration(plan.Configuration) || plan.Configuration.AccountID != o.Interval.Attribution.AccountID || plan.Configuration.UserID != o.Interval.Attribution.UserID || (plan.CompanySource != "company_verified" && plan.CompanySource != "user_declared_fallback") || len(plan.Parts) == 0 || len(plan.Parts) > 100 {
			return false
		}
		loc, e := time.LoadLocation(o.Interval.Attribution.Timezone)
		if e != nil {
			return false
		}
		cursor := o.Interval.Start
		attempted := false
		for _, p := range plan.Parts {
			if !validUUID(p.ID) || ids[p.ID] || p.Correlation != "tempo:v1:"+p.ID || p.Notes != syncMarker(o, plan.Configuration, p) || p.Attempts == nil {
				return false
			}
			ids[p.ID] = true
			if !p.Start.Equal(cursor) || p.Start.Location() != time.UTC || p.End.Location() != time.UTC || !p.End.After(p.Start) || p.End.After(o.Interval.End) || p.SpentDate != p.Start.In(loc).Format("2006-01-02") {
				return false
			}
			boundary := syncNextDay(p.Start, loc)
			if boundary.IsZero() || p.End.After(boundary) || (p.End.Before(o.Interval.End) && !p.End.Equal(boundary)) {
				return false
			}
			cursor = p.End
			n, ok := syncInt(p.DurationNS)
			if !ok || n <= 0 || n != int64(p.End.Sub(p.Start)) {
				return false
			}
			planned, ok := syncInt(p.PlannedDurationNS)
			if !ok || planned <= 0 {
				return false
			}
			residual, ok := syncInt(p.PlannedResidualNS)
			if !ok || new(big.Int).Sub(big.NewInt(n), big.NewInt(planned)).String() != strconv.FormatInt(residual, 10) {
				return false
			}
			_, hours := syncDecimal(json.Number(p.PlannedHours))
			if hours == nil || !syncRoundNS(hours).IsInt64() || syncRoundNS(hours).Int64() != planned {
				return false
			}
			if plan.Configuration.DurationPolicy == "nearest-hundredth-hour" {
				q := n / 36000000000
				if n%36000000000 >= 18000000000 {
					q++
				}
				if planned != q*36000000000 || hours.Cmp(new(big.Rat).SetFrac(big.NewInt(q), big.NewInt(100))) != 0 {
					return false
				}
			} else if planned != n {
				return false
			}
			if plan.Configuration.Mode == "duration" && (p.StartedTime != nil || p.EndedTime != nil) {
				return false
			}
			if plan.Configuration.Mode == "timestamp" {
				if !syncTimestampSafe(p.Start, p.End, loc) || p.StartedTime == nil || p.EndedTime == nil {
					return false
				}
				layout := "15:04"
				if *plan.Configuration.Clock == "12h" {
					layout = "3:04pm"
				}
				if *p.StartedTime != p.Start.In(loc).Format(layout) || *p.EndedTime != p.End.In(loc).Format(layout) {
					return false
				}
			}
			if !validSyncPartState(p.State) {
				return false
			}
			if p.EntryID != nil && !identity.Valid(*p.EntryID) {
				return false
			}
			if p.State == "queued" {
				if len(p.Attempts) != 0 || p.EntryID != nil || p.FailureCategory != nil {
					return false
				}
			} else if len(p.Attempts) == 0 && p.Attachment == nil {
				return false
			}
			if p.State == "submitting" && o.RunRequestID == nil {
				return false
			}
			for i, a := range p.Attempts {
				if !validUUID(a.ID) || !validUUID(a.RequestID) || ids[a.ID] || a.Number != strconv.Itoa(i+1) || !validSyncPartState(a.State) || a.State == "queued" {
					return false
				}
				origin, ok := st.Requests[a.RequestID]
				if !ok || origin.Operation != "sync.now" {
					return false
				}
				originRoots := []string{}
				if origin.PendingSync != nil && origin.PendingSync.Run != nil {
					originRoots = origin.PendingSync.RootIDs
				} else if origin.SyncRun != nil {
					originRoots = origin.SyncRun.AttemptedIDs
				} else {
					return false
				}
				found := false
				for _, id := range originRoots {
					if id == o.ID {
						found = true
					}
				}
				if !found {
					return false
				}
				ids[a.ID] = true
				if i < len(p.Attempts)-1 && a.State != "rejected" {
					return false
				}
				if a.EntryID != nil && !identity.Valid(*a.EntryID) {
					return false
				}
				if i == len(p.Attempts)-1 && (a.State != p.State || !reflect.DeepEqual(a.EntryID, p.EntryID) || !reflect.DeepEqual(a.FailureCategory, p.FailureCategory)) {
					return false
				}
			}
			if p.Attachment != nil {
				a := p.Attachment
				r, ok := st.Requests[a.RequestID]
				if !validUUID(a.RequestID) || !identity.Valid(a.EntryID) || p.EntryID == nil || a.EntryID != *p.EntryID || !ok || r.Operation != "sync.resolve" || r.MutationResult == nil || len(r.MutationResult.AffectedIDs) != 1 || r.MutationResult.AffectedIDs[0] != o.ID {
					return false
				}
			}
			if len(p.Attempts) > 0 || p.Attachment != nil {
				attempted = true
			}
			if !validSyncAmounts(p) {
				return false
			}
		}
		if !cursor.Equal(o.Interval.End) {
			return false
		}
		if !attempted {
			if o.State != "queued" && o.State != "needs_attention" {
				return false
			}
			if o.EntryID != nil {
				return false
			}
		} else if o.State != "submitting" && o.RetryRequestID == nil {
			expected := o
			syncFinishItem(&expected)
			attentionOverride := o.State == "needs_attention" && o.FailureCategory != nil && (*o.FailureCategory == "correlation_collision" || *o.FailureCategory == "response_mismatch") && expected.State != "synced"
			if !attentionOverride && (expected.State != o.State || !reflect.DeepEqual(expected.EntryID, o.EntryID) || !reflect.DeepEqual(expected.FailureCategory, o.FailureCategory)) {
				return false
			}
		}
	}
	return true
}
func syncInt(s string) (int64, bool) {
	n, e := strconv.ParseInt(s, 10, 64)
	return n, e == nil && strconv.FormatInt(n, 10) == s
}
func validSyncPartState(s string) bool {
	switch s {
	case "queued", "submitting", "synced", "rejected", "unknown", "needs_attention":
		return true
	}
	return false
}
func validSyncAmounts(p SyncPart) bool {
	if p.ConfirmedDurationNS == nil {
		return p.EntryID == nil && p.ReturnedHours == nil && p.RoundedHours == nil && p.ProviderDeltaNS == nil && p.TotalResidualNS == nil && p.State != "synced" && p.State != "needs_attention"
	}
	if p.EntryID == nil || p.ReturnedHours == nil || p.ProviderDeltaNS == nil || p.TotalResidualNS == nil || (p.State != "synced" && p.State != "needs_attention") {
		return false
	}
	_, r := syncDecimal(json.Number(*p.ReturnedHours))
	if r == nil {
		return false
	}
	confirmed := syncRoundNS(r)
	if !confirmed.IsInt64() || confirmed.String() != *p.ConfirmedDurationNS {
		return false
	}
	if p.RoundedHours != nil {
		_, rounded := syncDecimal(json.Number(*p.RoundedHours))
		if rounded == nil || !syncRoundNS(rounded).IsInt64() {
			return false
		}
	}
	planned, _ := new(big.Int).SetString(p.PlannedDurationNS, 10)
	exact, _ := new(big.Int).SetString(p.DurationNS, 10)
	if new(big.Int).Sub(planned, confirmed).String() != *p.ProviderDeltaNS || new(big.Int).Sub(exact, confirmed).String() != *p.TotalResidualNS {
		return false
	}
	intended, _ := new(big.Rat).SetString(p.PlannedHours)
	equal := intended.Cmp(r) == 0
	if equal {
		return p.State == "synced" && p.FailureCategory == nil
	}
	return p.State == "needs_attention" && p.FailureCategory != nil && *p.FailureCategory == "duration_mismatch"
}
