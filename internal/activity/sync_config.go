package activity

import (
	"context"
	"errors"
	"time"

	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/identity"
)

func syncRequired(fields ...string) *Error {
	e := failure("input_required")
	e.Message = "complete the explicit sync choices; link a project before configuring sync"
	e.Details = map[string]any{"required_fields": fields}
	return e
}
func syncConfigKey(account, user string) string { return account + "/" + user }
func syncSafeError(err error) *Error {
	code := "network"
	var ae *Error
	var he *harvest.Error
	var authErr *auth.Error
	switch {
	case errors.As(err, &ae):
		code = ae.Code
	case errors.As(err, &he):
		code = he.Code
	case errors.As(err, &authErr):
		code = authErr.Code
	case errors.Is(err, auth.ErrNotFound):
		code = "auth"
	}
	switch code {
	case "identity_conflict", "assignment_unavailable", "mode_conflict", "company_unavailable", "representation", "timezone", "auth", "forbidden", "validation", "network", "api", "rate_limit", "response", "keychain", "state_busy", "revision_conflict", "input_required":
	default:
		code = "network"
	}
	return failure(code)
}
func syncProvider(ctx context.Context, d SyncDependencies, account string) (harvest.Provider, error) {
	if d.NewProvider == nil {
		return nil, failure("auth")
	}
	p, e := d.NewProvider(ctx, account)
	if e != nil {
		return nil, syncSafeError(e)
	}
	if p == nil {
		return nil, failure("auth")
	}
	return p, nil
}
func syncIdentity(ctx context.Context, p harvest.Provider, account string) (harvest.Object, error) {
	rows, e := p.Accounts(ctx)
	if e != nil {
		return nil, syncSafeError(e)
	}
	found := false
	for _, a := range rows {
		if bindingID(a) == account && a["product"] == "harvest" && a["is_active"] != false {
			found = true
		}
	}
	if !found {
		return nil, failure("forbidden")
	}
	u, e := p.Get(ctx, "/users/me")
	if e != nil {
		return nil, syncSafeError(e)
	}
	if !identity.Valid(bindingID(u)) {
		return nil, failure("response")
	}
	active, ok := u["is_active"].(bool)
	if !ok {
		return nil, failure("response")
	}
	if !active {
		return nil, failure("forbidden")
	}
	return u, nil
}
func validateSyncConfig(in SyncConfigureInput) error {
	if !validUUID(in.RequestID) {
		return failure("validation")
	}
	if !in.Confirmed {
		return failure("confirmation_required")
	}
	if in.AccountID == "" || in.Mode == "" || in.DurationPolicy == "" || in.IfRevision == "" {
		return syncRequired("account_id", "mode", "duration_policy", "if_revision")
	}
	if !identity.Valid(in.AccountID) || in.UserID != "" && !identity.Valid(in.UserID) {
		return failure("validation")
	}
	if _, ok := counter(in.IfRevision); !ok {
		return failure("validation")
	}
	if in.Mode != "duration" && in.Mode != "timestamp" {
		return failure("validation")
	}
	if in.DurationPolicy != "exact" && in.DurationPolicy != "nearest-hundredth-hour" {
		return failure("validation")
	}
	if in.Mode == "duration" && in.Clock != "" {
		return failure("validation")
	}
	if in.Mode == "timestamp" && in.Clock == "" {
		return syncRequired("clock")
	}
	if in.Mode == "timestamp" && (in.DurationPolicy != "exact" || (in.Clock != "12h" && in.Clock != "24h")) {
		return failure("validation")
	}
	return nil
}
func (s *Service) SyncConfigure(ctx context.Context, in SyncConfigureInput, d SyncDependencies) (SyncConfigurationResult, error) {
	if e := validateSyncConfig(in); e != nil {
		return SyncConfigurationResult{}, e
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	fp := mutationFingerprint("sync.configure", in)
	old, ok, e := s.replayMutation(ctx, in.RequestID, "sync.configure", fp)
	if e != nil {
		return SyncConfigurationResult{}, e
	}
	if ok {
		return *old.SyncConfigurationResult, nil
	}
	_, exists, e := s.store.read(ctx)
	if e != nil {
		return SyncConfigurationResult{}, e
	}
	if !exists {
		return SyncConfigurationResult{}, syncRequired("binding")
	}
	p, e := syncProvider(ctx, d, in.AccountID)
	if e != nil {
		return SyncConfigurationResult{}, e
	}
	u, e := syncIdentity(ctx, p, in.AccountID)
	if e != nil {
		return SyncConfigurationResult{}, e
	}
	if in.UserID != "" && bindingID(u) != in.UserID {
		return SyncConfigurationResult{}, failure("identity_conflict")
	}
	key := syncConfigKey(in.AccountID, bindingID(u))
	var result SyncConfigurationResult
	e = s.store.update(ctx, func(st *state) (bool, error) {
		if r, ok, e := mutationLookup(st, in.RequestID, "sync.configure", fp); e != nil {
			return false, e
		} else if ok {
			result = *r.SyncConfigurationResult
			return false, nil
		}
		revision := "0"
		if c, ok := st.SyncConfigurations[key]; ok {
			revision = c.Revision
		}
		if revision != in.IfRevision {
			return false, failure("revision_conflict")
		}
		if n, _ := counter(revision); n == ^uint64(0) {
			return false, failure("validation")
		}
		c := SyncConfiguration{AccountID: in.AccountID, UserID: bindingID(u), Revision: bump(revision), Mode: in.Mode, DurationPolicy: in.DurationPolicy, PolicyVersion: in.DurationPolicy + "-v1", Declared: true, DeclaredAt: time.Now().UTC(), Source: "user_declared"}
		if in.Clock != "" {
			c.Clock = &in.Clock
		}
		if st.SyncConfigurations == nil {
			st.SyncConfigurations = map[string]SyncConfiguration{}
		}
		st.SyncConfigurations[key] = c
		result = SyncConfigurationResult{ContractVersion: 1, SnapshotRevision: bump(st.Revision), RequestID: in.RequestID, Changed: true, Configuration: c}
		saveMutation(st, in.RequestID, mutationRequest{Operation: "sync.configure", Fingerprint: fp, SyncConfigurationResult: &result})
		return true, nil
	})
	if e != nil {
		return SyncConfigurationResult{}, requestError(e, in.RequestID)
	}
	return result, nil
}
func (s *Service) SyncPause(ctx context.Context, id string) (MutationResult, error) {
	return s.syncControl(ctx, id, false)
}
func (s *Service) SyncResume(ctx context.Context, id string) (MutationResult, error) {
	return s.syncControl(ctx, id, true)
}
func (s *Service) syncControl(ctx context.Context, id string, enabled bool) (MutationResult, error) {
	if !validUUID(id) {
		return MutationResult{}, failure("validation")
	}
	op := "sync.pause"
	if enabled {
		op = "sync.resume"
	}
	fp := mutationFingerprint(op, id)
	old, ok, e := s.replayMutation(ctx, id, op, fp)
	if e != nil {
		return MutationResult{}, e
	}
	if ok {
		return *old.MutationResult, nil
	}
	_, exists, e := s.store.read(ctx)
	if e != nil {
		return MutationResult{}, e
	}
	if !exists {
		return MutationResult{}, syncRequired("binding")
	}
	var result MutationResult
	e = s.store.update(ctx, func(st *state) (bool, error) {
		if r, ok, e := mutationLookup(st, id, op, fp); e != nil {
			return false, e
		} else if ok {
			result = *r.MutationResult
			return false, nil
		}
		result = MutationResult{ContractVersion: 1, SnapshotRevision: bump(st.Revision), RequestID: id, Changed: st.SyncEnabled != enabled, AffectedIDs: []string{}}
		st.SyncEnabled = enabled
		saveMutation(st, id, mutationRequest{Operation: op, Fingerprint: fp, MutationResult: &result})
		return true, nil
	})
	if e != nil {
		return MutationResult{}, requestError(e, id)
	}
	return result, nil
}
