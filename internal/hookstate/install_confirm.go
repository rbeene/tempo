package hookstate

import (
	"context"
	"errors"
)

type InstalledConfirmInput struct {
	Selector                                   HookSelector
	Fingerprint, DeclarationVersion, RequestID string
	Confirmed                                  bool
}

// ConfirmInstalled is the shared CLI/UI bridge. Completed policy replay uses its
// original context before discovery so identical requests remain replayable after
// runtime/config drift. The explicit selector still has to match that context.
func (s *Service) ConfirmInstalled(ctx context.Context, in InstalledConfirmInput) (HookList, error) {
	m, _, err := s.read(ctx)
	if err != nil {
		return HookList{}, err
	}
	var c Context
	if saved, ok := m.Requests[in.RequestID]; ok && saved.Operation == "hooks.confirm-profile" && saved.Result.Context.Host == in.Selector.Host && saved.Result.Context.Scope == in.Selector.Scope && saved.Result.Context.Path == in.Selector.Path {
		c = saved.Result.Context
	} else {
		status, e := s.Status(ctx, in.Selector)
		if e != nil {
			return HookList{}, e
		}
		if len(status.Hooks) != 1 || status.Hooks[0].Profile.Fingerprint == "" {
			return HookList{}, problem("validation")
		}
		c = status.Hooks[0].Profile.Context
	}
	p, err := s.Confirm(ctx, ConfirmInput{Context: c, Fingerprint: in.Fingerprint, DeclarationVersion: in.DeclarationVersion, RequestID: in.RequestID, Confirmed: in.Confirmed})
	if err != nil {
		return HookList{}, err
	}
	return ProfileResult(p, in.RequestID), nil
}
func ProfileResult(p Profile, requestID ...string) HookList {
	id := ""
	if len(requestID) > 0 {
		id = requestID[0]
	}
	state := "approval_required"
	ordering := "unavailable"
	if p.CaptureEligible {
		state = "awaiting_real_event"
		ordering = "supported"
	}
	return HookList{RequestID: id, ContractVersion: 1, Hooks: []HookStatus{{Host: p.Context.Host, Scope: p.Context.Scope, Path: p.Context.Path, RuntimeVersion: p.Context.RuntimeVersion, State: state, Ordering: ordering, Diagnostics: []Diagnostic{}, Profile: p}}}
}

func requestError(err error, id string) error {
	if err == nil {
		return nil
	}
	var safe *Error
	if errors.As(err, &safe) && uuid(id) {
		copy := *safe
		copy.RequestID = id
		return &copy
	}
	return err
}
