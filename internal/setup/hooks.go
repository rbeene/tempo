package setup

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/terminal"
)

func hookSelector(in Input) (hookstate.HookSelector, error) {
	if in.Host == "" {
		in.Host = "both"
	}
	if in.Scope == "" {
		in.Scope = "project"
	}
	if in.Path == "" {
		var err error
		in.Path, err = os.Getwd()
		if err != nil {
			return hookstate.HookSelector{}, err
		}
	}
	return hookstate.HookSelector{Host: in.Host, Scope: in.Scope, Path: in.Path}, nil
}

// ManageHooks is the finite shared terminal control surface. Selection, preview,
// and confirmation delegate to the same service used by the noninteractive CLI.
func (s *Service) ManageHooks(ctx context.Context, in Input, p terminal.Prompter) (hookstate.HookList, error) {
	empty := hookstate.HookList{ContractVersion: 1, Hooks: []hookstate.HookStatus{}}
	if s.options.Hooks == nil {
		return empty, &hookstate.Error{Code: "unsupported_contract"}
	}
	selector, err := hookSelector(in)
	if err != nil {
		return empty, err
	}
	status, err := s.options.Hooks.Status(ctx, selector)
	if err != nil || p == nil {
		return status, err
	}
	choices := []terminal.Choice{{ID: "preview", Label: "Preview installation"}, {ID: "install", Label: "Install hooks and skill"}, {ID: "status", Label: "Inspect status"}, {ID: "verify", Label: "Verify current evidence"}, {ID: "repair", Label: "Repair owned installation"}, {ID: "uninstall", Label: "Uninstall owned resources"}, {ID: "confirm-profile", Label: "Confirm reviewed clean capture profile"}, {ID: "revoke-profile", Label: "Revoke capture profile"}, {ID: "cancel", Label: "Keep current state"}}
	action, err := p.Choose(ctx, "Hooks and capture policy", choices)
	if err != nil {
		return status, err
	}
	switch action {
	case "cancel", "status":
		return status, nil
	case "verify":
		return s.options.Hooks.Verify(ctx, selector)
	case "preview", "install", "repair", "uninstall":
		operation := action
		if operation == "preview" {
			operation = "install"
		}
		preview, e := s.options.Hooks.PreviewInstall(ctx, hookstate.InstallIntent{Host: selector.Host, Scope: selector.Scope, Path: selector.Path, Operation: operation})
		if e != nil {
			return status, e
		}
		var text strings.Builder
		text.WriteString("Review these exact changes:\n")
		for _, change := range preview.Changes {
			fmt.Fprintf(&text, "%s: %s\n%s\n", change.Operation, change.Path, change.SafeSummary)
		}
		for _, step := range preview.ApprovalSteps {
			text.WriteString(step + "\n")
		}
		if action == "preview" {
			text.WriteString("Finish preview without applying changes?")
		} else {
			text.WriteString("Apply this reviewed " + operation + "?")
		}
		yes, e := p.Confirm(ctx, terminal.Sanitize(text.String()))
		if e != nil {
			return status, e
		}
		if action == "preview" || !yes {
			return status, nil
		}
		return s.options.Hooks.ApplyInstall(ctx, hookstate.ApplyInstallInput{Intent: preview.Intent, Fingerprint: preview.Fingerprint, RequestID: uuid(), Confirmed: true})
	case "confirm-profile", "revoke-profile":
		if len(status.Hooks) != 1 {
			return status, missing("single_host")
		}
		profile := status.Hooks[0].Profile
		text := "Revoke retained capture eligibility for " + selector.Host + " in " + selector.Path + "?"
		if action == "confirm-profile" {
			text = "I reviewed the host's actual /hooks list, workspace trust and required synchronous Tempo handlers after reloading. There are no competing prompt veto, continuation, async, disabled or untrusted handlers. I understand undisclosed dynamic/plugin changes may make timing inaccurate. Retain this declaration for " + selector.Host + " in " + selector.Path + " until known drift or revocation?"
		}
		yes, e := p.Confirm(ctx, terminal.Sanitize(text))
		if e != nil {
			return status, e
		}
		if !yes {
			return status, nil
		}
		if action == "confirm-profile" {
			return s.options.Hooks.ConfirmInstalled(ctx, hookstate.InstalledConfirmInput{Selector: selector, Fingerprint: profile.Fingerprint, DeclarationVersion: profile.DeclarationVersion, RequestID: uuid(), Confirmed: true})
		}
		id := uuid()
		revoked, e := s.options.Hooks.Revoke(ctx, hookstate.RevokeInput{Host: selector.Host, Scope: selector.Scope, Path: selector.Path, IfRevision: profile.Revision, RequestID: id, Confirmed: true})
		if e != nil {
			return status, e
		}
		return hookstate.ProfileResult(revoked, id), nil
	default:
		return status, &hookstate.Error{Code: "validation"}
	}
}
func (s *Service) hookReadiness(ctx context.Context, in Input, result *Status) error {
	if s.options.Hooks != nil {
		selector, err := hookSelector(in)
		if err != nil {
			return err
		}
		list, err := s.options.Hooks.Status(ctx, selector)
		if err != nil {
			return err
		}
		states := []string{}
		for _, row := range list.Hooks {
			states = append(states, row.Host+": "+row.State)
		}
		result.Steps[2] = Step{"hooks.status", "input_required", []string{}, strings.Join(states, "; ") + ". Use hooks status or interactive setup to review installation, trust, retained policy and delivery separately."}
		ready := len(list.Hooks) > 0
		for _, row := range list.Hooks {
			ready = ready && row.Profile.CaptureEligible
		}
		if ready {
			result.Steps[2] = Step{"hooks.status", "complete", []string{}, "Operator-declared capture eligibility is ready; actual host delivery remains unverified. Upload consent and worker readiness are separate."}
		}
	}
	if s.options.Worker != nil {
		st, err := s.options.Worker.Status(ctx)
		if err != nil {
			return err
		}
		message := "Inspect worker status for installation and ownership; installation and capture do not enable uploads."
		if st.Status.Installed != nil && *st.Status.Installed {
			message = "User worker is installed; inspect worker status for current ownership. Upload consent remains separate."
		}
		result.Steps = append(result.Steps, Step{"worker.status", "input_required", []string{}, message})
	}
	return nil
}
