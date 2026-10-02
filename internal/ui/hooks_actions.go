package ui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/terminal"
)

// HookActions delegates installation and retained capture policy to the shared
// installer. Observed delivery and operator declarations remain distinct.
type HookActions struct {
	Status           func(context.Context, hookstate.HookSelector) (hookstate.HookList, error)
	Verify           func(context.Context, hookstate.HookSelector) (hookstate.HookList, error)
	PreviewInstall   func(context.Context, hookstate.InstallIntent) (hookstate.HookPreview, error)
	ApplyInstall     func(context.Context, hookstate.ApplyInstallInput) (hookstate.HookList, error)
	ConfirmInstalled func(context.Context, hookstate.InstalledConfirmInput) (hookstate.HookList, error)
	Revoke           func(context.Context, hookstate.RevokeInput) (hookstate.Profile, error)
}

type hookPending struct {
	operation string
	apply     hookstate.ApplyInstallInput
	confirm   hookstate.InstalledConfirmInput
	revoke    hookstate.RevokeInput
	err       error
}

type hookController struct{ pending *hookPending }

func (c *hookController) run(ctx context.Context, p *promptBridge, actions *HookActions) error {
	if c.pending != nil {
		return c.recover(ctx, p, actions)
	}
	if actions == nil {
		return p.View(ctx, "Hooks and capture", "Hook actions unavailable.")
	}
	op, err := p.Choose(ctx, "Hooks and capture", []terminal.Choice{
		{ID: "status", Label: "Inspect hook status"}, {ID: "verify", Label: "Verify current evidence"},
		{ID: "preview", Label: "Review proposed changes"}, {ID: "install", Label: "Install hooks and skill"},
		{ID: "repair", Label: "Repair owned installation"}, {ID: "uninstall", Label: "Uninstall owned resources"},
		{ID: "confirm-profile", Label: "Confirm reviewed capture declaration"}, {ID: "revoke-profile", Label: "Revoke capture declaration"},
		{ID: "back", Label: "Back"},
	})
	if err != nil || op == "back" {
		return err
	}
	switch op {
	case "status", "verify", "preview", "install", "repair", "uninstall", "confirm-profile", "revoke-profile":
	default:
		return hookFailure(ctx, p, &hookstate.Error{Code: "validation"})
	}
	selector, err := chooseHookSelector(ctx, p)
	if err != nil {
		return hookFailure(ctx, p, err)
	}
	read := actions.Status
	if op == "verify" {
		read = actions.Verify
	}
	if read == nil {
		return p.View(ctx, "Hooks and capture", "Hook inspection unavailable.")
	}
	status, err := observeView(ctx, func(ctx context.Context) (hookstate.HookList, error) { return read(ctx, selector) })
	if err != nil {
		return hookFailure(ctx, p, err)
	}
	if op == "status" || op == "verify" {
		return p.View(ctx, "Hooks · Read-only evidence", hookDetails(status))
	}
	// A persisted transaction is submitted intent, not permission to replace
	// it with a fresh preview. Selection does not itself replay anything.
	pending, err := choosePendingHook(ctx, p, status)
	if err != nil {
		return hookFailure(ctx, p, err)
	}
	if pending != nil {
		c.pending = pending
		return c.recover(ctx, p, actions)
	}
	var intent *hookPending
	switch op {
	case "preview", "install", "repair", "uninstall":
		if actions.PreviewInstall == nil || op != "preview" && actions.ApplyInstall == nil {
			return p.View(ctx, "Hooks and capture", "Requested hook action unavailable.")
		}
		operation := op
		if op == "preview" {
			operation, err = p.Choose(ctx, "Choose preview operation", []terminal.Choice{{ID: "install", Label: "Install"}, {ID: "repair", Label: "Repair"}, {ID: "uninstall", Label: "Uninstall"}})
			if err != nil {
				return err
			}
			if operation != "install" && operation != "repair" && operation != "uninstall" {
				return hookFailure(ctx, p, &hookstate.Error{Code: "validation"})
			}
		}
		previewCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		preview, err := actions.PreviewInstall(previewCtx, hookstate.InstallIntent{Host: selector.Host, Scope: selector.Scope, Path: selector.Path, Operation: operation})
		cancel()
		if err != nil {
			return hookFailure(ctx, p, err)
		}
		body := hookPreviewDetails(preview)
		if op == "preview" {
			return p.View(ctx, "Hooks · Read-only preview", body)
		}
		intent = &hookPending{operation: op, apply: hookstate.ApplyInstallInput{Intent: preview.Intent, Fingerprint: preview.Fingerprint, RequestID: requestID(), Confirmed: true}}
		yes, err := p.Confirm(ctx, body+"\nApply this exact reviewed "+operation+"? Request ID "+intent.apply.RequestID)
		if err != nil || !yes {
			return err
		}
	case "confirm-profile", "revoke-profile":
		if op == "confirm-profile" && actions.ConfirmInstalled == nil || op == "revoke-profile" && actions.Revoke == nil {
			return p.View(ctx, "Hooks and capture", "Requested profile action unavailable.")
		}
		row, err := chooseHookProfile(ctx, p, status)
		if err != nil {
			return hookFailure(ctx, p, err)
		}
		profile := row.Profile
		observed := hookstate.HookSelector{Host: row.Host, Scope: row.Scope, Path: row.Path}
		intent = &hookPending{operation: op}
		body := fmt.Sprintf("%s for host %s · scope %s\nExact project context %s\nObserved profile revision %s\nFingerprint %s\nDeclaration version %s\nActivity history remains retained. Upload consent and worker ownership are separate.\n", op, observed.Host, observed.Scope, observed.Path, profile.Revision, profile.Fingerprint, profile.DeclarationVersion)
		if op == "confirm-profile" {
			intent.confirm = hookstate.InstalledConfirmInput{Selector: observed, Fingerprint: profile.Fingerprint, DeclarationVersion: profile.DeclarationVersion, RequestID: requestID(), Confirmed: true}
			body += "I reviewed the host's actual /hooks list and workspace trust after reloading. Required synchronous Tempo handlers are enabled. No competing prompt veto, continuation, async, disabled or untrusted handlers are present. Unexported dynamic and plugin changes remain an operator_declared risk of inaccurate capture. Retaining this declaration does not prove real delivery or receiving. Review every inspected artifact:\n"
			for _, artifact := range profile.Context.Artifacts {
				body += fmt.Sprintf("%s · %s · SHA256 %s\n", artifact.Role, artifact.Path, artifact.SHA256)
			}
		} else {
			intent.revoke = hookstate.RevokeInput{Host: observed.Host, Scope: observed.Scope, Path: observed.Path, IfRevision: profile.Revision, RequestID: requestID(), Confirmed: true}
			body += "Revoke retained capture eligibility for this exact context; later adapter admission is blocked. Existing history is preserved."
		}
		yes, err := p.Confirm(ctx, body+"\nRequest ID "+intent.id())
		if err != nil || !yes {
			return err
		}
	}
	c.pending = intent
	result, err := c.dispatch(ctx, actions)
	if err == nil {
		return c.complete(ctx, p, result)
	}
	if !unknownOutcome(err) {
		c.pending = nil
		return hookFailure(ctx, p, err)
	}
	c.pending.err = err
	if p.View(ctx, "Hooks · Outcome unknown", c.unknownDetails()) != nil {
		return c.pending.err
	}
	return c.recover(ctx, p, actions)
}

func chooseHookSelector(ctx context.Context, p *promptBridge) (hookstate.HookSelector, error) {
	host, err := p.Choose(ctx, "Choose hook host", []terminal.Choice{{ID: "codex", Label: "Codex"}, {ID: "claude", Label: "Claude"}, {ID: "both", Label: "Both hosts"}})
	if err != nil {
		return hookstate.HookSelector{}, err
	}
	scope, err := p.Choose(ctx, "Choose hook scope", []terminal.Choice{{ID: "project", Label: "Project"}, {ID: "user", Label: "User"}})
	if err != nil {
		return hookstate.HookSelector{}, err
	}
	path, err := p.Text(ctx, "Absolute project context path", "")
	if err != nil {
		return hookstate.HookSelector{}, err
	}
	if host != "codex" && host != "claude" && host != "both" || scope != "project" && scope != "user" {
		return hookstate.HookSelector{}, &hookstate.Error{Code: "validation"}
	}
	path, err = linkPath(path)
	if err != nil {
		return hookstate.HookSelector{}, &hookstate.Error{Code: "validation"}
	}
	return hookstate.HookSelector{Host: host, Scope: scope, Path: path}, nil
}

func choosePendingHook(ctx context.Context, p *promptBridge, list hookstate.HookList) (*hookPending, error) {
	var pending []hookstate.PendingInstall
	for _, row := range list.Hooks {
		if row.Pending == nil {
			continue
		}
		duplicate := false
		for _, prior := range pending {
			if prior.RequestID == row.Pending.RequestID {
				if prior != *row.Pending {
					return nil, &hookstate.Error{Code: "state_corrupt"}
				}
				duplicate = true
			}
		}
		if !duplicate {
			pending = append(pending, *row.Pending)
		}
	}
	if len(pending) == 0 {
		return nil, nil
	}
	selected := pending[0]
	if len(pending) > 1 {
		choices := make([]terminal.Choice, 0, len(pending))
		for _, saved := range pending {
			choices = append(choices, terminal.Choice{ID: saved.RequestID, Label: saved.RequestID + " · " + saved.Intent.Operation + " · " + saved.Intent.Host + " · " + saved.Intent.Path})
		}
		id, err := p.Choose(ctx, "Hooks · Select pending request", choices)
		if err != nil {
			return nil, err
		}
		found := false
		for _, saved := range pending {
			if saved.RequestID == id {
				selected, found = saved, true
			}
		}
		if !found {
			return nil, &hookstate.Error{Code: "validation"}
		}
	}
	return &hookPending{operation: selected.Intent.Operation, apply: hookstate.ApplyInstallInput{Intent: selected.Intent, Fingerprint: selected.Fingerprint, RequestID: selected.RequestID, Confirmed: true}, err: &hookstate.Error{Code: "local_write_unknown", RequestID: selected.RequestID, Uncertain: true}}, nil
}

func chooseHookProfile(ctx context.Context, p *promptBridge, list hookstate.HookList) (hookstate.HookStatus, error) {
	if len(list.Hooks) == 1 {
		return list.Hooks[0], nil
	}
	choices := make([]terminal.Choice, 0, len(list.Hooks))
	for _, row := range list.Hooks {
		choices = append(choices, terminal.Choice{ID: row.Host, Label: row.Host + " · " + row.Scope + " · " + row.Path})
	}
	if len(choices) == 0 {
		return hookstate.HookStatus{}, &hookstate.Error{Code: "input_required"}
	}
	host, err := p.Choose(ctx, "Choose observed capture profile", choices)
	if err != nil {
		return hookstate.HookStatus{}, err
	}
	for _, row := range list.Hooks {
		if row.Host == host {
			return row, nil
		}
	}
	return hookstate.HookStatus{}, &hookstate.Error{Code: "validation"}
}

func (in *hookPending) id() string {
	switch in.operation {
	case "confirm-profile":
		return in.confirm.RequestID
	case "revoke-profile":
		return in.revoke.RequestID
	default:
		return in.apply.RequestID
	}
}
func (in *hookPending) selector() hookstate.HookSelector {
	switch in.operation {
	case "confirm-profile":
		return in.confirm.Selector
	case "revoke-profile":
		return hookstate.HookSelector{Host: in.revoke.Host, Scope: in.revoke.Scope, Path: in.revoke.Path}
	default:
		return hookstate.HookSelector{Host: in.apply.Intent.Host, Scope: in.apply.Intent.Scope, Path: in.apply.Intent.Path}
	}
}
func (c *hookController) dispatch(ctx context.Context, actions *HookActions) (hookstate.HookList, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if ctx.Err() != nil {
		return hookstate.HookList{}, context.Cause(ctx)
	}
	if actions != nil {
		switch c.pending.operation {
		case "confirm-profile":
			if actions.ConfirmInstalled != nil {
				return actions.ConfirmInstalled(ctx, c.pending.confirm)
			}
		case "revoke-profile":
			if actions.Revoke != nil {
				profile, err := actions.Revoke(ctx, c.pending.revoke)
				return hookstate.ProfileResult(profile, c.pending.id()), err
			}
		case "install", "repair", "uninstall":
			if actions.ApplyInstall != nil {
				return actions.ApplyInstall(ctx, c.pending.apply)
			}
		}
	}
	return hookstate.HookList{}, &hookstate.Error{Code: "unsupported_contract"}
}
func (c *hookController) complete(ctx context.Context, p *promptBridge, result hookstate.HookList) error {
	id := c.pending.id()
	c.pending = nil
	return p.View(ctx, "Hooks · Complete", "Request ID "+id+"\n"+hookDetails(result))
}
func (c *hookController) unknownDetails() string {
	selector := c.pending.selector()
	details := "Fingerprint " + c.pending.apply.Fingerprint
	switch c.pending.operation {
	case "confirm-profile":
		details = "Fingerprint " + c.pending.confirm.Fingerprint + "\nDeclaration version " + c.pending.confirm.DeclarationVersion
	case "revoke-profile":
		details = "Observed profile revision " + c.pending.revoke.IfRevision
	}
	return "local_write_unknown: the submitted hook outcome or durability is unknown.\nRequest ID " + c.pending.id() + "\nOperation " + c.pending.operation + "\nHost " + selector.Host + " · Scope " + selector.Scope + "\nExact context " + selector.Path + "\n" + details + "\nPreserve the same input, fingerprint/revision and request identity. Read-only status does not prove nonapplication. Only explicit same-intent replay can establish completion; a new preview or request is not recovery."
}
func (c *hookController) recover(ctx context.Context, p *promptBridge, actions *HookActions) error {
	for {
		id, err := p.Choose(ctx, "Hooks · Recover pending request", []terminal.Choice{{ID: "replay", Label: c.pending.id() + " · Replay exact intent"}, {ID: "status", Label: "Read-only status"}, {ID: "back", Label: "Back; preserve pending request"}})
		if err != nil || id == "back" {
			return c.pending.err
		}
		switch id {
		case "status":
			body := c.unknownDetails() + "\nRead-only hook inspection unavailable."
			if actions != nil && actions.Status != nil {
				result, err := observeView(ctx, func(ctx context.Context) (hookstate.HookList, error) {
					return actions.Status(ctx, c.pending.selector())
				})
				if err == nil {
					body = c.unknownDetails() + "\nRead-only observation:\n" + hookDetails(result)
				}
			}
			if p.View(ctx, "Hooks · Read-only status", body) != nil {
				return c.pending.err
			}
		case "replay":
			result, err := c.dispatch(ctx, actions)
			if err == nil {
				return c.complete(ctx, p, result)
			}
			if p.View(ctx, "Hooks · Outcome unknown", c.unknownDetails()+"\nThe exact replay did not establish completion; no replacement request was sent. The requested action may be unavailable.") != nil {
				return c.pending.err
			}
		default:
			return c.pending.err
		}
	}
}

func hookPreviewDetails(preview hookstate.HookPreview) string {
	var body strings.Builder
	fmt.Fprintf(&body, "Review exact %s · host %s · scope %s\nCanonical context %s\nFingerprint %s\n", preview.Intent.Operation, preview.Intent.Host, preview.Intent.Scope, preview.Intent.Path, preview.Fingerprint)
	for _, change := range preview.Changes {
		fmt.Fprintf(&body, "%s · %s · %s\n%s\n", change.Host, change.Operation, change.Path, change.SafeSummary)
	}
	for _, step := range preview.ApprovalSteps {
		fmt.Fprintln(&body, step)
	}
	body.WriteString("Review workspace trust and the actual /hooks list; reload the host as required. Installation does not grant capture eligibility, prove host delivery, enable uploads or control the worker. Activity history is retained.")
	return body.String()
}
func hookDetails(list hookstate.HookList) string {
	var body strings.Builder
	if list.RequestID != "" {
		fmt.Fprintf(&body, "Request ID %s\n", list.RequestID)
	}
	for _, row := range list.Hooks {
		fmt.Fprintf(&body, "\nHost %s · Scope %s\nContext %s\nState %s · Ordering %s · Runtime %s\nProfile basis %s · State %s · Revision %s\nCapture eligible %t\nFingerprint %s · Declaration %s\n", row.Host, row.Scope, row.Path, row.State, row.Ordering, row.RuntimeVersion, row.Profile.Basis, row.Profile.State, row.Profile.Revision, row.Profile.CaptureEligible, row.Profile.Fingerprint, row.Profile.DeclarationVersion)
		if row.Pending != nil {
			fmt.Fprintf(&body, "Pending request %s · %s\nFingerprint %s\n", row.Pending.RequestID, row.Pending.Intent.Operation, row.Pending.Fingerprint)
		}
		for _, diagnostic := range row.Diagnostics {
			fmt.Fprintf(&body, "%s · %s\n%s\n", diagnostic.Code, diagnostic.Severity, diagnostic.SafeMessage)
		}
	}
	body.WriteString("Installed resources, operator declarations and actual host delivery are separate. Verification is read-only and fires no event. Callback receipts have unverified origin and do not prove receiving. Capture history, upload consent and worker ownership remain separate.")
	return body.String()
}
func hookFailure(ctx context.Context, p *promptBridge, err error) error {
	var ended *terminal.ExitError
	if errors.As(err, &ended) {
		return err
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	code := "operation_failed"
	var domain *hookstate.Error
	if errors.As(err, &domain) {
		switch domain.Code {
		case "validation", "input_required", "confirmation_required", "revision_conflict", "request_conflict", "state_busy", "state_corrupt", "unsupported_contract", "binding_unavailable", "not_found", "ordering_unavailable":
			code = domain.Code
		}
	}
	return p.View(ctx, "Hooks · Failed", code+"\nReview current status, obtain a fresh preview or profile observation, and renew confirmation with a new request ID. No automatic retry follows this definite failure.")
}
