package ui

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
)

// LinkActions uses shared link preparation and mutations. No picker or storage
// rules are reimplemented by the presentation controller.
type LinkActions struct {
	Prepare func(context.Context, activity.LinkInput, terminal.Prompter) (activity.LinkInput, error)
	Commit  func(context.Context, activity.LinkInput) (activity.BindingResult, error)
	Unlink  func(context.Context, activity.UnlinkInput) (activity.MutationResult, error)
	Repair  func(context.Context, activity.RepairBindingInput) (activity.BindingResult, error)
}

type linkPending struct {
	operation string
	link      activity.LinkInput
	unlink    activity.UnlinkInput
	repair    activity.RepairBindingInput
	err       error
}
type linkController struct{ pending *linkPending }

func (c *linkController) run(ctx context.Context, p *promptBridge, views *ReadViews, actions *LinkActions) error {
	if actions == nil {
		return p.View(ctx, "Links actions", "Link actions unavailable.")
	}
	if c.pending != nil {
		return c.recover(ctx, p, views, actions, false)
	}
	if views == nil || views.Links == nil {
		return p.View(ctx, "Links actions", "Local links unavailable.")
	}
	list, err := observeView(ctx, views.Links)
	if err != nil {
		return linkFailure(ctx, p, err)
	}
	choices := []terminal.Choice{{ID: "create", Label: "Create link"}}
	for _, binding := range list.Bindings {
		choices = append(choices, terminal.Choice{ID: binding.ID, Label: binding.Kind + " " + binding.Locator})
	}
	id, err := p.Choose(ctx, "Links actions", choices)
	if err != nil {
		return err
	}
	var intent *linkPending
	if id == "create" {
		if actions.Prepare == nil || actions.Commit == nil {
			return p.View(ctx, "Links actions", "Create link unavailable.")
		}
		path, err := p.Text(ctx, "Absolute project path", "")
		if err != nil {
			return err
		}
		path, err = linkPath(path)
		if err != nil {
			return linkFailure(ctx, p, err)
		}
		prepared, err := actions.Prepare(ctx, activity.LinkInput{Path: path, RequestID: requestID()}, p)
		if err != nil {
			return linkFailure(ctx, p, err)
		}
		intent = &linkPending{operation: "create", link: prepared}
	} else {
		var target *activity.Binding
		for _, binding := range list.Bindings {
			if binding.ID == id {
				copy := binding
				target = &copy
				break
			}
		}
		if target == nil {
			return linkFailure(ctx, p, &activity.Error{Code: "not_found"})
		}
		operation, err := p.Choose(ctx, "Links · Binding actions", []terminal.Choice{{ID: "repair", Label: "Repair link location"}, {ID: "unlink", Label: "Unlink mapping"}})
		if err != nil {
			return err
		}
		warning := fmt.Sprintf("%s binding %s. Scope %s. Full locator %s. %s. Observed binding revision %s. ", operation, target.ID, target.Kind, target.Locator, attributionDetail(target.Attribution), target.Revision)
		intent = &linkPending{operation: operation}
		switch operation {
		case "repair":
			if actions.Repair == nil {
				return p.View(ctx, "Links actions", "Repair link unavailable.")
			}
			path, err := p.Text(ctx, "Replacement absolute path", "")
			if err != nil {
				return err
			}
			path, err = linkPath(path)
			if err != nil {
				return linkFailure(ctx, p, err)
			}
			intent.repair = activity.RepairBindingInput{BindingID: target.ID, Path: path, IfRevision: target.Revision, RequestID: requestID(), Confirmed: true}
			warning += "Replacement path " + path + ". "
		case "unlink":
			if actions.Unlink == nil {
				return p.View(ctx, "Links actions", "Unlink unavailable.")
			}
			intent.unlink = activity.UnlinkInput{BindingID: target.ID, IfRevision: target.Revision, RequestID: requestID(), Confirmed: true}
		default:
			return linkFailure(ctx, p, &activity.Error{Code: "validation"})
		}
		warning += "Historical attribution and captured intervals remain retained. This changes future capture mapping; it does not interrupt actors, pause uploads, or erase history. Active use may prevent unlinking. Review the complete scope before confirming."
		yes, err := p.Confirm(ctx, warning)
		if err != nil {
			return err
		}
		if !yes {
			return &terminal.ExitError{Code: 0}
		}
	}
	c.pending = intent
	revision, err := c.dispatch(ctx, actions)
	if err == nil {
		return c.complete(ctx, p, revision)
	}
	if !unknownOutcome(err) {
		c.pending = nil
		return linkFailure(ctx, p, err)
	}
	c.pending.err = err
	return c.recover(ctx, p, views, actions, true)
}

func requestID() string {
	var value [16]byte
	_, _ = rand.Read(value[:])
	value[6], value[8] = value[6]&15|64, value[8]&63|128
	h := hex.EncodeToString(value[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
func linkPath(path string) (string, error) {
	if path == "" || !filepath.IsAbs(path) {
		return "", &activity.Error{Code: "validation"}
	}
	return filepath.Clean(path), nil
}
func (c *linkController) id() string {
	switch c.pending.operation {
	case "create":
		return c.pending.link.RequestID
	case "repair":
		return c.pending.repair.RequestID
	default:
		return c.pending.unlink.RequestID
	}
}
func (c *linkController) dispatch(ctx context.Context, actions *LinkActions) (string, error) {
	switch c.pending.operation {
	case "create":
		result, err := actions.Commit(ctx, c.pending.link)
		return result.SnapshotRevision, err
	case "repair":
		result, err := actions.Repair(ctx, c.pending.repair)
		return result.SnapshotRevision, err
	default:
		result, err := actions.Unlink(ctx, c.pending.unlink)
		return result.SnapshotRevision, err
	}
}
func (c *linkController) complete(ctx context.Context, p *promptBridge, revision string) error {
	id := c.id()
	c.pending = nil
	return p.View(ctx, "Links · Complete", "Shared link operation completed.\nRequest ID "+id+"\nReturned snapshot revision "+revision+"\nHistorical attribution remains retained. Refresh reads current local state.")
}
func (c *linkController) recover(ctx context.Context, p *promptBridge, views *ReadViews, actions *LinkActions, acknowledge bool) error {
	if acknowledge {
		body := "local_write_unknown: the submitted link outcome or durability is unknown.\nRequest ID " + c.id() + "\nPreserve this exact input and identity. Read-only status does not establish nonapplication. Only an explicit same-intent replay is permitted; do not create a replacement request."
		if p.View(ctx, "Links · Outcome unknown", body) != nil {
			return c.pending.err
		}
	}
	for {
		choice, err := p.Choose(ctx, "Links · Recover submitted intent", []terminal.Choice{{ID: "replay", Label: c.id() + " · Replay exact intent"}, {ID: "status", Label: "Read-only status"}, {ID: "back", Label: "Back; preserve pending intent"}})
		if err != nil {
			return c.pending.err
		}
		switch choice {
		case "back":
			return c.pending.err
		case "status":
			body := "Local links unavailable."
			if views != nil && views.Links != nil {
				if list, err := observeView(ctx, views.Links); err == nil {
					var text strings.Builder
					fmt.Fprintf(&text, "Read-only status · Observation %s\nPending request %s\n", list.SnapshotRevision, c.id())
					for _, binding := range list.Bindings {
						fmt.Fprintf(&text, "\nBinding %s · Revision %s\n%s %s\n%s\n", binding.ID, binding.Revision, binding.Kind, binding.Locator, attributionDetail(binding.Attribution))
					}
					body = text.String()
				}
			}
			if p.View(ctx, "Links · Read-only status", body) != nil {
				return c.pending.err
			}
		case "replay":
			revision, err := c.dispatch(ctx, actions)
			if err == nil {
				return c.complete(ctx, p, revision)
			}
			// A failed replay cannot acknowledge the original uncertainty.
			if p.View(ctx, "Links · Outcome unknown", "The exact replay did not establish durable completion.\nRequest ID "+c.id()+"\nPreserve the original intent; no replacement request was sent.") != nil {
				return c.pending.err
			}
		}
	}
}
func linkFailure(ctx context.Context, p *promptBridge, err error) error {
	var ended *terminal.ExitError
	if errors.As(err, &ended) || ctx.Err() != nil {
		return err
	}
	code := "operation_failed"
	var domain *activity.Error
	if errors.As(err, &domain) {
		switch domain.Code {
		case "validation", "revision_conflict", "not_found", "state_busy", "binding_in_use", "request_conflict", "binding_unavailable":
			code = domain.Code
		}
	}
	guidance := "No automatic retry or mutation follows this failure. Return to Links actions to review a fresh intent."
	if code == "revision_conflict" {
		guidance = "Reload the target and review its changed state. Renew confirmation with a new request identity; no stale revision was replaced automatically."
	}
	return p.View(ctx, "Links · Failed", code+"\n"+guidance)
}
