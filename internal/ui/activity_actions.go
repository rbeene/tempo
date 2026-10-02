package ui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
)

// ActivityActions uses the existing capture/recovery engine. Timing decisions
// and interval projections remain authoritative shared-service results.
type ActivityActions struct {
	Status    func(context.Context) (activity.ActivitySnapshot, error)
	Review    func(context.Context, activity.ReviewInput) (activity.ReviewList, error)
	Preview   func(context.Context, activity.RecoveryInput) (activity.RecoveryPreview, error)
	Resolve   func(context.Context, activity.ResolveInput) (activity.MutationResult, error)
	Interrupt func(context.Context, activity.InterruptInput) (activity.MutationResult, error)
}

type activityPending struct {
	operation string
	interrupt activity.InterruptInput
	resolve   activity.ResolveInput
	err       error
}

type activityController struct{ pending *activityPending }

func (c *activityController) run(ctx context.Context, p *promptBridge, actions *ActivityActions) error {
	if actions == nil {
		return p.View(ctx, "Activity actions", "Activity actions unavailable.")
	}
	if c.pending != nil {
		return c.recover(ctx, p, actions, false)
	}
	id, err := p.Choose(ctx, "Activity actions", []terminal.Choice{{ID: "actors", Label: "Inspect and interrupt an actor"}, {ID: "review", Label: "Review uncertain timing"}, {ID: "capture", Label: "Inspect capture warnings"}, {ID: "back", Label: "Back"}})
	if err != nil || id == "back" {
		return err
	}
	var intent *activityPending
	switch id {
	case "actors":
		intent, err = prepareInterrupt(ctx, p, actions)
	case "review":
		intent, err = prepareRecovery(ctx, p, actions)
	case "capture":
		return captureReview(ctx, p, actions)
	default:
		err = &activity.Error{Code: "validation"}
	}
	if err != nil {
		return activityFailure(ctx, p, err)
	}
	if intent == nil {
		return nil
	}
	c.pending = intent
	result, err := c.dispatch(ctx, actions)
	if err == nil {
		return c.complete(ctx, p, result)
	}
	if !unknownOutcome(err) {
		c.pending = nil
		return activityFailure(ctx, p, err)
	}
	c.pending.err = err
	return c.recover(ctx, p, actions, true)
}

// Results used by forms own their nested slices/pointers independently of the
// reader. Background observations cannot edit a captured target or preview.
func captureValue[T any](value T) T {
	encoded, _ := json.Marshal(value)
	var owned T
	_ = json.Unmarshal(encoded, &owned)
	return owned
}

func prepareInterrupt(ctx context.Context, p *promptBridge, actions *ActivityActions) (*activityPending, error) {
	if actions.Status == nil || actions.Interrupt == nil {
		return nil, p.View(ctx, "Actor actions", "Actor interruption unavailable.")
	}
	snapshot, err := observeView(ctx, actions.Status)
	if err != nil {
		return nil, err
	}
	snapshot = captureValue(snapshot)
	choices := []terminal.Choice{}
	for _, actor := range snapshot.Actors {
		if actor.State == "working" || actor.State == "waiting" {
			choices = append(choices, terminal.Choice{ID: actor.ID, Label: actor.State + " · Project " + actor.Attribution.ProjectID + " · " + actor.Ref.Key.AgentID})
		}
	}
	if len(choices) == 0 {
		return nil, p.View(ctx, "Actor actions", "No working or waiting actors.")
	}
	id, err := p.Choose(ctx, "Actor actions", choices)
	if err != nil {
		return nil, err
	}
	for _, actor := range snapshot.Actors {
		if actor.ID != id || actor.State != "working" && actor.State != "waiting" {
			continue
		}
		warning := fmt.Sprintf("Interrupt actor %s\nComputer %s · Source %s · Session %s · Agent %s\nGeneration %s · Actor revision %s\n%s\nThis detaches only the selected actor generation and quarantines an unconfirmed working tail. It does not stop a child, pause sync, stop a worker or erase captured history. Review uncertain timing separately; interruption never invents a reliable stop.", actor.ID, actor.Ref.Key.ComputerID, actor.Ref.Key.Source, actor.Ref.Key.SessionID, actor.Ref.Key.AgentID, actor.Ref.Generation, actor.Revision, attributionDetail(actor.Attribution))
		yes, err := p.Confirm(ctx, warning)
		if err != nil || !yes {
			return nil, err
		}
		return &activityPending{operation: "interrupt", interrupt: activity.InterruptInput{ActorID: actor.ID, Generation: actor.Ref.Generation, IfRevision: actor.Revision, RequestID: requestID(), Confirmed: true}}, nil
	}
	return nil, &activity.Error{Code: "actor_not_found"}
}

func prepareRecovery(ctx context.Context, p *promptBridge, actions *ActivityActions) (*activityPending, error) {
	if actions.Review == nil || actions.Preview == nil || actions.Resolve == nil {
		return nil, p.View(ctx, "Recovery", "Timing recovery unavailable.")
	}
	list, err := observeView(ctx, func(ctx context.Context) (activity.ReviewList, error) {
		return actions.Review(ctx, activity.ReviewInput{})
	})
	if err != nil {
		return nil, err
	}
	list = captureValue(list)
	choices := []terminal.Choice{}
	for _, uncertainty := range list.Uncertainties {
		choices = append(choices, terminal.Choice{ID: uncertainty.ID, Label: "Project " + uncertainty.Attribution.ProjectID + " · " + uncertainty.Reason})
	}
	if len(choices) == 0 {
		return nil, p.View(ctx, "Recovery", "No unresolved timing uncertainties. Capture warnings remain separate read-only evidence.")
	}
	id, err := p.Choose(ctx, "Timing uncertainties", choices)
	if err != nil {
		return nil, err
	}
	var target *activity.Uncertainty
	for _, uncertainty := range list.Uncertainties {
		if uncertainty.ID == id {
			owned := uncertainty
			target = &owned
			break
		}
	}
	if target == nil {
		return nil, &activity.Error{Code: "uncertainty_not_found"}
	}
	for {
		boundary, err := p.Choose(ctx, "Recovery boundary", []terminal.Choice{{ID: "end", Label: "Use a reviewed UTC end"}, {ID: "discard", Label: "Discard the uncertain tail"}, {ID: "back", Label: "Back"}})
		if err != nil || boundary == "back" {
			return nil, err
		}
		input := activity.RecoveryInput{UncertaintyID: target.ID}
		switch boundary {
		case "discard":
			input.DiscardTail = true
		case "end":
			text, err := p.Text(ctx, "UTC recovery end (RFC3339)", target.LowerBound.Format(time.RFC3339Nano))
			if err != nil {
				return nil, err
			}
			end, err := time.Parse(time.RFC3339Nano, text)
			if err != nil {
				return nil, &activity.Error{Code: "validation"}
			}
			end = end.UTC()
			input.End = &end
		default:
			return nil, &activity.Error{Code: "validation"}
		}
		reason, err := p.Text(ctx, "Recovery reason", "")
		if err != nil {
			return nil, err
		}
		preview, err := observeView(ctx, func(ctx context.Context) (activity.RecoveryPreview, error) { return actions.Preview(ctx, input) })
		if err != nil {
			return nil, err
		}
		preview = captureValue(preview)
		choice, err := p.Choose(ctx, "Recovery preview", []terminal.Choice{{ID: "confirm", Label: "Review full preview and confirm"}, {ID: "edit", Label: "Edit boundary and reason; invalidate preview"}, {ID: "back", Label: "Back"}})
		if err != nil || choice == "back" {
			return nil, err
		}
		if choice == "edit" {
			continue
		}
		if choice != "confirm" {
			return nil, &activity.Error{Code: "validation"}
		}
		yes, err := p.Confirm(ctx, recoveryWarning(preview, input.DiscardTail, reason))
		if err != nil || !yes {
			return nil, err
		}
		return &activityPending{operation: "resolve", resolve: activity.ResolveInput{UncertaintyID: target.ID, End: input.End, DiscardTail: input.DiscardTail, IfRevision: preview.Uncertainty.Revision, Reason: reason, RequestID: requestID(), Confirmed: true}}, nil
	}
}

func recoveryWarning(preview activity.RecoveryPreview, discard bool, reason string) string {
	var text strings.Builder
	fmt.Fprintf(&text, "Resolve uncertainty %s · Revision %s\nObservation %s\n%s\nActor generation %s\nSegment start %s\nConfirmed prefix %s\nProposed end %s\nDiscard uncertain tail: %t\nReason %s\n", preview.Uncertainty.ID, preview.Uncertainty.Revision, preview.SnapshotRevision, attributionDetail(preview.Uncertainty.Attribution), preview.Uncertainty.Actor.Generation, utcInstant(preview.SegmentStart), rangeText(preview.ConfirmedPrefix), utcInstant(preview.ProposedEnd), discard, reason)
	if preview.DiscardedSuffix != nil {
		fmt.Fprintf(&text, "Discarded suffix %s\n", rangeText(*preview.DiscardedSuffix))
	}
	text.WriteString("Affected union before:\n")
	for _, interval := range preview.AffectedUnionBefore {
		text.WriteString(rangeText(interval) + "\n")
	}
	text.WriteString("Affected union after:\n")
	for _, interval := range preview.AffectedUnionAfter {
		text.WriteString(rangeText(interval) + "\n")
	}
	text.WriteString("Still blocked uncertainty IDs: " + strings.Join(preview.StillBlockedIDs, ", ") + "\nThe shared engine preserves confirmed evidence, unknown gaps and historical attribution. This resolves only the selected uncertainty and cannot correct a capture warning, stop another actor or automatically upload time. Review every boundary before confirming.")
	return text.String()
}

func utcInstant(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }
func rangeText(value activity.TimeRange) string {
	return utcInstant(value.Start) + " → " + utcInstant(value.End)
}

func captureReview(ctx context.Context, p *promptBridge, actions *ActivityActions) error {
	if actions.Status == nil {
		return p.View(ctx, "Capture warnings", "Capture observations unavailable.")
	}
	snapshot, err := observeView(ctx, actions.Status)
	if err != nil {
		return activityFailure(ctx, p, err)
	}
	snapshot = captureValue(snapshot)
	choices := []terminal.Choice{}
	for _, receipt := range snapshot.CaptureReviews {
		choices = append(choices, terminal.Choice{ID: receipt.ID, Label: receipt.Source + " · " + receipt.DiagnosticCode})
	}
	if len(choices) == 0 {
		return p.View(ctx, "Capture warnings", "No retained capture warnings. Absence is not proof of native delivery.")
	}
	id, err := p.Choose(ctx, "Capture warnings", choices)
	if err != nil {
		return err
	}
	for _, receipt := range snapshot.CaptureReviews {
		if receipt.ID == id {
			return p.View(ctx, "Capture warning details", fmt.Sprintf("Receipt %s\nSource %s · Session %s · Turn %s · Agent %s\nDiagnostic %s\nDisposition %s · Ordering %s · Durability %s\nOrigin %s\nProfile basis %s · Revision %s\nFingerprint %s\nObservation %s\nThis is retained capture evidence, not an interval boundary. It cannot be resolved with an end time; no observed delivery or source continuity is fabricated.", receipt.ID, receipt.Source, receipt.SessionID, receipt.TurnID, receipt.AgentID, receipt.DiagnosticCode, receipt.Disposition, receipt.Ordering, receipt.Durability, receipt.Origin, receipt.ProfileBasis, receipt.ProfileRevision, receipt.Fingerprint, utcInstant(receipt.ObservedAt)))
		}
	}
	return activityFailure(ctx, p, &activity.Error{Code: "not_found"})
}

func (c *activityController) id() string {
	if c.pending.operation == "resolve" {
		return c.pending.resolve.RequestID
	}
	return c.pending.interrupt.RequestID
}

func (c *activityController) dispatch(ctx context.Context, actions *ActivityActions) (activity.MutationResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if c.pending.operation == "resolve" && actions.Resolve != nil {
		return actions.Resolve(ctx, c.pending.resolve)
	}
	if c.pending.operation == "interrupt" && actions.Interrupt != nil {
		return actions.Interrupt(ctx, c.pending.interrupt)
	}
	return activity.MutationResult{}, &activity.Error{Code: "unsupported_contract"}
}

func (c *activityController) complete(ctx context.Context, p *promptBridge, result activity.MutationResult) error {
	id := c.id()
	c.pending = nil
	return p.View(ctx, "Activity · Complete", fmt.Sprintf("Shared activity operation completed.\nRequest ID %s\nReturned snapshot revision %s\nAffected IDs %s\nCaptured history and unrelated actor generations remain retained.", id, result.SnapshotRevision, strings.Join(result.AffectedIDs, ", ")))
}

func (c *activityController) recover(ctx context.Context, p *promptBridge, actions *ActivityActions, acknowledge bool) error {
	if acknowledge {
		if p.View(ctx, "Activity · Outcome unknown", "local_write_unknown: submitted activity outcome or durability is unknown.\nRequest ID "+c.id()+"\nPreserve the exact reviewed intent. Read-only status does not establish nonapplication; only explicit same-intent replay may establish durable completion.") != nil {
			return c.pending.err
		}
	}
	for {
		choice, err := p.Choose(ctx, "Activity · Recover submitted intent "+c.id(), []terminal.Choice{{ID: "replay", Label: "Replay exact submitted intent"}, {ID: "status", Label: "Read-only activity status"}, {ID: "back", Label: "Back; retain unknown outcome"}})
		if err != nil || choice == "back" {
			return c.pending.err
		}
		switch choice {
		case "status":
			body := "Local activity unavailable. The submitted outcome remains unknown."
			if actions.Status != nil {
				status, err := observeView(ctx, actions.Status)
				if err == nil {
					body = fmt.Sprintf("Read-only snapshot revision %s\nActors %d · Timing uncertainties %d · Capture warnings %d\nRequest ID %s\nThis observation cannot establish nonapplication or acknowledge durability.", status.SnapshotRevision, len(status.Actors), len(status.Uncertainties), len(status.CaptureReviews), c.id())
				}
			}
			if p.View(ctx, "Activity · Read-only status", body) != nil {
				return c.pending.err
			}
		case "replay":
			result, err := c.dispatch(ctx, actions)
			if err == nil {
				return c.complete(ctx, p, result)
			}
			if p.View(ctx, "Activity · Outcome unknown", "Exact replay did not establish durable completion.\nRequest ID "+c.id()+"\nNo replacement intent was sent; preserve the original reviewed request.") != nil {
				return c.pending.err
			}
		}
	}
}

func activityFailure(ctx context.Context, p *promptBridge, err error) error {
	var ended *terminal.ExitError
	if errors.As(err, &ended) || ctx.Err() != nil {
		return err
	}
	code := "operation_failed"
	var domain *activity.Error
	if errors.As(err, &domain) {
		switch domain.Code {
		case "validation", "revision_conflict", "request_conflict", "state_busy", "clock_unavailable", "clock_conflict", "recovery_bounds", "attribution_conflict", "invalid_transition", "actor_not_found", "uncertainty_not_found", "input_required", "unsupported_contract":
			code = domain.Code
		}
	}
	guidance := "No automatic retry follows. Inspect current activity; committed safety quarantine may remain even when recovery did not complete."
	if code == "revision_conflict" {
		guidance = "Reload the target and review a fresh preview. Renew confirmation with a new request ID; no stale revision was substituted."
	}
	return p.View(ctx, "Activity · Failed", code+"\n"+guidance)
}
