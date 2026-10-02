package ui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
)

// SetupActions delegates the guided wizard to the existing shared service.
type SetupActions struct {
	Run func(context.Context, setup.Input, terminal.Prompter) (setup.Status, error)
}

type setupCompletion struct {
	status setup.Status
	err    error
}

type setupController struct {
	completed *setupCompletion
	pending   error
}

func (c *setupController) run(ctx context.Context, p *promptBridge, actions *SetupActions, views *ReadViews) error {
	if c.pending != nil {
		return c.inspect(ctx, p, views)
	}
	id, err := p.Choose(ctx, "Setup", []terminal.Choice{{ID: "readiness", Label: "Inspect local readiness"}, {ID: "guided", Label: "Run guided setup"}, {ID: "back", Label: "Back"}})
	if err != nil || id == "back" {
		return err
	}
	if id == "readiness" {
		return setupReadiness(ctx, p, views)
	}
	if id != "guided" || actions == nil || actions.Run == nil {
		return p.View(ctx, "Setup", "Guided setup unavailable. Inspect readiness and retained outcomes before starting new changes.")
	}
	operationCtx, end := context.WithTimeout(ctx, 2*time.Minute)
	status, outcome := actions.Run(operationCtx, setup.Input{}, p)
	end()
	if status.Steps != nil {
		status.Steps = append([]setup.Step{}, status.Steps...)
	}
	for i := range status.Steps {
		if status.Steps[i].RequiredFields != nil {
			status.Steps[i].RequiredFields = append([]string{}, status.Steps[i].RequiredFields...)
		}
	}
	// Even a canceled presentation cannot erase the shared wizard's exact
	// acknowledged steps or an unresolved write returned during shutdown.
	c.completed = &setupCompletion{status: status, err: outcome}
	if unknownOutcome(outcome) {
		c.pending = outcome
		if p.View(ctx, "Setup · Outcome unknown", setupDetails(status)+"\nThe write outcome is unknown. Inspect readiness; do not rerun guided setup or replace the submitted changes.") != nil {
			return c.pending
		}
		return c.inspect(ctx, p, views)
	}
	body := setupDetails(status)
	if outcome != nil {
		body += "\nSetup stopped before all steps completed. Earlier completed steps remain applied; inspect readiness before making further changes."
	}
	return p.View(ctx, "Setup · Result", body)
}

func (c *setupController) inspect(ctx context.Context, p *promptBridge, views *ReadViews) error {
	for {
		id, err := p.Choose(ctx, "Setup · Outcome unknown", []terminal.Choice{{ID: "readiness", Label: "Inspect local readiness"}, {ID: "back", Label: "Back"}})
		if err != nil || id != "readiness" {
			return c.pending
		}
		if setupReadiness(ctx, p, views) != nil {
			return c.pending
		}
	}
}

func setupReadiness(ctx context.Context, p *promptBridge, views *ReadViews) error {
	if views == nil || views.Setup == nil {
		return p.View(ctx, "Setup · Readiness", "Local setup readiness unavailable.")
	}
	status, err := observeView(ctx, views.Setup)
	if err != nil {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		return p.View(ctx, "Setup · Readiness", "Local setup readiness unavailable; inspect configuration before checking again. Inspection does not prove nonapplication of an earlier write.")
	}
	return p.View(ctx, "Setup · Readiness", setupDetails(status)+"\nRead-only observation; readiness does not establish nonapplication of an earlier write.")
}

func setupDetails(status setup.Status) string {
	var body strings.Builder
	for _, step := range status.Steps {
		fmt.Fprintf(&body, "%s · %s\n%s\n", step.Action, step.State, step.SafeMessage)
		if len(step.RequiredFields) != 0 {
			fmt.Fprintf(&body, "Required: %s\n", strings.Join(step.RequiredFields, ", "))
		}
		body.WriteByte('\n')
	}
	body.WriteString("Credential storage, capture mapping, host trust/delivery, upload consent and worker ownership are separate.")
	return body.String()
}

func setupRequestID(outcome error) string {
	var local *activity.Error
	if errors.As(outcome, &local) {
		id, _ := local.Details["request_id"].(string)
		return id
	}
	var hooks *hookstate.Error
	if errors.As(outcome, &hooks) {
		return hooks.RequestID
	}
	return ""
}
