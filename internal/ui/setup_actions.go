package ui

import (
	"context"

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
	return nil
}
