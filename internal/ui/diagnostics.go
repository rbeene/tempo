package ui

import (
	"context"

	"github.com/rbeene/tempo/internal/setup"
)

func showDiagnostics(ctx context.Context, p *promptBridge, read func(context.Context, bool) (setup.Diagnostics, error)) error {
	return nil
}
