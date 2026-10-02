package ui

import (
	"context"
	"strings"
	"time"

	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
)

func showDiagnostics(ctx context.Context, p *promptBridge, read func(context.Context, bool) (setup.Diagnostics, error)) error {
	choice, err := p.Choose(ctx, "Diagnostics", []terminal.Choice{{ID: "local", Label: "Inspect local readiness"}, {ID: "check", Label: "Explicitly verify credentials and account access"}, {ID: "back", Label: "Back"}})
	if err != nil || choice == "back" {
		return err
	}
	if read == nil || choice != "local" && choice != "check" {
		return p.View(ctx, "Diagnostics", "Requested diagnostic inspection unavailable.")
	}
	check := choice == "check"
	budget := 250 * time.Millisecond
	if check {
		budget = 2 * time.Minute
	}
	readCtx, end := context.WithTimeout(ctx, budget)
	result, readErr := read(readCtx, check)
	end()
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	var body strings.Builder
	for _, item := range result.Items {
		body.WriteString(item.Code + " · " + item.Severity + "\n" + item.SafeMessage + "\n")
		if item.Action != nil {
			body.WriteString("Action: " + *item.Action + "\n")
		}
		body.WriteString("\n")
	}
	if readErr != nil {
		body.WriteString("Diagnostic inspection unavailable. Completed local observations remain shown; account access and real delivery remain unverified.\n")
	}
	if body.Len() == 0 {
		body.WriteString("No diagnostic observations returned.")
	}
	return p.View(ctx, "Diagnostics · Read-only observations", body.String())
}
