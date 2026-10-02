package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/hooks"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/worker"
)

// The margin is for process startup and the host's one-second Interrupt default.
const hookBudget = 900 * time.Millisecond

func runHook(ctx context.Context, host string, in io.Reader, out, errOut io.Writer, d Dependencies) int {
	ctx, cancel := context.WithTimeout(ctx, hookBudget)
	defer cancel()
	reader, cleanup, err := hookInput(ctx, in)
	if cleanup != nil {
		defer cleanup()
	}
	result := activity.HostReceipt{Durability: "not_committed"}
	if err == nil {
		var event activity.HostEvent
		if host == "claude" {
			event, err = hooks.DecodeClaude(reader)
		} else {
			event, err = hooks.DecodeCodex(reader)
		}
		if err == nil && ctx.Err() != nil {
			err = problem("validation", "hook input deadline exceeded")
		}
		if err == nil {
			service := d.Activity
			if service == nil {
				service = activity.New(activity.Options{Path: d.Getenv("TEMPO_STATE"), HookPolicies: hookstate.New(hookstate.Options{Path: d.Getenv("TEMPO_HOOK_STATE")})})
			}
			result, err = service.IngestHost(ctx, event)
			if err == nil && result.Durability == "committed" && (result.Disposition == "applied" || result.Disposition == "duplicate") {
				notifyWorker(ctx, d, worker.Wake)
			}
		}
	}
	if err != nil {
		code, durability := hookFailure(err)
		if result.Durability == "committed" || result.Durability == "unknown" {
			durability = result.Durability
		}
		fmt.Fprintf(errOut, "tempo hook: %s; durability=%s\n", code, durability)
	} else if result.DiagnosticCode != "" || result.Disposition == "untracked" || result.Disposition == "review_required" {
		code := result.DiagnosticCode
		if code == "" {
			code = result.Disposition
		}
		fmt.Fprintf(errOut, "tempo hook: %s; durability=%s\n", safeHookCode(code), result.Durability)
	}
	// No control fields, approvals, context, or Tempo envelope enter host output.
	if host == "codex" {
		if _, writeErr := io.WriteString(out, "{}\n"); writeErr != nil {
			return 1
		}
	}
	return 0
}

func hookInput(ctx context.Context, in io.Reader) (io.Reader, func(), error) {
	if ctx.Err() != nil {
		return nil, nil, problem("validation", "hook input deadline exceeded")
	}
	switch r := in.(type) {
	case *os.File:
		deadline, _ := ctx.Deadline()
		reader, cleanup, err := deadlineActivityFile(r, deadline)
		if err != nil {
			return nil, nil, problem("validation", "cannot bound hook input")
		}
		return reader, cleanup, nil
	case *strings.Reader, *bytes.Reader, *bytes.Buffer:
		return in, nil, nil
	default:
		// An opaque Reader may ignore cancellation indefinitely. Do not launch an
		// unbounded read goroutine that survives the embedded Run call.
		return nil, nil, problem("validation", "hook input requires a finite buffer or file")
	}
}

func hookFailure(err error) (string, string) {
	code := safeError(err).Code
	var policyErr *hookstate.Error
	if errors.As(err, &policyErr) {
		code = policyErr.Code
	}
	durability := "not_committed"
	if code == "local_write_unknown" {
		durability = "unknown"
	}
	return safeHookCode(code), durability
}

func safeHookCode(code string) string {
	switch code {
	case "validation", "unsupported_contract", "state_corrupt", "state_busy", "local_write_unknown", "clock_unavailable", "clock_conflict", "binding_unavailable", "event_conflict", "event_gap", "ordering_unavailable", "profile_required", "profile_invalidated", "profile_revoked", "untracked", "review_required", "source_lost", "restart_unknown", "incomplete_wait", "source_loss_while_waiting":
		return code
	default:
		return "internal"
	}
}
