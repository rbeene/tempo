package cli

import (
	"bytes"
	"context"
	"encoding/json"
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
	eventKind := ""
	var admissionDiagnostic *activity.HostCaptureDiagnostics
	if d.Getenv("TEMPO_HOOK_DIAGNOSTICS") == "1" {
		admissionDiagnostic = activity.NewHostCaptureDiagnostics()
	}
	if err == nil {
		var event activity.HostEvent
		if host == "claude" {
			event, err = hooks.DecodeClaude(reader)
		} else {
			event, err = hooks.DecodeCodex(reader)
		}
		if err == nil {
			eventKind = event.Kind
		}
		if err == nil && ctx.Err() != nil {
			err = problem("validation", "hook input deadline exceeded")
		}
		if err == nil {
			service := d.Activity
			if service == nil {
				service = activity.New(activity.Options{Path: d.Getenv("TEMPO_STATE"), HookPolicies: hookstate.New(hookstate.Options{Path: d.Getenv("TEMPO_HOOK_STATE")})})
			}
			// Retry only a positively known local refusal, within the original
			// context and with the one already decoded identity. Each synchronous
			// service call completes its own checked cleanup before returning.
			for attempt := 0; attempt < 3; attempt++ {
				if attempt > 0 && ctx.Err() != nil {
					break
				}
				attemptCtx := ctx
				if admissionDiagnostic != nil {
					attemptCtx = admissionDiagnostic.Begin(ctx)
				}
				result, err = service.IngestHost(attemptCtx, event)
				retry := hookRetryableBusy(ctx, result, err)
				if admissionDiagnostic != nil {
					admissionDiagnostic.End(attemptCtx, retry)
				}
				if !retry {
					break
				}
			}
			if err == nil && result.Durability == "committed" && (result.Disposition == "applied" || result.Disposition == "duplicate") {
				notifyWorker(ctx, d, worker.Wake)
			}
		}
	}
	diagnosticCode, diagnosticDurability := "", ""
	if err != nil {
		code, durability := hookFailure(err)
		if result.Durability == "committed" || result.Durability == "unknown" {
			durability = result.Durability
		}
		diagnosticCode, diagnosticDurability = code, durability
		fmt.Fprintf(errOut, "tempo hook: %s; durability=%s\n", code, durability)
	} else if result.DiagnosticCode != "" || result.Disposition == "untracked" || result.Disposition == "review_required" {
		code := result.DiagnosticCode
		if code == "" {
			code = result.Disposition
		}
		diagnosticCode, diagnosticDurability = safeHookCode(code), result.Durability
		fmt.Fprintf(errOut, "tempo hook: %s; durability=%s\n", diagnosticCode, diagnosticDurability)
	}
	// Start diagnostics deliberately inform the agent through Codex's native
	// additionalContext field. They carry only fixed enums, never a veto or a
	// retry instruction. Other events and clean captures keep the empty object.
	if host == "codex" {
		if diagnosticCode != "" && (eventKind == "SessionStart" || eventKind == "SubagentStart") {
			durability := diagnosticDurability
			switch durability {
			case "committed", "not_committed", "unknown":
			default:
				durability = "unknown"
			}
			text := "tempo capture: kind=" + eventKind + "; code=" + diagnosticCode + "; durability=" + durability
			if admissionDiagnostic != nil && err != nil && diagnosticCode == "state_busy" && durability == "not_committed" {
				text += hookAdmissionDiagnosticContext(admissionDiagnostic)
			}
			output := map[string]any{"hookSpecificOutput": map[string]string{
				"hookEventName": eventKind, "additionalContext": text,
			}}
			if err := json.NewEncoder(out).Encode(output); err != nil {
				return 1
			}
			return 0
		}
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

// A display code is not retry authority: every branch must describe a known
// precommit local refusal. The finite walk also refuses cyclic/oversized trees.
func hookRetryableBusy(ctx context.Context, result activity.HostReceipt, err error) bool {
	if ctx.Err() != nil || err == nil || result.Durability != "not_committed" {
		return false
	}
	remaining, domainBusy := 64, false
	var visit func(error, bool) bool
	visit = func(cause error, localCancellation bool) bool {
		if cause == nil || remaining == 0 {
			return false
		}
		remaining--
		if cause == context.Canceled || cause == context.DeadlineExceeded {
			return localCancellation
		}
		if domain, ok := cause.(*activity.Error); ok {
			if domain == nil || domain.Code != "state_busy" || !domain.Retryable || domain.Uncertain {
				return false
			}
			domainBusy = true
			return true
		}
		known, safe, admissionCancellation := hookNativeRetryNode(cause)
		if known && !safe {
			return false
		}
		// Permission applies only to this checked admission node's Cause
		// subtree. A cancellation sibling never inherits it.
		localCancellation = localCancellation || admissionCancellation
		switch wrapped := cause.(type) {
		case interface{ Unwrap() []error }:
			children := wrapped.Unwrap()
			if len(children) == 0 {
				return known && safe
			}
			for _, child := range children {
				if !visit(child, localCancellation) {
					return false
				}
			}
			return true
		case interface{ Unwrap() error }:
			return visit(wrapped.Unwrap(), localCancellation)
		default:
			return known && safe
		}
	}
	return visit(err, false) && domainBusy && ctx.Err() == nil
}
