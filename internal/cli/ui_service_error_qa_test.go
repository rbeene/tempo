package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/worker"
)

func TestQAUIServiceUnknownPrimaryDiagnosticProjectsSafeTypedOutcome(t *testing.T) {
	for _, family := range []string{"hooks", "worker"} {
		t.Run(family, func(t *testing.T) {
			const raw = "RAW-SERVICE-SECRET\x1b]52;secret\a"
			var original error
			if family == "hooks" {
				original = &hookstate.Error{Code: "local_write_unknown", RequestID: "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa", Uncertain: true}
			} else {
				original = &worker.Error{Code: "local_write_unknown", Message: raw, Uncertain: true}
			}
			originalText := original.Error()
			mapped := uiResultError(original)
			safe := safeError(mapped)
			if safe.Code != "local_write_unknown" || !safe.Uncertain || safe.Retryable || exitCode(safe.Code) != 8 {
				t.Fatalf("uncertain shared classification lost: %+v", safe)
			}
			if strings.Contains(safe.Message, "RAW-SERVICE-SECRET") || strings.Contains(safe.Message, "\x1b") || !strings.Contains(strings.ToLower(safe.Message), "inspect") {
				t.Errorf("final human primary diagnostic unsafe: %q", safe.Message)
			}
			var hook *hookstate.Error
			var owned *worker.Error
			if family == "hooks" {
				if !errors.As(mapped, &hook) || hook.RequestID != "aaaaaaaa-aaaa-1aaa-8aaa-aaaaaaaaaaaa" {
					t.Error("hook typed identity lost")
				}
			} else {
				if !errors.As(mapped, &owned) {
					t.Error("worker domain type lost")
				}
			}
			if original.Error() != originalText {
				t.Error("CLI presentation changed original retained service error")
			}
		})
	}
}
