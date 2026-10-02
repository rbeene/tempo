package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
)

func TestQAUIResultErrorKeepsSharedUncertaintyAndSafeRequestIdentity(t *testing.T) {
	const request = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	shared := &activity.Error{Code: "local_write_unknown", Message: "local write durability is unknown; retry with the same identity", Uncertain: true, Details: map[string]any{"request_id": request}}
	result := uiResultError(shared)
	safe := safeError(result)
	if safe.Code != shared.Code || !safe.Uncertain || exitCode(safe.Code) != 8 || safe.Details["request_id"] != request {
		t.Fatalf("UI boundary demoted shared mutation outcome: %+v", safe)
	}
	// cli.Run's human error branch prints this message after ui.Run has closed
	// the terminal. The exact retained request identity must be recoverable.
	if !strings.Contains(safe.Message, request) {
		t.Fatalf("post-screen human diagnostic discarded replay identity: %q", safe.Message)
	}
}

func TestQAUIResultErrorPreservesTerminalCausesAndBoundsRawIO(t *testing.T) {
	for _, code := range []int{0, 130, 143} {
		ended := &terminal.ExitError{Code: code}
		if result := uiResultError(ended); result != ended {
			t.Errorf("UI boundary replaced terminal exit%d: %v", code, result)
		}
	}
	if result := uiResultError(nil); result != nil {
		t.Errorf("successful UI became failure: %v", result)
	}
	result := uiResultError(errors.New("PRIVATE raw transport \x1b]52;c;SECRET\a"))
	var ended *terminal.ExitError
	if result == nil || !errors.As(result, &ended) || ended.Code != 1 || strings.Contains(result.Error(), "PRIVATE") || strings.Contains(result.Error(), "SECRET") {
		t.Fatalf("unexpected UI IO escaped safe terminal boundary: %v", result)
	}
}
