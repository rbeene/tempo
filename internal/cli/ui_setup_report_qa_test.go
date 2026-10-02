package cli

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/setup"
)

func TestQAUISetupReportProjectsOriginalPartialStepsAndSafeTypedOutcome(t *testing.T) {
	for _, mode := range []string{"known-partial", "known-failure", "auth-unknown", "later-link-unknown"} {
		t.Run(mode, func(t *testing.T) {
			status := setup.Status{ContractVersion: 1, Complete: false, Steps: []setup.Step{{Action: "auth.login", State: "complete", RequiredFields: []string{}, SafeMessage: "RAW-SETUP-SECRET-CANARY\x1b]52;c;CANARY\a"}, {Action: "bindings.link", State: "input_required", RequiredFields: []string{"project_id", "task_id", "timezone"}, SafeMessage: "RAW-SETUP-SECRET-CANARY"}, {Action: "RAW-SETUP-SECRET-CANARY", State: "RAW-SETUP-SECRET-CANARY", RequiredFields: []string{"RAW-SETUP-SECRET-CANARY"}, SafeMessage: "RAW-SETUP-SECRET-CANARY"}}}
			var outcome error
			if mode == "known-failure" {
				outcome = &harvest.Error{Code: "network", Message: "RAW-SETUP-SECRET-CANARY"}
			}
			if mode == "auth-unknown" {
				status.Steps[0].Action, status.Steps[0].State = "auth.status", "input_required"
				outcome = &auth.Error{Code: "credential_write_unknown", Message: "RAW-SETUP-SECRET-CANARY", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}
			}
			if mode == "later-link-unknown" {
				outcome = &activity.Error{Code: "local_write_unknown", Message: "RAW-SETUP-SECRET-CANARY", Uncertain: true, Details: map[string]any{"request_id": "12345678-1234-1234-1234-123456789abc", "untrusted_secret": "RAW-SETUP-SECRET-CANARY"}}
			}
			before, _ := json.Marshal(status)
			var out bytes.Buffer
			uiSetupReport(&out, status, outcome)
			text := out.String()
			for _, needle := range []string{"setup", "bindings.link", "input_required", "project_id", "task_id", "timezone"} {
				if !strings.Contains(text, needle) {
					t.Errorf("safe Setup report omitted original %q: %q", needle, text)
				}
			}
			if mode == "auth-unknown" {
				for _, needle := range []string{"auth.status", "credential_write_unknown", "credential unknown", "config unknown"} {
					if !strings.Contains(text, needle) {
						t.Errorf("unknown Setup status/effects omitted %q", needle)
					}
				}
				if strings.Contains(text, "auth.login") {
					t.Error("Setup report fabricated acknowledged credential commit")
				}
			} else {
				if !strings.Contains(text, "auth.login") || !strings.Contains(text, "complete") {
					t.Error("Setup report hid known completed auth.login step")
				}
			}
			if mode == "later-link-unknown" && (!strings.Contains(text, "local_write_unknown") || !strings.Contains(text, "12345678-1234-1234-1234-123456789abc")) {
				t.Error("Setup report lost original backend mutation identity")
			}
			if strings.Contains(text, "RAW-") || strings.ContainsRune(text, '\x1b') || mode != "later-link-unknown" && strings.Contains(text, "request ID:") || strings.Contains(text, "schema_version") || strings.Contains(text, "rollback") {
				t.Errorf("Setup report leaked raw data or fabricated identity/effects/envelope: %q", text)
			}
			after, _ := json.Marshal(status)
			if !reflect.DeepEqual(before, after) {
				t.Error("CLI projection changed original Setup Steps")
			}
			if failure, ok := outcome.(*auth.Error); ok && (!failure.Uncertain || failure.Message != "RAW-SETUP-SECRET-CANARY") {
				t.Error("CLI projection mutated original typed uncertainty")
			}
		})
	}
}
