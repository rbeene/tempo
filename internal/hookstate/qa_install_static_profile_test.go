package hookstate

import (
	"context"
	"testing"
)

func TestQAInstallStaticAdmissionConflictsCannotBeDeclaredAway(t *testing.T) {
	for name, document := range map[string]string{
		"prompt-handler":       `{"hooks":{"UserPromptSubmit":[{"hooks":[{"type":"command","command":"echo PRIVATE_SETTINGS_SENTINEL"}]}]}}`,
		"continuation-handler": `{"hooks":{"Stop":[{"hooks":[{"type":"command","command":"echo PRIVATE_SETTINGS_SENTINEL"}]}]}}`,
		"async-required-event": `{"hooks":{"UserPromptSubmit":[{"hooks":[{"type":"command","command":"echo PRIVATE_SETTINGS_SENTINEL","async":true}]}]}}`,
		"disabled-hooks":       `{"disableAllHooks":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			qaInstallWrite(t, f.target("claude", "project"), []byte(document))
			s := New(f.options)
			qaInstallApply(t, s, f.intent("claude", "project"), 111)
			status := qaInstallStatus(t, s, HookSelector{Host: "claude", Scope: "project", Path: f.project})
			if status.Profile.CaptureEligible || len(status.Profile.Context.Conflicts) == 0 {
				t.Fatalf("known static conflict not exposed: %+v", status)
			}
			before := qaInstallTree(t, f.root)
			_, err := s.Confirm(context.Background(), ConfirmInput{Context: status.Profile.Context, Fingerprint: status.Profile.Fingerprint, DeclarationVersion: DeclarationVersion, RequestID: qaInstallID(112), Confirmed: true})
			if err == nil {
				t.Fatal("explicit confirmation overrode a known conflicting static handler")
			}
			qaInstallAssertUnchanged(t, f, before)
		})
	}
}
func TestQAInstallUnrelatedNotificationHandlerDoesNotBlockDeclaration(t *testing.T) {
	f := qaNewInstallFixture(t)
	qaInstallWrite(t, f.target("claude", "project"), []byte(`{"hooks":{"Notification":[{"hooks":[{"type":"command","command":"echo PRIVATE_SETTINGS_SENTINEL","async":true}]}]}}`))
	s := New(f.options)
	qaInstallApply(t, s, f.intent("claude", "project"), 113)
	status := qaInstallStatus(t, s, HookSelector{Host: "claude", Scope: "project", Path: f.project})
	if len(status.Profile.Context.Conflicts) != 0 {
		t.Fatalf("unrelated notification handler incorrectly blocks lifecycle admission: %+v", status.Profile.Context.Conflicts)
	}
	qaInstallConfirmFromStatus(t, s, HookSelector{Host: "claude", Scope: "project", Path: f.project}, 114)
}

func TestQAInstallStatusOrderingUsesFiniteContractSeparateFromProfileBasis(t *testing.T) {
	f := qaNewInstallFixture(t)
	s := New(f.options)
	selector := HookSelector{Host: "claude", Scope: "project", Path: f.project}
	check := func(status HookStatus) {
		t.Helper()
		switch status.Ordering {
		case "supported", "review_required", "unavailable":
		default:
			t.Fatalf("ordering contains evidence basis or uncontracted state %q; profile=%+v", status.Ordering, status.Profile)
		}
	}
	check(qaInstallStatus(t, s, selector))
	qaInstallApply(t, s, f.intent("claude", "project"), 115)
	check(qaInstallStatus(t, s, selector))
	qaInstallConfirmFromStatus(t, s, selector, 116)
	status := qaInstallStatus(t, s, selector)
	check(status)
	if status.Profile.Basis != "operator_declared" {
		t.Fatal("ordering normalization lost explicit policy provenance")
	}
}
