package hookstate

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func qaInstallStatus(t *testing.T, s *Service, selector HookSelector) HookStatus {
	t.Helper()
	result, err := s.Status(context.Background(), selector)
	if err != nil {
		t.Fatal(err)
	}
	if result.ContractVersion != 1 || len(result.Hooks) != 1 {
		t.Fatalf("expected one status: %+v", result)
	}
	return result.Hooks[0]
}
func qaInstallConfirmFromStatus(t *testing.T, s *Service, selector HookSelector, n int) Profile {
	t.Helper()
	status := qaInstallStatus(t, s, selector)
	if status.Profile.Fingerprint == "" || status.Profile.DeclarationVersion != DeclarationVersion {
		t.Fatalf("status lacks current explicit confirmation inputs: %+v", status.Profile)
	}
	result, err := s.Confirm(context.Background(), ConfirmInput{Context: status.Profile.Context, Fingerprint: status.Profile.Fingerprint, DeclarationVersion: status.Profile.DeclarationVersion, RequestID: qaInstallID(n), Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Basis != "operator_declared" || !result.CaptureEligible {
		t.Fatalf("explicit declaration not retained: %+v", result)
	}
	return result
}
func TestQAInstallStatusAndVerifyArePureWithoutInstallation(t *testing.T) {
	f := qaNewInstallFixture(t)
	s := New(f.options)
	selector := HookSelector{Host: "both", Scope: "project", Path: f.project}
	before := qaInstallTree(t, f.root)
	status, err := s.Status(context.Background(), selector)
	if err != nil {
		t.Fatal(err)
	}
	verify, err := s.Verify(context.Background(), selector)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Hooks) != 2 || !reflect.DeepEqual(status, verify) {
		t.Fatalf("read-only verification inconsistent: %+v / %+v", status, verify)
	}
	for _, row := range status.Hooks {
		if row.State != "not_installed" || row.Profile.CaptureEligible || row.LastRealEvent != nil {
			t.Fatalf("absent installation promoted: %+v", row)
		}
	}
	qaInstallAssertUnchanged(t, f, before)
}
func TestQAInstallStatusSeparatesDeclarationFromDelivery(t *testing.T) {
	for _, host := range []string{"codex", "claude"} {
		for _, scope := range []string{"project", "user"} {
			t.Run(host+"/"+scope, func(t *testing.T) {
				f := qaNewInstallFixture(t)
				s := New(f.options)
				qaInstallApply(t, s, f.intent(host, scope), 61)
				selector := HookSelector{Host: host, Scope: scope, Path: f.project}
				before := qaInstallTree(t, f.root)
				status := qaInstallStatus(t, s, selector)
				if status.State == "receiving" || status.Profile.CaptureEligible || status.LastRealEvent != nil || status.Profile.Basis == "host_observed" {
					t.Fatalf("installed profile promoted before confirmation: %+v", status)
				}
				qaInstallAssertUnchanged(t, f, before)
				confirmed := qaInstallConfirmFromStatus(t, s, selector, 62)
				before = qaInstallTree(t, f.root)
				status = qaInstallStatus(t, New(f.options), selector)
				if !status.Profile.CaptureEligible || status.Profile.Basis != "operator_declared" || status.Profile.Revision != confirmed.Revision || status.State == "receiving" || status.LastRealEvent != nil {
					t.Fatalf("retained eligibility/delivery dimensions incorrect: %+v", status)
				}
				verified, err := New(f.options).Verify(context.Background(), selector)
				if err != nil || len(verified.Hooks) != 1 || verified.Hooks[0].LastRealEvent != nil || verified.Hooks[0].State == "receiving" {
					t.Fatalf("verify fabricated native receipt: %+v %v", verified, err)
				}
				qaInstallAssertUnchanged(t, f, before)
				eligible, err := New(f.options).Eligibility(context.Background(), host, f.project)
				if err != nil || !eligible.CaptureEligible {
					t.Fatalf("normal fresh adapter admission cannot inherit declaration: %+v %v", eligible, err)
				}
			})
		}
	}
}
func TestQAInstallUserPolicyRequiresAndRetainsExactProjectContext(t *testing.T) {
	f := qaNewInstallFixture(t)
	s := New(f.options)
	qaInstallApply(t, s, f.intent("codex", "user"), 63)
	status := qaInstallStatus(t, s, HookSelector{Host: "codex", Scope: "user"})
	if status.Profile.CaptureEligible || status.Profile.Fingerprint != "" {
		t.Fatalf("context-free user install offered broad declaration: %+v", status.Profile)
	}
	qaInstallConfirmFromStatus(t, s, HookSelector{Host: "codex", Scope: "user", Path: f.project}, 64)
	other := filepath.Join(f.root, "unrelated-project")
	if err := os.Mkdir(other, 0700); err != nil {
		t.Fatal(err)
	}
	eligible, err := New(f.options).Eligibility(context.Background(), "codex", other)
	if err != nil {
		t.Fatal(err)
	}
	if eligible.CaptureEligible {
		t.Fatalf("user policy leaked into unrelated context: %+v", eligible)
	}
}
func TestQAInstallProfileConfirmationRejectsUncheckedStaleAndUnknownVersion(t *testing.T) {
	for _, invalid := range []string{"unchecked", "stale", "version"} {
		t.Run(invalid, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			s := New(f.options)
			qaInstallApply(t, s, f.intent("claude", "project"), 65)
			status := qaInstallStatus(t, s, HookSelector{Host: "claude", Scope: "project", Path: f.project})
			in := ConfirmInput{Context: status.Profile.Context, Fingerprint: status.Profile.Fingerprint, DeclarationVersion: status.Profile.DeclarationVersion, RequestID: qaInstallID(66), Confirmed: true}
			code := "validation"
			switch invalid {
			case "unchecked":
				in.Confirmed = false
				code = "confirmation_required"
			case "stale":
				qaInstallWrite(t, f.executable, []byte("new executable since approval preview"))
				code = "revision_conflict"
			case "version":
				in.DeclarationVersion = "future-declaration"
			}
			before := qaInstallTree(t, f.root)
			_, err := s.Confirm(context.Background(), in)
			qaInstallError(t, err, code)
			qaInstallAssertUnchanged(t, f, before)
		})
	}
}
func TestQAInstallStatusDetectsDriftWithoutPersistingOrPromoting(t *testing.T) {
	for _, changed := range []string{"definitions", "skill", "executable", "runtime"} {
		t.Run(changed, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			s := New(f.options)
			qaInstallApply(t, s, f.intent("codex", "project"), 67)
			selector := HookSelector{Host: "codex", Scope: "project", Path: f.project}
			qaInstallConfirmFromStatus(t, s, selector, 68)
			path := f.target("codex", "project")
			switch changed {
			case "skill":
				path = f.skill("codex", "project")
			case "executable":
				path = f.executable
			case "runtime":
				path = filepath.Join(f.root, "inert-runtime")
			}
			qaInstallWrite(t, path, append(qaInstallRead(t, path), []byte(" \n")...))
			before := qaInstallTree(t, f.root)
			status := qaInstallStatus(t, New(f.options), selector)
			if status.Profile.CaptureEligible || status.Profile.State != "invalidated" || status.LastRealEvent != nil || status.State == "receiving" {
				t.Fatalf("known drift retained eligibility: %+v", status)
			}
			qaInstallAssertUnchanged(t, f, before)
		})
	}
}
func TestQAInstallRevokeAndUninstallDisableRetainedPolicy(t *testing.T) {
	for _, operation := range []string{"revoke", "uninstall"} {
		t.Run(operation, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			s := New(f.options)
			intent := f.intent("claude", "project")
			qaInstallApply(t, s, intent, 69)
			selector := HookSelector{Host: "claude", Scope: "project", Path: f.project}
			profile := qaInstallConfirmFromStatus(t, s, selector, 70)
			if operation == "revoke" {
				_, err := s.Revoke(context.Background(), RevokeInput{Host: "claude", Scope: "project", Path: f.project, IfRevision: profile.Revision, RequestID: qaInstallID(71), Confirmed: true})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				intent.Operation = "uninstall"
				qaInstallApply(t, s, intent, 71)
			}
			status := qaInstallStatus(t, New(f.options), selector)
			if status.Profile.CaptureEligible || status.LastRealEvent != nil {
				t.Fatalf("%s retained active policy: %+v", operation, status)
			}
			eligible, err := New(f.options).Eligibility(context.Background(), "claude", f.project)
			if err != nil {
				t.Fatal(err)
			}
			if eligible.CaptureEligible {
				t.Fatalf("%s did not block adapter admission", operation)
			}
		})
	}
}
