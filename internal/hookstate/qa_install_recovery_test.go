package hookstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestQAInstallUnknownResultAndPendingStatusExposeExactRecoveryIdentity(t *testing.T) {
	f := qaNewInstallFixture(t)
	qaInstallOriginals(t, f)
	s := New(f.options)
	in := qaInstallInput(t, s, f.intent("both", "project"), 151)
	crossed := false
	s.fail = func(stage string) error {
		if stage == "install_target_sync" {
			crossed = true
			return errors.New("PRIVATE_SETTINGS_SENTINEL synthetic failure")
		}
		return nil
	}
	partial, err := s.ApplyInstall(context.Background(), in)
	qaInstallError(t, err, "local_write_unknown")
	var safe *Error
	if !errors.As(err, &safe) {
		t.Fatal(err)
	}
	if !crossed || partial.RequestID != in.RequestID || safe.RequestID != in.RequestID {
		t.Fatalf("unknown outcome lost original request identity: result=%+v error=%+v", partial, safe)
	}
	before := qaInstallTree(t, f.root)
	status, err := New(f.options).Status(context.Background(), HookSelector{Host: "both", Scope: "project", Path: f.project})
	if err != nil {
		t.Fatal(err)
	}
	qaInstallAssertUnchanged(t, f, before)
	if status.RequestID != "" {
		t.Fatal("read-only status claimed a new mutation identity")
	}
	var pending *PendingInstall
	for _, row := range status.Hooks {
		if row.Pending != nil {
			if row.State != "needs_repair" {
				t.Fatalf("pending operation claims complete configuration: %+v", row)
			}
			if row.Pending.RequestID != in.RequestID || row.Pending.Fingerprint != in.Fingerprint || row.Pending.Intent != in.Intent {
				t.Fatalf("pending recovery doesn't reproduce original reviewed operation: %+v", row.Pending)
			}
			pending = row.Pending
		}
	}
	if pending == nil {
		t.Fatal("status hides exact pending recovery request")
	}
	b, _ := json.Marshal(struct {
		Partial HookList
		Status  HookList
		Error   *Error
	}{partial, status, safe})
	if bytes.Contains(b, []byte("PRIVATE_SETTINGS_SENTINEL")) {
		t.Fatal("safe recovery identity exposed raw foreign config or error")
	}
	resumed, err := New(f.options).ApplyInstall(context.Background(), ApplyInstallInput{Intent: pending.Intent, Fingerprint: pending.Fingerprint, RequestID: pending.RequestID, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.RequestID != in.RequestID {
		t.Fatal("recovery minted new operation identity")
	}
	status, err = New(f.options).Status(context.Background(), HookSelector{Host: "both", Scope: "project", Path: f.project})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range status.Hooks {
		if row.Pending != nil {
			t.Fatal("completed exact replay left unresolved operation")
		}
	}
}

// Test-only export permits external integration QA to trigger a genuine shared
// service failure without a production fault flag or synthetic host behavior.
func QAInstallSetFault(s *Service, fail func(string) error) { s.fail = fail }
