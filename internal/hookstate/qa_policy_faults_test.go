package hookstate

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func qaPolicyInput(t *testing.T, s *Service, c Context, id string) ConfirmInput {
	t.Helper()
	p, err := s.Preview(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	return ConfirmInput{Context: p.Context, Fingerprint: p.Fingerprint, DeclarationVersion: DeclarationVersion, RequestID: id, Confirmed: true}
}
func qaPolicyError(t *testing.T, err error, code string) {
	t.Helper()
	var pe *Error
	if !errors.As(err, &pe) || pe.Code != code {
		t.Fatalf("want %s, got %#v", code, err)
	}
}

func TestQAPolicyDefiniteWriteFailurePreservesOriginalAndCanRetry(t *testing.T) {
	for _, stage := range []string{"before_write", "file_sync", "rename"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metadata", "hooks.json")
			s := New(Options{Path: path})
			c := qaPolicyContext(t)
			p := qaPolicyConfirm(t, s, c, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			s.fail = func(at string) error {
				if at == stage {
					return errors.New("synthetic private fault")
				}
				return nil
			}
			in := RevokeInput{Host: "codex", Scope: "project", Path: c.Path, IfRevision: p.Revision, RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", Confirmed: true}
			_, err = s.Revoke(context.Background(), in)
			if err == nil {
				t.Fatal("failed write acknowledged")
			}
			var pe *Error
			if !errors.As(err, &pe) || pe.Uncertain {
				t.Fatalf("pre-rename failure misreported: %#v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatal("failed mutation changed existing policy/ledger bytes")
			}
			s = New(Options{Path: path})
			p, err = s.Eligibility(context.Background(), "codex", c.Path)
			if err != nil || !p.CaptureEligible {
				t.Fatalf("failed revoke changed eligible policy: %+v %v", p, err)
			}
			p, err = s.Revoke(context.Background(), in)
			if err != nil || p.CaptureEligible {
				t.Fatalf("same exact request could not apply after definite failure: %+v %v", p, err)
			}
		})
	}
}

func TestQAPolicyUnknownCommitReplaysBeforeArtifactReadAndSyncsAgain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata", "hooks.json")
	s := New(Options{Path: path})
	c := qaPolicyContext(t)
	in := qaPolicyInput(t, s, c, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	s.fail = func(stage string) error {
		if stage == "directory_sync" {
			return errors.New("synthetic directory sync fault")
		}
		return nil
	}
	original, err := s.Confirm(context.Background(), in)
	qaPolicyError(t, err, "local_write_unknown")
	visible, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("unknown commit must have visible receipt: %v", err)
	}
	for _, a := range c.Artifacts {
		if err := os.Remove(a.Path); err != nil {
			t.Fatal(err)
		}
	}
	_, err = s.Confirm(context.Background(), in)
	qaPolicyError(t, err, "local_write_unknown")
	s = New(Options{Path: path})
	replayed, err := s.Confirm(context.Background(), in)
	if err != nil {
		t.Fatalf("durable replay rehashed removed artifacts: %v", err)
	}
	if !reflect.DeepEqual(original, replayed) {
		t.Fatalf("unknown replay changed original result: %+v %+v", original, replayed)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(visible) != string(after) {
		t.Fatal("replay allocated new profile revision/receipt")
	}
	in.Confirmed = false
	_, err = s.Confirm(context.Background(), in)
	qaPolicyError(t, err, "request_conflict")
}

func TestQAPolicyCorruptTypedLedgerFailsClosedWithoutOverwrite(t *testing.T) {
	for _, variant := range []string{"wrong_operation", "future_result_revision", "wrong_provenance", "missing_profile", "same_revision_conflicting_result"} {
		t.Run(variant, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metadata", "hooks.json")
			s := New(Options{Path: path})
			c := qaPolicyContext(t)
			in := qaPolicyInput(t, s, c, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
			if _, err := s.Confirm(context.Background(), in); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var d metadata
			if err := json.Unmarshal(raw, &d); err != nil {
				t.Fatal(err)
			}
			r := d.Requests[in.RequestID]
			switch variant {
			case "wrong_operation":
				r.Operation = "hooks.revoke-profile"
			case "future_result_revision":
				r.Result.Revision = "999"
				r.Result.Fingerprint = profileFingerprint(r.Result.Context, r.Result.Revision)
			case "wrong_provenance":
				r.Result.Basis = "host_observed"
			case "missing_profile":
				d.Profiles = map[string]Profile{}
			case "same_revision_conflicting_result":
				r.Result.Context.Artifacts = append([]Artifact(nil), r.Result.Context.Artifacts...)
				r.Result.Context.Artifacts[0].SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
				r.Result.Fingerprint = profileFingerprint(r.Result.Context, r.Result.Revision)
			}
			d.Requests[in.RequestID] = r
			corrupt, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, corrupt, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = New(Options{Path: path}).Confirm(context.Background(), in)
			qaPolicyError(t, err, "state_corrupt")
			preserved, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(preserved) != string(corrupt) {
				t.Fatal("corrupt typed ledger was rewritten")
			}
		})
	}
}
