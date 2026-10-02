package hookstate

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func qaPolicyContext(t *testing.T) Context {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := Context{Host: "codex", Scope: "project", Path: root, RuntimeVersion: "0.159.3", Surface: "local", Conflicts: []string{}}
	for _, role := range []string{"runtime", "executable", "definitions"} {
		path := filepath.Join(root, role)
		if err := os.WriteFile(path, []byte("SYNTHETIC_SECRET_CONTENT_"+role), 0600); err != nil {
			t.Fatal(err)
		}
		c.Artifacts = append(c.Artifacts, Artifact{Role: role, Path: path})
	}
	return c
}

func qaPolicyConfirm(t *testing.T, s *Service, c Context, id string) Profile {
	t.Helper()
	p, err := s.Preview(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	p, err = s.Confirm(context.Background(), ConfirmInput{Context: p.Context, Fingerprint: p.Fingerprint, DeclarationVersion: DeclarationVersion, RequestID: id, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	if p.Basis != "operator_declared" || !p.CaptureEligible || p.Revision == "" {
		t.Fatalf("bad declared profile: %+v", p)
	}
	return p
}

func TestQAPolicyAbsentAndPreviewAreReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "hooks.json")
	s := New(Options{Path: path})
	c := qaPolicyContext(t)
	p, err := s.Eligibility(context.Background(), "codex", c.Path)
	if err != nil {
		t.Fatal(err)
	}
	if p.CaptureEligible || p.Basis == "host_observed" {
		t.Fatalf("absent policy grants capture: %+v", p)
	}
	first, err := s.Preview(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Preview(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if first.Fingerprint == "" || first.Fingerprint != second.Fingerprint {
		t.Fatalf("preview nondeterministic: %+v / %+v", first, second)
	}
	for _, a := range first.Context.Artifacts {
		if len(a.SHA256) != 64 {
			t.Fatalf("preview missing actual digest: %+v", a)
		}
	}
	b, _ := json.Marshal(first)
	if strings.Contains(string(b), "SYNTHETIC_SECRET_CONTENT") {
		t.Fatalf("preview retained contents: %s", b)
	}
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("preview/absent read wrote metadata: %v", err)
	}
}

func TestQAPolicyDeclarationReplayAndRevocation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata", "hooks.json")
	s := New(Options{Path: path})
	c := qaPolicyContext(t)
	preview, err := s.Preview(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	in := ConfirmInput{Context: preview.Context, Fingerprint: preview.Fingerprint, DeclarationVersion: DeclarationVersion, RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", Confirmed: true}
	first, err := s.Confirm(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if first.Basis != "operator_declared" || !first.CaptureEligible {
		t.Fatalf("wrong evidence level: %+v", first)
	}
	s = New(Options{Path: path})
	again, err := s.Confirm(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision != again.Revision || first.Fingerprint != again.Fingerprint {
		t.Fatalf("replay changed result: %+v / %+v", first, again)
	}
	conflict := RevokeInput{Host: "codex", Scope: "project", Path: c.Path, IfRevision: first.Revision, RequestID: in.RequestID, Confirmed: true}
	_, err = s.Revoke(context.Background(), conflict)
	var pe *Error
	if !errors.As(err, &pe) || pe.Code != "request_conflict" {
		t.Fatalf("cross-operation UUID reused: %v", err)
	}
	conflict.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	revoked, err := s.Revoke(context.Background(), conflict)
	if err != nil {
		t.Fatal(err)
	}
	if revoked.CaptureEligible {
		t.Fatalf("revocation eligible: %+v", revoked)
	}
	eligible, err := s.Eligibility(context.Background(), "codex", c.Path)
	if err != nil {
		t.Fatal(err)
	}
	if eligible.CaptureEligible {
		t.Fatalf("revoked profile admitted capture: %+v", eligible)
	}
	if _, err := s.Confirm(context.Background(), in); err != nil {
		t.Fatalf("original confirmation replay after revoke: %v", err)
	}
	eligible, err = s.Eligibility(context.Background(), "codex", c.Path)
	if err != nil || eligible.CaptureEligible {
		t.Fatalf("old receipt resurrected revoked policy: %+v %v", eligible, err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "SYNTHETIC_SECRET_CONTENT") {
		t.Fatal("policy persisted raw artifact content")
	}
}

func TestQAPolicyArtifactDriftCannotReviveByRestoringBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata", "hooks.json")
	s := New(Options{Path: path})
	c := qaPolicyContext(t)
	qaPolicyConfirm(t, s, c, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	artifact := c.Artifacts[0].Path
	original, err := os.ReadFile(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifact, []byte("changed runtime"), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := s.Eligibility(context.Background(), "codex", c.Path)
	if err != nil {
		t.Fatal(err)
	}
	if p.CaptureEligible {
		t.Fatal("drift retained eligibility")
	}
	if err := os.WriteFile(artifact, original, 0600); err != nil {
		t.Fatal(err)
	}
	s = New(Options{Path: path})
	p, err = s.Eligibility(context.Background(), "codex", c.Path)
	if err != nil {
		t.Fatal(err)
	}
	if p.CaptureEligible {
		t.Fatal("restored bytes revived invalidated declaration")
	}
}

func TestQAPolicyConfirmationRejectsStaleFingerprintAndKnownConflict(t *testing.T) {
	for _, kind := range []string{"missing_confirmation", "stale_artifact", "known_conflict"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metadata", "hooks.json")
			s := New(Options{Path: path})
			c := qaPolicyContext(t)
			p, err := s.Preview(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			in := ConfirmInput{Context: p.Context, Fingerprint: p.Fingerprint, DeclarationVersion: DeclarationVersion, RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", Confirmed: true}
			switch kind {
			case "missing_confirmation":
				in.Confirmed = false
			case "stale_artifact":
				if err := os.WriteFile(c.Artifacts[0].Path, []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			case "known_conflict":
				in.Conflicts = []string{"prompt_veto"}
			}
			if _, err := s.Confirm(context.Background(), in); err == nil {
				t.Fatal("unsafe declaration accepted")
			}
			eligible, err := s.Eligibility(context.Background(), "codex", c.Path)
			if err != nil {
				t.Fatal(err)
			}
			if eligible.CaptureEligible {
				t.Fatal("rejected confirmation left eligible state")
			}
		})
	}
}

func TestQAPolicyNarrowIneligibleContextShadowsAncestor(t *testing.T) {
	for _, mode := range []string{"revoked", "artifact_drift"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metadata", "hooks.json")
			s := New(Options{Path: path})
			parent := qaPolicyContext(t)
			gitCtx, gitCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer gitCancel()
			cmd := exec.CommandContext(gitCtx, "git", "-C", parent.Path, "init", "--quiet")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("synthetic git init: %v %s", err, out)
			}
			qaPolicyConfirm(t, s, parent, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
			child := parent
			child.Path = filepath.Join(parent.Path, "sub")
			if err := os.Mkdir(child.Path, 0700); err != nil {
				t.Fatal(err)
			}
			child.Artifacts = append([]Artifact(nil), parent.Artifacts...)
			child.Artifacts[0].Path = filepath.Join(child.Path, "runtime")
			if err := os.WriteFile(child.Artifacts[0].Path, []byte("child runtime"), 0600); err != nil {
				t.Fatal(err)
			}
			confirmed := qaPolicyConfirm(t, s, child, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
			if mode == "revoked" {
				if _, err := s.Revoke(context.Background(), RevokeInput{Host: "codex", Scope: "project", Path: child.Path, IfRevision: confirmed.Revision, RequestID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Confirmed: true}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(child.Artifacts[0].Path, []byte("drift"), 0600); err != nil {
					t.Fatal(err)
				}
				p, err := s.Eligibility(context.Background(), "codex", child.Path)
				if err != nil {
					t.Fatal(err)
				}
				if p.CaptureEligible {
					t.Fatalf("drift admitted via ancestor: %+v", p)
				}
				if err := os.WriteFile(child.Artifacts[0].Path, []byte("child runtime"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			s = New(Options{Path: path})
			p, err := s.Eligibility(context.Background(), "codex", child.Path)
			if err != nil {
				t.Fatal(err)
			}
			if p.CaptureEligible || p.Context.Path != child.Path {
				t.Fatalf("narrow ineligible context fell back to ancestor: %+v", p)
			}
			p, err = s.Eligibility(context.Background(), "codex", parent.Path)
			if err != nil {
				t.Fatal(err)
			}
			if !p.CaptureEligible {
				t.Fatalf("child invalidation contaminated parent: %+v", p)
			}
		})
	}
}

func TestQAPolicyIndependentGitBoundaryNeverInherits(t *testing.T) {
	s := New(Options{Path: filepath.Join(t.TempDir(), "metadata", "hooks.json")})
	parent := qaPolicyContext(t)
	qaPolicyConfirm(t, s, parent, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	child := filepath.Join(parent.Path, "independent")
	if err := os.Mkdir(child, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "git", "-C", child, "init", "--quiet").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v %s", err, out)
	}
	p, err := s.Eligibility(context.Background(), "codex", child)
	if err != nil {
		t.Fatal(err)
	}
	if p.CaptureEligible {
		t.Fatalf("inherited across independent Git boundary: %+v", p)
	}
	siblingPrefix := parent.Path + "-unrelated"
	if err := os.Mkdir(siblingPrefix, 0700); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(siblingPrefix)
	p, err = s.Eligibility(context.Background(), "codex", siblingPrefix)
	if err != nil {
		t.Fatal(err)
	}
	if p.CaptureEligible {
		t.Fatalf("string prefix mistaken for directory containment: %+v", p)
	}
}

func TestQAPolicyPreviewRejectsUnsafeArtifactAndCancellation(t *testing.T) {
	for _, mode := range []string{"symlink", "directory", "duplicate_role", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "metadata", "hooks.json")
			s := New(Options{Path: path})
			c := qaPolicyContext(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "symlink":
				link := filepath.Join(c.Path, "link")
				if err := os.Symlink(c.Artifacts[0].Path, link); err != nil {
					t.Fatal(err)
				}
				c.Artifacts[0].Path = link
			case "directory":
				c.Artifacts[0].Path = c.Path
			case "duplicate_role":
				c.Artifacts = append(c.Artifacts, c.Artifacts[0])
			case "cancelled":
				cancel()
			}
			if p, err := s.Preview(ctx, c); err == nil {
				t.Fatalf("unsafe/incomplete hashing accepted: %+v", p)
			}
			if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed preview initialized metadata: %v", err)
			}
		})
	}
}

func TestQAPolicyStalePreviewCannotUndoRevocation(t *testing.T) {
	s := New(Options{Path: filepath.Join(t.TempDir(), "metadata", "hooks.json")})
	c := qaPolicyContext(t)
	p := qaPolicyConfirm(t, s, c, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	old, err := s.Preview(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Revoke(context.Background(), RevokeInput{Host: "codex", Scope: "project", Path: c.Path, IfRevision: p.Revision, RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", Confirmed: true}); err != nil {
		t.Fatal(err)
	}
	_, err = s.Confirm(context.Background(), ConfirmInput{Context: old.Context, Fingerprint: old.Fingerprint, DeclarationVersion: DeclarationVersion, RequestID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Confirmed: true})
	if err == nil {
		t.Fatal("stale preview silently undid revocation")
	}
	fresh := qaPolicyConfirm(t, s, c, "dddddddd-dddd-4ddd-8ddd-dddddddddddd")
	if !fresh.CaptureEligible || fresh.Revision == p.Revision {
		t.Fatalf("fresh explicit declaration did not reactivate: %+v", fresh)
	}
}

func TestQAPolicyConcurrentDistinctContextsPreserveBothDeclarations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata", "hooks.json")
	contexts := []Context{qaPolicyContext(t), qaPolicyContext(t)}
	inputs := make([]ConfirmInput, 2)
	ids := []string{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}
	for i, c := range contexts {
		p, err := New(Options{Path: path}).Preview(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
		inputs[i] = ConfirmInput{Context: p.Context, Fingerprint: p.Fingerprint, DeclarationVersion: DeclarationVersion, RequestID: ids[i], Confirmed: true}
	}
	start := make(chan struct{})
	done := make(chan error, 2)
	for _, in := range inputs {
		go func(in ConfirmInput) {
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := New(Options{Path: path, LockTimeout: time.Second}).Confirm(ctx, in)
			done <- err
		}(in)
	}
	close(start)
	for range inputs {
		if err := <-done; err != nil {
			t.Fatalf("concurrent declaration: %v", err)
		}
	}
	for _, c := range contexts {
		p, err := New(Options{Path: path}).Eligibility(context.Background(), "codex", c.Path)
		if err != nil {
			t.Fatal(err)
		}
		if !p.CaptureEligible {
			t.Fatalf("concurrent metadata update lost context: %+v", p)
		}
	}
	for _, in := range inputs {
		if _, err := New(Options{Path: path}).Confirm(context.Background(), in); err != nil {
			t.Fatalf("concurrent write lost request receipt: %v", err)
		}
	}
}
