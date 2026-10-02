package hookstate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func profileKey(host, scope, path string) string {
	b, _ := json.Marshal([]string{host, scope, path})
	return string(b)
}
func digest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func profileFingerprint(c Context, revision string) string {
	return digest(struct {
		Context  Context
		Revision string
	}{c, revision})
}
func number(s string) (uint64, bool) {
	if s == "" || len(s) > 1 && s[0] == '0' || strings.Trim(s, "0123456789") != "" {
		return 0, false
	}
	n, err := strconv.ParseUint(s, 10, 64)
	return n, err == nil && n != ^uint64(0)
}
func next(s string) string { n, _ := number(s); return strconv.FormatUint(n+1, 10) }
func uuid(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func (s *Service) replay(ctx context.Context, id, operation, fingerprint string) (Profile, bool, error) {
	disk, _, err := s.read(ctx)
	if err != nil {
		return Profile{}, false, err
	}
	if _, exists := disk.InstallRequests[id]; exists {
		return Profile{}, true, problem("request_conflict")
	}
	r, ok := disk.Requests[id]
	if !ok {
		return Profile{}, false, nil
	}
	if r.Operation != operation || r.Fingerprint != fingerprint {
		return Profile{}, true, problem("request_conflict")
	}
	// Re-enter the single metadata lock to reconcile visible unknown durability.
	err = s.update(ctx, func(d *metadata) (bool, error) {
		current, ok := d.Requests[id]
		if !ok || current.Operation != operation || current.Fingerprint != fingerprint {
			return false, problem("request_conflict")
		}
		r = current
		return false, nil
	})
	return r.Result, true, err
}
func (s *Service) confirm(ctx context.Context, in ConfirmInput) (Profile, error) {
	if !uuid(in.RequestID) {
		return Profile{}, problem("validation")
	}
	const operation = "hooks.confirm-profile"
	fingerprint := digest(struct {
		Operation string
		Input     ConfirmInput
	}{operation, in})
	if old, found, err := s.replay(ctx, in.RequestID, operation, fingerprint); found || err != nil {
		return old, err
	}
	if !in.Confirmed {
		return Profile{}, problem("confirmation_required")
	}
	if in.DeclarationVersion != DeclarationVersion || len(in.Conflicts) != 0 {
		return Profile{}, problem("validation")
	}
	p, err := s.Preview(ctx, in.Context)
	if err != nil {
		return Profile{}, err
	}
	if in.Fingerprint != p.Fingerprint {
		return Profile{}, problem("revision_conflict")
	}
	var result Profile
	err = s.update(ctx, func(d *metadata) (bool, error) {
		if _, exists := d.InstallRequests[in.RequestID]; exists {
			return false, problem("request_conflict")
		}
		if old, ok := d.Requests[in.RequestID]; ok {
			if old.Operation != operation || old.Fingerprint != fingerprint {
				return false, problem("request_conflict")
			}
			result = old.Result
			return false, nil
		}
		current, err := sampleContext(ctx, in.Context)
		if err != nil {
			return false, err
		}
		key := profileKey(current.Host, current.Scope, current.Path)
		revision := "0"
		if old, ok := d.Profiles[key]; ok {
			revision = old.Revision
		}
		if in.Fingerprint != profileFingerprint(current, revision) {
			return false, problem("revision_conflict")
		}
		revision = next(revision)
		result = Profile{Basis: "operator_declared", State: "eligible", Revision: revision, Fingerprint: profileFingerprint(current, revision), DeclarationVersion: DeclarationVersion, CaptureEligible: true, Context: current, DiagnosticCode: "operator_declared_risk"}
		d.Profiles[key] = result
		d.Requests[in.RequestID] = policyRequest{Operation: operation, Fingerprint: fingerprint, Result: result}
		return true, nil
	})
	return result, err
}
func (s *Service) revoke(ctx context.Context, in RevokeInput) (Profile, error) {
	if !uuid(in.RequestID) {
		return Profile{}, problem("validation")
	}
	const operation = "hooks.revoke-profile"
	fingerprint := digest(struct {
		Operation string
		Input     RevokeInput
	}{operation, in})
	if old, found, err := s.replay(ctx, in.RequestID, operation, fingerprint); found || err != nil {
		return old, err
	}
	if !in.Confirmed {
		return Profile{}, problem("confirmation_required")
	}
	if in.Host != "codex" && in.Host != "claude" || in.Scope != "project" && in.Scope != "user" || !filepath.IsAbs(in.Path) || !safeText(in.Path, 4096) {
		return Profile{}, problem("validation")
	}
	if n, ok := number(in.IfRevision); !ok || n == 0 {
		return Profile{}, problem("validation")
	}
	path, err := filepath.EvalSymlinks(in.Path)
	if err != nil {
		return Profile{}, problem("binding_unavailable")
	}
	key := profileKey(in.Host, in.Scope, path)
	disk, exists, err := s.read(ctx)
	if err != nil {
		return Profile{}, err
	}
	if _, ok := disk.Profiles[key]; !exists || !ok {
		return Profile{}, problem("not_found")
	}
	var result Profile
	err = s.update(ctx, func(d *metadata) (bool, error) {
		if _, exists := d.InstallRequests[in.RequestID]; exists {
			return false, problem("request_conflict")
		}
		if old, ok := d.Requests[in.RequestID]; ok {
			if old.Operation != operation || old.Fingerprint != fingerprint {
				return false, problem("request_conflict")
			}
			result = old.Result
			return false, nil
		}
		p, ok := d.Profiles[key]
		if !ok || p.Revision != in.IfRevision {
			return false, problem("revision_conflict")
		}
		p.State, p.CaptureEligible, p.DiagnosticCode = "revoked", false, "profile_revoked"
		p.Revision = next(p.Revision)
		p.Fingerprint = profileFingerprint(p.Context, p.Revision)
		d.Profiles[key] = p
		result = p
		d.Requests[in.RequestID] = policyRequest{Operation: operation, Fingerprint: fingerprint, Result: result}
		return true, nil
	})
	return result, err
}
func contains(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}
func applicable(root, cwd string) bool {
	if !contains(root, cwd) {
		return false
	}
	for p := cwd; p != root; p = filepath.Dir(p) {
		if _, err := os.Lstat(filepath.Join(p, ".git")); !errors.Is(err, os.ErrNotExist) {
			return false
		}
	}
	return true
}
func (s *Service) eligibility(ctx context.Context, disk *metadata, host, cwd string) (Profile, error) {
	if host != "codex" && host != "claude" || !filepath.IsAbs(cwd) || !safeText(cwd, 4096) {
		return Profile{}, problem("validation")
	}
	path, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		return Profile{}, problem("binding_unavailable")
	}
	selected := Profile{Basis: "none", State: "absent", Revision: "0", DiagnosticCode: "profile_required"}
	for _, p := range disk.Profiles {
		if p.Context.Host == host && (p.Context.InventoryVersion == "" && applicable(p.Context.Path, path) || p.Context.InventoryVersion == installedInventoryVersion && p.Context.Path == path) && (len(p.Context.Path) > len(selected.Context.Path) || len(p.Context.Path) == len(selected.Context.Path) && p.Context.Scope == "project") {
			selected = p
		}
	}
	if !selected.CaptureEligible {
		return selected, nil
	}
	current, hashErr := sampleContext(ctx, selected.Context)
	if selected.Context.InventoryVersion == installedInventoryVersion && !s.normalHostRoot(host) {
		hashErr = problem("unsupported_contract")
	}
	if hashErr == nil && contextFingerprint(current) == contextFingerprint(selected.Context) {
		return selected, nil
	}
	if ctx.Err() != nil {
		return Profile{}, problem("state_busy")
	}
	key := profileKey(selected.Context.Host, selected.Context.Scope, selected.Context.Path)
	err = s.update(ctx, func(d *metadata) (bool, error) {
		p, ok := d.Profiles[key]
		if !ok || p.Revision != selected.Revision {
			return false, problem("revision_conflict")
		}
		p.State, p.CaptureEligible, p.DiagnosticCode = "invalidated", false, "profile_invalidated"
		p.Revision = next(p.Revision)
		p.Fingerprint = profileFingerprint(p.Context, p.Revision)
		d.Profiles[key] = p
		selected = p
		return true, nil
	})
	return selected, err
}
