package hookstate

import (
	"encoding/hex"
	"path/filepath"
)

// This is the authoritative hooks metadata domain. Installer manifests and
// future typed requests extend this record under its existing lock.
type metadata struct {
	SchemaVersion int                      `json:"schema_version"`
	Profiles      map[string]Profile       `json:"profiles"`
	Requests      map[string]policyRequest `json:"requests"`
}

type policyRequest struct {
	Operation   string  `json:"operation"`
	Fingerprint string  `json:"fingerprint"`
	Result      Profile `json:"result"`
}

func emptyMetadata() *metadata {
	return &metadata{SchemaVersion: 1, Profiles: map[string]Profile{}, Requests: map[string]policyRequest{}}
}

func hash(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

func validProfile(p Profile) bool {
	n, ok := number(p.Revision)
	if !ok || n == 0 || p.Basis != "operator_declared" || p.DeclarationVersion != DeclarationVersion || p.Fingerprint != profileFingerprint(p.Context, p.Revision) {
		return false
	}
	want := map[string]string{"eligible": "operator_declared_risk", "revoked": "profile_revoked", "invalidated": "profile_invalidated"}
	diagnostic, ok := want[p.State]
	if !ok || p.DiagnosticCode != diagnostic || p.CaptureEligible != (p.State == "eligible") {
		return false
	}
	c := p.Context
	if c.Host != "codex" && c.Host != "claude" || c.Scope != "project" || c.Surface != "local" || !filepath.IsAbs(c.Path) || filepath.Clean(c.Path) != c.Path || !safeText(c.Path, 4096) || len(c.Conflicts) != 0 || len(c.Artifacts) < 3 || len(c.Artifacts) > 16 {
		return false
	}
	if c.Host == "codex" && c.RuntimeVersion != "0.159.3" || c.Host == "claude" && c.RuntimeVersion != "2.1.286" {
		return false
	}
	roles := map[string]bool{}
	last := ""
	for _, a := range c.Artifacts {
		if !filepath.IsAbs(a.Path) || filepath.Clean(a.Path) != a.Path || !safeText(a.Path, 4096) || a.Path <= last || !hash(a.SHA256) && a.SHA256 != "absent" {
			return false
		}
		switch a.Role {
		case "runtime", "executable", "definitions":
			if a.SHA256 == "absent" {
				return false
			}
		case "skill", "configuration":
		default:
			return false
		}
		roles[a.Role] = true
		last = a.Path
	}
	return roles["runtime"] && roles["executable"] && roles["definitions"]
}

func validMetadata(d *metadata) bool {
	if d.SchemaVersion != 1 || d.Profiles == nil || d.Requests == nil {
		return false
	}
	for key, p := range d.Profiles {
		if key != profileKey(p.Context.Host, p.Context.Scope, p.Context.Path) || !validProfile(p) {
			return false
		}
	}
	for id, r := range d.Requests {
		if !uuid(id) || !hash(r.Fingerprint) || !validProfile(r.Result) {
			return false
		}
		if r.Operation != "hooks.confirm-profile" && r.Operation != "hooks.revoke-profile" {
			return false
		}
		if r.Operation == "hooks.confirm-profile" && r.Result.State != "eligible" || r.Operation == "hooks.revoke-profile" && r.Result.State != "revoked" {
			return false
		}
		current, ok := d.Profiles[profileKey(r.Result.Context.Host, r.Result.Context.Scope, r.Result.Context.Path)]
		before, _ := number(r.Result.Revision)
		after, _ := number(current.Revision)
		if !ok || before > after || before == after && digest(r.Result) != digest(current) {
			return false
		}
	}
	return true
}
