package hookstate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
)

// Verify deliberately shares the read-only status path. Neither method invokes a
// hook nor upgrades an operator declaration into observed host delivery.
func (s *Service) Verify(ctx context.Context, selector HookSelector) (HookList, error) {
	return s.Status(ctx, selector)
}
func (s *Service) Status(ctx context.Context, selector HookSelector) (HookList, error) {
	result := HookList{ContractVersion: 1, Hooks: []HookStatus{}}
	if selector.Host != "codex" && selector.Host != "claude" && selector.Host != "both" || selector.Scope != "user" && selector.Scope != "project" {
		return result, problem("validation")
	}
	m, _, err := s.read(ctx)
	if err != nil {
		return result, err
	}
	home := s.options.HomeDir
	if home == "" {
		home, err = os.UserHomeDir()
		if err != nil {
			return result, problem("validation")
		}
	}
	root := home
	if selector.Scope == "project" {
		root = selector.Path
	}
	if root == "" {
		return result, problem("validation")
	}
	root, err = canonicalInstallPath(root, selector.Scope == "project")
	if err != nil {
		return result, err
	}
	if selector.Path != "" {
		selector.Path, err = canonicalInstallPath(selector.Path, true)
		if err != nil {
			return result, err
		}
	}
	hosts := []string{selector.Host}
	if selector.Host == "both" {
		hosts = []string{"codex", "claude"}
	}
	for _, host := range hosts {
		row := HookStatus{Host: host, Scope: selector.Scope, Path: selector.Path, State: "not_installed", Ordering: "unavailable", Diagnostics: []Diagnostic{}, Profile: absentInstallProfile(host, selector.Scope, selector.Path)}
		installed, ok := m.Installations[installKey(host, selector.Scope, root)]
		for id, request := range m.InstallRequests {
			if request.State == "pending" {
				for _, planned := range request.Result.Hooks {
					if planned.Host == host && planned.Scope == selector.Scope && (selector.Scope == "user" || planned.Path == selector.Path) {
						row.State = "needs_repair"
						row.Pending = &PendingInstall{RequestID: id, Fingerprint: request.Fingerprint, Intent: request.Intent}
					}
				}
			}
		}
		if !ok || row.State == "needs_repair" {
			result.Hooks = append(result.Hooks, row)
			continue
		}
		row.State = "approval_required"
		row.RuntimeVersion = installed.Runtime.Version
		if selector.Path == "" {
			row.Diagnostics = append(row.Diagnostics, Diagnostic{"project_context_required", "info", "Select an explicit project context before confirming capture eligibility."})
			result.Hooks = append(result.Hooks, row)
			continue
		}
		c, e := s.installContext(ctx, installed, selector.Path, home)
		if e != nil {
			if ctx.Err() != nil {
				return result, problem("state_busy")
			}
			row.State = "unsupported"
			var issue *Error
			if errors.As(e, &issue) && issue.Code == "revision_conflict" {
				row.State = "needs_repair"
			}
			row.Diagnostics = append(row.Diagnostics, Diagnostic{"unsupported_contract", "error", "Current runtime or static configuration could not be safely inspected."})
			if old, found := m.Profiles[profileKey(host, selector.Scope, selector.Path)]; found {
				row.Profile = old
				row.Profile.State = "invalidated"
				row.Profile.CaptureEligible = false
				row.Profile.DiagnosticCode = "profile_invalidated"
				row.Profile.Fingerprint = ""
			}
			result.Hooks = append(result.Hooks, row)
			continue
		}
		old, retained := m.Profiles[profileKey(host, selector.Scope, selector.Path)]
		revision := "0"
		if retained {
			revision = old.Revision
		}
		row.Profile = Profile{Basis: "none", State: "absent", Revision: revision, Context: c, Fingerprint: profileFingerprint(c, revision), DeclarationVersion: DeclarationVersion, DiagnosticCode: "profile_required"}
		if retained {
			row.Profile = old
			if contextFingerprint(c) != contextFingerprint(old.Context) {
				row.Profile.State = "invalidated"
				row.Profile.CaptureEligible = false
				row.Profile.DiagnosticCode = "profile_invalidated"
				row.Profile.Context = c
				row.Profile.Fingerprint = profileFingerprint(c, revision)
			}
		}
		if len(c.Conflicts) > 0 {
			row.State = "unsupported"
			row.Profile.CaptureEligible = false
			row.Diagnostics = append(row.Diagnostics, Diagnostic{"ordering_unavailable", "error", "Known static hook conflicts prevent the automatic capture profile."})
		} else if row.Profile.CaptureEligible {
			row.State = "awaiting_real_event"
			row.Ordering = "supported"
		}
		row.Diagnostics = append(row.Diagnostics, Diagnostic{"operator_declared_risk", "info", "Review the host's effective hooks and workspace trust before confirming. Dynamic or plugin changes that the host does not export remain an operator-declared risk. Start or reload the host after installation; no real delivery has been verified."})
		result.Hooks = append(result.Hooks, row)
	}
	return result, nil
}

func (s *Service) installContext(ctx context.Context, installed installation, project, home string) (Context, error) {
	runtime, err := s.discover(ctx, installed.Host)
	if err != nil {
		return Context{}, err
	}
	c := Context{Host: installed.Host, Scope: installed.Scope, Path: project, RuntimeVersion: runtime.Version, Surface: runtime.Surface, Artifacts: []Artifact{{Role: "runtime", Path: runtime.Path}, {Role: "executable", Path: installed.Executable}, {Role: "definitions", Path: installed.Definition}, {Role: "skill", Path: installed.Skill}}, Conflicts: []string{}}
	c.InventoryVersion = installedInventoryVersion
	if !s.normalHostRoot(installed.Host) {
		return Context{}, problem("unsupported_contract")
	}
	if err := validateInstalledResources(ctx, installed); err != nil {
		return Context{}, err
	}
	c, err = s.staticInventory(ctx, c, project, home)
	if err != nil {
		return Context{}, err
	}
	conflicts := map[string]bool{}
	for i, a := range c.Artifacts {
		if a.Role != "definitions" && a.Role != "configuration" {
			continue
		}
		v, e := readInstallFile(ctx, installed.Host, a.Path, a.Role)
		if e != nil {
			return Context{}, e
		}
		c.Artifacts[i].SHA256 = v.Hash
		if a.Role == "definitions" {
			if _, err := ownedJSON(installed.Host, v.Bytes, installed.Group, installed.Group); err != nil {
				return Context{}, problem("revision_conflict")
			}
		}
		if !v.Exists {
			continue
		}
		if filepath.Ext(a.Path) == ".toml" {
			if e = inspectStaticTOML(v.Bytes, installed.Host, conflicts); e != nil {
				return Context{}, e
			}
		} else if filepath.Ext(a.Path) == ".json" {
			if e = inspectStaticJSON(v.Bytes, installed, a.Path, conflicts); e != nil {
				return Context{}, e
			}
		}
	}

	for conflict := range conflicts {
		c.Conflicts = append(c.Conflicts, conflict)
	}
	sort.Strings(c.Conflicts)
	for i, a := range c.Artifacts {
		if a.Role == "skill" {
			c.Artifacts[i].SHA256 = installed.SkillHash
		}
	}
	sampled, err := sampleContext(ctx, c)
	if err != nil {
		return Context{}, err
	}
	expected := map[string]string{}
	for _, a := range c.Artifacts {
		if a.SHA256 != "" {
			expected[a.Path] = a.SHA256
		}
	}
	for _, a := range sampled.Artifacts {
		if want, ok := expected[a.Path]; ok && want != a.SHA256 {
			return Context{}, problem("revision_conflict")
		}
	}
	return sampled, nil
}

func requiredNativeEvent(host, event string) bool {
	for _, required := range nativeEvents(host) {
		if event == required {
			return true
		}
	}
	return false
}
