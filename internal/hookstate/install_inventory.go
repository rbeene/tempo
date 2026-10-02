package hookstate

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"

	"github.com/pelletier/go-toml/v2"
)

func absentInstallProfile(host, scope, path string) Profile {
	return Profile{Basis: "none", State: "absent", Revision: "0", DiagnosticCode: "profile_required", Context: Context{Host: host, Scope: scope, Path: path, Surface: "local", Artifacts: []Artifact{}, Conflicts: []string{}}}
}

// An explicit HomeDir is an injected environment, used by isolated callers. The
// shipping default only claims the ordinary host roots; it does not guess at
// configuration selected by a host-root environment override.
func (s *Service) normalHostRoot(host string) bool {
	if s.options.HomeDir != "" {
		return true
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return false
	}
	key, dir := "CODEX_HOME", ".codex"
	if host == "claude" {
		key, dir = "CLAUDE_CONFIG_DIR", ".claude"
	}
	value := os.Getenv(key)
	return value == "" || filepath.Clean(value) == filepath.Join(home, dir)
}

func validateInstalledResources(ctx context.Context, installed installation) error {
	definitions, err := readInstallFile(ctx, installed.Host, installed.Definition, "definitions")
	if err != nil {
		return err
	}
	if !definitions.Exists {
		return problem("revision_conflict")
	}
	if _, err := ownedJSON(installed.Host, definitions.Bytes, installed.Group, installed.Group); err != nil {
		return problem("revision_conflict")
	}
	skill, err := readInstallFile(ctx, installed.Host, installed.Skill, "skill")
	if err != nil {
		return err
	}
	if !skill.Exists || skill.Hash != installed.SkillHash || skill.Hash != bytesHash([]byte(BundledSkill)) {
		return problem("revision_conflict")
	}
	return nil
}

func (s *Service) staticInventory(ctx context.Context, c Context, project, home string) (Context, error) {
	seen := map[string]bool{}
	for _, a := range c.Artifacts {
		seen[a.Path] = true
	}
	add := func(role, path string) error {
		path, err := canonicalInstallPath(path, false)
		if err != nil {
			return err
		}
		if !seen[path] {
			c.Artifacts = append(c.Artifacts, Artifact{Role: role, Path: path})
			seen[path] = true
		}
		if len(c.Artifacts) > maxProfileArtifacts {
			return problem("unsupported_contract")
		}
		return nil
	}
	candidates := []string{}
	if c.Host == "codex" {
		g, err := discoverGit(ctx, project)
		if err != nil {
			return Context{}, err
		}
		for _, a := range g.Artifacts {
			if err := add(a.Role, a.Path); err != nil {
				return Context{}, err
			}
			for i, item := range c.Artifacts {
				if item.Path == a.Path {
					c.Artifacts[i].SHA256 = a.SHA256
				}
			}
		}
		candidates = append(candidates, filepath.Join(home, ".codex", "config.toml"), filepath.Join(home, ".codex", "hooks.json"))
		for dir, n := project, 0; ; dir, n = filepath.Dir(dir), n+1 {
			if n >= 32 {
				return Context{}, problem("unsupported_contract")
			}
			candidates = append(candidates, filepath.Join(dir, ".codex", "config.toml"), filepath.Join(dir, ".codex", "hooks.json"))
			if g.MainRoot != g.CheckoutRoot {
				rel, e := filepath.Rel(g.CheckoutRoot, dir)
				if e != nil {
					return Context{}, problem("binding_unavailable")
				}
				paired := filepath.Join(g.MainRoot, rel)
				candidates = append(candidates, filepath.Join(paired, ".codex", "config.toml"), filepath.Join(paired, ".codex", "hooks.json"))
			}
			// Record missing nearer repository boundaries, so a new nested repository
			// invalidates the old source inventory without invoking Git in callbacks.
			if err := add("configuration", filepath.Join(dir, ".git")); err != nil {
				return Context{}, err
			}
			if dir == g.CheckoutRoot {
				break
			}
		}
		if len(g.Artifacts) == 0 {
			// Outside Git, creation of any discoverable ancestor boundary changes the
			// host's default project-root resolution even if cwd is unchanged.
			for dir, n := filepath.Dir(project), 0; ; dir, n = filepath.Dir(dir), n+1 {
				if n >= 32 {
					return Context{}, problem("unsupported_contract")
				}
				if err := add("configuration", filepath.Join(dir, ".git")); err != nil {
					return Context{}, err
				}
				if filepath.Dir(dir) == dir {
					break
				}
			}
		}
		system := s.options.CodexSystemDir
		if system == "" {
			system = "/etc/codex"
			if runtime.GOOS == "darwin" {
				system = "/private/etc/codex"
			}
		}
		for _, name := range []string{"config.toml", "hooks.json", "requirements.toml", "managed_config.toml"} {
			candidates = append(candidates, filepath.Join(system, name))
		}
	} else {
		managed := s.options.ClaudeManagedDir
		if managed == "" {
			managed = "/etc/claude-code"
			if runtime.GOOS == "darwin" {
				managed = "/Library/Application Support/ClaudeCode"
			}
		}
		candidates = append(candidates, filepath.Join(home, ".claude", "settings.json"), filepath.Join(project, ".claude", "settings.json"), filepath.Join(project, ".claude", "settings.local.json"), filepath.Join(managed, "managed-settings.json"), filepath.Join(managed, "managed-settings.d"))
		// A present managed-settings.d is intentionally unsupported. Its recorded
		// absence detects arrival during admission; no directory merge is invented.
	}
	for _, path := range candidates {
		if err := add("configuration", path); err != nil {
			return Context{}, err
		}
	}
	return c, nil
}

func inspectStaticJSON(b []byte, installed installation, path string, conflicts map[string]bool) error {
	if err := validateHookDocument(b); err != nil {
		return err
	}
	var doc struct {
		DisableAllHooks       bool                         `json:"disableAllHooks"`
		AllowManagedHooksOnly bool                         `json:"allowManagedHooksOnly"`
		Hooks                 map[string][]json.RawMessage `json:"hooks"`
	}
	if json.Unmarshal(b, &doc) != nil {
		return problem("validation")
	}
	if doc.DisableAllHooks || doc.AllowManagedHooksOnly {
		conflicts["untrusted"] = true
	}
	for event, groups := range doc.Hooks {
		for _, group := range groups {
			if path == installed.Definition && string(group) == installed.Group {
				continue
			}
			var parsed struct {
				Hooks []struct {
					Async bool `json:"async"`
				} `json:"hooks"`
			}
			if json.Unmarshal(group, &parsed) != nil {
				return problem("validation")
			}
			for _, handler := range parsed.Hooks {
				if handler.Async && requiredNativeEvent(installed.Host, event) {
					conflicts["async"] = true
				}
				if event == "UserPromptSubmit" {
					conflicts["prompt_veto"] = true
				}
				if event == "Stop" || event == "SubagentStop" {
					conflicts["stop_continuation"] = true
				}
			}
		}
	}
	return nil
}

// Decode structure only. Never serialize TOML back to disk or evaluate programs.
func inspectStaticTOML(b []byte, host string, conflicts map[string]bool) error {
	var doc map[string]any
	if toml.Unmarshal(b, &doc) != nil {
		return problem("validation")
	}
	for _, key := range []string{"project_root_markers", "profile", "profiles"} {
		if _, ok := doc[key]; ok {
			return problem("unsupported_contract")
		}
	}
	if v, ok := doc["allow_managed_hooks_only"]; ok {
		enabled, valid := v.(bool)
		if !valid {
			return problem("validation")
		}
		if enabled {
			conflicts["untrusted"] = true
		}
	}
	hooks, exists := doc["hooks"]
	if !exists {
		return nil
	}
	events, ok := hooks.(map[string]any)
	if !ok {
		return problem("unsupported_contract")
	}
	if state, exists := events["state"]; exists {
		if err := inspectHookTrustState(state, conflicts); err != nil {
			return err
		}
		delete(events, "state")
	}
	encoded, err := json.Marshal(map[string]any{"hooks": events})
	if err != nil {
		return problem("validation")
	}
	return inspectStaticJSON(encoded, installation{Host: host}, "", conflicts)
}

func inspectHookTrustState(value any, conflicts map[string]bool) error {
	state, ok := value.(map[string]any)
	if !ok {
		return problem("unsupported_contract")
	}
	for _, entry := range state {
		perHook, ok := entry.(map[string]any)
		if !ok {
			return problem("unsupported_contract")
		}
		for field, v := range perHook {
			switch field {
			case "trusted_hash":
				if _, ok := v.(string); !ok {
					return problem("unsupported_contract")
				}
			case "enabled":
				if enabled, ok := v.(bool); !ok || !enabled {
					conflicts["untrusted"] = true
				}
			default:
				return problem("unsupported_contract")
			}
		}
	}
	return nil
}
