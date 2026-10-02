package hookstate

import "path/filepath"

type installation struct {
	Configuration string            `json:"configuration,omitempty"`
	Tooltip       *tooltipOwnership `json:"tooltip,omitempty"`
	Host          string            `json:"host"`
	Scope         string            `json:"scope"`
	Root          string            `json:"root"`
	Definition    string            `json:"definition"`
	Skill         string            `json:"skill"`
	Group         string            `json:"group"`
	SkillHash     string            `json:"skill_hash"`
	Runtime       Runtime           `json:"runtime"`
	Executable    string            `json:"executable"`
}
type installTarget struct {
	Path   string `json:"path"`
	Before string `json:"before"`
	After  string `json:"after"`
	Mode   uint32 `json:"mode"`
	Backup string `json:"backup"`
	Staged string `json:"staged"`
}
type installRequest struct {
	Fingerprint string                  `json:"fingerprint"`
	InputHash   string                  `json:"input_hash"`
	Intent      InstallIntent           `json:"intent"`
	State       string                  `json:"state"`
	Targets     []installTarget         `json:"targets"`
	Set         map[string]installation `json:"set"`
	Remove      []string                `json:"remove"`
	Result      HookList                `json:"result"`
}

func installKey(host, scope, root string) string { return digest([]string{host, scope, root}) }
func validInstallation(i installation) bool {
	if i.Tooltip != nil && (i.Host != "codex" || !filepath.IsAbs(i.Configuration) || i.Tooltip.Before != "" && i.Tooltip.Before != "true" || len(i.Tooltip.Inserted) > 128) {
		return false
	}
	return (i.Host == "codex" || i.Host == "claude") && (i.Scope == "user" || i.Scope == "project") && filepath.IsAbs(i.Root) && filepath.IsAbs(i.Definition) && filepath.IsAbs(i.Skill) && filepath.IsAbs(i.Executable) && hash(i.SkillHash) && i.Group == nativeGroup(i.Host, i.Executable)
}
func validInstallMetadata(m *metadata) bool {
	for key, i := range m.Installations {
		if key != installKey(i.Host, i.Scope, i.Root) || !validInstallation(i) {
			return false
		}
	}
	for id, r := range m.InstallRequests {
		if !uuid(id) || !hash(r.InputHash) || !hash(r.Fingerprint) || r.InputHash != installInputHash(ApplyInstallInput{Intent: r.Intent, Fingerprint: r.Fingerprint}) || r.State != "pending" && r.State != "complete" || r.Result.ContractVersion != 1 {
			return false
		}
		if _, ok := m.Requests[id]; ok {
			return false
		}
		for key, i := range r.Set {
			if key != installKey(i.Host, i.Scope, i.Root) || !validInstallation(i) {
				return false
			}
		}
		for _, key := range r.Remove {
			if !hash(key) {
				return false
			}
		}
		for n, t := range r.Targets {
			if !filepath.IsAbs(t.Path) || !safeText(t.Path, 4096) || !hash(t.Before) && t.Before != "absent" || !hash(t.After) && t.After != "absent" || t.Mode > 0777 {
				return false
			}
			base := installArtifact(id, n)
			if t.Before != "absent" && t.Backup != base+".before" || t.Before == "absent" && t.Backup != "" || t.After != "absent" && t.Staged != base+".after" || t.After == "absent" && t.Staged != "" {
				return false
			}
		}
	}
	return true
}
