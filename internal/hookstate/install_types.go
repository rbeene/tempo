package hookstate

import "time"

// Runtime is bounded inventory evidence, not proof of delivery or hook trust.
type Runtime struct {
	Path    string `json:"path"`
	Version string `json:"version"`
	Surface string `json:"surface"`
}

type InstallIntent struct {
	Host      string `json:"host"`
	Scope     string `json:"scope"`
	Path      string `json:"path"`
	Operation string `json:"operation"`
}
type HookChange struct {
	Host        string `json:"host"`
	Path        string `json:"path"`
	Operation   string `json:"operation"`
	SafeSummary string `json:"safe_summary"`
}
type HookPreview struct {
	ContractVersion int           `json:"contract_version"`
	Intent          InstallIntent `json:"intent"`
	Fingerprint     string        `json:"fingerprint"`
	Changes         []HookChange  `json:"changes"`
	ApprovalSteps   []string      `json:"approval_steps"`
}
type Diagnostic struct {
	Code        string `json:"code"`
	Severity    string `json:"severity"`
	SafeMessage string `json:"safe_message"`
}
type HookStatus struct {
	Pending        *PendingInstall `json:"pending,omitempty"`
	Host           string          `json:"host"`
	Scope          string          `json:"scope"`
	Path           string          `json:"path"`
	RuntimeVersion string          `json:"runtime_version"`
	State          string          `json:"state"`
	Ordering       string          `json:"ordering"`
	LastRealEvent  *time.Time      `json:"last_real_event"`
	Diagnostics    []Diagnostic    `json:"diagnostics"`
	Profile        Profile         `json:"profile"`
}
type HookList struct {
	RequestID       string       `json:"request_id,omitempty"`
	ContractVersion int          `json:"contract_version"`
	Hooks           []HookStatus `json:"hooks"`
}
type HookSelector struct{ Host, Scope, Path string }
type ApplyInstallInput struct {
	Intent                 InstallIntent
	Fingerprint, RequestID string
	Confirmed              bool
}

type PendingInstall struct {
	RequestID   string        `json:"request_id"`
	Fingerprint string        `json:"fingerprint"`
	Intent      InstallIntent `json:"intent"`
}
