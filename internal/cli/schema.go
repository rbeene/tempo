package cli

import "github.com/rbeene/tempo/internal/activity"

// Command describes the stable, offline CLI contract.
type Command struct {
	Name        string            `json:"name"`
	Summary     string            `json:"summary"`
	Positionals string            `json:"positionals"`
	Flags       map[string]string `json:"flags"`
	Mutation    bool              `json:"mutation"`
}

var commands = []Command{
	{"hooks preview", "Preview exact owned edits and approval steps without writes", "", map[string]string{"host": "string", "scope": "string", "path": "string", "operation": "string"}, false},
	{"hooks install", "Install previewed native hooks and packaged skill; requires fingerprint and --yes; generates request ID if absent", "", map[string]string{"host": "string", "scope": "string", "path": "string", "fingerprint": "string", "request-id": "uuid"}, true},
	{"hooks status", "Inspect installation and retained policy without implying real delivery", "", map[string]string{"host": "string", "scope": "string", "path": "string"}, false},
	{"hooks verify", "Read current hook evidence without firing a synthetic event", "", map[string]string{"host": "string", "scope": "string", "path": "string"}, false},
	{"hooks repair", "Repair unchanged owned definitions after reviewing a fresh preview", "", map[string]string{"host": "string", "scope": "string", "path": "string", "fingerprint": "string", "request-id": "uuid"}, true},
	{"hooks uninstall", "Selectively remove unchanged owned resources; preserve foreign edits and history", "", map[string]string{"host": "string", "scope": "string", "path": "string", "fingerprint": "string", "request-id": "uuid"}, true},
	{"hooks confirm-profile", "Explicitly retain operator-declared clean-profile eligibility; does not grant host trust", "", map[string]string{"host": "string", "scope": "string", "path": "string", "fingerprint": "string", "request-id": "uuid", "declaration-version": "string"}, true},
	{"hooks revoke-profile", "Revoke retained profile eligibility; requires revision and --yes; generates request ID if absent", "", map[string]string{"host": "string", "scope": "string", "path": "string", "if-revision": "counter", "request-id": "uuid"}, true},
	{"themes list", "List local appearance palettes and saved selection", "", nil, false},
	{"themes show", "Inspect the saved theme or preview a theme without saving", "[THEME]", nil, false},
	{"themes set", "Save an appearance choice; optional preference revision and exact request replay", "THEME", map[string]string{"if-revision": "counter", "request-id": "uuid"}, true},
	{"themes reset", "Select terminal default while retaining preference revisions and replay history", "", map[string]string{"if-revision": "counter", "request-id": "uuid"}, true},
	{"ui", "View local project activity; redirected or forced output is one JSON snapshot", "", nil, false},
	{"worker install", "Install a disabled user service without starting uploads; requires --yes", "", map[string]string{"request-id": "uuid"}, true},
	{"worker start", "Start the installed user worker without changing sync consent", "", map[string]string{"request-id": "uuid"}, true},
	{"worker status", "Inspect worker ownership and saved sync counts without credentials", "", nil, false},
	{"worker stop", "Stop the worker while preserving capture and sync state", "", map[string]string{"request-id": "uuid"}, true},
	{"worker uninstall", "Remove the owned user service and retain local history; requires --yes", "", map[string]string{"request-id": "uuid"}, true},
	{"worker run", "Run the foreground worker until stopped or signaled; output flags retain this lifetime", "", nil, true},
	{"sync status", "Inspect saved sync configuration, outbox and exact/planned/confirmed totals", "", nil, false},
	{"sync configure", "Declare verified account tracking mode and representation policy; requires revision and --yes", "", map[string]string{"mode": "string", "duration-policy": "string", "clock": "string", "if-revision": "counter", "request-id": "uuid", "user": "id"}, true},
	{"sync now", "Run a bounded durable sync pass; unknown writes are never retried", "", map[string]string{"limit": "counter", "request-id": "uuid"}, true},
	{"sync reconcile", "Read Harvest to reconcile unique saved entry markers", "[UUID]", map[string]string{"limit": "counter", "request-id": "uuid"}, true},
	{"sync pause", "Pause uploads locally without stopping capture", "", map[string]string{"request-id": "uuid"}, true},
	{"sync resume", "Enable uploads locally; configuration remains explicit", "", map[string]string{"request-id": "uuid"}, true},
	{"sync resolve", "Attach an existing entry or explicitly retry a definite rejection; requires revision and --yes", "UUID", map[string]string{"entry": "id", "retry-rejected": "bool", "if-revision": "counter", "request-id": "uuid"}, true},
	{"hook codex", "Capture one native Codex lifecycle callback; emits host JSON", "", map[string]string{"input-stdin": "bool"}, true},
	{"hook claude", "Capture one native Claude lifecycle callback; stdout remains empty", "", map[string]string{"input-stdin": "bool"}, true},
	{"activity review", "List unresolved local timing uncertainty without credentials", "", map[string]string{"project": "id"}, false},
	{"activity preview", "Preview a recovery end or discarded tail without changes", "UUID", map[string]string{"end": "utc", "discard-tail": "bool"}, false},
	{"activity resolve", "Resolve uncertainty; requires revision, end/discard-tail and --yes", "UUID", map[string]string{"end": "utc", "discard-tail": "bool", "if-revision": "counter", "reason": "string", "request-id": "uuid"}, true},
	{"activity interrupt", "Detach one generation; preserve working tails for recovery", "UUID", map[string]string{"generation": "counter", "if-revision": "counter", "request-id": "uuid"}, true},
	{"setup", "Guide secure authentication and directory/project setup", "", map[string]string{"host": "string", "scope": "string", "path": "string"}, true},
	{"doctor", "Inspect local readiness; --check verifies credentials", "", map[string]string{"check": "bool"}, false},
	{"link", "Link a directory or entire Git repository and all its worktrees to a project", "[ID]", map[string]string{"task": "id", "path": "string", "timezone": "string", "if-revision": "counter", "request-id": "uuid"}, true},
	{"links list", "List local directory and repository bindings", "", nil, false},
	{"links show", "Inspect one local binding by ID or path", "[UUID]", map[string]string{"path": "string"}, false},
	{"links unlink", "Remove a binding; requires revision and --yes", "UUID", map[string]string{"if-revision": "counter", "request-id": "uuid"}, true},
	{"links repair", "Repair a moved binding; requires path, revision and --yes", "UUID", map[string]string{"path": "string", "if-revision": "counter", "request-id": "uuid"}, true},
	{"activity status", "Read local agent activity without credentials or network; --watch requests interactive refresh", "", map[string]string{"watch": "bool"}, false},
	{"activity event", "Persist one normalized lifecycle event from bounded JSON stdin", "", map[string]string{"input-stdin": "bool"}, true},
	{"auth login", "Validate a token from stdin and save in macOS Keychain", "", map[string]string{"token-stdin": "bool"}, true},
	{"auth status", "Inspect configured credential source; --check verifies remotely", "", map[string]string{"check": "bool"}, false},
	{"auth logout", "Remove Tempo's saved token and account; requires --yes", "", nil, true},
	{"accounts list", "List accessible Harvest accounts", "", nil, false},
	{"accounts use", "Validate and select an accessible Harvest account", "ID", nil, true},
	{"config show", "Show nonsecret configuration and precedence", "", nil, false},
	{"config set-account", "Validate and select an accessible Harvest account", "ID", nil, true},
	{"projects list", "List your assigned projects; --all uses admin catalog", "", map[string]string{"all": "bool"}, false},
	{"projects show", "Show one of your project assignments", "ID", nil, false},
	{"tasks list", "List assigned tasks, optionally for one project", "", map[string]string{"project": "id", "all": "bool"}, false},
	{"clients list", "List clients from assigned projects; --all uses admin catalog", "", map[string]string{"all": "bool"}, false},
	{"time list", "List your time entries, following every page", "", map[string]string{"from": "date", "to": "date", "project": "id", "task": "id", "client": "id", "running": "boolean"}, false},
	{"time show", "Show your time entry", "ID", nil, false},
	{"time create", "Create completed time; requires date, project, task and duration or start/end", "", entryFlags(), true},
	{"time update", "Update only explicitly supplied fields on your entry", "ID", entryFlags(), true},
	{"time delete", "Delete your entry; requires --yes", "ID", nil, true},
	{"timer status", "List your running timers across all dates", "", nil, false},
	{"timer start", "Start new time or restart ID; conflicts if a different timer is running", "[ID]", map[string]string{"project": "id", "task": "id", "date": "date", "notes": "string", "start": "time"}, true},
	{"timer stop", "Stop ID or your single running timer", "[ID]", nil, true},
	{"help", "Print offline command help", "", nil, false},
	{"schema", "Print machine-readable command and response contracts", "", nil, false},
	{"version", "Print build version", "", nil, false},
}

func entryFlags() map[string]string {
	return map[string]string{"project": "id", "task": "id", "date": "date", "duration": "duration", "hours": "hours", "start": "time", "end": "time", "notes": "string"}
}

var globalFlags = map[string]string{"json": "bool", "account": "id", "yes": "bool", "non-interactive": "bool"}

func schema() any {
	return map[string]any{
		"hooks_contract":      map[string]any{"contract_version": 1, "preview": "intent,fingerprint,changes (exact owned command/path),approval_steps; no writes", "result": "contract_version,hooks with installation state, ordering, profile, diagnostics and nullable last_real_event", "policy": "explicit retained operator_declared clean profile bound to full static evidence and exact project context; known conflicts or drift invalidate; unknown dynamic changes remain declared risk", "delivery": "installation, confirmation and synthetic calls never prove real host receipt; verify is read-only", "mutations": "reviewed fingerprint and --yes; canonical request UUID generated if omitted and exposed in results/errors; same request reconciles only exact before/after journal hashes; edited or ambiguous owned resources conflict"},
		"theme_contract":      map[string]any{"contract_version": 1, "ids": []string{"terminal-default", "tokyo-night", "gruvbox", "catppuccin"}, "store": "private OS config directory tempo/preferences.json; TEMPO_PREFERENCES isolates this store", "preference_revision": "selection revision; advances once when the saved selection changes", "snapshot_revision": "independent preference transaction revision; advances for every first-admitted successful request, including no-ops; not comparable to activity revisions", "mutation": "entity_revision is the resulting preference revision; changed reports selection change; affected_ids=[]", "replay": "known request ID is checked before current revision; exact replay returns its historical result after durability sync without reapplying; reread current selection", "intent": "omitted and explicit revision differ; reset normalizes to set terminal-default; output flags are not intent", "unknown": "local_write_unknown exits 8 with request_id; repeat only the same identity and exact input", "appearance": "preview does not save; Apply uses observed preference revision; conflict requires fresh review", "color": "terminal-default inherits; NO_COLOR nonempty or unknown/non-color/redirected output disables styling; JSON never styled or affected by presentation preferences"},
		"activity_contract":   activity.Schema(),
		"worker_contract":     map[string]any{"contract_version": 1, "result": "contract_version,status (shared activity worker status)", "request_id": "canonical UUID for finite mutations; generated when omitted; preserve for exact replay", "confirmation": "install/uninstall require --yes; other worker commands reject --yes", "account": "saved sync accounts only; --account rejected", "run": "explicit foreground lifetime, including --json/--non-interactive; signal cleanup before exit 130/143; each sync pass remains bounded", "status": "one activity snapshot plus read-only worker evidence; absent installation/instance knowledge is null", "instance_mode": "TEMPO_WORKER_MODE=managed is display evidence only; all other values use foreground", "notifications": "best-effort post-commit Wake for applied/duplicate activity events; Recheck for sync resume/configure, auth login, account selection and completed setup login; at most 25ms within caller context; failures preserve the original result", "unknown": "local_write_unknown exits 8 with request_id; repeat only the exact request"},
		"setup_contract":      map[string]any{"contract_version": 1, "finite": "local readiness only; no credential or network access", "steps": "action,state,required_fields,safe_message", "complete": "false until all required capabilities are verified", "partial_failure": "error.details.completed_steps retains earlier completed actions"},
		"credential_contract": map[string]any{"helper_timeout_seconds": 5, "mutation_lock_seconds": 1, "os_prompts": false, "effects": "credential: unchanged|applied|unknown; config: unchanged|saved|cleared|restored|unknown", "unknown": "exit 8; uncertain true; never retry automatically; inspect auth status and config show"},
		"name":                "tempo", "schema_version": 1, "commands": commands, "global_flags": globalFlags,
		"success":     map[string]any{"schema_version": 1, "data": "command result (object or array)"},
		"error":       map[string]any{"schema_version": 1, "error": map[string]any{"code": "stable code", "message": "safe human-readable description", "retryable": false, "uncertain": false}},
		"streams":     map[string]string{"success": "stdout", "error": "stderr", "prompts": "setup/link only when stdin and stdout are TTYs; --json or --non-interactive disables prompts"},
		"exit_codes":  map[string]string{"0": "success", "1": "internal/config/keychain/state_corrupt/state_path_in_use/clock_unavailable/unsupported/manager/control_history_full", "2": "usage/validation/input_required/invalid_transition/recovery_bounds/unsupported_contract", "3": "auth", "4": "forbidden", "5": "not_found/binding_unavailable/actor_not_found/uncertainty_not_found", "6": "conflict/confirmation_required/attribution_conflict/binding_in_use/revision_conflict/request_conflict/event_conflict/event_gap/clock_conflict/state_busy", "7": "network/api/rate_limit/response", "8": "uncertain_write/local_write_unknown/credential_write_unknown", "130": "Ctrl-C/SIGINT unless dispatched mutation outcome is uncertain (exit 8)", "143": "SIGTERM unless dispatched mutation outcome is uncertain (exit 8)"},
		"environment": []string{"HARVEST_TOKEN", "HARVEST_ACCOUNT_ID", "TEMPO_CONFIG", "TEMPO_STATE", "TEMPO_WORKER_MODE", "TEMPO_HOOK_STATE", "TEMPO_PREFERENCES", "TERM", "COLORTERM", "NO_COLOR"},
		"host_hooks":  map[string]any{"commands": []string{"hook codex", "hook claude"}, "stdout": "Codex: {} followed by newline; Claude: empty; no Tempo envelope", "capture_failure_exit": 0, "invalid_arguments": "ordinary usage error", "stderr": "fixed diagnostic category and committed/not_committed/unknown durability", "input_limit_bytes": 65536, "deadline_ms": 900, "unsupported_global_flags": []string{"account", "yes"}},

		"semantics": map[string]string{"date": "YYYY-MM-DD, today or yesterday; relative dates use machine local timezone", "duration": "decimal hours, H:MM, or Go duration such as 1h30m; range 0..24 hours", "time": "HH:MM, same-day end strictly after start; split overnight entries", "pagination": "complete arrays; errors never emit partial success", "auth": "HARVEST_TOKEN overrides macOS Keychain; --account overrides HARVEST_ACCOUNT_ID overrides config", "retry": "GET only; never automatically replay mutations", "timer": "current user, all dates; no automatic switch; preflight is not atomic"},
	}
}
