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
	{"activity review", "List unresolved local timing uncertainty without credentials", "", map[string]string{"project": "id"}, false},
	{"activity preview", "Preview a recovery end or discarded tail without changes", "UUID", map[string]string{"end": "utc", "discard-tail": "bool"}, false},
	{"activity resolve", "Resolve uncertainty; requires revision, end/discard-tail and --yes", "UUID", map[string]string{"end": "utc", "discard-tail": "bool", "if-revision": "counter", "reason": "string", "request-id": "uuid"}, true},
	{"activity interrupt", "Detach one generation; preserve working tails for recovery", "UUID", map[string]string{"generation": "counter", "if-revision": "counter", "request-id": "uuid"}, true},
	{"setup", "Guide secure authentication and directory/project setup", "", map[string]string{"host": "string", "scope": "string", "path": "string"}, false},
	{"doctor", "Inspect local readiness; --check verifies credentials", "", map[string]string{"check": "bool"}, false},
	{"link", "Link a directory or entire Git repository and all its worktrees to a project", "[ID]", map[string]string{"task": "id", "path": "string", "timezone": "string", "if-revision": "counter", "request-id": "uuid"}, true},
	{"links list", "List local directory and repository bindings", "", nil, false},
	{"links show", "Inspect one local binding by ID or path", "[UUID]", map[string]string{"path": "string"}, false},
	{"links unlink", "Remove a binding; requires revision and --yes", "UUID", map[string]string{"if-revision": "counter", "request-id": "uuid"}, true},
	{"links repair", "Repair a moved binding; requires path, revision and --yes", "UUID", map[string]string{"path": "string", "if-revision": "counter", "request-id": "uuid"}, true},
	{"activity status", "Read local agent activity without credentials or network", "", nil, false},
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
		"activity_contract": activity.Schema(),
		"name":              "tempo", "schema_version": 1, "commands": commands, "global_flags": globalFlags,
		"success":     map[string]any{"schema_version": 1, "data": "command result (object or array)"},
		"error":       map[string]any{"schema_version": 1, "error": map[string]any{"code": "stable code", "message": "safe human-readable description", "retryable": false, "uncertain": false}},
		"streams":     map[string]string{"success": "stdout", "error": "stderr", "prompts": "none; destructive actions require --yes"},
		"exit_codes":  map[string]string{"0": "success", "1": "internal/config/keychain/state_corrupt/clock_unavailable", "2": "usage/validation/input_required/invalid_transition/recovery_bounds/unsupported_contract", "3": "auth", "4": "forbidden", "5": "not_found/binding_unavailable/actor_not_found/uncertainty_not_found", "6": "conflict/confirmation_required/attribution_conflict/binding_in_use/revision_conflict/request_conflict/event_conflict/event_gap/clock_conflict/state_busy", "7": "network/api/rate_limit/response", "8": "uncertain_write/local_write_unknown"},
		"environment": []string{"HARVEST_TOKEN", "HARVEST_ACCOUNT_ID", "TEMPO_CONFIG", "TEMPO_STATE"},
		"semantics":   map[string]string{"date": "YYYY-MM-DD, today or yesterday; relative dates use machine local timezone", "duration": "decimal hours, H:MM, or Go duration such as 1h30m; range 0..24 hours", "time": "HH:MM, same-day end strictly after start; split overnight entries", "pagination": "complete arrays; errors never emit partial success", "auth": "HARVEST_TOKEN overrides macOS Keychain; --account overrides HARVEST_ACCOUNT_ID overrides config", "retry": "GET only; never automatically replay mutations", "timer": "current user, all dates; no automatic switch; preflight is not atomic"},
	}
}
