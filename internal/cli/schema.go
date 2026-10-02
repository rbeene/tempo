package cli

// Command describes the stable, offline CLI contract.
type Command struct {
	Name        string            `json:"name"`
	Summary     string            `json:"summary"`
	Positionals string            `json:"positionals"`
	Flags       map[string]string `json:"flags"`
	Mutation    bool              `json:"mutation"`
}

var commands = []Command{
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
		"name": "tempo", "schema_version": 1, "commands": commands, "global_flags": globalFlags,
		"success":     map[string]any{"schema_version": 1, "data": "command result (object or array)"},
		"error":       map[string]any{"schema_version": 1, "error": map[string]any{"code": "stable code", "message": "safe human-readable description", "retryable": false, "uncertain": false}},
		"streams":     map[string]string{"success": "stdout", "error": "stderr", "prompts": "none; destructive actions require --yes"},
		"exit_codes":  map[string]string{"0": "success", "1": "internal/config/keychain", "2": "usage/validation", "3": "auth", "4": "forbidden", "5": "not_found", "6": "conflict/confirmation_required", "7": "network/api/rate_limit/response", "8": "uncertain_write"},
		"environment": []string{"HARVEST_TOKEN", "HARVEST_ACCOUNT_ID", "TEMPO_CONFIG"},
		"semantics":   map[string]string{"date": "YYYY-MM-DD, today or yesterday; relative dates use machine local timezone", "duration": "decimal hours, H:MM, or Go duration such as 1h30m; range 0..24 hours", "time": "HH:MM, same-day end strictly after start; split overnight entries", "pagination": "complete arrays; errors never emit partial success", "auth": "HARVEST_TOKEN overrides macOS Keychain; --account overrides HARVEST_ACCOUNT_ID overrides config", "retry": "GET only; never automatically replay mutations", "timer": "current user, all dates; no automatic switch; preflight is not atomic"},
	}
}
