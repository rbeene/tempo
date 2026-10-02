// Package hooks decodes native host payloads into allowlisted activity metadata.
package hooks

import (
	"encoding/json"
	"io"
	"path/filepath"
	"unicode"

	"github.com/rbeene/tempo/internal/activity"
)

const MaxInputBytes = 64 << 10

func DecodeCodex(r io.Reader) (activity.HostEvent, error) {
	bad := func(code string) (activity.HostEvent, error) {
		return activity.HostEvent{}, &activity.Error{Code: code, Message: "invalid or unsupported Codex hook metadata"}
	}
	b, err := io.ReadAll(io.LimitReader(r, MaxInputBytes+1))
	if err != nil || len(b) > MaxInputBytes || !validJSON(b) {
		return bad("validation")
	}
	var raw map[string]json.RawMessage
	if json.Unmarshal(b, &raw) != nil || raw == nil {
		return bad("validation")
	}
	text := func(name string, limit int) (string, bool) {
		var s string
		v, ok := raw[name]
		if !ok || json.Unmarshal(v, &s) != nil || s == "" || len(s) > limit {
			return "", false
		}
		for _, c := range s {
			if unicode.IsControl(c) {
				return "", false
			}
		}
		return s, true
	}
	e := activity.HostEvent{Source: "codex"}
	var ok bool
	if e.Kind, ok = text("hook_event_name", 64); !ok {
		return bad("validation")
	}
	if e.SessionID, ok = text("session_id", 256); !ok {
		return bad("validation")
	}
	if e.CWD, ok = text("cwd", 4096); !ok || !filepath.IsAbs(e.CWD) {
		return bad("validation")
	}
	switch e.Kind {
	case "SessionStart":
		if e.SessionSource, ok = text("source", 32); !ok {
			return bad("validation")
		}
		switch e.SessionSource {
		case "startup", "resume", "clear", "compact":
		default:
			return bad("validation")
		}
	case "SessionEnd":
	case "UserPromptSubmit", "SubagentStart", "SubagentStop", "Stop", "Interrupt", "PreToolUse", "PostToolUse", "PermissionRequest", "PreCompact", "PostCompact":
		if e.TurnID, ok = text("turn_id", 256); !ok {
			return bad("validation")
		}
	default:
		return bad("unsupported_contract")
	}
	if e.Kind == "SubagentStart" || e.Kind == "SubagentStop" {
		if e.AgentID, ok = text("agent_id", 128); !ok {
			return bad("validation")
		}
	}
	if e.Kind == "PreToolUse" || e.Kind == "PostToolUse" || e.Kind == "PermissionRequest" {
		if e.ToolName, ok = text("tool_name", 256); !ok {
			return bad("validation")
		}
		if e.Kind != "PermissionRequest" {
			if e.ToolID, ok = text("tool_use_id", 256); !ok {
				return bad("validation")
			}
		}
	}
	if e.Kind == "Stop" || e.Kind == "SubagentStop" {
		v, exists := raw["stop_hook_active"]
		if !exists || string(v) == "null" || json.Unmarshal(v, &e.StopHookActive) != nil {
			return bad("validation")
		}
	}
	return e, nil
}
