package hooks

import (
	"encoding/json"
	"io"
	"path/filepath"
	"unicode"

	"github.com/rbeene/tempo/internal/activity"
)

// DecodeClaude retains only native identity metadata; event ordering and policy
// admission belong to the shared activity service.
func DecodeClaude(r io.Reader) (activity.HostEvent, error) {
	bad := func(code string) (activity.HostEvent, error) {
		return activity.HostEvent{}, &activity.Error{Code: code, Message: "invalid or unsupported Claude hook metadata"}
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
	e := activity.HostEvent{Source: "claude"}
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
		case "startup", "resume", "clear", "compact", "fork":
		default:
			return bad("validation")
		}
		return e, nil
	case "SessionEnd":
		return e, nil
	case "UserPromptSubmit", "Stop", "SubagentStart", "SubagentStop", "PreToolUse", "PostToolUse", "PostToolUseFailure", "PermissionRequest", "StopFailure", "TaskCreated", "TaskCompleted":
	default:
		return bad("unsupported_contract")
	}
	if e.TurnID, ok = text("prompt_id", 256); !ok {
		if e.Kind == "TaskCreated" || e.Kind == "TaskCompleted" {
			return bad("unsupported_contract")
		}
		return bad("validation")
	}
	_, hasAgent := raw["agent_id"]
	if e.Kind == "UserPromptSubmit" {
		// Reject child metadata rather than silently attributing it to root.
		if hasAgent {
			return bad("validation")
		}
	} else if hasAgent || e.Kind == "SubagentStart" || e.Kind == "SubagentStop" {
		if e.AgentID, ok = text("agent_id", 128); !ok {
			return bad("validation")
		}
	}
	if e.Kind == "PreToolUse" || e.Kind == "PostToolUse" || e.Kind == "PostToolUseFailure" || e.Kind == "PermissionRequest" {
		if e.ToolName, ok = text("tool_name", 256); !ok {
			return bad("validation")
		}
		// PermissionRequest has no documented tool identity; an extra field
		// cannot establish correlation with a concurrent tool operation.
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
