package hooks

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestQACodexOptionalChildIdentityNativeKinds(t *testing.T) {
	for _, kind := range []string{"UserPromptSubmit", "PreToolUse", "PostToolUse", "PermissionRequest", "PreCompact", "PostCompact"} {
		t.Run(kind, func(t *testing.T) {
			raw := map[string]any{"hook_event_name": kind, "session_id": "s", "turn_id": "child-turn", "cwd": "/synthetic/project", "agent_id": "child-agent", "tool_use_id": "tool", "tool_name": "shell"}
			b, _ := json.Marshal(raw)
			e, err := DecodeCodex(strings.NewReader(string(b)))
			if err != nil {
				t.Fatal(err)
			}
			if e.AgentID != "child-agent" {
				t.Fatalf("documented child identity discarded: %+v", e)
			}
			delete(raw, "agent_id")
			b, _ = json.Marshal(raw)
			e, err = DecodeCodex(strings.NewReader(string(b)))
			if err != nil || e.AgentID != "" {
				t.Fatalf("ordinary root shape changed: %+v %v", e, err)
			}
		})
	}
}

func TestQACodexOptionalChildIdentityRejectsMalformedPresentValues(t *testing.T) {
	for _, value := range []any{"", nil, 42, true, "bad\nidentity", strings.Repeat("x", 129)} {
		raw := map[string]any{"hook_event_name": "UserPromptSubmit", "session_id": "s", "turn_id": "child-turn", "cwd": "/synthetic/project", "agent_id": value}
		b, _ := json.Marshal(raw)
		if _, err := DecodeCodex(strings.NewReader(string(b))); err == nil {
			t.Fatalf("malformed present child identity accepted: %T", value)
		}
	}
}
