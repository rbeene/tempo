package hooks

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity"
)

func TestQACodexMetadataAllowlist(t *testing.T) {
	raw := `{"hook_event_name":"PreToolUse","session_id":"session/one","turn_id":"turn:two","agent_id":"root","cwd":"/synthetic/project","tool_use_id":"tool-3","tool_name":"Bash","parent_agent_id":"ignored-parent","prompt":"SECRET_PROMPT","transcript_path":"/NEVER_READ_SECRET","tool_input":{"command":"SECRET_COMMAND"},"tool_response":"SECRET_RESPONSE","origin":"host_observed","capture_eligible":true}`
	got, err := DecodeCodex(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	want := activity.HostEvent{Source: "codex", SessionID: "session/one", TurnID: "turn:two", Kind: "PreToolUse", CWD: "/synthetic/project", ToolID: "tool-3", ToolName: "Bash"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("allowlist mismatch: got %+v want %+v", got, want)
	}
	b, _ := json.Marshal(got)
	if strings.Contains(string(b), "SECRET") || strings.Contains(string(b), "ignored-parent") {
		t.Fatalf("private payload retained: %s", b)
	}
}

func TestQACodexNativeEventShapes(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		want      activity.HostEvent
	}{
		{"startup", `{"hook_event_name":"SessionStart","session_id":"s","cwd":"/project","source":"startup"}`, activity.HostEvent{Source: "codex", Kind: "SessionStart", SessionID: "s", CWD: "/project", SessionSource: "startup"}},
		{"stop", `{"hook_event_name":"Stop","session_id":"s","turn_id":"t","cwd":"/project","stop_hook_active":true}`, activity.HostEvent{Source: "codex", Kind: "Stop", SessionID: "s", TurnID: "t", CWD: "/project", StopHookActive: true}},
		{"permission_without_tool_id", `{"hook_event_name":"PermissionRequest","session_id":"s","turn_id":"t","cwd":"/project","tool_name":"Bash","tool_input":{"id":"not-an-id"}}`, activity.HostEvent{Source: "codex", Kind: "PermissionRequest", SessionID: "s", TurnID: "t", CWD: "/project", ToolName: "Bash"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeCodex(strings.NewReader(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v want %+v", got, tc.want)
			}
		})
	}
}

func TestQACodexRejectsMalformedWithoutPayloadEcho(t *testing.T) {
	base := `{"hook_event_name":"UserPromptSubmit","session_id":"s","turn_id":"t","cwd":"/project"}`
	cases := map[string]string{
		"duplicate_identity": `{"hook_event_name":"UserPromptSubmit","session_id":"s","session_id":"SECRET","turn_id":"t","cwd":"/project"}`,
		"duplicate_ignored":  `{"hook_event_name":"UserPromptSubmit","session_id":"s","turn_id":"t","cwd":"/project","prompt":"SECRET","prompt":"other"}`,
		"trailing":           base + ` {"SECRET":1}`,
		"array":              "[" + base + "]", "wrong_type": strings.Replace(base, `"turn_id":"t"`, `"turn_id":7`, 1),
		"missing_turn":     strings.Replace(base, `,"turn_id":"t"`, "", 1),
		"identity_control": strings.Replace(base, `"session_id":"s"`, `"session_id":"SECRET\u001b"`, 1),
		"bad_utf8":         strings.Replace(base, "/project", "/SECRET\xff", 1),
		"oversize":         strings.TrimSuffix(base, "}") + `,"prompt":"` + strings.Repeat("x", MaxInputBytes) + `SECRET"}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := DecodeCodex(strings.NewReader(raw))
			if err == nil {
				t.Fatalf("accepted malformed input: %+v", got)
			}
			if strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("echoed payload: %v", err)
			}
			if !reflect.DeepEqual(got, activity.HostEvent{}) {
				t.Fatalf("partial event escaped rejection: %+v", got)
			}
		})
	}
}

func TestQACodexChildAndPermissionMetadataAreEventSpecific(t *testing.T) {
	raw := `{"hook_event_name":"SubagentStop","session_id":"s","turn_id":"t","agent_id":"root","parent_agent_id":"unproven","cwd":"/project","stop_hook_active":false}`
	got, err := DecodeCodex(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if got.AgentID != "root" || got.ParentAgentID != "" {
		t.Fatalf("child lineage fabricated or omitted: %+v", got)
	}
	raw = `{"hook_event_name":"PermissionRequest","session_id":"s","turn_id":"t","cwd":"/project","tool_name":"Bash","tool_use_id":"unproven","tool_input":{"tool_use_id":"also-unproven"}}`
	got, err = DecodeCodex(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if got.ToolID != "" {
		t.Fatalf("permission invented correlation from extra metadata: %+v", got)
	}
}
