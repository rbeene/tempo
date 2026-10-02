package hooks

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity"
)

func qaClaudePayload(kind string) map[string]any {
	return map[string]any{"hook_event_name": kind, "session_id": "session/opaque", "prompt_id": "prompt:opaque", "cwd": "/synthetic/project"}
}
func qaClaudeJSON(t *testing.T, m map[string]any) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
func qaClaudeRejected(t *testing.T, r io.Reader, code string) {
	t.Helper()
	got, err := DecodeClaude(r)
	var e *activity.Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("want safe %s, got event=%+v err=%v", code, got, err)
	}
	if !reflect.DeepEqual(got, activity.HostEvent{}) {
		t.Fatalf("partial metadata escaped rejection: %+v", got)
	}
	b, _ := json.Marshal(e)
	if strings.Contains(string(b), "SECRET") {
		t.Fatalf("private input escaped through error: %s", b)
	}
}

func TestQAClaudeNativeMetadataShapes(t *testing.T) {
	for _, kind := range []string{"UserPromptSubmit", "Stop", "SubagentStart", "SubagentStop", "PreToolUse", "PostToolUse", "PostToolUseFailure", "PermissionRequest", "StopFailure", "TaskCreated", "TaskCompleted"} {
		children := []string{""}
		if kind == "SubagentStart" || kind == "SubagentStop" {
			children = []string{"root"}
		} else if kind != "UserPromptSubmit" {
			children = append(children, "child/α")
		}
		for _, child := range children {
			t.Run(kind+"/"+child, func(t *testing.T) {
				m := qaClaudePayload(kind)
				want := activity.HostEvent{Source: "claude", Kind: kind, SessionID: "session/opaque", TurnID: "prompt:opaque", CWD: "/synthetic/project", AgentID: child}
				if child != "" {
					m["agent_id"] = child
				}
				if kind == "Stop" || kind == "SubagentStop" {
					m["stop_hook_active"] = true
					want.StopHookActive = true
				}
				if kind == "PreToolUse" || kind == "PostToolUse" || kind == "PostToolUseFailure" || kind == "PermissionRequest" {
					m["tool_name"] = "AskUserQuestion"
					want.ToolName = "AskUserQuestion"
					m["tool_use_id"] = "tool/opaque"
					if kind != "PermissionRequest" {
						want.ToolID = "tool/opaque"
					}
				}
				m["turn_id"] = "SECRET_WRONG_ID"
				m["parent_agent_id"] = "SECRET_PARENT"
				m["source"] = "SECRET_FORGED_SOURCE"
				m["prompt"] = "SECRET_PROMPT"
				m["transcript_path"] = "/no/such/SECRET_TRANSCRIPT"
				m["tool_input"] = map[string]any{"command": "SECRET_COMMAND"}
				m["tool_response"] = "SECRET_RESPONSE"
				m["error"] = "SECRET_ERROR"
				m["error_details"] = map[string]any{"message": "SECRET_DETAIL"}
				m["task_subject"] = "SECRET_SUBJECT"
				m["task_description"] = "SECRET_DESCRIPTION"
				m["origin"] = "host_observed"
				m["capture_eligible"] = true
				got, err := DecodeClaude(strings.NewReader(qaClaudeJSON(t, m)))
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("native allowlist mismatch: got %+v want %+v", got, want)
				}
				b, _ := json.Marshal(got)
				if strings.Contains(string(b), "SECRET") {
					t.Fatalf("sensitive content retained: %s", b)
				}
			})
		}
	}
}

func TestQAClaudeSessionBoundariesAndStrictStop(t *testing.T) {
	for _, source := range []string{"startup", "resume", "clear", "compact", "fork"} {
		t.Run(source, func(t *testing.T) {
			m := qaClaudePayload("SessionStart")
			m["source"] = source
			m["agent_id"] = "incidental"
			m["turn_id"] = "ignored"
			got, err := DecodeClaude(strings.NewReader(qaClaudeJSON(t, m)))
			want := activity.HostEvent{Source: "claude", Kind: "SessionStart", SessionID: "session/opaque", CWD: "/synthetic/project", SessionSource: source}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("session metadata: %+v %v", got, err)
			}
		})
	}
	m := qaClaudePayload("SessionEnd")
	delete(m, "prompt_id")
	m["agent_id"] = "incidental"
	m["reason"] = "SECRET_EXIT"
	got, err := DecodeClaude(strings.NewReader(qaClaudeJSON(t, m)))
	want := activity.HostEvent{Source: "claude", Kind: "SessionEnd", SessionID: "session/opaque", CWD: "/synthetic/project"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("end metadata: %+v %v", got, err)
	}
	m = qaClaudePayload("Stop")
	m["stop_hook_active"] = false
	got, err = DecodeClaude(strings.NewReader(qaClaudeJSON(t, m)))
	if err != nil || got.StopHookActive || got.Kind != "Stop" {
		t.Fatalf("explicit false stop: %+v %v", got, err)
	}
}

func TestQAClaudeRequiredFieldsAndTypes(t *testing.T) {
	for _, field := range []string{"hook_event_name", "session_id", "prompt_id", "cwd"} {
		for _, bad := range []any{nil, "", 7, true, []any{"SECRET"}, map[string]any{"SECRET": true}} {
			t.Run(field+"/"+qaClaudeJSON(t, map[string]any{"v": bad}), func(t *testing.T) {
				m := qaClaudePayload("UserPromptSubmit")
				m[field] = bad
				qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
			})
		}
		t.Run(field+"/missing", func(t *testing.T) {
			m := qaClaudePayload("UserPromptSubmit")
			delete(m, field)
			m["turn_id"] = "SECRET_FALLBACK"
			qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
		})
	}
	for _, kind := range []string{"Stop", "SubagentStop"} {
		for _, bad := range []any{nil, "false", 0} {
			t.Run(kind+"/bad_bool", func(t *testing.T) {
				m := qaClaudePayload(kind)
				m["agent_id"] = "child"
				m["stop_hook_active"] = bad
				qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
			})
		}
		t.Run(kind+"/missing_bool", func(t *testing.T) {
			m := qaClaudePayload(kind)
			m["agent_id"] = "child"
			qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
		})
	}
	for _, kind := range []string{"PreToolUse", "PostToolUse", "PostToolUseFailure", "PermissionRequest"} {
		fields := []string{"tool_name"}
		if kind != "PermissionRequest" {
			fields = append(fields, "tool_use_id")
		}
		for _, field := range fields {
			for _, bad := range []any{nil, "", false, 17} {
				t.Run(kind+"/"+field, func(t *testing.T) {
					m := qaClaudePayload(kind)
					m["tool_name"] = "Read"
					m["tool_use_id"] = "id"
					m[field] = bad
					qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
				})
			}
			t.Run(kind+"/missing_"+field, func(t *testing.T) {
				m := qaClaudePayload(kind)
				m["tool_name"] = "Read"
				m["tool_use_id"] = "id"
				delete(m, field)
				qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
			})
		}
	}
	for _, kind := range []string{"SubagentStart", "SubagentStop", "Stop", "PreToolUse", "PostToolUse", "PostToolUseFailure", "StopFailure", "PermissionRequest", "TaskCreated", "TaskCompleted"} {
		for _, bad := range []any{nil, "", false, []any{"SECRET"}} {
			t.Run(kind+"/bad_child", func(t *testing.T) {
				m := qaClaudePayload(kind)
				m["stop_hook_active"] = false
				m["tool_name"] = "Read"
				m["tool_use_id"] = "id"
				m["agent_id"] = bad
				qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
			})
		}
	}
	for _, kind := range []string{"SubagentStart", "SubagentStop"} {
		t.Run(kind+"/missing_child", func(t *testing.T) {
			m := qaClaudePayload(kind)
			m["stop_hook_active"] = false
			qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
		})
	}
	for _, source := range []any{nil, "", "unknown", false} {
		m := qaClaudePayload("SessionStart")
		m["source"] = source
		qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
	}
}

func TestQAClaudeUnsupportedAndPromptIdentity(t *testing.T) {
	for _, kind := range []string{"Interrupt", "Elicitation", "ElicitationResult", "TeammateIdle", "Notification", "SECRET_UNKNOWN"} {
		t.Run(kind, func(t *testing.T) {
			qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, qaClaudePayload(kind))), "unsupported_contract")
		})
	}
	for _, kind := range []string{"TaskCreated", "TaskCompleted"} {
		for _, bad := range []any{nil, "", 3} {
			m := qaClaudePayload(kind)
			m["prompt_id"] = bad
			qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "unsupported_contract")
		}
		m := qaClaudePayload(kind)
		delete(m, "prompt_id")
		qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "unsupported_contract")
	}
	for _, child := range []any{"child", "", nil, false, 7} {
		m := qaClaudePayload("UserPromptSubmit")
		m["agent_id"] = child
		qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
	}
}

func TestQAClaudeIdentityBounds(t *testing.T) {
	for _, tc := range []struct {
		field string
		limit int
	}{{"session_id", 256}, {"prompt_id", 256}, {"agent_id", 128}, {"tool_use_id", 256}, {"tool_name", 256}, {"cwd", 4096}} {
		t.Run(tc.field, func(t *testing.T) {
			m := qaClaudePayload("PreToolUse")
			m["agent_id"] = "child"
			m["tool_use_id"] = "tool"
			m["tool_name"] = "Read"
			value := strings.Repeat("x", tc.limit)
			if tc.field == "cwd" {
				value = "/" + value[1:]
			}
			m[tc.field] = value
			if _, err := DecodeClaude(strings.NewReader(qaClaudeJSON(t, m))); err != nil {
				t.Fatalf("exact bound rejected: %v", err)
			}
			m[tc.field] = value + "x"
			qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
			for _, control := range []string{"\x00", "\x1b", "\n", "\u0085"} {
				m[tc.field] = "/SECRET" + control
				qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
			}
		})
	}
	m := qaClaudePayload("UserPromptSubmit")
	m["cwd"] = "relative/SECRET"
	qaClaudeRejected(t, strings.NewReader(qaClaudeJSON(t, m)), "validation")
}

type qaClaudeFailReader struct{}

func (qaClaudeFailReader) Read([]byte) (int, error) { return 0, errors.New("SECRET_IO_FAILURE") }
func TestQAClaudeStrictJSONAndFiniteInput(t *testing.T) {
	base := qaClaudeJSON(t, qaClaudePayload("UserPromptSubmit"))
	prefix := strings.TrimSuffix(base, "}")
	cases := map[string]string{"empty": "", "null": "null", "array": "[" + base + "]", "trailing": base + ` {"SECRET":1}`, "malformed": prefix + `,"SECRET":`, "duplicate": prefix + `,"session_id":"SECRET"}`, "nested_duplicate": prefix + `,"tool_input":{"SECRET":1,"SECRET":2}}`, "escaped_duplicate": prefix + `,"tool_input":{"SECRET":1,"\u0053ECRET":2}}`, "invalid_utf8": prefix + `,"prompt":"SECRET` + string([]byte{0xff}) + `"}`, "deep": prefix + `,"prompt":` + strings.Repeat("[", 64) + `"SECRET"` + strings.Repeat("]", 64) + `}`}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) { qaClaudeRejected(t, strings.NewReader(raw), "validation") })
	}
	qaClaudeRejected(t, qaClaudeFailReader{}, "validation")
	deepOK := prefix + `,"prompt":` + strings.Repeat("[", 63) + `"SECRET"` + strings.Repeat("]", 63) + `}`
	if _, err := DecodeClaude(strings.NewReader(deepOK)); err != nil {
		t.Fatalf("valid maximum depth rejected: %v", err)
	}
	exact := base + strings.Repeat(" ", MaxInputBytes-len(base))
	if _, err := DecodeClaude(strings.NewReader(exact)); err != nil {
		t.Fatalf("exact64KiB valid JSON rejected: %v", err)
	}
	oversized := bytes.NewReader([]byte(exact + strings.Repeat(" ", 8192)))
	qaClaudeRejected(t, oversized, "validation")
	if consumed := MaxInputBytes + 8192 - oversized.Len(); consumed != MaxInputBytes+1 {
		t.Fatalf("read past bounded sentinel: consumed%d", consumed)
	}
}
