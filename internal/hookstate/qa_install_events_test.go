package hookstate

import "testing"

// These inventories come from the approved native decoder contracts, not the
// renderer. Permission/loss callbacks are essential to avoid counting uncertain
// waits as working time even when common start/stop callbacks are configured.
func TestQAInstallDefinitionsCoverEverySupportedNativeLifecycle(t *testing.T) {
	cases := map[string][]string{
		"codex":  {"SessionStart", "SessionEnd", "UserPromptSubmit", "Stop", "SubagentStart", "SubagentStop", "PreToolUse", "PostToolUse", "Interrupt", "PermissionRequest", "PreCompact", "PostCompact"},
		"claude": {"SessionStart", "SessionEnd", "UserPromptSubmit", "Stop", "SubagentStart", "SubagentStop", "PreToolUse", "PostToolUse", "PostToolUseFailure", "PermissionRequest", "StopFailure", "TaskCreated", "TaskCompleted"},
	}
	for host, events := range cases {
		t.Run(host, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			qaInstallApply(t, New(f.options), f.intent(host, "project"), 131)
			actual := qaInstallNative(t, f.target(host, "project"))
			expected := map[string]bool{}
			for _, event := range events {
				expected[event] = true
				groups := actual[event]
				if len(groups) != 1 || len(groups[0].Hooks) != 1 {
					t.Errorf("supported %s callback missing or duplicated in installed %s definitions", event, host)
					continue
				}
				handler := groups[0].Hooks[0]
				if handler.Type != "command" || handler.Async || handler.Timeout <= 0 {
					t.Errorf("supported %s callback lacks synchronous bounded bridge: %+v", event, handler)
				}
			}
			for event := range actual {
				if !expected[event] {
					t.Errorf("installer registered unsupported %s callback for %s", event, host)
				}
			}
		})
	}
}
