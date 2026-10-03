//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// The older host fixture deliberately enters the private adapter. These extra
// cases use the existing explicit SQLite factory and the public host method.
func hxQAPublic(h *hiQAFixture) {
	h.s = NewSQLite(Options{Path: h.path, LockTimeout: sqliteFlowTestLockTimeout(), HookPolicies: h.policies, Clock: ClockFunc(func() (ClockSample, error) {
		h.clockCalls++
		return h.sample, h.clockErr
	})})
}

func hxQASend(h *hiQAFixture, at int64, e HostEvent) HostReceipt {
	h.t.Helper()
	h.at(at)
	r, err := h.s.IngestHost(context.Background(), e)
	if err != nil || r.Origin != "unverified" || r.Durability != "committed" {
		h.t.Fatal("public SQLite host callback", e.Kind, err, r)
	}
	return r
}

func TestSQLiteHostExtraClaudeTaskObservationsDoNotMoveCapture(t *testing.T) {
	h := hiQANew(t, "claude", 1)
	hxQAPublic(h)
	start := h.event("SessionStart", "", "")
	start.SessionSource = "startup"
	hxQASend(h, 0, start)
	root := hxQASend(h, 0, h.event("UserPromptSubmit", "prompt", ""))
	child := hxQASend(h, 5, h.event("SubagentStart", "prompt", "child"))
	before := h.snapshot()
	calls := h.clockCalls
	h.clockErr = errors.New("task observations must not sample capture clock")
	for _, tc := range []struct{ kind, agent string }{{"TaskCreated", ""}, {"TaskCompleted", "child"}} {
		r := hxQASend(h, 7, h.event(tc.kind, "prompt", tc.agent))
		if r.Kind != tc.kind || r.Disposition == "duplicate" || r.Ordering != "supported" {
			t.Fatal("task observation lost its independent receipt", r)
		}
	}
	if h.clockCalls != calls {
		t.Fatal("task observation sampled the clock")
	}
	after := h.snapshot()
	for _, table := range []string{"actors", "segments", "uncertainties", "intervals", "outbox", "host_tools"} {
		if !reflect.DeepEqual(before.Rows[table], after.Rows[table]) {
			t.Fatal("task observation changed timed membership", table)
		}
	}
	if h.actor(root.Actor).State != "working" || h.actor(child.Actor).State != "working" {
		t.Fatal("task observation moved root or child")
	}
	h.clockErr = nil
	hxQASend(h, 10, h.event("SubagentStop", "prompt", "child"))
	hxQASend(h, 20, h.event("Stop", "prompt", ""))
	h.intervals("3", [2]int64{0, 20})
}

func TestSQLiteHostExtraPermissionPostCannotRestoreCertainty(t *testing.T) {
	h := hiQANew(t, "codex", 1)
	hxQAPublic(h)
	start := h.event("SessionStart", "", "")
	start.SessionSource = "startup"
	hxQASend(h, 0, start)
	root := hxQASend(h, 0, h.event("UserPromptSubmit", "turn", ""))
	tool := h.event("PreToolUse", "turn", "")
	tool.ToolID, tool.ToolName = "tool", "Bash"
	hxQASend(h, 5, tool)
	permission := h.event("PermissionRequest", "turn", "")
	permission.ToolName = "Bash"
	r := hxQASend(h, 10, permission)
	before := h.snapshot()
	if r.Disposition != "review_required" || r.DiagnosticCode != "ordering_unavailable" || !reflect.DeepEqual(r.Actor, root.Actor) || len(before.Rows["uncertainties"]) != 1 || h.actor(root.Actor).Health != "stale" {
		t.Fatal("uncorrelated permission did not retain exact original-target loss", r)
	}
	tool.Kind = "PostToolUse"
	hxQASend(h, 20, tool)
	hxQASend(h, 30, h.event("Stop", "turn", ""))
	after := h.snapshot()
	if len(after.Rows["uncertainties"]) != 1 || before.Rows["uncertainties"][0][0] != after.Rows["uncertainties"][0][0] || len(after.Rows["intervals"]) != 0 || len(after.Rows["outbox"]) != 0 || h.actor(root.Actor).Health == "continuous" {
		t.Fatal("post/terminal fabricated permission completion or billable time")
	}
	h.reopen()
	hxQAPublic(h)
	h.clockErr = errors.New("permission replay must precede clock")
	prior := h.snapshot()
	replay, err := h.s.IngestHost(context.Background(), permission)
	if err != nil || replay.ID != r.ID || replay.Disposition != "duplicate" || !reflect.DeepEqual(replay.Actor, r.Actor) {
		t.Fatal("exact permission review receipt did not replay", err, replay)
	}
	hiQANonceOnly(t, prior, h.snapshot())
}

func TestSQLiteHostExtraSessionEndTargetsOnlyRegisteredRoot(t *testing.T) {
	h := hiQANew(t, "codex", 1)
	hxQAPublic(h)
	start := h.event("SessionStart", "", "")
	start.SessionSource = "startup"
	hxQASend(h, 0, start)
	root := hxQASend(h, 0, h.event("UserPromptSubmit", "root", ""))
	child := hxQASend(h, 5, h.event("SubagentStart", "child", "child"))
	end := h.event("SessionEnd", "", "")
	r := hxQASend(h, 10, end)
	if !reflect.DeepEqual(r.Actor, root.Actor) || h.actor(root.Actor).State != "interrupted" || h.actor(child.Actor).State != "working" || h.actor(child.Actor).Health != "continuous" {
		t.Fatal("SessionEnd cascaded to child or lost registered root", r)
	}
	h.intervals("3")
	before := h.snapshot()
	if len(before.Rows["uncertainties"]) != 1 || len(before.Rows["outbox"]) != 0 {
		t.Fatal("SessionEnd billed an unobserved root tail")
	}
	h.reopen()
	hxQAPublic(h)
	h.clockErr = errors.New("terminal replay must precede clock")
	replay, err := h.s.IngestHost(context.Background(), end)
	if err != nil || replay.ID != r.ID || replay.Disposition != "duplicate" || !reflect.DeepEqual(replay.Actor, root.Actor) {
		t.Fatal("SessionEnd replay changed exact terminal target", err, replay)
	}
	hiQANonceOnly(t, before, h.snapshot())
}

func TestSQLiteHostExtraClearIsFreshOnlyAfterNewWork(t *testing.T) {
	h := hiQANew(t, "codex", 1)
	hxQAPublic(h)
	start := h.event("SessionStart", "", "")
	start.SessionSource = "startup"
	hxQASend(h, 0, start)
	root := hxQASend(h, 0, h.event("UserPromptSubmit", "root", ""))
	child := hxQASend(h, 10, h.event("SubagentStart", "child", "child"))
	clear := h.event("SessionStart", "", "")
	clear.SessionSource = "clear"
	first := hxQASend(h, 20, clear)
	if h.actor(root.Actor).State != "interrupted" || h.actor(child.Actor).State != "working" || h.actor(child.Actor).Health != "stale" {
		t.Fatal("clear did not conservatively separate root and child")
	}
	before := h.snapshot()
	if len(before.Rows["uncertainties"]) != 2 || len(before.Rows["intervals"]) != 0 || len(before.Rows["outbox"]) != 0 {
		t.Fatal("clear manufactured a completed interval")
	}
	h.reopen()
	hxQAPublic(h)
	h.clockErr = errors.New("same boundary replay must not sample")
	replay, err := h.s.IngestHost(context.Background(), clear)
	if err != nil || replay.ID != first.ID || replay.Disposition != "duplicate" {
		t.Fatal("clear before new work was not replayed", err, replay)
	}
	hiQANonceOnly(t, before, h.snapshot())
	h.clockErr = nil
	next := hxQASend(h, 30, h.event("UserPromptSubmit", "new-root", ""))
	if root.Actor == nil || next.Actor == nil || *next.Actor == *root.Actor || next.Actor.Key.SessionID == root.Actor.Key.SessionID || next.Actor.Key.ComputerID != root.Actor.Key.ComputerID || next.Actor.Key.Source != root.Actor.Key.Source || next.Actor.Key.AgentID != root.Actor.Key.AgentID {
		t.Fatal("clear failed to create a fresh root incarnation")
	}
	second := hxQASend(h, 40, clear)
	if second.ID == first.ID || second.Disposition == "duplicate" || h.actor(next.Actor).State != "interrupted" {
		t.Fatal("clear after new work silently replayed old boundary", second)
	}
	h.intervals("3")
}

func TestSQLiteHostExtraDeniedPolicyChildPromptDoesNotSampleWrongGenerationOrWait(t *testing.T) {
	t.Run("stopped old child turn", func(t *testing.T) {
		h := hiQANew(t, "codex", 1)
		hxQAPublic(h)
		start := h.event("SessionStart", "", "")
		start.SessionSource = "startup"
		hxQASend(h, 0, start)
		root := hxQASend(h, 0, h.event("UserPromptSubmit", "root", ""))
		old := hxQASend(h, 10, h.event("SubagentStart", "old-child", "child"))
		hxQASend(h, 20, h.event("SubagentStop", "old-child", "child"))
		newer := hxQASend(h, 30, h.event("SubagentStart", "new-child", "child"))
		if old.Actor == nil || newer.Actor == nil || old.Actor.Key != newer.Actor.Key || old.Actor.Generation == newer.Actor.Generation {
			t.Fatal("SETUP did not supersede the exact old child generation")
		}
		h.revoke()
		before := h.snapshot()
		clockCalls := h.clockCalls
		h.clockErr = errors.New("old stopped child cannot authorize a clock sample")
		late := h.event("UserPromptSubmit", "old-child", "child")
		r := hxQASend(h, 40, late)
		if h.clockCalls != clockCalls || r.Disposition != "stale" || r.DiagnosticCode != "profile_revoked" || !reflect.DeepEqual(r.Actor, old.Actor) {
			t.Fatal("late old child prompt sampled or selected the newer generation", r)
		}
		after := h.snapshot()
		for _, table := range []string{"actors", "segments", "uncertainties", "uncertainty_evidence", "intervals", "outbox", "host_tools"} {
			if !reflect.DeepEqual(before.Rows[table], after.Rows[table]) {
				t.Fatal("old child prompt changed current capture", table)
			}
		}
		if h.actor(root.Actor).Health != "continuous" || h.actor(newer.Actor).Health != "continuous" {
			t.Fatal("old target quarantined root or new child")
		}
	})

	t.Run("current child with pending wait", func(t *testing.T) {
		h := hiQANew(t, "codex", 1)
		hxQAPublic(h)
		start := h.event("SessionStart", "", "")
		start.SessionSource = "startup"
		hxQASend(h, 0, start)
		root := hxQASend(h, 0, h.event("UserPromptSubmit", "root", ""))
		child := hxQASend(h, 5, h.event("SubagentStart", "child", "child"))
		wait := h.event("PreToolUse", "child", "child")
		wait.ToolID, wait.ToolName = "wait-1", "wait_agent"
		hxQASend(h, 10, wait)
		if h.actor(child.Actor).State != "wait_children" || h.actor(child.Actor).Health != "continuous" {
			t.Fatal("SETUP did not establish a real current child wait")
		}
		h.revoke()
		before := h.snapshot()
		rootBefore := h.actor(root.Actor)
		clockCalls := h.clockCalls
		h.clockErr = errors.New("known child wait cannot authorize a clock sample")
		r := hxQASend(h, 20, h.event("UserPromptSubmit", "child", "child"))
		if h.clockCalls != clockCalls || r.Disposition != "stale" || r.DiagnosticCode != "profile_revoked" || !reflect.DeepEqual(r.Actor, child.Actor) {
			t.Fatal("denied child prompt lost exact pending-wait target", r)
		}
		actor := h.actor(child.Actor)
		if actor.State != "wait_children" || actor.Health != "stale" || !reflect.DeepEqual(h.actor(root.Actor), rootBefore) {
			t.Fatal("wait loss resumed child or changed unrelated root", actor)
		}
		after := h.snapshot()
		for _, table := range []string{"segments", "uncertainties", "uncertainty_evidence", "intervals", "outbox", "host_tools"} {
			if !reflect.DeepEqual(before.Rows[table], after.Rows[table]) {
				t.Fatal("known idle wait gained fictitious timing", table)
			}
		}
		generation, err := sqliteEncodeUint64(child.Actor.Generation)
		if err != nil {
			t.Fatal(err)
		}
		retained := false
		for _, row := range after.Rows["host_receipts"] {
			if row[14] == "source_loss_while_waiting" && row[20] == actorKey(child.Actor.Key) && reflect.DeepEqual(row[21], generation[:]) {
				retained = true
			}
		}
		if !retained {
			t.Fatal("exact child wait-loss provenance was not retained")
		}
	})
}
