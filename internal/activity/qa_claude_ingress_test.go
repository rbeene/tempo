package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/hookstate"
)

type qaClaudeHarness struct{ *qaHostHarness }

func qaNewClaude(t *testing.T) *qaClaudeHarness {
	t.Helper()
	h := &qaClaudeHarness{qaNewHost(t)}
	c := hookstate.Context{Host: "claude", Scope: "project", Path: h.cwd, RuntimeVersion: "2.1.286", Surface: "local", Conflicts: []string{}}
	for _, role := range []string{"runtime", "executable", "definitions"} {
		c.Artifacts = append(c.Artifacts, hookstate.Artifact{Role: role, Path: filepath.Join(h.cwd, role)})
	}
	p, err := h.policies.Preview(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.policies.Confirm(context.Background(), hookstate.ConfirmInput{Context: p.Context, Fingerprint: p.Fingerprint, DeclarationVersion: hookstate.DeclarationVersion, RequestID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", Confirmed: true}); err != nil {
		t.Fatal(err)
	}
	return h
}
func (h *qaClaudeHarness) event(kind, turn, agent string) HostEvent {
	e := h.qaHostHarness.event(kind, turn, agent)
	e.Source = "claude"
	return e
}
func (h *qaClaudeHarness) startSession() {
	e := h.event("SessionStart", "", "")
	e.SessionSource = "startup"
	h.send(0, e)
}
func qaClaudeTool(h *qaClaudeHarness, kind, turn, agent, id, name string) HostEvent {
	e := h.event(kind, turn, agent)
	e.ToolID = id
	e.ToolName = name
	return e
}
func qaClaudeActor(t *testing.T, h *qaClaudeHarness, ref *ActorRef) Actor {
	t.Helper()
	if ref == nil {
		t.Fatal("missing admitted actor")
	}
	for _, a := range h.snapshot().Actors {
		if a.Ref == *ref {
			return a
		}
	}
	t.Fatal("actor disappeared")
	return Actor{}
}

func TestQAClaudeIngressAbsentNoFiles(t *testing.T) {
	h := qaNew(t)
	policy := filepath.Join(t.TempDir(), "absent", "hooks.json")
	h.service = New(Options{Path: h.path, HookPolicies: hookstate.New(hookstate.Options{Path: policy})})
	r, err := h.service.IngestHost(context.Background(), HostEvent{Source: "claude", Kind: "UserPromptSubmit", SessionID: "s", TurnID: "prompt", CWD: t.TempDir()})
	if err != nil || r.Disposition != "untracked" || r.Actor != nil || r.Durability != "not_committed" || r.Origin != "unverified" {
		t.Fatalf("unlinked Claude outcome %+v %v", r, err)
	}
	list, err := h.service.HostReceipts(context.Background(), HostReceiptFilter{Source: "claude"})
	if err != nil || list.Receipts == nil || len(list.Receipts) != 0 {
		t.Fatalf("empty source filter %+v %v", list, err)
	}
	for _, p := range []string{filepath.Dir(h.path), filepath.Dir(policy)} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unlinked initialized %s: %v", p, err)
		}
	}
}
func TestQAClaudeIngressQuietReplayAndSourceCoexistence(t *testing.T) {
	h := qaNewClaude(t)
	h.startSession()
	first := h.send(0, h.event("UserPromptSubmit", "same-prompt", ""))
	if first.Actor == nil || first.Disposition != "applied" || first.Durability != "committed" || first.ProfileBasis != "operator_declared" {
		t.Fatalf("Claude start not admitted %+v", first)
	}
	ce := h.qaHostHarness.event("SessionStart", "", "")
	ce.SessionSource = "startup"
	h.send(0, ce)
	codex := h.send(0, h.qaHostHarness.event("UserPromptSubmit", "same-prompt", ""))
	if codex.Actor == nil || *codex.Actor == *first.Actor {
		t.Fatalf("source namespace collision %+v %+v", first, codex)
	}
	h.restartHost()
	replay := h.send(20, h.event("UserPromptSubmit", "same-prompt", ""))
	if replay.ID != first.ID || replay.Disposition != "duplicate" || !reflect.DeepEqual(replay.Actor, first.Actor) {
		t.Fatalf("persisted native replay %+v", replay)
	}
	h.send(1800, h.event("Stop", "same-prompt", ""))
	qaHostState(t, h.qaHostHarness, first.Actor, "wait_user", "continuous")
	qaHostState(t, h.qaHostHarness, codex.Actor, "working", "continuous")
	h.send(1810, h.qaHostHarness.event("Stop", "same-prompt", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 1810}})
	before, _ := os.ReadFile(h.path)
	for _, source := range []string{"claude", "codex"} {
		list, err := h.service.HostReceipts(context.Background(), HostReceiptFilter{Source: source, SessionID: "host-session"})
		if err != nil || len(list.Receipts) < 3 {
			t.Fatalf("source receipts %+v %v", list, err)
		}
		for _, r := range list.Receipts {
			if r.Source != source || r.Origin != "unverified" {
				t.Fatalf("receipt provenance %+v", r)
			}
		}
	}
	after, _ := os.ReadFile(h.path)
	if string(before) != string(after) {
		t.Fatal("receipt reads mutate state")
	}
}
func TestQAClaudeIngressChildToolIdentityDoesNotSelectRoot(t *testing.T) {
	h := qaNewClaude(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "shared-prompt", ""))
	child := h.send(5, h.event("SubagentStart", "shared-prompt", "root"))
	before := qaClaudeActor(t, h, root.Actor)
	pre := h.send(10, qaClaudeTool(h, "PreToolUse", "shared-prompt", "root", "tool-child", "Read"))
	post := h.send(20, qaClaudeTool(h, "PostToolUse", "shared-prompt", "root", "tool-child", "Read"))
	if !reflect.DeepEqual(pre.Actor, child.Actor) || !reflect.DeepEqual(post.Actor, child.Actor) {
		t.Fatalf("child tool routed to another actor: %+v %+v", pre, post)
	}
	after := qaClaudeActor(t, h, root.Actor)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("child tool mutated root: before%+v after%+v", before, after)
	}
	h.send(30, h.event("SubagentStop", "shared-prompt", "root"))
	qaHostState(t, h.qaHostHarness, root.Actor, "working", "continuous")
	h.send(40, h.event("Stop", "shared-prompt", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 40}})
}
func TestQAClaudeIngressSourceObservationSurvivesRestartAndStop(t *testing.T) {
	h := qaNewClaude(t)
	h.startSession()
	r := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
	h.at(10)
	obs := HostObservation{Source: "claude", SessionID: "host-session", TurnID: "prompt", Reason: "source_lost", RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}
	if _, err := h.service.ObserveHost(context.Background(), obs); err != nil {
		t.Fatal(err)
	}
	h.restartHost()
	h.send(60, h.event("Stop", "prompt", ""))
	s := h.snapshot()
	qaIntervals(t, s, nil)
	if len(s.Uncertainties) != 1 {
		t.Fatalf("lost durable tail %+v", s.Uncertainties)
	}
	u := s.Uncertainties[0]
	if u.Actor != *r.Actor || u.State != "unresolved" || !u.LowerBound.Equal(qaEpochStart) || u.UpperBound == nil || !u.UpperBound.Equal(qaEpochStart.Add(60*time.Second)) {
		t.Fatalf("source tail changed/resolved %+v", u)
	}
	if _, err := h.service.ObserveHost(context.Background(), obs); err != nil {
		t.Fatal(err)
	}
	if len(h.snapshot().Uncertainties) != 1 {
		t.Fatal("observation replay duplicated tail")
	}
}

func TestQAClaudeIngressRetainedPolicyAcrossFreshAndForkSessions(t *testing.T) {
	h := qaNewClaude(t)
	h.startSession()
	first := h.send(0, h.event("UserPromptSubmit", "same", ""))
	h.send(10, h.event("Stop", "same", ""))
	h.restartHost()
	e := h.event("SessionStart", "", "")
	e.SessionID = "fork-session"
	e.SessionSource = "fork"
	h.send(20, e)
	e = h.event("UserPromptSubmit", "same", "")
	e.SessionID = "fork-session"
	second := h.send(20, e)
	if second.Actor == nil || first.Actor == nil || *second.Actor == *first.Actor || second.ProfileBasis != "operator_declared" {
		t.Fatalf("fresh session inherited continuity or lost retained eligibility %+v %+v", first, second)
	}
	e.Kind = "Stop"
	h.send(30, e)
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}, {20, 30}})
}
func TestQAClaudeIngressTerminalBeforeStartAndContinuationStayConservative(t *testing.T) {
	t.Run("terminal_first", func(t *testing.T) {
		h := qaNewClaude(t)
		h.startSession()
		h.send(0, h.event("Stop", "prompt", ""))
		r := h.send(10, h.event("UserPromptSubmit", "prompt", ""))
		if r.Actor != nil && r.Disposition == "applied" {
			t.Fatalf("late start resurrected tombstoned turn %+v", r)
		}
		if len(h.snapshot().Actors) != 0 {
			t.Fatal("terminal-first history admitted actor")
		}
	})
	t.Run("same_prompt_continuation", func(t *testing.T) {
		h := qaNewClaude(t)
		h.startSession()
		r := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
		h.send(10, h.event("Stop", "prompt", ""))
		h.send(20, qaClaudeTool(h, "PreToolUse", "prompt", "", "new-tool", "Read"))
		a := qaClaudeActor(t, h, r.Actor)
		if a.Health == "continuous" {
			t.Fatalf("new causal activity after ambiguous stopped prompt silently ignored %+v", a)
		}
		h.send(30, h.event("Stop", "prompt", ""))
		qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}})
	})
}
func TestQAClaudeIngressConcurrentDuplicatesAllocateExactlyOnce(t *testing.T) {
	h := qaNewClaude(t)
	h.startSession()
	h.at(0)
	e := h.event("UserPromptSubmit", "prompt", "")
	type outcome struct {
		r   HostReceipt
		err error
	}
	results := make(chan outcome, 8)
	for i := 0; i < 8; i++ {
		go func() { r, err := h.service.IngestHost(context.Background(), e); results <- outcome{r, err} }()
	}
	applied := 0
	var id string
	for i := 0; i < 8; i++ {
		o := <-results
		if o.err != nil {
			t.Fatal(o.err)
		}
		if o.r.Disposition == "applied" {
			applied++
		} else if o.r.Disposition != "duplicate" {
			t.Fatalf("concurrent outcome %+v", o.r)
		}
		if id != "" && id != o.r.ID {
			t.Fatal("duplicate concurrent ingress changed receipt identity")
		}
		id = o.r.ID
	}
	if applied != 1 || len(h.snapshot().Actors) != 1 {
		t.Fatalf("concurrent callbacks allocated %d effects", applied)
	}
	h.send(60, h.event("Stop", "prompt", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 60}})
}
