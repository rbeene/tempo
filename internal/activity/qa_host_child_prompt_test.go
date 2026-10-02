package activity

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
)

// A decoded child prompt must preserve its registered child identity and never
// allocate a new parent generation. Raw decoding is covered in cli_test.
func TestQAHostRegisteredChildPromptPreservesParent(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "parent-turn", ""))
	child := h.send(10, h.event("SubagentStart", "child-turn", "child-agent"))
	h.send(20, h.event("Stop", "parent-turn", ""))
	qaHostState(t, h, root.Actor, "wait_user", "continuous")
	qaHostState(t, h, child.Actor, "working", "continuous")
	prompt := h.event("UserPromptSubmit", "child-turn", "child-agent")
	got := h.send(30, prompt)
	if got.Actor == nil || *got.Actor != *child.Actor || got.Durability != "committed" {
		t.Fatalf("registered child prompt fabricated root identity: child=%+v receipt=%+v", child.Actor, got)
	}
	qaHostState(t, h, root.Actor, "wait_user", "continuous")
	qaHostState(t, h, child.Actor, "working", "continuous")
	if s := h.snapshot(); len(s.Actors) != 2 || len(s.Uncertainties) != 0 {
		t.Fatalf("child prompt created phantom generation or uncertainty: %+v", s)
	}
	h.restartHost()
	before, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	replay := h.send(35, prompt)
	if replay.ID != got.ID || replay.Disposition != "duplicate" || replay.Actor == nil || *replay.Actor != *child.Actor {
		t.Fatalf("child prompt replay changed identity: first=%+v replay=%+v", got, replay)
	}
	after, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("child prompt replay mutated durable history")
	}
	h.send(40, h.event("SubagentStop", "child-turn", "child-agent"))
	qaHostState(t, h, root.Actor, "wait_user", "continuous")
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 40}})
	next := h.send(50, h.event("UserPromptSubmit", "next-parent-turn", ""))
	if next.Actor == nil || next.Actor.Key != root.Actor.Key || next.Actor.Generation != "2" {
		t.Fatalf("child prompt consumed parent generation: root=%+v next=%+v", root.Actor, next)
	}
	h.send(60, h.event("SessionEnd", "", ""))
	qaHostState(t, h, next.Actor, "interrupted", "stale")
	s := h.snapshot()
	if len(s.Uncertainties) != 1 || s.Uncertainties[0].Actor != *next.Actor {
		t.Fatalf("session end failed exact new parent targeting: %+v", s.Uncertainties)
	}
	_, err = h.service.HostReceipts(context.Background(), HostReceiptFilter{Source: "codex"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestQAHostExplicitChildIdentityCannotSelectCollidingRoot(t *testing.T) {
	for _, kind := range []string{"UserPromptSubmit", "PreToolUse", "PostToolUse", "PermissionRequest", "PreCompact", "PostCompact"} {
		t.Run(kind, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			root := h.send(0, h.event("UserPromptSubmit", "shared-turn", ""))
			child := h.send(10, h.event("SubagentStart", "shared-turn", "child-agent"))
			before := h.snapshot()
			e := h.event(kind, "shared-turn", "child-agent")
			if kind == "PreToolUse" || kind == "PostToolUse" {
				e.ToolID = "child-tool"
				e.ToolName = "shell"
			}
			if kind == "PostToolUse" {
				pre := e
				pre.Kind = "PreToolUse"
				h.send(15, pre)
			}
			if kind == "PreCompact" || kind == "PostCompact" {
				h.at(20)
				_, err := h.service.IngestHost(context.Background(), e)
				var ae *Error
				if !errors.As(err, &ae) || ae.Code != "unsupported_contract" {
					t.Fatalf("existing compact rejection changed: %v", err)
				}
				if !reflect.DeepEqual(before.Actors, h.snapshot().Actors) {
					t.Fatal("unsupported compact mutated actors")
				}
				return
			}
			r := h.send(20, e)
			if r.Actor == nil || *r.Actor != *child.Actor {
				t.Fatalf("explicit child picked ambiguous/root actor: %+v", r)
			}
			after := h.snapshot()
			for _, a := range after.Actors {
				if a.Ref == *root.Actor {
					for _, old := range before.Actors {
						if old.Ref == *root.Actor && (a.Revision != old.Revision || !reflect.DeepEqual(a.LastEvidence, old.LastEvidence) || a.State != old.State || a.Health != old.Health) {
							t.Fatalf("child callback mutated colliding parent: before=%+v after=%+v", old, a)
						}
					}
				}
			}
			if len(after.Actors) != 2 {
				t.Fatalf("child callback allocated phantom actor: %+v", after.Actors)
			}
		})
	}
}

func TestQAHostDelayedChildPromptCannotResurrectStoppedGeneration(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "root-turn", ""))
	child := h.send(10, h.event("SubagentStart", "old-child-turn", "child-agent"))
	h.send(20, h.event("SubagentStop", "old-child-turn", "child-agent"))
	newer := h.send(30, h.event("SubagentStart", "new-child-turn", "child-agent"))
	r := h.send(40, h.event("UserPromptSubmit", "old-child-turn", "child-agent"))
	if r.Disposition != "stale" || r.Actor == nil || *r.Actor != *child.Actor {
		t.Fatalf("late child prompt resurrected or selected newer actor: %+v", r)
	}
	qaHostState(t, h, root.Actor, "working", "continuous")
	qaHostState(t, h, newer.Actor, "working", "continuous")
	if len(h.snapshot().Actors) != 2 {
		t.Fatal("late child prompt created phantom actor")
	}
}

func TestQAHostWrongExplicitChildToolCannotSelectKnownActor(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "shared-turn", ""))
	h.send(10, h.event("SubagentStart", "shared-turn", "known-child"))
	before := h.snapshot()
	e := h.event("PreToolUse", "shared-turn", "different-child")
	e.ToolID = "other-tool"
	e.ToolName = "shell"
	r := h.send(20, e)
	if r.Actor != nil || r.Disposition != "review_required" {
		t.Fatalf("unknown explicit child selected a known actor: %+v", r)
	}
	if !reflect.DeepEqual(before.Actors, h.snapshot().Actors) {
		t.Fatal("wrong child callback changed known actors")
	}
}

func TestQAHostFreshExplicitChildPromptCannotRepointSessionRoot(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "root-turn", ""))
	child := h.send(10, h.event("UserPromptSubmit", "child-turn", "child-agent"))
	if child.Actor == nil || child.Actor.Key == root.Actor.Key {
		t.Fatalf("child allocated root identity: %+v", child)
	}
	h.send(20, h.event("SessionEnd", "", ""))
	qaHostState(t, h, root.Actor, "interrupted", "stale")
	qaHostState(t, h, child.Actor, "working", "continuous")
	// SessionEnd targets the known root and cannot cap the independent child.
	s := h.snapshot()
	for _, u := range s.Uncertainties {
		if u.Actor == *child.Actor && u.UpperBound != nil {
			t.Fatalf("SessionEnd capped independent child: %+v", u)
		}
	}
}

func TestQAHostHistoricalChildPromptCannotAllocateAfterResume(t *testing.T) {
	for _, priorPrompt := range []bool{false, true} {
		name := "late_first_prompt"
		if priorPrompt {
			name = "exact_replay"
		}
		t.Run(name, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			h.send(0, h.event("UserPromptSubmit", "old-root", ""))
			child := h.send(10, h.event("SubagentStart", "old-child-turn", "child-agent"))
			e := h.event("UserPromptSubmit", "old-child-turn", "child-agent")
			var original HostReceipt
			if priorPrompt {
				original = h.send(15, e)
			}
			boundary := h.event("SessionStart", "", "")
			boundary.SessionSource = "resume"
			h.send(20, boundary)
			root := h.send(30, h.event("UserPromptSubmit", "new-root", ""))
			before := h.snapshot()
			bytesBefore, err := os.ReadFile(h.path)
			if err != nil {
				t.Fatal(err)
			}
			r := h.send(40, e)
			after := h.snapshot()
			if r.Actor == nil || *r.Actor != *child.Actor {
				t.Fatalf("historical child prompt minted or selected a new incarnation: old=%+v got=%+v", child.Actor, r)
			}
			if !reflect.DeepEqual(before.Actors, after.Actors) {
				t.Fatalf("historical child prompt changed actors: before=%+v after=%+v", before.Actors, after.Actors)
			}
			qaHostState(t, h, root.Actor, "working", "continuous")
			if priorPrompt {
				bytesAfter, err := os.ReadFile(h.path)
				if err != nil {
					t.Fatal(err)
				}
				if r.ID != original.ID || r.Disposition != "duplicate" || string(bytesBefore) != string(bytesAfter) {
					t.Fatalf("historical child receipt replay not exact: first=%+v replay=%+v", original, r)
				}
			} else if r.Disposition != "stale" {
				t.Fatalf("historical late first prompt resumed work: %+v", r)
			}
		})
	}
}
