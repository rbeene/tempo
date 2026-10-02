package activity

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/rbeene/tempo/internal/hookstate"
)

func qaWaitEvent(h *qaHostHarness, kind, id, name string) HostEvent {
	e := h.event(kind, "turn", "")
	e.ToolID = id
	e.ToolName = name
	return e
}
func qaHostState(t *testing.T, h *qaHostHarness, ref *ActorRef, state, health string) {
	t.Helper()
	if ref == nil {
		t.Fatal("missing actor reference")
	}
	for _, a := range h.snapshot().Actors {
		if a.Ref == *ref {
			if a.State != state || health != "" && a.Health != health {
				t.Fatalf("actor state/health=%s/%s want %s/%s", a.State, a.Health, state, health)
			}
			return
		}
	}
	t.Fatal("actor missing from snapshot")
}
func qaWaitReview(t *testing.T, h *qaHostHarness, ref *ActorRef) {
	t.Helper()
	s := h.snapshot()
	if len(s.Uncertainties) != 0 {
		t.Fatalf("known wait became fabricated end-resolvable uncertainty: %+v", s.Uncertainties)
	}
	found := false
	for _, r := range s.CaptureReviews {
		if r.Actor != nil && ref != nil && *r.Actor == *ref && (r.DiagnosticCode == "incomplete_wait" || r.DiagnosticCode == "source_loss_while_waiting") {
			found = true
			if r.Origin != "unverified" || r.Durability != "committed" {
				t.Fatalf("invalid capture review provenance: %+v", r)
			}
		}
	}
	if !found {
		t.Fatalf("non-reconstructable wait missing durable review: %+v", s.CaptureReviews)
	}
}

func TestQAHostMatchedBuiltInWaitSplitsWorkingIntervals(t *testing.T) {
	for _, name := range []string{"wait_agent", "multi_agent_v1wait_agent"} {
		t.Run(name, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
			pre := qaWaitEvent(h, "PreToolUse", "wait-1", name)
			h.send(10, pre)
			qaHostState(t, h, root.Actor, "wait_children", "continuous")
			h.send(15, pre)
			qaHostState(t, h, root.Actor, "wait_children", "continuous")
			h.send(30, qaWaitEvent(h, "PostToolUse", "wait-1", name))
			qaHostState(t, h, root.Actor, "working", "continuous")
			h.send(40, h.event("Stop", "turn", ""))
			s := h.snapshot()
			qaIntervals(t, s, [][2]int64{{0, 10}, {30, 40}})
			if len(s.Uncertainties) != 0 || s.CaptureReviews == nil || len(s.CaptureReviews) != 0 {
				t.Fatalf("clean matched wait falsely needs review: %+v", s)
			}
		})
	}
}

func TestQAHostWaitOnlyPausesInvokingParent(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
	child := h.send(5, h.event("SubagentStart", "child-turn", "child"))
	h.send(10, qaWaitEvent(h, "PreToolUse", "wait-1", "wait_agent"))
	qaHostState(t, h, root.Actor, "wait_children", "continuous")
	qaHostState(t, h, child.Actor, "working", "continuous")
	h.send(20, h.event("SubagentStop", "child-turn", "child"))
	qaHostState(t, h, root.Actor, "wait_children", "continuous")
	h.send(30, qaWaitEvent(h, "PostToolUse", "wait-1", "wait_agent"))
	h.send(40, h.event("Stop", "turn", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 20}, {30, 40}})
}

func TestQAHostOverlappingWaitsAndOrdinaryToolUsePhaseSet(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
	h.send(10, qaWaitEvent(h, "PreToolUse", "wait-1", "wait_agent"))
	h.send(15, qaWaitEvent(h, "PreToolUse", "wait-2", "multi_agent_v1wait_agent"))
	qaHostState(t, h, root.Actor, "wait_children", "continuous")
	h.send(20, qaWaitEvent(h, "PreToolUse", "ordinary", "Bash"))
	qaHostState(t, h, root.Actor, "working", "continuous")
	h.send(25, qaWaitEvent(h, "PostToolUse", "ordinary", "Bash"))
	qaHostState(t, h, root.Actor, "wait_children", "continuous")
	h.send(30, qaWaitEvent(h, "PostToolUse", "wait-1", "wait_agent"))
	qaHostState(t, h, root.Actor, "wait_children", "continuous")
	h.send(35, qaWaitEvent(h, "PostToolUse", "wait-2", "multi_agent_v1wait_agent"))
	qaHostState(t, h, root.Actor, "working", "continuous")
	h.send(40, h.event("Stop", "turn", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}, {20, 25}, {35, 40}})
}

func TestQAHostWaitMappingNeverGuessesNames(t *testing.T) {
	for _, name := range []string{"wait", "Wait", "multi_agent_v1.wait_agent", "custom_wait_agent"} {
		t.Run(name, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
			h.send(10, qaWaitEvent(h, "PreToolUse", "tool", name))
			qaHostState(t, h, root.Actor, "working", "continuous")
			h.send(30, qaWaitEvent(h, "PostToolUse", "tool", name))
			h.send(40, h.event("Stop", "turn", ""))
			qaIntervals(t, h.snapshot(), [][2]int64{{0, 40}})
		})
	}
}

func TestQAHostIncompleteWaitTerminalPreservesPrefixAndFencesLatePost(t *testing.T) {
	for _, kind := range []string{"Stop", "Interrupt", "SessionEnd"} {
		t.Run(kind, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
			h.send(10, qaWaitEvent(h, "PreToolUse", "wait-1", "wait_agent"))
			qaHostState(t, h, root.Actor, "wait_children", "continuous")
			e := h.event(kind, "turn", "")
			if kind == "SessionEnd" {
				e.TurnID = ""
			}
			h.send(20, e)
			qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}})
			qaWaitReview(t, h, root.Actor)
			h.send(30, qaWaitEvent(h, "PostToolUse", "wait-1", "wait_agent"))
			s := h.snapshot()
			qaIntervals(t, s, [][2]int64{{0, 10}})
			qaWaitReview(t, h, root.Actor)
			for _, a := range s.Actors {
				if root.Actor != nil && a.Ref == *root.Actor && (a.State == "working" || a.Health == "continuous") {
					t.Fatalf("late post healed incomplete wait: %+v", a)
				}
			}
			before, err := os.ReadFile(h.path)
			if err != nil {
				t.Fatal(err)
			}
			h.snapshot()
			after, err := os.ReadFile(h.path)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatal("capture review projection wrote state")
			}
		})
	}
}

func TestQAHostSourceOrPermissionLossDuringWaitBlocksMatchedResume(t *testing.T) {
	for _, loss := range []string{"source", "permission"} {
		t.Run(loss, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
			h.send(10, qaWaitEvent(h, "PreToolUse", "wait-1", "wait_agent"))
			qaHostState(t, h, root.Actor, "wait_children", "continuous")
			if loss == "source" {
				h.at(15)
				if _, err := h.service.ObserveHost(context.Background(), HostObservation{Source: "codex", SessionID: "host-session", TurnID: "turn", Reason: "source_lost", RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}); err != nil {
					t.Fatal(err)
				}
			} else {
				e := h.event("PermissionRequest", "turn", "")
				e.ToolName = "Bash"
				h.send(15, e)
			}
			qaWaitReview(t, h, root.Actor)
			h.send(30, qaWaitEvent(h, "PostToolUse", "wait-1", "wait_agent"))
			h.send(40, h.event("Stop", "turn", ""))
			s := h.snapshot()
			qaIntervals(t, s, [][2]int64{{0, 10}})
			qaWaitReview(t, h, root.Actor)
			for _, a := range s.Actors {
				if a.Ref == *root.Actor && a.Health == "continuous" {
					t.Fatalf("matched post erased waiting-source loss: %+v", a)
				}
			}
			fresh := h.send(50, h.event("UserPromptSubmit", "fresh-turn", ""))
			qaHostState(t, h, fresh.Actor, "working", "continuous")
			h.send(60, h.event("Stop", "fresh-turn", ""))
			qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}, {50, 60}})
			qaWaitReview(t, h, root.Actor)
		})
	}
}

func TestQAHostUnmatchedWaitPostCannotAuthorizeLatePreOrBillGap(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
	h.send(10, qaWaitEvent(h, "PostToolUse", "unmatched", "wait_agent"))
	h.send(20, qaWaitEvent(h, "PreToolUse", "unmatched", "wait_agent"))
	for _, a := range h.snapshot().Actors {
		if a.Ref == *root.Actor && a.State == "wait_children" {
			t.Fatalf("late pre rewound completed wait phase: %+v", a)
		}
	}
	h.send(30, h.event("Stop", "turn", ""))
	s := h.snapshot()
	qaIntervals(t, s, nil)
	if len(s.Uncertainties) == 0 {
		t.Fatalf("unmatched wait post silently certified unknown wait gap: %+v", s)
	}
}

func TestQAHostWaitWithoutNativeToolIDDoesNotMutate(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
	before, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	h.at(10)
	_, err = h.service.IngestHost(context.Background(), qaWaitEvent(h, "PreToolUse", "", "wait_agent"))
	qaCode(t, err, "validation")
	after, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("missing tool ID invented wait membership")
	}
	qaHostState(t, h, root.Actor, "working", "continuous")
}

func TestQAHostClockLossWhileWaitingCannotResumeAndNewGenerationIsUsable(t *testing.T) {
	for _, mode := range []string{"ordinary_pre", "matched_post"} {
		t.Run(mode, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
			h.send(10, qaWaitEvent(h, "PreToolUse", "wait-1", "wait_agent"))
			qaHostState(t, h, root.Actor, "wait_children", "continuous")
			h.at(15)
			h.clockErr = errors.New("synthetic unavailable clock while known waiting")
			event := qaWaitEvent(h, "PreToolUse", "ordinary", "Bash")
			if mode == "matched_post" {
				event = qaWaitEvent(h, "PostToolUse", "wait-1", "wait_agent")
			}
			r, err := h.service.IngestHost(context.Background(), event)
			qaCode(t, err, "clock_unavailable")
			if r.Durability != "committed" {
				t.Fatalf("clock loss while waiting not durably reviewed: %+v", r)
			}
			h.at(20)
			qaWaitReview(t, h, root.Actor)
			if mode == "ordinary_pre" {
				h.send(20, qaWaitEvent(h, "PostToolUse", "ordinary", "Bash"))
				h.send(30, qaWaitEvent(h, "PostToolUse", "wait-1", "wait_agent"))
			} else {
				h.send(20, qaWaitEvent(h, "PreToolUse", "ordinary", "Bash"))
				h.send(25, qaWaitEvent(h, "PostToolUse", "ordinary", "Bash"))
				h.at(30)
				_, err = h.service.IngestHost(context.Background(), event)
				qaCode(t, err, "clock_unavailable")
			}
			h.send(40, h.event("Stop", "turn", ""))
			qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}})
			qaWaitReview(t, h, root.Actor)
			fresh := h.send(50, h.event("UserPromptSubmit", "fresh-turn", ""))
			qaHostState(t, h, fresh.Actor, "working", "continuous")
			h.send(60, h.event("Stop", "fresh-turn", ""))
			qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}, {50, 60}})
			qaWaitReview(t, h, root.Actor)
		})
	}
}

func TestQAHostGlobalClockObservationFencesWaitingRoot(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
	h.send(10, qaWaitEvent(h, "PreToolUse", "wait-1", "wait_agent"))
	qaHostState(t, h, root.Actor, "wait_children", "continuous")
	h.at(20)
	h.clockErr = errors.New("synthetic globally unavailable clock")
	observed, err := h.service.ObserveClock(context.Background(), ClockObservation{RequestID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"})
	if err != nil {
		t.Fatal(err)
	}
	h.at(25)
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}})
	qaWaitReview(t, h, root.Actor)
	if !observed.Changed {
		t.Fatal("global observation did not affect waiting root continuity")
	}
	h.send(30, qaWaitEvent(h, "PostToolUse", "wait-1", "wait_agent"))
	h.send(40, h.event("Stop", "turn", ""))
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 10}})
	qaWaitReview(t, h, root.Actor)
	found := false
	for _, r := range s.CaptureReviews {
		if r.Actor != nil && *r.Actor == *root.Actor {
			found = true
			if r.Source != "codex" || r.SessionID != "host-session" || r.TurnID != "turn" {
				t.Fatalf("global clock review lost exact waiting-root provenance: %+v", r)
			}
		}
	}
	if !found {
		t.Fatal("missing root capture review")
	}
}

func TestQAHostPolicyRevocationWhileWaitingRetainsReview(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
	h.send(10, qaWaitEvent(h, "PreToolUse", "wait-1", "wait_agent"))
	p, err := h.policies.Eligibility(context.Background(), "codex", h.cwd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.policies.Revoke(context.Background(), hookstate.RevokeInput{Host: "codex", Scope: "project", Path: p.Context.Path, IfRevision: p.Revision, RequestID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", Confirmed: true}); err != nil {
		t.Fatal(err)
	}
	h.send(30, qaWaitEvent(h, "PostToolUse", "wait-1", "wait_agent"))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}})
	qaWaitReview(t, h, root.Actor)
	h.restartHost()
	qaWaitReview(t, h, root.Actor)
}

func TestQAHostResumeWhileWaitingRetainsReview(t *testing.T) {
	for _, source := range []string{"resume", "clear"} {
		t.Run(source, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
			h.send(10, qaWaitEvent(h, "PreToolUse", "wait-1", "wait_agent"))
			e := h.event("SessionStart", "", "")
			e.SessionSource = source
			h.send(20, e)
			qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}})
			qaWaitReview(t, h, root.Actor)
			fresh := h.send(30, h.event("UserPromptSubmit", "fresh-turn", ""))
			qaHostState(t, h, fresh.Actor, "working", "continuous")
			h.send(40, h.event("Stop", "fresh-turn", ""))
			qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}, {30, 40}})
			qaWaitReview(t, h, root.Actor)
		})
	}
}

func TestQAHostSharedLossEntriesRetainWaitingReview(t *testing.T) {
	for _, mode := range []string{"observe_source", "interrupt", "superseding_prompt", "conflict"} {
		t.Run(mode, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
			pre := qaWaitEvent(h, "PreToolUse", "wait-1", "wait_agent")
			h.send(10, pre)
			h.at(20)
			switch mode {
			case "observe_source":
				_, err := h.service.ObserveSource(context.Background(), SourceObservation{Actor: *root.Actor, Reason: "source_lost", RequestID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"})
				if err != nil {
					t.Fatal(err)
				}
			case "interrupt":
				var target Actor
				for _, a := range h.snapshot().Actors {
					if a.Ref == *root.Actor {
						target = a
					}
				}
				_, err := h.service.Interrupt(context.Background(), InterruptInput{ActorID: target.ID, Generation: target.Ref.Generation, IfRevision: target.Revision, RequestID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", Confirmed: true})
				if err != nil {
					t.Fatal(err)
				}
			case "superseding_prompt":
				h.send(20, h.event("UserPromptSubmit", "superseding-turn", ""))
				h.send(25, h.event("Stop", "superseding-turn", ""))
			case "conflict":
				pre.ToolName = "Bash"
				r, err := h.service.IngestHost(context.Background(), pre)
				qaCode(t, err, "event_conflict")
				if r.Durability != "committed" {
					t.Fatalf("conflict loss not durable: %+v", r)
				}
			}
			qaWaitReview(t, h, root.Actor)
			h.send(30, qaWaitEvent(h, "PostToolUse", "wait-1", "wait_agent"))
			h.send(35, h.event("Stop", "turn", ""))
			bounds := [][2]int64{{0, 10}}
			if mode == "superseding_prompt" {
				bounds = append(bounds, [2]int64{20, 25})
			}
			qaIntervals(t, h.snapshot(), bounds)
			fresh := h.send(40, h.event("UserPromptSubmit", "fresh-turn", ""))
			qaHostState(t, h, fresh.Actor, "working", "continuous")
			h.send(50, h.event("Stop", "fresh-turn", ""))
			bounds = append(bounds, [2]int64{40, 50})
			qaIntervals(t, h.snapshot(), bounds)
			qaWaitReview(t, h, root.Actor)
			for _, r := range h.snapshot().CaptureReviews {
				if r.Actor != nil && *r.Actor == *root.Actor && (r.Source != "codex" || r.SessionID != "host-session" || r.TurnID != "turn") {
					t.Fatalf("loss review changed actor provenance: %+v", r)
				}
			}
		})
	}
}

func TestQAHostAmbiguousCallbackPreservesWaitingCandidateReview(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
	h.send(10, qaWaitEvent(h, "PreToolUse", "wait-1", "wait_agent"))
	child := h.send(15, h.event("SubagentStart", "turn", "child"))
	r := h.send(20, qaWaitEvent(h, "PostToolUse", "wait-1", "wait_agent"))
	if r.Actor != nil || r.Disposition != "review_required" {
		t.Fatalf("ambiguous callback guessed candidate: %+v", r)
	}
	s := h.snapshot()
	found := false
	for _, review := range s.CaptureReviews {
		if review.Actor != nil && *review.Actor == *root.Actor {
			found = true
			if review.Source != "codex" || review.SessionID != "host-session" || review.TurnID != "turn" || review.DiagnosticCode != "source_loss_while_waiting" || review.Durability != "committed" {
				t.Fatalf("ambiguous loss review lost exact provenance: %+v", review)
			}
		}
	}
	if !found {
		t.Fatalf("ambiguous receipt discarded waiting candidate review: %+v", s.CaptureReviews)
	}
	for _, u := range s.Uncertainties {
		if u.Actor == *root.Actor {
			t.Fatalf("known root wait became synthetic uncertainty: %+v", u)
		}
	}
	qaIntervals(t, s, [][2]int64{{0, 10}})
	h.send(25, h.event("SubagentStop", "turn", "child"))
	qaHostState(t, h, child.Actor, "wait_user", "stale")
	fresh := h.send(30, h.event("UserPromptSubmit", "fresh-turn", ""))
	qaHostState(t, h, fresh.Actor, "working", "continuous")
}
