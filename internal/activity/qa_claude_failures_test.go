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

func TestQAClaudeTaskObservationsDoNotSampleClockOrChangeMembership(t *testing.T) {
	h := qaNewClaude(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "prompt", ""))
	h.send(5, h.event("SubagentStart", "prompt", "child"))
	before := h.snapshot().Actors
	h.clockErr = errors.New("task must not require timing evidence")
	for _, kind := range []string{"TaskCreated", "TaskCompleted"} {
		for _, agent := range []string{"", "child"} {
			r, err := h.service.IngestHost(context.Background(), h.event(kind, "prompt", agent))
			if err != nil || r.Kind != kind || r.Origin != "unverified" {
				t.Fatalf("task observation %+v %v", r, err)
			}
		}
	}
	h.clockErr = nil
	if !reflect.DeepEqual(before, h.snapshot().Actors) || len(h.snapshot().Uncertainties) != 0 {
		t.Fatal("task observation changed timed membership/evidence")
	}
	h.send(10, h.event("SubagentStop", "prompt", "child"))
	h.send(20, h.event("Stop", "prompt", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 20}})
}
func TestQAClaudeSessionEndCannotBillUnobservedInterruptionOrCapChild(t *testing.T) {
	h := qaNewClaude(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
	child := h.send(5, h.event("SubagentStart", "child-prompt", "child"))
	h.at(20)
	if _, err := h.service.ObserveHost(context.Background(), HostObservation{Source: "claude", SessionID: "host-session", TurnID: "child-prompt", AgentID: "child", Reason: "source_lost", RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}); err != nil {
		t.Fatal(err)
	}
	h.send(600, h.event("SessionEnd", "", ""))
	h.restartHost()
	s := h.snapshot()
	qaIntervals(t, s, nil)
	if len(s.Uncertainties) != 2 {
		t.Fatalf("exit lost independent tails %+v", s.Uncertainties)
	}
	for _, u := range s.Uncertainties {
		switch u.Actor {
		case *root.Actor:
			if u.UpperBound == nil || !u.UpperBound.Equal(qaEpochStart.Add(600*time.Second)) || !u.LowerBound.Equal(qaEpochStart) {
				t.Fatalf("wrong root loss bound %+v", u)
			}
		case *child.Actor:
			if u.UpperBound != nil || !u.LowerBound.Equal(qaEpochStart.Add(5*time.Second)) {
				t.Fatalf("root exit capped independent child %+v", u)
			}
		default:
			t.Fatalf("unknown tail %+v", u)
		}
	}
}
func TestQAClaudeCompletedStopThenSessionEndAddsNoIdleOrReview(t *testing.T) {
	h := qaNewClaude(t)
	h.startSession()
	r := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
	h.send(10, h.event("Stop", "prompt", ""))
	h.send(600, h.event("SessionEnd", "", ""))
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 10}})
	if len(s.Uncertainties) != 0 || len(s.CaptureReviews) != 0 {
		t.Fatalf("normal completed wait confused with missing question %+v", s)
	}
	a := qaClaudeActor(t, h, r.Actor)
	if a.State != "interrupted" {
		t.Fatalf("completed waiting root was not detached: %+v", a)
	}
}
func TestQAClaudeStopFailurePreservesOnlyExactActorUnknownTail(t *testing.T) {
	h := qaNewClaude(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
	child := h.send(5, h.event("SubagentStart", "prompt", "child"))
	before := qaClaudeActor(t, h, root.Actor)
	h.send(10, qaClaudeTool(h, "PreToolUse", "prompt", "child", "tool", "Read"))
	r := h.send(20, h.event("StopFailure", "prompt", "child"))
	if r.Kind != "StopFailure" || !reflect.DeepEqual(r.Actor, child.Actor) {
		t.Fatalf("failure translated or misrouted %+v", r)
	}
	if !reflect.DeepEqual(before, qaClaudeActor(t, h, root.Actor)) {
		t.Fatal("child failure mutated root")
	}
	s := h.snapshot()
	if len(s.Uncertainties) != 1 || s.Uncertainties[0].Actor != *child.Actor || !s.Uncertainties[0].LowerBound.Equal(qaEpochStart.Add(10*time.Second)) {
		t.Fatalf("failure auto-confirmed unknown tail %+v", s.Uncertainties)
	}
	h.send(30, h.event("Stop", "prompt", ""))
	h.restartHost()
	if len(h.snapshot().Uncertainties) != 1 {
		t.Fatal("later root stop erased child failure")
	}
}
func TestQAClaudeRepeatedStoppedChildIsReviewNotNewGeneration(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(map[bool]string{false: "same", true: "newer_child"}[replacement], func(t *testing.T) {
			h := qaNewClaude(t)
			h.startSession()
			h.send(0, h.event("UserPromptSubmit", "prompt", ""))
			start := h.send(5, h.event("SubagentStart", "prompt", "child"))
			h.send(10, h.event("SubagentStop", "prompt", "child"))
			target := start.Actor
			if replacement {
				next := h.send(15, h.event("SubagentStart", "next-prompt", "child"))
				target = next.Actor
			}
			before := qaClaudeActor(t, h, target)
			original, err := h.service.HostReceipts(context.Background(), HostReceiptFilter{Source: "claude"})
			if err != nil {
				t.Fatal(err)
			}
			h.at(20)
			r, err := h.service.IngestHost(context.Background(), h.event("SubagentStart", "prompt", "child"))
			if err != nil {
				t.Fatal(err)
			}
			if r.Disposition != "review_required" || r.ID == start.ID {
				t.Fatalf("ambiguous resumed child silently replayed/allocated %+v", r)
			}
			after := qaClaudeActor(t, h, target)
			if after.Ref != before.Ref || replacement && !reflect.DeepEqual(before, after) {
				t.Fatalf("ambiguous old start mutated newer generation: %+v %+v", before, after)
			}
			current, err := h.service.HostReceipts(context.Background(), HostReceiptFilter{Source: "claude"})
			if err != nil {
				t.Fatal(err)
			}
			var old, now HostReceipt
			for _, x := range original.Receipts {
				if x.ID == start.ID {
					old = x
				}
			}
			for _, x := range current.Receipts {
				if x.ID == start.ID {
					now = x
				}
			}
			if old.ID == "" || !reflect.DeepEqual(old, now) {
				t.Fatal("ambiguity rewrote original accepted start receipt")
			}
		})
	}
}
func TestQAClaudeDurabilityFaultsReconcileWithoutFabricatedLoss(t *testing.T) {
	for _, stage := range []string{"before_write", "directory_sync"} {
		t.Run(stage, func(t *testing.T) {
			h := qaNewClaude(t)
			h.startSession()
			h.at(0)
			before, _ := os.ReadFile(h.path)
			h.service.store.fail = func(s string) error {
				if s == stage {
					return errors.New("synthetic failure")
				}
				return nil
			}
			e := h.event("UserPromptSubmit", "prompt", "")
			r, err := h.service.IngestHost(context.Background(), e)
			if err == nil {
				t.Fatal("fault acknowledged")
			}
			h.service.store.fail = nil
			if stage == "before_write" {
				if r.Durability != "not_committed" {
					t.Fatalf("false durability %+v", r)
				}
				after, _ := os.ReadFile(h.path)
				if string(before) != string(after) {
					t.Fatal("definite failure changed durable bytes")
				}
				h.restartHost()
				if len(h.snapshot().Actors) != 0 || len(h.snapshot().Uncertainties) != 0 {
					t.Fatal("unpersisted failure invented marker")
				}
				h.send(0, e)
			} else {
				qaCode(t, err, "local_write_unknown")
				if r.Durability != "unknown" {
					t.Fatalf("unknown false outcome %+v", r)
				}
				h.restartHost()
				h.clockErr = errors.New("replay must not sample clock")
				again, err := h.service.IngestHost(context.Background(), e)
				if err != nil || again.Disposition != "duplicate" || again.Durability != "committed" {
					t.Fatalf("reconcile %+v %v", again, err)
				}
				h.clockErr = nil
			}
			h.send(60, h.event("Stop", "prompt", ""))
			qaIntervals(t, h.snapshot(), [][2]int64{{0, 60}})
			if len(h.snapshot().Actors) != 1 || len(h.snapshot().Uncertainties) != 0 {
				t.Fatal("fault retry duplicated work or fabricated loss")
			}
		})
	}
}
func TestQAClaudeKnownPolicyLossBlocksNewWorkAndQuarantinesLiveTail(t *testing.T) {
	for _, mode := range []string{"revoke", "drift"} {
		t.Run(mode, func(t *testing.T) {
			h := qaNewClaude(t)
			h.startSession()
			h.send(0, h.event("UserPromptSubmit", "prompt", ""))
			if mode == "revoke" {
				p, err := h.policies.Eligibility(context.Background(), "claude", h.cwd)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = h.policies.Revoke(context.Background(), hookstate.RevokeInput{Host: "claude", Scope: "project", Path: p.Context.Path, IfRevision: p.Revision, RequestID: "ffffffff-ffff-4fff-8fff-ffffffffffff", Confirmed: true}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(filepath.Join(h.cwd, "runtime"), []byte("known drift"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			h.send(20, h.event("Stop", "prompt", ""))
			s := h.snapshot()
			qaIntervals(t, s, nil)
			if len(s.Uncertainties) != 1 {
				t.Fatalf("policy loss certified live tail %+v", s)
			}
			h.send(30, h.event("UserPromptSubmit", "new-prompt", ""))
			if len(h.snapshot().Actors) != 1 {
				t.Fatal("known ineligible profile admitted new work")
			}
		})
	}
}

func TestQAClaudeSessionEndReusedIncarnationCannotChooseNewestRoot(t *testing.T) {
	h := qaNewClaude(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "old", ""))
	h.send(10, h.event("Stop", "old", ""))
	e := h.event("SessionStart", "", "")
	e.SessionSource = "resume"
	h.send(20, e)
	current := h.send(30, h.event("UserPromptSubmit", "new", ""))
	before := qaClaudeActor(t, h, current.Actor)
	r := h.send(40, h.event("SessionEnd", "", ""))
	if r.Disposition != "review_required" || r.DiagnosticCode != "ordering_unavailable" {
		t.Fatalf("identity-free old exit chose current incarnation %+v", r)
	}
	if !reflect.DeepEqual(before, qaClaudeActor(t, h, current.Actor)) {
		t.Fatal("ambiguous session exit mutated new actor")
	}
	h.send(50, h.event("Stop", "new", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}, {30, 50}})
}

func TestQAClaudeTaskObservationWithRevokedPolicyStillCannotMutateTiming(t *testing.T) {
	h := qaNewClaude(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "prompt", ""))
	before := h.snapshot().Actors
	p, err := h.policies.Eligibility(context.Background(), "claude", h.cwd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.policies.Revoke(context.Background(), hookstate.RevokeInput{Host: "claude", Scope: "project", Path: h.cwd, IfRevision: p.Revision, RequestID: "ffffffff-ffff-4fff-8fff-ffffffffffff", Confirmed: true}); err != nil {
		t.Fatal(err)
	}
	h.clockErr = errors.New("observation must not sample clock")
	_, err = h.service.IngestHost(context.Background(), h.event("TaskCompleted", "prompt", ""))
	if err != nil {
		t.Fatal(err)
	}
	h.clockErr = nil
	s := h.snapshot()
	if !reflect.DeepEqual(before, s.Actors) || len(s.Uncertainties) != 0 || len(s.ClosedIntervals) != 0 {
		t.Fatal("task observation under revoked policy mutated timer")
	}
}
func TestQAClaudeAndCodexIngressEnumsRemainSourceSpecific(t *testing.T) {
	h := qaNewClaude(t)
	for _, kind := range []string{"StopFailure", "PostToolUseFailure", "TaskCreated", "TaskCompleted"} {
		e := h.qaHostHarness.event(kind, "prompt", "")
		e.ToolID = "tool"
		e.ToolName = "Read"
		_, err := h.service.IngestHost(context.Background(), e)
		qaCode(t, err, "unsupported_contract")
	}
	for _, kind := range []string{"Interrupt", "PreCompact", "PostCompact"} {
		_, err := h.service.IngestHost(context.Background(), h.event(kind, "prompt", ""))
		qaCode(t, err, "unsupported_contract")
	}
}

func TestQAClaudeReusedPromptCallbacksCannotMutateNewIncarnation(t *testing.T) {
	for _, kind := range []string{"Stop", "StopFailure", "PreToolUse", "PostToolUseFailure"} {
		t.Run(kind, func(t *testing.T) {
			h := qaNewClaude(t)
			h.startSession()
			old := h.send(0, h.event("UserPromptSubmit", "reused", ""))
			h.send(10, h.event("Stop", "reused", ""))
			resume := h.event("SessionStart", "", "")
			resume.SessionSource = "resume"
			h.send(20, resume)
			current := h.send(30, h.event("UserPromptSubmit", "reused", ""))
			if current.Actor == nil || old.Actor == nil || *current.Actor == *old.Actor {
				t.Fatal("fixture failed to establish distinct incarnations")
			}
			before := h.snapshot()
			e := h.event(kind, "reused", "")
			if kind == "PreToolUse" || kind == "PostToolUseFailure" {
				e.ToolID = "ambiguous-tool"
				e.ToolName = "Read"
			}
			r := h.send(40, e)
			if r.Disposition != "review_required" || r.DiagnosticCode != "ordering_unavailable" || r.Actor != nil || r.Durability != "committed" {
				t.Fatalf("ambiguous callback selected an incarnation %+v", r)
			}
			after := h.snapshot()
			if !reflect.DeepEqual(before.Actors, after.Actors) || !reflect.DeepEqual(before.Uncertainties, after.Uncertainties) || !reflect.DeepEqual(before.ClosedIntervals, after.ClosedIntervals) {
				t.Fatalf("ambiguous %s changed actor evidence or timing: before%+v after%+v", kind, before.Actors, after.Actors)
			}
			// A durable diagnostic may be added, but it cannot certify or fence newer work.
			h.restartHost()
			if !reflect.DeepEqual(after.Actors, h.snapshot().Actors) {
				t.Fatal("ambiguity handling not stable after restart")
			}
		})
	}
}

func TestQAClaudeDelayedStoppedChildStartAcrossResumeCannotMintActor(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_replacement", true: "newer_child"}[replacement], func(t *testing.T) {
			h := qaNewClaude(t)
			h.startSession()
			h.send(0, h.event("UserPromptSubmit", "root-old", ""))
			original := h.send(5, h.event("SubagentStart", "child-old", "child"))
			h.send(10, h.event("SubagentStop", "child-old", "child"))
			h.send(15, h.event("Stop", "root-old", ""))
			resume := h.event("SessionStart", "", "")
			resume.SessionSource = "resume"
			h.send(20, resume)
			h.send(25, h.event("UserPromptSubmit", "root-new", ""))
			if replacement {
				h.send(30, h.event("SubagentStart", "child-new", "child"))
			}
			before := h.snapshot()
			receipts, err := h.service.HostReceipts(context.Background(), HostReceiptFilter{Source: "claude"})
			if err != nil {
				t.Fatal(err)
			}
			var originalReceipt HostReceipt
			for _, r := range receipts.Receipts {
				if r.ID == original.ID {
					originalReceipt = r
				}
			}
			r := h.send(40, h.event("SubagentStart", "child-old", "child"))
			if r.Disposition != "review_required" || r.ID == original.ID || r.Durability != "committed" {
				t.Fatalf("delayed stopped child became new work or false replay %+v", r)
			}
			after := h.snapshot()
			if !reflect.DeepEqual(before.Actors, after.Actors) || !reflect.DeepEqual(before.Uncertainties, after.Uncertainties) || !reflect.DeepEqual(before.ClosedIntervals, after.ClosedIntervals) {
				t.Fatal("delayed child start allocated/mutated current actor or timing")
			}
			current, err := h.service.HostReceipts(context.Background(), HostReceiptFilter{Source: "claude"})
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, r := range current.Receipts {
				if r.ID == original.ID {
					found = true
					if !reflect.DeepEqual(originalReceipt, r) {
						t.Fatal("delayed start rewrote original receipt")
					}
				}
			}
			if !found {
				t.Fatal("original child start disappeared")
			}
		})
	}
}
