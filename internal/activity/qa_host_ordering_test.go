package activity

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestQAHostTerminalBeforeStartDoesNotResurrectTurn(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(10, h.event("Stop", "late-start", ""))
	r := h.send(20, h.event("UserPromptSubmit", "late-start", ""))
	if r.Disposition != "stale" {
		t.Fatalf("terminal tombstone allowed late start: %+v", r)
	}
	s := h.snapshot()
	qaIntervals(t, s, nil)
	for _, a := range s.Actors {
		if a.State == "working" {
			t.Fatalf("late start resurrected work: %+v", s)
		}
	}
}

func TestQAHostOpaqueOldTurnStopCannotCloseNewTurn(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "zzz-first", ""))
	h.send(10, h.event("Stop", "zzz-first", ""))
	h.send(20, h.event("UserPromptSubmit", "aaa-next", ""))
	h.send(30, h.event("Stop", "zzz-first", ""))
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 10}})
	working := 0
	for _, a := range s.Actors {
		if a.State == "working" {
			working++
		}
	}
	if working != 1 {
		t.Fatalf("old stop closed opaque new turn: %+v", s.Actors)
	}
	h.send(40, h.event("Stop", "aaa-next", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}, {20, 40}})
}

func TestQAHostDelayedToolPreCannotRewindCompletedPhase(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "turn", ""))
	e := h.event("PostToolUse", "turn", "")
	e.ToolID = "tool"
	e.ToolName = "Bash"
	h.send(10, e)
	e.Kind = "PreToolUse"
	h.send(20, e)
	s := h.snapshot()
	for _, a := range s.Actors {
		if a.State != "working" {
			t.Fatalf("late pre rewound tool/actor phase: %+v", a)
		}
	}
	// Contradictory delivery may conservatively quarantine; it cannot create a
	// fabricated permission wait or close and reopen an automatic billable span.
	h.send(30, h.event("Stop", "turn", ""))
	s = h.snapshot()
	if len(s.Uncertainties) == 0 {
		qaIntervals(t, s, [][2]int64{{0, 30}})
	} else {
		qaIntervals(t, s, nil)
	}
}

func TestQAHostContinuationFlagNeverInventsAutomaticResume(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "turn", ""))
	stop := h.event("Stop", "turn", "")
	stop.StopHookActive = true
	r := h.send(20, stop)
	s := h.snapshot()
	if r.Disposition != "review_required" || len(s.Uncertainties) == 0 {
		t.Fatalf("ambiguous continuation treated as ordinary terminal proof: receipt=%+v snapshot=%+v", r, s)
	}
	qaIntervals(t, s, nil)
}

func TestQAHostKnownCompactionPreservesActiveContinuity(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	first := h.send(0, h.event("UserPromptSubmit", "turn", ""))
	compact := h.event("SessionStart", "", "")
	compact.SessionSource = "compact"
	h.send(30, compact)
	h.send(60, h.event("Stop", "turn", ""))
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 60}})
	if len(s.Actors) != 1 || first.Actor == nil || s.Actors[0].Ref != *first.Actor || len(s.Uncertainties) != 0 {
		t.Fatalf("known compaction invented new incarnation/gap: %+v", s)
	}
}

func TestQAHostDelayedToolCannotResumeCompletedPrompt(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "turn", ""))
	h.send(10, h.event("Stop", "turn", ""))
	e := h.event("PostToolUse", "turn", "")
	e.ToolID = "late-tool"
	e.ToolName = "Bash"
	h.send(20, e)
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 10}})
	for _, a := range s.Actors {
		if a.State == "working" {
			t.Fatalf("delayed tool resumed completed prompt: %+v", a)
		}
	}
}

func TestQAHostUncorrelatedPermissionThenPostCannotRestoreContinuity(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "turn", ""))
	tool := h.event("PreToolUse", "turn", "")
	tool.ToolID = "tool"
	tool.ToolName = "Bash"
	h.send(5, tool)
	permission := h.event("PermissionRequest", "turn", "")
	permission.ToolName = "Bash"
	h.send(10, permission)
	before := h.snapshot()
	if len(before.Uncertainties) != 1 {
		t.Fatalf("permission without documented tool correlation retained certainty: %+v", before)
	}
	tool.Kind = "PostToolUse"
	h.send(20, tool)
	h.send(30, h.event("Stop", "turn", ""))
	after := h.snapshot()
	qaIntervals(t, after, nil)
	if len(after.Uncertainties) != 1 || after.Uncertainties[0].ID != before.Uncertainties[0].ID || after.Uncertainties[0].State != "unresolved" || after.Uncertainties[0].ResolutionEnd != nil {
		t.Fatalf("post fabricated permission completion/continuity: before=%+v after=%+v", before.Uncertainties, after.Uncertainties)
	}
}

func TestQAHostInterruptAndSessionEndDoNotCascadeToChild(t *testing.T) {
	for _, kind := range []string{"Interrupt", "SessionEnd"} {
		t.Run(kind, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			parent := h.send(0, h.event("UserPromptSubmit", "root-turn", ""))
			child := h.send(5, h.event("SubagentStart", "child-turn", "child"))
			terminal := h.event(kind, "root-turn", "")
			if kind == "SessionEnd" {
				terminal.TurnID = ""
			}
			h.send(10, terminal)
			s := h.snapshot()
			qaIntervals(t, s, nil)
			childWorking := false
			for _, a := range s.Actors {
				if child.Actor != nil && a.Ref == *child.Actor {
					childWorking = a.State == "working" && a.Health == "continuous"
				}
			}
			if !childWorking || len(s.Uncertainties) != 1 || parent.Actor == nil || s.Uncertainties[0].Actor != *parent.Actor {
				t.Fatalf("root terminal contaminated child or lost root tail: %+v", s)
			}
			if kind == "Interrupt" {
				r := h.send(11, h.event("SessionEnd", "", ""))
				if r.Disposition != "stale" || r.Actor == nil || *r.Actor != *parent.Actor {
					t.Fatalf("SessionEnd lost exact interrupted root: %+v", r)
				}
			}
		})
	}
}

func TestQAHostResumeCapsPriorUncertaintyAndAllowsDistinctNewTurn(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	first := h.send(0, h.event("UserPromptSubmit", "old-turn", ""))
	resume := h.event("SessionStart", "", "")
	resume.SessionSource = "resume"
	h.send(20, resume)
	next := h.send(30, h.event("UserPromptSubmit", "new-turn", ""))
	if first.Actor == nil || next.Actor == nil || *first.Actor == *next.Actor {
		t.Fatalf("resume reused uncertain incarnation: %+v %+v", first, next)
	}
	h.send(40, h.event("Stop", "new-turn", ""))
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{30, 40}})
	if len(s.Uncertainties) != 1 || s.Uncertainties[0].Actor != *first.Actor || s.Uncertainties[0].UpperBound == nil || !s.Uncertainties[0].UpperBound.Equal(qaEpochStart.Add(20*time.Second)) || s.Uncertainties[0].State != "unresolved" {
		t.Fatalf("resume lost/crossed old source boundary: %+v", s.Uncertainties)
	}
}

func TestQAHostObserveReusedNativeTupleCannotChooseNewestIncarnation(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	first := h.send(0, h.event("UserPromptSubmit", "reused-turn", ""))
	resume := h.event("SessionStart", "", "")
	resume.SessionSource = "resume"
	h.send(20, resume)
	next := h.send(30, h.event("UserPromptSubmit", "reused-turn", ""))
	if first.Actor == nil || next.Actor == nil || *first.Actor == *next.Actor {
		t.Fatalf("fixture lacks separate incarnations: %+v %+v", first, next)
	}
	before, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.service.ObserveHost(context.Background(), HostObservation{Source: "codex", SessionID: "host-session", TurnID: "reused-turn", Reason: "source_lost", RequestID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"})
	if err == nil {
		t.Fatal("ambiguous native tuple selected an incarnation")
	}
	after, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("ambiguous observation modified one of the candidate actors")
	}
}

func TestQAHostResumeDoesNotFinishHistoricalChild(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "old-root", ""))
	child := h.send(5, h.event("SubagentStart", "old-child-turn", "child"))
	resume := h.event("SessionStart", "", "")
	resume.SessionSource = "resume"
	h.send(20, resume)
	s := h.snapshot()
	if child.Actor == nil {
		t.Fatal("fixture child missing")
	}
	found := false
	for _, a := range s.Actors {
		if a.Ref == *child.Actor {
			found = true
			if a.State == "finished" || a.State == "interrupted" {
				t.Fatalf("root resume fabricated child termination: %+v", a)
			}
		}
	}
	if !found {
		t.Fatal("resume lost old child")
	}
	for _, u := range s.Uncertainties {
		if u.Actor == *child.Actor && u.UpperBound != nil {
			t.Fatalf("root resume asserted child end: %+v", u)
		}
	}
	h.send(30, h.event("UserPromptSubmit", "new-root", ""))
	stopped := h.send(35, h.event("SubagentStop", "old-child-turn", "child"))
	if stopped.Actor == nil || *stopped.Actor != *child.Actor {
		t.Fatalf("historical child stop rebound to fresh incarnation: %+v", stopped)
	}
	h.send(40, h.event("Stop", "new-root", ""))
	s = h.snapshot()
	qaIntervals(t, s, nil)
	found = false
	for _, u := range s.Uncertainties {
		if u.Actor == *child.Actor {
			found = true
			if u.UpperBound == nil || !u.UpperBound.Equal(qaEpochStart.Add(35*time.Second)) || u.State != "unresolved" {
				t.Fatalf("historical child stop lost exact unknown tail: %+v", u)
			}
		}
	}
	if !found {
		t.Fatalf("resume omitted independent child uncertainty: %+v", s.Uncertainties)
	}
}
