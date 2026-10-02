package activity

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func qaClaudeQuestion(h *qaClaudeHarness, kind, id string) HostEvent {
	return qaClaudeTool(h, kind, "prompt", "", id, "AskUserQuestion")
}
func TestQAClaudeQuestionMatchedSuccessOrFailureExcludesWait(t *testing.T) {
	for _, end := range []string{"PostToolUse", "PostToolUseFailure"} {
		t.Run(end, func(t *testing.T) {
			h := qaNewClaude(t)
			h.startSession()
			root := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
			h.send(10, qaClaudeQuestion(h, "PreToolUse", "q"))
			qaHostState(t, h.qaHostHarness, root.Actor, "wait_user", "continuous")
			h.send(15, qaClaudeQuestion(h, "PreToolUse", "q"))
			r := h.send(30, qaClaudeQuestion(h, end, "q"))
			if r.Kind != end || r.Actor == nil || *r.Actor != *root.Actor {
				t.Fatalf("tool terminal identity/classification changed %+v", r)
			}
			qaHostState(t, h.qaHostHarness, root.Actor, "working", "continuous")
			h.send(40, h.event("Stop", "prompt", ""))
			h.restartHost()
			s := h.snapshot()
			qaIntervals(t, s, [][2]int64{{0, 10}, {30, 40}})
			if len(s.CaptureReviews) != 0 || len(s.Uncertainties) != 0 {
				t.Fatalf("clean matched question falsely reviewed %+v", s)
			}
		})
	}
}
func TestQAClaudeQuestionPhaseSetAndIndependentChild(t *testing.T) {
	h := qaNewClaude(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
	child := h.send(5, h.event("SubagentStart", "child-prompt", "child"))
	h.send(10, qaClaudeQuestion(h, "PreToolUse", "q1"))
	h.send(12, qaClaudeQuestion(h, "PreToolUse", "q2"))
	qaHostState(t, h.qaHostHarness, root.Actor, "wait_user", "continuous")
	qaHostState(t, h.qaHostHarness, child.Actor, "working", "continuous")
	h.send(15, h.event("SubagentStop", "child-prompt", "child"))
	h.send(20, qaClaudeTool(h, "PreToolUse", "prompt", "", "ordinary", "Read"))
	qaHostState(t, h.qaHostHarness, root.Actor, "working", "continuous")
	h.send(25, qaClaudeTool(h, "PostToolUseFailure", "prompt", "", "ordinary", "Read"))
	qaHostState(t, h.qaHostHarness, root.Actor, "wait_user", "continuous")
	h.send(30, qaClaudeQuestion(h, "PostToolUse", "q1"))
	qaHostState(t, h.qaHostHarness, root.Actor, "wait_user", "continuous")
	h.send(35, qaClaudeQuestion(h, "PostToolUseFailure", "q2"))
	qaHostState(t, h.qaHostHarness, root.Actor, "working", "continuous")
	h.send(40, h.event("Stop", "prompt", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 15}, {20, 25}, {35, 40}})
}
func TestQAClaudeQuestionNamesAreSourceSpecific(t *testing.T) {
	for _, name := range []string{"wait_agent", "multi_agent_v1wait_agent", "ask_user_question", "AskUserQuestionCustom", "Agent"} {
		t.Run(name, func(t *testing.T) {
			h := qaNewClaude(t)
			h.startSession()
			r := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
			h.send(10, qaClaudeTool(h, "PreToolUse", "prompt", "", "tool", name))
			qaHostState(t, h.qaHostHarness, r.Actor, "working", "continuous")
			h.send(30, qaClaudeTool(h, "PostToolUse", "prompt", "", "tool", name))
			h.send(40, h.event("Stop", "prompt", ""))
			qaIntervals(t, h.snapshot(), [][2]int64{{0, 40}})
		})
	}
	h := qaNewHost(t)
	h.startSession()
	r := h.send(0, h.event("UserPromptSubmit", "turn", ""))
	e := h.event("PreToolUse", "turn", "")
	e.ToolID = "tool"
	e.ToolName = "AskUserQuestion"
	h.send(10, e)
	qaHostState(t, h, r.Actor, "working", "continuous")
}
func TestQAClaudeQuestionLostCompletionRetainsReviewAndFencesLatePost(t *testing.T) {
	for _, loss := range []string{"Stop", "StopFailure", "SessionEnd", "source", "shared_source", "permission", "clock", "global_clock", "resume", "public_interrupt", "next_prompt"} {
		t.Run(loss, func(t *testing.T) {
			h := qaNewClaude(t)
			h.startSession()
			r := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
			h.send(10, qaClaudeQuestion(h, "PreToolUse", "q"))
			h.at(20)
			switch loss {
			case "source":
				_, err := h.service.ObserveHost(context.Background(), HostObservation{Source: "claude", SessionID: "host-session", TurnID: "prompt", Reason: "source_lost", RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"})
				if err != nil {
					t.Fatal(err)
				}
			case "shared_source":
				_, err := h.service.ObserveSource(context.Background(), SourceObservation{Actor: *r.Actor, Reason: "source_lost", RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"})
				if err != nil {
					t.Fatal(err)
				}
			case "global_clock":
				h.clockErr = errors.New("synthetic unavailable")
				observed, err := h.service.ObserveClock(context.Background(), ClockObservation{RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"})
				if err != nil || !observed.Changed {
					t.Fatalf("global loss observation did not commit fence: %+v %v", observed, err)
				}
				h.clockErr = nil
			case "permission":
				e := h.event("PermissionRequest", "prompt", "")
				e.ToolName = "Read"
				h.send(20, e)
			case "clock":
				h.clockErr = errors.New("synthetic unavailable")
				_, err := h.service.IngestHost(context.Background(), qaClaudeQuestion(h, "PostToolUse", "q"))
				qaCode(t, err, "clock_unavailable")
				h.clockErr = nil
			case "resume":
				e := h.event("SessionStart", "", "")
				e.SessionSource = "resume"
				h.send(20, e)
			case "public_interrupt":
				a := qaClaudeActor(t, h, r.Actor)
				_, err := h.service.Interrupt(context.Background(), InterruptInput{ActorID: a.ID, Generation: r.Actor.Generation, Confirmed: true, IfRevision: a.Revision, RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"})
				if err != nil {
					t.Fatal(err)
				}
			case "next_prompt":
				h.send(20, h.event("UserPromptSubmit", "next", ""))
				h.send(25, h.event("Stop", "next", ""))
			default:
				e := h.event(loss, "prompt", "")
				if loss == "SessionEnd" {
					e.TurnID = ""
				}
				h.send(20, e)
			}
			qaWaitReview(t, h.qaHostHarness, r.Actor)
			h.restartHost()
			beforePost := h.snapshot().Actors
			if loss == "clock" {
				h.at(30)
				replay, err := h.service.IngestHost(context.Background(), qaClaudeQuestion(h, "PostToolUse", "q"))
				qaCode(t, err, "clock_unavailable")
				if replay.Durability != "committed" || replay.Disposition != "duplicate" {
					t.Fatalf("saved error replay lost committed outcome: %+v", replay)
				}
			} else {
				h.send(30, qaClaudeQuestion(h, "PostToolUse", "q"))
			}
			if loss == "next_prompt" {
				if !reflect.DeepEqual(beforePost, h.snapshot().Actors) {
					t.Fatal("old question post changed replacement generation")
				}
			} else {
				a := qaClaudeActor(t, h, r.Actor)
				if a.State == "working" || a.Health == "continuous" {
					t.Fatalf("late question post healed missing continuity %+v", a)
				}
			}
			qaWaitReview(t, h.qaHostHarness, r.Actor)
			expected := [][2]int64{{0, 10}}
			if loss == "next_prompt" {
				expected = append(expected, [2]int64{20, 25})
			}
			qaIntervals(t, h.snapshot(), expected)
			before, _ := os.ReadFile(h.path)
			h.snapshot()
			after, _ := os.ReadFile(h.path)
			if string(before) != string(after) {
				t.Fatal("read-only capture review changed state")
			}
			h.send(40, h.event("UserPromptSubmit", "future", ""))
			h.send(50, h.event("Stop", "future", ""))
			expected = append(expected, [2]int64{40, 50})
			qaIntervals(t, h.snapshot(), expected)
			qaWaitReview(t, h.qaHostHarness, r.Actor)
		})
	}
}
func TestQAClaudeQuestionLatePreAndConflictingTerminalCannotResume(t *testing.T) {
	for _, mode := range []string{"post_before_pre", "success_then_failure", "failure_then_success"} {
		t.Run(mode, func(t *testing.T) {
			h := qaNewClaude(t)
			h.startSession()
			r := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
			if mode == "post_before_pre" {
				h.send(10, qaClaudeQuestion(h, "PostToolUseFailure", "q"))
				h.send(20, qaClaudeQuestion(h, "PreToolUse", "q"))
			} else {
				h.send(10, qaClaudeQuestion(h, "PreToolUse", "q"))
				first, second := "PostToolUse", "PostToolUseFailure"
				if mode == "failure_then_success" {
					first, second = second, first
				}
				h.send(20, qaClaudeQuestion(h, first, "q"))
				h.at(30)
				_, _ = h.service.IngestHost(context.Background(), qaClaudeQuestion(h, second, "q"))
			}
			a := qaClaudeActor(t, h, r.Actor)
			if a.Health == "continuous" {
				t.Fatalf("contradictory phase kept trustworthy continuity %+v", a)
			}
		})
	}
}

func TestQAClaudeQuestionValidClockDiscontinuityCannotResume(t *testing.T) {
	for _, terminal := range []string{"PostToolUse", "PostToolUseFailure"} {
		for _, mode := range []string{"new_epoch", "suspend"} {
			t.Run(terminal+"/"+mode, func(t *testing.T) {
				h := qaNewClaude(t)
				h.startSession()
				root := h.send(0, h.event("UserPromptSubmit", "prompt", ""))
				h.send(10, qaClaudeQuestion(h, "PreToolUse", "question"))
				qaHostState(t, h.qaHostHarness, root.Actor, "wait_user", "continuous")
				// A valid sample contains positive discontinuity evidence. There is no
				// Clock.Sample error, and later samples remain in this new clock history.
				sampleAt := func(seconds int64) {
					h.at(seconds)
					if mode == "new_epoch" {
						epoch := "boot-2"
						n := strconv.FormatInt((seconds-30)*int64(time.Second), 10)
						h.sample.Epoch = &epoch
						h.sample.ElapsedNS = &n
						h.sample.AwakeNS = &n
					} else {
						awake := strconv.FormatInt((seconds-20)*int64(time.Second), 10)
						h.sample.AwakeNS = &awake
					}
				}
				sampleAt(30)
				r, err := h.service.IngestHost(context.Background(), qaClaudeQuestion(h, terminal, "question"))
				if r.Durability != "committed" {
					t.Fatalf("positive discontinuity was not retained: %+v %v", r, err)
				}
				a := qaClaudeActor(t, h, root.Actor)
				if a.State == "working" || a.Health == "continuous" {
					t.Fatalf("matched %s erased pending-wait clock evidence and resumed: %+v", terminal, a)
				}
				qaWaitReview(t, h.qaHostHarness, root.Actor)
				qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}})
				h.restartHost()
				sampleAt(35)
				if _, err = h.service.IngestHost(context.Background(), h.event("Stop", "prompt", "")); err != nil {
					t.Fatal(err)
				}
				qaWaitReview(t, h.qaHostHarness, root.Actor)
				qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}})
				sampleAt(40)
				fresh, err := h.service.IngestHost(context.Background(), h.event("UserPromptSubmit", "future", ""))
				if err != nil || fresh.Actor == nil {
					t.Fatalf("new generation after discontinuity unavailable: %+v %v", fresh, err)
				}
				qaHostState(t, h.qaHostHarness, fresh.Actor, "working", "continuous")
				sampleAt(50)
				if _, err = h.service.IngestHost(context.Background(), h.event("Stop", "future", "")); err != nil {
					t.Fatal(err)
				}
				qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}, {40, 50}})
				qaWaitReview(t, h.qaHostHarness, root.Actor)
			})
		}
	}
}
