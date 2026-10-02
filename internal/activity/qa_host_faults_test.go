package activity

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rbeene/tempo/internal/hookstate"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestQAHostUnknownCommitReplayPreservesOneActorWithoutClock(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.at(0)
	h.service.store.fail = func(stage string) error {
		if stage == "directory_sync" {
			return errors.New("synthetic sync failure")
		}
		return nil
	}
	e := h.event("UserPromptSubmit", "turn", "")
	r, err := h.service.IngestHost(context.Background(), e)
	qaCode(t, err, "local_write_unknown")
	if r.Durability != "unknown" {
		t.Fatalf("commit uncertainty misreported: %+v", r)
	}
	h.service.store.fail = nil
	h.clockErr = errors.New("retry must not require a new clock sample")
	r, err = h.service.IngestHost(context.Background(), e)
	if err != nil {
		t.Fatal(err)
	}
	if r.Disposition != "duplicate" || r.Durability != "committed" || r.Actor == nil {
		t.Fatalf("unknown replay did not reconcile: %+v", r)
	}
	h.clockErr = nil
	h.send(60, h.event("Stop", "turn", ""))
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 60}})
	if len(s.Actors) != 1 {
		t.Fatalf("unknown retry allocated duplicate actor: %+v", s.Actors)
	}
}

func TestQAHostDefiniteFailureDoesNotInventDurableLoss(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "turn", ""))
	before, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	h.service.store.fail = func(stage string) error {
		if stage == "before_write" {
			return errors.New("synthetic precommit failure")
		}
		return nil
	}
	h.at(20)
	e := h.event("PreToolUse", "turn", "")
	e.ToolID = "tool"
	e.ToolName = "Bash"
	r, err := h.service.IngestHost(context.Background(), e)
	if err == nil {
		t.Fatal("definite write failure acknowledged")
	}
	if r.Durability != "not_committed" {
		t.Fatalf("failed commit claimed durable evidence: %+v", r)
	}
	after, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("definite failure changed durable state")
	}
	h.restartHost()
	h.send(60, h.event("Stop", "turn", ""))
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 60}})
	if len(s.Uncertainties) != 0 {
		t.Fatalf("fresh process fabricated detection of unpersisted failure: %+v", s.Uncertainties)
	}
}

func TestQAHostDurableSourceLossStopCapsWithoutResolving(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "turn", ""))
	h.at(10)
	obs := HostObservation{Source: "codex", SessionID: "host-session", TurnID: "turn", Reason: "source_lost", RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}
	if _, err := h.service.ObserveHost(context.Background(), obs); err != nil {
		t.Fatal(err)
	}
	h.restartHost()
	h.send(60, h.event("Stop", "turn", ""))
	s := h.snapshot()
	qaIntervals(t, s, nil)
	if len(s.Uncertainties) != 1 {
		t.Fatalf("source failure not retained: %+v", s)
	}
	u := s.Uncertainties[0]
	if !u.LowerBound.Equal(qaEpochStart) || u.UpperBound == nil || !u.UpperBound.Equal(qaEpochStart.Add(60*time.Second)) || u.ResolutionEnd != nil || u.State != "unresolved" {
		t.Fatalf("stop resolved or changed source-loss tail: %+v", u)
	}
	if _, err := h.service.ObserveHost(context.Background(), obs); err != nil {
		t.Fatalf("source observation replay failed: %v", err)
	}
	if len(h.snapshot().Uncertainties) != 1 {
		t.Fatal("observation replay duplicated uncertainty")
	}
}

func TestQAHostConcurrentDuplicateIngressAllocatesOnce(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.at(0)
	e := h.event("UserPromptSubmit", "turn", "")
	type outcome struct {
		receipt HostReceipt
		err     error
	}
	done := make(chan outcome, 8)
	start := make(chan struct{})
	for range 8 {
		go func() {
			<-start
			s := New(Options{Path: h.path, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, nil }), HookPolicies: h.policies, LockTimeout: time.Second})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			r, err := s.IngestHost(ctx, e)
			done <- outcome{r, err}
		}()
	}
	close(start)
	var ref *ActorRef
	applied := 0
	for range 8 {
		o := <-done
		if o.err != nil {
			t.Fatal(o.err)
		}
		if o.receipt.Actor == nil {
			t.Fatalf("duplicate ingress lost identity: %+v", o.receipt)
		}
		if ref == nil {
			ref = o.receipt.Actor
		} else if *ref != *o.receipt.Actor {
			t.Fatalf("concurrent duplicate allocated another actor: %+v vs %+v", ref, o.receipt.Actor)
		}
		if o.receipt.Disposition == "applied" {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("applied %d copies of one callback", applied)
	}
	h.send(60, h.event("Stop", "turn", ""))
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 60}})
	if len(s.Actors) != 1 {
		t.Fatalf("concurrent duplicate actors: %+v", s.Actors)
	}
}

func TestQAHostClockFailureQuarantinesAllOpenActors(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "turn", ""))
	h.send(5, h.event("SubagentStart", "turn", "child"))
	h.at(10)
	h.clockErr = errors.New("synthetic unavailable clock")
	_, err := h.service.IngestHost(context.Background(), h.event("Stop", "turn", ""))
	qaCode(t, err, "clock_unavailable")
	h.at(20)
	h.restartHost()
	h.send(20, h.event("SubagentStop", "turn", "child"))
	s := h.snapshot()
	qaIntervals(t, s, nil)
	if len(s.Uncertainties) != 2 {
		t.Fatalf("host error branch missed global clock quarantine: %+v", s.Uncertainties)
	}
	for _, u := range s.Uncertainties {
		if u.State != "unresolved" || u.ResolutionEnd != nil {
			t.Fatalf("later child callback resolved unavailable tail: %+v", u)
		}
	}
}

func TestQAHostLivePolicyLossCannotFinalizeUnprovenTail(t *testing.T) {
	for _, mode := range []string{"revoke", "drift"} {
		t.Run(mode, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			h.send(0, h.event("UserPromptSubmit", "turn", ""))
			if mode == "revoke" {
				p, err := h.policies.Eligibility(context.Background(), "codex", h.cwd)
				if err != nil {
					t.Fatal(err)
				}
				_, err = h.policies.Revoke(context.Background(), hookstate.RevokeInput{Host: "codex", Scope: "project", Path: p.Context.Path, IfRevision: p.Revision, RequestID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", Confirmed: true})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(filepath.Join(h.cwd, "definitions"), []byte("changed hook definitions"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			h.send(20, h.event("Stop", "turn", ""))
			s := h.snapshot()
			qaIntervals(t, s, nil)
			if len(s.Uncertainties) != 1 || s.Uncertainties[0].State != "unresolved" || !s.Uncertainties[0].LowerBound.Equal(qaEpochStart) {
				t.Fatalf("known policy loss left live tail without durable uncertainty: %+v", s)
			}
		})
	}
}

func TestQAHostCorruptRootPointerRejectsReadsAndCallbacks(t *testing.T) {
	for _, variant := range []string{"child", "foreign_session"} {
		t.Run(variant, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			h.send(0, h.event("UserPromptSubmit", "root-turn", ""))
			h.send(5, h.event("SubagentStart", "child-turn", "child"))
			if variant == "foreign_session" {
				e := h.event("SessionStart", "", "")
				e.SessionID = "foreign-session"
				e.SessionSource = "startup"
				h.send(6, e)
				e.Kind = "UserPromptSubmit"
				e.SessionSource = ""
				e.TurnID = "foreign-root"
				h.send(7, e)
			}
			raw, err := os.ReadFile(h.path)
			if err != nil {
				t.Fatal(err)
			}
			var st state
			if err := json.Unmarshal(raw, &st); err != nil {
				t.Fatal(err)
			}
			target := ""
			for key, turn := range st.HostTurns {
				if variant == "child" && turn.AgentID == "child" || variant == "foreign_session" && turn.SessionID == "foreign-session" && turn.AgentID == "" {
					target = key
				}
			}
			if target == "" {
				t.Fatal("fixture target missing")
			}
			st.HostSessions[hostSessionKey(HostEvent{Source: "codex", SessionID: "host-session"})].RootTurn = target
			corrupt, err := json.Marshal(st)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(h.path, corrupt, 0600); err != nil {
				t.Fatal(err)
			}
			h.restartHost()
			_, err = h.service.Status(context.Background())
			qaCode(t, err, "state_corrupt")
			_, err = h.service.HostReceipts(context.Background(), HostReceiptFilter{})
			qaCode(t, err, "state_corrupt")
			_, err = h.service.IngestHost(context.Background(), h.event("SessionEnd", "", ""))
			qaCode(t, err, "state_corrupt")
			preserved, err := os.ReadFile(h.path)
			if err != nil {
				t.Fatal(err)
			}
			if string(corrupt) != string(preserved) {
				t.Fatal("invalid root pointer was overwritten or applied")
			}
		})
	}
}

func TestQAHostUnavailableClockAtResumeCommitsQuarantineWithoutCap(t *testing.T) {
	for _, source := range []string{"resume", "clear"} {
		t.Run(source, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			root := h.send(0, h.event("UserPromptSubmit", "root-turn", ""))
			child := h.send(5, h.event("SubagentStart", "child-turn", "child"))
			h.at(10)
			h.clockErr = errors.New("synthetic unavailable clock at boundary")
			boundary := h.event("SessionStart", "", "")
			boundary.SessionSource = source
			r, err := h.service.IngestHost(context.Background(), boundary)
			qaCode(t, err, "clock_unavailable")
			if r.Durability != "committed" {
				t.Fatalf("observed global loss not committed: %+v", r)
			}
			h.at(15)
			s := h.snapshot()
			qaIntervals(t, s, nil)
			if len(s.Uncertainties) != 2 {
				t.Fatalf("boundary lost global clock quarantine: %+v", s.Uncertainties)
			}
			for _, u := range s.Uncertainties {
				if u.UpperBound != nil {
					t.Fatalf("unavailable boundary invented trusted end: %+v", u)
				}
			}
			h.send(20, h.event("Stop", "root-turn", ""))
			h.send(25, h.event("SubagentStop", "child-turn", "child"))
			s = h.snapshot()
			qaIntervals(t, s, nil)
			if root.Actor == nil || child.Actor == nil || len(s.Uncertainties) != 2 {
				t.Fatalf("later good callbacks lost original uncertainties: %+v", s)
			}
			for _, u := range s.Uncertainties {
				if u.Actor != *root.Actor && u.Actor != *child.Actor || u.State != "unresolved" {
					t.Fatalf("boundary remapped or resolved old uncertainty: %+v", u)
				}
			}
		})
	}
}

func TestQAHostFailedPromptCannotAliasLaterGeneration(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "turn-A", ""))
	h.at(10)
	h.clockErr = errors.New("synthetic unavailable clock during prompt admission")
	_, err := h.service.IngestHost(context.Background(), h.event("UserPromptSubmit", "turn-B", ""))
	qaCode(t, err, "clock_unavailable")
	admitted := h.send(20, h.event("UserPromptSubmit", "turn-C", ""))
	if admitted.Actor == nil {
		t.Fatal("later valid prompt missing actor")
	}
	h.send(30, h.event("Stop", "turn-B", ""))
	s := h.snapshot()
	found := false
	for _, a := range s.Actors {
		if a.Ref == *admitted.Actor {
			found = true
			if a.State != "working" {
				t.Fatalf("stop of failed B admission closed C generation: %+v", a)
			}
		}
	}
	if !found {
		t.Fatalf("C generation lost after failed B terminal: %+v", s.Actors)
	}
	for _, interval := range s.ClosedIntervals {
		if interval.Start.Equal(qaEpochStart.Add(20 * time.Second)) {
			t.Fatalf("failed B alias automatically billed C: %+v", interval)
		}
	}
}

func TestQAHostHistoricalTerminalReplayReconcilesOriginalReceipt(t *testing.T) {
	for _, kind := range []string{"Stop", "SubagentStop"} {
		t.Run(kind, func(t *testing.T) {
			h := qaNewHost(t)
			h.startSession()
			h.send(0, h.event("UserPromptSubmit", "old-root", ""))
			h.send(5, h.event("SubagentStart", "old-child", "child"))
			terminal := h.event(kind, "old-root", "")
			if kind == "SubagentStop" {
				terminal.TurnID = "old-child"
				terminal.AgentID = "child"
			}
			original := h.send(10, terminal)
			boundary := h.event("SessionStart", "", "")
			boundary.SessionSource = "resume"
			h.send(20, boundary)
			h.send(30, h.event("UserPromptSubmit", "new-root", ""))
			p, err := h.policies.Eligibility(context.Background(), "codex", h.cwd)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.policies.Revoke(context.Background(), hookstate.RevokeInput{Host: "codex", Scope: "project", Path: p.Context.Path, IfRevision: p.Revision, RequestID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", Confirmed: true}); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(h.path)
			if err != nil {
				t.Fatal(err)
			}
			h.at(40)
			h.clockErr = errors.New("historical replay must not sample clock")
			replay, err := h.service.IngestHost(context.Background(), terminal)
			if err != nil {
				t.Fatalf("historical replay consulted current clock/policy: %#v", err)
			}
			if replay.Disposition != "duplicate" || replay.ID != original.ID || replay.Actor == nil || original.Actor == nil || *replay.Actor != *original.Actor {
				t.Fatalf("historical replay allocated new identity: original=%+v replay=%+v", original, replay)
			}
			after, err := os.ReadFile(h.path)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatal("historical terminal replay created a receipt or changed actor state")
			}
		})
	}
}
