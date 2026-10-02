package activity

import (
	"context"
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
