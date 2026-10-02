package activity

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"
)

func TestQARecoveryUnknownDurabilityReplaysWithoutReapplying(t *testing.T) {
	h, u := qaRecoveryUncertain(t)
	in := ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(1200), IfRevision: u.Revision, RequestID: qaRecoveryRequest, Confirmed: true}
	h.service.store.fail = func(stage string) error {
		if stage == "directory_sync" {
			return errors.New("synthetic sync failure")
		}
		return nil
	}
	_, err := h.service.Resolve(context.Background(), in)
	qaCode(t, err, "local_write_unknown")
	h.restart()
	h.service.clock = ClockFunc(func() (ClockSample, error) { t.Fatal("committed replay sampled clock"); return ClockSample{}, nil })
	first, err := h.service.Resolve(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	again, err := h.service.Resolve(context.Background(), in)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatalf("uncertain replay changed effect %+v %v", again, err)
	}
	h.restart()
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 1200}})
	st, _, err := h.service.store.read(context.Background())
	if err != nil || len(st.Outbox) != 1 {
		t.Fatalf("replayed duplicate outbox %v %+v", err, st)
	}
}
func TestQARecoveryDefiniteWriteFailurePreservesState(t *testing.T) {
	h, u := qaRecoveryUncertain(t)
	before := qaRecoveryRead(t, h)
	h.service.store.fail = func(stage string) error {
		if stage == "before_write" {
			return errors.New("synthetic failure")
		}
		return nil
	}
	_, err := h.service.Resolve(context.Background(), ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(1200), IfRevision: u.Revision, RequestID: qaRecoveryRequest, Confirmed: true})
	if err == nil {
		t.Fatal("write failure acknowledged")
	}
	if string(before) != string(qaRecoveryRead(t, h)) {
		t.Fatal("definite failed recovery partially changed state")
	}
}
func TestQARecoveryProcessHelper(t *testing.T) {
	if os.Getenv("TEMPO_QA_RECOVERY_HELPER") != "1" {
		return
	}
	h := qaNew(t)
	h.path = os.Getenv("TEMPO_QA_RECOVERY_PATH")
	h.at(2400)
	h.restart()
	_, err := h.service.Resolve(context.Background(), ResolveInput{UncertaintyID: os.Getenv("TEMPO_QA_RECOVERY_UNCERTAINTY"), End: qaRecoveryTime(1200), IfRevision: os.Getenv("TEMPO_QA_RECOVERY_REVISION"), RequestID: qaRecoveryRequest, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
}
func TestQARecoveryConcurrentSameRequestHasOneDecisionAndOutbox(t *testing.T) {
	h, u := qaRecoveryUncertain(t)
	type outcome struct {
		out []byte
		err error
	}
	done := make(chan outcome, 3)
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQARecoveryProcessHelper$")
		cmd.Env = append(os.Environ(), "TEMPO_QA_RECOVERY_HELPER=1", "TEMPO_QA_RECOVERY_PATH="+h.path, "TEMPO_QA_RECOVERY_UNCERTAINTY="+u.ID, "TEMPO_QA_RECOVERY_REVISION="+u.Revision)
		go func() { out, err := cmd.CombinedOutput(); done <- outcome{out, err} }()
	}
	var failed bool
	for i := 0; i < 3; i++ {
		r := <-done
		if r.err != nil {
			failed = true
			t.Errorf("concurrent recovery %v %s", r.err, r.out)
		}
	}
	if failed {
		return
	}
	h.restart()
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 1200}})
	st, _, err := h.service.store.read(context.Background())
	if err != nil || len(st.Outbox) != 1 {
		t.Fatalf("duplicate output %+v %v", st, err)
	}
}

func TestQARecoveryCompetingResolutionsCannotBothApply(t *testing.T) {
	h, u := qaRecoveryUncertain(t)
	type outcome struct {
		result MutationResult
		err    error
	}
	done := make(chan outcome, 2)
	for i, id := range []string{qaRecoveryRequest, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"} {
		in := ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(int64(1200 + i*100)), IfRevision: u.Revision, RequestID: id, Confirmed: true}
		go func() { r, err := h.service.Resolve(context.Background(), in); done <- outcome{r, err} }()
	}
	winners := 0
	for i := 0; i < 2; i++ {
		r := <-done
		if r.err == nil {
			if !r.result.Changed {
				t.Fatal("new resolution reported no effect")
			}
			winners++
		} else {
			var ae *Error
			if !errors.As(r.err, &ae) || (ae.Code != "revision_conflict" && ae.Code != "invalid_transition") {
				t.Fatalf("wrong competing conflict: %v", r.err)
			}
		}
	}
	if winners != 1 {
		t.Fatalf("competing resolution winners=%d", winners)
	}
	st, _, err := h.service.store.read(context.Background())
	if err != nil || len(st.Outbox) != 1 || len(st.Intervals) != 1 {
		t.Fatalf("duplicate competing effects %+v %v", st, err)
	}
}

func TestQARecoveryUnknownSafetyErrorCommitReplaysOriginalError(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
	actor := h.snapshot().Actors[0]
	h.at(20)
	h.clockErr = errors.New("clock unavailable")
	in := InterruptInput{ActorID: actor.ID, Generation: "1", IfRevision: actor.Revision, RequestID: qaRecoveryRequest, Confirmed: true}
	h.service.store.fail = func(stage string) error {
		if stage == "directory_sync" {
			return errors.New("sync failure")
		}
		return nil
	}
	_, err := h.service.Interrupt(context.Background(), in)
	qaCode(t, err, "local_write_unknown")
	h.restart()
	h.service.clock = ClockFunc(func() (ClockSample, error) {
		t.Fatal("committed error replay sampled clock")
		return ClockSample{}, nil
	})
	_, err = h.service.Interrupt(context.Background(), in)
	qaCode(t, err, "clock_unavailable")
	h.at(30)
	h.restart()
	s := h.snapshot()
	if len(s.Uncertainties) != 1 || terminal(&s.Actors[0]) {
		t.Fatalf("unknown error commit lost quarantine or detached %+v", s)
	}
}
