package activity

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestQARecoveryWaitingInterruptDetachesOnlyParentWithoutClock(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("parent", "1", "1", "work", qaBindingA))
	h.ingest(10, qaEvent("child", "1", "1", "work", qaBindingA))
	h.ingest(20, qaEvent("parent", "1", "2", "wait_children", ""))
	var parent Actor
	for _, a := range h.snapshot().Actors {
		if a.Ref.Key.AgentID == "parent" {
			parent = a
		}
	}
	h.clockErr = errors.New("unavailable")
	_, err := h.service.Interrupt(context.Background(), InterruptInput{ActorID: parent.ID, Generation: "1", IfRevision: parent.Revision, RequestID: qaRecoveryRequest, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	h.clockErr = nil
	s := h.snapshot()
	for _, a := range s.Actors {
		if a.Ref.Key.AgentID == "parent" && !terminal(&a) {
			t.Fatalf("parent remained attached %+v", a)
		}
		if a.Ref.Key.AgentID == "child" && a.State != "working" {
			t.Fatalf("parent interruption cascaded to child %+v", a)
		}
	}
	if len(s.Uncertainties) != 0 {
		t.Fatal("waiting interrupt invented work tail")
	}
	h.ingest(30, qaEvent("child", "1", "2", "finish", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 30}})
}
func TestQARecoveryUnavailableInterruptPersistsSafetyBeforeError(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
	actor := h.snapshot().Actors[0]
	h.at(20)
	h.clockErr = errors.New("unavailable")
	in := InterruptInput{ActorID: actor.ID, Generation: "1", IfRevision: actor.Revision, RequestID: qaRecoveryRequest, Confirmed: true}
	_, err := h.service.Interrupt(context.Background(), in)
	qaCode(t, err, "clock_unavailable")
	h.at(30)
	h.restart()
	s := h.snapshot()
	if len(s.Uncertainties) != 1 || s.Uncertainties[0].LowerBound != *qaRecoveryTime(10) || terminal(&s.Actors[0]) {
		t.Fatalf("failed interrupt lost safety effect or detached: %+v", s)
	}
	before := qaRecoveryRead(t, h)
	h.service.clock = ClockFunc(func() (ClockSample, error) { t.Fatal("error replay sampled clock"); return ClockSample{}, nil })
	_, err = h.service.Interrupt(context.Background(), in)
	qaCode(t, err, "clock_unavailable")
	if string(before) != string(qaRecoveryRead(t, h)) {
		t.Fatal("error replay changed state")
	}
	h.restart()
	h.ingest(40, qaEvent("A", "1", "3", "finish", ""))
	if len(h.snapshot().ClosedIntervals) != 0 {
		t.Fatal("later stop billed unknown tail")
	}
}
func TestQARecoverySourceLossNeverAdvancesConfirmedTime(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(300, qaEvent("A", "1", "2", "observe_work", ""))
	actor := h.snapshot().Actors[0]
	h.at(86400)
	input := SourceObservation{Actor: actor.Ref, Reason: "source_lost", RequestID: qaRecoveryRequest}
	first, err := h.service.ObserveSource(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	h.restart()
	s := h.snapshot()
	if len(s.Uncertainties) != 1 || !s.Uncertainties[0].LowerBound.Equal(*qaRecoveryTime(300)) || s.Uncertainties[0].UpperBound != nil || s.Uncertainties[0].Reason != "source_lost" || len(s.ClosedIntervals) != 0 {
		t.Fatalf("source loss guessed overnight work %+v", s)
	}
	h.ingest(86410, qaEvent("A", "1", "3", "work", ""))
	before := qaRecoveryRead(t, h)
	h.service.clock = ClockFunc(func() (ClockSample, error) {
		t.Fatal("source observation replay sampled resumed work")
		return ClockSample{}, nil
	})
	again, err := h.service.ObserveSource(context.Background(), input)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatalf("source observation replay changed %+v %v", again, err)
	}
	if string(before) != string(qaRecoveryRead(t, h)) {
		t.Fatal("old source loss touched resumed segment")
	}
}

func TestQARecoveryClockObservationQuarantinesAllActorsAndReplaysBeforeSampling(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(5, qaEvent("B", "1", "1", "work", qaBindingB))
	h.at(20)
	awake := "10000000000"
	h.sample.AwakeNS = &awake
	in := ClockObservation{RequestID: qaRecoveryRequest}
	first, err := h.service.ObserveClock(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Changed {
		t.Fatal("positive suspend observation not applied")
	}
	h.at(30)
	h.restart()
	s := h.snapshot()
	if len(s.Uncertainties) != 2 {
		t.Fatalf("global clock loss omitted peer: %+v", s.Uncertainties)
	}
	h.ingest(30, qaEvent("A", "1", "2", "wait_user", ""))
	h.ingest(40, qaEvent("A", "1", "3", "work", ""))
	before := qaRecoveryRead(t, h)
	h.service.clock = ClockFunc(func() (ClockSample, error) {
		t.Fatal("clock observation replay sampled later work")
		return ClockSample{}, nil
	})
	again, err := h.service.ObserveClock(context.Background(), in)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatalf("observation replay changed %+v %v", again, err)
	}
	if string(before) != string(qaRecoveryRead(t, h)) {
		t.Fatal("old clock observation changed resumed work")
	}
}
func TestQARecoveryContinuousClockObservationDoesNotConfirmWork(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.at(1800)
	input := ClockObservation{RequestID: qaRecoveryRequest}
	first, err := h.service.ObserveClock(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if first.Changed || len(first.AffectedIDs) != 0 {
		t.Fatalf("continuous clock invented evidence: %+v", first)
	}
	st, _, err := h.service.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range st.Segments {
		if !seg.Confirmed.Equal(*qaRecoveryTime(0)) {
			t.Fatal("clock observation acted as work heartbeat")
		}
	}
	h.at(1900)
	h.clockErr = errors.New("later failure")
	h.restart()
	h.service.clock = ClockFunc(func() (ClockSample, error) { t.Fatal("no-op replay sampled clock"); return ClockSample{}, nil })
	again, err := h.service.ObserveClock(context.Background(), input)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatalf("no-op replay changed %+v %v", again, err)
	}
}
func TestQARecoveryOlderSourceObservationCannotAffectNewGeneration(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	old := h.snapshot().Actors[0].Ref
	h.ingest(10, qaEvent("A", "1", "2", "finish", ""))
	h.ingest(20, qaEvent("A", "2", "1", "work", qaBindingA))
	h.at(30)
	r, err := h.service.ObserveSource(context.Background(), SourceObservation{Actor: old, Reason: "source_lost", RequestID: qaRecoveryRequest})
	if err != nil {
		t.Fatal(err)
	}
	if r.Changed {
		t.Fatal("late source loss changed new generation")
	}
	s := h.snapshot()
	if len(s.Uncertainties) != 0 || s.Actors[0].Ref.Generation != "2" || s.Actors[0].State != "working" {
		t.Fatalf("new generation quarantined by old observation %+v", s)
	}
}

func TestQARecoveryPreviewClockFailureIsReadOnlyButResolveQuarantinesPeers(t *testing.T) {
	h, u := qaRecoveryUncertain(t)
	h.ingest(2000, qaEvent("B", "1", "1", "work", qaBindingB))
	h.at(2200)
	h.clockErr = errors.New("unavailable")
	before := qaRecoveryRead(t, h)
	_, err := h.service.Preview(context.Background(), RecoveryInput{UncertaintyID: u.ID, End: qaRecoveryTime(1200)})
	qaCode(t, err, "clock_unavailable")
	if string(before) != string(qaRecoveryRead(t, h)) {
		t.Fatal("failed preview persisted safety projection")
	}
	input := ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(1200), IfRevision: u.Revision, RequestID: qaRecoveryRequest, Confirmed: true}
	_, err = h.service.Resolve(context.Background(), input)
	qaCode(t, err, "clock_unavailable")
	h.at(2300)
	h.restart()
	s := h.snapshot()
	if len(s.Uncertainties) != 2 {
		t.Fatalf("failed resolution lost global clock quarantine %+v", s.Uncertainties)
	}
	h.ingest(2400, qaEvent("B", "1", "2", "finish", ""))
	if len(h.snapshot().ClosedIntervals) != 0 {
		t.Fatal("later peer stop billed globally unknown tail")
	}
}
func TestQARecoveryWorkingInterruptKeepsConfirmedPrefixAndUnknownTail(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
	a := h.snapshot().Actors[0]
	h.at(20)
	_, err := h.service.Interrupt(context.Background(), InterruptInput{ActorID: a.ID, Generation: "1", IfRevision: a.Revision, RequestID: qaRecoveryRequest, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	h.restart()
	s := h.snapshot()
	if !terminal(&s.Actors[0]) || len(s.Uncertainties) != 1 || !s.Uncertainties[0].LowerBound.Equal(*qaRecoveryTime(10)) || s.Uncertainties[0].UpperBound == nil || !s.Uncertainties[0].UpperBound.Equal(*qaRecoveryTime(20)) || len(s.ClosedIntervals) != 0 {
		t.Fatalf("interrupt invented confirmed stop %+v", s)
	}
	before := qaRecoveryRead(t, h)
	r, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "3", "wait_user", ""))
	if err != nil || r.Disposition != "stale" {
		t.Fatalf("late old wait revived actor %+v %v", r, err)
	}
	if string(before) != string(qaRecoveryRead(t, h)) {
		t.Fatal("late old wait changed tombstone")
	}
}
func TestQARecoveryOrderingUnavailableStaysBlocked(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	actor := h.snapshot().Actors[0]
	h.at(10)
	_, err := h.service.ObserveSource(context.Background(), SourceObservation{Actor: actor.Ref, Reason: "ordering_unavailable", RequestID: qaRecoveryRequest})
	if err != nil {
		t.Fatal(err)
	}
	h.at(20)
	_, err = h.service.Ingest(context.Background(), qaEvent("A", "1", "2", "finish", ""))
	qaCode(t, err, "event_gap")
	s := h.snapshot()
	if len(s.Uncertainties) != 1 || s.Actors[0].Sequence != "1" {
		t.Fatalf("lost ordering uncertainty %+v", s)
	}
}

func TestQARecoveryCorruptErrorReceiptsNeverReplayUnsafeOutcome(t *testing.T) {
	for _, name := range []string{"unsafe-message", "arbitrary-details", "wrong-operation", "mixed-outcomes"} {
		t.Run(name, func(t *testing.T) {
			h := qaNew(t)
			h.seed()
			h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
			actor := h.snapshot().Actors[0]
			h.at(10)
			h.clockErr = errors.New("unavailable")
			_, err := h.service.Interrupt(context.Background(), InterruptInput{ActorID: actor.ID, Generation: "1", IfRevision: actor.Revision, RequestID: qaRecoveryRequest, Confirmed: true})
			qaCode(t, err, "clock_unavailable")
			var data map[string]any
			if err := json.Unmarshal(qaRecoveryRead(t, h), &data); err != nil {
				t.Fatal(err)
			}
			receipt := data["requests"].(map[string]any)[qaRecoveryRequest].(map[string]any)
			switch name {
			case "unsafe-message":
				receipt["error"].(map[string]any)["message"] = "UNTRUSTED-PAYLOAD"
			case "arbitrary-details":
				receipt["error"].(map[string]any)["details"] = map[string]any{"raw": "UNTRUSTED-PAYLOAD"}
			case "wrong-operation":
				receipt["operation"] = "bindings.link"
			case "mixed-outcomes":
				receipt["mutation_result"] = map[string]any{"contract_version": 1, "snapshot_revision": "1", "request_id": qaRecoveryRequest, "changed": false, "affected_ids": []any{}, "entity_revision": nil}
			}
			corrupt, err := json.Marshal(data)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(h.path, corrupt, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = h.service.Review(context.Background(), ReviewInput{})
			qaCode(t, err, "state_corrupt")
			if string(corrupt) != string(qaRecoveryRead(t, h)) {
				t.Fatal("corrupt error evidence overwritten")
			}
		})
	}
}
