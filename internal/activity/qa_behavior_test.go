package activity

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const qaComputer = "11111111-1111-4111-8111-111111111111"
const qaBindingA = "22222222-2222-4222-8222-222222222222"
const qaBindingB = "33333333-3333-4333-8333-333333333333"

var qaEpochStart = time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)

type qaHarness struct {
	t        *testing.T
	path     string
	sample   ClockSample
	clockErr error
	bindings map[string]BindingSnapshot
	service  *Service
}

func qaNew(t *testing.T) *qaHarness {
	t.Helper()
	h := &qaHarness{t: t, path: filepath.Join(t.TempDir(), "state", "activity.json"), bindings: map[string]BindingSnapshot{}}
	a := Attribution{AccountID: "1", UserID: "2", ProjectID: "3", TaskID: "4", Timezone: "UTC"}
	for _, id := range []string{qaBindingA, qaBindingB} {
		h.bindings[id] = BindingSnapshot{ID: id, Revision: "1", Attribution: a}
	}
	h.at(0)
	h.restart()
	return h
}

func (h *qaHarness) restart() {
	h.service = qaLegacyNew(Options{Path: h.path, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, h.clockErr }), ResolveBinding: func(_ context.Context, e Event) (BindingSnapshot, bool, error) {
		b, ok := h.bindings[e.BindingID]
		return b, ok, nil
	}})
}

func (h *qaHarness) at(seconds int64) {
	epoch, n := "boot-1", strconv.FormatInt(seconds*int64(time.Second), 10)
	h.sample = ClockSample{Capability: "available", WallUTC: qaEpochStart.Add(time.Duration(seconds) * time.Second), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}
	h.clockErr = nil
}

// Production identity initialization belongs to linking (#8). This is only a
// private synthetic fixture, persisted through the same file transaction seam.
func (h *qaHarness) seed() {
	h.t.Helper()
	err := h.service.store.update(context.Background(), func(s *state) (bool, error) {
		s.ComputerID = qaComputer
		for id, b := range h.bindings {
			s.Bindings[id] = b
		}
		return true, nil
	})
	if err != nil {
		h.t.Fatalf("synthetic initialization transaction: %v", err)
	}
	if _, err := os.Stat(h.path); err != nil {
		h.t.Fatalf("successful initialization did not persist state: %v", err)
	}
}

func qaEvent(actor, generation, sequence, kind, binding string) Event {
	e := Event{ContractVersion: 1, Actor: ActorKey{ComputerID: qaComputer, Source: "manual-test", SessionID: "qa-session", AgentID: actor}, Generation: generation, Sequence: sequence, EventID: fmt.Sprintf("%s/%s/%s", actor, generation, sequence), Kind: kind}
	if binding != "" {
		e.BindingID = binding
		e.BindingRevision = "1"
	}
	return e
}

func (h *qaHarness) ingest(at int64, e Event) EventResult {
	h.t.Helper()
	h.at(at)
	r, err := h.service.Ingest(context.Background(), e)
	if err != nil {
		h.t.Fatalf("%s %s/%s at %d: %v", e.Kind, e.Generation, e.Sequence, at, err)
	}
	return r
}

func (h *qaHarness) snapshot() ActivitySnapshot {
	h.t.Helper()
	s, err := h.service.Status(context.Background())
	if err != nil {
		h.t.Fatalf("status: %v", err)
	}
	return s
}

func qaCode(t *testing.T, err error, code string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("wanted %s, got %#v", code, err)
	}
}

func qaIntervals(t *testing.T, s ActivitySnapshot, want [][2]int64) {
	t.Helper()
	got := append([]Interval(nil), s.ClosedIntervals...)
	sort.Slice(got, func(i, j int) bool { return got[i].Start.Before(got[j].Start) })
	if len(got) != len(want) {
		t.Fatalf("intervals=%+v, want ranges=%v", got, want)
	}
	for i, r := range want {
		if !got[i].Start.Equal(qaEpochStart.Add(time.Duration(r[0])*time.Second)) || !got[i].End.Equal(qaEpochStart.Add(time.Duration(r[1])*time.Second)) || got[i].DurationNS != strconv.FormatInt((r[1]-r[0])*int64(time.Second), 10) {
			t.Fatalf("interval[%d]=%+v, want %v", i, got[i], r)
		}
		if got[i].ID == "" || len(got[i].SegmentIDs) == 0 {
			t.Fatalf("interval lost stable identity/evidence: %+v", got[i])
		}
	}
}

func TestQAActivityAbsentStatusAndUnlinkedIngressNeverCreateStore(t *testing.T) {
	h := qaNew(t)
	h.bindings = map[string]BindingSnapshot{}
	s := h.snapshot()
	if s.ContractVersion != 1 || s.ComputerID != nil || s.SnapshotRevision != "0" || s.Projects == nil || s.Actors == nil || s.Uncertainties == nil || s.ClosedIntervals == nil {
		t.Fatalf("invalid empty snapshot: %+v", s)
	}
	if len(s.Projects)+len(s.Actors)+len(s.Uncertainties)+len(s.ClosedIntervals) != 0 {
		t.Fatalf("nonempty absent snapshot: %+v", s)
	}
	r := h.ingest(0, qaEvent("A", "1", "1", "work", ""))
	if r.Disposition != "untracked" {
		t.Fatalf("unlinked disposition=%q", r.Disposition)
	}
	if _, err := os.Stat(filepath.Dir(h.path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read/untracked ingress created state directory: %v", err)
	}
}

func TestQAActivityCompatibleBindingsShareUnionAndSurviveReload(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	bStart := qaEvent("B", "1", "1", "work", qaBindingB)
	bStart.Actor.SessionID = "qa-independent-session"
	h.ingest(5, bStart)
	h.ingest(10, qaEvent("A", "1", "2", "finish", ""))
	s := h.snapshot()
	qaIntervals(t, s, nil)
	if len(s.Projects) != 1 || len(s.Projects[0].ActiveActorRefs) != 1 || s.Projects[0].ActiveActorRefs[0].Key.AgentID != "B" {
		t.Fatalf("A finish stopped B or split compatible bindings: %+v", s.Projects)
	}
	bStop := qaEvent("B", "1", "2", "finish", "")
	bStop.Actor.SessionID = bStart.Actor.SessionID
	h.ingest(15, bStop)
	s = h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 15}})
	if len(s.Uncertainties) != 0 {
		t.Fatalf("continuous union became uncertain: %+v", s.Uncertainties)
	}
	h.restart()
	reloaded := h.snapshot()
	if !reflect.DeepEqual(s.ClosedIntervals, reloaded.ClosedIntervals) {
		t.Fatalf("reload changed immutable interval: before=%+v after=%+v", s.ClosedIntervals, reloaded.ClosedIntervals)
	}
	err := h.service.store.update(context.Background(), func(st *state) (bool, error) {
		if len(st.Outbox) != 1 {
			t.Fatalf("one union needs one durable outbox, got %d", len(st.Outbox))
		}
		for _, item := range st.Outbox {
			if item.State != "queued" || item.Interval.ID != s.ClosedIntervals[0].ID {
				t.Fatalf("outbox lost interval attribution: %+v", item)
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestQAActivityWaitGapReplayAndTerminalTombstone(t *testing.T) {
	h := qaNew(t)
	h.seed()
	events := []Event{qaEvent("A", "1", "1", "work", qaBindingA), qaEvent("A", "1", "2", "wait_user", ""), qaEvent("A", "1", "3", "work", ""), qaEvent("A", "1", "4", "finish", "")}
	results := make([]EventResult, len(events))
	for i, e := range events {
		results[i] = h.ingest(int64(i*20), e)
	}
	before := h.snapshot()
	qaIntervals(t, before, [][2]int64{{0, 20}, {40, 60}})
	h.restart()
	for i, e := range events {
		r := h.ingest(80+int64(i), e)
		if r.Disposition != "duplicate" || !reflect.DeepEqual(r.SegmentID, results[i].SegmentID) || !reflect.DeepEqual(r.UncertaintyIDs, results[i].UncertaintyIDs) {
			t.Fatalf("replay changed original effect: %+v vs %+v", r, results[i])
		}
	}
	r := h.ingest(90, qaEvent("A", "1", "5", "work", ""))
	if r.Disposition != "stale" {
		t.Fatalf("terminal generation reopened: %+v", r)
	}
	after := h.snapshot()
	if !reflect.DeepEqual(before.ClosedIntervals, after.ClosedIntervals) {
		t.Fatal("replay changed finalized history")
	}
	h.ingest(100, qaEvent("A", "2", "1", "work", qaBindingA))
	r = h.ingest(101, qaEvent("A", "1", "6", "wait_user", ""))
	if r.Disposition != "stale" {
		t.Fatalf("old wait not stale: %+v", r)
	}
	h.ingest(120, qaEvent("A", "2", "2", "finish", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 20}, {40, 60}, {100, 120}})
}

func TestQAActivitySequenceGapCommitsQuarantineBeforeError(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
	gap := qaEvent("A", "1", "4", "work", "")
	h.at(20)
	_, err := h.service.Ingest(context.Background(), gap)
	qaCode(t, err, "event_gap")
	h.restart()
	s := h.snapshot()
	if len(s.Uncertainties) != 1 || !s.Uncertainties[0].LowerBound.Equal(qaEpochStart.Add(10*time.Second)) || s.Uncertainties[0].UpperBound != nil || s.Uncertainties[0].Reason != "event_gap" || s.Uncertainties[0].State != "unresolved" {
		t.Fatalf("gap rejection did not durably quarantine: %+v", s.Uncertainties)
	}
	if len(s.Actors) != 1 || s.Actors[0].Sequence != "2" || s.Actors[0].Health != "order_blocked" {
		t.Fatalf("gap advanced accepted sequence or lost order block: %+v", s.Actors)
	}
	id := s.Uncertainties[0].ID
	_, err = h.service.Ingest(context.Background(), gap)
	qaCode(t, err, "event_gap")
	s = h.snapshot()
	if len(s.Uncertainties) != 1 || s.Uncertainties[0].ID != id {
		t.Fatal("repeated rejected gap duplicated uncertainty")
	}
	h.ingest(30, qaEvent("A", "2", "1", "work", qaBindingA))
	h.ingest(40, qaEvent("A", "2", "2", "finish", ""))
	s = h.snapshot()
	qaIntervals(t, s, nil)
	if len(s.Uncertainties) != 1 || s.Uncertainties[0].UpperBound == nil || !s.Uncertainties[0].UpperBound.Equal(qaEpochStart.Add(30*time.Second)) || s.Uncertainties[0].State != "unresolved" {
		t.Fatalf("replacement silently resolved or failed to cap prior tail: %+v", s.Uncertainties)
	}
}

func TestQAActivityElapsedUnionDoesNotBillSubToleranceWallShift(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	for _, step := range []struct {
		at int64
		e  Event
	}{{10, qaEvent("B", "1", "1", "work", qaBindingB)}, {20, qaEvent("A", "1", "2", "finish", "")}, {30, qaEvent("B", "1", "2", "finish", "")}} {
		h.at(step.at)
		h.sample.WallUTC = h.sample.WallUTC.Add(900 * time.Millisecond)
		if _, err := h.service.Ingest(context.Background(), step.e); err != nil {
			t.Fatal(err)
		}
	}
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 30}})
	if len(s.Uncertainties) != 0 {
		t.Fatalf("sub-tolerance wall shift created uncertainty: %+v", s.Uncertainties)
	}
}

func TestQAActivityClockFailureQuarantinesBeforeReturningError(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
	h.at(20)
	h.clockErr = errors.New("PRIVATE-CLOCK-DETAIL-MUST-NOT-LEAK")
	_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "3", "finish", ""))
	qaCode(t, err, "clock_unavailable")
	if strings.Contains(err.Error(), "PRIVATE-CLOCK") {
		t.Fatalf("raw clock error escaped: %v", err)
	}
	h.at(30)
	h.restart()
	s := h.snapshot()
	if len(s.Uncertainties) != 1 || !s.Uncertainties[0].LowerBound.Equal(qaEpochStart.Add(10*time.Second)) || s.Uncertainties[0].State != "unresolved" {
		t.Fatalf("clock error lost prior work uncertainty: %+v", s.Uncertainties)
	}
	if len(s.Actors) != 1 || s.Actors[0].Sequence != "2" {
		t.Fatalf("rejected clock transition consumed sequence: %+v", s.Actors)
	}
	id := s.Uncertainties[0].ID
	h.ingest(30, qaEvent("A", "1", "3", "finish", ""))
	s = h.snapshot()
	qaIntervals(t, s, nil)
	if len(s.Uncertainties) != 1 || s.Uncertainties[0].ID != id || s.Uncertainties[0].State != "unresolved" || s.Uncertainties[0].UpperBound == nil || !s.Uncertainties[0].UpperBound.Equal(qaEpochStart.Add(30*time.Second)) {
		t.Fatalf("normal stop validated missing clock evidence: %+v", s.Uncertainties)
	}
}

func TestQAActivityLongSilentTurnAndStatusRemainReadOnly(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	before, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	h.at(900)
	h.service.resolve = func(context.Context, Event) (BindingSnapshot, bool, error) {
		t.Fatal("status read binding resolver")
		return BindingSnapshot{}, false, nil
	}
	s := h.snapshot()
	if len(s.Uncertainties) != 0 || len(s.ClosedIntervals) != 0 || len(s.Projects) != 1 || len(s.Projects[0].ActiveActorRefs) != 1 {
		t.Fatalf("silence became inactivity or uncertainty: %+v", s)
	}
	after, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("status extended evidence or changed durable health")
	}
	h.restart()
	h.ingest(1800, qaEvent("A", "1", "2", "wait_user", ""))
	s = h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 1800}})
	if len(s.Uncertainties) != 0 || len(s.Actors) != 1 || s.Actors[0].State != "wait_user" {
		t.Fatalf("long continuous turn did not close in waiting state: %+v", s)
	}
}

func TestQAActivityParentWaitingAndFinishingDoesNotStopNestedChild(t *testing.T) {
	h := qaNew(t)
	h.seed()
	p := qaEvent("P", "1", "1", "work", qaBindingA)
	h.ingest(0, p)
	a := qaEvent("A", "1", "1", "work", "")
	a.Parent = &ActorRef{Key: p.Actor, Generation: "1"}
	h.ingest(5, a)
	h.ingest(10, qaEvent("P", "1", "2", "wait_children", ""))
	b := qaEvent("B", "1", "1", "work", "")
	b.Parent = &ActorRef{Key: a.Actor, Generation: "1"}
	h.ingest(15, b)
	h.ingest(20, qaEvent("A", "1", "2", "finish", ""))
	h.ingest(25, qaEvent("P", "1", "3", "finish", ""))
	s := h.snapshot()
	if len(s.Projects) != 1 || len(s.Projects[0].ActiveActorRefs) != 1 || s.Projects[0].ActiveActorRefs[0].Key.AgentID != "B" {
		t.Fatalf("parent termination cascaded or inheritance failed: %+v", s.Projects)
	}
	h.ingest(30, qaEvent("B", "1", "2", "finish", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 30}})
}

func TestQAActivityForeignComputerAndConflictingEventCannotMutate(t *testing.T) {
	h := qaNew(t)
	h.seed()
	e := qaEvent("A", "1", "1", "work", qaBindingA)
	foreign := e
	foreign.Actor.ComputerID = "44444444-4444-4444-8444-444444444444"
	before, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.service.Ingest(context.Background(), foreign)
	qaCode(t, err, "validation")
	after, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("foreign computer ingress changed state")
	}
	h.ingest(0, e)
	before, err = os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	conflict := e
	conflict.Kind = "finish"
	_, err = h.service.Ingest(context.Background(), conflict)
	qaCode(t, err, "event_conflict")
	reused := qaEvent("B", "1", "1", "work", qaBindingB)
	reused.EventID = e.EventID
	_, err = h.service.Ingest(context.Background(), reused)
	qaCode(t, err, "event_conflict")
	after, err = os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("event identity conflict changed state")
	}
}

func TestQAActivityDifferentProjectsRetainIndependentElapsedTime(t *testing.T) {
	h := qaNew(t)
	b := h.bindings[qaBindingB]
	b.Attribution.ProjectID = "5"
	h.bindings[qaBindingB] = b
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(0, qaEvent("B", "1", "1", "work", qaBindingB))
	h.ingest(20, qaEvent("A", "1", "2", "finish", ""))
	h.ingest(20, qaEvent("B", "1", "2", "finish", ""))
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 20}, {0, 20}})
	got := map[string]string{}
	for _, i := range s.ClosedIntervals {
		got[i.Attribution.ProjectID] = i.DurationNS
	}
	if !reflect.DeepEqual(got, map[string]string{"3": "20000000000", "5": "20000000000"}) {
		t.Fatalf("independent clocks collapsed: %+v", got)
	}
}

func TestQAActivityClockDiscontinuitiesNeverValidateUnknownTail(t *testing.T) {
	for _, kind := range []string{"suspend", "new-boot", "counter-regression", "wall-forward"} {
		t.Run(kind, func(t *testing.T) {
			h := qaNew(t)
			h.seed()
			h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
			h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
			h.at(30)
			switch kind {
			case "suspend":
				n := "10000000000"
				h.sample.AwakeNS = &n
			case "new-boot":
				epoch, n := "boot-2", "0"
				h.sample.Epoch = &epoch
				h.sample.ElapsedNS = &n
				h.sample.AwakeNS = &n
			case "counter-regression":
				n := "5000000000"
				h.sample.ElapsedNS = &n
				h.sample.AwakeNS = &n
			case "wall-forward":
				h.sample.WallUTC = qaEpochStart.Add(70 * time.Second)
			}
			if _, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "3", "finish", "")); err != nil {
				t.Fatalf("trusted terminal should retain quarantine while detaching: %v", err)
			}
			h.restart()
			s := h.snapshot()
			qaIntervals(t, s, nil)
			if len(s.Uncertainties) != 1 || s.Uncertainties[0].State != "unresolved" || !s.Uncertainties[0].LowerBound.Equal(qaEpochStart.Add(10*time.Second)) {
				t.Fatalf("discontinuity billed unknown tail or lost confirmed bound: %+v", s.Uncertainties)
			}
		})
	}
}

func TestQAActivityStopUsesImmutableAttributionWithoutResolvingAgain(t *testing.T) {
	h := qaNew(t)
	h.seed()
	original := h.bindings[qaBindingA]
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.service.resolve = func(context.Context, Event) (BindingSnapshot, bool, error) {
		t.Fatal("stop re-resolved binding/current context")
		return BindingSnapshot{}, false, nil
	}
	stop := qaEvent("A", "1", "2", "finish", "")
	stop.CWD = "/unlinked/other-repository"
	h.ingest(20, stop)
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 20}})
	if s.ClosedIntervals[0].Attribution != original.Attribution {
		t.Fatalf("historical attribution changed: %+v", s.ClosedIntervals[0])
	}
}

func TestQAActivityZeroLengthAndInvalidHigherGenerationDoNotCreateTime(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.at(10)
	_, err := h.service.Ingest(context.Background(), qaEvent("A", "2", "1", "finish", ""))
	qaCode(t, err, "event_gap")
	s := h.snapshot()
	if len(s.Actors) != 1 || s.Actors[0].Ref.Generation != "1" || s.Actors[0].State != "working" || len(s.Uncertainties) != 0 {
		t.Fatalf("invalid higher generation altered active work: %+v", s)
	}
	h.ingest(20, qaEvent("A", "1", "2", "finish", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 20}})
	h.ingest(30, qaEvent("A", "2", "1", "work", qaBindingA))
	h.ingest(30, qaEvent("A", "2", "2", "finish", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 20}})
}

func TestQAActivityIngressProcessHelper(t *testing.T) {
	path := os.Getenv("TEMPO_QA_INGRESS_PATH")
	if path == "" {
		t.Skip("subprocess helper")
	}
	h := qaNew(t)
	h.path = path
	h.restart()
	h.service.store.timeout = time.Second
	kind := os.Getenv("TEMPO_QA_INGRESS_KIND")
	actor := os.Getenv("TEMPO_QA_INGRESS_ACTOR")
	e := qaEvent(actor, "1", "1", "work", qaBindingA)
	if kind == "finish" {
		h.at(10)
		e = qaEvent(actor, "1", "2", "finish", "")
	}
	r, err := h.service.Ingest(context.Background(), e)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(r.Disposition)
}

func qaIngressWave(t *testing.T, path, kind string, actors []string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	commands := make([]*exec.Cmd, len(actors))
	outputs := make([]bytes.Buffer, len(actors))
	dispositions := make([]string, len(actors))
	for i, actor := range actors {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQAActivityIngressProcessHelper$")
		cmd.Env = append(os.Environ(), "TEMPO_QA_INGRESS_PATH="+path, "TEMPO_QA_INGRESS_KIND="+kind, "TEMPO_QA_INGRESS_ACTOR="+actor)
		cmd.Stdout = &outputs[i]
		cmd.Stderr = &outputs[i]
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		commands[i] = cmd
	}
	for i, cmd := range commands {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("ingress%d %s: %v %s", i, kind, err, outputs[i].String())
		}
		dispositions[i] = strings.SplitN(outputs[i].String(), "\n", 2)[0]
	}
	return dispositions
}

func TestQAActivityConcurrentProcessesKeepActorsAndOneUnion(t *testing.T) {
	h := qaNew(t)
	h.seed()
	actors := []string{"A", "B", "C", "D", "E", "F"}
	for _, kind := range []string{"work", "finish"} {
		for _, got := range qaIngressWave(t, h.path, kind, actors) {
			if got != "applied" {
				t.Fatalf("distinct event disposition=%q", got)
			}
		}
	}
	h.at(10)
	h.restart()
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 10}})
	if len(s.Actors) != len(actors) {
		t.Fatalf("lost concurrent actors: %d", len(s.Actors))
	}
	st, ok, err := h.service.store.read(context.Background())
	if err != nil || !ok {
		t.Fatalf("read: %v", err)
	}
	if len(st.Outbox) != 1 {
		t.Fatalf("concurrent finalization duplicated outbox: %d", len(st.Outbox))
	}
	for _, got := range qaIngressWave(t, h.path, "finish", actors) {
		if got != "duplicate" {
			t.Fatalf("restarted process lost event receipt: %q", got)
		}
	}
	if !reflect.DeepEqual(s.ClosedIntervals, h.snapshot().ClosedIntervals) {
		t.Fatal("concurrent receipt replays changed immutable interval")
	}
}

func TestQAActivityConcurrentDuplicateIngressHasOneEffect(t *testing.T) {
	h := qaNew(t)
	h.seed()
	results := qaIngressWave(t, h.path, "work", []string{"A", "A", "A", "A", "A", "A"})
	counts := map[string]int{}
	for _, r := range results {
		counts[r]++
	}
	if counts["applied"] != 1 || counts["duplicate"] != 5 {
		t.Fatalf("duplicate concurrent effects: %+v", counts)
	}
	h.restart()
	s := h.snapshot()
	if len(s.Actors) != 1 {
		t.Fatalf("duplicate actor rows: %+v", s.Actors)
	}
	h.ingest(10, qaEvent("A", "1", "2", "finish", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 10}})
}

func TestQAActivityObservedClockLossQuarantinesEveryOpenProject(t *testing.T) {
	h := qaNew(t)
	b := h.bindings[qaBindingB]
	b.Attribution.ProjectID = "5"
	h.bindings[qaBindingB] = b
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(5, qaEvent("B", "1", "1", "work", qaBindingB))
	h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
	h.at(15)
	h.clockErr = errors.New("synthetic whole-computer clock failure")
	_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "3", "finish", ""))
	qaCode(t, err, "clock_unavailable")
	h.at(20)
	h.restart()
	s := h.snapshot()
	if len(s.Uncertainties) != 2 {
		t.Fatalf("machine clock loss quarantined only callback actor: %+v", s.Uncertainties)
	}
	lower := map[string]time.Time{}
	for _, u := range s.Uncertainties {
		lower[u.Actor.Key.AgentID] = u.LowerBound
	}
	if !lower["A"].Equal(qaEpochStart.Add(10*time.Second)) || !lower["B"].Equal(qaEpochStart.Add(5*time.Second)) {
		t.Fatalf("global quarantine lost individual confirmed bounds: %+v", lower)
	}
	h.ingest(20, qaEvent("B", "1", "2", "finish", ""))
	s = h.snapshot()
	qaIntervals(t, s, nil)
	for _, u := range s.Uncertainties {
		if u.Actor.Key.AgentID == "B" && (u.State != "unresolved" || u.UpperBound == nil || !u.UpperBound.Equal(qaEpochStart.Add(20*time.Second))) {
			t.Fatalf("later B stop validated globally unknown tail: %+v", u)
		}
	}
}

func TestQAActivityRejectedStaleObservationCannotCapUnknownTail(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
	h.at(20)
	h.clockErr = errors.New("synthetic clock loss")
	_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "3", "finish", ""))
	qaCode(t, err, "clock_unavailable")
	h.at(30)
	before := h.snapshot()
	if len(before.Uncertainties) != 1 || before.Uncertainties[0].UpperBound != nil {
		t.Fatalf("fixture missing unbounded uncertainty: %+v", before.Uncertainties)
	}
	_, err = h.service.Ingest(context.Background(), qaEvent("A", "1", "3", "observe_work", ""))
	qaCode(t, err, "invalid_transition")
	h.restart()
	after := h.snapshot()
	if len(after.Uncertainties) != 1 || after.Uncertainties[0].ID != before.Uncertainties[0].ID || after.Uncertainties[0].UpperBound != nil {
		t.Fatalf("rejected observation constrained possible working tail: %+v", after.Uncertainties)
	}
	if len(after.Actors) != 1 || after.Actors[0].Sequence != "2" {
		t.Fatalf("rejected observation advanced accepted sequence: %+v", after.Actors)
	}
	h.ingest(60, qaEvent("A", "1", "3", "finish", ""))
	s := h.snapshot()
	qaIntervals(t, s, nil)
	if len(s.Uncertainties) != 1 || s.Uncertainties[0].State != "unresolved" || s.Uncertainties[0].UpperBound == nil || !s.Uncertainties[0].UpperBound.Equal(qaEpochStart.Add(60*time.Second)) {
		t.Fatalf("valid finish lost legitimate recovery bound: %+v", s.Uncertainties)
	}
}

func TestQAActivitySequenceGapStillQuarantinesGlobalClockLoss(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(5, qaEvent("B", "1", "1", "work", qaBindingB))
	h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
	h.at(15)
	h.clockErr = errors.New("synthetic machine clock loss at sequence gap")
	_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "4", "work", ""))
	qaCode(t, err, "event_gap")
	h.at(20)
	h.restart()
	s := h.snapshot()
	if len(s.Uncertainties) != 2 {
		t.Fatalf("gap branch bypassed global clock quarantine: %+v", s.Uncertainties)
	}
	for _, a := range s.Actors {
		if a.Ref.Key.AgentID == "A" && (a.Sequence != "2" || a.Health != "order_blocked") {
			t.Fatalf("gap consumed event or failed to order-block: %+v", a)
		}
	}
	h.ingest(20, qaEvent("B", "1", "2", "finish", ""))
	s = h.snapshot()
	qaIntervals(t, s, nil)
	found := false
	for _, u := range s.Uncertainties {
		if u.Actor.Key.AgentID == "B" {
			found = true
			if u.State != "unresolved" || u.UpperBound == nil || !u.UpperBound.Equal(qaEpochStart.Add(20*time.Second)) {
				t.Fatalf("B tail became billable after rejected A gap: %+v", u)
			}
		}
	}
	if !found {
		t.Fatal("B clock uncertainty disappeared")
	}
}

func TestQAActivityUnlinkedChildCWDInheritsLiveParent(t *testing.T) {
	h := qaNew(t)
	h.seed()
	parent := qaEvent("P", "1", "1", "work", qaBindingA)
	h.ingest(0, parent)
	child := qaEvent("A", "1", "1", "work", "")
	child.CWD = "/unlinked/child-working-directory"
	child.Parent = &ActorRef{Key: parent.Actor, Generation: "1"}
	r := h.ingest(5, child)
	if r.Disposition != "applied" {
		t.Fatalf("unlinked CWD suppressed valid parent inheritance: %+v", r)
	}
	s := h.snapshot()
	found := false
	for _, a := range s.Actors {
		if a.Ref.Key.AgentID == "A" {
			found = true
			if a.BindingID != qaBindingA || a.Attribution != h.bindings[qaBindingA].Attribution {
				t.Fatalf("child did not capture exact parent snapshot: %+v", a)
			}
		}
	}
	if !found {
		t.Fatal("inherited child missing")
	}
	h.ingest(10, qaEvent("P", "1", "2", "finish", ""))
	h.ingest(20, qaEvent("A", "1", "2", "finish", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 20}})
}
