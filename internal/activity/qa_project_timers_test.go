package activity

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/harvest"
)

func qaProjectTimer(t *testing.T, s ActivitySnapshot, account, project string) ProjectTimer {
	t.Helper()
	var matches []ProjectTimer
	for _, timer := range s.ProjectTimers {
		if timer.AccountID == account && timer.ProjectID == project {
			matches = append(matches, timer)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("want one computer/account/project timer for %s/%s, got %+v", account, project, s.ProjectTimers)
	}
	r := matches[0]
	if s.ComputerID == nil || r.ComputerID != *s.ComputerID || r.ActiveActorRefs == nil || r.WaitingActorRefs == nil || r.UnresolvedIDs == nil {
		t.Fatalf("timer lost identity or non-null collections: %+v", r)
	}
	return r
}

func qaTimerDurations(t *testing.T, timer ProjectTimer, provisional, confirmed int64) {
	t.Helper()
	if timer.ProvisionalUnionNS != strconv.FormatInt(provisional*int64(time.Second), 10) || timer.ConfirmedClosedNS != strconv.FormatInt(confirmed*int64(time.Second), 10) {
		t.Fatalf("timer must use shared union, not actor sums: %+v; want provisional=%ds confirmed=%ds", timer, provisional, confirmed)
	}
}

func TestQAProjectTimersAbsentIsEmptyWithoutIdentityOrWrites(t *testing.T) {
	h := qaNew(t)
	s := h.snapshot()
	if s.ProjectTimers == nil || len(s.ProjectTimers) != 0 {
		t.Errorf("absent timers must be [], got %+v", s.ProjectTimers)
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["project_timers"]) != "[]" {
		t.Errorf("machine projection must include project_timers:[], got %s", fields["project_timers"])
	}
	if s.ComputerID != nil || s.SnapshotRevision != "0" {
		t.Errorf("read invented identity/revision: %+v", s)
	}
	if _, err := os.Stat(filepath.Dir(h.path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("absent status initialized files: %v", err)
	}
}

func TestQAProjectTimersOverlappingParentChildAndClockOnlyRefresh(t *testing.T) {
	h := qaNew(t)
	b := h.bindings[qaBindingB]
	b.Attribution.ProjectID = "9"
	h.bindings[qaBindingB] = b
	h.seed()
	parent := qaEvent("P", "1", "1", "work", qaBindingA)
	h.ingest(0, parent)
	child := qaEvent("A", "1", "1", "work", "")
	child.Parent = &ActorRef{Key: parent.Actor, Generation: "1"}
	h.ingest(5, child)
	h.ingest(10, qaEvent("P", "1", "2", "wait_children", ""))
	h.ingest(15, qaEvent("B", "1", "1", "work", qaBindingB))
	h.at(30)
	before := qaRecoveryRead(t, h)
	s := h.snapshot()
	// Existing attribution projections are independently checked and retained.
	if len(s.Projects) != 2 {
		t.Fatalf("existing attribution rows changed: %+v", s.Projects)
	}
	for _, p := range s.Projects {
		want := "30000000000"
		if p.Attribution.ProjectID == "9" {
			want = "15000000000"
		}
		if p.ProvisionalUnionNS != want || p.ConfirmedClosedNS != "0" {
			t.Fatalf("attribution detail changed: %+v", p)
		}
	}
	a := qaProjectTimer(t, s, "1", "3")
	qaTimerDurations(t, a, 30, 0)
	if len(a.ActiveActorRefs) != 1 || a.ActiveActorRefs[0].Key.AgentID != "A" || len(a.WaitingActorRefs) != 1 || a.WaitingActorRefs[0].Key.AgentID != "P" {
		t.Fatalf("parent/child states lost: %+v", a)
	}
	qaTimerDurations(t, qaProjectTimer(t, s, "1", "9"), 15, 0)
	h.at(45)
	newer := h.snapshot()
	if newer.SnapshotRevision != s.SnapshotRevision || !newer.ObservedAt.After(s.ObservedAt) {
		t.Fatalf("clock-only refresh must advance observation without writes: before=%+v after=%+v", s, newer)
	}
	qaTimerDurations(t, qaProjectTimer(t, newer, "1", "3"), 45, 0)
	qaTimerDurations(t, qaProjectTimer(t, newer, "1", "9"), 30, 0)
	if !reflect.DeepEqual(before, qaRecoveryRead(t, h)) {
		t.Fatal("project timer observation mutated store")
	}
}

func TestQAProjectTimersPartitionAccountAndProjectDeterministically(t *testing.T) {
	h := qaNew(t)
	b := h.bindings[qaBindingB]
	b.Attribution.AccountID = "2"
	h.bindings[qaBindingB] = b
	const third = "44444444-4444-4444-8444-444444444444"
	c := h.bindings[qaBindingA]
	c.ID = third
	c.Attribution.ProjectID = "5"
	h.bindings[third] = c
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(10, qaEvent("B", "1", "1", "work", qaBindingB))
	h.ingest(20, qaEvent("C", "1", "1", "work", third))
	h.at(30)
	s := h.snapshot()
	if len(s.ProjectTimers) != 3 {
		t.Fatalf("separate accounts/projects collapsed: %+v", s.ProjectTimers)
	}
	qaTimerDurations(t, qaProjectTimer(t, s, "1", "3"), 30, 0)
	qaTimerDurations(t, qaProjectTimer(t, s, "1", "5"), 10, 0)
	qaTimerDurations(t, qaProjectTimer(t, s, "2", "3"), 20, 0)
	for i, want := range []string{"1/3", "1/5", "2/3"} {
		p := s.ProjectTimers[i]
		if p.AccountID+"/"+p.ProjectID != want {
			t.Fatalf("unstable project ordering: %+v", s.ProjectTimers)
		}
	}
	h.restart()
	if got := h.snapshot().ProjectTimers; !reflect.DeepEqual(s.ProjectTimers, got) {
		t.Fatalf("restart changed pure projection: %+v -> %+v", s.ProjectTimers, got)
	}
}

func TestQAProjectTimersSequentialAttributionEpochsPreserveHistory(t *testing.T) {
	h := qaNew(t)
	h.service = New(Options{Path: h.path, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, h.clockErr })})
	in := qaLinkInput(t)
	p := qaNewLinkProvider(t)
	first, err := h.service.Link(context.Background(), in, qaLinkDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	computer := *h.snapshot().ComputerID
	event := func(actor, seq, kind string, b Binding) Event {
		e := qaEvent(actor, "1", seq, kind, "")
		e.Actor.ComputerID = computer
		if kind == "work" {
			e.BindingID = b.ID
			e.BindingRevision = b.Revision
		}
		return e
	}
	h.ingest(0, event("old", "1", "work", first.Binding))
	h.ingest(10, event("old", "2", "finish", first.Binding))
	old := h.snapshot()
	qaIntervals(t, old, [][2]int64{{0, 10}})
	// Relinking is legal only after attached actors finish. Use actual shared
	// Link validation; do not fabricate overlapping incompatible histories.
	in.IfRevision = first.Binding.Revision
	in.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	in.TaskID = "5"
	in.Timezone = "America/New_York"
	p.user["id"] = json.Number("99")
	p.assignments[0]["task_assignments"] = []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number("5")}}}
	second, err := h.service.Link(context.Background(), in, qaLinkDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	h.ingest(20, event("new-closed", "1", "work", second.Binding))
	h.ingest(35, event("new-closed", "2", "finish", second.Binding))
	h.ingest(40, event("new-active", "1", "work", second.Binding))
	h.at(45)
	before := qaRecoveryRead(t, h)
	calls := len(p.calls)
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 10}, {20, 35}})
	if !reflect.DeepEqual(s.ClosedIntervals[0], old.ClosedIntervals[0]) {
		t.Fatal("projection rewrote immutable old interval")
	}
	if len(s.Projects) != 2 {
		t.Fatalf("distinct task/user/timezone detail epochs lost: %+v", s.Projects)
	}
	for _, detail := range s.Projects {
		switch detail.Attribution {
		case first.Binding.Attribution:
			if detail.ConfirmedClosedNS != "10000000000" || detail.ProvisionalUnionNS != "0" {
				t.Fatalf("old detail changed: %+v", detail)
			}
		case second.Binding.Attribution:
			if detail.ConfirmedClosedNS != "15000000000" || detail.ProvisionalUnionNS != "5000000000" {
				t.Fatalf("new detail changed: %+v", detail)
			}
		default:
			t.Fatalf("invented attribution: %+v", detail)
		}
	}
	if len(s.ProjectTimers) != 1 {
		t.Fatalf("attribution epochs must share one project timer: %+v", s.ProjectTimers)
	}
	timer := qaProjectTimer(t, s, "1", "3")
	qaTimerDurations(t, timer, 5, 25)
	if timer.QueuedCount != 2 || timer.SyncedCount != 0 || timer.NeedsAttentionCount != 0 || len(timer.ActiveActorRefs) != 1 || timer.ActiveActorRefs[0].Key.AgentID != "new-active" {
		t.Fatalf("timer lost epoch states: %+v", timer)
	}
	if calls != len(p.calls) || !reflect.DeepEqual(before, qaRecoveryRead(t, h)) {
		t.Fatal("pure project status accessed provider or rewrote history")
	}
}

func TestQAProjectTimersUncertaintyFreezesConfirmedPrefix(t *testing.T) {
	h, u := qaRecoveryUncertain(t)
	before := qaRecoveryRead(t, h)
	s := h.snapshot()
	timer := qaProjectTimer(t, s, "1", "3")
	qaTimerDurations(t, timer, 300, 0)
	if timer.NeedsAttentionCount != 1 || !reflect.DeepEqual(timer.UnresolvedIDs, []string{u.ID}) || len(timer.ActiveActorRefs) != 0 {
		t.Fatalf("uncertain tail reported as live/confirmed time: %+v", timer)
	}
	h.at(3600)
	later := h.snapshot()
	if later.SnapshotRevision != s.SnapshotRevision || !reflect.DeepEqual(timer, qaProjectTimer(t, later, "1", "3")) {
		t.Fatalf("silent time extended uncertain tail: before=%+v after=%+v", timer, later.ProjectTimers)
	}
	if !reflect.DeepEqual(before, qaRecoveryRead(t, h)) {
		t.Fatal("uncertainty projection wrote evidence")
	}
}
