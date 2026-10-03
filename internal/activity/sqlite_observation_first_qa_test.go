//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"
)

func TestSQLiteObservationAbsentReadsAndMutationsDoNotInitialize(t *testing.T) {
	q := obQANew(t, 0, false)
	q.quiet(func() {
		for _, f := range []HostReceiptFilter{{}, {Source: "codex"}, {SessionID: "missing"}} {
			r, err := q.service().HostReceipts(q.ctx, f)
			if err != nil || r.ContractVersion != 1 || r.SnapshotRevision != "0" || r.Receipts == nil || len(r.Receipts) != 0 {
				t.Fatal("absent HostReceipts", err)
			}
		}
		for _, f := range []HostReceiptFilter{{Source: "other"}, {SessionID: "\x00"}} {
			r, err := q.service().HostReceipts(q.ctx, f)
			obQAError(t, err, "validation")
			if !reflect.DeepEqual(r, HostReceiptList{}) {
				t.Fatal("invalid receipt filter returned data")
			}
		}
		ref := ActorRef{Key: ActorKey{ComputerID: "11111111-1111-4111-8111-111111111111", Source: "manual-test", SessionID: "s", AgentID: "a"}, Generation: "1"}
		a, err := q.service().ObserveSource(q.ctx, SourceObservation{Actor: ref, Reason: "source_lost", RequestID: obQAID(10)})
		obQAError(t, err, "input_required")
		b, err := q.service().ObserveClock(q.ctx, ClockObservation{RequestID: obQAID(11)})
		obQAError(t, err, "input_required")
		c, err := q.service().ObserveHost(q.ctx, HostObservation{Source: "codex", SessionID: "s", TurnID: "t", Reason: "source_lost", RequestID: obQAID(12)})
		obQAError(t, err, "input_required")
		if !reflect.DeepEqual(a, MutationResult{}) || !reflect.DeepEqual(b, MutationResult{}) || !reflect.DeepEqual(c, MutationResult{}) {
			t.Fatal("absent observation returned mutation data")
		}
		_, err = q.service().ObserveSource(q.ctx, SourceObservation{Actor: ref, Reason: "silence", RequestID: obQAID(13)})
		obQAError(t, err, "validation")
		_, err = q.service().ObserveHost(q.ctx, HostObservation{Source: "codex", SessionID: "s", TurnID: "t", Reason: "silence", RequestID: obQAID(14)})
		obQAError(t, err, "validation")
		_, err = q.service().ObserveClock(q.ctx, ClockObservation{RequestID: "invalid"})
		obQAError(t, err, "validation")
	})
	for _, p := range []string{filepath.Dir(q.path), filepath.Dir(q.policyPath)} {
		if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("absent observation created storage", err)
		}
	}
}

func TestSQLiteObservationSourceExactLossAndStaleGeneration(t *testing.T) {
	for _, reason := range []string{"source_lost", "ordering_unavailable", "restart_unknown"} {
		t.Run(reason, func(t *testing.T) {
			q := obQANew(t, 1, false)
			start := q.event(0, 0, "A", "1", "1", "work")
			q.event(10, 0, "A", "1", "2", "observe_work")
			before := q.snapshot()
			original := obQAActor(t, before, start.Actor)
			q.at(86400) // Positive loss never confirms the intervening day.
			in := SourceObservation{Actor: start.Actor, Reason: reason, RequestID: obQAID(20)}
			calls := q.calls
			r, err := q.service().ObserveSource(q.ctx, in)
			if err != nil || q.calls != calls+1 {
				t.Fatal("exact source observation/sample", err)
			}
			after := q.snapshot()
			a := obQAActor(t, after, start.Actor)
			u := obQAUncertainty(t, after, start.Actor)
			health := "stale"
			if reason == "ordering_unavailable" {
				health = "order_blocked"
			}
			if a.Health != health || a.State != "working" || !reflect.DeepEqual(a.LastEvidence, original.LastEvidence) || u.Reason != reason || !u.LowerBound.Equal(q.when(10)) || u.UpperBound != nil || u.State != "unresolved" || u.ResolutionEnd != nil || len(after.ClosedIntervals) != 0 || after.Worker.QueuedCount != 0 {
				t.Fatal("source loss advanced confirmed work or invented a bound")
			}
			obQAMutation(t, r, in.RequestID, true, []string{a.ID, u.ID}, before.SnapshotRevision, after.SnapshotRevision)
			q.quiet(func() {
				replay, e := q.service().ObserveSource(q.ctx, in)
				if e != nil || !reflect.DeepEqual(replay, r) {
					t.Fatal("source receipt replay", e)
				}
			})
			if !reflect.DeepEqual(q.snapshot(), after) {
				t.Fatal("source replay changed retained public state")
			}
			conflict := in
			conflict.Reason = "source_lost"
			if reason == "source_lost" {
				conflict.Reason = "restart_unknown"
			}
			q.quiet(func() {
				got, e := q.service().ObserveSource(q.ctx, conflict)
				obQAError(t, e, "request_conflict")
				if !reflect.DeepEqual(got, MutationResult{}) {
					t.Fatal("conflict returned data")
				}
			})
			if !reflect.DeepEqual(q.snapshot(), after) {
				t.Fatal("conflicting source receipt changed history")
			}
		})
	}
	t.Run("older-and-future-generation", func(t *testing.T) {
		q := obQANew(t, 1, false)
		old := q.event(0, 0, "A", "1", "1", "work").Actor
		q.event(10, 0, "A", "1", "2", "finish")
		current := q.event(20, 0, "A", "2", "1", "work").Actor
		q.at(30)
		before := q.snapshot()
		if current.Key != old.Key || current.Generation != "2" || len(before.Uncertainties) != 0 {
			t.Fatal("generation premise")
		}
		in := SourceObservation{Actor: old, Reason: "source_lost", RequestID: obQAID(21)}
		var r MutationResult
		q.quiet(func() {
			var err error
			r, err = q.service().ObserveSource(q.ctx, in)
			if err != nil {
				t.Fatal(err)
			}
		})
		after := q.snapshot()
		obQAMutation(t, r, in.RequestID, false, []string{}, before.SnapshotRevision, after.SnapshotRevision)
		obQASameFacts(t, before, after)
		if a := obQAActor(t, after, current); a.State != "working" || a.Health != "continuous" {
			t.Fatal("old generation touched the current actor")
		}
		q.quiet(func() {
			replay, err := q.service().ObserveSource(q.ctx, in)
			if err != nil || !reflect.DeepEqual(replay, r) {
				t.Fatal("stale receipt replay", err)
			}
		})
		for i, ref := range []ActorRef{{Key: current.Key, Generation: "3"}, {Key: ActorKey{ComputerID: current.Key.ComputerID, Source: "manual-test", SessionID: "observation", AgentID: "absent"}, Generation: "1"}} {
			q.quiet(func() {
				result, err := q.service().ObserveSource(q.ctx, SourceObservation{Actor: ref, Reason: "source_lost", RequestID: obQAID(22 + i)})
				obQAError(t, err, "actor_not_found")
				if !reflect.DeepEqual(result, MutationResult{}) {
					t.Fatal("missing actor returned data")
				}
			})
		}
		if !reflect.DeepEqual(q.snapshot(), after) {
			t.Fatal("missing/future source observation changed state")
		}
	})
}

func TestSQLiteObservationClockNoHeartbeatAndGlobalQuarantine(t *testing.T) {
	for _, kind := range []string{"epoch", "suspend", "unavailable"} {
		t.Run(kind, func(t *testing.T) {
			q := obQANew(t, 2, false)
			a := q.event(0, 0, "A", "1", "1", "work").Actor
			b := q.event(5, 1, "B", "1", "1", "work").Actor
			q.event(10, 0, "A", "1", "2", "observe_work")
			q.event(10, 1, "B", "1", "2", "observe_work")
			q.at(20)
			before := q.snapshot()
			continuous := ClockObservation{RequestID: obQAID(30)}
			calls := q.calls
			noop, err := q.service().ObserveClock(q.ctx, continuous)
			if err != nil || q.calls != calls+1 {
				t.Fatal("continuous observation", err)
			}
			unchanged := q.snapshot()
			obQAMutation(t, noop, continuous.RequestID, false, []string{}, before.SnapshotRevision, unchanged.SnapshotRevision)
			obQASameFacts(t, before, unchanged)
			q.quiet(func() {
				replay, e := q.service().ObserveClock(q.ctx, continuous)
				if e != nil || !reflect.DeepEqual(replay, noop) {
					t.Fatal("continuous no-op replay", e)
				}
			})
			q.at(30)
			reason := "restart_unknown"
			switch kind {
			case "epoch":
				epoch := "rebooted"
				q.sample.Epoch = &epoch
			case "suspend":
				awake := "15000000000"
				q.sample.AwakeNS = &awake
				reason = "suspend"
			case "unavailable":
				q.clockErr = errors.New("synthetic unavailable")
			}
			in := ClockObservation{RequestID: obQAID(31)}
			calls = q.calls
			r, err := q.service().ObserveClock(q.ctx, in)
			if err != nil || q.calls != calls+1 {
				t.Fatal("positive global clock observation", err)
			}
			q.clockErr = nil
			after := q.snapshot()
			ids := []string{}
			if len(after.Uncertainties) != 2 || len(after.ClosedIntervals) != 0 || after.Worker.QueuedCount != 0 {
				t.Fatal("global clock quarantine omitted or billed a project")
			}
			for _, ref := range []ActorRef{a, b} {
				actor := obQAActor(t, after, ref)
				u := obQAUncertainty(t, after, ref)
				old := obQAActor(t, before, ref)
				if actor.Health != "stale" || actor.State != "working" || !reflect.DeepEqual(actor.LastEvidence, old.LastEvidence) || u.Reason != reason || !u.LowerBound.Equal(q.when(10)) || u.UpperBound != nil || u.State != "unresolved" {
					t.Fatal("clock detection invented confirmation or failed to retain a peer")
				}
				ids = append(ids, actor.ID, u.ID)
			}
			obQAMutation(t, r, in.RequestID, true, ids, unchanged.SnapshotRevision, after.SnapshotRevision)
			q.quiet(func() {
				replay, e := q.service().ObserveClock(q.ctx, in)
				if e != nil || !reflect.DeepEqual(replay, r) {
					t.Fatal("global clock replay sampled again", e)
				}
			})
			if !reflect.DeepEqual(q.snapshot(), after) {
				t.Fatal("clock replay changed quarantine")
			}
		})
	}
}

func TestSQLiteObservationHostExactTurnAndHistoricalTurn(t *testing.T) {
	for _, historical := range []bool{false, true} {
		name := "current"
		if historical {
			name = "historical"
		}
		t.Run(name, func(t *testing.T) {
			q := obQANew(t, 1, true)
			q.host(0, "host", "SessionStart", "", "")
			first := q.host(0, "host", "UserPromptSubmit", "old-turn", "")
			if first.Actor == nil {
				t.Fatal("host root premise")
			}
			var peer *ActorRef
			if historical {
				q.host(10, "host", "Stop", "old-turn", "")
				next := q.host(20, "host", "UserPromptSubmit", "new-turn", "")
				if next.Actor == nil || next.Actor.Key != first.Actor.Key || next.Actor.Generation != "2" {
					t.Fatal("historical turn did not retain the old generation")
				}
				peer = next.Actor
			} else {
				child := q.host(5, "host", "SubagentStart", "child-turn", "child")
				peer = child.Actor
				if peer == nil {
					t.Fatal("child premise")
				}
			}
			q.at(30)
			before := q.snapshot()
			in := HostObservation{Source: "codex", SessionID: "host", TurnID: "old-turn", Reason: "source_lost", RequestID: obQAID(40)}
			var r MutationResult
			var err error
			calls := q.calls
			if historical {
				q.quiet(func() { r, err = q.service().ObserveHost(q.ctx, in) })
			} else {
				r, err = q.service().ObserveHost(q.ctx, in)
			}
			if err != nil || !historical && q.calls != calls+1 {
				t.Fatal("host observation", err)
			}
			after := q.snapshot()
			ids := []string{}
			if historical {
				obQASameFacts(t, before, after)
			} else {
				u := obQAUncertainty(t, after, *first.Actor)
				target := obQAActor(t, after, *first.Actor)
				if len(after.Uncertainties) != 1 || target.Health != "stale" || u.Reason != "source_lost" || !u.LowerBound.Equal(q.when(0)) || u.UpperBound != nil || len(after.ClosedIntervals) != 0 {
					t.Fatal("native turn loss missed exact actor or billed silence")
				}
				ids = []string{target.ID, u.ID}
			}
			if !reflect.DeepEqual(obQAActor(t, before, *peer), obQAActor(t, after, *peer)) {
				t.Fatal("host observation affected another actor/generation")
			}
			obQAMutation(t, r, in.RequestID, !historical, ids, before.SnapshotRevision, after.SnapshotRevision)
			q.quiet(func() {
				replay, e := q.service().ObserveHost(q.ctx, in)
				if e != nil || !reflect.DeepEqual(replay, r) {
					t.Fatal("host replay", e)
				}
			})
			q.quiet(func() {
				missing := in
				missing.TurnID = "missing-turn"
				missing.RequestID = obQAID(41)
				got, e := q.service().ObserveHost(q.ctx, missing)
				obQAError(t, e, "actor_not_found")
				if !reflect.DeepEqual(got, MutationResult{}) {
					t.Fatal("missing turn returned data")
				}
			})
			if !reflect.DeepEqual(q.snapshot(), after) {
				t.Fatal("host replay/missing tuple changed state")
			}
		})
	}
}

func TestSQLiteObservationHostReceiptFiltersNumericOrderAndOwnership(t *testing.T) {
	q := obQANew(t, 1, true)
	want := []HostReceipt{}
	for i, session := range []string{"receipt-c", "receipt-a", "receipt-b"} {
		at := int64(i * 20)
		want = append(want, q.host(at, session, "SessionStart", "", ""), q.host(at, session, "UserPromptSubmit", "turn", ""), q.host(at+10, session, "Stop", "turn", ""))
	}
	before := q.snapshot()
	if len(want) != 9 || want[0].SnapshotRevision != "2" || want[8].SnapshotRevision != "10" {
		t.Fatal("numeric revision ordering premise did not cross9/10")
	}
	sort.Slice(want, func(i, j int) bool {
		a, _ := strconv.ParseUint(want[i].SnapshotRevision, 10, 64)
		b, _ := strconv.ParseUint(want[j].SnapshotRevision, 10, 64)
		if a != b {
			return a < b
		}
		return want[i].ID < want[j].ID
	})
	q.quiet(func() {
		for _, filter := range []HostReceiptFilter{{}, {Source: "codex"}, {Source: "claude"}, {SessionID: "receipt-a"}, {Source: "codex", SessionID: "receipt-b"}, {Source: "claude", SessionID: "receipt-a"}, {SessionID: "absent"}} {
			expected := []HostReceipt{}
			for _, r := range want {
				if (filter.Source == "" || filter.Source == r.Source) && (filter.SessionID == "" || filter.SessionID == r.SessionID) {
					expected = append(expected, r)
				}
			}
			got, err := q.service().HostReceipts(q.ctx, filter)
			if err != nil || got.ContractVersion != 1 || got.SnapshotRevision != before.SnapshotRevision || got.Receipts == nil || !reflect.DeepEqual(got.Receipts, expected) {
				t.Fatal("host receipt filter/order/materialization", err)
			}
			for i := range got.Receipts {
				got.Receipts[i].Kind = "caller mutation"
				if got.Receipts[i].Actor != nil {
					got.Receipts[i].Actor.Key.AgentID = "caller mutation"
				}
			}
		}
		again, err := q.service().HostReceipts(q.ctx, HostReceiptFilter{})
		if err != nil || !reflect.DeepEqual(again.Receipts, want) {
			t.Fatal("caller mutation changed retained host receipts", err)
		}
	})
	if !reflect.DeepEqual(q.snapshot(), before) {
		t.Fatal("host receipt reads changed public state/revision")
	}
}
