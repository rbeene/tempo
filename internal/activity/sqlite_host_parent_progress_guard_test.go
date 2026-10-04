//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"reflect"
	"testing"
	"time"
)

// These are valid owned local projections for a pure comparator check, not a
// fabricated whole SQL state. The separate public native race remains required.
func hpQAFacts() (HostEvent, sqliteHostFacts) {
	e := HostEvent{Source: "codex", Kind: "SubagentStart", SessionID: "native-parent", TurnID: "child-turn", AgentID: "child", CWD: "/synthetic/parent"}
	incarnation := spQAID(60)
	ref := ActorRef{Key: ActorKey{ComputerID: interopComputer, Source: "codex", SessionID: incarnation, AgentID: "root"}, Generation: "1"}
	root := sqliteHostTurnRow{Source: "codex", NativeSession: e.SessionID, Incarnation: incarnation, TurnID: "root-turn", CWD: e.CWD, Actor: &ref}
	root.Key = hostTurnKey(incarnation, HostEvent{TurnID: root.TurnID})
	a := sqliteActorLocalRow{ID: spQAID(61), Revision: "2", Ref: ref, Sequence: "2", State: "working", Health: "continuous", BindingID: spQAID(1), BindingRevision: "1",
		Attribution: Attribution{AccountID: "11", UserID: "7", ProjectID: "100", TaskID: "200", Timezone: "UTC"}, SegmentID: asQAPointer(spQAID(62)),
		LastEvidence: ClockSample{Capability: "available", WallUTC: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Epoch: asQAPointer("parent-boot"), ElapsedNS: asQAPointer("1"), AwakeNS: asQAPointer("1")}}
	session := sqliteHostSessionRow{Key: hostSessionKey(e), Value: hostSession{ID: incarnation, Source: e.Source, NativeID: e.SessionID, CWD: e.CWD, RootTurn: root.Key}}
	return e, sqliteHostFacts{ComputerID: interopComputer, Session: &session, RootTurn: &root, RootActor: &a, ReceiptKey: hostEventKey(incarnation, e)}
}

func hpQAProgress(t *testing.T, prepared sqliteHostFacts, stop bool) sqliteHostFacts {
	t.Helper()
	current := asQAJSON(t, prepared)
	current.RootActor.Revision, current.RootActor.Sequence = "3", "3"
	current.RootActor.LastEvidence.WallUTC = current.RootActor.LastEvidence.WallUTC.Add(time.Second)
	current.RootActor.LastEvidence.ElapsedNS, current.RootActor.LastEvidence.AwakeNS = asQAPointer("2"), asQAPointer("2")
	if stop {
		current.RootActor.State = "wait_user"
		current.RootActor.SegmentID = nil
		current.RootTurn.Stopped = true
	}
	return current
}

func hpQAValid(t *testing.T, f sqliteHostFacts) {
	t.Helper()
	if f.RootActor != nil {
		if _, err := sqliteEncodeActorLocal(*f.RootActor); err != nil {
			t.Fatal("actor fixture is not independently valid", err)
		}
	}
	if f.RootTurn != nil {
		if _, err := sqliteEncodeHostTurn(f.ComputerID, *f.RootTurn); err != nil {
			t.Fatal("turn fixture is not independently valid", err)
		}
	}
}

func hpQACompare(t *testing.T, e HostEvent, current, prepared sqliteHostFacts, want bool) {
	t.Helper()
	hpQAValid(t, current)
	hpQAValid(t, prepared)
	oldCurrent, oldPrepared := asQAJSON(t, current), asQAJSON(t, prepared)
	got, err := sqliteHostSamePreparedFacts(e, current, prepared)
	if err != nil || got != want {
		t.Fatal("parent fact comparison", got, want, err)
	}
	if !reflect.DeepEqual(current, oldCurrent) || !reflect.DeepEqual(prepared, oldPrepared) {
		t.Fatal("comparison mutated owned facts")
	}
}

func TestSQLiteHostChildParentProgressKeepsImmutableWitnesses(t *testing.T) {
	t.Run("PostToolUse", func(t *testing.T) {
		e, before := hpQAFacts()
		hpQACompare(t, e, hpQAProgress(t, before, false), before, true)
	})
	t.Run("Stop", func(t *testing.T) {
		e, before := hpQAFacts()
		hpQACompare(t, e, hpQAProgress(t, before, true), before, true)
	})
	for _, axis := range []string{"actor-id", "generation", "binding-id", "binding-revision", "account", "user", "project", "task", "timezone", "parent", "health", "finished", "interrupted", "revision-backward", "sequence-backward", "stopped-backward", "turn-source", "native-session", "incarnation", "turn-id", "agent-id", "cwd", "turn-actor-generation", "missing-turn", "missing-actor", "session-cwd", "receipt-key", "highest"} {
		t.Run(axis, func(t *testing.T) {
			e, before := hpQAFacts()
			current := hpQAProgress(t, before, false)
			switch axis {
			case "actor-id":
				current.RootActor.ID = spQAID(63)
			case "generation":
				current.RootActor.Ref.Generation = "2"
				ref := current.RootActor.Ref
				current.RootTurn.Actor = &ref
			case "binding-id":
				current.RootActor.BindingID = spQAID(2)
			case "binding-revision":
				current.RootActor.BindingRevision = "2"
			case "account":
				current.RootActor.Attribution.AccountID = "12"
			case "user":
				current.RootActor.Attribution.UserID = "8"
			case "project":
				current.RootActor.Attribution.ProjectID = "101"
			case "task":
				current.RootActor.Attribution.TaskID = "201"
			case "timezone":
				current.RootActor.Attribution.Timezone = "America/New_York"
			case "parent":
				ref := ActorRef{Key: ActorKey{ComputerID: interopComputer, Source: "manual-test", SessionID: "ancestor", AgentID: "ancestor"}, Generation: "1"}
				current.RootActor.Parent = &ref
			case "health":
				current.RootActor.Health = "stale"
			case "finished", "interrupted":
				current.RootActor.State = axis
				current.RootActor.SegmentID = nil
			case "revision-backward":
				current.RootActor.Revision = "1"
			case "sequence-backward":
				current.RootActor.Sequence = "1"
			case "stopped-backward":
				before.RootTurn.Stopped = true
			case "turn-source":
				current.RootTurn.Source = "claude"
				current.RootActor.Ref.Key.Source = "claude"
				ref := current.RootActor.Ref
				current.RootTurn.Actor = &ref
			case "native-session":
				current.RootTurn.NativeSession = "other-native"
			case "incarnation":
				current.RootTurn.Incarnation = spQAID(64)
				current.RootActor.Ref.Key.SessionID = current.RootTurn.Incarnation
				ref := current.RootActor.Ref
				current.RootTurn.Actor = &ref
			case "turn-id":
				current.RootTurn.TurnID = "other-root-turn"
			case "agent-id":
				current.RootTurn.AgentID = "other-agent"
				current.RootActor.Ref.Key.AgentID = hostAgent(current.RootTurn.AgentID)
				ref := current.RootActor.Ref
				current.RootTurn.Actor = &ref
			case "cwd":
				current.RootTurn.CWD = "/synthetic/other"
			case "turn-actor-generation":
				current.RootTurn.Actor.Generation = "9"
			case "missing-turn":
				current.RootTurn = nil
			case "missing-actor":
				current.RootActor = nil
			case "session-cwd":
				current.Session.Value.CWD = "/synthetic/other"
			case "receipt-key":
				current.ReceiptKey = hostHash([]string{"different-receipt"})
			case "highest":
				ref := ActorRef{Key: ActorKey{ComputerID: interopComputer, Source: "codex", SessionID: before.Session.Value.ID, AgentID: "child"}, Generation: "1"}
				current.Highest = &ref
			}
			if current.RootTurn != nil {
				current.RootTurn.Key = hostTurnKey(current.RootTurn.Incarnation, HostEvent{TurnID: current.RootTurn.TurnID, AgentID: current.RootTurn.AgentID})
			}
			if reflect.DeepEqual(current, hpQAProgress(t, before, false)) && axis != "stopped-backward" {
				t.Fatal("negative axis did not change its witness")
			}
			hpQACompare(t, e, current, before, false)
		})
	}
}

func TestSQLiteHostChildParentProgressScopeRetainsStrictComparison(t *testing.T) {
	for _, scope := range []string{"UserPromptSubmit", "Stop", "SubagentStop", "boundary-current", "boundary-prepared", "boundary-both", "existing-current", "existing-prepared", "existing-both"} {
		t.Run(scope, func(t *testing.T) {
			e, before := hpQAFacts()
			current := hpQAProgress(t, before, false)
			switch scope {
			case "boundary-current":
				current.Boundary = true
			case "boundary-prepared":
				before.Boundary = true
			case "boundary-both":
				current.Boundary, before.Boundary = true, true
			case "existing-current":
				current.Turn = current.RootTurn
			case "existing-prepared":
				before.Turn = before.RootTurn
			case "existing-both":
				current.Turn, before.Turn = current.RootTurn, before.RootTurn
			default:
				e.Kind = scope
			}
			strict, err := sqliteHostSameFacts(current, before)
			if err != nil || strict {
				t.Fatal("strict mismatch control", strict, err)
			}
			hpQACompare(t, e, current, before, false)
		})
	}
	for _, unchanged := range []string{"working", "finished", "absent-actor", "absent-root"} {
		t.Run("unchanged-"+unchanged, func(t *testing.T) {
			e, before := hpQAFacts()
			switch unchanged {
			case "finished":
				before.RootActor.State, before.RootActor.SegmentID = "finished", nil
			case "absent-actor":
				before.RootActor = nil
			case "absent-root":
				before.RootActor, before.RootTurn = nil, nil
			}
			current := asQAJSON(t, before)
			strict, err := sqliteHostSameFacts(current, before)
			if err != nil || !strict {
				t.Fatal("unchanged strict control", strict, err)
			}
			hpQACompare(t, e, current, before, true)
		})
	}
}
