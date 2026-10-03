//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"reflect"
	"sort"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Only facts that authorize this observation survive preparation. Host turns
// are the complete exact native tuple, including retained older incarnations.
type sqliteObservationFacts struct {
	ComputerID, RefusalCode string
	Sample                  bool
	Actor                   *sqliteActorLocalRow
	Observed                *sqliteCaptureClockActor
	Turns                   []sqliteHostTurnRow
	ClockActors             []sqliteCaptureClockActor
}

func sqliteReadObservationFacts(tx *sqliteio.Tx, meta sqliteStoreMeta, in sqliteObservationInput) (facts sqliteObservationFacts, err error) {
	facts.ComputerID = meta.ComputerID
	ref := in.Actor
	if in.Host != nil {
		h := *in.Host
		facts.Turns, err = sqliteHistoricalHostTurns(tx, meta.ComputerID, HostEvent{Source: h.Source, SessionID: h.SessionID, TurnID: h.TurnID, AgentID: h.AgentID})
		if err != nil {
			return facts, err
		}
		sort.Slice(facts.Turns, func(i, j int) bool { return facts.Turns[i].Key < facts.Turns[j].Key })
		for _, turn := range facts.Turns {
			if turn.Actor == nil {
				continue
			}
			if turn.Actor.Key.Source != turn.Source {
				return facts, failure("state_corrupt")
			}
			if ref.Generation != "" && ref != *turn.Actor {
				facts.RefusalCode = "event_conflict"
				return facts, nil
			}
			ref = *turn.Actor
		}
		if ref.Generation == "" {
			facts.RefusalCode = "actor_not_found"
			return facts, nil
		}
	}
	if in.Operation != "activity.observe_clock" {
		facts.Actor, err = sqliteCaptureReadActor(tx, meta.ComputerID, meta.Revision, ref.Key)
		if err != nil {
			return facts, err
		}
		if facts.Actor == nil {
			facts.RefusalCode = "actor_not_found"
			return facts, nil
		}
		if facts.Actor.Ref != ref {
			requested, _ := counter(ref.Generation)
			current, _ := counter(facts.Actor.Ref.Generation)
			if requested > current {
				facts.RefusalCode = "actor_not_found"
			}
			return facts, nil
		}
		if sqliteCaptureTerminal(*facts.Actor) {
			return facts, nil
		}
	}
	facts.Sample = true
	facts.ClockActors, err = sqliteCaptureObserveClock(tx, meta.ComputerID, meta.Revision)
	if err != nil {
		return facts, err
	}
	if facts.Actor != nil {
		// Reuse the owned full clock projection when this target is a candidate.
		for i := range facts.ClockActors {
			if facts.ClockActors[i].Actor.Ref == facts.Actor.Ref {
				facts.Observed = &facts.ClockActors[i]
				return facts, nil
			}
		}
		observed, readErr := sqliteCaptureObserveActor(tx, meta.ComputerID, meta.Revision, *facts.Actor)
		if readErr != nil {
			return facts, readErr
		}
		facts.Observed = &observed
	}
	return facts, nil
}

func sqliteSameObservationFacts(a, b sqliteObservationFacts) (bool, error) {
	if a.ComputerID != b.ComputerID || a.RefusalCode != b.RefusalCode || a.Sample != b.Sample || len(a.Turns) != len(b.Turns) || (a.Observed == nil) != (b.Observed == nil) {
		return false, nil
	}
	same, err := sqliteCaptureSameActor(a.Actor, b.Actor)
	if err != nil || !same {
		return same, err
	}
	for i := range a.Turns {
		x, err := sqliteEncodeHostTurn(a.ComputerID, a.Turns[i])
		if err != nil {
			return false, err
		}
		y, err := sqliteEncodeHostTurn(b.ComputerID, b.Turns[i])
		if err != nil {
			return false, err
		}
		if !reflect.DeepEqual(x.values, y.values) {
			return false, nil
		}
	}
	if a.Observed != nil {
		same, err = sqliteCaptureSameClock([]sqliteCaptureClockActor{*a.Observed}, []sqliteCaptureClockActor{*b.Observed})
		if err != nil || !same {
			return same, err
		}
	}
	return sqliteCaptureSameClock(a.ClockActors, b.ClockActors)
}

func sqliteApplyObservation(m *sqliteCaptureMutation, in sqliteObservationInput, facts sqliteObservationFacts, sample ClockSample) ([]string, error) {
	if !facts.Sample {
		return []string{}, nil
	}
	before := make(map[ActorKey]sqliteActorLocalRow, len(facts.ClockActors)+1)
	for _, observed := range facts.ClockActors {
		before[observed.Actor.Ref.Key] = observed.Actor
	}
	if facts.Actor != nil {
		before[facts.Actor.Ref.Key] = *facts.Actor
		// Explicit loss owns the target reason even if this sample also exposes
		// a global discontinuity. It never confirms or caps an unknown tail.
		if _, err := m.quarantine(*facts.Actor, in.Reason, "SourceObservation", sample); err != nil {
			return nil, err
		}
	}
	if _, err := m.quarantineClock(sample); err != nil {
		return nil, err
	}
	if facts.Actor != nil && in.Reason == "ordering_unavailable" {
		a, err := m.readActor(facts.Actor.Ref.Key)
		if err != nil {
			return nil, err
		}
		if a == nil || a.Ref != facts.Actor.Ref {
			return nil, failure("state_corrupt")
		}
		if a.Health != "order_blocked" {
			next := *a
			next.Health, next.Revision = "order_blocked", bump(a.Revision)
			if err = m.writeActor(a, next); err != nil {
				return nil, err
			}
		}
	}
	// Match changedActors: changed actor IDs and their currently unresolved
	// memberships, including retained uncertainties from earlier observations.
	ids, seen := map[string]bool{}, map[ActorKey]bool{}
	for _, key := range m.transition.Dependencies.ActorKeys {
		if seen[key] {
			continue
		}
		seen[key] = true
		old, selected := before[key]
		a, err := m.readActor(key)
		if err != nil {
			return nil, err
		}
		if !selected || a == nil || a.ID != old.ID || a.Ref != old.Ref {
			return nil, failure("state_corrupt")
		}
		if a.Revision == old.Revision {
			continue
		}
		ids[a.ID] = true
		members, err := sqliteActorUncertaintyIDsLocal(m.tx, m.meta.ComputerID, key)
		if err != nil {
			return nil, err
		}
		for ordinal, id := range members {
			u, found, err := sqliteReadUncertaintyScalar(m.tx, m.meta.ComputerID, id)
			if err != nil {
				return nil, err
			}
			if !found || u.Actor.Key != key {
				return nil, failure("state_corrupt")
			}
			m.transition.Dependencies.ActorUncertaintyEdges = append(m.transition.Dependencies.ActorUncertaintyEdges, sqliteActorUncertaintySelection{Key: key, Ordinal: int64(ordinal)})
			if u.State == "unresolved" {
				ids[id] = true
			}
		}
	}
	return sqliteCaptureSet(ids), nil
}
