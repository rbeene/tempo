//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// This first composer covers the Codex root/child start and stop route. Other
// native event families and fresh incarnation boundaries remain explicit.
func sqliteHostSupported(e HostEvent) bool {
	if e.Source != "codex" {
		return false
	}
	switch e.Kind {
	case "SessionStart":
		return e.SessionSource == "startup"
	case "UserPromptSubmit":
		return e.AgentID == ""
	case "SubagentStart", "Stop", "SubagentStop":
		return true
	}
	return false
}

type sqliteHostPolicy struct {
	CaptureEligible                              bool
	Basis, Revision, Fingerprint, DiagnosticCode string
}
type sqliteHostFacts struct {
	ComputerID string
	Session    *sqliteHostSessionRow
	Turn       *sqliteHostTurnRow
	Historical []sqliteHostTurnRow
	Actor      *sqliteActorLocalRow
	RootTurn   *sqliteHostTurnRow
	RootActor  *sqliteActorLocalRow
	Highest    *ActorRef
	ReceiptKey string
	Receipt    *sqliteHostReceiptRow
	Conflict   *sqliteHostReceiptRow
}
type sqliteHostPrepared struct {
	Facts                                   sqliteHostFacts
	CWD                                     string
	Policy                                  sqliteHostPolicy
	Admission                               sqliteCaptureBinding
	Normalized                              *Event
	Capture                                 sqliteCapturePreparation
	Clock                                   sqliteCaptureClock
	ReceiptID, NewIncarnation, BaseRevision string
	ObservedAt                              time.Time
}

const sqliteHostHighestGenerationSQL = "SELECT actor_key,generation,computer_id,source,session_id,agent_id FROM actor_generations WHERE actor_key=? ORDER BY generation DESC LIMIT 1"

func sqliteHostHighestGeneration(tx *sqliteio.Tx, key ActorKey) (result *ActorRef, err error) {
	s, err := tx.Prepare(sqliteHostHighestGenerationSQL, sqliteio.Text(actorKey(key)))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	more, err := s.Step()
	if err != nil || !more {
		return nil, err
	}
	if s.ColumnCount() != 6 {
		return nil, failure("state_corrupt")
	}
	var values [6]string
	for i := range values {
		kind, e := s.Kind(i)
		if e != nil {
			return nil, e
		}
		if i == 1 {
			if kind != sqliteio.BlobKind {
				return nil, failure("state_corrupt")
			}
			raw, e := s.Blob(i)
			if e != nil {
				return nil, e
			}
			values[i], e = sqliteDecodeUint64(raw)
			if e != nil {
				return nil, e
			}
		} else {
			if kind != sqliteio.TextKind {
				return nil, failure("state_corrupt")
			}
			values[i], err = sqliteHostNormalizationText(s, i)
			if err != nil {
				return nil, err
			}
		}
	}
	ref := ActorRef{Key: ActorKey{ComputerID: values[2], Source: values[3], SessionID: values[4], AgentID: values[5]}, Generation: values[1]}
	if !validRef(ref) || ref.Key != key || values[0] != actorKey(key) {
		return nil, failure("state_corrupt")
	}
	if more, err = s.Step(); err != nil {
		return nil, err
	} else if more {
		return nil, failure("state_corrupt")
	}
	return &ref, nil
}

// Facts are owned typed projections, with absence and complete candidate sets
// retained for the later writer. Exact receipts precede actor/clock selection.
func sqliteReadHostFacts(tx *sqliteio.Tx, meta sqliteStoreMeta, e HostEvent) (f sqliteHostFacts, err error) {
	f.ComputerID = meta.ComputerID
	f.Historical = []sqliteHostTurnRow{}
	session, found, err := sqliteReadHostSession(tx, meta.ComputerID, e.Source, e.SessionID)
	if err != nil || !found {
		return f, err
	}
	f.Session = &session
	incarnation := session.Value.ID
	if e.Kind != "SessionStart" {
		if e.Kind == "Stop" || e.Kind == "SubagentStop" {
			f.Historical, err = sqliteHistoricalHostTurns(tx, meta.ComputerID, e)
			if err != nil {
				return f, err
			}
			if len(f.Historical) == 1 {
				row := f.Historical[0]
				f.Turn = &row
				incarnation = row.Incarnation
			}
		} else {
			row, ok, x := sqliteReadHostTurn(tx, meta.ComputerID, hostTurnKey(incarnation, e))
			if x != nil {
				return f, x
			}
			if ok {
				f.Turn = &row
			}
		}
	}
	f.ReceiptKey = hostEventKey(incarnation, e)
	receipt, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, f.ReceiptKey)
	if err != nil {
		return f, err
	}
	if found {
		f.Receipt = &receipt
		if receipt.Record.Fingerprint == hostFingerprint(e) {
			return f, nil
		}
		conflict, ok, x := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, hostConflictKey(f.ReceiptKey, hostFingerprint(e)))
		if x != nil {
			return f, x
		}
		if ok {
			f.Conflict = &conflict
			return f, nil
		}
	}
	var key *ActorKey
	if f.Turn != nil && f.Turn.Actor != nil {
		k := f.Turn.Actor.Key
		key = &k
	}
	if (e.Kind == "UserPromptSubmit" || e.Kind == "SubagentStart") && f.Turn == nil {
		k := ActorKey{ComputerID: meta.ComputerID, Source: e.Source, SessionID: session.Value.ID, AgentID: hostAgent(e.AgentID)}
		key = &k
		f.Highest, err = sqliteHostHighestGeneration(tx, k)
		if err != nil {
			return f, err
		}
	}
	if key != nil {
		f.Actor, err = sqliteCaptureReadActor(tx, meta.ComputerID, meta.Revision, *key)
		if err != nil {
			return f, err
		}
	}
	// A new child inherits only the exact actor selected by this session's root
	// turn. Keep the owned turn and current actor (including absence) for the
	// writer; the session's root key alone cannot prove that these facts stayed put.
	if e.Kind == "SubagentStart" && f.Turn == nil && session.Value.RootTurn != "" {
		root, ok, x := sqliteReadHostTurn(tx, meta.ComputerID, session.Value.RootTurn)
		if x != nil {
			return f, x
		}
		if !ok || root.AgentID != "" || root.Incarnation != session.Value.ID || root.Source != session.Value.Source || root.NativeSession != session.Value.NativeID || root.Actor == nil || root.Actor.Key.Source != root.Source {
			return f, failure("state_corrupt")
		}
		f.RootTurn = &root
		f.RootActor, err = sqliteCaptureReadActor(tx, meta.ComputerID, meta.Revision, root.Actor.Key)
		if err != nil {
			return f, err
		}
	}
	return f, nil
}
func sqliteHostExact(f sqliteHostFacts, e HostEvent) *sqliteHostReceiptRow {
	if f.Receipt != nil && f.Receipt.Record.Fingerprint == hostFingerprint(e) {
		return f.Receipt
	}
	if f.Conflict != nil && f.Conflict.Record.Fingerprint == hostFingerprint(e) {
		return f.Conflict
	}
	return nil
}
func sqliteHostSameFacts(a, b sqliteHostFacts) (bool, error) {
	if a.ComputerID != b.ComputerID || a.ReceiptKey != b.ReceiptKey || !reflect.DeepEqual(a.Session, b.Session) || !reflect.DeepEqual(a.Turn, b.Turn) || !reflect.DeepEqual(a.Historical, b.Historical) || !reflect.DeepEqual(a.Highest, b.Highest) || !reflect.DeepEqual(a.RootTurn, b.RootTurn) {
		return false, nil
	}
	same, err := sqliteCaptureSameActor(a.Actor, b.Actor)
	if err != nil || !same {
		return same, err
	}
	same, err = sqliteCaptureSameActor(a.RootActor, b.RootActor)
	if err != nil || !same {
		return same, err
	}
	for _, pair := range [][2]*sqliteHostReceiptRow{{a.Receipt, b.Receipt}, {a.Conflict, b.Conflict}} {
		if pair[0] == nil || pair[1] == nil {
			if pair[0] != nil || pair[1] != nil {
				return false, nil
			}
			continue
		}
		x, e := sqliteEncodeHostReceipt(*pair[0])
		if e != nil {
			return false, e
		}
		y, e := sqliteEncodeHostReceipt(*pair[1])
		if e != nil {
			return false, e
		}
		if !reflect.DeepEqual(x.values, y.values) {
			return false, nil
		}
	}
	return true, nil
}

// Binding-only admission also serves SessionStart; no synthetic normalized
// event is passed through the reducer or clock preparation.
func (s *Service) sqlitePrepareHostAdmission(ctx context.Context, cwd string, a sqliteCaptureAdmission) (b sqliteCaptureBinding, err error) {
	b.Needed = true
	if err = sqliteCaptureDeadline(ctx, a); err != nil {
		return b, err
	}
	if s.resolve != nil {
		snapshot, ok, x := s.resolve(ctx, Event{CWD: cwd})
		if x != nil {
			return b, failure("binding_unavailable")
		}
		if ok {
			b.Snapshot = &snapshot
		}
		return b, nil
	}
	loc, err := DiscoverLocation(ctx, cwd)
	if err != nil {
		return b, err
	}
	c, tx, meta, found, err := sqliteOpenCapture(ctx, a, sqliteio.Read)
	if err != nil {
		return b, err
	}
	if !found {
		return b, failure("state_busy")
	}
	row, lookups, readErr := sqliteCaptureLocation(tx, meta.ComputerID, loc)
	b.Locations = lookups
	if row != nil {
		snapshot := row.Snapshot
		b.Snapshot = &snapshot
	}
	return b, errors.Join(readErr, sqliteCaptureCleanup(c, tx))
}

// New host work already owns its default-resolver location selection. Recheck
// that selection together with normalized priority, target, timer and clock
// facts in one read snapshot; only the external clock runs after checked release.
func (s *Service) sqlitePrepareHostCapture(ctx context.Context, e Event, a sqliteCaptureAdmission, binding sqliteCaptureBinding) (p sqliteCapturePreparation, clock sqliteCaptureClock, found bool, err error) {
	clock.Mode = sqliteCapturePrepared
	if s.nativeCaptureClock {
		clock.Mode = sqliteCaptureNative
	}
	if err = validateEvent(e); err != nil {
		return p, clock, false, err
	}
	if e.Parent != nil {
		parent := *e.Parent
		e.Parent = &parent
	}
	c, tx, meta, found, err := sqliteOpenCapture(ctx, a, sqliteio.Read)
	if err != nil || !found {
		return p, clock, found, err
	}
	p, err = sqliteCaptureInitial(tx, meta, e)
	if err == nil && p.Binding.Needed {
		p.Binding = binding
		snapshot := *binding.Snapshot
		p.Binding.Snapshot = &snapshot
		p.Binding.Locations = append([]sqliteCaptureBindingLookup{}, binding.Locations...)
		err = sqliteCaptureCheckBinding(tx, meta, e, p.Binding)
		if err != nil {
			var domain *Error
			if errors.As(err, &domain) && domain.Code == "state_busy" {
				p.RefusalCode = "state_busy"
				err = nil
			}
		}
		if err == nil && p.RefusalCode == "" {
			_, validRevision := counter(snapshot.Revision)
			if !validUUID(snapshot.ID) || !validAttribution(snapshot.Attribution) || !validRevision {
				p.Binding.ErrorCode = "binding_unavailable"
			} else {
				p.Binding.TimerChecked = true
				p.Binding.Timer, err = sqliteCaptureTimerBindings(tx, meta.ComputerID, snapshot.Attribution)
				if err == nil {
					for _, row := range p.Binding.Timer {
						if row.Snapshot.Attribution != snapshot.Attribution {
							attribution := row.Snapshot.Attribution
							p.Binding.Conflict = &attribution
							p.Binding.ErrorCode = "attribution_conflict"
							break
						}
					}
				}
			}
		}
	}
	if err == nil && p.RefusalCode == "" && p.Branch != sqliteCaptureNoSample && (!p.Binding.Needed || p.Binding.ErrorCode == "" && p.Binding.Snapshot != nil) && clock.Mode == sqliteCapturePrepared {
		p.ClockActors, err = sqliteCaptureObserveClock(tx, meta.ComputerID, meta.Revision)
	}
	err = errors.Join(err, sqliteCaptureCleanup(c, tx))
	if err != nil {
		return p, clock, true, err
	}
	if p.RefusalCode != "" || p.Binding.Needed && (p.Binding.ErrorCode != "" || p.Binding.Snapshot == nil) {
		p.Branch = sqliteCaptureNoSample
	}
	if p.Branch == sqliteCaptureNoSample {
		return p, clock, true, nil
	}
	clock.Site = sqliteCaptureSite(p.Branch)
	if clock.Mode == sqliteCapturePrepared {
		if err = sqliteCaptureDeadline(ctx, a); err != nil {
			return p, clock, true, err
		}
		sample, sampleErr := s.sample()
		clock.Prepared = &sqliteCaptureSample{Value: sqliteCaptureCopySample(sample), Unavailable: sampleErr != nil}
	}
	return p, clock, true, nil
}

func (s *Service) sqlitePrepareHost(ctx context.Context, e HostEvent, a sqliteCaptureAdmission) (p sqliteHostPrepared, found bool, err error) {
	if err = validateHost(e); err != nil {
		return p, false, err
	}
	if !sqliteHostSupported(e) {
		return p, false, failure("unsupported_contract")
	}
	p.Clock.Mode = sqliteCapturePrepared
	if s.nativeCaptureClock {
		p.Clock.Mode = sqliteCaptureNative
	}
	c, tx, meta, found, err := sqliteOpenCapture(ctx, a, sqliteio.Read)
	if err != nil || !found {
		return p, found, err
	}
	p.BaseRevision = meta.Revision
	p.Facts, err = sqliteReadHostFacts(tx, meta, e)
	err = errors.Join(err, sqliteCaptureCleanup(c, tx))
	if err != nil {
		return p, true, err
	}
	if sqliteHostExact(p.Facts, e) != nil {
		return p, true, nil
	}
	// Ambiguity/conflict quarantine and boundary rotation are separate native
	// branches, not guessed choices in this first root/child composition slice.
	if p.Facts.Receipt != nil || len(p.Facts.Historical) > 1 {
		return p, true, failure("unsupported_contract")
	}
	if p.Facts.Session == nil && e.Kind != "SessionStart" {
		return p, true, failure("unsupported_contract")
	}
	p.CWD = e.CWD
	if p.Facts.Turn != nil {
		p.CWD = p.Facts.Turn.CWD
	}
	if p.Facts.Turn == nil || p.Facts.Turn.Actor == nil {
		p.Admission, err = s.sqlitePrepareHostAdmission(ctx, p.CWD, a)
		if err != nil || p.Admission.Snapshot == nil {
			return p, true, err
		}
	}
	if err = sqliteCaptureDeadline(ctx, a); err != nil {
		return p, true, err
	}
	policy, err := s.policies.Eligibility(ctx, e.Source, p.CWD)
	if err != nil {
		return p, true, err
	}
	p.Policy = sqliteHostPolicy{CaptureEligible: policy.CaptureEligible, Basis: policy.Basis, Revision: policy.Revision, Fingerprint: policy.Fingerprint, DiagnosticCode: policy.DiagnosticCode}
	if !policy.CaptureEligible && (p.Facts.Turn == nil || p.Facts.Turn.Actor == nil) {
		return p, true, nil
	}
	p.ReceiptID = newID()
	p.ObservedAt = time.Now().UTC()
	if p.Facts.Session == nil {
		p.NewIncarnation = newID()
	}
	switch e.Kind {
	case "UserPromptSubmit", "SubagentStart":
		if p.Facts.Turn == nil {
			generation := "1"
			if p.Facts.Highest != nil {
				if p.Facts.Highest.Generation == "18446744073709551615" {
					return p, true, failure("validation")
				}
				generation = bump(p.Facts.Highest.Generation)
			}
			key := ActorKey{ComputerID: meta.ComputerID, Source: e.Source, SessionID: p.Facts.Session.Value.ID, AgentID: hostAgent(e.AgentID)}
			p.Normalized = &Event{ContractVersion: 1, Actor: key, Generation: generation, Sequence: "1", EventID: p.ReceiptID, Kind: "work", CWD: p.CWD}
			if e.Kind == "SubagentStart" && p.Facts.RootTurn != nil && p.Facts.RootTurn.Actor != nil && p.Facts.RootActor != nil && p.Facts.RootActor.Ref == *p.Facts.RootTurn.Actor {
				parent := p.Facts.RootActor.Ref
				p.Normalized.Parent = &parent
			}
		}
	case "Stop", "SubagentStop":
		turn, actor := p.Facts.Turn, p.Facts.Actor
		if turn != nil && turn.Actor != nil && actor != nil && actor.Ref == *turn.Actor && !turn.Stopped && !sqliteCaptureTerminal(*actor) && !e.StopHookActive {
			if actor.Sequence == "18446744073709551615" {
				return p, true, failure("validation")
			}
			p.Normalized = &Event{ContractVersion: 1, Actor: actor.Ref.Key, Generation: actor.Ref.Generation, Sequence: bump(actor.Sequence), EventID: p.ReceiptID, Kind: "wait_user"}
		}
	}
	if p.Normalized != nil {
		var exists bool
		lookups := p.Admission.Locations
		if s.resolve == nil && (e.Kind == "UserPromptSubmit" || e.Kind == "SubagentStart") && p.Facts.Turn == nil && p.Admission.Needed && p.Admission.Snapshot != nil && len(lookups) > 0 && lookups[len(lookups)-1].Row != nil && lookups[len(lookups)-1].Row.Snapshot == *p.Admission.Snapshot {
			p.Capture, p.Clock, exists, err = s.sqlitePrepareHostCapture(ctx, *p.Normalized, a, p.Admission)
		} else {
			p.Capture, p.Clock, exists, err = s.sqlitePrepareCapture(ctx, *p.Normalized, a)
		}
		if err != nil {
			return p, true, err
		}
		if !exists {
			return p, true, failure("state_busy")
		}
	} else if p.Facts.Turn != nil && p.Facts.Turn.Actor != nil && (!p.Policy.CaptureEligible || e.StopHookActive) {
		// These supported stop safety branches have no normalized proposal, but
		// still use one clock with every selected observation outside SQL ownership.
		c, tx, meta, exists, x := sqliteOpenCapture(ctx, a, sqliteio.Read)
		if x != nil {
			return p, true, x
		}
		if !exists {
			return p, true, failure("state_busy")
		}
		p.Capture.ClockActors, err = sqliteCaptureObserveClock(tx, meta.ComputerID, meta.Revision)
		err = errors.Join(err, sqliteCaptureCleanup(c, tx))
		if err != nil {
			return p, true, err
		}
		p.Clock.Site = sqliteCaptureClockReduce
		if p.Clock.Mode == sqliteCapturePrepared {
			sample, x := s.sample()
			p.Clock.Prepared = &sqliteCaptureSample{Value: sqliteCaptureCopySample(sample), Unavailable: x != nil}
		}
	}
	// The writer rechecks these owned facts after every external callback, with
	// exact committed receipt priority and before capacity or any domain write.
	return p, true, nil
}
