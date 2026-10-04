//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"sort"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// A review has one concrete native sample site, or none for a waiting/absent
// actor. Injected clocks are evaluated only after the preparation read closes.
type sqliteHostReviewPlan struct {
	Ref    *ActorRef
	Reason string
	Retain bool
	Clock  sqliteCaptureClock
}

func sqliteHostExists(tx *sqliteio.Tx, query string, values ...sqliteio.Value) (found bool, err error) {
	s, err := tx.Prepare(query, values...)
	if err != nil {
		return false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			found = false
		}
	}()
	found, err = s.Step()
	if err != nil || !found {
		return false, err
	}
	if s.ColumnCount() != 1 {
		return false, failure("state_corrupt")
	}
	k, err := s.Kind(0)
	if err != nil {
		return false, err
	}
	if k != sqliteio.TextKind {
		return false, failure("state_corrupt")
	}
	key, err := s.Text(0)
	if err != nil {
		return false, err
	}
	if len(key) != 64 {
		return false, failure("state_corrupt")
	}
	if more, x := s.Step(); x != nil {
		return false, x
	} else if more {
		return false, failure("state_corrupt")
	}
	return true, nil
}

func sqliteHostLiveHeads(tx *sqliteio.Tx, meta sqliteStoreMeta, incarnation string) (rows []sqliteCaptureClockActor, err error) {
	s, err := tx.Prepare("SELECT actor_key,generation FROM actors WHERE computer_id=? AND state NOT IN ('finished','interrupted')", sqliteio.Text(meta.ComputerID))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			rows = nil
		}
	}()
	refs := []ActorRef{}
	for {
		more, x := s.Step()
		if x != nil {
			return nil, x
		}
		if !more {
			break
		}
		if s.ColumnCount() != 2 {
			return nil, failure("state_corrupt")
		}
		ref, x := sqliteDependencyStoredActor(tx, s, meta.ComputerID)
		if x != nil {
			return nil, x
		}
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return actorKey(refs[i].Key) < actorKey(refs[j].Key) })
	rows = []sqliteCaptureClockActor{}
	for _, ref := range refs {
		a, x := sqliteCaptureReadActor(tx, meta.ComputerID, meta.Revision, ref.Key)
		if x != nil {
			return nil, x
		}
		if a == nil || a.Ref != ref || sqliteCaptureTerminal(*a) {
			return nil, failure("state_corrupt")
		}
		if a.Ref.Key.SessionID != incarnation {
			continue
		}
		row, x := sqliteCaptureObserveActor(tx, meta.ComputerID, meta.Revision, *a)
		if x != nil {
			return nil, x
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func sqliteHostExtended(e HostEvent, f sqliteHostFacts) bool {
	if f.Boundary || f.Ambiguous || len(f.Historical) > 1 || f.Receipt != nil && f.Receipt.Record.Fingerprint != hostFingerprint(e) {
		return true
	}
	switch e.Kind {
	case "UserPromptSubmit", "SubagentStart":
		return f.Turn != nil
	case "PreToolUse", "PostToolUse", "PostToolUseFailure", "PermissionRequest", "Interrupt", "StopFailure", "SessionEnd", "TaskCreated", "TaskCompleted":
		return true
	case "Stop", "SubagentStop":
		return f.Observed != nil && sqliteCapturePendingWait(*f.Observed)
	}
	return false
}

func sqliteHostObserved(f sqliteHostFacts, ref ActorRef) *sqliteCaptureClockActor {
	if f.Observed != nil && f.Observed.Actor.Ref == ref {
		return f.Observed
	}
	for i := range f.Candidates {
		if f.Candidates[i].Actor.Ref == ref {
			return &f.Candidates[i]
		}
	}
	return nil
}

func sqliteHostPending(source string, pending map[string]hostTool) bool {
	if len(pending) == 0 {
		return false
	}
	for _, tool := range pending {
		if !hostWaitTool(source, tool.Name) {
			return false
		}
	}
	return true
}

func (s *Service) sqlitePrepareHostEffects(ctx context.Context, e HostEvent, a sqliteCaptureAdmission, p *sqliteHostPrepared) error {
	f := p.Facts
	mode := sqliteCapturePrepared
	if s.nativeCaptureClock {
		mode = sqliteCaptureNative
	}
	addReview := func(ref *ActorRef, reason string, retain bool) {
		r := sqliteHostReviewPlan{Reason: reason, Retain: retain, Clock: sqliteCaptureClock{Mode: mode}}
		if ref != nil {
			v := *ref
			r.Ref = &v
			if row := sqliteHostObserved(f, v); row != nil && !sqliteCaptureTerminal(row.Actor) && !sqliteCapturePendingWait(*row) {
				r.Clock.Site = sqliteCaptureClockReduce
			}
		}
		p.Reviews = append(p.Reviews, r)
	}
	setEvent := func(kind string) error {
		if f.Actor == nil || f.Turn == nil || f.Turn.Actor == nil || f.Actor.Ref != *f.Turn.Actor {
			return failure("state_corrupt")
		}
		if f.Actor.Sequence == "18446744073709551615" {
			return failure("validation")
		}
		p.Normalized = &Event{ContractVersion: 1, Actor: f.Actor.Ref.Key, Generation: f.Actor.Ref.Generation, Sequence: bump(f.Actor.Sequence), EventID: p.ReceiptID, Kind: kind}
		return nil
	}
	if f.Boundary {
		p.Clock = sqliteCaptureClock{Mode: mode, Site: sqliteCaptureClockReduce}
	} else if f.Receipt != nil && f.Receipt.Record.Fingerprint != hostFingerprint(e) {
		p.Early = true
		addReview(f.Receipt.Record.Result.Actor, "ordering_unavailable", true)
	} else if f.Ambiguous || e.Kind == "TaskCreated" || e.Kind == "TaskCompleted" {
		p.Early = true
	} else {
		if !p.Policy.CaptureEligible && f.Turn != nil && f.Turn.Actor != nil {
			addReview(f.Turn.Actor, "ordering_unavailable", true)
		}
		if len(f.Historical) > 1 {
			for _, turn := range f.Historical {
				addReview(turn.Actor, "ordering_unavailable", true)
			}
			p.Early = true
		} else {
			switch e.Kind {
			case "UserPromptSubmit", "SubagentStart":
				// Existing turns preserve their recorded generation. Policy review
				// above supplies only the clock/provenance that this target warrants.
				p.Stale = true
			case "PreToolUse", "PostToolUse", "PostToolUseFailure":
				if f.Turn == nil || f.Turn.Actor == nil {
					p.Missing = true
					break
				}
				if f.Actor == nil || f.Actor.Ref != *f.Turn.Actor || sqliteCaptureTerminal(*f.Actor) {
					p.Stale = true
					break
				}
				if f.Turn.Stopped {
					p.Stale = true
					if e.Source == "claude" {
						p.FenceCode = "source_loss_while_waiting"
					}
					break
				}
				p.WasWaiting = f.Observed != nil && sqliteCapturePendingWait(*f.Observed)
				if f.Actor.State != "working" && !p.WasWaiting {
					p.Stale = true
					break
				}
				if f.Tool != nil && f.Tool.Value.Name != e.ToolName {
					addReview(f.Turn.Actor, "ordering_unavailable", false)
					break
				}
				if e.Kind == "PreToolUse" && f.Tool != nil && f.Tool.Value.Phase != "pre" {
					p.Stale = true
					break
				}
				if e.Kind != "PreToolUse" && f.Tool == nil {
					addReview(f.Turn.Actor, "ordering_unavailable", false)
				}
				phase := "pre"
				if e.Kind == "PostToolUse" {
					phase = "post"
				} else if e.Kind == "PostToolUseFailure" {
					phase = "failed"
				}
				if e.Source == "claude" && f.Tool != nil && f.Tool.Value.Phase != "pre" && phase != f.Tool.Value.Phase {
					addReview(f.Turn.Actor, "ordering_unavailable", false)
					break
				}
				p.ToolAfter = &sqliteHostToolRow{TurnKey: f.Turn.Key, ID: e.ToolID, Value: hostTool{Name: e.ToolName, Phase: phase}}
				if _, err := sqliteEncodeHostTool(f.ComputerID, *p.ToolAfter); err != nil {
					return err
				}
				pending := map[string]hostTool{}
				for k, v := range f.Pending {
					pending[k] = v
				}
				delete(pending, e.ToolID)
				if phase == "pre" {
					pending[e.ToolID] = p.ToolAfter.Value
				}
				blocked := f.Actor.Health != "continuous"
				for _, r := range p.Reviews {
					if r.Ref != nil && *r.Ref == f.Actor.Ref && (f.Actor.State == "working" || p.WasWaiting) {
						blocked = true
					}
				}
				if !blocked {
					kind := "observe_work"
					if sqliteHostPending(e.Source, pending) {
						kind = hostWaitState(e.Source)
					} else if p.WasWaiting {
						kind = "work"
					}
					if err := setEvent(kind); err != nil {
						return err
					}
				}
			case "PermissionRequest":
				if f.Turn != nil {
					addReview(f.Turn.Actor, "ordering_unavailable", false)
				} else {
					p.Missing = true
				}
			case "Stop", "SubagentStop":
				p.Stale = true
				if f.Turn != nil && f.Turn.Actor != nil && f.Actor != nil && f.Actor.Ref == *f.Turn.Actor && !sqliteCaptureTerminal(*f.Actor) && !f.Turn.Stopped {
					p.Stale = false
					if e.StopHookActive {
						addReview(f.Turn.Actor, "ordering_unavailable", false)
					} else {
						if f.Observed != nil && sqliteCapturePendingWait(*f.Observed) && sqliteHostPending(e.Source, f.Pending) {
							p.FenceCode = "incomplete_wait"
						}
						p.StopTurn = true
						if err := setEvent("wait_user"); err != nil {
							return err
						}
					}
				}
			case "Interrupt", "StopFailure", "SessionEnd":
				p.Stale = true
				if f.Turn != nil && f.Turn.Actor != nil && f.Actor != nil && f.Actor.Ref == *f.Turn.Actor && !sqliteCaptureTerminal(*f.Actor) && (!f.Turn.Stopped || e.Source == "claude" && e.Kind == "SessionEnd" && f.Actor.State == "wait_user") {
					p.Stale = false
					if !(e.Source == "claude" && e.Kind == "SessionEnd" && f.Actor.State == "wait_user" && f.Observed != nil && !sqliteCapturePendingWait(*f.Observed)) {
						addReview(f.Turn.Actor, "source_lost", false)
					}
					p.StopTurn = true
					if err := setEvent("interrupt"); err != nil {
						return err
					}
				}
			}
		}
	}
	// Normalize preparation and all clock candidates in one actual read snapshot.
	needClock := p.Clock.Site != sqliteCaptureClockNone
	for _, r := range p.Reviews {
		needClock = needClock || r.Clock.Site != sqliteCaptureClockNone
	}
	if p.Normalized == nil && !needClock {
		return nil
	}
	c, tx, meta, found, err := sqliteOpenCapture(ctx, a, sqliteio.Read)
	if err != nil {
		return err
	}
	if !found {
		return failure("state_busy")
	}
	if p.Normalized != nil {
		p.Capture, err = sqliteCaptureInitial(tx, meta, *p.Normalized)
		if err == nil && p.Capture.Branch != sqliteCaptureNoSample {
			p.Clock = sqliteCaptureClock{Mode: mode, Site: sqliteCaptureSite(p.Capture.Branch)}
			needClock = true
		}
	}
	if err == nil && needClock && mode == sqliteCapturePrepared {
		p.ClockFacts, err = sqliteCaptureObserveClock(tx, meta.ComputerID, meta.Revision)
		p.Capture.ClockActors = p.ClockFacts
	}
	err = errors.Join(err, sqliteCaptureCleanup(c, tx))
	if err != nil {
		return err
	}
	sample := func(clock *sqliteCaptureClock) error {
		if clock.Site == sqliteCaptureClockNone || clock.Mode != sqliteCapturePrepared {
			return nil
		}
		if err := sqliteCaptureDeadline(ctx, a); err != nil {
			return err
		}
		value, e := s.sample()
		clock.Prepared = &sqliteCaptureSample{Value: sqliteCaptureCopySample(value), Unavailable: e != nil}
		return nil
	}
	// Concrete legacy order: boundary OR target reviews, then normalized reducer.
	for i := range p.Reviews {
		if err := sample(&p.Reviews[i].Clock); err != nil {
			return err
		}
	}
	return sample(&p.Clock)
}

func sqliteHostTake(clock *sqliteCaptureClock) (sqliteCaptureSample, error) {
	value, err := clock.take(clock.Site)
	if err != nil {
		var domain *Error
		if !errors.As(err, &domain) || domain.Code != "clock_unavailable" {
			return sqliteCaptureSample{}, err
		}
	}
	return sqliteCaptureSample{Value: value, Unavailable: err != nil}, nil
}

// Initial candidates were validated at meta.Revision before the first write.
// Subsequent review sites read actual staged rows at the new ceiling.
func sqliteHostClockSafety(m *sqliteCaptureMutation, sample ClockSample) (bool, error) {
	rows, err := sqliteCaptureObserveClock(m.tx, m.meta.ComputerID, m.nextRevision)
	if err != nil {
		return false, err
	}
	changed := false
	for _, row := range rows {
		why := discontinuity(row.Actor.LastEvidence, sample)
		if why == "" && row.Epoch != nil {
			why = discontinuity(row.Epoch.Value.Anchor, sample)
		}
		if why != "" {
			did, err := m.quarantine(row.Actor, why, "ClockObservation", sample)
			if err != nil {
				return false, err
			}
			changed = changed || did
		}
	}
	return changed, nil
}

func sqliteHostFence(m *sqliteCaptureMutation, a sqliteActorLocalRow, r *HostReceipt, code string) (bool, error) {
	ref := a.Ref
	r.Actor = &ref
	r.Disposition = "review_required"
	r.Ordering = "review_required"
	r.DiagnosticCode = code
	if a.Health != "continuous" {
		return false, nil
	}
	next := a
	next.Health = "stale"
	next.Revision = bump(a.Revision)
	if err := m.writeActor(&a, next); err != nil {
		return false, err
	}
	return true, nil
}

func sqliteHostReview(m *sqliteCaptureMutation, plan *sqliteHostReviewPlan, e HostEvent, r *HostReceipt) (bool, error) {
	r.Disposition = "review_required"
	r.Ordering = "review_required"
	r.DiagnosticCode = plan.Reason
	r.Actor = plan.Ref
	if plan.Ref == nil {
		return false, nil
	}
	a, err := m.readActor(plan.Ref.Key)
	if err != nil {
		return false, err
	}
	if a == nil || a.Ref != *plan.Ref || sqliteCaptureTerminal(*a) {
		return false, nil
	}
	observed, err := sqliteCaptureObserveActor(m.tx, m.meta.ComputerID, m.nextRevision, *a)
	if err != nil {
		return false, err
	}
	changed := false
	if sqliteCapturePendingWait(observed) {
		if plan.Retain {
			changed, err = m.quarantine(*a, plan.Reason, e.Kind, ClockSample{WallUTC: r.ObservedAt})
			if err != nil {
				return false, err
			}
			a, err = m.readActor(plan.Ref.Key)
			if err != nil {
				return false, err
			}
			if a == nil {
				return false, failure("state_corrupt")
			}
		}
		did, err := sqliteHostFence(m, *a, r, "source_loss_while_waiting")
		return changed || did, err
	}
	sample, err := sqliteHostTake(&plan.Clock)
	if err != nil {
		return false, err
	}
	changed, err = m.quarantine(*a, plan.Reason, e.Kind, sample.Value)
	if err != nil {
		return false, err
	}
	did, err := sqliteHostClockSafety(m, sample.Value)
	r.ObservedAt = sample.Value.WallUTC
	return changed || did, err
}

func sqliteHostSave(m *sqliteCaptureMutation, e HostEvent, p sqliteHostPrepared, f sqliteHostFacts, r HostReceipt, code string, key string) (sqliteHostTransition, error) {
	if key == "" {
		key = f.ReceiptKey
	}
	if key == "" {
		return sqliteHostTransition{}, failure("state_corrupt")
	}
	row := sqliteHostReceiptRow{Key: key, Record: hostReceiptRecord{Fingerprint: hostFingerprint(e), Result: r, ErrorCode: code}}
	var delta int64
	var err error
	if f.Boundary && key == f.ReceiptKey && f.Receipt != nil {
		delta, err = sqliteUpdateHostReceipt(m.tx, m.meta.ComputerID, m.nextRevision, *f.Receipt, row)
	} else {
		delta, err = sqliteInsertHostReceipt(m.tx, m.meta.ComputerID, m.nextRevision, row)
	}
	if err = m.add(delta, err); err != nil {
		return sqliteHostTransition{}, err
	}
	m.transition.Dependencies.HostReceiptKeys = append(m.transition.Dependencies.HostReceiptKeys, key)
	m.transition.Dependencies.HostSessions = append(m.transition.Dependencies.HostSessions, sqliteHostSessionIdentity{Source: e.Source, NativeSession: e.SessionID})
	if f.Turn != nil {
		m.transition.Dependencies.HostTurnKeys = append(m.transition.Dependencies.HostTurnKeys, f.Turn.Key)
	}
	var op *Error
	if code != "" {
		op = failure(code)
	}
	return sqliteHostTransition{Result: r, Changed: true, OperationError: op, Delta: m.transition.Delta, Dependencies: m.transition.Dependencies, Finalization: m.transition.Finalization}, nil
}

func sqliteReduceHostEffects(tx *sqliteio.Tx, meta sqliteStoreMeta, e HostEvent, p sqliteHostPrepared, f sqliteHostFacts) (sqliteHostTransition, error) {
	next := meta.Revision
	if next != "18446744073709551615" {
		next = bump(next)
	}
	m := sqliteCaptureMutation{tx: tx, meta: meta, nextRevision: next}
	m.transition.Finalization = sqliteEmptyFinalizationSelection()
	r := hostBase(e, next)
	r.ID = p.ReceiptID
	r.Disposition = "applied"
	r.Ordering = "supported"
	r.Durability = "committed"
	r.ObservedAt = p.ObservedAt
	r.ProfileBasis = p.Policy.Basis
	r.ProfileRevision = p.Policy.Revision
	r.Fingerprint = p.Policy.Fingerprint
	if f.Turn != nil && f.Turn.Actor != nil {
		ref := *f.Turn.Actor
		r.Actor = &ref
	}
	var early sqliteEventTransition
	ready := false
	var err error
	if p.Normalized != nil {
		early, ready, err = sqliteCheckPreparedEvent(tx, meta, *p.Normalized, p.Capture, &p.Clock)
		if err != nil {
			return sqliteHostTransition{}, err
		}
	}
	needClock := p.Clock.Site != sqliteCaptureClockNone
	for _, review := range p.Reviews {
		needClock = needClock || review.Clock.Site != sqliteCaptureClockNone
	}
	if needClock {
		rows, x := sqliteCaptureObserveClock(tx, meta.ComputerID, meta.Revision)
		if x != nil {
			return sqliteHostTransition{}, x
		}
		if p.Clock.Mode == sqliteCapturePrepared {
			same, x := sqliteCaptureSameClock(rows, p.ClockFacts)
			if x != nil {
				return sqliteHostTransition{}, x
			}
			if !same {
				return sqliteHostTransition{}, failure("state_busy")
			}
		}
	}
	if f.Boundary {
		return sqliteHostBoundary(&m, e, p, f, r)
	}
	changed := false
	for i := range p.Reviews {
		did, x := sqliteHostReview(&m, &p.Reviews[i], e, &r)
		if x != nil {
			return sqliteHostTransition{}, x
		}
		changed = changed || did
	}
	if f.Receipt != nil && f.Receipt.Record.Fingerprint != hostFingerprint(e) {
		r.DiagnosticCode = "event_conflict"
		return sqliteHostSave(&m, e, p, f, r, "event_conflict", hostConflictKey(f.ReceiptKey, hostFingerprint(e)))
	}
	if p.Early {
		if f.Ambiguous || e.Kind == "TaskCreated" || e.Kind == "TaskCompleted" {
			if r.Actor == nil {
				r.Disposition = "review_required"
				r.Ordering = "review_required"
				r.DiagnosticCode = "ordering_unavailable"
			} else if !p.Policy.CaptureEligible {
				r.Disposition = "review_required"
				r.Ordering = "review_required"
				r.DiagnosticCode = p.Policy.DiagnosticCode
			}
		} else {
			r.Actor = nil
			if e.Kind == "PreToolUse" || e.Kind == "PostToolUse" || e.Kind == "PostToolUseFailure" || e.Kind == "PermissionRequest" {
				r.Disposition = "review_required"
				r.Ordering = "review_required"
				r.DiagnosticCode = "ordering_unavailable"
			}
		}
		return sqliteHostSave(&m, e, p, f, r, "", "")
	}
	if !p.Policy.CaptureEligible && f.Turn != nil && f.Turn.Actor != nil {
		r.DiagnosticCode = p.Policy.DiagnosticCode
	}
	if p.Stale {
		r.Disposition = "stale"
	}
	if p.Missing {
		r.Disposition = "review_required"
		r.Ordering = "review_required"
		r.DiagnosticCode = "ordering_unavailable"
	}
	if p.FenceCode != "" && f.Turn != nil && f.Turn.Actor != nil {
		a, x := m.readActor(f.Turn.Actor.Key)
		if x != nil {
			return sqliteHostTransition{}, x
		}
		if a != nil && a.Ref == *f.Turn.Actor {
			did, x := sqliteHostFence(&m, *a, &r, p.FenceCode)
			if x != nil {
				return sqliteHostTransition{}, x
			}
			changed = changed || did
		}
	}
	// Ordinary tool phases become visible before reduction, except Claude pending
	// questions whose PRE evidence must survive the resume continuity guard.
	delayedTool := p.ToolAfter != nil && e.Source == "claude" && p.WasWaiting
	writeTool := func() error {
		n, x := sqliteWriteHostTool(tx, meta.ComputerID, f.Tool, *p.ToolAfter)
		if x = m.add(n, x); x != nil {
			return x
		}
		m.transition.Dependencies.HostTools = append(m.transition.Dependencies.HostTools, sqliteHostToolIdentity{TurnKey: p.ToolAfter.TurnKey, ToolID: p.ToolAfter.ID})
		return nil
	}
	if p.ToolAfter != nil && !delayedTool {
		if err := writeTool(); err != nil {
			return sqliteHostTransition{}, err
		}
	}
	// A native Interrupt is a terminal boundary even though its work tail
	// stays uncertain. Pending-wait loss keeps its distinct capture review.
	if e.Kind == "Interrupt" && p.Normalized != nil && !captureReview(r) {
		r.Disposition = "applied"
		r.Ordering = "supported"
	}
	code := ""
	if p.Normalized != nil {
		n := early
		if ready {
			sample, x := sqliteHostTake(&p.Clock)
			if x != nil {
				return sqliteHostTransition{}, x
			}
			did, x := sqliteHostClockSafety(&m, sample.Value)
			if x != nil {
				return sqliteHostTransition{}, x
			}
			changed = changed || did
			n, x = m.applyPreparedEvent(*p.Normalized, p.Capture.Binding, sample, e.Source == "claude" && p.WasWaiting && p.Normalized.Kind == "work", changed)
			if x != nil {
				return sqliteHostTransition{}, x
			}
		}
		if n.OperationError != nil {
			if f.Turn != nil && f.Turn.Actor != nil {
				a, x := m.readActor(f.Turn.Actor.Key)
				if x != nil {
					return sqliteHostTransition{}, x
				}
				if a != nil && a.Ref == *f.Turn.Actor {
					observed, x := sqliteCaptureObserveActor(tx, meta.ComputerID, next, *a)
					if x != nil {
						return sqliteHostTransition{}, x
					}
					if p.WasWaiting || sqliteCapturePendingWait(observed) {
						did, x := sqliteHostFence(&m, *a, &r, "source_loss_while_waiting")
						if x != nil {
							return sqliteHostTransition{}, x
						}
						changed = changed || did
					}
				}
			}
			if !n.Changed && !changed {
				return sqliteHostTransition{}, n.OperationError
			}
			code = n.OperationError.Code
			r.Disposition = "review_required"
			r.Ordering = "review_required"
			if !captureReview(r) {
				r.DiagnosticCode = code
			}
		} else if r.Disposition != "review_required" {
			r.Disposition = n.Result.Disposition
		}
		a, x := m.readActor(p.Normalized.Actor)
		if x != nil {
			return sqliteHostTransition{}, x
		}
		if a != nil {
			r.ObservedAt = a.LastEvidence.WallUTC
		}
	}
	if delayedTool {
		if err := writeTool(); err != nil {
			return sqliteHostTransition{}, err
		}
	}
	if p.StopTurn && f.Turn != nil {
		after := *f.Turn
		after.Stopped = true
		n, x := sqliteWriteHostTurn(tx, meta.ComputerID, f.Turn, after)
		if x = m.add(n, x); x != nil {
			return sqliteHostTransition{}, x
		}
		m.transition.Dependencies.ChangedHostTurnKeys = append(m.transition.Dependencies.ChangedHostTurnKeys, after.Key)
	}
	if (e.Kind == "Interrupt" || e.Kind == "StopFailure") && f.Turn == nil {
		row := sqliteHostTurnRow{Key: hostTurnKey(f.Session.Value.ID, e), Source: e.Source, NativeSession: e.SessionID, Incarnation: f.Session.Value.ID, TurnID: e.TurnID, AgentID: e.AgentID, CWD: p.CWD, Stopped: true}
		n, x := sqliteWriteHostTurn(tx, meta.ComputerID, nil, row)
		if x = m.add(n, x); x != nil {
			return sqliteHostTransition{}, x
		}
		m.transition.Dependencies.ChangedHostTurnKeys = append(m.transition.Dependencies.ChangedHostTurnKeys, row.Key)
	}
	return sqliteHostSave(&m, e, p, f, r, code, "")
}

func sqliteHostBoundary(m *sqliteCaptureMutation, e HostEvent, p sqliteHostPrepared, f sqliteHostFacts, r HostReceipt) (sqliteHostTransition, error) {
	sample, err := sqliteHostTake(&p.Clock)
	if err != nil {
		return sqliteHostTransition{}, err
	}
	if _, err = sqliteHostClockSafety(m, sample.Value); err != nil {
		return sqliteHostTransition{}, err
	}
	if sample.Unavailable {
		r.Disposition = "review_required"
		r.Ordering = "review_required"
		r.DiagnosticCode = "clock_unavailable"
		if f.RootTurn != nil && f.RootTurn.Actor != nil {
			ref := *f.RootTurn.Actor
			r.Actor = &ref
		}
		return sqliteHostSave(m, e, p, f, r, "clock_unavailable", f.ReceiptKey)
	}
	for _, before := range f.Live {
		a, err := m.readActor(before.Actor.Ref.Key)
		if err != nil {
			return sqliteHostTransition{}, err
		}
		if a == nil || a.Ref != before.Actor.Ref {
			return sqliteHostTransition{}, failure("state_corrupt")
		}
		if _, err = m.quarantine(*a, "restart_unknown", "SessionStart", sample.Value); err != nil {
			return sqliteHostTransition{}, err
		}
		a, err = m.readActor(a.Ref.Key)
		if err != nil {
			return sqliteHostTransition{}, err
		}
		if a == nil {
			return sqliteHostTransition{}, failure("state_corrupt")
		}
		if a.Ref.Key.AgentID == "root" {
			op, fatal := m.cap(*a, sample.Value)
			if fatal != nil {
				return sqliteHostTransition{}, fatal
			}
			if op != nil {
				return sqliteHostTransition{}, op
			}
			next := *a
			next.State = "interrupted"
			next.SegmentID = nil
			next.Revision = bump(a.Revision)
			if err = m.writeActor(a, next); err != nil {
				return sqliteHostTransition{}, err
			}
		}
	}
	session := *f.Session
	session.Value = hostSession{ID: p.NewIncarnation, Source: e.Source, NativeID: e.SessionID, CWD: p.CWD}
	n, err := sqliteWriteHostSession(m.tx, m.meta.ComputerID, f.Session, session)
	if err = m.add(n, err); err != nil {
		return sqliteHostTransition{}, err
	}
	return sqliteHostSave(m, e, p, f, r, "", hostEventKey(session.Value.ID, e))
}
