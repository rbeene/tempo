//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteHostTransition struct {
	Result         HostReceipt
	Changed        bool
	OperationError *Error
	Delta          int64
	Dependencies   sqliteDependencySelection
	Finalization   sqliteFinalizationSelection
}

func sqliteHostUnknown(cause error) error {
	return errors.Join(failure("local_write_unknown"), sqliteCaptureError(cause))
}

// SQLite host ingress shares one capture transaction with native normalization.
func (s *Service) ingestHostSQLite(ctx context.Context, e HostEvent) (HostReceipt, error) {
	result := hostBase(e, "0")
	if err := validateHost(e); err != nil {
		return result, err
	}
	if !sqliteHostSupported(e) {
		return result, failure("unsupported_contract")
	}
	directory, authority, database, err := sqliteLocation(s.store.path)
	if err != nil {
		return result, err
	}
	timeout := s.store.timeout
	if timeout == 0 {
		timeout = 250 * time.Millisecond
	}
	if timeout < 0 || timeout > time.Second {
		return result, failure("validation")
	}
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	hostCaptureTraceDeadline(ctx, deadline)
	admission := sqliteCaptureAdmission{Directory: directory, StateBasename: authority, DatabaseBasename: database, AcquireDeadline: deadline}
	prepared, found, err := s.sqlitePrepareHost(ctx, e, admission)
	historical := sqliteHostExact(prepared.Facts, e) != nil
	if err != nil {
		if historical {
			result.Durability = "unknown"
			return result, sqliteHostUnknown(err)
		}
		return result, sqliteCaptureError(err)
	}
	if !found {
		return result, nil
	}
	result.SnapshotRevision = prepared.BaseRevision
	if !historical && prepared.ReceiptID == "" {
		if prepared.Admission.Snapshot != nil && !prepared.Policy.CaptureEligible {
			result.Disposition = "review_required"
			result.ProfileBasis = prepared.Policy.Basis
			result.ProfileRevision = prepared.Policy.Revision
			result.Fingerprint = prepared.Policy.Fingerprint
			result.DiagnosticCode = prepared.Policy.DiagnosticCode
		}
		return result, nil
	}
	admission.AcquireDeadline = prepared.AcquireDeadline
	hostCaptureTraceMark(ctx, hostTraceWriterBegin)
	c, tx, meta, found, err := sqliteOpenCapture(ctx, admission, sqliteio.Write)
	hostCaptureTraceMark(ctx, hostTraceWriterEnd)
	if err != nil || !found {
		if err == nil {
			err = failure("state_busy")
		}
		if historical {
			result.Durability = "unknown"
			return result, sqliteHostUnknown(err)
		}
		return result, sqliteCaptureError(err)
	}
	fail := func(cause error, uncertain bool) (HostReceipt, error) {
		cleanup := sqliteCaptureCleanup(c, tx)
		if uncertain || historical || cleanup != nil {
			result.Durability = "unknown"
			return result, sqliteHostUnknown(errors.Join(cause, cleanup))
		}
		result.Durability = "not_committed"
		return result, sqliteCaptureError(cause)
	}
	if meta.ComputerID != prepared.Facts.ComputerID {
		return fail(failure("state_busy"), false)
	}
	facts, err := sqliteReadHostFacts(tx, meta, e)
	if err != nil {
		return fail(err, false)
	}
	var transition sqliteHostTransition
	if prior := sqliteHostExact(facts, e); prior != nil {
		historical = true
		transition.Result = prior.Record.Result
		transition.Result.Disposition = "duplicate"
		transition.Finalization = sqliteEmptyFinalizationSelection()
		if prior.Record.ErrorCode != "" {
			transition.OperationError = failure(prior.Record.ErrorCode)
		}
	}
	// This replaces the separate postcallback read. Keep the original host-fact
	// refusal ahead of pressure, while a committed winner still takes priority.
	if !historical {
		same, x := sqliteHostSamePreparedFacts(e, facts, prepared.Facts)
		if x != nil {
			return fail(x, false)
		}
		if !same {
			return fail(failure("state_busy"), false)
		}
	}
	// Existing receipts establish historical uncertainty before pressure can
	// refuse the required nonce fence. No new domain write precedes this check.
	if err = sqliteCaptureCapacity(tx, meta); err != nil {
		return fail(err, false)
	}
	if !historical {
		transition, err = sqliteReduceHostFacts(tx, meta, e, prepared, facts)
		if err != nil {
			return fail(err, false)
		}
	} else if transition.Result.ID == "" {
		return fail(failure("state_busy"), false)
	}
	result = transition.Result
	if !transition.Changed && transition.Delta != 0 {
		return fail(failure("state_corrupt"), false)
	}
	after := meta
	if transition.Changed {
		if meta.Revision == "18446744073709551615" {
			return fail(failure("validation"), false)
		}
		after.Revision = bump(meta.Revision)
		if err = sqliteValidateSelectedCaptureDependencies(tx, meta.ComputerID, after.Revision, transition.Dependencies); err != nil {
			return fail(err, false)
		}
		if err = sqliteValidateSelectedFinalization(tx, meta.ComputerID, after.Revision, transition.Finalization); err != nil {
			return fail(err, false)
		}
	}
	if err = sqliteFinalizationAdd(&after.LogicalBytes, transition.Delta, nil); err != nil {
		return fail(err, false)
	}
	after.DurabilityNonce, err = sqliteNextNonce(meta.DurabilityNonce[:])
	if err != nil {
		return fail(err, false)
	}
	if err = sqliteUpdateMeta(tx, meta, after); err != nil {
		return fail(err, false)
	}
	outcome, err := tx.Commit()
	if err != nil || outcome != sqliteio.Committed {
		if err == nil {
			err = failure("state_corrupt")
		}
		return fail(err, outcome == sqliteio.Unknown || outcome == sqliteio.Committed)
	}
	if err = c.CloseDurably(ctx); err != nil {
		result.Durability = "unknown"
		return result, sqliteHostUnknown(errors.Join(err, sqliteCaptureClose(c)))
	}
	if transition.OperationError != nil {
		return result, transition.OperationError
	}
	return result, nil
}

func sqliteReduceHost(tx *sqliteio.Tx, meta sqliteStoreMeta, e HostEvent, p sqliteHostPrepared) (result sqliteHostTransition, err error) {
	if err = validateHost(e); err != nil {
		return result, err
	}
	if !sqliteHostSupported(e) {
		return result, failure("unsupported_contract")
	}
	facts, err := sqliteReadHostFacts(tx, meta, e)
	if err != nil {
		return result, err
	}
	return sqliteReduceHostFacts(tx, meta, e, p, facts)
}

// The coordinator supplies facts selected in this same writer transaction;
// direct reducer callers retain their own selection through sqliteReduceHost.
func sqliteReduceHostFacts(tx *sqliteio.Tx, meta sqliteStoreMeta, e HostEvent, p sqliteHostPrepared, facts sqliteHostFacts) (result sqliteHostTransition, err error) {
	defer func() {
		if err != nil {
			result = sqliteHostTransition{}
		}
	}()
	if err = validateHost(e); err != nil {
		return result, err
	}
	if !sqliteHostSupported(e) {
		return result, failure("unsupported_contract")
	}
	if old := sqliteHostExact(facts, e); old != nil {
		result.Result = old.Record.Result
		result.Result.Disposition = "duplicate"
		result.Finalization = sqliteEmptyFinalizationSelection()
		if old.Record.ErrorCode != "" {
			result.OperationError = failure(old.Record.ErrorCode)
		}
		return result, nil
	}
	same, err := sqliteHostSamePreparedFacts(e, facts, p.Facts)
	if err != nil {
		return result, err
	}
	if !same {
		return result, failure("state_busy")
	}
	if err = sqliteCaptureCheckBinding(tx, meta, Event{CWD: p.CWD}, p.Admission); err != nil {
		return result, err
	}
	if p.Extended {
		return sqliteReduceHostEffects(tx, meta, e, p, facts)
	}
	next := meta.Revision
	if next != "18446744073709551615" {
		next = bump(next)
	}
	m := sqliteCaptureMutation{tx: tx, meta: meta, nextRevision: next}
	m.transition.Finalization = sqliteEmptyFinalizationSelection()
	receipt := hostBase(e, next)
	receipt.ID = p.ReceiptID
	receipt.Disposition = "applied"
	receipt.Ordering = "supported"
	receipt.Durability = "committed"
	receipt.ObservedAt = p.ObservedAt
	receipt.ProfileBasis = p.Policy.Basis
	receipt.ProfileRevision = p.Policy.Revision
	receipt.Fingerprint = p.Policy.Fingerprint
	var normalizedEarly sqliteEventTransition
	ready := false
	if p.Normalized != nil {
		normalizedEarly, ready, err = sqliteCheckPreparedEvent(tx, meta, *p.Normalized, p.Capture, &p.Clock)
		if err != nil {
			return result, err
		}
		if !ready && normalizedEarly.OperationError != nil {
			return result, normalizedEarly.OperationError
		}
	} else if p.Clock.Site != sqliteCaptureClockNone && p.Clock.Mode == sqliteCapturePrepared {
		observed, x := sqliteCaptureObserveClock(tx, meta.ComputerID, meta.Revision)
		if x != nil {
			return result, x
		}
		same, x := sqliteCaptureSameClock(observed, p.Capture.ClockActors)
		if x != nil {
			return result, x
		}
		if !same {
			return result, failure("state_busy")
		}
	}
	// Validate original preparation before any safety/host/normalized row. One
	// concrete sample and one global safety pass belong to this mutation owner.
	var sample sqliteCaptureSample
	safetyChanged := false
	if ready || p.Clock.Site != sqliteCaptureClockNone {
		value, x := p.Clock.take(p.Clock.Site)
		if x != nil {
			var domain *Error
			if !errors.As(x, &domain) || domain.Code != "clock_unavailable" {
				return result, x
			}
		}
		sample = sqliteCaptureSample{Value: value, Unavailable: x != nil}
		safetyChanged, err = m.quarantineClock(value)
		if err != nil {
			return result, err
		}
	}
	if facts.Turn != nil && facts.Turn.Actor != nil && (!p.Policy.CaptureEligible || e.StopHookActive) {
		ref := *facts.Turn.Actor
		receipt.Actor = &ref
		receipt.Disposition = "review_required"
		receipt.Ordering = "review_required"
		receipt.DiagnosticCode = "ordering_unavailable"
		if !p.Policy.CaptureEligible {
			receipt.DiagnosticCode = p.Policy.DiagnosticCode
		}
		a, x := m.readActor(ref.Key)
		if x != nil {
			return result, x
		}
		if a != nil && a.Ref == ref && !sqliteCaptureTerminal(*a) {
			changed, x := m.quarantine(*a, "ordering_unavailable", e.Kind, sample.Value)
			if x != nil {
				return result, x
			}
			safetyChanged = changed || safetyChanged
			receipt.ObservedAt = sample.Value.WallUTC
		}
	}
	if p.Normalized != nil {
		n := normalizedEarly
		if ready {
			n, err = m.applyPreparedEvent(*p.Normalized, p.Capture.Binding, sample, false, safetyChanged)
			if err != nil {
				return result, err
			}
		}
		if n.OperationError != nil {
			if !n.Changed && !safetyChanged {
				return result, n.OperationError
			}
			receipt.Disposition = "review_required"
			receipt.Ordering = "review_required"
			receipt.DiagnosticCode = n.OperationError.Code
		} else if receipt.Disposition != "review_required" {
			receipt.Disposition = n.Result.Disposition
		}
		m.transition.OperationError = n.OperationError
	}
	session := facts.Session
	if session == nil {
		session = &sqliteHostSessionRow{Key: hostSessionKey(e), Value: hostSession{ID: p.NewIncarnation, Source: e.Source, NativeID: e.SessionID, CWD: p.CWD}}
	}
	afterSession := *session
	turn := facts.Turn
	switch e.Kind {
	case "UserPromptSubmit", "SubagentStart":
		if turn != nil {
			receipt.Disposition = "stale"
			if turn.Actor != nil {
				ref := *turn.Actor
				receipt.Actor = &ref
			}
		} else {
			if p.Normalized == nil {
				return result, failure("state_corrupt")
			}
			ref := ActorRef{Key: p.Normalized.Actor, Generation: p.Normalized.Generation}
			delta, x := sqliteEnsureActorGeneration(tx, ref)
			if err = m.add(delta, x); err != nil {
				return result, err
			}
			row := sqliteHostTurnRow{Key: hostTurnKey(session.Value.ID, e), Source: e.Source, NativeSession: e.SessionID, Incarnation: session.Value.ID, TurnID: e.TurnID, AgentID: e.AgentID, CWD: p.CWD, Actor: &ref}
			delta, x = sqliteWriteHostTurn(tx, meta.ComputerID, nil, row)
			if err = m.add(delta, x); err != nil {
				return result, err
			}
			turn = &row
			receipt.Actor = &ref
			m.transition.Dependencies.ChangedHostTurnKeys = append(m.transition.Dependencies.ChangedHostTurnKeys, row.Key)
			if e.Kind == "UserPromptSubmit" && e.AgentID == "" {
				afterSession.Value.RootTurn = row.Key
			}
		}
	case "Stop", "SubagentStop":
		if turn == nil {
			row := sqliteHostTurnRow{Key: hostTurnKey(session.Value.ID, e), Source: e.Source, NativeSession: e.SessionID, Incarnation: session.Value.ID, TurnID: e.TurnID, AgentID: e.AgentID, CWD: p.CWD, Stopped: true}
			delta, x := sqliteWriteHostTurn(tx, meta.ComputerID, nil, row)
			if err = m.add(delta, x); err != nil {
				return result, err
			}
			turn = &row
			m.transition.Dependencies.ChangedHostTurnKeys = append(m.transition.Dependencies.ChangedHostTurnKeys, row.Key)
		} else if p.Normalized != nil {
			after := *turn
			after.Stopped = true
			delta, x := sqliteWriteHostTurn(tx, meta.ComputerID, turn, after)
			if err = m.add(delta, x); err != nil {
				return result, err
			}
			turn = &after
			m.transition.Dependencies.ChangedHostTurnKeys = append(m.transition.Dependencies.ChangedHostTurnKeys, after.Key)
		}
		if turn.Actor != nil {
			ref := *turn.Actor
			receipt.Actor = &ref
		}
		if p.Normalized == nil && receipt.Disposition != "review_required" {
			receipt.Disposition = "stale"
		}
	}
	if facts.Session == nil || afterSession.Value.RootTurn != facts.Session.Value.RootTurn {
		delta, x := sqliteWriteHostSession(tx, meta.ComputerID, facts.Session, afterSession)
		if err = m.add(delta, x); err != nil {
			return result, err
		}
	}
	if p.Normalized != nil {
		a, x := m.readActor(p.Normalized.Actor)
		if x != nil {
			return result, x
		}
		if a != nil {
			receipt.ObservedAt = a.LastEvidence.WallUTC
		}
	}
	key := facts.ReceiptKey
	if key == "" {
		key = hostEventKey(session.Value.ID, e)
	}
	code := ""
	if m.transition.OperationError != nil {
		code = m.transition.OperationError.Code
	}
	row := sqliteHostReceiptRow{Key: key, Record: hostReceiptRecord{Fingerprint: hostFingerprint(e), Result: receipt, ErrorCode: code}}
	delta, x := sqliteInsertHostReceipt(tx, meta.ComputerID, next, row)
	if err = m.add(delta, x); err != nil {
		return result, err
	}
	m.transition.Dependencies.HostReceiptKeys = append(m.transition.Dependencies.HostReceiptKeys, key)
	m.transition.Dependencies.HostSessions = append(m.transition.Dependencies.HostSessions, sqliteHostSessionIdentity{Source: e.Source, NativeSession: e.SessionID})
	result = sqliteHostTransition{Result: receipt, Changed: true, OperationError: m.transition.OperationError, Delta: m.transition.Delta, Dependencies: m.transition.Dependencies, Finalization: m.transition.Finalization}
	return result, nil
}
