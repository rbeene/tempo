package activity

import (
	"context"
	"errors"
	"path/filepath"
	"time"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/hookstate"
)

func validateHost(e HostEvent) error {
	if e.Source != "codex" {
		return failure("unsupported_contract")
	}
	if !safeIdentifier(e.SessionID, 256) || !utf8.ValidString(e.SessionID) || !filepath.IsAbs(e.CWD) || !safeIdentifier(e.CWD, 4096) || e.ParentAgentID != "" {
		return failure("validation")
	}
	switch e.Kind {
	case "SessionStart":
		if e.TurnID != "" || e.AgentID != "" {
			return failure("validation")
		}
		switch e.SessionSource {
		case "startup", "resume", "clear", "compact":
		default:
			return failure("validation")
		}
	case "SessionEnd":
		if e.TurnID != "" || e.AgentID != "" {
			return failure("validation")
		}
	case "UserPromptSubmit", "Stop", "Interrupt", "SubagentStart", "SubagentStop", "PreToolUse", "PostToolUse", "PermissionRequest", "PreCompact", "PostCompact":
		if !safeIdentifier(e.TurnID, 256) || !utf8.ValidString(e.TurnID) {
			return failure("validation")
		}
	default:
		return failure("unsupported_contract")
	}
	if e.Kind == "SubagentStart" || e.Kind == "SubagentStop" {
		if !safeIdentifier(e.AgentID, 128) || !utf8.ValidString(e.AgentID) {
			return failure("validation")
		}
	} else if e.AgentID != "" {
		return failure("validation")
	}
	if e.Kind == "PreToolUse" || e.Kind == "PostToolUse" {
		if !safeIdentifier(e.ToolID, 256) || !safeIdentifier(e.ToolName, 256) {
			return failure("validation")
		}
	}
	return nil
}
func hostBase(e HostEvent, revision string) HostReceipt {
	return HostReceipt{ContractVersion: 1, SnapshotRevision: revision, Source: e.Source, SessionID: e.SessionID, TurnID: e.TurnID, AgentID: e.AgentID, Kind: e.Kind, ToolID: e.ToolID, Disposition: "untracked", Ordering: "unavailable", Durability: "not_committed", Origin: "unverified"}
}
func hostTarget(st *state, e HostEvent) *hostTurn {
	session := st.HostSessions[hostSessionKey(e)]
	if session == nil {
		return nil
	}
	if e.Kind == "SessionEnd" {
		return st.HostTurns[session.RootTurn]
	}
	if e.Kind == "PreToolUse" || e.Kind == "PostToolUse" || e.Kind == "PermissionRequest" || e.Kind == "PreCompact" || e.Kind == "PostCompact" {
		var target *hostTurn
		for _, t := range st.HostTurns {
			if t.Session == session.ID && t.TurnID == e.TurnID {
				if target != nil {
					return nil
				}
				target = t
			}
		}
		return target
	}
	return st.HostTurns[hostTurnKey(session.ID, e)]
}
func hostContext(st *state, e HostEvent) string {
	if turn := hostTarget(st, e); turn != nil {
		return turn.CWD
	}
	if session := st.HostSessions[hostSessionKey(e)]; session != nil && e.Kind == "SessionEnd" {
		return session.CWD
	}
	return e.CWD
}
func (s *Service) IngestHost(ctx context.Context, e HostEvent) (HostReceipt, error) {
	result := hostBase(e, "0")
	if err := validateHost(e); err != nil {
		return result, err
	}
	st, exists, err := s.store.read(ctx)
	if err != nil || !exists {
		return result, err
	}
	result.SnapshotRevision = st.Revision
	// Exact committed replays reconcile durability before clocks or live policy.
	if session := st.HostSessions[hostSessionKey(e)]; session != nil {
		key := hostEventKey(session.ID, e)
		if old, ok := st.HostReceipts[key]; ok && old.Fingerprint == hostFingerprint(e) && !freshHostBoundary(st, session, e) {
			err = s.store.update(ctx, func(current *state) (bool, error) {
				r, ok := current.HostReceipts[key]
				if !ok || r.Fingerprint != old.Fingerprint {
					return false, failure("event_conflict")
				}
				result = r.Result
				result.Disposition = "duplicate"
				return false, nil
			})
			return hostOutcome(result, err, old.ErrorCode)
		}
	}
	cwd := hostContext(st, e)
	target := hostTarget(st, e)
	if target == nil || target.Actor == nil {
		_, linked, err := s.resolveInitial(ctx, st, Event{CWD: cwd})
		if err != nil || !linked {
			return result, err
		}
	}
	p, err := s.policies.Eligibility(ctx, e.Source, cwd)
	if err != nil {
		return result, err
	}
	if !p.CaptureEligible && (target == nil || target.Actor == nil) {
		result.Disposition = "review_required"
		result.ProfileBasis, result.ProfileRevision, result.Fingerprint, result.DiagnosticCode = p.Basis, p.Revision, p.Fingerprint, p.DiagnosticCode
		return result, nil
	}
	var operationError string
	err = s.store.update(ctx, func(current *state) (bool, error) {
		initHostState(current)
		if hostContext(current, e) != cwd {
			return false, failure("state_busy")
		}
		var changed bool
		result, changed, operationError, err = s.reduceHost(ctx, current, e, p, cwd)
		return changed, err
	})
	return hostOutcome(result, err, operationError)
}
func hostOutcome(result HostReceipt, err error, operationError string) (HostReceipt, error) {
	if err != nil {
		result.Durability = "not_committed"
		var ae *Error
		if errors.As(err, &ae) && ae.Uncertain {
			result.Durability = "unknown"
		}
		return result, err
	}
	if operationError != "" {
		return result, failure(operationError)
	}
	return result, nil
}
func (s *Service) reduceHost(ctx context.Context, st *state, e HostEvent, p hookstate.Profile, cwd string) (HostReceipt, bool, string, error) {
	result := hostBase(e, st.Revision)
	session := st.HostSessions[hostSessionKey(e)]
	if session == nil {
		if e.Kind != "SessionStart" || e.SessionSource == "compact" {
			return result, false, "", failure("unsupported_contract")
		}
		_, linked, err := s.resolveInitial(ctx, st, Event{CWD: cwd})
		if err != nil || !linked {
			return result, false, "", err
		}
		session = &hostSession{ID: newID(), Source: e.Source, NativeID: e.SessionID, CWD: cwd}
		st.HostSessions[hostSessionKey(e)] = session
	}
	if freshHostBoundary(st, session, e) {
		sample, _ := s.sample()
		quarantineClock(st, sample)
		for _, a := range st.Actors {
			if a.Ref.Key.SessionID != session.ID || terminal(a) {
				continue
			}
			quarantine(st, a, "restart_unknown", sample)
			// A root resume is not a child termination signal. Unknown child tails stay
			// open for their individually identified terminal/recovery observation.
			if a.Ref.Key.AgentID == "root" {
				if err := capUncertainties(st, a, sample); err != nil {
					return result, false, "", err
				}
				detach(a)
			}
		}
		session = &hostSession{ID: newID(), Source: e.Source, NativeID: e.SessionID, CWD: cwd}
		st.HostSessions[hostSessionKey(e)] = session
	}

	key := hostEventKey(session.ID, e)
	if old, ok := st.HostReceipts[key]; ok {
		if old.Fingerprint != hostFingerprint(e) {
			return result, false, "", failure("event_conflict")
		}
		result = old.Result
		result.Disposition = "duplicate"
		return result, false, old.ErrorCode, nil
	}
	result.ID = newID()
	result.SnapshotRevision = bump(st.Revision)
	result.Disposition = "applied"
	result.Ordering = "supported"
	result.Durability = "committed"
	result.ProfileBasis = p.Basis
	result.ProfileRevision = p.Revision
	result.Fingerprint = p.Fingerprint
	result.ObservedAt = time.Now().UTC()
	turnKey := hostTurnKey(session.ID, e)
	turn := hostTarget(st, e)
	safetyChanged := false
	if !p.CaptureEligible && turn != nil && turn.Actor != nil {
		safetyChanged = s.reviewHost(st, turn, &result, "ordering_unavailable")
		result.DiagnosticCode = p.DiagnosticCode
	}
	var normalized *Event
	switch e.Kind {
	case "SessionStart":

	case "UserPromptSubmit", "SubagentStart":
		if turn != nil {
			result.Actor = turn.Actor
			result.Disposition = "stale"
			break
		}
		actor := ActorKey{ComputerID: st.ComputerID, Source: e.Source, SessionID: session.ID, AgentID: hostAgent(e.AgentID)}
		generation := "1"
		if old := st.Actors[actorKey(actor)]; old != nil {
			generation = bump(old.Ref.Generation)
		}
		turn = &hostTurn{Source: e.Source, SessionID: e.SessionID, Session: session.ID, TurnID: e.TurnID, AgentID: e.AgentID, CWD: cwd, Actor: &ActorRef{Key: actor, Generation: generation}}
		st.HostTurns[turnKey] = turn
		if e.Kind == "UserPromptSubmit" {
			session.RootTurn = turnKey
		}
		normalized = &Event{ContractVersion: 1, Actor: actor, Generation: generation, Sequence: "1", EventID: result.ID, Kind: "work", CWD: cwd}
	case "Stop", "SubagentStop":
		if turn == nil {
			turn = &hostTurn{Source: e.Source, SessionID: e.SessionID, Session: session.ID, TurnID: e.TurnID, AgentID: e.AgentID, CWD: cwd, Stopped: true}
			st.HostTurns[turnKey] = turn
		}
		if turn.Actor == nil {
			result.Disposition = "stale"
			break
		}
		if e.StopHookActive {
			safetyChanged = s.reviewHost(st, turn, &result, "ordering_unavailable") || safetyChanged
			break
		}
		a := st.Actors[actorKey(turn.Actor.Key)]
		if a == nil || a.Ref != *turn.Actor || turn.Stopped || terminal(a) {
			result.Actor = turn.Actor
			result.Disposition = "stale"
			break
		}
		normalized = &Event{ContractVersion: 1, Actor: a.Ref.Key, Generation: a.Ref.Generation, Sequence: bump(a.Sequence), EventID: result.ID, Kind: "wait_user"}
		turn.Stopped = true
	case "PreToolUse", "PostToolUse":
		if turn == nil || turn.Actor == nil {
			result.Disposition = "review_required"
			result.Ordering = "review_required"
			result.DiagnosticCode = "ordering_unavailable"
			break
		}
		result.Actor = turn.Actor
		a := st.Actors[actorKey(turn.Actor.Key)]
		if a == nil || a.Ref != *turn.Actor || turn.Stopped || terminal(a) || a.State != "working" {
			result.Disposition = "stale"
			break
		}
		if turn.Tools == nil {
			turn.Tools = map[string]hostTool{}
		}
		phase, known := turn.Tools[e.ToolID]
		if known && phase.Name != e.ToolName {
			safetyChanged = s.reviewHost(st, turn, &result, "ordering_unavailable") || safetyChanged
			break
		}
		if e.Kind == "PreToolUse" && known && phase.Phase == "post" {
			result.Disposition = "stale"
			break
		}
		if e.Kind == "PostToolUse" && !known {
			safetyChanged = s.reviewHost(st, turn, &result, "ordering_unavailable") || safetyChanged
		}
		next := "pre"
		if e.Kind == "PostToolUse" {
			next = "post"
		}
		turn.Tools[e.ToolID] = hostTool{Name: e.ToolName, Phase: next}
		if a.Health == "continuous" {
			normalized = &Event{ContractVersion: 1, Actor: a.Ref.Key, Generation: a.Ref.Generation, Sequence: bump(a.Sequence), EventID: result.ID, Kind: "observe_work"}
		}
	case "PermissionRequest":
		if turn == nil || turn.Actor == nil {
			result.Disposition = "review_required"
			result.Ordering = "review_required"
			result.DiagnosticCode = "ordering_unavailable"
			break
		}
		safetyChanged = s.reviewHost(st, turn, &result, "ordering_unavailable") || safetyChanged
	case "Interrupt", "SessionEnd":
		if turn == nil || turn.Actor == nil {
			result.Disposition = "stale"
			break
		}
		result.Actor = turn.Actor
		a := st.Actors[actorKey(turn.Actor.Key)]
		if a == nil || a.Ref != *turn.Actor || turn.Stopped || terminal(a) {
			result.Disposition = "stale"
			break
		}
		safetyChanged = s.reviewHost(st, turn, &result, "source_lost") || safetyChanged
		// Interrupt is a native terminal boundary, but its unconfirmed tail remains
		// uncertain. SessionEnd alone provides only loss, never a reliable finish.
		if e.Kind == "Interrupt" {
			result.Disposition = "applied"
			result.Ordering = "supported"
		}
		normalized = &Event{ContractVersion: 1, Actor: a.Ref.Key, Generation: a.Ref.Generation, Sequence: bump(a.Sequence), EventID: result.ID, Kind: "interrupt"}
		turn.Stopped = true

	default:
		return result, false, "", failure("unsupported_contract")
	}
	operationError := ""
	if normalized != nil {
		r, changed, err := s.reduce(ctx, st, *normalized)
		if err != nil {
			if !changed && !safetyChanged {
				return result, false, "", err
			}
			operationError = err.(*Error).Code
			result.Disposition = "review_required"
			result.Ordering = "review_required"
			result.DiagnosticCode = operationError
		} else if result.Disposition != "review_required" {
			result.Disposition = r.Disposition
		}
		result.Actor = turn.Actor
		if a := st.Actors[actorKey(turn.Actor.Key)]; a != nil {
			result.ObservedAt = a.LastEvidence.WallUTC
		}
	}
	st.HostReceipts[key] = hostReceiptRecord{Fingerprint: hostFingerprint(e), Result: result, ErrorCode: operationError}
	return result, true, operationError, nil
}

// A positively observed causal contradiction can quarantine a known actor. No
// equivalent promise is made for failures that never commit this transaction.
func (s *Service) reviewHost(st *state, turn *hostTurn, result *HostReceipt, reason string) bool {
	result.Disposition = "review_required"
	result.Ordering = "review_required"
	result.DiagnosticCode = reason
	result.Actor = turn.Actor
	if turn.Actor == nil {
		return false
	}
	a := st.Actors[actorKey(turn.Actor.Key)]
	if a == nil || a.Ref != *turn.Actor || terminal(a) {
		return false
	}
	sample, _ := s.sample()
	changed := quarantine(st, a, reason, sample)
	changed = quarantineClock(st, sample) || changed
	result.ObservedAt = sample.WallUTC
	return changed
}

func freshHostBoundary(st *state, session *hostSession, e HostEvent) bool {
	if e.Kind != "SessionStart" || e.SessionSource != "resume" && e.SessionSource != "clear" {
		return false
	}
	for _, t := range st.HostTurns {
		if t.Session == session.ID && t.Actor != nil {
			return true
		}
	}
	return false
}
