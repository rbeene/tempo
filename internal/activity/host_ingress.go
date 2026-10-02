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
	if e.Kind == "Stop" || e.Kind == "SubagentStop" || e.Kind == "Interrupt" {
		candidates := historicalActors(st, e)
		if len(candidates) == 1 {
			return candidates[0]
		}
		return nil
	}
	if e.Kind == "PreToolUse" || e.Kind == "PostToolUse" || e.Kind == "PermissionRequest" || e.Kind == "PreCompact" || e.Kind == "PostCompact" {
		candidates := toolCandidates(st, e)
		if len(candidates) == 1 {
			return candidates[0]
		}
		return nil
	}
	return st.HostTurns[hostTurnKey(session.ID, e)]
}

func toolCandidates(st *state, e HostEvent) []*hostTurn {
	var targets []*hostTurn
	for _, t := range st.HostTurns {
		if t.Source == e.Source && t.SessionID == e.SessionID && t.TurnID == e.TurnID {
			targets = append(targets, t)
		}
	}
	return targets
}
func hostConflictKey(key, fingerprint string) string {
	return hostHash([]string{key, fingerprint, "conflict"})
}

func hostReceiptKey(st *state, session *hostSession, e HostEvent) string {
	incarnation := session.ID
	if e.Kind != "SessionStart" && e.Kind != "UserPromptSubmit" && e.Kind != "SubagentStart" {
		if target := hostTarget(st, e); target != nil {
			incarnation = target.Session
		}
	}
	return hostEventKey(incarnation, e)
}
func historicalActors(st *state, e HostEvent) []*hostTurn {
	var candidates []*hostTurn
	for _, t := range st.HostTurns {
		if t.Source == e.Source && t.SessionID == e.SessionID && t.TurnID == e.TurnID && t.AgentID == e.AgentID {
			candidates = append(candidates, t)
		}
	}
	return candidates
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
		key := hostReceiptKey(st, session, e)
		if old, ok := st.HostReceipts[key]; ok && old.Fingerprint != hostFingerprint(e) {
			key = hostConflictKey(key, hostFingerprint(e))
		}
		if old, ok := st.HostReceipts[key]; ok && old.Fingerprint == hostFingerprint(e) && (old.ErrorCode != "" || !freshHostBoundary(st, session, e)) {
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
		sample, clockErr := s.sample()
		quarantineClock(st, sample)
		if clockErr != nil {
			result = committedHostReceipt(e, st.Revision, p)
			result.Disposition = "review_required"
			result.Ordering = "review_required"
			result.DiagnosticCode = "clock_unavailable"
			if root := st.HostTurns[session.RootTurn]; root != nil {
				result.Actor = root.Actor
			}
			st.HostReceipts[hostEventKey(session.ID, e)] = hostReceiptRecord{Fingerprint: hostFingerprint(e), Result: result, ErrorCode: "clock_unavailable"}
			return result, true, "clock_unavailable", nil
		}
		for _, a := range st.Actors {
			if a.Ref.Key.SessionID != session.ID || terminal(a) {
				continue
			}
			retainHostWaitLoss(st, a, "SessionStart", sample.WallUTC)
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

	key := hostReceiptKey(st, session, e)
	if old, ok := st.HostReceipts[key]; ok {
		if old.Fingerprint != hostFingerprint(e) {
			conflictKey := hostConflictKey(key, hostFingerprint(e))
			if prior, ok := st.HostReceipts[conflictKey]; ok {
				result = prior.Result
				result.Disposition = "duplicate"
				return result, false, prior.ErrorCode, nil
			}
			result = committedHostReceipt(e, st.Revision, p)
			// The original receipt identifies the affected generation; conflicting new
			// payload cannot redirect the safety observation to another actor.
			if old.Result.Actor != nil {
				if a := st.Actors[actorKey(old.Result.Actor.Key)]; a != nil && a.Ref == *old.Result.Actor {
					retainHostWaitLoss(st, a, e.Kind, result.ObservedAt)
				}
				s.reviewHost(st, &hostTurn{Actor: old.Result.Actor}, &result, "ordering_unavailable")
			} else {
				result.Disposition = "review_required"
				result.Ordering = "review_required"
			}
			result.DiagnosticCode = "event_conflict"
			st.HostReceipts[conflictKey] = hostReceiptRecord{Fingerprint: hostFingerprint(e), Result: result, ErrorCode: "event_conflict"}
			return result, true, "event_conflict", nil
		}
		result = old.Result
		result.Disposition = "duplicate"
		return result, false, old.ErrorCode, nil
	}
	result = committedHostReceipt(e, st.Revision, p)
	turnKey := hostTurnKey(session.ID, e)
	turn := hostTarget(st, e)
	safetyChanged := false
	if !p.CaptureEligible && turn != nil && turn.Actor != nil {
		if a := st.Actors[actorKey(turn.Actor.Key)]; a != nil && a.Ref == *turn.Actor {
			safetyChanged = retainHostWaitLoss(st, a, e.Kind, result.ObservedAt)
		}
		safetyChanged = s.reviewHost(st, turn, &result, "ordering_unavailable") || safetyChanged
		result.DiagnosticCode = p.DiagnosticCode
	}
	if e.Kind == "PreToolUse" || e.Kind == "PostToolUse" || e.Kind == "PermissionRequest" {
		candidates := toolCandidates(st, e)
		if len(candidates) > 1 {
			for _, candidate := range candidates {
				if candidate.Actor != nil {
					if a := st.Actors[actorKey(candidate.Actor.Key)]; a != nil && a.Ref == *candidate.Actor {
						safetyChanged = retainHostWaitLoss(st, a, e.Kind, result.ObservedAt) || safetyChanged
					}
				}
				safetyChanged = s.reviewHost(st, candidate, &result, "ordering_unavailable") || safetyChanged
			}
			result.Actor = nil
		}
	}

	if e.Kind == "Stop" || e.Kind == "SubagentStop" || e.Kind == "Interrupt" {
		candidates := historicalActors(st, e)
		if len(candidates) > 1 {
			for _, candidate := range candidates {
				if candidate.Actor != nil {
					if a := st.Actors[actorKey(candidate.Actor.Key)]; a != nil && a.Ref == *candidate.Actor {
						retainHostWaitLoss(st, a, e.Kind, result.ObservedAt)
					}
				}
				s.reviewHost(st, candidate, &result, "ordering_unavailable")
			}
			result.Actor = nil
			st.HostReceipts[key] = hostReceiptRecord{Fingerprint: hostFingerprint(e), Result: result}
			return result, true, "", nil
		}
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
		// A safely rejected admission may still reserve its native generation in a
		// committed receipt. Never let a later distinct turn reuse that generation.
		for _, prior := range st.HostTurns {
			if prior.Actor != nil && prior.Actor.Key == actor {
				n, _ := counter(prior.Actor.Generation)
				next, _ := counter(generation)
				if n >= next {
					generation = bump(prior.Actor.Generation)
				}
			}
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
		if a.State == "wait_children" && hostWaiting(turn) {
			safetyChanged = fenceHostWait(a, &result, "incomplete_wait") || safetyChanged
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
		if a == nil || a.Ref != *turn.Actor || turn.Stopped || terminal(a) || a.State != "working" && a.State != "wait_children" {
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
			kind := "observe_work"
			if hostWaiting(turn) {
				kind = "wait_children"
			} else if a.State == "wait_children" {
				kind = "work"
			}
			normalized = &Event{ContractVersion: 1, Actor: a.Ref.Key, Generation: a.Ref.Generation, Sequence: bump(a.Sequence), EventID: result.ID, Kind: kind}
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
		if e.Kind == "Interrupt" && turn == nil {
			turn = &hostTurn{Source: e.Source, SessionID: e.SessionID, Session: session.ID, TurnID: e.TurnID, AgentID: e.AgentID, CWD: cwd, Stopped: true}
			st.HostTurns[turnKey] = turn
		}
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
		if e.Kind == "Interrupt" && !captureReview(result) {
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
			if a := st.Actors[actorKey(normalized.Actor)]; a != nil && a.Ref == *turn.Actor && a.State == "wait_children" {
				safetyChanged = fenceHostWait(a, &result, "source_loss_while_waiting") || safetyChanged
			}
			if !changed && !safetyChanged {
				return result, false, "", err
			}
			operationError = err.(*Error).Code
			result.Disposition = "review_required"
			result.Ordering = "review_required"
			if !captureReview(result) {
				result.DiagnosticCode = operationError
			}
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
	if a.State == "wait_children" {
		return fenceHostWait(a, result, "source_loss_while_waiting")
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

func committedHostReceipt(e HostEvent, revision string, p hookstate.Profile) HostReceipt {
	result := hostBase(e, bump(revision))
	result.ID = newID()
	result.Disposition = "applied"
	result.Ordering = "supported"
	result.Durability = "committed"
	result.ProfileBasis = p.Basis
	result.ProfileRevision = p.Revision
	result.Fingerprint = p.Fingerprint
	result.ObservedAt = time.Now().UTC()
	return result
}
