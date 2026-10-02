package activity

import (
	"context"
	"sort"
	"time"
)

// HostEvent is sanitized identity metadata. It contains no transcript, prompt,
// tool arguments/results, source timestamps, or caller-selected eligibility.
type HostEvent struct {
	Source, SessionID, TurnID, AgentID, ParentAgentID string
	Kind, CWD, ToolID, ToolName, SessionSource        string
	StopHookActive                                    bool
}

// HostReceipt records capture, not proof that a native host delivered it.
// Origin remains unverified until an independent production correlation exists.
type HostReceipt struct {
	ContractVersion  int       `json:"contract_version"`
	SnapshotRevision string    `json:"snapshot_revision"`
	ID               string    `json:"id"`
	Source           string    `json:"source"`
	SessionID        string    `json:"session_id"`
	TurnID           string    `json:"turn_id"`
	AgentID          string    `json:"agent_id"`
	Kind             string    `json:"kind"`
	ToolID           string    `json:"tool_id"`
	Disposition      string    `json:"disposition"`
	Ordering         string    `json:"ordering"`
	DiagnosticCode   string    `json:"diagnostic_code"`
	Durability       string    `json:"durability"`
	Origin           string    `json:"origin"`
	ProfileBasis     string    `json:"profile_basis"`
	ProfileRevision  string    `json:"profile_revision"`
	Fingerprint      string    `json:"fingerprint"`
	Actor            *ActorRef `json:"actor"`
	ObservedAt       time.Time `json:"observed_at"`
}

type HostReceiptFilter struct{ Source, SessionID string }
type HostReceiptList struct {
	ContractVersion  int           `json:"contract_version"`
	SnapshotRevision string        `json:"snapshot_revision"`
	Receipts         []HostReceipt `json:"receipts"`
}

// HostObservation carries positively observed loss, not silence. It targets one
// exact native turn/actor and reuses the shared recovery transaction helpers.
type HostObservation struct {
	Source, SessionID, TurnID, AgentID, Reason, RequestID string
}

func (s *Service) HostReceipts(ctx context.Context, filter HostReceiptFilter) (HostReceiptList, error) {
	if filter.Source != "" && filter.Source != "codex" && filter.Source != "claude" || filter.SessionID != "" && !safeIdentifier(filter.SessionID, 256) {
		return HostReceiptList{}, failure("validation")
	}
	st, _, err := s.store.read(ctx)
	if err != nil {
		return HostReceiptList{}, err
	}
	list := HostReceiptList{ContractVersion: 1, SnapshotRevision: st.Revision, Receipts: []HostReceipt{}}
	for _, r := range st.HostReceipts {
		if (filter.Source == "" || r.Result.Source == filter.Source) && (filter.SessionID == "" || r.Result.SessionID == filter.SessionID) {
			list.Receipts = append(list.Receipts, r.Result)
		}
	}
	sort.Slice(list.Receipts, func(i, j int) bool {
		a, b := list.Receipts[i], list.Receipts[j]
		x, _ := counter(a.SnapshotRevision)
		y, _ := counter(b.SnapshotRevision)
		if x != y {
			return x < y
		}
		return a.ID < b.ID
	})
	return list, nil
}

func (s *Service) ObserveHost(ctx context.Context, in HostObservation) (MutationResult, error) {
	if !hostSource(in.Source) || !safeIdentifier(in.SessionID, 256) || !safeIdentifier(in.TurnID, 256) || in.AgentID != "" && !safeIdentifier(in.AgentID, 128) {
		return MutationResult{}, failure("validation")
	}
	switch in.Reason {
	case "source_lost", "ordering_unavailable", "restart_unknown":
	default:
		return MutationResult{}, failure("validation")
	}
	return s.recoveryMutation(ctx, in.RequestID, "activity.observe_host", in, func(st *state) (MutationResult, bool, error) {
		var target *ActorRef
		for _, turn := range st.HostTurns {
			if turn.Source == in.Source && turn.SessionID == in.SessionID && turn.TurnID == in.TurnID && turn.AgentID == in.AgentID && turn.Actor != nil {
				if target != nil && *target != *turn.Actor {
					return MutationResult{}, false, failure("event_conflict")
				}
				target = turn.Actor
			}
		}
		if target == nil {
			return MutationResult{}, false, failure("actor_not_found")
		}
		return s.observeSourceState(st, SourceObservation{Actor: *target, Reason: in.Reason})
	})
}
