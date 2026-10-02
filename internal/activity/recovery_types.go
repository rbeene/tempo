package activity

import "time"

type ReviewInput struct{ AccountID, ProjectID string }

type ReviewList struct {
	ContractVersion  int           `json:"contract_version"`
	SnapshotRevision string        `json:"snapshot_revision"`
	Uncertainties    []Uncertainty `json:"uncertainties"`
}

type TimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type RecoveryInput struct {
	UncertaintyID string
	End           *time.Time
	DiscardTail   bool
}

type RecoveryPreview struct {
	ContractVersion     int         `json:"contract_version"`
	SnapshotRevision    string      `json:"snapshot_revision"`
	Uncertainty         Uncertainty `json:"uncertainty"`
	SegmentStart        time.Time   `json:"segment_start"`
	ConfirmedPrefix     TimeRange   `json:"confirmed_prefix"`
	ProposedEnd         time.Time   `json:"proposed_end"`
	DiscardedSuffix     *TimeRange  `json:"discarded_suffix"`
	AffectedUnionBefore []TimeRange `json:"affected_union_before"`
	AffectedUnionAfter  []TimeRange `json:"affected_union_after"`
	StillBlockedIDs     []string    `json:"still_blocked_ids"`
}

type ResolveInput struct {
	UncertaintyID                 string
	End                           *time.Time
	DiscardTail                   bool
	IfRevision, Reason, RequestID string
	Confirmed                     bool
}

type InterruptInput struct {
	ActorID, Generation, IfRevision, RequestID string
	Confirmed                                  bool
}

// SourceObservation carries positive loss of source continuity, never inactivity
// inferred from elapsed time. Actor selects exactly one recorded generation.
type SourceObservation struct {
	Actor             ActorRef
	Reason, RequestID string
}

type ClockObservation struct{ RequestID string }
