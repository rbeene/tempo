// Package activity owns durable local activity independently of Harvest timers.
package activity

import (
	"context"
	"time"

	"github.com/rbeene/tempo/internal/hookstate"
)

type Clock interface{ Sample() (ClockSample, error) }
type ClockFunc func() (ClockSample, error)

func (f ClockFunc) Sample() (ClockSample, error) { return f() }

type Options struct {
	ObserveWorker  WorkerObserver
	Path           string
	Clock          Clock
	ResolveBinding BindingResolver
	LockTimeout    time.Duration
	HookPolicies   *hookstate.Service
}
type BindingResolver func(context.Context, Event) (BindingSnapshot, bool, error)
type ActorKey struct {
	ComputerID string `json:"computer_id"`
	Source     string `json:"source"`
	SessionID  string `json:"session_id"`
	AgentID    string `json:"agent_id"`
}
type ActorRef struct {
	Key        ActorKey `json:"key"`
	Generation string   `json:"generation"`
}
type Attribution struct {
	AccountID string `json:"account_id"`
	UserID    string `json:"user_id"`
	ProjectID string `json:"project_id"`
	TaskID    string `json:"task_id"`
	Timezone  string `json:"timezone"`
}
type BindingSnapshot struct {
	ID          string      `json:"id"`
	Revision    string      `json:"revision"`
	Attribution Attribution `json:"attribution"`
}
type Event struct {
	ContractVersion int       `json:"contract_version"`
	Actor           ActorKey  `json:"actor"`
	Generation      string    `json:"generation"`
	Sequence        string    `json:"sequence"`
	EventID         string    `json:"event_id"`
	Kind            string    `json:"kind"`
	BindingID       string    `json:"binding_id,omitempty"`
	BindingRevision string    `json:"binding_revision,omitempty"`
	Parent          *ActorRef `json:"parent,omitempty"`
	CWD             string    `json:"cwd,omitempty"`
}
type ClockSample struct {
	Capability string    `json:"capability"`
	WallUTC    time.Time `json:"wall_utc"`
	Epoch      *string   `json:"epoch"`
	ElapsedNS  *string   `json:"elapsed_ns"`
	AwakeNS    *string   `json:"awake_ns"`
}
type EventResult struct {
	ContractVersion  int      `json:"contract_version"`
	SnapshotRevision string   `json:"snapshot_revision"`
	Disposition      string   `json:"disposition"`
	Actor            ActorRef `json:"actor"`
	SegmentID        *string  `json:"segment_id"`
	UncertaintyIDs   []string `json:"uncertainty_ids"`
}
type Error struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Uncertain bool           `json:"uncertain"`
	Details   map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string { return e.Message }

type ActivitySnapshot struct {
	// CaptureReviews retain non-reconstructable host capture warnings. They are
	// not timing uncertainties and cannot be resolved with an interval end.
	CaptureReviews   []HostReceipt     `json:"capture_reviews"`
	ContractVersion  int               `json:"contract_version"`
	SnapshotRevision string            `json:"snapshot_revision"`
	ObservedAt       time.Time         `json:"observed_at"`
	ComputerID       *string           `json:"computer_id"`
	Projects         []ProjectActivity `json:"projects"`
	ProjectTimers    []ProjectTimer    `json:"project_timers"`
	Actors           []Actor           `json:"actors"`
	Uncertainties    []Uncertainty     `json:"uncertainties"`
	ClosedIntervals  []Interval        `json:"closed_intervals"`
	Worker           WorkerStatus      `json:"worker"`
	SyncEnabled      bool              `json:"sync_enabled"`
}
type Actor struct {
	ID              string      `json:"id"`
	Revision        string      `json:"revision"`
	Ref             ActorRef    `json:"ref"`
	Sequence        string      `json:"sequence"`
	State           string      `json:"state"`
	Health          string      `json:"health"`
	BindingID       string      `json:"binding_id"`
	BindingRevision string      `json:"binding_revision"`
	Attribution     Attribution `json:"attribution"`
	Parent          *ActorRef   `json:"parent"`
	SegmentID       *string     `json:"segment_id"`
	LastEvidence    ClockSample `json:"last_evidence"`
	UncertaintyIDs  []string    `json:"uncertainty_ids"`
}
type Interval struct {
	ID          string      `json:"id"`
	ComputerID  string      `json:"computer_id"`
	Attribution Attribution `json:"attribution"`
	Start       time.Time   `json:"start"`
	End         time.Time   `json:"end"`
	DurationNS  string      `json:"duration_ns"`
	SegmentIDs  []string    `json:"segment_ids"`
}
type Uncertainty struct {
	ID            string      `json:"id"`
	Revision      string      `json:"revision"`
	Actor         ActorRef    `json:"actor"`
	SegmentID     string      `json:"segment_id"`
	Attribution   Attribution `json:"attribution"`
	LowerBound    time.Time   `json:"lower_bound"`
	UpperBound    *time.Time  `json:"upper_bound"`
	Reason        string      `json:"reason"`
	State         string      `json:"state"`
	ResolutionEnd *time.Time  `json:"resolution_end"`
	Discarded     bool        `json:"discarded"`
}
type ProjectActivity struct {
	ComputerID          string      `json:"computer_id"`
	Attribution         Attribution `json:"attribution"`
	ActiveActorRefs     []ActorRef  `json:"active_actor_refs"`
	WaitingActorRefs    []ActorRef  `json:"waiting_actor_refs"`
	ProvisionalUnionNS  string      `json:"provisional_union_ns"`
	ConfirmedClosedNS   string      `json:"confirmed_closed_ns"`
	UnresolvedIDs       []string    `json:"unresolved_ids"`
	QueuedCount         int         `json:"queued_count"`
	SyncedCount         int         `json:"synced_count"`
	NeedsAttentionCount int         `json:"needs_attention_count"`
}

// ProjectTimer projects one local computer/account/project elapsed-time union.
// Attribution epochs remain available in ActivitySnapshot.Projects.
type ProjectTimer struct {
	ComputerID          string     `json:"computer_id"`
	AccountID           string     `json:"account_id"`
	ProjectID           string     `json:"project_id"`
	ProvisionalUnionNS  string     `json:"provisional_union_ns"`
	ConfirmedClosedNS   string     `json:"confirmed_closed_ns"`
	ActiveActorRefs     []ActorRef `json:"active_actor_refs"`
	WaitingActorRefs    []ActorRef `json:"waiting_actor_refs"`
	UnresolvedIDs       []string   `json:"unresolved_ids"`
	QueuedCount         int        `json:"queued_count"`
	SyncedCount         int        `json:"synced_count"`
	NeedsAttentionCount int        `json:"needs_attention_count"`
}

type WorkerStatus struct {
	Installed       *bool      `json:"installed"`
	InstanceMode    *string    `json:"instance_mode"`
	SubmittingCount int        `json:"submitting_count"`
	State           string     `json:"state"`
	LastSuccess     *time.Time `json:"last_success"`
	QueuedCount     int        `json:"queued_count"`
	UnknownCount    int        `json:"unknown_count"`
	FailureCategory *string    `json:"failure_category"`
	SyncEnabled     bool       `json:"sync_enabled"`
}
type OutboxItem struct {
	RetryRequestID  *string   `json:"retry_request_id,omitempty"`
	Plan            *SyncPlan `json:"plan,omitempty"`
	RunRequestID    *string   `json:"run_request_id,omitempty"`
	ID              string    `json:"id"`
	Revision        string    `json:"revision"`
	Interval        Interval  `json:"interval"`
	State           string    `json:"state"`
	Correlation     string    `json:"correlation"`
	EntryID         *string   `json:"entry_id"`
	FailureCategory *string   `json:"failure_category"`
}
