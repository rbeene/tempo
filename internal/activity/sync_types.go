package activity

import "time"

// SyncDependencies resolves credentials lazily for the persisted account.
type SyncDependencies struct{ NewProvider AccountProvider }
type SyncConfigureInput struct {
	AccountID, Mode, DurationPolicy, Clock, IfRevision, RequestID string
	Confirmed                                                     bool
}
type SyncRunInput struct {
	RequestID string `json:"request_id"`
	Limit     int    `json:"limit"`
}
type SyncReconcileInput struct {
	RequestID string `json:"request_id"`
	OutboxID  string `json:"outbox_id"`
	Limit     int    `json:"limit"`
}
type SyncResolveInput struct {
	RequestID     string `json:"request_id"`
	OutboxID      string `json:"outbox_id"`
	EntryID       string `json:"entry_id"`
	IfRevision    string `json:"if_revision"`
	RetryRejected bool   `json:"retry_rejected"`
	Confirmed     bool   `json:"confirmed"`
}
type SyncConfiguration struct {
	AccountID      string    `json:"account_id"`
	UserID         string    `json:"user_id"`
	Revision       string    `json:"revision"`
	Mode           string    `json:"mode"`
	DurationPolicy string    `json:"duration_policy"`
	PolicyVersion  string    `json:"policy_version"`
	Clock          *string   `json:"clock"`
	Declared       bool      `json:"declared"`
	DeclaredAt     time.Time `json:"declared_at"`
	Source         string    `json:"source"`
}
type SyncConfigurationResult struct {
	ContractVersion  int               `json:"contract_version"`
	SnapshotRevision string            `json:"snapshot_revision"`
	RequestID        string            `json:"request_id"`
	Changed          bool              `json:"changed"`
	Configuration    SyncConfiguration `json:"configuration"`
}
type SyncStatus struct {
	ContractVersion  int                 `json:"contract_version"`
	SnapshotRevision string              `json:"snapshot_revision"`
	Enabled          bool                `json:"enabled"`
	Configurations   []SyncConfiguration `json:"configurations"`
	Items            []OutboxItem        `json:"items"`
	Worker           WorkerStatus        `json:"worker"`
	Totals           SyncTotals          `json:"totals"`
	Accounting       []SyncAccounting    `json:"accounting"`
}
type SyncRun struct {
	ContractVersion  int      `json:"contract_version"`
	RequestID        string   `json:"request_id"`
	SnapshotRevision string   `json:"snapshot_revision"`
	State            string   `json:"state"`
	AttemptedIDs     []string `json:"attempted_ids"`
	ResolvedIDs      []string `json:"resolved_ids"`
	BlockedIDs       []string `json:"blocked_ids"`
	RemainingCount   int      `json:"remaining_count"`
}
type SyncTotals struct {
	ExactDurationNS     string  `json:"exact_duration_ns"`
	PlannedDurationNS   *string `json:"planned_duration_ns"`
	ConfirmedDurationNS *string `json:"confirmed_duration_ns"`
	PlannedResidualNS   *string `json:"planned_residual_ns"`
	TotalResidualNS     *string `json:"total_residual_ns"`
}
type SyncAccounting struct {
	Scope       string      `json:"scope"`
	Attribution Attribution `json:"attribution"`
	Date        *string     `json:"date"`
	Totals      SyncTotals  `json:"totals"`
}
type SyncPlan struct {
	Configuration SyncConfiguration `json:"configuration"`
	CompanySource string            `json:"company_source"`
	Parts         []SyncPart        `json:"parts"`
}
type SyncAttachment struct {
	RequestID string `json:"request_id"`
	EntryID   string `json:"entry_id"`
}
type SyncPart struct {
	Attachment          *SyncAttachment `json:"attachment,omitempty"`
	ID                  string          `json:"id"`
	SpentDate           string          `json:"spent_date"`
	DurationNS          string          `json:"duration_ns"`
	Start               time.Time       `json:"start"`
	End                 time.Time       `json:"end"`
	PlannedHours        string          `json:"planned_hours"`
	PlannedDurationNS   string          `json:"planned_duration_ns"`
	PlannedResidualNS   string          `json:"planned_residual_ns"`
	StartedTime         *string         `json:"started_time"`
	EndedTime           *string         `json:"ended_time"`
	Correlation         string          `json:"correlation"`
	Notes               string          `json:"notes"`
	State               string          `json:"state"`
	EntryID             *string         `json:"entry_id"`
	FailureCategory     *string         `json:"failure_category"`
	ReturnedHours       *string         `json:"returned_hours"`
	RoundedHours        *string         `json:"rounded_hours"`
	ConfirmedDurationNS *string         `json:"confirmed_duration_ns"`
	ProviderDeltaNS     *string         `json:"provider_delta_ns"`
	TotalResidualNS     *string         `json:"total_residual_ns"`
	Attempts            []SyncAttempt   `json:"attempts"`
}
type SyncAttempt struct {
	RequestID       string  `json:"request_id"`
	ID              string  `json:"id"`
	Number          string  `json:"number"`
	State           string  `json:"state"`
	EntryID         *string `json:"entry_id"`
	FailureCategory *string `json:"failure_category"`
}

// syncReservation fixes a bounded operation's targets before external I/O.
type syncReservation struct {
	EffectCommitted  bool                `json:"effect_committed,omitempty"`
	Run              *SyncRunInput       `json:"run,omitempty"`
	Reconcile        *SyncReconcileInput `json:"reconcile,omitempty"`
	Resolve          *SyncResolveInput   `json:"resolve,omitempty"`
	RootIDs          []string            `json:"root_ids"`
	SnapshotRevision string              `json:"snapshot_revision"`
}
