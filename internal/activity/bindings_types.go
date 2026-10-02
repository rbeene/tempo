package activity

import (
	"context"
	"github.com/rbeene/tempo/internal/harvest"
)

// Location describes the local scope of an explicit link. Repository locators
// are common Git directories shared by every worktree of that repository.
type Location struct {
	Kind    string `json:"kind"`
	Locator string `json:"locator"`
	Root    string `json:"root"`
	Path    string `json:"path"`
}
type Binding struct {
	ID             string      `json:"id"`
	Revision       string      `json:"revision"`
	Kind           string      `json:"kind"`
	Locator        string      `json:"locator"`
	Attribution    Attribution `json:"attribution"`
	AttachedActors []ActorRef  `json:"attached_actors"`
}
type BindingList struct {
	ContractVersion  int       `json:"contract_version"`
	SnapshotRevision string    `json:"snapshot_revision"`
	Bindings         []Binding `json:"bindings"`
}
type BindingResult struct {
	ContractVersion  int     `json:"contract_version"`
	SnapshotRevision string  `json:"snapshot_revision"`
	RequestID        string  `json:"request_id"`
	Changed          bool    `json:"changed"`
	Binding          Binding `json:"binding"`
}
type MutationResult struct {
	ContractVersion  int      `json:"contract_version"`
	SnapshotRevision string   `json:"snapshot_revision"`
	RequestID        string   `json:"request_id"`
	Changed          bool     `json:"changed"`
	AffectedIDs      []string `json:"affected_ids"`
	EntityRevision   *string  `json:"entity_revision"`
}
type LinkInput struct{ ProjectID, TaskID, Path, AccountID, Timezone, IfRevision, RequestID string }
type ShowBindingInput struct{ BindingID, Path string }
type UnlinkInput struct {
	BindingID, IfRevision, RequestID string
	Confirmed                        bool
}
type RepairBindingInput struct {
	BindingID, Path, IfRevision, RequestID string
	Confirmed                              bool
}
type AccountProvider func(context.Context, string) (harvest.Provider, error)

// LinkDependencies is lazy: an exact durable replay calls neither function.
// The service supplies the selected account to NewProvider, preventing an
// independently supplied provider and attribution account from disagreeing.
type LinkDependencies struct {
	ResolveAccount func(context.Context) (string, error)
	NewProvider    AccountProvider
}
