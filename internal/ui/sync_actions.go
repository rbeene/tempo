package ui

import (
	"context"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/harvest"
)

// SyncActions delegates immutable upload consent and recovery to the shared
// engine. Remote ambiguity never authorizes another POST.
type SyncActions struct {
	Accounts  func(context.Context) ([]harvest.Object, error)
	Identity  func(context.Context, string) (activity.SyncAccountIdentity, error)
	Status    func(context.Context) (activity.SyncStatus, error)
	Configure func(context.Context, activity.SyncConfigureInput) (activity.SyncConfigurationResult, error)
	Now       func(context.Context, activity.SyncRunInput) (activity.SyncRun, error)
	Reconcile func(context.Context, activity.SyncReconcileInput) (activity.SyncRun, error)
	Resolve   func(context.Context, activity.SyncResolveInput) (activity.MutationResult, error)
	Pause     func(context.Context, string) (activity.MutationResult, error)
	Resume    func(context.Context, string) (activity.MutationResult, error)
}

type syncPending struct {
	operation string
	configure activity.SyncConfigureInput
	run       activity.SyncRunInput
	reconcile activity.SyncReconcileInput
	resolve   activity.SyncResolveInput
	requestID string
	err       error
}

type syncController struct{ pending *syncPending }

func (c *syncController) run(ctx context.Context, p *promptBridge, actions *SyncActions) error {
	return nil
}
