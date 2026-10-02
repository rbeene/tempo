package ui

import (
	"context"

	"github.com/rbeene/tempo/internal/activity"
)

// ActivityActions uses the existing capture/recovery engine. Timing decisions
// and interval projections remain authoritative shared-service results.
type ActivityActions struct {
	Status    func(context.Context) (activity.ActivitySnapshot, error)
	Review    func(context.Context, activity.ReviewInput) (activity.ReviewList, error)
	Preview   func(context.Context, activity.RecoveryInput) (activity.RecoveryPreview, error)
	Resolve   func(context.Context, activity.ResolveInput) (activity.MutationResult, error)
	Interrupt func(context.Context, activity.InterruptInput) (activity.MutationResult, error)
}

type activityPending struct {
	operation string
	interrupt activity.InterruptInput
	resolve   activity.ResolveInput
	err       error
}

type activityController struct{ pending *activityPending }

func (c *activityController) run(ctx context.Context, p *promptBridge, actions *ActivityActions) error {
	return nil
}
