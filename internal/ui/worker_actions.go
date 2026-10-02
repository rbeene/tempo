package ui

import (
	"context"

	"github.com/rbeene/tempo/internal/worker"
)

// WorkerActions controls the independently owned shared worker service. The UI
// never starts a foreground runtime or stops a worker as terminal cleanup.
type WorkerActions struct {
	Status    func(context.Context) (worker.Result, error)
	Install   func(context.Context, worker.ControlRequest) (worker.Result, error)
	Start     func(context.Context, worker.ControlRequest) (worker.Result, error)
	Stop      func(context.Context, worker.ControlRequest) (worker.Result, error)
	Uninstall func(context.Context, worker.ControlRequest) (worker.Result, error)
}

type workerPending struct {
	operation string
	request   worker.ControlRequest
	err       error
}

type workerController struct{ pending *workerPending }

func (c *workerController) run(ctx context.Context, p *promptBridge, actions *WorkerActions) error {
	return nil
}
