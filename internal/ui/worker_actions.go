package ui

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rbeene/tempo/internal/terminal"
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
	if c.pending != nil {
		return c.recover(ctx, p, actions)
	}
	if actions == nil {
		return p.View(ctx, "Worker controls", "Worker controls unavailable.")
	}
	op, err := p.Choose(ctx, "Worker controls", []terminal.Choice{{ID: "status", Label: "Read-only worker status"}, {ID: "install", Label: "Install user service without starting"}, {ID: "start", Label: "Start worker and enable login startup"}, {ID: "stop", Label: "Stop worker and disable login startup"}, {ID: "uninstall", Label: "Remove owned user service"}, {ID: "back", Label: "Back"}})
	if err != nil || op == "back" {
		return err
	}
	if op == "status" {
		if actions.Status == nil {
			return p.View(ctx, "Worker status", "Read-only worker status unavailable.")
		}
		result, err := observeView(ctx, actions.Status)
		if err != nil {
			return workerFailure(ctx, p, err)
		}
		return p.View(ctx, "Worker · Read-only status", workerDetails(result))
	}
	if workerAction(actions, op) == nil {
		return p.View(ctx, "Worker controls", "Requested worker control unavailable.")
	}
	confirmed := op == "install" || op == "uninstall"
	intent := &workerPending{operation: op, request: worker.ControlRequest{RequestID: requestID(), Confirmed: confirmed}}
	if confirmed {
		body := "Install an owned user service without starting the worker? Explicit Start enables login startup and launches it. Installation does not enable uploads or grant upload consent. Local activity, history and pending sync requests remain retained."
		if op == "uninstall" {
			body = "Uninstall the unchanged owned user service? The shared controller disables startup and removes only its owned definition. Local activity, history, pending sync identities and control receipts remain retained. Upload consent remains separate; this does not pause sync or interrupt capture."
		}
		yes, err := p.Confirm(ctx, body+"\nRequest ID "+intent.request.RequestID)
		if err != nil || !yes {
			return err
		}
	}
	c.pending = intent
	result, err := c.dispatch(ctx, actions)
	if err == nil {
		return c.complete(ctx, p, result)
	}
	if !unknownOutcome(err) {
		c.pending = nil
		return workerFailure(ctx, p, err)
	}
	c.pending.err = err
	if p.View(ctx, "Worker · Outcome unknown", c.unknownDetails()) != nil {
		return c.pending.err
	}
	return c.recover(ctx, p, actions)
}

func workerAction(actions *WorkerActions, operation string) func(context.Context, worker.ControlRequest) (worker.Result, error) {
	if actions != nil {
		switch operation {
		case "install":
			return actions.Install
		case "start":
			return actions.Start
		case "stop":
			return actions.Stop
		case "uninstall":
			return actions.Uninstall
		}
	}
	return nil
}
func (c *workerController) dispatch(ctx context.Context, actions *WorkerActions) (worker.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if ctx.Err() != nil {
		return worker.Result{}, context.Cause(ctx)
	}
	if action := workerAction(actions, c.pending.operation); action != nil {
		return action(ctx, c.pending.request)
	}
	return worker.Result{}, &worker.Error{Code: "unsupported"}
}
func (c *workerController) complete(ctx context.Context, p *promptBridge, result worker.Result) error {
	id := c.pending.request.RequestID
	c.pending = nil
	return p.View(ctx, "Worker · Complete", "Request ID "+id+"\n"+workerDetails(result))
}
func (c *workerController) unknownDetails() string {
	return "local_write_unknown: the submitted worker outcome or durability is unknown.\nOperation " + c.pending.operation + "\nRequest ID " + c.pending.request.RequestID + "\nPreserve the same operation, consent and request identity. Read-only status does not prove nonapplication. Only explicit same-intent replay can establish completion; a new request is not recovery."
}
func (c *workerController) recover(ctx context.Context, p *promptBridge, actions *WorkerActions) error {
	for {
		id, err := p.Choose(ctx, "Worker · Recover pending request", []terminal.Choice{{ID: "replay", Label: c.pending.request.RequestID + " · Replay exact intent"}, {ID: "status", Label: "Read-only status"}, {ID: "back", Label: "Back; retain pending request"}})
		if err != nil || id == "back" {
			return c.pending.err
		}
		switch id {
		case "status":
			body := c.unknownDetails() + "\nRead-only worker status unavailable."
			if actions != nil && actions.Status != nil {
				result, err := observeView(ctx, actions.Status)
				if err == nil {
					body = c.unknownDetails() + "\nRead-only observation:\n" + workerDetails(result)
				}
			}
			if p.View(ctx, "Worker · Read-only status", body) != nil {
				return c.pending.err
			}
		case "replay":
			result, err := c.dispatch(ctx, actions)
			if err == nil {
				return c.complete(ctx, p, result)
			}
			if p.View(ctx, "Worker · Outcome unknown", c.unknownDetails()+"\nThe exact replay did not establish completion; no replacement request was sent. The requested action may be unavailable.") != nil {
				return c.pending.err
			}
		default:
			return c.pending.err
		}
	}
}
func workerDetails(result worker.Result) string {
	status := result.Status
	installed := "unknown"
	if status.Installed != nil {
		installed = fmt.Sprint(*status.Installed)
	}
	lastSuccess := "unknown"
	if status.LastSuccess != nil {
		lastSuccess = status.LastSuccess.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("State %s\nInstalled %s · Instance mode %s\nSync enabled %t\nQueued %d · Unknown %d · Submitting %d\nLast success %s\nFailure category %s\nThe worker is independently owned. Quitting the UI does not stop it. Capture, upload consent and worker ownership are separate.", status.State, installed, optionalCounter(status.InstanceMode), status.SyncEnabled, status.QueuedCount, status.UnknownCount, status.SubmittingCount, lastSuccess, optionalCounter(status.FailureCategory))
}
func workerFailure(ctx context.Context, p *promptBridge, err error) error {
	var ended *terminal.ExitError
	if errors.As(err, &ended) {
		return err
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	code := "operation_failed"
	var domain *worker.Error
	if errors.As(err, &domain) {
		switch domain.Code {
		case "validation", "confirmation_required", "request_conflict", "revision_conflict", "state_corrupt", "state_busy", "control_history_full", "unsupported", "manager":
			code = domain.Code
		}
	}
	return p.View(ctx, "Worker · Failed", code+"\nReview current worker status and the owned service definition before choosing a new operation with a new request identity. Renew installation or removal confirmation. No automatic retry follows this definite failure.")
}
