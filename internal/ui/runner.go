package ui

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
)

// Screen is the existing terminal owner's presentation and event boundary.
// Run is its sole event consumer and writer, and closes it after joining work.
type Screen interface {
	Next(context.Context) (terminal.Event, error)
	Size() (int, int, error)
	EnterScreen(context.Context) error
	Draw(context.Context, []string) error
	Close() error
}

// SnapshotReader supplies the authoritative shared-engine observation.
type SnapshotReader interface {
	Status(context.Context) (activity.ActivitySnapshot, error)
}

type Options struct {
	Appearance   *themes.Service
	Capabilities themes.Capabilities
	Styler       terminal.Styler
	Views        *ReadViews
	Links        *LinkActions
	Activity     *ActivityActions
	Auth         *AuthActions
	Hooks        *HookActions
	Worker       *WorkerActions
	Sync         *SyncActions
	Setup        *SetupActions
	Diagnostics  func(context.Context, bool) (setup.Diagnostics, error)
	// Outcome callbacks run only after owned work joins and Close has attempted
	// terminal restoration. They must report safe shared observations only.
	OnAuthResult         func(string, auth.Result, error)
	OnSetupResult        func(setup.Status, error)
	OnRetainedOutcome    func(string, string, error)
	OnRestorationFailure func()
	// Refresh is an optional testable refresh source. Nil uses a one-second
	// ticker; closing an injected channel disables further scheduled refreshes.
	Refresh <-chan time.Time
}

// Run presents authoritative timers and explicit shared-service controls. The
// caller supplies Session.Context; opening or refreshing performs local reads.
func Run(ctx context.Context, screen Screen, reader SnapshotReader, options Options) (err error) {
	ctx, cancel := context.WithCancelCause(ctx)
	var workers sync.WaitGroup
	links := &linkController{}
	capture := &activityController{}
	credential := &authController{}
	hooks := &hookController{}
	service := &workerController{}
	uploads := &syncController{}
	wizard := &setupController{}
	appearance := newAppearanceFlow(options.Appearance, options.Capabilities)
	type flowResult struct {
		name  string
		style terminal.Styler
		err   error
	}
	flowDone := make(chan flowResult, 1)
	retained := make(map[string]error)
	remember := func(result flowResult) {
		if unknownOutcome(result.err) {
			retained[result.name] = result.err
		} else {
			delete(retained, result.name)
		}
	}
	bridge := newPromptBridge(ctx)
	var modal *promptModel
	var active *promptRequest
	clearReply := func(request *promptRequest) {
		if request == nil {
			return
		}
		select {
		case reply := <-request.reply:
			clear(reply.secret)
		default:
		}
	}
	defer func() {
		cancel(nil)
		if modal != nil {
			modal.Close()
		}
		workers.Wait()
		select {
		case result := <-flowDone:
			remember(result)
		default:
		}
		// Catchable cancellation and presentation/cleanup failure cannot hide a
		// joined uncertain shared write. Read flows never clear another family.
		if outcome := primaryOutcome(retained); outcome != nil {
			err = outcome
		}
		clearReply(active)
		for {
			select {
			case request := <-bridge.requests:
				clearReply(&request)
			default:
				goto drained
			}
		}
	drained:
		if closeErr := screen.Close(); closeErr != nil {
			var ended *terminal.ExitError
			if err == nil || errors.As(err, &ended) && ended.Code == 0 {
				err = closeErr
			}
			if options.OnRestorationFailure != nil {
				options.OnRestorationFailure()
			}
		}
		if credential.completed != nil && options.OnAuthResult != nil {
			result := credential.completed
			options.OnAuthResult(result.operation, result.result, result.err)
		}
		if wizard.completed != nil && wizard.pending == nil && options.OnSetupResult != nil {
			options.OnSetupResult(wizard.completed.status, wizard.completed.err)
		}
		if options.OnRetainedOutcome != nil || options.OnSetupResult != nil {
			for _, name := range retainedNames(retained) {
				outcome := retained[name]
				if name == "setup" && wizard.completed != nil && options.OnSetupResult != nil {
					options.OnSetupResult(wizard.completed.status, wizard.completed.err)
					continue
				}
				if options.OnRetainedOutcome == nil {
					continue
				}
				id := ""
				var authFailure *auth.Error
				if !errors.As(outcome, &authFailure) {
					switch name {
					case "links":
						if links.pending != nil {
							id = links.id()
						}
					case "activity":
						if capture.pending != nil {
							id = capture.id()
						}
					case "hooks":
						if hooks.pending != nil {
							id = hooks.pending.id()
						}
					case "worker":
						if service.pending != nil {
							id = service.pending.request.RequestID
						}
					case "sync":
						if uploads.pending != nil {
							id = uploads.pending.id()
						}
					case "appearance":
						if appearance.pending != nil {
							id = appearance.pending.RequestID
						}
					case "setup":
						id = setupRequestID(outcome)
					}
				}
				options.OnRetainedOutcome(name, id, outcome)
				if name == "sync" && uploads.pending != nil && uploads.pending.recovery != nil {
					recovery := uploads.pending.recovery
					if unknownOutcome(recovery.err) {
						options.OnRetainedOutcome(name, recovery.id(), recovery.err)
					}
				}
			}
		}
	}()
	columns, rows, err := screen.Size()
	if err != nil {
		return err
	}
	if err = screen.EnterScreen(ctx); err != nil {
		return err
	}
	model := NewModel(columns, rows)
	refreshAppearance := func() {
		if options.Appearance == nil {
			return
		}
		observed, readErr := currentAppearance(ctx, options.Appearance, options.Capabilities)
		if readErr != nil {
			options.Styler, _ = themes.NewStyler("terminal-default", options.Capabilities)
			model.appearanceWarning = "Appearance unavailable; using terminal default"
			return
		}
		options.Styler = observed
		model.appearanceWarning = ""
		if retained["appearance"] != nil {
			model.appearanceWarning = "Appearance outcome unknown; A retries the exact request"
		}
	}
	refreshAppearance()
	draw := func() error {
		outputCtx, end := context.WithTimeout(ctx, 250*time.Millisecond)
		defer end()
		frame := model.Render(options.Styler)
		if modal != nil {
			frame = modal.Render(options.Styler)
		}
		if err := screen.Draw(outputCtx, frame); err != nil {
			if ctx.Err() != nil && errors.Is(err, context.Canceled) {
				return context.Cause(ctx)
			}
			return err
		}
		return nil
	}
	if err = draw(); err != nil {
		return err
	}

	// The terminal owner still owns the raw input pump. This single consumer
	// forwards bounded events so status completion and input remain responsive.
	type inputResult struct {
		event terminal.Event
		err   error
	}
	events := make(chan inputResult, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			event, err := screen.Next(ctx)
			select {
			case events <- inputResult{event, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	type observation struct {
		sequence uint64
		snapshot activity.ActivitySnapshot
		err      error
	}
	requests := make(chan uint64, 1)
	observations := make(chan observation, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case sequence := <-requests:
				if ctx.Err() != nil {
					return
				}
				readCtx, end := context.WithTimeout(ctx, 250*time.Millisecond)
				snapshot, err := reader.Status(readCtx)
				end()
				select {
				case observations <- observation{sequence, snapshot, err}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	refresh := options.Refresh
	if refresh == nil {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		refresh = ticker.C
	}
	var sequence uint64
	busy, pending := false, false
	request := func() {
		if busy {
			pending = true
			return
		}
		sequence++
		busy = true
		requests <- sequence
	}
	request()
	flowBusy := false
	var endFlow context.CancelFunc
	startFlow := func(name string, run func(context.Context) (terminal.Styler, error)) {
		if flowBusy {
			return
		}
		flowCtx, end := context.WithTimeout(ctx, 2*time.Minute)
		endFlow, flowBusy = end, true
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer end()
			style, flowErr := run(flowCtx)
			flowDone <- flowResult{name, style, flowErr}
		}()
	}
	startView := func(view localView) {
		snapshot := model.snapshot
		key, selected := model.SelectedKey()
		startFlow("read", func(flowCtx context.Context) (terminal.Styler, error) {
			return nil, showView(flowCtx, view, snapshot, key, selected, bridge, options.Views)
		})
	}
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case request := <-bridge.requests:
			if request.ctx.Err() != nil {
				clearReply(&request)
				continue
			}
			active = &request
			if modal != nil {
				modal.Close()
			}
			modal = newPromptModel(request, columns, rows)
			if err = draw(); err != nil {
				return err
			}
		case result := <-flowDone:
			remember(result)
			if result.err == nil && result.style != nil {
				options.Styler = result.style
			}
			if result.name == "links" && links.pending == nil {
				request()
			}
			if result.name == "activity" && capture.pending == nil {
				request()
			}
			if result.name == "hooks" && hooks.pending == nil || result.name == "worker" && service.pending == nil || result.name == "sync" && uploads.pending == nil {
				request()
			}
			if result.name == "appearance" {
				// Pure saved metadata refresh does not acknowledge durability or
				// alter the retained mutation outcome or its pending identity.
				refreshAppearance()
				if ctx.Err() != nil {
					return context.Cause(ctx)
				}
				if result.err != nil && !unknownOutcome(result.err) && !appearanceCancelled(result.err) {
					return result.err
				}
			}
			flowBusy = false
			endFlow = nil
			if modal != nil {
				modal.Close()
				modal = nil
			}
			clearReply(active)
			active = nil
			if err = draw(); err != nil {
				return err
			}
		case _, open := <-refresh:
			if !open {
				refresh = nil
				continue
			}
			request()
		case result := <-observations:
			busy = false
			if result.err != nil {
				model.MarkStale(result.sequence, "Local activity unavailable; r to retry")
			} else if !model.ApplySnapshot(result.sequence, result.snapshot) {
				model.MarkStale(result.sequence, "Local activity observation unavailable; r to retry")
			}
			if err = draw(); err != nil {
				return err
			}
			if pending {
				pending = false
				request()
			}
		case result := <-events:
			if result.err != nil {
				return result.err
			}
			event := result.event
			if event.Kind == "resize" {
				columns, rows = event.Columns, event.Rows
				model.Resize(columns, rows)
				if modal != nil {
					modal.Resize(columns, rows)
				}
				if err = draw(); err != nil {
					return err
				}
				continue
			}
			if modal != nil {
				if modal.done {
					if event.Kind == "escape" {
						endFlow()
						modal.Close()
					}
					if event.Kind == "text" && event.Text == "q" {
						return nil
					}
					continue
				}
				reply, done := modal.Handle(event)
				if done {
					if active.ctx.Err() == nil && ctx.Err() == nil {
						select {
						case active.reply <- reply:
						default:
							clear(reply.secret)
						}
					} else {
						clear(reply.secret)
					}
					if event.Kind == "escape" {
						endFlow()
						modal.Close()
					}
					// Keep the completed frame until the service either opens
					// its next prompt or finishes the flow.
					// Retain the reply channel until the flow has consumed or
					// abandoned it; teardown clears any private bytes.
					// The dashboard is shown only after the flow finishes, so
					// visible navigation always accepts the next explicit view.
					continue
				}
				if err = draw(); err != nil {
					return err
				}
				continue
			}
			switch event.Kind {
			case "escape":
				if flowBusy {
					endFlow()
					continue
				}
				return nil
			case "enter":
				startView(timerDetails)
			case "text":
				switch event.Text {
				case "A":
					startFlow("appearance", func(flowCtx context.Context) (terminal.Styler, error) {
						if options.Appearance == nil {
							return nil, bridge.View(flowCtx, "Appearance", "Appearance unavailable.")
						}
						return appearance.run(flowCtx, bridge)
					})
				case "q":
					return nil
				case "r":
					request()
				case "2":
					startView(linksView)
				case "3":
					startView(syncView)
				case ",":
					startFlow("setup", func(flowCtx context.Context) (terminal.Styler, error) {
						actions := options.Setup
						if wizard.pending == nil && (credential.pending != nil || links.pending != nil || hooks.pending != nil) && actions != nil {
							copy := *actions
							copy.Run = nil
							actions = &copy
						}
						return nil, wizard.run(flowCtx, bridge, actions, options.Views)
					})
				case "?":
					startView(helpView)
				case "l":
					startFlow("links", func(flowCtx context.Context) (terminal.Styler, error) {
						actions := options.Links
						if wizard.pending != nil && links.pending == nil && actions != nil {
							copy := *actions
							copy.Prepare, copy.Commit, copy.Unlink, copy.Repair = nil, nil, nil, nil
							actions = &copy
						}
						return nil, links.run(flowCtx, bridge, options.Views, actions)
					})
				case "x":
					startFlow("activity", func(flowCtx context.Context) (terminal.Styler, error) {
						return nil, capture.run(flowCtx, bridge, options.Activity)
					})
				case "a":
					startFlow("auth", func(flowCtx context.Context) (terminal.Styler, error) {
						actions := options.Auth
						if wizard.pending != nil && credential.pending == nil && actions != nil {
							copy := *actions
							copy.PrepareLogin, copy.CommitLogin, copy.UseAccount, copy.Logout = nil, nil, nil, nil
							copy.Accounts = nil
							actions = &copy
						}
						return nil, credential.run(flowCtx, bridge, actions)
					})
				case "h":
					startFlow("hooks", func(flowCtx context.Context) (terminal.Styler, error) {
						actions := options.Hooks
						if wizard.pending != nil && hooks.pending == nil && actions != nil {
							copy := *actions
							copy.ApplyInstall, copy.ConfirmInstalled, copy.Revoke = nil, nil, nil
							actions = &copy
						}
						return nil, hooks.run(flowCtx, bridge, actions)
					})
				case "w":
					startFlow("worker", func(flowCtx context.Context) (terminal.Styler, error) {
						return nil, service.run(flowCtx, bridge, options.Worker)
					})
				case "d":
					startFlow("diagnostics", func(flowCtx context.Context) (terminal.Styler, error) {
						return nil, showDiagnostics(flowCtx, bridge, options.Diagnostics)
					})
				case "s":
					startFlow("sync", func(flowCtx context.Context) (terminal.Styler, error) {
						return nil, uploads.run(flowCtx, bridge, options.Sync)
					})
				}
				continue
			case "up":
				model.Move(-1)
			case "down":
				model.Move(1)
			case "resize":
				model.Resize(event.Columns, event.Rows)
			default:
				// Paste is data, never navigation or a request to act.
				continue
			}
			if err = draw(); err != nil {
				return err
			}
		}
	}
}
