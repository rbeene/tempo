package ui

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
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
	Styler terminal.Styler
	Views  *ReadViews
	Links  *LinkActions
	// Refresh is an optional testable refresh source. Nil uses a one-second
	// ticker; closing an injected channel disables further scheduled refreshes.
	Refresh <-chan time.Time
}

// Run presents read-only project timers. The caller supplies Session.Context.
func Run(ctx context.Context, screen Screen, reader SnapshotReader, options Options) (err error) {
	ctx, cancel := context.WithCancelCause(ctx)
	var workers sync.WaitGroup
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
		for _, outcome := range retained {
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
	links := &linkController{}
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
						modal = nil
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
						modal = nil
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
				case "q":
					return nil
				case "r":
					request()
				case "2":
					startView(linksView)
				case "3":
					startView(syncView)
				case ",":
					startView(setupView)
				case "?":
					startView(helpView)
				case "l":
					startFlow("links", func(flowCtx context.Context) (terminal.Styler, error) {
						return nil, links.run(flowCtx, bridge, options.Views, options.Links)
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
