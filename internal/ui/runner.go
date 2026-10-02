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
	// Refresh is an optional testable refresh source. Nil uses a one-second
	// ticker; closing an injected channel disables further scheduled refreshes.
	Refresh <-chan time.Time
}

// Run presents read-only project timers. The caller supplies Session.Context.
func Run(ctx context.Context, screen Screen, reader SnapshotReader, options Options) (err error) {
	ctx, cancel := context.WithCancelCause(ctx)
	var workers sync.WaitGroup
	defer func() {
		cancel(nil)
		workers.Wait()
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
		if err := screen.Draw(outputCtx, model.Render(options.Styler)); err != nil {
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
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
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
			switch event := result.event; event.Kind {
			case "escape":
				return nil
			case "text":
				switch event.Text {
				case "q":
					return nil
				case "r":
					request()
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
