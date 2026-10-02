package ui_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
)

type qaRunnerReader struct {
	read                   func(context.Context) (activity.ActivitySnapshot, error)
	active, maximum, calls atomic.Int32
}

func (r *qaRunnerReader) Status(ctx context.Context) (activity.ActivitySnapshot, error) {
	n := r.active.Add(1)
	defer r.active.Add(-1)
	r.calls.Add(1)
	for old := r.maximum.Load(); n > old; old = r.maximum.Load() {
		if r.maximum.CompareAndSwap(old, n) {
			break
		}
	}
	return r.read(ctx)
}

type qaRunnerScreen struct {
	events                                  chan terminal.Event
	frames                                  chan []string
	nextActive, nextMaximum, closes, enters atomic.Int32
	mu                                      sync.Mutex
	enterErr, drawErr, sizeErr, closeErr    error
	reader                                  *qaRunnerReader
	closeBeforeJoin                         bool
}

func qaNewRunnerScreen() *qaRunnerScreen {
	return &qaRunnerScreen{events: make(chan terminal.Event, 128), frames: make(chan []string, 128)}
}
func (s *qaRunnerScreen) Next(ctx context.Context) (terminal.Event, error) {
	n := s.nextActive.Add(1)
	defer s.nextActive.Add(-1)
	for old := s.nextMaximum.Load(); n > old; old = s.nextMaximum.Load() {
		if s.nextMaximum.CompareAndSwap(old, n) {
			break
		}
	}
	select {
	case e := <-s.events:
		return e, nil
	case <-ctx.Done():
		return terminal.Event{}, context.Cause(ctx)
	}
}
func (s *qaRunnerScreen) Size() (int, int, error)           { return 120, 24, s.sizeErr }
func (s *qaRunnerScreen) EnterScreen(context.Context) error { s.enters.Add(1); return s.enterErr }
func (s *qaRunnerScreen) Draw(ctx context.Context, f []string) error {
	if s.drawErr != nil {
		return s.drawErr
	}
	select {
	case s.frames <- append([]string(nil), f...):
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func (s *qaRunnerScreen) Close() error {
	s.closes.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeBeforeJoin = s.nextActive.Load() != 0 || s.reader != nil && s.reader.active.Load() != 0
	return s.closeErr
}

type qaRunnerRig struct {
	screen *qaRunnerScreen
	reader *qaRunnerReader
	ticks  chan time.Time
	done   chan error
	cancel context.CancelCauseFunc
}

func qaStartRunner(t *testing.T, s *qaRunnerScreen, r *qaRunnerReader, refresh <-chan time.Time) *qaRunnerRig {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	x := &qaRunnerRig{screen: s, reader: r, done: make(chan error, 1), cancel: cancel}
	s.reader = r
	go func() { x.done <- ui.Run(ctx, s, r, ui.Options{Refresh: refresh}) }()
	t.Cleanup(func() {
		cancel(context.Canceled)
		select {
		case <-x.done:
		case <-time.After(time.Second):
			t.Error("runner owned work survived cancellation")
		}
		if s.closes.Load() != 1 {
			t.Errorf("Close calls=%d, want one", s.closes.Load())
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.closeBeforeJoin {
			t.Error("Close ran before input/status workers joined")
		}
	})
	return x
}
func (x *qaRunnerRig) frame(t *testing.T, contains string) []string {
	t.Helper()
	deadline := time.NewTimer(1500 * time.Millisecond)
	defer deadline.Stop()
	for {
		select {
		case f := <-x.screen.frames:
			if strings.Contains(strings.Join(f, "\n"), contains) {
				return f
			}
		case err := <-x.done:
			x.done <- err
			t.Fatalf("runner returned before frame %q: %v", contains, err)
		case <-deadline.C:
			t.Fatalf("no frame containing %q", contains)
		}
	}
}
func (x *qaRunnerRig) finish(t *testing.T, want error) {
	t.Helper()
	select {
	case err := <-x.done:
		x.done <- err
		if !errors.Is(err, want) {
			t.Fatalf("runner exit=%v want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("quit/cancellation did not complete within 1s")
	}
}

func TestQAUIRunnerNavigationResizeAndQuit(t *testing.T) {
	for _, quit := range []terminal.Event{{Kind: "text", Text: "q"}, {Kind: "escape"}} {
		t.Run(quit.Kind, func(t *testing.T) {
			s := qaNewRunnerScreen()
			r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("1", "100", "200"), nil }}
			ticks := make(chan time.Time)
			x := qaStartRunner(t, s, r, ticks)
			x.frame(t, "> Project 100")
			s.events <- terminal.Event{Kind: "down"}
			x.frame(t, "> Project 200")
			s.events <- terminal.Event{Kind: "up"}
			x.frame(t, "> Project 100")
			s.events <- terminal.Event{Kind: "resize", Columns: 20, Rows: 5}
			f := x.frame(t, "too small")
			qaUIFrame(t, f, 20, 5)
			s.events <- terminal.Event{Kind: "resize", Columns: 80, Rows: 24}
			qaUIFrame(t, x.frame(t, "> Project 100"), 80, 24)
			s.events <- quit
			x.finish(t, nil)
			if r.calls.Load() != 1 || s.nextMaximum.Load() != 1 || s.enters.Load() != 1 {
				t.Errorf("read-only navigation ownership: calls=%d Next concurrency=%d entries=%d", r.calls.Load(), s.nextMaximum.Load(), s.enters.Load())
			}
		})
	}
}
func TestQAUIRunnerSameRevisionRefreshStaleFreezeAndRecovery(t *testing.T) {
	s := qaNewRunnerScreen()
	var n atomic.Int32
	r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) {
		i := n.Add(1)
		snap := qaUISnapshot("10", "100")
		switch i {
		case 1:
		case 2:
			snap.ProjectTimers[0].ProvisionalUnionNS = "11000000000"
		case 3:
			return activity.ActivitySnapshot{}, errors.New("PRIVATE transport body \x1b]52;c;SECRET\a")
		default:
			snap.ProjectTimers[0].ProvisionalUnionNS = "12000000000"
		}
		return snap, nil
	}}
	ticks := make(chan time.Time, 4)
	x := qaStartRunner(t, s, r, ticks)
	x.frame(t, "00:00:10")
	s.events <- terminal.Event{Kind: "text", Text: "r"}
	x.frame(t, "00:00:11")
	ticks <- time.Now()
	f := x.frame(t, "Stale")
	text := strings.Join(f, "\n")
	if !strings.Contains(text, "00:00:11") || strings.Contains(text, "PRIVATE") || strings.Contains(text, "SECRET") {
		t.Errorf("failed refresh lost frozen value or exposed raw error: %q", text)
	}
	s.events <- terminal.Event{Kind: "text", Text: "r"}
	f = x.frame(t, "00:00:12")
	if strings.Contains(strings.Join(f, "\n"), "Stale") {
		t.Error("success did not clear stale marker")
	}
	s.events <- terminal.Event{Kind: "text", Text: "q"}
	x.finish(t, nil)
}
func TestQAUIRunnerReadBudgetCoalescingAndJoin(t *testing.T) {
	s := qaNewRunnerScreen()
	started := make(chan time.Duration, 4)
	release := make(chan struct{})
	var n atomic.Int32
	r := &qaRunnerReader{read: func(ctx context.Context) (activity.ActivitySnapshot, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			started <- time.Hour
		} else {
			started <- time.Until(deadline)
		}
		i := n.Add(1)
		if i == 1 {
			select {
			case <-release:
			case <-ctx.Done():
				return activity.ActivitySnapshot{}, ctx.Err()
			}
		}
		return qaUISnapshot("1", "100"), nil
	}}
	ticks := make(chan time.Time)
	x := qaStartRunner(t, s, r, ticks)
	select {
	case budget := <-started:
		if budget <= 0 || budget > 250*time.Millisecond {
			t.Errorf("Status budget=%v, want <=250ms", budget)
		}
	case <-time.After(time.Second):
		t.Fatal("initial shared Status was not called")
	}
	for i := 0; i < 40; i++ {
		select {
		case ticks <- time.Now():
		case <-time.After(time.Second):
			t.Fatal("event loop blocked during pending Status")
		}
	}
	s.events <- terminal.Event{Kind: "text", Text: "r"}
	time.Sleep(30 * time.Millisecond)
	if r.calls.Load() != 1 {
		t.Errorf("started overlapping read: %d", r.calls.Load())
	}
	close(release)
	x.frame(t, "Project 100")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("coalesced refresh was lost")
	}
	time.Sleep(50 * time.Millisecond)
	if r.calls.Load() != 2 || r.maximum.Load() != 1 {
		t.Errorf("refresh burst should yield one pending read: calls=%d concurrency=%d", r.calls.Load(), r.maximum.Load())
	}
	s.events <- terminal.Event{Kind: "text", Text: "q"}
	x.finish(t, nil)
}
func TestQAUIRunnerTimeoutAndQuitCancelPendingRead(t *testing.T) {
	for _, mode := range []string{"timeout", "quit", "cause"} {
		t.Run(mode, func(t *testing.T) {
			s := qaNewRunnerScreen()
			started := make(chan struct{}, 1)
			r := &qaRunnerReader{read: func(ctx context.Context) (activity.ActivitySnapshot, error) {
				started <- struct{}{}
				<-ctx.Done()
				return activity.ActivitySnapshot{}, ctx.Err()
			}}
			x := qaStartRunner(t, s, r, make(chan time.Time))
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("initial read missing")
			}
			var want error
			if mode == "timeout" {
				x.frame(t, "Local activity unavailable")
				s.events <- terminal.Event{Kind: "text", Text: "q"}
			} else if mode == "quit" {
				s.events <- terminal.Event{Kind: "text", Text: "q"}
			} else {
				want = &terminal.ExitError{Code: 143}
				x.cancel(want)
			}
			x.finish(t, want)
		})
	}
}
func TestQAUIRunnerPasteAndClosedRefreshDoNotActOrSpin(t *testing.T) {
	s := qaNewRunnerScreen()
	r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("1", "100"), nil }}
	ticks := make(chan time.Time)
	close(ticks)
	x := qaStartRunner(t, s, r, ticks)
	x.frame(t, "Project 100")
	s.events <- terminal.Event{Kind: "paste", Text: "qr\x1b[A\x03"}
	time.Sleep(80 * time.Millisecond)
	select {
	case err := <-x.done:
		x.done <- err
		t.Fatalf("paste payload quit runner: %v", err)
	default:
	}
	if r.calls.Load() != 1 {
		t.Errorf("paste/closed ticks triggered refresh loop: %d", r.calls.Load())
	}
	s.events <- terminal.Event{Kind: "text", Text: "q"}
	x.finish(t, nil)
}
func TestQAUIRunnerStartupOutputAndCleanupFailures(t *testing.T) {
	for _, step := range []string{"size", "enter", "draw", "close"} {
		t.Run(step, func(t *testing.T) {
			s := qaNewRunnerScreen()
			want := errors.New("synthetic " + step + " failure")
			switch step {
			case "size":
				s.sizeErr = want
			case "enter":
				s.enterErr = want
			case "draw":
				s.drawErr = want
			case "close":
				s.closeErr = want
			}
			r := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("1"), nil }}
			x := qaStartRunner(t, s, r, make(chan time.Time))
			if step == "close" {
				x.frame(t, "No activity")
				s.events <- terminal.Event{Kind: "text", Text: "q"}
			}
			x.finish(t, want)
		})
	}
}
