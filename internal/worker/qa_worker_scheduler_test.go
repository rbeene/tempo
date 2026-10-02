package worker

import (
	"context"
	"errors"
	"github.com/rbeene/tempo/internal/activity"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type qaWorkerTimer struct {
	clock   *qaWorkerClock
	at      time.Time
	ch      chan time.Time
	stopped bool
}

func (t *qaWorkerTimer) C() <-chan time.Time { return t.ch }
func (t *qaWorkerTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	old := t.stopped
	t.stopped = true
	return !old
}

type qaWorkerClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*qaWorkerTimer
	created chan time.Duration
}

func qaWorkerNewClock() *qaWorkerClock {
	return &qaWorkerClock{now: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC), created: make(chan time.Duration, 64)}
}
func (c *qaWorkerClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *qaWorkerClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	t := &qaWorkerTimer{clock: c, at: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	c.mu.Unlock()
	c.created <- d
	return t
}
func (c *qaWorkerClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range c.timers {
		if !t.stopped && !c.now.Before(t.at) {
			t.stopped = true
			t.ch <- c.now
		}
	}
}

type qaWorkerScheduledSync struct {
	mu           sync.Mutex
	status       activity.SyncStatus
	readErr      error
	reads, calls int
	readEvents   chan int
	run          func(context.Context, activity.SyncRunInput) (activity.SyncRun, error)
}

func (a *qaWorkerScheduledSync) SyncStatus(context.Context) (activity.SyncStatus, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reads++
	if a.readEvents != nil {
		a.readEvents <- a.reads
	}
	return a.status, a.readErr
}
func (a *qaWorkerScheduledSync) SyncNow(ctx context.Context, in activity.SyncRunInput, _ activity.SyncDependencies) (activity.SyncRun, error) {
	a.mu.Lock()
	a.calls++
	f := a.run
	a.mu.Unlock()
	if f != nil {
		return f(ctx, in)
	}
	return activity.SyncRun{}, errors.New("unexpected pass")
}
func (a *qaWorkerScheduledSync) counts() (int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reads, a.calls
}
func qaWorkerStartLoop(t *testing.T, s *Service) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("worker failed bounded cancellation")
		}
	})
	return cancel, done
}
func qaWorkerWaitTimer(t *testing.T, c *qaWorkerClock, done <-chan error) time.Duration {
	t.Helper()
	select {
	case d := <-c.created:
		return d
	case err := <-done:
		t.Fatalf("worker exited before scheduler barrier: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("worker never reached timer barrier")
	}
	return 0
}
func TestQAWorkerIdleAndPausedPollingNeverCreatesPasses(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "paused", true: "empty"}[enabled], func(t *testing.T) {
			o, _ := qaWorkerOptions(t)
			clock := qaWorkerNewClock()
			a := &qaWorkerScheduledSync{status: activity.SyncStatus{Enabled: enabled, Worker: activity.WorkerStatus{SyncEnabled: enabled}}}
			o.Clock = clock
			o.Sync = a
			o.NewRequestID = func() (string, error) { t.Error("idle poll minted request"); return qaWorkerID(901), nil }
			_, done := qaWorkerStartLoop(t, qaWorkerNew(t, o))
			for i := 0; i < 3; i++ {
				d := qaWorkerWaitTimer(t, clock, done)
				if d != 30*time.Second {
					t.Fatalf("idle cadence=%v", d)
				}
				reads, calls := a.counts()
				if reads != i+1 || calls != 0 {
					t.Fatalf("idle reads%d passes%d", reads, calls)
				}
				clock.advance(30 * time.Second)
			}
		})
	}
}
func TestQAWorkerReadFailureBackoffAndClockJumpDoNotStorm(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	clock := qaWorkerNewClock()
	a := &qaWorkerScheduledSync{readErr: &activity.Error{Code: "network", Message: "synthetic read failure", Retryable: true}}
	o.Clock = clock
	o.Sync = a
	_, done := qaWorkerStartLoop(t, qaWorkerNew(t, o))
	for i, want := range []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second} {
		d := qaWorkerWaitTimer(t, clock, done)
		if d != want {
			t.Fatalf("backoff%d=%v want%v", i, d, want)
		}
		reads, calls := a.counts()
		if reads != i+1 || calls != 0 {
			t.Fatalf("read failure effects reads%d passes%d", reads, calls)
		}
		if i == 0 {
			clock.advance(24 * time.Hour)
		} else {
			clock.advance(d)
		}
	}
}
func TestQAWorkerAuthenticationFailureWaitsFiveMinutes(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	clock := qaWorkerNewClock()
	a := &qaWorkerScheduledSync{readErr: &activity.Error{Code: "auth", Message: "synthetic auth failure"}}
	o.Clock = clock
	o.Sync = a
	_, done := qaWorkerStartLoop(t, qaWorkerNew(t, o))
	if d := qaWorkerWaitTimer(t, clock, done); d != 5*time.Minute {
		t.Fatalf("auth delay=%v", d)
	}
	if reads, calls := a.counts(); reads != 1 || calls != 0 {
		t.Fatalf("auth effects reads%d passes%d", reads, calls)
	}
}

func TestQAWorkerProgressPassHasOneSecondGapAndFreshDurableIdentity(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	clock := qaWorkerNewClock()
	a := &qaWorkerScheduledSync{status: activity.SyncStatus{Enabled: true, Items: []activity.OutboxItem{{ID: qaWorkerID(700), State: "queued"}}}}
	o.Clock = clock
	o.Sync = a
	next := 100
	o.NewRequestID = func() (string, error) { next++; return qaWorkerID(next), nil }
	var seen []string
	a.run = func(_ context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
		r := qaWorkerReadRuntime(t, o)
		if r.Pending == nil || *r.Pending != in || in.Limit != 20 {
			return activity.SyncRun{}, errors.New("pass not durably reserved")
		}
		seen = append(seen, in.RequestID)
		return activity.SyncRun{ContractVersion: 1, RequestID: in.RequestID, State: "complete", ResolvedIDs: []string{qaWorkerID(700)}, RemainingCount: 1}, nil
	}
	_, done := qaWorkerStartLoop(t, qaWorkerNew(t, o))
	for i := 0; i < 2; i++ {
		if d := qaWorkerWaitTimer(t, clock, done); d != time.Second {
			t.Fatalf("progress delay%v want1s", d)
		}
		_, calls := a.counts()
		if calls != i+1 {
			t.Fatalf("progress passes%d", calls)
		}
		if i == 0 {
			clock.advance(time.Second)
		}
	}
	if len(seen) != 2 || seen[0] == seen[1] {
		t.Fatalf("fresh progress reused receipt IDs%v", seen)
	}
}
func TestQAWorkerWakeDoesNotBypassAuthBackoffAndRecheckIsThrottled(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	short, err := os.MkdirTemp("/tmp", "tw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(short) })
	o.StatePath = filepath.Join(short, "activity.json")
	clock := qaWorkerNewClock()
	a := &qaWorkerScheduledSync{readErr: &activity.Error{Code: "auth", Message: "synthetic auth"}, readEvents: make(chan int, 128)}
	o.Clock = clock
	o.Sync = a
	_, done := qaWorkerStartLoop(t, qaWorkerNew(t, o))
	if d := qaWorkerWaitTimer(t, clock, done); d != 5*time.Minute {
		t.Fatalf("auth delay%v", d)
	}
	<-a.readEvents
	for i := 0; i < 20; i++ {
		_ = Notify(context.Background(), o.StatePath, Wake)
	}
	select {
	case n := <-a.readEvents:
		t.Fatalf("ordinary wake bypassed auth backoff read%d", n)
	case <-time.After(50 * time.Millisecond):
	}
	if err := Notify(context.Background(), o.StatePath, Recheck); err != nil {
		t.Fatalf("live worker lacks bounded recheck endpoint%v", err)
	}
	select {
	case n := <-a.readEvents:
		if n != 2 {
			t.Fatalf("recheck count%d", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recheck did not request immediate readiness")
	}
	for i := 0; i < 20; i++ {
		_ = Notify(context.Background(), o.StatePath, Recheck)
	}
	select {
	case n := <-a.readEvents:
		t.Fatalf("recheck flood bypassed5s throttle read%d", n)
	case <-time.After(50 * time.Millisecond):
	}
	clock.advance(5 * time.Second)
	if err := Notify(context.Background(), o.StatePath, Recheck); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-a.readEvents:
		if n != 3 {
			t.Fatalf("second allowed recheck count%d", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recheck remained throttled after5s")
	}
	if _, calls := a.counts(); calls != 0 {
		t.Fatalf("readiness errors created passes%d", calls)
	}
}

func TestQAWorkerTerminalBlockedAuthResultUsesFiveMinuteBackoff(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	clock := qaWorkerNewClock()
	category := "auth"
	id := qaWorkerID(700)
	a := &qaWorkerScheduledSync{status: activity.SyncStatus{Enabled: true, Items: []activity.OutboxItem{{ID: id, State: "queued"}}}}
	o.Clock = clock
	o.Sync = a
	a.run = func(_ context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
		// Actual #11 preflight failures are terminal receipts with queued failure state,
		// not errors returned by SyncNow. Preserve that shape in this focused scheduler fixture.
		a.mu.Lock()
		a.status.Items[0].FailureCategory = &category
		a.status.Worker.FailureCategory = &category
		a.mu.Unlock()
		return activity.SyncRun{ContractVersion: 1, RequestID: in.RequestID, State: "complete", BlockedIDs: []string{id}, AttemptedIDs: []string{}, ResolvedIDs: []string{}, RemainingCount: 1}, nil
	}
	_, done := qaWorkerStartLoop(t, qaWorkerNew(t, o))
	if d := qaWorkerWaitTimer(t, clock, done); d != 5*time.Minute {
		t.Fatalf("terminal blocked auth waited%v instead of5min", d)
	}
	r := qaWorkerReadRuntime(t, o)
	if r.Pending != nil || r.LastSuccess != nil || r.FailureCategory == nil || *r.FailureCategory != "auth" || r.BackoffUntil == nil {
		t.Fatalf("blocked result lost failure or invented delivery%+v", r)
	}
}

func TestQAWorkerRestartBackoffClampsFutureDeadlineWithoutImmediateAttempt(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	clock := qaWorkerNewClock()
	future := clock.Now().Add(365 * 24 * time.Hour)
	category := "network"
	qaWorkerWriteRuntime(t, o, runtimeRecord{Version: 1, InstanceMode: "foreground", FailureCategory: &category, BackoffNS: int64(5 * time.Second), BackoffUntil: &future})
	a := &qaWorkerScheduledSync{status: activity.SyncStatus{Enabled: true, Items: []activity.OutboxItem{{ID: qaWorkerID(700), State: "queued"}}}}
	o.Clock = clock
	o.Sync = a
	_, done := qaWorkerStartLoop(t, qaWorkerNew(t, o))
	if d := qaWorkerWaitTimer(t, clock, done); d != 5*time.Minute {
		t.Fatalf("restart deadline was not clamped to5min: %v", d)
	}
	if _, calls := a.counts(); calls != 0 {
		t.Fatalf("restart bypassed persisted failure backoff passes%d", calls)
	}
}
func TestQAWorkerUncertainPassRetriesSameUUIDAndNeverStampsReplay(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	clock := qaWorkerNewClock()
	a := &qaWorkerScheduledSync{status: activity.SyncStatus{Enabled: true, Items: []activity.OutboxItem{{ID: qaWorkerID(700), State: "queued"}}}}
	o.Clock = clock
	o.Sync = a
	generated := 0
	o.NewRequestID = func() (string, error) { generated++; return qaWorkerID(400), nil }
	var first activity.SyncRunInput
	attempts := 0
	a.run = func(_ context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
		attempts++
		if attempts == 1 {
			first = in
			return activity.SyncRun{}, &activity.Error{Code: "local_write_unknown", Uncertain: true}
		}
		if in != first {
			return activity.SyncRun{}, errors.New("changed uncertain identity")
		}
		return activity.SyncRun{ContractVersion: 1, RequestID: in.RequestID, State: "interrupted", ResolvedIDs: []string{qaWorkerID(700)}}, nil
	}
	s := qaWorkerNew(t, o)
	s.fail = func(stage string) error {
		if stage == "runtime_after_result_saved" {
			return errors.New("stop after replay")
		}
		return nil
	}
	_, done := qaWorkerStartLoop(t, s)
	d := qaWorkerWaitTimer(t, clock, done)
	if d < 5*time.Second || d > 5*time.Minute {
		t.Fatalf("uncertain retry unbounded%v", d)
	}
	r := qaWorkerReadRuntime(t, o)
	if r.Pending == nil || *r.Pending != first || r.LastSuccess != nil {
		t.Fatalf("uncertain outcome lost pending%+v", r)
	}
	clock.advance(d)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("missing result barrier")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("replay failed to complete")
	}
	if attempts != 2 || generated != 1 {
		t.Fatalf("uncertain request attempts%d generated%d", attempts, generated)
	}
	r = qaWorkerReadRuntime(t, o)
	if r.Pending != nil || r.LastSuccess != nil {
		t.Fatalf("replay stamped success%+v", r)
	}
}

func TestQAWorkerCancellationWaitsForOwnedPassCleanup(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	a := &qaWorkerScheduledSync{status: activity.SyncStatus{Enabled: true, Items: []activity.OutboxItem{{ID: qaWorkerID(700), State: "queued"}}}}
	o.Sync = a
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	a.run = func(ctx context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-release
		return activity.SyncRun{}, ctx.Err()
	}
	cancel, done := qaWorkerStartLoop(t, qaWorkerNew(t, o))
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("pass didn't start")
	}
	cancel()
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("owned pass never received cancellation")
	}
	select {
	case err := <-done:
		t.Fatalf("worker returned before owned cleanup completed%v", err)
	case <-time.After(50 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation cause%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker didn't join completed pass")
	}
	r := qaWorkerReadRuntime(t, o)
	if r.Pending == nil || r.Pending.RequestID != qaWorkerID(900) {
		t.Fatalf("cancel erased uncertain pending%+v", r)
	}
}
func TestQAWorkerLongSocketPathFallsBackToPolling(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	o.StatePath = filepath.Join(filepath.Dir(o.StatePath), "long-synthetic-worker-state-path-which-exceeds-darwin-unix-socket-address-size-while-remaining-valid.json")
	clock := qaWorkerNewClock()
	a := &qaWorkerScheduledSync{}
	o.Clock = clock
	o.Sync = a
	_, done := qaWorkerStartLoop(t, qaWorkerNew(t, o))
	if d := qaWorkerWaitTimer(t, clock, done); d != 30*time.Second {
		t.Fatalf("fallback idle cadence%v", d)
	}
	start := time.Now()
	_ = Notify(context.Background(), o.StatePath, Wake)
	if time.Since(start) > 250*time.Millisecond {
		t.Fatal("unsupported socket path stalled sender")
	}
	clock.advance(30 * time.Second)
	if d := qaWorkerWaitTimer(t, clock, done); d != 30*time.Second {
		t.Fatalf("fallback polling stopped%v", d)
	}
	if reads, calls := a.counts(); reads != 2 || calls != 0 {
		t.Fatalf("fallback effects reads%d calls%d", reads, calls)
	}
}

func TestQAWorkerForegroundStopSurvivesNotificationFlood(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	short, err := os.MkdirTemp("/tmp", "tws-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(short) })
	o.StatePath = filepath.Join(short, "activity.json")
	clock := qaWorkerNewClock()
	a := &qaWorkerScheduledSync{}
	o.Clock = clock
	o.Sync = a
	s := qaWorkerNew(t, o)
	_, done := qaWorkerStartLoop(t, s)
	if d := qaWorkerWaitTimer(t, clock, done); d != 30*time.Second {
		t.Fatal("foreground idle not reached")
	}
	if err = Notify(context.Background(), o.StatePath, Wake); err != nil {
		t.Fatalf("fixture has no live endpoint%v", err)
	}
	flooded := make(chan struct{})
	go func() {
		defer close(flooded)
		for i := 0; i < 40; i++ {
			_ = Notify(context.Background(), o.StatePath, Wake)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := qaWorkerNew(t, o).Stop(ctx, ControlRequest{RequestID: qaWorkerID(800)})
	if err != nil {
		t.Fatalf("foreground stop failed%v", err)
	}
	if result.Status.State == "running" {
		t.Fatal("stop returned while owner still running")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop didn't join foreground worker")
	}
	select {
	case <-flooded:
	case <-time.After(time.Second):
		t.Fatal("bounded notification sender leaked")
	}
	control := qaWorkerReadControl(t, o)
	if control.Pending != nil || control.Receipts[qaWorkerID(800)].Result == nil {
		t.Fatal("foreground stop missing durable receipt")
	}
	if _, err = os.Stat(o.StatePath); !os.IsNotExist(err) {
		t.Fatalf("foreground stop minted activity state%v", err)
	}
}
