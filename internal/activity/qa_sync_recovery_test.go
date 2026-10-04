package activity

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestQASyncInterruptedClaimRecoversBeforePausedAndSameIDNeverPosts(t *testing.T) {
	s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	input := SyncRunInput{RequestID: qaSyncID(30)}
	reached := 0
	s.store.fail = func(stage string) error {
		if stage == "sync_after_claim" {
			reached++
			return errors.New("synthetic crash boundary private detail")
		}
		return nil
	}
	_, err := s.SyncNow(context.Background(), input, qaSyncDeps(t, p))
	qaCode(t, err, "local_write_unknown")
	s.store.fail = nil
	if reached != 1 || len(p.posts) != 0 {
		t.Fatalf("barrier reached=%d posts=%d", reached, len(p.posts))
	}
	claimed := qaSyncOnlyItem(t, s)
	if claimed.State != "submitting" || claimed.RunRequestID == nil || *claimed.RunRequestID != input.RequestID {
		t.Fatalf("claim not saved: %+v", claimed)
	}
	// Status cannot classify a live or orphaned claim by observing it.
	if again := qaSyncOnlyItem(t, s); !reflect.DeepEqual(claimed, again) {
		t.Fatal("status mutated orphan")
	}
	if _, err = s.SyncPause(context.Background(), qaSyncID(31)); err != nil {
		t.Fatal(err)
	}
	restarted := qaLegacyNew(Options{Path: path})
	recovered, err := restarted.SyncNow(context.Background(), input, qaSyncNoProvider(t))
	if err != nil {
		t.Fatal(err)
	}
	if recovered.State != "interrupted" {
		t.Fatalf("pending replay started fresh run: %+v", recovered)
	}
	item := qaSyncOnlyItem(t, restarted)
	if item.State != "unknown" || item.RunRequestID != nil || item.Plan == nil || item.Plan.Parts[0].State != "unknown" || len(item.Plan.Parts[0].Attempts) != 1 {
		t.Fatalf("paused orphan recovery=%+v", item)
	}
	again, err := restarted.SyncNow(context.Background(), input, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(recovered, again) {
		t.Fatalf("terminal recovery replay=%+v err=%v", again, err)
	}
	if len(p.posts) != 0 {
		t.Fatal("recovery posted ambiguous claim")
	}
}
func TestQASyncSavedRemoteSuccessFinalizesLocallyAfterInterruption(t *testing.T) {
	s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	reached := 0
	s.store.fail = func(stage string) error {
		if stage == "sync_after_part_saved" {
			reached++
			return errors.New("synthetic interruption")
		}
		return nil
	}
	input := SyncRunInput{RequestID: qaSyncID(40)}
	_, err := s.SyncNow(context.Background(), input, qaSyncDeps(t, p))
	qaCode(t, err, "local_write_unknown")
	s.store.fail = nil
	if reached != 1 || len(p.posts) != 1 {
		t.Fatalf("saved part barrier=%d posts=%d", reached, len(p.posts))
	}
	item := qaSyncOnlyItem(t, s)
	if item.State != "submitting" || item.Plan == nil || item.Plan.Parts[0].State != "synced" {
		t.Fatalf("root ownership lost before final receipt: %+v", item)
	}
	recovered, err := qaLegacyNew(Options{Path: path}).SyncNow(context.Background(), input, qaSyncNoProvider(t))
	if err != nil {
		t.Fatal(err)
	}
	item = qaSyncOnlyItem(t, s)
	if item.State != "synced" || item.RunRequestID != nil || item.EntryID == nil || *item.EntryID != "901" || recovered.State != "interrupted" {
		t.Fatalf("saved success discarded/reposted: %+v run=%+v", item, recovered)
	}
	if len(p.posts) != 1 {
		t.Fatal("successful remote effect repeated")
	}
}
func TestQASyncRunLockBoundsContentionWithoutBlockingOfflineReadsOrPause(t *testing.T) {
	s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
	held, err := s.acquireSyncLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.close()
	other := qaLegacyNew(Options{Path: path, LockTimeout: 40 * time.Millisecond})
	start := time.Now()
	_, err = other.acquireSyncLock(context.Background())
	qaCode(t, err, "state_busy")
	if time.Since(start) > time.Second {
		t.Fatal("run contention exceeded finite bound")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err = other.SyncStatus(ctx); err != nil {
		t.Fatalf("run lock blocks offline status: %v", err)
	}
	if _, err = other.SyncPause(ctx, qaSyncID(50)); err != nil {
		t.Fatalf("run lock blocks pause: %v", err)
	}
}

func TestQASyncPauseDuringMultipartRunPreservesSuccessAndNeverAutoResumes(t *testing.T) {
	s := qaSyncHistorical(t, qaSyncTime(t, "2026-10-02T23:59:40Z"), qaSyncTime(t, "2026-10-03T00:00:20Z"), "UTC")
	p := qaNewSyncProvider(t)
	p.returnedHours = ""
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	saved := 0
	s.store.fail = func(stage string) error {
		if stage == "sync_after_part_saved" {
			saved++
			if saved == 1 {
				if _, err := s.SyncPause(context.Background(), qaSyncID(141)); err != nil {
					t.Fatalf("pause blocked by remote run: %v", err)
				}
			}
		}
		return nil
	}
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(140)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	s.store.fail = nil
	item := qaSyncOnlyItem(t, s)
	if saved != 1 || len(p.posts) != 1 || item.State != "needs_attention" || item.Plan == nil || len(item.Plan.Parts) != 2 || item.Plan.Parts[0].State != "synced" || len(item.Plan.Parts[1].Attempts) != 0 {
		t.Fatalf("partial pause lost state: %+v saved%d posts%d", item, saved, len(p.posts))
	}
	if item.FailureCategory == nil || *item.FailureCategory != "partial_submission_interrupted" {
		t.Fatalf("partial interruption reason=%v", func() string {
			if item.FailureCategory == nil {
				return "nil"
			}
			return *item.FailureCategory
		}())
	}
	if _, err = s.SyncResume(context.Background(), qaSyncID(142)); err != nil {
		t.Fatal(err)
	}
	_, err = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(143)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.posts) != 1 {
		t.Fatal("partial completion resumed without reviewed action")
	}
}
func TestQASyncLiveCreateExcludesSecondRunButPermitsStatusAndPause(t *testing.T) {
	s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	p.beforeCreate = func(_ map[string]any) { close(entered); <-release }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.SyncNow(ctx, SyncRunInput{RequestID: qaSyncID(144)}, qaSyncDeps(t, p))
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("run never reached POST: %v", err)
	case <-ctx.Done():
		t.Fatal("POST barrier deadline")
	}
	other := qaLegacyNew(Options{Path: path, LockTimeout: 30 * time.Millisecond})
	// The second run must fail on ownership, before attempting any provider access.
	for _, id := range []string{qaSyncID(144), qaSyncID(145)} {
		_, err := other.SyncNow(ctx, SyncRunInput{RequestID: id}, qaSyncNoProvider(t))
		qaCode(t, err, "state_busy")
	}
	var err error
	item := qaSyncOnlyItem(t, other)
	if item.State != "submitting" {
		t.Fatalf("live claim mistaken for orphan: %+v", item)
	}
	if _, err = other.SyncPause(ctx, qaSyncID(146)); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("run did not finish after provider returned")
	}
	if len(p.posts) != 1 {
		t.Fatalf("concurrent submission count%d", len(p.posts))
	}
}

func qaSyncAppendCapturedInterval(t *testing.T, s *Service) Interval {
	t.Helper()
	st, _, err := s.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	start := st.Intervals[len(st.Intervals)-1].End.Add(time.Minute)
	epoch, n := "qa-later-boot", "0"
	sample := ClockSample{Capability: "available", WallUTC: start, Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}
	s.clock = ClockFunc(func() (ClockSample, error) { return sample, nil })
	var binding BindingSnapshot
	for _, b := range st.Bindings {
		binding = b
		break
	}
	event := qaEvent("later-capture", "1", "1", "work", binding.ID)
	event.Actor.ComputerID = st.ComputerID
	if _, err = s.Ingest(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	n = "36000000000"
	sample.WallUTC = start.Add(36 * time.Second)
	event.Sequence = "2"
	event.EventID = "later-capture/1/2"
	event.Kind = "finish"
	event.BindingID = ""
	event.BindingRevision = ""
	if _, err = s.Ingest(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	after, _, err := s.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Intervals) != len(st.Intervals)+1 {
		t.Fatal("later interval fixture did not close")
	}
	return after.Intervals[len(after.Intervals)-1]
}
func TestQASyncPendingReplayNeverClaimsNewlyCapturedRoot(t *testing.T) {
	s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	input := SyncRunInput{RequestID: qaSyncID(160)}
	s.store.fail = func(stage string) error {
		if stage == "sync_after_claim" {
			return errors.New("fixture interruption")
		}
		return nil
	}
	_, err := s.SyncNow(context.Background(), input, qaSyncDeps(t, p))
	qaCode(t, err, "local_write_unknown")
	s.store.fail = nil
	old := qaSyncOnlyItem(t, s)
	later := qaSyncAppendCapturedInterval(t, s)
	recovered, err := qaLegacyNew(Options{Path: path}).SyncNow(context.Background(), input, qaSyncNoProvider(t))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recovered.AttemptedIDs, []string{old.ID}) {
		t.Fatalf("pending target set changed: %+v", recovered)
	}
	st, _, err := s.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Outbox[later.ID].State != "queued" || st.Outbox[later.ID].Plan != nil || len(p.posts) != 0 {
		t.Fatalf("pending replay consumed newly queued root: %+v", st.Outbox[later.ID])
	}
}

func TestQASyncRemoteSuccessThenLocalSaveFailureRemainsUnknownWithoutRepost(t *testing.T) {
	s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	armed, reached := false, 0
	p.beforeCreate = func(_ map[string]any) {
		armed = true
		s.store.fail = func(stage string) error {
			if armed && stage == "before_write" {
				reached++
				return errors.New("synthetic private persistence failure")
			}
			return nil
		}
	}
	input := SyncRunInput{RequestID: qaSyncID(161)}
	_, err := s.SyncNow(context.Background(), input, qaSyncDeps(t, p))
	qaCode(t, err, "local_write_unknown")
	s.store.fail = nil
	if len(p.posts) != 1 || reached != 1 {
		t.Fatalf("save failure barrier%d posts%d", reached, len(p.posts))
	}
	item := qaSyncOnlyItem(t, s)
	if item.State != "submitting" || item.Plan.Parts[0].EntryID != nil {
		t.Fatalf("unsaved response appeared durable: %+v", item)
	}
	_, err = qaLegacyNew(Options{Path: path}).SyncNow(context.Background(), input, qaSyncNoProvider(t))
	if err != nil {
		t.Fatal(err)
	}
	item = qaSyncOnlyItem(t, s)
	if item.State != "unknown" || len(p.posts) != 1 {
		t.Fatalf("unsaved success retried/lost uncertainty: %+v posts%d", item, len(p.posts))
	}
}
