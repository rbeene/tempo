package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/harvest"
)

func qaSyncEntryFromPost(in harvest.Object) harvest.Object {
	out := harvest.Object{}
	for k, v := range in {
		out[k] = v
	}
	out["id"] = json.Number("901")
	out["hours"] = json.Number("0.040")
	out["rounded_hours"] = json.Number("0.25")
	out["is_running"] = false
	out["user"] = harvest.Object{"id": json.Number("2")}
	out["project"] = harvest.Object{"id": json.Number("3")}
	out["task"] = harvest.Object{"id": json.Number("4")}
	return out
}
func qaSyncUnknown(t *testing.T) (*Service, *qaSyncProvider, OutboxItem) {
	t.Helper()
	s, _, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	p.createErr = &harvest.Error{Code: "timeout", Uncertain: true}
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(80)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	item := qaSyncOnlyItem(t, s)
	if item.State != "unknown" || len(p.posts) != 1 {
		t.Fatalf("unknown fixture=%+v posts%d", item, len(p.posts))
	}
	return s, p, item
}
func TestQASyncReconcileRequiresUniqueFullPayloadAndUnfilteredScope(t *testing.T) {
	for _, tc := range []struct {
		name       string
		alter      func(harvest.Object)
		duplicate  bool
		wantSynced bool
	}{
		{"exact", nil, false, true},
		{"wrong-date", func(o harvest.Object) { o["spent_date"] = "2026-10-01" }, false, false},
		{"wrong-project", func(o harvest.Object) { o["project"] = harvest.Object{"id": json.Number("99")} }, false, false},
		{"running", func(o harvest.Object) { o["is_running"] = true }, false, false},
		{"different-rational-same-ns", func(o harvest.Object) { o["hours"] = json.Number("0.04000000000000001") }, false, false},
		{"notes-only-not-auto", func(o harvest.Object) { o["external_reference"] = nil }, false, false},
		{"correct-plus-conflicting-collision", func(o harvest.Object) { o["task"] = harvest.Object{"id": json.Number("99")} }, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, p, item := qaSyncUnknown(t)
			candidate := qaSyncEntryFromPost(p.posts[0])
			if tc.alter != nil {
				tc.alter(candidate)
			}
			p.entryRows = []harvest.Object{candidate}
			if tc.duplicate {
				p.entryRows = append([]harvest.Object{qaSyncEntryFromPost(p.posts[0])}, candidate)
			}
			_, err := s.SyncReconcile(context.Background(), SyncReconcileInput{RequestID: qaSyncID(81), OutboxID: item.ID}, qaSyncDeps(t, p))
			if err != nil {
				t.Fatal(err)
			}
			after := qaSyncOnlyItem(t, s)
			if (after.State == "synced") != tc.wantSynced || len(p.posts) != 1 {
				t.Fatalf("reconcile disposition=%+v posts%d", after, len(p.posts))
			}
			if len(p.listQueries) != 1 {
				t.Fatalf("list count=%d", len(p.listQueries))
			}
			want := url.Values{"user_id": {"2"}, "external_reference_id": {"tempo:v1:" + item.Plan.Parts[0].ID}}
			if !reflect.DeepEqual(p.listQueries[0], want) {
				t.Fatalf("collision-hiding filter=%v want=%v", p.listQueries[0], want)
			}
		})
	}
}
func TestQASyncReconcileAbsenceNeverPermitsRetry(t *testing.T) {
	s, p, item := qaSyncUnknown(t)
	p.entryRows = []harvest.Object{}
	_, err := s.SyncReconcile(context.Background(), SyncReconcileInput{RequestID: qaSyncID(82), OutboxID: item.ID}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	after := qaSyncOnlyItem(t, s)
	if after.State == "queued" || after.State == "rejected" || after.State == "synced" {
		t.Fatalf("absence authorized a new charge: %+v", after)
	}
	_, err = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(83)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.posts) != 1 {
		t.Fatal("absent unknown was rebilled")
	}
}

func TestQASyncManualNotesOnlyAttachmentRequiresFullUserCollisionScan(t *testing.T) {
	for _, collision := range []bool{false, true} {
		t.Run(fmt.Sprintf("collision-%t", collision), func(t *testing.T) {
			s, p, item := qaSyncUnknown(t)
			entry := qaSyncEntryFromPost(p.posts[0])
			entry["external_reference"] = nil
			p.entry = entry
			p.entryRows = []harvest.Object{entry}
			if collision {
				second := qaSyncEntryFromPost(p.posts[0])
				second["id"] = json.Number("902")
				second["external_reference"] = nil
				p.entryRows = append(p.entryRows, second)
			}
			_, err := s.SyncResolve(context.Background(), SyncResolveInput{RequestID: qaSyncID(84), OutboxID: item.ID, EntryID: "901", IfRevision: item.Revision, Confirmed: true}, qaSyncDeps(t, p))
			if !collision && err != nil {
				t.Fatal(err)
			}
			after := qaSyncOnlyItem(t, s)
			if (after.State == "synced") == collision || len(p.posts) != 1 {
				t.Fatalf("manual attachment=%+v posts%d err%v", after, len(p.posts), err)
			}
			if len(p.listQueries) != 1 || !reflect.DeepEqual(p.listQueries[0], url.Values{"user_id": {"2"}}) {
				t.Fatalf("manual collision scan hid entries: %v", p.listQueries)
			}
		})
	}
}
func TestQASyncUnknownCannotBeAuthorizedForRejectedRetry(t *testing.T) {
	s, p, item := qaSyncUnknown(t)
	_, err := s.SyncResolve(context.Background(), SyncResolveInput{RequestID: qaSyncID(85), OutboxID: item.ID, RetryRejected: true, Confirmed: true, IfRevision: item.Revision}, qaSyncDeps(t, p))
	if err == nil {
		t.Fatal("unknown effect accepted as rejected")
	}
	after := qaSyncOnlyItem(t, s)
	if !reflect.DeepEqual(item, after) || len(p.posts) != 1 {
		t.Fatal("retry authorization erased ambiguous intent")
	}
}

func TestQASyncReviewedRejectedRetryPreservesSuccessfulPartAndFrozenPlan(t *testing.T) {
	s := qaSyncHistorical(t, qaSyncTime(t, "2026-10-02T23:59:40Z"), qaSyncTime(t, "2026-10-03T00:00:20Z"), "UTC")
	p := qaNewSyncProvider(t)
	p.returnedHours = ""
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	p.beforeCreate = func(_ harvest.Object) {
		if len(p.posts) == 2 {
			p.createErr = &harvest.Error{Code: "validation", Status: 422}
		}
	}
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(190)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	before := qaSyncOnlyItem(t, s)
	if len(p.posts) != 2 || before.Plan.Parts[0].State != "synced" || before.Plan.Parts[1].State != "rejected" {
		t.Fatalf("rejected fixture=%+v posts%d", before, len(p.posts))
	}
	request := SyncResolveInput{RequestID: qaSyncID(191), OutboxID: before.ID, IfRevision: before.Revision, RetryRejected: true, Confirmed: false}
	_, err = s.SyncResolve(context.Background(), request, qaSyncNoProvider(t))
	qaCode(t, err, "confirmation_required")
	request.Confirmed = true
	// Later user settings must not rewrite a payload which already had intent.
	_, err = s.SyncConfigure(context.Background(), SyncConfigureInput{AccountID: "1", Mode: "duration", DurationPolicy: "exact", IfRevision: "1", RequestID: qaSyncID(192), Confirmed: true}, qaSyncDeps(t, qaNewSyncProvider(t)))
	if err != nil {
		t.Fatal(err)
	}
	p.beforeCreate = nil
	p.createErr = nil
	authorized, err := s.SyncResolve(context.Background(), request, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := s.SyncResolve(context.Background(), request, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(authorized, replay) {
		t.Fatalf("resolve replay=%+v err%v", replay, err)
	}
	if len(p.posts) != 2 {
		t.Fatal("authorization itself posted")
	}
	retryRun, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(193)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	after := qaSyncOnlyItem(t, s)
	if len(p.posts) != 3 || after.State != "synced" || !reflect.DeepEqual(before.Plan.Parts[0], after.Plan.Parts[0]) || !reflect.DeepEqual(p.posts[1], p.posts[2]) || !reflect.DeepEqual(before.Plan.Configuration, after.Plan.Configuration) {
		t.Fatalf("retry lost frozen plan/success: %+v posts%d", after, len(p.posts))
	}
	if !reflect.DeepEqual(retryRun.AttemptedIDs, []string{before.ID}) || !reflect.DeepEqual(retryRun.ResolvedIDs, []string{before.ID}) {
		t.Fatalf("actual retry current-run IDs=%+v", retryRun)
	}
	attempts := after.Plan.Parts[1].Attempts
	if len(attempts) != 2 || attempts[0].State != "rejected" || attempts[1].State != "synced" || attempts[0].ID == attempts[1].ID {
		t.Fatalf("retry erased audit=%+v", attempts)
	}
}

func TestQASyncManualCompletionAttachesNeverAttemptedRemainder(t *testing.T) {
	s := qaSyncHistorical(t, qaSyncTime(t, "2026-10-02T23:59:40Z"), qaSyncTime(t, "2026-10-03T00:00:20Z"), "UTC")
	p := qaNewSyncProvider(t)
	p.returnedHours = ""
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	s.store.fail = func(stage string) error {
		if stage == "sync_after_part_saved" {
			_, err := s.SyncPause(context.Background(), qaSyncID(201))
			return err
		}
		return nil
	}
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(200)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	s.store.fail = nil
	before := qaSyncOnlyItem(t, s)
	part := before.Plan.Parts[1]
	if len(p.posts) != 1 || len(part.Attempts) != 0 {
		t.Fatal("fixture remainder attempted")
	}
	entry := qaSyncEntryFromPost(harvest.Object{"spent_date": part.SpentDate, "notes": part.Notes, "external_reference": nil})
	entry["id"] = json.Number("902")
	entry["hours"] = json.Number(part.PlannedHours)
	p.entry = entry
	p.entryRows = []harvest.Object{entry}
	_, err = s.SyncResolve(context.Background(), SyncResolveInput{RequestID: qaSyncID(202), OutboxID: before.ID, EntryID: "902", IfRevision: before.Revision, Confirmed: true}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	after := qaSyncOnlyItem(t, s)
	if after.State != "synced" || len(p.posts) != 1 || len(after.Plan.Parts[1].Attempts) != 0 || !reflect.DeepEqual(before.Plan.Parts[0], after.Plan.Parts[0]) {
		t.Fatalf("manual completion fabricated submission or lost success: %+v", after)
	}
	qaSyncString(t, after.Plan.Parts[1].EntryID, "902")
}
func TestQASyncManualCollisionScanAlsoChecksExternalReferenceWithDifferentNotes(t *testing.T) {
	s, p, item := qaSyncUnknown(t)
	selected := qaSyncEntryFromPost(p.posts[0])
	selected["external_reference"] = nil
	other := qaSyncEntryFromPost(p.posts[0])
	other["id"] = json.Number("902")
	other["notes"] = "another note with same correlation reference"
	p.entry = selected
	p.entryRows = []harvest.Object{selected, other}
	_, _ = s.SyncResolve(context.Background(), SyncResolveInput{RequestID: qaSyncID(203), OutboxID: item.ID, EntryID: "901", IfRevision: item.Revision, Confirmed: true}, qaSyncDeps(t, p))
	after := qaSyncOnlyItem(t, s)
	if len(p.listQueries) != 1 || after.State == "synced" || len(p.posts) != 1 {
		t.Fatalf("same reference collision hidden by different notes: %+v queries%v posts%d", after, p.listQueries, len(p.posts))
	}
}
func TestQASyncInterruptedResolveReplayPersistsTerminalReceipt(t *testing.T) {
	s, p, item := qaSyncUnknown(t)
	p.entryErr = &harvest.Error{Code: "network"}
	in := SyncResolveInput{RequestID: qaSyncID(204), OutboxID: item.ID, EntryID: "901", IfRevision: item.Revision, Confirmed: true}
	_, firstErr := s.SyncResolve(context.Background(), in, qaSyncDeps(t, p))
	if firstErr == nil {
		t.Fatal("injected entry read failure accepted")
	}
	before, _, err := s.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pending := before.Requests[in.RequestID].PendingSync != nil
	_, err = s.SyncResolve(context.Background(), in, qaSyncNoProvider(t))
	if pending {
		qaCode(t, err, "local_write_unknown")
	} else {
		if err == nil {
			t.Fatal("failed operation replay became success")
		}
	}
	after, _, readErr := s.store.read(context.Background())
	if readErr != nil {
		t.Fatal(readErr)
	}
	receipt := after.Requests[in.RequestID]
	if receipt.PendingSync != nil || receipt.Error == nil {
		t.Fatalf("replay did not durably terminalize: %+v", receipt)
	}
	_, _ = s.SyncResolve(context.Background(), in, qaSyncNoProvider(t))
	again, _, readErr := s.store.read(context.Background())
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !reflect.DeepEqual(receipt, again.Requests[in.RequestID]) || len(p.posts) != 1 {
		t.Fatal("terminal replay changed receipt or remote effects")
	}
}

func TestQASyncInterruptedReconcileWithoutEffectGetsDurableErrorReceipt(t *testing.T) {
	s, p, item := qaSyncUnknown(t)
	p.entryListErr = &harvest.Error{Code: "network"}
	writes := 0
	p.beforeEntryList = func() {
		s.store.fail = func(stage string) error {
			if stage == "before_write" {
				writes++
				return errors.New("synthetic final receipt failure")
			}
			return nil
		}
	}
	in := SyncReconcileInput{RequestID: qaSyncID(220), OutboxID: item.ID}
	_, err := s.SyncReconcile(context.Background(), in, qaSyncDeps(t, p))
	if err == nil {
		t.Fatal("receipt failure accepted")
	}
	s.store.fail = nil
	if writes != 1 || len(p.listQueries) != 1 {
		t.Fatalf("no-effect interrupted fixture writes%d lists%d", writes, len(p.listQueries))
	}
	before, _, err := s.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if before.Requests[in.RequestID].PendingSync == nil {
		t.Fatal("fixture did not retain pending operation")
	}
	_, err = s.SyncReconcile(context.Background(), in, qaSyncNoProvider(t))
	qaCode(t, err, "local_write_unknown")
	after, _, err := s.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	receipt := after.Requests[in.RequestID]
	if receipt.PendingSync != nil || receipt.Error == nil || receipt.SyncRun != nil {
		t.Fatalf("zero-effect replay fabricated interrupted success: %+v", receipt)
	}
	_, err = s.SyncReconcile(context.Background(), in, qaSyncNoProvider(t))
	qaCode(t, err, "local_write_unknown")
	if len(p.posts) != 1 {
		t.Fatal("interrupted reconcile posted")
	}
}
func TestQASyncInterruptedReconcileAfterSavedEffectReturnsOfflineInterruptedRun(t *testing.T) {
	s, p, item := qaSyncUnknown(t)
	p.entryRows = []harvest.Object{qaSyncEntryFromPost(p.posts[0])}
	reached := 0
	s.store.fail = func(stage string) error {
		if stage == "sync_before_complete" {
			reached++
			return errors.New("synthetic lost final receipt")
		}
		return nil
	}
	in := SyncReconcileInput{RequestID: qaSyncID(221), OutboxID: item.ID}
	_, err := s.SyncReconcile(context.Background(), in, qaSyncDeps(t, p))
	qaCode(t, err, "local_write_unknown")
	s.store.fail = nil
	if reached != 1 || qaSyncOnlyItem(t, s).State != "synced" {
		t.Fatalf("saved effect barrier%d", reached)
	}
	recovered, err := s.SyncReconcile(context.Background(), in, qaSyncNoProvider(t))
	if err != nil {
		t.Fatal(err)
	}
	if recovered.State != "interrupted" || !reflect.DeepEqual(recovered.ResolvedIDs, []string{item.ID}) || len(p.posts) != 1 {
		t.Fatalf("persisted effect recovery=%+v", recovered)
	}
	again, err := s.SyncReconcile(context.Background(), in, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(recovered, again) {
		t.Fatalf("recovered receipt unstable: %+v err%v", again, err)
	}
}

func qaSyncAuthorizedRejected(t *testing.T) (*Service, *qaSyncProvider, OutboxItem) {
	t.Helper()
	s, _, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	p.createErr = &harvest.Error{Code: "validation", Status: 422}
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(230)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	rejected := qaSyncOnlyItem(t, s)
	if len(p.posts) != 1 || rejected.Plan.Parts[0].State != "rejected" {
		t.Fatal("fixture not definitely rejected")
	}
	_, err = s.SyncResolve(context.Background(), SyncResolveInput{RequestID: qaSyncID(231), OutboxID: rejected.ID, IfRevision: rejected.Revision, RetryRejected: true, Confirmed: true}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	p.createErr = nil
	return s, p, qaSyncOnlyItem(t, s)
}
func TestQASyncAuthorizedRetryPreflightConflictPersistsVisibleBlockWithoutNewIntent(t *testing.T) {
	s, p, before := qaSyncAuthorizedRejected(t)
	p.company["wants_timestamp_timers"] = true
	run, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(232)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatalf("truthful preflight blocker could not be persisted: %v", err)
	}
	after := qaSyncOnlyItem(t, s)
	if len(p.posts) != 1 || (after.State != "needs_attention" && after.State != "queued") || after.FailureCategory == nil || *after.FailureCategory != "mode_conflict" || !reflect.DeepEqual(before.Plan, after.Plan) {
		t.Fatalf("retry blocker damaged saved evidence: %+v posts%d", after, len(p.posts))
	}
	if len(run.AttemptedIDs) != 0 {
		t.Fatalf("preflight blocker reported old intent as current: %+v", run)
	}
}
func TestQASyncAuthorizedRetryTransientPreflightDoesNotReportHistoricalIntent(t *testing.T) {
	s, p, before := qaSyncAuthorizedRejected(t)
	reached := 0
	d := SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) {
		reached++
		return nil, &harvest.Error{Code: "network"}
	}}
	run, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(233)}, d)
	if err != nil {
		t.Fatal(err)
	}
	after := qaSyncOnlyItem(t, s)
	if reached != 1 || len(p.posts) != 1 || !reflect.DeepEqual(before.Plan, after.Plan) {
		t.Fatalf("preflight failure altered prior effects: %+v reached%d posts%d", after, reached, len(p.posts))
	}
	if len(run.AttemptedIDs) != 0 {
		t.Fatalf("historical rejected attempt attributed to current run: %+v", run)
	}
}
