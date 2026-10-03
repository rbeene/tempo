//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/harvest"
)

func TestSQLiteFirstSyncNowPublicCompletedACKAndNoSecondPOST(t *testing.T) {
	q := snQACaptured(t)
	p := qaNewSyncProvider(t)
	p.returnedHours = "0.010"
	in := SyncRunInput{RequestID: snQAID(10)}
	var commits, durableReleases, lastDurableCommit, barriersAtPOST atomic.Int64
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		// sqlite3_step(COMMIT) actually succeeds with SQLITE_DONE (101).
		if e.Phase == "commit-after-engine" && e.Code == 101 {
			commits.Add(1)
		}
		if e.Phase == "durable-release-before" && e.Code == 0 {
			durableReleases.Add(1)
			lastDurableCommit.Store(commits.Load())
		}
	}})
	var claim snQASnapshot
	p.beforeCreate = func(payload harvest.Object) {
		if len(p.posts) != 1 || commits.Load() == 0 || durableReleases.Load() == 0 || lastDurableCommit.Load() != commits.Load() {
			t.Fatal("POST before checked COMMIT/durable barrier or repeated POST")
		}
		barriersAtPOST.Store(durableReleases.Load())
		claim = snQAClaim(t, q, in.RequestID) // Actual independent SQL reader inside provider call.
		part := claim.item.Plan.Parts[0]
		expected := harvest.Object{"user_id": json.Number("2"), "project_id": json.Number("3"), "task_id": json.Number("4"), "spent_date": "2026-10-02", "hours": json.Number("0.01"), "notes": part.Notes, "external_reference": harvest.Object{"id": part.Correlation, "group_id": claim.item.ID, "account_id": q.interval.ComputerID}}
		if !reflect.DeepEqual(payload, expected) {
			t.Fatal("completed-duration POST differs from frozen owned plan")
		}
	}
	run, err := q.s.SyncNow(context.Background(), in, qaSyncDeps(t, p))
	spQASetSQLHooks(spQASQLHooks{})
	if err != nil {
		t.Fatal("public SyncNow completed workflow", err)
	}
	if len(p.posts) != 1 || claim.item.ID == "" || lastDurableCommit.Load() != commits.Load() || durableReleases.Load() <= barriersAtPOST.Load() {
		t.Fatal("missing claim/ACK/durable completion witness")
	}
	if run.ContractVersion != 1 || run.RequestID != in.RequestID || run.State != "complete" || !reflect.DeepEqual(run.AttemptedIDs, []string{claim.item.ID}) || !reflect.DeepEqual(run.ResolvedIDs, []string{claim.item.ID}) || run.BlockedIDs == nil || len(run.BlockedIDs) != 0 || run.RemainingCount != 0 {
		t.Fatal("completed run receipt", run)
	}
	complete := snQARead(t, q, in.RequestID)
	item := complete.item
	if item.State != "synced" || item.RunRequestID != nil || item.RetryRequestID != nil || item.Plan == nil || len(item.Plan.Parts) != 1 || !reflect.DeepEqual(item.Interval, q.interval) || !reflect.DeepEqual(item.Plan.Configuration, q.configuration) || item.Plan.CompanySource != "company_verified" {
		t.Fatal("ACK changed immutable attribution/configuration or failed to finish root")
	}
	part := item.Plan.Parts[0]
	if part.State != "synced" || part.ID != claim.item.Plan.Parts[0].ID || part.Notes != claim.item.Plan.Parts[0].Notes || part.Correlation != "tempo:v1:"+part.ID || !strings.HasPrefix(part.Notes, "Tempo activity [tempo:v1:"+part.ID+":") || !strings.HasSuffix(part.Notes, "]") || part.SpentDate != "2026-10-02" || part.DurationNS != "36000000000" || part.PlannedHours != "0.01" || part.PlannedDurationNS != "36000000000" || part.PlannedResidualNS != "0" || part.StartedTime != nil || part.EndedTime != nil {
		t.Fatal("frozen plan/calendar/amount/marker changed")
	}
	snQAString(t, item.EntryID, "901")
	snQAString(t, part.EntryID, "901")
	snQAString(t, part.ReturnedHours, "0.010")
	snQAString(t, part.RoundedHours, "0.25")
	snQAString(t, part.ConfirmedDurationNS, "36000000000")
	snQAString(t, part.ProviderDeltaNS, "0")
	snQAString(t, part.TotalResidualNS, "0")
	if len(part.Attempts) != 1 || part.Attempts[0].ID != claim.item.Plan.Parts[0].Attempts[0].ID || part.Attempts[0].RequestID != in.RequestID || part.Attempts[0].Number != "1" || part.Attempts[0].State != "synced" || part.Attempts[0].FailureCategory != nil {
		t.Fatal("ACK lost or duplicated original attempt")
	}
	snQAString(t, part.Attempts[0].EntryID, "901")
	stored := complete.requests[in.RequestID].Value
	if stored.PendingSync != nil || !reflect.DeepEqual(stored.SyncRun, &run) || stored.Fingerprint != mutationFingerprint("sync.now", SyncRunInput{RequestID: in.RequestID, Limit: 20}) {
		t.Fatal("terminal typed run/normalized limit not atomic")
	}
	status, err := q.reopen().Status(context.Background())
	if err != nil || status.Worker.QueuedCount != 0 || status.Worker.SubmittingCount != 0 || status.Worker.UnknownCount != 0 || len(status.ProjectTimers) != 1 || status.ProjectTimers[0].SyncedCount != 1 {
		t.Fatal("public activity status omitted completed sync graph", err)
	}
	replay, err := q.reopen().SyncNow(context.Background(), in, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(replay, run) {
		t.Fatal("same-request offline completed replay", err)
	}
	snQANonceOnly(t, complete, snQARead(t, q, in.RequestID))
	freshID := snQAID(11)
	fresh, err := q.reopen().SyncNow(context.Background(), SyncRunInput{RequestID: freshID}, qaSyncNoProvider(t))
	if err != nil || fresh.State != "complete" || fresh.RequestID != freshID || fresh.AttemptedIDs == nil || len(fresh.AttemptedIDs) != 0 || fresh.ResolvedIDs == nil || len(fresh.ResolvedIDs) != 0 || fresh.BlockedIDs == nil || len(fresh.BlockedIDs) != 0 || fresh.RemainingCount != 0 {
		t.Fatal("fresh empty run retried completed root", err)
	}
	after := snQARead(t, q, in.RequestID, freshID)
	if len(p.posts) != 1 || !reflect.DeepEqual(after.item, complete.item) || !reflect.DeepEqual(after.requests[in.RequestID], complete.requests[in.RequestID]) || !reflect.DeepEqual(after.requests[freshID].Value.SyncRun, &fresh) {
		t.Fatal("completed history changed or posted again")
	}
}

func TestSQLiteFirstSyncNowPublicInterruptedACKNeverAutoRetries(t *testing.T) {
	q := snQACaptured(t)
	p := qaNewSyncProvider(t)
	p.returnedHours = "0.010" // Mock returns a successful completed entry after beforeCreate.
	in := SyncRunInput{RequestID: snQAID(20)}
	var armed, faulted, rolledBack atomic.Bool
	var claim snQASnapshot
	p.beforeCreate = func(harvest.Object) {
		if len(p.posts) != 1 {
			t.Fatal("interrupted ACK already caused a repeated POST")
		}
		claim = snQAClaim(t, q, in.RequestID)
		armed.Store(true)
	}
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		if e.Phase == "rollback-after" && e.Code == 0 && faulted.Load() {
			rolledBack.Store(true)
		}
	}, Fault: func(e spQASQLEvent) error {
		if e.Phase == "commit-before-dispatch" && armed.Load() && faulted.CompareAndSwap(false, true) {
			return &sqliteio.Error{Phase: sqliteio.CommitPhase, Category: sqliteio.IO, Code: 10}
		}
		return nil
	}})
	run, err := q.s.SyncNow(context.Background(), in, qaSyncDeps(t, p))
	spQASetSQLHooks(spQASQLHooks{})
	var domain *Error
	if !reflect.DeepEqual(run, SyncRun{}) || !errors.As(err, &domain) || domain.Code != "local_write_unknown" || !domain.Uncertain || len(domain.Details) != 1 || domain.Details["request_id"] != in.RequestID {
		t.Fatal("post-success ACK failure was not request-scoped unknown", err)
	}
	if !faulted.Load() || !rolledBack.Load() || len(p.posts) != 1 || claim.item.ID == "" {
		t.Fatal("fault missed actual post-Create ACK COMMIT/rollback")
	}
	failed := snQARead(t, q, in.RequestID)
	if !reflect.DeepEqual(failed, claim) {
		t.Fatal("failed ACK changed durable submitting claim or invented saved response")
	}
	recovered, err := q.reopen().SyncNow(context.Background(), in, qaSyncNoProvider(t))
	if err != nil || recovered.State != "interrupted" || recovered.RequestID != in.RequestID || !reflect.DeepEqual(recovered.AttemptedIDs, []string{claim.item.ID}) || !reflect.DeepEqual(recovered.BlockedIDs, []string{claim.item.ID}) || recovered.ResolvedIDs == nil || len(recovered.ResolvedIDs) != 0 || recovered.RemainingCount != 1 {
		t.Fatal("exact pending request recovery did not retain interrupted outcome", err)
	}
	unknown := snQARead(t, q, in.RequestID)
	item := unknown.item
	if item.State != "unknown" || item.RunRequestID != nil || item.RetryRequestID != nil || item.EntryID != nil || item.Plan == nil || len(item.Plan.Parts) != 1 || !reflect.DeepEqual(item.Interval, q.interval) || !reflect.DeepEqual(item.Plan.Configuration, claim.item.Plan.Configuration) {
		t.Fatal("interrupted ACK lost immutable graph or became retryable")
	}
	part := item.Plan.Parts[0]
	beforePart := claim.item.Plan.Parts[0]
	if part.State != "unknown" || part.ID != beforePart.ID || part.Notes != beforePart.Notes || part.Correlation != beforePart.Correlation || part.PlannedHours != beforePart.PlannedHours || part.EntryID != nil || part.ReturnedHours != nil || part.ConfirmedDurationNS != nil || len(part.Attempts) != 1 || part.Attempts[0].ID != beforePart.Attempts[0].ID || part.Attempts[0].RequestID != in.RequestID || part.Attempts[0].Number != "1" || part.Attempts[0].State != "unknown" || part.Attempts[0].EntryID != nil {
		t.Fatal("recovery fabricated ACK or changed attempt identity")
	}
	snQAString(t, item.FailureCategory, "interrupted_submission")
	snQAString(t, part.FailureCategory, "interrupted_submission")
	snQAString(t, part.Attempts[0].FailureCategory, "interrupted_submission")
	if unknown.requests[in.RequestID].Value.PendingSync != nil || !reflect.DeepEqual(unknown.requests[in.RequestID].Value.SyncRun, &recovered) {
		t.Fatal("recovery did not terminalize original typed request")
	}
	replay, err := q.reopen().SyncNow(context.Background(), in, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(replay, recovered) {
		t.Fatal("interrupted history replay changed", err)
	}
	snQANonceOnly(t, unknown, snQARead(t, q, in.RequestID))
	freshID := snQAID(21)
	fresh, err := q.reopen().SyncNow(context.Background(), SyncRunInput{RequestID: freshID}, qaSyncNoProvider(t))
	if err != nil || fresh.State != "complete" || fresh.AttemptedIDs == nil || len(fresh.AttemptedIDs) != 0 || fresh.ResolvedIDs == nil || len(fresh.ResolvedIDs) != 0 || fresh.BlockedIDs == nil || len(fresh.BlockedIDs) != 0 || fresh.RemainingCount != 1 {
		t.Fatal("fresh run automatically retried unknown submission", err)
	}
	after := snQARead(t, q, in.RequestID, freshID)
	if len(p.posts) != 1 || !reflect.DeepEqual(after.item, unknown.item) || !reflect.DeepEqual(after.requests[in.RequestID], unknown.requests[in.RequestID]) || !reflect.DeepEqual(after.requests[freshID].Value.SyncRun, &fresh) {
		t.Fatal("unknown attempt/history changed or POST repeated")
	}
	status, err := q.reopen().Status(context.Background())
	if err != nil || status.Worker.UnknownCount != 1 || status.Worker.SubmittingCount != 0 {
		t.Fatal("public activity status hid retained unknown graph", err)
	}
}
