//go:build (darwin || linux) && (amd64 || arm64)

package cli_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
)

// Real CLI -> private SQL Service -> Harvest HTTP client -> owned httptest.
// Synthetic Codex callbacks do not claim installed host delivery.
func TestSQLiteFirstCLISyncCompletedHTTPAndOfflineReplay(t *testing.T) {
	q := csQANew(t)
	linkArgs := []string{"link", "100", "--account", "11", "--task", "200", "--timezone", "UTC", "--path", q.cwd, "--request-id", csQAID(1)}
	var linked activity.BindingResult
	q.json(linkArgs, &linked)
	wantAttribution := activity.Attribution{AccountID: "11", UserID: "7", ProjectID: "100", TaskID: "200", Timezone: "UTC"}
	if !linked.Changed || linked.Binding.ID == "" || linked.Binding.Kind != "directory" || linked.Binding.Locator != q.cwd || linked.Binding.Attribution != wantAttribution || linked.SnapshotRevision != "1" {
		t.Fatal("CLI Link did not bind the discovered current user")
	}
	q.profile()
	q.hook(0, "SessionStart", "", "")
	q.hook(0, "UserPromptSubmit", "root-turn", "")
	q.hook(12, "SubagentStart", "child-turn", "child")
	q.hook(24, "Stop", "root-turn", "")
	q.hook(36, "SubagentStop", "child-turn", "child")
	var captured activity.ActivitySnapshot
	q.json([]string{"activity", "status"}, &captured)
	if captured.ComputerID == nil || len(captured.ClosedIntervals) != 1 || len(captured.Actors) != 2 || len(captured.Uncertainties) != 0 || len(captured.CaptureReviews) != 0 || captured.Worker.QueuedCount != 1 {
		t.Fatal("CLI hook prerequisite did not seal one clean union")
	}
	interval := captured.ClosedIntervals[0]
	if interval.ID == "" || interval.ComputerID != *captured.ComputerID || interval.Attribution != wantAttribution || interval.DurationNS != "36000000000" || !interval.Start.Equal(q.base) || !interval.End.Equal(q.base.Add(36*time.Second)) || len(interval.SegmentIDs) != 2 || interval.SegmentIDs[0] == interval.SegmentIDs[1] {
		t.Fatal("overlapping hook work was not the exact 36-second union")
	}
	q.captured.Store(true)
	var configured activity.SyncConfigurationResult
	q.json([]string{"sync", "configure", "--account", "11", "--user", "7", "--mode", "duration", "--duration-policy", "exact", "--if-revision", "0", "--yes", "--request-id", csQAID(2)}, &configured)
	if !configured.Changed || configured.Configuration.AccountID != "11" || configured.Configuration.UserID != "7" || configured.Configuration.Revision != "1" || configured.Configuration.Mode != "duration" || configured.Configuration.DurationPolicy != "exact" || configured.Configuration.Clock != nil {
		t.Fatal("CLI sync configuration lost explicit current-user duration consent")
	}
	var resumed activity.MutationResult
	q.json([]string{"sync", "resume", "--request-id", csQAID(3)}, &resumed)
	if !resumed.Changed || resumed.RequestID != csQAID(3) {
		t.Fatal("CLI sync resume did not enable the saved configuration")
	}
	var queued activity.SyncStatus
	q.json([]string{"sync", "status"}, &queued) // Separate public SyncStatus port prerequisite.
	if !queued.Enabled || len(queued.Configurations) != 1 || !reflect.DeepEqual(queued.Configurations[0], configured.Configuration) || len(queued.Items) != 1 || queued.Items[0].State != "queued" || queued.Items[0].Plan != nil || !reflect.DeepEqual(queued.Items[0].Interval, interval) || queued.Worker.QueuedCount != 1 {
		t.Fatal("CLI SyncStatus prerequisite omitted the unattempted captured root")
	}
	args := []string{"sync", "now", "--limit", "1", "--request-id", csQAID(4)}
	var run activity.SyncRun
	q.json(args, &run)
	q.mu.Lock()
	claim, posts, getReads, requests := q.claim, q.posts, q.getReads, append([]string{}, q.requests...)
	q.mu.Unlock()
	if posts != 1 || len(claim.Items) != 1 || claim.Items[0].ID != queued.Items[0].ID || !reflect.DeepEqual(claim.Items[0].Interval, interval) || run.ContractVersion != 1 || run.RequestID != csQAID(4) || run.State != "complete" || !reflect.DeepEqual(run.AttemptedIDs, []string{queued.Items[0].ID}) || !reflect.DeepEqual(run.ResolvedIDs, run.AttemptedIDs) || run.BlockedIDs == nil || len(run.BlockedIDs) != 0 || run.RemainingCount != 0 {
		t.Fatal("real HTTP upload did not complete exactly the claimed root")
	}
	wantRequests := []string{
		"GET /id/accounts", "GET /v2/users/me", "GET /v2/users/me/project_assignments",
		"GET /id/accounts", "GET /v2/users/me",
		"GET /id/accounts", "GET /v2/users/me", "GET /id/accounts", "GET /v2/users/me", "GET /v2/users/me/project_assignments", "GET /v2/company",
		"POST /v2/time_entries",
	}
	if !reflect.DeepEqual(requests, wantRequests) || getReads != 11 || q.providers.Load() != 3 || q.nativeAuth.Load() != 0 || q.store.gets != 0 || q.store.sets != 0 || q.store.deletes != 0 {
		t.Fatal("CLI provider scope, released GET ownership, or credential isolation changed")
	}
	q.offline.Store(true)
	q.server.Close() // Subsequent CLI operations must succeed without a reachable provider.
	credentials, providers := q.credentials.Load(), q.providers.Load()
	var complete activity.SyncStatus
	q.json([]string{"sync", "status"}, &complete)
	if len(complete.Items) != 1 || complete.Worker.QueuedCount != 0 || complete.Worker.SubmittingCount != 0 || complete.Worker.UnknownCount != 0 {
		t.Fatal("cold CLI sync status lost the completed root")
	}
	item := complete.Items[0]
	if item.ID != queued.Items[0].ID || item.State != "synced" || item.RunRequestID != nil || item.RetryRequestID != nil || !reflect.DeepEqual(item.Interval, interval) || item.Plan == nil || len(item.Plan.Parts) != 1 || item.Plan.CompanySource != "company_verified" || !reflect.DeepEqual(item.Plan.Configuration, configured.Configuration) {
		t.Fatal("cold ACK graph changed captured identity or consent")
	}
	part, before := item.Plan.Parts[0], claim.Items[0].Plan.Parts[0]
	if part.State != "synced" || part.ID != before.ID || part.Notes != before.Notes || part.Correlation != before.Correlation || !strings.HasPrefix(part.Notes, "Tempo activity [tempo:v1:"+part.ID+":") || !strings.HasSuffix(part.Notes, "]") || part.PlannedHours != "0.01" || part.DurationNS != "36000000000" || part.PlannedDurationNS != "36000000000" || part.PlannedResidualNS != "0" || part.StartedTime != nil || part.EndedTime != nil || len(part.Attempts) != 1 || part.Attempts[0].ID != before.Attempts[0].ID || part.Attempts[0].RequestID != csQAID(4) || part.Attempts[0].Number != "1" || part.Attempts[0].State != "synced" {
		t.Fatal("wire ACK changed the frozen part or duplicated its attempt")
	}
	for _, field := range []*string{item.EntryID, part.EntryID, part.Attempts[0].EntryID} {
		csQAString(t, field, "901")
	}
	csQAString(t, part.ReturnedHours, "0.01")
	csQAString(t, part.RoundedHours, "0.01")
	csQAString(t, part.ConfirmedDurationNS, "36000000000")
	csQAString(t, part.ProviderDeltaNS, "0")
	csQAString(t, part.TotalResidualNS, "0")
	if complete.Totals.ExactDurationNS != "36000000000" {
		t.Fatal("sync exact total changed")
	}
	csQAString(t, complete.Totals.PlannedDurationNS, "36000000000")
	csQAString(t, complete.Totals.ConfirmedDurationNS, "36000000000")
	csQAString(t, complete.Totals.TotalResidualNS, "0")
	var activityStatus activity.ActivitySnapshot
	q.json([]string{"activity", "status"}, &activityStatus)
	if !reflect.DeepEqual(activityStatus.ClosedIntervals, captured.ClosedIntervals) || len(activityStatus.ProjectTimers) != 1 || activityStatus.ProjectTimers[0].SyncedCount != 1 || activityStatus.Worker.QueuedCount != 0 || activityStatus.Worker.SubmittingCount != 0 || activityStatus.Worker.UnknownCount != 0 {
		t.Fatal("cold activity status lost uploaded capture history")
	}
	var replay activity.SyncRun
	q.json(args, &replay)
	if !reflect.DeepEqual(replay, run) {
		t.Fatal("offline exact CLI request replay changed its receipt")
	}
	var afterReplay activity.SyncStatus
	q.json([]string{"sync", "status"}, &afterReplay)
	if !reflect.DeepEqual(afterReplay, complete) {
		t.Fatal("offline exact replay changed sync history or public revision")
	}
	var empty activity.SyncRun
	q.json([]string{"sync", "now", "--limit", "1", "--request-id", csQAID(5)}, &empty)
	if empty.State != "complete" || empty.RequestID != csQAID(5) || empty.RemainingCount != 0 || empty.AttemptedIDs == nil || len(empty.AttemptedIDs) != 0 || empty.ResolvedIDs == nil || len(empty.ResolvedIDs) != 0 || empty.BlockedIDs == nil || len(empty.BlockedIDs) != 0 {
		t.Fatal("new empty CLI run attempted the completed root again")
	}
	var afterEmpty activity.SyncStatus
	q.json([]string{"sync", "status"}, &afterEmpty)
	if !reflect.DeepEqual(afterEmpty.Items, complete.Items) || !reflect.DeepEqual(afterEmpty.Configurations, complete.Configurations) || !reflect.DeepEqual(afterEmpty.Totals, complete.Totals) || !reflect.DeepEqual(afterEmpty.Accounting, complete.Accounting) || q.credentials.Load() != credentials || q.providers.Load() != providers || q.nativeAuth.Load() != 0 || q.store.gets != 0 || q.store.sets != 0 || q.store.deletes != 0 {
		t.Fatal("offline completion replay read credentials or changed completed billing")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.posts != 1 || !reflect.DeepEqual(q.requests, wantRequests) {
		t.Fatal("completed upload issued a second HTTP request")
	}
}

func csQAString(t *testing.T, got *string, want string) {
	t.Helper()
	if got == nil || *got != want {
		t.Fatal("missing or incorrect persisted decimal/identity")
	}
}
