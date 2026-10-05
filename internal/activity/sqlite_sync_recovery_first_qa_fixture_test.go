//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/rbeene/tempo/internal/harvest"
)

// These fixtures compose public SQLite operations. snQARead independently
// decodes the committed graph and recomputes all 32 tables' logical charge.
// No legacy state, SQL seed, write hook, or additional provider seam is used.
func srQAFailedSubmission(t *testing.T, rejected bool) (*snQAFixture, *qaSyncProvider, snQASnapshot) {
	t.Helper()
	q := snQACaptured(t)
	p := qaNewSyncProvider(t)
	p.returnedHours = "0.01"
	p.createErr = &harvest.Error{Code: "timeout", Uncertain: true}
	want := "unknown"
	if rejected {
		p.createErr = &harvest.Error{Code: "validation", Status: 422}
		want = "rejected"
	}
	posts := 0
	p.beforeCreate = func(payload harvest.Object) {
		posts++
		claim := snQAClaim(t, q, snQAID(300))
		part := claim.item.Plan.Parts[0]
		expected := harvest.Object{"user_id": json.Number("2"), "project_id": json.Number("3"), "task_id": json.Number("4"), "spent_date": "2026-10-02", "hours": json.Number("0.01"), "notes": part.Notes, "external_reference": harvest.Object{"id": part.Correlation, "group_id": claim.item.ID, "account_id": q.interval.ComputerID}}
		if !reflect.DeepEqual(payload, expected) {
			t.Fatal("SETUP POST is not the actual frozen 36-second plan")
		}
	}
	syncContext, syncDiagnostics := withSQLiteSyncFailureDiagnostics(context.Background())
	postsBefore := len(p.posts)
	run, err := q.reopen().SyncNow(syncContext, SyncRunInput{RequestID: snQAID(300)}, qaSyncDeps(t, p))
	if err != nil {
		t.Log(sqliteSyncFailureDiagnosticLog(syncDiagnostics, true, len(p.posts) == postsBefore))
		t.Fatal("SETUP existing public SyncNow prerequisite", err)
	}
	after := snQARead(t, q, snQAID(300))
	if posts != 1 || len(p.posts) != 1 || run.State != "complete" || !reflect.DeepEqual(run.AttemptedIDs, []string{after.item.ID}) || !reflect.DeepEqual(run.BlockedIDs, []string{after.item.ID}) || run.ResolvedIDs == nil || len(run.ResolvedIDs) != 0 || run.RemainingCount != 1 || after.item.State != want || after.item.Plan == nil || len(after.item.Plan.Parts) != 1 || after.item.RunRequestID != nil || after.item.RetryRequestID != nil {
		t.Fatal("SETUP actual failed submission/run was not retained", want)
	}
	part := after.item.Plan.Parts[0]
	if part.State != want || part.DurationNS != "36000000000" || part.PlannedHours != "0.01" || part.PlannedDurationNS != "36000000000" || part.PlannedResidualNS != "0" || part.EntryID != nil || len(part.Attempts) != 1 || part.Attempts[0].State != want || part.Attempts[0].RequestID != snQAID(300) || part.Attempts[0].Number != "1" || !validUUID(part.Attempts[0].ID) {
		t.Fatal("SETUP genuine failed attempt or exact amount missing")
	}
	row, ok := after.requests[snQAID(300)]
	if !ok || row.Value.PendingSync != nil || !reflect.DeepEqual(row.Value.SyncRun, &run) || row.Value.Operation != "sync.now" {
		t.Fatal("SETUP original complete run receipt missing")
	}
	p.beforeCreate = nil
	p.calls = nil
	return q, p, after
}

// This is an owned response for exactly 36 seconds, unlike qaSyncEntryFromPost's
// unrelated 0.040-hour legacy fixture. Clone the complete wire object first.
func srQAEntry(t *testing.T, posted harvest.Object, id string) harvest.Object {
	t.Helper()
	b, err := json.Marshal(posted)
	if err != nil {
		t.Fatal(err)
	}
	var out harvest.Object
	if err = json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	out["id"], out["hours"], out["rounded_hours"] = json.Number(id), json.Number("0.01"), json.Number("0.25")
	out["is_running"] = false
	out["user"], out["project"], out["task"] = harvest.Object{"id": json.Number("2")}, harvest.Object{"id": json.Number("3")}, harvest.Object{"id": json.Number("4")}
	return out
}

func srQAPrior(t *testing.T, before, after snQASnapshot) {
	t.Helper()
	if !reflect.DeepEqual(before.requests[snQAID(300)], after.requests[snQAID(300)]) || !reflect.DeepEqual(before.item.Interval, after.item.Interval) || before.item.ID != after.item.ID {
		t.Fatal("recovery rewrote original run history, interval or root identity")
	}
}

func srQAProgress(t *testing.T, before, after snQASnapshot) {
	t.Helper()
	a, ok := counter(before.meta.Revision)
	if !ok {
		t.Fatal("before revision")
	}
	b, ok := counter(after.meta.Revision)
	if !ok || b <= a || before.meta.DurabilityNonce == after.meta.DurabilityNonce {
		t.Fatal("successful recovery lacks committed revision/nonce progress")
	}
	srQAPrior(t, before, after)
}

// Called from real provider callbacks while a sync guard remains held. Both a
// public cold reader and a separately opened typed SQL reader must finish.
func srQAReserved(t *testing.T, q *snQAFixture, before snQASnapshot, id string, reconcile *SyncReconcileInput, resolve *SyncResolveInput) {
	t.Helper()
	statusContext, statusDiagnostics := withSQLiteStatusFailureDiagnostics(context.Background())
	status, err := q.reopen().Status(statusContext)
	if err != nil || status.Worker.UnknownCount != 1 || status.Worker.SubmittingCount != 0 {
		t.Log(sqliteStatusFailureDiagnosticLog(statusDiagnostics, true))
		t.Fatal("provider retained SQL ownership or lost unknown state", err)
	}
	current := snQARead(t, q, snQAID(300), id)
	r, ok := current.requests[id]
	if !ok || r.Value.PendingSync == nil || r.Value.SyncRun != nil || r.Value.MutationResult != nil || r.Value.Error != nil {
		t.Fatal("provider lacks real pending request")
	}
	pending := r.Value.PendingSync
	if pending.Run != nil || pending.EffectCommitted || !reflect.DeepEqual(pending.Reconcile, reconcile) || !reflect.DeepEqual(pending.Resolve, resolve) || !reflect.DeepEqual(pending.RootIDs, []string{before.item.ID}) || pending.SnapshotRevision != current.meta.Revision || !reflect.DeepEqual(current.item, before.item) {
		t.Fatal("reservation changed the attempted graph or admitted wrong targets")
	}
	srQAProgress(t, before, current)
}

func srQASynced(t *testing.T, before, after OutboxItem, entry, attachment string) {
	t.Helper()
	if after.State != "synced" || after.Revision != bump(before.Revision) || after.RunRequestID != nil || after.RetryRequestID != nil || after.FailureCategory != nil || after.Plan == nil || len(after.Plan.Parts) != 1 || !reflect.DeepEqual(before.Plan.Configuration, after.Plan.Configuration) || before.Plan.CompanySource != after.Plan.CompanySource {
		t.Fatal("resolution did not preserve frozen root/plan")
	}
	snQAString(t, after.EntryID, entry)
	b, a := before.Plan.Parts[0], after.Plan.Parts[0]
	if a.State != "synced" || a.FailureCategory != nil || len(a.Attempts) != 1 {
		t.Fatal("resolution fabricated an attempt or retained a blocker")
	}
	snQAString(t, a.EntryID, entry)
	snQAString(t, a.ReturnedHours, "0.01")
	snQAString(t, a.RoundedHours, "0.25")
	snQAString(t, a.ConfirmedDurationNS, "36000000000")
	snQAString(t, a.ProviderDeltaNS, "0")
	snQAString(t, a.TotalResidualNS, "0")
	wantAttempt := b.Attempts[0]
	wantAttempt.State, wantAttempt.EntryID, wantAttempt.FailureCategory = "synced", &entry, nil
	if !reflect.DeepEqual(a.Attempts[0], wantAttempt) {
		t.Fatal("resolution replaced original attempt identity")
	}
	if attachment == "" {
		if a.Attachment != nil {
			t.Fatal("automatic reconcile invented manual attachment")
		}
	} else if !reflect.DeepEqual(a.Attachment, &SyncAttachment{RequestID: attachment, EntryID: entry}) {
		t.Fatal("manual attachment audit missing")
	}
	// Normalize only the enumerated acknowledgement fields; all other payload
	// fields must remain byte/value identical to the actual attempted plan.
	a.State, a.EntryID, a.FailureCategory, a.ReturnedHours, a.RoundedHours = b.State, b.EntryID, b.FailureCategory, b.ReturnedHours, b.RoundedHours
	a.ConfirmedDurationNS, a.ProviderDeltaNS, a.TotalResidualNS = b.ConfirmedDurationNS, b.ProviderDeltaNS, b.TotalResidualNS
	a.Attempts, a.Attachment = b.Attempts, b.Attachment
	if !reflect.DeepEqual(a, b) {
		t.Fatal("resolution rewrote immutable attempted payload")
	}
}

func srQAMutationReceipt(t *testing.T, after snQASnapshot, in SyncResolveInput, result MutationResult) {
	t.Helper()
	r, ok := after.requests[in.RequestID]
	if result.ContractVersion != 1 || result.RequestID != in.RequestID || !result.Changed || result.SnapshotRevision != after.meta.Revision || !reflect.DeepEqual(result.AffectedIDs, []string{after.item.ID}) {
		t.Fatal("manual/retry result identity or revision")
	}
	snQAString(t, result.EntityRevision, after.item.Revision)
	if !ok || r.Value.Operation != "sync.resolve" || r.Value.Fingerprint != mutationFingerprint("sync.resolve", in) || r.Value.PendingSync != nil || r.Value.Error != nil || r.Value.SyncRun != nil || !reflect.DeepEqual(r.Value.MutationResult, &result) {
		t.Fatal("typed terminal resolve receipt mismatch")
	}
}
