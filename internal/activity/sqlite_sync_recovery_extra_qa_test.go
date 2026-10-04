//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/harvest"
)

func TestSQLiteSyncRecoveryExtraMultipartFrozenRejectedRetry(t *testing.T) {
	q, p, before := sxQAPartial(t, true)
	ctx := context.Background()
	configured, err := q.reopen().SyncConfigure(ctx, SyncConfigureInput{AccountID: "1", UserID: "2", Mode: "duration", DurationPolicy: "nearest-hundredth-hour", IfRevision: q.configuration.Revision, RequestID: snQAID(410), Confirmed: true}, qaSyncDeps(t, qaNewSyncProvider(t)))
	if err != nil || !configured.Changed || configured.Configuration.DurationPolicy != "nearest-hundredth-hour" {
		t.Fatal("later consent", err)
	}
	in := SyncResolveInput{RequestID: snQAID(411), OutboxID: before.item.ID, IfRevision: before.item.Revision, RetryRejected: true, Confirmed: true}
	result, err := q.reopen().SyncResolve(ctx, in, qaSyncNoProvider(t))
	if err != nil {
		t.Fatal("multipart retry authorization", err)
	}
	authorized := snQARead(t, q, snQAID(400), in.RequestID)
	want := before.item
	want.State, want.RetryRequestID, want.FailureCategory, want.Revision = "queued", &in.RequestID, nil, bump(want.Revision)
	if len(p.posts) != 2 || !reflect.DeepEqual(authorized.item, want) || !reflect.DeepEqual(before.requests[snQAID(400)], authorized.requests[snQAID(400)]) {
		t.Fatal("authorization changed frozen history or submitted")
	}
	srQAMutationReceipt(t, authorized, in, result)
	replay, err := q.reopen().SyncResolve(ctx, in, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(replay, result) {
		t.Fatal("authorization replay", err)
	}
	snQANonceOnly(t, authorized, snQARead(t, q, snQAID(400), in.RequestID))
	p.createErr = nil
	p.beforeCreate = func(payload harvest.Object) {
		if len(p.posts) != 3 || !reflect.DeepEqual(payload, p.posts[1]) {
			t.Fatal("retry did not reuse only second frozen payload")
		}
		status, err := q.reopen().Status(ctx)
		if err != nil || status.Worker.SubmittingCount != 1 {
			t.Fatal("retry POST retained SQL owner", err)
		}
		claim := snQARead(t, q, snQAID(400), in.RequestID, snQAID(412))
		if claim.item.Plan == nil || len(claim.item.Plan.Parts) != 2 {
			t.Fatal("retry claim lost multipart plan")
		}
		parts := claim.item.Plan.Parts
		if !reflect.DeepEqual(parts[0], before.item.Plan.Parts[0]) || len(parts[1].Attempts) != 2 || !reflect.DeepEqual(parts[1].Attempts[0], before.item.Plan.Parts[1].Attempts[0]) || parts[1].Attempts[1].State != "submitting" || parts[1].Attempts[1].RequestID != snQAID(412) {
			t.Fatal("retry claim replaced a success or prior attempt")
		}
	}
	run, err := q.reopen().SyncNow(ctx, SyncRunInput{RequestID: snQAID(412)}, qaSyncDeps(t, p))
	if err != nil || len(p.posts) != 3 || run.State != "complete" || !reflect.DeepEqual(run.ResolvedIDs, []string{before.item.ID}) || !reflect.DeepEqual(run.AttemptedIDs, []string{before.item.ID}) || len(run.BlockedIDs) != 0 || run.RemainingCount != 0 {
		t.Fatal("separate multipart retry Now", err)
	}
	after := snQARead(t, q, snQAID(400), in.RequestID, snQAID(412))
	if after.item.Plan == nil || len(after.item.Plan.Parts) != 2 {
		t.Fatal("retry lost multipart plan")
	}
	parts := after.item.Plan.Parts
	if after.item.State != "synced" || !reflect.DeepEqual(parts[0], before.item.Plan.Parts[0]) || !reflect.DeepEqual(after.item.Plan.Configuration, before.item.Plan.Configuration) || len(parts[1].Attempts) != 2 || !reflect.DeepEqual(parts[1].Attempts[0], before.item.Plan.Parts[1].Attempts[0]) || parts[1].Attempts[1].State != "synced" || parts[1].Attempts[1].Number != "2" || parts[1].Attempts[1].ID == parts[1].Attempts[0].ID || !reflect.DeepEqual(after.requests[in.RequestID], authorized.requests[in.RequestID]) || !reflect.DeepEqual(after.requests[snQAID(400)], before.requests[snQAID(400)]) {
		t.Fatal("multipart frozen success/consent/audit changed")
	}
	again, err := q.reopen().SyncNow(ctx, SyncRunInput{RequestID: snQAID(412)}, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(again, run) {
		t.Fatal("multipart run replay", err)
	}
	snQANonceOnly(t, after, snQARead(t, q, snQAID(400), in.RequestID, snQAID(412)))
}

func TestSQLiteSyncRecoveryExtraManualNeverAttemptedRemainder(t *testing.T) {
	q, p, before := sxQAPartial(t, false)
	ctx := context.Background()
	part := before.item.Plan.Parts[1]
	in := SyncResolveInput{RequestID: snQAID(420), OutboxID: before.item.ID, EntryID: "902", IfRevision: before.item.Revision, Confirmed: true}
	entry := srQAEntry(t, p.posts[0], "902")
	entry["spent_date"], entry["notes"], entry["hours"], entry["external_reference"] = part.SpentDate, part.Notes, json.Number(part.PlannedHours), nil
	firstEntry := srQAEntry(t, p.posts[0], "901")
	firstEntry["hours"] = json.Number(before.item.Plan.Parts[0].PlannedHours)
	p.entry, p.entryRows = entry, []harvest.Object{firstEntry, entry}
	reads := 0
	witness := func() {
		reads++
		status, err := q.reopen().Status(ctx)
		if err != nil || status.ComputerID == nil || *status.ComputerID != q.interval.ComputerID {
			t.Fatal("manual GET retained SQL owner", err)
		}
		current := snQARead(t, q, snQAID(400), snQAID(401), in.RequestID)
		row := current.requests[in.RequestID]
		if !reflect.DeepEqual(current.item, before.item) || row.Value.PendingSync == nil || row.Value.PendingSync.EffectCommitted || !reflect.DeepEqual(row.Value.PendingSync.Resolve, &in) || !reflect.DeepEqual(row.Value.PendingSync.RootIDs, []string{before.item.ID}) {
			t.Fatal("manual provider lacks exact pending remainder")
		}
	}
	p.beforeUser, p.beforeEntryList = witness, witness
	result, err := q.reopen().SyncResolve(ctx, in, qaSyncDeps(t, p))
	if err != nil || reads != 2 || len(p.posts) != 1 || len(p.listQueries) != 1 || !reflect.DeepEqual(p.listQueries[0], url.Values{"user_id": {"2"}}) || entry["external_reference"] != nil {
		t.Fatal("manual remainder scan/write boundary", err)
	}
	after := snQARead(t, q, snQAID(400), snQAID(401), in.RequestID)
	if after.item.Plan == nil || len(after.item.Plan.Parts) != 2 {
		t.Fatal("manual completion lost multipart plan")
	}
	got := after.item.Plan.Parts[1]
	if after.item.State != "synced" || after.meta.SyncEnabled || len(got.Attempts) != 0 || !reflect.DeepEqual(after.item.Plan.Parts[0], before.item.Plan.Parts[0]) || !reflect.DeepEqual(after.item.Plan.Configuration, before.item.Plan.Configuration) || !reflect.DeepEqual(after.requests[snQAID(400)], before.requests[snQAID(400)]) || !reflect.DeepEqual(got.Attachment, &SyncAttachment{RequestID: in.RequestID, EntryID: "902"}) {
		t.Fatal("manual completion fabricated attempt or rewrote successful part")
	}
	snQAString(t, got.ConfirmedDurationNS, "18000000000")
	srQAMutationReceipt(t, after, in, result)
	replay, err := q.reopen().SyncResolve(ctx, in, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(result, replay) {
		t.Fatal("manual remainder replay", err)
	}
	snQANonceOnly(t, after, snQARead(t, q, snQAID(400), snQAID(401), in.RequestID))
}

func TestSQLiteSyncRecoveryExtraInterruptedReconcileReceipt(t *testing.T) {
	for _, saved := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-saved-effect", true: "saved-ack"}[saved], func(t *testing.T) {
			q, p, before := srQAFailedSubmission(t, false)
			in := SyncReconcileInput{RequestID: snQAID(450), OutboxID: before.item.ID}
			normalized := in
			normalized.Limit = 20
			if saved {
				p.entryRows = []harvest.Object{srQAEntry(t, p.posts[0], "901")}
			}
			var armed, faulted, rolledBack atomic.Bool
			var commits, dispatched atomic.Int32
			target := int32(1)
			if saved {
				target = 2
			}
			p.beforeEntryList = func() { srQAReserved(t, q, before, in.RequestID, &normalized, nil); armed.Store(true) }
			spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
				if armed.Load() && e.Phase == "commit-after-engine" && e.Code == 101 {
					commits.Add(1)
				}
				if faulted.Load() && e.Phase == "rollback-after" && e.Code == 0 {
					rolledBack.Store(true)
				}
			}, Fault: func(e spQASQLEvent) error {
				if armed.Load() && e.Phase == "commit-before-dispatch" && dispatched.Add(1) == target && faulted.CompareAndSwap(false, true) {
					return &sqliteio.Error{Phase: sqliteio.CommitPhase, Category: sqliteio.IO, Code: 10}
				}
				return nil
			}})
			run, err := q.reopen().SyncReconcile(context.Background(), in, qaSyncDeps(t, p))
			spQASetSQLHooks(spQASQLHooks{})
			if err == nil || !reflect.DeepEqual(run, SyncRun{}) || !faulted.Load() || !rolledBack.Load() || dispatched.Load() != target || commits.Load() != target-1 || len(p.posts) != 1 {
				t.Fatal("missed actual final receipt COMMIT/rollback", err)
			}
			if saved {
				sxQAUnknown(t, err, in.RequestID)
			}
			failed := snQARead(t, q, snQAID(300), in.RequestID)
			pending := failed.requests[in.RequestID].Value.PendingSync
			if pending == nil || pending.EffectCommitted != saved || !reflect.DeepEqual(pending.Reconcile, &normalized) || !reflect.DeepEqual(pending.RootIDs, []string{before.item.ID}) {
				t.Fatal("interruption lost exact pending/effect witness")
			}
			if saved {
				srQASynced(t, before.item, failed.item, "901", "")
			} else if !reflect.DeepEqual(before.item, failed.item) {
				t.Fatal("failed zero-effect completion changed unknown item")
			}
			p.beforeEntryList = nil
			recovered, err := q.reopen().SyncReconcile(context.Background(), in, qaSyncNoProvider(t))
			if saved {
				if err != nil || recovered.State != "interrupted" || !reflect.DeepEqual(recovered.ResolvedIDs, []string{before.item.ID}) || recovered.AttemptedIDs == nil || len(recovered.AttemptedIDs) != 0 || recovered.RemainingCount != 0 {
					t.Fatal("saved effect did not recover original interrupted run", err)
				}
			} else {
				sxQAUnknown(t, err, in.RequestID)
				if !reflect.DeepEqual(recovered, SyncRun{}) {
					t.Fatal("zero-effect recovery claimed result")
				}
			}
			after := snQARead(t, q, snQAID(300), in.RequestID)
			r := after.requests[in.RequestID].Value
			if r.PendingSync != nil || !reflect.DeepEqual(after.item, failed.item) || !reflect.DeepEqual(after.requests[snQAID(300)], before.requests[snQAID(300)]) {
				t.Fatal("recovery rewrote retained graph")
			}
			if saved {
				if r.Error != nil || !reflect.DeepEqual(r.SyncRun, &recovered) {
					t.Fatal("interrupted terminal receipt")
				}
			} else if r.Error == nil || r.Error.Code != "local_write_unknown" || r.SyncRun != nil {
				t.Fatal("zero-effect terminal error receipt")
			}
			again, err := q.reopen().SyncReconcile(context.Background(), in, qaSyncNoProvider(t))
			if saved {
				if err != nil || !reflect.DeepEqual(again, recovered) {
					t.Fatal("historical interrupted replay", err)
				}
			} else {
				sxQAUnknown(t, err, in.RequestID)
			}
			replayed := snQARead(t, q, snQAID(300), in.RequestID)
			snQANonceOnly(t, after, replayed)
			changed := in
			changed.Limit = 1
			_, err = q.reopen().SyncReconcile(context.Background(), changed, qaSyncNoProvider(t))
			qaCode(t, err, "request_conflict")
			if !reflect.DeepEqual(replayed, snQARead(t, q, snQAID(300), in.RequestID)) {
				t.Fatal("changed request mutated receipt/nonce")
			}
			fresh, err := q.reopen().SyncNow(context.Background(), SyncRunInput{RequestID: snQAID(451)}, qaSyncNoProvider(t))
			if err != nil || fresh.AttemptedIDs == nil || len(fresh.AttemptedIDs) != 0 || len(p.posts) != 1 {
				t.Fatal("reconcile recovery authorized rePOST", err)
			}
		})
	}
}

func TestSQLiteSyncRecoveryExtraManualProviderBoundaries(t *testing.T) {
	for _, mode := range []string{"factory-error", "other-user", "entry-error", "list-error", "list-payload-mismatch"} {
		t.Run(mode, func(t *testing.T) {
			q, p, before := srQAFailedSubmission(t, false)
			in := SyncResolveInput{RequestID: snQAID(460), OutboxID: before.item.ID, EntryID: "901", IfRevision: before.item.Revision, Confirmed: true}
			p.entry = srQAEntry(t, p.posts[0], "901")
			p.entryRows = []harvest.Object{srQAEntry(t, p.posts[0], "901")}
			want := "network"
			switch mode {
			case "other-user":
				want = "attribution_conflict"
			case "entry-error":
				p.entryErr = errors.New("PRIVATE_RECOVERY_TRANSPORT")
			case "list-error":
				p.entryListErr = errors.New("PRIVATE_RECOVERY_TRANSPORT")
			case "list-payload-mismatch":
				p.entryRows[0]["task"] = harvest.Object{"id": json.Number("99")}
				want = "conflict"
			}
			factories := 0
			witness := func() { srQAReserved(t, q, before, in.RequestID, nil, &in) }
			p.beforeUser, p.beforeEntryList = witness, witness
			deps := SyncDependencies{NewProvider: func(_ context.Context, account string) (harvest.Provider, error) {
				factories++
				witness()
				if account != "1" {
					t.Fatal("wrong stored account")
				}
				if mode == "factory-error" {
					return nil, errors.New("PRIVATE_RECOVERY_TRANSPORT")
				}
				if mode == "other-user" {
					return sxQAOtherUser{p}, nil
				}
				return p, nil
			}}
			result, err := q.reopen().SyncResolve(context.Background(), in, deps)
			qaCode(t, err, want)
			if factories != 1 || !reflect.DeepEqual(result, MutationResult{}) || len(p.posts) != 1 || strings.Contains(err.Error(), "PRIVATE_RECOVERY") {
				t.Fatal("provider refusal leaked, POSTed or returned success")
			}
			wantCalls := []string{"accounts", "/users/me", "/time_entries/901", "/time_entries"}
			if mode == "factory-error" {
				wantCalls = nil
			} else if mode == "other-user" {
				wantCalls = wantCalls[:2]
			} else if mode == "entry-error" {
				wantCalls = wantCalls[:3]
			}
			if !reflect.DeepEqual(p.calls, wantCalls) {
				t.Fatal("refusal crossed wrong remote boundary")
			}
			if len(wantCalls) == 4 && (len(p.listQueries) != 1 || !reflect.DeepEqual(p.listQueries[0], url.Values{"user_id": {"2"}})) {
				t.Fatal("manual scan hid current-user collision candidates")
			}
			after := snQARead(t, q, snQAID(300), in.RequestID)
			r := after.requests[in.RequestID].Value
			if !reflect.DeepEqual(before.item, after.item) || r.PendingSync == nil || r.PendingSync.EffectCommitted || !reflect.DeepEqual(r.PendingSync.Resolve, &in) || r.MutationResult != nil || r.Error != nil {
				t.Fatal("GET refusal changed attempted graph or claimed terminal success")
			}
			srQAProgress(t, before, after)
			_, err = q.reopen().SyncResolve(context.Background(), in, qaSyncNoProvider(t))
			sxQAUnknown(t, err, in.RequestID)
			recovered := snQARead(t, q, snQAID(300), in.RequestID)
			terminal := recovered.requests[in.RequestID].Value
			if !reflect.DeepEqual(recovered.item, before.item) || terminal.PendingSync != nil || terminal.Error == nil || terminal.Error.Code != "local_write_unknown" || terminal.MutationResult != nil {
				t.Fatal("failed attachment recovered as success")
			}
			_, err = q.reopen().SyncResolve(context.Background(), in, qaSyncNoProvider(t))
			sxQAUnknown(t, err, in.RequestID)
			snQANonceOnly(t, recovered, snQARead(t, q, snQAID(300), in.RequestID))
			fresh, err := q.reopen().SyncNow(context.Background(), SyncRunInput{RequestID: snQAID(461)}, qaSyncNoProvider(t))
			if err != nil || fresh.AttemptedIDs == nil || len(fresh.AttemptedIDs) != 0 || fresh.RemainingCount != 1 || len(p.posts) != 1 {
				t.Fatal("failed attachment authorized rePOST", err)
			}
		})
	}
}
