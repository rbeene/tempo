//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"encoding/json"
	"net/url"
	"reflect"
	"testing"

	"github.com/rbeene/tempo/internal/harvest"
)

func TestSQLiteSyncRecoveryFirstReconcileUniqueCompletePayload(t *testing.T) {
	for _, name := range []string{"exact", "absent", "collision", "wrong-project"} {
		t.Run(name, func(t *testing.T) {
			q, p, before := srQAFailedSubmission(t, false)
			entry := srQAEntry(t, p.posts[0], "901")
			p.entryRows = []harvest.Object{entry}
			switch name {
			case "absent":
				p.entryRows = []harvest.Object{}
			case "collision":
				other := srQAEntry(t, p.posts[0], "902")
				other["task"] = harvest.Object{"id": json.Number("99")}
				p.entryRows = append(p.entryRows, other)
			case "wrong-project":
				entry["project"] = harvest.Object{"id": json.Number("99")}
			}
			in := SyncReconcileInput{RequestID: snQAID(310), OutboxID: before.item.ID}
			normalized := in
			normalized.Limit = 20
			identityReads, listReads := 0, 0
			p.beforeUser = func() { identityReads++; srQAReserved(t, q, before, in.RequestID, &normalized, nil) }
			p.beforeEntryList = func() { listReads++; srQAReserved(t, q, before, in.RequestID, &normalized, nil) }
			run, err := q.reopen().SyncReconcile(context.Background(), in, qaSyncDeps(t, p))
			if err != nil {
				t.Fatal("public SQLite Reconcile", err)
			}
			if identityReads != 1 || listReads != 1 || !reflect.DeepEqual(p.calls, []string{"accounts", "/users/me", "/time_entries"}) || len(p.listQueries) != 1 || !reflect.DeepEqual(p.listQueries[0], url.Values{"user_id": {"2"}, "external_reference_id": {before.item.Plan.Parts[0].Correlation}}) || len(p.posts) != 1 {
				t.Fatal("reconcile hid candidates, skipped current-user/released-reader checks, or POSTed")
			}
			after := snQARead(t, q, snQAID(300), in.RequestID)
			srQAProgress(t, before, after)
			row, ok := after.requests[in.RequestID]
			if !ok || row.Value.Operation != "sync.reconcile" || row.Value.Fingerprint != mutationFingerprint("sync.reconcile", normalized) || row.Value.PendingSync != nil || row.Value.Error != nil || !reflect.DeepEqual(row.Value.SyncRun, &run) || run.ContractVersion != 1 || run.RequestID != in.RequestID || run.SnapshotRevision != after.meta.Revision || run.State != "complete" || run.AttemptedIDs == nil || len(run.AttemptedIDs) != 0 || run.ResolvedIDs == nil || run.BlockedIDs == nil {
				t.Fatal("reconcile terminal receipt or no-new-attempt result")
			}
			if name == "exact" {
				srQASynced(t, before.item, after.item, "901", "")
				if !reflect.DeepEqual(run.ResolvedIDs, []string{before.item.ID}) || len(run.BlockedIDs) != 0 || run.RemainingCount != 0 {
					t.Fatal("resolved root result")
				}
			} else {
				if len(run.ResolvedIDs) != 0 || !reflect.DeepEqual(run.BlockedIDs, []string{before.item.ID}) || run.RemainingCount != 1 || !reflect.DeepEqual(before.item.Plan, after.item.Plan) || after.item.RetryRequestID != nil {
					t.Fatal("unresolved result rewrote attempt or authorized retry")
				}
				want := before.item
				if name != "absent" {
					category := "response_mismatch"
					if name == "collision" {
						category = "correlation_collision"
					}
					want.State, want.FailureCategory, want.Revision = "needs_attention", &category, bump(want.Revision)
				}
				if !reflect.DeepEqual(want, after.item) {
					t.Fatal("wrong unresolved-root disposition")
				}
			}
			replay, err := q.reopen().SyncReconcile(context.Background(), in, qaSyncNoProvider(t))
			if err != nil || !reflect.DeepEqual(run, replay) {
				t.Fatal("offline exact reconcile replay", err)
			}
			snQANonceOnly(t, after, snQARead(t, q, snQAID(300), in.RequestID))
			if name != "exact" {
				freshContext := context.Background()
				var diagnostic *sqliteSyncFailureDiagnostics
				if name == "wrong-project" {
					freshContext, diagnostic = withSQLiteSyncFailureDiagnostics(freshContext)
				}
				fresh, err := q.reopen().SyncNow(freshContext, SyncRunInput{RequestID: snQAID(311)}, qaSyncNoProvider(t))
				if name == "wrong-project" && err != nil {
					t.Log(sqliteSyncFailureDiagnosticLog(diagnostic, true, len(p.posts) == 1))
				}
				if err != nil || fresh.State != "complete" || fresh.AttemptedIDs == nil || len(fresh.AttemptedIDs) != 0 || fresh.RemainingCount != 1 || len(p.posts) != 1 {
					t.Fatal("reconciliation absence/conflict authorized another POST", err)
				}
				if got := snQARead(t, q, snQAID(300), in.RequestID); !reflect.DeepEqual(after.item, got.item) || !reflect.DeepEqual(after.requests, got.requests) {
					t.Fatal("fresh empty run rewrote recovery evidence")
				}
			}
		})
	}
}

func TestSQLiteSyncRecoveryFirstManualNotesAttachmentAndCollisionScan(t *testing.T) {
	for _, name := range []string{"unique", "same-notes", "same-reference-other-notes"} {
		t.Run(name, func(t *testing.T) {
			q, p, before := srQAFailedSubmission(t, false)
			in := SyncResolveInput{RequestID: snQAID(320), OutboxID: before.item.ID, EntryID: "901", IfRevision: before.item.Revision, Confirmed: true}
			bad := in
			bad.Confirmed = false
			_, err := q.reopen().SyncResolve(context.Background(), bad, qaSyncNoProvider(t))
			qaCode(t, err, "confirmation_required")
			bad = in
			bad.IfRevision = "0"
			_, err = q.reopen().SyncResolve(context.Background(), bad, qaSyncNoProvider(t))
			qaCode(t, err, "revision_conflict")
			if got := snQARead(t, q, snQAID(300), in.RequestID); !reflect.DeepEqual(before, got) {
				t.Fatal("preflight refusal admitted a request or changed charge/nonce")
			}
			p.entry = srQAEntry(t, p.posts[0], "901")
			p.entry["external_reference"] = nil
			p.entryRows = []harvest.Object{p.entry}
			if name != "unique" {
				other := srQAEntry(t, p.posts[0], "902")
				if name == "same-notes" {
					other["external_reference"] = nil
				} else {
					other["notes"] = "different notes with the same immutable correlation"
				}
				p.entryRows = append(p.entryRows, other)
			}
			identityReads, listReads := 0, 0
			p.beforeUser = func() { identityReads++; srQAReserved(t, q, before, in.RequestID, nil, &in) }
			p.beforeEntryList = func() { listReads++; srQAReserved(t, q, before, in.RequestID, nil, &in) }
			result, err := q.reopen().SyncResolve(context.Background(), in, qaSyncDeps(t, p))
			if identityReads != 1 || listReads != 1 || !reflect.DeepEqual(p.calls, []string{"accounts", "/users/me", "/time_entries/901", "/time_entries"}) || len(p.listQueries) != 1 || !reflect.DeepEqual(p.listQueries[0], url.Values{"user_id": {"2"}}) || len(p.posts) != 1 || p.entry["external_reference"] != nil {
				t.Fatal("manual attachment skipped full-user scan, retained SQL ownership, mutated provider row or POSTed")
			}
			after := snQARead(t, q, snQAID(300), in.RequestID)
			srQAProgress(t, before, after)
			if name != "unique" {
				qaCode(t, err, "conflict")
				row := after.requests[in.RequestID]
				if !reflect.DeepEqual(result, MutationResult{}) || !reflect.DeepEqual(before.item, after.item) || row.Value.PendingSync == nil || row.Value.PendingSync.EffectCommitted || row.Value.MutationResult != nil || row.Value.Error != nil {
					t.Fatal("collision claimed attachment or lost honest pending reservation")
				}
				// A failed external scan cannot become a successful local receipt.
				_, err = q.reopen().SyncResolve(context.Background(), in, qaSyncNoProvider(t))
				qaCode(t, err, "local_write_unknown")
				recovered := snQARead(t, q, snQAID(300), in.RequestID)
				receipt := recovered.requests[in.RequestID]
				if !reflect.DeepEqual(before.item, recovered.item) || receipt.Value.PendingSync != nil || receipt.Value.MutationResult != nil || receipt.Value.Error == nil || receipt.Value.Error.Code != "local_write_unknown" || !receipt.Value.Error.Uncertain {
					t.Fatal("pending collision replay fabricated a success")
				}
				srQAPrior(t, before, recovered)
				_, err = q.reopen().SyncResolve(context.Background(), in, qaSyncNoProvider(t))
				qaCode(t, err, "local_write_unknown")
				snQANonceOnly(t, recovered, snQARead(t, q, snQAID(300), in.RequestID))
				return
			}
			if err != nil {
				t.Fatal("public notes-only attachment", err)
			}
			srQASynced(t, before.item, after.item, "901", in.RequestID)
			srQAMutationReceipt(t, after, in, result)
			replay, err := q.reopen().SyncResolve(context.Background(), in, qaSyncNoProvider(t))
			if err != nil || !reflect.DeepEqual(result, replay) {
				t.Fatal("offline manual attachment replay", err)
			}
			snQANonceOnly(t, after, snQARead(t, q, snQAID(300), in.RequestID))
		})
	}
}

func TestSQLiteSyncRecoveryFirstUnknownCannotAuthorizeRetry(t *testing.T) {
	q, p, before := srQAFailedSubmission(t, false)
	in := SyncResolveInput{RequestID: snQAID(330), OutboxID: before.item.ID, IfRevision: before.item.Revision, RetryRejected: true, Confirmed: true}
	_, err := q.reopen().SyncResolve(context.Background(), in, qaSyncNoProvider(t))
	qaCode(t, err, "invalid_transition")
	if got := snQARead(t, q, snQAID(300), in.RequestID); !reflect.DeepEqual(before, got) {
		t.Fatal("unknown retry refusal mutated graph, requests, nonce or charge")
	}
	run, err := q.reopen().SyncNow(context.Background(), SyncRunInput{RequestID: snQAID(331)}, qaSyncNoProvider(t))
	if err != nil || run.State != "complete" || run.AttemptedIDs == nil || len(run.AttemptedIDs) != 0 || run.RemainingCount != 1 || len(p.posts) != 1 {
		t.Fatal("unknown failure was automatically retried", err)
	}
	after := snQARead(t, q, snQAID(300), in.RequestID, snQAID(331))
	if !reflect.DeepEqual(before.item, after.item) || after.requests[in.RequestID].Value.Operation != "" || !reflect.DeepEqual(after.requests[snQAID(331)].Value.SyncRun, &run) {
		t.Fatal("unknown intent or no-op receipt changed")
	}
	srQAPrior(t, before, after)
}

func TestSQLiteSyncRecoveryFirstDefiniteRejectedRetryRequiresSeparateNow(t *testing.T) {
	q, p, before := srQAFailedSubmission(t, true)
	in := SyncResolveInput{RequestID: snQAID(340), OutboxID: before.item.ID, IfRevision: before.item.Revision, RetryRejected: true, Confirmed: true}
	bad := in
	bad.Confirmed = false
	_, err := q.reopen().SyncResolve(context.Background(), bad, qaSyncNoProvider(t))
	qaCode(t, err, "confirmation_required")
	bad = in
	bad.IfRevision = "0"
	_, err = q.reopen().SyncResolve(context.Background(), bad, qaSyncNoProvider(t))
	qaCode(t, err, "revision_conflict")
	if got := snQARead(t, q, snQAID(300), in.RequestID); !reflect.DeepEqual(before, got) {
		t.Fatal("retry preflight changed state")
	}
	// A later configuration change cannot replace an already attempted payload.
	configured, err := q.reopen().SyncConfigure(context.Background(), SyncConfigureInput{AccountID: "1", UserID: "2", Mode: "duration", DurationPolicy: "nearest-hundredth-hour", IfRevision: q.configuration.Revision, RequestID: snQAID(341), Confirmed: true}, qaSyncDeps(t, qaNewSyncProvider(t)))
	if err != nil || !configured.Changed || configured.Configuration.DurationPolicy != "nearest-hundredth-hour" {
		t.Fatal("later configuration prerequisite", err)
	}
	current := snQARead(t, q, snQAID(300), in.RequestID)
	if !reflect.DeepEqual(before.item, current.item) {
		t.Fatal("configuration rewrote attempted plan")
	}
	result, err := q.reopen().SyncResolve(context.Background(), in, qaSyncNoProvider(t))
	if err != nil {
		t.Fatal("explicit rejected retry authorization", err)
	}
	authorized := snQARead(t, q, snQAID(300), in.RequestID)
	want := before.item
	want.State, want.RetryRequestID, want.FailureCategory, want.Revision = "queued", &in.RequestID, nil, bump(want.Revision)
	if len(p.posts) != 1 || !reflect.DeepEqual(want, authorized.item) {
		t.Fatal("authorization POSTed or rewrote frozen attempt/plan")
	}
	srQAProgress(t, current, authorized)
	srQAMutationReceipt(t, authorized, in, result)
	replay, err := q.reopen().SyncResolve(context.Background(), in, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(result, replay) {
		t.Fatal("offline retry authorization replay", err)
	}
	snQANonceOnly(t, authorized, snQARead(t, q, snQAID(300), in.RequestID))
	p.createErr = nil
	posts := 0
	p.beforeCreate = func(payload harvest.Object) {
		posts++
		status, err := q.reopen().Status(context.Background())
		if err != nil || status.Worker.SubmittingCount != 1 {
			t.Fatal("retry POST holds SQL ownership", err)
		}
		claim := snQARead(t, q, snQAID(300), in.RequestID, snQAID(342))
		part := claim.item.Plan.Parts[0]
		if !reflect.DeepEqual(payload, p.posts[0]) || len(p.posts) != 2 || claim.item.State != "submitting" || len(part.Attempts) != 2 || !reflect.DeepEqual(part.Attempts[0], before.item.Plan.Parts[0].Attempts[0]) || part.Attempts[1].RequestID != snQAID(342) || part.Attempts[1].Number != "2" || part.Attempts[1].State != "submitting" || part.Attempts[1].ID == part.Attempts[0].ID {
			t.Fatal("explicit new run did not preserve rejected audit and exact payload")
		}
		pending := claim.requests[snQAID(342)].Value.PendingSync
		if pending == nil || pending.Run == nil || pending.Run.RequestID != snQAID(342) || !reflect.DeepEqual(pending.RootIDs, []string{before.item.ID}) {
			t.Fatal("retry POST missing new actual reservation")
		}
	}
	run, err := q.reopen().SyncNow(context.Background(), SyncRunInput{RequestID: snQAID(342)}, qaSyncDeps(t, p))
	if err != nil || posts != 1 || len(p.posts) != 2 || run.State != "complete" || !reflect.DeepEqual(run.AttemptedIDs, []string{before.item.ID}) || !reflect.DeepEqual(run.ResolvedIDs, []string{before.item.ID}) || run.BlockedIDs == nil || len(run.BlockedIDs) != 0 || run.RemainingCount != 0 {
		t.Fatal("separately requested retry run", err)
	}
	after := snQARead(t, q, snQAID(300), in.RequestID, snQAID(342))
	part := after.item.Plan.Parts[0]
	if after.item.State != "synced" || after.item.RetryRequestID != nil || after.item.RunRequestID != nil || !reflect.DeepEqual(before.item.Plan.Configuration, after.item.Plan.Configuration) || len(part.Attempts) != 2 || !reflect.DeepEqual(before.item.Plan.Parts[0].Attempts[0], part.Attempts[0]) || part.Attempts[1].State != "synced" || part.Attempts[1].RequestID != snQAID(342) || part.Attempts[1].Number != "2" || part.Attempts[1].ID == part.Attempts[0].ID {
		t.Fatal("retry lost frozen configuration or attempt history")
	}
	snQAString(t, after.item.EntryID, "902")
	snQAString(t, part.EntryID, "902")
	snQAString(t, part.ConfirmedDurationNS, "36000000000")
	snQAString(t, part.ProviderDeltaNS, "0")
	snQAString(t, part.TotalResidualNS, "0")
	if !reflect.DeepEqual(authorized.requests[in.RequestID], after.requests[in.RequestID]) || !reflect.DeepEqual(after.requests[snQAID(342)].Value.SyncRun, &run) {
		t.Fatal("retry rewrote authorization or lost terminal run receipt")
	}
	srQAPrior(t, before, after)
	replayed, err := q.reopen().SyncNow(context.Background(), SyncRunInput{RequestID: snQAID(342)}, qaSyncNoProvider(t))
	if err != nil || !reflect.DeepEqual(run, replayed) {
		t.Fatal("retry run replay", err)
	}
	snQANonceOnly(t, after, snQARead(t, q, snQAID(300), in.RequestID, snQAID(342)))
}
