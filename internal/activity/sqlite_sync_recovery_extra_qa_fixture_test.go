//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/harvest"
)

// Actual public capture crosses UTC midnight: two exact 18-second daily parts.
// No outcome, request, plan, actor or interval is seeded through SQL or State.
func sxQACaptured(t *testing.T) *snQAFixture {
	t.Helper()
	base := time.Date(2026, 10, 2, 23, 59, 42, 0, time.UTC)
	q := &snQAFixture{t: t, f: interopLocation(t), sample: stQAAt(0)}
	q.sample.WallUTC = base
	q.s = q.reopen()
	ctx := context.Background()
	input := qaLinkInput(t)
	linked, err := q.s.Link(ctx, input, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil || !linked.Changed {
		t.Fatal("SETUP public multipart Link", err)
	}
	initial, err := q.s.Status(ctx)
	if err != nil || initial.ComputerID == nil {
		t.Fatal("SETUP multipart identity", err)
	}
	e := Event{ContractVersion: 1, Actor: ActorKey{ComputerID: *initial.ComputerID, Source: "manual-test", SessionID: "recovery-midnight", AgentID: "root"}, Generation: "1", BindingID: linked.Binding.ID, BindingRevision: linked.Binding.Revision, CWD: input.Path}
	for i, phase := range []struct {
		at             int64
		kind, sequence string
	}{{0, "work", "1"}, {18, "observe_work", "2"}, {36, "finish", "3"}} {
		q.sample = stQAAt(phase.at)
		q.sample.WallUTC = base.Add(time.Duration(phase.at) * time.Second)
		e.Kind, e.Sequence, e.EventID = phase.kind, phase.sequence, "recovery-midnight/"+phase.sequence
		if i != 0 {
			e.BindingID, e.BindingRevision, e.CWD = "", "", ""
		}
		if r, err := q.s.Ingest(ctx, e); err != nil || r.Disposition != "applied" {
			t.Fatal("SETUP public multipart capture", err)
		}
	}
	closed, err := q.s.Status(ctx)
	if err != nil || len(closed.ClosedIntervals) != 1 || len(closed.Uncertainties) != 0 || closed.Worker.QueuedCount != 1 {
		t.Fatal("SETUP closed multipart interval", err)
	}
	q.interval = closed.ClosedIntervals[0]
	if q.interval.DurationNS != "36000000000" || !q.interval.Start.Equal(base) || !q.interval.End.Equal(base.Add(36*time.Second)) {
		t.Fatal("SETUP exact midnight interval")
	}
	c, err := q.s.SyncConfigure(ctx, SyncConfigureInput{AccountID: "1", UserID: "2", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: snQAID(1), Confirmed: true}, qaSyncDeps(t, qaNewSyncProvider(t)))
	if err != nil || !c.Changed {
		t.Fatal("SETUP multipart consent", err)
	}
	q.configuration = c.Configuration
	if r, err := q.s.SyncResume(ctx, snQAID(2)); err != nil || !r.Changed {
		t.Fatal("SETUP multipart enable", err)
	}
	return q
}

// The first part succeeds. The second either gets a real definite rejection,
// or is never claimed because public Pause runs during the first mock POST.
func sxQAPartial(t *testing.T, rejected bool) (*snQAFixture, *qaSyncProvider, snQASnapshot) {
	t.Helper()
	q, p := sxQACaptured(t), qaNewSyncProvider(t)
	p.returnedHours = ""
	p.beforeCreate = func(harvest.Object) {
		status, err := q.reopen().Status(context.Background())
		if err != nil || status.Worker.SubmittingCount != 1 {
			t.Fatal("SETUP POST retained SQL owner", err)
		}
		claim := snQARead(t, q, snQAID(400))
		if claim.item.Plan == nil || len(claim.item.Plan.Parts) != 2 || claim.requests[snQAID(400)].Value.PendingSync == nil {
			t.Fatal("SETUP actual multipart claim")
		}
		if rejected && len(p.posts) == 2 {
			p.createErr = &harvest.Error{Code: "validation", Status: 422}
		}
		if !rejected {
			if len(p.posts) != 1 {
				t.Fatal("SETUP paused remainder was POSTed")
			}
			if r, err := q.reopen().SyncPause(context.Background(), snQAID(401)); err != nil || !r.Changed {
				t.Fatal("SETUP public Pause during POST", err)
			}
		}
	}
	run, err := q.reopen().SyncNow(context.Background(), SyncRunInput{RequestID: snQAID(400)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal("SETUP public multipart Now", err)
	}
	p.beforeCreate = nil
	before := snQARead(t, q, snQAID(400), snQAID(401))
	if run.State != "complete" || !reflect.DeepEqual(run.BlockedIDs, []string{before.item.ID}) || run.RemainingCount != 1 || before.item.State != "needs_attention" || before.item.Plan == nil || len(before.item.Plan.Parts) != 2 || before.item.RunRequestID != nil || before.item.RetryRequestID != nil || !reflect.DeepEqual(before.requests[snQAID(400)].Value.SyncRun, &run) {
		t.Fatal("SETUP partial terminal graph")
	}
	parts := before.item.Plan.Parts
	for i, part := range parts {
		if part.DurationNS != "18000000000" || part.PlannedHours != "0.005" || part.PlannedDurationNS != "18000000000" || part.SpentDate != []string{"2026-10-02", "2026-10-03"}[i] {
			t.Fatal("SETUP exact daily split")
		}
	}
	if parts[0].State != "synced" || len(parts[0].Attempts) != 1 || parts[0].Attempts[0].RequestID != snQAID(400) {
		t.Fatal("SETUP first part not acknowledged")
	}
	if rejected {
		if len(p.posts) != 2 || parts[1].State != "rejected" || len(parts[1].Attempts) != 1 || parts[1].Attempts[0].State != "rejected" {
			t.Fatal("SETUP second part not definitely rejected")
		}
	} else if len(p.posts) != 1 || parts[1].State != "queued" || len(parts[1].Attempts) != 0 || before.meta.SyncEnabled {
		t.Fatal("SETUP remainder was attempted or not paused")
	}
	p.calls, p.listQueries = nil, nil
	return q, p, before
}

func sxQAUnknown(t *testing.T, err error, id string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != "local_write_unknown" || !e.Uncertain || len(e.Details) != 1 || e.Details["request_id"] != id {
		t.Fatal("missing exact request-scoped unknown", err)
	}
}

// Override only current-user identity, preserving the existing mock's complete
// provider interface and call recording. There is no new production seam.
type sxQAOtherUser struct{ *qaSyncProvider }

func (p sxQAOtherUser) Get(ctx context.Context, path string) (harvest.Object, error) {
	row, err := p.qaSyncProvider.Get(ctx, path)
	if err == nil && path == "/users/me" {
		row["id"] = json.Number("3")
	}
	return row, err
}
