//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"reflect"
	"testing"
)

// Public Link and declared synthetic policy are the only setup. These receipts
// do not assert delivery by a real Codex process. The native smoke acceptance
// predicate remains unchanged: applied/duplicate, committed, operator_declared.
func TestSQLiteHostOrdinaryInterruptKeepsAcceptedReceiptAndUncertainTail(t *testing.T) {
	q := obQANew(t, 1, true)
	q.host(0, "interrupt-session", "SessionStart", "", "")
	root := q.host(0, "interrupt-session", "UserPromptSubmit", "root", "")
	child := q.host(5, "interrupt-session", "SubagentStart", "child-turn", "child")
	if root.Actor == nil || child.Actor == nil || root.Disposition != "applied" || child.Disposition != "applied" {
		t.Fatal("public root/child setup did not admit exact actors")
	}
	q.at(20)
	before := q.snapshot()
	childBefore := obQAActor(t, before, *child.Actor)
	in := HostEvent{Source: "codex", SessionID: "interrupt-session", Kind: "Interrupt", TurnID: "root", CWD: q.cwd[0]}
	r, err := q.service().IngestHost(q.ctx, in)
	if err != nil || r.ID == "" || r.Durability != "committed" || r.Origin != "unverified" || r.ProfileBasis != "operator_declared" || !reflect.DeepEqual(r.Actor, root.Actor) {
		t.Fatal("ordinary Interrupt failed to commit its exact root receipt")
	}
	if r.Disposition != "applied" || r.Ordering != "supported" || r.DiagnosticCode != "source_lost" {
		t.Errorf("ordinary Interrupt lost accepted terminal receipt: disposition=%s ordering=%s diagnostic=%s", r.Disposition, r.Ordering, r.DiagnosticCode)
	}
	after := q.snapshot()
	a := obQAActor(t, after, *root.Actor)
	u := obQAUncertainty(t, after, *root.Actor)
	if a.State != "interrupted" || a.Health != "stale" || a.SegmentID != nil || a.Sequence != "2" || !a.LastEvidence.WallUTC.Equal(q.when(20)) ||
		u.Reason != "source_lost" || u.State != "unresolved" || !u.LowerBound.Equal(q.when(0)) || u.UpperBound == nil || !u.UpperBound.Equal(q.when(20)) || u.ResolutionEnd != nil || u.Discarded ||
		len(after.Uncertainties) != 1 || len(after.ClosedIntervals) != 0 || after.Worker.QueuedCount != 0 || len(after.CaptureReviews) != 0 ||
		!reflect.DeepEqual(obQAActor(t, after, *child.Actor), childBefore) {
		t.Fatal("ordinary Interrupt billed unknown work, lost its bounded uncertainty, or changed the child")
	}
	// Cold public receipt read must expose the same first-call disposition.
	list, err := q.service().HostReceipts(q.ctx, HostReceiptFilter{Source: "codex", SessionID: in.SessionID})
	if err != nil {
		t.Fatal("cold public receipt read")
	}
	matched := 0
	for _, stored := range list.Receipts {
		if stored.ID == r.ID {
			matched++
			if !reflect.DeepEqual(stored, r) {
				t.Fatal("stored Interrupt differs from committed return")
			}
		}
	}
	if matched != 1 {
		t.Fatal("ordinary Interrupt lacks one immutable receipt")
	}
	q.quiet(func() {
		replay, e := q.service().IngestHost(q.ctx, in)
		want := r
		want.Disposition = "duplicate"
		if e != nil || !reflect.DeepEqual(replay, want) {
			t.Fatal("cold exact Interrupt replay changed retained receipt or used live clock")
		}
	})
	if !reflect.DeepEqual(q.snapshot(), after) {
		t.Fatal("Interrupt replay changed public actor, uncertainty or queue state")
	}
	end := q.host(20, in.SessionID, "SessionEnd", "", "")
	final := q.snapshot()
	if end.Disposition != "stale" || !reflect.DeepEqual(end.Actor, root.Actor) || !reflect.DeepEqual(final.Actors, after.Actors) || !reflect.DeepEqual(final.Uncertainties, after.Uncertainties) || len(final.ClosedIntervals) != 0 || final.Worker.QueuedCount != 0 {
		t.Fatal("SessionEnd repeated terminal effects or changed the independent child")
	}
}

func TestSQLiteHostInterruptPreservesReviewAndFailureControls(t *testing.T) {
	for _, mode := range []string{"waiting", "session-end", "clock-unavailable"} {
		t.Run(mode, func(t *testing.T) {
			q := obQANew(t, 1, true)
			q.host(0, "interrupt-control", "SessionStart", "", "")
			root := q.host(0, "interrupt-control", "UserPromptSubmit", "root", "")
			if root.Actor == nil {
				t.Fatal("control setup lacks root")
			}
			if mode == "waiting" {
				q.at(10)
				pre := HostEvent{Source: "codex", SessionID: "interrupt-control", Kind: "PreToolUse", TurnID: "root", ToolID: "pending-wait", ToolName: "wait_agent", CWD: q.cwd[0]}
				r, err := q.service().IngestHost(q.ctx, pre)
				if err != nil || r.Disposition != "applied" || r.Durability != "committed" || obQAActor(t, q.snapshot(), *root.Actor).State != "wait_children" {
					t.Fatal("control did not enter a real pending wait")
				}
			}
			q.at(20)
			in := HostEvent{Source: "codex", SessionID: "interrupt-control", Kind: "Interrupt", TurnID: "root", CWD: q.cwd[0]}
			wantDiagnostic := "source_loss_while_waiting"
			if mode == "session-end" {
				in.Kind, in.TurnID, wantDiagnostic = "SessionEnd", "", "source_lost"
			} else if mode == "clock-unavailable" {
				q.clockErr = errors.New("synthetic unavailable clock")
				wantDiagnostic = "clock_unavailable"
			}
			r, err := q.service().IngestHost(q.ctx, in)
			if mode == "clock-unavailable" {
				obQAError(t, err, "clock_unavailable")
			} else if err != nil {
				t.Fatal("control unexpectedly returned an operation error")
			}
			if r.ID == "" || r.Disposition != "review_required" || r.Ordering != "review_required" || r.DiagnosticCode != wantDiagnostic || r.Durability != "committed" || !reflect.DeepEqual(r.Actor, root.Actor) {
				t.Fatal("terminal receipt hid a waiting/loss/failure review")
			}
			q.clockErr = nil
			after := q.snapshot()
			a := obQAActor(t, after, *root.Actor)
			if a.Health != "stale" {
				t.Fatal("control restored false continuity")
			}
			if mode == "waiting" {
				if a.State != "interrupted" || len(after.Uncertainties) != 0 || len(after.CaptureReviews) != 1 || after.CaptureReviews[0].ID != r.ID || len(after.ClosedIntervals) != 1 || !after.ClosedIntervals[0].Start.Equal(q.when(0)) || !after.ClosedIntervals[0].End.Equal(q.when(10)) || after.Worker.QueuedCount != 1 {
					t.Fatal("waiting Interrupt lost prefix/review or invented idle uncertainty")
				}
			} else {
				u := obQAUncertainty(t, after, *root.Actor)
				if len(after.Uncertainties) != 1 || u.Reason != "source_lost" || u.State != "unresolved" || !u.LowerBound.Equal(q.when(0)) || u.ResolutionEnd != nil || len(after.ClosedIntervals) != 0 || after.Worker.QueuedCount != 0 {
					t.Fatal("loss/error control invented completed work")
				}
				if mode == "session-end" && (a.State != "interrupted" || u.UpperBound == nil || !u.UpperBound.Equal(q.when(20))) || mode == "clock-unavailable" && (a.State != "working" || u.UpperBound != nil) {
					t.Fatal("loss/error control changed its terminal or uncertainty boundary")
				}
			}
			q.quiet(func() {
				replay, e := q.service().IngestHost(q.ctx, in)
				if mode == "clock-unavailable" {
					obQAError(t, e, "clock_unavailable")
				} else if e != nil {
					t.Fatal("review replay returned a new error")
				}
				want := r
				want.Disposition = "duplicate"
				if !reflect.DeepEqual(replay, want) {
					t.Fatal("review/error replay changed its immutable receipt")
				}
			})
			if !reflect.DeepEqual(q.snapshot(), after) {
				t.Fatal("control replay changed public capture state")
			}
		})
	}
}
