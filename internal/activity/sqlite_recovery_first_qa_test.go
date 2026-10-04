//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func TestSQLiteRecoveryFirstReviewAbsentFilteredAndColdReadOnly(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		f := interopLocation(t)
		path := filepath.Join(f.directory, "uncreated", f.authority)
		for n := 0; n < 2; n++ {
			calls := 0
			s := NewSQLite(Options{Path: path, LockTimeout: sqliteFlowTestLockTimeout(), Clock: ClockFunc(func() (ClockSample, error) { calls++; return ClockSample{}, errors.New("forbidden") })})
			got, err := s.Review(context.Background(), ReviewInput{})
			want := ReviewList{ContractVersion: 1, SnapshotRevision: "0", Uncertainties: []Uncertainty{}}
			if err != nil || !reflect.DeepEqual(got, want) || calls != 0 {
				t.Fatal("absent public Review", got, err, calls)
			}
			if _, err := os.Lstat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("Review created missing store directory", err)
			}
		}
	})
	t.Run("populated", func(t *testing.T) {
		q, p := rfQAUncertain(t, true)
		m, rows := flQASnapshot(t, q.f)
		q.forbidClock = true
		for _, in := range []ReviewInput{{}, {AccountID: "1"}, {ProjectID: "3"}, {AccountID: "1", ProjectID: "3"}, {AccountID: "2"}, {ProjectID: "4"}} {
			q.reopen()
			calls := q.calls
			got, err := q.s.Review(context.Background(), in)
			want := []Uncertainty{p.u}
			if in.AccountID == "2" || in.ProjectID == "4" {
				want = []Uncertainty{}
			}
			if err != nil || got.ContractVersion != 1 || got.SnapshotRevision != m.Revision || !reflect.DeepEqual(got.Uncertainties, want) || q.calls != calls {
				t.Fatal("cold filtered Review", in, got, err)
			}
			flQAUnchanged(t, q.f, m, rows)
		}
		rfQANoLegacyAuthority(t, q.f)
	})
}

func TestSQLiteRecoveryFirstPreviewExactPrefixAndRecordedCeiling(t *testing.T) {
	q, p := rfQAUncertain(t, true)
	m, rows := flQASnapshot(t, q.f)
	end := qaRecoveryTime(500)
	calls := q.calls
	got, err := q.s.Preview(context.Background(), RecoveryInput{UncertaintyID: p.u.ID, End: end})
	if err != nil || got.ContractVersion != 1 || got.SnapshotRevision != m.Revision || !reflect.DeepEqual(got.Uncertainty, p.u) || !got.SegmentStart.Equal(*qaRecoveryTime(0)) || !got.ConfirmedPrefix.Start.Equal(*qaRecoveryTime(0)) || !got.ConfirmedPrefix.End.Equal(*qaRecoveryTime(300)) || !got.ProposedEnd.Equal(*end) || got.DiscardedSuffix == nil || !got.DiscardedSuffix.Start.Equal(*end) || !got.DiscardedSuffix.End.Equal(*qaRecoveryTime(900)) || got.StillBlockedIDs == nil || len(got.StillBlockedIDs) != 0 || q.calls != calls+1 {
		t.Fatal("public Preview exact evidence/projection", got, err)
	}
	qaRecoveryRanges(t, got.AffectedUnionBefore, [][2]int64{{0, 300}})
	qaRecoveryRanges(t, got.AffectedUnionAfter, [][2]int64{{0, 500}})
	if !end.Equal(*qaRecoveryTime(500)) {
		t.Fatal("Preview mutated caller's proposed end")
	}
	flQAUnchanged(t, q.f, m, rows)
	for _, tc := range []struct {
		name, id, code string
		now, end       int64
	}{
		{"before-prefix", p.u.ID, "recovery_bounds", 1000, 299},
		{"after-recorded-cap", p.u.ID, "recovery_bounds", 1000, 901},
		{"rolled-back-current-ceiling", p.u.ID, "clock_conflict", 600, 500},
		{"unknown-reference", rfQAMissing, "uncertainty_not_found", 1000, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q.sample = ncQASample(tc.now)
			q.reopen()
			calls := q.calls
			r, err := q.s.Preview(context.Background(), RecoveryInput{UncertaintyID: tc.id, End: qaRecoveryTime(tc.end)})
			rfQACode(t, err, tc.code)
			if !reflect.DeepEqual(r, RecoveryPreview{}) {
				t.Fatal("failed preview leaked projection", r)
			}
			if tc.id == rfQAMissing && q.calls != calls {
				t.Fatal("unknown uncertainty sampled before lookup refusal")
			}
			flQAUnchanged(t, q.f, m, rows)
		})
	}
}

func TestSQLiteRecoveryFirstResolveConfirmationCASCapAndDurableReplay(t *testing.T) {
	q, p := rfQAUncertain(t, true)
	in := ResolveInput{UncertaintyID: p.u.ID, End: qaRecoveryTime(900), IfRevision: p.u.Revision, Reason: "reviewed lifecycle cap", RequestID: rfQARequest, Confirmed: true}
	m, rows := flQASnapshot(t, q.f)
	for _, tc := range []struct {
		name, code string
		edit       func(*ResolveInput)
		now        int64
	}{
		{"confirmation", "confirmation_required", func(v *ResolveInput) { v.Confirmed = false }, 1000},
		{"stale-revision", "revision_conflict", func(v *ResolveInput) { v.IfRevision = "99" }, 1000},
		{"recorded-cap-cannot-expand", "recovery_bounds", func(v *ResolveInput) { v.End = qaRecoveryTime(901) }, 1000},
		{"clock-cannot-erase-recorded-cap", "clock_conflict", func(v *ResolveInput) { v.End = qaRecoveryTime(500) }, 600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := in
			tc.edit(&bad)
			q.sample = ncQASample(tc.now)
			calls := q.calls
			r, err := q.s.Resolve(context.Background(), bad)
			rfQAMutationError(t, r, err, tc.code)
			if (tc.code == "confirmation_required" || tc.code == "revision_conflict") && q.calls != calls {
				t.Fatal("Resolve sampled before confirmation/revision refusal")
			}
			flQAUnchanged(t, q.f, m, rows)
		})
	}
	q.sample = ncQASample(1000)
	r, err := q.s.Resolve(context.Background(), in)
	if err != nil {
		t.Fatal("inclusive recorded cap must resolve", err)
	}
	if !reflect.DeepEqual(r.AffectedIDs, []string{p.u.ID}) {
		t.Fatal("resolution altered unrelated/current waiting actor", r)
	}
	q.reopen()
	q.resolved(p, in, r, 900)
	after := q.proof(p.u.ID, in.RequestID)
	if !reflect.DeepEqual(after.u.UpperBound, p.u.UpperBound) || after.decision.ObservedSample == nil || !reflect.DeepEqual(*after.decision.ObservedSample, q.sample) || after.decision.DiscardedSuffix == nil || !after.decision.DiscardedSuffix.Start.Equal(*qaRecoveryTime(900)) || !after.decision.DiscardedSuffix.End.Equal(*qaRecoveryTime(900)) {
		t.Fatal("recorded cap/explicit decision lost", after)
	}
	m, rows = flQASnapshot(t, q.f)
	q.forbidClock = true
	q.reopen()
	calls := q.calls
	again, err := q.s.Resolve(context.Background(), in)
	if err != nil || !reflect.DeepEqual(again, r) || q.calls != calls {
		t.Fatal("cold exact Resolve replay", again, err)
	}
	rfQANonceOnly(t, q.f, m, rows)
	m, rows = flQASnapshot(t, q.f)
	changed := in
	changed.Reason = "different assertion with same ID"
	conflict, err := q.s.Resolve(context.Background(), changed)
	rfQAMutationError(t, conflict, err, "request_conflict")
	flQAUnchanged(t, q.f, m, rows)
	list, err := q.s.Review(context.Background(), ReviewInput{})
	if err != nil || list.Uncertainties == nil || len(list.Uncertainties) != 0 || list.SnapshotRevision != m.Revision {
		t.Fatal("resolved history leaked into Review", list, err)
	}
	flQAUnchanged(t, q.f, m, rows)
	rfQANoLegacyAuthority(t, q.f)
}

func TestSQLiteRecoveryFirstDiscardWithoutClockKeepsConfirmedPrefix(t *testing.T) {
	q, p := rfQAUncertain(t, false)
	m, rows := flQASnapshot(t, q.f)
	q.clockErr, q.forbidClock = errors.New("synthetic unavailable"), true
	calls := q.calls
	preview, err := q.s.Preview(context.Background(), RecoveryInput{UncertaintyID: p.u.ID, DiscardTail: true})
	if err != nil || !reflect.DeepEqual(preview.Uncertainty, p.u) || !preview.ProposedEnd.Equal(p.u.LowerBound) || preview.DiscardedSuffix != nil || q.calls != calls {
		t.Fatal("clockless discard Preview", preview, err)
	}
	qaRecoveryRanges(t, preview.AffectedUnionBefore, [][2]int64{{0, 300}})
	qaRecoveryRanges(t, preview.AffectedUnionAfter, [][2]int64{{0, 300}})
	flQAUnchanged(t, q.f, m, rows)
	actor := q.actor("A")
	in := ResolveInput{UncertaintyID: p.u.ID, DiscardTail: true, IfRevision: p.u.Revision, Reason: "retain confirmed prefix only", RequestID: rfQARequest, Confirmed: true}
	r, err := q.s.Resolve(context.Background(), in)
	if err != nil || q.calls != calls {
		t.Fatal("clockless discard Resolve", r, err)
	}
	wantIDs := []string{p.u.ID, actor.ID}
	sort.Strings(wantIDs)
	if !reflect.DeepEqual(r.AffectedIDs, wantIDs) {
		t.Fatal("discard did not detach exact current stale actor", r)
	}
	q.reopen()
	q.resolved(p, in, r, 300)
	after := q.proof(p.u.ID, in.RequestID)
	if after.u.UpperBound != nil || after.evidence.BoundSample != nil || after.decision.ObservedSample != nil || after.decision.DiscardedSuffix != nil {
		t.Fatal("discard invented clock or cap", after)
	}
	a := q.actor("A")
	if a.Ref != actor.Ref || a.State != "interrupted" || a.SegmentID != nil || a.Revision != rfQANext(t, actor.Revision) {
		t.Fatal("discard exact-generation detach", a)
	}
	m, rows = flQASnapshot(t, q.f)
	again, err := q.s.Resolve(context.Background(), in)
	if err != nil || !reflect.DeepEqual(again, r) || q.calls != calls {
		t.Fatal("clockless exact discard replay", again, err)
	}
	rfQANonceOnly(t, q.f, m, rows)
	rfQANoLegacyAuthority(t, q.f)
}

func TestSQLiteRecoveryFirstInterruptExactGenerationWorkingAndWaiting(t *testing.T) {
	for _, waiting := range []bool{false, true} {
		name := "working"
		if waiting {
			name = "waiting"
		}
		t.Run(name, func(t *testing.T) {
			q := rfQALinked(t)
			parent := q.ingest(0, q.event("parent", "2", "1", "work"))
			childEvent := q.event("child", "1", "1", "work")
			childEvent.Parent = &parent.Actor
			q.ingest(5, childEvent)
			kind := "observe_work"
			if waiting {
				kind = "wait_children"
			}
			q.ingest(10, q.event("parent", "2", "2", kind))
			actor, child := q.actor("parent"), q.actor("child")
			if actor.Ref.Generation != "2" || child.Parent == nil || *child.Parent != actor.Ref {
				t.Fatal("SETUP actual generation/independent child", actor, child)
			}
			m, rows := flQASnapshot(t, q.f)
			in := InterruptInput{ActorID: actor.ID, Generation: actor.Ref.Generation, IfRevision: actor.Revision, RequestID: rfQARequest, Confirmed: true}
			for _, tc := range []struct {
				name, code string
				edit       func(*InterruptInput)
			}{
				{"confirmation", "confirmation_required", func(v *InterruptInput) { v.Confirmed = false }},
				{"old-generation", "revision_conflict", func(v *InterruptInput) { v.Generation = "1" }},
				{"old-revision", "revision_conflict", func(v *InterruptInput) { v.IfRevision = "99" }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					bad := in
					tc.edit(&bad)
					q.forbidClock = true
					r, err := q.s.Interrupt(context.Background(), bad)
					rfQAMutationError(t, r, err, tc.code)
					flQAUnchanged(t, q.f, m, rows)
				})
			}
			q.sample, q.forbidClock = ncQASample(20), waiting
			calls := q.calls
			r, err := q.s.Interrupt(context.Background(), in)
			if err != nil || !r.Changed || r.ContractVersion != 1 || r.RequestID != in.RequestID || r.SnapshotRevision != rfQANext(t, m.Revision) || r.EntityRevision == nil {
				t.Fatal("public Interrupt", r, err)
			}
			wantCalls := calls + 1
			if waiting {
				wantCalls = calls
			}
			if q.calls != wantCalls {
				t.Fatal("Interrupt clock branch", waiting, q.calls, wantCalls)
			}
			q.reopen()
			after := q.actor("parent")
			if after.ID != actor.ID || after.Ref != actor.Ref || after.State != "interrupted" || after.SegmentID != nil || after.Revision != *r.EntityRevision || !reflect.DeepEqual(q.actor("child"), child) {
				t.Fatal("Interrupt detached wrong actor/child", after)
			}
			c, tx := interopOpen(t, q.f, false, sqliteio.Read)
			ids := ncQAStrings(t, tx, "SELECT uncertainty_id FROM uncertainties ORDER BY uncertainty_id")
			stored, found, readErr := sqliteReadMutationRequestLocal(tx, q.computer, in.RequestID, r.SnapshotRevision)
			interopRollback(t, tx)
			interopClose(t, c)
			if readErr != nil || !found || stored.Value.Operation != "activity.interrupt" || stored.Value.MutationResult == nil || !reflect.DeepEqual(*stored.Value.MutationResult, r) {
				t.Fatal("Interrupt request not atomic", stored, readErr)
			}
			wantIDs := []string{actor.ID}
			if waiting {
				if len(ids) != 0 || after.Revision != rfQANext(t, actor.Revision) {
					t.Fatal("waiting Interrupt invented a work tail", ids, after)
				}
			} else {
				if len(ids) != 1 {
					t.Fatal("working Interrupt omitted uncertainty", ids)
				}
				p := q.proof(ids[0], in.RequestID)
				if p.u.Actor != actor.Ref || p.u.State != "unresolved" || p.u.Reason != "source_lost" || !p.u.LowerBound.Equal(*qaRecoveryTime(10)) || p.u.UpperBound == nil || !p.u.UpperBound.Equal(*qaRecoveryTime(20)) || p.u.Revision != "2" || p.segment.End != nil || p.segment.Finalized || !p.segment.Confirmed.Equal(*qaRecoveryTime(10)) || !reflect.DeepEqual(p.segment.ConfirmedSample, actor.LastEvidence) || p.hasDecision || after.Revision != rfQANext(t, rfQANext(t, actor.Revision)) {
					t.Fatal("working Interrupt guessed time or lost safe cap", p)
				}
				wantIDs = append(wantIDs, ids[0])
				sort.Strings(wantIDs)
			}
			if !reflect.DeepEqual(r.AffectedIDs, wantIDs) {
				t.Fatal("Interrupt affected closure", r, wantIDs)
			}
			m, rows = flQASnapshot(t, q.f)
			q.forbidClock = true
			q.reopen()
			calls = q.calls
			again, err := q.s.Interrupt(context.Background(), in)
			if err != nil || !reflect.DeepEqual(again, r) || q.calls != calls {
				t.Fatal("cold Interrupt replay", again, err)
			}
			rfQANonceOnly(t, q.f, m, rows)
			rfQANoLegacyAuthority(t, q.f)
		})
	}
}
