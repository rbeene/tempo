//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const rfdQARequest = "ed000000-0000-4000-8000-000000000021"

type rfdQAInterval struct {
	Row      sqliteIntervalLocalRow
	Supports []string
	Root     sqliteOutboxLocalRow
}

// These helpers are cold assertions only. All fixture writes use public
// NewSQLite Link/Ingest/Resolve; no historical SQL or legacy State is seeded.
func rfdQAIntervals(t *testing.T, q *rfQAFixture) []rfdQAInterval {
	t.Helper()
	c, tx := interopOpen(t, q.f, false, sqliteio.Read)
	defer func() { interopRollback(t, tx); interopClose(t, c) }()
	ids := ncQAStrings(t, tx, "SELECT interval_id FROM intervals ORDER BY creation_ordinal")
	result := make([]rfdQAInterval, 0, len(ids))
	for _, id := range ids {
		row, found, err := sqliteReadIntervalLocal(tx, q.computer, id)
		if err != nil || !found {
			t.Fatal("cold interval", id, found, err)
		}
		supports, err := sqliteIntervalSegmentIDsLocal(tx, q.computer, id)
		if err != nil {
			t.Fatal("cold interval supports", err)
		}
		root, found, err := sqliteReadOutboxLocal(tx, q.computer, id)
		if err != nil || !found {
			t.Fatal("cold interval root", found, err)
		}
		result = append(result, rfdQAInterval{Row: row, Supports: supports, Root: root})
	}
	return result
}
func rfdQASegment(t *testing.T, q *rfQAFixture, id string) sqliteSegmentLocalRow {
	t.Helper()
	c, tx := interopOpen(t, q.f, false, sqliteio.Read)
	defer func() { interopRollback(t, tx); interopClose(t, c) }()
	row, found, err := sqliteReadSegmentLocal(tx, q.computer, id)
	if err != nil || !found {
		t.Fatal("cold segment", found, err)
	}
	return row
}
func rfdQAGenerations(t *testing.T, q *rfQAFixture, refs ...ActorRef) {
	t.Helper()
	c, tx := interopOpen(t, q.f, false, sqliteio.Read)
	defer func() { interopRollback(t, tx); interopClose(t, c) }()
	for _, ref := range refs {
		got, found, err := sqliteReadActorGeneration(tx, ref)
		if err != nil || !found || got != ref {
			t.Fatal("retained generation identity", got, ref, found, err)
		}
	}
}

func TestSQLiteRecoveryDeferredSupersededGenerationKeepsCurrentWorkAndGap(t *testing.T) {
	q, first := rfQAUncertain(t, false)
	q.ingest(900, q.event("A", "2", "1", "work"))
	q.ingest(1000, q.event("A", "2", "2", "observe_work"))
	before := q.proof(first.u.ID, "")
	current := q.actor("A")
	if before.u.Actor.Generation != "1" || before.u.State != "unresolved" || before.u.UpperBound == nil || !before.u.UpperBound.Equal(*qaRecoveryTime(900)) || before.evidence.BoundSample == nil || !before.evidence.BoundSample.WallUTC.Equal(*qaRecoveryTime(900)) || current.Ref.Generation != "2" || current.State != "working" || current.Health != "continuous" || current.Sequence != "2" || current.SegmentID == nil || *current.SegmentID == before.segment.ID {
		t.Fatal("SETUP genuine superseded uncertainty/current generation", before, current)
	}
	live := rfdQASegment(t, q, *current.SegmentID)
	if !live.Start.Equal(*qaRecoveryTime(900)) || !live.Confirmed.Equal(*qaRecoveryTime(1000)) || live.End != nil || live.Finalized {
		t.Fatal("SETUP current work coordinates", live)
	}
	if before.segment.Actor != first.segment.Actor || !before.segment.Confirmed.Equal(first.segment.Confirmed) || !reflect.DeepEqual(before.segment.StartSample, first.segment.StartSample) || !reflect.DeepEqual(before.segment.ConfirmedSample, first.segment.ConfirmedSample) {
		t.Fatal("SETUP supersession rewrote original evidence")
	}
	rfdQAGenerations(t, q, before.u.Actor, current.Ref)
	q.sample = ncQASample(1100)
	m, rows := flQASnapshot(t, q.f)
	preview, err := q.s.Preview(context.Background(), RecoveryInput{UncertaintyID: before.u.ID, End: qaRecoveryTime(600)})
	if err != nil || !reflect.DeepEqual(preview.Uncertainty, before.u) || preview.SnapshotRevision != m.Revision || !preview.ProposedEnd.Equal(*qaRecoveryTime(600)) || preview.DiscardedSuffix == nil || !preview.DiscardedSuffix.Start.Equal(*qaRecoveryTime(600)) || !preview.DiscardedSuffix.End.Equal(*qaRecoveryTime(900)) {
		t.Fatal("superseded Preview", preview, err)
	}
	qaRecoveryRanges(t, preview.AffectedUnionBefore, [][2]int64{{0, 300}, {900, 1000}})
	qaRecoveryRanges(t, preview.AffectedUnionAfter, [][2]int64{{0, 600}, {900, 1000}})
	flQAUnchanged(t, q.f, m, rows)
	// Public supersession supplies a cap at 900. This rejection proves that
	// boundary, not an isolated missing-cap/all-generation selector premise.
	bad := ResolveInput{UncertaintyID: before.u.ID, End: qaRecoveryTime(901), IfRevision: before.u.Revision, Reason: "past successor", RequestID: rfdQARequest, Confirmed: true}
	rejected, err := q.s.Resolve(context.Background(), bad)
	rfQAMutationError(t, rejected, err, "recovery_bounds")
	flQAUnchanged(t, q.f, m, rows)
	in := bad
	in.End = qaRecoveryTime(600)
	in.Reason = "old generation only"
	result, err := q.s.Resolve(context.Background(), in)
	if err != nil || !reflect.DeepEqual(result.AffectedIDs, []string{before.u.ID}) {
		t.Fatal("resolve old generation", result, err)
	}
	q.reopen()
	q.resolved(before, in, result, 600)
	if !reflect.DeepEqual(q.actor("A"), current) || !reflect.DeepEqual(rfdQASegment(t, q, live.ID), live) {
		t.Fatal("old recovery altered current generation or segment")
	}
	rfdQAGenerations(t, q, before.u.Actor, current.Ref)
	oldIntervals := rfdQAIntervals(t, q)
	if len(oldIntervals) != 1 {
		t.Fatal("old prefix did not seal separately", oldIntervals)
	}
	q.ingest(1200, q.event("A", "2", "3", "finish"))
	q.reopen()
	intervals := rfdQAIntervals(t, q)
	if len(intervals) != 2 || !reflect.DeepEqual(intervals[0], oldIntervals[0]) || !intervals[1].Row.Start.Equal(*qaRecoveryTime(900)) || !intervals[1].Row.End.Equal(*qaRecoveryTime(1200)) || intervals[1].Row.DurationNS != "300000000000" || !reflect.DeepEqual(intervals[1].Supports, []string{live.ID}) || intervals[1].Root.State != "queued" || intervals[1].Root.PlanPresent {
		t.Fatal("current finish rewrote old output or billed gap", intervals)
	}
	if !intervals[0].Row.End.Before(intervals[1].Row.Start) {
		t.Fatal("recovery lost the unbilled 600–900 gap")
	}
	afterCurrent := q.actor("A")
	if afterCurrent.Ref != current.Ref || afterCurrent.State != "finished" || afterCurrent.Sequence != "3" {
		t.Fatal("successor did not complete independently", afterCurrent)
	}
	m, rows = flQASnapshot(t, q.f)
	q.forbidClock = true
	q.reopen()
	calls := q.calls
	replay, err := q.s.Resolve(context.Background(), in)
	if err != nil || !reflect.DeepEqual(replay, result) || q.calls != calls {
		t.Fatal("cold old-generation replay", replay, err)
	}
	rfQANonceOnly(t, q.f, m, rows)
	if !reflect.DeepEqual(q.actor("A"), afterCurrent) {
		t.Fatal("historical replay changed successor")
	}
	rfQANoLegacyAuthority(t, q.f)
}

func rfdQAEndpointFixture(t *testing.T, start int64) (*rfQAFixture, rfQAProof, rfdQAInterval) {
	t.Helper()
	q := rfQALinked(t)
	q.ingest(0, q.event("closed", "1", "1", "work"))
	q.ingest(10, q.event("closed", "1", "2", "finish"))
	closed := rfdQAIntervals(t, q)
	if len(closed) != 1 || !closed[0].Row.Start.Equal(*qaRecoveryTime(0)) || !closed[0].Row.End.Equal(*qaRecoveryTime(10)) || closed[0].Root.State != "queued" || closed[0].Root.PlanPresent {
		t.Fatal("SETUP immutable interval", closed)
	}
	q.ingest(start, q.event("tail", "1", "1", "work"))
	q.ingest(20, q.event("tail", "1", "2", "observe_work"))
	q.sample, q.clockErr = ncQASample(30), errors.New("synthetic evidence loss")
	_, err := q.s.Ingest(context.Background(), q.event("tail", "1", "3", "finish"))
	rfQACode(t, err, "clock_unavailable")
	q.sample, q.clockErr = ncQASample(40), nil
	q.reopen()
	c, tx := interopOpen(t, q.f, false, sqliteio.Read)
	ids := ncQAStrings(t, tx, "SELECT uncertainty_id FROM uncertainties ORDER BY uncertainty_id")
	interopRollback(t, tx)
	interopClose(t, c)
	if len(ids) != 1 {
		t.Fatal("SETUP one public retained tail", ids)
	}
	p := q.proof(ids[0], "")
	if p.u.Actor.Key.AgentID != "tail" || p.u.State != "unresolved" || p.u.UpperBound != nil || !p.segment.Start.Equal(*qaRecoveryTime(start)) || !p.segment.Confirmed.Equal(*qaRecoveryTime(20)) || p.segment.End != nil || p.segment.Finalized || !reflect.DeepEqual(rfdQAIntervals(t, q), closed) {
		t.Fatal("SETUP tail and unchanged immutable interval", p)
	}
	return q, p, closed[0]
}

func TestSQLiteRecoveryDeferredImmutableEndpointContactAndSafetyPrefix(t *testing.T) {
	t.Run("touch-without-overlap-is-pure-refusal", func(t *testing.T) {
		q, p, prior := rfdQAEndpointFixture(t, 10)
		if !p.segment.Start.Equal(prior.Row.End) || p.segment.Start.Before(prior.Row.End) {
			t.Fatal("SETUP exact contact, not strict overlap")
		}
		m, rows := flQASnapshot(t, q.f)
		for _, discard := range []bool{false, true} {
			input := RecoveryInput{UncertaintyID: p.u.ID, DiscardTail: discard}
			if !discard {
				input.End = qaRecoveryTime(25)
			}
			calls := q.calls
			preview, err := q.s.Preview(context.Background(), input)
			rfQACode(t, err, "clock_conflict")
			if !reflect.DeepEqual(preview, RecoveryPreview{}) {
				t.Fatal("rejected endpoint preview leaked result", preview)
			}
			if discard && q.calls != calls {
				t.Fatal("clockless discard endpoint preview sampled")
			}
			flQAUnchanged(t, q.f, m, rows)
			in := ResolveInput{UncertaintyID: p.u.ID, End: input.End, DiscardTail: discard, IfRevision: p.u.Revision, RequestID: rfdQARequest, Confirmed: true}
			calls = q.calls
			result, err := q.s.Resolve(context.Background(), in)
			rfQAMutationError(t, result, err, "clock_conflict")
			if discard && q.calls != calls {
				t.Fatal("clockless discard endpoint resolution sampled")
			}
			flQAUnchanged(t, q.f, m, rows)
			q.reopen()
			if !reflect.DeepEqual(q.proof(p.u.ID, ""), p) || !reflect.DeepEqual(rfdQAIntervals(t, q), []rfdQAInterval{prior}) {
				t.Fatal("pure endpoint refusal changed target/history/output")
			}
		}
	})
	t.Run("strict-gap-positive-control", func(t *testing.T) {
		q, p, prior := rfdQAEndpointFixture(t, 11)
		m, rows := flQASnapshot(t, q.f)
		preview, err := q.s.Preview(context.Background(), RecoveryInput{UncertaintyID: p.u.ID, End: qaRecoveryTime(25)})
		if err != nil || !preview.ProposedEnd.Equal(*qaRecoveryTime(25)) {
			t.Fatal("strict gap preview", preview, err)
		}
		qaRecoveryRanges(t, preview.AffectedUnionBefore, [][2]int64{{0, 10}, {11, 20}})
		qaRecoveryRanges(t, preview.AffectedUnionAfter, [][2]int64{{0, 10}, {11, 25}})
		flQAUnchanged(t, q.f, m, rows)
		in := ResolveInput{UncertaintyID: p.u.ID, End: qaRecoveryTime(25), IfRevision: p.u.Revision, Reason: "separate interval", RequestID: rfdQARequest, Confirmed: true}
		result, err := q.s.Resolve(context.Background(), in)
		if err != nil || !result.Changed || result.SnapshotRevision != rfQANext(t, m.Revision) {
			t.Fatal("strict gap Resolve", result, err)
		}
		q.reopen()
		after := q.proof(p.u.ID, in.RequestID)
		if after.u.State != "resolved" || after.u.ResolutionEnd == nil || !after.u.ResolutionEnd.Equal(*in.End) || after.segment.End == nil || !after.segment.End.Equal(*in.End) || !after.segment.Finalized || !reflect.DeepEqual(after.segment.StartSample, p.segment.StartSample) || !reflect.DeepEqual(after.segment.ConfirmedSample, p.segment.ConfirmedSample) || !reflect.DeepEqual(after.evidence.Detection, p.evidence.Detection) || !reflect.DeepEqual(after.evidence.LastConfirmed, p.evidence.LastConfirmed) || !after.hasDecision || !after.hasRequest || after.request.Value.MutationResult == nil || !reflect.DeepEqual(*after.request.Value.MutationResult, result) {
			t.Fatal("strict gap lost typed recovery proof", after)
		}
		intervals := rfdQAIntervals(t, q)
		if len(intervals) != 2 || !reflect.DeepEqual(intervals[0], prior) || !intervals[1].Row.Start.Equal(*qaRecoveryTime(11)) || !intervals[1].Row.End.Equal(*qaRecoveryTime(25)) || intervals[1].Row.DurationNS != "14000000000" || !reflect.DeepEqual(intervals[1].Supports, []string{p.segment.ID}) || intervals[1].Root.State != "queued" || intervals[1].Root.PlanPresent || !intervals[0].Row.End.Before(intervals[1].Row.Start) {
			t.Fatal("strict gap changed immutable output/billed gap", intervals)
		}
	})
	t.Run("rejected-proposal-retains-only-new-peer-quarantine", func(t *testing.T) {
		q, p, prior := rfdQAEndpointFixture(t, 10)
		q.ingest(40, q.event("peer", "1", "1", "work"))
		peer, tail, closedActor := q.actor("peer"), q.actor("tail"), q.actor("closed")
		if peer.SegmentID == nil || peer.State != "working" || peer.Health != "continuous" {
			t.Fatal("SETUP live peer", peer)
		}
		peerSegment := rfdQASegment(t, q, *peer.SegmentID)
		p = q.proof(p.u.ID, "")
		m, rows := flQASnapshot(t, q.f)
		// Available but inconsistent wall/elapsed evidence discovers a peer clock
		// discontinuity. The target's endpoint refusal remains clock_conflict.
		q.sample = ncQASample(50)
		q.sample.WallUTC = *qaRecoveryTime(100)
		preview, err := q.s.Preview(context.Background(), RecoveryInput{UncertaintyID: p.u.ID, End: qaRecoveryTime(25)})
		rfQACode(t, err, "clock_conflict")
		if !reflect.DeepEqual(preview, RecoveryPreview{}) {
			t.Fatal("unsafe preview projection", preview)
		}
		flQAUnchanged(t, q.f, m, rows)
		in := ResolveInput{UncertaintyID: p.u.ID, End: qaRecoveryTime(25), IfRevision: p.u.Revision, Reason: "endpoint remains forbidden", RequestID: rfdQARequest, Confirmed: true}
		result, err := q.s.Resolve(context.Background(), in)
		rfQAMutationError(t, result, err, "clock_conflict")
		q.reopen()
		afterMeta, afterRows := flQASnapshot(t, q.f)
		if afterMeta.Revision != rfQANext(t, m.Revision) || afterMeta.DurabilityNonce == m.DurabilityNonce {
			t.Fatal("safety error did not durably record one mutation", afterMeta, m)
		}
		if afterMeta.LogicalBytes <= m.LogicalBytes {
			t.Fatal("safety rows were not charged", afterMeta.LogicalBytes, m.LogicalBytes)
		}
		fixedMeta := afterMeta
		fixedMeta.Revision = m.Revision
		fixedMeta.DurabilityNonce = m.DurabilityNonce
		fixedMeta.LogicalBytes = m.LogicalBytes
		if !reflect.DeepEqual(fixedMeta, m) {
			t.Fatal("safety error changed unrelated metadata")
		}
		for name, priorRows := range rows {
			switch name {
			case "store_meta", "actors", "segments", "uncertainties", "uncertainty_evidence", "actor_uncertainties", "requests":
				continue
			}
			if !reflect.DeepEqual(afterRows[name], priorRows) {
				t.Fatal("safety prefix changed unrelated history/derived output", name)
			}
		}
		untouched := q.proof(p.u.ID, in.RequestID)
		if !reflect.DeepEqual(untouched.u, p.u) || !reflect.DeepEqual(untouched.segment, p.segment) || !reflect.DeepEqual(untouched.evidence, p.evidence) || untouched.hasDecision || !untouched.hasRequest || untouched.request.Value.Operation != "activity.resolve" || untouched.request.Value.Fingerprint != mutationFingerprint("activity.resolve", in) || untouched.request.Value.MutationResult != nil || !reflect.DeepEqual(untouched.request.Value.Error, failure("clock_conflict")) {
			t.Fatal("rejected proposal leaked resolution instead of exclusive safe error", untouched)
		}
		if !reflect.DeepEqual(q.actor("tail"), tail) || !reflect.DeepEqual(q.actor("closed"), closedActor) || !reflect.DeepEqual(rfdQAIntervals(t, q), []rfdQAInterval{prior}) {
			t.Fatal("safety error changed target/closed actor or immutable output")
		}
		current := q.actor("peer")
		if current.ID != peer.ID || current.Ref != peer.Ref || current.State != "working" || current.Health != "stale" || current.Revision != rfQANext(t, peer.Revision) || !reflect.DeepEqual(current.SegmentID, peer.SegmentID) || !reflect.DeepEqual(current.LastEvidence, peer.LastEvidence) {
			t.Fatal("safety prefix detached/rewrote peer", current)
		}
		c, tx := interopOpen(t, q.f, false, sqliteio.Read)
		ids := ncQAStrings(t, tx, "SELECT uncertainty_id FROM uncertainties ORDER BY uncertainty_id")
		interopRollback(t, tx)
		interopClose(t, c)
		if len(ids) != 2 {
			t.Fatal("safety prefix omitted/duplicated uncertainty", ids)
		}
		peerID := ""
		for _, id := range ids {
			if id != p.u.ID {
				peerID = id
			}
		}
		if peerID == "" {
			t.Fatal("missing distinct peer uncertainty")
		}
		quarantined := q.proof(peerID, "")
		if quarantined.u.Actor != peer.Ref || quarantined.u.State != "unresolved" || quarantined.u.Reason != "clock_changed" || quarantined.u.UpperBound != nil || !quarantined.u.LowerBound.Equal(peerSegment.Confirmed) || quarantined.segment.End != nil || quarantined.segment.Finalized || !reflect.DeepEqual(quarantined.segment.ConfirmedSample, peerSegment.ConfirmedSample) || !reflect.DeepEqual(quarantined.evidence.LastConfirmed, peerSegment.ConfirmedSample) || !reflect.DeepEqual(quarantined.evidence.Detection, q.sample) || quarantined.evidence.BoundSample != nil || quarantined.hasDecision {
			t.Fatal("safety prefix invented cap/time or lost clock evidence", quarantined)
		}
		q.forbidClock = true
		q.reopen()
		calls := q.calls
		replay, err := q.s.Resolve(context.Background(), in)
		rfQAMutationError(t, replay, err, "clock_conflict")
		if q.calls != calls {
			t.Fatal("safe error replay sampled again")
		}
		rfQANonceOnly(t, q.f, afterMeta, afterRows)
		rfQANoLegacyAuthority(t, q.f)
	})
}
