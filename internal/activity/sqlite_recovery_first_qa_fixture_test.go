//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const rfQARequest = "ed000000-0000-4000-8000-000000000001"
const rfQAMissing = "ed000000-0000-4000-8000-000000000099"

// Every setup mutation goes through public Link/Ingest on NewSQLite. Native
// read helpers below are cold assertions only; no legacy State or SQL seed.
type rfQAFixture struct {
	t           *testing.T
	f           interopFixture
	s           *Service
	computer    string
	binding     Binding
	sample      ClockSample
	clockErr    error
	calls       int
	forbidClock bool
}

func (q *rfQAFixture) reopen() {
	q.s = NewSQLite(Options{Path: filepath.Join(q.f.directory, q.f.authority), LockTimeout: sqliteFlowTestLockTimeout(), Clock: ClockFunc(func() (ClockSample, error) {
		q.calls++
		if q.forbidClock {
			q.t.Error("recovery sampled a clock on a clockless/read/replay branch")
			return q.sample, errors.New("synthetic forbidden clock")
		}
		return q.sample, q.clockErr
	})})
}

func rfQALinked(t *testing.T) *rfQAFixture {
	t.Helper()
	q := &rfQAFixture{t: t, f: interopLocation(t), sample: ncQASample(0)}
	q.reopen()
	in := qaLinkInput(t)
	r, err := q.s.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil || !r.Changed || r.SnapshotRevision != "1" || q.calls != 0 {
		t.Fatal("SETUP public fresh Link", r, err, q.calls)
	}
	q.binding = r.Binding
	m, rows := flQASnapshot(t, q.f)
	if !validUUID(m.ComputerID) || len(rows["bindings"]) != 1 || len(rows["requests"]) != 1 || m.SyncEnabled {
		t.Fatal("SETUP fresh identity/binding/receipt", m)
	}
	q.computer = m.ComputerID
	return q
}

func (q *rfQAFixture) event(agent, generation, sequence, kind string) Event {
	e := qaEvent(agent, generation, sequence, kind, "")
	e.Actor.ComputerID = q.computer
	if kind == "work" && sequence == "1" {
		e.BindingID, e.BindingRevision = q.binding.ID, q.binding.Revision
	}
	return e
}

func (q *rfQAFixture) ingest(at int64, e Event) EventResult {
	q.t.Helper()
	q.sample, q.clockErr = ncQASample(at), nil
	r, err := q.s.Ingest(context.Background(), e)
	if err != nil || r.Disposition != "applied" || r.Actor != (ActorRef{Key: e.Actor, Generation: e.Generation}) {
		q.t.Fatal("SETUP public normalized Ingest", e.Kind, r, err)
	}
	return r
}

func (q *rfQAFixture) actor(agent string) sqliteActorLocalRow {
	q.t.Helper()
	c, tx := interopOpen(q.t, q.f, false, sqliteio.Read)
	defer func() { interopRollback(q.t, tx); interopClose(q.t, c) }()
	a, found, err := sqliteReadActorLocal(tx, q.computer, q.event(agent, "1", "1", "work").Actor)
	if err != nil || !found {
		q.t.Fatal("cold actor", err, found)
	}
	return a
}

type rfQAProof struct {
	meta        sqliteStoreMeta
	u           Uncertainty
	segment     sqliteSegmentLocalRow
	evidence    uncertaintyEvidence
	decision    recoveryDecision
	hasDecision bool
	request     sqliteMutationRequestRow
	hasRequest  bool
}

func (q *rfQAFixture) proof(id, request string) rfQAProof {
	q.t.Helper()
	c, tx := interopOpen(q.t, q.f, false, sqliteio.Read)
	defer func() { interopRollback(q.t, tx); interopClose(q.t, c) }()
	var p rfQAProof
	var err error
	var found bool
	p.meta, err = sqliteReadMeta(tx, q.f.authority, q.f.database)
	if err != nil {
		q.t.Fatal(err)
	}
	p.u, found, err = sqliteReadUncertaintyScalar(tx, q.computer, id)
	if err != nil || !found {
		q.t.Fatal("cold uncertainty", err, found)
	}
	p.segment, found, err = sqliteReadSegmentLocal(tx, q.computer, p.u.SegmentID)
	if err != nil || !found {
		q.t.Fatal("cold segment", err, found)
	}
	p.evidence, found, err = sqliteReadUncertaintyEvidenceScalar(tx, id)
	if err != nil || !found {
		q.t.Fatal("cold uncertainty evidence", err, found)
	}
	p.decision, p.hasDecision, err = sqliteReadRecoveryDecisionScalar(tx, id)
	if err != nil {
		q.t.Fatal(err)
	}
	if request != "" {
		p.request, p.hasRequest, err = sqliteReadMutationRequestLocal(tx, q.computer, request, p.meta.Revision)
		if err != nil {
			q.t.Fatal(err)
		}
	}
	return p
}

func rfQAUncertain(t *testing.T, bounded bool) (*rfQAFixture, rfQAProof) {
	t.Helper()
	q := rfQALinked(t)
	q.ingest(0, q.event("A", "1", "1", "work"))
	q.ingest(300, q.event("A", "1", "2", "observe_work"))
	q.sample, q.clockErr = ncQASample(1800), errors.New("synthetic unavailable capture clock")
	_, err := q.s.Ingest(context.Background(), q.event("A", "1", "3", "finish"))
	rfQACode(t, err, "clock_unavailable")
	// The rejected finish did not consume sequence 3. A later real wait records
	// a trustworthy cap, not the unavailable detection's wall timestamp.
	if bounded {
		q.ingest(900, q.event("A", "1", "3", "wait_user"))
	}
	q.sample, q.clockErr = ncQASample(1000), nil
	q.reopen()
	c, tx := interopOpen(t, q.f, false, sqliteio.Read)
	ids := ncQAStrings(t, tx, "SELECT uncertainty_id FROM uncertainties ORDER BY uncertainty_id")
	interopRollback(t, tx)
	interopClose(t, c)
	if len(ids) != 1 {
		t.Fatal("SETUP public clock discontinuity did not retain one tail", ids)
	}
	p := q.proof(ids[0], "")
	if p.u.Actor != (ActorRef{Key: q.event("A", "1", "1", "work").Actor, Generation: "1"}) || p.u.State != "unresolved" || p.u.Reason != "restart_unknown" || !p.u.LowerBound.Equal(*qaRecoveryTime(300)) || !p.segment.Start.Equal(*qaRecoveryTime(0)) || !p.segment.Confirmed.Equal(*qaRecoveryTime(300)) || p.segment.End != nil || p.segment.Finalized || p.hasDecision || !p.evidence.Detection.WallUTC.Equal(*qaRecoveryTime(1800)) {
		t.Fatal("SETUP wrong recorded prefix/tail", p)
	}
	if bounded {
		if p.u.UpperBound == nil || !p.u.UpperBound.Equal(*qaRecoveryTime(900)) || p.evidence.BoundSample == nil || !p.evidence.BoundSample.WallUTC.Equal(*qaRecoveryTime(900)) || p.u.Revision != "2" {
			t.Fatal("SETUP lifecycle cap", p)
		}
	} else if p.u.UpperBound != nil || p.evidence.BoundSample != nil || p.u.Revision != "1" {
		t.Fatal("SETUP invented unbounded cap", p)
	}
	return q, p
}

func rfQACode(t *testing.T, err error, code string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("expected %s, got %T %v", code, err, err)
	}
}

func rfQAMutationError(t *testing.T, got MutationResult, err error, code string) {
	t.Helper()
	rfQACode(t, err, code)
	if !reflect.DeepEqual(got, MutationResult{}) {
		t.Fatal("failed recovery leaked result", got)
	}
}

func rfQANext(t *testing.T, before string) string {
	t.Helper()
	n, err := strconv.ParseUint(before, 10, 64)
	if err != nil || n == ^uint64(0) {
		t.Fatal("test revision", before, err)
	}
	return strconv.FormatUint(n+1, 10)
}

func (q *rfQAFixture) resolved(before rfQAProof, in ResolveInput, r MutationResult, end int64) {
	q.t.Helper()
	p := q.proof(before.u.ID, in.RequestID)
	if !r.Changed || r.ContractVersion != 1 || r.RequestID != in.RequestID || r.SnapshotRevision != rfQANext(q.t, before.meta.Revision) || r.EntityRevision == nil || *r.EntityRevision != rfQANext(q.t, before.u.Revision) || p.meta.Revision != r.SnapshotRevision || p.meta.DurabilityNonce == before.meta.DurabilityNonce {
		q.t.Fatal("resolution receipt/revision", r, p)
	}
	if p.u.State != "resolved" || p.u.Revision != *r.EntityRevision || p.u.ResolutionEnd == nil || !p.u.ResolutionEnd.Equal(*qaRecoveryTime(end)) || p.u.Discarded != in.DiscardTail || p.segment.End == nil || !p.segment.End.Equal(*qaRecoveryTime(end)) || !p.segment.Finalized {
		q.t.Fatal("resolution not materialized", p)
	}
	if !p.segment.Confirmed.Equal(before.segment.Confirmed) || !reflect.DeepEqual(p.segment.ConfirmedSample, before.segment.ConfirmedSample) || !reflect.DeepEqual(p.segment.StartSample, before.segment.StartSample) || !reflect.DeepEqual(p.evidence, before.evidence) {
		q.t.Fatal("resolution fabricated original evidence", p)
	}
	if !p.hasDecision || p.decision.RequestID != in.RequestID || p.decision.UncertaintyID != before.u.ID || p.decision.PreviousRevision != before.u.Revision || p.decision.Reason != in.Reason || p.decision.Discarded != in.DiscardTail || !p.decision.ResolutionEnd.Equal(*qaRecoveryTime(end)) || !p.hasRequest || p.request.Value.Operation != "activity.resolve" || p.request.Value.Fingerprint != mutationFingerprint("activity.resolve", in) || p.request.Value.Error != nil || p.request.Value.MutationResult == nil || !reflect.DeepEqual(*p.request.Value.MutationResult, r) {
		q.t.Fatal("missing atomic typed decision/request", p)
	}
	c, tx := interopOpen(q.t, q.f, false, sqliteio.Read)
	defer func() { interopRollback(q.t, tx); interopClose(q.t, c) }()
	ids := ncQAStrings(q.t, tx, "SELECT interval_id FROM intervals ORDER BY creation_ordinal")
	if len(ids) != 1 {
		q.t.Fatal("resolved prefix did not finalize one union", ids)
	}
	interval, found, err := sqliteReadIntervalLocal(tx, q.computer, ids[0])
	if err != nil || !found || interval.Attribution != before.u.Attribution || !interval.Start.Equal(*qaRecoveryTime(0)) || !interval.End.Equal(*qaRecoveryTime(end)) || interval.DurationNS != strconv.FormatInt(end*int64(time.Second), 10) {
		q.t.Fatal("recovered billing range", interval, err)
	}
	segments, err := sqliteIntervalSegmentIDsLocal(tx, q.computer, interval.ID)
	if err != nil || !reflect.DeepEqual(segments, []string{before.segment.ID}) {
		q.t.Fatal("resolved interval lost original support identity", segments, err)
	}
	box, found, err := sqliteReadOutboxLocal(tx, q.computer, interval.ID)
	if err != nil || !found || box.State != "queued" || box.PlanPresent || box.EntryID != nil {
		q.t.Fatal("atomic local queued intent", box, err)
	}
}

func rfQANonceOnly(t *testing.T, f interopFixture, before sqliteStoreMeta, rows map[string][][]string) {
	t.Helper()
	after, got := flQASnapshot(t, f)
	if before.DurabilityNonce == after.DurabilityNonce {
		t.Fatal("exact replay omitted fresh durability fence")
	}
	before.DurabilityNonce = after.DurabilityNonce
	if !reflect.DeepEqual(before, after) {
		t.Fatal("exact replay changed metadata/revision/charge")
	}
	for name, expected := range rows {
		if name != "store_meta" && !reflect.DeepEqual(expected, got[name]) {
			t.Fatal("exact replay changed retained rows", name)
		}
	}
}

func rfQANoLegacyAuthority(t *testing.T, f interopFixture) {
	t.Helper()
	if _, err := os.Lstat(filepath.Join(f.directory, f.authority)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("recovery created a legacy authority", err)
	}
}
