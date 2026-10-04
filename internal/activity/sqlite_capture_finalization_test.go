//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func TestSQLiteCaptureFinalizationRefreshBridgeSplitAndZero(t *testing.T) {
	a := cfQAAttr()
	st := cfQAState(t, cfQASpec{20, cfQATime(0), cfQATime(2), a}, cfQASpec{40, cfQATime(4), cfQATime(6), a}, cfQASpec{60, cfQATime(8), cfQATime(10), a}, cfQASpec{10, cfQATime(2), cfQATime(8), a}, cfQASpec{1, cfQATime(0), cfQATime(0), a})
	st.Segments[cfQAUUID(10)].End = nil
	st = hnQAValid(t, st)
	f, m, _ := cfQASeed(t, st)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	total := cfQARefreshAll(t, tx, st)
	original := cfQAComponents(st)
	before := asQASegment(st.Segments[cfQAUUID(10)])
	after := before
	end := after.Confirmed
	after.End = &end
	n, err := sqliteWriteSegmentLocal(tx, st.ComputerID, &before, after)
	if err != nil {
		t.Fatal(err)
	}
	total += n
	st.Segments[after.ID].End = &end
	r, err := sqliteRefreshSegmentFrontier(tx, st.ComputerID, &before, after)
	if err != nil {
		t.Fatal(err)
	}
	want := cfQAComponents(st)
	if len(want) != 1 || len(want[0].IDs) != 4 || want[0].IDs[0] != cfQAUUID(10) {
		t.Fatal("independent bridge oracle")
	}
	if r.Delta != cfQALiveCharge(t, st.ComputerID, want)-cfQALiveCharge(t, st.ComputerID, original) {
		t.Fatal("bridge exact signed delta")
	}
	total += r.Delta
	cfQAFrontiers(t, tx, st, want, true)
	for _, old := range original {
		if !cfQAContains(r.Selection.RemovedFrontierIDs, old.ID) {
			t.Fatal("old reverse owner omitted")
		}
	}
	for _, id := range want[0].IDs {
		if !cfQAContains(r.Selection.ChangedSegmentIDs, id) {
			t.Fatal("touched member seed omitted")
		}
	}
	cfQAValidate(t, tx, st, r.Selection)
	before = after
	after.End = nil
	n, err = sqliteWriteSegmentLocal(tx, st.ComputerID, &before, after)
	if err != nil {
		t.Fatal(err)
	}
	total += n
	st.Segments[after.ID].End = nil
	r, err = sqliteRefreshSegmentFrontier(tx, st.ComputerID, &before, after)
	if err != nil {
		t.Fatal(err)
	}
	total += r.Delta
	split := cfQAComponents(st)
	if !reflect.DeepEqual(split, original) || r.Delta != cfQALiveCharge(t, st.ComputerID, split)-cfQALiveCharge(t, st.ComputerID, want) {
		t.Fatal("split support/minimum/charge")
	}
	cfQAFrontiers(t, tx, st, split, true)
	cfQAValidate(t, tx, st, r.Selection)
	m, snap := cfQACommit(t, tx, m, total)
	interopClose(t, c)
	cfQAReopen(t, f, m, snap)
}

func TestSQLiteCaptureFinalizationStableMinimumAndExactBoundaryBytes(t *testing.T) {
	a := cfQAAttr()
	plus := time.FixedZone("east", 3600)
	minus := time.FixedZone("west", -7200)
	// The lower UUID starts at the same instant but has a LATER End. It must
	// supply fc1, while the shorter range supplies the retained Start bytes.
	st := cfQAState(t, cfQASpec{30, cfQATime(0).In(plus), cfQATime(2).In(plus), a}, cfQASpec{10, cfQATime(0).In(minus), cfQATime(10).In(minus), a}, cfQASpec{20, cfQATime(2), cfQATime(10), a}, cfQASpec{40, cfQATime(0), cfQATime(2), a})
	f, m, _ := cfQASeed(t, st)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	total := cfQARefreshAll(t, tx, st)
	want := cfQAComponents(st)
	if len(want) != 1 {
		t.Fatal("connected boundary oracle")
	}
	asQATimeWitness(t, st.Segments[cfQAUUID(30)].Start, want[0].Start)
	asQATimeWitness(t, st.Segments[cfQAUUID(10)].Confirmed, want[0].End)
	before := asQASegment(st.Segments[cfQAUUID(40)])
	after := before
	after.End = nil
	n, err := sqliteWriteSegmentLocal(tx, st.ComputerID, &before, after)
	if err != nil {
		t.Fatal(err)
	}
	total += n
	st.Segments[after.ID].End = nil
	r, err := sqliteRefreshSegmentFrontier(tx, st.ComputerID, &before, after)
	if err != nil {
		t.Fatal(err)
	}
	total += r.Delta
	remaining := cfQAComponents(st)
	if remaining[0].ID != want[0].ID || cfQAContains(r.Selection.RemovedFrontierIDs, want[0].ID) {
		t.Fatal("recreated stable minimum classified absent")
	}
	cfQAFrontiers(t, tx, st, remaining, true)
	cfQAValidate(t, tx, st, r.Selection)
	drain := cfQADrain(t, tx, st)
	total += cfQASealed(t, tx, st, remaining, drain)
	m, snap := cfQACommit(t, tx, m, total)
	interopClose(t, c)
	cfQAReopen(t, f, m, snap)
}

func TestSQLiteCaptureFinalizationUnionBilledOnceAndSeparateProjectTimers(t *testing.T) {
	a := cfQAAttr()
	b := a
	b.ProjectID = "5"
	st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(10), a}, cfQASpec{20, cfQATime(5), cfQATime(20), a}, cfQASpec{30, cfQATime(0), cfQATime(20), b})
	f, m, _ := cfQASeed(t, st)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	total := cfQARefreshAll(t, tx, st)
	want := cfQAComponents(st)
	drain := cfQADrain(t, tx, st)
	total += cfQASealed(t, tx, st, want, drain)
	// A later permitted drain sees updated SQL. No duplicate seal, queue, flags
	// or self-conflict; this is unit QA, not evidence of operation call sites.
	second := cfQADrain(t, tx, st)
	if second.Delta != 0 || second.OperationError != nil || len(second.Selection.NewSeals) != 0 {
		t.Fatal("later drain false self-conflict/duplicate")
	}
	cfQAValidate(t, tx, st, drain.Selection)
	m, snap := cfQACommit(t, tx, m, total)
	interopClose(t, c)
	cfQAReopen(t, f, m, snap)
}

func TestSQLiteCaptureFinalizationReservationInvalidatesOldNewDisconnectedGroups(t *testing.T) {
	a := cfQAAttr()
	b := a
	b.TaskID = "9"
	b.Timezone = "Etc/UTC"
	foreign := a
	foreign.ProjectID = "5"
	st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(1), a}, cfQASpec{20, cfQATime(2), cfQATime(3), b}, cfQASpec{30, cfQATime(5), cfQATime(6), a}, cfQASpec{40, cfQATime(10), cfQATime(12), b}, cfQASpec{50, cfQATime(20), cfQATime(22), a}, cfQASpec{60, cfQATime(0), cfQATime(30), foreign})
	f, m, _ := cfQASeed(t, st)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	total := cfQARefreshAll(t, tx, st)
	total += cfQAClearPending(t, tx, st)
	end := cfQATime(3)
	old := &sqliteReservationWindow{Start: cfQATime(1), End: &end}
	next := &sqliteReservationWindow{Start: cfQATime(11)}
	timer := sqliteTimerKey{ComputerID: st.ComputerID, AccountID: a.AccountID, ProjectID: a.ProjectID}
	r, err := sqliteInvalidateReservation(tx, timer, old, next)
	if err != nil {
		t.Fatal(err)
	}
	total += r.Delta
	want := []cfQAComponent{}
	var charge int64
	for _, row := range cfQAComponents(st) {
		if row.Attribution.ProjectID == a.ProjectID && (!row.End.Before(old.Start) && !row.Start.After(*old.End) || !row.End.Before(next.Start)) {
			want = append(want, row)
			charge += cfQAPendingCharge(t, st.ComputerID, row)
		}
	}
	if r.Delta != charge || len(r.Selection.RequiredPendingIDs) != len(want) {
		t.Fatal("old/new union exact pending charge/selections")
	}
	cursor := (*sqlitePendingLocalRow)(nil)
	got := []string{}
	for {
		p, found, err := sqliteNextPendingLocal(tx, st.ComputerID, cursor)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
		got = append(got, p.ID)
		copy := p
		cursor = &copy
	}
	ids := []string{}
	for _, row := range want {
		ids = append(ids, row.ID)
	}
	if !reflect.DeepEqual(got, ids) {
		t.Fatal("pending original group/start/end order", got, ids)
	}
	again, err := sqliteInvalidateReservation(tx, timer, old, next)
	if err != nil || again.Delta != 0 {
		t.Fatal("idempotent exact invalidation", err)
	}
	noop, err := sqliteInvalidateReservation(tx, timer, nil, nil)
	if err != nil || noop.Delta != 0 || noop.Selection.FrontierIDs == nil || noop.Selection.RequiredPendingIDs == nil {
		t.Fatal("allocated validated absence no-op", err)
	}
	// Validate every rebuilt component, and require pending only for the union.
	selected := cfQASelectedLive(st, cfQAComponents(st))
	selected.RequiredPendingIDs = ids
	selected.ClearedPendingIDs = []string{}
	for _, row := range cfQAComponents(st) {
		if !cfQAContains(ids, row.ID) {
			selected.ClearedPendingIDs = append(selected.ClearedPendingIDs, row.ID)
		}
	}
	cfQAValidate(t, tx, st, selected)
	m, snap := cfQACommit(t, tx, m, total)
	interopClose(t, c)
	cfQAReopen(t, f, m, snap)
}

func TestSQLiteCaptureFinalizationBlockedWorkingReleaseRequeuesAllGroups(t *testing.T) {
	a := cfQAAttr()
	b := a
	b.TaskID = "9"
	b.Timezone = "Etc/UTC"
	p := a
	p.ProjectID = "5"
	st := cfQAState(t, cfQASpec{10, cfQATime(2), cfQATime(3), a}, cfQASpec{20, cfQATime(10), cfQATime(12), b}, cfQASpec{30, cfQATime(2), cfQATime(3), p}, cfQASpec{99, cfQATime(0), cfQATime(0), a})
	seg := st.Segments[cfQAUUID(99)]
	seg.End = nil
	actor := &Actor{ID: cfQAUUID(900), Revision: "1", Ref: seg.Actor, Sequence: "1", State: "working", Health: "continuous", BindingID: seg.Binding.ID, BindingRevision: "1", Attribution: a, SegmentID: &seg.ID, LastEvidence: seg.ConfirmedSample, UncertaintyIDs: []string{}}
	st.Actors[actorKey(actor.Ref.Key)] = actor
	st = hnQAValid(t, st)
	seg = st.Segments[cfQAUUID(99)]
	actor = st.Actors[actorKey(seg.Actor.Key)]
	f, m, _ := cfQASeed(t, st)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	total := cfQARefreshAll(t, tx, st)
	first := cfQADrain(t, tx, st)
	if first.OperationError != nil || len(first.Selection.NewSeals) != 1 || len(first.Selection.ClearedPendingIDs) != 2 {
		t.Fatal("working cross-group reservation and separate timer")
	}
	total += first.Delta
	cfQAValidate(t, tx, st, first.Selection)
	for _, id := range []string{cfQAUUID(10), cfQAUUID(20)} {
		component := cfQAHash(st.ComputerID, st.Segments[id].Binding.Attribution, []string{id})
		if !cfQAContains(first.Selection.ClearedPendingIDs, component) {
			t.Fatal("blocked component not recorded cleared")
		}
		if _, found, err := sqliteReadPendingLocal(tx, st.ComputerID, component); err != nil || found {
			t.Fatal("blocked pending retained", err)
		}
	}
	// A blocked component may have no pending row. A real full-before CAS
	// followed by refresh must rebuild that owner and enqueue it successfully.
	unchanged := asQASegment(st.Segments[cfQAUUID(10)])
	if _, err := sqliteWriteSegmentLocal(tx, st.ComputerID, &unchanged, unchanged); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := sqliteRefreshSegmentFrontier(tx, st.ComputerID, &unchanged, unchanged)
	if err != nil {
		t.Fatal("absent blocked pending is valid", err)
	}
	total += rebuilt.Delta
	cfQAValidate(t, tx, st, rebuilt.Selection)
	st.Segments[cfQAUUID(30)].Finalized = true
	beforeActor := asQAActor(actor)
	afterActor := beforeActor
	afterActor.State = "finished"
	afterActor.SegmentID = nil
	n, err := sqliteWriteActorLocal(tx, st.ComputerID, &beforeActor, afterActor)
	if err != nil {
		t.Fatal(err)
	}
	total += n
	actor.State = "finished"
	actor.SegmentID = nil
	before := asQASegment(seg)
	after := before
	end := after.Confirmed
	after.End = &end
	n, err = sqliteWriteSegmentLocal(tx, st.ComputerID, &before, after)
	if err != nil {
		t.Fatal(err)
	}
	total += n
	seg.End = &end
	refresh, err := sqliteRefreshSegmentFrontier(tx, st.ComputerID, &before, after)
	if err != nil {
		t.Fatal("zero released owner refresh", err)
	}
	total += refresh.Delta
	old := &sqliteReservationWindow{Start: seg.Start}
	r, err := sqliteInvalidateReservation(tx, sqliteTimerKey{ComputerID: st.ComputerID, AccountID: a.AccountID, ProjectID: a.ProjectID}, old, nil)
	if err != nil || len(r.Selection.RequiredPendingIDs) != 2 {
		t.Fatal("removal missed disconnected groups", err)
	}
	total += r.Delta
	last := cfQADrain(t, tx, st)
	total += cfQASealed(t, tx, st, cfQAComponents(st), last)
	m, snap := cfQACommit(t, tx, m, total)
	interopClose(t, c)
	cfQAReopen(t, f, m, snap)
}

func TestSQLiteCaptureFinalizationBoundedUnresolvedReservationContact(t *testing.T) {
	a := cfQAAttr()
	b := a
	b.TaskID = "9"
	st := cfQAState(t, cfQASpec{10, cfQATime(2), cfQATime(3), b}, cfQASpec{20, cfQATime(5), cfQATime(6), b}, cfQASpec{99, cfQATime(0), cfQATime(1), a})
	seg := st.Segments[cfQAUUID(99)]
	id := cfQAUUID(990)
	seg.End = nil
	seg.UncertaintyID = &id
	bound := cfQATime(3)
	sample := cfQASample(bound)
	st.Uncertainties[id] = &Uncertainty{ID: id, Revision: "1", Actor: seg.Actor, SegmentID: seg.ID, Attribution: a, LowerBound: seg.Confirmed, UpperBound: &bound, Reason: "source_lost", State: "unresolved"}
	st.UncertaintyEvidence[id] = uncertaintyEvidence{Detection: sample, LastConfirmed: seg.ConfirmedSample, BoundSample: &sample}
	st = hnQAValid(t, st)
	f, m, snap := cfQASeed(t, st)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	_ = cfQARefreshAll(t, tx, st)
	r := cfQADrain(t, tx, st)
	if r.OperationError != nil || len(r.Selection.NewSeals) != 1 || len(r.Selection.ClearedPendingIDs) != 2 {
		t.Fatal("inclusive bounded reservation did not fence endpoint and release later group")
	}
	sealed, found, err := sqliteReadIntervalLocal(tx, st.ComputerID, r.Selection.NewSeals[0].IntervalID)
	if err != nil || !found || !sealed.Start.Equal(cfQATime(5)) {
		t.Fatal("wrong bounded-reservation candidate sealed", err)
	}
	cfQAValidate(t, tx, st, r.Selection)
	// A bounds shrink must select the OLD contact range even though it is no
	// longer hit by the new window. Invalidation itself does no domain writes.
	smaller := cfQATime(1)
	old := &sqliteReservationWindow{Start: cfQATime(1), End: &bound}
	next := &sqliteReservationWindow{Start: cfQATime(1), End: &smaller}
	inv, err := sqliteInvalidateReservation(tx, sqliteTimerKey{ComputerID: st.ComputerID, AccountID: a.AccountID, ProjectID: a.ProjectID}, old, next)
	if err != nil || len(inv.Selection.RequiredPendingIDs) != 2 {
		t.Fatal("shrink omitted old reservation contact", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	cfQAReopen(t, f, m, snap)
}

func TestSQLiteCaptureFinalizationStrictOverlapPrefixAndAdjacentLaterDrain(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		t.Run(map[bool]string{false: "adjacent", true: "overlap"}[overlap], func(t *testing.T) {
			a := cfQAAttr()
			b := a
			b.TaskID = "9"
			start := cfQATime(10)
			if overlap {
				start = cfQATime(9)
			}
			st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(10), a}, cfQASpec{20, start, cfQATime(20), b})
			f, m, snap := cfQASeed(t, st)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			total := cfQARefreshAll(t, tx, st)
			r := cfQADrain(t, tx, st)
			if !overlap {
				total += cfQASealed(t, tx, st, cfQAComponents(st), r)
				again := cfQADrain(t, tx, st)
				if again.OperationError != nil || again.Delta != 0 {
					t.Fatal("adjacent later drain/self-exclusion")
				}
				m, snap = cfQACommit(t, tx, m, total)
				interopClose(t, c)
				cfQAReopen(t, f, m, snap)
				return
			}
			if r.OperationError == nil {
				t.Fatal("same timer different attribution overlap admitted")
			}
			cfQACode(t, r.OperationError, "clock_conflict")
			if len(r.Selection.NewSeals) != 1 || interopCount(t, tx, "SELECT COUNT(*) FROM intervals") != 1 || interopCount(t, tx, "SELECT COUNT(*) FROM union_frontier") != 1 {
				t.Fatal("complete ordered valid prefix/failing component")
			}
			cfQAValidate(t, tx, st, r.Selection)
			_, charge := cfQAAudit(t, tx, m)
			if charge != m.LogicalBytes+total+r.Delta {
				t.Fatal("domain prefix exact charge")
			}
			// The unit has no clockChanged input. This verifies its uncommitted prefix;
			// actual Changed=false/true commit behavior belongs to operation QA later.
			interopRollback(t, tx)
			interopClose(t, c)
			cfQAReopen(t, f, m, snap)
		})
	}
}

func TestSQLiteCaptureFinalizationLateNativeBoundaryFailureRollsBackWholeUnit(t *testing.T) {
	cases := []struct{ name, event, table, condition string }{{"interval", "INSERT", "intervals", "NEW.start_sec="}, {"support", "INSERT", "interval_segments", "NEW.segment_id='" + cfQAUUID(20) + "'"}, {"seal", "INSERT", "interval_components", "NEW.component_id='"}, {"outbox", "INSERT", "outbox", "NEW.interval_id IN (SELECT interval_id FROM intervals WHERE start_sec="}, {"flag", "UPDATE", "segments", "NEW.segment_id='" + cfQAUUID(20) + "' AND NEW.finalized=1"}, {"member", "DELETE", "component_segments", "OLD.segment_id='" + cfQAUUID(20) + "'"}, {"pending", "DELETE", "pending_finalization", "OLD.component_id='"}, {"frontier", "DELETE", "union_frontier", "OLD.component_id='"}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := cfQAAttr()
			st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(2), a}, cfQASpec{20, cfQATime(4), cfQATime(6), a})
			f, m, snap := cfQASeed(t, st)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			_ = cfQARefreshAll(t, tx, st)
			component := cfQAComponents(st)[1].ID
			condition := tc.condition
			switch tc.name {
			case "interval":
				condition += strconvInt(cfQATime(4).Unix())
			case "seal", "pending", "frontier":
				condition += component + "'"
			case "outbox":
				condition += strconvInt(cfQATime(4).Unix()) + ")"
			}
			interopDone(t, tx, "CREATE TEMP TRIGGER cf_qa_failure BEFORE "+tc.event+" ON "+tc.table+" WHEN "+condition+" BEGIN SELECT RAISE(ABORT,'owned fixture refusal'); END")
			r, err := sqliteDrainFinalization(tx, st.ComputerID, st.Revision)
			cfQANative(t, err, 1811)
			if !reflect.DeepEqual(r, sqliteFinalizationResult{}) {
				t.Fatal("fatal native error returned usable partial result")
			}
			if interopCount(t, tx, "SELECT COUNT(*) FROM intervals") == 0 {
				t.Fatal("failure did not occur after earlier sibling mutation")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			cfQAReopen(t, f, m, snap)
		})
	}
}
func strconvInt(n int64) string { return strconv.FormatInt(n, 10) }

func TestSQLiteCaptureFinalizationDeferredFKRefusesActualCommit(t *testing.T) {
	a := cfQAAttr()
	st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(2), a})
	f, m, snap := cfQASeed(t, st)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	_ = cfQARefreshAll(t, tx, st)
	r := cfQADrain(t, tx, st)
	cfQASealed(t, tx, st, cfQAComponents(st), r)
	// Actual DDL's deferred FK admits this rollback-only missing target after a
	// complete sibling seal. COMMIT, rather than a pragma, must reject it.
	interopDone(t, tx, "INSERT INTO interval_components(interval_id,component_id) VALUES(?,?)", sqliteio.Text(cfQAUUID(9999)), sqliteio.Text(cfQAHash(st.ComputerID, a, []string{cfQAUUID(9998)})))
	outcome, err := tx.Commit()
	cfQANative(t, err, 787)
	var native *sqliteio.Error
	if outcome != sqliteio.Unknown || !errors.As(err, &native) || native.Phase != sqliteio.CommitPhase {
		t.Fatal("native deferred refusal outcome", outcome, err)
	}
	if cleanup := tx.Rollback(); cleanup != native.Cleanup {
		t.Fatal("terminal rollback changed native cleanup")
	}
	if cached, cachedErr := tx.Commit(); cached != outcome || cachedErr != err {
		t.Fatal("terminal COMMIT retried")
	}
	interopClose(t, c)
	cfQAReopen(t, f, m, snap)
}

func TestSQLiteCaptureFinalizationRefreshRejectsCorruptOldOwnership(t *testing.T) {
	for _, defect := range []string{"missing-owner", "missing-member", "wrong-boundary-bytes", "wrong-pending-copy", "selected-epoch"} {
		t.Run(defect, func(t *testing.T) {
			a := cfQAAttr()
			st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(2), a}, cfQASpec{20, cfQATime(2), cfQATime(4), a})
			f, m, snap := cfQASeed(t, st)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			_ = cfQARefreshAll(t, tx, st)
			owner := cfQAComponents(st)[0]
			switch defect {
			case "missing-owner":
				interopDone(t, tx, "DELETE FROM component_segments WHERE segment_id=?", sqliteio.Text(cfQAUUID(10)))
			case "missing-member":
				interopDone(t, tx, "DELETE FROM component_segments WHERE segment_id=?", sqliteio.Text(cfQAUUID(20)))
			case "wrong-boundary-bytes":
				b, _ := owner.Start.In(time.FixedZone("wrong", 3600)).MarshalJSON()
				interopDone(t, tx, "UPDATE union_frontier SET start_json=? WHERE component_id=?", sqliteio.Text(string(b)), sqliteio.Text(owner.ID))
			case "wrong-pending-copy":
				b, _ := owner.Start.In(time.FixedZone("wrong", 3600)).MarshalJSON()
				interopDone(t, tx, "UPDATE pending_finalization SET start_json=? WHERE component_id=?", sqliteio.Text(string(b)), sqliteio.Text(owner.ID))
			case "selected-epoch":
				interopDone(t, tx, "UPDATE epochs SET task_id='99' WHERE epoch_id=?", sqliteio.Text(st.Segments[cfQAUUID(20)].EpochID))
			}
			before := asQASegment(st.Segments[cfQAUUID(10)])
			after := before
			after.End = nil
			if _, err := sqliteWriteSegmentLocal(tx, st.ComputerID, &before, after); err != nil {
				t.Fatal("real full-before CAS", err)
			}
			r, err := sqliteRefreshSegmentFrontier(tx, st.ComputerID, &before, after)
			cfQACode(t, err, "state_corrupt")
			if !reflect.DeepEqual(r, sqliteFrontierMutationResult{}) {
				t.Fatal("corrupt selected old owner returned usable result")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			cfQAReopen(t, f, m, snap)
		})
	}
}

func TestSQLiteCaptureFinalizationSingleSupportCASRefreshPrecondition(t *testing.T) {
	a := cfQAAttr()
	st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(2), a}, cfQASpec{20, cfQATime(2), cfQATime(4), a})
	f, m, snap := cfQASeed(t, st)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	_ = cfQARefreshAll(t, tx, st)
	first := asQASegment(st.Segments[cfQAUUID(10)])
	second := asQASegment(st.Segments[cfQAUUID(20)])
	firstAfter := first
	secondAfter := second
	firstAfter.End = nil
	secondAfter.End = nil
	if _, err := sqliteWriteSegmentLocal(tx, st.ComputerID, &first, firstAfter); err != nil {
		t.Fatal(err)
	}
	if _, err := sqliteWriteSegmentLocal(tx, st.ComputerID, &second, secondAfter); err != nil {
		t.Fatal(err)
	}
	// Only this support's exact previous scalar may be substituted. The second
	// CAS violates the explicit sequencing protocol; do not invent an overlay.
	r, err := sqliteRefreshSegmentFrontier(tx, st.ComputerID, &first, firstAfter)
	cfQACode(t, err, "state_corrupt")
	if !reflect.DeepEqual(r, sqliteFrontierMutationResult{}) {
		t.Fatal("batched stale component accepted")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	cfQAReopen(t, f, m, snap)
}

func TestSQLiteCaptureFinalizationTouchedClosureDoesNotAuditUnrelatedHistory(t *testing.T) {
	for _, selected := range []bool{false, true} {
		t.Run(map[bool]string{false: "unrelated", true: "selected"}[selected], func(t *testing.T) {
			a := cfQAAttr()
			b := a
			b.ProjectID = "5"
			st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(2), a}, cfQASpec{20, cfQATime(0), cfQATime(2), b})
			f, m, snap := cfQASeed(t, st)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			_ = cfQARefreshAll(t, tx, st)
			id := cfQAUUID(20)
			if selected {
				id = cfQAUUID(10)
			}
			interopDone(t, tx, "UPDATE epochs SET task_id='99' WHERE epoch_id=?", sqliteio.Text(st.Segments[id].EpochID))
			selection := cfQASelectedLive(st, []cfQAComponent{cfQAComponents(st)[0]})
			selection.ChangedSegmentIDs = []string{cfQAUUID(10)}
			err := sqliteValidateSelectedFinalization(tx, st.ComputerID, st.Revision, selection)
			if selected {
				cfQACode(t, err, "state_corrupt")
			} else if err != nil {
				t.Fatal("selected validator scanned unrelated retained graph", err)
			}
			// Deliberate corruption is always rollback-only. This proves selection
			// boundaries; it makes no claim that the entire corrupt graph is valid.
			interopRollback(t, tx)
			interopClose(t, c)
			cfQAReopen(t, f, m, snap)
		})
	}
}

func TestSQLiteCaptureFinalizationSelectedSealChecksAtomicMembershipAndRoot(t *testing.T) {
	for _, defect := range []string{"second-seal", "pending-orphan", "root-not-initial", "plan-hidden", "reverse-live-owner"} {
		t.Run(defect, func(t *testing.T) {
			a := cfQAAttr()
			st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(2), a})
			f, m, snap := cfQASeed(t, st)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			_ = cfQARefreshAll(t, tx, st)
			r := cfQADrain(t, tx, st)
			cfQASealed(t, tx, st, cfQAComponents(st), r)
			seal := r.Selection.NewSeals[0]
			switch defect {
			case "second-seal":
				interopDone(t, tx, "INSERT INTO interval_components(interval_id,component_id) VALUES(?,?)", sqliteio.Text(seal.IntervalID), sqliteio.Text(cfQAHash(st.ComputerID, a, []string{cfQAUUID(888)})))
			case "pending-orphan":
				interopDone(t, tx, "INSERT INTO pending_finalization(component_id,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json) VALUES(?,?,?,?,?,?,?,?)", append([]sqliteio.Value{sqliteio.Text(seal.ComponentID), sqliteio.Text(cfQAGroup(st.ComputerID, a))}, append(asQATime(t, cfQATime(0)), asQATime(t, cfQATime(2))...)...)...)
			case "root-not-initial":
				interopDone(t, tx, "UPDATE outbox SET revision=? WHERE interval_id=?", interopCounter(t, "2"), sqliteio.Text(seal.IntervalID))
			case "plan-hidden":
				// Use the actual frozen fourteen-column plan; no relaxed schema.
				interopDone(t, tx, "INSERT INTO sync_plans(interval_id,company_source,config_account_id,config_user_id,config_revision,config_mode,config_duration_policy,config_policy_version,config_clock,config_declared,config_declared_at_sec,config_declared_at_nsec,config_declared_at_json,config_source) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", sqliteio.Text(seal.IntervalID), sqliteio.Text("user_declared_fallback"), sqliteio.Text("1"), sqliteio.Text("2"), interopCounter(t, "1"), sqliteio.Text("duration"), sqliteio.Text("exact"), sqliteio.Text("exact-v1"), sqliteio.Null(), sqliteio.Integer(1), sqliteio.Integer(cfQATime(0).Unix()), sqliteio.Integer(0), sqliteio.Text("\"2026-10-02T09:00:00Z\""), sqliteio.Text("user_declared"))
			case "reverse-live-owner":
				row := cfQAComponents(st)[0]
				_, err := sqliteWriteFrontierLocal(tx, st.ComputerID, nil, sqliteFrontierLocalRow{ID: row.ID, ComputerID: st.ComputerID, Attribution: a, Start: row.Start, End: row.End})
				if err != nil {
					t.Fatal(err)
				}
				interopDone(t, tx, "INSERT INTO component_segments(component_id,segment_id) VALUES(?,?)", sqliteio.Text(row.ID), sqliteio.Text(cfQAUUID(10)))
			}
			cfQACode(t, sqliteValidateSelectedFinalization(tx, st.ComputerID, st.Revision, r.Selection), "state_corrupt")
			interopRollback(t, tx)
			interopClose(t, c)
			cfQAReopen(t, f, m, snap)
		})
	}
}

func TestSQLiteCaptureFinalizationNativeDuplicateSealRootAndCrossFamilyCollisions(t *testing.T) {
	for _, family := range []string{"duplicate-seal", "duplicate-root", "part", "attempt"} {
		t.Run(family, func(t *testing.T) {
			a := cfQAAttr()
			st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(2), a})
			f, m, snap := cfQASeed(t, st)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			_ = cfQARefreshAll(t, tx, st)
			r := cfQADrain(t, tx, st)
			cfQASealed(t, tx, st, cfQAComponents(st), r)
			seal := r.Selection.NewSeals[0]
			// Additional interval/plan targets below are explicitly rollback-only native
			// uniqueness isolation; they are never called complete committed seals.
			in := sqliteIntervalLocalRow{ID: cfQAUUID(777), ComputerID: st.ComputerID, Attribution: a, Start: cfQATime(4), End: cfQATime(6), DurationNS: "2000000000", Ordinal: 1}
			if _, err := sqliteInsertIntervalLocal(tx, st.ComputerID, in); err != nil {
				t.Fatal(err)
			}
			switch family {
			case "duplicate-seal":
				n, err := sqliteInsertIntervalComponent(tx, st.ComputerID, in.ID, seal.ComponentID)
				cfQANative(t, err, 2067)
				if n != 0 {
					t.Fatal("duplicate seal nonzero delta")
				}
			case "duplicate-root":
				n, err := sqliteInsertQueuedOutbox(tx, st.ComputerID, in.ID, seal.OutboxID)
				cfQANative(t, err, 2067)
				if n != 0 {
					t.Fatal("duplicate root nonzero delta")
				}
			case "part", "attempt":
				insert := cfQARawPart
				if family == "attempt" {
					insert = cfQARawAttempt
				}
				cfQANative(t, insert(t, tx, seal.IntervalID, seal.OutboxID), 1811)
				partID := cfQAUUID(778)
				if err := insert(t, tx, seal.IntervalID, partID); err != nil {
					t.Fatal("different identity positive control", err)
				}
				n, err := sqliteInsertQueuedOutbox(tx, st.ComputerID, in.ID, partID)
				cfQANative(t, err, 1811)
				if n != 0 {
					t.Fatal("cross-family root collision nonzero delta")
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			cfQAReopen(t, f, m, snap)
		})
	}
}
