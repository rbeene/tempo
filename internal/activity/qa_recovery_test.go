package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"
)

const qaRecoveryRequest = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

func qaRecoveryTime(seconds int64) *time.Time {
	v := qaEpochStart.Add(time.Duration(seconds) * time.Second)
	return &v
}
func qaRecoveryUncertain(t *testing.T) (*qaHarness, Uncertainty) {
	t.Helper()
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(300, qaEvent("A", "1", "2", "observe_work", ""))
	h.at(1800)
	h.clockErr = errors.New("synthetic unavailable")
	_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "3", "finish", ""))
	qaCode(t, err, "clock_unavailable")
	h.at(2400)
	s := h.snapshot()
	if len(s.Uncertainties) != 1 {
		t.Fatalf("fixture uncertainty=%+v", s.Uncertainties)
	}
	return h, s.Uncertainties[0]
}
func qaRecoveryRead(t *testing.T, h *qaHarness) []byte {
	t.Helper()
	b, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func qaRecoveryRanges(t *testing.T, got []TimeRange, want [][2]int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ranges=%+v want=%v", got, want)
	}
	for i, r := range got {
		if !r.Start.Equal(*qaRecoveryTime(want[i][0])) || !r.End.Equal(*qaRecoveryTime(want[i][1])) {
			t.Fatalf("range %+v want %v", r, want[i])
		}
	}
}

func TestQARecoveryEmptyReviewAndObservationNeverInitialize(t *testing.T) {
	h := qaNew(t)
	r, err := h.service.Review(context.Background(), ReviewInput{})
	if err != nil {
		t.Fatal(err)
	}
	if r.ContractVersion != 1 || r.SnapshotRevision != "0" || r.Uncertainties == nil || len(r.Uncertainties) != 0 {
		t.Fatalf("empty review %+v", r)
	}
	_, err = h.service.ObserveClock(context.Background(), ClockObservation{RequestID: qaRecoveryRequest})
	qaCode(t, err, "input_required")
	if _, err := os.Stat(h.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty reads initialized state: %v", err)
	}
}
func TestQARecoveryPreviewPreservesBytesAndConfirmedEvidence(t *testing.T) {
	h, u := qaRecoveryUncertain(t)
	before := qaRecoveryRead(t, h)
	end := qaRecoveryTime(1200)
	r, err := h.service.Preview(context.Background(), RecoveryInput{UncertaintyID: u.ID, End: end})
	if err != nil {
		t.Fatal(err)
	}
	if r.Uncertainty.ID != u.ID || !r.SegmentStart.Equal(*qaRecoveryTime(0)) || !r.ConfirmedPrefix.Start.Equal(*qaRecoveryTime(0)) || !r.ConfirmedPrefix.End.Equal(*qaRecoveryTime(300)) || !r.ProposedEnd.Equal(*end) {
		t.Fatalf("preview changed evidence %+v", r)
	}
	qaRecoveryRanges(t, r.AffectedUnionBefore, [][2]int64{{0, 300}})
	qaRecoveryRanges(t, r.AffectedUnionAfter, [][2]int64{{0, 1200}})
	if string(before) != string(qaRecoveryRead(t, h)) {
		t.Fatal("preview mutated durable state")
	}
}
func TestQARecoveryPreservesGapAndFinalizedIdentity(t *testing.T) {
	h, u := qaRecoveryUncertain(t)
	h.ingest(7200, qaEvent("B", "1", "1", "work", qaBindingB))
	h.at(7800)
	before, _, err := h.service.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	originalSample := before.Segments[u.SegmentID].ConfirmedSample
	r, err := h.service.Resolve(context.Background(), ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(3600), IfRevision: u.Revision, Reason: "verified work ended at ten", RequestID: qaRecoveryRequest, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Changed {
		t.Fatal("recovery did not change state")
	}
	snap := h.snapshot()
	qaIntervals(t, snap, [][2]int64{{0, 3600}})
	intervalID := snap.ClosedIntervals[0].ID
	after, _, err := h.service.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	seg := after.Segments[u.SegmentID]
	if !seg.Confirmed.Equal(*qaRecoveryTime(300)) || !reflect.DeepEqual(seg.ConfirmedSample, originalSample) || seg.End == nil || !seg.End.Equal(*qaRecoveryTime(3600)) {
		t.Fatalf("recovery fabricated confirmed continuity: %+v", seg)
	}
	outboxID := after.Outbox[intervalID].ID
	h.ingest(10800, qaEvent("B", "1", "2", "finish", ""))
	h.restart()
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 3600}, {7200, 10800}})
	after, _, err = h.service.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after.Outbox[intervalID].ID != outboxID {
		t.Fatal("peer close rewrote finalized outbox identity")
	}
	raw := qaRecoveryRead(t, h)
	var persisted map[string]json.RawMessage
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	var decisions map[string]map[string]any
	if err := json.Unmarshal(persisted["recovery_decisions"], &decisions); err != nil {
		t.Fatal(err)
	}
	if decisions[u.ID]["request_id"] != qaRecoveryRequest || decisions[u.ID]["previous_revision"] != u.Revision {
		t.Fatalf("missing durable recovery audit: %+v", decisions)
	}
}
func TestQARecoveryDiscardWithUnavailableClockKeepsOnlyConfirmedPrefix(t *testing.T) {
	h, u := qaRecoveryUncertain(t)
	h.clockErr = errors.New("unavailable")
	before := qaRecoveryRead(t, h)
	p, err := h.service.Preview(context.Background(), RecoveryInput{UncertaintyID: u.ID, DiscardTail: true})
	if err != nil {
		t.Fatal(err)
	}
	if !p.ProposedEnd.Equal(u.LowerBound) || p.DiscardedSuffix != nil {
		t.Fatalf("clockless discard invented bound: %+v", p)
	}
	if string(before) != string(qaRecoveryRead(t, h)) {
		t.Fatal("discard preview wrote state")
	}
	_, err = h.service.Resolve(context.Background(), ResolveInput{UncertaintyID: u.ID, DiscardTail: true, IfRevision: u.Revision, Reason: "discard unverified tail", RequestID: qaRecoveryRequest, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	h.clockErr = nil
	h.restart()
	snap := h.snapshot()
	qaIntervals(t, snap, [][2]int64{{0, 300}})
	if len(snap.Uncertainties) != 1 || snap.Uncertainties[0].State != "resolved" || !snap.Uncertainties[0].Discarded || snap.Uncertainties[0].UpperBound != nil {
		t.Fatalf("discard fabricated evidence: %+v", snap.Uncertainties)
	}
}
func TestQARecoveryBoundsAndRevisionFailuresPreserveState(t *testing.T) {
	for _, tc := range []struct {
		name           string
		end            int64
		revision, code string
	}{{"below-confirmed", 299, "", "recovery_bounds"}, {"future", 2401, "", "recovery_bounds"}, {"stale-revision", 1200, "99", "revision_conflict"}} {
		t.Run(tc.name, func(t *testing.T) {
			h, u := qaRecoveryUncertain(t)
			rev := u.Revision
			if tc.revision != "" {
				rev = tc.revision
			}
			before := qaRecoveryRead(t, h)
			_, err := h.service.Resolve(context.Background(), ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(tc.end), IfRevision: rev, RequestID: qaRecoveryRequest, Confirmed: true})
			qaCode(t, err, tc.code)
			if string(before) != string(qaRecoveryRead(t, h)) {
				t.Fatal("pure rejection mutated state")
			}
		})
	}
}

func TestQARecoveryMultipleUncertaintiesResolveInEitherOrderWithoutStoppingResume(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse-%t", reverse), func(t *testing.T) {
			h := qaNew(t)
			h.seed()
			h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
			h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
			h.at(20)
			h.clockErr = errors.New("clock lost")
			_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "3", "finish", ""))
			qaCode(t, err, "clock_unavailable")
			h.ingest(30, qaEvent("A", "1", "3", "work", ""))
			h.ingest(40, qaEvent("A", "1", "4", "observe_work", ""))
			h.at(50)
			h.clockErr = errors.New("clock lost again")
			_, err = h.service.Ingest(context.Background(), qaEvent("A", "1", "5", "finish", ""))
			qaCode(t, err, "clock_unavailable")
			h.ingest(60, qaEvent("A", "1", "5", "work", ""))
			s := h.snapshot()
			if len(s.Uncertainties) != 2 {
				t.Fatalf("fixture uncertainty %+v", s.Uncertainties)
			}
			sort.Slice(s.Uncertainties, func(i, j int) bool { return s.Uncertainties[i].LowerBound.Before(s.Uncertainties[j].LowerBound) })
			order := []int{0, 1}
			if reverse {
				order = []int{1, 0}
			}
			for _, idx := range order {
				u := s.Uncertainties[idx]
				end := int64(15)
				if idx == 1 {
					end = 45
				}
				_, err = h.service.Resolve(context.Background(), ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(end), IfRevision: u.Revision, RequestID: fmt.Sprintf("aaaaaaaa-aaaa-4aaa-8aaa-%012d", idx), Confirmed: true})
				if err != nil {
					t.Fatal(err)
				}
				current := h.snapshot()
				if current.Actors[0].State != "working" || current.Actors[0].Sequence != "5" {
					t.Fatalf("old uncertainty detached resumed work %+v", current.Actors)
				}
			}
			h.ingest(70, qaEvent("A", "1", "6", "finish", ""))
			h.restart()
			qaIntervals(t, h.snapshot(), [][2]int64{{0, 15}, {30, 45}, {60, 70}})
		})
	}
}
func TestQARecoveryResolutionReplayIsDurableAndClockIndependent(t *testing.T) {
	h, u := qaRecoveryUncertain(t)
	in := ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(1200), IfRevision: u.Revision, Reason: "verified end", RequestID: qaRecoveryRequest, Confirmed: true}
	first, err := h.service.Resolve(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	h.restart()
	before := qaRecoveryRead(t, h)
	h.service.clock = ClockFunc(func() (ClockSample, error) { t.Fatal("resolve replay sampled clock"); return ClockSample{}, nil })
	again, err := h.service.Resolve(context.Background(), in)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatalf("resolve replay changed %+v %v", again, err)
	}
	if string(before) != string(qaRecoveryRead(t, h)) {
		t.Fatal("resolve replay mutated state")
	}
	in.Reason = "changed reason"
	_, err = h.service.Resolve(context.Background(), in)
	qaCode(t, err, "request_conflict")
	in.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	_, err = h.service.Resolve(context.Background(), in)
	qaCode(t, err, "invalid_transition")
}
func TestQARecoveryCorrectedClockCanRecoverAfterErroneousDetection(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(300, qaEvent("A", "1", "2", "observe_work", ""))
	h.at(600)
	h.sample.WallUTC = *qaRecoveryTime(3600)
	if _, err := h.service.ObserveClock(context.Background(), ClockObservation{RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}); err != nil {
		t.Fatal(err)
	}
	h.at(600)
	u := h.snapshot().Uncertainties[0]
	_, err := h.service.Resolve(context.Background(), ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(360), IfRevision: u.Revision, RequestID: qaRecoveryRequest, Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 360}})
	st, _, err := h.service.store.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !st.UncertaintyEvidence[u.ID].Detection.WallUTC.Equal(*qaRecoveryTime(3600)) {
		t.Fatal("corrected recovery erased original erroneous detection")
	}
}

func TestQARecoveryTrustworthyLaterBoundRejectsRolledBackCeiling(t *testing.T) {
	h, u := qaRecoveryUncertain(t)
	h.ingest(900, qaEvent("A", "1", "3", "wait_user", ""))
	h.at(600)
	current := h.snapshot().Uncertainties[0]
	_, err := h.service.Resolve(context.Background(), ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(500), IfRevision: current.Revision, RequestID: qaRecoveryRequest, Confirmed: true})
	qaCode(t, err, "clock_conflict")
	if len(h.snapshot().ClosedIntervals) != 0 {
		t.Fatal("rollback recovery finalized inconsistent time")
	}
}
func TestQARecoveryExistingUpperBoundIsInclusiveAndCannotExpand(t *testing.T) {
	for _, end := range []int64{300, 900, 901} {
		t.Run(fmt.Sprintf("end-%d", end), func(t *testing.T) {
			h, _ := qaRecoveryUncertain(t)
			h.ingest(900, qaEvent("A", "1", "3", "wait_user", ""))
			h.at(1000)
			u := h.snapshot().Uncertainties[0]
			_, err := h.service.Resolve(context.Background(), ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(end), IfRevision: u.Revision, RequestID: qaRecoveryRequest, Confirmed: true})
			if end > 900 {
				qaCode(t, err, "recovery_bounds")
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			qaIntervals(t, h.snapshot(), [][2]int64{{0, end}})
		})
	}
}
func TestQARecoveryRejectsFabricatedContinuityWithoutMatchingAudit(t *testing.T) {
	for _, name := range []string{"missing-decision", "decision-end-mismatch", "original-confirmed-rewritten", "decision-ceiling-before-end"} {
		t.Run(name, func(t *testing.T) {
			h, u := qaRecoveryUncertain(t)
			_, err := h.service.Resolve(context.Background(), ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(1200), IfRevision: u.Revision, RequestID: qaRecoveryRequest, Confirmed: true})
			if err != nil {
				t.Fatal(err)
			}
			raw := qaRecoveryRead(t, h)
			var data map[string]any
			if err := json.Unmarshal(raw, &data); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "missing-decision":
				delete(data["recovery_decisions"].(map[string]any), u.ID)
			case "decision-end-mismatch":
				data["recovery_decisions"].(map[string]any)[u.ID].(map[string]any)["resolution_end"] = qaRecoveryTime(1500).Format(time.RFC3339Nano)
			case "original-confirmed-rewritten":
				data["segments"].(map[string]any)[u.SegmentID].(map[string]any)["confirmed"] = qaRecoveryTime(1200).Format(time.RFC3339Nano)
			case "decision-ceiling-before-end":
				sample := data["recovery_decisions"].(map[string]any)[u.ID].(map[string]any)["observed_sample"].(map[string]any)
				sample["wall_utc"] = qaRecoveryTime(0).Format(time.RFC3339Nano)
				sample["elapsed_ns"] = "0"
				sample["awake_ns"] = "0"
			}
			corrupt, err := json.Marshal(data)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(h.path, corrupt, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = h.service.Review(context.Background(), ReviewInput{})
			qaCode(t, err, "state_corrupt")
			if string(corrupt) != string(qaRecoveryRead(t, h)) {
				t.Fatal("fabricated continuity evidence was overwritten")
			}
		})
	}
}

func TestQARecoveryReviewFiltersPersistedHistoryWithoutWrites(t *testing.T) {
	h := qaNew(t)
	b := h.bindings[qaBindingB]
	b.Attribution.ProjectID = "5"
	h.bindings[qaBindingB] = b
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(5, qaEvent("B", "1", "1", "work", qaBindingB))
	h.at(10)
	h.clockErr = errors.New("unavailable")
	_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "2", "finish", ""))
	qaCode(t, err, "clock_unavailable")
	h.at(20)
	before := qaRecoveryRead(t, h)
	all, err := h.service.Review(context.Background(), ReviewInput{})
	if err != nil || len(all.Uncertainties) != 2 {
		t.Fatalf("review missing persistent history %+v %v", all, err)
	}
	one, err := h.service.Review(context.Background(), ReviewInput{AccountID: "1", ProjectID: "3"})
	if err != nil || len(one.Uncertainties) != 1 || one.Uncertainties[0].Actor.Key.AgentID != "A" {
		t.Fatalf("review crossed project scope %+v %v", one, err)
	}
	empty, err := h.service.Review(context.Background(), ReviewInput{AccountID: "99"})
	if err != nil || empty.Uncertainties == nil || len(empty.Uncertainties) != 0 {
		t.Fatalf("empty filter contract %+v %v", empty, err)
	}
	if string(before) != string(qaRecoveryRead(t, h)) {
		t.Fatal("review filters mutated state")
	}
}

func TestQARecoveryCannotTouchImmutableFinalizedInterval(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(10, qaEvent("A", "1", "2", "finish", ""))
	prior := h.snapshot().ClosedIntervals[0]
	h.ingest(10, qaEvent("B", "1", "1", "work", qaBindingB))
	h.ingest(20, qaEvent("B", "1", "2", "observe_work", ""))
	h.at(30)
	h.clockErr = errors.New("unavailable")
	_, err := h.service.Ingest(context.Background(), qaEvent("B", "1", "3", "finish", ""))
	qaCode(t, err, "clock_unavailable")
	h.at(40)
	u := h.snapshot().Uncertainties[0]
	before := qaRecoveryRead(t, h)
	_, err = h.service.Preview(context.Background(), RecoveryInput{UncertaintyID: u.ID, End: qaRecoveryTime(25)})
	qaCode(t, err, "clock_conflict")
	_, err = h.service.Resolve(context.Background(), ResolveInput{UncertaintyID: u.ID, End: qaRecoveryTime(25), IfRevision: u.Revision, RequestID: qaRecoveryRequest, Confirmed: true})
	qaCode(t, err, "clock_conflict")
	if string(before) != string(qaRecoveryRead(t, h)) {
		t.Fatal("rejected touching recovery changed immutable output")
	}
	s := h.snapshot()
	if len(s.ClosedIntervals) != 1 || !reflect.DeepEqual(s.ClosedIntervals[0], prior) {
		t.Fatal("recovery rewrote finalized interval")
	}
}

func TestQARecoveryLaterAttributionEpochResolvesBeforeEarlierBoundedUncertainty(t *testing.T) {
	h := qaNew(t)
	// Exercise disjoint recovery semantics with the race fixture budget.
	h.service = New(Options{Path: h.path, LockTimeout: sqliteFlowTestLockTimeout(), Clock: ClockFunc(func() (ClockSample, error) { return h.sample, h.clockErr })})
	input := qaLinkInput(t)
	input.RequestID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	first, err := h.service.Link(context.Background(), input, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil {
		t.Fatal(err)
	}
	computer := *h.snapshot().ComputerID
	event := qaEvent("A", "1", "1", "work", first.Binding.ID)
	event.Actor.ComputerID = computer
	h.ingest(0, event)
	oldActor := h.snapshot().Actors[0]
	h.at(300)
	_, err = h.service.ObserveSource(context.Background(), SourceObservation{Actor: oldActor.Ref, Reason: "source_lost", RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"})
	if err != nil {
		t.Fatal(err)
	}
	event.Sequence = "2"
	event.EventID = "A-finish"
	event.Kind = "finish"
	event.BindingID = ""
	event.BindingRevision = ""
	h.ingest(3600, event)
	oldUncertainty := h.snapshot().Uncertainties[0]
	if oldUncertainty.UpperBound == nil || !oldUncertainty.UpperBound.Equal(*qaRecoveryTime(3600)) {
		t.Fatalf("old epoch missing trustworthy cap %+v", oldUncertainty)
	}
	input.Timezone = "America/New_York"
	input.IfRevision = first.Binding.Revision
	input.RequestID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	second, err := h.service.Link(context.Background(), input, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil {
		t.Fatal(err)
	}
	event = qaEvent("B", "1", "1", "work", second.Binding.ID)
	event.Actor.ComputerID = computer
	event.BindingRevision = second.Binding.Revision
	h.ingest(7200, event)
	var later Actor
	for _, a := range h.snapshot().Actors {
		if a.Ref.Key.AgentID == "B" {
			later = a
		}
	}
	h.at(7500)
	_, err = h.service.ObserveSource(context.Background(), SourceObservation{Actor: later.Ref, Reason: "source_lost", RequestID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd"})
	if err != nil {
		t.Fatal(err)
	}
	var target Uncertainty
	for _, u := range h.snapshot().Uncertainties {
		if u.Actor.Key.AgentID == "B" {
			target = u
		}
	}
	h.at(8000)
	preview, err := h.service.Preview(context.Background(), RecoveryInput{UncertaintyID: target.ID, End: qaRecoveryTime(7800)})
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.StillBlockedIDs) != 0 {
		t.Fatalf("preview reports disjoint incompatible epoch as blocked: %v", preview.StillBlockedIDs)
	}
	_, err = h.service.Resolve(context.Background(), ResolveInput{UncertaintyID: target.ID, End: qaRecoveryTime(7800), IfRevision: target.Revision, RequestID: qaRecoveryRequest, Confirmed: true})
	if err != nil {
		t.Fatalf("disjoint later epoch blocked by bounded old uncertainty: %v", err)
	}
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{7200, 7800}})
	if s.ClosedIntervals[0].Attribution != second.Binding.Attribution {
		t.Fatal("later recovery used historical attribution")
	}
	for _, u := range s.Uncertainties {
		if u.ID == oldUncertainty.ID && !reflect.DeepEqual(u, oldUncertainty) {
			t.Fatalf("later recovery changed earlier uncertainty: %+v", u)
		}
	}
}

func TestQARecoveryCorruptBoundEvidenceRejectsBeforeProjection(t *testing.T) {
	for _, name := range []string{"sample-without-upper", "upper-without-sample", "unusable-bound-sample"} {
		t.Run(name, func(t *testing.T) {
			h, u := qaRecoveryUncertain(t)
			if name != "sample-without-upper" {
				h.ingest(900, qaEvent("A", "1", "3", "wait_user", ""))
				h.at(1000)
			}
			var data map[string]any
			if err := json.Unmarshal(qaRecoveryRead(t, h), &data); err != nil {
				t.Fatal(err)
			}
			evidence := data["uncertainty_evidence"].(map[string]any)[u.ID].(map[string]any)
			switch name {
			case "sample-without-upper":
				sample, _ := json.Marshal(h.sample)
				var decoded any
				if err := json.Unmarshal(sample, &decoded); err != nil {
					t.Fatal(err)
				}
				evidence["bound_sample"] = decoded
			case "upper-without-sample":
				evidence["bound_sample"] = nil
			case "unusable-bound-sample":
				evidence["bound_sample"].(map[string]any)["capability"] = "unavailable"
			}
			corrupt, err := json.Marshal(data)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(h.path, corrupt, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = h.service.Review(context.Background(), ReviewInput{})
			qaCode(t, err, "state_corrupt")
			if string(corrupt) != string(qaRecoveryRead(t, h)) {
				t.Fatal("malformed bound evidence changed")
			}
		})
	}
}
func TestQARecoveryNullActorWithReceiptRejectsWithoutPanic(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(10, qaEvent("A", "1", "2", "wait_user", ""))
	actor := h.snapshot().Actors[0]
	input := InterruptInput{ActorID: actor.ID, Generation: "1", IfRevision: actor.Revision, RequestID: qaRecoveryRequest, Confirmed: true}
	if _, err := h.service.Interrupt(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	var data map[string]any
	if err := json.Unmarshal(qaRecoveryRead(t, h), &data); err != nil {
		t.Fatal(err)
	}
	for key := range data["actors"].(map[string]any) {
		data["actors"].(map[string]any)[key] = nil
	}
	corrupt, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(h.path, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"review", "status", "replay"} {
		t.Run(op, func(t *testing.T) {
			var callErr error
			var panicked any
			func() {
				defer func() { panicked = recover() }()
				switch op {
				case "review":
					_, callErr = h.service.Review(context.Background(), ReviewInput{})
				case "status":
					_, callErr = h.service.Status(context.Background())
				case "replay":
					_, callErr = h.service.Interrupt(context.Background(), input)
				}
			}()
			if panicked != nil {
				t.Errorf("corrupt state panicked: %v", panicked)
			} else {
				qaCode(t, callErr, "state_corrupt")
			}
			if string(corrupt) != string(qaRecoveryRead(t, h)) {
				t.Fatal("corrupt null actor evidence changed")
			}
		})
	}
}
