//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func TestSQLiteSyncGraphRetainedLaterStatesOwnedCompleteAndReadOnly(t *testing.T) {
	for _, phase := range []string{"queued", "needs_attention", "submitting", "synced", "rejected", "unknown"} {
		t.Run(phase, func(t *testing.T) {
			st, _ := sqQALegacy(t, phase)
			f := sgQASeed(t, st)
			c, tx := interopOpen(t, f.location, false, sqliteio.Read)
			want := sgQAFirst(f.completeState)
			got := sgQAPositive(t, tx, f, want)
			roots := []string{want.ID, want.ID}
			requests := []string{}
			for id, r := range f.completeState.Requests {
				if syncOperation(r.Operation) {
					requests = append(requests, id, id)
				}
			}
			beforeRoots, beforeRequests := mqQASlice(roots), mqQASlice(requests)
			bgQAFixtureError(t, "explicit duplicate seeds positive", sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, roots, requests))
			if !reflect.DeepEqual(roots, beforeRoots) || !reflect.DeepEqual(requests, beforeRequests) {
				t.Fatal("validator mutated caller slices")
			}
			got.Interval.SegmentIDs[0] = sqQAMissing
			if got.EntryID != nil {
				*got.EntryID = "999"
			}
			if got.RunRequestID != nil {
				*got.RunRequestID = sqQAMissing
			}
			if got.FailureCategory != nil {
				*got.FailureCategory = "mutated"
			}
			if got.Plan != nil {
				got.Plan.CompanySource = "mutated"
				if got.Plan.Configuration.Clock != nil {
					*got.Plan.Configuration.Clock = "mutated"
				}
				p := &got.Plan.Parts[0]
				p.Notes = "mutated"
				for _, value := range []*string{p.EntryID, p.FailureCategory, p.ReturnedHours, p.RoundedHours, p.ConfirmedDurationNS, p.ProviderDeltaNS, p.TotalResidualNS, p.StartedTime, p.EndedTime} {
					if value != nil {
						*value = "mutated"
					}
				}
				if len(p.Attempts) > 0 {
					p.Attempts[0].ID = sqQAMissing
					if p.Attempts[0].EntryID != nil {
						*p.Attempts[0].EntryID = "999"
					}
				}
			}
			_ = sgQAPositive(t, tx, f, want)
			sgQAUnchanged(t, tx, f.meta, f.snapshot)
			interopRollback(t, tx)
			interopClose(t, c)
			sgQAReopen(t, f)
		})
	}
}

func TestSQLiteSyncGraphCanonicalScopeAndAbsence(t *testing.T) {
	for _, input := range []struct{ computer, id, revision string }{
		{"", sqQAMissing, "1"}, {"not-a-uuid", sqQAMissing, "1"},
		{interopComputer, "", "1"}, {interopComputer, "bad", "1"},
		{interopComputer, sqQAMissing, ""}, {interopComputer, sqQAMissing, "0"},
		{interopComputer, sqQAMissing, "01"}, {interopComputer, sqQAMissing, "-1"},
		{interopComputer, sqQAMissing, "18446744073709551616"},
	} {
		in, found, err := sqliteReadRetainedSyncInterval(nil, input.computer, input.id, input.revision)
		bgQAValidation(t, err)
		if found || !reflect.DeepEqual(in, Interval{}) {
			t.Fatal("invalid input exposed interval")
		}
		item, found, err := sqliteReadSyncItem(nil, input.computer, input.id, input.revision)
		bgQAValidation(t, err)
		if found || !reflect.DeepEqual(item, OutboxItem{}) {
			t.Fatal("invalid input exposed item")
		}
		bgQAValidation(t, sqliteValidateSelectedSync(nil, input.computer, input.revision, []string{input.id}, nil))
		bgQAValidation(t, sqliteValidateSelectedSync(nil, input.computer, input.revision, nil, []string{input.id}))
	}
	st, _ := sqQALegacy(t, "queued")
	f := sgQASeed(t, st)
	c, tx := interopOpen(t, f.location, false, sqliteio.Read)
	in, found, err := sqliteReadRetainedSyncInterval(tx, f.meta.ComputerID, sqQAMissing, f.meta.Revision)
	if err != nil || found || !reflect.DeepEqual(in, Interval{}) {
		t.Fatal("missing interval must be true absence", err)
	}
	item, found, err := sqliteReadSyncItem(tx, f.meta.ComputerID, sqQAMissing, f.meta.Revision)
	if err != nil || found || !reflect.DeepEqual(item, OutboxItem{}) {
		t.Fatal("missing root must be true absence", err)
	}
	bgQACorrupt(t, sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, []string{sqQAMissing}, nil))
	bgQACorrupt(t, sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{sqQAMissing}))
	want := sgQAFirst(f.completeState)
	foreign := cfQAUUID(9999)
	in, found, err = sqliteReadRetainedSyncInterval(tx, foreign, want.Interval.ID, f.meta.Revision)
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(in, Interval{}) {
		t.Fatal("wrong valid computer exposed interval")
	}
	item, found, err = sqliteReadSyncItem(tx, foreign, want.ID, f.meta.Revision)
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(item, OutboxItem{}) {
		t.Fatal("wrong valid computer exposed item")
	}
	bgQACorrupt(t, sqliteValidateSelectedSync(tx, foreign, f.meta.Revision, []string{want.ID}, nil))
	nonsync := false
	for id, receipt := range f.completeState.Requests {
		if !syncOperation(receipt.Operation) {
			nonsync = true
			bgQACorrupt(t, sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id}))
		}
	}
	if !nonsync {
		t.Fatal("actual non-sync receipt control absent")
	}
	bgQAFixtureError(t, "allocated empty selected sets", sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, []string{}, []string{}))
	sgQAUnchanged(t, tx, f.meta, f.snapshot)
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteSyncGraphImmutableSelectedCorruptionAndUnrelatedControl(t *testing.T) {
	a, other := cfQAAttr(), cfQAAttr()
	other.ProjectID = "99"
	st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(36), a}, cfQASpec{20, cfQATime(0), cfQATime(36), other})
	bgQAFixtureError(t, "real full legacy finalization", finalize(st))
	f := sgQASeed(t, st)
	var selected, unrelated OutboxItem
	for _, in := range f.completeState.Intervals {
		if in.Attribution == a {
			selected = f.completeState.Outbox[in.ID]
		} else {
			unrelated = f.completeState.Outbox[in.ID]
		}
	}
	for _, defect := range []string{"support-missing", "support-owner", "support-finalized", "epoch-missing", "epoch-attribution", "generation-missing", "segment-attribution", "projection", "seal-missing", "seal-wrong", "reverse-live-owner", "pending-survives", "frontier-survives", "outbox-missing"} {
		t.Run(defect, func(t *testing.T) {
			c, tx := interopOpen(t, f.location, false, sqliteio.Write)
			_ = sgQAPositive(t, tx, f, selected)
			_ = sgQAPositive(t, tx, f, unrelated)
			in := selected.Interval
			s := f.completeState.Segments[in.SegmentIDs[0]]
			component := sqQAComponent(t, f.completeState, in)
			switch defect {
			case "support-missing":
				interopDone(t, tx, "DELETE FROM interval_segments WHERE interval_id=?", sqliteio.Text(in.ID))
			case "support-owner":
				interopDone(t, tx, "UPDATE interval_segments SET interval_id=? WHERE segment_id=?", sqliteio.Text(sqQAMissing), sqliteio.Text(s.ID))
			case "support-finalized":
				interopDone(t, tx, "UPDATE segments SET finalized=0 WHERE segment_id=?", sqliteio.Text(s.ID))
			case "epoch-missing":
				interopDone(t, tx, "DELETE FROM epochs WHERE epoch_id=?", sqliteio.Text(s.EpochID))
			case "epoch-attribution":
				interopDone(t, tx, "UPDATE epochs SET task_id='999' WHERE epoch_id=?", sqliteio.Text(s.EpochID))
			case "generation-missing":
				interopDone(t, tx, "DELETE FROM actor_generations WHERE actor_key=? AND generation=?", sqliteio.Text(actorKey(s.Actor.Key)), interopCounter(t, s.Actor.Generation))
			case "segment-attribution":
				interopDone(t, tx, "UPDATE segments SET task_id='999' WHERE segment_id=?", sqliteio.Text(s.ID))
			case "projection":
				interopDone(t, tx, "UPDATE segments SET confirmed_sec=confirmed_sec+1 WHERE segment_id=?", sqliteio.Text(s.ID))
			case "seal-missing":
				interopDone(t, tx, "DELETE FROM interval_components WHERE interval_id=?", sqliteio.Text(in.ID))
			case "seal-wrong":
				interopDone(t, tx, "UPDATE interval_components SET component_id=? WHERE interval_id=?", sqliteio.Text(cfQAHash(f.meta.ComputerID, a, []string{cfQAUUID(999)})), sqliteio.Text(in.ID))
			case "reverse-live-owner":
				interopDone(t, tx, "INSERT INTO component_segments(component_id,segment_id) VALUES(?,?)", sqliteio.Text(component), sqliteio.Text(s.ID))
			case "pending-survives":
				values := []sqliteio.Value{sqliteio.Text(component), sqliteio.Text(attributionKey(f.meta.ComputerID, a))}
				values = append(values, ueQATimeValues(t, in.Start)...)
				values = append(values, ueQATimeValues(t, in.End)...)
				asQABindFixture(t, tx, "pending_finalization", cfQAPendingColumns, values)
			case "frontier-survives":
				_, err := sqliteWriteFrontierLocal(tx, f.meta.ComputerID, nil, sqliteFrontierLocalRow{ID: component, ComputerID: f.meta.ComputerID, Attribution: a, Start: in.Start, End: in.End})
				bgQAFixtureError(t, "locally valid stale frontier", err)
			case "outbox-missing":
				interopDone(t, tx, "DELETE FROM outbox WHERE interval_id=?", sqliteio.Text(in.ID))
			}
			inGot, found, err := sqliteReadRetainedSyncInterval(tx, f.meta.ComputerID, in.ID, f.meta.Revision)
			bgQACorrupt(t, err)
			if found || !reflect.DeepEqual(inGot, Interval{}) {
				t.Fatal("immutable failure exposed interval")
			}
			if defect == "outbox-missing" {
				got, found, err := sqliteReadSyncItem(tx, f.meta.ComputerID, selected.ID, f.meta.Revision)
				if err != nil || found || !reflect.DeepEqual(got, OutboxItem{}) {
					t.Fatal("deleted indexed root must be absent", err)
				}
				bgQACorrupt(t, sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, []string{selected.ID}, nil))
			} else {
				sgQAItemZero(t, tx, f, selected.ID)
			}
			_ = sgQAPositive(t, tx, f, unrelated)
			interopRollback(t, tx)
			interopClose(t, c)
			sgQAReopen(t, f)
		})
	}
}

func TestSQLiteSyncGraphRecoveryEvidenceDecisionRequestClosure(t *testing.T) {
	for _, shape := range []string{"resolved", "discarded"} {
		t.Run(shape, func(t *testing.T) {
			_, raw, u, _, decision, _ := rdQASource(t, shape)
			if len(raw.Intervals) == 0 {
				bgQAFixtureError(t, "finalize recovered complete state", finalize(raw))
			}
			f := sgQASeed(t, raw)
			var item OutboxItem
			for _, in := range f.completeState.Intervals {
				if cfQAContains(in.SegmentIDs, u.SegmentID) {
					item = f.completeState.Outbox[in.ID]
				}
			}
			if item.ID == "" {
				t.Fatal("actual recovered finalized support absent")
			}
			for _, defect := range []string{"evidence", "decision", "proof", "proof-ceiling"} {
				t.Run(defect, func(t *testing.T) {
					c, tx := interopOpen(t, f.location, false, sqliteio.Write)
					_ = sgQAPositive(t, tx, f, item)
					switch defect {
					case "evidence":
						interopDone(t, tx, "DELETE FROM uncertainty_evidence WHERE uncertainty_id=?", sqliteio.Text(u.ID))
					case "decision":
						interopDone(t, tx, "DELETE FROM recovery_decisions WHERE uncertainty_id=?", sqliteio.Text(u.ID))
					case "proof":
						interopDone(t, tx, "DELETE FROM requests WHERE request_id=?", sqliteio.Text(decision.RequestID))
					case "proof-ceiling":
						r := mqQAClone(sqliteMutationRequestRow{Value: f.completeState.Requests[decision.RequestID]}).Value
						r.MutationResult.SnapshotRevision = bump(f.meta.Revision)
						sgQAReplaceRequest(t, tx, decision.RequestID, r)
					}
					sgQAIntervalZero(t, tx, f, item.Interval)
					interopRollback(t, tx)
					interopClose(t, c)
					sgQAReopen(t, f)
				})
			}
		})
	}
}

func TestSQLiteSyncGraphCalendarPlansFrozenConsentAndExactBytes(t *testing.T) {
	for _, spec := range []struct {
		name, start, end, zone, mode, policy, clock string
		parts                                       int
	}{
		{"exact", "2026-10-02T09:00:00Z", "2026-10-02T09:00:36Z", "UTC", "duration", "exact", "", 1},
		{"tie", "2026-10-02T09:00:00Z", "2026-10-02T09:00:18Z", "UTC", "duration", "nearest-hundredth-hour", "", 1},
		{"signed-residual", "2026-10-02T09:00:00Z", "2026-10-02T09:02:17.482Z", "UTC", "duration", "nearest-hundredth-hour", "", 1},
		{"midnight", "2026-10-02T23:59:30Z", "2026-10-03T00:00:30Z", "UTC", "duration", "exact", "", 2},
		{"spring-short-day", "2026-03-08T05:00:00Z", "2026-03-10T04:00:00Z", "America/New_York", "duration", "exact", "", 2},
		{"fall-long-day", "2026-11-01T04:00:00Z", "2026-11-03T05:00:00Z", "America/New_York", "duration", "exact", "", 2},
		{"skipped-date", "2011-12-29T10:00:00Z", "2011-12-31T10:00:00Z", "Pacific/Apia", "duration", "exact", "", 2},
		{"timestamp12", "2026-10-02T09:00:00Z", "2026-10-02T09:02:00Z", "UTC", "timestamp", "exact", "12h", 1},
		{"timestamp24", "2026-10-02T09:00:00Z", "2026-10-02T09:02:00Z", "UTC", "timestamp", "exact", "24h", 1},
	} {
		t.Run(spec.name, func(t *testing.T) {
			start, err := time.Parse(time.RFC3339Nano, spec.start)
			bgQAFixtureError(t, "start", err)
			end, err := time.Parse(time.RFC3339Nano, spec.end)
			bgQAFixtureError(t, "end", err)
			st := sgQACalendar(t, start, end, spec.zone, spec.mode, spec.policy, spec.clock)
			item := sgQAFirst(st)
			if len(item.Plan.Parts) != spec.parts || len(st.SyncConfigurations) != 0 {
				t.Fatal("calendar/frozen-only positive shape")
			}
			// Decimal spelling is semantic data: the composer must not rebuild it.
			for i := range item.Plan.Parts {
				part := &item.Plan.Parts[i]
				if strings.Contains(part.PlannedHours, ".") {
					part.PlannedHours += "0"
				} else {
					part.PlannedHours += ".0"
				}
				// The marker authenticates exact decimal text, not just its value.
				part.Notes = syncMarker(item, item.Plan.Configuration, *part)
				if part.Attempts == nil {
					t.Fatal("queued attempts must be allocated")
				}
			}
			st.Outbox[item.Interval.ID] = item
			f := sgQASeed(t, st)
			c, tx := interopOpen(t, f.location, false, sqliteio.Read)
			_ = sgQAPositive(t, tx, f, sgQAFirst(f.completeState))
			sgQAUnchanged(t, tx, f.meta, f.snapshot)
			interopRollback(t, tx)
			interopClose(t, c)
			sgQAReopen(t, f)
		})
	}
}

func TestSQLiteSyncGraphPlanGraphOnlyDefectsHaveScalarPositiveControls(t *testing.T) {
	for _, defect := range []string{"notes", "date", "coverage-start", "coverage-end", "part-attribution-zone", "policy-rounding", "timestamp-clock", "timestamp-minute", "plan-presence", "part-gap", "empty-plan", "unexpected-part", "unexpected-attempt"} {
		t.Run(defect, func(t *testing.T) {
			mode, clock := "duration", ""
			if defect == "timestamp-clock" || defect == "timestamp-minute" {
				mode, clock = "timestamp", "24h"
			}
			st := sgQACalendar(t, cfQATime(0), cfQATime(120), "UTC", mode, "exact", clock)
			f := sgQASeed(t, st)
			item := sgQAFirst(f.completeState)
			c, tx := interopOpen(t, f.location, false, sqliteio.Write)
			_ = sgQAPositive(t, tx, f, item)
			p := spQAPart(item.Interval.ID, 0, item.Plan.Parts[0])
			switch defect {
			case "notes":
				p.Notes += "wrong"
			case "date":
				p.SpentDate = "2026-10-03"
			case "coverage-start":
				p.Start = p.Start.Add(time.Second)
				p.DurationNS = "119000000000"
				p.PlannedDurationNS = p.DurationNS
				p.PlannedHours = "0.033055555555555556"
			case "coverage-end":
				p.End = p.End.Add(-time.Second)
				p.DurationNS = "119000000000"
				p.PlannedDurationNS = p.DurationNS
				p.PlannedHours = "0.033055555555555556"
			case "part-attribution-zone":
				interopDone(t, tx, "UPDATE sync_plans SET config_account_id='999' WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "policy-rounding":
				interopDone(t, tx, "UPDATE sync_plans SET config_duration_policy='nearest-hundredth-hour',config_policy_version='nearest-hundredth-hour-v1' WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "timestamp-clock":
				p.StartedTime = spQAPtr("9:00am")
			case "timestamp-minute":
				p.StartedTime = spQAPtr("09:01")
			case "plan-presence":
				interopDone(t, tx, "UPDATE outbox SET plan_present=0 WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "part-gap":
				interopDone(t, tx, "UPDATE sync_parts SET ordinal=1 WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "empty-plan":
				interopDone(t, tx, "DELETE FROM sync_parts WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "unexpected-part", "unexpected-attempt":
				interopDone(t, tx, "DELETE FROM sync_plans WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
				interopDone(t, tx, "UPDATE outbox SET plan_present=0 WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
				if defect == "unexpected-attempt" {
					interopDone(t, tx, "DELETE FROM sync_parts WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
					bgQAFixtureError(t, "raw genuine native orphan attempt", cfQARawAttempt(t, tx, item.Interval.ID, cfQAUUID(777)))
				}
			}
			if defect == "date" || defect == "coverage-start" || defect == "coverage-end" || defect == "policy-rounding" || defect == "timestamp-clock" || defect == "timestamp-minute" {
				// Keep the complete positive capture/request graph, mutate only the
				// intended semantic fact and regenerate its contextual marker. A
				// stale Notes digest must not mask a missing graph predicate.
				negative := sgQACloneState(t, f.completeState)
				changed := sgQAFirst(negative)
				if defect == "policy-rounding" {
					changed.Plan.Configuration.DurationPolicy, changed.Plan.Configuration.PolicyVersion = "nearest-hundredth-hour", "nearest-hundredth-hour-v1"
				}
				part := changed.Plan.Parts[0]
				part.SpentDate, part.Start, part.End = p.SpentDate, p.Start, p.End
				part.DurationNS, part.PlannedHours = p.DurationNS, p.PlannedHours
				part.PlannedDurationNS, part.PlannedResidualNS = p.PlannedDurationNS, p.PlannedResidualNS
				part.StartedTime, part.EndedTime = p.StartedTime, p.EndedTime
				part.Notes = syncMarker(changed, changed.Plan.Configuration, part)
				p.Notes = part.Notes
				changed.Plan.Parts[0] = part
				negative.Outbox[changed.Interval.ID] = changed
				if part.Notes != syncMarker(changed, changed.Plan.Configuration, part) {
					t.Fatal("targeted semantic negative has a stale marker")
				}
				if validSyncState(negative) || validState(negative) {
					t.Fatal("complete legacy negative oracle accepted targeted semantic defect", defect)
				}
			}
			if defect == "notes" || defect == "date" || defect == "coverage-start" || defect == "coverage-end" || defect == "policy-rounding" || defect == "timestamp-clock" || defect == "timestamp-minute" {
				interopDone(t, tx, "DELETE FROM sync_parts WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
				_, err := sqliteWriteSyncPartLocal(tx, f.meta.ComputerID, nil, p)
				bgQAFixtureError(t, "otherwise locally valid scalar part", err)
				_ = spQAReadPart(t, tx, f.meta.ComputerID, p)
			}
			sgQAItemZero(t, tx, f, item.ID)
			interopRollback(t, tx)
			interopClose(t, c)
			sgQAReopen(t, f)
		})
	}
}

func TestSQLiteSyncGraphStandaloneResolveMatchesActualUUIDOnlyPolicy(t *testing.T) {
	for _, ids := range [][]string{{}, {sqQAMissing}, {sqQAMissing, sqQAMissing}, {sqQAMissing, cfQAUUID(999)}} {
		for _, changed := range []bool{false, true} {
			for _, entity := range []*string{nil, spQAPtr("opaque-not-a-counter")} {
				st, _ := sqQALegacy(t, "queued")
				id := sgQAStandalone(t, st, mqQASlice(ids), changed, entity)
				f := sgQASeed(t, st)
				c, tx := interopOpen(t, f.location, false, sqliteio.Read)
				row := mqQARead(t, tx, f.meta.ComputerID, id, f.meta.Revision)
				if !reflect.DeepEqual(row.Value, f.completeState.Requests[id]) {
					t.Fatal("actual scalar policy positive changed")
				}
				bgQAFixtureError(t, "standalone terminal result adds no forward roots", sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id, id}))
				sgQAUnchanged(t, tx, f.meta, f.snapshot)
				interopRollback(t, tx)
				interopClose(t, c)
			}
		}
	}
}

func TestSQLiteSyncGraphActualRetryAndAttachmentRequireSingletonOwner(t *testing.T) {
	for _, family := range []string{"retry", "attachment"} {
		var raw *state
		var id string
		if family == "retry" {
			raw, id = sgQARetry(t)
		} else {
			raw, id = sgQAAttached(t)
		}
		f := sgQASeed(t, raw)
		item := sgQAFirst(f.completeState)
		for _, wrong := range [][]string{{}, {sqQAMissing}, {item.ID, item.ID}, {item.ID, sqQAMissing}} {
			t.Run(family+"/"+string(rune('0'+len(wrong))), func(t *testing.T) {
				c, tx := interopOpen(t, f.location, false, sqliteio.Write)
				_ = sgQAPositive(t, tx, f, item)
				bgQAFixtureError(t, "reverse-only valid owner", sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id}))
				r := mqQAClone(sqliteMutationRequestRow{Value: f.completeState.Requests[id]}).Value
				r.MutationResult.AffectedIDs = mqQASlice(wrong)
				sgQAReplaceRequest(t, tx, id, r)
				_ = mqQARead(t, tx, f.meta.ComputerID, id, f.meta.Revision)
				sgQAItemZero(t, tx, f, item.ID)
				bgQACorrupt(t, sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id}))
				interopRollback(t, tx)
				interopClose(t, c)
				sgQAReopen(t, f)
			})
		}
	}
}

func TestSQLiteSyncGraphAttemptsPendingTerminalAndHistoricalClassifications(t *testing.T) {
	st, item := sqQALegacy(t, "synced")
	request := item.Plan.Parts[0].Attempts[0].RequestID
	r := st.Requests[request]
	r.SyncRun.ResolvedIDs, r.SyncRun.BlockedIDs = []string{}, []string{item.ID}
	r.SyncRun.RemainingCount = 777
	st.Requests[request] = r
	// Historical classifications deliberately differ from today's synced root.
	f := sgQASeed(t, st)
	for _, defect := range []string{"origin-op", "origin-membership", "last-state", "last-entry", "last-failure", "attempt-gap", "part-state", "root-state", "missing-plan", "run-endpoint"} {
		t.Run(defect, func(t *testing.T) {
			c, tx := interopOpen(t, f.location, false, sqliteio.Write)
			item := sgQAFirst(f.completeState)
			_ = sgQAPositive(t, tx, f, item)
			bgQAFixtureError(t, "historical terminal owner reverse positive", sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{request}))
			switch defect {
			case "origin-op":
				r := mutationRequest{Operation: "sync.reconcile", Fingerprint: f.completeState.Requests[request].Fingerprint, SyncRun: f.completeState.Requests[request].SyncRun}
				sgQAReplaceRequest(t, tx, request, r)
			case "origin-membership", "run-endpoint":
				r := mqQAClone(sqliteMutationRequestRow{Value: f.completeState.Requests[request]}).Value
				if defect == "origin-membership" {
					r.SyncRun.AttemptedIDs = []string{}
				} else {
					r.SyncRun.ResolvedIDs = []string{sqQAMissing}
				}
				sgQAReplaceRequest(t, tx, request, r)
			case "last-state":
				interopDone(t, tx, "UPDATE sync_attempts SET state='rejected' WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "last-entry":
				interopDone(t, tx, "UPDATE sync_attempts SET entry_id='999' WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "last-failure":
				interopDone(t, tx, "UPDATE sync_attempts SET failure_category='validation' WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "attempt-gap":
				interopDone(t, tx, "UPDATE sync_attempts SET ordinal=1,number='2' WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "part-state":
				interopDone(t, tx, "UPDATE sync_parts SET state='queued' WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "root-state":
				interopDone(t, tx, "UPDATE outbox SET state='unknown' WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "missing-plan":
				interopDone(t, tx, "DELETE FROM sync_plans WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			}
			sgQAItemZero(t, tx, f, item.ID)
			bgQACorrupt(t, sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{request}))
			interopRollback(t, tx)
			interopClose(t, c)
			sgQAReopen(t, f)
		})
	}
}

func TestSQLiteSyncGraphNativeErrorsReturnZeroAndPreserveCause(t *testing.T) {
	st, _ := sqQALegacy(t, "synced")
	f := sgQASeed(t, st)
	item := sgQAFirst(f.completeState)
	for _, fault := range []string{"prepare", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := sqliteio.Open(ctx, f.location.directory, f.location.database, sqliteio.Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			bgQAFixtureError(t, "actual native open", err)
			t.Cleanup(func() {
				if err := c.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			tx, err := c.Begin(ctx, sqliteio.Write)
			bgQAFixtureError(t, "actual native begin", err)
			_ = sgQAPositive(t, tx, f, item)
			if fault == "prepare" {
				interopDone(t, tx, "DROP TABLE interval_segments")
			} else {
				cancel()
			}
			in, found, err := sqliteReadRetainedSyncInterval(tx, f.meta.ComputerID, item.Interval.ID, f.meta.Revision)
			var native *sqliteio.Error
			if !errors.As(err, &native) || fault == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal("actual native cause lost", err)
			}
			if found || !reflect.DeepEqual(in, Interval{}) {
				t.Fatal("native error leaked interval")
			}
			got, found, err := sqliteReadSyncItem(tx, f.meta.ComputerID, item.ID, f.meta.Revision)
			if !errors.As(err, &native) || fault == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal("actual native cause lost", err)
			}
			if found || !reflect.DeepEqual(got, OutboxItem{}) {
				t.Fatal("native error leaked item")
			}
			err = sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, []string{item.ID}, nil)
			if !errors.As(err, &native) || fault == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal("actual native graph cause lost", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			sgQAReopen(t, f)
		})
	}
}

func TestSQLiteSyncGraphAllFourExplicitRequestReverseOwnersAreUnconditional(t *testing.T) {
	for _, family := range []string{"run", "retry", "attachment", "attempt"} {
		var st *state
		var id string
		switch family {
		case "run":
			st, _ = sqQALegacy(t, "submitting")
			id = *sgQAFirst(st).RunRequestID
		case "retry":
			st, id = sgQARetry(t)
		case "attachment":
			st, id = sgQAAttached(t)
		case "attempt":
			st, _ = sqQALegacy(t, "synced")
			id = sgQAFirst(st).Plan.Parts[0].Attempts[0].RequestID
		}
		f := sgQASeed(t, st)
		item := sgQAFirst(f.completeState)
		for _, defect := range []string{"terminal-uuid-only-result", "orphan-owner", "malformed-tuple"} {
			t.Run(family+"/"+defect, func(t *testing.T) {
				c, tx := interopOpen(t, f.location, false, sqliteio.Write)
				_ = sgQAPositive(t, tx, f, item)
				bgQAFixtureError(t, "actual reverse family positive with no root seeds", sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id}))
				switch defect {
				case "terminal-uuid-only-result":
					// This scalar admits arbitrary UUIDs and creates no forward roots.
					// Every family must still expand the actual reverse owner.
					if family == "run" {
						interopDone(t, tx, "DELETE FROM pending_sync_roots WHERE request_id=?", sqliteio.Text(id))
						interopDone(t, tx, "DELETE FROM pending_sync WHERE request_id=?", sqliteio.Text(id))
					}
					r := mutationRequest{Operation: "sync.resolve", Fingerprint: mutationFingerprint("sync.resolve", SyncResolveInput{RequestID: id}), MutationResult: &MutationResult{ContractVersion: 1, RequestID: id, SnapshotRevision: "1", AffectedIDs: []string{sqQAMissing}, EntityRevision: spQAPtr("opaque"), Changed: false}}
					sgQAReplaceRequest(t, tx, id, r)
					_ = mqQARead(t, tx, f.meta.ComputerID, id, f.meta.Revision)
				case "orphan-owner":
					if family == "run" || family == "retry" {
						interopDone(t, tx, "UPDATE outbox SET interval_id=?,correlation=? WHERE id=?", sqliteio.Text(sqQAMissing), sqliteio.Text("tempo:"+sqQAMissing), sqliteio.Text(item.ID))
					} else if family == "attachment" {
						interopDone(t, tx, "UPDATE sync_parts SET interval_id=? WHERE attachment_request_id=?", sqliteio.Text(sqQAMissing), sqliteio.Text(id))
					} else {
						interopDone(t, tx, "UPDATE sync_attempts SET interval_id=? WHERE request_id=?", sqliteio.Text(sqQAMissing), sqliteio.Text(id))
					}
				case "malformed-tuple":
					if family == "run" || family == "retry" {
						sgQARelax(t, tx, "outbox")
						interopDone(t, tx, "UPDATE outbox SET interval_id=CAST(interval_id AS BLOB) WHERE id=?", sqliteio.Text(item.ID))
					} else if family == "attachment" {
						sgQARelax(t, tx, "sync_parts")
						interopDone(t, tx, "UPDATE sync_parts SET ordinal=CAST('0' AS TEXT) WHERE attachment_request_id=?", sqliteio.Text(id))
					} else {
						sgQARelax(t, tx, "sync_attempts")
						interopDone(t, tx, "UPDATE sync_attempts SET part_ordinal=CAST('0' AS TEXT) WHERE request_id=?", sqliteio.Text(id))
					}
				}
				bgQACorrupt(t, sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, []string{}, []string{id}))
				interopRollback(t, tx)
				interopClose(t, c)
				sgQAReopen(t, f)
			})
		}
	}
}

func TestSQLiteSyncGraphReadDependencyDoesNotRecursivelyExpandChangedReceipt(t *testing.T) {
	s, _, _ := qaSyncFixture(t, 36*time.Second)
	_ = qaSyncAppendCapturedInterval(t, s)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	id := qaSyncID(891)
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: id}, qaSyncDeps(t, p))
	bgQAFixtureError(t, "actual two-root shared origin", err)
	if len(p.posts) != 2 {
		t.Fatal("mock positive requires two actual attempts")
	}
	f := sgQASeed(t, bgQAReadLegacy(t, s))
	first := f.completeState.Outbox[f.completeState.Intervals[0].ID]
	second := f.completeState.Outbox[f.completeState.Intervals[1].ID]
	c, tx := interopOpen(t, f.location, false, sqliteio.Write)
	_ = sgQAPositive(t, tx, f, first)
	_ = sgQAPositive(t, tx, f, second)
	bgQAFixtureError(t, "explicit shared receipt positive", sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id}))
	interopDone(t, tx, "UPDATE sync_parts SET notes=notes || 'wrong' WHERE interval_id=?", sqliteio.Text(second.Interval.ID))
	// Reading the origin checks both scalar endpoints, not the second complete
	// tree or that root's receipts. An explicit changed request checks all owners.
	_ = sgQAPositive(t, tx, f, first)
	sgQAItemZero(t, tx, f, second.ID)
	bgQACorrupt(t, sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id}))
	interopRollback(t, tx)
	interopClose(t, c)
	sgQAReopen(t, f)
}

func TestSQLiteSyncGraphPendingProjectionSingletonAndRunRelationships(t *testing.T) {
	st, item := sqQALegacy(t, "submitting")
	id := *item.RunRequestID
	f := sgQASeed(t, st)
	for _, defect := range []string{"root-without-run", "run-owner-not-submitting", "pending-root-missing", "projection-missing", "projection-owner", "reservation-root-orphan", "singleton-orphan", "ceiling"} {
		t.Run(defect, func(t *testing.T) {
			c, tx := interopOpen(t, f.location, false, sqliteio.Write)
			_ = sgQAPositive(t, tx, f, sgQAFirst(f.completeState))
			bgQAFixtureError(t, "pending explicit request positive", sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id}))
			switch defect {
			case "root-without-run":
				interopDone(t, tx, "UPDATE outbox SET run_request_id=NULL WHERE id=?", sqliteio.Text(item.ID))
			case "run-owner-not-submitting":
				interopDone(t, tx, "UPDATE outbox SET state='queued' WHERE id=?", sqliteio.Text(item.ID))
			case "pending-root-missing":
				r := mqQAClone(sqliteMutationRequestRow{Value: f.completeState.Requests[id]}).Value
				r.PendingSync.RootIDs = []string{}
				sgQAReplaceRequest(t, tx, id, r)
				interopDone(t, tx, "DELETE FROM pending_sync_roots WHERE request_id=?", sqliteio.Text(id))
			case "projection-missing":
				interopDone(t, tx, "DELETE FROM pending_sync WHERE request_id=?", sqliteio.Text(id))
			case "projection-owner":
				interopDone(t, tx, "UPDATE pending_sync_roots SET request_id=? WHERE request_id=?", sqliteio.Text(sqQAMissing), sqliteio.Text(id))
			case "reservation-root-orphan":
				r := mqQAClone(sqliteMutationRequestRow{Value: f.completeState.Requests[id]}).Value
				r.PendingSync.RootIDs = []string{sqQAMissing}
				sgQAReplaceRequest(t, tx, id, r)
				interopDone(t, tx, "UPDATE pending_sync_roots SET outbox_id=? WHERE request_id=?", sqliteio.Text(sqQAMissing), sqliteio.Text(id))
			case "singleton-orphan":
				interopDone(t, tx, "UPDATE pending_sync SET request_id=? WHERE request_id=?", sqliteio.Text(sqQAMissing), sqliteio.Text(id))
			case "ceiling":
				r := mqQAClone(sqliteMutationRequestRow{Value: f.completeState.Requests[id]}).Value
				r.PendingSync.SnapshotRevision = bump(f.meta.Revision)
				sgQAReplaceRequest(t, tx, id, r)
				interopDone(t, tx, "UPDATE pending_sync SET snapshot_revision=? WHERE request_id=?", interopCounter(t, bump(f.meta.Revision)), sqliteio.Text(id))
			}
			sgQAItemZero(t, tx, f, item.ID)
			bgQACorrupt(t, sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id}))
			interopRollback(t, tx)
			interopClose(t, c)
			sgQAReopen(t, f)
		})
	}
}

func TestSQLiteSyncGraphNativeKindsNullsRawUTF8AndAmountPointers(t *testing.T) {
	st, _ := sqQALegacy(t, "synced")
	f := sgQASeed(t, st)
	for _, defect := range []string{"duration-text", "duration-null", "hours-blob", "hours-empty", "notes-invalid-utf8", "rounded-empty", "confirmed-text", "delta-null", "time-byte-mismatch", "attempt-number-integer", "root-revision-integer"} {
		t.Run(defect, func(t *testing.T) {
			c, tx := interopOpen(t, f.location, false, sqliteio.Write)
			item := sgQAFirst(f.completeState)
			_ = sgQAPositive(t, tx, f, item)
			switch defect {
			case "attempt-number-integer":
				sgQARelax(t, tx, "sync_attempts")
				interopDone(t, tx, "UPDATE sync_attempts SET number=1 WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "root-revision-integer":
				sgQARelax(t, tx, "outbox")
				interopDone(t, tx, "UPDATE outbox SET revision=1 WHERE id=?", sqliteio.Text(item.ID))
			default:
				sgQARelax(t, tx, "sync_parts")
				assignment := map[string]string{"duration-text": "duration_ns=CAST(duration_ns AS TEXT)", "duration-null": "duration_ns=NULL", "hours-blob": "planned_hours=CAST(planned_hours AS BLOB)", "hours-empty": "planned_hours=''", "notes-invalid-utf8": "notes=CAST(X'FF' AS TEXT)", "rounded-empty": "rounded_hours=''", "confirmed-text": "confirmed_duration_ns=CAST(confirmed_duration_ns AS TEXT)", "delta-null": "provider_delta_ns=NULL", "time-byte-mismatch": "start_json='\"2026-10-02T09:00:01Z\"'"}[defect]
				interopDone(t, tx, "UPDATE sync_parts SET "+assignment+" WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			}
			sgQAItemZero(t, tx, f, item.ID)
			interopRollback(t, tx)
			interopClose(t, c)
			sgQAReopen(t, f)
		})
	}
	// RoundedHours is optional informational data even for a confirmed part.
	st = sgQACloneState(t, f.completeState)
	item := sgQAFirst(st)
	item.Plan.Parts[0].RoundedHours = nil
	st.Outbox[item.Interval.ID] = item
	positive := sgQASeed(t, st)
	sgQAReopen(t, positive)
}

func TestSQLiteSyncGraphCrossFamilyIDsAreProbedForEverySelectedPartAndAttempt(t *testing.T) {
	st, _ := sqQALegacy(t, "synced")
	f := sgQASeed(t, st)
	for _, defect := range []string{"part-as-root", "part-as-attempt", "attempt-as-root", "attempt-as-part"} {
		t.Run(defect, func(t *testing.T) {
			c, tx := interopOpen(t, f.location, false, sqliteio.Write)
			item := sgQAFirst(f.completeState)
			_ = sgQAPositive(t, tx, f, item)
			partID, attemptID := item.Plan.Parts[0].ID, item.Plan.Parts[0].Attempts[0].ID
			// A separate relaxed row has the conflicting ID; original selected
			// scalars stay locally valid. No native trigger is disabled in production.
			switch defect {
			case "part-as-root", "attempt-as-root":
				sgQARelax(t, tx, "outbox")
				id := partID
				if defect == "attempt-as-root" {
					id = attemptID
				}
				interopDone(t, tx, "INSERT INTO outbox SELECT ?,?,revision,state,?,entry_id,failure_category,retry_request_id,run_request_id,plan_present FROM outbox WHERE id=?", sqliteio.Text(sqQAMissing), sqliteio.Text(id), sqliteio.Text("tempo:"+sqQAMissing), sqliteio.Text(item.ID))
			case "part-as-attempt":
				sgQARelax(t, tx, "sync_attempts")
				interopDone(t, tx, "INSERT INTO sync_attempts SELECT ?,0,0,request_id,?,number,state,entry_id,failure_category FROM sync_attempts WHERE id=?", sqliteio.Text(sqQAMissing), sqliteio.Text(partID), sqliteio.Text(attemptID))
			case "attempt-as-part":
				sgQARelax(t, tx, "sync_parts")
				interopDone(t, tx, "INSERT INTO sync_parts SELECT ?,ordinal,?,spent_date,duration_ns,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,planned_hours,planned_duration_ns,planned_residual_ns,started_time,ended_time,?,notes,state,entry_id,failure_category,returned_hours,rounded_hours,confirmed_duration_ns,provider_delta_ns,total_residual_ns,attachment_request_id,attachment_entry_id FROM sync_parts WHERE id=?", sqliteio.Text(sqQAMissing), sqliteio.Text(attemptID), sqliteio.Text("tempo:v1:"+attemptID), sqliteio.Text(partID))
			}
			sgQAItemZero(t, tx, f, item.ID)
			interopRollback(t, tx)
			interopClose(t, c)
			sgQAReopen(t, f)
		})
	}
}

func TestSQLiteSyncGraphHistoricalBindingAbsentCurrentActorAndNonUTCBoundaries(t *testing.T) {
	a := cfQAAttr()
	zone := time.FixedZone("retained-east", 3600)
	st := cfQAState(t, cfQASpec{10, cfQATime(0).In(zone), cfQATime(36).In(zone), a})
	bgQAFixtureError(t, "offset complete legacy finalization", finalize(st))
	// Current consent is a different full attribution and counter. The segment's
	// historical snapshot and generation remain authoritative after this change.
	b := st.Bindings[qaBindingA]
	b.Revision, b.Attribution.TaskID = asQAMax, "999"
	st.Bindings[qaBindingA] = b
	if record, exists := st.BindingRecords[qaBindingA]; exists {
		record.Snapshot = b
		st.BindingRecords[qaBindingA] = record
	}
	if len(st.Actors) != 0 {
		t.Fatal("fixture must have no current actor")
	}
	f := sgQASeed(t, st)
	c, tx := interopOpen(t, f.location, false, sqliteio.Read)
	item := sgQAFirst(f.completeState)
	got := sgQAPositive(t, tx, f, item)
	asQATimeWitness(t, item.Interval.Start, got.Interval.Start)
	asQATimeWitness(t, item.Interval.End, got.Interval.End)
	_, offset := got.Interval.Start.Zone()
	if offset != 3600 {
		t.Fatal("retained interval representation was normalized")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	sgQAReopen(t, f)
}

func TestSQLiteSyncGraphOtherPredecessorAllowsTouchRejectsOneNanosecondOverlap(t *testing.T) {
	a := cfQAAttr()
	st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(36), a}, cfQASpec{20, cfQATime(36), cfQATime(72), a})
	second := st.Segments[cfQAUUID(20)]
	var secondEpoch *timelineEpoch
	for i, ep := range st.Epochs {
		if ep.ID == second.EpochID {
			secondEpoch = ep
			st.Epochs = append(st.Epochs[:i], st.Epochs[i+1:]...)
			break
		}
	}
	delete(st.Segments, second.ID)
	bgQAFixtureError(t, "first retained component", finalize(st))
	st.Segments[second.ID] = second
	st.Epochs = append(st.Epochs, secondEpoch)
	bgQAFixtureError(t, "later touching retained component", finalize(st))
	if len(st.Intervals) != 2 || !st.Intervals[0].End.Equal(st.Intervals[1].Start) {
		t.Fatal("exact touching fixture")
	}
	f := sgQASeed(t, st)
	c, tx := interopOpen(t, f.location, false, sqliteio.Write)
	firstItem := f.completeState.Outbox[f.completeState.Intervals[0].ID]
	lastItem := f.completeState.Outbox[f.completeState.Intervals[1].ID]
	_ = sgQAPositive(t, tx, f, firstItem)
	_ = sgQAPositive(t, tx, f, lastItem)
	before := asQASegment(f.completeState.Segments[firstItem.Interval.SegmentIDs[0]])
	after := asQAJSON(t, before)
	end := after.Confirmed.Add(time.Nanosecond)
	after.Confirmed, after.End = end, &end
	after.ConfirmedSample.WallUTC = end
	n, err := strconv.ParseInt(*after.ConfirmedSample.ElapsedNS, 10, 64)
	bgQAFixtureError(t, "elapsed positive", err)
	value := strconv.FormatInt(n+1, 10)
	after.ConfirmedSample.ElapsedNS, after.ConfirmedSample.AwakeNS = &value, &value
	_, err = sqliteWriteSegmentLocal(tx, f.meta.ComputerID, &before, after)
	bgQAFixtureError(t, "otherwise valid overlapping support scalar", err)
	times := ueQATimeValues(t, end)
	values := append(times, interopCounter(t, "36000000001"), sqliteio.Text(firstItem.Interval.ID))
	interopDone(t, tx, "UPDATE intervals SET end_sec=?,end_nsec=?,end_json=?,duration_ns=? WHERE interval_id=?", values...)
	// Both finalization reconstructions would be individually valid; the other
	// predecessor interval overlaps the selected later root by exactly one ns.
	sgQAIntervalZero(t, tx, f, lastItem.Interval)
	interopRollback(t, tx)
	interopClose(t, c)
	sgQAReopen(t, f)
}

func TestSQLiteSyncGraphNonlastAttemptsRejectedAndExactHistoryOrder(t *testing.T) {
	st, item := sqQALegacy(t, "synced")
	p := &item.Plan.Parts[0]
	last := p.Attempts[0]
	prior := SyncAttempt{ID: qaSyncID(895), RequestID: last.RequestID, Number: "1", State: "rejected", FailureCategory: spQAPtr("validation")}
	last.Number = "2"
	p.Attempts = []SyncAttempt{prior, last}
	st.Outbox[item.Interval.ID] = item
	f := sgQASeed(t, st)
	for _, defect := range []string{"nonlast-unknown", "nonlast-submitting", "wrong-number", "queued-with-history"} {
		t.Run(defect, func(t *testing.T) {
			c, tx := interopOpen(t, f.location, false, sqliteio.Write)
			_ = sgQAPositive(t, tx, f, sgQAFirst(f.completeState))
			switch defect {
			case "nonlast-unknown":
				interopDone(t, tx, "UPDATE sync_attempts SET state='unknown' WHERE interval_id=? AND ordinal=0", sqliteio.Text(item.Interval.ID))
			case "nonlast-submitting":
				interopDone(t, tx, "UPDATE sync_attempts SET state='submitting' WHERE interval_id=? AND ordinal=0", sqliteio.Text(item.Interval.ID))
			case "wrong-number":
				sgQARelax(t, tx, "sync_attempts")
				interopDone(t, tx, "UPDATE sync_attempts SET number='1' WHERE interval_id=? AND ordinal=1", sqliteio.Text(item.Interval.ID))
			case "queued-with-history":
				part := spQAQueued(spQAPart(item.Interval.ID, 0, item.Plan.Parts[0]))
				interopDone(t, tx, "DELETE FROM sync_parts WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
				spQABindPart(t, tx, part)
				interopDone(t, tx, "UPDATE outbox SET state='queued',entry_id=NULL WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			}
			sgQAItemZero(t, tx, f, item.ID)
			interopRollback(t, tx)
			interopClose(t, c)
			sgQAReopen(t, f)
		})
	}
}

func TestSQLiteSyncGraphTimestampUnsafeCalendarFactsAreNeverRebuilt(t *testing.T) {
	for _, spec := range []struct{ name, start, end, zone string }{
		{"fractional-minute", "2026-10-02T09:00:00.5Z", "2026-10-02T09:02:00.5Z", "UTC"},
		{"ambiguous-fold", "2026-11-01T05:10:00Z", "2026-11-01T05:20:00Z", "America/New_York"},
		{"transition", "2026-11-01T05:55:00Z", "2026-11-01T06:05:00Z", "America/New_York"},
		{"midnight-ending", "2026-10-02T23:59:00Z", "2026-10-03T00:00:00Z", "UTC"},
	} {
		t.Run(spec.name, func(t *testing.T) {
			start, end := qaSyncTime(t, spec.start), qaSyncTime(t, spec.end)
			st := sgQACalendar(t, start, end, spec.zone, "duration", "exact", "")
			f := sgQASeed(t, st)
			item := sgQAFirst(f.completeState)
			loc, err := time.LoadLocation(spec.zone)
			bgQAFixtureError(t, "positive timezone", err)
			if syncTimestampSafe(start, end, loc) {
				t.Fatal("pure actual safety oracle unexpectedly accepts negative")
			}
			negative := sgQACloneState(t, f.completeState)
			bad := sgQAFirst(negative)
			bad.Plan.Configuration.Mode, bad.Plan.Configuration.Clock = "timestamp", spQAPtr("24h")
			for i := range bad.Plan.Parts {
				p := &bad.Plan.Parts[i]
				p.StartedTime, p.EndedTime = spQAPtr(p.Start.In(loc).Format("15:04")), spQAPtr(p.End.In(loc).Format("15:04"))
				p.Notes = syncMarker(bad, bad.Plan.Configuration, *p)
			}
			negative.Outbox[bad.Interval.ID] = bad
			if validSyncState(negative) {
				t.Fatal("complete actual sync predicate accepts unsafe timestamp negative")
			}
			c, tx := interopOpen(t, f.location, false, sqliteio.Write)
			_ = sgQAPositive(t, tx, f, item)
			interopDone(t, tx, "UPDATE sync_plans SET config_mode='timestamp',config_clock='24h' WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			interopDone(t, tx, "DELETE FROM sync_parts WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			for i, part := range bad.Plan.Parts {
				row := spQAPart(item.Interval.ID, int64(i), part)
				_, err := sqliteWriteSyncPartLocal(tx, f.meta.ComputerID, nil, row)
				bgQAFixtureError(t, "actual locally valid unsafe timestamp facts", err)
				_ = spQAReadPart(t, tx, f.meta.ComputerID, row)
			}
			sgQAItemZero(t, tx, f, item.ID)
			interopRollback(t, tx)
			interopClose(t, c)
			sgQAReopen(t, f)
		})
	}
}

func TestSQLiteSyncGraphFrozenPlanAndHistoricalConfigurationReceiptHaveSeparateDependencies(t *testing.T) {
	st, item := sqQALegacy(t, "synced")
	id := qaSyncID(1)
	key := syncConfigKey(item.Interval.Attribution.AccountID, item.Interval.Attribution.UserID)
	current := st.SyncConfigurations[key]
	current.Revision, current.Mode, current.DurationPolicy, current.PolicyVersion, current.Clock = asQAMax, "timestamp", "exact", "exact-v1", spQAPtr("24h")
	st.SyncConfigurations[key] = current
	f := sgQASeed(t, st)
	c, tx := interopOpen(t, f.location, false, sqliteio.Write)
	_ = sgQAPositive(t, tx, f, sgQAFirst(f.completeState))
	bgQAFixtureError(t, "historical configuration receipt monotone-current positive", sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id}))
	interopDone(t, tx, "DELETE FROM sync_configurations WHERE config_key=?", sqliteio.Text(key))
	_ = sgQAPositive(t, tx, f, sgQAFirst(f.completeState))
	bgQACorrupt(t, sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id}))
	interopRollback(t, tx)
	interopClose(t, c)
	sgQAReopen(t, f)
}

func TestSQLiteSyncGraphOldReservationOwnersRemainExplicitAfterProjectionRemoval(t *testing.T) {
	_, after, id := mqQAPhase(t, "resolve")
	f := sgQASeed(t, after)
	item := sgQAFirst(f.completeState)
	if f.completeState.Requests[id].Error == nil {
		t.Fatal("actual removed-reservation terminal error absent")
	}
	c, tx := interopOpen(t, f.location, false, sqliteio.Write)
	_ = sgQAPositive(t, tx, f, item)
	bgQAFixtureError(t, "removed reservation receipt positive", sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id}))
	interopDone(t, tx, "UPDATE sync_parts SET notes=notes || 'changed' WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
	// A completed error retains no forward root or reverse owner. The writer's
	// explicit old-owner seed is required to certify its changed former target.
	bgQAFixtureError(t, "terminal receipt does not invent removed owner", sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, nil, []string{id}))
	bgQACorrupt(t, sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, []string{item.ID}, []string{id}))
	interopRollback(t, tx)
	interopClose(t, c)
	sgQAReopen(t, f)
}

func TestSQLiteSyncGraphCalibratedLateNativeROWDoneAndCloseReturnZero(t *testing.T) {
	st, _ := sqQALegacy(t, "synced")
	f := sgQASeed(t, st)
	item := sgQAFirst(f.completeState)
	request := item.Plan.Parts[0].Attempts[0].RequestID
	// The existing process-global native hooks require serial tests; no Parallel.
	for _, family := range []string{"interval", "item", "selected"} {
		for _, boundary := range []string{"ROW", "DONE", "Close"} {
			t.Run(family+"/"+boundary, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				conn, err := sqliteio.Open(ctx, f.location.directory, f.location.database, sqliteio.Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
				bgQAFixtureError(t, "late-boundary actual native open", err)
				t.Cleanup(func() {
					if err := conn.Close(context.Background()); err != nil {
						t.Error("late-boundary checked fixture close", err)
					}
				})
				tx, err := conn.Begin(ctx, sqliteio.Read)
				bgQAFixtureError(t, "late-boundary caller-owned Begin", err)
				t.Cleanup(func() {
					if err := tx.Rollback(); err != nil {
						t.Error("late-boundary checked fixture rollback", err)
					}
				})
				roots, requests := []string{item.ID}, []string{request}
				beforeRoots, beforeRequests := mqQASlice(roots), mqQASlice(requests)
				spQAHookPositive(t, tx) // Actual known SELECT1 ROW100/DONE101/finalize0 ABI control.
				calibrated := sgQALateTrace(t, tx, f, item, family, roots, requests)
				repeated := sgQALateTrace(t, tx, f, item, family, roots, requests)
				if !reflect.DeepEqual(calibrated, repeated) {
					t.Fatal("same exact operation's positive native traces differ; cannot arm a truthful late target")
				}
				sgQAUnchanged(t, tx, f.meta, f.snapshot)
				targetIndex, target := sgQALateTarget(t, calibrated, boundary)
				actual := []spQASQLEvent{}
				fired := false
				observedTarget := spQASQLEvent{}
				cause := sqliteio.ErrUnsafe // Existing sanitized, unwrap-preserved adapter cause.
				spQAHooks(t, spQASQLHooks{
					Observe: func(e spQASQLEvent) { actual = append(actual, e) },
					Fault: func(e spQASQLEvent) error {
						if !fired && len(actual)-1 == targetIndex && e == target {
							fired, observedTarget = true, e
							return cause
						}
						return nil
					},
				})
				got := sgQALateRead(tx, f, item, family, roots, requests)
				spQASetSQLHooks(spQASQLHooks{})
				if !fired || observedTarget != target || len(actual) <= targetIndex || !reflect.DeepEqual(actual[:targetIndex+1], calibrated[:targetIndex+1]) {
					t.Fatal("calibrated native target/prefix was not actually reached", observedTarget, target)
				}
				if got.Found || !reflect.DeepEqual(got.Interval, Interval{}) || !reflect.DeepEqual(got.Item, OutboxItem{}) {
					t.Fatal("late native error exposed a complete or partial successful value")
				}
				spQANativeError(t, got.Err, cause)
				var native *sqliteio.Error
				wantPhase := sqliteio.StepPhase
				if boundary == "Close" {
					wantPhase = sqliteio.FinalizePhase
				}
				if !errors.As(got.Err, &native) || native.Phase != wantPhase || native.Category != sqliteio.Unsafe || native.Code != 0 {
					t.Fatal("late hook refusal lost actual sanitized adapter phase/cause", got.Err)
				}
				interopSafeError(t, got.Err, item.ID, item.Interval.ID, request)
				prepares, finalized := 0, 0
				for i, e := range actual {
					if e.Operation != "statement" && e.Operation != "prepare" {
						t.Fatal("failed read ended caller's transaction or connection", e)
					}
					if e.Phase == "prepare-before" {
						prepares++
						if i > targetIndex {
							t.Fatal("dependent SQL began after late selected cursor failure")
						}
					}
					if e.Phase == "finalize-after" {
						finalized++
						if e.Code != 0 {
							t.Fatal("fault-pass native cursor did not finalize cleanly", e)
						}
					}
				}
				if prepares != finalized {
					t.Fatal("late failure left a caller cursor open", prepares, finalized)
				}
				if ctx.Err() != nil || !reflect.DeepEqual(roots, beforeRoots) || !reflect.DeepEqual(requests, beforeRequests) {
					t.Fatal("read changed caller context or selection ownership")
				}
				sgQAUnchanged(t, tx, f.meta, f.snapshot)
				// Hook reset occurs before any SQL. A real subsequent read proves the
				// caller still owns the same active Tx; only the caller rolls it back.
				sgQALatePositive(t, family, sgQALateRead(tx, f, item, family, roots, requests), item)
				interopRollback(t, tx)
				interopClose(t, conn)
				sgQAReopen(t, f)
			})
		}
	}
}
