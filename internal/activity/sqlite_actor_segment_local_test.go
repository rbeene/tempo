//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent source-only before-code QA against actor/segment local contract
// 72da3ec4472e8615cb07f24f12147fbfa1daf6f1027975bca3bea41b3c4ceb8b.
// Coordinator owns application, formatting and native verification.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func asQAEnsureActor(t *testing.T, tx *sqliteio.Tx, row sqliteActorLocalRow) {
	t.Helper()
	for _, ref := range []*ActorRef{&row.Ref, row.Parent} {
		if ref != nil {
			if _, err := sqliteEnsureActorGeneration(tx, *ref); err != nil {
				t.Fatal(err)
			}
			if got, found, err := sqliteReadActorGeneration(tx, *ref); err != nil || !found || got != *ref {
				t.Fatal("exact generation control absent", err)
			}
		}
	}
}

func TestSQLiteActorSegmentLocalActualLifecycleRoundtripsChargesAndOwnedPointers(t *testing.T) {
	h := qaNew(t)
	h.seed()
	for i, event := range []Event{qaEvent("lifecycle", "1", "1", "work", qaBindingA), qaEvent("lifecycle", "1", "2", "observe_work", ""), qaEvent("lifecycle", "1", "3", "wait_user", ""), qaEvent("lifecycle", "2", "1", "work", qaBindingB)} {
		h.ingest(int64(i*10), event)
		source := bgQAReadLegacy(t, h.service)
		t.Run(fmt.Sprintf("actual-step-%d", i), func(t *testing.T) {
			f, m, snapshot := asQASeed(t, source)
			asQAReopen(t, f, m, snapshot, source)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			for _, a := range source.Actors {
				row := asQAActor(a)
				original := asQAJSON(t, row)
				charge, err := sqliteActorLocalCharge(row)
				if err != nil || charge != asQAActorCharge(t, row) || !reflect.DeepEqual(row, original) {
					t.Fatal("independent299 Actor charge/caller differs", err)
				}
				got, found, err := sqliteReadActorLocal(tx, source.ComputerID, a.Ref.Key)
				if err != nil || !found || !reflect.DeepEqual(got, original) {
					t.Fatal(err)
				}
				if got.LastEvidence.Epoch == a.LastEvidence.Epoch || got.LastEvidence.ElapsedNS == a.LastEvidence.ElapsedNS || got.LastEvidence.AwakeNS == a.LastEvidence.AwakeNS {
					t.Fatal("Actor clock pointers alias source")
				}
				*got.LastEvidence.Epoch = "caller-owned-change"
				*got.LastEvidence.ElapsedNS = "0"
				*got.LastEvidence.AwakeNS = "0"
				if got.SegmentID != nil {
					if got.SegmentID == a.SegmentID {
						t.Fatal("Actor segment pointer aliases source")
					}
					*got.SegmentID = qaRecoveryRequest
				}
				if again, found, err := sqliteReadActorLocal(tx, source.ComputerID, a.Ref.Key); err != nil || !found || !reflect.DeepEqual(again, original) {
					t.Fatal("Actor returned-pointer mutation reached storage", err)
				}
				ids, err := sqliteActorUncertaintyIDsLocal(tx, source.ComputerID, a.Ref.Key)
				if err != nil || ids == nil || !reflect.DeepEqual(ids, a.UncertaintyIDs) {
					t.Fatal("allocated original child order differs", err)
				}
			}
			for _, s := range source.Segments {
				row := asQASegment(s)
				original := asQAJSON(t, row)
				charge, err := sqliteSegmentLocalCharge(row)
				if err != nil || charge != asQASegmentCharge(t, row) || !reflect.DeepEqual(row, original) {
					t.Fatal("independent426 segment charge/caller differs", err)
				}
				got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, s.ID)
				if err != nil || !found || !reflect.DeepEqual(got, original) {
					t.Fatal(err)
				}
				if got.StartSample.Epoch == s.StartSample.Epoch || got.ConfirmedSample.ElapsedNS == s.ConfirmedSample.ElapsedNS {
					t.Fatal("segment clock pointer alias")
				}
				*got.StartSample.Epoch = "caller-owned-change"
				*got.ConfirmedSample.ElapsedNS = "0"
				if got.End != nil {
					if got.End == s.End {
						t.Fatal("End pointer alias")
					}
					*got.End = qaEpochStart
				}
				if got.UncertaintyID != nil {
					if got.UncertaintyID == s.UncertaintyID {
						t.Fatal("uncertainty pointer alias")
					}
					*got.UncertaintyID = qaRecoveryRequest
				}
				if again, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, s.ID); err != nil || !found || !reflect.DeepEqual(again, original) {
					t.Fatal("segment returned-pointer mutation reached storage", err)
				}
			}
			absent := event.Actor
			absent.AgentID = "absent"
			if got, found, err := sqliteReadActorLocal(tx, source.ComputerID, absent); err != nil || found || !reflect.DeepEqual(got, sqliteActorLocalRow{}) {
				t.Fatal("Actor absence differs", err)
			}
			if got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, qaRecoveryRequest); err != nil || found || !reflect.DeepEqual(got, sqliteSegmentLocalRow{}) {
				t.Fatal("segment absence differs", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			asQAReopen(t, f, m, snapshot, source)
		})
	}
	_, source := asQALegacy(t)
	actor, _ := asQARows(t, source)
	f, m, snapshot := asQASeed(t, source)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	got, found, err := sqliteReadActorLocal(tx, source.ComputerID, actor.Ref.Key)
	if err != nil || !found || got.Parent == nil || *got.Parent != *actor.Parent || got.Parent == actor.Parent || got.Parent.Key.ComputerID != asQAForeign {
		t.Fatal("historical foreign Parent roundtrip differs", err)
	}
	if interopCount(t, tx, "SELECT count(*) FROM actors") != int64(len(source.Actors)) {
		t.Fatal("historical Parent invented a current head")
	}
	got.Parent.Key.AgentID = "caller-parent"
	if again, found, err := sqliteReadActorLocal(tx, source.ComputerID, actor.Ref.Key); err != nil || !found || !reflect.DeepEqual(again, actor) {
		t.Fatal("Parent pointer mutation reached storage", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	asQAReopen(t, f, m, snapshot, source)
}

func asQAActorAxes() []string {
	return []string{"revision", "generation", "sequence", "state-segment-null", "health", "binding-id", "binding-revision", "account", "user", "project", "task", "timezone", "parent-key", "parent-generation", "parent-null", "segment-id", "wall", "epoch", "elapsed", "awake"}
}

func asQAActorAxis(t *testing.T, base sqliteActorLocalRow, axis string) sqliteActorLocalRow {
	r := asQAJSON(t, base)
	switch axis {
	case "revision":
		r.Revision = asQAMax
	case "generation":
		r.Ref.Generation = asQAMax
	case "sequence":
		r.Sequence = asQAMax
	case "state-segment-null":
		r.State, r.SegmentID = "wait_children", nil
	case "health":
		r.Health = "stale"
	case "binding-id":
		r.BindingID = qaBindingA
	case "binding-revision":
		r.BindingRevision = asQAMax
	case "account":
		r.Attribution.AccountID = "11"
	case "user":
		r.Attribution.UserID = "12"
	case "project":
		r.Attribution.ProjectID = "13"
	case "task":
		r.Attribution.TaskID = "14"
	case "timezone":
		r.Attribution.Timezone = "Etc/UTC"
	case "parent-key":
		r.Parent.Key.AgentID = "other-retained-parent"
	case "parent-generation":
		r.Parent.Generation = "9223372036854775808"
	case "parent-null":
		r.Parent = nil
	case "segment-id":
		r.SegmentID = asQAPointer(qaRecoveryRequest)
	case "wall":
		r.LastEvidence.WallUTC = r.LastEvidence.WallUTC.Add(time.Nanosecond).In(time.FixedZone("offset", 3600))
	case "epoch":
		r.LastEvidence.Epoch = asQAPointer("different-valid-epoch")
	case "elapsed":
		r.LastEvidence.ElapsedNS = asQAPointer("0")
	case "awake":
		r.LastEvidence.AwakeNS = asQAPointer("0")
	default:
		t.Fatal("unknown Actor axis")
	}
	if reflect.DeepEqual(r, base) {
		t.Fatal("vacuous Actor CAS axis", axis)
	}
	return r
}

func asQASegmentAxes() []string {
	return []string{"actor-key", "actor-generation", "binding-id", "binding-revision", "account", "user", "project", "task", "timezone", "epoch-id", "start-wall", "start-epoch", "start-elapsed", "start-awake", "confirmed-wall", "confirmed-epoch", "confirmed-elapsed", "confirmed-awake", "start", "confirmed", "end", "uncertainty", "finalized"}
}

func asQASegmentAxis(t *testing.T, base sqliteSegmentLocalRow, axis string) sqliteSegmentLocalRow {
	r := asQAJSON(t, base)
	switch axis {
	case "actor-key":
		r.Actor.Key.AgentID = "other-historical-actor"
	case "actor-generation":
		r.Actor.Generation = asQAMax
	case "binding-id":
		r.Binding.ID = qaBindingA
	case "binding-revision":
		r.Binding.Revision = asQAMax
	case "account":
		r.Binding.Attribution.AccountID = "11"
	case "user":
		r.Binding.Attribution.UserID = "12"
	case "project":
		r.Binding.Attribution.ProjectID = "13"
	case "task":
		r.Binding.Attribution.TaskID = "14"
	case "timezone":
		r.Binding.Attribution.Timezone = "Etc/UTC"
	case "epoch-id":
		r.EpochID = qaRecoveryRequest
	case "start-wall":
		r.StartSample.WallUTC = r.StartSample.WallUTC.Add(time.Nanosecond).In(time.FixedZone("offset", 3600))
	case "start-epoch":
		r.StartSample.Epoch = asQAPointer("different-valid-epoch")
	case "start-elapsed":
		r.StartSample.ElapsedNS = asQAPointer("0")
	case "start-awake":
		r.StartSample.AwakeNS = asQAPointer("0")
	case "confirmed-wall":
		r.ConfirmedSample.WallUTC = r.ConfirmedSample.WallUTC.Add(time.Nanosecond).In(time.FixedZone("offset", 3600))
	case "confirmed-epoch":
		r.ConfirmedSample.Epoch = asQAPointer("different-valid-epoch")
	case "confirmed-elapsed":
		r.ConfirmedSample.ElapsedNS = asQAPointer("0")
	case "confirmed-awake":
		r.ConfirmedSample.AwakeNS = asQAPointer("0")
	case "start":
		r.Start = r.Start.Add(-time.Nanosecond).In(time.FixedZone("offset", 3600))
	case "confirmed":
		r.Confirmed = r.Confirmed.Add(time.Nanosecond).In(time.FixedZone("offset", 3600))
	case "end":
		v := r.Start.Add(-time.Second)
		r.End = &v // Local API deliberately imposes no End>=Confirmed policy.
	case "uncertainty":
		r.UncertaintyID = asQAPointer(qaRecoveryRequest)
	case "finalized":
		r.Finalized = !r.Finalized
	default:
		t.Fatal("unknown segment axis")
	}
	if reflect.DeepEqual(r, base) {
		t.Fatal("vacuous segment CAS axis", axis)
	}
	return r
}

func TestSQLiteActorSegmentLocalFullOldCASValidAxesSignedDeltasAndGenerationChildren(t *testing.T) {
	_, source := asQALegacy(t)
	actor, segment := asQARows(t, source)
	f, m, snapshot := asQASeed(t, source)
	for _, axis := range asQAActorAxes() {
		t.Run("Actor/"+axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			selected := asQAActorAxis(t, actor, axis)
			asQAEnsureActor(t, tx, selected)
			if charge, err := sqliteActorLocalCharge(selected); err != nil || charge != asQAActorCharge(t, selected) {
				t.Fatal("valid Actor charge control failed", err)
			}
			if delta, err := sqliteWriteActorLocal(tx, source.ComputerID, &actor, selected); err != nil || delta != asQAActorCharge(t, selected)-asQAActorCharge(t, actor) {
				t.Fatal("valid Actor CAS control failed", err)
			}
			persisted := asQAJSON(t, selected)
			asQATimeWitness(t, selected.LastEvidence.WallUTC, persisted.LastEvidence.WallUTC)
			if got, found, err := sqliteReadActorLocal(tx, source.ComputerID, selected.Ref.Key); err != nil || !found || !reflect.DeepEqual(got, persisted) {
				t.Fatal("selected stale axis is not locally valid", err)
			}
			proposal := asQAJSON(t, actor)
			proposal.Revision = bump(actor.Revision)
			beforeSnapshot, _ := asQAAudit(t, tx)
			delta, err := sqliteWriteActorLocal(tx, source.ComputerID, &actor, proposal)
			bgQACorrupt(t, err)
			if delta != 0 {
				t.Fatal("stale Actor CAS returned charge")
			}
			afterSnapshot, _ := asQAAudit(t, tx)
			if !reflect.DeepEqual(beforeSnapshot, afterSnapshot) {
				t.Fatal("stale Actor CAS mutated rows")
			}
			ids, err := sqliteActorUncertaintyIDsLocal(tx, source.ComputerID, selected.Ref.Key)
			if err != nil || !reflect.DeepEqual(ids, source.Actors[actorKey(actor.Ref.Key)].UncertaintyIDs) {
				t.Fatal("generation/scalar replacement rewrote duplicate old-generation children", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			asQAReopen(t, f, m, snapshot, source)
		})
	}
	for _, axis := range asQASegmentAxes() {
		t.Run("segment/"+axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			selected := asQASegmentAxis(t, segment, axis)
			if _, err := sqliteEnsureActorGeneration(tx, selected.Actor); err != nil {
				t.Fatal(err)
			}
			if charge, err := sqliteSegmentLocalCharge(selected); err != nil || charge != asQASegmentCharge(t, selected) {
				t.Fatal("valid segment charge control failed", err)
			}
			if delta, err := sqliteWriteSegmentLocal(tx, source.ComputerID, &segment, selected); err != nil || delta != asQASegmentCharge(t, selected)-asQASegmentCharge(t, segment) {
				t.Fatal("valid segment CAS control failed", err)
			}
			persisted := asQAJSON(t, selected)
			for _, pair := range [][2]time.Time{{selected.StartSample.WallUTC, persisted.StartSample.WallUTC}, {selected.ConfirmedSample.WallUTC, persisted.ConfirmedSample.WallUTC}, {selected.Start, persisted.Start}, {selected.Confirmed, persisted.Confirmed}} {
				asQATimeWitness(t, pair[0], pair[1])
			}
			if got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, selected.ID); err != nil || !found || !reflect.DeepEqual(got, persisted) {
				t.Fatal("selected stale segment axis is not locally valid", err)
			}
			proposal := asQAJSON(t, segment)
			proposal.Confirmed = proposal.Confirmed.Add(time.Second)
			beforeSnapshot, _ := asQAAudit(t, tx)
			delta, err := sqliteWriteSegmentLocal(tx, source.ComputerID, &segment, proposal)
			bgQACorrupt(t, err)
			if delta != 0 {
				t.Fatal("stale segment CAS returned charge")
			}
			afterSnapshot, _ := asQAAudit(t, tx)
			if !reflect.DeepEqual(beforeSnapshot, afterSnapshot) {
				t.Fatal("stale segment CAS mutated rows")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			asQAReopen(t, f, m, snapshot, source)
		})
	}
	for _, axis := range []string{"Actor-id", "Actor-key", "Actor-source", "Actor-session", "Actor-computer", "segment-id", "segment-computer"} {
		t.Run("immutable/"+axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			// Fresh valid replacement controls precede each immutable refusal.
			afterActor := asQAJSON(t, actor)
			afterActor.Revision = bump(actor.Revision)
			if _, err := sqliteWriteActorLocal(tx, source.ComputerID, &actor, afterActor); err != nil {
				t.Fatal(err)
			}
			if _, err := sqliteWriteActorLocal(tx, source.ComputerID, &afterActor, actor); err != nil {
				t.Fatal(err)
			}
			afterSegment := asQAJSON(t, segment)
			afterSegment.Confirmed = segment.Confirmed.Add(time.Second)
			if _, err := sqliteWriteSegmentLocal(tx, source.ComputerID, &segment, afterSegment); err != nil {
				t.Fatal(err)
			}
			if _, err := sqliteWriteSegmentLocal(tx, source.ComputerID, &afterSegment, segment); err != nil {
				t.Fatal(err)
			}
			beforeSnapshot, _ := asQAAudit(t, tx)
			var delta int64
			var err error
			switch axis {
			case "Actor-id":
				afterActor.ID = qaRecoveryRequest
				delta, err = sqliteWriteActorLocal(tx, source.ComputerID, &actor, afterActor)
			case "Actor-key":
				afterActor.Ref.Key.AgentID = "changed-key"
				asQAEnsureActor(t, tx, afterActor)
				beforeSnapshot, _ = asQAAudit(t, tx)
				delta, err = sqliteWriteActorLocal(tx, source.ComputerID, &actor, afterActor)
			case "Actor-source":
				afterActor.Ref.Key.Source = "codex"
				asQAEnsureActor(t, tx, afterActor)
				beforeSnapshot, _ = asQAAudit(t, tx)
				delta, err = sqliteWriteActorLocal(tx, source.ComputerID, &actor, afterActor)
			case "Actor-session":
				afterActor.Ref.Key.SessionID = "changed-session"
				asQAEnsureActor(t, tx, afterActor)
				beforeSnapshot, _ = asQAAudit(t, tx)
				delta, err = sqliteWriteActorLocal(tx, source.ComputerID, &actor, afterActor)
			case "Actor-computer":
				afterActor.Ref.Key.ComputerID = asQAForeign
				asQAEnsureActor(t, tx, afterActor)
				beforeSnapshot, _ = asQAAudit(t, tx)
				delta, err = sqliteWriteActorLocal(tx, source.ComputerID, &actor, afterActor)
			case "segment-id":
				afterSegment.ID = qaRecoveryRequest
				delta, err = sqliteWriteSegmentLocal(tx, source.ComputerID, &segment, afterSegment)
			case "segment-computer":
				afterSegment.Actor.Key.ComputerID = asQAForeign
				if _, err := sqliteEnsureActorGeneration(tx, afterSegment.Actor); err != nil {
					t.Fatal(err)
				}
				beforeSnapshot, _ = asQAAudit(t, tx)
				delta, err = sqliteWriteSegmentLocal(tx, source.ComputerID, &segment, afterSegment)
			}
			bgQAValidation(t, err)
			if delta != 0 {
				t.Fatal("immutable refusal charged")
			}
			afterSnapshot, _ := asQAAudit(t, tx)
			if !reflect.DeepEqual(beforeSnapshot, afterSnapshot) {
				t.Fatal("immutable refusal mutated local rows")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			asQAReopen(t, f, m, snapshot, source)
		})
	}
}

func asQAPlan(t *testing.T, tx *sqliteio.Tx, sql, index string, values ...sqliteio.Value) {
	t.Helper()
	bgQAPlan(t, tx, sql, index, values...)
	s := interopPrepare(t, tx, "EXPLAIN QUERY PLAN "+sql, values...)
	for {
		present, err := s.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !present {
			break
		}
		detail, err := s.Text(3)
		if err != nil || strings.Contains(detail, "TEMP B-TREE") {
			t.Fatal("indexed child selector sorted history", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteActorSegmentLocalLargeOrderedDuplicateOpaqueChildrenAndIndexedAppend(t *testing.T) {
	h, source := asQALegacy(t)
	actor, segment := asQARows(t, source)
	a := source.Actors[actorKey(actor.Ref.Key)]
	s := source.Segments[segment.ID]
	id := a.UncertaintyIDs[0]
	a.UncertaintyIDs = []string{}
	s.EventReferences = []string{}
	for i := 0; i < 1024; i++ {
		a.UncertaintyIDs = append(a.UncertaintyIDs, id)
		switch i % 4 {
		case 0:
			s.EventReferences = append(s.EventReferences, "")
		case 1:
			s.EventReferences = append(s.EventReferences, "opaque\x00reference")
		default:
			s.EventReferences = append(s.EventReferences, "duplicate")
		}
	}
	source = bgQALegacyMarshalOracle(t, h.service, h.path, source, true)
	f, m, snapshot := asQASeed(t, source)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	key := actorKey(actor.Ref.Key)
	for _, descending := range []bool{false, true} {
		order := "ASC"
		suffix := ""
		if descending {
			order, suffix = "DESC", " LIMIT 1"
		}
		asQAPlan(t, tx, "SELECT actor_key,ordinal,uncertainty_id FROM actor_uncertainties WHERE actor_key=? ORDER BY ordinal "+order+suffix, "sqlite_autoindex_actor_uncertainties_1", sqliteio.Text(key))
		asQAPlan(t, tx, "SELECT segment_id,ordinal,event_reference FROM segment_events WHERE segment_id=? ORDER BY ordinal "+order+suffix, "sqlite_autoindex_segment_events_1", sqliteio.Text(segment.ID))
	}
	asQAPlan(t, tx, "SELECT "+asQAActorColumns+" FROM actors WHERE actor_key=?", "sqlite_autoindex_actors_1", sqliteio.Text(key))
	asQAPlan(t, tx, "SELECT "+asQASegmentColumns+" FROM segments WHERE segment_id=?", "sqlite_autoindex_segments_1", sqliteio.Text(segment.ID))
	if got, err := sqliteActorUncertaintyIDsLocal(tx, source.ComputerID, actor.Ref.Key); err != nil || got == nil || !reflect.DeepEqual(got, source.Actors[key].UncertaintyIDs) {
		t.Fatal("ordered duplicate uncertainty IDs lost", err)
	}
	if got, err := sqliteSegmentEventsLocal(tx, source.ComputerID, segment.ID); err != nil || got == nil || !reflect.DeepEqual(got, source.Segments[segment.ID].EventReferences) {
		t.Fatal("opaque duplicate/empty/NUL event references lost", err)
	}
	for _, which := range []string{"Actor", "segment"} {
		var next int64
		var err error
		if which == "Actor" {
			next, err = sqliteNextActorUncertaintyOrdinalLocal(tx, source.ComputerID, actor.Ref.Key)
		} else {
			next, err = sqliteNextSegmentEventOrdinalLocal(tx, source.ComputerID, segment.ID)
		}
		if err != nil || next != 1024 {
			t.Fatal("indexed next differs", err)
		}
		beforeSnapshot, _ := asQAAudit(t, tx)
		var delta int64
		if which == "Actor" {
			delta, err = sqliteAppendActorUncertaintyLocal(tx, source.ComputerID, actor.Ref.Key, next-1, id)
		} else {
			delta, err = sqliteAppendSegmentEventLocal(tx, source.ComputerID, segment.ID, next-1, "stale")
		}
		bgQACorrupt(t, err)
		if delta != 0 {
			t.Fatal("stale ordinal append charged")
		}
		afterSnapshot, _ := asQAAudit(t, tx)
		if !reflect.DeepEqual(beforeSnapshot, afterSnapshot) {
			t.Fatal("stale ordinal append mutated history")
		}
	}
	rawReference := "appended-" + string([]byte{0xff}) + "\x00opaque"
	d1, err := sqliteAppendActorUncertaintyLocal(tx, source.ComputerID, actor.Ref.Key, 1024, id)
	if err != nil || d1 != asQAChildCharge(t, key, id) {
		t.Fatal("single duplicate append charge differs", err)
	}
	d2, err := sqliteAppendSegmentEventLocal(tx, source.ComputerID, segment.ID, 1024, rawReference)
	if err != nil || d2 != asQAChildCharge(t, segment.ID, rawReference) {
		t.Fatal("opaque final-JSON append charge differs", err)
	}
	raw := asQAJSON(t, source)
	raw.Actors[key].UncertaintyIDs = append(raw.Actors[key].UncertaintyIDs, id)
	raw.Segments[segment.ID].EventReferences = append(raw.Segments[segment.ID].EventReferences, rawReference)
	canonical := bgQALegacyMarshalOracle(t, h.service, h.path, raw, true)
	if got, err := sqliteSegmentEventsLocal(tx, source.ComputerID, segment.ID); err != nil || !reflect.DeepEqual(got, canonical.Segments[segment.ID].EventReferences) || len(got) != 1025 {
		t.Fatal("append changed prior event ordinals or final repaired text", err)
	}
	if got, err := sqliteActorUncertaintyIDsLocal(tx, source.ComputerID, actor.Ref.Key); err != nil || !reflect.DeepEqual(got, canonical.Actors[key].UncertaintyIDs) || len(got) != 1025 {
		t.Fatal("append changed prior uncertainty ordinals", err)
	}
	if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, m) {
		t.Fatal("child helpers mutated public metadata", err)
	}
	next := metaQANext(t, m)
	next.Revision = bump(m.Revision)
	next.LogicalBytes += d1 + d2
	if err := sqliteUpdateMeta(tx, m, next); err != nil {
		t.Fatal(err)
	}
	newSnapshot, total := asQAAudit(t, tx)
	if total != next.LogicalBytes {
		t.Fatal("single composed metadata charge differs")
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	asQAReopen(t, f, next, newSnapshot, canonical)
	// Earlier gap belongs only to explicit list validation. Next inspects the
	// last row; scalar readers and scalar CAS remain independent of arrays.
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	interopDone(t, tx, "DELETE FROM actor_uncertainties WHERE actor_key=? AND ordinal=5", sqliteio.Text(key))
	interopDone(t, tx, "DELETE FROM segment_events WHERE segment_id=? AND ordinal=5", sqliteio.Text(segment.ID))
	if got, err := sqliteActorUncertaintyIDsLocal(tx, source.ComputerID, actor.Ref.Key); got != nil {
		t.Fatal("gap list leaked partial Actor IDs")
	} else {
		bgQACorrupt(t, err)
	}
	if got, err := sqliteSegmentEventsLocal(tx, source.ComputerID, segment.ID); got != nil {
		t.Fatal("gap list leaked partial event references")
	} else {
		bgQACorrupt(t, err)
	}
	if got, err := sqliteNextActorUncertaintyOrdinalLocal(tx, source.ComputerID, actor.Ref.Key); err != nil || got != 1025 {
		t.Fatal("Next scanned/repaired an earlier Actor gap", err)
	}
	if got, err := sqliteNextSegmentEventOrdinalLocal(tx, source.ComputerID, segment.ID); err != nil || got != 1025 {
		t.Fatal("Next scanned/repaired an earlier segment gap", err)
	}
	if got, found, err := sqliteReadActorLocal(tx, source.ComputerID, actor.Ref.Key); err != nil || !found || !reflect.DeepEqual(got, actor) {
		t.Fatal("scalar Actor read materialized gap array", err)
	}
	if got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, segment.ID); err != nil || !found || !reflect.DeepEqual(got, segment) {
		t.Fatal("scalar segment read materialized gap array", err)
	}
	afterActor := asQAJSON(t, actor)
	afterActor.Revision = bump(actor.Revision)
	if _, err := sqliteWriteActorLocal(tx, source.ComputerID, &actor, afterActor); err != nil {
		t.Fatal("scalar Actor CAS materialized gap array", err)
	}
	afterSegment := asQAJSON(t, segment)
	afterSegment.Confirmed = segment.Confirmed.Add(time.Second)
	if _, err := sqliteWriteSegmentLocal(tx, source.ComputerID, &segment, afterSegment); err != nil {
		t.Fatal("scalar segment CAS materialized gap array", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	asQAReopen(t, f, next, newSnapshot, canonical)
	_ = snapshot // Original snapshot belongs to the pre-append committed incarnation.
}

func TestSQLiteActorSegmentLocalChildOwnerOrdinalKindsExhaustionAndEmptyControls(t *testing.T) {
	_, source := asQALegacy(t)
	actor, segment := asQARows(t, source)
	f, m, snapshot := asQASeed(t, source)
	for _, which := range []string{"Actor", "segment"} {
		for _, defect := range []string{"empty", "negative", "text-kind", "blob-kind", "reference-kind", "invalid-utf8", "exhaustion"} {
			t.Run(which+"/"+defect, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				table, columns, ownerColumn, owner, reference := "actor_uncertainties", "actor_key,ordinal,uncertainty_id", "actor_key", actorKey(actor.Ref.Key), source.Actors[actorKey(actor.Ref.Key)].UncertaintyIDs[0]
				if which == "segment" {
					table, columns, ownerColumn, owner, reference = "segment_events", "segment_id,ordinal,event_reference", "segment_id", segment.ID, "opaque"
				}
				asQAShadow(t, tx, table, columns)
				interopDone(t, tx, "DELETE FROM "+table+" WHERE "+ownerColumn+"=?", sqliteio.Text(owner))
				ordinal, refValue := sqliteio.Integer(0), sqliteio.Text(reference)
				switch defect {
				case "negative":
					ordinal = sqliteio.Integer(-1)
				case "text-kind":
					ordinal = sqliteio.Text("0")
				case "blob-kind":
					ordinal = sqliteio.Blob([]byte{0})
				case "reference-kind":
					refValue = sqliteio.Blob([]byte(reference))
				case "invalid-utf8":
					refValue = sqliteio.Text(string([]byte{0xff}))
				case "exhaustion":
					ordinal = sqliteio.Integer(math.MaxInt64)
				}
				if defect != "empty" {
					asQABindFixture(t, tx, table, columns, []sqliteio.Value{sqliteio.Text(owner), ordinal, refValue})
				}
				var refs []string
				var next int64
				var listErr, nextErr error
				if which == "Actor" {
					refs, listErr = sqliteActorUncertaintyIDsLocal(tx, source.ComputerID, actor.Ref.Key)
					next, nextErr = sqliteNextActorUncertaintyOrdinalLocal(tx, source.ComputerID, actor.Ref.Key)
				} else {
					refs, listErr = sqliteSegmentEventsLocal(tx, source.ComputerID, segment.ID)
					next, nextErr = sqliteNextSegmentEventOrdinalLocal(tx, source.ComputerID, segment.ID)
				}
				if defect == "empty" {
					if listErr != nil || refs == nil || len(refs) != 0 || nextErr != nil || next != 0 {
						t.Fatal("successful empty child owner conflated with absence", listErr, nextErr)
					}
				} else {
					bgQACorrupt(t, listErr)
					if refs != nil || next != 0 {
						t.Fatal("bad child kind/ordinal leaked result")
					}
					if defect == "exhaustion" {
						bgQAValidation(t, nextErr)
					} else {
						bgQACorrupt(t, nextErr)
					}
				}
				beforeSnapshot, _ := asQAAudit(t, tx)
				var delta int64
				var err error
				if which == "Actor" {
					delta, err = sqliteAppendActorUncertaintyLocal(tx, source.ComputerID, actor.Ref.Key, -1, reference)
				} else {
					delta, err = sqliteAppendSegmentEventLocal(tx, source.ComputerID, segment.ID, -1, reference)
				}
				bgQAValidation(t, err)
				if delta != 0 {
					t.Fatal("negative explicit ordinal charged")
				}
				if defect == "exhaustion" {
					if which == "Actor" {
						delta, err = sqliteAppendActorUncertaintyLocal(tx, source.ComputerID, actor.Ref.Key, math.MaxInt64, reference)
					} else {
						delta, err = sqliteAppendSegmentEventLocal(tx, source.ComputerID, segment.ID, math.MaxInt64, reference)
					}
					bgQAValidation(t, err)
					if delta != 0 {
						t.Fatal("ordinal exhaustion wrapped or charged")
					}
				}
				afterSnapshot, _ := asQAAudit(t, tx)
				if !reflect.DeepEqual(beforeSnapshot, afterSnapshot) {
					t.Fatal("ordinal refusal mutated children")
				}
				interopRollback(t, tx)
				interopClose(t, c)
				asQAReopen(t, f, m, snapshot, source)
			})
		}
	}
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	missing := actor.Ref.Key
	missing.AgentID = "missing-owner"
	if refs, err := sqliteActorUncertaintyIDsLocal(tx, source.ComputerID, missing); refs != nil {
		t.Fatal("missing Actor returned list")
	} else {
		bgQACorrupt(t, err)
	}
	if next, err := sqliteNextActorUncertaintyOrdinalLocal(tx, source.ComputerID, missing); next != 0 {
		t.Fatal("missing Actor returned ordinal")
	} else {
		bgQACorrupt(t, err)
	}
	if refs, err := sqliteSegmentEventsLocal(tx, source.ComputerID, qaRecoveryRequest); refs != nil {
		t.Fatal("missing segment returned list")
	} else {
		bgQACorrupt(t, err)
	}
	if next, err := sqliteNextSegmentEventOrdinalLocal(tx, source.ComputerID, qaRecoveryRequest); next != 0 {
		t.Fatal("missing segment returned ordinal")
	} else {
		bgQACorrupt(t, err)
	}
	if delta, err := sqliteAppendActorUncertaintyLocal(tx, source.ComputerID, missing, 0, source.Actors[actorKey(actor.Ref.Key)].UncertaintyIDs[0]); delta != 0 {
		t.Fatal("missing Actor owner append charged")
	} else {
		bgQACorrupt(t, err)
	}
	if delta, err := sqliteAppendSegmentEventLocal(tx, source.ComputerID, qaRecoveryRequest, 0, "opaque"); delta != 0 {
		t.Fatal("missing segment owner append charged")
	} else {
		bgQACorrupt(t, err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteActorSegmentLocalLaterChildFailureClearsAccumulatedResults(t *testing.T) {
	_, source := asQALegacy(t)
	actor, segment := asQARows(t, source)
	f, m, snapshot := asQASeed(t, source)
	for _, which := range []string{"Actor", "segment"} {
		t.Run(which, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			table, columns, ownerColumn, owner := "actor_uncertainties", "actor_key,ordinal,uncertainty_id", "actor_key", actorKey(actor.Ref.Key)
			if which == "segment" {
				table, columns, ownerColumn, owner = "segment_events", "segment_id,ordinal,event_reference", "segment_id", segment.ID
			}
			var next int64
			var err error
			if which == "Actor" {
				refs, err := sqliteActorUncertaintyIDsLocal(tx, source.ComputerID, actor.Ref.Key)
				if err != nil || refs == nil || len(refs) == 0 {
					t.Fatal("nonempty valid Actor prefix absent", err)
				}
				next, err = sqliteNextActorUncertaintyOrdinalLocal(tx, source.ComputerID, actor.Ref.Key)
			} else {
				refs, err := sqliteSegmentEventsLocal(tx, source.ComputerID, segment.ID)
				if err != nil || refs == nil || len(refs) == 0 {
					t.Fatal("nonempty valid segment prefix absent", err)
				}
				next, err = sqliteNextSegmentEventOrdinalLocal(tx, source.ComputerID, segment.ID)
			}
			if err != nil || next == 0 {
				t.Fatal("valid ordered last ordinal absent", err)
			}
			asQAShadow(t, tx, table, columns)
			asQABindFixture(t, tx, table, columns, []sqliteio.Value{sqliteio.Text(owner), sqliteio.Integer(next), sqliteio.Text(string([]byte{0xff}))})
			// Observe the native valid prefix then malformed last reference using the
			// exact contract projection/order, independently of public result order.
			s := interopPrepare(t, tx, "SELECT "+columns+" FROM "+table+" WHERE "+ownerColumn+"=? ORDER BY ordinal ASC", sqliteio.Text(owner))
			var seen int64
			for {
				present, err := s.Step()
				if err != nil {
					t.Fatal(err)
				}
				if !present {
					break
				}
				ordinal, err := s.Int64(1)
				if err != nil || ordinal != seen {
					t.Fatal("native ordered prefix witness differs", err)
				}
				reference, err := s.Text(2)
				if err != nil {
					t.Fatal(err)
				}
				if seen < next && reference == string([]byte{0xff}) || seen == next && reference != string([]byte{0xff}) {
					t.Fatal("native later-error witness absent")
				}
				seen++
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if seen != next+1 {
				t.Fatal("native later error prefix count differs")
			}
			if which == "Actor" {
				refs, err := sqliteActorUncertaintyIDsLocal(tx, source.ComputerID, actor.Ref.Key)
				bgQACorrupt(t, err)
				if refs != nil {
					t.Fatal("later Actor error leaked accumulated IDs")
				}
				ordinal, err := sqliteNextActorUncertaintyOrdinalLocal(tx, source.ComputerID, actor.Ref.Key)
				bgQACorrupt(t, err)
				if ordinal != 0 {
					t.Fatal("bad selected last Actor edge returned next")
				}
			} else {
				refs, err := sqliteSegmentEventsLocal(tx, source.ComputerID, segment.ID)
				bgQACorrupt(t, err)
				if refs != nil {
					t.Fatal("later segment error leaked accumulated references")
				}
				ordinal, err := sqliteNextSegmentEventOrdinalLocal(tx, source.ComputerID, segment.ID)
				bgQACorrupt(t, err)
				if ordinal != 0 {
					t.Fatal("bad selected last event returned next")
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			asQAReopen(t, f, m, snapshot, source)
		})
	}
}

func TestSQLiteActorSegmentLocalSelectedKindsNullGroupsAndSemanticCorruption(t *testing.T) {
	_, source := asQALegacy(t)
	actor, segment := asQARows(t, source)
	f, m, snapshot := asQASeed(t, source)
	for _, family := range []struct{ table, columns, key, identity string }{{"actors", asQAActorColumns, "actor_key", actorKey(actor.Ref.Key)}, {"segments", asQASegmentColumns, "segment_id", segment.ID}} {
		// A wrong-kind identity no longer matches the exact TEXT lookup. Do
		// not claim that absence exercises a selected decoder failure.
		for i, column := range strings.Split(family.columns, ",") {
			if i == 0 {
				continue
			}
			t.Run(family.table+"/kind/"+column, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				if family.table == "actors" {
					if got, found, err := sqliteReadActorLocal(tx, source.ComputerID, actor.Ref.Key); err != nil || !found || !reflect.DeepEqual(got, actor) {
						t.Fatal("Actor kind baseline invalid", err)
					}
				} else {
					if got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, segment.ID); err != nil || !found || !reflect.DeepEqual(got, segment) {
						t.Fatal("segment kind baseline invalid", err)
					}
				}
				asQAShadow(t, tx, family.table, family.columns)
				// Read the real baseline kind before substituting an incompatible kind.
				s := interopPrepare(t, tx, "SELECT "+family.columns+" FROM "+family.table+" WHERE "+family.key+"=?", sqliteio.Text(family.identity))
				if present, err := s.Step(); err != nil || !present {
					t.Fatal(err)
				}
				kind, err := s.Kind(i)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				bad := sqliteio.Text("wrong-kind")
				if kind == sqliteio.TextKind || kind == sqliteio.NullKind {
					bad = sqliteio.Blob([]byte{1})
				}
				interopDone(t, tx, "UPDATE "+family.table+" SET "+column+"=? WHERE "+family.key+"=?", bad, sqliteio.Text(family.identity))
				if family.table == "actors" {
					got, found, err := sqliteReadActorLocal(tx, source.ComputerID, actor.Ref.Key)
					bgQACorrupt(t, err)
					if found || !reflect.DeepEqual(got, sqliteActorLocalRow{}) {
						t.Fatal("bad Actor kind leaked projection")
					}
				} else {
					got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, segment.ID)
					bgQACorrupt(t, err)
					if found || !reflect.DeepEqual(got, sqliteSegmentLocalRow{}) {
						t.Fatal("bad segment kind leaked projection")
					}
				}
				interopRollback(t, tx)
				interopClose(t, c)
				asQAReopen(t, f, m, snapshot, source)
			})
		}
	}
	for _, defect := range []string{"Actor-parent-key-null", "Actor-parent-generation-null", "Actor-zero-revision", "Actor-short-generation", "Actor-state", "Actor-health", "Actor-segment-null", "Actor-key", "Actor-wall", "Actor-epoch", "Actor-elapsed", "Actor-awake", "Actor-utf8", "segment-end-sec", "segment-end-nsec", "segment-end-json", "segment-zero-generation", "segment-group-order", "segment-wall", "segment-raw-projection", "segment-finalized", "segment-utf8", "missing-actor-generation", "missing-parent-generation"} {
		t.Run(defect, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			table, columns, key, identity := "actors", asQAActorColumns, "actor_key", actorKey(actor.Ref.Key)
			if strings.HasPrefix(defect, "segment-") {
				table, columns, key, identity = "segments", asQASegmentColumns, "segment_id", segment.ID
			}
			asQAShadow(t, tx, table, columns)
			column, bad := "state", sqliteio.Text("bad")
			switch defect {
			case "Actor-parent-key-null":
				column, bad = "parent_key", sqliteio.Null()
			case "Actor-parent-generation-null":
				column, bad = "parent_generation", sqliteio.Null()
			case "Actor-zero-revision":
				column, bad = "revision", interopCounter(t, "0")
			case "Actor-short-generation":
				column, bad = "generation", sqliteio.Blob([]byte{1})
			case "Actor-state":
				column, bad = "state", sqliteio.Text("unknown")
			case "Actor-health":
				column, bad = "health", sqliteio.Text("unknown")
			case "Actor-segment-null":
				column, bad = "segment_id", sqliteio.Null()
			case "Actor-key":
				column, bad = "actor_key", sqliteio.Text("unparseable-private-key")
			case "Actor-wall":
				column, bad = "last_evidence_wall_nsec", sqliteio.Integer(1000000000)
			case "Actor-epoch":
				column, bad = "last_evidence_epoch", sqliteio.Text("")
			case "Actor-elapsed":
				column, bad = "last_evidence_elapsed", interopCounter(t, "0")
			case "Actor-awake":
				column, bad = "last_evidence_awake", interopCounter(t, "0")
			case "Actor-utf8":
				column, bad = "timezone", sqliteio.Text(string([]byte{0xff}))
			case "segment-end-sec":
				column, bad = "end_sec", sqliteio.Integer(segment.Confirmed.Unix())
			case "segment-end-nsec":
				column, bad = "end_nsec", sqliteio.Integer(0)
			case "segment-end-json":
				column, bad = "end_json", sqliteio.Text("null")
			case "segment-zero-generation":
				column, bad = "actor_generation", interopCounter(t, "0")
			case "segment-group-order":
				column, bad = "group_order", sqliteio.Text("different-valid-order")
			case "segment-wall":
				column, bad = "confirmed_json", sqliteio.Text("\"2000-01-01T00:00:00Z\"")
			case "segment-raw-projection":
				column, bad = "start_sample_elapsed", interopCounter(t, "0")
			case "segment-finalized":
				column, bad = "finalized", sqliteio.Integer(2)
			case "segment-utf8":
				column, bad = "group_order", sqliteio.Text(string([]byte{0xff}))
			case "missing-actor-generation":
				interopDone(t, tx, "DELETE FROM actor_generations WHERE actor_key=? AND generation=?", sqliteio.Text(actorKey(actor.Ref.Key)), interopCounter(t, actor.Ref.Generation))
			case "missing-parent-generation":
				interopDone(t, tx, "DELETE FROM actor_generations WHERE actor_key=? AND generation=?", sqliteio.Text(actorKey(actor.Parent.Key)), interopCounter(t, actor.Parent.Generation))
			}
			if !strings.HasPrefix(defect, "missing-") {
				interopDone(t, tx, "UPDATE "+table+" SET "+column+"=? WHERE "+key+"=?", bad, sqliteio.Text(identity))
			}
			queryKey := actor.Ref.Key
			if defect == "Actor-key" {
				queryKey.AgentID = "absent-request-key"
				interopDone(t, tx, "UPDATE actors SET actor_key=?", sqliteio.Text(actorKey(queryKey)))
			}
			if table == "actors" {
				got, found, err := sqliteReadActorLocal(tx, source.ComputerID, queryKey)
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(got, sqliteActorLocalRow{}) {
					t.Fatal("semantic Actor corruption leaked row")
				}
			} else {
				got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, segment.ID)
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(got, sqliteSegmentLocalRow{}) {
					t.Fatal("semantic segment corruption leaked row")
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			asQAReopen(t, f, m, snapshot, source)
		})
	}
	// A real valid foreign scalar is readable in its own scope, then corruption
	// when selected by the same identity using this fixture's computer.
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	foreign := asQAJSON(t, actor)
	foreign.ID = qaRecoveryRequest
	foreign.Ref.Key.ComputerID = asQAForeign
	asQAEnsureActor(t, tx, foreign)
	if _, err := sqliteWriteActorLocal(tx, asQAForeign, nil, foreign); err != nil {
		t.Fatal("foreign Actor control failed", err)
	}
	if got, found, err := sqliteReadActorLocal(tx, asQAForeign, foreign.Ref.Key); err != nil || !found || !reflect.DeepEqual(got, foreign) {
		t.Fatal("foreign Actor not locally valid", err)
	}
	if got, found, err := sqliteReadActorLocal(tx, source.ComputerID, foreign.Ref.Key); found || !reflect.DeepEqual(got, sqliteActorLocalRow{}) {
		t.Fatal("foreign selected Actor leaked")
	} else {
		bgQACorrupt(t, err)
	}
	foreignSegment := asQAJSON(t, segment)
	foreignSegment.ID = qaRecoveryRequest
	foreignSegment.Actor = foreign.Ref
	if _, err := sqliteWriteSegmentLocal(tx, asQAForeign, nil, foreignSegment); err != nil {
		t.Fatal("foreign segment control failed", err)
	}
	if got, found, err := sqliteReadSegmentLocal(tx, asQAForeign, foreignSegment.ID); err != nil || !found || !reflect.DeepEqual(got, foreignSegment) {
		t.Fatal("foreign segment not locally valid", err)
	}
	if got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, foreignSegment.ID); found || !reflect.DeepEqual(got, sqliteSegmentLocalRow{}) {
		t.Fatal("foreign selected segment leaked")
	} else {
		bgQACorrupt(t, err)
	}
	asQAShadow(t, tx, "actors", asQAActorColumns)
	asQAShadow(t, tx, "segments", asQASegmentColumns)
	interopDone(t, tx, "UPDATE actors SET health='unknown' WHERE actor_key=?", sqliteio.Text(actorKey(foreign.Ref.Key)))
	interopDone(t, tx, "UPDATE segments SET group_order='unrelated-corruption' WHERE segment_id=?", sqliteio.Text(foreignSegment.ID))
	if got, found, err := sqliteReadActorLocal(tx, source.ComputerID, actor.Ref.Key); err != nil || !found || !reflect.DeepEqual(got, actor) {
		t.Fatal("Actor reader scanned unrelated corrupt head", err)
	}
	if got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, segment.ID); err != nil || !found || !reflect.DeepEqual(got, segment) {
		t.Fatal("segment reader scanned unrelated corrupt row", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	asQAReopen(t, f, m, snapshot, source)
}

func TestSQLiteActorSegmentLocalRawJSONIdentityOwnershipFinalRefAndClockPolicy(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("raw-original", "1", "1", "work", qaBindingA))
	baseline := bgQAReadLegacy(t, h.service)
	raw := asQAJSON(t, baseline)
	var a *Actor
	for _, value := range raw.Actors {
		a = value
	}
	a.Ref.Key.SessionID = "raw-session-" + string([]byte{0xff})
	a.Ref.Key.AgentID = "raw-agent-" + string([]byte{0xfe})
	raw.Actors = map[string]*Actor{actorKey(a.Ref.Key): a}
	for _, s := range raw.Segments {
		s.Actor = a.Ref
	}
	for key, r := range raw.Receipts {
		r.Result.Actor = a.Ref
		raw.Receipts[key] = r
	}
	rawActor := asQAActor(a)
	rawSegment := asQASegment(raw.Segments[*a.SegmentID])
	canonicalActor := asQAJSON(t, rawActor)
	canonicalSegment := asQAJSON(t, rawSegment)
	stable := actorKey(rawActor.Ref.Key) == actorKey(canonicalActor.Ref.Key)
	decoded := bgQALegacyMarshalOracle(t, h.service, h.path, raw, stable)
	if stable && (!validState(decoded) || canonicalActor.Ref.Key == rawActor.Ref.Key) {
		t.Fatal("actual stable JSONv2 identity repair witness absent")
	}
	f, m, snapshot := asQASeed(t, baseline)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	// Retain original caller bytes outside JSON cloning, which would repair them.
	actorOriginal := rawActor
	actorOriginal.LastEvidence = asQAJSON(t, rawActor.LastEvidence)
	segmentOriginal := rawSegment
	segmentOriginal.StartSample = asQAJSON(t, rawSegment.StartSample)
	segmentOriginal.ConfirmedSample = asQAJSON(t, rawSegment.ConfirmedSample)
	rawActor.ID = qaRecoveryRequest
	actorOriginal.ID = rawActor.ID
	rawSegment.ID = qaRecoveryRequest
	segmentOriginal.ID = rawSegment.ID
	rawActor.SegmentID = asQAPointer(rawSegment.ID)
	actorOriginal.SegmentID = asQAPointer(rawSegment.ID)
	if stable {
		if _, err := sqliteEnsureActorGeneration(tx, rawActor.Ref); err != nil {
			t.Fatal(err)
		}
		if delta, err := sqliteWriteSegmentLocal(tx, baseline.ComputerID, nil, rawSegment); err != nil || delta != asQASegmentCharge(t, rawSegment) {
			t.Fatal("stable raw segment identity first INSERT failed", err)
		}
		if delta, err := sqliteWriteActorLocal(tx, baseline.ComputerID, nil, rawActor); err != nil || delta != asQAActorCharge(t, rawActor) {
			t.Fatal("stable raw Actor identity first INSERT failed", err)
		}
		wantActor := asQAJSON(t, rawActor)
		wantSegment := asQAJSON(t, rawSegment)
		for _, key := range []ActorKey{rawActor.Ref.Key, wantActor.Ref.Key} {
			if got, found, err := sqliteReadActorLocal(tx, baseline.ComputerID, key); err != nil || !found || !reflect.DeepEqual(got, wantActor) {
				t.Fatal("raw/repeated/canonical Actor read differs", err)
			}
		}
		if got, found, err := sqliteReadSegmentLocal(tx, baseline.ComputerID, rawSegment.ID); err != nil || !found || !reflect.DeepEqual(got, wantSegment) {
			t.Fatal("stable raw segment read differs", err)
		}
		if delta, err := sqliteWriteActorLocal(tx, baseline.ComputerID, &rawActor, rawActor); err != nil || delta != 0 {
			t.Fatal("stable raw Actor repeated full-old CAS failed", err)
		}
		if delta, err := sqliteWriteSegmentLocal(tx, baseline.ComputerID, &rawSegment, rawSegment); err != nil || delta != 0 {
			t.Fatal("stable raw segment repeated full-old CAS failed", err)
		}
	} else {
		// Under an older encoder, real raw map-key drift is refused as specified.
		if delta, err := sqliteWriteActorLocal(tx, baseline.ComputerID, nil, rawActor); delta != 0 {
			t.Fatal("drifting Actor charged")
		} else {
			bgQAValidation(t, err)
		}
		if delta, err := sqliteWriteSegmentLocal(tx, baseline.ComputerID, nil, rawSegment); delta != 0 {
			t.Fatal("drifting segment charged")
		} else {
			bgQAValidation(t, err)
		}
	}
	if !reflect.DeepEqual(rawActor, actorOriginal) || !reflect.DeepEqual(rawSegment, segmentOriginal) {
		t.Fatal("identity encoding rewrote raw caller Ref")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	asQAReopen(t, f, m, snapshot, baseline)
	// Ref raw shape can fit256 yet become invalid after actual JSON repair.
	finalRaw := asQAJSON(t, baseline)
	var finalActor *Actor
	for _, value := range finalRaw.Actors {
		finalActor = value
	}
	finalActor.Ref.Key.SessionID = strings.Repeat("x", 255) + string([]byte{0xff})
	finalRaw.Actors = map[string]*Actor{actorKey(finalActor.Ref.Key): finalActor}
	for _, s := range finalRaw.Segments {
		s.Actor = finalActor.Ref
	}
	for key, receipt := range finalRaw.Receipts {
		receipt.Result.Actor = finalActor.Ref
		finalRaw.Receipts[key] = receipt
	}
	finalDecoded := bgQALegacyMarshalOracle(t, h.service, h.path, finalRaw, false)
	if validState(finalDecoded) {
		t.Fatal("actual strict legacy final Ref refusal absent")
	}
	bad := asQAActor(finalActor)
	bad.ID = qaRecoveryRequest
	if !validRef(bad.Ref) || validRef(asQAJSON(t, bad.Ref)) {
		t.Fatal("raw-valid final-invalid Ref witness absent")
	}
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	beforeSnapshot, _ := asQAAudit(t, tx)
	if charge, err := sqliteActorLocalCharge(bad); charge != 0 {
		t.Fatal("final-invalid Ref pure charge nonzero")
	} else {
		bgQAValidation(t, err)
	}
	if delta, err := sqliteWriteActorLocal(tx, baseline.ComputerID, nil, bad); delta != 0 {
		t.Fatal("final-invalid Ref INSERT charged")
	} else {
		bgQAValidation(t, err)
	}
	badSegment := canonicalSegment
	badSegment.ID, badSegment.Actor = qaRecoveryRequest, bad.Ref
	if charge, err := sqliteSegmentLocalCharge(badSegment); charge != 0 {
		t.Fatal("final-invalid segment Ref charge nonzero")
	} else {
		bgQAValidation(t, err)
	}
	if delta, err := sqliteWriteSegmentLocal(tx, baseline.ComputerID, nil, badSegment); delta != 0 {
		t.Fatal("final-invalid segment Ref INSERT charged")
	} else {
		bgQAValidation(t, err)
	}
	afterSnapshot, _ := asQAAudit(t, tx)
	if !reflect.DeepEqual(beforeSnapshot, afterSnapshot) {
		t.Fatal("final Ref refusal mutated local rows")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	asQAReopen(t, f, m, snapshot, baseline)
	// Preserve the approved raw clock admission/final repair/read-corruption edge.
	clockRaw := asQAJSON(t, baseline)
	var clockActor *Actor
	for _, value := range clockRaw.Actors {
		clockActor = value
	}
	expandingEpoch := strings.Repeat("x", 255) + string([]byte{0xff})
	clockActor.LastEvidence.Epoch = &expandingEpoch
	clockDecoded := bgQALegacyMarshalOracle(t, h.service, h.path, clockRaw, false)
	if len(*clockDecoded.Actors[actorKey(clockActor.Ref.Key)].LastEvidence.Epoch) != 258 {
		t.Fatal("actual clock expansion edge absent")
	}
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	old := asQAActor(baseline.Actors[actorKey(clockActor.Ref.Key)])
	after := asQAActor(clockActor)
	if charge, err := sqliteActorLocalCharge(after); err != nil || charge != asQAActorCharge(t, after) {
		t.Fatal("approved raw clock encoding admission changed", err)
	}
	if delta, err := sqliteWriteActorLocal(tx, baseline.ComputerID, &old, after); err != nil || delta != asQAActorCharge(t, after)-asQAActorCharge(t, old) {
		t.Fatal("raw clock expansion must retain existing write policy", err)
	}
	if got, found, err := sqliteReadActorLocal(tx, baseline.ComputerID, after.Ref.Key); found || !reflect.DeepEqual(got, sqliteActorLocalRow{}) {
		t.Fatal("invalid final clock returned usable row")
	} else {
		bgQACorrupt(t, err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	asQAReopen(t, f, m, snapshot, baseline)
	// The same clock encoding policy applies to both segment sample projections.
	segmentClockRaw := asQAJSON(t, baseline)
	for _, ep := range segmentClockRaw.Epochs {
		ep.Anchor.Epoch = &expandingEpoch
	}
	for _, a := range segmentClockRaw.Actors {
		a.LastEvidence.Epoch = &expandingEpoch
	}
	for _, s := range segmentClockRaw.Segments {
		s.StartSample.Epoch, s.ConfirmedSample.Epoch = &expandingEpoch, &expandingEpoch
	}
	_ = bgQALegacyMarshalOracle(t, h.service, h.path, segmentClockRaw, false)
	for _, which := range []string{"start", "confirmed"} {
		c, tx = interopOpen(t, f, false, sqliteio.Write)
		var original *segment
		for _, s := range baseline.Segments {
			original = s
			break
		}
		oldSegment := asQASegment(original)
		proposed := asQAJSON(t, oldSegment)
		if which == "start" {
			proposed.StartSample.Epoch = &expandingEpoch
		} else {
			proposed.ConfirmedSample.Epoch = &expandingEpoch
		}
		if charge, err := sqliteSegmentLocalCharge(proposed); err != nil || charge != asQASegmentCharge(t, proposed) {
			t.Fatal("segment raw clock pure admission changed", err)
		}
		if delta, err := sqliteWriteSegmentLocal(tx, baseline.ComputerID, &oldSegment, proposed); err != nil || delta != asQASegmentCharge(t, proposed)-asQASegmentCharge(t, oldSegment) {
			t.Fatal("segment raw clock write policy changed", err)
		}
		if got, found, err := sqliteReadSegmentLocal(tx, baseline.ComputerID, proposed.ID); found || !reflect.DeepEqual(got, sqliteSegmentLocalRow{}) {
			t.Fatal("expanded segment clock returned usable projection")
		} else {
			bgQACorrupt(t, err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
		asQAReopen(t, f, m, snapshot, baseline)
	}
}

func TestSQLiteActorSegmentLocalInputValidationUnencodableTimeAndUnsignedCounters(t *testing.T) {
	h, source := asQALegacy(t)
	actor, segment := asQARows(t, source)
	f, m, snapshot := asQASeed(t, source)
	for _, value := range []string{"9223372036854775808", asQAMax} {
		t.Run(value, func(t *testing.T) {
			// Import counters are admitted from a complete strict legacy graph with
			// final metadata inserted once, separately from ordinary mutable CAS.
			high := asQAJSON(t, source)
			high.Revision = value
			head := high.Actors[actorKey(actor.Ref.Key)]
			head.Revision, head.Sequence, head.BindingRevision, head.Ref.Generation = value, value, value, value
			high.Segments[segment.ID].Actor.Generation, high.Segments[segment.ID].Binding.Revision = value, value
			high = bgQALegacyMarshalOracle(t, h.service, h.path, high, true)
			highF, highM, highSnapshot := asQASeed(t, high)
			if highM.Revision != value {
				t.Fatal("fresh metadata import truncated unsigned counter")
			}
			asQAReopen(t, highF, highM, highSnapshot, high)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			proposal := asQAJSON(t, actor)
			proposal.Revision, proposal.Sequence, proposal.BindingRevision, proposal.Ref.Generation = value, value, value, value
			asQAEnsureActor(t, tx, proposal)
			if delta, err := sqliteWriteActorLocal(tx, source.ComputerID, &actor, proposal); err != nil || delta != asQAActorCharge(t, proposal)-asQAActorCharge(t, actor) {
				t.Fatal("full unsigned Actor counters truncated/refused", err)
			}
			if got, found, err := sqliteReadActorLocal(tx, source.ComputerID, actor.Ref.Key); err != nil || !found || !reflect.DeepEqual(got, proposal) {
				t.Fatal("unsigned Actor counters did not roundtrip", err)
			}
			seg := asQAJSON(t, segment)
			seg.Actor.Generation, seg.Binding.Revision = value, value
			if delta, err := sqliteWriteSegmentLocal(tx, source.ComputerID, &segment, seg); err != nil || delta != asQASegmentCharge(t, seg)-asQASegmentCharge(t, segment) {
				t.Fatal("unsigned segment history refused", err)
			}
			if got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, seg.ID); err != nil || !found || !reflect.DeepEqual(got, seg) {
				t.Fatal("unsigned segment history roundtrip differs", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			asQAReopen(t, f, m, snapshot, source)
		})
	}
	for _, field := range []string{"Actor-wall", "segment-start", "segment-confirmed", "segment-end", "segment-start-sample", "segment-confirmed-sample"} {
		t.Run("unencodable/"+field, func(t *testing.T) {
			badTime := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			if _, err := badTime.MarshalJSON(); err == nil {
				t.Fatal("time witness actually encodes")
			}
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			beforeSnapshot, _ := asQAAudit(t, tx)
			afterActor, afterSegment := asQAJSON(t, actor), asQAJSON(t, segment)
			var charge, delta int64
			var chargeErr, writeErr error
			switch field {
			case "Actor-wall":
				afterActor.LastEvidence.WallUTC = badTime
			case "segment-start":
				afterSegment.Start, afterSegment.Confirmed = badTime, badTime
			case "segment-confirmed":
				afterSegment.Confirmed = badTime
			case "segment-end":
				afterSegment.End = &badTime
			case "segment-start-sample":
				afterSegment.StartSample.WallUTC = badTime
			case "segment-confirmed-sample":
				afterSegment.ConfirmedSample.WallUTC = badTime
			}
			if field == "Actor-wall" {
				charge, chargeErr = sqliteActorLocalCharge(afterActor)
				delta, writeErr = sqliteWriteActorLocal(tx, source.ComputerID, &actor, afterActor)
			} else {
				charge, chargeErr = sqliteSegmentLocalCharge(afterSegment)
				delta, writeErr = sqliteWriteSegmentLocal(tx, source.ComputerID, &segment, afterSegment)
			}
			bgQAValidation(t, chargeErr)
			bgQAValidation(t, writeErr)
			if charge != 0 || delta != 0 {
				t.Fatal("unencodable time returned charge")
			}
			afterSnapshot, _ := asQAAudit(t, tx)
			if !reflect.DeepEqual(beforeSnapshot, afterSnapshot) {
				t.Fatal("unencodable time mutated rows")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			asQAReopen(t, f, m, snapshot, source)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	for _, state := range []string{"working", "wait_user", "wait_permission", "wait_children", "interrupted", "finished"} {
		for _, health := range []string{"continuous", "stale", "order_blocked"} {
			after := asQAJSON(t, actor)
			after.State, after.Health = state, health
			if state != "working" {
				after.SegmentID = nil
			}
			if _, err := sqliteActorLocalCharge(after); err != nil {
				t.Fatal("valid enum control failed", err)
			}
			if _, err := sqliteWriteActorLocal(tx, source.ComputerID, &actor, after); err != nil {
				t.Fatal("valid enum UPDATE failed", err)
			}
			if _, err := sqliteWriteActorLocal(tx, source.ComputerID, &after, actor); err != nil {
				t.Fatal("valid enum restore failed", err)
			}
		}
	}
	for _, defect := range []string{"id", "revision-zero", "revision-leading-zero", "sequence-zero", "binding-revision-zero", "state", "health", "working-nil", "waiting-present", "clock-unavailable", "parent-invalid"} {
		after := asQAJSON(t, actor)
		switch defect {
		case "id":
			after.ID = "bad"
		case "revision-zero":
			after.Revision = "0"
		case "revision-leading-zero":
			after.Revision = "01"
		case "sequence-zero":
			after.Sequence = "0"
		case "binding-revision-zero":
			after.BindingRevision = "0"
		case "state":
			after.State = "bad"
		case "health":
			after.Health = "bad"
		case "working-nil":
			after.SegmentID = nil
		case "waiting-present":
			after.State = "wait_user"
		case "clock-unavailable":
			after.LastEvidence.Capability = "unavailable"
		case "parent-invalid":
			after.Parent.Generation = "0"
		}
		if charge, err := sqliteActorLocalCharge(after); charge != 0 {
			t.Fatal("invalid Actor pure charge nonzero", defect)
		} else {
			bgQAValidation(t, err)
		}
		if delta, err := sqliteWriteActorLocal(tx, source.ComputerID, &actor, after); delta != 0 {
			t.Fatal("invalid Actor write charged", defect)
		} else {
			bgQAValidation(t, err)
		}
	}
	interopRollback(t, tx)
	interopClose(t, c)
	asQAReopen(t, f, m, snapshot, source)
	for _, defect := range []string{"id", "actor-ref", "binding-revision", "epoch-id", "start-sample", "confirmed-sample", "confirmed-before-start", "uncertainty-id"} {
		c, tx = interopOpen(t, f, false, sqliteio.Write)
		after := asQAJSON(t, segment)
		switch defect {
		case "id":
			after.ID = "bad"
		case "actor-ref":
			after.Actor.Generation = "0"
		case "binding-revision":
			after.Binding.Revision = "0"
		case "epoch-id":
			after.EpochID = "bad"
		case "start-sample":
			after.StartSample.Capability = "unavailable"
		case "confirmed-sample":
			after.ConfirmedSample.AwakeNS = asQAPointer("9223372036854775808")
		case "confirmed-before-start":
			after.Confirmed = after.Start.Add(-time.Second)
		case "uncertainty-id":
			after.UncertaintyID = asQAPointer("bad")
		}
		if charge, err := sqliteSegmentLocalCharge(after); charge != 0 {
			t.Fatal("invalid segment pure charge nonzero", defect)
		} else {
			bgQAValidation(t, err)
		}
		if delta, err := sqliteWriteSegmentLocal(tx, source.ComputerID, &segment, after); delta != 0 {
			t.Fatal("invalid segment write charged", defect)
		} else {
			bgQAValidation(t, err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
		asQAReopen(t, f, m, snapshot, source)
	}
}

func TestSQLiteActorSegmentLocalParentRawIdentityPolicyAndExplicitValidation(t *testing.T) {
	h, source := asQALegacy(t)
	actor, segment := asQARows(t, source)
	f, m, snapshot := asQASeed(t, source)
	raw := asQAJSON(t, source)
	head := raw.Actors[actorKey(actor.Ref.Key)]
	head.Parent.Key.SessionID = "raw-parent-" + string([]byte{0xff})
	parentRaw := *head.Parent
	parentCanonical := asQAJSON(t, parentRaw)
	stable := actorKey(parentRaw.Key) == actorKey(parentCanonical.Key)
	_ = bgQALegacyMarshalOracle(t, h.service, h.path, raw, stable)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	if stable {
		if _, err := sqliteEnsureActorGeneration(tx, parentRaw); err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err := sqliteEnsureActorGeneration(tx, parentCanonical); err != nil {
			t.Fatal(err)
		}
	}
	proposal := asQAActor(head)
	original := proposal
	original.Parent = &parentRaw
	delta, err := sqliteWriteActorLocal(tx, source.ComputerID, &actor, proposal)
	if stable {
		if err != nil || delta != asQAActorCharge(t, proposal)-asQAActorCharge(t, actor) {
			t.Fatal("stable raw Parent CAS failed", err)
		}
		if got, found, err := sqliteReadActorLocal(tx, source.ComputerID, actor.Ref.Key); err != nil || !found || !reflect.DeepEqual(got, asQAJSON(t, proposal)) {
			t.Fatal("raw Parent canonical read failed", err)
		}
		if delta, err := sqliteWriteActorLocal(tx, source.ComputerID, &proposal, proposal); err != nil || delta != 0 {
			t.Fatal("raw Parent repeated CAS failed", err)
		}
	} else {
		bgQAValidation(t, err)
		if delta != 0 {
			t.Fatal("drifting Parent returned delta")
		}
	}
	if !reflect.DeepEqual(proposal, original) {
		t.Fatal("Parent encoder rewrote caller")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	asQAReopen(t, f, m, snapshot, source)
	finalRaw := asQAJSON(t, source)
	finalHead := finalRaw.Actors[actorKey(actor.Ref.Key)]
	finalHead.Parent.Key.SessionID = strings.Repeat("x", 255) + string([]byte{0xff})
	_ = bgQALegacyMarshalOracle(t, h.service, h.path, finalRaw, false)
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	invalid := asQAActor(finalHead)
	if charge, err := sqliteActorLocalCharge(invalid); charge != 0 {
		t.Fatal("final-invalid Parent charge nonzero")
	} else {
		bgQAValidation(t, err)
	}
	if delta, err := sqliteWriteActorLocal(tx, source.ComputerID, &actor, invalid); delta != 0 {
		t.Fatal("final-invalid Parent write charged")
	} else {
		bgQAValidation(t, err)
	}
	for _, computer := range []string{"", "bad"} {
		if got, found, err := sqliteReadActorLocal(tx, computer, actor.Ref.Key); found || !reflect.DeepEqual(got, sqliteActorLocalRow{}) {
			t.Fatal("invalid explicit Actor scope leaked row")
		} else {
			bgQAValidation(t, err)
		}
		if got, found, err := sqliteReadSegmentLocal(tx, computer, segment.ID); found || !reflect.DeepEqual(got, sqliteSegmentLocalRow{}) {
			t.Fatal("invalid explicit segment scope leaked row")
		} else {
			bgQAValidation(t, err)
		}
	}
	badKey := actor.Ref.Key
	badKey.SessionID = ""
	if got, found, err := sqliteReadActorLocal(tx, source.ComputerID, badKey); found || !reflect.DeepEqual(got, sqliteActorLocalRow{}) {
		t.Fatal("invalid explicit Actor key leaked")
	} else {
		bgQAValidation(t, err)
	}
	if got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, "bad"); found || !reflect.DeepEqual(got, sqliteSegmentLocalRow{}) {
		t.Fatal("invalid explicit segment ID leaked")
	} else {
		bgQAValidation(t, err)
	}
	if refs, err := sqliteActorUncertaintyIDsLocal(tx, source.ComputerID, badKey); refs != nil {
		t.Fatal("invalid Actor child key leaked")
	} else {
		bgQAValidation(t, err)
	}
	if ordinal, err := sqliteNextActorUncertaintyOrdinalLocal(tx, source.ComputerID, badKey); ordinal != 0 {
		t.Fatal("invalid Actor child key returned ordinal")
	} else {
		bgQAValidation(t, err)
	}
	if refs, err := sqliteSegmentEventsLocal(tx, source.ComputerID, "bad"); refs != nil {
		t.Fatal("invalid segment child ID leaked")
	} else {
		bgQAValidation(t, err)
	}
	if ordinal, err := sqliteNextSegmentEventOrdinalLocal(tx, source.ComputerID, "bad"); ordinal != 0 {
		t.Fatal("invalid segment child ID returned ordinal")
	} else {
		bgQAValidation(t, err)
	}
	nextOrdinal, err := sqliteNextActorUncertaintyOrdinalLocal(tx, source.ComputerID, actor.Ref.Key)
	if err != nil {
		t.Fatal(err)
	}
	if delta, err := sqliteAppendActorUncertaintyLocal(tx, source.ComputerID, actor.Ref.Key, nextOrdinal, "bad"); delta != 0 {
		t.Fatal("invalid uncertainty UUID charged")
	} else {
		bgQAValidation(t, err)
	}
	if delta, err := sqliteAppendSegmentEventLocal(tx, source.ComputerID, "bad", 0, "opaque"); delta != 0 {
		t.Fatal("invalid segment append ID charged")
	} else {
		bgQAValidation(t, err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	asQAReopen(t, f, m, snapshot, source)
}

func TestSQLiteActorSegmentLocalTransientCycleAndDeferredTargetRequireCallerFinalization(t *testing.T) {
	h, source := asQALegacy(t)
	actor, segment := asQARows(t, source)
	f, m, snapshot := asQASeed(t, source)
	for _, supplyTarget := range []bool{false, true} {
		t.Run(fmt.Sprintf("target-staged-%t", supplyTarget), func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			closed := asQAJSON(t, segment)
			end := closed.Confirmed
			closed.End = &end
			d1, err := sqliteWriteSegmentLocal(tx, source.ComputerID, &segment, closed)
			if err != nil || d1 != asQASegmentCharge(t, closed)-asQASegmentCharge(t, segment) {
				t.Fatal("transient segment close refused locally", err)
			}
			if got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, closed.ID); err != nil || !found || !reflect.DeepEqual(got, closed) {
				t.Fatal("local closed scalar projection failed", err)
			}
			if got, found, err := sqliteReadActorLocal(tx, source.ComputerID, actor.Ref.Key); err != nil || !found || !reflect.DeepEqual(got, actor) {
				t.Fatal("local reader incorrectly required working segment graph agreement", err)
			}
			brokenGraph := asQAJSON(t, source)
			brokenGraph.Segments[segment.ID].End = &end
			if validState(brokenGraph) {
				t.Fatal("cycle fixture failed to distinguish local success from complete legacy graph validity")
			}
			// Restore the transient graph projection before exercising target staging.
			if delta, err := sqliteWriteSegmentLocal(tx, source.ComputerID, &closed, segment); err != nil || delta != -d1 {
				t.Fatal("caller segment restore failed", err)
			}
			ordinal, err := sqliteNextActorUncertaintyOrdinalLocal(tx, source.ComputerID, actor.Ref.Key)
			if err != nil {
				t.Fatal(err)
			}
			newID := qaRecoveryRequest
			delta, err := sqliteAppendActorUncertaintyLocal(tx, source.ComputerID, actor.Ref.Key, ordinal, newID)
			if err != nil || delta != asQAChildCharge(t, actorKey(actor.Ref.Key), newID) {
				t.Fatal("Local append imposed premature target lookup", err)
			}
			if err := tx.CheckForeignKeys(); err == nil {
				t.Fatal("missing uncertainty target did not produce actual final FK evidence")
			} else {
				interopSafeError(t, err, newID, f.directory)
			}
			if supplyTarget {
				// Prove this copied actual unresolved target/evidence in a complete
				// strict legacy graph before using the narrow test-only literal binder.
				graph := asQAJSON(t, source)
				var original *Uncertainty
				for _, u := range graph.Uncertainties {
					original = u
					break
				}
				copy := asQAJSON(t, original)
				copy.ID = newID
				graph.Uncertainties[newID] = copy
				graph.UncertaintyEvidence[newID] = graph.UncertaintyEvidence[original.ID]
				graph.Actors[actorKey(actor.Ref.Key)].UncertaintyIDs = append(graph.Actors[actorKey(actor.Ref.Key)].UncertaintyIDs, newID)
				graph = bgQALegacyMarshalOracle(t, h.service, h.path, graph, true)
				asQAUncertaintyFixture(t, tx, graph.Uncertainties[newID], graph.UncertaintyEvidence[newID])
				if err := tx.CheckForeignKeys(); err != nil {
					t.Fatal("real cyclic target staging did not satisfy FKs", err)
				}
				interopRollback(t, tx)
			} else {
				outcome, commitErr := tx.Commit()
				var native *sqliteio.Error
				if outcome != sqliteio.Unknown || !errors.As(commitErr, &native) || native.Code&255 != 19 {
					t.Fatal("missing target commit lost deferred FK/native outcome evidence", commitErr)
				}
				interopSafeError(t, commitErr, newID, f.directory)
				if cleanup := tx.Rollback(); cleanup != nil {
					var checked *sqliteio.Error
					if !errors.As(cleanup, &checked) || checked.Phase != sqliteio.FinalizePhase || checked.Code&255 != 19 {
						t.Fatal("deferred FK cleanup evidence changed", cleanup)
					}
				}
			}
			interopClose(t, c)
			asQAReopen(t, f, m, snapshot, source)
		})
	}
}

func TestSQLiteActorSegmentLocalFiniteInsertUpdateUniquenessAndMissingDependencies(t *testing.T) {
	_, source := asQALegacy(t)
	actor, segment := asQARows(t, source)
	f, m, snapshot := asQASeed(t, source)
	for _, family := range []string{"Actor", "segment"} {
		for _, operation := range []string{"duplicate-insert", "missing-update", "missing-generation"} {
			t.Run(family+"/"+operation, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				a := asQAJSON(t, actor)
				s := asQAJSON(t, segment)
				var beforeA *sqliteActorLocalRow
				var beforeS *sqliteSegmentLocalRow
				if operation == "missing-update" {
					a.ID, a.Ref.Key.AgentID = qaRecoveryRequest, "missing-current-head"
					asQAEnsureActor(t, tx, a)
					s.ID = qaRecoveryRequest
					beforeA, beforeS = &a, &s
				}
				if operation == "missing-generation" {
					a.ID, a.Ref.Key.AgentID = qaRecoveryRequest, "unensured-historical-generation"
					s.ID, s.Actor = qaRecoveryRequest, a.Ref
				}
				beforeSnapshot, _ := asQAAudit(t, tx)
				var delta int64
				var err error
				if family == "Actor" {
					delta, err = sqliteWriteActorLocal(tx, source.ComputerID, beforeA, a)
				} else {
					delta, err = sqliteWriteSegmentLocal(tx, source.ComputerID, beforeS, s)
				}
				if operation == "duplicate-insert" {
					bgQAValidation(t, err)
					var native *sqliteio.Error
					if !errors.As(err, &native) || native.Category != sqliteio.Constraint || (native.Code != 1555 && native.Code != 2067) {
						t.Fatal("ordinary duplicate INSERT lost native uniqueness evidence", err)
					}
				} else {
					bgQACorrupt(t, err)
				}
				if delta != 0 {
					t.Fatal("failed finite writer returned delta")
				}
				interopSafeError(t, err, actor.ID, segment.ID, f.directory)
				afterSnapshot, _ := asQAAudit(t, tx)
				if !reflect.DeepEqual(beforeSnapshot, afterSnapshot) {
					t.Fatal("finite writer fabricated/replaced a scalar row")
				}
				interopRollback(t, tx)
				interopClose(t, c)
				asQAReopen(t, f, m, snapshot, source)
			})
		}
	}
	// Real-schema duplicate immutable Actor ID under a different valid raw key.
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	collision := asQAJSON(t, actor)
	collision.Ref.Key.AgentID = "other-valid-actor-key"
	asQAEnsureActor(t, tx, collision)
	delta, err := sqliteWriteActorLocal(tx, source.ComputerID, nil, collision)
	bgQAValidation(t, err)
	var native *sqliteio.Error
	if delta != 0 || !errors.As(err, &native) || native.Category != sqliteio.Constraint || native.Code != 2067 {
		t.Fatal("immutable Actor ID collision lost native unique evidence", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	asQAReopen(t, f, m, snapshot, source)
}

func TestSQLiteActorSegmentLocalCallerCancellationAllDatabaseOperationsAndTerminalRefusal(t *testing.T) {
	_, source := asQALegacy(t)
	actor, segment := asQARows(t, source)
	f, m, snapshot := asQASeed(t, source)
	for _, operation := range []string{"Actor-read", "Actor-insert", "Actor-update", "Actor-list", "Actor-next", "Actor-append", "segment-read", "segment-insert", "segment-update", "segment-list", "segment-next", "segment-append"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			c, err := sqliteio.Open(ctx, f.directory, f.database, sqliteio.Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close(context.Background()) })
			tx, err := c.Begin(ctx, sqliteio.Write)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tx.Rollback() })
			staged := asQAJSON(t, segment)
			staged.ID = qaRecoveryRequest
			if delta, err := sqliteWriteSegmentLocal(tx, source.ComputerID, nil, staged); err != nil || delta != asQASegmentCharge(t, staged) {
				t.Fatal("valid pre-cancel staged sibling failed", err)
			}
			actorOrdinal, err := sqliteNextActorUncertaintyOrdinalLocal(tx, source.ComputerID, actor.Ref.Key)
			if err != nil {
				t.Fatal(err)
			}
			segmentOrdinal, err := sqliteNextSegmentEventOrdinalLocal(tx, source.ComputerID, segment.ID)
			if err != nil {
				t.Fatal(err)
			}
			cancel()
			var gotA sqliteActorLocalRow
			var gotS sqliteSegmentLocalRow
			var found bool
			var result int64
			var refs []string
			switch operation {
			case "Actor-read":
				gotA, found, err = sqliteReadActorLocal(tx, source.ComputerID, actor.Ref.Key)
			case "Actor-insert":
				result, err = sqliteWriteActorLocal(tx, source.ComputerID, nil, actor)
			case "Actor-update":
				result, err = sqliteWriteActorLocal(tx, source.ComputerID, &actor, actor)
			case "Actor-list":
				refs, err = sqliteActorUncertaintyIDsLocal(tx, source.ComputerID, actor.Ref.Key)
			case "Actor-next":
				result, err = sqliteNextActorUncertaintyOrdinalLocal(tx, source.ComputerID, actor.Ref.Key)
			case "Actor-append":
				result, err = sqliteAppendActorUncertaintyLocal(tx, source.ComputerID, actor.Ref.Key, actorOrdinal, source.Actors[actorKey(actor.Ref.Key)].UncertaintyIDs[0])
			case "segment-read":
				gotS, found, err = sqliteReadSegmentLocal(tx, source.ComputerID, segment.ID)
			case "segment-insert":
				result, err = sqliteWriteSegmentLocal(tx, source.ComputerID, nil, segment)
			case "segment-update":
				result, err = sqliteWriteSegmentLocal(tx, source.ComputerID, &segment, segment)
			case "segment-list":
				refs, err = sqliteSegmentEventsLocal(tx, source.ComputerID, segment.ID)
			case "segment-next":
				result, err = sqliteNextSegmentEventOrdinalLocal(tx, source.ComputerID, segment.ID)
			case "segment-append":
				result, err = sqliteAppendSegmentEventLocal(tx, source.ComputerID, segment.ID, segmentOrdinal, "opaque\x00reference")
			}
			if !errors.Is(err, context.Canceled) || result != 0 || found || refs != nil || !reflect.DeepEqual(gotA, sqliteActorLocalRow{}) || !reflect.DeepEqual(gotS, sqliteSegmentLocalRow{}) {
				t.Fatal("canceled Local operation lost evidence/zero output", err)
			}
			interopSafeError(t, err, actor.ID, segment.ID, f.directory)
			if cleanup := tx.Rollback(); cleanup != nil {
				interopSafeError(t, cleanup, f.directory)
			}
			interopClose(t, c)
			asQAReopen(t, f, m, snapshot, source)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	if delta, err := sqliteWriteActorLocal(tx, source.ComputerID, &actor, actor); delta != 0 || err == nil {
		t.Fatal("read transaction Actor mutation unexpectedly admitted", err)
	} else {
		var native *sqliteio.Error
		if !errors.As(err, &native) || native.Category == sqliteio.Constraint {
			t.Fatal("adapter refusal relabeled as uniqueness", err)
		}
	}
	interopRollback(t, tx)
	if got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, segment.ID); err == nil || found || !reflect.DeepEqual(got, sqliteSegmentLocalRow{}) {
		t.Fatal("closed transaction conflated with absence", err)
	}
	interopClose(t, c)
	asQAReopen(t, f, m, snapshot, source)
}
