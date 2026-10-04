//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent tests-first selected graph QA. No global audit or fake adapter.
// All rejected staged variants roll back; cold readback checks the original
// metadata/nonce, stored bytes and independent literal storage charge.

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func TestSQLiteSelectedDependenciesActualCyclicGraphsAndColdHistory(t *testing.T) {
	for _, shape := range []string{"unresolved-history", "resolved", "discarded", "host-history"} {
		t.Run(shape, func(t *testing.T) {
			var raw *state
			switch shape {
			case "unresolved-history":
				_, raw = asQALegacy(t)
			case "host-history":
				_, source := hnQASource(t, false)
				raw = hnQAHistorical(t, source)
			default:
				_, raw, _, _ = ueQALegacy(t, shape)
			}
			f, m, snapshot, st := sdQASeed(t, raw)
			c, tx := interopOpen(t, f, false, sqliteio.Read)
			selection := sdQASelection(st)
			before := asQAJSON(t, selection)
			bgQAFixtureError(t, "complete selected cyclic graph", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selection))
			if !reflect.DeepEqual(selection, before) {
				t.Fatal("validator mutated caller selection")
			}
			sdQAUnchanged(t, tx, m, snapshot)
			interopRollback(t, tx)
			interopClose(t, c)
			sdQAReopen(t, f, m, snapshot, st)
		})
	}
}

func TestSQLiteSelectedDependenciesMaterializedGuardsAndSelectedDecoderErrors(t *testing.T) {
	_, raw := asQALegacy(t)
	f, m, snapshot, st := sdQASeed(t, raw)
	a, seg := asQARows(t, st)
	for _, axis := range []string{"short-invalid-utf8", "unencodable-wall"} {
		t.Run(axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Read)
			bgQAFixtureError(t, "owned direct Actor control", sqliteValidateActorDependencies(tx, m.ComputerID, a))
			bad := asQAJSON(t, a)
			if axis == "short-invalid-utf8" {
				bad.LastEvidence.Epoch = asQAPointer("e" + string([]byte{0xff}))
				if utf8.ValidString(*bad.LastEvidence.Epoch) {
					t.Fatal("invalid byte witness lost")
				}
			} else {
				bad.LastEvidence.WallUTC = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
				if _, err := bad.LastEvidence.WallUTC.MarshalJSON(); err == nil {
					t.Fatal("time witness is JSON encodable")
				}
			}
			if !sqliteValidActorLocal(bad) {
				t.Fatal("raw local control does not isolate materialized boundary")
			}
			if _, _, ok := sampleValues(bad.LastEvidence); !ok {
				t.Fatal("raw sample control refused shape")
			}
			owned := bad
			owned.LastEvidence = ueQACloneClock(bad.LastEvidence)
			bgQACorrupt(t, sqliteValidateActorDependencies(tx, m.ComputerID, bad))
			if !reflect.DeepEqual(bad, owned) {
				t.Fatal("materialized evidence was repaired or mutated")
			}
			bgQAFixtureError(t, "restored owned Actor", sqliteValidateActorDependencies(tx, m.ComputerID, a))
			sdQAUnchanged(t, tx, m, snapshot)
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	for _, ceiling := range []string{"0", "01", "", "18446744073709551616"} {
		c, tx := interopOpen(t, f, false, sqliteio.Read)
		bgQAValidation(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, ceiling, sqliteDependencySelection{}))
		interopRollback(t, tx)
		interopClose(t, c)
	}
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	bgQAValidation(t, sqliteValidateSelectedCaptureDependencies(tx, "invalid", m.Revision, sqliteDependencySelection{}))
	bgQAValidation(t, sqliteValidateActorUncertaintyEdge(tx, m.ComputerID, a.Ref.Key, -1))
	wrongScope := a
	wrongScope.Ref.Key.ComputerID = asQAForeign
	bgQACorrupt(t, sqliteValidateActorDependencies(tx, m.ComputerID, wrongScope))
	interopRollback(t, tx)
	interopClose(t, c)
	for _, axis := range []string{"selected-clock-text", "selected-time-json", "selected-kind", "missing-generation"} {
		t.Run(axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			bgQAFixtureError(t, "before selected corruption", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ActorKeys: []ActorKey{a.Ref.Key}}))
			switch axis {
			case "selected-clock-text":
				interopDone(t, tx, "UPDATE actors SET last_evidence_epoch=? WHERE actor_key=?", sqliteio.Text("e"+string([]byte{0xff})), sqliteio.Text(actorKey(a.Ref.Key)))
			case "selected-time-json":
				interopDone(t, tx, "UPDATE actors SET last_evidence_wall_json=? WHERE actor_key=?", sqliteio.Text("not a JSON time"), sqliteio.Text(actorKey(a.Ref.Key)))
			case "selected-kind":
				asQAShadow(t, tx, "actors", asQAActorColumns)
				interopDone(t, tx, "UPDATE actors SET last_evidence_wall_nsec=? WHERE actor_key=?", sqliteio.Blob([]byte{1}), sqliteio.Text(actorKey(a.Ref.Key)))
			case "missing-generation":
				interopDone(t, tx, "DELETE FROM actor_generations WHERE actor_key=? AND generation=?", sqliteio.Text(actorKey(a.Ref.Key)), interopCounter(t, a.Ref.Generation))
			}
			bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ActorKeys: []ActorKey{a.Ref.Key}}))
			// Unrelated selected epoch is not promoted into a global Actor scan.
			bgQAFixtureError(t, "unselected corruption control", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{EpochIDs: []string{seg.EpochID}}))
			interopRollback(t, tx)
			interopClose(t, c)
			sdQAReopen(t, f, m, snapshot, st)
		})
	}
	t.Run("actual-cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		conn, err := sqliteio.Open(ctx, f.directory, f.database, sqliteio.Options{ReadOnly: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := conn.Close(context.Background()); err != nil {
				t.Errorf("owned cancellation connection cleanup: %v", err)
			}
		})
		owned, err := conn.Begin(ctx, sqliteio.Read)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := owned.Rollback(); err != nil {
				t.Errorf("owned cancellation transaction cleanup: %v", err)
			}
		})
		bgQAFixtureError(t, "before cancellation", sqliteValidateSelectedCaptureDependencies(owned, m.ComputerID, m.Revision, sdQASelection(st)))
		cancel()
		err = sqliteValidateSelectedCaptureDependencies(owned, m.ComputerID, m.Revision, sqliteDependencySelection{ActorKeys: []ActorKey{a.Ref.Key}})
		var native *sqliteio.Error
		if !errors.Is(err, context.Canceled) || !errors.As(err, &native) || native.Category != sqliteio.Canceled {
			t.Fatalf("lost native cancellation: %v", err)
		}
		interopSafeError(t, err, f.directory, a.ID)
		if err := owned.Rollback(); err != nil {
			t.Fatal("checked cancellation rollback", err)
		}
		if err := conn.Close(context.Background()); err != nil {
			t.Fatal("checked cancellation close", err)
		}
		sdQAReopen(t, f, m, snapshot, st)
	})
}

func TestSQLiteSelectedDependenciesActorSegmentEpochAndUnchangedUncertainty(t *testing.T) {
	raw := sdQAClosedHistory(t)
	f, m, snapshot, st := sdQASeed(t, raw)
	a, working := asQARows(t, st)
	t.Run("close-only-then-detach", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		after := asQAJSON(t, working)
		end := after.Confirmed
		after.End = &end
		oracle := hnQAClone(t, st)
		oracle.Segments[working.ID].End = &end
		if validState(oracle) {
			t.Fatal("real legacy validator accepted working Actor on closed segment")
		}
		oracle.Actors[actorKey(a.Ref.Key)].State, oracle.Actors[actorKey(a.Ref.Key)].SegmentID = "wait_user", nil
		hnQAValid(t, oracle)
		_, err := sqliteWriteSegmentLocal(tx, m.ComputerID, &working, after)
		bgQAFixtureError(t, "close working segment", err)
		selected := sqliteDependencySelection{ChangedSegmentIDs: []string{after.ID}}
		bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
		detached := asQAJSON(t, a)
		detached.State, detached.SegmentID = "wait_user", nil
		_, err = sqliteWriteActorLocal(tx, m.ComputerID, &a, detached)
		bgQAFixtureError(t, "detach working head", err)
		bgQAFixtureError(t, "close plus detach", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
		interopRollback(t, tx)
		interopClose(t, c)
		sdQAReopen(t, f, m, snapshot, st)
	})
	for _, axis := range []string{"projection", "clock-coordinate", "full-attribution", "foreign-epoch"} {
		t.Run(axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			bgQAFixtureError(t, "exact historical epoch control", sqliteValidateSegmentDependencies(tx, m.ComputerID, working))
			bad := asQAJSON(t, working)
			switch axis {
			case "projection":
				bad.Confirmed = bad.Confirmed.Add(time.Nanosecond)
			case "clock-coordinate":
				bad.ConfirmedSample.Epoch = asQAPointer("other valid coordinate")
			case "full-attribution":
				bad.Binding.Attribution.Timezone = "UTC"
				if working.Binding.Attribution.Timezone == "UTC" {
					bad.Binding.Attribution.Timezone = "America/New_York"
				}
			case "foreign-epoch":
				ep, found, err := sqliteReadEpoch(tx, m.ComputerID, working.EpochID)
				if err != nil || !found {
					t.Fatal(err)
				}
				ep.Value.ID, ep.Value.ComputerID = sdQAMissing, asQAForeign
				ep.Ordinal, err = sqliteNextEpochOrdinal(tx)
				bgQAFixtureError(t, "next epoch", err)
				_, err = sqliteInsertEpoch(tx, ep)
				bgQAFixtureError(t, "valid foreign epoch", err)
				if _, found, err := sqliteReadEpoch(tx, asQAForeign, ep.Value.ID); err != nil || !found {
					t.Fatal("foreign positive scope", err)
				}
				bad.EpochID = ep.Value.ID
			}
			if !sqliteValidSegmentLocal(bad) {
				t.Fatal("variant is not locally valid")
			}
			bgQACorrupt(t, sqliteValidateSegmentDependencies(tx, m.ComputerID, bad))
			// A newer same-attribution epoch must not replace the named epoch.
			ep, found, err := sqliteReadEpoch(tx, m.ComputerID, working.EpochID)
			if err != nil || !found {
				t.Fatal(err)
			}
			ep.Value.ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
			ep.Value.Anchor.WallUTC = ep.Value.Anchor.WallUTC.Add(24 * time.Hour)
			ep.Ordinal, err = sqliteNextEpochOrdinal(tx)
			bgQAFixtureError(t, "new ordinal", err)
			_, err = sqliteInsertEpoch(tx, ep)
			bgQAFixtureError(t, "newer epoch", err)
			bgQAFixtureError(t, "retained exact epoch after newer insertion", sqliteValidateSegmentDependencies(tx, m.ComputerID, working))
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	t.Run("confirmed-change-rescans-unchanged-uncertainty", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		u := sdQAFirstU(t, st)
		before := asQASegment(st.Segments[u.SegmentID])
		after := asQAJSON(t, before)
		if after.End == nil || !after.End.Equal(after.Confirmed) || u.State != "unresolved" {
			t.Fatal("requires closed unresolved historical segment")
		}
		after.Confirmed = after.Confirmed.Add(time.Nanosecond)
		end := after.Confirmed
		after.End = &end
		after.ConfirmedSample.WallUTC = after.ConfirmedSample.WallUTC.Add(time.Nanosecond)
		n, _ := counter(*after.ConfirmedSample.ElapsedNS)
		after.ConfirmedSample.ElapsedNS = asQAPointer(strconv.FormatUint(n+1, 10))
		n, _ = counter(*after.ConfirmedSample.AwakeNS)
		after.ConfirmedSample.AwakeNS = asQAPointer(strconv.FormatUint(n+1, 10))
		_, err := sqliteWriteSegmentLocal(tx, m.ComputerID, &before, after)
		bgQAFixtureError(t, "compatible projected sample change", err)
		bgQAFixtureError(t, "segment relationship still local-valid", sqliteValidateSegmentDependencies(tx, m.ComputerID, after))
		selected := sqliteDependencySelection{ChangedSegmentIDs: []string{after.ID}}
		bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
		updated := ueQACloneU(u)
		updated.LowerBound = after.Confirmed
		_, err = sqliteWriteUncertainty(tx, m.ComputerID, &u, updated)
		bgQAFixtureError(t, "matching lower bound", err)
		bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
		e := ueQACloneE(st.UncertaintyEvidence[u.ID])
		nextE := ueQACloneE(e)
		nextE.LastConfirmed = ueQACloneClock(after.ConfirmedSample)
		_, err = sqliteWriteUncertaintyEvidence(tx, u.ID, &e, nextE)
		bgQAFixtureError(t, "matching LastConfirmed", err)
		bgQAFixtureError(t, "coupled repair after all staging", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
		interopRollback(t, tx)
		interopClose(t, c)
		sdQAReopen(t, f, m, snapshot, st)
	})
}

func TestSQLiteSelectedDependenciesTerminalReverseMembershipAndMissingOwner(t *testing.T) {
	raw := sdQAClosedHistory(t)
	a, working := asQARows(t, raw)
	owner := raw.Actors[actorKey(a.Ref.Key)]
	owner.State, owner.SegmentID = "finished", nil
	end := working.Confirmed
	raw.Segments[working.ID].End = &end
	f, m, snapshot, st := sdQASeed(t, raw)
	u := sdQAFirstU(t, st)
	if u.Actor.Generation == st.Actors[actorKey(u.Actor.Key)].Ref.Generation {
		t.Fatal("old generation positive control absent")
	}
	if len(st.Actors[actorKey(u.Actor.Key)].UncertaintyIDs) < 2 {
		t.Fatal("duplicate membership fixture absent")
	}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	bgQAFixtureError(t, "terminal older-generation duplicate edges", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ChangedUncertaintyIDs: []string{u.ID}}))
	seg := asQASegment(st.Segments[u.SegmentID])
	movedSeg := asQAJSON(t, seg)
	movedU := ueQACloneU(u)
	ref := u.Actor
	ref.Key.AgentID = "valid-other-historical-owner"
	_, err := sqliteEnsureActorGeneration(tx, ref)
	bgQAFixtureError(t, "real retained identity", err)
	movedSeg.Actor, movedU.Actor = ref, ref
	_, err = sqliteWriteSegmentLocal(tx, m.ComputerID, &seg, movedSeg)
	bgQAFixtureError(t, "move closed segment Ref", err)
	_, err = sqliteWriteUncertainty(tx, m.ComputerID, &u, movedU)
	bgQAFixtureError(t, "move uncertainty Ref", err)
	bgQAFixtureError(t, "mutual scalar relationships still valid", sqliteValidateUncertaintyDependencies(tx, m.ComputerID, m.Revision, movedU))
	bgQAFixtureError(t, "moved segment relationship", sqliteValidateSegmentDependencies(tx, m.ComputerID, movedSeg))
	bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ChangedUncertaintyIDs: []string{u.ID}}))
	interopRollback(t, tx)
	interopClose(t, c)
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	interopDone(t, tx, "DELETE FROM actors WHERE actor_key=?", sqliteio.Text(actorKey(u.Actor.Key)))
	if erQACount(t, tx, "SELECT count(*) FROM actor_uncertainties WHERE uncertainty_id=?", sqliteio.Text(u.ID)) < 2 {
		t.Fatal("missing owner edges unexpectedly vanished")
	}
	bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ChangedUncertaintyIDs: []string{u.ID}}))
	bgQACorrupt(t, sqliteValidateActorUncertaintyEdge(tx, m.ComputerID, u.Actor.Key, 0))
	interopRollback(t, tx)
	interopClose(t, c)
	sdQAReopen(t, f, m, snapshot, st)
}

func TestSQLiteSelectedDependenciesRecoveryCouplingProofCeilingAndWallFallback(t *testing.T) {
	for _, shape := range []string{"resolved", "discarded"} {
		t.Run(shape, func(t *testing.T) {
			_, raw, u, _, _, _ := rdQASource(t, shape)
			// EntityRevision is historical, not current U.Revision. Snapshot is
			// positive historical evidence, not the present metadata revision.
			d := raw.RecoveryDecisions[u.ID]
			receipt := raw.Requests[d.RequestID]
			result := *receipt.MutationResult
			result.EntityRevision, result.SnapshotRevision = asQAPointer(asQAMax), "1"
			receipt.MutationResult = &result
			raw.Requests[d.RequestID] = receipt
			f, m, snapshot, st := sdQASeed(t, raw)
			u = ueQACloneU(*st.Uncertainties[u.ID])
			axes := []string{"missing-evidence", "missing-decision", "missing-proof", "previous-max", "previous-mismatch", "decision-end", "end-before-confirmed", "suffix-presence", "proof-without-selected-id", "proof-missing-other-id", "proof-foreign-other-id", "proof-ceiling", "last-confirmed-representation"}
			if shape == "resolved" {
				axes = append(axes, "observed-unavailable", "observed-fallback-too-early", "end-after-upper", "suffix-start", "suffix-end")
			} else {
				axes = append(axes, "discarded-end-after-confirmed")
			}
			for _, axis := range axes {
				t.Run(axis, func(t *testing.T) {
					c, tx := interopOpen(t, f, false, sqliteio.Write)
					selected := sqliteDependencySelection{ChangedUncertaintyIDs: []string{u.ID}}
					bgQAFixtureError(t, "real recovery positive control", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
					decision := rdQAClone(st.RecoveryDecisions[u.ID])
					proof := sdQAProof(t, st, u)
					switch axis {
					case "missing-evidence":
						interopDone(t, tx, "DELETE FROM uncertainty_evidence WHERE uncertainty_id=?", sqliteio.Text(u.ID))
					case "missing-decision":
						interopDone(t, tx, "DELETE FROM recovery_decisions WHERE uncertainty_id=?", sqliteio.Text(u.ID))
					case "missing-proof":
						interopDone(t, tx, "DELETE FROM requests WHERE request_id=?", sqliteio.Text(proof.ID))
					case "previous-max":
						interopDone(t, tx, "UPDATE recovery_decisions SET previous_revision=? WHERE uncertainty_id=?", interopCounter(t, asQAMax), sqliteio.Text(u.ID))
					case "previous-mismatch":
						interopDone(t, tx, "UPDATE recovery_decisions SET previous_revision=? WHERE uncertainty_id=?", interopCounter(t, u.Revision), sqliteio.Text(u.ID))
					case "decision-end":
						interopDone(t, tx, "DELETE FROM recovery_decisions WHERE uncertainty_id=?", sqliteio.Text(u.ID))
						decision.ResolutionEnd = decision.ResolutionEnd.Add(time.Nanosecond)
						_, err := sqliteInsertRecoveryDecision(tx, decision)
						bgQAFixtureError(t, "locally valid unmatched end", err)
					case "suffix-presence":
						interopDone(t, tx, "DELETE FROM recovery_decisions WHERE uncertainty_id=?", sqliteio.Text(u.ID))
						if u.UpperBound == nil {
							decision.DiscardedSuffix = &TimeRange{Start: decision.ResolutionEnd, End: decision.ResolutionEnd}
						} else {
							decision.DiscardedSuffix = nil
						}
						_, err := sqliteInsertRecoveryDecision(tx, decision)
						bgQAFixtureError(t, "locally valid wrong suffix presence", err)
					case "suffix-start", "suffix-end":
						if decision.DiscardedSuffix == nil {
							t.Fatal("real suffix fixture absent")
						}
						if axis == "suffix-start" {
							decision.DiscardedSuffix.Start = decision.DiscardedSuffix.Start.Add(time.Nanosecond)
						} else {
							decision.DiscardedSuffix.End = decision.DiscardedSuffix.End.Add(time.Nanosecond)
						}
						interopDone(t, tx, "DELETE FROM recovery_decisions WHERE uncertainty_id=?", sqliteio.Text(u.ID))
						_, err := sqliteInsertRecoveryDecision(tx, decision)
						bgQAFixtureError(t, "locally valid wrong suffix endpoint", err)
					case "end-before-confirmed", "end-after-upper", "discarded-end-after-confirmed":
						before := asQASegment(st.Segments[u.SegmentID])
						after := asQAJSON(t, before)
						newEnd := before.Confirmed.Add(-time.Nanosecond)
						if axis == "discarded-end-after-confirmed" {
							newEnd = before.Confirmed.Add(time.Nanosecond)
						}
						if axis == "end-after-upper" {
							if u.UpperBound == nil || decision.ObservedSample == nil {
								t.Fatal("real bounded resolution fixture absent")
							}
							newEnd = u.UpperBound.Add(time.Nanosecond)
							decision.ObservedSample.Epoch = asQAPointer("recovery wall fallback coordinate")
							decision.ObservedSample.WallUTC = newEnd.Add(time.Second)
						}
						after.End = &newEnd
						updated := ueQACloneU(u)
						updated.ResolutionEnd = &newEnd
						decision.ResolutionEnd = newEnd
						if decision.DiscardedSuffix != nil {
							decision.DiscardedSuffix.Start = newEnd
						}
						_, err := sqliteWriteSegmentLocal(tx, m.ComputerID, &before, after)
						bgQAFixtureError(t, "coupled segment end", err)
						_, err = sqliteWriteUncertainty(tx, m.ComputerID, &u, updated)
						bgQAFixtureError(t, "coupled uncertainty end", err)
						interopDone(t, tx, "DELETE FROM recovery_decisions WHERE uncertainty_id=?", sqliteio.Text(u.ID))
						_, err = sqliteInsertRecoveryDecision(tx, decision)
						bgQAFixtureError(t, "coupled decision end", err)
					case "proof-without-selected-id":
						for _, a := range st.Actors {
							proof.Result.AffectedIDs = []string{a.ID}
							break
						}
						if len(proof.Result.AffectedIDs) != 1 || proof.Result.AffectedIDs[0] == u.ID {
							t.Fatal("real Actor affected-ID control absent")
						}
						sdQAReplaceProof(t, tx, proof)
						bgQAFixtureError(t, "scoped proof remains independently valid", sqliteValidateResolveRequestDependencies(tx, m.ComputerID, m.Revision, proof))
						bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ChangedResolveRequestIDs: []string{proof.ID}}))
					case "proof-missing-other-id":
						proof.Result.AffectedIDs = []string{u.ID, sdQAMissing}
						sdQAReplaceProof(t, tx, proof)
					case "proof-foreign-other-id":
						var foreign sqliteActorLocalRow
						for _, a := range st.Actors {
							foreign = asQAActor(a)
							break
						}
						foreign.ID, foreign.Ref.Key.ComputerID, foreign.Ref.Key.SessionID = sdQAMissing, asQAForeign, "retained-foreign-proof-control"
						foreign.Parent, foreign.SegmentID, foreign.State = nil, nil, "finished"
						_, err := sqliteEnsureActorGeneration(tx, foreign.Ref)
						bgQAFixtureError(t, "foreign generation", err)
						_, err = sqliteWriteActorLocal(tx, asQAForeign, nil, foreign)
						bgQAFixtureError(t, "foreign Actor positive control", err)
						if _, found, err := sqliteReadActorLocal(tx, asQAForeign, foreign.Ref.Key); err != nil || !found {
							t.Fatal("foreign selected scope control", err)
						}
						proof.Result.AffectedIDs = []string{u.ID, foreign.ID}
						sdQAReplaceProof(t, tx, proof)
					case "proof-ceiling":
						proof.Result.SnapshotRevision = bump(m.Revision)
						sdQAReplaceProof(t, tx, proof)
					case "last-confirmed-representation":
						e := ueQACloneE(st.UncertaintyEvidence[u.ID])
						after := ueQACloneE(e)
						after.LastConfirmed.WallUTC = after.LastConfirmed.WallUTC.In(time.FixedZone("owned offset", 3600))
						if !after.LastConfirmed.WallUTC.Equal(e.LastConfirmed.WallUTC) || reflect.DeepEqual(after.LastConfirmed, e.LastConfirmed) {
							t.Fatal("equal instant / distinct representation witness absent")
						}
						_, err := sqliteWriteUncertaintyEvidence(tx, u.ID, &e, after)
						bgQAFixtureError(t, "retained time representation", err)
					case "observed-unavailable":
						interopDone(t, tx, "UPDATE recovery_decisions SET observed_capability=? WHERE uncertainty_id=?", sqliteio.Text("unavailable"), sqliteio.Text(u.ID))
					case "observed-fallback-too-early":
						if decision.ObservedSample == nil || u.UpperBound == nil {
							t.Fatal("resolved observed/upper fixture absent")
						}
						decision.ObservedSample.Epoch = asQAPointer("different available clock")
						decision.ObservedSample.WallUTC = st.Segments[u.SegmentID].Confirmed.Add(-time.Nanosecond)
						interopDone(t, tx, "DELETE FROM recovery_decisions WHERE uncertainty_id=?", sqliteio.Text(u.ID))
						_, err := sqliteInsertRecoveryDecision(tx, decision)
						bgQAFixtureError(t, "available too-early recovery fallback", err)
					}
					bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
					interopRollback(t, tx)
					interopClose(t, c)
				})
			}
			sdQAReopen(t, f, m, snapshot, st)
		})
	}
	t.Run("actual-complete-recovery-only-wall-fallback", func(t *testing.T) {
		_, raw, u, _, d, _ := rdQASource(t, "resolved")
		if d.ObservedSample == nil || u.UpperBound == nil {
			t.Fatal("available recovery ceiling fixture absent")
		}
		d.ObservedSample.Epoch = asQAPointer("different available clock")
		d.ObservedSample.WallUTC = u.UpperBound.Add(time.Nanosecond)
		if _, ok := projectSample(findEpoch(raw, raw.Segments[u.SegmentID].EpochID), *d.ObservedSample); ok {
			t.Fatal("fallback witness unexpectedly projects")
		}
		raw.RecoveryDecisions[u.ID] = d
		f, m, snapshot, st := sdQASeed(t, raw)
		c, tx := interopOpen(t, f, false, sqliteio.Read)
		bgQAFixtureError(t, "real recovery fallback acceptance", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{UncertaintyIDs: []string{u.ID}}))
		sdQAUnchanged(t, tx, m, snapshot)
		interopRollback(t, tx)
		interopClose(t, c)
		sdQAReopen(t, f, m, snapshot, st)
	})
	t.Run("discarded-raw-observation-zero-and-optional-text", func(t *testing.T) {
		_, raw, u, _, d, _ := rdQASource(t, "discarded")
		d.ObservedSample = &ClockSample{Capability: "arbitrary retained capability", WallUTC: time.Time{}, Epoch: asQAPointer("opaque raw epoch"), ElapsedNS: asQAPointer("not a counter"), AwakeNS: nil}
		if _, _, ok := sampleValues(*d.ObservedSample); ok {
			t.Fatal("discarded permissive witness accidentally available")
		}
		raw.RecoveryDecisions[u.ID] = d
		f, m, snapshot, st := sdQASeed(t, raw)
		c, tx := interopOpen(t, f, false, sqliteio.Read)
		bgQAFixtureError(t, "discarded raw observation acceptance", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{UncertaintyIDs: []string{u.ID}}))
		sdQAUnchanged(t, tx, m, snapshot)
		interopRollback(t, tx)
		interopClose(t, c)
		sdQAReopen(t, f, m, snapshot, st)
	})
}

func TestSQLiteSelectedDependenciesHostRootReverseOwnerAndIndex(t *testing.T) {
	_, raw := hnQASource(t, false)
	st := hnQAHistorical(t, raw)
	rootKey, root := hnQARoot(t, st)
	// The complete strict-valid historical graph has actorless retained turns
	// and explicitly permits Actor.Source != native Source.
	f, m, snapshot, st := sdQASeed(t, st)
	for _, action := range []string{"unchanged-root-rejected", "clear-root", "repoint-root"} {
		t.Run(action, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			selected := sqliteDependencySelection{ChangedHostTurnKeys: []string{rootKey}}
			bgQAFixtureError(t, "root owner baseline", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
			before := hnQATurn(rootKey, root)
			after := hnQACopyTurn(before)
			after.Actor = nil
			if action == "unchanged-root-rejected" {
				_, err := sqliteWriteHostTurn(tx, m.ComputerID, &before, after)
				bgQAFixtureError(t, "stage root actor removal", err)
				bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
			} else {
				// The real session CAS checks its old root. Clear/repoint while
				// that root is still valid, then stage the old turn mutation.
				for key, session := range st.HostSessions {
					old := sqliteHostSessionRow{Key: key, Value: *session}
					next := old
					next.Value.RootTurn = ""
					if action == "repoint-root" {
						newRoot := hnQACopyTurn(before)
						newRoot.TurnID = "new actual root identity"
						newRoot.Key = hostTurnKey(newRoot.Incarnation, HostEvent{TurnID: newRoot.TurnID, AgentID: newRoot.AgentID})
						_, err := sqliteWriteHostTurn(tx, m.ComputerID, nil, newRoot)
						bgQAFixtureError(t, "new valid root", err)
						next.Value.RootTurn = newRoot.Key
					}
					_, err := sqliteWriteHostSession(tx, m.ComputerID, &old, next)
					bgQAFixtureError(t, "clear/repoint root in same staging transaction", err)
				}
				_, err := sqliteWriteHostTurn(tx, m.ComputerID, &before, after)
				bgQAFixtureError(t, "stage old root actor removal after session update", err)
				bgQAFixtureError(t, "final repaired root ownership", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	t.Run("reverse-selector-native-index-feature", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Read)
		// The approved new partial index is intentionally absent from the
		// baseline. This is a real native plan feature assertion, not a stub.
		hnQAPlan(t, tx, "SELECT source,native_session,session_key FROM host_sessions WHERE root_turn_key=? AND root_turn_key<>''", "host_session_root", sqliteio.Text(rootKey))
		interopRollback(t, tx)
		interopClose(t, c)
	})
	t.Run("cwd-update-does-not-promote-unselected-tool-history", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		before := hnQATurn(rootKey, root)
		after := hnQACopyTurn(before)
		after.CWD = "/synthetic/changed-directory"
		_, err := sqliteWriteHostTurn(tx, m.ComputerID, &before, after)
		bgQAFixtureError(t, "root CWD update", err)
		// Owned stored corruption in a completed historical tool is selected
		// only when the caller asks for that tool, never by a CWD update.
		var oldTurn, toolID string
		for key, turn := range st.HostTurns {
			for id, tool := range turn.Tools {
				if tool.Phase == "post" {
					oldTurn, toolID = key, id
				}
			}
		}
		if oldTurn == "" {
			t.Fatal("actual completed historical tool fixture absent")
		}
		interopDone(t, tx, "UPDATE host_tools SET phase=? WHERE turn_key=? AND tool_id=?", sqliteio.Text("failed"), sqliteio.Text(oldTurn), sqliteio.Text(toolID))
		bgQAFixtureError(t, "unselected retained tool control", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ChangedHostTurnKeys: []string{rootKey}}))
		bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{HostTools: []sqliteHostToolIdentity{{TurnKey: oldTurn, ToolID: toolID}}}))
		interopRollback(t, tx)
		interopClose(t, c)
	})
	sdQAReopen(t, f, m, snapshot, st)
}

func TestSQLiteSelectedDependenciesHistoricalPermissivenessOpaqueChildrenAndForeignEvent(t *testing.T) {
	t.Run("historical-binding-and-parent-without-current-head", func(t *testing.T) {
		_, raw := asQALegacy(t)
		a, working := asQARows(t, raw)
		actor := raw.Actors[actorKey(a.Ref.Key)]
		actor.BindingID, actor.BindingRevision, actor.Revision, actor.Sequence = sdQAMissing, asQAMax, asQAMax, asQAMax
		if raw.Bindings[actor.BindingID].ID != "" || actor.Parent == nil || raw.Actors[actorKey(actor.Parent.Key)] != nil {
			t.Fatal("historical absent binding/current parent controls lost")
		}
		if actor.BindingID == working.Binding.ID {
			t.Fatal("Actor/segment binding difference witness absent")
		}
		f, m, snapshot, st := sdQASeed(t, raw)
		c, tx := interopOpen(t, f, false, sqliteio.Read)
		bgQAFixtureError(t, "historical positive counters and absent current dependencies", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ActorKeys: []ActorKey{a.Ref.Key}}))
		sdQAUnchanged(t, tx, m, snapshot)
		interopRollback(t, tx)
		interopClose(t, c)
		sdQAReopen(t, f, m, snapshot, st)
	})
	t.Run("unnamed-uncertainty-attribution-and-unprojected-bound", func(t *testing.T) {
		_, raw, u, e := ueQALegacy(t, "bounded")
		if e.BoundSample == nil || u.UpperBound == nil {
			t.Fatal("bounded real fixture absent")
		}
		seg := raw.Segments[u.SegmentID]
		seg.UncertaintyID = nil
		u.Attribution.Timezone = "UTC"
		if seg.Binding.Attribution.Timezone == "UTC" {
			u.Attribution.Timezone = "America/New_York"
		}
		raw.Uncertainties[u.ID] = &u
		// Available, positive, but a different elapsed projection than Upper.
		bound := ueQACloneClock(*e.BoundSample)
		bound.WallUTC = bound.WallUTC.Add(time.Second)
		n, _ := counter(*bound.ElapsedNS)
		bound.ElapsedNS = asQAPointer(strconv.FormatUint(n+uint64(time.Second), 10))
		n, _ = counter(*bound.AwakeNS)
		bound.AwakeNS = asQAPointer(strconv.FormatUint(n+uint64(time.Second), 10))
		e.BoundSample = &bound
		projected, ok := projectSample(findEpoch(raw, seg.EpochID), bound)
		if !ok || projected.Equal(*u.UpperBound) {
			t.Fatal("different bound projection witness absent")
		}
		e.Detection.Epoch, e.Detection.ElapsedNS, e.Detection.AwakeNS = nil, asQAPointer("permissive raw detection"), nil
		e.MissingFrom, e.MissingThrough = "raw opaque from", "raw opaque through"
		raw.UncertaintyEvidence[u.ID] = e
		f, m, snapshot, st := sdQASeed(t, raw)
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		bgQAFixtureError(t, "real legacy directional attribution policy", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{UncertaintyIDs: []string{u.ID}}))
		before := asQASegment(st.Segments[u.SegmentID])
		after := asQAJSON(t, before)
		after.UncertaintyID = asQAPointer(u.ID)
		_, err := sqliteWriteSegmentLocal(tx, m.ComputerID, &before, after)
		bgQAFixtureError(t, "name mismatched attribution", err)
		bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ChangedSegmentIDs: []string{after.ID}}))
		interopRollback(t, tx)
		interopClose(t, c)
		sdQAReopen(t, f, m, snapshot, st)
	})
	t.Run("foreign-event-zero-snapshot-and-opaque-arrays", func(t *testing.T) {
		_, raw, pairs := erQASource(t)
		if len(pairs) == 0 || len(raw.Segments) < 2 {
			t.Fatal("actual event history fixture absent")
		}
		pair := pairs[0]
		pair.Row.Value.Result.Actor.Key.ComputerID = asQAForeign
		pair.Row.Value.Result.Actor.Key.SessionID = "foreign-without-current-head"
		pair.Row.Value.Result.Actor.Generation = asQAMax
		pair.Row.Value.Result.SnapshotRevision = "0"
		pair.Row.Value.Result.UncertaintyIDs = []string{"opaque non-UUID", "", "opaque non-UUID"}
		for id := range raw.Segments {
			if pair.Row.Value.Result.SegmentID == nil || id != *pair.Row.Value.Result.SegmentID {
				pair.Row.Value.Result.SegmentID = asQAPointer(id)
				break
			}
		}
		raw.Receipts[pair.Row.Key] = pair.Row.Value
		var childID string
		for id, seg := range raw.Segments {
			seg.EventReferences = append(seg.EventReferences, "opaque event has no receipt")
			childID = id
			break
		}
		f, m, snapshot, st := sdQASeed(t, raw)
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		ordinal := int64(len(st.Segments[childID].EventReferences) - 1)
		selection := sqliteDependencySelection{EventReceiptKeys: []string{pair.Row.Key}, SegmentEventEdges: []sqliteSegmentEventSelection{{SegmentID: childID, Ordinal: ordinal}}}
		bgQAFixtureError(t, "historical event permissiveness", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selection))
		if erQACount(t, tx, "SELECT count(*) FROM actors WHERE actor_key=?", sqliteio.Text(actorKey(pair.Row.Value.Result.Actor.Key))) != 0 {
			t.Fatal("fixture fabricated a foreign current head")
		}
		interopDone(t, tx, "UPDATE segment_events SET event_reference=? WHERE segment_id=? AND ordinal=?", sqliteio.Text(string([]byte{0xff})), sqliteio.Text(childID), sqliteio.Integer(ordinal))
		bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{SegmentEventEdges: selection.SegmentEventEdges}))
		// The retained event unit does not acquire segment-event graph checks.
		bgQAFixtureError(t, "opaque receipt remains independently selected", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{EventReceiptKeys: selection.EventReceiptKeys}))
		interopRollback(t, tx)
		interopClose(t, c)
		c, tx = interopOpen(t, f, false, sqliteio.Write)
		bgQAFixtureError(t, "opaque edge before missing owner", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{SegmentEventEdges: selection.SegmentEventEdges}))
		interopDone(t, tx, "DELETE FROM segments WHERE segment_id=?", sqliteio.Text(childID))
		bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{SegmentEventEdges: selection.SegmentEventEdges}))
		interopRollback(t, tx)
		interopClose(t, c)
		sdQAReopen(t, f, m, snapshot, st)
	})
}

func TestSQLiteSelectedDependenciesOnlyChangedUnionsAndOrphanOwners(t *testing.T) {
	t.Run("segment-only-changed-no-reverse-users", func(t *testing.T) {
		_, raw := asQALegacy(t)
		_, original := asQARows(t, raw)
		clone := *raw.Segments[original.ID]
		clone.ID, clone.UncertaintyID, clone.Finalized, clone.EventReferences = sdQAMissing, nil, false, []string{}
		raw.Segments[clone.ID] = &clone
		f, m, snapshot, st := sdQASeed(t, raw)
		selected := sqliteDependencySelection{ChangedSegmentIDs: []string{clone.ID}}
		for _, axis := range []string{"projection-invalid", "missing"} {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if erQACount(t, tx, "SELECT count(*) FROM actors WHERE segment_id=?", sqliteio.Text(clone.ID))+erQACount(t, tx, "SELECT count(*) FROM uncertainties WHERE segment_id=?", sqliteio.Text(clone.ID)) != 0 {
				t.Fatal("standalone changed segment has reverse users")
			}
			bgQAFixtureError(t, "only ChangedSegmentIDs positive", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
			if axis == "missing" {
				interopDone(t, tx, "DELETE FROM segments WHERE segment_id=?", sqliteio.Text(clone.ID))
			} else {
				before := asQASegment(st.Segments[clone.ID])
				after := asQAJSON(t, before)
				after.Confirmed = after.Confirmed.Add(time.Nanosecond)
				_, err := sqliteWriteSegmentLocal(tx, m.ComputerID, &before, after)
				bgQAFixtureError(t, "locally readable bad projection", err)
				if _, found, err := sqliteReadSegmentLocal(tx, m.ComputerID, clone.ID); err != nil || !found {
					t.Fatal("local projection control", err)
				}
			}
			bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
			interopRollback(t, tx)
			interopClose(t, c)
		}
		sdQAReopen(t, f, m, snapshot, st)
	})
	t.Run("uncertainty-only-changed-no-reverse-users", func(t *testing.T) {
		_, raw, u, _ := ueQALegacy(t, "unbounded")
		raw.Segments[u.SegmentID].UncertaintyID = nil
		for _, a := range raw.Actors {
			a.UncertaintyIDs = []string{}
		}
		f, m, snapshot, st := sdQASeed(t, raw)
		selected := sqliteDependencySelection{ChangedUncertaintyIDs: []string{u.ID}}
		for _, axis := range []string{"missing", "stored-malformed"} {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if erQACount(t, tx, "SELECT count(*) FROM segments WHERE uncertainty_id=?", sqliteio.Text(u.ID))+erQACount(t, tx, "SELECT count(*) FROM actor_uncertainties WHERE uncertainty_id=?", sqliteio.Text(u.ID)) != 0 {
				t.Fatal("standalone changed U has reverse users")
			}
			bgQAFixtureError(t, "only ChangedUncertaintyIDs positive", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
			if axis == "missing" {
				interopDone(t, tx, "DELETE FROM uncertainties WHERE uncertainty_id=?", sqliteio.Text(u.ID))
			} else {
				interopDone(t, tx, "UPDATE uncertainties SET lower_bound_json=? WHERE uncertainty_id=?", sqliteio.Text("malformed time"), sqliteio.Text(u.ID))
			}
			bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
			interopRollback(t, tx)
			interopClose(t, c)
		}
		sdQAReopen(t, f, m, snapshot, st)
	})
	t.Run("turn-only-changed-no-reverse-users", func(t *testing.T) {
		_, raw := hnQASource(t, false)
		raw = hnQAHistorical(t, raw)
		var key string
		for candidate, turn := range raw.HostTurns {
			if turn.Actor == nil && turn.Stopped {
				key = candidate
				break
			}
		}
		if key == "" {
			t.Fatal("real actorless retained turn absent")
		}
		f, m, snapshot, st := sdQASeed(t, raw)
		selected := sqliteDependencySelection{ChangedHostTurnKeys: []string{key}}
		for _, axis := range []string{"missing", "stored-malformed"} {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if erQACount(t, tx, "SELECT count(*) FROM host_sessions WHERE root_turn_key=?", sqliteio.Text(key)) != 0 {
				t.Fatal("standalone changed turn has root users")
			}
			bgQAFixtureError(t, "only ChangedHostTurnKeys positive", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
			if axis == "missing" {
				interopDone(t, tx, "DELETE FROM host_turns WHERE turn_key=?", sqliteio.Text(key))
			} else {
				interopDone(t, tx, "UPDATE host_turns SET cwd=? WHERE turn_key=?", sqliteio.Text("relative bad path"), sqliteio.Text(key))
			}
			bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
			interopRollback(t, tx)
			interopClose(t, c)
		}
		sdQAReopen(t, f, m, snapshot, st)
	})
	t.Run("proof-only-changed-no-decision-owners", func(t *testing.T) {
		_, raw, u, _ := ueQALegacy(t, "unbounded")
		_, resolved, _, _, _, actualProof := rdQASource(t, "resolved")
		proof := rpQAClone(actualProof)
		proof.ID, proof.Result.RequestID = sdQAMissing, sdQAMissing
		proof.Result.SnapshotRevision, proof.Result.EntityRevision, proof.Result.AffectedIDs = "1", asQAPointer("1"), []string{u.ID}
		receipt := resolved.Requests[actualProof.ID]
		receipt.MutationResult = &proof.Result
		if raw.Requests == nil {
			raw.Requests = make(map[string]mutationRequest)
		}
		raw.Requests[proof.ID] = receipt
		f, m, snapshot, st := sdQASeed(t, raw)
		selected := sqliteDependencySelection{ChangedResolveRequestIDs: []string{proof.ID}}
		for _, axis := range []string{"missing", "strict-payload", "missing-affected-id"} {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if erQACount(t, tx, "SELECT count(*) FROM recovery_decisions WHERE request_id=?", sqliteio.Text(proof.ID)) != 0 {
				t.Fatal("standalone proof has decision owners")
			}
			bgQAFixtureError(t, "only ChangedResolveRequestIDs positive", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
			switch axis {
			case "missing":
				interopDone(t, tx, "DELETE FROM requests WHERE request_id=?", sqliteio.Text(proof.ID))
			case "strict-payload":
				interopDone(t, tx, "UPDATE requests SET payload=? WHERE request_id=?", sqliteio.Text("{}"), sqliteio.Text(proof.ID))
			case "missing-affected-id":
				bad := rpQAClone(proof)
				bad.Result.AffectedIDs = []string{rpQAFreshID}
				sdQAReplaceProof(t, tx, bad)
			}
			bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, selected))
			interopRollback(t, tx)
			interopClose(t, c)
		}
		sdQAReopen(t, f, m, snapshot, st)
	})
	for _, fact := range []string{"evidence", "decision"} {
		t.Run("orphan-"+fact+"-only-changed-owner", func(t *testing.T) {
			_, raw, u, e, d, _ := rdQASource(t, "resolved")
			f, m, snapshot, st := sdQASeed(t, raw)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			bgQAFixtureError(t, "existing owner positive", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ChangedUncertaintyIDs: []string{u.ID}}))
			if fact == "evidence" {
				_, err := sqliteWriteUncertaintyEvidence(tx, sdQAMissing, nil, e)
				bgQAFixtureError(t, "stage real orphan evidence", err)
			} else {
				d.UncertaintyID = sdQAMissing
				_, err := sqliteInsertRecoveryDecision(tx, d)
				bgQAFixtureError(t, "stage real orphan decision", err)
			}
			if erQACount(t, tx, "SELECT count(*) FROM segments WHERE uncertainty_id=?", sqliteio.Text(sdQAMissing))+erQACount(t, tx, "SELECT count(*) FROM actor_uncertainties WHERE uncertainty_id=?", sqliteio.Text(sdQAMissing)) != 0 {
				t.Fatal("orphan unexpectedly has reverse users")
			}
			bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ChangedUncertaintyIDs: []string{sdQAMissing}}))
			interopRollback(t, tx)
			interopClose(t, c)
			sdQAReopen(t, f, m, snapshot, st)
		})
	}
}
