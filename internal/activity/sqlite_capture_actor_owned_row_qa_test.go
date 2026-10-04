//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// These serial tests use the existing complete semantic fixtures and real typed
// SQL writers. The only work observer is the existing native statement hook.
// No production deadline, native result, or selected dependency is replaced.
func aorQAOpen(t *testing.T, ctx context.Context, f interopFixture, mode sqliteio.Mode) *stQAOwner {
	t.Helper()
	c, err := sqliteio.Open(ctx, f.directory, f.database, sqliteio.Options{ReadOnly: mode == sqliteio.Read, AcquireDeadline: time.Now().Add(time.Second)})
	o := &stQAOwner{t: t, c: c}
	t.Cleanup(o.cleanup) // Register even a retained owner returned with an error.
	if err == nil {
		o.tx, err = c.Begin(ctx, mode) // Preserve a returned Tx before checking error.
	}
	if err != nil {
		cleanup := o.finish(false)
		o.reported = true
		t.Fatal("owned native fixture acquisition", errors.Join(err, cleanup))
	}
	return o
}

func aorQAIdle(t *testing.T) (interopFixture, sqliteStoreMeta, map[string][][]string, *state, sqliteActorLocalRow) {
	t.Helper()
	h := qaNew(t)
	h.seed()
	event := qaEvent("A", "1", "1", "work", qaBindingA)
	h.ingest(0, event)
	h.ingest(1, qaEvent("A", "1", "2", "wait_user", ""))
	f, m, snapshot, st := sdQASeed(t, bgQAReadLegacy(t, h.service))
	a := st.Actors[actorKey(event.Actor)]
	if a == nil || len(st.Actors) != 1 || a.Parent != nil || a.SegmentID != nil || a.State != "wait_user" {
		t.Fatal("actual parentless idle actor premise")
	}
	return f, m, snapshot, st, asQAJSON(t, asQAActor(a))
}

func aorQAPositive(t *testing.T, tx *sqliteio.Tx, m sqliteStoreMeta, want sqliteActorLocalRow) {
	t.Helper()
	got, err := sqliteCaptureReadActor(tx, m.ComputerID, m.Revision, want.Ref.Key)
	if err != nil || got == nil || !reflect.DeepEqual(*got, want) {
		t.Fatal("owned actor and selected closure positive control", err)
	}
	bgQAFixtureError(t, "unchanged reference composer", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ActorKeys: []ActorKey{want.Ref.Key}}))
}

func aorQARefusal(t *testing.T, tx *sqliteio.Tx, m sqliteStoreMeta, key ActorKey) {
	t.Helper()
	before, charge := sdQAAudit(t, tx, m)
	got, err := sqliteCaptureReadActor(tx, m.ComputerID, m.Revision, key)
	if got != nil {
		t.Fatal("failed dependency read exposed a partial actor")
	}
	bgQACorrupt(t, err)
	bgQACorrupt(t, sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ActorKeys: []ActorKey{key}}))
	after, afterCharge := sdQAAudit(t, tx, m)
	if !reflect.DeepEqual(before, after) || charge != afterCharge {
		t.Fatal("refusal changed the deliberately staged fixture")
	}
}

func TestSQLiteCaptureActorOwnedRowNativeWork(t *testing.T) {
	f, m, snapshot, st, want := aorQAIdle(t)
	for _, mode := range []struct {
		name  string
		value sqliteio.Mode
	}{{"read", sqliteio.Read}, {"write", sqliteio.Write}} {
		t.Run(mode.name, func(t *testing.T) {
			o := aorQAOpen(t, context.Background(), f, mode.value)
			var work [5]int // prepare, native step, ROW, DONE, successful finalize.
			ncQAHooks(t, ncQASQLHooks{Observe: func(e ncQASQLEvent) {
				switch {
				case e.Phase == "prepare-before" && e.Operation == "prepare":
					work[0]++
				case e.Phase == "step-before-native" && e.Operation == "statement":
					work[1]++
				case e.Phase == "step-after" && e.Operation == "statement" && e.Code == 100:
					work[2]++
				case e.Phase == "step-after" && e.Operation == "statement" && e.Code == 101:
					work[3]++
				case e.Phase == "finalize-after" && e.Operation == "statement" && e.Code == 0:
					work[4]++
				}
			}})
			if got := interopCount(t, o.tx, "SELECT 17"); got != 17 || work != [5]int{1, 2, 1, 1, 1} {
				t.Fatalf("real native ROW/DONE/finalize calibration: value=%d work=%v", got, work)
			}
			work = [5]int{}
			local, found, err := sqliteReadActorLocal(o.tx, m.ComputerID, want.Ref.Key)
			if err != nil || !found || !reflect.DeepEqual(local, want) || work != [5]int{2, 4, 2, 2, 2} {
				t.Fatalf("actual typed actor plus generation calibration: found=%t work=%v error=%v", found, work, err)
			}
			work = [5]int{}
			got, err := sqliteCaptureReadActor(o.tx, m.ComputerID, m.Revision, want.Ref.Key)
			measured := work
			ncQASetSQLHooks(ncQASQLHooks{})
			if err != nil || got == nil || !reflect.DeepEqual(*got, want) {
				t.Fatal("measured actor read did not produce its exact owned row", err)
			}
			t.Logf("actual native actor work prepare/step/ROW/DONE/finalize=%v", measured)
			if measured != [5]int{3, 6, 3, 3, 3} {
				// Nonfatal: the original duplicate read must still reach every
				// unchanged-state, checked-close and cold-history postcondition.
				t.Errorf("redundant owned actor read: work=%v want=[3 6 3 3 3]", measured)
			}
			aorQAPositive(t, o.tx, m, want)
			sdQAUnchanged(t, o.tx, m, snapshot)
			stQAClose(t, o, false)
			sdQAReopen(t, f, m, snapshot, st)
		})
	}
}

func TestSQLiteCaptureActorOwnedRowSelectedRefusals(t *testing.T) {
	_, raw := asQALegacy(t)
	f, m, snapshot, st := sdQASeed(t, raw)
	a, segment := asQARows(t, st)
	if a.Parent == nil || a.Ref.Generation != "2" || a.SegmentID == nil || segment.End != nil {
		t.Fatal("actual historical parent and open current segment premise")
	}
	for _, axis := range []string{"actor-kind", "actor-clock-json", "actor-generation", "parent-generation", "missing-segment", "closed-segment", "segment-owner", "missing-epoch", "projected-confirmed"} {
		t.Run(axis, func(t *testing.T) {
			o := aorQAOpen(t, context.Background(), f, sqliteio.Write)
			aorQAPositive(t, o.tx, m, a)
			switch axis {
			case "actor-kind":
				asQAShadow(t, o.tx, "actors", asQAActorColumns)
				interopDone(t, o.tx, "UPDATE actors SET last_evidence_wall_nsec=? WHERE actor_key=?", sqliteio.Blob([]byte{0}), sqliteio.Text(actorKey(a.Ref.Key)))
			case "actor-clock-json":
				interopDone(t, o.tx, "UPDATE actors SET last_evidence_wall_json=? WHERE actor_key=?", sqliteio.Text("invalid time JSON"), sqliteio.Text(actorKey(a.Ref.Key)))
			case "actor-generation":
				interopDone(t, o.tx, "DELETE FROM actor_generations WHERE actor_key=? AND generation=?", sqliteio.Text(actorKey(a.Ref.Key)), interopCounter(t, a.Ref.Generation))
			case "parent-generation":
				interopDone(t, o.tx, "DELETE FROM actor_generations WHERE actor_key=? AND generation=?", sqliteio.Text(actorKey(a.Parent.Key)), interopCounter(t, a.Parent.Generation))
			case "missing-segment":
				interopDone(t, o.tx, "UPDATE actors SET segment_id=? WHERE actor_key=?", sqliteio.Text(sdQAMissing), sqliteio.Text(actorKey(a.Ref.Key)))
			case "closed-segment":
				bad := asQAJSON(t, segment)
				end := bad.Confirmed
				bad.End = &end
				_, err := sqliteWriteSegmentLocal(o.tx, m.ComputerID, &segment, bad)
				bgQAFixtureError(t, "locally valid closed segment", err)
			case "segment-owner":
				interopDone(t, o.tx, "UPDATE segments SET actor_generation=? WHERE segment_id=?", interopCounter(t, "1"), sqliteio.Text(segment.ID))
			case "missing-epoch":
				interopDone(t, o.tx, "UPDATE segments SET epoch_id=? WHERE segment_id=?", sqliteio.Text(sdQAMissing), sqliteio.Text(segment.ID))
			case "projected-confirmed":
				bad := asQAJSON(t, segment)
				bad.Confirmed = bad.Confirmed.Add(time.Nanosecond)
				_, err := sqliteWriteSegmentLocal(o.tx, m.ComputerID, &segment, bad)
				bgQAFixtureError(t, "locally valid projection mismatch", err)
			}
			aorQARefusal(t, o.tx, m, a.Ref.Key)
			stQAClose(t, o, false)
			sdQAReopen(t, f, m, snapshot, st)
		})
	}
}

func TestSQLiteCaptureActorOwnedRowUnresolvedClosure(t *testing.T) {
	_, raw, _, _ := ueQALegacy(t, "unresolved")
	f, m, snapshot, st := sdQASeed(t, raw)
	u := sdQAFirstU(t, st)
	a, segment := asQARows(t, st)
	if u.State != "unresolved" || u.UpperBound != nil || segment.End != nil || segment.ID != u.SegmentID || segment.UncertaintyID == nil || *segment.UncertaintyID != u.ID || a.Ref != u.Actor {
		t.Fatal("real quarantine must retain the open actor/segment/uncertainty cycle")
	}
	for _, axis := range []string{"missing-evidence", "lower-bound", "uncertainty-backref"} {
		t.Run(axis, func(t *testing.T) {
			o := aorQAOpen(t, context.Background(), f, sqliteio.Write)
			aorQAPositive(t, o.tx, m, a)
			switch axis {
			case "missing-evidence":
				interopDone(t, o.tx, "DELETE FROM uncertainty_evidence WHERE uncertainty_id=?", sqliteio.Text(u.ID))
			case "lower-bound":
				bad := ueQACloneU(u)
				bad.LowerBound = bad.LowerBound.Add(time.Nanosecond)
				_, err := sqliteWriteUncertainty(o.tx, m.ComputerID, &u, bad)
				bgQAFixtureError(t, "locally valid uncertainty lower bound", err)
			case "uncertainty-backref":
				bad := ueQACloneU(u)
				bad.SegmentID = sdQAMissing
				_, err := sqliteWriteUncertainty(o.tx, m.ComputerID, &u, bad)
				bgQAFixtureError(t, "locally valid uncertainty back reference", err)
			}
			aorQARefusal(t, o.tx, m, a.Ref.Key)
			stQAClose(t, o, false)
			sdQAReopen(t, f, m, snapshot, st)
		})
	}
}

func TestSQLiteCaptureActorOwnedRowScopeAndNativeErrors(t *testing.T) {
	f, m, snapshot, st, want := aorQAIdle(t)
	for _, ceiling := range []string{"", "0", "01", "18446744073709551616"} {
		t.Run("ceiling="+ceiling, func(t *testing.T) {
			o := aorQAOpen(t, context.Background(), f, sqliteio.Write)
			aorQAPositive(t, o.tx, m, want)
			got, err := sqliteCaptureReadActor(o.tx, m.ComputerID, ceiling, want.Ref.Key)
			if got != nil {
				t.Fatal("invalid ceiling returned actor")
			}
			bgQAValidation(t, err)
			absent := want.Ref.Key
			absent.AgentID = "absent-owned-actor"
			got, err = sqliteCaptureReadActor(o.tx, m.ComputerID, ceiling, absent)
			if got != nil || err != nil {
				t.Fatal("initial absent lookup precedence changed", err)
			}
			sdQAUnchanged(t, o.tx, m, snapshot)
			stQAClose(t, o, false)
			sdQAReopen(t, f, m, snapshot, st)
		})
	}
	for _, axis := range []string{"canceled", "checked-finalize"} {
		t.Run(axis, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			o := aorQAOpen(t, ctx, f, sqliteio.Write)
			aorQAPositive(t, o.tx, m, want)
			fired := false
			if axis == "canceled" {
				cancel()
			} else {
				// Reset is registered after the owner so even an assertion Fatal
				// clears the fault before its checked rollback/close cleanup.
				ncQAHooks(t, ncQASQLHooks{Fault: func(e ncQASQLEvent) error {
					if !fired && e.Phase == "finalize-after" && e.Operation == "statement" && e.Code == 0 {
						fired = true
						return syscall.EIO
					}
					return nil
				}})
			}
			got, err := sqliteCaptureReadActor(o.tx, m.ComputerID, m.Revision, want.Ref.Key)
			ncQASetSQLHooks(ncQASQLHooks{})
			var native *sqliteio.Error
			if got != nil || !errors.As(err, &native) {
				t.Fatal("native error lost or partial actor returned", err)
			}
			if axis == "canceled" {
				if native.Category != sqliteio.Canceled || !errors.Is(err, context.Canceled) {
					t.Fatal("caller cancellation lost", err)
				}
			} else if !fired || native.Phase != sqliteio.FinalizePhase || native.Category != sqliteio.IO {
				t.Fatal("checked finalizer error lost", err)
			}
			stQAClose(t, o, false)
			sdQAReopen(t, f, m, snapshot, st)
		})
	}
}
