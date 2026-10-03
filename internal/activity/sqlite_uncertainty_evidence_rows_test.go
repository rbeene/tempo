//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent local-row QA only. Real complete legacy recovery snapshots supply
// the semantic oracle; literal historical dependencies only satisfy deferred SQL
// FKs. No partial SQL state is passed to validState or a future graph validator.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const ueQAUncertaintyColumns = "uncertainty_id,revision,actor_key,actor_generation,segment_id,account_id,user_id,project_id,task_id,timezone,computer_id,lower_bound_sec,lower_bound_nsec,lower_bound_json,upper_bound_sec,upper_bound_nsec,upper_bound_json,reason,state,resolution_end_sec,resolution_end_nsec,resolution_end_json,discarded"
const ueQAEvidenceColumns = "uncertainty_id,detection_capability,detection_wall_sec,detection_wall_nsec,detection_wall_json,detection_epoch,detection_elapsed_raw,detection_awake_raw,last_confirmed_capability,last_confirmed_wall_sec,last_confirmed_wall_nsec,last_confirmed_wall_json,last_confirmed_epoch,last_confirmed_elapsed_raw,last_confirmed_awake_raw,last_confirmed_elapsed,last_confirmed_awake,bound_capability,bound_wall_sec,bound_wall_nsec,bound_wall_json,bound_epoch,bound_elapsed_raw,bound_awake_raw,bound_elapsed,bound_awake,missing_from,missing_through"
const ueQAEpochColumns = "epoch_id,computer_id,account_id,user_id,project_id,task_id,timezone,creation_ordinal,group_order,anchor_capability,anchor_wall_sec,anchor_wall_nsec,anchor_wall_json,anchor_epoch,anchor_elapsed_raw,anchor_awake_raw,anchor_elapsed,anchor_awake"
const ueQASegmentColumns = "segment_id,actor_key,actor_generation,binding_id,binding_revision,account_id,user_id,project_id,task_id,timezone,computer_id,group_order,epoch_id,start_sample_capability,start_sample_wall_sec,start_sample_wall_nsec,start_sample_wall_json,start_sample_epoch,start_sample_elapsed_raw,start_sample_awake_raw,start_sample_elapsed,start_sample_awake,confirmed_sample_capability,confirmed_sample_wall_sec,confirmed_sample_wall_nsec,confirmed_sample_wall_json,confirmed_sample_epoch,confirmed_sample_elapsed_raw,confirmed_sample_awake_raw,confirmed_sample_elapsed,confirmed_sample_awake,start_sec,start_nsec,start_json,confirmed_sec,confirmed_nsec,confirmed_json,end_sec,end_nsec,end_json,uncertainty_id,finalized"
const ueQAFreshID = "99999999-9999-0999-0999-999999999999"

func ueQACloneU(u Uncertainty) Uncertainty {
	if u.UpperBound != nil {
		v := *u.UpperBound
		u.UpperBound = &v
	}
	if u.ResolutionEnd != nil {
		v := *u.ResolutionEnd
		u.ResolutionEnd = &v
	}
	return u
}
func ueQACloneClock(c ClockSample) ClockSample {
	for _, field := range []**string{&c.Epoch, &c.ElapsedNS, &c.AwakeNS} {
		if *field != nil {
			v := **field
			*field = &v
		}
	}
	return c
}
func ueQACloneE(e uncertaintyEvidence) uncertaintyEvidence {
	e.Detection, e.LastConfirmed = ueQACloneClock(e.Detection), ueQACloneClock(e.LastConfirmed)
	if e.BoundSample != nil {
		c := ueQACloneClock(*e.BoundSample)
		e.BoundSample = &c
	}
	return e
}
func ueQAJSONTime(t *testing.T, wall time.Time) string {
	t.Helper()
	b, err := wall.MarshalJSON()
	if err != nil {
		t.Fatal("fixture time JSON encoding failed")
	}
	return string(b)
}
func ueQAChargeU(t *testing.T, u Uncertainty) int64 {
	t.Helper()
	a := u.Attribution
	n := int64(207)
	for _, s := range []string{u.ID, actorKey(u.Actor.Key), u.SegmentID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, u.Actor.Key.ComputerID, ueQAJSONTime(t, u.LowerBound), u.Reason, u.State} {
		n += int64(len(bgQAPersistedString(t, s)))
	}
	for _, bound := range []*time.Time{u.UpperBound, u.ResolutionEnd} {
		if bound != nil {
			n += 24 + int64(len(ueQAJSONTime(t, *bound)))
		}
	}
	return n
}
func ueQAClockTexts(t *testing.T, c ClockSample) []string {
	t.Helper()
	return []string{c.Capability, ueQAJSONTime(t, c.WallUTC), *c.Epoch, *c.ElapsedNS, *c.AwakeNS}
}
func ueQAChargeE(t *testing.T, id string, e uncertaintyEvidence) int64 {
	t.Helper()
	n := int64(204)
	texts := []string{id, e.Detection.Capability, ueQAJSONTime(t, e.Detection.WallUTC), e.LastConfirmed.Capability, ueQAJSONTime(t, e.LastConfirmed.WallUTC), *e.LastConfirmed.Epoch, *e.LastConfirmed.ElapsedNS, *e.LastConfirmed.AwakeNS, e.MissingFrom, e.MissingThrough}
	for _, s := range texts {
		n += int64(len(bgQAPersistedString(t, s)))
	}
	for _, s := range []*string{e.Detection.Epoch, e.Detection.ElapsedNS, e.Detection.AwakeNS} {
		if s != nil {
			n += 8 + int64(len(bgQAPersistedString(t, *s)))
		}
	}
	if e.BoundSample != nil {
		n += 88
		for _, s := range ueQAClockTexts(t, *e.BoundSample) {
			n += int64(len(bgQAPersistedString(t, s)))
		}
	}
	return n
}

func ueQALegacy(t *testing.T, shape string) (*qaHarness, *state, Uncertainty, uncertaintyEvidence) {
	t.Helper()
	h, u := qaRecoveryUncertain(t)
	if shape == "bounded" {
		h.ingest(900, qaEvent("A", "1", "3", "wait_user", ""))
	}
	if shape == "resolved" || shape == "discarded" {
		in := ResolveInput{UncertaintyID: u.ID, IfRevision: u.Revision, RequestID: qaRecoveryRequest, Reason: "owned verified recovery fixture", Confirmed: true}
		if shape == "discarded" {
			in.DiscardTail = true
			h.clockErr = errors.New("synthetic unavailable")
		} else {
			in.End = qaRecoveryTime(1200)
		}
		_, err := h.service.Resolve(context.Background(), in)
		bgQAFixtureError(t, "uncertainty-real-resolve-fixture", err)
	}
	st := bgQAReadLegacy(t, h.service)
	return h, st, ueQACloneU(*st.Uncertainties[u.ID]), ueQACloneE(st.UncertaintyEvidence[u.ID])
}

// Literal dependency fixtures, never an importer or production row mapper.
func ueQAUint(t *testing.T, raw string) sqliteio.Value {
	t.Helper()
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatal("fixture counter parse failed")
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return sqliteio.Blob(b[:])
}
func ueQATimeValues(t *testing.T, wall time.Time) []sqliteio.Value {
	t.Helper()
	text := ueQAJSONTime(t, wall)
	var saved time.Time
	if err := saved.UnmarshalJSON([]byte(text)); err != nil {
		t.Fatal("fixture time oracle failed")
	}
	return []sqliteio.Value{sqliteio.Integer(saved.Unix()), sqliteio.Integer(int64(saved.Nanosecond())), sqliteio.Text(text)}
}
func ueQAOptionalTime(t *testing.T, wall *time.Time) []sqliteio.Value {
	if wall == nil {
		return []sqliteio.Value{sqliteio.Null(), sqliteio.Null(), sqliteio.Null()}
	}
	return ueQATimeValues(t, *wall)
}
func ueQATextOptional(s *string) sqliteio.Value {
	if s == nil {
		return sqliteio.Null()
	}
	return sqliteio.Text(*s)
}
func ueQAClockValues(t *testing.T, c ClockSample) []sqliteio.Value {
	values := []sqliteio.Value{sqliteio.Text(c.Capability)}
	values = append(values, ueQATimeValues(t, c.WallUTC)...)
	return append(values, sqliteio.Text(*c.Epoch), sqliteio.Text(*c.ElapsedNS), sqliteio.Text(*c.AwakeNS), ueQAUint(t, *c.ElapsedNS), ueQAUint(t, *c.AwakeNS))
}
func ueQAGroup(t *testing.T, computer string, a Attribution) string {
	b, err := json.Marshal(a)
	if err != nil {
		t.Fatal("fixture attribution JSON failed")
	}
	return computer + "/" + string(b)
}
func ueQAGeneration(t *testing.T, tx *sqliteio.Tx, ref ActorRef) {
	interopDone(t, tx, "INSERT INTO actor_generations(actor_key,generation,computer_id,source,session_id,agent_id) VALUES(?,?,?,?,?,?)", sqliteio.Text(actorKey(ref.Key)), ueQAUint(t, ref.Generation), sqliteio.Text(ref.Key.ComputerID), sqliteio.Text(ref.Key.Source), sqliteio.Text(ref.Key.SessionID), sqliteio.Text(ref.Key.AgentID))
}
func ueQADependencies(t *testing.T, tx *sqliteio.Tx, st *state, u Uncertainty) {
	t.Helper()
	ueQAGeneration(t, tx, u.Actor)
	for ordinal, ep := range st.Epochs {
		a := ep.Attribution
		v := []sqliteio.Value{sqliteio.Text(ep.ID), sqliteio.Text(ep.ComputerID), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID), sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Integer(int64(ordinal)), sqliteio.Text(ueQAGroup(t, ep.ComputerID, a))}
		v = append(v, ueQAClockValues(t, ep.Anchor)...)
		interopDone(t, tx, "INSERT INTO epochs("+ueQAEpochColumns+") VALUES("+strings.TrimSuffix(strings.Repeat("?,", 18), ",")+")", v...)
	}
	seg := st.Segments[u.SegmentID]
	a := seg.Binding.Attribution
	v := []sqliteio.Value{sqliteio.Text(seg.ID), sqliteio.Text(actorKey(seg.Actor.Key)), ueQAUint(t, seg.Actor.Generation), sqliteio.Text(seg.Binding.ID), ueQAUint(t, seg.Binding.Revision), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID), sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Text(seg.Actor.Key.ComputerID), sqliteio.Text(ueQAGroup(t, seg.Actor.Key.ComputerID, a)), sqliteio.Text(seg.EpochID)}
	v = append(v, ueQAClockValues(t, seg.StartSample)...)
	v = append(v, ueQAClockValues(t, seg.ConfirmedSample)...)
	v = append(v, ueQATimeValues(t, seg.Start)...)
	v = append(v, ueQATimeValues(t, seg.Confirmed)...)
	v = append(v, ueQAOptionalTime(t, seg.End)...)
	finalized := int64(0)
	if seg.Finalized {
		finalized = 1
	}
	v = append(v, ueQATextOptional(seg.UncertaintyID), sqliteio.Integer(finalized))
	if len(v) != 42 {
		t.Fatal("literal segment dependency width differs")
	}
	interopDone(t, tx, "INSERT INTO segments("+ueQASegmentColumns+") VALUES("+strings.TrimSuffix(strings.Repeat("?,", 42), ",")+")", v...)
}

func ueQAStored(t *testing.T, tx *sqliteio.Tx) int64 {
	t.Helper()
	var total int64
	for _, q := range []string{"SELECT singleton,schema_version,legacy_schema_version,computer_id,revision,sync_enabled,state_basename,database_basename,migration_id,backup_sha256 FROM store_meta", "SELECT actor_key,generation,computer_id,source,session_id,agent_id FROM actor_generations", "SELECT " + ueQAEpochColumns + " FROM epochs", "SELECT " + ueQASegmentColumns + " FROM segments", "SELECT " + ueQAUncertaintyColumns + " FROM uncertainties", "SELECT " + ueQAEvidenceColumns + " FROM uncertainty_evidence"} {
		s := interopPrepare(t, tx, q)
		for {
			row, err := s.Step()
			if err != nil {
				t.Fatal("independent stored charge step failed")
			}
			if !row {
				break
			}
			total += 32
			for i := 0; i < s.ColumnCount(); i++ {
				kind, err := s.Kind(i)
				if err != nil {
					t.Fatal("independent kind read failed")
				}
				total++
				switch kind {
				case sqliteio.NullKind:
				case sqliteio.IntegerKind:
					total += 8
				case sqliteio.TextKind:
					v, err := s.Text(i)
					if err != nil {
						t.Fatal("independent text read failed")
					}
					total += 8 + int64(len(v))
				case sqliteio.BlobKind:
					v, err := s.Blob(i)
					if err != nil {
						t.Fatal("independent blob read failed")
					}
					total += 8 + int64(len(v))
				default:
					t.Fatal("unhandled stored kind")
				}
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal("independent stored charge close failed")
		}
	}
	return total
}
func ueQASeed(t *testing.T, st *state, u Uncertainty, e uncertaintyEvidence) (interopFixture, sqliteStoreMeta) {
	t.Helper()
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal("schema bootstrap failed")
	}
	ueQADependencies(t, tx, st, u)
	if delta, err := sqliteWriteUncertainty(tx, st.ComputerID, nil, u); err != nil || delta != ueQAChargeU(t, u) {
		t.Fatal("uncertainty insert/independent charge differs", err)
	}
	if delta, err := sqliteWriteUncertaintyEvidence(tx, u.ID, nil, e); err != nil || delta != ueQAChargeE(t, u.ID, e) {
		t.Fatal("evidence insert/independent charge differs", err)
	}
	m := interopMeta(f)
	m.ComputerID, m.Revision, m.SyncEnabled = st.ComputerID, st.Revision, st.SyncEnabled
	m.LogicalBytes = ueQAStored(t, tx) + int64(114+len(m.ComputerID)+len(f.authority)+len(f.database))
	if err := sqliteInsertMeta(tx, m); err != nil {
		t.Fatal("final metadata bootstrap failed")
	}
	if ueQAStored(t, tx) != m.LogicalBytes {
		t.Fatal("bootstrap charge differs")
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal("literal historical dependency FK audit failed")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return f, m
}
func ueQAReadPair(t *testing.T, tx *sqliteio.Tx, computer string, u Uncertainty, e uncertaintyEvidence) {
	t.Helper()
	got, found, err := sqliteReadUncertaintyScalar(tx, computer, u.ID)
	if err != nil || !found || !reflect.DeepEqual(got, u) {
		t.Fatal("local uncertainty differs", err)
	}
	actual, found, err := sqliteReadUncertaintyEvidenceScalar(tx, u.ID)
	if err != nil || !found || !reflect.DeepEqual(actual, e) {
		t.Fatal("local evidence differs", err)
	}
}
func ueQAReopen(t *testing.T, f interopFixture, m sqliteStoreMeta, u Uncertainty, e uncertaintyEvidence) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	ueQAReadPair(t, tx, m.ComputerID, u, e)
	got, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(got, m) || ueQAStored(t, tx) != m.LogicalBytes {
		t.Fatal("cold metadata/nonce/charge differs")
	}
	if interopCount(t, tx, "SELECT count(*) FROM uncertainties") != 1 || interopCount(t, tx, "SELECT count(*) FROM uncertainty_evidence") != 1 || interopCount(t, tx, "SELECT count(*) FROM actors") != 0 {
		t.Fatal("local row history/current-head count changed")
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteUncertaintyEvidenceActualLegacyLocalRoundtripKindsChargeAndOwnership(t *testing.T) {
	for _, shape := range []string{"unbounded", "bounded", "resolved", "discarded"} {
		t.Run(shape, func(t *testing.T) {
			_, st, u, e := ueQALegacy(t, shape)
			f, m := ueQASeed(t, st, u, e)
			ueQAReopen(t, f, m, u, e)
			if charge, err := sqliteUncertaintyCharge(u); err != nil || charge != ueQAChargeU(t, u) {
				t.Fatal("literal207 uncertainty charge differs", err)
			}
			if charge, err := sqliteUncertaintyEvidenceCharge(u.ID, e); err != nil || charge != ueQAChargeE(t, u.ID, e) {
				t.Fatal("literal204 evidence charge differs", err)
			}
			c, tx := interopOpen(t, f, false, sqliteio.Read)
			for _, table := range []string{"uncertainties", "uncertainty_evidence"} {
				columns := ueQAUncertaintyColumns
				if table == "uncertainty_evidence" {
					columns = ueQAEvidenceColumns
				}
				s := interopPrepare(t, tx, "SELECT "+columns+" FROM "+table+" WHERE uncertainty_id=?", sqliteio.Text(u.ID))
				if row, err := s.Step(); err != nil || !row {
					t.Fatal("literal projection missing")
				}
				for i := 0; i < s.ColumnCount(); i++ {
					want := sqliteio.TextKind
					if table == "uncertainties" {
						if i == 1 || i == 3 {
							want = sqliteio.BlobKind
						}
						if i == 11 || i == 12 || i == 14 || i == 15 || i == 19 || i == 20 || i == 22 {
							want = sqliteio.IntegerKind
						}
						if i >= 14 && i <= 16 && u.UpperBound == nil || i >= 19 && i <= 21 && u.ResolutionEnd == nil {
							want = sqliteio.NullKind
						}
					} else {
						if i == 2 || i == 3 || i == 9 || i == 10 || i == 18 || i == 19 {
							want = sqliteio.IntegerKind
						}
						if i == 15 || i == 16 || i == 24 || i == 25 {
							want = sqliteio.BlobKind
						}
						if i == 5 && e.Detection.Epoch == nil || i == 6 && e.Detection.ElapsedNS == nil || i == 7 && e.Detection.AwakeNS == nil || i >= 17 && i <= 25 && e.BoundSample == nil {
							want = sqliteio.NullKind
						}
					}
					kind, err := s.Kind(i)
					if err != nil || kind != want {
						t.Fatalf("%s column%d wrong kind", table, i)
					}
					if want == sqliteio.BlobKind {
						b, err := s.Blob(i)
						if err != nil || len(b) != 8 {
							t.Fatal("counter is not BLOB8")
						}
					}
				}
				if row, err := s.Step(); err != nil || row {
					t.Fatal("projection not singleton/DONE")
				}
				if err := s.Close(); err != nil {
					t.Fatal("projection close failed")
				}
			}
			got, found, err := sqliteReadUncertaintyScalar(tx, m.ComputerID, u.ID)
			if err != nil || !found {
				t.Fatal("ownership uncertainty read failed")
			}
			if got.UpperBound != nil {
				*got.UpperBound = got.UpperBound.Add(time.Hour)
			}
			if got.ResolutionEnd != nil {
				*got.ResolutionEnd = got.ResolutionEnd.Add(time.Hour)
			}
			actual, found, err := sqliteReadUncertaintyEvidenceScalar(tx, u.ID)
			if err != nil || !found {
				t.Fatal("ownership evidence read failed")
			}
			*actual.LastConfirmed.Epoch = "changed-result"
			if actual.Detection.Epoch != nil {
				*actual.Detection.Epoch = "changed-detection"
			}
			if actual.BoundSample != nil {
				*actual.BoundSample.ElapsedNS = "1"
			}
			ueQAReadPair(t, tx, m.ComputerID, u, e)
			if got, found, err := sqliteReadUncertaintyScalar(tx, m.ComputerID, ueQAFreshID); err != nil || found || !reflect.DeepEqual(got, Uncertainty{}) {
				t.Fatal("uncertainty absence is not zero,false,nil")
			}
			if got, found, err := sqliteReadUncertaintyEvidenceScalar(tx, ueQAFreshID); err != nil || found || !reflect.DeepEqual(got, uncertaintyEvidence{}) {
				t.Fatal("evidence absence is not zero,false,nil")
			}
			bgQAPlan(t, tx, "SELECT "+ueQAUncertaintyColumns+" FROM uncertainties WHERE uncertainty_id=?", "sqlite_autoindex_uncertainties_1", sqliteio.Text(u.ID))
			bgQAPlan(t, tx, "SELECT "+ueQAEvidenceColumns+" FROM uncertainty_evidence WHERE uncertainty_id=?", "sqlite_autoindex_uncertainty_evidence_1", sqliteio.Text(u.ID))
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}

func TestSQLiteUncertaintyEvidencePermissiveDetectionAndBoundLegacyOracle(t *testing.T) {
	for _, which := range []string{"nil-raw", "present-empty-raw", "unchecked-raw", "malformed-text", "offset-and-bound-not-upper"} {
		t.Run(which, func(t *testing.T) {
			h, st, u, e := ueQALegacy(t, "bounded")
			switch which {
			case "nil-raw":
				e.Detection.Epoch, e.Detection.ElapsedNS, e.Detection.AwakeNS = nil, nil, nil
			case "present-empty-raw":
				v := ""
				e.Detection.Epoch, e.Detection.ElapsedNS, e.Detection.AwakeNS = &v, &v, &v
			case "unchecked-raw":
				epoch, elapsed, awake := "private\x00epoch", "001", "18446744073709551616"
				e.Detection.Capability = "arbitrary\x00capability"
				e.Detection.Epoch, e.Detection.ElapsedNS, e.Detection.AwakeNS = &epoch, &elapsed, &awake
				e.MissingFrom, e.MissingThrough = "opaque\x00from", "not-a-counter"
			case "malformed-text":
				bad := string([]byte{0xff, 0xfe})
				e.Detection.Capability = "raw" + bad
				e.Detection.Epoch = &bad
				e.Detection.ElapsedNS = &bad
				e.Detection.AwakeNS = &bad
				e.MissingFrom, e.MissingThrough = bad, "through"+bad
			case "offset-and-bound-not-upper":
				e.Detection.WallUTC = time.Date(2026, 10, 3, 12, 34, 56, 123456789, time.FixedZone("fixture offset", 20700))
				e.BoundSample.WallUTC = e.BoundSample.WallUTC.Add(13 * time.Second)
				boot := "other-valid-bound-boot"
				e.BoundSample.Epoch = &boot
			}
			st.UncertaintyEvidence[u.ID] = e
			if !validState(st) {
				t.Fatal("complete raw legacy permissive control invalid")
			}
			saved := bgQALegacyMarshalOracle(t, h.service, h.path, st, true)
			want := saved.UncertaintyEvidence[u.ID]
			f, m := ueQASeed(t, saved, *saved.Uncertainties[u.ID], want)
			// Exercise encoding from raw caller values, not just repaired imports.
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			before := ueQACloneE(want)
			original := ueQACloneE(e)
			delta, err := sqliteWriteUncertaintyEvidence(tx, u.ID, &before, e)
			if err != nil || delta != ueQAChargeE(t, u.ID, e)-ueQAChargeE(t, u.ID, before) || !reflect.DeepEqual(e, original) {
				t.Fatal("permissive persistence boundary differs or changed caller", err)
			}
			ueQAReadPair(t, tx, m.ComputerID, u, want)
			interopRollback(t, tx)
			interopClose(t, c)
			ueQAReopen(t, f, m, u, want)
		})
	}
}

func TestSQLiteUncertaintyEvidenceSignedDeltaStagingCommitAndRollback(t *testing.T) {
	_, st, u, e := ueQALegacy(t, "unbounded")
	f, m := ueQASeed(t, st, u, e)
	bounded := ueQACloneU(u)
	upper := u.LowerBound.Add(90 * time.Second)
	bounded.UpperBound = &upper
	withBound := ueQACloneE(e)
	sample := ueQACloneClock(e.LastConfirmed)
	sample.WallUTC = sample.WallUTC.Add(90 * time.Second)
	elapsed, awake := "390000000000", "390000000000"
	sample.ElapsedNS, sample.AwakeNS = &elapsed, &awake
	withBound.BoundSample = &sample
	for _, commit := range []bool{false, true} {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		originalU, originalE := ueQACloneU(bounded), ueQACloneE(withBound)
		// Evidence can be staged before the owner bound. Its local scalar read
		// cannot require the peer row's final cyclic relationship yet.
		de, err := sqliteWriteUncertaintyEvidence(tx, u.ID, &e, withBound)
		if err != nil || de != ueQAChargeE(t, u.ID, withBound)-ueQAChargeE(t, u.ID, e) || de <= 0 {
			t.Fatal("positive evidence delta/staging refused", err)
		}
		if got, found, err := sqliteReadUncertaintyEvidenceScalar(tx, u.ID); err != nil || !found || !reflect.DeepEqual(got, withBound) {
			t.Fatal("local staged evidence demanded full graph")
		}
		du, err := sqliteWriteUncertainty(tx, m.ComputerID, &u, bounded)
		if err != nil || du != ueQAChargeU(t, bounded)-ueQAChargeU(t, u) || du <= 0 {
			t.Fatal("positive uncertainty delta/staging refused", err)
		}
		if !reflect.DeepEqual(bounded, originalU) || !reflect.DeepEqual(withBound, originalE) {
			t.Fatal("writer changed owned after pointers")
		}
		ueQAReadPair(t, tx, m.ComputerID, bounded, withBound)
		if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, m) {
			t.Fatal("row writers spent metadata revision/nonce")
		}
		if !commit {
			interopRollback(t, tx)
			interopClose(t, c)
			ueQAReopen(t, f, m, u, e)
			continue
		}
		next := metaQANext(t, m)
		next.Revision, next.LogicalBytes = bump(m.Revision), m.LogicalBytes+du+de
		if err := sqliteUpdateMeta(tx, m, next); err != nil {
			t.Fatal("caller +1 metadata composition refused")
		}
		if err := tx.CheckForeignKeys(); err != nil {
			t.Fatal("final local FK composition refused")
		}
		interopCommit(t, tx)
		interopClose(t, c)
		ueQAReopen(t, f, next, bounded, withBound)
		m = next
	}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	du, err := sqliteWriteUncertainty(tx, m.ComputerID, &bounded, u)
	if err != nil || du != ueQAChargeU(t, u)-ueQAChargeU(t, bounded) || du >= 0 {
		t.Fatal("negative uncertainty delta refused", err)
	}
	de, err := sqliteWriteUncertaintyEvidence(tx, u.ID, &withBound, e)
	if err != nil || de != ueQAChargeE(t, u.ID, e)-ueQAChargeE(t, u.ID, withBound) || de >= 0 {
		t.Fatal("negative evidence delta refused", err)
	}
	if delta, err := sqliteWriteUncertainty(tx, m.ComputerID, &u, u); err != nil || delta != 0 {
		t.Fatal("same-revision equal row imposed revision policy")
	}
	if delta, err := sqliteWriteUncertaintyEvidence(tx, u.ID, &e, e); err != nil || delta != 0 {
		t.Fatal("equal evidence replacement mischarged")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	ueQAReopen(t, f, m, bounded, withBound)
}

func TestSQLiteUncertaintyEvidenceValidFullOldCASAndNullableGroups(t *testing.T) {
	_, st, u, e := ueQALegacy(t, "resolved")
	f, m := ueQASeed(t, st, u, e)
	for _, axis := range []string{"revision", "generation", "actor-key", "segment-id", "account", "user", "project", "task", "timezone", "lower-time", "lower-nsec", "upper-nil", "upper-time", "upper-nsec", "resolution-time", "resolution-nsec", "state-and-resolution-nil", "discarded", "reason"} {
		t.Run("uncertainty/"+axis, func(t *testing.T) {
			actual := ueQACloneU(u)
			switch axis {
			case "revision":
				actual.Revision = bump(u.Revision)
			case "generation":
				actual.Actor.Generation = "2"
			case "actor-key":
				actual.Actor.Key.AgentID = "other-retained-historical-agent"
			case "segment-id":
				actual.SegmentID = ueQAFreshID
			case "account":
				actual.Attribution.AccountID = "5"
			case "user":
				actual.Attribution.UserID = "6"
			case "project":
				actual.Attribution.ProjectID = "7"
			case "task":
				actual.Attribution.TaskID = "8"
			case "timezone":
				actual.Attribution.Timezone = "America/New_York"
			case "lower-time":
				actual.LowerBound = actual.LowerBound.Add(time.Second)
			case "lower-nsec":
				actual.LowerBound = actual.LowerBound.Add(time.Nanosecond)
			case "upper-nil":
				actual.UpperBound = nil
			case "upper-time":
				*actual.UpperBound = actual.UpperBound.Add(time.Second)
			case "upper-nsec":
				*actual.UpperBound = actual.UpperBound.Add(time.Nanosecond)
			case "resolution-time":
				*actual.ResolutionEnd = actual.ResolutionEnd.Add(time.Second)
			case "resolution-nsec":
				*actual.ResolutionEnd = actual.ResolutionEnd.Add(time.Nanosecond)
			case "state-and-resolution-nil":
				actual.State = "unresolved"
				actual.ResolutionEnd = nil
				actual.Discarded = false
			case "discarded":
				actual.Discarded = !actual.Discarded
			case "reason":
				actual.Reason = "source_lost"
			}
			if reflect.DeepEqual(actual, u) {
				t.Fatal("uncertainty stale-CAS axis did not change baseline")
			}
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if actual.Actor != u.Actor {
				ueQAGeneration(t, tx, actual.Actor)
			}
			if axis == "segment-id" {
				interopDone(t, tx, "INSERT INTO segments("+ueQASegmentColumns+") SELECT ?,"+strings.Join(strings.Split(ueQASegmentColumns, ",")[1:], ",")+" FROM segments WHERE segment_id=?", sqliteio.Text(actual.SegmentID), sqliteio.Text(u.SegmentID))
			}
			if delta, err := sqliteWriteUncertainty(tx, m.ComputerID, &u, actual); err != nil || delta != ueQAChargeU(t, actual)-ueQAChargeU(t, u) {
				t.Fatal("otherwise-valid scalar CAS control refused", err)
			}
			if got, found, err := sqliteReadUncertaintyScalar(tx, m.ComputerID, u.ID); err != nil || !found || !reflect.DeepEqual(got, actual) {
				t.Fatal("valid changed row cannot be decoded locally")
			}
			proposal := ueQACloneU(u)
			proposal.Reason = "ordering_unavailable"
			old := ueQACloneU(u)
			owned := ueQACloneU(proposal)
			delta, err := sqliteWriteUncertainty(tx, m.ComputerID, &old, proposal)
			bgQACorrupt(t, err)
			if delta != 0 || !reflect.DeepEqual(old, u) || !reflect.DeepEqual(proposal, owned) {
				t.Fatal("stale full-old CAS returned charge or changed caller")
			}
			if got, found, err := sqliteReadUncertaintyScalar(tx, m.ComputerID, u.ID); err != nil || !found || !reflect.DeepEqual(got, actual) {
				t.Fatal("stale CAS changed valid current row")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			ueQAReopen(t, f, m, u, e)
		})
	}
	for _, axis := range []string{"detection-capability", "detection-wall", "detection-wall-nsec", "detection-epoch-nil", "detection-elapsed-nil", "detection-awake-nil", "last-epoch", "last-wall-second", "last-wall-nsec", "last-elapsed", "last-awake", "bound-nil", "bound-epoch", "bound-wall-second", "bound-wall-nsec", "bound-elapsed", "bound-awake", "missing-from", "missing-through"} {
		t.Run("evidence/"+axis, func(t *testing.T) {
			actual := ueQACloneE(e)
			switch axis {
			case "detection-capability":
				actual.Detection.Capability = "raw-any-capability"
			case "detection-wall":
				actual.Detection.WallUTC = actual.Detection.WallUTC.Add(time.Second)
			case "detection-wall-nsec":
				actual.Detection.WallUTC = actual.Detection.WallUTC.Add(time.Nanosecond)
			case "detection-epoch-nil":
				if actual.Detection.Epoch == nil {
					v := "permissive-raw-epoch"
					actual.Detection.Epoch = &v
				} else {
					actual.Detection.Epoch = nil
				}
			case "detection-elapsed-nil":
				if actual.Detection.ElapsedNS == nil {
					v := "001"
					actual.Detection.ElapsedNS = &v
				} else {
					actual.Detection.ElapsedNS = nil
				}
			case "detection-awake-nil":
				if actual.Detection.AwakeNS == nil {
					v := "18446744073709551616"
					actual.Detection.AwakeNS = &v
				} else {
					actual.Detection.AwakeNS = nil
				}
			case "last-epoch":
				*actual.LastConfirmed.Epoch = "other-valid-last-boot"
			case "last-wall-second":
				actual.LastConfirmed.WallUTC = actual.LastConfirmed.WallUTC.Add(time.Second)
			case "last-wall-nsec":
				actual.LastConfirmed.WallUTC = actual.LastConfirmed.WallUTC.Add(time.Nanosecond)
			case "last-elapsed":
				*actual.LastConfirmed.ElapsedNS = "300000000001"
			case "last-awake":
				*actual.LastConfirmed.AwakeNS = "300000000001"
			case "bound-nil":
				actual.BoundSample = nil
			case "bound-epoch":
				*actual.BoundSample.Epoch = "other-valid-bound-boot"
			case "bound-wall-second":
				actual.BoundSample.WallUTC = actual.BoundSample.WallUTC.Add(time.Second)
			case "bound-wall-nsec":
				actual.BoundSample.WallUTC = actual.BoundSample.WallUTC.Add(time.Nanosecond)
			case "bound-elapsed":
				*actual.BoundSample.ElapsedNS = "2400000000001"
			case "bound-awake":
				*actual.BoundSample.AwakeNS = "2400000000001"
			case "missing-from":
				actual.MissingFrom = "opaque\x00from"
			case "missing-through":
				actual.MissingThrough = "opaque\x00through"
			}
			if reflect.DeepEqual(actual, e) {
				t.Fatal("evidence stale-CAS axis did not change baseline")
			}
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if delta, err := sqliteWriteUncertaintyEvidence(tx, u.ID, &e, actual); err != nil || delta != ueQAChargeE(t, u.ID, actual)-ueQAChargeE(t, u.ID, e) {
				t.Fatal("otherwise-valid evidence CAS control refused", err)
			}
			if got, found, err := sqliteReadUncertaintyEvidenceScalar(tx, u.ID); err != nil || !found || !reflect.DeepEqual(got, actual) {
				t.Fatal("valid evidence change cannot be decoded locally")
			}
			old := ueQACloneE(e)
			proposal := ueQACloneE(e)
			proposal.MissingThrough = "new-proposal"
			owned := ueQACloneE(proposal)
			delta, err := sqliteWriteUncertaintyEvidence(tx, u.ID, &old, proposal)
			bgQACorrupt(t, err)
			if delta != 0 || !reflect.DeepEqual(old, e) || !reflect.DeepEqual(proposal, owned) {
				t.Fatal("stale evidence CAS returned charge or changed caller")
			}
			if got, found, err := sqliteReadUncertaintyEvidenceScalar(tx, u.ID); err != nil || !found || !reflect.DeepEqual(got, actual) {
				t.Fatal("stale evidence CAS changed current row")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			ueQAReopen(t, f, m, u, e)
		})
	}
}

func TestSQLiteUncertaintyEvidenceUnsignedImportsOffsetsAndAvailableClockLimits(t *testing.T) {
	h, st, u, e := ueQALegacy(t, "bounded")
	u.Revision = "18446744073709551615"
	u.LowerBound = u.LowerBound.In(time.FixedZone("offset lower", 20700))
	upper := u.UpperBound.In(time.FixedZone("offset upper", -25200))
	u.UpperBound = &upper
	st.Uncertainties[u.ID] = &u
	saved := bgQALegacyMarshalOracle(t, h.service, h.path, st, true)
	u = *saved.Uncertainties[u.ID]
	e = saved.UncertaintyEvidence[u.ID]
	f, m := ueQASeed(t, saved, u, e)
	ueQAReopen(t, f, m, u, e)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	actual := ueQACloneU(u)
	actual.Actor.Generation = "18446744073709551615"
	ueQAGeneration(t, tx, actual.Actor)
	if delta, err := sqliteWriteUncertainty(tx, m.ComputerID, &u, actual); err != nil || delta != 0 {
		t.Fatal("full unsigned historical generation import narrowed", err)
	}
	if got, found, err := sqliteReadUncertaintyScalar(tx, m.ComputerID, u.ID); err != nil || !found || !reflect.DeepEqual(got, actual) {
		t.Fatal("unsigned generation with no head lost identity")
	}
	ceiling := ueQACloneE(e)
	*ceiling.LastConfirmed.ElapsedNS = "9223372036854775807"
	*ceiling.LastConfirmed.AwakeNS = "0"
	*ceiling.BoundSample.ElapsedNS = "0"
	*ceiling.BoundSample.AwakeNS = "9223372036854775807"
	if delta, err := sqliteWriteUncertaintyEvidence(tx, u.ID, &e, ceiling); err != nil || delta != ueQAChargeE(t, u.ID, ceiling)-ueQAChargeE(t, u.ID, e) {
		t.Fatal("available clock zero/MaxInt64/Awake>Elapsed domain narrowed", err)
	}
	if got, found, err := sqliteReadUncertaintyEvidenceScalar(tx, u.ID); err != nil || !found || !reflect.DeepEqual(got, ceiling) {
		t.Fatal("available clock local domain cannot roundtrip")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	ueQAReopen(t, f, m, u, e)
}

func TestSQLiteUncertaintyEvidenceSelectedKindsAndLocalProjectionCorruption(t *testing.T) {
	_, st, u, e := ueQALegacy(t, "resolved")
	f, m := ueQASeed(t, st, u, e)
	for _, table := range []string{"uncertainties", "uncertainty_evidence"} {
		columns := ueQAUncertaintyColumns
		if table == "uncertainty_evidence" {
			columns = ueQAEvidenceColumns
		}
		// A malformed PK cannot be selected by a valid exact ID. Preserve that
		// absence boundary rather than manufacture an invalid query as corruption.
		for column, name := range strings.Split(columns, ",") {
			if column == 0 {
				continue
			}
			t.Run(table+"/kind/"+name, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				ueQAReadPair(t, tx, m.ComputerID, u, e)
				interopDone(t, tx, "ALTER TABLE "+table+" RENAME TO qa_original_"+table)
				interopDone(t, tx, "CREATE TABLE "+table+"("+columns+")")
				interopDone(t, tx, "INSERT INTO "+table+"("+columns+") SELECT "+columns+" FROM qa_original_"+table)
				// Wrong-kind values keep the selected immutable ID intact. Relaxed
				// tables are decoder-only fixtures, never real-schema admission proof.
				value := sqliteio.Blob([]byte("wrong-text-kind"))
				if strings.HasSuffix(name, "_sec") || strings.HasSuffix(name, "_nsec") || name == "discarded" || name == "revision" || name == "actor_generation" || name == "last_confirmed_elapsed" || name == "last_confirmed_awake" || name == "bound_elapsed" || name == "bound_awake" {
					value = sqliteio.Text("0")
				}
				interopDone(t, tx, "UPDATE "+table+" SET "+name+"=?", value)
				if table == "uncertainties" {
					got, found, err := sqliteReadUncertaintyScalar(tx, m.ComputerID, u.ID)
					bgQACorrupt(t, err)
					if found || !reflect.DeepEqual(got, Uncertainty{}) {
						t.Fatal("wrong-kind uncertainty exposed row")
					}
				} else {
					got, found, err := sqliteReadUncertaintyEvidenceScalar(tx, u.ID)
					bgQACorrupt(t, err)
					if found || !reflect.DeepEqual(got, uncertaintyEvidence{}) {
						t.Fatal("wrong-kind evidence exposed row")
					}
				}
				interopRollback(t, tx)
				interopClose(t, c)
				ueQAReopen(t, f, m, u, e)
			})
		}
	}
	for _, tc := range []struct {
		name, table, sql string
		relax            bool
	}{
		{"revision-width", "uncertainties", "revision=X'01'", true}, {"revision-zero", "uncertainties", "revision=zeroblob(8)", true}, {"actor-width", "uncertainties", "actor_generation=X'01'", true},
		{"reason", "uncertainties", "reason='private-invalid-reason'", true}, {"state", "uncertainties", "state='private-invalid-state'", true}, {"discarded", "uncertainties", "discarded=2", true},
		{"attribution", "uncertainties", "task_id='0'", false}, {"segment-id", "uncertainties", "segment_id='private-invalid-id'", false}, {"zero-lower", "uncertainties", "lower_bound_sec=-62135596800,lower_bound_nsec=0,lower_bound_json='\"0001-01-01T00:00:00Z\"'", false},
		{"lower-time-projection", "uncertainties", "lower_bound_sec=lower_bound_sec+1", false}, {"partial-upper", "uncertainties", "upper_bound_nsec=NULL", true}, {"partial-end", "uncertainties", "resolution_end_json=NULL", true}, {"upper-before-lower", "uncertainties", "upper_bound_sec=0,upper_bound_nsec=0,upper_bound_json='\"1970-01-01T00:00:00Z\"'", true}, {"unresolved-end", "uncertainties", "state='unresolved'", true},
		{"detection-zero-wall", "uncertainty_evidence", "detection_wall_sec=-62135596800,detection_wall_nsec=0,detection_wall_json='\"0001-01-01T00:00:00Z\"'", false}, {"detection-projection", "uncertainty_evidence", "detection_wall_nsec=detection_wall_nsec+1", false},
		{"last-unavailable", "uncertainty_evidence", "last_confirmed_capability='unavailable'", true}, {"last-counter-projection", "uncertainty_evidence", "last_confirmed_elapsed=X'0000000000000001'", false}, {"last-raw-counter", "uncertainty_evidence", "last_confirmed_awake_raw='01'", false}, {"last-clock-width", "uncertainty_evidence", "last_confirmed_awake=X'01'", true},
		{"partial-bound", "uncertainty_evidence", "bound_awake=NULL", true}, {"bound-counter-projection", "uncertainty_evidence", "bound_elapsed=X'0000000000000001'", false}, {"bound-wall-projection", "uncertainty_evidence", "bound_wall_sec=bound_wall_sec+1", false}, {"bound-clock-ceiling", "uncertainty_evidence", "bound_elapsed_raw='9223372036854775808',bound_elapsed=X'8000000000000000'", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			ueQAReadPair(t, tx, m.ComputerID, u, e)
			if tc.relax {
				cols := ueQAUncertaintyColumns
				if tc.table == "uncertainty_evidence" {
					cols = ueQAEvidenceColumns
				}
				interopDone(t, tx, "ALTER TABLE "+tc.table+" RENAME TO qa_original_"+tc.table)
				interopDone(t, tx, "CREATE TABLE "+tc.table+"("+cols+")")
				interopDone(t, tx, "INSERT INTO "+tc.table+"("+cols+") SELECT "+cols+" FROM qa_original_"+tc.table)
			}
			interopDone(t, tx, "UPDATE "+tc.table+" SET "+tc.sql)
			if tc.table == "uncertainties" {
				got, found, err := sqliteReadUncertaintyScalar(tx, m.ComputerID, u.ID)
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(got, Uncertainty{}) {
					t.Fatal("corrupt uncertainty returned row")
				}
			} else {
				got, found, err := sqliteReadUncertaintyEvidenceScalar(tx, u.ID)
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(got, uncertaintyEvidence{}) {
					t.Fatal("corrupt evidence returned row")
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			ueQAReopen(t, f, m, u, e)
		})
	}
	for _, which := range []string{"missing-generation", "mismatched-generation-fields", "extra-uncertainty", "extra-evidence"} {
		t.Run(which, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			ueQAReadPair(t, tx, m.ComputerID, u, e)
			switch which {
			case "missing-generation":
				interopDone(t, tx, "DELETE FROM actor_generations")
			case "mismatched-generation-fields":
				interopDone(t, tx, "UPDATE actor_generations SET session_id='other-session'")
			default:
				table, cols := "uncertainties", ueQAUncertaintyColumns
				if which == "extra-evidence" {
					table, cols = "uncertainty_evidence", ueQAEvidenceColumns
				}
				interopDone(t, tx, "ALTER TABLE "+table+" RENAME TO qa_original_"+table)
				interopDone(t, tx, "CREATE TABLE "+table+"("+cols+")")
				for i := 0; i < 2; i++ {
					interopDone(t, tx, "INSERT INTO "+table+"("+cols+") SELECT "+cols+" FROM qa_original_"+table)
				}
			}
			if which == "extra-evidence" {
				got, found, err := sqliteReadUncertaintyEvidenceScalar(tx, u.ID)
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(got, uncertaintyEvidence{}) {
					t.Fatal("extra evidence returned row")
				}
			} else {
				got, found, err := sqliteReadUncertaintyScalar(tx, m.ComputerID, u.ID)
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(got, Uncertainty{}) {
					t.Fatal("selected generation/extra uncertainty returned row")
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			ueQAReopen(t, f, m, u, e)
		})
	}
}

func TestSQLiteUncertaintyEvidenceInvalidExplicitInputsHaveValidationZeroAndOwnership(t *testing.T) {
	_, st, u, e := ueQALegacy(t, "unbounded")
	f, m := ueQASeed(t, st, u, e)
	for _, axis := range []string{"id", "revision-zero", "revision-noncanonical", "revision-overflow", "actor-generation", "actor-source", "actor-computer", "segment-id", "account", "user", "project", "task", "timezone", "zero-lower", "unencodable-lower", "upper-before-lower", "unencodable-upper", "reason", "state", "unresolved-end", "unresolved-discarded", "resolved-missing-end", "unencodable-end"} {
		t.Run("uncertainty/"+axis, func(t *testing.T) {
			candidate := ueQACloneU(u)
			candidate.ID = ueQAFreshID
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if delta, err := sqliteWriteUncertainty(tx, m.ComputerID, nil, candidate); err != nil || delta != ueQAChargeU(t, candidate) {
				t.Fatal("fresh valid uncertainty insert control failed", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			switch axis {
			case "id":
				candidate.ID = "private-invalid-id"
			case "revision-zero":
				candidate.Revision = "0"
			case "revision-noncanonical":
				candidate.Revision = "01"
			case "revision-overflow":
				candidate.Revision = "18446744073709551616"
			case "actor-generation":
				candidate.Actor.Generation = "0"
			case "actor-source":
				candidate.Actor.Key.Source = "private-invalid-source"
			case "actor-computer":
				candidate.Actor.Key.ComputerID = "private-invalid-computer"
			case "segment-id":
				candidate.SegmentID = "private-invalid-segment"
			case "account":
				candidate.Attribution.AccountID = "0"
			case "user":
				candidate.Attribution.UserID = "01"
			case "project":
				candidate.Attribution.ProjectID = "-1"
			case "task":
				candidate.Attribution.TaskID = "9223372036854775808"
			case "timezone":
				candidate.Attribution.Timezone = "private-invalid-timezone"
			case "zero-lower":
				candidate.LowerBound = time.Time{}
			case "unencodable-lower":
				candidate.LowerBound = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			case "upper-before-lower":
				v := u.LowerBound.Add(-time.Second)
				candidate.UpperBound = &v
			case "unencodable-upper":
				v := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
				candidate.UpperBound = &v
			case "reason":
				candidate.Reason = "private-invalid-reason"
			case "state":
				candidate.State = "private-invalid-state"
			case "unresolved-end":
				v := u.LowerBound
				candidate.ResolutionEnd = &v
			case "unresolved-discarded":
				candidate.Discarded = true
			case "resolved-missing-end":
				candidate.State = "resolved"
			case "unencodable-end":
				candidate.State = "resolved"
				v := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
				candidate.ResolutionEnd = &v
			}
			owned := ueQACloneU(candidate)
			if charge, err := sqliteUncertaintyCharge(candidate); err == nil || charge != 0 {
				t.Fatal("invalid standalone uncertainty charge produced result")
			} else {
				bgQAValidation(t, err)
			}
			c, tx = interopOpen(t, f, false, sqliteio.Write)
			delta, err := sqliteWriteUncertainty(tx, m.ComputerID, nil, candidate)
			bgQAValidation(t, err)
			interopSafeError(t, err, "private-invalid", f.directory)
			if delta != 0 || !reflect.DeepEqual(candidate, owned) {
				t.Fatal("invalid uncertainty insert returned charge or mutated caller")
			}
			if axis != "id" {
				before := ueQACloneU(u)
				candidate.ID = u.ID
				owned = ueQACloneU(candidate)
				delta, err = sqliteWriteUncertainty(tx, m.ComputerID, &before, candidate)
				bgQAValidation(t, err)
				if delta != 0 || !reflect.DeepEqual(before, u) || !reflect.DeepEqual(candidate, owned) {
					t.Fatal("invalid update returned charge or changed owned inputs")
				}
			}
			ueQAReadPair(t, tx, m.ComputerID, u, e)
			interopRollback(t, tx)
			interopClose(t, c)
			ueQAReopen(t, f, m, u, e)
		})
	}
	for _, axis := range []string{"zero-detection-wall", "unencodable-detection-wall", "last-capability", "last-zero-wall", "last-unencodable-wall", "last-nil-epoch", "last-empty-epoch", "last-nil-elapsed", "last-nil-awake", "last-noncanonical-counter", "last-counter-ceiling", "bound-capability", "bound-nil-epoch", "bound-counter-ceiling", "bound-unencodable-wall"} {
		t.Run("evidence/"+axis, func(t *testing.T) {
			candidate := ueQACloneE(e)
			if strings.HasPrefix(axis, "bound-") {
				bound := ueQACloneClock(e.LastConfirmed)
				candidate.BoundSample = &bound
			}
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if delta, err := sqliteWriteUncertaintyEvidence(tx, u.ID, &e, candidate); err != nil || delta != ueQAChargeE(t, u.ID, candidate)-ueQAChargeE(t, u.ID, e) {
				t.Fatal("valid evidence proposal control failed", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			switch axis {
			case "zero-detection-wall":
				candidate.Detection.WallUTC = time.Time{}
			case "unencodable-detection-wall":
				candidate.Detection.WallUTC = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			case "last-capability":
				candidate.LastConfirmed.Capability = "unavailable"
			case "last-zero-wall":
				candidate.LastConfirmed.WallUTC = time.Time{}
			case "last-unencodable-wall":
				candidate.LastConfirmed.WallUTC = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			case "last-nil-epoch":
				candidate.LastConfirmed.Epoch = nil
			case "last-empty-epoch":
				*candidate.LastConfirmed.Epoch = ""
			case "last-nil-elapsed":
				candidate.LastConfirmed.ElapsedNS = nil
			case "last-nil-awake":
				candidate.LastConfirmed.AwakeNS = nil
			case "last-noncanonical-counter":
				*candidate.LastConfirmed.ElapsedNS = "01"
			case "last-counter-ceiling":
				*candidate.LastConfirmed.AwakeNS = "9223372036854775808"
			case "bound-capability":
				candidate.BoundSample.Capability = "unavailable"
			case "bound-nil-epoch":
				candidate.BoundSample.Epoch = nil
			case "bound-counter-ceiling":
				*candidate.BoundSample.ElapsedNS = "9223372036854775808"
			case "bound-unencodable-wall":
				candidate.BoundSample.WallUTC = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			}
			owned := ueQACloneE(candidate)
			if charge, err := sqliteUncertaintyEvidenceCharge(u.ID, candidate); err == nil || charge != 0 {
				t.Fatal("invalid evidence standalone charge produced result")
			} else {
				bgQAValidation(t, err)
			}
			c, tx = interopOpen(t, f, false, sqliteio.Write)
			before := ueQACloneE(e)
			delta, err := sqliteWriteUncertaintyEvidence(tx, u.ID, &before, candidate)
			bgQAValidation(t, err)
			if delta != 0 || !reflect.DeepEqual(candidate, owned) || !reflect.DeepEqual(before, e) {
				t.Fatal("invalid evidence proposal changed owned inputs or returned delta")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			ueQAReopen(t, f, m, u, e)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	for _, computer := range []string{m.ComputerID, "private-invalid-computer"} {
		id := u.ID
		if computer == m.ComputerID {
			id = "private-invalid-id"
		}
		got, found, err := sqliteReadUncertaintyScalar(tx, computer, id)
		bgQAValidation(t, err)
		if found || !reflect.DeepEqual(got, Uncertainty{}) {
			t.Fatal("invalid uncertainty lookup produced result")
		}
	}
	if got, found, err := sqliteReadUncertaintyEvidenceScalar(tx, "private-invalid-id"); err == nil || found || !reflect.DeepEqual(got, uncertaintyEvidence{}) {
		t.Fatal("invalid evidence lookup produced result")
	} else {
		bgQAValidation(t, err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	if charge, err := sqliteUncertaintyEvidenceCharge("private-invalid-id", e); err == nil || charge != 0 {
		t.Fatal("invalid evidence owner charge produced result")
	} else {
		bgQAValidation(t, err)
	}
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	if delta, err := sqliteWriteUncertainty(tx, "private-invalid-computer", &u, u); err == nil || delta != 0 {
		t.Fatal("invalid explicit computer write produced result")
	} else {
		bgQAValidation(t, err)
	}
	for _, before := range []*uncertaintyEvidence{nil, &e} {
		if delta, err := sqliteWriteUncertaintyEvidence(tx, "private-invalid-id", before, e); err == nil || delta != 0 {
			t.Fatal("invalid evidence owner write produced result")
		} else {
			bgQAValidation(t, err)
		}
	}
	interopRollback(t, tx)
	interopClose(t, c)
	ueQAReopen(t, f, m, u, e)
}

func TestSQLiteUncertaintyEvidenceScopeMissingUpdatesAndIndependentIdentityConflicts(t *testing.T) {
	_, st, u, e := ueQALegacy(t, "unbounded")
	f, m := ueQASeed(t, st, u, e)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	foreign := ueQACloneU(u)
	foreign.ID = ueQAFreshID
	foreign.Actor.Key.ComputerID = "aaaaaaaa-aaaa-0aaa-0aaa-aaaaaaaaaaaa"
	ueQAGeneration(t, tx, foreign.Actor)
	if delta, err := sqliteWriteUncertainty(tx, foreign.Actor.Key.ComputerID, nil, foreign); err != nil || delta != ueQAChargeU(t, foreign) {
		t.Fatal("fully valid foreign historical scope insert refused", err)
	}
	if got, found, err := sqliteReadUncertaintyScalar(tx, foreign.Actor.Key.ComputerID, foreign.ID); err != nil || !found || !reflect.DeepEqual(got, foreign) {
		t.Fatal("valid foreign local read control refused")
	}
	got, found, err := sqliteReadUncertaintyScalar(tx, m.ComputerID, foreign.ID)
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(got, Uncertainty{}) {
		t.Fatal("selected foreign row leaked through intended scope")
	}
	local := ueQACloneU(u)
	local.ID = "bbbbbbbb-bbbb-0bbb-0bbb-bbbbbbbbbbbb"
	local.Actor = foreign.Actor
	if delta, err := sqliteWriteUncertainty(tx, m.ComputerID, nil, local); err == nil || delta != 0 {
		t.Fatal("wrong explicit computer admitted foreign Actor")
	} else {
		bgQAValidation(t, err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	ueQAReopen(t, f, m, u, e)
	for _, table := range []string{"uncertainty", "evidence"} {
		t.Run(table, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			fresh := ueQACloneU(u)
			fresh.ID = ueQAFreshID
			if _, err := sqliteWriteUncertainty(tx, m.ComputerID, nil, fresh); err != nil {
				t.Fatal("fresh uncertainty positive control failed")
			}
			if _, err := sqliteWriteUncertaintyEvidence(tx, fresh.ID, nil, e); err != nil {
				t.Fatal("fresh evidence positive control failed")
			}
			var delta int64
			var err error
			if table == "uncertainty" {
				owned := ueQACloneU(u)
				delta, err = sqliteWriteUncertainty(tx, m.ComputerID, nil, u)
				if !reflect.DeepEqual(u, owned) {
					t.Fatal("duplicate insert changed caller")
				}
			} else {
				owned := ueQACloneE(e)
				delta, err = sqliteWriteUncertaintyEvidence(tx, u.ID, nil, e)
				if !reflect.DeepEqual(e, owned) {
					t.Fatal("duplicate evidence insert changed caller")
				}
			}
			bgQAValidation(t, err)
			var native *sqliteio.Error
			if delta != 0 || !errors.As(err, &native) || native.Category != sqliteio.Constraint || (native.Code != 1555 && native.Code != 2067) {
				t.Fatal("identity conflict lost zero/native uniqueness evidence")
			}
			interopSafeError(t, err, u.ID, f.directory)
			interopRollback(t, tx)
			interopClose(t, c)
			ueQAReopen(t, f, m, u, e)
		})
	}
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	missing := ueQACloneU(u)
	missing.ID = ueQAFreshID
	if delta, err := sqliteWriteUncertainty(tx, m.ComputerID, &missing, missing); err == nil || delta != 0 {
		t.Fatal("missing UPDATE became INSERT")
	} else {
		bgQACorrupt(t, err)
	}
	if delta, err := sqliteWriteUncertaintyEvidence(tx, ueQAFreshID, &e, e); err == nil || delta != 0 {
		t.Fatal("missing evidence UPDATE became INSERT")
	} else {
		bgQACorrupt(t, err)
	}
	if delta, err := sqliteWriteUncertainty(tx, m.ComputerID, &u, missing); err == nil || delta != 0 {
		t.Fatal("immutable uncertainty ID rewrite admitted")
	} else {
		bgQAValidation(t, err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	ueQAReopen(t, f, m, u, e)
}

func TestSQLiteUncertaintyEvidenceCallerCancellationAndTerminalAdapterRefusal(t *testing.T) {
	_, st, u, e := ueQALegacy(t, "unbounded")
	f, m := ueQASeed(t, st, u, e)
	for _, op := range []string{"read-uncertainty", "read-evidence", "insert-uncertainty", "update-uncertainty", "insert-evidence", "update-evidence"} {
		t.Run(op, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			c, err := sqliteio.Open(ctx, f.directory, f.database, sqliteio.Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			if err != nil {
				t.Fatal("cancellation fixture open failed")
			}
			t.Cleanup(func() { _ = c.Close(context.Background()) })
			tx, err := c.Begin(ctx, sqliteio.Write)
			if err != nil {
				t.Fatal("cancellation fixture begin failed")
			}
			t.Cleanup(func() { _ = tx.Rollback() })
			fresh := ueQACloneU(u)
			fresh.ID = ueQAFreshID
			if _, err := sqliteWriteUncertainty(tx, m.ComputerID, nil, fresh); err != nil {
				t.Fatal("pre-cancel staged control failed")
			}
			afterU := ueQACloneU(u)
			afterU.Reason = "source_lost"
			afterE := ueQACloneE(e)
			afterE.MissingThrough = "never-admitted-after-cancel"
			ownedU, ownedE := ueQACloneU(afterU), ueQACloneE(afterE)
			cancel()
			var delta int64
			var found bool
			var gotU Uncertainty
			var gotE uncertaintyEvidence
			switch op {
			case "read-uncertainty":
				gotU, found, err = sqliteReadUncertaintyScalar(tx, m.ComputerID, u.ID)
			case "read-evidence":
				gotE, found, err = sqliteReadUncertaintyEvidenceScalar(tx, u.ID)
			case "insert-uncertainty":
				afterU.ID = "bbbbbbbb-bbbb-0bbb-0bbb-bbbbbbbbbbbb"
				ownedU = ueQACloneU(afterU)
				delta, err = sqliteWriteUncertainty(tx, m.ComputerID, nil, afterU)
			case "update-uncertainty":
				delta, err = sqliteWriteUncertainty(tx, m.ComputerID, &u, afterU)
			case "insert-evidence":
				delta, err = sqliteWriteUncertaintyEvidence(tx, fresh.ID, nil, afterE)
			case "update-evidence":
				delta, err = sqliteWriteUncertaintyEvidence(tx, u.ID, &e, afterE)
			}
			if !errors.Is(err, context.Canceled) || delta != 0 || found || !reflect.DeepEqual(gotU, Uncertainty{}) || !reflect.DeepEqual(gotE, uncertaintyEvidence{}) || !reflect.DeepEqual(afterU, ownedU) || !reflect.DeepEqual(afterE, ownedE) {
				t.Fatal("cancellation lost evidence/zero/ownership contract")
			}
			interopSafeError(t, err, f.directory, u.ID)
			if cleanup := tx.Rollback(); cleanup != nil {
				interopSafeError(t, cleanup, f.directory)
			}
			interopClose(t, c)
			ueQAReopen(t, f, m, u, e)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	interopRollback(t, tx)
	if got, found, err := sqliteReadUncertaintyScalar(tx, m.ComputerID, u.ID); err == nil || found || !reflect.DeepEqual(got, Uncertainty{}) {
		t.Fatal("terminal transaction returned usable uncertainty")
	} else {
		var native *sqliteio.Error
		if !errors.As(err, &native) {
			t.Fatal("terminal uncertainty read lost adapter evidence")
		}
	}
	if got, found, err := sqliteReadUncertaintyEvidenceScalar(tx, u.ID); err == nil || found || !reflect.DeepEqual(got, uncertaintyEvidence{}) {
		t.Fatal("terminal transaction returned usable evidence")
	} else {
		var native *sqliteio.Error
		if !errors.As(err, &native) {
			t.Fatal("terminal evidence read lost adapter evidence")
		}
	}
	interopClose(t, c)
	ueQAReopen(t, f, m, u, e)
	for _, table := range []string{"uncertainty", "evidence"} {
		c, tx := interopOpen(t, f, false, sqliteio.Read)
		var delta int64
		var err error
		if table == "uncertainty" {
			after := ueQACloneU(u)
			after.Reason = "source_lost"
			delta, err = sqliteWriteUncertainty(tx, m.ComputerID, &u, after)
		} else {
			after := ueQACloneE(e)
			after.MissingThrough = "unwritable-read-transaction"
			delta, err = sqliteWriteUncertaintyEvidence(tx, u.ID, &e, after)
		}
		var native *sqliteio.Error
		if delta != 0 || !errors.As(err, &native) || native.Category == sqliteio.Constraint {
			t.Fatal("read-mode write refusal lost zero/native evidence")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		ueQAReopen(t, f, m, u, e)
	}
}

func TestSQLiteUncertaintyEvidenceAvailableMalformedClockMatchesActualLegacy(t *testing.T) {
	for _, expanded := range []bool{false, true} {
		t.Run(strconv.FormatBool(expanded), func(t *testing.T) {
			h, st, u, e := ueQALegacy(t, "unbounded")
			f, m := ueQASeed(t, st, u, e)
			boot := "boot-" + string([]byte{0xff, 0xfe})
			if expanded {
				boot = strings.Repeat(string([]byte{0xff}), 256)
			}
			// Change the complete raw legacy coordinate consistently, so available
			// evidence passes the actual graph validator before JSON replacement.
			for _, ep := range st.Epochs {
				ep.Anchor.Epoch = &boot
			}
			for _, seg := range st.Segments {
				seg.StartSample.Epoch = &boot
				seg.ConfirmedSample.Epoch = &boot
			}
			for _, actor := range st.Actors {
				actor.LastEvidence.Epoch = &boot
			}
			candidate := ueQACloneE(e)
			candidate.LastConfirmed.Epoch = &boot
			st.UncertaintyEvidence[u.ID] = candidate
			saved := bgQALegacyMarshalOracle(t, h.service, h.path, st, !expanded)
			owned := ueQACloneE(candidate)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if charge, err := sqliteUncertaintyEvidenceCharge(u.ID, candidate); err != nil || charge != ueQAChargeE(t, u.ID, candidate) {
				t.Fatal("raw-valid available clock charge introduced post-repair refusal", err)
			}
			if delta, err := sqliteWriteUncertaintyEvidence(tx, u.ID, &e, candidate); err != nil || delta != ueQAChargeE(t, u.ID, candidate)-ueQAChargeE(t, u.ID, e) || !reflect.DeepEqual(candidate, owned) {
				t.Fatal("raw-valid available clock writer changed legacy admission/ownership", err)
			}
			actual, found, err := sqliteReadUncertaintyEvidenceScalar(tx, u.ID)
			if expanded {
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(actual, uncertaintyEvidence{}) {
					t.Fatal("expanded stored available clock returned usable evidence")
				}
			} else {
				if err != nil || !found || !reflect.DeepEqual(actual, saved.UncertaintyEvidence[u.ID]) {
					t.Fatal("short malformed available clock differs from actual strict legacy repair")
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			ueQAReopen(t, f, m, u, e)
		})
	}
}
