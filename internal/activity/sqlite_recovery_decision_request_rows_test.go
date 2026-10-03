//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Local decision/proof QA only. Depends on the verified uncertainty/evidence
// checkpoint carrying the unchanged approved ueQA source proposal. Complete
// actual legacy graphs are oracles; partial SQL graphs are never self-validated.

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const rdQAColumns = "uncertainty_id,request_id,previous_revision,resolution_end_sec,resolution_end_nsec,resolution_end_json,discarded,reason,observed_capability,observed_wall_sec,observed_wall_nsec,observed_wall_json,observed_epoch,observed_elapsed_raw,observed_awake_raw,discarded_start_sec,discarded_start_nsec,discarded_start_json,discarded_end_sec,discarded_end_nsec,discarded_end_json"
const rpQAColumns = "request_id,operation,fingerprint,outcome_kind,payload"
const rpQAFreshID = "bbbbbbbb-bbbb-0bbb-0bbb-bbbbbbbbbbbb"

func rdQAClone(d recoveryDecision) recoveryDecision {
	if d.ObservedSample != nil {
		v := ueQACloneClock(*d.ObservedSample)
		d.ObservedSample = &v
	}
	if d.DiscardedSuffix != nil {
		v := *d.DiscardedSuffix
		d.DiscardedSuffix = &v
	}
	return d
}
func rpQAClone(r sqliteResolveRequestRow) sqliteResolveRequestRow {
	if r.Result.EntityRevision != nil {
		v := *r.Result.EntityRevision
		r.Result.EntityRevision = &v
	}
	if r.Result.AffectedIDs != nil {
		v := make([]string, len(r.Result.AffectedIDs))
		copy(v, r.Result.AffectedIDs)
		r.Result.AffectedIDs = v
	}
	return r
}
func rpQAPayload(t *testing.T, r sqliteResolveRequestRow) string {
	t.Helper()
	b, err := json.Marshal(r.Result)
	if err != nil {
		t.Fatal("typed proof oracle JSON failed")
	}
	return string(b)
}
func rdQACharge(t *testing.T, d recoveryDecision) int64 {
	t.Helper()
	n := int64(125)
	for _, v := range []string{d.UncertaintyID, d.RequestID, ueQAJSONTime(t, d.ResolutionEnd), d.Reason} {
		n += int64(len(bgQAPersistedString(t, v)))
	}
	if d.ObservedSample != nil {
		c := d.ObservedSample
		n += 32 + int64(len(bgQAPersistedString(t, c.Capability))) + int64(len(ueQAJSONTime(t, c.WallUTC)))
		for _, v := range []*string{c.Epoch, c.ElapsedNS, c.AwakeNS} {
			if v != nil {
				n += 8 + int64(len(bgQAPersistedString(t, *v)))
			}
		}
	}
	if d.DiscardedSuffix != nil {
		n += 48 + int64(len(ueQAJSONTime(t, d.DiscardedSuffix.Start))) + int64(len(ueQAJSONTime(t, d.DiscardedSuffix.End)))
	}
	return n
}
func rpQACharge(t *testing.T, r sqliteResolveRequestRow) int64 {
	t.Helper()
	return int64(77 + len(r.ID) + len("activity.resolve") + len(r.Fingerprint) + len("mutation_result") + len(rpQAPayload(t, r)))
}
func rdQASource(t *testing.T, shape string) (*qaHarness, *state, Uncertainty, uncertaintyEvidence, recoveryDecision, sqliteResolveRequestRow) {
	t.Helper()
	h, st, u, e := ueQALegacy(t, shape)
	d := rdQAClone(st.RecoveryDecisions[u.ID])
	receipt := st.Requests[d.RequestID]
	if receipt.Operation != "activity.resolve" || receipt.Error != nil || receipt.MutationResult == nil {
		t.Fatal("real recovery fixture lacks exclusive typed resolve receipt")
	}
	r := rpQAClone(sqliteResolveRequestRow{ID: d.RequestID, Fingerprint: receipt.Fingerprint, Result: *receipt.MutationResult})
	return h, st, u, e, d, r
}
func rdQAStored(t *testing.T, tx *sqliteio.Tx) int64 {
	t.Helper()
	var total int64
	for _, q := range []string{"SELECT " + rdQAColumns + " FROM recovery_decisions", "SELECT " + rpQAColumns + " FROM requests"} {
		s := interopPrepare(t, tx, q)
		for {
			row, err := s.Step()
			if err != nil {
				t.Fatal("independent decision/proof charge step failed")
			}
			if !row {
				break
			}
			total += 32
			for i := 0; i < s.ColumnCount(); i++ {
				kind, err := s.Kind(i)
				if err != nil {
					t.Fatal("independent decision/proof kind failed")
				}
				total++
				switch kind {
				case sqliteio.NullKind:
				case sqliteio.IntegerKind:
					total += 8
				case sqliteio.TextKind:
					v, err := s.Text(i)
					if err != nil {
						t.Fatal("independent proof text failed")
					}
					total += 8 + int64(len(v))
				case sqliteio.BlobKind:
					v, err := s.Blob(i)
					if err != nil {
						t.Fatal("independent decision blob failed")
					}
					total += 8 + int64(len(v))
				default:
					t.Fatal("unhandled decision/proof stored kind")
				}
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal("independent decision/proof charge Close failed")
		}
	}
	return total
}
func rdQASeed(t *testing.T, st *state, u Uncertainty, e uncertaintyEvidence, d recoveryDecision, r sqliteResolveRequestRow) (interopFixture, sqliteStoreMeta) {
	t.Helper()
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal("schema bootstrap failed")
	}
	ueQADependencies(t, tx, st, u)
	// Prior verified row APIs supply only literal historical dependencies. No
	// current Actor head or future uncertainty graph validator is introduced.
	if _, err := sqliteWriteUncertainty(tx, st.ComputerID, nil, u); err != nil {
		t.Fatal("prior uncertainty bootstrap failed", err)
	}
	if _, err := sqliteWriteUncertaintyEvidence(tx, u.ID, nil, e); err != nil {
		t.Fatal("prior evidence bootstrap failed", err)
	}
	// The decision may precede its request proof in the same writer transaction.
	if delta, err := sqliteInsertRecoveryDecision(tx, d); err != nil || delta != rdQACharge(t, d) {
		t.Fatal("decision bootstrap/125 charge differs", err)
	}
	if got, found, err := sqliteReadRecoveryDecisionScalar(tx, d.UncertaintyID); err != nil || !found || !reflect.DeepEqual(got, d) {
		t.Fatal("local staged decision demanded its absent request graph", err)
	}
	if delta, err := sqliteInsertResolveRequest(tx, r); err != nil || delta != rpQACharge(t, r) {
		t.Fatal("proof bootstrap/77 charge differs", err)
	}
	m := interopMeta(f)
	m.ComputerID, m.Revision, m.SyncEnabled = st.ComputerID, st.Revision, st.SyncEnabled
	m.LogicalBytes = ueQAStored(t, tx) + rdQAStored(t, tx) + int64(114+len(m.ComputerID)+len(f.authority)+len(f.database))
	if err := sqliteInsertMeta(tx, m); err != nil {
		t.Fatal("one final metadata bootstrap failed")
	}
	if ueQAStored(t, tx)+rdQAStored(t, tx) != m.LogicalBytes {
		t.Fatal("decision/proof aggregate charge differs")
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal("actual decision deferred-FK dependencies absent")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return f, m
}
func rdQAReadPair(t *testing.T, tx *sqliteio.Tx, ceiling string, d recoveryDecision, r sqliteResolveRequestRow) {
	t.Helper()
	got, found, err := sqliteReadRecoveryDecisionScalar(tx, d.UncertaintyID)
	if err != nil || !found || !reflect.DeepEqual(got, d) {
		t.Fatal("local decision differs", err)
	}
	proof, found, err := sqliteReadResolveRequestProof(tx, r.ID, ceiling)
	if err != nil || !found || !reflect.DeepEqual(proof, r) {
		t.Fatal("local typed proof differs", err)
	}
}
func rdQAReopen(t *testing.T, f interopFixture, m sqliteStoreMeta, u Uncertainty, e uncertaintyEvidence, d recoveryDecision, r sqliteResolveRequestRow) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	ueQAReadPair(t, tx, m.ComputerID, u, e)
	rdQAReadPair(t, tx, m.Revision, d, r)
	got, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(got, m) || ueQAStored(t, tx)+rdQAStored(t, tx) != m.LogicalBytes {
		t.Fatal("cold metadata/nonce/charge differs")
	}
	if interopCount(t, tx, "SELECT count(*) FROM recovery_decisions") != 1 || interopCount(t, tx, "SELECT count(*) FROM requests") != 1 || interopCount(t, tx, "SELECT count(*) FROM actors") != 0 {
		t.Fatal("decision/proof history or current-head count changed")
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func rdQAPreSQLValidation(t *testing.T, err error) {
	t.Helper()
	bgQAValidation(t, err)
	var native *sqliteio.Error
	if errors.As(err, &native) && native.Category == sqliteio.Constraint {
		t.Fatal("explicit input guard was masked by native SQL constraint")
	}
}

func rdQARelax(t *testing.T, tx *sqliteio.Tx, table, columns string) {
	t.Helper()
	interopDone(t, tx, "ALTER TABLE "+table+" RENAME TO qa_original_"+table)
	interopDone(t, tx, "CREATE TABLE "+table+"("+columns+")")
	interopDone(t, tx, "INSERT INTO "+table+"("+columns+") SELECT "+columns+" FROM qa_original_"+table)
}

func TestSQLiteRecoveryDecisionRequestActualLegacyKindsChargeOwnershipAndPlans(t *testing.T) {
	for _, shape := range []string{"resolved", "discarded"} {
		t.Run(shape, func(t *testing.T) {
			h, st, u, e, d, r := rdQASource(t, shape)
			saved := bgQALegacyMarshalOracle(t, h.service, h.path, st, true)
			if !reflect.DeepEqual(saved.RecoveryDecisions[u.ID], d) || !reflect.DeepEqual(*saved.Requests[r.ID].MutationResult, r.Result) {
				t.Fatal("real strict legacy source projection changed")
			}
			f, m := rdQASeed(t, saved, u, e, d, r)
			rdQAReopen(t, f, m, u, e, d, r)
			if n, err := sqliteRecoveryDecisionCharge(d); err != nil || n != rdQACharge(t, d) {
				t.Fatal("independent literal125 decision charge differs", err)
			}
			if n, err := sqliteResolveRequestCharge(r); err != nil || n != rpQACharge(t, r) {
				t.Fatal("independent literal77 request charge differs", err)
			}
			c, tx := interopOpen(t, f, false, sqliteio.Read)
			for _, table := range []string{"recovery_decisions", "requests"} {
				cols, key := rdQAColumns, "uncertainty_id"
				id := d.UncertaintyID
				if table == "requests" {
					cols, key, id = rpQAColumns, "request_id", r.ID
				}
				s := interopPrepare(t, tx, "SELECT "+cols+" FROM "+table+" WHERE "+key+"=?", sqliteio.Text(id))
				if row, err := s.Step(); err != nil || !row {
					t.Fatal("literal21/5 projection missing")
				}
				wantCount := 21
				if table == "requests" {
					wantCount = 5
				}
				if s.ColumnCount() != wantCount {
					t.Fatal("literal projection count differs")
				}
				for i := 0; i < wantCount; i++ {
					want := sqliteio.TextKind
					if table == "recovery_decisions" {
						if i == 2 {
							want = sqliteio.BlobKind
						}
						if i == 3 || i == 4 || i == 6 || i == 9 || i == 10 || i == 15 || i == 16 || i == 18 || i == 19 {
							want = sqliteio.IntegerKind
						}
						if i >= 8 && i <= 14 && d.ObservedSample == nil || i >= 15 && d.DiscardedSuffix == nil {
							want = sqliteio.NullKind
						}
						if d.ObservedSample != nil && (i == 12 && d.ObservedSample.Epoch == nil || i == 13 && d.ObservedSample.ElapsedNS == nil || i == 14 && d.ObservedSample.AwakeNS == nil) {
							want = sqliteio.NullKind
						}
					}
					kind, err := s.Kind(i)
					if err != nil || kind != want {
						t.Fatalf("%s column%d wrong storage kind", table, i)
					}
					if want == sqliteio.BlobKind {
						b, err := s.Blob(i)
						if err != nil || len(b) != 8 {
							t.Fatal("previous revision not BLOB8")
						}
					}
				}
				if row, err := s.Step(); err != nil || row {
					t.Fatal("projection not singleton/DONE")
				}
				if err := s.Close(); err != nil {
					t.Fatal("projection checked Close failed")
				}
			}
			got, found, err := sqliteReadRecoveryDecisionScalar(tx, u.ID)
			if err != nil || !found {
				t.Fatal("decision ownership control missing")
			}
			if got.ObservedSample != nil {
				*got.ObservedSample.Epoch = "owned-result-change"
			}
			if got.DiscardedSuffix != nil {
				got.DiscardedSuffix.Start = got.DiscardedSuffix.Start.Add(time.Hour)
			}
			proof, found, err := sqliteReadResolveRequestProof(tx, r.ID, m.Revision)
			if err != nil || !found {
				t.Fatal("proof ownership control missing")
			}
			proof.Result.AffectedIDs[0] = rpQAFreshID
			*proof.Result.EntityRevision = "1"
			rdQAReadPair(t, tx, m.Revision, d, r)
			if got, found, err := sqliteReadRecoveryDecisionScalar(tx, rpQAFreshID); err != nil || found || !reflect.DeepEqual(got, recoveryDecision{}) {
				t.Fatal("decision absence not zero,false,nil")
			}
			if got, found, err := sqliteReadResolveRequestProof(tx, rpQAFreshID, m.Revision); err != nil || found || !reflect.DeepEqual(got, sqliteResolveRequestRow{}) {
				t.Fatal("proof absence not zero,false,nil")
			}
			bgQAPlan(t, tx, "SELECT "+rdQAColumns+" FROM recovery_decisions WHERE uncertainty_id=?", "sqlite_autoindex_recovery_decisions_1", sqliteio.Text(u.ID))
			bgQAPlan(t, tx, "SELECT "+rpQAColumns+" FROM requests WHERE request_id=?", "sqlite_autoindex_requests_1", sqliteio.Text(r.ID))
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}

func TestSQLiteRecoveryDecisionDiscardedRawSevenAndOffsetLegacyOracles(t *testing.T) {
	for _, axis := range []string{"nil", "present-zero-wall-nil-fields", "present-empty-fields", "arbitrary-fields", "malformed-text", "offset-suffix"} {
		t.Run(axis, func(t *testing.T) {
			h, st, u, e, d, r := rdQASource(t, "discarded")
			switch axis {
			case "nil":
				d.ObservedSample = nil
			case "present-zero-wall-nil-fields":
				d.ObservedSample = &ClockSample{}
			case "present-empty-fields":
				v := ""
				d.ObservedSample = &ClockSample{Epoch: &v, ElapsedNS: &v, AwakeNS: &v}
			case "arbitrary-fields":
				epoch, elapsed, awake := "raw\x00epoch", "001", "18446744073709551616"
				d.ObservedSample = &ClockSample{Capability: "arbitrary\x00capability", Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &awake}
			case "malformed-text":
				bad := string([]byte{0xff, 0xfe})
				d.ObservedSample = &ClockSample{Capability: bad, Epoch: &bad, ElapsedNS: &bad, AwakeNS: &bad}
			case "offset-suffix":
				offset := time.FixedZone("fixture offset", 20700)
				d.ResolutionEnd = d.ResolutionEnd.In(offset)
				end := d.ResolutionEnd.Add(90 * time.Second)
				d.DiscardedSuffix = &TimeRange{Start: d.ResolutionEnd, End: end}
				u.ResolutionEnd = &d.ResolutionEnd
				u.UpperBound = &end
				st.Uncertainties[u.ID] = &u
				segment := st.Segments[u.SegmentID]
				segment.End = &d.ResolutionEnd
				st.Segments[u.SegmentID] = segment
				bound := ueQACloneClock(e.LastConfirmed)
				bound.WallUTC = end
				e.BoundSample = &bound
				st.UncertaintyEvidence[u.ID] = e
			}
			st.RecoveryDecisions[u.ID] = d
			if !validState(st) {
				t.Fatal("complete discarded permissive raw source invalid")
			}
			saved := bgQALegacyMarshalOracle(t, h.service, h.path, st, true)
			want := saved.RecoveryDecisions[u.ID]
			savedU := ueQACloneU(*saved.Uncertainties[u.ID])
			savedE := ueQACloneE(saved.UncertaintyEvidence[u.ID])
			f, m := rdQASeed(t, saved, savedU, savedE, want, r)
			// Encode the caller's raw values, while comparing against actual JSON
			// persistence repair. No available-clock validator applies to discard.
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			interopDone(t, tx, "DELETE FROM recovery_decisions")
			owned := rdQAClone(d)
			if n, err := sqliteRecoveryDecisionCharge(d); err != nil || n != rdQACharge(t, d) {
				t.Fatal("discarded raw charge refused", err)
			}
			if delta, err := sqliteInsertRecoveryDecision(tx, d); err != nil || delta != rdQACharge(t, d) || !reflect.DeepEqual(d, owned) {
				t.Fatal("discarded raw insertion/ownership differs", err)
			}
			rdQAReadPair(t, tx, m.Revision, want, r)
			interopRollback(t, tx)
			interopClose(t, c)
			rdQAReopen(t, f, m, savedU, savedE, want, r)
		})
	}
}

func TestSQLiteRecoveryDecisionRequestUnsignedHistoricalProofAndLocalOnlyStaging(t *testing.T) {
	h, st, u, e, d, r := rdQASource(t, "resolved")
	d.PreviousRevision = "9223372036854775808"
	u.Revision = "9223372036854775809"
	st.Uncertainties[u.ID] = &u
	st.RecoveryDecisions[u.ID] = d
	st.Revision = "18446744073709551615"
	r.Result.SnapshotRevision = "9223372036854775810"
	entity := "9223372036854775809"
	r.Result.EntityRevision = &entity
	receipt := st.Requests[r.ID]
	receipt.MutationResult = &r.Result
	st.Requests[r.ID] = receipt
	if !validState(st) {
		t.Fatal("actual complete unsigned historical source invalid")
	}
	saved := bgQALegacyMarshalOracle(t, h.service, h.path, st, true)
	f, m := rdQASeed(t, saved, u, e, d, r)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	rdQAReadPair(t, tx, r.Result.SnapshotRevision, d, r)
	if interopCount(t, tx, "SELECT count(*) FROM actors") != 0 {
		t.Fatal("fixture invented current Actor heads")
	}
	// Snapshot != metadata and entity != a locally staged current U revision
	// remain historical facts. The proof reader has no graph-membership API.
	stagedU := ueQACloneU(u)
	stagedU.Revision = "9223372036854775811"
	if _, err := sqliteWriteUncertainty(tx, m.ComputerID, &u, stagedU); err != nil {
		t.Fatal("local historical revision staging refused")
	}
	rdQAReadPair(t, tx, m.Revision, d, r)
	local := rpQAClone(r)
	local.ID, local.Result.RequestID = rpQAFreshID, rpQAFreshID
	local.Result.AffectedIDs = []string{"cccccccc-cccc-0ccc-0ccc-cccccccccccc"}
	owned := rpQAClone(local)
	if delta, err := sqliteInsertResolveRequest(tx, local); err != nil || delta != rpQACharge(t, local) || !reflect.DeepEqual(local, owned) {
		t.Fatal("local proof incorrectly demanded selected membership/existence", err)
	}
	if got, found, err := sqliteReadResolveRequestProof(tx, local.ID, m.Revision); err != nil || !found || !reflect.DeepEqual(got, local) {
		t.Fatal("local-only proof read demanded graph closure", err)
	}
	low, _ := strconv.ParseUint(r.Result.SnapshotRevision, 10, 64)
	got, found, err := sqliteReadResolveRequestProof(tx, r.ID, strconv.FormatUint(low-1, 10))
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(got, sqliteResolveRequestRow{}) {
		t.Fatal("proof above supplied ceiling returned row")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	rdQAReopen(t, f, m, u, e, d, r)
}

func TestSQLiteRecoveryDecisionRequestSelectedKindsAndDecisionProjectionCorruption(t *testing.T) {
	_, st, u, e, d, r := rdQASource(t, "resolved")
	f, m := rdQASeed(t, st, u, e, d, r)
	for _, table := range []string{"recovery_decisions", "requests"} {
		cols := rdQAColumns
		if table == "requests" {
			cols = rpQAColumns
		}
		for i, name := range strings.Split(cols, ",") {
			if i == 0 {
				continue
			}
			t.Run(table+"/kind/"+name, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				rdQAReadPair(t, tx, m.Revision, d, r)
				rdQARelax(t, tx, table, cols)
				v := sqliteio.Blob([]byte("wrong-text-kind"))
				if strings.HasSuffix(name, "_sec") || strings.HasSuffix(name, "_nsec") || name == "discarded" || name == "previous_revision" {
					v = sqliteio.Text("0")
				}
				interopDone(t, tx, "UPDATE "+table+" SET "+name+"=?", v)
				if table == "recovery_decisions" {
					got, found, err := sqliteReadRecoveryDecisionScalar(tx, u.ID)
					bgQACorrupt(t, err)
					if found || !reflect.DeepEqual(got, recoveryDecision{}) {
						t.Fatal("wrong-kind decision exposed row")
					}
				} else {
					got, found, err := sqliteReadResolveRequestProof(tx, r.ID, m.Revision)
					bgQACorrupt(t, err)
					if found || !reflect.DeepEqual(got, sqliteResolveRequestRow{}) {
						t.Fatal("wrong-kind request exposed row")
					}
				}
				interopRollback(t, tx)
				interopClose(t, c)
				rdQAReopen(t, f, m, u, e, d, r)
			})
		}
	}
	for _, tc := range []struct {
		name, sql string
		relax     bool
	}{
		{"previous-width", "previous_revision=X'01'", true}, {"previous-zero", "previous_revision=zeroblob(8)", true}, {"request-id", "request_id='private-invalid-id'", false}, {"discarded-bool", "discarded=2", true}, {"reason-control", "reason=char(0)", false}, {"reason-length", "reason=printf('%0513d',0)", false},
		{"end-sec-projection", "resolution_end_sec=resolution_end_sec+1", false}, {"end-nsec-projection", "resolution_end_nsec=resolution_end_nsec+1", false}, {"end-json", "resolution_end_json='not-json'", false}, {"end-year10000", "resolution_end_json='\"10000-01-01T00:00:00Z\"'", false},
		{"partial-observed", "observed_wall_json=NULL", true}, {"nondiscarded-absent-observed", "observed_capability=NULL,observed_wall_sec=NULL,observed_wall_nsec=NULL,observed_wall_json=NULL,observed_epoch=NULL,observed_elapsed_raw=NULL,observed_awake_raw=NULL", true}, {"observed-projection", "observed_wall_sec=observed_wall_sec+1", false}, {"observed-capability", "observed_capability='unavailable'", false}, {"observed-counter", "observed_elapsed_raw='01'", false}, {"observed-ceiling", "observed_awake_raw='9223372036854775808'", false}, {"observed-nil-epoch", "observed_epoch=NULL", false},
		{"partial-suffix", "discarded_start_nsec=NULL", true}, {"suffix-start-projection", "discarded_start_sec=discarded_start_sec+1", false}, {"suffix-end-projection", "discarded_end_nsec=discarded_end_nsec+1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			rdQAReadPair(t, tx, m.Revision, d, r)
			if tc.relax {
				rdQARelax(t, tx, "recovery_decisions", rdQAColumns)
			}
			interopDone(t, tx, "UPDATE recovery_decisions SET "+tc.sql)
			got, found, err := sqliteReadRecoveryDecisionScalar(tx, u.ID)
			bgQACorrupt(t, err)
			if found || !reflect.DeepEqual(got, recoveryDecision{}) {
				t.Fatal("decision local corruption exposed row")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			rdQAReopen(t, f, m, u, e, d, r)
		})
	}
	for _, table := range []string{"recovery_decisions", "requests"} {
		t.Run(table+"/extra-row", func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			cols := rdQAColumns
			if table == "requests" {
				cols = rpQAColumns
			}
			rdQARelax(t, tx, table, cols)
			interopDone(t, tx, "INSERT INTO "+table+" SELECT * FROM qa_original_"+table)
			if table == "requests" {
				got, found, err := sqliteReadResolveRequestProof(tx, r.ID, m.Revision)
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(got, sqliteResolveRequestRow{}) {
					t.Fatal("duplicate selected proof exposed row")
				}
			} else {
				got, found, err := sqliteReadRecoveryDecisionScalar(tx, u.ID)
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(got, recoveryDecision{}) {
					t.Fatal("duplicate selected decision exposed row")
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			rdQAReopen(t, f, m, u, e, d, r)
		})
	}
}

func rpQAInvalid(r sqliteResolveRequestRow, axis string) sqliteResolveRequestRow {
	r = rpQAClone(r)
	switch axis {
	case "id":
		r.ID = "private-invalid-id"
	case "fingerprint-upper":
		r.Fingerprint = strings.Repeat("A", 64)
	case "fingerprint-length":
		r.Fingerprint = strings.Repeat("a", 63)
	case "fingerprint-nonhex":
		r.Fingerprint = strings.Repeat("z", 64)
	case "contract":
		r.Result.ContractVersion = 2
	case "request-mismatch":
		r.Result.RequestID = "cccccccc-cccc-0ccc-0ccc-cccccccccccc"
	case "snapshot-zero":
		r.Result.SnapshotRevision = "0"
	case "snapshot-noncanonical":
		r.Result.SnapshotRevision = "01"
	case "snapshot-overflow":
		r.Result.SnapshotRevision = "18446744073709551616"
	case "changed":
		r.Result.Changed = false
	case "entity-nil":
		r.Result.EntityRevision = nil
	case "entity-zero":
		v := "0"
		r.Result.EntityRevision = &v
	case "entity-noncanonical":
		v := "01"
		r.Result.EntityRevision = &v
	case "entity-overflow":
		v := "18446744073709551616"
		r.Result.EntityRevision = &v
	case "affected-nil":
		r.Result.AffectedIDs = nil
	case "affected-empty":
		r.Result.AffectedIDs = []string{}
	case "affected-duplicate":
		r.Result.AffectedIDs = []string{r.Result.AffectedIDs[0], r.Result.AffectedIDs[0]}
	case "affected-invalid":
		r.Result.AffectedIDs = []string{"private-invalid-id"}
	}
	return r
}

func TestSQLiteResolveRequestProofStrictPayloadAndSelectedHistory(t *testing.T) {
	_, st, u, e, d, r := rdQASource(t, "resolved")
	f, m := rdQASeed(t, st, u, e, d, r)
	for _, axis := range []string{"contract", "request-mismatch", "snapshot-zero", "snapshot-noncanonical", "snapshot-overflow", "changed", "entity-nil", "entity-zero", "entity-noncanonical", "entity-overflow", "affected-nil", "affected-empty", "affected-duplicate", "affected-invalid", "duplicate-key", "unknown-key", "wrong-case-key", "missing-key", "invalid-utf8", "trailing-json", "array-top", "null-top", "wrong-field-type"} {
		t.Run(axis, func(t *testing.T) {
			bad := rpQAInvalid(r, axis)
			payload := rpQAPayload(t, bad)
			switch axis {
			case "duplicate-key":
				payload = "{\"contract_version\":1," + rpQAPayload(t, r)[1:]
			case "unknown-key":
				payload = "{\"private_unknown\":0," + rpQAPayload(t, r)[1:]
			case "wrong-case-key":
				payload = strings.Replace(rpQAPayload(t, r), "contract_version", "Contract_Version", 1)
			case "missing-key":
				payload = strings.Replace(rpQAPayload(t, r), "\"changed\":true,", "", 1)
			case "invalid-utf8":
				valid := rpQAPayload(t, r)
				prefix := "\"request_id\":\""
				at := strings.Index(valid, prefix+r.Result.RequestID+"\"")
				if at < 0 || !utf8.ValidString(valid) {
					t.Fatal("complete known-field UTF8 baseline invalid")
				}
				at += len(prefix)
				raw := []byte(valid)
				original := raw[at]
				raw[at] = 0xff
				payload = string(raw)
				if utf8.ValidString(payload) {
					t.Fatal("malformed UTF8 premise is false")
				}
				raw[at] = original
				if string(raw) != valid {
					t.Fatal("UTF8 mutation changed other fields or bytes")
				}
			case "trailing-json":
				payload = rpQAPayload(t, r) + " {}"
			case "array-top":
				payload = "[]"
			case "null-top":
				payload = "null"
			case "wrong-field-type":
				payload = strings.Replace(rpQAPayload(t, r), "\"changed\":true", "\"changed\":\"true\"", 1)
			}
			if payload == rpQAPayload(t, r) {
				t.Fatal("corruption axis is vacuous")
			}
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			rdQAReadPair(t, tx, m.Revision, d, r)
			interopDone(t, tx, "UPDATE requests SET payload=? WHERE request_id=?", sqliteio.Text(payload), sqliteio.Text(r.ID))
			got, found, err := sqliteReadResolveRequestProof(tx, r.ID, m.Revision)
			bgQACorrupt(t, err)
			interopSafeError(t, err, "private_unknown", "private-invalid", f.directory)
			if found || !reflect.DeepEqual(got, sqliteResolveRequestRow{}) {
				t.Fatal("strict typed proof corruption exposed result")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			rdQAReopen(t, f, m, u, e, d, r)
		})
	}
	for _, tc := range []struct {
		name, sql string
		relax     bool
	}{{"wrong-family", "operation='activity.interrupt'", false}, {"wrong-kind", "outcome_kind='error'", false}, {"fingerprint-upper", "fingerprint='" + strings.Repeat("A", 64) + "'", true}, {"fingerprint-width", "fingerprint='aa'", true}, {"fingerprint-nonhex", "fingerprint='" + strings.Repeat("z", 64) + "'", true}, {"null-payload", "payload=NULL", true}} {
		t.Run(tc.name, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if tc.relax {
				rdQARelax(t, tx, "requests", rpQAColumns)
			}
			interopDone(t, tx, "UPDATE requests SET "+tc.sql)
			got, found, err := sqliteReadResolveRequestProof(tx, r.ID, m.Revision)
			bgQACorrupt(t, err)
			if found || !reflect.DeepEqual(got, sqliteResolveRequestRow{}) {
				t.Fatal("selected wrong family/kind/fingerprint returned proof")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			rdQAReopen(t, f, m, u, e, d, r)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	// Strict valid JSON need not use the producer's byte order or whitespace.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(rpQAPayload(t, r)), &fields); err != nil {
		t.Fatal("independent JSON control failed")
	}
	pretty, err := json.MarshalIndent(fields, "", "  ")
	if err != nil {
		t.Fatal("independent pretty JSON control failed")
	}
	interopDone(t, tx, "UPDATE requests SET payload=? WHERE request_id=?", sqliteio.Text(string(pretty)), sqliteio.Text(r.ID))
	rdQAReadPair(t, tx, m.Revision, d, r)
	interopDone(t, tx, "INSERT INTO requests("+rpQAColumns+") VALUES(?,?,?,?,?)", sqliteio.Text(rpQAFreshID), sqliteio.Text("activity.resolve"), sqliteio.Text(strings.Repeat("a", 64)), sqliteio.Text("mutation_result"), sqliteio.Text("private-invalid-json"))
	rdQAReadPair(t, tx, m.Revision, d, r)
	interopRollback(t, tx)
	interopClose(t, c)
	rdQAReopen(t, f, m, u, e, d, r)
}

func TestSQLiteRecoveryDecisionRequestExplicitValidationZeroAndOwnedInputs(t *testing.T) {
	_, st, u, e, d, r := rdQASource(t, "resolved")
	f, m := rdQASeed(t, st, u, e, d, r)
	// An encodable year9999 time is a local codec boundary control. Its
	// temporary peer disagreement is deliberately left to the later graph seam.
	boundary := rdQAClone(d)
	wall := time.Date(9999, 12, 31, 23, 59, 59, 123456789, time.FixedZone("boundary offset", 20700))
	if _, err := wall.MarshalJSON(); err != nil {
		t.Fatal("year9999 encoding control failed")
	}
	boundary.ResolutionEnd = wall
	boundary.ObservedSample.WallUTC = wall
	boundary.DiscardedSuffix = &TimeRange{Start: wall, End: wall}
	ownedBoundary := rdQAClone(boundary)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	interopDone(t, tx, "DELETE FROM recovery_decisions")
	if n, err := sqliteRecoveryDecisionCharge(boundary); err != nil || n != rdQACharge(t, boundary) {
		t.Fatal("year9999 standalone charge refused", err)
	}
	if n, err := sqliteInsertRecoveryDecision(tx, boundary); err != nil || n != rdQACharge(t, boundary) || !reflect.DeepEqual(boundary, ownedBoundary) {
		t.Fatal("year9999 local insertion refused or mutated caller", err)
	}
	gotBoundary, foundBoundary, errBoundary := sqliteReadRecoveryDecisionScalar(tx, u.ID)
	if errBoundary != nil || !foundBoundary {
		t.Fatal("year9999 local decode refused", errBoundary)
	}
	if ueQAJSONTime(t, gotBoundary.ResolutionEnd) != ueQAJSONTime(t, wall) || ueQAJSONTime(t, gotBoundary.ObservedSample.WallUTC) != ueQAJSONTime(t, wall) || ueQAJSONTime(t, gotBoundary.DiscardedSuffix.End) != ueQAJSONTime(t, wall) {
		t.Fatal("year9999 original offset JSON lost")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	rdQAReopen(t, f, m, u, e, d, r)
	for _, axis := range []string{"id", "request-id", "previous-zero", "previous-noncanonical", "previous-overflow", "reason-control", "reason-length", "reason-utf8", "unencodable-end", "observed-absent", "observed-capability", "observed-zero-wall", "observed-unencodable-wall", "observed-nil-epoch", "observed-empty-epoch", "observed-long-epoch", "observed-nil-elapsed", "observed-nil-awake", "observed-noncanonical-counter", "observed-ceiling", "suffix-unencodable-start", "suffix-unencodable-end"} {
		t.Run("decision/"+axis, func(t *testing.T) {
			candidate := rdQAClone(d)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			interopDone(t, tx, "DELETE FROM recovery_decisions")
			if interopCount(t, tx, "SELECT count(*) FROM recovery_decisions") != 0 {
				t.Fatal("valid decision insertion control collides")
			}
			owned := rdQAClone(candidate)
			if n, err := sqliteInsertRecoveryDecision(tx, candidate); err != nil || n != rdQACharge(t, candidate) || !reflect.DeepEqual(candidate, owned) {
				t.Fatal("valid decision insertion control failed", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			unencodable := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			if _, err := unencodable.MarshalJSON(); err == nil {
				t.Fatal("year10000 MarshalJSON failure premise false")
			}
			switch axis {
			case "id":
				candidate.UncertaintyID = "private-invalid-id"
			case "request-id":
				candidate.RequestID = "private-invalid-id"
			case "previous-zero":
				candidate.PreviousRevision = "0"
			case "previous-noncanonical":
				candidate.PreviousRevision = "01"
			case "previous-overflow":
				candidate.PreviousRevision = "18446744073709551616"
			case "reason-control":
				candidate.Reason = "private\x00reason"
			case "reason-length":
				candidate.Reason = strings.Repeat("r", 513)
			case "reason-utf8":
				candidate.Reason = string([]byte{0xff})
			case "unencodable-end":
				candidate.ResolutionEnd = unencodable
			case "observed-absent":
				candidate.ObservedSample = nil
			case "observed-capability":
				candidate.ObservedSample.Capability = "unavailable"
			case "observed-zero-wall":
				candidate.ObservedSample.WallUTC = time.Time{}
			case "observed-unencodable-wall":
				candidate.ObservedSample.WallUTC = unencodable
			case "observed-nil-epoch":
				candidate.ObservedSample.Epoch = nil
			case "observed-empty-epoch":
				*candidate.ObservedSample.Epoch = ""
			case "observed-long-epoch":
				*candidate.ObservedSample.Epoch = strings.Repeat("e", 257)
			case "observed-nil-elapsed":
				candidate.ObservedSample.ElapsedNS = nil
			case "observed-nil-awake":
				candidate.ObservedSample.AwakeNS = nil
			case "observed-noncanonical-counter":
				*candidate.ObservedSample.ElapsedNS = "01"
			case "observed-ceiling":
				*candidate.ObservedSample.AwakeNS = "9223372036854775808"
			case "suffix-unencodable-start":
				candidate.DiscardedSuffix.Start = unencodable
			case "suffix-unencodable-end":
				candidate.DiscardedSuffix.End = unencodable
			}
			owned = rdQAClone(candidate)
			if n, err := sqliteRecoveryDecisionCharge(candidate); err == nil || n != 0 {
				t.Fatal("invalid decision charge exposed result")
			} else {
				rdQAPreSQLValidation(t, err)
			}
			c, tx = interopOpen(t, f, false, sqliteio.Write)
			interopDone(t, tx, "DELETE FROM recovery_decisions")
			if interopCount(t, tx, "SELECT count(*) FROM recovery_decisions") != 0 {
				t.Fatal("invalid decision premise collides with retained PK")
			}
			delta, err := sqliteInsertRecoveryDecision(tx, candidate)
			rdQAPreSQLValidation(t, err)
			interopSafeError(t, err, "private", f.directory)
			if delta != 0 || !reflect.DeepEqual(candidate, owned) || interopCount(t, tx, "SELECT count(*) FROM recovery_decisions") != 0 {
				t.Fatal("invalid decision changed caller, inserted row or returned delta")
			}
			proof, found, proofErr := sqliteReadResolveRequestProof(tx, r.ID, m.Revision)
			if proofErr != nil || !found || !reflect.DeepEqual(proof, r) {
				t.Fatal("invalid decision disturbed retained proof")
			}
			meta, metaErr := sqliteReadMeta(tx, f.authority, f.database)
			if metaErr != nil || !reflect.DeepEqual(meta, m) {
				t.Fatal("invalid decision spent metadata revision/nonce")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			rdQAReopen(t, f, m, u, e, d, r)
		})
	}
	for _, axis := range []string{"id", "fingerprint-upper", "fingerprint-length", "fingerprint-nonhex", "contract", "request-mismatch", "snapshot-zero", "snapshot-noncanonical", "snapshot-overflow", "changed", "entity-nil", "entity-zero", "entity-noncanonical", "entity-overflow", "affected-nil", "affected-empty", "affected-duplicate", "affected-invalid"} {
		t.Run("proof/"+axis, func(t *testing.T) {
			candidate := rpQAClone(r)
			candidate.ID, candidate.Result.RequestID = rpQAFreshID, rpQAFreshID
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if delta, err := sqliteInsertResolveRequest(tx, candidate); err != nil || delta != rpQACharge(t, candidate) {
				t.Fatal("valid proof control insertion failed", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			candidate = rpQAInvalid(candidate, axis)
			owned := rpQAClone(candidate)
			if n, err := sqliteResolveRequestCharge(candidate); err == nil || n != 0 {
				t.Fatal("invalid proof standalone charge exposed result")
			} else {
				rdQAPreSQLValidation(t, err)
			}
			c, tx = interopOpen(t, f, false, sqliteio.Write)
			delta, err := sqliteInsertResolveRequest(tx, candidate)
			rdQAPreSQLValidation(t, err)
			if delta != 0 || !reflect.DeepEqual(candidate, owned) {
				t.Fatal("invalid proof changed caller arrays/pointers or returned delta")
			}
			rdQAReadPair(t, tx, m.Revision, d, r)
			interopRollback(t, tx)
			interopClose(t, c)
			rdQAReopen(t, f, m, u, e, d, r)
		})
	}
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	if got, found, err := sqliteReadRecoveryDecisionScalar(tx, "private-invalid-id"); err == nil || found || !reflect.DeepEqual(got, recoveryDecision{}) {
		t.Fatal("invalid decision lookup produced result")
	} else {
		bgQAValidation(t, err)
	}
	for _, ceiling := range []string{"0", "01", "18446744073709551616", "private-invalid"} {
		for _, id := range []string{r.ID, rpQAFreshID} {
			got, found, err := sqliteReadResolveRequestProof(tx, id, ceiling)
			bgQAValidation(t, err)
			if found || !reflect.DeepEqual(got, sqliteResolveRequestRow{}) {
				t.Fatal("invalid explicit ceiling produced proof")
			}
		}
	}
	if got, found, err := sqliteReadResolveRequestProof(tx, "private-invalid-id", m.Revision); err == nil || found || !reflect.DeepEqual(got, sqliteResolveRequestRow{}) {
		t.Fatal("invalid explicit proof ID produced result")
	} else {
		bgQAValidation(t, err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteRecoveryDecisionAvailableLimitsAndActualMalformedPersistence(t *testing.T) {
	for _, axis := range []string{"empty-reason", "512-byte-reason", "signed-max-awake-greater", "malformed-short", "malformed-expands"} {
		t.Run(axis, func(t *testing.T) {
			h, st, u, e, d, r := rdQASource(t, "resolved")
			baseD := rdQAClone(d)
			persistedValid := true
			switch axis {
			case "empty-reason":
				d.Reason = ""
			case "512-byte-reason":
				d.Reason = strings.Repeat("r", 512)
			case "signed-max-awake-greater":
				elapsed, awake := "0", "9223372036854775807"
				d.ObservedSample.ElapsedNS, d.ObservedSample.AwakeNS = &elapsed, &awake
				epoch := "independent-valid-epoch"
				d.ObservedSample.Epoch = &epoch
			case "malformed-short":
				bad := string([]byte{0xff, 0xfe})
				d.ObservedSample.Epoch = &bad
			case "malformed-expands":
				bad := strings.Repeat(string([]byte{0xff}), 256)
				d.ObservedSample.Epoch = &bad
				persistedValid = false
			}
			st.RecoveryDecisions[u.ID] = d
			if !validState(st) {
				t.Fatal("complete raw available policy witness invalid")
			}
			saved := bgQALegacyMarshalOracle(t, h.service, h.path, st, persistedValid)
			// The original valid fixture supplies dependencies. Admission validates
			// raw sampleValues before persistence repair, exactly like legacy save.
			f, m := rdQASeed(t, st, u, e, baseD, r)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			interopDone(t, tx, "DELETE FROM recovery_decisions")
			owned := rdQAClone(d)
			if n, err := sqliteRecoveryDecisionCharge(d); err != nil || n != rdQACharge(t, d) {
				t.Fatal("raw available standalone admission/charge differs", err)
			}
			if delta, err := sqliteInsertRecoveryDecision(tx, d); err != nil || delta != rdQACharge(t, d) || !reflect.DeepEqual(d, owned) {
				t.Fatal("raw available write added postrepair limit or changed caller", err)
			}
			got, found, err := sqliteReadRecoveryDecisionScalar(tx, u.ID)
			if persistedValid {
				if err != nil || !found || !reflect.DeepEqual(got, saved.RecoveryDecisions[u.ID]) {
					t.Fatal("available persisted decode differs from strict legacy oracle", err)
				}
			} else {
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(got, recoveryDecision{}) {
					t.Fatal("expanded stored epoch escaped corruption boundary")
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			rdQAReopen(t, f, m, u, e, baseD, r)
		})
	}
}

func TestSQLiteRecoveryDecisionRequestInsertOnlyNativeConflictRollbackAndMetadataCommit(t *testing.T) {
	_, st, u, e, d, r := rdQASource(t, "resolved")
	f, m := rdQASeed(t, st, u, e, d, r)
	for _, which := range []string{"decision", "proof"} {
		t.Run(which, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			fresh := rpQAClone(r)
			fresh.ID, fresh.Result.RequestID = rpQAFreshID, rpQAFreshID
			if delta, err := sqliteInsertResolveRequest(tx, fresh); err != nil || delta != rpQACharge(t, fresh) {
				t.Fatal("earlier staged valid proof failed", err)
			}
			ownedD, ownedR := rdQAClone(d), rpQAClone(r)
			var delta int64
			var err error
			if which == "decision" {
				delta, err = sqliteInsertRecoveryDecision(tx, d)
			} else {
				delta, err = sqliteInsertResolveRequest(tx, r)
			}
			bgQAValidation(t, err)
			var native *sqliteio.Error
			if delta != 0 || !errors.As(err, &native) || native.Category != sqliteio.Constraint || (native.Code != 1555 && native.Code != 2067) || !reflect.DeepEqual(d, ownedD) || !reflect.DeepEqual(r, ownedR) {
				t.Fatal("insert-only identity conflict lost native/zero/ownership evidence")
			}
			interopSafeError(t, err, u.ID, r.ID, f.directory)
			rdQAReadPair(t, tx, m.Revision, d, r)
			if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, m) {
				t.Fatal("row insert/conflict spent metadata revision/nonce")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			rdQAReopen(t, f, m, u, e, d, r)
		})
	}
	for _, commit := range []bool{false, true} {
		t.Run(strconv.FormatBool(commit), func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			fresh := rpQAClone(r)
			fresh.ID, fresh.Result.RequestID = rpQAFreshID, rpQAFreshID
			owned := rpQAClone(fresh)
			delta, err := sqliteInsertResolveRequest(tx, fresh)
			if err != nil || delta != rpQACharge(t, fresh) || !reflect.DeepEqual(fresh, owned) {
				t.Fatal("fresh proof staged charge/ownership differs", err)
			}
			if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, m) {
				t.Fatal("insert secretly composed metadata")
			}
			if !commit {
				interopRollback(t, tx)
				interopClose(t, c)
				rdQAReopen(t, f, m, u, e, d, r)
				return
			}
			next := metaQANext(t, m)
			next.Revision, next.LogicalBytes = bump(m.Revision), m.LogicalBytes+delta
			if err := sqliteUpdateMeta(tx, m, next); err != nil {
				t.Fatal("caller +1 metadata composition refused")
			}
			if err := tx.CheckForeignKeys(); err != nil {
				t.Fatal("valid deferred-FK final composition refused")
			}
			interopCommit(t, tx)
			interopClose(t, c)
			c, tx = interopOpen(t, f, false, sqliteio.Read)
			rdQAReadPair(t, tx, next.Revision, d, r)
			got, found, err := sqliteReadResolveRequestProof(tx, fresh.ID, next.Revision)
			if err != nil || !found || !reflect.DeepEqual(got, fresh) {
				t.Fatal("cold committed proof differs")
			}
			meta, err := sqliteReadMeta(tx, f.authority, f.database)
			if err != nil || !reflect.DeepEqual(meta, next) || ueQAStored(t, tx)+rdQAStored(t, tx) != next.LogicalBytes || interopCount(t, tx, "SELECT count(*) FROM requests") != 2 || interopCount(t, tx, "SELECT count(*) FROM recovery_decisions") != 1 {
				t.Fatal("cold +1/nonce/charge/history differs")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}

func TestSQLiteRecoveryDecisionRequestCancellationAndTerminalRefusal(t *testing.T) {
	_, st, u, e, d, r := rdQASource(t, "resolved")
	f, m := rdQASeed(t, st, u, e, d, r)
	for _, op := range []string{"read-decision", "read-proof", "insert-decision", "insert-proof"} {
		t.Run(op, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			c, err := sqliteio.Open(ctx, f.directory, f.database, sqliteio.Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			if err != nil {
				t.Fatal("cancellation fixture open failed")
			}
			t.Cleanup(func() { _ = c.Close(context.Background()) })
			tx, err := c.Begin(ctx, sqliteio.Write)
			if err != nil {
				t.Fatal("cancellation begin failed")
			}
			t.Cleanup(func() { _ = tx.Rollback() })
			fresh := rpQAClone(r)
			fresh.ID, fresh.Result.RequestID = rpQAFreshID, rpQAFreshID
			if _, err := sqliteInsertResolveRequest(tx, fresh); err != nil {
				t.Fatal("pre-cancel valid staged proof failed")
			}
			ownedD, ownedR := rdQAClone(d), rpQAClone(r)
			cancel()
			var delta int64
			var found bool
			var gotD recoveryDecision
			var gotR sqliteResolveRequestRow
			switch op {
			case "read-decision":
				gotD, found, err = sqliteReadRecoveryDecisionScalar(tx, u.ID)
			case "read-proof":
				gotR, found, err = sqliteReadResolveRequestProof(tx, r.ID, m.Revision)
			case "insert-decision":
				delta, err = sqliteInsertRecoveryDecision(tx, d)
			case "insert-proof":
				delta, err = sqliteInsertResolveRequest(tx, r)
			}
			if !errors.Is(err, context.Canceled) || delta != 0 || found || !reflect.DeepEqual(gotD, recoveryDecision{}) || !reflect.DeepEqual(gotR, sqliteResolveRequestRow{}) || !reflect.DeepEqual(d, ownedD) || !reflect.DeepEqual(r, ownedR) {
				t.Fatal("cancellation lost zero/owned-input evidence")
			}
			interopSafeError(t, err, f.directory, u.ID, r.ID)
			if cleanup := tx.Rollback(); cleanup != nil {
				interopSafeError(t, cleanup, f.directory)
			}
			interopClose(t, c)
			rdQAReopen(t, f, m, u, e, d, r)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	interopRollback(t, tx)
	if got, found, err := sqliteReadRecoveryDecisionScalar(tx, u.ID); err == nil || found || !reflect.DeepEqual(got, recoveryDecision{}) {
		t.Fatal("terminal decision returned usable row")
	} else {
		var native *sqliteio.Error
		if !errors.As(err, &native) {
			t.Fatal("terminal decision lost adapter evidence")
		}
	}
	if got, found, err := sqliteReadResolveRequestProof(tx, r.ID, m.Revision); err == nil || found || !reflect.DeepEqual(got, sqliteResolveRequestRow{}) {
		t.Fatal("terminal proof returned usable row")
	} else {
		var native *sqliteio.Error
		if !errors.As(err, &native) {
			t.Fatal("terminal proof lost adapter evidence")
		}
	}
	interopClose(t, c)
	for _, which := range []string{"decision", "proof"} {
		c, tx = interopOpen(t, f, false, sqliteio.Read)
		var delta int64
		var err error
		if which == "decision" {
			delta, err = sqliteInsertRecoveryDecision(tx, d)
		} else {
			fresh := rpQAClone(r)
			fresh.ID, fresh.Result.RequestID = rpQAFreshID, rpQAFreshID
			delta, err = sqliteInsertResolveRequest(tx, fresh)
		}
		var native *sqliteio.Error
		if delta != 0 || !errors.As(err, &native) || native.Category == sqliteio.Constraint {
			t.Fatal("read-mode insertion lost zero/native refusal")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		rdQAReopen(t, f, m, u, e, d, r)
	}
}
