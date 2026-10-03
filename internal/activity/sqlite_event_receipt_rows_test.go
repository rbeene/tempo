//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent source-first QA of the inactive immutable receipt unit. Complete
// legacy states supply admission/replay oracles; direct segment fixtures only
// transport real dependencies and do not claim importer/reducer acceptance.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const erQAReceiptColumns = "event_key,fingerprint,contract_version,snapshot_revision,disposition,actor_key,actor_generation,segment_id"
const erQAChildColumns = "event_key,ordinal,uncertainty_id"
const erQASegmentColumns = "segment_id,actor_key,actor_generation,binding_id,binding_revision,account_id,user_id,project_id,task_id,timezone,computer_id,group_order,epoch_id,start_sample_capability,start_sample_wall_sec,start_sample_wall_nsec,start_sample_wall_json,start_sample_epoch,start_sample_elapsed_raw,start_sample_awake_raw,start_sample_elapsed,start_sample_awake,confirmed_sample_capability,confirmed_sample_wall_sec,confirmed_sample_wall_nsec,confirmed_sample_wall_json,confirmed_sample_epoch,confirmed_sample_elapsed_raw,confirmed_sample_awake_raw,confirmed_sample_elapsed,confirmed_sample_awake,start_sec,start_nsec,start_json,confirmed_sec,confirmed_nsec,confirmed_json,end_sec,end_nsec,end_json,uncertainty_id,finalized"

type erQAPair struct {
	Row sqliteEventReceiptRow
	ID  string
}

func erQAClone(row sqliteEventReceiptRow) sqliteEventReceiptRow {
	if row.Value.Result.SegmentID != nil {
		s := *row.Value.Result.SegmentID
		row.Value.Result.SegmentID = &s
	}
	if row.Value.Result.UncertaintyIDs != nil {
		row.Value.Result.UncertaintyIDs = append([]string{}, row.Value.Result.UncertaintyIDs...)
	}
	return row
}
func erQAMaterialized(t *testing.T, row sqliteEventReceiptRow) sqliteEventReceiptRow {
	t.Helper()
	b, err := json.Marshal(row.Value)
	if err != nil {
		t.Fatal("receipt oracle marshal")
	}
	var value eventReceipt
	if err := json.Unmarshal(b, &value); err != nil {
		t.Fatal("receipt oracle decode")
	}
	return sqliteEventReceiptRow{Key: bgQAPersistedString(t, row.Key), Value: value}
}
func erQAPairs(t *testing.T, source *state) []erQAPair {
	t.Helper()
	keys := make([]string, 0, len(source.Receipts))
	for key := range source.Receipts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]erQAPair, 0, len(keys))
	for _, key := range keys {
		id := ""
		found := false
		for candidate, target := range source.EventIDs {
			if target == key {
				if found {
					t.Fatal("legacy inverse not singleton")
				}
				id, found = candidate, true
			}
		}
		if !found {
			t.Fatal("legacy inverse missing")
		}
		pairs = append(pairs, erQAPair{Row: erQAClone(sqliteEventReceiptRow{Key: key, Value: source.Receipts[key]}), ID: id})
	}
	return pairs
}
func erQASource(t *testing.T) (*qaHarness, *state, []erQAPair) {
	t.Helper()
	h, source := epQALegacy(t)
	// The real first work input establishes an independently hashed Event oracle.
	e := qaEvent("epoch-A", "1", "1", "work", qaBindingA)
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal("event oracle marshal")
	}
	digest := sha256.Sum256(b)
	saved, ok := source.Receipts[eventKey(e)]
	if !ok || saved.Fingerprint != hex.EncodeToString(digest[:]) || source.EventIDs[e.EventID] != eventKey(e) {
		t.Fatal("actual fingerprint/key oracle missing")
	}
	if len(source.Uncertainties) != 0 {
		t.Fatal("transport fixture unexpectedly needs uncertainty rows")
	}
	return h, source, erQAPairs(t, source)
}
func erQAStateClone(t *testing.T, source *state) *state {
	t.Helper()
	b, err := json.Marshal(source)
	if err != nil {
		t.Fatal("state clone marshal")
	}
	var copy state
	if err = json.Unmarshal(b, &copy); err != nil {
		t.Fatal("state clone decode")
	}
	return &copy
}
func erQAReplace(source *state, old, next erQAPair) {
	delete(source.Receipts, old.Row.Key)
	delete(source.EventIDs, old.ID)
	source.Receipts[next.Row.Key] = next.Row.Value
	source.EventIDs[next.ID] = next.Row.Key
}
func erQAStrict(t *testing.T, source *state) *state {
	t.Helper()
	h := qaNew(t)
	h.seed()
	return bgQALegacyMarshalOracle(t, h.service, h.path, source, true)
}
func erQALegacyWrite(t *testing.T, source *state, wantValid bool) *state {
	t.Helper()
	if !validState(source) {
		t.Fatal("raw complete legacy state is not valid")
	}
	h := qaNew(t)
	h.seed()
	if err := h.service.store.update(context.Background(), func(st *state) (bool, error) { *st = *source; return true, nil }); err != nil {
		bgQAFixtureError(t, "actual legacy boundary write", err)
	}
	b, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal("owned legacy evidence read")
	}
	var decoded state
	if err = json.Unmarshal(b, &decoded); err != nil {
		t.Fatal("legacy JSON decode")
	}
	got, exists, readErr := h.service.store.read(context.Background())
	if wantValid {
		if readErr != nil || !exists || !validState(&decoded) || !reflect.DeepEqual(got, &decoded) {
			t.Fatal("actual legacy strict read did not accept", bgQAFixtureCode(readErr))
		}
	} else {
		qaCode(t, readErr, "state_corrupt")
	}
	after, err := os.ReadFile(h.path)
	if err != nil || !bytes.Equal(after, b) {
		t.Fatal("legacy read changed preserved evidence")
	}
	return &decoded
}
func erQACharge(t *testing.T, row sqliteEventReceiptRow, id string) int64 {
	t.Helper()
	row = erQAMaterialized(t, row)
	id = bgQAPersistedString(t, id)
	r := row.Value.Result
	n := int64(112 + len(row.Key) + len(row.Value.Fingerprint) + len(r.Disposition) + len(actorKey(r.Actor.Key)))
	if r.SegmentID != nil {
		n += int64(8 + len(*r.SegmentID))
	}
	for _, ref := range r.UncertaintyIDs {
		n += int64(59 + len(row.Key) + len(ref))
	}
	return n + int64(50+len(id)+len(row.Key))
}
func erQAQueryCharge(t *testing.T, tx *sqliteio.Tx, queries ...string) int64 {
	t.Helper()
	var total int64
	for _, q := range queries {
		s := interopPrepare(t, tx, q)
		for {
			row, err := s.Step()
			if err != nil {
				t.Fatal("charge step")
			}
			if !row {
				break
			}
			total += 32
			for i := 0; i < s.ColumnCount(); i++ {
				total++
				kind, err := s.Kind(i)
				if err != nil {
					t.Fatal("charge kind")
				}
				switch kind {
				case sqliteio.NullKind:
				case sqliteio.IntegerKind:
					total += 8
				case sqliteio.TextKind:
					v, e := s.Text(i)
					if e != nil {
						t.Fatal("charge text")
					}
					total += 8 + int64(len(v))
				case sqliteio.BlobKind:
					v, e := s.Blob(i)
					if e != nil {
						t.Fatal("charge blob")
					}
					total += 8 + int64(len(v))
				default:
					t.Fatal("charge unknown kind")
				}
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal("charge cleanup")
		}
	}
	return total
}
func erQAAudit(t *testing.T, tx *sqliteio.Tx) int64 {
	return bgQAStoredCharge(t, tx) + epQAStoredCharge(t, tx) + erQAQueryCharge(t, tx, "SELECT "+erQASegmentColumns+" FROM segments", "SELECT "+erQAReceiptColumns+" FROM event_receipts", "SELECT "+erQAChildColumns+" FROM event_receipt_uncertainties", "SELECT event_id,event_key FROM event_ids")
}
func erQATime(t *testing.T, v time.Time) []sqliteio.Value {
	t.Helper()
	b, err := v.MarshalJSON()
	if err != nil {
		t.Fatal("time oracle")
	}
	return []sqliteio.Value{sqliteio.Integer(v.Unix()), sqliteio.Integer(int64(v.Nanosecond())), sqliteio.Text(string(b))}
}
func erQAClock(t *testing.T, s ClockSample) []sqliteio.Value {
	t.Helper()
	if _, _, ok := sampleValues(s); !ok || s.Epoch == nil || s.ElapsedNS == nil || s.AwakeNS == nil {
		t.Fatal("real available dependency sample")
	}
	values := []sqliteio.Value{sqliteio.Text(s.Capability)}
	values = append(values, erQATime(t, s.WallUTC)...)
	return append(values, sqliteio.Text(*s.Epoch), sqliteio.Text(*s.ElapsedNS), sqliteio.Text(*s.AwakeNS), interopCounter(t, *s.ElapsedNS), interopCounter(t, *s.AwakeNS))
}
func erQASegment(t *testing.T, tx *sqliteio.Tx, seg *segment) {
	t.Helper()
	if seg.UncertaintyID != nil {
		t.Fatal("source dependency has unplanned uncertainty")
	}
	a := seg.Binding.Attribution
	b, _ := json.Marshal(a)
	values := []sqliteio.Value{sqliteio.Text(seg.ID), sqliteio.Text(actorKey(seg.Actor.Key)), interopCounter(t, seg.Actor.Generation), sqliteio.Text(seg.Binding.ID), interopCounter(t, seg.Binding.Revision), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID), sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Text(seg.Actor.Key.ComputerID), sqliteio.Text(seg.Actor.Key.ComputerID + "/" + string(b)), sqliteio.Text(seg.EpochID)}
	values = append(values, erQAClock(t, seg.StartSample)...)
	values = append(values, erQAClock(t, seg.ConfirmedSample)...)
	values = append(values, erQATime(t, seg.Start)...)
	values = append(values, erQATime(t, seg.Confirmed)...)
	if seg.End == nil {
		values = append(values, sqliteio.Null(), sqliteio.Null(), sqliteio.Null())
	} else {
		values = append(values, erQATime(t, *seg.End)...)
	}
	finalized := int64(0)
	if seg.Finalized {
		finalized = 1
	}
	values = append(values, sqliteio.Null(), sqliteio.Integer(finalized))
	if len(values) != 42 {
		t.Fatal("real segment dependency projection width")
	}
	interopDone(t, tx, "INSERT INTO segments("+erQASegmentColumns+") VALUES("+strings.TrimSuffix(strings.Repeat("?,", 42), ",")+")", values...)
}
func erQASeed(t *testing.T, source *state, pairs []erQAPair, ceiling string) (interopFixture, sqliteStoreMeta) {
	t.Helper()
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal("schema bootstrap")
	}
	m := interopMeta(f)
	m.ComputerID, m.Revision, m.SyncEnabled = source.ComputerID, ceiling, source.SyncEnabled
	m.LogicalBytes = int64(114 + len(source.ComputerID) + len(f.authority) + len(f.database))
	for _, row := range bgQARowsFromLegacy(source) {
		d, e := sqliteInsertBinding(tx, row)
		if e != nil || d != bgQABindingCharge(t, row) {
			t.Fatal("binding dependency")
		}
		m.LogicalBytes += d
	}
	refs := map[string]ActorRef{}
	for _, seg := range source.Segments {
		refs[actorKey(seg.Actor.Key)+"/"+seg.Actor.Generation] = seg.Actor
	}
	for _, p := range pairs {
		r := p.Row.Value.Result.Actor
		refs[actorKey(r.Key)+"/"+r.Generation] = r
	}
	for _, ref := range refs {
		d, e := sqliteEnsureActorGeneration(tx, ref)
		if e != nil || d != bgQAGenerationCharge(t, ref) {
			t.Fatal("generation dependency", bgQAFixtureCode(e))
		}
		m.LogicalBytes += d
	}
	for _, row := range epQARows(source) {
		d, e := sqliteInsertEpoch(tx, row)
		if e != nil || d != epQACharge(t, row) {
			t.Fatal("epoch dependency")
		}
		m.LogicalBytes += d
	}
	for _, seg := range source.Segments {
		erQASegment(t, tx, seg)
	}
	m.LogicalBytes += erQAQueryCharge(t, tx, "SELECT "+erQASegmentColumns+" FROM segments")
	for _, p := range pairs {
		d, e := sqliteInsertEventReceipt(tx, p.Row, p.ID)
		if e != nil || d != erQACharge(t, p.Row, p.ID) {
			t.Fatal("receipt bootstrap", bgQAFixtureCode(e))
		}
		m.LogicalBytes += d
		if e = sqliteValidateEventReceiptID(tx, erQAMaterialized(t, p.Row).Key, ceiling); e != nil {
			t.Fatal("final receipt proof", bgQAFixtureCode(e))
		}
	}
	if err := sqliteInsertMeta(tx, m); err != nil {
		t.Fatal("final metadata bootstrap")
	}
	if erQAAudit(t, tx) != m.LogicalBytes {
		t.Fatal("literal stored charge differs")
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal("dependency FK proof")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return f, m
}
func erQAMetaUnchanged(t *testing.T, tx *sqliteio.Tx, f interopFixture, m sqliteStoreMeta) {
	t.Helper()
	got, e := sqliteReadMeta(tx, f.authority, f.database)
	if e != nil || !reflect.DeepEqual(got, m) {
		t.Fatal("row helper changed metadata/revision/nonce/charge")
	}
}
func erQARead(t *testing.T, tx *sqliteio.Tx, p erQAPair, ceiling string) {
	t.Helper()
	want := erQAMaterialized(t, p.Row)
	id := bgQAPersistedString(t, p.ID)
	got, found, e := sqliteReadEventReceipt(tx, want.Key, ceiling)
	if e != nil || !found || !reflect.DeepEqual(got, want) || got.Value.Result.UncertaintyIDs == nil {
		t.Fatal("typed historical result differs", bgQAFixtureCode(e))
	}
	key, found, e := sqliteReadEventID(tx, id)
	if e != nil || !found || key != want.Key {
		t.Fatal("typed EventID mapping differs", bgQAFixtureCode(e))
	}
	if e = sqliteAssertEventReceipt(tx, ceiling, want, id); e != nil {
		t.Fatal("complete before assertion", bgQAFixtureCode(e))
	}
	if e = sqliteValidateEventReceiptID(tx, want.Key, ceiling); e != nil {
		t.Fatal("inverse/final proof", bgQAFixtureCode(e))
	}
}
func erQAReopen(t *testing.T, f interopFixture, m sqliteStoreMeta, pairs []erQAPair) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	erQAMetaUnchanged(t, tx, f, m)
	if interopCount(t, tx, "SELECT count(*) FROM actors") != 0 {
		t.Fatal("fixture synthesized current Actor heads")
	}
	if interopCount(t, tx, "SELECT count(*) FROM event_receipts") != int64(len(pairs)) || interopCount(t, tx, "SELECT count(*) FROM event_ids") != int64(len(pairs)) {
		t.Fatal("unit count changed")
	}
	n := 0
	for _, p := range pairs {
		erQARead(t, tx, p, m.Revision)
		n += len(p.Row.Value.Result.UncertaintyIDs)
	}
	if interopCount(t, tx, "SELECT count(*) FROM event_receipt_uncertainties") != int64(n) || erQAAudit(t, tx) != m.LogicalBytes {
		t.Fatal("baseline child/charge changed")
	}
	interopRollback(t, tx)
	interopClose(t, c)
}
func erQACount(t *testing.T, tx *sqliteio.Tx, sql string, args ...sqliteio.Value) int64 {
	t.Helper()
	s := interopPrepare(t, tx, sql, args...)
	ok, e := s.Step()
	if !ok || e != nil {
		t.Fatal("bound count")
	}
	n, e := s.Int64(0)
	if e != nil {
		t.Fatal("bound count kind")
	}
	if ok, e = s.Step(); ok || e != nil {
		t.Fatal("count not singleton")
	}
	if e = s.Close(); e != nil {
		t.Fatal("count cleanup")
	}
	return n
}
func erQAFresh(p erQAPair, label string) erQAPair {
	p.Row = erQAClone(p.Row)
	p.Row.Key = "synthetic-receipt-" + label
	p.ID = "synthetic-event-" + label
	return p
}
func erQAConstraint(t *testing.T, err error, code int32) {
	t.Helper()
	bgQAValidation(t, err)
	var native *sqliteio.Error
	if !errors.As(err, &native) || native.Category != sqliteio.Constraint || native.Code != code {
		t.Fatalf("distinct native constraint missing: type=%T", err)
	}
	interopSafeError(t, err, "synthetic-receipt-", "synthetic-event-")
}

func TestSQLiteEventReceiptRowsActualLegacyRoundtripChargeAndOwnedResults(t *testing.T) {
	_, source, pairs := erQASource(t)
	erQAStrict(t, source)
	// Retained opaque/duplicate strings are admitted history, not uncertainty FKs.
	rich := erQAClone(pairs[0].Row)
	rich.Value.Result.UncertaintyIDs = []string{"first", "second", "first", "", "\x00missing"}
	next := pairs[0]
	next.Row = rich
	erQAReplace(source, pairs[0], next)
	pairs = erQAPairs(t, erQAStrict(t, source))
	f, m := erQASeed(t, source, pairs, source.Revision)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	for _, p := range pairs {
		erQARead(t, tx, p, m.Revision)
		d, e := sqliteEventReceiptCharge(p.Row, p.ID)
		if e != nil || d != erQACharge(t, p.Row, p.ID) {
			t.Fatal("literal unit charge")
		}
	}
	got, found, e := sqliteReadEventReceipt(tx, next.Row.Key, m.Revision)
	if e != nil || !found || got.Value.Result.SegmentID == nil {
		t.Fatal("owned result premise")
	}
	*got.Value.Result.SegmentID = "caller-mutation"
	got.Value.Result.UncertaintyIDs[0] = "caller-mutation"
	erQARead(t, tx, next, m.Revision)
	s := interopPrepare(t, tx, "SELECT "+erQAReceiptColumns+" FROM event_receipts WHERE event_key=?", sqliteio.Text(next.Row.Key))
	ok, e := s.Step()
	if !ok || e != nil || s.ColumnCount() != 8 {
		t.Fatal("eight-column scalar")
	}
	for col, want := range []sqliteio.Kind{sqliteio.TextKind, sqliteio.TextKind, sqliteio.IntegerKind, sqliteio.BlobKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.BlobKind, sqliteio.TextKind} {
		kind, e := s.Kind(col)
		if e != nil || kind != want {
			t.Fatal("scalar kind", col)
		}
	}
	for _, col := range []int{3, 6} {
		b, e := s.Blob(col)
		if e != nil || len(b) != 8 {
			t.Fatal("canonical counter width")
		}
	}
	if e = s.Close(); e != nil {
		t.Fatal("scalar cleanup")
	}
	s = interopPrepare(t, tx, "SELECT "+erQAChildColumns+" FROM event_receipt_uncertainties WHERE event_key=? ORDER BY ordinal", sqliteio.Text(next.Row.Key))
	for ordinal, ref := range next.Row.Value.Result.UncertaintyIDs {
		ok, e = s.Step()
		if !ok || e != nil || s.ColumnCount() != 3 {
			t.Fatal("ordered child projection")
		}
		for col, want := range []sqliteio.Kind{sqliteio.TextKind, sqliteio.IntegerKind, sqliteio.TextKind} {
			kind, e := s.Kind(col)
			if e != nil || kind != want {
				t.Fatal("child kind", col)
			}
		}
		owner, e := s.Text(0)
		if e != nil || owner != next.Row.Key {
			t.Fatal("child owner")
		}
		index, e := s.Int64(1)
		if e != nil || index != int64(ordinal) {
			t.Fatal("child ordinal")
		}
		value, e := s.Text(2)
		if e != nil || value != ref {
			t.Fatal("child order/value")
		}
	}
	if ok, e = s.Step(); ok || e != nil {
		t.Fatal("child extra row")
	}
	if e = s.Close(); e != nil {
		t.Fatal("child cleanup")
	}
	s = interopPrepare(t, tx, "SELECT event_id,event_key FROM event_ids WHERE event_id=?", sqliteio.Text(next.ID))
	ok, e = s.Step()
	if !ok || e != nil || s.ColumnCount() != 2 {
		t.Fatal("two-column EventID")
	}
	for col, want := range []string{next.ID, next.Row.Key} {
		kind, e := s.Kind(col)
		if e != nil || kind != sqliteio.TextKind {
			t.Fatal("EventID stored kind")
		}
		value, e := s.Text(col)
		if e != nil || value != want {
			t.Fatal("EventID projection")
		}
	}
	if ok, e = s.Step(); ok || e != nil {
		t.Fatal("EventID extra row")
	}
	if e = s.Close(); e != nil {
		t.Fatal("EventID cleanup")
	}
	bgQAPlan(t, tx, "SELECT "+erQAReceiptColumns+" FROM event_receipts WHERE event_key=?", "sqlite_autoindex_event_receipts_1", sqliteio.Text(next.Row.Key))
	bgQAPlan(t, tx, "SELECT event_id,event_key FROM event_ids WHERE event_id=?", "sqlite_autoindex_event_ids_1", sqliteio.Text(next.ID))
	bgQAPlan(t, tx, "SELECT event_id,event_key FROM event_ids WHERE event_key=?", "sqlite_autoindex_event_ids_2", sqliteio.Text(next.Row.Key))
	bgQAPlan(t, tx, "SELECT "+erQAChildColumns+" FROM event_receipt_uncertainties WHERE event_key=? ORDER BY ordinal", "sqlite_autoindex_event_receipt_uncertainties_1", sqliteio.Text(next.Row.Key))
	erQAMetaUnchanged(t, tx, f, m)
	interopRollback(t, tx)
	interopClose(t, c)
	erQAReopen(t, f, m, pairs)
}

func TestSQLiteEventReceiptRowsStrictHistoricalAdmissionsAndUnsignedCeilings(t *testing.T) {
	_, baseline, original := erQASource(t)
	cases := []struct {
		name   string
		mutate func(*state, *erQAPair)
	}{
		{"uppercase-fingerprint", func(_ *state, p *erQAPair) { p.Row.Value.Fingerprint = strings.ToUpper(p.Row.Value.Fingerprint) }},
		{"foreign-max-generation", func(_ *state, p *erQAPair) {
			p.Row.Value.Result.Actor.Key.ComputerID = "99999999-9999-4999-8999-999999999999"
			p.Row.Value.Result.Actor.Generation = "18446744073709551615"
		}},
		{"unassociated-existing-segment", func(s *state, p *erQAPair) {
			for id, seg := range s.Segments {
				if seg.Actor != p.Row.Value.Result.Actor {
					p.Row.Value.Result.SegmentID = &id
					return
				}
			}
			t.Fatal("distinct segment control missing")
		}},
		{"absent-segment", func(_ *state, p *erQAPair) { p.Row.Value.Result.SegmentID = nil }},
		{"opaque-empty-key", func(_ *state, p *erQAPair) { p.Row.Key = "" }},
		{"opaque-nul-key-children", func(_ *state, p *erQAPair) {
			p.Row.Key = "opaque\x00receipt"
			p.Row.Value.Result.UncertaintyIDs = []string{"", "not-a-UUID", "not-a-UUID", "\x00missing"}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := erQAStateClone(t, baseline)
			next := original[0]
			next.Row = erQAClone(next.Row)
			tc.mutate(source, &next)
			erQAReplace(source, original[0], next)
			source = erQAStrict(t, source)
			pairs := erQAPairs(t, source)
			f, m := erQASeed(t, source, pairs, source.Revision)
			erQAReopen(t, f, m, pairs)
		})
	}
	for _, rev := range []string{"0", "9223372036854775807", "9223372036854775808", "18446744073709551615"} {
		t.Run("snapshot-"+rev, func(t *testing.T) {
			source := erQAStateClone(t, baseline)
			source.Revision = "18446744073709551615"
			next := original[0]
			next.Row = erQAClone(next.Row)
			next.Row.Value.Result.SnapshotRevision = rev
			erQAReplace(source, original[0], next)
			source = erQAStrict(t, source)
			pairs := erQAPairs(t, source)
			f, m := erQASeed(t, source, pairs, source.Revision)
			c, tx := interopOpen(t, f, false, sqliteio.Read)
			s := interopPrepare(t, tx, "SELECT snapshot_revision FROM event_receipts WHERE event_key=?", sqliteio.Text(next.Row.Key))
			ok, e := s.Step()
			if !ok || e != nil {
				t.Fatal("unsigned witness row")
			}
			b, e := s.Blob(0)
			n, parseErr := strconv.ParseUint(rev, 10, 64)
			if e != nil || parseErr != nil || len(b) != 8 || binary.BigEndian.Uint64(b) != n {
				t.Fatal("unsigned storage narrowed")
			}
			if e = s.Close(); e != nil {
				t.Fatal("unsigned witness cleanup")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			erQAReopen(t, f, m, pairs)
		})
	}
}

func TestSQLiteEventReceiptRowsCompleteBeforeAndFinalCeiling(t *testing.T) {
	_, source, pairs := erQASource(t)
	rich := pairs[0]
	rich.Row = erQAClone(rich.Row)
	rich.Row.Value.Result.UncertaintyIDs = []string{"one", "two", "one", ""}
	erQAReplace(source, pairs[0], rich)
	pairs = erQAPairs(t, erQAStrict(t, source))
	f, m := erQASeed(t, source, pairs, source.Revision)
	mutations := []struct {
		name   string
		change func(*erQAPair)
	}{
		{"fingerprint", func(p *erQAPair) { p.Row.Value.Fingerprint = strings.Repeat("f", 64) }},
		{"snapshot", func(p *erQAPair) { p.Row.Value.Result.SnapshotRevision = "0" }},
		{"actor-key", func(p *erQAPair) {
			for _, other := range pairs {
				if other.Row.Value.Result.Actor.Key != p.Row.Value.Result.Actor.Key {
					p.Row.Value.Result.Actor = other.Row.Value.Result.Actor
					return
				}
			}
			t.Fatal("different actor control")
		}},
		{"actor-generation", func(p *erQAPair) {
			for _, other := range pairs {
				if other.Row.Value.Result.Actor.Key == p.Row.Value.Result.Actor.Key && other.Row.Value.Result.Actor.Generation != p.Row.Value.Result.Actor.Generation {
					p.Row.Value.Result.Actor = other.Row.Value.Result.Actor
					return
				}
			}
			t.Fatal("different generation control")
		}},
		{"segment-null", func(p *erQAPair) { p.Row.Value.Result.SegmentID = nil }},
		{"segment-different", func(p *erQAPair) {
			for id := range source.Segments {
				if id != *p.Row.Value.Result.SegmentID {
					p.Row.Value.Result.SegmentID = &id
					return
				}
			}
			t.Fatal("different segment control")
		}},
		{"child-position", func(p *erQAPair) { p.Row.Value.Result.UncertaintyIDs[1] = "different" }},
		{"child-count", func(p *erQAPair) { p.Row.Value.Result.UncertaintyIDs = p.Row.Value.Result.UncertaintyIDs[:3] }},
		{"child-order", func(p *erQAPair) {
			p.Row.Value.Result.UncertaintyIDs[0], p.Row.Value.Result.UncertaintyIDs[1] = p.Row.Value.Result.UncertaintyIDs[1], p.Row.Value.Result.UncertaintyIDs[0]
		}},
		{"event-id", func(p *erQAPair) { p.ID = "different-valid-event-id" }},
		{"missing-key", func(p *erQAPair) { p.Row.Key = "different-valid-opaque-key" }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			before := rich
			before.Row = erQAClone(before.Row)
			tc.change(&before)
			// Full-state admission proves the expected row is otherwise valid. This is
			// a stale complete observation, not a validation-error substitute.
			control := erQAStateClone(t, source)
			erQAReplace(control, rich, before)
			erQAStrict(t, control)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			bgQACorrupt(t, sqliteAssertEventReceipt(tx, m.Revision, before.Row, before.ID))
			erQAMetaUnchanged(t, tx, f, m)
			interopRollback(t, tx)
			interopClose(t, c)
			erQAReopen(t, f, m, pairs)
		})
	}
	for _, ceiling := range []string{"0", "01", "18446744073709551616"} {
		t.Run("invalid-ceiling-"+ceiling, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Read)
			row, found, e := sqliteReadEventReceipt(tx, rich.Row.Key, ceiling)
			bgQAValidation(t, e)
			if found || !reflect.DeepEqual(row, sqliteEventReceiptRow{}) {
				t.Fatal("invalid ceiling leaked row")
			}
			bgQAValidation(t, sqliteAssertEventReceipt(tx, ceiling, rich.Row, rich.ID))
			bgQAValidation(t, sqliteValidateEventReceiptID(tx, rich.Row.Key, ceiling))
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	row, found, e := sqliteReadEventReceipt(tx, "absent-receipt", m.Revision)
	if e != nil || found || !reflect.DeepEqual(row, sqliteEventReceiptRow{}) {
		t.Fatal("missing public receipt")
	}
	bgQACorrupt(t, sqliteValidateEventReceiptID(tx, "absent-receipt", m.Revision))
	proposed := erQAFresh(rich, "above-ceiling")
	proposed.Row.Value.Result.SnapshotRevision = bump(m.Revision)
	d, e := sqliteInsertEventReceipt(tx, proposed.Row, proposed.ID)
	if e != nil || d != erQACharge(t, proposed.Row, proposed.ID) {
		t.Fatal("ceiling-free local insert")
	}
	bgQACorrupt(t, sqliteValidateEventReceiptID(tx, proposed.Row.Key, m.Revision))
	row, found, e = sqliteReadEventReceipt(tx, proposed.Row.Key, m.Revision)
	bgQACorrupt(t, e)
	if found || !reflect.DeepEqual(row, sqliteEventReceiptRow{}) {
		t.Fatal("above-ceiling usable output")
	}
	if erQACount(t, tx, "SELECT count(*) FROM event_receipts WHERE event_key=?", sqliteio.Text(proposed.Row.Key)) != 1 {
		t.Fatal("staged-unit premise")
	}
	erQAMetaUnchanged(t, tx, f, m)
	interopRollback(t, tx)
	interopClose(t, c)
	erQAReopen(t, f, m, pairs)
	// The same proposal with a valid selected final ceiling and explicit caller
	// metadata/nonce/charge composition commits; the helper owns none of them.
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	d, e = sqliteInsertEventReceipt(tx, proposed.Row, proposed.ID)
	if e != nil {
		t.Fatal("accepted staged unit")
	}
	next := metaQANext(t, m)
	next.Revision = bump(m.Revision)
	next.LogicalBytes += d
	if e = sqliteValidateEventReceiptID(tx, proposed.Row.Key, next.Revision); e != nil {
		t.Fatal("proposed final proof")
	}
	erQAMetaUnchanged(t, tx, f, m)
	if e = sqliteUpdateMeta(tx, m, next); e != nil {
		t.Fatal("explicit caller meta composition")
	}
	if e = tx.CheckForeignKeys(); e != nil || erQAAudit(t, tx) != next.LogicalBytes {
		t.Fatal("composed acceptance gates")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	erQAReopen(t, f, next, append(pairs, proposed))
}

func TestSQLiteEventReceiptRowsActualJSONPreparationAndRawLookup(t *testing.T) {
	_, baseline, pairs := erQASource(t)
	old := pairs[0]
	raw := erQAFresh(old, "repair")
	raw.Row.Key = "opaque-" + string([]byte{0xff, 0xfe}) + "\x00key"
	raw.ID = "repair-" + string([]byte{0xff})
	raw.Row.Value.Result.UncertaintyIDs = []string{"raw-" + string([]byte{0xff, 0xfe}), "", "\x00missing", "same", "same"}
	raw.Row.Value.Result.Actor.Key.SessionID = "raw-session-" + string([]byte{0xff})
	legacy := erQAStateClone(t, baseline)
	erQAReplace(legacy, old, raw)
	decoded := erQALegacyWrite(t, legacy, true)
	persisted := erQAMaterialized(t, raw.Row)
	if !reflect.DeepEqual(decoded.Receipts[persisted.Key], persisted.Value) {
		t.Fatal("complete legacy persisted projection differs")
	}
	f, m := erQASeed(t, baseline, pairs[1:], baseline.Revision)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	ref := raw.Row.Value.Result.Actor
	d, e := sqliteEnsureActorGeneration(tx, ref)
	if e != nil || d != bgQAGenerationCharge(t, ref) {
		t.Fatal("materialized generation dependency")
	}
	generationDelta := d
	input := erQAClone(raw.Row)
	row, found, e := sqliteReadEventReceipt(tx, raw.Row.Key, m.Revision)
	if e != nil || found || !reflect.DeepEqual(row, sqliteEventReceiptRow{}) {
		t.Fatal("raw lookup was normalized")
	}
	key, found, e := sqliteReadEventID(tx, raw.ID)
	if e != nil || found || key != "" {
		t.Fatal("raw EventID lookup was normalized")
	}
	d, e = sqliteInsertEventReceipt(tx, raw.Row, raw.ID)
	if e != nil || d != erQACharge(t, raw.Row, raw.ID) || !reflect.DeepEqual(raw.Row, input) {
		t.Fatal("final repair/charge/ownership")
	}
	if e = sqliteValidateEventReceiptID(tx, persisted.Key, m.Revision); e != nil {
		t.Fatal("materialized complete unit proof")
	}
	erQARead(t, tx, raw, m.Revision)
	if erQACount(t, tx, "SELECT count(*) FROM event_receipt_uncertainties WHERE event_key=?", sqliteio.Text(persisted.Key)) != int64(len(raw.Row.Value.Result.UncertaintyIDs)) || erQACount(t, tx, "SELECT count(*) FROM event_ids WHERE event_key=?", sqliteio.Text(persisted.Key)) != 1 {
		t.Fatal("all owner keys must use identical materialized bytes")
	}
	row, found, e = sqliteReadEventReceipt(tx, raw.Row.Key, m.Revision)
	if e != nil || found || !reflect.DeepEqual(row, sqliteEventReceiptRow{}) {
		t.Fatal("raw postwrite lookup changed")
	}
	key, found, e = sqliteReadEventID(tx, raw.ID)
	if e != nil || found || key != "" {
		t.Fatal("raw postwrite EventID lookup changed")
	}
	next := metaQANext(t, m)
	next.Revision = bump(m.Revision)
	next.LogicalBytes += generationDelta + d
	erQAMetaUnchanged(t, tx, f, m)
	if e = sqliteUpdateMeta(tx, m, next); e != nil {
		t.Fatal("explicit repair composition")
	}
	if e = tx.CheckForeignKeys(); e != nil || erQAAudit(t, tx) != next.LogicalBytes {
		t.Fatal("repair composition gates")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	erQAReopen(t, f, next, append(pairs[1:], raw))
}

func TestSQLiteEventReceiptRowsEventIDFinalBoundAndGenuineShortCollisions(t *testing.T) {
	_, baseline, pairs := erQASource(t)
	old := pairs[0]
	for _, tc := range []struct {
		name, id string
		valid    bool
	}{{"repaired-258", strings.Repeat(string([]byte{0xff}), 86), false}, {"repaired-256", strings.Repeat(string([]byte{0xff}), 85) + "x", true}} {
		t.Run(tc.name, func(t *testing.T) {
			proposal := erQAFresh(old, tc.name)
			proposal.ID = tc.id
			if !safeIdentifier(proposal.ID, 256) || len(bgQAPersistedString(t, proposal.ID)) != map[bool]int{true: 256, false: 258}[tc.valid] {
				t.Fatal("actual byte-boundary premise")
			}
			raw := erQAStateClone(t, baseline)
			erQAReplace(raw, old, proposal)
			erQALegacyWrite(t, raw, tc.valid)
			f, m := erQASeed(t, baseline, pairs, baseline.Revision)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			input := erQAClone(proposal.Row)
			d, e := sqliteInsertEventReceipt(tx, proposal.Row, proposal.ID)
			if tc.valid {
				if e != nil || d != erQACharge(t, proposal.Row, proposal.ID) {
					t.Fatal("valid 256-byte final ID refused")
				}
				erQARead(t, tx, proposal, m.Revision)
				if erQACount(t, tx, "SELECT length(CAST(event_id AS BLOB)) FROM event_ids WHERE event_key=?", sqliteio.Text(proposal.Row.Key)) != 256 {
					t.Fatal("stored ID byte length")
				}
			} else {
				bgQAValidation(t, e)
				if d != 0 || erQACount(t, tx, "SELECT count(*) FROM event_receipts WHERE event_key=?", sqliteio.Text(proposal.Row.Key)) != 0 || erQACount(t, tx, "SELECT count(*) FROM event_receipt_uncertainties WHERE event_key=?", sqliteio.Text(proposal.Row.Key)) != 0 || erQACount(t, tx, "SELECT count(*) FROM event_ids WHERE event_key=?", sqliteio.Text(proposal.Row.Key)) != 0 {
					t.Fatal("invalid final ID must refuse before any insert")
				}
			}
			if !reflect.DeepEqual(input, proposal.Row) {
				t.Fatal("boundary preparation mutated input")
			}
			erQAMetaUnchanged(t, tx, f, m)
			interopRollback(t, tx)
			interopClose(t, c)
			erQAReopen(t, f, m, pairs)
		})
	}
	for _, mode := range []string{"id", "receipt-key"} {
		t.Run("short-"+mode+"-collision", func(t *testing.T) {
			source := erQAStateClone(t, baseline)
			first := erQAFresh(old, "canonical-"+mode)
			second := erQAFresh(pairs[1], "raw-"+mode)
			if mode == "id" {
				first.ID = "short-\ufffd"
				second.ID = "short-" + string([]byte{0xff})
			} else {
				first.Row.Key = "short-\ufffd"
				second.Row.Key = "short-" + string([]byte{0xff})
			}
			erQAReplace(source, old, first)
			erQAReplace(source, pairs[1], second)
			if !safeIdentifier(first.ID, 256) || !safeIdentifier(second.ID, 256) {
				t.Fatal("short ID validity premise")
			}
			// Both complete raw map names are admitted; the actual store serializes a
			// duplicate decoded name. The strict next-read refusal is the real oracle.
			erQALegacyWrite(t, source, false)
			retained := append([]erQAPair{first}, pairs[2:]...)
			f, m := erQASeed(t, baseline, retained, baseline.Revision)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if mode == "id" {
				key, found, e := sqliteReadEventID(tx, second.ID)
				if e != nil || found || key != "" {
					t.Fatal("raw short ID should remain absent")
				}
			} else {
				row, found, e := sqliteReadEventReceipt(tx, second.Row.Key, m.Revision)
				if e != nil || found || !reflect.DeepEqual(row, sqliteEventReceiptRow{}) {
					t.Fatal("raw short key should remain absent")
				}
			}
			d, e := sqliteInsertEventReceipt(tx, second.Row, second.ID)
			erQAConstraint(t, e, 1555)
			if d != 0 {
				t.Fatal("collision returned usable delta")
			}
			erQARead(t, tx, first, m.Revision)
			if mode == "id" && erQACount(t, tx, "SELECT count(*) FROM event_receipts WHERE event_key=?", sqliteio.Text(second.Row.Key)) != 1 {
				t.Fatal("late ID collision must reach final insertion")
			}
			erQAMetaUnchanged(t, tx, f, m)
			interopRollback(t, tx)
			interopClose(t, c)
			erQAReopen(t, f, m, retained)
		})
	}
}

func TestSQLiteEventReceiptRowsDistinctNativeUniquenessAndCallerRollback(t *testing.T) {
	_, source, pairs := erQASource(t)
	rich := pairs[0]
	rich.Row = erQAClone(rich.Row)
	rich.Row.Value.Result.UncertaintyIDs = []string{"retained", "retained", "\x00opaque"}
	erQAReplace(source, pairs[0], rich)
	pairs = erQAPairs(t, erQAStrict(t, source))
	f, m := erQASeed(t, source, pairs, source.Revision)
	for _, mode := range []string{"receipt-pk", "id-pk", "inverse-key-unique"} {
		t.Run(mode, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			p := erQAFresh(rich, mode)
			code := int32(1555)
			if mode == "receipt-pk" {
				p.Row.Key = rich.Row.Key
			} else if mode == "id-pk" {
				p.ID = rich.ID
			} else {
				code = 2067
				if erQACount(t, tx, "SELECT count(*) FROM event_receipts WHERE event_key=?", sqliteio.Text(p.Row.Key)) != 0 || erQACount(t, tx, "SELECT count(*) FROM event_ids WHERE event_id=?", sqliteio.Text(p.ID)) != 0 {
					t.Fatal("future-key/fresh-ID premise")
				}
				// Deliberately deferred FK, not a committed duplicate receipt. Therefore
				// only the final event_ids UNIQUE(event_key) can be the intended failure.
				interopDone(t, tx, "INSERT INTO event_ids(event_id,event_key) VALUES(?,?)", sqliteio.Text("synthetic-premapped-id"), sqliteio.Text(p.Row.Key))
			}
			d, e := sqliteInsertEventReceipt(tx, p.Row, p.ID)
			erQAConstraint(t, e, code)
			if d != 0 {
				t.Fatal("failed unit returned charge")
			}
			if mode != "receipt-pk" {
				if erQACount(t, tx, "SELECT count(*) FROM event_receipts WHERE event_key=?", sqliteio.Text(p.Row.Key)) != 1 || erQACount(t, tx, "SELECT count(*) FROM event_receipt_uncertainties WHERE event_key=?", sqliteio.Text(p.Row.Key)) != int64(len(p.Row.Value.Result.UncertaintyIDs)) {
					t.Fatal("final-ID failure not isolated from scalar/children")
				}
				if mode == "inverse-key-unique" {
					if erQACount(t, tx, "SELECT count(*) FROM event_ids WHERE event_id=?", sqliteio.Text(p.ID)) != 0 || erQACount(t, tx, "SELECT count(*) FROM event_ids WHERE event_id='synthetic-premapped-id'") != 1 {
						t.Fatal("inverse UNIQUE failure premise changed")
					}
				}
			}
			erQAMetaUnchanged(t, tx, f, m)
			interopRollback(t, tx)
			interopClose(t, c)
			erQAReopen(t, f, m, pairs)
			c, tx = interopOpen(t, f, false, sqliteio.Read)
			if erQACount(t, tx, "SELECT count(*) FROM event_ids WHERE event_id='synthetic-premapped-id'") != 0 {
				t.Fatal("deferred premapping survived rollback")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}

func TestSQLiteEventReceiptRowsExplicitInvalidInputIsolatedBeforeCommit(t *testing.T) {
	_, source, pairs := erQASource(t)
	f, m := erQASeed(t, source, pairs, source.Revision)
	cases := []struct {
		name   string
		change func(*erQAPair)
	}{
		{"contract-version", func(p *erQAPair) { p.Row.Value.Result.ContractVersion = 2 }},
		{"response-only-duplicate", func(p *erQAPair) { p.Row.Value.Result.Disposition = "duplicate" }},
		{"nil-uncertainty-array", func(p *erQAPair) { p.Row.Value.Result.UncertaintyIDs = nil }},
		{"fingerprint-short", func(p *erQAPair) { p.Row.Value.Fingerprint = strings.Repeat("a", 63) }},
		{"fingerprint-nonhex", func(p *erQAPair) { p.Row.Value.Fingerprint = strings.Repeat("g", 64) }},
		{"snapshot-leading-zero", func(p *erQAPair) { p.Row.Value.Result.SnapshotRevision = "01" }},
		{"snapshot-overflow", func(p *erQAPair) { p.Row.Value.Result.SnapshotRevision = "18446744073709551616" }},
		{"actor-source", func(p *erQAPair) { p.Row.Value.Result.Actor.Key.Source = "invented" }},
		{"actor-session-empty", func(p *erQAPair) { p.Row.Value.Result.Actor.Key.SessionID = "" }},
		{"actor-generation-zero", func(p *erQAPair) { p.Row.Value.Result.Actor.Generation = "0" }},
		{"actor-generation-leading-zero", func(p *erQAPair) { p.Row.Value.Result.Actor.Generation = "01" }},
		{"event-id-empty", func(p *erQAPair) { p.ID = "" }},
		{"event-id-nul", func(p *erQAPair) { p.ID = "synthetic-id\x00invalid" }},
		{"event-id-raw-too-long", func(p *erQAPair) { p.ID = strings.Repeat("x", 257) }},
		{"event-id-final-too-long", func(p *erQAPair) { p.ID = strings.Repeat(string([]byte{0xff}), 86) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			control := erQAFresh(pairs[0], tc.name)
			// Each fresh otherwise-valid proposal first succeeds and rolls back; no
			// unrelated receipt/ID uniqueness can stand in for explicit validation.
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			d, e := sqliteInsertEventReceipt(tx, control.Row, control.ID)
			if e != nil || d != erQACharge(t, control.Row, control.ID) {
				t.Fatal("otherwise-valid control", bgQAFixtureCode(e))
			}
			erQARead(t, tx, control, m.Revision)
			interopRollback(t, tx)
			interopClose(t, c)
			invalid := control
			invalid.Row = erQAClone(control.Row)
			tc.change(&invalid)
			owned := erQAClone(invalid.Row)
			c, tx = interopOpen(t, f, false, sqliteio.Write)
			d, e = sqliteInsertEventReceipt(tx, invalid.Row, invalid.ID)
			bgQAValidation(t, e)
			interopSafeError(t, e, "synthetic-receipt-", "synthetic-id")
			if d != 0 || !reflect.DeepEqual(invalid.Row, owned) || erQACount(t, tx, "SELECT count(*) FROM event_receipts WHERE event_key=?", sqliteio.Text(control.Row.Key)) != 0 {
				t.Fatal("invalid admission outputs/input/early insertion")
			}
			d, e = sqliteEventReceiptCharge(invalid.Row, invalid.ID)
			bgQAValidation(t, e)
			if d != 0 {
				t.Fatal("invalid pure charge returned usable delta")
			}
			erQAMetaUnchanged(t, tx, f, m)
			interopRollback(t, tx)
			interopClose(t, c)
			erQAReopen(t, f, m, pairs)
		})
	}
}

func erQAShadow(t *testing.T, tx *sqliteio.Tx, table, columns string) {
	t.Helper()
	interopDone(t, tx, "ALTER TABLE "+table+" RENAME TO qa_original_"+table)
	interopDone(t, tx, "CREATE TABLE "+table+"("+columns+")")
	interopDone(t, tx, "INSERT INTO "+table+"("+columns+") SELECT "+columns+" FROM qa_original_"+table)
	// This rollback-only non-STRICT table admits impossible stored kinds for
	// decoder QA. It is never published as the actual schema or import source.
}
func erQAZeros(t *testing.T, row sqliteEventReceiptRow, found bool, err error) {
	t.Helper()
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(row, sqliteEventReceiptRow{}) {
		t.Fatal("corrupt evidence returned usable historical result")
	}
}
func TestSQLiteEventReceiptRowsSelectedCorruptionAndFiniteIDPresenceScope(t *testing.T) {
	_, source, pairs := erQASource(t)
	rich := pairs[0]
	rich.Row = erQAClone(rich.Row)
	rich.Row.Value.Result.UncertaintyIDs = []string{"first", "second"}
	erQAReplace(source, pairs[0], rich)
	pairs = erQAPairs(t, erQAStrict(t, source))
	f, m := erQASeed(t, source, pairs, source.Revision)
	cases := []struct {
		name       string
		mutate     func(*testing.T, *sqliteio.Tx)
		idPresence bool
	}{
		{"fingerprint-kind", func(t *testing.T, tx *sqliteio.Tx) {
			erQAShadow(t, tx, "event_receipts", erQAReceiptColumns)
			interopDone(t, tx, "UPDATE event_receipts SET fingerprint=? WHERE event_key=?", sqliteio.Blob([]byte(strings.Repeat("a", 64))), sqliteio.Text(rich.Row.Key))
		}, true},
		{"contract-kind", func(t *testing.T, tx *sqliteio.Tx) {
			erQAShadow(t, tx, "event_receipts", erQAReceiptColumns)
			interopDone(t, tx, "UPDATE event_receipts SET contract_version=? WHERE event_key=?", sqliteio.Text("1"), sqliteio.Text(rich.Row.Key))
		}, true},
		{"contract-value", func(t *testing.T, tx *sqliteio.Tx) {
			erQAShadow(t, tx, "event_receipts", erQAReceiptColumns)
			interopDone(t, tx, "UPDATE event_receipts SET contract_version=2 WHERE event_key=?", sqliteio.Text(rich.Row.Key))
		}, true},
		{"disposition-value", func(t *testing.T, tx *sqliteio.Tx) {
			erQAShadow(t, tx, "event_receipts", erQAReceiptColumns)
			interopDone(t, tx, "UPDATE event_receipts SET disposition='duplicate' WHERE event_key=?", sqliteio.Text(rich.Row.Key))
		}, true},
		{"snapshot-width", func(t *testing.T, tx *sqliteio.Tx) {
			erQAShadow(t, tx, "event_receipts", erQAReceiptColumns)
			interopDone(t, tx, "UPDATE event_receipts SET snapshot_revision=? WHERE event_key=?", sqliteio.Blob([]byte{1, 2}), sqliteio.Text(rich.Row.Key))
		}, true},
		{"snapshot-above-ceiling", func(t *testing.T, tx *sqliteio.Tx) {
			interopDone(t, tx, "UPDATE event_receipts SET snapshot_revision=? WHERE event_key=?", interopCounter(t, bump(m.Revision)), sqliteio.Text(rich.Row.Key))
		}, true},
		{"actor-key-kind", func(t *testing.T, tx *sqliteio.Tx) {
			erQAShadow(t, tx, "event_receipts", erQAReceiptColumns)
			interopDone(t, tx, "UPDATE event_receipts SET actor_key=? WHERE event_key=?", sqliteio.Blob([]byte(actorKey(rich.Row.Value.Result.Actor.Key))), sqliteio.Text(rich.Row.Key))
		}, true},
		{"generation-width", func(t *testing.T, tx *sqliteio.Tx) {
			erQAShadow(t, tx, "event_receipts", erQAReceiptColumns)
			interopDone(t, tx, "UPDATE event_receipts SET actor_generation=? WHERE event_key=?", sqliteio.Blob([]byte{1}), sqliteio.Text(rich.Row.Key))
		}, true},
		{"segment-kind", func(t *testing.T, tx *sqliteio.Tx) {
			erQAShadow(t, tx, "event_receipts", erQAReceiptColumns)
			interopDone(t, tx, "UPDATE event_receipts SET segment_id=? WHERE event_key=?", sqliteio.Blob([]byte(*rich.Row.Value.Result.SegmentID)), sqliteio.Text(rich.Row.Key))
		}, true},
		{"missing-generation", func(t *testing.T, tx *sqliteio.Tx) {
			ref := rich.Row.Value.Result.Actor
			interopDone(t, tx, "DELETE FROM actor_generations WHERE actor_key=? AND generation=?", sqliteio.Text(actorKey(ref.Key)), interopCounter(t, ref.Generation))
		}, true},
		{"missing-segment", func(t *testing.T, tx *sqliteio.Tx) {
			interopDone(t, tx, "DELETE FROM segments WHERE segment_id=?", sqliteio.Text(*rich.Row.Value.Result.SegmentID))
		}, true},
		{"child-ordinal-gap", func(t *testing.T, tx *sqliteio.Tx) {
			interopDone(t, tx, "UPDATE event_receipt_uncertainties SET ordinal=3 WHERE event_key=? AND ordinal=1", sqliteio.Text(rich.Row.Key))
		}, true},
		{"child-ordinal-kind", func(t *testing.T, tx *sqliteio.Tx) {
			erQAShadow(t, tx, "event_receipt_uncertainties", erQAChildColumns)
			interopDone(t, tx, "UPDATE event_receipt_uncertainties SET ordinal=? WHERE event_key=? AND ordinal=1", sqliteio.Text("1"), sqliteio.Text(rich.Row.Key))
		}, true},
		{"child-reference-kind", func(t *testing.T, tx *sqliteio.Tx) {
			erQAShadow(t, tx, "event_receipt_uncertainties", erQAChildColumns)
			interopDone(t, tx, "UPDATE event_receipt_uncertainties SET uncertainty_id=? WHERE event_key=? AND ordinal=1", sqliteio.Blob([]byte("second")), sqliteio.Text(rich.Row.Key))
		}, true},
		{"child-invalid-utf8", func(t *testing.T, tx *sqliteio.Tx) {
			interopDone(t, tx, "UPDATE event_receipt_uncertainties SET uncertainty_id=? WHERE event_key=? AND ordinal=1", sqliteio.Text(string([]byte{0xff})), sqliteio.Text(rich.Row.Key))
		}, true},
		{"selected-child-owner", func(t *testing.T, tx *sqliteio.Tx) {
			// A rollback-only altered collation makes the mandated WHERE select an
			// otherwise well-typed wrong owner. This isolates the owner-equality check.
			interopDone(t, tx, "ALTER TABLE event_receipt_uncertainties RENAME TO qa_original_event_receipt_uncertainties")
			interopDone(t, tx, "CREATE TABLE event_receipt_uncertainties(event_key TEXT COLLATE NOCASE,ordinal INTEGER,uncertainty_id TEXT)")
			interopDone(t, tx, "INSERT INTO event_receipt_uncertainties SELECT event_key,ordinal,uncertainty_id FROM qa_original_event_receipt_uncertainties")
			wrong := strings.ToUpper(rich.Row.Key)
			if wrong == rich.Row.Key {
				t.Fatal("case-distinct owner premise")
			}
			interopDone(t, tx, "UPDATE event_receipt_uncertainties SET event_key=? WHERE event_key=?", sqliteio.Text(wrong), sqliteio.Text(rich.Row.Key))
			if erQACount(t, tx, "SELECT count(*) FROM event_receipt_uncertainties WHERE event_key=?", sqliteio.Text(rich.Row.Key)) != 2 {
				t.Fatal("wrong owner not selected")
			}
		}, true},
		{"selected-inverse-owner", func(t *testing.T, tx *sqliteio.Tx) {
			interopDone(t, tx, "ALTER TABLE event_ids RENAME TO qa_original_event_ids")
			interopDone(t, tx, "CREATE TABLE event_ids(event_id TEXT,event_key TEXT COLLATE NOCASE)")
			interopDone(t, tx, "INSERT INTO event_ids SELECT event_id,event_key FROM qa_original_event_ids")
			wrong := strings.ToUpper(rich.Row.Key)
			if wrong == rich.Row.Key {
				t.Fatal("case-distinct inverse owner premise")
			}
			interopDone(t, tx, "UPDATE event_ids SET event_key=? WHERE event_id=?", sqliteio.Text(wrong), sqliteio.Text(rich.ID))
			if erQACount(t, tx, "SELECT count(*) FROM event_ids WHERE event_key=?", sqliteio.Text(rich.Row.Key)) != 1 {
				t.Fatal("wrong inverse owner not selected")
			}
			proof := interopPrepare(t, tx, "SELECT event_id,event_key FROM event_ids WHERE event_key=?", sqliteio.Text(rich.Row.Key))
			selected, e := proof.Step()
			if !selected || e != nil || proof.ColumnCount() != 2 {
				t.Fatal("selected inverse projection premise")
			}
			for col, want := range []string{rich.ID, wrong} {
				kind, e := proof.Kind(col)
				if e != nil || kind != sqliteio.TextKind {
					t.Fatal("inverse wrong-owner witness must be typed TEXT")
				}
				value, e := proof.Text(col)
				if e != nil || value != want {
					t.Fatal("inverse wrong-owner exact selected bytes")
				}
			}
			if selected, e = proof.Step(); selected || e != nil {
				t.Fatal("wrong inverse witness not singleton/DONE")
			}
			if e = proof.Close(); e != nil {
				t.Fatal("inverse witness cleanup")
			}
		}, false},
		{"missing-inverse-id", func(t *testing.T, tx *sqliteio.Tx) {
			interopDone(t, tx, "DELETE FROM event_ids WHERE event_id=?", sqliteio.Text(rich.ID))
		}, false},
		{"inverse-id-invalid", func(t *testing.T, tx *sqliteio.Tx) {
			interopDone(t, tx, "UPDATE event_ids SET event_id=? WHERE event_id=?", sqliteio.Text("bad\x00id"), sqliteio.Text(rich.ID))
		}, false},
		{"inverse-id-kind", func(t *testing.T, tx *sqliteio.Tx) {
			erQAShadow(t, tx, "event_ids", "event_id,event_key")
			interopDone(t, tx, "UPDATE event_ids SET event_id=? WHERE event_key=?", sqliteio.Blob([]byte(rich.ID)), sqliteio.Text(rich.Row.Key))
		}, false},
		{"inverse-not-singleton", func(t *testing.T, tx *sqliteio.Tx) {
			erQAShadow(t, tx, "event_ids", "event_id,event_key")
			interopDone(t, tx, "INSERT INTO event_ids VALUES(?,?)", sqliteio.Text("second-valid-inverse"), sqliteio.Text(rich.Row.Key))
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			erQARead(t, tx, rich, m.Revision)
			tc.mutate(t, tx)
			row, found, e := sqliteReadEventReceipt(tx, rich.Row.Key, m.Revision)
			erQAZeros(t, row, found, e)
			bgQACorrupt(t, sqliteValidateEventReceiptID(tx, rich.Row.Key, m.Revision))
			if tc.idPresence {
				key, found, e := sqliteReadEventID(tx, rich.ID)
				if e != nil || !found || key != rich.Row.Key {
					t.Fatal("ID presence reader recursively validated historical result", bgQAFixtureCode(e))
				}
			}
			erQAMetaUnchanged(t, tx, f, m)
			interopRollback(t, tx)
			interopClose(t, c)
			erQAReopen(t, f, m, pairs)
		})
	}
	t.Run("unrelated-corrupt-result-remains-unselected", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		other := pairs[len(pairs)-1]
		if other.Row.Key == rich.Row.Key {
			t.Fatal("unrelated control")
		}
		erQAShadow(t, tx, "event_receipts", erQAReceiptColumns)
		interopDone(t, tx, "UPDATE event_receipts SET fingerprint='invalid' WHERE event_key=?", sqliteio.Text(other.Row.Key))
		erQARead(t, tx, rich, m.Revision)
		key, found, e := sqliteReadEventID(tx, other.ID)
		if e != nil || !found || key != other.Row.Key {
			t.Fatal("presence-only ID loaded unrelated result")
		}
		row, found, e := sqliteReadEventReceipt(tx, other.Row.Key, m.Revision)
		erQAZeros(t, row, found, e)
		interopRollback(t, tx)
		interopClose(t, c)
		erQAReopen(t, f, m, pairs)
	})
	t.Run("id-target-missing", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		interopDone(t, tx, "DELETE FROM event_receipts WHERE event_key=?", sqliteio.Text(rich.Row.Key))
		key, found, e := sqliteReadEventID(tx, rich.ID)
		bgQACorrupt(t, e)
		if key != "" || found {
			t.Fatal("dangling ID returned usable key")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		erQAReopen(t, f, m, pairs)
	})
	t.Run("id-selected-value-kind", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		erQAShadow(t, tx, "event_ids", "event_id,event_key")
		interopDone(t, tx, "UPDATE event_ids SET event_key=? WHERE event_id=?", sqliteio.Blob([]byte(rich.Row.Key)), sqliteio.Text(rich.ID))
		key, found, e := sqliteReadEventID(tx, rich.ID)
		bgQACorrupt(t, e)
		if key != "" || found {
			t.Fatal("wrong-kind selected ID mapping output")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		erQAReopen(t, f, m, pairs)
	})
}

func TestSQLiteEventReceiptRowsActualLegacyReplayPriorityAndGapNonLedger(t *testing.T) {
	h, source, pairs := erQASource(t)
	first := qaEvent("epoch-A", "1", "1", "work", qaBindingA)
	saved := source.Receipts[eventKey(first)].Result
	before, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal("owned replay evidence read")
	}
	h.clockErr = failure("clock_unavailable")
	result, err := h.service.Ingest(context.Background(), first)
	want := saved
	want.Disposition = "duplicate"
	if err != nil || !reflect.DeepEqual(result, want) {
		t.Fatal("exact legacy replay did not bypass terminal/new-generation/clock")
	}
	after, err := os.ReadFile(h.path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("exact replay changed public retained state")
	}
	changed := first
	changed.CWD = "/synthetic/changed-input"
	_, err = h.service.Ingest(context.Background(), changed)
	qaCode(t, err, "event_conflict")
	conflicting := qaEvent("fresh-event-id-conflict", "1", "1", "work", qaBindingA)
	conflicting.EventID = first.EventID
	_, err = h.service.Ingest(context.Background(), conflicting)
	qaCode(t, err, "event_conflict")
	after, err = os.ReadFile(h.path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("raw identity/fingerprint conflicts changed history")
	}
	f, m := erQASeed(t, source, pairs, source.Revision)
	erQAReopen(t, f, m, pairs)
	// There is deliberately no new SQL reducer mock here. The actual accepted
	// operation/error oracle proves why a successful receipt is not a safety log.
	gapHarness := qaNew(t)
	gapHarness.seed()
	first = qaEvent("gap-actor", "1", "1", "work", qaBindingA)
	gapHarness.ingest(0, first)
	prior := bgQAReadLegacy(t, gapHarness.service)
	gap := qaEvent("gap-actor", "1", "3", "observe_work", "")
	gapHarness.at(10)
	_, err = gapHarness.service.Ingest(context.Background(), gap)
	qaCode(t, err, "event_gap")
	final := bgQAReadLegacy(t, gapHarness.service)
	if final.Revision == prior.Revision || final.Actors[actorKey(gap.Actor)].Health != "order_blocked" || len(final.Uncertainties) == 0 {
		t.Fatal("real gap did not retain durable quarantine")
	}
	if _, ok := final.Receipts[eventKey(gap)]; ok {
		t.Fatal("gap fabricated applied receipt")
	}
	if _, ok := final.EventIDs[gap.EventID]; ok {
		t.Fatal("gap fabricated success EventID")
	}
	if !reflect.DeepEqual(final.Receipts[eventKey(first)], prior.Receipts[eventKey(first)]) || len(final.Receipts) != len(prior.Receipts) || len(final.EventIDs) != len(prior.EventIDs) {
		t.Fatal("safety outcome rewrote successful ledger")
	}
}

func TestSQLiteEventReceiptRowsCancellationAndTerminalOutputs(t *testing.T) {
	_, source, pairs := erQASource(t)
	f, m := erQASeed(t, source, pairs, source.Revision)
	for _, name := range []string{"read-receipt", "read-id", "insert", "assert", "final-proof"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, e := sqliteio.Open(ctx, f.directory, f.database, sqliteio.Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			if e != nil {
				t.Fatal("cancellation fixture open")
			}
			t.Cleanup(func() { _ = c.Close(context.Background()) })
			tx, e := c.Begin(ctx, sqliteio.Write)
			if e != nil {
				t.Fatal("cancellation fixture begin")
			}
			t.Cleanup(func() { _ = tx.Rollback() })
			staged := erQAFresh(pairs[0], "pre-cancel")
			if d, e := sqliteInsertEventReceipt(tx, staged.Row, staged.ID); e != nil || d != erQACharge(t, staged.Row, staged.ID) {
				t.Fatal("pre-cancel successful unit control")
			}
			erQARead(t, tx, staged, m.Revision)
			cancel()
			p := erQAFresh(pairs[0], "cancel")
			switch name {
			case "read-receipt":
				row, found, err := sqliteReadEventReceipt(tx, pairs[0].Row.Key, m.Revision)
				e = err
				if found || !reflect.DeepEqual(row, sqliteEventReceiptRow{}) {
					t.Fatal("cancelled read output")
				}
			case "read-id":
				key, found, err := sqliteReadEventID(tx, pairs[0].ID)
				e = err
				if found || key != "" {
					t.Fatal("cancelled ID output")
				}
			case "insert":
				d, err := sqliteInsertEventReceipt(tx, p.Row, p.ID)
				e = err
				if d != 0 {
					t.Fatal("cancelled insertion delta")
				}
			case "assert":
				e = sqliteAssertEventReceipt(tx, m.Revision, pairs[0].Row, pairs[0].ID)
			case "final-proof":
				e = sqliteValidateEventReceiptID(tx, pairs[0].Row.Key, m.Revision)
			}
			if !errors.Is(e, context.Canceled) {
				t.Fatalf("cancel identity lost type=%T code=%s", e, bgQAFixtureCode(e))
			}
			interopSafeError(t, e, "synthetic-receipt-cancel")
			if err := tx.Rollback(); err != nil {
				interopSafeError(t, err)
			}
			if err := c.Close(context.Background()); err != nil {
				t.Fatal("owned cancelled connection cleanup")
			}
			erQAReopen(t, f, m, pairs)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	d, e := sqliteInsertEventReceipt(tx, erQAFresh(pairs[0], "read-only").Row, "read-only-id")
	var checked *sqliteio.Error
	if d != 0 || !errors.As(e, &checked) || checked.Category == sqliteio.Constraint {
		t.Fatal("read-only caller refusal lost adapter evidence")
	}
	interopSafeError(t, e, "synthetic-receipt-read-only")
	interopRollback(t, tx)
	interopClose(t, c)
	erQAReopen(t, f, m, pairs)
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	interopRollback(t, tx)
	row, found, e := sqliteReadEventReceipt(tx, pairs[0].Row.Key, m.Revision)
	var native *sqliteio.Error
	if !errors.As(e, &native) || found || !reflect.DeepEqual(row, sqliteEventReceiptRow{}) {
		t.Fatal("terminal checked error/output")
	}
	key, found, e := sqliteReadEventID(tx, pairs[0].ID)
	if !errors.As(e, &native) || found || key != "" {
		t.Fatal("terminal ID output")
	}
	d, e = sqliteInsertEventReceipt(tx, erQAFresh(pairs[0], "terminal").Row, "terminal-id")
	if !errors.As(e, &native) || d != 0 {
		t.Fatal("terminal insert output")
	}
	if e = sqliteAssertEventReceipt(tx, m.Revision, pairs[0].Row, pairs[0].ID); !errors.As(e, &native) {
		t.Fatal("terminal assertion")
	}
	if e = sqliteValidateEventReceiptID(tx, pairs[0].Row.Key, m.Revision); !errors.As(e, &native) {
		t.Fatal("terminal final proof")
	}
	interopClose(t, c)
	erQAReopen(t, f, m, pairs)
}
