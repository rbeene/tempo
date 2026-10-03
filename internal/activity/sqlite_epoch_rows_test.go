//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent source-first QA for the inactive epoch-only row slice. The
// legacy reducer/file store supplies the semantic oracle; the SQL projection
// and charge oracle below use literal schema facts, never producer helpers.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const epQAColumns = "epoch_id,computer_id,account_id,user_id,project_id,task_id,timezone,creation_ordinal,group_order,anchor_capability,anchor_wall_sec,anchor_wall_nsec,anchor_wall_json,anchor_epoch,anchor_elapsed_raw,anchor_awake_raw,anchor_elapsed,anchor_awake"

func epQAClone(row sqliteEpochRow) sqliteEpochRow {
	for _, field := range []**string{&row.Value.Anchor.Epoch, &row.Value.Anchor.ElapsedNS, &row.Value.Anchor.AwakeNS} {
		if *field != nil {
			copy := **field
			*field = &copy
		}
	}
	return row
}

func epQAPersisted(t *testing.T, row sqliteEpochRow) sqliteEpochRow {
	t.Helper()
	b, err := json.Marshal(row.Value)
	if err != nil {
		t.Fatal("legacy epoch marshal failed")
	}
	var value timelineEpoch
	if err := json.Unmarshal(b, &value); err != nil {
		t.Fatal("legacy epoch unmarshal failed")
	}
	return sqliteEpochRow{Ordinal: row.Ordinal, Value: value}
}

func epQAGroup(t *testing.T, value timelineEpoch) string {
	t.Helper()
	b, err := json.Marshal(value.Attribution)
	if err != nil {
		t.Fatal("attribution oracle marshal failed")
	}
	return value.ComputerID + "/" + string(b)
}

func epQACharge(t *testing.T, raw sqliteEpochRow) int64 {
	t.Helper()
	e := epQAPersisted(t, raw).Value
	wall, err := e.Anchor.WallUTC.MarshalJSON()
	if err != nil {
		t.Fatal("time oracle marshal failed")
	}
	a := e.Attribution
	// 32 row bytes +18 kind bytes +3 INTEGER widths +13 TEXT length
	// headers +2 BLOB length headers +2 eight-byte counter payloads.
	n := int64(210)
	for _, value := range []string{e.ID, e.ComputerID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, epQAGroup(t, e), e.Anchor.Capability, string(wall), *e.Anchor.Epoch, *e.Anchor.ElapsedNS, *e.Anchor.AwakeNS} {
		n += int64(len(value))
	}
	return n
}

func epQAStoredCharge(t *testing.T, tx *sqliteio.Tx) int64 {
	t.Helper()
	s := interopPrepare(t, tx, "SELECT "+epQAColumns+" FROM epochs ORDER BY creation_ordinal")
	var total int64
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal("stored charge step failed")
		}
		if !row {
			break
		}
		if s.ColumnCount() != 18 {
			t.Fatal("epoch projection width changed")
		}
		total += 210
		for column := 0; column < 18; column++ {
			want := sqliteio.TextKind
			if column == 7 || column == 10 || column == 11 {
				want = sqliteio.IntegerKind
			} else if column == 16 || column == 17 {
				want = sqliteio.BlobKind
			}
			kind, err := s.Kind(column)
			if err != nil || kind != want {
				t.Fatalf("epoch column %d has wrong stored kind", column)
			}
			if want == sqliteio.TextKind {
				value, err := s.Text(column)
				if err != nil {
					t.Fatal("stored text read failed")
				}
				total += int64(len(value))
			} else if want == sqliteio.BlobKind {
				value, err := s.Blob(column)
				if err != nil || len(value) != 8 {
					t.Fatal("stored epoch counter width changed")
				}
			}
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal("stored charge cleanup failed")
	}
	return total
}

func epQARows(source *state) []sqliteEpochRow {
	rows := make([]sqliteEpochRow, 0, len(source.Epochs))
	for ordinal, value := range source.Epochs {
		rows = append(rows, epQAClone(sqliteEpochRow{Ordinal: int64(ordinal), Value: *value}))
	}
	return rows
}

func epQALegacy(t *testing.T) (*qaHarness, *state) {
	t.Helper()
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("epoch-A", "1", "1", "work", qaBindingA))
	h.ingest(5, qaEvent("epoch-A", "1", "2", "observe_work", ""))
	h.ingest(10, qaEvent("epoch-A", "1", "3", "wait_user", ""))
	h.ingest(20, qaEvent("epoch-A", "1", "4", "work", ""))
	h.ingest(30, qaEvent("epoch-A", "1", "5", "finish", ""))
	h.ingest(40, qaEvent("epoch-A", "2", "1", "work", qaBindingA))
	h.ingest(45, qaEvent("epoch-A", "2", "2", "observe_work", ""))
	h.ingest(50, qaEvent("epoch-A", "2", "3", "finish", ""))
	// Distinct binding IDs still share one immutable attribution coordinate.
	h.ingest(55, qaEvent("epoch-B", "1", "1", "work", qaBindingB))
	h.ingest(60, qaEvent("epoch-B", "1", "2", "finish", ""))
	source := bgQAReadLegacy(t, h.service)
	if len(source.Epochs) != 1 || len(source.Segments) < 3 || len(source.Receipts) < 8 {
		t.Fatal("real reducer fixture lacks retained epoch/history controls")
	}
	return h, source
}

func epQASeed(t *testing.T, source *state, rows []sqliteEpochRow) (interopFixture, sqliteStoreMeta) {
	t.Helper()
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal("schema bootstrap failed")
	}
	m := interopMeta(f)
	m.ComputerID, m.Revision, m.SyncEnabled = source.ComputerID, source.Revision, source.SyncEnabled
	m.LogicalBytes = int64(114 + len(source.ComputerID) + len(f.authority) + len(f.database))
	for _, row := range bgQARowsFromLegacy(source) {
		delta, err := sqliteInsertBinding(tx, row)
		if err != nil || delta != bgQABindingCharge(t, row) {
			t.Fatal("real binding bootstrap failed")
		}
		m.LogicalBytes += delta
	}
	for _, row := range rows {
		delta, err := sqliteInsertEpoch(tx, row)
		if err != nil || delta != epQACharge(t, row) {
			t.Fatalf("epoch bootstrap charge differs: delta=%d code=%s", delta, bgQAFixtureCode(err))
		}
		m.LogicalBytes += delta
	}
	// The real source revision is imported once. This never jumps an existing
	// metadata revision through sqliteUpdateMeta to create a counter fixture.
	if err := sqliteInsertMeta(tx, m); err != nil {
		t.Fatal("final metadata bootstrap failed")
	}
	if bgQAStoredCharge(t, tx)+epQAStoredCharge(t, tx) != m.LogicalBytes {
		t.Fatal("independent bootstrap charge differs")
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal("bootstrap foreign key audit failed")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return f, m
}

func epQAReopen(t *testing.T, f interopFixture, meta sqliteStoreMeta, rows []sqliteEpochRow) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	if interopCount(t, tx, "SELECT count(*) FROM epochs") != int64(len(rows)) {
		t.Fatal("epoch row count changed")
	}
	for _, want := range rows {
		got, found, err := sqliteReadEpoch(tx, want.Value.ComputerID, want.Value.ID)
		if err != nil || !found || !reflect.DeepEqual(got, epQAPersisted(t, want)) {
			t.Fatalf("cold epoch differs: found=%t code=%s", found, bgQAFixtureCode(err))
		}
		legacy, err := json.Marshal(epQAPersisted(t, want).Value)
		if err != nil {
			t.Fatal("legacy JSON oracle encode failed")
		}
		actual, err := json.Marshal(got.Value)
		if err != nil || !bytes.Equal(legacy, actual) {
			t.Fatal("cold epoch changed strict legacy JSON")
		}
	}
	gotMeta, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(gotMeta, meta) || bgQAStoredCharge(t, tx)+epQAStoredCharge(t, tx) != meta.LogicalBytes {
		t.Fatal("cold metadata/charge changed")
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func epQAFresh(row sqliteEpochRow, ordinal int64, suffix int) sqliteEpochRow {
	row = epQAClone(row)
	row.Ordinal = ordinal
	row.Value.ID = fmt.Sprintf("00000000-0000-0000-0000-%012x", suffix)
	return row
}

func TestSQLiteEpochRowsRealLegacyRoundtripAndCallerOwnership(t *testing.T) {
	_, source := epQALegacy(t)
	rows := epQARows(source)
	f, meta := epQASeed(t, source, rows)
	epQAReopen(t, f, meta, rows)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	next, err := sqliteNextEpochOrdinal(tx)
	if err != nil || next != int64(len(rows)) {
		t.Fatal("next ordinal differs from original legacy array order")
	}
	proposal := epQAFresh(rows[0], next, 100)
	original := epQAClone(proposal)
	wantCharge := epQACharge(t, proposal)
	charge, err := sqliteEpochRowCharge(proposal)
	if err != nil || charge != wantCharge || !reflect.DeepEqual(proposal, original) {
		t.Fatal("pure epoch charge differs or mutated caller")
	}
	delta, err := sqliteInsertEpoch(tx, proposal)
	if err != nil || delta != wantCharge || !reflect.DeepEqual(proposal, original) {
		t.Fatal("epoch insert differs or mutated caller")
	}
	if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, meta) {
		t.Fatal("epoch helper changed caller-owned metadata")
	}
	got, found, err := sqliteReadEpoch(tx, source.ComputerID, proposal.Value.ID)
	if err != nil || !found || !reflect.DeepEqual(got, proposal) {
		t.Fatal("newly staged epoch differs")
	}
	*got.Value.Anchor.Epoch, *got.Value.Anchor.ElapsedNS, *got.Value.Anchor.AwakeNS = "mutated-result", "99", "98"
	*proposal.Value.Anchor.Epoch = "mutated-caller"
	again, found, err := sqliteReadEpoch(tx, source.ComputerID, original.Value.ID)
	if err != nil || !found || !reflect.DeepEqual(again, original) {
		t.Fatal("owned result/caller pointer mutation changed storage")
	}
	after := metaQANext(t, meta)
	after.Revision, after.LogicalBytes = bump(meta.Revision), meta.LogicalBytes+delta
	if err := sqliteUpdateMeta(tx, meta, after); err != nil {
		t.Fatal("caller metadata composition failed")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	epQAReopen(t, f, after, append(rows, original))
}

func TestSQLiteEpochRowsLatestUsesFullAttributionAndOriginalOrdinal(t *testing.T) {
	h, source := epQALegacy(t)
	base := source.Epochs[0].Attribution
	// These are real valid legacy epochs, persisted with their original array
	// order through the owned store. They need no fabricated Actor/segment rows.
	if err := h.service.store.update(context.Background(), func(st *state) (bool, error) {
		for i := 0; i < 5; i++ {
			a := base
			switch i {
			case 0:
				a.AccountID = "5"
			case 1:
				a.UserID = "6"
			case 2:
				a.ProjectID = "7"
			case 3:
				a.TaskID = "8"
			case 4:
				a.Timezone = "America/New_York"
			}
			selectEpoch(st, st.ComputerID, a, h.sample)
		}
		sample := h.sample
		boot := "boot-newest-incompatible"
		sample.Epoch = &boot
		selectEpoch(st, st.ComputerID, base, sample)
		// Interleave another group after the latest base row.
		a := base
		a.TaskID = "9"
		selectEpoch(st, st.ComputerID, a, sample)
		return true, nil
	}); err != nil {
		t.Fatal("real interleaved legacy epoch fixture failed")
	}
	source = bgQAReadLegacy(t, h.service)
	rows := epQARows(source)
	f, meta := epQASeed(t, source, rows)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	for _, row := range rows {
		want := row
		for _, candidate := range rows {
			if candidate.Value.Attribution == row.Value.Attribution && candidate.Ordinal > want.Ordinal {
				want = candidate
			}
		}
		got, found, err := sqliteLatestEpoch(tx, source.ComputerID, row.Value.Attribution)
		if err != nil || !found || !reflect.DeepEqual(got, want) {
			t.Fatalf("latest full-attribution lookup differs: ordinal=%d code=%s", row.Ordinal, bgQAFixtureCode(err))
		}
	}
	latest, found, err := sqliteLatestEpoch(tx, source.ComputerID, base)
	if err != nil || !found || latest.Ordinal != 6 {
		t.Fatal("latest returned wrong original ordinal")
	}
	if _, ok := projectSample(source.Epochs[0], h.sample); !ok {
		t.Fatal("older compatible positive control failed")
	}
	if _, ok := projectSample(&latest.Value, h.sample); ok {
		t.Fatal("newest incompatible control unexpectedly projects")
	}
	count := len(source.Epochs)
	chosen := selectEpoch(source, source.ComputerID, base, h.sample)
	if len(source.Epochs) != count+1 || chosen.ID == latest.Value.ID || chosen.ID == rows[0].Value.ID {
		t.Fatal("legacy latest-only oracle reused older compatible epoch")
	}
	if interopCount(t, tx, "SELECT count(*) FROM epochs") != int64(len(rows)) {
		t.Fatal("latest read created an epoch")
	}
	missing := base
	missing.TaskID = "999"
	if got, found, err := sqliteLatestEpoch(tx, source.ComputerID, missing); err != nil || found || !reflect.DeepEqual(got, sqliteEpochRow{}) {
		t.Fatal("missing group is not clean absence")
	}
	if got, found, err := sqliteReadEpoch(tx, source.ComputerID, "ffffffff-ffff-ffff-ffff-ffffffffffff"); err != nil || found || !reflect.DeepEqual(got, sqliteEpochRow{}) {
		t.Fatal("missing ID is not clean absence")
	}
	bgQAPlan(t, tx, "SELECT "+epQAColumns+" FROM epochs WHERE epoch_id=?", "sqlite_autoindex_epochs_1", sqliteio.Text(rows[0].Value.ID))
	a := base
	bgQAPlan(t, tx, "SELECT "+epQAColumns+" FROM epochs WHERE computer_id=? AND account_id=? AND user_id=? AND project_id=? AND task_id=? AND timezone=? ORDER BY creation_ordinal DESC LIMIT 1", "epoch_latest", sqliteio.Text(source.ComputerID), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID), sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone))
	// An ordered covering-index walk is correct for the global next ordinal;
	// unlike a keyed lookup EXPLAIN names SCAN, so inspect it separately.
	s := interopPrepare(t, tx, "EXPLAIN QUERY PLAN SELECT creation_ordinal FROM epochs ORDER BY creation_ordinal DESC LIMIT 1")
	var plan string
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal("next plan failed")
		}
		if !row {
			break
		}
		detail, err := s.Text(3)
		if err != nil {
			t.Fatal("next plan read failed")
		}
		plan += detail
	}
	if err := s.Close(); err != nil {
		t.Fatal("next plan cleanup failed")
	}
	if !strings.Contains(plan, "COVERING INDEX sqlite_autoindex_epochs_2") || strings.Contains(plan, "TEMP B-TREE") {
		t.Fatal("next ordinal does not use ordered UNIQUE index")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	epQAReopen(t, f, meta, rows)
}

func TestSQLiteEpochRowsClockTimePersistenceMatchesActualLegacy(t *testing.T) {
	for _, which := range []string{"offset", "sub-minute-offset", "year-zero", "year-9999", "zero-counters", "clock-ceiling", "awake-above-elapsed", "malformed-short"} {
		t.Run(which, func(t *testing.T) {
			h := qaNew(t)
			h.seed()
			sample := h.sample
			switch which {
			case "offset":
				sample.WallUTC = time.Date(2026, 10, 3, 12, 5, 6, 123456789, time.FixedZone("private-zone", -7*3600))
			case "sub-minute-offset":
				sample.WallUTC = time.Date(2026, 10, 3, 12, 5, 6, 1, time.FixedZone("private-zone", 61))
			case "year-zero":
				sample.WallUTC = time.Date(0, 1, 2, 3, 4, 5, 0, time.UTC)
			case "year-9999":
				sample.WallUTC = time.Date(9999, 12, 30, 3, 4, 5, 0, time.UTC)
			case "clock-ceiling":
				elapsed, awake := "9223372036854775807", "0"
				sample.ElapsedNS, sample.AwakeNS = &elapsed, &awake
			case "awake-above-elapsed":
				elapsed, awake := "2", "5"
				sample.ElapsedNS, sample.AwakeNS = &elapsed, &awake
			case "malformed-short":
				boot := "boot-" + string([]byte{0xff, 0xfe})
				sample.Epoch = &boot
			}
			if _, _, ok := sampleValues(sample); !ok {
				t.Fatal("raw legacy clock positive control failed")
			}
			h.sample = sample
			// Persist only a real selectEpoch result here. Sub-minute timezone
			// canonicalization can alter a segment projection on subsequent read;
			// epochs themselves have the narrower legacy sampleValues contract.
			var raw timelineEpoch
			if err := h.service.store.update(context.Background(), func(st *state) (bool, error) {
				raw = *selectEpoch(st, st.ComputerID, h.bindings[qaBindingA].Attribution, sample)
				return true, nil
			}); err != nil {
				t.Fatal("actual legacy epoch write failed")
			}
			source := bgQAReadLegacy(t, h.service)
			rows := epQARows(source)
			if len(rows) != 1 {
				t.Fatal("legacy persistence fixture epoch count differs")
			}
			rawRow := sqliteEpochRow{Ordinal: 0, Value: raw}
			if !reflect.DeepEqual(epQAPersisted(t, rawRow), rows[0]) {
				t.Fatal("independent JSON oracle differs from actual legacy file read")
			}
			original := epQAClone(rawRow)
			f, meta := epQASeed(t, source, []sqliteEpochRow{rawRow})
			if !reflect.DeepEqual(rawRow, original) {
				t.Fatal("persistence repair mutated caller")
			}
			epQAReopen(t, f, meta, rows)
			c, tx := interopOpen(t, f, false, sqliteio.Read)
			s := interopPrepare(t, tx, "SELECT anchor_wall_json,group_order FROM epochs WHERE epoch_id=?", sqliteio.Text(raw.ID))
			if row, err := s.Step(); err != nil || !row {
				t.Fatal("stored time/group row missing")
			}
			wall, _ := raw.Anchor.WallUTC.MarshalJSON()
			for i, want := range []string{string(wall), epQAGroup(t, raw)} {
				got, err := s.Text(i)
				if err != nil || got != want {
					t.Fatal("time offset JSON or full attribution group changed")
				}
			}
			if row, err := s.Step(); err != nil || row {
				t.Fatal("stored time/group result not singleton")
			}
			if err := s.Close(); err != nil {
				t.Fatal("stored time/group cleanup failed")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}

func TestSQLiteEpochRowsRetainLegacyMalformedEpochExpansionOutcome(t *testing.T) {
	h := qaNew(t)
	h.seed()
	boot := strings.Repeat(string([]byte{0xff}), 256)
	sample := h.sample
	sample.Epoch = &boot
	if _, _, ok := sampleValues(sample); !ok {
		t.Fatal("raw 256-byte positive control failed")
	}
	var raw timelineEpoch
	if err := h.service.store.update(context.Background(), func(st *state) (bool, error) {
		raw = *selectEpoch(st, st.ComputerID, h.bindings[qaBindingA].Attribution, sample)
		return true, nil
	}); err != nil {
		t.Fatal("legacy raw-valid write unexpectedly refused")
	}
	// Establish the actual saved bytes and the actual later file-reader refusal.
	b, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal("legacy saved bytes unavailable")
	}
	var saved state
	if err := json.Unmarshal(b, &saved); err != nil || len(saved.Epochs) != 1 || len(*saved.Epochs[0].Anchor.Epoch) != 768 {
		t.Fatal("legacy replacement expansion control failed")
	}
	if _, _, err := h.service.store.read(context.Background()); err == nil {
		t.Fatal("legacy reader unexpectedly accepted expanded sample")
	} else {
		bgQACorrupt(t, err)
	}
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal("schema creation failed")
	}
	row := sqliteEpochRow{Ordinal: 0, Value: raw}
	original := epQAClone(row)
	delta, err := sqliteInsertEpoch(tx, row)
	if err != nil || delta != epQACharge(t, row) || !reflect.DeepEqual(row, original) {
		t.Fatal("epoch writer added post-repair refusal or changed caller")
	}
	if charge, err := sqliteEpochRowCharge(row); err != nil || charge != delta {
		t.Fatal("pure charge differs on legacy expansion")
	}
	got, found, err := sqliteReadEpoch(tx, qaComputer, raw.ID)
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(got, sqliteEpochRow{}) {
		t.Fatal("expanded stored sample returned usable epoch")
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteEpochRowsConflictsAndCallerRollbackPreserveHistory(t *testing.T) {
	_, source := epQALegacy(t)
	rows := epQARows(source)
	f, meta := epQASeed(t, source, rows)
	for _, which := range []string{"duplicate-id", "duplicate-ordinal"} {
		t.Run(which, func(t *testing.T) {
			candidate := epQAFresh(rows[0], 2, 102)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if delta, err := sqliteInsertEpoch(tx, candidate); err != nil || delta != epQACharge(t, candidate) {
				t.Fatal("otherwise-valid candidate insert control failed")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			c, tx = interopOpen(t, f, false, sqliteio.Write)
			staged := epQAFresh(rows[0], 1, 101)
			if delta, err := sqliteInsertEpoch(tx, staged); err != nil || delta != epQACharge(t, staged) {
				t.Fatal("fresh insert positive control failed")
			}
			if which == "duplicate-id" {
				candidate.Value.ID = rows[0].Value.ID
			} else {
				candidate.Ordinal = rows[0].Ordinal
			}
			original := epQAClone(candidate)
			delta, err := sqliteInsertEpoch(tx, candidate)
			bgQAValidation(t, err)
			var native *sqliteio.Error
			if !errors.As(err, &native) || native.Category != sqliteio.Constraint || (native.Code != 1555 && native.Code != 2067) {
				t.Fatal("known identity conflict lost native uniqueness evidence")
			}
			interopSafeError(t, err, candidate.Value.ID, *candidate.Value.Anchor.Epoch, f.directory)
			if delta != 0 || !reflect.DeepEqual(candidate, original) {
				t.Fatal("conflict returned charge or mutated caller")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			epQAReopen(t, f, meta, rows)
		})
	}
}

func TestSQLiteEpochRowsNextOrdinalEmptySparseAndExhaustion(t *testing.T) {
	_, source := epQALegacy(t)
	base := epQARows(source)[0]
	f, meta := epQASeed(t, source, nil)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	if got, err := sqliteNextEpochOrdinal(tx); err != nil || got != 0 {
		t.Fatal("empty next ordinal differs")
	}
	for _, ordinal := range []int64{9, 2, 4} {
		row := epQAFresh(base, ordinal, int(ordinal)+200)
		row.Value.Attribution.TaskID = fmt.Sprint(ordinal + 1)
		if _, err := sqliteInsertEpoch(tx, row); err != nil {
			t.Fatal("explicit sparse ordinal positive control failed")
		}
	}
	if got, err := sqliteNextEpochOrdinal(tx); err != nil || got != 10 {
		t.Fatal("global ordinal used group/row count/insertion order")
	}
	row := epQAFresh(base, math.MaxInt64, 250)
	if _, err := sqliteInsertEpoch(tx, row); err != nil {
		t.Fatal("valid maximum explicit ordinal refused")
	}
	if got, found, err := sqliteReadEpoch(tx, qaComputer, row.Value.ID); err != nil || !found || got.Ordinal != math.MaxInt64 {
		t.Fatal("maximum ordinal read positive control failed")
	}
	got, err := sqliteNextEpochOrdinal(tx)
	bgQAValidation(t, err)
	if got != 0 {
		t.Fatal("exhaustion returned usable ordinal")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	epQAReopen(t, f, meta, nil)
	// A stored public MaxUint64 revision is an isolated bootstrap import; it is
	// unrelated to the signed clock/ordinal ceilings and is never incremented.
	copy := *source
	copy.Revision = "18446744073709551615"
	f, meta = epQASeed(t, &copy, []sqliteEpochRow{base})
	epQAReopen(t, f, meta, []sqliteEpochRow{base})
}

func TestSQLiteEpochRowsInvalidInputsHaveValidationAndZeroResults(t *testing.T) {
	_, source := epQALegacy(t)
	rows := epQARows(source)
	f, meta := epQASeed(t, source, rows)
	for _, which := range []string{"ordinal", "id", "computer", "account", "user", "project", "task", "timezone", "capability", "zero-wall", "time-year-overflow", "nil-epoch", "empty-epoch", "control-epoch", "long-epoch", "nil-elapsed", "nil-awake", "leading-zero", "negative-counter", "clock-overflow"} {
		t.Run(which, func(t *testing.T) {
			row := epQAFresh(rows[0], 1, 300)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if delta, err := sqliteInsertEpoch(tx, row); err != nil || delta != epQACharge(t, row) {
				t.Fatal("otherwise-valid fresh insert control failed")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			switch which {
			case "ordinal":
				row.Ordinal = -1
			case "id":
				row.Value.ID = "private-invalid-id"
			case "computer":
				row.Value.ComputerID = "private-invalid-computer"
			case "account":
				row.Value.Attribution.AccountID = "0"
			case "user":
				row.Value.Attribution.UserID = "01"
			case "project":
				row.Value.Attribution.ProjectID = "-1"
			case "task":
				row.Value.Attribution.TaskID = "9223372036854775808"
			case "timezone":
				row.Value.Attribution.Timezone = "private-invalid-zone"
			case "capability":
				row.Value.Anchor.Capability = "unavailable"
			case "zero-wall":
				row.Value.Anchor.WallUTC = time.Time{}
			case "time-year-overflow":
				row.Value.Anchor.WallUTC = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			case "nil-epoch":
				row.Value.Anchor.Epoch = nil
			case "empty-epoch":
				*row.Value.Anchor.Epoch = ""
			case "control-epoch":
				*row.Value.Anchor.Epoch = "private\x00boot"
			case "long-epoch":
				*row.Value.Anchor.Epoch = strings.Repeat("x", 257)
			case "nil-elapsed":
				row.Value.Anchor.ElapsedNS = nil
			case "nil-awake":
				row.Value.Anchor.AwakeNS = nil
			case "leading-zero":
				*row.Value.Anchor.ElapsedNS = "01"
			case "negative-counter":
				*row.Value.Anchor.AwakeNS = "-1"
			case "clock-overflow":
				*row.Value.Anchor.ElapsedNS = "9223372036854775808"
			}
			original := epQAClone(row)
			charge, err := sqliteEpochRowCharge(row)
			bgQAValidation(t, err)
			if charge != 0 || !reflect.DeepEqual(row, original) {
				t.Fatal("invalid charge returned value or changed caller")
			}
			c, tx = interopOpen(t, f, false, sqliteio.Write)
			delta, err := sqliteInsertEpoch(tx, row)
			bgQAValidation(t, err)
			interopSafeError(t, err, "private-invalid", f.directory)
			if delta != 0 || !reflect.DeepEqual(row, original) || interopCount(t, tx, "SELECT count(*) FROM epochs") != int64(len(rows)) {
				t.Fatal("invalid insert returned value, changed caller or staged rows")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			epQAReopen(t, f, meta, rows)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	for _, query := range [][2]string{{"private-invalid-computer", rows[0].Value.ID}, {qaComputer, "private-invalid-id"}} {
		got, found, err := sqliteReadEpoch(tx, query[0], query[1])
		bgQAValidation(t, err)
		if found || !reflect.DeepEqual(got, sqliteEpochRow{}) {
			t.Fatal("invalid read input returned usable row")
		}
	}
	a := rows[0].Value.Attribution
	a.TaskID = "0"
	if got, found, err := sqliteLatestEpoch(tx, qaComputer, a); err == nil || found || !reflect.DeepEqual(got, sqliteEpochRow{}) {
		t.Fatal("invalid latest attribution returned usable row")
	} else {
		bgQAValidation(t, err)
	}
	if got, found, err := sqliteLatestEpoch(tx, "private-invalid-computer", rows[0].Value.Attribution); err == nil || found || !reflect.DeepEqual(got, sqliteEpochRow{}) {
		t.Fatal("invalid latest computer returned usable row")
	} else {
		bgQAValidation(t, err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteEpochRowsSelectedCorruptionAndValidForeignScopeControl(t *testing.T) {
	_, source := epQALegacy(t)
	base := epQARows(source)[0]
	// Real STRICT/NOT NULL prevents these fixtures. Relax only the test table
	// to prove every selected decoder column independently refuses coercion and
	// NULL; this does not claim the real schema admits the malformed values.
	for column, name := range strings.Split(epQAColumns, ",") {
		for _, which := range []string{"wrong-kind", "null"} {
			t.Run(name+"/"+which, func(t *testing.T) {
				f := interopLocation(t)
				c, tx := interopOpen(t, f, true, sqliteio.Write)
				if err := sqliteCreateSchema(tx); err != nil {
					t.Fatal("schema creation failed")
				}
				interopDone(t, tx, "ALTER TABLE epochs RENAME TO qa_original_epochs")
				interopDone(t, tx, "CREATE TABLE epochs("+epQAColumns+")")
				if _, err := sqliteInsertEpoch(tx, base); err != nil {
					t.Fatal("kind/null baseline insert failed")
				}
				if got, found, err := sqliteReadEpoch(tx, qaComputer, base.Value.ID); err != nil || !found || !reflect.DeepEqual(got, base) {
					t.Fatal("kind/null baseline read failed")
				}
				value := sqliteio.Blob([]byte("wrong-kind"))
				if column == 7 || column == 10 || column == 11 || column == 16 || column == 17 {
					value = sqliteio.Text("0")
				}
				if which == "null" {
					value = sqliteio.Null()
				}
				interopDone(t, tx, "UPDATE epochs SET "+name+"=?", value)
				var got sqliteEpochRow
				var found bool
				var err error
				if column == 0 {
					got, found, err = sqliteLatestEpoch(tx, qaComputer, base.Value.Attribution)
				} else {
					got, found, err = sqliteReadEpoch(tx, qaComputer, base.Value.ID)
				}
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(got, sqliteEpochRow{}) {
					t.Fatal("wrong-kind/NULL column returned usable row")
				}
				interopRollback(t, tx)
				interopClose(t, c)
			})
		}
	}
	for _, which := range []string{"id-shape", "attribution", "group", "capability", "empty-epoch", "malformed-utf8", "wall-seconds", "wall-json", "wall-noncanonical-json", "raw-counter", "blob-projection", "ordinal-kind", "ordinal-negative", "wall-kind", "wall-nanoseconds", "clock-null", "blob-kind", "blob-width", "clock-ceiling", "extra-id-row"} {
		t.Run(which, func(t *testing.T) {
			f := interopLocation(t)
			c, tx := interopOpen(t, f, true, sqliteio.Write)
			if err := sqliteCreateSchema(tx); err != nil {
				t.Fatal("schema creation failed")
			}
			relaxed := which == "ordinal-kind" || which == "ordinal-negative" || which == "wall-kind" || which == "wall-nanoseconds" || which == "clock-null" || which == "blob-kind" || which == "blob-width" || which == "clock-ceiling" || which == "extra-id-row" || which == "capability"
			if relaxed {
				interopDone(t, tx, "ALTER TABLE epochs RENAME TO qa_original_epochs")
				interopDone(t, tx, "CREATE TABLE epochs("+epQAColumns+")")
			}
			if _, err := sqliteInsertEpoch(tx, base); err != nil {
				t.Fatal("decoder baseline insert failed")
			}
			if got, found, err := sqliteReadEpoch(tx, qaComputer, base.Value.ID); err != nil || !found || !reflect.DeepEqual(got, base) {
				t.Fatal("decoder baseline read positive control failed")
			}
			id := base.Value.ID
			switch which {
			case "id-shape":
				id = "private-invalid-id"
				interopDone(t, tx, "UPDATE epochs SET epoch_id=?", sqliteio.Text(id))
			case "attribution":
				interopDone(t, tx, "UPDATE epochs SET task_id='0'")
			case "group":
				interopDone(t, tx, "UPDATE epochs SET group_order='private-wrong-group'")
			case "capability":
				interopDone(t, tx, "UPDATE epochs SET anchor_capability='unavailable'")
			case "empty-epoch":
				interopDone(t, tx, "UPDATE epochs SET anchor_epoch=''")
			case "malformed-utf8":
				interopDone(t, tx, "UPDATE epochs SET anchor_epoch=?", sqliteio.Text(string([]byte{0xff})))
			case "wall-seconds":
				interopDone(t, tx, "UPDATE epochs SET anchor_wall_sec=anchor_wall_sec+1")
			case "wall-json":
				interopDone(t, tx, "UPDATE epochs SET anchor_wall_json='private-invalid-time'")
			case "wall-noncanonical-json":
				interopDone(t, tx, "UPDATE epochs SET anchor_wall_json=?", sqliteio.Text("\"2026-10-02T09:00:00.000Z\""))
			case "raw-counter":
				interopDone(t, tx, "UPDATE epochs SET anchor_elapsed_raw='01'")
			case "blob-projection":
				interopDone(t, tx, "UPDATE epochs SET anchor_elapsed=X'0000000000000001'")
			case "ordinal-kind":
				interopDone(t, tx, "UPDATE epochs SET creation_ordinal='0'")
			case "ordinal-negative":
				interopDone(t, tx, "UPDATE epochs SET creation_ordinal=-1")
			case "wall-kind":
				interopDone(t, tx, "UPDATE epochs SET anchor_wall_sec='0'")
			case "wall-nanoseconds":
				interopDone(t, tx, "UPDATE epochs SET anchor_wall_nsec=1000000000")
			case "clock-null":
				interopDone(t, tx, "UPDATE epochs SET anchor_epoch=NULL")
			case "blob-kind":
				interopDone(t, tx, "UPDATE epochs SET anchor_awake='00000000'")
			case "blob-width":
				interopDone(t, tx, "UPDATE epochs SET anchor_awake=X'01'")
			case "clock-ceiling":
				interopDone(t, tx, "UPDATE epochs SET anchor_elapsed_raw='9223372036854775808',anchor_elapsed=X'8000000000000000'")
			case "extra-id-row":
				interopDone(t, tx, "INSERT INTO epochs("+epQAColumns+") SELECT "+epQAColumns+" FROM epochs")
			}
			// An invalid lookup ID is an input error, so select the invalid stored
			// ID through its valid full-attribution group instead of raw ID lookup.
			var got sqliteEpochRow
			var found bool
			var err error
			if which == "id-shape" {
				got, found, err = sqliteLatestEpoch(tx, qaComputer, base.Value.Attribution)
			} else {
				got, found, err = sqliteReadEpoch(tx, qaComputer, id)
			}
			bgQACorrupt(t, err)
			interopSafeError(t, err, "private-wrong", "private-invalid", f.directory)
			if found || !reflect.DeepEqual(got, sqliteEpochRow{}) {
				t.Fatal("selected corruption returned usable row")
			}
			if which == "ordinal-kind" || which == "ordinal-negative" {
				got, err := sqliteNextEpochOrdinal(tx)
				bgQACorrupt(t, err)
				if got != 0 {
					t.Fatal("malformed selected ordinal returned usable value")
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	f, meta := epQASeed(t, source, []sqliteEpochRow{base})
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	foreign := epQAFresh(base, 1, 400)
	foreign.Value.ComputerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	if _, err := sqliteInsertEpoch(tx, foreign); err != nil {
		t.Fatal("valid foreign epoch insert control failed")
	}
	if got, found, err := sqliteReadEpoch(tx, foreign.Value.ComputerID, foreign.Value.ID); err != nil || !found || !reflect.DeepEqual(got, foreign) {
		t.Fatal("valid foreign scope read control failed")
	}
	if got, found, err := sqliteLatestEpoch(tx, foreign.Value.ComputerID, foreign.Value.Attribution); err != nil || !found || !reflect.DeepEqual(got, foreign) {
		t.Fatal("valid foreign latest control failed")
	}
	got, found, err := sqliteReadEpoch(tx, qaComputer, foreign.Value.ID)
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(got, sqliteEpochRow{}) {
		t.Fatal("foreign selected ID returned usable local epoch")
	}
	// Corrupt unrelated foreign history and older local history. Only the
	// newest selected row is decoded by latest; neither is globally audited.
	newest := epQAFresh(base, 2, 401)
	if _, err := sqliteInsertEpoch(tx, newest); err != nil {
		t.Fatal("newest valid insert control failed")
	}
	interopDone(t, tx, "UPDATE epochs SET group_order='private-unselected-corruption' WHERE epoch_id IN (?,?)", sqliteio.Text(base.Value.ID), sqliteio.Text(foreign.Value.ID))
	if got, found, err := sqliteLatestEpoch(tx, qaComputer, base.Value.Attribution); err != nil || !found || !reflect.DeepEqual(got, newest) {
		t.Fatal("latest scanned unrelated/older corrupted history")
	}
	interopDone(t, tx, "UPDATE epochs SET group_order='private-selected-corruption' WHERE epoch_id=?", sqliteio.Text(newest.Value.ID))
	got, found, err = sqliteLatestEpoch(tx, qaComputer, base.Value.Attribution)
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(got, sqliteEpochRow{}) {
		t.Fatal("latest fell back after selected corruption")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	epQAReopen(t, f, meta, []sqliteEpochRow{base})
}

func TestSQLiteEpochRowsCallerCancellationAndTerminalAdapterRefusal(t *testing.T) {
	_, source := epQALegacy(t)
	rows := epQARows(source)
	f, meta := epQASeed(t, source, rows)
	for _, which := range []string{"read", "latest", "next", "insert"} {
		t.Run(which, func(t *testing.T) {
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
			staged := epQAFresh(rows[0], 1, 500)
			if _, err := sqliteInsertEpoch(tx, staged); err != nil {
				t.Fatal("pre-cancel staged row control failed")
			}
			cancel()
			var row sqliteEpochRow
			var found bool
			var delta int64
			switch which {
			case "read":
				row, found, err = sqliteReadEpoch(tx, qaComputer, rows[0].Value.ID)
			case "latest":
				row, found, err = sqliteLatestEpoch(tx, qaComputer, rows[0].Value.Attribution)
			case "next":
				delta, err = sqliteNextEpochOrdinal(tx)
			case "insert":
				delta, err = sqliteInsertEpoch(tx, epQAFresh(rows[0], 2, 501))
			}
			if !errors.Is(err, context.Canceled) || delta != 0 || found || !reflect.DeepEqual(row, sqliteEpochRow{}) {
				t.Fatal("canceled helper lost evidence or returned usable result")
			}
			interopSafeError(t, err, f.directory, rows[0].Value.ID)
			if cleanup := tx.Rollback(); cleanup != nil {
				interopSafeError(t, cleanup, f.directory)
			}
			interopClose(t, c)
			epQAReopen(t, f, meta, rows)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	delta, err := sqliteInsertEpoch(tx, epQAFresh(rows[0], 1, 600))
	var native *sqliteio.Error
	if delta != 0 || !errors.As(err, &native) || native.Category == sqliteio.Constraint {
		t.Fatal("read transaction write refusal lost adapter evidence")
	}
	interopSafeError(t, err, f.directory)
	interopRollback(t, tx)
	interopClose(t, c)
	epQAReopen(t, f, meta, rows)
	// A closed caller transaction is also an adapter refusal, not absence.
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	interopRollback(t, tx)
	if got, found, err := sqliteReadEpoch(tx, qaComputer, rows[0].Value.ID); err == nil || found || !reflect.DeepEqual(got, sqliteEpochRow{}) {
		t.Fatal("terminal transaction returned usable read")
	}
	interopClose(t, c)
	epQAReopen(t, f, meta, rows)
}
