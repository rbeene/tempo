//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent source-first QA for the approved inactive host-receipt row API.
// The coordinator applies this file before implementation and owns execution.
// Synthetic owned fixtures only; no public Service activation is claimed.

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const hrQAColumns = "receipt_key,input_fingerprint,error_code,contract_version,snapshot_revision,receipt_id,source,native_session,turn_id,agent_id,kind,tool_id,disposition,ordering,diagnostic_code,durability,origin,profile_basis,profile_revision,policy_fingerprint,actor_key,actor_generation,observed_at_sec,observed_at_nsec,observed_at_json"
const hrQAMax = "18446744073709551615"

func hrQAClone(row sqliteHostReceiptRow) sqliteHostReceiptRow {
	if row.Record.Result.Actor != nil {
		ref := *row.Record.Result.Actor
		row.Record.Result.Actor = &ref
	}
	return row
}

func hrQACompleteLegacy(t *testing.T, computer, ceiling string, row sqliteHostReceiptRow) (*qaHarness, *state) {
	t.Helper()
	h := qaNew(t)
	h.seed()
	st := bgQAReadLegacy(t, h.service)
	st.ComputerID, st.Revision = computer, ceiling
	st.HostReceipts = map[string]hostReceiptRecord{row.Key: row.Record}
	if !validState(st) {
		t.Fatal("complete raw legacy fixture is outside actual validation")
	}
	return h, st
}

func hrQALegacyRead(t *testing.T, computer, ceiling string, row sqliteHostReceiptRow, wantValid bool) *state {
	t.Helper()
	h, raw := hrQACompleteLegacy(t, computer, ceiling, row)
	return bgQALegacyMarshalOracle(t, h.service, h.path, raw, wantValid)
}

func hrQALegacy(t *testing.T, computer, ceiling string, row sqliteHostReceiptRow) sqliteHostReceiptRow {
	t.Helper()
	saved := hrQALegacyRead(t, computer, ceiling, row, true)
	for key, record := range saved.HostReceipts {
		return sqliteHostReceiptRow{Key: key, Record: record}
	}
	t.Fatal("legacy oracle lost receipt")
	return sqliteHostReceiptRow{}
}

func hrQASource(t *testing.T) (*state, []sqliteHostReceiptRow) {
	t.Helper()
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "working", ""))
	h.send(10, h.event("Stop", "late", ""))
	h.send(20, h.event("UserPromptSubmit", "late", ""))
	h.at(30)
	h.clockErr = errors.New("synthetic host clock unavailable")
	_, err := h.service.IngestHost(context.Background(), h.event("Stop", "working", ""))
	qaCode(t, err, "clock_unavailable")
	st := bgQAReadLegacy(t, h.service)
	rows := make([]sqliteHostReceiptRow, 0, len(st.HostReceipts))
	seen := map[string]bool{}
	for key, record := range st.HostReceipts {
		rows = append(rows, hrQALegacy(t, st.ComputerID, st.Revision, sqliteHostReceiptRow{Key: key, Record: record}))
		seen[record.Result.Disposition] = true
		if record.ErrorCode != "" {
			seen[record.ErrorCode] = true
		}
		if record.Result.Actor == nil {
			seen["nil"] = true
		} else {
			seen["present"] = true
		}
	}
	for _, required := range []string{"applied", "stale", "review_required", "clock_unavailable", "nil", "present"} {
		if !seen[required] {
			t.Fatalf("actual host fixture lacks %s", required)
		}
	}
	return st, rows
}

func hrQABase(t *testing.T, source *state, rows []sqliteHostReceiptRow) sqliteHostReceiptRow {
	t.Helper()
	for _, row := range rows {
		if row.Record.Result.Actor != nil && row.Record.Result.ProfileBasis == "operator_declared" {
			row = hrQAClone(row)
			row.Key = strings.Repeat("K", 64)
			row.Record.Fingerprint = strings.Repeat("I", 64)
			row.Record.Result.Fingerprint = strings.Repeat("P", 64)
			row.Record.Result.ID = "01234567-89ab-cdef-0123-456789abcdef"
			row.Record.Result.ObservedAt = time.Date(2026, 10, 3, 12, 34, 56, 123456789, time.FixedZone("fixture offset", 5*3600+45*60))
			row.Record.Result.Actor = &ActorRef{Key: ActorKey{ComputerID: source.ComputerID, Source: "manual-test", SessionID: "unrelated historical session", AgentID: "historical agent"}, Generation: hrQAMax}
			return hrQALegacy(t, source.ComputerID, source.Revision, row)
		}
	}
	t.Fatal("actual host fixture lacks a present operator receipt")
	return sqliteHostReceiptRow{}
}

// Literal25 independent stored-value audit: no production encoder/charge helper.
// Verifies every kind before measuring 32 bytes/row, one type byte/value,
// 8 bytes/INTEGER and 8-byte length headers for TEXT/BLOB plus their real bytes.
func hrQAStoredCharge(t *testing.T, tx *sqliteio.Tx) int64 {
	t.Helper()
	s := interopPrepare(t, tx, "SELECT "+hrQAColumns+" FROM host_receipts")
	var total int64
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !row {
			break
		}
		if s.ColumnCount() != 25 {
			t.Fatal("receipt projection width changed")
		}
		actorKind, err := s.Kind(20)
		if err != nil {
			t.Fatal(err)
		}
		present := actorKind != sqliteio.NullKind
		total += 32
		for i := 0; i < 25; i++ {
			want := sqliteio.TextKind
			switch i {
			case 3, 22, 23:
				want = sqliteio.IntegerKind
			case 4, 18:
				want = sqliteio.BlobKind
			case 20:
				if !present {
					want = sqliteio.NullKind
				}
			case 21:
				if present {
					want = sqliteio.BlobKind
				} else {
					want = sqliteio.NullKind
				}
			}
			kind, err := s.Kind(i)
			if err != nil || kind != want {
				t.Fatalf("column %d kind=%v want=%v error=%v", i, kind, want, err)
			}
			total++
			switch kind {
			case sqliteio.IntegerKind:
				total += 8
			case sqliteio.TextKind:
				value, err := s.Text(i)
				if err != nil {
					t.Fatal(err)
				}
				total += 8 + int64(len(value))
			case sqliteio.BlobKind:
				value, err := s.Blob(i)
				if err != nil || len(value) != 8 {
					t.Fatal("counter not BLOB8", err)
				}
				total += 8 + int64(len(value))
			}
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return total
}

// Separate input oracle for delta assertions; uses the real JSON boundary,
// literal schema counts and time JSON, never sqliteHostReceiptCharge.
func hrQACharge(t *testing.T, row sqliteHostReceiptRow) int64 {
	t.Helper()
	r := row.Record.Result
	wall, err := json.Marshal(r.ObservedAt)
	if err != nil {
		t.Fatal(err)
	}
	texts := []string{row.Key, row.Record.Fingerprint, row.Record.ErrorCode, r.ID, r.Source, r.SessionID, r.TurnID, r.AgentID, r.Kind, r.ToolID, r.Disposition, r.Ordering, r.DiagnosticCode, r.Durability, r.Origin, r.ProfileBasis, r.Fingerprint, string(wall)}
	// 32 +3*9 INTEGER +2 NULL +18*9 TEXT +2*17 BLOB8.
	n := int64(257)
	if r.Actor != nil {
		n += 24
		texts = append(texts, actorKey(r.Actor.Key))
	}
	for _, value := range texts {
		n += int64(len(bgQAPersistedString(t, value)))
	}
	return n
}

func hrQASeed(t *testing.T, source *state, rows []sqliteHostReceiptRow) (interopFixture, sqliteStoreMeta) {
	return hrQASeedAt(t, source, rows, source.Revision)
}

func hrQASeedAt(t *testing.T, source *state, rows []sqliteHostReceiptRow, ceiling string) (interopFixture, sqliteStoreMeta) {
	t.Helper()
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal(err)
	}
	next := interopMeta(f)
	next.ComputerID, next.Revision, next.SyncEnabled = source.ComputerID, ceiling, source.SyncEnabled
	// Bootstrap the real legacy binding/request rows and all receipt dependencies
	// in one transaction; metadata is inserted exactly once at the chosen ceiling.
	for _, row := range bgQARowsFromLegacy(source) {
		delta, err := sqliteInsertBinding(tx, row)
		if err != nil || delta != bgQABindingCharge(t, row) {
			t.Fatal("bootstrap binding differs", err)
		}
		next.LogicalBytes += delta
	}
	for id, receipt := range source.Requests {
		kind, payload := bgQAReceiptPayload(t, receipt)
		interopDone(t, tx, "INSERT INTO requests(request_id,operation,fingerprint,outcome_kind,payload) VALUES(?,?,?,?,?)", sqliteio.Text(id), sqliteio.Text(receipt.Operation), sqliteio.Text(receipt.Fingerprint), sqliteio.Text(kind), sqliteio.Text(payload))
		next.LogicalBytes += int64(77 + len(id) + len(receipt.Operation) + len(receipt.Fingerprint) + len(kind) + len(payload))
	}
	for _, row := range rows {
		hrQALegacy(t, source.ComputerID, ceiling, row)
		if row.Record.Result.Actor != nil {
			delta, err := sqliteEnsureActorGeneration(tx, *row.Record.Result.Actor)
			if err != nil {
				t.Fatal(err)
			}
			next.LogicalBytes += delta
		}
		delta, err := sqliteInsertHostReceipt(tx, source.ComputerID, ceiling, row)
		if err != nil || delta != hrQACharge(t, row) {
			t.Fatalf("insert charge=%d want=%d error=%v", delta, hrQACharge(t, row), err)
		}
		next.LogicalBytes += delta
		charge, err := sqliteHostReceiptCharge(row)
		if err != nil || charge != delta {
			t.Fatal("standalone charge differs from independent oracle", err)
		}
	}
	if interopCount(t, tx, "SELECT count(*) FROM store_meta") != 0 {
		t.Fatal("row helper invented public metadata")
	}
	if err := sqliteInsertMeta(tx, next); err != nil {
		t.Fatal(err)
	}
	if total := bgQAStoredCharge(t, tx) + hrQAStoredCharge(t, tx); total != next.LogicalBytes {
		t.Fatalf("stored audit=%d metadata=%d", total, next.LogicalBytes)
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return f, next
}

func hrQAReopen(t *testing.T, f interopFixture, meta sqliteStoreMeta, rows []sqliteHostReceiptRow) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	for _, want := range rows {
		got, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, want.Key)
		if err != nil || !found || !reflect.DeepEqual(got, want) {
			t.Fatal("reopened full receipt differs", err)
		}
	}
	got, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(got, meta) {
		t.Fatal("reopened metadata differs", err)
	}
	if total := bgQAStoredCharge(t, tx) + hrQAStoredCharge(t, tx); total != meta.LogicalBytes {
		t.Fatal("reopened stored-value accounting differs")
	}
	if interopCount(t, tx, "SELECT count(*) FROM host_receipts") != int64(len(rows)) {
		t.Fatal("retained receipt count changed")
	}
	for _, table := range []string{"actors", "host_sessions", "host_turns"} {
		if interopCount(t, tx, "SELECT count(*) FROM "+table) != 0 {
			t.Fatal("receipt helper invented a current domain row")
		}
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteHostReceiptRowsActualLegacyOwnedRoundTripAndCharge(t *testing.T) {
	source, rows := hrQASource(t)
	base := hrQABase(t, source, rows)
	base.Record.Result.TurnID = "free\x00turn"
	base.Record.Result.AgentID = ""
	base.Record.Result.Kind = "unchecked\x00kind"
	base.Record.Result.ToolID = ""
	base.Record.Result.DiagnosticCode = "diagnostic 雪\x00"
	base = hrQALegacy(t, source.ComputerID, hrQAMax, base)
	none := hrQAClone(base)
	none.Key = strings.Repeat("N", 64)
	none.Record.Result.Actor = nil
	none.Record.Result.ProfileBasis, none.Record.Result.ProfileRevision, none.Record.Result.Fingerprint = "none", "0", ""
	none.Record.Result.Disposition = "review_required"
	none = hrQALegacy(t, source.ComputerID, hrQAMax, none)
	rows = append(rows, base, none) // duplicate public ID is intentionally retained.
	for i, code := range []string{"", "clock_unavailable", "clock_conflict", "event_gap", "event_conflict", "invalid_transition"} {
		row := hrQAClone(base)
		row.Key = strings.Repeat("e", 63) + strconv.Itoa(i)
		row.Record.ErrorCode = code
		row.Record.Result.Source = "claude" // Receipt and historical Actor sources need not agree.
		rows = append(rows, hrQALegacy(t, source.ComputerID, hrQAMax, row))
	}
	// Actual malformed ActorRef legacy-read witness, without any assumption about
	// JSON escape spelling or the separately frozen generation-writer diagnosis.
	rawActor := hrQAClone(base)
	rawActor.Key = strings.Repeat("R", 64)
	rawActor.Record.Result.Actor.Key.SessionID = "historical-" + string([]byte{0xff})
	imported := hrQALegacy(t, source.ComputerID, hrQAMax, rawActor)
	if imported.Record.Result.Actor.Key.SessionID != bgQAPersistedString(t, rawActor.Record.Result.Actor.Key.SessionID) {
		t.Fatal("Actor identity import differs from actual JSON oracle")
	}
	rows = append(rows, imported)
	f, meta := hrQASeed(t, source, rows)
	hrQAReopen(t, f, meta, rows)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	got, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, base.Key)
	if err != nil || !found || got.Record.Result.Actor == base.Record.Result.Actor {
		t.Fatal("read did not return an owned Actor", err)
	}
	got.Record.Result.Actor.Key.SessionID = "caller-owned change"
	got.Record.Result.Actor.Generation = "1"
	again, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, base.Key)
	if err != nil || !found || !reflect.DeepEqual(again, base) {
		t.Fatal("returned Actor mutation changed stored result", err)
	}
	if absent, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, strings.Repeat("A", 64)); err != nil || found || !reflect.DeepEqual(absent, sqliteHostReceiptRow{}) {
		t.Fatal("missing exact key did not return zero absence", err)
	}
	hrQAPlan(t, tx, "SELECT "+hrQAColumns+" FROM host_receipts WHERE receipt_key=?", "sqlite_autoindex_host_receipts_1", sqliteio.Text(base.Key))
	interopRollback(t, tx)
	interopClose(t, c)
}

func hrQAPlan(t *testing.T, tx *sqliteio.Tx, query, index string, values ...sqliteio.Value) {
	t.Helper()
	bgQAPlan(t, tx, query, index, values...)
	s := interopPrepare(t, tx, "EXPLAIN QUERY PLAN "+query, values...)
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !row {
			break
		}
		detail, err := s.Text(3)
		if err != nil || strings.Contains(detail, "TEMP B-TREE") {
			t.Fatal("indexed lookup added history sorting", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteHostReceiptRowsUnsignedLatestExactGenerationTiesAndSelectedOnly(t *testing.T) {
	source, realRows := hrQASource(t)
	base := hrQABase(t, source, realRows)
	ref := *base.Record.Result.Actor
	var rows []sqliteHostReceiptRow
	for i, revision := range []string{"1", "2", "10", "9223372036854775807", "9223372036854775808", "18446744073709551614", hrQAMax, hrQAMax} {
		row := hrQAClone(base)
		row.Key = strings.Repeat(strconv.Itoa(i), 64)
		row.Record.Result.SnapshotRevision = revision
		row.Record.Result.ID = "ffffffff-ffff-ffff-ffff-ffffffffffff"
		if i >= 5 {
			row.Record.Result.ID = "00000000-0000-0000-0000-000000000001"
		}
		if i == 7 {
			row.Record.Result.ID = "00000000-0000-0000-0000-000000000002"
		}
		rows = append(rows, hrQALegacy(t, source.ComputerID, hrQAMax, row))
	}
	tie := hrQAClone(rows[7])
	tie.Key = strings.Repeat("T", 64)
	rows = append(rows, tie)
	for i := 0; i < 96; i++ {
		row := hrQAClone(base)
		row.Key = strings.Repeat("u", 61) + strconv.FormatInt(int64(100+i), 10)
		row.Record.Result.SnapshotRevision = hrQAMax
		if i == 0 {
			row.Record.Result.Actor.Generation = "1"
		} else {
			row.Record.Result.Actor.Key.AgentID = "unrelated-" + strconv.Itoa(i)
		}
		rows = append(rows, hrQALegacy(t, source.ComputerID, hrQAMax, row))
	}
	f, meta := hrQASeedAt(t, source, rows, hrQAMax)
	hrQAReopen(t, f, meta, rows)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	got, found, err := sqliteLatestHostReceipt(tx, meta.ComputerID, meta.Revision, ref)
	if err != nil || !found || (got.Key != rows[7].Key && got.Key != tie.Key) {
		t.Fatal("latest is not an exact-generation co-maximum", err)
	}
	for _, row := range []sqliteHostReceiptRow{rows[7], tie} {
		got, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, row.Key)
		if err != nil || !found || !reflect.DeepEqual(got, row) {
			t.Fatal("tie row was discarded", err)
		}
	}
	hrQAPlan(t, tx, "SELECT "+hrQAColumns+" FROM host_receipts WHERE actor_key=? AND actor_generation=? ORDER BY snapshot_revision DESC,receipt_id DESC LIMIT 1", "host_actor_latest", sqliteio.Text(actorKey(ref.Key)), interopCounter(t, ref.Generation))
	// An unselected old receipt and unrelated generation are deliberately corrupt.
	// The bounded latest operation must inspect only its selected dependency.
	interopDone(t, tx, "UPDATE host_receipts SET native_session='' WHERE receipt_key=?", sqliteio.Text(rows[0].Key))
	interopDone(t, tx, "UPDATE actor_generations SET session_id='different projection' WHERE agent_id='unrelated-1'")
	got, found, err = sqliteLatestHostReceipt(tx, meta.ComputerID, meta.Revision, ref)
	if err != nil || !found || (got.Key != rows[7].Key && got.Key != tie.Key) {
		t.Fatal("latest scanned unrelated historical corruption", err)
	}
	missing := ref
	missing.Generation = "2"
	if got, found, err := sqliteLatestHostReceipt(tx, meta.ComputerID, meta.Revision, missing); err != nil || found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
		t.Fatal("missing generation latest fabricated a result", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	hrQAReopen(t, f, meta, rows)
}

func TestSQLiteHostReceiptRowsReplacementDeltasCallerRollbackAndCommit(t *testing.T) {
	source, realRows := hrQASource(t)
	before := hrQABase(t, source, realRows)
	before.Record.Result.DiagnosticCode = "same"
	before = hrQALegacy(t, source.ComputerID, hrQAMax, before)
	f, meta := hrQASeed(t, source, []sqliteHostReceiptRow{before})
	for _, diagnostic := range []string{"same", "longer diagnostic 雪", ""} {
		after := hrQAClone(before)
		after.Record.Result.DiagnosticCode = diagnostic // snapshot revision intentionally unchanged.
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		delta, err := sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, before, after)
		if err != nil || delta != hrQACharge(t, after)-hrQACharge(t, before) {
			t.Fatal("signed replacement delta differs", err)
		}
		if got := hrQAStoredCharge(t, tx); got != hrQACharge(t, after) {
			t.Fatal("staged replacement stored charge differs")
		}
		if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, meta) {
			t.Fatal("replacement spent metadata revision", err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
		hrQAReopen(t, f, meta, []sqliteHostReceiptRow{before})
	}
	// Both optional Actor transitions are legitimate full-row replacements.
	originalRef := *before.Record.Result.Actor
	for _, present := range []bool{false, true} {
		after := hrQAClone(before)
		if !present {
			after.Record.Result.Actor = nil
		} else {
			ref := originalRef
			after.Record.Result.Actor = &ref
		}
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		delta, err := sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, before, after)
		if err != nil || delta != hrQACharge(t, after)-hrQACharge(t, before) {
			t.Fatal("optional Actor replacement delta differs", err)
		}
		next := metaQANext(t, meta)
		next.Revision = bump(meta.Revision)
		next.LogicalBytes += delta
		if err := sqliteUpdateMeta(tx, meta, next); err != nil {
			t.Fatal(err)
		}
		interopCommit(t, tx)
		interopClose(t, c)
		hrQAReopen(t, f, next, []sqliteHostReceiptRow{after})
		before, meta = after, next
	}
}

func hrQARelax(t *testing.T, tx *sqliteio.Tx) {
	t.Helper()
	// Decoder/CAS fixture only: the real schema was installed and successfully
	// read first. This relaxed table is never claimed as real-schema admission.
	interopDone(t, tx, "ALTER TABLE host_receipts RENAME TO qa_original_host_receipts")
	interopDone(t, tx, "CREATE TABLE host_receipts("+hrQAColumns+")")
	interopDone(t, tx, "INSERT INTO host_receipts("+hrQAColumns+") SELECT "+hrQAColumns+" FROM qa_original_host_receipts")
}

func TestSQLiteHostReceiptRowsFullOldCASAll25ColumnsAndBothActorShapes(t *testing.T) {
	source, realRows := hrQASource(t)
	base := hrQABase(t, source, realRows)
	for _, present := range []bool{true, false} {
		before := hrQAClone(base)
		if !present {
			before.Record.Result.Actor = nil
		}
		f, meta := hrQASeed(t, source, []sqliteHostReceiptRow{before})
		for i, column := range strings.Split(hrQAColumns, ",") {
			t.Run(strconv.FormatBool(present)+"/"+column, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				after := hrQAClone(before)
				after.Record.Result.DiagnosticCode = "valid replacement control"
				if delta, err := sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, before, after); err != nil || delta != hrQACharge(t, after)-hrQACharge(t, before) {
					t.Fatal("otherwise-valid CAS control failed", err)
				}
				if _, err := sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, after, before); err != nil {
					t.Fatal("control restoration failed", err)
				}
				hrQARelax(t, tx)
				var value sqliteio.Value
				switch i {
				case 0, 1, 19:
					value = sqliteio.Text(strings.Repeat("Z", 64))
				case 2:
					code := "event_gap"
					if before.Record.ErrorCode == code {
						code = "clock_conflict"
					}
					value = sqliteio.Text(code)
				case 3:
					value = sqliteio.Integer(2)
				case 5:
					value = sqliteio.Text("00000000-0000-0000-0000-000000000002")
				case 6:
					value = sqliteio.Text("claude")
				case 12:
					disposition := "stale"
					if before.Record.Result.Disposition == disposition {
						disposition = "applied"
					}
					value = sqliteio.Text(disposition)
				case 13:
					ordering := "supported"
					if before.Record.Result.Ordering == ordering {
						ordering = "unavailable"
					}
					value = sqliteio.Text(ordering)
				case 4:
					counter := "1"
					if before.Record.Result.SnapshotRevision == counter {
						counter = "2"
					}
					value = interopCounter(t, counter)
				case 18:
					counter := "1"
					if before.Record.Result.ProfileRevision == counter {
						counter = "2"
					}
					value = interopCounter(t, counter)
				case 20:
					if present {
						value = sqliteio.Null()
					} else {
						value = sqliteio.Text(actorKey(base.Record.Result.Actor.Key))
					}
				case 21:
					if present {
						value = sqliteio.Null()
					} else {
						value = interopCounter(t, base.Record.Result.Actor.Generation)
					}
				case 22:
					value = sqliteio.Integer(before.Record.Result.ObservedAt.Unix() + 1)
				case 23:
					value = sqliteio.Integer(int64(before.Record.Result.ObservedAt.Nanosecond()) + 1)
				case 24:
					value = sqliteio.Text(`"2026-10-03T00:00:00Z"`)
				default:
					value = sqliteio.Text("different stored old value")
				}
				interopDone(t, tx, "UPDATE host_receipts SET "+column+"=? WHERE receipt_key=?", value, sqliteio.Text(before.Key))
				delta, err := sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, before, after)
				bgQACorrupt(t, err)
				interopSafeError(t, err, before.Key, before.Record.Result.DiagnosticCode, f.directory)
				if delta != 0 {
					t.Fatal("stale old field produced a usable delta")
				}
				interopRollback(t, tx)
				interopClose(t, c)
				hrQAReopen(t, f, meta, []sqliteHostReceiptRow{before})
			})
		}
		// Unlike the partial-NULL decoder witnesses above, this stored Actor-pair
		// change is fully valid and must still fail the exact expected-old CAS.
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		actual := hrQAClone(before)
		if present {
			actual.Record.Result.Actor = nil
			interopDone(t, tx, "UPDATE host_receipts SET actor_key=NULL,actor_generation=NULL")
		} else {
			ref := *base.Record.Result.Actor
			if _, err := sqliteEnsureActorGeneration(tx, ref); err != nil {
				t.Fatal(err)
			}
			actual.Record.Result.Actor = &ref
			interopDone(t, tx, "UPDATE host_receipts SET actor_key=?,actor_generation=?", sqliteio.Text(actorKey(ref.Key)), interopCounter(t, ref.Generation))
		}
		actual = hrQALegacy(t, meta.ComputerID, meta.Revision, actual)
		if got, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, before.Key); err != nil || !found || !reflect.DeepEqual(got, actual) {
			t.Fatal("valid stale Actor-pair fixture did not decode", err)
		}
		proposal := hrQAClone(before)
		proposal.Record.Result.DiagnosticCode = "new proposal"
		if delta, err := sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, before, proposal); delta != 0 {
			t.Fatal("valid stale Actor pair returned delta")
		} else {
			bgQACorrupt(t, err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
		hrQAReopen(t, f, meta, []sqliteHostReceiptRow{before})
		c, tx = interopOpen(t, f, false, sqliteio.Write)
		wrong := hrQAClone(before)
		wrong.Key = strings.Repeat("W", 64)
		if delta, err := sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, wrong, wrong); delta != 0 {
			t.Fatal("missing old key returned charge")
		} else {
			bgQACorrupt(t, err)
		}
		after := hrQAClone(before)
		after.Key = strings.Repeat("R", 64)
		if delta, err := sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, before, after); delta != 0 {
			t.Fatal("key rewrite returned charge")
		} else {
			bgQAValidation(t, err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
		hrQAReopen(t, f, meta, []sqliteHostReceiptRow{before})
	}
}

func TestSQLiteHostReceiptRowsIndependentActorKeyGenerationCASAndComputerScope(t *testing.T) {
	source, realRows := hrQASource(t)
	base := hrQABase(t, source, realRows)
	f, meta := hrQASeed(t, source, []sqliteHostReceiptRow{base})
	for _, which := range []string{"same-key-different-generation", "different-key-same-generation", "valid-foreign-computer"} {
		t.Run(which, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			actual := hrQAClone(base)
			ref := *actual.Record.Result.Actor
			if which == "same-key-different-generation" {
				ref.Generation = "1"
			}
			if which == "different-key-same-generation" {
				ref.Key.AgentID = "other historical agent"
			}
			if which == "valid-foreign-computer" {
				ref.Key.ComputerID = "99999999-9999-4999-8999-999999999999"
			}
			actual.Record.Result.Actor = &ref
			if delta, err := sqliteEnsureActorGeneration(tx, ref); err != nil || delta != bgQAGenerationCharge(t, ref) {
				t.Fatal("valid alternate generation dependency failed", err)
			}
			if got, found, err := sqliteReadActorGeneration(tx, ref); err != nil || !found || got != ref {
				t.Fatal("alternate generation projection is not independently valid", err)
			}
			interopDone(t, tx, "UPDATE host_receipts SET actor_key=?,actor_generation=? WHERE receipt_key=?", sqliteio.Text(actorKey(ref.Key)), interopCounter(t, ref.Generation), sqliteio.Text(base.Key))
			computer := meta.ComputerID
			if which == "valid-foreign-computer" {
				computer = ref.Key.ComputerID
			}
			actual = hrQALegacy(t, computer, meta.Revision, actual)
			got, found, err := sqliteReadHostReceipt(tx, computer, meta.Revision, base.Key)
			if err != nil || !found || !reflect.DeepEqual(got, actual) {
				t.Fatal("otherwise-valid changed Actor receipt did not decode", err)
			}
			if which == "valid-foreign-computer" {
				latest, found, err := sqliteLatestHostReceipt(tx, computer, meta.Revision, ref)
				if err != nil || !found || !reflect.DeepEqual(latest, actual) {
					t.Fatal("foreign-scope positive control failed", err)
				}
				got, found, err = sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, base.Key)
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
					t.Fatal("wrong intended-computer scope exposed valid foreign receipt")
				}
			} else {
				proposal := hrQAClone(base)
				proposal.Record.Result.DiagnosticCode = "valid actor-CAS replacement"
				delta, err := sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, base, proposal)
				bgQACorrupt(t, err)
				if delta != 0 {
					t.Fatal("independent stale Actor key/generation produced a usable delta")
				}
				if got, found, err := sqliteReadHostReceipt(tx, computer, meta.Revision, base.Key); err != nil || !found || !reflect.DeepEqual(got, actual) {
					t.Fatal("failed CAS changed valid alternate Actor row", err)
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			hrQAReopen(t, f, meta, []sqliteHostReceiptRow{base})
		})
	}
}

func TestSQLiteHostReceiptRowsLegacyTextRepairRawLookupAndDistinctConstraints(t *testing.T) {
	source, realRows := hrQASource(t)
	base := hrQABase(t, source, realRows)
	f, meta := hrQASeed(t, source, []sqliteHostReceiptRow{base})
	for _, field := range []string{"unchecked-result-text", "key", "input-fingerprint", "policy-fingerprint", "ordinary-duplicate"} {
		t.Run(field, func(t *testing.T) {
			row := hrQAClone(base)
			row.Key = strings.Repeat("F", 64)
			bad := strings.Repeat("x", 63) + string([]byte{0xff})
			switch field {
			case "unchecked-result-text":
				row.Record.Result.TurnID = "raw\x00turn-" + string([]byte{0xff})
				row.Record.Result.AgentID = "raw agent-" + string([]byte{0xfe})
				row.Record.Result.Kind = "raw kind-" + string([]byte{0xff})
				row.Record.Result.ToolID = "raw tool-" + string([]byte{0xff})
				row.Record.Result.DiagnosticCode = "raw diagnostic-" + string([]byte{0xff})
			case "key":
				row.Key = bad
			case "input-fingerprint":
				row.Record.Fingerprint = bad
			case "policy-fingerprint":
				row.Record.Result.Fingerprint = bad
			case "ordinary-duplicate":
				row.Key = base.Key
			}
			original := hrQAClone(row)
			accepted := field == "unchecked-result-text" || field == "ordinary-duplicate"
			decoded := hrQALegacyRead(t, meta.ComputerID, meta.Revision, row, accepted)
			if field == "unchecked-result-text" || field == "ordinary-duplicate" {
				if !validState(decoded) {
					t.Fatal("actual strict legacy read unexpectedly refused")
				}
			} else {
				if validState(decoded) || len(bgQAPersistedString(t, bad)) <= 64 {
					t.Fatal("actual strict legacy expansion/read refusal not established")
				}
			}
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if field == "ordinary-duplicate" {
				staged := hrQAClone(base)
				staged.Key = strings.Repeat("S", 64)
				if _, err := sqliteInsertHostReceipt(tx, meta.ComputerID, meta.Revision, staged); err != nil {
					t.Fatal("valid earlier proposal failed", err)
				}
			}
			if field == "key" {
				got, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, row.Key)
				if err != nil || found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
					t.Fatal("raw malformed key lookup was repaired/coerced", err)
				}
			}
			delta, err := sqliteInsertHostReceipt(tx, meta.ComputerID, meta.Revision, row)
			if field == "unchecked-result-text" {
				want := hrQALegacy(t, meta.ComputerID, meta.Revision, row)
				if err != nil || delta != hrQACharge(t, row) {
					t.Fatal("valid unchecked text repair refused or mischarged", err)
				}
				got, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, row.Key)
				if err != nil || !found || !reflect.DeepEqual(got, want) {
					t.Fatal("stored repaired text differs from actual JSON oracle", err)
				}
				if hrQAStoredCharge(t, tx) != hrQACharge(t, base)+delta {
					t.Fatal("repaired UTF-8 byte charge differs from stored audit")
				}
			} else {
				bgQAValidation(t, err)
				interopSafeError(t, err, row.Key, bad, f.directory)
				var checked *sqliteio.Error
				if delta != 0 || !errors.As(err, &checked) || checked.Category != sqliteio.Constraint {
					t.Fatal("constraint evidence/delta was lost", err)
				}
				if field == "ordinary-duplicate" {
					if checked.Code != 1555 && checked.Code != 2067 {
						t.Fatal("ordinary duplicate lacks PRIMARYKEY/UNIQUE evidence", err)
					}
				} else if checked.Code != 275 {
					t.Fatal("raw expansion did not reach the actual CHECK275", err)
				}
			}
			if !reflect.DeepEqual(row, original) {
				t.Fatal("persistence mutated raw caller bytes")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			hrQAReopen(t, f, meta, []sqliteHostReceiptRow{base})
		})
	}
}

func TestSQLiteHostReceiptRowsTypedDecoderRejectsSelectedCorruption(t *testing.T) {
	source, realRows := hrQASource(t)
	base := hrQABase(t, source, realRows)
	f, meta := hrQASeed(t, source, []sqliteHostReceiptRow{base})
	type defect struct {
		name, sql string
		relaxed   bool
		values    []sqliteio.Value
	}
	cases := []defect{
		{"snapshot-kind", "UPDATE host_receipts SET snapshot_revision='00000001'", true, nil},
		{"snapshot-width", "UPDATE host_receipts SET snapshot_revision=X'01'", true, nil},
		{"snapshot-zero", "UPDATE host_receipts SET snapshot_revision=zeroblob(8)", true, nil},
		{"contract-kind", "UPDATE host_receipts SET contract_version='1'", true, nil},
		{"fingerprint-kind", "UPDATE host_receipts SET input_fingerprint=zeroblob(64)", true, nil},
		{"partial-actor-null", "UPDATE host_receipts SET actor_key=NULL", true, nil},
		{"actor-generation-kind", "UPDATE host_receipts SET actor_generation='00000001'", true, nil},
		{"actor-generation-width", "UPDATE host_receipts SET actor_generation=X'01'", true, nil},
		{"actor-generation-zero", "UPDATE host_receipts SET actor_generation=zeroblob(8)", true, nil},
		{"profile-width", "UPDATE host_receipts SET profile_revision=X'01'", true, nil},
		{"profile-pair", "UPDATE host_receipts SET profile_basis='none'", true, nil},
		{"profile-zero", "UPDATE host_receipts SET profile_revision=zeroblob(8)", true, nil},
		{"time-kind", "UPDATE host_receipts SET observed_at_sec='0'", true, nil},
		{"time-nsec-range", "UPDATE host_receipts SET observed_at_nsec=1000000000", true, nil},
		{"time-seconds-crosscheck", "UPDATE host_receipts SET observed_at_sec=observed_at_sec+1", false, nil},
		{"time-nanos-crosscheck", "UPDATE host_receipts SET observed_at_nsec=observed_at_nsec+1", false, nil},
		{"time-json-crosscheck", "UPDATE host_receipts SET observed_at_json='\"2026-10-03T00:00:00Z\"'", false, nil},
		{"time-zero", "UPDATE host_receipts SET observed_at_sec=-62135596800,observed_at_nsec=0,observed_at_json='\"0001-01-01T00:00:00Z\"'", false, nil},
		{"native-session-empty", "UPDATE host_receipts SET native_session=''", false, nil},
		{"missing-generation", "DELETE FROM actor_generations", false, nil},
		{"generation-projection", "UPDATE actor_generations SET session_id='disagreeing historical session'", false, nil},
		{"extra-selected-row", "INSERT INTO host_receipts(" + hrQAColumns + ") SELECT " + hrQAColumns + " FROM host_receipts", true, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			got, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, base.Key)
			if err != nil || !found || !reflect.DeepEqual(got, base) {
				t.Fatal("decoder baseline did not validate", err)
			}
			if tc.relaxed {
				hrQARelax(t, tx)
			}
			interopDone(t, tx, tc.sql, tc.values...)
			got, found, err = sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, base.Key)
			bgQACorrupt(t, err)
			interopSafeError(t, err, base.Key, base.Record.Result.Actor.Key.SessionID, f.directory)
			if found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
				t.Fatal("corrupt decode exposed a usable/partial result")
			}
			if !tc.relaxed {
				got, found, err = sqliteLatestHostReceipt(tx, meta.ComputerID, meta.Revision, *base.Record.Result.Actor)
				bgQACorrupt(t, err)
				if found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
					t.Fatal("corrupt selected latest exposed a result")
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			hrQAReopen(t, f, meta, []sqliteHostReceiptRow{base})
		})
	}
	// This ceiling refusal uses the real row and real schema, no relaxed fixture.
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	interopDone(t, tx, "UPDATE host_receipts SET snapshot_revision=?", interopCounter(t, hrQAMax))
	got, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, "9223372036854775807", base.Key)
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
		t.Fatal("above-ceiling read exposed result")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	hrQAReopen(t, f, meta, []sqliteHostReceiptRow{base})
	// The private receipt dependency reader must also enforce one exact
	// six-column generation followed by DONE, without parsing actor_key.
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	if got, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, base.Key); err != nil || !found || !reflect.DeepEqual(got, base) {
		t.Fatal("generation decoder baseline invalid", err)
	}
	interopDone(t, tx, "ALTER TABLE actor_generations RENAME TO qa_original_actor_generations")
	interopDone(t, tx, "CREATE TABLE actor_generations(actor_key,generation,computer_id,source,session_id,agent_id)")
	interopDone(t, tx, "INSERT INTO actor_generations(actor_key,generation,computer_id,source,session_id,agent_id) SELECT actor_key,generation,computer_id,source,session_id,agent_id FROM qa_original_actor_generations")
	interopDone(t, tx, "INSERT INTO actor_generations(actor_key,generation,computer_id,source,session_id,agent_id) SELECT actor_key,generation,computer_id,source,session_id,agent_id FROM qa_original_actor_generations")
	got, found, err = sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, base.Key)
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
		t.Fatal("extra selected dependency produced a usable receipt")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	hrQAReopen(t, f, meta, []sqliteHostReceiptRow{base})
}

func TestSQLiteHostReceiptRowsExplicitInputsAndMissingWriteDependency(t *testing.T) {
	source, realRows := hrQASource(t)
	base := hrQABase(t, source, realRows)
	f, meta := hrQASeed(t, source, []sqliteHostReceiptRow{base})
	for _, which := range []string{"key-length", "fingerprint-length", "error-code", "contract", "revision-zero", "revision-leading-zero", "revision-overflow", "receipt-id", "source", "session", "disposition", "ordering", "durability", "origin", "profile-zero", "profile-none-pair", "time-zero", "actor-generation-zero", "actor-computer", "ceiling-zero", "ceiling-leading-zero", "ceiling-overflow", "ceiling-below-row", "computer"} {
		t.Run(which, func(t *testing.T) {
			row := hrQAClone(base)
			row.Key = strings.Repeat("V", 64)
			ceiling, computer := meta.Revision, meta.ComputerID
			r := &row.Record.Result
			switch which {
			case "key-length":
				row.Key = "private short key"
			case "fingerprint-length":
				row.Record.Fingerprint = "private short fingerprint"
			case "error-code":
				row.Record.ErrorCode = "private unsupported error"
			case "contract":
				r.ContractVersion = 2
			case "revision-zero":
				r.SnapshotRevision = "0"
			case "revision-leading-zero":
				r.SnapshotRevision = "01"
			case "revision-overflow":
				r.SnapshotRevision = "18446744073709551616"
			case "receipt-id":
				r.ID = "private invalid id"
			case "source":
				r.Source = "manual-test"
			case "session":
				r.SessionID = "private\x00session"
			case "disposition":
				r.Disposition = "duplicate"
			case "ordering":
				r.Ordering = "private ordering"
			case "durability":
				r.Durability = "unknown"
			case "origin":
				r.Origin = "verified"
			case "profile-zero":
				r.ProfileRevision = "0"
			case "profile-none-pair":
				r.ProfileBasis = "none"
			case "time-zero":
				r.ObservedAt = time.Time{}
			case "actor-generation-zero":
				r.Actor.Generation = "0"
			case "actor-computer":
				r.Actor.Key.ComputerID = "99999999-9999-4999-8999-999999999999"
			case "ceiling-zero":
				ceiling = "0"
			case "ceiling-leading-zero":
				ceiling = "01"
			case "ceiling-overflow":
				ceiling = "18446744073709551616"
			case "ceiling-below-row":
				ceiling, r.SnapshotRevision = "1", "2"
			case "computer":
				computer = "private invalid computer"
			}
			original := hrQAClone(row)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			delta, err := sqliteInsertHostReceipt(tx, computer, ceiling, row)
			bgQAValidation(t, err)
			interopSafeError(t, err, row.Key, f.directory)
			if delta != 0 || !reflect.DeepEqual(row, original) {
				t.Fatal("invalid input changed caller or returned delta")
			}
			if strings.HasPrefix(which, "ceiling-") && which != "ceiling-below-row" || which == "computer" || which == "key-length" {
				lookup := base.Key
				if which == "key-length" {
					lookup = row.Key
				}
				got, found, err := sqliteReadHostReceipt(tx, computer, ceiling, lookup)
				bgQAValidation(t, err)
				if found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
					t.Fatal("invalid read input exposed a result")
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			hrQAReopen(t, f, meta, []sqliteHostReceiptRow{base})
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	row := hrQAClone(base)
	row.Key = strings.Repeat("D", 64)
	row.Record.Result.Actor.Generation = "1" // Valid shape, absent exact historical row.
	if charge, err := sqliteHostReceiptCharge(row); err != nil || charge != hrQACharge(t, row) {
		t.Fatal("standalone charge incorrectly required SQL dependency", err)
	}
	delta, err := sqliteInsertHostReceipt(tx, meta.ComputerID, meta.Revision, row)
	bgQACorrupt(t, err)
	if delta != 0 || interopCount(t, tx, "SELECT count(*) FROM actor_generations") != 1 {
		t.Fatal("writer invented dependency or charged failed row")
	}
	delta, err = sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, base, row)
	bgQAValidation(t, err) // Key rewrite is an explicit-input refusal before dependency work.
	if delta != 0 {
		t.Fatal("failed key rewrite returned charge")
	}
	row.Key = base.Key
	delta, err = sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, base, row)
	bgQACorrupt(t, err)
	if delta != 0 {
		t.Fatal("missing update dependency returned charge")
	}
	foreign := *base.Record.Result.Actor
	foreign.Key.ComputerID = "99999999-9999-4999-8999-999999999999"
	got, found, err := sqliteLatestHostReceipt(tx, meta.ComputerID, meta.Revision, foreign)
	bgQAValidation(t, err)
	if found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
		t.Fatal("foreign latest input exposed a result")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	hrQAReopen(t, f, meta, []sqliteHostReceiptRow{base})
}

func TestSQLiteHostReceiptRowsCancellationAndTerminalAdapterErrorsPreserveHistory(t *testing.T) {
	source, realRows := hrQASource(t)
	base := hrQABase(t, source, realRows)
	f, meta := hrQASeed(t, source, []sqliteHostReceiptRow{base})
	for _, operation := range []string{"read", "latest", "insert", "update"} {
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
			after := hrQAClone(base)
			after.Record.Result.DiagnosticCode = "private canceled proposal"
			cancel()
			switch operation {
			case "read", "latest":
				var got sqliteHostReceiptRow
				var found bool
				if operation == "read" {
					got, found, err = sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, base.Key)
				} else {
					got, found, err = sqliteLatestHostReceipt(tx, meta.ComputerID, meta.Revision, *base.Record.Result.Actor)
				}
				if found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
					t.Fatal("canceled read exposed stale success")
				}
			case "insert", "update":
				var delta int64
				if operation == "insert" {
					after.Key = strings.Repeat("C", 64)
					delta, err = sqliteInsertHostReceipt(tx, meta.ComputerID, meta.Revision, after)
				} else {
					delta, err = sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, base, after)
				}
				if delta != 0 {
					t.Fatal("canceled write exposed usable delta")
				}
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatal("adapter cancellation evidence was replaced", err)
			}
			interopSafeError(t, err, after.Record.Result.DiagnosticCode, f.directory)
			if cleanup := tx.Rollback(); cleanup != nil && !errors.Is(cleanup, context.Canceled) {
				interopSafeError(t, cleanup, f.directory)
			}
			interopClose(t, c)
			hrQAReopen(t, f, meta, []sqliteHostReceiptRow{base})
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	interopRollback(t, tx)
	got, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, base.Key)
	var checked *sqliteio.Error
	if !errors.As(err, &checked) || found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
		t.Fatal("terminal adapter error became a usable result", err)
	}
	var domain *Error
	if errors.As(err, &domain) && domain.Code == "validation" {
		t.Fatal("unrelated adapter error became validation")
	}
	interopSafeError(t, err, base.Key, f.directory)
	interopClose(t, c)
	hrQAReopen(t, f, meta, []sqliteHostReceiptRow{base})
}

func TestSQLiteHostReceiptRowsLatestRawExactRefAbsenceAfterSelectedDependencyValidation(t *testing.T) {
	source, realRows := hrQASource(t)
	rawRow := hrQABase(t, source, realRows)
	rawRow.Record.Result.Actor.Key.SessionID = "raw-history-" + string([]byte{0xff})
	rawRef := *rawRow.Record.Result.Actor
	// Complete owned state and actual strict legacy read establish this admitted
	// raw shape and the canonical stored identity for the active JSON encoder.
	canonical := hrQALegacy(t, source.ComputerID, source.Revision, rawRow)
	canonicalRef := *canonical.Record.Result.Actor
	if rawRef == canonicalRef {
		t.Fatal("malformed raw Ref fixture did not repair")
	}
	stableKey := actorKey(rawRef.Key) == actorKey(canonicalRef.Key)
	f, meta := hrQASeed(t, source, []sqliteHostReceiptRow{canonical})
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	got, found, err := sqliteLatestHostReceipt(tx, meta.ComputerID, meta.Revision, canonicalRef)
	if err != nil || !found || !reflect.DeepEqual(got, canonical) {
		t.Fatal("canonical exact Ref positive control failed", err)
	}
	got, found, err = sqliteLatestHostReceipt(tx, meta.ComputerID, meta.Revision, rawRef)
	if err != nil || found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
		t.Fatal("raw exact Ref incorrectly matched its repaired stored Ref", err)
	}
	if rawRef != *rawRow.Record.Result.Actor {
		t.Fatal("latest lookup rewrote caller Ref")
	}
	// In the stable-key case, the indexed raw query really selects the receipt;
	// its mismatching exact Ref must not hide a corrupt selected dependency.
	// A genuine encoder key drift means that raw query selects no receipt and
	// legitimately remains absent; do not manufacture a drift or collision.
	interopDone(t, tx, "UPDATE actor_generations SET session_id='disagreeing selected projection' WHERE actor_key=? AND generation=?", sqliteio.Text(actorKey(canonicalRef.Key)), interopCounter(t, canonicalRef.Generation))
	got, found, err = sqliteLatestHostReceipt(tx, meta.ComputerID, meta.Revision, canonicalRef)
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) {
		t.Fatal("canonical selected corruption exposed a usable result")
	}
	got, found, err = sqliteLatestHostReceipt(tx, meta.ComputerID, meta.Revision, rawRef)
	if stableKey {
		bgQACorrupt(t, err)
	} else if err != nil {
		t.Fatal("unmatched raw key inspected unrelated dependency", err)
	}
	if found || !reflect.DeepEqual(got, sqliteHostReceiptRow{}) || rawRef != *rawRow.Record.Result.Actor {
		t.Fatal("raw mismatch/corruption exposed result or mutated caller")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	hrQAReopen(t, f, meta, []sqliteHostReceiptRow{canonical})
}

// Explicit proposals use validation when a nonzero time passes the legacy
// domain but cannot cross its JSON persistence boundary. Stored malformed time
// remains state_corrupt under the unchanged decoder group above.
func TestSQLiteHostReceiptRowsExplicitUnencodableTimeValidationPreservesHistory(t *testing.T) {
	source, realRows := hrQASource(t)
	originalBase := hrQABase(t, source, realRows)
	for _, shape := range []string{"actor-present", "actor-nil"} {
		t.Run(shape, func(t *testing.T) {
			base := hrQAClone(originalBase)
			if shape == "actor-nil" {
				base.Record.Result.Actor = nil
			}
			base = hrQALegacy(t, source.ComputerID, source.Revision, base)
			f, meta := hrQASeed(t, source, []sqliteHostReceiptRow{base})
			for _, operation := range []string{"charge", "insert", "update"} {
				t.Run(operation, func(t *testing.T) {
					control := hrQAClone(base)
					if operation != "update" {
						control.Key = strings.Repeat("V", 64)
					}
					control.Record.Result.ObservedAt = time.Date(9999, 12, 30, 12, 34, 56, 123456789, time.UTC)
					control = hrQALegacy(t, meta.ComputerID, meta.Revision, control)
					ownedControl, ownedBefore := hrQAClone(control), hrQAClone(base)
					if b, err := control.Record.Result.ObservedAt.MarshalJSON(); err != nil || len(b) == 0 {
						t.Fatal("encodable time positive control failed")
					}
					if charge, err := sqliteHostReceiptCharge(control); err != nil || charge != hrQACharge(t, control) {
						t.Fatal("otherwise-valid standalone charge control failed", err)
					}
					c, tx := interopOpen(t, f, false, sqliteio.Write)
					if operation == "insert" {
						if delta, err := sqliteInsertHostReceipt(tx, meta.ComputerID, meta.Revision, control); err != nil || delta != hrQACharge(t, control) {
							t.Fatal("otherwise-valid insert control failed", err)
						}
					} else if operation == "update" {
						if delta, err := sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, base, control); err != nil || delta != hrQACharge(t, control)-hrQACharge(t, base) {
							t.Fatal("otherwise-valid update control failed", err)
						}
					}
					if !reflect.DeepEqual(control, ownedControl) || !reflect.DeepEqual(base, ownedBefore) {
						t.Fatal("valid control changed owned input")
					}
					interopRollback(t, tx)
					interopClose(t, c)
					hrQAReopen(t, f, meta, []sqliteHostReceiptRow{base})

					candidate := hrQAClone(control)
					candidate.Record.Result.ObservedAt = time.Date(10000, 1, 1, 12, 34, 56, 123456789, time.UTC)
					if candidate.Record.Result.ObservedAt.IsZero() {
						t.Fatal("unencodable time fixture must remain nonzero")
					}
					// The actual complete legacy validator accepts this raw shape;
					// only the explicit persistence encoding is inadmissible.
					hrQACompleteLegacy(t, meta.ComputerID, meta.Revision, candidate)
					if _, err := candidate.Record.Result.ObservedAt.MarshalJSON(); err == nil {
						t.Fatal("year10000 did not exercise actual time.MarshalJSON refusal")
					}
					ownedCandidate := hrQAClone(candidate)
					c, tx = interopOpen(t, f, false, sqliteio.Write)
					staged := hrQAClone(base)
					staged.Key = strings.Repeat("S", 64)
					if delta, err := sqliteInsertHostReceipt(tx, meta.ComputerID, meta.Revision, staged); err != nil || delta != hrQACharge(t, staged) {
						t.Fatal("earlier staged row control failed", err)
					}
					var delta int64
					var err error
					switch operation {
					case "charge":
						delta, err = sqliteHostReceiptCharge(candidate)
					case "insert":
						delta, err = sqliteInsertHostReceipt(tx, meta.ComputerID, meta.Revision, candidate)
					case "update":
						delta, err = sqliteUpdateHostReceipt(tx, meta.ComputerID, meta.Revision, base, candidate)
					}
					bgQAValidation(t, err)
					interopSafeError(t, err, candidate.Key, f.directory)
					if delta != 0 || !reflect.DeepEqual(candidate, ownedCandidate) || !reflect.DeepEqual(base, ownedBefore) {
						t.Fatal("unencodable explicit time returned charge or changed owned input")
					}
					if got, found, err := sqliteReadHostReceipt(tx, meta.ComputerID, meta.Revision, base.Key); err != nil || !found || !reflect.DeepEqual(got, base) {
						t.Fatal("unencodable time changed prior receipt")
					}
					if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, meta) {
						t.Fatal("unencodable time changed caller-owned metadata")
					}
					if interopCount(t, tx, "SELECT count(*) FROM host_receipts") != 2 || hrQAStoredCharge(t, tx) != hrQACharge(t, base)+hrQACharge(t, staged) {
						t.Fatal("unencodable time staged an extra effect or changed history")
					}
					interopRollback(t, tx)
					interopClose(t, c)
					hrQAReopen(t, f, meta, []sqliteHostReceiptRow{base})
				})
			}
		})
	}
}
