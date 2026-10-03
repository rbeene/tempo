//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent source-first QA of exactly six Local/initial-row APIs. The real
// approved interval Local producer is a prerequisite, never replaced by a stub.

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func TestSQLiteSealQueuedRowsInitialLiteral372PairAndColdPersistence(t *testing.T) {
	st, item := sqQALegacy(t, "queued")
	f, m, before := sqQASeed(t, st, item)
	component := sqQAComponent(t, st, item.Interval)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	if got, found, err := sqliteReadIntervalComponent(tx, m.ComputerID, item.Interval.ID); err != nil || found || got != "" {
		t.Fatal("seal absence", err)
	}
	if got, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID); err != nil || found || !reflect.DeepEqual(got, sqliteOutboxLocalRow{}) {
		t.Fatal("root absence", err)
	}
	d1, err := sqliteInsertIntervalComponent(tx, m.ComputerID, item.Interval.ID, component)
	if err != nil || d1 != 154 {
		t.Fatal("literal seal charge154", err)
	}
	d2, err := sqliteInsertQueuedOutbox(tx, m.ComputerID, item.Interval.ID, item.ID)
	if err != nil || d2 != 218 || d1+d2 != 372 {
		t.Fatal("literal fresh root218 / pair372", err)
	}
	if charge, err := sqliteIntervalComponentCharge(item.Interval.ID, component); err != nil || charge != 50+int64(len(item.Interval.ID)+len(component)) {
		t.Fatal("independent seal charge", err)
	}
	expected := sqliteOutboxLocalRow{IntervalID: item.Interval.ID, ID: item.ID, Revision: "1", State: "queued", Correlation: "tempo:" + item.Interval.ID}
	if got, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID); err != nil || !found || !reflect.DeepEqual(got, expected) {
		t.Fatal("initial full ten-column projection", err)
	}
	if charge, err := sqliteOutboxLocalCharge(expected); err != nil || charge != sqQACharge(expected) {
		t.Fatal("independent root charge", err)
	}
	seal, found, err := sqliteReadIntervalComponent(tx, m.ComputerID, item.Interval.ID)
	if err != nil || !found || seal != component {
		t.Fatal("exact singular seal projection", err)
	}
	// Independent physical-kind and big-endian witnesses do not trust the
	// producer's matching encoder/decoder to agree on a mistaken representation.
	raw := interopPrepare(t, tx, "SELECT "+sqQAOutboxCols+" FROM outbox WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
	present, err := raw.Step()
	if err != nil || !present || raw.ColumnCount() != 10 {
		t.Fatal("literal outbox10", err)
	}
	for column, wanted := range []sqliteio.Kind{sqliteio.TextKind, sqliteio.TextKind, sqliteio.BlobKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.NullKind, sqliteio.NullKind, sqliteio.NullKind, sqliteio.NullKind, sqliteio.IntegerKind} {
		kind, err := raw.Kind(column)
		if err != nil || kind != wanted {
			t.Fatal("literal initial outbox kind", column, err)
		}
	}
	revision, err := raw.Blob(2)
	if err != nil || !bytes.Equal(revision, []byte{0, 0, 0, 0, 0, 0, 0, 1}) {
		t.Fatal("initial revision is not literal big-endian1", err)
	}
	flag, err := raw.Int64(9)
	if err != nil || flag != 0 {
		t.Fatal("literal initial plan flag", err)
	}
	if present, err = raw.Step(); err != nil || present {
		t.Fatal("outbox singleton DONE", err)
	}
	bgQAFixtureError(t, "literal outbox Close", raw.Close())
	raw = interopPrepare(t, tx, "SELECT "+sqQASealCols+" FROM interval_components WHERE interval_id=? ORDER BY component_id", sqliteio.Text(item.Interval.ID))
	present, err = raw.Step()
	if err != nil || !present || raw.ColumnCount() != 2 {
		t.Fatal("literal seal2", err)
	}
	for column := 0; column < 2; column++ {
		kind, err := raw.Kind(column)
		if err != nil || kind != sqliteio.TextKind {
			t.Fatal("literal seal TEXT kind", column, err)
		}
	}
	if present, err = raw.Step(); err != nil || present {
		t.Fatal("seal singleton DONE", err)
	}
	bgQAFixtureError(t, "literal seal Close", raw.Close())
	// Singular provenance is retained without a live-frontier FK or invented
	// frontier row; no complete support/seal composer acceptance is claimed.
	if interopCount(t, tx, "SELECT count(*) FROM union_frontier") != 0 || interopCount(t, tx, "SELECT count(*) FROM sync_plans") != 0 || interopCount(t, tx, "SELECT count(*) FROM requests") != 0 {
		t.Fatal("initial row writers invented frontier/plan/request")
	}
	if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, m) {
		t.Fatal("row APIs changed metadata", err)
	}
	next := metaQANext(t, m)
	next.Revision, next.LogicalBytes = bump(m.Revision), m.LogicalBytes+372
	bgQAFixtureError(t, "single enclosing row metadata CAS", sqliteUpdateMeta(tx, m, next))
	snapshot, total := sqQAAudit(t, tx, next)
	if total != next.LogicalBytes || reflect.DeepEqual(snapshot, before) {
		t.Fatal("literal charged rows differ from exact372 metadata increment")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	sqQAReopen(t, f, next, snapshot)
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	got, found, err := sqliteReadOutboxLocal(tx, next.ComputerID, item.Interval.ID)
	if err != nil || !found || !reflect.DeepEqual(got, expected) {
		t.Fatal("cold initial root", err)
	}
	seal, found, err = sqliteReadIntervalComponent(tx, next.ComputerID, item.Interval.ID)
	if err != nil || !found || seal != component {
		t.Fatal("cold retained seal", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteSealQueuedRowsActualLaterStateScalarsAndOwnedOptionalText(t *testing.T) {
	for _, phase := range []string{"queued", "submitting", "synced", "rejected", "unknown", "needs_attention"} {
		t.Run(phase, func(t *testing.T) {
			st, item := sqQALegacy(t, phase)
			f, m, snapshot := sqQASeed(t, st, item)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			expected := sqQAClone(sqQARow(item))
			// Actual complete reference scalar, staged literally because no later
			// mutable sync writer is part of these six APIs. Deferred request/plan
			// dependencies are intentionally absent; this local stage rolls back.
			sqQABindOutbox(t, tx, expected)
			got, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID)
			if err != nil || !found || !reflect.DeepEqual(got, expected) {
				t.Fatal("real complete reference root scalar", err)
			}
			if charge, err := sqliteOutboxLocalCharge(got); err != nil || charge != sqQACharge(expected) {
				t.Fatal("later scalar literal charge", err)
			}
			for _, p := range []*string{got.EntryID, got.FailureCategory, got.RetryRequestID, got.RunRequestID} {
				if p != nil {
					*p = "caller mutated owned pointer"
				}
			}
			again, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID)
			if err != nil || !found || !reflect.DeepEqual(again, expected) {
				t.Fatal("caller pointers changed retained scalar", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			sqQAReopen(t, f, m, snapshot)
		})
	}
	t.Run("actual-authorized-retry-pointer", func(t *testing.T) {
		service, _, saved := qaSyncAuthorizedRejected(t)
		st := hnQAValid(t, bgQAReadLegacy(t, service))
		item := st.Outbox[saved.Interval.ID]
		if item.State != "queued" || item.RetryRequestID == nil || *item.RetryRequestID != qaSyncID(231) {
			t.Fatal("actual authorized retry scalar absent")
		}
		f, m, snapshot := sqQASeed(t, st, item)
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		expected := sqQAClone(sqQARow(item))
		sqQABindOutbox(t, tx, expected)
		got, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID)
		if err != nil || !found || !reflect.DeepEqual(got, expected) {
			t.Fatal("actual retained retry pointer", err)
		}
		if charge, err := sqliteOutboxLocalCharge(got); err != nil || charge != sqQACharge(expected) {
			t.Fatal("actual retry scalar charge", err)
		}
		*got.RetryRequestID = "caller changed retry"
		again, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID)
		if err != nil || !found || !reflect.DeepEqual(again, expected) {
			t.Fatal("retry pointer ownership", err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
		sqQAReopen(t, f, m, snapshot)
	})
	t.Run("max-counter-and-nil-versus-present-empty-failure", func(t *testing.T) {
		st, item := sqQALegacy(t, "queued")
		item.Revision, item.FailureCategory = asQAMax, asQAPointer("")
		st.Outbox[item.Interval.ID] = item
		st = hnQAValid(t, st) // Complete strict legacy positive for this policy.
		item = st.Outbox[item.Interval.ID]
		f, m, snapshot := sqQASeed(t, st, item)
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		r := sqQARow(item)
		sqQABindOutbox(t, tx, r)
		got, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, r.IntervalID)
		if err != nil || !found || got.FailureCategory == nil || *got.FailureCategory != "" || got.Revision != asQAMax {
			t.Fatal("NULL/present-empty or full uint64 collapsed", err)
		}
		nilFailure := sqQAClone(r)
		nilFailure.FailureCategory = nil
		charge, err := sqliteOutboxLocalCharge(r)
		bgQAFixtureError(t, "present-empty charge", err)
		lower, err := sqliteOutboxLocalCharge(nilFailure)
		bgQAFixtureError(t, "NULL charge", err)
		if charge-lower != 8 || charge != sqQACharge(r) {
			t.Fatal("present-empty TEXT length header not charged")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		sqQAReopen(t, f, m, snapshot)
	})
	t.Run("local-optional-increments-and-utf8-byte-length", func(t *testing.T) {
		st, item := sqQALegacy(t, "queued")
		f, m, snapshot := sqQASeed(t, st, item)
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		r := sqQARow(item)
		r.EntryID, r.FailureCategory, r.RetryRequestID, r.RunRequestID, r.PlanPresent = asQAPointer("42"), asQAPointer("retained é 東京"), asQAPointer(qaSyncID(61)), asQAPointer(qaSyncID(62)), true
		owned := sqQAClone(r)
		sqQABindOutbox(t, tx, r)
		got, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, r.IntervalID)
		if err != nil || !found || !reflect.DeepEqual(got, r) {
			t.Fatal("local scalar strengthened into full sync graph policy", err)
		}
		charge, err := sqliteOutboxLocalCharge(r)
		if err != nil || charge != sqQACharge(r) || !reflect.DeepEqual(r, owned) {
			t.Fatal("optional owned UTF-8 byte charge", err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
		sqQAReopen(t, f, m, snapshot)
	})
}

func TestSQLiteSealQueuedRowsInputValidationAndMissingVersusSelectedScope(t *testing.T) {
	st, item := sqQALegacy(t, "queued")
	f, m, snapshot := sqQASeed(t, st, item)
	component := sqQAComponent(t, st, item.Interval)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	for _, args := range [][3]string{{"bad computer", item.Interval.ID, sqQARoot}, {m.ComputerID, "bad interval", sqQARoot}, {m.ComputerID, item.Interval.ID, "bad root"}, {m.ComputerID, item.Interval.ID, item.Interval.ID}} {
		delta, err := sqliteInsertQueuedOutbox(tx, args[0], args[1], args[2])
		bgQAValidation(t, err)
		var native *sqliteio.Error
		if delta != 0 || errors.As(err, &native) {
			t.Fatal("definite queued input validation reached native constraints")
		}
	}
	for _, bad := range []string{"", "fc1:" + strings.Repeat("a", 63), "fc1:" + strings.Repeat("A", 64), "fc2:" + strings.Repeat("a", 64), item.Interval.ID} {
		delta, err := sqliteInsertIntervalComponent(tx, m.ComputerID, item.Interval.ID, bad)
		bgQAValidation(t, err)
		var native *sqliteio.Error
		if delta != 0 || errors.As(err, &native) {
			t.Fatal("definite component input validation reached native constraints")
		}
		if charge, err := sqliteIntervalComponentCharge(item.Interval.ID, bad); charge != 0 {
			t.Fatal("invalid component charge output")
		} else {
			bgQAValidation(t, err)
		}
	}
	for _, axis := range []string{"same-root-interval", "zero-revision", "noncanonical-revision", "state", "correlation", "entry-empty", "entry-leading-zero", "retry-empty", "run-empty", "failure-invalid-utf8"} {
		r := sqQARow(item)
		switch axis {
		case "same-root-interval":
			r.ID = r.IntervalID
		case "zero-revision":
			r.Revision = "0"
		case "noncanonical-revision":
			r.Revision = "01"
		case "state":
			r.State = "invalid"
		case "correlation":
			r.Correlation += "wrong"
		case "entry-empty":
			r.EntryID = asQAPointer("")
		case "entry-leading-zero":
			r.EntryID = asQAPointer("01")
		case "retry-empty":
			r.RetryRequestID = asQAPointer("")
		case "run-empty":
			r.RunRequestID = asQAPointer("")
		case "failure-invalid-utf8":
			r.FailureCategory = asQAPointer(string([]byte{0xff}))
			if utf8.ValidString(*r.FailureCategory) {
				t.Fatal("UTF-8 witness lost")
			}
		}
		owned := sqQAClone(r)
		charge, err := sqliteOutboxLocalCharge(r)
		bgQAValidation(t, err)
		if charge != 0 || !reflect.DeepEqual(r, owned) {
			t.Fatal("invalid charge yielded bytes or repaired caller")
		}
	}
	if row, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, sqQAMissing); err != nil || found || !reflect.DeepEqual(row, sqliteOutboxLocalRow{}) {
		t.Fatal("missing primary root confused with required owner", err)
	}
	if id, found, err := sqliteReadIntervalComponent(tx, m.ComputerID, sqQAMissing); err != nil || found || id != "" {
		t.Fatal("missing primary seal confused with required owner", err)
	}
	if delta, err := sqliteInsertQueuedOutbox(tx, m.ComputerID, sqQAMissing, sqQARoot); delta != 0 {
		t.Fatal("missing interval queued delta")
	} else {
		bgQACorrupt(t, err)
	}
	if delta, err := sqliteInsertIntervalComponent(tx, m.ComputerID, sqQAMissing, component); delta != 0 {
		t.Fatal("missing interval seal delta")
	} else {
		bgQACorrupt(t, err)
	}
	foreign := sqQAInterval(item, 2)
	foreign.ID, foreign.ComputerID = sqQAMissing, asQAForeign
	_, err := sqliteInsertIntervalLocal(tx, asQAForeign, foreign)
	bgQAFixtureError(t, "real foreign interval control", err)
	_, err = sqliteInsertQueuedOutbox(tx, asQAForeign, foreign.ID, sqQARoot)
	bgQAFixtureError(t, "real foreign queued control", err)
	_, err = sqliteInsertIntervalComponent(tx, asQAForeign, foreign.ID, component)
	bgQAFixtureError(t, "real foreign retained seal control", err)
	if _, found, err := sqliteReadOutboxLocal(tx, asQAForeign, foreign.ID); err != nil || !found {
		t.Fatal("foreign positive scope", err)
	}
	if _, found, err := sqliteReadIntervalComponent(tx, asQAForeign, foreign.ID); err != nil || !found {
		t.Fatal("foreign seal positive scope", err)
	}
	row, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, foreign.ID)
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(row, sqliteOutboxLocalRow{}) {
		t.Fatal("selected foreign root hidden or leaked")
	}
	seal, found, err := sqliteReadIntervalComponent(tx, m.ComputerID, foreign.ID)
	bgQACorrupt(t, err)
	if found || seal != "" {
		t.Fatal("selected foreign seal hidden or leaked")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	sqQAReopen(t, f, m, snapshot)
}

func TestSQLiteSealQueuedRowsSelectedKindsValuesOwnersAndCollision(t *testing.T) {
	st, item := sqQALegacy(t, "queued")
	f, m, snapshot := sqQASeed(t, st, item)
	component := sqQAComponent(t, st, item.Interval)
	for _, column := range strings.Split(sqQAOutboxCols, ",") {
		if column == "interval_id" {
			continue
		} // Raw PK query cannot select a different PK.
		t.Run("outbox-kind-"+column, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			_, err := sqliteInsertQueuedOutbox(tx, m.ComputerID, item.Interval.ID, item.ID)
			bgQAFixtureError(t, "valid selected root", err)
			if _, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID); err != nil || !found {
				t.Fatal("root positive", err)
			}
			asQAShadow(t, tx, "outbox", sqQAOutboxCols)
			badKind := sqliteio.Integer(1)
			if column == "revision" || column == "plan_present" {
				badKind = sqliteio.Text("wrong native kind")
			}
			interopDone(t, tx, "UPDATE outbox SET "+column+"=? WHERE interval_id=?", badKind, sqliteio.Text(item.Interval.ID))
			row, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID)
			bgQACorrupt(t, err)
			if found || !reflect.DeepEqual(row, sqliteOutboxLocalRow{}) {
				t.Fatal("wrong-kind selected row leaked partial output")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	for _, axis := range []string{"root-equals-interval", "root-uuid", "revision-size", "revision-zero", "revision-null", "state", "correlation", "entry", "failure-utf8", "retry", "run", "bool"} {
		t.Run("outbox-value-"+axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			_, err := sqliteInsertQueuedOutbox(tx, m.ComputerID, item.Interval.ID, item.ID)
			bgQAFixtureError(t, "root before selected corruption", err)
			asQAShadow(t, tx, "outbox", sqQAOutboxCols)
			column, value := "id", sqliteio.Text(item.Interval.ID)
			switch axis {
			case "root-uuid":
				value = sqliteio.Text("bad UUID")
			case "revision-size":
				column, value = "revision", sqliteio.Blob([]byte{1})
			case "revision-zero":
				column, value = "revision", sqliteio.Blob(make([]byte, 8))
			case "revision-null":
				column, value = "revision", sqliteio.Null()
			case "state":
				column, value = "state", sqliteio.Text("bad state")
			case "correlation":
				column, value = "correlation", sqliteio.Text("wrong correlation")
			case "entry":
				column, value = "entry_id", sqliteio.Text("")
			case "failure-utf8":
				column, value = "failure_category", sqliteio.Text(string([]byte{0xff}))
			case "retry":
				column, value = "retry_request_id", sqliteio.Text("")
			case "run":
				column, value = "run_request_id", sqliteio.Text("not a UUID")
			case "bool":
				column, value = "plan_present", sqliteio.Integer(2)
			}
			interopDone(t, tx, "UPDATE outbox SET "+column+"=? WHERE interval_id=?", value, sqliteio.Text(item.Interval.ID))
			row, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID)
			bgQACorrupt(t, err)
			if found || !reflect.DeepEqual(row, sqliteOutboxLocalRow{}) {
				t.Fatal("invalid selected scalar leaked partial output")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	for _, axis := range []string{"component-kind", "component-null", "component-shape", "second-component", "missing-owner", "invalid-owner"} {
		t.Run("seal-"+axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			_, err := sqliteInsertIntervalComponent(tx, m.ComputerID, item.Interval.ID, component)
			bgQAFixtureError(t, "valid seal before corruption", err)
			if _, found, err := sqliteReadIntervalComponent(tx, m.ComputerID, item.Interval.ID); err != nil || !found {
				t.Fatal("seal positive", err)
			}
			switch axis {
			case "component-kind", "component-null":
				asQAShadow(t, tx, "interval_components", sqQASealCols)
				value := sqliteio.Blob([]byte(component))
				if axis == "component-null" {
					value = sqliteio.Null()
				}
				interopDone(t, tx, "UPDATE interval_components SET component_id=? WHERE interval_id=?", value, sqliteio.Text(item.Interval.ID))
			case "component-shape":
				interopDone(t, tx, "UPDATE interval_components SET component_id=? WHERE interval_id=?", sqliteio.Text("fc1:"+strings.Repeat("A", 64)), sqliteio.Text(item.Interval.ID))
			case "second-component":
				other := "fc1:" + strings.Repeat("a", 64)
				if other == component {
					other = "fc1:" + strings.Repeat("b", 64)
				}
				interopDone(t, tx, "INSERT INTO interval_components(interval_id,component_id) VALUES(?,?)", sqliteio.Text(item.Interval.ID), sqliteio.Text(other))
			case "missing-owner":
				interopDone(t, tx, "DELETE FROM intervals WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "invalid-owner":
				interopDone(t, tx, "UPDATE intervals SET start_json=? WHERE interval_id=?", sqliteio.Text("invalid JSON time"), sqliteio.Text(item.Interval.ID))
			}
			got, found, err := sqliteReadIntervalComponent(tx, m.ComputerID, item.Interval.ID)
			bgQACorrupt(t, err)
			if found || got != "" {
				t.Fatal("invalid selected seal/owner leaked partial output")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	for _, axis := range []string{"missing-owner", "invalid-owner", "part-collision", "attempt-collision"} {
		t.Run("outbox-"+axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			_, err := sqliteInsertQueuedOutbox(tx, m.ComputerID, item.Interval.ID, item.ID)
			bgQAFixtureError(t, "valid root owner control", err)
			switch axis {
			case "missing-owner":
				interopDone(t, tx, "DELETE FROM intervals WHERE interval_id=?", sqliteio.Text(item.Interval.ID))
			case "invalid-owner":
				interopDone(t, tx, "UPDATE intervals SET end_json=? WHERE interval_id=?", sqliteio.Text("invalid JSON time"), sqliteio.Text(item.Interval.ID))
			case "part-collision", "attempt-collision":
				// Deliberate selected corruption in a relaxed owned table bypasses
				// its insert trigger; the separate native group tests real triggers.
				table, columns := "sync_parts", sqQAPartCols
				if axis == "attempt-collision" {
					table, columns = "sync_attempts", sqQAAttemptCols
				}
				asQAShadow(t, tx, table, columns)
				interopDone(t, tx, "INSERT INTO "+table+"(id) VALUES(?)", sqliteio.Text(sqQARoot))
				if _, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID); err != nil || !found {
					t.Fatal("unrelated child ID control", err)
				}
				interopDone(t, tx, "UPDATE "+table+" SET id=?", sqliteio.Text(item.ID))
			}
			row, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID)
			bgQACorrupt(t, err)
			if found || !reflect.DeepEqual(row, sqliteOutboxLocalRow{}) {
				t.Fatal("selected owner/cross-family collision hidden or leaked")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	sqQAReopen(t, f, m, snapshot)
}

func TestSQLiteSealQueuedRowsActual1811TriggersAndIsolated1555Or2067(t *testing.T) {
	st, item := sqQALegacy(t, "synced")
	if item.Plan == nil || len(item.Plan.Parts) != 1 || len(item.Plan.Parts[0].Attempts) != 1 {
		t.Fatal("complete actual part/attempt fixture absent")
	}
	part := item.Plan.Parts[0]
	attempt := part.Attempts[0]
	seen := map[string]bool{}
	for _, id := range []string{item.Interval.ID, item.ID, part.ID, attempt.ID, sqQARoot, sqQAOtherInterval, qaSyncID(79)} {
		if seen[id] {
			t.Fatal("native collision fixture/control identities overlap")
		}
		seen[id] = true
	}
	f, m, snapshot := sqQASeed(t, st, item)
	for _, subject := range []string{"outbox", "sync_parts", "sync_attempts"} {
		for _, operation := range []string{"insert", "update"} {
			families := []string{"root"}
			if subject == "outbox" {
				families = []string{"part", "attempt"}
			}
			for _, family := range families {
				t.Run(subject+"_id_"+operation+"/"+family, func(t *testing.T) {
					for _, collide := range []bool{false, true} {
						c, tx := interopOpen(t, f, false, sqliteio.Write)
						// Real strict-schema, all-column rows from a complete valid
						// sync oracle. Missing plan/request/part FK targets are allowed
						// only for this explicitly rollback-only constraint witness.
						insertPart := func() error {
							return sqQARawWrite(tx, "INSERT INTO sync_parts("+sqQAPartCols+") VALUES("+strings.TrimSuffix(strings.Repeat("?,", 28), ",")+")", sqQAPartValues(t, item.Interval.ID, part)...)
						}
						insertAttempt := func() error {
							return sqQARawWrite(tx, "INSERT INTO sync_attempts("+sqQAAttemptCols+") VALUES(?,?,?,?,?,?,?,?,?)", sqQAAttemptValues(item.Interval.ID, attempt)...)
						}
						var err error
						if subject == "outbox" {
							childID := part.ID
							if family == "part" {
								bgQAFixtureError(t, "otherwise valid strict part fixture", insertPart())
							} else {
								childID = attempt.ID
								bgQAFixtureError(t, "otherwise valid strict attempt fixture", insertAttempt())
							}
							rootID := sqQARoot
							if collide {
								rootID = childID
							}
							if operation == "insert" {
								if erQACount(t, tx, "SELECT count(*) FROM outbox WHERE interval_id=?", sqliteio.Text(item.Interval.ID)) != 0 {
									t.Fatal("1811 insert masked by root PK")
								}
								delta, failure := sqliteInsertQueuedOutbox(tx, m.ComputerID, item.Interval.ID, rootID)
								err = failure
								if collide && delta != 0 || !collide && delta != 218 {
									t.Fatal("trigger insert output charge incorrect")
								}
							} else {
								_, failure := sqliteInsertQueuedOutbox(tx, m.ComputerID, item.Interval.ID, sqQARoot)
								bgQAFixtureError(t, "initial root before trigger update", failure)
								if !collide {
									rootID = qaSyncID(79)
								}
								err = sqQARawWrite(tx, "UPDATE outbox SET id=? WHERE interval_id=?", sqliteio.Text(rootID), sqliteio.Text(item.Interval.ID))
							}
						} else {
							if operation == "update" {
								if subject == "sync_parts" {
									bgQAFixtureError(t, "part before update trigger", insertPart())
								} else {
									bgQAFixtureError(t, "attempt before update trigger", insertAttempt())
								}
							}
							rootID := sqQARoot
							if operation == "insert" && collide {
								if subject == "sync_parts" {
									rootID = part.ID
								} else {
									rootID = attempt.ID
								}
							}
							_, failure := sqliteInsertQueuedOutbox(tx, m.ComputerID, item.Interval.ID, rootID)
							bgQAFixtureError(t, "root before reverse trigger", failure)
							if operation == "insert" {
								if subject == "sync_parts" {
									err = insertPart()
								} else {
									err = insertAttempt()
								}
							} else {
								newID := qaSyncID(79)
								if collide {
									newID = rootID
								}
								if subject == "sync_parts" {
									err = sqQARawWrite(tx, "UPDATE sync_parts SET id=?,correlation=? WHERE interval_id=? AND ordinal=0", sqliteio.Text(newID), sqliteio.Text("tempo:v1:"+newID), sqliteio.Text(item.Interval.ID))
								} else {
									err = sqQARawWrite(tx, "UPDATE sync_attempts SET id=? WHERE interval_id=? AND part_ordinal=0 AND ordinal=0", sqliteio.Text(newID), sqliteio.Text(item.Interval.ID))
								}
							}
						}
						if collide {
							sqQAConstraint(t, err, 1811)
							interopSafeError(t, err, item.ID, part.ID, attempt.ID, f.directory)
						} else {
							bgQAFixtureError(t, "isolated trigger positive control", err)
						}
						interopRollback(t, tx)
						interopClose(t, c)
					}
				})
			}
		}
	}
	for _, axis := range []string{"outbox-primary1555", "outbox-id2067", "seal-existing-precheck", "seal-component2067"} {
		t.Run(axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			component := sqQAComponent(t, st, item.Interval)
			_, err := sqliteInsertQueuedOutbox(tx, m.ComputerID, item.Interval.ID, item.ID)
			bgQAFixtureError(t, "noncolliding root control", err)
			_, err = sqliteInsertIntervalComponent(tx, m.ComputerID, item.Interval.ID, component)
			bgQAFixtureError(t, "noncolliding seal control", err)
			var delta int64
			switch axis {
			case "outbox-primary1555":
				delta, err = sqliteInsertQueuedOutbox(tx, m.ComputerID, item.Interval.ID, sqQARoot)
				sqQAConstraint(t, err, 1555)
			case "outbox-id2067":
				delta, err = sqliteInsertQueuedOutbox(tx, m.ComputerID, sqQAOtherInterval, item.ID)
				sqQAConstraint(t, err, 2067)
			case "seal-existing-precheck":
				delta, err = sqliteInsertIntervalComponent(tx, m.ComputerID, item.Interval.ID, component)
				bgQAValidation(t, err)
				var native *sqliteio.Error
				if errors.As(err, &native) {
					t.Fatal("existing selected seal precheck misrepresented as native PK")
				}
			case "seal-component2067":
				delta, err = sqliteInsertIntervalComponent(tx, m.ComputerID, sqQAOtherInterval, component)
				sqQAConstraint(t, err, 2067)
			}
			if delta != 0 {
				t.Fatal("failed row insert exposed nonzero delta")
			}
			interopSafeError(t, err, f.directory, item.ID, component)
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	sqQAReopen(t, f, m, snapshot)
}

func TestSQLiteSealQueuedRowsCancellationReadOnlyAndPartialStageRollback(t *testing.T) {
	st, item := sqQALegacy(t, "queued")
	f, m, snapshot := sqQASeed(t, st, item)
	component := sqQAComponent(t, st, item.Interval)
	t.Run("actual-cancel-clears-row-and-insert-outputs", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		conn, err := sqliteio.Open(ctx, f.directory, f.database, sqliteio.Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := conn.Close(context.Background()); err != nil {
				t.Errorf("owned connection cleanup: %v", err)
			}
		})
		tx, err := conn.Begin(ctx, sqliteio.Write)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := tx.Rollback(); err != nil {
				t.Errorf("owned transaction cleanup: %v", err)
			}
		})
		if _, found, err := sqliteReadIntervalLocal(tx, m.ComputerID, item.Interval.ID); err != nil || !found {
			t.Fatal("real owner before cancellation", err)
		}
		cancel()
		check := func(err error) {
			t.Helper()
			var native *sqliteio.Error
			if !errors.Is(err, context.Canceled) || !errors.As(err, &native) || native.Category != sqliteio.Canceled {
				t.Fatal("cancellation cause/category lost", err)
			}
			interopSafeError(t, err, f.directory, item.Interval.ID)
		}
		row, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID)
		check(err)
		if found || !reflect.DeepEqual(row, sqliteOutboxLocalRow{}) {
			t.Fatal("canceled root leaked output")
		}
		seal, found, err := sqliteReadIntervalComponent(tx, m.ComputerID, item.Interval.ID)
		check(err)
		if found || seal != "" {
			t.Fatal("canceled seal leaked output")
		}
		delta, err := sqliteInsertQueuedOutbox(tx, m.ComputerID, item.Interval.ID, item.ID)
		check(err)
		if delta != 0 {
			t.Fatal("canceled root charge")
		}
		delta, err = sqliteInsertIntervalComponent(tx, m.ComputerID, item.Interval.ID, component)
		check(err)
		if delta != 0 {
			t.Fatal("canceled seal charge")
		}
		interopRollback(t, tx)
		interopClose(t, conn)
		sqQAReopen(t, f, m, snapshot)
	})
	t.Run("actual-read-only-mutation-refusal", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Read)
		for _, seal := range []bool{false, true} {
			var delta int64
			var err error
			if seal {
				delta, err = sqliteInsertIntervalComponent(tx, m.ComputerID, item.Interval.ID, component)
			} else {
				delta, err = sqliteInsertQueuedOutbox(tx, m.ComputerID, item.Interval.ID, item.ID)
			}
			var native *sqliteio.Error
			if delta != 0 || !errors.As(err, &native) {
				t.Fatal("read-only SQL mutation lost native evidence/zero delta", err)
			}
			interopSafeError(t, err, f.directory)
		}
		interopRollback(t, tx)
		interopClose(t, c)
		sqQAReopen(t, f, m, snapshot)
	})
	t.Run("real-trigger-after-sibling-seal-requires-rollback", func(t *testing.T) {
		_, later := sqQALegacy(t, "synced")
		if later.Plan == nil || len(later.Plan.Parts) != 1 {
			t.Fatal("actual collision part missing")
		}
		part := later.Plan.Parts[0]
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		// Strict DDL constraint witness, deliberately rollback-only with its
		// deferred plan target absent. A prior successful seal is real staging.
		asQABindFixture(t, tx, "sync_parts", sqQAPartCols, sqQAPartValues(t, item.Interval.ID, part))
		delta, err := sqliteInsertIntervalComponent(tx, m.ComputerID, item.Interval.ID, component)
		if err != nil || delta != 154 {
			t.Fatal("sibling seal staging", err)
		}
		failed, err := sqliteInsertQueuedOutbox(tx, m.ComputerID, item.Interval.ID, part.ID)
		sqQAConstraint(t, err, 1811)
		if failed != 0 {
			t.Fatal("failed trigger root delta")
		}
		if seal, found, err := sqliteReadIntervalComponent(tx, m.ComputerID, item.Interval.ID); err != nil || !found || seal != component {
			t.Fatal("zero delta falsely implied sibling rollback", err)
		}
		if row, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, item.Interval.ID); err != nil || found || !reflect.DeepEqual(row, sqliteOutboxLocalRow{}) {
			t.Fatal("failed initial root partially acknowledged", err)
		}
		if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, m) {
			t.Fatal("row failure mutated metadata/nonce", err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
		sqQAReopen(t, f, m, snapshot)
	})
}
