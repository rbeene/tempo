//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Test-first SOURCE ONLY. Missing producers are prerequisites, not meaningful
// RED. These tests invoke actual local APIs and native fresh SQLite; no runtime
// gate, skipped test, expected failure, installed index or fake row producer.

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func TestSQLiteSyncPartsAttemptsLiteral28NineTenChargesAndCallerCommit(t *testing.T) {
	f, m, old, confirmed, actualAttempt, root := spQASeed(t, "synced", false, false)
	q := spQAQueued(confirmed)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	spQAHookPositive(t, tx)
	if got, found, err := sqliteReadSyncPartLocal(tx, m.ComputerID, q.IntervalID, 0); err != nil || found || !reflect.DeepEqual(got, sqliteSyncPartLocalRow{}) {
		t.Fatal("part absence", err)
	}
	if list, err := sqliteSyncPartsLocal(tx, m.ComputerID, q.IntervalID); err != nil || list == nil || len(list) != 0 {
		t.Fatal("allocated empty parts", err)
	}
	// This otherwise-valid queued control has EVERY confirmation field nil.
	if q.EntryID != nil || q.ReturnedHours != nil || q.RoundedHours != nil || q.ConfirmedDurationNS != nil || q.ProviderDeltaNS != nil || q.TotalResidualNS != nil {
		t.Fatal("queued no-confirmation control is not isolated")
	}
	dp, err := sqliteWriteSyncPartLocal(tx, m.ComputerID, nil, q)
	if err != nil || dp != spQAChargePart(q) {
		t.Fatal("part literal196 baseline plus nine required TEXT lengths", dp, err)
	}
	spQAReadPart(t, tx, m.ComputerID, q)
	if charge, err := sqliteSyncPartLocalCharge(q); err != nil || charge != dp {
		t.Fatal("independent part charge", err)
	}
	spQAKinds(t, tx, "sync_parts", sqQAPartCols, "interval_id=? AND ordinal=0", []sqliteio.Value{sqliteio.Text(q.IntervalID)}, []sqliteio.Kind{
		sqliteio.TextKind, sqliteio.IntegerKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.IntegerKind,
		sqliteio.IntegerKind, sqliteio.IntegerKind, sqliteio.TextKind, sqliteio.IntegerKind, sqliteio.IntegerKind, sqliteio.TextKind,
		sqliteio.TextKind, sqliteio.IntegerKind, sqliteio.IntegerKind, sqliteio.NullKind, sqliteio.NullKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind,
		sqliteio.NullKind, sqliteio.NullKind, sqliteio.NullKind, sqliteio.NullKind, sqliteio.NullKind, sqliteio.NullKind, sqliteio.NullKind, sqliteio.NullKind, sqliteio.NullKind})
	if got, err := sqliteSyncAttemptsLocal(tx, m.ComputerID, q.IntervalID, 0); err != nil || got == nil || len(got) != 0 {
		t.Fatal("allocated empty attempts", err)
	}
	claim := spQANewAttempt(q, 0, actualAttempt.Value.RequestID)
	da, err := sqliteAppendSyncAttempt(tx, m.ComputerID, claim)
	if err != nil || da != spQAChargeAttempt(claim) {
		t.Fatal("attempt literal97 plus five required TEXT lengths", da, err)
	}
	if charge, err := sqliteSyncAttemptLocalCharge(claim); err != nil || charge != da {
		t.Fatal("independent attempt charge", err)
	}
	spQAKinds(t, tx, "sync_attempts", sqQAAttemptCols, "interval_id=? AND part_ordinal=0 AND ordinal=0", []sqliteio.Value{sqliteio.Text(q.IntervalID)}, []sqliteio.Kind{sqliteio.TextKind, sqliteio.IntegerKind, sqliteio.IntegerKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.NullKind, sqliteio.NullKind})
	attempts, err := sqliteSyncAttemptsLocal(tx, m.ComputerID, q.IntervalID, 0)
	if err != nil || !reflect.DeepEqual(attempts, []sqliteSyncAttemptLocalRow{claim}) {
		t.Fatal("actual complete attempt list", err)
	}
	// Existing history does not make a locally valid queued scalar corrupt.
	// The complete graph, outside these eleven APIs, enforces queued/history.
	spQAReadPart(t, tx, m.ComputerID, q)
	confirmed = spQAClonePart(confirmed)
	confirmed.RoundedHours = spQAPtr("0.040")
	du, err := sqliteWriteSyncPartLocal(tx, m.ComputerID, &q, confirmed)
	if err != nil || du != spQAChargePart(confirmed)-dp {
		t.Fatal("actual nullable acknowledgement delta", du, err)
	}
	ack := spQACloneAttempt(claim)
	ack.Value.State, ack.Value.EntryID = "synced", spQAPtr(*confirmed.EntryID)
	dack, err := sqliteUpdateSyncAttempt(tx, m.ComputerID, claim, ack)
	if err != nil || dack != spQAChargeAttempt(ack)-da {
		t.Fatal("last actual attempt acknowledgement delta", err)
	}
	gotRoot, found, err := sqliteReadOutboxByIDLocal(tx, m.ComputerID, root.ID)
	if err != nil || !found || !reflect.DeepEqual(gotRoot, root) {
		t.Fatal("indexed UUID first-pair then interval-local reader", err)
	}
	if charge, err := sqliteOutboxLocalCharge(gotRoot); err != nil || charge != sqQACharge(root) {
		t.Fatal("root independent98 charge", err)
	}
	spQAKinds(t, tx, "outbox", "interval_id,id", "id=?", []sqliteio.Value{sqliteio.Text(root.ID)}, []sqliteio.Kind{sqliteio.TextKind, sqliteio.TextKind})
	spQAKinds(t, tx, "outbox", sqQAOutboxCols, "id=?", []sqliteio.Value{sqliteio.Text(root.ID)}, []sqliteio.Kind{sqliteio.TextKind, sqliteio.TextKind, sqliteio.BlobKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.NullKind, sqliteio.NullKind, sqliteio.NullKind, sqliteio.IntegerKind})
	nextRoot := sqQAClone(root)
	nextRoot.Revision, nextRoot.FailureCategory = bump(root.Revision), spQAPtr("")
	dr, err := sqliteUpdateOutboxLocal(tx, m.ComputerID, root, nextRoot)
	if err != nil || dr != 8 {
		t.Fatal("root present-empty optional exact8 delta", dr, err)
	}
	if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, m) {
		t.Fatal("eleven row APIs secretly changed meta", err)
	}
	next := metaQANext(t, m)
	next.Revision, next.LogicalBytes = bump(m.Revision), m.LogicalBytes+dp+da+du+dack+dr
	bgQAFixtureError(t, "one caller-owned metadata CAS", sqliteUpdateMeta(tx, m, next))
	snapshot, billed := spQAAudit(t, tx)
	if billed != next.LogicalBytes || reflect.DeepEqual(snapshot, old) {
		t.Fatal("literal rows differ from independent aggregate delta")
	}
	bgQAFixtureError(t, "actual final FK closure", tx.CheckForeignKeys())
	interopCommit(t, tx)
	interopClose(t, c)
	spQAReopen(t, f, next, snapshot)
}

func TestSQLiteSyncPartsAttemptsActualScalarsOwnedNullEmptyAndZeroAmounts(t *testing.T) {
	for _, phase := range []string{"submitting", "synced", "rejected", "unknown"} {
		t.Run(phase, func(t *testing.T) {
			f, m, snapshot, p, a, root := spQASeed(t, phase, true, true)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			got := spQAReadPart(t, tx, m.ComputerID, p)
			attempts, err := sqliteSyncAttemptsLocal(tx, m.ComputerID, p.IntervalID, p.Ordinal)
			if err != nil || !reflect.DeepEqual(attempts, []sqliteSyncAttemptLocalRow{a}) {
				t.Fatal("actual phase history", err)
			}
			gotRoot, found, err := sqliteReadOutboxByIDLocal(tx, m.ComputerID, root.ID)
			if err != nil || !found || !reflect.DeepEqual(gotRoot, root) {
				t.Fatal("actual phase indexed root", err)
			}
			if charge, err := sqliteSyncPartLocalCharge(p); err != nil || charge != spQAChargePart(p) {
				t.Fatal("literal actual phase part charge", err)
			}
			if charge, err := sqliteSyncAttemptLocalCharge(a); err != nil || charge != spQAChargeAttempt(a) {
				t.Fatal("literal actual phase attempt charge", err)
			}
			for _, target := range []*string{got.StartedTime, got.EndedTime, got.EntryID, got.FailureCategory, got.ReturnedHours, got.RoundedHours, got.ConfirmedDurationNS, got.ProviderDeltaNS, got.TotalResidualNS, attempts[0].Value.EntryID, attempts[0].Value.FailureCategory, gotRoot.EntryID, gotRoot.FailureCategory, gotRoot.RetryRequestID, gotRoot.RunRequestID} {
				if target != nil {
					*target = "caller-owned mutation"
				}
			}
			if got.Attachment != nil {
				got.Attachment.EntryID = "caller-owned attachment"
			}
			spQAReadPart(t, tx, m.ComputerID, p)
			if list, err := sqliteSyncAttemptsLocal(tx, m.ComputerID, p.IntervalID, p.Ordinal); err != nil || !reflect.DeepEqual(list, []sqliteSyncAttemptLocalRow{a}) {
				t.Fatal("caller changed retained attempt", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			spQAReopen(t, f, m, snapshot)
		})
	}
	f, m, snapshot, p, a, _ := spQASeed(t, "synced", true, true)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	// Local scalars preserve spellings, embedded NUL, calendar strings and
	// empty clock strings; complete calendar/marker checking belongs elsewhere.
	p.StartedTime, p.EndedTime = spQAPtr(""), spQAPtr("\x00雪")
	p.PlannedHours, p.ReturnedHours = "0.0400", spQAPtr("0.04000")
	p.RoundedHours = spQAPtr("0")
	p.Attachment = &SyncAttachment{RequestID: a.Value.RequestID, EntryID: *p.EntryID}
	interopDone(t, tx, "DELETE FROM sync_parts WHERE interval_id=?", sqliteio.Text(p.IntervalID))
	spQABindPart(t, tx, p)
	got := spQAReadPart(t, tx, m.ComputerID, p)
	got.Attachment.EntryID = "owned"
	*got.StartedTime, *got.EndedTime = "owned", "owned"
	spQAReadPart(t, tx, m.ComputerID, p)
	if charge, err := sqliteSyncPartLocalCharge(p); err != nil || charge != spQAChargePart(p) {
		t.Fatal("nullable attachment/text amounts billing", err)
	}
	// Zero returned amount is valid mismatch evidence; negative provider and
	// total residuals remain signed when a larger amount is acknowledged.
	for _, tc := range []struct{ hours, confirmed, delta, residual string }{{"0", "0", p.PlannedDurationNS, p.DurationNS}, {"0.05", "180000000000", "-36000000000", "-42518000000"}} {
		x := spQAClonePart(p)
		x.State, x.FailureCategory = "needs_attention", spQAPtr("duration_mismatch")
		x.ReturnedHours, x.ConfirmedDurationNS = spQAPtr(tc.hours), spQAPtr(tc.confirmed)
		x.ProviderDeltaNS, x.TotalResidualNS = spQAPtr(tc.delta), spQAPtr(tc.residual)
		delta, err := sqliteWriteSyncPartLocal(tx, m.ComputerID, &p, x)
		if err != nil || delta != spQAChargePart(x)-spQAChargePart(p) {
			t.Fatal("zero/negative canonical amount control", tc, err)
		}
		spQAReadPart(t, tx, m.ComputerID, x)
		back, err := sqliteWriteSyncPartLocal(tx, m.ComputerID, &x, p)
		if err != nil || back != -delta {
			t.Fatal("amount control restore", err)
		}
	}
	interopRollback(t, tx)
	interopClose(t, c)
	spQAReopen(t, f, m, snapshot)
}

func TestSQLiteSyncPartsAttemptsRawValidationBeforeSQLAndIsolatedRoundedHours(t *testing.T) {
	_, _, _, p, a, root := spQASeed(t, "synced", false, false)
	q := spQAQueued(p)
	if charge, err := sqliteSyncPartLocalCharge(q); err != nil || charge != spQAChargePart(q) {
		t.Fatal("otherwise-valid nil confirmation charge control", err)
	}
	bad := spQAClonePart(q)
	bad.RoundedHours = spQAPtr("0.04") // Isolated single-field violation.
	if !reflect.DeepEqual(spQAQueued(bad), q) {
		t.Fatal("rounded-hours negative changed another field")
	}
	delta, err := sqliteSyncPartLocalCharge(bad)
	spQADeltaValidation(t, delta, err)
	delta, err = sqliteWriteSyncPartLocal(nil, interopComputer, nil, bad)
	spQADeltaValidation(t, delta, err)
	delta, err = sqliteWriteSyncPartLocal(nil, interopComputer, &q, bad)
	spQADeltaValidation(t, delta, err)
	for _, mutate := range []struct {
		name   string
		change func(*sqliteSyncPartLocalRow)
	}{
		{"owner", func(x *sqliteSyncPartLocalRow) { x.IntervalID = "invalid" }},
		{"ordinal-negative", func(x *sqliteSyncPartLocalRow) { x.Ordinal = -1 }}, {"ordinal100", func(x *sqliteSyncPartLocalRow) { x.Ordinal = 100 }},
		{"id", func(x *sqliteSyncPartLocalRow) { x.ID = strings.ToUpper(x.ID) + "A" }},
		{"duration-leading-zero", func(x *sqliteSyncPartLocalRow) { x.DurationNS = "0" + x.DurationNS }},
		{"duration-plus", func(x *sqliteSyncPartLocalRow) { x.DurationNS = "+" + x.DurationNS }},
		{"duration-zero", func(x *sqliteSyncPartLocalRow) { x.DurationNS = "0" }},
		{"duration-overflow", func(x *sqliteSyncPartLocalRow) { x.DurationNS = "9223372036854775808" }},
		{"duration-range", func(x *sqliteSyncPartLocalRow) { x.DurationNS = "1" }},
		{"planned-negative", func(x *sqliteSyncPartLocalRow) { x.PlannedDurationNS = "-1" }},
		{"planned-zero", func(x *sqliteSyncPartLocalRow) { x.PlannedDurationNS = "0" }},
		{"residual-noncanonical", func(x *sqliteSyncPartLocalRow) { x.PlannedResidualNS = "-0" }},
		{"residual-arithmetic", func(x *sqliteSyncPartLocalRow) { x.PlannedResidualNS = "0" }},
		{"hours-malformed", func(x *sqliteSyncPartLocalRow) { x.PlannedHours = "NaN" }},
		{"hours-exponent", func(x *sqliteSyncPartLocalRow) { x.PlannedHours = "4e-2" }},
		{"hours-negative", func(x *sqliteSyncPartLocalRow) { x.PlannedHours = "-0.04" }},
		{"hours-too-long", func(x *sqliteSyncPartLocalRow) { x.PlannedHours = "0." + strings.Repeat("0", 129) }},
		{"hours-disagree", func(x *sqliteSyncPartLocalRow) { x.PlannedHours = "0.05" }},
		{"start-nonutc", func(x *sqliteSyncPartLocalRow) { x.Start = x.Start.In(time.FixedZone("named UTC", 0)) }},
		{"end-nonutc", func(x *sqliteSyncPartLocalRow) { x.End = x.End.In(time.FixedZone("named UTC", 0)) }},
		{"empty-range", func(x *sqliteSyncPartLocalRow) { x.End = x.Start }},
		{"unencodable-time", func(x *sqliteSyncPartLocalRow) {
			x.Start = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			x.End = x.Start.Add(time.Duration(137482000000))
		}},
		{"correlation", func(x *sqliteSyncPartLocalRow) { x.Correlation = "tempo:" + x.ID }},
		{"state", func(x *sqliteSyncPartLocalRow) { x.State = "complete" }},
		{"unconfirmed-entry", func(x *sqliteSyncPartLocalRow) { x.EntryID = spQAPtr("1") }},
		{"unconfirmed-returned", func(x *sqliteSyncPartLocalRow) { x.ReturnedHours = spQAPtr("0.04") }},
		{"unconfirmed-delta", func(x *sqliteSyncPartLocalRow) { x.ProviderDeltaNS = spQAPtr("0") }},
		{"unconfirmed-residual", func(x *sqliteSyncPartLocalRow) { x.TotalResidualNS = spQAPtr("0") }},
		{"unconfirmed-synced", func(x *sqliteSyncPartLocalRow) { x.State = "synced" }},
		{"unconfirmed-attention", func(x *sqliteSyncPartLocalRow) { x.State = "needs_attention" }},
		{"attachment-request", func(x *sqliteSyncPartLocalRow) { x.Attachment = &SyncAttachment{RequestID: "invalid", EntryID: "1"} }},
		{"attachment-entry", func(x *sqliteSyncPartLocalRow) {
			x.Attachment = &SyncAttachment{RequestID: a.Value.RequestID, EntryID: ""}
		}},
	} {
		t.Run("raw-part/"+mutate.name, func(t *testing.T) {
			x := spQAClonePart(q)
			mutate.change(&x)
			delta, err := sqliteSyncPartLocalCharge(x)
			spQADeltaValidation(t, delta, err)
			delta, err = sqliteWriteSyncPartLocal(nil, interopComputer, nil, x)
			spQADeltaValidation(t, delta, err)
		})
	}
	// Every raw TEXT member, including all optional TEXTs and attachment
	// fields, rejects malformed UTF8 without JSON replacement or SQL access.
	for _, field := range []string{"IntervalID", "ID", "SpentDate", "PlannedHours", "Correlation", "Notes", "State", "StartedTime", "EndedTime", "EntryID", "FailureCategory", "ReturnedHours", "RoundedHours"} {
		t.Run("raw-part-utf8/"+field, func(t *testing.T) {
			x := spQAClonePart(p)
			target := reflect.ValueOf(&x).Elem().FieldByName(field)
			badText := string([]byte{0xff})
			if target.Kind() == reflect.String {
				target.SetString(badText)
			} else {
				target.Set(reflect.ValueOf(spQAPtr(badText)))
			}
			delta, err := sqliteSyncPartLocalCharge(x)
			spQADeltaValidation(t, delta, err)
			delta, err = sqliteWriteSyncPartLocal(nil, interopComputer, nil, x)
			spQADeltaValidation(t, delta, err)
		})
	}
	for _, field := range []string{"RequestID", "ID", "Number", "State", "EntryID", "FailureCategory"} {
		t.Run("raw-attempt-utf8/"+field, func(t *testing.T) {
			x := spQACloneAttempt(a)
			target := reflect.ValueOf(&x.Value).Elem().FieldByName(field)
			if target.Kind() == reflect.String {
				target.SetString(string([]byte{0xff}))
			} else {
				target.Set(reflect.ValueOf(spQAPtr(string([]byte{0xff}))))
			}
			delta, err := sqliteSyncAttemptLocalCharge(x)
			spQADeltaValidation(t, delta, err)
			delta, err = sqliteAppendSyncAttempt(nil, interopComputer, x)
			spQADeltaValidation(t, delta, err)
		})
	}
	for _, field := range []string{"IntervalID", "ID", "Revision", "State", "Correlation", "EntryID", "FailureCategory", "RetryRequestID", "RunRequestID"} {
		t.Run("raw-root-utf8/"+field, func(t *testing.T) {
			x := sqQAClone(root)
			target := reflect.ValueOf(&x).Elem().FieldByName(field)
			if target.Kind() == reflect.String {
				target.SetString(string([]byte{0xff}))
			} else {
				target.Set(reflect.ValueOf(spQAPtr(string([]byte{0xff}))))
			}
			delta, err := sqliteUpdateOutboxLocal(nil, interopComputer, root, x)
			spQADeltaValidation(t, delta, err)
		})
	}
}

func TestSQLiteSyncPartsAttemptsSelectedKindsUTF8TimeAmountsAndAttachment(t *testing.T) {
	f, m, snapshot, p, a, root := spQASeed(t, "synced", true, true)
	for _, table := range []struct{ name, cols, predicate string }{{"sync_parts", sqQAPartCols, "interval_id=?"}, {"sync_attempts", sqQAAttemptCols, "interval_id=?"}, {"outbox", sqQAOutboxCols, "id=?"}} {
		for _, column := range strings.Split(table.cols, ",") {
			if column == "interval_id" || table.name == "sync_attempts" && column == "part_ordinal" || table.name == "outbox" && column == "id" {
				continue
			} // Exact key equality cannot select a differently typed key.
			t.Run(table.name+"/kind/"+column, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				asQAShadow(t, tx, table.name, table.cols)
				value := sqliteio.Blob([]byte("wrong native kind"))
				if column == "revision" {
					value = sqliteio.Text("1")
				}
				key := p.IntervalID
				if table.name == "outbox" {
					key = root.ID
				}
				interopDone(t, tx, "UPDATE "+table.name+" SET "+column+"=? WHERE "+table.predicate, value, sqliteio.Text(key))
				switch table.name {
				case "sync_parts":
					if column != "ordinal" {
						row, found, err := sqliteReadSyncPartLocal(tx, m.ComputerID, p.IntervalID, 0)
						spQAPartZero(t, row, found, err)
					}
					list, err := sqliteSyncPartsLocal(tx, m.ComputerID, p.IntervalID)
					bgQACorrupt(t, err)
					if list != nil {
						t.Fatal("part list kind error leaked output")
					}
				case "sync_attempts":
					list, err := sqliteSyncAttemptsLocal(tx, m.ComputerID, p.IntervalID, 0)
					spQAAttemptsZero(t, list, err)
				case "outbox":
					row, found, err := sqliteReadOutboxByIDLocal(tx, m.ComputerID, root.ID)
					bgQACorrupt(t, err)
					if found || !reflect.DeepEqual(row, sqliteOutboxLocalRow{}) {
						t.Fatal("indexed root kind error leaked output")
					}
				}
				interopRollback(t, tx)
				interopClose(t, c)
			})
		}
	}
	for _, table := range []struct{ name, cols, textCols string }{
		{"sync_parts", sqQAPartCols, "id,spent_date,start_json,end_json,planned_hours,started_time,ended_time,correlation,notes,state,entry_id,failure_category,returned_hours,rounded_hours,attachment_request_id,attachment_entry_id"},
		{"sync_attempts", sqQAAttemptCols, "request_id,id,number,state,entry_id,failure_category"},
		{"outbox", sqQAOutboxCols, "id,state,correlation,entry_id,failure_category,retry_request_id,run_request_id"},
	} {
		for _, column := range strings.Split(table.textCols, ",") {
			t.Run(table.name+"/utf8/"+column, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				asQAShadow(t, tx, table.name, table.cols)
				interopDone(t, tx, "UPDATE "+table.name+" SET "+column+"=? WHERE interval_id=?", sqliteio.Text(string([]byte{0xff})), sqliteio.Text(p.IntervalID))
				if table.name == "sync_parts" {
					row, found, err := sqliteReadSyncPartLocal(tx, m.ComputerID, p.IntervalID, 0)
					spQAPartZero(t, row, found, err)
				}
				if table.name == "sync_attempts" {
					list, err := sqliteSyncAttemptsLocal(tx, m.ComputerID, p.IntervalID, 0)
					spQAAttemptsZero(t, list, err)
				}
				if table.name == "outbox" {
					// An invalid selected root ID no longer matches the indexed UUID;
					// it is selected by its unchanged interval-keyed local dependency.
					row, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, p.IntervalID)
					bgQACorrupt(t, err)
					if found || !reflect.DeepEqual(row, sqliteOutboxLocalRow{}) {
						t.Fatal("root UTF8 leaked output")
					}
				}
				interopRollback(t, tx)
				interopClose(t, c)
			})
		}
	}
	for _, axis := range []struct {
		name, column string
		value        sqliteio.Value
	}{
		{"start-second-disagree", "start_sec", sqliteio.Integer(p.Start.Unix() + 1)},
		{"start-nanosecond", "start_nsec", sqliteio.Integer(1000000000)},
		{"end-second-disagree", "end_sec", sqliteio.Integer(p.End.Unix() + 1)},
		{"end-nanosecond", "end_nsec", sqliteio.Integer(-1)},
		{"start-noncanonical-json", "start_json", sqliteio.Text(strings.Replace(spQAJSONTime(p.Start), "Z", "+00:00", 1))},
		{"end-malformed-json", "end_json", sqliteio.Text("not JSON")},
		{"duration-disagrees", "duration_ns", sqliteio.Integer(1)},
		{"planned-nonpositive", "planned_duration_ns", sqliteio.Integer(0)},
		{"planned-hours-disagree", "planned_hours", sqliteio.Text("0.05")},
		{"planned-residual-disagree", "planned_residual_ns", sqliteio.Integer(0)},
		{"returned-malformed", "returned_hours", sqliteio.Text("NaN")},
		{"rounded-malformed", "rounded_hours", sqliteio.Text("NaN")},
		{"confirmed-null-group", "confirmed_duration_ns", sqliteio.Null()},
		{"confirmed-disagree", "confirmed_duration_ns", sqliteio.Integer(0)},
		{"provider-delta-disagree", "provider_delta_ns", sqliteio.Integer(1)},
		{"total-residual-disagree", "total_residual_ns", sqliteio.Integer(1)},
		{"entry-empty", "entry_id", sqliteio.Text("")},
		{"attachment-only-request", "attachment_request_id", sqliteio.Text(a.Value.RequestID)},
		{"attachment-only-entry", "attachment_entry_id", sqliteio.Text("1")},
	} {
		t.Run("stored-part/"+axis.name, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			asQAShadow(t, tx, "sync_parts", sqQAPartCols)
			interopDone(t, tx, "UPDATE sync_parts SET "+axis.column+"=? WHERE interval_id=?", axis.value, sqliteio.Text(p.IntervalID))
			row, found, err := sqliteReadSyncPartLocal(tx, m.ComputerID, p.IntervalID, 0)
			spQAPartZero(t, row, found, err)
			list, err := sqliteSyncPartsLocal(tx, m.ComputerID, p.IntervalID)
			bgQACorrupt(t, err)
			if list != nil {
				t.Fatal("stored amount list leaked output")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	// Exact single-field RoundedHours negative also exercises the reader.
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	interopDone(t, tx, "DELETE FROM sync_parts WHERE interval_id=?", sqliteio.Text(p.IntervalID))
	q := spQAQueued(p)
	spQABindPart(t, tx, q)
	spQAReadPart(t, tx, m.ComputerID, q)
	interopDone(t, tx, "UPDATE sync_parts SET rounded_hours=? WHERE interval_id=?", sqliteio.Text("0.04"), sqliteio.Text(p.IntervalID))
	row, found, err := sqliteReadSyncPartLocal(tx, m.ComputerID, p.IntervalID, 0)
	spQAPartZero(t, row, found, err)
	interopRollback(t, tx)
	interopClose(t, c)
	spQAReopen(t, f, m, snapshot)
}

func TestSQLiteSyncPartsAttemptsFull54FifteenNineteenCASAndAllBeforeColumns(t *testing.T) {
	f, m, snapshot, p, a, root := spQASeed(t, "synced", true, true)
	for _, family := range []struct{ table, cols, pk, sets string }{
		{"sync_parts", sqQAPartCols, "interval_id,ordinal", "id,spent_date,duration_ns,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,planned_hours,planned_duration_ns,planned_residual_ns,started_time,ended_time,correlation,notes,state,entry_id,failure_category,returned_hours,rounded_hours,confirmed_duration_ns,provider_delta_ns,total_residual_ns,attachment_request_id,attachment_entry_id"},
		{"sync_attempts", sqQAAttemptCols, "interval_id,part_ordinal,ordinal", "request_id,id,number,state,entry_id,failure_category"},
		{"outbox", sqQAOutboxCols, "interval_id", "id,revision,state,correlation,entry_id,failure_category,retry_request_id,run_request_id,plan_present"},
	} {
		afterP := spQAClonePart(p)
		afterP.RoundedHours = spQAPtr("0.040")
		afterA := spQACloneAttempt(a)
		afterA.Value.FailureCategory = spQAPtr("")
		afterRoot := sqQAClone(root)
		afterRoot.FailureCategory = spQAPtr("")
		mutate := func(tx *sqliteio.Tx) (int64, error) {
			switch family.table {
			case "sync_parts":
				return sqliteWriteSyncPartLocal(tx, m.ComputerID, &p, afterP)
			case "sync_attempts":
				return sqliteUpdateSyncAttempt(tx, m.ComputerID, a, afterA)
			default:
				return sqliteUpdateOutboxLocal(tx, m.ComputerID, root, afterRoot)
			}
		}
		for _, column := range strings.Split(family.cols, ",") {
			t.Run(family.table+"/full-before/"+column, func(t *testing.T) {
				// Each stale axis has its own actual successful all-before control.
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				delta, err := mutate(tx)
				if err != nil {
					t.Fatal("positive full-CAS before isolated stale axis", err)
				}
				want := int64(8)
				if family.table == "sync_parts" {
					want = spQAChargePart(afterP) - spQAChargePart(p)
				}
				if delta != want {
					t.Fatal("positive full-CAS independent delta", delta, want)
				}
				interopRollback(t, tx)
				interopClose(t, c)
				c, tx = interopOpen(t, f, false, sqliteio.Write)
				asQAShadow(t, tx, family.table, family.cols)
				// Alter the STORED before column, not the caller's valid before.
				// Relaxation permits every old NULL/type/time-byte axis separately.
				query := "UPDATE " + family.table + " SET " + column + "=COALESCE(" + column + ",'')||'x' WHERE interval_id=?"
				if strings.Contains(",ordinal,part_ordinal,duration_ns,start_sec,start_nsec,end_sec,end_nsec,planned_duration_ns,planned_residual_ns,confirmed_duration_ns,provider_delta_ns,total_residual_ns,plan_present,", ","+column+",") {
					query = "UPDATE " + family.table + " SET " + column + "=COALESCE(" + column + ",0)+1 WHERE interval_id=?"
				}
				interopDone(t, tx, query, sqliteio.Text(p.IntervalID))
				delta, err = mutate(tx)
				spQADeltaCorrupt(t, delta, err)
				interopRollback(t, tx)
				interopClose(t, c)
			})
		}
		t.Run(family.table+"/repeated-immutable-and-every-nonPK-SET", func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			// SQLite UPDATE OF proves actual SET inclusion even for unchanged
			// immutable columns. Parameter count itself is checked by native
			// Prepare; no nonexistent bind-count hook field is asserted.
			spQARequireSetColumns(t, tx, family.table, family.sets, func() (int64, error) { return mutate(tx) })
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	spQAReopen(t, f, m, snapshot)
}

func TestSQLiteSyncPartsAttemptsImmutableCoordinatesAndOriginsValidateBeforeSQL(t *testing.T) {
	_, _, _, p, a, root := spQASeed(t, "synced", true, true)
	for _, field := range []string{"IntervalID", "Ordinal", "ID", "SpentDate", "DurationNS", "Start", "End", "PlannedHours", "PlannedDurationNS", "PlannedResidualNS", "StartedTime", "EndedTime", "Correlation", "Notes"} {
		t.Run("part/"+field, func(t *testing.T) {
			x := spQAClonePart(p)
			switch field {
			case "IntervalID":
				x.IntervalID = sqQAOtherInterval
			case "Ordinal":
				x.Ordinal = 1
			case "ID":
				x.ID, x.Correlation = spQAID(3000), "tempo:v1:"+spQAID(3000)
			case "SpentDate":
				x.SpentDate = "locally arbitrary date"
			case "DurationNS":
				x.DurationNS = "1" // Raw scalar invalidity is still preSQL.
			case "Start":
				x.Start = x.Start.Add(time.Nanosecond)
			case "End":
				x.End = x.End.Add(time.Nanosecond)
			case "PlannedHours":
				x.PlannedHours = "0.0400" // Equivalent rational, different frozen spelling.
			case "PlannedDurationNS":
				x.PlannedDurationNS = "1"
			case "PlannedResidualNS":
				x.PlannedResidualNS = "0"
			case "StartedTime":
				x.StartedTime = spQAPtr("")
			case "EndedTime":
				x.EndedTime = spQAPtr("")
			case "Correlation":
				x.Correlation = "different"
			case "Notes":
				x.Notes += "\x00雪"
			}
			delta, err := sqliteWriteSyncPartLocal(nil, interopComputer, &p, x)
			spQADeltaValidation(t, delta, err)
		})
	}
	for _, field := range []string{"IntervalID", "PartOrdinal", "Ordinal", "RequestID", "ID", "Number"} {
		t.Run("attempt/"+field, func(t *testing.T) {
			x := spQACloneAttempt(a)
			switch field {
			case "IntervalID":
				x.IntervalID = sqQAOtherInterval
			case "PartOrdinal":
				x.PartOrdinal = 1
			case "Ordinal":
				x.Ordinal, x.Value.Number = 1, "2"
			case "RequestID":
				x.Value.RequestID = spQAID(3001)
			case "ID":
				x.Value.ID = spQAID(3002)
			case "Number":
				x.Value.Number = "01"
			}
			delta, err := sqliteUpdateSyncAttempt(nil, interopComputer, a, x)
			spQADeltaValidation(t, delta, err)
		})
	}
	for _, field := range []string{"IntervalID", "ID", "Correlation"} {
		t.Run("root/"+field, func(t *testing.T) {
			x := sqQAClone(root)
			switch field {
			case "IntervalID":
				x.IntervalID, x.Correlation = sqQAOtherInterval, "tempo:"+sqQAOtherInterval
			case "ID":
				x.ID = spQAID(3003)
			case "Correlation":
				x.Correlation = "different"
			}
			delta, err := sqliteUpdateOutboxLocal(nil, interopComputer, root, x)
			spQADeltaValidation(t, delta, err)
		})
	}
}

func TestSQLiteSyncPartsAttemptsLocalParentsIndexedRootPairAndAllocatedAbsence(t *testing.T) {
	f, m, snapshot, p, _, root := spQASeed(t, "synced", true, false)
	for _, axis := range []string{"missing-plan", "corrupt-plan", "missing-root", "corrupt-root", "missing-interval", "foreign-interval", "corrupt-interval"} {
		t.Run(axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			spQAReadPart(t, tx, m.ComputerID, p)
			switch axis {
			case "missing-plan":
				interopDone(t, tx, "DELETE FROM sync_plans WHERE interval_id=?", sqliteio.Text(p.IntervalID))
			case "corrupt-plan":
				interopDone(t, tx, "UPDATE sync_plans SET config_declared_at_json=? WHERE interval_id=?", sqliteio.Text("bad JSON time"), sqliteio.Text(p.IntervalID))
			case "missing-root":
				interopDone(t, tx, "DELETE FROM outbox WHERE interval_id=?", sqliteio.Text(p.IntervalID))
			case "corrupt-root":
				interopDone(t, tx, "UPDATE outbox SET failure_category=? WHERE interval_id=?", sqliteio.Text(string([]byte{0xff})), sqliteio.Text(p.IntervalID))
			case "missing-interval":
				interopDone(t, tx, "DELETE FROM intervals WHERE interval_id=?", sqliteio.Text(p.IntervalID))
			case "foreign-interval":
				owned, found, err := sqliteReadIntervalLocal(tx, m.ComputerID, p.IntervalID)
				if err != nil || !found {
					t.Fatal("valid owned interval before foreign scope control", err)
				}
				foreign := owned
				foreign.ComputerID = spQAID(4000)
				interopDone(t, tx, "UPDATE intervals SET computer_id=?,group_order=? WHERE interval_id=?", sqliteio.Text(foreign.ComputerID), sqliteio.Text(ueQAGroup(t, foreign.ComputerID, foreign.Attribution)), sqliteio.Text(p.IntervalID))
				got, found, err := sqliteReadIntervalLocal(tx, foreign.ComputerID, p.IntervalID)
				if err != nil || !found || !reflect.DeepEqual(got, foreign) {
					t.Fatal("otherwise-valid foreign interval scope positive", err)
				}
			case "corrupt-interval":
				interopDone(t, tx, "UPDATE intervals SET end_json=? WHERE interval_id=?", sqliteio.Text("bad JSON time"), sqliteio.Text(p.IntervalID))
			}
			row, found, err := sqliteReadSyncPartLocal(tx, m.ComputerID, p.IntervalID, 0)
			spQAPartZero(t, row, found, err)
			list, err := sqliteSyncPartsLocal(tx, m.ComputerID, p.IntervalID)
			bgQACorrupt(t, err)
			if list != nil {
				t.Fatal("local parent list leaked partial output")
			}
			// Empty attempts still check their real part parent rather than
			// certifying an orphan scope as a successful empty history.
			attempts, err := sqliteSyncAttemptsLocal(tx, m.ComputerID, p.IntervalID, 0)
			spQAAttemptsZero(t, attempts, err)
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	for _, axis := range []string{"interval-pair-kind", "interval-pair-utf8", "interval-pair-invalid", "duplicate-index-pair", "missing-owned-interval"} {
		t.Run("indexed-root/"+axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if got, found, err := sqliteReadOutboxByIDLocal(tx, m.ComputerID, root.ID); err != nil || !found || !reflect.DeepEqual(got, root) {
				t.Fatal("indexed pair positive", err)
			}
			asQAShadow(t, tx, "outbox", sqQAOutboxCols)
			switch axis {
			case "interval-pair-kind":
				interopDone(t, tx, "UPDATE outbox SET interval_id=? WHERE id=?", sqliteio.Blob([]byte(p.IntervalID)), sqliteio.Text(root.ID))
			case "interval-pair-utf8":
				interopDone(t, tx, "UPDATE outbox SET interval_id=? WHERE id=?", sqliteio.Text(string([]byte{0xff})), sqliteio.Text(root.ID))
			case "interval-pair-invalid":
				interopDone(t, tx, "UPDATE outbox SET interval_id='' WHERE id=?", sqliteio.Text(root.ID))
			case "duplicate-index-pair":
				interopDone(t, tx, "INSERT INTO outbox SELECT * FROM outbox WHERE id=?", sqliteio.Text(root.ID))
			case "missing-owned-interval":
				interopDone(t, tx, "DELETE FROM intervals WHERE interval_id=?", sqliteio.Text(p.IntervalID))
			}
			got, found, err := sqliteReadOutboxByIDLocal(tx, m.ComputerID, root.ID)
			bgQACorrupt(t, err)
			if found || !reflect.DeepEqual(got, sqliteOutboxLocalRow{}) {
				t.Fatal("indexed literal pair/dependent read leaked output")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	// PlanPresent is a staging flag; the real local header still exists.
	interopDone(t, tx, "UPDATE outbox SET plan_present=0 WHERE interval_id=?", sqliteio.Text(p.IntervalID))
	spQAReadPart(t, tx, m.ComputerID, p)
	if rows, err := sqliteSyncPartsLocal(tx, m.ComputerID, p.IntervalID); err != nil || !reflect.DeepEqual(rows, []sqliteSyncPartLocalRow{p}) {
		t.Fatal("local scalar invented full-graph flag equality", err)
	}
	got, found, err := sqliteReadOutboxByIDLocal(tx, m.ComputerID, sqQAMissing)
	if err != nil || found || !reflect.DeepEqual(got, sqliteOutboxLocalRow{}) {
		t.Fatal("indexed root missing is absence", err)
	}
	gotP, found, err := sqliteReadSyncPartLocal(tx, m.ComputerID, p.IntervalID, 1)
	if err != nil || found || !reflect.DeepEqual(gotP, sqliteSyncPartLocalRow{}) {
		t.Fatal("missing selected part is absence", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	spQAReopen(t, f, m, snapshot)
}

func TestSQLiteSyncPartsAttemptsConsecutiveLastOnlyListsAndExhaustion(t *testing.T) {
	f, m, snapshot, p, actual, _ := spQASeed(t, "synced", false, false)
	q := spQAQueued(p)
	t.Run("real-consecutive-parts-and-part100", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		want := []sqliteSyncPartLocalRow{}
		for ordinal := int64(0); ordinal < 100; ordinal++ {
			x := spQANextPart(q, ordinal)
			delta, err := sqliteWriteSyncPartLocal(tx, m.ComputerID, nil, x)
			if err != nil || delta != spQAChargePart(x) {
				t.Fatal("consecutive actual ordinal append", ordinal, err)
			}
			want = append(want, x)
		}
		list, err := sqliteSyncPartsLocal(tx, m.ComputerID, p.IntervalID)
		if err != nil || !reflect.DeepEqual(list, want) {
			t.Fatal("complete ordered100 part list", err)
		}
		x := spQANextPart(q, 100)
		delta, err := sqliteWriteSyncPartLocal(tx, m.ComputerID, nil, x)
		spQADeltaValidation(t, delta, err)
		if interopCount(t, tx, "SELECT count(*) FROM sync_parts") != 100 {
			t.Fatal("part100 affected retained history")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
	t.Run("part-last-only-is-not-gap-audit", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		spQABindPart(t, tx, spQANextPart(q, 0))
		spQABindPart(t, tx, spQANextPart(q, 2))
		x := spQANextPart(q, 3)
		delta, err := sqliteWriteSyncPartLocal(tx, m.ComputerID, nil, x)
		if err != nil || delta != spQAChargePart(x) {
			t.Fatal("actual last2 permits3 without scanning earlier gap", err)
		}
		list, err := sqliteSyncPartsLocal(tx, m.ComputerID, p.IntervalID)
		bgQACorrupt(t, err)
		if list != nil {
			t.Fatal("full part list silently skipped gap1")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
	t.Run("part-last-scalar-must-be-complete", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		spQABindPart(t, tx, q)
		interopDone(t, tx, "UPDATE sync_parts SET notes=? WHERE interval_id=?", sqliteio.Text(string([]byte{0xff})), sqliteio.Text(q.IntervalID))
		delta, err := sqliteWriteSyncPartLocal(tx, m.ComputerID, nil, spQANextPart(q, 1))
		spQADeltaCorrupt(t, delta, err)
		interopRollback(t, tx)
		interopClose(t, c)
	})
	t.Run("actual-attempt-list-and-last-only", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		spQABindPart(t, tx, q)
		first := spQANewAttempt(q, 0, actual.Value.RequestID)
		delta, err := sqliteAppendSyncAttempt(tx, m.ComputerID, first)
		if err != nil || delta != spQAChargeAttempt(first) {
			t.Fatal("first real submitting append", err)
		}
		rejected := spQACloneAttempt(first)
		rejected.Value.State, rejected.Value.FailureCategory = "rejected", spQAPtr("validation")
		delta, err = sqliteUpdateSyncAttempt(tx, m.ComputerID, first, rejected)
		if err != nil || delta != spQAChargeAttempt(rejected)-spQAChargeAttempt(first) {
			t.Fatal("actual last rejected outcome", err)
		}
		second := spQANewAttempt(q, 1, actual.Value.RequestID)
		delta, err = sqliteAppendSyncAttempt(tx, m.ComputerID, second)
		if err != nil || delta != spQAChargeAttempt(second) {
			t.Fatal("next2 real submitting append", err)
		}
		list, err := sqliteSyncAttemptsLocal(tx, m.ComputerID, q.IntervalID, 0)
		if err != nil || !reflect.DeepEqual(list, []sqliteSyncAttemptLocalRow{rejected, second}) {
			t.Fatal("ordered actual attempt list", err)
		}
		changedOld := spQACloneAttempt(rejected)
		changedOld.Value.State = "unknown"
		delta, err = sqliteUpdateSyncAttempt(tx, m.ComputerID, rejected, changedOld)
		spQADeltaCorrupt(t, delta, err)
		if got, err := sqliteSyncAttemptsLocal(tx, m.ComputerID, q.IntervalID, 0); err != nil || !reflect.DeepEqual(got, list) {
			t.Fatal("earlier rejected history changed", err)
		}
		interopDone(t, tx, "DELETE FROM sync_attempts WHERE interval_id=? AND ordinal=1", sqliteio.Text(q.IntervalID)) // Fixture-only gap, no production delete API.
		third := spQANewAttempt(q, 2, actual.Value.RequestID)
		third.Value.State = "rejected"
		spQABindAttempt(t, tx, third)
		fourth := spQANewAttempt(q, 3, actual.Value.RequestID)
		delta, err = sqliteAppendSyncAttempt(tx, m.ComputerID, fourth)
		if err != nil || delta != spQAChargeAttempt(fourth) {
			t.Fatal("actual last2 permits3 without earlier-gap scan", err)
		}
		list, err = sqliteSyncAttemptsLocal(tx, m.ComputerID, q.IntervalID, 0)
		spQAAttemptsZero(t, list, err)
		interopRollback(t, tx)
		interopClose(t, c)
	})
	for _, axis := range []string{"negative", "part100", "max-ordinal", "number", "queued", "synced", "rejected", "unknown", "needs_attention", "entry", "failure"} {
		t.Run("append-admission/"+axis, func(t *testing.T) {
			x := spQANewAttempt(q, 0, actual.Value.RequestID)
			switch axis {
			case "negative":
				x.Ordinal, x.Value.Number = -1, "0"
			case "part100":
				x.PartOrdinal = 100
			case "max-ordinal":
				x.Ordinal, x.Value.Number = math.MaxInt64, "9223372036854775808"
			case "number":
				x.Value.Number = "01"
			case "entry":
				x.Value.EntryID = spQAPtr("1")
			case "failure":
				x.Value.FailureCategory = spQAPtr("")
			default:
				x.Value.State = axis
			}
			delta, err := sqliteAppendSyncAttempt(nil, interopComputer, x)
			spQADeltaValidation(t, delta, err)
		})
	}
	t.Run("actual-last-exhaustion-and-impossible-stored-ordinal", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		spQABindPart(t, tx, q)
		last := spQANewAttempt(q, math.MaxInt64-2, actual.Value.RequestID)
		last.Value.State, last.Value.ID = "rejected", spQAID(7100)
		spQABindAttempt(t, tx, last)
		x := spQANewAttempt(q, math.MaxInt64-1, actual.Value.RequestID)
		x.Value.ID = spQAID(7101)
		delta, err := sqliteAppendSyncAttempt(tx, m.ComputerID, x)
		if err != nil || delta != spQAChargeAttempt(x) || x.Value.Number != "9223372036854775807" {
			t.Fatal("last representable ordinal/Number exact TEXT", err)
		}
		exhausted := x
		exhausted.Ordinal, exhausted.Value.Number, exhausted.Value.ID = math.MaxInt64, "9223372036854775808", spQAID(7000)
		delta, err = sqliteAppendSyncAttempt(tx, m.ComputerID, exhausted)
		spQADeltaValidation(t, delta, err)
		asQAShadow(t, tx, "sync_attempts", sqQAAttemptCols)
		interopDone(t, tx, "UPDATE sync_attempts SET ordinal=?,number=? WHERE ordinal=?", sqliteio.Integer(math.MaxInt64), sqliteio.Text("9223372036854775808"), sqliteio.Integer(math.MaxInt64-1))
		// A raw-valid candidate forces the last-only append decoder to inspect
		// the impossible selected maximum, independently of earlier list gaps.
		probe := spQANewAttempt(q, 0, actual.Value.RequestID)
		probe.Value.ID = spQAID(7102)
		if charge, err := sqliteSyncAttemptLocalCharge(probe); err != nil || charge != spQAChargeAttempt(probe) {
			t.Fatal("raw-valid ordinal0/Number1 append probe", err)
		}
		delta, err = sqliteAppendSyncAttempt(tx, m.ComputerID, probe)
		spQADeltaCorrupt(t, delta, err)
		list, err := sqliteSyncAttemptsLocal(tx, m.ComputerID, q.IntervalID, 0)
		spQAAttemptsZero(t, list, err)
		interopRollback(t, tx)
		interopClose(t, c)
	})
	spQAReopen(t, f, m, snapshot)
}

func TestSQLiteSyncPartsAttemptsDeleteFull28BeforeUnattemptedAndPlanRollback(t *testing.T) {
	f, m, snapshot, p, actual, _ := spQASeed(t, "synced", false, false)
	q := spQAQueued(p)
	for _, column := range strings.Split(sqQAPartCols, ",") {
		t.Run("delete-full28-before/"+column, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			spQABindPart(t, tx, q)
			delta, err := sqliteDeleteUnattemptedSyncPart(tx, m.ComputerID, q)
			if err != nil || delta != -spQAChargePart(q) {
				t.Fatal("positive full28 DELETE/negative charge before stale axis", err)
			}
			if row, found, err := sqliteReadSyncPartLocal(tx, m.ComputerID, q.IntervalID, 0); err != nil || found || !reflect.DeepEqual(row, sqliteSyncPartLocalRow{}) {
				t.Fatal("positive unattempted delete retained row", err)
			}
			spQABindPart(t, tx, q)
			asQAShadow(t, tx, "sync_parts", sqQAPartCols)
			query := "UPDATE sync_parts SET " + column + "=COALESCE(" + column + ",'')||'x' WHERE interval_id=?"
			if strings.Contains(",ordinal,duration_ns,start_sec,start_nsec,end_sec,end_nsec,planned_duration_ns,planned_residual_ns,confirmed_duration_ns,provider_delta_ns,total_residual_ns,", ","+column+",") {
				query = "UPDATE sync_parts SET " + column + "=COALESCE(" + column + ",0)+1 WHERE interval_id=?"
			}
			interopDone(t, tx, query, sqliteio.Text(q.IntervalID))
			delta, err = sqliteDeleteUnattemptedSyncPart(tx, m.ComputerID, q)
			spQADeltaCorrupt(t, delta, err)
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	for _, axis := range []string{"state", "entry", "failure-empty", "attachment", "actual-attempt", "malformed-attempt-probe"} {
		t.Run("delete-refusal/"+axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			x := spQAClonePart(q)
			switch axis {
			case "state":
				x.State = "unknown"
			case "entry":
				x = spQAClonePart(p)
				x.State = "synced"
			case "failure-empty":
				x.FailureCategory = spQAPtr("")
			case "attachment":
				x.Attachment = &SyncAttachment{RequestID: actual.Value.RequestID, EntryID: "1"}
			}
			spQABindPart(t, tx, x)
			if axis == "actual-attempt" || axis == "malformed-attempt-probe" {
				a := spQANewAttempt(q, 0, actual.Value.RequestID)
				spQABindAttempt(t, tx, a)
				if axis == "malformed-attempt-probe" {
					asQAShadow(t, tx, "sync_attempts", sqQAAttemptCols)
					interopDone(t, tx, "UPDATE sync_attempts SET ordinal=?", sqliteio.Blob([]byte{1}))
				}
			}
			delta, err := sqliteDeleteUnattemptedSyncPart(tx, m.ComputerID, x)
			if delta != 0 || err == nil {
				t.Fatal("attempted/attached/nonqueued deletion accepted", axis, err)
			}
			if axis == "malformed-attempt-probe" {
				bgQACorrupt(t, err)
			}
			if interopCount(t, tx, "SELECT count(*) FROM sync_parts") != 1 {
				t.Fatal("refused delete changed retained part")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	t.Run("failed-middle-delete-restores-old-and-staged-new-header-sibling-meta", func(t *testing.T) {
		f, m, _, p, _, root := spQASeed(t, "synced", false, false)
		q := spQAQueued(p)
		// First freeze a committed complete OLD local plan of three queued
		// scalars. This fixture doesn't claim calendar-composer certification.
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		parts := []sqliteSyncPartLocalRow{spQANextPart(q, 0), spQANextPart(q, 1), spQANextPart(q, 2)}
		var add int64
		for _, x := range parts {
			spQABindPart(t, tx, x)
			add += spQAChargePart(x)
		}
		baseline := metaQANext(t, m)
		baseline.Revision, baseline.LogicalBytes = bump(m.Revision), m.LogicalBytes+add
		bgQAFixtureError(t, "freeze old-plan metadata", sqliteUpdateMeta(tx, m, baseline))
		old, total := spQAAudit(t, tx)
		if total != baseline.LogicalBytes {
			t.Fatal("old-plan literal charge")
		}
		interopCommit(t, tx)
		interopClose(t, c)
		c, tx = interopOpen(t, f, false, sqliteio.Write)
		delta, err := sqliteDeleteUnattemptedSyncPart(tx, baseline.ComputerID, parts[0])
		if err != nil || delta != -spQAChargePart(parts[0]) {
			t.Fatal("first old-part deletion", err)
		}
		// Header is an independently staged caller sibling, not a second
		// implementation of the separate seven configuration/plan APIs.
		interopDone(t, tx, "UPDATE sync_plans SET company_source='user_declared_fallback' WHERE interval_id=?", sqliteio.Text(q.IntervalID))
		newRoot := sqQAClone(root)
		newRoot.Revision = bump(root.Revision)
		droot, err := sqliteUpdateOutboxLocal(tx, baseline.ComputerID, root, newRoot)
		if err != nil || droot != 0 {
			t.Fatal("actual sibling root CAS", err)
		}
		next := metaQANext(t, baseline)
		next.Revision, next.LogicalBytes = bump(baseline.Revision), baseline.LogicalBytes+delta+int64(len("user_declared_fallback")-len("company_verified"))
		bgQAFixtureError(t, "caller sibling metadata staged", sqliteUpdateMeta(tx, baseline, next))
		if _, billed := spQAAudit(t, tx); billed != next.LogicalBytes {
			t.Fatal("staged old/new header and sibling metadata charge disagree")
		}
		stale := spQAClonePart(parts[1])
		stale.Notes += "stale caller before"
		delta, err = sqliteDeleteUnattemptedSyncPart(tx, baseline.ComputerID, stale)
		spQADeltaCorrupt(t, delta, err)
		if interopCount(t, tx, "SELECT count(*) FROM sync_parts") != 2 {
			t.Fatal("delta0 incorrectly rolled back caller sibling deletion")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		spQAReopen(t, f, baseline, old)
		// A second failure follows real deletion of all old parts and the
		// first actual new-part insert. Both old and new plan bytes must roll
		// back together when the following insert hits the real1811 trigger.
		c, tx = interopOpen(t, f, false, sqliteio.Write)
		removed := int64(0)
		for _, x := range parts {
			d, err := sqliteDeleteUnattemptedSyncPart(tx, baseline.ComputerID, x)
			if err != nil || d != -spQAChargePart(x) {
				t.Fatal("old replacement deletion", err)
			}
			removed += d
		}
		interopDone(t, tx, "UPDATE sync_plans SET company_source='user_declared_fallback' WHERE interval_id=?", sqliteio.Text(q.IntervalID))
		newPart := spQANextPart(q, 0)
		newPart.ID, newPart.Correlation = spQAID(9000), "tempo:v1:"+spQAID(9000)
		newPart.Notes += "replacement"
		inserted, err := sqliteWriteSyncPartLocal(tx, baseline.ComputerID, nil, newPart)
		if err != nil || inserted != spQAChargePart(newPart) {
			t.Fatal("first real replacement insert", err)
		}
		droot, err = sqliteUpdateOutboxLocal(tx, baseline.ComputerID, root, newRoot)
		if err != nil || droot != 0 {
			t.Fatal("replacement sibling root", err)
		}
		next = metaQANext(t, baseline)
		next.Revision, next.LogicalBytes = bump(baseline.Revision), baseline.LogicalBytes+removed+inserted+int64(len("user_declared_fallback")-len("company_verified"))
		bgQAFixtureError(t, "replacement sibling metadata", sqliteUpdateMeta(tx, baseline, next))
		if _, billed := spQAAudit(t, tx); billed != next.LogicalBytes {
			t.Fatal("replacement staged charge disagrees")
		}
		badNew := spQANextPart(q, 1)
		badNew.ID, badNew.Correlation = root.ID, "tempo:v1:"+root.ID
		delta, err = sqliteWriteSyncPartLocal(tx, baseline.ComputerID, nil, badNew)
		sqQAConstraint(t, err, 1811)
		if delta != 0 {
			t.Fatal("failed new replacement delta")
		}
		spQAReadPart(t, tx, baseline.ComputerID, newPart)
		interopRollback(t, tx)
		interopClose(t, c)
		spQAReopen(t, f, baseline, old)
	})
	spQAReopen(t, f, m, snapshot)
}

func TestSQLiteSyncPartsAttemptsActualNative1555And2067AndCrossFamily1811(t *testing.T) {
	f, m, snapshot, p, actual, root := spQASeed(t, "synced", false, false)
	q := spQAQueued(p)
	for _, family := range []string{"part", "attempt"} {
		for _, axis := range []string{"primary1555", "unique2067", "cross-root1811", "cross-other-child1811"} {
			t.Run(family+"/"+axis, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				part := q
				attempt := spQANewAttempt(q, 0, actual.Value.RequestID)
				if family == "attempt" {
					spQABindPart(t, tx, q)
				}
				var delta int64
				var err error
				code := int32(1555)
				switch axis {
				case "primary1555":
					// Isolated real PK failure after a BEFORE INSERT races a
					// different same-family ID into the otherwise-empty PK.
					// No duplicate ID/cross-family trigger can mask1555.
					table, cols := "sync_parts", sqQAPartCols
					if family == "attempt" {
						table, cols = "sync_attempts", sqQAAttemptCols
					}
					selects := []string{}
					for _, col := range strings.Split(cols, ",") {
						value := "NEW." + col
						if col == "id" {
							value = "'" + spQAID(8000) + "'"
						}
						if family == "part" && col == "correlation" {
							value = "'tempo:v1:" + spQAID(8000) + "'"
						}
						selects = append(selects, value)
					}
					interopDone(t, tx, "CREATE TRIGGER qa_pk_race BEFORE INSERT ON "+table+" BEGIN INSERT INTO "+table+"("+cols+") SELECT "+strings.Join(selects, ",")+"; END")
				case "unique2067":
					code = 2067
					if family == "part" {
						spQABindPart(t, tx, q)
						part.Ordinal = 1
					} else {
						spQABindAttempt(t, tx, attempt)
						attempt.Ordinal, attempt.Value.Number = 1, "2"
					}
				case "cross-root1811":
					code = 1811
					if family == "part" {
						part.ID, part.Correlation = root.ID, "tempo:v1:"+root.ID
					} else {
						attempt.Value.ID = root.ID
					}
				case "cross-other-child1811":
					code = 1811
					if family == "part" {
						sibling := spQANewAttempt(q, 0, actual.Value.RequestID)
						spQABindAttempt(t, tx, sibling)
						part.ID, part.Correlation = sibling.Value.ID, "tempo:v1:"+sibling.Value.ID
					} else {
						attempt.Value.ID = q.ID
					}
				}
				if family == "part" {
					delta, err = sqliteWriteSyncPartLocal(tx, m.ComputerID, nil, part)
				} else {
					delta, err = sqliteAppendSyncAttempt(tx, m.ComputerID, attempt)
				}
				sqQAConstraint(t, err, code)
				if delta != 0 {
					t.Fatal("native failed insert exposed nonzero delta")
				}
				var domain *Error
				if code == 1555 || code == 2067 {
					if !errors.As(err, &domain) || domain.Code != "validation" {
						t.Fatal("native1555/2067 cause lacks identity-validation wrapper", err)
					}
				}
				interopSafeError(t, err, f.directory, part.ID, attempt.Value.ID)
				interopRollback(t, tx)
				interopClose(t, c)
			})
		}
	}
	spQAReopen(t, f, m, snapshot)
}

func TestSQLiteSyncPartsAttemptsActualCancellationReadonlyDONECloseAndOneBegin(t *testing.T) {
	f, m, snapshot, p, a, root := spQASeed(t, "synced", true, true)
	for _, family := range []string{"part-read", "parts-list", "part-update", "part-insert", "part-delete", "attempts-list", "attempt-append", "attempt-update", "root-indexed-read", "root-update"} {
		for _, fault := range []string{"DONE", "Close", "canceled", "deadline"} {
			t.Run(family+"/"+fault, func(t *testing.T) {
				operationBase := context.Background()
				if fault == "deadline" {
					timed, stop := context.WithTimeout(operationBase, 250*time.Millisecond)
					defer stop()
					operationBase = timed
				}
				ctx, cancel := context.WithCancelCause(operationBase)
				defer cancel(context.Canceled)
				conn, err := sqliteio.Open(ctx, f.directory, f.database, sqliteio.Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := conn.Close(context.Background()); err != nil {
						t.Error("fault fixture close", err)
					}
				})
				tx, err := conn.Begin(ctx, sqliteio.Write)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := tx.Rollback(); err != nil {
						t.Error("fault fixture rollback", err)
					}
				})
				beforePart, beforeAttempt := spQAClonePart(p), spQACloneAttempt(a)
				// Give each mutator its otherwise-valid actual local preconditions.
				if family == "part-delete" {
					interopDone(t, tx, "DELETE FROM sync_attempts WHERE interval_id=?", sqliteio.Text(p.IntervalID))
					interopDone(t, tx, "DELETE FROM sync_parts WHERE interval_id=?", sqliteio.Text(p.IntervalID))
					beforePart = spQAQueued(p)
					spQABindPart(t, tx, beforePart)
				}
				if family == "part-insert" {
					beforePart = spQANextPart(spQAQueued(p), 1)
				}
				if family == "attempt-append" {
					beforeAttempt = spQANewAttempt(p, 1, a.Value.RequestID)
				}
				spQAHookPositive(t, tx)
				cause := error(sqliteio.ErrUnsafe)
				fired, prepares, begins := false, 0, 0
				spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
					if e.Phase == "prepare-before" {
						prepares++
					}
					if e.Phase == "control-before-native" && e.Operation == "begin" {
						begins++
					}
				}, Fault: func(e spQASQLEvent) error {
					if !fired && (fault == "DONE" && e.Phase == "step-after" && e.Code == 101 || fault == "Close" && e.Phase == "finalize-after") {
						fired = true
						return cause
					}
					return nil
				}})
				if fault == "canceled" {
					cancel(context.Canceled)
					cause = context.Canceled
				}
				if fault == "deadline" {
					<-operationBase.Done()
					cause = context.DeadlineExceeded
				}

				var delta int64
				switch family {
				case "part-read":
					got, found, failure := sqliteReadSyncPartLocal(tx, m.ComputerID, p.IntervalID, 0)
					err = failure
					if found || !reflect.DeepEqual(got, sqliteSyncPartLocalRow{}) {
						t.Fatal("native part read partial output")
					}
				case "parts-list":
					got, failure := sqliteSyncPartsLocal(tx, m.ComputerID, p.IntervalID)
					err = failure
					if got != nil {
						t.Fatal("native parts list partial output")
					}
				case "part-update":
					after := spQAClonePart(beforePart)
					after.RoundedHours = spQAPtr("0.040")
					delta, err = sqliteWriteSyncPartLocal(tx, m.ComputerID, &beforePart, after)
				case "part-insert":
					delta, err = sqliteWriteSyncPartLocal(tx, m.ComputerID, nil, beforePart)
				case "part-delete":
					delta, err = sqliteDeleteUnattemptedSyncPart(tx, m.ComputerID, beforePart)
				case "attempts-list":
					got, failure := sqliteSyncAttemptsLocal(tx, m.ComputerID, p.IntervalID, 0)
					err = failure
					if got != nil {
						t.Fatal("native attempts list partial output")
					}
				case "attempt-append":
					delta, err = sqliteAppendSyncAttempt(tx, m.ComputerID, beforeAttempt)
				case "attempt-update":
					after := spQACloneAttempt(beforeAttempt)
					after.Value.FailureCategory = spQAPtr("")
					delta, err = sqliteUpdateSyncAttempt(tx, m.ComputerID, beforeAttempt, after)
				case "root-indexed-read":
					got, found, failure := sqliteReadOutboxByIDLocal(tx, m.ComputerID, root.ID)
					err = failure
					if found || !reflect.DeepEqual(got, sqliteOutboxLocalRow{}) {
						t.Fatal("native indexed root partial output")
					}
				case "root-update":
					after := sqQAClone(root)
					after.FailureCategory = spQAPtr("")
					delta, err = sqliteUpdateOutboxLocal(tx, m.ComputerID, root, after)
				}
				spQASetSQLHooks(spQASQLHooks{})
				spQANativeError(t, err, cause)
				if fault == "canceled" || fault == "deadline" {
					var native *sqliteio.Error
					if !errors.As(err, &native) || native.Category != sqliteio.Canceled {
						t.Fatal("real cancellation/deadline category", err)
					}
				}
				if delta != 0 {
					t.Fatal("native failure leaked delta", delta)
				}
				if fault == "DONE" || fault == "Close" {
					if !fired || prepares != 1 || begins != 0 {
						t.Fatal("first selected cursor fault was ignored, dependent SQL begun, or hidden Begin", fired, prepares, begins)
					}
				}
				interopRollback(t, tx)
				interopClose(t, conn)
				spQAReopen(t, f, m, snapshot)
			})
		}
	}
	for _, family := range []string{"part-update", "part-insert", "part-delete", "attempt-append", "attempt-update", "root-update"} {
		t.Run("actual-readonly/"+family, func(t *testing.T) {
			// Separate fixture supplies a genuinely queued delete control.
			ff, mm, frozen, pp, aa, rr := spQASeed(t, "synced", true, true)
			if family == "part-delete" {
				c, tx := interopOpen(t, ff, false, sqliteio.Write)
				interopDone(t, tx, "DELETE FROM sync_attempts WHERE interval_id=?", sqliteio.Text(pp.IntervalID))
				interopDone(t, tx, "DELETE FROM sync_parts WHERE interval_id=?", sqliteio.Text(pp.IntervalID))
				pp = spQAQueued(pp)
				spQABindPart(t, tx, pp)
				next := metaQANext(t, mm)
				next.Revision = bump(mm.Revision)
				_, next.LogicalBytes = spQAAudit(t, tx)
				bgQAFixtureError(t, "readonly queued fixture meta", sqliteUpdateMeta(tx, mm, next))
				frozen, _ = spQAAudit(t, tx)
				mm = next
				interopCommit(t, tx)
				interopClose(t, c)
			}
			c, tx := interopOpen(t, ff, false, sqliteio.Read)
			var delta int64
			var err error
			switch family {
			case "part-update":
				after := spQAClonePart(pp)
				after.RoundedHours = spQAPtr("0.040")
				delta, err = sqliteWriteSyncPartLocal(tx, mm.ComputerID, &pp, after)
			case "part-insert":
				delta, err = sqliteWriteSyncPartLocal(tx, mm.ComputerID, nil, spQANextPart(spQAQueued(pp), 1))
			case "part-delete":
				delta, err = sqliteDeleteUnattemptedSyncPart(tx, mm.ComputerID, pp)
			case "attempt-append":
				delta, err = sqliteAppendSyncAttempt(tx, mm.ComputerID, spQANewAttempt(pp, 1, aa.Value.RequestID))
			case "attempt-update":
				after := spQACloneAttempt(aa)
				after.Value.FailureCategory = spQAPtr("")
				delta, err = sqliteUpdateSyncAttempt(tx, mm.ComputerID, aa, after)
			case "root-update":
				after := sqQAClone(rr)
				after.FailureCategory = spQAPtr("")
				delta, err = sqliteUpdateOutboxLocal(tx, mm.ComputerID, rr, after)
			}
			spQANativeError(t, err, nil)
			if delta != 0 {
				t.Fatal("native readonly delta")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			spQAReopen(t, ff, mm, frozen)
		})
	}
}

func TestSQLiteSyncPartsAttemptsCallerSiblingMetaRollbackAfterActualTriggerAndOneBegin(t *testing.T) {
	f, m, snapshot, p, a, root := spQASeed(t, "synced", false, false)
	q := spQAQueued(p)
	var begins int
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		if e.Phase == "control-before-native" && e.Operation == "begin" {
			begins++
		}
	}})
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	if begins != 1 {
		t.Fatal("actual enclosing writer Begin control", begins)
	}
	d1, err := sqliteWriteSyncPartLocal(tx, m.ComputerID, nil, q)
	if err != nil || d1 != spQAChargePart(q) {
		t.Fatal("actual caller sibling part", err)
	}
	claim := spQANewAttempt(q, 0, a.Value.RequestID)
	d2, err := sqliteAppendSyncAttempt(tx, m.ComputerID, claim)
	if err != nil || d2 != spQAChargeAttempt(claim) {
		t.Fatal("actual caller sibling attempt", err)
	}
	nextRoot := sqQAClone(root)
	nextRoot.Revision = bump(root.Revision)
	dr, err := sqliteUpdateOutboxLocal(tx, m.ComputerID, root, nextRoot)
	if err != nil || dr != 0 {
		t.Fatal("actual caller sibling root", err)
	}
	next := metaQANext(t, m)
	next.Revision, next.LogicalBytes = bump(m.Revision), m.LogicalBytes+d1+d2
	bgQAFixtureError(t, "caller sibling nonce/revision/charge", sqliteUpdateMeta(tx, m, next))
	collision := spQANextPart(q, 1)
	collision.ID, collision.Correlation = root.ID, "tempo:v1:"+root.ID
	delta, err := sqliteWriteSyncPartLocal(tx, m.ComputerID, nil, collision)
	sqQAConstraint(t, err, 1811)
	if delta != 0 || begins != 1 {
		t.Fatal("failed row delta/secret Begin", delta, begins)
	}
	spQASetSQLHooks(spQASQLHooks{})
	// Error delta0 describes the failed primitive, never the whole caller Tx.
	spQAReadPart(t, tx, m.ComputerID, q)
	if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, next) {
		t.Fatal("delta0 falsely acknowledged rollback of sibling meta", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	spQAReopen(t, f, m, snapshot)
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	interopRollback(t, tx)
	got, found, err := sqliteReadOutboxByIDLocal(tx, m.ComputerID, root.ID)
	spQANativeError(t, err, sqliteio.ErrClosed)
	if found || !reflect.DeepEqual(got, sqliteOutboxLocalRow{}) {
		t.Fatal("terminal Tx root read leaked row")
	}
	interopClose(t, c)
}

func TestSQLiteSyncPartsAttemptsRawNullableAmountsAttachmentsAndCheckedArguments(t *testing.T) {
	_, _, _, p, a, root := spQASeed(t, "synced", false, false)
	for _, field := range []string{"EntryID", "ReturnedHours", "ConfirmedDurationNS", "ProviderDeltaNS", "TotalResidualNS"} {
		t.Run("missing-confirmed-group/"+field, func(t *testing.T) {
			x := spQAClonePart(p)
			reflect.ValueOf(&x).Elem().FieldByName(field).Set(reflect.Zero(reflect.TypeOf((*string)(nil))))
			delta, err := sqliteSyncPartLocalCharge(x)
			spQADeltaValidation(t, delta, err)
			delta, err = sqliteWriteSyncPartLocal(nil, interopComputer, nil, x)
			spQADeltaValidation(t, delta, err)
			delta, err = sqliteWriteSyncPartLocal(nil, interopComputer, &x, p)
			spQADeltaValidation(t, delta, err)
		})
	}
	for _, field := range []string{"DurationNS", "PlannedDurationNS", "PlannedResidualNS", "ConfirmedDurationNS", "ProviderDeltaNS", "TotalResidualNS"} {
		for _, spelling := range []string{"+0", "-0", "00", " 0", "9223372036854775808", "-9223372036854775809", string([]byte{0xff})} {
			t.Run("canonical-signed/"+field+"/"+spelling, func(t *testing.T) {
				x := spQAClonePart(p)
				target := reflect.ValueOf(&x).Elem().FieldByName(field)
				if target.Kind() == reflect.String {
					target.SetString(spelling)
				} else {
					target.Set(reflect.ValueOf(spQAPtr(spelling)))
				}
				delta, err := sqliteSyncPartLocalCharge(x)
				spQADeltaValidation(t, delta, err)
				delta, err = sqliteWriteSyncPartLocal(nil, interopComputer, nil, x)
				spQADeltaValidation(t, delta, err)
			})
		}
	}
	for _, axis := range []string{"attachment-request-utf8", "attachment-entry-utf8", "confirmed-wrong-state", "confirmed-equal-with-failure", "confirmed-mismatch-without-failure", "confirmed-mismatch-wrong-failure", "entry-invalid", "returned-decimal-invalid", "rounded-decimal-invalid"} {
		t.Run(axis, func(t *testing.T) {
			x := spQAClonePart(p)
			switch axis {
			case "attachment-request-utf8":
				x.Attachment = &SyncAttachment{RequestID: string([]byte{0xff}), EntryID: "1"}
			case "attachment-entry-utf8":
				x.Attachment = &SyncAttachment{RequestID: a.Value.RequestID, EntryID: string([]byte{0xff})}
			case "confirmed-wrong-state":
				x.State = "unknown"
			case "confirmed-equal-with-failure":
				x.FailureCategory = spQAPtr("")
			case "confirmed-mismatch-without-failure":
				x.State, x.ReturnedHours, x.ConfirmedDurationNS, x.ProviderDeltaNS, x.TotalResidualNS = "needs_attention", spQAPtr("0"), spQAPtr("0"), spQAPtr(x.PlannedDurationNS), spQAPtr(x.DurationNS)
			case "confirmed-mismatch-wrong-failure":
				x.State, x.FailureCategory, x.ReturnedHours, x.ConfirmedDurationNS, x.ProviderDeltaNS, x.TotalResidualNS = "needs_attention", spQAPtr("other"), spQAPtr("0"), spQAPtr("0"), spQAPtr(x.PlannedDurationNS), spQAPtr(x.DurationNS)
			case "entry-invalid":
				x.EntryID = spQAPtr("01")
			case "returned-decimal-invalid":
				x.ReturnedHours = spQAPtr("not a decimal")
			case "rounded-decimal-invalid":
				x.RoundedHours = spQAPtr("not a decimal")
			}
			delta, err := sqliteSyncPartLocalCharge(x)
			spQADeltaValidation(t, delta, err)
			delta, err = sqliteWriteSyncPartLocal(nil, interopComputer, nil, x)
			spQADeltaValidation(t, delta, err)
		})
	}
	for _, axis := range []string{"owner", "request", "id", "part-negative", "part100", "ordinal-negative", "number-zero", "number-leading-zero", "number-not-ordinal", "queued", "entry", "failure-utf8"} {
		t.Run("attempt-scalar/"+axis, func(t *testing.T) {
			x := spQACloneAttempt(a)
			switch axis {
			case "owner":
				x.IntervalID = string([]byte{0xff})
			case "request":
				x.Value.RequestID = "invalid"
			case "id":
				x.Value.ID = "invalid"
			case "part-negative":
				x.PartOrdinal = -1
			case "part100":
				x.PartOrdinal = 100
			case "ordinal-negative":
				x.Ordinal = -1
			case "number-zero":
				x.Value.Number = "0"
			case "number-leading-zero":
				x.Value.Number = "01"
			case "number-not-ordinal":
				x.Value.Number = "2"
			case "queued":
				x.Value.State = "queued"
			case "entry":
				x.Value.EntryID = spQAPtr("")
			case "failure-utf8":
				x.Value.FailureCategory = spQAPtr(string([]byte{0xff}))
			}
			delta, err := sqliteSyncAttemptLocalCharge(x)
			spQADeltaValidation(t, delta, err)
			delta, err = sqliteUpdateSyncAttempt(nil, interopComputer, x, a)
			spQADeltaValidation(t, delta, err)
		})
	}
	// Invalid raw owner/key arguments must be validated before a nil Tx can
	// be dereferenced. These cover reads/lists as well as mutable row codecs.
	got, found, err := sqliteReadSyncPartLocal(nil, "invalid", p.IntervalID, 0)
	bgQAValidation(t, err)
	if found || !reflect.DeepEqual(got, sqliteSyncPartLocalRow{}) {
		t.Fatal("bad part owner output")
	}
	got, found, err = sqliteReadSyncPartLocal(nil, interopComputer, p.IntervalID, 100)
	bgQAValidation(t, err)
	if found || !reflect.DeepEqual(got, sqliteSyncPartLocalRow{}) {
		t.Fatal("bad part key output")
	}
	parts, err := sqliteSyncPartsLocal(nil, interopComputer, "invalid")
	bgQAValidation(t, err)
	if parts != nil {
		t.Fatal("bad part list output")
	}
	attempts, err := sqliteSyncAttemptsLocal(nil, interopComputer, p.IntervalID, 100)
	bgQAValidation(t, err)
	if attempts != nil {
		t.Fatal("bad attempt list output")
	}
	indexed, found, err := sqliteReadOutboxByIDLocal(nil, interopComputer, root.ID+" ")
	bgQAValidation(t, err)
	if found || !reflect.DeepEqual(indexed, sqliteOutboxLocalRow{}) {
		t.Fatal("raw indexed root was trimmed")
	}
	delta, err := sqliteWriteSyncPartLocal(nil, "invalid", nil, p)
	spQADeltaValidation(t, delta, err)
	delta, err = sqliteDeleteUnattemptedSyncPart(nil, "invalid", spQAQueued(p))
	spQADeltaValidation(t, delta, err)
	delta, err = sqliteAppendSyncAttempt(nil, "invalid", spQANewAttempt(p, 0, a.Value.RequestID))
	spQADeltaValidation(t, delta, err)
	delta, err = sqliteUpdateSyncAttempt(nil, "invalid", a, a)
	spQADeltaValidation(t, delta, err)
	delta, err = sqliteUpdateOutboxLocal(nil, "invalid", root, root)
	spQADeltaValidation(t, delta, err)
}

func TestSQLiteSyncPartsAttemptsActualConnectionCloseAndSpentBeginAdmission(t *testing.T) {
	f, m, snapshot, _, _, root := spQASeed(t, "synced", true, true)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	spQAHookPositive(t, tx)
	got, found, err := sqliteReadOutboxByIDLocal(tx, m.ComputerID, root.ID)
	if err != nil || !found || !reflect.DeepEqual(got, root) {
		t.Fatal("native connection control", err)
	}
	interopRollback(t, tx)
	var begins int
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		if e.Phase == "control-before-native" && e.Operation == "begin" {
			begins++
		}
	}})
	second, err := c.Begin(context.Background(), sqliteio.Read)
	spQASetSQLHooks(spQASQLHooks{})
	spQANativeError(t, err, sqliteio.ErrClosed)
	if second != nil || begins != 0 {
		t.Fatal("one-use connection dispatched a second BEGIN", begins)
	}
	fired := false
	spQAHooks(t, spQASQLHooks{Fault: func(e spQASQLEvent) error {
		if e.Phase == "close-before" && e.Operation == "close" {
			fired = true
			return sqliteio.ErrUnsafe
		}
		return nil
	}})
	err = c.Close(context.Background())
	spQASetSQLHooks(spQASQLHooks{})
	spQANativeError(t, err, sqliteio.ErrUnsafe)
	if !fired {
		t.Fatal("native connection close boundary did not run")
	}
	// A failed before-close still owns the connection; checked cleanup retries
	// only native ownership closure, never a row write or a transaction.
	interopClose(t, c)
	spent, err := sqliteio.Open(context.Background(), f.directory, f.database, sqliteio.Options{AcquireDeadline: time.Now().Add(-time.Millisecond)})
	spQANativeError(t, err, sqliteio.ErrBusy)
	if spent != nil {
		t.Fatal("spent admission deadline returned a live connection")
	}
	spQAReopen(t, f, m, snapshot)
}
