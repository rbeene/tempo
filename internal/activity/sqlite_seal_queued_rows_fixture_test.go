//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Six-row-API QA only. Complete owned legacy graphs are behavior/billing
// oracles; fresh local SQL subsets never claim a complete seal or sync graph.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/harvest"
)

const sqQASealCols = "interval_id,component_id"
const sqQAOutboxCols = "interval_id,id,revision,state,correlation,entry_id,failure_category,retry_request_id,run_request_id,plan_present"
const sqQAIntervalCols = "interval_id,computer_id,account_id,user_id,project_id,task_id,timezone,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,duration_ns,creation_ordinal"
const sqQAPartCols = "interval_id,ordinal,id,spent_date,duration_ns,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,planned_hours,planned_duration_ns,planned_residual_ns,started_time,ended_time,correlation,notes,state,entry_id,failure_category,returned_hours,rounded_hours,confirmed_duration_ns,provider_delta_ns,total_residual_ns,attachment_request_id,attachment_entry_id"
const sqQAAttemptCols = "interval_id,part_ordinal,ordinal,request_id,id,number,state,entry_id,failure_category"
const sqQAMissing = "77777777-7777-4777-8777-777777777777"
const sqQAOtherInterval = "88888888-8888-4888-8888-888888888888"
const sqQARoot = "99999999-9999-4999-8999-999999999991"

func sqQALegacy(t *testing.T, phase string) (*state, OutboxItem) {
	t.Helper()
	s, path, interval := qaSyncFixture(t, 137482*time.Millisecond)
	var captured *state
	if phase != "queued" {
		p := qaNewSyncProvider(t) // Owned in-memory QA provider; no network.
		if phase != "needs_attention" {
			qaSyncConfigure(t, s, p)
		}
		qaSyncEnable(t, s)
		switch phase {
		case "unknown":
			p.createErr = &harvest.Error{Code: "timeout", Uncertain: true}
		case "rejected":
			p.createErr = &harvest.Error{Code: "validation", Status: 422}
		case "submitting":
			p.beforeCreate = func(_ harvest.Object) { captured = asQAJSON(t, bgQAReadLegacy(t, s)) }
		}
		_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(3)}, qaSyncDeps(t, p))
		bgQAFixtureError(t, "actual reference sync lifecycle", err)
	}
	raw := bgQAReadLegacy(t, s)
	if phase == "submitting" {
		if captured == nil {
			t.Fatal("actual durable intent snapshot absent")
		}
		raw = captured
	}
	st := bgQALegacyMarshalOracle(t, s, path, raw, true)
	item, ok := st.Outbox[interval.ID]
	if !ok || item.State != phase || !reflect.DeepEqual(item.Interval, st.Intervals[0]) {
		t.Fatal("complete strict reference root/billing shape absent")
	}
	return st, item
}

func sqQARow(item OutboxItem) sqliteOutboxLocalRow {
	return sqliteOutboxLocalRow{IntervalID: item.Interval.ID, ID: item.ID, Revision: item.Revision, State: item.State, Correlation: item.Correlation, EntryID: item.EntryID, FailureCategory: item.FailureCategory, RetryRequestID: item.RetryRequestID, RunRequestID: item.RunRequestID, PlanPresent: item.Plan != nil}
}

func sqQAClone(r sqliteOutboxLocalRow) sqliteOutboxLocalRow {
	for _, p := range []**string{&r.EntryID, &r.FailureCategory, &r.RetryRequestID, &r.RunRequestID} {
		if *p != nil {
			value := **p
			*p = &value
		}
	}
	return r
}

func sqQAComponent(t *testing.T, st *state, in Interval) string {
	t.Helper()
	ids := []string{}
	for _, id := range in.SegmentIDs {
		s := st.Segments[id]
		if s == nil {
			t.Fatal("actual support missing")
		}
		if segmentEnd(s).After(s.Start) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		t.Fatal("actual positive support missing")
	}
	h := sha256.New()
	h.Write([]byte("tempo-frontier-component-v1\x00"))
	a := in.Attribution
	for _, value := range []string{in.ComputerID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, ids[0]} {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(value)))
		h.Write(n[:])
		h.Write([]byte(value))
	}
	return fmt.Sprintf("fc1:%x", h.Sum(nil))
}

func sqQAInterval(item OutboxItem, ordinal int64) sqliteIntervalLocalRow {
	i := item.Interval
	return sqliteIntervalLocalRow{ID: i.ID, ComputerID: i.ComputerID, Attribution: i.Attribution, Start: i.Start, End: i.End, DurationNS: i.DurationNS, Ordinal: ordinal}
}

func sqQAIntervalCharge(t *testing.T, r sqliteIntervalLocalRow) int64 {
	t.Helper()
	a := r.Attribution
	n := int64(184)
	for _, v := range []string{r.ID, r.ComputerID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, ueQAGroup(t, r.ComputerID, a), ueQAJSONTime(t, r.Start), ueQAJSONTime(t, r.End)} {
		n += int64(len(v))
	}
	return n
}

func sqQACharge(r sqliteOutboxLocalRow) int64 {
	n := int64(98 + len(r.IntervalID) + len(r.ID) + len(r.State) + len(r.Correlation))
	for _, v := range []*string{r.EntryID, r.FailureCategory, r.RetryRequestID, r.RunRequestID} {
		if v != nil {
			n += 8 + int64(len(*v))
		}
	}
	return n
}

func sqQABindOutbox(t *testing.T, tx *sqliteio.Tx, r sqliteOutboxLocalRow) {
	t.Helper()
	plan := int64(0)
	if r.PlanPresent {
		plan = 1
	}
	asQABindFixture(t, tx, "outbox", sqQAOutboxCols, []sqliteio.Value{sqliteio.Text(r.IntervalID), sqliteio.Text(r.ID), interopCounter(t, r.Revision), sqliteio.Text(r.State), sqliteio.Text(r.Correlation), ueQATextOptional(r.EntryID), ueQATextOptional(r.FailureCategory), ueQATextOptional(r.RetryRequestID), ueQATextOptional(r.RunRequestID), sqliteio.Integer(plan)})
}

func sqQAPartValues(t *testing.T, interval string, p SyncPart) []sqliteio.Value {
	t.Helper()
	duration, ok := syncInt(p.DurationNS)
	if !ok {
		t.Fatal("actual part duration invalid")
	}
	planned, ok := syncInt(p.PlannedDurationNS)
	if !ok {
		t.Fatal("actual planned duration invalid")
	}
	residual, ok := syncInt(p.PlannedResidualNS)
	if !ok {
		t.Fatal("actual residual invalid")
	}
	v := []sqliteio.Value{sqliteio.Text(interval), sqliteio.Integer(0), sqliteio.Text(p.ID), sqliteio.Text(p.SpentDate), sqliteio.Integer(duration)}
	v = append(v, ueQATimeValues(t, p.Start)...)
	v = append(v, ueQATimeValues(t, p.End)...)
	v = append(v, sqliteio.Text(p.PlannedHours), sqliteio.Integer(planned), sqliteio.Integer(residual), ueQATextOptional(p.StartedTime), ueQATextOptional(p.EndedTime), sqliteio.Text(p.Correlation), sqliteio.Text(p.Notes), sqliteio.Text(p.State), ueQATextOptional(p.EntryID), ueQATextOptional(p.FailureCategory), ueQATextOptional(p.ReturnedHours), ueQATextOptional(p.RoundedHours))
	for _, text := range []*string{p.ConfirmedDurationNS, p.ProviderDeltaNS, p.TotalResidualNS} {
		value := sqliteio.Null()
		if text != nil {
			n, ok := syncInt(*text)
			if !ok {
				t.Fatal("actual amount invalid")
			}
			value = sqliteio.Integer(n)
		}
		v = append(v, value)
	}
	request, entry := sqliteio.Null(), sqliteio.Null()
	if p.Attachment != nil {
		request, entry = sqliteio.Text(p.Attachment.RequestID), sqliteio.Text(p.Attachment.EntryID)
	}
	return append(v, request, entry)
}

func sqQAAttemptValues(interval string, a SyncAttempt) []sqliteio.Value {
	return []sqliteio.Value{sqliteio.Text(interval), sqliteio.Integer(0), sqliteio.Integer(0), sqliteio.Text(a.RequestID), sqliteio.Text(a.ID), sqliteio.Text(a.Number), sqliteio.Text(a.State), ueQATextOptional(a.EntryID), ueQATextOptional(a.FailureCategory)}
}

func sqQAAudit(t *testing.T, tx *sqliteio.Tx, m sqliteStoreMeta) (map[string][][]string, int64) {
	t.Helper()
	result := map[string][][]string{}
	total := int64(114 + len(m.ComputerID) + len(m.StateBasename) + len(m.DatabaseBasename))
	for _, table := range []struct{ name, cols, order string }{{"intervals", sqQAIntervalCols, "creation_ordinal"}, {"interval_components", sqQASealCols, "interval_id,component_id"}, {"outbox", sqQAOutboxCols, "interval_id"}, {"sync_parts", sqQAPartCols, "interval_id,ordinal"}, {"sync_attempts", sqQAAttemptCols, "interval_id,part_ordinal,ordinal"}} {
		rows, charge := asQALiteralAudit(t, tx, table.name, table.cols, table.order)
		result[table.name] = rows
		total += charge
	}
	result["store_meta"], _ = asQALiteralAudit(t, tx, "store_meta", "singleton,schema_version,legacy_schema_version,computer_id,revision,sync_enabled,durability_nonce,state_basename,database_basename,migration_id,backup_sha256,logical_bytes", "singleton")
	return result, total
}

func sqQASeed(t *testing.T, st *state, item OutboxItem) (interopFixture, sqliteStoreMeta, map[string][][]string) {
	t.Helper()
	if !validState(st) || !reflect.DeepEqual(st.Outbox[item.Interval.ID], item) {
		t.Fatal("complete reference state does not match owned interval/root")
	}
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	bgQAFixtureError(t, "fresh row schema", sqliteCreateSchema(tx))
	m := interopMeta(f)
	m.ComputerID, m.Revision = st.ComputerID, st.Revision
	m.LogicalBytes = int64(114 + len(m.ComputerID) + len(m.StateBasename) + len(m.DatabaseBasename))
	first := sqQAInterval(item, 0)
	second := first
	second.ID, second.Ordinal = sqQAOtherInterval, 1
	for _, r := range []sqliteIntervalLocalRow{first, second} {
		delta, err := sqliteInsertIntervalLocal(tx, m.ComputerID, r)
		if err != nil || delta != sqQAIntervalCharge(t, r) {
			t.Fatal("real prerequisite interval Local insert", err)
		}
		m.LogicalBytes += delta
	}
	bgQAFixtureError(t, "row bootstrap metadata", sqliteInsertMeta(tx, m))
	snapshot, total := sqQAAudit(t, tx, m)
	if total != m.LogicalBytes {
		t.Fatal("literal initial row billing differs")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return f, m, snapshot
}

func sqQAReopen(t *testing.T, f interopFixture, m sqliteStoreMeta, want map[string][][]string) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	got, total := sqQAAudit(t, tx, m)
	if !reflect.DeepEqual(got, want) || total != m.LogicalBytes {
		t.Fatal("cold row bytes/charge/metadata/nonce changed")
	}
	meta, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(meta, m) {
		t.Fatal("cold metadata mismatch", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func sqQAConstraint(t *testing.T, err error, code int32) {
	t.Helper()
	var e *sqliteio.Error
	if !errors.As(err, &e) || e.Category != sqliteio.Constraint || e.Code != code {
		t.Fatalf("wanted native Constraint %d, got %v", code, err)
	}
	var domain *Error
	if code == 1811 && errors.As(err, &domain) && domain.Code == "validation" {
		t.Fatal("actual trigger1811 blanket-mapped to input/identity validation")
	}
}

func sqQARawWrite(tx *sqliteio.Tx, query string, values ...sqliteio.Value) error {
	s, err := tx.Prepare(query, values...)
	if err != nil {
		return err
	}
	row, err := s.Step()
	closeErr := s.Close()
	if row {
		return errors.Join(fmt.Errorf("QA fixture DML unexpectedly returned a row"), err, closeErr)
	}
	return errors.Join(err, closeErr)
}
