//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Fresh-store local QA. Actual legacy states are semantic/billing oracles only.
// These local SQL subsets do not claim complete seals, outbox or Service validity.
import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const fpiQAFrontierCols = "component_id,computer_id,account_id,user_id,project_id,task_id,timezone,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json"
const fpiQAPendingCols = "component_id,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json"
const fpiQAIntervalCols = "interval_id,computer_id,account_id,user_id,project_id,task_id,timezone,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,duration_ns,creation_ordinal"
const fpiQAMissing = "77777777-7777-4777-8777-777777777777"

func fpiQAID(n int) string {
	h := sha256.Sum256([]byte(fmt.Sprint("local-row-", n)))
	return fmt.Sprintf("fc1:%x", h)
}
func fpiQAHash(computer string, a Attribution, ids []string) string {
	copyIDs := append([]string(nil), ids...)
	sort.Strings(copyIDs)
	if len(copyIDs) == 0 {
		panic("positive support required for fixture identity")
	}
	h := sha256.New()
	h.Write([]byte("tempo-frontier-component-v1\x00"))
	for _, s := range []string{computer, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, copyIDs[0]} {
		var width [8]byte
		binary.BigEndian.PutUint64(width[:], uint64(len(s)))
		h.Write(width[:])
		h.Write([]byte(s))
	}
	return fmt.Sprintf("fc1:%x", h.Sum(nil))
}

func fpiQALegacy(t *testing.T) *state {
	t.Helper()
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
	h.ingest(5, qaEvent("B", "1", "1", "work", qaBindingB))
	h.ingest(5, qaEvent("zero", "1", "1", "work", qaBindingA))
	h.ingest(5, qaEvent("zero", "1", "2", "finish", ""))
	h.ingest(10, qaEvent("A", "1", "2", "finish", ""))
	h.ingest(20, qaEvent("B", "1", "2", "finish", ""))
	raw := bgQAReadLegacy(t, h.service)
	if len(raw.Intervals) != 1 || len(raw.Intervals[0].SegmentIDs) != 2 {
		t.Fatal("actual overlapping supports did not form one billed union")
	}
	// validState intentionally permits an empty historical contribution beside
	// positive coverage. Prove this with actual strict JSON/read, not declaration.
	for id, s := range raw.Segments {
		if s.Start.Equal(segmentEnd(s)) {
			s.Finalized = true
			raw.Intervals[0].SegmentIDs = append(raw.Intervals[0].SegmentIDs, id)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(raw.Intervals[0].SegmentIDs)))
	item := raw.Outbox[raw.Intervals[0].ID]
	item.Interval = raw.Intervals[0]
	raw.Outbox[item.Interval.ID] = item
	got := bgQALegacyMarshalOracle(t, h.service, h.path, raw, true)
	if !validState(got) || got.Intervals[0].DurationNS != "20000000000" || len(got.Intervals[0].SegmentIDs) != 3 {
		t.Fatal("strict billing/unsorted/zero-support oracle failed")
	}
	return got
}

func fpiQARows(t *testing.T, s *state) (sqliteFrontierLocalRow, sqlitePendingLocalRow, sqliteIntervalLocalRow) {
	t.Helper()
	in := s.Intervals[0]
	positive := []string{}
	for _, id := range in.SegmentIDs {
		seg := s.Segments[id]
		if segmentEnd(seg).After(seg.Start) {
			positive = append(positive, id)
		}
	}
	f := sqliteFrontierLocalRow{ID: fpiQAHash(s.ComputerID, in.Attribution, positive), ComputerID: s.ComputerID, Attribution: in.Attribution, Start: in.Start, End: in.End}
	p := sqlitePendingLocalRow{ID: f.ID, ComputerID: f.ComputerID, Attribution: f.Attribution, Start: f.Start, End: f.End}
	r := sqliteIntervalLocalRow{ID: in.ID, ComputerID: s.ComputerID, Attribution: in.Attribution, Start: in.Start, End: in.End, DurationNS: in.DurationNS, Ordinal: 0}
	return f, p, r
}
func fpiQAFrontierValues(t *testing.T, r sqliteFrontierLocalRow) []sqliteio.Value {
	a := r.Attribution
	v := []sqliteio.Value{sqliteio.Text(r.ID), sqliteio.Text(r.ComputerID), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID), sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Text(attributionKey(r.ComputerID, a))}
	v = append(v, asQATime(t, r.Start)...)
	return append(v, asQATime(t, r.End)...)
}
func fpiQAPendingValues(t *testing.T, r sqlitePendingLocalRow) []sqliteio.Value {
	v := []sqliteio.Value{sqliteio.Text(r.ID), sqliteio.Text(attributionKey(r.ComputerID, r.Attribution))}
	v = append(v, asQATime(t, r.Start)...)
	return append(v, asQATime(t, r.End)...)
}
func fpiQAIntervalValues(t *testing.T, r sqliteIntervalLocalRow) []sqliteio.Value {
	v := fpiQAFrontierValues(t, sqliteFrontierLocalRow{ID: r.ID, ComputerID: r.ComputerID, Attribution: r.Attribution, Start: r.Start, End: r.End})
	return append(v, interopCounter(t, r.DurationNS), sqliteio.Integer(r.Ordinal))
}
func fpiQACharge(t *testing.T, base int64, id, computer string, a Attribution, start, end time.Time) int64 {
	t.Helper()
	n := base
	for _, s := range []string{id, computer, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, attributionKey(computer, a)} {
		n += int64(len(s))
	}
	for _, v := range []time.Time{start, end} {
		b, err := v.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		n += int64(len(b))
	}
	return n
}
func fpiQAFCharge(t *testing.T, r sqliteFrontierLocalRow) int64 {
	return fpiQACharge(t, 158, r.ID, r.ComputerID, r.Attribution, r.Start, r.End)
}
func fpiQAPCharge(t *testing.T, r sqlitePendingLocalRow) int64 {
	n := int64(104 + len(r.ID) + len(attributionKey(r.ComputerID, r.Attribution)))
	for _, v := range []time.Time{r.Start, r.End} {
		b, err := v.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		n += int64(len(b))
	}
	return n
}
func fpiQAIGCharge(t *testing.T, r sqliteIntervalLocalRow) int64 {
	return fpiQACharge(t, 184, r.ID, r.ComputerID, r.Attribution, r.Start, r.End)
}
func fpiQAMCharge(component, segment string) int64 { return int64(50 + len(component) + len(segment)) }
func fpiQASCharge(interval, segment string) int64  { return int64(59 + len(interval) + len(segment)) }

func fpiQAAudit(t *testing.T, tx *sqliteio.Tx) (map[string][][]string, int64) {
	snapshot, total := asQAAudit(t, tx)
	for _, r := range []struct{ table, cols, order string }{{"union_frontier", fpiQAFrontierCols, "component_id"}, {"pending_finalization", fpiQAPendingCols, "component_id"}, {"intervals", fpiQAIntervalCols, "creation_ordinal"}, {"component_segments", "component_id,segment_id", "component_id,segment_id"}, {"interval_segments", "interval_id,ordinal,segment_id", "interval_id,ordinal"}} {
		rows, n := asQALiteralAudit(t, tx, r.table, r.cols, r.order)
		snapshot[r.table] = rows
		total += n
	}
	return snapshot, total
}
func fpiQASeed(t *testing.T, s *state) (interopFixture, sqliteStoreMeta, map[string][][]string) {
	// Actual integrated binding/generation/epoch/actor/segment APIs create FK
	// dependencies in a fresh schema. No legacy interval/outbox is imported.
	f, m, _ := asQASeed(t, s)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	snap, n := fpiQAAudit(t, tx)
	if n != m.LogicalBytes {
		t.Fatal("fresh local dependency accounting differs")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	return f, m, snap
}
func fpiQAReopen(t *testing.T, f interopFixture, m sqliteStoreMeta, snap map[string][][]string) {
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	got, n := fpiQAAudit(t, tx)
	if !reflect.DeepEqual(got, snap) || n != m.LogicalBytes {
		t.Fatal("rollback/reopen changed retained rows or literal charge")
	}
	meta, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(meta, m) {
		t.Fatal("rollback/reopen changed metadata", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}
func fpiQACommit(t *testing.T, tx *sqliteio.Tx, old sqliteStoreMeta, delta int64) (sqliteStoreMeta, map[string][][]string) {
	next := metaQANext(t, old)
	next.Revision = bump(old.Revision)
	next.LogicalBytes += delta
	if err := sqliteUpdateMeta(tx, old, next); err != nil {
		t.Fatal(err)
	}
	snap, n := fpiQAAudit(t, tx)
	if n != next.LogicalBytes {
		t.Fatalf("independent stored charge=%d meta=%d", n, next.LogicalBytes)
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	return next, snap
}
func fpiQANativeIdentity(t *testing.T, err error) {
	t.Helper()
	bgQAValidation(t, err)
	var n *sqliteio.Error
	if !errors.As(err, &n) || (n.Code != 1555 && n.Code != 2067) {
		t.Fatal("PK/UNIQUE refusal lost exact native evidence", err)
	}
}
func fpiQASame(t *testing.T, a, b any) {
	t.Helper()
	if !reflect.DeepEqual(a, b) {
		t.Fatal("owned canonical value differs")
	}
}
func fpiQATimeOracle(t *testing.T, raw time.Time) time.Time {
	t.Helper()
	b, err := raw.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var saved time.Time
	if err := json.Unmarshal(b, &saved); err != nil {
		t.Fatal(err)
	}
	return saved
}
func fpiQAShadow(t *testing.T, tx *sqliteio.Tx, table, cols string) { asQAShadow(t, tx, table, cols) }
func fpiQAFill(t *testing.T, tx *sqliteio.Tx, f sqliteFrontierLocalRow, p sqlitePendingLocalRow, r sqliteIntervalLocalRow) {
	asQABindFixture(t, tx, "union_frontier", fpiQAFrontierCols, fpiQAFrontierValues(t, f))
	asQABindFixture(t, tx, "pending_finalization", fpiQAPendingCols, fpiQAPendingValues(t, p))
	asQABindFixture(t, tx, "intervals", fpiQAIntervalCols, fpiQAIntervalValues(t, r))
}
func fpiQAAxis(t *testing.T, r sqliteFrontierLocalRow, axis string) sqliteFrontierLocalRow {
	switch axis {
	case "id":
		r.ID = fpiQAID(900)
	case "computer":
		r.ComputerID = asQAForeign
	case "account":
		r.Attribution.AccountID = "11"
	case "user":
		r.Attribution.UserID = "12"
	case "project":
		r.Attribution.ProjectID = "13"
	case "task":
		r.Attribution.TaskID = "14"
	case "timezone":
		r.Attribution.Timezone = "Etc/UTC"
	case "start-sec":
		r.Start = r.Start.Add(-time.Second)
	case "start-nsec":
		r.Start = r.Start.Add(time.Nanosecond)
	case "start-json":
		r.Start = r.Start.In(time.FixedZone("named-offset", 3600))
	case "end-sec":
		r.End = r.End.Add(time.Second)
	case "end-nsec":
		r.End = r.End.Add(time.Nanosecond)
	case "end-json":
		r.End = r.End.In(time.FixedZone("named-offset", -3600))
	default:
		t.Fatal("unknown independent scalar old axis", axis)
	}
	return r
}
func fpiQAAxes() []string {
	return []string{"id", "computer", "account", "user", "project", "task", "timezone", "start-sec", "start-nsec", "start-json", "end-sec", "end-nsec", "end-json"}
}
func fpiQAPendingFrom(r sqliteFrontierLocalRow) sqlitePendingLocalRow {
	return sqlitePendingLocalRow{ID: r.ID, ComputerID: r.ComputerID, Attribution: r.Attribution, Start: r.Start, End: r.End}
}
func fpiQAIDs(rows []sqliteFrontierLocalRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}
func fpiQAColumns(cols string) []string { return strings.Split(cols, ",") }

// An ordered SCAN of a small pending index is legitimate; the existing generic
// actor helper intentionally requires SEARCH and cannot be used for this case.
func fpiQAOrderedPlan(t *testing.T, tx *sqliteio.Tx, sql, index string) {
	t.Helper()
	stmt := interopPrepare(t, tx, "EXPLAIN QUERY PLAN "+sql)
	details := []string{}
	for {
		row, err := stmt.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !row {
			break
		}
		text, err := stmt.Text(3)
		if err != nil {
			t.Fatal(err)
		}
		details = append(details, text)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, index) || strings.Contains(joined, "TEMP B-TREE") {
		t.Fatal("ordered pending index plan missing or sorting", joined)
	}
}
