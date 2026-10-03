//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent composer QA uses complete strict legacy states only as oracles.
// All SQL dependencies are staged by actual approved Local writers. The fixture
// never substitutes a reducer, graph validator, schema or transaction adapter.
import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const cfQAFrontierColumns = "component_id,computer_id,account_id,user_id,project_id,task_id,timezone,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json"
const cfQAPendingColumns = "component_id,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json"
const cfQAIntervalColumns = "interval_id,computer_id,account_id,user_id,project_id,task_id,timezone,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,duration_ns,creation_ordinal"
const cfQAOutboxColumns = "interval_id,id,revision,state,correlation,entry_id,failure_category,retry_request_id,run_request_id,plan_present"

type cfQASpec struct {
	N           int
	Start, End  time.Time
	Attribution Attribution
}
type cfQAComponent struct {
	ID          string
	Attribution Attribution
	Start, End  time.Time
	IDs         []string
}

func cfQAUUID(n int) string { return fmt.Sprintf("70000000-0000-4000-8000-%012x", n) }
func cfQAAttr() Attribution {
	return Attribution{AccountID: "1", UserID: "2", ProjectID: "3", TaskID: "4", Timezone: "UTC"}
}
func cfQATime(n int) time.Time { return qaEpochStart.Add(time.Duration(n) * time.Second) }
func cfQASample(at time.Time) ClockSample {
	epoch := "cf-boot"
	ns := strconv.FormatInt(at.Sub(qaEpochStart).Nanoseconds(), 10)
	return ClockSample{Capability: "available", WallUTC: at, Epoch: &epoch, ElapsedNS: &ns, AwakeNS: &ns}
}
func cfQAState(t *testing.T, specs ...cfQASpec) *state {
	t.Helper()
	h := qaNew(t)
	h.seed()
	st := bgQAReadLegacy(t, h.service)
	for _, s := range specs {
		end := s.End
		ref := ActorRef{Key: ActorKey{ComputerID: st.ComputerID, Source: "manual-test", SessionID: "cf-session", AgentID: fmt.Sprint("support-", s.N)}, Generation: "1"}
		ep := &timelineEpoch{ID: cfQAUUID(10000 + s.N), ComputerID: st.ComputerID, Attribution: s.Attribution, Anchor: cfQASample(s.Start)}
		st.Epochs = append(st.Epochs, ep)
		st.Segments[cfQAUUID(s.N)] = &segment{ID: cfQAUUID(s.N), Actor: ref, Binding: BindingSnapshot{ID: qaBindingA, Revision: "1", Attribution: s.Attribution}, EpochID: ep.ID, StartSample: cfQASample(s.Start), ConfirmedSample: cfQASample(s.End), Start: s.Start, Confirmed: s.End, End: &end, EventReferences: []string{}}
	}
	return hnQAValid(t, st)
}
func cfQAHash(computer string, a Attribution, ids []string) string {
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	if len(sorted) == 0 {
		panic("no positive support")
	}
	h := sha256.New()
	_, _ = h.Write([]byte("tempo-frontier-component-v1\x00"))
	for _, s := range []string{computer, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, sorted[0]} {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(len(s)))
		_, _ = h.Write(b[:])
		_, _ = h.Write([]byte(s))
	}
	return fmt.Sprintf("fc1:%x", h.Sum(nil))
}

// This oracle implements the ordered interval sweep independently. It does not
// call mergeRanges, finalize, any production component-ID or charge helper.
func cfQAComponents(st *state) []cfQAComponent {
	rows := []cfQAComponent{}
	for _, s := range st.Segments {
		if s.Finalized || s.End == nil && s.UncertaintyID == nil {
			continue
		}
		end := s.Confirmed
		if s.End != nil {
			end = *s.End
		}
		if !end.After(s.Start) {
			continue
		}
		rows = append(rows, cfQAComponent{Attribution: s.Binding.Attribution, Start: s.Start, End: end, IDs: []string{s.ID}})
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		ka, kb := cfQAGroup(st.ComputerID, a.Attribution), cfQAGroup(st.ComputerID, b.Attribution)
		if ka != kb {
			return ka < kb
		}
		if !a.Start.Equal(b.Start) {
			return a.Start.Before(b.Start)
		}
		if !a.End.Equal(b.End) {
			return a.End.Before(b.End)
		}
		return a.IDs[0] < b.IDs[0]
	})
	out := []cfQAComponent{}
	for _, r := range rows {
		if len(out) == 0 || out[len(out)-1].Attribution != r.Attribution || r.Start.After(out[len(out)-1].End) {
			out = append(out, r)
			continue
		}
		last := &out[len(out)-1]
		if r.End.After(last.End) {
			last.End = r.End
		}
		last.IDs = append(last.IDs, r.IDs...)
	}
	for i := range out {
		sort.Strings(out[i].IDs)
		out[i].ID = cfQAHash(st.ComputerID, out[i].Attribution, out[i].IDs)
	}
	return out
}
func cfQAAudit(t *testing.T, tx *sqliteio.Tx, m sqliteStoreMeta) (map[string][][]string, int64) {
	t.Helper()
	snap, total := sdQAAudit(t, tx, m)
	for _, r := range []struct{ table, cols, order string }{{"union_frontier", cfQAFrontierColumns, "component_id"}, {"pending_finalization", cfQAPendingColumns, "component_id"}, {"component_segments", "component_id,segment_id", "component_id,segment_id"}, {"intervals", cfQAIntervalColumns, "creation_ordinal"}, {"interval_segments", "interval_id,ordinal,segment_id", "interval_id,ordinal"}, {"interval_components", "interval_id,component_id", "interval_id,component_id"}, {"outbox", cfQAOutboxColumns, "interval_id"}} {
		rows, n := asQALiteralAudit(t, tx, r.table, r.cols, r.order)
		snap[r.table] = rows
		total += n
	}
	// These are expected empty in capture fixtures, not omitted from the audit.
	for _, table := range []string{"sync_configurations", "sync_plans", "sync_parts", "sync_attempts", "pending_sync", "pending_sync_roots"} {
		if interopCount(t, tx, "SELECT COUNT(*) FROM "+table) != 0 {
			t.Fatalf("capture created unrelated %s", table)
		}
	}
	return snap, total
}
func cfQASeed(t *testing.T, st *state) (interopFixture, sqliteStoreMeta, map[string][][]string) {
	t.Helper()
	f, m, _, _ := sdQASeed(t, st)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	snap, n := cfQAAudit(t, tx, m)
	if n != m.LogicalBytes {
		t.Fatal("baseline literal charge differs")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	return f, m, snap
}
func cfQAReopen(t *testing.T, f interopFixture, m sqliteStoreMeta, snap map[string][][]string) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	got, n := cfQAAudit(t, tx, m)
	stored, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(stored, m) || n != m.LogicalBytes || !reflect.DeepEqual(got, snap) {
		t.Fatal("cold complete rows/metadata/nonce/charge differ", err)
	}
	cfQAColdClosure(t, tx, m, len(snap["union_frontier"])+len(snap["intervals"]) > 0)
	interopRollback(t, tx)
	interopClose(t, c)
}
func cfQACommit(t *testing.T, tx *sqliteio.Tx, old sqliteStoreMeta, delta int64) (sqliteStoreMeta, map[string][][]string) {
	t.Helper()
	next := metaQANext(t, old)
	next.Revision = bump(old.Revision)
	next.LogicalBytes += delta
	if err := sqliteUpdateMeta(tx, old, next); err != nil {
		t.Fatal(err)
	}
	snap, n := cfQAAudit(t, tx, next)
	if n != next.LogicalBytes {
		t.Fatalf("independent charge=%d meta=%d reported delta=%d", n, next.LogicalBytes, delta)
	}
	interopCommit(t, tx)
	return next, snap
}
func cfQAChargeJSON(t *testing.T, at time.Time) int64 {
	t.Helper()
	b, err := json.Marshal(at)
	if err != nil {
		t.Fatal(err)
	}
	return int64(len(b))
}
func cfQARangeCharge(t *testing.T, base int64, id, computer string, a Attribution, start, end time.Time) int64 {
	n := base
	for _, s := range []string{id, computer, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, cfQAGroup(computer, a)} {
		n += int64(len(s))
	}
	return n + cfQAChargeJSON(t, start) + cfQAChargeJSON(t, end)
}
func cfQAFrontierCharge(t *testing.T, computer string, r cfQAComponent) int64 {
	return cfQARangeCharge(t, 158, r.ID, computer, r.Attribution, r.Start, r.End)
}
func cfQAPendingCharge(t *testing.T, computer string, r cfQAComponent) int64 {
	return int64(104+len(r.ID)+len(cfQAGroup(computer, r.Attribution))) + cfQAChargeJSON(t, r.Start) + cfQAChargeJSON(t, r.End)
}
func cfQALiveCharge(t *testing.T, computer string, rows []cfQAComponent) int64 {
	var n int64
	for _, r := range rows {
		n += cfQAFrontierCharge(t, computer, r) + cfQAPendingCharge(t, computer, r)
		for _, id := range r.IDs {
			n += int64(50 + len(r.ID) + len(id))
		}
	}
	return n
}
func cfQAFrontiers(t *testing.T, tx *sqliteio.Tx, st *state, want []cfQAComponent, pending bool) {
	t.Helper()
	if interopCount(t, tx, "SELECT COUNT(*) FROM union_frontier") != int64(len(want)) {
		t.Fatal("frontier cardinality")
	}
	var members int64
	for _, r := range want {
		got, found, err := sqliteReadFrontierLocal(tx, st.ComputerID, r.ID)
		if err != nil || !found || got.Attribution != r.Attribution {
			t.Fatal("frontier owner", err)
		}
		asQATimeWitness(t, r.Start, got.Start)
		asQATimeWitness(t, r.End, got.End)
		ids, err := sqliteComponentSegmentIDsLocal(tx, st.ComputerID, r.ID)
		if err != nil || !reflect.DeepEqual(ids, r.IDs) {
			t.Fatal("exact sorted membership", ids, err)
		}
		members += int64(len(ids))
		p, found, err := sqliteReadPendingLocal(tx, st.ComputerID, r.ID)
		if err != nil || found != pending {
			t.Fatal("pending presence", err)
		}
		if found {
			asQATimeWitness(t, r.Start, p.Start)
			asQATimeWitness(t, r.End, p.End)
			if p.Attribution != r.Attribution {
				t.Fatal("pending group")
			}
		}
	}
	if interopCount(t, tx, "SELECT COUNT(*) FROM component_segments") != members {
		t.Fatal("orphan or duplicate live support")
	}
}
func cfQACode(t *testing.T, err error, code string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("want %s got %v", code, err)
	}
}
func cfQANative(t *testing.T, err error, code int32) {
	t.Helper()
	var e *sqliteio.Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("want native %d got %v", code, err)
	}
}
func cfQARefreshAll(t *testing.T, tx *sqliteio.Tx, st *state) int64 {
	t.Helper()
	ids := make([]string, 0, len(st.Segments))
	for id := range st.Segments {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var total int64
	for _, id := range ids {
		r, err := sqliteRefreshSegmentFrontier(tx, st.ComputerID, nil, asQASegment(st.Segments[id]))
		if err != nil {
			t.Fatal("real refresh", err)
		}
		if !cfQAContains(r.Selection.ChangedSegmentIDs, id) {
			t.Fatal("refresh lost changed seed")
		}
		total += r.Delta
	}
	want := cfQAComponents(st)
	cfQAFrontiers(t, tx, st, want, true)
	if total != cfQALiveCharge(t, st.ComputerID, want) {
		t.Fatalf("refresh literal delta=%d want=%d", total, cfQALiveCharge(t, st.ComputerID, want))
	}
	return total
}
func cfQAContains(ids []string, id string) bool {
	for _, got := range ids {
		if got == id {
			return true
		}
	}
	return false
}
func cfQASelectedLive(st *state, rows []cfQAComponent) sqliteFinalizationSelection {
	s := sqliteFinalizationSelection{ChangedSegmentIDs: []string{}, FrontierIDs: []string{}, RequiredPendingIDs: []string{}, RemovedFrontierIDs: []string{}, ClearedPendingIDs: []string{}, NewSeals: []sqliteSealSelection{}}
	for id := range st.Segments {
		s.ChangedSegmentIDs = append(s.ChangedSegmentIDs, id)
	}
	sort.Strings(s.ChangedSegmentIDs)
	for _, r := range rows {
		s.FrontierIDs = append(s.FrontierIDs, r.ID)
		s.RequiredPendingIDs = append(s.RequiredPendingIDs, r.ID)
	}
	return s
}
func cfQAValidate(t *testing.T, tx *sqliteio.Tx, st *state, selected sqliteFinalizationSelection) {
	t.Helper()
	if err := sqliteValidateSelectedFinalization(tx, st.ComputerID, st.Revision, selected); err != nil {
		t.Fatal("actual finalization graph", err)
	}
	selection := sdQASelection(st)
	selection.ChangedSegmentIDs = selected.ChangedSegmentIDs
	if err := sqliteValidateSelectedCaptureDependencies(tx, st.ComputerID, st.Revision, selection); err != nil {
		t.Fatal("actual complete selected dependency closure", err)
	}
}
func cfQASealed(t *testing.T, tx *sqliteio.Tx, st *state, want []cfQAComponent, result sqliteFinalizationResult) int64 {
	t.Helper()
	if result.OperationError != nil || len(result.Selection.NewSeals) != len(want) {
		t.Fatal("seal count/domain outcome", result.OperationError)
	}
	base := interopCount(t, tx, "SELECT COUNT(*) FROM intervals") - int64(len(want))
	if base < 0 {
		t.Fatal("retained interval cardinality")
	}
	var delta int64
	for i, r := range want {
		seal := result.Selection.NewSeals[i]
		if seal.ComponentID != r.ID || seal.IntervalID == seal.OutboxID || !validUUID(seal.IntervalID) || !validUUID(seal.OutboxID) {
			t.Fatal("seal identities/order")
		}
		in, found, err := sqliteReadIntervalLocal(tx, st.ComputerID, seal.IntervalID)
		if err != nil || !found || in.Attribution != r.Attribution || in.Ordinal != base+int64(i) || in.DurationNS != strconv.FormatInt(r.End.Sub(r.Start).Nanoseconds(), 10) {
			t.Fatal("interval scalar/billing", err)
		}
		asQATimeWitness(t, r.Start, in.Start)
		asQATimeWitness(t, r.End, in.End)
		ids, err := sqliteIntervalSegmentIDsLocal(tx, st.ComputerID, in.ID)
		if err != nil || !reflect.DeepEqual(ids, r.IDs) {
			t.Fatal("exact immutable ordered supports", ids, err)
		}
		component, found, err := sqliteReadIntervalComponent(tx, st.ComputerID, in.ID)
		if err != nil || !found || component != r.ID {
			t.Fatal("singular retained seal", err)
		}
		root, found, err := sqliteReadOutboxLocal(tx, st.ComputerID, in.ID)
		if err != nil || !found || root.ID != seal.OutboxID || root.Revision != "1" || root.State != "queued" || root.Correlation != "tempo:"+in.ID || root.EntryID != nil || root.FailureCategory != nil || root.RetryRequestID != nil || root.RunRequestID != nil || root.PlanPresent {
			t.Fatal("exact initial root", err)
		}
		delta += cfQARangeCharge(t, 184, in.ID, st.ComputerID, r.Attribution, r.Start, r.End) + 154 + 218 - cfQAFrontierCharge(t, st.ComputerID, r) - cfQAPendingCharge(t, st.ComputerID, r)
		for _, id := range ids {
			seg, found, err := sqliteReadSegmentLocal(tx, st.ComputerID, id)
			before := asQASegment(st.Segments[id])
			before.Finalized = true
			if err != nil || !found || !reflect.DeepEqual(seg, before) {
				t.Fatal("only segment finalized flag changed", err)
			}
			delta += int64(59+len(in.ID)+len(id)) - int64(50+len(r.ID)+len(id))
		}
	}
	for _, table := range []string{"union_frontier", "pending_finalization", "component_segments"} {
		if interopCount(t, tx, "SELECT COUNT(*) FROM "+table) != 0 {
			t.Fatal("live owner survived seal", table)
		}
	}
	if delta != result.Delta {
		t.Fatalf("exact seal delta=%d reported=%d", delta, result.Delta)
	}
	cfQAValidate(t, tx, st, result.Selection)
	return delta
}
func cfQADrain(t *testing.T, tx *sqliteio.Tx, st *state) sqliteFinalizationResult {
	t.Helper()
	r, err := sqliteDrainFinalization(tx, st.ComputerID, st.Revision)
	if err != nil {
		t.Fatal("drain storage failure", err)
	}
	return r
}
func cfQAClearPending(t *testing.T, tx *sqliteio.Tx, st *state) int64 {
	var total int64
	for _, r := range cfQAComponents(st) {
		p, found, err := sqliteReadPendingLocal(tx, st.ComputerID, r.ID)
		if err != nil || !found {
			t.Fatal(err)
		}
		n, err := sqliteDeletePendingLocal(tx, st.ComputerID, p)
		if err != nil {
			t.Fatal(err)
		}
		if n != -cfQAPendingCharge(t, st.ComputerID, r) {
			t.Fatal("pending exact negative charge")
		}
		total += n
	}
	return total
}
func cfQARawPart(t *testing.T, tx *sqliteio.Tx, interval, id string) error {
	t.Helper()
	values := []sqliteio.Value{sqliteio.Text(interval), sqliteio.Integer(0), sqliteio.Text(id), sqliteio.Text("2026-10-02"), sqliteio.Integer(int64(2 * time.Second))}
	values = append(values, asQATime(t, cfQATime(0))...)
	values = append(values, asQATime(t, cfQATime(2))...)
	values = append(values, sqliteio.Text("0.0005555555555555556"), sqliteio.Integer(int64(2*time.Second)), sqliteio.Integer(0), sqliteio.Null(), sqliteio.Null(), sqliteio.Text("tempo:v1:"+id), sqliteio.Text("owned fixture"), sqliteio.Text("queued"), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null(), sqliteio.Null())
	stmt, err := tx.Prepare("INSERT INTO sync_parts(interval_id,ordinal,id,spent_date,duration_ns,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,planned_hours,planned_duration_ns,planned_residual_ns,started_time,ended_time,correlation,notes,state,entry_id,failure_category,returned_hours,rounded_hours,confirmed_duration_ns,provider_delta_ns,total_residual_ns,attachment_request_id,attachment_entry_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", values...)
	if err != nil {
		return err
	}
	row, err := stmt.Step()
	closeErr := stmt.Close()
	if row {
		t.Fatal("unexpected INSERT row")
	}
	return errors.Join(err, closeErr)
}
func cfQARawAttempt(t *testing.T, tx *sqliteio.Tx, interval, id string) error {
	stmt, err := tx.Prepare("INSERT INTO sync_attempts(interval_id,part_ordinal,ordinal,request_id,id,number,state,entry_id,failure_category) VALUES(?,?,?,?,?,?,?,?,?)", sqliteio.Text(interval), sqliteio.Integer(0), sqliteio.Integer(0), sqliteio.Text(cfQAUUID(7777)), sqliteio.Text(id), sqliteio.Text("1"), sqliteio.Text("unknown"), sqliteio.Null(), sqliteio.Null())
	if err != nil {
		return err
	}
	row, err := stmt.Step()
	closeErr := stmt.Close()
	if row {
		t.Fatal("unexpected INSERT row")
	}
	return errors.Join(err, closeErr)
}
func cfQAGroup(computer string, a Attribution) string {
	b, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}
	return computer + "/" + string(b)
}
func cfQAStrings(t *testing.T, tx *sqliteio.Tx, query string) []string {
	t.Helper()
	s := interopPrepare(t, tx, query)
	ids := []string{}
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !row {
			break
		}
		kind, err := s.Kind(0)
		if err != nil || kind != sqliteio.TextKind || s.ColumnCount() != 1 {
			t.Fatal("cold identity projection/kind", err)
		}
		id, err := s.Text(0)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return ids
}
func cfQAColdClosure(t *testing.T, tx *sqliteio.Tx, m sqliteStoreMeta, derived bool) {
	t.Helper()
	ids := cfQAStrings(t, tx, "SELECT segment_id FROM segments ORDER BY segment_id")
	if err := sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sqliteDependencySelection{ChangedSegmentIDs: ids}); err != nil {
		t.Fatal("cold real dependency closure", err)
	}
	// The pre-refresh fixture baseline is an explicitly labeled dependency-only
	// setup. Only successfully committed derived/sealed outcomes use this audit.
	if !derived {
		return
	}
	s := sqliteFinalizationSelection{ChangedSegmentIDs: ids, FrontierIDs: []string{}, RequiredPendingIDs: []string{}, RemovedFrontierIDs: []string{}, ClearedPendingIDs: []string{}, NewSeals: []sqliteSealSelection{}}
	for _, id := range cfQAStrings(t, tx, "SELECT component_id FROM union_frontier ORDER BY component_id") {
		s.FrontierIDs = append(s.FrontierIDs, id)
		_, found, err := sqliteReadPendingLocal(tx, m.ComputerID, id)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			s.RequiredPendingIDs = append(s.RequiredPendingIDs, id)
		} else {
			s.ClearedPendingIDs = append(s.ClearedPendingIDs, id)
		}
	}
	for _, id := range cfQAStrings(t, tx, "SELECT interval_id FROM intervals ORDER BY creation_ordinal") {
		component, found, err := sqliteReadIntervalComponent(tx, m.ComputerID, id)
		if err != nil || !found {
			t.Fatal("cold singular seal", err)
		}
		root, found, err := sqliteReadOutboxLocal(tx, m.ComputerID, id)
		if err != nil || !found {
			t.Fatal("cold root", err)
		}
		s.RemovedFrontierIDs = append(s.RemovedFrontierIDs, component)
		s.NewSeals = append(s.NewSeals, sqliteSealSelection{ComponentID: component, IntervalID: id, OutboxID: root.ID})
	}
	if err := sqliteValidateSelectedFinalization(tx, m.ComputerID, m.Revision, s); err != nil {
		t.Fatal("cold complete derived/sealed graph", err)
	}
}
