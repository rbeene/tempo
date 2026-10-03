//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

// Native relational query fixtures only: actual STRICT schema, owned literal
// projections, complete FK targets, checked COMMIT/reopen. No claim of complete
// billing, evidence/reducer graph, seal implementation or Service validity.
import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

const iwName = "capture-query-work.sqlite3"
const iwComputer = "11111111-1111-4111-8111-111111111111"
const iwNegativeComputer = "22222222-2222-4222-8222-222222222222"
const iwCurrent = "88888888-8888-4888-8888-888888888888"
const iwLower = "00000000-0000-4000-8000-000000000001"
const iwHigher = "ffffffff-ffff-4fff-8fff-ffffffffffff"
const iwOtherInc = "44444444-4444-4444-8444-444444444444"
const iwNative = "retained-current"
const iwActorCols = "actor_key,id,revision,generation,sequence,state,health,binding_id,binding_revision,account_id,user_id,project_id,task_id,timezone,computer_id,parent_key,parent_generation,segment_id,last_evidence_capability,last_evidence_wall_sec,last_evidence_wall_nsec,last_evidence_wall_json,last_evidence_epoch,last_evidence_elapsed_raw,last_evidence_awake_raw,last_evidence_elapsed,last_evidence_awake"
const iwSegmentCols = "segment_id,actor_key,actor_generation,binding_id,binding_revision,account_id,user_id,project_id,task_id,timezone,computer_id,group_order,epoch_id,start_sample_capability,start_sample_wall_sec,start_sample_wall_nsec,start_sample_wall_json,start_sample_epoch,start_sample_elapsed_raw,start_sample_awake_raw,start_sample_elapsed,start_sample_awake,confirmed_sample_capability,confirmed_sample_wall_sec,confirmed_sample_wall_nsec,confirmed_sample_wall_json,confirmed_sample_epoch,confirmed_sample_elapsed_raw,confirmed_sample_awake_raw,confirmed_sample_elapsed,confirmed_sample_awake,start_sec,start_nsec,start_json,confirmed_sec,confirmed_nsec,confirmed_json,end_sec,end_nsec,end_json,uncertainty_id,finalized"
const iwUncertaintyCols = "uncertainty_id,revision,actor_key,actor_generation,segment_id,account_id,user_id,project_id,task_id,timezone,computer_id,lower_bound_sec,lower_bound_nsec,lower_bound_json,upper_bound_sec,upper_bound_nsec,upper_bound_json,reason,state,resolution_end_sec,resolution_end_nsec,resolution_end_json,discarded"
const iwFrontierCols = "component_id,computer_id,account_id,user_id,project_id,task_id,timezone,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json"
const iwIntervalCols = "interval_id,computer_id,account_id,user_id,project_id,task_id,timezone,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,duration_ns,creation_ordinal"
const iwDecisionCols = "uncertainty_id,request_id,previous_revision,resolution_end_sec,resolution_end_nsec,resolution_end_json,discarded,reason,observed_capability,observed_wall_sec,observed_wall_nsec,observed_wall_json,observed_epoch,observed_elapsed_raw,observed_awake_raw,discarded_start_sec,discarded_start_nsec,discarded_start_json,discarded_end_sec,discarded_end_nsec,discarded_end_json"

type iwActorIdentity struct {
	ComputerID string `json:"computer_id"`
	Source     string `json:"source"`
	SessionID  string `json:"session_id"`
	AgentID    string `json:"agent_id"`
}
type iwAttribution struct {
	AccountID string `json:"account_id"`
	UserID    string `json:"user_id"`
	ProjectID string `json:"project_id"`
	TaskID    string `json:"task_id"`
	Timezone  string `json:"timezone"`
}

var iwAttr = iwAttribution{"1", "2", "3", "4", "UTC"}

func iwUUID(family, n int) string { return fmt.Sprintf("%08x-0000-4000-8000-%012x", family, n+1) }
func iwFC(n int) string {
	h := sha256.Sum256([]byte(fmt.Sprint("query-frontier-", n)))
	return fmt.Sprintf("fc1:%x", h)
}
func iwJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
func iwHash(v any) string { h := sha256.Sum256([]byte(iwJSON(v))); return fmt.Sprintf("%x", h) }
func iwOutboxID(id string) string {
	h := iwHash([]string{"outbox", id})
	return h[:8] + "-" + h[8:12] + "-4" + h[13:16] + "-8" + h[17:20] + "-" + h[20:32]
}
func iwCounter(n uint64) Value { var b [8]byte; binary.BigEndian.PutUint64(b[:], n); return Blob(b[:]) }
func iwClock(sec int64) []Value {
	return []Value{Text("available"), Integer(sec), Integer(0), Text(iwTimeJSON(sec, 0)), Text("query-boot"), Text("0"), Text("0"), iwCounter(0), iwCounter(0)}
}
func iwTime(sec int64, nsec int64) []Value {
	return []Value{Integer(sec), Integer(nsec), Text(iwTimeJSON(sec, nsec))}
}
func iwTimeJSON(sec, nsec int64) string {
	b, err := time.Unix(sec, nsec).UTC().MarshalJSON()
	if err != nil {
		panic(err)
	}
	return string(b)
}
func iwGroup(computer string, a iwAttribution) string { return computer + "/" + iwJSON(a) }
func iwIdentity(n int, computer string) iwActorIdentity {
	if n == 2 {
		return iwActorIdentity{computer, "codex", iwOtherInc, "child:" + base64.RawURLEncoding.EncodeToString([]byte("gen-reserved"))}
	}
	return iwActorIdentity{computer, "manual-test", fmt.Sprint("query-session-", n), "actor"}
}
func iwKey(n int) string { return iwJSON(iwIdentity(n, iwComputer)) }
func iwBind(t *testing.T, tx *Tx, table, cols string, v []Value) {
	t.Helper()
	width := len(strings.Split(cols, ","))
	if len(v) != width {
		t.Fatalf("literal %s width=%d want=%d", table, len(v), width)
	}
	qaDone(t, tx, "INSERT INTO "+table+"("+cols+") VALUES("+strings.TrimSuffix(strings.Repeat("?,", width), ",")+")", v...)
}
func iwGeneration(t *testing.T, tx *Tx, id iwActorIdentity, g uint64) {
	iwBind(t, tx, "actor_generations", "actor_key,generation,computer_id,source,session_id,agent_id", []Value{Text(iwJSON(id)), iwCounter(g), Text(id.ComputerID), Text(id.Source), Text(id.SessionID), Text(id.AgentID)})
}
func iwActor(t *testing.T, tx *Tx, id iwActorIdentity, uuid, state, segment string) {
	v := []Value{Text(iwJSON(id)), Text(uuid), iwCounter(1), iwCounter(1), iwCounter(1), Text(state), Text("continuous"), Text(iwUUID(8, 0)), iwCounter(1), Text("1"), Text("2"), Text("3"), Text("4"), Text("UTC"), Text(id.ComputerID), Null(), Null(), Null()}
	if segment != "" {
		v[17] = Text(segment)
	}
	v = append(v, iwClock(0)...)
	iwBind(t, tx, "actors", iwActorCols, v)
}
func iwSegment(t *testing.T, tx *Tx, id string, actor iwActorIdentity, g uint64, a iwAttribution, start, end int64, u string, finalized bool) {
	v := []Value{Text(id), Text(iwJSON(actor)), iwCounter(g), Text(iwUUID(8, 0)), iwCounter(1), Text(a.AccountID), Text(a.UserID), Text(a.ProjectID), Text(a.TaskID), Text(a.Timezone), Text(actor.ComputerID), Text(iwGroup(actor.ComputerID, a)), Text(iwUUID(7, 0))}
	v = append(v, iwClock(start)...)
	v = append(v, iwClock(end)...)
	v = append(v, iwTime(start, 0)...)
	v = append(v, iwTime(end, 0)...)
	v = append(v, iwTime(end, 0)...)
	if u == "" {
		v = append(v, Null())
	} else {
		v = append(v, Text(u))
	}
	flag := int64(0)
	if finalized {
		flag = 1
	}
	v = append(v, Integer(flag))
	iwBind(t, tx, "segments", iwSegmentCols, v)
}
func iwUncertainty(t *testing.T, tx *Tx, id, segment string, actor iwActorIdentity, g uint64, a iwAttribution, lower int64, upper *int64, resolved bool) {
	revision := uint64(1)
	state := "unresolved"
	if resolved {
		revision = 2
		state = "resolved"
	}
	v := []Value{Text(id), iwCounter(revision), Text(iwJSON(actor)), iwCounter(g), Text(segment), Text(a.AccountID), Text(a.UserID), Text(a.ProjectID), Text(a.TaskID), Text(a.Timezone), Text(actor.ComputerID)}
	v = append(v, iwTime(lower, 0)...)
	if upper == nil {
		v = append(v, Null(), Null(), Null())
	} else {
		v = append(v, iwTime(*upper, 0)...)
	}
	v = append(v, Text("source_lost"), Text(state))
	if resolved {
		v = append(v, iwTime(lower, 0)...)
	} else {
		v = append(v, Null(), Null(), Null())
	}
	v = append(v, Integer(0))
	iwBind(t, tx, "uncertainties", iwUncertaintyCols, v)
}
func iwFrontier(t *testing.T, tx *Tx, n int, start, end int64, a iwAttribution, pending bool) {
	v := []Value{Text(iwFC(n)), Text(iwComputer), Text(a.AccountID), Text(a.UserID), Text(a.ProjectID), Text(a.TaskID), Text(a.Timezone), Text(iwGroup(iwComputer, a))}
	v = append(v, iwTime(start, 0)...)
	v = append(v, iwTime(end, 0)...)
	iwBind(t, tx, "union_frontier", iwFrontierCols, v)
	if pending {
		p := []Value{Text(iwFC(n)), Text(iwGroup(iwComputer, a))}
		p = append(p, iwTime(start, 0)...)
		p = append(p, iwTime(end, 0)...)
		iwBind(t, tx, "pending_finalization", "component_id,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json", p)
	}
}
func iwInterval(t *testing.T, tx *Tx, id, segment string, start, end, ordinal int64, a iwAttribution) {
	v := []Value{Text(id), Text(iwComputer), Text(a.AccountID), Text(a.UserID), Text(a.ProjectID), Text(a.TaskID), Text(a.Timezone), Text(iwGroup(iwComputer, a))}
	v = append(v, iwTime(start, 0)...)
	v = append(v, iwTime(end, 0)...)
	v = append(v, iwCounter(uint64((end-start)*int64(time.Second))), Integer(ordinal))
	iwBind(t, tx, "intervals", iwIntervalCols, v)
	iwBind(t, tx, "interval_segments", "interval_id,ordinal,segment_id", []Value{Text(id), Integer(0), Text(segment)})
	iwBind(t, tx, "outbox", "interval_id,id,revision,state,correlation,entry_id,failure_category,retry_request_id,run_request_id,plan_present", []Value{Text(id), Text(iwOutboxID(id)), iwCounter(1), Text("queued"), Text("tempo:" + id), Null(), Null(), Null(), Null(), Integer(0)})
}
func iwTurn(t *testing.T, tx *Tx, source, native, inc, turn, agent string, actor *iwActorIdentity, g uint64, stopped bool) string {
	key := iwHash([]string{inc, turn, agent})
	v := []Value{Text(key), Text(source), Text(native), Text(inc), Text(turn), Text(agent), Text("/query-owned"), Null(), Null(), Integer(0)}
	if actor != nil {
		v[7], v[8] = Text(iwJSON(*actor)), iwCounter(g)
	}
	if stopped {
		v[9] = Integer(1)
	}
	iwBind(t, tx, "host_turns", "turn_key,source,native_session,incarnation,turn_id,agent_id,cwd,actor_key,actor_generation,stopped", v)
	return key
}
func iwSeed(t *testing.T, n int) string {
	t.Helper()
	dir := qaDirectory(t)
	c := qaOpen(t, dir, iwName, true)
	tx := qaBegin(t, c, context.Background(), Write)
	schema, err := os.ReadFile("../sqlite_schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.InstallSchema(string(schema)); err != nil {
		t.Fatal(err)
	}
	// No proposed index is installed here: adoption must be in production schema.
	iwBind(t, tx, "bindings", "binding_id,revision,account_id,user_id,project_id,task_id,timezone,computer_id,active,record_present,kind,locator,deleted", []Value{Text(iwUUID(8, 0)), iwCounter(1), Text("1"), Text("2"), Text("3"), Text("4"), Text("UTC"), Text(iwComputer), Integer(1), Integer(0), Null(), Null(), Null()})
	epoch := []Value{Text(iwUUID(7, 0)), Text(iwComputer), Text("1"), Text("2"), Text("3"), Text("4"), Text("UTC"), Integer(0), Text(iwGroup(iwComputer, iwAttr))}
	epoch = append(epoch, iwClock(0)...)
	iwBind(t, tx, "epochs", "epoch_id,computer_id,account_id,user_id,project_id,task_id,timezone,creation_ordinal,group_order,anchor_capability,anchor_wall_sec,anchor_wall_nsec,anchor_wall_json,anchor_epoch,anchor_elapsed_raw,anchor_awake_raw,anchor_elapsed,anchor_awake", epoch)
	for i, state := range []string{"working", "wait_user", "wait_children", "wait_permission"} {
		actor := iwIdentity(i, iwComputer)
		iwGeneration(t, tx, actor, 1)
		segment := ""
		if i == 0 {
			segment = iwUUID(1, 0)
		}
		iwActor(t, tx, actor, iwUUID(3, i), state, segment)
	}
	for i := 0; i < 4; i++ {
		a := iwAttr
		lower := int64(-1)
		var upper *int64
		if i == 1 {
			a.TaskID = "5"
			lower = 10
		}
		if i == 2 {
			a.UserID = "3"
			lower = -5
			x := int64(5)
			upper = &x
		}
		if i == 3 {
			lower = 10
			x := int64(20)
			upper = &x
		}
		actor := iwIdentity(0, iwComputer)
		if i >= 2 {
			actor = iwIdentity(1, iwComputer)
		}
		iwSegment(t, tx, iwUUID(1, i), actor, 1, a, -10, 0, iwUUID(2, i), false)
		iwUncertainty(t, tx, iwUUID(2, i), iwUUID(1, i), actor, 1, a, lower, upper, false)
	}
	for i := 0; i < n; i++ {
		for _, computer := range []string{iwComputer, iwNegativeComputer} {
			actor := iwIdentity(10000+i, computer)
			iwGeneration(t, tx, actor, 1)
			family := 30
			if computer == iwNegativeComputer {
				family = 31
			}
			iwActor(t, tx, actor, iwUUID(family, i), "finished", "")
		}
		start := int64(-100 - 4*(i/2))
		if i%2 == 1 {
			start = int64(100 + 4*(i/2))
		}
		end := start + 1
		// Timer nonoverlap is broader than full user/task/timezone attribution.
		historicalAttr := iwAttr
		if i%4 == 1 {
			historicalAttr.UserID = "5"
		}
		if i%4 == 2 {
			historicalAttr.TaskID = "6"
		}
		if i%4 == 3 {
			historicalAttr.Timezone = "Europe/London"
		}
		for owner := 0; owner < 2; owner++ {
			actor := iwIdentity(owner, iwComputer)
			g := uint64(i + 2)
			iwGeneration(t, tx, actor, g)
			sid, uid := iwUUID(40+owner, i), iwUUID(50+owner, i)
			iwSegment(t, tx, sid, actor, g, historicalAttr, start, end, uid, owner == 0)
			upper := end
			iwUncertainty(t, tx, uid, sid, actor, g, historicalAttr, start, &upper, true)
		}
		iwInterval(t, tx, iwUUID(60, i), iwUUID(40, i), start, end, int64(i), historicalAttr)
		iwBind(t, tx, "actor_uncertainties", "actor_key,ordinal,uncertainty_id", []Value{Text(iwKey(0)), Integer(int64(i)), Text(iwUUID(50, i))})
		iwTurn(t, tx, "claude", iwNative, iwCurrent, fmt.Sprintf("retained-%06d", i), "agent", nil, 0, true)
	}
	for i := 0; i < 2; i++ {
		iwBind(t, tx, "actor_uncertainties", "actor_key,ordinal,uncertainty_id", []Value{Text(iwKey(0)), Integer(int64(n + i)), Text(iwUUID(2, 0))})
	}
	// Latest immutable end stays fixed; append ordinal necessarily depends on N.
	iwSegment(t, tx, iwUUID(70, 0), iwIdentity(0, iwComputer), 1, iwAttr, 1000000000, 1000000001, "", true)
	iwInterval(t, tx, iwUUID(71, 0), iwUUID(70, 0), 1000000000, 1000000001, int64(n), iwAttr)
	bounds := [][2]int64{{-40, -30}, {-20, -10}, {10, 20}, {30, 40}, {50, 60}}
	support := 0
	for i, b := range bounds {
		a := iwAttr
		if i == 4 {
			a.TaskID = "5"
		}
		iwFrontier(t, tx, i, b[0], b[1], a, i < 4)
		count := 1
		if i == 0 {
			count = 4
		}
		for j := 0; j < count; j++ {
			id := iwUUID(80, support)
			support++
			iwSegment(t, tx, id, iwIdentity(3, iwComputer), 1, a, b[0], b[1], "", false)
			iwBind(t, tx, "component_segments", "component_id,segment_id", []Value{Text(iwFC(i)), Text(id)})
		}
	}
	reserved := iwIdentity(2, iwComputer)
	iwGeneration(t, tx, reserved, 9223372036854775807)
	iwGeneration(t, tx, reserved, 9223372036854775809)
	iwGeneration(t, tx, reserved, ^uint64(0))
	for i, g := range []uint64{1, 9223372036854775807, 9223372036854775809, 9223372036854775809} {
		iwTurn(t, tx, "codex", "generation-native", iwOtherInc, fmt.Sprint("generation-", i), "gen-reserved", &reserved, g, true)
	}
	iwGeneration(t, tx, iwIdentity(3, iwComputer), ^uint64(0))
	for _, host := range []struct{ source, native, inc string }{{"claude", iwNative, iwCurrent}, {"codex", "generation-native", iwOtherInc}} {
		iwBind(t, tx, "host_sessions", "source,native_session,session_key,incarnation,cwd,root_turn_key", []Value{Text(host.source), Text(host.native), Text(iwHash([]string{host.source, host.native})), Text(host.inc), Text("/query-owned"), Text("")})
	}
	// Typed structural mutation-result proof; this does not certify a Service
	// outcome. Native query25 only selects the exact request reverse-owner set.
	ids := []string{}
	for i := 0; i < 4; i++ {
		ids = append(ids, iwUUID(51, i))
	}
	rid := iwUUID(90, 0)
	payload := fmt.Sprintf(`{"contract_version":1,"snapshot_revision":"2","request_id":%q,"changed":true,"affected_ids":%s,"entity_revision":"2"}`, rid, iwJSON(ids))
	iwBind(t, tx, "requests", "request_id,operation,fingerprint,outcome_kind,payload", []Value{Text(rid), Text("activity.resolve"), Text(strings.Repeat("a", 64)), Text("mutation_result"), Text(payload)})
	for i, id := range ids {
		start := int64(-100 - 4*(i/2))
		if i%2 == 1 {
			start = int64(100 + 4*(i/2))
		}
		v := []Value{Text(id), Text(rid), iwCounter(1)}
		v = append(v, iwTime(start, 0)...)
		v = append(v, Integer(0), Text("fixture-owned"))
		v = append(v, iwClock(start)[:7]...)
		for j := 0; j < 6; j++ {
			v = append(v, Null())
		}
		iwBind(t, tx, "recovery_decisions", iwDecisionCols, v)
	}
	if support != 8 {
		t.Fatal("fixed connected support budget changed")
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal("real schema native FK fixture refused", err)
	}
	qaCommit(t, tx)
	qaClose(t, c)
	c = qaOpen(t, dir, iwName, false)
	tx = qaBegin(t, c, context.Background(), Read)
	iwFixtureInventory(t, tx, n)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
	return dir
}
func iwFixtureInventory(t *testing.T, tx *Tx, n int) {
	expected := map[string]int64{"bindings": 1, "epochs": 1, "actors": int64(2*n + 4), "actor_generations": int64(4*n + 8), "segments": int64(2*n + 13), "uncertainties": int64(2*n + 4), "actor_uncertainties": int64(n + 2), "host_sessions": 2, "host_turns": int64(n + 4), "union_frontier": 5, "component_segments": 8, "pending_finalization": 4, "intervals": int64(n + 1), "interval_segments": int64(n + 1), "outbox": int64(n + 1), "requests": 1, "recovery_decisions": 4}
	for table, want := range expected {
		got := capacityQAScalar(t, tx, "SELECT count(*) FROM "+table)
		if got != want {
			t.Fatalf("fixture %s count=%d want=%d", table, got, want)
		}
	}
	// Both Q19 one-sided range prefixes contain N/2 rows at the fixed gap.
	if got := capacityQAScalar(t, tx, "SELECT count(*) FROM intervals WHERE computer_id=? AND account_id='1' AND project_id='3' AND start_sec<1", Text(iwComputer)); got != int64(n/2) {
		t.Fatal("left Q19 prefix premise", got)
	}
	if got := capacityQAScalar(t, tx, "SELECT count(*) FROM intervals WHERE computer_id=? AND account_id='1' AND project_id='3' AND end_sec>0", Text(iwComputer)); got != int64(n/2+1) {
		t.Fatal("right Q19 prefix premise", got)
	}
}

// Independent complete fixture snapshot, outside target work measurement. The
// existing full strict range audit supplies nonoverlap/canonical prerequisites.
// This does not call the measured neighbor decoder or stand in for its result.
func iwQ19Snapshot(t *testing.T, tx *Tx) ([]iwRange, map[string][]string) {
	t.Helper()
	ranges, valid := iwAuditIntervals(t, tx)
	if !valid {
		t.Fatal("Q19 complete fixture nonoverlap/canonical prerequisite refused")
	}
	snapshot := map[string][]string{}
	s := qaPrepare(t, tx, "SELECT "+iwIntervalCols+" FROM intervals WHERE computer_id=? AND account_id=? AND project_id=? ORDER BY creation_ordinal", iwTimer()...)
	for {
		present, err := s.Step()
		if err != nil {
			t.Fatal("independent full16 snapshot step", err)
		}
		if !present {
			break
		}
		if s.ColumnCount() != 16 {
			t.Fatal("independent full16 snapshot width")
		}
		cells := make([]string, 16)
		for i, k := range []Kind{TextKind, TextKind, TextKind, TextKind, TextKind, TextKind, TextKind, TextKind, IntegerKind, IntegerKind, TextKind, IntegerKind, IntegerKind, TextKind, BlobKind, IntegerKind} {
			kind, e := s.Kind(i)
			if e != nil || kind != k {
				t.Fatal("independent full16 snapshot native kind", i, e)
			}
			switch k {
			case TextKind:
				cells[i], e = s.Text(i)
			case IntegerKind:
				var n int64
				n, e = s.Int64(i)
				cells[i] = fmt.Sprint(n)
			case BlobKind:
				var b []byte
				b, e = s.Blob(i)
				cells[i] = fmt.Sprintf("%x", b)
			}
			if e != nil {
				t.Fatal("independent full16 snapshot owned value", e)
			}
		}
		if _, exists := snapshot[cells[0]]; exists {
			t.Fatal("duplicate interval identity in independent full snapshot")
		}
		snapshot[cells[0]] = cells
	}
	if err := s.Close(); err != nil {
		t.Fatal("independent full16 snapshot checked Close", err)
	}
	if len(snapshot) != len(ranges) {
		t.Fatal("full16 snapshot/range audit inventory differs")
	}
	for _, r := range ranges {
		if _, ok := snapshot[r.id]; !ok {
			t.Fatal("range absent from full16 snapshot")
		}
	}
	return ranges, snapshot
}
