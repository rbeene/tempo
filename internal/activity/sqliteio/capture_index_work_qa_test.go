//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

// Independent native work QA for root-approved fresh-only plan 41b69115.
// No production observer/API, test-installed index, native or timing claim.
import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	identitypkg "github.com/rbeene/tempo/internal/identity"

	lib "modernc.org/sqlite/lib"
)

type iwQuery struct {
	id, sql string
	indexes []string
	M       int
	kinds   []Kind
}

var iwQueries = []iwQuery{
	{"Q01", `SELECT actor_key FROM actors
WHERE computer_id=? AND health='continuous' AND state IN ('working','wait_children','wait_user');`, []string{"actor_clock"}, 4, []Kind{TextKind}},
	{"Q02", `SELECT actor_key FROM actors
WHERE computer_id=? AND state NOT IN ('finished','interrupted');`, []string{"actor_live"}, 4, []Kind{TextKind}},
	{"Q03", `SELECT turn_key FROM host_turns
WHERE incarnation=? AND actor_key IS NOT NULL LIMIT 1;`, []string{"turn_incarnation_actor"}, 1, []Kind{TextKind}},
	{"Q04", `SELECT turn_key,actor_key,actor_generation FROM host_turns
WHERE actor_key=? ORDER BY actor_generation DESC LIMIT 1;`, []string{"turn_actor"}, 1, []Kind{TextKind, TextKind, BlobKind}},
	{"Q05", `SELECT uncertainty_id FROM uncertainties
WHERE actor_key=? AND state='unresolved' AND upper_bound_sec IS NULL;`, []string{"uncertainty_open_actor"}, 4, []Kind{TextKind}},
	{"Q06", `SELECT actor_key,ordinal,uncertainty_id FROM actor_uncertainties
WHERE actor_key=? AND uncertainty_id=? ORDER BY ordinal ASC LIMIT 1;`, []string{"actor_uncertainty_member", "actor_uncertainty_ref"}, 1, []Kind{TextKind, IntegerKind, TextKind}},
	{"Q07", `SELECT interval_id FROM intervals
WHERE computer_id=? AND account_id=? AND project_id=?
ORDER BY end_sec DESC,end_nsec DESC LIMIT 1;`, []string{"interval_timer_end"}, 1, []Kind{TextKind}},
	{"Q08", `SELECT uncertainty_id FROM uncertainties
WHERE computer_id=? AND account_id=? AND project_id=? AND state='unresolved'
AND upper_bound_sec IS NULL AND (user_id<>? OR task_id<>? OR timezone<>?) LIMIT 1;`, []string{"uncertainty_timer_open", "uncertainty_timer_upper"}, 4, []Kind{TextKind}},
	{"Q09", `SELECT uncertainty_id FROM uncertainties
WHERE computer_id=? AND account_id=? AND project_id=? AND state='unresolved'
AND (upper_bound_sec,upper_bound_nsec)>=(?,?)
AND (user_id<>? OR task_id<>? OR timezone<>?) LIMIT 1;`, []string{"uncertainty_timer_upper"}, 4, []Kind{TextKind}},
	{"Q10", `SELECT component_id,segment_id FROM component_segments WHERE segment_id=?;`, []string{"sqlite_autoindex_component_segments_2"}, 4, []Kind{TextKind, TextKind}},
	{"Q11", `SELECT component_id,segment_id FROM component_segments WHERE component_id=? ORDER BY segment_id;`, []string{"sqlite_autoindex_component_segments_1"}, 4, []Kind{TextKind, TextKind}},
	{"Q12", `SELECT component_id FROM union_frontier
WHERE computer_id=? AND account_id=? AND user_id=? AND project_id=? AND task_id=? AND timezone=?
AND (start_sec,start_nsec)<=(?,?) AND (end_sec,end_nsec)>=(?,?);`, []string{"frontier_group_start", "frontier_group_end"}, 8, []Kind{TextKind}},
	{"Q13", `SELECT component_id FROM union_frontier
WHERE computer_id=? AND account_id=? AND project_id=? AND (end_sec,end_nsec)>=(?,?);`, []string{"frontier_timer_end"}, 8, []Kind{TextKind}},
	{"Q14", `SELECT component_id FROM union_frontier
WHERE computer_id=? AND account_id=? AND project_id=?
AND (end_sec,end_nsec)>=(?,?) AND (start_sec,start_nsec)<=(?,?);`, []string{"frontier_timer_end", "frontier_timer_start"}, 8, []Kind{TextKind}},
	{"Q15", `SELECT component_id,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json
FROM pending_finalization ORDER BY group_order,start_sec,start_nsec,end_sec,end_nsec,component_id;`, []string{"pending_order"}, 4, []Kind{TextKind, TextKind, IntegerKind, IntegerKind, TextKind, IntegerKind, IntegerKind, TextKind}},
	{"Q16", `SELECT a.actor_key,a.segment_id FROM actors AS a JOIN segments AS s ON s.segment_id=a.segment_id
WHERE a.computer_id=? AND a.account_id=? AND a.project_id=? AND a.state='working' AND a.health='continuous'
AND (s.start_sec,s.start_nsec)<=(?,?) LIMIT 1;`, []string{"actor_timer", "sqlite_autoindex_segments_1"}, 4, []Kind{TextKind, TextKind}},
	{"Q17", `SELECT uncertainty_id FROM uncertainties
WHERE computer_id=? AND account_id=? AND project_id=? AND state='unresolved' AND upper_bound_sec IS NULL
AND (lower_bound_sec,lower_bound_nsec)<=(?,?) LIMIT 1;`, []string{"uncertainty_timer_open", "uncertainty_timer_upper", "uncertainty_timer_lower"}, 4, []Kind{TextKind}},
	{"Q18", `SELECT uncertainty_id FROM uncertainties
WHERE computer_id=? AND account_id=? AND project_id=? AND state='unresolved'
AND (upper_bound_sec,upper_bound_nsec)>=(?,?) AND (lower_bound_sec,lower_bound_nsec)<=(?,?) LIMIT 1;`, []string{"uncertainty_timer_upper", "uncertainty_timer_lower"}, 4, []Kind{TextKind}},
	{"Q19", `SELECT interval_id FROM intervals
WHERE computer_id=? AND account_id=? AND project_id=?
AND (start_sec,start_nsec)<(?,?) AND (end_sec,end_nsec)>(?,?) LIMIT 1;`, []string{"interval_timer_start", "interval_timer_end"}, 1, []Kind{TextKind}},
	{"Q20", `SELECT creation_ordinal FROM intervals ORDER BY creation_ordinal DESC LIMIT 1;`, []string{"sqlite_autoindex_intervals_2"}, 1, []Kind{IntegerKind}},
	{"Q21", `SELECT actor_key FROM actors WHERE segment_id=?;`, []string{"actor_segment"}, 4, []Kind{TextKind}},
	{"Q22", `SELECT uncertainty_id FROM uncertainties WHERE segment_id=?;`, []string{"uncertainty_segment"}, 4, []Kind{TextKind}},
	{"Q23", `SELECT segment_id FROM segments WHERE uncertainty_id=?;`, []string{"segment_uncertainty"}, 4, []Kind{TextKind}},
	{"Q24", `SELECT uncertainty_id,actor_key FROM actor_uncertainties WHERE uncertainty_id=?;`, []string{"actor_uncertainty_ref"}, 4, []Kind{TextKind, TextKind}},
	{"Q25", `SELECT uncertainty_id FROM recovery_decisions WHERE request_id=?;`, []string{"recovery_request_ref"}, 4, []Kind{TextKind}},
	{"Q26", `SELECT EXISTS(SELECT 1 FROM host_turns
WHERE source=? AND native_session=? AND incarnation<? LIMIT 1);`, []string{"turn_native_incarnation"}, 1, []Kind{IntegerKind}},
	{"Q27", `SELECT EXISTS(SELECT 1 FROM host_turns
WHERE source=? AND native_session=? AND incarnation>? LIMIT 1);`, []string{"turn_native_incarnation"}, 1, []Kind{IntegerKind}},
}
var iwDDL = []string{
	`CREATE INDEX actor_live ON actors(computer_id,state,actor_key,generation)
WHERE state NOT IN ('finished','interrupted');`,
	`CREATE INDEX uncertainty_open_actor ON uncertainties(actor_key,uncertainty_id)
WHERE state='unresolved' AND upper_bound_sec IS NULL;`,
	`CREATE INDEX actor_uncertainty_member ON actor_uncertainties(actor_key,uncertainty_id,ordinal);`,
	`CREATE INDEX uncertainty_timer_open ON uncertainties(computer_id,account_id,project_id,lower_bound_sec,lower_bound_nsec)
WHERE state='unresolved' AND upper_bound_sec IS NULL;`,
	`CREATE INDEX turn_incarnation_actor ON host_turns(incarnation)
WHERE actor_key IS NOT NULL;`,
	`CREATE INDEX turn_native_incarnation ON host_turns(source,native_session,incarnation);`,
}

type iwCase struct {
	q           int
	name        string
	args        []Value
	want, oneOf [][]string
	ordered     bool
	M           int
}
type iwWork struct {
	VM, Full, Sort, Auto int32
	rows                 [][]string
	plan                 []string
	q19Statements        []iwQ19Statement
}

func iwRows(ids ...string) [][]string {
	r := make([][]string, len(ids))
	for i, id := range ids {
		r[i] = []string{id}
	}
	return r
}
func iwTimer() []Value                { return []Value{Text(iwComputer), Text("1"), Text("3")} }
func iwPoint(sec, nsec int64) []Value { return []Value{Integer(sec), Integer(nsec)} }
func iwCases(n int) []iwCase {
	full := []Value{Text(iwComputer), Text("1"), Text("2"), Text("3"), Text("4"), Text("UTC")}
	timer := iwTimer()
	badTimer := []Value{Text(iwComputer), Text("99"), Text("3")}
	host := []Value{Text("claude"), Text(iwNative), Text(iwCurrent)}
	positive := []iwCase{
		{0, "relevant-clock", []Value{Text(iwComputer)}, iwRows(iwKey(0), iwKey(1), iwKey(2)), nil, false, 4},
		{1, "live-heads", []Value{Text(iwComputer)}, iwRows(iwKey(0), iwKey(1), iwKey(2), iwKey(3)), nil, false, 4},
		{2, "all-actorless-current", []Value{Text(iwCurrent)}, nil, nil, false, 1},
		{3, "unsigned-tied-maximum", []Value{Text(iwKey(2))}, nil, [][]string{{iwHash([]string{iwOtherInc, "generation-2", "gen-reserved"}), iwKey(2), "8000000000000001"}, {iwHash([]string{iwOtherInc, "generation-3", "gen-reserved"}), iwKey(2), "8000000000000001"}}, false, 1},
		{4, "open-candidates", []Value{Text(iwKey(0))}, iwRows(iwUUID(2, 0), iwUUID(2, 1)), nil, false, 4},
		{5, "first-duplicate-tail-ordinal", []Value{Text(iwKey(0)), Text(iwUUID(2, 0))}, [][]string{{iwKey(0), strconv.Itoa(n), iwUUID(2, 0)}}, nil, true, 1},
		{6, "latest-fixed-end", timer, iwRows(iwUUID(71, 0)), nil, false, 1},
		{7, "incompatible-open", append(append([]Value{}, timer...), Text("2"), Text("4"), Text("UTC")), iwRows(iwUUID(2, 1)), nil, false, 4},
		{8, "incompatible-bounded", append(append(append([]Value{}, timer...), iwPoint(0, 0)...), Text("2"), Text("4"), Text("UTC")), iwRows(iwUUID(2, 2)), nil, false, 4},
		{9, "reverse-component", []Value{Text(iwUUID(80, 0))}, [][]string{{iwFC(0), iwUUID(80, 0)}}, nil, false, 4},
		{10, "ordered-component-members", []Value{Text(iwFC(0))}, [][]string{{iwFC(0), iwUUID(80, 0)}, {iwFC(0), iwUUID(80, 1)}, {iwFC(0), iwUUID(80, 2)}, {iwFC(0), iwUUID(80, 3)}}, nil, true, 4},
		{11, "full-group-touching-range", append(append(append([]Value{}, full...), iwPoint(30, 0)...), iwPoint(20, 0)...), iwRows(iwFC(2), iwFC(3)), nil, false, 8},
		{12, "unbounded-timer-invalidation", append(append([]Value{}, timer...), iwPoint(20, 0)...), iwRows(iwFC(2), iwFC(3), iwFC(4)), nil, false, 8},
		{13, "bounded-both-contacts", append(append(append([]Value{}, timer...), iwPoint(20, 0)...), iwPoint(30, 0)...), iwRows(iwFC(2), iwFC(3)), nil, false, 8},
		{14, "complete-ordered-pending", nil, nil, nil, true, 4},
		{15, "working-actor", append(append([]Value{}, timer...), iwPoint(0, 0)...), [][]string{{iwKey(0), iwUUID(1, 0)}}, nil, false, 4},
		{16, "open-lower-bound", append(append([]Value{}, timer...), iwPoint(0, 0)...), iwRows(iwUUID(2, 0)), nil, false, 4},
		{17, "bounded-reservation", append(append(append([]Value{}, timer...), iwPoint(0, 0)...), iwPoint(0, 0)...), iwRows(iwUUID(2, 2)), nil, false, 4},
		{18, "middle-gap-retained-prefixes", append(append(append([]Value{}, timer...), iwPoint(1, 0)...), iwPoint(0, 0)...), nil, nil, false, 1},
		{19, "max-append-ordinal", nil, [][]string{{strconv.Itoa(n)}}, nil, false, 1},
		{20, "segment-actor-owner", []Value{Text(iwUUID(1, 0))}, iwRows(iwKey(0)), nil, false, 4},
		{21, "segment-uncertainty-owner", []Value{Text(iwUUID(1, 0))}, iwRows(iwUUID(2, 0)), nil, false, 4},
		{22, "uncertainty-segment-owner", []Value{Text(iwUUID(2, 0))}, iwRows(iwUUID(1, 0)), nil, false, 4},
		{23, "duplicate-membership-reverse", []Value{Text(iwUUID(2, 0))}, [][]string{{iwUUID(2, 0), iwKey(0)}, {iwUUID(2, 0), iwKey(0)}}, nil, false, 4},
		{24, "selected-request-owners", []Value{Text(iwUUID(90, 0))}, iwRows(iwUUID(51, 0), iwUUID(51, 1), iwUUID(51, 2), iwUUID(51, 3)), nil, false, 4},
		{25, "current-only-lower-negative", host, [][]string{{"0"}}, nil, false, 1},
		{26, "current-only-higher-negative", host, [][]string{{"0"}}, nil, false, 1},
	}
	bounds := [][2]int64{{-40, -30}, {-20, -10}, {10, 20}, {30, 40}}
	pending := [][]string{}
	for i, b := range bounds {
		pending = append(pending, []string{iwFC(i), iwGroup(iwComputer, iwAttr), strconv.FormatInt(b[0], 10), "0", iwTimeJSON(b[0], 0), strconv.FormatInt(b[1], 10), "0", iwTimeJSON(b[1], 0)})
	}
	positive[14].want = pending
	negative := []iwCase{
		{0, "no-clock-candidates", []Value{Text(iwNegativeComputer)}, nil, nil, false, 4},
		{1, "terminal-heads-only", []Value{Text(iwNegativeComputer)}, nil, nil, false, 4},
		{3, "identity-only-no-turn", []Value{Text(iwKey(3))}, nil, nil, false, 1},
		{4, "resolved-only-owner", []Value{Text(iwKey(1))}, nil, nil, false, 4},
		{5, "missing-tail-member", []Value{Text(iwKey(0)), Text(iwUUID(2, 999))}, nil, nil, true, 1},
		{6, "other-timer-empty", badTimer, nil, nil, false, 1},
		{7, "other-timer-empty", append(append([]Value{}, badTimer...), Text("2"), Text("4"), Text("UTC")), nil, nil, false, 4},
		{8, "bounded-before-proposed-start", append(append(append([]Value{}, timer...), iwPoint(100, 0)...), Text("2"), Text("4"), Text("UTC")), nil, nil, false, 4},
		{9, "no-reverse-component", []Value{Text(iwUUID(80, 999))}, nil, nil, false, 4},
		{10, "no-component-members", []Value{Text(iwFC(999))}, nil, nil, true, 4},
		{11, "same-group-middle-gap", append(append(append([]Value{}, full...), iwPoint(9, 0)...), iwPoint(-9, 0)...), nil, nil, false, 8},
		{12, "no-end-after-window", append(append([]Value{}, timer...), iwPoint(100, 0)...), nil, nil, false, 8},
		{13, "no-bounded-intersection", append(append(append([]Value{}, timer...), iwPoint(100, 0)...), iwPoint(200, 0)...), nil, nil, false, 8},
		{15, "working-start-after-end", append(append([]Value{}, timer...), iwPoint(-11, 0)...), nil, nil, false, 4},
		{16, "open-lower-after-end", append(append([]Value{}, timer...), iwPoint(-2, 0)...), nil, nil, false, 4},
		{17, "bounded-active-gap", append(append(append([]Value{}, timer...), iwPoint(6, 0)...), iwPoint(9, 0)...), nil, nil, false, 4},
		{18, "strict-single-overlap", append(append(append([]Value{}, timer...), iwPoint(101, 0)...), iwPoint(100, 0)...), iwRows(iwUUID(60, 1)), nil, false, 1},
		{18, "before-all-retained-intervals", append(append(append([]Value{}, timer...), iwPoint(-1999999999, 0)...), iwPoint(-2000000000, 0)...), nil, nil, false, 1},
		{18, "after-all-retained-intervals", append(append(append([]Value{}, timer...), iwPoint(2000000001, 0)...), iwPoint(2000000000, 0)...), nil, nil, false, 1},
		{20, "absent-segment-owner", []Value{Text(iwUUID(1, 999))}, nil, nil, false, 4},
		{21, "absent-uncertainty-owner", []Value{Text(iwUUID(1, 999))}, nil, nil, false, 4},
		{22, "absent-segment-reverse", []Value{Text(iwUUID(2, 999))}, nil, nil, false, 4},
		{23, "absent-membership-reverse", []Value{Text(iwUUID(2, 999))}, nil, nil, false, 4},
		{24, "absent-request-owner", []Value{Text(iwUUID(90, 999))}, nil, nil, false, 4},
	}
	return append(positive, negative...)
}

func iwCounters(t *testing.T, s *Stmt, reset int32) iwWork {
	t.Helper()
	if s.ptr == 0 || s.tx == nil || s.tx.conn == nil || s.tx.conn.tls == nil {
		t.Fatal("counter attempted outside live owned statement")
	}
	c := s.tx.conn
	w := iwWork{VM: lib.Xsqlite3_stmt_status(c.tls, s.ptr, lib.SQLITE_STMTSTATUS_VM_STEP, reset), Full: lib.Xsqlite3_stmt_status(c.tls, s.ptr, lib.SQLITE_STMTSTATUS_FULLSCAN_STEP, reset), Sort: lib.Xsqlite3_stmt_status(c.tls, s.ptr, lib.SQLITE_STMTSTATUS_SORT, reset), Auto: lib.Xsqlite3_stmt_status(c.tls, s.ptr, lib.SQLITE_STMTSTATUS_AUTOINDEX, reset)}
	if w.VM < 0 || w.Full < 0 || w.Sort < 0 || w.Auto < 0 {
		t.Fatal("native statement counter overflow")
	}
	return w
}
func iwMeasure(t *testing.T, tx *Tx, sql string, kinds []Kind, args []Value, cachedDone bool) iwWork {
	t.Helper()
	s := qaPrepare(t, tx, sql, args...)
	zero := iwCounters(t, s, 1)
	if zero.VM != 0 || zero.Full != 0 || zero.Sort != 0 || zero.Auto != 0 {
		t.Fatal("fresh target counter was nonzero")
	}
	rows := [][]string{}
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal("real target step failed", err)
		}
		if !row {
			break
		}
		if s.ColumnCount() != len(kinds) {
			t.Fatal("target projection width differs")
		}
		values := []string{}
		for i, want := range kinds {
			kind, err := s.Kind(i)
			if err != nil || kind != want {
				t.Fatal("target exact native kind differs", i, err)
			}
			var text string
			switch kind {
			case TextKind:
				text, err = s.Text(i)
			case IntegerKind:
				var n int64
				n, err = s.Int64(i)
				text = strconv.FormatInt(n, 10)
			case BlobKind:
				var b []byte
				b, err = s.Blob(i)
				text = fmt.Sprintf("%x", b)
			default:
				t.Fatal("unexpected target kind")
			}
			if err != nil {
				t.Fatal(err)
			}
			values = append(values, text)
		}
		rows = append(rows, values)
	}
	w := iwCounters(t, s, 0)
	if cachedDone {
		if row, err := s.Step(); row || err != nil {
			t.Fatal("cached DONE control", err)
		}
		again := iwCounters(t, s, 0)
		if again.VM != w.VM || again.Full != w.Full || again.Sort != w.Sort || again.Auto != w.Auto {
			t.Fatal("cached DONE changed native work")
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal("checked target Close", err)
	}
	w.rows = rows
	explain := qaPrepare(t, tx, "EXPLAIN QUERY PLAN "+sql, args...)
	for {
		row, err := explain.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !row {
			break
		}
		if explain.ColumnCount() != 4 {
			t.Fatal("EQP literal width")
		}
		for i := 0; i < 3; i++ {
			kind, err := explain.Kind(i)
			if err != nil || kind != IntegerKind {
				t.Fatal("EQP integer kind")
			}
			if _, err := explain.Int64(i); err != nil {
				t.Fatal(err)
			}
		}
		kind, err := explain.Kind(3)
		if err != nil || kind != TextKind {
			t.Fatal("EQP detail kind")
		}
		detail, err := explain.Text(3)
		if err != nil {
			t.Fatal(err)
		}
		w.plan = append(w.plan, detail)
	}
	if err := explain.Close(); err != nil {
		t.Fatal(err)
	}
	return w
}
func iwCheckRows(t *testing.T, c iwCase, w iwWork) {
	t.Helper()
	if c.oneOf != nil {
		if len(w.rows) != 1 {
			t.Fatal("LIMIT maximum did not return one row")
		}
		for _, row := range c.oneOf {
			if reflect.DeepEqual(w.rows[0], row) {
				return
			}
		}
		t.Fatal("selected turn outside independently computed tied maximum set")
	}
	want := append([][]string{}, c.want...)
	got := append([][]string{}, w.rows...)
	if !c.ordered {
		less := func(a, b []string) bool { return iwJSON(a) < iwJSON(b) }
		sort.Slice(want, func(i, j int) bool { return less(want[i], want[j]) })
		sort.Slice(got, func(i, j int) bool { return less(got[i], got[j]) })
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("query%s/%s correctness got=%v want=%v", iwQueries[c.q].id, c.name, got, want)
	}
}
func iwRecord(t *testing.T, n int, c iwCase, w iwWork) {
	t.Helper()
	if c.q == 18 {
		iwRecordQ19(t, n, c, w)
		return
	}
	q := iwQueries[c.q]
	sqlHash := sha256.Sum256([]byte(q.sql))
	digest := sha256.Sum256([]byte(iwJSON(w.rows)))
	// Value's fields are private, so the bound digest below separately uses their
	// owned native representation rather than trusting JSON's empty-object view.
	bindBytes := []byte{}
	for _, v := range c.args {
		bindBytes = append(bindBytes, []byte(fmt.Sprintf("%#v;", v))...)
	}
	bindHash := sha256.Sum256(bindBytes)
	t.Logf("QUERY_WORK N=%d query=%s case=%s M=%d VM_STEP=%d FULLSCAN_STEP=%d SORT=%d AUTOINDEX=%d rows=%d sql_sha=%x bind_sha=%x result_sha=%x plan=%q", n, q.id, c.name, c.M, w.VM, w.Full, w.Sort, w.Auto, len(w.rows), sqlHash, bindHash, digest, w.plan)
}
func iwCheckBound(t *testing.T, n int, c iwCase, w iwWork) {
	t.Helper()
	q := iwQueries[c.q]
	if w.VM > int32(256+64*c.M) {
		t.Errorf("N%d %s/%s VM%d exceeds fixed active budget%d", n, q.id, c.name, w.VM, 256+64*c.M)
	}
	fullBudget := int32(0)
	if c.q == 14 {
		fullBudget = int32(c.M)
	}
	if c.q == 19 {
		fullBudget = 1
	}
	if w.Full > fullBudget {
		t.Errorf("%s/%s FULLSCAN%d exceeds%d", q.id, c.name, w.Full, fullBudget)
	}
	if w.Sort != 0 || w.Auto != 0 {
		t.Errorf("%s/%s unexpected SORT%d/AUTOINDEX%d", q.id, c.name, w.Sort, w.Auto)
	}
	joined := strings.Join(w.plan, "\n")
	hasIndex := false
	for _, name := range q.indexes {
		if strings.Contains(joined, name) {
			hasIndex = true
		}
	}
	if !hasIndex {
		t.Errorf("%s/%s expected bounded index family missing: %s", q.id, c.name, joined)
	}
	if strings.Contains(joined, "TEMP B-TREE") {
		t.Errorf("%s/%s temporary sort plan", q.id, c.name)
	}
	if c.q != 14 && c.q != 19 && c.q != 2 && !strings.Contains(joined, "SEARCH") {
		t.Errorf("%s/%s keyed SEARCH missing", q.id, c.name)
	}
}
func iwIndexAdoption(t *testing.T, tx *Tx) {
	t.Helper()
	normalize := func(s string) string {
		return strings.ToLower(strings.Join(strings.Fields(strings.TrimSuffix(s, ";")), ""))
	}
	for _, ddl := range iwDDL {
		fields := strings.Fields(ddl)
		name := fields[2]
		s := qaPrepare(t, tx, "SELECT name,sql FROM sqlite_schema WHERE type='index' AND name=?", Text(name))
		row, err := s.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !row {
			t.Errorf("missing adopted production index %s", name)
		} else {
			n, err := s.Text(0)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := s.Text(1)
			if err != nil {
				t.Fatal(err)
			}
			if n != name || normalize(actual) != normalize(ddl) {
				t.Errorf("production index%s column/predicate definition differs", name)
			}
			if row, err := s.Step(); row || err != nil {
				t.Fatal("index metadata duplicate/DONE", err)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func iwRunCases(t *testing.T, dir string, n int, cases []iwCase, records map[string]iwWork) {
	t.Helper()
	c := qaOpen(t, dir, iwName, false)
	tx := qaBegin(t, c, context.Background(), Read)
	var ranges []iwRange
	var snapshot map[string][]string
	for _, test := range cases {
		if test.q == 18 {
			ranges, snapshot = iwQ19Snapshot(t, tx)
			break
		}
	}
	for _, test := range cases {
		q := iwQueries[test.q]
		var w iwWork
		if test.q == 18 {
			// Q19's seven-value catalog input preserves End then Start. The
			// selected real implementation is two full16, five-bind statements.
			var err error
			w, err = iwMeasureQ19Neighbors(t, tx, test.args)
			if err != nil {
				t.Fatal("actual selected neighbor scalar refused", err)
			}
		} else {
			if strings.Count(q.sql, "?") != len(test.args) {
				t.Fatal("fixed catalog bind count differs", q.id)
			}
			w = iwMeasure(t, tx, q.sql, q.kinds, test.args, false)
		}
		if test.q == 18 {
			iwCheckQ19Snapshot(t, test.args, w, ranges, snapshot)
		}
		iwCheckRows(t, test, w)
		iwRecord(t, n, test, w)
		iwCheckBound(t, n, test, w)
		records[q.id+"/"+test.name] = w
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
}
func iwHostVariants(t *testing.T, dir string, n int, records map[string]iwWork) {
	for _, variant := range []string{"actor-current-other-source-native", "lower-stopped-actorless", "higher-stopped-actorless", "wrong-source-both-directions", "wrong-native-both-directions"} {
		c := qaOpen(t, dir, iwName, false)
		tx := qaBegin(t, c, context.Background(), Write)
		inserted := []string{}
		switch variant {
		case "actor-current-other-source-native":
			actor := iwActorIdentity{iwComputer, "codex", iwCurrent, "root"}
			iwGeneration(t, tx, actor, 1)
			inserted = append(inserted, iwTurn(t, tx, "codex", "different-native", iwCurrent, "one-actor-membership", "", &actor, 1, false))
		case "lower-stopped-actorless":
			inserted = append(inserted, iwTurn(t, tx, "claude", iwNative, iwLower, "lower", "", nil, 0, true))
		case "higher-stopped-actorless":
			inserted = append(inserted, iwTurn(t, tx, "claude", iwNative, iwHigher, "higher", "", nil, 0, true))
		case "wrong-source-both-directions":
			for _, inc := range []string{iwLower, iwHigher} {
				inserted = append(inserted, iwTurn(t, tx, "codex", iwNative, inc, "wrong-source", "", nil, 0, true))
			}
		case "wrong-native-both-directions":
			for _, inc := range []string{iwLower, iwHigher} {
				inserted = append(inserted, iwTurn(t, tx, "claude", "wrong-native", inc, "wrong-native", "", nil, 0, true))
			}
		}
		if err := tx.CheckForeignKeys(); err != nil {
			t.Fatal(err)
		}
		qaCommit(t, tx)
		qaClose(t, c)
		cases := []iwCase{}
		if variant == "actor-current-other-source-native" {
			cases = append(cases, iwCase{2, variant, []Value{Text(iwCurrent)}, iwRows(inserted[0]), nil, false, 1})
		} else {
			for q := 25; q <= 26; q++ {
				want := "0"
				if (q == 25 && variant == "lower-stopped-actorless") || (q == 26 && variant == "higher-stopped-actorless") {
					want = "1"
				}
				cases = append(cases, iwCase{q, variant, []Value{Text("claude"), Text(iwNative), Text(iwCurrent)}, [][]string{{want}}, nil, false, 1})
			}
		}
		iwRunCases(t, dir, n, cases, records)
		// Owned test fixture cleanup between separately committed/reopened variants.
		// This is never a production retained-history deletion/API permission.
		c = qaOpen(t, dir, iwName, false)
		tx = qaBegin(t, c, context.Background(), Write)
		for _, key := range inserted {
			qaDone(t, tx, "DELETE FROM host_turns WHERE turn_key=?", Text(key))
		}
		if variant == "actor-current-other-source-native" {
			actor := iwActorIdentity{iwComputer, "codex", iwCurrent, "root"}
			qaDone(t, tx, "DELETE FROM actor_generations WHERE actor_key=? AND generation=?", Text(iwJSON(actor)), iwCounter(1))
		}
		qaCommit(t, tx)
		qaClose(t, c)
	}
}
func iwEmptyControls(t *testing.T, dir string, n int, records map[string]iwWork) {
	c := qaOpen(t, dir, iwName, false)
	tx := qaBegin(t, c, context.Background(), Write)
	qaDone(t, tx, "DELETE FROM pending_finalization")
	qaCommit(t, tx)
	qaClose(t, c)
	iwRunCases(t, dir, n, []iwCase{{14, "empty-pending", nil, nil, nil, true, 0}}, records)
	// Separate fresh schema with retained terminal heads but no intervals. Both
	// retained scales preserve the legitimate global-maximum empty premise.
	empty := qaDirectory(t)
	c = qaOpen(t, empty, iwName, true)
	tx = qaBegin(t, c, context.Background(), Write)
	ddl, err := os.ReadFile("../sqlite_schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.InstallSchema(string(ddl)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		a := iwIdentity(20000+i, iwComputer)
		iwGeneration(t, tx, a, 1)
		iwActor(t, tx, a, iwUUID(99, i), "finished", "")
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	qaCommit(t, tx)
	qaClose(t, c)
	iwRunCases(t, empty, n, []iwCase{{19, "empty-intervals-retained-actors", nil, nil, nil, true, 1}}, records)
}
func TestSQLiteCaptureIndexWorkFixedQueriesRetained1000And5000(t *testing.T) {
	all := map[int]map[string]iwWork{}
	calibration := map[int]iwWork{}
	for _, n := range []int{1000, 5000} {
		dir := iwSeed(t, n)
		all[n] = map[string]iwWork{}
		schema, err := os.ReadFile("../sqlite_schema.sql")
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(schema)
		t.Logf("QUERY_FIXTURE N=%d production_schema_sha=%x A=4 H=4 U=4 K_supports=8 frontier_rows=5 P=4 returned_max=4; native_relational_only=true", n, h)
		c := qaOpen(t, dir, iwName, false)
		tx := qaBegin(t, c, context.Background(), Read)
		cw := iwMeasure(t, tx, "SELECT actor_key FROM actors NOT INDEXED WHERE actor_key=?", []Kind{TextKind}, []Value{Text(iwKey(10000))}, true)
		if !reflect.DeepEqual(cw.rows, iwRows(iwKey(10000))) || cw.Full <= 0 || cw.VM <= 0 {
			t.Fatal("actual full-table calibration or typed control failed")
		}
		calibration[n] = cw
		t.Logf("COUNTER_CALIBRATION N=%d VM_STEP=%d FULLSCAN_STEP=%d cached_DONE_unchanged=true", n, cw.VM, cw.Full)
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		qaClose(t, c)
		iwRunCases(t, dir, n, iwCases(n), all[n])
		iwHostVariants(t, dir, n, all[n])
		iwEmptyControls(t, dir, n, all[n])
		c = qaOpen(t, dir, iwName, false)
		tx = qaBegin(t, c, context.Background(), Read)
		iwIndexAdoption(t, tx)
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		qaClose(t, c)
	}
	if calibration[5000].Full <= calibration[1000].Full || calibration[5000].VM <= calibration[1000].VM {
		t.Fatal("native counter calibration failed retained-growth control")
	}
	if len(all[1000]) != len(all[5000]) {
		t.Fatal("retained-scale case inventory differs")
	}
	for key, a := range all[1000] {
		b, ok := all[5000][key]
		if !ok {
			t.Fatal("missing retained-scale case", key)
		}
		diff := int64(b.VM) - int64(a.VM)
		if diff < 0 {
			diff = -diff
		}
		if diff > 128 {
			t.Errorf("%s actual VM retained-growth=%d exceeds128 (1000:%d 5000:%d FULLSCAN1000:%d FULLSCAN5000:%d)", key, diff, a.VM, b.VM, a.Full, b.Full)
		}
		if b.Full > a.Full {
			t.Errorf("%s growing actual full scan%d ->%d", key, a.Full, b.Full)
		}
	}
	// No fixed count of failing index families is asserted. Q06 can already be
	// bounded through actor_uncertainty_ref, with a tiny matching-set SORT.
}

const iwCandidatePredecessor = "SELECT interval_id FROM intervals WHERE computer_id=? AND account_id=? AND project_id=? AND (start_sec,start_nsec)<(?,?) ORDER BY start_sec DESC,start_nsec DESC LIMIT 1;"

type iwRange struct {
	id         string
	start, end time.Time
}

func iwAuditIntervals(t *testing.T, tx *Tx) ([]iwRange, bool) {
	t.Helper()
	s := qaPrepare(t, tx, "SELECT interval_id,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,duration_ns FROM intervals WHERE computer_id=? AND account_id=? AND project_id=? ORDER BY start_sec,start_nsec", iwTimer()...)
	out := []iwRange{}
	valid := true
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !row {
			break
		}
		want := []Kind{TextKind, IntegerKind, IntegerKind, TextKind, IntegerKind, IntegerKind, TextKind, BlobKind}
		for i, k := range want {
			got, err := s.Kind(i)
			if err != nil || got != k {
				t.Fatal("audit native kind", err)
			}
		}
		id, err := s.Text(0)
		if err != nil {
			t.Fatal(err)
		}
		sec, err := s.Int64(1)
		if err != nil {
			t.Fatal(err)
		}
		ns, err := s.Int64(2)
		if err != nil {
			t.Fatal(err)
		}
		startJSON, err := s.Text(3)
		if err != nil {
			t.Fatal(err)
		}
		endSec, err := s.Int64(4)
		if err != nil {
			t.Fatal(err)
		}
		endNS, err := s.Int64(5)
		if err != nil {
			t.Fatal(err)
		}
		endJSON, err := s.Text(6)
		if err != nil {
			t.Fatal(err)
		}
		duration, err := s.Blob(7)
		if err != nil {
			t.Fatal(err)
		}
		var start, end time.Time
		if json.Unmarshal([]byte(startJSON), &start) != nil || json.Unmarshal([]byte(endJSON), &end) != nil || start.Unix() != sec || int64(start.Nanosecond()) != ns || end.Unix() != endSec || int64(end.Nanosecond()) != endNS || iwTimeJSON(sec, ns) != startJSON || iwTimeJSON(endSec, endNS) != endJSON || ns < 0 || ns >= 1000000000 || endNS < 0 || endNS >= 1000000000 || !end.After(start) || len(duration) != 8 {
			valid = false
		} else if binary.BigEndian.Uint64(duration) != uint64(end.Sub(start)) {
			valid = false
		}
		if len(out) > 0 && start.Before(out[len(out)-1].end) {
			valid = false
		}
		out = append(out, iwRange{id, start, end})
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return out, valid
}
func TestSQLiteCaptureIndexWorkQ19CandidatePredecessorSoundnessSeparateFromCatalog(t *testing.T) {
	counts := map[int]map[string]int32{}
	for _, n := range []int{1000, 5000} {
		dir := iwSeed(t, n)
		c := qaOpen(t, dir, iwName, false)
		tx := qaBegin(t, c, context.Background(), Read)
		ranges, valid := iwAuditIntervals(t, tx)
		if !valid || len(ranges) != n+1 {
			t.Fatal("actual new-history nonoverlap/canonical premise failed")
		}
		byID := map[string]iwRange{}
		for _, r := range ranges {
			byID[r.id] = r
		}
		counts[n] = map[string]int32{}
		type window struct {
			name       string
			start, end time.Time
		}
		point := func(sec, nsec int64) time.Time { return time.Unix(sec, nsec).UTC() }
		windows := []window{{"middle-gap", point(0, 0), point(1, 0)}, {"before-all", point(-2000000000, 0), point(-1999999999, 0)}, {"after-all", point(2000000000, 0), point(2000000001, 0)}, {"identical-overlap", point(100, 0), point(101, 0)}, {"nested-overlap", point(100, 1), point(100, 2)}, {"left-touch", point(99, 0), point(100, 0)}, {"right-touch", point(101, 0), point(102, 0)}, {"one-ns-overlap", point(100, 999999999), point(101, 1)}, {"one-ns-gap", point(101, 1), point(102, 0)}, {"negative-epoch-backdated", point(-100, 1), point(-99, 0)}}
		for _, window := range windows {
			oracle := false
			for _, r := range ranges {
				if r.start.Before(window.end) && r.end.After(window.start) {
					oracle = true
				}
			}
			args := append(append([]Value{}, iwTimer()...), Integer(window.end.Unix()), Integer(int64(window.end.Nanosecond())))
			w := iwMeasure(t, tx, iwCandidatePredecessor, []Kind{TextKind}, args, false)
			candidate := false
			if len(w.rows) > 1 {
				t.Fatal("candidate returned multiple predecessors")
			}
			if len(w.rows) == 1 {
				r, ok := byID[w.rows[0][0]]
				if !ok {
					t.Fatal("candidate selected unknown audited scalar")
				}
				candidate = r.end.After(window.start)
			}
			if candidate != oracle {
				t.Fatal("candidate disagrees with independent full strictOverlap oracle", window.name)
			}
			if w.VM > 320 || w.Full != 0 || w.Sort != 0 || w.Auto != 0 {
				t.Errorf("candidate%s N%d nativework=%+v", window.name, n, w)
			}
			counts[n][window.name] = w.VM
			t.Logf("Q19_CANDIDATE N=%d case=%s conflict=%t VM_STEP=%d FULLSCAN_STEP=%d full_oracle_outside_measurement=true prospective_query_only=true", n, window.name, candidate, w.VM, w.Full)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		qaClose(t, c)
		// Actual schema permits cross-row overlap and inconsistent JSON. The explicit
		// test audit refuses both; no SQL CHECK/global invariant claim is substituted.
		for _, defect := range []string{"overlap", "canonical-projection"} {
			c = qaOpen(t, dir, iwName, false)
			tx = qaBegin(t, c, context.Background(), Write)
			if defect == "overlap" {
				qaDone(t, tx, "UPDATE intervals SET start_sec=100,start_nsec=0,start_json=?,end_sec=101,end_nsec=0,end_json=? WHERE interval_id=?", Text(iwTimeJSON(100, 0)), Text(iwTimeJSON(101, 0)), Text(iwUUID(60, 3)))
			} else {
				qaDone(t, tx, "UPDATE intervals SET end_json=? WHERE interval_id=?", Text(iwTimeJSON(102, 0)), Text(iwUUID(60, 1)))
			}
			if _, valid := iwAuditIntervals(t, tx); valid {
				t.Fatal("corrupt new-history premise accepted", defect)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			qaClose(t, c)
		}
	}
	for name, a := range counts[1000] {
		diff := int64(counts[5000][name]) - int64(a)
		if math.Abs(float64(diff)) > 128 {
			t.Errorf("candidate%s VM retained-growth%d", name, diff)
		}
	}
	// Audited native fixtures establish this mathematical experiment's premise.
	// Fresh immutable-seal invariant preservation and selected production decoder
	// behavior belong to composer/local-row QA, not this test-only full audit.
}

// Literal byte-for-byte statements from the actual local interval producers.
// The original never-activated Q19 descriptor/artifacts remain preserved.
const iwQ19Predecessor = "SELECT " + iwIntervalCols + " FROM intervals WHERE computer_id=? AND account_id=? AND project_id=? AND (start_sec,start_nsec)<=(?,?) ORDER BY start_sec DESC,start_nsec DESC,end_sec DESC,end_nsec DESC LIMIT 1"
const iwQ19Successor = "SELECT " + iwIntervalCols + " FROM intervals WHERE computer_id=? AND account_id=? AND project_id=? AND (start_sec,start_nsec)>(?,?) ORDER BY start_sec ASC,start_nsec ASC,end_sec ASC,end_nsec ASC LIMIT 1"

var iwQ19Kinds = []Kind{TextKind, TextKind, TextKind, TextKind, TextKind, TextKind, TextKind, TextKind, IntegerKind, IntegerKind, TextKind, IntegerKind, IntegerKind, TextKind, BlobKind, IntegerKind}

type iwQ19Statement struct {
	name, sql string
	args      []Value
	work      iwWork
}
type iwQ19Scalar struct {
	id, computer string
	attribution  iwAttribution
	start, end   time.Time
	duration     uint64
	ordinal      int64
}

func iwQ19UUID(s string) bool {
	parts := strings.Split(s, "-")
	if len(parts) != 5 || len(parts[0]) != 8 || len(parts[1]) != 4 || len(parts[2]) != 4 || len(parts[3]) != 4 || len(parts[4]) != 12 || strings.ToLower(s) != s {
		return false
	}
	b, err := hex.DecodeString(strings.Join(parts, ""))
	return err == nil && len(b) == 16
}
func iwQ19ScalarTime(secText, nsText, jsonText string) (time.Time, bool) {
	sec, err := strconv.ParseInt(secText, 10, 64)
	if err != nil || strconv.FormatInt(sec, 10) != secText {
		return time.Time{}, false
	}
	ns, err := strconv.ParseInt(nsText, 10, 64)
	if err != nil || strconv.FormatInt(ns, 10) != nsText || ns < 0 || ns >= 1000000000 {
		return time.Time{}, false
	}
	var value time.Time
	if json.Unmarshal([]byte(jsonText), &value) != nil || value.Unix() != sec || int64(value.Nanosecond()) != ns {
		return time.Time{}, false
	}
	canonical, err := value.MarshalJSON()
	if err != nil || string(canonical) != jsonText {
		return time.Time{}, false
	}
	return value, true
}
func iwQ19Decode(row []string, args []Value, predecessor bool) (iwQ19Scalar, bool) {
	// This decoder consumes the measured native full16 row, never an oracle ID
	// lookup. Native classes were checked while the actual cursor was owned.
	if len(row) != 16 || len(args) != 5 {
		return iwQ19Scalar{}, false
	}
	for _, i := range []int{0, 1, 2, 3, 4, 5, 6, 7, 10, 13} {
		if !utf8.ValidString(row[i]) {
			return iwQ19Scalar{}, false
		}
	}
	if !iwQ19UUID(row[0]) || !iwQ19UUID(row[1]) {
		return iwQ19Scalar{}, false
	}
	a := iwAttribution{row[2], row[3], row[4], row[5], row[6]}
	for _, id := range []string{a.AccountID, a.UserID, a.ProjectID, a.TaskID} {
		if !identitypkg.Valid(id) {
			return iwQ19Scalar{}, false
		}
	}
	// Match the concrete scalar's retained timezone/group shape; different
	// user/task/timezones remain valid within this computer/account/project timer.
	if len(a.Timezone) == 0 || len(a.Timezone) > 128 || path.Clean(a.Timezone) != a.Timezone || a.Timezone == "Local" || strings.Contains(a.Timezone, "\\") || strings.Contains(a.Timezone, "..") || strings.HasPrefix(a.Timezone, "/") {
		return iwQ19Scalar{}, false
	}
	for _, r := range a.Timezone {
		if unicode.IsControl(r) {
			return iwQ19Scalar{}, false
		}
	}
	if _, err := time.LoadLocation(a.Timezone); err != nil {
		return iwQ19Scalar{}, false
	}
	if row[7] != iwGroup(row[1], a) {
		return iwQ19Scalar{}, false
	}
	start, ok := iwQ19ScalarTime(row[8], row[9], row[10])
	if !ok {
		return iwQ19Scalar{}, false
	}
	end, ok := iwQ19ScalarTime(row[11], row[12], row[13])
	if !ok || !end.After(start) {
		return iwQ19Scalar{}, false
	}
	blob, err := hex.DecodeString(row[14])
	if err != nil || len(blob) != 8 {
		return iwQ19Scalar{}, false
	}
	duration := binary.BigEndian.Uint64(blob)
	if duration == 0 || duration != uint64(end.Sub(start)) {
		return iwQ19Scalar{}, false
	}
	ordinal, err := strconv.ParseInt(row[15], 10, 64)
	if err != nil || ordinal < 0 || strconv.FormatInt(ordinal, 10) != row[15] {
		return iwQ19Scalar{}, false
	}
	if args[0].kind != TextKind || args[1].kind != TextKind || args[2].kind != TextKind || args[3].kind != IntegerKind || args[4].kind != IntegerKind || args[4].integer < 0 || args[4].integer >= 1000000000 {
		return iwQ19Scalar{}, false
	}
	if row[1] != args[0].text || a.AccountID != args[1].text || a.ProjectID != args[2].text {
		return iwQ19Scalar{}, false
	}
	// Producer comparison uses supplied raw Unix/nanosecond tuple, without
	// materializing a JSON boundary or substituting a fixture endpoint.
	cmp := 0
	if start.Unix() < args[3].integer {
		cmp = -1
	} else if start.Unix() > args[3].integer {
		cmp = 1
	} else if int64(start.Nanosecond()) < args[4].integer {
		cmp = -1
	} else if int64(start.Nanosecond()) > args[4].integer {
		cmp = 1
	}
	if predecessor && cmp > 0 || !predecessor && cmp <= 0 {
		return iwQ19Scalar{}, false
	}
	return iwQ19Scalar{row[0], row[1], a, start, end, duration, ordinal}, true
}
func iwMeasureQ19Neighbors(t *testing.T, tx *Tx, catalogArgs []Value) (iwWork, error) {
	t.Helper()
	if len(catalogArgs) != 7 {
		return iwWork{}, fmt.Errorf("Q19 fixed seven-value case input required")
	}
	for _, i := range []int{3, 4, 5, 6} {
		if catalogArgs[i].kind != IntegerKind {
			return iwWork{}, fmt.Errorf("Q19 exact bound kind required")
		}
	}
	start := time.Unix(catalogArgs[5].integer, catalogArgs[6].integer).UTC()
	end := time.Unix(catalogArgs[3].integer, catalogArgs[4].integer).UTC()
	if catalogArgs[4].integer < 0 || catalogArgs[4].integer >= 1000000000 || catalogArgs[6].integer < 0 || catalogArgs[6].integer >= 1000000000 || !end.After(start) {
		return iwWork{}, fmt.Errorf("Q19 positive supplied window required")
	}
	args := append(append([]Value{}, catalogArgs[:3]...), catalogArgs[5:]...)
	for _, sql := range []string{iwQ19Predecessor, iwQ19Successor} {
		if strings.Count(sql, "?") != 5 || len(args) != 5 {
			return iwWork{}, fmt.Errorf("Q19 actual fixed five-bind count differs")
		}
	}
	pre := iwMeasure(t, tx, iwQ19Predecessor, iwQ19Kinds, args, true)
	succ := iwMeasure(t, tx, iwQ19Successor, iwQ19Kinds, args, true)
	combined := iwWork{rows: [][]string{}, q19Statements: []iwQ19Statement{{"predecessor", iwQ19Predecessor, append([]Value{}, args...), pre}, {"successor", iwQ19Successor, append([]Value{}, args...), succ}}}
	for _, pair := range []struct {
		a, b int32
		out  *int32
	}{{pre.VM, succ.VM, &combined.VM}, {pre.Full, succ.Full, &combined.Full}, {pre.Sort, succ.Sort, &combined.Sort}, {pre.Auto, succ.Auto, &combined.Auto}} {
		sum := int64(pair.a) + int64(pair.b)
		if pair.a < 0 || pair.b < 0 || sum > math.MaxInt32 {
			return iwWork{}, fmt.Errorf("Q19 aggregate native counter overflow")
		}
		*pair.out = int32(sum)
	}
	selected := ""
	for index, w := range []iwWork{pre, succ} {
		for _, detail := range w.plan {
			combined.plan = append(combined.plan, combined.q19Statements[index].name+": "+detail)
		}
		if len(w.rows) > 1 {
			return iwWork{}, fmt.Errorf("Q19 actual neighbor singleton required")
		}
		if len(w.rows) == 1 {
			r, valid := iwQ19Decode(w.rows[0], args, index == 0)
			if !valid {
				return iwWork{}, fmt.Errorf("Q19 selected full16 scalar corrupt")
			}
			conflict := index == 0 && r.end.After(start) || index == 1 && r.start.Before(end)
			if conflict && selected == "" {
				selected = r.id
			}
		}
		joined := strings.Join(w.plan, "\n")
		if !strings.Contains(joined, "SEARCH") || !strings.Contains(joined, "interval_timer_start") || strings.Contains(joined, "TEMP B-TREE") {
			t.Errorf("Q19 %s actual keyed range plan differs: %s", combined.q19Statements[index].name, joined)
		}
	}
	if selected != "" {
		combined.rows = iwRows(selected)
	}
	return combined, nil
}
func iwRecordQ19(t *testing.T, n int, c iwCase, w iwWork) {
	t.Helper()
	if len(w.q19Statements) != 2 {
		t.Fatal("Q19 missing actual two-statement evidence")
	}
	sqlBytes, bindBytes := []byte{}, []byte{}
	for _, statement := range w.q19Statements {
		sqlHash := sha256.Sum256([]byte(statement.sql))
		bind := []byte{}
		for _, v := range statement.args {
			bind = append(bind, []byte(fmt.Sprintf("%#v;", v))...)
		}
		bindHash := sha256.Sum256(bind)
		resultHash := sha256.Sum256([]byte(iwJSON(statement.work.rows)))
		t.Logf("QUERY_WORK_STATEMENT N=%d query=Q19 case=%s branch=%s binds=5 full_projection=16 VM_STEP=%d FULLSCAN_STEP=%d SORT=%d AUTOINDEX=%d rows=%d sql_sha=%x bind_sha=%x result_sha=%x plan=%q cached_DONE_unchanged=true", n, c.name, statement.name, statement.work.VM, statement.work.Full, statement.work.Sort, statement.work.Auto, len(statement.work.rows), sqlHash, bindHash, resultHash, statement.work.plan)
		sqlBytes = append(sqlBytes, []byte(statement.sql)...)
		sqlBytes = append(sqlBytes, 0)
		bindBytes = append(bindBytes, bind...)
		bindBytes = append(bindBytes, 0)
	}
	sqlHash := sha256.Sum256(sqlBytes)
	bindHash := sha256.Sum256(bindBytes)
	resultHash := sha256.Sum256([]byte(iwJSON(w.rows)))
	t.Logf("QUERY_WORK N=%d query=Q19 case=%s M=%d VM_STEP=%d FULLSCAN_STEP=%d SORT=%d AUTOINDEX=%d rows=%d statements=2 counters_summed=true native_scalar_overlap=true sql_sha=%x bind_sha=%x result_sha=%x plan=%q", n, c.name, c.M, w.VM, w.Full, w.Sort, w.Auto, len(w.rows), sqlHash, bindHash, resultHash, w.plan)
}
func iwCheckQ19Snapshot(t *testing.T, args []Value, w iwWork, ranges []iwRange, snapshot map[string][]string) {
	t.Helper()
	start, end := time.Unix(args[5].integer, args[6].integer).UTC(), time.Unix(args[3].integer, args[4].integer).UTC()
	oracle := false
	for _, r := range ranges {
		if r.start.Before(end) && r.end.After(start) {
			oracle = true
		}
	}
	if (len(w.rows) > 0) != oracle {
		t.Fatal("implemented neighbors disagree with independent complete strictOverlap oracle")
	}
	for _, statement := range w.q19Statements {
		for _, row := range statement.work.rows {
			saved, ok := snapshot[row[0]]
			if !ok || !reflect.DeepEqual(row, saved) {
				t.Fatal("selected full16 scalar differs from independent complete snapshot")
			}
		}
	}
}

func TestSQLiteCaptureIndexWorkQ19ImplementedNeighborsNativeScalarsAndWindowControls(t *testing.T) {
	counts := map[int]map[string]iwWork{}
	for _, n := range []int{1000, 5000} {
		dir := iwSeed(t, n)
		c := qaOpen(t, dir, iwName, false)
		tx := qaBegin(t, c, context.Background(), Read)
		ranges, snapshot := iwQ19Snapshot(t, tx)
		if len(ranges) != n+1 {
			t.Fatal("implemented neighbor complete fixture count")
		}
		counts[n] = map[string]iwWork{}
		type window struct {
			name       string
			start, end time.Time
		}
		point := func(sec, nsec int64) time.Time { return time.Unix(sec, nsec).UTC() }
		windows := []window{{"middle-gap", point(0, 0), point(1, 0)}, {"before-all", point(-2000000000, 0), point(-1999999999, 0)}, {"after-all", point(2000000000, 0), point(2000000001, 0)}, {"identical-overlap", point(100, 0), point(101, 0)}, {"nested-overlap", point(100, 1), point(100, 2)}, {"left-touch", point(99, 0), point(100, 0)}, {"right-touch", point(101, 0), point(102, 0)}, {"one-ns-overlap", point(100, 999999999), point(101, 1)}, {"one-ns-gap", point(101, 1), point(102, 0)}, {"negative-epoch-backdated", point(-100, 1), point(-99, 0)}}
		for _, window := range windows {
			args := append(append(append([]Value{}, iwTimer()...), iwPoint(window.end.Unix(), int64(window.end.Nanosecond()))...), iwPoint(window.start.Unix(), int64(window.start.Nanosecond()))...)
			w, err := iwMeasureQ19Neighbors(t, tx, args)
			if err != nil {
				t.Fatal("actual implemented neighbors", window.name, err)
			}
			iwCheckQ19Snapshot(t, args, w, ranges, snapshot)
			control := iwCase{q: 18, name: "implemented-" + window.name, args: args, M: 1}
			iwRecord(t, n, control, w)
			iwCheckBound(t, n, control, w)
			counts[n][window.name] = w
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		qaClose(t, c)
		// A real empty interval set gives two absence reads; the fixed positive
		// window remains intact. Owned fixture deletion rolls back after measurement.
		c = qaOpen(t, dir, iwName, false)
		tx = qaBegin(t, c, context.Background(), Write)
		qaDone(t, tx, "DELETE FROM outbox")
		qaDone(t, tx, "DELETE FROM interval_segments")
		qaDone(t, tx, "DELETE FROM intervals")
		ranges, snapshot = iwQ19Snapshot(t, tx)
		if len(ranges) != 0 || len(snapshot) != 0 {
			t.Fatal("actual empty interval premise")
		}
		args := append(append(append([]Value{}, iwTimer()...), iwPoint(1, 0)...), iwPoint(0, 0)...)
		w, err := iwMeasureQ19Neighbors(t, tx, args)
		if err != nil {
			t.Fatal("empty actual neighbor bounds", err)
		}
		iwCheckQ19Snapshot(t, args, w, ranges, snapshot)
		if len(w.rows) != 0 || len(w.q19Statements[0].work.rows) != 0 || len(w.q19Statements[1].work.rows) != 0 {
			t.Fatal("empty bounds fabricated a selected row")
		}
		control := iwCase{q: 18, name: "implemented-empty-interval-bounds", args: args, M: 1}
		iwRecord(t, n, control, w)
		iwCheckBound(t, n, control, w)
		counts[n]["empty-interval-bounds"] = w
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		qaClose(t, c)
		c = qaOpen(t, dir, iwName, false)
		tx = qaBegin(t, c, context.Background(), Read)
		iwFixtureInventory(t, tx, n)
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		qaClose(t, c)
	}
	if len(counts[1000]) != 11 || len(counts[5000]) != 11 {
		t.Fatal("implemented fixed window inventory changed")
	}
	for name, a := range counts[1000] {
		b, ok := counts[5000][name]
		if !ok {
			t.Fatal("implemented window missing at retained scale")
		}
		diff := int64(b.VM) - int64(a.VM)
		if diff < 0 {
			diff = -diff
		}
		if diff > 128 {
			t.Errorf("implemented Q19 %s VM retained-growth%d exceeds128", name, diff)
		}
		if b.Full > a.Full {
			t.Errorf("implemented Q19 %s growing FULLSCAN%d ->%d", name, a.Full, b.Full)
		}
	}
}

func TestSQLiteCaptureIndexWorkQ19SelectedCanonicalAndTupleCorruptionRefused(t *testing.T) {
	type corruption struct {
		name, column string
		value        Value
		successor    bool
	}
	defects := []corruption{
		{"predecessor-start-json-index-mismatch", "start_json", Text(iwTimeJSON(101, 0)), false},
		{"predecessor-end-json-index-mismatch", "end_json", Text(iwTimeJSON(102, 0)), false},
		{"predecessor-start-nsec-index-mismatch", "start_nsec", Integer(1), false},
		{"predecessor-end-nsec-index-mismatch", "end_nsec", Integer(1), false},
		{"predecessor-same-instant-noncanonical-json", "start_json", Text("\"1970-01-01T00:01:40.000Z\""), false},
		{"successor-start-json-index-mismatch", "start_json", Text(iwTimeJSON(101, 0)), true},
		{"successor-end-sec-index-mismatch", "end_sec", Integer(102), true},
		{"successor-same-instant-noncanonical-json", "end_json", Text("\"1970-01-01T00:01:41+00:00\""), true},
		{"successor-invalid-utf8-json", "end_json", Text(string([]byte{0xff})), true},
		{"selected-saturating-duration-mismatch", "duration_ns", iwCounter(2), false},
		{"selected-group-tuple-mismatch", "group_order", Text("different stored tuple"), false},
	}
	dir := iwSeed(t, 1000)
	for _, defect := range defects {
		t.Run(defect.name, func(t *testing.T) {
			c := qaOpen(t, dir, iwName, false)
			tx := qaBegin(t, c, context.Background(), Write)
			startSec, startNS := int64(100), int64(2)
			if defect.successor {
				startSec, startNS = 99, 0
			}
			args := append(append(append([]Value{}, iwTimer()...), iwPoint(101, 0)...), iwPoint(startSec, startNS)...)
			ranges, snapshot := iwQ19Snapshot(t, tx)
			positive, err := iwMeasureQ19Neighbors(t, tx, args)
			if err != nil {
				t.Fatal("uncorrupted selected native scalar control", err)
			}
			iwCheckQ19Snapshot(t, args, positive, ranges, snapshot)
			selected := 0
			if defect.successor {
				selected = 1
			}
			if len(positive.q19Statements[selected].work.rows) != 1 || positive.q19Statements[selected].work.rows[0][0] != iwUUID(60, 1) {
				t.Fatal("corruption does not target the actual selected neighbor")
			}
			// Actual STRICT DDL permits these inconsistent projections. Refusal
			// must come from the measured full16 scalar, without reconsulting the
			// old snapshot for the overlap decision or pre-auditing the mutation.
			qaDone(t, tx, "UPDATE intervals SET "+defect.column+"=? WHERE interval_id=?", defect.value, Text(iwUUID(60, 1)))
			refused, err := iwMeasureQ19Neighbors(t, tx, args)
			if err == nil || !reflect.DeepEqual(refused, iwWork{}) {
				t.Fatal("selected full16 corruption leaked result/work", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			qaClose(t, c)
			c = qaOpen(t, dir, iwName, false)
			tx = qaBegin(t, c, context.Background(), Read)
			_, restored := iwQ19Snapshot(t, tx)
			if !reflect.DeepEqual(restored, snapshot) {
				t.Fatal("selected tuple corruption rollback did not restore full fixture")
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			qaClose(t, c)
		})
	}
}
