//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent source-only proposal for the approved native-normalization seam.
// Coordinator owns application, formatting and runtime verification.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const hnQASessionColumns = "source,native_session,session_key,incarnation,cwd,root_turn_key"
const hnQATurnColumns = "turn_key,source,native_session,incarnation,turn_id,agent_id,cwd,actor_key,actor_generation,stopped"
const hnQAToolColumns = "turn_key,tool_id,name,phase"
const hnQAMax = "18446744073709551615"

func hnQAClone(t *testing.T, st *state) *state {
	t.Helper()
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	var result state
	if err := json.Unmarshal(b, &result); err != nil {
		t.Fatal(err)
	}
	return &result
}

func hnQASource(t *testing.T, claude bool) (*qaHostHarness, *state) {
	t.Helper()
	var h *qaHostHarness
	if claude {
		h = qaNewClaude(t).qaHostHarness
	} else {
		h = qaNewHost(t)
	}
	event := func(kind, turn, agent string) HostEvent {
		e := h.event(kind, turn, agent)
		if claude {
			e.Source = "claude"
		}
		return e
	}
	start := event("SessionStart", "", "")
	start.SessionSource = "startup"
	h.send(0, start)
	h.send(1, event("UserPromptSubmit", "root", ""))
	h.send(2, event("SubagentStart", "child-turn", "child"))
	tool := event("PreToolUse", "root", "")
	tool.ToolID, tool.ToolName = "actual-tool", "Bash"
	h.send(3, tool)
	h.send(4, event("Stop", "actorless", ""))
	return h, bgQAReadLegacy(t, h.service)
}

func hnQATurn(key string, value *hostTurn) sqliteHostTurnRow {
	row := sqliteHostTurnRow{Key: key, Source: value.Source, NativeSession: value.SessionID, Incarnation: value.Session, TurnID: value.TurnID, AgentID: value.AgentID, CWD: value.CWD, Stopped: value.Stopped}
	if value.Actor != nil {
		ref := *value.Actor
		row.Actor = &ref
	}
	return row
}

func hnQACopyTurn(row sqliteHostTurnRow) sqliteHostTurnRow {
	if row.Actor != nil {
		ref := *row.Actor
		row.Actor = &ref
	}
	return row
}

func hnQAValid(t *testing.T, graph *state) *state {
	t.Helper()
	if !validState(graph) {
		t.Fatal("complete fixture is outside actual legacy validation")
	}
	h := qaNew(t)
	h.seed()
	return bgQALegacyMarshalOracle(t, h.service, h.path, graph, true)
}

func hnQACharge(t *testing.T, values []string, overhead int64) int64 {
	t.Helper()
	for _, value := range values {
		overhead += int64(len(bgQAPersistedString(t, value)))
	}
	return overhead
}

func hnQASessionCharge(t *testing.T, row sqliteHostSessionRow) int64 {
	s := row.Value
	return hnQACharge(t, []string{s.Source, s.NativeID, row.Key, s.ID, s.CWD, s.RootTurn}, 86)
}

func hnQATurnCharge(t *testing.T, row sqliteHostTurnRow) int64 {
	values := []string{row.Key, row.Source, row.NativeSession, row.Incarnation, row.TurnID, row.AgentID, row.CWD}
	overhead := int64(106)
	if row.Actor != nil {
		overhead = 130
		values = append(values, actorKey(row.Actor.Key))
	}
	return hnQACharge(t, values, overhead)
}

func hnQAToolCharge(t *testing.T, row sqliteHostToolRow) int64 {
	return hnQACharge(t, []string{row.TurnKey, row.ID, row.Value.Name, row.Value.Phase}, 68)
}

// Independent literal6/10/4 stored-kind and byte audit, never production charge.
func hnQAStoredCharge(t *testing.T, tx *sqliteio.Tx) int64 {
	t.Helper()
	var total int64
	for _, table := range []struct {
		name, columns string
		width         int
	}{{"host_sessions", hnQASessionColumns, 6}, {"host_turns", hnQATurnColumns, 10}, {"host_tools", hnQAToolColumns, 4}} {
		s := interopPrepare(t, tx, "SELECT "+table.columns+" FROM "+table.name)
		for {
			row, err := s.Step()
			if err != nil {
				t.Fatal(err)
			}
			if !row {
				break
			}
			if s.ColumnCount() != table.width {
				t.Fatal("literal row width changed")
			}
			total += 32
			present := false
			if table.width == 10 {
				k, err := s.Kind(7)
				if err != nil {
					t.Fatal(err)
				}
				present = k != sqliteio.NullKind
			}
			for i := 0; i < table.width; i++ {
				want := sqliteio.TextKind
				if table.width == 10 {
					if i == 9 {
						want = sqliteio.IntegerKind
					}
					if i == 7 && !present {
						want = sqliteio.NullKind
					}
					if i == 8 {
						if present {
							want = sqliteio.BlobKind
						} else {
							want = sqliteio.NullKind
						}
					}
				}
				kind, err := s.Kind(i)
				if err != nil || kind != want {
					t.Fatalf("%s column%d kind=%v want=%v error=%v", table.name, i, kind, want, err)
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
						t.Fatal("generation not BLOB8", err)
					}
					total += 8 + int64(len(value))
				}
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return total
}

func hnQASeed(t *testing.T, source, graph *state) (interopFixture, sqliteStoreMeta) {
	t.Helper()
	graph = hnQAValid(t, graph)
	f, before := bgQASeed(t, source)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	next := metaQANext(t, before)
	next.Revision = bump(before.Revision)
	for key, turn := range graph.HostTurns {
		row := hnQATurn(key, turn)
		if row.Actor != nil {
			delta, err := sqliteEnsureActorGeneration(tx, *row.Actor)
			if err != nil {
				t.Fatal(err)
			}
			next.LogicalBytes += delta
		}
		delta, err := sqliteWriteHostTurn(tx, graph.ComputerID, nil, row)
		if err != nil || delta != hnQATurnCharge(t, row) {
			t.Fatal("turn insert differs from independent charge", err)
		}
		next.LogicalBytes += delta
	}
	for key, session := range graph.HostSessions {
		row := sqliteHostSessionRow{Key: key, Value: *session}
		delta, err := sqliteWriteHostSession(tx, graph.ComputerID, nil, row)
		if err != nil || delta != hnQASessionCharge(t, row) {
			t.Fatal("session insert differs from independent charge", err)
		}
		next.LogicalBytes += delta
	}
	for key, turn := range graph.HostTurns {
		for id, tool := range turn.Tools {
			row := sqliteHostToolRow{TurnKey: key, ID: id, Value: tool}
			delta, err := sqliteWriteHostTool(tx, graph.ComputerID, nil, row)
			if err != nil || delta != hnQAToolCharge(t, row) {
				t.Fatal("tool insert differs from independent charge", err)
			}
			next.LogicalBytes += delta
		}
	}
	if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, before) {
		t.Fatal("revisionless row helpers changed metadata", err)
	}
	if err := sqliteUpdateMeta(tx, before, next); err != nil {
		t.Fatal(err)
	}
	if got := bgQAStoredCharge(t, tx) + hnQAStoredCharge(t, tx); got != next.LogicalBytes {
		t.Fatalf("independent stored audit=%d metadata=%d", got, next.LogicalBytes)
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return f, next
}

func hnQAReopen(t *testing.T, f interopFixture, meta sqliteStoreMeta, graph *state) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	for key, session := range graph.HostSessions {
		got, found, err := sqliteReadHostSession(tx, meta.ComputerID, session.Source, session.NativeID)
		if err != nil || !found || !reflect.DeepEqual(got, sqliteHostSessionRow{Key: key, Value: *session}) {
			t.Fatal("reopened session differs", err)
		}
	}
	tools := 0
	for key, turn := range graph.HostTurns {
		got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, key)
		if err != nil || !found || !reflect.DeepEqual(got, hnQATurn(key, turn)) {
			t.Fatal("reopened scalar turn differs", err)
		}
		wantPending := map[string]hostTool{}
		for id, tool := range turn.Tools {
			tools++
			got, found, err := sqliteReadHostTool(tx, meta.ComputerID, key, id)
			if err != nil || !found || got != (sqliteHostToolRow{TurnKey: key, ID: id, Value: tool}) {
				t.Fatal("reopened tool differs", err)
			}
			if tool.Phase == "pre" {
				wantPending[id] = tool
			}
		}
		pending, err := sqlitePendingHostTools(tx, meta.ComputerID, key)
		if err != nil || pending == nil || !reflect.DeepEqual(pending, wantPending) {
			t.Fatal("reopened pending set differs", err)
		}
	}
	if interopCount(t, tx, "SELECT count(*) FROM host_sessions") != int64(len(graph.HostSessions)) || interopCount(t, tx, "SELECT count(*) FROM host_turns") != int64(len(graph.HostTurns)) || interopCount(t, tx, "SELECT count(*) FROM host_tools") != int64(tools) || interopCount(t, tx, "SELECT count(*) FROM actors") != 0 {
		t.Fatal("helper lost history or invented current Actor")
	}
	if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, meta) {
		t.Fatal("reopened metadata differs", err)
	}
	if bgQAStoredCharge(t, tx)+hnQAStoredCharge(t, tx) != meta.LogicalBytes {
		t.Fatal("reopened aggregate charges differ")
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func hnQARoot(t *testing.T, graph *state) (string, *hostTurn) {
	t.Helper()
	for key, turn := range graph.HostTurns {
		if turn.AgentID == "" && turn.Actor != nil {
			return key, turn
		}
	}
	t.Fatal("actual legacy fixture lacks root Actor turn")
	return "", nil
}

func hnQAHistorical(t *testing.T, source *state) *state {
	t.Helper()
	graph := hnQAClone(t, source)
	graph.HostReceipts = nil // This row-family import is independently scoped.
	rootKey, root := hnQARoot(t, graph)
	root.CWD = "/synthetic/a/../b\x00cwd"
	ref := *root.Actor
	ref.Key.Source, ref.Generation = "manual-test", hnQAMax
	root.Actor = &ref
	for _, session := range graph.HostSessions {
		session.CWD = root.CWD
		session.RootTurn = rootKey
	}
	old := *root
	old.Session, old.Actor, old.Tools, old.Stopped = "01234567-89ab-cdef-0123-456789abcdef", nil, map[string]hostTool{"retained": {Name: "Bash", Phase: "post"}}, true
	graph.HostTurns[hostTurnKey(old.Session, HostEvent{TurnID: old.TurnID, AgentID: old.AgentID})] = &old
	return hnQAValid(t, graph)
}

func TestSQLiteHostNormalizationActualHistoryOwnedRoundTripAndLiteralCharge(t *testing.T) {
	_, source := hnQASource(t, false)
	graph := hnQAHistorical(t, source)
	f, meta := hnQASeed(t, source, graph)
	hnQAReopen(t, f, meta, graph)
	key, root := hnQARoot(t, graph)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, key)
	if err != nil || !found || got.Actor == root.Actor {
		t.Fatal("turn Actor is not an owned copy", err)
	}
	got.Actor.Generation = "1"
	if again, found, err := sqliteReadHostTurn(tx, meta.ComputerID, key); err != nil || !found || !reflect.DeepEqual(again, hnQATurn(key, root)) {
		t.Fatal("caller Actor mutation changed retained turn", err)
	}
	pending, err := sqlitePendingHostTools(tx, meta.ComputerID, key)
	if err != nil {
		t.Fatal(err)
	}
	pending["caller"] = hostTool{Name: "Bash", Phase: "pre"}
	if again, err := sqlitePendingHostTools(tx, meta.ComputerID, key); err != nil || len(again) != len(root.Tools) {
		t.Fatal("caller map mutation changed persisted pending set", err)
	}
	if row, found, err := sqliteReadHostTurn(tx, meta.ComputerID, strings.Repeat("a", 64)); err != nil || found || !reflect.DeepEqual(row, sqliteHostTurnRow{}) {
		t.Fatal("turn absence not zero", err)
	}
	if row, found, err := sqliteReadHostSession(tx, meta.ComputerID, "codex", "absent"); err != nil || found || !reflect.DeepEqual(row, sqliteHostSessionRow{}) {
		t.Fatal("session absence not zero", err)
	}
	if row, found, err := sqliteReadHostTool(tx, meta.ComputerID, key, "absent"); err != nil || found || row != (sqliteHostToolRow{}) {
		t.Fatal("tool absence not zero", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func hnQASet(t *testing.T, rows []sqliteHostTurnRow) map[string]sqliteHostTurnRow {
	t.Helper()
	if rows == nil {
		t.Fatal("successful candidate list must be allocated")
	}
	set := map[string]sqliteHostTurnRow{}
	for _, row := range rows {
		if _, found := set[row.Key]; found {
			t.Fatal("candidate repeated")
		}
		set[row.Key] = row
	}
	return set
}

func hnQALegacySet(turns []*hostTurn) map[string]sqliteHostTurnRow {
	set := map[string]sqliteHostTurnRow{}
	for _, turn := range turns {
		key := hostTurnKey(turn.Session, HostEvent{TurnID: turn.TurnID, AgentID: turn.AgentID})
		set[key] = hnQATurn(key, turn)
	}
	return set
}

func hnQAPlan(t *testing.T, tx *sqliteio.Tx, sql, index string, values ...sqliteio.Value) {
	t.Helper()
	bgQAPlan(t, tx, sql, index, values...)
	s := interopPrepare(t, tx, "EXPLAIN QUERY PLAN "+sql, values...)
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
			t.Fatal("bounded selector sorted retained history", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteHostNormalizationAllHistoricalAndToolCandidatesUseActualConsumerSets(t *testing.T) {
	_, source := hnQASource(t, false)
	graph := hnQAHistorical(t, source)
	_, template := hnQARoot(t, graph)
	for i := 0; i < 80; i++ {
		turn := *template
		turn.Tools = map[string]hostTool{}
		turn.Session = fmt.Sprintf("%08x-0000-0000-0000-000000000000", i+1)
		turn.Source = "codex"
		if i >= 8 && i < 16 {
			turn.Source = "claude"
		}
		turn.SessionID, turn.TurnID, turn.AgentID = "shared-native", "reused-turn", ""
		if i%4 != 0 {
			turn.AgentID = fmt.Sprintf("child-%d", i%4)
		}
		if i >= 16 {
			turn.SessionID = fmt.Sprintf("unrelated-%d", i)
		}
		turn.Actor = nil
		if i%3 != 0 {
			ref := *template.Actor
			ref.Key.SessionID, ref.Key.AgentID = turn.Session, hostAgent(turn.AgentID)
			turn.Actor = &ref
		}
		turn.Stopped = i%2 == 0
		key := hostTurnKey(turn.Session, HostEvent{TurnID: turn.TurnID, AgentID: turn.AgentID})
		graph.HostTurns[key] = &turn
	}
	claudeIncarnation := "00000009-0000-0000-0000-000000000000"
	claudeCurrent := &hostSession{Source: "claude", NativeID: "shared-native", ID: claudeIncarnation, CWD: template.CWD, RootTurn: hostTurnKey(claudeIncarnation, HostEvent{TurnID: "reused-turn"})}
	graph.HostSessions[hostSessionKey(HostEvent{Source: "claude", SessionID: claudeCurrent.NativeID})] = claudeCurrent
	graph = hnQAValid(t, graph)
	f, meta := hnQASeed(t, source, graph)
	hnQAReopen(t, f, meta, graph)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	for _, src := range []string{"codex", "claude"} {
		for _, agent := range []string{"", "child-1", "absent"} {
			e := HostEvent{Source: src, SessionID: "shared-native", TurnID: "reused-turn", AgentID: agent, Kind: "Stop", CWD: t.TempDir(), ToolID: "probe", ToolName: "Bash"}
			if agent != "" {
				e.Kind = "SubagentStop"
			}
			got, err := sqliteHistoricalHostTurns(tx, meta.ComputerID, e)
			if err != nil || !reflect.DeepEqual(hnQASet(t, got), hnQALegacySet(historicalActors(graph, e))) {
				t.Fatal("historical selector differs from actual full candidate set", err)
			}
			e.Kind = "PreToolUse"
			got, err = sqliteToolHostTurns(tx, meta.ComputerID, e)
			if err != nil || !reflect.DeepEqual(hnQASet(t, got), hnQALegacySet(toolCandidates(graph, e))) {
				t.Fatal("tool selector lost safety targets or invented newest selection", err)
			}
			if src == "codex" && agent == "" && len(got) != 8 {
				t.Fatal("omitted Codex agent must enumerate all8 targets, including nil/stopped and children")
			}
		}
	}
	// Actual Claude ambiguity consumers include old actorless/stopped incarnations.
	claudeEvent := HostEvent{Source: "claude", SessionID: "shared-native", TurnID: "reused-turn", AgentID: "", Kind: "SessionEnd"}
	current := graph.HostSessions[hostSessionKey(claudeEvent)]
	if current == nil {
		t.Fatal("strict-valid current Claude mapping absent")
	}
	if !ambiguousClaudeIdentity(graph, current, claudeEvent) {
		t.Fatal("source oracle lost old actorless SessionEnd ambiguity")
	}
	claudeEvent.Kind = "SubagentStart"
	claudeEvent.AgentID = "child-2"
	if !ambiguousClaudeIdentity(graph, current, claudeEvent) {
		t.Fatal("source oracle lost stopped/historical SubagentStart target")
	}
	hnQAPlan(t, tx, "SELECT "+hnQATurnColumns+" FROM host_turns WHERE source=? AND native_session=? AND turn_id=? AND agent_id=?", "turn_native", sqliteio.Text("codex"), sqliteio.Text("shared-native"), sqliteio.Text("reused-turn"), sqliteio.Text(""))
	hnQAPlan(t, tx, "SELECT "+hnQATurnColumns+" FROM host_turns WHERE source=? AND native_session=? AND turn_id=?", "turn_tool_target", sqliteio.Text("codex"), sqliteio.Text("shared-native"), sqliteio.Text("reused-turn"))
	key, root := hnQARoot(t, graph)
	hnQAPlan(t, tx, "SELECT "+hnQATurnColumns+" FROM host_turns WHERE turn_key=?", "sqlite_autoindex_host_turns_1", sqliteio.Text(key))
	hnQAPlan(t, tx, "SELECT "+hnQASessionColumns+" FROM host_sessions WHERE session_key=?", "sqlite_autoindex_host_sessions_2", sqliteio.Text(hostSessionKey(HostEvent{Source: root.Source, SessionID: root.SessionID})))
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteHostNormalizationPendingToolsSourceRulesOwnedMapsAndRetainedCompletion(t *testing.T) {
	for _, claude := range []bool{false, true} {
		_, source := hnQASource(t, claude)
		graph := hnQAHistorical(t, source)
		key, root := hnQARoot(t, graph)
		waitNames := []string{"wait_agent", "multi_agent_v1wait_agent"}
		if claude {
			waitNames = []string{"AskUserQuestion"}
		}
		root.Tools = map[string]hostTool{"completed": {Name: "Bash", Phase: "post"}}
		for i, name := range waitNames {
			root.Tools[fmt.Sprintf("wait%d", i)] = hostTool{Name: name, Phase: "pre"}
		}
		if claude {
			root.Tools["failed"] = hostTool{Name: "AskUserQuestion", Phase: "failed"}
		}
		graph = hnQAValid(t, graph)
		f, meta := hnQASeed(t, source, graph)
		hnQAReopen(t, f, meta, graph)
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		pending, err := sqlitePendingHostTools(tx, meta.ComputerID, key)
		if err != nil || len(pending) != len(waitNames) || !hostWaiting(&hostTurn{Source: root.Source, Tools: pending}) {
			t.Fatal("pending-only wait rule differs from actual consumer", err)
		}
		pending["caller-mutation"] = hostTool{Name: "Bash", Phase: "pre"}
		again, err := sqlitePendingHostTools(tx, meta.ComputerID, key)
		if err != nil || len(again) != len(waitNames) {
			t.Fatal("returned map aliases stored tools", err)
		}
		ordinary := sqliteHostToolRow{TurnKey: key, ID: "ordinary", Value: hostTool{Name: "prefix_AskUserQuestion", Phase: "pre"}}
		if _, err := sqliteWriteHostTool(tx, meta.ComputerID, nil, ordinary); err != nil {
			t.Fatal(err)
		}
		pending, err = sqlitePendingHostTools(tx, meta.ComputerID, key)
		if err != nil || hostWaiting(&hostTurn{Source: root.Source, Tools: pending}) {
			t.Fatal("mixed ordinary pending tool or suffix inference claimed waiting", err)
		}
		failed := sqliteHostToolRow{TurnKey: key, ID: "new-failed", Value: hostTool{Name: "Bash", Phase: "failed"}}
		delta, err := sqliteWriteHostTool(tx, meta.ComputerID, nil, failed)
		if claude {
			if err != nil || delta != hnQAToolCharge(t, failed) {
				t.Fatal("actual Claude parent rejected failed phase", err)
			}
		} else {
			bgQAValidation(t, err)
			if delta != 0 {
				t.Fatal("Codex failed phase returned charge")
			}
		}
		hnQAPlan(t, tx, "SELECT "+hnQAToolColumns+" FROM host_tools WHERE turn_key=? AND tool_id=?", "sqlite_autoindex_host_tools_1", sqliteio.Text(key), sqliteio.Text("completed"))
		hnQAPlan(t, tx, "SELECT "+hnQAToolColumns+" FROM host_tools WHERE turn_key=? AND phase='pre'", "tool_pending", sqliteio.Text(key))
		interopRollback(t, tx)
		interopClose(t, c)
		hnQAReopen(t, f, meta, graph)
	}
}

func TestSQLiteHostNormalizationActualJSONRawHashesAndDirectHistoricalLookup(t *testing.T) {
	h, source := hnQASource(t, false)
	raw := hnQAClone(t, source)
	key, root := hnQARoot(t, raw)
	delete(raw.HostTurns, key)
	badNative, badTurn := "native-"+string([]byte{0xff}), "turn-"+string([]byte{0xfe})
	root.SessionID, root.TurnID, root.Actor = badNative, badTurn, nil
	root.CWD = "/synthetic/../raw\x00cwd-" + string([]byte{0xff})
	root.Tools = map[string]hostTool{"short-" + string([]byte{0xff}): {Name: "name-" + string([]byte{0xfe}), Phase: "pre"}}
	rawTurnKey := hostTurnKey(root.Session, HostEvent{TurnID: badTurn})
	raw.HostTurns[rawTurnKey] = root
	var session *hostSession
	for k, value := range raw.HostSessions {
		session = value
		delete(raw.HostSessions, k)
		break
	}
	if session == nil {
		t.Fatal("actual host session absent")
	}
	session.NativeID, session.RootTurn, session.CWD = badNative, "", root.CWD
	rawSessionKey := hostSessionKey(HostEvent{Source: session.Source, SessionID: badNative})
	raw.HostSessions[rawSessionKey] = session
	// This executes the actual strict legacy read of owned fixture bytes.
	decoded := bgQALegacyMarshalOracle(t, h.service, h.path, raw, true)
	persistedNative, persistedTurn := bgQAPersistedString(t, badNative), bgQAPersistedString(t, badTurn)
	if rawSessionKey != hostSessionKey(HostEvent{Source: session.Source, SessionID: persistedNative}) || rawTurnKey != hostTurnKey(root.Session, HostEvent{TurnID: persistedTurn}) {
		t.Fatal("active JSON raw/decoded hash equality witness failed")
	}
	f, meta := hnQASeed(t, source, decoded)
	hnQAReopen(t, f, meta, decoded)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	got, found, err := sqliteReadHostSession(tx, meta.ComputerID, session.Source, badNative)
	if err != nil || !found || got.Key != rawSessionKey || got.Value.NativeID != persistedNative {
		t.Fatal("raw hashed session lookup incorrectly requires raw field equality", err)
	}
	turn, found, err := sqliteReadHostTurn(tx, meta.ComputerID, rawTurnKey)
	if err != nil || !found || turn.NativeSession != persistedNative || turn.TurnID != persistedTurn || turn.CWD != bgQAPersistedString(t, root.CWD) {
		t.Fatal("raw turn hash or copied final text differs from legacy oracle", err)
	}
	for _, native := range []string{badNative, persistedNative} {
		e := HostEvent{Source: session.Source, SessionID: native, TurnID: persistedTurn}
		rows, err := sqliteHistoricalHostTurns(tx, meta.ComputerID, e)
		if err != nil || !reflect.DeepEqual(hnQASet(t, rows), hnQALegacySet(historicalActors(decoded, e))) {
			t.Fatal("direct historical lookup repaired raw native input", err)
		}
	}
	for _, id := range []string{"short-" + string([]byte{0xff}), bgQAPersistedString(t, "short-"+string([]byte{0xff}))} {
		_, found, err := sqliteReadHostTool(tx, meta.ComputerID, rawTurnKey, id)
		if err != nil || found != (id == bgQAPersistedString(t, id)) {
			t.Fatal("direct tool lookup repaired raw ID", err)
		}
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteHostNormalizationWritersRepairOwnedShortCWDAndToolNameAtLegacyBoundary(t *testing.T) {
	_, source := hnQASource(t, false)
	graph := hnQAHistorical(t, source)
	key, root := hnQARoot(t, graph)
	var beforeSession sqliteHostSessionRow
	for k, value := range graph.HostSessions {
		beforeSession = sqliteHostSessionRow{Key: k, Value: *value}
		break
	}
	beforeTurn := hnQATurn(key, root)
	badCWD := "/raw/a/../b\x00cwd-" + string([]byte{0xff})
	badName := "raw tool name-" + string([]byte{0xfe})
	afterSession := beforeSession
	afterSession.Value.CWD = badCWD
	afterTurn := hnQACopyTurn(beforeTurn)
	afterTurn.CWD = badCWD
	tool := sqliteHostToolRow{TurnKey: key, ID: "raw-name-tool", Value: hostTool{Name: badName, Phase: "pre"}}
	rawGraph := hnQAClone(t, graph)
	rawGraph.HostSessions[beforeSession.Key].CWD = badCWD
	rawGraph.HostTurns[key].CWD = badCWD
	rawGraph.HostTurns[key].Tools[tool.ID] = tool.Value
	canonical := hnQAValid(t, rawGraph) // Complete state and actual strict store read.
	f, meta := hnQASeed(t, source, graph)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	d1, err := sqliteWriteHostSession(tx, meta.ComputerID, &beforeSession, afterSession)
	if err != nil || d1 != hnQASessionCharge(t, afterSession)-hnQASessionCharge(t, beforeSession) {
		t.Fatal("short malformed session CWD repair/charge differs", err)
	}
	d2, err := sqliteWriteHostTurn(tx, meta.ComputerID, &beforeTurn, afterTurn)
	if err != nil || d2 != hnQATurnCharge(t, afterTurn)-hnQATurnCharge(t, beforeTurn) {
		t.Fatal("short malformed turn CWD repair/charge differs", err)
	}
	d3, err := sqliteWriteHostTool(tx, meta.ComputerID, nil, tool)
	if err != nil || d3 != hnQAToolCharge(t, tool) {
		t.Fatal("short malformed tool name repair/charge differs", err)
	}
	if got, found, err := sqliteReadHostSession(tx, meta.ComputerID, beforeSession.Value.Source, beforeSession.Value.NativeID); err != nil || !found || got.Value != *canonical.HostSessions[beforeSession.Key] {
		t.Fatal("session copied encoding differs from actual legacy read", err)
	}
	if got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, key); err != nil || !found || !reflect.DeepEqual(got, hnQATurn(key, canonical.HostTurns[key])) {
		t.Fatal("turn copied encoding differs from actual legacy read", err)
	}
	if got, found, err := sqliteReadHostTool(tx, meta.ComputerID, key, tool.ID); err != nil || !found || got.Value != canonical.HostTurns[key].Tools[tool.ID] {
		t.Fatal("tool copied encoding differs from actual legacy read", err)
	}
	if afterSession.Value.CWD != badCWD || afterTurn.CWD != badCWD || tool.Value.Name != badName {
		t.Fatal("encoding rewrote caller bytes")
	}
	if bgQAStoredCharge(t, tx)+hnQAStoredCharge(t, tx) != meta.LogicalBytes+d1+d2+d3 {
		t.Fatal("copied UTF-8 stored audit differs from aggregate deltas")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	hnQAReopen(t, f, meta, graph)
}

func TestSQLiteHostNormalizationShortToolCollisionRetainsNativeEvidenceAndRollbackHistory(t *testing.T) {
	h, source := hnQASource(t, false)
	graph := hnQAHistorical(t, source)
	key, root := hnQARoot(t, graph)
	rawID := "short-" + string([]byte{0xff})
	persistedID := bgQAPersistedString(t, rawID)
	if rawID == persistedID || len(persistedID) > 256 {
		t.Fatal("short real collision witness was not established")
	}
	root.Tools[persistedID] = hostTool{Name: "existing ordinary", Phase: "post"}
	graph = hnQAValid(t, graph)
	// Prove this raw proposed tool independently has a strict-valid legacy read.
	raw := hnQAClone(t, source)
	_, rawRoot := hnQARoot(t, raw)
	rawRoot.Tools = map[string]hostTool{rawID: {Name: "new proposed name", Phase: "pre"}}
	decoded := bgQALegacyMarshalOracle(t, h.service, h.path, raw, true)
	_, decodedRoot := hnQARoot(t, decoded)
	if _, exists := decodedRoot.Tools[persistedID]; !exists {
		t.Fatal("actual legacy decode did not create genuine matching stored ID")
	}
	f, meta := hnQASeed(t, source, graph)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	if _, found, err := sqliteReadHostTool(tx, meta.ComputerID, key, rawID); err != nil || found {
		t.Fatal("raw direct tool lookup must remain absent", err)
	}
	sibling := sqliteHostToolRow{TurnKey: key, ID: "staged-sibling", Value: hostTool{Name: "Bash", Phase: "pre"}}
	if _, err := sqliteWriteHostTool(tx, meta.ComputerID, nil, sibling); err != nil {
		t.Fatal(err)
	}
	proposal := sqliteHostToolRow{TurnKey: key, ID: rawID, Value: hostTool{Name: "new proposed name", Phase: "pre"}}
	delta, err := sqliteWriteHostTool(tx, meta.ComputerID, nil, proposal)
	bgQAValidation(t, err)
	interopSafeError(t, err, rawID, proposal.Value.Name, f.directory)
	var checked *sqliteio.Error
	if delta != 0 || !errors.As(err, &checked) || checked.Category != sqliteio.Constraint || (checked.Code != 1555 && checked.Code != 2067) {
		t.Fatal("real short collision lost native PRIMARYKEY/UNIQUE evidence", err)
	}
	if proposal.ID != rawID || proposal.Value.Name != "new proposed name" {
		t.Fatal("collision changed raw caller bytes")
	}
	if got, found, err := sqliteReadHostTool(tx, meta.ComputerID, key, persistedID); err != nil || !found || got.Value != root.Tools[persistedID] {
		t.Fatal("collision replaced existing retained tool", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	hnQAReopen(t, f, meta, graph)
}

func TestSQLiteHostNormalizationFinal256ToolLengthRefusalBeforeMutationAndBoundaryControls(t *testing.T) {
	for _, field := range []string{"id", "name"} {
		t.Run(field, func(t *testing.T) {
			h, source := hnQASource(t, false)
			graph := hnQAHistorical(t, source)
			key, _ := hnQARoot(t, graph)
			f, meta := hnQASeed(t, source, graph)
			bad := strings.Repeat("x", 255) + string([]byte{0xff})
			if !safeIdentifier(bad, 256) || len(bgQAPersistedString(t, bad)) <= 256 {
				t.Fatal("raw-valid/final-invalid256 witness absent")
			}
			proposal := sqliteHostToolRow{TurnKey: key, ID: "length-candidate", Value: hostTool{Name: "Bash", Phase: "pre"}}
			if field == "id" {
				proposal.ID = bad
			} else {
				proposal.Value.Name = bad
			}
			raw := hnQAClone(t, source)
			_, rawRoot := hnQARoot(t, raw)
			rawRoot.Tools = map[string]hostTool{proposal.ID: proposal.Value}
			decoded := bgQALegacyMarshalOracle(t, h.service, h.path, raw, false)
			if validHostState(decoded) {
				t.Fatal("actual expanded legacy graph unexpectedly admitted")
			}
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			beforeCount := interopCount(t, tx, "SELECT count(*) FROM host_tools")
			if field == "id" {
				if got, found, err := sqliteReadHostTool(tx, meta.ComputerID, key, bad); err != nil || found || got != (sqliteHostToolRow{}) {
					t.Fatal("raw length-boundary lookup was repaired", err)
				}
			}
			delta, err := sqliteWriteHostTool(tx, meta.ComputerID, nil, proposal)
			bgQAValidation(t, err)
			interopSafeError(t, err, bad, f.directory)
			if delta != 0 || interopCount(t, tx, "SELECT count(*) FROM host_tools") != beforeCount {
				t.Fatal("final length refusal mutated table or returned delta")
			}
			if field == "id" && proposal.ID != bad || field == "name" && proposal.Value.Name != bad {
				t.Fatal("length refusal truncated/repaired caller")
			}
			control := proposal
			if field == "id" {
				control.ID = strings.Repeat("v", 256)
			} else {
				control.Value.Name = strings.Repeat("v", 256)
			}
			if delta, err := sqliteWriteHostTool(tx, meta.ComputerID, nil, control); err != nil || delta != hnQAToolCharge(t, control) {
				t.Fatal("otherwise-valid exact256 control failed", err)
			}
			if got, found, err := sqliteReadHostTool(tx, meta.ComputerID, control.TurnKey, control.ID); err != nil || !found || got != control {
				t.Fatal("strict-valid exact256 value was not lossless", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			hnQAReopen(t, f, meta, graph)
		})
	}
}

func hnQAShadow(t *testing.T, tx *sqliteio.Tx, table, columns string) {
	t.Helper()
	interopDone(t, tx, "ALTER TABLE "+table+" RENAME TO qa_original_"+table)
	interopDone(t, tx, "CREATE TABLE "+table+"("+columns+")")
	interopDone(t, tx, "INSERT INTO "+table+"("+columns+") SELECT "+columns+" FROM qa_original_"+table)
}

func TestSQLiteHostNormalizationFullOldCASAll6And10And4ColumnsAndImmutableIdentities(t *testing.T) {
	_, source := hnQASource(t, false)
	graph := hnQAHistorical(t, source)
	turnKey, turn := hnQARoot(t, graph)
	var session sqliteHostSessionRow
	for key, value := range graph.HostSessions {
		session = sqliteHostSessionRow{Key: key, Value: *value}
		break
	}
	tool := sqliteHostToolRow{TurnKey: turnKey, ID: "cas-tool", Value: hostTool{Name: "old name", Phase: "pre"}}
	turn.Tools[tool.ID] = tool.Value
	graph = hnQAValid(t, graph)
	f, meta := hnQASeed(t, source, graph)
	for _, family := range []struct{ table, columns string }{{"host_sessions", hnQASessionColumns}, {"host_turns", hnQATurnColumns}, {"host_tools", hnQAToolColumns}} {
		for i, column := range strings.Split(family.columns, ",") {
			t.Run(family.table+"/"+column, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				write := func() (int64, error) {
					switch family.table {
					case "host_sessions":
						after := session
						after.Value.CWD = "/valid new cwd"
						return sqliteWriteHostSession(tx, meta.ComputerID, &session, after)
					case "host_turns":
						before := hnQATurn(turnKey, turn)
						after := hnQACopyTurn(before)
						after.CWD = "/valid new cwd"
						return sqliteWriteHostTurn(tx, meta.ComputerID, &before, after)
					default:
						after := tool
						after.Value.Name = "valid replacement name"
						return sqliteWriteHostTool(tx, meta.ComputerID, &tool, after)
					}
				}
				// Prove the proposal is valid with the actual schema, then roll it
				// back before installing the isolated stale-old decoder/CAS fixture.
				if _, err := write(); err != nil {
					t.Fatal("valid compare-update control failed", err)
				}
				interopRollback(t, tx)
				interopClose(t, c)
				c, tx = interopOpen(t, f, false, sqliteio.Write)
				hnQAShadow(t, tx, family.table, family.columns)
				value := sqliteio.Text("different valid scalar")
				if strings.Contains(column, "key") {
					value = sqliteio.Text(strings.Repeat("z", 64))
				}
				if column == "source" {
					value = sqliteio.Text("claude")
				}
				if column == "incarnation" {
					value = sqliteio.Text("99999999-9999-4999-8999-999999999999")
				}
				if column == "cwd" {
					value = sqliteio.Text("/different\x00absolute cwd")
				}
				if family.table == "host_turns" && i == 8 {
					value = interopCounter(t, "1")
				}
				if family.table == "host_turns" && i == 9 {
					if turn.Stopped {
						value = sqliteio.Integer(0)
					} else {
						value = sqliteio.Integer(1)
					}
				}
				if family.table == "host_tools" && i == 3 {
					value = sqliteio.Text("post")
				}
				where, identity := "session_key", session.Key
				if family.table == "host_turns" {
					where, identity = "turn_key", turnKey
				}
				if family.table == "host_tools" {
					where, identity = "tool_id", tool.ID
				}
				interopDone(t, tx, "UPDATE "+family.table+" SET "+column+"=? WHERE "+where+"=?", value, sqliteio.Text(identity))
				delta, err := write()
				bgQACorrupt(t, err)
				if delta != 0 {
					t.Fatal("stale materialized old column returned delta")
				}
				interopSafeError(t, err, f.directory)
				interopRollback(t, tx)
				interopClose(t, c)
				hnQAReopen(t, f, meta, graph)
			})
		}
	}
	for _, identity := range []string{"session-source", "session-native", "session-key", "turn-key", "turn-source", "turn-native", "turn-incarnation", "turn-id", "turn-agent", "tool-parent", "tool-id"} {
		t.Run("identity/"+identity, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			var delta int64
			var err error
			switch {
			case strings.HasPrefix(identity, "session-"):
				after := session
				switch identity {
				case "session-source":
					after.Value.Source = "claude"
				case "session-native":
					after.Value.NativeID = "different native"
				case "session-key":
					after.Key = strings.Repeat("q", 64)
				}
				delta, err = sqliteWriteHostSession(tx, meta.ComputerID, &session, after)
			case strings.HasPrefix(identity, "turn-"):
				before := hnQATurn(turnKey, turn)
				after := hnQACopyTurn(before)
				switch identity {
				case "turn-key":
					after.Key = strings.Repeat("q", 64)
				case "turn-source":
					after.Source = "claude"
				case "turn-native":
					after.NativeSession = "different native"
				case "turn-incarnation":
					after.Incarnation = "99999999-9999-4999-8999-999999999999"
				case "turn-id":
					after.TurnID = "different turn"
				case "turn-agent":
					after.AgentID = "different agent"
				}
				delta, err = sqliteWriteHostTurn(tx, meta.ComputerID, &before, after)
			default:
				after := tool
				if identity == "tool-parent" {
					after.TurnKey = strings.Repeat("q", 64)
				} else {
					after.ID = "different id"
				}
				delta, err = sqliteWriteHostTool(tx, meta.ComputerID, &tool, after)
			}
			bgQAValidation(t, err)
			if delta != 0 {
				t.Fatal("immutable identity rewrite returned charge")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			hnQAReopen(t, f, meta, graph)
		})
	}
}

func TestSQLiteHostNormalizationStagedSignedDeltasReplacementAndCurrentIncarnationRetainsHistory(t *testing.T) {
	_, source := hnQASource(t, false)
	graph := hnQAHistorical(t, source)
	key, root := hnQARoot(t, graph)
	f, meta := hnQASeed(t, source, graph)
	for _, cwd := range []string{root.CWD, "/a much longer proposed absolute path", "/s"} {
		before := hnQATurn(key, root)
		after := hnQACopyTurn(before)
		after.CWD = cwd
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		delta, err := sqliteWriteHostTurn(tx, meta.ComputerID, &before, after)
		if err != nil || delta != hnQATurnCharge(t, after)-hnQATurnCharge(t, before) {
			t.Fatal("turn replacement signed delta differs", err)
		}
		if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, meta) {
			t.Fatal("row replacement changed metadata", err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
		hnQAReopen(t, f, meta, graph)
	}
	for sessionKey, value := range graph.HostSessions {
		before := sqliteHostSessionRow{Key: sessionKey, Value: *value}
		for _, cwd := range []string{value.CWD, "/a much longer proposed absolute session directory", "/s"} {
			after := before
			after.Value.CWD = cwd
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			delta, err := sqliteWriteHostSession(tx, meta.ComputerID, &before, after)
			if err != nil || delta != hnQASessionCharge(t, after)-hnQASessionCharge(t, before) {
				t.Fatal("session signed delta differs", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			hnQAReopen(t, f, meta, graph)
		}
	}
	for id, value := range root.Tools {
		before := sqliteHostToolRow{TurnKey: key, ID: id, Value: value}
		for _, name := range []string{value.Name, "a much longer valid tool name", "x"} {
			after := before
			after.Value.Name = name
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			delta, err := sqliteWriteHostTool(tx, meta.ComputerID, &before, after)
			if err != nil || delta != hnQAToolCharge(t, after)-hnQAToolCharge(t, before) {
				t.Fatal("tool signed delta differs", err)
			}
			if bgQAStoredCharge(t, tx)+hnQAStoredCharge(t, tx) != meta.LogicalBytes+delta {
				t.Fatal("staged tool value audit differs from delta")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			hnQAReopen(t, f, meta, graph)
		}
	}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	var childKey string
	var child *hostTurn
	for k, turn := range graph.HostTurns {
		if turn.AgentID != "" && turn.Actor != nil {
			childKey, child = k, turn
			break
		}
	}
	if child == nil {
		t.Fatal("actual fixture lacks a child for optional-Actor composition")
	}
	before := hnQATurn(childKey, child)
	after := hnQACopyTurn(before)
	after.Actor, after.Stopped = nil, true
	delta, err := sqliteWriteHostTurn(tx, meta.ComputerID, &before, after)
	if err != nil || delta != hnQATurnCharge(t, after)-hnQATurnCharge(t, before) {
		t.Fatal("optional Actor replacement delta differs", err)
	}
	total := delta
	for sessionKey, session := range graph.HostSessions {
		old := sqliteHostSessionRow{Key: sessionKey, Value: *session}
		afterSession := sqliteHostSessionRow{Key: sessionKey, Value: *session}
		afterSession.Value.ID, afterSession.Value.RootTurn = "99999999-9999-4999-8999-999999999999", ""
		d, err := sqliteWriteHostSession(tx, meta.ComputerID, &old, afterSession)
		if err != nil || d != hnQASessionCharge(t, afterSession)-hnQASessionCharge(t, old) {
			t.Fatal("current incarnation update failed", err)
		}
		total += d
		*session = afterSession.Value
	}
	last := metaQANext(t, meta)
	last.Revision, last.LogicalBytes = bump(meta.Revision), meta.LogicalBytes+total
	if err := sqliteUpdateMeta(tx, meta, last); err != nil {
		t.Fatal(err)
	}
	if bgQAStoredCharge(t, tx)+hnQAStoredCharge(t, tx) != last.LogicalBytes {
		t.Fatal("composition counted dependencies/children more than once")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	child.Actor, child.Stopped = nil, true
	graph = hnQAValid(t, graph)
	hnQAReopen(t, f, last, graph)
}

func TestSQLiteHostNormalizationOptionalActorValidStalePairsAndHistoricalGenerationOnly(t *testing.T) {
	_, source := hnQASource(t, false)
	graph := hnQAHistorical(t, source)
	_, template := hnQARoot(t, graph)
	var key string
	var turn *hostTurn
	for k, value := range graph.HostTurns {
		if value.Actor == nil {
			key, turn = k, value
			break
		}
	}
	if turn == nil {
		t.Fatal("actual fixture lacks actorless/stopped history")
	}
	f, meta := hnQASeed(t, source, graph)
	for _, present := range []bool{false, true} {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		ref := *template.Actor
		ref.Key.SessionID, ref.Key.AgentID = turn.Session, hostAgent(turn.AgentID)
		if _, err := sqliteEnsureActorGeneration(tx, ref); err != nil {
			t.Fatal(err)
		}
		alternate := ref
		alternate.Generation = "1"
		if _, err := sqliteEnsureActorGeneration(tx, alternate); err != nil {
			t.Fatal(err)
		}
		before := hnQATurn(key, turn)
		if present {
			after := hnQACopyTurn(before)
			after.Actor = &ref
			if delta, err := sqliteWriteHostTurn(tx, meta.ComputerID, &before, after); err != nil || delta != hnQATurnCharge(t, after)-hnQATurnCharge(t, before) {
				t.Fatal("valid nil-to-present control failed", err)
			}
			before = after
		}
		actual := hnQACopyTurn(before)
		if present {
			actual.Actor = &alternate
		} else {
			actual.Actor = &ref
		}
		interopDone(t, tx, "UPDATE host_turns SET actor_key=?,actor_generation=? WHERE turn_key=?", sqliteio.Text(actorKey(actual.Actor.Key)), interopCounter(t, actual.Actor.Generation), sqliteio.Text(key))
		if got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, key); err != nil || !found || !reflect.DeepEqual(got, actual) {
			t.Fatal("fully valid stale Actor witness did not decode", err)
		}
		proposal := hnQACopyTurn(before)
		proposal.CWD = "/valid replacement proposal"
		delta, err := sqliteWriteHostTurn(tx, meta.ComputerID, &before, proposal)
		bgQACorrupt(t, err)
		if delta != 0 || interopCount(t, tx, "SELECT count(*) FROM actors") != 0 {
			t.Fatal("stale Actor replacement charged/invented current head")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		hnQAReopen(t, f, meta, graph)
	}
}

func TestSQLiteHostNormalizationSelectedDecoderAndDependenciesDiscardPartialOutputs(t *testing.T) {
	_, source := hnQASource(t, false)
	graph := hnQAHistorical(t, source)
	key, root := hnQARoot(t, graph)
	root.Tools["decoder"] = hostTool{Name: "wait_agent", Phase: "pre"}
	graph = hnQAValid(t, graph)
	var session sqliteHostSessionRow
	for k, value := range graph.HostSessions {
		session = sqliteHostSessionRow{Key: k, Value: *value}
		break
	}
	f, meta := hnQASeed(t, source, graph)
	type defect struct {
		name, family, sql, relaxed string
		values                     []sqliteio.Value
	}
	cases := []defect{
		{"session-kind", "session", "UPDATE host_sessions SET incarnation=42", "host_sessions", nil},
		{"session-source-projection", "session", "UPDATE host_sessions SET source='claude'", "", nil},
		{"session-native-projection", "session", "UPDATE host_sessions SET native_session='different native'", "", nil},
		{"session-cwd", "session", "UPDATE host_sessions SET cwd='relative'", "", nil},
		{"missing-root", "session", "UPDATE host_sessions SET root_turn_key=?", "", []sqliteio.Value{sqliteio.Text(strings.Repeat("m", 64))}},
		{"extra-session", "session", "INSERT INTO host_sessions(" + hnQASessionColumns + ") SELECT " + hnQASessionColumns + " FROM host_sessions", "host_sessions", nil},
		{"turn-cwd", "turn", "UPDATE host_turns SET cwd='relative' WHERE turn_key=?", "", []sqliteio.Value{sqliteio.Text(key)}},
		{"turn-key-projection", "turn", "UPDATE host_turns SET turn_id='different turn' WHERE turn_key=?", "", []sqliteio.Value{sqliteio.Text(key)}},
		{"turn-null-pair", "turn", "UPDATE host_turns SET actor_key=NULL WHERE turn_key=?", "host_turns", []sqliteio.Value{sqliteio.Text(key)}},
		{"generation-kind", "turn", "UPDATE host_turns SET actor_generation='00000001' WHERE turn_key=?", "host_turns", []sqliteio.Value{sqliteio.Text(key)}},
		{"generation-width", "turn", "UPDATE host_turns SET actor_generation=X'01' WHERE turn_key=?", "host_turns", []sqliteio.Value{sqliteio.Text(key)}},
		{"stopped-flag", "turn", "UPDATE host_turns SET stopped=2 WHERE turn_key=?", "host_turns", []sqliteio.Value{sqliteio.Text(key)}},
		{"extra-turn", "turn", "INSERT INTO host_turns(" + hnQATurnColumns + ") SELECT " + hnQATurnColumns + " FROM host_turns WHERE turn_key=?", "host_turns", []sqliteio.Value{sqliteio.Text(key)}},
		{"missing-generation", "turn", "DELETE FROM actor_generations WHERE actor_key=?", "", []sqliteio.Value{sqliteio.Text(actorKey(root.Actor.Key))}},
		{"generation-projection", "turn", "UPDATE actor_generations SET session_id='different projection' WHERE actor_key=?", "", []sqliteio.Value{sqliteio.Text(actorKey(root.Actor.Key))}},
		{"tool-kind", "tool", "UPDATE host_tools SET name=42 WHERE tool_id='decoder'", "host_tools", nil},
		{"tool-name", "tool", "UPDATE host_tools SET name='' WHERE tool_id='decoder'", "", nil},
		{"tool-phase", "tool", "UPDATE host_tools SET phase='invalid' WHERE tool_id='decoder'", "host_tools", nil},
		{"codex-failed", "tool", "UPDATE host_tools SET phase='failed' WHERE tool_id='decoder'", "", nil},
		{"missing-parent", "tool", "DELETE FROM host_turns WHERE turn_key=?", "", []sqliteio.Value{sqliteio.Text(key)}},
		{"extra-tool", "tool", "INSERT INTO host_tools(" + hnQAToolColumns + ") SELECT " + hnQAToolColumns + " FROM host_tools WHERE tool_id='decoder'", "host_tools", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			// Full positive controls before the corruption/shadow fixture.
			if _, found, err := sqliteReadHostSession(tx, meta.ComputerID, session.Value.Source, session.Value.NativeID); err != nil || !found {
				t.Fatal("session baseline invalid", err)
			}
			if _, found, err := sqliteReadHostTurn(tx, meta.ComputerID, key); err != nil || !found {
				t.Fatal("turn baseline invalid", err)
			}
			if _, found, err := sqliteReadHostTool(tx, meta.ComputerID, key, "decoder"); err != nil || !found {
				t.Fatal("tool baseline invalid", err)
			}
			if tc.relaxed != "" {
				columns := hnQASessionColumns
				if tc.relaxed == "host_turns" {
					columns = hnQATurnColumns
				}
				if tc.relaxed == "host_tools" {
					columns = hnQAToolColumns
				}
				hnQAShadow(t, tx, tc.relaxed, columns)
			}
			interopDone(t, tx, tc.sql, tc.values...)
			var err error
			switch tc.family {
			case "session":
				row, found, readErr := sqliteReadHostSession(tx, meta.ComputerID, session.Value.Source, session.Value.NativeID)
				err = readErr
				if found || !reflect.DeepEqual(row, sqliteHostSessionRow{}) {
					t.Fatal("corrupt session exposed a usable result")
				}
			case "turn":
				row, found, readErr := sqliteReadHostTurn(tx, meta.ComputerID, key)
				err = readErr
				if found || !reflect.DeepEqual(row, sqliteHostTurnRow{}) {
					t.Fatal("corrupt turn exposed a usable result")
				}
			case "tool":
				row, found, readErr := sqliteReadHostTool(tx, meta.ComputerID, key, "decoder")
				err = readErr
				if found || row != (sqliteHostToolRow{}) {
					t.Fatal("corrupt tool exposed a usable result")
				}
			}
			bgQACorrupt(t, err)
			interopSafeError(t, err, f.directory)
			interopRollback(t, tx)
			interopClose(t, c)
			hnQAReopen(t, f, meta, graph)
		})
	}
}

// Observe the actual fixed selector's native row visitation solely to establish
// a valid-prefix witness. No particular public candidate/map order is required.
func hnQAObservedPrefix(t *testing.T, tx *sqliteio.Tx, sql string, keyColumn int, badKey string, values ...sqliteio.Value) int {
	t.Helper()
	s := interopPrepare(t, tx, sql, values...)
	prefix, seen := 0, false
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !row {
			break
		}
		key, err := s.Text(keyColumn)
		if err != nil {
			t.Fatal(err)
		}
		if key == badKey {
			seen = true
		} else if !seen {
			prefix++
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !seen {
		t.Fatal("corruption fixture was not selected by the approved query")
	}
	return prefix
}

func TestSQLiteHostNormalizationLaterCandidateAndPendingErrorsClearAccumulatedResults(t *testing.T) {
	_, source := hnQASource(t, false)
	graph := hnQAHistorical(t, source)
	key, root := hnQARoot(t, graph)
	root.Tools["later-error"] = hostTool{Name: "wait_agent", Phase: "pre"}
	graph = hnQAValid(t, graph)
	f, meta := hnQASeed(t, source, graph)
	e := HostEvent{Source: root.Source, SessionID: root.SessionID, TurnID: root.TurnID}
	for _, selector := range []string{"historical", "tool-targets"} {
		candidates := historicalActors(graph, e)
		if selector == "tool-targets" {
			candidates = toolCandidates(graph, e)
		}
		if len(candidates) < 2 {
			t.Fatal("later-error candidate fixture needs at least two valid rows")
		}
		observedLater := false
		for _, candidate := range candidates {
			badKey := hostTurnKey(candidate.Session, HostEvent{TurnID: candidate.TurnID, AgentID: candidate.AgentID})
			t.Run(selector+"/"+badKey, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				// Every selected row/dependency is independently valid before each
				// isolated scalar corruption; it does not change query membership.
				for _, value := range candidates {
					k := hostTurnKey(value.Session, HostEvent{TurnID: value.TurnID, AgentID: value.AgentID})
					if got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, k); err != nil || !found || !reflect.DeepEqual(got, hnQATurn(k, value)) {
						t.Fatal("candidate positive control failed", err)
					}
				}
				interopDone(t, tx, "UPDATE host_turns SET cwd='relative' WHERE turn_key=?", sqliteio.Text(badKey))
				query := "SELECT " + hnQATurnColumns + " FROM host_turns WHERE source=? AND native_session=? AND turn_id=?"
				values := []sqliteio.Value{sqliteio.Text(e.Source), sqliteio.Text(e.SessionID), sqliteio.Text(e.TurnID)}
				if selector == "historical" {
					query += " AND agent_id=?"
					values = append(values, sqliteio.Text(e.AgentID))
				}
				if hnQAObservedPrefix(t, tx, query, 0, badKey, values...) > 0 {
					observedLater = true
				}
				var rows []sqliteHostTurnRow
				var err error
				if selector == "historical" {
					rows, err = sqliteHistoricalHostTurns(tx, meta.ComputerID, e)
				} else {
					rows, err = sqliteToolHostTurns(tx, meta.ComputerID, e)
				}
				bgQACorrupt(t, err)
				if rows != nil {
					t.Fatal("candidate decoder returned accumulated rows with an error")
				}
				interopRollback(t, tx)
				interopClose(t, c)
				hnQAReopen(t, f, meta, graph)
			})
		}
		if !observedLater {
			t.Fatal("candidate corruptions never established a valid prefix before an error")
		}
	}
	pendingCount, observedLater := 0, false
	for id, value := range root.Tools {
		if value.Phase != "pre" {
			continue
		}
		pendingCount++
		t.Run("pending/"+id, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			for toolID, tool := range root.Tools {
				if tool.Phase != "pre" {
					continue
				}
				if got, found, err := sqliteReadHostTool(tx, meta.ComputerID, key, toolID); err != nil || !found || got.Value != tool {
					t.Fatal("pending tool positive control failed", err)
				}
			}
			// Append a control byte, rather than replacing name with empty TEXT
			// that would always sort before the other tool_pending names.
			interopDone(t, tx, "UPDATE host_tools SET name=? WHERE turn_key=? AND tool_id=?", sqliteio.Text(value.Name+"\x00"), sqliteio.Text(key), sqliteio.Text(id))
			if hnQAObservedPrefix(t, tx, "SELECT "+hnQAToolColumns+" FROM host_tools WHERE turn_key=? AND phase='pre'", 1, id, sqliteio.Text(key)) > 0 {
				observedLater = true
			}
			pending, err := sqlitePendingHostTools(tx, meta.ComputerID, key)
			bgQACorrupt(t, err)
			if pending != nil {
				t.Fatal("pending decoder returned an accumulated map with an error")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			hnQAReopen(t, f, meta, graph)
		})
	}
	if pendingCount < 2 || !observedLater {
		t.Fatal("pending corruptions never established a valid prefix before an error")
	}
}

func TestSQLiteHostNormalizationSessionRootGuardsUseOtherwiseValidRetainedTargets(t *testing.T) {
	_, source := hnQASource(t, false)
	graph := hnQAHistorical(t, source)
	correctKey, correct := hnQARoot(t, graph)
	var session sqliteHostSessionRow
	for k, value := range graph.HostSessions {
		session = sqliteHostSessionRow{Key: k, Value: *value}
		break
	}
	alternatives := map[string]string{}
	for _, guard := range []string{"agent", "incarnation", "native-session", "source", "actor-present"} {
		turn := *correct
		turn.TurnID, turn.Tools = "otherwise-valid-"+guard, map[string]hostTool{}
		switch guard {
		case "agent":
			turn.AgentID = "otherwise-valid-child"
		case "incarnation":
			turn.Session = "99999999-9999-4999-8999-999999999999"
		case "native-session":
			turn.SessionID = "otherwise-valid-native"
		case "source":
			turn.Source = "claude"
		}
		ref := *correct.Actor
		ref.Key.SessionID, ref.Key.AgentID = turn.Session, hostAgent(turn.AgentID)
		turn.Actor = &ref
		if guard == "actor-present" {
			turn.Actor = nil
		}
		key := hostTurnKey(turn.Session, HostEvent{TurnID: turn.TurnID, AgentID: turn.AgentID})
		graph.HostTurns[key], alternatives[guard] = &turn, key
	}
	graph = hnQAValid(t, graph) // All alternatives have a strict-valid legacy read.
	f, meta := hnQASeed(t, source, graph)
	for guard, alternativeKey := range alternatives {
		t.Run(guard, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if got, found, err := sqliteReadHostSession(tx, meta.ComputerID, session.Value.Source, session.Value.NativeID); err != nil || !found || !reflect.DeepEqual(got, session) {
				t.Fatal("correct-root session positive control failed", err)
			}
			if got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, correctKey); err != nil || !found || !reflect.DeepEqual(got, hnQATurn(correctKey, graph.HostTurns[correctKey])) {
				t.Fatal("correct root positive control failed", err)
			}
			if got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, alternativeKey); err != nil || !found || !reflect.DeepEqual(got, hnQATurn(alternativeKey, graph.HostTurns[alternativeKey])) {
				t.Fatal("otherwise-valid retained target positive control failed", err)
			}
			// Only session selection changes. The target's hashed turn key,
			// Actor projection/dependency and every scalar field stay valid.
			interopDone(t, tx, "UPDATE host_sessions SET root_turn_key=? WHERE session_key=?", sqliteio.Text(alternativeKey), sqliteio.Text(session.Key))
			got, found, err := sqliteReadHostSession(tx, meta.ComputerID, session.Value.Source, session.Value.NativeID)
			bgQACorrupt(t, err)
			if found || !reflect.DeepEqual(got, sqliteHostSessionRow{}) {
				t.Fatal("mismatching valid retained target bypassed session root guard")
			}
			if target, found, err := sqliteReadHostTurn(tx, meta.ComputerID, alternativeKey); err != nil || !found || !reflect.DeepEqual(target, hnQATurn(alternativeKey, graph.HostTurns[alternativeKey])) {
				t.Fatal("session refusal changed the otherwise-valid target", err)
			}
			interopDone(t, tx, "UPDATE host_sessions SET root_turn_key=? WHERE session_key=?", sqliteio.Text(correctKey), sqliteio.Text(session.Key))
			if got, found, err := sqliteReadHostSession(tx, meta.ComputerID, session.Value.Source, session.Value.NativeID); err != nil || !found || !reflect.DeepEqual(got, session) {
				t.Fatal("restored correct root failed", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			hnQAReopen(t, f, meta, graph)
		})
	}
}

func TestSQLiteHostNormalizationFiniteInsertVsUpdateAndSelectedComputerScope(t *testing.T) {
	_, source := hnQASource(t, false)
	graph := hnQAHistorical(t, source)
	key, root := hnQARoot(t, graph)
	root.Tools["finite"] = hostTool{Name: "Bash", Phase: "pre"}
	graph = hnQAValid(t, graph)
	var session sqliteHostSessionRow
	for k, value := range graph.HostSessions {
		session = sqliteHostSessionRow{Key: k, Value: *value}
		break
	}
	f, meta := hnQASeed(t, source, graph)
	for _, family := range []string{"session", "turn", "tool"} {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		sibling := sqliteHostToolRow{TurnKey: key, ID: "staged-sibling", Value: hostTool{Name: "Bash", Phase: "pre"}}
		if _, err := sqliteWriteHostTool(tx, meta.ComputerID, nil, sibling); err != nil {
			t.Fatal(err)
		}
		var delta int64
		var err error
		switch family {
		case "session":
			delta, err = sqliteWriteHostSession(tx, meta.ComputerID, nil, session)
		case "turn":
			delta, err = sqliteWriteHostTurn(tx, meta.ComputerID, nil, hnQATurn(key, root))
		case "tool":
			delta, err = sqliteWriteHostTool(tx, meta.ComputerID, nil, sqliteHostToolRow{TurnKey: key, ID: "finite", Value: root.Tools["finite"]})
		}
		bgQAValidation(t, err)
		var checked *sqliteio.Error
		if delta != 0 || !errors.As(err, &checked) || checked.Category != sqliteio.Constraint || (checked.Code != 1555 && checked.Code != 2067) {
			t.Fatal("finite INSERT silently updated/merged or lost native conflict", err)
		}
		interopSafeError(t, err, f.directory)
		interopRollback(t, tx)
		interopClose(t, c)
		hnQAReopen(t, f, meta, graph)
	}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	missingSession := sqliteHostSessionRow{Value: hostSession{Source: "claude", NativeID: "missing-native", ID: "99999999-9999-4999-8999-999999999999", CWD: "/valid cwd"}}
	missingSession.Key = hostSessionKey(HostEvent{Source: missingSession.Value.Source, SessionID: missingSession.Value.NativeID})
	if delta, err := sqliteWriteHostSession(tx, meta.ComputerID, &missingSession, missingSession); delta != 0 {
		t.Fatal("missing session UPDATE inserted")
	} else {
		bgQACorrupt(t, err)
	}
	missingTurn := hnQATurn(key, root)
	missingTurn.TurnID, missingTurn.Actor = "missing-turn", nil
	missingTurn.Key = hostTurnKey(missingTurn.Incarnation, HostEvent{TurnID: missingTurn.TurnID, AgentID: missingTurn.AgentID})
	if delta, err := sqliteWriteHostTurn(tx, meta.ComputerID, &missingTurn, missingTurn); delta != 0 {
		t.Fatal("missing turn UPDATE inserted")
	} else {
		bgQACorrupt(t, err)
	}
	missingTool := sqliteHostToolRow{TurnKey: key, ID: "missing-tool", Value: hostTool{Name: "Bash", Phase: "pre"}}
	if delta, err := sqliteWriteHostTool(tx, meta.ComputerID, &missingTool, missingTool); delta != 0 {
		t.Fatal("missing tool UPDATE inserted")
	} else {
		bgQACorrupt(t, err)
	}
	missingTool.TurnKey = strings.Repeat("m", 64)
	if delta, err := sqliteWriteHostTool(tx, meta.ComputerID, nil, missingTool); delta != 0 {
		t.Fatal("writer invented missing parent")
	} else {
		bgQACorrupt(t, err)
	}
	// Fully valid foreign Ref isolates scope from key/projection corruption.
	foreign := *root.Actor
	foreign.Key.ComputerID = "99999999-9999-4999-8999-999999999999"
	if _, err := sqliteEnsureActorGeneration(tx, foreign); err != nil {
		t.Fatal(err)
	}
	if got, found, err := sqliteReadActorGeneration(tx, foreign); err != nil || !found || got != foreign {
		t.Fatal("foreign generation positive control failed", err)
	}
	interopDone(t, tx, "UPDATE host_turns SET actor_key=?,actor_generation=? WHERE turn_key=?", sqliteio.Text(actorKey(foreign.Key)), interopCounter(t, foreign.Generation), sqliteio.Text(key))
	if got, found, err := sqliteReadHostTurn(tx, foreign.Key.ComputerID, key); err != nil || !found || got.Actor == nil || *got.Actor != foreign {
		t.Fatal("foreign turn scope positive control failed", err)
	}
	if got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, key); found || !reflect.DeepEqual(got, sqliteHostTurnRow{}) {
		t.Fatal("wrong computer exposed a turn")
	} else {
		bgQACorrupt(t, err)
	}
	if got, found, err := sqliteReadHostTool(tx, foreign.Key.ComputerID, key, "finite"); err != nil || !found || got.Value != root.Tools["finite"] {
		t.Fatal("foreign parent/tool positive control failed", err)
	}
	if got, found, err := sqliteReadHostTool(tx, meta.ComputerID, key, "finite"); found || got != (sqliteHostToolRow{}) {
		t.Fatal("wrong parent computer exposed a tool")
	} else {
		bgQACorrupt(t, err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	hnQAReopen(t, f, meta, graph)
}

func TestSQLiteHostNormalizationCallerCancellationAllNineOperations(t *testing.T) {
	_, source := hnQASource(t, false)
	graph := hnQAHistorical(t, source)
	key, root := hnQARoot(t, graph)
	root.Tools["cancel"] = hostTool{Name: "Bash", Phase: "pre"}
	graph = hnQAValid(t, graph)
	var session sqliteHostSessionRow
	for k, value := range graph.HostSessions {
		session = sqliteHostSessionRow{Key: k, Value: *value}
		break
	}
	f, meta := hnQASeed(t, source, graph)
	for _, operation := range []string{"session-read", "turn-read", "tool-read", "session-write", "turn-write", "tool-write", "historical", "tool-targets", "pending"} {
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
			cancel()
			switch operation {
			case "session-read":
				row, found, e := sqliteReadHostSession(tx, meta.ComputerID, session.Value.Source, session.Value.NativeID)
				err = e
				if found || !reflect.DeepEqual(row, sqliteHostSessionRow{}) {
					t.Fatal("canceled session read exposed success")
				}
			case "turn-read":
				row, found, e := sqliteReadHostTurn(tx, meta.ComputerID, key)
				err = e
				if found || !reflect.DeepEqual(row, sqliteHostTurnRow{}) {
					t.Fatal("canceled turn read exposed success")
				}
			case "tool-read":
				row, found, e := sqliteReadHostTool(tx, meta.ComputerID, key, "cancel")
				err = e
				if found || row != (sqliteHostToolRow{}) {
					t.Fatal("canceled tool read exposed success")
				}
			case "session-write":
				after := session
				after.Value.CWD = "/canceled proposal"
				delta, e := sqliteWriteHostSession(tx, meta.ComputerID, &session, after)
				err = e
				if delta != 0 {
					t.Fatal("canceled session write exposed delta")
				}
			case "turn-write":
				before := hnQATurn(key, root)
				after := hnQACopyTurn(before)
				after.CWD = "/canceled proposal"
				delta, e := sqliteWriteHostTurn(tx, meta.ComputerID, &before, after)
				err = e
				if delta != 0 {
					t.Fatal("canceled turn write exposed delta")
				}
			case "tool-write":
				before := sqliteHostToolRow{TurnKey: key, ID: "cancel", Value: root.Tools["cancel"]}
				after := before
				after.Value.Name = "canceled proposal"
				delta, e := sqliteWriteHostTool(tx, meta.ComputerID, &before, after)
				err = e
				if delta != 0 {
					t.Fatal("canceled tool write exposed delta")
				}
			case "historical", "tool-targets":
				e := HostEvent{Source: root.Source, SessionID: root.SessionID, TurnID: root.TurnID}
				var rows []sqliteHostTurnRow
				if operation == "historical" {
					rows, err = sqliteHistoricalHostTurns(tx, meta.ComputerID, e)
				} else {
					rows, err = sqliteToolHostTurns(tx, meta.ComputerID, e)
				}
				if rows != nil {
					t.Fatal("canceled candidate selection exposed partial array")
				}
			case "pending":
				pending, e := sqlitePendingHostTools(tx, meta.ComputerID, key)
				err = e
				if pending != nil {
					t.Fatal("canceled pending selection exposed partial map")
				}
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatal("caller cancellation evidence replaced", err)
			}
			interopSafeError(t, err, f.directory)
			if cleanup := tx.Rollback(); cleanup != nil && !errors.Is(cleanup, context.Canceled) {
				interopSafeError(t, cleanup, f.directory)
			}
			interopClose(t, c)
			hnQAReopen(t, f, meta, graph)
		})
	}
}

// v3 independent regressions: validate the owned final host shape before any
// row mutation. Raw identifiers, their JSON-derived hashes and historical Refs
// retain the existing contract; short fitting malformed strings remain accepted.
const hnQAIdentityIncarnation = "12345678-1234-4234-8234-123456789abc"

func hnQAIdentityTurn(t *testing.T, graph *state) sqliteHostTurnRow {
	t.Helper()
	_, root := hnQARoot(t, graph)
	row := sqliteHostTurnRow{Source: root.Source, NativeSession: "identity-native", Incarnation: hnQAIdentityIncarnation, TurnID: "identity-turn", CWD: root.CWD}
	row.Key = hostTurnKey(row.Incarnation, HostEvent{TurnID: row.TurnID, AgentID: row.AgentID})
	return row
}

func hnQAIdentityGraph(t *testing.T, graph *state, session *sqliteHostSessionRow, turn *sqliteHostTurnRow) *state {
	t.Helper()
	raw := hnQAClone(t, graph)
	if session != nil {
		value := session.Value
		raw.HostSessions[session.Key] = &value
	}
	if turn != nil {
		value := &hostTurn{Source: turn.Source, SessionID: turn.NativeSession, Session: turn.Incarnation, TurnID: turn.TurnID, AgentID: turn.AgentID, CWD: turn.CWD, Tools: map[string]hostTool{}, Stopped: turn.Stopped}
		if turn.Actor != nil {
			ref := *turn.Actor
			value.Actor = &ref
		}
		raw.HostTurns[turn.Key] = value
	}
	return raw
}

// Snapshot literal stored kinds and bytes, independent of production decoders.
// Counts alone would miss a nil-Actor UPDATE that overwrites an existing row.
func hnQAIdentitySnapshot(t *testing.T, tx *sqliteio.Tx) map[string][][]string {
	t.Helper()
	result := map[string][][]string{}
	for _, table := range []struct{ name, columns, order string }{
		{"host_sessions", hnQASessionColumns, "session_key"},
		{"host_turns", hnQATurnColumns, "turn_key"},
		{"host_tools", hnQAToolColumns, "turn_key,tool_id"},
		{"actor_generations", bgQAGenerationColumns, "actor_key,generation"},
	} {
		result[table.name] = [][]string{}
		s := interopPrepare(t, tx, "SELECT "+table.columns+" FROM "+table.name+" ORDER BY "+table.order)
		for {
			present, err := s.Step()
			if err != nil {
				t.Fatal(err)
			}
			if !present {
				break
			}
			values := []string{}
			for i := 0; i < s.ColumnCount(); i++ {
				kind, err := s.Kind(i)
				if err != nil {
					t.Fatal(err)
				}
				var value string
				switch kind {
				case sqliteio.TextKind:
					value, err = s.Text(i)
				case sqliteio.BlobKind:
					var b []byte
					b, err = s.Blob(i)
					value = fmt.Sprintf("%x", b)
				case sqliteio.IntegerKind:
					var n int64
					n, err = s.Int64(i)
					value = fmt.Sprintf("%d", n)
				case sqliteio.NullKind:
					value = "NULL"
				default:
					t.Fatal("unexpected stored identity kind", kind)
				}
				if err != nil {
					t.Fatal(err)
				}
				values = append(values, fmt.Sprintf("%v:%s", kind, value))
			}
			result[table.name] = append(result[table.name], values)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func hnQAIdentityStage(t *testing.T, tx *sqliteio.Tx, f interopFixture, meta sqliteStoreMeta, graph *state, priorDelta int64) sqliteStoreMeta {
	t.Helper()
	rootKey, _ := hnQARoot(t, graph)
	sibling := sqliteHostToolRow{TurnKey: rootKey, ID: "identity-staged-sibling", Value: hostTool{Name: "Bash", Phase: "pre"}}
	delta, err := sqliteWriteHostTool(tx, meta.ComputerID, nil, sibling)
	if err != nil || delta != hnQAToolCharge(t, sibling) {
		t.Fatal("valid staged sibling failed", err)
	}
	if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, meta) {
		t.Fatal("revisionless staging changed metadata", err)
	}
	next := metaQANext(t, meta)
	next.Revision = bump(meta.Revision)
	next.LogicalBytes += priorDelta + delta
	if got := bgQAStoredCharge(t, tx) + hnQAStoredCharge(t, tx); got != next.LogicalBytes {
		t.Fatal("staged independent charge mismatch")
	}
	if err := sqliteUpdateMeta(tx, meta, next); err != nil {
		t.Fatal(err)
	}
	return next
}

func hnQAIdentityUnchanged(t *testing.T, tx *sqliteio.Tx, f interopFixture, next sqliteStoreMeta, snapshot map[string][][]string) {
	t.Helper()
	if !reflect.DeepEqual(hnQAIdentitySnapshot(t, tx), snapshot) {
		t.Fatal("refused final identity changed stored rows")
	}
	if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, next) {
		t.Fatal("refused final identity changed staged metadata", err)
	}
	if bgQAStoredCharge(t, tx)+hnQAStoredCharge(t, tx) != next.LogicalBytes {
		t.Fatal("refused final identity changed charge")
	}
}

func hnQAIdentityReopen(t *testing.T, f interopFixture, meta sqliteStoreMeta, graph *state, generations int64) {
	t.Helper()
	hnQAReopen(t, f, meta, graph)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	if interopCount(t, tx, "SELECT count(*) FROM actor_generations") != generations {
		t.Fatal("caller rollback retained a staged historical generation")
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteHostNormalizationFinalChildActorIdentityRefusalInsertAndUpdate(t *testing.T) {
	for _, update := range []bool{false, true} {
		name := "insert"
		if update {
			name = "nil-to-present-actor-update"
		}
		t.Run(name, func(t *testing.T) {
			h, source := hnQASource(t, false)
			graph := hnQAHistorical(t, source)
			proposal := hnQAIdentityTurn(t, graph)
			proposal.AgentID = "short-child-" + string([]byte{0xff})
			proposal.Key = hostTurnKey(proposal.Incarnation, HostEvent{TurnID: proposal.TurnID, AgentID: proposal.AgentID})
			ref := ActorRef{Key: ActorKey{ComputerID: graph.ComputerID, Source: "manual-test", SessionID: proposal.Incarnation, AgentID: hostAgent(proposal.AgentID)}, Generation: "1"}
			proposal.Actor = &ref
			original := hnQACopyTurn(proposal)
			raw := hnQAIdentityGraph(t, graph, nil, &proposal)
			decoded := bgQALegacyMarshalOracle(t, h.service, h.path, raw, false)
			final := hnQATurn(proposal.Key, decoded.HostTurns[proposal.Key])
			if final.AgentID != bgQAPersistedString(t, proposal.AgentID) || final.AgentID == proposal.AgentID || !safeIdentifier(final.AgentID, 128) || final.Actor == nil || *final.Actor != ref || final.Actor.Key.AgentID == hostAgent(final.AgentID) {
				t.Fatal("short child Ref drift witness was not isolated")
			}
			if validHostState(decoded) || proposal.Key != hostTurnKey(final.Incarnation, HostEvent{TurnID: final.TurnID, AgentID: final.AgentID}) {
				t.Fatal("refusal was masked by hash drift or not established")
			}
			f, meta := hnQASeed(t, source, graph)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			generations := interopCount(t, tx, "SELECT count(*) FROM actor_generations")
			generationDelta, err := sqliteEnsureActorGeneration(tx, ref)
			if err != nil || generationDelta != bgQAGenerationCharge(t, ref) {
				t.Fatal("raw historical Ref dependency staging failed", err)
			}
			if got, found, err := sqliteReadActorGeneration(tx, ref); err != nil || !found || got != ref {
				t.Fatal("raw historical Ref dependency missing or changed", err)
			}
			priorDelta := generationDelta
			var before *sqliteHostTurnRow
			if update {
				actorless := hnQACopyTurn(proposal)
				actorless.Actor = nil
				accepted := bgQALegacyMarshalOracle(t, h.service, h.path, hnQAIdentityGraph(t, graph, nil, &actorless), true)
				want := hnQATurn(actorless.Key, accepted.HostTurns[actorless.Key])
				delta, err := sqliteWriteHostTurn(tx, meta.ComputerID, nil, actorless)
				if err != nil || delta != hnQATurnCharge(t, actorless) {
					t.Fatal("short malformed actorless INSERT control failed", err)
				}
				priorDelta += delta
				if got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, actorless.Key); err != nil || !found || !reflect.DeepEqual(got, want) {
					t.Fatal("actorless exact read did not accept canonical repaired identity", err)
				}
				control := hnQACopyTurn(actorless)
				control.CWD = actorless.CWD + "/cas-control"
				if delta, err := sqliteWriteHostTurn(tx, meta.ComputerID, &actorless, control); err != nil || delta != hnQATurnCharge(t, control)-hnQATurnCharge(t, actorless) {
					t.Fatal("valid raw-identity full-old CAS control failed", err)
				}
				want.CWD = control.CWD
				if got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, actorless.Key); err != nil || !found || !reflect.DeepEqual(got, want) {
					t.Fatal("valid raw-identity CAS control read differs", err)
				}
				if delta, err := sqliteWriteHostTurn(tx, meta.ComputerID, &control, actorless); err != nil || delta != hnQATurnCharge(t, actorless)-hnQATurnCharge(t, control) {
					t.Fatal("valid CAS control restore failed", err)
				}
				want.CWD = actorless.CWD
				if got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, actorless.Key); err != nil || !found || !reflect.DeepEqual(got, want) {
					t.Fatal("restored actorless old witness read differs", err)
				}
				before = &actorless
			}
			next := hnQAIdentityStage(t, tx, f, meta, graph, priorDelta)
			snapshot := hnQAIdentitySnapshot(t, tx)
			delta, err := sqliteWriteHostTurn(tx, meta.ComputerID, before, proposal)
			bgQAValidation(t, err)
			interopSafeError(t, err, proposal.AgentID, ref.Key.AgentID, f.directory)
			if delta != 0 || !reflect.DeepEqual(proposal, original) {
				t.Fatal("child Actor refusal charged or rewrote caller")
			}
			if before != nil {
				oldOriginal := hnQACopyTurn(original)
				oldOriginal.Actor = nil
				if !reflect.DeepEqual(*before, oldOriginal) {
					t.Fatal("child Actor refusal rewrote old witness")
				}
			}
			hnQAIdentityUnchanged(t, tx, f, next, snapshot)
			interopRollback(t, tx)
			interopClose(t, c)
			hnQAIdentityReopen(t, f, meta, graph, generations)
		})
	}
}

func TestSQLiteHostNormalizationFinalSessionAndTurnIdentityLengthRefusal(t *testing.T) {
	for _, field := range []string{"session-native", "turn-native", "turn-id", "actorless-agent"} {
		t.Run(field, func(t *testing.T) {
			h, source := hnQASource(t, false)
			graph := hnQAHistorical(t, source)
			limit := 256
			if field == "actorless-agent" {
				limit = 128
			}
			bad := strings.Repeat("x", limit-1) + string([]byte{0xff})
			if len(bad) != limit || !safeIdentifier(bad, limit) || len(bgQAPersistedString(t, bad)) != limit+2 {
				t.Fatal("exact raw-to-final expansion witness missing")
			}
			turn := hnQAIdentityTurn(t, graph)
			session := sqliteHostSessionRow{Value: hostSession{ID: hnQAIdentityIncarnation, Source: turn.Source, NativeID: bad, CWD: turn.CWD}}
			session.Key = hostSessionKey(HostEvent{Source: session.Value.Source, SessionID: session.Value.NativeID})
			if field == "turn-native" {
				turn.NativeSession = bad
			}
			if field == "turn-id" {
				turn.TurnID = bad
			}
			if field == "actorless-agent" {
				turn.AgentID = bad
			}
			turn.Key = hostTurnKey(turn.Incarnation, HostEvent{TurnID: turn.TurnID, AgentID: turn.AgentID})
			var raw *state
			if field == "session-native" {
				raw = hnQAIdentityGraph(t, graph, &session, nil)
			} else {
				raw = hnQAIdentityGraph(t, graph, nil, &turn)
			}
			decoded := bgQALegacyMarshalOracle(t, h.service, h.path, raw, false)
			var final string
			if field == "session-native" {
				value := decoded.HostSessions[session.Key]
				final = value.NativeID
				if value.RootTurn != "" || session.Key != hostSessionKey(HostEvent{Source: value.Source, SessionID: value.NativeID}) {
					t.Fatal("session final-length witness coupled to root or hash drift")
				}
			} else {
				value := decoded.HostTurns[turn.Key]
				if value.Actor != nil || turn.Key != hostTurnKey(value.Session, HostEvent{TurnID: value.TurnID, AgentID: value.AgentID}) {
					t.Fatal("turn final-length witness coupled to Actor or hash drift")
				}
				switch field {
				case "turn-native":
					final = value.SessionID
				case "turn-id":
					final = value.TurnID
				case "actorless-agent":
					final = value.AgentID
				}
			}
			if final != bgQAPersistedString(t, bad) || len(final) != limit+2 || safeIdentifier(final, limit) || validHostState(decoded) {
				t.Fatal("actual strict-reader final-length refusal not established")
			}
			originalSession, originalTurn := session, hnQACopyTurn(turn)
			f, meta := hnQASeed(t, source, graph)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			generations := interopCount(t, tx, "SELECT count(*) FROM actor_generations")
			next := hnQAIdentityStage(t, tx, f, meta, graph, 0)
			snapshot := hnQAIdentitySnapshot(t, tx)
			var delta int64
			var err error
			if field == "session-native" {
				delta, err = sqliteWriteHostSession(tx, meta.ComputerID, nil, session)
			} else {
				delta, err = sqliteWriteHostTurn(tx, meta.ComputerID, nil, turn)
			}
			bgQAValidation(t, err)
			interopSafeError(t, err, bad, f.directory)
			if delta != 0 || session != originalSession || !reflect.DeepEqual(turn, originalTurn) {
				t.Fatal("final length refusal charged or rewrote caller")
			}
			hnQAIdentityUnchanged(t, tx, f, next, snapshot)
			interopRollback(t, tx)
			interopClose(t, c)
			hnQAIdentityReopen(t, f, meta, graph, generations)
		})
	}
}

func TestSQLiteHostNormalizationFittingIdentityRepairsAndCanonicalBoundaries(t *testing.T) {
	for _, field := range []string{"session-native", "turn-native", "turn-id", "actorless-agent", "canonical-child"} {
		for _, boundary := range []bool{false, true} {
			if field == "canonical-child" && boundary {
				continue
			}
			name := field + "/short-repair"
			if boundary {
				name = field + "/exact-final-boundary"
			}
			t.Run(name, func(t *testing.T) {
				h, source := hnQASource(t, false)
				graph := hnQAHistorical(t, source)
				turn := hnQAIdentityTurn(t, graph)
				limit := 256
				if field == "actorless-agent" {
					limit = 128
				}
				value := "fitting-" + string([]byte{0xff})
				if boundary {
					value = strings.Repeat("v", limit)
				}
				session := sqliteHostSessionRow{Value: hostSession{ID: hnQAIdentityIncarnation, Source: turn.Source, NativeID: value, CWD: turn.CWD}}
				session.Key = hostSessionKey(HostEvent{Source: session.Value.Source, SessionID: session.Value.NativeID})
				if field == "turn-native" {
					turn.NativeSession = value
				}
				if field == "turn-id" {
					turn.TurnID = value
				}
				if field == "actorless-agent" {
					turn.AgentID = value
				}
				if field == "canonical-child" {
					turn.AgentID = bgQAPersistedString(t, value)
					turn.Actor = &ActorRef{Key: ActorKey{ComputerID: graph.ComputerID, Source: "manual-test", SessionID: turn.Incarnation, AgentID: hostAgent(turn.AgentID)}, Generation: "1"}
				}
				turn.Key = hostTurnKey(turn.Incarnation, HostEvent{TurnID: turn.TurnID, AgentID: turn.AgentID})
				var raw *state
				if field == "session-native" {
					raw = hnQAIdentityGraph(t, graph, &session, nil)
				} else {
					raw = hnQAIdentityGraph(t, graph, nil, &turn)
				}
				decoded := bgQALegacyMarshalOracle(t, h.service, h.path, raw, true)
				originalSession, originalTurn := session, hnQACopyTurn(turn)
				f, meta := hnQASeed(t, source, graph)
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				generations := interopCount(t, tx, "SELECT count(*) FROM actor_generations")
				if turn.Actor != nil {
					if delta, err := sqliteEnsureActorGeneration(tx, *turn.Actor); err != nil || delta != bgQAGenerationCharge(t, *turn.Actor) {
						t.Fatal("canonical child generation charge differs", err)
					}
					if got, found, err := sqliteReadActorGeneration(tx, *turn.Actor); err != nil || !found || got != *turn.Actor {
						t.Fatal("canonical child dependency unavailable", err)
					}
				}
				if field == "session-native" {
					want := sqliteHostSessionRow{Key: session.Key, Value: *decoded.HostSessions[session.Key]}
					if delta, err := sqliteWriteHostSession(tx, meta.ComputerID, nil, session); err != nil || delta != hnQASessionCharge(t, session) {
						t.Fatal("fitting session INSERT failed", err)
					}
					if got, found, err := sqliteReadHostSession(tx, meta.ComputerID, session.Value.Source, session.Value.NativeID); err != nil || !found || got != want {
						t.Fatal("raw hashed session control not canonical", err)
					}
					after := session
					after.Value.CWD += "/control"
					if delta, err := sqliteWriteHostSession(tx, meta.ComputerID, &session, after); err != nil || delta != hnQASessionCharge(t, after)-hnQASessionCharge(t, session) {
						t.Fatal("fitting session raw-identity CAS failed", err)
					}
					want.Value.CWD = after.Value.CWD
					if got, found, err := sqliteReadHostSession(tx, meta.ComputerID, session.Value.Source, session.Value.NativeID); err != nil || !found || got != want {
						t.Fatal("fitting session CAS read differs", err)
					}
				} else {
					want := hnQATurn(turn.Key, decoded.HostTurns[turn.Key])
					if delta, err := sqliteWriteHostTurn(tx, meta.ComputerID, nil, turn); err != nil || delta != hnQATurnCharge(t, turn) {
						t.Fatal("fitting turn INSERT failed", err)
					}
					if got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, turn.Key); err != nil || !found || !reflect.DeepEqual(got, want) {
						t.Fatal("raw hashed turn control not canonical", err)
					}
					after := hnQACopyTurn(turn)
					after.CWD += "/control"
					if delta, err := sqliteWriteHostTurn(tx, meta.ComputerID, &turn, after); err != nil || delta != hnQATurnCharge(t, after)-hnQATurnCharge(t, turn) {
						t.Fatal("fitting turn raw-identity CAS failed", err)
					}
					want.CWD = after.CWD
					if got, found, err := sqliteReadHostTurn(tx, meta.ComputerID, turn.Key); err != nil || !found || !reflect.DeepEqual(got, want) {
						t.Fatal("fitting turn CAS read differs", err)
					}
				}
				if session != originalSession || !reflect.DeepEqual(turn, originalTurn) {
					t.Fatal("positive encoding control rewrote caller")
				}
				interopRollback(t, tx)
				interopClose(t, c)
				hnQAIdentityReopen(t, f, meta, graph, generations)
			})
		}
	}
}
