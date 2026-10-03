//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Owned complete legacy graphs are semantic oracles only. These fresh SQL
// fixtures populate the selected-dependency families through their real APIs;
// interval/frontier/outbox and unrelated request families remain separate gates.

import (
	"reflect"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const sdQAMissing = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"

func sdQAClosedHistory(t *testing.T) *state {
	t.Helper()
	_, st := asQALegacy(t)
	// Quarantine retains an open historical segment; explicitly close at its
	// confirmed prefix for the CLOSED segment mutation/reverse-edge cases.
	for _, u := range st.Uncertainties {
		seg := st.Segments[u.SegmentID]
		if seg == nil {
			t.Fatal("real historical segment absent")
		}
		if seg.End == nil {
			end := seg.Confirmed
			seg.End = &end
		}
	}
	return st
}

func sdQAAudit(t *testing.T, tx *sqliteio.Tx, m sqliteStoreMeta) (map[string][][]string, int64) {
	t.Helper()
	result := map[string][][]string{}
	total := int64(114 + len(m.ComputerID) + len(m.StateBasename) + len(m.DatabaseBasename))
	for _, table := range []struct{ name, columns, order string }{
		{"bindings", bgQABindingColumns, "binding_id"},
		{"actor_generations", bgQAGenerationColumns, "actor_key,generation"},
		{"epochs", epQAColumns, "creation_ordinal"},
		{"actors", asQAActorColumns, "actor_key"},
		{"segments", asQASegmentColumns, "segment_id"},
		{"actor_uncertainties", "actor_key,ordinal,uncertainty_id", "actor_key,ordinal"},
		{"segment_events", "segment_id,ordinal,event_reference", "segment_id,ordinal"},
		{"uncertainties", asQAUncertaintyColumns, "uncertainty_id"},
		{"uncertainty_evidence", asQAEvidenceColumns, "uncertainty_id"},
		{"recovery_decisions", rdQAColumns, "uncertainty_id"},
		{"requests", rpQAColumns, "request_id"},
		{"host_sessions", "source,native_session,session_key,incarnation,cwd,root_turn_key", "session_key"},
		{"host_turns", "turn_key,source,native_session,incarnation,turn_id,agent_id,cwd,actor_key,actor_generation,stopped", "turn_key"},
		{"host_tools", "turn_key,tool_id,name,phase", "turn_key,tool_id"},
		{"host_receipts", hrQAColumns, "receipt_key"},
		{"event_receipts", erQAReceiptColumns, "event_key"},
		{"event_receipt_uncertainties", erQAChildColumns, "event_key,ordinal"},
		{"event_ids", "event_id,event_key", "event_id"},
	} {
		rows, charge := asQALiteralAudit(t, tx, table.name, table.columns, table.order)
		result[table.name] = rows
		total += charge
	}
	rows, _ := asQALiteralAudit(t, tx, "store_meta", "singleton,schema_version,legacy_schema_version,computer_id,revision,sync_enabled,durability_nonce,state_basename,database_basename,migration_id,backup_sha256,logical_bytes", "singleton")
	result["store_meta"] = rows
	return result, total
}

func sdQASelection(st *state) sqliteDependencySelection {
	var s sqliteDependencySelection
	for _, a := range st.Actors {
		s.ActorKeys = append(s.ActorKeys, a.Ref.Key)
		for ordinal := range a.UncertaintyIDs {
			s.ActorUncertaintyEdges = append(s.ActorUncertaintyEdges, sqliteActorUncertaintySelection{Key: a.Ref.Key, Ordinal: int64(ordinal)})
		}
	}
	for id, seg := range st.Segments {
		s.SegmentIDs = append(s.SegmentIDs, id)
		for ordinal := range seg.EventReferences {
			s.SegmentEventEdges = append(s.SegmentEventEdges, sqliteSegmentEventSelection{SegmentID: id, Ordinal: int64(ordinal)})
		}
	}
	for id := range st.Uncertainties {
		s.UncertaintyIDs = append(s.UncertaintyIDs, id)
	}
	for _, e := range st.Epochs {
		s.EpochIDs = append(s.EpochIDs, e.ID)
	}
	for id, r := range st.Requests {
		if r.Operation == "activity.resolve" && r.MutationResult != nil {
			s.ResolveRequestIDs = append(s.ResolveRequestIDs, id)
		}
	}
	for _, row := range st.HostSessions {
		s.HostSessions = append(s.HostSessions, sqliteHostSessionIdentity{Source: row.Source, NativeSession: row.NativeID})
	}
	for key, row := range st.HostTurns {
		s.HostTurnKeys = append(s.HostTurnKeys, key)
		for id := range row.Tools {
			s.HostTools = append(s.HostTools, sqliteHostToolIdentity{TurnKey: key, ToolID: id})
		}
	}
	for key := range st.HostReceipts {
		s.HostReceiptKeys = append(s.HostReceiptKeys, key)
	}
	for key := range st.Receipts {
		s.EventReceiptKeys = append(s.EventReceiptKeys, key)
	}
	return s
}

func sdQASeed(t *testing.T, raw *state) (interopFixture, sqliteStoreMeta, map[string][][]string, *state) {
	t.Helper()
	// This invokes real full validState, exact/strict JSON and fileStore.read on
	// an owned file. No partial state is ever passed to the legacy validator.
	st := hnQAValid(t, raw)
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	bgQAFixtureError(t, "fresh schema", sqliteCreateSchema(tx))
	m := interopMeta(f)
	m.ComputerID, m.Revision, m.SyncEnabled = st.ComputerID, st.Revision, st.SyncEnabled
	m.LogicalBytes = int64(114 + len(m.ComputerID) + len(m.StateBasename) + len(m.DatabaseBasename))
	add := func(delta int64, err error) {
		t.Helper()
		if err != nil {
			t.Fatal("real fixture writer", err)
		}
		m.LogicalBytes += delta
	}
	ensure := func(ref ActorRef) { add(sqliteEnsureActorGeneration(tx, ref)) }
	for _, row := range bgQARowsFromLegacy(st) {
		add(sqliteInsertBinding(tx, row))
	}
	for _, a := range st.Actors {
		ensure(a.Ref)
		if a.Parent != nil {
			ensure(*a.Parent)
		}
	}
	for _, seg := range st.Segments {
		ensure(seg.Actor)
	}
	for _, u := range st.Uncertainties {
		ensure(u.Actor)
	}
	for _, r := range st.Receipts {
		ensure(r.Result.Actor)
	}
	for _, r := range st.HostReceipts {
		if r.Result.Actor != nil {
			ensure(*r.Result.Actor)
		}
	}
	for _, row := range st.HostTurns {
		if row.Actor != nil {
			ensure(*row.Actor)
		}
	}
	for _, row := range epQARows(st) {
		add(sqliteInsertEpoch(tx, row))
	}
	// Actual deferred cycles are staged through the independently completed APIs.
	for _, u := range st.Uncertainties {
		add(sqliteWriteUncertainty(tx, st.ComputerID, nil, *u))
	}
	for id, e := range st.UncertaintyEvidence {
		add(sqliteWriteUncertaintyEvidence(tx, id, nil, e))
	}
	for _, seg := range st.Segments {
		add(sqliteWriteSegmentLocal(tx, st.ComputerID, nil, asQASegment(seg)))
	}
	for _, a := range st.Actors {
		add(sqliteWriteActorLocal(tx, st.ComputerID, nil, asQAActor(a)))
	}
	for _, seg := range st.Segments {
		for ordinal, key := range seg.EventReferences {
			add(sqliteAppendSegmentEventLocal(tx, st.ComputerID, seg.ID, int64(ordinal), key))
		}
	}
	for _, a := range st.Actors {
		for ordinal, id := range a.UncertaintyIDs {
			add(sqliteAppendActorUncertaintyLocal(tx, st.ComputerID, a.Ref.Key, int64(ordinal), id))
		}
	}
	for id, receipt := range st.Requests {
		if receipt.Operation == "activity.resolve" && receipt.MutationResult != nil {
			add(sqliteInsertResolveRequest(tx, sqliteResolveRequestRow{ID: id, Fingerprint: receipt.Fingerprint, Result: *receipt.MutationResult}))
		}
	}
	for _, d := range st.RecoveryDecisions {
		add(sqliteInsertRecoveryDecision(tx, d))
	}
	for key, turn := range st.HostTurns {
		add(sqliteWriteHostTurn(tx, st.ComputerID, nil, hnQATurn(key, turn)))
	}
	for key, session := range st.HostSessions {
		add(sqliteWriteHostSession(tx, st.ComputerID, nil, sqliteHostSessionRow{Key: key, Value: *session}))
	}
	for key, turn := range st.HostTurns {
		for id, tool := range turn.Tools {
			add(sqliteWriteHostTool(tx, st.ComputerID, nil, sqliteHostToolRow{TurnKey: key, ID: id, Value: tool}))
		}
	}
	for key, receipt := range st.HostReceipts {
		add(sqliteInsertHostReceipt(tx, st.ComputerID, st.Revision, sqliteHostReceiptRow{Key: key, Record: receipt}))
	}
	for _, pair := range erQAPairs(t, st) {
		add(sqliteInsertEventReceipt(tx, pair.Row, pair.ID))
	}
	bgQAFixtureError(t, "bootstrap meta", sqliteInsertMeta(tx, m))
	snapshot, charge := sdQAAudit(t, tx, m)
	if charge != m.LogicalBytes {
		t.Fatalf("literal stored charge=%d writer sum=%d", charge, m.LogicalBytes)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return f, m, snapshot, st
}

func sdQAUnchanged(t *testing.T, tx *sqliteio.Tx, m sqliteStoreMeta, snapshot map[string][][]string) {
	t.Helper()
	got, charge := sdQAAudit(t, tx, m)
	if !reflect.DeepEqual(got, snapshot) || charge != m.LogicalBytes {
		t.Fatal("read-only validator changed staged rows, metadata or charge")
	}
}

func sdQAReopen(t *testing.T, f interopFixture, m sqliteStoreMeta, snapshot map[string][][]string, st *state) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	sdQAUnchanged(t, tx, m, snapshot)
	bgQAFixtureError(t, "cold selected closure", sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, m.Revision, sdQASelection(st)))
	for _, a := range st.Actors {
		got, found, err := sqliteReadActorLocal(tx, m.ComputerID, a.Ref.Key)
		if err != nil || !found || !reflect.DeepEqual(got, asQAActor(a)) {
			t.Fatal("cold Actor projection", err)
		}
	}
	for id, seg := range st.Segments {
		got, found, err := sqliteReadSegmentLocal(tx, m.ComputerID, id)
		if err != nil || !found || !reflect.DeepEqual(got, asQASegment(seg)) {
			t.Fatal("cold segment projection", err)
		}
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func sdQAFirstU(t *testing.T, st *state) Uncertainty {
	t.Helper()
	for _, u := range st.Uncertainties {
		return ueQACloneU(*u)
	}
	t.Fatal("fixture has no real uncertainty")
	return Uncertainty{}
}

func sdQAProof(t *testing.T, st *state, u Uncertainty) sqliteResolveRequestRow {
	t.Helper()
	d := st.RecoveryDecisions[u.ID]
	r := st.Requests[d.RequestID]
	if r.Operation != "activity.resolve" || r.MutationResult == nil {
		t.Fatal("fixture lacks actual resolve proof")
	}
	return rpQAClone(sqliteResolveRequestRow{ID: d.RequestID, Fingerprint: r.Fingerprint, Result: *r.MutationResult})
}

func sdQAReplaceProof(t *testing.T, tx *sqliteio.Tx, r sqliteResolveRequestRow) {
	t.Helper()
	// Retained proof mutation is deliberate owned corruption, never a supported
	// writer. It preserves the other four columns and uses complete typed JSON.
	interopDone(t, tx, "UPDATE requests SET payload=? WHERE request_id=?", sqliteio.Text(rpQAPayload(t, r)), sqliteio.Text(r.ID))
}
