//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/hookstate"
)

// Minimal fresh native fixture: literal installed catalog, metadata and real
// directory binding records. No JSON authority, legacy state or importer.
type hiQAFixture struct {
	t                        *testing.T
	f                        interopFixture
	path, source, policyPath string
	cwd                      []string
	policies                 *hookstate.Service
	s                        *Service
	sample                   ClockSample
	clockErr                 error
	clockCalls               int
}

func hiQANew(t *testing.T, source string, bindings int) *hiQAFixture {
	t.Helper()
	h := &hiQAFixture{t: t, f: interopLocation(t), source: source}
	h.path = filepath.Join(h.f.directory, h.f.authority)
	h.policyPath = filepath.Join(t.TempDir(), "absent-policy", "hooks.json")
	h.policies = hookstate.New(hookstate.Options{Path: h.policyPath})
	for n := 0; n < max(bindings, 1); n++ {
		cwd, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		h.cwd = append(h.cwd, cwd)
	}
	if bindings >= 0 {
		c, tx := interopOpen(t, h.f, true, sqliteio.Write)
		if err := sqliteCreateSchema(tx); err != nil {
			t.Fatal(err)
		}
		m := interopMeta(h.f)
		for n := 0; n < bindings; n++ {
			snapshot := BindingSnapshot{ID: spQAID(int64(n + 1)), Revision: "1", Attribution: Attribution{AccountID: "1", UserID: "2", ProjectID: strconv.Itoa(n + 3), TaskID: "4", Timezone: "UTC"}}
			record := bindingRecord{Snapshot: snapshot, Kind: "directory", Locator: h.cwd[n]}
			row := sqliteBindingRow{ComputerID: interopComputer, Snapshot: snapshot, Record: &record}
			delta, err := sqliteInsertBinding(tx, row)
			if err != nil || delta != bgQABindingCharge(t, row) {
				t.Fatal("literal native binding fixture", err)
			}
			m.LogicalBytes += delta
		}
		if err := sqliteInsertMeta(tx, m); err != nil {
			t.Fatal(err)
		}
		interopCommit(t, tx)
		interopClose(t, c)
		for n := 0; n < bindings; n++ {
			p := hookstate.Context{Host: source, Scope: "project", Path: h.cwd[n], RuntimeVersion: "0.159.3", Surface: "local", Conflicts: []string{}}
			if source == "claude" {
				p.RuntimeVersion = "2.1.286"
			}
			for _, role := range []string{"runtime", "executable", "definitions"} {
				path := filepath.Join(h.cwd[n], role)
				if err := os.WriteFile(path, []byte("synthetic host policy "+role), 0600); err != nil {
					t.Fatal(err)
				}
				p.Artifacts = append(p.Artifacts, hookstate.Artifact{Role: role, Path: path})
			}
			preview, err := h.policies.Preview(context.Background(), p)
			if err != nil {
				t.Fatal(err)
			}
			confirmed, err := h.policies.Confirm(context.Background(), hookstate.ConfirmInput{Context: preview.Context, Fingerprint: preview.Fingerprint, DeclarationVersion: hookstate.DeclarationVersion, RequestID: fmt.Sprintf("81000000-0000-4000-8000-%012d", n+1), Confirmed: true})
			if err != nil || !confirmed.CaptureEligible {
				t.Fatal("actual declared fixture policy", err)
			}
		}
	}
	h.at(0)
	h.reopen()
	return h
}

func (h *hiQAFixture) at(seconds int64) {
	epoch, value := "boot-1", strconv.FormatInt(seconds*int64(time.Second), 10)
	h.sample = ClockSample{Capability: "available", WallUTC: qaEpochStart.Add(time.Duration(seconds) * time.Second), Epoch: &epoch, ElapsedNS: &value, AwakeNS: &value}
}
func (h *hiQAFixture) reopen() {
	h.s = New(Options{Path: h.path, HookPolicies: h.policies, Clock: ClockFunc(func() (ClockSample, error) {
		h.clockCalls++
		return h.sample, h.clockErr
	})})
}
func (h *hiQAFixture) event(kind, turn, agent string) HostEvent {
	return HostEvent{Source: h.source, SessionID: "native-shaped-session", TurnID: turn, AgentID: agent, Kind: kind, CWD: h.cwd[0]}
}
func (h *hiQAFixture) send(at int64, e HostEvent) HostReceipt {
	h.t.Helper()
	h.at(at)
	r, err := h.s.ingestHostSQLite(context.Background(), e)
	if err != nil {
		h.t.Fatal("host SQLite callback", e.Kind, err)
	}
	if r.Origin != "unverified" || r.Durability != "committed" {
		h.t.Fatal("fixture callback invented delivery/durability", r)
	}
	return r
}
func (h *hiQAFixture) start() HostReceipt {
	e := h.event("SessionStart", "", "")
	e.SessionSource = "startup"
	return h.send(0, e)
}
func (h *hiQAFixture) revoke() {
	h.t.Helper()
	p, err := h.policies.Eligibility(context.Background(), h.source, h.cwd[0])
	if err != nil {
		h.t.Fatal(err)
	}
	_, err = h.policies.Revoke(context.Background(), hookstate.RevokeInput{Host: h.source, Scope: "project", Path: p.Context.Path, IfRevision: p.Revision, RequestID: "82000000-0000-4000-8000-000000000001", Confirmed: true})
	if err != nil {
		h.t.Fatal(err)
	}
}

type hiQASnapshot struct {
	Meta sqliteStoreMeta
	Rows map[string][][]any
}

func (h *hiQAFixture) snapshot() hiQASnapshot {
	h.t.Helper()
	c, tx := interopOpen(h.t, h.f, false, sqliteio.Read)
	m, err := sqliteReadMeta(tx, h.f.authority, h.f.database)
	if err != nil {
		h.t.Fatal(err)
	}
	result := hiQASnapshot{Meta: m, Rows: map[string][][]any{}}
	for _, query := range []struct{ table, columns, order string }{
		{"bindings", bgQABindingColumns, "binding_id"},
		{"actor_generations", bgQAGenerationColumns, "actor_key,generation"},
		{"actors", asQAActorColumns, "actor_key"},
		{"segments", asQASegmentColumns, "segment_id"},
		{"segment_events", "segment_id,ordinal,event_reference", "segment_id,ordinal"},
		{"epochs", sqliteEpochColumns, "creation_ordinal"},
		{"host_sessions", hnQASessionColumns, "session_key"},
		{"host_turns", hnQATurnColumns, "turn_key"},
		{"host_tools", hnQAToolColumns, "turn_key,tool_id"},
		{"host_receipts", sqliteHostReceiptColumns, "receipt_key"},
		{"event_receipts", sqliteEventReceiptColumns, "event_key"},
		{"uncertainties", asQAUncertaintyColumns, "uncertainty_id"},
		{"uncertainty_evidence", asQAEvidenceColumns, "uncertainty_id"},
		{"union_frontier", sqliteFrontierLocalColumns, "component_id"},
		{"intervals", sqliteIntervalLocalColumns, "creation_ordinal"},
		{"outbox", sqliteOutboxLocalColumns, "interval_id"},
		{"actor_uncertainties", "actor_key,ordinal,uncertainty_id", "actor_key,ordinal"},
		{"event_receipt_uncertainties", "event_key,ordinal,uncertainty_id", "event_key,ordinal"},
		{"event_ids", "event_id,event_key", "event_id"},
		{"component_segments", "component_id,segment_id", "component_id,segment_id"},
		{"pending_finalization", "component_id,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json", "component_id"},
		{"interval_segments", "interval_id,ordinal,segment_id", "interval_id,ordinal"},
		{"interval_components", "interval_id,component_id", "interval_id,component_id"},
	} {
		rows, _ := mqQAScan(h.t, tx, "SELECT "+query.columns+" FROM "+query.table+" ORDER BY "+query.order)
		result.Rows[query.table] = rows
	}
	interopRollback(h.t, tx)
	interopClose(h.t, c)
	return result
}
func hiQANonceOnly(t *testing.T, before, after hiQASnapshot) {
	t.Helper()
	if before.Meta.DurabilityNonce == after.Meta.DurabilityNonce {
		t.Fatal("replay did not fence durability nonce")
	}
	before.Meta.DurabilityNonce, after.Meta.DurabilityNonce = [16]byte{}, [16]byte{}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("exact replay changed retained domain values/revision")
	}
}
func (h *hiQAFixture) actor(ref *ActorRef) sqliteActorLocalRow {
	h.t.Helper()
	if ref == nil {
		h.t.Fatal("missing admitted host actor")
	}
	c, tx := interopOpen(h.t, h.f, false, sqliteio.Read)
	a, found, err := sqliteReadActorLocal(tx, interopComputer, ref.Key)
	if err != nil || !found || a.Ref != *ref {
		h.t.Fatal("exact host actor unavailable", err)
	}
	interopRollback(h.t, tx)
	interopClose(h.t, c)
	return a
}
func (h *hiQAFixture) intervals(project string, bounds ...[2]int64) {
	h.t.Helper()
	c, tx := interopOpen(h.t, h.f, false, sqliteio.Read)
	rows, _ := mqQAScan(h.t, tx, "SELECT interval_id FROM intervals WHERE project_id=? ORDER BY start_sec,start_nsec", sqliteio.Text(project))
	if len(rows) != len(bounds) {
		h.t.Fatal("project union count", project, len(rows), len(bounds))
	}
	for n, raw := range rows {
		id := raw[0].(string)
		r, found, err := sqliteReadIntervalLocal(tx, interopComputer, id)
		if err != nil || !found || r.Attribution.ProjectID != project || !r.Start.Equal(qaEpochStart.Add(time.Duration(bounds[n][0])*time.Second)) || !r.End.Equal(qaEpochStart.Add(time.Duration(bounds[n][1])*time.Second)) || r.DurationNS != strconv.FormatInt((bounds[n][1]-bounds[n][0])*int64(time.Second), 10) {
			h.t.Fatal("exact immutable union interval", err)
		}
		o, found, err := sqliteReadOutboxLocal(tx, interopComputer, id)
		if err != nil || !found || o.State != "queued" || o.PlanPresent || o.Correlation != "tempo:"+id {
			h.t.Fatal("one queued root per sealed union", err)
		}
	}
	interopRollback(h.t, tx)
	interopClose(h.t, c)
}
func (h *hiQAFixture) calibrateHook() {
	h.t.Helper()
	c, tx := interopOpen(h.t, h.f, false, sqliteio.Read)
	spQAHookPositive(h.t, tx) // Actual ROW100, DONE101, checked finalize0 ABI.
	interopRollback(h.t, tx)
	interopClose(h.t, c)
}
