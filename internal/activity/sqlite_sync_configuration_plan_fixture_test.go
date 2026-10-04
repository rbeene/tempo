//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent literal fixtures for seven fresh local configuration/plan APIs.
// Complete legacy domain values are oracles; this SQL subset is not a sync graph.
import (
	"context"
	"encoding/binary"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const scpQAConfigColumns = "config_key,account_id,user_id,revision,mode,duration_policy,policy_version,clock,declared,declared_at_sec,declared_at_nsec,declared_at_json,source"
const scpQAPlanColumns = "interval_id,company_source,config_account_id,config_user_id,config_revision,config_mode,config_duration_policy,config_policy_version,config_clock,config_declared,config_declared_at_sec,config_declared_at_nsec,config_declared_at_json,config_source"
const scpQAOutboxColumns = "interval_id,id,revision,state,correlation,entry_id,failure_category,retry_request_id,run_request_id,plan_present"

func scpQAOracle(t *testing.T) (SyncConfiguration, sqliteSyncPlanLocalRow, sqliteIntervalLocalRow, string) {
	t.Helper()
	s, _, in := qaSyncFixture(t, 36*time.Second)
	config := qaSyncConfigure(t, s, qaNewSyncProvider(t)).Configuration
	if !validSyncConfiguration(config) {
		t.Fatal("actual saved configuration oracle invalid")
	}
	item := qaSyncOnlyItem(t, s)
	plan, err := syncBuildPlan(item, config, "company_verified")
	if err != nil || plan == nil || len(plan.Parts) != 1 {
		t.Fatal("actual pure SyncPlan oracle", err)
	}
	st := bgQAReadLegacy(t, s)
	ir := sqliteIntervalLocalRow{ID: in.ID, ComputerID: st.ComputerID, Attribution: in.Attribution, Start: in.Start, End: in.End, DurationNS: in.DurationNS, Ordinal: 0}
	return plan.Configuration, sqliteSyncPlanLocalRow{IntervalID: in.ID, CompanySource: plan.CompanySource, Configuration: plan.Configuration}, ir, item.ID
}
func scpQACounter(t *testing.T, s string) sqliteio.Value {
	t.Helper()
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != s {
		t.Fatal("independent counter fixture", err)
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return sqliteio.Blob(b[:])
}
func scpQAConfigValues(t *testing.T, c SyncConfiguration) []sqliteio.Value {
	t.Helper()
	clock := sqliteio.Null()
	if c.Clock != nil {
		clock = sqliteio.Text(*c.Clock)
	}
	declared := int64(0)
	if c.Declared {
		declared = 1
	}
	v := []sqliteio.Value{sqliteio.Text(c.AccountID + "/" + c.UserID), sqliteio.Text(c.AccountID), sqliteio.Text(c.UserID), scpQACounter(t, c.Revision), sqliteio.Text(c.Mode), sqliteio.Text(c.DurationPolicy), sqliteio.Text(c.PolicyVersion), clock, sqliteio.Integer(declared)}
	v = append(v, asQATime(t, c.DeclaredAt)...)
	return append(v, sqliteio.Text(c.Source))
}
func scpQAPlanValues(t *testing.T, p sqliteSyncPlanLocalRow) []sqliteio.Value {
	c := scpQAConfigValues(t, p.Configuration)
	v := []sqliteio.Value{sqliteio.Text(p.IntervalID), sqliteio.Text(p.CompanySource)}
	return append(v, c[1:]...)
}
func scpQAConfigCharge(t *testing.T, c SyncConfiguration) int64 {
	t.Helper()
	b, err := c.DeclaredAt.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	n := int64(149)
	for _, s := range []string{c.AccountID + "/" + c.UserID, c.AccountID, c.UserID, c.Mode, c.DurationPolicy, c.PolicyVersion, string(b), c.Source} {
		n += int64(len(s))
	}
	if c.Clock != nil {
		n += 8 + int64(len(*c.Clock))
	}
	return n
}
func scpQAPlanCharge(t *testing.T, p sqliteSyncPlanLocalRow) int64 {
	c := p.Configuration
	return scpQAConfigCharge(t, c) - 149 - int64(len(c.AccountID+"/"+c.UserID)) + 158 + int64(len(p.IntervalID)+len(p.CompanySource))
}
func scpQASeed(t *testing.T, computer ...string) (interopFixture, sqliteStoreMeta) {
	t.Helper()
	f := interopLocation(t)
	m := interopMeta(f)
	if len(computer) > 0 {
		m.ComputerID = computer[0]
	}
	interopInitialize(t, f, m)
	return f, m
}
func scpQAParents(t *testing.T, tx *sqliteio.Tx, ir sqliteIntervalLocalRow, root string) int64 {
	t.Helper()
	d, err := sqliteInsertIntervalLocal(tx, ir.ComputerID, ir)
	if err != nil || d != fpiQAIGCharge(t, ir) {
		t.Fatal("real interval parent", d, err)
	}
	o, err := sqliteInsertQueuedOutbox(tx, ir.ComputerID, ir.ID, root)
	if err != nil || o != 98+int64(len(ir.ID)+len(root)+len("queued")+len("tempo:"+ir.ID)) {
		t.Fatal("real initial-only outbox parent", o, err)
	}
	return d + o
}
func scpQAAudit(t *testing.T, tx *sqliteio.Tx) (map[string][][]string, int64) {
	t.Helper()
	snap := map[string][][]string{}
	var n int64
	for _, r := range []struct{ table, cols, order string }{
		{"intervals", fpiQAIntervalCols, "interval_id"}, {"outbox", scpQAOutboxColumns, "interval_id"},
		{"sync_configurations", scpQAConfigColumns, "config_key"}, {"sync_plans", scpQAPlanColumns, "interval_id"},
	} {
		rows, charge := asQALiteralAudit(t, tx, r.table, r.cols, r.order)
		snap[r.table] = rows
		n += charge
	}
	return snap, n
}
func scpQACommit(t *testing.T, tx *sqliteio.Tx, old sqliteStoreMeta, delta int64) (sqliteStoreMeta, map[string][][]string) {
	t.Helper()
	next := metaQANext(t, old)
	next.Revision = bump(old.Revision)
	next.LogicalBytes += delta
	if err := sqliteUpdateMeta(tx, old, next); err != nil {
		t.Fatal(err)
	}
	snap, n := scpQAAudit(t, tx)
	if n+interopMeta(interopFixture{authority: old.StateBasename, database: old.DatabaseBasename}).LogicalBytes != next.LogicalBytes {
		t.Fatal("literal stored charge disagrees with meta", n, next.LogicalBytes)
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	return next, snap
}
func scpQAReopen(t *testing.T, f interopFixture, m sqliteStoreMeta, want map[string][][]string) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	got, _ := scpQAAudit(t, tx)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("rollback/reopen retained literal rows differ")
	}
	meta, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(meta, m) {
		t.Fatal("rollback/reopen retained metadata differs", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}
func scpQAEmptySnapshot(t *testing.T, f interopFixture) map[string][][]string {
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	snap, _ := scpQAAudit(t, tx)
	interopRollback(t, tx)
	interopClose(t, c)
	return snap
}
func scpQAZeroConfig(t *testing.T, got SyncConfiguration, found bool) {
	t.Helper()
	if found || !reflect.DeepEqual(got, SyncConfiguration{}) {
		t.Fatal("error/absence leaked configuration")
	}
}
func scpQAZeroPlan(t *testing.T, got sqliteSyncPlanLocalRow, found bool) {
	t.Helper()
	if found || !reflect.DeepEqual(got, sqliteSyncPlanLocalRow{}) {
		t.Fatal("error/absence leaked plan")
	}
}
func scpQASameConfig(t *testing.T, got, want SyncConfiguration) {
	t.Helper()
	if !reflect.DeepEqual(got, asQAJSON(t, want)) {
		t.Fatal("owned configuration differs", got, want)
	}
}
func scpQAReadPlan(t *testing.T, tx *sqliteio.Tx, computer string, want sqliteSyncPlanLocalRow) {
	t.Helper()
	got, found, err := sqliteReadSyncPlanLocal(tx, computer, want.IntervalID)
	if err != nil || !found || !reflect.DeepEqual(got, asQAJSON(t, want)) {
		t.Fatal("owned scoped plan differs", err)
	}
}
func scpQAShadowConfig(t *testing.T, tx *sqliteio.Tx) {
	asQAShadow(t, tx, "sync_configurations", scpQAConfigColumns)
}
func scpQAShadowPlan(t *testing.T, tx *sqliteio.Tx) {
	asQAShadow(t, tx, "sync_plans", scpQAPlanColumns)
}
func scpQASet(t *testing.T, tx *sqliteio.Tx, table, column, key string, v sqliteio.Value) {
	t.Helper()
	pk := "config_key"
	if table == "sync_plans" {
		pk = "interval_id"
	}
	interopDone(t, tx, "UPDATE "+table+" SET "+column+"=? WHERE "+pk+"=?", v, sqliteio.Text(key))
}
func scpQAClone(c SyncConfiguration) SyncConfiguration {
	if c.Clock != nil {
		v := *c.Clock
		c.Clock = &v
	}
	return c
}
func scpQAColumnNames(plan bool) []string {
	if plan {
		return strings.Split(scpQAPlanColumns, ",")
	}
	return strings.Split(scpQAConfigColumns, ",")
}
func scpQACanceledCleanup(t *testing.T, tx *sqliteio.Tx, conn *sqliteio.Conn) {
	t.Helper()
	if err := tx.Rollback(); err != nil {
		interopSafeError(t, err)
	}
	if err := conn.Close(context.Background()); err != nil {
		interopSafeError(t, err)
	}
}

// This single literal child is an actual syncBuildPlan output. It is used only
// to exercise the plan header's replacement absence guard; no part API is stubbed.
func scpQAPartFixture(t *testing.T, tx *sqliteio.Tx, ir sqliteIntervalLocalRow, root string, config SyncConfiguration) {
	t.Helper()
	o := OutboxItem{ID: root, Revision: "1", State: "queued", Correlation: "tempo:" + ir.ID, Interval: Interval{ID: ir.ID, ComputerID: ir.ComputerID, Attribution: ir.Attribution, Start: ir.Start, End: ir.End, DurationNS: ir.DurationNS, SegmentIDs: []string{}}}
	plan, err := syncBuildPlan(o, config, "company_verified")
	if err != nil || len(plan.Parts) != 1 {
		t.Fatal("actual child oracle", err)
	}
	p := plan.Parts[0]
	duration, err := strconv.ParseInt(p.DurationNS, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	planned, err := strconv.ParseInt(p.PlannedDurationNS, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	residual, err := strconv.ParseInt(p.PlannedResidualNS, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	values := []sqliteio.Value{sqliteio.Text(ir.ID), sqliteio.Integer(0), sqliteio.Text(p.ID), sqliteio.Text(p.SpentDate), sqliteio.Integer(duration)}
	values = append(values, asQATime(t, p.Start)...)
	values = append(values, asQATime(t, p.End)...)
	values = append(values, sqliteio.Text(p.PlannedHours), sqliteio.Integer(planned), sqliteio.Integer(residual), sqliteio.Null(), sqliteio.Null(), sqliteio.Text(p.Correlation), sqliteio.Text(p.Notes), sqliteio.Text(p.State))
	for i := 0; i < 9; i++ {
		values = append(values, sqliteio.Null())
	}
	asQABindFixture(t, tx, "sync_parts", "interval_id,ordinal,id,spent_date,duration_ns,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,planned_hours,planned_duration_ns,planned_residual_ns,started_time,ended_time,correlation,notes,state,entry_id,failure_category,returned_hours,rounded_hours,confirmed_duration_ns,provider_delta_ns,total_residual_ns,attachment_request_id,attachment_entry_id", values)
}
