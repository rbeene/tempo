//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Source-first independent QA for corrected d84c275b/fca4aca6. Root executes real producers.
// Selected-item graph, parts, attempts, provider claims and Service routing are separate.
import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func TestSQLiteSyncConfigurationPlanFreshCommitReopenLiteralChargesOwnership(t *testing.T) {
	config, plan, ir, root := scpQAOracle(t)
	config.Revision = asQAMax
	config.Clock = asQAPointer("")
	config.DeclaredAt = time.Date(9999, 12, 31, 23, 59, 58, 987654321, time.UTC)
	plan.Configuration = scpQAClone(config)
	f, m := scpQASeed(t, ir.ComputerID)
	empty := scpQAEmptySnapshot(t, f)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	if rows, err := sqliteSyncConfigurations(tx); err != nil || rows == nil || len(rows) != 0 {
		t.Fatal("allocated empty configurations", err)
	}
	got, found, err := sqliteReadSyncConfiguration(tx, config.AccountID, config.UserID)
	if err != nil {
		t.Fatal(err)
	}
	scpQAZeroConfig(t, got, found)
	gp, found, err := sqliteReadSyncPlanLocal(tx, ir.ComputerID, plan.IntervalID)
	if err != nil {
		t.Fatal(err)
	}
	scpQAZeroPlan(t, gp, found)
	delta := scpQAParents(t, tx, ir, root)
	for _, row := range []struct {
		name string
		run  func() (int64, error)
		want int64
	}{
		{"config charge149", func() (int64, error) { return sqliteSyncConfigurationCharge(config) }, scpQAConfigCharge(t, config)},
		{"plan charge158", func() (int64, error) { return sqliteSyncPlanLocalCharge(plan) }, scpQAPlanCharge(t, plan)},
	} {
		n, e := row.run()
		if e != nil || n != row.want {
			t.Fatal(row.name, n, row.want, e)
		}
	}
	before := scpQAClone(config)
	beforePlan := asQAJSON(t, plan)
	n, err := sqliteWriteSyncConfiguration(tx, nil, config)
	if err != nil || n != scpQAConfigCharge(t, config) {
		t.Fatal("insert13", n, err)
	}
	delta += n
	n, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, nil, plan)
	if err != nil || n != scpQAPlanCharge(t, plan) {
		t.Fatal("insert14", n, err)
	}
	delta += n
	got, found, err = sqliteReadSyncConfiguration(tx, config.AccountID, config.UserID)
	if err != nil || !found {
		t.Fatal(err)
	}
	scpQASameConfig(t, got, config)
	if got.Clock == config.Clock {
		t.Fatal("read aliases caller Clock")
	}
	*got.Clock = "caller changes returned pointer"
	gp, found, err = sqliteReadSyncPlanLocal(tx, ir.ComputerID, plan.IntervalID)
	if err != nil || !found {
		t.Fatal(err)
	}
	if gp.Configuration.Clock == plan.Configuration.Clock {
		t.Fatal("plan aliases caller configuration")
	}
	*gp.Configuration.Clock = "caller change"
	scpQAReadPlan(t, tx, ir.ComputerID, plan)
	rows, err := sqliteSyncConfigurations(tx)
	if err != nil || rows == nil || len(rows) != 1 {
		t.Fatal(err)
	}
	*rows[0].Clock = "list caller change"
	rows[0].AccountID = "900"
	got, found, err = sqliteReadSyncConfiguration(tx, config.AccountID, config.UserID)
	if err != nil || !found {
		t.Fatal(err)
	}
	scpQASameConfig(t, got, config)
	if !reflect.DeepEqual(config, before) || !reflect.DeepEqual(plan, beforePlan) {
		t.Fatal("caller inputs mutated")
	}
	// Local staging permits false PlanPresent and does not require a current
	// configuration row to equal frozen consent. Final graph checking is separate.
	if interopCount(t, tx, "SELECT plan_present FROM outbox") != 0 {
		t.Fatal("primitive altered root flag")
	}
	stmt := interopPrepare(t, tx, "SELECT "+scpQAConfigColumns+" FROM sync_configurations")
	if row, e := stmt.Step(); e != nil || !row || stmt.ColumnCount() != 13 {
		t.Fatal("literal13", e)
	}
	for i, want := range []sqliteio.Kind{sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.BlobKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.IntegerKind, sqliteio.IntegerKind, sqliteio.IntegerKind, sqliteio.TextKind, sqliteio.TextKind} {
		kind, e := stmt.Kind(i)
		if e != nil || kind != want {
			t.Fatal("literal configuration native kind", i, e)
		}
	}
	blob, e := stmt.Blob(3)
	if e != nil || !reflect.DeepEqual(blob, []byte{255, 255, 255, 255, 255, 255, 255, 255}) {
		t.Fatal("full uint64 BLOB8", e)
	}
	if row, e := stmt.Step(); e != nil || row {
		t.Fatal("singleton DONE", e)
	}
	if err = stmt.Close(); err != nil {
		t.Fatal(err)
	}
	stmt = interopPrepare(t, tx, "SELECT "+scpQAPlanColumns+" FROM sync_plans")
	if row, e := stmt.Step(); e != nil || !row || stmt.ColumnCount() != 14 {
		t.Fatal("literal14", e)
	}
	for i, want := range []sqliteio.Kind{sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.BlobKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.TextKind, sqliteio.IntegerKind, sqliteio.IntegerKind, sqliteio.IntegerKind, sqliteio.TextKind, sqliteio.TextKind} {
		kind, e := stmt.Kind(i)
		if e != nil || kind != want {
			t.Fatal("literal plan native kind", i, e)
		}
	}
	if row, e := stmt.Step(); e != nil || row {
		t.Fatal("plan singleton DONE", e)
	}
	if err = stmt.Close(); err != nil {
		t.Fatal(err)
	}
	next, snap := scpQACommit(t, tx, m, delta)
	interopClose(t, c)
	scpQAReopen(t, f, next, snap)
	if reflect.DeepEqual(empty, snap) {
		t.Fatal("vacuous commit")
	}
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	got, found, err = sqliteReadSyncConfiguration(tx, config.AccountID, config.UserID)
	if err != nil || !found {
		t.Fatal(err)
	}
	scpQASameConfig(t, got, config)
	scpQAReadPlan(t, tx, ir.ComputerID, plan)
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteSyncConfigurationPlanActualPolicyVariantsAndNullableCAS(t *testing.T) {
	base, original, ir, root := scpQAOracle(t)
	for _, kind := range []string{"duration-null", "duration-empty", "duration-exact", "timestamp12", "timestamp24"} {
		t.Run(kind, func(t *testing.T) {
			config := scpQAClone(base)
			switch kind {
			case "duration-null":
				config.Clock = nil
			case "duration-empty":
				config.Clock = asQAPointer("")
			case "duration-exact":
				config.DurationPolicy = "exact"
				config.PolicyVersion = "exact-v1"
				config.Clock = nil
			case "timestamp12", "timestamp24":
				config.Mode = "timestamp"
				config.DurationPolicy = "exact"
				config.PolicyVersion = "exact-v1"
				clock := "12h"
				if kind == "timestamp24" {
					clock = "24h"
				}
				config.Clock = &clock
			}
			if !validSyncConfiguration(config) {
				t.Fatal("actual scalar validity disagreement")
			}
			f, m := scpQASeed(t, ir.ComputerID)
			snap := scpQAEmptySnapshot(t, f)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			scpQAParents(t, tx, ir, root)
			plan := original
			plan.Configuration = config
			plan.CompanySource = "user_declared_fallback"
			n, err := sqliteWriteSyncConfiguration(tx, nil, config)
			if err != nil || n != scpQAConfigCharge(t, config) {
				t.Fatal("actual policy insert", err)
			}
			n, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, nil, plan)
			if err != nil || n != scpQAPlanCharge(t, plan) {
				t.Fatal(err)
			}
			got, found, err := sqliteReadSyncConfiguration(tx, config.AccountID, config.UserID)
			if err != nil || !found {
				t.Fatal(err)
			}
			scpQASameConfig(t, got, config)
			scpQAReadPlan(t, tx, ir.ComputerID, plan)
			after := scpQAClone(config)
			after.Revision = "18446744073709551614"
			after.DeclaredAt = after.DeclaredAt.Add(time.Nanosecond)
			n, err = sqliteWriteSyncConfiguration(tx, &config, after)
			if err != nil || n != scpQAConfigCharge(t, after)-scpQAConfigCharge(t, config) {
				t.Fatal("positive25-bind CAS", err)
			}
			newPlan := plan
			newPlan.Configuration = scpQAClone(after)
			newPlan.CompanySource = "company_verified"
			n, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &plan, newPlan)
			if err != nil || n != scpQAPlanCharge(t, newPlan)-scpQAPlanCharge(t, plan) {
				t.Fatal("positive27-bind CAS", err)
			}
			scpQAReadPlan(t, tx, ir.ComputerID, newPlan)
			if config.Mode == "duration" {
				// Two valid duration forms compare by nullable IS, not empty coercion.
				changed := scpQAClone(after)
				if after.Clock == nil {
					changed.Clock = asQAPointer("")
				} else {
					changed.Clock = nil
				}
				n, err = sqliteWriteSyncConfiguration(tx, &after, changed)
				if err != nil || n != scpQAConfigCharge(t, changed)-scpQAConfigCharge(t, after) {
					t.Fatal("NULL/present-empty delta", n, err)
				}
				newer := newPlan
				newer.Configuration = scpQAClone(changed)
				n, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &newPlan, newer)
				if err != nil || n != scpQAPlanCharge(t, newer)-scpQAPlanCharge(t, newPlan) {
					t.Fatal("plan NULL IS update", n, err)
				}
				if n, err = sqliteWriteSyncConfiguration(tx, &after, changed); n != 0 {
					t.Fatal("stale NULL delta")
				} else {
					bgQACorrupt(t, err)
				}
				if n, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &newPlan, newer); n != 0 {
					t.Fatal("stale NULL plan delta")
				} else {
					bgQACorrupt(t, err)
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			scpQAReopen(t, f, m, snap)
		})
	}
}

func TestSQLiteSyncConfigurationsCompleteSortedAllocatedListAndLateFailure(t *testing.T) {
	base, _, _, _ := scpQAOracle(t)
	f, m := scpQASeed(t)
	snap := scpQAEmptySnapshot(t, f)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	configs := []SyncConfiguration{scpQAClone(base), scpQAClone(base), scpQAClone(base)}
	configs[0].AccountID = "20"
	configs[1].AccountID = "10"
	configs[2].AccountID = "3"
	configs[0].Clock = asQAPointer("")
	configs[1].Clock = nil
	configs[2].Clock = asQAPointer("")
	for _, config := range configs {
		if _, err := sqliteWriteSyncConfiguration(tx, nil, config); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := sqliteSyncConfigurations(tx)
	if err != nil || rows == nil || len(rows) != 3 {
		t.Fatal("complete list truncated", err)
	}
	for i, want := range []SyncConfiguration{configs[1], configs[0], configs[2]} {
		scpQASameConfig(t, rows[i], want)
	}
	if rows[1].Clock == rows[2].Clock {
		t.Fatal("different rows share owned Clock")
	}
	*rows[1].Clock = "returned mutation"
	rows[0] = SyncConfiguration{}
	rows, err = sqliteSyncConfigurations(tx)
	if err != nil || len(rows) != 3 {
		t.Fatal(err)
	}
	scpQASameConfig(t, rows[1], configs[0])
	scpQAShadowConfig(t, tx)
	// The first two are valid. A corrupted final selected row must discard them.
	scpQASet(t, tx, "sync_configurations", "source", "3/"+base.UserID, sqliteio.Text("malformed selected private profile"))
	rows, err = sqliteSyncConfigurations(tx)
	bgQACorrupt(t, err)
	if rows != nil {
		t.Fatal("late list failure returned partial allocated values")
	}
	interopSafeError(t, err, "malformed selected private profile")
	interopRollback(t, tx)
	interopClose(t, c)
	scpQAReopen(t, f, m, snap)
}

func TestSQLiteSyncConfigurationPlanEveryStoredBeforeColumnCASAndLateRollback(t *testing.T) {
	config, plan, ir, root := scpQAOracle(t)
	f, m := scpQASeed(t, ir.ComputerID)
	snap := scpQAEmptySnapshot(t, f)
	for _, family := range []string{"config", "plan"} {
		isPlan := family == "plan"
		for index, column := range scpQAColumnNames(isPlan) {
			t.Run(family+"/"+column, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				scpQAParents(t, tx, ir, root)
				if _, err := sqliteWriteSyncConfiguration(tx, nil, config); err != nil {
					t.Fatal(err)
				}
				if _, err := sqliteWriteSyncPlanLocal(tx, ir.ComputerID, nil, plan); err != nil {
					t.Fatal(err)
				}
				after := scpQAClone(config)
				after.Revision = "2"
				after.DeclaredAt = after.DeclaredAt.Add(time.Second)
				newPlan := plan
				newPlan.Configuration = scpQAClone(after)
				newPlan.CompanySource = "user_declared_fallback"
				// First certify the actual complete-before update with an unchanged
				// exact literal old row, then restore it before varying a sole column.
				var delta int64
				var err error
				if isPlan {
					delta, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &plan, newPlan)
					if err != nil || delta != scpQAPlanCharge(t, newPlan)-scpQAPlanCharge(t, plan) {
						t.Fatal("positive27", err)
					}
					scpQAReadPlan(t, tx, ir.ComputerID, newPlan)
					_, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &newPlan, plan)
				} else {
					delta, err = sqliteWriteSyncConfiguration(tx, &config, after)
					if err != nil || delta != scpQAConfigCharge(t, after)-scpQAConfigCharge(t, config) {
						t.Fatal("positive25", err)
					}
					_, err = sqliteWriteSyncConfiguration(tx, &after, config)
				}
				if err != nil {
					t.Fatal("exact restoration", err)
				}
				// Staging a sibling and its real metadata proves zero error delta is
				// not an implicit transaction rollback by the row unit.
				sibling := scpQAClone(config)
				sibling.AccountID = "88"
				sibling.UserID = "99"
				if _, err = sqliteWriteSyncConfiguration(tx, nil, sibling); err != nil {
					t.Fatal("sibling stage", err)
				}
				next := metaQANext(t, m)
				next.Revision = "2"
				next.LogicalBytes += scpQAConfigCharge(t, sibling)
				if err = sqliteUpdateMeta(tx, m, next); err != nil {
					t.Fatal("metadata stage", err)
				}
				table, key := "sync_configurations", config.AccountID+"/"+config.UserID
				values := scpQAConfigValues(t, config)
				if isPlan {
					table, key = "sync_plans", plan.IntervalID
					values = scpQAPlanValues(t, plan)
					scpQAShadowPlan(t, tx)
				} else {
					scpQAShadowConfig(t, tx)
				}
				changed := sqliteio.Text("stale single-column witness")
				// Match native classes for numeric/BLOB axes; Clock uses the two
				// semantically valid but representationally different duration forms.
				if column == "revision" || column == "config_revision" {
					changed = scpQACounter(t, "3")
				}
				if strings.HasSuffix(column, "_sec") {
					changed = sqliteio.Integer(config.DeclaredAt.Unix() + 1)
				}
				if strings.HasSuffix(column, "_nsec") {
					changed = sqliteio.Integer(int64((config.DeclaredAt.Nanosecond() + 1) % 1000000000))
				}
				if column == "declared" || column == "config_declared" {
					changed = sqliteio.Integer(0)
				}
				if column == "clock" || column == "config_clock" {
					if config.Clock == nil {
						changed = sqliteio.Text("")
					} else {
						changed = sqliteio.Null()
					}
				}
				if column == "declared_at_json" || column == "config_declared_at_json" {
					b, e := config.DeclaredAt.MarshalJSON()
					if e != nil {
						t.Fatal(e)
					}
					changed = sqliteio.Text(strings.Replace(string(b), "Z\"", "+00:00\"", 1))
				}
				if reflect.DeepEqual(values[index], changed) {
					t.Fatal("vacuous independent old column")
				}
				scpQASet(t, tx, table, column, key, changed)
				if isPlan {
					delta, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &plan, newPlan)
				} else {
					delta, err = sqliteWriteSyncConfiguration(tx, &config, after)
				}
				bgQACorrupt(t, err)
				if delta != 0 {
					t.Fatal("full-before mismatch returned delta")
				}
				got, found, e := sqliteReadSyncConfiguration(tx, sibling.AccountID, sibling.UserID)
				if e != nil || !found {
					t.Fatal("row unit incorrectly rolled back caller work", e)
				}
				scpQASameConfig(t, got, sibling)
				interopRollback(t, tx)
				interopClose(t, c)
				scpQAReopen(t, f, m, snap)
			})
		}
	}
}

func TestSQLiteSyncConfigurationPlanStrictEveryNativeKindAndTextUTF8(t *testing.T) {
	config, plan, ir, root := scpQAOracle(t)
	config.Clock = asQAPointer("")
	plan.Configuration = scpQAClone(config)
	f, m := scpQASeed(t, ir.ComputerID)
	snap := scpQAEmptySnapshot(t, f)
	for _, family := range []string{"config", "plan"} {
		isPlan := family == "plan"
		columns := scpQAColumnNames(isPlan)
		for _, column := range columns {
			if column == "config_key" || column == "interval_id" {
				continue
			}
			// Every field is selected with its native class; TEXT must never
			// coerce BLOB bytes and revision must never coerce text/INTEGER.
			for _, axis := range []string{"wrong-kind", "null", "bad-utf8"} {
				if axis == "null" && (column == "clock" || column == "config_clock") {
					continue
				}
				isNumeric := column == "revision" || column == "config_revision" || column == "declared" || column == "config_declared" || strings.HasSuffix(column, "_sec") || strings.HasSuffix(column, "_nsec")
				if axis == "bad-utf8" && isNumeric {
					continue
				}
				t.Run(family+"/"+column+"/"+axis, func(t *testing.T) {
					c, tx := interopOpen(t, f, false, sqliteio.Write)
					scpQAParents(t, tx, ir, root)
					if isPlan {
						scpQAShadowPlan(t, tx)
						asQABindFixture(t, tx, "sync_plans", scpQAPlanColumns, scpQAPlanValues(t, plan))
						scpQAReadPlan(t, tx, ir.ComputerID, plan)
					} else {
						scpQAShadowConfig(t, tx)
						asQABindFixture(t, tx, "sync_configurations", scpQAConfigColumns, scpQAConfigValues(t, config))
						got, found, e := sqliteReadSyncConfiguration(tx, config.AccountID, config.UserID)
						if e != nil || !found {
							t.Fatal("positive decoder", e)
						}
						scpQASameConfig(t, got, config)
					}
					bad := sqliteio.Blob([]byte("uncoerced native class"))
					if isNumeric {
						bad = sqliteio.Text("1")
					}
					if axis == "null" {
						bad = sqliteio.Null()
					}
					if axis == "bad-utf8" {
						bad = sqliteio.Text(string([]byte{'x', 0xff, 'y'}))
					}
					key, table := config.AccountID+"/"+config.UserID, "sync_configurations"
					if isPlan {
						key, table = plan.IntervalID, "sync_plans"
					}
					scpQASet(t, tx, table, column, key, bad)
					if isPlan {
						got, found, e := sqliteReadSyncPlanLocal(tx, ir.ComputerID, plan.IntervalID)
						bgQACorrupt(t, e)
						scpQAZeroPlan(t, got, found)
					} else {
						got, found, e := sqliteReadSyncConfiguration(tx, config.AccountID, config.UserID)
						bgQACorrupt(t, e)
						scpQAZeroConfig(t, got, found)
						rows, e := sqliteSyncConfigurations(tx)
						bgQACorrupt(t, e)
						if rows != nil {
							t.Fatal("kind/UTF8 malformed list partial output")
						}
					}
					interopRollback(t, tx)
					interopClose(t, c)
					scpQAReopen(t, f, m, snap)
				})
			}
		}
	}
}

func TestSQLiteSyncConfigurationPlanMalformedScalarsScopeAndTime(t *testing.T) {
	config, plan, ir, root := scpQAOracle(t)
	f, m := scpQASeed(t, ir.ComputerID)
	snap := scpQAEmptySnapshot(t, f)
	cases := []struct {
		name, column string
		value        sqliteio.Value
	}{
		{"account-scope", "account_id", sqliteio.Text("111")}, {"user-scope", "user_id", sqliteio.Text("222")},
		{"invalid-account", "account_id", sqliteio.Text("01")}, {"invalid-user", "user_id", sqliteio.Text("0")},
		{"zero-revision", "revision", sqliteio.Blob(make([]byte, 8))}, {"short-revision", "revision", sqliteio.Blob([]byte{1})},
		{"bad-mode", "mode", sqliteio.Text("durations")}, {"bad-policy", "duration_policy", sqliteio.Text("rounded")},
		{"bad-version", "policy_version", sqliteio.Text("exact-v2")}, {"bad-duration-clock", "clock", sqliteio.Text("24h")},
		{"undeclared", "declared", sqliteio.Integer(0)}, {"nonboolean", "declared", sqliteio.Integer(2)},
		{"bad-source", "source", sqliteio.Text("company_verified")},
		{"time-second-mismatch", "declared_at_sec", sqliteio.Integer(config.DeclaredAt.Unix() + 1)},
		{"negative-nanosecond", "declared_at_nsec", sqliteio.Integer(-1)}, {"large-nanosecond", "declared_at_nsec", sqliteio.Integer(1000000000)},
		{"time-json-invalid", "declared_at_json", sqliteio.Text("private profile bad JSON")},
		{"time-json-noncanonical", "declared_at_json", sqliteio.Text("\"2025-01-01T00:00:00.000Z\"")},
	}
	for _, family := range []string{"config", "plan"} {
		for _, tc := range cases {
			// Frozen plan account/user need not equal interval attribution.
			if family == "plan" && (tc.name == "account-scope" || tc.name == "user-scope") {
				continue
			}
			t.Run(family+"/"+tc.name, func(t *testing.T) {
				c, tx := interopOpen(t, f, false, sqliteio.Write)
				scpQAParents(t, tx, ir, root)
				if family == "config" {
					scpQAShadowConfig(t, tx)
					asQABindFixture(t, tx, "sync_configurations", scpQAConfigColumns, scpQAConfigValues(t, config))
					scpQASet(t, tx, "sync_configurations", tc.column, config.AccountID+"/"+config.UserID, tc.value)
					got, found, e := sqliteReadSyncConfiguration(tx, config.AccountID, config.UserID)
					bgQACorrupt(t, e)
					scpQAZeroConfig(t, got, found)
					rows, e := sqliteSyncConfigurations(tx)
					bgQACorrupt(t, e)
					if rows != nil {
						t.Fatal("malformed list partial")
					}
				} else {
					scpQAShadowPlan(t, tx)
					asQABindFixture(t, tx, "sync_plans", scpQAPlanColumns, scpQAPlanValues(t, plan))
					scpQASet(t, tx, "sync_plans", "config_"+tc.column, plan.IntervalID, tc.value)
					got, found, e := sqliteReadSyncPlanLocal(tx, ir.ComputerID, plan.IntervalID)
					bgQACorrupt(t, e)
					scpQAZeroPlan(t, got, found)
				}
				interopRollback(t, tx)
				interopClose(t, c)
				scpQAReopen(t, f, m, snap)
			})
		}
	}
	t.Run("list-stored-key-must-be-recomputed", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		scpQAShadowConfig(t, tx)
		v := scpQAConfigValues(t, config)
		v[0] = sqliteio.Text("777/888")
		asQABindFixture(t, tx, "sync_configurations", scpQAConfigColumns, v)
		rows, e := sqliteSyncConfigurations(tx)
		bgQACorrupt(t, e)
		if rows != nil {
			t.Fatal("stored-key mismatch leaked list")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		scpQAReopen(t, f, m, snap)
	})
	t.Run("timestamp-missing-clock", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		scpQAShadowConfig(t, tx)
		cfg := scpQAClone(config)
		cfg.Mode = "timestamp"
		cfg.DurationPolicy = "exact"
		cfg.PolicyVersion = "exact-v1"
		cfg.Clock = nil
		asQABindFixture(t, tx, "sync_configurations", scpQAConfigColumns, scpQAConfigValues(t, cfg))
		got, found, e := sqliteReadSyncConfiguration(tx, cfg.AccountID, cfg.UserID)
		bgQACorrupt(t, e)
		scpQAZeroConfig(t, got, found)
		interopRollback(t, tx)
		interopClose(t, c)
		scpQAReopen(t, f, m, snap)
	})
}

func TestSQLiteSyncPlanRealScopedParentsFrozenConsentAndLocalStaging(t *testing.T) {
	config, plan, ir, root := scpQAOracle(t)
	f, m := scpQASeed(t, ir.ComputerID)
	snap := scpQAEmptySnapshot(t, f)
	for _, axis := range []string{"absent-primary", "absent-root", "absent-interval", "foreign-interval", "corrupt-interval", "corrupt-root", "bad-company", "frozen-consent-independent"} {
		t.Run(axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if axis == "absent-primary" {
				got, found, e := sqliteReadSyncPlanLocal(tx, ir.ComputerID, plan.IntervalID)
				if e != nil {
					t.Fatal(e)
				}
				scpQAZeroPlan(t, got, found)
				if d, e := sqliteWriteSyncPlanLocal(tx, ir.ComputerID, nil, plan); d != 0 {
					t.Fatal("missing parent delta")
				} else {
					bgQACorrupt(t, e)
				}
			} else {
				scpQAParents(t, tx, ir, root)
				if axis == "frozen-consent-independent" {
					// The local plan is a frozen scalar, not the complete selected
					// item. Account/user matching is the later graph's responsibility.
					frozen := plan
					frozen.Configuration = scpQAClone(config)
					frozen.Configuration.AccountID = "77"
					frozen.Configuration.UserID = "88"
					if d, e := sqliteWriteSyncPlanLocal(tx, ir.ComputerID, nil, frozen); e != nil || d != scpQAPlanCharge(t, frozen) {
						t.Fatal("frozen mismatch local admission", e)
					}
					scpQAReadPlan(t, tx, ir.ComputerID, frozen)
					current := scpQAClone(config)
					current.Revision = "99"
					current.DurationPolicy = "exact"
					current.PolicyVersion = "exact-v1"
					if _, e := sqliteWriteSyncConfiguration(tx, nil, current); e != nil {
						t.Fatal(e)
					}
					scpQAReadPlan(t, tx, ir.ComputerID, frozen)
					if interopCount(t, tx, "SELECT plan_present FROM outbox") != 0 {
						t.Fatal("root staging flag touched")
					}
				} else {
					if _, e := sqliteWriteSyncPlanLocal(tx, ir.ComputerID, nil, plan); e != nil {
						t.Fatal("real local parents positive", e)
					}
					scpQAReadPlan(t, tx, ir.ComputerID, plan)
					switch axis {
					case "absent-root":
						asQAShadow(t, tx, "outbox", scpQAOutboxColumns)
						interopDone(t, tx, "DELETE FROM outbox WHERE interval_id=?", sqliteio.Text(ir.ID))
					case "absent-interval":
						asQAShadow(t, tx, "intervals", fpiQAIntervalCols)
						interopDone(t, tx, "DELETE FROM intervals WHERE interval_id=?", sqliteio.Text(ir.ID))
					case "foreign-interval":
						interopDone(t, tx, "UPDATE intervals SET computer_id=?,group_order=? WHERE interval_id=?", sqliteio.Text(asQAForeign), sqliteio.Text(attributionKey(asQAForeign, ir.Attribution)), sqliteio.Text(ir.ID))
						if _, found, e := sqliteReadOutboxLocal(tx, asQAForeign, ir.ID); e != nil || !found {
							t.Fatal("valid real foreign owner positive", e)
						}
					case "corrupt-interval":
						asQAShadow(t, tx, "intervals", fpiQAIntervalCols)
						interopDone(t, tx, "UPDATE intervals SET duration_ns=? WHERE interval_id=?", sqliteio.Text("36000000000"), sqliteio.Text(ir.ID))
					case "corrupt-root":
						asQAShadow(t, tx, "outbox", scpQAOutboxColumns)
						interopDone(t, tx, "UPDATE outbox SET revision=? WHERE interval_id=?", sqliteio.Integer(1), sqliteio.Text(ir.ID))
					case "bad-company":
						scpQAShadowPlan(t, tx)
						scpQASet(t, tx, "sync_plans", "company_source", ir.ID, sqliteio.Text("private invalid company"))
					}
					got, found, e := sqliteReadSyncPlanLocal(tx, ir.ComputerID, plan.IntervalID)
					bgQACorrupt(t, e)
					scpQAZeroPlan(t, got, found)
					if axis != "bad-company" {
						after := plan
						after.CompanySource = "user_declared_fallback"
						if d, e := sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &plan, after); d != 0 {
							t.Fatal("corrupt selected parent delta")
						} else {
							bgQACorrupt(t, e)
						}
					}
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
			scpQAReopen(t, f, m, snap)
		})
	}
}

func TestSQLiteSyncConfigurationPlanRawRefusalBeforeSQLAndInputOwnership(t *testing.T) {
	base, plan, ir, _ := scpQAOracle(t)
	invalid := []struct {
		name   string
		change func(*SyncConfiguration)
	}{
		{"account", func(c *SyncConfiguration) { c.AccountID = "0" }}, {"user", func(c *SyncConfiguration) { c.UserID = "01" }},
		{"revision-zero", func(c *SyncConfiguration) { c.Revision = "0" }}, {"revision-overflow", func(c *SyncConfiguration) { c.Revision = "18446744073709551616" }},
		{"revision-noncanonical", func(c *SyncConfiguration) { c.Revision = "01" }}, {"revision-signed", func(c *SyncConfiguration) { c.Revision = "-1" }},
		{"mode", func(c *SyncConfiguration) { c.Mode = "bogus" }}, {"policy", func(c *SyncConfiguration) { c.DurationPolicy = "bogus" }},
		{"policy-version", func(c *SyncConfiguration) { c.PolicyVersion = "exact-v1" }}, {"undeclared", func(c *SyncConfiguration) { c.Declared = false }},
		{"source", func(c *SyncConfiguration) { c.Source = "company_verified" }}, {"zero-time", func(c *SyncConfiguration) { c.DeclaredAt = time.Time{} }},
		{"nonUTC-time", func(c *SyncConfiguration) { c.DeclaredAt = c.DeclaredAt.In(time.FixedZone("private zone", 0)) }},
		{"unencodable-time", func(c *SyncConfiguration) { c.DeclaredAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"duration-clock", func(c *SyncConfiguration) { c.Clock = asQAPointer("12h") }},
		{"timestamp-null-clock", func(c *SyncConfiguration) {
			c.Mode = "timestamp"
			c.DurationPolicy = "exact"
			c.PolicyVersion = "exact-v1"
			c.Clock = nil
		}},
		{"timestamp-empty-clock", func(c *SyncConfiguration) {
			c.Mode = "timestamp"
			c.DurationPolicy = "exact"
			c.PolicyVersion = "exact-v1"
			c.Clock = asQAPointer("")
		}},
		{"timestamp-rounded-policy", func(c *SyncConfiguration) { c.Mode = "timestamp"; c.Clock = asQAPointer("24h") }},
	}
	for _, field := range []string{"account", "user", "revision", "mode", "policy", "version", "clock", "source"} {
		field := field
		invalid = append(invalid, struct {
			name   string
			change func(*SyncConfiguration)
		}{"utf8-" + field, func(c *SyncConfiguration) {
			bad := string([]byte{'x', 0xff})
			switch field {
			case "account":
				c.AccountID = bad
			case "user":
				c.UserID = bad
			case "revision":
				c.Revision = bad
			case "mode":
				c.Mode = bad
			case "policy":
				c.DurationPolicy = bad
			case "version":
				c.PolicyVersion = bad
			case "clock":
				c.Clock = &bad
			case "source":
				c.Source = bad
			}
		}})
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			bad := scpQAClone(base)
			tc.change(&bad)
			owned := scpQAClone(bad)
			if d, e := sqliteSyncConfigurationCharge(bad); d != 0 {
				t.Fatal("bad scalar charge")
			} else {
				bgQAValidation(t, e)
			}
			// nil Tx is deliberate: raw and final materialization validation must
			// finish before any SQL or dependent parent read is attempted.
			if d, e := sqliteWriteSyncConfiguration(nil, nil, bad); d != 0 {
				t.Fatal("bad scalar insert delta")
			} else {
				bgQAValidation(t, e)
			}
			if d, e := sqliteWriteSyncConfiguration(nil, &base, bad); d != 0 {
				t.Fatal("bad scalar update delta")
			} else {
				bgQAValidation(t, e)
			}
			p := plan
			p.Configuration = bad
			if d, e := sqliteSyncPlanLocalCharge(p); d != 0 {
				t.Fatal("bad embedded charge")
			} else {
				bgQAValidation(t, e)
			}
			if d, e := sqliteWriteSyncPlanLocal(nil, ir.ComputerID, nil, p); d != 0 {
				t.Fatal("bad embedded insert delta")
			} else {
				bgQAValidation(t, e)
			}
			if d, e := sqliteWriteSyncPlanLocal(nil, ir.ComputerID, &plan, p); d != 0 {
				t.Fatal("bad embedded update delta")
			} else {
				bgQAValidation(t, e)
			}
			if !reflect.DeepEqual(bad, owned) {
				t.Fatal("rejected caller input repaired or mutated")
			}
		})
	}
	for _, company := range []string{"", "company", string([]byte{0xff})} {
		p := plan
		p.CompanySource = company
		if d, e := sqliteSyncPlanLocalCharge(p); d != 0 {
			t.Fatal("bad company charge")
		} else {
			bgQAValidation(t, e)
		}
		if d, e := sqliteWriteSyncPlanLocal(nil, ir.ComputerID, nil, p); d != 0 {
			t.Fatal("bad company insert delta")
		} else {
			bgQAValidation(t, e)
		}
	}
	for _, id := range []string{"", strings.ToUpper(plan.IntervalID), "not-uuid"} {
		if id == plan.IntervalID {
			continue
		}
		p := plan
		p.IntervalID = id
		if d, e := sqliteSyncPlanLocalCharge(p); d != 0 {
			t.Fatal("bad owner charge")
		} else {
			bgQAValidation(t, e)
		}
		if d, e := sqliteWriteSyncPlanLocal(nil, ir.ComputerID, nil, p); d != 0 {
			t.Fatal("bad owner insert delta")
		} else {
			bgQAValidation(t, e)
		}
	}
	for _, scope := range []string{"account", "user"} {
		after := scpQAClone(base)
		if scope == "account" {
			after.AccountID = "77"
		} else {
			after.UserID = "88"
		}
		if d, e := sqliteWriteSyncConfiguration(nil, &base, after); d != 0 {
			t.Fatal("changed configuration owner delta")
		} else {
			bgQAValidation(t, e)
		}
	}
	other := plan
	other.IntervalID = qaSyncID(800)
	if d, e := sqliteWriteSyncPlanLocal(nil, ir.ComputerID, &plan, other); d != 0 {
		t.Fatal("changed plan owner delta")
	} else {
		bgQAValidation(t, e)
	}
	if d, e := sqliteWriteSyncPlanLocal(nil, "not-computer", nil, plan); d != 0 {
		t.Fatal("bad computer delta")
	} else {
		bgQAValidation(t, e)
	}
	got, found, e := sqliteReadSyncConfiguration(nil, "01", base.UserID)
	bgQAValidation(t, e)
	scpQAZeroConfig(t, got, found)
	gotPlan, found, e := sqliteReadSyncPlanLocal(nil, "bad", plan.IntervalID)
	bgQAValidation(t, e)
	scpQAZeroPlan(t, gotPlan, found)
}

func TestSQLiteSyncPlanReplacementRequiresActualAbsentChildrenAndAttempts(t *testing.T) {
	config, plan, ir, root := scpQAOracle(t)
	f, m := scpQASeed(t, ir.ComputerID)
	snap := scpQAEmptySnapshot(t, f)
	for _, axis := range []string{"queued-part", "attempt-without-part", "wrong-native-part-owner", "wrong-native-attempt-owner"} {
		t.Run(axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			scpQAParents(t, tx, ir, root)
			if _, e := sqliteWriteSyncPlanLocal(tx, ir.ComputerID, nil, plan); e != nil {
				t.Fatal(e)
			}
			after := plan
			after.CompanySource = "user_declared_fallback"
			after.Configuration = scpQAClone(config)
			after.Configuration.Revision = "2"
			if _, e := sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &plan, after); e != nil {
				t.Fatal("absence positive replacement", e)
			}
			if _, e := sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &after, plan); e != nil {
				t.Fatal(e)
			}
			if axis == "queued-part" || axis == "wrong-native-part-owner" {
				scpQAPartFixture(t, tx, ir, root, config)
				if axis == "wrong-native-part-owner" {
					asQAShadow(t, tx, "sync_parts", "interval_id,ordinal,id,spent_date,duration_ns,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,planned_hours,planned_duration_ns,planned_residual_ns,started_time,ended_time,correlation,notes,state,entry_id,failure_category,returned_hours,rounded_hours,confirmed_duration_ns,provider_delta_ns,total_residual_ns,attachment_request_id,attachment_entry_id")
					interopDone(t, tx, "UPDATE sync_parts SET ordinal=? WHERE interval_id=?", sqliteio.Text("0"), sqliteio.Text(ir.ID))
				}
			} else {
				// Deferred FKs intentionally permit this caller staging. A plan
				// header must not erase consent beneath any retained attempt.
				asQABindFixture(t, tx, "sync_attempts", "interval_id,part_ordinal,ordinal,request_id,id,number,state,entry_id,failure_category", []sqliteio.Value{sqliteio.Text(ir.ID), sqliteio.Integer(0), sqliteio.Integer(0), sqliteio.Text(qaSyncID(811)), sqliteio.Text(qaSyncID(812)), sqliteio.Text("1"), sqliteio.Text("submitting"), sqliteio.Null(), sqliteio.Null()})
				if axis == "wrong-native-attempt-owner" {
					asQAShadow(t, tx, "sync_attempts", "interval_id,part_ordinal,ordinal,request_id,id,number,state,entry_id,failure_category")
					interopDone(t, tx, "UPDATE sync_attempts SET ordinal=? WHERE interval_id=?", sqliteio.Text("0"), sqliteio.Text(ir.ID))
				}
			}
			if d, e := sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &plan, after); d != 0 {
				t.Fatal("replacement with retained child delta")
			} else {
				bgQACorrupt(t, e)
			}
			scpQAReadPlan(t, tx, ir.ComputerID, plan)
			interopRollback(t, tx)
			interopClose(t, c)
			scpQAReopen(t, f, m, snap)
		})
	}
}

func TestSQLiteSyncConfigurationPlanSingletonReturningAndNativeConstraintsPreserved(t *testing.T) {
	config, plan, ir, root := scpQAOracle(t)
	f, m := scpQASeed(t, ir.ComputerID)
	snap := scpQAEmptySnapshot(t, f)
	for _, axis := range []string{"configuration-primary1555", "plan-primary1555", "configuration-unique2067", "configuration-trigger1811", "plan-trigger1811", "configuration-update-multiple", "plan-update-multiple", "configuration-read-multiple", "plan-read-multiple", "configuration-list-primary-kind", "configuration-list-primary-null", "configuration-list-primary-utf8"} {
		t.Run(axis, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			scpQAParents(t, tx, ir, root)
			if _, e := sqliteWriteSyncConfiguration(tx, nil, config); e != nil {
				t.Fatal(e)
			}
			if _, e := sqliteWriteSyncPlanLocal(tx, ir.ComputerID, nil, plan); e != nil {
				t.Fatal(e)
			}
			var d int64
			var err error
			switch axis {
			case "configuration-primary1555":
				// Isolate PRIMARY KEY from UNIQUE(account_id,user_id) under the
				// real STRICT schema. This deliberately malformed retained key is
				// rollback-only staging, not a valid decoded consent profile.
				key := config.AccountID + "/" + config.UserID
				interopDone(t, tx, "UPDATE sync_configurations SET account_id=?,user_id=? WHERE config_key=?", sqliteio.Text("77"), sqliteio.Text("88"), sqliteio.Text(key))
				witness := interopPrepare(t, tx, "SELECT config_key,account_id,user_id,(SELECT count(*) FROM sync_configurations WHERE account_id=? AND user_id=?) FROM sync_configurations WHERE config_key=?", sqliteio.Text(config.AccountID), sqliteio.Text(config.UserID), sqliteio.Text(key))
				if present, e := witness.Step(); e != nil || !present || witness.ColumnCount() != 4 {
					t.Fatal("isolated PK fixture witness", e)
				}
				for i, want := range []string{key, "77", "88"} {
					kind, e := witness.Kind(i)
					if e != nil || kind != sqliteio.TextKind {
						t.Fatal("isolated PK witness native TEXT", i, e)
					}
					got, e := witness.Text(i)
					if e != nil || got != want {
						t.Fatal("isolated PK witness literal value", i, e)
					}
				}
				kind, e := witness.Kind(3)
				if e != nil || kind != sqliteio.IntegerKind {
					t.Fatal("isolated UNIQUE absence native INTEGER", e)
				}
				count, e := witness.Int64(3)
				if e != nil || count != 0 {
					t.Fatal("competing owner pair must be absent for primary1555", e)
				}
				if present, e := witness.Step(); e != nil || present {
					t.Fatal("isolated PK fixture singleton DONE", e)
				}
				if e := witness.Close(); e != nil {
					t.Fatal("isolated PK fixture checked Close", e)
				}
				d, err = sqliteWriteSyncConfiguration(tx, nil, config)
			case "plan-primary1555":
				d, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, nil, plan)
			case "configuration-unique2067":
				// Keep the real STRICT/UNIQUE schema. A malformed preexisting key
				// exercises the actual secondary owner constraint on INSERT.
				interopDone(t, tx, "UPDATE sync_configurations SET config_key=? WHERE config_key=?", sqliteio.Text("77/88"), sqliteio.Text(config.AccountID+"/"+config.UserID))
				d, err = sqliteWriteSyncConfiguration(tx, nil, config)
			case "configuration-trigger1811":
				interopDone(t, tx, "CREATE TRIGGER qa_sync_config_refusal BEFORE UPDATE ON sync_configurations BEGIN SELECT RAISE(ABORT,'private synthetic trigger text'); END")
				after := scpQAClone(config)
				after.Revision = "2"
				d, err = sqliteWriteSyncConfiguration(tx, &config, after)
			case "plan-trigger1811":
				interopDone(t, tx, "CREATE TRIGGER qa_sync_plan_refusal BEFORE UPDATE ON sync_plans BEGIN SELECT RAISE(ABORT,'private synthetic trigger text'); END")
				after := plan
				after.CompanySource = "user_declared_fallback"
				d, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &plan, after)
			case "configuration-update-multiple", "configuration-read-multiple":
				scpQAShadowConfig(t, tx)
				asQABindFixture(t, tx, "sync_configurations", scpQAConfigColumns, scpQAConfigValues(t, config))
				if strings.Contains(axis, "update") {
					after := scpQAClone(config)
					after.Revision = "2"
					d, err = sqliteWriteSyncConfiguration(tx, &config, after)
				} else {
					got, found, e := sqliteReadSyncConfiguration(tx, config.AccountID, config.UserID)
					err = e
					scpQAZeroConfig(t, got, found)
				}
			case "plan-update-multiple", "plan-read-multiple":
				scpQAShadowPlan(t, tx)
				asQABindFixture(t, tx, "sync_plans", scpQAPlanColumns, scpQAPlanValues(t, plan))
				if strings.Contains(axis, "update") {
					after := plan
					after.CompanySource = "user_declared_fallback"
					d, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &plan, after)
				} else {
					got, found, e := sqliteReadSyncPlanLocal(tx, ir.ComputerID, plan.IntervalID)
					err = e
					scpQAZeroPlan(t, got, found)
				}
			default:
				scpQAShadowConfig(t, tx)
				bad := sqliteio.Integer(1)
				if strings.HasSuffix(axis, "null") {
					bad = sqliteio.Null()
				}
				if strings.HasSuffix(axis, "utf8") {
					bad = sqliteio.Text(string([]byte{0xff}))
				}
				scpQASet(t, tx, "sync_configurations", "config_key", config.AccountID+"/"+config.UserID, bad)
				rows, e := sqliteSyncConfigurations(tx)
				err = e
				if rows != nil {
					t.Fatal("bad primary list returned partial output")
				}
			}
			if d != 0 {
				t.Fatal("native/singleton refusal delta")
			}
			if strings.Contains(axis, "1555") || strings.Contains(axis, "2067") {
				fpiQANativeIdentity(t, err)
				var n *sqliteio.Error
				if !errors.As(err, &n) {
					t.Fatal("lost native constraint")
				}
				want := int32(1555)
				if strings.Contains(axis, "2067") {
					want = 2067
				}
				if n.Code != want {
					t.Fatal("wrong native exact code", n.Code, want)
				}
			} else if strings.Contains(axis, "1811") {
				var n *sqliteio.Error
				var domain *Error
				if !errors.As(err, &n) || n.Code != 1811 || errors.As(err, &domain) {
					t.Fatal("trigger incorrectly reclassified", err)
				}
				interopSafeError(t, err, "private synthetic trigger text")
			} else {
				bgQACorrupt(t, err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			scpQAReopen(t, f, m, snap)
		})
	}
}

func TestSQLiteSyncConfigurationPlanCancellationReadOnlyDeadlineAndOneBegin(t *testing.T) {
	config, plan, ir, root := scpQAOracle(t)
	f, m := scpQASeed(t, ir.ComputerID)
	snap := scpQAEmptySnapshot(t, f)
	for _, op := range []string{"configuration-read", "configuration-list", "configuration-insert", "configuration-update", "plan-read", "plan-insert", "plan-update"} {
		t.Run("cancel/"+op, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			conn, err := sqliteio.Open(ctx, f.directory, f.database, sqliteio.Options{Create: false, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close(context.Background()) })
			tx, err := conn.Begin(ctx, sqliteio.Write)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tx.Rollback() })
			scpQAParents(t, tx, ir, root)
			if _, e := sqliteWriteSyncConfiguration(tx, nil, config); e != nil {
				t.Fatal(e)
			}
			if _, e := sqliteWriteSyncPlanLocal(tx, ir.ComputerID, nil, plan); e != nil {
				t.Fatal(e)
			}
			owned := scpQAClone(config)
			ownedPlan := asQAJSON(t, plan)
			cancel()
			var d int64
			switch op {
			case "configuration-read":
				got, found, e := sqliteReadSyncConfiguration(tx, config.AccountID, config.UserID)
				err = e
				scpQAZeroConfig(t, got, found)
			case "configuration-list":
				rows, e := sqliteSyncConfigurations(tx)
				err = e
				if rows != nil {
					t.Fatal("canceled list returned output")
				}
			case "configuration-insert":
				d, err = sqliteWriteSyncConfiguration(tx, nil, config)
			case "configuration-update":
				after := scpQAClone(config)
				after.Revision = "2"
				d, err = sqliteWriteSyncConfiguration(tx, &config, after)
			case "plan-read":
				got, found, e := sqliteReadSyncPlanLocal(tx, ir.ComputerID, plan.IntervalID)
				err = e
				scpQAZeroPlan(t, got, found)
			case "plan-insert":
				d, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, nil, plan)
			case "plan-update":
				after := plan
				after.CompanySource = "user_declared_fallback"
				d, err = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &plan, after)
			}
			var native *sqliteio.Error
			if !errors.Is(err, context.Canceled) || !errors.As(err, &native) || native.Category != sqliteio.Canceled || d != 0 {
				t.Fatal("cancellation lost zero outputs/native cause", err)
			}
			if !reflect.DeepEqual(config, owned) || !reflect.DeepEqual(plan, ownedPlan) {
				t.Fatal("canceled mutation changed inputs")
			}
			scpQACanceledCleanup(t, tx, conn)
			scpQAReopen(t, f, m, snap)
		})
	}
	t.Run("read-only-no-hidden-writer", func(t *testing.T) {
		f2, m2 := scpQASeed(t, ir.ComputerID)
		c2, stage := interopOpen(t, f2, false, sqliteio.Write)
		delta := scpQAParents(t, stage, ir, root)
		d, e := sqliteWriteSyncConfiguration(stage, nil, config)
		if e != nil {
			t.Fatal(e)
		}
		delta += d
		d, e = sqliteWriteSyncPlanLocal(stage, ir.ComputerID, nil, plan)
		if e != nil {
			t.Fatal(e)
		}
		delta += d
		next, saved := scpQACommit(t, stage, m2, delta)
		interopClose(t, c2)
		for _, family := range []string{"configuration", "plan"} {
			conn, err := sqliteio.Open(context.Background(), f2.directory, f2.database, sqliteio.Options{Create: false, ReadOnly: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close(context.Background()) })
			tx, err := conn.Begin(context.Background(), sqliteio.Read)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tx.Rollback() })
			if family == "configuration" {
				got, found, e := sqliteReadSyncConfiguration(tx, config.AccountID, config.UserID)
				if e != nil || !found {
					t.Fatal(e)
				}
				scpQASameConfig(t, got, config)
			} else {
				scpQAReadPlan(t, tx, ir.ComputerID, plan)
			}
			var amount int64
			var refusal error
			if family == "configuration" {
				after := scpQAClone(config)
				after.Revision = "2"
				amount, refusal = sqliteWriteSyncConfiguration(tx, &config, after)
			} else {
				after := plan
				after.CompanySource = "user_declared_fallback"
				amount, refusal = sqliteWriteSyncPlanLocal(tx, ir.ComputerID, &plan, after)
			}
			var native *sqliteio.Error
			var domain *Error
			if amount != 0 || !errors.As(refusal, &native) || errors.As(refusal, &domain) {
				t.Fatal("read-only write lost native owner refusal", refusal)
			}
			interopRollback(t, tx)
			second, e := conn.Begin(context.Background(), sqliteio.Read)
			if second != nil || e == nil {
				t.Fatal("oneBegin connection reused after row calls", e)
			}
			interopClose(t, conn)
			scpQAReopen(t, f2, next, saved)
		}
	})
	t.Run("real-admission-deadline", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, e := sqliteio.Open(ctx, f.directory, f.database, sqliteio.Options{Create: false, AcquireDeadline: time.Now().Add(-time.Millisecond)})
		if conn != nil {
			if cleanup := conn.Close(context.Background()); cleanup != nil {
				t.Fatal("unexpected expired-admission connection cleanup failed", cleanup)
			}
		}
		if e == nil {
			t.Fatal("expired native admission accepted")
		}
		var native *sqliteio.Error
		if conn != nil || ctx.Err() != nil || !errors.As(e, &native) || native.Phase != sqliteio.Admission || native.Category != sqliteio.Busy || native.Code != 0 || !errors.Is(e, sqliteio.ErrBusy) {
			t.Fatal("expired admission with live context lost exact Busy refusal", e)
		}
		scpQAReopen(t, f, m, snap)
	})
}
