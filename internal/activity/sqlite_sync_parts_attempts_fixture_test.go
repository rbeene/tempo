//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Source-first QA for the eleven mutable part/attempt/root local APIs. Literal
// SQL fixture binding supplies real fresh-schema parents; it is not a composer,
// migration, producer stub or certificate of a complete selected sync graph.

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	_ "unsafe" // Solely for the existing private inert SQL test hook below.

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const spQAPlanCols = "interval_id,company_source,config_account_id,config_user_id,config_revision,config_mode,config_duration_policy,config_policy_version,config_clock,config_declared,config_declared_at_sec,config_declared_at_nsec,config_declared_at_json,config_source"
const spQAMetaCols = "singleton,schema_version,legacy_schema_version,computer_id,revision,sync_enabled,durability_nonce,state_basename,database_basename,migration_id,backup_sha256,logical_bytes"

// Exact current sqliteio sqlTestEvent/sqlTestHooks ABI; no invented query or
// bind fields. This source requires independent approval and re-review on any
// adapter ABI change. The linked function is the existing inert test seam; no
// native pointers or fake SQLite results are used. Tests are deliberately serial.
type spQASQLEvent struct {
	Phase, Operation string
	Code             int32
}
type spQASQLHooks struct {
	Observe func(spQASQLEvent)
	Fault   func(spQASQLEvent) error
}

//go:linkname spQASetSQLHooks github.com/rbeene/tempo/internal/activity/sqliteio.setSQLHooksForTest
func spQASetSQLHooks(spQASQLHooks)

func spQAHooks(t *testing.T, h spQASQLHooks) {
	t.Helper()
	spQASetSQLHooks(h)
	t.Cleanup(func() { spQASetSQLHooks(spQASQLHooks{}) })
}

func spQAHookPositive(t *testing.T, tx *sqliteio.Tx) {
	t.Helper()
	var events []spQASQLEvent
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		if e.Operation == "statement" || e.Operation == "prepare" {
			events = append(events, e)
		}
	}})
	s := interopPrepare(t, tx, "SELECT 1")
	if present, err := s.Step(); err != nil || !present {
		t.Fatal("native hook positive ROW", err)
	}
	if n, err := s.Int64(0); err != nil || n != 1 {
		t.Fatal("actual native result", err)
	}
	if present, err := s.Step(); err != nil || present {
		t.Fatal("native hook positive DONE", err)
	}
	bgQAFixtureError(t, "actual hook positive checked Close", s.Close())
	spQASetSQLHooks(spQASQLHooks{})
	want := []spQASQLEvent{
		{Phase: "prepare-before", Operation: "prepare"},
		{Phase: "step-before", Operation: "statement"},
		{Phase: "step-before-native", Operation: "statement"},
		{Phase: "step-after", Operation: "statement", Code: 100},
		{Phase: "step-before", Operation: "statement"},
		{Phase: "step-before-native", Operation: "statement"},
		{Phase: "step-after", Operation: "statement", Code: 101},
		{Phase: "finalize-after", Operation: "statement"},
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("private hook ABI/event control differs: %#v", events)
	}
}

func spQAPtr(s string) *string { return &s }
func spQAID(n int64) string    { return fmt.Sprintf("65000000-0000-4000-8000-%012d", n) }

func spQAPart(interval string, ordinal int64, p SyncPart) sqliteSyncPartLocalRow {
	return sqliteSyncPartLocalRow{IntervalID: interval, Ordinal: ordinal,
		ID: p.ID, SpentDate: p.SpentDate, DurationNS: p.DurationNS, Start: p.Start, End: p.End,
		PlannedHours: p.PlannedHours, PlannedDurationNS: p.PlannedDurationNS, PlannedResidualNS: p.PlannedResidualNS,
		StartedTime: p.StartedTime, EndedTime: p.EndedTime, Correlation: p.Correlation, Notes: p.Notes,
		State: p.State, EntryID: p.EntryID, FailureCategory: p.FailureCategory, ReturnedHours: p.ReturnedHours,
		RoundedHours: p.RoundedHours, ConfirmedDurationNS: p.ConfirmedDurationNS,
		ProviderDeltaNS: p.ProviderDeltaNS, TotalResidualNS: p.TotalResidualNS, Attachment: p.Attachment}
}
func spQAClonePart(r sqliteSyncPartLocalRow) sqliteSyncPartLocalRow {
	for _, target := range []**string{&r.StartedTime, &r.EndedTime, &r.EntryID, &r.FailureCategory, &r.ReturnedHours, &r.RoundedHours, &r.ConfirmedDurationNS, &r.ProviderDeltaNS, &r.TotalResidualNS} {
		if *target != nil {
			v := **target
			*target = &v
		}
	}
	if r.Attachment != nil {
		v := *r.Attachment
		r.Attachment = &v
	}
	return r
}
func spQACloneAttempt(r sqliteSyncAttemptLocalRow) sqliteSyncAttemptLocalRow {
	if r.Value.EntryID != nil {
		r.Value.EntryID = spQAPtr(*r.Value.EntryID)
	}
	if r.Value.FailureCategory != nil {
		r.Value.FailureCategory = spQAPtr(*r.Value.FailureCategory)
	}
	return r
}
func spQAQueued(r sqliteSyncPartLocalRow) sqliteSyncPartLocalRow {
	r = spQAClonePart(r)
	r.State = "queued"
	r.EntryID, r.FailureCategory, r.ReturnedHours, r.RoundedHours = nil, nil, nil, nil
	r.ConfirmedDurationNS, r.ProviderDeltaNS, r.TotalResidualNS, r.Attachment = nil, nil, nil, nil
	return r
}
func spQANextPart(r sqliteSyncPartLocalRow, ordinal int64) sqliteSyncPartLocalRow {
	r = spQAClonePart(r)
	r.Ordinal, r.ID = ordinal, spQAID(1000+ordinal)
	r.Correlation = "tempo:v1:" + r.ID
	return r
}
func spQANewAttempt(r sqliteSyncPartLocalRow, ordinal int64, request string) sqliteSyncAttemptLocalRow {
	return sqliteSyncAttemptLocalRow{IntervalID: r.IntervalID, PartOrdinal: r.Ordinal, Ordinal: ordinal,
		Value: SyncAttempt{RequestID: request, ID: spQAID(2000 + ordinal), Number: strconv.FormatInt(ordinal+1, 10), State: "submitting"}}
}

func spQAChargePart(r sqliteSyncPartLocalRow) int64 {
	n := int64(196)
	for _, s := range []string{r.IntervalID, r.ID, r.SpentDate, spQAJSONTime(r.Start), spQAJSONTime(r.End), r.PlannedHours, r.Correlation, r.Notes, r.State} {
		n += int64(len(s))
	}
	for _, s := range []*string{r.StartedTime, r.EndedTime, r.EntryID, r.FailureCategory, r.ReturnedHours, r.RoundedHours} {
		if s != nil {
			n += 8 + int64(len(*s))
		}
	}
	for _, s := range []*string{r.ConfirmedDurationNS, r.ProviderDeltaNS, r.TotalResidualNS} {
		if s != nil {
			n += 8
		}
	}
	if r.Attachment != nil {
		n += 16 + int64(len(r.Attachment.RequestID)+len(r.Attachment.EntryID))
	}
	return n
}
func spQAJSONTime(v time.Time) string {
	b, err := v.MarshalJSON()
	if err != nil {
		panic("only validated QA positive time reaches literal charge")
	}
	return string(b)
}
func spQAChargeAttempt(r sqliteSyncAttemptLocalRow) int64 {
	n := int64(97 + len(r.IntervalID) + len(r.Value.RequestID) + len(r.Value.ID) + len(r.Value.Number) + len(r.Value.State))
	for _, s := range []*string{r.Value.EntryID, r.Value.FailureCategory} {
		if s != nil {
			n += 8 + int64(len(*s))
		}
	}
	return n
}

func spQAPartValues(t *testing.T, r sqliteSyncPartLocalRow) []sqliteio.Value {
	t.Helper()
	integer := func(s string) sqliteio.Value {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatal("literal positive amount", err)
		}
		return sqliteio.Integer(n)
	}
	v := []sqliteio.Value{sqliteio.Text(r.IntervalID), sqliteio.Integer(r.Ordinal), sqliteio.Text(r.ID), sqliteio.Text(r.SpentDate), integer(r.DurationNS)}
	v = append(v, ueQATimeValues(t, r.Start)...)
	v = append(v, ueQATimeValues(t, r.End)...)
	v = append(v, sqliteio.Text(r.PlannedHours), integer(r.PlannedDurationNS), integer(r.PlannedResidualNS), ueQATextOptional(r.StartedTime), ueQATextOptional(r.EndedTime), sqliteio.Text(r.Correlation), sqliteio.Text(r.Notes), sqliteio.Text(r.State), ueQATextOptional(r.EntryID), ueQATextOptional(r.FailureCategory), ueQATextOptional(r.ReturnedHours), ueQATextOptional(r.RoundedHours))
	for _, s := range []*string{r.ConfirmedDurationNS, r.ProviderDeltaNS, r.TotalResidualNS} {
		value := sqliteio.Null()
		if s != nil {
			value = integer(*s)
		}
		v = append(v, value)
	}
	request, entry := sqliteio.Null(), sqliteio.Null()
	if r.Attachment != nil {
		request, entry = sqliteio.Text(r.Attachment.RequestID), sqliteio.Text(r.Attachment.EntryID)
	}
	return append(v, request, entry)
}
func spQAAttemptValues(r sqliteSyncAttemptLocalRow) []sqliteio.Value {
	return []sqliteio.Value{sqliteio.Text(r.IntervalID), sqliteio.Integer(r.PartOrdinal), sqliteio.Integer(r.Ordinal), sqliteio.Text(r.Value.RequestID), sqliteio.Text(r.Value.ID), sqliteio.Text(r.Value.Number), sqliteio.Text(r.Value.State), ueQATextOptional(r.Value.EntryID), ueQATextOptional(r.Value.FailureCategory)}
}
func spQABindPart(t *testing.T, tx *sqliteio.Tx, r sqliteSyncPartLocalRow) {
	t.Helper()
	asQABindFixture(t, tx, "sync_parts", sqQAPartCols, spQAPartValues(t, r))
}
func spQABindAttempt(t *testing.T, tx *sqliteio.Tx, r sqliteSyncAttemptLocalRow) {
	t.Helper()
	asQABindFixture(t, tx, "sync_attempts", sqQAAttemptCols, spQAAttemptValues(r))
}
func spQABindPlan(t *testing.T, tx *sqliteio.Tx, item OutboxItem) {
	t.Helper()
	cfg := item.Plan.Configuration
	v := []sqliteio.Value{sqliteio.Text(item.Interval.ID), sqliteio.Text(item.Plan.CompanySource), sqliteio.Text(cfg.AccountID), sqliteio.Text(cfg.UserID), interopCounter(t, cfg.Revision), sqliteio.Text(cfg.Mode), sqliteio.Text(cfg.DurationPolicy), sqliteio.Text(cfg.PolicyVersion), ueQATextOptional(cfg.Clock), sqliteio.Integer(1)}
	v = append(v, ueQATimeValues(t, cfg.DeclaredAt)...)
	v = append(v, sqliteio.Text(cfg.Source))
	asQABindFixture(t, tx, "sync_plans", spQAPlanCols, v)
}

func spQAAudit(t *testing.T, tx *sqliteio.Tx) (map[string][][]string, int64) {
	t.Helper()
	rows := map[string][][]string{}
	var total int64
	for _, table := range []struct{ name, columns, order string }{
		{"intervals", sqQAIntervalCols, "creation_ordinal"}, {"interval_components", sqQASealCols, "interval_id,component_id"},
		{"outbox", sqQAOutboxCols, "interval_id"}, {"sync_plans", spQAPlanCols, "interval_id"},
		{"sync_parts", sqQAPartCols, "interval_id,ordinal"}, {"sync_attempts", sqQAAttemptCols, "interval_id,part_ordinal,ordinal"},
		{"requests", mqQARequests, "request_id"}, {"store_meta", spQAMetaCols, "singleton"},
	} {
		literal, charge := asQALiteralAudit(t, tx, table.name, table.columns, table.order)
		rows[table.name] = literal
		// logical_bytes INTEGER9 and durability_nonce BLOB25 are excluded
		// from the charge, while their complete bytes remain in the snapshot.
		if table.name == "store_meta" {
			charge -= 34
		}
		total += charge
	}
	return rows, total
}

// Seeds owned real interval/outbox/plan parents, optionally one actual part and
// its actual history, plus strict literal request FK targets. No current config,
// supports, live frontier or pending projection is invented. Local primitives
// must not demand those unrelated complete-graph dependencies. The complete
// legacy oracle is valid, but the selected SQL subset is explicitly local.
func spQASeed(t *testing.T, phase string, partPresent, attemptsPresent bool) (interopFixture, sqliteStoreMeta, map[string][][]string, sqliteSyncPartLocalRow, sqliteSyncAttemptLocalRow, sqliteOutboxLocalRow) {
	t.Helper()
	st, item := sqQALegacy(t, phase)
	if item.Plan == nil || len(item.Plan.Parts) != 1 || len(item.Plan.Parts[0].Attempts) != 1 {
		t.Fatal("actual complete one-part/attempt oracle absent")
	}
	p := spQAPart(item.Interval.ID, 0, item.Plan.Parts[0])
	a := sqliteSyncAttemptLocalRow{IntervalID: p.IntervalID, PartOrdinal: 0, Ordinal: 0, Value: item.Plan.Parts[0].Attempts[0]}
	root := sqQAClone(sqQARow(item))
	f, m, _ := sqQASeed(t, st, item)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	sqQABindOutbox(t, tx, root)
	spQABindPlan(t, tx, item)
	ids := make([]string, 0, len(st.Requests))
	for id := range st.Requests {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := st.Requests[id]
		kind, payload := mqQAPayload(t, r)
		asQABindFixture(t, tx, "requests", mqQARequests, []sqliteio.Value{sqliteio.Text(id), sqliteio.Text(r.Operation), sqliteio.Text(r.Fingerprint), sqliteio.Text(kind), sqliteio.Text(payload)})
	}
	if partPresent {
		spQABindPart(t, tx, p)
	}
	if attemptsPresent {
		if !partPresent {
			t.Fatal("attempt needs real part parent")
		}
		spQABindAttempt(t, tx, a)
	}
	_, total := spQAAudit(t, tx)
	next := metaQANext(t, m)
	next.Revision, next.LogicalBytes = bump(m.Revision), total
	bgQAFixtureError(t, "literal local fixture metadata CAS", sqliteUpdateMeta(tx, m, next))
	bgQAFixtureError(t, "actual local fixture foreign keys", tx.CheckForeignKeys())
	snapshot, checked := spQAAudit(t, tx)
	if checked != next.LogicalBytes {
		t.Fatal("literal fixture billing mismatch")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return f, next, snapshot, p, a, root
}

func spQAReopen(t *testing.T, f interopFixture, m sqliteStoreMeta, want map[string][][]string) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	got, charge := spQAAudit(t, tx)
	if !reflect.DeepEqual(got, want) || charge != m.LogicalBytes {
		t.Fatal("cold literal rows/charges/metadata changed")
	}
	meta, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(meta, m) {
		t.Fatal("cold actual metadata changed", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}
func spQAReadPart(t *testing.T, tx *sqliteio.Tx, computer string, want sqliteSyncPartLocalRow) sqliteSyncPartLocalRow {
	t.Helper()
	got, found, err := sqliteReadSyncPartLocal(tx, computer, want.IntervalID, want.Ordinal)
	if err != nil || !found || !reflect.DeepEqual(got, want) {
		t.Fatal("owned actual part full28 projection", err)
	}
	return got
}
func spQAPartZero(t *testing.T, got sqliteSyncPartLocalRow, found bool, err error) {
	t.Helper()
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(got, sqliteSyncPartLocalRow{}) {
		t.Fatal("part error leaked partial output")
	}
}
func spQAAttemptsZero(t *testing.T, got []sqliteSyncAttemptLocalRow, err error) {
	t.Helper()
	bgQACorrupt(t, err)
	if got != nil {
		t.Fatal("attempt list error leaked partial output")
	}
}
func spQADeltaCorrupt(t *testing.T, delta int64, err error) {
	t.Helper()
	bgQACorrupt(t, err)
	if delta != 0 {
		t.Fatal("CAS error exposed nonzero delta")
	}
}
func spQADeltaValidation(t *testing.T, delta int64, err error) {
	t.Helper()
	bgQAValidation(t, err)
	if delta != 0 {
		t.Fatal("raw validation exposed nonzero delta")
	}
}

func spQAKinds(t *testing.T, tx *sqliteio.Tx, table, cols, where string, args []sqliteio.Value, want []sqliteio.Kind) {
	t.Helper()
	s := interopPrepare(t, tx, "SELECT "+cols+" FROM "+table+" WHERE "+where, args...)
	if present, err := s.Step(); err != nil || !present || s.ColumnCount() != len(want) {
		t.Fatal("literal native projection width", err)
	}
	for i, expected := range want {
		if got, err := s.Kind(i); err != nil || got != expected {
			t.Fatal("literal strict native kind", table, i, got, err)
		}
	}
	if present, err := s.Step(); err != nil || present {
		t.Fatal("literal projection singleton DONE", err)
	}
	bgQAFixtureError(t, "literal projection checked Close", s.Close())
}

// Scalar invocation lists are fixed to actual APIs; no adaptive error budget,
// expected failure or mock implementation. Every native failure must return nil
// or zero. Validation cases use a nil Tx to prove admission precedes any SQL.
func spQANativeError(t *testing.T, err error, cause error) {
	t.Helper()
	var native *sqliteio.Error
	if !errors.As(err, &native) || cause != nil && !errors.Is(err, cause) {
		t.Fatal("actual native cause/category lost", err)
	}
}

func spQARequireSetColumns(t *testing.T, tx *sqliteio.Tx, table, columns string, mutate func() (int64, error)) {
	t.Helper()
	for _, column := range strings.Split(columns, ",") {
		interopDone(t, tx, "CREATE TRIGGER qa_set_"+column+" BEFORE UPDATE OF "+column+" ON "+table+" BEGIN SELECT RAISE(ABORT,'QA full SET witness'); END")
		delta, err := mutate()
		if delta != 0 {
			t.Fatal("full SET trigger error exposed delta", column)
		}
		sqQAConstraint(t, err, 1811)
		interopDone(t, tx, "DROP TRIGGER qa_set_"+column)
	}
}
