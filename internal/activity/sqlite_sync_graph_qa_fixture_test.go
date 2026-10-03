//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Complete state is a TEST-ONLY cold oracle. It is never supplied to production
// SQLite composition, and the fixture is not an importer or a writer port.
import (
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sgQAFixture struct {
	location      interopFixture
	meta          sqliteStoreMeta
	completeState *state
	snapshot      map[string][][]string
}

func sgQAComplete(t *testing.T, raw *state) *state {
	t.Helper()
	st := hnQAValid(t, raw)
	n, ok := counter(st.Revision)
	if !ok || !validSyncState(st) {
		t.Fatal("complete actual sync-state oracle rejected positive fixture")
	}
	for id, r := range st.Requests {
		if syncOperation(r.Operation) && !validSyncReceipt(st, id, r, n) {
			t.Fatal("complete actual receipt oracle rejected positive", id)
		}
	}
	return st
}

func sgQAAudit(t *testing.T, tx *sqliteio.Tx, m sqliteStoreMeta) (map[string][][]string, int64) {
	t.Helper()
	rows, total := sdQAAudit(t, tx, m)
	for _, table := range []struct{ name, columns, order string }{
		{"union_frontier", cfQAFrontierColumns, "component_id"},
		{"pending_finalization", cfQAPendingColumns, "component_id"},
		{"component_segments", "component_id,segment_id", "component_id,segment_id"},
		{"intervals", sqQAIntervalCols, "creation_ordinal"},
		{"interval_segments", "interval_id,ordinal,segment_id", "interval_id,ordinal"},
		{"interval_components", sqQASealCols, "interval_id,component_id"},
		{"outbox", sqQAOutboxCols, "interval_id"},
		{"sync_configurations", scpQAConfigColumns, "config_key"},
		{"sync_plans", spQAPlanCols, "interval_id"},
		{"sync_parts", sqQAPartCols, "interval_id,ordinal"},
		{"sync_attempts", sqQAAttemptCols, "interval_id,part_ordinal,ordinal"},
		{"pending_sync", mqQAPending, "request_id"},
		{"pending_sync_roots", mqQARoots, "request_id,ordinal"},
	} {
		literal, charge := asQALiteralAudit(t, tx, table.name, table.columns, table.order)
		rows[table.name] = literal
		total += charge
	}
	return rows, total
}

func sgQASeed(t *testing.T, raw *state) sgQAFixture {
	t.Helper()
	st := sgQAComplete(t, raw)
	f, old, _, st := sdQASeed(t, st)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	// Literal retained immutable rows include every finalized support, its real
	// owner, and the independently hashed fc1 seal. No live frontier is fabricated.
	for ordinal, in := range st.Intervals {
		a := in.Attribution
		values := []sqliteio.Value{sqliteio.Text(in.ID), sqliteio.Text(st.ComputerID), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID), sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Text(attributionKey(st.ComputerID, a))}
		values = append(values, ueQATimeValues(t, in.Start)...)
		values = append(values, ueQATimeValues(t, in.End)...)
		values = append(values, interopCounter(t, in.DurationNS), sqliteio.Integer(int64(ordinal)))
		asQABindFixture(t, tx, "intervals", sqQAIntervalCols, values)
		for index, id := range in.SegmentIDs {
			asQABindFixture(t, tx, "interval_segments", "interval_id,ordinal,segment_id", []sqliteio.Value{sqliteio.Text(in.ID), sqliteio.Integer(int64(index)), sqliteio.Text(id)})
		}
		asQABindFixture(t, tx, "interval_components", sqQASealCols, []sqliteio.Value{sqliteio.Text(in.ID), sqliteio.Text(sqQAComponent(t, st, in))})
		item := st.Outbox[in.ID]
		sqQABindOutbox(t, tx, sqQARow(item))
	}
	add := func(delta int64, err error) {
		t.Helper()
		if err != nil {
			t.Fatal("actual complete graph fixture writer", err)
		}
		if delta <= 0 {
			t.Fatal("actual insertion reported nonpositive charge")
		}
	}
	// These are the real configuration/plan/part/attempt and general request
	// primitives. Cycles use the actual deferred FKs, checked before commit.
	configKeys := make([]string, 0, len(st.SyncConfigurations))
	for key := range st.SyncConfigurations {
		configKeys = append(configKeys, key)
	}
	sort.Strings(configKeys)
	for _, key := range configKeys {
		add(sqliteWriteSyncConfiguration(tx, nil, st.SyncConfigurations[key]))
	}
	for _, id := range mqQAIDs(st) {
		r := st.Requests[id]
		if r.Operation == "activity.resolve" && r.MutationResult != nil {
			continue
		}
		add(sqliteWriteMutationRequest(tx, st.ComputerID, nil, sqliteMutationRequestRow{ID: id, Value: r}))
	}
	var writtenAttemptCharge int64
	for _, in := range st.Intervals {
		item := st.Outbox[in.ID]
		if item.Plan == nil {
			continue
		}
		add(sqliteWriteSyncPlanLocal(tx, st.ComputerID, nil, sqliteSyncPlanLocalRow{IntervalID: in.ID, CompanySource: item.Plan.CompanySource, Configuration: item.Plan.Configuration}))
		for ordinal, p := range item.Plan.Parts {
			add(sqliteWriteSyncPartLocal(tx, st.ComputerID, nil, spQAPart(in.ID, int64(ordinal), p)))
			for index, attempt := range p.Attempts {
				claim := attempt
				claim.State, claim.EntryID, claim.FailureCategory = "submitting", nil, nil
				before := sqliteSyncAttemptLocalRow{IntervalID: in.ID, PartOrdinal: int64(ordinal), Ordinal: int64(index), Value: claim}
				delta, err := sqliteAppendSyncAttempt(tx, st.ComputerID, before)
				add(delta, err)
				writtenAttemptCharge += delta
				if !reflect.DeepEqual(claim, attempt) {
					after := before
					after.Value = attempt
					delta, err = sqliteUpdateSyncAttempt(tx, st.ComputerID, before, after)
					bgQAFixtureError(t, "actual complete fixture terminal attempt", err)
					writtenAttemptCharge += delta
				}
			}
		}
	}
	_, actualAttemptCharge := asQALiteralAudit(t, tx, "sync_attempts", sqQAAttemptCols, "interval_id,part_ordinal,ordinal")
	if actualAttemptCharge != writtenAttemptCharge {
		t.Fatal("actual signed attempt charge differs from stored rows")
	}
	_, total := sgQAAudit(t, tx, old)
	m := metaQANext(t, old)
	m.Revision, m.LogicalBytes = bump(old.Revision), total
	bgQAFixtureError(t, "actual complete fixture metadata CAS", sqliteUpdateMeta(tx, old, m))
	bgQAFixtureError(t, "actual complete fixture deferred FK closure", tx.CheckForeignKeys())
	snapshot, charge := sgQAAudit(t, tx, m)
	if charge != m.LogicalBytes {
		t.Fatal("independent all-table charge differs")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return sgQAFixture{f, m, st, snapshot}
}

func sgQAUnchanged(t *testing.T, tx *sqliteio.Tx, m sqliteStoreMeta, snapshot map[string][][]string) {
	t.Helper()
	got, charge := sgQAAudit(t, tx, m)
	if charge != m.LogicalBytes || !reflect.DeepEqual(got, snapshot) {
		t.Fatal("graph reads changed row bytes, kinds, metadata, nonce or logical charge")
	}
}

func sgQAReopen(t *testing.T, f sgQAFixture) {
	t.Helper()
	c, tx := interopOpen(t, f.location, false, sqliteio.Read)
	sgQAUnchanged(t, tx, f.meta, f.snapshot)
	m, err := sqliteReadMeta(tx, f.location.authority, f.location.database)
	if err != nil || !reflect.DeepEqual(m, f.meta) {
		t.Fatal("cold meta read", err)
	}
	for _, in := range f.completeState.Intervals {
		sgQAPositive(t, tx, f, f.completeState.Outbox[in.ID])
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func sgQAPositive(t *testing.T, tx *sqliteio.Tx, f sgQAFixture, want OutboxItem) OutboxItem {
	t.Helper()
	snapshot, charge := sgQAAudit(t, tx, f.meta)
	supports, component, owner, err := sqliteReadFinalizationInterval(tx, f.meta.ComputerID, want.Interval.ID)
	if err != nil || len(supports) != len(want.Interval.SegmentIDs) || component != sqQAComponent(t, f.completeState, want.Interval) || owner.ID != want.ID {
		t.Fatal("actual retained finalizer prerequisite rejected positive", err)
	}
	bgQAFixtureError(t, "actual read-only support dependency prerequisite", sqliteValidateSelectedCaptureDependencies(tx, f.meta.ComputerID, f.meta.Revision, sqliteDependencySelection{SegmentIDs: want.Interval.SegmentIDs}))
	in, found, err := sqliteReadRetainedSyncInterval(tx, f.meta.ComputerID, want.Interval.ID, f.meta.Revision)
	if err != nil || !found || !reflect.DeepEqual(in, want.Interval) {
		t.Fatal("complete retained interval differs", err)
	}
	got, found, err := sqliteReadSyncItem(tx, f.meta.ComputerID, want.ID, f.meta.Revision)
	if err != nil || !found || !reflect.DeepEqual(got, want) {
		t.Fatalf("complete item differs: found=%t err=%v\ngot=%#v\nwant=%#v", found, err, got, want)
	}
	bgQAFixtureError(t, "explicit root selected graph positive", sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, []string{want.ID}, nil))
	sgQAStagedUnchanged(t, tx, f.meta, snapshot, charge)
	return got
}

func sgQAStagedUnchanged(t *testing.T, tx *sqliteio.Tx, m sqliteStoreMeta, snapshot map[string][][]string, charge int64) {
	t.Helper()
	got, actual := sgQAAudit(t, tx, m)
	if actual != charge || !reflect.DeepEqual(got, snapshot) {
		t.Fatal("read changed staged source bytes or materialized charge")
	}
}

func sgQAItemZero(t *testing.T, tx *sqliteio.Tx, f sgQAFixture, root string) {
	t.Helper()
	snapshot, charge := sgQAAudit(t, tx, f.meta)
	got, found, err := sqliteReadSyncItem(tx, f.meta.ComputerID, root, f.meta.Revision)
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(got, OutboxItem{}) {
		t.Fatal("corrupt item leaked usable partial value")
	}
	bgQACorrupt(t, sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, []string{root}, nil))
	sgQAStagedUnchanged(t, tx, f.meta, snapshot, charge)
}

func sgQAIntervalZero(t *testing.T, tx *sqliteio.Tx, f sgQAFixture, in Interval) {
	t.Helper()
	snapshot, charge := sgQAAudit(t, tx, f.meta)
	got, found, err := sqliteReadRetainedSyncInterval(tx, f.meta.ComputerID, in.ID, f.meta.Revision)
	bgQACorrupt(t, err)
	if found || !reflect.DeepEqual(got, Interval{}) {
		t.Fatal("corrupt interval leaked usable partial value")
	}
	sgQAItemZero(t, tx, f, f.completeState.Outbox[in.ID].ID)
	sgQAStagedUnchanged(t, tx, f.meta, snapshot, charge)
}

func sgQACalendar(t *testing.T, start, end time.Time, timezone, mode, policy, clock string) *state {
	t.Helper()
	a := cfQAAttr()
	a.Timezone = timezone
	h := qaNew(t)
	h.seed()
	st := bgQAReadLegacy(t, h.service)
	epoch, zero, elapsed := "sg-calendar-boot", "0", strconv.FormatInt(int64(end.Sub(start)), 10)
	first := ClockSample{Capability: "available", WallUTC: start.UTC(), Epoch: &epoch, ElapsedNS: &zero, AwakeNS: &zero}
	last := ClockSample{Capability: "available", WallUTC: end.UTC(), Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &elapsed}
	epID, segID := cfQAUUID(10010), cfQAUUID(10)
	st.Epochs = append(st.Epochs, &timelineEpoch{ID: epID, ComputerID: st.ComputerID, Attribution: a, Anchor: first})
	endUTC := end.UTC()
	st.Segments[segID] = &segment{ID: segID, Actor: ActorRef{Key: ActorKey{ComputerID: st.ComputerID, Source: "manual-test", SessionID: "sg-calendar", AgentID: "support"}, Generation: "1"}, Binding: BindingSnapshot{ID: qaBindingA, Revision: "1", Attribution: a}, EpochID: epID, StartSample: first, ConfirmedSample: last, Start: start.UTC(), Confirmed: endUTC, End: &endUTC, EventReferences: []string{}}
	bgQAFixtureError(t, "complete legacy finalization", finalize(st))
	cfg := SyncConfiguration{AccountID: a.AccountID, UserID: a.UserID, Revision: asQAMax, Mode: mode, DurationPolicy: policy, PolicyVersion: policy + "-v1", Declared: true, DeclaredAt: cfQATime(0), Source: "user_declared"}
	if clock != "" {
		cfg.Clock = spQAPtr(clock)
	}
	for id, item := range st.Outbox {
		plan, err := syncBuildPlan(item, cfg, "user_declared_fallback")
		if err != nil {
			t.Fatal("pure plan positive", err)
		}
		item.Plan, item.Revision = plan, asQAMax
		st.Outbox[id] = item
	}
	return sgQAComplete(t, st)
}

func sgQAFirst(st *state) OutboxItem { return st.Outbox[st.Intervals[0].ID] }

func sgQAStandalone(t *testing.T, st *state, ids []string, changed bool, entity *string) string {
	t.Helper()
	id := qaSyncID(890)
	if st.Requests == nil {
		st.Requests = map[string]mutationRequest{}
	}
	r := mutationRequest{Operation: "sync.resolve", Fingerprint: mutationFingerprint("sync.resolve", SyncResolveInput{RequestID: id}), MutationResult: &MutationResult{ContractVersion: 1, RequestID: id, SnapshotRevision: "1", AffectedIDs: ids, Changed: changed, EntityRevision: entity}}
	st.Requests[id] = r
	n, _ := counter(st.Revision)
	if !validSyncReceipt(st, id, r, n) || !validSyncState(st) {
		t.Fatal("actual standalone scalar/complete predicate policy changed")
	}
	return id
}

func sgQAReplaceRequest(t *testing.T, tx *sqliteio.Tx, id string, r mutationRequest) {
	t.Helper()
	kind, payload := mqQAPayload(t, r)
	interopDone(t, tx, "UPDATE requests SET operation=?,fingerprint=?,outcome_kind=?,payload=? WHERE request_id=?", sqliteio.Text(r.Operation), sqliteio.Text(r.Fingerprint), sqliteio.Text(kind), sqliteio.Text(payload), sqliteio.Text(id))
}

func sgQARelax(t *testing.T, tx *sqliteio.Tx, table string) {
	t.Helper()
	columns := map[string]string{"outbox": sqQAOutboxCols, "sync_parts": sqQAPartCols, "sync_attempts": sqQAAttemptCols}[table]
	if columns == "" {
		t.Fatal("no fixed relaxed table definition", table)
	}
	interopDone(t, tx, "ALTER TABLE "+table+" RENAME TO qa_original_"+table)
	interopDone(t, tx, "CREATE TABLE "+table+"("+columns+")")
	interopDone(t, tx, "INSERT INTO "+table+" SELECT "+columns+" FROM qa_original_"+table)
}

func sgQACloneState(t *testing.T, st *state) *state {
	t.Helper()
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	var owned state
	if err = json.Unmarshal(b, &owned); err != nil {
		t.Fatal(err)
	}
	return &owned
}

func sgQAAttached(t *testing.T) (*state, string) {
	t.Helper()
	st := sgQACalendar(t, cfQATime(0), cfQATime(36), "UTC", "duration", "exact", "")
	item := sgQAFirst(st)
	item.Revision = "1"
	id := sgQAStandalone(t, st, []string{item.ID}, false, spQAPtr("opaque historical entity"))
	p := &item.Plan.Parts[0]
	p.State, p.EntryID, p.ReturnedHours = "synced", spQAPtr("901"), spQAPtr(p.PlannedHours)
	p.ConfirmedDurationNS, p.ProviderDeltaNS, p.TotalResidualNS = spQAPtr(p.PlannedDurationNS), spQAPtr("0"), spQAPtr("0")
	p.Attachment = &SyncAttachment{RequestID: id, EntryID: "901"}
	syncFinishItem(&item)
	st.Outbox[item.Interval.ID] = item
	return sgQAComplete(t, st), id
}

func sgQARetry(t *testing.T) (*state, string) {
	t.Helper()
	st, item := sqQALegacy(t, "rejected")
	id := sgQAStandalone(t, st, []string{item.ID}, false, nil)
	item.State, item.RetryRequestID, item.FailureCategory = "queued", spQAPtr(id), nil
	st.Outbox[item.Interval.ID] = item
	return sgQAComplete(t, st), id
}

// The existing spQA test helpers own the already accepted inert hook bridge.
// This amendment introduces no hook ABI, native pointer, linkname or callback
// SQL. All observations and faults below only append or compare owned events.
type sgQALateReadResult struct {
	Interval Interval
	Item     OutboxItem
	Found    bool
	Err      error
}

func sgQALateRead(tx *sqliteio.Tx, f sgQAFixture, item OutboxItem, family string, roots, requests []string) sgQALateReadResult {
	switch family {
	case "interval":
		value, found, err := sqliteReadRetainedSyncInterval(tx, f.meta.ComputerID, item.Interval.ID, f.meta.Revision)
		return sgQALateReadResult{Interval: value, Found: found, Err: err}
	case "item":
		value, found, err := sqliteReadSyncItem(tx, f.meta.ComputerID, item.ID, f.meta.Revision)
		return sgQALateReadResult{Item: value, Found: found, Err: err}
	case "selected":
		return sgQALateReadResult{Err: sqliteValidateSelectedSync(tx, f.meta.ComputerID, f.meta.Revision, roots, requests)}
	default:
		panic("fixed graph QA family required")
	}
}

func sgQALatePositive(t *testing.T, family string, got sgQALateReadResult, item OutboxItem) {
	t.Helper()
	if got.Err != nil {
		t.Fatal("exact-operation native positive calibration", got.Err)
	}
	switch family {
	case "interval":
		if !got.Found || !reflect.DeepEqual(got.Interval, item.Interval) || !reflect.DeepEqual(got.Item, OutboxItem{}) {
			t.Fatal("calibrated complete interval differs")
		}
	case "item":
		if !got.Found || !reflect.DeepEqual(got.Item, item) || !reflect.DeepEqual(got.Interval, Interval{}) {
			t.Fatal("calibrated complete item differs")
		}
	case "selected":
		if got.Found || !reflect.DeepEqual(got.Item, OutboxItem{}) || !reflect.DeepEqual(got.Interval, Interval{}) {
			t.Fatal("selected calibration invented output")
		}
	default:
		t.Fatal("fixed graph calibration family required")
	}
}

func sgQALateTrace(t *testing.T, tx *sqliteio.Tx, f sgQAFixture, item OutboxItem, family string, roots, requests []string) []spQASQLEvent {
	t.Helper()
	trace := []spQASQLEvent{}
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) { trace = append(trace, e) }})
	got := sgQALateRead(tx, f, item, family, roots, requests)
	spQASetSQLHooks(spQASQLHooks{})
	sgQALatePositive(t, family, got, item)
	rows, done, closed, prepares := 0, 0, 0, 0
	for i, e := range trace {
		if e.Operation != "statement" && e.Operation != "prepare" {
			t.Fatal("graph operation took caller transaction/connection ownership", e)
		}
		if e.Phase == "prepare-before" {
			prepares++
		}
		if e.Phase == "step-after" {
			if i == 0 || trace[i-1] != (spQASQLEvent{Phase: "step-before-native", Operation: "statement"}) {
				t.Fatal("calibration does not witness actual native Step", e)
			}
			switch e.Code {
			case 100:
				rows++
			case 101:
				done++
			default:
				t.Fatal("positive native Step returned unexpected code", e)
			}
		}
		if e.Phase == "finalize-after" {
			if e.Code != 0 {
				t.Fatal("positive native finalize was not SQLITE_OK", e)
			}
			closed++
		}
	}
	if rows == 0 || done == 0 || closed == 0 || closed != prepares {
		t.Fatal("positive operation lacks real ROW100/DONE101/all-cursor-finalize0 control", rows, done, closed, prepares)
	}
	return trace
}

func sgQALateTarget(t *testing.T, trace []spQASQLEvent, boundary string) (int, spQASQLEvent) {
	t.Helper()
	target := spQASQLEvent{Phase: "step-after", Operation: "statement", Code: 100}
	switch boundary {
	case "ROW":
	case "DONE":
		target.Code = 101
	case "Close":
		target.Phase, target.Code = "finalize-after", 0
	default:
		t.Fatal("fixed native boundary required")
	}
	// The index comes solely from the exact operation's completed positive
	// trace. No statement identity, query text or guessed ordinal is available
	// in this hook ABI. Prefix equality during the fault pass certifies replay.
	for i := len(trace) - 1; i >= 0; i-- {
		if trace[i] == target {
			return i, target
		}
	}
	t.Fatal("actual calibrated target boundary absent", target)
	return 0, spQASQLEvent{}
}
