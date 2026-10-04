//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Complete legacy files are behavior oracles only. The SQL fixtures below are
// explicitly local row subsets, never passed to validState or called migrated
// stores. Their real interval/outbox identities satisfy the actual deferred FKs.

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/harvest"
)

const mqQARequests = "request_id,operation,fingerprint,outcome_kind,payload"
const mqQAPending = "request_id,singleton,kind,effect_committed,snapshot_revision,limit_count,outbox_id,entry_id,if_revision,retry_rejected,confirmed"
const mqQARoots = "request_id,ordinal,outbox_id"
const mqQANewID = "79000000-0000-0000-0000-000000000001"

func mqQASlice[T any](a []T) []T {
	if a == nil {
		return nil
	}
	b := make([]T, len(a))
	copy(b, a)
	return b
}

func mqQAClone(row sqliteMutationRequestRow) sqliteMutationRequestRow {
	r := row.Value
	if r.BindingResult != nil {
		v := *r.BindingResult
		v.Binding.AttachedActors = mqQASlice(v.Binding.AttachedActors)
		r.BindingResult = &v
	}
	if r.MutationResult != nil {
		v := *r.MutationResult
		v.AffectedIDs = mqQASlice(v.AffectedIDs)
		if v.EntityRevision != nil {
			x := *v.EntityRevision
			v.EntityRevision = &x
		}
		r.MutationResult = &v
	}
	if r.SyncConfigurationResult != nil {
		v := *r.SyncConfigurationResult
		if v.Configuration.Clock != nil {
			x := *v.Configuration.Clock
			v.Configuration.Clock = &x
		}
		r.SyncConfigurationResult = &v
	}
	if r.SyncRun != nil {
		v := *r.SyncRun
		v.AttemptedIDs = mqQASlice(v.AttemptedIDs)
		v.ResolvedIDs = mqQASlice(v.ResolvedIDs)
		v.BlockedIDs = mqQASlice(v.BlockedIDs)
		r.SyncRun = &v
	}
	if r.PendingSync != nil {
		v := *r.PendingSync
		v.RootIDs = mqQASlice(v.RootIDs)
		if v.Run != nil {
			x := *v.Run
			v.Run = &x
		}
		if v.Reconcile != nil {
			x := *v.Reconcile
			v.Reconcile = &x
		}
		if v.Resolve != nil {
			x := *v.Resolve
			v.Resolve = &x
		}
		r.PendingSync = &v
	}
	if r.Error != nil {
		v := *r.Error
		if v.Details != nil {
			v.Details = make(map[string]any, len(r.Error.Details))
			for k, x := range r.Error.Details {
				v.Details[k] = x
			}
		}
		r.Error = &v
	}
	row.Value = r
	return row
}

func mqQAKindValue(r mutationRequest) (string, any) {
	switch {
	case r.BindingResult != nil:
		return "binding_result", r.BindingResult
	case r.MutationResult != nil:
		return "mutation_result", r.MutationResult
	case r.SyncConfigurationResult != nil:
		return "sync_configuration_result", r.SyncConfigurationResult
	case r.SyncRun != nil:
		return "sync_run", r.SyncRun
	case r.Error != nil:
		return "error", r.Error
	case r.PendingSync != nil:
		return "pending_sync", r.PendingSync
	default:
		return "", nil
	}
}

func mqQAPayload(t *testing.T, r mutationRequest) (string, string) {
	t.Helper()
	kind, value := mqQAKindValue(r)
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal("fixture payload encoding", err)
	}
	return kind, string(b)
}

func mqQAMaterialize(t *testing.T, row sqliteMutationRequestRow) sqliteMutationRequestRow {
	t.Helper()
	b, err := json.Marshal(row.Value)
	if err != nil {
		t.Fatal(err)
	}
	var r mutationRequest
	if json.Unmarshal(b, &r) != nil {
		t.Fatal("fixture typed materialization")
	}
	row.Value = r
	_, row.payload = mqQAPayload(t, r)
	return row
}

func mqQAReID(row sqliteMutationRequestRow, id string) sqliteMutationRequestRow {
	r := mqQAClone(row)
	r.ID, r.payload = id, ""
	if r.Value.BindingResult != nil {
		r.Value.BindingResult.RequestID = id
	}
	if r.Value.MutationResult != nil {
		r.Value.MutationResult.RequestID = id
	}
	if r.Value.SyncConfigurationResult != nil {
		r.Value.SyncConfigurationResult.RequestID = id
	}
	if r.Value.SyncRun != nil {
		r.Value.SyncRun.RequestID = id
	}
	if p := r.Value.PendingSync; p != nil {
		if p.Run != nil {
			p.Run.RequestID = id
			r.Value.Fingerprint = mutationFingerprint(r.Value.Operation, *p.Run)
		}
		if p.Reconcile != nil {
			p.Reconcile.RequestID = id
			r.Value.Fingerprint = mutationFingerprint(r.Value.Operation, *p.Reconcile)
		}
		if p.Resolve != nil {
			p.Resolve.RequestID = id
			r.Value.Fingerprint = mutationFingerprint(r.Value.Operation, *p.Resolve)
		}
	}
	return r
}

func mqQAFrom(st *state, id string) sqliteMutationRequestRow {
	return mqQAClone(sqliteMutationRequestRow{ID: id, Value: st.Requests[id]})
}

func mqQAComplete(t *testing.T, st *state, wantValid bool) *state {
	t.Helper()
	h := qaNew(t)
	h.seed()
	return bgQALegacyMarshalOracle(t, h.service, h.path, st, wantValid)
}

func mqQABindings(t *testing.T) *state {
	t.Helper()
	s, _ := qaLegacyLinkService(t)
	in := qaLinkInput(t)
	b, err := s.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
	bgQAFixtureError(t, "request-link", err)
	r, err := s.RepairBinding(context.Background(), RepairBindingInput{BindingID: b.Binding.ID, IfRevision: b.Binding.Revision, Path: t.TempDir(), RequestID: qaSyncID(701), Confirmed: true})
	bgQAFixtureError(t, "request-repair", err)
	_, err = s.Unlink(context.Background(), UnlinkInput{BindingID: b.Binding.ID, IfRevision: r.Binding.Revision, RequestID: qaSyncID(702), Confirmed: true})
	bgQAFixtureError(t, "request-unlink", err)
	return mqQAComplete(t, bgQAReadLegacy(t, s), true)
}

func mqQASyncComplete(t *testing.T) *state {
	t.Helper()
	s, _, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(710)}, qaSyncDeps(t, p))
	bgQAFixtureError(t, "request-sync-now", err)
	_, err = s.SyncPause(context.Background(), qaSyncID(711))
	bgQAFixtureError(t, "request-sync-pause", err)
	if len(p.posts) != 1 {
		t.Fatal("native terminal run fixture did not post exactly once to mock")
	}
	return mqQAComplete(t, bgQAReadLegacy(t, s), true)
}

// These are the actual saved native phases, with reached-barrier witnesses.
// Return both complete strict file snapshots around existing offline recovery.
func mqQAPhase(t *testing.T, shape string) (*state, *state, string) {
	t.Helper()
	id := qaSyncID(720)
	if shape == "run" || shape == "run-two" {
		s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
		p := qaNewSyncProvider(t)
		if shape == "run-two" {
			qaSyncAppendCapturedInterval(t, s)
		}
		qaSyncConfigure(t, s, p)
		qaSyncEnable(t, s)
		reached := 0
		s.store.fail = func(stage string) error {
			if stage == "sync_after_claim" {
				reached++
				return errors.New("owned request phase interruption")
			}
			return nil
		}
		in := SyncRunInput{RequestID: id}
		_, err := s.SyncNow(context.Background(), in, qaSyncDeps(t, p))
		qaCode(t, err, "local_write_unknown")
		s.store.fail = nil
		if reached != 1 || len(p.posts) != 0 {
			t.Fatal("actual claimed-run barrier not reached")
		}
		before := mqQAComplete(t, bgQAReadLegacy(t, s), true)
		_, err = qaLegacyNew(Options{Path: path}).SyncNow(context.Background(), in, qaSyncNoProvider(t))
		bgQAFixtureError(t, "request-run-recovery", err)
		return before, mqQAComplete(t, bgQAReadLegacy(t, s), true), id
	}
	s, p, item := qaSyncUnknown(t)
	if shape == "resolve" {
		p.entryErr = &harvest.Error{Code: "network"}
		in := SyncResolveInput{RequestID: id, OutboxID: item.ID, EntryID: "901", IfRevision: item.Revision, Confirmed: true}
		_, err := s.SyncResolve(context.Background(), in, qaSyncDeps(t, p))
		if err == nil {
			t.Fatal("resolve network boundary did not fail")
		}
		before := mqQAComplete(t, bgQAReadLegacy(t, s), true)
		if before.Requests[id].PendingSync == nil {
			t.Fatal("resolve did not durably reserve before mock read")
		}
		_, err = s.SyncResolve(context.Background(), in, qaSyncNoProvider(t))
		qaCode(t, err, "local_write_unknown")
		if len(p.posts) != 1 {
			t.Fatal("resolve recovery repeated mock POST")
		}
		return before, mqQAComplete(t, bgQAReadLegacy(t, s), true), id
	}
	if shape != "reconcile-no-effect" && shape != "reconcile-effect" {
		t.Fatal("unknown fixed phase fixture")
	}
	reached := 0
	if shape == "reconcile-no-effect" {
		p.entryListErr = &harvest.Error{Code: "network"}
		p.beforeEntryList = func() {
			s.store.fail = func(stage string) error {
				if stage == "before_write" {
					reached++
					return errors.New("owned final receipt interruption")
				}
				return nil
			}
		}
	} else {
		p.entryRows = []harvest.Object{qaSyncEntryFromPost(p.posts[0])}
		s.store.fail = func(stage string) error {
			if stage == "sync_before_complete" {
				reached++
				return errors.New("owned saved effect interruption")
			}
			return nil
		}
	}
	in := SyncReconcileInput{RequestID: id, OutboxID: item.ID}
	_, err := s.SyncReconcile(context.Background(), in, qaSyncDeps(t, p))
	if err == nil {
		t.Fatal("reconcile interruption accepted")
	}
	s.store.fail = nil
	if reached != 1 || len(p.listQueries) != 1 {
		t.Fatal("reconcile actual phase witness missing")
	}
	before := mqQAComplete(t, bgQAReadLegacy(t, s), true)
	if before.Requests[id].PendingSync == nil || before.Requests[id].PendingSync.EffectCommitted != (shape == "reconcile-effect") {
		t.Fatal("wrong native reconcile phase")
	}
	_, err = s.SyncReconcile(context.Background(), in, qaSyncNoProvider(t))
	if shape == "reconcile-no-effect" {
		qaCode(t, err, "local_write_unknown")
	} else {
		bgQAFixtureError(t, "request-reconcile-recovery", err)
	}
	if len(p.posts) != 1 {
		t.Fatal("reconcile recovery repeated mock POST")
	}
	return before, mqQAComplete(t, bgQAReadLegacy(t, s), true), id
}

func mqQAExpectedCharge(t *testing.T, row sqliteMutationRequestRow) int64 {
	t.Helper()
	kind, payload := mqQAPayload(t, row.Value)
	if row.payload != "" {
		payload = row.payload
	}
	n := int64(77 + len(row.ID) + len(row.Value.Operation) + len(row.Value.Fingerprint) + len(kind) + len(payload))
	if p := row.Value.PendingSync; p != nil {
		switch {
		case p.Run != nil:
			n += int64(99 + len(row.ID) + len("run"))
		case p.Reconcile != nil:
			n += int64(107 + len(row.ID) + len("reconcile") + len(bgQAPersistedString(t, p.Reconcile.OutboxID)))
		case p.Resolve != nil:
			n += int64(131 + len(row.ID) + len("resolve") + len(bgQAPersistedString(t, p.Resolve.OutboxID)) + len(bgQAPersistedString(t, p.Resolve.EntryID)) + len(bgQAPersistedString(t, p.Resolve.IfRevision)))
		}
		for _, root := range p.RootIDs {
			n += int64(59 + len(row.ID) + len(root))
		}
	}
	return n
}

// Literal schema-byte accounting, independent of all production charge helpers.
func mqQAScan(t *testing.T, tx *sqliteio.Tx, sql string, values ...sqliteio.Value) ([][]any, int64) {
	t.Helper()
	s := interopPrepare(t, tx, sql, values...)
	result := [][]any{}
	var total int64
	for {
		more, err := s.Step()
		if err != nil {
			t.Fatal("fixture scan step", err)
		}
		if !more {
			break
		}
		row := []any{}
		total += 32
		for i := 0; i < s.ColumnCount(); i++ {
			kind, err := s.Kind(i)
			if err != nil {
				t.Fatal(err)
			}
			total++
			switch kind {
			case sqliteio.NullKind:
				row = append(row, nil)
			case sqliteio.IntegerKind:
				v, err := s.Int64(i)
				if err != nil {
					t.Fatal(err)
				}
				row = append(row, v)
				total += 8
			case sqliteio.TextKind:
				v, err := s.Text(i)
				if err != nil {
					t.Fatal(err)
				}
				row = append(row, v)
				total += 8 + int64(len(v))
			case sqliteio.BlobKind:
				v, err := s.Blob(i)
				if err != nil {
					t.Fatal(err)
				}
				row = append(row, v)
				total += 8 + int64(len(v))
			default:
				t.Fatal("unexpected fixture SQL kind")
			}
		}
		result = append(result, row)
	}
	if err := s.Close(); err != nil {
		t.Fatal("fixture scan close", err)
	}
	return result, total
}

func mqQAUnit(t *testing.T, tx *sqliteio.Tx, id string) ([]any, int64) {
	t.Helper()
	all := []any{}
	var total int64
	for _, q := range []string{"SELECT " + mqQARequests + " FROM requests WHERE request_id=?", "SELECT " + mqQAPending + " FROM pending_sync WHERE request_id=?", "SELECT " + mqQARoots + " FROM pending_sync_roots WHERE request_id=? ORDER BY ordinal"} {
		rows, charge := mqQAScan(t, tx, q, sqliteio.Text(id))
		all = append(all, rows)
		total += charge
	}
	return all, total
}

func mqQAStart(t *testing.T, st *state) (interopFixture, *sqliteio.Conn, *sqliteio.Tx, sqliteStoreMeta) {
	t.Helper()
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal(err)
	}
	for _, row := range bgQARowsFromLegacy(st) {
		if _, err := sqliteInsertBinding(tx, row); err != nil {
			t.Fatal("actual binding dependency", err)
		}
	}
	for i, in := range st.Intervals {
		a := in.Attribution
		v := []sqliteio.Value{sqliteio.Text(in.ID), sqliteio.Text(st.ComputerID), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID), sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Text(attributionKey(st.ComputerID, a))}
		v = append(v, ueQATimeValues(t, in.Start)...)
		v = append(v, ueQATimeValues(t, in.End)...)
		v = append(v, ueQAUint(t, in.DurationNS), sqliteio.Integer(int64(i)))
		asQABindFixture(t, tx, "intervals", "interval_id,computer_id,account_id,user_id,project_id,task_id,timezone,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,duration_ns,creation_ordinal", v)
		o, exists := st.Outbox[in.ID]
		if !exists {
			t.Fatal("complete source lost real outbox target")
		}
		plan := int64(0)
		if o.Plan != nil {
			plan = 1
		}
		asQABindFixture(t, tx, "outbox", "interval_id,id,revision,state,correlation,entry_id,failure_category,retry_request_id,run_request_id,plan_present", []sqliteio.Value{sqliteio.Text(in.ID), sqliteio.Text(o.ID), ueQAUint(t, o.Revision), sqliteio.Text(o.State), sqliteio.Text(o.Correlation), asQAOptionalText(t, o.EntryID), asQAOptionalText(t, o.FailureCategory), asQAOptionalText(t, o.RetryRequestID), asQAOptionalText(t, o.RunRequestID), sqliteio.Integer(plan)})
	}
	m := interopMeta(f)
	m.ComputerID, m.Revision = st.ComputerID, st.Revision
	return f, c, tx, m
}

func mqQAFinishSeed(t *testing.T, f interopFixture, c *sqliteio.Conn, tx *sqliteio.Tx, m sqliteStoreMeta) sqliteStoreMeta {
	t.Helper()
	for _, q := range []string{"SELECT " + bgQABindingColumns + " FROM bindings", "SELECT interval_id,computer_id,account_id,user_id,project_id,task_id,timezone,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,duration_ns,creation_ordinal FROM intervals", "SELECT interval_id,id,revision,state,correlation,entry_id,failure_category,retry_request_id,run_request_id,plan_present FROM outbox", "SELECT " + mqQARequests + " FROM requests", "SELECT " + mqQAPending + " FROM pending_sync", "SELECT " + mqQARoots + " FROM pending_sync_roots"} {
		_, n := mqQAScan(t, tx, q)
		m.LogicalBytes += n
	}
	if err := sqliteInsertMeta(tx, m); err != nil {
		t.Fatal("fixture metadata", err)
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal("fixture actual FK dependencies", err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return m
}

func mqQASave(t *testing.T, st *state, row sqliteMutationRequestRow) (interopFixture, sqliteStoreMeta) {
	t.Helper()
	f, c, tx, m := mqQAStart(t, st)
	original := mqQAClone(row)
	delta, err := sqliteWriteMutationRequest(tx, st.ComputerID, nil, row)
	if err != nil || delta != mqQAExpectedCharge(t, row) {
		t.Fatal("fresh request unit/independent charge", delta, err)
	}
	if !reflect.DeepEqual(row, original) {
		t.Fatal("writer changed source DTO")
	}
	_, actual := mqQAUnit(t, tx, row.ID)
	if actual != delta {
		t.Fatal("materialized unit bytes differ from charge")
	}
	return f, mqQAFinishSeed(t, f, c, tx, m)
}

func mqQARead(t *testing.T, tx *sqliteio.Tx, computer, id, ceiling string) sqliteMutationRequestRow {
	t.Helper()
	got, ok, err := sqliteReadMutationRequestLocal(tx, computer, id, ceiling)
	if err != nil || !ok {
		t.Fatal("selected request read", ok, err)
	}
	return got
}

func mqQACode(t *testing.T, err error, want string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != want {
		t.Fatalf("expected %s, got %T %v", want, err, err)
	}
}

func mqQAZeroRead(t *testing.T, row sqliteMutationRequestRow, found bool, err error, code string) {
	t.Helper()
	mqQACode(t, err, code)
	if found || !reflect.DeepEqual(row, sqliteMutationRequestRow{}) {
		t.Fatal("failed read leaked usable request")
	}
}

func mqQARawInsert(t *testing.T, tx *sqliteio.Tx, row sqliteMutationRequestRow) {
	t.Helper()
	kind, payload := mqQAPayload(t, row.Value)
	if row.payload != "" {
		payload = row.payload
	}
	interopDone(t, tx, "INSERT INTO requests("+mqQARequests+") VALUES(?,?,?,?,?)", sqliteio.Text(row.ID), sqliteio.Text(row.Value.Operation), sqliteio.Text(row.Value.Fingerprint), sqliteio.Text(kind), sqliteio.Text(payload))
}

func mqQAIDs(st *state) []string {
	ids := []string{}
	for id := range st.Requests {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func mqQAReopen(t *testing.T, f interopFixture, m sqliteStoreMeta, row sqliteMutationRequestRow) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	got := mqQARead(t, tx, m.ComputerID, row.ID, m.Revision)
	want := mqQAMaterialize(t, row)
	if row.payload != "" {
		want.payload = row.payload
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("cold reopen changed typed result or actual payload")
	}
	meta, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(meta, m) {
		t.Fatal("local request helper changed caller-owned metadata", err)
	}
	_, bytes := mqQAUnit(t, tx, row.ID)
	if bytes != mqQAExpectedCharge(t, row) {
		t.Fatal("cold materialized bytes differ")
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func mqQAString(s string) *string { return &s }

func mqQANative(t *testing.T, err error, code int32) {
	t.Helper()
	var n *sqliteio.Error
	if !errors.As(err, &n) || n.Code != code {
		t.Fatalf("expected native code %d; got %v", code, err)
	}
	interopSafeError(t, err, "owned-private-trigger-message")
}

// Only corruption-reader tests use this deliberately permissive selected table.
// All writer/constraint/commit cases use the real embedded STRICT schema.
func mqQARelaxRequests(t *testing.T, tx *sqliteio.Tx) {
	t.Helper()
	interopDone(t, tx, "ALTER TABLE requests RENAME TO qa_original_requests")
	interopDone(t, tx, "CREATE TABLE requests(request_id,operation,fingerprint,outcome_kind,payload)")
}

func mqQANoncanonical(t *testing.T, r mutationRequest) string {
	t.Helper()
	_, p := mqQAPayload(t, r)
	return " \n\t" + strings.ReplaceAll(p, ",", ", ") + " \n"
}
