//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Test-only local projection fixture from complete, actual strict legacy states.
// This committed SQL subset is NOT claimed to be a fully migrated domain graph.
// Unresolved uncertainty/evidence rows are bound literally until that independent
// production slice lands; this fixture is not a dependency validator.

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const asQAActorColumns = "actor_key,id,revision,generation,sequence,state,health,binding_id,binding_revision,account_id,user_id,project_id,task_id,timezone,computer_id,parent_key,parent_generation,segment_id,last_evidence_capability,last_evidence_wall_sec,last_evidence_wall_nsec,last_evidence_wall_json,last_evidence_epoch,last_evidence_elapsed_raw,last_evidence_awake_raw,last_evidence_elapsed,last_evidence_awake"
const asQASegmentColumns = "segment_id,actor_key,actor_generation,binding_id,binding_revision,account_id,user_id,project_id,task_id,timezone,computer_id,group_order,epoch_id,start_sample_capability,start_sample_wall_sec,start_sample_wall_nsec,start_sample_wall_json,start_sample_epoch,start_sample_elapsed_raw,start_sample_awake_raw,start_sample_elapsed,start_sample_awake,confirmed_sample_capability,confirmed_sample_wall_sec,confirmed_sample_wall_nsec,confirmed_sample_wall_json,confirmed_sample_epoch,confirmed_sample_elapsed_raw,confirmed_sample_awake_raw,confirmed_sample_elapsed,confirmed_sample_awake,start_sec,start_nsec,start_json,confirmed_sec,confirmed_nsec,confirmed_json,end_sec,end_nsec,end_json,uncertainty_id,finalized"
const asQAUncertaintyColumns = "uncertainty_id,revision,actor_key,actor_generation,segment_id,account_id,user_id,project_id,task_id,timezone,computer_id,lower_bound_sec,lower_bound_nsec,lower_bound_json,upper_bound_sec,upper_bound_nsec,upper_bound_json,reason,state,resolution_end_sec,resolution_end_nsec,resolution_end_json,discarded"
const asQAEvidenceColumns = "uncertainty_id,detection_capability,detection_wall_sec,detection_wall_nsec,detection_wall_json,detection_epoch,detection_elapsed_raw,detection_awake_raw,last_confirmed_capability,last_confirmed_wall_sec,last_confirmed_wall_nsec,last_confirmed_wall_json,last_confirmed_epoch,last_confirmed_elapsed_raw,last_confirmed_awake_raw,last_confirmed_elapsed,last_confirmed_awake,bound_capability,bound_wall_sec,bound_wall_nsec,bound_wall_json,bound_epoch,bound_elapsed_raw,bound_awake_raw,bound_elapsed,bound_awake,missing_from,missing_through"
const asQAMax = "18446744073709551615"
const asQAForeign = "99999999-9999-4999-8999-999999999999"

func asQAPointer(value string) *string { return &value }

func asQAJSON[T any](t *testing.T, raw T) T {
	t.Helper()
	b, err := json.Marshal(raw)
	if err != nil {
		t.Fatal("fixture JSON encode failed", err)
	}
	var value T
	if err := json.Unmarshal(b, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func asQATimeWitness(t *testing.T, raw, persisted time.Time) {
	t.Helper()
	_, rawOffset := raw.Zone()
	_, persistedOffset := persisted.Zone()
	rawJSON, rawErr := raw.MarshalJSON()
	persistedJSON, persistedErr := persisted.MarshalJSON()
	if rawErr != nil || persistedErr != nil || string(rawJSON) != string(persistedJSON) || raw.Unix() != persisted.Unix() || raw.Nanosecond() != persisted.Nanosecond() || rawOffset != persistedOffset {
		t.Fatal("actual time persistence changed offset/seconds/nanoseconds/JSON")
	}
}

func asQAActor(a *Actor) sqliteActorLocalRow {
	return sqliteActorLocalRow{ID: a.ID, Revision: a.Revision, Ref: a.Ref, Sequence: a.Sequence, State: a.State, Health: a.Health, BindingID: a.BindingID, BindingRevision: a.BindingRevision, Attribution: a.Attribution, Parent: a.Parent, SegmentID: a.SegmentID, LastEvidence: a.LastEvidence}
}

func asQASegment(s *segment) sqliteSegmentLocalRow {
	return sqliteSegmentLocalRow{ID: s.ID, Actor: s.Actor, Binding: s.Binding, EpochID: s.EpochID, StartSample: s.StartSample, ConfirmedSample: s.ConfirmedSample, Start: s.Start, Confirmed: s.Confirmed, End: s.End, UncertaintyID: s.UncertaintyID, Finalized: s.Finalized}
}

func asQALegacy(t *testing.T) (*qaHarness, *state) {
	t.Helper()
	h, u := qaRecoveryUncertain(t)
	h.ingest(2500, qaEvent("A", "2", "1", "work", qaBindingB))
	h.ingest(2600, qaEvent("A", "2", "2", "observe_work", ""))
	h.ingest(2700, qaEvent("A", "2", "3", "wait_user", ""))
	h.ingest(2800, qaEvent("A", "2", "4", "work", ""))
	raw := bgQAReadLegacy(t, h.service)
	if !validState(raw) || len(raw.Uncertainties) == 0 || raw.Uncertainties[u.ID].Actor.Generation != "1" {
		t.Fatal("actual legacy generation-history fixture absent")
	}
	for _, a := range raw.Actors {
		// The actual legacy validator permits historical cross-computer Parent
		// refs without a current parent head; the SQL import preserves that policy.
		a.Parent = &ActorRef{Key: ActorKey{ComputerID: asQAForeign, Source: "manual-test", SessionID: "foreign-retained-parent", AgentID: "parent"}, Generation: asQAMax}
		a.UncertaintyIDs = append(a.UncertaintyIDs, u.ID)
	}
	return h, bgQALegacyMarshalOracle(t, h.service, h.path, raw, true)
}

func asQARows(t *testing.T, source *state) (sqliteActorLocalRow, sqliteSegmentLocalRow) {
	t.Helper()
	for _, a := range source.Actors {
		if a.State == "working" && a.SegmentID != nil {
			s := source.Segments[*a.SegmentID]
			if s == nil {
				t.Fatal("actual working segment absent")
			}
			return asQAJSON(t, asQAActor(a)), asQAJSON(t, asQASegment(s))
		}
	}
	t.Fatal("actual working Actor fixture absent")
	return sqliteActorLocalRow{}, sqliteSegmentLocalRow{}
}

func asQATime(t *testing.T, value time.Time) []sqliteio.Value {
	t.Helper()
	b, err := value.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var canonical time.Time
	if err := canonical.UnmarshalJSON(b); err != nil {
		t.Fatal(err)
	}
	return []sqliteio.Value{sqliteio.Integer(canonical.Unix()), sqliteio.Integer(int64(canonical.Nanosecond())), sqliteio.Text(string(b))}
}

func asQAOptionalTime(t *testing.T, value *time.Time) []sqliteio.Value {
	if value == nil {
		return []sqliteio.Value{sqliteio.Null(), sqliteio.Null(), sqliteio.Null()}
	}
	return asQATime(t, *value)
}

func asQAOptionalText(t *testing.T, value *string) sqliteio.Value {
	if value == nil {
		return sqliteio.Null()
	}
	return sqliteio.Text(bgQAPersistedString(t, *value))
}

func asQAClock(t *testing.T, sample ClockSample, available bool) []sqliteio.Value {
	t.Helper()
	s := asQAJSON(t, sample)
	values := []sqliteio.Value{sqliteio.Text(s.Capability)}
	values = append(values, asQATime(t, s.WallUTC)...)
	values = append(values, asQAOptionalText(t, s.Epoch), asQAOptionalText(t, s.ElapsedNS), asQAOptionalText(t, s.AwakeNS))
	if available {
		values = append(values, interopCounter(t, *s.ElapsedNS), interopCounter(t, *s.AwakeNS))
	}
	return values
}

func asQABindFixture(t *testing.T, tx *sqliteio.Tx, table, columns string, values []sqliteio.Value) {
	t.Helper()
	if len(values) != len(strings.Split(columns, ",")) {
		t.Fatal("literal fixture projection width differs", table)
	}
	interopDone(t, tx, "INSERT INTO "+table+"("+columns+") VALUES("+strings.TrimSuffix(strings.Repeat("?,", len(values)), ",")+")", values...)
}

func asQAUncertaintyFixture(t *testing.T, tx *sqliteio.Tx, raw *Uncertainty, evidence uncertaintyEvidence) {
	t.Helper()
	u := asQAJSON(t, raw)
	if u.State != "unresolved" || u.Discarded || u.ResolutionEnd != nil {
		t.Fatal("local fixture binder does not fabricate recovery decisions")
	}
	a := u.Attribution
	v := []sqliteio.Value{sqliteio.Text(u.ID), interopCounter(t, u.Revision), sqliteio.Text(actorKey(u.Actor.Key)), interopCounter(t, u.Actor.Generation), sqliteio.Text(u.SegmentID), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID), sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Text(u.Actor.Key.ComputerID)}
	v = append(v, asQATime(t, u.LowerBound)...)
	v = append(v, asQAOptionalTime(t, u.UpperBound)...)
	v = append(v, sqliteio.Text(u.Reason), sqliteio.Text(u.State))
	v = append(v, asQAOptionalTime(t, u.ResolutionEnd)...)
	v = append(v, sqliteio.Integer(0))
	asQABindFixture(t, tx, "uncertainties", asQAUncertaintyColumns, v)
	e := asQAJSON(t, evidence)
	v = []sqliteio.Value{sqliteio.Text(u.ID)}
	v = append(v, asQAClock(t, e.Detection, false)...)
	v = append(v, asQAClock(t, e.LastConfirmed, true)...)
	if e.BoundSample == nil {
		for i := 0; i < 9; i++ {
			v = append(v, sqliteio.Null())
		}
	} else {
		v = append(v, asQAClock(t, *e.BoundSample, true)...)
	}
	v = append(v, sqliteio.Text(e.MissingFrom), sqliteio.Text(e.MissingThrough))
	asQABindFixture(t, tx, "uncertainty_evidence", asQAEvidenceColumns, v)
}

func asQAActorCharge(t *testing.T, raw sqliteActorLocalRow) int64 {
	t.Helper()
	r := asQAJSON(t, raw)
	a := r.Attribution
	wall, err := r.LastEvidence.WallUTC.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	n := int64(299)
	for _, text := range []string{actorKey(r.Ref.Key), r.ID, r.State, r.Health, r.BindingID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, r.Ref.Key.ComputerID, r.LastEvidence.Capability, string(wall), *r.LastEvidence.Epoch, *r.LastEvidence.ElapsedNS, *r.LastEvidence.AwakeNS} {
		n += int64(len(text))
	}
	if r.Parent != nil {
		n += 24 + int64(len(actorKey(r.Parent.Key)))
	}
	if r.SegmentID != nil {
		n += 8 + int64(len(*r.SegmentID))
	}
	return n
}

func asQASegmentCharge(t *testing.T, raw sqliteSegmentLocalRow) int64 {
	t.Helper()
	r := asQAJSON(t, raw)
	a := r.Binding.Attribution
	n := int64(426)
	for _, text := range []string{r.ID, actorKey(r.Actor.Key), r.Binding.ID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, r.Actor.Key.ComputerID, epQAGroup(t, timelineEpoch{ComputerID: r.Actor.Key.ComputerID, Attribution: a}), r.EpochID} {
		n += int64(len(text))
	}
	for _, sample := range []ClockSample{r.StartSample, r.ConfirmedSample} {
		wall, err := sample.WallUTC.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		for _, text := range []string{sample.Capability, string(wall), *sample.Epoch, *sample.ElapsedNS, *sample.AwakeNS} {
			n += int64(len(text))
		}
	}
	for _, value := range []time.Time{r.Start, r.Confirmed} {
		b, err := value.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		n += int64(len(b))
	}
	if r.End != nil {
		b, err := r.End.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		n += 24 + int64(len(b))
	}
	if r.UncertaintyID != nil {
		n += 8 + int64(len(*r.UncertaintyID))
	}
	return n
}

func asQAChildCharge(t *testing.T, owner, reference string) int64 {
	return 59 + int64(len(bgQAPersistedString(t, owner))+len(bgQAPersistedString(t, reference)))
}

// Generic only within independent QA: literal stored bytes/kinds and accounting.
// It does not map production rows or validate domain relationships.
func asQALiteralAudit(t *testing.T, tx *sqliteio.Tx, table, columns, order string) ([][]string, int64) {
	t.Helper()
	rows := [][]string{}
	var total int64
	s := interopPrepare(t, tx, "SELECT "+columns+" FROM "+table+" ORDER BY "+order)
	for {
		present, err := s.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !present {
			break
		}
		if s.ColumnCount() != len(strings.Split(columns, ",")) {
			t.Fatal("literal audit width differs")
		}
		row := []string{}
		total += 32
		for i := 0; i < s.ColumnCount(); i++ {
			kind, err := s.Kind(i)
			if err != nil {
				t.Fatal(err)
			}
			total++
			var text string
			switch kind {
			case sqliteio.TextKind:
				text, err = s.Text(i)
				total += 8 + int64(len(text))
			case sqliteio.BlobKind:
				var b []byte
				b, err = s.Blob(i)
				total += 8 + int64(len(b))
				text = fmt.Sprintf("%x", b)
			case sqliteio.IntegerKind:
				var n int64
				n, err = s.Int64(i)
				total += 8
				text = fmt.Sprint(n)
			case sqliteio.NullKind:
				text = "NULL"
			default:
				t.Fatal("unexpected fixture stored kind")
			}
			if err != nil {
				t.Fatal(err)
			}
			row = append(row, fmt.Sprintf("%v:%s", kind, text))
		}
		rows = append(rows, row)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return rows, total
}

func asQAAudit(t *testing.T, tx *sqliteio.Tx) (map[string][][]string, int64) {
	t.Helper()
	result := map[string][][]string{}
	total := bgQAStoredCharge(t, tx) + epQAStoredCharge(t, tx)
	// These charges are already included above; snapshot their bytes as well so
	// a refused local writer cannot quietly mutate identities or metadata.
	for _, table := range []struct{ name, columns, order string }{
		{"actor_generations", bgQAGenerationColumns, "actor_key,generation"}, {"bindings", bgQABindingColumns, "binding_id"}, {"epochs", epQAColumns, "creation_ordinal"},
		{"store_meta", "singleton,schema_version,legacy_schema_version,computer_id,revision,sync_enabled,durability_nonce,state_basename,database_basename,migration_id,backup_sha256,logical_bytes", "singleton"},
	} {
		rows, _ := asQALiteralAudit(t, tx, table.name, table.columns, table.order)
		result[table.name] = rows
	}
	for _, table := range []struct{ name, columns, order string }{
		{"actors", asQAActorColumns, "actor_key"}, {"segments", asQASegmentColumns, "segment_id"},
		{"actor_uncertainties", "actor_key,ordinal,uncertainty_id", "actor_key,ordinal"}, {"segment_events", "segment_id,ordinal,event_reference", "segment_id,ordinal"},
		{"uncertainties", asQAUncertaintyColumns, "uncertainty_id"}, {"uncertainty_evidence", asQAEvidenceColumns, "uncertainty_id"},
	} {
		rows, charge := asQALiteralAudit(t, tx, table.name, table.columns, table.order)
		result[table.name] = rows
		total += charge
	}
	return result, total
}

func asQASeed(t *testing.T, source *state) (interopFixture, sqliteStoreMeta, map[string][][]string) {
	t.Helper()
	if !validState(source) {
		t.Fatal("complete source fixture outside real validator")
	}
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal(err)
	}
	m := interopMeta(f)
	m.ComputerID, m.Revision, m.SyncEnabled = source.ComputerID, source.Revision, source.SyncEnabled
	m.LogicalBytes = int64(114 + len(source.ComputerID) + len(f.authority) + len(f.database))
	for _, row := range bgQARowsFromLegacy(source) {
		delta, err := sqliteInsertBinding(tx, row)
		if err != nil || delta != bgQABindingCharge(t, row) {
			t.Fatal(err)
		}
		m.LogicalBytes += delta
	}
	ensure := func(ref ActorRef) {
		delta, err := sqliteEnsureActorGeneration(tx, ref)
		if err != nil || delta != 0 && delta != bgQAGenerationCharge(t, ref) {
			t.Fatal("historical generation fixture failed", err)
		}
		m.LogicalBytes += delta
	}
	for _, a := range source.Actors {
		ensure(a.Ref)
		if a.Parent != nil {
			ensure(*a.Parent)
		}
	}
	for _, s := range source.Segments {
		ensure(s.Actor)
	}
	for _, u := range source.Uncertainties {
		ensure(u.Actor)
	}
	for _, row := range epQARows(source) {
		delta, err := sqliteInsertEpoch(tx, row)
		if err != nil || delta != epQACharge(t, row) {
			t.Fatal(err)
		}
		m.LogicalBytes += delta
	}
	for id, u := range source.Uncertainties {
		asQAUncertaintyFixture(t, tx, u, source.UncertaintyEvidence[id])
	}
	for _, table := range []struct{ name, columns string }{{"uncertainties", asQAUncertaintyColumns}, {"uncertainty_evidence", asQAEvidenceColumns}} {
		_, charge := asQALiteralAudit(t, tx, table.name, table.columns, "uncertainty_id")
		m.LogicalBytes += charge
	}
	for _, s := range source.Segments {
		row := asQASegment(s)
		delta, err := sqliteWriteSegmentLocal(tx, source.ComputerID, nil, row)
		if err != nil || delta != asQASegmentCharge(t, row) {
			t.Fatal("segment fixture failed", err)
		}
		m.LogicalBytes += delta
	}
	for _, a := range source.Actors {
		row := asQAActor(a)
		delta, err := sqliteWriteActorLocal(tx, source.ComputerID, nil, row)
		if err != nil || delta != asQAActorCharge(t, row) {
			t.Fatal("Actor fixture failed", err)
		}
		m.LogicalBytes += delta
	}
	for _, s := range source.Segments {
		for ordinal, reference := range s.EventReferences {
			delta, err := sqliteAppendSegmentEventLocal(tx, source.ComputerID, s.ID, int64(ordinal), reference)
			if err != nil || delta != asQAChildCharge(t, s.ID, reference) {
				t.Fatal("event fixture failed", err)
			}
			m.LogicalBytes += delta
		}
	}
	for key, a := range source.Actors {
		for ordinal, id := range a.UncertaintyIDs {
			delta, err := sqliteAppendActorUncertaintyLocal(tx, source.ComputerID, a.Ref.Key, int64(ordinal), id)
			if err != nil || delta != asQAChildCharge(t, key, id) {
				t.Fatal("uncertainty edge fixture failed", err)
			}
			m.LogicalBytes += delta
		}
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal("real local subset FK check failed", err)
	}
	if err := sqliteInsertMeta(tx, m); err != nil {
		t.Fatal(err)
	}
	snapshot, total := asQAAudit(t, tx)
	if total != m.LogicalBytes {
		t.Fatalf("literal subset audit=%d metadata=%d", total, m.LogicalBytes)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return f, m, snapshot
}

func asQAReopen(t *testing.T, f interopFixture, meta sqliteStoreMeta, snapshot map[string][][]string, source *state) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	gotSnapshot, total := asQAAudit(t, tx)
	if !reflect.DeepEqual(gotSnapshot, snapshot) || total != meta.LogicalBytes {
		t.Fatal("reopen lost retained subset bytes/charge")
	}
	if got, err := sqliteReadMeta(tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, meta) {
		t.Fatal("reopen metadata differs", err)
	}
	for key, a := range source.Actors {
		if got, found, err := sqliteReadActorLocal(tx, source.ComputerID, a.Ref.Key); err != nil || !found || !reflect.DeepEqual(got, asQAJSON(t, asQAActor(a))) {
			t.Fatal("cold Actor scalar differs", err)
		}
		ids, err := sqliteActorUncertaintyIDsLocal(tx, source.ComputerID, a.Ref.Key)
		if err != nil || ids == nil || !reflect.DeepEqual(ids, a.UncertaintyIDs) {
			t.Fatal("cold Actor ordered children differ", key, err)
		}
	}
	for id, s := range source.Segments {
		if got, found, err := sqliteReadSegmentLocal(tx, source.ComputerID, id); err != nil || !found || !reflect.DeepEqual(got, asQAJSON(t, asQASegment(s))) {
			t.Fatal("cold segment scalar differs", err)
		}
		refs, err := sqliteSegmentEventsLocal(tx, source.ComputerID, id)
		if err != nil || refs == nil || !reflect.DeepEqual(refs, s.EventReferences) {
			t.Fatal("cold segment ordered children differ", err)
		}
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func asQAShadow(t *testing.T, tx *sqliteio.Tx, table, columns string) {
	t.Helper()
	interopDone(t, tx, "ALTER TABLE "+table+" RENAME TO qa_original_"+table)
	interopDone(t, tx, "CREATE TABLE "+table+"("+columns+")")
	interopDone(t, tx, "INSERT INTO "+table+"("+columns+") SELECT "+columns+" FROM qa_original_"+table)
}
