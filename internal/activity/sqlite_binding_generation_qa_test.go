//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Source-first independent fixtures for the proposed inactive row APIs.
// Reuses the real embedded installer and owned interop fixture helpers. Direct
// receipt fixture writes use actual saved legacy results, not invented payloads.
// No importer, Service activation or complete-domain accounting is accepted here.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const bgQABindingColumns = "binding_id,revision,account_id,user_id,project_id,task_id,timezone,computer_id,active,record_present,kind,locator,deleted"
const bgQAGenerationColumns = "actor_key,generation,computer_id,source,session_id,agent_id"

func bgQAReadLegacy(t *testing.T, s *Service) *state {
	t.Helper()
	st, exists, err := s.store.read(context.Background())
	if err != nil || !exists || !validState(st) {
		t.Fatalf("real legacy fixture did not validate: exists=%t error=%v", exists, err)
	}
	return st
}

func bgQARowsFromLegacy(st *state) []sqliteBindingRow {
	rows := make([]sqliteBindingRow, 0, len(st.Bindings)+len(st.BindingRecords))
	for _, record := range st.BindingRecords {
		r := record
		rows = append(rows, sqliteBindingRow{ComputerID: st.ComputerID, Snapshot: r.Snapshot, Record: &r})
	}
	for id, snapshot := range st.Bindings {
		if _, recorded := st.BindingRecords[id]; !recorded {
			rows = append(rows, sqliteBindingRow{ComputerID: st.ComputerID, Snapshot: snapshot})
		}
	}
	return rows
}

func bgQACloneRow(row sqliteBindingRow) sqliteBindingRow {
	if row.Record != nil {
		r := *row.Record
		row.Record = &r
	}
	return row
}

// Independent known-schema charge oracle. JSON is the legacy string boundary;
// this helper never calls the production row-charge or persistence helper.
func bgQAPersistedString(t *testing.T, raw string) string {
	t.Helper()
	b, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var saved string
	if err := json.Unmarshal(b, &saved); err != nil {
		t.Fatal(err)
	}
	return saved
}

func bgQABindingCharge(t *testing.T, row sqliteBindingRow) int64 {
	t.Helper()
	a := row.Snapshot.Attribution
	texts := []string{row.Snapshot.ID, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, row.ComputerID}
	// 32 row bytes +13 type bytes +2 INTEGER widths +7 TEXT lengths + BLOB8.
	n := int64(133)
	if row.Record != nil {
		// Third INTEGER, two TEXT length headers; NULL type bytes were already counted.
		n += 24
		texts = append(texts, row.Record.Kind, row.Record.Locator)
	}
	for _, text := range texts {
		n += int64(len(bgQAPersistedString(t, text)))
	}
	return n
}

func bgQAGenerationCharge(t *testing.T, ref ActorRef) int64 {
	t.Helper()
	// 32 row bytes +6 type bytes +5 TEXT length headers + one BLOB length/width.
	n := int64(94)
	for _, text := range []string{actorKey(ref.Key), ref.Key.ComputerID, ref.Key.Source, ref.Key.SessionID, ref.Key.AgentID} {
		n += int64(len(bgQAPersistedString(t, text)))
	}
	return n
}

// Independent audit of stored kinds/bytes, using literal table projections only.
// The metadata projection deliberately excludes its nonce/counter columns.
func bgQAStoredCharge(t *testing.T, tx *sqliteio.Tx) int64 {
	t.Helper()
	queries := []string{
		"SELECT singleton,schema_version,legacy_schema_version,computer_id,revision,sync_enabled,state_basename,database_basename,migration_id,backup_sha256 FROM store_meta",
		"SELECT " + bgQABindingColumns + " FROM bindings",
		"SELECT " + bgQAGenerationColumns + " FROM actor_generations",
		"SELECT request_id,operation,fingerprint,outcome_kind,payload FROM requests",
	}
	var total int64
	for _, query := range queries {
		s := interopPrepare(t, tx, query)
		for {
			row, err := s.Step()
			if err != nil {
				t.Fatal(err)
			}
			if !row {
				break
			}
			total += 32
			for i := 0; i < s.ColumnCount(); i++ {
				total++
				kind, err := s.Kind(i)
				if err != nil {
					t.Fatal(err)
				}
				switch kind {
				case sqliteio.NullKind:
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
					if err != nil {
						t.Fatal(err)
					}
					total += 8 + int64(len(value))
				default:
					t.Fatal("unexpected stored charge kind")
				}
			}
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return total
}

func bgQAReceiptPayload(t *testing.T, request mutationRequest) (string, string) {
	t.Helper()
	var value any
	var kind string
	switch {
	case request.BindingResult != nil:
		value, kind = request.BindingResult, "binding_result"
	case request.MutationResult != nil:
		value, kind = request.MutationResult, "mutation_result"
	default:
		t.Fatal("fixture requires an actual saved binding outcome")
	}
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return kind, string(b)
}

func bgQASeed(t *testing.T, source *state) (interopFixture, sqliteStoreMeta) {
	t.Helper()
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal(err)
	}
	m := interopMeta(f)
	m.ComputerID, m.Revision, m.SyncEnabled = source.ComputerID, source.Revision, source.SyncEnabled
	for _, row := range bgQARowsFromLegacy(source) {
		delta, err := sqliteInsertBinding(tx, row)
		want := bgQABindingCharge(t, row)
		if err != nil || delta != want {
			t.Fatalf("actual legacy binding insert delta=%d want=%d error=%v", delta, want, err)
		}
		charge, err := sqliteBindingCharge(row)
		if err != nil || charge != want {
			t.Fatalf("binding charge=%d want=%d error=%v", charge, want, err)
		}
		m.LogicalBytes += delta
	}
	for id, receipt := range source.Requests {
		kind, payload := bgQAReceiptPayload(t, receipt)
		interopDone(t, tx, "INSERT INTO requests(request_id,operation,fingerprint,outcome_kind,payload) VALUES(?,?,?,?,?)", sqliteio.Text(id), sqliteio.Text(receipt.Operation), sqliteio.Text(receipt.Fingerprint), sqliteio.Text(kind), sqliteio.Text(payload))
		m.LogicalBytes += int64(77 + len(id) + len(receipt.Operation) + len(receipt.Fingerprint) + len(kind) + len(payload))
	}
	if err := sqliteInsertMeta(tx, m); err != nil {
		t.Fatal(err)
	}
	if got := bgQAStoredCharge(t, tx); got != m.LogicalBytes {
		t.Fatalf("independent bootstrap charge=%d recorded=%d", got, m.LogicalBytes)
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	return f, m
}

// Fixture diagnostics expose the operation phase and safe typed code only.
// Never print an arbitrary error message or provider/transport contents here.
func bgQAFixtureCode(err error) string {
	if err == nil {
		return "none"
	}
	var domain *Error
	if errors.As(err, &domain) {
		return domain.Code
	}
	return "non-domain"
}

func bgQAFixtureError(t *testing.T, phase string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	var domain *Error
	if errors.As(err, &domain) {
		t.Fatalf("fixture phase=%s type=%T code=%s uncertain=%t", phase, err, domain.Code, domain.Uncertain)
	}
	t.Fatalf("fixture phase=%s type=%T domain=false", phase, err)
}

func bgQALinked(t *testing.T) (*Service, *state, sqliteBindingRow) {
	t.Helper()
	s, _ := qaLinkService(t)
	in := qaLinkInput(t)
	_, err := DiscoverLocation(context.Background(), in.Path)
	bgQAFixtureError(t, "link-location-discovery", err)
	result, err := s.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
	bgQAFixtureError(t, "link", err)
	source := bgQAReadLegacy(t, s)
	r := source.BindingRecords[result.Binding.ID]
	return s, source, sqliteBindingRow{ComputerID: source.ComputerID, Snapshot: r.Snapshot, Record: &r}
}

func bgQAAssertReceipts(t *testing.T, tx *sqliteio.Tx, source *state) {
	t.Helper()
	if got := interopCount(t, tx, "SELECT count(*) FROM requests"); got != int64(len(source.Requests)) {
		t.Fatal("historical receipt count changed")
	}
	for id, receipt := range source.Requests {
		kind, payload := bgQAReceiptPayload(t, receipt)
		s := interopPrepare(t, tx, "SELECT operation,fingerprint,outcome_kind,payload FROM requests WHERE request_id=?", sqliteio.Text(id))
		if row, err := s.Step(); !row || err != nil {
			t.Fatal("historical receipt missing", err)
		}
		for i, want := range []string{receipt.Operation, receipt.Fingerprint, kind, payload} {
			got, err := s.Text(i)
			if err != nil || got != want {
				t.Fatal("historical receipt/fingerprint/result changed", err)
			}
		}
		if row, err := s.Step(); row || err != nil {
			t.Fatal("receipt identity not singleton", err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func bgQAAssertReopen(t *testing.T, f interopFixture, meta sqliteStoreMeta, row sqliteBindingRow, source *state) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	got, found, err := sqliteReadBinding(tx, row.ComputerID, row.Snapshot.ID)
	if err != nil || !found || !reflect.DeepEqual(got, row) {
		t.Fatalf("reopened binding differs: found=%t error_type=%T error_code=%s", found, err, bgQAFixtureCode(err))
	}
	m, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(m, meta) || bgQAStoredCharge(t, tx) != meta.LogicalBytes {
		t.Fatal("reopened metadata/charge differs", err)
	}
	bgQAAssertReceipts(t, tx, source)
	interopRollback(t, tx)
	interopClose(t, c)
}

func bgQAValidation(t *testing.T, err error) {
	t.Helper()
	var domain *Error
	if !errors.As(err, &domain) || domain.Code != "validation" || domain.Uncertain {
		t.Fatalf("expected definite safe validation refusal: %v", err)
	}
}

func bgQACorrupt(t *testing.T, err error) {
	t.Helper()
	var domain *Error
	if !errors.As(err, &domain) || domain.Code != "state_corrupt" || domain.Uncertain {
		t.Fatalf("expected definite typed state_corrupt refusal: %v", err)
	}
}

func bgQAPlan(t *testing.T, tx *sqliteio.Tx, sql string, index string, values ...sqliteio.Value) {
	t.Helper()
	s := interopPrepare(t, tx, "EXPLAIN QUERY PLAN "+sql, values...)
	var details []string
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !row {
			break
		}
		text, err := s.Text(3)
		if err != nil {
			t.Fatal(err)
		}
		details = append(details, text)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(details, "\n")
	if !strings.Contains(joined, "SEARCH") || !strings.Contains(joined, index) || strings.Contains(joined, "SCAN ") {
		t.Fatalf("expected bounded key/index lookup %q, plan=%s", index, joined)
	}
}

func TestSQLiteBindingRowsRecordlessAndSavedTombstoneHistory(t *testing.T) {
	t.Run("recordless", func(t *testing.T) {
		h := qaNew(t)
		h.seed()
		source := bgQAReadLegacy(t, h.service)
		f, m := bgQASeed(t, source)
		c, tx := interopOpen(t, f, false, sqliteio.Read)
		for _, want := range bgQARowsFromLegacy(source) {
			got, found, err := sqliteReadBinding(tx, source.ComputerID, want.Snapshot.ID)
			if err != nil || !found || !reflect.DeepEqual(got, want) || got.Record != nil {
				t.Fatal("recordless snapshot changed", err)
			}
			s := interopPrepare(t, tx, "SELECT active,record_present,kind,locator,deleted FROM bindings WHERE binding_id=?", sqliteio.Text(want.Snapshot.ID))
			if row, err := s.Step(); !row || err != nil {
				t.Fatal(err)
			}
			for i, flag := range []int64{1, 0} {
				value, err := s.Int64(i)
				if err != nil || value != flag {
					t.Fatal("recordless flags changed", err)
				}
			}
			for _, i := range []int{2, 3, 4} {
				isNull, err := s.IsNull(i)
				if err != nil || !isNull {
					t.Fatal("recordless triple was invented", err)
				}
			}
			if row, err := s.Step(); row || err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if _, found, err := sqliteReadBindingLocation(tx, source.ComputerID, "directory", "/synthetic/missing"); err != nil || found {
			t.Fatal("recordless row acquired a locator", err)
		}
		if _, found, err := sqliteReadBinding(tx, source.ComputerID, "99999999-9999-4999-8999-999999999999"); err != nil || found {
			t.Fatal("missing ID was not absence", err)
		}
		if bgQAStoredCharge(t, tx) != m.LogicalBytes {
			t.Fatal("recordless independent stored charge differs")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
	t.Run("real-link-repair-unlink", func(t *testing.T) {
		s, _ := qaLinkService(t)
		in := qaLinkInput(t)
		_, err := DiscoverLocation(context.Background(), in.Path)
		bgQAFixtureError(t, "history-link-location-discovery", err)
		first, err := s.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
		bgQAFixtureError(t, "history-link", err)
		moved := in.Path + "-moved"
		if err := os.Rename(in.Path, moved); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(moved) })
		repaired, err := s.RepairBinding(context.Background(), RepairBindingInput{BindingID: first.Binding.ID, Path: moved, IfRevision: first.Binding.Revision, RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", Confirmed: true})
		bgQAFixtureError(t, "history-repair", err)
		_, err = s.Unlink(context.Background(), UnlinkInput{BindingID: repaired.Binding.ID, IfRevision: repaired.Binding.Revision, RequestID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Confirmed: true})
		bgQAFixtureError(t, "history-unlink", err)
		source := bgQAReadLegacy(t, s)
		r := source.BindingRecords[first.Binding.ID]
		want := sqliteBindingRow{ComputerID: source.ComputerID, Snapshot: r.Snapshot, Record: &r}
		if !r.Deleted || len(source.Bindings) != 0 || !reflect.DeepEqual(source.Requests[in.RequestID].BindingResult, &first) {
			t.Fatal("real legacy tombstone fixture lost original result")
		}
		f, m := bgQASeed(t, source)
		bgQAAssertReopen(t, f, m, want, source)
		c, tx := interopOpen(t, f, false, sqliteio.Read)
		if _, found, err := sqliteReadBindingLocation(tx, source.ComputerID, r.Kind, r.Locator); err != nil || found {
			t.Fatal("deleted binding appears active", err)
		}
		got, found, err := sqliteReadBinding(tx, source.ComputerID, r.Snapshot.ID)
		if err != nil || !found {
			t.Fatal(err)
		}
		got.Record.Locator, got.Record.Snapshot.Revision, got.Snapshot.Revision = "/changed-returned-copy", "99", "99"
		again, found, err := sqliteReadBinding(tx, source.ComputerID, r.Snapshot.ID)
		if err != nil || !found || !reflect.DeepEqual(again, want) {
			t.Fatal("reader aliases persisted/caller objects", err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
}

func TestSQLiteBindingRowsUpdateChargeStaleAndCallerRollback(t *testing.T) {
	_, source, initial := bgQALinked(t)
	f, m := bgQASeed(t, source)
	before := bgQACloneRow(initial)
	for i, locator := range []string{"/synthetic/" + strings.Repeat("long", 512), "/s"} {
		after := bgQACloneRow(before)
		after.Snapshot.Revision = []string{"2", "3"}[i]
		after.Record.Snapshot, after.Record.Locator = after.Snapshot, locator
		wantDelta := bgQABindingCharge(t, after) - bgQABindingCharge(t, before)
		if i == 0 && wantDelta <= 0 || i == 1 && wantDelta >= 0 {
			t.Fatal("fixture did not exercise positive and negative deltas")
		}
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		delta, err := sqliteUpdateBinding(tx, before, after)
		if err != nil || delta != wantDelta {
			t.Fatalf("update delta=%d want=%d error=%v", delta, wantDelta, err)
		}
		if i == 0 {
			// Real staged update is visible, but rollback must restore row and ledger.
			got, found, err := sqliteReadBinding(tx, after.ComputerID, after.Snapshot.ID)
			if err != nil || !found || !reflect.DeepEqual(got, after) {
				t.Fatal("update did not stage its actual row", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			bgQAAssertReopen(t, f, m, before, source)
			c, tx = interopOpen(t, f, false, sqliteio.Write)
			delta, err = sqliteUpdateBinding(tx, before, after)
			if err != nil || delta != wantDelta {
				t.Fatal("repeated caller proposal differed", err)
			}
		}
		next := metaQANext(t, m)
		next.Revision = []string{"2", "3"}[i]
		next.LogicalBytes += delta
		if err := sqliteUpdateMeta(tx, m, next); err != nil {
			t.Fatal(err)
		}
		if bgQAStoredCharge(t, tx) != next.LogicalBytes {
			t.Fatal("row delta and metadata were not composed exactly once")
		}
		interopCommit(t, tx)
		interopClose(t, c)
		bgQAAssertReopen(t, f, next, after, source)
		before, m = after, next
	}
	for _, staleField := range []string{"revision", "locator", "account", "user", "project", "task", "timezone", "kind", "deleted-active", "expected-computer", "record_presence"} {
		t.Run(staleField, func(t *testing.T) {
			stale := bgQACloneRow(before)
			switch staleField {
			case "revision":
				stale.Snapshot.Revision, stale.Record.Snapshot.Revision = "2", "2"
			case "locator":
				stale.Record.Locator = "/stale"
			case "account":
				stale.Snapshot.Attribution.AccountID = "7"
				stale.Record.Snapshot = stale.Snapshot
			case "user":
				stale.Snapshot.Attribution.UserID = "7"
				stale.Record.Snapshot = stale.Snapshot
			case "project":
				stale.Snapshot.Attribution.ProjectID = "7"
				stale.Record.Snapshot = stale.Snapshot
			case "task":
				stale.Snapshot.Attribution.TaskID = "5"
				stale.Record.Snapshot = stale.Snapshot
			case "timezone":
				stale.Snapshot.Attribution.Timezone = "Etc/UTC"
				stale.Record.Snapshot = stale.Snapshot
			case "kind":
				stale.Record.Kind = "repository"
			case "deleted-active":
				stale.Record.Deleted = true
			case "expected-computer":
				stale.ComputerID = "99999999-9999-4999-8999-999999999999"
			case "record_presence":
				stale.Record = nil
			}
			after := bgQACloneRow(stale)
			after.Snapshot.Revision = "4"
			if after.Record != nil {
				after.Record.Snapshot = after.Snapshot
			}
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			delta, err := sqliteUpdateBinding(tx, stale, after)
			interopSafeError(t, err, stale.Snapshot.ID, "/stale", f.directory)
			if delta != 0 {
				t.Fatal("failed update returned a usable delta")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			bgQAAssertReopen(t, f, m, before, source)
		})
	}
	for _, rewrite := range []string{"id", "computer"} {
		t.Run("identity-rewrite/"+rewrite, func(t *testing.T) {
			// The exact before row exists. A valid mutable proposal succeeds in a
			// rollback control, isolating the single forbidden identity rewrite.
			validAfter := bgQACloneRow(before)
			validAfter.Snapshot.Revision, validAfter.Record.Snapshot.Revision = "4", "4"
			validAfter.Record.Locator = "/valid-identity-control"
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if delta, err := sqliteUpdateBinding(tx, before, validAfter); err != nil || delta != bgQABindingCharge(t, validAfter)-bgQABindingCharge(t, before) {
				t.Fatal("otherwise-valid update control refused", err)
			}
			if got, found, err := sqliteReadBinding(tx, validAfter.ComputerID, validAfter.Snapshot.ID); err != nil || !found || !reflect.DeepEqual(got, validAfter) {
				t.Fatal("otherwise-valid update control did not stage its row", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			bgQAAssertReopen(t, f, m, before, source)
			after := bgQACloneRow(validAfter)
			if rewrite == "id" {
				after.Snapshot.ID, after.Record.Snapshot.ID = qaBindingA, qaBindingA
			} else {
				after.ComputerID = "99999999-9999-4999-8999-999999999999"
			}
			c, tx = interopOpen(t, f, false, sqliteio.Write)
			delta, err := sqliteUpdateBinding(tx, before, after)
			bgQAValidation(t, err)
			interopSafeError(t, err, after.Snapshot.ID, after.ComputerID, f.directory)
			if delta != 0 {
				t.Fatal("identity rewrite returned usable delta")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			bgQAAssertReopen(t, f, m, before, source)
		})
	}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	delta, err := sqliteInsertBinding(tx, initial)
	interopSafeError(t, err, initial.Snapshot.ID, f.directory)
	if delta != 0 {
		t.Fatal("duplicate insert returned a usable delta")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	bgQAAssertReopen(t, f, m, before, source)
}

func TestSQLiteBindingRowsExactLocationIndexAndNormalizationCollision(t *testing.T) {
	_, source, row := bgQALinked(t)
	row.Record.Locator = "/synthetic/" + "\ufffd"
	// Actual link result/fingerprint remains historical; only the fixture's current
	// locator changes to a valid stored value before the shared row API is seeded.
	source.BindingRecords[row.Snapshot.ID] = *row.Record
	if !validState(source) {
		t.Fatal("canonical collision fixture is not a valid legacy state")
	}
	f, m := bgQASeed(t, source)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	got, found, err := sqliteReadBindingLocation(tx, row.ComputerID, row.Record.Kind, row.Record.Locator)
	if err != nil || !found || !reflect.DeepEqual(got, row) {
		t.Fatal("exact canonical location lookup differs", err)
	}
	rawLocator := "/synthetic/" + string([]byte{0xff})
	if _, found, err := sqliteReadBindingLocation(tx, row.ComputerID, row.Record.Kind, rawLocator); err != nil || found {
		t.Fatal("raw lookup was normalized before original identity decisions", err)
	}
	bgQAPlan(t, tx, "SELECT "+bgQABindingColumns+" FROM bindings WHERE kind=? AND locator=? AND record_present=1 AND deleted=0", "binding_active_locator", sqliteio.Text(row.Record.Kind), sqliteio.Text(row.Record.Locator))
	bgQAPlan(t, tx, "SELECT "+bgQABindingColumns+" FROM bindings WHERE binding_id=?", "sqlite_autoindex_bindings_1", sqliteio.Text(row.Snapshot.ID))
	interopRollback(t, tx)
	interopClose(t, c)
	for _, locator := range []string{row.Record.Locator, rawLocator} {
		t.Run(map[bool]string{true: "raw-normalization-collision", false: "already-canonical-collision"}[locator == rawLocator], func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			staged := bgQACloneRow(row)
			staged.Snapshot.ID, staged.Record.Snapshot.ID = qaBindingB, qaBindingB
			staged.Record.Locator = "/staged-then-aborted"
			if _, err := sqliteInsertBinding(tx, staged); err != nil {
				t.Fatal("unrelated fixture proposal did not reach SQLite", err)
			}
			collision := bgQACloneRow(row)
			collision.Snapshot.ID, collision.Record.Snapshot.ID = qaBindingA, qaBindingA
			collision.Record.Locator = locator
			original := bgQACloneRow(collision)
			delta, err := sqliteInsertBinding(tx, collision)
			bgQAValidation(t, err)
			interopSafeError(t, err, locator, collision.Snapshot.ID, f.directory)
			if delta != 0 || !reflect.DeepEqual(collision, original) {
				t.Fatal("collision returned charge or mutated caller identity")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			bgQAAssertReopen(t, f, m, row, source)
			c, tx = interopOpen(t, f, false, sqliteio.Read)
			if interopCount(t, tx, "SELECT count(*) FROM bindings") != 1 {
				t.Fatal("caller rollback retained proposed effects")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}

// The real legacy file reader remains the strict oracle, including field/UTF-8/
// graph validation. Only a test-owned state file is replaced; its lock stays put.
func bgQALegacyMarshalOracle(t *testing.T, service *Service, path string, raw *state, wantValid bool) *state {
	t.Helper()
	if !validState(raw) {
		t.Fatal("fixture did not pass actual raw legacy validator")
	}
	b, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var decoded state
	if err := json.Unmarshal(b, &decoded); err != nil || !strictJSON(b) || !exactJSONFields(b, reflect.TypeOf(state{})) {
		t.Fatal("actual legacy serialized JSON/field shape rejected", err)
	}
	if validState(&decoded) != wantValid {
		t.Fatalf("legacy decoded validity=%t want=%t", validState(&decoded), wantValid)
	}
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	read, exists, err := service.store.read(context.Background())
	if wantValid {
		if err != nil || !exists || !reflect.DeepEqual(read, &decoded) {
			t.Fatal("actual strict legacy read differs from JSON oracle", err)
		}
	} else {
		qaCode(t, err, "state_corrupt")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, b) {
		t.Fatal("legacy read rewrote preserved serialized evidence", err)
	}
	return &decoded
}

func TestSQLiteBindingRowsLegacyJSONRepairAndFinalLocatorLimit(t *testing.T) {
	for _, tc := range []struct {
		name           string
		locator        string
		validPersisted bool
	}{
		{"invalid-bytes-repaired", "/raw/" + string([]byte{0xff, 0xfe}), true},
		{"exact-4096-valid-bytes", "/" + strings.Repeat("x", 4095), true},
		{"invalid-expands-within-limit", "/" + strings.Repeat(string([]byte{0xff}), 1365), true},
		{"1366-invalid-bytes-exceed-final-limit", "/" + strings.Repeat(string([]byte{0xff}), 1366), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := qaNew(t)
			h.seed()
			raw := bgQAReadLegacy(t, h.service)
			snapshot := raw.Bindings[qaBindingA]
			raw.BindingRecords = map[string]bindingRecord{snapshot.ID: {Snapshot: snapshot, Kind: "directory", Locator: tc.locator}}
			if !validStoredLocation("directory", tc.locator) {
				t.Fatal("raw locator did not pass actual pre-serialization validation")
			}
			decoded := bgQALegacyMarshalOracle(t, h.service, h.path, raw, tc.validPersisted)
			persisted := decoded.BindingRecords[snapshot.ID].Locator
			if tc.name == "1366-invalid-bytes-exceed-final-limit" && (len(tc.locator) != 1367 || len(persisted) != 4099) {
				t.Fatal("length-expansion edge did not hit required byte boundary")
			}
			_, source, baseline := bgQALinked(t)
			f, m := bgQASeed(t, source)
			proposal := bgQACloneRow(baseline)
			proposal.Snapshot.ID, proposal.Record.Snapshot.ID = snapshot.ID, snapshot.ID
			proposal.Record.Locator = tc.locator
			original := bgQACloneRow(proposal)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			delta, err := sqliteInsertBinding(tx, proposal)
			if !tc.validPersisted {
				bgQAValidation(t, err)
				interopSafeError(t, err, tc.locator, f.directory)
				if delta != 0 || !reflect.DeepEqual(proposal, original) {
					t.Fatal("final invalid locator returned delta or changed caller")
				}
				interopRollback(t, tx)
				interopClose(t, c)
				bgQAAssertReopen(t, f, m, baseline, source)
				return
			}
			want := bgQABindingCharge(t, proposal)
			if err != nil || delta != want || !reflect.DeepEqual(proposal, original) {
				t.Fatalf("legacy repair insert delta=%d want=%d error=%v", delta, want, err)
			}
			next := metaQANext(t, m)
			next.Revision, next.LogicalBytes = "2", m.LogicalBytes+delta
			if err := sqliteUpdateMeta(tx, m, next); err != nil {
				t.Fatal(err)
			}
			interopCommit(t, tx)
			interopClose(t, c)
			wantRow := bgQACloneRow(proposal)
			wantRow.Record.Locator = persisted
			bgQAAssertReopen(t, f, next, wantRow, source)
			// This accepted strict-decoded identity is also valid import input;
			// no second replacement or NEW collision rule changes its meaning.
			c, tx = interopOpen(t, f, false, sqliteio.Read)
			if _, found, err := sqliteReadBindingLocation(tx, baseline.ComputerID, "directory", tc.locator); err != nil || found != (tc.locator == persisted) {
				t.Fatal("lookup arguments were repaired before query", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}

func TestSQLiteGenerationRowsHistoricalIdentityUnsignedOrderAndReuse(t *testing.T) {
	_, source, binding := bgQALinked(t)
	f, m := bgQASeed(t, source)
	key := ActorKey{ComputerID: "99999999-9999-4999-8999-999999999999", Source: "manual-test", SessionID: "quotes\" and backslash\\ and 雪", AgentID: "parent"}
	want := []string{"1", "2", "10", "9223372036854775807", "9223372036854775808", "18446744073709551615"}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	var sum int64
	for i := len(want) - 1; i >= 0; i-- {
		ref := ActorRef{Key: key, Generation: want[i]}
		delta, err := sqliteEnsureActorGeneration(tx, ref)
		charge := bgQAGenerationCharge(t, ref)
		if err != nil || delta != charge {
			t.Fatalf("generation insert delta=%d want=%d error=%v", delta, charge, err)
		}
		gotCharge, err := sqliteGenerationCharge(ref)
		if err != nil || gotCharge != charge {
			t.Fatal("generation charge differs from independent byte oracle", err)
		}
		sum += delta
		if delta, err := sqliteEnsureActorGeneration(tx, ref); err != nil || delta != 0 {
			t.Fatal("identity-only exact reuse added another row/charge", err)
		}
	}
	next := metaQANext(t, m)
	next.Revision, next.LogicalBytes = "2", m.LogicalBytes+sum
	if err := sqliteUpdateMeta(tx, m, next); err != nil {
		t.Fatal(err)
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	bgQAAssertReopen(t, f, next, binding, source)
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	for _, generation := range want {
		ref := ActorRef{Key: key, Generation: generation}
		got, found, err := sqliteReadActorGeneration(tx, ref)
		if err != nil || !found || got != ref {
			t.Fatal("historical foreign-parent identity changed", err)
		}
	}
	if _, found, err := sqliteReadActorGeneration(tx, ActorRef{Key: key, Generation: "3"}); err != nil || found {
		t.Fatal("missing generation did not return absence", err)
	}
	if interopCount(t, tx, "SELECT count(*) FROM actors") != 0 || interopCount(t, tx, "SELECT count(*) FROM actor_generations") != int64(len(want)) {
		t.Fatal("generation transport invented Actor head/history")
	}
	s := interopPrepare(t, tx, "SELECT generation FROM actor_generations WHERE actor_key=? ORDER BY generation", sqliteio.Text(actorKey(key)))
	var ordered []string
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !row {
			break
		}
		b, err := s.Blob(0)
		if err != nil || len(b) != 8 {
			t.Fatal("generation is not exact BLOB8", err)
		}
		generation, err := sqliteDecodeUint64(b)
		if err != nil {
			t.Fatal(err)
		}
		ordered = append(ordered, generation)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ordered, want) {
		t.Fatal("unsigned generation ordering truncated or sorted text")
	}
	s = interopPrepare(t, tx, "SELECT count(*) FROM actor_generations WHERE actor_key=? AND generation>?", sqliteio.Text(actorKey(key)), interopCounter(t, "9223372036854775807"))
	if row, err := s.Step(); !row || err != nil {
		t.Fatal(err)
	}
	n, err := s.Int64(0)
	if err != nil || n != 2 {
		t.Fatal("unsigned range omitted values above signed64", err)
	}
	if row, err := s.Step(); row || err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	bgQAPlan(t, tx, "SELECT "+bgQAGenerationColumns+" FROM actor_generations WHERE actor_key=? AND generation=?", "sqlite_autoindex_actor_generations_1", sqliteio.Text(actorKey(key)), interopCounter(t, "1"))
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteGenerationRowsLegacyRawActorKeyDriftRefusesBeforeCommit(t *testing.T) {
	h := qaNew(t)
	h.seed()
	if _, err := h.service.Ingest(context.Background(), qaEvent("original", "1", "1", "work", qaBindingA)); err != nil {
		t.Fatal(err)
	}
	raw := bgQAReadLegacy(t, h.service)
	var actor *Actor
	for _, a := range raw.Actors {
		actor = a
	}
	if actor == nil {
		t.Fatal("real reducer fixture lacks Actor")
	}
	actor.Ref.Key.SessionID = "raw-session-" + string([]byte{0xff})
	raw.Actors = map[string]*Actor{actorKey(actor.Ref.Key): actor}
	for _, segment := range raw.Segments {
		segment.Actor = actor.Ref
	}
	for key, receipt := range raw.Receipts {
		receipt.Result.Actor = actor.Ref
		raw.Receipts[key] = receipt
	}
	rawKey := actorKey(actor.Ref.Key)
	// Derive the persistence projection with this exact native JSON encoder.
	// Go 1.27 JSONv2 replaces invalid bytes with literal RuneError; the older
	// encoder uses a different escape spelling and can produce map-key drift.
	encodedKey, err := json.Marshal(actor.Ref.Key)
	if err != nil {
		t.Fatal(err)
	}
	var repairedKey ActorKey
	if err := json.Unmarshal(encodedKey, &repairedKey); err != nil {
		t.Fatal(err)
	}
	drift := rawKey != actorKey(repairedKey)
	decoded := bgQALegacyMarshalOracle(t, h.service, h.path, raw, !drift)
	var decodedActor *Actor
	for _, a := range decoded.Actors {
		decodedActor = a
	}
	if decodedActor == nil || decodedActor.Ref.Key != repairedKey || len(decoded.Actors) != 1 {
		t.Fatal("actual JSON actor projection or map count differs from typed oracle")
	}
	for storedKey := range decoded.Actors {
		if storedKey != rawKey || (storedKey != actorKey(decodedActor.Ref.Key)) != drift {
			t.Fatal("actual JSON map-key/decoded-identity invariant differs from derived branch")
		}
	}
	if !drift {
		bgQAStableRawActorIdentity(t, actor.Ref, decodedActor.Ref)
		return
	}
	_, source, binding := bgQALinked(t)
	f, m := bgQASeed(t, source)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	original := actor.Ref
	// No equivalent identity exists: refusal cannot be explained by uniqueness
	// or an existing-row projection check. The new raw encoding itself is unsafe.
	delta, err := sqliteEnsureActorGeneration(tx, original)
	bgQAValidation(t, err)
	interopSafeError(t, err, original.Key.SessionID, rawKey, f.directory)
	if delta != 0 || actor.Ref != original || interopCount(t, tx, "SELECT count(*) FROM actor_generations") != 0 {
		t.Fatal("fresh raw identity refusal mutated caller or staged a row/charge")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	bgQAAssertReopen(t, f, m, binding, source)
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	canonical := decodedActor.Ref
	// Canonical strict-valid decoded reference is a legitimate identity input.
	delta, err = sqliteEnsureActorGeneration(tx, canonical)
	if err != nil || delta != bgQAGenerationCharge(t, canonical) {
		t.Fatal("canonical identity refused", err)
	}
	next := metaQANext(t, m)
	next.Revision, next.LogicalBytes = "2", m.LogicalBytes+delta
	if err := sqliteUpdateMeta(tx, m, next); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	delta, err = sqliteEnsureActorGeneration(tx, original)
	bgQAValidation(t, err)
	interopSafeError(t, err, original.Key.SessionID, rawKey, f.directory)
	if delta != 0 || actor.Ref != original {
		t.Fatal("raw identity refusal altered caller or returned charge")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	bgQAAssertReopen(t, f, next, binding, source)
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	got, found, err := sqliteReadActorGeneration(tx, canonical)
	if err != nil || !found || got != canonical || interopCount(t, tx, "SELECT count(*) FROM actor_generations") != 1 {
		t.Fatal("raw refusal lost or duplicated prior canonical identity", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

// The stable-key branch is reached only when the real legacy encoder preserves
// the packed identity key while repairing raw text. Public legacy replay/lookup
// establishes the expected canonical result before exercising the SQL helpers.
func bgQAStableRawActorIdentity(t *testing.T, original, canonical ActorRef) {
	t.Helper()
	if original == canonical || actorKey(original.Key) != actorKey(canonical.Key) {
		t.Fatal("stable-key fixture lacks a real persistence text repair")
	}
	h := qaNew(t)
	h.seed()
	e := qaEvent("original", "1", "1", "work", qaBindingA)
	e.Actor = original.Key
	first, err := h.service.Ingest(context.Background(), e)
	bgQAFixtureError(t, "legacy-raw-ingest", err)
	if first.Disposition != "applied" || first.Actor != original {
		t.Fatal("legacy first ingress did not return its original raw actor")
	}
	persisted := bgQAReadLegacy(t, h.service)
	storedActor := persisted.Actors[actorKey(original.Key)]
	if storedActor == nil || storedActor.Ref != canonical || len(persisted.Actors) != 1 {
		t.Fatal("legacy raw packed-key lookup did not retain one canonical actor")
	}
	beforeReplay, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []ActorKey{original.Key, canonical.Key} {
		replay := e
		replay.Actor = key
		result, err := h.service.Ingest(context.Background(), replay)
		bgQAFixtureError(t, "legacy-exact-replay", err)
		if result.Disposition != "duplicate" || result.Actor != canonical {
			t.Fatal("legacy replay did not return saved canonical actor identity")
		}
		afterReplay, err := os.ReadFile(h.path)
		if err != nil || !bytes.Equal(beforeReplay, afterReplay) {
			t.Fatal("legacy exact replay rewrote historical state")
		}
	}
	nextEvent := qaEvent("original", "1", "2", "observe_work", "")
	nextEvent.Actor = original.Key
	h.at(1)
	result, err := h.service.Ingest(context.Background(), nextEvent)
	bgQAFixtureError(t, "legacy-raw-existing-actor-lookup", err)
	if result.Disposition != "applied" {
		t.Fatal("legacy raw key failed existing-actor sequence lookup")
	}
	after := bgQAReadLegacy(t, h.service)
	if len(after.Actors) != 1 || after.Actors[actorKey(original.Key)] == nil || after.Actors[actorKey(original.Key)].Ref != canonical || after.Actors[actorKey(original.Key)].Sequence != "2" {
		t.Fatal("legacy raw existing-actor lookup duplicated or changed canonical identity")
	}
	if e.Actor != original.Key || nextEvent.Actor != original.Key {
		t.Fatal("legacy ingress mutated caller-owned event identity")
	}

	_, source, binding := bgQALinked(t)
	f, m := bgQASeed(t, source)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	savedOriginal := original
	delta, err := sqliteEnsureActorGeneration(tx, original)
	if err != nil || delta != bgQAGenerationCharge(t, canonical) || original != savedOriginal {
		t.Fatal("stable-key first ensure failed canonical storage/charge or changed caller", err)
	}
	// Exercise both first-transaction lookup and reuse before any reopen.
	got, found, err := sqliteReadActorGeneration(tx, canonical)
	if err != nil || !found || got != canonical {
		t.Fatal("fresh stored canonical identity cannot be read", err)
	}
	got, found, err = sqliteReadActorGeneration(tx, original)
	if err != nil || !found || got != canonical {
		t.Errorf("fresh stable packed-key raw read must return canonical identity: found=%t error_type=%T error_code=%s", found, err, bgQAFixtureCode(err))
	}
	repeatDelta, repeatErr := sqliteEnsureActorGeneration(tx, original)
	if repeatErr != nil || repeatDelta != 0 {
		t.Errorf("fresh stable packed-key raw ensure must reuse canonical identity: delta=%d error_type=%T error_code=%s", repeatDelta, repeatErr, bgQAFixtureCode(repeatErr))
	}
	next := metaQANext(t, m)
	next.Revision, next.LogicalBytes = "2", m.LogicalBytes+delta
	if err := sqliteUpdateMeta(tx, m, next); err != nil {
		t.Fatal(err)
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	bgQAAssertReopen(t, f, next, binding, source)
	for _, which := range []string{"canonical-read", "raw-read", "raw-repeat-ensure"} {
		t.Run(which, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if which == "raw-repeat-ensure" {
				delta, err := sqliteEnsureActorGeneration(tx, original)
				if err != nil || delta != 0 {
					t.Errorf("stable packed-key repeated raw ensure must reuse canonical identity with zero charge: delta=%d error_type=%T error_code=%s", delta, err, bgQAFixtureCode(err))
				}
			} else {
				query := original
				if which == "canonical-read" {
					query = canonical
				}
				got, found, err := sqliteReadActorGeneration(tx, query)
				if err != nil || !found || got != canonical {
					t.Errorf("stable packed-key %s must return stored canonical identity: found=%t error_type=%T error_code=%s", which, found, err, bgQAFixtureCode(err))
				}
			}
			if original != savedOriginal || interopCount(t, tx, "SELECT count(*) FROM actor_generations") != 1 || interopCount(t, tx, "SELECT count(*) FROM actors") != 0 || bgQAStoredCharge(t, tx) != next.LogicalBytes {
				t.Fatal("stable-key repeat changed caller, identity-only row count or recorded charge")
			}
			bgQAAssertReceipts(t, tx, source)
			interopRollback(t, tx)
			interopClose(t, c)
			bgQAAssertReopen(t, f, next, binding, source)
		})
	}
}

func TestSQLiteBindingGenerationInputsRefuseWithoutPartialRows(t *testing.T) {
	_, source, baseline := bgQALinked(t)
	f, m := bgQASeed(t, source)
	for _, which := range []string{"zero-revision", "noncanonical-revision", "invalid-id", "invalid-computer", "record-snapshot-mismatch", "empty-record-locator", "relative-locator", "overlong-locator", "nul-locator", "invalid-kind"} {
		t.Run("binding/"+which, func(t *testing.T) {
			row := bgQACloneRow(baseline)
			row.Snapshot.ID, row.Record.Snapshot.ID = qaBindingA, qaBindingA
			row.Record.Locator = "/fresh-valid-candidate/" + which
			// First prove this otherwise-valid candidate succeeds with the current
			// API and does not collide with the existing active locator.
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if delta, err := sqliteInsertBinding(tx, row); err != nil || delta != bgQABindingCharge(t, row) {
				t.Fatal("otherwise-valid fresh insert control refused", err)
			}
			if got, found, err := sqliteReadBinding(tx, row.ComputerID, row.Snapshot.ID); err != nil || !found || !reflect.DeepEqual(got, row) {
				t.Fatal("otherwise-valid fresh insert control did not stage its row", err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			bgQAAssertReopen(t, f, m, baseline, source)
			switch which {
			case "zero-revision":
				row.Snapshot.Revision, row.Record.Snapshot.Revision = "0", "0"
			case "noncanonical-revision":
				row.Snapshot.Revision, row.Record.Snapshot.Revision = "01", "01"
			case "invalid-id":
				row.Snapshot.ID, row.Record.Snapshot.ID = "private-invalid-id", "private-invalid-id"
			case "invalid-computer":
				row.ComputerID = "private-invalid-computer"
			case "record-snapshot-mismatch":
				row.Record.Snapshot.Attribution.TaskID = "5"
			case "empty-record-locator":
				row.Record.Locator = ""
			case "relative-locator":
				row.Record.Locator = "relative/private-locator"
			case "overlong-locator":
				row.Record.Locator = "/" + strings.Repeat("x", 4096)
			case "nul-locator":
				row.Record.Locator = "/private\x00locator"
			case "invalid-kind":
				row.Record.Kind = "private-invalid-kind"
			}
			original := bgQACloneRow(row)
			c, tx = interopOpen(t, f, false, sqliteio.Write)
			delta, err := sqliteInsertBinding(tx, row)
			bgQAValidation(t, err)
			interopSafeError(t, err, row.Record.Locator, row.Snapshot.ID, f.directory)
			if delta != 0 || !reflect.DeepEqual(row, original) {
				t.Fatal("invalid row returned delta or mutated caller")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			bgQAAssertReopen(t, f, m, baseline, source)
		})
	}
	for _, which := range []string{"zero", "leading-zero", "overflow", "invalid-source", "empty-session", "nul-agent", "invalid-computer"} {
		t.Run("generation/"+which, func(t *testing.T) {
			ref := ActorRef{Key: ActorKey{ComputerID: source.ComputerID, Source: "codex", SessionID: "history", AgentID: "agent"}, Generation: "1"}
			switch which {
			case "zero":
				ref.Generation = "0"
			case "leading-zero":
				ref.Generation = "01"
			case "overflow":
				ref.Generation = "18446744073709551616"
			case "invalid-source":
				ref.Key.Source = "private-invalid-source"
			case "empty-session":
				ref.Key.SessionID = ""
			case "nul-agent":
				ref.Key.AgentID = "agent\x00private"
			case "invalid-computer":
				ref.Key.ComputerID = "private-invalid-computer"
			}
			original := ref
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			delta, err := sqliteEnsureActorGeneration(tx, ref)
			interopSafeError(t, err, ref.Key.SessionID, ref.Key.AgentID, f.directory)
			if delta != 0 || ref != original {
				t.Fatal("invalid generation returned delta or mutated caller")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			bgQAAssertReopen(t, f, m, baseline, source)
		})
	}
}

func TestSQLiteBindingRowsLegacyUUIDAndCallerOwnedSameRevision(t *testing.T) {
	_, source, baseline := bgQALinked(t)
	f, m := bgQASeed(t, source)
	const nonV4 = "01234567-89ab-cdef-0123-456789abcdef"
	if !validUUID(nonV4) {
		t.Fatal("fixture did not hit the legacy lowercase non-v4 UUID domain")
	}
	row := bgQACloneRow(baseline)
	row.Snapshot.ID, row.Record.Snapshot.ID = nonV4, nonV4
	row.Record.Locator = "/non-v4/one"
	ref := ActorRef{Key: ActorKey{ComputerID: nonV4, Source: "manual-test", SessionID: "historical", AgentID: "agent"}, Generation: "1"}
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	rowDelta, err := sqliteInsertBinding(tx, row)
	if err != nil || rowDelta != bgQABindingCharge(t, row) {
		t.Fatal("legacy non-v4 binding UUID refused", err)
	}
	refDelta, err := sqliteEnsureActorGeneration(tx, ref)
	if err != nil || refDelta != bgQAGenerationCharge(t, ref) {
		t.Fatal("legacy non-v4 historical computer UUID refused", err)
	}
	next := metaQANext(t, m)
	next.Revision, next.LogicalBytes = "2", m.LogicalBytes+rowDelta+refDelta
	if err := sqliteUpdateMeta(tx, m, next); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	bgQAAssertReopen(t, f, next, row, source)
	after := bgQACloneRow(row)
	after.Record.Locator = "/non-v4/two" // Same byte length; entity revision remains exactly1.
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	delta, err := sqliteUpdateBinding(tx, row, after)
	if err != nil || delta != 0 {
		t.Fatal("row helper imposed entity revision policy or changed fixed charge", err)
	}
	// The composing caller still spends its one semantic public revision and
	// nonce; this private helper witness does not authorize a public API bypass.
	last := metaQANext(t, next)
	last.Revision = "3"
	if err := sqliteUpdateMeta(tx, next, last); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	bgQAAssertReopen(t, f, last, after, source)
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	got, found, err := sqliteReadActorGeneration(tx, ref)
	if err != nil || !found || got != ref || interopCount(t, tx, "SELECT count(*) FROM actors") != 0 {
		t.Fatal("non-v4 identity changed or invented a current Actor", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteBindingGenerationTypedReadsRejectStoredDisagreement(t *testing.T) {
	// Real schema constraints prevent some deliberately malformed kinds/groups.
	// For those cases only, install the actual schema first then shadow one table
	// with a relaxed decoder fixture in this caller-owned transaction. This is
	// decoder refusal QA, never evidence that the real STRICT schema admits them.
	_, source, row := bgQALinked(t)
	for _, which := range []string{"computer", "attribution", "empty-locator", "revision-kind", "revision-width", "revision-zero", "partial-record-null", "active-flag", "extra-row"} {
		t.Run("binding/"+which, func(t *testing.T) {
			f := interopLocation(t)
			c, tx := interopOpen(t, f, true, sqliteio.Write)
			if err := sqliteCreateSchema(tx); err != nil {
				t.Fatal(err)
			}
			relaxed := which != "computer" && which != "attribution" && which != "empty-locator"
			if relaxed {
				interopDone(t, tx, "ALTER TABLE bindings RENAME TO qa_original_bindings")
				interopDone(t, tx, "CREATE TABLE bindings(binding_id,revision,account_id,user_id,project_id,task_id,timezone,computer_id,active,record_present,kind,locator,deleted)")
			}
			if _, err := sqliteInsertBinding(tx, row); err != nil {
				t.Fatal(err)
			}
			if got, found, err := sqliteReadBinding(tx, row.ComputerID, row.Snapshot.ID); err != nil || !found || !reflect.DeepEqual(got, row) {
				t.Fatal("decoder fixture baseline invalid", err)
			}
			switch which {
			case "computer":
				interopDone(t, tx, "UPDATE bindings SET computer_id=?", sqliteio.Text("99999999-9999-4999-8999-999999999999"))
			case "attribution":
				interopDone(t, tx, "UPDATE bindings SET account_id='invalid-account'")
			case "empty-locator":
				interopDone(t, tx, "UPDATE bindings SET locator=''")
			case "revision-kind":
				interopDone(t, tx, "UPDATE bindings SET revision='00000001'")
			case "revision-width":
				interopDone(t, tx, "UPDATE bindings SET revision=X'01'")
			case "revision-zero":
				interopDone(t, tx, "UPDATE bindings SET revision=zeroblob(8)")
			case "partial-record-null":
				interopDone(t, tx, "UPDATE bindings SET kind=NULL")
			case "active-flag":
				interopDone(t, tx, "UPDATE bindings SET active=2")
			case "extra-row":
				interopDone(t, tx, "INSERT INTO bindings("+bgQABindingColumns+") SELECT "+bgQABindingColumns+" FROM bindings")
			}
			got, found, err := sqliteReadBinding(tx, row.ComputerID, row.Snapshot.ID)
			bgQACorrupt(t, err)
			interopSafeError(t, err, row.Record.Locator, row.Snapshot.ID, f.directory)
			if found || !reflect.DeepEqual(got, sqliteBindingRow{}) {
				t.Fatal("corrupt read returned a usable/partial binding")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	ref := ActorRef{Key: ActorKey{ComputerID: source.ComputerID, Source: "codex", SessionID: "native-session", AgentID: "native-agent"}, Generation: "1"}
	for _, which := range []string{"key-projection", "field-projection", "generation-kind", "generation-width", "generation-zero", "extra-row"} {
		t.Run("generation/"+which, func(t *testing.T) {
			f := interopLocation(t)
			c, tx := interopOpen(t, f, true, sqliteio.Write)
			if err := sqliteCreateSchema(tx); err != nil {
				t.Fatal(err)
			}
			if which != "key-projection" && which != "field-projection" {
				interopDone(t, tx, "ALTER TABLE actor_generations RENAME TO qa_original_generations")
				interopDone(t, tx, "CREATE TABLE actor_generations(actor_key,generation,computer_id,source,session_id,agent_id)")
			}
			if _, err := sqliteEnsureActorGeneration(tx, ref); err != nil {
				t.Fatal(err)
			}
			queryRef := ref
			switch which {
			case "key-projection":
				// Query a valid alternate key that deliberately carries the old fields.
				queryRef.Key.AgentID = "other-agent"
				interopDone(t, tx, "UPDATE actor_generations SET actor_key=?", sqliteio.Text(actorKey(queryRef.Key)))
			case "field-projection":
				interopDone(t, tx, "UPDATE actor_generations SET session_id='different-session'")
			case "generation-kind":
				interopDone(t, tx, "UPDATE actor_generations SET generation='00000001'")
			case "generation-width":
				interopDone(t, tx, "UPDATE actor_generations SET generation=X'01'")
			case "generation-zero":
				interopDone(t, tx, "UPDATE actor_generations SET generation=zeroblob(8)")
			case "extra-row":
				interopDone(t, tx, "INSERT INTO actor_generations("+bgQAGenerationColumns+") SELECT "+bgQAGenerationColumns+" FROM actor_generations")
			}
			if which == "generation-kind" || which == "generation-width" || which == "generation-zero" {
				// An exact key+BLOB lookup correctly reports absence when its stored
				// generation differs. The actual malformed row remains preserved;
				// no consumer may infer it is an existing valid generation.
				if got, found, err := sqliteReadActorGeneration(tx, queryRef); err != nil || found || got != (ActorRef{}) {
					t.Fatal("nonmatching malformed stored generation was coerced into identity", err)
				}
				if interopCount(t, tx, "SELECT count(*) FROM actor_generations") != 1 {
					t.Fatal("nonmatching malformed row was repaired/removed")
				}
			} else {
				got, found, err := sqliteReadActorGeneration(tx, queryRef)
				bgQACorrupt(t, err)
				interopSafeError(t, err, queryRef.Key.SessionID, actorKey(queryRef.Key), f.directory)
				if found || got != (ActorRef{}) {
					t.Fatal("corrupt read returned usable/partial generation")
				}
				if which != "extra-row" {
					delta, err := sqliteEnsureActorGeneration(tx, queryRef)
					bgQACorrupt(t, err)
					interopSafeError(t, err, queryRef.Key.SessionID, f.directory)
					if delta != 0 {
						t.Fatal("disagreeing identity was reused or replaced")
					}
				}
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}

func TestSQLiteBindingGenerationCallerContextCancellationPreservesRows(t *testing.T) {
	_, source, row := bgQALinked(t)
	f, m := bgQASeed(t, source)
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
	after := bgQACloneRow(row)
	after.Snapshot.Revision, after.Record.Snapshot.Revision = "2", "2"
	after.Record.Locator = "/never-admitted-after-cancel"
	cancel()
	delta, err := sqliteUpdateBinding(tx, row, after)
	if delta != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation not preserved: delta=%d error_type=%T error_code=%s", delta, err, bgQAFixtureCode(err))
	}
	interopSafeError(t, err, after.Record.Locator, f.directory)
	// The adapter owns cancellation/rollback cleanup; an already terminal abort
	// may retain cancellation evidence, but it cannot turn this into a commit.
	if cleanup := tx.Rollback(); cleanup != nil && !errors.Is(cleanup, context.Canceled) {
		interopSafeError(t, cleanup, f.directory)
	}
	interopClose(t, c)
	bgQAAssertReopen(t, f, m, row, source)
}
