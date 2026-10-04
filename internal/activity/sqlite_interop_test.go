//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Source-first tests of the inactive embedded schema and typed transport.
// Direct fixture writes are not acceptance of an importer, reducer or global accounting.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

const interopComputer = "00000000-0000-4000-8000-000000000001"

type interopFixture struct{ directory, authority, database string }

func interopLocation(t *testing.T) interopFixture {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	d, a, b, err := sqliteLocation(filepath.Join(dir, "activity-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	return interopFixture{d, a, b}
}

func interopOpen(t *testing.T, f interopFixture, create bool, mode sqliteio.Mode) (*sqliteio.Conn, *sqliteio.Tx) {
	t.Helper()
	c, err := sqliteio.Open(context.Background(), f.directory, f.database, sqliteio.Options{Create: create, AcquireDeadline: time.Now().Add(max(250*time.Millisecond, sqliteFlowTestLockTimeout()))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(context.Background()); err != nil {
			t.Errorf("fixture close: %v", err)
		}
	})
	tx, err := c.Begin(context.Background(), mode)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return c, tx
}

func interopClose(t *testing.T, c *sqliteio.Conn) {
	t.Helper()
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func interopRollback(t *testing.T, tx *sqliteio.Tx) {
	t.Helper()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
func interopCommit(t *testing.T, tx *sqliteio.Tx) {
	t.Helper()
	outcome, err := tx.Commit()
	if err != nil || outcome != sqliteio.Committed {
		t.Fatalf("caller commit outcome=%v error=%v", outcome, err)
	}
}
func interopPrepare(t *testing.T, tx *sqliteio.Tx, sql string, values ...sqliteio.Value) *sqliteio.Stmt {
	t.Helper()
	s, err := tx.Prepare(sql, values...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func interopDone(t *testing.T, tx *sqliteio.Tx, sql string, values ...sqliteio.Value) {
	t.Helper()
	s := interopPrepare(t, tx, sql, values...)
	row, err := s.Step()
	closeErr := s.Close()
	if row || err != nil || closeErr != nil {
		t.Fatalf("fixture write row=%t error=%v cleanup=%v", row, err, closeErr)
	}
}
func interopCount(t *testing.T, tx *sqliteio.Tx, sql string) int64 {
	t.Helper()
	s := interopPrepare(t, tx, sql)
	row, err := s.Step()
	if !row || err != nil {
		t.Fatal("missing count row", err)
	}
	n, err := s.Int64(0)
	if err != nil {
		t.Fatal(err)
	}
	if row, err := s.Step(); row || err != nil {
		t.Fatal("extra count row", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return n
}
func interopCounter(t *testing.T, value string) sqliteio.Value {
	t.Helper()
	b, err := sqliteEncodeUint64(value)
	if err != nil {
		t.Fatal(err)
	}
	return sqliteio.Blob(b[:])
}
func interopMeta(f interopFixture) sqliteStoreMeta {
	// This synthetic metadata-only revision 1 is for private helper transport.
	// The separate first-mutation test below requires domain rows and a receipt.
	return sqliteStoreMeta{ComputerID: interopComputer, Revision: "1", StateBasename: f.authority, DatabaseBasename: f.database, LogicalBytes: int64(114 + len(interopComputer) + len(f.authority) + len(f.database))}
}
func interopInitialize(t *testing.T, f interopFixture, meta sqliteStoreMeta) {
	t.Helper()
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal(err)
	}
	if err := sqliteInsertMeta(tx, meta); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
}
func interopSafeError(t *testing.T, err error, markers ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected safe refusal")
	}
	var domain *Error
	var native *sqliteio.Error
	if !errors.As(err, &domain) && !errors.As(err, &native) {
		t.Fatalf("unchecked error type %T", err)
	}
	var inspect func(error)
	inspect = func(e error) {
		if e == nil {
			return
		}
		for _, marker := range markers {
			if marker != "" && strings.Contains(e.Error(), marker) {
				t.Fatalf("error leaks synthetic input %q", marker)
			}
		}
		if n, ok := e.(*sqliteio.Error); ok {
			inspect(n.Cleanup)
		}
		if joined, ok := e.(interface{ Unwrap() []error }); ok {
			for _, part := range joined.Unwrap() {
				inspect(part)
			}
		} else if wrapped, ok := e.(interface{ Unwrap() error }); ok {
			inspect(wrapped.Unwrap())
		}
	}
	inspect(err)
}

func TestSQLiteEmbeddedSchemaFirstMutationRowsReceiptAndRevisionCommitTogether(t *testing.T) {
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal(err)
	}
	const bindingID = "00000000-0000-4000-8000-000000000002"
	const requestID = "00000000-0000-4000-8000-000000000003"
	binding := Binding{ID: bindingID, Revision: "1", Kind: "directory", Locator: "/synthetic/repo", Attribution: Attribution{AccountID: "1", UserID: "2", ProjectID: "3", TaskID: "4", Timezone: "UTC"}, AttachedActors: []ActorRef{}}
	result := BindingResult{ContractVersion: 1, SnapshotRevision: "1", RequestID: requestID, Changed: true, Binding: binding}
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	interopDone(t, tx, "INSERT INTO bindings(binding_id,revision,account_id,user_id,project_id,task_id,timezone,computer_id,active,record_present,kind,locator,deleted) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", sqliteio.Text(bindingID), interopCounter(t, "1"), sqliteio.Text("1"), sqliteio.Text("2"), sqliteio.Text("3"), sqliteio.Text("4"), sqliteio.Text("UTC"), sqliteio.Text(interopComputer), sqliteio.Integer(1), sqliteio.Integer(1), sqliteio.Text("directory"), sqliteio.Text("/synthetic/repo"), sqliteio.Integer(0))
	interopDone(t, tx, "INSERT INTO requests(request_id,operation,fingerprint,outcome_kind,payload) VALUES(?,?,?,?,?)", sqliteio.Text(requestID), sqliteio.Text("bindings.link"), sqliteio.Text(strings.Repeat("a", 64)), sqliteio.Text("binding_result"), sqliteio.Text(string(payload)))
	meta := interopMeta(f)
	// Independent charge formula for these explicitly materialized fixture rows.
	meta.LogicalBytes += int64(157 + len(bindingID) + 4 + len("UTC") + len(interopComputer) + len("directory") + len("/synthetic/repo"))
	meta.LogicalBytes += int64(77 + len(requestID) + len("bindings.link") + 64 + len("binding_result") + len(payload))
	if err := sqliteInsertMeta(tx, meta); err != nil {
		t.Fatal(err)
	}
	got, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(got, meta) {
		t.Fatalf("same-transaction final metadata differs: %v", err)
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	got, err = sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(got, meta) {
		t.Fatalf("reopened first mutation metadata differs: %v", err)
	}
	if interopCount(t, tx, "SELECT count(*) FROM bindings") != 1 || interopCount(t, tx, "SELECT count(*) FROM requests") != 1 {
		t.Fatal("first domain write and receipt were not durable together")
	}
	s := interopPrepare(t, tx, "SELECT payload FROM requests WHERE request_id=?", sqliteio.Text(requestID))
	if row, err := s.Step(); !row || err != nil {
		t.Fatal("receipt missing", err)
	}
	text, err := s.Text(0)
	if err != nil || text != string(payload) {
		t.Fatal("typed receipt payload changed", err)
	}
	if row, err := s.Step(); row || err != nil {
		t.Fatal("receipt identity not singleton", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteEmbeddedSchemaPartialInstallationRollsBackAndExistingCatalogRefuses(t *testing.T) {
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	err := tx.InstallSchema(sqliteSchema + "\nCREATE TABLE deliberately_broken(;")
	var native *sqliteio.Error
	if !errors.As(err, &native) || native.Code&255 != 1 {
		t.Fatalf("malformed tail did not reach SQLite parser: %v", err)
	}
	interopSafeError(t, err, f.directory, "deliberately_broken", "CREATE TABLE")
	if interopCount(t, tx, "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='bindings'") != 1 {
		t.Fatal("failure occurred before actual embedded schema installation")
	}
	// InstallSchema returned with ownership still here, so rollback removes all DDL.
	interopRollback(t, tx)
	interopClose(t, c)
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	if interopCount(t, tx, "SELECT count(*) FROM sqlite_schema") != 0 {
		t.Fatal("partial schema escaped caller rollback")
	}
	interopDone(t, tx, "CREATE TABLE existing_fixture(value TEXT NOT NULL)")
	interopDone(t, tx, "INSERT INTO existing_fixture VALUES(?)", sqliteio.Text("preserved"))
	err = sqliteCreateSchema(tx)
	interopSafeError(t, err, f.directory, "existing_fixture", "preserved")
	if interopCount(t, tx, "SELECT count(*) FROM sqlite_schema WHERE name='store_meta'") != 0 {
		t.Fatal("fresh helper adopted an existing catalog")
	}
	interopCommit(t, tx)
	interopClose(t, c)
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	if interopCount(t, tx, "SELECT count(*) FROM existing_fixture WHERE value='preserved'") != 1 {
		t.Fatal("refusal altered existing data")
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteEmbeddedDeferredHistoricalReferenceNeedsNoCurrentActor(t *testing.T) {
	for _, resolve := range []bool{true, false} {
		t.Run(map[bool]string{true: "resolved", false: "missing"}[resolve], func(t *testing.T) {
			f := interopLocation(t)
			c, tx := interopOpen(t, f, true, sqliteio.Write)
			if err := sqliteCreateSchema(tx); err != nil {
				t.Fatal(err)
			}
			interopDone(t, tx, "INSERT INTO host_turns(turn_key,source,native_session,incarnation,turn_id,agent_id,cwd,actor_key,actor_generation,stopped) VALUES(?,?,?,?,?,?,?,?,?,?)", sqliteio.Text("turn"), sqliteio.Text("codex"), sqliteio.Text("session"), sqliteio.Text("incarnation"), sqliteio.Text("turn-id"), sqliteio.Text("agent"), sqliteio.Text("/synthetic"), sqliteio.Text("historical-key"), interopCounter(t, "18446744073709551615"), sqliteio.Integer(0))
			if resolve {
				interopDone(t, tx, "INSERT INTO actor_generations(actor_key,generation,computer_id,source,session_id,agent_id) VALUES(?,?,?,?,?,?)", sqliteio.Text("historical-key"), interopCounter(t, "18446744073709551615"), sqliteio.Text(interopComputer), sqliteio.Text("codex"), sqliteio.Text("session"), sqliteio.Text("agent"))
			}
			if interopCount(t, tx, "SELECT count(*) FROM actors") != 0 {
				t.Fatal("fixture accidentally supplied current actor")
			}
			if err := sqliteInsertMeta(tx, interopMeta(f)); err != nil {
				t.Fatal(err)
			}
			err := tx.CheckForeignKeys()
			if resolve {
				if err != nil {
					t.Fatal(err)
				}
				interopCommit(t, tx)
			} else {
				interopSafeError(t, err, "historical-key", "host_turns", f.directory)
				// Dispatched CONSTRAINT stays Unknown under the established outcome contract.
				outcome, commitErr := tx.Commit()
				var native *sqliteio.Error
				if outcome != sqliteio.Unknown || !errors.As(commitErr, &native) || native.Code&255 != 19 {
					t.Fatalf("deferred FK commit outcome=%v error=%v", outcome, commitErr)
				}
				interopSafeError(t, commitErr, "historical-key", "host_turns", f.directory)
				// Finalizing the failed COMMIT can retain checked constraint cleanup.
				if cleanup := tx.Rollback(); cleanup != nil {
					var checked *sqliteio.Error
					if !errors.As(cleanup, &checked) || checked.Phase != sqliteio.FinalizePhase || checked.Code&255 != 19 {
						t.Fatalf("unexpected terminal cleanup: %v", cleanup)
					}
				}
				again, cachedErr := tx.Commit()
				if again != outcome || !reflect.DeepEqual(cachedErr, commitErr) {
					t.Fatal("terminal commit outcome/evidence changed on repeat")
				}
			}
			interopClose(t, c)
			c, tx = interopOpen(t, f, false, sqliteio.Read)
			if resolve {
				if interopCount(t, tx, "SELECT count(*) FROM host_turns") != 1 || interopCount(t, tx, "SELECT count(*) FROM actors") != 0 {
					t.Fatal("historical reference durability changed")
				}
			} else if interopCount(t, tx, "SELECT count(*) FROM sqlite_schema") != 0 {
				t.Fatal("failed deferred commit retained partial bootstrap")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}

func TestSQLiteEmbeddedUnsignedRangeAndCrossTableTrigger(t *testing.T) {
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal(err)
	}
	want := []string{"1", "2", "10", "9223372036854775807", "9223372036854775808", "18446744073709551615"}
	for i := len(want) - 1; i >= 0; i-- {
		interopDone(t, tx, "INSERT INTO bindings(binding_id,revision,account_id,user_id,project_id,task_id,timezone,computer_id,active,record_present) VALUES(?,?,?,?,?,?,?,?,1,0)", sqliteio.Text("binding-"+want[i]), interopCounter(t, want[i]), sqliteio.Text("1"), sqliteio.Text("2"), sqliteio.Text("3"), sqliteio.Text("4"), sqliteio.Text("UTC"), sqliteio.Text(interopComputer))
	}
	s := interopPrepare(t, tx, "SELECT revision FROM bindings ORDER BY revision")
	var ordered []string
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !row {
			break
		}
		kind, err := s.Kind(0)
		if err != nil || kind != sqliteio.BlobKind {
			t.Fatal("counter storage kind changed")
		}
		raw, err := s.Blob(0)
		if err != nil {
			t.Fatal(err)
		}
		value, err := sqliteDecodeUint64(raw)
		if err != nil {
			t.Fatal(err)
		}
		ordered = append(ordered, value)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ordered, want) {
		t.Fatalf("unsigned order=%v", ordered)
	}
	s = interopPrepare(t, tx, "SELECT count(*) FROM bindings WHERE revision>?", interopCounter(t, "9223372036854775807"))
	if row, err := s.Step(); !row || err != nil {
		t.Fatal(err)
	}
	n, err := s.Int64(0)
	if err != nil || n != 2 {
		t.Fatalf("unsigned range count=%d error=%v", n, err)
	}
	if row, err := s.Step(); row || err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	start := time.Date(1960, 2, 3, 4, 5, 6, 123456789, time.FixedZone("offset", 5*3600+30*60))
	end := start.Add(time.Second)
	a, err := sqliteEncodeTime(start)
	if err != nil {
		t.Fatal(err)
	}
	b, err := sqliteEncodeTime(end)
	if err != nil {
		t.Fatal(err)
	}
	interopDone(t, tx, "INSERT INTO intervals(interval_id,computer_id,account_id,user_id,project_id,task_id,timezone,group_order,start_sec,start_nsec,start_json,end_sec,end_nsec,end_json,duration_ns,creation_ordinal) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", sqliteio.Text("interval"), sqliteio.Text(interopComputer), sqliteio.Text("1"), sqliteio.Text("2"), sqliteio.Text("3"), sqliteio.Text("4"), sqliteio.Text("UTC"), sqliteio.Text("group"), sqliteio.Integer(a.Seconds), sqliteio.Integer(a.Nanoseconds), sqliteio.Text(a.JSON), sqliteio.Integer(b.Seconds), sqliteio.Integer(b.Nanoseconds), sqliteio.Text(b.JSON), interopCounter(t, "1000000000"), sqliteio.Integer(0))
	interopDone(t, tx, "INSERT INTO outbox(interval_id,id,revision,state,correlation,plan_present) VALUES(?,?,?,?,?,?)", sqliteio.Text("interval"), sqliteio.Text("global-id"), interopCounter(t, "1"), sqliteio.Text("queued"), sqliteio.Text("tempo:interval"), sqliteio.Integer(0))
	s = interopPrepare(t, tx, "INSERT INTO sync_attempts(interval_id,part_ordinal,ordinal,request_id,id,number,state) VALUES('interval',0,0,'request','global-id','1','submitting')")
	row, stepErr := s.Step()
	_ = s.Close()
	var native *sqliteio.Error
	if row || !errors.As(stepErr, &native) || native.Code&255 != 19 {
		t.Fatalf("actual embedded cross-table trigger did not reject ID reuse: %v", stepErr)
	}
	interopSafeError(t, stepErr, "global-id", "sync_attempts", f.directory)
	if interopCount(t, tx, "SELECT count(*) FROM sync_attempts") != 0 {
		t.Fatal("collision partially inserted sync attempt")
	}
	if err := tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	s = interopPrepare(t, tx, "SELECT start_sec,start_nsec,start_json FROM intervals WHERE interval_id='interval'")
	if row, err := s.Step(); !row || err != nil {
		t.Fatal(err)
	}
	seconds, err := s.Int64(0)
	if err != nil {
		t.Fatal(err)
	}
	nanos, err := s.Int64(1)
	if err != nil {
		t.Fatal(err)
	}
	wallJSON, err := s.Text(2)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := sqliteDecodeTime(sqliteTimeValue{Seconds: seconds, Nanoseconds: nanos, JSON: wallJSON})
	legacyJSON, _ := start.MarshalJSON()
	if err != nil || wallJSON != string(legacyJSON) || !decoded.Equal(start) {
		t.Fatalf("offset/pre-epoch native wall transport changed: %v", err)
	}
	if row, err := s.Step(); row || err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteCodecNativeTransportMatchesLegacyJSONAndOptionalTime(t *testing.T) {
	// Supplementary table isolates adapter+codec transport, not a new domain schema.
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := sqliteCreateSchema(tx); err != nil {
		t.Fatal(err)
	}
	interopDone(t, tx, "CREATE TABLE qa_codec(id INTEGER PRIMARY KEY,s INTEGER,n INTEGER,j TEXT,cap TEXT,epoch TEXT,elapsed TEXT,awake TEXT,raw TEXT,empty BLOB) STRICT")
	rawEpoch := "epoch\x00" + string([]byte{0xff, 0xfe})
	elapsed := "nonnumeric\x00"
	awake := ""
	sample := ClockSample{Capability: "discarded\x00" + string([]byte{0xff}), WallUTC: time.Time{}, Epoch: &rawEpoch, ElapsedNS: &elapsed, AwakeNS: &awake}
	legacyBytes, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	var legacy ClockSample
	if err := json.Unmarshal(legacyBytes, &legacy); err != nil {
		t.Fatal(err)
	}
	encoded, err := sqliteEncodeClock(sample, false)
	if err != nil {
		t.Fatal(err)
	}
	interopDone(t, tx, "INSERT INTO qa_codec VALUES(?,?,?,?,?,?,?,?,?,?)", sqliteio.Integer(1), sqliteio.Integer(encoded.Wall.Seconds), sqliteio.Integer(encoded.Wall.Nanoseconds), sqliteio.Text(encoded.Wall.JSON), sqliteio.Text(encoded.Capability), sqliteio.Text(*encoded.Epoch), sqliteio.Text(*encoded.ElapsedRaw), sqliteio.Text(*encoded.AwakeRaw), sqliteio.Text("raw\x00雪"+string([]byte{0xff})), sqliteio.Blob(nil))
	interopDone(t, tx, "INSERT INTO qa_codec(id,s,n,j,raw,empty) VALUES(2,NULL,NULL,NULL,'',NULL)")
	interopCommit(t, tx)
	interopClose(t, c)
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	s := interopPrepare(t, tx, "SELECT s,n,j,cap,epoch,elapsed,awake,raw,empty FROM qa_codec ORDER BY id")
	if row, err := s.Step(); !row || err != nil {
		t.Fatal(err)
	}
	sec, err := s.Int64(0)
	if err != nil {
		t.Fatal(err)
	}
	ns, err := s.Int64(1)
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.Text(2)
	if err != nil {
		t.Fatal(err)
	}
	texts := make([]string, 5)
	for i := range texts {
		texts[i], err = s.Text(i + 3)
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := sqliteDecodeClock(sqliteClockValue{Wall: sqliteTimeValue{sec, ns, j}, Capability: texts[0], Epoch: &texts[1], ElapsedRaw: &texts[2], AwakeRaw: &texts[3]})
	if err != nil || !reflect.DeepEqual(got, legacy) {
		t.Fatalf("discarded raw clock differs from legacy JSON oracle: %v", err)
	}
	if !bytes.Equal([]byte(texts[4]), []byte("raw\x00雪"+string([]byte{0xff}))) {
		t.Fatal("native byte-counted TEXT transport altered bytes")
	}
	kind, err := s.Kind(8)
	if err != nil || kind != sqliteio.BlobKind {
		t.Fatal("empty blob became NULL")
	}
	empty, err := s.Blob(8)
	if err != nil || len(empty) != 0 {
		t.Fatal("empty blob transport changed")
	}
	zero, err := sqliteDecodeOptionalTime(&sqliteTimeValue{sec, ns, j})
	if err != nil || zero == nil || !zero.IsZero() {
		t.Fatal("present zero time lost presence")
	}
	if row, err := s.Step(); !row || err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 1, 2, 8} {
		isNull, err := s.IsNull(i)
		if err != nil || !isNull {
			t.Fatal("absent optional triple or NULL blob lost NULL")
		}
	}
	nilTime, err := sqliteDecodeOptionalTime(nil)
	if err != nil || nilTime != nil {
		t.Fatal("absent time became present")
	}
	if _, err := sqliteDecodeTime(sqliteTimeValue{sec + 1, ns, j}); err == nil {
		t.Fatal("wall indexed projection disagreement admitted")
	}
	if row, err := s.Step(); row || err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
}
