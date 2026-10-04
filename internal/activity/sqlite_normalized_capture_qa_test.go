//go:build (darwin || linux) && (amd64 || arm64)

package activity

// SOURCE-ONLY, authored before the inactive operation. Missing reviewed private
// ABI/fresh native prerequisites are NOT behavioral RED or runtime evidence.
// No test activates default Ingest dispatch. All paths and account data are fake.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Raw all-row comparison permits an intentionally malformed selected receipt;
// the normal cold closure oracle correctly rejects this controlled fixture.
func ncQARawAudit(t *testing.T, f interopFixture) (sqliteStoreMeta, map[string][][]string) {
	t.Helper()
	h := ncQAOpen(t, f, sqliteio.Read)
	meta, err := sqliteReadMeta(h.tx, f.authority, f.database)
	if err != nil {
		t.Fatal(err)
	}
	rows, charge := cfQAAudit(t, h.tx, meta)
	if charge != meta.LogicalBytes {
		t.Fatal("raw malformed fixture charge changed")
	}
	h.end(t, false)
	return meta, rows
}

func TestSQLiteNormalizedCaptureN01AbsentValidationAndOccupiedAuthority(t *testing.T) {
	for _, basename := range []string{"activity-state.json", "custom.capture.json"} {
		t.Run(basename, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "absent-parent", basename)
			calls := 0
			s := New(Options{Path: path, Clock: ClockFunc(func() (ClockSample, error) { calls++; return ncQASample(0), nil })})
			e := qaEvent("A", "1", "1", "work", "")
			invalid := e
			invalid.ContractVersion = 2
			r, err := s.ingestSQLite(context.Background(), invalid)
			if ncQAErrorCode(err) != "unsupported_contract" || !reflect.DeepEqual(r, EventResult{}) {
				t.Fatal("validation before admission", err)
			}
			r, err = s.ingestSQLite(context.Background(), e)
			if err != nil || r.SnapshotRevision != "0" || r.Disposition != "untracked" || calls != 0 {
				t.Fatal("absent parent sampled or admitted", err)
			}
			if _, err = os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("absent parent created", err)
			}
			if err = os.Mkdir(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			r, err = s.ingestSQLite(context.Background(), e)
			if err != nil || r.Disposition != "untracked" || len(ncQAFiles(t, filepath.Dir(path))) != 0 {
				t.Fatal("absent D roles created files", err)
			}
			if err = os.WriteFile(path, []byte("preserve synthetic occupied authority"), 0600); err != nil {
				t.Fatal(err)
			}
			before := ncQAFiles(t, filepath.Dir(path))
			r, err = s.ingestSQLite(context.Background(), e)
			if ncQAErrorCode(err) != "state_path_in_use" || !reflect.DeepEqual(r, EventResult{}) || !reflect.DeepEqual(before, ncQAFiles(t, filepath.Dir(path))) || calls != 0 {
				t.Fatal("occupied P preserved/refused before sample", err)
			}
		})
	}
}

func TestSQLiteNormalizedCaptureN01MainMissingSidecarEmptyAndReservedMetadata(t *testing.T) {
	for _, suffix := range []string{"-wal", "-shm", "-journal", ""} {
		t.Run("unadmitted"+suffix, func(t *testing.T) {
			f := interopLocation(t)
			if err := os.WriteFile(filepath.Join(f.directory, f.database+suffix), []byte{}, 0600); err != nil {
				t.Fatal(err)
			}
			before := ncQAFiles(t, f.directory)
			s := New(Options{Path: filepath.Join(f.directory, f.authority), Clock: ClockFunc(func() (ClockSample, error) { t.Fatal("unadmitted store sampled"); return ClockSample{}, nil })})
			r, err := s.ingestSQLite(context.Background(), qaEvent("A", "1", "1", "work", ""))
			if err == nil || !reflect.DeepEqual(r, EventResult{}) || !reflect.DeepEqual(before, ncQAFiles(t, f.directory)) {
				t.Fatal("incomplete/empty D adopted or changed", err)
			}
		})
	}
	t.Run("valid-reserved-pair", func(t *testing.T) {
		q := ncQAWorking(t)
		w := ncQAOpen(t, q.f, sqliteio.Write)
		m, err := sqliteReadMeta(w.tx, q.f.authority, q.f.database)
		if err != nil {
			t.Fatal(err)
		}
		next := metaQANext(t, m)
		next.Revision = bump(m.Revision)
		migration, backup := "88888888-8888-4888-8888-888888888881", strings.Repeat("a", 64)
		next.MigrationID, next.BackupSHA256 = &migration, &backup
		oldCharge, err := sqliteMetaCharge(m)
		if err != nil {
			t.Fatal(err)
		}
		newCharge, err := sqliteMetaCharge(next)
		if err != nil {
			t.Fatal(err)
		}
		next.LogicalBytes += newCharge - oldCharge
		ncQAStageMetaBoundary(t, w.tx, q.f, m, next)
		if accepted, err := sqliteReadCaptureSchema(w.tx, q.f.authority, q.f.database); ncQAErrorCode(err) != "state_corrupt" || !reflect.DeepEqual(accepted, sqliteStoreMeta{}) {
			t.Fatal("fresh capture schema admitted reserved metadata", err)
		}
		w.end(t, true)
		stored, _ := ncQAAudit(t, q.f)
		if !reflect.DeepEqual(stored, next) {
			t.Fatal("reserved pair lost actual metadata charge/nonce")
		}
		before := ncQAFiles(t, q.f.directory)
		r, err := q.service.ingestSQLite(context.Background(), qaEvent("A", "1", "2", "observe_work", ""))
		if err == nil || !reflect.DeepEqual(r, EventResult{}) || q.clockCalls != 0 || !reflect.DeepEqual(before, ncQAFiles(t, q.f.directory)) {
			t.Fatal("reserved nonfresh store changed", err)
		}
	})
}

func TestSQLiteNormalizedCaptureN02ExactHistoricalReplayFingerprintAndEventIDPriority(t *testing.T) {
	q := ncQAWorking(t)
	e := qaEvent("A", "1", "1", "work", qaBindingA)
	// Advance both stores first; replay is still the original historical result.
	ncQACompare(t, q, qaEvent("A", "1", "2", "observe_work", ""), 10, "", 1)
	before, rows := ncQAAudit(t, q.f)
	r := ncQACompare(t, q, e, 20, "", 0)
	if r.Disposition != "duplicate" || r.SnapshotRevision == before.Revision {
		t.Fatal("replay returned current head revision")
	}
	ncQANonceOnly(t, before, rows, q.f)
	before, rows = ncQAAudit(t, q.f)
	conflict := e
	conflict.EventID = "different-ID-at-exact-key"
	ncQACompare(t, q, conflict, 20, "event_conflict", 0)
	ncQAUnchanged(t, before, rows, q.f)
	// A raw EventID collision wins before the otherwise invalid initial branch.
	collision := qaEvent("missing", "1", "9", "finish", "")
	collision.EventID = e.EventID
	ncQACompare(t, q, collision, 20, "event_conflict", 0)
	ncQAUnchanged(t, before, rows, q.f)
	key, fingerprint := ncQARawIdentity(t, e)
	h := ncQAOpen(t, q.f, sqliteio.Read)
	stored, ok, err := sqliteReadEventReceipt(h.tx, key, before.Revision)
	if err != nil || !ok || stored.Value.Fingerprint != fingerprint {
		t.Fatal("raw marshal fingerprint independent oracle", err)
	}
	h.end(t, false)
}

func TestSQLiteNormalizedCaptureN03NoSampleBranchTable(t *testing.T) {
	for _, test := range []struct {
		name    string
		e       Event
		code    string
		prepare func(*qaHarness)
	}{
		{"missing-finish", qaEvent("missing", "1", "1", "finish", ""), "event_gap", nil},
		{"missing-sequence", qaEvent("missing", "1", "2", "work", qaBindingA), "event_gap", nil},
		{"computer", func() Event { e := qaEvent("A", "1", "2", "work", ""); e.Actor.ComputerID = asQAForeign; return e }(), "validation", nil},
		{"same-sequence", qaEvent("A", "1", "1", "finish", ""), "event_conflict", nil},
		{"binding-placement", qaEvent("A", "1", "2", "work", qaBindingA), "validation", nil},
		{"parent-placement", func() Event {
			e := qaEvent("A", "1", "2", "work", "")
			r := ActorRef{Key: e.Actor, Generation: "1"}
			e.Parent = &r
			return e
		}(), "validation", nil},
		{"lower-generation", qaEvent("A", "1", "2", "finish", ""), "", func(h *qaHarness) { h.ingest(10, qaEvent("A", "2", "1", "work", qaBindingA)) }},
		{"terminal", qaEvent("A", "1", "3", "work", ""), "", func(h *qaHarness) { h.ingest(0, qaEvent("A", "1", "2", "finish", "")) }},
		{"blocked", qaEvent("A", "1", "2", "work", ""), "event_gap", func(h *qaHarness) {
			h.at(10)
			_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "4", "work", ""))
			qaCode(t, err, "event_gap")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := ncQANew(t, func(h *qaHarness) {
				h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
				if test.prepare != nil {
					test.prepare(h)
				}
			})
			before, rows := ncQAAudit(t, q.f)
			r := ncQACompare(t, q, test.e, 20, test.code, 0)
			if test.code != "" {
				if !reflect.DeepEqual(r, EventResult{}) {
					t.Fatal("false Changed public error must be zero")
				}
				ncQAUnchanged(t, before, rows, q.f)
			} else {
				if r.Disposition != "stale" {
					t.Fatal("stale disposition")
				}
				ncQANonceOnly(t, before, rows, q.f)
			}
			if q.resolverCalls != 0 {
				t.Fatal("early branch resolved binding")
			}
		})
	}
}

func TestSQLiteNormalizedCaptureN03AppliedKindsAndGeneration(t *testing.T) {
	for _, kind := range []string{"work", "observe_work", "wait_user", "wait_permission", "wait_children", "finish", "interrupt"} {
		t.Run(kind, func(t *testing.T) {
			q := ncQAWorking(t)
			ncQACompare(t, q, qaEvent("A", "1", "2", kind, ""), 10, "", 1)
		})
	}
	q := ncQAWorking(t)
	ncQACompare(t, q, qaEvent("A", "2", "1", "work", qaBindingB), 10, "", 1)
	if q.resolverCalls != 1 {
		t.Fatal("new generation resolver cardinality")
	}
}

func TestSQLiteNormalizedCaptureN04ResolverValidationAndDeclinedExplicit(t *testing.T) {
	for _, kind := range []string{"accept", "decline", "error", "invalid", "conflict", "other-timer"} {
		t.Run(kind, func(t *testing.T) {
			q := ncQAWorking(t)
			e := qaEvent("B", "1", "1", "work", qaBindingB)
			r := ActorRef{Key: qaEvent("A", "1", "1", "work", "").Actor, Generation: "1"}
			e.Parent = &r
			code := ""
			calls := 1
			snapshot := q.legacy.bindings[qaBindingB]
			switch kind {
			case "error":
				code = "binding_unavailable"
				calls = 0
			case "invalid":
				snapshot.ID = "bad"
				code = "binding_unavailable"
				calls = 0
			case "conflict":
				snapshot.Attribution.TaskID = "9"
				code = "attribution_conflict"
				calls = 0
			case "other-timer":
				snapshot.Attribution.ProjectID = "9"
			case "decline":
				calls = 0
			}
			resolver := func(context.Context, Event) (BindingSnapshot, bool, error) {
				if kind == "error" {
					return BindingSnapshot{}, false, errors.New("synthetic resolver secret")
				}
				return snapshot, kind != "decline", nil
			}
			q.service.resolve = func(ctx context.Context, e Event) (BindingSnapshot, bool, error) {
				q.resolverCalls++
				return resolver(ctx, e)
			}
			q.legacy.service.resolve = resolver
			ncQACompare(t, q, e, 10, code, calls)
			if q.resolverCalls != 1 {
				t.Fatal("resolver must run exactly once")
			}
			if kind == "decline" {
				st := ncQAReadState(t, q)
				if st.Actors[actorKey(e.Actor)] != nil {
					t.Fatal("declined explicit ID incorrectly fell back to Parent")
				}
			}
		})
	}
}

func TestSQLiteNormalizedCaptureN04DefaultDirectoryExplicitAndHistoricalParent(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(root, "nearest")
	leaf := filepath.Join(child, "leaf")
	if err = os.MkdirAll(leaf, 0700); err != nil {
		t.Fatal(err)
	}
	for _, axis := range []string{"nearest", "explicit", "missing", "wrong-revision", "parent", "terminal-parent"} {
		t.Run(axis, func(t *testing.T) {
			q := ncQANew(t, func(h *qaHarness) {
				h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
				if axis == "terminal-parent" {
					h.ingest(0, qaEvent("A", "1", "2", "finish", ""))
				}
				if err := h.service.store.update(context.Background(), func(st *state) (bool, error) {
					st.BindingRecords = map[string]bindingRecord{
						qaBindingA: {Snapshot: st.Bindings[qaBindingA], Kind: "directory", Locator: root},
						qaBindingB: {Snapshot: st.Bindings[qaBindingB], Kind: "directory", Locator: child},
					}
					if axis == "parent" {
						r := st.BindingRecords[qaBindingA]
						r.Deleted = true
						st.BindingRecords[qaBindingA] = r
						delete(st.Bindings, qaBindingA)
					}
					return true, nil
				}); err != nil {
					t.Fatal(err)
				}
			})
			q.service.resolve = nil
			q.legacy.service.resolve = nil
			e := qaEvent("B", "1", "1", "work", "")
			code := ""
			calls := 1
			switch axis {
			case "nearest":
				e.CWD = leaf
			case "explicit":
				e.BindingID = qaBindingB
				e.BindingRevision = "1"
				e.CWD = leaf
			case "missing":
				e.BindingID = sdQAMissing
				e.BindingRevision = "1"
				code = "binding_unavailable"
				calls = 0
			case "wrong-revision":
				e.BindingID = qaBindingB
				e.BindingRevision = "2"
				code = "binding_unavailable"
				calls = 0
			case "parent", "terminal-parent":
				e.Parent = &ActorRef{Key: qaEvent("A", "1", "1", "work", "").Actor, Generation: "1"}
				if axis == "terminal-parent" {
					calls = 0
				}
			}
			r := ncQACompare(t, q, e, 10, code, calls)
			if axis == "terminal-parent" && r.Disposition != "untracked" {
				t.Fatal("terminal Parent fallback")
			}
			if axis == "nearest" || axis == "explicit" {
				a := ncQAReadState(t, q).Actors[actorKey(e.Actor)]
				if a.BindingID != qaBindingB {
					t.Fatal("nearest admitted record")
				}
			}
		})
	}
}

func TestSQLiteNormalizedCaptureN04ZeroBindingRevisionIsSyntacticThenFatalPersistence(t *testing.T) {
	q := ncQANew(t, nil)
	b := q.legacy.bindings[qaBindingA]
	b.Revision = "0"
	q.legacy.bindings[qaBindingA] = b
	e := qaEvent("A", "1", "1", "work", qaBindingA)
	e.BindingRevision = "0"
	if err := validateEvent(e); err != nil {
		t.Fatal("zero revision pre-sample syntax was strengthened", err)
	}
	before, rows := ncQAAudit(t, q.f)
	ncQACompare(t, q, e, 0, "validation", 1)
	ncQAUnchanged(t, before, rows, q.f)
}

func TestSQLiteNormalizedCaptureN05PreparedClockSlotsAndScalarSanitation(t *testing.T) {
	for _, site := range []sqliteCaptureClockSite{sqliteCaptureClockGap, sqliteCaptureClockReduce} {
		t.Run(fmt.Sprint(site), func(t *testing.T) {
			sample := ncQASample(10)
			clock := sqliteCaptureClock{Mode: sqliteCapturePrepared, Site: site, Prepared: &sqliteCaptureSample{Value: sample}}
			got, err := clock.take(site)
			if err != nil || !clock.Consumed || !reflect.DeepEqual(got, sample) {
				t.Fatal("one owned slot", err)
			}
			if _, err = clock.take(site); err == nil {
				t.Fatal("double slot consumption accepted")
			}
		})
	}
	for _, clock := range []sqliteCaptureClock{
		{Mode: sqliteCapturePrepared, Site: sqliteCaptureClockNone},
		{Mode: sqliteCapturePrepared, Site: sqliteCaptureClockReduce},
		{Mode: sqliteCaptureClockMode(99), Site: sqliteCaptureClockReduce},
		{Mode: sqliteCapturePrepared, Site: sqliteCaptureClockGap, Prepared: &sqliteCaptureSample{Value: ncQASample(0)}},
	} {
		if _, err := clock.take(sqliteCaptureClockReduce); err == nil {
			t.Fatal("invalid mode/site/slot accepted")
		}
	}
	for _, axis := range []string{"available", "callback-error", "unavailable", "nil-epoch", "leading-zero", "overflow", "zero-wall", "raw-epoch"} {
		t.Run(axis, func(t *testing.T) {
			value := ncQASample(10)
			var callbackErr error
			switch axis {
			case "callback-error":
				callbackErr = errors.New("synthetic clock secret")
			case "unavailable":
				value.Capability = "unavailable"
			case "nil-epoch":
				value.Epoch = nil
			case "leading-zero":
				value.ElapsedNS = asQAPointer("01")
			case "overflow":
				value.AwakeNS = asQAPointer("9223372036854775808")
			case "zero-wall":
				value.WallUTC = time.Time{}
			case "raw-epoch":
				value.Epoch = asQAPointer("raw-" + string([]byte{0xff}))
			}
			fallback := qaEpochStart.Add(time.Hour)
			got, err := normalizeActivitySample(value, callbackErr, fallback)
			expected := value
			if expected.WallUTC.IsZero() {
				expected.WallUTC = fallback
			}
			expected.WallUTC = expected.WallUTC.UTC()
			_, _, valid := sampleValues(expected)
			if callbackErr != nil || !valid {
				expected.Capability = "unavailable"
				expected.Epoch = nil
				expected.ElapsedNS = nil
				expected.AwakeNS = nil
				if ncQAErrorCode(err) != "clock_unavailable" {
					t.Fatal("sanitized unavailable", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, expected) {
				t.Fatal("shared sanitation differs from actual sample policy")
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatal("clock text leak")
			}
		})
	}
}

func TestSQLiteNormalizedCaptureN05ExternalCallbacksAfterNativeCloseCanAcquireWriter(t *testing.T) {
	q := ncQANew(t, nil)
	ncQAHookPositive(t, q.f)
	e := qaEvent("A", "1", "1", "work", qaBindingA)
	order := []string{}
	closed := 0
	ncQAHooks(t, ncQASQLHooks{Observe: func(e ncQASQLEvent) {
		if e.Phase == "close-after" && e.Operation == "close" {
			closed++
		}
	}})
	probe := func(label string) {
		order = append(order, label)
		if label == "resolver" && closed < 1 || label == "clock" && closed < 3 {
			t.Fatal("callback entered before actual checked native read closure", label, closed)
		}
		w := ncQAOpen(t, q.f, sqliteio.Write)
		ncQADone(t, w.tx, "UPDATE store_meta SET logical_bytes=logical_bytes WHERE singleton=1")
		w.end(t, true)
	}
	q.resolverAction = func() { probe("resolver") }
	q.clockAction = func() { probe("clock") }
	ncQACompare(t, q, e, 10, "", 1)
	ncQASetSQLHooks(ncQASQLHooks{})
	if !reflect.DeepEqual(order, []string{"resolver", "clock"}) {
		t.Fatal("resolver/clock ordering", order)
	}
}

func TestSQLiteNormalizedCaptureN06WriterReplayWinsAfterInjectedClockAndSavedBindingError(t *testing.T) {
	for _, callback := range []string{"clock", "resolver-error", "resolver-refusal"} {
		t.Run(callback, func(t *testing.T) {
			q := ncQANew(t, nil)
			q.service.store.timeout = time.Second // Outer call includes a complete peer capture.
			e := qaEvent("A", "1", "1", "work", qaBindingA)
			other := New(Options{Path: filepath.Join(q.f.directory, q.f.authority), Clock: ClockFunc(func() (ClockSample, error) { return ncQASample(10), nil }), ResolveBinding: q.legacy.service.resolve})
			var committed EventResult
			commitOther := func() {
				var err error
				committed, err = other.ingestSQLite(context.Background(), e)
				if err != nil {
					t.Fatal("concurrent exact receipt control", err)
				}
			}
			if callback == "clock" {
				q.sample = ncQASample(10)
				q.clockAction = commitOther
			} else if callback == "resolver-refusal" {
				q.resolverAction = commitOther
			} else {
				q.service.resolve = func(context.Context, Event) (BindingSnapshot, bool, error) {
					q.resolverCalls++
					commitOther()
					return BindingSnapshot{}, false, errors.New("synthetic resolver failure")
				}
			}
			r, err := q.service.ingestSQLite(context.Background(), e)
			if err != nil || r.Disposition != "duplicate" || r.SnapshotRevision != committed.SnapshotRevision {
				t.Fatal("writer receipt did not outrank saved preparation", err)
			}
			if callback == "clock" && q.clockCalls != 1 || callback != "clock" && q.clockCalls != 0 || q.resolverCalls != 1 {
				t.Fatal("callbacks repeated or branch sampled")
			}
			ncQAAudit(t, q.f)
		})
	}
}

func TestSQLiteNormalizedCaptureN06ConcurrentExactReceiptPrecedesFootprintRefusal(t *testing.T) {
	q := ncQANew(t, nil)
	q.service.store.timeout = time.Second // Outer call includes peer capture and cold audit.
	e := qaEvent("A", "1", "1", "work", qaBindingA)
	peer := New(Options{Path: filepath.Join(q.f.directory, q.f.authority), Clock: ClockFunc(func() (ClockSample, error) { return ncQASample(10), nil }), ResolveBinding: q.legacy.service.resolve})
	mainPath := filepath.Join(q.f.directory, q.f.database)
	var committed EventResult
	var before sqliteStoreMeta
	var rows map[string][][]string
	hit, journalAfter, rolledBack, closed := false, false, false, false
	t.Cleanup(func() {
		ncQASetFSHooks(ncQAFSHooks{})
		ncQASetSQLHooks(ncQASQLHooks{})
		if err := os.Chmod(mainPath, 0600); err != nil {
			t.Error("restore private test file", err)
		}
	})
	q.sample = ncQASample(10)
	q.clockAction = func() {
		var err error
		committed, err = peer.ingestSQLite(context.Background(), e)
		if err != nil || committed.Disposition != "applied" {
			t.Fatal("real peer receipt prerequisite", err)
		}
		before, rows = ncQAAudit(t, q.f)
		ncQASetFSHooks(ncQAFSHooks{Observe: func(ev ncQAFSEvent) {
			if ev.Op == "footprint" && ev.Phase == "after" && ev.Role == "journal" {
				journalAfter = true
			}
			if ev.Op == "footprint" && ev.Phase == "before" && ev.Role == "main" && !hit {
				hit = true
				if x := os.Chmod(mainPath, 0644); x != nil {
					t.Fatal("controlled private mode refusal", x)
				}
			}
		}})
		ncQASetSQLHooks(ncQASQLHooks{Observe: func(ev ncQASQLEvent) {
			if hit && ev.Operation == "rollback" && ev.Phase == "rollback-after" && ev.Code == 0 {
				rolledBack = true
			}
			if hit && ev.Operation == "close" && ev.Phase == "close-after" {
				closed = true
			}
		}})
	}
	r, err := q.service.ingestSQLite(context.Background(), e)
	ncQASetFSHooks(ncQAFSHooks{})
	ncQASetSQLHooks(ncQASQLHooks{})
	if x := os.Chmod(mainPath, 0600); x != nil {
		t.Fatal("restore private test file", x)
	}
	if !hit || journalAfter || !rolledBack || !closed || ncQAErrorCode(err) != "local_write_unknown" || !reflect.DeepEqual(r, EventResult{}) || q.clockCalls != 1 || q.resolverCalls != 1 {
		t.Fatalf("historical receipt/Footprint refusal hit=%t journalAfter=%t rollback=%t closed=%t clock=%d resolver=%d err=%v", hit, journalAfter, rolledBack, closed, q.clockCalls, q.resolverCalls, err)
	}
	var checked *sqliteio.Error
	if !errors.As(err, &checked) || checked.Phase != sqliteio.VerifyPhase || checked.Category != sqliteio.Unsafe {
		t.Fatal("actual checked Footprint refusal was not retained", err)
	}
	ncQAUnchanged(t, before, rows, q.f)
	r, err = q.service.ingestSQLite(context.Background(), e)
	if err != nil || r.Disposition != "duplicate" || r.SnapshotRevision != committed.SnapshotRevision || q.clockCalls != 1 {
		t.Fatal("exact receipt unavailable after Footprint refusal removed", err)
	}
	ncQANonceOnly(t, before, rows, q.f)
}

func TestSQLiteNormalizedCaptureN06SelectedFullRowRechecksAndUnrelatedRevision(t *testing.T) {
	for _, axis := range []string{"target", "clock-candidate", "target-same-revision", "candidate-same-revision", "selected-segment", "selected-epoch", "candidate-add", "candidate-remove", "unrelated-receipt", "unrelated-terminal"} {
		t.Run(axis, func(t *testing.T) {
			q := ncQANew(t, func(h *qaHarness) {
				h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
				h.ingest(0, qaEvent("B", "1", "1", "work", qaBindingA))
				h.ingest(0, qaEvent("C", "1", "1", "work", qaBindingA))
				h.ingest(0, qaEvent("C", "1", "2", "finish", ""))
			})
			q.service.store.timeout = time.Second // Outer call includes competing typed commit and cold validation.
			t.Cleanup(func() { ncQASetFSHooks(ncQAFSHooks{}) })
			ncQAReadState(t, q) // Actual complete valid positive before-fact graph.
			e := qaEvent("A", "1", "2", "observe_work", "")
			q.sample = ncQASample(10)
			var changedMeta sqliteStoreMeta
			var changedRows map[string][][]string
			callbackComplete, reachedFootprint := false, false
			q.clockAction = func() {
				w := ncQAOpen(t, q.f, sqliteio.Write)
				m, err := sqliteReadMeta(w.tx, q.f.authority, q.f.database)
				if err != nil {
					t.Fatal(err)
				}
				var delta int64
				if axis == "unrelated-receipt" {
					extra := qaEvent("other", "1", "1", "work", "")
					key, fp := ncQARawIdentity(t, extra)
					if d, err := sqliteEnsureActorGeneration(w.tx, ActorRef{Key: extra.Actor, Generation: "1"}); err != nil {
						t.Fatal(err)
					} else {
						delta += d
					}
					row := sqliteEventReceiptRow{Key: key, Value: eventReceipt{Fingerprint: fp, Result: baseResult(extra, bump(m.Revision), "applied")}}
					if d, err := sqliteInsertEventReceipt(w.tx, row, extra.EventID); err != nil {
						t.Fatal(err)
					} else {
						delta += d
					}
				} else {
					actor := "A"
					if axis == "clock-candidate" || axis == "candidate-same-revision" || axis == "selected-segment" || axis == "selected-epoch" || axis == "candidate-remove" {
						actor = "B"
					}
					if axis == "unrelated-terminal" || axis == "candidate-add" {
						actor = "C"
					}
					key := qaEvent(actor, "1", "1", "work", "").Actor
					before, ok, err := sqliteReadActorLocal(w.tx, m.ComputerID, key)
					if !ok || err != nil {
						t.Fatal(err)
					}
					switch axis {
					case "selected-segment", "selected-epoch":
						if before.SegmentID == nil {
							t.Fatal("selected working candidate segment absent")
						}
						seg, ok, err := sqliteReadSegmentLocal(w.tx, m.ComputerID, *before.SegmentID)
						if !ok || err != nil {
							t.Fatal(err)
						}
						if axis == "selected-segment" {
							next := seg
							next.ConfirmedSample.WallUTC = seg.ConfirmedSample.WallUTC.In(time.FixedZone("qa-selected-offset", 3600))
							if reflect.DeepEqual(next, seg) || !next.ConfirmedSample.WallUTC.Equal(seg.ConfirmedSample.WallUTC) {
								t.Fatal("valid full segment scalar witness")
							}
							if d, err := sqliteWriteSegmentLocal(w.tx, m.ComputerID, &seg, next); err != nil {
								t.Fatal(err)
							} else {
								delta += d
							}
						} else {
							ep, ok, err := sqliteReadEpoch(w.tx, m.ComputerID, seg.EpochID)
							if !ok || err != nil {
								t.Fatal(err)
							}
							next := ep
							next.Value.Anchor.WallUTC = ep.Value.Anchor.WallUTC.In(time.FixedZone("qa-selected-offset", 3600))
							oldClock, err := sqliteEncodeClock(ep.Value.Anchor, true)
							if err != nil {
								t.Fatal(err)
							}
							newClock, err := sqliteEncodeClock(next.Value.Anchor, true)
							if err != nil {
								t.Fatal(err)
							}
							if oldClock.Wall.JSON == newClock.Wall.JSON || oldClock.Wall.Seconds != newClock.Wall.Seconds || oldClock.Wall.Nanoseconds != newClock.Wall.Nanoseconds {
								t.Fatal("valid full epoch scalar with unchanged projected instant")
							}
							oldCharge, err := sqliteEpochRowCharge(ep)
							if err != nil {
								t.Fatal(err)
							}
							newCharge, err := sqliteEpochRowCharge(next)
							if err != nil {
								t.Fatal(err)
							}
							ncQADone(t, w.tx, "UPDATE epochs SET anchor_wall_json=? WHERE epoch_id=? AND anchor_wall_json=?", sqliteio.Text(newClock.Wall.JSON), sqliteio.Text(ep.Value.ID), sqliteio.Text(oldClock.Wall.JSON))
							delta += newCharge - oldCharge
						}
						unchanged, ok, err := sqliteReadActorLocal(w.tx, m.ComputerID, key)
						if !ok || err != nil || !reflect.DeepEqual(before, unchanged) {
							t.Fatal("dependency-only witness changed actor", err)
						}
					default:
						after := before
						switch axis {
						case "target-same-revision", "candidate-same-revision":
							after.LastEvidence = ncQASample(1)
						case "candidate-add":
							after.State = "wait_user"
						case "candidate-remove":
							after.Health = "stale"
						default:
							after.Revision = bump(before.Revision)
						}
						if reflect.DeepEqual(before, after) {
							t.Fatal("selected scalar mutation absent")
						}
						if axis == "target-same-revision" || axis == "candidate-same-revision" || axis == "candidate-add" || axis == "candidate-remove" {
							if after.Revision != before.Revision {
								t.Fatal("full fact witness changed actor revision")
							}
						}
						if d, err := sqliteWriteActorLocal(w.tx, m.ComputerID, &before, after); err != nil {
							t.Fatal(err)
						} else {
							delta += d
						}
					}
				}
				next := metaQANext(t, m)
				next.Revision = bump(m.Revision)
				next.LogicalBytes += delta
				if err := sqliteUpdateMeta(w.tx, m, next); err != nil {
					t.Fatal(err)
				}
				w.end(t, true)
				changedMeta, changedRows = ncQAAudit(t, q.f)
				ncQAReadState(t, q) // Independent typed/whole validity after concurrent commit.
				callbackComplete = true
				ncQASetFSHooks(ncQAFSHooks{Observe: func(ev ncQAFSEvent) {
					if ev.Role == "journal" && ev.Op == "footprint" && ev.Phase == "after" {
						reachedFootprint = true
					}
				}})
			}
			r, err := q.service.ingestSQLite(context.Background(), e)
			ncQASetFSHooks(ncQAFSHooks{})
			if !strings.HasPrefix(axis, "unrelated-") {
				if !callbackComplete || !reachedFootprint || ncQAErrorCode(err) != "state_busy" || !reflect.DeepEqual(r, EventResult{}) {
					t.Fatalf("selected fact refusal callback=%t journalAfter=%t err=%v", callbackComplete, reachedFootprint, err)
				}
				ncQAUnchanged(t, changedMeta, changedRows, q.f)
			} else {
				if err != nil || r.SnapshotRevision != bump(changedMeta.Revision) {
					t.Fatal("unrelated revision rejected or wrong final revision", err)
				}
				ncQAAudit(t, q.f)
			}
			if q.clockCalls != 1 {
				t.Fatal("preparation clock retry")
			}
		})
	}
}

func TestSQLiteNormalizedCaptureN06BindingAbsentLocationParentAndTimerSetRechecks(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(root, "leaf")
	if err = os.Mkdir(leaf, 0700); err != nil {
		t.Fatal(err)
	}
	for _, axis := range []string{"explicit-full-row", "absent-location", "parent", "same-timer-membership", "other-timer-membership"} {
		t.Run(axis, func(t *testing.T) {
			q := ncQANew(t, func(h *qaHarness) {
				h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
				if axis == "parent" {
					h.ingest(0, qaEvent("A", "1", "2", "wait_permission", ""))
				}
				if err := h.service.store.update(context.Background(), func(st *state) (bool, error) {
					st.BindingRecords = map[string]bindingRecord{qaBindingA: {Snapshot: st.Bindings[qaBindingA], Kind: "directory", Locator: root}}
					return true, nil
				}); err != nil {
					t.Fatal(err)
				}
			})
			q.service.store.timeout = time.Second // Outer call includes competing typed commit and cold audit.
			t.Cleanup(func() { ncQASetFSHooks(ncQAFSHooks{}) })
			e := qaEvent("B", "1", "1", "work", qaBindingA)
			if axis == "explicit-full-row" || axis == "absent-location" || axis == "parent" {
				q.service.resolve = nil
			}
			if axis == "absent-location" {
				e.BindingID = ""
				e.BindingRevision = ""
				e.CWD = leaf
			}
			if axis == "parent" {
				e.BindingID = ""
				e.BindingRevision = ""
				e.Parent = &ActorRef{Key: qaEvent("A", "1", "1", "work", "").Actor, Generation: "1"}
			}
			q.sample = ncQASample(10)
			var after sqliteStoreMeta
			var rows map[string][][]string
			callbackComplete, reachedFootprint := false, false
			q.clockAction = func() {
				w := ncQAOpen(t, q.f, sqliteio.Write)
				m, err := sqliteReadMeta(w.tx, q.f.authority, q.f.database)
				if err != nil {
					t.Fatal(err)
				}
				var delta int64
				if axis == "parent" {
					before, ok, err := sqliteReadActorLocal(w.tx, m.ComputerID, e.Parent.Key)
					if !ok || err != nil {
						t.Fatal(err)
					}
					next := before
					next.Revision = bump(next.Revision)
					delta, err = sqliteWriteActorLocal(w.tx, m.ComputerID, &before, next)
					if err != nil {
						t.Fatal(err)
					}
				} else if axis == "explicit-full-row" {
					before, ok, err := sqliteReadBinding(w.tx, m.ComputerID, qaBindingA)
					if !ok || err != nil {
						t.Fatal(err)
					}
					next := bgQACloneRow(before)
					next.Record.Locator = leaf
					delta, err = sqliteUpdateBinding(w.tx, before, next)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					snapshot := q.legacy.bindings[qaBindingA]
					snapshot.ID = sdQAMissing
					if axis == "other-timer-membership" || axis == "absent-location" {
						snapshot.Attribution.ProjectID = "9"
					}
					row := sqliteBindingRow{ComputerID: m.ComputerID, Snapshot: snapshot}
					if axis == "absent-location" {
						row.Record = &bindingRecord{Snapshot: snapshot, Kind: "directory", Locator: leaf}
					}
					delta, err = sqliteInsertBinding(w.tx, row)
					if err != nil {
						t.Fatal(err)
					}
				}
				next := metaQANext(t, m)
				next.Revision = bump(m.Revision)
				next.LogicalBytes += delta
				if err = sqliteUpdateMeta(w.tx, m, next); err != nil {
					t.Fatal(err)
				}
				w.end(t, true)
				after, rows = ncQAAudit(t, q.f)
				callbackComplete = true
				ncQASetFSHooks(ncQAFSHooks{Observe: func(ev ncQAFSEvent) {
					if ev.Role == "journal" && ev.Op == "footprint" && ev.Phase == "after" {
						reachedFootprint = true
					}
				}})
			}
			r, err := q.service.ingestSQLite(context.Background(), e)
			ncQASetFSHooks(ncQAFSHooks{})
			if axis == "other-timer-membership" {
				if err != nil || r.Disposition != "applied" {
					t.Fatal("unrelated timer change refused", err)
				}
				ncQAAudit(t, q.f)
			} else {
				if !callbackComplete || !reachedFootprint || ncQAErrorCode(err) != "state_busy" || !reflect.DeepEqual(r, EventResult{}) {
					t.Fatalf("selected binding/parent/membership refusal callback=%t journalAfter=%t err=%v", callbackComplete, reachedFootprint, err)
				}
				ncQAUnchanged(t, after, rows, q.f)
			}
			if q.clockCalls != 1 {
				t.Fatal("changed preparation retried clock")
			}
		})
	}
}

func TestSQLiteNormalizedCaptureN06ClaudePendingToolsAndLatestReceiptRechecks(t *testing.T) {
	for _, axis := range []string{"pending-tools", "latest-receipt"} {
		t.Run(axis, func(t *testing.T) {
			host := qaNewClaude(t)
			host.startSession()
			root := host.send(0, host.event("UserPromptSubmit", "prompt", ""))
			host.send(0, qaClaudeQuestion(host, "PreToolUse", "q"))
			if root.Actor == nil {
				t.Fatal("actual native wait fixture")
			}
			st := bgQAReadLegacy(t, host.service)
			q := ncQANew(t, func(h *qaHarness) {
				bgQALegacyMarshalOracle(t, h.service, h.path, st, true)
				h.bindings = map[string]BindingSnapshot{}
				for id, b := range st.Bindings {
					h.bindings[id] = b
				}
			})
			q.service.store.timeout = time.Second // Outer call includes competing typed commit and cold audit.
			t.Cleanup(func() { ncQASetFSHooks(ncQAFSHooks{}) })
			var binding BindingSnapshot
			for _, b := range st.Bindings {
				binding = b
				break
			}
			e := qaEvent("observer", "1", "1", "work", binding.ID)
			e.Actor.ComputerID = st.ComputerID
			e.BindingRevision = binding.Revision
			q.sample = ncQASample(10)
			q.sample.Epoch = asQAPointer("new-boot")
			var changed sqliteStoreMeta
			var rows map[string][][]string
			callbackComplete, reachedFootprint := false, false
			q.clockAction = func() {
				w := ncQAOpen(t, q.f, sqliteio.Write)
				m, err := sqliteReadMeta(w.tx, q.f.authority, q.f.database)
				if err != nil {
					t.Fatal(err)
				}
				var delta int64
				if axis == "pending-tools" {
					var key string
					for k, turn := range st.HostTurns {
						if turn.Actor != nil && *turn.Actor == *root.Actor {
							key = k
							break
						}
					}
					before, ok, err := sqliteReadHostTool(w.tx, m.ComputerID, key, "q")
					if !ok || err != nil {
						t.Fatal(err)
					}
					after := before
					after.Value.Phase = "post"
					delta, err = sqliteWriteHostTool(w.tx, m.ComputerID, &before, after)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					prior, ok, err := sqliteLatestHostReceipt(w.tx, m.ComputerID, m.Revision, *root.Actor)
					if !ok || err != nil {
						t.Fatal(err)
					}
					prior.Key = strings.Repeat("b", 64)
					prior.Record.Result.ID = cfQAUUID(991)
					prior.Record.Result.SnapshotRevision = bump(m.Revision)
					prior.Record.Fingerprint = hostHash(prior.Record.Result)
					delta, err = sqliteInsertHostReceipt(w.tx, m.ComputerID, bump(m.Revision), prior)
					if err != nil {
						t.Fatal(err)
					}
				}
				next := metaQANext(t, m)
				next.Revision = bump(m.Revision)
				next.LogicalBytes += delta
				if err = sqliteUpdateMeta(w.tx, m, next); err != nil {
					t.Fatal(err)
				}
				w.end(t, true)
				changed, rows = ncQAAudit(t, q.f)
				callbackComplete = true
				ncQASetFSHooks(ncQAFSHooks{Observe: func(ev ncQAFSEvent) {
					if ev.Role == "journal" && ev.Op == "footprint" && ev.Phase == "after" {
						reachedFootprint = true
					}
				}})
			}
			r, err := q.service.ingestSQLite(context.Background(), e)
			ncQASetFSHooks(ncQAFSHooks{})
			if !callbackComplete || !reachedFootprint || ncQAErrorCode(err) != "state_busy" || !reflect.DeepEqual(r, EventResult{}) || q.clockCalls != 1 {
				t.Fatalf("prepared exact wait fact refusal callback=%t journalAfter=%t clock=%d err=%v", callbackComplete, reachedFootprint, q.clockCalls, err)
			}
			ncQAUnchanged(t, changed, rows, q.f)
		})
	}
}

func TestSQLiteNormalizedCaptureN07OriginalDeadlineExpiredDuringResolver(t *testing.T) {
	q := ncQANew(t, nil)
	entered, leaseAfter, beginAfter := false, false, false
	t.Cleanup(func() { ncQASetFSHooks(ncQAFSHooks{}); ncQASetSQLHooks(ncQASQLHooks{}) })
	q.resolverAction = func() {
		entered = true
		timer := time.NewTimer(275 * time.Millisecond)
		defer timer.Stop()
		<-timer.C
		ncQASetFSHooks(ncQAFSHooks{Observe: func(e ncQAFSEvent) {
			if e.Role == "root" && e.Op == "lease" && e.Phase == "acquired" {
				leaseAfter = true
			}
		}})
		ncQASetSQLHooks(ncQASQLHooks{Observe: func(e ncQASQLEvent) {
			if e.Operation == "begin" && e.Phase == "control-before-native" {
				beginAfter = true
			}
		}})
	}
	before, rows := ncQAAudit(t, q.f)
	start := time.Now()
	r, err := q.service.ingestSQLite(context.Background(), qaEvent("A", "1", "1", "work", qaBindingA))
	ncQASetFSHooks(ncQAFSHooks{})
	ncQASetSQLHooks(ncQASQLHooks{})
	if !entered || leaseAfter || beginAfter || ncQAErrorCode(err) != "state_busy" || !reflect.DeepEqual(r, EventResult{}) || q.clockCalls != 0 || q.resolverCalls != 1 {
		t.Fatalf("original deadline resolver entry=%t leaseAfter=%t beginAfter=%t elapsed=%s clock=%d resolver=%d err=%v", entered, leaseAfter, beginAfter, time.Since(start), q.clockCalls, q.resolverCalls, err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("bounded original deadline")
	}
	ncQAUnchanged(t, before, rows, q.f)
}

func TestSQLiteNormalizedCaptureN07CancellationAndWriterContention(t *testing.T) {
	q := ncQAWorking(t)
	before, rows := ncQAAudit(t, q.f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := q.service.ingestSQLite(ctx, qaEvent("A", "1", "2", "observe_work", ""))
	if ncQAErrorCode(err) != "state_busy" || !reflect.DeepEqual(r, EventResult{}) || q.clockCalls != 0 {
		t.Fatal("canceled admission", err)
	}
	ncQAUnchanged(t, before, rows, q.f)
	q.service.store.timeout = 20 * time.Millisecond
	w := ncQAOpen(t, q.f, sqliteio.Write)
	start := time.Now()
	r, err = q.service.ingestSQLite(context.Background(), qaEvent("A", "1", "2", "observe_work", ""))
	if ncQAErrorCode(err) != "state_busy" || !reflect.DeepEqual(r, EventResult{}) || time.Since(start) > time.Second {
		t.Fatal("writer contention did not use bounded single admission", err)
	}
	w.end(t, false)
	ncQAUnchanged(t, before, rows, q.f)
}

func TestSQLiteNormalizedCaptureN07ClockDeadlineAndActualStatementCancellation(t *testing.T) {
	for _, axis := range []string{"clock-expiry", "statement-cancel", "invalid-negative", "invalid-large"} {
		t.Run(axis, func(t *testing.T) {
			q := ncQAWorking(t)
			before, rows := ncQAAudit(t, q.f)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			armed, hit := false, false
			entered, leaseAfter, beginAfter := false, false, false
			switch axis {
			case "clock-expiry":
				t.Cleanup(func() { ncQASetFSHooks(ncQAFSHooks{}); ncQASetSQLHooks(ncQASQLHooks{}) })
				q.clockAction = func() {
					entered = true
					timer := time.NewTimer(275 * time.Millisecond)
					defer timer.Stop()
					<-timer.C
					ncQASetFSHooks(ncQAFSHooks{Observe: func(e ncQAFSEvent) {
						if e.Role == "root" && e.Op == "lease" && e.Phase == "acquired" {
							leaseAfter = true
						}
					}})
					ncQASetSQLHooks(ncQASQLHooks{Observe: func(e ncQASQLEvent) {
						if e.Operation == "begin" && e.Phase == "control-before-native" {
							beginAfter = true
						}
					}})
				}
			case "statement-cancel":
				ncQAHookPositive(t, q.f)
				q.clockAction = func() { armed = true }
				ncQAHooks(t, ncQASQLHooks{Observe: func(e ncQASQLEvent) {
					if e.Phase == "step-before-native" && e.Operation == "statement" && armed && !hit {
						hit = true
						cancel()
					}
				}})
			case "invalid-negative":
				q.service.store.timeout = -time.Nanosecond
			case "invalid-large":
				q.service.store.timeout = time.Second + time.Nanosecond
			}
			if axis == "clock-expiry" {
				ctx = context.Background()
			}
			start := time.Now()
			r, err := q.service.ingestSQLite(ctx, qaEvent("A", "1", "2", "observe_work", ""))
			ncQASetFSHooks(ncQAFSHooks{})
			ncQASetSQLHooks(ncQASQLHooks{})
			code := "state_busy"
			calls := 1
			if strings.HasPrefix(axis, "invalid-") {
				code = "validation"
				calls = 0
			}
			if ncQAErrorCode(err) != code || !reflect.DeepEqual(r, EventResult{}) || q.clockCalls != calls {
				t.Fatalf("single-deadline/cancellation public outcome entry=%t leaseAfter=%t beginAfter=%t elapsed=%s err=%v", entered, leaseAfter, beginAfter, time.Since(start), err)
			}
			if axis == "clock-expiry" && (!entered || leaseAfter || beginAfter || time.Since(start) > time.Second) {
				t.Fatalf("original clock deadline entry=%t leaseAfter=%t beginAfter=%t elapsed=%s", entered, leaseAfter, beginAfter, time.Since(start))
			}
			if axis == "statement-cancel" && !hit {
				t.Fatal("actual writer statement cancellation control not reached")
			}
			ncQAUnchanged(t, before, rows, q.f)
		})
	}
}

func TestSQLiteNormalizedCaptureN08SharedDiscontinuityAndWaitControls(t *testing.T) {
	for _, loss := range []string{"epoch", "elapsed", "awake", "wall", "unavailable"} {
		t.Run(loss, func(t *testing.T) {
			q := ncQANew(t, func(h *qaHarness) {
				for _, actor := range []string{"A", "B", "permission", "manual-wait", "manual-children"} {
					h.ingest(0, qaEvent(actor, "1", "1", "work", qaBindingA))
				}
				h.ingest(0, qaEvent("permission", "1", "2", "wait_permission", ""))
				h.ingest(0, qaEvent("manual-wait", "1", "2", "wait_user", ""))
				h.ingest(0, qaEvent("manual-children", "1", "2", "wait_children", ""))
			})
			e := qaEvent("A", "1", "2", "work", "")
			q.sample = ncQASample(10)
			switch loss {
			case "epoch":
				q.sample.Epoch = asQAPointer("new-boot")
			case "elapsed":
				q.sample.ElapsedNS = asQAPointer("0")
			case "awake":
				q.sample.AwakeNS = asQAPointer("0")
			case "wall":
				q.sample.WallUTC = q.sample.WallUTC.Add(time.Hour)
			case "unavailable":
				q.sample.Capability = "unavailable"
			}
			q.legacy.sample = q.sample
			q.legacy.clockErr = q.sampleErr
			want, wantErr := q.legacy.service.Ingest(context.Background(), e)
			got, gotErr := q.service.ingestSQLite(context.Background(), e)
			if ncQAErrorCode(wantErr) != ncQAErrorCode(gotErr) || q.clockCalls != 1 {
				t.Fatal("shared single sample outcome", wantErr, gotErr)
			}
			ws := bgQAReadLegacy(t, q.legacy.service)
			gs := ncQAReadState(t, q)
			if !reflect.DeepEqual(ncQACanonical(t, ws, ws), ncQACanonical(t, gs, gs)) || !reflect.DeepEqual(ncQACanonical(t, ws, want), ncQACanonical(t, gs, got)) {
				t.Fatal("shared quarantine/target alias/wait controls differ")
			}
			ncQAAudit(t, q.f)
		})
	}
}

func TestSQLiteNormalizedCaptureN08ClaudeExactRefTurnsPendingSetAndLatestProvenance(t *testing.T) {
	for _, axis := range []string{"pending-question", "stopped-historical", "nonwait-same-turn", "nonwait-other-turn", "no-pending"} {
		t.Run(axis, func(t *testing.T) {
			host := qaNewClaude(t)
			host.startSession()
			root := host.send(0, host.event("UserPromptSubmit", "prompt", ""))
			host.send(0, qaClaudeQuestion(host, "PreToolUse", "q"))
			if root.Actor == nil {
				t.Fatal("real host actor positive control")
			}
			st := bgQAReadLegacy(t, host.service)
			var question *hostTurn
			for _, turn := range st.HostTurns {
				if turn.Actor != nil && *turn.Actor == *root.Actor {
					question = turn
					break
				}
			}
			if question == nil || !hostWaiting(question) {
				t.Fatal("actual pending wait positive control")
			}
			switch axis {
			case "stopped-historical":
				question.Stopped = true
				for _, s := range st.HostSessions {
					s.ID = cfQAUUID(900)
					s.RootTurn = ""
				}
			case "nonwait-same-turn":
				question.Tools["ordinary"] = hostTool{Name: "Read", Phase: "pre"}
			case "nonwait-other-turn":
				copy := *question
				copy.TurnID = "other-turn"
				copy.Tools = map[string]hostTool{"ordinary": {Name: "Read", Phase: "pre"}}
				st.HostTurns[hostTurnKey(copy.Session, HostEvent{TurnID: copy.TurnID, AgentID: copy.AgentID})] = &copy
			case "no-pending":
				question.Tools["q"] = hostTool{Name: "AskUserQuestion", Phase: "post"}
			}
			st = hnQAValid(t, st)
			// Select by unsigned revision, then public ID. Use two real valid typed
			// provenance records with the same exact Ref, not current-session filters.
			var prior HostReceipt
			for _, r := range st.HostReceipts {
				if r.Result.Actor != nil && *r.Result.Actor == *root.Actor {
					n, _ := counter(r.Result.SnapshotRevision)
					p, _ := counter(prior.SnapshotRevision)
					if n > p || n == p && r.Result.ID > prior.ID {
						prior = r.Result
					}
				}
			}
			if prior.ID == "" {
				t.Fatal("actual latest wait provenance absent")
			}
			prior.ID = "ffffffff-ffff-4fff-8fff-ffffffffffff"
			st.HostReceipts[hostHash([]string{"qa-latest", prior.ID})] = hostReceiptRecord{Fingerprint: hostHash(prior), Result: prior}
			st = hnQAValid(t, st)
			q := ncQANew(t, func(h *qaHarness) {
				bgQALegacyMarshalOracle(t, h.service, h.path, st, true)
				h.bindings = map[string]BindingSnapshot{}
				for id, b := range st.Bindings {
					h.bindings[id] = b
				}
			})
			var binding BindingSnapshot
			for _, b := range st.Bindings {
				binding = b
				break
			}
			e := qaEvent("cross-project-observer", "1", "1", "work", binding.ID)
			e.Actor.ComputerID = st.ComputerID
			e.BindingRevision = binding.Revision
			q.sample = ncQASample(10)
			q.sample.Epoch = asQAPointer("new-boot")
			q.legacy.sample = q.sample
			want, we := q.legacy.service.Ingest(context.Background(), e)
			got, ge := q.service.ingestSQLite(context.Background(), e)
			if we != nil || ge != nil || q.clockCalls != 1 {
				t.Fatal("native wait shared-clock operation", we, ge)
			}
			ws := bgQAReadLegacy(t, q.legacy.service)
			gs := ncQAReadState(t, q)
			if !reflect.DeepEqual(ncQACanonical(t, ws, ws), ncQACanonical(t, gs, gs)) || !reflect.DeepEqual(ncQACanonical(t, ws, want), ncQACanonical(t, gs, got)) {
				t.Fatal("exact-Ref turn/pending/provenance differs from legacy store")
			}
			wait := gs.Actors[actorKey(root.Actor.Key)]
			witness := axis != "nonwait-same-turn" && axis != "no-pending"
			if witness && wait.Health != "stale" || !witness && wait.Health != "continuous" {
				t.Fatal("per-turn pending wait predicate")
			}
			loss := 0
			for _, r := range gs.HostReceipts {
				if r.Result.Kind == "ClockObservation" {
					loss++
					if r.Result.Actor == nil || *r.Result.Actor != *root.Actor || r.Result.SessionID != prior.SessionID || r.Result.TurnID != prior.TurnID || r.Result.DiagnosticCode != "source_loss_while_waiting" {
						t.Fatal("latest exact wait provenance lost")
					}
				}
			}
			if witness && loss != 1 || !witness && loss != 0 || len(wait.UncertaintyIDs) != 0 {
				t.Fatal("wait loss receipt/uncertainty cardinality")
			}
			ncQAAudit(t, q.f)
		})
	}
}

func TestSQLiteNormalizedCaptureN08CodexWaitChildrenRetainsActualProvenance(t *testing.T) {
	host := qaNewHost(t)
	host.startSession()
	root := host.send(0, host.event("UserPromptSubmit", "turn", ""))
	host.send(0, qaWaitEvent(host, "PreToolUse", "w", "wait_agent"))
	if root.Actor == nil {
		t.Fatal("actual Codex wait root")
	}
	qaHostState(t, host, root.Actor, "wait_children", "continuous")
	st := bgQAReadLegacy(t, host.service)
	q := ncQANew(t, func(h *qaHarness) {
		bgQALegacyMarshalOracle(t, h.service, h.path, st, true)
		h.bindings = map[string]BindingSnapshot{}
		for id, b := range st.Bindings {
			h.bindings[id] = b
		}
	})
	var binding BindingSnapshot
	for _, b := range st.Bindings {
		binding = b
		break
	}
	e := qaEvent("observer", "1", "1", "work", binding.ID)
	e.Actor.ComputerID = st.ComputerID
	e.BindingRevision = binding.Revision
	sample := ncQASample(10)
	sample.Epoch = asQAPointer("new-boot")
	ncQACompareSample(t, q, e, sample, "", 1)
	got := ncQAReadState(t, q)
	wait := got.Actors[actorKey(root.Actor.Key)]
	if wait.Health != "stale" || len(wait.UncertaintyIDs) != 0 {
		t.Fatal("known Codex wait loss became timing uncertainty")
	}
	loss := 0
	for _, r := range got.HostReceipts {
		if r.Result.Kind == "ClockObservation" && r.Result.Actor != nil && *r.Result.Actor == *root.Actor {
			loss++
			if r.Result.Source != "codex" || r.Result.SessionID != root.SessionID || r.Result.DiagnosticCode != "source_loss_while_waiting" {
				t.Fatal("Codex wait provenance lost")
			}
		}
	}
	if loss != 1 {
		t.Fatal("one shared wait-loss receipt required")
	}
}

func TestSQLiteNormalizedCaptureN08InitialWaitReceiptUsesCurrentCeiling(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprint(native), func(t *testing.T) {
			host := qaNewHost(t)
			host.startSession()
			root := host.send(0, host.event("UserPromptSubmit", "turn", ""))
			host.send(0, qaWaitEvent(host, "PreToolUse", "w", "wait_agent"))
			if root.Actor == nil {
				t.Fatal("real selected wait head prerequisite")
			}
			st := bgQAReadLegacy(t, host.service)
			q := ncQANew(t, func(h *qaHarness) {
				bgQALegacyMarshalOracle(t, h.service, h.path, st, true)
				h.bindings = map[string]BindingSnapshot{}
				for id, b := range st.Bindings {
					h.bindings[id] = b
				}
			})
			var binding BindingSnapshot
			for _, b := range st.Bindings {
				binding = b
				break
			}
			e := qaEvent("unrelated-observer", "1", "1", "work", binding.ID)
			e.Actor.ComputerID = st.ComputerID
			e.BindingRevision = binding.Revision
			q.service.nativeCaptureClock = native
			if !native {
				q.sample = ncQASample(10)
				q.sample.Epoch = asQAPointer("new-boot")
			}
			ncQAAudit(t, q.f) // valid current-revision wait provenance control
			w := ncQAOpen(t, q.f, sqliteio.Write)
			meta, err := sqliteReadMeta(w.tx, q.f.authority, q.f.database)
			if err != nil {
				t.Fatal(err)
			}
			latest, ok, err := sqliteLatestHostReceipt(w.tx, meta.ComputerID, meta.Revision, *root.Actor)
			if err != nil || !ok {
				t.Fatal("actual latest selected wait receipt", err)
			}
			latest.Record.Result.SnapshotRevision = bump(meta.Revision)
			latest.Record.Fingerprint = hostHash(latest.Record.Result)
			ncQADone(t, w.tx, "UPDATE host_receipts SET snapshot_revision=?,input_fingerprint=? WHERE receipt_key=?", interopCounter(t, latest.Record.Result.SnapshotRevision), sqliteio.Text(latest.Record.Fingerprint), sqliteio.Text(latest.Key))
			w.end(t, true)
			before, rows := ncQARawAudit(t, q.f)
			r, err := q.service.ingestSQLite(context.Background(), e)
			if ncQAErrorCode(err) != "state_corrupt" || !reflect.DeepEqual(r, EventResult{}) {
				t.Fatal("future initial selected wait provenance admitted", err)
			}
			after, got := ncQARawAudit(t, q.f)
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(rows, got) {
				t.Fatal("future selected wait refusal did not completely roll back")
			}
		})
	}
}

func TestSQLiteNormalizedCaptureN09GapUsesOneSampleMissingRangeAndOriginalRevision(t *testing.T) {
	for _, available := range []bool{true, false} {
		t.Run(fmt.Sprint(available), func(t *testing.T) {
			q := ncQAWorking(t)
			before, _ := ncQAAudit(t, q.f)
			e := qaEvent("A", "1", "4", "work", "")
			q.sample = ncQASample(10)
			if !available {
				q.sample.Capability = "unavailable"
			}
			q.legacy.sample = q.sample
			want, we := q.legacy.service.Ingest(context.Background(), e)
			got, ge := q.service.ingestSQLite(context.Background(), e)
			if ncQAErrorCode(we) != "event_gap" || ncQAErrorCode(ge) != "event_gap" || got.SnapshotRevision != before.Revision || q.clockCalls != 1 {
				t.Fatal("gap public outcome", we, ge)
			}
			ws := bgQAReadLegacy(t, q.legacy.service)
			gs := ncQAReadState(t, q)
			if !reflect.DeepEqual(ncQACanonical(t, ws, ws), ncQACanonical(t, gs, gs)) || !reflect.DeepEqual(ncQACanonical(t, ws, want), ncQACanonical(t, gs, got)) {
				t.Fatal("gap complete valid prefix differs")
			}
			a := gs.Actors[actorKey(e.Actor)]
			if a.Sequence != "1" || a.Health != "order_blocked" || len(a.UncertaintyIDs) != 1 {
				t.Fatal("gap high-water and membership")
			}
			u := gs.Uncertainties[a.UncertaintyIDs[0]]
			ev := gs.UncertaintyEvidence[u.ID]
			if ev.MissingFrom != "2" || ev.MissingThrough != "4" {
				t.Fatal("exact missing range")
			}
			if available && u.Reason != "event_gap" || !available && u.Reason != "restart_unknown" {
				t.Fatal("shared reason overwritten")
			}
			key, _ := ncQARawIdentity(t, e)
			if _, ok := gs.Receipts[key]; ok {
				t.Fatal("gap receipt invented")
			}
			if _, ok := gs.EventIDs[e.EventID]; ok {
				t.Fatal("gap ID inserted")
			}
			after, _ := ncQAAudit(t, q.f)
			if after.Revision != bump(before.Revision) {
				t.Fatal("gap increments public revision once")
			}
		})
	}
}

func TestSQLiteNormalizedCaptureN10CapsHistoricalDuplicateMembershipAndConfirmOrdinals(t *testing.T) {
	q := ncQANew(t, func(h *qaHarness) {
		h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
		h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
		h.at(20)
		_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "4", "work", ""))
		qaCode(t, err, "event_gap")
		err = h.service.store.update(context.Background(), func(st *state) (bool, error) {
			a := st.Actors[actorKey(qaEvent("A", "1", "1", "work", "").Actor)]
			a.UncertaintyIDs = append(a.UncertaintyIDs, a.UncertaintyIDs[0])
			return true, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	ncQACompare(t, q, qaEvent("A", "2", "1", "work", qaBindingA), 30, "", 1)
	st := ncQAReadState(t, q)
	a := st.Actors[actorKey(qaEvent("A", "1", "1", "work", "").Actor)]
	if len(a.UncertaintyIDs) != 2 || a.UncertaintyIDs[0] != a.UncertaintyIDs[1] {
		t.Fatal("historical duplicate memberships lost")
	}
	u := st.Uncertainties[a.UncertaintyIDs[0]]
	if u.Revision != "2" || u.UpperBound == nil || !u.UpperBound.Equal(qaEpochStart.Add(30*time.Second)) {
		t.Fatal("cap applied more than once or wrong projection")
	}
	ncQACompare(t, q, qaEvent("A", "2", "2", "observe_work", ""), 40, "", 1)
	ncQACompare(t, q, qaEvent("A", "2", "3", "wait_user", ""), 50, "", 1)
	st = ncQAReadState(t, q)
	for _, seg := range st.Segments {
		if seg.Actor.Generation == "2" {
			if len(seg.EventReferences) != 3 || seg.End == nil || !seg.End.Equal(seg.Confirmed) {
				t.Fatal("confirm ordinals or exact close boundary")
			}
		}
	}
}

func TestSQLiteNormalizedCaptureN11ChangedFalseVersusChangedClockDomainPrefixes(t *testing.T) {
	for _, kind := range []string{"clock_unavailable", "invalid_transition"} {
		for _, changed := range []bool{false, true} {
			t.Run(kind+"/"+fmt.Sprint(changed), func(t *testing.T) {
				q := ncQANew(t, func(h *qaHarness) {
					h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
					h.ingest(0, qaEvent("A", "1", "2", "wait_user", ""))
					if changed {
						h.ingest(10, qaEvent("B", "1", "1", "work", qaBindingA))
					}
				})
				e := qaEvent("A", "1", "3", "observe_work", "")
				q.sample = ncQASample(20)
				if kind == "clock_unavailable" {
					q.sample.Capability = "unavailable"
				} else if changed {
					q.sample.Epoch = asQAPointer("new-boot")
				}
				q.legacy.sample = q.sample
				before, rows := ncQAAudit(t, q.f)
				want, we := q.legacy.service.Ingest(context.Background(), e)
				got, ge := q.service.ingestSQLite(context.Background(), e)
				if ncQAErrorCode(we) != kind || ncQAErrorCode(ge) != kind || q.clockCalls != 1 {
					t.Fatal("domain prefix positive control", we, ge)
				}
				ws := bgQAReadLegacy(t, q.legacy.service)
				gs := ncQAReadState(t, q)
				if !reflect.DeepEqual(ncQACanonical(t, ws, ws), ncQACanonical(t, gs, gs)) || !reflect.DeepEqual(ncQACanonical(t, ws, want), ncQACanonical(t, gs, got)) {
					t.Fatal("actual store prefix retention differs")
				}
				if changed {
					after, _ := ncQAAudit(t, q.f)
					if after.Revision != bump(before.Revision) || got.SnapshotRevision != before.Revision {
						t.Fatal("Changed rejection public/stored revision")
					}
				} else {
					if !reflect.DeepEqual(got, EventResult{}) {
						t.Fatal("false Changed error result")
					}
					ncQAUnchanged(t, before, rows, q.f)
				}
			})
		}
	}
}

func TestSQLiteNormalizedCaptureN11EarlierHistoricalCapSurvivesLaterLowerBoundFailure(t *testing.T) {
	for _, newer := range []bool{false, true} {
		t.Run(fmt.Sprint(newer), func(t *testing.T) {
			q := ncQANew(t, func(h *qaHarness) {
				h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
				h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
				h.at(20)
				_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "4", "work", ""))
				qaCode(t, err, "event_gap")
				h.ingest(30, qaEvent("A", "2", "1", "work", qaBindingA))
				h.ingest(40, qaEvent("A", "2", "2", "observe_work", ""))
				h.at(50)
				_, err = h.service.Ingest(context.Background(), qaEvent("A", "2", "4", "work", ""))
				qaCode(t, err, "event_gap")
				if err = h.service.store.update(context.Background(), func(st *state) (bool, error) {
					a := st.Actors[actorKey(qaEvent("A", "1", "1", "work", "").Actor)]
					a.Health = "stale"
					for _, id := range a.UncertaintyIDs {
						u := st.Uncertainties[id]
						u.UpperBound = nil
						ev := st.UncertaintyEvidence[id]
						ev.BoundSample = nil
						st.UncertaintyEvidence[id] = ev
					}
					return true, nil
				}); err != nil {
					t.Fatal("complete valid historical cap fixture", err)
				}
			})
			before, _ := ncQAAudit(t, q.f)
			e := qaEvent("A", "2", "3", "work", "")
			if newer {
				e = qaEvent("A", "3", "1", "work", qaBindingA)
			}
			r := ncQACompare(t, q, e, 25, "clock_conflict", 1)
			if r.SnapshotRevision != before.Revision {
				t.Fatal("committed rejection must retain initial base revision")
			}
			st := ncQAReadState(t, q)
			a := st.Actors[actorKey(e.Actor)]
			if len(a.UncertaintyIDs) != 2 {
				t.Fatal("actual two historical cap witnesses")
			}
			first, second := st.Uncertainties[a.UncertaintyIDs[0]], st.Uncertainties[a.UncertaintyIDs[1]]
			if first.UpperBound == nil || !first.UpperBound.Equal(qaEpochStart.Add(25*time.Second)) || second.UpperBound != nil {
				t.Fatal("later cap failure erased earlier cap or applied failing cap")
			}
			after, _ := ncQAAudit(t, q.f)
			if after.Revision != bump(before.Revision) {
				t.Fatal("cap prefix revision charge")
			}
		})
	}
}

func TestSQLiteNormalizedCaptureN11ObserveWorkAndClosingConfirmFailureClockPredicates(t *testing.T) {
	for _, kind := range []string{"observe_work", "work", "finish"} {
		for _, changed := range []bool{false, true} {
			t.Run(kind+"/"+fmt.Sprint(changed), func(t *testing.T) {
				q := ncQANew(t, func(h *qaHarness) {
					h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
					h.ingest(10, qaEvent("A", "1", "2", "observe_work", ""))
					if changed {
						h.ingest(10, qaEvent("B", "1", "1", "work", qaBindingB))
					}
					if err := h.service.store.update(context.Background(), func(st *state) (bool, error) {
						// Actual whole validators allow independently retained evidence.
						// Target's last evidence is compatible, while its confirmed prefix
						// is later than the new projected end. This isolates confirm failure.
						st.Actors[actorKey(qaEvent("A", "1", "1", "work", "").Actor)].LastEvidence = ncQASample(0)
						if changed {
							b := st.Actors[actorKey(qaEvent("B", "1", "1", "work", "").Actor)]
							b.LastEvidence = ncQASample(0)
							b.LastEvidence.Epoch = asQAPointer("other-observed-boot")
						}
						return true, nil
					}); err != nil {
						t.Fatal("complete valid confirm failure fixture", err)
					}
				})
				before, rows := ncQAAudit(t, q.f)
				r := ncQACompare(t, q, qaEvent("A", "1", "3", kind, ""), 5, "clock_conflict", 1)
				if changed {
					after, _ := ncQAAudit(t, q.f)
					if after.Revision != bump(before.Revision) || r.SnapshotRevision != before.Revision {
						t.Fatal("confirm Changed prefix revision")
					}
				} else {
					if !reflect.DeepEqual(r, EventResult{}) {
						t.Fatal("false Changed confirm error result")
					}
					ncQAUnchanged(t, before, rows, q.f)
				}
			})
		}
	}
}

func TestSQLiteNormalizedCaptureN10N11LatestEndOpenFailureRetainsEpochOnlyWithChangedPrefix(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			q := ncQAWorking(t)
			ncQACompare(t, q, qaEvent("A", "1", "2", "finish", ""), 20, "", 1)
			if changed {
				b := q.legacy.bindings[qaBindingB]
				b.Attribution.ProjectID = "9"
				q.legacy.bindings[qaBindingB] = b
				if err := q.legacy.service.store.update(context.Background(), func(st *state) (bool, error) { st.Bindings[qaBindingB] = b; return true, nil }); err != nil {
					t.Fatal(err)
				}
				w := ncQAOpen(t, q.f, sqliteio.Write)
				m, err := sqliteReadMeta(w.tx, q.f.authority, q.f.database)
				if err != nil {
					t.Fatal(err)
				}
				before, ok, err := sqliteReadBinding(w.tx, m.ComputerID, qaBindingB)
				if !ok || err != nil {
					t.Fatal(err)
				}
				after := before
				after.Snapshot = b
				d, err := sqliteUpdateBinding(w.tx, before, after)
				if err != nil {
					t.Fatal(err)
				}
				next := metaQANext(t, m)
				next.Revision = bump(m.Revision)
				next.LogicalBytes += d
				if err = sqliteUpdateMeta(w.tx, m, next); err != nil {
					t.Fatal(err)
				}
				w.end(t, true)
				ncQACompare(t, q, qaEvent("other-timer", "1", "1", "work", qaBindingB), 30, "", 1)
			}
			before, rows := ncQAAudit(t, q.f)
			oldState := ncQAReadState(t, q)
			e := qaEvent("new-head", "1", "1", "work", qaBindingA)
			sample := ncQASample(10)
			sample.Epoch = asQAPointer("new-boot")
			r := ncQACompareSample(t, q, e, sample, "clock_conflict", 1)
			if changed {
				st := ncQAReadState(t, q)
				if len(st.Epochs) != len(oldState.Epochs)+1 || st.Actors[actorKey(e.Actor)] != nil || r.SnapshotRevision != before.Revision {
					t.Fatal("appended epoch/premature head prefix policy")
				}
				h := ncQAOpen(t, q.f, sqliteio.Read)
				if len(ncQAStrings(t, h.tx, "SELECT actor_key FROM actor_generations WHERE actor_key=?", sqliteio.Text(actorKey(e.Actor)))) != 0 {
					t.Fatal("failed open invented generation identity")
				}
				h.end(t, false)
			} else {
				ncQAUnchanged(t, before, rows, q.f)
			}
		})
	}
	// The strict immutable End guard allows equality; incompatible reservation
	// endpoint equality has a separate N05 literal witness.
	q := ncQAWorking(t)
	ncQACompare(t, q, qaEvent("A", "1", "2", "finish", ""), 20, "", 1)
	ncQACompare(t, q, qaEvent("B", "1", "1", "work", qaBindingA), 20, "", 1)
}

func TestSQLiteNormalizedCaptureN11N14StaleExistingAndNewerOpenPrefixesNegativeReplacement(t *testing.T) {
	for _, newer := range []bool{false, true} {
		t.Run(fmt.Sprint(newer), func(t *testing.T) {
			q := ncQAWorking(t)
			ncQACompare(t, q, qaEvent("A", "1", "2", "finish", ""), 20, "", 1)
			ncQACompare(t, q, qaEvent("A", "2", "1", "work", qaBindingA), 30, "", 1)
			key := qaEvent("A", "1", "1", "work", "").Actor
			if err := q.legacy.service.store.update(context.Background(), func(st *state) (bool, error) { st.Actors[actorKey(key)].Health = "stale"; return true, nil }); err != nil {
				t.Fatal("actual whole stale fixture", err)
			}
			w := ncQAOpen(t, q.f, sqliteio.Write)
			m, err := sqliteReadMeta(w.tx, q.f.authority, q.f.database)
			if err != nil {
				t.Fatal(err)
			}
			before, ok, err := sqliteReadActorLocal(w.tx, m.ComputerID, key)
			if !ok || err != nil {
				t.Fatal(err)
			}
			after := before
			after.Health = "stale"
			delta, err := sqliteWriteActorLocal(w.tx, m.ComputerID, &before, after)
			if err != nil || delta != -5 {
				t.Fatal("real signed TEXT replacement delta", delta, err)
			}
			next := metaQANext(t, m)
			next.Revision = bump(m.Revision)
			next.LogicalBytes += delta
			if err = sqliteUpdateMeta(w.tx, m, next); err != nil {
				t.Fatal(err)
			}
			w.end(t, true)
			old := ncQAReadState(t, q)
			meta, _ := ncQAAudit(t, q.f)
			e := qaEvent("A", "2", "2", "work", "")
			if newer {
				e = qaEvent("A", "3", "1", "work", qaBindingA)
			}
			sample := ncQASample(10)
			sample.Epoch = asQAPointer("new-boot")
			r := ncQACompareSample(t, q, e, sample, "clock_conflict", 1)
			st := ncQAReadState(t, q)
			a := st.Actors[actorKey(key)]
			if len(st.Epochs) != len(old.Epochs)+1 || a.Ref.Generation != "2" || a.Sequence != "1" || r.SnapshotRevision != meta.Revision {
				t.Fatal("source stale/newer open prefix predicate")
			}
		})
	}
}

func TestSQLiteNormalizedCaptureN11FinalizationConflictAfterPriorSealExactChangedPolicy(t *testing.T) {
	for _, clockChanged := range []bool{false, true} {
		t.Run(fmt.Sprint(clockChanged), func(t *testing.T) {
			a := cfQAAttr()
			b := a
			b.TaskID = "9"
			st := cfQAState(t, cfQASpec{10, cfQATime(0), cfQATime(10), a}, cfQASpec{20, cfQATime(9), cfQATime(20), b})
			if clockChanged {
				other := a
				other.ProjectID = "9"
				extra := cfQAState(t, cfQASpec{99, cfQATime(30), cfQATime(30), other})
				seg := extra.Segments[cfQAUUID(99)]
				seg.End = nil
				st.Epochs = append(st.Epochs, extra.Epochs...)
				st.Segments[seg.ID] = seg
				id := seg.ID
				st.Actors[actorKey(seg.Actor.Key)] = &Actor{ID: cfQAUUID(199), Revision: "1", Ref: seg.Actor, Sequence: "1", State: "working", Health: "continuous", BindingID: qaBindingA, BindingRevision: "1", Attribution: other, SegmentID: &id, LastEvidence: seg.ConfirmedSample, UncertaintyIDs: []string{}}
			}
			st = hnQAValid(t, st)
			q := ncQANew(t, func(h *qaHarness) { bgQALegacyMarshalOracle(t, h.service, h.path, st, true) })
			before, rows := ncQAAudit(t, q.f)
			e := qaEvent("new-head", "1", "1", "work", qaBindingA)
			r := ncQACompare(t, q, e, 40, "clock_conflict", 1)
			if clockChanged {
				gs := ncQAReadState(t, q)
				if len(gs.Intervals) != 1 || len(gs.Outbox) != 1 || r.SnapshotRevision != before.Revision {
					t.Fatal("valid earlier seal prefix not retained")
				}
			} else {
				if !reflect.DeepEqual(r, EventResult{}) {
					t.Fatal("false Changed finalization error result")
				}
				ncQAUnchanged(t, before, rows, q.f)
			}
		})
	}
}

func TestSQLiteNormalizedCaptureN12StagedFamilyNativeConstraintsRollBackCompleteReducer(t *testing.T) {
	for _, table := range []string{"segments", "segment_events", "actors", "event_receipts", "event_receipt_uncertainties", "event_ids", "intervals", "interval_segments", "interval_components", "outbox"} {
		t.Run(table, func(t *testing.T) {
			prepare := func(h *qaHarness) {
				h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
				if table == "event_receipt_uncertainties" {
					h.at(5)
					_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "3", "work", ""))
					qaCode(t, err, "event_gap")
				}
			}
			q := ncQANew(t, prepare)
			ncQAHookPositive(t, q.f)
			e := qaEvent("A", "1", "2", "finish", "")
			if table == "event_receipt_uncertainties" {
				e = qaEvent("A", "2", "1", "work", qaBindingA)
			}
			// Exact positive direct reducer control first, with the same event/graph.
			control := ncQANew(t, prepare)
			cw, cm, positive, err := ncQAReduceFixture(t, control, e, nil)
			if err != nil || !positive.Changed || positive.OperationError != nil {
				t.Fatal("otherwise valid direct reducer control", err, positive.OperationError)
			}
			if err = sqliteValidateSelectedCaptureDependencies(cw.tx, cm.ComputerID, bump(cm.Revision), positive.Dependencies); err != nil {
				t.Fatal(err)
			}
			if err = sqliteValidateSelectedFinalization(cw.tx, cm.ComputerID, bump(cm.Revision), positive.Finalization); err != nil {
				t.Fatal(err)
			}
			_, charge := cfQAAudit(t, cw.tx, cm)
			if charge != cm.LogicalBytes+positive.Delta {
				t.Fatal("positive complete transition literal charge")
			}
			cw.end(t, false)
			before, rows := ncQAAudit(t, q.f)
			w, _, _, err := ncQAReduceFixture(t, q, e, func(tx *sqliteio.Tx) {
				// Temporary real DDL is installed AFTER exact catalog admission, on
				// the actual owned writer, BEFORE reduction. Never in a native hook.
				ncQADone(t, tx, "CREATE TRIGGER nc_qa_fault BEFORE INSERT ON "+table+" BEGIN SELECT RAISE(ABORT,'synthetic native constraint'); END")
				if table == "actors" || table == "segments" {
					ncQADone(t, tx, "CREATE TRIGGER nc_qa_update_fault BEFORE UPDATE ON "+table+" BEGIN SELECT RAISE(ABORT,'synthetic native constraint'); END")
				}
			})
			var native *sqliteio.Error
			if !errors.As(err, &native) || native.Code&255 != 19 {
				t.Fatal("real SQLite staged family constraint not reached", err)
			}
			interopSafeError(t, err, "synthetic native constraint", q.f.directory)
			w.end(t, false)
			ncQAUnchanged(t, before, rows, q.f)
		})
	}
}

func TestSQLiteNormalizedCaptureN12CompatibleUnionBridgeReservationReleaseAndQueuedRoot(t *testing.T) {
	q := ncQAWorking(t)
	ncQACompare(t, q, qaEvent("B", "1", "1", "work", qaBindingB), 5, "", 1)
	ncQACompare(t, q, qaEvent("A", "1", "2", "finish", ""), 10, "", 1)
	h := ncQAOpen(t, q.f, sqliteio.Read)
	if len(ncQAStrings(t, h.tx, "SELECT component_id FROM union_frontier")) != 1 || len(ncQAStrings(t, h.tx, "SELECT component_id FROM pending_finalization")) != 0 || len(ncQAStrings(t, h.tx, "SELECT interval_id FROM intervals")) != 0 {
		t.Fatal("remaining inclusive working reservation did not clear pending/fence seal")
	}
	h.end(t, false)
	ncQACompare(t, q, qaEvent("B", "1", "2", "finish", ""), 15, "", 1)
	st := ncQAReadState(t, q)
	if len(st.Intervals) != 1 || len(st.Intervals[0].SegmentIDs) != 2 || st.Intervals[0].DurationNS != "15000000000" || len(st.Outbox) != 1 {
		t.Fatal("bridged supports did not produce one immutable union/root")
	}
	root := st.Outbox[st.Intervals[0].ID]
	if root.State != "queued" || root.Revision != "1" || root.Correlation != "tempo:"+st.Intervals[0].ID || root.ID == st.Intervals[0].ID {
		t.Fatal("initial queued root identity")
	}
	h = ncQAOpen(t, q.f, sqliteio.Read)
	for _, table := range []string{"union_frontier", "pending_finalization", "component_segments"} {
		if len(ncQAStrings(t, h.tx, "SELECT component_id FROM "+table)) != 0 {
			t.Fatal("consumed live support/pending owner retained", table)
		}
	}
	if len(ncQAStrings(t, h.tx, "SELECT component_id FROM interval_components")) != 1 {
		t.Fatal("retained singular seal")
	}
	h.end(t, false)
}

func TestSQLiteNormalizedCaptureN12DeferredFKFailsRealCommitAfterReceiptStage(t *testing.T) {
	q := ncQAWorking(t)
	ncQAHookPositive(t, q.f)
	before, rows := ncQAAudit(t, q.f)
	w, m, transition, err := ncQAReduceFixture(t, q, qaEvent("A", "1", "2", "observe_work", ""), func(tx *sqliteio.Tx) {
		ncQADone(t, tx, "CREATE TABLE nc_qa_deferred(value TEXT PRIMARY KEY, peer TEXT REFERENCES nc_qa_deferred(value) DEFERRABLE INITIALLY DEFERRED) STRICT")
		ncQADone(t, tx, "CREATE TRIGGER nc_qa_missing_peer AFTER INSERT ON event_receipts BEGIN INSERT INTO nc_qa_deferred VALUES('staged','absent'); END")
	})
	if err != nil || !transition.Changed || transition.OperationError != nil {
		t.Fatal("deferred violation did not survive actual valid staging", err)
	}
	if err = sqliteValidateSelectedCaptureDependencies(w.tx, m.ComputerID, bump(m.Revision), transition.Dependencies); err != nil {
		t.Fatal("real selected dependencies before COMMIT", err)
	}
	if err = sqliteValidateSelectedFinalization(w.tx, m.ComputerID, bump(m.Revision), transition.Finalization); err != nil {
		t.Fatal("real selected graph before COMMIT", err)
	}
	if len(ncQAStrings(t, w.tx, "SELECT value FROM nc_qa_deferred WHERE value='staged'")) != 1 {
		t.Fatal("actual missing-peer staging absent")
	}
	next := metaQANext(t, m)
	next.Revision = bump(m.Revision)
	next.LogicalBytes += transition.Delta
	if err = sqliteUpdateMeta(w.tx, m, next); err != nil {
		t.Fatal(err)
	}
	out, commitErr := w.tx.Commit()
	var native *sqliteio.Error
	if out != sqliteio.Unknown || !errors.As(commitErr, &native) || native.Code&255 != 19 {
		t.Fatalf("actual deferred COMMIT outcome=%v error=%v", out, commitErr)
	}
	if cleanup := w.tx.Rollback(); cleanup != nil {
		var checked *sqliteio.Error
		if !errors.As(cleanup, &checked) || checked.Phase != sqliteio.FinalizePhase || checked.Code&255 != 19 {
			t.Fatal("unexpected checked failed-COMMIT cleanup", cleanup)
		}
	}
	w.ended = true
	if err = w.c.Close(context.Background()); err != nil {
		t.Fatal("actual constraint connection closure", err)
	}
	w.closed = true
	ncQAUnchanged(t, before, rows, q.f)
	// Direct reducer scope proves native deferred failure/rollback; the public
	// Unknown -> zero/local_write_unknown mapping has separate N15 hook witnesses.
}

func TestSQLiteNormalizedCaptureN13RawFingerprintIdentityAndOwnedResults(t *testing.T) {
	q := ncQANew(t, nil)
	e := qaEvent("A", "1", "1", "work", qaBindingA)
	e.EventID = "raw-ID-" + string([]byte{0xff})
	key, fp := ncQARawIdentity(t, e)
	ncQACompare(t, q, e, 0, "", 1)
	h := ncQAOpen(t, q.f, sqliteio.Read)
	row, ok, err := sqliteReadEventReceipt(h.tx, key, "2")
	if !ok || err != nil || row.Value.Fingerprint != fp {
		t.Fatal("raw fingerprint repaired before persistence", err)
	}
	h.end(t, false)
	// Stable raw key with repaired EventID retries by exact receipt first.
	r, err := q.service.ingestSQLite(context.Background(), e)
	if err != nil || r.Disposition != "duplicate" {
		t.Fatal("raw exact retry", err)
	}
	if r.SegmentID == nil || r.UncertaintyIDs == nil {
		t.Fatal("owned result shape")
	}
	*r.SegmentID = qaBindingB
	r.UncertaintyIDs = append(r.UncertaintyIDs, qaBindingB)
	r, err = q.service.ingestSQLite(context.Background(), e)
	if err != nil || r.SegmentID == nil || *r.SegmentID == qaBindingB || len(r.UncertaintyIDs) != 0 {
		t.Fatal("caller mutated retained receipt", err)
	}
	q2 := ncQANew(t, nil)
	bad := qaEvent("raw-"+string([]byte{0xff}), "1", "1", "work", qaBindingA)
	before, rows := ncQAAudit(t, q2.f)
	r, err = q2.service.ingestSQLite(context.Background(), bad)
	if err == nil || !reflect.DeepEqual(r, EventResult{}) {
		t.Fatal("raw key drift committed invalid graph", err)
	}
	ncQAUnchanged(t, before, rows, q2.f)
}

func TestSQLiteNormalizedCaptureN13OwnedPreparedSampleAndRetainedNonemptyChildren(t *testing.T) {
	q := ncQAWorking(t)
	q.sample = ncQASample(10)
	e := qaEvent("A", "1", "2", "observe_work", "")
	_, clock, found, err := q.service.sqlitePrepareCapture(context.Background(), e, ncQAAdmission(q.f, time.Now().Add(time.Second)))
	if err != nil || !found || clock.Prepared == nil || clock.Prepared.Value.Epoch == nil || q.clockCalls != 1 {
		t.Fatal("owned injected slot positive fixture", err)
	}
	*q.sample.Epoch = "caller-mutated-epoch"
	*q.sample.ElapsedNS = "999"
	*q.sample.AwakeNS = "888"
	if *clock.Prepared.Value.Epoch != "boot-1" || *clock.Prepared.Value.ElapsedNS != "10000000000" || *clock.Prepared.Value.AwakeNS != "10000000000" {
		t.Fatal("prepared sample retained callback-owned pointers")
	}
	q2 := ncQANew(t, func(h *qaHarness) {
		h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
		h.at(5)
		_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "3", "work", ""))
		qaCode(t, err, "event_gap")
	})
	e = qaEvent("A", "2", "1", "work", qaBindingA)
	r := ncQACompare(t, q2, e, 10, "", 1)
	if len(r.UncertaintyIDs) != 1 {
		t.Fatal("nonempty child ownership positive fixture")
	}
	original := r.UncertaintyIDs[0]
	r.UncertaintyIDs[0] = qaBindingB
	again, err := q2.service.ingestSQLite(context.Background(), e)
	if err != nil || len(again.UncertaintyIDs) != 1 || again.UncertaintyIDs[0] != original {
		t.Fatal("caller mutated retained nonempty receipt array", err)
	}
	raw := ncQASample(20)
	raw.Epoch = asQAPointer("raw-boot-" + string([]byte{0xff}))
	ncQACompareSample(t, q2, qaEvent("A", "2", "2", "work", ""), raw, "", 1)
}

func TestSQLiteNormalizedCaptureN13InjectedBindingDoesNotInventUTF8OrCWDRefusal(t *testing.T) {
	q := ncQANew(t, nil)
	e := qaEvent("A", "1", "1", "work", qaBindingA)
	e.CWD = "/synthetic/" + strings.Repeat("雪", 100) + string([]byte{0xff})
	if err := validateEvent(e); err != nil {
		t.Fatal("actual raw syntactic positive fixture", err)
	}
	ncQACompare(t, q, e, 0, "", 1)
	if q.resolverCalls != 1 {
		t.Fatal("accepted resolver routing")
	}
}

func TestSQLiteNormalizedCaptureN14MaximumRevisionUnchangedSampledDomainErrors(t *testing.T) {
	for _, code := range []string{"clock_unavailable", "invalid_transition"} {
		t.Run(code, func(t *testing.T) {
			q := ncQANew(t, func(h *qaHarness) {
				h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
				h.ingest(0, qaEvent("A", "1", "2", "wait_user", ""))
			})
			w := ncQAOpen(t, q.f, sqliteio.Write)
			meta, err := sqliteReadMeta(w.tx, q.f.authority, q.f.database)
			if err != nil {
				t.Fatal(err)
			}
			next := metaQANext(t, meta)
			next.Revision = asQAMax
			ncQAStageMetaBoundary(t, w.tx, q.f, meta, next)
			w.end(t, true)
			q.sample = ncQASample(20)
			if code == "clock_unavailable" {
				q.sample.Capability = "unavailable"
			}
			before, rows := ncQAAudit(t, q.f)
			r, err := q.service.ingestSQLite(context.Background(), qaEvent("A", "1", "3", "observe_work", ""))
			if ncQAErrorCode(err) != code || !reflect.DeepEqual(r, EventResult{}) || q.clockCalls != 1 {
				t.Fatal("unchanged sampled domain error overridden by overflow", err)
			}
			ncQAUnchanged(t, before, rows, q.f)
		})
	}
}

func TestSQLiteNormalizedCaptureN14PublicRevisionOverflowReplayAndChange(t *testing.T) {
	q := ncQAWorking(t)
	w := ncQAOpen(t, q.f, sqliteio.Write)
	m, err := sqliteReadMeta(w.tx, q.f.authority, q.f.database)
	if err != nil {
		t.Fatal(err)
	}
	next := metaQANext(t, m)
	next.Revision = asQAMax
	ncQAStageMetaBoundary(t, w.tx, q.f, m, next)
	w.end(t, true)
	before, rows := ncQAAudit(t, q.f)
	r, err := q.service.ingestSQLite(context.Background(), qaEvent("A", "1", "1", "work", qaBindingA))
	if err != nil || r.Disposition != "duplicate" {
		t.Fatal("overflow unchanged replay refused", err)
	}
	ncQANonceOnly(t, before, rows, q.f)
	before, rows = ncQAAudit(t, q.f)
	r, err = q.service.ingestSQLite(context.Background(), qaEvent("A", "1", "2", "observe_work", ""))
	if ncQAErrorCode(err) != "validation" || !reflect.DeepEqual(r, EventResult{}) {
		t.Fatal("public revision overflow wrapped/acknowledged", err)
	}
	ncQAUnchanged(t, before, rows, q.f)
}

func TestSQLiteNormalizedCaptureN15CheckedErrorTreeAndCategoryPrecedence(t *testing.T) {
	cases := []struct {
		name             string
		primary, cleanup *sqliteio.Error
		want             string
		capacity         bool
	}{
		{"busy-enospc", &sqliteio.Error{Phase: sqliteio.VerifyPhase, Category: sqliteio.Busy, Cause: syscall.ENOSPC}, &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO, Cause: syscall.EIO}, "state_busy", false},
		{"canceled-enospc", &sqliteio.Error{Phase: sqliteio.VerifyPhase, Category: sqliteio.Canceled, Cause: syscall.ENOSPC}, &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO, Cause: syscall.EIO}, "state_busy", false},
		{"cleanup-enospc", &sqliteio.Error{Phase: sqliteio.VerifyPhase, Category: sqliteio.IO, Cause: syscall.EIO}, &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO, Cause: syscall.ENOSPC}, "state_corrupt", false},
		{"bare-enospc", &sqliteio.Error{Phase: sqliteio.VerifyPhase, Category: sqliteio.IO, Cause: syscall.ENOSPC}, &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO, Cause: syscall.EIO}, "validation", true},
		{"native-full", &sqliteio.Error{Phase: sqliteio.VerifyPhase, Category: sqliteio.Full, Code: 13}, &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO, Cause: syscall.EIO}, "validation", true},
		{"generic-io", &sqliteio.Error{Phase: sqliteio.VerifyPhase, Category: sqliteio.IO, Cause: syscall.EIO}, &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO, Cause: syscall.EINVAL}, "state_corrupt", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := sqliteCaptureError(errors.Join(tc.primary, tc.cleanup))
			if !errors.Is(err, tc.primary) || !errors.Is(err, tc.cleanup) {
				t.Fatal("checked primary/cleanup native siblings dropped", err)
			}
			if tc.primary.Cause != nil && !errors.Is(err, tc.primary.Cause) || tc.cleanup.Cause != nil && !errors.Is(err, tc.cleanup.Cause) {
				t.Fatal("checked primary/cleanup causes dropped", err)
			}
			var domain *Error
			if !errors.As(err, &domain) || domain.Code != tc.want {
				t.Fatal("safe domain precedence", err)
			}
			_, capacity := domain.Details["reason"]
			if capacity != tc.capacity {
				t.Fatal("capacity classification", err)
			}
		})
	}
	cleanup := &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.IO, Cause: syscall.ENOSPC}
	err := sqliteCaptureError(errors.Join(context.Canceled, cleanup))
	if ncQAErrorCode(err) != "state_busy" || !errors.Is(err, context.Canceled) || !errors.Is(err, cleanup) {
		t.Fatal("canceled primary lost to cleanup ENOSPC", err)
	}
}

func TestSQLiteNormalizedCaptureN15CommitAndDurableClosureFaultsReturnZeroThenExactRetry(t *testing.T) {
	// Phases are literal e1 fresh-native source hooks; integration is a pinned
	// prerequisite. They are not fabricated native return codes or power-loss proof.
	for _, fault := range []struct {
		phase, op string
		committed bool
	}{
		{"commit-before-dispatch", "commit", false},
		{"commit-after-engine", "commit", true},
		{"commit-before-verify", "commit", true},
		{"close-before", "close", true},
		{"durable-native-closed", "close-durably", true},
		{"fsync-before", "durable-main", true},
		{"fsync-after", "durable-main", true},
		{"fsync-before", "durable-wal", true},
		{"fsync-before", "durable-parent", true},
		{"durable-release-before", "close-durably", true},
	} {
		t.Run(fault.phase+"/"+fault.op, func(t *testing.T) {
			q := ncQAWorking(t)
			ncQACommitPositive(t, q.f)
			before, rows := ncQAAudit(t, q.f)
			hit, committedSeen, armed := false, false, false
			q.sample = ncQASample(10)
			q.clockAction = func() { armed = true }
			ncQAHooks(t, ncQASQLHooks{Observe: func(e ncQASQLEvent) {
				if armed && e.Phase == "commit-after-engine" && e.Operation == "commit" && e.Code == 101 {
					committedSeen = true
				}
			}, Fault: func(e ncQASQLEvent) error {
				if armed && !hit && e.Phase == fault.phase && e.Operation == fault.op && (fault.op != "close" || committedSeen) {
					hit = true
					return errors.New("synthetic durable secret")
				}
				return nil
			}})
			e := qaEvent("A", "1", "2", "observe_work", "")
			r, err := q.service.ingestSQLite(context.Background(), e)
			ncQASetSQLHooks(ncQASQLHooks{})
			q.clockAction = nil
			if !armed || q.clockCalls != 1 || !hit || err == nil || !reflect.DeepEqual(r, EventResult{}) {
				t.Fatal("native writer fault positive branch not reached or acknowledged", err)
			}
			if fault.committed && ncQAErrorCode(err) != "local_write_unknown" {
				t.Fatal("post-dispatch uncertainty lost", err)
			}
			interopSafeError(t, err, "synthetic durable secret", q.f.directory)
			if !fault.committed {
				ncQAUnchanged(t, before, rows, q.f)
			} else {
				after, _ := ncQAAudit(t, q.f)
				if after.Revision != bump(before.Revision) {
					t.Fatal("committed historical result absent")
				}
			}
			r, err = q.service.ingestSQLite(context.Background(), e)
			if err != nil {
				t.Fatal("exact retry successful fence", err)
			}
			if fault.committed && r.Disposition != "duplicate" || !fault.committed && r.Disposition != "applied" {
				t.Fatal("retry identity misclassified")
			}
			ncQAAudit(t, q.f)
		})
	}
}

func TestSQLiteNormalizedCaptureN15CancellationAfterActualCommitReturnsZero(t *testing.T) {
	q := ncQAWorking(t)
	ncQACommitPositive(t, q.f)
	before, _ := ncQAAudit(t, q.f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hit, armed := false, false
	q.sample = ncQASample(10)
	q.clockAction = func() { armed = true }
	ncQAHooks(t, ncQASQLHooks{Observe: func(e ncQASQLEvent) {
		if armed && e.Phase == "commit-after-verify" && e.Operation == "commit" && e.Code == 101 {
			hit = true
			cancel()
		}
	}})
	e := qaEvent("A", "1", "2", "observe_work", "")
	r, err := q.service.ingestSQLite(ctx, e)
	ncQASetSQLHooks(ncQASQLHooks{})
	q.clockAction = nil
	if !armed || q.clockCalls != 1 || !hit || ncQAErrorCode(err) != "local_write_unknown" || !reflect.DeepEqual(r, EventResult{}) {
		t.Fatal("post-writer-COMMIT cancellation acknowledged", err)
	}
	after, rows := ncQAAudit(t, q.f)
	if after.Revision != bump(before.Revision) {
		t.Fatal("actual committed receipt not retained")
	}
	r, err = q.service.ingestSQLite(context.Background(), e)
	if err != nil || r.Disposition != "duplicate" {
		t.Fatal("post-cancel original retry", err)
	}
	ncQANonceOnly(t, after, rows, q.f)
}

func TestSQLiteNormalizedCaptureN15HistoricalReplayFenceFailureNeverClaimsNoncommit(t *testing.T) {
	q := ncQAWorking(t)
	ncQACommitPositive(t, q.f)
	e := qaEvent("A", "1", "1", "work", qaBindingA)
	before, rows := ncQAAudit(t, q.f)
	hit, armed := false, false
	ncQAHooks(t, ncQASQLHooks{Observe: func(e ncQASQLEvent) {
		if e.Phase == "close-after" && e.Operation == "close" {
			armed = true
		}
	}, Fault: func(e ncQASQLEvent) error {
		if armed && e.Phase == "commit-before-dispatch" && e.Operation == "commit" {
			hit = true
			return errors.New("synthetic fence refusal")
		}
		return nil
	}})
	r, err := q.service.ingestSQLite(context.Background(), e)
	ncQASetSQLHooks(ncQASQLHooks{})
	if !armed || !hit || ncQAErrorCode(err) != "local_write_unknown" || !reflect.DeepEqual(r, EventResult{}) {
		t.Fatal("historical writer fence refusal claimed old noncommit", err)
	}
	ncQAUnchanged(t, before, rows, q.f)
	r, err = q.service.ingestSQLite(context.Background(), e)
	if err != nil || r.Disposition != "duplicate" {
		t.Fatal("original identity lost", err)
	}
	ncQANonceOnly(t, before, rows, q.f)
}

func TestSQLiteNormalizedCaptureN15HistoricalReceiptWriterAdmissionBusyIsUnknown(t *testing.T) {
	q := ncQAWorking(t)
	ncQAHookPositive(t, q.f)
	e := qaEvent("A", "1", "1", "work", qaBindingA)
	p, clock, found, err := q.service.sqlitePrepareCapture(context.Background(), e, ncQAAdmission(q.f, time.Now().Add(time.Second)))
	if err != nil || !found || p.Receipt == nil || p.Receipt.Value.Result.Disposition != "applied" || clock.Site != sqliteCaptureClockNone {
		t.Fatal("actual exact receipt/read positive control", err)
	}
	before, rows := ncQAAudit(t, q.f)
	q.service.store.timeout = 20 * time.Millisecond
	release := ncQAOwnedWriter(t, q.f)
	readRows, readDone, readClosed := 0, false, false
	ncQAHooks(t, ncQASQLHooks{Observe: func(e ncQASQLEvent) {
		if !readClosed && e.Operation == "statement" && e.Phase == "step-after" {
			if e.Code == 100 {
				readRows++
			}
			if e.Code == 101 {
				readDone = true
			}
		}
		if e.Operation == "close" && e.Phase == "close-after" {
			readClosed = true
		}
	}})
	r, err := q.service.ingestSQLite(context.Background(), e)
	ncQASetSQLHooks(ncQASQLHooks{})
	release()
	if readRows < 2 || !readDone || !readClosed || ncQAErrorCode(err) != "local_write_unknown" || !reflect.DeepEqual(r, EventResult{}) || q.clockCalls != 0 {
		t.Fatal("actual WAL receipt read/closure before writer fence contention not witnessed", readRows, readDone, readClosed, err)
	}
	ncQAUnchanged(t, before, rows, q.f)
}

func TestSQLiteNormalizedCaptureN16ExactCandidateGenerationProjectionAndBoundedCardinalities(t *testing.T) {
	// Final literal N01, not the former Q01 key-only projection. This identity
	// witness does not claim native visited-work or a general complexity bound.
	q := ncQANew(t, func(h *qaHarness) {
		for _, a := range []string{"A", "B", "C"} {
			h.ingest(0, qaEvent(a, "1", "1", "work", qaBindingA))
		}
		h.ingest(0, qaEvent("B", "1", "2", "wait_user", ""))
		h.ingest(0, qaEvent("C", "1", "2", "wait_permission", ""))
	})
	h := ncQAOpen(t, q.f, sqliteio.Read)
	s, err := h.tx.Prepare("SELECT actor_key,generation FROM actors WHERE computer_id=? AND health='continuous' AND state IN ('working','wait_children','wait_user')", sqliteio.Text(qaComputer))
	if err != nil {
		t.Fatal(err)
	}
	refs := []ActorRef{}
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal(errors.Join(err, s.Close()))
		}
		if !row {
			break
		}
		if s.ColumnCount() != 2 {
			t.Fatal("N01 exact width", s.Close())
		}
		ref, err := sqliteDependencyStoredActor(h.tx, s, qaComputer)
		if err != nil {
			t.Fatal(errors.Join(err, s.Close()))
		}
		refs = append(refs, ref)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Key.AgentID < refs[j].Key.AgentID })
	if len(refs) != 2 || refs[0].Key.AgentID != "A" || refs[1].Key.AgentID != "B" || refs[0].Generation != "1" || refs[1].Generation != "1" {
		t.Fatal("N01 candidate exact identities", refs)
	}
	h.end(t, false)
	// Honest local cardinalities; native lane must later vary A/U/K/P/R plus
	// N06 bindings, N07 exact-Ref turns and complete pending tools at 1k/5k history.
	t.Log("source fixture A=2, U=0, K=0, P=0, R=0; native cumulative work remains required")
}

func TestSQLiteNormalizedCaptureN16SevenLiteralSelectorsTypedPeersAndNegativeLookups(t *testing.T) {
	// N01 is independently exercised above. These exact missing literals are
	// query fixtures, not assertions that future production invokes them. Root's
	// later producer review must compare its called literals and native counters.
	other := cfQAAttr()
	other.TaskID = "9"
	q := ncQANew(t, func(h *qaHarness) {
		h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
		h.at(5)
		_, err := h.service.Ingest(context.Background(), qaEvent("A", "1", "3", "work", ""))
		qaCode(t, err, "event_gap")
		if err = h.service.store.update(context.Background(), func(st *state) (bool, error) {
			a := st.Actors[actorKey(qaEvent("A", "1", "1", "work", "").Actor)]
			a.Attribution = other
			a.UncertaintyIDs = append(a.UncertaintyIDs, a.UncertaintyIDs[0])
			seg := st.Segments[*a.SegmentID]
			seg.Binding.Attribution = other
			findEpoch(st, seg.EpochID).Attribution = other
			st.Uncertainties[a.UncertaintyIDs[0]].Attribution = other
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	st := bgQAReadLegacy(t, q.legacy.service)
	key := qaEvent("A", "1", "1", "work", "").Actor
	a := st.Actors[actorKey(key)]
	id := a.UncertaintyIDs[0]
	w := ncQAOpen(t, q.f, sqliteio.Write)
	n02 := "SELECT uncertainty_id FROM uncertainties WHERE actor_key=? AND state='unresolved' AND upper_bound_sec IS NULL"
	if ids := ncQAStrings(t, w.tx, n02, sqliteio.Text(actorKey(key))); !reflect.DeepEqual(ids, []string{id}) {
		t.Fatal("N02 exact unbounded identity", ids)
	}
	if ids := ncQAStrings(t, w.tx, n02, sqliteio.Text(actorKey(qaEvent("absent", "1", "1", "work", "").Actor))); len(ids) != 0 {
		t.Fatal("N02 negative exact lookup")
	}
	n03 := "SELECT actor_key,ordinal,uncertainty_id FROM actor_uncertainties WHERE actor_key=? AND uncertainty_id=? ORDER BY ordinal ASC LIMIT 1"
	s, err := w.tx.Prepare(n03, sqliteio.Text(actorKey(key)), sqliteio.Text(id))
	if err != nil {
		t.Fatal(err)
	}
	if row, err := s.Step(); !row || err != nil {
		t.Fatal("N03 first membership", errors.Join(err, s.Close()))
	}
	if s.ColumnCount() != 3 {
		t.Fatal("N03 width", s.Close())
	}
	k, kerr := s.Text(0)
	ordinal, oerr := s.Int64(1)
	u, uerr := s.Text(2)
	for col, kind := range []sqliteio.Kind{sqliteio.TextKind, sqliteio.IntegerKind, sqliteio.TextKind} {
		got, err := s.Kind(col)
		if err != nil || got != kind {
			t.Fatal("N03 kinds", errors.Join(err, s.Close()))
		}
	}
	if kerr != nil || oerr != nil || uerr != nil || k != actorKey(key) || ordinal != 0 || u != id {
		t.Fatal("N03 first duplicate ordinal", errors.Join(kerr, oerr, uerr, s.Close()))
	}
	if row, err := s.Step(); row || err != nil {
		t.Fatal("N03 exact DONE", errors.Join(err, s.Close()))
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	urow, ok, err := sqliteReadUncertaintyScalar(w.tx, st.ComputerID, id)
	if !ok || err != nil {
		t.Fatal("selected N02/N03 actual scalar peer", err)
	}
	if err = sqliteValidateSelectedCaptureDependencies(w.tx, st.ComputerID, st.Revision, sqliteDependencySelection{UncertaintyIDs: []string{id}, ActorKeys: []ActorKey{key}}); err != nil {
		t.Fatal("selected literal peers", err)
	}
	n04 := "SELECT uncertainty_id FROM uncertainties WHERE computer_id=? AND account_id=? AND project_id=? AND state='unresolved' AND upper_bound_sec IS NULL AND (user_id<>? OR task_id<>? OR timezone<>?) LIMIT 1"
	base := []sqliteio.Value{sqliteio.Text(st.ComputerID), sqliteio.Text("1"), sqliteio.Text("3")}
	binds := append(append([]sqliteio.Value(nil), base...), sqliteio.Text("2"), sqliteio.Text("4"), sqliteio.Text("UTC"))
	if ids := ncQAStrings(t, w.tx, n04, binds...); !reflect.DeepEqual(ids, []string{id}) {
		t.Fatal("N04 incompatible unbounded witness")
	}
	compatible := append(append([]sqliteio.Value(nil), base...), sqliteio.Text("2"), sqliteio.Text("9"), sqliteio.Text("UTC"))
	if ids := ncQAStrings(t, w.tx, n04, compatible...); len(ids) != 0 {
		t.Fatal("N04 same attribution blocked")
	}
	bound := qaEpochStart.Add(10 * time.Second)
	next := urow
	next.UpperBound = &bound
	if _, err = sqliteWriteUncertainty(w.tx, st.ComputerID, &urow, next); err != nil {
		t.Fatal(err)
	}
	if ids := ncQAStrings(t, w.tx, n04, binds...); len(ids) != 0 {
		t.Fatal("N04 bounded candidate retained")
	}
	n05 := "SELECT uncertainty_id FROM uncertainties WHERE computer_id=? AND account_id=? AND project_id=? AND state='unresolved' AND (upper_bound_sec,upper_bound_nsec)>=(?,?) AND (user_id<>? OR task_id<>? OR timezone<>?) LIMIT 1"
	// A subminute UTC offset serializes with time repair; query by RAW instant.
	point := bound.In(time.FixedZone("raw-second-offset", 19))
	pointBinds := append(append([]sqliteio.Value(nil), base...), sqliteio.Integer(point.Unix()), sqliteio.Integer(int64(point.Nanosecond())), sqliteio.Text("2"), sqliteio.Text("4"), sqliteio.Text("UTC"))
	if ids := ncQAStrings(t, w.tx, n05, pointBinds...); !reflect.DeepEqual(ids, []string{id}) {
		t.Fatal("N05 inclusive raw instant endpoint")
	}
	pointBinds[4] = sqliteio.Integer(int64(point.Nanosecond() + 1))
	if ids := ncQAStrings(t, w.tx, n05, pointBinds...); len(ids) != 0 {
		t.Fatal("N05 point beyond endpoint")
	}
	n06 := "SELECT binding_id FROM bindings WHERE computer_id=? AND account_id=? AND project_id=? AND active=1"
	bindings := ncQAStrings(t, w.tx, n06, base...)
	sort.Strings(bindings)
	if !reflect.DeepEqual(bindings, []string{qaBindingA, qaBindingB}) {
		t.Fatal("N06 complete active timer set")
	}
	for _, id := range bindings {
		row, ok, err := sqliteReadBinding(w.tx, st.ComputerID, id)
		if !ok || err != nil || row.Snapshot.Attribution.AccountID != "1" || row.Snapshot.Attribution.ProjectID != "3" {
			t.Fatal("N06 actual complete peers", err)
		}
	}
	w.end(t, false)
	host := qaNewClaude(t)
	host.startSession()
	root := host.send(0, host.event("UserPromptSubmit", "question", ""))
	host.send(0, qaClaudeQuestion(host, "PreToolUse", "q"))
	if root.Actor == nil {
		t.Fatal("N07 actual Ref positive fixture")
	}
	hst := bgQAReadLegacy(t, host.service)
	var turn *hostTurn
	for _, r := range hst.HostTurns {
		if r.Actor != nil && *r.Actor == *root.Actor {
			turn = r
			break
		}
	}
	if turn == nil {
		t.Fatal("N07 retained turn absent")
	}
	clone := *turn
	clone.TurnID = "historical-exact-ref"
	clone.Stopped = true
	clone.Tools = map[string]hostTool{"ordinary": {Name: "Read", Phase: "pre"}}
	hst.HostTurns[hostTurnKey(clone.Session, HostEvent{TurnID: clone.TurnID, AgentID: clone.AgentID})] = &clone
	q2 := ncQANew(t, func(h *qaHarness) { bgQALegacyMarshalOracle(t, h.service, h.path, hst, true) })
	r := ncQAOpen(t, q2.f, sqliteio.Read)
	n07 := "SELECT turn_key FROM host_turns WHERE actor_key=? AND actor_generation=?"
	turns := ncQAStrings(t, r.tx, n07, sqliteio.Text(actorKey(root.Actor.Key)), interopCounter(t, root.Actor.Generation))
	if len(turns) != 2 {
		t.Fatal("N07 omitted exact-Ref stopped/history turn")
	}
	pending := 0
	for _, key := range turns {
		row, ok, err := sqliteReadHostTurn(r.tx, hst.ComputerID, key)
		if !ok || err != nil || row.Actor == nil || *row.Actor != *root.Actor {
			t.Fatal("N07 full owned peer", err)
		}
		tools, err := sqlitePendingHostTools(r.tx, hst.ComputerID, key)
		if err != nil {
			t.Fatal(err)
		}
		pending += len(tools)
	}
	if pending != 2 {
		t.Fatal("N07 complete pending-tool cardinality")
	}
	if ids := ncQAStrings(t, r.tx, n07, sqliteio.Text(actorKey(root.Actor.Key)), interopCounter(t, "2")); len(ids) != 0 {
		t.Fatal("N07 wrong generation matched")
	}
	r.end(t, false)
	t.Log("literal fixture A/U/K/P/R accounted separately; N06=2, exact-Ref N07=2, pending tools=2; no native work claim")
}

func TestSQLiteNormalizedCaptureNativeSlotUnderActualWriterAndNoSampleReplay(t *testing.T) {
	q := ncQANew(t, nil)
	s := New(Options{Path: filepath.Join(q.f.directory, q.f.authority), ResolveBinding: q.legacy.service.resolve})
	e := qaEvent("A", "1", "1", "work", qaBindingA)
	admission := ncQAAdmission(q.f, time.Now().Add(time.Second))
	prepared, clock, found, err := s.sqlitePrepareCapture(context.Background(), e, admission)
	if err != nil || !found || clock.Mode != sqliteCaptureNative || clock.Consumed || clock.Prepared != nil || clock.Site != sqliteCaptureClockReduce {
		t.Fatal("native preparation sampled", err)
	}
	// Acquire a real blocker only AFTER checked preparation reads release the
	// same-process root gate. A bounded separate owner releases it while Open
	// waits; channel join precedes every fixture cleanup, including Fatal exits.
	blocker := ncQAOpen(t, q.f, sqliteio.Write)
	type releaseResult struct {
		at  time.Time
		err error
	}
	joined := make(chan releaseResult, 1)
	go func() {
		timer := time.NewTimer(20 * time.Millisecond)
		<-timer.C
		timer.Stop()
		at := time.Now().UTC()
		rollbackErr := blocker.tx.Rollback()
		blocker.ended = true
		closeErr := blocker.c.Close(context.Background())
		blocker.closed = true
		joined <- releaseResult{at, errors.Join(rollbackErr, closeErr)}
	}()
	var released releaseResult
	didJoin := false
	join := func() {
		if !didJoin {
			released = <-joined
			didJoin = true
			if released.err != nil {
				t.Error("checked blocker release", released.err)
			}
		}
	}
	t.Cleanup(join)
	c, tx, m, found, err := sqliteOpenCapture(context.Background(), admission, sqliteio.Write)
	join()
	if err != nil || !found {
		t.Fatal("native writer admission", err)
	}
	// Register checked failure cleanup before the transaction-local probe.
	ended, closed := false, false
	t.Cleanup(func() {
		if !ended {
			if err := tx.Rollback(); err != nil {
				t.Errorf("native probe rollback: %v", err)
			}
		}
		if !closed {
			if err := c.Close(context.Background()); err != nil {
				t.Errorf("native probe close: %v", err)
			}
		}
	})
	writerAt := time.Now().UTC()
	if writerAt.Before(released.at) {
		t.Fatal("writer preceded bounded blocker release")
	}
	transition, err := sqliteReduceEvent(tx, m, e, prepared, &clock, false)
	if err != nil || !transition.Changed || transition.OperationError != nil || !clock.Consumed {
		t.Fatal("native sample not consumed under real writer", err, transition.OperationError)
	}
	a, ok, readErr := sqliteReadActorLocal(tx, m.ComputerID, e.Actor)
	if readErr != nil || !ok || a.LastEvidence.WallUTC.Before(writerAt) {
		t.Fatal("native evidence predates actual writer boundary", readErr)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	ended = true
	if err = c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	closed = true
	// Replay plans carry no slot and leave native Consumed false.
	q2 := ncQAWorking(t)
	native := New(Options{Path: filepath.Join(q2.f.directory, q2.f.authority)})
	_, replayClock, found, err := native.sqlitePrepareCapture(context.Background(), e, ncQAAdmission(q2.f, time.Now().Add(time.Second)))
	if err != nil || !found || replayClock.Site != sqliteCaptureClockNone || replayClock.Consumed {
		t.Fatal("native replay sampled", err)
	}
	// An explicitly injected nativeClock value stays on the prepared path; the
	// native selection is the Options nil decision, not an interface type check.
	injected := New(Options{Path: filepath.Join(q.f.directory, q.f.authority), Clock: nativeClock{}, ResolveBinding: q.legacy.service.resolve})
	_, injectedClock, found, err := injected.sqlitePrepareCapture(context.Background(), e, ncQAAdmission(q.f, time.Now().Add(time.Second)))
	if err != nil || !found || injectedClock.Mode != sqliteCapturePrepared || injectedClock.Prepared == nil || injectedClock.Consumed {
		t.Fatal("interface type substituted native trust", err)
	}
}

func TestSQLiteNormalizedCaptureGuardUsesSharedClockChangedPredicate(t *testing.T) {
	q := ncQAWorking(t)
	q.sample = ncQASample(10)
	q.sample.Epoch = asQAPointer("new-boot")
	e := qaEvent("A", "1", "2", "work", "")
	admission := ncQAAdmission(q.f, time.Now().Add(time.Second))
	p, clock, ok, err := q.service.sqlitePrepareCapture(context.Background(), e, admission)
	if err != nil || !ok {
		t.Fatal(err)
	}
	c, tx, m, ok, err := sqliteOpenCapture(context.Background(), admission, sqliteio.Write)
	if err != nil || !ok {
		t.Fatal(err)
	}
	ended, closed := false, false
	t.Cleanup(func() {
		if !ended {
			if err := tx.Rollback(); err != nil {
				t.Errorf("guard rollback: %v", err)
			}
		}
		if !closed {
			if err := c.Close(context.Background()); err != nil {
				t.Errorf("guard close: %v", err)
			}
		}
	})
	r, err := sqliteReduceEvent(tx, m, e, p, &clock, true)
	if err != nil || !r.Changed || ncQAErrorCode(r.OperationError) != "clock_conflict" || !clock.Consumed || q.clockCalls != 1 {
		t.Fatal("guard shared safety predicate", err, r.OperationError)
	}
	legacy := bgQAReadLegacy(t, q.legacy.service)
	q.legacy.sample = q.sample
	_, changed, legacyErr := q.legacy.service.reduceWithWaitGuard(context.Background(), legacy, e, true)
	if !changed || ncQAErrorCode(legacyErr) != "clock_conflict" {
		t.Fatal("actual legacy guard control", legacyErr)
	}
	if err = sqliteValidateSelectedCaptureDependencies(tx, m.ComputerID, bump(m.Revision), r.Dependencies); err != nil {
		t.Fatal("guard selected dependencies", err)
	}
	if err = sqliteValidateSelectedFinalization(tx, m.ComputerID, bump(m.Revision), r.Finalization); err != nil {
		t.Fatal("guard final graph", err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	ended = true
	if err = c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	closed = true
}

// Keep strconv as an independently exercised unsigned/decimal oracle here.
func TestSQLiteNormalizedCaptureUnsignedGenerationBoundary(t *testing.T) {
	q := ncQAWorking(t)
	e := qaEvent("A", strconv.FormatUint(^uint64(0), 10), "1", "work", qaBindingA)
	before, rows := ncQAAudit(t, q.f)
	r, err := q.service.ingestSQLite(context.Background(), e)
	if ncQAErrorCode(err) != "validation" || !reflect.DeepEqual(r, EventResult{}) || q.clockCalls != 0 {
		t.Fatal("reserved maximum event generation admitted", err)
	}
	ncQAUnchanged(t, before, rows, q.f)
}
