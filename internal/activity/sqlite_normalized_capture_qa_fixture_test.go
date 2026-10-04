//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent SOURCE-ONLY operation fixtures. Compilation requires the reviewed
// normalized-capture ABI and verified fresh-native CloseDurably integration.
// Complete state reconstruction below is exclusively a cold test oracle; no
// partial state, reducer callback, producer stub or activation seam is supplied.

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	_ "unsafe"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"golang.org/x/sys/unix"
)

// Exact reviewed private hook ABI, mirrored from spQA's source-approved fixture.
// Serial setup/reset only; these hooks never replace a native result or pointer.
type ncQASQLEvent struct {
	Phase, Operation string
	Code             int32
}
type ncQASQLHooks struct {
	Observe func(ncQASQLEvent)
	Fault   func(ncQASQLEvent) error
}

//go:linkname ncQASetSQLHooks github.com/rbeene/tempo/internal/activity/sqliteio.setSQLHooksForTest
func ncQASetSQLHooks(ncQASQLHooks)

func ncQAHooks(t *testing.T, h ncQASQLHooks) {
	t.Helper()
	ncQASetSQLHooks(h)
	t.Cleanup(func() { ncQASetSQLHooks(ncQASQLHooks{}) })
}

// Existing sqliteio filesystem observation ABI; the hook only observes. A
// fixture changes its own private file at the actual Footprint boundary.
type ncQAFSEvent struct {
	Namespace, Role, Op, Phase string
	Flags, FD, Errno           int
	Code                       int32
}
type ncQAFSHooks struct{ Observe func(ncQAFSEvent) }

//go:linkname ncQASetFSHooks github.com/rbeene/tempo/internal/activity/sqliteio.setHooksForTest
func ncQASetFSHooks(ncQAFSHooks)

type ncQAConnection struct {
	c             *sqliteio.Conn
	tx            *sqliteio.Tx
	ended, closed bool
}

func ncQAOpen(t *testing.T, f interopFixture, mode sqliteio.Mode) *ncQAConnection {
	t.Helper()
	c, err := sqliteio.Open(context.Background(), f.directory, f.database, sqliteio.Options{ReadOnly: mode == sqliteio.Read, AcquireDeadline: time.Now().Add(time.Second)})
	if err != nil {
		t.Fatal("real fixture Open", err)
	}
	h := &ncQAConnection{c: c}
	t.Cleanup(func() {
		if !h.ended && h.tx != nil {
			if err := h.tx.Rollback(); err != nil {
				t.Errorf("checked fixture rollback: %v", err)
			}
			h.ended = true
		}
		if !h.closed {
			if err := h.c.Close(context.Background()); err != nil {
				t.Errorf("checked fixture close: %v", err)
			}
			h.closed = true
		}
	})
	h.tx, err = c.Begin(context.Background(), mode)
	if err != nil {
		t.Fatal("real fixture Begin", err)
	}
	return h
}
func (h *ncQAConnection) end(t *testing.T, commit bool) {
	t.Helper()
	if commit {
		out, err := h.tx.Commit()
		if err != nil || out != sqliteio.Committed {
			t.Fatalf("fixture Commit outcome=%v err=%v", out, err)
		}
	} else if err := h.tx.Rollback(); err != nil {
		t.Fatal("fixture rollback", err)
	}
	h.ended = true
	if err := h.c.Close(context.Background()); err != nil {
		t.Fatal("fixture close", err)
	}
	h.closed = true
}
func ncQADone(t *testing.T, tx *sqliteio.Tx, sql string, binds ...sqliteio.Value) {
	t.Helper()
	s, err := tx.Prepare(sql, binds...)
	if err != nil {
		t.Fatal("fixture prepare", err)
	}
	row, stepErr := s.Step()
	closeErr := s.Close()
	if row || stepErr != nil || closeErr != nil {
		t.Fatalf("fixture DONE row=%t err=%v close=%v", row, stepErr, closeErr)
	}
}
func ncQAStrings(t *testing.T, tx *sqliteio.Tx, sql string, binds ...sqliteio.Value) []string {
	t.Helper()
	s, err := tx.Prepare(sql, binds...)
	if err != nil {
		t.Fatal(err)
	}
	values := []string{}
	for {
		row, stepErr := s.Step()
		if stepErr != nil {
			t.Fatal(errors.Join(stepErr, s.Close()))
		}
		if !row {
			break
		}
		kind, kindErr := s.Kind(0)
		if kindErr != nil || kind != sqliteio.TextKind || s.ColumnCount() != 1 {
			t.Fatal("fixture exact TEXT projection", errors.Join(kindErr, s.Close()))
		}
		v, readErr := s.Text(0)
		if readErr != nil {
			t.Fatal(errors.Join(readErr, s.Close()))
		}
		values = append(values, v)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return values
}
func ncQAHookPositive(t *testing.T, f interopFixture) {
	t.Helper()
	h := ncQAOpen(t, f, sqliteio.Read)
	var events []ncQASQLEvent
	ncQAHooks(t, ncQASQLHooks{Observe: func(e ncQASQLEvent) {
		if e.Operation == "statement" || e.Operation == "prepare" {
			events = append(events, e)
		}
	}})
	s, err := h.tx.Prepare("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if row, err := s.Step(); !row || err != nil {
		t.Fatal("real ROW control", errors.Join(err, s.Close()))
	}
	if n, err := s.Int64(0); n != 1 || err != nil {
		t.Fatal("real scalar control", errors.Join(err, s.Close()))
	}
	if row, err := s.Step(); row || err != nil {
		t.Fatal("real DONE control", errors.Join(err, s.Close()))
	}
	if err := s.Close(); err != nil {
		t.Fatal("real finalize control", err)
	}
	ncQASetSQLHooks(ncQASQLHooks{})
	want := []ncQASQLEvent{{"prepare-before", "prepare", 0}, {"step-before", "statement", 0}, {"step-before-native", "statement", 0}, {"step-after", "statement", 100}, {"step-before", "statement", 0}, {"step-before-native", "statement", 0}, {"step-after", "statement", 101}, {"finalize-after", "statement", 0}}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("reviewed hook ABI control differs: %#v", events)
	}
	h.end(t, false)
}

// Actual writer COMMIT calibration: SQLite emits DONE101, not OK0, after step.
func ncQACommitPositive(t *testing.T, f interopFixture) {
	t.Helper()
	ncQAHookPositive(t, f)
	h := ncQAOpen(t, f, sqliteio.Write)
	ncQADone(t, h.tx, "UPDATE store_meta SET logical_bytes=logical_bytes WHERE singleton=1")
	var events []ncQASQLEvent
	ncQAHooks(t, ncQASQLHooks{Observe: func(e ncQASQLEvent) {
		if e.Operation == "commit" {
			events = append(events, e)
		}
	}})
	h.end(t, true)
	ncQASetSQLHooks(ncQASQLHooks{})
	want := []ncQASQLEvent{{"commit-before-dispatch", "commit", 0}, {"control-before-native", "commit", 0}, {"commit-after-engine", "commit", 101}, {"commit-before-verify", "commit", 101}, {"commit-after-verify", "commit", 101}}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("actual writer COMMIT phase/code control differs: %#v", events)
	}
}

// Confined mirror of sqliteio's actual TestSQLiteIOOwnedWriterHelper protocol.
// One child, one mode, no descendants; inactive during every ordinary outer run.
func TestSQLiteNormalizedCaptureOwnedWriterHelper(t *testing.T) {
	if os.Getenv("TEMPO_NC_QA_MODE") != "writer" {
		return
	}
	f := interopFixture{directory: os.Getenv("TEMPO_NC_QA_DIR"), database: os.Getenv("TEMPO_NC_QA_NAME")}
	h := ncQAOpen(t, f, sqliteio.Write)
	if _, err := fmt.Fprintln(os.Stdout, "NC_QA_WRITER_READY"); err != nil {
		t.Fatal("owned ready pipe", err)
	}
	var release [1]byte
	if _, err := io.ReadFull(os.Stdin, release[:]); err != nil || release[0] != 'x' {
		t.Fatal("owned release pipe", err)
	}
	h.end(t, false)
}

// Real separate-process BEGIN IMMEDIATE permits WAL readers; same-process
// root registration would block the prerequisite receipt read. Runtime must
// establish coexistence, readiness, successful release/join and PID/PG absence.
func ncQAOwnedWriter(t *testing.T, f interopFixture) func() {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	cmd := exec.CommandContext(ctx, bin, "-test.run=^TestSQLiteNormalizedCaptureOwnedWriterHelper$", "-test.v", "-test.timeout=5s")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "GOMAXPROCS=2", "GORACE=atexit_sleep_ms=0", "TMPDIR=" + filepath.Dir(f.directory), "TEMPO_NC_QA_MODE=writer", "TEMPO_NC_QA_DIR=" + f.directory, "TEMPO_NC_QA_NAME=" + f.database}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	in, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		if closeErr := in.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err = cmd.Start(); err != nil {
		cancel()
		if closeErr := in.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		if closeErr := out.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	reader := bufio.NewReader(out)
	ready := false
	var once sync.Once
	finish := func(normal bool) {
		once.Do(func() {
			var writeErr error
			if normal {
				_, writeErr = in.Write([]byte{'x'})
			} else {
				cancel()
			}
			closeErr := in.Close()
			_, readErr := io.Copy(io.Discard, reader)
			waitErr := cmd.Wait()
			cancel()
			if normal && (writeErr != nil || closeErr != nil || readErr != nil || waitErr != nil) {
				t.Errorf("owned child release/join write=%v close=%v read=%v wait=%v", writeErr, closeErr, readErr, waitErr)
			}
			if !normal {
				if closeErr != nil || readErr != nil {
					t.Errorf("owned failure cleanup close=%v read=%v", closeErr, readErr)
				}
				if waitErr != nil {
					var exit *exec.ExitError
					if !errors.As(waitErr, &exit) {
						t.Errorf("owned kill join has unexpected error: %v", waitErr)
					}
				}
			}
			if cmd.ProcessState == nil {
				t.Error("owned child lacks actual joined process state")
			}
			if err := unix.Kill(pid, 0); !errors.Is(err, unix.ESRCH) {
				t.Errorf("owned child PID still present or unverified: %v", err)
			}
			if err := unix.Kill(-pid, 0); !errors.Is(err, unix.ESRCH) {
				t.Errorf("owned child process group still present or unverified: %v", err)
			}
			exitCode := -999
			if cmd.ProcessState != nil {
				exitCode = cmd.ProcessState.ExitCode()
			}
			t.Logf("owned native writer joined pid=%d pgid=%d exit=%d normal=%t", pid, pid, exitCode, normal)
		})
	}
	t.Cleanup(func() { finish(false) })
	pgid, err := unix.Getpgid(pid)
	if err != nil || pgid != pid {
		t.Fatal("owned process group identity", pgid, err)
	}
	t.Logf("owned native writer started pid=%d pgid=%d", pid, pgid)
	for n := 0; n < 16; n++ {
		line, readErr := reader.ReadString('\n')
		if strings.TrimSpace(line) == "NC_QA_WRITER_READY" {
			ready = true
			break
		}
		if readErr != nil {
			break
		}
	}
	if !ready {
		t.Fatal("owned child never reached real BEGIN IMMEDIATE")
	}
	return func() { finish(true) }
}

type ncQAFixture struct {
	f                         interopFixture
	legacy                    *qaHarness
	service                   *Service
	sample                    ClockSample
	sampleErr                 error
	clockCalls, resolverCalls int
	clockAction               func()
	resolverAction            func()
}

func ncQASample(at int64) ClockSample {
	epoch, ns := "boot-1", strconv.FormatInt(at*int64(time.Second), 10)
	return ClockSample{Capability: "available", WallUTC: qaEpochStart.Add(time.Duration(at) * time.Second), Epoch: &epoch, ElapsedNS: &ns, AwakeNS: &ns}
}
func ncQANew(t *testing.T, prepare func(*qaHarness)) *ncQAFixture {
	t.Helper()
	h := qaNew(t)
	h.seed()
	if prepare != nil {
		prepare(h)
	}
	st := hnQAValid(t, bgQAReadLegacy(t, h.service))
	if len(st.Intervals) != 0 {
		t.Fatal("this fixture seeds pre-seal graphs; seals must arise from the tested operation")
	}
	if len(st.SyncConfigurations) != 0 {
		t.Fatal("normalized capture fixture requires empty unrelated sync configuration")
	}
	f, m, _, _ := sdQASeed(t, st)
	q := &ncQAFixture{f: f, legacy: h, sample: ncQASample(0)}
	// Existing composer makes the initial derived graph complete before capture.
	w := ncQAOpen(t, f, sqliteio.Write)
	delta := cfQARefreshAll(t, w.tx, st)
	for _, id := range mqQAIDs(st) {
		r := st.Requests[id]
		if r.Operation == "activity.resolve" && r.MutationResult != nil {
			continue
		} // sdQASeed already wrote this complete unit
		d, err := sqliteWriteMutationRequest(w.tx, st.ComputerID, nil, sqliteMutationRequestRow{ID: id, Value: r})
		if err != nil {
			t.Fatal("actual general request fixture writer", err)
		}
		delta += d
	}
	if delta != 0 {
		next := metaQANext(t, m)
		next.Revision = bump(m.Revision)
		next.LogicalBytes += delta
		if err := sqliteUpdateMeta(w.tx, m, next); err != nil {
			t.Fatal(err)
		}
		if got, err := sqliteReadMeta(w.tx, f.authority, f.database); err != nil || !reflect.DeepEqual(got, next) {
			t.Fatal("initial fixture metadata readback", err)
		}
	}
	w.end(t, true)
	if delta != 0 {
		// The extra SQL fixture commit must have the same public revision as
		// the legacy oracle used for all later result and cold-state comparisons.
		err := h.service.store.update(context.Background(), func(current *state) (bool, error) {
			if current.Revision != st.Revision {
				return false, failure("state_corrupt")
			}
			return true, nil
		})
		if err != nil {
			t.Fatal("legacy fixture revision alignment", err)
		}
		expected := *st
		expected.Revision = bump(st.Revision)
		if actual := bgQAReadLegacy(t, h.service); !reflect.DeepEqual(actual, &expected) {
			t.Fatal("fixture-only revision alignment changed legacy graph")
		}
	}
	q.service = New(Options{Path: filepath.Join(f.directory, f.authority), Clock: ClockFunc(func() (ClockSample, error) {
		q.clockCalls++
		if q.clockAction != nil {
			q.clockAction()
		}
		return q.sample, q.sampleErr
	}), ResolveBinding: func(_ context.Context, e Event) (BindingSnapshot, bool, error) {
		q.resolverCalls++
		if q.resolverAction != nil {
			q.resolverAction()
		}
		b, ok := h.bindings[e.BindingID]
		return b, ok, nil
	}})
	return q
}
func ncQAWorking(t *testing.T) *ncQAFixture {
	return ncQANew(t, func(h *qaHarness) { h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA)) })
}

// A test-owned reserved or maximum metadata boundary cannot use the ordinary
// mutation CAS: that API intentionally forbids changing migration identity or
// skipping revisions. Keep the actual encoded row, nonce and charge readable.
func ncQAStageMetaBoundary(t *testing.T, tx *sqliteio.Tx, f interopFixture, before, after sqliteStoreMeta) {
	t.Helper()
	if err := sqliteValidateMeta(after); err != nil {
		t.Fatal("invalid boundary metadata", err)
	}
	oldRevision, err := sqliteEncodeUint64(before.Revision)
	if err != nil {
		t.Fatal(err)
	}
	newRevision, err := sqliteEncodeUint64(after.Revision)
	if err != nil {
		t.Fatal(err)
	}
	ncQADone(t, tx, "UPDATE store_meta SET revision=?,durability_nonce=?,migration_id=?,backup_sha256=?,logical_bytes=? WHERE singleton=1 AND revision=? AND durability_nonce=?",
		sqliteio.Blob(newRevision[:]), sqliteio.Blob(after.DurabilityNonce[:]), sqliteMetaOptional(after.MigrationID), sqliteMetaOptional(after.BackupSHA256), sqliteio.Integer(after.LogicalBytes), sqliteio.Blob(oldRevision[:]), sqliteio.Blob(before.DurabilityNonce[:]))
	got, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(got, after) {
		t.Fatal("actual metadata reader rejected boundary fixture", err)
	}
}
func ncQAErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) {
		if e == nil {
			return ""
		}
		return e.Code
	}
	return fmt.Sprintf("unexpected:%T", err)
}
func ncQARawIdentity(t *testing.T, e Event) (string, string) {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(b)
	k, err := json.Marshal(e.Actor)
	if err != nil {
		t.Fatal(err)
	}
	return string(k) + "/" + e.Generation + "/" + e.Sequence, hex.EncodeToString(digest[:])
}

// Cold all-row audit is independent of Delta. It runs only outside the capture
// path; cfQAAudit counts literal actual values and checks unrelated sync tables.
func ncQAAudit(t *testing.T, f interopFixture) (sqliteStoreMeta, map[string][][]string) {
	t.Helper()
	h := ncQAOpen(t, f, sqliteio.Read)
	m, err := sqliteReadMeta(h.tx, f.authority, f.database)
	if err != nil {
		t.Fatal(err)
	}
	rows, charge := cfQAAudit(t, h.tx, m)
	if charge != m.LogicalBytes {
		t.Fatalf("literal charge=%d metadata=%d", charge, m.LogicalBytes)
	}
	cfQAColdClosure(t, h.tx, m, true)
	h.end(t, false)
	return m, rows
}
func ncQANonceOnly(t *testing.T, before sqliteStoreMeta, rows map[string][][]string, f interopFixture) {
	t.Helper()
	after, got := ncQAAudit(t, f)
	if before.Revision != after.Revision || before.LogicalBytes != after.LogicalBytes || before.DurabilityNonce == after.DurabilityNonce {
		t.Fatal("no-change fence revision/charge/nonce")
	}
	delete(rows, "store_meta")
	delete(got, "store_meta")
	if !reflect.DeepEqual(rows, got) {
		t.Fatal("nonce fence altered semantic rows")
	}
	before.DurabilityNonce = after.DurabilityNonce
	if !reflect.DeepEqual(before, after) {
		t.Fatal("nonce fence altered other metadata")
	}
}
func ncQAUnchanged(t *testing.T, before sqliteStoreMeta, rows map[string][][]string, f interopFixture) {
	t.Helper()
	after, got := ncQAAudit(t, f)
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(rows, got) {
		t.Fatal("refusal retained partial rows, charge, revision or nonce")
	}
}

// Entire retained domain reconstruction is TEST ONLY. Every relationship is
// read by the actual typed reader. No guessed ActorKey parsing is performed.
func ncQAReadState(t *testing.T, q *ncQAFixture) *state {
	t.Helper()
	h := ncQAOpen(t, q.f, sqliteio.Read)
	m, err := sqliteReadMeta(h.tx, q.f.authority, q.f.database)
	if err != nil {
		t.Fatal(err)
	}
	st := emptyState()
	st.ComputerID, st.Revision, st.SyncEnabled = m.ComputerID, m.Revision, m.SyncEnabled
	baseline := bgQAReadLegacy(t, q.legacy.service)
	st.RecoveryDecisions = map[string]recoveryDecision{}
	st.SyncConfigurations = baseline.SyncConfigurations
	st.BindingRecords = map[string]bindingRecord{}
	st.Requests = map[string]mutationRequest{}
	for _, id := range ncQAStrings(t, h.tx, "SELECT binding_id FROM bindings ORDER BY binding_id") {
		r, ok, err := sqliteReadBinding(h.tx, m.ComputerID, id)
		if !ok || err != nil {
			t.Fatal("cold actual binding", err)
		}
		if r.Record != nil {
			st.BindingRecords[id] = *r.Record
		}
		if r.Record == nil || !r.Record.Deleted {
			st.Bindings[id] = r.Snapshot
		}
	}
	for _, id := range ncQAStrings(t, h.tx, "SELECT request_id FROM requests ORDER BY request_id") {
		r, ok, err := sqliteReadMutationRequestLocal(h.tx, m.ComputerID, id, m.Revision)
		if !ok || err != nil {
			t.Fatal("cold actual general request", err)
		}
		st.Requests[id] = r.Value
	}
	for _, id := range ncQAStrings(t, h.tx, "SELECT epoch_id FROM epochs ORDER BY creation_ordinal") {
		r, ok, err := sqliteReadEpoch(h.tx, m.ComputerID, id)
		if !ok || err != nil {
			t.Fatal("cold epoch", err)
		}
		ep := r.Value
		st.Epochs = append(st.Epochs, &ep)
	}
	s, err := h.tx.Prepare("SELECT actor_key,generation FROM actors ORDER BY actor_key")
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
			t.Fatal("cold actor projection", s.Close())
		}
		ref, err := sqliteDependencyStoredActor(h.tx, s, m.ComputerID)
		if err != nil {
			t.Fatal(errors.Join(err, s.Close()))
		}
		refs = append(refs, ref)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		r, ok, err := sqliteReadActorLocal(h.tx, m.ComputerID, ref.Key)
		if !ok || err != nil {
			t.Fatal("cold actor", err)
		}
		ids, err := sqliteActorUncertaintyIDsLocal(h.tx, m.ComputerID, ref.Key)
		if err != nil {
			t.Fatal(err)
		}
		st.Actors[actorKey(ref.Key)] = &Actor{ID: r.ID, Revision: r.Revision, Ref: r.Ref, Sequence: r.Sequence, State: r.State, Health: r.Health, BindingID: r.BindingID, BindingRevision: r.BindingRevision, Attribution: r.Attribution, Parent: r.Parent, SegmentID: r.SegmentID, LastEvidence: r.LastEvidence, UncertaintyIDs: ids}
	}
	for _, id := range ncQAStrings(t, h.tx, "SELECT segment_id FROM segments ORDER BY segment_id") {
		r, ok, err := sqliteReadSegmentLocal(h.tx, m.ComputerID, id)
		if !ok || err != nil {
			t.Fatal("cold segment", err)
		}
		events, err := sqliteSegmentEventsLocal(h.tx, m.ComputerID, id)
		if err != nil {
			t.Fatal(err)
		}
		st.Segments[id] = &segment{ID: r.ID, Actor: r.Actor, Binding: r.Binding, EpochID: r.EpochID, StartSample: r.StartSample, ConfirmedSample: r.ConfirmedSample, Start: r.Start, Confirmed: r.Confirmed, End: r.End, UncertaintyID: r.UncertaintyID, Finalized: r.Finalized, EventReferences: events}
	}
	for _, id := range ncQAStrings(t, h.tx, "SELECT uncertainty_id FROM uncertainties ORDER BY uncertainty_id") {
		u, ok, err := sqliteReadUncertaintyScalar(h.tx, m.ComputerID, id)
		if !ok || err != nil {
			t.Fatal("cold uncertainty", err)
		}
		st.Uncertainties[id] = &u
		e, ok, err := sqliteReadUncertaintyEvidenceScalar(h.tx, id)
		if !ok || err != nil {
			t.Fatal("cold evidence", err)
		}
		st.UncertaintyEvidence[id] = e
	}
	for _, id := range ncQAStrings(t, h.tx, "SELECT uncertainty_id FROM recovery_decisions ORDER BY uncertainty_id") {
		r, ok, err := sqliteReadRecoveryDecisionScalar(h.tx, id)
		if !ok || err != nil {
			t.Fatal("cold actual recovery decision", err)
		}
		st.RecoveryDecisions[id] = r
	}
	for _, key := range ncQAStrings(t, h.tx, "SELECT event_key FROM event_receipts ORDER BY event_key") {
		r, ok, err := sqliteReadEventReceipt(h.tx, key, m.Revision)
		if !ok || err != nil {
			t.Fatal("cold event receipt", err)
		}
		st.Receipts[key] = r.Value
	}
	for _, id := range ncQAStrings(t, h.tx, "SELECT event_id FROM event_ids ORDER BY event_id") {
		key, ok, err := sqliteReadEventID(h.tx, id)
		if !ok || err != nil {
			t.Fatal("cold event ID", err)
		}
		st.EventIDs[id] = key
	}
	for _, id := range ncQAStrings(t, h.tx, "SELECT interval_id FROM intervals ORDER BY creation_ordinal") {
		r, ok, err := sqliteReadIntervalLocal(h.tx, m.ComputerID, id)
		if !ok || err != nil {
			t.Fatal("cold interval", err)
		}
		ids, err := sqliteIntervalSegmentIDsLocal(h.tx, m.ComputerID, id)
		if err != nil {
			t.Fatal(err)
		}
		ordered := append([]string(nil), ids...)
		sort.Strings(ordered)
		if !reflect.DeepEqual(ids, ordered) {
			t.Fatal("actual immutable supports are not raw-ID ordered")
		}
		in := Interval{ID: r.ID, ComputerID: r.ComputerID, Attribution: r.Attribution, Start: r.Start, End: r.End, DurationNS: r.DurationNS, SegmentIDs: ids}
		st.Intervals = append(st.Intervals, in)
		root, ok, err := sqliteReadOutboxLocal(h.tx, m.ComputerID, id)
		if !ok || err != nil {
			t.Fatal("cold queued root", err)
		}
		st.Outbox[id] = OutboxItem{ID: root.ID, Revision: root.Revision, Interval: in, State: root.State, Correlation: root.Correlation, EntryID: root.EntryID, FailureCategory: root.FailureCategory, RetryRequestID: root.RetryRequestID, RunRequestID: root.RunRequestID}
	}
	st.HostSessions = map[string]*hostSession{}
	st.HostTurns = map[string]*hostTurn{}
	st.HostReceipts = map[string]hostReceiptRecord{}
	sessions, err := h.tx.Prepare("SELECT source,native_session FROM host_sessions ORDER BY session_key")
	if err != nil {
		t.Fatal(err)
	}
	pairs := [][2]string{}
	for {
		row, err := sessions.Step()
		if err != nil {
			t.Fatal(errors.Join(err, sessions.Close()))
		}
		if !row {
			break
		}
		if sessions.ColumnCount() != 2 {
			t.Fatal("cold host session width", sessions.Close())
		}
		source, e1 := sessions.Text(0)
		native, e2 := sessions.Text(1)
		if e1 != nil || e2 != nil {
			t.Fatal(errors.Join(e1, e2, sessions.Close()))
		}
		pairs = append(pairs, [2]string{source, native})
	}
	if err = sessions.Close(); err != nil {
		t.Fatal(err)
	}
	for _, pair := range pairs {
		r, ok, err := sqliteReadHostSession(h.tx, m.ComputerID, pair[0], pair[1])
		if !ok || err != nil {
			t.Fatal("cold actual host session", err)
		}
		v := r.Value
		st.HostSessions[r.Key] = &v
	}
	for _, key := range ncQAStrings(t, h.tx, "SELECT turn_key FROM host_turns ORDER BY turn_key") {
		r, ok, err := sqliteReadHostTurn(h.tx, m.ComputerID, key)
		if !ok || err != nil {
			t.Fatal("cold host turn", err)
		}
		tools := map[string]hostTool{}
		for _, id := range ncQAStrings(t, h.tx, "SELECT tool_id FROM host_tools WHERE turn_key=? ORDER BY tool_id", sqliteio.Text(key)) {
			tool, ok, err := sqliteReadHostTool(h.tx, m.ComputerID, key, id)
			if !ok || err != nil {
				t.Fatal("cold tool", err)
			}
			tools[id] = tool.Value
		}
		st.HostTurns[key] = &hostTurn{Source: r.Source, SessionID: r.NativeSession, Session: r.Incarnation, TurnID: r.TurnID, AgentID: r.AgentID, CWD: r.CWD, Actor: r.Actor, Stopped: r.Stopped, Tools: tools}
	}
	for _, key := range ncQAStrings(t, h.tx, "SELECT receipt_key FROM host_receipts ORDER BY receipt_key") {
		r, ok, err := sqliteReadHostReceipt(h.tx, m.ComputerID, m.Revision, key)
		if !ok || err != nil {
			t.Fatal("cold host receipt", err)
		}
		st.HostReceipts[key] = r.Record
	}
	ncQAGenerations(t, h.tx, st)
	h.end(t, false)
	return hnQAValid(t, st) // actual whole validState, marshal, strict decode + store
}

func ncQAGenerations(t *testing.T, tx *sqliteio.Tx, st *state) {
	t.Helper()
	expected := map[string]ActorRef{}
	add := func(ref ActorRef) { expected[actorKey(ref.Key)+"/"+ref.Generation] = ref }
	for _, a := range st.Actors {
		add(a.Ref)
		if a.Parent != nil {
			add(*a.Parent)
		}
	}
	for _, s := range st.Segments {
		add(s.Actor)
	}
	for _, u := range st.Uncertainties {
		add(u.Actor)
	}
	for _, r := range st.Receipts {
		add(r.Result.Actor)
	}
	for _, r := range st.HostReceipts {
		if r.Result.Actor != nil {
			add(*r.Result.Actor)
		}
	}
	for _, r := range st.HostTurns {
		if r.Actor != nil {
			add(*r.Actor)
		}
	}
	s, err := tx.Prepare("SELECT actor_key,generation FROM actor_generations ORDER BY actor_key,generation")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]ActorRef{}
	for {
		row, err := s.Step()
		if err != nil {
			t.Fatal(errors.Join(err, s.Close()))
		}
		if !row {
			break
		}
		if s.ColumnCount() != 2 {
			t.Fatal("immutable generation width", s.Close())
		}
		key, e1 := s.Text(0)
		blob, e2 := s.Blob(1)
		if e1 != nil || e2 != nil {
			t.Fatal(errors.Join(e1, e2, s.Close()))
		}
		generation, err := sqliteDecodeUint64(blob)
		if err != nil {
			t.Fatal(errors.Join(err, s.Close()))
		}
		ref, err := sqliteReadHostReceiptGeneration(tx, key, generation)
		if err != nil {
			t.Fatal(errors.Join(err, s.Close()))
		}
		seen[key+"/"+generation] = ref
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, expected) {
		t.Fatal("immutable generation graph has missing or invented identities")
	}
}

// UUID equivalence uses retained one-to-one edges, never replaces allocations.
// Collision and missing-edge checks keep this oracle from masking extra rows.
func ncQACanonical(t *testing.T, st *state, value any) any {
	t.Helper()
	ids := map[string]string{}
	labels := map[string]string{}
	put := func(id, label string) {
		if prior, ok := labels[label]; ok && prior != id {
			t.Fatal("non-unique retained identity relation", label)
		}
		if prior, ok := ids[id]; ok && prior != label {
			t.Fatal("identity aliases distinct relations")
		}
		ids[id] = label
		labels[label] = id
	}
	for key, a := range st.Actors {
		put(a.ID, "actor:"+key)
	}
	for i, ep := range st.Epochs {
		put(ep.ID, fmt.Sprint("epoch:", i))
	}
	for id, s := range st.Segments {
		b, _ := json.Marshal(s.Actor)
		put(id, "segment:"+string(b)+":"+strings.Join(s.EventReferences[:min(1, len(s.EventReferences))], ""))
	}
	for id, u := range st.Uncertainties {
		put(id, "uncertainty:"+ids[u.SegmentID])
	}
	for _, in := range st.Intervals {
		supports := []string{}
		for _, id := range in.SegmentIDs {
			supports = append(supports, ids[id])
		}
		sort.Strings(supports)
		put(in.ID, "interval:"+in.Start.Format(time.RFC3339Nano)+":"+in.End.Format(time.RFC3339Nano)+":"+strings.Join(supports, "|"))
		put(st.Outbox[in.ID].ID, "root:"+ids[in.ID])
	}
	for key, r := range st.HostReceipts {
		if r.Result.Kind == "ClockObservation" || r.Result.Kind == "SourceObservation" {
			ref, _ := json.Marshal(r.Result.Actor)
			label := "wait-loss:" + string(ref) + ":" + r.Result.Source + ":" + r.Result.SessionID + ":" + r.Result.Kind + ":" + r.Result.ObservedAt.Format(time.RFC3339Nano)
			put(r.Result.ID, label)
			put(key, "receipt-key:"+label)
			put(r.Fingerprint, "receipt-fingerprint:"+label)
		}
	}
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err = json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	var rewrite func(any) any
	rewrite = func(x any) any {
		switch y := x.(type) {
		case string:
			if z, ok := ids[y]; ok {
				return z
			}
			if strings.HasPrefix(y, "tempo:") {
				if z, ok := ids[strings.TrimPrefix(y, "tempo:")]; ok {
					return "tempo:" + z
				}
			}
			return y
		case []any:
			for i := range y {
				y[i] = rewrite(y[i])
			}
			return y
		case map[string]any:
			out := map[string]any{}
			for k, v := range y {
				key := k
				if z, ok := ids[k]; ok {
					key = z
				}
				next := rewrite(v)
				if k == "segment_ids" {
					if a, ok := next.([]any); ok {
						sort.Slice(a, func(i, j int) bool { return a[i].(string) < a[j].(string) })
					}
				}
				out[key] = next
			}
			return out
		default:
			return x
		}
	}
	return rewrite(v)
}
func ncQACompare(t *testing.T, q *ncQAFixture, e Event, at int64, wantCode string, wantCalls int) EventResult {
	t.Helper()
	return ncQACompareSample(t, q, e, ncQASample(at), wantCode, wantCalls)
}
func ncQACompareSample(t *testing.T, q *ncQAFixture, e Event, sample ClockSample, wantCode string, wantCalls int) EventResult {
	t.Helper()
	q.sample = sample
	q.legacy.sample = sample
	q.legacy.clockErr = q.sampleErr
	beforeCalls := q.clockCalls
	want, wantErr := q.legacy.service.Ingest(context.Background(), e)
	got, gotErr := q.service.ingestSQLite(context.Background(), e)
	if ncQAErrorCode(wantErr) != wantCode || ncQAErrorCode(gotErr) != wantCode {
		t.Fatalf("legacy=%v SQLite=%v expected=%s", wantErr, gotErr, wantCode)
	}
	if q.clockCalls-beforeCalls != wantCalls {
		t.Fatalf("samples=%d want=%d", q.clockCalls-beforeCalls, wantCalls)
	}
	wantState := bgQAReadLegacy(t, q.legacy.service)
	gotState := ncQAReadState(t, q)
	if !reflect.DeepEqual(ncQACanonical(t, wantState, wantState), ncQACanonical(t, gotState, gotState)) {
		t.Fatal("complete reopened retained state differs from actual legacy commit oracle")
	}
	if !reflect.DeepEqual(ncQACanonical(t, wantState, want), ncQACanonical(t, gotState, got)) {
		t.Fatalf("public outcome differs legacy=%+v SQL=%+v", want, got)
	}
	ncQAAudit(t, q.f)
	return got
}

func ncQAAdmission(f interopFixture, deadline time.Time) sqliteCaptureAdmission {
	return sqliteCaptureAdmission{Directory: f.directory, StateBasename: f.authority, DatabaseBasename: f.database, AcquireDeadline: deadline}
}
func ncQAReduceFixture(t *testing.T, q *ncQAFixture, e Event, setup func(*sqliteio.Tx)) (*ncQAConnection, sqliteStoreMeta, sqliteEventTransition, error) {
	t.Helper()
	q.sample = ncQASample(10)
	admission := ncQAAdmission(q.f, time.Now().Add(time.Second))
	p, clock, found, err := q.service.sqlitePrepareCapture(context.Background(), e, admission)
	if err != nil || !found {
		t.Fatal("real direct reducer preparation", err)
	}
	c, tx, m, found, err := sqliteOpenCapture(context.Background(), admission, sqliteio.Write)
	if err != nil || !found {
		t.Fatal("real admitted direct reducer writer", err)
	}
	w := &ncQAConnection{c: c, tx: tx}
	t.Cleanup(func() {
		if !w.ended {
			if err := w.tx.Rollback(); err != nil {
				t.Errorf("checked direct reducer rollback: %v", err)
			}
		}
		if !w.closed {
			if err := w.c.Close(context.Background()); err != nil {
				t.Errorf("checked direct reducer close: %v", err)
			}
		}
	})
	if setup != nil {
		setup(tx)
	}
	r, err := sqliteReduceEvent(tx, m, e, p, &clock, false)
	return w, m, r, err
}
func ncQAFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(b)
		out[e.Name()] = hex.EncodeToString(h[:])
	}
	return out
}
