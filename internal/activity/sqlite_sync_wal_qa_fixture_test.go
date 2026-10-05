//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"golang.org/x/sys/unix"
)

const mwQASoft = int64(4 << 20)

type mwQAFixture struct {
	f         interopFixture
	s         *Service
	sample    ClockSample
	event     Event
	receipt   EventResult
	remaining int
}

func mwQAID(n int) string { return fmt.Sprintf("dc000000-0000-4000-8000-%012d", n) }

func (q *mwQAFixture) reopen() *Service {
	return NewSQLite(Options{Path: filepath.Join(q.f.directory, q.f.authority), Clock: ClockFunc(func() (ClockSample, error) { return q.sample, nil })})
}

// Fresh public Link/capture only. No operation outcome or history is SQL seeded.
func mwQANew(t *testing.T, empty bool) *mwQAFixture {
	t.Helper()
	q := &mwQAFixture{f: interopLocation(t), sample: stQAAt(0), remaining: 1}
	q.s = q.reopen()
	in := qaLinkInput(t)
	linked, err := q.s.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil {
		t.Fatal("SETUP public Link", err)
	}
	snapshot, err := q.s.Status(context.Background())
	if err != nil || snapshot.ComputerID == nil {
		t.Fatal("SETUP public identity", err)
	}
	q.event = Event{ContractVersion: 1, Actor: ActorKey{ComputerID: *snapshot.ComputerID, Source: "manual-test", SessionID: "wal-maintenance", AgentID: "root"}, Generation: "1", Sequence: "1", EventID: "wal-maintenance/1", Kind: "work", BindingID: linked.Binding.ID, BindingRevision: linked.Binding.Revision, CWD: in.Path}
	captureStarted := time.Now()
	q.receipt, err = q.s.Ingest(context.Background(), q.event)
	captureElapsed := time.Since(captureStarted)
	if err != nil || q.receipt.Disposition != "applied" {
		t.Log(sqliteCaptureFixtureFailureLog(captureElapsed, err, q.s.store.timeout == 0))
		t.Fatal("SETUP public capture", err)
	}
	// Exact replay returns the saved receipt with only its disposition projected to duplicate.
	q.receipt.Disposition = "duplicate"
	if empty {
		// An active actor has no finalized outbox root. The enabled batch is empty.
		q.remaining = 0
		if _, err = q.s.SyncResume(context.Background(), mwQAID(1)); err != nil {
			t.Fatal("SETUP public Resume", err)
		}
	} else {
		q.sample = stQAAt(10)
		finish := q.event
		finish.Sequence, finish.EventID, finish.Kind = "2", "wal-maintenance/2", "finish"
		finish.BindingID, finish.BindingRevision, finish.CWD = "", "", ""
		if r, err := q.s.Ingest(context.Background(), finish); err != nil || r.Disposition != "applied" {
			t.Fatal("SETUP public finish", err)
		}
	}
	status, err := q.s.SyncStatus(context.Background())
	if err != nil || status.Enabled != empty || len(status.Items) != q.remaining || q.s.store.timeout != 0 {
		t.Fatal("SETUP paused/empty/default-budget premise", err)
	}
	return q
}

// Retain the exact native owner before examining Open/Begin errors. Native
// failures may return a cleanup-only Conn; the existing bounded owner handles it.
func mwQAOpen(t *testing.T, f interopFixture, mode sqliteio.Mode) *stQAOwner {
	t.Helper()
	c, err := sqliteio.Open(context.Background(), f.directory, f.database, sqliteio.Options{ReadOnly: mode == sqliteio.Read, AcquireDeadline: time.Now().Add(time.Second)})
	owner := &stQAOwner{t: t, c: c}
	t.Cleanup(owner.cleanup)
	if err != nil {
		cleanup := owner.finish(false)
		owner.reported = true
		t.Fatal("owned fixture Open/checked close", errors.Join(err, cleanup))
	}
	tx, err := c.Begin(context.Background(), mode)
	owner.tx = tx
	if err != nil {
		cleanup := owner.finish(false)
		owner.reported = true
		t.Fatal("owned fixture Begin/checked close", errors.Join(err, cleanup))
	}
	return owner
}

func mwQAAudit(t *testing.T, f interopFixture) (sqliteStoreMeta, map[string][][]string) {
	t.Helper()
	o := mwQAOpen(t, f, sqliteio.Read)
	defer o.cleanup()
	m, err := sqliteReadCaptureSchema(o.tx, f.authority, f.database)
	if err != nil {
		t.Fatal("cold exact catalog/meta", err)
	}
	rows, charge := sgQAAudit(t, o.tx, m)
	if len(rows) != 32 || charge != m.LogicalBytes {
		t.Fatal("cold literal32 accounting", len(rows), charge, m.LogicalBytes)
	}
	stQAClose(t, o, false)
	return m, rows
}

func mwQAWAL(t *testing.T, f interopFixture) int64 {
	t.Helper()
	i, err := os.Lstat(filepath.Join(f.directory, f.database+"-wal"))
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil || !i.Mode().IsRegular() {
		t.Fatal("real WAL stat", err)
	}
	return i.Size()
}

func mwQANonceOnly(t *testing.T, before sqliteStoreMeta, rows map[string][][]string, f interopFixture) {
	t.Helper()
	after, got := mwQAAudit(t, f)
	if !reflect.DeepEqual(metaQANext(t, before), after) || !reflect.DeepEqual(flQAWithoutNonce(rows), flQAWithoutNonce(got)) {
		t.Fatal("exact replay did not preserve all32 rows apart from one nonce fence")
	}
}

// A valid private native durability fence changes only the real metadata nonce.
// It runs actual CAS, COMMIT and CloseDurably; no fake file, size or outcome.
func mwQAFence(t *testing.T, f interopFixture) {
	t.Helper()
	o := mwQAOpen(t, f, sqliteio.Write)
	defer o.cleanup()
	if err := o.tx.CheckAuthorityAbsent(f.authority); err != nil {
		t.Fatal("conditioning authority", err)
	}
	m, err := sqliteReadMeta(o.tx, f.authority, f.database)
	if err != nil {
		t.Fatal("conditioning typed meta", err)
	}
	if err = sqliteUpdateMeta(o.tx, m, metaQANext(t, m)); err != nil {
		t.Fatal("conditioning real nonce CAS", err)
	}
	outcome, err := o.tx.Commit()
	o.ended = outcome != sqliteio.NotAttempted
	if err != nil || outcome != sqliteio.Committed {
		t.Fatal("conditioning actual COMMIT", outcome, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = o.c.CloseDurably(ctx); err != nil {
		t.Fatal("conditioning checked durable close", err)
	}
	stQAClose(t, o, false)
}

func mwQAGrow(t *testing.T, f interopFixture) {
	t.Helper()
	m, rows := mwQAAudit(t, f)
	start, n := time.Now(), 0
	for mwQAWAL(t, f) < mwQASoft+(64<<10) {
		if n >= 1200 || time.Since(start) > 2*time.Minute {
			t.Fatal("SETUP bounded real WAL conditioning exhausted", n)
		}
		mwQAFence(t, f)
		n++
	}
	elapsed := time.Since(start)
	after, got := mwQAAudit(t, f)
	want := m
	for i := 0; i < n; i++ {
		want = metaQANext(t, want)
	}
	if !reflect.DeepEqual(want, after) || !reflect.DeepEqual(flQAWithoutNonce(rows), flQAWithoutNonce(got)) {
		t.Fatal("conditioning changed logical history")
	}
	if size := mwQAWAL(t, f); size < mwQASoft || size >= 64<<20 {
		t.Fatal("SETUP genuine soft-only WAL premise", size)
	}
	t.Logf("WAL_CONDITIONING native_nonce_commits=%d elapsed_ns=%d wal_bytes=%d", n, elapsed.Nanoseconds(), mwQAWAL(t, f))
}

type mwQATrace struct {
	mu                                                                                     sync.Mutex
	checkpoints, completed, commits, commitsBefore, guardAcquired, guardReleased           int
	guardHeld, guardVerified, checkpointOwned, ordinaryClosed, beganAfter, nextBeginClosed bool
	code                                                                                   int32
	started                                                                                time.Time
	nativeTime                                                                             time.Duration
}

// Observation only: no injected result, SQL, provider or filesystem mutation.
func mwQARun(t *testing.T, s *Service, id string) (SyncRun, error, *mwQATrace, time.Duration) {
	t.Helper()
	return mwQARunContext(t, context.Background(), s, id)
}

// The explicit post-reader-release retry alone supplies a diagnostic context.
func mwQARunContext(t *testing.T, ctx context.Context, s *Service, id string) (SyncRun, error, *mwQATrace, time.Duration) {
	t.Helper()
	p := &mwQATrace{}
	ncQAHooks(t, ncQASQLHooks{Observe: func(e ncQASQLEvent) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if e.Operation == "commit" && e.Phase == "commit-after-engine" && e.Code == 101 {
			p.commits++
		}
		if e.Phase == "checkpoint-before-native" {
			p.checkpoints++
			p.commitsBefore = p.commits
			p.checkpointOwned = p.guardHeld && p.guardVerified
			p.started = time.Now()
		}
		if e.Phase == "checkpoint-after-engine" {
			p.completed++
			p.code = e.Code
			p.nativeTime = time.Since(p.started)
		}
		if p.completed > 0 && e.Operation == "close" && e.Phase == "close-after" {
			p.ordinaryClosed = true
		}
		if p.completed > 0 && !p.beganAfter && e.Operation == "begin" && e.Phase == "control-before-native" {
			p.beganAfter = true
			p.nextBeginClosed = p.ordinaryClosed
		}
	}})
	ncQASetFSHooks(ncQAFSHooks{Observe: func(e ncQAFSEvent) {
		if e.Role != "sync-guard" {
			return
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if e.Op == "lease" && e.Phase == "acquired" {
			p.guardAcquired++
			p.guardHeld = true
			p.guardVerified = false
		}
		if e.Op == "verify" && e.Phase == "after" {
			p.guardVerified = true
		}
		if e.Op == "lease" && e.Phase == "released" {
			p.guardReleased++
			p.guardHeld = false
		}
	}})
	clear := func() { ncQASetSQLHooks(ncQASQLHooks{}); ncQASetFSHooks(ncQAFSHooks{}) }
	defer clear()
	t.Cleanup(clear)
	start := time.Now()
	r, err := s.SyncNow(ctx, SyncRunInput{RequestID: id}, qaSyncNoProvider(t))
	return r, err, p, time.Since(start)
}

func mwQAMaintained(t *testing.T, f interopFixture, before sqliteStoreMeta, rows map[string][][]string, run SyncRun) {
	t.Helper()
	after, got := mwQAAudit(t, f)
	want := metaQANext(t, metaQANext(t, before)) // Existing reserve and finish; maintenance is read-only.
	want.Revision, want.LogicalBytes = bump(bump(before.Revision)), after.LogicalBytes
	if !reflect.DeepEqual(want, after) || after.LogicalBytes <= before.LogicalBytes {
		t.Fatal("maintenance added a semantic/nonce mutation")
	}
	for table, old := range rows {
		if table != "store_meta" && table != "requests" && !reflect.DeepEqual(old, got[table]) {
			t.Fatal("maintenance rewrote retained table", table)
		}
	}
	oldRequests := [][]string{}
	for _, row := range got["requests"] {
		if row[0] != fmt.Sprintf("%v:%s", sqliteio.TextKind, run.RequestID) {
			oldRequests = append(oldRequests, row)
		}
	}
	if len(got["requests"]) != len(rows["requests"])+1 || !reflect.DeepEqual(rows["requests"], oldRequests) {
		t.Fatal("maintenance changed historical receipts")
	}
	o := mwQAOpen(t, f, sqliteio.Read)
	defer o.cleanup()
	r, found, err := sqliteReadMutationRequestLocal(o.tx, after.ComputerID, run.RequestID, after.Revision)
	if err != nil || !found || r.Value.Operation != "sync.now" || r.Value.Fingerprint != mutationFingerprint("sync.now", SyncRunInput{RequestID: run.RequestID, Limit: 20}) || r.Value.PendingSync != nil || !reflect.DeepEqual(r.Value.SyncRun, &run) {
		t.Fatal("new Now receipt is not exact and terminal", err)
	}
	stQAClose(t, o, false)
	if size := mwQAWAL(t, f); size >= mwQASoft {
		t.Fatal("real checkpoint did not reclaim WAL", size)
	}
}

// One known-owned process holds a genuine native reader. It inherits the outer
// runner's group; only its exact PID is canceled. No raw main-file alias FD.
func TestSQLiteSyncWALOwnedReaderHelper(t *testing.T) {
	if os.Getenv("TEMPO_SYNC_WAL_QA_READER") != "1" {
		return
	}
	f := interopFixture{os.Getenv("TEMPO_SYNC_WAL_QA_DIR"), os.Getenv("TEMPO_SYNC_WAL_QA_A"), os.Getenv("TEMPO_SYNC_WAL_QA_D")}
	if !filepath.IsAbs(f.directory) || f.authority == "" || f.database == "" || filepath.Base(f.authority) != f.authority || filepath.Base(f.database) != f.database {
		t.Fatal("invalid owned reader location")
	}
	o := mwQAOpen(t, f, sqliteio.Read)
	defer o.cleanup()
	before, err := sqliteReadMeta(o.tx, f.authority, f.database)
	if err != nil {
		t.Fatal("actual reader snapshot", err)
	}
	fmt.Println("SYNC_WAL_READER_READY")
	if _, err = io.Copy(io.Discard, os.Stdin); err != nil {
		t.Fatal("reader release", err)
	}
	after, err := sqliteReadMeta(o.tx, f.authority, f.database)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("pinned reader snapshot changed", err)
	}
	stQAClose(t, o, false)
}

func mwQAReader(t *testing.T, f interopFixture) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSQLiteSyncWALOwnedReaderHelper$", "-test.v")
	cmd.Env = append(os.Environ(), "TEMPO_SYNC_WAL_QA_READER=1", "TEMPO_SYNC_WAL_QA_DIR="+f.directory, "TEMPO_SYNC_WAL_QA_A="+f.authority, "TEMPO_SYNC_WAL_QA_D="+f.database)
	var in io.WriteCloser
	var out io.ReadCloser
	var scanned chan struct{}
	var scanErr error
	started := false
	var once sync.Once
	release := func() {
		once.Do(func() {
			if in != nil {
				if err := in.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
					t.Error("reader stdin close", err)
				}
			}
			if started {
				<-scanned
				err := cmd.Wait()
				exit := -1
				if cmd.ProcessState != nil {
					exit = cmd.ProcessState.ExitCode()
				}
				t.Logf("owned WAL reader joined pid=%d exit=%d", cmd.Process.Pid, exit)
				if err != nil || scanErr != nil {
					t.Error("reader Wait/scanner", err, scanErr)
				}
			}
			if out != nil {
				if err := out.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
					t.Error("reader stdout close", err)
				}
			}
			cancel()
		})
	}
	t.Cleanup(release)
	var err error
	in, err = cmd.StdinPipe()
	if err != nil {
		release()
		t.Fatal(err)
	}
	out, err = cmd.StdoutPipe()
	if err != nil {
		release()
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		release()
		t.Fatal(err)
	}
	started = true
	ready := make(chan error, 1)
	scanned = make(chan struct{})
	go func() {
		defer close(scanned)
		sc := bufio.NewScanner(out)
		seen := false
		for sc.Scan() {
			if sc.Text() == "SYNC_WAL_READER_READY" && !seen {
				seen = true
				ready <- nil
			}
		}
		scanErr = sc.Err()
		if !seen {
			ready <- errors.New("reader failed before snapshot handshake")
		}
	}()
	pgid, err := unix.Getpgid(cmd.Process.Pid)
	if err != nil {
		cancel()
		release()
		t.Fatal(err)
	}
	t.Logf("owned WAL reader started pid=%d pgid=%d", cmd.Process.Pid, pgid)
	select {
	case err = <-ready:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		cancel()
		release()
		t.Fatal(err)
	}
	return release
}
