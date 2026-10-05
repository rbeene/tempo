//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"golang.org/x/sys/unix"
)

// These fixtures use the old host API, real hookstate lock and real artifact
// hashes. No replacement Eligibility callback or proposed deadline field exists.
type habQAPlan struct {
	consume, policyHold, writerAfterUnlock, cancelAfter, caller time.Duration
	expireBeforePolicy                                          bool
}
type habQALock struct {
	f    *os.File
	once sync.Once
	err  error
}

func habQALockPolicy(t *testing.T, path string) *habQALock {
	t.Helper()
	f, err := os.OpenFile(path+".lock", os.O_RDWR|unix.O_NOFOLLOW, 0)
	l := &habQALock{f: f}
	t.Cleanup(func() {
		if err := l.close(); err != nil {
			t.Error("owned policy lock cleanup", err)
		}
	})
	if err != nil {
		t.Fatal("SETUP existing policy lock open", err)
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatal("SETUP private policy lock", err)
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal("SETUP actual exclusive policy lock", err)
	}
	probe, err := os.OpenFile(path+".lock", os.O_RDWR|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal("SETUP independent policy lock probe", err)
	}
	probeErr := unix.Flock(int(probe.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	var unlockErr error
	if probeErr == nil {
		unlockErr = unix.Flock(int(probe.Fd()), unix.LOCK_UN)
	}
	closeErr := probe.Close()
	if (!errors.Is(probeErr, unix.EWOULDBLOCK) && !errors.Is(probeErr, unix.EAGAIN)) || unlockErr != nil || closeErr != nil {
		t.Fatal("SETUP independent file description did not observe real policy lock contention", errors.Join(probeErr, unlockErr, closeErr))
	}
	return l
}
func (l *habQALock) close() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		if l.f != nil {
			l.err = errors.Join(unix.Flock(int(l.f.Fd()), unix.LOCK_UN), l.f.Close())
		}
	})
	return l.err
}

type habQARun struct {
	h                                                                                            *hiQAFixture
	plan                                                                                         habQAPlan
	binding                                                                                      BindingSnapshot
	before                                                                                       sqliteStoreMeta
	rows                                                                                         map[string][][]string
	policy                                                                                       []byte
	d                                                                                            *HostCaptureDiagnostics
	r                                                                                            HostReceipt
	err, callerErr                                                                               error
	row                                                                                          HostCaptureDiagnosticAttempt
	resolverCalls, beginAfterResolver                                                            int
	resolverEntered, resolverReturned, ended, lockReleased, writerReleaseStarted, writerReleased time.Time
	lockErr                                                                                      error
	leases, acquired, released                                                                   atomic.Int64
	resolverDone                                                                                 bool
	stop, done                                                                                   chan struct{}
	stopOnce                                                                                     sync.Once
	writerRelease                                                                                func()
}

func habQANew(t *testing.T, plan habQAPlan) *habQARun {
	t.Helper()
	q := &habQARun{h: hiQANew(t, "codex", 1), plan: plan}
	q.before, q.rows = mwQAAudit(t, q.h.f)
	owner := mwQAOpen(t, q.h.f, sqliteio.Read)
	r, found, err := sqliteReadBindingLocation(owner.tx, q.before.ComputerID, "directory", q.h.cwd[0])
	cleanup := owner.finish(false)
	owner.reported = cleanup != nil
	if err != nil || cleanup != nil || !found || r.Record == nil {
		t.Fatal("SETUP actual owned binding", errors.Join(err, cleanup))
	}
	q.binding = r.Snapshot
	q.policy, err = os.ReadFile(q.h.policyPath)
	if err != nil {
		t.Fatal("SETUP retained policy bytes", err)
	}
	if q.h.s.store.timeout != 0 {
		t.Fatal("SETUP host default250 changed")
	}
	if plan.writerAfterUnlock > 0 {
		q.writerRelease = ncQAOwnedWriter(t, q.h.f)
	}
	return q
}
func habQAWait(ctx context.Context, until time.Time) {
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
func (q *habQARun) stopAndJoin() {
	if q.done == nil {
		return
	}
	q.stopOnce.Do(func() { close(q.stop) })
	<-q.done
}
func (q *habQARun) coordinate(t *testing.T, lock *habQALock, cancel context.CancelFunc) {
	q.stop, q.done = make(chan struct{}), make(chan struct{})
	start := time.Now()
	t.Cleanup(q.stopAndJoin)
	go func() {
		defer close(q.done)
		wait := func(at time.Time) {
			timer := time.NewTimer(time.Until(at))
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-q.stop:
			}
		}
		if q.plan.cancelAfter > 0 {
			wait(start.Add(q.plan.cancelAfter))
			cancel()
		}
		wait(start.Add(q.plan.policyHold))
		q.lockErr = lock.close()
		q.lockReleased = time.Now()
		if q.writerRelease != nil {
			wait(start.Add(q.plan.policyHold + q.plan.writerAfterUnlock))
			// This existing helper sends release, drains and checks Wait/PID/PG.
			q.writerReleaseStarted = time.Now()
			q.writerRelease()
			q.writerReleased = time.Now()
		}
	}()
}
func (q *habQARun) invoke(t *testing.T) {
	t.Helper()
	caller := q.plan.caller
	if caller == 0 {
		caller = 900 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), caller)
	defer cancel()
	q.d = NewHostCaptureDiagnostics()
	ctx = q.d.Begin(ctx)
	ncQASetFSHooks(ncQAFSHooks{Observe: func(e ncQAFSEvent) {
		if e.Role != "root" || e.Op != "lease" {
			return
		}
		if e.Phase == "acquired" {
			q.acquired.Add(1)
			q.leases.Add(1)
		}
		if e.Phase == "released" {
			q.released.Add(1)
			q.leases.Add(-1)
		}
	}})
	ncQASetSQLHooks(ncQASQLHooks{Observe: func(e ncQASQLEvent) {
		if q.resolverDone && e.Operation == "begin" && e.Phase == "control-before-native" {
			q.beginAfterResolver++
		}
	}})
	t.Cleanup(func() { ncQASetFSHooks(ncQAFSHooks{}); ncQASetSQLHooks(ncQASQLHooks{}) })
	q.h.s = NewSQLite(Options{Path: q.h.path, HookPolicies: q.h.policies, Clock: q.h.s.clock, ResolveBinding: func(c context.Context, e Event) (BindingSnapshot, bool, error) {
		q.resolverCalls++
		q.resolverEntered = time.Now()
		if q.leases.Load() != 0 || e.CWD != q.h.cwd[0] {
			t.Fatal("SETUP resolver must follow checked native release")
		}
		row := q.d.Snapshot()[0]
		deadline := q.d.start.Add(time.Duration(row.DeadlineUS) * time.Microsecond)
		target := q.d.start.Add(time.Duration(row.StartUS)*time.Microsecond + q.plan.consume)
		if q.plan.expireBeforePolicy {
			target = deadline.Add(25 * time.Millisecond)
		}
		habQAWait(c, target)
		if !q.plan.expireBeforePolicy {
			if c.Err() != nil || !time.Now().Before(deadline) {
				t.Fatal("SETUP preliminary work exhausted original allowance")
			}
			lock := habQALockPolicy(t, q.h.policyPath)
			q.coordinate(t, lock, cancel)
		}
		q.resolverReturned, q.resolverDone = time.Now(), true
		return q.binding, true, nil
	}})
	e := q.h.event("SessionStart", "", "")
	e.SessionSource = "startup"
	q.r, q.err = q.h.s.IngestHost(ctx, e)
	q.ended, q.callerErr = time.Now(), ctx.Err()
	q.d.End(ctx, false)
	rows := q.d.Snapshot()
	if len(rows) != 1 {
		t.Fatal("one old-API capture attempt")
	}
	q.row = rows[0]
	// Joining precedes reading coordinator fields and every result assertion.
	if q.done != nil {
		<-q.done
	}
	ncQASetFSHooks(ncQAFSHooks{})
	ncQASetSQLHooks(ncQASQLHooks{})
	if q.lockErr != nil || q.leases.Load() != 0 || q.acquired.Load() == 0 || q.acquired.Load() != q.released.Load() {
		t.Fatal("checked lock/native ownership did not end", q.lockErr, q.leases.Load())
	}
	if q.resolverCalls != 1 || q.h.clockCalls != 0 {
		t.Fatal("SessionStart prerequisite resolver/clock multiplicity")
	}
	policy, err := os.ReadFile(q.h.policyPath)
	if err != nil || !reflect.DeepEqual(policy, q.policy) {
		t.Fatal("eligible read changed retained policy", err)
	}
	if _, err := os.Lstat(q.h.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("host capture created selector authority")
	}
	if _, err := os.Lstat(q.h.path + ".lock"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("host capture created initialization selector")
	}
	t.Logf("host admission core start_us=%d policy_begin_us=%d policy_end_us=%d original_deadline_us=%d end_us=%d native_begin_after_resolver=%d leases=%d/%d", q.row.StartUS, q.row.PhaseUS[4], q.row.PhaseUS[5], q.row.DeadlineUS, q.row.EndUS, q.beginAfterResolver, q.acquired.Load(), q.released.Load())
}
func (q *habQARun) policySucceeded(t *testing.T) {
	t.Helper()
	d := q.row.Eligibility
	if d == nil || d.D || d.P[6] < 0 || d.P[0] < 0 || d.P[4] < 0 || d.P[5] != -1 || q.row.PhaseUS[4] < 0 || q.row.PhaseUS[5] <= q.row.PhaseUS[4] || q.row.PhaseUS[4] >= q.row.DeadlineUS {
		t.Fatal("SETUP actual successful eligible lock/read/hash not reached")
	}
	if d.P[0] < (q.plan.policyHold - 20*time.Millisecond).Microseconds() {
		t.Fatal("SETUP actual policy metadata wait did not reach measured lock interval")
	}
	for i, role := range []string{"runtime", "executable", "definitions"} {
		info, err := os.Stat(filepath.Join(q.h.cwd[0], role))
		if err != nil || d.R[i][0] != 1 || d.R[i][1] != 1 || d.R[i][2] != info.Size() || d.R[i][3] < 2 {
			t.Fatal("SETUP each confirmed artifact was not fully reread once", role, err)
		}
	}
	if d.R[3][0] != 0 || d.R[4][0] != 0 || d.R[5][0] != 0 {
		t.Fatal("SETUP unexpected artifact roles")
	}
}
func (q *habQARun) unchanged(t *testing.T) {
	t.Helper()
	m, rows := mwQAAudit(t, q.h.f)
	if !reflect.DeepEqual(m, q.before) || !reflect.DeepEqual(rows, q.rows) {
		t.Fatal("refusal changed all32 rows, metadata, charge or nonce")
	}
}
func (q *habQARun) committed(t *testing.T) {
	t.Helper()
	if q.err != nil || q.r.Disposition != "applied" || q.r.Durability != "committed" || q.r.Origin != "unverified" || q.r.Actor != nil || q.r.ProfileRevision != "1" {
		t.Error("successful policy must retain one committed SessionStart", q.r.Disposition, q.r.Durability, q.err)
	}
	if q.err != nil {
		if ncQAErrorCode(q.err) != "state_busy" || q.r.Durability != "not_committed" {
			t.Error("old RED was not the intended definite admission refusal")
		}
		q.unchanged(t)
		return
	}
	m, rows := mwQAAudit(t, q.h.f)
	if m.ComputerID != q.before.ComputerID || m.Revision != bump(q.before.Revision) || m.DurabilityNonce == q.before.DurabilityNonce || len(rows["host_sessions"]) != 1 || len(rows["host_receipts"]) != 1 {
		t.Fatal("committed SessionStart identity/receipt/nonce projection")
	}
	for table, old := range q.rows {
		if table != "store_meta" && table != "host_sessions" && table != "host_receipts" && !reflect.DeepEqual(old, rows[table]) {
			t.Fatal("SessionStart changed unrelated typed table", table)
		}
	}
	list, err := q.h.s.HostReceipts(context.Background(), HostReceiptFilter{})
	if err != nil || len(list.Receipts) != 1 || !reflect.DeepEqual(list.Receipts[0], q.r) {
		t.Fatal("actual cold receipt differs from committed result", err)
	}
	// Holding the real policy lock makes replay bypass observable without a mock.
	lock := habQALockPolicy(t, q.h.policyPath)
	q.h.s.resolve = func(context.Context, Event) (BindingSnapshot, bool, error) {
		t.Error("exact replay reentered live resolver")
		return q.binding, true, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	d := NewHostCaptureDiagnostics()
	ctx = d.Begin(ctx)
	e := q.h.event("SessionStart", "", "")
	e.SessionSource = "startup"
	r, err := q.h.s.IngestHost(ctx, e)
	d.End(ctx, false)
	cancel()
	closeErr := lock.close()
	want := q.r
	want.Disposition = "duplicate"
	if err != nil || closeErr != nil || !reflect.DeepEqual(r, want) || d.Snapshot()[0].Eligibility.P[6] != -1 {
		t.Error("exact receipt replay did not bypass unavailable policy", errors.Join(err, closeErr))
	}
	mwQANonceOnly(t, m, rows, q.h.f)
}
