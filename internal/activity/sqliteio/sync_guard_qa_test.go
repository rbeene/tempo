//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestSQLiteSyncGuardSQLClaimProviderRendezvousACKWithExternalContender(t *testing.T) {
	dir := qaDirectory(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	g := sgQAAcquire(t, dir, sgQAState, sgQADB, ctx, time.Now().Add(90*time.Millisecond))
	claim := qaOpen(t, dir, sgQADB, true)
	tx := qaBegin(t, claim, ctx, Write)
	qaDone(t, tx, "CREATE TABLE qa_sync_receipts(id INTEGER PRIMARY KEY, phase TEXT NOT NULL)")
	qaDone(t, tx, "INSERT INTO qa_sync_receipts VALUES(1,'claimed')")
	qaCommit(t, tx)
	if err := claim.CloseDurably(ctx); err != nil {
		t.Fatal("actual claim durable close", err)
	}
	// Expired admission budget cannot expire a live, already acquired guard.
	time.Sleep(100 * time.Millisecond)
	if err := g.Verify(); err != nil {
		t.Fatal("held guard failed verification after admission budget", err)
	}
	before := statsForTest()
	if before.Active != 0 || before.NativeActive != 0 || before.SyncGuards != 1 {
		t.Fatalf("mock provider interval still owned SQL or lost guard: %+v", before)
	}
	// A bounded no-SQL mock POST rendezvous. The competing process attempts a
	// genuine flock while the synthetic effect is pending.
	posted := make(chan struct{})
	releasePOST := make(chan struct{})
	completed := make(chan struct{})
	var releaseOnce sync.Once
	finishPOST := func() {
		releaseOnce.Do(func() { close(releasePOST) })
		<-completed
	}
	go func() {
		close(posted)
		<-releasePOST
		close(completed)
	}()
	t.Cleanup(finishPOST)
	select {
	case <-posted:
	case <-time.After(time.Second):
		t.Fatal("mock POST rendezvous stalled")
	}
	sgQAStartChild(t, "try", dir, sgQAState, sgQADB)
	if now := statsForTest(); now.Active != 0 || now.NativeActive != 0 || now.SyncGuards != 1 {
		t.Fatalf("external contention altered provider-interval ownership: %+v", now)
	}
	finishPOST()
	if err := g.Verify(); err != nil {
		t.Fatal("guard lost before ACK", err)
	}
	ack := qaOpen(t, dir, sgQADB, false)
	ackTx := qaBegin(t, ack, ctx, Write)
	qaDone(t, ackTx, "UPDATE qa_sync_receipts SET phase='acknowledged' WHERE id=1")
	qaCommit(t, ackTx)
	if err := ack.CloseDurably(ctx); err != nil {
		t.Fatal("actual ACK durable close", err)
	}
	sgQAClose(t, g)
	if n := qaCountSyncReceipts(t, dir); n != 1 {
		t.Fatal("cold ACK receipt was not persisted", n)
	}
	if now := statsForTest(); now.Active != 0 || now.NativeActive != 0 || now.SyncGuards != 0 {
		t.Fatalf("terminal guard/SQL ownership: %+v", now)
	}
}

func qaCountSyncReceipts(t *testing.T, dir string) int64 {
	t.Helper()
	c := qaOpen(t, dir, sgQADB, false)
	tx := qaBegin(t, c, context.Background(), Read)
	s := qaPrepare(t, tx, "SELECT count(*) FROM qa_sync_receipts WHERE phase='acknowledged'")
	row, err := s.Step()
	if !row || err != nil {
		t.Fatal("cold receipt read", err)
	}
	n, err := s.Int64(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
	return n
}

func TestSQLiteSyncGuardFixedNamespaceAndExistingFile(t *testing.T) {
	dir := qaDirectory(t)
	trace := &freshQATrace{}
	qaFSHooks(t, hooks{Observe: trace.observeFS})
	g := sgQAAcquire(t, dir, sgQAState, sgQADB, context.Background(), time.Now().Add(time.Second))
	lockName := sgQAState + ".sync.lock"
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != lockName {
		t.Fatalf("guard created unexpected namespace: entries=%d err=%v", len(entries), err)
	}
	sgQAClose(t, g)
	lockPath := filepath.Join(dir, lockName)
	want := []byte("opaque fixture bytes")
	if err := os.WriteFile(lockPath, want, 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	g = sgQAAcquire(t, dir, sgQAState, sgQADB, context.Background(), time.Now().Add(time.Second))
	sgQAClose(t, g)
	after, err := os.Stat(lockPath)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("existing guard inode changed", err)
	}
	b, err := os.ReadFile(lockPath)
	if err != nil || !bytes.Equal(b, want) {
		t.Fatal("existing guard contents changed", err)
	}
	for _, e := range trace.snapshot() {
		if e.Op == "fsync" {
			t.Fatal("empty coordination lock file needlessly synced")
		}
	}
	if err := os.WriteFile(filepath.Join(dir, sgQAState), []byte("occupied authority"), 0600); err != nil {
		t.Fatal(err)
	}
	owner, err := AcquireSyncRunGuard(context.Background(), dir, sgQAState, sgQADB, time.Now().Add(time.Second))
	sgQAOwn(t, owner)
	if owner != nil || !errors.Is(err, os.ErrExist) {
		t.Fatal("occupied authority was not a bare existence refusal", err)
	}
}

func TestSQLiteSyncGuardSameNamespaceWaitAndOtherNamespace(t *testing.T) {
	dir := qaDirectory(t)
	g := sgQAAcquire(t, dir, sgQAState, sgQADB, context.Background(), time.Now().Add(time.Second))
	trace := &freshQATrace{}
	qaFSHooks(t, hooks{Observe: trace.observeFS})
	other := sgQAAcquire(t, dir, "other-state.json", "other.sqlite3", context.Background(), time.Now().Add(time.Second))
	if err := other.Verify(); err != nil {
		t.Fatal("distinct sync namespace did not coexist", err)
	}
	sgQAClose(t, other)
	beforeWait := len(trace.snapshot())
	start := time.Now()
	duplicate, err := AcquireSyncRunGuard(context.Background(), dir, sgQAState, "second.sqlite3", start.Add(40*time.Millisecond))
	sgQAOwn(t, duplicate)
	if duplicate != nil || !errors.Is(err, ErrBusy) {
		t.Fatal("same S with distinct valid D escaped owner gate", err)
	}
	for _, e := range trace.snapshot()[beforeWait:] {
		if e.Role == "sync-guard" && e.Op == "open" {
			t.Fatal("same-process waiter opened redundant S descriptor")
		}
	}
	sgQAClose(t, g)
	if again := sgQAAcquire(t, dir, sgQAState, sgQADB, context.Background(), time.Now().Add(time.Second)); again != nil {
		sgQAClose(t, again)
	}
}

func TestSQLiteSyncGuardChildDeathReleasesActualFlock(t *testing.T) {
	dir := qaDirectory(t)
	child := sgQAStartChild(t, "hold", dir, sgQAState, sgQADB)
	owner, err := AcquireSyncRunGuard(context.Background(), dir, sgQAState, sgQADB, time.Now().Add(50*time.Millisecond))
	sgQAOwn(t, owner)
	if owner != nil || !errors.Is(err, ErrBusy) {
		t.Fatal("cross-process actual flock did not exclude parent", err)
	}
	child.join(t, true)
	g := sgQAAcquire(t, dir, sgQAState, sgQADB, context.Background(), time.Now().Add(time.Second))
	sgQAClose(t, g)
}

func TestSQLiteSyncGuardCanceledBudgetAndVerifiedLifetime(t *testing.T) {
	dir := qaDirectory(t)
	deadline := time.Now().Add(-time.Millisecond)
	owner, err := AcquireSyncRunGuard(context.Background(), dir, sgQAState, sgQADB, deadline)
	sgQAOwn(t, owner)
	if owner != nil || !errors.Is(err, ErrBusy) {
		t.Fatal("live expired budget was not busy", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	owner, err = AcquireSyncRunGuard(ctx, dir, sgQAState, sgQADB, deadline)
	sgQAOwn(t, owner)
	if owner != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("actual cancellation lost precedence", err)
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil || len(entries) != 0 {
		t.Fatalf("pre-admission refusal mutated namespace: entries=%d err=%v", len(entries), readErr)
	}
	live, stop := context.WithCancel(context.Background())
	g := sgQAAcquire(t, dir, sgQAState, sgQADB, live, time.Now().Add(20*time.Millisecond))
	time.Sleep(30 * time.Millisecond)
	if err := g.Verify(); err != nil {
		t.Fatal("admission deadline incorrectly became ownership lifetime", err)
	}
	stop()
	if err := g.Verify(); !errors.Is(err, context.Canceled) {
		t.Fatal("verify ignored original action cancellation", err)
	}
	sgQAClose(t, g)
}

func TestSQLiteSyncGuardVerifyReplacementAndSharedTerminalClose(t *testing.T) {
	dir := qaDirectory(t)
	g := sgQAAcquire(t, dir, sgQAState, sgQADB, context.Background(), time.Now().Add(time.Second))
	path := filepath.Join(dir, sgQAState+".sync.lock")
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	want := []byte("replacement must survive")
	if err := os.WriteFile(path, want, 0600); err != nil {
		t.Fatal(err)
	}
	if err := g.Verify(); !errors.Is(err, ErrUnsafe) {
		t.Fatal("verify accepted replacement S", err)
	}
	sgQAClose(t, g)
	actual, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(actual, want) {
		t.Fatal("close modified replacement S", err)
	}
	var nilGuard *SyncRunGuard
	if terminal, err := nilGuard.Close(); !terminal || err != nil {
		t.Fatal("nil close did not terminate harmlessly", terminal, err)
	}
	if nilGuard.Verify() == nil {
		t.Fatal("nil guard verified")
	}
	var zero SyncRunGuard
	if terminal, err := zero.Close(); terminal || !errors.Is(err, ErrClosed) {
		t.Fatal("zero owner invented terminal cleanup", terminal, err)
	}
	if zero.Verify() == nil {
		t.Fatal("zero guard verified")
	}
	// A fresh namespace proves copies share the one actual owner disposition.
	other := sgQAAcquire(t, dir, "copy-state.json", "copy.sqlite3", context.Background(), time.Now().Add(time.Second))
	copyGuard := *other
	var wg sync.WaitGroup
	type closeResult struct {
		terminal bool
		err      error
	}
	results := make(chan closeResult, 2)
	for _, owner := range []*SyncRunGuard{other, &copyGuard} {
		wg.Add(1)
		go func(owner *SyncRunGuard) {
			defer wg.Done()
			terminal, closeErr := owner.Close()
			results <- closeResult{terminal, closeErr}
		}(owner)
	}
	wg.Wait()
	close(results)
	for result := range results {
		if !result.terminal || result.err != nil {
			t.Fatal("copied concurrent close did not share terminal outcome", result)
		}
	}
	if err := other.Verify(); err == nil {
		t.Fatal("closed guard verified")
	}
	if terminal, err := other.Close(); !terminal || err != nil {
		t.Fatal("repeated terminal close changed outcome", terminal, err)
	}
}

func TestSQLiteSyncGuardUnsafeEntryModesAndNames(t *testing.T) {
	for _, setup := range []struct {
		name string
		make func(string) error
	}{
		{"symlink", func(path string) error { return os.Symlink("missing", path) }},
		{"directory", func(path string) error { return os.Mkdir(path, 0700) }},
		{"world-readable", func(path string) error {
			if err := os.WriteFile(path, nil, 0600); err != nil {
				return err
			}
			return os.Chmod(path, 0644)
		}},
		{"linked", func(path string) error {
			if err := os.WriteFile(path+".peer", nil, 0600); err != nil {
				return err
			}
			return os.Link(path+".peer", path)
		}},
	} {
		t.Run(setup.name, func(t *testing.T) {
			dir := qaDirectory(t)
			if err := setup.make(filepath.Join(dir, sgQAState+".sync.lock")); err != nil {
				t.Fatal(err)
			}
			g, err := AcquireSyncRunGuard(context.Background(), dir, sgQAState, sgQADB, time.Now().Add(time.Second))
			sgQAOwn(t, g)
			if g != nil || err == nil {
				t.Fatal("unsafe S entry admitted", err)
			}
		})
	}
	for _, names := range [][2]string{{"", sgQADB}, {"bad/name", sgQADB}, {sgQAState, sgQAState + ".sync.lock"}} {
		dir := qaDirectory(t)
		g, err := AcquireSyncRunGuard(context.Background(), dir, names[0], names[1], time.Now().Add(time.Second))
		sgQAOwn(t, g)
		if g != nil || err == nil {
			t.Fatal("invalid/colliding raw name admitted", err)
		}
		entries, readErr := os.ReadDir(dir)
		if readErr != nil || len(entries) != 0 {
			t.Fatal("invalid name mutated namespace", readErr)
		}
	}
	// Explicit OS control: some filesystems reject raw non-UTF8 bytes.
	dir := qaDirectory(t)
	raw := "raw-\xff-state"
	fd, err := unix.Open(filepath.Join(dir, raw+".sync.lock"), unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		g, acquireErr := AcquireSyncRunGuard(context.Background(), dir, raw, sgQADB, time.Now().Add(time.Second))
		sgQAOwn(t, g)
		if g != nil || acquireErr == nil {
			t.Fatal("producer accepted a raw name the OS refused", acquireErr)
		}
		return
	}
	if err := unix.Close(fd); err != nil {
		t.Fatal(err)
	}
	g := sgQAAcquire(t, dir, raw, sgQADB, context.Background(), time.Now().Add(time.Second))
	sgQAClose(t, g)
}
