//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func TestSQLiteSyncGuardAdapterNamesSQLPhasesAndIdempotentClose(t *testing.T) {
	f := interopLocation(t)
	s := New(Options{Path: filepath.Join(f.directory, f.authority)})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	g, err := s.acquireSQLiteSyncLock(ctx, time.Now().Add(70*time.Millisecond))
	if g != nil {
		t.Cleanup(func() { _ = g.Close() })
	}
	if err != nil || g == nil {
		t.Fatal("adapter failed to acquire native guard", err)
	}
	entries, err := os.ReadDir(f.directory)
	if err != nil || len(entries) != 1 || entries[0].Name() != f.authority+".sync.lock" {
		t.Fatal("adapter changed sqliteLocation namespace or opened P/D", err)
	}
	// The adapter owns S while an independent native SQL lease claims and closes.
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	interopDone(t, tx, "CREATE TABLE adapter_qa_claim(id INTEGER PRIMARY KEY) STRICT")
	interopCommit(t, tx)
	if err := c.CloseDurably(ctx); err != nil {
		t.Fatal("adapter-held SQL claim close", err)
	}
	time.Sleep(80 * time.Millisecond)
	if err := g.Verify(); err != nil {
		t.Fatal("adapter guard expired with acquisition deadline", err)
	}
	contender, err := sqliteio.AcquireSyncRunGuard(ctx, f.directory, f.authority, f.database, time.Now().Add(30*time.Millisecond))
	if contender != nil {
		t.Cleanup(func() { _, _ = contender.Close() })
	}
	if contender != nil || !errors.Is(err, sqliteio.ErrBusy) {
		t.Fatal("adapter did not retain actual native S ownership", err)
	}
	if err := g.Close(); err != nil {
		t.Fatal("adapter close", err)
	}
	if err := g.Close(); err != nil {
		t.Fatal("adapter terminal outcome changed", err)
	}
	if err := g.Verify(); err == nil {
		t.Fatal("closed adapter guard verified")
	}
	post, err := sqliteio.AcquireSyncRunGuard(ctx, f.directory, f.authority, f.database, time.Now().Add(time.Second))
	if post != nil {
		t.Cleanup(func() { _, _ = post.Close() })
	}
	if err != nil || post == nil {
		t.Fatal("adapter failed to release native S", err)
	}
	if terminal, closeErr := post.Close(); !terminal || closeErr != nil {
		t.Fatal("post-adapter native guard close", terminal, closeErr)
	}
}

func TestSQLiteSyncGuardAdapterRejectsInvalidTimeoutBeforeMutation(t *testing.T) {
	f := interopLocation(t)
	s := New(Options{Path: filepath.Join(f.directory, f.authority), LockTimeout: -time.Second})
	g, err := s.acquireSQLiteSyncLock(context.Background(), time.Now().Add(time.Second))
	if g != nil {
		t.Cleanup(func() { _ = g.Close() })
	}
	if g != nil || err == nil {
		t.Fatal("invalid configured timeout admitted", err)
	}
	entries, readErr := os.ReadDir(f.directory)
	if readErr != nil || len(entries) != 0 {
		t.Fatal("invalid timeout mutated guard namespace", readErr)
	}
}
