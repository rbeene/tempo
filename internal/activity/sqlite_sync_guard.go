//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"sync"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// This private adapter preserves terminal cleanup evidence separately from its
// native pointer. It does not select a backend or activate any public route.
type sqliteSyncRunGuard struct {
	mu       sync.Mutex
	native   *sqliteio.SyncRunGuard
	closed   bool
	closeErr error
}

func (s *Service) acquireSQLiteSyncLock(ctx context.Context, deadline time.Time) (*sqliteSyncRunGuard, error) {
	if s == nil || s.store == nil {
		return nil, failure("validation")
	}
	if s.store.timeout < 0 || s.store.timeout > time.Second {
		return nil, failure("validation")
	}
	directory, authority, database, err := sqliteLocation(s.store.path)
	if err != nil {
		return nil, err
	}
	native, err := sqliteio.AcquireSyncRunGuard(ctx, directory, authority, database, deadline)
	if native == nil {
		return nil, err
	}
	return &sqliteSyncRunGuard{native: native}, err
}
func (g *sqliteSyncRunGuard) Verify() error {
	if g == nil {
		return &sqliteio.Error{Phase: sqliteio.VerifyPhase, Category: sqliteio.Closed, Cause: sqliteio.ErrClosed}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.native == nil || g.closed {
		return &sqliteio.Error{Phase: sqliteio.VerifyPhase, Category: sqliteio.Closed, Cause: sqliteio.ErrClosed}
	}
	return g.native.Verify()
}
func (g *sqliteSyncRunGuard) Close() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return g.closeErr
	}
	if g.native == nil {
		return &sqliteio.Error{Phase: sqliteio.ClosePhase, Category: sqliteio.Closed, Cause: sqliteio.ErrClosed}
	}
	terminal, err := g.native.Close()
	if terminal {
		g.closeErr, g.closed = err, true
		g.native = nil
	}
	return err
}
