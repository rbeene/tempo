//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// A fresh sync pass may reclaim WAL before any reservation or provider work.
// This owner never writes semantic rows or a durability nonce. Its read must
// commit conclusively before the adapter permits one explicit checkpoint.
func sqliteSyncNowMaintainWAL(ctx context.Context, a sqliteCaptureAdmission, guard *sqliteSyncRunGuard, requestID string) (err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	defer func() {
		cleanup, owner := sqliteLinkCleanup(tx, c)
		if err != nil || cleanup != nil || owner != nil {
			err = sqliteLinkFailure(err, cleanup, owner, false, false, requestID, nil)
		}
	}()
	// Observe already qualified this existing store, and the caller holds a
	// verified sync guard. Preserve its original admission deadline.
	c, err = sqliteio.Open(ctx, a.Directory, a.DatabaseBasename, sqliteio.Options{AcquireDeadline: a.AcquireDeadline})
	if err != nil {
		return err
	}
	tx, err = c.Begin(ctx, sqliteio.Read)
	if err != nil {
		return err
	}
	if _, err = sqliteReadCaptureSchema(tx, a.StateBasename, a.DatabaseBasename); err != nil {
		return err
	}
	files, err := tx.Footprint()
	if err != nil {
		return err
	}
	if files.WAL < sqliteio.JournalLimitBytes {
		// The ordinary cleanup rolls back this read and closes without a
		// checkpoint; small WALs do not gain an extra native COMMIT.
		return nil
	}
	outcome, err := tx.Commit()
	if err != nil {
		return err
	}
	if outcome != sqliteio.Committed {
		return failure("state_corrupt")
	}
	tx = nil
	if err = guard.Verify(); err != nil {
		return err
	}
	// Truncate can free physical WAL space before the write-pressure gate.
	// Checkpoint owns its zero busy timeout and consumes this clean read once.
	_, err = c.Checkpoint(ctx, sqliteio.Truncate)
	return err
}
