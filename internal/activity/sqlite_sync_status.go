//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func (s *Service) syncStatusSQLite(ctx context.Context) (SyncStatus, error) {
	if ctx == nil || s == nil || s.store == nil {
		return SyncStatus{}, failure("validation")
	}
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	a, _, err := s.sqliteSyncConfigAdmission(ctx)
	if err != nil {
		return SyncStatus{}, err
	}
	result, err := sqliteSyncStatusRead(ctx, a)
	if err != nil {
		return SyncStatus{}, err
	}
	if err = ctx.Err(); err != nil {
		return SyncStatus{}, sqliteCaptureError(err)
	}
	// The read transaction and exact native owner have been checked released.
	// This observer cannot replace authoritative counts or enabled consent.
	result.Worker = s.observedWorker(ctx, result.Worker)
	return result, nil
}

func sqliteSyncStatusRead(ctx context.Context, a sqliteCaptureAdmission) (result SyncStatus, err error) {
	var c *sqliteio.Conn
	var tx *sqliteio.Tx
	defer func() {
		cleanup := sqliteCaptureCleanup(c, tx)
		if err != nil || cleanup != nil {
			err = sqliteCaptureError(errors.Join(err, cleanup))
			result = SyncStatus{}
		}
	}()
	var kind sqliteio.LinkInspection
	c, kind, err = sqliteio.InspectForLink(ctx, a.Directory, a.StateBasename, a.DatabaseBasename, a.AcquireDeadline)
	if err != nil {
		return result, err
	}
	if kind == sqliteio.LinkAbsent || kind == sqliteio.LinkPristine {
		if c != nil {
			return result, failure("state_corrupt")
		}
		return syncStatusProjection("0", false, []SyncConfiguration{}, []OutboxItem{})
	}
	if kind != sqliteio.LinkWAL || c == nil {
		return result, failure("state_corrupt")
	}
	tx, err = c.Begin(ctx, sqliteio.Read)
	if err != nil {
		return result, err
	}
	meta, current, err := sqliteReadLinkSchema(tx, a.StateBasename, a.DatabaseBasename)
	if err != nil {
		return result, err
	}
	if !current {
		return syncStatusProjection("0", false, []SyncConfiguration{}, []OutboxItem{})
	}
	return sqliteSyncStatusSnapshot(tx, meta)
}

func sqliteSyncStatusSnapshot(tx *sqliteio.Tx, meta sqliteStoreMeta) (SyncStatus, error) {
	if !sqliteDependencyScope(meta.ComputerID, meta.Revision) {
		return SyncStatus{}, failure("validation")
	}
	configurations, err := sqliteSyncConfigurations(tx)
	if err != nil {
		return SyncStatus{}, err
	}
	// SG16 retains an orphan through LEFT JOIN so it cannot disappear from a
	// successful status. Native INTEGER kinds, order and endpoint are checked.
	s, err := tx.Prepare("SELECT o.interval_id,o.id,i.start_sec,i.start_nsec FROM outbox AS o LEFT JOIN intervals AS i ON i.interval_id=o.interval_id ORDER BY i.start_sec,i.start_nsec,o.id")
	if err != nil {
		return SyncStatus{}, err
	}
	roots, err := sqliteSyncStatusRoots(s)
	if err != nil {
		return SyncStatus{}, err
	}
	items := make([]OutboxItem, 0, len(roots))
	for _, root := range roots {
		o, found, err := sqliteReadSyncItem(tx, meta.ComputerID, root.id, meta.Revision)
		if err != nil {
			return SyncStatus{}, err
		}
		if !found || o.Interval.ID != root.interval || o.Interval.Start.Unix() != root.seconds || int64(o.Interval.Start.Nanosecond()) != root.nanoseconds {
			return SyncStatus{}, failure("state_corrupt")
		}
		items = append(items, o)
	}
	if len(items) == 0 {
		// Complete item reads check this singleton themselves. Empty history
		// still must not hide a malformed pending reservation/projection.
		if err = sqliteValidateSelectedSync(tx, meta.ComputerID, meta.Revision, nil, nil); err != nil {
			return SyncStatus{}, err
		}
	}
	return syncStatusProjection(meta.Revision, meta.SyncEnabled, configurations, items)
}

type sqliteSyncStatusRoot struct {
	interval, id         string
	seconds, nanoseconds int64
}

func sqliteSyncStatusRoots(s *sqliteio.Stmt) (result []sqliteSyncStatusRoot, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = make([]sqliteSyncStatusRoot, 0)
	for {
		present, e := s.Step()
		if e != nil {
			return nil, e
		}
		if !present {
			return result, nil
		}
		if s.ColumnCount() != 4 {
			return nil, failure("state_corrupt")
		}
		var row sqliteSyncStatusRoot
		if row.interval, err = sqliteDependencyText(s, 0); err != nil {
			return nil, err
		}
		if row.id, err = sqliteDependencyText(s, 1); err != nil {
			return nil, err
		}
		if row.seconds, err = sqliteLocalRangeInteger(s, 2); err != nil {
			return nil, err
		}
		if row.nanoseconds, err = sqliteLocalRangeInteger(s, 3); err != nil {
			return nil, err
		}
		if !validUUID(row.interval) || !validUUID(row.id) || row.nanoseconds < 0 || row.nanoseconds >= 1000000000 {
			return nil, failure("state_corrupt")
		}
		if len(result) > 0 {
			last := result[len(result)-1]
			if last.seconds > row.seconds || last.seconds == row.seconds && (last.nanoseconds > row.nanoseconds || last.nanoseconds == row.nanoseconds && last.id >= row.id) {
				return nil, failure("state_corrupt")
			}
		}
		result = append(result, row)
	}
}
