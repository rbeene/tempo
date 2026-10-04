//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"testing"
	"time"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

// This is private bulk-fixture preparation, never production maintenance.
// Conn.Checkpoint correctly forbids the completed Write used to create this
// fixture. Do not change its eligibility flags or expired admission deadline.
// Exactly one direct native checkpoint follows the checked seed COMMIT and
// precedes the first close/reopen; all measured query work remains afterward.
func iwConditionCommittedSeed(t *testing.T, n int, c *Conn, tx *Tx) {
	t.Helper()
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	condition := func() (result CheckpointResult, err error) {
		result.LogFrames, result.CheckpointedFrames = -1, -1
		if c == nil || c.gate == nil || tx == nil || tx.conn != c {
			return result, errors.New("fixture checkpoint missing owned seed")
		}
		if err := c.lockContext(ctx, time.Time{}); err != nil {
			return result, err
		}
		defer c.unlock()
		if c.closed || c.poisoned || c.readOnly || !c.used || c.cleanRead || !c.cleanWrite ||
			c.db == 0 || c.tls == nil || c.authMode == 0 || c.cancelFlag == 0 ||
			tx.mode != Write || !tx.terminal || tx.outcome != Committed || tx.commitErr != nil ||
			tx.cleanupErr != nil || tx.stopInterrupt != nil || len(tx.statements) != 0 ||
			nativeLoad[uint32](c.authMode) != authApplication ||
			lib.Xsqlite3_get_autocommit(c.tls, c.db) != 1 || lib.Xsqlite3_next_stmt(c.tls, c.db, 0) != 0 {
			return result, errors.New("fixture checkpoint lacks terminal clean writer")
		}
		deadline := c.acquireDeadline
		result.Before, err = c.footprint()
		if err != nil {
			return result, err
		}
		result.BeforeValid = true
		if result.Before.WAL <= 0 {
			return result, errors.New("fixture checkpoint lacks retained WAL")
		}
		if rc := lib.Xsqlite3_busy_timeout(c.tls, c.db, 0); rc != lib.SQLITE_OK {
			return result, engineError(CheckpointPhase, rc, ctx)
		}
		name, err := libc.CString("main")
		if err != nil {
			return result, err
		}
		defer libc.Xfree(c.tls, name)
		output := lib.Xsqlite3_malloc64(c.tls, 8)
		if output == 0 {
			return result, engineError(CheckpointPhase, lib.SQLITE_NOMEM, nil)
		}
		defer lib.Xsqlite3_free(c.tls, output)
		libc.AssignPtrInt32(output, -1)
		libc.AssignPtrInt32(output+4, -1)
		libc.AssignPtrUint32(c.authMode, authPragma)
		defer func() { libc.AssignPtrUint32(c.authMode, authApplication) }()
		stop := c.interruptWith(ctx)
		defer stop()
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.Attempted = true
		result.Code = lib.Xsqlite3_wal_checkpoint_v2(c.tls, c.db, name, lib.SQLITE_CHECKPOINT_TRUNCATE, output, output+4)
		result.LogFrames, result.CheckpointedFrames = nativeLoad[int32](output), nativeLoad[int32](output+4)
		if result.Code != lib.SQLITE_OK {
			err = engineError(CheckpointPhase, result.Code, ctx)
		}
		err = errors.Join(err, ctx.Err())
		var verify error
		result.After, verify = c.footprint()
		result.AfterValid = verify == nil
		err = errors.Join(err, verify)
		if c.acquireDeadline != deadline || !c.used || c.cleanRead || !c.cleanWrite ||
			lib.Xsqlite3_get_autocommit(c.tls, c.db) != 1 || lib.Xsqlite3_next_stmt(c.tls, c.db, 0) != 0 {
			err = errors.Join(err, errors.New("fixture checkpoint changed admission or terminal ownership"))
		}
		return result, err
	}
	result, err := condition()
	t.Logf("IW_FIXTURE_CONDITION N=%d attempted=%t code=%d log_frames=%d checkpointed_frames=%d before_valid=%t after_valid=%t wal_before_bytes=%d wal_after_bytes=%d main_before_bytes=%d main_after_bytes=%d elapsed_us=%d condition_ms=3000 error=%t", n, result.Attempted, result.Code, result.LogFrames, result.CheckpointedFrames, result.BeforeValid, result.AfterValid, result.Before.WAL, result.After.WAL, result.Before.Main, result.After.Main, time.Since(started).Microseconds(), err != nil)
	if err != nil || !result.Attempted || result.Code != lib.SQLITE_OK ||
		!result.BeforeValid || !result.AfterValid || result.Before.WAL <= 0 || result.After.WAL != 0 ||
		result.LogFrames < 0 || result.CheckpointedFrames != result.LogFrames {
		t.Fatal("private seed checkpoint did not complete once with checked WAL reclaim")
	}
}
