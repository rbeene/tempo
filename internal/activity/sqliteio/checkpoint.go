//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

type CheckpointMode uint8

const (
	Passive CheckpointMode = iota
	Truncate
)

type CheckpointResult struct {
	Attempted bool
	Code      int32
	// CheckpointedFrames is cumulative WAL backfill, not work by this call.
	// Both counts stay -1 when the engine does not supply them.
	LogFrames, CheckpointedFrames int32
	Before, After                 Footprint
	BeforeValid, AfterValid       bool
}

// Checkpoint consumes an unused connection or one conclusively completed Read.
// It makes at most one native call, with no busy wait or automatic escalation.
func (c *Conn) Checkpoint(ctx context.Context, mode CheckpointMode) (result CheckpointResult, err error) {
	result.LogFrames, result.CheckpointedFrames = -1, -1
	if c == nil || c.gate == nil {
		return result, safeError(CheckpointPhase, ErrClosed)
	}
	var nativeMode int32
	switch mode {
	case Passive:
		nativeMode = lib.SQLITE_CHECKPOINT_PASSIVE
	case Truncate:
		nativeMode = lib.SQLITE_CHECKPOINT_TRUNCATE
	default:
		return result, &Error{Phase: CheckpointPhase, Category: Invalid}
	}
	if err = c.lockContext(ctx, c.acquireDeadline); err != nil {
		return result, err
	}
	defer c.unlock()
	if c.closed || c.poisoned || c.used && !c.cleanRead {
		return result, safeError(CheckpointPhase, ErrClosed)
	}
	if c.readOnly || lib.Xsqlite3_get_autocommit(c.tls, c.db) == 0 || lib.Xsqlite3_next_stmt(c.tls, c.db, 0) != 0 {
		return result, misuse(CheckpointPhase)
	}
	// Consume this ownership before preparation or dispatch can fail. A failed
	// attempt must never become a fresh Begin or a second checkpoint.
	c.used, c.cleanRead = true, false
	result.Before, err = c.footprint()
	if err != nil {
		return result, err
	}
	result.BeforeValid = true
	if rc := lib.Xsqlite3_busy_timeout(c.tls, c.db, 0); rc != lib.SQLITE_OK {
		return result, engineError(CheckpointPhase, rc, ctx)
	}
	name, err := libc.CString("main")
	if err != nil {
		return result, safeError(CheckpointPhase, err)
	}
	defer libc.Xfree(c.tls, name)
	output := lib.Xsqlite3_malloc64(c.tls, 8)
	if output == 0 {
		return result, engineError(CheckpointPhase, lib.SQLITE_NOMEM, nil)
	}
	defer lib.Xsqlite3_free(c.tls, output)
	libc.AssignPtrInt32(output, -1)
	libc.AssignPtrInt32(output+4, -1)
	// The pinned pager can run an internal PRAGMA for an empty WAL. No caller
	// statement is exposed while this narrow authorization is in force.
	libc.AssignPtrUint32(c.authMode, authPragma)
	defer func() { libc.AssignPtrUint32(c.authMode, authApplication) }()
	stop := c.interruptWith(ctx)
	defer stop()
	if err = admissionError(ctx, c.acquireDeadline); err != nil {
		return result, safeError(CheckpointPhase, err)
	}
	observeSQL(sqlTestEvent{Phase: "checkpoint-before-native", Operation: "checkpoint"})
	result.Attempted = true
	result.Code = lib.Xsqlite3_wal_checkpoint_v2(c.tls, c.db, name, nativeMode, output, output+4)
	result.LogFrames = nativeLoad[int32](output)
	result.CheckpointedFrames = nativeLoad[int32](output + 4)
	observeSQL(sqlTestEvent{Phase: "checkpoint-after-engine", Operation: "checkpoint", Code: result.Code})
	if result.Code != lib.SQLITE_OK {
		err = engineError(CheckpointPhase, result.Code, ctx)
	} else if cause := ctx.Err(); cause != nil {
		err = safeError(CheckpointPhase, cause)
	}
	// These stats remain useful after BUSY/cancellation. Failure invalidates the
	// after observation but cannot erase an attempted or partial native result.
	var verify error
	result.After, verify = c.footprint()
	result.AfterValid = verify == nil
	if verify != nil {
		if err == nil {
			err = verify
		} else {
			e := safeError(CheckpointPhase, err)
			e.Cleanup = joinCleanup(e.Cleanup, verify)
			err = e
		}
	}
	return result, err
}
