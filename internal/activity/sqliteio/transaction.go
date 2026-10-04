//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

type Mode uint8

const (
	Read Mode = iota
	Write
)

type Outcome uint8

const (
	NotAttempted Outcome = iota
	NotCommitted
	Committed
	Unknown
)

// Tx and its statements have one operation owner. Conn admission is concurrent.
type Tx struct {
	conn                  *Conn
	ctx                   context.Context
	mode                  Mode
	statements            map[*Stmt]struct{}
	stopInterrupt         func()
	terminal              bool
	nativeLost            bool
	readFailed            bool
	outcome               Outcome
	commitErr, cleanupErr error
}

func (c *Conn) Begin(ctx context.Context, mode Mode) (*Tx, error) {
	if c == nil || c.gate == nil {
		return nil, safeError(BeginPhase, ErrClosed)
	}
	if mode != Read && mode != Write {
		return nil, &Error{Phase: BeginPhase, Category: Invalid}
	}
	if err := c.lockContext(ctx, c.acquireDeadline); err != nil {
		return nil, err
	}
	owned := false
	defer func() {
		if !owned {
			c.unlock()
		}
	}()
	if c.closed || c.used || c.poisoned {
		return nil, safeError(BeginPhase, ErrClosed)
	}
	if c.readOnly && mode == Write {
		return nil, misuse(BeginPhase)
	}
	if c.oversize && mode == Write {
		return nil, &Error{Phase: BeginPhase, Category: Full}
	}
	if err := validateRoot(c.root); err != nil {
		return nil, safeError(VerifyPhase, err)
	}
	admissionCtx, cancelAdmission := context.WithDeadline(ctx, c.acquireDeadline)
	stopAdmission := c.interruptWith(admissionCtx)
	sql := "BEGIN"
	if mode == Write {
		sql = "BEGIN IMMEDIATE"
	}
	_, err := c.control(admissionCtx, sql, authTransaction, BeginPhase)
	stopAdmission()
	cancelAdmission()
	if err == nil {
		// A completed native BEGIN is not admitted after its original budget.
		if cause := admissionError(ctx, c.acquireDeadline); cause != nil {
			err = safeError(BeginPhase, cause)
		} else if lib.Xsqlite3_get_autocommit(c.tls, c.db) != 0 {
			err = misuse(BeginPhase)
		}
	}
	if err != nil {
		e := safeError(BeginPhase, err)
		if lib.Xsqlite3_get_autocommit(c.tls, c.db) == 0 {
			_, cleanup := c.control(nil, "ROLLBACK", authTransaction, RollbackPhase)
			e.Cleanup = joinCleanup(e.Cleanup, cleanup)
			if cleanup != nil || lib.Xsqlite3_get_autocommit(c.tls, c.db) == 0 {
				c.poisoned = true
			}
		}
		return nil, e
	}
	c.used = true
	// The admission watcher has joined and its flag is clear before the
	// overall operation context takes ownership. The gate remains held.
	t := &Tx{conn: c, ctx: ctx, mode: mode, statements: make(map[*Stmt]struct{}), stopInterrupt: c.interruptWith(ctx)}
	// Writer admission consumed the shared deadline. Never restart a busy wait.
	if rc := lib.Xsqlite3_busy_timeout(c.tls, c.db, 0); rc != lib.SQLITE_OK {
		owned = true
		e := engineError(BeginPhase, rc, ctx)
		e.Cleanup = joinCleanup(e.Cleanup, t.Rollback())
		return nil, e
	}
	owned = true
	if mode == Write {
		if _, err = t.PageInfo(); err != nil {
			e := safeError(BeginPhase, err)
			e.Cleanup = joinCleanup(e.Cleanup, t.Rollback())
			return nil, e
		}
	}
	return t, nil
}
func (t *Tx) readFailure(err error) {
	if err != nil && t != nil && !t.terminal && t.mode == Read {
		t.readFailed = true
	}
}
func (t *Tx) check(phase Phase) (err error) {
	defer func() { t.readFailure(err) }()
	if t == nil || t.conn == nil || t.terminal {
		return safeError(phase, ErrClosed)
	}
	if err := t.ctx.Err(); err != nil {
		return safeError(phase, err)
	}
	if t.conn.poisoned {
		return safeError(phase, ErrUnsafe)
	}
	if t.nativeLost || lib.Xsqlite3_get_autocommit(t.conn.tls, t.conn.db) != 0 {
		// SQLite may roll back internally on an error. Retain cleanup ownership,
		// but never dispatch another application statement in autocommit mode.
		t.nativeLost = true
		return safeError(phase, ErrClosed)
	}
	if err := validateRoot(t.conn.root); err != nil {
		return safeError(VerifyPhase, err)
	}
	return nil
}
func (t *Tx) stop() {
	if t.stopInterrupt != nil {
		t.stopInterrupt()
		t.stopInterrupt = nil
	}
}

// rollbackCleanup does not use the canceled request context. It cannot free
// TLS/root ownership: that remains with Conn until conclusive sqlite3_close.
func (t *Tx) rollbackCleanup() (cleanup error, ended bool) {
	t.stop()
	for s := range t.statements {
		cleanup = joinCleanup(cleanup, s.Close())
	}
	c := t.conn
	if lib.Xsqlite3_get_autocommit(c.tls, c.db) == 0 {
		fault := sqlEvent(sqlTestEvent{Phase: "rollback-before", Operation: "rollback"})
		if fault != nil {
			cleanup = joinCleanup(cleanup, safeError(RollbackPhase, fault))
		} else {
			_, err := c.control(nil, "ROLLBACK", authTransaction, RollbackPhase)
			cleanup = joinCleanup(cleanup, err)
			// Code zero records checked SQLITE_OK (the control reached DONE
			// and finalized); failures retain their actual engine code.
			var code int32
			if err != nil {
				code = safeError(RollbackPhase, err).Code
				if code == 0 {
					code = lib.SQLITE_ERROR
				}
			}
			if fault = sqlEvent(sqlTestEvent{Phase: "rollback-after", Operation: "rollback", Code: code}); fault != nil {
				cleanup = joinCleanup(cleanup, safeError(RollbackPhase, fault))
			}
		}
	}
	ended = lib.Xsqlite3_get_autocommit(c.tls, c.db) != 0
	if !ended {
		c.poisoned = true
	}
	return cleanup, ended
}
func (t *Tx) finish(outcome Outcome, err, cleanup error) {
	t.stop()
	if checked, ok := err.(*Error); ok {
		e := *checked
		cleanup = joinCleanup(e.Cleanup, cleanup)
		e.Cleanup = cleanup
		err = &e
	} else {
		err = joinCleanup(err, cleanup)
	}
	t.terminal, t.outcome, t.cleanupErr = true, outcome, cleanup
	t.commitErr = err
	// The read witness is set only after all native ownership and cancellation
	// cleanup ended conclusively; ordinary writes never grant it.
	if t.mode == Read && !t.readFailed && outcome == Committed && err == nil && cleanup == nil &&
		t.ctx.Err() == nil && len(t.statements) == 0 &&
		lib.Xsqlite3_get_autocommit(t.conn.tls, t.conn.db) != 0 && !t.conn.poisoned {
		if verify := validateRoot(t.conn.root); verify == nil {
			t.conn.cleanRead = true
		} else {
			t.commitErr = safeError(VerifyPhase, verify)
			t.outcome = Unknown
		}
	}
	// A durable acknowledgement requires a clean real writer COMMIT, not merely
	// an autocommit connection or a successful read/checkpoint.
	if t.mode == Write && outcome == Committed && err == nil && cleanup == nil &&
		t.ctx.Err() == nil && len(t.statements) == 0 && !t.conn.poisoned &&
		lib.Xsqlite3_get_autocommit(t.conn.tls, t.conn.db) != 0 &&
		lib.Xsqlite3_next_stmt(t.conn.tls, t.conn.db, 0) == 0 && validateRoot(t.conn.root) == nil {
		t.conn.cleanWrite = true
	}
	t.conn.unlock()
}
func (t *Tx) abortBeforeCommit(cause error) (Outcome, error) {
	cleanup, ended := t.rollbackCleanup()
	outcome := Unknown
	if ended {
		outcome = NotCommitted
	}
	t.finish(outcome, cause, cleanup)
	return t.outcome, t.commitErr
}
func (t *Tx) Commit() (Outcome, error) {
	if t == nil || t.conn == nil {
		return NotAttempted, misuse(CommitPhase)
	}
	if t.terminal {
		return t.outcome, t.commitErr
	}
	if len(t.statements) != 0 {
		err := misuse(CommitPhase)
		t.readFailure(err)
		return NotAttempted, err
	}
	if err := t.check(CommitPhase); err != nil {
		return t.abortBeforeCommit(err)
	}
	c := t.conn
	libc.AssignPtrUint32(c.authMode, authTransaction)
	stmt, err := c.prepareRaw("COMMIT", CommitPhase)
	libc.AssignPtrUint32(c.authMode, authApplication)
	if err != nil {
		return t.abortBeforeCommit(contextualError(CommitPhase, err, t.ctx))
	}
	finalize := func() error {
		rc := lib.Xsqlite3_finalize(c.tls, stmt)
		stmt = 0
		if rc != lib.SQLITE_OK {
			return engineError(FinalizePhase, rc, nil)
		}
		return nil
	}
	if err = sqlEvent(sqlTestEvent{Phase: "commit-before-dispatch", Operation: "commit"}); err == nil {
		err = t.check(CommitPhase)
	}
	if err != nil {
		f := finalize()
		if f != nil {
			e := safeError(CommitPhase, err)
			e.Cleanup = joinCleanup(e.Cleanup, f)
			err = e
		}
		return t.abortBeforeCommit(safeError(CommitPhase, err))
	}
	libc.AssignPtrUint32(c.authMode, authTransaction)
	observeSQL(sqlTestEvent{Phase: "control-before-native", Operation: "commit"})
	rc := lib.Xsqlite3_step(c.tls, stmt)
	libc.AssignPtrUint32(c.authMode, authApplication)
	if rc != lib.SQLITE_DONE {
		err = engineError(CommitPhase, rc, t.ctx)
	}
	fault := sqlEvent(sqlTestEvent{Phase: "commit-after-engine", Operation: "commit", Code: rc})
	if err == nil && fault != nil {
		err = safeError(CommitPhase, fault)
	}
	wasBusyActive := (rc&255) == lib.SQLITE_BUSY && lib.Xsqlite3_get_autocommit(c.tls, c.db) == 0 && fault == nil
	finErr := finalize()
	if err == nil && finErr != nil {
		err = finErr
	}
	outcome := Unknown
	if err == nil {
		err = sqlEvent(sqlTestEvent{Phase: "commit-before-verify", Operation: "commit", Code: rc})
		if err == nil {
			err = validateRoot(c.root)
		}
		if err == nil {
			outcome = Committed
			err = sqlEvent(sqlTestEvent{Phase: "commit-after-verify", Operation: "commit", Code: rc})
		}
		if err != nil {
			err = safeError(VerifyPhase, err)
		}
	}
	cleanup, ended := t.rollbackCleanup()
	if wasBusyActive && ended {
		outcome = NotCommitted
	}
	cleanup = joinCleanup(finErr, cleanup)
	t.finish(outcome, err, cleanup)
	return t.outcome, t.commitErr
}
func (t *Tx) Rollback() error {
	if t == nil {
		return nil
	}
	if t.conn == nil {
		return safeError(RollbackPhase, ErrClosed)
	}
	if t.terminal {
		return t.cleanupErr
	}
	cleanup, ended := t.rollbackCleanup()
	outcome := Unknown
	if ended {
		outcome = NotCommitted
	}
	t.finish(outcome, safeError(CommitPhase, ErrClosed), cleanup)
	return cleanup
}
