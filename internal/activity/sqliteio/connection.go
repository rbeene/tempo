//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

type Options struct {
	Create, ReadOnly bool
	ExclusiveCreate  bool
	AcquireDeadline  time.Time
	testMaxPages     int64
}
type Phase string

const (
	Admission       Phase = "admission"
	OpenPhase       Phase = "open"
	BeginPhase      Phase = "begin"
	PreparePhase    Phase = "prepare"
	BindPhase       Phase = "bind"
	StepPhase       Phase = "step"
	CommitPhase     Phase = "commit"
	VerifyPhase     Phase = "verify"
	RollbackPhase   Phase = "rollback"
	FinalizePhase   Phase = "finalize"
	ClosePhase      Phase = "close"
	CheckpointPhase Phase = "checkpoint"
)

type Category string

const (
	Invalid    Category = "invalid"
	Busy       Category = "busy"
	Canceled   Category = "canceled"
	Unsafe     Category = "unsafe"
	Corrupt    Category = "corrupt"
	Full       Category = "full"
	Constraint Category = "constraint"
	IO         Category = "io"
	Closed     Category = "closed"
	Misuse     Category = "misuse"
)

// Error deliberately omits SQL, values, paths and the engine's errmsg text.
type Error struct {
	Phase    Phase
	Category Category
	Code     int32
	Cause    error
	Cleanup  error
}

func (e *Error) Error() string {
	return fmt.Sprintf("SQLite %s failure (%s, code %d)", e.Phase, e.Category, e.Code)
}
func (e *Error) Unwrap() []error {
	var causes []error
	if e.Cause != nil {
		causes = append(causes, e.Cause)
	}
	if e.Cleanup != nil {
		causes = append(causes, e.Cleanup)
	}
	return causes
}
func sqliteError(code int32) error { return engineError(OpenPhase, code, nil) }
func engineError(phase Phase, code int32, ctx context.Context) *Error {
	e := &Error{Phase: phase, Category: IO, Code: code}
	switch code & 255 {
	case lib.SQLITE_BUSY, lib.SQLITE_LOCKED:
		e.Category = Busy
	case lib.SQLITE_INTERRUPT:
		e.Category = Canceled
	case lib.SQLITE_CORRUPT, lib.SQLITE_NOTADB:
		e.Category = Corrupt
	case lib.SQLITE_FULL:
		e.Category = Full
	case lib.SQLITE_CONSTRAINT:
		e.Category = Constraint
	case lib.SQLITE_AUTH, lib.SQLITE_MISUSE, lib.SQLITE_RANGE:
		e.Category = Misuse
	}
	if ctx != nil && ctx.Err() != nil {
		e.Category, e.Cause = Canceled, ctx.Err()
	}
	return e
}
func safeError(phase Phase, cause error) *Error {
	if e, ok := cause.(*Error); ok {
		clone := *e
		return &clone
	}
	e := &Error{Phase: phase, Category: IO}
	switch {
	case errors.Is(cause, context.Canceled), errors.Is(cause, context.DeadlineExceeded):
		e.Category, e.Cause = Canceled, cause
	case errors.Is(cause, ErrBusy):
		e.Category, e.Cause = Busy, ErrBusy
	case errors.Is(cause, ErrUnsafe):
		e.Category, e.Cause = Unsafe, ErrUnsafe
	case errors.Is(cause, ErrClosed):
		e.Category, e.Cause = Closed, ErrClosed
	}
	return e
}
func misuse(phase Phase) error { return &Error{Phase: phase, Category: Misuse} }
func contextualError(phase Phase, cause error, ctx context.Context) *Error {
	e := safeError(phase, cause)
	if ctx != nil && ctx.Err() != nil {
		e.Category, e.Cause = Canceled, ctx.Err()
	}
	return e
}

// Both inputs are already checked adapter errors. Keep their unwrap evidence;
// a later successful cleanup must never erase an earlier failure.
func joinCleanup(earlier, later error) error {
	if earlier == nil {
		return later
	}
	if later == nil {
		return earlier
	}
	return errors.Join(earlier, later)
}

type Conn struct {
	gate                         chan struct{}
	tls                          *libc.TLS
	db                           uintptr
	authMode                     uintptr
	cancelFlag                   uintptr
	root                         *rootEntry
	acquireDeadline              time.Time
	readOnly                     bool
	maxPages                     int64
	oversize, cleanRead          bool
	used, closed, poisoned       bool
	closeErr                     error
	nativeCounted                bool
	cleanWrite, durableAttempted bool
}

const (
	authApplication uint32 = iota
	authTransaction
	authPragma
)

var authorizerValue = authorize
var progressValue = progress

// The flag lives in SQLite-allocated memory until conclusive connection close.
// This callback does not enter SQLite or call application code.
func progress(_ *libc.TLS, cell uintptr) int32 {
	if cell == 0 {
		return 1
	}
	return int32(atomic.LoadUint32((*uint32)(unsafe.Pointer(cell))))
}

func authorize(_ *libc.TLS, cell uintptr, action int32, _, _, _, _ uintptr) int32 {
	if cell == 0 {
		return lib.SQLITE_DENY
	}
	mode := *(*uint32)(unsafe.Pointer(cell))
	switch action {
	case lib.SQLITE_ATTACH, lib.SQLITE_DETACH, lib.SQLITE_SAVEPOINT:
		return lib.SQLITE_DENY
	case lib.SQLITE_TRANSACTION:
		if mode != authTransaction {
			return lib.SQLITE_DENY
		}
	case lib.SQLITE_PRAGMA:
		if mode != authPragma {
			return lib.SQLITE_DENY
		}
	}
	return lib.SQLITE_OK
}
func admissionError(ctx context.Context, deadline time.Time) error {
	if ctx == nil {
		return &Error{Phase: Admission, Category: Invalid}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline.IsZero() {
		return &Error{Phase: Admission, Category: Invalid}
	}
	if !time.Now().Before(deadline) {
		return ErrBusy
	}
	return nil
}
func Open(ctx context.Context, directory, basename string, opts Options) (*Conn, error) {
	if err := admissionError(ctx, opts.AcquireDeadline); err != nil {
		return nil, safeError(Admission, err)
	}
	if opts.Create && opts.ReadOnly || opts.ExclusiveCreate && (!opts.Create || opts.ReadOnly) || basename == "" || basename == "." || basename == ".." ||
		strings.ContainsAny(basename, "/\x00?") || len(basename)+len("-journal") > 255 ||
		opts.testMaxPages < 0 || opts.testMaxPages > MaxPages {
		return nil, &Error{Phase: OpenPhase, Category: Invalid}
	}
	r, err := acquireRoot(ctx, directory, basename, opts.Create, opts.AcquireDeadline)
	if err != nil {
		return nil, safeError(Admission, err)
	}
	if err = initialize(); err != nil {
		cleanup := releaseRoot(r)
		e := safeError(OpenPhase, err)
		if cleanup != nil {
			e.Cleanup = safeError(ClosePhase, cleanup)
		}
		return nil, e
	}
	c := newConnection(r, opts)
	if opts.ExclusiveCreate {
		if err = armExclusive(r); err != nil {
			return failOpen(c, safeError(Admission, err))
		}
	}
	if err = c.openNative(ctx, opts); err != nil {
		return failOpen(c, err)
	}
	setupCtx, cancel := context.WithDeadline(ctx, opts.AcquireDeadline)
	finish := c.interruptWith(setupCtx)
	err = c.setup(setupCtx, opts.Create)
	finish()
	cancel()
	if err != nil {
		return failOpen(c, err)
	}
	if err = validateRoot(r); err != nil {
		return failOpen(c, safeError(VerifyPhase, err))
	}
	return c, nil
}

func newConnection(r *rootEntry, opts Options) *Conn {
	c := &Conn{root: r, gate: make(chan struct{}, 1),
		acquireDeadline: opts.AcquireDeadline, readOnly: opts.ReadOnly, maxPages: MaxPages}
	if opts.testMaxPages != 0 {
		c.maxPages = opts.testMaxPages
	}
	c.gate <- struct{}{}
	return c
}

// A native handle may exist even on sqlite3_open_v2 failure. Count before entry
// and relinquish only after checked sqlite3_close, never at lease handoff.
func (c *Conn) openNative(ctx context.Context, opts Options) error {
	c.tls = libc.NewTLS()
	c.readOnly = opts.ReadOnly
	r := c.root
	name, err := libc.CString(r.key + "/" + r.databaseName)
	if err != nil {
		return safeError(OpenPhase, err)
	}
	flags := int32(lib.SQLITE_OPEN_READWRITE | lib.SQLITE_OPEN_FULLMUTEX | lib.SQLITE_OPEN_NOFOLLOW | lib.SQLITE_OPEN_PRIVATECACHE)
	if opts.ReadOnly {
		flags &^= lib.SQLITE_OPEN_READWRITE
		flags |= lib.SQLITE_OPEN_READONLY
	}
	if opts.Create {
		flags |= lib.SQLITE_OPEN_CREATE
	}
	output := lib.Xsqlite3_malloc64(c.tls, uint64(unsafe.Sizeof(uintptr(0))))
	if output == 0 {
		libc.Xfree(c.tls, name)
		return engineError(OpenPhase, lib.SQLITE_NOMEM, nil)
	}
	*(*uintptr)(unsafe.Pointer(output)) = 0
	if err = admissionError(ctx, opts.AcquireDeadline); err != nil {
		lib.Xsqlite3_free(c.tls, output)
		libc.Xfree(c.tls, name)
		return safeError(Admission, err)
	}
	registry.Lock()
	if registry.poisoned || registry.fatal || r.poisoned {
		registry.Unlock()
		lib.Xsqlite3_free(c.tls, output)
		libc.Xfree(c.tls, name)
		return safeError(OpenPhase, ErrUnsafe)
	}
	registry.nativeActive++
	c.nativeCounted = true
	registry.Unlock()
	rc := lib.Xsqlite3_open_v2(c.tls, name, output, flags, vfsNameMemory)
	c.db = *(*uintptr)(unsafe.Pointer(output))
	lib.Xsqlite3_free(c.tls, output)
	libc.Xfree(c.tls, name)
	if rc != lib.SQLITE_OK {
		return engineError(OpenPhase, rc, ctx)
	}
	registry.Lock()
	badExclusive := r.exclusive && (!r.exclusiveCreated || r.exclusiveRefused || r.main == (identity{}))
	registry.Unlock()
	if badExclusive {
		return safeError(OpenPhase, ErrUnsafe)
	}
	if rc = lib.Xsqlite3_extended_result_codes(c.tls, c.db, 1); rc != lib.SQLITE_OK {
		return engineError(OpenPhase, rc, ctx)
	}
	c.authMode = lib.Xsqlite3_malloc64(c.tls, 4)
	if c.authMode == 0 {
		return engineError(OpenPhase, lib.SQLITE_NOMEM, nil)
	}
	*(*uint32)(unsafe.Pointer(c.authMode)) = authApplication
	if rc = lib.Xsqlite3_set_authorizer(c.tls, c.db, functionPointer(authorizerValue), c.authMode); rc != lib.SQLITE_OK {
		return engineError(OpenPhase, rc, ctx)
	}
	c.cancelFlag = lib.Xsqlite3_malloc64(c.tls, 4)
	if c.cancelFlag == 0 {
		return engineError(OpenPhase, lib.SQLITE_NOMEM, nil)
	}
	atomic.StoreUint32((*uint32)(unsafe.Pointer(c.cancelFlag)), 0)
	lib.Xsqlite3_progress_handler(c.tls, c.db, 1000, functionPointer(progressValue), c.cancelFlag)
	for _, op := range []int32{lib.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, lib.SQLITE_DBCONFIG_DEFENSIVE} {
		v, e := c.config(op, 1)
		if e != nil {
			return e
		}
		if !v {
			return safeError(OpenPhase, ErrUnsafe)
		}
	}
	for _, op := range []int32{lib.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, lib.SQLITE_DBCONFIG_TRUSTED_SCHEMA, lib.SQLITE_DBCONFIG_DQS_DDL, lib.SQLITE_DBCONFIG_DQS_DML} {
		v, e := c.config(op, 0)
		if e != nil {
			return e
		}
		if v {
			return safeError(OpenPhase, ErrUnsafe)
		}
	}
	lib.Xsqlite3_limit(c.tls, c.db, lib.SQLITE_LIMIT_ATTACHED, 0)
	return nil
}

func failOpen(c *Conn, cause error) (*Conn, error) {
	if err := c.Close(context.Background()); err != nil {
		e := safeError(OpenPhase, cause)
		e.Cleanup = joinCleanup(e.Cleanup, err)
		if c.closed {
			return nil, e
		}
		return c, e
	}
	return nil, cause
}
func (c *Conn) config(op, value int32) (bool, error) {
	out := lib.Xsqlite3_malloc64(c.tls, 8)
	if out == 0 {
		return false, engineError(OpenPhase, lib.SQLITE_NOMEM, nil)
	}
	defer lib.Xsqlite3_free(c.tls, out)
	*(*int32)(unsafe.Pointer(out)) = -1
	args := libc.NewVaList(value, out)
	if args == 0 {
		return false, engineError(OpenPhase, lib.SQLITE_NOMEM, nil)
	}
	defer libc.Xfree(c.tls, args)
	if rc := lib.Xsqlite3_db_config(c.tls, c.db, op, args); rc != lib.SQLITE_OK {
		return false, engineError(OpenPhase, rc, nil)
	}
	return *(*int32)(unsafe.Pointer(out)) == 1, nil
}
func (c *Conn) setup(ctx context.Context, create bool) error {
	for _, sql := range []string{"PRAGMA temp_store=MEMORY", "PRAGMA foreign_keys=ON", "PRAGMA synchronous=FULL", "PRAGMA wal_autocheckpoint=0"} {
		if _, err := c.control(ctx, sql, authPragma, OpenPhase); err != nil {
			return err
		}
	}
	if err := c.setupPagePolicy(ctx); err != nil {
		return err
	}
	if create {
		if _, err := c.control(ctx, "PRAGMA journal_mode=WAL", authPragma, OpenPhase); err != nil {
			return err
		}
	}
	for _, q := range []struct{ sql, want string }{
		{"SELECT sqlite_version()", "3.53.4"},
		{"SELECT sqlite_source_id()", "2026-07-24 19:02:57 bf7c7f30031888f4e796e429ab3978879485813aaca6f641c7b33e4e09459bcc"},
		{"PRAGMA journal_mode", "wal"}, {"PRAGMA synchronous", "2"},
		{"PRAGMA foreign_keys", "1"}, {"PRAGMA wal_autocheckpoint", "0"},
	} {
		got, err := c.control(ctx, q.sql, authPragma, OpenPhase)
		if err != nil {
			return err
		}
		if got != q.want {
			return safeError(OpenPhase, ErrUnsafe)
		}
	}
	value, err := c.config(lib.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, -1)
	if err != nil {
		return err
	}
	if !value {
		return safeError(OpenPhase, ErrUnsafe)
	}
	return nil
}

// control is only called with fixed internal SQL. It has no user callback.
func (c *Conn) control(ctx context.Context, sql string, mode uint32, phase Phase) (result string, err error) {
	if ctx != nil {
		if err = ctx.Err(); err != nil {
			return "", safeError(phase, err)
		}
	}
	if phase == OpenPhase || phase == BeginPhase {
		if err = admissionError(ctx, c.acquireDeadline); err != nil {
			return "", safeError(phase, err)
		}
		if err = c.busyRemaining(ctx); err != nil {
			return "", err
		}
	}
	*(*uint32)(unsafe.Pointer(c.authMode)) = mode
	defer func() { *(*uint32)(unsafe.Pointer(c.authMode)) = authApplication }()
	stmt, err := c.prepareRaw(sql, phase)
	if err != nil {
		return "", contextualError(phase, err, ctx)
	}
	defer func() {
		if rc := lib.Xsqlite3_finalize(c.tls, stmt); rc != lib.SQLITE_OK {
			f := engineError(FinalizePhase, rc, ctx)
			if err == nil {
				err = f
			} else {
				e := safeError(phase, err)
				e.Cleanup = joinCleanup(e.Cleanup, f)
				err = e
			}
		}
	}()
	operation := "setup"
	if phase == BeginPhase {
		operation = "begin"
	} else if phase == RollbackPhase {
		operation = "rollback"
	}
	for {
		if ctx != nil {
			if err = ctx.Err(); err != nil {
				return "", safeError(phase, err)
			}
		}
		if phase == OpenPhase || phase == BeginPhase {
			// Preparation is part of the same wall-clock admission budget.
			if err = c.busyRemaining(ctx); err != nil {
				return "", err
			}
		}
		observeSQL(sqlTestEvent{Phase: "control-before-native", Operation: operation})
		rc := lib.Xsqlite3_step(c.tls, stmt)
		if rc == lib.SQLITE_DONE {
			return result, nil
		}
		if rc != lib.SQLITE_ROW {
			return "", engineError(phase, rc, ctx)
		}
		switch lib.Xsqlite3_column_type(c.tls, stmt, 0) {
		case lib.SQLITE_INTEGER:
			result = fmt.Sprint(lib.Xsqlite3_column_int64(c.tls, stmt, 0))
		case lib.SQLITE_TEXT:
			p := lib.Xsqlite3_column_text(c.tls, stmt, 0)
			n := lib.Xsqlite3_column_bytes(c.tls, stmt, 0)
			if p == 0 {
				return "", engineError(phase, lib.SQLITE_NOMEM, nil)
			}
			result = string(unsafe.Slice((*byte)(unsafe.Pointer(p)), int(n)))
		default:
			return "", misuse(phase)
		}
	}
}
func (c *Conn) busyRemaining(ctx context.Context) error {
	left := time.Until(c.acquireDeadline)
	if d, ok := ctx.Deadline(); ok && time.Until(d) < left {
		left = time.Until(d)
	}
	if left <= 0 {
		return safeError(Admission, ErrBusy)
	}
	ms := left / time.Millisecond
	if ms > time.Duration(2147483647) {
		ms = time.Duration(2147483647)
	}
	if rc := lib.Xsqlite3_busy_timeout(c.tls, c.db, int32(ms)); rc != lib.SQLITE_OK {
		return engineError(Admission, rc, ctx)
	}
	return nil
}
func (c *Conn) lockContext(ctx context.Context, deadline time.Time) error {
	if ctx == nil {
		return &Error{Phase: Admission, Category: Invalid}
	}
	if err := ctx.Err(); err != nil {
		return safeError(Admission, err)
	}
	var timeout <-chan time.Time
	if !deadline.IsZero() {
		if !time.Now().Before(deadline) {
			return safeError(Admission, ErrBusy)
		}
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-ctx.Done():
		return safeError(Admission, ctx.Err())
	case <-timeout:
		return safeError(Admission, ErrBusy)
	case <-c.gate:
		if err := ctx.Err(); err != nil {
			c.unlock()
			return safeError(Admission, err)
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			c.unlock()
			return safeError(Admission, ErrBusy)
		}
		return nil
	}
}
func (c *Conn) unlock() { c.gate <- struct{}{} }
func (c *Conn) interruptWith(ctx context.Context) func() {
	flag := (*uint32)(unsafe.Pointer(c.cancelFlag))
	atomic.StoreUint32(flag, 0)
	stop, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-ctx.Done():
			// Unlike sqlite3_interrupt on an idle VM, this remains visible to
			// the next native prepare/step even if cancellation won before entry.
			atomic.StoreUint32(flag, 1)
			tls := libc.NewTLS()
			lib.Xsqlite3_interrupt(tls, c.db)
			tls.Close()
			observeSQL(sqlTestEvent{Phase: "interrupt-after", Operation: "interrupt"})
		case <-stop:
		}
	}()
	return func() {
		close(stop)
		<-joined
		// Only the operation owner clears the flag, after its watcher joined.
		// Fixed internal rollback/finalize can then run on a canceled request.
		atomic.StoreUint32(flag, 0)
	}
}
func (c *Conn) Close(ctx context.Context) error {
	_, err := c.CloseChecked(ctx)
	return err
}

// CloseChecked reports terminal caller ownership under the same gate as Close.
// A true result does not erase cleanup errors or certify durability. A false
// result leaves terminal ownership unproved; the caller retains this pointer.
func (c *Conn) CloseChecked(ctx context.Context) (terminal bool, err error) {
	if c == nil {
		return true, nil
	}
	if c.gate == nil {
		return false, safeError(ClosePhase, ErrClosed)
	}
	if err := c.lockContext(ctx, time.Time{}); err != nil {
		return false, err
	}
	defer c.unlock()
	if c.closed {
		return true, c.closeErr
	}
	if err := sqlEvent(sqlTestEvent{Phase: "close-before", Operation: "close"}); err != nil {
		return false, safeError(ClosePhase, err)
	}
	if err := c.closeNative(); err != nil {
		if c.db != 0 || c.nativeCounted {
			return false, combineClose(err, c.closeErr)
		}
		c.finishRelease(combineClose(c.closeErr, err))
		return true, c.closeErr
	}
	c.finishRelease(c.closeErr)
	emit(event{Namespace: c.root.key, Role: "main", Op: "close", Phase: "closed", FD: -1})
	if err := sqlEvent(sqlTestEvent{Phase: "close-after", Operation: "close"}); err != nil {
		c.closeErr = combineClose(c.closeErr, safeError(ClosePhase, err))
	}
	return true, c.closeErr
}

// No watcher or statement owner may coexist with this call: all callers hold
// the connection gate, and Tx.finish/initial probe join their watcher first.
func (c *Conn) closeNative() error {
	emit(event{Namespace: c.root.key, Role: "main", Op: "close", Phase: "before", FD: -1})
	if c.db != 0 {
		if rc := lib.Xsqlite3_close(c.tls, c.db); rc != lib.SQLITE_OK {
			return engineError(ClosePhase, rc, nil)
		}
		c.db = 0
	}
	if c.tls != nil {
		if c.authMode != 0 {
			lib.Xsqlite3_free(c.tls, c.authMode)
			c.authMode = 0
		}
		if c.cancelFlag != 0 {
			lib.Xsqlite3_free(c.tls, c.cancelFlag)
			c.cancelFlag = 0
		}
		c.tls.Close()
		c.tls = nil
	}
	registry.Lock()
	if c.nativeCounted {
		registry.nativeActive--
		c.nativeCounted = false
	}
	registry.Unlock()
	return drainRejected()
}

func combineClose(primary, cleanup error) error {
	if primary == nil {
		return cleanup
	}
	if cleanup == nil {
		return primary
	}
	e := safeError(ClosePhase, primary)
	e.Cleanup = joinCleanup(e.Cleanup, cleanup)
	return e
}
func (c *Conn) finishRelease(primary error) {
	if c.closed {
		return
	}
	c.closeErr = combineClose(primary, releaseRoot(c.root))
	c.closed = true
}

type sqlTestEvent struct {
	Phase, Operation string
	Code             int32
}
type sqlTestHooks struct {
	Observe func(sqlTestEvent)
	Fault   func(sqlTestEvent) error
}

var sqlObservations struct {
	sync.RWMutex
	hooks sqlTestHooks
}

func setSQLHooksForTest(h sqlTestHooks) {
	sqlObservations.Lock()
	sqlObservations.hooks = h
	sqlObservations.Unlock()
}

// observeSQL provides inert observation points for exact native-boundary tests.
// It deliberately does not invoke the fault hook or change control flow.
func observeSQL(e sqlTestEvent) {
	sqlObservations.RLock()
	observe := sqlObservations.hooks.Observe
	sqlObservations.RUnlock()
	if observe != nil {
		observe(e)
	}
}
func sqlEvent(e sqlTestEvent) error {
	sqlObservations.RLock()
	h := sqlObservations.hooks
	sqlObservations.RUnlock()
	if h.Observe != nil {
		h.Observe(e)
	}
	if h.Fault != nil {
		return h.Fault(e)
	}
	return nil
}
func (c *Conn) namespaceForTest() string { return c.root.key }
func (c *Conn) identityForTest() (uint64, uint64) {
	registry.Lock()
	defer registry.Unlock()
	return c.root.main.dev, c.root.main.ino
}
func (c *Conn) holdStatementForTest(sql string) (func() error, error) {
	if err := c.lockContext(context.Background(), time.Time{}); err != nil {
		return nil, err
	}
	defer c.unlock()
	if c.closed || c.used {
		return nil, safeError(PreparePhase, ErrClosed)
	}
	stmt, err := c.prepareRaw(sql, PreparePhase)
	if err != nil {
		return nil, err
	}
	return func() error {
		if err := c.lockContext(context.Background(), time.Time{}); err != nil {
			return err
		}
		defer c.unlock()
		if stmt == 0 {
			return nil
		}
		rc := lib.Xsqlite3_finalize(c.tls, stmt)
		stmt = 0
		if rc != lib.SQLITE_OK {
			return engineError(FinalizePhase, rc, nil)
		}
		return nil
	}, nil
}
