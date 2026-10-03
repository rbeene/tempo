//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

func validInitialNames(a, d string) bool {
	if a == "" || a == "." || a == ".." || strings.ContainsAny(a, "/\x00") || len(a)+5 > 255 ||
		d == "" || d == "." || d == ".." || strings.ContainsAny(d, "/\x00?") || len(d)+8 > 255 {
		return false
	}
	for _, name := range []string{d, d + "-wal", d + "-shm", d + "-journal"} {
		if a == name || a+".lock" == name {
			return false
		}
	}
	return true
}
func requireAbsent(r *rootEntry, name string) error {
	var st unix.Stat_t
	err := unix.Fstatat(r.fd, name, &st, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err == nil {
		return safeError(Admission, ErrUnsafe)
	}
	return safeError(Admission, err)
}

// os.ErrExist is reserved for the configured authority, not SQLite collisions.
func requireAuthorityAbsent(r *rootEntry, name string) error {
	var st unix.Stat_t
	err := unix.Fstatat(r.fd, name, &st, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err == nil {
		return &Error{Phase: Admission, Category: Unsafe, Cause: os.ErrExist}
	}
	return safeError(Admission, err)
}
func noSidecars(r *rootEntry) error {
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if err := requireAbsent(r, r.databaseName+suffix); err != nil {
			return err
		}
	}
	return nil
}

// The initial profile must be established from the actual main file before
// any sidecar can make a DELETE prefix appear to be an existing WAL database.
func beginInitialProbe(r *rootEntry) {
	registry.Lock()
	r.initialProbe = true
	registry.Unlock()
}
func initialProbeError(r *rootEntry) error {
	registry.Lock()
	refused := r.initialProbeRefused
	registry.Unlock()
	if refused {
		return safeError(OpenPhase, ErrUnsafe)
	}
	return nil
}
func finishInitialProbe(r *rootEntry) error {
	registry.Lock()
	defer registry.Unlock()
	if r.initialProbeRefused {
		return safeError(OpenPhase, ErrUnsafe)
	}
	r.initialProbe = false
	return nil
}
func refuseInitialSidecar(r *rootEntry, name, operation string) bool {
	registry.Lock()
	refused := r.initialProbe && name != r.databaseName
	if refused {
		r.initialProbeRefused = true
	}
	registry.Unlock()
	if refused {
		emit(pathEvent(r, name, operation, "initial-probe-refused", 0, -1, ErrUnsafe))
	}
	return refused
}

func armExclusive(r *rootEntry) error {
	if err := requireAbsent(r, r.databaseName); err != nil {
		return err
	}
	if err := noSidecars(r); err != nil {
		return err
	}
	registry.Lock()
	r.exclusive = true
	registry.Unlock()
	return nil
}
func refuseExclusive(r *rootEntry, name string, cause error) {
	registry.Lock()
	active := name == r.databaseName && r.exclusive
	if active {
		r.exclusiveRefused = true
	}
	registry.Unlock()
	if active {
		emit(pathEvent(r, name, "exclusive", "refused", 0, -1, cause))
	}
}
func exclusiveAttempt(r *rootEntry, flags int) error {
	registry.Lock()
	bad := r.exclusiveRefused || r.exclusiveCreated || flags&unix.O_ACCMODE != unix.O_RDWR || flags&unix.O_CREAT == 0
	if bad {
		r.exclusiveRefused = true
	}
	registry.Unlock()
	if bad {
		emit(pathEvent(r, r.databaseName, "exclusive", "refused", flags, -1, ErrUnsafe))
		return ErrUnsafe
	}
	if err := admissionError(r.openContext, r.openDeadline); err != nil {
		refuseExclusive(r, r.databaseName, err)
		return err
	}
	emit(pathEvent(r, r.databaseName, "exclusive", "attempt", flags|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, -1, nil))
	return nil
}

// This helper opens only a registered guard or a closed-native main/WAL role.
// Preflight never substitutes for validation of the actual descriptor. Rejected
// descriptors enter the same bounded poison/quarantine as native VFS opens.
func openOwnedFile(r *rootEntry, name string, flags int, guard bool) (int, error) {
	var expected identity
	var st unix.Stat_t
	err := unix.Fstatat(r.fd, name, &st, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil {
		if !privateFile(&st) {
			return -1, ErrUnsafe
		}
		expected = fileIdentity(&st)
	} else if !errors.Is(err, unix.ENOENT) || flags&unix.O_CREAT == 0 {
		return -1, err
	}
	registry.Lock()
	bad := registry.poisoned || registry.fatal || r.poisoned || expected != (identity{}) && aliasLocked(expected, r, name)
	if !guard {
		known := r.known[name]
		bad = bad || known != (identity{}) && expected != known
	}
	registry.Unlock()
	if bad {
		return -1, ErrUnsafe
	}
	flags |= unix.O_NOFOLLOW | unix.O_CLOEXEC
	emit(pathEvent(r, name, "open", "before", flags, -1, nil))
	registry.Lock()
	if registry.poisoned || registry.fatal || r.poisoned {
		registry.Unlock()
		return -1, ErrUnsafe
	}
	fd, err := unix.Openat(r.fd, name, flags, 0600)
	if err != nil {
		registry.Unlock()
		emit(pathEvent(r, name, "open", "after", flags, -1, err))
		return -1, err
	}
	var actual, named unix.Stat_t
	e1, e2 := unix.Fstat(fd, &actual), unix.Fstatat(r.fd, name, &named, unix.AT_SYMLINK_NOFOLLOW)
	id := fileIdentity(&actual)
	bad = e1 != nil || e2 != nil || !privateFile(&actual) || !privateFile(&named) || id != fileIdentity(&named) || expected != (identity{}) && id != expected || aliasLocked(id, r, name)
	if !guard {
		known := r.known[name]
		bad = bad || known != (identity{}) && id != known
	}
	if bad {
		registry.rejected = append(registry.rejected, fd)
		registry.poisoned, r.poisoned = true, true
		registry.Unlock()
		emit(pathEvent(r, name, "open", "quarantined", flags, fd, ErrUnsafe))
		return -1, ErrUnsafe
	}
	if guard {
		r.guardFD, r.guardID = fd, id
	} else {
		r.known[name] = id
	}
	registry.Unlock()
	emit(pathEvent(r, name, "open", "validated", flags, fd, nil))
	return fd, nil
}

func closeOwnedFile(fd int) error {
	if err := unix.Close(fd); err != nil {
		// The descriptor's disposition is unknown. Never retry its numeric value.
		registry.Lock()
		registry.fatal, registry.poisoned = true, true
		registry.Unlock()
		return safeError(ClosePhase, err)
	}
	return nil
}
func closeGuard(r *rootEntry) error {
	registry.Lock()
	fd, id := r.guardFD, r.guardID
	if fd < 0 || id == (identity{}) {
		registry.Unlock()
		return nil
	}
	// Keep the identity registered across unlock/close. No new alias admission can
	// race this close and silently discard another connection's POSIX locks.
	var st unix.Stat_t
	var result error
	if err := unix.Fstat(fd, &st); err != nil || fileIdentity(&st) != id {
		result = safeError(ClosePhase, ErrUnsafe)
	} else {
		if err := unix.Flock(fd, unix.LOCK_UN); err != nil {
			result = safeError(ClosePhase, err)
		}
		if err := unix.Close(fd); err != nil {
			result = combineClose(result, safeError(ClosePhase, err))
		}
	}
	r.guardFD, r.guardID = -1, identity{}
	if result != nil {
		registry.fatal, registry.poisoned = true, true
	}
	registry.Unlock()
	phase := "closed"
	if result != nil {
		phase = "failed"
	}
	emit(pathEvent(r, r.guardName, "close", phase, 0, fd, result))
	return result
}

func syncOwned(ctx context.Context, r *rootEntry, fd int, role, operation string) error {
	if err := ctx.Err(); err != nil {
		return safeError(ClosePhase, err)
	}
	emit(event{Namespace: r.key, Role: role, Op: "fsync", Phase: "before", FD: fd})
	if err := sqlEvent(sqlTestEvent{Phase: "fsync-before", Operation: operation}); err != nil {
		return safeError(ClosePhase, err)
	}
	if err := ctx.Err(); err != nil {
		return safeError(ClosePhase, err)
	}
	err := unix.Fsync(fd)
	e := event{Namespace: r.key, Role: role, Op: "fsync", Phase: "after", FD: fd}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		e.Errno = int(errno)
	}
	emit(e)
	if err != nil {
		return safeError(ClosePhase, err)
	}
	if err = sqlEvent(sqlTestEvent{Phase: "fsync-after", Operation: operation}); err != nil {
		return safeError(ClosePhase, err)
	}
	if err = ctx.Err(); err != nil {
		return safeError(ClosePhase, err)
	}
	return nil
}
func acquireInitialGuard(ctx context.Context, r *rootEntry, a string) error {
	if err := requireAuthorityAbsent(r, a); err != nil {
		return err
	}
	registry.Lock()
	r.guardName = a + ".lock"
	registry.Unlock()
	var st unix.Stat_t
	err := unix.Fstatat(r.fd, r.guardName, &st, unix.AT_SYMLINK_NOFOLLOW)
	created := errors.Is(err, unix.ENOENT)
	flags := unix.O_RDWR
	if created {
		flags |= unix.O_CREAT | unix.O_EXCL
	} else if err != nil {
		return safeError(Admission, err)
	}
	fd, err := openOwnedFile(r, r.guardName, flags, true)
	if created && errors.Is(err, unix.EEXIST) {
		// Exactly one bounded admission of an existing guard after a creation race.
		created = false
		if err = admissionError(ctx, r.openDeadline); err == nil {
			fd, err = openOwnedFile(r, r.guardName, unix.O_RDWR, true)
		}
	}
	if err != nil {
		return safeError(Admission, err)
	}
	if err = admissionError(ctx, r.openDeadline); err != nil {
		return safeError(Admission, err)
	}
	if created {
		if err = syncOwned(ctx, r, fd, "guard", "guard-file"); err != nil {
			return err
		}
		if err = syncOwned(ctx, r, r.fd, "root", "guard-parent"); err != nil {
			return err
		}
	}
	for {
		if err = admissionError(ctx, r.openDeadline); err != nil {
			return safeError(Admission, err)
		}
		err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			return safeError(Admission, err)
		}
		timer := time.NewTimer(min(2*time.Millisecond, time.Until(r.openDeadline)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return safeError(Admission, ctx.Err())
		case <-timer.C:
		}
	}
	emit(event{Namespace: r.key, Role: "guard", Op: "flock", Phase: "acquired", FD: fd})
	if err = admissionError(ctx, r.openDeadline); err != nil {
		return safeError(Admission, err)
	}
	if err = validateRoot(r); err != nil {
		return safeError(Admission, err)
	}
	return requireAuthorityAbsent(r, a)
}

// OpenForInitialLink holds the existing authority guard and one root lease
// throughout read-only inspection, checked close, and non-creating reopen.
// It installs no application schema and never adopts an occupied authority.
func OpenForInitialLink(ctx context.Context, directory, a, d string, deadline time.Time) (*Conn, error) {
	if err := admissionError(ctx, deadline); err != nil {
		return nil, safeError(Admission, err)
	}
	if !validInitialNames(a, d) {
		return nil, &Error{Phase: Admission, Category: Invalid}
	}
	r, err := acquireRoot(ctx, directory, d, false, deadline)
	if err != nil {
		return nil, safeError(Admission, err)
	}
	c := newConnection(r, Options{AcquireDeadline: deadline})
	fail := func(e error) (*Conn, error) { return failOpen(c, combineClose(e, initialProbeError(r))) }
	admit, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err = acquireInitialGuard(admit, r, a); err != nil {
		return fail(err)
	}
	if err = initialize(); err != nil {
		return fail(safeError(OpenPhase, err))
	}
	var st unix.Stat_t
	err = unix.Fstatat(r.fd, d, &st, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		if err = armExclusive(r); err != nil {
			return fail(err)
		}
		r.create = true
		if err = c.openNative(admit, Options{Create: true, ExclusiveCreate: true, AcquireDeadline: deadline}); err != nil {
			return fail(err)
		}
		stop := c.interruptWith(admit)
		err = c.setup(admit, true)
		stop()
		if err != nil {
			return fail(err)
		}
	} else {
		if err != nil || !privateFile(&st) {
			return fail(safeError(Admission, ErrUnsafe))
		}
		beginInitialProbe(r)
		if err = c.openNative(admit, Options{ReadOnly: true, AcquireDeadline: deadline}); err != nil {
			return fail(err)
		}
		stop := c.interruptWith(admit)
		pristine, proofErr := c.initialMainProfile(admit)
		if proofErr == nil && !pristine {
			proofErr = finishInitialProbe(r)
		}
		var image []byte
		var mode string
		if proofErr == nil {
			mode, proofErr = c.probeText(admit, "PRAGMA journal_mode")
		}
		if proofErr == nil {
			if pristine && mode == "delete" {
				image, proofErr = c.provePristine(admit)
			} else if !pristine && mode == "wal" {
				proofErr = c.setup(admit, false)
			} else {
				proofErr = safeError(OpenPhase, ErrUnsafe)
			}
		}
		stop()
		if proofErr != nil {
			return fail(proofErr)
		}
		if err = sqlEvent(sqlTestEvent{Phase: "initial-probe-verified", Operation: "initial-link"}); err != nil {
			return fail(safeError(OpenPhase, err))
		}
		if err = admissionError(admit, deadline); err != nil {
			return fail(safeError(Admission, err))
		}
		if err = c.closeNative(); err != nil {
			return fail(err)
		}
		if err = sqlEvent(sqlTestEvent{Phase: "initial-probe-closed", Operation: "initial-link"}); err != nil {
			return fail(safeError(OpenPhase, err))
		}
		if err = admissionError(admit, deadline); err != nil {
			return fail(safeError(Admission, err))
		}
		if err = validateRoot(r); err != nil {
			return fail(safeError(VerifyPhase, err))
		}
		if pristine {
			if err = closedPristineImage(r, image); err != nil {
				return fail(err)
			}
		}
		if err = sqlEvent(sqlTestEvent{Phase: "initial-reopen-before", Operation: "initial-link"}); err != nil {
			return fail(safeError(OpenPhase, err))
		}
		if err = admissionError(admit, deadline); err != nil {
			return fail(safeError(Admission, err))
		}
		if err = validateRoot(r); err != nil {
			return fail(safeError(VerifyPhase, err))
		}
		if pristine {
			if err = noSidecars(r); err != nil {
				return fail(err)
			}
		}
		beginInitialProbe(r)
		if err = c.openNative(admit, Options{AcquireDeadline: deadline}); err != nil {
			return fail(err)
		}
		stop = c.interruptWith(admit)
		againPristine, profileErr := c.initialMainProfile(admit)
		err = profileErr
		if err == nil && againPristine != pristine {
			err = safeError(VerifyPhase, ErrUnsafe)
		}
		if err == nil && !pristine {
			err = finishInitialProbe(r)
		}
		if err == nil && pristine {
			var again []byte
			again, err = c.provePristine(admit)
			if err == nil && !bytes.Equal(image, again) {
				err = safeError(VerifyPhase, ErrUnsafe)
			}
		} else if err == nil {
			mode, err = c.probeText(admit, "PRAGMA journal_mode")
			if err == nil && mode != "wal" {
				err = safeError(VerifyPhase, ErrUnsafe)
			}
		}
		if err == nil && pristine {
			err = noSidecars(r)
			if err == nil {
				err = finishInitialProbe(r)
			}
		}
		if err == nil {
			err = c.setup(admit, pristine)
		}
		stop()
		if err != nil {
			return fail(err)
		}
	}
	if err = sqlEvent(sqlTestEvent{Phase: "initial-wal-ready", Operation: "initial-link"}); err != nil {
		return fail(safeError(OpenPhase, err))
	}
	if err = admissionError(admit, deadline); err != nil {
		return fail(safeError(Admission, err))
	}
	if err = validateRoot(r); err != nil {
		return fail(safeError(VerifyPhase, err))
	}
	if err = requireAuthorityAbsent(r, a); err != nil {
		return fail(err)
	}
	return c, nil
}

// Fixed probe SQL only: exactly one column, exactly one typed row and DONE.
// Authorizer remains installed during prepare and automatic reprepare.
func (c *Conn) probeScalar(ctx context.Context, sql string, kind int32) (number int64, text string, err error) {
	if err = admissionError(ctx, c.acquireDeadline); err != nil {
		return 0, "", safeError(Admission, err)
	}
	if err = initialProbeError(c.root); err != nil {
		return 0, "", err
	}
	if err = c.busyRemaining(ctx); err != nil {
		return 0, "", err
	}
	*(*uint32)(unsafe.Pointer(c.authMode)) = authPragma
	defer func() { *(*uint32)(unsafe.Pointer(c.authMode)) = authApplication }()
	stmt, err := c.prepareRaw(sql, OpenPhase)
	if err != nil {
		return 0, "", contextualError(OpenPhase, err, ctx)
	}
	defer func() {
		rc := lib.Xsqlite3_finalize(c.tls, stmt)
		if rc != lib.SQLITE_OK {
			err = combineClose(err, engineError(FinalizePhase, rc, ctx))
		}
		err = combineClose(err, initialProbeError(c.root))
		if err != nil {
			number, text = 0, ""
		}
	}()
	if lib.Xsqlite3_column_count(c.tls, stmt) != 1 || lib.Xsqlite3_bind_parameter_count(c.tls, stmt) != 0 {
		return 0, "", safeError(OpenPhase, ErrUnsafe)
	}
	if err = admissionError(ctx, c.acquireDeadline); err != nil {
		return 0, "", safeError(Admission, err)
	}
	rc := lib.Xsqlite3_step(c.tls, stmt)
	if rc != lib.SQLITE_ROW {
		return 0, "", engineError(OpenPhase, rc, ctx)
	}
	if lib.Xsqlite3_column_type(c.tls, stmt, 0) != kind {
		return 0, "", safeError(OpenPhase, ErrUnsafe)
	}
	if kind == lib.SQLITE_INTEGER {
		number = lib.Xsqlite3_column_int64(c.tls, stmt, 0)
	} else {
		p := lib.Xsqlite3_column_text(c.tls, stmt, 0)
		n := lib.Xsqlite3_column_bytes(c.tls, stmt, 0)
		if p == 0 || n < 0 || n > 256 {
			return 0, "", safeError(OpenPhase, ErrUnsafe)
		}
		text = string(unsafe.Slice((*byte)(unsafe.Pointer(p)), int(n)))
	}
	if err = admissionError(ctx, c.acquireDeadline); err != nil {
		return 0, "", safeError(Admission, err)
	}
	if rc = lib.Xsqlite3_step(c.tls, stmt); rc != lib.SQLITE_DONE {
		return 0, "", engineError(OpenPhase, rc, ctx)
	}
	return number, text, nil
}
func (c *Conn) probeText(ctx context.Context, sql string) (string, error) {
	_, s, e := c.probeScalar(ctx, sql, lib.SQLITE_TEXT)
	return s, e
}
func (c *Conn) probeInteger(ctx context.Context, sql string) (int64, error) {
	n, _, e := c.probeScalar(ctx, sql, lib.SQLITE_INTEGER)
	return n, e
}

// Read through SQLite's existing main file. Opening a duplicate OS main FD here
// would release POSIX locks when closed and is forbidden while native is live.
func (c *Conn) nativeSmallImage() ([]byte, error) {
	image, _, err := c.nativeMainImage(true)
	return image, err
}

// prefix mode reads the complete 100-byte header through the native descriptor.
// pristine mode retains the existing exact 0-or-4096-byte image proof.
func (c *Conn) nativeMainImage(pristine bool) (image []byte, physicalSize int64, err error) {
	p := lib.Xsqlite3_malloc64(c.tls, 8)
	if p == 0 {
		return nil, 0, engineError(OpenPhase, lib.SQLITE_NOMEM, nil)
	}
	defer lib.Xsqlite3_free(c.tls, p)
	*(*uintptr)(unsafe.Pointer(p)) = 0
	if rc := lib.Xsqlite3_file_control(c.tls, c.db, 0, lib.SQLITE_FCNTL_FILE_POINTER, p); rc != lib.SQLITE_OK {
		return nil, 0, engineError(OpenPhase, rc, nil)
	}
	file := *(*uintptr)(unsafe.Pointer(p))
	if file == 0 {
		return nil, 0, safeError(OpenPhase, ErrUnsafe)
	}
	methods := (*lib.Tsqlite3_file)(unsafe.Pointer(file)).FpMethods
	if methods == 0 {
		return nil, 0, safeError(OpenPhase, ErrUnsafe)
	}
	io := (*lib.Tsqlite3_io_methods)(unsafe.Pointer(methods))
	if io.FxFileSize == 0 || io.FxRead == 0 {
		return nil, 0, safeError(OpenPhase, ErrUnsafe)
	}
	sizeFn := *(*func(*libc.TLS, uintptr, uintptr) int32)(unsafe.Pointer(&io.FxFileSize))
	*(*int64)(unsafe.Pointer(p)) = -1
	if rc := sizeFn(c.tls, file, p); rc != lib.SQLITE_OK {
		return nil, 0, engineError(OpenPhase, rc, nil)
	}
	physicalSize = *(*int64)(unsafe.Pointer(p))
	n := physicalSize
	if pristine {
		if n != 0 && n != 4096 {
			return nil, 0, safeError(OpenPhase, ErrUnsafe)
		}
	} else if n != 0 {
		if n < PageSize {
			return nil, 0, safeError(OpenPhase, ErrUnsafe)
		}
		n = 100
	}
	image = make([]byte, int(n))
	if n == 0 {
		return image, physicalSize, nil
	}
	buffer := lib.Xsqlite3_malloc64(c.tls, uint64(n))
	if buffer == 0 {
		return nil, 0, engineError(OpenPhase, lib.SQLITE_NOMEM, nil)
	}
	defer lib.Xsqlite3_free(c.tls, buffer)
	readFn := *(*func(*libc.TLS, uintptr, uintptr, int32, lib.Tsqlite3_int64) int32)(unsafe.Pointer(&io.FxRead))
	if rc := readFn(c.tls, file, buffer, int32(n), 0); rc != lib.SQLITE_OK {
		return nil, 0, engineError(OpenPhase, rc, nil)
	}
	copy(image, unsafe.Slice((*byte)(unsafe.Pointer(buffer)), int(n)))
	return image, physicalSize, nil
}

// The prefix selects an admissible probe path, not a valid database verdict.
// No SQL, another main FD, WAL read, or sidecar mutation supplies this evidence.
func (c *Conn) initialMainProfile(ctx context.Context) (pristine bool, err error) {
	if err = admissionError(ctx, c.acquireDeadline); err != nil {
		return false, safeError(Admission, err)
	}
	if err = initialProbeError(c.root); err != nil {
		return false, err
	}
	if err = validateRoot(c.root); err != nil {
		return false, safeError(VerifyPhase, err)
	}
	prefix, physicalSize, err := c.nativeMainImage(false)
	if err != nil {
		return false, err
	}
	pristine = physicalSize == 0
	if !pristine {
		// These fixed fields cannot be supplied by a sidecar to qualify an
		// unsupported main. Dynamic page/schema counters remain SQLite's job.
		if len(prefix) != 100 || physicalSize < PageSize ||
			string(prefix[:16]) != "SQLite format 3\x00" ||
			prefix[16] != 0x10 || prefix[17] != 0 ||
			!bytes.Equal(prefix[21:24], []byte{64, 32, 32}) ||
			PageSize-int64(prefix[20]) < 480 {
			return false, safeError(OpenPhase, ErrUnsafe)
		}
		switch {
		case prefix[18] == 1 && prefix[19] == 1:
			pristine = true
		case prefix[18] == 2 && prefix[19] == 2:
			// Existing supported WAL profile; native recovery still verifies it.
		default:
			return false, safeError(OpenPhase, ErrUnsafe)
		}
	}
	if pristine {
		if err = noSidecars(c.root); err != nil {
			return false, err
		}
	}
	if err = admissionError(ctx, c.acquireDeadline); err != nil {
		return false, safeError(Admission, err)
	}
	if err = validateRoot(c.root); err != nil {
		return false, safeError(VerifyPhase, err)
	}
	return pristine, initialProbeError(c.root)
}

func (c *Conn) provePristine(ctx context.Context) ([]byte, error) {
	if err := noSidecars(c.root); err != nil {
		return nil, err
	}
	image, err := c.nativeSmallImage()
	if err != nil {
		return nil, err
	}
	for _, q := range []struct {
		sql  string
		want int64
	}{
		{"PRAGMA page_size", 4096}, {"PRAGMA page_count", int64(len(image) / 4096)},
		{"SELECT EXISTS(SELECT 1 FROM sqlite_schema LIMIT 1)", 0},
		{"PRAGMA freelist_count", 0}, {"PRAGMA schema_version", 0}, {"PRAGMA user_version", 0},
		{"PRAGMA application_id", 0}, {"PRAGMA auto_vacuum", 0},
	} {
		n, e := c.probeInteger(ctx, q.sql)
		if e != nil {
			return nil, e
		}
		if n != q.want {
			return nil, safeError(OpenPhase, ErrUnsafe)
		}
	}
	text, err := c.probeText(ctx, "PRAGMA integrity_check")
	if err != nil {
		return nil, err
	}
	if text != "ok" {
		return nil, safeError(OpenPhase, ErrUnsafe)
	}
	if err = noSidecars(c.root); err != nil {
		return nil, err
	}
	if err = validateRoot(c.root); err != nil {
		return nil, safeError(VerifyPhase, err)
	}
	return image, nil
}
func closedPristineImage(r *rootEntry, want []byte) (err error) {
	if err = noSidecars(r); err != nil {
		return err
	}
	fd, e := openOwnedFile(r, r.databaseName, unix.O_RDONLY, false)
	if e != nil {
		return safeError(VerifyPhase, e)
	}
	defer func() { err = combineClose(err, closeOwnedFile(fd)) }()
	var st unix.Stat_t
	if e = unix.Fstat(fd, &st); e != nil || st.Size != int64(len(want)) {
		return safeError(VerifyPhase, ErrUnsafe)
	}
	got := make([]byte, len(want))
	if len(got) > 0 {
		n, e := unix.Pread(fd, got, 0)
		if e != nil || n != len(got) {
			return safeError(VerifyPhase, ErrUnsafe)
		}
	}
	if !bytes.Equal(got, want) {
		return safeError(VerifyPhase, ErrUnsafe)
	}
	if err = noSidecars(r); err != nil {
		return err
	}
	if err = validateRoot(r); err != nil {
		return safeError(VerifyPhase, err)
	}
	return nil
}
