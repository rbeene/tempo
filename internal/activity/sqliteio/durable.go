//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"time"

	"golang.org/x/sys/unix"
	lib "modernc.org/sqlite/lib"
)

// familyIdentity admits no previously unobserved role. Native authorized unlink
// removes its known identity; unrelated namespace changes never count as such.
// No main, WAL or SHM descriptor is opened by this inspection.
func familyIdentity(r *rootEntry) (map[string]identity, error) {
	if err := validateRoot(r); err != nil {
		return nil, safeError(VerifyPhase, err)
	}
	result := make(map[string]identity, 4)
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		name := r.databaseName + suffix
		var st unix.Stat_t
		err := unix.Fstatat(r.fd, name, &st, unix.AT_SYMLINK_NOFOLLOW)
		registry.Lock()
		expected := r.known[name]
		bad := registry.poisoned || registry.fatal || r.poisoned
		if err == nil {
			bad = bad || aliasLocked(fileIdentity(&st), r, name)
		}
		registry.Unlock()
		if bad {
			return nil, safeError(VerifyPhase, ErrUnsafe)
		}
		if errors.Is(err, unix.ENOENT) && suffix != "" && expected == (identity{}) {
			continue
		}
		if err != nil || !privateFile(&st) || expected == (identity{}) || fileIdentity(&st) != expected || suffix == "-journal" && st.Size != 0 {
			return nil, safeError(VerifyPhase, ErrUnsafe)
		}
		result[name] = expected
	}
	return result, nil
}
func verifyFamily(r *rootEntry, want map[string]identity) error {
	got, err := familyIdentity(r)
	if err != nil {
		return err
	}
	if len(got) != len(want) {
		return safeError(VerifyPhase, ErrUnsafe)
	}
	for name, id := range want {
		if got[name] != id {
			return safeError(VerifyPhase, ErrUnsafe)
		}
	}
	return nil
}
func syncClosedRole(ctx context.Context, r *rootEntry, name, operation string) (err error) {
	fd, e := openOwnedFile(r, name, unix.O_RDWR, false)
	if e != nil {
		return safeError(ClosePhase, e)
	}
	defer func() { err = combineClose(err, closeOwnedFile(fd)) }()
	if err = validateRoot(r); err != nil {
		return safeError(VerifyPhase, err)
	}
	return syncOwned(ctx, r, fd, role(r, name), operation)
}

// CloseDurably adds the closed-file namespace barrier to a clean actual writer
// COMMIT. It neither commits nor checkpoints. A terminal barrier failure is
// cached: cleanup may not turn uncertain acknowledgement into later success.
func (c *Conn) CloseDurably(ctx context.Context) error {
	if c == nil || c.gate == nil {
		return safeError(ClosePhase, ErrClosed)
	}
	if err := c.lockContext(ctx, time.Time{}); err != nil {
		return err
	}
	defer c.unlock()
	if c.closed {
		if c.durableAttempted {
			return c.closeErr
		}
		return misuse(ClosePhase)
	}
	if c.durableAttempted {
		return c.closeErr
	}
	if !c.cleanWrite || c.poisoned || c.db == 0 {
		return misuse(ClosePhase)
	}
	// Retain ordinary Close ownership if native close cannot finish. This attempt
	// may never be retried as an acknowledgement, even after a statement is freed.
	c.durableAttempted = true
	failLive := func(err error) error { c.closeErr = err; return err }
	if err := ctx.Err(); err != nil {
		return failLive(safeError(ClosePhase, err))
	}
	if lib.Xsqlite3_get_autocommit(c.tls, c.db) == 0 {
		return failLive(misuse(ClosePhase))
	}
	if _, err := familyIdentity(c.root); err != nil {
		return failLive(err)
	}
	if err := sqlEvent(sqlTestEvent{Phase: "close-before", Operation: "close"}); err != nil {
		return failLive(safeError(ClosePhase, err))
	}
	if err := ctx.Err(); err != nil {
		return failLive(safeError(ClosePhase, err))
	}
	if err := c.closeNative(); err != nil {
		if c.db != 0 || c.nativeCounted {
			return failLive(err)
		}
		c.finishRelease(err)
		return c.closeErr
	}
	// This snapshot is after legitimate native cleanup, before a test observer or
	// another process can redirect one of the later closed-file barrier opens.
	family, err := familyIdentity(c.root)
	if err == nil {
		err = sqlEvent(sqlTestEvent{Phase: "durable-native-closed", Operation: "close-durably", Code: lib.SQLITE_OK})
	}
	if err != nil {
		err = safeError(ClosePhase, err)
	}
	if err == nil && ctx.Err() != nil {
		err = safeError(ClosePhase, ctx.Err())
	}
	if err == nil {
		err = verifyFamily(c.root, family)
	}
	if err == nil {
		err = syncClosedRole(ctx, c.root, c.root.databaseName, "durable-main")
	}
	if err == nil {
		err = verifyFamily(c.root, family)
	}
	if err == nil && family[c.root.databaseName+"-wal"] != (identity{}) {
		err = syncClosedRole(ctx, c.root, c.root.databaseName+"-wal", "durable-wal")
	}
	if err == nil {
		err = verifyFamily(c.root, family)
	}
	if err == nil {
		err = syncOwned(ctx, c.root, c.root.fd, "root", "durable-parent")
	}
	if err == nil {
		err = verifyFamily(c.root, family)
	}
	if err == nil {
		err = sqlEvent(sqlTestEvent{Phase: "durable-release-before", Operation: "close-durably"})
	}
	if err != nil {
		err = safeError(ClosePhase, err)
	}
	if err == nil && ctx.Err() != nil {
		err = safeError(ClosePhase, ctx.Err())
	}
	c.finishRelease(err)
	return c.closeErr
}
