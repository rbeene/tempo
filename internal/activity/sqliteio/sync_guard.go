//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// SyncRunGuard owns only the fixed process-coordination file. Copies share one
// checked owner; the admission deadline does not expire an acquired flock.
type SyncRunGuard struct{ owner *syncGuardOwner }

type syncGuardKey struct {
	directory identity
	name      string
}

type syncGuardEntry struct {
	key       syncGuardKey
	namespace string
	gate      chan struct{}
	refs      int
	fd        int
	id        identity
	unknown   bool
}

type syncGuardOwner struct {
	mu                             sync.Mutex
	ctx                            context.Context
	path, authority                string
	dirFD                          int
	dirID                          identity
	entry                          *syncGuardEntry
	gateHeld                       bool
	usable, terminal, closeStarted bool
	pendingDrain                   bool
	lostDirectories                []int // disposition unknown; these numbers are never retried
	closeErr                       error
}

func validSyncNames(a, d string) bool {
	if !validInitialNames(a, d) || len(a)+len(".sync.lock") > 255 {
		return false
	}
	s := a + ".sync.lock"
	for _, name := range []string{a, a + ".lock", d, d + "-wal", d + "-shm", d + "-journal"} {
		if s == name {
			return false
		}
	}
	return true
}

// AcquireSyncRunGuard never opens the authority or a SQLite role and never
// creates a parent. An error can carry a cleanup-only guard which must be kept.
func AcquireSyncRunGuard(ctx context.Context, directory, stateBasename, databaseBasename string, deadline time.Time) (*SyncRunGuard, error) {
	if err := admissionError(ctx, deadline); err != nil {
		return nil, safeError(Admission, err)
	}
	if !validSyncNames(stateBasename, databaseBasename) || !filepath.IsAbs(directory) || strings.ContainsRune(directory, 0) {
		return nil, safeError(Admission, ErrUnsafe)
	}
	for _, part := range strings.Split(directory, "/") {
		if part == ".." {
			return nil, safeError(Admission, ErrUnsafe)
		}
	}
	o := &syncGuardOwner{ctx: ctx, path: filepath.Clean(directory), authority: stateBasename, dirFD: -1}
	g := &SyncRunGuard{owner: o}
	fail := func(primary error) (*SyncRunGuard, error) {
		o.usable = false
		terminal, cleanup := g.Close()
		err := joinCleanup(primary, cleanup)
		if terminal {
			return nil, err
		}
		return g, err
	}
	fd, id, err := o.pinDirectory(deadline, Admission)
	if err != nil {
		return fail(err)
	}
	o.dirFD, o.dirID = fd, id
	key := syncGuardKey{directory: id, name: stateBasename + ".sync.lock"}
	registry.Lock()
	if registry.poisoned || registry.fatal {
		registry.Unlock()
		return fail(safeError(Admission, ErrUnsafe))
	}
	e := registry.syncGuards[key]
	if e == nil {
		e = &syncGuardEntry{key: key, namespace: fmt.Sprintf("%ssync-%x-%x-%x", namespacePrefix, id.dev, id.ino, sha256.Sum256([]byte(key.name))), fd: -1, gate: make(chan struct{}, 1)}
		e.gate <- struct{}{}
		registry.syncGuards[key] = e
	}
	e.refs++
	o.entry = e
	registry.Unlock()
	timer := time.NewTimer(time.Until(deadline))
	select {
	case <-ctx.Done():
		err = safeError(Admission, ctx.Err())
	case <-timer.C:
		err = safeError(Admission, ErrBusy)
	case <-e.gate:
		o.gateHeld = true
	}
	timer.Stop()
	// Cancellation wins a simultaneous ready timer/gate without creating S.
	if cause := admissionError(ctx, deadline); cause != nil {
		err = safeError(Admission, cause)
	}
	if err != nil {
		return fail(err)
	}
	if err = o.verifyDirectory(deadline, Admission); err != nil {
		return fail(err)
	}
	if err = o.authorityAbsent(Admission); err != nil {
		return fail(err)
	}
	if err = o.open(deadline); err != nil {
		return fail(err)
	}
	for {
		if cause := admissionError(ctx, deadline); cause != nil {
			return fail(safeError(Admission, cause))
		}
		emit(o.event("flock", "before", unix.LOCK_EX|unix.LOCK_NB, e.fd, nil))
		registry.Lock()
		if registry.poisoned || registry.fatal || e.unknown {
			err = ErrUnsafe
		} else {
			var actual unix.Stat_t
			if cause := unix.Fstat(e.fd, &actual); cause != nil || fileIdentity(&actual) != e.id || actual.Mode&unix.S_IFMT != unix.S_IFREG {
				e.unknown = true
				registry.fatal, registry.poisoned = true, true
				err = ErrUnsafe
			} else if !privateFile(&actual) {
				err = ErrUnsafe
			} else {
				err = unix.Flock(e.fd, unix.LOCK_EX|unix.LOCK_NB)
			}
		}
		registry.Unlock()
		emit(o.event("flock", "after", unix.LOCK_EX|unix.LOCK_NB, e.fd, err))
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			return fail(safeError(Admission, err))
		}
		wait := 5 * time.Millisecond
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
	}
	if cause := admissionError(ctx, deadline); cause != nil {
		return fail(safeError(Admission, cause))
	}
	if err = o.verify(deadline, Admission); err != nil {
		return fail(err)
	}
	o.usable = true
	emit(o.event("lease", "acquired", 0, e.fd, nil))
	return g, nil
}

func (o *syncGuardOwner) event(op, phase string, flags, fd int, err error) event {
	e := event{Role: "sync-guard", Op: op, Phase: phase, Flags: flags, FD: fd}
	if o.entry != nil {
		e.Namespace = o.entry.namespace
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		e.Errno = int(errno)
	} else if err != nil {
		e.Errno = int(unix.EIO)
	}
	return e
}

// Temporary directory descriptors are independently checked and closed. A lost
// disposition is retained on this owner and poisons admission, never retried.
func (o *syncGuardOwner) loseDirectory(fd int, cause error) error {
	o.lostDirectories = append(o.lostDirectories, fd)
	registry.Lock()
	registry.fatal, registry.poisoned = true, true
	registry.Unlock()
	err := safeError(ClosePhase, cause)
	o.closeErr = joinCleanup(o.closeErr, err)
	return err
}
func (o *syncGuardOwner) closeDirectory(fd int, id identity) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || fileIdentity(&st) != id {
		return o.loseDirectory(fd, ErrUnsafe)
	}
	if err := unix.Close(fd); err != nil {
		return o.loseDirectory(fd, err)
	}
	return nil
}
func (o *syncGuardOwner) check(deadline time.Time, phase Phase) error {
	if err := o.ctx.Err(); err != nil {
		return safeError(phase, err)
	}
	if !deadline.IsZero() {
		if err := admissionError(o.ctx, deadline); err != nil {
			return safeError(phase, err)
		}
	}
	return nil
}
func (o *syncGuardOwner) pinDirectory(deadline time.Time, phase Phase) (out int, id identity, err error) {
	out = -1
	if err = o.check(deadline, phase); err != nil {
		return
	}
	fd, cause := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if cause != nil {
		err = safeError(phase, cause)
		return
	}
	var st unix.Stat_t
	if cause = unix.Fstat(fd, &st); cause != nil {
		err = o.loseDirectory(fd, cause)
		return
	}
	heldID := fileIdentity(&st)
	keep := false
	defer func() {
		if !keep {
			err = joinCleanup(err, o.closeDirectory(fd, heldID))
			out, id = -1, identity{}
		}
	}()
	for _, part := range strings.Split(strings.TrimPrefix(o.path, "/"), "/") {
		if part == "" {
			continue
		}
		if err = o.check(deadline, phase); err != nil {
			return
		}
		next, cause := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if cause != nil {
			err = safeError(phase, cause)
			return
		}
		var nextStat unix.Stat_t
		if cause = unix.Fstat(next, &nextStat); cause != nil {
			err = o.loseDirectory(next, cause)
			return
		}
		previous, previousID := fd, heldID
		fd, heldID = next, fileIdentity(&nextStat)
		if err = o.closeDirectory(previous, previousID); err != nil {
			return
		}
	}
	if cause = unix.Fstat(fd, &st); cause != nil {
		err = safeError(phase, cause)
		return
	}
	if !privateDirectory(&st) || fileIdentity(&st) != heldID {
		err = safeError(phase, ErrUnsafe)
		return
	}
	if err = o.check(deadline, phase); err != nil {
		return
	}
	keep = true
	return fd, heldID, nil
}
func (o *syncGuardOwner) verifyDirectory(deadline time.Time, phase Phase) error {
	if err := o.check(deadline, phase); err != nil {
		return err
	}
	registry.Lock()
	bad := registry.poisoned || registry.fatal
	registry.Unlock()
	if bad {
		return safeError(phase, ErrUnsafe)
	}
	var held unix.Stat_t
	if err := unix.Fstat(o.dirFD, &held); err != nil || !privateDirectory(&held) || fileIdentity(&held) != o.dirID {
		return safeError(phase, ErrUnsafe)
	}
	fd, id, err := o.pinDirectory(deadline, phase)
	if err != nil {
		return err
	}
	closeErr := o.closeDirectory(fd, id)
	if id != o.dirID {
		err = safeError(phase, ErrUnsafe)
	}
	return joinCleanup(err, closeErr)
}
func (o *syncGuardOwner) authorityAbsent(phase Phase) error {
	var st unix.Stat_t
	err := unix.Fstatat(o.dirFD, o.authority, &st, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err == nil {
		return &Error{Phase: phase, Category: Unsafe, Cause: os.ErrExist}
	}
	return safeError(phase, err)
}

// All actual file opens and identity registration share the native registry
// mutex with closes. Observation hooks deliberately run outside that mutex.
func (o *syncGuardOwner) open(deadline time.Time) error {
	e := o.entry
	for attempt := 0; attempt < 2; attempt++ {
		if err := o.check(deadline, Admission); err != nil {
			return err
		}
		var named unix.Stat_t
		err := unix.Fstatat(o.dirFD, e.key.name, &named, unix.AT_SYMLINK_NOFOLLOW)
		expected := identity{}
		flags := unix.O_RDWR | unix.O_NOFOLLOW | unix.O_CLOEXEC
		if err == nil {
			if !privateFile(&named) {
				return safeError(Admission, ErrUnsafe)
			}
			expected = fileIdentity(&named)
		} else if errors.Is(err, unix.ENOENT) && attempt == 0 {
			flags |= unix.O_CREAT | unix.O_EXCL
		} else {
			return safeError(Admission, err)
		}
		registry.Lock()
		bad := registry.poisoned || registry.fatal || expected != (identity{}) && aliasLocked(expected, nil, "")
		registry.Unlock()
		if bad {
			return safeError(Admission, ErrUnsafe)
		}
		emit(o.event("open", "before", flags, -1, nil))
		registry.Lock()
		if cause := admissionError(o.ctx, deadline); cause != nil {
			registry.Unlock()
			return safeError(Admission, cause)
		}
		if registry.poisoned || registry.fatal {
			registry.Unlock()
			return safeError(Admission, ErrUnsafe)
		}
		fd, cause := unix.Openat(o.dirFD, e.key.name, flags, 0600)
		if cause != nil {
			registry.Unlock()
			emit(o.event("open", "after", flags, -1, cause))
			if attempt == 0 && flags&unix.O_EXCL != 0 && errors.Is(cause, unix.EEXIST) {
				continue
			}
			return safeError(Admission, cause)
		}
		var actual, current unix.Stat_t
		e1, e2 := unix.Fstat(fd, &actual), unix.Fstatat(o.dirFD, e.key.name, &current, unix.AT_SYMLINK_NOFOLLOW)
		id := fileIdentity(&actual)
		if e1 != nil || e2 != nil || !privateFile(&actual) || !privateFile(&current) || id != fileIdentity(&current) || expected != (identity{}) && expected != id || aliasLocked(id, nil, "") {
			registry.rejected = append(registry.rejected, fd)
			registry.poisoned = true
			o.pendingDrain = true
			registry.Unlock()
			emit(o.event("open", "quarantined", flags, fd, ErrUnsafe))
			return safeError(Admission, ErrUnsafe)
		}
		e.fd, e.id = fd, id
		registry.Unlock()
		emit(o.event("open", "validated", flags, fd, nil))
		return nil
	}
	return safeError(Admission, ErrUnsafe)
}

func (o *syncGuardOwner) verify(deadline time.Time, phase Phase) error {
	if err := o.verifyDirectory(deadline, phase); err != nil {
		return err
	}
	e := o.entry
	emit(o.event("verify", "before", 0, e.fd, nil))
	registry.Lock()
	var actual, named unix.Stat_t
	bad := registry.poisoned || registry.fatal || e.unknown || e.fd < 0 || e.id == (identity{}) || registry.syncGuards[e.key] != e
	if !bad {
		if cause := unix.Fstat(e.fd, &actual); cause != nil || fileIdentity(&actual) != e.id || actual.Mode&unix.S_IFMT != unix.S_IFREG {
			e.unknown = true
			registry.fatal, registry.poisoned = true, true
			bad = true
		} else {
			bad = unix.Fstatat(o.dirFD, e.key.name, &named, unix.AT_SYMLINK_NOFOLLOW) != nil || !privateFile(&actual) || !privateFile(&named) || fileIdentity(&named) != e.id
		}
	}
	registry.Unlock()
	if bad {
		return safeError(phase, ErrUnsafe)
	}
	if err := o.authorityAbsent(phase); err != nil {
		return err
	}
	if err := o.check(deadline, phase); err != nil {
		return err
	}
	emit(o.event("verify", "after", 0, e.fd, nil))
	return nil
}
func (g *SyncRunGuard) Verify() error {
	if g == nil || g.owner == nil {
		return safeError(VerifyPhase, ErrClosed)
	}
	o := g.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.usable || o.terminal || o.closeStarted {
		return safeError(VerifyPhase, ErrClosed)
	}
	return o.verify(time.Time{}, VerifyPhase)
}

// Close makes one bounded cleanup pass. A false result retains this exact owner;
// only an action known not to have been attempted can be retried. In particular,
// an unknown numeric descriptor is never closed a second time.
func (g *SyncRunGuard) Close() (bool, error) {
	if g == nil {
		return true, nil
	}
	if g.owner == nil {
		return false, safeError(ClosePhase, ErrClosed)
	}
	o := g.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.terminal {
		return true, o.closeErr
	}
	o.usable = false
	if !o.closeStarted {
		if err := sqlEvent(sqlTestEvent{Phase: "close-before", Operation: "sync-guard"}); err != nil {
			o.closeErr = joinCleanup(o.closeErr, safeError(ClosePhase, err))
			return false, o.closeErr
		}
		o.closeStarted = true
	}
	e := o.entry
	if e != nil && o.gateHeld {
		registry.Lock()
		fd, id, unknown := e.fd, e.id, e.unknown
		registry.Unlock()
		if fd >= 0 && !unknown {
			emit(o.event("close", "before", 0, fd, nil))
			registry.Lock()
			var st unix.Stat_t
			var closeErr error
			if cause := unix.Fstat(fd, &st); cause != nil || fileIdentity(&st) != id || st.Mode&unix.S_IFMT != unix.S_IFREG {
				closeErr = safeError(ClosePhase, ErrUnsafe)
				e.unknown = true
			} else {
				if cause := unix.Flock(fd, unix.LOCK_UN); cause != nil {
					closeErr = safeError(ClosePhase, cause)
				}
				if cause := unix.Close(fd); cause != nil {
					closeErr = joinCleanup(closeErr, safeError(ClosePhase, cause))
					e.unknown = true
				} else {
					e.fd, e.id = -1, identity{}
				}
			}
			if e.unknown {
				registry.fatal, registry.poisoned = true, true
			}
			registry.Unlock()
			o.closeErr = joinCleanup(o.closeErr, closeErr)
			emit(o.event("close", "after", 0, fd, closeErr))
		}
	}
	o.closeErr = joinCleanup(o.closeErr, drainRejected())
	if o.dirFD >= 0 {
		fd := o.dirFD
		o.dirFD = -1
		o.closeErr = joinCleanup(o.closeErr, o.closeDirectory(fd, o.dirID))
	}
	registry.Lock()
	if o.pendingDrain && len(registry.rejected) == 0 && !registry.fatal {
		o.pendingDrain = false
	}
	unresolved := o.pendingDrain || len(o.lostDirectories) != 0 || e != nil && o.gateHeld && (e.fd >= 0 || e.unknown)
	if !unresolved && e != nil {
		e.refs--
		if e.refs == 0 {
			delete(registry.syncGuards, e.key)
		}
	}
	registry.Unlock()
	if unresolved {
		if o.closeErr == nil {
			o.closeErr = safeError(ClosePhase, ErrUnsafe)
		}
		return false, o.closeErr
	}
	if e != nil && o.gateHeld {
		e.gate <- struct{}{}
		o.gateHeld = false
	}
	o.terminal = true
	emit(o.event("lease", "released", 0, -1, nil))
	if err := sqlEvent(sqlTestEvent{Phase: "close-after", Operation: "sync-guard"}); err != nil {
		o.closeErr = joinCleanup(o.closeErr, safeError(ClosePhase, err))
	}
	return true, o.closeErr
}
