//go:build (darwin || linux) && (amd64 || arm64)

// Package sqliteio owns the inactive direct SQLite adapter and pinned Unix VFS.
package sqliteio

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

var (
	ErrUnsafe = errors.New("unsafe pinned SQLite namespace")
	ErrBusy   = errors.New("pinned SQLite ownership busy")
	ErrClosed = errors.New("pinned SQLite connection closed")
)

const namespacePrefix = "/__tempo_sqlite_v1/"

type identity struct{ dev, ino uint64 }

func fileIdentity(s *unix.Stat_t) identity { return identity{uint64(s.Dev), uint64(s.Ino)} }

type rootEntry struct {
	key          string
	databaseName string
	path         string
	fd           int
	id           identity
	main         identity
	known        map[string]identity
	gate         chan struct{}
	refs         int
	active       bool
	poisoned     bool
	create       bool
}

var registry = struct {
	sync.Mutex
	roots    map[string]*rootEntry
	active   int
	rejected []int
	poisoned bool
	fatal    bool
}{roots: make(map[string]*rootEntry)}

// Private observations are nil in production and installed only by package tests.
// hooks run synchronously; use another goroutine/process for coordination.
type event struct {
	Namespace string
	Role      string
	Op        string
	Phase     string
	Flags     int
	FD        int
	Errno     int
}
type hooks struct{ Observe func(event) }

var observations struct {
	sync.RWMutex
	hooks hooks
}

func setHooksForTest(h hooks) {
	observations.Lock()
	observations.hooks = h
	observations.Unlock()
}
func emit(e event) {
	observations.RLock()
	fn := observations.hooks.Observe
	observations.RUnlock()
	if fn != nil {
		fn(e)
	}
}

type registryStats struct {
	Active, Entries, RejectedFDs int
	Poisoned, Fatal              bool
}

func statsForTest() registryStats {
	registry.Lock()
	defer registry.Unlock()
	return registryStats{registry.active, len(registry.roots), len(registry.rejected), registry.poisoned, registry.fatal}
}

func privateDirectory(s *unix.Stat_t) bool {
	return s.Mode&unix.S_IFMT == unix.S_IFDIR && s.Mode&0777 == 0700 && int(s.Uid) == unix.Geteuid()
}
func privateFile(s *unix.Stat_t) bool {
	return s.Mode&unix.S_IFMT == unix.S_IFREG && s.Mode&0777 == 0600 && int(s.Uid) == unix.Geteuid() && s.Nlink == 1
}

// pinDirectory walks via directory FDs and never creates a path. No main DB
// descriptor is opened by this helper, including during identity rechecks.
func pinDirectory(path string) (int, identity, error) {
	if !filepath.IsAbs(path) || strings.ContainsRune(path, 0) {
		return -1, identity{}, ErrUnsafe
	}
	for _, p := range strings.Split(path, "/") {
		if p == ".." {
			return -1, identity{}, ErrUnsafe
		}
	}
	path = filepath.Clean(path)
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, identity{}, err
	}
	for _, p := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if p == "" {
			continue
		}
		next, e := unix.Openat(fd, p, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(fd)
		if e != nil {
			return -1, identity{}, ErrUnsafe
		}
		fd = next
	}
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil || !privateDirectory(&st) {
		_ = unix.Close(fd)
		return -1, identity{}, ErrUnsafe
	}
	return fd, fileIdentity(&st), nil
}

func acquireRoot(ctx context.Context, path, basename string, create bool, deadline time.Time) (*rootEntry, error) {
	fd, id, err := pinDirectory(path)
	if err != nil {
		return nil, err
	}
	if err = admissionError(ctx, deadline); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	key := fmt.Sprintf("%s%x-%x-%x", namespacePrefix, id.dev, id.ino, sha256.Sum256([]byte(basename)))
	registry.Lock()
	if registry.poisoned || registry.fatal {
		registry.Unlock()
		_ = unix.Close(fd)
		return nil, ErrUnsafe
	}
	r := registry.roots[key]
	if r != nil && r.databaseName != basename {
		registry.Unlock()
		_ = unix.Close(fd)
		return nil, ErrUnsafe
	}
	if r == nil {
		r = &rootEntry{key: key, databaseName: basename, id: id, fd: -1, gate: make(chan struct{}, 1)}
		r.gate <- struct{}{}
		registry.roots[key] = r
	}
	r.refs++
	registry.Unlock()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-timer.C:
		err = ErrBusy
	case <-r.gate:
		registry.Lock()
		if err = admissionError(ctx, deadline); err != nil {
			// A ready lease and canceled context can both win select. Do not
			// activate SQLite ownership for that canceled waiter.
		} else if registry.poisoned || registry.fatal {
			err = ErrUnsafe
		} else {
			r.fd, r.path, r.create = fd, filepath.Clean(path), create
			r.main, r.known = identity{}, make(map[string]identity)
			r.active, r.poisoned = true, false
			registry.active++
		}
		registry.Unlock()
		if err == nil {
			emit(event{Namespace: key, Role: "root", Op: "lease", Phase: "acquired", FD: fd})
			return r, nil
		}
		r.gate <- struct{}{}
	}
	_ = unix.Close(fd)
	registry.Lock()
	r.refs--
	if r.refs == 0 {
		delete(registry.roots, key)
	}
	registry.Unlock()
	return nil, err
}

// releaseRoot is called only after conclusive sqlite3_close success, including
// all SQLite cleanup callbacks. It never frees a BUSY/zombie connection.
func releaseRoot(r *rootEntry) error {
	registry.Lock()
	r.active = false
	registry.active--
	fd := r.fd
	r.fd = -1
	r.refs--
	if r.refs == 0 {
		delete(registry.roots, r.key)
	}
	last := registry.active == 0 && len(registry.rejected) != 0
	registry.Unlock()
	if last {
		emit(event{Op: "quarantine", Phase: "before-drain", FD: -1})
	}
	registry.Lock()
	var first error
	// New admissions remain barred by registry.poisoned until this drain ends.
	if registry.active == 0 && len(registry.rejected) != 0 {
		for _, rejected := range registry.rejected {
			if err := unix.Close(rejected); err != nil && first == nil {
				first = err
			}
		}
		registry.rejected = nil
		if first != nil {
			registry.fatal = true
		} else {
			registry.poisoned = false
		}
	}
	registry.Unlock()
	if err := unix.Close(fd); err != nil && first == nil {
		first = err
	}
	r.gate <- struct{}{}
	if last {
		emit(event{Op: "quarantine", Phase: "drained", FD: -1})
	}
	emit(event{Namespace: r.key, Role: "root", Op: "lease", Phase: "released", FD: fd})
	return first
}

func lookupPath(path string) (*rootEntry, string, error) {
	pos := strings.LastIndexByte(path, '/')
	if pos < 0 {
		return nil, "", ErrUnsafe
	}
	key, name := path[:pos], path[pos+1:]
	registry.Lock()
	defer registry.Unlock()
	r := registry.roots[key]
	if r == nil || !r.active || r.fd < 0 || (name != r.databaseName && name != r.databaseName+"-wal" && name != r.databaseName+"-shm" && name != r.databaseName+"-journal") {
		return nil, "", ErrUnsafe
	}
	return r, name, nil
}
func role(r *rootEntry, name string) string {
	if name == r.databaseName {
		return "main"
	}
	return strings.TrimPrefix(name, r.databaseName+"-")
}
func pathEvent(r *rootEntry, name, op, phase string, flags, fd int, err error) event {
	e := event{Namespace: r.key, Role: role(r, name), Op: op, Phase: phase, Flags: flags, FD: fd}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		e.Errno = int(errno)
	} else if err != nil {
		e.Errno = int(unix.EIO)
	}
	return e
}

func validateRoot(r *rootEntry) error {
	fd, id, err := pinDirectory(r.path)
	if err != nil {
		return ErrUnsafe
	}
	_ = unix.Close(fd)
	if id != r.id {
		return ErrUnsafe
	}
	registry.Lock()
	poisoned := r.poisoned || registry.poisoned || registry.fatal
	known := make(map[string]identity, len(r.known))
	for name, id := range r.known {
		known[name] = id
	}
	registry.Unlock()
	if poisoned {
		return ErrUnsafe
	}
	// An unlinked/replaced WAL can hold a successful commit that no fresh opener
	// can recover. Validate every known still-live role before acknowledging.
	// Legitimate SQLite deletion removes the role through the unlink hook.
	for name, expected := range known {
		var st unix.Stat_t
		if unix.Fstatat(r.fd, name, &st, unix.AT_SYMLINK_NOFOLLOW) != nil || !privateFile(&st) || fileIdentity(&st) != expected {
			return ErrUnsafe
		}
	}
	return nil
}

// preflight uses no duplicate DB descriptor. Call the race hook after this.
func preflight(r *rootEntry, name string, allowMissing bool) error {
	registry.Lock()
	bad := r.poisoned || registry.poisoned || registry.fatal
	expected := r.known[name]
	registry.Unlock()
	if bad {
		return ErrUnsafe
	}
	var st unix.Stat_t
	err := unix.Fstatat(r.fd, name, &st, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) && allowMissing {
		return nil
	}
	if err != nil {
		return err
	}
	if !privateFile(&st) || expected != (identity{}) && fileIdentity(&st) != expected {
		return ErrUnsafe
	}
	registry.Lock()
	defer registry.Unlock()
	if aliasLocked(fileIdentity(&st), r, name) {
		return ErrUnsafe
	}
	// Capture identity at preflight so replacement before the actual open is
	// detected even when this connection has not previously opened that role.
	if r.known[name] == (identity{}) {
		r.known[name] = fileIdentity(&st)
	}
	return nil
}
