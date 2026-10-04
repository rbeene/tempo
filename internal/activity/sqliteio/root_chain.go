//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// This is a held descriptor chain, never a cached trust verdict. Every use
// rechecks each actual FD, parent/name binding and effective access. The leaf
// entry borrows rootEntry.fd; only ancestors have independent close ownership.
type rootDirectory struct {
	fd             int
	name           string
	id             identity
	mode, uid, gid uint32
	borrowed       bool
}

const maxRootChain = 64

func rootDirectoryStat(fd int, name string, st *unix.Stat_t) rootDirectory {
	return rootDirectory{fd: fd, name: name, id: fileIdentity(st), mode: uint32(st.Mode), uid: st.Uid, gid: st.Gid}
}

func rootChainFatal(fd int, cause error) error {
	registry.Lock()
	registry.fatal, registry.poisoned = true, true
	// Keep the unknown descriptor number in the existing never-drained fatal
	// quarantine. Do not lose its ownership record or retry a potentially reused FD.
	if fd >= 0 {
		registry.rejected = append(registry.rejected, fd)
	}
	registry.Unlock()
	return safeError(ClosePhase, cause)
}

func closeRootDirectory(d rootDirectory) error {
	var st unix.Stat_t
	if d.id == (identity{}) || unix.Fstat(d.fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || fileIdentity(&st) != d.id {
		// A missing/reused numeric FD must not close another owner's file or locks.
		return rootChainFatal(d.fd, ErrUnsafe)
	}
	if err := unix.Close(d.fd); err != nil {
		return rootChainFatal(d.fd, err)
	}
	return nil
}

func closeRootChain(chain []rootDirectory) (err error) {
	for i := len(chain) - 1; i >= 0; i-- {
		d := chain[i]
		chain[i].fd = -1
		if d.fd >= 0 && !d.borrowed {
			err = joinCleanup(err, closeRootDirectory(d))
		}
	}
	return err
}

func retainRootChain(ctx context.Context, r *rootEntry, deadline time.Time) (chain []rootDirectory, err error) {
	parts := strings.Split(strings.TrimPrefix(r.path, "/"), "/")
	if r.path == "/" {
		parts = nil
	}
	// Very deep paths retain the original walker and its constant FD footprint.
	if len(parts)+1 > maxRootChain {
		return nil, nil
	}
	keep := false
	defer func() {
		if !keep {
			cleanup := closeRootChain(chain)
			if cleanup != nil {
				if err == nil {
					err = cleanup
				} else {
					failure := safeError(Admission, err)
					failure.Cleanup = joinCleanup(failure.Cleanup, cleanup)
					err = failure
				}
			}
			chain = nil
		}
	}()
	open := func(parent int, name string) (int, error) {
		if e := admissionError(ctx, deadline); e != nil {
			return -1, e
		}
		fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil && !errors.Is(err, unix.EMFILE) && !errors.Is(err, unix.ENFILE) {
			return -1, ErrUnsafe
		}
		return fd, err
	}
	fd, cause := open(unix.AT_FDCWD, "/")
	if errors.Is(cause, unix.EMFILE) || errors.Is(cause, unix.ENFILE) {
		return nil, nil
	}
	if cause != nil {
		return nil, cause
	}
	chain = append(chain, rootDirectory{fd: fd, name: "/"})
	var st unix.Stat_t
	if cause = unix.Fstat(fd, &st); cause != nil {
		return chain, ErrUnsafe
	}
	chain[0] = rootDirectoryStat(fd, "/", &st)
	for _, name := range parts {
		next, e := open(fd, name)
		if errors.Is(e, unix.EMFILE) || errors.Is(e, unix.ENFILE) {
			return chain, nil
		}
		if e != nil {
			return chain, e
		}
		chain = append(chain, rootDirectory{fd: next, name: name})
		if e = unix.Fstat(next, &st); e != nil {
			return chain, ErrUnsafe
		}
		chain[len(chain)-1] = rootDirectoryStat(next, name, &st)
		fd = next
	}
	leaf := &chain[len(chain)-1]
	if !privateDirectory(&st) || leaf.id != r.id {
		return chain, ErrUnsafe
	}
	// Keep the original leased leaf descriptor; this duplicate is directory-only.
	if cause = closeRootDirectory(*leaf); cause != nil {
		leaf.fd = -1
		return chain, cause
	}
	leaf.fd, leaf.borrowed = r.fd, true
	if cause = admissionError(ctx, deadline); cause != nil {
		return chain, cause
	}
	keep = true
	return chain, nil
}

func validateRootPath(r *rootEntry) error {
	fallback := func() error {
		fd, id, err := pinDirectory(r.path)
		if err != nil {
			return ErrUnsafe
		}
		// Preserve the existing fallback's observation and identity requirement.
		closeErr := closeRootDirectory(rootDirectory{fd: fd, id: id})
		if id != r.id {
			return joinCleanup(ErrUnsafe, closeErr)
		}
		return closeErr
	}
	if len(r.chain) == 0 {
		return fallback()
	}
	useWalker := false
	for i, d := range r.chain {
		var held, named unix.Stat_t
		if unix.Fstat(d.fd, &held) != nil || held.Mode&unix.S_IFMT != unix.S_IFDIR || fileIdentity(&held) != d.id {
			return ErrUnsafe
		}
		parent, name := unix.AT_FDCWD, "/"
		if i > 0 {
			parent, name = r.chain[i-1].fd, d.name
		}
		if unix.Fstatat(parent, name, &named, unix.AT_SYMLINK_NOFOLLOW) != nil || named.Mode&unix.S_IFMT != unix.S_IFDIR || fileIdentity(&named) != d.id {
			return ErrUnsafe
		}
		if uint32(held.Mode) != d.mode || held.Uid != d.uid || held.Gid != d.gid || uint32(named.Mode) != d.mode || named.Uid != d.uid || named.Gid != d.gid {
			// An allowed permission/owner change gets the original admission rule,
			// never acceptance based only on an earlier permission observation.
			useWalker = true
		}
		if err := rootDirectoryAccess(d.fd); err != nil {
			if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EPERM) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) {
				useWalker = true
			} else {
				return ErrUnsafe
			}
		}
	}
	if r.chain[len(r.chain)-1].fd != r.fd || r.chain[len(r.chain)-1].id != r.id {
		return ErrUnsafe
	}
	var leaf unix.Stat_t
	if unix.Fstat(r.fd, &leaf) != nil || !privateDirectory(&leaf) || fileIdentity(&leaf) != r.id {
		return ErrUnsafe
	}
	if useWalker {
		return fallback()
	}
	return nil
}
