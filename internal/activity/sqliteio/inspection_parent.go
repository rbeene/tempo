//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// inspectLinkParent owns only temporary directory descriptors. Unlike ordinary
// pinDirectory, an actual ENOENT can be qualified as an absent observation.
// Every close is checked before returning an identity or absence to the caller.
func inspectLinkParent(ctx context.Context, directory string, deadline time.Time) (id identity, absent bool, err error) {
	if err = admissionError(ctx, deadline); err != nil {
		return identity{}, false, safeError(Admission, err)
	}
	if !filepath.IsAbs(directory) || strings.ContainsRune(directory, 0) {
		return identity{}, false, safeError(Admission, ErrUnsafe)
	}
	for _, part := range strings.Split(directory, "/") {
		if part == ".." {
			return identity{}, false, safeError(Admission, ErrUnsafe)
		}
	}
	parts := strings.Split(strings.TrimPrefix(filepath.Clean(directory), "/"), "/")
	fd, cause := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if cause != nil {
		return identity{}, false, safeError(Admission, cause)
	}
	defer func() {
		// A failed OS close has unknown disposition. closeOwnedFile records
		// the existing fatal state; its numeric descriptor is never retried.
		err = combineClose(err, closeOwnedFile(fd))
		if err == nil {
			if cause := admissionError(ctx, deadline); cause != nil {
				err = safeError(Admission, cause)
			}
		}
		if err != nil {
			id, absent = identity{}, false
		}
	}()
	for i, part := range parts {
		if part == "" {
			continue
		}
		if cause = admissionError(ctx, deadline); cause != nil {
			return identity{}, false, safeError(Admission, cause)
		}
		next, openErr := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if errors.Is(openErr, unix.ENOENT) {
			if err = recheckLinkMissing(ctx, fd, parts[:i], part, deadline); err != nil {
				return identity{}, false, err
			}
			return identity{}, true, nil
		}
		if openErr != nil {
			return identity{}, false, safeError(Admission, openErr)
		}
		previous := fd
		fd = next
		if err = closeOwnedFile(previous); err != nil {
			return identity{}, false, err
		}
	}
	var st unix.Stat_t
	if cause = unix.Fstat(fd, &st); cause != nil {
		return identity{}, false, safeError(Admission, cause)
	}
	if !privateDirectory(&st) {
		return identity{}, false, safeError(Admission, ErrUnsafe)
	}
	return fileIdentity(&st), false, nil
}

// The held parent survives the independent nofollow rewalk. Only its dev/inode
// identity is compared; existing ancestors do not need final-directory 0700
// permissions. Both missing-component observations use the held parent.
func recheckLinkMissing(ctx context.Context, parent int, prefix []string, name string, deadline time.Time) (err error) {
	if cause := admissionError(ctx, deadline); cause != nil {
		return safeError(Admission, cause)
	}
	var held unix.Stat_t
	if cause := unix.Fstat(parent, &held); cause != nil {
		return safeError(Admission, cause)
	}
	if held.Mode&unix.S_IFMT != unix.S_IFDIR {
		return safeError(Admission, ErrUnsafe)
	}
	missing := func() error {
		var st unix.Stat_t
		cause := unix.Fstatat(parent, name, &st, unix.AT_SYMLINK_NOFOLLOW)
		if errors.Is(cause, unix.ENOENT) {
			return nil
		}
		if cause == nil {
			return safeError(Admission, ErrUnsafe)
		}
		return safeError(Admission, cause)
	}
	if err = missing(); err != nil {
		return err
	}
	fd, cause := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if cause != nil {
		return safeError(Admission, cause)
	}
	defer func() { err = combineClose(err, closeOwnedFile(fd)) }()
	for _, part := range prefix {
		if part == "" {
			continue
		}
		if cause = admissionError(ctx, deadline); cause != nil {
			return safeError(Admission, cause)
		}
		next, openErr := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if openErr != nil {
			return safeError(Admission, openErr)
		}
		previous := fd
		fd = next
		if err = closeOwnedFile(previous); err != nil {
			return err
		}
	}
	var observed unix.Stat_t
	if cause = unix.Fstat(fd, &observed); cause != nil {
		return safeError(Admission, cause)
	}
	if observed.Mode&unix.S_IFMT != unix.S_IFDIR || fileIdentity(&observed) != fileIdentity(&held) {
		return safeError(Admission, ErrUnsafe)
	}
	if err = missing(); err != nil {
		return err
	}
	if cause = admissionError(ctx, deadline); cause != nil {
		return safeError(Admission, cause)
	}
	return nil
}
