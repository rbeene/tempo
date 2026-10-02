//go:build darwin || linux

package auth

import (
	"context"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

func AcquireMutationLock(ctx context.Context, path string) (*os.File, error) {
	if path == "" {
		var e error
		path, e = defaultMutationLockPath()
		if e != nil {
			return nil, e
		}
	}
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, issue("config", unchanged())
	}
	info, e := os.Lstat(dir)
	if e != nil || !privateLockInfo(info, true) {
		return nil, issue("config", unchanged())
	}
	root, e := os.OpenRoot(dir)
	if e != nil {
		return nil, issue("config", unchanged())
	}
	defer root.Close()
	f, e := root.OpenFile(filepath.Base(path), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if e != nil {
		return nil, issue("config", unchanged())
	}
	info, e = f.Stat()
	if e != nil || !privateLockInfo(info, false) {
		f.Close()
		return nil, issue("config", unchanged())
	}
	c, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	for {
		if e := c.Err(); e != nil {
			f.Close()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, issue("state_busy", unchanged())
		}
		e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if e == nil {
			return f, nil
		}
		if !errors.Is(e, syscall.EWOULDBLOCK) && !errors.Is(e, syscall.EAGAIN) {
			f.Close()
			return nil, issue("config", unchanged())
		}
		select {
		case <-c.Done():
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func privateLockInfo(i os.FileInfo, dir bool) bool {
	if i == nil || i.IsDir() != dir || (!dir && i.Mode().Perm()&0077 != 0) || (dir && i.Mode().Perm()&0022 != 0) || (!dir && !i.Mode().IsRegular()) {
		return false
	}
	s, ok := i.Sys().(*syscall.Stat_t)
	return ok && s.Uid == uint32(os.Getuid()) && (dir || s.Nlink == 1)
}

func defaultMutationLockPath() (string, error) {
	u, e := user.LookupId(strconv.Itoa(os.Getuid()))
	if e != nil {
		return "", issue("config", unchanged())
	}
	return filepath.Join(u.HomeDir, ".tempo", "auth.lock"), nil
}
func validateMutationOwner(f *os.File, path string) error {
	if f == nil {
		return issue("validation", unchanged())
	}
	if path == "" {
		var e error
		path, e = defaultMutationLockPath()
		if e != nil {
			return e
		}
	}
	expected, e := os.Lstat(path)
	if e != nil || !privateLockInfo(expected, false) {
		return issue("validation", unchanged())
	}
	actual, e := f.Stat()
	if e != nil || !os.SameFile(expected, actual) {
		return issue("validation", unchanged())
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return issue("state_busy", unchanged())
	}
	return nil
}
