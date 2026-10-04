//go:build darwin && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	lib "modernc.org/sqlite/lib"
	"os/exec"
	"testing"
	"time"
)

func rcQAKernelAccess(fd int) error {
	return unix.Faccessat(fd, ".", unix.R_OK|unix.X_OK, unix.AT_EACCESS)
}

func TestSQLiteRootChainFreshDirectoryACLIsEnforced(t *testing.T) {
	o, dir, ancestor := rcQAOpen(t)
	if err := swQASelect(o.tx); err != nil {
		t.Fatal("native ACL baseline", err)
	}
	s, err := o.tx.Prepare("SELECT 7")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	var before unix.Stat_t
	if err := unix.Stat(ancestor, &before); err != nil {
		t.Fatal(err)
	}
	// Local chmod's documented directory ACL syntax. The fixture owns only the
	// exact temporary deny-list rule; other ACL permissions stay intact.
	run := func(args ...string) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rcQACommand(t, exec.CommandContext(ctx, "/bin/chmod", args...))
	}
	added := false
	restore := func() {
		if added {
			added = false
			run("-a", "everyone deny list", ancestor)
		}
	}
	t.Cleanup(restore)
	added = true
	run("+a", "everyone deny list", ancestor)
	var after unix.Stat_t
	if err := unix.Stat(ancestor, &after); err != nil {
		t.Fatal(err)
	}
	if fileIdentity(&before) != fileIdentity(&after) || before.Mode != after.Mode || before.Uid != after.Uid || before.Gid != after.Gid {
		t.Fatal("ACL witness changed mode/identity instead of ACL only")
	}
	fd, _, walkErr := pinDirectory(dir)
	if fd >= 0 {
		if err := unix.Close(fd); err != nil {
			t.Fatal(err)
		}
	}
	if walkErr == nil {
		t.Fatal("real original walker did not enforce ACL denial; premise not established")
	}
	row, got := s.Step()
	runCount := lib.Xsqlite3_stmt_status(o.c.tls, s.ptr, lib.SQLITE_STMTSTATUS_RUN, 0)
	restore()
	var native *Error
	if row || !errors.As(got, &native) || native.Category != Unsafe || runCount != 0 || !o.tx.readFailed {
		t.Fatal("ACL-only change did not prevent native dispatch", row, got, runCount)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	freshQAQuiet(t)
}
