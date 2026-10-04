//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	lib "modernc.org/sqlite/lib"
)

// The regression compiles against the unchanged adapter. It measures whether
// an already-admitted real native read needs another descriptor, not elapsed
// time or a fabricated counter. Process-wide FD pressure is child-only.
func TestSQLiteRootChainNativeReadDoesNotReopenAncestors(t *testing.T) {
	rcQAChild(t, "native-pressure")
}

func TestSQLiteRootChainFreshAncestorBindingsAndModes(t *testing.T) {
	for _, scenario := range []string{"ancestor-replacement", "ancestor-symlink", "leaf-mode", "ancestor-read-denial", "allowed-ancestor-mode"} {
		t.Run(scenario, func(t *testing.T) {
			o, dir, ancestor := rcQAOpen(t)
			if err := swQASelect(o.tx); err != nil {
				t.Fatal("real native baseline", err)
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
			moved := ancestor + "-moved"
			renamed, replacement, modeChanged := false, false, false
			restore := func() error {
				if replacement {
					if err := os.Remove(ancestor); err != nil {
						return err
					}
					replacement = false
				}
				if renamed {
					if err := os.Rename(moved, ancestor); err != nil {
						return err
					}
					renamed = false
				}
				if modeChanged {
					target := ancestor
					if scenario == "leaf-mode" {
						target = dir
					}
					if err := os.Chmod(target, 0700); err != nil {
						return err
					}
					modeChanged = false
				}
				return nil
			}
			t.Cleanup(func() {
				if err := restore(); err != nil {
					t.Error("restore owned namespace", err)
				}
			})
			switch scenario {
			case "ancestor-replacement", "ancestor-symlink":
				if err := os.Rename(ancestor, moved); err != nil {
					t.Fatal(err)
				}
				renamed = true
				if scenario == "ancestor-replacement" {
					err = os.Mkdir(ancestor, 0700)
				} else {
					err = os.Symlink(moved, ancestor)
				}
				if err != nil {
					t.Fatal(err)
				}
				replacement = true
			case "leaf-mode":
				if err := os.Chmod(dir, 0770); err != nil {
					t.Fatal(err)
				}
				modeChanged = true
			case "ancestor-read-denial":
				if err := os.Chmod(ancestor, 0300); err != nil {
					t.Fatal(err)
				}
				modeChanged = true
			case "allowed-ancestor-mode":
				if err := os.Chmod(ancestor, 0755); err != nil {
					t.Fatal(err)
				}
				modeChanged = true
			}
			// Actual original-walker result controls permission cases, including a
			// privileged test user. Identity/symlink cases must refuse independently.
			fd, _, walkErr := pinDirectory(dir)
			if fd >= 0 {
				if err := unix.Close(fd); err != nil {
					t.Fatal(err)
				}
			}
			wantRefusal := walkErr != nil
			if scenario == "allowed-ancestor-mode" && wantRefusal {
				t.Fatal("allowed mode baseline", walkErr)
			}
			if scenario != "ancestor-read-denial" && scenario != "allowed-ancestor-mode" && !wantRefusal {
				t.Fatal("negative namespace baseline unexpectedly admitted")
			}
			row, got := s.Step()
			run := lib.Xsqlite3_stmt_status(o.c.tls, s.ptr, lib.SQLITE_STMTSTATUS_RUN, 0)
			if err := restore(); err != nil {
				t.Fatal(err)
			}
			if wantRefusal {
				var e *Error
				if row || !errors.As(got, &e) || e.Category != Unsafe || run != 0 || !o.tx.readFailed {
					t.Fatal("fresh namespace was not refused before engine", row, got, run)
				}
			} else {
				if !row || got != nil || run != 1 {
					t.Fatal("unchanged valid native query rejected", row, got, run)
				}
				value, err := s.Int64(0)
				if err != nil || value != 7 {
					t.Fatal("native value", value, err)
				}
				if row, err := s.Step(); row || err != nil {
					t.Fatal("native DONE", row, err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := o.close(); err != nil {
				t.Fatal(err)
			}
			freshQAQuiet(t)
		})
	}
}

func TestSQLiteRootChainDirectoryOwnershipAndPartialFallback(t *testing.T) {
	for _, mode := range []string{"partial-pressure", "reused-directory"} {
		t.Run(mode, func(t *testing.T) { rcQAChild(t, mode) })
	}
}

func TestSQLiteRootChainRepeatedLeasesAndCanceledAdmissionReleaseFDs(t *testing.T) {
	// Warm native runtime descriptors before measuring fixture-owned directory FDs.
	warm, _, _ := rcQAOpen(t)
	if err := warm.close(); err != nil {
		t.Fatal(err)
	}
	base := qaDirectory(t)
	dirs := make([]string, 8)
	for i := range dirs {
		dirs[i] = filepath.Join(base, fmt.Sprint(i), "a", "b", "store")
		if err := os.MkdirAll(dirs[i], 0700); err != nil {
			t.Fatal(err)
		}
	}
	before := rcQAFDSet()
	var owners []*rootEntry
	release := func() error {
		var err error
		for i := len(owners) - 1; i >= 0; i-- {
			err = errors.Join(err, releaseRoot(owners[i]))
		}
		owners = nil
		return err
	}
	t.Cleanup(func() {
		if err := release(); err != nil {
			t.Error(err)
		}
	})
	for _, dir := range dirs {
		r, err := acquireRoot(context.Background(), dir, qaBasename, false, time.Now().Add(250*time.Millisecond))
		if r != nil {
			owners = append(owners, r)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := validateRoot(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	if after := rcQAFDSet(); fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatal("completed directory leases leaked FDs", before, after)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	acquired, released := 0, 0
	setHooksForTest(hooks{Observe: func(e event) {
		if e.Role == "root" && e.Op == "lease" {
			if e.Phase == "acquired" {
				acquired++
				cancel()
			}
			if e.Phase == "released" {
				released++
			}
		}
	}})
	t.Cleanup(func() { setHooksForTest(hooks{}) })
	c, err := Open(ctx, dirs[0], qaBasename, Options{Create: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	o := &swQAOwner{c: c}
	t.Cleanup(func() {
		if err := o.close(); err != nil {
			t.Error(err)
		}
	})
	setHooksForTest(hooks{})
	if cleanup := o.close(); cleanup != nil {
		t.Fatal(cleanup)
	}
	if !errors.Is(err, context.Canceled) || acquired != 1 || released != 1 {
		t.Fatal("canceled acquired lease was not conclusively released", err, acquired, released)
	}
	if after := rcQAFDSet(); fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatal("canceled acquisition leaked directory FDs", before, after)
	}
	freshQAQuiet(t)
}

func rcQAOpen(t *testing.T) (*swQAOwner, string, string) {
	t.Helper()
	ancestor := filepath.Join(qaDirectory(t), "ancestor")
	dir := filepath.Join(ancestor, "middle", "store")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	c, err := Open(context.Background(), dir, qaBasename, Options{Create: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	o := &swQAOwner{c: c}
	t.Cleanup(func() {
		if err := o.close(); err != nil {
			t.Error("checked root-chain fixture close", err)
		}
	})
	if err != nil {
		t.Fatal("native acquisition", errors.Join(err, o.close()))
	}
	o.tx, err = c.Begin(context.Background(), Read)
	if err != nil {
		t.Fatal("native BEGIN", errors.Join(err, o.close()))
	}
	return o, dir, ancestor
}

// Only this explicitly selected entry changes its process's descriptor limit.
// Default suite discovery is inert; there is no global environment mutation.
func TestSQLiteRootChainOwnedHelper(t *testing.T) {
	mode := os.Getenv("TEMPO_ROOT_CHAIN_QA_MODE")
	if mode == "" {
		return
	}
	switch mode {
	case "native-pressure":
		rcQANativePressure(t)
	case "partial-pressure":
		rcQAPartialPressure(t)
	case "reused-directory":
		rcQAReusedDirectory(t)
	default:
		t.Fatal("unknown root-chain helper mode")
	}
}

func rcQANativePressure(t *testing.T) {
	o, _, _ := rcQAOpen(t)
	if err := swQASelect(o.tx); err != nil {
		t.Fatal("native positive control", err)
	}
	// This is a real kernel capability control, not a producer hook. On a
	// kernel denying the effective-access primitive the contract falls back;
	// such a run cannot establish the optimized work result.
	if err := rcQAKernelAccess(o.c.root.fd); err != nil {
		t.Fatal("native effective-access prerequisite unavailable", err)
	}
	p := rcQAPressure(t)
	t.Cleanup(func() {
		if err := p.close(); err != nil {
			t.Error(err)
		}
	})
	if err := p.fill(); err != nil {
		t.Fatal(err)
	}
	probe, probeErr := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if probe >= 0 {
		_ = unix.Close(probe)
	}
	if !errors.Is(probeErr, unix.EMFILE) {
		t.Fatal("real directory-open pressure control did not reach EMFILE", probeErr)
	}
	got := swQASelect(o.tx)
	restoreErr := p.close()
	closeErr := o.close()
	if restoreErr != nil || closeErr != nil {
		t.Fatal("checked pressure/native cleanup", restoreErr, closeErr)
	}
	freshQAQuiet(t)
	if got != nil {
		t.Fatal("already-admitted native ROW/DONE reopened an ancestor under calibrated descriptor pressure", got)
	}
	t.Log("actual native ROW/DONE succeeded while a new directory open was EMFILE")
}

func rcQAPartialPressure(t *testing.T) {
	dir := filepath.Join(qaDirectory(t), "a", "b", "c", "d", "store")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	p := rcQAPressure(t)
	t.Cleanup(func() {
		if err := p.close(); err != nil {
			t.Error(err)
		}
	})
	if err := p.fill(); err != nil {
		t.Fatal(err)
	}
	if len(p.fds) < 3 {
		t.Fatal("insufficient owned pressure descriptors")
	}
	for i := 0; i < 3; i++ {
		n := len(p.fds) - 1
		fd := p.fds[n]
		p.fds = p.fds[:n]
		if err := unix.Close(fd); err != nil {
			t.Fatal(err)
		}
	}
	before := rcQAFDSet()
	r, err := acquireRoot(context.Background(), dir, qaBasename, false, time.Now().Add(250*time.Millisecond))
	// Register cleanup immediately, even on an unexpected retained owner.
	released := false
	release := func() error {
		if r == nil || released {
			return nil
		}
		released = true
		return releaseRoot(r)
	}
	t.Cleanup(func() {
		if err := release(); err != nil {
			t.Error(err)
		}
	})
	var validateErr error
	if err == nil {
		validateErr = validateRoot(r)
	}
	releaseErr := release()
	after := rcQAFDSet()
	pressureErr := p.close()
	if err != nil || validateErr != nil || releaseErr != nil || pressureErr != nil {
		t.Fatal("bounded partial-chain fallback", err, validateErr, releaseErr, pressureErr)
	}
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatal("partial chain leaked owned descriptors", before, after)
	}
	freshQAQuiet(t)
}

func rcQAReusedDirectory(t *testing.T) {
	dir := filepath.Join(qaDirectory(t), "a", "b", "store")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	before := rcQAFDSet()
	r, err := acquireRoot(context.Background(), dir, qaBasename, false, time.Now().Add(250*time.Millisecond))
	released := false
	release := func() error {
		if r == nil || released {
			return nil
		}
		released = true
		return releaseRoot(r)
	}
	t.Cleanup(func() {
		if err := release(); err != nil {
			t.Error("unexpected late release", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	var victim = -1
	for _, fd := range rcQAFDSet() {
		if fd == r.fd || rcQAContains(before, fd) {
			continue
		}
		var st unix.Stat_t
		if unix.Fstat(fd, &st) == nil && st.Mode&unix.S_IFMT == unix.S_IFDIR {
			victim = fd
			break
		}
	}
	if victim < 0 {
		t.Fatal("post-fix ownership control requires an independently retained ancestor")
	}
	if flags, err := unix.FcntlInt(uintptr(victim), unix.F_GETFD, 0); err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatal("retained directory CLOEXEC", flags, err)
	}
	// Known fixture-owned replacement; production may not close this reused FD.
	sentinel, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if sentinel >= 0 {
			if err := unix.Close(sentinel); err != nil {
				t.Error(err)
			}
		}
	})
	if err := unix.Close(victim); err != nil {
		t.Fatal(err)
	}
	if err := unix.Dup2(sentinel, victim); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if victim >= 0 {
			if err := unix.Close(victim); err != nil {
				t.Error(err)
			}
		}
	})
	got := validateRoot(r)
	releaseErr := release()
	var st unix.Stat_t
	if !errors.Is(got, ErrUnsafe) || releaseErr == nil || unix.Fstat(victim, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFCHR {
		t.Fatal("reused directory FD was accepted or closed", got, releaseErr)
	}
	stats := statsForTest()
	if !stats.Fatal || !stats.Poisoned || stats.Active != 0 || stats.Entries != 0 || stats.NativeActive != 0 || stats.Guards != 0 || stats.SyncGuards != 0 || stats.SyncEntries != 0 || stats.RejectedFDs != 1 {
		t.Fatal("inconclusive ownership did not remain fail-closed", stats)
	}
	// Only this child owns the replacement descriptor; explicitly close it once.
	if err := unix.Close(victim); err != nil {
		t.Fatal(err)
	}
	victim = -1
	if err := unix.Close(sentinel); err != nil {
		t.Fatal(err)
	}
	sentinel = -1
}
