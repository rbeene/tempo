//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	lib "modernc.org/sqlite/lib"
)

// The new API is an explicit compile prerequisite. The original actual hard64
// public-operation failure supplies behavioral RED; no adapter fakes this API.
func TestSQLiteCaptureWriterHandoffAbsentAndPristine(t *testing.T) {
	for _, profile := range []string{"absent", "absent-parent", "zero", "single", "occupied"} {
		t.Run(profile, func(t *testing.T) {
			dir := qaDirectory(t)
			base := dir
			want := LinkAbsent
			if profile == "absent-parent" {
				dir = filepath.Join(dir, "absent", "child")
			}
			if profile == "zero" || profile == "single" {
				freshQAProfile(t, dir, profile)
				want = LinkPristine
			}
			var occupied string
			var saved freshQAOccupiedPath
			if profile == "occupied" {
				occupied = linkQAOccupied(t, dir, "file")
				saved = freshQAOccupiedSnapshot(t, occupied)
			}
			before := freshQAImage(t, base, freshQAName)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL})
			c, kind, err := whQAOpen(t, context.Background(), dir, time.Now().Add(250*time.Millisecond))
			if occupied != "" {
				if err == nil || !errors.Is(err, os.ErrExist) || kind != 0 {
					t.Fatal("occupied authority precedence", err)
				}
				linkQASameOccupied(t, occupied, saved)
			} else if err != nil || kind != want {
				t.Fatal("absent/pristine classification", kind, err)
			}
			if c != nil || trace.sqlCount("handoff-before-writer", "capture-write-handoff") != 0 {
				t.Fatal("non-WAL result acquired a writer")
			}
			linkQANoCreating(t, trace, true)
			freshQASameImage(t, base, freshQAName, before)
			freshQAQuiet(t)
		})
	}
}

func TestSQLiteCaptureWriterHandoffOverlapAndSequentialControl(t *testing.T) {
	for _, route := range []string{"handoff", "legacy-sequential"} {
		t.Run(route, func(t *testing.T) {
			dir := whQASeed(t)
			before := freshQAImage(t, dir, freshQAName)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			var observed []registryStats
			qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
				trace.observeSQL(e)
				if e.Operation == "capture-write-handoff" {
					observed = append(observed, statsForTest())
				}
			}})
			deadline := time.Now().Add(250 * time.Millisecond)
			var c *Conn
			if route == "handoff" {
				var kind LinkInspection
				var err error
				c, kind, err = whQAOpen(t, context.Background(), dir, deadline)
				if err != nil || kind != LinkWAL || c == nil || c.readOnly || c.used || c.poisoned || c.acquireDeadline != deadline {
					t.Fatal("unused writer handoff", kind, err)
				}
				if len(observed) != 4 {
					t.Fatal("handoff phases missing", len(observed))
				}
				for i, n := range []int{1, 2, 2, 1} {
					if s := observed[i]; s.Active != 1 || s.Entries != 1 || s.NativeActive != n || s.Poisoned || s.Fatal {
						t.Fatal("root/native ownership changed during overlap", i, s)
					}
				}
				if trace.count("root", "lease", "released") != 0 || trace.count("shm", "open", "validated") != 1 {
					t.Fatal("handoff released root or reopened native SHM")
				}
				if c.handoffProbe != nil && (!c.handoffProbe.closed || c.handoffProbe.db != 0) {
					t.Fatal("read prerequisite survived into usable writer")
				}
			} else {
				probe, kind, err := InspectForCaptureWrite(context.Background(), dir, freshQAState, freshQAName, deadline)
				whQAOwn(t, probe)
				if err != nil || kind != LinkWAL || probe == nil || !probe.readOnly || !probe.used {
					t.Fatal("legacy cleanup-only probe changed", err)
				}
				whQAClose(t, probe, false)
				c, err = Open(context.Background(), dir, freshQAName, Options{AcquireDeadline: deadline})
				whQAOwn(t, c)
				if err != nil || c == nil || trace.count("shm", "open", "validated") != 2 {
					t.Fatal("sequential native SHM-open control", err)
				}
			}
			if s := statsForTest(); s.Active != 1 || s.NativeActive != 1 {
				t.Fatal("writer Begin would overlap prerequisite", s)
			}
			tx := whQABegin(t, c, Write)
			if err := tx.CheckAuthorityAbsent(freshQAState); err != nil {
				t.Fatal("first writer authority observation", err)
			}
			info, err := tx.PageInfo()
			if err != nil || info.PageSize != PageSize || info.MaxPages != MaxPages || info.JournalLimitBytes != JournalLimitBytes {
				t.Fatal("writer omitted full page policy", info, err)
			}
			linkQARawReceipt(t, tx, 20)
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			whQAClose(t, c, false)
			wantReleases := 1
			if route == "legacy-sequential" {
				wantReleases = 2
			}
			if trace.count("root", "lease", "released") != wantReleases || trace.sqlCount("checkpoint-before-native", "checkpoint") != 0 {
				t.Fatal("unexpected lease release or checkpoint")
			}
			freshQAQuiet(t)
			after := freshQAImage(t, dir, freshQAName)
			if !bytes.Equal(before[""], after[""]) || !bytes.Equal(before["-wal"], after["-wal"]) {
				t.Fatal("unused/read-only rollback changed main or WAL")
			}
			setHooksForTest(hooks{})
			setSQLHooksForTest(sqlTestHooks{})
			linkQAColdReceipt(t, dir, 20)
			linkQANoAuthority(t, dir)
		})
	}
}

func TestSQLiteCaptureWriterHandoffBoundaryFailures(t *testing.T) {
	for _, phase := range []string{"handoff-before-writer", "handoff-writer-opened", "handoff-writer-ready", "handoff-probe-closed", "read-close-before", "read-close-after", "partial-native-open"} {
		t.Run(phase, func(t *testing.T) {
			dir := whQASeed(t)
			trace := &freshQATrace{}
			reached, keep := false, phase == "read-close-before" || phase == "partial-native-open"
			mainOpens := 0
			var mutationErr error
			qaFSHooks(t, hooks{Observe: func(e event) {
				trace.observeFS(e)
				if phase == "partial-native-open" && e.Role == "main" && e.Op == "open" && e.Phase == "before" {
					mainOpens++
					if mainOpens == 2 {
						reached = true
						mutationErr = os.Chmod(filepath.Join(dir, freshQAName), 0644)
					}
				}
			}})
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL, Fault: func(e sqlTestEvent) error {
				if keep && e.Phase == "close-before" && e.Operation == "close" {
					if phase == "read-close-before" {
						reached = true
					}
					return unix.EIO
				}
				if !reached && (e.Operation == "capture-write-handoff" && e.Phase == phase || phase == "read-close-after" && e.Operation == "close" && e.Phase == "close-after") {
					reached = true
					return syscall.ENOSPC
				}
				return nil
			}})
			c, kind, err := whQAOpen(t, context.Background(), dir, time.Now().Add(250*time.Millisecond))
			// Restore permissions even if a preceding assertion fails. These are
			// owned test bytes; no production cleanup is allowed to repair them.
			t.Cleanup(func() { _ = os.Chmod(filepath.Join(dir, freshQAName), 0600) })
			if mutationErr != nil || !reached || err == nil || kind != 0 {
				t.Fatal("real selected boundary not refused", phase, err, mutationErr)
			}
			qaSafeError(t, err, dir)
			if !keep && !errors.Is(err, syscall.ENOSPC) {
				t.Fatal("bare injected cause lost", err)
			}
			if keep {
				if c == nil || c.db == 0 || c.handoffProbe == nil || c.handoffProbe.db == 0 || c.db == c.handoffProbe.db || c.root != c.handoffProbe.root || !c.handoffProbe.borrowedRoot {
					t.Fatal("dual partial native owners not retained", err)
				}
				if s := statsForTest(); s.Active != 1 || s.NativeActive != 2 || trace.count("root", "lease", "released") != 0 {
					t.Fatal("dual refusal released shared root", s)
				}
				if phase == "partial-native-open" && (whQAErrorCount(err, IO, lib.SQLITE_CANTOPEN) == 0 || c.authMode != 0 || c.cancelFlag != 0) {
					t.Fatal("partial actual sqlite3_open_v2 failure not reached", err)
				}
				whQAOnlyCleanup(t, c)
			} else if c != nil {
				t.Fatal("conclusive fault cleanup retained caller owner")
			}
			setSQLHooksForTest(sqlTestHooks{})
			setHooksForTest(hooks{Observe: trace.observeFS})
			if err := os.Chmod(filepath.Join(dir, freshQAName), 0600); err != nil {
				t.Fatal(err)
			}
			whQAClose(t, c, keep)
			if trace.count("root", "lease", "released") != 1 {
				t.Fatal("failure released shared root more than once")
			}
			freshQAQuiet(t)
			setHooksForTest(hooks{})
			linkQAColdReceipt(t, dir, 20)
		})
	}
}

func TestSQLiteCaptureWriterHandoffRealDualBusyCleanup(t *testing.T) {
	dir := whQASeed(t)
	qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
		if e.Phase == "close-before" && e.Operation == "close" {
			return unix.EIO
		}
		return nil
	}})
	c, kind, err := whQAOpen(t, context.Background(), dir, time.Now().Add(250*time.Millisecond))
	if err == nil || kind != 0 || c == nil || c.handoffProbe == nil || c.db == 0 || c.handoffProbe.db == 0 || c.db == c.handoffProbe.db {
		t.Fatal("retained two-handle native fixture", err)
	}
	probe := c.handoffProbe
	if lib.Xsqlite3_get_autocommit(c.tls, c.db) != 1 || lib.Xsqlite3_get_autocommit(probe.tls, probe.db) != 1 || lib.Xsqlite3_next_stmt(c.tls, c.db, 0) != 0 || lib.Xsqlite3_next_stmt(probe.tls, probe.db, 0) != 0 {
		t.Fatal("handoff left a transaction or setup statement live")
	}
	whQAOnlyCleanup(t, c)
	// These actual prepared statements force sqlite3_close BUSY on the returned
	// cleanup aggregate. This is not an initial inspection-close BUSY claim.
	releaseWriter, releaseProbe := whQAHoldRaw(t, c), whQAHoldRaw(t, probe)
	trace := &freshQATrace{}
	qaFSHooks(t, hooks{Observe: trace.observeFS})
	setSQLHooksForTest(sqlTestHooks{})
	terminal, busy := c.CloseChecked(context.Background())
	if terminal || whQAErrorCount(busy, Busy, lib.SQLITE_BUSY) < 2 || whQAErrorCount(busy, IO, 0) == 0 || statsForTest().NativeActive != 2 || trace.count("main", "close", "before") != 2 || trace.count("root", "lease", "released") != 0 {
		t.Fatal("both real BUSY owners or original cleanup failure lost", terminal, busy)
	}
	qaSafeError(t, busy, dir, "SELECT 1")
	releaseProbe()
	terminal, busy = c.CloseChecked(context.Background())
	if terminal || !probe.closed || probe.db != 0 || c.db == 0 || statsForTest().NativeActive != 1 || trace.count("root", "lease", "released") != 0 {
		t.Fatal("one surviving native owner lost shared root", terminal, busy)
	}
	whQANativeGone(t, probe)
	whQAOnlyCleanup(t, c)
	releaseWriter()
	saved := whQAClose(t, c, true)
	if whQAErrorCount(saved, Busy, lib.SQLITE_BUSY) < 2 || whQAErrorCount(saved, IO, 0) == 0 || trace.count("root", "lease", "released") != 1 || trace.count("main", "close", "before") != 5 {
		t.Fatal("terminal cleanup erased earlier BUSY/error or repeated native close", saved)
	}
	freshQAQuiet(t)
	setHooksForTest(hooks{})
	linkQAColdReceipt(t, dir, 20)
}

func TestSQLiteCaptureWriterHandoffOriginalDeadlineAndCancellation(t *testing.T) {
	for _, mode := range []string{"already-canceled", "expired", "before-writer-expiry", "writer-ready-expiry", "writer-setup-cancel"} {
		t.Run(mode, func(t *testing.T) {
			dir := whQASeed(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			deadline := time.Now().Add(250 * time.Millisecond)
			if mode == "already-canceled" {
				cancel()
			}
			if mode == "expired" {
				deadline = time.Now().Add(-time.Second)
			}
			var interrupts atomic.Int32
			interrupted := make(chan struct{}, 2)
			trace := &freshQATrace{}
			reached := false
			var hookErr error
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
				trace.observeSQL(e)
				if e.Phase == "interrupt-after" {
					interrupts.Add(1)
					select {
					case interrupted <- struct{}{}:
					default:
					}
					return
				}
				if reached {
					return
				}
				if mode == "writer-setup-cancel" && e.Operation == "setup" && e.Phase == "control-before-native" && statsForTest().NativeActive == 2 {
					reached = true
					cancel()
					timer := time.NewTimer(time.Second)
					defer timer.Stop()
					select {
					case <-interrupted:
					case <-timer.C:
						hookErr = errors.New("active writer cancellation watcher not reached")
					}
				}
				if e.Operation == "capture-write-handoff" && (mode == "before-writer-expiry" && e.Phase == "handoff-before-writer" || mode == "writer-ready-expiry" && e.Phase == "handoff-writer-ready") {
					reached = true
					timer := time.NewTimer(time.Until(deadline) + time.Millisecond)
					defer timer.Stop()
					<-timer.C
				}
			}})
			c, kind, err := whQAOpen(t, ctx, dir, deadline)
			if hookErr != nil || err == nil || kind != 0 || c != nil {
				t.Fatal("original cancellation/deadline failed conclusive cleanup", mode, err, hookErr)
			}
			if mode == "already-canceled" || mode == "writer-setup-cancel" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal("caller cancellation precedence", err)
				}
			} else if !errors.Is(err, ErrBusy) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("original absolute deadline was renewed", err)
			}
			if mode == "already-canceled" || mode == "expired" {
				if reached || trace.count("main", "open", "validated") != 0 {
					t.Fatal("pre-admission refusal entered native open")
				}
			} else if !reached || trace.count("root", "lease", "released") != 1 {
				t.Fatal("reached cancellation/expiry did not release one root")
			}
			if mode == "writer-setup-cancel" && interrupts.Load() != 1 {
				t.Fatal("RO watcher overlapped canceled writer or writer interrupt missing", interrupts.Load())
			}
			freshQAQuiet(t)
			setHooksForTest(hooks{})
			setSQLHooksForTest(sqlTestHooks{})
			linkQAColdReceipt(t, dir, 20)
		})
	}
}

// F1 regression: a terminal or never-opened writer is not another cleanup
// attempt. TLS-only partial native ownership is explicitly not modeled as zero.
func TestSQLiteCaptureWriterHandoffTerminalWriterDoesNotReclose(t *testing.T) {
	for _, mode := range []string{"writer-closed-probe-busy", "writer-never-opened"} {
		t.Run(mode, func(t *testing.T) {
			dir := whQASeed(t)
			qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
				if mode == "writer-never-opened" && e.Operation == "capture-write-handoff" && e.Phase == "handoff-before-writer" {
					return ErrUnsafe
				}
				if e.Operation == "close" && e.Phase == "close-before" {
					return unix.EIO
				}
				return nil
			}})
			c, kind, err := whQAOpen(t, context.Background(), dir, time.Now().Add(250*time.Millisecond))
			if c == nil || c.handoffProbe == nil || c.handoffProbe.db == 0 || err == nil || kind != 0 {
				t.Fatal("retained predecessor fixture", err)
			}
			probe := c.handoffProbe
			if mode == "writer-closed-probe-busy" {
				release := whQAHoldRaw(t, probe)
				setSQLHooksForTest(sqlTestHooks{})
				terminal, err := c.CloseChecked(context.Background())
				if terminal || whQAErrorCount(err, Busy, lib.SQLITE_BUSY) == 0 || statsForTest().NativeActive != 1 || probe.db == 0 {
					t.Fatal("writer did not close beside real predecessor BUSY", terminal, err)
				}
				release()
			}
			if c.db != 0 || c.nativeCounted || c.tls != nil || c.authMode != 0 || c.cancelFlag != 0 || statsForTest().NativeActive != 1 {
				t.Fatal("writer slot is not conclusively terminal")
			}
			closeCalls := 0
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			setSQLHooksForTest(sqlTestHooks{Fault: func(e sqlTestEvent) error {
				if e.Operation == "close" && e.Phase == "close-before" {
					closeCalls++
					if closeCalls > 1 {
						return syscall.ENOSPC
					}
				}
				return nil
			}})
			saved := whQAClose(t, c, true)
			if closeCalls != 1 || trace.count("main", "close", "before") != 1 || trace.count("root", "lease", "released") != 1 || errors.Is(saved, syscall.ENOSPC) || whQAErrorCount(saved, IO, 0) == 0 {
				t.Fatal("terminal writer was reclosed or original cleanup error discarded", closeCalls, saved)
			}
			whQANativeGone(t, probe)
			freshQAQuiet(t)
		})
	}
}

func TestSQLiteCaptureWriterHandoffFreshNamespaceChecks(t *testing.T) {
	for _, change := range []string{"shm-replace", "shm-unlink", "shm-mode", "wal-replace", "main-replace", "root-replace", "ancestor-replace", "root-mode", "selector"} {
		t.Run(change, func(t *testing.T) {
			dir := whQASeed(t)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			var mutateErr error
			reached, restored := false, false
			var target, displaced string
			restore := func() {
				if !reached || restored {
					return
				}
				var err error
				switch change {
				case "shm-mode":
					err = os.Chmod(target, 0600)
				case "root-mode":
					err = os.Chmod(target, 0700)
				case "selector":
					err = os.Remove(target)
				case "root-replace", "ancestor-replace":
					err = os.Remove(target)
					if err == nil {
						err = os.Rename(displaced, target)
					}
				default:
					if change != "shm-unlink" {
						err = os.Remove(target)
					}
					if err == nil {
						err = os.Rename(displaced, target)
					}
				}
				if err != nil {
					t.Error("restore owned namespace mutation", err)
				}
				restored = err == nil
			}
			qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
				if e.Phase == "close-before" && e.Operation == "close" {
					return unix.EIO // Keep both native owners until test restores names.
				}
				return nil
			}, Observe: func(e sqlTestEvent) {
				if reached || e.Phase != "handoff-writer-ready" || e.Operation != "capture-write-handoff" {
					return
				}
				reached = true
				target = filepath.Join(dir, freshQAName+"-shm")
				switch change {
				case "shm-mode":
					mutateErr = os.Chmod(target, 0644)
				case "root-mode":
					// The owned leaf is also an ancestor of every database role;
					// its retained FD must not cache permission qualification.
					target = dir
					mutateErr = os.Chmod(target, 0755)
				case "selector":
					target = filepath.Join(dir, freshQAState)
					mutateErr = os.WriteFile(target, []byte("owned authority canary"), 0600)
				case "root-replace", "ancestor-replace":
					target = dir
					if change == "ancestor-replace" {
						target = filepath.Dir(dir)
					}
					displaced = target + "-owned-displaced"
					mutateErr = os.Rename(target, displaced)
					if mutateErr == nil {
						mutateErr = os.Mkdir(target, 0700)
					}
				default:
					if change == "wal-replace" {
						target = filepath.Join(dir, freshQAName+"-wal")
					}
					if change == "main-replace" {
						target = filepath.Join(dir, freshQAName)
					}
					displaced = target + "-owned-displaced"
					mutateErr = os.Rename(target, displaced)
					if mutateErr == nil && change != "shm-unlink" {
						mutateErr = os.WriteFile(target, []byte("owned replacement canary"), 0600)
					}
				}
			}})
			c, kind, err := whQAOpen(t, context.Background(), dir, time.Now().Add(250*time.Millisecond))
			t.Cleanup(restore) // Restore before the just-registered aggregate cleanup.
			if mutateErr != nil || !reached || err == nil || kind != 0 || c == nil {
				t.Fatal("fresh namespace refusal not reached", change, err, mutateErr)
			}
			if trace.count("shm", "open", "validated") != 1 || statsForTest().NativeActive != 2 {
				t.Fatal("mutation did not reach actual two-handle SHM reuse")
			}
			whQAOnlyCleanup(t, c)
			if change == "selector" || change == "shm-replace" || change == "wal-replace" || change == "main-replace" {
				got, readErr := os.ReadFile(target)
				want := "owned replacement canary"
				if change == "selector" {
					want = "owned authority canary"
				}
				if readErr != nil || string(got) != want {
					t.Fatal("refusal changed exact private target", readErr)
				}
			}
			restore()
			setSQLHooksForTest(sqlTestHooks{})
			whQAClose(t, c, true)
			if trace.count("root", "lease", "released") != 1 {
				t.Fatal("namespace failure lost single lease release")
			}
			freshQAQuiet(t)
			setHooksForTest(hooks{})
			linkQAColdReceipt(t, dir, 20)
		})
	}
}

func TestSQLiteCaptureWriterHandoffAliasKeepsSurvivingNativeLock(t *testing.T) {
	dir, other := whQASeed(t), qaDirectory(t)
	const otherName = "owned-live.sqlite3"
	live, err := Open(context.Background(), other, otherName, Options{Create: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	whQAOwn(t, live)
	if err != nil || live == nil {
		t.Fatal("live native alias control", err)
	}
	tx := whQABegin(t, live, Write)
	qaDone(t, tx, "CREATE TABLE owned_handoff_lock(id INTEGER)")
	freshQALockWitness(t, other, otherName, "main")
	target, source := filepath.Join(dir, freshQAName), filepath.Join(other, otherName)
	displaced := target + "-owned-original"
	moved, quarantined, rwReached := false, false, false
	mainOpens := 0
	var hookErr error
	restore := func() {
		if !moved {
			return
		}
		if err := os.Rename(target, source); err != nil {
			t.Error("restore actual native inode", err)
			return
		}
		if err := os.Rename(displaced, target); err != nil {
			t.Error("restore original main", err)
			return
		}
		moved = false
	}
	qaFSHooks(t, hooks{Observe: func(e event) {
		if e.Namespace != live.root.key && e.Role == "main" && e.Op == "open" && e.Phase == "before" {
			mainOpens++
			if mainOpens == 2 {
				rwReached = true
				hookErr = os.Rename(target, displaced)
				if hookErr == nil {
					hookErr = os.Rename(source, target)
				}
				moved = hookErr == nil
			}
		}
		if e.Role == "main" && e.Op == "open" && e.Phase == "quarantined" {
			quarantined = e.FD >= 0
		}
	}})
	c, kind, err := whQAOpen(t, context.Background(), dir, time.Now().Add(250*time.Millisecond))
	t.Cleanup(restore)
	if hookErr != nil || !rwReached || !moved || !quarantined || err == nil || kind != 0 || c != nil {
		t.Fatal("actual RW-open alias refusal", err, hookErr)
	}
	if s := statsForTest(); s.NativeActive != 1 || s.Active != 1 || !s.Poisoned || s.RejectedFDs != 1 {
		t.Fatal("alias FD drained beside surviving native owner", s)
	}
	freshQALockWitness(t, dir, freshQAName, "main")
	setHooksForTest(hooks{})
	restore()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	whQAClose(t, live, false)
	freshQAQuiet(t)
	linkQAColdReceipt(t, dir, 20)
}

func TestSQLiteCaptureWriterHandoffHotJournalAndTerminalRootError(t *testing.T) {
	t.Run("hot-journal", func(t *testing.T) {
		dir := qaDirectory(t)
		child := freshQAChildStart(t, "hot-journal", "spilled", dir, freshQAName)
		child.join(t, true)
		image := freshQAImage(t, dir, freshQAName)
		magic := []byte{0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7}
		if len(image[""]) < 4096 || len(image["-journal"]) <= 512 || !bytes.Equal(image["-journal"][:8], magic) {
			t.Fatal("actual hot journal premise")
		}
		main := append([]byte(nil), image[""]...)
		main[18], main[19] = 2, 2
		if err := os.WriteFile(filepath.Join(dir, freshQAName), main, 0600); err != nil {
			t.Fatal(err)
		}
		before := freshQAImage(t, dir, freshQAName)
		trace := &freshQATrace{}
		qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL})
		c, kind, err := whQAOpen(t, context.Background(), dir, time.Now().Add(250*time.Millisecond))
		var native *Error
		if c != nil || kind != 0 || !errors.As(err, &native) || native.Code != lib.SQLITE_READONLY_ROLLBACK || trace.sqlCount("handoff-before-writer", "capture-write-handoff") != 0 {
			t.Fatal("hot journal entered writable recovery", err)
		}
		freshQAQuiet(t)
		freshQASameImage(t, dir, freshQAName, before)
	})
	t.Run("terminal-root-error", func(t *testing.T) {
		dir := whQASeed(t)
		whQAFatalChild(t, dir)
		freshQAQuiet(t)
		linkQAColdReceipt(t, dir, 20)
	})
}
