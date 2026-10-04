//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	lib "modernc.org/sqlite/lib"
)

// SG06: the raced Openat must succeed on a native main or SHM inode, then
// quarantine its actual FD while the other native owner keeps its POSIX lock.
func TestSQLiteSyncGuardActualNativeAliasQuarantine(t *testing.T) {
	for _, role := range []string{"main", "shm"} {
		t.Run(role, func(t *testing.T) {
			nativeDir := qaDirectory(t)
			const nativeName = "native-lock-owner.sqlite3"
			owner := qaOpen(t, nativeDir, nativeName, true)
			tx := qaBegin(t, owner, context.Background(), Write)
			t.Cleanup(func() { _ = tx.Rollback() })
			qaDone(t, tx, "CREATE TABLE guard_alias_witness(id INTEGER PRIMARY KEY)")
			sourceName := nativeName
			if role == "shm" {
				sourceName += "-shm"
			}
			freshQALockWitness(t, nativeDir, sourceName, role)
			guardDir := qaDirectory(t)
			guardName := sgQAState + ".sync.lock"
			target := filepath.Join(guardDir, guardName)
			if err := os.WriteFile(target, nil, 0600); err != nil {
				t.Fatal(err)
			}
			moved, quarantined, attempted := false, false, false
			quarantinedFD := -1
			var hookErr error
			qaFSHooks(t, hooks{Observe: func(e event) {
				if e.Role == "sync-guard" && e.Op == "open" && e.Phase == "before" && !attempted {
					attempted = true
					hookErr = os.Rename(filepath.Join(nativeDir, sourceName), target)
					moved = hookErr == nil
				}
				if e.Role == "sync-guard" && e.Op == "open" && e.Phase == "quarantined" {
					quarantined = true
					quarantinedFD = e.FD
				}
			}})
			g, err := AcquireSyncRunGuard(context.Background(), guardDir, sgQAState, sgQADB, time.Now().Add(250*time.Millisecond))
			sgQAOwn(t, g)
			t.Cleanup(func() {
				setHooksForTest(hooks{})
				if moved {
					if _, statErr := os.Lstat(target); statErr == nil {
						if restoreErr := os.Rename(target, filepath.Join(nativeDir, sourceName)); restoreErr != nil {
							t.Errorf("restore native inode on cleanup: %v", restoreErr)
						}
					}
				}
				_ = tx.Rollback()
				_ = owner.Close(context.Background())
				if g != nil {
					_, _ = g.Close()
				}
			})
			if hookErr != nil || !moved || !quarantined || quarantinedFD < 0 || !errors.Is(err, ErrUnsafe) || g == nil {
				t.Fatal("actual raced native alias lacked cleanup-only owner", err, hookErr, quarantinedFD)
			}
			if terminal, closeErr := g.Close(); terminal || closeErr == nil {
				t.Fatal("native-active quarantine falsely drained", terminal, closeErr)
			}
			if s := statsForTest(); !s.Poisoned || s.RejectedFDs != 1 || s.NativeActive != 1 {
				t.Fatalf("shared registry lost native/quarantine evidence: %+v", s)
			}
			freshQALockWitness(t, guardDir, guardName, role)
			setHooksForTest(hooks{})
			if err := os.Rename(target, filepath.Join(nativeDir, sourceName)); err != nil {
				t.Fatal("restore native inode", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			qaClose(t, owner)
			if terminal, closeErr := g.Close(); !terminal || closeErr == nil {
				t.Fatal("cleanup-only owner did not finish after native close", terminal, closeErr)
			}
			if s := statsForTest(); s.RejectedFDs != 0 || s.SyncEntries != 0 || s.NativeActive != 0 || s.Poisoned || s.Fatal {
				t.Fatalf("alias quarantine retained after actual native release: %+v", s)
			}
		})
	}
	t.Run("reverse-native-open-to-held-S", sgQANativeAdmissionRejectsHeldSAlias)
}

// The converse race reaches native main Openat after its preflight: a held S
// inode replaces a previously valid database file. This exercises the same
// registry in the native-to-sync direction, with the actual S flock witnessed
// by another process before and after the rejected native open.
func sgQANativeAdmissionRejectsHeldSAlias(t *testing.T) {
	guardDir := qaDirectory(t)
	holder := sgQAAcquire(t, guardDir, sgQAState, sgQADB, context.Background(), time.Now().Add(250*time.Millisecond))
	sgQAStartChild(t, "try", guardDir, sgQAState, sgQADB)
	nativeDir := qaDirectory(t)
	const nativeName = "native-alias.sync.lock"
	initial := qaOpen(t, nativeDir, nativeName, true)
	qaClose(t, initial)
	target := filepath.Join(nativeDir, nativeName)
	saved := filepath.Join(nativeDir, "owned-original-main")
	guardPath := filepath.Join(guardDir, sgQAState+".sync.lock")
	moved, quarantined, attempted := false, false, false
	quarantinedFD := -1
	var hookErr error
	qaFSHooks(t, hooks{Observe: func(e event) {
		if e.Role == "main" && e.Op == "open" && e.Phase == "before" && !attempted {
			attempted = true
			hookErr = os.Rename(target, saved)
			if hookErr == nil {
				hookErr = os.Rename(guardPath, target)
			}
			moved = hookErr == nil
		}
		if e.Role == "main" && e.Op == "open" && e.Phase == "quarantined" {
			quarantined = true
			quarantinedFD = e.FD
		}
	}})
	c, err := Open(context.Background(), nativeDir, nativeName, Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	if c != nil {
		t.Cleanup(func() { _, _ = c.CloseChecked(context.Background()) })
	}
	t.Cleanup(func() {
		setHooksForTest(hooks{})
		if moved {
			if _, statErr := os.Lstat(guardPath); errors.Is(statErr, os.ErrNotExist) {
				if restoreErr := os.Rename(target, guardPath); restoreErr != nil {
					t.Errorf("restore held S on cleanup: %v", restoreErr)
				}
			}
		}
		if _, statErr := os.Lstat(saved); statErr == nil {
			if restoreErr := os.Rename(saved, target); restoreErr != nil {
				t.Errorf("restore native main on cleanup: %v", restoreErr)
			}
		}
		if c != nil {
			_, _ = c.CloseChecked(context.Background())
		}
		_, _ = holder.Close()
	})
	if c != nil {
		if closeErr := c.Close(context.Background()); closeErr != nil {
			t.Fatal("failed native open cleanup", closeErr)
		}
	}
	nativeErr, ok := err.(*Error)
	if hookErr != nil || !moved || !quarantined || quarantinedFD < 0 || !ok || nativeErr.Phase != OpenPhase || nativeErr.Category != IO || nativeErr.Code != lib.SQLITE_CANTOPEN {
		t.Fatal("native main opened held S alias", err, hookErr, quarantinedFD)
	}
	if s := statsForTest(); s.SyncGuards != 1 || s.RejectedFDs != 1 || !s.Poisoned || s.NativeActive != 0 {
		t.Fatalf("native rejected FD drained beside held S: %+v", s)
	}
	// A different process sees the same flock at its new pathname.
	sgQAStartChild(t, "try", nativeDir, "native-alias", "probe.sqlite3")
	blocked, blockedErr := AcquireSyncRunGuard(context.Background(), qaDirectory(t), "blocked-state.json", sgQADB, time.Now().Add(250*time.Millisecond))
	sgQAOwn(t, blocked)
	if blocked != nil || !errors.Is(blockedErr, ErrUnsafe) {
		t.Fatal("poison admitted another guard", blockedErr)
	}
	setHooksForTest(hooks{})
	if err := os.Rename(target, guardPath); err != nil {
		t.Fatal("restore held S name", err)
	}
	if err := os.Rename(saved, target); err != nil {
		t.Fatal("restore native main", err)
	}
	sgQAClose(t, holder)
	if s := statsForTest(); s.SyncGuards != 0 || s.SyncEntries != 0 || s.RejectedFDs != 0 || s.Poisoned || s.Fatal {
		t.Fatalf("reverse alias was not released after actual S close: %+v", s)
	}
}

// SG07: even without native SQL, the live registered S descriptor must stop
// another S name from opening its inode. The rejected FD drains only after the
// real flock holder closes; an external child proves that flock was held.
func TestSQLiteSyncGuardGuardOnlyAliasPartialClose(t *testing.T) {
	holderDir := qaDirectory(t)
	holder := sgQAAcquire(t, holderDir, sgQAState, sgQADB, context.Background(), time.Now().Add(250*time.Millisecond))
	targetDir := qaDirectory(t)
	const targetState = "target-state.json"
	target := filepath.Join(targetDir, targetState+".sync.lock")
	if err := os.WriteFile(target, nil, 0600); err != nil {
		t.Fatal(err)
	}
	moved, quarantined, attempted := false, false, false
	quarantinedFD := -1
	var hookErr error
	qaFSHooks(t, hooks{Observe: func(e event) {
		if e.Role == "sync-guard" && e.Op == "open" && e.Phase == "before" && !attempted {
			attempted = true
			hookErr = os.Rename(filepath.Join(holderDir, sgQAState+".sync.lock"), target)
			moved = hookErr == nil
		}
		if e.Role == "sync-guard" && e.Op == "open" && e.Phase == "quarantined" {
			quarantined = true
			quarantinedFD = e.FD
		}
	}})
	g, err := AcquireSyncRunGuard(context.Background(), targetDir, targetState, sgQADB, time.Now().Add(250*time.Millisecond))
	sgQAOwn(t, g)
	t.Cleanup(func() {
		setHooksForTest(hooks{})
		if moved {
			if _, statErr := os.Lstat(target); statErr == nil {
				if restoreErr := os.Rename(target, filepath.Join(holderDir, sgQAState+".sync.lock")); restoreErr != nil {
					t.Errorf("restore held S on cleanup: %v", restoreErr)
				}
			}
		}
		_, _ = holder.Close()
		if g != nil {
			_, _ = g.Close()
		}
	})
	if hookErr != nil || !moved || !quarantined || quarantinedFD < 0 || !errors.Is(err, ErrUnsafe) || g == nil {
		t.Fatal("guard-only actual alias was not quarantined", err, hookErr, quarantinedFD)
	}
	if s := statsForTest(); s.NativeActive != 0 || s.SyncGuards != 1 || s.RejectedFDs != 1 || !s.Poisoned {
		t.Fatalf("guard-only quarantine/owner counts: %+v", s)
	}
	sgQAStartChild(t, "try", targetDir, targetState, sgQADB)
	if terminal, closeErr := g.Close(); terminal || closeErr == nil {
		t.Fatal("blocked drain claimed terminal cleanup", terminal, closeErr)
	}
	setHooksForTest(hooks{})
	if err := os.Rename(target, filepath.Join(holderDir, sgQAState+".sync.lock")); err != nil {
		t.Fatal("restore held S before close", err)
	}
	sgQAClose(t, holder)
	if terminal, closeErr := g.Close(); !terminal || closeErr == nil {
		t.Fatal("pending owner did not finish after holder release", terminal, closeErr)
	}
	if s := statsForTest(); s.RejectedFDs != 0 || s.SyncGuards != 0 || s.SyncEntries != 0 || s.Poisoned || s.Fatal {
		t.Fatalf("guard-only quarantine did not drain: %+v", s)
	}
}

// SG08: close-before retains the live flock for one checked retry; close-after
// caches a terminal error after the flock has actually gone away.
func TestSQLiteSyncGuardCloseFaultOwnership(t *testing.T) {
	for _, phase := range []string{"close-before", "close-after"} {
		t.Run(phase, func(t *testing.T) {
			dir := qaDirectory(t)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			g := sgQAAcquire(t, dir, sgQAState, sgQADB, context.Background(), time.Now().Add(250*time.Millisecond))
			if trace.count("sync-guard", "open", "validated") != 1 || trace.count("sync-guard", "flock", "after") < 1 {
				t.Fatal("positive native open/flock control was not reached")
			}
			reached := false
			qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
				if e.Operation == "sync-guard" && e.Phase == phase && !reached {
					reached = true
					return unix.EIO
				}
				return nil
			}})
			terminal, first := g.Close()
			var safe *Error
			if !reached || !errors.As(first, &safe) || safe.Phase != ClosePhase || safe.Category != IO || terminal != (phase == "close-after") {
				t.Fatal("fault disposition at actual close boundary", phase, terminal, first)
			}
			setSQLHooksForTest(sqlTestHooks{})
			if phase == "close-before" {
				if s := statsForTest(); s.SyncGuards != 1 || s.SyncEntries != 1 {
					t.Fatalf("before-close fault released actual owner: %+v", s)
				}
				sgQAStartChild(t, "try", dir, sgQAState, sgQADB)
				terminal, retry := g.Close()
				if !terminal || retry != first {
					t.Fatal("known-unattempted close did not retry", terminal, retry)
				}
			}
			terminal, cached := g.Close()
			if !terminal || cached != first {
				t.Fatal("terminal close lost cached fault", terminal, cached)
			}
			if trace.count("sync-guard", "close", "after") != 1 {
				t.Fatal("actual S descriptor close was missing or repeated")
			}
			next := sgQAAcquire(t, dir, sgQAState, sgQADB, context.Background(), time.Now().Add(250*time.Millisecond))
			sgQAClose(t, next)
			if s := statsForTest(); s.SyncGuards != 0 || s.SyncEntries != 0 || s.Poisoned || s.Fatal {
				t.Fatalf("faulted terminal close leaked shared entry: %+v", s)
			}
		})
	}
	t.Run("cancel-after-actual-flock", func(t *testing.T) {
		dir := qaDirectory(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		trace := &freshQATrace{}
		qaFSHooks(t, hooks{Observe: func(e event) {
			trace.observeFS(e)
			if e.Role == "sync-guard" && e.Op == "flock" && e.Phase == "after" && e.Errno == 0 {
				cancel()
			}
		}})
		g, err := AcquireSyncRunGuard(ctx, dir, sgQAState, sgQADB, time.Now().Add(250*time.Millisecond))
		sgQAOwn(t, g)
		if g != nil || !errors.Is(err, context.Canceled) {
			t.Fatal("cancel after actual flock did not cleanly refuse", err)
		}
		if trace.count("sync-guard", "open", "validated") != 1 || trace.count("sync-guard", "flock", "after") != 1 || trace.count("sync-guard", "close", "after") != 1 {
			t.Fatal("partial acquisition did not open, flock, and close actual S")
		}
		if s := statsForTest(); s.SyncGuards != 0 || s.SyncEntries != 0 || s.RejectedFDs != 0 || s.Poisoned || s.Fatal {
			t.Fatalf("canceled partial owner leaked: %+v", s)
		}
	})
}

func TestSQLiteSyncGuardFatalReusedFDInOwnedChild(t *testing.T) {
	dir := qaDirectory(t)
	sgQAStartChild(t, "fatal", dir, sgQAState, sgQADB)
	g := sgQAAcquire(t, dir, sgQAState, sgQADB, context.Background(), time.Now().Add(250*time.Millisecond))
	sgQAClose(t, g)
}
