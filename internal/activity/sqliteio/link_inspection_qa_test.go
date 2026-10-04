//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

// Test-first source for the approved first-Link ABI v2 native prerequisites.
// No Go/native/formatter execution or compile RED/PASS is claimed at freeze.
// Fault cases below prove existing sanitizer/ownership plumbing, never a real
// exhausted filesystem. Fresh fixtures/native owners must be integrated first.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	lib "modernc.org/sqlite/lib"
)

func TestSQLiteLinkInspectAbsentParentsAndCheckedNofollow(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint("missing-", missing), func(t *testing.T) {
			ancestor := qaDirectory(t)
			dir := ancestor
			if missing {
				dir = filepath.Join(ancestor, "absent-one", "absent-two")
			}
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL})
			c, kind, err := linkQAInspect(t, dir)
			if err != nil || c != nil || kind != LinkAbsent {
				t.Fatal("safe missing family not absent", kind, err)
			}
			linkQAEmpty(t, ancestor)
			linkQANoCreating(t, trace, true)
			if trace.count("main", "open", "validated") != 0 || trace.sqlCount("close-before", "close") != 0 {
				t.Fatal("absent path entered native main/close")
			}
			freshQAQuiet(t)
		})
	}
	for _, kind := range []string{"ancestor-symlink", "ancestor-file", "unsafe-final", "relative", "dotdot", "nul"} {
		t.Run(kind, func(t *testing.T) {
			ancestor := qaDirectory(t)
			dir := filepath.Join(ancestor, "prefix", "missing")
			switch kind {
			case "ancestor-symlink":
				if err := os.Symlink(qaDirectory(t), filepath.Join(ancestor, "prefix")); err != nil {
					t.Fatal(err)
				}
			case "ancestor-file":
				if err := os.WriteFile(filepath.Join(ancestor, "prefix"), []byte("owned"), 0600); err != nil {
					t.Fatal(err)
				}
			case "unsafe-final":
				dir = filepath.Join(ancestor, "prefix")
				if err := os.Mkdir(dir, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(dir, 0755); err != nil {
					t.Fatal(err)
				}
			case "relative":
				dir = "owned-relative/missing"
			case "dotdot":
				dir = ancestor + "/prefix/../missing"
			case "nul":
				dir = ancestor + "/bad\x00/missing"
			}
			c, inspection, err := linkQAInspect(t, dir)
			linkQARefused(t, c, inspection, err)
		})
	}
}

func TestSQLiteLinkInspectParentHelperIdentityAndPermissions(t *testing.T) {
	ancestor := qaDirectory(t)
	// Ancestors need no 0700 policy; only an existing final activity directory.
	if err := os.Chmod(ancestor, 0755); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(ancestor, "absent", "child")
	id, absent, err := inspectLinkParent(freshQAContext(t), missing, time.Now().Add(time.Second))
	if err != nil || !absent || id != (identity{}) {
		t.Fatal("qualified ancestor ENOENT lost", absent, err)
	}
	linkQAEmpty(t, ancestor)
	dir := filepath.Join(ancestor, "present")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	id, absent, err = inspectLinkParent(freshQAContext(t), dir, time.Now().Add(time.Second))
	var st unix.Stat_t
	if unix.Lstat(dir, &st) != nil || err != nil || absent || id != fileIdentity(&st) {
		t.Fatal("checked present identity mismatch", err)
	}
	if err := os.Mkdir(filepath.Join(ancestor, "denied"), 0700); err != nil {
		t.Fatal(err)
	}
	denied := filepath.Join(ancestor, "denied")
	if err := os.Chmod(denied, 0000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(denied, 0700) })
	if unix.Geteuid() == 0 {
		t.Skip("root bypasses inaccessible-ancestor OS premise; remaining assertions already ran")
	}
	id, absent, err = inspectLinkParent(freshQAContext(t), filepath.Join(denied, "missing"), time.Now().Add(time.Second))
	if err == nil || absent || id != (identity{}) {
		t.Fatal("inaccessible ancestor inferred absence", err)
	}
}

func TestSQLiteLinkInspectOccupiedAuthorityPrecedesNative(t *testing.T) {
	for _, kind := range []string{"file", "directory", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			dir := qaDirectory(t)
			path := linkQAOccupied(t, dir, kind)
			before := freshQAOccupiedSnapshot(t, path)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			c, inspection, err := linkQAInspect(t, dir)
			var e *Error
			if !errors.As(err, &e) || e.Phase != Admission || e.Category != Unsafe || !errors.Is(err, os.ErrExist) || c != nil {
				t.Fatal("occupied authority exact evidence/early refusal lost", err)
			}
			linkQARefused(t, c, inspection, err)
			linkQASameOccupied(t, path, before)
			if trace.count("main", "open", "validated") != 0 {
				t.Fatal("occupied authority entered native main")
			}
			if _, err := os.Lstat(filepath.Join(dir, freshQAState+".lock")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("occupied authority created guard")
			}
		})
	}
}

func TestSQLiteLinkInspectNamesAndSidecarOnlyNeverCreate(t *testing.T) {
	for _, names := range [][2]string{{"", freshQAName}, {"../bad", freshQAName}, {"nul\x00name", freshQAName}, {freshQAName, freshQAName}, {freshQAName + "-wal", freshQAName}, {freshQAState, "bad?name"}, {freshQAState, "../bad"}} {
		dir := qaDirectory(t)
		c, kind, err := InspectForLink(freshQAContext(t), dir, names[0], names[1], time.Now().Add(time.Second))
		freshQAOwn(t, c)
		linkQARefused(t, c, kind, err)
		linkQAEmpty(t, dir)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		t.Run(suffix, func(t *testing.T) {
			dir := qaDirectory(t)
			if err := os.WriteFile(filepath.Join(dir, freshQAName+suffix), []byte("owned sidecar"), 0600); err != nil {
				t.Fatal(err)
			}
			before := freshQAImage(t, dir, freshQAName)
			c, kind, err := linkQAInspect(t, dir)
			linkQARefused(t, c, kind, err)
			freshQASameImage(t, dir, freshQAName, before)
			linkQANoAuthority(t, dir)
		})
	}
}

func TestSQLiteLinkInspectActualPristineNativeProofAndUnchangedImage(t *testing.T) {
	for _, profile := range []string{"zero", "single"} {
		t.Run(profile, func(t *testing.T) {
			dir := qaDirectory(t)
			freshQAProfile(t, dir, profile)
			before := freshQAImage(t, dir, freshQAName)
			identityBefore := freshQAOccupiedSnapshot(t, filepath.Join(dir, freshQAName))
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL})
			c, kind, err := linkQAInspect(t, dir)
			if err != nil || c != nil || kind != LinkPristine {
				t.Fatal("genuine pristine native classification", kind, err)
			}
			if trace.count("main", "open", "validated") < 1 || trace.count("root", "lease", "released") != 1 {
				t.Fatal("pristine did not prove actual native admission and checked root release")
			}
			linkQANoCreating(t, trace, true)
			freshQASameImage(t, dir, freshQAName, before)
			linkQASameOccupied(t, filepath.Join(dir, freshQAName), identityBefore)
			linkQANoAuthority(t, dir)
			freshQAQuiet(t)
		})
	}
}

func TestSQLiteLinkInspectForeignProfilesAndPhysicalHeaderRefusals(t *testing.T) {
	for _, profile := range []string{"page-size", "application-id", "user-version", "schema-version", "auto-vacuum", "objects", "allocated-free"} {
		t.Run(profile, func(t *testing.T) {
			dir := qaDirectory(t)
			freshQAProfile(t, dir, profile)
			before := freshQAImage(t, dir, freshQAName)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			c, kind, err := linkQAInspect(t, dir)
			if trace.count("main", "open", "validated") < 1 {
				t.Fatal("native profile refusal did not reach native main")
			}
			if err == nil || kind != LinkInspection(0) || c != nil {
				t.Fatal("conclusive native profile refusal returned usable classification/owner", err)
			}
			if trace.count("root", "lease", "released") != 1 {
				t.Fatal("native refusal did not complete checked release before caller cleanup")
			}
			freshQAQuiet(t) // Must already be quiet before linkQARefused cleanup.
			linkQARefused(t, c, kind, err)
			freshQASameImage(t, dir, freshQAName, before)
			linkQANoAuthority(t, dir)
		})
	}
	for _, mutation := range []string{"short", "magic", "mixed-journal-versions", "wal-page-size", "reserved", "payload-fractions", "page-count", "truncated-physical", "trailing-physical"} {
		t.Run(mutation, func(t *testing.T) {
			dir := qaDirectory(t)
			freshQAProfile(t, dir, "single")
			b := freshQAImage(t, dir, freshQAName)[""]
			if len(b) != 4096 {
				t.Fatal("native physical fixture premise")
			}
			switch mutation {
			case "short":
				b = []byte("SQLite format 3\x00")
			case "magic":
				b[0] = 'X'
			case "mixed-journal-versions":
				b[18], b[19] = 2, 1
			case "wal-page-size":
				b[18], b[19], b[16], b[17] = 2, 2, 32, 0
			case "reserved":
				b[20] = 1
			case "payload-fractions":
				b[21] = 63
			case "page-count":
				b[28], b[31] = 1, 3
			case "truncated-physical":
				b = b[:100]
			case "trailing-physical":
				b = append(b, 0)
			}
			if err := os.WriteFile(filepath.Join(dir, freshQAName), b, 0600); err != nil {
				t.Fatal(err)
			}
			before := freshQAImage(t, dir, freshQAName)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			c, kind, err := linkQAInspect(t, dir)
			linkQARefused(t, c, kind, err)
			freshQASameImage(t, dir, freshQAName, before)
			linkQANoCreating(t, trace, true)
			linkQANoAuthority(t, dir)
		})
	}
}

func TestSQLiteLinkInspectQualifiedWALReturnsOneUnusedReadOnlyOwner(t *testing.T) {
	dir := qaDirectory(t)
	linkQASeedWAL(t, dir)
	before := freshQAImage(t, dir, freshQAName)[""]
	trace := &freshQATrace{}
	qaFSHooks(t, hooks{Observe: trace.observeFS})
	c, kind, err := linkQAInspect(t, dir)
	if err != nil || c == nil || kind != LinkWAL || !c.readOnly || c.used || c.closed || c.db == 0 {
		t.Fatal("WAL inspection did not return exact unused native read-only owner", kind, err)
	}
	if statsForTest().NativeActive != 1 || statsForTest().Active != 1 {
		t.Fatal("WAL owner count")
	}
	if tx, err := c.Begin(freshQAContext(t), Write); tx != nil || err == nil {
		t.Fatal("WAL inspection authorized Write")
	}
	// Actual connection-local controls; complete SQL setter enumeration needs
	// source review because the existing observer deliberately omits SQL text.
	for _, q := range []struct{ sql, want string }{{"PRAGMA temp_store", "2"}, {"PRAGMA foreign_keys", "1"}, {"PRAGMA synchronous", "2"}, {"PRAGMA wal_autocheckpoint", "0"}, {"PRAGMA journal_mode", "wal"}, {"PRAGMA page_size", "4096"}} {
		got, err := c.control(freshQAContext(t), q.sql, authPragma, VerifyPhase)
		if err != nil || got != q.want {
			t.Fatal("read-only setup value", q.sql, err)
		}
	}
	tx := qaBegin(t, c, freshQAContext(t), Read)
	linkQARawReceipt(t, tx)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
	qaClose(t, c)
	if trace.count("root", "lease", "released") != 1 {
		t.Fatal("WAL owner released other than once")
	}
	linkQANoCreating(t, trace, false)
	if !bytes.Equal(freshQAImage(t, dir, freshQAName)[""], before) {
		t.Fatal("inspection changed physical main")
	}
	linkQANoAuthority(t, dir)
	freshQAQuiet(t)
	linkQAColdReceipt(t, dir)
}

func TestSQLiteLinkInspectCancellationDeadlineAndReachedCloseOwnership(t *testing.T) {
	for _, expired := range []bool{false, true} {
		dir := qaDirectory(t)
		freshQAProfile(t, dir, "single")
		before := freshQAImage(t, dir, freshQAName)
		ctx, cancel := context.WithCancel(freshQAContext(t))
		deadline := time.Now().Add(time.Second)
		if expired {
			deadline = time.Now().Add(-time.Second)
		} else {
			cancel()
		}
		c, kind, err := InspectForLink(ctx, dir, freshQAState, freshQAName, deadline)
		cancel()
		freshQAOwn(t, c)
		linkQARefused(t, c, kind, err)
		if !expired && !errors.Is(err, context.Canceled) {
			t.Fatal("cancellation precedence lost")
		}
		freshQASameImage(t, dir, freshQAName, before)
	}
	for _, cancelAtClose := range []bool{false, true} {
		t.Run(fmt.Sprint("reached-close-cancel-", cancelAtClose), func(t *testing.T) {
			dir := qaDirectory(t)
			freshQAProfile(t, dir, "single")
			before := freshQAImage(t, dir, freshQAName)
			ctx, cancel := context.WithCancel(freshQAContext(t))
			defer cancel()
			reached := false
			qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
				if e.Phase == "close-before" && e.Operation == "close" {
					reached = true
					if cancelAtClose {
						cancel()
					}
					return unix.EIO
				}
				return nil
			}})
			c, kind, err := InspectForLink(ctx, dir, freshQAState, freshQAName, time.Now().Add(time.Second))
			freshQAOwn(t, c)
			if !reached || err == nil || kind != LinkInspection(0) || c == nil || c.closed || statsForTest().Active != 1 {
				t.Fatal("reached probe/root close fault dropped exact cleanup-only owner", err)
			}
			// After an earlier conclusive closeNative this may own only the root.
			// Any still-live native handle must remain counted; never Begin/ACK.
			if c.db != 0 {
				if statsForTest().NativeActive != 1 {
					t.Fatal("live native owner lost")
				}
				_ = lib.Xsqlite3_get_autocommit(c.tls, c.db)
			} else if statsForTest().NativeActive != 0 {
				t.Fatal("closed native owner remained counted")
			}
			setSQLHooksForTest(sqlTestHooks{})
			if err := c.Close(context.Background()); err != nil {
				t.Fatal("eventual checked cleanup", err)
			}
			freshQAQuiet(t)
			freshQASameImage(t, dir, freshQAName, before)
			linkQANoAuthority(t, dir)
		})
	}
}

func TestSQLiteLinkInspectActualNativeCloseBusyRetainsWALOwner(t *testing.T) {
	dir := qaDirectory(t)
	linkQASeedWAL(t, dir)
	c, kind, err := linkQAInspect(t, dir)
	if err != nil || c == nil || kind != LinkWAL {
		t.Fatal("WAL control", err)
	}
	finalize, err := c.holdStatementForTest("SELECT request_id FROM link_qa_receipts")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = finalize() })
	err = c.Close(context.Background())
	var e *Error
	if !errors.As(err, &e) || e.Phase != ClosePhase || e.Code&255 != lib.SQLITE_BUSY || c.db == 0 || c.closed || statsForTest().NativeActive != 1 {
		t.Fatal("actual sqlite3_close BUSY released live owner", err)
	}
	if err := finalize(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
	freshQAQuiet(t)
	linkQAColdReceipt(t, dir)
}

func TestSQLiteLinkInspectRootReplacementAndDELETEUnexpectedSidecars(t *testing.T) {
	for _, race := range []string{"root-replaced", "main-replaced", "journal-created"} {
		t.Run(race, func(t *testing.T) {
			dir := qaDirectory(t)
			freshQAProfile(t, dir, "single")
			before := freshQAImage(t, dir, freshQAName)
			moved := false
			old := dir + "-owned-displaced"
			t.Cleanup(func() { _ = os.RemoveAll(old) })
			qaFSHooks(t, hooks{Observe: func(e event) {
				if moved || e.Role != "main" || e.Op != "open" || e.Phase != "before" {
					return
				}
				moved = true
				switch race {
				case "root-replaced":
					if err := os.Rename(dir, old); err != nil {
						t.Fatal(err)
					}
					if err := os.Mkdir(dir, 0700); err != nil {
						t.Fatal(err)
					}
				case "main-replaced":
					if err := os.Rename(filepath.Join(dir, freshQAName), filepath.Join(dir, "owned-displaced-main")); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(dir, freshQAName), before[""], 0600); err != nil {
						t.Fatal(err)
					}
				case "journal-created":
					if err := os.WriteFile(filepath.Join(dir, freshQAName+"-journal"), []byte("owned unexpected journal"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}})
			c, kind, err := linkQAInspect(t, dir)
			if !moved {
				t.Fatal("actual native open race not reached")
			}
			linkQARefused(t, c, kind, err)
			setHooksForTest(hooks{})
			switch race {
			case "root-replaced":
				freshQASameImage(t, old, freshQAName, before)
				linkQAEmpty(t, dir)
			case "main-replaced":
				freshQASameImage(t, dir, freshQAName, before)
				freshQASameImage(t, dir, "owned-displaced-main", before)
			case "journal-created":
				before["-journal"] = []byte("owned unexpected journal")
				freshQASameImage(t, dir, freshQAName, before)
			}
		})
	}
}

func TestSQLiteLinkInspectCrossStoreUnsafeAliasKeepsActualPOSIXLocks(t *testing.T) {
	dir, other := qaDirectory(t), qaDirectory(t)
	name := "owned-alias-source.sqlite3"
	owner := qaOpen(t, other, name, true)
	tx := qaBegin(t, owner, freshQAContext(t), Write)
	qaDone(t, tx, "CREATE TABLE owned_lock(id INTEGER)")
	freshQALockWitness(t, other, name, "main")
	// Rename preserves single-link policy and avoids a hardlink preflight-only
	// refusal. Replacement happens after target preflight at actual Openat.
	freshQAProfile(t, dir, "single")
	target, source := filepath.Join(dir, freshQAName), filepath.Join(other, name)
	displaced := filepath.Join(dir, "owned-displaced-main")
	moved, quarantined := false, false
	qaFSHooks(t, hooks{Observe: func(e event) {
		if !moved && e.Namespace != owner.root.key && e.Role == "main" && e.Op == "open" && e.Phase == "before" {
			if err := os.Rename(target, displaced); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(source, target); err != nil {
				t.Fatal(err)
			}
			moved = true
		}
		if e.Role == "main" && e.Op == "open" && e.Phase == "quarantined" {
			if e.FD < 0 {
				t.Fatal("alias quarantine lacks actual FD")
			}
			quarantined = true
		}
	}})
	c, kind, err := linkQAInspect(t, dir)
	if !moved || !quarantined || err == nil || kind != LinkInspection(0) {
		t.Fatal("inspection actual alias rejection not reached", err)
	}
	if c != nil {
		if err := c.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	s := statsForTest()
	if !s.Poisoned || s.RejectedFDs < 1 || s.NativeActive != 1 {
		t.Fatal("alias close drained beside native POSIX lock", s)
	}
	freshQALockWitness(t, dir, freshQAName, "main")
	setHooksForTest(hooks{})
	if err := os.Rename(target, source); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(displaced, target); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, owner)
	freshQAQuiet(t)
}

func TestSQLiteLinkAuthorityActualWriterAfterOpenAndBeginWait(t *testing.T) {
	for _, wait := range []bool{false, true} {
		for _, kind := range []string{"file", "directory", "symlink"} {
			t.Run(fmt.Sprintf("wait-%t-%s", wait, kind), func(t *testing.T) {
				dir := qaDirectory(t)
				linkQASeedWAL(t, dir)
				var path string
				var release func()
				// Child startup/handshake precedes the contender's one Open/Begin
				// deadline; its admission budget is never restarted.
				if wait {
					release = qaWriter(t, dir, freshQAName)
				} else {
					path = linkQAOccupied(t, dir, kind)
				}
				c := qaOpen(t, dir, freshQAName, false)
				trace := &freshQATrace{}
				entered := make(chan struct{})
				var once sync.Once
				qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
					trace.observeSQL(e)
					if wait && e.Phase == "control-before-native" && e.Operation == "begin" {
						once.Do(func() { close(entered) })
					}
				}})
				operationCtx := freshQAContext(t)
				var coordinator *linkQAAuthorityWait
				if wait {
					coordinator = linkQAAuthorityCoordinator(operationCtx, dir, kind, entered, release)
					// Both normal/error return and Fatal/Goexit join the coordinator,
					// including when Begin never emits the observation.
					defer coordinator.join()
					t.Cleanup(coordinator.join)
				}
				started := time.Now()
				tx, beginErr := c.Begin(operationCtx, Write)
				if coordinator != nil {
					coordinator.join()
				}
				if tx != nil {
					t.Cleanup(func() { _ = tx.Rollback() })
				}
				if beginErr != nil {
					t.Fatal("actual writer Begin failed after coordinator joined", beginErr)
				}
				if wait {
					if err := <-coordinator.result; err != nil {
						t.Fatal("owned authority race setup", err)
					}
					if time.Since(started) < 50*time.Millisecond {
						t.Fatal("actual native BEGIN wait premise absent")
					}
					path = filepath.Join(dir, freshQAState)
				}
				before := freshQAOccupiedSnapshot(t, path)
				prepared := trace.sqlCount("prepare-before", "prepare")
				err := tx.CheckAuthorityAbsent(freshQAState)
				var e *Error
				if !errors.As(err, &e) || e.Phase != Admission || e.Category != Unsafe || !errors.Is(err, os.ErrExist) {
					t.Fatal("actual writer missed occupied authority after native BEGIN", err)
				}
				if trace.sqlCount("prepare-before", "prepare") != prepared {
					t.Fatal("authority check dispatched application SQL")
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				qaClose(t, c)
				linkQASameOccupied(t, path, before)
				if _, err := os.Lstat(filepath.Join(dir, freshQAState+".lock")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("ordinary writer acquired first-link guard")
				}
				linkQAColdReceipt(t, dir)
			})
		}
	}
}

func TestSQLiteLinkAuthorityLiveWriterPositiveAndMisuse(t *testing.T) {
	for _, misuseCase := range []string{"positive", "read", "terminal", "statement", "bad-name", "same-main", "same-wal", "canceled", "nil", "nonlive"} {
		t.Run(misuseCase, func(t *testing.T) {
			dir := qaDirectory(t)
			linkQASeedWAL(t, dir)
			c := qaOpen(t, dir, freshQAName, false)
			ctx, cancel := context.WithCancel(freshQAContext(t))
			defer cancel()
			mode := Write
			if misuseCase == "read" {
				mode = Read
			}
			tx := qaBegin(t, c, ctx, mode)
			name := freshQAState
			var stmt *Stmt
			switch misuseCase {
			case "terminal":
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			case "statement":
				stmt = qaPrepare(t, tx, "SELECT nonce FROM link_qa_meta")
			case "bad-name":
				name = "../owned-state"
			case "same-main":
				name = freshQAName
			case "same-wal":
				name = freshQAName + "-wal"
			case "canceled":
				cancel()
			}
			var err error
			if misuseCase == "nil" {
				var absent *Tx
				err = absent.CheckAuthorityAbsent(name)
			} else if misuseCase == "nonlive" {
				err = (&Tx{}).CheckAuthorityAbsent(name)
			} else {
				err = tx.CheckAuthorityAbsent(name)
			}
			if misuseCase == "positive" {
				if err != nil {
					t.Fatal("live writer absent authority refused", err)
				}
			} else {
				if err == nil {
					t.Fatal("authority method admitted misuse", misuseCase)
				}
				qaSafeError(t, err, name, dir)
				if misuseCase == "canceled" && !errors.Is(err, context.Canceled) {
					t.Fatal("authority context precedence lost")
				}
			}
			if stmt != nil {
				if err := stmt.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			qaClose(t, c)
			linkQANoAuthority(t, dir)
			linkQAColdReceipt(t, dir)
		})
	}
}

func TestSQLiteLinkAuthorityPinnedRootReplacementRefusal(t *testing.T) {
	dir := qaDirectory(t)
	linkQASeedWAL(t, dir)
	c := qaOpen(t, dir, freshQAName, false)
	tx := qaBegin(t, c, freshQAContext(t), Write)
	saved := dir + "-owned-authority-root"
	if err := os.Rename(dir, saved); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(saved) })
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	err := tx.CheckAuthorityAbsent(freshQAState)
	if err == nil {
		t.Fatal("authority check accepted replaced pinned root")
	}
	qaSafeError(t, err, dir)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	// Restore the root before checked cleanup; no application statement ran.
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(saved, dir); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
	linkQAColdReceipt(t, dir)
}

func TestSQLiteLinkSafeErrorBareKnownENOSPCAndContextPrecedence(t *testing.T) {
	for _, phase := range []Phase{Admission, OpenPhase, StepPhase, CommitPhase, ClosePhase} {
		wrapped := &os.PathError{Op: "owned-secret-operation", Path: "owned-secret-path", Err: syscall.ENOSPC}
		e := safeError(phase, fmt.Errorf("owned-secret-wrapper: %w", wrapped))
		linkQABareENOSPC(t, e, phase)
		qaSafeError(t, e, "owned-secret")
		clone := safeError(VerifyPhase, e)
		if clone == e || clone.Cause != syscall.ENOSPC || clone.Phase != phase {
			t.Fatal("sanitized clone changed evidence")
		}
		joined := joinCleanup(e, safeError(ClosePhase, syscall.EIO))
		if !errors.Is(joined, syscall.ENOSPC) {
			t.Fatal("checked cleanup join discarded ENOSPC")
		}
	}
	for _, cause := range []error{syscall.EIO, syscall.EACCES, &os.PathError{Op: "owned-secret-operation", Path: "owned-secret-path", Err: syscall.EIO}, fmt.Errorf("owned-secret-wrapper: %w", syscall.EACCES)} {
		e := safeError(Admission, cause)
		if e.Category != IO || e.Code != 0 || e.Cause != nil || errors.Is(e, syscall.ENOSPC) {
			t.Fatal("generic IO inferred capacity", e)
		}
		qaSafeError(t, e, "owned-secret", "permission", "input/output")
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, ErrBusy, ErrUnsafe, ErrClosed} {
		e := safeError(Admission, errors.Join(cause, syscall.ENOSPC))
		if e.Category == IO || e.Cause == syscall.ENOSPC {
			t.Fatal("ENOSPC overrode existing context/safety precedence", e)
		}
		// Existing context sanitizer retains its original context cause. A
		// joined errno there cannot authorize capacity over Canceled category.
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := contextualError(StepPhase, &os.PathError{Op: "owned", Path: "owned", Err: syscall.ENOSPC}, ctx)
	if e.Category != Canceled || e.Cause != context.Canceled || errors.Is(e, syscall.ENOSPC) {
		t.Fatal("canceled operation became capacity")
	}
	if e := engineError(StepPhase, lib.SQLITE_IOERR, nil); e.Cause != nil || errors.Is(e, syscall.ENOSPC) {
		t.Fatal("native generic IO invented errno")
	}
	if e := engineError(StepPhase, lib.SQLITE_FULL, nil); e.Category != Full || e.Code != lib.SQLITE_FULL {
		t.Fatal("native FULL changed")
	}
}

func TestSQLiteLinkENOSPCActualWrapperFaultPrecommitVsPostcommit(t *testing.T) {
	// These are approved test faults at existing actual wrapper boundaries.
	// A returned ENOSPC proves sanitizer/phase plumbing, not real disk FULL.
	for _, errno := range []syscall.Errno{syscall.ENOSPC, syscall.EIO, syscall.EACCES} {
		t.Run(fmt.Sprintf("precommit-errno-%d", errno), func(t *testing.T) {
			dir := qaDirectory(t)
			linkQASeedWAL(t, dir)
			c := qaOpen(t, dir, freshQAName, false)
			tx := qaBegin(t, c, freshQAContext(t), Write)
			if err := tx.CheckAuthorityAbsent(freshQAState); err != nil {
				t.Fatal(err)
			}
			qaDone(t, tx, "UPDATE link_qa_meta SET nonce=20")
			reached := false
			qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
				if e.Phase == "commit-before-dispatch" && e.Operation == "commit" {
					reached = true
					return &os.PathError{Op: "owned-fault", Path: "owned-secret-path", Err: errno}
				}
				return nil
			}})
			outcome, err := tx.Commit()
			if !reached || outcome != NotCommitted || err == nil {
				t.Fatal("pre-dispatch wrapper failure not definitely rolled back", outcome, err)
			}
			if errno == syscall.ENOSPC {
				linkQABareENOSPC(t, err, CommitPhase)
			} else if errors.Is(err, syscall.ENOSPC) {
				t.Fatal("non-ENOSPC became capacity")
			}
			qaSafeError(t, err, "owned-secret", "owned-fault")
			setSQLHooksForTest(sqlTestHooks{})
			qaClose(t, c)
			linkQAColdReceipt(t, dir)
		})
	}
	for _, operation := range []string{"durable-main", "durable-wal", "durable-parent"} {
		t.Run(operation, func(t *testing.T) {
			dir := qaDirectory(t)
			linkQASeedWAL(t, dir)
			c, openErr := Open(freshQAContext(t), dir, freshQAName, Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			freshQAOwn(t, c) // Expected cached terminal error is asserted below.
			if openErr != nil {
				t.Fatal("ordinary durable-fault fixture Open", openErr)
			}
			tx := qaBegin(t, c, freshQAContext(t), Write)
			if err := tx.CheckAuthorityAbsent(freshQAState); err != nil {
				t.Fatal(err)
			}
			// Receipt-established no-op writer is the separate durability fence.
			qaCommit(t, tx)
			reached := false
			qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
				if e.Phase == "fsync-before" && e.Operation == operation {
					reached = true
					return &os.PathError{Op: "owned-fsync-fault", Path: "owned-secret-path", Err: syscall.ENOSPC}
				}
				return nil
			}})
			err := c.CloseDurably(freshQAContext(t))
			if !reached || err == nil {
				t.Fatal("postcommit actual durable wrapper fault not reached", err)
			}
			linkQABareENOSPC(t, err, ClosePhase)
			qaSafeError(t, err, "owned-secret", "owned-fsync-fault")
			setSQLHooksForTest(sqlTestHooks{})
			// Ordinary cleanup cannot erase the failed durable acknowledgment.
			if cached := c.Close(context.Background()); cached != err {
				t.Fatal("terminal durable error changed/erased by ordinary cleanup", cached)
			}
			if cached := c.CloseDurably(context.Background()); cached != err {
				t.Fatal("second durable call changed/erased earlier failure", cached)
			}
			if !c.closed || c.db != 0 || c.nativeCounted {
				t.Fatal("terminal durable fault retained native ownership")
			}
			freshQAQuiet(t)
			linkQAColdReceipt(t, dir)
		})
	}
}

func TestSQLiteLinkInspectReachedNativeCancellationKeepsCleanupOnlyOwner(t *testing.T) {
	// Existing validated-main observation cancels a real native open; existing
	// close-before fault then prevents its first cleanup. No failOpen override.
	dir := qaDirectory(t)
	freshQAProfile(t, dir, "single")
	before := freshQAImage(t, dir, freshQAName)
	ctx, cancel := context.WithCancel(freshQAContext(t))
	defer cancel()
	opened, closeReached := false, false
	qaFSHooks(t, hooks{Observe: func(e event) {
		if e.Role == "main" && e.Op == "open" && e.Phase == "validated" {
			opened = true
			cancel()
		}
	}})
	qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
		if e.Phase == "close-before" && e.Operation == "close" {
			closeReached = true
			return unix.EIO
		}
		return nil
	}})
	c, kind, err := InspectForLink(ctx, dir, freshQAState, freshQAName, time.Now().Add(time.Second))
	freshQAOwn(t, c)
	if !opened || !closeReached || c == nil || c.db == 0 || c.closed || err == nil || kind != LinkInspection(0) || statsForTest().NativeActive != 1 {
		t.Fatal("actual reached native cancellation dropped live cleanup-only owner", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatal("native cancellation evidence lost")
	}
	_ = lib.Xsqlite3_get_autocommit(c.tls, c.db)
	setSQLHooksForTest(sqlTestHooks{})
	setHooksForTest(hooks{})
	if err := c.Close(context.Background()); err != nil {
		t.Fatal("live native cancellation owner cleanup", err)
	}
	freshQAQuiet(t)
	freshQASameImage(t, dir, freshQAName, before)
}

func TestSQLiteLinkInspectReachedNativeRetainsOriginalDeadline(t *testing.T) {
	dir := qaDirectory(t)
	freshQAProfile(t, dir, "single")
	before := freshQAImage(t, dir, freshQAName)
	deadline := time.Now().Add(50 * time.Millisecond)
	reached := false
	qaFSHooks(t, hooks{Observe: func(e event) {
		if !reached && e.Role == "main" && e.Op == "open" && e.Phase == "validated" {
			reached = true
			timer := time.NewTimer(time.Until(deadline) + 20*time.Millisecond)
			defer timer.Stop()
			<-timer.C
		}
	}})
	c, kind, err := InspectForLink(freshQAContext(t), dir, freshQAState, freshQAName, deadline)
	freshQAOwn(t, c)
	if !reached {
		t.Fatal("actual native deadline boundary not reached")
	}
	linkQARefused(t, c, kind, err)
	freshQASameImage(t, dir, freshQAName, before)
	linkQANoAuthority(t, dir)
}

func TestSQLiteLinkENOSPCUncertainNativeCommitKeepsPersistentFacts(t *testing.T) {
	dir := qaDirectory(t)
	linkQASeedWAL(t, dir)
	c := qaOpen(t, dir, freshQAName, false)
	tx := qaBegin(t, c, freshQAContext(t), Write)
	if err := tx.CheckAuthorityAbsent(freshQAState); err != nil {
		t.Fatal(err)
	}
	qaDone(t, tx, "UPDATE link_qa_meta SET nonce=20")
	reached := false
	qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
		if e.Phase == "commit-after-engine" && e.Operation == "commit" && e.Code == lib.SQLITE_DONE {
			reached = true
			return &os.PathError{Op: "owned-postcommit-fault", Path: "owned-secret-path", Err: syscall.ENOSPC}
		}
		return nil
	}})
	outcome, err := tx.Commit()
	if !reached || outcome != Unknown || err == nil {
		t.Fatal("successful dispatched COMMIT fault became definite nonapplication", outcome, err)
	}
	linkQABareENOSPC(t, err, CommitPhase)
	qaSafeError(t, err, "owned-secret", "owned-postcommit")
	setSQLHooksForTest(sqlTestHooks{})
	qaClose(t, c)
	if replayOutcome, replayError := tx.Commit(); replayOutcome != Unknown || replayError != err {
		t.Fatal("terminal uncertain commit evidence changed")
	}
	freshQAQuiet(t)
	linkQAColdReceipt(t, dir, 20)
	// Native proof only: activity-level request_id/local_write_unknown mapping
	// belongs to the separately authorized private Link coordinator QA.
}

func TestSQLiteLinkAuthorityRawBasenamePolicyWithoutUTF8Gate(t *testing.T) {
	// Valid A policy is bytes/NUL/slash/role collision, not UTF-8 repair.
	for _, a := range []string{"owned-question?.json", "owned-\xff.json"} {
		dir := qaDirectory(t)
		c, kind, err := InspectForLink(freshQAContext(t), dir, a, freshQAName, time.Now().Add(time.Second))
		freshQAOwn(t, c)
		if err != nil || c != nil || kind != LinkAbsent {
			t.Fatal("native valid raw authority name rejected", err)
		}
		linkQAEmpty(t, dir)
		linkQASeedWAL(t, dir)
		c = qaOpen(t, dir, freshQAName, false)
		tx := qaBegin(t, c, freshQAContext(t), Write)
		if err := tx.CheckAuthorityAbsent(a); err != nil {
			t.Fatal("writer repaired/rejected valid raw authority name", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		qaClose(t, c)
		linkQAColdReceipt(t, dir)
	}
}

func TestSQLiteLinkInspectConcurrentActualDELETEWriterRefusesWithoutRecovery(t *testing.T) {
	// Additional exact future dependency: freshQAChildStart's existing finite
	// hot-journal mode. The child pauses with actual native BEGIN/dirty journal;
	// it alone owns that process's descriptors, and joins before terminal checks.
	dir := qaDirectory(t)
	child := freshQAChildStart(t, "hot-journal", "active-native-write", dir, freshQAName)
	trace := &freshQATrace{}
	qaFSHooks(t, hooks{Observe: trace.observeFS})
	c, kind, err := linkQAInspect(t, dir)
	linkQARefused(t, c, kind, err)
	linkQANoCreating(t, trace, true)
	linkQANoAuthority(t, dir)
	// Successful refusal does not replace/delete the writer's hot journal.
	info, err := os.Lstat(filepath.Join(dir, freshQAName+"-journal"))
	if err != nil || !info.Mode().IsRegular() || info.Size() < 28 {
		t.Fatal("inspection recovered/deleted actual writer journal", err)
	}
	child.join(t, false)
	freshQAQuiet(t)
}
