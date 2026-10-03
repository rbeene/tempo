//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

// Exactly eight native families from frozen plan b2e1ca38. Synthetic receipts
// prove native persistence and ownership, never typed Link or Service activation.
import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	lib "modernc.org/sqlite/lib"
)

func TestSQLiteFreshExclusiveActualCreateCollisionAndFallback(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"without-create", Options{ExclusiveCreate: true}},
		{"readonly", Options{Create: true, ReadOnly: true, ExclusiveCreate: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := qaDirectory(t)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			tc.opts.AcquireDeadline = time.Now().Add(250 * time.Millisecond)
			c, err := Open(freshQAContext(t), dir, freshQAName, tc.opts)
			freshQAOwn(t, c)
			if c != nil || err == nil || trace.count("main", "open", "before") != 0 {
				t.Fatal("invalid exclusive options reached main", err)
			}
			entries, e := os.ReadDir(dir)
			if e != nil || len(entries) != 0 {
				t.Fatal("invalid options changed namespace", e)
			}
			freshQAQuiet(t)
		})
	}
	t.Run("real-exclusive-fresh-inode", func(t *testing.T) {
		dir := qaDirectory(t)
		trace := &freshQATrace{}
		qaFSHooks(t, hooks{Observe: trace.observeFS})
		c, err := Open(freshQAContext(t), dir, freshQAName, Options{Create: true, ExclusiveCreate: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
		freshQAOwn(t, c)
		if err != nil {
			t.Fatal(err)
		}
		if trace.count("main", "exclusive", "created") != 1 {
			t.Fatal("no single actual creation witness")
		}
		for _, e := range trace.snapshot() {
			if e.Role == "main" && e.Op == "open" && e.Phase == "validated" && e.Flags&(unix.O_CREAT|unix.O_EXCL|unix.O_RDWR) != (unix.O_CREAT|unix.O_EXCL|unix.O_RDWR) {
				t.Fatal("validated main lacked actual exclusive RW flags")
			}
			if e.Role == "main" && e.Op == "vfs-open" && e.Phase == "returned" && e.Code == lib.SQLITE_OK && trace.count("main", "exclusive", "created") != 1 {
				t.Fatal("xOpen without actual creation")
			}
		}
		dev, ino := c.identityForTest()
		if dev == 0 || ino == 0 {
			t.Fatal("actual native identity not owned")
		}
		freshQASeed(t, c)
		if err := c.CloseDurably(freshQAContext(t)); err != nil {
			t.Fatal(err)
		}
		if freshQARows(t, dir) != 1 {
			t.Fatal("exclusive native receipt lost")
		}
		freshQAQuiet(t)
	})
	t.Run("actual-EEXIST-race-preserves-collision", func(t *testing.T) {
		dir := qaDirectory(t)
		freshQAProfile(t, dir, "single")
		displaced := filepath.Join(dir, "collision-source.sqlite3")
		before := freshQAImage(t, dir, freshQAName)
		info, err := os.Lstat(filepath.Join(dir, freshQAName))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.Rename(filepath.Join(dir, freshQAName), displaced); err != nil {
			t.Fatal(err)
		}
		trace := &freshQATrace{}
		moved := false
		qaFSHooks(t, hooks{Observe: func(e event) {
			trace.observeFS(e)
			if !moved && e.Role == "main" && e.Op == "open" && e.Phase == "before" {
				if err := os.Rename(displaced, filepath.Join(dir, freshQAName)); err != nil {
					t.Fatal(err)
				}
				moved = true
			}
		}})
		c, err := Open(freshQAContext(t), dir, freshQAName, Options{Create: true, ExclusiveCreate: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
		freshQAOwn(t, c)
		if err == nil || !moved {
			t.Fatal("post-preflight exclusive collision absent", err)
		}
		if c != nil {
			if e := c.Close(context.Background()); e != nil {
				t.Fatal(e)
			}
		}
		actualEEXIST := false
		for _, e := range trace.snapshot() {
			if e.Role == "main" && e.Op == "open" && e.Phase == "after" && e.Errno == int(unix.EEXIST) {
				actualEEXIST = true
			}
			if e.Role == "main" && e.Op == "open" && e.Phase == "validated" || e.Role == "main" && e.Op == "vfs-open" && e.Phase == "returned" && e.Code == lib.SQLITE_OK {
				t.Fatal("collision was accepted before later setup refusal")
			}
		}
		if !actualEEXIST || trace.count("main", "exclusive", "created") != 0 {
			t.Fatal("no actual exclusive syscall collision witness")
		}
		after, e := os.Lstat(filepath.Join(dir, freshQAName))
		if e != nil || !os.SameFile(info, after) {
			t.Fatal("collision inode adopted/replaced", e)
		}
		freshQASameImage(t, dir, freshQAName, before)
		qaSafeError(t, err, dir, freshQAName)
		freshQAQuiet(t)
	})
	for _, cause := range []error{unix.EINTR, unix.EIO} {
		t.Run("fixed-errno-"+cause.Error(), func(t *testing.T) {
			dir := qaDirectory(t)
			trace := &freshQATrace{}
			attempts := 0
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL, Fault: func(e sqlTestEvent) error {
				if e.Phase == "exclusive-open-before" && e.Operation == "exclusive-open" {
					attempts++
					if attempts == 1 {
						return cause
					}
				}
				return nil
			}})
			c, err := Open(freshQAContext(t), dir, freshQAName, Options{Create: true, ExclusiveCreate: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			freshQAOwn(t, c)
			if cause == unix.EINTR {
				if err != nil || attempts != 2 || trace.count("main", "exclusive", "created") != 1 {
					t.Fatal("exact exclusive EINTR retry absent", err)
				}
				qaClose(t, c)
			} else {
				if err == nil || trace.count("main", "exclusive", "created") != 0 || attempts != 1 {
					t.Fatal("non-EINTR failure weakened into another admission", err)
				}
				if c != nil {
					if e := c.Close(context.Background()); e != nil {
						t.Fatal(e)
					}
				}
				for _, e := range trace.snapshot() {
					if e.Role == "main" && e.Op == "vfs-open" && e.Phase == "returned" && e.Code == lib.SQLITE_OK {
						t.Fatal("latched refusal accepted fallback")
					}
				}
			}
			freshQAQuiet(t)
		})
	}
	t.Run("actual-upstream-low-descriptor-branch", func(t *testing.T) {
		dir := qaDirectory(t)
		child := freshQAChildStart(t, "lowfd", "actual", dir, freshQAName)
		child.join(t, false)
		info, err := os.Lstat(filepath.Join(dir, freshQAName))
		if err != nil || !info.Mode().IsRegular() {
			t.Fatal("low-FD prefix lost", err)
		}
	})
}

func TestSQLiteFreshGenuineUnusedFDCacheRefusalAndExternalLock(t *testing.T) {
	dir := qaDirectory(t)
	child := freshQAChildStart(t, "reuse", "cache-owned", dir, freshQAName)
	child.join(t, false) // Child requires nonzero upstream pUnused and actual F_GETLK.
	freshQAQuiet(t)
}

func TestSQLiteFreshInitialGuardOccupiedPathFollowerAndDeadline(t *testing.T) {
	for _, shape := range []string{"file", "directory", "symlink", "missing-parent", "sidecar-only"} {
		t.Run(shape, func(t *testing.T) {
			dir := qaDirectory(t)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			path := filepath.Join(dir, freshQAState)
			switch shape {
			case "file":
				if err := os.WriteFile(path, []byte("{\"schema_version\":1,\"owned\":true}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, "owned-sentinel"), []byte("owned directory refusal sentinel"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink("owned-unused-target", path); err != nil {
					t.Fatal(err)
				}
			case "missing-parent":
				dir = filepath.Join(dir, "absent")
			case "sidecar-only":
				if err := os.WriteFile(filepath.Join(dir, freshQAName+"-wal"), []byte("owned sidecar"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := os.ReadDir(dir)
			occupied := shape == "file" || shape == "directory" || shape == "symlink"
			var ownedBefore freshQAOccupiedPath
			if occupied {
				ownedBefore = freshQAOccupiedSnapshot(t, path)
			}
			c, err := OpenForInitialLink(freshQAContext(t), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
			freshQAOwn(t, c)
			if c != nil || err == nil || trace.count("main", "open", "validated") != 0 {
				t.Fatal("unsafe initial prefix admitted", err)
			}
			if shape == "file" || shape == "directory" || shape == "symlink" {
				var e *Error
				if !errors.As(err, &e) || e.Category != Unsafe || !errors.Is(err, os.ErrExist) {
					t.Fatal("occupied P fixed cause lost", err)
				}
				if trace.count("guard", "open", "before") != 0 || trace.count("main", "open", "before") != 0 {
					t.Fatal("occupied P reached guard/main admission")
				}
				if !reflect.DeepEqual(ownedBefore, freshQAOccupiedSnapshot(t, path)) {
					t.Fatal("occupied P inode/type/contents changed")
				}
			}
			after, readErr := os.ReadDir(dir)
			if shape == "missing-parent" {
				if !errors.Is(readErr, os.ErrNotExist) {
					t.Fatal("native opener created parent")
				}
			} else {
				// Sidecar-only may create the explicit Link guard, never main/sidecar data.
				if shape != "sidecar-only" && !reflect.DeepEqual(before, after) {
					t.Fatal("occupied namespace altered")
				}
				if _, e := os.Lstat(filepath.Join(dir, freshQAName)); !errors.Is(e, os.ErrNotExist) {
					t.Fatal("refusal created main")
				}
			}
			freshQAQuiet(t)
		})
	}
	t.Run("guard-held-through-close-and-original-budget", func(t *testing.T) {
		dir := qaDirectory(t)
		winner := freshQAInitial(t, dir)
		freshQASeed(t, winner)
		// Child holds an actual admitted guard FD and independently proves that
		// the first process's flock prevents its nonblocking exclusive lock.
		follower := freshQAChildStart(t, "follower-wait", "guard-blocked", dir, freshQAName)
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
		other, err := OpenForInitialLink(ctx, dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
		cancel()
		freshQAOwn(t, other)
		if other != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("lease admission restarted canceled budget", err)
		}
		if err := winner.CloseDurably(freshQAContext(t)); err != nil {
			t.Fatal(err)
		}
		follower.join(t, false)
		// Existing follower's native admission never creates another main.
		trace := &freshQATrace{}
		qaFSHooks(t, hooks{Observe: trace.observeFS})
		c := freshQAInitial(t, dir)
		if trace.count("main", "exclusive", "created") != 0 {
			t.Fatal("follower created second inode")
		}
		qaClose(t, c)
		freshQAQuiet(t)
	})
	t.Run("native-closed-lease-is-still-held", func(t *testing.T) {
		dir := qaDirectory(t)
		winner := freshQAInitial(t, dir)
		freshQASeed(t, winner)
		blocked := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)
		var releaseOnce, joinOnce sync.Once
		ctx := freshQAContext(t)
		qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
			if e.Phase == "durable-native-closed" && e.Operation == "close-durably" {
				close(blocked)
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
		}})
		join := func() {
			releaseOnce.Do(func() { close(release) })
			joinOnce.Do(func() {
				if err := <-done; err != nil {
					t.Errorf("owned durable goroutine terminal: %v", err)
				}
			})
		}
		t.Cleanup(join)
		go func() { done <- winner.CloseDurably(ctx) }()
		select {
		case <-blocked:
		case err := <-done:
			joinOnce.Do(func() {})
			t.Fatal("native-closed boundary absent", err)
		case <-ctx.Done():
			t.Fatal("native-closed handshake deadline")
		}
		probe, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		other, err := Open(probe, dir, freshQAName, Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
		cancel()
		freshQAOwn(t, other)
		if other != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("retained lease allowed same-process reopen", err)
		}
		join()
		setSQLHooksForTest(sqlTestHooks{})
		freshQAQuiet(t)
	})
	for _, stateName := range []string{"owned-é-state", "owned-" + string([]byte{0xff}) + "-state"} {
		t.Run("raw-state-basename", func(t *testing.T) {
			dir := qaDirectory(t)
			baseline := freshQARawNameOSBaseline(t, dir, stateName)
			if baseline.AuthorityErrno != int(unix.ENOENT) || baseline.GuardErrno != int(unix.ENOENT) {
				t.Fatal("OS raw-basename absence control differs from qualified baseline")
			}
			if baseline.OpenErrno != 0 {
				// Only the fixed actual EPERM counterexample is qualified here.
				// No UTF-8 predicate, arbitrary errno, or generic IO refusal passes.
				if stateName != "owned-"+string([]byte{0xff})+"-state" || baseline.OpenErrno != int(unix.EPERM) || baseline.ExactEntry || !baseline.Empty {
					t.Fatal("OS raw-basename refusal differs from qualified EPERM control")
				}
				freshQAQuiet(t)
				var before unix.Stat_t
				if err := unix.Lstat(dir, &before); err != nil {
					t.Fatal("owned raw-basename producer root snapshot failed")
				}
				entries, err := os.ReadDir(dir)
				if err != nil || len(entries) != 0 {
					t.Fatal("owned raw-basename producer fixture not empty")
				}
				trace := &freshQATrace{}
				qaFSHooks(t, hooks{Observe: trace.observeFS})
				c, err := OpenForInitialLink(freshQAContext(t), dir, stateName, freshQAName, time.Now().Add(250*time.Millisecond))
				freshQAOwn(t, c)
				if c != nil {
					qaClose(t, c)
				}
				freshQAQuiet(t)
				var after unix.Stat_t
				if e := unix.Lstat(dir, &after); e != nil {
					t.Fatal("owned raw-basename producer root recheck failed")
				}
				entries, readErr := os.ReadDir(dir)
				if readErr != nil || len(entries) != 0 || before.Dev != after.Dev || before.Ino != after.Ino || before.Mode != after.Mode || before.Uid != after.Uid || before.Gid != after.Gid || before.Nlink != after.Nlink || before.Size != after.Size {
					t.Error("OS-refused raw basename changed producer root or created authority/guard/main/roles")
				}
				guardBefore, guardAfter := 0, 0
				for _, e := range trace.snapshot() {
					if e.Role == "guard" && e.Op == "open" && e.Phase == "before" {
						guardBefore++
						if e.FD != -1 || e.Flags != unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC {
							t.Error("OS-refused guard dispatch lost exact native flags")
						}
					}
					if e.Role == "guard" && e.Op == "open" && e.Phase == "after" {
						guardAfter++
						if e.FD != -1 || e.Errno != baseline.OpenErrno {
							t.Error("producer raw guard errno differs from actual independent OS refusal")
						}
					}
					// closeNative emits before and Close emits closed even when the
					// failed guard admission never created a native database handle.
					// Permit only these exact terminal notifications, never role I/O.
					terminalMainClose := e.Role == "main" && e.Op == "close" &&
						(e.Phase == "before" || e.Phase == "closed") && e.FD == -1 && e.Flags == 0 && e.Errno == 0
					if e.Role == "guard" && (e.Phase == "validated" || e.Phase == "quarantined" || e.Op == "flock" || e.Op == "fsync") || e.Role == "main" && !terminalMainClose || e.Role == "wal" || e.Role == "shm" || e.Role == "journal" {
						t.Error("OS-refused raw basename admitted guard or reached database family", e.Role, e.Op, e.Phase)
					}
				}
				var failure *Error
				if c != nil || err == nil || !errors.As(err, &failure) || failure.Phase != Admission || failure.Category != IO || failure.Code != 0 || failure.Cleanup != nil || guardBefore != 1 || guardAfter != 1 || trace.count("main", "close", "before") != 1 || trace.count("main", "close", "closed") != 1 {
					t.Fatal("actual OS EPERM did not yield exact terminal admission IO/code0 refusal")
				}
				qaSafeError(t, err, dir, freshQAName)
				return // This subcase asserted the actual refusal and unchanged namespace.
			}
			if !baseline.ExactEntry || baseline.Empty {
				t.Fatal("OS-supported raw basename control did not preserve exact bytes")
			}
			c, err := OpenForInitialLink(freshQAContext(t), dir, stateName, freshQAName, time.Now().Add(250*time.Millisecond))
			freshQAOwn(t, c)
			if err != nil {
				t.Fatal("raw basename bytes normalized/rejected", err)
			}
			if _, err := os.Lstat(filepath.Join(dir, stateName+".lock")); err != nil {
				t.Fatal("exact raw guard basename absent", err)
			}
			qaClose(t, c)
			freshQAQuiet(t)
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal("OS-supported producer raw guard listing failed")
			}
			guardEntries := 0
			for _, entry := range entries {
				name := entry.Name()
				if name == stateName+".lock" {
					guardEntries++
				} else if name != freshQAName && name != freshQAName+"-wal" && name != freshQAName+"-shm" && name != freshQAName+"-journal" {
					t.Fatal("OS-supported raw guard namespace contains altered basename")
				}
			}
			if guardEntries != 1 {
				t.Fatal("OS-supported producer guard bytes differ from exact OS control")
			}
		})
	}
	for _, input := range []string{"nil-context", "zero-deadline", "expired-deadline", "canceled"} {
		t.Run(input, func(t *testing.T) {
			dir := qaDirectory(t)
			ctx := freshQAContext(t)
			deadline := time.Now().Add(250 * time.Millisecond)
			switch input {
			case "nil-context":
				ctx = nil
			case "zero-deadline":
				deadline = time.Time{}
			case "expired-deadline":
				deadline = time.Now().Add(-time.Second)
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			c, err := OpenForInitialLink(ctx, dir, freshQAState, freshQAName, deadline)
			freshQAOwn(t, c)
			if c != nil || err == nil {
				t.Fatal("invalid initial admission input accepted", input, err)
			}
			entries, e := os.ReadDir(dir)
			if e != nil || len(entries) != 0 {
				t.Fatal("invalid admission created guard/main", e)
			}
			freshQAQuiet(t)
		})
	}
	for _, args := range [][2]string{{"../bad", freshQAName}, {freshQAName, freshQAName}, {freshQAName + "-wal", freshQAName}, {freshQAState, "../bad"}} {
		dir := qaDirectory(t)
		c, err := OpenForInitialLink(freshQAContext(t), dir, args[0], args[1], time.Now().Add(250*time.Millisecond))
		freshQAOwn(t, c)
		if c != nil || err == nil {
			t.Fatal("unsafe/distinct basename guard accepted", err)
		}
		entries, e := os.ReadDir(dir)
		if e != nil || len(entries) != 0 {
			t.Fatal("invalid guard names created roles", e)
		}
		freshQAQuiet(t)
	}
}

func TestSQLiteFreshPristineProfilesRecoveryIdentityAndCleanup(t *testing.T) {
	for _, profile := range []string{"zero", "single", "wal-empty"} {
		t.Run("positive-"+profile, func(t *testing.T) {
			dir := qaDirectory(t)
			freshQAProfile(t, dir, profile)
			original, err := os.Lstat(filepath.Join(dir, freshQAName))
			if err != nil {
				t.Fatal(err)
			}
			if profile == "zero" && original.Size() != 0 || profile == "single" && original.Size() != 4096 {
				t.Fatal("actual pristine physical profile not reached")
			}
			if profile == "single" {
				before := freshQAImage(t, dir, freshQAName)
				reader, e := Open(freshQAContext(t), dir, freshQAName, Options{ReadOnly: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
				freshQAOwn(t, reader)
				if e == nil {
					if reader != nil {
						qaClose(t, reader)
					}
					t.Fatal("ordinary readonly Open completed DELETE prefix")
				}
				if reader != nil {
					if e := reader.Close(context.Background()); e != nil {
						t.Fatal(e)
					}
				}
				freshQASameImage(t, dir, freshQAName, before)
				freshQAQuiet(t)
			}
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL})
			c := freshQAInitial(t, dir)
			if trace.count("main", "exclusive", "created") != 0 || trace.sqlCount("initial-probe-closed", "initial-link") != 1 {
				t.Fatal("existing profile recreated or probe lifetime absent")
			}
			for _, e := range trace.snapshot() {
				if e.Role == "main" && e.Op == "open" && e.Phase == "validated" && e.Flags&unix.O_CREAT != 0 {
					t.Fatal("existing recovery used CREATE")
				}
			}
			now, e := os.Lstat(filepath.Join(dir, freshQAName))
			if e != nil || !os.SameFile(original, now) {
				t.Fatal("pristine recovery changed main inode", e)
			}
			freshQASeed(t, c)
			if err := c.CloseDurably(freshQAContext(t)); err != nil {
				t.Fatal(err)
			}
			if freshQARows(t, dir) != 1 {
				t.Fatal("pristine recovery lost native receipt")
			}
			freshQAQuiet(t)
		})
	}
	// Each negative begins with a genuine, conclusively closed SQLite WAL
	// profile. The unsupported-size case is a complete native8192 page;
	// other cases mutate one fraction field or truncate an actual4096 page.
	for _, defect := range []string{"truncated-20", "truncated-4095", "declared-page-size", "maximum-fraction", "minimum-fraction", "leaf-fraction"} {
		for _, roles := range []string{"none", "wal", "shm", "journal", "all"} {
			t.Run("negative-WAL-header-"+defect+"-roles-"+roles, func(t *testing.T) {
				dir := qaDirectory(t)
				// Build a complete page in DELETE first. Transition to WAL
				// without any WAL transaction; NO_CKPT_ON_CLOSE cannot be
				// treated as proof of a checkpoint or sidecar removal.
				wantSize := 4096
				r := freshQARawRoot(t, dir, freshQAName)
				h := freshQARawOpen(t, r)
				freshQARawSQL(t, h, "PRAGMA journal_mode=DELETE")
				if defect == "declared-page-size" {
					wantSize = 8192
					freshQARawSQL(t, h, "PRAGMA page_size=8192")
				} else {
					freshQARawSQL(t, h, "PRAGMA page_size=4096")
				}
				freshQARawSQL(t, h, "PRAGMA application_id=1")
				freshQARawSQL(t, h, "PRAGMA application_id=0")
				freshQARawSQL(t, h, "PRAGMA journal_mode=WAL")
				if err := h.close(); err != nil {
					t.Fatal(err)
				}
				freshQARawRelease(t, r)
				freshQAQuiet(t)
				mainPath := filepath.Join(dir, freshQAName)
				image := freshQAImage(t, dir, freshQAName)
				main := image[""]
				if len(image) != 1 || len(main) != wantSize || !bytes.Equal(main[:16], []byte("SQLite format 3\x00")) ||
					main[16] != byte(wantSize>>8) || main[17] != 0 || main[18] != 2 || main[19] != 2 ||
					main[21] != 64 || main[22] != 32 || main[23] != 32 {
					t.Fatal("genuine complete WAL main and absent roles fixture not reached")
				}
				switch defect {
				case "truncated-20":
					if err := os.Truncate(mainPath, 20); err != nil {
						t.Fatal("owned actual WAL header truncate failed")
					}
				case "truncated-4095":
					if err := os.Truncate(mainPath, 4095); err != nil {
						t.Fatal("owned complete-header incomplete-page truncate failed")
					}
				case "declared-page-size":
					// Already a genuine complete 8192-byte native WAL page.
					// Do not confound unsupported page policy with truncation.
				case "maximum-fraction":
					main[21] = 63
				case "minimum-fraction":
					main[22] = 31
				case "leaf-fraction":
					main[23] = 31
				}
				if defect == "maximum-fraction" || defect == "minimum-fraction" || defect == "leaf-fraction" {
					if err := os.WriteFile(mainPath, main, 0600); err != nil {
						t.Fatal("owned isolated WAL header mutation failed")
					}
				}
				for _, role := range []string{"wal", "shm", "journal"} {
					if roles == role || roles == "all" {
						if err := os.WriteFile(filepath.Join(dir, freshQAName+"-"+role), []byte("owned unproved "+role), 0600); err != nil {
							t.Fatal("owned unproved role fixture write failed")
						}
					}
				}
				type physicalRole struct {
					Entry freshQAOccupiedPath
					UID   uint32
					GID   uint32
					Links uint64
					Size  int64
				}
				snapshot := func() map[string]physicalRole {
					freshQAQuiet(t) // No duplicate OS main/role descriptor while native is live.
					result := map[string]physicalRole{}
					for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
						path := filepath.Join(dir, freshQAName+suffix)
						var stat unix.Stat_t
						if err := unix.Lstat(path, &stat); errors.Is(err, unix.ENOENT) {
							continue
						} else if err != nil {
							t.Fatal("owned physical family lstat failed")
						}
						if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0777 != 0600 || stat.Uid != uint32(unix.Geteuid()) || stat.Nlink != 1 {
							t.Fatal("owned physical family private fixture not reached")
						}
						result[suffix] = physicalRole{Entry: freshQAOccupiedSnapshot(t, path), UID: stat.Uid, GID: stat.Gid, Links: uint64(stat.Nlink), Size: stat.Size}
					}
					return result
				}
				before := snapshot()
				trace := &freshQATrace{}
				qaFSHooks(t, hooks{Observe: trace.observeFS})
				c, err := OpenForInitialLink(freshQAContext(t), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
				freshQAOwn(t, c)
				if c != nil {
					qaClose(t, c)
				}
				after := snapshot() // Checked close and terminal registry proof precede OS reads.
				if !reflect.DeepEqual(before, after) {
					t.Error("malformed WAL refusal changed physical family identity/mode/owner/group/links/size/bytes")
				}
				for _, e := range trace.snapshot() {
					if e.Role != "wal" && e.Role != "shm" && e.Role != "journal" {
						continue
					}
					// These phases precede actual dispatch or record an admitted FD/
					// completed unlink. Refused intent and truthful stat/access are allowed.
					if e.Op == "vfs-open" && e.Phase == "role" ||
						e.Op == "open" && (e.Phase == "before" || e.Phase == "after" || e.Phase == "validated" || e.Phase == "quarantined") ||
						e.Op == "unlink" && (e.Phase == "before" || e.Phase == "after") {
						t.Error("malformed WAL reached sidecar dispatch", e.Role, e.Op, e.Phase, e.Flags, e.Errno)
					}
				}
				if err == nil {
					t.Fatal("malformed physical WAL profile admitted")
				}
				qaSafeError(t, err, dir, freshQAName)
				freshQAQuiet(t)
			})
		}
	}
	t.Run("positive-multiple-page-WAL-main", func(t *testing.T) {
		dir := qaDirectory(t)
		// Independent genuine SQLite writes; no synthetic header or I/O return.
		r := freshQARawRoot(t, dir, freshQAName)
		h := freshQARawOpen(t, r)
		freshQARawSQL(t, h, "PRAGMA journal_mode=DELETE")
		freshQARawSQL(t, h, "PRAGMA page_size=4096")
		freshQARawSQL(t, h, "CREATE TABLE fresh_profile_large(payload BLOB NOT NULL) STRICT")
		freshQARawSQL(t, h, "INSERT INTO fresh_profile_large VALUES(zeroblob(16384))")
		// Complete the actual multi-page history in DELETE, then transition.
		// No WAL transaction or close-time checkpoint premise is used.
		freshQARawSQL(t, h, "PRAGMA journal_mode=WAL")
		if err := h.close(); err != nil {
			t.Fatal(err)
		}
		freshQARawRelease(t, r)
		freshQAQuiet(t)
		image := freshQAImage(t, dir, freshQAName)
		before := image[""]
		if len(image) != 1 || len(before) <= 4096 || !bytes.Equal(before[:16], []byte("SQLite format 3\x00")) ||
			before[16] != 16 || before[17] != 0 || before[18] != 2 || before[19] != 2 ||
			before[21] != 64 || before[22] != 32 || before[23] != 32 {
			t.Fatal("genuine multiple-page supported WAL main not reached")
		}
		original, err := os.Lstat(filepath.Join(dir, freshQAName))
		if err != nil {
			t.Fatal(err)
		}
		trace := &freshQATrace{}
		qaFSHooks(t, hooks{Observe: trace.observeFS})
		c := freshQAInitial(t, dir)
		tx := qaBegin(t, c, freshQAContext(t), Read)
		if capacityQAScalar(t, tx, "SELECT count(*) FROM fresh_profile_large WHERE length(payload)=16384") != 1 {
			t.Fatal("supported multiple-page WAL recovery lost native payload")
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		qaClose(t, c)
		freshQAQuiet(t)
		now, err := os.Lstat(filepath.Join(dir, freshQAName))
		if err != nil || !os.SameFile(original, now) || trace.count("main", "exclusive", "created") != 0 {
			t.Fatal("supported multiple-page WAL recovery recreated main")
		}
	})
	for _, profile := range []string{"page-size", "application-id", "user-version", "schema-version", "auto-vacuum", "objects", "allocated-free", "malformed", "wal-sidecar", "shm-sidecar", "journal", "permissions", "hardlink"} {
		t.Run("negative-"+profile, func(t *testing.T) {
			dir := qaDirectory(t)
			if profile == "malformed" {
				if err := os.WriteFile(filepath.Join(dir, freshQAName), bytes.Repeat([]byte{0x71}, 4096), 0600); err != nil {
					t.Fatal(err)
				}
			} else if profile == "wal-sidecar" || profile == "shm-sidecar" || profile == "journal" || profile == "permissions" || profile == "hardlink" {
				freshQAProfile(t, dir, "single")
				switch profile {
				case "permissions":
					if err := os.Chmod(filepath.Join(dir, freshQAName), 0644); err != nil {
						t.Fatal(err)
					}
				case "hardlink":
					if err := os.Link(filepath.Join(dir, freshQAName), filepath.Join(dir, "owned-alias")); err != nil {
						t.Fatal(err)
					}
				default:
					suffix := "-journal"
					if profile == "wal-sidecar" {
						suffix = "-wal"
					}
					if profile == "shm-sidecar" {
						suffix = "-shm"
					}
					if err := os.WriteFile(filepath.Join(dir, freshQAName+suffix), []byte("owned unproved role"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				freshQAProfile(t, dir, profile)
			}
			before := freshQAImage(t, dir, freshQAName)
			c, err := OpenForInitialLink(freshQAContext(t), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
			freshQAOwn(t, c)
			if err == nil {
				if c != nil {
					qaClose(t, c)
				}
			}
			if c != nil {
				if e := c.Close(context.Background()); e != nil {
					t.Fatal(e)
				}
			}
			freshQASameImage(t, dir, freshQAName, before)
			if err == nil {
				t.Fatal("unproved DELETE prefix recovered", profile)
			}
			qaSafeError(t, err, dir, freshQAName)
			freshQAQuiet(t)
		})
	}
	t.Run("late-owned-WAL-refusal-remains-after-role-removed", func(t *testing.T) {
		dir := qaDirectory(t)
		freshQAProfile(t, dir, "single")
		freshQAQuiet(t)
		before := freshQAImage(t, dir, freshQAName)
		if len(before) != 1 || len(before[""]) != 4096 {
			t.Fatal("late-role genuine DELETE/no-role fixture not reached")
		}
		mainBefore := freshQAOccupiedSnapshot(t, filepath.Join(dir, freshQAName))
		var mainBeforeStat unix.Stat_t
		if unix.Lstat(filepath.Join(dir, freshQAName), &mainBeforeStat) != nil {
			t.Fatal("late-role original main metadata snapshot failed")
		}
		walPath := filepath.Join(dir, freshQAName+"-wal")
		walBytes := []byte("owned causally late unproved WAL")
		var owned unix.Stat_t
		created, removed := false, false
		fixtureFailure := ""
		trace := &freshQATrace{}
		qaFSHooks(t, hooks{Observe: func(e event) {
			trace.observeFS(e)
			if !created && fixtureFailure == "" && e.Role == "wal" && e.Op == "stat" && e.Phase == "before" {
				// The actual stat callback has finished preflight and has not
				// dispatched fstatat. A genuinely new O_EXCL WAL inode has no
				// native owner; closing this QA-created FD cannot close an alias
				// of the live main. No main/SHM FD or reentrant SQL is used.
				f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
				if err != nil {
					fixtureFailure = "late owned WAL exclusive create failed"
					return
				}
				n, writeErr := f.Write(walBytes)
				statErr := unix.Fstat(int(f.Fd()), &owned)
				closeErr := f.Close()
				if n != len(walBytes) || writeErr != nil || statErr != nil || closeErr != nil ||
					owned.Mode&unix.S_IFMT != unix.S_IFREG || owned.Mode&0777 != 0600 || owned.Uid != uint32(unix.Geteuid()) || owned.Nlink != 1 || owned.Size != int64(len(walBytes)) {
					fixtureFailure = "late owned WAL write/identity/checked close premise failed"
					return
				}
				created = true
			}
			if created && !removed && fixtureFailure == "" && e.Role == "wal" && e.Phase == "initial-probe-refused" && (e.Op == "vfs-open" || e.Op == "open" || e.Op == "unlink") {
				// Remove only the exact inode this QA just created, after the
				// native sidecar attempt was refused before dispatch. Its absence
				// must not clear the recorded refusal and permit initialization.
				var named unix.Stat_t
				if unix.Lstat(walPath, &named) != nil || named.Dev != owned.Dev || named.Ino != owned.Ino || named.Mode != owned.Mode || named.Uid != owned.Uid || named.Gid != owned.Gid || named.Nlink != owned.Nlink || named.Size != owned.Size {
					fixtureFailure = "late owned WAL inode changed before explicit QA removal"
					return
				}
				if err := os.Remove(walPath); err != nil {
					fixtureFailure = "late owned WAL explicit QA removal failed"
					return
				}
				removed = true
			}
		}})
		qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL})
		c, err := OpenForInitialLink(freshQAContext(t), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
		freshQAOwn(t, c)
		if c != nil {
			qaClose(t, c)
		}
		freshQAQuiet(t)
		if fixtureFailure != "" {
			t.Fatal(fixtureFailure) // Fixture failure is not evidence of producer RED.
		}
		refused := trace.count("wal", "vfs-open", "initial-probe-refused") + trace.count("wal", "open", "initial-probe-refused") + trace.count("wal", "unlink", "initial-probe-refused")
		if !created || !removed || refused == 0 {
			t.Fatal("actual late-role injection/refused attempt/owned removal seam not reached")
		}
		freshQASameImage(t, dir, freshQAName, before)
		mainAfter := freshQAOccupiedSnapshot(t, filepath.Join(dir, freshQAName))
		var mainAfterStat unix.Stat_t
		if unix.Lstat(filepath.Join(dir, freshQAName), &mainAfterStat) != nil || mainAfterStat.Uid != mainBeforeStat.Uid || mainAfterStat.Gid != mainBeforeStat.Gid || mainAfterStat.Nlink != mainBeforeStat.Nlink || mainAfterStat.Size != mainBeforeStat.Size {
			t.Error("late sticky refusal changed original main owner/group/links/size")
		}
		if !reflect.DeepEqual(mainBefore, mainAfter) {
			t.Error("late sticky refusal changed original main identity/mode/bytes")
		}
		for _, e := range trace.snapshot() {
			if e.Role != "wal" && e.Role != "shm" && e.Role != "journal" {
				continue
			}
			if e.Op == "vfs-open" && e.Phase == "role" ||
				e.Op == "open" && (e.Phase == "before" || e.Phase == "after" || e.Phase == "validated" || e.Phase == "quarantined") ||
				e.Op == "unlink" && (e.Phase == "before" || e.Phase == "after") {
				t.Error("late sticky refusal reached native sidecar dispatch", e.Role, e.Op, e.Phase)
			}
		}
		if c != nil || err == nil || !errors.Is(err, ErrUnsafe) ||
			trace.sqlCount("initial-probe-verified", "initial-link") != 0 || trace.sqlCount("initial-reopen-before", "initial-link") != 0 || trace.sqlCount("initial-wal-ready", "initial-link") != 0 {
			t.Fatal("removed late role cleared refusal and permitted initial recovery")
		}
		qaSafeError(t, err, dir, freshQAName)
	})
	t.Run("actual-native-hot-journal", func(t *testing.T) {
		dir := qaDirectory(t)
		child := freshQAChildStart(t, "hot-journal", "spilled", dir, freshQAName)
		child.join(t, true)
		before := freshQAImage(t, dir, freshQAName)
		journal := before["-journal"]
		magic := []byte{0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7}
		if len(journal) <= 512 || !bytes.Equal(journal[:8], magic) {
			t.Fatal("genuine native hot-journal header not reached")
		}
		c, err := OpenForInitialLink(freshQAContext(t), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
		freshQAOwn(t, c)
		if err == nil {
			t.Fatal("unproved hot-journal prefix completed")
		}
		if c != nil {
			if e := c.Close(context.Background()); e != nil {
				t.Fatal(e)
			}
		}
		freshQASameImage(t, dir, freshQAName, before)
		freshQAQuiet(t)
	})
	t.Run("same-inode-content-change-after-probe-close", func(t *testing.T) {
		dir := qaDirectory(t)
		freshQAProfile(t, dir, "single")
		before := freshQAImage(t, dir, freshQAName)
		changed := false
		trace := &freshQATrace{}
		qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
			trace.observeSQL(e)
			if !changed && e.Phase == "initial-probe-closed" && e.Operation == "initial-link" {
				fd, err := unix.Open(filepath.Join(dir, freshQAName), unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
				if err != nil {
					t.Fatal(err)
				}
				n, writeErr := unix.Pwrite(fd, []byte{0, 0, 0, 7}, 60)
				closeErr := unix.Close(fd)
				if n != 4 || writeErr != nil || closeErr != nil {
					t.Fatal("owned closed-file content race", writeErr, closeErr)
				}
				changed = true
			}
		}})
		c, err := OpenForInitialLink(freshQAContext(t), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
		freshQAOwn(t, c)
		if !changed || err == nil || trace.sqlCount("initial-reopen-before", "initial-link") != 0 {
			t.Fatal("bounded byte witness missed closed-file content race", err)
		}
		if c != nil {
			if e := c.Close(context.Background()); e != nil {
				t.Fatal(e)
			}
		}
		want := append([]byte(nil), before[""]...)
		copy(want[60:64], []byte{0, 0, 0, 7})
		before[""] = want
		freshQASameImage(t, dir, freshQAName, before)
		freshQAQuiet(t)
	})
	t.Run("probe-reopen-retains-original-admission-deadline", func(t *testing.T) {
		dir := qaDirectory(t)
		freshQAProfile(t, dir, "single")
		before := freshQAImage(t, dir, freshQAName)
		deadline := time.Now().Add(75 * time.Millisecond)
		reached := false
		qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
			if e.Phase == "initial-reopen-before" && e.Operation == "initial-link" {
				reached = true
				timer := time.NewTimer(time.Until(deadline) + 10*time.Millisecond)
				defer timer.Stop()
				<-timer.C
			}
		}})
		c, err := OpenForInitialLink(freshQAContext(t), dir, freshQAState, freshQAName, deadline)
		freshQAOwn(t, c)
		if !reached || err == nil {
			t.Fatal("reopen restarted exhausted original budget", err)
		}
		if c != nil {
			if e := c.Close(context.Background()); e != nil {
				t.Fatal(e)
			}
		}
		freshQASameImage(t, dir, freshQAName, before)
		freshQAQuiet(t)
	})
	for _, phase := range []string{"initial-probe-closed", "initial-reopen-before"} {
		t.Run("fault-owned-"+phase, func(t *testing.T) {
			dir := qaDirectory(t)
			freshQAProfile(t, dir, "single")
			before := freshQAImage(t, dir, freshQAName)
			reached := false
			qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
				if e.Phase == phase && e.Operation == "initial-link" {
					reached = true
					return unix.EIO
				}
				return nil
			}})
			c, err := OpenForInitialLink(freshQAContext(t), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
			freshQAOwn(t, c)
			if !reached || err == nil {
				t.Fatal("retained probe fault lost", err)
			}
			if c != nil {
				if e := c.Close(context.Background()); e != nil {
					t.Fatal(e)
				}
			}
			setSQLHooksForTest(sqlTestHooks{})
			freshQASameImage(t, dir, freshQAName, before)
			freshQAQuiet(t)
			next := freshQAInitial(t, dir)
			qaClose(t, next)
			freshQAQuiet(t)
		})
		t.Run("replacement-"+phase, func(t *testing.T) {
			dir := qaDirectory(t)
			freshQAProfile(t, dir, "single")
			original := freshQAImage(t, dir, freshQAName)
			moved := false
			displaced := filepath.Join(dir, "owned-pristine-displaced")
			qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
				if !moved && e.Phase == phase && e.Operation == "initial-link" {
					if err := os.Rename(filepath.Join(dir, freshQAName), displaced); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(dir, freshQAName), original[""], 0600); err != nil {
						t.Fatal(err)
					}
					moved = true
				}
			}})
			c, err := OpenForInitialLink(freshQAContext(t), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
			freshQAOwn(t, c)
			if !moved || err == nil {
				t.Fatal("probe-close replacement accepted", err)
			}
			if c != nil {
				if e := c.Close(context.Background()); e != nil {
					t.Fatal(e)
				}
			}
			setSQLHooksForTest(sqlTestHooks{})
			freshQASameImage(t, dir, freshQAName, original)
			if err := os.Remove(filepath.Join(dir, freshQAName)); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(displaced, filepath.Join(dir, freshQAName)); err != nil {
				t.Fatal(err)
			}
			freshQAQuiet(t)
		})
		t.Run("cancel-owned-"+phase, func(t *testing.T) {
			dir := qaDirectory(t)
			freshQAProfile(t, dir, "single")
			ctx, cancel := context.WithCancel(freshQAContext(t))
			t.Cleanup(cancel)
			reached := false
			qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
				if e.Phase == phase && e.Operation == "initial-link" {
					reached = true
					cancel()
				}
			}})
			c, err := OpenForInitialLink(ctx, dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
			freshQAOwn(t, c)
			if !reached || !errors.Is(err, context.Canceled) {
				t.Fatal("retained probe cancellation lost", err)
			}
			if c != nil {
				if e := c.Close(context.Background()); e != nil {
					t.Fatal(e)
				}
			}
			setSQLHooksForTest(sqlTestHooks{})
			freshQAQuiet(t)
			next := freshQAInitial(t, dir)
			qaClose(t, next)
			freshQAQuiet(t)
		})
	}
}

func TestSQLiteFreshGuardActualFDAliasPoisonAndExternalPOSIXLock(t *testing.T) {
	for _, role := range []string{"main", "shm"} {
		t.Run(role, func(t *testing.T) {
			dir := qaDirectory(t)
			ownerName := "other-owned.sqlite3"
			owner := qaOpen(t, dir, ownerName, true)
			tx := qaBegin(t, owner, freshQAContext(t), Write)
			qaDone(t, tx, "CREATE TABLE lock_owner(id INTEGER PRIMARY KEY)")
			// Keep the real WAL writer active for both its main shared and SHM write lock.
			targetDir := qaDirectory(t)
			sourceName := ownerName
			if role == "shm" {
				sourceName += "-shm"
			}
			target := filepath.Join(targetDir, freshQAState+".lock")
			moved := false
			quarantined := false
			fd := -1
			// Existing safe guard opens without O_EXCL; otherwise the race merely
			// produces EEXIST and never reaches the required successful alias FD.
			if err := os.WriteFile(target, []byte("owned guard"), 0600); err != nil {
				t.Fatal(err)
			}
			freshQALockWitness(t, dir, sourceName, role)
			qaFSHooks(t, hooks{Observe: func(e event) {
				if e.Role == "guard" && e.Op == "open" && e.Phase == "before" && !moved {
					if err := os.Rename(filepath.Join(dir, sourceName), target); err != nil {
						t.Fatal(err)
					}
					moved = true
				}
				if e.Role == "guard" && e.Op == "open" && e.Phase == "quarantined" {
					quarantined = true
					fd = e.FD
					freshQALockWitness(t, targetDir, freshQAState+".lock", role)
				}
			}})
			c, err := OpenForInitialLink(freshQAContext(t), targetDir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
			freshQAOwn(t, c)
			if err == nil || !moved || !quarantined || fd < 0 {
				t.Fatal("actual opened guard alias rejection not reached", err)
			}
			if c != nil {
				if e := c.Close(context.Background()); e != nil {
					t.Fatal(e)
				}
			}
			s := statsForTest()
			if !s.Poisoned || s.RejectedFDs < 1 || s.NativeActive < 1 {
				t.Fatalf("alias FD closed before native quiescence: %+v", s)
			}
			freshQALockWitness(t, targetDir, freshQAState+".lock", role)
			rejected, e := Open(freshQAContext(t), qaDirectory(t), "blocked.sqlite3", Options{Create: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
			freshQAOwn(t, rejected)
			if e == nil || rejected != nil {
				t.Fatal("poison allowed new native admission", e)
			}
			setHooksForTest(hooks{})
			if err := os.Rename(target, filepath.Join(dir, sourceName)); err != nil {
				t.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			qaClose(t, owner)
			freshQAQuiet(t)
		})
	}
	for _, phase := range []string{"fsync-before", "fsync-after"} {
		for _, op := range []string{"guard-file", "guard-parent"} {
			t.Run(phase+"/"+op, func(t *testing.T) {
				dir := qaDirectory(t)
				reached := false
				trace := &freshQATrace{}
				qaFSHooks(t, hooks{Observe: trace.observeFS})
				qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
					if e.Phase == phase && e.Operation == op {
						reached = true
						return unix.EIO
					}
					return nil
				}})
				c, err := OpenForInitialLink(freshQAContext(t), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
				freshQAOwn(t, c)
				if !reached || err == nil || trace.count("main", "open", "validated") != 0 {
					t.Fatal("guard durability fault created native main", err)
				}
				if c != nil {
					if e := c.Close(context.Background()); e != nil {
						t.Fatal(e)
					}
				}
				setSQLHooksForTest(sqlTestHooks{})
				setHooksForTest(hooks{})
				freshQAQuiet(t)
				next := freshQAInitial(t, dir)
				qaClose(t, next)
				freshQAQuiet(t)
			})
		}
	}
	t.Run("registered-guard-protects-later-native-alias", func(t *testing.T) {
		dir := qaDirectory(t)
		c := freshQAInitial(t, dir)
		other := qaDirectory(t)
		guard := filepath.Join(dir, freshQAState+".lock")
		if err := os.Rename(guard, filepath.Join(other, "guard-alias.sqlite3")); err != nil {
			t.Fatal(err)
		}
		trace := &freshQATrace{}
		qaFSHooks(t, hooks{Observe: trace.observeFS})
		alias, err := Open(freshQAContext(t), other, "guard-alias.sqlite3", Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
		freshQAOwn(t, alias)
		if err == nil || alias != nil || trace.count("main", "open", "validated") != 0 {
			t.Fatal("registered guard was accepted as native main", err)
		}
		if err := os.Rename(filepath.Join(other, "guard-alias.sqlite3"), guard); err != nil {
			t.Fatal(err)
		}
		qaClose(t, c)
		freshQAQuiet(t)
	})
	t.Run("cancel-after-registered-guard", func(t *testing.T) {
		dir := qaDirectory(t)
		ctx, cancel := context.WithCancel(freshQAContext(t))
		t.Cleanup(cancel)
		reached := false
		qaFSHooks(t, hooks{Observe: func(e event) {
			if e.Role == "guard" && e.Op == "open" && e.Phase == "validated" {
				reached = true
				cancel()
			}
		}})
		c, err := OpenForInitialLink(ctx, dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
		freshQAOwn(t, c)
		if !reached || !errors.Is(err, context.Canceled) {
			t.Fatal("registered guard cancellation lost", err)
		}
		if c != nil {
			if e := c.Close(context.Background()); e != nil {
				t.Fatal(e)
			}
		}
		setHooksForTest(hooks{})
		freshQAQuiet(t)
		next := freshQAInitial(t, dir)
		qaClose(t, next)
		freshQAQuiet(t)
	})
}

func TestSQLiteFreshCloseDurablyWALFailuresEligibilityAndTerminalOwnership(t *testing.T) {
	t.Run("real-WAL-receipt-native-close-files-parent-release", func(t *testing.T) {
		dir := qaDirectory(t)
		trace := &freshQATrace{}
		nativeClosed := false
		barrierAttempt := false
		var barrierOpens [4]int
		qaFSHooks(t, hooks{Observe: func(e event) {
			trace.observeFS(e)
			if barrierAttempt && e.Op == "open" && (e.Phase == "before" || e.Phase == "validated") && (e.Role == "main" || e.Role == "wal") {
				if !nativeClosed || statsForTest().NativeActive != 0 {
					t.Fatal("actual barrier open preceded conclusive native close")
				}
				index := 0
				if e.Role == "wal" {
					index = 2
				}
				if e.Phase == "validated" {
					index++
				}
				barrierOpens[index]++
			}
			if e.Op == "fsync" && e.Phase == "before" && (e.Role == "main" || e.Role == "wal") {
				if !nativeClosed || statsForTest().NativeActive != 0 || e.FD < 0 {
					t.Fatal("barrier descriptor used before actual native close")
				}
			}
		}})
		qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
			trace.observeSQL(e)
			if e.Phase == "commit-after-engine" && e.Operation == "commit" && e.Code != lib.SQLITE_DONE {
				t.Fatal("COMMIT result is not actual SQLITE_DONE")
			}
			if e.Phase == "durable-native-closed" && e.Operation == "close-durably" {
				if e.Code != lib.SQLITE_OK {
					t.Fatal("native close result is not actual SQLITE_OK")
				}
				nativeClosed = true
			}
		}})
		c := freshQAInitial(t, dir)
		freshQASeed(t, c)
		wal, err := os.Lstat(filepath.Join(dir, freshQAName+"-wal"))
		if err != nil || wal.Size() <= 32 {
			t.Fatal("real committed WAL-only witness absent", err)
		}
		mainBefore, err := os.Lstat(filepath.Join(dir, freshQAName))
		if err != nil {
			t.Fatal(err)
		}
		if trace.sqlCount("commit-after-engine", "commit") != 1 {
			t.Fatal("actual native COMMIT absent")
		}
		barrierAttempt = true
		if err := c.CloseDurably(freshQAContext(t)); err != nil {
			t.Fatal(err)
		}
		for _, count := range barrierOpens {
			if count < 1 {
				t.Fatal("actual main/WAL before/validated barrier-open witness absent")
			}
		}
		mainAfter, e := os.Lstat(filepath.Join(dir, freshQAName))
		walAfter, w := os.Lstat(filepath.Join(dir, freshQAName+"-wal"))
		if e != nil || w != nil || mainAfter.Size() != mainBefore.Size() || walAfter.Size() <= 32 {
			t.Fatal("durable close relocated/deleted nonempty WAL evidence", e, w)
		}
		if trace.sqlCount("durable-native-closed", "close-durably") != 1 || trace.count("root", "lease", "released") != 1 || trace.count("guard", "close", "closed") != 1 {
			t.Fatal("durable terminal ownership not singular")
		}
		events := trace.snapshot()
		main, walAt, parentAt := -1, -1, -1
		for i, e := range events {
			if e.Op == "fsync" && e.Phase == "after" && e.Errno == 0 {
				if e.Role == "main" {
					main = i
				}
				if e.Role == "wal" {
					walAt = i
				}
				if e.Role == "root" {
					parentAt = i
				}
			}
		}
		if main < 0 || walAt <= main || parentAt <= walAt {
			t.Fatal("actual main/WAL/parent sync order absent")
		}
		after := len(events)
		if err := c.CloseDurably(freshQAContext(t)); err != nil {
			t.Fatal(err)
		}
		if err := c.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(trace.snapshot()) != after {
			t.Fatal("terminal success repeated native/sync/release")
		}
		barrierAttempt = false
		if freshQARows(t, dir) != 1 {
			t.Fatal("cold WAL durable receipt absent")
		}
		freshQAQuiet(t)
	})
	for _, state := range []string{"unused", "read", "rollback", "unknown", "failed-verify", "failed-cleanup", "ordinary-closed", "checkpoint"} {
		t.Run("ineligible-"+state, func(t *testing.T) {
			dir := qaDirectory(t)
			seed := freshQAInitial(t, dir)
			freshQASeed(t, seed)
			if err := seed.CloseDurably(freshQAContext(t)); err != nil {
				t.Fatal(err)
			}
			c := qaOpen(t, dir, freshQAName, false)
			trace := &freshQATrace{}
			qaFSHooks(t, hooks{Observe: trace.observeFS})
			qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL})
			switch state {
			case "read":
				tx := qaBegin(t, c, freshQAContext(t), Read)
				_ = capacityQAScalar(t, tx, "SELECT count(*) FROM fresh_receipts")
				qaCommit(t, tx)
			case "rollback":
				tx := qaBegin(t, c, freshQAContext(t), Write)
				qaDone(t, tx, "INSERT INTO fresh_receipts VALUES(2,?)", Text("owned concurrent receipt"))
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			case "unknown":
				tx := qaBegin(t, c, freshQAContext(t), Write)
				qaDone(t, tx, "UPDATE fresh_receipts SET payload=payload WHERE id=1")
				qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
					if e.Phase == "commit-after-engine" && e.Operation == "commit" {
						return unix.EIO
					}
					return nil
				}})
				outcome, err := tx.Commit()
				if outcome != Unknown || err == nil {
					t.Fatal("actual-engine Unknown fixture absent", outcome, err)
				}
				setSQLHooksForTest(sqlTestHooks{})
			case "failed-verify":
				tx := qaBegin(t, c, freshQAContext(t), Write)
				qaDone(t, tx, "UPDATE fresh_receipts SET payload=payload WHERE id=1")
				qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
					if e.Phase == "commit-after-verify" && e.Operation == "commit" {
						return unix.EIO
					}
					return nil
				}})
				outcome, err := tx.Commit()
				if outcome != Committed || err == nil {
					t.Fatal("actual committed-with-error fixture absent", outcome, err)
				}
				setSQLHooksForTest(sqlTestHooks{})
			case "failed-cleanup":
				tx := qaBegin(t, c, freshQAContext(t), Write)
				qaDone(t, tx, "UPDATE fresh_receipts SET payload=payload WHERE id=1")
				qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
					if e.Phase == "commit-before-dispatch" || e.Phase == "rollback-before" {
						return unix.EIO
					}
					return nil
				}})
				outcome, err := tx.Commit()
				var checked *Error
				if outcome != Unknown || !errors.As(err, &checked) || checked.Cleanup == nil {
					t.Fatal("earlier refusal/later rollback cleanup failure absent", outcome, err)
				}
				setSQLHooksForTest(sqlTestHooks{})
			case "ordinary-closed":
				tx := qaBegin(t, c, freshQAContext(t), Write)
				qaDone(t, tx, "UPDATE fresh_receipts SET payload=payload WHERE id=1")
				qaCommit(t, tx)
				qaClose(t, c)
			case "checkpoint":
				_, err := c.Checkpoint(freshQAContext(t), Passive)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := c.CloseDurably(freshQAContext(t)); err == nil {
				t.Fatal("ineligible state falsely certified durability", state)
			}
			if trace.sqlCount("durable-native-closed", "close-durably") != 0 || trace.count("main", "fsync", "before") != 0 {
				t.Fatal("ineligible state opened barrier")
			}
			if err := c.Close(context.Background()); err != nil { /* A cached refusal remains an error; it must still release ownership. */
				qaSafeError(t, err, dir)
			}
			freshQAQuiet(t)
		})
	}
	t.Run("actual-barrier-FD-alias-retains-other-native-lock", func(t *testing.T) {
		dir := qaDirectory(t)
		c := freshQAInitial(t, dir)
		freshQASeed(t, c)
		otherDir := qaDirectory(t)
		otherName := "barrier-lock-owner.sqlite3"
		owner := qaOpen(t, otherDir, otherName, true)
		tx := qaBegin(t, owner, freshQAContext(t), Write)
		qaDone(t, tx, "CREATE TABLE lock_owner(id INTEGER)")
		freshQALockWitness(t, otherDir, otherName, "main")
		target := filepath.Join(dir, freshQAName)
		saved := filepath.Join(dir, "owned-original-main")
		source := filepath.Join(otherDir, otherName)
		moved := false
		quarantined := false
		qaFSHooks(t, hooks{Observe: func(e event) {
			if !moved && e.Namespace == c.root.key && e.Role == "main" && e.Op == "open" && e.Phase == "before" && c.db == 0 {
				if err := os.Rename(target, saved); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(source, target); err != nil {
					t.Fatal(err)
				}
				moved = true
			}
			if e.Namespace == c.root.key && e.Role == "main" && e.Op == "open" && e.Phase == "quarantined" {
				if e.FD < 0 {
					t.Fatal("barrier rejection lacks actual owned FD")
				}
				quarantined = true
				freshQALockWitness(t, dir, freshQAName, "main")
			}
		}})
		err := c.CloseDurably(freshQAContext(t))
		if err == nil || !moved || !quarantined {
			t.Fatal("actual barrier alias rejection not reached", err)
		}
		s := statsForTest()
		if !s.Poisoned || s.RejectedFDs < 1 || s.NativeActive != 1 {
			t.Fatalf("barrier quarantine drained beside other native lock: %+v", s)
		}
		freshQALockWitness(t, dir, freshQAName, "main")
		setHooksForTest(hooks{})
		if err := os.Rename(target, source); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(saved, target); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		qaClose(t, owner)
		freshQAQuiet(t)
		if freshQARows(t, dir) != 1 {
			t.Fatal("conservative barrier rejection lost committed receipt")
		}
	})
	t.Run("earlier-fsync-later-actual-guard-release-failure", func(t *testing.T) {
		dir := qaDirectory(t)
		child := freshQAChildStart(t, "guard-close-fault", "two-failures", dir, freshQAName)
		child.join(t, false)
		if freshQARows(t, dir) != 1 {
			t.Fatal("actual release failure lost committed receipt")
		}
		freshQAQuiet(t)
	})
	t.Run("real-native-BUSY-retains-ownership", func(t *testing.T) {
		dir := qaDirectory(t)
		c := freshQAInitial(t, dir)
		freshQASeed(t, c)
		if err := c.lockContext(context.Background(), time.Time{}); err != nil {
			t.Fatal(err)
		}
		stmt, err := c.prepareRaw("SELECT id FROM fresh_receipts", PreparePhase)
		c.unlock()
		if err != nil {
			t.Fatal(err)
		}
		finalized := false
		t.Cleanup(func() {
			if !finalized && c.db != 0 {
				if err := c.lockContext(context.Background(), time.Time{}); err != nil {
					t.Errorf("owned native statement cleanup gate: %v", err)
					return
				}
				code := lib.Xsqlite3_finalize(c.tls, stmt)
				finalized = true
				c.unlock()
				if code != lib.SQLITE_OK {
					t.Errorf("owned native statement cleanup code=%d", code)
				}
			}
		})
		trace := &freshQATrace{}
		qaFSHooks(t, hooks{Observe: trace.observeFS})
		if err := c.CloseDurably(freshQAContext(t)); err == nil {
			t.Fatal("native outstanding statement durability acknowledged")
		}
		err = c.Close(context.Background())
		var native *Error
		if !errors.As(err, &native) || native.Code&255 != lib.SQLITE_BUSY {
			t.Fatal("actual sqlite3_close BUSY not reached", err)
		}
		if statsForTest().NativeActive != 1 || trace.count("main", "fsync", "before") != 0 {
			t.Fatal("BUSY released ownership or opened barrier")
		}
		if err := c.lockContext(context.Background(), time.Time{}); err != nil {
			t.Fatal(err)
		}
		code := lib.Xsqlite3_finalize(c.tls, stmt)
		finalized = true
		c.unlock()
		if code != lib.SQLITE_OK {
			t.Fatal("native statement finalize", code)
		}
		if err := c.Close(context.Background()); err != nil {
			qaSafeError(t, err, dir)
		}
		freshQAQuiet(t)
	})
	for _, boundary := range []struct{ phase, op string }{
		{"durable-native-closed", "close-durably"}, {"fsync-before", "durable-main"}, {"fsync-after", "durable-main"},
		{"fsync-before", "durable-wal"}, {"fsync-after", "durable-wal"}, {"fsync-before", "durable-parent"}, {"fsync-after", "durable-parent"}, {"durable-release-before", "close-durably"},
	} {
		for _, cancelled := range []bool{false, true} {
			t.Run(boundary.phase+"/"+boundary.op+"/"+map[bool]string{false: "fault", true: "cancel"}[cancelled], func(t *testing.T) {
				dir := qaDirectory(t)
				c := freshQAInitial(t, dir)
				freshQASeed(t, c)
				ctx, cancel := context.WithCancel(freshQAContext(t))
				t.Cleanup(cancel)
				trace := &freshQATrace{}
				reached := false
				qaFSHooks(t, hooks{Observe: trace.observeFS})
				qaSQLHooks(t, sqlTestHooks{Observe: trace.observeSQL, Fault: func(e sqlTestEvent) error {
					if !reached && e.Phase == boundary.phase && e.Operation == boundary.op {
						reached = true
						if cancelled {
							cancel()
							return nil
						}
						return unix.EIO
					}
					return nil
				}})
				err := c.CloseDurably(ctx)
				if !reached || err == nil || cancelled && !errors.Is(err, context.Canceled) {
					t.Fatal("postcommit barrier boundary lost", err)
				}
				qaSafeError(t, err, dir, freshQAValue)
				freshQAQuiet(t)
				count := len(trace.snapshot())
				if again := c.CloseDurably(context.Background()); again == nil {
					t.Fatal("cached failed barrier became success")
				}
				if again := c.Close(context.Background()); again == nil {
					t.Fatal("ordinary close erased saved durable failure")
				}
				if len(trace.snapshot()) != count || trace.count("root", "lease", "released") != 1 {
					t.Fatal("failed terminal repeated sync/release")
				}
				setSQLHooksForTest(sqlTestHooks{})
				setHooksForTest(hooks{})
				if freshQARows(t, dir) != 1 {
					t.Fatal("actual committed receipt lost after barrier failure")
				}
			})
		}
	}
}

func TestSQLiteFreshReachedStageKillJoinRecovery(t *testing.T) {
	for _, stage := range []string{"exclusive-created", "initial-wal-ready", "rows-before-commit", "commit-after-engine", "durable-native-closed", "fsync-after-wal", "fsync-after-parent"} {
		t.Run(stage, func(t *testing.T) {
			dir := qaDirectory(t)
			child := freshQAChildStart(t, "crash", stage, dir, freshQAName)
			child.join(t, true)
			committed := stage == "commit-after-engine" || stage == "durable-native-closed" || strings.HasPrefix(stage, "fsync-after-")
			if committed {
				if stage == "commit-after-engine" {
					// Observe only after actual SIGKILL/join, before any ordinary
					// reopen can checkpoint the WAL and hide the stale-main premise.
					freshQAQuiet(t)
					image := freshQAImage(t, dir, freshQAName)
					main, wal := image[""], image["-wal"]
					const frameSize = 24 + 4096
					if len(main) < 4096 || !bytes.Equal(main[:16], []byte("SQLite format 3\x00")) ||
						main[16] != 16 || main[17] != 0 || main[18] != 2 || main[19] != 2 ||
						main[21] != 64 || main[22] != 32 || main[23] != 32 ||
						len(wal) < 32+frameSize || (len(wal)-32)%frameSize != 0 || binary.BigEndian.Uint32(wal[8:12]) != 4096 {
						t.Fatal("actual committed WAL stale-main header/frame fixture not reached")
					}
					magic := binary.BigEndian.Uint32(wal[:4])
					lastFrame := len(wal) - frameSize
					committedPages := binary.BigEndian.Uint32(wal[lastFrame+4 : lastFrame+8])
					if magic != 0x377f0682 && magic != 0x377f0683 || committedPages <= uint32(len(main)/4096) {
						t.Fatal("actual WAL commit does not prove main lags logical page count")
					}
					c := freshQAInitial(t, dir)
					tx := qaBegin(t, c, freshQAContext(t), Read)
					if capacityQAScalar(t, tx, "SELECT count(*) FROM fresh_receipts WHERE id=1 AND payload=?", Text(freshQAValue)) != 1 {
						t.Fatal("initial recovery lost actual committed stale-main receipt")
					}
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
					qaClose(t, c)
					freshQAQuiet(t)
				}
				if freshQARows(t, dir) != 1 {
					t.Fatal("joined committed prefix lost exact native receipt")
				}
				c := qaOpen(t, dir, freshQAName, false)
				tx := qaBegin(t, c, freshQAContext(t), Write)
				qaDone(t, tx, "UPDATE fresh_receipts SET payload=payload WHERE id=1")
				qaCommit(t, tx)
				if err := c.CloseDurably(freshQAContext(t)); err != nil {
					qaSafeError(t, err, dir)
				}
				if freshQARows(t, dir) != 1 {
					t.Fatal("fresh nonce-like fence lost committed prefix")
				}
				freshQAQuiet(t)
			} else {
				before := freshQAImage(t, dir, freshQAName)
				c, err := OpenForInitialLink(freshQAContext(t), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
				freshQAOwn(t, c)
				if err != nil {
					if c != nil {
						if e := c.Close(context.Background()); e != nil {
							t.Fatal(e)
						}
					}
					freshQASameImage(t, dir, freshQAName, before)
					freshQAQuiet(t)
					return
				}
				tx := qaBegin(t, c, freshQAContext(t), Read)
				if capacityQAScalar(t, tx, "SELECT count(*) FROM sqlite_schema WHERE name='fresh_receipts'") != 0 {
					t.Fatal("uncommitted prefix recovered phantom receipt")
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				qaClose(t, c)
				freshQAQuiet(t)
			}
		})
	}
}

func TestSQLiteFreshCloseDurablyConcurrentReaderWriterCheckpointAndRoleRace(t *testing.T) {
	t.Run("actual-cross-process-reader-writer-checkpoint", func(t *testing.T) {
		dir := qaDirectory(t)
		c := freshQAInitial(t, dir)
		freshQASeed(t, c)
		reader := freshQAChildStart(t, "reader", "pinned", dir, freshQAName)
		reached := false
		qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
			if !reached && e.Phase == "durable-native-closed" && e.Operation == "close-durably" {
				reached = true
				writer := freshQAChildStart(t, "writer", "committed", dir, freshQAName)
				writer.join(t, false)
				checkpoint := freshQAChildStart(t, "checkpoint", "actual", dir, freshQAName)
				checkpoint.join(t, false)
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
				forbidden, err := Open(ctx, dir, freshQAName, Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
				cancel()
				freshQAOwn(t, forbidden)
				if forbidden != nil || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("same-process reopened before retained lease release", err)
				}
			}
		}})
		err := c.CloseDurably(freshQAContext(t))
		if !reached {
			t.Fatal("native-closed concurrent boundary absent")
		}
		if err != nil {
			qaSafeError(t, err, dir, freshQAValue)
		}
		setSQLHooksForTest(sqlTestHooks{})
		reader.join(t, false)
		if freshQARows(t, dir) != 2 {
			t.Fatal("actual concurrent committed rows lost")
		}
		freshQAQuiet(t)
	})
	t.Run("actual-role-replacement-refuses-keeps-commit", func(t *testing.T) {
		dir := qaDirectory(t)
		c := freshQAInitial(t, dir)
		freshQASeed(t, c)
		moved := false
		original := filepath.Join(dir, freshQAName+"-wal")
		saved := filepath.Join(dir, "owned-displaced-wal")
		qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
			if !moved && e.Phase == "durable-native-closed" && e.Operation == "close-durably" {
				if err := os.Rename(original, saved); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(original, []byte("owned invalid replacement"), 0600); err != nil {
					t.Fatal(err)
				}
				moved = true
			}
		}})
		err := c.CloseDurably(freshQAContext(t))
		if !moved || err == nil {
			t.Fatal("role identity race acknowledged", err)
		}
		qaSafeError(t, err, dir)
		setSQLHooksForTest(sqlTestHooks{})
		freshQAQuiet(t)
		if err := os.Remove(original); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(saved, original); err != nil {
			t.Fatal(err)
		}
		if freshQARows(t, dir) != 1 {
			t.Fatal("actual SQL commit lost on conservative barrier refusal")
		}
	})
}
