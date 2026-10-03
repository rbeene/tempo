//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

// Independent source-first fixtures for capacity blueprint 59b34730.
// These files have not been compiled or run. Missing APIs are not a RED witness.

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

const capacityQAName = "capacity-owned.sqlite3"

func capacityQAContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func capacityQAOpen(t *testing.T, dir string, create bool, maxPages int64) *Conn {
	t.Helper()
	c, err := Open(capacityQAContext(t), dir, capacityQAName, Options{
		Create: create, AcquireDeadline: time.Now().Add(250 * time.Millisecond), testMaxPages: maxPages,
	})
	if c != nil {
		t.Cleanup(func() {
			if closeErr := c.Close(context.Background()); closeErr != nil {
				t.Errorf("capacity owned close: %v", closeErr)
			}
		})
	}
	if err != nil {
		t.Fatalf("capacity open: %v", err)
	}
	return c
}

func capacityQAScalar(t *testing.T, tx *Tx, sql string, args ...Value) int64 {
	t.Helper()
	s := qaPrepare(t, tx, sql, args...)
	row, err := s.Step()
	if !row || err != nil {
		_ = s.Close()
		t.Fatalf("capacity scalar row=%t error=%v", row, err)
	}
	n, err := s.Int64(0)
	if err != nil {
		_ = s.Close()
		t.Fatal(err)
	}
	if row, err = s.Step(); row || err != nil {
		_ = s.Close()
		t.Fatalf("capacity scalar extra row=%t error=%v", row, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	return n
}

func capacityQAInitialize(t *testing.T, dir string, maxPages int64) {
	t.Helper()
	c := capacityQAOpen(t, dir, true, maxPages)
	tx := qaBegin(t, c, capacityQAContext(t), Write)
	qaDone(t, tx, "CREATE TABLE capacity_items(id INTEGER PRIMARY KEY, body BLOB NOT NULL) STRICT")
	qaDone(t, tx, "INSERT INTO capacity_items(id,body) VALUES(1,?)", Blob([]byte("retained-seed")))
	qaCommit(t, tx)
	qaClose(t, c)
}

func capacityQAAppend(t *testing.T, dir string, id int64, size int) {
	t.Helper()
	c := capacityQAOpen(t, dir, false, 0)
	tx := qaBegin(t, c, capacityQAContext(t), Write)
	qaDone(t, tx, "INSERT INTO capacity_items(id,body) VALUES(?,?)", Integer(id), Blob(bytes.Repeat([]byte{byte(id)}, size)))
	qaCommit(t, tx)
	qaClose(t, c)
}

func capacityQACount(t *testing.T, dir string) int64 {
	t.Helper()
	c := capacityQAOpen(t, dir, false, 0)
	tx := qaBegin(t, c, capacityQAContext(t), Read)
	n := capacityQAScalar(t, tx, "SELECT count(*) FROM capacity_items")
	qaCommit(t, tx)
	qaClose(t, c)
	return n
}

func capacityQACleanRead(t *testing.T, c *Conn) {
	t.Helper()
	tx := qaBegin(t, c, capacityQAContext(t), Read)
	if n := capacityQAScalar(t, tx, "SELECT count(*) FROM capacity_items"); n < 1 {
		t.Fatal("fixture lost retained seed")
	}
	qaCommit(t, tx)
}

// Probe the permanent authorizer after the operation is terminal. This bypasses
// only the Go transaction API in the test; no statement is stepped or mutation
// attempted. The connection's actual SQLite authorizer must refuse preparation.
func capacityQAApplicationAuth(t *testing.T, c *Conn) {
	t.Helper()
	if c.authMode == 0 || *(*uint32)(unsafe.Pointer(c.authMode)) != authApplication {
		t.Error("administrative call did not restore application authorizer mode")
	}
	for _, sql := range []string{"PRAGMA synchronous", "COMMIT", "ATTACH DATABASE ':memory:' AS forbidden"} {
		stmt, err := c.prepareRaw(sql, PreparePhase)
		if stmt != 0 {
			if rc := lib.Xsqlite3_finalize(c.tls, stmt); rc != lib.SQLITE_OK {
				t.Errorf("unexpected prepared control finalize code=%d", rc)
			}
		}
		if err == nil {
			t.Errorf("application authorizer admitted fixed forbidden control %q", sql)
		}
	}
}

// Only this fixed incompatible fixture bypasses production Open's page policy.
// It still uses the actual pinned VFS/registry and checked native close, creates
// one small synthetic database, and is fully closed before the test opens it.
// No production option can select a non-4096 page size.
func qaCreate8192PageFixture(t *testing.T, directory, basename string) {
	t.Helper()
	ctx := capacityQAContext(t)
	deadline := time.Now().Add(250 * time.Millisecond)
	r, err := acquireRoot(ctx, directory, basename, true, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err = initialize(); err != nil {
		_ = releaseRoot(r)
		t.Fatal(err)
	}
	c := &Conn{tls: libc.NewTLS(), root: r, gate: make(chan struct{}, 1), acquireDeadline: deadline}
	c.gate <- struct{}{}
	t.Cleanup(func() {
		if err := c.Close(context.Background()); err != nil {
			t.Errorf("wrong-page fixture close: %v", err)
		}
	})
	name, err := libc.CString(r.key + "/" + basename)
	if err != nil {
		t.Fatal(err)
	}
	out := lib.Xsqlite3_malloc64(c.tls, uint64(unsafe.Sizeof(uintptr(0))))
	if out == 0 {
		libc.Xfree(c.tls, name)
		t.Fatal("wrong-page fixture native allocation failed")
	}
	*(*uintptr)(unsafe.Pointer(out)) = 0
	flags := int32(lib.SQLITE_OPEN_READWRITE | lib.SQLITE_OPEN_CREATE | lib.SQLITE_OPEN_FULLMUTEX | lib.SQLITE_OPEN_NOFOLLOW | lib.SQLITE_OPEN_PRIVATECACHE)
	rc := lib.Xsqlite3_open_v2(c.tls, name, out, flags, vfsNameMemory)
	c.db = *(*uintptr)(unsafe.Pointer(out))
	lib.Xsqlite3_free(c.tls, out)
	libc.Xfree(c.tls, name)
	if rc != lib.SQLITE_OK {
		t.Fatalf("wrong-page native open code=%d", rc)
	}
	if ok, err := c.config(lib.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1); err != nil || !ok {
		t.Fatalf("wrong-page fixture no-close-checkpoint=%t error=%v", ok, err)
	}
	// Each statement is trusted fixed fixture SQL. No authorizer is installed
	// on this fixture-only connection; production Open installs its own guards.
	for _, sql := range []string{
		"PRAGMA page_size=8192", "PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL",
		"PRAGMA wal_autocheckpoint=0", "CREATE TABLE incompatible_page_fixture(id INTEGER PRIMARY KEY)",
	} {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		stmt, err := c.prepareRaw(sql, PreparePhase)
		if err != nil {
			t.Fatal(err)
		}
		for {
			rc = lib.Xsqlite3_step(c.tls, stmt)
			if rc != lib.SQLITE_ROW {
				break
			}
		}
		fin := lib.Xsqlite3_finalize(c.tls, stmt)
		if rc != lib.SQLITE_DONE || fin != lib.SQLITE_OK {
			t.Fatalf("wrong-page seed step=%d finalize=%d", rc, fin)
		}
	}
	qaClose(t, c)
}

func capacityQAHelperCommand(t *testing.T, ctx context.Context, dir, mode string) *exec.Cmd {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, bin, "-test.run=^TestSQLiteCapacityOwnedHelper$", "-test.v")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "GOMAXPROCS=2", "TMPDIR=" + filepath.Dir(dir), "TEMPO_CAPACITY_QA_MODE=" + mode, "TEMPO_CAPACITY_QA_DIR=" + dir}
	cmd.WaitDelay = time.Second
	return cmd
}

func TestSQLiteCapacityOwnedHelper(t *testing.T) {
	mode, dir := os.Getenv("TEMPO_CAPACITY_QA_MODE"), os.Getenv("TEMPO_CAPACITY_QA_DIR")
	if mode == "" {
		return
	}
	if mode == "lock" {
		fd, err := unix.Open(filepath.Join(dir, capacityQAName), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		var lock unix.Flock_t
		lock.Type, lock.Whence, lock.Start, lock.Len = unix.F_WRLCK, 0, 0x40000002, 510
		err = unix.FcntlFlock(uintptr(fd), unix.F_GETLK, &lock)
		closeErr := unix.Close(fd)
		if err != nil || closeErr != nil {
			t.Fatalf("owned lock probe query=%v close=%v", err, closeErr)
		}
		fmt.Fprintf(os.Stdout, "CAPACITY_QA_LOCK %d %d\n", lock.Type, lock.Pid)
		return
	}
	if mode != "reader" {
		t.Fatal("invalid synthetic capacity helper mode")
	}
	c := capacityQAOpen(t, dir, false, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Second)
	defer cancel()
	tx := qaBegin(t, c, ctx, Read)
	before := capacityQAScalar(t, tx, "SELECT count(*) FROM capacity_items")
	fmt.Fprintln(os.Stdout, "CAPACITY_QA_READER_READY")
	var release [1]byte
	if _, err := io.ReadFull(os.Stdin, release[:]); err != nil {
		t.Fatal("owned reader release pipe failed")
	}
	if after := capacityQAScalar(t, tx, "SELECT count(*) FROM capacity_items"); after != before {
		t.Fatal("pinned reader snapshot changed")
	}
	qaCommit(t, tx)
	qaClose(t, c)
}

func capacityQAReader(t *testing.T, dir string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	cmd := capacityQAHelperCommand(t, ctx, dir, "reader")
	in, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		closeErr := in.Close()
		cancel()
		t.Fatalf("owned reader stdout pipe: %v; stdin close: %v", err, closeErr)
	}
	cmd.Stderr = cmd.Stdout
	if err = cmd.Start(); err != nil {
		inErr, outErr := in.Close(), out.Close()
		cancel()
		t.Fatalf("owned reader start: %v; pipe closes: %v %v", err, inErr, outErr)
	}
	pgid, groupErr := unix.Getpgid(cmd.Process.Pid)
	if groupErr != nil {
		t.Logf("owned capacity reader started pid=%d pgid_unavailable=true", cmd.Process.Pid)
		cancel()
		closeErr := in.Close()
		_, drainErr := io.Copy(io.Discard, out)
		waitErr := cmd.Wait()
		exit := -1
		if cmd.ProcessState != nil {
			exit = cmd.ProcessState.ExitCode()
		}
		t.Logf("owned capacity reader joined pid=%d exit=%d", cmd.Process.Pid, exit)
		t.Fatalf("owned reader getpgid: %v; cleanup close=%v drain=%v wait=%v", groupErr, closeErr, drainErr, waitErr)
	}
	t.Logf("owned capacity reader started pid=%d pgid=%d", cmd.Process.Pid, pgid)
	reader := bufio.NewReader(out)
	ready := false
	for {
		line, err := reader.ReadString('\n')
		if strings.TrimSpace(line) == "CAPACITY_QA_READER_READY" {
			ready = true
			break
		}
		if err != nil {
			break
		}
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			_, writeErr := in.Write([]byte{'x'})
			closeErr := in.Close()
			_, drainErr := io.Copy(io.Discard, reader)
			waitErr := cmd.Wait()
			cancel()
			exit := -1
			if cmd.ProcessState != nil {
				exit = cmd.ProcessState.ExitCode()
			}
			t.Logf("owned capacity reader joined pid=%d exit=%d", cmd.Process.Pid, exit)
			if writeErr != nil || closeErr != nil || drainErr != nil || waitErr != nil {
				t.Errorf("owned reader cleanup write=%v close=%v drain=%v wait=%v", writeErr, closeErr, drainErr, waitErr)
			}
		})
	}
	t.Cleanup(release)
	if !ready {
		release()
		t.Fatal("owned reader did not establish actual snapshot")
	}
	return release
}

func capacityQALockWitness(t *testing.T, dir string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := capacityQAHelperCommand(t, ctx, dir, "lock")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("owned capacity lock witness start: %v", err)
	}
	pgid, groupErr := unix.Getpgid(cmd.Process.Pid)
	if groupErr != nil {
		t.Logf("owned capacity lock witness started pid=%d pgid_unavailable=true", cmd.Process.Pid)
		cancel()
	} else {
		t.Logf("owned capacity lock witness started pid=%d pgid=%d", cmd.Process.Pid, pgid)
	}
	waitErr := cmd.Wait()
	exit := -1
	if cmd.ProcessState != nil {
		exit = cmd.ProcessState.ExitCode()
	}
	t.Logf("owned capacity lock witness joined pid=%d exit=%d", cmd.Process.Pid, exit)
	if groupErr != nil || waitErr != nil {
		t.Fatalf("owned capacity lock witness getpgid=%v wait=%v", groupErr, waitErr)
	}
	found := false
	for _, line := range strings.Split(output.String(), "\n") {
		if !strings.HasPrefix(line, "CAPACITY_QA_LOCK ") {
			continue
		}
		var kind, pid int
		if _, err := fmt.Sscanf(line, "CAPACITY_QA_LOCK %d %d", &kind, &pid); err != nil {
			t.Fatal("malformed owned lock witness")
		}
		if kind != unix.F_RDLCK || pid != os.Getpid() {
			t.Fatalf("main shared lock missing: type=%d owner=%d expected=%d", kind, pid, os.Getpid())
		}
		found = true
	}
	if !found {
		t.Fatal("owned lock witness produced no result")
	}
}
