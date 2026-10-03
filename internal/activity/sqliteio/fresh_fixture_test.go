//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

// Independent finite native fixtures for plan b2e1ca38. Raw fixture handles
// count individually in the same registry; no public seam is substituted.
import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
	"modernc.org/libc"
	lib "modernc.org/sqlite/lib"
)

const freshQAState = "fresh-owned-state.json"
const freshQAName = "fresh-owned.sqlite3"
const freshQAValue = "owned durable native receipt"
const freshQADDL = "CREATE TABLE fresh_receipts(id INTEGER PRIMARY KEY,payload TEXT NOT NULL) STRICT"

func freshQAContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func freshQAOwn(t *testing.T, c *Conn) {
	t.Helper()
	if c != nil {
		t.Cleanup(func() { _ = c.Close(context.Background()) })
	}
	// Explicit terminal assertions own expected cached failures below.
}

type freshQARawNameOSResult struct {
	AuthorityErrno, GuardErrno, OpenErrno int
	ExactEntry, Empty                     bool
}

// Independent direct OS control, interpreted only by the qualified raw-name QA.
// All names and descriptors are confined to a separately owned empty fixture,
// conclusively closed before the producer is called. No SQLite/VFS seam is used.
func freshQARawNameOSBaseline(t *testing.T, producerDir, stateName string) freshQARawNameOSResult {
	t.Helper()
	controlDir := qaDirectory(t)
	fd, err := unix.Open(controlDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal("OS basename control root open failed")
	}
	defer func() {
		if err := unix.Close(fd); err != nil {
			t.Fatal("OS basename control root close failed")
		}
	}()
	var control, producer unix.Stat_t
	if unix.Fstat(fd, &control) != nil || unix.Stat(producerDir, &producer) != nil || control.Dev != producer.Dev {
		t.Fatal("OS basename control filesystem mismatch")
	}
	errno := func(err error) int {
		if err == nil {
			return 0
		}
		var n syscall.Errno
		if !errors.As(err, &n) {
			t.Fatal("OS basename control non-errno failure")
		}
		return int(n)
	}
	var authority, guard unix.Stat_t
	authorityErrno := errno(unix.Fstatat(fd, stateName, &authority, unix.AT_SYMLINK_NOFOLLOW))
	guardName := stateName + ".lock"
	guardErrno := errno(unix.Fstatat(fd, guardName, &guard, unix.AT_SYMLINK_NOFOLLOW))
	guardFD, openErr := unix.Openat(fd, guardName, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	openErrno := errno(openErr)
	if openErr == nil {
		if err := unix.Close(guardFD); err != nil {
			t.Fatal("OS basename control guard close failed")
		}
	}
	entries, err := os.ReadDir(controlDir)
	if err != nil {
		t.Fatal("OS basename control listing failed")
	}
	exactEntry := len(entries) == 1 && entries[0].Name() == guardName
	empty := len(entries) == 0
	// Numeric errno and booleans only: never log paths, names or raw errors.
	t.Logf("OS raw-basename control authority-fstatat=%d guard-fstatat=%d guard-openat=%d exact-entry=%t empty=%t", authorityErrno, guardErrno, openErrno, exactEntry, empty)
	return freshQARawNameOSResult{AuthorityErrno: authorityErrno, GuardErrno: guardErrno, OpenErrno: openErrno, ExactEntry: exactEntry, Empty: empty}
}

func freshQAInitial(t *testing.T, dir string) *Conn {
	t.Helper()
	c, err := OpenForInitialLink(freshQAContext(t), dir, freshQAState, freshQAName, time.Now().Add(250*time.Millisecond))
	freshQAOwn(t, c)
	if err != nil {
		t.Fatal("owned initial native opener", err)
	}
	return c
}
func freshQASeed(t *testing.T, c *Conn) {
	t.Helper()
	tx := qaBegin(t, c, freshQAContext(t), Write)
	qaDone(t, tx, freshQADDL)
	qaDone(t, tx, "INSERT INTO fresh_receipts VALUES(1,?)", Text(freshQAValue))
	qaCommit(t, tx)
}
func freshQARows(t *testing.T, dir string) int64 {
	t.Helper()
	c := qaOpen(t, dir, freshQAName, false)
	tx := qaBegin(t, c, freshQAContext(t), Read)
	stmt := qaPrepare(t, tx, "SELECT id,payload FROM fresh_receipts ORDER BY id")
	t.Cleanup(func() { _ = stmt.Close() })
	var n int64
	for {
		row, err := stmt.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !row {
			break
		}
		if stmt.ColumnCount() != 2 {
			t.Fatal("cold exact receipt projection")
		}
		id, err := stmt.Int64(0)
		if err != nil {
			t.Fatal(err)
		}
		value, err := stmt.Text(1)
		if err != nil {
			t.Fatal(err)
		}
		n++
		expected := freshQAValue
		if n == 2 {
			expected = "owned concurrent receipt"
		}
		if id != n || n > 2 || value != expected {
			t.Fatal("cold native receipt identity/payload changed")
		}
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
	return n
}
func freshQAQuiet(t *testing.T) {
	t.Helper()
	s := statsForTest()
	if s.Active != 0 || s.Entries != 0 || s.NativeActive != 0 || s.Guards != 0 || s.RejectedFDs != 0 || s.Poisoned || s.Fatal {
		t.Fatalf("owned terminal state leaked: %+v", s)
	}
}
func freshQAImage(t *testing.T, dir, name string) map[string][]byte {
	t.Helper()
	result := map[string][]byte{}
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		b, err := os.ReadFile(filepath.Join(dir, name+suffix))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		result[suffix] = b
	}
	return result // Called only with same-store native ownership conclusively closed.
}
func freshQASameImage(t *testing.T, dir, name string, want map[string][]byte) {
	t.Helper()
	got := freshQAImage(t, dir, name)
	if len(got) != len(want) {
		t.Fatal("refusal changed owned family presence")
	}
	for role, b := range want {
		if !bytes.Equal(got[role], b) {
			t.Fatal("refusal changed owned family bytes", role)
		}
	}
}

// Exact owned occupied-P observations; these synthetic paths have no native
// owner. Lstat preserves link identity/type and never follows the symlink.
type freshQAOccupiedPath struct {
	Dev, Ino uint64
	Mode     uint32
	Bytes    []byte
	Target   string
	Names    []string
	Sentinel *freshQAOccupiedPath
}

func freshQAOccupiedSnapshot(t *testing.T, path string) freshQAOccupiedPath {
	t.Helper()
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil {
		t.Fatal("owned occupied P lstat", err)
	}
	out := freshQAOccupiedPath{Dev: uint64(stat.Dev), Ino: uint64(stat.Ino), Mode: uint32(stat.Mode)}
	switch stat.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal("owned occupied file bytes", err)
		}
		out.Bytes = b
	case unix.S_IFLNK:
		target, err := os.Readlink(path)
		if err != nil {
			t.Fatal("owned occupied symlink target", err)
		}
		out.Target = target
	case unix.S_IFDIR:
		entries, err := os.ReadDir(path)
		if err != nil {
			t.Fatal("owned occupied directory contents", err)
		}
		for _, entry := range entries {
			out.Names = append(out.Names, entry.Name())
		}
		if len(out.Names) != 1 || out.Names[0] != "owned-sentinel" {
			t.Fatal("owned occupied directory sentinel premise")
		}
		sentinel := freshQAOccupiedSnapshot(t, filepath.Join(path, "owned-sentinel"))
		if sentinel.Mode&unix.S_IFMT != unix.S_IFREG {
			t.Fatal("owned directory sentinel type changed")
		}
		out.Sentinel = &sentinel
	default:
		t.Fatal("unknown owned occupied P type")
	}
	return out
}

type freshQATrace struct {
	mu  sync.Mutex
	fs  []event
	sql []sqlTestEvent
}

func (q *freshQATrace) observeFS(e event) { q.mu.Lock(); q.fs = append(q.fs, e); q.mu.Unlock() }
func (q *freshQATrace) observeSQL(e sqlTestEvent) {
	q.mu.Lock()
	q.sql = append(q.sql, e)
	q.mu.Unlock()
}
func (q *freshQATrace) count(role, op, phase string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, e := range q.fs {
		if e.Role == role && e.Op == op && e.Phase == phase {
			n++
		}
	}
	return n
}
func (q *freshQATrace) sqlCount(phase, op string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, e := range q.sql {
		if e.Phase == phase && e.Operation == op {
			n++
		}
	}
	return n
}
func (q *freshQATrace) snapshot() []event {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]event(nil), q.fs...)
}

// Two raw handles under one owned registry root exist only in the confined
// fixture subprocess for genuine Unix cache reuse. Other profiles use one.
type freshQARaw struct {
	tls     *libc.TLS
	db      uintptr
	counted bool
}

func freshQARawOpen(t *testing.T, owner *freshQARoot) *freshQARaw {
	r := owner.root
	t.Helper()
	h := &freshQARaw{tls: libc.NewTLS()}
	owner.handles = append(owner.handles, h)
	registry.Lock()
	registry.nativeActive++
	h.counted = true
	registry.Unlock()
	t.Cleanup(func() {
		if err := h.close(); err != nil {
			t.Errorf("raw native cleanup: %v", err)
		}
	})
	name, err := libc.CString(r.key + "/" + r.databaseName)
	if err != nil {
		t.Fatal(err)
	}
	out := lib.Xsqlite3_malloc64(h.tls, 8)
	if out == 0 {
		libc.Xfree(h.tls, name)
		t.Fatal("raw allocation")
	}
	*(*uintptr)(unsafe.Pointer(out)) = 0
	flags := int32(lib.SQLITE_OPEN_READWRITE | lib.SQLITE_OPEN_CREATE | lib.SQLITE_OPEN_FULLMUTEX | lib.SQLITE_OPEN_NOFOLLOW | lib.SQLITE_OPEN_PRIVATECACHE)
	code := lib.Xsqlite3_open_v2(h.tls, name, out, flags, vfsNameMemory)
	h.db = *(*uintptr)(unsafe.Pointer(out))
	lib.Xsqlite3_free(h.tls, out)
	libc.Xfree(h.tls, name)
	if code != lib.SQLITE_OK {
		t.Fatalf("raw native open code=%d", code)
	}
	// Prevent a fixture close from relocating WAL-only evidence.
	native := &Conn{tls: h.tls, db: h.db}
	if ok, err := native.config(lib.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1); err != nil || !ok {
		t.Fatal("raw fixture no-close-checkpoint", err)
	}
	return h
}
func (h *freshQARaw) close() error {
	if !h.counted {
		return nil
	}
	if h.db != 0 {
		code := lib.Xsqlite3_close(h.tls, h.db)
		if code != lib.SQLITE_OK {
			return engineError(ClosePhase, code, nil)
		}
	}
	h.db = 0
	registry.Lock()
	registry.nativeActive--
	registry.Unlock()
	h.counted = false
	h.tls.Close()
	h.tls = nil
	return nil
}
func freshQARawSQL(t *testing.T, h *freshQARaw, sql string) {
	t.Helper()
	query, err := libc.CString(sql)
	if err != nil {
		t.Fatal(err)
	}
	out := lib.Xsqlite3_malloc64(h.tls, 8)
	if out == 0 {
		libc.Xfree(h.tls, query)
		t.Fatal("raw statement allocation")
	}
	*(*uintptr)(unsafe.Pointer(out)) = 0
	code := lib.Xsqlite3_prepare_v2(h.tls, h.db, query, -1, out, 0)
	stmt := *(*uintptr)(unsafe.Pointer(out))
	lib.Xsqlite3_free(h.tls, out)
	libc.Xfree(h.tls, query)
	if code != lib.SQLITE_OK {
		if stmt != 0 {
			lib.Xsqlite3_finalize(h.tls, stmt)
		}
		t.Fatalf("raw fixed prepare code=%d", code)
	}
	for {
		code = lib.Xsqlite3_step(h.tls, stmt)
		if code != lib.SQLITE_ROW {
			break
		}
	}
	fin := lib.Xsqlite3_finalize(h.tls, stmt)
	if code != lib.SQLITE_DONE || fin != lib.SQLITE_OK {
		t.Fatalf("raw fixed step=%d finalize=%d", code, fin)
	}
}

type freshQARoot struct {
	root     *rootEntry
	released bool
	handles  []*freshQARaw
}

func freshQARawRoot(t *testing.T, dir, name string) *freshQARoot {
	t.Helper()
	r, err := acquireRoot(freshQAContext(t), dir, name, true, time.Now().Add(250*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err = initialize(); err != nil {
		_ = releaseRoot(r)
		t.Fatal(err)
	}
	owner := &freshQARoot{root: r}
	t.Cleanup(func() {
		if !owner.released {
			for _, h := range owner.handles {
				if h.counted {
					t.Errorf("raw native close inconclusive; root remains owned")
					return
				}
			}
			if err := releaseRoot(r); err != nil {
				t.Errorf("raw root release: %v", err)
			}
			owner.released = true
		}
	})
	return owner
}
func freshQARawRelease(t *testing.T, owner *freshQARoot) {
	t.Helper()
	if owner.released {
		t.Fatal("raw root released twice")
	}
	for _, h := range owner.handles {
		if h.counted {
			t.Fatal("raw shared root cannot release beside live handle")
		}
	}
	if err := releaseRoot(owner.root); err != nil {
		t.Fatal(err)
	}
	owner.released = true
}
func freshQAProfile(t *testing.T, dir, profile string) {
	t.Helper()
	if profile == "zero" {
		f, err := os.OpenFile(filepath.Join(dir, freshQAName), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if err = f.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	r := freshQARawRoot(t, dir, freshQAName)
	h := freshQARawOpen(t, r)
	freshQARawSQL(t, h, "PRAGMA page_size=4096")
	switch profile {
	case "single":
		freshQARawSQL(t, h, "PRAGMA application_id=1")
		freshQARawSQL(t, h, "PRAGMA application_id=0")
	case "wal-empty":
		freshQARawSQL(t, h, "PRAGMA journal_mode=WAL")
		freshQARawSQL(t, h, "BEGIN IMMEDIATE")
		freshQARawSQL(t, h, "CREATE TABLE uncommitted_profile(id INTEGER PRIMARY KEY)")
		freshQARawSQL(t, h, "ROLLBACK")
	case "page-size":
		freshQARawSQL(t, h, "PRAGMA page_size=8192")
		freshQARawSQL(t, h, "PRAGMA application_id=1")
		freshQARawSQL(t, h, "PRAGMA application_id=0")
	case "application-id":
		freshQARawSQL(t, h, "PRAGMA application_id=7")
	case "user-version":
		freshQARawSQL(t, h, "PRAGMA user_version=7")
	case "schema-version":
		freshQARawSQL(t, h, "PRAGMA schema_version=7")
	case "auto-vacuum":
		freshQARawSQL(t, h, "PRAGMA auto_vacuum=1")
		freshQARawSQL(t, h, "PRAGMA application_id=1")
		freshQARawSQL(t, h, "PRAGMA application_id=0")
	case "objects":
		freshQARawSQL(t, h, "CREATE TABLE foreign_profile(id INTEGER)")
	case "allocated-free":
		freshQARawSQL(t, h, "CREATE TABLE foreign_profile(id INTEGER PRIMARY KEY,body BLOB)")
		freshQARawSQL(t, h, "INSERT INTO foreign_profile VALUES(1,zeroblob(20000))")
		freshQARawSQL(t, h, "DROP TABLE foreign_profile")
	default:
		t.Fatal("unknown fixed profile")
	}
	if err := h.close(); err != nil {
		t.Fatal(err)
	}
	freshQARawRelease(t, r)
}

type freshQAChild struct {
	cmd    *exec.Cmd
	in     io.WriteCloser
	reader *bufio.Reader
	cancel context.CancelFunc
	once   sync.Once
}

func freshQAChildStart(t *testing.T, mode, stage, dir, name string) *freshQAChild {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	cmd := exec.CommandContext(ctx, bin, "-test.run=^TestSQLiteFreshOwnedHelper$", "-test.v")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "GOMAXPROCS=2", "TMPDIR=" + filepath.Dir(dir), "TEMPO_FRESH_QA_MODE=" + mode, "TEMPO_FRESH_QA_STAGE=" + stage, "TEMPO_FRESH_QA_DIR=" + dir, "TEMPO_FRESH_QA_NAME=" + name}
	cmd.WaitDelay = time.Second
	in, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		_ = in.Close()
		cancel()
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	child := &freshQAChild{cmd: cmd, in: in, reader: bufio.NewReader(out), cancel: cancel}
	if err = cmd.Start(); err != nil {
		_ = in.Close()
		_ = out.Close()
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { child.join(t, false) })
	pgid, err := unix.Getpgid(cmd.Process.Pid)
	if err != nil {
		t.Fatal("owned child process group", err)
	}
	t.Logf("owned fresh child started pid=%d pgid=%d", cmd.Process.Pid, pgid)
	wanted := "FRESH_QA_READY " + mode + " " + stage
	for {
		line, err := child.reader.ReadString('\n')
		if strings.TrimSpace(line) == wanted {
			break
		}
		if err != nil {
			t.Fatal("owned reached-stage handshake absent", err)
		}
	}
	return child
}
func (c *freshQAChild) join(t *testing.T, kill bool) {
	t.Helper()
	c.once.Do(func() {
		var signalErr, errorWrite error
		if kill {
			signalErr = c.cmd.Process.Kill()
		} else {
			_, errorWrite = c.in.Write([]byte{'x'})
		}
		closeErr := c.in.Close()
		_, drainErr := io.Copy(io.Discard, c.reader)
		waitErr := c.cmd.Wait()
		c.cancel()
		code := -1
		if c.cmd.ProcessState != nil {
			code = c.cmd.ProcessState.ExitCode()
		}
		t.Logf("owned fresh child joined pid=%d exit=%d", c.cmd.Process.Pid, code)
		if kill {
			status, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus)
			if signalErr != nil || waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Errorf("owned kill proof: signal=%v wait=%v status=%v", signalErr, waitErr, status)
			}
		} else if errorWrite != nil || closeErr != nil || drainErr != nil || waitErr != nil {
			t.Errorf("owned child terminal write=%v close=%v drain=%v wait=%v", errorWrite, closeErr, drainErr, waitErr)
		}
	})
}
func freshQALockWitness(t *testing.T, dir, name, role string) {
	t.Helper()
	child := freshQAChildStart(t, "lock", role, dir, name)
	line, err := child.reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	var kind, pid int
	if _, err = fmt.Sscanf(line, "FRESH_QA_LOCK %d %d", &kind, &pid); err != nil {
		t.Fatal("owned lock result", err)
	}
	want := unix.F_RDLCK
	if role == "shm" {
		want = unix.F_WRLCK
	}
	if kind != want || pid != os.Getpid() {
		t.Fatalf("actual POSIX lock type=%d pid=%d expected type=%d pid=%d", kind, pid, want, os.Getpid())
	}
	child.join(t, false)
}
func freshQAReady(mode, stage string) {
	fmt.Fprintln(os.Stdout, "FRESH_QA_READY "+mode+" "+stage)
	var b [1]byte
	if _, err := io.ReadFull(os.Stdin, b[:]); err != nil {
		panic("owned fixture release pipe")
	}
}

// Finite modes, synthetic explicit paths, no inherited user environment.
func TestSQLiteFreshOwnedHelper(t *testing.T) {
	mode := os.Getenv("TEMPO_FRESH_QA_MODE")
	if mode == "" {
		return
	}
	dir, name, stage := os.Getenv("TEMPO_FRESH_QA_DIR"), os.Getenv("TEMPO_FRESH_QA_NAME"), os.Getenv("TEMPO_FRESH_QA_STAGE")
	switch mode {
	case "lock":
		fd, err := unix.Open(filepath.Join(dir, name), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		var lock unix.Flock_t
		lock.Type = unix.F_WRLCK
		lock.Whence = 0
		lock.Start = 0x40000002
		lock.Len = 510
		if stage == "shm" {
			lock.Type = unix.F_RDLCK
			lock.Start = 120
			lock.Len = 1
		}
		err = unix.FcntlFlock(uintptr(fd), unix.F_GETLK, &lock)
		ce := unix.Close(fd)
		if err != nil || ce != nil {
			t.Fatal("actual external lock query/close", err, ce)
		}
		fmt.Fprintln(os.Stdout, "FRESH_QA_READY "+mode+" "+stage)
		fmt.Fprintf(os.Stdout, "FRESH_QA_LOCK %d %d\n", lock.Type, lock.Pid)
		var b [1]byte
		if _, err = io.ReadFull(os.Stdin, b[:]); err != nil {
			t.Fatal(err)
		}
	case "reader":
		c := qaOpen(t, dir, name, false)
		tx := qaBegin(t, c, freshQAContext(t), Read)
		before := capacityQAScalar(t, tx, "SELECT count(*) FROM fresh_receipts")
		freshQAReady(mode, stage)
		if capacityQAScalar(t, tx, "SELECT count(*) FROM fresh_receipts") != before {
			t.Fatal("owned snapshot changed")
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		qaClose(t, c)
	case "writer":
		c := qaOpen(t, dir, name, false)
		tx := qaBegin(t, c, freshQAContext(t), Write)
		qaDone(t, tx, "INSERT INTO fresh_receipts VALUES(2,?)", Text("owned concurrent receipt"))
		qaCommit(t, tx)
		if err := c.CloseDurably(freshQAContext(t)); err != nil {
			t.Fatal(err)
		}
		freshQAReady(mode, stage)
	case "checkpoint":
		c := qaOpen(t, dir, name, false)
		result, err := c.Checkpoint(freshQAContext(t), Truncate)
		if !result.Attempted {
			t.Fatal("actual checkpoint not dispatched", err)
		}
		if err != nil {
			qaSafeError(t, err, dir, name)
		}
		qaClose(t, c)
		freshQAReady(mode, stage)
	case "follower", "follower-wait":
		if mode == "follower-wait" {
			qaFSHooks(t, hooks{Observe: func(e event) {
				if e.Role == "guard" && e.Op == "open" && e.Phase == "validated" {
					err := unix.Flock(e.FD, unix.LOCK_EX|unix.LOCK_NB)
					if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
						if err == nil {
							_ = unix.Flock(e.FD, unix.LOCK_UN)
						}
						t.Fatal("actual first-Link winner guard not held", err)
					}
					freshQAReady(mode, "guard-blocked")
				}
			}})
		}
		c := freshQAInitial(t, dir)
		tx := qaBegin(t, c, freshQAContext(t), Read)
		if capacityQAScalar(t, tx, "SELECT count(*) FROM fresh_receipts WHERE payload=?", Text(freshQAValue)) != 1 {
			t.Fatal("guard follower lost committed row")
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		qaClose(t, c)
		if mode == "follower" {
			freshQAReady(mode, stage)
		}
	case "guard-close-fault":
		guardFD := -1
		closed := false
		qaFSHooks(t, hooks{Observe: func(e event) {
			if e.Role == "guard" && e.Op == "open" && e.Phase == "validated" {
				guardFD = e.FD
			}
			if !closed && e.Role == "main" && e.Op == "fsync" && e.Phase == "after" && e.Errno == 0 {
				if guardFD < 0 || statsForTest().NativeActive != 0 {
					t.Fatal("admitted guard FD/native-closed boundary absent")
				}
				// Native has conclusively closed; all earlier main barrier FDs are still
				// owned, preventing reuse of the just-closed guard number before release.
				if err := unix.Close(guardFD); err != nil {
					t.Fatal(err)
				}
				closed = true
			}
		}})
		c := freshQAInitial(t, dir)
		freshQASeed(t, c)
		qaSQLHooks(t, sqlTestHooks{Fault: func(e sqlTestEvent) error {
			if e.Phase == "fsync-after" && e.Operation == "durable-main" {
				return unix.EIO
			}
			return nil
		}})
		err := c.CloseDurably(freshQAContext(t))
		var checked *Error
		if !closed || !errors.As(err, &checked) || checked.Cleanup == nil {
			t.Fatal("earlier barrier/later actual guard-release failure not retained", err)
		}
		if statsForTest().Active != 0 || statsForTest().NativeActive != 0 {
			t.Fatal("failed terminal left a root/native owner")
		}
		if c.CloseDurably(context.Background()) == nil || c.Close(context.Background()) == nil {
			t.Fatal("failed terminal outcome erased")
		}
		freshQAReady(mode, stage)
	case "hot-journal":
		freshQAProfile(t, dir, "single")
		r := freshQARawRoot(t, dir, name)
		h := freshQARawOpen(t, r)
		freshQARawSQL(t, h, "PRAGMA cache_size=2")
		freshQARawSQL(t, h, "BEGIN IMMEDIATE")
		freshQARawSQL(t, h, "CREATE TABLE hot_fixture(id INTEGER PRIMARY KEY,body BLOB)")
		freshQARawSQL(t, h, "INSERT INTO hot_fixture VALUES(1,zeroblob(50000))")
		freshQAReady(mode, stage)
		freshQARawSQL(t, h, "ROLLBACK")
		if err := h.close(); err != nil {
			t.Fatal(err)
		}
		freshQARawRelease(t, r)
	case "crash":
		qaFSHooks(t, hooks{Observe: func(e event) {
			if stage == "exclusive-created" && e.Role == "main" && e.Op == "exclusive" && e.Phase == "created" {
				freshQAReady(mode, stage)
			}
		}})
		qaSQLHooks(t, sqlTestHooks{Observe: func(e sqlTestEvent) {
			reached := e.Phase == stage && (e.Operation == "initial-link" || e.Operation == "close-durably" || e.Operation == "commit" && e.Code == lib.SQLITE_DONE)
			if stage == "fsync-after-wal" {
				reached = e.Phase == "fsync-after" && e.Operation == "durable-wal"
			}
			if stage == "fsync-after-parent" {
				reached = e.Phase == "fsync-after" && e.Operation == "durable-parent"
			}
			if reached {
				freshQAReady(mode, stage)
			}
		}})
		c := freshQAInitial(t, dir)
		tx := qaBegin(t, c, freshQAContext(t), Write)
		qaDone(t, tx, freshQADDL)
		qaDone(t, tx, "INSERT INTO fresh_receipts VALUES(1,?)", Text(freshQAValue))
		if stage == "rows-before-commit" {
			freshQAReady(mode, stage)
		}
		qaCommit(t, tx)
		if err := c.CloseDurably(freshQAContext(t)); err != nil {
			t.Fatal(err)
		}
	case "reuse":
		freshQAReuseChild(t, dir, name, stage)
	case "lowfd":
		freshQALowFDChild(t, dir, name, stage)
	default:
		t.Fatal("unknown fixed fresh child mode")
	}
}

func freshQAReuseChild(t *testing.T, dir, name, stage string) {
	owner := "reuse-source.sqlite3"
	freshQAProfile(t, dir, "single")
	if err := os.Rename(filepath.Join(dir, freshQAName), filepath.Join(dir, owner)); err != nil {
		t.Fatal(err)
	}
	r := freshQARawRoot(t, dir, owner)
	a := freshQARawOpen(t, r)
	freshQARawSQL(t, a, "CREATE TABLE cache_fixture(id INTEGER)")
	freshQARawSQL(t, a, "BEGIN")
	freshQARawSQL(t, a, "SELECT count(*) FROM cache_fixture")
	b := freshQARawOpen(t, r)
	if err := b.close(); err != nil {
		t.Fatal(err)
	}
	out := lib.Xsqlite3_malloc64(a.tls, 8)
	if out == 0 {
		t.Fatal("cache pointer allocation")
	}
	*(*uintptr)(unsafe.Pointer(out)) = 0
	code := lib.Xsqlite3_file_control(a.tls, a.db, 0, lib.SQLITE_FCNTL_FILE_POINTER, out)
	file := *(*uintptr)(unsafe.Pointer(out))
	lib.Xsqlite3_free(a.tls, out)
	if code != lib.SQLITE_OK || file == 0 {
		t.Fatal("actual file pointer unavailable", code)
	}
	inode := (*lib.TunixFile)(unsafe.Pointer(file)).FpInode
	if inode == 0 || (*lib.TunixInodeInfo)(unsafe.Pointer(inode)).FpUnused == 0 {
		t.Fatal("genuine Unix unused FD cache not reached")
	}
	trace := &freshQATrace{}
	moved := false
	qaFSHooks(t, hooks{Observe: func(e event) {
		trace.observeFS(e)
		if !moved && e.Role == "main" && e.Op == "stat" && e.Phase == "before" {
			if err := os.Rename(filepath.Join(dir, owner), filepath.Join(dir, name)); err != nil {
				t.Fatal(err)
			}
			moved = true
		}
	}})
	c, err := Open(freshQAContext(t), dir, name, Options{Create: true, ExclusiveCreate: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	freshQAOwn(t, c)
	if err == nil || !moved || trace.count("main", "exclusive", "created") != 0 {
		t.Fatal("genuine reuse alias admitted", err)
	}
	for _, e := range trace.snapshot() {
		if e.Role == "main" && e.Op == "vfs-open" && e.Phase == "returned" && e.Code == lib.SQLITE_OK {
			t.Fatal("cached existing FD accepted by exclusive xOpen")
		}
	}
	if c != nil {
		if err := c.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	freshQALockWitness(t, dir, name, "main")
	freshQAReady("reuse", stage)
	setHooksForTest(hooks{})
	if err := os.Rename(filepath.Join(dir, name), filepath.Join(dir, owner)); err != nil {
		t.Fatal(err)
	}
	freshQARawSQL(t, a, "ROLLBACK")
	if err := a.close(); err != nil {
		t.Fatal(err)
	}
	freshQARawRelease(t, r)
	freshQAQuiet(t)
}
func freshQALowFDChild(t *testing.T, dir, name, stage string) {
	// Keep the owned handshake pipes above the actual low-FD native fixture.
	out, err := unix.FcntlInt(os.Stdout.Fd(), unix.F_DUPFD_CLOEXEC, 20)
	if err != nil {
		t.Fatal(err)
	}
	in, err := unix.FcntlInt(os.Stdin.Fd(), unix.F_DUPFD_CLOEXEC, 20)
	if err != nil {
		_ = unix.Close(out)
		t.Fatal(err)
	}
	output, input := os.NewFile(uintptr(out), "owned-output"), os.NewFile(uintptr(in), "owned-input")
	t.Cleanup(func() { _ = output.Close(); _ = input.Close() })
	os.Stdout, os.Stdin = output, input
	// Pin the parent first so root directory traversal cannot consume all low
	// slots. Then release stdio exactly at the actual main-open boundary.
	reached := false
	low := false
	unlinked := false
	qaFSHooks(t, hooks{Observe: func(e event) {
		if e.Role == "main" && e.Op == "open" && e.Phase == "before" && !reached {
			reached = true
			for _, fd := range []int{0, 1, 2} {
				_ = unix.Close(fd)
			}
		}
		if e.Role == "main" && e.Op == "open" && e.Phase == "validated" && e.FD >= 0 && e.FD < 3 {
			low = true
		}
		if e.Role == "main" && e.Op == "unlink" && e.Phase == "after" {
			unlinked = true
		}
	}})
	c, openErr := Open(freshQAContext(t), dir, name, Options{Create: true, ExclusiveCreate: true, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	freshQAOwn(t, c)
	if !reached || !low {
		t.Fatal("actual upstream low-FD branch not reached")
	}
	if unlinked || openErr == nil {
		t.Fatal("low-FD retry unlinked/recreated or accepted main", openErr)
	}
	if c != nil {
		if err := c.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	info, err := os.Lstat(filepath.Join(dir, name))
	if err != nil || !info.Mode().IsRegular() {
		t.Fatal("exclusive-created low-FD prefix lost", err)
	}
	freshQAQuiet(t)
	freshQAReady("lowfd", stage)
}
