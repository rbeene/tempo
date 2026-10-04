//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

// Independent QA source authored against API plan 2f67611c01304bf5cfd63349bfeea12e174ce5561d90d3a5e39842fd08e9c802.
// At initial source freeze the production API does not exist. No compile RED,
// runtime PASS or final storage acceptance is claimed by these source files.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const qaBasename = "synthetic-activity.sqlite3"

func qaDirectory(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}
func qaOpen(t *testing.T, dir, name string, create bool) *Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Open(ctx, dir, name, Options{Create: create, AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	if err != nil {
		if c != nil {
			_ = c.Close(context.Background())
		}
		t.Fatalf("synthetic open: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(context.Background()); err != nil {
			t.Errorf("synthetic close: %v", err)
		}
	})
	return c
}
func qaClose(t *testing.T, c *Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}
func qaBegin(t *testing.T, c *Conn, ctx context.Context, mode Mode) *Tx {
	t.Helper()
	tx, err := c.Begin(ctx, mode)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Tests assert cleanup evidence explicitly. A cached expected failure from
	// an injected cleanup boundary is not a second unexpected test failure.
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}
func qaPrepare(t *testing.T, tx *Tx, sql string, args ...Value) *Stmt {
	t.Helper()
	s, err := tx.Prepare(sql, args...)
	if err != nil {
		t.Fatalf("prepare constant QA SQL: %v", err)
	}
	return s
}
func qaDone(t *testing.T, tx *Tx, sql string, args ...Value) {
	t.Helper()
	s := qaPrepare(t, tx, sql, args...)
	row, err := s.Step()
	closeErr := s.Close()
	if row || err != nil || closeErr != nil {
		t.Fatalf("DML/DDL row=%t step=%v close=%v", row, err, closeErr)
	}
}
func qaCommit(t *testing.T, tx *Tx) {
	t.Helper()
	outcome, err := tx.Commit()
	if outcome != Committed || err != nil {
		t.Fatalf("commit outcome=%v error=%v", outcome, err)
	}
}
func qaCount(t *testing.T, dir, name string) int64 {
	t.Helper()
	c := qaOpen(t, dir, name, false)
	tx := qaBegin(t, c, context.Background(), Read)
	s := qaPrepare(t, tx, "SELECT count(*) FROM qa_items")
	row, err := s.Step()
	if !row || err != nil {
		t.Fatalf("count row=%t error=%v", row, err)
	}
	n, err := s.Int64(0)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
	return n
}
func qaInitialize(t *testing.T, dir, name string) {
	t.Helper()
	c := qaOpen(t, dir, name, true)
	tx := qaBegin(t, c, context.Background(), Write)
	qaDone(t, tx, "CREATE TABLE qa_items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
	qaCommit(t, tx)
	qaClose(t, c)
}

type qaSQLTrace struct {
	mu     sync.Mutex
	events []sqlTestEvent
}

func (q *qaSQLTrace) observe(e sqlTestEvent) {
	q.mu.Lock()
	q.events = append(q.events, e)
	q.mu.Unlock()
}
func (q *qaSQLTrace) count(phase string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, e := range q.events {
		if e.Phase == phase {
			n++
		}
	}
	return n
}
func (q *qaSQLTrace) countCode(phase string, code int32) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, e := range q.events {
		if e.Phase == phase && e.Code == code {
			n++
		}
	}
	return n
}
func qaSafeError(t *testing.T, err error, markers ...string) {
	t.Helper()
	var checked *Error
	if !errors.As(err, &checked) {
		t.Fatalf("error is not the checked adapter type: %v", err)
	}
	public := err.Error()
	if checked.Cleanup != nil {
		public += " " + checked.Cleanup.Error()
	}
	for _, marker := range markers {
		if strings.Contains(public, marker) {
			t.Fatalf("safe error disclosed synthetic marker %q", marker)
		}
	}
}
func qaSQLHooks(t *testing.T, h sqlTestHooks) {
	t.Helper()
	setSQLHooksForTest(h)
	t.Cleanup(func() { setSQLHooksForTest(sqlTestHooks{}) })
}
func qaFSHooks(t *testing.T, h hooks) {
	t.Helper()
	setHooksForTest(h)
	t.Cleanup(func() { setHooksForTest(hooks{}) })
}

// The owned helper uses a compiled test binary and synthetic explicit paths.
// Its environment contains no HOME, inherited tokens, credentials or state.
func TestSQLiteIOOwnedWriterHelper(t *testing.T) {
	if os.Getenv("TEMPO_SQLITEIO_QA_MODE") != "writer" {
		return
	}
	dir, name := os.Getenv("TEMPO_SQLITEIO_QA_DIR"), os.Getenv("TEMPO_SQLITEIO_QA_NAME")
	c := qaOpen(t, dir, name, false)
	tx := qaBegin(t, c, context.Background(), Write)
	fmt.Fprintln(os.Stdout, "SQLITEIO_QA_WRITER_READY")
	var release [1]byte
	if _, err := io.ReadFull(os.Stdin, release[:]); err != nil {
		t.Fatal("owned release pipe failed")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	qaClose(t, c)
}
func qaWriter(t *testing.T, dir, name string) func() {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	cmd := exec.CommandContext(ctx, bin, "-test.run=^TestSQLiteIOOwnedWriterHelper$", "-test.v")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "GOMAXPROCS=2", "TMPDIR=" + filepath.Dir(dir), "TEMPO_SQLITEIO_QA_MODE=writer", "TEMPO_SQLITEIO_QA_DIR=" + dir, "TEMPO_SQLITEIO_QA_NAME=" + name}
	in, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err = cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	pgid, groupErr := unix.Getpgid(cmd.Process.Pid)
	if groupErr != nil {
		t.Logf("owned writer started pid=%d pgid_unavailable=true", cmd.Process.Pid)
	} else {
		t.Logf("owned writer started pid=%d pgid=%d", cmd.Process.Pid, pgid)
	}
	reader := bufio.NewReader(out)
	ready := false
	for {
		line, readErr := reader.ReadString('\n')
		if strings.TrimSpace(line) == "SQLITEIO_QA_WRITER_READY" {
			ready = true
			break
		}
		if readErr != nil {
			break
		}
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			_, writeErr := in.Write([]byte{'x'})
			_ = in.Close()
			_, readErr := io.Copy(io.Discard, reader)
			waitErr := cmd.Wait()
			cancel()
			t.Logf("owned writer joined pid=%d exit=%d", cmd.Process.Pid, cmd.ProcessState.ExitCode())
			if writeErr != nil || readErr != nil || waitErr != nil {
				t.Errorf("owned writer cleanup write=%v read=%v wait=%v", writeErr, readErr, waitErr)
			}
		})
	}
	t.Cleanup(release)
	if !ready {
		release()
		t.Fatal("owned writer never acquired actual BEGIN IMMEDIATE")
	}
	return release
}
