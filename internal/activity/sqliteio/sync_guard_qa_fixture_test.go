//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

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
	"syscall"
	"testing"
	"time"
)

const sgQAState = "guard-state.json"
const sgQADB = "guard-data.sqlite3"

// Register even a partial owner before making an assertion about its error.
func sgQAOwn(t *testing.T, g *SyncRunGuard) {
	t.Helper()
	if g != nil {
		t.Cleanup(func() { _, _ = g.Close() })
	}
}

func sgQAAcquire(t *testing.T, dir, state, db string, ctx context.Context, deadline time.Time) *SyncRunGuard {
	t.Helper()
	g, err := AcquireSyncRunGuard(ctx, dir, state, db, deadline)
	sgQAOwn(t, g)
	if err != nil || g == nil {
		t.Fatalf("owned sync guard acquisition: %v", err)
	}
	return g
}

func sgQAClose(t *testing.T, g *SyncRunGuard) {
	t.Helper()
	if terminal, err := g.Close(); !terminal || err != nil {
		t.Fatalf("owned sync guard close terminal=%t err=%v", terminal, err)
	}
}

// The only child modes are an actual flock holder and a single timed contender.
// The parent starts at most one child per call, registers its join immediately,
// and the child's environment contains only synthetic fixture paths.
func TestSQLiteSyncGuardOwnedHelper(t *testing.T) {
	mode := os.Getenv("TEMPO_SYNC_GUARD_QA_MODE")
	if mode == "" {
		return
	}
	dir := os.Getenv("TEMPO_SYNC_GUARD_QA_DIR")
	state := os.Getenv("TEMPO_SYNC_GUARD_QA_STATE")
	db := os.Getenv("TEMPO_SYNC_GUARD_QA_DB")
	if mode != "hold" && mode != "try" {
		t.Fatal("unsupported owned child mode")
	}
	g, err := AcquireSyncRunGuard(context.Background(), dir, state, db, time.Now().Add(180*time.Millisecond))
	if g != nil {
		defer func() {
			if terminal, closeErr := g.Close(); !terminal || closeErr != nil {
				t.Error("owned child guard cleanup", terminal, closeErr)
			}
		}()
	}
	if mode == "try" {
		if g != nil || !errors.Is(err, ErrBusy) {
			t.Fatal("cross-process contender was not denied by actual flock", err)
		}
		fmt.Fprintln(os.Stdout, "SYNC_GUARD_QA_DENIED")
		return
	}
	if err != nil || g == nil {
		t.Fatal("owned child did not acquire actual guard", err)
	}
	fmt.Fprintln(os.Stdout, "SYNC_GUARD_QA_READY")
	var release [1]byte
	if _, err := io.ReadFull(os.Stdin, release[:]); err != nil {
		t.Fatal("owned child release pipe", err)
	}
}

type sgQAChild struct {
	cmd       *exec.Cmd
	mode      string
	pid, pgid int
	in        io.WriteCloser
	reader    *bufio.Reader
	cancel    context.CancelFunc
	once      sync.Once
}

func sgQAStartChild(t *testing.T, mode, dir, state, db string) *sgQAChild {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	cmd := exec.CommandContext(ctx, bin, "-test.run=^TestSQLiteSyncGuardOwnedHelper$", "-test.v")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "GOMAXPROCS=2", "TMPDIR=" + filepath.Dir(dir), "TEMPO_SYNC_GUARD_QA_MODE=" + mode, "TEMPO_SYNC_GUARD_QA_DIR=" + dir, "TEMPO_SYNC_GUARD_QA_STATE=" + state, "TEMPO_SYNC_GUARD_QA_DB=" + db}
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
	child := &sgQAChild{cmd: cmd, mode: mode, in: in, reader: bufio.NewReader(out), cancel: cancel}
	if err := cmd.Start(); err != nil {
		_ = in.Close()
		_ = out.Close()
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { child.join(t, false) })
	child.pid = cmd.Process.Pid
	child.pgid, err = syscall.Getpgid(child.pid)
	if err != nil {
		t.Fatal("owned sync guard child process group", err)
	}
	t.Logf("owned sync guard child started pid=%d pgid=%d", child.pid, child.pgid)
	want := "SYNC_GUARD_QA_READY"
	if mode == "try" {
		want = "SYNC_GUARD_QA_DENIED"
	}
	for {
		line, readErr := child.reader.ReadString('\n')
		if strings.TrimSpace(line) == want {
			break
		}
		if readErr != nil {
			t.Fatal("owned child did not reach guard stage", readErr)
		}
	}
	if mode == "try" {
		child.join(t, false)
	}
	return child
}

func (c *sgQAChild) join(t *testing.T, kill bool) {
	t.Helper()
	c.once.Do(func() {
		var signalErr, writeErr error
		if kill {
			t.Logf("owned sync guard child signaling SIGKILL pid=%d pgid=%d", c.pid, c.pgid)
			signalErr = c.cmd.Process.Kill()
		} else if c.mode == "hold" {
			_, writeErr = c.in.Write([]byte{'x'})
		}
		closeErr := c.in.Close()
		_, readErr := io.Copy(io.Discard, c.reader)
		waitErr := c.cmd.Wait()
		exit := -1
		if c.cmd.ProcessState != nil {
			exit = c.cmd.ProcessState.ExitCode()
		}
		t.Logf("owned sync guard child joined pid=%d pgid=%d exit=%d", c.pid, c.pgid, exit)
		c.cancel()
		if kill {
			status, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus)
			if signalErr != nil || waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Errorf("owned guard kill/join signal=%v wait=%v status=%v", signalErr, waitErr, status)
			}
		} else if writeErr != nil || closeErr != nil || readErr != nil || waitErr != nil {
			t.Errorf("owned guard join write=%v close=%v read=%v wait=%v", writeErr, closeErr, readErr, waitErr)
		}
	})
}
