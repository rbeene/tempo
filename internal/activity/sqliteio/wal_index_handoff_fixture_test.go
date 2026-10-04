//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	lib "modernc.org/sqlite/lib"
)

// Every returned owner is registered before examining its error. Expected
// cached failures are asserted by each case; cleanup still proves termination.
func whQAOwn(t *testing.T, c *Conn) *Conn {
	t.Helper()
	t.Cleanup(func() {
		setHooksForTest(hooks{})
		setSQLHooksForTest(sqlTestHooks{})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		terminal, err := c.CloseChecked(ctx)
		if !terminal {
			terminal, err = c.CloseChecked(ctx)
		}
		if !terminal {
			t.Error("handoff fixture retained a native owner", err)
		}
	})
	return c
}

func whQAOpen(t *testing.T, ctx context.Context, dir string, deadline time.Time) (*Conn, LinkInspection, error) {
	t.Helper()
	c, kind, err := OpenForCaptureWrite(ctx, dir, freshQAState, freshQAName, deadline)
	whQAOwn(t, c)
	return c, kind, err
}

func whQABegin(t *testing.T, c *Conn, mode Mode) *Tx {
	t.Helper()
	tx, err := c.Begin(context.Background(), mode)
	if tx != nil {
		t.Cleanup(func() { _ = tx.Rollback() })
	}
	if err != nil || tx == nil {
		t.Fatal("handoff fixture begin", err)
	}
	return tx
}

func whQASeed(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(qaDirectory(t), "store")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	linkQASeedWAL(t, dir)
	c, err := Open(context.Background(), dir, freshQAName, Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
	whQAOwn(t, c)
	if err != nil || c == nil {
		t.Fatal("real WAL conditioning open", err)
	}
	tx := whQABegin(t, c, Write)
	qaDone(t, tx, "UPDATE link_qa_meta SET nonce=20")
	qaCommit(t, tx)
	whQAClose(t, c, false)
	image := freshQAImage(t, dir, freshQAName)
	if len(image[""]) < 4096 || len(image["-wal"]) <= 32 {
		t.Fatal("real retained WAL conditioning missing")
	}
	freshQAQuiet(t)
	return dir
}

func whQAClose(t *testing.T, c *Conn, wantError bool) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	terminal, err := c.CloseChecked(ctx)
	if !terminal || (err != nil) != wantError {
		t.Fatal("checked handoff close outcome", terminal, err)
	}
	if againTerminal, again := c.CloseChecked(ctx); !againTerminal || again != err {
		t.Fatal("terminal handoff close did not retain exact cached outcome", againTerminal, again)
	}
	if c != nil {
		whQANativeGone(t, c)
	}
	return err
}

func whQANativeGone(t *testing.T, c *Conn) {
	t.Helper()
	if c == nil || !c.closed || c.db != 0 || c.nativeCounted || c.tls != nil || c.authMode != 0 || c.cancelFlag != 0 {
		t.Fatal("terminal wrapper retained native handle or callback memory")
	}
	if p := c.handoffProbe; p != nil && (!p.closed || p.db != 0 || p.nativeCounted || p.tls != nil || p.authMode != 0 || p.cancelFlag != 0) {
		t.Fatal("terminal aggregate retained predecessor native state")
	}
}

func whQAOnlyCleanup(t *testing.T, c *Conn) {
	t.Helper()
	if c == nil || c.closed || !c.used || !c.poisoned {
		t.Fatal("failed handoff did not retain a permanently cleanup-only owner")
	}
	before := statsForTest()
	for _, mode := range []Mode{Read, Write} {
		tx, err := c.Begin(context.Background(), mode)
		if tx != nil {
			_ = tx.Rollback()
		}
		if tx != nil || err == nil {
			t.Fatal("failed handoff admitted a transaction", mode, err)
		}
	}
	result, err := c.Checkpoint(context.Background(), Truncate)
	if err == nil || result.Attempted || result.BeforeValid || result.AfterValid {
		t.Fatal("failed handoff dispatched checkpoint", err)
	}
	if err := c.CloseDurably(context.Background()); err == nil || c.durableAttempted {
		t.Fatal("failed handoff admitted durable acknowledgement", err)
	}
	if statsForTest() != before {
		t.Fatal("cleanup-only refusals changed ownership")
	}
}

func whQAErrorCount(err error, category Category, code int32) int {
	if err == nil {
		return 0
	}
	n := 0
	if e, ok := err.(*Error); ok && e.Category == category && e.Code == code {
		n++
	}
	if many, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range many.Unwrap() {
			n += whQAErrorCount(child, category, code)
		}
	} else if one, ok := err.(interface{ Unwrap() error }); ok {
		n += whQAErrorCount(one.Unwrap(), category, code)
	}
	return n
}

func whQAHoldRaw(t *testing.T, c *Conn) func() {
	t.Helper()
	stmt, err := c.prepareRaw("SELECT 1", PreparePhase)
	if err != nil || stmt == 0 {
		t.Fatal("owned real native statement", err)
	}
	finalize := func() {
		if stmt == 0 {
			return
		}
		rc := lib.Xsqlite3_finalize(c.tls, stmt)
		stmt = 0
		if rc != lib.SQLITE_OK {
			t.Error("owned native finalizer", rc)
		}
	}
	t.Cleanup(finalize) // LIFO: finalize before the previously registered Conn.
	return finalize
}

// One separate child contains intentional registry-fatal descriptor reuse.
// No Setpgid: the root runner owns the inherited process group. Start registers
// bounded kill/join before any fallible PID observation; normal completion joins.
func whQAFatalChild(t *testing.T, dir string) {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "-test.run=^TestSQLiteCaptureWriterHandoffOwnedHelper$", "-test.count=1", "-test.timeout=12s")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "GOMAXPROCS=2", "TMPDIR=" + filepath.Dir(dir), "TEMPO_HANDOFF_QA_MODE=root-release", "TEMPO_HANDOFF_QA_DIR=" + dir}
	cmd.WaitDelay = time.Second
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal("owned handoff helper input", err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		_ = in.Close()
		t.Fatal("owned handoff helper output", err)
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		_ = in.Close()
		_ = out.Close()
		t.Fatal("owned handoff helper start", err)
	}
	joined := false
	t.Cleanup(func() {
		if !joined {
			_ = cmd.Process.Kill()
			_ = in.Close()
			_ = cmd.Wait()
			joined = true
			code := -1
			if cmd.ProcessState != nil {
				code = cmd.ProcessState.ExitCode()
			}
			t.Logf("owned handoff child joined pid=%d exit=%d", cmd.Process.Pid, code)
		}
	})
	reader := bufio.NewReader(out)
	line, err := reader.ReadString('\n')
	if err != nil || line != "FRESH_QA_READY handoff root-release\n" {
		t.Fatal("owned handoff readiness handshake", err)
	}
	pgid, err := unix.Getpgid(cmd.Process.Pid)
	if err != nil {
		t.Fatal("owned handoff process group", err)
	}
	t.Logf("owned handoff child started pid=%d pgid=%d", cmd.Process.Pid, pgid)
	if _, err := in.Write([]byte{'x'}); err != nil {
		t.Fatal("owned handoff release", err)
	}
	if err := in.Close(); err != nil {
		t.Fatal("owned handoff input close", err)
	}
	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatal("owned handoff output join", err)
	}
	err = cmd.Wait()
	joined = true
	code := -1
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
	}
	t.Logf("owned handoff child joined pid=%d exit=%d", cmd.Process.Pid, code)
	if err != nil || ctx.Err() != nil {
		t.Fatal("isolated root release assertions failed", err, ctx.Err())
	}
}

func TestSQLiteCaptureWriterHandoffOwnedHelper(t *testing.T) {
	mode := os.Getenv("TEMPO_HANDOFF_QA_MODE")
	if mode == "" {
		return
	}
	if mode != "root-release" {
		t.Fatal("unsupported handoff child mode")
	}
	freshQAReady("handoff", "root-release")
	dir := os.Getenv("TEMPO_HANDOFF_QA_DIR")
	c, kind, err := whQAOpen(t, context.Background(), dir, time.Now().Add(250*time.Millisecond))
	if err != nil || kind != LinkWAL || c == nil {
		t.Fatal("isolated handoff positive open", err)
	}
	// The owned ancestor FD is deliberately replaced after successful handoff.
	// It is never a main/WAL/SHM FD. The registry must quarantine that number,
	// close native state, and report terminal caller ownership with cached error.
	if len(c.root.chain) < 2 || c.root.chain[0].borrowed {
		t.Fatal("retained owned ancestor fixture missing")
	}
	fd := c.root.chain[0].fd
	replacement, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal("owned replacement open", err)
	}
	if err := unix.Dup2(replacement, fd); err != nil {
		_ = unix.Close(replacement)
		t.Fatal("owned ancestor replacement", err)
	}
	if err := unix.Close(replacement); err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd) // This test owns the replacement; production must not close it.
	trace := &freshQATrace{}
	qaFSHooks(t, hooks{Observe: trace.observeFS})
	err = whQAClose(t, c, true)
	if !errors.Is(err, ErrUnsafe) || trace.count("root", "lease", "released") != 1 {
		t.Fatal("root release lost terminal error", err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFCHR {
		t.Fatal("terminal cleanup closed reused descriptor", err)
	}
	s := statsForTest()
	if s.NativeActive != 0 || s.Active != 0 || s.Entries != 0 || !s.Fatal || !s.Poisoned || s.RejectedFDs != 1 {
		t.Fatal("terminal caller release discarded fatal quarantine", s)
	}
	other, _, refused := whQAOpen(t, context.Background(), dir, time.Now().Add(250*time.Millisecond))
	if other != nil || !errors.Is(refused, ErrUnsafe) {
		t.Fatal("fatal registry readmitted native ownership", refused)
	}
}
