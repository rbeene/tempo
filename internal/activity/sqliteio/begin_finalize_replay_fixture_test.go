//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"bufio"
	"context"
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

// This regression joins its real writer child inside the original 900ms caller
// lifetime. Only this child disables the race runtime's default 1s exit grace;
// race detection, the parent environment and all admission budgets stay intact.
// The actual child entrypoint and normal pipe-release/Wait protocol are the
// existing qaWriter fixture's. No process, socket or thread is added by it.
func bfrQAWriter(t *testing.T, dir, name string) func() {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	cmd := exec.CommandContext(ctx, bin, "-test.run=^TestSQLiteIOOwnedWriterHelper$", "-test.v")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "GOMAXPROCS=2", "GORACE=atexit_sleep_ms=0", "TMPDIR=" + filepath.Dir(dir), "TEMPO_SQLITEIO_QA_MODE=writer", "TEMPO_SQLITEIO_QA_DIR=" + dir, "TEMPO_SQLITEIO_QA_NAME=" + name}
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
