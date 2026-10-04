//go:build (darwin || linux) && (amd64 || arm64)

package sqliteio

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func rcQAChild(t *testing.T, mode string) {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, bin, "-test.run=^TestSQLiteRootChainOwnedHelper$", "-test.v")
	cmd.Env = append(rcQAEnvironment(), "TEMPO_ROOT_CHAIN_QA_MODE="+mode)
	rcQACommand(t, cmd)
}

// All helper starts/joins are actual records. No process-group signal is used;
// CommandContext owns only the unreaped child PID, and Wait always joins it.
func rcQAEnvironment() []string {
	env := []string{"PATH=/usr/bin:/bin", "GOMAXPROCS=2", "TMPDIR=" + os.TempDir()}
	if v, ok := os.LookupEnv("GORACE"); ok {
		env = append(env, "GORACE="+v)
	}
	return env
}

func rcQACommand(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd.Env == nil {
		cmd.Env = rcQAEnvironment()
	}
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	cmd.WaitDelay = time.Second
	if err := cmd.Start(); err != nil {
		t.Fatal("owned root-chain child start", err)
	}
	joined := false
	t.Cleanup(func() {
		if !joined {
			killErr := cmd.Process.Kill()
			waitErr := cmd.Wait()
			joined = true
			code := -1
			if cmd.ProcessState != nil {
				code = cmd.ProcessState.ExitCode()
			}
			t.Logf("owned root-chain child joined pid=%d exit=%d", cmd.Process.Pid, code)
			if killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
				t.Error(killErr)
			}
			if waitErr == nil {
				t.Error("unexpected unfinished child cleanup")
			}
		}
	})
	pg, err := unix.Getpgid(cmd.Process.Pid)
	if err != nil {
		t.Fatal("owned root-chain child PG", err)
	}
	t.Logf("owned root-chain child started pid=%d pgid=%d", cmd.Process.Pid, pg)
	err = cmd.Wait()
	joined = true
	code := -1
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
	}
	t.Logf("owned root-chain child joined pid=%d exit=%d", cmd.Process.Pid, code)
	if output.Len() != 0 {
		t.Log(output.String())
	}
	if err != nil || code != 0 {
		t.Fatal("owned root-chain child failed", err, code)
	}
}

type rcQAFDPressure struct {
	old   unix.Rlimit
	fds   []int
	ended bool
	err   error
}

func rcQAPressure(t *testing.T) *rcQAFDPressure {
	t.Helper()
	p := &rcQAFDPressure{}
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &p.old); err != nil {
		t.Fatal(err)
	}
	limited := p.old
	if limited.Cur > 256 {
		limited.Cur = 256
	}
	if limited.Cur < 64 {
		t.Fatal("finite FD-pressure premise", limited.Cur)
	}
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &limited); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.close(); err != nil {
			t.Error(err)
		}
	})
	return p
}
func (p *rcQAFDPressure) fill() error {
	for {
		fd, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if errors.Is(err, unix.EMFILE) {
			return nil
		}
		if err != nil {
			return err
		}
		p.fds = append(p.fds, fd)
	}
}
func (p *rcQAFDPressure) close() error {
	if p.ended {
		return p.err
	}
	p.ended = true
	for i := len(p.fds) - 1; i >= 0; i-- {
		p.err = errors.Join(p.err, unix.Close(p.fds[i]))
	}
	p.fds = nil
	p.err = errors.Join(p.err, unix.Setrlimit(unix.RLIMIT_NOFILE, &p.old))
	return p.err
}
func rcQAFDSet() []int {
	var out []int
	for fd := 0; fd < 256; fd++ {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err == nil {
			out = append(out, fd)
		}
	}
	return out
}
func rcQAContains(list []int, want int) bool {
	for _, n := range list {
		if n == want {
			return true
		}
	}
	return false
}
