package worker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/rbeene/tempo/internal/activity"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestQAWorkerProcessHelper(t *testing.T) {
	state := os.Getenv("TEMPO_QA_WORKER_STATE")
	if state == "" {
		return
	}
	a := &qaWorkerSync{t: t}
	qaWorkerQueued(a)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(filepath.Dir(state))
	o := Options{StatePath: state, ConfigPath: filepath.Join(root, "config.json"), Executable: exe, ServiceDir: filepath.Join(root, "services"), Platform: "darwin", UID: 501, Sync: a, Runner: qaWorkerRunnerFunc(func(context.Context, Command) (CommandResult, error) {
		t.Fatal("child invoked manager")
		return CommandResult{}, nil
	}), NewRequestID: func() (string, error) { return qaWorkerID(81), nil }}
	s := qaWorkerNew(t, o)
	s.fail = func(stage string) error {
		if stage == "runtime_after_pending_saved" {
			r := qaWorkerReadRuntime(t, o)
			if r.Pending == nil || r.Pending.RequestID != qaWorkerID(81) {
				t.Fatal("child missing pending")
			}
			fmt.Println("QA_WORKER_OWNED")
			select {}
		}
		return nil
	}
	if err = s.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestQAWorkerProcessDeathReleasesOwnershipAndRetainsPending(t *testing.T) {
	o, a := qaWorkerOptions(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestQAWorkerProcessHelper$", "-test.count=1")
	cmd.Env = []string{"TEMPO_QA_WORKER_STATE=" + o.StatePath, "GORACE=atexit_sleep_ms=0"}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(out)
		for scanner.Scan() {
			if scanner.Text() == "QA_WORKER_OWNED" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("child exited before owning worker")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("child never reached durable ownership barrier")
	}
	before := qaWorkerReadRuntime(t, o)
	if before.Pending == nil || before.Pending.RequestID != qaWorkerID(81) {
		t.Fatal("child did not durably reserve")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, _ = ctx, cancel
	err = qaWorkerNew(t, o).Run(ctx)
	cancel()
	qaWorkerCode(t, err, "state_busy")
	if a.calls != 0 || a.reads != 0 {
		t.Fatalf("second owner touched sync reads%d calls%d", a.reads, a.calls)
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	waited = true
	a.run = func(_ context.Context, in activity.SyncRunInput, _ activity.SyncDependencies) (activity.SyncRun, error) {
		if in != *before.Pending {
			t.Fatalf("restart replaced killed owner's pass%+v", in)
		}
		return activity.SyncRun{ContractVersion: 1, RequestID: in.RequestID, State: "complete"}, nil
	}
	o.NewRequestID = func() (string, error) { t.Fatal("killed owner recovery minted UUID"); return "", nil }
	restarted := qaWorkerNew(t, o)
	saved := 0
	restarted.fail = func(stage string) error {
		if stage == "runtime_after_result_saved" {
			saved++
			return errors.New("stop after recovery")
		}
		return nil
	}
	err = qaWorkerBoundedRun(t, restarted)
	if err == nil || saved != 1 || a.calls != 1 {
		t.Fatalf("OS lock not released/replay missing saved%d calls%d err%v", saved, a.calls, err)
	}
	after := qaWorkerReadRuntime(t, o)
	if after.Pending != nil || after.LastSuccess != nil {
		t.Fatalf("bad killed-process recovery%+v", after)
	}
}

func TestQAWorkerRunnerHelper(t *testing.T) {
	if len(os.Args) < 3 {
		return
	}
	mode := os.Args[len(os.Args)-2]
	marker := os.Args[len(os.Args)-1]
	if !strings.HasPrefix(mode, "qa-runner-") {
		return
	}
	// Publish readiness only after the complete PID is visible. Stat must not
	// observe an empty marker between file creation and the payload write.
	pending := marker + ".pending"
	if err := os.WriteFile(pending, []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(pending, marker); err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "qa-runner-success":
		fmt.Fprint(os.Stdout, "safe fixture stdout")
		fmt.Fprint(os.Stderr, "safe fixture stderr")
	case "qa-runner-overflow":
		fmt.Fprint(os.Stdout, strings.Repeat("synthetic-private-output", 2000))
		fmt.Fprint(os.Stderr, strings.Repeat("synthetic-private-output", 2000))
	case "qa-runner-failure":
		fmt.Fprint(os.Stderr, "synthetic-private-output")
		os.Exit(7)
	case "qa-runner-hang":
		for {
			time.Sleep(time.Hour)
		}
	default:
		t.Fatal("bad synthetic runner mode")
	}
	os.Exit(0)
}
func qaWorkerRunnerCommand(t *testing.T, mode string) (Command, string) {
	t.Helper()
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "child.pid")
	return Command{Executable: exe, Args: []string{"-test.run=^TestQAWorkerRunnerHelper$", "--", "qa-runner-" + mode, marker}}, marker
}
func qaWorkerRunnerPID(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("synthetic command never ran: %v", err)
	}
	pid, err := strconv.Atoi(string(b))
	if err != nil || pid <= 0 {
		t.Fatal("bad synthetic PID")
	}
	return pid
}
func qaWorkerAssertReaped(t *testing.T, pid int) {
	t.Helper()
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("owned subprocess still exists pid%d err%v", pid, err)
	}
	var status syscall.WaitStatus
	if _, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil); !errors.Is(err, syscall.ECHILD) {
		t.Fatalf("owned subprocess not reaped pid%d err%v", pid, err)
	}
}
func TestQAWorkerProcessRunnerCapturesBoundedSuccess(t *testing.T) {
	command, marker := qaWorkerRunnerCommand(t, "success")
	result, err := (ProcessRunner{}).Run(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	pid := qaWorkerRunnerPID(t, marker)
	if result.ExitCode != 0 || string(result.Stdout) != "safe fixture stdout" || string(result.Stderr) != "safe fixture stderr" {
		t.Fatalf("wrong captured result%+v", result)
	}
	qaWorkerAssertReaped(t, pid)
}
func TestQAWorkerProcessRunnerAggregateLimitAndSafeFailure(t *testing.T) {
	for _, mode := range []string{"overflow", "failure"} {
		t.Run(mode, func(t *testing.T) {
			command, marker := qaWorkerRunnerCommand(t, mode)
			result, err := (ProcessRunner{}).Run(context.Background(), command)
			pid := qaWorkerRunnerPID(t, marker)
			if err == nil || strings.Contains(err.Error(), "synthetic-private-output") || len(result.Stdout) != 0 || len(result.Stderr) != 0 {
				t.Fatalf("unsafe runner failure result bytes%d/%d err%v", len(result.Stdout), len(result.Stderr), err)
			}
			qaWorkerAssertReaped(t, pid)
		})
	}
}
func TestQAWorkerProcessRunnerCancellationKillsAndReaps(t *testing.T) {
	command, marker := qaWorkerRunnerCommand(t, "hang")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := (ProcessRunner{}).Run(ctx, command); done <- err }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	ready := false
	for !ready {
		select {
		case err := <-done:
			t.Fatalf("runner exited before child readiness%v", err)
		case <-deadline.C:
			t.Fatal("runner never started child")
		case <-ticker.C:
			_, err := os.Stat(marker)
			ready = err == nil
		}
	}
	pid := qaWorkerRunnerPID(t, marker)
	reaped := false
	t.Cleanup(func() {
		if !reaped {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled runner returned success")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not terminate owned child")
	}
	qaWorkerAssertReaped(t, pid)
	reaped = true
}
func TestQAWorkerProcessRunnerHardDeadlineWithoutCallerTimeout(t *testing.T) {
	command, marker := qaWorkerRunnerCommand(t, "hang")
	started := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchdog := time.AfterFunc(8*time.Second, cancel)
	defer watchdog.Stop()
	_, err := (ProcessRunner{}).Run(ctx, command)
	pid := qaWorkerRunnerPID(t, marker)
	reaped := false
	t.Cleanup(func() {
		if !reaped {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	if err == nil || time.Since(started) > 8*time.Second {
		t.Fatalf("manager command escaped5s bound elapsed%v err%v", time.Since(started), err)
	}
	qaWorkerAssertReaped(t, pid)
	reaped = true
}
