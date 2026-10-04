package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/worker"
)

const qaWorkerCLIRequest = "12000000-0000-4000-8000-000000000701"

type qaWorkerCLIActions struct {
	t      *testing.T
	reads  int
	status func(context.Context) (activity.SyncStatus, error)
}

func (a *qaWorkerCLIActions) SyncStatus(ctx context.Context) (activity.SyncStatus, error) {
	a.reads++
	if a.status != nil {
		return a.status(ctx)
	}
	return activity.SyncStatus{ContractVersion: 1, Worker: activity.WorkerStatus{QueuedCount: 7, SubmittingCount: 2, UnknownCount: 3, SyncEnabled: false}}, nil
}
func (a *qaWorkerCLIActions) SyncNow(context.Context, activity.SyncRunInput, activity.SyncDependencies) (activity.SyncRun, error) {
	a.t.Fatal("finite or paused CLI invoked sync")
	return activity.SyncRun{}, nil
}

type qaWorkerCLIRunner struct{ t *testing.T }

func (r qaWorkerCLIRunner) Run(context.Context, worker.Command) (worker.CommandResult, error) {
	r.t.Fatal("offline CLI invoked service manager")
	return worker.CommandResult{}, nil
}
func qaWorkerCLIDeps(t *testing.T, platform string) (cli.Dependencies, *qaWorkerCLIActions, string) {
	t.Helper()
	d, path := qaSyncCLIDeps(t)
	a := &qaWorkerCLIActions{t: t}
	w, err := worker.New(worker.Options{StatePath: path, ConfigPath: d.ConfigPath, Executable: filepath.Join(filepath.Dir(path), "tempo"), ServiceDir: filepath.Join(filepath.Dir(path), "services"), Platform: platform, UID: 501, Sync: a, Runner: qaWorkerCLIRunner{t}})
	if err != nil {
		t.Fatal(err)
	}
	d.Worker = w
	return d, a, path
}
func TestQAWorkerCLIOfflineStatusUsesOneSnapshot(t *testing.T) {
	for _, flag := range []string{"--json", "--non-interactive", ""} {
		t.Run(flag, func(t *testing.T) {
			d, a, path := qaWorkerCLIDeps(t, "darwin")
			args := []string{"worker", "status"}
			if flag != "" {
				args = append(args, flag)
			}
			v := qaSyncCLIEnvelope(t, args, d, 0)
			data, _ := v["data"].(map[string]any)
			status, _ := data["status"].(map[string]any)
			if data["contract_version"] != float64(1) || status["queued_count"] != float64(7) || status["submitting_count"] != float64(2) || status["unknown_count"] != float64(3) || status["sync_enabled"] != false || status["installed"] != nil || status["instance_mode"] != nil || a.reads != 1 {
				t.Fatalf("wrong composed status: %+v reads=%d", v, a.reads)
			}
			if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
				t.Fatalf("status wrote absent state: %v", err)
			}
		})
	}
}
func TestQAWorkerCLIControlsDispatchTypedOfflineErrors(t *testing.T) {
	for _, action := range []string{"install", "start", "stop", "uninstall"} {
		t.Run(action, func(t *testing.T) {
			d, _, path := qaWorkerCLIDeps(t, "plan9")
			args := []string{"worker", action, "--request-id", qaWorkerCLIRequest, "--non-interactive"}
			if action == "install" || action == "uninstall" {
				args = append(args, "--yes")
			}
			v := qaSyncCLIEnvelope(t, args, d, 1)
			e, _ := v["error"].(map[string]any)
			if e["code"] != "unsupported" || e["uncertain"] != false {
				t.Fatalf("worker error lost typed meaning: %+v", v)
			}
			if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
				t.Fatalf("unsupported service wrote state: %v", err)
			}
		})
	}
}
func TestQAWorkerCLIConfirmationAndValidationPrecedeEffects(t *testing.T) {
	for _, action := range []string{"install", "uninstall"} {
		t.Run(action, func(t *testing.T) {
			d, _, _ := qaWorkerCLIDeps(t, "darwin")
			v := qaSyncCLIEnvelope(t, []string{"worker", action, "--request-id", qaWorkerCLIRequest, "--json"}, d, 6)
			e, _ := v["error"].(map[string]any)
			if e["code"] != "confirmation_required" {
				t.Fatalf("missing explicit confirmation: %+v", v)
			}
		})
	}
	for _, args := range [][]string{{"worker", "install", "--yes", "--request-id", "bad"}, {"worker", "start", "--yes", "--request-id", qaWorkerCLIRequest}, {"worker", "stop", "--yes", "--request-id", qaWorkerCLIRequest}, {"worker", "status", "--request-id", qaWorkerCLIRequest}, {"worker", "run", "--request-id", qaWorkerCLIRequest}, {"worker", "run", "--yes"}, {"worker", "run", "unexpected"}, {"worker", "status", "--unknown"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			d, _, path := qaWorkerCLIDeps(t, "darwin")
			qaSyncCLIEnvelope(t, append(args, "--non-interactive"), d, 2)
			if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
				t.Fatalf("invalid input wrote state: %v", err)
			}
		})
	}
}
func TestQAWorkerCLITypedErrorDurabilityAndRedaction(t *testing.T) {
	for _, tc := range []struct {
		code      string
		exit      int
		uncertain bool
	}{{"state_busy", 6, false}, {"local_write_unknown", 8, true}, {"control_history_full", 1, false}} {
		t.Run(tc.code, func(t *testing.T) {
			d, a, _ := qaWorkerCLIDeps(t, "darwin")
			a.status = func(context.Context) (activity.SyncStatus, error) {
				return activity.SyncStatus{}, &worker.Error{Code: tc.code, Message: "fixed safe worker category", Uncertain: tc.uncertain, Retryable: tc.code == "state_busy"}
			}
			v := qaSyncCLIEnvelope(t, []string{"worker", "status", "--json"}, d, tc.exit)
			e, _ := v["error"].(map[string]any)
			if e["code"] != tc.code || e["uncertain"] != tc.uncertain || e["retryable"] != (tc.code == "state_busy") {
				t.Fatalf("worker typed outcome lost: %+v", v)
			}
		})
	}
	d, a, _ := qaWorkerCLIDeps(t, "darwin")
	a.status = func(context.Context) (activity.SyncStatus, error) {
		return activity.SyncStatus{}, errors.New("SECRET_RAW_MANAGER_REPLY\x1b[31m")
	}
	v := qaSyncCLIEnvelope(t, []string{"worker", "status", "--json"}, d, 1)
	e, _ := v["error"].(map[string]any)
	if e["code"] != "internal" || strings.Contains(e["message"].(string), "SECRET") {
		t.Fatalf("raw diagnostic leaked: %+v", v)
	}
}
func TestQAWorkerRunLifetimeSelectorOnlyAcceptsValidatedRun(t *testing.T) {
	for _, args := range [][]string{{"worker", "run"}, {"worker", "run", "--json"}, {"--non-interactive", "worker", "run"}, {"worker", "run", "--json", "--non-interactive"}} {
		if !cli.WorkerRunInvocation(args) {
			t.Errorf("valid explicit lifetime rejected: %v", args)
		}
	}
	for _, args := range [][]string{nil, {"worker", "status"}, {"worker", "start", "--request-id", qaWorkerCLIRequest}, {"worker", "run", "--yes"}, {"worker", "run", "--request-id", qaWorkerCLIRequest}, {"worker", "run", "--help"}, {"worker", "run", "--json=false"}, {"worker", "run", "--unknown"}, {"worker", "run", "extra"}, {"worker", "run", "--json", "--json"}, {"time", "create", "--notes", "worker run"}} {
		if cli.WorkerRunInvocation(args) {
			t.Errorf("finite/malformed input got indefinite lifetime: %v", args)
		}
	}
}
func TestQAWorkerCLIRunCancellationPreservesSignalExitAfterCleanup(t *testing.T) {
	for _, exit := range []int{130, 143} {
		t.Run(strconv.Itoa(exit), func(t *testing.T) {
			d, a, path := qaWorkerCLIDeps(t, "darwin")
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			watchdog := time.AfterFunc(2*time.Second, func() { cancel(errors.New("QA watchdog")) })
			defer watchdog.Stop()
			a.status = func(context.Context) (activity.SyncStatus, error) {
				cancel(&terminal.ExitError{Code: exit})
				return activity.SyncStatus{ContractVersion: 1, Worker: activity.WorkerStatus{}}, nil
			}
			var out, stderr bytes.Buffer
			code := cli.Run(ctx, []string{"worker", "run", "--non-interactive"}, strings.NewReader(""), &out, &stderr, d)
			if code != exit || a.reads == 0 || out.Len() != 0 || stderr.Len() != 0 {
				t.Fatalf("signal mapping/lifetime wrong: exit=%d reads=%d stdout=%q stderr=%q", code, a.reads, out.String(), stderr.String())
			}
			status := worker.Observe(context.Background(), path, activity.WorkerStatus{State: "running"})
			if status.State == "running" {
				t.Fatal("signal exit left worker ownership live")
			}
		})
	}
}
func TestQAWorkerCLISchemaAndHelpStayOffline(t *testing.T) {
	d, _, _ := qaWorkerCLIDeps(t, "darwin")
	for _, args := range [][]string{{"schema"}, {"help"}} {
		var out, stderr bytes.Buffer
		if code := cli.Run(context.Background(), args, strings.NewReader(""), &out, &stderr, d); code != 0 {
			t.Fatalf("offline command failed %v: %s", args, stderr.String())
		}
		for _, name := range []string{"worker install", "worker start", "worker status", "worker stop", "worker uninstall", "worker run"} {
			if !strings.Contains(out.String(), name) {
				t.Errorf("%s omits %s", args[0], name)
			}
		}
	}
}

func TestQAWorkerCLICompiledForegroundSignalsReleaseOwnership(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "tempo")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer buildCancel()
	build := exec.CommandContext(buildCtx, filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", binary, "../../cmd/tempo")
	if b, err := build.CombinedOutput(); err != nil {
		t.Fatalf("compile CLI: %v %s", err, b)
	}
	for _, tc := range []struct {
		signal os.Signal
		exit   int
	}{{os.Interrupt, 130}, {syscall.SIGTERM, 143}} {
		t.Run(strconv.Itoa(tc.exit), func(t *testing.T) {
			owned := t.TempDir()
			path := filepath.Join(owned, "state", "activity.json")
			home := filepath.Join(owned, "home")
			configHome := filepath.Join(home, "config")
			if err := os.MkdirAll(configHome, 0700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "worker", "run", "--non-interactive")
			// The child owns its user service roots even when the suite deliberately
			// omits HOME. Do not inherit or modify an actual user's service directory.
			cmd.Env = append(os.Environ(), "HOME="+home, "XDG_CONFIG_HOME="+configHome, "TEMPO_WORKER_MODE=foreground", "TEMPO_STATE="+path, "TEMPO_CONFIG="+filepath.Join(root, "unused-config"), "HARVEST_TOKEN=", "HARVEST_ACCOUNT_ID=")
			var out, stderr bytes.Buffer
			cmd.Stdout = &out
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			waited := false
			defer func() {
				if !waited {
					_ = cmd.Process.Kill()
					<-done
				}
			}()
			ready := time.NewTimer(3 * time.Second)
			defer ready.Stop()
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
		readiness:
			for {
				select {
				case err := <-done:
					waited = true
					t.Fatalf("worker exited before owning lifetime: %v stdout=%q stderr=%q", err, out.String(), stderr.String())
				case <-ready.C:
					t.Fatal("worker did not establish bounded startup ownership")
				case <-tick.C:
					if _, err := os.Stat(path + ".worker-runtime.json"); err == nil {
						if worker.Observe(context.Background(), path, activity.WorkerStatus{}).State == "running" {
							break readiness
						}
					}
				}
			}
			client := exec.CommandContext(ctx, binary, "sync", "status", "--json")
			client.Env = cmd.Env
			if b, err := client.CombinedOutput(); err != nil {
				t.Fatalf("independent finite client failed: %v %s", err, b)
			}
			select {
			case err := <-done:
				waited = true
				t.Fatalf("finite client exit stopped independent worker: %v", err)
			default:
			}
			if worker.Observe(context.Background(), path, activity.WorkerStatus{}).State != "running" {
				t.Fatal("finite client exit lost worker ownership")
			}
			if err := cmd.Process.Signal(tc.signal); err != nil {
				t.Fatal(err)
			}
			var err error
			select {
			case err = <-done:
				waited = true
			case <-ctx.Done():
				t.Fatal("worker did not terminate after signal")
			}
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != tc.exit {
				t.Fatalf("signal exit wrong: %v stdout=%q stderr=%q", err, out.String(), stderr.String())
			}
			if out.Len() != 0 || stderr.Len() != 0 {
				t.Fatalf("signal path emitted unexpected output: %q %q", out.String(), stderr.String())
			}
			if worker.Observe(context.Background(), path, activity.WorkerStatus{State: "running"}).State == "running" {
				t.Fatal("process signal left ownership live")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("empty worker created the selector file: %v", err)
			}
			snapshot, err := activity.New(activity.Options{Path: path}).Status(context.Background())
			if err != nil || snapshot.ComputerID != nil || snapshot.SnapshotRevision != "0" {
				t.Fatalf("empty worker initialized SQLite capture identity: %+v %v", snapshot, err)
			}
		})
	}
}

func TestQAWorkerCLIControlsGenerateCanonicalRequestWhenOmitted(t *testing.T) {
	for _, action := range []string{"install", "start", "stop", "uninstall"} {
		t.Run(action, func(t *testing.T) {
			d, _, path := qaWorkerCLIDeps(t, "plan9")
			args := []string{"worker", action, "--json"}
			if action == "install" || action == "uninstall" {
				args = append(args, "--yes")
			}
			// Unsupported platform is checked by the real API after canonical request validation.
			// Reaching this error proves omitted CLI identity was supplied to the API.
			v := qaSyncCLIEnvelope(t, args, d, 1)
			e, _ := v["error"].(map[string]any)
			if e["code"] != "unsupported" {
				t.Fatalf("missing CLI request was not generated: %+v", v)
			}
			if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
				t.Fatalf("unsupported control wrote state: %v", err)
			}
		})
	}
}

func qaWorkerNotificationSocket(t *testing.T) (string, *net.UnixConn) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "tempo-wq-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path, err := activity.ResolveStatePath(filepath.Join(dir, "activity.json"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path + ".worker.sock", Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return path, c
}
func qaWorkerNotification(t *testing.T, c *net.UnixConn, want string) {
	t.Helper()
	deadline := 150 * time.Millisecond
	if want == "" {
		deadline = 30 * time.Millisecond
	}
	_ = c.SetReadDeadline(time.Now().Add(deadline))
	var b [32]byte
	n, _, err := c.ReadFromUnix(b[:])
	if want == "" {
		if err == nil {
			t.Fatalf("unexpected readiness notification %q", b[:n])
		}
		if e, ok := err.(net.Error); !ok || !e.Timeout() {
			t.Fatal(err)
		}
		return
	}
	if err != nil || string(b[:n]) != want {
		t.Fatalf("notification=%q err=%v want=%q", b[:n], err, want)
	}
}
func qaWorkerNotificationActivity(t *testing.T, path string) (cli.Dependencies, activity.Event, *int64) {
	t.Helper()
	seconds := new(int64)
	base := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	s := activity.New(activity.Options{Path: path, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		epoch, n := "notification-boot", strconv.FormatInt(*seconds*int64(time.Second), 10)
		return activity.ClockSample{Capability: "available", WallUTC: base.Add(time.Duration(*seconds) * time.Second), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}, nil
	})})
	linked, err := s.Link(context.Background(), activity.LinkInput{Path: filepath.Dir(path), AccountID: "11", ProjectID: "100", TaskID: "200", Timezone: "UTC", RequestID: "12000000-0000-4000-8000-000000000801"}, activity.LinkDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { return &qaCLILinkAPI{}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	event := activity.Event{ContractVersion: 1, Actor: activity.ActorKey{ComputerID: *snapshot.ComputerID, Source: "manual-test", SessionID: "notification", AgentID: "root"}, Generation: "1", Sequence: "1", EventID: "work", Kind: "work", BindingID: linked.Binding.ID, BindingRevision: linked.Binding.Revision}
	d := cli.Dependencies{Activity: s, Store: &fakeStore{}, ConfigPath: filepath.Join(filepath.Dir(path), "cfg"), Getenv: func(k string) string {
		switch k {
		case "TEMPO_STATE":
			return path
		case "HARVEST_TOKEN":
			return "synthetic-secret"
		}
		return ""
	}, NewProvider: func(string, string) harvest.Provider { return &qaCLILinkAPI{} }, Prompter: qaForbiddenPrompt{t}, TerminalEligible: func(io.Reader, io.Writer) bool { return false }}
	return d, event, seconds
}
func TestQAWorkerCLIDurableCaptureDispatchesWake(t *testing.T) {
	for _, socketAvailable := range []bool{true, false} {
		t.Run(strconv.FormatBool(socketAvailable), func(t *testing.T) {
			path, c := qaWorkerNotificationSocket(t)
			d, e, seconds := qaWorkerNotificationActivity(t, path)
			if _, err := d.Activity.Ingest(context.Background(), e); err != nil {
				t.Fatal(err)
			}
			if !socketAvailable {
				c.Close()
				if err := os.Remove(path + ".worker.sock"); err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
			}
			*seconds = 10
			e.Sequence = "2"
			e.EventID = "finish"
			e.Kind = "finish"
			e.BindingID = ""
			e.BindingRevision = ""
			b, _ := json.Marshal(e)
			var out, stderr bytes.Buffer
			start := time.Now()
			code := cli.Run(context.Background(), []string{"activity", "event", "--input-stdin", "--json"}, strings.NewReader(string(b)), &out, &stderr, d)
			envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 0, "")
			if time.Since(start) > time.Second {
				t.Fatal("wake availability delayed durable capture beyond bounded budget")
			}
			s, err := d.Activity.Status(context.Background())
			if err != nil || len(s.ClosedIntervals) != 1 || s.ClosedIntervals[0].DurationNS != "10000000000" {
				t.Fatalf("capture outcome not durably committed: %+v %v", s, err)
			}
			if socketAvailable {
				qaWorkerNotification(t, c, "wake")
			}
		})
	}
}
func TestQAWorkerCLIReadinessDispatchesAfterSyncCommit(t *testing.T) {
	for _, mode := range []string{"resume", "configure"} {
		t.Run(mode, func(t *testing.T) {
			path, c := qaWorkerNotificationSocket(t)
			d, _, _ := qaWorkerNotificationActivity(t, path)
			args := []string{"sync", mode, "--request-id", "12000000-0000-4000-8000-000000000802", "--json"}
			if mode == "configure" {
				args = append(args, "--account", "11", "--mode", "duration", "--duration-policy", "exact", "--if-revision", "0", "--yes")
			}
			qaSyncCLIEnvelope(t, args, d, 0)
			s, err := d.Activity.SyncStatus(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if mode == "resume" && !s.Enabled || mode == "configure" && len(s.Configurations) != 1 {
				t.Fatalf("readiness action not committed: %+v", s)
			}
			qaWorkerNotification(t, c, "recheck")
		})
	}
}
func TestQAWorkerCLINotificationSkipsReadsValidationAndUnknown(t *testing.T) {
	path, c := qaWorkerNotificationSocket(t)
	d, _, _ := qaWorkerNotificationActivity(t, path)
	qaSyncCLIEnvelope(t, []string{"sync", "status", "--json"}, d, 0)
	qaWorkerNotification(t, c, "")
	qaSyncCLIEnvelope(t, []string{"sync", "resume", "--request-id", "invalid", "--json"}, d, 2)
	qaWorkerNotification(t, c, "")
	d.Auth = auth.NewService(auth.Options{ConfigPath: d.ConfigPath, LockPath: filepath.Join(filepath.Dir(path), "auth.lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, NewProvider: func(string, string) harvest.Provider { return &fakeAPI{} }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		return auth.NativeReply{}, &auth.Error{Code: "credential_write_unknown", Message: "synthetic uncertain save", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}
	})})
	var out, stderr bytes.Buffer
	code := cli.Run(context.Background(), []string{"auth", "login", "--token-stdin", "--account", "11", "--json"}, strings.NewReader("synthetic-secret"), &out, &stderr, d)
	envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 8, "credential_write_unknown")
	var failure struct {
		Error struct {
			Uncertain bool `json:"uncertain"`
		} `json:"error"`
	}
	if err := json.Unmarshal(stderr.Bytes(), &failure); err != nil || !failure.Error.Uncertain {
		t.Fatalf("did not reach uncertain credential outcome: %v %s", err, stderr.String())
	}
	qaWorkerNotification(t, c, "")
}
func TestQAWorkerCLIAuthCommitDispatchesRecheck(t *testing.T) {
	path, c := qaWorkerNotificationSocket(t)
	d, _, _ := qaWorkerNotificationActivity(t, path)
	d.Getenv = func(k string) string {
		if k == "TEMPO_STATE" {
			return path
		}
		return ""
	}
	var out, stderr bytes.Buffer
	code := qaLegacyRun(t, context.Background(), []string{"auth", "login", "--token-stdin", "--account", "11", "--json"}, strings.NewReader("synthetic-secret"), &out, &stderr, d)
	envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 0, "")
	cfg, err := auth.Load(d.ConfigPath)
	if err != nil || cfg.Account != "11" || d.Store.(*fakeStore).sets != 1 {
		t.Fatalf("auth success not durably applied: %+v %v", cfg, err)
	}
	qaWorkerNotification(t, c, "recheck")
}
func TestQAWorkerCLISetupPartialAuthCommitStillRechecks(t *testing.T) {
	path, c := qaWorkerNotificationSocket(t)
	dir := filepath.Dir(path)
	stored := false
	commits := 0
	a := auth.NewService(auth.Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: filepath.Join(dir, "auth.lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, NewProvider: func(string, string) harvest.Provider { return &fakeAPI{} }, Runner: auth.RunnerFunc(func(_ context.Context, r auth.NativeRequest, l *os.File) (auth.NativeReply, error) {
		if r.Operation == "read" {
			if stored {
				return auth.NativeReply{Token: []byte("synthetic-secret"), Effects: auth.Effects{Credential: "unchanged", Config: "unchanged"}}, nil
			}
			return auth.NativeReply{Code: "not_found", Effects: auth.Effects{Credential: "unchanged", Config: "unchanged"}}, nil
		}
		if r.Operation != "login" || l == nil {
			t.Fatalf("unexpected auth operation %s", r.Operation)
		}
		stored = true
		commits++
		if err := auth.Save(filepath.Join(dir, "cfg"), auth.Config{Account: r.AccountID}); err != nil {
			t.Fatal(err)
		}
		return auth.NativeReply{Effects: auth.Effects{Credential: "applied", Config: "saved"}}, nil
	})})
	p := &qaCLIPartialPrompt{t: t}
	var out, stderr bytes.Buffer
	code := cli.Run(context.Background(), []string{"setup", "--path", dir}, strings.NewReader(""), &out, &stderr, cli.Dependencies{Auth: a, Activity: activity.New(activity.Options{Path: path}), Prompter: p, TerminalEligible: func(io.Reader, io.Writer) bool { return true }, ConfigPath: filepath.Join(dir, "cfg"), Getenv: func(k string) string {
		if k == "TEMPO_STATE" {
			return path
		}
		return ""
	}})
	if code == 0 || commits != 1 || p.confirms != 1 {
		t.Fatalf("did not reach auth commit followed by link failure: exit%d commits%d confirms%d stderr=%q", code, commits, p.confirms, stderr.String())
	}
	cfg, err := auth.Load(filepath.Join(dir, "cfg"))
	if err != nil || cfg.Account != "11" {
		t.Fatalf("partial auth did not persist: %+v %v", cfg, err)
	}
	qaWorkerNotification(t, c, "recheck")
}
