//go:build darwin || linux

package ui_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
	"github.com/rbeene/tempo/internal/worker"
)

type qaHWNeverManager struct{ t *testing.T }

func (r qaHWNeverManager) Run(context.Context, worker.Command) (worker.CommandResult, error) {
	r.t.Error("fixture launched real or unrequested service manager")
	return worker.CommandResult{}, errors.New("unrequested manager")
}
func TestQAUIHooksWorkerActualServicePTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_HW_MODE")
	if mode == "" {
		return
	}
	root := os.Getenv("TEMPO_QA_HW_ROOT")
	fd, e := strconv.Atoi(os.Getenv("TEMPO_QA_HW_REPORT_FD"))
	if e != nil || !filepath.IsAbs(root) {
		t.Fatal("invalid isolated fixture")
	}
	reportFile := os.NewFile(uintptr(fd), "hw-report")
	report := func(k, m string) { json.NewEncoder(reportFile).Encode(map[string]string{"kind": k, "message": m}) }
	project := filepath.Join(root, "project")
	if e := os.Mkdir(project, 0700); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"tempo", "runtime"} {
		if e := os.WriteFile(filepath.Join(root, name), []byte("inert synthetic build"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	hooks := hookstate.New(hookstate.Options{Path: filepath.Join(root, "metadata", "hooks.json"), HomeDir: filepath.Join(root, "home"), CodexSystemDir: filepath.Join(root, "system-codex"), ClaudeManagedDir: filepath.Join(root, "system-claude"), Executable: filepath.Join(root, "tempo"), BuildVersion: "qa-ui16", DiscoverRuntime: func(context.Context, string) (hookstate.Runtime, error) {
		return hookstate.Runtime{Path: filepath.Join(root, "runtime"), Version: "2.1.286", Surface: "local"}, nil
	}})
	preview, e := hooks.PreviewInstall(context.Background(), hookstate.InstallIntent{Host: "claude", Scope: "project", Path: project, Operation: "install"})
	if e != nil || len(preview.Changes) == 0 || len(preview.Fingerprint) != 64 {
		t.Fatalf("invalid real hook fixture: %+v %v", preview, e)
	}
	epoch, n := "hw-pty-epoch", "0"
	path := filepath.Join(root, "activity", "state.json")
	local := activity.New(activity.Options{Path: path, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}, nil
	})})
	service, e := worker.New(worker.Options{StatePath: path, ConfigPath: filepath.Join(root, "config.json"), Executable: filepath.Join(root, "tempo"), ServiceDir: filepath.Join(root, "services"), Platform: "darwin", UID: 12345, Sync: local, Runner: qaHWNeverManager{t}})
	if e != nil {
		t.Fatal(e)
	}
	if _, e := service.Stop(context.Background(), worker.ControlRequest{RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}); e != nil {
		t.Fatalf("invalid synthetic real worker control: %v", e)
	}
	report("fixture-valid", project)
	ctx, cancel := context.WithCancelCause(context.Background())
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	signalJoined := make(chan struct{})
	go func() {
		defer close(signalJoined)
		select {
		case sig := <-signals:
			code := 130
			if sig == syscall.SIGTERM {
				code = 143
			}
			cancel(&terminal.ExitError{Code: code})
		case <-ctx.Done():
		}
	}()
	var active, applies, stops, installs, doctors atomic.Int32
	var submittedID string
	var strong error
	credentials := auth.NewService(auth.Options{ConfigPath: filepath.Join(root, "missing-config"), Getenv: func(string) string { return "" }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Error("local diagnostics touched credentials")
		return auth.NativeReply{}, errors.New("forbidden credentials")
	})})
	doctor := setup.New(setup.Options{Auth: credentials, Activity: local, Hooks: hooks, Worker: service})
	options := ui.Options{Refresh: make(chan time.Time), Hooks: &ui.HookActions{Status: hooks.Status, Verify: hooks.Verify, PreviewInstall: hooks.PreviewInstall, ConfirmInstalled: hooks.ConfirmInstalled, Revoke: hooks.Revoke, ApplyInstall: func(ctx context.Context, in hookstate.ApplyInstallInput) (hookstate.HookList, error) {
		active.Add(1)
		defer active.Add(-1)
		applies.Add(1)
		result, e := hooks.ApplyInstall(ctx, in)
		if e != nil {
			return result, e
		}
		submittedID = in.RequestID
		report("applied", in.RequestID)
		return result, nil
	}}, Worker: &ui.WorkerActions{Status: service.Status, Install: func(ctx context.Context, in worker.ControlRequest) (worker.Result, error) {
		installs.Add(1)
		return service.Install(ctx, in)
	}, Start: service.Start, Uninstall: service.Uninstall, Stop: func(ctx context.Context, in worker.ControlRequest) (worker.Result, error) {
		active.Add(1)
		defer active.Add(-1)
		stops.Add(1)
		result, e := service.Stop(ctx, in)
		if e != nil {
			return result, e
		}
		submittedID = in.RequestID
		report("stopped", in.RequestID)
		if strings.HasPrefix(mode, "unknown-") {
			strong = &worker.Error{Code: "local_write_unknown", Uncertain: true, Message: "RAW-MANAGER-CANARY"}
			<-ctx.Done()
			return worker.Result{}, strong
		}
		return result, nil
	}}, Diagnostics: func(ctx context.Context, remote bool) (setup.Diagnostics, error) {
		doctors.Add(1)
		if remote {
			t.Error("local diagnostic became remote")
		}
		return doctor.Doctor(ctx, remote)
	}}
	session, e := terminal.Open(ctx, os.Stdin, os.Stdout)
	if e != nil {
		t.Fatal(e)
	}
	options.OnRetainedOutcome = func(family, id string, e error) {
		if family != "worker" || id != submittedID || e != strong || active.Load() != 0 {
			t.Error("unknown callback lost exact joined outcome")
		}
		report("retained", id)
	}
	e = ui.Run(session.Context(), &qaRunnerPTYScreen{Session: session, report: report}, local, options)
	cancel(nil)
	signal.Stop(signals)
	<-signalJoined
	if strings.HasPrefix(mode, "unknown-") {
		if e != strong {
			t.Errorf("unknown worker lost to EOF/signal: %v", e)
		}
	} else if e != nil {
		t.Errorf("real service UI failed: %v", e)
	}
	if active.Load() != 0 || installs.Load() != 0 {
		t.Error("worker install/owned work escaped consent or UI lifetime")
	}
	wantApply, wantStop, wantDoctor := int32(0), int32(0), int32(0)
	if mode == "hooks-install" {
		wantApply = 1
	}
	if mode == "worker-stop" || strings.HasPrefix(mode, "unknown-") {
		wantStop = 1
	}
	if mode == "doctor-local" {
		wantDoctor = 1
	}
	if applies.Load() != wantApply || stops.Load() != wantStop || doctors.Load() != wantDoctor {
		t.Errorf("unrequested repeat/missing service effects: apply%d stop%d doctor%d", applies.Load(), stops.Load(), doctors.Load())
	}
	if wantApply == 1 {
		list, e := hooks.Status(context.Background(), hookstate.HookSelector{Host: "claude", Scope: "project", Path: project})
		if e != nil || len(list.Hooks) != 1 || list.Hooks[0].State != "approval_required" || list.Hooks[0].Profile.CaptureEligible {
			t.Errorf("installation invented delivery/trust: %+v %v", list, e)
		}
	}
	if wantStop == 1 {
		b, e := os.ReadFile(path + ".worker-control.json")
		if e != nil || !strings.Contains(string(b), submittedID) {
			t.Error("real worker submitted receipt lost")
		}
	}
	if _, e := os.Stat(path); !errors.Is(e, os.ErrNotExist) {
		t.Error("foreground UI initialized capture activity")
	}
	report("closed", "")
}
func TestQAUIHooksWorkerActualTerminalAndSharedServices(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"hooks-preview", "hooks-install", "worker-stop", "unknown-eof", "unknown-signal", "doctor-local"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, python, "-c", qaHWPTYScript, os.Args[0], mode, t.TempDir())
			command.Env = []string{"PATH=/usr/bin:/bin"}
			if b, e := command.CombinedOutput(); e != nil {
				t.Fatalf("actual service terminal%s: %v\n%s", mode, e, b)
			}
		})
	}
}

const qaHWPTYScript = `
import os,sys,pty,termios,subprocess,select,time,fcntl,struct,json,signal
binary,mode,root=sys.argv[1:];master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0));before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL);r,w=os.pipe()
env={'PATH':'/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_HW_MODE':mode,'TEMPO_QA_HW_ROOT':root,'TEMPO_QA_HW_REPORT_FD':str(w)}
p=subprocess.Popen([binary,'-test.run=^TestQAUIHooksWorkerActualServicePTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(w,),preexec_fn=os.setpgrp);os.close(w);transcript=b'';pending=b'';reports=[]
def poll(timeout=.01):
 global transcript,pending
 ready=select.select([master,r],[],[],timeout)[0]
 if master in ready:
  try:transcript+=os.read(master,65536)
  except OSError:pass
 if r in ready:
  pending+=os.read(r,65536)
  while b'\n' in pending:
   line,pending=pending.split(b'\n',1)
   if line:reports.append(json.loads(line))
def kind(k):return [x for x in reports if x['kind']==k]
def frame():return (kind('frame')[-1]['message'] if kind('frame') else '')
def header(title):return title in frame().split('\n')[0]
def until(pred,seconds=2):
 deadline=time.monotonic()+seconds
 while not pred() and time.monotonic()<deadline:poll()
 if not pred():raise AssertionError('missing actual service navigation '+repr(reports[-3:])+' '+repr(transcript[-600:]))
def key(data):os.write(master,data)
def choose(title,choice):until(lambda:header(title));key(choice.encode()+b'\r')
def resize(rows,columns):fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',rows,columns,0,0));os.kill(p.pid,signal.SIGWINCH)
def no_mutation():
 for _ in range(8):poll(.01)
 if kind('applied') or kind('stopped'):raise AssertionError('operation preceded reviewed explicit consent')
try:
 until(lambda:bool(kind('fixture-valid')));project=kind('fixture-valid')[0]['message'];until(lambda:header('TEMPO'))
 if mode.startswith('hooks-'):
  key(b'h');choose('Hooks and capture','preview' if mode=='hooks-preview' else 'install');choose('Choose hook host','claude');choose('Choose hook scope','project');until(lambda:header('Absolute project context path'))
  key(b'\x1b[200~'+project.encode()+b'\n\x1b[201~');no_mutation()
  if not header('Absolute project context path'):raise AssertionError('paste submitted path')
  if mode=='hooks-install':
   # Establish a narrow viewport before constructing the confirmation. A warning
   # already seen in full at120x24 stays reviewed after a later resize.
   resize(12,80);until(lambda:header('Absolute project context path') and max(len(line) for line in frame().split('\n'))<=79)
  key(b'\r')
  if mode=='hooks-preview':choose('Choose preview operation','install');until(lambda:header('Hooks · Read-only preview'));key(b'\r')
  else:
   until(lambda:header('Review scoped change'));resize(7,39);until(lambda:header('Terminal too small'));key(b'y\r');no_mutation();resize(12,80);until(lambda:header('Review scoped change'));key(b'y\r');no_mutation()
   for _ in range(80):key(b'\x1b[B');poll(.005)
   until(lambda:'Request ID' in frame());key(b'y\r');until(lambda:header('Hooks · Complete'));key(b'\r')
 elif mode=='doctor-local':key(b'd');choose('Diagnostics','local');until(lambda:'credential_unverified' in frame());key(b'\r')
 else:
  key(b'w')
  if mode=='worker-stop':
   choose('Worker controls','install');until(lambda:header('Review scoped change'));resize(7,39);until(lambda:header('Terminal too small'));key(b'y\r');no_mutation();resize(24,120);until(lambda:header('Review scoped change'));key(b'n\r');until(lambda:header('TEMPO'));key(b'w')
  choose('Worker controls','stop');until(lambda:bool(kind('stopped')))
  if mode=='unknown-eof':key(b'\x04')
  elif mode=='unknown-signal':os.kill(p.pid,signal.SIGTERM)
  else:until(lambda:header('Worker · Complete'));key(b'\r')
 if not mode.startswith('unknown-'):until(lambda:header('TEMPO'));key(b'q')
 until(lambda:bool(kind('closed')));deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll()
 if p.poll() is None:raise AssertionError('real service UI retained owned work')
 for _ in range(5):poll()
 after=termios.tcgetattr(slave);afterflags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':afterflags&=~0x10000;flags&=~0x10000;after[3]&=~termios.PENDIN;before[3]&=~termios.PENDIN
 if after!=before or afterflags!=flags:raise AssertionError('real service UI did not restore original TTY')
 if p.returncode or b'FAIL' in transcript:raise AssertionError('real service child failed '+repr(transcript[-1200:]))
 for seq in (b'\x1b[?1049h',b'\x1b[?25l',b'\x1b[?2004h',b'\x1b[0m',b'\x1b[?25h',b'\x1b[?1049l',b'\x1b[?2004l'):
  if seq not in transcript:raise AssertionError('missing actual terminal restoration '+repr(seq))
 if b'RAW-MANAGER-CANARY' in transcript:raise AssertionError('raw service error leaked')
 if mode.startswith('unknown-') and len(kind('retained'))!=1:raise AssertionError('joined shared worker uncertainty missing')
 print('actual terminal/shared hook worker/doctor verified '+mode)
finally:
 if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait()
 for fd in (r,master,slave):os.close(fd)
`
