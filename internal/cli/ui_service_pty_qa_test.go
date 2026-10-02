//go:build darwin || linux

package cli_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/themes"
	"github.com/rbeene/tempo/internal/worker"
)

type qaCLIServiceNeverManager struct{ t *testing.T }

func (r qaCLIServiceNeverManager) Run(context.Context, worker.Command) (worker.CommandResult, error) {
	r.t.Error("read-only CLI service view touched manager")
	return worker.CommandResult{}, errors.New("forbidden manager")
}
func TestQAUIServiceCLIReadPTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_SERVICE_CLI_MODE")
	if mode == "" {
		return
	}
	root := os.Getenv("TEMPO_QA_SERVICE_CLI_ROOT")
	fd, e := strconv.Atoi(os.Getenv("TEMPO_QA_SERVICE_CLI_REPORT_FD"))
	if e != nil || !filepath.IsAbs(root) {
		t.Fatal("invalid isolated CLI fixture")
	}
	file := os.NewFile(uintptr(fd), "service-cli-report")
	report := func(k, m string) { json.NewEncoder(file).Encode(map[string]string{"kind": k, "message": m}) }
	project := filepath.Join(root, "project")
	if e := os.Mkdir(project, 0700); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"tempo", "runtime"} {
		if e := os.WriteFile(filepath.Join(root, name), []byte("inert synthetic build"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	hooksPath := filepath.Join(root, "hook-state", "hooks.json")
	path := filepath.Join(root, "activity", "state.json")
	config := filepath.Join(root, "malformed-config")
	configBytes := []byte("synthetic malformed account config")
	if e := os.WriteFile(config, configBytes, 0600); e != nil {
		t.Fatal(e)
	}
	hooks := hookstate.New(hookstate.Options{Path: hooksPath, HomeDir: filepath.Join(root, "home"), CodexSystemDir: filepath.Join(root, "system-codex"), ClaudeManagedDir: filepath.Join(root, "system-claude"), Executable: filepath.Join(root, "tempo"), DiscoverRuntime: func(context.Context, string) (hookstate.Runtime, error) {
		return hookstate.Runtime{Path: filepath.Join(root, "runtime"), Version: "2.1.286", Surface: "local"}, nil
	}})
	epoch, n := "cli-service-epoch", "0"
	local := activity.New(activity.Options{Path: path, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}, nil
	})})
	service, e := worker.New(worker.Options{StatePath: path, ConfigPath: config, Executable: filepath.Join(root, "tempo"), ServiceDir: filepath.Join(root, "services"), Platform: "darwin", UID: 12345, Sync: local, Runner: qaCLIServiceNeverManager{t}})
	if e != nil {
		t.Fatal(e)
	}
	credentials := auth.NewService(auth.Options{ConfigPath: config, Getenv: func(string) string { return "" }, NewProvider: func(string, string) harvest.Provider {
		t.Error("local service view constructed remote provider")
		return nil
	}, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Error("local service view accessed credentials")
		return auth.NativeReply{}, errors.New("forbidden credentials")
	})})
	if list, e := hooks.Status(context.Background(), hookstate.HookSelector{Host: "claude", Scope: "project", Path: project}); e != nil || len(list.Hooks) != 1 {
		t.Fatalf("invalid real Hooks read fixture: %+v %v", list, e)
	}
	if _, e := service.Status(context.Background()); e != nil {
		t.Fatalf("invalid real Worker read fixture: %v", e)
	}
	if _, e := setup.New(setup.Options{Auth: credentials, Activity: local, Hooks: hooks, Worker: service}).Doctor(context.Background(), false); e != nil {
		t.Fatalf("invalid real Doctor fixture: %v", e)
	}
	report("fixture-valid", project)
	preferences := filepath.Join(root, "appearance-private", "preferences.json")
	code := cli.Run(context.Background(), []string{"ui"}, os.Stdin, os.Stdout, os.Stderr, cli.Dependencies{Activity: local, Hooks: hooks, Worker: service, Auth: credentials, ConfigPath: config, Themes: themes.New(themes.Options{Path: preferences}), Getenv: func(key string) string {
		switch key {
		case "TERM":
			return "dumb"
		case "COLORTERM", "NO_COLOR":
			return ""
		}
		t.Error("injected dashboard inspected default service/environment paths")
		return ""
	}})
	if _, err := os.Stat(filepath.Dir(preferences)); !errors.Is(err, os.ErrNotExist) {
		t.Error("read-only service view initialized private preferences")
	}
	if code != 0 {
		t.Errorf("CLI service view exit%d", code)
	}
	for _, p := range []string{path, path + ".worker-control.json", hooksPath} {
		if _, e := os.Stat(p); !errors.Is(e, os.ErrNotExist) {
			t.Errorf("read-only menu/quit created service state: %s", p)
		}
	}
	if b, e := os.ReadFile(config); e != nil || string(b) != string(configBytes) {
		t.Error("service read changed account config")
	}
	report("closed", "")
}
func TestQAUIServiceCLIActualReadViewsStayLazyOfflineFiniteSafe(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"hooks", "worker", "doctor"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, python, "-c", qaCLIServicePTYScript, os.Args[0], mode, t.TempDir())
			command.Env = []string{"PATH=/usr/bin:/bin"}
			if b, e := command.CombinedOutput(); e != nil {
				t.Fatalf("actual CLI%s read: %v\n%s", mode, e, b)
			}
		})
	}
}

const qaCLIServicePTYScript = `
import os,sys,pty,termios,subprocess,select,time,fcntl,struct,json,signal
binary,mode,root=sys.argv[1:];master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0));before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL);r,w=os.pipe()
env={'PATH':'/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_SERVICE_CLI_MODE':mode,'TEMPO_QA_SERVICE_CLI_ROOT':root,'TEMPO_QA_SERVICE_CLI_REPORT_FD':str(w)}
p=subprocess.Popen([binary,'-test.run=^TestQAUIServiceCLIReadPTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(w,),preexec_fn=os.setpgrp);os.close(w);pending=b'';transcript=b'';reports=[]
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
def frame():return transcript.split(b'\x1b[H\x1b[J')[-1]
def header(title):return title in frame().split(b'\r\n')[0]
def until(pred,seconds=2):
 deadline=time.monotonic()+seconds
 while not pred() and time.monotonic()<deadline:poll()
 if not pred():raise AssertionError('missing actual CLI service view '+repr(reports)+' '+repr(transcript[-900:]))
def key(data):os.write(master,data)
def choose(title,choice):until(lambda:header(title));key(choice.encode()+b'\r')
try:
 until(lambda:bool(kind('fixture-valid')));project=kind('fixture-valid')[0]['message'];until(lambda:header(b'TEMPO'))
 if mode=='hooks':
  key(b'h');choose(b'Hooks and capture','status');choose(b'Choose hook host','claude');choose(b'Choose hook scope','project');until(lambda:header(b'Absolute project context path'));key(b'\x1b[200~'+project.encode()+b'\x1b[201~\r');until(lambda:header('Hooks · Read-only evidence'.encode()));key(b'\r')
 elif mode=='worker':key(b'w');choose(b'Worker controls','status');until(lambda:header('Worker · Read-only status'.encode()));key(b'\r')
 else:key(b'd');choose(b'Diagnostics','local');until(lambda:b'credential_unverified' in frame());key(b'\r')
 until(lambda:header(b'TEMPO'));key(b'q');until(lambda:bool(kind('closed')));deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll()
 if p.poll() is None:raise AssertionError('CLI view retained work')
 for _ in range(5):poll()
 after=termios.tcgetattr(slave);afterflags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':afterflags&=~0x10000;flags&=~0x10000;after[3]&=~termios.PENDIN;before[3]&=~termios.PENDIN
 if after!=before or afterflags!=flags:raise AssertionError('CLI service view did not restore original TTY')
 if p.returncode or b'FAIL' in transcript:raise AssertionError('CLI view child failed '+repr(transcript[-1200:]))
 if b'"schema_version"' in transcript or b'"data"' in transcript:raise AssertionError('opened UI emitted trailing finite envelope')
 for seq in (b'\x1b[?1049h',b'\x1b[?2004h',b'\x1b[0m',b'\x1b[?1049l',b'\x1b[?2004l'):
  if seq not in transcript:raise AssertionError('missing actual CLI restoration '+repr(seq))
 print('actual CLI offline service view verified '+mode)
finally:
 if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait()
 for fd in (r,master,slave):os.close(fd)
`
