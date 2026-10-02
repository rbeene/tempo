package cli_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/themes"
)

func TestQAUILinksCLIActionPTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_CLI_LINK_MODE")
	if mode == "" {
		return
	}
	fd, err := strconv.Atoi(os.Getenv("TEMPO_QA_CLI_LINK_REPORT_FD"))
	if err != nil {
		t.Fatal(err)
	}
	reportFile := os.NewFile(uintptr(fd), "cli-link-report")
	report := func(kind string, value any) {
		_ = json.NewEncoder(reportFile).Encode(map[string]any{"kind": kind, "value": value})
	}
	root := os.Getenv("TEMPO_QA_CLI_LINK_ROOT")
	if !filepath.IsAbs(root) {
		t.Fatal("isolated absolute fixture required")
	}
	project := filepath.Join(root, "project")
	if err := os.Mkdir(project, 0700); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(root, "activity", "state.json")
	epoch, elapsed := "synthetic-cli-link", "0"
	service := activity.New(activity.Options{Path: state, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC), Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &elapsed}, nil
	}), HookPolicies: hookstate.New(hookstate.Options{Path: filepath.Join(root, "missing-hooks.json")})})
	first, err := service.Link(context.Background(), activity.LinkInput{Path: project, ProjectID: "100", TaskID: "200", AccountID: "11", Timezone: "UTC", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, activity.LinkDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { return &qaCLILinkAPI{}, nil }})
	if err != nil {
		var local *activity.Error
		if errors.As(err, &local) {
			t.Fatalf("synthetic shared seed failed: code=%s", local.Code)
		}
		t.Fatal("synthetic shared seed failed before UI")
	}
	before, err := os.ReadFile(state)
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "malformed-config")
	configBytes := []byte("synthetic malformed account configuration")
	if err := os.WriteFile(config, configBytes, 0600); err != nil {
		t.Fatal(err)
	}
	a := auth.NewService(auth.Options{ConfigPath: config, LockPath: filepath.Join(root, "forbidden-auth-lock"), Getenv: func(string) string { t.Error("offline unlink read auth environment"); return "" }, NewProvider: func(string, string) harvest.Provider { t.Error("offline unlink constructed provider"); return nil }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Error("offline unlink accessed credentials")
		return auth.NativeReply{}, errors.New("forbidden native access")
	})})
	report("binding", first.Binding.ID)
	preferences := filepath.Join(root, "appearance-private", "preferences.json")
	code := cli.Run(context.Background(), []string{"ui"}, os.Stdin, os.Stdout, os.Stderr, cli.Dependencies{Activity: service, Auth: a, ConfigPath: config, Themes: themes.New(themes.Options{Path: preferences}), Getenv: func(key string) string {
		switch key {
		case "TERM":
			return "dumb"
		case "COLORTERM", "NO_COLOR":
			return ""
		}
		t.Error("local UI unlink resolved account config")
		return ""
	}})
	if _, err := os.Stat(filepath.Dir(preferences)); !errors.Is(err, os.ErrNotExist) {
		t.Error("UI without Appearance selection initialized private preferences")
	}
	if code != 0 {
		t.Errorf("actual UI action CLI exit=%d", code)
	}
	list, err := service.ListBindings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if mode == "unlink" {
		if len(list.Bindings) != 0 {
			t.Error("CLI UI did not dispatch shared Unlink")
		}
	} else {
		after, err := os.ReadFile(state)
		if err != nil || !reflect.DeepEqual(before, after) || len(list.Bindings) != 1 {
			t.Error("canceled CLI UI confirmation changed shared state")
		}
	}
	actualConfig, err := os.ReadFile(config)
	if err != nil || !reflect.DeepEqual(configBytes, actualConfig) {
		t.Error("UI local action changed malformed auth configuration")
	}
	for _, path := range []string{"forbidden-auth-lock", "missing-hooks.json"} {
		if _, err := os.Stat(filepath.Join(root, path)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("UI action wrote unrelated fixture %s", path)
		}
	}
	report("closed", code)
}

func TestQAUILinksCLIActualSharedUnlinkAndCanceledConsent(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"unlink", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", qaUILinksCLIPTYScript, os.Args[0], mode, t.TempDir(), filepath.Dir(git))
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("actual CLI link PTY %s: %v\n%s", mode, err, output)
			}
		})
	}
}

const qaUILinksCLIPTYScript = `
import os,sys,pty,termios,subprocess,select,time,fcntl,struct,json
binary,mode,root,gitdir=sys.argv[1:]
master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0))
before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL)
report_r,report_w=os.pipe()
env={'PATH':gitdir+':/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_CLI_LINK_MODE':mode,'TEMPO_QA_CLI_LINK_ROOT':root,'TEMPO_QA_CLI_LINK_REPORT_FD':str(report_w)}
p=subprocess.Popen([binary,'-test.run=^TestQAUILinksCLIActionPTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(report_w,),preexec_fn=os.setpgrp)
os.close(report_w);transcript=b'';pending=b'';reports=[]
def poll(timeout=.02):
 global transcript,pending
 ready,_,_=select.select([master,report_r],[],[],timeout)
 if master in ready:
  try:transcript+=os.read(master,65536)
  except OSError:pass
 if report_r in ready:
  pending+=os.read(report_r,65536)
  while b'\n' in pending:
   line,pending=pending.split(b'\n',1)
   if line:reports.append(json.loads(line))
def closed():return any(r['kind']=='closed' for r in reports)
def header(title):return title in transcript.split(b'\x1b[H\x1b[J')[-1].split(b'\r\n')[0]
def until(pred,seconds=2):
 deadline=time.monotonic()+seconds
 while not pred() and time.monotonic()<deadline:
  poll()
  if closed() and not pred():break
 if not pred():raise AssertionError('missing CLI link behavior '+repr(reports)+' '+repr(transcript[-1000:]))
def key(data):os.write(master,data)
try:
 until(lambda:header(b'TEMPO'));key(b'l');until(lambda:header(b'Links actions'))
 binding=[r['value'] for r in reports if r['kind']=='binding'][0]
 key(binding.encode()+b'\r');until(lambda:header(b'Links \xc2\xb7 Binding actions'))
 key(b'unlink\r');until(lambda:header(b'Review scoped change'))
 if mode=='cancel':key(b'\x1b')
 else:key(b'y\r');until(lambda:header(b'Links \xc2\xb7 Complete'));key(b'\r')
 until(lambda:header(b'TEMPO'));key(b'q');until(closed)
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll(.01)
 if p.poll() is None:raise AssertionError('CLI action retained owned work')
 while select.select([master],[],[],.01)[0]:
  old=len(transcript);poll(.01)
  if len(transcript)==old:break
 after=termios.tcgetattr(slave);after_flags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':after_flags&=~0x10000;flags&=~0x10000;after[3]&=~termios.PENDIN;before[3]&=~termios.PENDIN
 if before!=after or flags!=after_flags:raise AssertionError('CLI action terminal restoration failed')
 for seq in (b'\x1b[?1049h',b'\x1b[?25l',b'\x1b[?2004h',b'\x1b[0m',b'\x1b[?25h',b'\x1b[?1049l',b'\x1b[?2004l'):
  if seq not in transcript:raise AssertionError('missing CLI terminal lifetime '+repr(seq))
 if b'"schema_version"' in transcript or b'"data"' in transcript:raise AssertionError('interactive action leaked trailing finite envelope')
 if p.returncode or b'FAIL' in transcript:raise AssertionError('CLI child failed '+repr(transcript[-1000:]))
 print('actual CLI shared unlink/consent verified: '+mode)
finally:
 if p.poll() is None:p.kill();p.wait()
 os.close(report_r);os.close(master);os.close(slave)
`
