package cli_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestQAUIActivityCLIReadPTYChild(t *testing.T) {
	if os.Getenv("TEMPO_QA_CLI_ACTIVITY_MODE") == "" {
		return
	}
	root := os.Getenv("TEMPO_QA_CLI_ACTIVITY_ROOT")
	fd, e := strconv.Atoi(os.Getenv("TEMPO_QA_CLI_ACTIVITY_REPORT_FD"))
	if e != nil || !filepath.IsAbs(root) {
		t.Fatal("invalidfixture")
	}
	f := os.NewFile(uintptr(fd), "activity-report")
	report := func(k string, v any) { json.NewEncoder(f).Encode(map[string]any{"kind": k, "value": v}) }
	state := filepath.Join(root, "absent-state", "activity.json")
	epoch, elapsed := "synthetic-cli-activity", "0"
	local := activity.New(activity.Options{Path: state, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC), Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &elapsed}, nil
	})})
	if _, err := local.Status(context.Background()); err != nil {
		var ae *activity.Error
		errors.As(err, &ae)
		t.Fatalf("synthetic absent Status fixture invalid: %#v", ae)
	}
	report("fixture", "absent Status valid")
	config := filepath.Join(root, "config")
	before := []byte("synthetic-malformed-config")
	if e = os.WriteFile(config, before, 0600); e != nil {
		t.Fatal(e)
	}
	a := auth.NewService(auth.Options{ConfigPath: config, Getenv: func(string) string { t.Error("Activity read resolved credentials"); return "" }, PersistentAvailable: func() bool { t.Error("Activity read queried persistence"); return false }, NewProvider: func(string, string) harvest.Provider { t.Error("Activity read built remoteprovider"); return nil }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Error("Activity read accessed credentials")
		return auth.NativeReply{}, errors.New("forbidden")
	})})
	code := cli.Run(context.Background(), []string{"ui"}, os.Stdin, os.Stdout, os.Stderr, cli.Dependencies{Activity: local, Auth: a, ConfigPath: config, Getenv: func(string) string { t.Error("Activity read touched config selection"); return "" }})
	if code != 0 {
		t.Errorf("Activity read CLI exit%d", code)
	}
	after, _ := os.ReadFile(config)
	if string(after) != string(before) {
		t.Error("Activity read changed config")
	}
	if _, e = os.Stat(filepath.Dir(state)); !errors.Is(e, os.ErrNotExist) {
		t.Error("Activity read initializedstate")
	}
	report("closed", code)
}
func TestQAUIActivityCLIActualTerminalReadNavigation(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, "-c", qaCLIActivityPTYScript, os.Args[0], "actors", t.TempDir())
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	if b, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("actual CLI Activityread: %v\n%s", e, b)
	}
}

const qaCLIActivityPTYScript = `
import os,sys,pty,termios,subprocess,select,time,fcntl,struct,json
binary,mode,root=sys.argv[1:]
master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0))
before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL)
report_r,report_w=os.pipe()
env={'PATH':'/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_CLI_ACTIVITY_MODE':mode,'TEMPO_QA_CLI_ACTIVITY_ROOT':root,'TEMPO_QA_CLI_ACTIVITY_REPORT_FD':str(report_w)}
p=subprocess.Popen([binary,'-test.run=^TestQAUIActivityCLIReadPTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(report_w,),preexec_fn=os.setpgrp)
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
 until(lambda:header(b'TEMPO'));key(b'x');until(lambda:header(b'Activity actions'));key(b'actors\r')
 until(lambda:b'No working or waiting actors' in transcript.split(b'\x1b[H\x1b[J')[-1]);key(b'\r');until(lambda:header(b'TEMPO'));key(b'q');until(closed)

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
 print('actual CLI Activity readonly navigation verified: '+mode)
finally:
 if p.poll() is None:p.kill();p.wait()
 os.close(report_r);os.close(master);os.close(slave)
`
