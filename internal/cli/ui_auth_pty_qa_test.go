package cli_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const qaAuthPTYSecret = "synthetic-ui-auth-secret"

func TestQAUIAuthCLIPrivateNativeChild(t *testing.T) {
	mode := ""
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "qa-auth-native=") {
			mode = strings.TrimPrefix(arg, "qa-auth-native=")
		}
	}
	if mode == "" {
		return
	}
	in, out := os.NewFile(3, "private-request"), os.NewFile(4, "private-reply")
	defer in.Close()
	defer out.Close()
	var r auth.NativeRequest
	if json.NewDecoder(in).Decode(&r) != nil {
		os.Exit(31)
	}
	for _, s := range append(os.Args, os.Environ()...) {
		if strings.Contains(s, qaAuthPTYSecret) {
			os.Exit(32)
		}
	}
	if r.Operation != "login" || string(r.Token) != qaAuthPTYSecret {
		os.Exit(33)
	}
	if os.WriteFile(r.ConfigPath+".pid", []byte(strconv.Itoa(os.Getpid())), 0600) != nil {
		os.Exit(34)
	}
	if os.WriteFile(r.ConfigPath+".possible-dispatch", []byte("synthetic-dispatch"), 0600) != nil {
		os.Exit(35)
	}
	clear(r.Token)
	if strings.HasPrefix(mode, "unknown-") {
		time.Sleep(time.Minute)
		os.Exit(36)
	}
	json.NewEncoder(out).Encode(auth.NativeReply{Effects: auth.Effects{Credential: "applied", Config: "saved"}})
	os.Exit(0)
}

type qaCLIAuthProvider struct{ harvest.Provider }

func (qaCLIAuthProvider) Accounts(context.Context) ([]harvest.Object, error) {
	return []harvest.Object{{"id": "11", "product": "harvest", "name": "Synthetic account"}}, nil
}
func TestQAUIAuthCLIActionPTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_CLI_AUTH_MODE")
	if mode == "" {
		return
	}
	root := os.Getenv("TEMPO_QA_CLI_AUTH_ROOT")
	fd, e := strconv.Atoi(os.Getenv("TEMPO_QA_CLI_AUTH_REPORT_FD"))
	if e != nil || !filepath.IsAbs(root) {
		t.Fatal("invalid isolated fixture")
	}
	f := os.NewFile(uintptr(fd), "cli-auth-report")
	report := func(k, m string) { json.NewEncoder(f).Encode(map[string]string{"kind": k, "message": m}) }
	config := filepath.Join(root, "config")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	native := auth.ProcessRunner{Executable: os.Args[0], Args: []string{"-test.run=^TestQAUIAuthCLIPrivateNativeChild$", "--", "qa-auth-native=" + mode}, Timeout: 5 * time.Second}
	s := auth.NewService(auth.Options{ConfigPath: config, LockPath: filepath.Join(root, "lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, NewProvider: func(string, string) harvest.Provider { return qaCLIAuthProvider{} }, Runner: auth.RunnerFunc(func(ctx context.Context, r auth.NativeRequest, lock *os.File) (auth.NativeReply, error) {
		watchCtx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-tick.C:
					if _, e := os.Stat(config + ".possible-dispatch"); e == nil {
						report("dispatched", "")
						return
					}
				case <-watchCtx.Done():
					return
				}
			}
		}()
		reply, e := native.Run(ctx, r, lock)
		stop()
		<-done
		if b, pe := os.ReadFile(config + ".pid"); pe == nil {
			pid, _ := strconv.Atoi(string(b))
			if e := syscall.Kill(pid, 0); !errors.Is(e, syscall.ESRCH) {
				t.Error("CLI auth child not reaped")
			}
			report("reaped", "")
		}
		report("action-joined", "")
		if mode == "known-cancel" && e == nil {
			cancel(&terminal.ExitError{Code: 143})
		}
		return reply, e
	})})
	epoch, elapsed := "synthetic-auth-cli-epoch", "0"
	local := activity.New(activity.Options{Path: filepath.Join(root, "absent-state", "activity.json"), Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC), Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &elapsed}, nil
	})})
	code := cli.Run(ctx, []string{"ui"}, os.Stdin, os.Stdout, os.Stderr, cli.Dependencies{Activity: local, Auth: s, ConfigPath: config, Getenv: func(string) string { return "" }})
	want := 0
	if strings.HasPrefix(mode, "unknown-") {
		want = 8
	}
	if mode == "known-cancel" {
		want = 143
	}
	if code != want {
		t.Errorf("CLI auth exit%d want%d", code, want)
	}
	if _, e = os.Stat(filepath.Join(root, "absent-state")); !errors.Is(e, os.ErrNotExist) {
		t.Error("auth wrote activity state")
	}
	report("outcome", strconv.Itoa(code))
	report("closed", "")
}
func TestQAUIAuthCLIActualTerminalNativeOutcomes(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"success", "known-cancel", "unknown-eof"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", qaCLIAuthPTYScript, os.Args[0], mode, t.TempDir())
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if b, e := cmd.CombinedOutput(); e != nil {
				t.Fatalf("auth CLI%s: %v\n%s", mode, e, b)
			}
		})
	}
}

const qaCLIAuthPTYScript = `
import os,sys,pty,termios,subprocess,select,time,fcntl,struct,json
binary,mode,root=sys.argv[1:]
master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0))
before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL)
report_r,report_w=os.pipe()
env={'PATH':'/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_CLI_AUTH_MODE':mode,'TEMPO_QA_CLI_AUTH_ROOT':root,'TEMPO_QA_CLI_AUTH_REPORT_FD':str(report_w)}
p=subprocess.Popen([binary,'-test.run=^TestQAUIAuthCLIActionPTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(report_w,),preexec_fn=os.setpgrp)
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
 until(lambda:header(b'TEMPO'));key(b'a');until(lambda:header(b'Accounts and auth'));key(b'login\r');until(lambda:header(b'Harvest personal access token'))
 key(b'\x1b[200~synthetic-ui-auth-secret\x1b[201~');key(b'\r');until(lambda:header(b'Choose a Harvest account'));key(b'11\r');until(lambda:b'Replace the saved credential' in transcript.split(b'\x1b[H\x1b[J')[-1]);key(b'y\r')
 if mode.startswith('unknown-'):
  until(lambda:any(r['kind']=='dispatched' for r in reports));key(b'\x04')
 elif mode=='success':until(lambda:header(b'Auth complete'));key(b'\r');until(lambda:header(b'TEMPO'));key(b'q')
 until(closed)

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
 if b'synthetic-ui-auth-secret' in transcript:raise AssertionError('secret leaked after CLI auth')
 reset=transcript.rfind(b'\x1b[?1049l');after_cleanup=transcript[reset:].lower()
 if mode.startswith('unknown-'):
  if b'credential_write_unknown' not in after_cleanup or b'unknown' not in after_cleanup:raise AssertionError('CLI did not safely report unknown effects after restoration')
 else:
  if b'applied' not in after_cleanup or b'saved' not in after_cleanup:raise AssertionError('CLI hid valid completed AuthResult after restoration')
 if [r['message'] for r in reports if r['kind']=='outcome'][-1]!=('8' if mode.startswith('unknown-') else ('143' if mode=='known-cancel' else '0')):raise AssertionError('CLI classified auth incorrectly')
 if not any(r['kind']=='reaped' for r in reports):raise AssertionError('CLI native helper not joined/reaped')
 print('actual CLI auth native outcome verified: '+mode)
finally:
 if p.poll() is None:p.kill();p.wait()
 os.close(report_r);os.close(master);os.close(slave)
`
