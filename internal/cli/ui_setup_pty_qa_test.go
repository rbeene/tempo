//go:build darwin || linux

package cli_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
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
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
)

type qaCLISetupProvider struct {
	harvest.Provider
	fail bool
}

func (qaCLISetupProvider) Accounts(context.Context) ([]harvest.Object, error) {
	return []harvest.Object{{"id": "11", "product": "harvest", "name": "Synthetic account"}}, nil
}
func (qaCLISetupProvider) Get(context.Context, string) (harvest.Object, error) {
	return harvest.Object{"id": "2", "is_active": true, "timezone": "UTC"}, nil
}
func (p qaCLISetupProvider) List(context.Context, string, url.Values) ([]harvest.Object, error) {
	if p.fail {
		return nil, &harvest.Error{Code: "network", Message: "RAW-SETUP-NETWORK-CANARY"}
	}
	return []harvest.Object{{"is_active": true, "project": harvest.Object{"id": "100", "name": "Synthetic project"}, "task_assignments": []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": "200", "name": "Synthetic task"}}}}}, nil
}

func TestQAUISetupCLISharedWizardChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_SETUP_MODE")
	if mode == "" {
		return
	}
	root := os.Getenv("TEMPO_QA_SETUP_ROOT")
	fd, e := strconv.Atoi(os.Getenv("TEMPO_QA_SETUP_REPORT_FD"))
	if e != nil || !filepath.IsAbs(root) {
		t.Fatal("invalid owned fixture")
	}
	reportFile := os.NewFile(uintptr(fd), "setup-report")
	report := func(kind, message string) {
		json.NewEncoder(reportFile).Encode(map[string]string{"kind": kind, "message": message})
	}
	project := filepath.Join(root, "project")
	if e = os.Mkdir(project, 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.Chdir(project); e != nil {
		t.Fatal(e)
	}
	config := filepath.Join(root, "config")
	state := filepath.Join(root, "state", "activity.json")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	signalDone := make(chan struct{})
	go func() {
		defer close(signalDone)
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
	var nativeActive, logins, reads, providers atomic.Int32
	stored := false
	nativeMode := "success"
	if strings.HasPrefix(mode, "unknown-") {
		nativeMode = "unknown-eof"
	}
	nativeTimeout := 5 * time.Second
	if mode == "unknown-back" {
		nativeTimeout = 150 * time.Millisecond
	}
	native := auth.ProcessRunner{Executable: os.Args[0], Args: []string{"-test.run=^TestQAUIAuthCLIPrivateNativeChild$", "--", "qa-auth-native=" + nativeMode}, Timeout: nativeTimeout}
	credentials := auth.NewService(auth.Options{ConfigPath: config, LockPath: filepath.Join(root, "auth.lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, NewProvider: func(token, account string) harvest.Provider {
		providers.Add(1)
		if token != qaAuthPTYSecret || account != "" && account != "11" {
			t.Error("wizard changed private provider scope")
		}
		return qaCLISetupProvider{fail: mode == "partial-network"}
	}, Runner: auth.RunnerFunc(func(actionCtx context.Context, in auth.NativeRequest, lock *os.File) (auth.NativeReply, error) {
		if in.Operation == "read" {
			reads.Add(1)
			if stored {
				return auth.NativeReply{Token: []byte(qaAuthPTYSecret)}, nil
			}
			return auth.NativeReply{Code: "not_found"}, nil
		}
		if in.Operation != "login" || lock == nil {
			t.Fatal("wizard attempted unexpected auth mutation")
		}
		logins.Add(1)
		nativeActive.Add(1)
		defer nativeActive.Add(-1)
		watchCtx, stop := context.WithCancel(actionCtx)
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-tick.C:
					if _, e := os.Stat(config + ".possible-dispatch"); e == nil {
						report("possible-dispatch", "")
						return
					}
				case <-watchCtx.Done():
					return
				}
			}
		}()
		reply, outcome := native.Run(actionCtx, in, lock)
		stop()
		<-watchDone
		if pidBytes, e := os.ReadFile(config + ".pid"); e == nil {
			pid, _ := strconv.Atoi(string(pidBytes))
			if e = syscall.Kill(pid, 0); !errors.Is(e, syscall.ESRCH) {
				t.Error("setup private native child was not reaped")
			}
			report("native-reaped", "")
		}
		if outcome == nil {
			if e := auth.Save(config, auth.Config{Account: in.AccountID}); e != nil {
				t.Fatal(e)
			}
			stored = true
			report("auth-committed", "")
		}
		if mode == "known-cancel" && outcome == nil {
			cancel(&terminal.ExitError{Code: 143})
		}
		report("native-joined", "")
		return reply, outcome
	})})
	epoch, elapsed := "synthetic-guided-setup", "0"
	local := activity.New(activity.Options{Path: state, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC), Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &elapsed}, nil
	})})
	var hooks *hookstate.Service
	if mode == "hooks-install" {
		for _, name := range []string{"tempo", "runtime"} {
			if e = os.WriteFile(filepath.Join(root, name), []byte("inert synthetic build"), 0600); e != nil {
				t.Fatal(e)
			}
		}
		hooks = hookstate.New(hookstate.Options{Path: filepath.Join(root, "hook-state", "hooks.json"), HomeDir: filepath.Join(root, "synthetic-home"), CodexSystemDir: filepath.Join(root, "system-codex"), ClaudeManagedDir: filepath.Join(root, "system-claude"), Executable: filepath.Join(root, "tempo"), BuildVersion: "qa-setup", DiscoverRuntime: func(_ context.Context, host string) (hookstate.Runtime, error) {
			version := "0.159.3"
			if host == "claude" {
				version = "2.1.286"
			}
			return hookstate.Runtime{Path: filepath.Join(root, "runtime"), Version: version, Surface: "local"}, nil
		}})
		if preview, e := hooks.PreviewInstall(context.Background(), hookstate.InstallIntent{Host: "both", Scope: "project", Path: project, Operation: "install"}); e != nil || len(preview.Changes) == 0 || len(preview.Fingerprint) != 64 {
			t.Fatalf("actual inert installer fixture invalid: %+v %v", preview, e)
		}
	}
	shared := setup.New(setup.Options{Auth: credentials, Activity: local, Hooks: hooks})
	before, e := shared.Run(context.Background(), setup.Input{Path: project}, nil)
	if e != nil || before.ContractVersion != 1 || before.Complete || logins.Load() != 0 || reads.Load() != 0 || providers.Load() != 0 {
		t.Fatalf("invalid lazy readiness fixture %+v %v", before, e)
	}
	if _, e = os.Stat(filepath.Dir(state)); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("preflight readiness wrote state")
	}
	report("fixture-valid", project)
	code := cli.Run(ctx, []string{"ui"}, os.Stdin, os.Stdout, os.Stderr, cli.Dependencies{Activity: local, Auth: credentials, Hooks: hooks, ConfigPath: config, Getenv: func(key string) string {
		if key != "TEMPO_STATE" {
			t.Errorf("guided UI used undeclared default dependency %s", key)
		}
		return ""
	}})
	cancel(nil)
	signal.Stop(signals)
	<-signalDone
	want := 0
	if strings.HasPrefix(mode, "unknown-") {
		want = 8
	}
	if mode == "known-cancel" {
		want = 143
	}
	if code != want {
		t.Errorf("guided Setup exit%d want%d", code, want)
	}
	if nativeActive.Load() != 0 {
		t.Error("terminal return preceded joined native action")
	}
	if mode == "menu-back" {
		if logins.Load() != 0 || reads.Load() != 0 || providers.Load() != 0 {
			t.Error("opening/back Setup caused auth/provider action")
		}
	} else if logins.Load() != 1 {
		t.Error("wizard/unknown recovery repeated native mutation")
	}
	if !strings.HasPrefix(mode, "unknown-") && mode != "menu-back" {
		cfg, e := auth.Load(config)
		if e != nil || cfg.Account != "11" {
			t.Error("known partial lost applied synthetic config")
		}
	}
	if mode == "hooks-install" {
		list, e := local.ListBindings(context.Background())
		if e != nil || len(list.Bindings) != 1 {
			t.Fatal("guided shared Link was not applied")
		}
		status, e := hooks.Status(context.Background(), hookstate.HookSelector{Host: "both", Scope: "project", Path: project})
		if e != nil || len(status.Hooks) != 2 {
			t.Fatal("guided shared installer lost both scopes")
		}
		for _, row := range status.Hooks {
			if row.State != "approval_required" || row.Profile.CaptureEligible {
				t.Error("setup installation invented approval/capture delivery")
			}
		}
	} else if _, e = os.Stat(filepath.Dir(state)); !errors.Is(e, os.ErrNotExist) {
		t.Error("unknown/partial/declined wizard wrote link state")
	}
	report("outcome", strconv.Itoa(code))
	report("closed", "")
}

func TestQAUISetupCLIActualSharedGuidedPartialUnknownAndCancel(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal(e)
	}
	git, e := exec.LookPath("git")
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"menu-back", "partial-network", "link-declined", "known-cancel", "unknown-eof", "unknown-sigterm", "unknown-back", "hooks-install"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", qaCLISetupPTYScript, os.Args[0], mode, t.TempDir(), filepath.Dir(git))
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if output, e := cmd.CombinedOutput(); e != nil {
				t.Fatalf("guided Setup CLI%s: %v\n%s", mode, e, output)
			}
		})
	}
}

const qaCLISetupPTYScript = `
import os,sys,pty,termios,subprocess,select,time,signal,fcntl,struct,json
binary,mode,root,gitdir=sys.argv[1:]
master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0))
before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL);report_r,report_w=os.pipe()
env={'PATH':gitdir+':/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_SETUP_MODE':mode,'TEMPO_QA_SETUP_ROOT':root,'TEMPO_QA_SETUP_REPORT_FD':str(report_w)}
p=subprocess.Popen([binary,'-test.run=^TestQAUISetupCLISharedWizardChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(report_w,),preexec_fn=os.setpgrp)
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
def frame():return transcript.split(b'\x1b[H\x1b[J')[-1]
def header(title):return frame().split(b'\r\n')[0]==title
def menu(title):return header(title) and b'Search: ' in frame()
def closed():return any(r['kind']=='closed' for r in reports)
def until(pred,seconds=2):
 deadline=time.monotonic()+seconds
 while not pred() and time.monotonic()<deadline:
  poll()
  if closed() and not pred():break
 if not pred():raise AssertionError('missing guided Setup behavior '+repr(reports)+' '+repr(transcript[-1200:]))
def key(data):os.write(master,data)
def resize(rows,columns):fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',rows,columns,0,0));os.kill(p.pid,signal.SIGWINCH)
def setupmenu():key(b',');until(lambda:menu(b'Setup'))
def dashboard():until(lambda:frame().startswith(b'TEMPO'))
try:
 until(lambda:any(r['kind']=='fixture-valid' for r in reports));dashboard();setupmenu()
 if mode=='menu-back':key(b'back\r');dashboard();key(b'q')
 else:
  key(b'guided\r');until(lambda:header(b'Harvest personal access token (hidden)'))
  key(b'\x1b[200~synthetic-ui-auth-secret\x1b[201~');poll(.04)
  if any(r['kind']=='possible-dispatch' for r in reports):raise AssertionError('paste submitted secret without Enter')
  key(b'\r');until(lambda:b'Save this credential securely for account 11?' in frame());key(b'y\r')
  if mode in ('unknown-eof','unknown-sigterm'):
   until(lambda:any(r['kind']=='possible-dispatch' for r in reports));key(b'\x04') if mode=='unknown-eof' else os.kill(p.pid,signal.SIGTERM)
  elif mode=='unknown-back':
   until(lambda:header(b'Setup \xc2\xb7 Outcome unknown') and b'Search: ' not in frame());key(b'\r');until(lambda:menu(b'Setup \xc2\xb7 Outcome unknown'));key(b'back\r');dashboard()
   key(b'a');until(lambda:menu(b'Accounts and auth'));key(b'login\r');until(lambda:b'unavailable' in frame());key(b'\x1b');dashboard()
   key(b',');until(lambda:menu(b'Setup \xc2\xb7 Outcome unknown'));key(b'readiness\r');until(lambda:header(b'Setup \xc2\xb7 Readiness'));key(b'\r');until(lambda:menu(b'Setup \xc2\xb7 Outcome unknown'));key(b'back\r');dashboard();key(b'q')
  elif mode!='known-cancel':
   until(lambda:header(b'Directory to link (Enter uses current directory)'));key(b'\x1b[200~'+(root+'/project').encode()+b'\x1b[201~');key(b'\r')
   if mode in ('link-declined','hooks-install'):
    until(lambda:menu(b'Choose a Harvest project'));key(b'100\r');until(lambda:header(b'IANA timezone (for example UTC or America/New_York)'));key(b'UTC\r');until(lambda:b'Link directory' in frame());key(b'n\r' if mode=='link-declined' else b'y\r')
   if mode=='hooks-install':
    until(lambda:menu(b'Hooks and capture policy'));resize(12,80);until(lambda:menu(b'Hooks and capture policy') and max(map(len,frame().split(b'\r\n')))<80)
    key(b'\x1b[B\r');until(lambda:header(b'Review scoped change') and b'Read full warning before Yes' in frame());key(b'y\r');poll(.05)
    if not header(b'Review scoped change') or os.path.exists(root+'/project/.claude/settings.json'):raise AssertionError('hidden setup warning committed')
    resize(7,39);until(lambda:header(b'Terminal too small'));key(b'y\r');poll(.04)
    if os.path.exists(root+'/project/.claude/settings.json'):raise AssertionError('tiny Setup warning committed')
    resize(12,80);until(lambda:header(b'Review scoped change'));key(b'\x1b[B'*120);until(lambda:b'Read full warning before Yes' not in frame() and header(b'Review scoped change'));key(b'y\r')
   until(lambda:header(b'Setup \xc2\xb7 Result') and b'auth.login' in frame());key(b'\x1b');dashboard();setupmenu();key(b'readiness\r');until(lambda:header(b'Setup \xc2\xb7 Readiness'));key(b'\x1b');dashboard();key(b'q')
 until(closed)
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll(.01)
 if p.poll() is None:raise AssertionError('Setup retained owned process/work')
 while select.select([master],[],[],.01)[0]:
  old=len(transcript);poll(.01)
  if len(transcript)==old:break
 after=termios.tcgetattr(slave);afterflags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':afterflags&=~0x10000;flags&=~0x10000;after[3]&=~termios.PENDIN;before[3]&=~termios.PENDIN
 if before!=after or flags!=afterflags:raise AssertionError('Setup failed TTY/descriptor restoration')
 for seq in (b'\x1b[?1049h',b'\x1b[?2004h',b'\x1b[?2004l',b'\x1b[0m',b'\x1b[?25h',b'\x1b[?1049l'):
  if seq not in transcript:raise AssertionError('Setup missing terminal ownership '+repr(seq))
 if p.returncode or b'FAIL' in transcript:raise AssertionError('Setup child failed '+repr(transcript[-1200:]))
 if b'synthetic-ui-auth-secret' in transcript or b'RAW-SETUP-' in transcript or b'"schema_version"' in transcript:raise AssertionError('Setup leaked secret/raw transport/trailing finite envelope')
 post=transcript[transcript.rfind(b'\x1b[?1049l'):].lower()
 if mode!='menu-back':
  if b'setup' not in post or b'bindings.link' not in post:raise AssertionError('exact partial shared Setup steps not reported after Close')
  if mode.startswith('unknown-'):
   if b'credential_write_unknown' not in post or b'credential unknown' not in post or b'config unknown' not in post or b'auth.login' in post:raise AssertionError('Setup uncertainty/effects demoted or completed login fabricated')
  elif b'auth.login' not in post or b'complete' not in post:raise AssertionError('known applied auth step lost after cancellation/readiness/Close')
  if not any(r['kind']=='native-reaped' for r in reports) or not any(r['kind']=='native-joined' for r in reports):raise AssertionError('native Setup action not reaped/joined')
 want='8' if mode.startswith('unknown-') else ('143' if mode=='known-cancel' else '0')
 if [r['message'] for r in reports if r['kind']=='outcome'][-1]!=want:raise AssertionError('Setup exit classification changed')
 print('actual guided shared Setup verified: '+mode)
finally:
 if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait()
 os.close(report_r);os.close(master);os.close(slave)
`
