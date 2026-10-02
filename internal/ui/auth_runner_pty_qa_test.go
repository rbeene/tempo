package ui_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
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
)

const qaAuthPTYSecret = "synthetic-ui-auth-secret"

func TestQAUIAuthPrivateNativeChild(t *testing.T) {
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

type qaAuthPTYProvider struct{ harvest.Provider }

func (qaAuthPTYProvider) Accounts(context.Context) ([]harvest.Object, error) {
	return []harvest.Object{{"id": "11", "product": "harvest", "name": "Synthetic account"}}, nil
}

type qaAuthPTYScreen struct {
	*qaRunnerPTYScreen
	active      *atomic.Int32
	closed      *atomic.Bool
	drawFailure *atomic.Bool
}

func (s *qaAuthPTYScreen) Draw(ctx context.Context, lines []string) error {
	if s.drawFailure.Load() {
		return errors.New("RAW-AUTH-OUTPUT-SECRET")
	}
	return s.qaRunnerPTYScreen.Draw(ctx, lines)
}
func (s *qaAuthPTYScreen) Close() error {
	if s.active.Load() != 0 {
		s.report("error", "auth native work active during terminal Close")
	}
	err := s.Session.Close()
	s.closed.Store(true)
	s.report("terminal-closed", "")
	return err
}
func TestQAUIAuthRunnerPTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_AUTH_MODE")
	if mode == "" {
		return
	}
	root := os.Getenv("TEMPO_QA_AUTH_ROOT")
	fd, e := strconv.Atoi(os.Getenv("TEMPO_QA_AUTH_REPORT_FD"))
	if e != nil || !filepath.IsAbs(root) {
		t.Fatal("invalid synthetic fixture")
	}
	f := os.NewFile(uintptr(fd), "auth-reports")
	report := func(k, m string) { json.NewEncoder(f).Encode(map[string]string{"kind": k, "message": m}) }
	config := filepath.Join(root, "config")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
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

	var active atomic.Int32
	var closed, drawFailure atomic.Bool
	native := auth.ProcessRunner{Executable: os.Args[0], Args: []string{"-test.run=^TestQAUIAuthPrivateNativeChild$", "--", "qa-auth-native=" + mode}, Timeout: 5 * time.Second}
	service := auth.NewService(auth.Options{ConfigPath: config, LockPath: filepath.Join(root, "credential-lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, NewProvider: func(string, string) harvest.Provider { return qaAuthPTYProvider{} }, Runner: auth.RunnerFunc(func(ctx context.Context, r auth.NativeRequest, lock *os.File) (auth.NativeReply, error) {
		active.Add(1)
		defer func() { active.Add(-1); report("action-joined", "") }()
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
			if ke := syscall.Kill(pid, 0); !errors.Is(ke, syscall.ESRCH) {
				t.Error("native helper not reaped")
			}
			report("reaped", "")
		}
		if mode == "known-cancel" && e == nil {
			cancel(&terminal.ExitError{Code: 143})
		}
		return reply, e
	})})
	actions := &ui.AuthActions{CanPersist: service.CanPersist, Status: service.Status, Accounts: service.Accounts, PrepareLogin: service.PrepareLogin, CommitLogin: service.CommitLogin, Logout: service.Logout, UseAccount: service.UseAccount, ConfigShow: service.ConfigShow}
	session, e := terminal.Open(ctx, os.Stdin, os.Stdout)
	if e != nil {
		t.Fatal(e)
	}
	screen := &qaAuthPTYScreen{qaRunnerPTYScreen: &qaRunnerPTYScreen{Session: session, report: report}, active: &active, closed: &closed, drawFailure: &drawFailure}
	reader := &qaRunnerReader{read: func(context.Context) (activity.ActivitySnapshot, error) { return qaUISnapshot("1", "100"), nil }}
	knownCallbacks, unknownCallbacks := 0, 0
	err := ui.Run(session.Context(), screen, reader, ui.Options{Refresh: make(chan time.Time), Auth: actions, OnAuthResult: func(op string, r auth.Result, err error) {
		knownCallbacks++
		if !closed.Load() || active.Load() != 0 {
			t.Error("known auth report before cleanup")
		}
		if op != "login" || err != nil || r.AccountID != "11" || r.Effects.Credential != "applied" || r.Effects.Config != "saved" {
			t.Errorf("lost known AuthResult: %s %#v %v", op, r, err)
		}
		report("auth-result", op+":"+r.Effects.Credential+":"+r.Effects.Config)
	}, OnRetainedOutcome: func(family, requestID string, err error) {
		unknownCallbacks++
		if requestID != "" {
			t.Error("auth invented request ledger identity")
		}
		if !closed.Load() || active.Load() != 0 {
			t.Error("unknown auth report before cleanup")
		}
		var ae *auth.Error
		if family != "auth" || !errors.As(err, &ae) || !ae.Uncertain || ae.Code != "credential_write_unknown" || ae.Effects.Credential != "unknown" || ae.Effects.Config != "unknown" {
			t.Errorf("lost shared auth effects: %s %v", family, err)
		}
		report("auth-unknown", family+":"+ae.Effects.Credential+":"+ae.Effects.Config)
	}})
	cancel(nil)
	<-signalJoined
	if strings.HasPrefix(mode, "unknown-") {
		var ae *auth.Error
		if !errors.As(err, &ae) || !ae.Uncertain || ae.Code != "credential_write_unknown" || unknownCallbacks != 1 {
			t.Errorf("unknown became clean exit: %v", err)
		}
		report("outcome", "8")
	} else if mode == "known-cancel" {
		var ee *terminal.ExitError
		if !errors.As(err, &ee) || ee.Code != 143 || knownCallbacks != 1 {
			t.Errorf("valid reply after canceled dispatch hidden: %v", err)
		}
		report("outcome", "143")
	} else {
		if err != nil || knownCallbacks != 1 {
			t.Errorf("auth success/report failed: %v", err)
		}
		report("outcome", "0")
	}
	if active.Load() != 0 {
		t.Error("native work not joined")
	}
	report("closed", "")
}
func TestQAUIAuthActualTerminalSecretAndMissingFinalNativeReply(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"success", "known-cancel", "unknown-eof", "unknown-ctrlc", "unknown-sigterm", "unknown-quit"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", qaAuthRunnerPTYScript, os.Args[0], mode, t.TempDir())
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if b, e := cmd.CombinedOutput(); e != nil {
				t.Fatalf("auth actualPTY%s: %v\n%s", mode, e, b)
			}
		})
	}
}

const qaAuthRunnerPTYScript = `
import os,sys,pty,termios,subprocess,select,time,signal,fcntl,struct,json
binary,mode,root=sys.argv[1:]
master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0))
before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL)
report_r,report_w=os.pipe()
env={'PATH':'/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_AUTH_MODE':mode,'TEMPO_QA_AUTH_ROOT':root,'TEMPO_QA_AUTH_REPORT_FD':str(report_w)}
p=subprocess.Popen([binary,'-test.run=^TestQAUIAuthRunnerPTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(report_w,),preexec_fn=os.setpgrp)
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
def header(title):return any(r['kind']=='frame' and title in r['message'].split('\n')[0] for r in reports)
def body(text):return any(r['kind']=='frame' and text.lower() in r['message'].lower() for r in reports)
def kind(name):return [r for r in reports if r['kind']==name]
def until(pred,seconds=2):
 deadline=time.monotonic()+seconds
 while not pred() and time.monotonic()<deadline:
  poll()
  if closed() and not pred():break
 if not pred():raise AssertionError('missing action behavior '+repr(reports))
def key(data):os.write(master,data)
def dashboard_after_back():
 count=len([r for r in reports if r['kind']=='frame' and 'TEMPO' in r['message'].split('\n')[0]])
 key(b'\r')
 until(lambda:len([r for r in reports if r['kind']=='frame' and 'TEMPO' in r['message'].split('\n')[0]])>count)
try:
 until(lambda:header('TEMPO'));key(b'a');until(lambda:header('Accounts and auth'));key(b'login\r')
 until(lambda:header('Harvest personal access token'));key(b'q');deadline=time.monotonic()+.06
 while time.monotonic()<deadline:poll(.01)
 if closed():raise AssertionError('q in Secret quit screen')
 key(b'\x7f');key(b'\x1b[200~synthetic-ui-auth-secret\n\x1b[201~');deadline=time.monotonic()+.06
 while time.monotonic()<deadline:poll(.01)
 if header('Choose a Harvest account'):raise AssertionError('secret paste submitted')
 key(b'\r');until(lambda:header('Choose a Harvest account'));key(b'11\r');until(lambda:body('Replace the saved credential'))
 if kind('dispatched'):raise AssertionError('native credential dispatch before explicit confirmation')
 key(b'y\r')
 if mode.startswith('unknown-'):
  until(lambda:kind('dispatched'))
  if mode=='unknown-eof':key(b'\x04')
  elif mode=='unknown-ctrlc':key(b'\x03')
  elif mode=='unknown-sigterm':p.send_signal(signal.SIGTERM)
  else:
   key(b'\x1b');until(lambda:kind('action-joined'));until(lambda:kind('frame')[-1]['message'].split('\n')[0].startswith('TEMPO'));key(b'q')
 elif mode=='success':until(lambda:header('Auth complete'));dashboard_after_back();key(b'q')
 until(closed,2)
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll(.01)
 if p.poll() is None:raise AssertionError('auth retained child/work after cleanup')
 while select.select([master],[],[],.01)[0]:
  old=len(transcript);poll(.01)
  if len(transcript)==old:break
 after=termios.tcgetattr(slave);after_flags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':after_flags&=~0x10000;flags&=~0x10000;after[3]&=~termios.PENDIN;before[3]&=~termios.PENDIN
 if after!=before or after_flags!=flags:raise AssertionError('auth failed original terminal restoration')
 for seq in (b'\x1b[?1049h',b'\x1b[?25l',b'\x1b[?2004h',b'\x1b[0m',b'\x1b[?25h',b'\x1b[?1049l',b'\x1b[?2004l'):
  if seq not in transcript:raise AssertionError('missing auth terminal cleanup '+repr(seq))
 if b'synthetic-ui-auth-secret' in transcript:raise AssertionError('private token leaked in terminal transcript')
 expected='8' if mode.startswith('unknown-') else ('143' if mode=='known-cancel' else '0')
 if kind('outcome')[-1]['message']!=expected:raise AssertionError('shared auth classification lost')
 names=[r['kind'] for r in reports]
 if names.index('action-joined')>names.index('terminal-closed') or names.index('reaped')>names.index('terminal-closed'):raise AssertionError('terminal closed before native result joined/reaped')
 reported='auth-unknown' if mode.startswith('unknown-') else 'auth-result'
 if names.index(reported)<names.index('terminal-closed'):raise AssertionError('effects reported before terminal cleanup')
 if kind('error') or p.returncode or b'FAIL' in transcript:raise AssertionError('auth errors '+repr(kind('error'))+' exit '+str(p.returncode)+' transcript '+repr(transcript[-1000:]))
 print('actual secret/native IPC and joined auth result verified: '+mode)
finally:
 if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait()
 os.close(report_r);os.close(master);os.close(slave)
`

func TestQAUIAuthNativeFixturePrivateIPCAndReap(t *testing.T) {
	for _, mode := range []string{"success", "unknown-eof"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "cfg")
			runner := auth.ProcessRunner{Executable: os.Args[0], Args: []string{"-test.run=^TestQAUIAuthPrivateNativeChild$", "--", "qa-auth-native=" + mode}, Timeout: 3 * time.Second}
			s := auth.NewService(auth.Options{ConfigPath: path, LockPath: filepath.Join(root, "lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, Runner: runner, NewProvider: func(string, string) harvest.Provider { return qaAuthPTYProvider{} }})
			attempt, e := s.PrepareLogin(context.Background(), []byte(qaAuthPTYSecret))
			if e != nil {
				t.Fatal(e)
			}
			defer attempt.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				result, e := s.CommitLogin(ctx, attempt, "11")
				if mode == "success" && e == nil && (result.Effects.Credential != "applied" || result.Effects.Config != "saved") {
					e = errors.New("known fixture effects changed")
				}
				done <- e
			}()
			deadline := time.Now().Add(2 * time.Second)
			for {
				if _, e := os.Stat(path + ".possible-dispatch"); e == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("synthetic native fixture never received admitted private request")
				}
				time.Sleep(time.Millisecond)
			}
			if mode != "success" {
				cancel()
			}
			select {
			case e := <-done:
				if mode == "success" {
					if e != nil {
						t.Fatalf("synthetic native success: %v", e)
					}
				} else {
					var ae *auth.Error
					if !errors.As(e, &ae) || ae.Code != "credential_write_unknown" || !ae.Uncertain || ae.Effects != (auth.Effects{Credential: "unknown", Config: "unknown"}) {
						t.Errorf("synthetic native admitted unknown lost: %#v %v", ae, e)
					}
				}
			case <-time.After(2 * time.Second):
				t.Fatal("native fixture did not join")
			}
			b, e := os.ReadFile(path + ".pid")
			if e != nil {
				t.Fatal(e)
			}
			pid, _ := strconv.Atoi(string(b))
			if e := syscall.Kill(pid, 0); !errors.Is(e, syscall.ESRCH) {
				t.Error("synthetic native child still alive")
			}
		})
	}
}
