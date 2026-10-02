package ui_test

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
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
)

type qaLinkPTYProvider struct {
	harvest.Provider
	t *testing.T
}

func (p qaLinkPTYProvider) Accounts(context.Context) ([]harvest.Object, error) {
	return []harvest.Object{{"id": "1", "product": "harvest"}}, nil
}
func (p qaLinkPTYProvider) Get(context.Context, string) (harvest.Object, error) {
	return harvest.Object{"id": "2", "is_active": true}, nil
}
func (p qaLinkPTYProvider) List(context.Context, string, url.Values) ([]harvest.Object, error) {
	return []harvest.Object{{"is_active": true, "project": harvest.Object{"id": "100", "name": "Synthetic Project"}, "task_assignments": []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": "7", "name": "Synthetic Work"}}}}}, nil
}
func (p qaLinkPTYProvider) Create(context.Context, string, harvest.Object) (harvest.Object, error) {
	p.t.Error("local link action invoked remote Create")
	return nil, errors.New("forbidden remote write")
}
func (p qaLinkPTYProvider) Update(context.Context, string, harvest.Object) (harvest.Object, error) {
	p.t.Error("local link action invoked remote Update")
	return nil, errors.New("forbidden remote write")
}
func (p qaLinkPTYProvider) Delete(context.Context, string) error {
	p.t.Error("local link action invoked remote Delete")
	return errors.New("forbidden remote write")
}

type qaLinkActionPTYScreen struct {
	*qaRunnerPTYScreen
	active *atomic.Int32
}

func (s *qaLinkActionPTYScreen) Close() error {
	if s.active.Load() != 0 {
		s.report("error", "terminal closed before admitted action joined")
	}
	err := s.Session.Close()
	s.report("terminal-closed", "")
	return err
}

// Real Session and real Activity mutations, with a synthetic missing final
// acknowledgment after committed Unlink for the unknown-outcome modes.
func TestQAUILinksActionPTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_LINK_ACTION_MODE")
	if mode == "" {
		return
	}
	fd, err := strconv.Atoi(os.Getenv("TEMPO_QA_LINK_ACTION_REPORT_FD"))
	if err != nil {
		t.Fatal(err)
	}
	reportFile := os.NewFile(uintptr(fd), "link-action-report")
	report := func(kind, message string) {
		_ = json.NewEncoder(reportFile).Encode(map[string]string{"kind": kind, "message": message})
	}
	root := os.Getenv("TEMPO_QA_LINK_ACTION_ROOT")
	if !filepath.IsAbs(root) {
		t.Fatal("isolated absolute fixture required")
	}
	project := filepath.Join(root, "project")
	if err := os.Mkdir(project, 0700); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(root, "activity", "state.json")
	epoch, elapsed := "synthetic-link-epoch", "0"
	service := activity.New(activity.Options{Path: state, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC), Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &elapsed}, nil
	}), HookPolicies: hookstate.New(hookstate.Options{Path: filepath.Join(root, "hooks.json")})})
	api := qaLinkPTYProvider{t: t}
	a := auth.NewService(auth.Options{ConfigPath: filepath.Join(root, "missing-config"), LockPath: filepath.Join(root, "forbidden-auth-lock"), Getenv: func(key string) string {
		switch key {
		case "HARVEST_TOKEN":
			return "synthetic-only-token"
		case "HARVEST_ACCOUNT_ID":
			return "1"
		}
		return ""
	}, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Error("link action accessed native credentials")
		return auth.NativeReply{}, errors.New("forbidden native access")
	}), NewProvider: func(_ string, account string) harvest.Provider {
		if account != "1" {
			t.Error("cross-account link provider")
		}
		return api
	}})
	shared := setup.New(setup.Options{Auth: a, Activity: service})
	var binding activity.Binding
	if mode != "create" && mode != "cancel-create" {
		seed, err := shared.Link(context.Background(), activity.LinkInput{Path: project, AccountID: "1", ProjectID: "100", TaskID: "7", Timezone: "UTC", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, nil)
		if err != nil {
			var local *activity.Error
			if errors.As(err, &local) {
				t.Fatalf("synthetic shared seed failed: code=%s", local.Code)
			}
			t.Fatal("synthetic shared seed failed before UI")
		}
		binding = seed.Binding
		report("binding", binding.ID)
	}
	if mode == "repair" {
		if err := os.Rename(project, filepath.Join(root, "moved")); err != nil {
			t.Fatal(err)
		}
	}
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
	var active, writes atomic.Int32
	var submitted string
	actions := &ui.LinkActions{Prepare: shared.PrepareLink, Commit: func(ctx context.Context, in activity.LinkInput) (activity.BindingResult, error) {
		writes.Add(1)
		return shared.CommitLink(ctx, in)
	}, Repair: func(ctx context.Context, in activity.RepairBindingInput) (activity.BindingResult, error) {
		writes.Add(1)
		return service.RepairBinding(ctx, in)
	}, Unlink: func(ctx context.Context, in activity.UnlinkInput) (activity.MutationResult, error) {
		writes.Add(1)
		if !strings.HasPrefix(mode, "unknown-") {
			return service.Unlink(ctx, in)
		}
		active.Add(1)
		defer func() { active.Add(-1); report("action-joined", in.RequestID) }()
		result, err := service.Unlink(ctx, in)
		if err != nil {
			return result, err
		}
		submitted = in.RequestID
		report("dispatched", in.RequestID)
		<-ctx.Done()
		return activity.MutationResult{}, &activity.Error{Code: "local_write_unknown", Message: "local write durability is unknown; preserve request identity", Uncertain: true, Details: map[string]any{"request_id": in.RequestID}}
	}}
	session, err := terminal.Open(ctx, os.Stdin, os.Stdout)
	if err != nil {
		t.Fatal(err)
	}
	screen := &qaLinkActionPTYScreen{qaRunnerPTYScreen: &qaRunnerPTYScreen{Session: session, report: report}, active: &active}
	err = ui.Run(session.Context(), screen, service, ui.Options{Refresh: make(chan time.Time), Views: &ui.ReadViews{Links: service.ListBindings}, Links: actions})
	cancel(nil)
	<-signalJoined
	list, listErr := service.ListBindings(context.Background())
	if listErr != nil {
		t.Fatal(listErr)
	}
	if strings.HasPrefix(mode, "unknown-") {
		var unknown *activity.Error
		if !errors.As(err, &unknown) || unknown.Code != "local_write_unknown" || !unknown.Uncertain || unknown.Details["request_id"] != submitted || submitted == "" {
			t.Errorf("terminal cancellation demoted submitted outcome: %v", err)
			report("error", "lost submitted uncertainty/request identity")
		}
		if writes.Load() != 1 || len(list.Bindings) != 0 || active.Load() != 0 {
			t.Error("unknown shutdown duplicated or failed to join shared unlink")
		}
		report("outcome", "8:"+submitted)
	} else {
		if err != nil {
			t.Errorf("link action terminal exit: %v", err)
		}
		if mode == "cancel-create" {
			if writes.Load() != 0 || len(list.Bindings) != 0 {
				t.Error("canceled path mutated activity")
			}
			if _, err := os.Stat(filepath.Dir(state)); !errors.Is(err, os.ErrNotExist) {
				t.Error("canceled create initialized state")
			}
		} else if writes.Load() != 1 {
			t.Errorf("explicit action writes=%d, want1", writes.Load())
		}
		switch mode {
		case "create":
			if len(list.Bindings) != 1 || list.Bindings[0].Attribution.ProjectID != "100" || list.Bindings[0].Attribution.TaskID != "7" || list.Bindings[0].Attribution.Timezone != "UTC" {
				t.Error("shared create lost guided attribution")
			}
		case "unlink":
			if len(list.Bindings) != 0 {
				t.Error("shared unlink did not remove active mapping")
			}
		case "repair":
			if len(list.Bindings) != 1 || list.Bindings[0].ID != binding.ID || list.Bindings[0].Revision != "2" || filepath.Base(list.Bindings[0].Locator) != "moved" {
				t.Error("shared repair changed identity or omitted moved scope")
			}
		}
		report("outcome", "0")
	}
	for _, path := range []string{"missing-config", "forbidden-auth-lock", "hooks.json"} {
		if _, err := os.Stat(filepath.Join(root, path)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("link action wrote unrelated fixture %s", path)
		}
	}
	report("closed", "")
}

func TestQAUILinksActualTerminalSharedActionsAndUnknownShutdown(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"create", "unlink", "repair", "cancel-create", "unknown-ctrlc", "unknown-sigterm", "unknown-eof", "unknown-quit"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", qaLinksActionPTYScript, os.Args[0], mode, t.TempDir(), filepath.Dir(git))
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("actual link action PTY %s: %v\n%s", mode, err, output)
			}
		})
	}
}

const qaLinksActionPTYScript = `
import os,sys,pty,termios,subprocess,select,time,signal,fcntl,struct,json
binary,mode,root,gitdir=sys.argv[1:]
master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0))
before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL)
report_r,report_w=os.pipe()
env={'PATH':gitdir+':/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_LINK_ACTION_MODE':mode,'TEMPO_QA_LINK_ACTION_ROOT':root,'TEMPO_QA_LINK_ACTION_REPORT_FD':str(report_w)}
p=subprocess.Popen([binary,'-test.run=^TestQAUILinksActionPTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(report_w,),preexec_fn=os.setpgrp)
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
 until(lambda:header('TEMPO'));key(b'l');until(lambda:header('Links actions'))
 if mode in ('create','cancel-create'):
  key(b'create\r');until(lambda:header('Absolute project path'))
  if mode=='cancel-create':
   key(b'q');deadline=time.monotonic()+.06
   while time.monotonic()<deadline:poll(.01)
   if closed():raise AssertionError('q inside path draft quit navigation')
   key(b'\x1b');until(lambda:kind('frame')[-1]['message'].split('\n')[0].startswith('TEMPO'));key(b'q')
  else:
   key(b'\x1b[200~'+os.path.join(root,'project').encode()+b'\x1b[201~')
   deadline=time.monotonic()+.05
   while time.monotonic()<deadline:poll(.01)
   if header('Choose a Harvest project'):raise AssertionError('paste submitted path')
   key(b'\r');until(lambda:header('Choose a Harvest project'));key(b'100\r')
   until(lambda:header('IANA timezone'));key(b'UTC\r');until(lambda:body('Link directory'))
   key(b'y\r');until(lambda:header('Links · Complete'));dashboard_after_back();key(b'q')
 else:
  until(lambda:kind('binding'));key(kind('binding')[0]['message'].encode()+b'\r');until(lambda:header('Links · Binding actions'))
  key(b'repair\r' if mode=='repair' else b'unlink\r')
  if mode=='repair':until(lambda:header('Replacement absolute path'));key(os.path.join(root,'moved').encode()+b'\r')
  until(lambda:header('Review scoped change'));until(lambda:body('histor'))
  key(b'y\r')
  if mode.startswith('unknown-'):
   until(lambda:kind('dispatched'))
   if mode=='unknown-ctrlc':key(b'\x03')
   elif mode=='unknown-sigterm':p.send_signal(signal.SIGTERM)
   elif mode=='unknown-eof':key(b'\x04')
   else:
    key(b'\x1b');until(lambda:kind('action-joined'));until(lambda:kind('frame')[-1]['message'].split('\n')[0].startswith('TEMPO'));key(b'q')
  else:until(lambda:header('Links · Complete'));dashboard_after_back();key(b'q')
 until(closed,2)
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll(.01)
 if p.poll() is None:raise AssertionError('action retained owned work after cleanup')
 while select.select([master],[],[],.01)[0]:
  old=len(transcript);poll(.01)
  if len(transcript)==old:break
 after=termios.tcgetattr(slave);after_flags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':after_flags&=~0x10000;flags&=~0x10000;after[3]&=~termios.PENDIN;before[3]&=~termios.PENDIN
 if after!=before or after_flags!=flags:raise AssertionError('action failed terminal restoration')
 for seq in (b'\x1b[?1049h',b'\x1b[?25l',b'\x1b[?2004h',b'\x1b[0m',b'\x1b[?25h',b'\x1b[?1049l',b'\x1b[?2004l'):
  if seq not in transcript:raise AssertionError('missing terminal lifetime sequence '+repr(seq))
 outcome=kind('outcome')[-1]['message']
 if mode.startswith('unknown-'):
  request=kind('dispatched')[0]['message']
  if outcome!='8:'+request:raise AssertionError('strong unknown lost request identity')
  names=[r['kind'] for r in reports]
  if names.index('action-joined')>names.index('terminal-closed'):raise AssertionError('terminal closed before joined result')
 elif outcome!='0':raise AssertionError('ordinary action failed')
 if kind('error') or p.returncode or b'FAIL' in transcript:raise AssertionError('action errors '+repr(kind('error'))+' exit '+str(p.returncode)+' transcript '+repr(transcript[-1000:]))
 print('actual shared link action and terminal lifetime verified: '+mode)
finally:
 if p.poll() is None:p.kill();p.wait()
 os.close(report_r);os.close(master);os.close(slave)
`
