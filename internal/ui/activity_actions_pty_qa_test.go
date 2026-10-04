package ui_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestQAUIActivityActionPTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_ACTIVITY_MODE")
	if mode == "" {
		return
	}
	fd, e := strconv.Atoi(os.Getenv("TEMPO_QA_ACTIVITY_REPORT_FD"))
	if e != nil {
		t.Fatal(e)
	}
	f := os.NewFile(uintptr(fd), "activity-report")
	report := func(kind, msg string) { json.NewEncoder(f).Encode(map[string]string{"kind": kind, "message": msg}) }
	startupStage := "owned-root"
	stage := func(name string) { startupStage = name; report("startup-stage", name) }
	startupError := func(err error) {
		code := "none"
		if err != nil {
			code = "non_domain"
			var domain *activity.Error
			if errors.As(err, &domain) && domain != nil {
				code = "other_domain"
				switch domain.Code {
				case "state_busy", "binding_unavailable", "state_corrupt", "validation", "clock_unavailable", "local_write_unknown":
					code = domain.Code
				}
			}
		}
		report("startup-result", startupStage+":"+code)
	}
	stage("owned-root")
	root := os.Getenv("TEMPO_QA_ACTIVITY_ROOT")
	if !filepath.IsAbs(root) {
		startupError(nil)
		t.Fatal("synthetic root required")
	}
	project := filepath.Join(root, "project")
	stage("scope-create")
	if e = os.Mkdir(project, 0700); e != nil {
		startupError(e)
		t.Fatal(e)
	}
	epoch, elapsed := "qa-activity-epoch", "0"
	start := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	wall := start
	service := activity.New(activity.Options{Path: filepath.Join(root, "state", "activity.json"), HookPolicies: hookstate.New(hookstate.Options{Path: filepath.Join(root, "hooks.json")}), Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: wall, Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &elapsed}, nil
	})})
	api := qaLinkPTYProvider{t: t}
	a := auth.NewService(auth.Options{ConfigPath: filepath.Join(root, "config"), Getenv: func(k string) string {
		if k == "HARVEST_TOKEN" {
			return "synthetic-token"
		}
		if k == "HARVEST_ACCOUNT_ID" {
			return "1"
		}
		return ""
	}, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Error("activity touched credentials")
		return auth.NativeReply{}, errors.New("forbidden")
	}), NewProvider: func(string, string) harvest.Provider { return api }})
	shared := setup.New(setup.Options{Auth: a, Activity: service})
	stage("seed-link")
	binding, e := shared.Link(context.Background(), activity.LinkInput{Path: project, AccountID: "1", ProjectID: "100", TaskID: "7", Timezone: "UTC", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, nil)
	if e != nil {
		var ae *activity.Error
		errors.As(e, &ae)
		startupError(e)
		t.Fatalf("actual Activity seed link failed: %#v", ae)
	}
	stage("seed-identity")
	snapshot, e := service.Status(context.Background())
	if e != nil || snapshot.ComputerID == nil {
		startupError(e)
		t.Fatal("seed no computer identity")
	}
	event := activity.Event{ContractVersion: 1, Actor: activity.ActorKey{ComputerID: *snapshot.ComputerID, Source: "manual-test", SessionID: "synthetic-session", AgentID: "A"}, Generation: "1", Sequence: "1", EventID: "A/1/1", Kind: "work", BindingID: binding.Binding.ID, BindingRevision: binding.Binding.Revision}
	stage("seed-work")
	if _, e = service.Ingest(context.Background(), event); e != nil {
		startupError(e)
		t.Fatalf("actual work seed: %v", e)
	}
	wall = start.Add(time.Minute)
	elapsed = strconv.FormatInt(int64(time.Minute), 10)
	event.Sequence = "2"
	event.EventID = "A/1/2"
	event.Kind = "observe_work"
	event.BindingID = ""
	event.BindingRevision = ""
	stage("seed-observe")
	if _, e = service.Ingest(context.Background(), event); e != nil {
		startupError(e)
		t.Fatalf("actual observation seed: %v", e)
	}
	wall = start.Add(3 * time.Minute)
	elapsed = strconv.FormatInt(int64(3*time.Minute), 10)
	stage("seed-actor")
	snapshot, e = service.Status(context.Background())
	if e != nil || len(snapshot.Actors) != 1 {
		startupError(e)
		t.Fatal("seed lacks actor")
	}
	actor := snapshot.Actors[0]
	report("actor", actor.ID)
	if mode == "resolve" {
		stage("seed-interrupt")
		_, e = service.Interrupt(context.Background(), activity.InterruptInput{ActorID: actor.ID, Generation: actor.Ref.Generation, IfRevision: actor.Revision, RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", Confirmed: true})
		if e != nil {
			startupError(e)
			t.Fatalf("actual uncertainty seed: %v", e)
		}
		stage("seed-review")
		list, e := service.Review(context.Background(), activity.ReviewInput{})
		if e != nil || len(list.Uncertainties) != 1 {
			startupError(e)
			t.Fatal("uncertainty fixture invalid")
		}
		report("uncertainty", list.Uncertainties[0].ID)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	var active, writes atomic.Int32
	submitted := ""
	actions := &ui.ActivityActions{Status: service.Status, Review: service.Review, Preview: service.Preview, Resolve: func(ctx context.Context, in activity.ResolveInput) (activity.MutationResult, error) {
		writes.Add(1)
		return service.Resolve(ctx, in)
	}, Interrupt: func(ctx context.Context, in activity.InterruptInput) (activity.MutationResult, error) {
		writes.Add(1)
		if mode != "unknown-eof" {
			return service.Interrupt(ctx, in)
		}
		active.Add(1)
		defer func() { active.Add(-1); report("action-joined", in.RequestID) }()
		r, e := service.Interrupt(ctx, in)
		if e != nil {
			return r, e
		}
		submitted = in.RequestID
		report("dispatched", in.RequestID)
		<-ctx.Done()
		return activity.MutationResult{}, &activity.Error{Code: "local_write_unknown", Uncertain: true, Details: map[string]any{"request_id": in.RequestID}}
	}}
	stage("terminal-open")
	session, e := terminal.Open(ctx, os.Stdin, os.Stdout)
	if e != nil {
		startupError(e)
		t.Fatal(e)
	}
	screen := &qaLinkActionPTYScreen{qaRunnerPTYScreen: &qaRunnerPTYScreen{Session: session, report: report}, active: &active}
	stage("ui-run")
	e = ui.Run(session.Context(), screen, service, ui.Options{Refresh: make(chan time.Time), Activity: actions})
	if mode == "unknown-eof" {
		var u *activity.Error
		if !errors.As(e, &u) || !u.Uncertain || u.Details["request_id"] != submitted || submitted == "" {
			t.Errorf("lost joined unknown: %v", e)
		}
		report("outcome", "8:"+submitted)
	} else {
		if e != nil {
			t.Errorf("action error: %v", e)
		}
		report("outcome", "0")
	}
	if active.Load() != 0 || writes.Load() != 1 {
		t.Error("duplicated or unjoined activity mutation")
	}
	review, e := service.Review(context.Background(), activity.ReviewInput{})
	if e != nil {
		t.Fatal(e)
	}
	if mode == "resolve" {
		history, err := service.Status(context.Background())
		if err != nil || len(review.Uncertainties) != 0 || len(history.Uncertainties) != 1 || history.Uncertainties[0].State != "resolved" || history.Uncertainties[0].ResolutionEnd == nil || !history.Uncertainties[0].ResolutionEnd.Equal(start.Add(2*time.Minute)) {
			t.Errorf("real recovery mismatch: review=%#v history=%#v err=%v", review, history.Uncertainties, err)
		}
	} else if len(review.Uncertainties) != 1 {
		t.Errorf("real interrupt did not quarantine tail: %#v", review)
	}
	for _, p := range []string{"config", "hooks.json"} {
		if _, e = os.Stat(filepath.Join(root, p)); !errors.Is(e, os.ErrNotExist) {
			t.Errorf("activity touched unrelated %s", p)
		}
	}
	report("closed", "")
}
func TestQAUIActivityActualTerminalSharedInterruptRecoveryAndUnknown(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal(e)
	}
	git, e := exec.LookPath("git")
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"interrupt", "resolve", "unknown-eof"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", qaActivityActionPTYScript, os.Args[0], mode, t.TempDir(), filepath.Dir(git))
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if b, e := cmd.CombinedOutput(); e != nil {
				t.Fatalf("actual activity %s: %v\n%s", mode, e, b)
			}
		})
	}
}

const qaActivityActionPTYScript = `
import os,sys,pty,termios,subprocess,select,time,signal,fcntl,struct,json
binary,mode,root,gitdir=sys.argv[1:]
master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0))
before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL)
report_r,report_w=os.pipe()
env={'PATH':gitdir+':/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_ACTIVITY_MODE':mode,'TEMPO_QA_ACTIVITY_ROOT':root,'TEMPO_QA_ACTIVITY_REPORT_FD':str(report_w)}
p=subprocess.Popen([binary,'-test.run=^TestQAUIActivityActionPTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(report_w,),preexec_fn=os.setpgrp)
os.close(report_w);transcript=b'';pending=b'';reports=[];diagnostic_failure=False
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
def startup_diagnostic():
 stages=('owned-root','scope-create','seed-link','seed-identity','seed-work','seed-observe','seed-actor','seed-interrupt','seed-review','terminal-open','ui-run')
 codes=('none','non_domain','other_domain','state_busy','binding_unavailable','state_corrupt','validation','clock_unavailable','local_write_unknown')
 observed=[r['message'] for r in reports if r.get('kind')=='startup-stage' and r.get('message') in stages]
 results=[r['message'].split(':') for r in reports if r.get('kind')=='startup-result' and isinstance(r.get('message'),str)]
 results=[v for v in results if len(v)==2 and v[0] in stages and v[1] in codes]
 tail=transcript[-4096:]
 markers=((b'--- FAIL:','test_fail'),(b'panic:','panic'),(b'actual Activity seed link failed:','seed_link_failure'),(b'seed no computer identity','seed_identity_failure'),(b'actual work seed:','seed_work_failure'),(b'actual observation seed:','seed_observe_failure'),(b'seed lacks actor','seed_actor_failure'),(b'actual uncertainty seed:','seed_interrupt_failure'),(b'uncertainty fixture invalid','seed_review_failure'))
 return json.dumps({'stage':observed[-1] if observed else 'unobserved','result':results[-1] if results else [],'child_exit':p.poll(),'terminal_tail_classes':[label for token,label in markers if token in tail]},sort_keys=True)
def until(pred,seconds=2):
 global diagnostic_failure
 deadline=time.monotonic()+seconds
 while not pred() and time.monotonic()<deadline:
  poll()
  if closed() and not pred():break
 if not pred():
  diagnostic_failure=True
  raise AssertionError('missing action behavior '+startup_diagnostic())
def key(data):os.write(master,data)
def dashboard_after_back():
 count=len([r for r in reports if r['kind']=='frame' and 'TEMPO' in r['message'].split('\n')[0]])
 key(b'\r')
 until(lambda:len([r for r in reports if r['kind']=='frame' and 'TEMPO' in r['message'].split('\n')[0]])>count)
try:
 until(lambda:header('TEMPO'));key(b'x');until(lambda:header('Activity actions'))
 if mode=='resolve':
  key(b'review\r');until(lambda:kind('uncertainty'));until(lambda:body(kind('uncertainty')[0]['message']))
  key(kind('uncertainty')[0]['message'].encode()+b'\r');until(lambda:header('Recovery boundary'));key(b'end\r')
  until(lambda:header('UTC recovery end'));key(b'\x7f'*100+b'2026-10-02T12:02:00Z\r');until(lambda:header('Recovery reason'));key(b'verified synthetic end\r')
  until(lambda:header('Recovery preview'));key(b'confirm\r');until(lambda:header('Review scoped change'));key(b'\x1b[B'*100+b'y\r')
 else:
  key(b'actors\r');until(lambda:header('Actor actions'));until(lambda:kind('actor'));key(kind('actor')[0]['message'].encode()+b'\r')
  until(lambda:body('quarant'));key(b'y\r')
 if mode.startswith('unknown-'):
  until(lambda:kind('dispatched'));key(b'\x04')
 else:until(lambda:header('Activity · Complete'));dashboard_after_back();key(b'q')

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
 print('actual shared activity action and terminal lifetime verified: '+mode)
finally:
 forced_kill=p.poll() is None
 if forced_kill:p.kill();p.wait()
 if diagnostic_failure:print('UI_DIAGNOSTIC_JOIN '+json.dumps({'child_pid':p.pid,'child_exit':p.returncode,'forced_kill':forced_kill},sort_keys=True),file=sys.stderr)
 os.close(report_r);os.close(master);os.close(slave)
`
