//go:build darwin || linux

package ui_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
)

type qaSyncTransport struct {
	t                  *testing.T
	user, posts, reads atomic.Int32
	report             func(string, string)
}

func (p *qaSyncTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	p.reads.Add(1)
	code := 200
	body := ""
	switch r.URL.Path {
	case "/id/accounts":
		body = `{"accounts":[{"id":11,"product":"harvest","name":"Synthetic"}]}`
	case "/v2/users/me":
		body = `{"id":` + strconv.Itoa(int(p.user.Load())) + `,"is_active":true,"timezone":"UTC"}`
	case "/v2/company":
		body = `{"is_active":true,"wants_timestamp_timers":false,"clock":"24h"}`
	case "/v2/users/me/project_assignments":
		body = `{"project_assignments":[{"is_active":true,"project":{"id":100,"name":"Synthetic"},"task_assignments":[{"is_active":true,"task":{"id":200}}]}],"links":{"next":null}}`
	case "/v2/time_entries":
		if r.Method == "POST" {
			p.posts.Add(1)
			p.report("http-post", "synthetic only")
			code = 500
			body = `{"error":"RAW-SYNC-HTTP-CANARY"}`
		} else {
			if r.URL.Query().Get("user_id") != "22" {
				p.t.Error("reconcile escaped frozen current-user scope")
			}
			body = `{"time_entries":[],"links":{"next":null}}`
			p.report("http-get", "complete empty current-user list")
		}
	default:
		p.t.Errorf("unrequested mock route %s %s", r.Method, r.URL.Path)
		body = `{}`
	}
	return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}
func qaSyncTerminalFixture(t *testing.T, root string, report func(string, string)) (*activity.Service, *harvest.Client, *qaSyncTransport) {
	t.Helper()
	project := filepath.Join(root, "project")
	if e := os.Mkdir(project, 0700); e != nil {
		t.Fatal(e)
	}
	transport := &qaSyncTransport{t: t, report: report}
	transport.user.Store(22)
	provider := harvest.NewWithHTTP("synthetic-only-token", "11", "http://synthetic.invalid/v2", "http://synthetic.invalid/id", &http.Client{Transport: transport})
	epoch, elapsed := "synthetic-sync-pty", "0"
	wall := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	local := activity.New(activity.Options{Path: filepath.Join(root, "state", "activity.json"), Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: wall, Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &elapsed}, nil
	})})
	deps := activity.LinkDependencies{NewProvider: func(_ context.Context, account string) (harvest.Provider, error) {
		if account != "11" {
			t.Error("wrong binding account")
		}
		return provider, nil
	}}
	b, e := local.Link(context.Background(), activity.LinkInput{Path: project, AccountID: "11", ProjectID: "100", TaskID: "200", Timezone: "UTC", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, deps)
	if e != nil {
		t.Fatalf("synthetic Link seed: %v", e)
	}
	snap, e := local.Status(context.Background())
	if e != nil || snap.ComputerID == nil {
		t.Fatal("synthetic computer fixture")
	}
	event := activity.Event{ContractVersion: 1, Actor: activity.ActorKey{ComputerID: *snap.ComputerID, Source: "manual-test", SessionID: "sync-pty", AgentID: "synthetic"}, Generation: "1", Sequence: "1", EventID: "sync-pty/1", Kind: "work", BindingID: b.Binding.ID, BindingRevision: b.Binding.Revision}
	if _, e = local.Ingest(context.Background(), event); e != nil {
		t.Fatal(e)
	}
	wall = wall.Add(30 * time.Second)
	elapsed = "30000000000"
	event.Sequence = "2"
	event.EventID = "sync-pty/2"
	event.Kind = "finish"
	event.BindingID = ""
	event.BindingRevision = ""
	if _, e = local.Ingest(context.Background(), event); e != nil {
		t.Fatal(e)
	}
	sd := activity.SyncDependencies{NewProvider: deps.NewProvider}
	if _, e = local.SyncConfigure(context.Background(), activity.SyncConfigureInput{AccountID: "11", UserID: "22", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", Confirmed: true}, sd); e != nil {
		t.Fatal(e)
	}
	if _, e = local.SyncResume(context.Background(), "cccccccc-cccc-4ccc-8ccc-cccccccccccc"); e != nil {
		t.Fatal(e)
	}
	transport.reads.Store(0)
	return local, provider, transport
}

type qaSyncTerminalScreen struct {
	*qaRunnerPTYScreen
	active    *atomic.Int32
	closed    *atomic.Bool
	failClose bool
}

func (s *qaSyncTerminalScreen) Close() error {
	if s.active.Load() != 0 {
		s.report("error", "closed before Sync work joined")
	}
	e := s.Session.Close()
	s.closed.Store(true)
	s.report("terminal-closed", "")
	if s.failClose {
		return errors.New("RAW-SYNC-CLOSE-CANARY")
	}
	return e
}
func TestQAUISyncActualTerminalChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_SYNC_MODE")
	if mode == "" {
		return
	}
	root := os.Getenv("TEMPO_QA_SYNC_ROOT")
	fd, e := strconv.Atoi(os.Getenv("TEMPO_QA_SYNC_FD"))
	if e != nil || !filepath.IsAbs(root) {
		t.Fatal("invalid private fixture")
	}
	f := os.NewFile(uintptr(fd), "sync-report")
	defer f.Close()
	var mu sync.Mutex
	report := func(k, v string) {
		mu.Lock()
		defer mu.Unlock()
		json.NewEncoder(f).Encode(map[string]string{"kind": k, "message": v})
	}
	local, p, transport := qaSyncTerminalFixture(t, root, report)
	sd := activity.SyncDependencies{NewProvider: func(_ context.Context, account string) (harvest.Provider, error) {
		if account != "11" {
			t.Error("wrong sync account")
		}
		return p, nil
	}}
	report("fixture-valid", "")
	ctx, cancel := context.WithCancelCause(context.Background())
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
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
	var active, pauses, runs, reconciles, configures atomic.Int32
	var pauseID, postID, getID string
	var inputs []activity.SyncReconcileInput
	remote := &harvest.Error{Code: "uncertain_write", Uncertain: true, Message: "RAW-SYNC-HTTP-CANARY"}
	localUnknown := &activity.Error{Code: "local_write_unknown", Uncertain: true, Message: "RAW-SYNC-HTTP-CANARY"}
	actions := &ui.SyncActions{Accounts: p.Accounts, Identity: func(ctx context.Context, id string) (activity.SyncAccountIdentity, error) {
		return local.SyncIdentity(ctx, id, sd)
	}, Status: local.SyncStatus, Configure: func(ctx context.Context, in activity.SyncConfigureInput) (activity.SyncConfigurationResult, error) {
		configures.Add(1)
		if in.AccountID != "11" || in.UserID != "22" || in.IfRevision != "1" || !in.Confirmed {
			t.Errorf("shared Configure lost observed identity: %+v", in)
		}
		if mode == "configure-conflict" {
			transport.user.Store(33)
		}
		r, e := local.SyncConfigure(ctx, in, sd)
		report("configured", in.RequestID)
		return r, e
	}, Pause: func(ctx context.Context, id string) (activity.MutationResult, error) {
		active.Add(1)
		defer func() { active.Add(-1); report("action-joined", id) }()
		n := pauses.Add(1)
		pauseID = id
		r, e := local.SyncPause(ctx, id)
		if e != nil {
			return r, e
		}
		report("paused", id)
		if n == 1 && (mode == "local-replay" || strings.HasPrefix(mode, "unknown-")) {
			localUnknown.Details = map[string]any{"request_id": id}
			if strings.HasPrefix(mode, "unknown-") {
				<-ctx.Done()
			}
			return activity.MutationResult{}, localUnknown
		}
		return r, nil
	}, Resume: local.SyncResume, Resolve: func(ctx context.Context, in activity.SyncResolveInput) (activity.MutationResult, error) {
		return local.SyncResolve(ctx, in, sd)
	}, Now: func(ctx context.Context, in activity.SyncRunInput) (activity.SyncRun, error) {
		runs.Add(1)
		postID = in.RequestID
		r, e := local.SyncNow(ctx, in, sd)
		report("run", in.RequestID)
		if e == nil && strings.HasPrefix(mode, "nested-") {
			return activity.SyncRun{}, remote
		}
		return r, e
	}, Reconcile: func(ctx context.Context, in activity.SyncReconcileInput) (activity.SyncRun, error) {
		reconciles.Add(1)
		inputs = append(inputs, in)
		getID = in.RequestID
		r, e := local.SyncReconcile(ctx, in, sd)
		report("reconciled", in.RequestID)
		if e == nil && strings.HasPrefix(mode, "nested-") {
			return activity.SyncRun{}, localUnknown
		}
		return r, e
	}}
	session, e := terminal.Open(ctx, os.Stdin, os.Stdout)
	if e != nil {
		t.Fatal(e)
	}
	var closed atomic.Bool
	screen := &qaSyncTerminalScreen{qaRunnerPTYScreen: &qaRunnerPTYScreen{Session: session, report: report}, active: &active, closed: &closed, failClose: strings.HasSuffix(mode, "close")}
	var retained []string
	outcome := ui.Run(session.Context(), screen, local, ui.Options{Refresh: make(chan time.Time), Sync: actions, OnRetainedOutcome: func(family, id string, e error) {
		if !closed.Load() || active.Load() != 0 || family != "sync" {
			t.Error("outcome before joined Close")
		}
		if strings.HasPrefix(mode, "nested-") {
			if len(retained) == 0 && (id != postID || e != remote) {
				t.Error("lost original POST identity")
			}
			if len(retained) == 1 && (id != getID || e != localUnknown) {
				t.Error("lost nested GET receipt identity")
			}
		} else if id != pauseID || e != localUnknown {
			t.Error("lost exact local typed outcome")
		}
		retained = append(retained, id)
		report("retained", id)
	}})
	cancel(nil)
	signal.Stop(signals)
	<-joined
	switch {
	case strings.HasPrefix(mode, "unknown-"):
		if outcome != localUnknown || len(retained) != 1 {
			t.Error("unknown lost to terminal exit")
		}
	case strings.HasPrefix(mode, "nested-"):
		if outcome != remote || len(retained) != 2 || postID == getID {
			t.Error("nested ambiguity erased")
		}
	default:
		if outcome != nil {
			t.Errorf("unexpected UI exit: %v", outcome)
		}
	}
	status, e := local.SyncStatus(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if mode == "local-replay" && (pauses.Load() != 2 || status.Enabled) {
		t.Error("exact real receipt replay did not resolve")
	}
	if strings.HasPrefix(mode, "now-") || strings.HasPrefix(mode, "nested-") {
		if transport.posts.Load() != 1 || len(status.Items) != 1 || status.Items[0].State != "unknown" {
			t.Errorf("shared remote unknown reposted/lost: posts%d", transport.posts.Load())
		}
	}
	if mode == "configure" && (configures.Load() != 1 || status.Configurations[0].Revision != "2" || !status.Enabled) {
		t.Error("real consent not applied/separate enabling changed")
	}
	if mode == "configure-conflict" && status.Configurations[0].Revision != "1" {
		t.Error("current-user drift substituted fresh consent")
	}
	if mode == "cancel-configure" && configures.Load() != 0 {
		t.Error("cancel dispatched Configure")
	}
	if strings.HasPrefix(mode, "unknown-") || strings.HasPrefix(mode, "nested-") {
		report("outcome", "8")
	} else {
		report("outcome", "0")
	}
	report("closed", "")
}
func TestQAUISyncActualTerminalAndSharedServices(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"configure", "configure-conflict", "cancel-configure", "local-replay", "now-unknown-queue", "unknown-eof", "unknown-ctrlc", "unknown-sigterm", "unknown-close", "nested-eof", "nested-sigterm", "nested-close"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", qaSyncTerminalScript, os.Args[0], mode, t.TempDir())
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if b, e := cmd.CombinedOutput(); e != nil {
				t.Fatalf("actual Sync terminal %s: %v\n%s", mode, e, b)
			}
		})
	}
}

const qaSyncTerminalScript = `
import os,sys,pty,termios,subprocess,select,time,fcntl,struct,json,signal
binary,mode,root=sys.argv[1:];master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0));before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL);r,w=os.pipe()
env={'PATH':'/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_SYNC_MODE':mode,'TEMPO_QA_SYNC_ROOT':root,'TEMPO_QA_SYNC_FD':str(w)}
p=subprocess.Popen([binary,'-test.run=^TestQAUISyncActualTerminalChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(w,),preexec_fn=os.setpgrp);os.close(w);transcript=b'';pending=b'';reports=[]
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
def frame():return kind('frame')[-1]['message'] if kind('frame') else ''
def header(s):return s in frame().split('\n')[0]
def until(pred,seconds=2):
 deadline=time.monotonic()+seconds
 while not pred() and time.monotonic()<deadline:poll()
 if not pred():raise AssertionError('missing Sync terminal boundary '+repr(reports[-3:])+' '+repr(transcript[-500:]))
def key(b):os.write(master,b)
def pick(title,id):until(lambda:header(title));key(id.encode()+b'\r')
def dashboard():until(lambda:header('TEMPO'))
def viewback(title):until(lambda:header(title));key(b'\r');dashboard()
def confirm():
 until(lambda:header('Review scoped change'))
 for _ in range(80):key(b'\x1b[B');poll(.003)
 until(lambda:'Request ID' in frame());key(b'y\r')
def open_action(op):dashboard();key(b's');pick('Sync actions',op)
def limit():until(lambda:header('Bounded batch limit'));key(b'\r')
def recover(op):dashboard();key(b's');pick('Recover submitted intent',op)
def recoverback():pick('Recover submitted intent','back');dashboard()
def unknownback():
 until(lambda:header('Sync · Outcome unknown'));key(b'\r');recoverback()
def resize(rows,cols):fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',rows,cols,0,0));p.send_signal(signal.SIGWINCH)
try:
 until(lambda:kind('fixture-valid'));dashboard()
 if mode in ('configure','configure-conflict','cancel-configure'):
  open_action('configure');pick('Choose a Harvest account','11');pick('Tracking mode','duration')
  until(lambda:header('Duration policy'));count=len(kind('frame'));resize(12,80);until(lambda:len(kind('frame'))>count and header('Duration policy'));pick('Duration policy','nearest-hundredth-hour');until(lambda:header('Review scoped change'))
  if 'Request ID' in frame() or 'Read full warning before Yes' not in frame():raise AssertionError('warning fixture was already fully reviewed before geometry checks')
  resize(7,39);until(lambda:header('Terminal too small'));key(b'y\r')
  for _ in range(10):poll()
  if kind('configured'):raise AssertionError('tiny screen authorized Configure')
  resize(12,80);until(lambda:header('Review scoped change'));key(b'y\r')
  for _ in range(10):poll()
  if kind('configured'):raise AssertionError('unreviewed warning authorized Configure')
  if mode=='cancel-configure':key(b'\x1b');dashboard()
  else:
   seen=frame()
   for _ in range(100):key(b'\x1b[B');poll(.003);seen+=frame()
   for warning in ('Verified current user 22','configuration revision 1','18-second','capture is not rounded','does not enable','Request ID'):
    if warning not in ' '.join(seen.split()):raise AssertionError('full consent warning not rendered '+warning)
   key(b'y\r');viewback('Sync · Failed' if mode=='configure-conflict' else 'Sync · Local operation complete')
 elif mode=='local-replay' or mode.startswith('unknown-'):
  open_action('pause');confirm();until(lambda:kind('paused'))
  if mode=='local-replay':
   unknownback();recover('status');until(lambda:header('Sync · Read-only status'));key(b'\r');recoverback();recover('replay');viewback('Sync · Exact local receipt complete')
  elif mode=='unknown-eof':key(b'\x04')
  elif mode=='unknown-ctrlc':key(b'\x03')
  elif mode=='unknown-sigterm':p.send_signal(signal.SIGTERM)
  else:key(b'\x1b');dashboard();key(b'q')
 else:
  open_action('now');limit();confirm()
  if mode.startswith('nested-'):
   unknownback();recover('reconcile');pick('Observed outbox target','all');limit();until(lambda:header('Sync · Read-only reconciliation'));key(b'\r');recoverback()
   if mode=='nested-eof':key(b'\x04')
   elif mode=='nested-sigterm':p.send_signal(signal.SIGTERM)
   else:key(b'q')
  else:
   viewback('Sync · Local operation complete');open_action('now');limit();confirm();viewback('Sync · Local operation complete')
 if not mode.startswith('unknown-') and not mode.startswith('nested-'):dashboard();key(b'q')
 until(lambda:kind('closed'));deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll()
 if p.poll() is None:raise AssertionError('Sync work survived terminal cleanup')
 for _ in range(5):poll()
 after=termios.tcgetattr(slave);afterflags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':afterflags&=~0x10000;flags&=~0x10000;after[3]&=~termios.PENDIN;before[3]&=~termios.PENDIN
 if before!=after or flags!=afterflags:raise AssertionError('Sync failed original TTY restoration')
 for seq in (b'\x1b[?1049h',b'\x1b[?25l',b'\x1b[?2004h',b'\x1b[0m',b'\x1b[?25h',b'\x1b[?1049l',b'\x1b[?2004l'):
  if seq not in transcript:raise AssertionError('missing terminal restoration '+repr(seq))
 if b'RAW-SYNC' in transcript or b'synthetic-only-token' in transcript:raise AssertionError('raw Sync error/credential leaked')
 names=[x['kind'] for x in reports]
 if kind('retained') and names.index('retained')<names.index('terminal-closed'):raise AssertionError('Sync IDs reported before Close')
 if mode.startswith('nested-') and [x['message'] for x in kind('retained')]!=[kind('run')[0]['message'],kind('reconciled')[0]['message']]:raise AssertionError('nested terminal report omitted original POST/GET receipt IDs')
 if mode=='local-replay' and len(set(x['message'] for x in kind('paused')))!=1:raise AssertionError('new UUID replaced exact receipt')
 if p.returncode or b'FAIL' in transcript:raise AssertionError('Sync child failed '+repr(transcript[-1500:]))
 print('actual Sync terminal/shared service verified '+mode)
finally:
 if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait()
 for fd in (r,master,slave):os.close(fd)
`

// Admission check distinguishes an invalid seed/transport from the RED missing runner seam.
func TestQAUISyncTerminalSyntheticFixtureAdmission(t *testing.T) {
	local, p, http := qaSyncTerminalFixture(t, t.TempDir(), func(string, string) {})
	deps := activity.SyncDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { return p, nil }}
	for _, id := range []string{"dddddddd-dddd-4ddd-8ddd-dddddddddddd", "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"} {
		if _, e := local.SyncNow(context.Background(), activity.SyncRunInput{RequestID: id, Limit: 20}, deps); e != nil {
			t.Fatal(e)
		}
	}
	status, e := local.SyncStatus(context.Background())
	if e != nil || len(status.Items) != 1 || status.Items[0].State != "unknown" || http.posts.Load() != 1 {
		t.Fatal("synthetic uncertain transport or nonretryable queue fixture invalid")
	}
}
