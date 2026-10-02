//go:build darwin || linux

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
	"io"
	"net"
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
)

type qaCLISyncTransport struct {
	t                  *testing.T
	user, posts, reads atomic.Int32
	block              atomic.Bool
	drift              atomic.Bool
	userChecks         atomic.Int32
	report             func(string, string)
}

func (p *qaCLISyncTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	p.reads.Add(1)
	code := 200
	body := ""
	switch r.URL.Path {
	case "/id/accounts":
		body = `{"accounts":[{"id":11,"product":"harvest","name":"Synthetic"}]}`
	case "/v2/users/me":
		if p.drift.Load() && p.userChecks.Add(1) > 1 {
			p.user.Store(33)
		}
		body = `{"id":` + strconv.Itoa(int(p.user.Load())) + `,"is_active":true,"timezone":"UTC"}`
	case "/v2/company":
		body = `{"is_active":true,"wants_timestamp_timers":false,"clock":"24h"}`
	case "/v2/users/me/project_assignments":
		body = `{"project_assignments":[{"is_active":true,"project":{"id":100,"name":"Synthetic"},"task_assignments":[{"is_active":true,"task":{"id":200}}]}],"links":{"next":null}}`
	case "/v2/time_entries":
		if r.Method == "POST" {
			p.posts.Add(1)
			p.report("http-post", "synthetic only")
			if p.block.Load() {
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
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
func qaCLISyncFixture(t *testing.T, root string, report func(string, string)) (*activity.Service, *harvest.Client, *qaCLISyncTransport) {
	t.Helper()
	project := filepath.Join(root, "project")
	if e := os.Mkdir(project, 0700); e != nil {
		t.Fatal(e)
	}
	transport := &qaCLISyncTransport{t: t, report: report}
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

func TestQASyncCLIActualTerminalChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_CLI_SYNC_MODE")
	if mode == "" {
		return
	}
	root := os.Getenv("TEMPO_QA_CLI_SYNC_ROOT")
	fd, e := strconv.Atoi(os.Getenv("TEMPO_QA_CLI_SYNC_FD"))
	if e != nil || !filepath.IsAbs(root) {
		t.Fatal("isolated private fixture required")
	}
	f := os.NewFile(uintptr(fd), "cli-sync-report")
	defer f.Close()
	var mu sync.Mutex
	report := func(k, v string) {
		mu.Lock()
		defer mu.Unlock()
		json.NewEncoder(f).Encode(map[string]string{"kind": k, "message": v})
	}
	local, p, transport := qaCLISyncFixture(t, root, report)
	if strings.HasPrefix(mode, "unknown-") {
		transport.block.Store(true)
	}

	notify := strings.HasPrefix(mode, "notify-")
	var socket *net.UnixConn
	hintPath := ""
	if notify {
		dir, e := os.MkdirTemp("/tmp", "tempo-sync-hint-")
		if e != nil {
			t.Fatal(e)
		}
		defer os.RemoveAll(dir)
		hintPath = filepath.Join(dir, "state")
		socket, e = net.ListenUnixgram("unixgram", &net.UnixAddr{Name: hintPath + ".worker.sock", Net: "unixgram"})
		if e != nil {
			t.Fatal(e)
		}
		defer socket.Close()
		if e = os.Chmod(hintPath+".worker.sock", 0600); e != nil {
			t.Fatal(e)
		}
	}
	if mode == "notify-configure-error" {
		transport.drift.Store(true)
	}
	config := filepath.Join(root, "config")
	before := []byte("synthetic malformed private config")
	if e = os.WriteFile(config, before, 0600); e != nil {
		t.Fatal(e)
	}
	lazy := strings.HasPrefix(mode, "lazy-")
	credentials := auth.NewService(auth.Options{ConfigPath: config, Getenv: func(key string) string {
		if lazy {
			t.Error("opening/back/status resolved credentials")
			return ""
		}
		switch key {
		case "HARVEST_TOKEN":
			return "synthetic-only-token"
		case "HARVEST_ACCOUNT_ID":
			return "11"
		}
		return ""
	}, NewProvider: func(_, account string) harvest.Provider {
		if lazy {
			t.Error("local Sync action constructed provider")
		}
		if account != "11" && account != "" {
			t.Error("cross-account provider")
		}
		return p
	}, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		t.Error("Sync CLI touched native credentials")
		return auth.NativeReply{}, errors.New("forbidden native")
	})})
	report("fixture-valid", "")
	// cli.Run is called with the executable's existing signal-owner contract.
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
	code := cli.Run(ctx, []string{"ui"}, os.Stdin, os.Stdout, os.Stderr, cli.Dependencies{Activity: local, Auth: credentials, ConfigPath: config, Getenv: func(key string) string {
		if key == "TEMPO_STATE" {
			return hintPath
		}
		t.Error("UI resolved unrelated defaults")
		return ""
	}})
	cancel(nil)
	signal.Stop(signals)
	<-joined
	want := 0
	if strings.HasPrefix(mode, "unknown-") {
		want = 8
	}
	if code != want {
		t.Errorf("Sync CLI exit%d want%d", code, want)
	}
	after, e := os.ReadFile(config)
	if e != nil || string(after) != string(before) {
		t.Error("Sync changed saved default config")
	}
	status, e := local.SyncStatus(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if lazy && transport.reads.Load() != 0 {
		t.Error("local Sync navigation performed network")
	}
	if mode == "pause" && status.Enabled {
		t.Error("explicit pause not wired")
	}
	if mode == "configure" && (len(status.Configurations) != 1 || status.Configurations[0].Revision != "2" || !status.Enabled) {
		t.Error("actual Configure lost independent consent")
	}
	if strings.HasPrefix(mode, "unknown-") {
		if transport.posts.Load() != 1 || len(status.Items) != 1 || status.Items[0].State != "submitting" {
			t.Error("CLI unknown lost durable nonretryable claim")
		}
		b, e := os.ReadFile(filepath.Join(root, "state", "activity.json"))
		if e != nil {
			t.Fatal(e)
		}
		var state struct {
			Requests map[string]struct {
				Operation string `json:"operation"`
			} `json:"requests"`
		}
		if json.Unmarshal(b, &state) != nil {
			t.Fatal("private receipt decode")
		}
		id := ""
		for k, v := range state.Requests {
			if v.Operation == "sync.now" {
				id = k
			}
		}
		if id == "" {
			t.Error("missing durable submitted ID")
		}
		report("submitted", id)
	}

	if notify {
		socket.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		buf := make([]byte, 32)
		n, _, e := socket.ReadFromUnix(buf)
		want := mode == "notify-configure" || mode == "notify-resume"
		if want && (e != nil || string(buf[:n]) != "recheck") {
			t.Error("successful shared consent did not send bounded recheck hint")
		}
		if !want && e == nil {
			t.Error("failed/nonconsent operation notified worker")
		}
		if want {
			socket.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
			if _, _, e = socket.ReadFromUnix(buf); e == nil {
				t.Error("duplicate notification")
			}
		}
	}
	report("outcome", strconv.Itoa(code))
	report("closed", "")
}
func TestQASyncCLIActualTerminalComposition(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"lazy-back", "lazy-status", "pause", "configure", "unknown-eof", "unknown-ctrlc", "unknown-sigterm", "notify-configure", "notify-resume", "notify-configure-error", "notify-pause"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", qaCLISyncScript, os.Args[0], mode, t.TempDir())
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if b, e := cmd.CombinedOutput(); e != nil {
				t.Fatalf("actual Sync CLI%s: %v\n%s", mode, e, b)
			}
		})
	}
}

const qaCLISyncScript = `
import os,sys,pty,termios,subprocess,select,time,fcntl,struct,json,signal,re
binary,mode,root=sys.argv[1:];master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0));before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL);r,w=os.pipe()
env={'PATH':'/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_CLI_SYNC_MODE':mode,'TEMPO_QA_CLI_SYNC_ROOT':root,'TEMPO_QA_CLI_SYNC_FD':str(w)}
p=subprocess.Popen([binary,'-test.run=^TestQASyncCLIActualTerminalChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(w,),preexec_fn=os.setpgrp);os.close(w);transcript=b'';pending=b'';reports=[]
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
def header(s):return s.encode() in frame().split(b'\r\n')[0]
def until(pred,seconds=2):
 deadline=time.monotonic()+seconds
 while not pred() and time.monotonic()<deadline:poll()
 if not pred():raise AssertionError('missing actual CLI Sync boundary '+repr(reports[-3:])+' '+repr(transcript[-800:]))
def key(b):os.write(master,b)
def pick(title,id):until(lambda:header(title));key(id.encode()+b'\r')
def confirm():
 until(lambda:header('Review scoped change'))
 for _ in range(80):key(b'\x1b[B');poll(.003)
 until(lambda:b'Request ID' in frame());key(b'y\r')
try:
 until(lambda:kind('fixture-valid'));until(lambda:header('TEMPO'));key(b's');until(lambda:header('Sync actions'))
 if mode=='lazy-back':key(b'back\r')
 elif mode=='lazy-status':key(b'status\r');until(lambda:header('Sync · Read-only status'));key(b'\r')
 elif mode in ('pause','notify-pause','notify-resume'):key(b'resume\r' if mode=='notify-resume' else b'pause\r');confirm();until(lambda:header('Sync · Local operation complete'));key(b'\r')
 elif mode in ('configure','notify-configure','notify-configure-error'):
  key(b'configure\r');pick('Choose a Harvest account','11');pick('Tracking mode','duration');pick('Duration policy','exact');confirm();until(lambda:header('Sync · Failed' if mode=='notify-configure-error' else 'Sync · Local operation complete'));key(b'\r')
 else:
  key(b'now\r');until(lambda:header('Bounded batch limit'));key(b'\r');confirm();until(lambda:kind('http-post'))
  if mode=='unknown-eof':key(b'\x04')
  elif mode=='unknown-ctrlc':key(b'\x03')
  else:p.send_signal(signal.SIGTERM)
 if not mode.startswith('unknown-'):until(lambda:header('TEMPO'));key(b'q')
 until(lambda:kind('closed'));deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll()
 if p.poll() is None:raise AssertionError('CLI retained owned Sync request')
 for _ in range(5):poll()
 after=termios.tcgetattr(slave);afterflags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':afterflags&=~0x10000;flags&=~0x10000;after[3]&=~termios.PENDIN;before[3]&=~termios.PENDIN
 if before!=after or flags!=afterflags:raise AssertionError('CLI Sync failed original terminal restoration')
 if b'RAW-SYNC' in transcript or b'synthetic-only-token' in transcript:raise AssertionError('CLI Sync leaked raw provider/credential data')
 for seq in (b'\x1b[?1049h',b'\x1b[?25l',b'\x1b[?2004h',b'\x1b[0m',b'\x1b[?25h',b'\x1b[?1049l',b'\x1b[?2004l'):
  if seq not in transcript:raise AssertionError('missing CLI restoration '+repr(seq))
 if mode.startswith('unknown-'):
  tail=transcript.split(b'\x1b[?1049l')[-1];id=kind('submitted')[-1]['message'];expected='8'
  if not re.fullmatch('[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}',id):raise AssertionError('durable UUID invalid')
  if id.encode() not in tail or b'tempo: sync: local_write_unknown' not in tail:raise AssertionError('CLI lost safe exact UUID after terminal Close')
  if len(kind('http-post'))!=1:raise AssertionError('CLI duplicated uncertain POST')
 else:expected='0'
 if kind('outcome')[-1]['message']!=expected:raise AssertionError('CLI Sync exit contract lost')
 if p.returncode or b'FAIL' in transcript:raise AssertionError('CLI child failed '+repr(transcript[-1800:]))
 print('actual CLI Sync composition verified '+mode)
finally:
 if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait()
 for fd in (r,master,slave):os.close(fd)
`
