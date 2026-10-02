package cli_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
)

func TestQACLIThemePTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_CLI_THEME_MODE")
	if mode == "" {
		return
	}
	fd, err := strconv.Atoi(os.Getenv("TEMPO_QA_CLI_THEME_REPORT_FD"))
	if err != nil {
		t.Fatal(err)
	}
	reportFile := os.NewFile(uintptr(fd), "cli-theme-report")
	report := func(kind string, value any) {
		_ = json.NewEncoder(reportFile).Encode(map[string]any{"kind": kind, "value": value})
	}
	root := os.Getenv("TEMPO_QA_CLI_THEME_ROOT")
	if !filepath.IsAbs(root) {
		t.Fatal("synthetic absolute fixture required")
	}
	prefs := filepath.Join(root, "preferences.json")
	s := themes.New(themes.Options{Path: prefs})
	if _, err := s.Set(context.Background(), themes.SetInput{Theme: "tokyo-night", IfRevision: "0", RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}); err != nil {
		t.Fatal(err)
	}
	getenv := func(k string) string {
		switch k {
		case "TEMPO_PREFERENCES":
			return prefs
		case "TERM":
			return "xterm-256color"
		case "COLORTERM":
			return "truecolor"
		}
		return ""
	}
	config := filepath.Join(root, "config.json")
	if err := os.WriteFile(config, []byte("PRIVATE ACCOUNT SENTINEL"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-sigs:
			cancel(&terminal.ExitError{Code: 143})
		case <-ctx.Done():
		}
	}()
	deps := cli.Dependencies{Getenv: getenv, ConfigPath: config, Store: qaForbiddenLegacyStore{t}, NewProvider: func(string, string) harvest.Provider { t.Fatal("Appearance accessed Harvest"); return nil },
		Activity: activity.New(activity.Options{Path: filepath.Join(root, "absent", "activity.json")}),
		Auth: auth.NewService(auth.Options{ConfigPath: config, LockPath: filepath.Join(root, "auth.lock"), Getenv: func(string) string { t.Fatal("Appearance read credentials"); return "" }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
			t.Fatal("Appearance used native auth")
			return auth.NativeReply{}, nil
		})}),
	}
	args := []string{"ui"}
	switch mode {
	case "default":
		args = nil
	case "watch":
		args = []string{"activity", "status", "--watch"}
	case "unknown-sigterm":
		deps.Themes = themes.New(themes.Options{Path: prefs, Fault: func(stage string) error {
			if stage == "directory_sync" {
				report("crossed", "")
				<-ctx.Done()
				return errors.New("PRIVATE synthetic durability")
			}
			return nil
		}})
	case "finite-link":
		project := filepath.Join(root, "project")
		if err := os.Mkdir(project, 0700); err != nil {
			t.Fatal(err)
		}
		args = []string{"link", "--path", project, "--timezone", "UTC"}
		deps.Auth = auth.NewService(auth.Options{ConfigPath: config, LockPath: filepath.Join(root, "auth.lock"), Getenv: func(k string) string {
			if k == "HARVEST_TOKEN" {
				return "SYNTHETIC_ONLY_TOKEN"
			}
			if k == "HARVEST_ACCOUNT_ID" {
				return "11"
			}
			return ""
		}, NewProvider: func(string, string) harvest.Provider { report("stage", "mock-provider"); return &qaGuidedCLIProvider{} }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
			t.Fatal("finite mocked picker used native auth")
			return auth.NativeReply{}, nil
		})})
	}
	report("stage", "before-cli")
	code := cli.Run(ctx, args, os.Stdin, os.Stdout, os.Stderr, deps)
	signal.Stop(sigs)
	cancel(nil)
	<-joined
	want := 0
	if mode == "unknown-sigterm" {
		want = 8
	}
	if code != want {
		report("error", "actual CLI theme outcome="+strconv.Itoa(code)+" want="+strconv.Itoa(want))
	}
	if b, err := os.ReadFile(config); err != nil || string(b) != "PRIVATE ACCOUNT SENTINEL" {
		report("error", "appearance changed account configuration")
	}
	if _, err := os.Stat(filepath.Join(root, "absent")); !errors.Is(err, os.ErrNotExist) {
		report("error", "appearance/cancel initialized capture state")
	}
	report("closed", code)
}

func TestQACLIActualThemeSessionInjectionAndUncertaintyBoundary(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal("real Git required for shared finite-link discovery")
	}
	for _, mode := range []string{"default", "ui", "watch", "finite-link", "unknown-sigterm"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
			defer cancel()
			root := t.TempDir()
			if err := os.Chmod(root, 0700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(ctx, python, "-c", qaCLIThemePTYScript, os.Args[0], mode, root, filepath.Dir(git))
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("actual CLI theme wiring PTY: %v\n%s", err, output)
			}
			got, err := themes.New(themes.Options{Path: filepath.Join(root, "preferences.json")}).Show(context.Background(), "")
			want := "gruvbox"
			if mode == "finite-link" {
				want = "tokyo-night"
			}
			if err != nil || got.Theme.ID != want {
				t.Fatalf("CLI shared preference result %+v %v want%s", got, err, want)
			}
		})
	}
}

const qaCLIThemePTYScript = `
import os,sys,pty,termios,subprocess,select,time,signal,fcntl,struct,json,re
binary,mode,root,git_bin=sys.argv[1:]
master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,80,0,0))
before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL)
rr,rw=os.pipe();env={'PATH':git_bin+os.pathsep+'/usr/bin:/bin','GOMAXPROCS':'2','GORACE':'atexit_sleep_ms=0','TEMPO_QA_CLI_THEME_MODE':mode,'TEMPO_QA_CLI_THEME_REPORT_FD':str(rw),'TEMPO_QA_CLI_THEME_ROOT':root}
p=subprocess.Popen([binary,'-test.run=^TestQACLIThemePTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(rw,),preexec_fn=os.setpgrp)
os.close(rw);transcript=b'';pending=b'';reports=[];sgr=re.compile(rb'\x1b\[[0-9;]*m')
def poll(timeout=.02):
 global transcript,pending
 ready,_,_=select.select([master,rr],[],[],timeout)
 if master in ready:
  try:transcript+=os.read(master,65536)
  except OSError:pass
 if rr in ready:
  pending+=os.read(rr,65536)
  while b'\n' in pending:
   line,pending=pending.split(b'\n',1)
   if line:reports.append(json.loads(line))
def closed():return any(r['kind']=='closed' for r in reports)
def lastframe():return sgr.sub(b'',transcript.split(b'\x1b[H\x1b[J')[-1])
def until(pred):
 deadline=time.monotonic()+2
 while not pred() and time.monotonic()<deadline:
  poll()
  if closed() and not pred():break
 if not pred():raise AssertionError('actual CLI behavior missing '+repr(reports)+' '+repr(transcript[-400:]))
try:
 if mode=='finite-link':
  until(lambda:b'Choose a Harvest project' in transcript)
  rows=[row for row in transcript.split(b'\r\n') if b'Choose a Harvest project' in row]
  if not any(b'48;2;26;27;38' in row for row in rows):raise AssertionError('CLI finite shared picker did not load saved theme')
  os.write(master,b'\x1b')
 else:
  until(lambda:b'No activity yet' in transcript)
  frame=transcript.split(b'\x1b[H\x1b[J')[-1]
  if b'48;2;26;27;38' not in frame:raise AssertionError('CLI dashboard did not inject saved style/capabilities')
  os.write(master,b'A');until(lambda:b'[tokyo-night]' in lastframe())
  os.write(master,b'gruvbox');until(lambda:b'Search: gruvbox' in lastframe())
  os.write(master,b'\r');until(lambda:b'Review scoped change' in lastframe())
  os.write(master,b'y');until(lambda:b'Selected: Yes' in lastframe());os.write(master,b'\r')
  if mode=='unknown-sigterm':until(lambda:any(r['kind']=='crossed' for r in reports));p.send_signal(signal.SIGTERM)
  else:
   until(lambda:b'No activity yet' in lastframe())
   if b'48;2;40;40;40' not in transcript.split(b'\x1b[H\x1b[J')[-1]:raise AssertionError('CLI Apply did not activate shared persisted palette')
   os.write(master,b'q')
 until(closed)
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll(.01)
 for _ in range(3):poll(.01)
 if p.poll() is None:raise AssertionError('CLI Appearance retained terminal work')
 if mode=='unknown-sigterm':
  if b'local_write_unknown' not in transcript or b'PRIVATE' in transcript:raise AssertionError('CLI erased or leaked uncertain preference outcome')
  st=json.load(open(os.path.join(root,'preferences.json')))
  ids=[k for k in st['requests'] if k!='bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb']
  if len(ids)!=1 or ids[0].encode() not in transcript or b'gruvbox' not in transcript:raise AssertionError('CLI unknown output lacks exact pending request identity')
 after=termios.tcgetattr(slave);af=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':af &= ~0x10000;flags &= ~0x10000;after[3] &= ~termios.PENDIN;before[3] &= ~termios.PENDIN
 if after!=before or af!=flags:raise AssertionError('CLI theme session failed termios/descriptor restoration')
 if b'\x1b[0m\x1b[?2004l' not in transcript:raise AssertionError('CLI theme output failed final reset')
 errors=[r['value'] for r in reports if r['kind']=='error']
 if errors or p.returncode:raise AssertionError('CLI theme errors '+repr(errors)+' exit '+str(p.returncode))
 print('actual CLI shared themes/finite picker/outcome/cleanup verified '+mode)
finally:
 if p.poll() is None:p.kill();p.wait()
 os.close(rr);os.close(master);os.close(slave)
`
