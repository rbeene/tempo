package ui_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
	"github.com/rbeene/tempo/internal/ui"
	"github.com/rivo/uniseg"
)

type qaAppearancePTYScreen struct {
	*terminal.Session
	report func(string, string)
}

func (s *qaAppearancePTYScreen) Draw(ctx context.Context, lines []string) error {
	cols, rows, err := s.Size()
	if err != nil {
		return err
	}
	if len(lines) > rows {
		s.report("error", "frame exceeds row budget")
	}
	for _, line := range lines {
		plain := qaAppearanceRunSGR.ReplaceAllString(line, "")
		if uniseg.StringWidth(plain) > cols-1 || strings.ContainsAny(plain, "\x1b\x00\x03\x07") {
			s.report("error", "frame violates cell/control budget")
		}
	}
	err = s.Session.Draw(ctx, lines)
	if err == nil {
		s.report("frame", strings.Join(lines, "\n"))
	}
	return err
}

func TestQAAppearancePTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_APPEARANCE_MODE")
	if mode == "" {
		return
	}
	fd, err := strconv.Atoi(os.Getenv("TEMPO_QA_APPEARANCE_REPORT_FD"))
	if err != nil {
		os.Exit(81)
	}
	reportFile := os.NewFile(uintptr(fd), "appearance-report")
	report := func(kind, message string) {
		_ = json.NewEncoder(reportFile).Encode(map[string]string{"kind": kind, "message": message})
	}
	root := os.Getenv("TEMPO_QA_APPEARANCE_ROOT")
	if !filepath.IsAbs(root) {
		report("error", "synthetic absolute root required")
		os.Exit(82)
	}
	caps := themes.Capabilities{OutputTTY: true, Term: "xterm-256color"}
	switch os.Getenv("TEMPO_QA_APPEARANCE_COLORS") {
	case "truecolor":
		caps.ColorTerm = "truecolor"
	case "ansi16":
		caps.Term = "ansi"
	case "none":
		caps.NoColor = "1"
	case "unknown":
		caps.Term = "unknown-terminal"
	}
	path := filepath.Join(root, "preferences.json")
	service := qaAppearanceRunService(path, nil)
	initial := os.Getenv("TEMPO_QA_APPEARANCE_INITIAL")
	if initial != "terminal-default" {
		_, err = service.Set(context.Background(), themes.SetInput{Theme: initial, IfRevision: "0", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"})
		if err != nil {
			report("error", "synthetic preference seed failed")
			os.Exit(83)
		}
	}
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
	if mode == "unknown-sigterm" {
		service = qaAppearanceRunService(path, func(stage string) error {
			if stage == "directory_sync" {
				report("crossed", "")
				<-ctx.Done()
				return errors.New("PRIVATE synthetic durability")
			}
			return nil
		})
	}
	activityPath := filepath.Join(root, "absent", "activity-state.json")
	activityService := activity.New(activity.Options{Path: activityPath, HookPolicies: hookstate.New(hookstate.Options{Path: filepath.Join(root, "absent", "hooks.json")})})
	session, err := terminal.Open(ctx, os.Stdin, os.Stdout)
	if err == nil {
		err = ui.Run(session.Context(), &qaAppearancePTYScreen{Session: session, report: report}, activityService, ui.Options{Appearance: service, Capabilities: caps, Refresh: make(chan time.Time)})
	}
	signal.Stop(signals)
	cancel(nil)
	<-joined
	want := 0
	if mode == "sigint" {
		want = 130
	}
	if mode == "sigterm" {
		want = 143
	}
	if mode == "unknown-sigterm" {
		var safe *themes.Error
		if !errors.As(err, &safe) || safe.Code != "local_write_unknown" || !safe.Uncertain || safe.Details["theme"] != "gruvbox" || safe.Details["if_revision"] != "1" || safe.Details["request_id"] == "" {
			report("error", "actual signal lost joined uncertain replay identity")
		}
	} else if err != nil {
		var end *terminal.ExitError
		if !errors.As(err, &end) || end.Code != want {
			report("error", "Appearance lost terminal outcome")
		}
	} else if want != 0 {
		report("error", "signal incorrectly reported clean exit")
	}
	if _, e := os.Stat(filepath.Join(root, "absent")); !errors.Is(e, os.ErrNotExist) {
		report("error", "Appearance initialized activity/auth/hook state")
	}
	report("closed", "")
	os.Exit(0)
}

func TestQAAppearanceActualPTYWidthsColorsAndSharedPersistence(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 required for real Appearance PTY")
	}
	for _, color := range []string{"truecolor", "ansi256", "ansi16", "none", "unknown"} {
		for _, initial := range []string{"terminal-default", "tokyo-night", "gruvbox", "catppuccin"} {
			t.Run(color+"/"+initial, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
				defer cancel()
				root := filepath.Dir(qaAppearanceRunPath(t))
				cmd := exec.CommandContext(ctx, python, "-c", qaAppearancePTYScript, os.Args[0], "exercise", root, color, initial)
				cmd.Env = []string{"PATH=/usr/bin:/bin"}
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("actual Appearance PTY: %v\n%s", err, output)
				}
				got, err := qaAppearanceRunService(filepath.Join(root, "preferences.json"), nil).Show(context.Background(), "")
				if err != nil || got.Theme.ID != "terminal-default" || got.PreferenceRevision == "0" {
					t.Fatalf("PTY Apply/reset did not persist shared preference: %+v %v", got, err)
				}
			})
		}
	}
}

func TestQAAppearanceActualPTYPreviewSignalsAndUnknownPrecedence(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"sigint", "sigterm", "eof", "unknown-sigterm"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			root := filepath.Dir(qaAppearanceRunPath(t))
			cmd := exec.CommandContext(ctx, python, "-c", qaAppearancePTYScript, os.Args[0], mode, root, "truecolor", "tokyo-night")
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("actual Appearance signal PTY: %v\n%s", err, output)
			}
			want, rev := "tokyo-night", "1"
			if mode == "unknown-sigterm" {
				want, rev = "gruvbox", "2"
			}
			got, err := qaAppearanceRunService(filepath.Join(root, "preferences.json"), nil).Show(context.Background(), "")
			if err != nil || got.Theme.ID != want || got.PreferenceRevision != rev {
				t.Fatalf("signal rolled back or saved draft: %+v %v", got, err)
			}
		})
	}
}

const qaAppearancePTYScript = `
import os,sys,pty,termios,subprocess,select,time,signal,fcntl,struct,json,re
binary,mode,root,color,initial=sys.argv[1:]
master,slave=pty.openpty()
fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',40,120,0,0))
before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL)
report_r,report_w=os.pipe()
env={'PATH':'/usr/bin:/bin','GOMAXPROCS':'2','GORACE':'atexit_sleep_ms=0','TEMPO_QA_APPEARANCE_MODE':mode,'TEMPO_QA_APPEARANCE_REPORT_FD':str(report_w),'TEMPO_QA_APPEARANCE_ROOT':root,'TEMPO_QA_APPEARANCE_COLORS':color,'TEMPO_QA_APPEARANCE_INITIAL':initial}
p=subprocess.Popen([binary,'-test.run=^TestQAAppearancePTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(report_w,),preexec_fn=os.setpgrp)
os.close(report_w);transcript=b'';pending=b'';reports=[]
sgr=re.compile(r'\x1b\[[0-9;]*m')
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
def frames(needle):return [r['message'] for r in reports if r['kind']=='frame' and needle in sgr.sub('',r['message'])]
def closed():return any(r['kind']=='closed' for r in reports)
def until(pred,seconds=2):
 deadline=time.monotonic()+seconds
 while not pred() and time.monotonic()<deadline:
  poll()
  if closed() and not pred():break
 if not pred():raise AssertionError('missing actual Appearance behavior: '+repr(reports[-4:]))
def newframe(needle,send):
 count=len(frames(needle));os.write(master,send);until(lambda:len(frames(needle))>count);return frames(needle)[-1]
def resize(cols,rows,needle):
 count=len(frames(needle));fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',rows,cols,0,0));p.send_signal(signal.SIGWINCH);until(lambda:len(frames(needle))>count)
def assert_palette(frame,palette):
 codes=sgr.findall(frame)
 if color in ('none','unknown') or palette=='terminal-default':
  if codes:raise AssertionError('no-color/default frame contains style escapes')
 elif color=='truecolor':
  bg={'tokyo-night':'48;2;26;27;38','gruvbox':'48;2;40;40;40','catppuccin':'48;2;30;30;46'}[palette]
  if not any(bg in code for code in codes):raise AssertionError('real frame not using candidate exact RGB base')
 elif color=='ansi256':
  bg={'tokyo-night':'48;5;234','gruvbox':'48;5;235','catppuccin':'48;5;235'}[palette]
  if not any(bg in code for code in codes):raise AssertionError('real frame not using approved256 base')
 elif color=='ansi16' and not codes:raise AssertionError('16-color semantic role frame missing')
try:
 until(lambda:len(frames('No activity yet'))>0)
 assert_palette(frames('No activity yet')[-1],initial)
 if mode=='exercise':
  prefs=os.path.join(root,'preferences.json')
  baseline=open(prefs,'rb').read() if os.path.exists(prefs) else None
  for candidate in ('terminal-default','tokyo-night','gruvbox','catppuccin'):
   newframe('['+initial+']',b'A')
   frame=newframe('Search: '+candidate,candidate.encode())
   assert_palette(frame,candidate)
   for cols,rows in ((80,24),(40,12),(20,5),(120,40)):
    resize(cols,rows,'too small' if cols==20 else 'Search: '+candidate)
   # Preview and all resizing remain read-only, including absent preferences.
   now=open(prefs,'rb').read() if os.path.exists(prefs) else None
   if now!=baseline:raise AssertionError('preview/resize persisted preference')
   frame=newframe('No activity yet',b'\x1b');assert_palette(frame,initial)
  target='catppuccin' if initial=='gruvbox' else 'gruvbox'
  newframe('['+initial+']',b'A');newframe('Search: '+target,target.encode())
  newframe('Review scoped change',b'\r');newframe('Selected: Yes',b'y')
  frame=newframe('No activity yet',b'\r');assert_palette(frame,target)
  newframe('['+target+']',b'A');newframe('Search: terminal-default',b'terminal-default')
  newframe('Review scoped change',b'\r');newframe('Selected: Yes',b'y')
  frame=newframe('No activity yet',b'\r');assert_palette(frame,'terminal-default')
  os.write(master,b'q')
 else:
  newframe('['+initial+']',b'A');newframe('Search: gruvbox',b'gruvbox')
  if mode=='unknown-sigterm':
   newframe('Review scoped change',b'\r');newframe('Selected: Yes',b'y');os.write(master,b'\r')
   until(lambda:any(r['kind']=='crossed' for r in reports));p.send_signal(signal.SIGTERM)
  elif mode=='eof':os.write(master,b'\x04')
  else:p.send_signal(signal.SIGINT if mode=='sigint' else signal.SIGTERM)
 until(closed,2)
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll(.01)
 if p.poll() is None:raise AssertionError('Appearance retained owned work after cleanup')
 while select.select([master],[],[],.01)[0]:
  old=len(transcript);poll(.01)
  if len(transcript)==old:break
 after=termios.tcgetattr(slave);after_flags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':
  after_flags &= ~0x10000;flags &= ~0x10000;after[3] &= ~termios.PENDIN;before[3] &= ~termios.PENDIN
 if after!=before or after_flags!=flags:raise AssertionError('Appearance failed actual termios/descriptor restoration')
 for seq in (b'\x1b[?1049h',b'\x1b[?25l',b'\x1b[?2004h',b'\x1b[0m',b'\x1b[?25h',b'\x1b[?1049l',b'\x1b[?2004l'):
  if seq not in transcript:raise AssertionError('missing actual ownership/cleanup sequence '+repr(seq))
 if transcript.count(b'\x1b[?1049h')!=1 or transcript.count(b'\x1b[?1049l')!=1:raise AssertionError('Appearance launched another terminal owner')
 if b'No activity yet' not in transcript:raise AssertionError('reported frames missing from real output')
 errors=[r['message'] for r in reports if r['kind']=='error']
 if errors or p.returncode:raise AssertionError('Appearance child errors '+repr(errors)+' exit '+str(p.returncode))
 print('actual Appearance frame/input/persistence/width/color/cleanup verified '+mode+' '+color+' '+initial)
finally:
 if p.poll() is None:p.kill();p.wait()
 os.close(report_r);os.close(master);os.close(slave)
`
