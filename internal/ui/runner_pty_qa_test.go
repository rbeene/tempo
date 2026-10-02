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
	"github.com/rbeene/tempo/internal/ui"
)

type qaRunnerPTYScreen struct {
	*terminal.Session
	report func(string, string)
}

func (s *qaRunnerPTYScreen) Draw(ctx context.Context, lines []string) error {
	err := s.Session.Draw(ctx, lines)
	if err == nil {
		s.report("frame", strings.Join(lines, "\n"))
	}
	return err
}

// This child exercises the actual shared Status and actual terminal owner,
// never a simulated renderer or private user state. The report pipe only
// synchronizes the driver; terminal bytes still come from Session.Draw.
func TestQAUIRunnerPTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_RUNNER_MODE")
	if mode == "" {
		return
	}
	fd, err := strconv.Atoi(os.Getenv("TEMPO_QA_RUNNER_REPORT_FD"))
	if err != nil {
		os.Exit(71)
	}
	reportFile := os.NewFile(uintptr(fd), "runner-qa-report")
	report := func(kind, message string) {
		_ = json.NewEncoder(reportFile).Encode(map[string]string{"kind": kind, "message": message})
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
	path := os.Getenv("TEMPO_QA_RUNNER_STATE")
	if !filepath.IsAbs(path) {
		report("error", "synthetic absolute state missing")
		os.Exit(72)
	}
	epoch, n := "qa-runner-epoch", "0"
	service := activity.New(activity.Options{Path: path, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}, nil
	}), HookPolicies: hookstate.New(hookstate.Options{Path: filepath.Join(filepath.Dir(path), "hooks.json")})})
	s, err := terminal.Open(ctx, os.Stdin, os.Stdout)
	if err == nil {
		options := ui.Options{}
		if mode != "default-tick" {
			options.Refresh = make(chan time.Time)
		}
		err = ui.Run(s.Context(), &qaRunnerPTYScreen{Session: s, report: report}, service, options)
	}
	signal.Stop(signals)
	cancel(nil)
	<-joined
	want := 0
	switch mode {
	case "ctrlc":
		want = 130
	case "sigterm":
		want = 143
	case "eof":
		want = 0 // The shared terminal contract treats clean input EOF as exit 0.
	}
	if want == 0 && mode != "eof" {
		if err != nil {
			report("error", "runner failed: "+err.Error())
		}
	} else {
		var end *terminal.ExitError
		if !errors.As(err, &end) || end.Code != want {
			report("error", "runner lost terminal exit cause")
		}
	}
	if _, statErr := os.Stat(filepath.Dir(path)); !errors.Is(statErr, os.ErrNotExist) {
		report("error", "read-only dashboard initialized local state")
	}
	report("closed", "")
	if err != nil && want == 0 && mode != "eof" {
		os.Exit(73)
	}
	os.Exit(0)
}

func TestQAUIRunnerActualTerminalAndSharedStatus(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 required for actual PTY verification")
	}
	for _, mode := range []string{"quit", "escape", "paste-refresh-resize", "default-tick", "ctrlc", "sigterm", "eof"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			path := filepath.Join(t.TempDir(), "absent", "activity-state.json")
			cmd := exec.CommandContext(ctx, python, "-c", qaRunnerPTYScript, os.Args[0], mode, path)
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("actual runner PTY %s: %v\n%s", mode, err, output)
			}
		})
	}
}

const qaRunnerPTYScript = `
import os,sys,pty,termios,subprocess,select,time,signal,fcntl,struct,json
binary,mode,state=sys.argv[1:]
master,slave=pty.openpty()
fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,80,0,0))
before=termios.tcgetattr(slave); flags=fcntl.fcntl(slave,fcntl.F_GETFL)
report_r,report_w=os.pipe()
env={'PATH':'/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_RUNNER_MODE':mode,'TEMPO_QA_RUNNER_REPORT_FD':str(report_w),'TEMPO_QA_RUNNER_STATE':state}
p=subprocess.Popen([binary,'-test.run=^TestQAUIRunnerPTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(report_w,),preexec_fn=os.setpgrp)
os.close(report_w)
transcript=b''; pending=b''; reports=[]
def poll(timeout=.02):
 global transcript,pending
 ready,_,_=select.select([master,report_r],[],[],timeout)
 if master in ready:
  try: transcript+=os.read(master,65536)
  except OSError: pass
 if report_r in ready:
  pending+=os.read(report_r,65536)
  while b'\n' in pending:
   line,pending=pending.split(b'\n',1)
   if line: reports.append(json.loads(line))
def closed(): return any(r['kind']=='closed' for r in reports)
def frames(needle): return [r for r in reports if r['kind']=='frame' and needle in r['message']]
def until(predicate,seconds=2):
 deadline=time.monotonic()+seconds
 while not predicate() and time.monotonic()<deadline:
  poll()
  if closed() and not predicate(): break
 if not predicate(): raise AssertionError('expected runner behavior missing: '+repr(reports))
try:
 until(lambda:len(frames('No activity yet'))>=1)
 if b'TEMPO' not in transcript:
  poll(.03)
 if b'TEMPO' not in transcript or b'No activity yet' not in transcript: raise AssertionError('reported shared Status frame was not drawn to actual terminal')
 if mode=='paste-refresh-resize':
  os.write(master,b'\x1b[200~qr\x03\x1b[A\x1b[201~')
  deadline=time.monotonic()+.12
  while time.monotonic()<deadline: poll(.01)
  if closed(): raise AssertionError('paste payload executed quit/cancel action')
  count=len(frames('No activity yet')); os.write(master,b'r')
  until(lambda:len(frames('No activity yet'))>count)
  fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',5,20,0,0)); p.send_signal(signal.SIGWINCH)
  until(lambda:len(frames('too small'))>=1)
  count=len(frames('No activity yet'))
  fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,80,0,0)); p.send_signal(signal.SIGWINCH)
  until(lambda:len(frames('No activity yet'))>count)
  os.write(master,b'q')
 elif mode=='default-tick':
  count=len(frames('No activity yet'))
  until(lambda:len(frames('No activity yet'))>count,1.7)
  os.write(master,b'q')
 elif mode=='sigterm': p.send_signal(signal.SIGTERM)
 else: os.write(master,{'quit':b'q','escape':b'\x1b','ctrlc':b'\x03','eof':b'\x04'}[mode])
 until(closed,2)
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline: poll(.01)
 if p.poll() is None: raise AssertionError('runner child retained owned work')
 while select.select([master],[],[],.01)[0]:
  old=len(transcript);poll(.01)
  if len(transcript)==old: break
 after=termios.tcgetattr(slave); after_flags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':
  after_flags &= ~0x10000; flags &= ~0x10000
  after[3] &= ~termios.PENDIN; before[3] &= ~termios.PENDIN
 if after!=before or after_flags!=flags: raise AssertionError('runner failed to restore original termios/descriptor flags')
 for seq in (b'\x1b[?1049h',b'\x1b[?25l',b'\x1b[?2004h',b'\x1b[0m',b'\x1b[?25h',b'\x1b[?1049l',b'\x1b[?2004l'):
  if seq not in transcript: raise AssertionError('runner missing screen ownership/cleanup sequence '+repr(seq))
 errors=[r['message'] for r in reports if r['kind']=='error']
 if errors or p.returncode: raise AssertionError('runner errors: '+repr(errors)+' exit '+str(p.returncode))
 print('actual shared Status, frame, input, cleanup verified: '+mode)
finally:
 if p.poll() is None: p.kill();p.wait()
 os.close(report_r);os.close(master);os.close(slave)
`
