package terminal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Extend the #10 test-binary/Python kernel-PTY pattern. The extra pipe carries
// synchronization only, so test markers never compete with the terminal writer.
func TestQAUIDashboardPTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_UI_PTY_MODE")
	if mode == "" {
		return
	}
	fd, err := strconv.Atoi(os.Getenv("TEMPO_QA_UI_REPORT_FD"))
	if err != nil {
		os.Exit(60)
	}
	reportFile := os.NewFile(uintptr(fd), "qa-report")
	report := func(kind, message string) {
		_ = json.NewEncoder(reportFile).Encode(map[string]string{"kind": kind, "message": message})
	}
	pipeIn, pipeOut, err := os.Pipe()
	if err != nil {
		report("error", "synthetic pipe creation failed")
		os.Exit(63)
	}
	if !Eligible(os.Stdin, os.Stdout) || Eligible(pipeIn, os.Stdout) || Eligible(os.Stdin, pipeOut) || Eligible(pipeIn, pipeOut) {
		report("error", "actual PTY/pipe stream eligibility was incorrect")
		os.Exit(64)
	}
	_ = pipeIn.Close()
	_ = pipeOut.Close()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	var peer chan os.Signal
	if mode == "suspend-scope" {
		peer = make(chan os.Signal, 4)
		signal.Notify(peer, syscall.SIGTSTP)
		defer signal.Stop(peer)
	}
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
			cancel(&ExitError{Code: code})
		case <-ctx.Done():
		}
	}()
	s, err := Open(ctx, os.Stdin, os.Stdout)
	if err != nil {
		report("error", "Open failed")
		os.Exit(61)
	}
	stops := 0
	if mode == "suspend-scope" {
		stopOwner := s.stopSignals
		s.stopSignals = func() { stops++; stopOwner() }
	}
	screen := !strings.HasPrefix(mode, "prompt-") && !strings.HasPrefix(mode, "entry-")
	if screen {
		err = s.EnterScreen(ctx)
		if err == nil {
			err = s.Draw(ctx, []string{"QA dashboard", "Provisional union: 0s"})
		}
	}
	if err == nil {
		report("ready", "")
		err = qaExerciseUISession(ctx, cancel, s, mode, report)
	}
	if mode == "suspend-scope" {
		select {
		case <-peer:
		default:
			if err == nil {
				err = fmt.Errorf("terminal ownership displaced independent signal subscriber")
			}
		}
	}
	closeErr := s.Close()
	if mode == "write-failure" || mode == "entry-output-failure" {
		if closeErr == nil && err == nil {
			err = fmt.Errorf("failed cleanup output was not reported")
		}
	} else if closeErr != nil && mode != "blocked-output" && err == nil {
		err = fmt.Errorf("Close failed: %v", closeErr)
	}
	if again := s.Close(); !reflectQAUIError(closeErr, again) && err == nil {
		err = fmt.Errorf("Close was not idempotent")
	}
	if mode == "suspend-scope" && stops != 1 && err == nil {
		err = fmt.Errorf("owner subscription cleanup calls=%d, want exactly one", stops)
	}
	select {
	case <-s.stopped:
	default:
		if err == nil {
			err = fmt.Errorf("input owner survived Close")
		}
	}
	signal.Stop(signals)
	cancel(nil)
	<-joined
	if err != nil {
		report("error", err.Error())
	}
	report("closed", "")
	if mode == "suspend-scope" {
		// Peer preservation is paired with the owner-cleanup call assertion
		// above and source audit of its two scoped signal.Stop calls. A peer
		// alone cannot prove unsubscribe. Go 1.27.1 Darwin does not restore
		// kernel-default SIGTSTP stopping after Notify/Stop (review approved).
		select {
		case <-peer:
			report("peer-after-close", "")
		case <-time.After(time.Second):
			report("error", "Close removed independent SIGTSTP subscriber")
			err = fmt.Errorf("independent subscriber lost")
		}
		signal.Stop(peer)
	}
	if err != nil {
		os.Exit(62)
	}
	os.Exit(0)
}

func reflectQAUIError(a, b error) bool {
	return a == b || a != nil && b != nil && a.Error() == b.Error()
}

func qaExerciseUISession(ctx context.Context, cancel context.CancelCauseFunc, s *Session, mode string, report func(string, string)) error {
	next := func() (Event, error) {
		readCtx, stop := context.WithTimeout(ctx, 1500*time.Millisecond)
		defer stop()
		for {
			event, err := s.Next(readCtx)
			if err != nil || event.Kind != "" {
				return event, err
			}
		}
	}
	expect := func(kind, text string) error {
		e, err := next()
		if err != nil {
			return fmt.Errorf("wanted %s event, got %v", kind, err)
		}
		if e.Kind != kind || e.Text != text {
			return fmt.Errorf("wanted %s event with %d text bytes, got %s with %d", kind, len(text), e.Kind, len(e.Text))
		}
		return nil
	}
	expectExit := func(want int) error {
		_, err := next()
		var end *ExitError
		if !errors.As(err, &end) || end.Code != want {
			return fmt.Errorf("wanted terminal exit %d, got %v", want, err)
		}
		return nil
	}
	switch mode {
	case "escape-event":
		if err := expect("escape", ""); err != nil {
			return err
		}
		if s.Context().Err() != nil {
			return fmt.Errorf("Escape canceled dashboard owner")
		}
		report("escape", "")
		return expect("text", "q")
	case "keyboard":
		for _, e := range []Event{{Kind: "text", Text: "q"}, {Kind: "text", Text: "界"}, {Kind: "down"}, {Kind: "up"}, {Kind: "backspace"}, {Kind: "enter"}} {
			if err := expect(e.Kind, e.Text); err != nil {
				return err
			}
		}
		return nil
	case "paste":
		if err := expect("paste", "q\nConfirm\r"); err != nil {
			return err
		}
		report("paste", "")
		return expect("text", "z")
	case "paste-oversize":
		return expectExit(1)
	case "malformed-sequence", "incomplete-sequence", "incomplete-utf8":
		return expect("text", "q")
	case "resize":
		cols, rows, err := s.Size()
		if err != nil || cols != 80 || rows != 24 {
			return fmt.Errorf("initial terminal size=%dx%d err=%v", cols, rows, err)
		}
		report("sized", "")
		// Hold the consumer while the driver delivers a resize burst. Only
		// the latest dimensions should remain pending when it resumes.
		time.Sleep(150 * time.Millisecond)
		e, err := next()
		if err != nil || e.Kind != "resize" || e.Columns != 40 || e.Rows != 12 {
			return fmt.Errorf("resize event=%+v err=%v", e, err)
		}
		cols, rows, err = s.Size()
		if err != nil || cols != 40 || rows != 12 {
			return fmt.Errorf("resized terminal size=%dx%d err=%v", cols, rows, err)
		}
		quietCtx, stop := context.WithTimeout(ctx, 80*time.Millisecond)
		defer stop()
		if e, err := s.Next(quietCtx); !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("resize burst left queued events: kind=%s err=%v", e.Kind, err)
		}
		if s.Context().Err() != nil {
			return fmt.Errorf("bounded Next canceled terminal owner")
		}
		return nil
	case "cancel-cleanup":
		cancel(&ExitError{Code: 130})
		return expectExit(130)
	case "ctrlc", "sigint":
		return expectExit(130)
	case "sigterm":
		return expectExit(143)
	case "eof":
		return expectExit(0)
	case "read-failure":
		_ = s.in.Close()
		return expectExit(1)
	case "write-failure":
		_ = s.out.Close()
		err := s.Draw(ctx, []string{"after failure"})
		var end *ExitError
		if !errors.As(err, &end) || end.Code != 1 {
			return fmt.Errorf("Draw did not preserve output failure: %v", err)
		}
		return nil
	case "entry-output-failure", "entry-canceled":
		// Raw mode and the input pump are already acquired. A failed screen
		// acquisition must still unwind those independent resources.
		want := 1
		if mode == "entry-output-failure" {
			_ = s.out.Close()
		} else {
			want = 130
			cancel(&ExitError{Code: want})
		}
		err := s.EnterScreen(ctx)
		var end *ExitError
		if !errors.As(err, &end) || end.Code != want {
			return fmt.Errorf("failed screen entry lost cause %d: %v", want, err)
		}
		return nil
	case "blocked-output":
		// The PTY driver deliberately stops draining. A fresh cleanup context
		// must still allow Close to finish and restore observable termios.
		writeCtx, stop := context.WithTimeout(ctx, 300*time.Millisecond)
		defer stop()
		for i := 0; i < 100000; i++ {
			if err := s.Draw(writeCtx, []string{strings.Repeat("x", 79)}); err != nil {
				return nil
			}
		}
		return fmt.Errorf("bounded output test never reached backpressure")
	case "prompt-escape":
		_, err := s.Choose(ctx, "Choose", []Choice{{ID: "a", Label: "Alpha"}})
		var end *ExitError
		if !errors.As(err, &end) || end.Code != 0 {
			return fmt.Errorf("legacy Escape translation lost: %v", err)
		}
		return nil
	case "prompt-text-paste":
		text, err := s.Text(ctx, "QA_TEXT_PASTE", "")
		if err != nil || text != "alphabetaq" {
			return fmt.Errorf("pasted text was lost, split, or executed as input controls")
		}
		return nil
	case "prompt-secret-paste":
		secret, err := s.Secret(ctx, "QA_SECRET_PASTE")
		defer clear(secret)
		if err != nil || string(secret) != "synthetic-qa-secret" {
			return fmt.Errorf("pasted secret was lost, split, or executed as input controls")
		}
		return nil
	case "prompt-choose-paste":
		id, err := s.Choose(ctx, "QA_CHOOSE_PASTE", []Choice{{ID: "a", Label: "Alpha"}, {ID: "q", Label: "Quiet account"}})
		if err != nil || id != "q" {
			return fmt.Errorf("pasted search did not preserve printable text and explicit selection")
		}
		return nil
	case "prompt-confirm-paste":
		confirmed, err := s.Confirm(ctx, "QA_CONFIRM_PASTE")
		var end *ExitError
		if confirmed || !errors.As(err, &end) || end.Code != 0 {
			return fmt.Errorf("pasted confirmation text acted as consent or swallowed explicit Escape")
		}
		return nil
	case "prompt-secret-suspend":
		secret, err := s.Secret(ctx, "Secret")
		defer clear(secret)
		if err != nil || string(secret) != "synthetic-qa-secret" {
			return fmt.Errorf("suspend interrupted secret input or changed its bytes")
		}
		if s.Context().Err() != nil {
			return fmt.Errorf("suspend canceled secret owner")
		}
		return nil
	case "prompt-search-suspend":
		id, err := s.Choose(ctx, "Pick", []Choice{{ID: "q", Label: "Quiet account"}, {ID: "b", Label: "Beta account"}})
		if err != nil || id != "q" {
			return fmt.Errorf("suspend changed search selection or canceled picker")
		}
		return nil
	case "suspend-action":
		// A synthetic pending action remains live during suspension attempts.
		actionDone := make(chan error, 1)
		actionCtx, stop := context.WithTimeout(s.Context(), 400*time.Millisecond)
		defer stop()
		go func() { <-actionCtx.Done(); actionDone <- context.Cause(actionCtx) }()
		if err := <-actionDone; !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("suspend canceled pending action: %v", err)
		}
		return expect("text", "q")
	case "suspend-idle", "suspend-scope", "screen-cleanup":
		if err := expect("text", "q"); err != nil {
			return err
		}
		if s.Context().Err() != nil {
			return fmt.Errorf("suspend canceled terminal owner")
		}
		return nil
	}
	return fmt.Errorf("unknown synthetic mode")
}

func TestQAUIDashboardKernelPTY(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 required for actual PTY verification")
	}
	for _, mode := range []string{
		"screen-cleanup", "escape-event", "keyboard", "paste", "paste-oversize", "malformed-sequence", "incomplete-sequence", "incomplete-utf8", "resize",
		"cancel-cleanup", "ctrlc", "sigint", "sigterm", "eof", "read-failure", "write-failure", "blocked-output",
		"prompt-escape", "prompt-secret-suspend", "prompt-search-suspend", "suspend-idle", "suspend-action", "suspend-scope",
		"prompt-text-paste", "prompt-secret-paste", "prompt-choose-paste", "prompt-confirm-paste", "entry-output-failure", "entry-canceled",
	} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", qaUIDashboardPTYScript, os.Args[0], mode)
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("kernel PTY %s: %v\n%s", mode, err, output)
			}
		})
	}
}

const qaUIDashboardPTYScript = `
import os, sys, pty, termios, subprocess, select, time, signal, fcntl, struct, json
binary, mode=sys.argv[1:]
master, slave=pty.openpty()
fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,80,0,0))
before=termios.tcgetattr(slave)
file_flags=fcntl.fcntl(slave,fcntl.F_GETFL)
report_r,report_w=os.pipe()
env={'PATH':'/usr/bin:/bin','TEMPO_QA_UI_PTY_MODE':mode,'TEMPO_QA_UI_REPORT_FD':str(report_w)}
# A child process group with its parent outside that group makes SIGTSTP a
# real kernel stop if Tempo fails to consume it; orphan groups can ignore it.
p=subprocess.Popen([binary,'-test.run=^TestQAUIDashboardPTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(report_w,),preexec_fn=os.setpgrp)
os.close(report_w)
transcript=b''; pending=b''; reports=[]; failures=[]
def poll(timeout=.02, drain=True):
 global transcript,pending
 ready,_,_=select.select(([master] if drain else [])+[report_r],[],[],timeout)
 if master in ready:
  try: transcript+=os.read(master,65536)
  except OSError: pass
 if report_r in ready:
  pending+=os.read(report_r,65536)
  while b'\n' in pending:
   line,pending=pending.split(b'\n',1)
   if line: reports.append(json.loads(line))
def seen(kind): return any(r['kind']==kind for r in reports)
def until(kind,timeout=2,drain=True):
 deadline=time.monotonic()+timeout
 while not seen(kind) and time.monotonic()<deadline:
  poll(.02,drain)
  if seen('closed') and kind!='closed': break
 return seen(kind)
def send(payload):
 # Small chunks avoid blocking the test driver on an oversize rejected paste.
 for start in range(0,len(payload),512):
  if seen('closed'): break
  os.write(master,payload[start:start+512]);poll(.001)
def stopped():
 pid,status=os.waitpid(p.pid,os.WNOHANG|os.WUNTRACED)
 if pid and os.WIFSTOPPED(status): return True
 if pid and (os.WIFEXITED(status) or os.WIFSIGNALED(status)):
  p.returncode=os.waitstatus_to_exitcode(status)
 return False
try:
 if not until('ready'): raise AssertionError('session not ready: '+repr(reports))
 if mode.startswith('prompt-') and mode.endswith('-paste'):
  # A terminal only frames clipboard data when the application enables paste
  # mode. Verify this before injecting framed bytes; parser-only tests miss it.
  deadline=time.monotonic()+.2
  while time.monotonic()<deadline: poll(.01)
  if b'\x1b[?2004h' not in transcript: failures.append('standalone prompt did not enable bracketed paste before accepting input')
  payload={'prompt-text-paste':b'alpha\nbeta\rq\x03\x1a','prompt-secret-paste':b'synthetic-qa-\nsecret\r\x03\x1a','prompt-choose-paste':b'q\r\n\x03\x1a','prompt-confirm-paste':b'Confirm\r\n\x03\x1a'}[mode]
  send(b'\x1b[200~'+payload+b'\x1b[201~')
  deadline=time.monotonic()+.15
  while time.monotonic()<deadline: poll(.01)
  if seen('closed'): failures.append('pasted newline/control completed or canceled the prompt without an explicit key')
  else: send(b'\x1b' if mode=='prompt-confirm-paste' else b'\r')
 elif mode in ('suspend-idle','suspend-action','suspend-scope','prompt-secret-suspend','prompt-search-suspend'):
  if mode=='prompt-secret-suspend': send(b'synthetic-qa-')
  if mode=='prompt-search-suspend': send(b'q')
  send(b'\x1a')
  deadline=time.monotonic()+.08
  while time.monotonic()<deadline: poll(.01)
  if seen('closed'): failures.append('raw Ctrl-Z ended the session')
  p.send_signal(signal.SIGTSTP)
  deadline=time.monotonic()+.12
  while time.monotonic()<deadline:
   if stopped():
    failures.append('SIGTSTP stopped the interactive process')
    os.kill(p.pid,signal.SIGCONT);break
   poll(.01)
  if mode=='prompt-secret-suspend': send(b'secret\r')
  elif mode=='prompt-search-suspend': send(b'\r')
  else: send(b'q')
 elif mode=='escape-event':
  send(b'\x1b')
  if not until('escape'): failures.append('standalone Escape did not remain an event')
  else: send(b'q')
 elif mode=='keyboard': send('q界'.encode()+b'\x1b[B\x1b[A\x7f\r')
 elif mode=='paste':
  send(b'\x1b[200~q\nConfirm\r\x1b[201~')
  if not until('paste'): failures.append('bracketed paste did not yield one atomic payload')
  else: send(b'z')
 elif mode=='paste-oversize': send(b'\x1b[200~'+b'x'*16385+b'\x1b[201~')
 elif mode=='malformed-sequence': send(b'\x1b[99;1~q')
 elif mode in ('incomplete-sequence','incomplete-utf8'):
  send(b'\x1b[' if mode=='incomplete-sequence' else b'\xe2')
  deadline=time.monotonic()+.25
  while time.monotonic()<deadline: poll(.01)
  send(b'q')
 elif mode=='resize':
  if not until('sized'): failures.append('initial Size observation failed')
  fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',12,40,0,0))
  for _ in range(8): p.send_signal(signal.SIGWINCH)
 elif mode in ('ctrlc','eof','prompt-escape'): send({'ctrlc':b'\x03','eof':b'\x04','prompt-escape':b'\x1b'}[mode])
 elif mode in ('sigint','sigterm'): p.send_signal(signal.SIGINT if mode=='sigint' else signal.SIGTERM)
 elif mode=='screen-cleanup': send(b'q')
 if not until('closed',3,drain=mode!='blocked-output'): raise AssertionError('terminal owner/cleanup did not finish within 3s: '+repr(reports))
 if mode=='suspend-scope':
  p.send_signal(signal.SIGTSTP)
  deadline=time.monotonic()+1.5
  while not seen('peer-after-close') and time.monotonic()<deadline: poll(.01)
  if not seen('peer-after-close'): failures.append('independent SIGTSTP subscriber lost after Close')
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline: poll(.01)
 if p.poll() is None: raise AssertionError('owned work survived child completion')
 while select.select([master],[],[],.01)[0]:
  old=len(transcript);poll(.01)
  if len(transcript)==old: break
 after=termios.tcgetattr(slave)
 restored_flags=fcntl.fcntl(slave,fcntl.F_GETFL)
 # Preserve the existing #10 XNU allowances for kernel-maintained flags only.
 if sys.platform=='darwin':
  restored_flags &= ~0x10000;file_flags &= ~0x10000
  after[3] &= ~termios.PENDIN;before[3] &= ~termios.PENDIN
 if after!=before: failures.append('original termios not restored')
 if restored_flags!=file_flags: failures.append('original descriptor flags not restored')
 failures.extend(r['message'] for r in reports if r['kind']=='error')
 if p.returncode: failures.append('child exit '+str(p.returncode))
 if mode.startswith('prompt-') and mode.endswith('-paste'):
  if b'\x1b[?2004l' not in transcript: failures.append('standalone prompt did not disable bracketed paste on Close')
 if mode=='entry-canceled':
  if b'\x1b[?25h' not in transcript or b'\x1b[?1049l' not in transcript: failures.append('canceled screen acquisition skipped independent cleanup attempts')
 if not mode.startswith(('prompt-','entry-')) and mode!='blocked-output':
  for sequence,label in [(b'\x1b[?1049h','alternate screen entry'),(b'\x1b[?25l','cursor hide'),(b'\x1b[?2004h','bracketed paste enable')]:
   if sequence not in transcript: failures.append('missing '+label)
  if mode!='write-failure':
   for sequence,label in [(b'\x1b[0m','style reset'),(b'\x1b[?25h','cursor restore'),(b'\x1b[?1049l','alternate screen exit'),(b'\x1b[?2004l','bracketed paste disable')]:
    if sequence not in transcript: failures.append('missing '+label)
 if mode in ('prompt-secret-suspend','prompt-secret-paste') and (b'synthetic-qa-' in transcript or b'secret\r' in transcript): failures.append('secret bytes echoed')
 if failures: raise AssertionError('; '.join(failures))
finally:
 if p.poll() is None: os.kill(p.pid,signal.SIGCONT);p.kill();p.wait()
 os.close(report_r);os.close(master);os.close(slave)
`
