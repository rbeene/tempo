package terminal

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestQATerminalSanitizeRetainsTextRemovesControls(t *testing.T) {
	s := Sanitize("Café 測試\x1b[31mRED\x1b[0m\r\n\x00\x7f\u009bq")
	if !strings.Contains(s, "Café 測試") || !strings.Contains(s, "q") {
		t.Fatalf("useful label text lost %q", s)
	}
	if strings.ContainsAny(s, "\x1b\r\n\x00\x7f\u009b") {
		t.Fatalf("terminal control injection survived %q", s)
	}
}
func TestQATerminalBuffersAreNotTTY(t *testing.T) {
	if Eligible(strings.NewReader(""), &bytes.Buffer{}) {
		t.Fatal("non-file streams marked terminal")
	}
}
func TestQATerminalPTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_PTY_MODE")
	if mode == "" {
		return
	}
	if !Eligible(os.Stdin, os.Stdout) {
		fmt.Fprintln(os.Stderr, "not both TTY")
		os.Exit(30)
	}
	if Eligible(strings.NewReader(""), os.Stdout) || Eligible(os.Stdin, &bytes.Buffer{}) {
		os.Exit(31)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if mode == "sigint" || mode == "sigterm" {
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer stop()
	}
	s, e := Open(ctx, os.Stdin, os.Stdout)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(32)
	}
	code := 0
	switch mode {
	case "secret", "oversize":
		var secret []byte
		secret, e = s.Secret(ctx, "QA_SECRET_READY")
		if mode == "secret" && string(secret) != "synthetic-qa-secret" {
			code = 33
		}
		if mode == "oversize" && e == nil {
			code = 34
		}
	case "action-ctrlc":
		fmt.Fprintln(os.Stdout, "QA_ACTION_READY")
		<-s.Context().Done()
		var interrupted *ExitError
		if !errors.As(context.Cause(s.Context()), &interrupted) || interrupted.Code != 130 {
			code = 42
		}
	case "read-failure", "render-failure":
		fmt.Fprintln(os.Stdout, "QA_FAULT_READY")
		if mode == "read-failure" {
			s.in.Close()
		} else {
			s.out.Close()
		}
		_, e = s.Text(ctx, "fault", "")
		if e == nil {
			code = 40
		}
	case "sigint", "sigterm", "deadline":
		_, e = s.Text(ctx, "QA_TEXT_READY", "")
		if e == nil {
			code = 35
		}
	default:
		var chosen string
		chosen, e = s.Choose(ctx, "QA_PICK_READY", []Choice{{ID: "a", Label: "Alpha account 11"}, {ID: "b", Label: "Quiet Café account 11"}, {ID: "c", Label: "Quiet Zebra account 11"}})
		if mode == "choose" && (e != nil || chosen != "c") {
			code = 36
		}
		if mode == "escape" || mode == "ctrlc" || mode == "eof" {
			var ee *ExitError
			if !errors.As(e, &ee) {
				code = 37
			} else {
				want := 0
				if mode == "ctrlc" {
					want = 130
				}
				if ee.Code != want {
					code = 38
				}
			}
		}
	}
	if e = s.Close(); e != nil {
		code = 39
	}
	fmt.Fprintln(os.Stdout, "QA_CLOSED")
	if mode == "stream-reuse" {
		line, e := bufio.NewReader(os.Stdin).ReadString('\n')
		if e != nil || line != "after-close\n" {
			code = 41
		}
	}
	os.Exit(code)
}
func TestQARealPTYSelectionSecretsCancellationRestore(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal("python3 required for actual PTY verification")
	}
	for _, mode := range []string{"choose", "secret", "oversize", "escape", "ctrlc", "eof", "deadline", "sigint", "sigterm", "read-failure", "render-failure", "stream-reuse", "action-ctrlc"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", qaPTYScript, os.Args[0], mode)
			out, e := cmd.CombinedOutput()
			if e != nil {
				t.Fatalf("actual PTY %s: %v\n%s", mode, e, out)
			}
		})
	}
}

const qaPTYScript = `
import os, sys, pty, termios, subprocess, select, time, signal, fcntl
binary, mode=sys.argv[1:]
master, slave=pty.openpty()
before=termios.tcgetattr(slave)
file_flags=fcntl.fcntl(slave,fcntl.F_GETFL)
env=dict(os.environ, TEMPO_QA_PTY_MODE=mode)
p=subprocess.Popen([binary,'-test.run=^TestQATerminalPTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,close_fds=True)
transcript=b''
def drain(timeout):
 global transcript
 ready,_,_=select.select([master],[],[],timeout)
 if ready:
  try: transcript+=os.read(master,65536)
  except OSError: pass
try:
 deadline=time.monotonic()+3
 while b'QA_' not in transcript and p.poll() is None and time.monotonic()<deadline: drain(.03)
 if b'QA_' not in transcript: raise AssertionError('prompt absent: '+repr(transcript))
 payload={'choose':b'zzzz\rq\x1b[B\r','secret':b'synthetic-qa-secret\r','oversize':b'x'*16385+b'\r','escape':b'\x1b','ctrlc':b'\x03','eof':b'\x04','deadline':b'','sigint':b'','sigterm':b'','read-failure':b'','render-failure':b'','stream-reuse':b'q\r','action-ctrlc':b'\x03'}[mode]
 # Clear a no-match filter before searching q; no-match Enter must be inert.
 if mode=='choose': payload=b'zzzz\r'+b'\x7f'*4+b'q\x1b[B\r'
 if mode in ("sigint","sigterm"): p.send_signal(signal.SIGINT if mode=="sigint" else signal.SIGTERM)
 if payload: os.write(master,payload)
 if mode=='stream-reuse':
  deadline=time.monotonic()+2
  while b'QA_CLOSED' not in transcript and time.monotonic()<deadline: drain(.01)
  if b'QA_CLOSED' not in transcript: raise AssertionError('no stream reuse cleanup marker')
  os.write(master,b'after-close');time.sleep(.04)
  if p.poll() is not None: raise AssertionError('original input did not wait for canonical newline')
  os.write(master,b'\n')
 deadline=time.monotonic()+4
 while p.poll() is None and time.monotonic()<deadline: drain(.03)
 if p.poll() is None: raise AssertionError('terminal operation did not end')
 drain(.01)
 if p.returncode: raise AssertionError('child exit '+str(p.returncode)+': '+repr(transcript))
 restored_flags=fcntl.fcntl(slave,fcntl.F_GETFL)
 # Darwin F_GETFL exposes internal FWASWRITTEN, which F_SETFL cannot clear.
 # https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/fcntl.h
 if sys.platform=='darwin':
  restored_flags &= ~0x10000
  file_flags &= ~0x10000
 if restored_flags!=file_flags: raise AssertionError("original stream status flags not restored")
 after=termios.tcgetattr(slave)
 # XNU tty.c sets PENDIN when canonical mode resumes; this is transient
 # kernel pending-input state, not a requested terminal setting.
 # https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/tty.c
 if sys.platform=='darwin':
  after[3] &= ~termios.PENDIN
  before[3] &= ~termios.PENDIN
 if after!=before: raise AssertionError('terminal attributes not restored: before='+repr(before)+' after='+repr(after))
 if mode=='stream-reuse' and b'after-close' not in transcript: raise AssertionError('echo not restored')
 if mode=='secret' and b'synthetic-qa-secret' in transcript: raise AssertionError('secret echoed')
 if b'QA_CLOSED' not in transcript: raise AssertionError('cleanup marker missing')
finally:
 if p.poll() is None: p.kill(); p.wait()
 os.close(master); os.close(slave)
`
