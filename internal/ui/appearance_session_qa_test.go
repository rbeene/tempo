package ui_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
)

type qaAppearanceSecretGuard struct {
	style  terminal.Styler
	leaked bool
}

func (s *qaAppearanceSecretGuard) Paint(role terminal.Role, text string) string {
	if strings.Contains(text, "SYNTHETIC_PRIVATE_VALUE") {
		s.leaked = true
	}
	return s.style.Paint(role, text)
}

func TestQAAppearanceFiniteSessionChild(t *testing.T) {
	color := os.Getenv("TEMPO_QA_SESSION_COLORS")
	if color == "" {
		return
	}
	fd, err := strconv.Atoi(os.Getenv("TEMPO_QA_SESSION_REPORT_FD"))
	if err != nil {
		os.Exit(91)
	}
	reportFile := os.NewFile(uintptr(fd), "session-theme-report")
	report := func(kind, message string) {
		_ = json.NewEncoder(reportFile).Encode(map[string]string{"kind": kind, "message": message})
	}
	caps := themes.Capabilities{OutputTTY: true, Term: "xterm-256color"}
	switch color {
	case "truecolor":
		caps.ColorTerm = "truecolor"
	case "ansi16":
		caps.Term = "ansi"
	case "none":
		caps.NoColor = "1"
	}
	style, err := themes.NewStyler(os.Getenv("TEMPO_QA_SESSION_THEME"), caps)
	if err != nil {
		report("error", "invalid fixture style")
		os.Exit(92)
	}
	guard := &qaAppearanceSecretGuard{style: style}
	s, err := terminal.Open(context.Background(), os.Stdin, os.Stdout)
	if err != nil {
		report("error", "synthetic PTY open failed")
		os.Exit(93)
	}
	s.SetStyler(guard)
	mode := os.Getenv("TEMPO_QA_SESSION_MODE")
	id, err := s.Choose(s.Context(), "Palette choose", []terminal.Choice{{ID: "a", Label: "Alpha project"}, {ID: "b", Label: "Beta project"}})
	if mode == "cancel" {
		var ended *terminal.ExitError
		if !errors.As(err, &ended) || ended.Code != 0 {
			report("error", "finite Escape lost clean cancellation")
		}
	} else if err != nil || id != "b" {
		report("error", "finite choice changed selected ID")
	}
	if mode == "exercise" {
		text, err := s.Text(s.Context(), "Palette text", "draft")
		if err != nil || text != "draft" {
			report("error", "styled text altered default value")
		}
		secret, err := s.Secret(s.Context(), "Palette secret")
		if err != nil || string(secret) != "SYNTHETIC_PRIVATE_VALUE" {
			report("error", "styled secret altered synthetic input")
		}
		clear(secret)
		confirmed, err := s.Confirm(s.Context(), "Palette confirmation")
		if err != nil || !confirmed {
			report("error", "styled confirmation altered shared consent")
		}
	}
	if guard.leaked {
		report("error", "secret passed through styling")
	}
	if err := s.Close(); err != nil {
		report("error", "finite restoration failed")
	}
	report("closed", "")
	os.Exit(0)
}

func TestQAAppearanceActualFiniteSessionStylingAndReset(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	for _, color := range []string{"truecolor", "ansi256", "ansi16", "none"} {
		for _, theme := range []string{"terminal-default", "tokyo-night", "gruvbox", "catppuccin"} {
			t.Run(color+"/"+theme, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, python, "-c", qaAppearanceSessionPTYScript, os.Args[0], color, theme, "exercise")
				cmd.Env = []string{"PATH=/usr/bin:/bin"}
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("finite Session actual theme PTY: %v\n%s", err, output)
				}
			})
		}
	}
}

func TestQAAppearanceFiniteSessionEscapeRestoresStylesAndTerminal(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, "-c", qaAppearanceSessionPTYScript, os.Args[0], "truecolor", "tokyo-night", "cancel")
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("finite Session cancel actual PTY: %v\n%s", err, output)
	}
}

const qaAppearanceSessionPTYScript = `
import os,sys,pty,termios,subprocess,select,time,fcntl,struct,json,re
binary,color,theme,mode=sys.argv[1:]
master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,80,0,0))
before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL)
rr,rw=os.pipe();env={'PATH':'/usr/bin:/bin','GOMAXPROCS':'2','GORACE':'atexit_sleep_ms=0','TEMPO_QA_SESSION_COLORS':color,'TEMPO_QA_SESSION_THEME':theme,'TEMPO_QA_SESSION_MODE':mode,'TEMPO_QA_SESSION_REPORT_FD':str(rw)}
p=subprocess.Popen([binary,'-test.run=^TestQAAppearanceFiniteSessionChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(rw,),preexec_fn=os.setpgrp)
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
def until(pred):
 deadline=time.monotonic()+2
 while not pred() and time.monotonic()<deadline:
  poll()
  if closed() and not pred():break
 if not pred():raise AssertionError('missing finite prompt behavior: '+repr(transcript[-500:])+' '+repr(reports))
def styled_title(title):
 until(lambda:title in transcript)
 if color=='none' or theme=='terminal-default':
  if sgr.search(transcript):raise AssertionError('default/no-color finite content styled')
 else:
  matches=[line for line in transcript.split(b'\r\n') if title in line]
  if not matches or not any(sgr.search(line) for line in matches):raise AssertionError('actual finite prompt title lacks semantic styling')
try:
 styled_title(b'Palette choose')
 if mode=='cancel':os.write(master,b'\x1b')
 else:
  os.write(master,b'\x1b[B\r');styled_title(b'Palette text');os.write(master,b'\r')
  styled_title(b'Palette secret');os.write(master,b'SYNTHETIC_PRIVATE_VALUE\r')
  styled_title(b'Palette confirmation');os.write(master,b'\x1b[B\r')
 until(closed)
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll(.01)
 for _ in range(3):poll(.01)
 if p.poll() is None:raise AssertionError('finite styled Session retained raw pump')
 if b'SYNTHETIC_PRIVATE_VALUE' in transcript:raise AssertionError('secret echoed into real output')
 if color!='none' and theme!='terminal-default' and b'\x1b[0m\x1b[?2004l' not in transcript:raise AssertionError('finite Close did not reset final palette')
 if b'\x1b[?1049h' in transcript:raise AssertionError('finite prompt unexpectedly opened dashboard screen')
 after=termios.tcgetattr(slave);af=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':af &= ~0x10000;flags &= ~0x10000;after[3] &= ~termios.PENDIN;before[3] &= ~termios.PENDIN
 if after!=before or af!=flags:raise AssertionError('finite styled Session failed termios/descriptor restoration')
 errors=[r['message'] for r in reports if r['kind']=='error']
 if errors or p.returncode:raise AssertionError('finite theme errors '+repr(errors)+' exit '+str(p.returncode))
 print('finite shared prompt styles, synthetic secrets, reset, actual restoration verified '+color+' '+theme+' '+mode)
finally:
 if p.poll() is None:p.kill();p.wait()
 os.close(rr);os.close(master);os.close(slave)
`
