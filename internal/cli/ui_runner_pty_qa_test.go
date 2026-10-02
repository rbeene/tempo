package cli_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/terminal"
)

// Exercise actual cli.Run on actual TTYs. All backend construction comes from
// the existing synthetic fixture, whose cleanup verifies no state/auth/worker
// effects and untouched malformed account config.
func TestQAUIRunnerCLIChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_CLI_RUNNER_MODE")
	if mode == "" {
		return
	}
	fd, err := strconv.Atoi(os.Getenv("TEMPO_QA_CLI_RUNNER_REPORT_FD"))
	if err != nil {
		t.Fatal(err)
	}
	reportFile := os.NewFile(uintptr(fd), "cli-runner-report")
	report := func(kind string, value any) {
		_ = json.NewEncoder(reportFile).Encode(map[string]any{"kind": kind, "value": value})
	}
	var args []string
	switch mode {
	case "default", "default-ctrlc":
	case "ui", "stdin-pipe", "stdout-pipe", "read-navigation":
		args = []string{"ui"}
	case "watch":
		args = []string{"activity", "status", "--watch"}
	case "json":
		args = []string{"ui", "--json"}
	case "noninteractive":
		args = []string{"--non-interactive"}
	default:
		t.Fatal("unknown synthetic mode")
	}
	live := mode == "default" || mode == "ui" || mode == "watch" || mode == "default-ctrlc" || mode == "read-navigation"
	if eligible := cli.InteractiveInvocation(args, os.Stdin, os.Stdout); eligible != live {
		report("error", "incorrect interactive deadline classification")
	}
	f := qaNewUIModeFixture(t)
	if mode == "read-navigation" {
		f.deps.Worker = nil
		f.deps.Hooks = nil
		if err := os.Chdir(filepath.Dir(f.deps.ConfigPath)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
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
	code := cli.Run(ctx, args, os.Stdin, os.Stdout, os.Stderr, f.deps)
	cancel(nil)
	<-joined
	want := 0
	if mode == "default-ctrlc" {
		want = 130
	}
	if code != want {
		report("error", "incorrect CLI exit code "+strconv.Itoa(code))
	}
	report("closed", code)
}

func TestQAUIRunnerCLIActualModesAndLifetime(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("python3 required for actual CLI PTY verification")
	}
	for _, mode := range []string{"default", "ui", "watch", "default-ctrlc", "json", "noninteractive", "stdin-pipe", "stdout-pipe", "read-navigation"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "-c", qaUIRunnerCLIPTYScript, os.Args[0], mode)
			cmd.Env = []string{"PATH=/usr/bin:/bin"}
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("CLI kernel PTY %s: %v\n%s", mode, err, output)
			}
		})
	}
}

const qaUIRunnerCLIPTYScript = `
import os,sys,pty,termios,subprocess,select,time,fcntl,struct,json
binary,mode=sys.argv[1:]
master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,80,0,0))
before=termios.tcgetattr(slave); flags=fcntl.fcntl(slave,fcntl.F_GETFL)
report_r,report_w=os.pipe()
env={'PATH':'/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_CLI_RUNNER_MODE':mode,'TEMPO_QA_CLI_RUNNER_REPORT_FD':str(report_w)}
stdin=subprocess.PIPE if mode=='stdin-pipe' else slave
stdout=subprocess.PIPE if mode=='stdout-pipe' else slave
p=subprocess.Popen([binary,'-test.run=^TestQAUIRunnerCLIChild$'],stdin=stdin,stdout=stdout,stderr=slave,env=env,pass_fds=(report_w,),preexec_fn=os.setpgrp)
os.close(report_w)
transcript=b'';pipe_output=b'';pending=b'';reports=[]
def poll(timeout=.02):
 global transcript,pipe_output,pending
 streams=[master,report_r]+([p.stdout] if p.stdout is not None else [])
 ready,_,_=select.select(streams,[],[],timeout)
 if master in ready:
  try: transcript+=os.read(master,65536)
  except OSError: pass
 if p.stdout is not None and p.stdout in ready:pipe_output+=os.read(p.stdout.fileno(),65536)
 if report_r in ready:
  pending+=os.read(report_r,65536)
  while b'\n' in pending:
   line,pending=pending.split(b'\n',1)
   if line:reports.append(json.loads(line))
def closed():return any(r['kind']=='closed' for r in reports)
def until(predicate,seconds=2):
 deadline=time.monotonic()+seconds
 while not predicate() and time.monotonic()<deadline:
  poll()
  if closed() and not predicate():break
 if not predicate():raise AssertionError('CLI behavior missing: '+repr(reports)+' '+repr(transcript[-600:]))
def last_frame_has(word):
 frame=transcript.split(b'\x1b[H\x1b[J')[-1]
 return word in frame.split(b'\r\n')[0]
live=mode in ('default','ui','watch','default-ctrlc','read-navigation')
try:
 if live:
  until(lambda:b'TEMPO' in transcript and b'No activity yet' in transcript)
  if b'\x1b[?1049h' not in transcript:raise AssertionError('CLI dashboard did not enter actual screen')
  if mode=='read-navigation':
   os.write(master,b'?');until(lambda:last_frame_has(b'Help'))
   os.write(master,b'q')
   deadline=time.monotonic()+.06
   while time.monotonic()<deadline:poll(.01)
   if closed():raise AssertionError('q inside CLI readonly modal quit dashboard')
   os.write(master,b'\x1b');until(lambda:last_frame_has(b'TEMPO'))
   for key,title in ((b'2',b'Links'),(b'3',b'Sync'),(b',',b'Setup')):
    os.write(master,key);until(lambda:last_frame_has(title))
    os.write(master,b'\x1b');until(lambda:last_frame_has(b'TEMPO'))
   os.write(master,b'q')
  else:os.write(master,b'\x03' if mode=='default-ctrlc' else b'q')
 until(closed)
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll(.01)
 if p.poll() is None:raise AssertionError('CLI terminal owner/work survived exit')
 for _ in range(5):poll(.01)
 errors=[r['value'] for r in reports if r['kind']=='error']
 if errors or p.returncode:raise AssertionError('CLI errors '+repr(errors)+' subprocess '+str(p.returncode)+' '+repr(transcript[-600:]))
 after=termios.tcgetattr(slave);after_flags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':
  after_flags &= ~0x10000; flags &= ~0x10000
  after[3] &= ~termios.PENDIN; before[3] &= ~termios.PENDIN
 if after!=before or after_flags!=flags:raise AssertionError('CLI did not restore actual termios/flags')
 if live:
  for seq in (b'\x1b[0m',b'\x1b[?25h',b'\x1b[?1049l',b'\x1b[?2004l'):
   if seq not in transcript:raise AssertionError('CLI missed screen cleanup '+repr(seq))
  if b'"schema_version"' in transcript:raise AssertionError('interactive CLI printed a trailing machine envelope')
 else:
  output=pipe_output if mode=='stdout-pipe' else transcript
  if b'\x1b' in output:raise AssertionError('forced/redirected mode opened terminal screen')
  lines=[line for line in output.splitlines() if line.startswith(b'{')]
  if len(lines)!=1:raise AssertionError('finite CLI did not return exactly one envelope: '+repr(output))
  envelope=json.loads(lines[0]);data=envelope.get('data',{})
  if envelope.get('schema_version')!=1 or data.get('snapshot_revision')!='0' or data.get('computer_id') is not None:raise AssertionError('finite CLI output not shared empty Status')
 print('CLI actual modes/lifetime verified: '+mode)
finally:
 if p.poll() is None:p.kill();p.wait()
 os.close(report_r);os.close(master);os.close(slave)
`
