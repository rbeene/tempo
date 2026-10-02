//go:build darwin || linux

package terminal_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/rbeene/tempo/internal/terminal"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// Darwin FWASWRITTEN is a sticky kernel observation, not an F_SETFL option.
// XNU bsd/sys/fcntl.h defines it as0x10000 and excludes it from FCNTLFLAGS.
// All writable flags (including O_NONBLOCK) remain part of the comparison.
func qaDiagnosticMutableFlags(flags uintptr) uintptr {
	if runtime.GOOS == "darwin" {
		return flags &^ 0x10000
	}
	return flags
}

func TestQADiagnosticTrustedWritersAndCanceledContext(t *testing.T) {
	t.Run("buffer", func(t *testing.T) {
		var b bytes.Buffer
		data := []byte("safe credential effect unknown\n")
		n, e := terminal.WriteDiagnostic(context.Background(), &b, data)
		if e != nil || n != len(data) || !bytes.Equal(b.Bytes(), data) {
			t.Errorf("trusted diagnostic lost: n%d err%v bytes%q", n, e, b.Bytes())
		}
	})
	t.Run("pre-canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		cause := &terminal.ExitError{Code: 143}
		cancel(cause)
		var b bytes.Buffer
		n, e := terminal.WriteDiagnostic(ctx, &b, []byte("must not write"))
		if n != 0 || !errors.Is(e, cause) || b.Len() != 0 {
			t.Errorf("canceled diagnostic wrote orlostcause: n%d err%v bytes%q", n, e, b.Bytes())
		}
	})
	t.Run("file-flags", func(t *testing.T) {
		f, e := os.CreateTemp(t.TempDir(), "diagnostic")
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		fd := f.Fd()
		before, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
		if errno != 0 {
			t.Fatal(errno)
		}
		data := []byte("fixed safe restoration notice\n")
		n, e := terminal.WriteDiagnostic(context.Background(), f, data)
		after, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
		if e != nil || n != len(data) || errno != 0 || qaDiagnosticMutableFlags(before) != qaDiagnosticMutableFlags(after) {
			t.Errorf("diagnostic changed flags/lost bytes: %d %v flags%v/%v", n, e, before, after)
		}
		f.Seek(0, 0)
		got := make([]byte, len(data))
		read, _ := f.Read(got)
		if read != len(data) || !bytes.Equal(got, data) {
			t.Error("file diagnostic not exact")
		}
	})
	t.Run("broken-pipe", func(t *testing.T) {
		r, w, e := os.Pipe()
		if e != nil {
			t.Fatal(e)
		}
		r.Close()
		defer w.Close()
		n, e := terminal.WriteDiagnostic(context.Background(), w, []byte("safe"))
		if n != 0 || e == nil {
			t.Errorf("broken descriptor appeared successful: %d %v", n, e)
		}
	})
}
func TestQADiagnosticStoppedDrainPTYChild(t *testing.T) {
	if os.Getenv("TEMPO_QA_DIAGNOSTIC_CHILD") == "" {
		return
	}
	fd, e := strconv.Atoi(os.Getenv("TEMPO_QA_DIAGNOSTIC_REPORT_FD"))
	if e != nil {
		t.Fatal(e)
	}
	reportFile := os.NewFile(uintptr(fd), "diagnostic-report")
	report := func(k string, v any) { json.NewEncoder(reportFile).Encode(map[string]any{"kind": k, "value": v}) }
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	session, e := terminal.Open(ctx, os.Stdin, os.Stdout)
	if e != nil {
		t.Fatal(e)
	}
	if e = session.EnterScreen(ctx); e != nil {
		t.Fatal(e)
	}
	if e = session.Close(); e != nil {
		t.Fatal(e)
	}
	report("restored", true)
	outFD := os.Stdout.Fd()
	original, _, errno := syscall.Syscall(syscall.SYS_FCNTL, outFD, syscall.F_GETFL, 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	if e = syscall.SetNonblock(int(outFD), true); e != nil {
		t.Fatal(e)
	}
	padding := bytes.Repeat([]byte("X"), 4096)
	total, stalls := 0, 0
	deadline := time.Now().Add(time.Second)
	for stalls < 3 && time.Now().Before(deadline) {
		n, e := syscall.Write(int(outFD), padding)
		if n > 0 {
			total += n
		}
		if e == syscall.EAGAIN || e == syscall.EWOULDBLOCK {
			for time.Now().Before(deadline) {
				n, e := syscall.Write(int(outFD), []byte("X"))
				if n > 0 {
					total += n
				}
				if e == syscall.EAGAIN || e == syscall.EWOULDBLOCK {
					break
				}
				if e != nil {
					t.Fatal(e)
				}
			}
			stalls++
			time.Sleep(3 * time.Millisecond)
		} else if e != nil {
			t.Fatal(e)
		} else {
			stalls = 0
		}
	}
	_, _, errno = syscall.Syscall(syscall.SYS_FCNTL, outFD, syscall.F_SETFL, original)
	if errno != 0 || stalls != 3 || total == 0 {
		t.Fatalf("stopped-drain fixture did not saturate safely: flags%v stalls%d bytes%d", errno, stalls, total)
	}
	report("saturated", total)
	bounded, end := context.WithTimeout(context.Background(), 180*time.Millisecond)
	start := time.Now()
	n, e := terminal.WriteDiagnostic(bounded, os.Stdout, []byte("POST-CLOSE-DIAGNOSTIC-CANARY\n"))
	elapsed := time.Since(start)
	end()
	after, _, errno := syscall.Syscall(syscall.SYS_FCNTL, outFD, syscall.F_GETFL, 0)
	if e == nil || n != 0 || elapsed > 500*time.Millisecond || qaDiagnosticMutableFlags(after) != qaDiagnosticMutableFlags(original) || errno != 0 {
		t.Errorf("blocked diagnostic unbounded or changed restored flags: n%d err%v elapsed%v flags%v/%v", n, e, elapsed, original, after)
	}
	report("diagnostic-done", map[string]any{"n": n, "error": e != nil, "elapsed_ms": elapsed.Milliseconds(), "flags_restored": qaDiagnosticMutableFlags(after) == qaDiagnosticMutableFlags(original)})
	var release [1]byte
	if _, e = os.Stdin.Read(release[:]); e != nil {
		t.Error("parent did not release owned fixture")
	}
	report("closed", true)
}
func TestQADiagnosticActualPTYStoppedDrainAfterTerminalClose(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, "-c", qaDiagnosticPTYScript, os.Args[0])
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	if b, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("stopped-drain diagnostic: %v\n%s", e, b)
	}
}

const qaDiagnosticPTYScript = `
import os,sys,pty,termios,subprocess,select,time,fcntl,struct,json,signal
binary=sys.argv[1];master,slave=pty.openpty();fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,80,0,0))
before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL);r,w=os.pipe()
env={'PATH':'/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_DIAGNOSTIC_CHILD':'1','TEMPO_QA_DIAGNOSTIC_REPORT_FD':str(w)}
p=subprocess.Popen([binary,'-test.run=^TestQADiagnosticStoppedDrainPTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(w,),preexec_fn=os.setpgrp);os.close(w);reports=[];pending=b'';transcript=b''
def report_poll(timeout=.01):
 global pending
 if select.select([r],[],[],timeout)[0]:
  pending+=os.read(r,65536)
  while b'\n' in pending:
   line,pending=pending.split(b'\n',1)
   if line:reports.append(json.loads(line))
def kind(name):return [x for x in reports if x['kind']==name]
try:
 deadline=time.monotonic()+2
 while not kind('diagnostic-done') and time.monotonic()<deadline:report_poll()
 if not kind('restored') or not kind('saturated') or not kind('diagnostic-done'):raise AssertionError('post-Close diagnostic did not terminate while output stopped '+repr(reports))
 result=kind('diagnostic-done')[0]['value']
 # Only now resume draining; an abandoned writer must not emit late bytes.
 deadline=time.monotonic()+.2
 while time.monotonic()<deadline:
  if select.select([master],[],[],.01)[0]:
   try:transcript+=os.read(master,65536)
   except OSError:break
 if b'POST-CLOSE-DIAGNOSTIC-CANARY' in transcript:raise AssertionError('diagnostic wrote after its canceled budget')
 after=termios.tcgetattr(slave);after_flags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':after_flags&=~0x10000;flags&=~0x10000;after[3]&=~termios.PENDIN;before[3]&=~termios.PENDIN
 if after!=before or after_flags!=flags:raise AssertionError('post-Close output changed original terminal modes')
 if result['n']!=0 or not result['error'] or not result['flags_restored'] or result['elapsed_ms']>500:raise AssertionError('blocked diagnostic falsely succeeded '+repr(result))
 os.write(master,b'\n');deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:report_poll()
 if p.poll() is None:raise AssertionError('diagnostic fixture retained blocked work')
 while select.select([master],[],[],.01)[0]:
  try:transcript+=os.read(master,65536)
  except OSError:break
 if p.returncode or b'FAIL' in transcript:raise AssertionError('diagnostic child failed '+repr(transcript[-1000:]))
 for seq in (b'\x1b[?1049h',b'\x1b[?25l',b'\x1b[0m',b'\x1b[?25h',b'\x1b[?1049l'):
  if seq not in transcript:raise AssertionError('missing actual restoration attempt '+repr(seq))
 print('bounded post-Close diagnostic with stopped drain and no late writer verified')
finally:
 if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait()
 os.close(r);os.close(master);os.close(slave)
`
