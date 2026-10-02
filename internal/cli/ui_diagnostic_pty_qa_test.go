//go:build darwin || linux

package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/terminal"
)

// Mask only Darwin's non-restorable kernel FWASWRITTEN observation.
func qaUIDiagnosticMutableFlags(flags uintptr) uintptr {
	if runtime.GOOS == "darwin" {
		return flags &^ 0x10000
	}
	return flags
}

func TestQAUIDiagnosticCLIStoppedDrainChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_DIAGNOSTIC_CLI_MODE")
	if mode == "" {
		return
	}
	root := os.Getenv("TEMPO_QA_DIAGNOSTIC_CLI_ROOT")
	fd, e := strconv.Atoi(os.Getenv("TEMPO_QA_DIAGNOSTIC_CLI_REPORT_FD"))
	if e != nil || !filepath.IsAbs(root) {
		t.Fatal("invalid synthetic fixture")
	}
	reportFile := os.NewFile(uintptr(fd), "diagnostic-cli-report")
	report := func(k string, v any) { json.NewEncoder(reportFile).Encode(map[string]any{"kind": k, "value": v}) }
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	expectedFlags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, os.Stderr.Fd(), syscall.F_GETFL, 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	var diagnosticStart time.Time
	credentials := auth.NewService(auth.Options{ConfigPath: filepath.Join(root, "config"), LockPath: filepath.Join(root, "lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, NewProvider: func(string, string) harvest.Provider { return qaCLIAuthProvider{} }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		// Saturate a separate error PTY while the dashboard's output continues to drain.
		// This isolates the post-Close diagnostic from terminal Draw/Close budgets.
		if e := syscall.SetNonblock(int(os.Stderr.Fd()), true); e != nil {
			t.Fatal(e)
		}
		padding := bytes.Repeat([]byte("X"), 4096)
		stalls, total := 0, 0
		deadline := time.Now().Add(time.Second)
		for stalls < 3 && time.Now().Before(deadline) {
			n, e := syscall.Write(int(os.Stderr.Fd()), padding)
			if n > 0 {
				total += n
			}
			if errors.Is(e, syscall.EAGAIN) || errors.Is(e, syscall.EWOULDBLOCK) {
				for time.Now().Before(deadline) {
					n, e = syscall.Write(int(os.Stderr.Fd()), []byte("X"))
					if n > 0 {
						total += n
					}
					if errors.Is(e, syscall.EAGAIN) || errors.Is(e, syscall.EWOULDBLOCK) {
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
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, os.Stderr.Fd(), syscall.F_SETFL, expectedFlags)
		if errno != 0 || stalls != 3 || total == 0 {
			t.Fatal("error PTY did not saturate")
		}
		diagnosticStart = time.Now()
		report("saturated", total)
		cancel(&terminal.ExitError{Code: 143})
		if mode == "unknown" {
			return auth.NativeReply{Code: "credential_write_unknown", Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}, nil
		}
		return auth.NativeReply{Effects: auth.Effects{Credential: "applied", Config: "saved"}}, nil
	})})
	epoch, elapsed := "diagnostic-cli-epoch", "0"
	local := activity.New(activity.Options{Path: filepath.Join(root, "absent-state", "activity.json"), Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
		return activity.ClockSample{Capability: "available", WallUTC: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC), Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &elapsed}, nil
	})})
	code := cli.Run(ctx, []string{"ui"}, os.Stdin, os.Stdout, os.Stderr, cli.Dependencies{Activity: local, Auth: credentials, ConfigPath: filepath.Join(root, "config"), Getenv: func(string) string { return "" }})
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, os.Stderr.Fd(), syscall.F_GETFL, 0)
	want := 143
	if mode == "unknown" {
		want = 8
	}
	if code != want || qaUIDiagnosticMutableFlags(flags) != qaUIDiagnosticMutableFlags(expectedFlags) || errno != 0 {
		t.Errorf("diagnostic lost primary or changed flags: %d/%d flags%d/%d", code, want, flags, expectedFlags)
	}
	if _, e := os.Stat(filepath.Join(root, "absent-state")); !errors.Is(e, os.ErrNotExist) {
		t.Error("diagnostic initialized activity state")
	}
	report("closed", map[string]any{"code": code, "elapsed_ms": time.Since(diagnosticStart).Milliseconds(), "flags_restored": qaUIDiagnosticMutableFlags(flags) == qaUIDiagnosticMutableFlags(expectedFlags)})
}

func TestQAUIDiagnosticCLIActualStoppedDrainKeepsPrimaryAndJoins(t *testing.T) {
	python, e := exec.LookPath("python3")
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"known", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, python, "-c", qaUIDiagnosticCLIStoppedDrainScript, os.Args[0], mode, t.TempDir())
			command.Env = []string{"PATH=/usr/bin:/bin"}
			if b, e := command.CombinedOutput(); e != nil {
				t.Fatalf("opened UI blocked on post-Close diagnostic: %v\n%s", e, b)
			}
		})
	}
}

const qaUIDiagnosticCLIStoppedDrainScript = `
import os,sys,pty,termios,subprocess,select,time,fcntl,struct,json,signal
binary,mode,root=sys.argv[1:];master,slave=pty.openpty();errmaster,errslave=pty.openpty()
fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0));before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL);eflags=fcntl.fcntl(errslave,fcntl.F_GETFL);r,w=os.pipe()
env={'PATH':'/usr/bin:/bin','GORACE':'atexit_sleep_ms=0','TEMPO_QA_DIAGNOSTIC_CLI_MODE':mode,'TEMPO_QA_DIAGNOSTIC_CLI_ROOT':root,'TEMPO_QA_DIAGNOSTIC_CLI_REPORT_FD':str(w)}
p=subprocess.Popen([binary,'-test.run=^TestQAUIDiagnosticCLIStoppedDrainChild$'],stdin=slave,stdout=slave,stderr=errslave,env=env,pass_fds=(w,),preexec_fn=os.setpgrp);os.close(w);pending=b'';transcript=b'';reports=[];errbytes=b''
def poll(timeout=.01):
 global pending,transcript
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
def header(text):return text in transcript.split(b'\x1b[H\x1b[J')[-1].split(b'\r\n')[0]
def until(pred,seconds=2):
 deadline=time.monotonic()+seconds
 while not pred() and time.monotonic()<deadline:poll()
 if not pred():raise AssertionError('missing CLI fixture behavior '+repr(reports)+' '+repr(transcript[-800:]))
def key(data):os.write(master,data)
try:
 until(lambda:header(b'TEMPO'));key(b'a');until(lambda:header(b'Accounts and auth'));key(b'login\r');until(lambda:header(b'Harvest personal access token'))
 key(b'\x1b[200~synthetic-ui-auth-secret\x1b[201~\r');until(lambda:header(b'Choose a Harvest account'));key(b'11\r');until(lambda:b'Replace the saved credential' in transcript.split(b'\x1b[H\x1b[J')[-1]);key(b'y\r')
 until(lambda:bool(kind('saturated')))
 until(lambda:bool(kind('closed')),1.2)
 result=kind('closed')[0]['value']
 if result['code']!=(8 if mode=='unknown' else 143) or not result['flags_restored'] or result['elapsed_ms']>650:raise AssertionError('unbounded/corrupt primary diagnostic '+repr(result))
 if b'\x1b[?1049l' not in transcript:raise AssertionError('CLI diagnostic ran before actual Close')
 # Resume error output only after the entire Run returned. No abandoned report is allowed.
 deadline=time.monotonic()+.2
 while time.monotonic()<deadline:
  poll()
  if select.select([errmaster],[],[],.005)[0]:
   try:errbytes+=os.read(errmaster,65536)
   except OSError:break
 if b'tempo:' in errbytes:raise AssertionError('post-budget diagnostic appeared after Run returned')
 if b'synthetic-ui-auth-secret' in transcript+errbytes:raise AssertionError('diagnostic leaked secret')
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll()
 if p.poll() is None:raise AssertionError('CLI diagnostic left work after completion')
 after=termios.tcgetattr(slave);afterflags=fcntl.fcntl(slave,fcntl.F_GETFL)
 currenteflags=fcntl.fcntl(errslave,fcntl.F_GETFL)
 if sys.platform=='darwin':afterflags&=~0x10000;flags&=~0x10000;currenteflags&=~0x10000;eflags&=~0x10000;after[3]&=~termios.PENDIN;before[3]&=~termios.PENDIN
 if after!=before or afterflags!=flags or currenteflags!=eflags:raise AssertionError('CLI diagnostic changed restored descriptor state')
 if p.returncode or b'FAIL' in transcript+errbytes:raise AssertionError('CLI child failed '+repr((transcript+errbytes)[-800:]))
 print('opened CLI diagnostic bounded across all post-Close reports: '+mode)
finally:
 if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait()
 for fd in (r,master,slave,errmaster,errslave):os.close(fd)
`
