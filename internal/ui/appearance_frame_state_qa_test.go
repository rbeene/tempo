package ui_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
	"github.com/rbeene/tempo/internal/ui"
)

// This helper uses the production terminal owner. Frame reports synchronize
// input only: the assertions interpret the actual PTY output, carrying SGR
// state across every emitted frame rather than inspecting renderer strings.
func TestQAAppearanceFrameStatePTYChild(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_FRAME_STATE_MODE")
	if mode == "" {
		return
	}
	fd, err := strconv.Atoi(os.Getenv("TEMPO_QA_FRAME_STATE_FD"))
	if err != nil {
		os.Exit(81)
	}
	reportFile := os.NewFile(uintptr(fd), "frame-state-report")
	report := func(kind, message string) {
		_ = json.NewEncoder(reportFile).Encode(map[string]string{"kind": kind, "message": message})
	}
	root, candidate := os.Getenv("TEMPO_QA_FRAME_STATE_ROOT"), os.Getenv("TEMPO_QA_FRAME_STATE_THEME")
	if !filepath.IsAbs(root) {
		os.Exit(82)
	}
	path := filepath.Join(root, "preferences.json")
	caps := themes.Capabilities{OutputTTY: true, Term: "xterm-256color"}
	switch os.Getenv("TEMPO_QA_FRAME_STATE_COLOR") {
	case "truecolor":
		caps.ColorTerm = "truecolor"
	case "ansi16":
		caps.Term = "ansi"
	case "none":
		caps.NoColor = "1"
	case "unknown":
		caps.Term = "unknown-terminal"
	}
	service := qaAppearanceRunService(path, nil)
	if mode == "fallback" {
		if _, err := service.Set(context.Background(), themes.SetInput{Theme: candidate, IfRevision: "0", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}); err != nil {
			os.Exit(83)
		}
		service = qaAppearanceRunService(path, func(stage string) error {
			if stage == "directory_sync" {
				return errors.New("PRIVATE synthetic durability")
			}
			return nil
		})
	}
	session, err := terminal.Open(context.Background(), os.Stdin, os.Stdout)
	if err == nil {
		reader := activity.New(activity.Options{Path: filepath.Join(root, "absent", "activity.json")})
		err = ui.Run(session.Context(), &qaAppearancePTYScreen{Session: session, report: report}, reader, ui.Options{Appearance: service, Capabilities: caps, Refresh: make(chan time.Time)})
	}
	if mode == "fallback" {
		var safe *themes.Error
		if !errors.As(err, &safe) || safe.Code != "local_write_unknown" || !safe.Uncertain || safe.Details["theme"] != candidate || safe.Details["if_revision"] != "1" || safe.Details["request_id"] == "" {
			report("error", "fallback lost original uncertain intent")
		}
	} else if err != nil {
		report("error", "sequential Appearance flow failed")
	}
	if _, err := os.Stat(filepath.Join(root, "absent")); !errors.Is(err, os.ErrNotExist) {
		report("error", "frame presentation initialized unrelated state")
	}
	report("closed", "")
	os.Exit(0)
}

func TestQAAppearanceActualPTYSequentialFrameAttributes(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	for _, color := range []string{"truecolor", "ansi256", "ansi16", "none", "unknown"} {
		candidates := []string{"tokyo-night", "gruvbox", "catppuccin"}
		if color != "truecolor" && color != "ansi256" {
			candidates = candidates[:1]
		}
		for _, candidate := range candidates {
			for _, mode := range []string{"transitions", "fallback"} {
				t.Run(strings.Join([]string{color, candidate, mode}, "/"), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					root := filepath.Dir(qaAppearanceRunPath(t))
					cmd := exec.CommandContext(ctx, python, "-c", qaAppearanceFrameStateScript, os.Args[0], mode, root, color, candidate)
					cmd.Env = []string{"PATH=/usr/bin:/bin"}
					if output, err := cmd.CombinedOutput(); err != nil {
						t.Fatalf("actual sequential frame attributes: %v\n%s", err, output)
					}
					if mode == "transitions" {
						shown, err := qaAppearanceRunService(filepath.Join(root, "preferences.json"), nil).Show(context.Background(), "")
						if err != nil || shown.Theme.ID != "terminal-default" || shown.PreferenceRevision != "2" {
							t.Fatalf("sequential Apply/reset changed persistence contract: %+v %v", shown, err)
						}
					} else if b, err := os.ReadFile(filepath.Join(root, "preferences.json")); err != nil || string(b) != "PRIVATE intervening corrupt preferences" {
						t.Fatalf("fallback rewrote corrupt preferences: %q %v", b, err)
					}
				})
			}
		}
	}
}

const qaAppearanceFrameStateScript = `
import os,sys,pty,termios,subprocess,select,time,fcntl,struct,json,re
binary,mode,root,color,candidate=sys.argv[1:]
master,slave=pty.openpty()
fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',24,120,0,0))
before=termios.tcgetattr(slave);flags=fcntl.fcntl(slave,fcntl.F_GETFL)
rr,rw=os.pipe()
env={'PATH':'/usr/bin:/bin','GOMAXPROCS':'2','GORACE':'atexit_sleep_ms=0','TEMPO_QA_FRAME_STATE_MODE':mode,'TEMPO_QA_FRAME_STATE_FD':str(rw),'TEMPO_QA_FRAME_STATE_ROOT':root,'TEMPO_QA_FRAME_STATE_COLOR':color,'TEMPO_QA_FRAME_STATE_THEME':candidate}
p=subprocess.Popen([binary,'-test.run=^TestQAAppearanceFrameStatePTYChild$'],stdin=slave,stdout=slave,stderr=slave,env=env,pass_fds=(rw,),preexec_fn=os.setpgrp)
os.close(rw);transcript=b'';pending=b'';reports=[];violations=[]
sgr=re.compile(r'\x1b\[[0-9;]*m')
csi=re.compile(rb'\x1b\[([0-9;?]*)([A-Za-z~])')
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
def until(pred,seconds=2):
 deadline=time.monotonic()+seconds
 while not pred() and time.monotonic()<deadline:
  poll()
  if closed() and not pred():break
 if not pred():raise AssertionError('missing synchronized actual frame '+repr(reports[-3:]))
def matching(needle,start):return [r for r in reports[start:] if r['kind']=='frame' and needle in sgr.sub('',r['message'])]
def frame(needle,send=None):
 start=len(reports) if send is not None else 0
 mark=len(transcript) if send is not None else 0
 if send is not None:os.write(master,send)
 until(lambda:bool(matching(needle,start)))
 rendered=matching(needle,start)[-1]['message'].replace('\n','\r\n').encode()
 # Draw has completed before its report. Wait for those exact trusted bytes
 # on the actual PTY, never use the report itself as a color-state oracle.
 until(lambda:rendered in transcript[mark:])
 return actual_state()
def actual_state():
 fg=bg=None;attrs=set();current=None;position=0
 while position<len(transcript):
  if transcript[position]==27:
   match=csi.match(transcript,position)
   if not match:position+=1;continue
   params,op=match.groups();position=match.end()
   if op==b'm':
    nums=[int(x or b'0') for x in params.split(b';')];i=0
    while i<len(nums):
     n=nums[i];i+=1
     if n==0:fg=bg=None;attrs.clear()
     elif n in (38,48):
      if nums[i]==2:value=('rgb',*nums[i+1:i+4]);i+=4
      elif nums[i]==5:value=('index',nums[i+1]);i+=2
      else:raise AssertionError('unsupported color encoding')
      if n==38:fg=value
      else:bg=value
     elif n==39:fg=None
     elif n==49:bg=None
     elif 30<=n<=37 or 90<=n<=97:fg=('ansi',n)
     elif 40<=n<=47 or 100<=n<=107:bg=('ansi',n)
     elif n in (1,2,3,4,5,7,8,9):attrs.add(n)
     elif n in (22,23,24,25,27,28,29):
      for off in {22:(1,2),23:(3,),24:(4,),25:(5,),27:(7,),28:(8,),29:(9,)}[n]:attrs.discard(off)
     else:raise AssertionError('unhandled SGR '+str(n))
   elif op==b'H':current={'erase':[], 'chars':[]}
   elif op==b'J' and current is not None:current['erase'].append((fg,bg,tuple(sorted(attrs))))
  else:
   byte=transcript[position];position+=1
   if current is not None and byte>32:current['chars'].append((fg,bg,tuple(sorted(attrs))))
 if current is None:raise AssertionError('actual frame boundary absent')
 return current
def check(state,palette,label):
 if not state['erase'] or not state['chars']:raise AssertionError('actual frame lacked erase/text '+label)
 if palette=='terminal-default' or color in ('none','unknown'):
  bad=[v for v in state['erase']+state['chars'] if v!=(None,None,())]
  if bad:violations.append(label+': default effective text/cleared-cell SGR inherited '+repr(bad[0]))
 elif color in ('truecolor','ansi256'):
  if color=='truecolor':
   base={'tokyo-night':(('rgb',192,202,245),('rgb',26,27,38),()),'gruvbox':(('rgb',212,190,152),('rgb',40,40,40),()),'catppuccin':(('rgb',205,214,244),('rgb',30,30,46),())}[palette]
  else:base={'tokyo-night':(('index',153),('index',234),()),'gruvbox':(('index',180),('index',235),()),'catppuccin':(('index',189),('index',235),())}[palette]
  if any(v!=base for v in state['erase']):violations.append(label+': cleared cells use previous rather than current named base '+repr(state['erase']))
  if not any(v[1]==base[1] for v in state['chars']):violations.append(label+': actual named text missing current background')
 elif any(v[:2]!=(None,None) or v[2] for v in state['erase']):violations.append(label+': ANSI16 erase inherits role attributes')
def choose(theme):return frame('Search: '+theme,b'\x1b[200~'+theme.encode()+b'\x1b[201~')
def confirm_apply():
 frame('Review scoped change',b'\r');frame('Selected: Yes',b'y')
 return frame('No activity yet',b'\r')
try:
 initial=candidate if mode=='fallback' else 'terminal-default'
 check(frame('No activity yet'),initial,'initial dashboard')
 frame('['+initial+']',b'A')
 check(choose(candidate),candidate,'named preview')
 prefs=os.path.join(root,'preferences.json')
 if mode=='transitions':
  if os.path.exists(prefs):raise AssertionError('preview created preference store')
  check(frame('Search: terminal-default',b'\x7f'*len(candidate)+b'terminal-default'),'terminal-default','named preview -> default preview')
  check(frame('Search: '+candidate,b'\x7f'*len('terminal-default')+candidate.encode()),candidate,'default -> named preview')
  check(frame('No activity yet',b'\x1b'),'terminal-default','named preview -> cancel saved default')
  if os.path.exists(prefs):raise AssertionError('cancel preview persisted draft')
  frame('[terminal-default]',b'A');choose(candidate)
  check(confirm_apply(),candidate,'named Apply dashboard')
  frame('['+candidate+']',b'A');choose('terminal-default')
  check(confirm_apply(),'terminal-default','named Apply -> Reset default dashboard')
 else:
  frame('Review scoped change',b'\r');frame('Selected: Yes',b'y')
  check(frame('local_write_unknown',b'\r'),candidate,'retained uncertain named outcome')
  with open(prefs,'wb') as f:f.write(b'PRIVATE intervening corrupt preferences')
  check(frame('Appearance unavailable',b'\x1b'),'terminal-default','named unknown -> corrupt-current default fallback')
  if open(prefs,'rb').read()!=b'PRIVATE intervening corrupt preferences':raise AssertionError('fallback modified corrupt data')
 # All live SGR assertions above occur before Close can rescue old attributes.
 os.write(master,b'q');until(closed)
 deadline=time.monotonic()+1
 while p.poll() is None and time.monotonic()<deadline:poll(.01)
 if p.poll() is None:raise AssertionError('owned work survived cleanup')
 while select.select([master],[],[],.01)[0]:
  old=len(transcript);poll(.01)
  if old==len(transcript):break
 after=termios.tcgetattr(slave);after_flags=fcntl.fcntl(slave,fcntl.F_GETFL)
 if sys.platform=='darwin':after_flags&=~0x10000;flags&=~0x10000;after[3]&=~termios.PENDIN;before[3]&=~termios.PENDIN
 if before!=after or flags!=after_flags:raise AssertionError('termios/descriptor restoration changed')
 if transcript.count(b'\x1b[?1049h')!=1 or transcript.count(b'\x1b[?1049l')!=1:raise AssertionError('multiple terminal owners')
 if color in ('none','unknown') and re.search(rb'\x1b\[[0-9;]*(?:38|48);(?:2|5);',transcript):raise AssertionError('disabled capability selected color')
 errors=[r['message'] for r in reports if r['kind']=='error']
 if errors or p.returncode:raise AssertionError('child outcome '+repr(errors)+' exit '+str(p.returncode))
 if violations:raise AssertionError('\n'.join(violations))
 print('actual sequential SGR text/erase state verified '+color+' '+candidate+' '+mode)
finally:
 if p.poll() is None:p.kill();p.wait()
 os.close(rr);os.close(master);os.close(slave)
`
