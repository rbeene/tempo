//go:build darwin || linux

package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestQAProcessSyntheticChild(t *testing.T) {
	mode := ""
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "qa-mode=") {
			mode = strings.TrimPrefix(arg, "qa-mode=")
		}
	}
	if mode == "" {
		return
	}
	in := os.NewFile(3, "qa-private-request")
	out := os.NewFile(4, "qa-private-reply")
	defer in.Close()
	defer out.Close()
	var req NativeRequest
	if json.NewDecoder(in).Decode(&req) != nil {
		os.Exit(31)
	}
	// The only secret transport is fd3; neither argv nor environment may contain it.
	for _, s := range append(os.Args, os.Environ()...) {
		if strings.Contains(s, "synthetic-qa-secret") {
			os.Exit(32)
		}
	}
	if req.ConfigPath != "" {
		os.WriteFile(req.ConfigPath+".pid", []byte(strconv.Itoa(os.Getpid())), 0600)
	}
	switch mode {
	case "read":
		json.NewEncoder(out).Encode(NativeReply{Token: []byte("synthetic-qa-secret"), Effects: Effects{Credential: "unchanged", Config: "unchanged"}})
	case "success":
		json.NewEncoder(out).Encode(NativeReply{Effects: Effects{Credential: "applied", Config: "saved"}})
	case "late-effect":
		if os.WriteFile(req.ConfigPath+".accepted", []byte("accepted"), 0600) != nil {
			os.Exit(40)
		}
		time.Sleep(5 * time.Second)
	case "stall", "mutated-stall":
		if mode == "mutated-stall" {
			os.WriteFile(req.ConfigPath+".applied", []byte("applied"), 0600)
		}
		time.Sleep(5 * time.Second)
	case "partial":
		fmt.Fprint(out, `{"effects":{"credential":"`)
		time.Sleep(5 * time.Second)
	case "oversize":
		fmt.Fprint(out, strings.Repeat("x", 40000))
	case "fake-success":
		fmt.Fprint(out, `{"effects":{"credential":"unchanged","config":"unchanged"}}`)
	case "secret-error":
		fmt.Fprint(out, `{"code":"synthetic-qa-secret","effects":{"credential":"unknown","config":"unknown"}}`)
	case "extra":
		fmt.Fprint(out, `{"effects":{"credential":"applied","config":"saved"},"untrusted":true}`)
	case "duplicate":
		fmt.Fprint(out, `{"effects":{"credential":"applied","credential":"unchanged","config":"saved"}}`)
	case "two":
		fmt.Fprint(out, `{"effects":{"credential":"applied","config":"saved"}} {}`)
	}
	os.Exit(0)
}
func qaProcess(mode string, d time.Duration) ProcessRunner {
	return ProcessRunner{Executable: os.Args[0], Args: []string{"-test.run=^TestQAProcessSyntheticChild$", "--", "qa-mode=" + mode}, Timeout: d}
}
func qaAssertReaped(t *testing.T, path string) {
	t.Helper()
	b, e := os.ReadFile(path + ".pid")
	if os.IsNotExist(e) {
		return
	}
	if e != nil {
		t.Fatal(e)
	}
	pid, e := strconv.Atoi(string(b))
	if e != nil {
		t.Fatal(e)
	}
	if e = syscall.Kill(pid, 0); !errors.Is(e, syscall.ESRCH) {
		t.Fatalf("child %d still alive after return: %v", pid, e)
	}
}
func TestQAProcessPrivateRead(t *testing.T) {
	p := qaProcess("read", time.Second)
	r, e := p.Run(context.Background(), NativeRequest{Operation: "read"}, nil)
	if e != nil || string(r.Token) != "synthetic-qa-secret" {
		t.Fatalf("read failed code=%s err=%v", r.Code, e)
	}
}
func TestQAProcessPreCanceledNeverDispatches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cfg")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e := qaProcess("mutated-stall", time.Second).Run(ctx, NativeRequest{Operation: "login", ConfigPath: path, AccountID: "11", Token: []byte("synthetic-qa-secret")}, qaLock(t))
	if e == nil {
		t.Fatal("canceled operation succeeded")
	}
	if _, e = os.Stat(path + ".pid"); !os.IsNotExist(e) {
		t.Fatal("pre-canceled runner launched helper")
	}
	if _, e = os.Stat(path + ".applied"); !os.IsNotExist(e) {
		t.Fatal("pre-cancel mutated")
	}
}
func TestQAProcessPostDispatchFailuresStayUnknownAndReap(t *testing.T) {
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	for _, mode := range []string{"stall", "mutated-stall", "partial", "oversize", "fake-success", "secret-error", "extra", "two", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cfg")
			timeout := 2 * time.Second
			stalled := mode == "stall" || mode == "mutated-stall" || mode == "partial"
			if stalled {
				timeout = 350 * time.Millisecond
			}
			start := time.Now()
			r, e := qaProcess(mode, timeout).Run(context.Background(), NativeRequest{Operation: "login", ConfigPath: path, AccountID: "11", Token: []byte("synthetic-qa-secret")}, qaLock(t))
			if time.Since(start) > timeout+time.Second {
				t.Fatal("helper did not return within bounded deadline")
			}
			var ae *Error
			if !errors.As(e, &ae) || ae.Code != "credential_write_unknown" || !ae.Uncertain || ae.Retryable || ae.Effects != (Effects{Credential: "unknown", Config: "unknown"}) {
				t.Fatalf("post-dispatch failure lost uncertainty reply=%+v err=%#v", r, e)
			}
			if strings.Contains(e.Error(), "synthetic-qa-secret") {
				t.Fatal("secret escaped error boundary")
			}
			if _, statErr := os.Stat(path + ".pid"); statErr != nil {
				t.Fatalf("synthetic child did not reach reply scenario: %v", statErr)
			}
			qaAssertReaped(t, path)
		})
	}
}
func TestQAProcessUnknownAccountNeverClaimsCredentialMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cfg")
	_, e := qaProcess("stall", 100*time.Millisecond).Run(context.Background(), NativeRequest{Operation: "account", ConfigPath: path, AccountID: "11"}, qaLock(t))
	var ae *Error
	if !errors.As(e, &ae) || ae.Code != "credential_write_unknown" || ae.Effects != (Effects{Credential: "unchanged", Config: "unknown"}) {
		t.Fatalf("account-only uncertainty %#v", e)
	}
	qaAssertReaped(t, path)
}
func TestQAProtocolRejectsInvalidRequestsWithoutAccess(t *testing.T) {
	for _, raw := range []string{`{"operation":"arbitrary"}`, `{"operation":"read","unknown":true}`, `{"operation":"read","operation":"logout"}`, `{"operation":"read"} {}`, `{"operation":"read"`, strings.Repeat("x", 40000)} {
		calls := []string{}
		s := &qaStore{calls: &calls}
		var out strings.Builder
		e := ServeNative(context.Background(), strings.NewReader(raw), &out, nil, HandlerDependencies{Store: s})
		if e == nil && out.Len() == 0 {
			t.Fatal("invalid protocol silently accepted")
		}
		if len(calls) != 0 {
			t.Fatalf("bad protocol accessed store %v", calls)
		}
		if strings.Contains(out.String(), "synthetic-qa-secret") {
			t.Fatal("invalid protocol leaked secret")
		}
	}
}

var _ io.Reader = (*os.File)(nil)

func TestQAExternalAcceptedMutationCanFinishAfterHelperDeath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cfg")
	finish := make(chan struct{})
	done := make(chan error, 1)
	// The simulated OS service is independent of the helper lifetime.
	go func() { <-finish; done <- os.WriteFile(path+".late-applied", []byte("applied"), 0600) }()
	_, err := qaProcess("late-effect", 350*time.Millisecond).Run(context.Background(), NativeRequest{Operation: "login", ConfigPath: path, AccountID: "11", Token: []byte("synthetic-qa-secret")}, qaLock(t))
	qaAssertReaped(t, path)
	if _, e := os.Stat(path + ".accepted"); e != nil {
		close(finish)
		<-done
		t.Fatal("external simulator did not accept mutation")
	}
	var ae *Error
	if !errors.As(err, &ae) || ae.Code != "credential_write_unknown" || !ae.Uncertain || ae.Retryable {
		close(finish)
		<-done
		t.Fatalf("unknown lost before late effect %#v", err)
	}
	if _, e := os.Stat(path + ".late-applied"); !os.IsNotExist(e) {
		close(finish)
		<-done
		t.Fatal("simulator applied before release")
	}
	close(finish)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if !ae.Uncertain || ae.Effects.Credential != "unknown" {
		t.Fatal("later observation falsely reconciled original uncertainty")
	}
}
