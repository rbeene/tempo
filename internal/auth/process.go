package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/rbeene/tempo/internal/identity"
	"io"
	"os"
	"os/exec"
	"time"
)

const protocolLimit = 32 * 1024

func validRequest(r NativeRequest) bool {
	switch r.Operation {
	case "read":
		return r.ConfigPath == "" && r.AccountID == "" && len(r.Token) == 0
	case "login":
		_, e := validateToken(string(r.Token))
		return e == nil && r.ConfigPath != "" && identity.Valid(r.AccountID)
	case "logout":
		return r.ConfigPath != "" && r.AccountID == "" && len(r.Token) == 0
	case "account":
		return r.ConfigPath != "" && identity.Valid(r.AccountID) && len(r.Token) == 0
	}
	return false
}
func strictJSON(b []byte, v any) error {
	if len(b) > protocolLimit {
		return errors.New("protocol")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	var visit func() error
	visit = func() error {
		t, e := d.Token()
		if e != nil {
			return e
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return e
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return errors.New("protocol")
				}
				seen[s] = true
				if e = visit(); e != nil {
					return e
				}
			}
		case '[':
			for d.More() {
				if e = visit(); e != nil {
					return e
				}
			}
		default:
			return errors.New("protocol")
		}
		_, e = d.Token()
		return e
	}
	if e := visit(); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return errors.New("protocol")
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(v)
}
func validReply(r NativeReply, op string) bool {
	c, g := r.Effects.Credential, r.Effects.Config
	if c != "unchanged" && c != "applied" && c != "unknown" {
		return false
	}
	if g != "unchanged" && g != "saved" && g != "cleared" && g != "restored" && g != "unknown" {
		return false
	}
	if op == "read" {
		if c != "unchanged" || g != "unchanged" {
			return false
		}
		if r.Code == "" {
			_, e := validateToken(string(r.Token))
			return e == nil
		}
		return len(r.Token) == 0 && (r.Code == "not_found" || r.Code == "keychain" || r.Code == "validation")
	}
	if len(r.Token) > 0 {
		return false
	}
	if r.Code == "" {
		switch op {
		case "login":
			return c == "applied" && g == "saved"
		case "logout":
			return c == "applied" && g == "cleared"
		case "account":
			return c == "unchanged" && g == "saved"
		}
		return false
	}
	if op == "account" && c != "unchanged" {
		return false
	}
	switch r.Code {
	case "validation", "state_busy":
		return c == "unchanged" && g == "unchanged"
	case "keychain":
		return c == "unchanged" && (g == "unchanged" || g == "restored")
	case "config":
		return (c == "unchanged" || op == "logout" && c == "applied") && (g == "unchanged" || g == "restored" || g == "unknown")
	case "credential_write_unknown":
		return c == "unknown" && (g == "unknown" || g == "saved" || g == "unchanged") || op == "account" && c == "unchanged" && g == "unknown"
	}
	return false
}
func (p ProcessRunner) Run(ctx context.Context, r NativeRequest, lock *os.File) (NativeReply, error) {
	if e := ctx.Err(); e != nil {
		return NativeReply{}, e
	}
	if !validRequest(r) || r.Operation != "read" && lock == nil {
		return NativeReply{}, issue("validation", unchanged())
	}
	b, e := json.Marshal(r)
	if e != nil || len(b) > protocolLimit {
		return NativeReply{}, issue("validation", unchanged())
	}
	defer clear(b)
	exe := p.Executable
	if exe == "" {
		exe, e = os.Executable()
		if e != nil {
			return NativeReply{}, issue("keychain", unchanged())
		}
	}
	args := p.Args
	if len(args) == 0 {
		args = []string{"--tempo-auth-helper"}
	}
	timeout := p.Timeout
	if timeout <= 0 || timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	bounded, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	reqR, reqW, e := os.Pipe()
	if e != nil {
		return NativeReply{}, issue("keychain", unchanged())
	}
	defer reqR.Close()
	defer reqW.Close()
	repR, repW, e := os.Pipe()
	if e != nil {
		return NativeReply{}, issue("keychain", unchanged())
	}
	defer repR.Close()
	defer repW.Close()
	cmd := exec.Command(exe, args...)
	cmd.ExtraFiles = []*os.File{reqR, repW}
	if lock != nil {
		cmd.ExtraFiles = append(cmd.ExtraFiles, lock)
	}
	// Helpers never need credential/environment overrides. Do not inherit secrets.
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	if e = cmd.Start(); e != nil {
		return NativeReply{}, issue("keychain", unchanged())
	}
	reqR.Close()
	repW.Close()
	type writeResult struct {
		n int
		e error
	}
	wrote := make(chan writeResult, 1)
	go func() { n, e := reqW.Write(b); reqW.Close(); wrote <- writeResult{n, e} }()
	type readResult struct {
		r NativeReply
		e error
	}
	read := make(chan readResult, 1)
	go func() {
		data, e := io.ReadAll(io.LimitReader(repR, protocolLimit+1))
		var result NativeReply
		if e == nil {
			e = strictJSON(data, &result)
		}
		clear(data)
		if e == nil && !validReply(result, r.Operation) {
			e = errors.New("protocol")
		}
		read <- readResult{result, e}
	}()
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	var rr readResult
	gotReply := false
	select {
	case rr = <-read:
		gotReply = true
	case <-bounded.Done():
	}
	// Closing pipes releases blocked readers/writers, then reaping completes before
	// the caller releases its lock descriptor. A complete reply wins cancellation.
	_ = cmd.Process.Kill()
	reqW.Close()
	repR.Close()
	<-waited
	if !gotReply {
		rr = <-read
	}
	wr := <-wrote
	if rr.e == nil {
		return rr.r, nil
	}
	if r.Operation != "read" && wr.n > 0 {
		effects := Effects{"unknown", "unknown"}
		if r.Operation == "account" {
			effects.Credential = "unchanged"
		}
		return NativeReply{}, issue("credential_write_unknown", effects)
	}
	if ctx.Err() != nil {
		return NativeReply{}, ctx.Err()
	}
	return NativeReply{}, issue("keychain", unchanged())
}
func ServeNative(ctx context.Context, in io.Reader, out io.Writer, lock *os.File, d HandlerDependencies) error {
	b, e := io.ReadAll(io.LimitReader(in, protocolLimit+1))
	if e != nil {
		return issue("validation", unchanged())
	}
	defer clear(b)
	var r NativeRequest
	if strictJSON(b, &r) != nil || !validRequest(r) {
		return issue("validation", unchanged())
	}
	defer clear(r.Token)
	reply, e := HandleNative(ctx, r, lock, d)
	if e != nil {
		return sanitizeRunnerError(e)
	}
	defer clear(reply.Token)
	if !validReply(reply, r.Operation) {
		return issue("response", unchanged())
	}
	return json.NewEncoder(out).Encode(reply)
}

// HelperMain is called before normal CLI parsing. Only inherited private pipes
// permit the internal mode; ordinary stdout/stderr never carry credentials.
func HelperMain(args []string) (bool, int) {
	if len(args) != 1 || args[0] != "--tempo-auth-helper" {
		return false, 0
	}
	in, out := os.NewFile(3, "tempo-private-request"), os.NewFile(4, "tempo-private-reply")
	defer in.Close()
	defer out.Close()
	for _, f := range []*os.File{in, out} {
		i, e := f.Stat()
		if e != nil || i.Mode()&os.ModeNamedPipe == 0 {
			return true, 1
		}
	}
	b, e := io.ReadAll(io.LimitReader(in, protocolLimit+1))
	if e != nil {
		return true, 1
	}
	defer clear(b)
	var r NativeRequest
	if strictJSON(b, &r) != nil || !validRequest(r) {
		return true, 1
	}
	defer clear(r.Token)
	var lock *os.File
	if r.Operation != "read" {
		lock = os.NewFile(5, "tempo-auth-owner")
		defer lock.Close()
		i, e := lock.Stat()
		if e != nil || !i.Mode().IsRegular() {
			return true, 1
		}
	}
	var store Store
	if r.Operation != "account" {
		store, e = noninteractiveStore()
		if e != nil {
			_ = json.NewEncoder(out).Encode(NativeReply{Code: "keychain", Effects: unchanged()})
			return true, 0
		}
	}
	reply, e := HandleNative(context.Background(), r, lock, HandlerDependencies{Store: store})
	if e != nil || !validReply(reply, r.Operation) {
		return true, 1
	}
	defer clear(reply.Token)
	if json.NewEncoder(out).Encode(reply) != nil {
		return true, 1
	}
	return true, 0
}
