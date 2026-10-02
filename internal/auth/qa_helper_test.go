package auth

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type qaStore struct {
	calls             *[]string
	token             string
	setErr, deleteErr error
}

func (s *qaStore) Get() (string, error) {
	*s.calls = append(*s.calls, "get")
	if s.token == "" {
		return "", ErrNotFound
	}
	return s.token, nil
}
func (s *qaStore) Set(v string) error {
	*s.calls = append(*s.calls, "set")
	if s.setErr == nil {
		s.token = v
	}
	return s.setErr
}
func (s *qaStore) Delete() error {
	*s.calls = append(*s.calls, "delete")
	if s.deleteErr == nil {
		s.token = ""
	}
	return s.deleteErr
}
func qaLock(t *testing.T) *os.File {
	t.Helper()
	f, e := AcquireMutationLock(context.Background(), filepath.Join(t.TempDir(), "owner.lock"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { f.Close() })
	return f
}
func TestQAHandlerLoginPersistsConfigBeforeToken(t *testing.T) {
	calls := []string{}
	s := &qaStore{calls: &calls}
	lock := qaLock(t)
	reply, err := qaHandleNative(context.Background(), NativeRequest{Operation: "login", ConfigPath: filepath.Join(t.TempDir(), "config.json"), AccountID: "11", Token: []byte("synthetic-qa-secret")}, lock, HandlerDependencies{Store: s, LoadConfig: func(string) (Config, error) { calls = append(calls, "load"); return Config{Account: "22"}, nil }, SaveConfig: func(_ string, c Config) error { calls = append(calls, "save:"+c.Account); return nil }})
	if err != nil || reply.Code != "" {
		t.Fatalf("login failed: %+v %v", reply, err)
	}
	if !reflect.DeepEqual(calls, []string{"load", "save:11", "set"}) {
		t.Fatalf("wrong persistence order %v", calls)
	}
	if reply.Effects != (Effects{Credential: "applied", Config: "saved"}) {
		t.Fatalf("effects %+v", reply.Effects)
	}
	if len(reply.Token) != 0 {
		t.Fatal("mutation reply retained secret")
	}
}
func TestQAHandlerConfigFailureNeverDispatchesToken(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		for _, restoreFails := range []bool{false, true} {
			t.Run(map[bool]string{false: "pre-replace", true: "post-replace"}[replaced]+map[bool]string{false: "-restore-ok", true: "-restore-fails"}[restoreFails], func(t *testing.T) {
				calls := []string{}
				s := &qaStore{calls: &calls}
				saves := 0
				reply, err := qaHandleNative(context.Background(), NativeRequest{Operation: "login", ConfigPath: filepath.Join(t.TempDir(), "cfg"), AccountID: "11", Token: []byte("synthetic-qa-secret")}, qaLock(t), HandlerDependencies{Store: s, LoadConfig: func(string) (Config, error) { return Config{Account: "22"}, nil }, SaveConfig: func(_ string, c Config) error {
					saves++
					calls = append(calls, "save:"+c.Account)
					if saves == 1 {
						if replaced {
							return &SaveError{Replaced: true}
						}
						return errors.New("synthetic-qa-secret private-path")
					}
					if restoreFails {
						return errors.New("synthetic-qa-secret private-path")
					}
					return nil
				}})
				if err == nil && reply.Code == "" {
					t.Fatal("save failure reported success")
				}
				if strings.Contains(strings.Join(calls, ","), "set") {
					t.Fatal("token changed after config failure")
				}
				want := []string{"save:11"}
				if replaced {
					want = append(want, "save:22")
				}
				if !reflect.DeepEqual(calls, want) {
					t.Fatalf("save sequence %v", calls)
				}
				if reply.Effects.Credential != "unchanged" {
					t.Fatalf("credential effects %+v", reply.Effects)
				}
				if strings.Contains(reply.Code, "synthetic") || (err != nil && strings.Contains(err.Error(), "synthetic")) {
					t.Fatal("raw error leaked")
				}
			})
		}
	}
}
func TestQAHandlerLogoutDoesNotParseOldConfig(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "present", true: "absent"}[missing], func(t *testing.T) {
			calls := []string{}
			s := &qaStore{calls: &calls}
			if missing {
				s.deleteErr = ErrNotFound
			}
			reply, err := qaHandleNative(context.Background(), NativeRequest{Operation: "logout", ConfigPath: filepath.Join(t.TempDir(), "cfg")}, qaLock(t), HandlerDependencies{Store: s, LoadConfig: func(string) (Config, error) { t.Fatal("logout parsed malformed old config"); return Config{}, nil }, SaveConfig: func(_ string, c Config) error { calls = append(calls, "save:"+c.Account); return nil }})
			if err != nil || reply.Code != "" {
				t.Fatalf("logout %+v %v", reply, err)
			}
			if !reflect.DeepEqual(calls, []string{"delete", "save:"}) {
				t.Fatalf("calls %v", calls)
			}
			if reply.Effects != (Effects{Credential: "applied", Config: "cleared"}) {
				t.Fatalf("effects %+v", reply.Effects)
			}
		})
	}
}
func TestQAHandlerRejectsMutationWithoutOwner(t *testing.T) {
	for _, op := range []string{"login", "logout", "account", "arbitrary"} {
		t.Run(op, func(t *testing.T) {
			calls := []string{}
			s := &qaStore{calls: &calls}
			reply, err := qaHandleNative(context.Background(), NativeRequest{Operation: op, AccountID: "11", Token: []byte("synthetic-qa-secret")}, nil, HandlerDependencies{Store: s, SaveConfig: func(string, Config) error { t.Fatal("unowned save"); return nil }})
			if err == nil && reply.Code == "" {
				t.Fatal("unowned mutation accepted")
			}
			if len(calls) != 0 {
				t.Fatalf("unowned native access %v", calls)
			}
		})
	}
}

func TestQAHandlerCredentialFailureKnowledgeControlsRestoration(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown", true: "definite-rejection"}[known], func(t *testing.T) {
			calls := []string{}
			var failure error = errors.New("synthetic-qa-secret native-internal")
			if known {
				failure = &Error{Code: "keychain", Message: "denied", Effects: Effects{Credential: "unchanged", Config: "unchanged"}}
			}
			s := &qaStore{calls: &calls, setErr: failure}
			reply, err := qaHandleNative(context.Background(), NativeRequest{Operation: "login", ConfigPath: filepath.Join(t.TempDir(), "cfg"), AccountID: "11", Token: []byte("synthetic-qa-secret")}, qaLock(t), HandlerDependencies{Store: s, LoadConfig: func(string) (Config, error) { return Config{Account: "22"}, nil }, SaveConfig: func(_ string, c Config) error { calls = append(calls, "save:"+c.Account); return nil }})
			want := []string{"save:11", "set"}
			if known {
				want = append(want, "save:22")
			}
			if !reflect.DeepEqual(calls, want) {
				t.Fatalf("unsafe compensation/order %v", calls)
			}
			if err == nil && reply.Code == "" {
				t.Fatal("native failure success")
			}
			if known {
				if reply.Effects != (Effects{Credential: "unchanged", Config: "restored"}) {
					t.Fatalf("known effects %+v", reply.Effects)
				}
			} else {
				if reply.Code != "credential_write_unknown" || reply.Effects != (Effects{Credential: "unknown", Config: "saved"}) {
					t.Fatalf("lost uncertainty %+v", reply)
				}
			}
			if err != nil && strings.Contains(err.Error(), "synthetic-qa-secret") {
				t.Fatal("secret error leaked")
			}
		})
	}
}
func TestQAHandlerDeleteThenConfigFailureReportsPartialState(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "unknown"}[replaced], func(t *testing.T) {
			calls := []string{}
			s := &qaStore{calls: &calls}
			reply, err := qaHandleNative(context.Background(), NativeRequest{Operation: "logout", ConfigPath: filepath.Join(t.TempDir(), "cfg")}, qaLock(t), HandlerDependencies{Store: s, SaveConfig: func(string, Config) error {
				calls = append(calls, "clear")
				if replaced {
					return &SaveError{Replaced: true}
				}
				return errors.New("synthetic-qa-secret")
			}})
			if err == nil && reply.Code == "" {
				t.Fatal("clear failure success")
			}
			wantConfig := "unchanged"
			if replaced {
				wantConfig = "unknown"
			}
			if reply.Effects != (Effects{Credential: "applied", Config: wantConfig}) {
				t.Fatalf("effects %+v", reply.Effects)
			}
			if !reflect.DeepEqual(calls, []string{"delete", "clear"}) {
				t.Fatalf("calls %v", calls)
			}
		})
	}
}
func TestQAHandlerReadAndAccountNeverWriteCredentials(t *testing.T) {
	for _, op := range []string{"read", "account"} {
		t.Run(op, func(t *testing.T) {
			calls := []string{}
			s := &qaStore{calls: &calls, token: "synthetic-qa-secret"}
			var lock *os.File
			if op == "account" {
				lock = qaLock(t)
			}
			reply, err := qaHandleNative(context.Background(), NativeRequest{Operation: op, ConfigPath: filepath.Join(t.TempDir(), "cfg"), AccountID: "11"}, lock, HandlerDependencies{Store: s, SaveConfig: func(_ string, c Config) error { calls = append(calls, "save:"+c.Account); return nil }})
			if err != nil || reply.Code != "" {
				t.Fatalf("%s %+v %v", op, reply, err)
			}
			want := []string{"get"}
			effects := Effects{Credential: "unchanged", Config: "unchanged"}
			if op == "account" {
				want = []string{"save:11"}
				effects.Config = "saved"
				if len(reply.Token) > 0 {
					t.Fatal("config reply leaked token")
				}
			} else if string(reply.Token) != "synthetic-qa-secret" {
				t.Fatal("read did not privately return token")
			}
			if !reflect.DeepEqual(calls, want) || reply.Effects != effects {
				t.Fatalf("calls=%v effects=%+v", calls, reply.Effects)
			}
		})
	}
}
func TestQAHandlerRejectsOrdinaryUnownedDescriptor(t *testing.T) {
	f, e := os.CreateTemp(t.TempDir(), "not-a-lock")
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	calls := []string{}
	s := &qaStore{calls: &calls}
	r, e := qaHandleNative(context.Background(), NativeRequest{Operation: "logout", ConfigPath: filepath.Join(t.TempDir(), "cfg")}, f, HandlerDependencies{Store: s, SaveConfig: func(string, Config) error { calls = append(calls, "save"); return nil }})
	if e == nil && r.Code == "" {
		t.Fatal("ordinary unlocked descriptor authorized mutation")
	}
	if len(calls) != 0 {
		t.Fatalf("unowned mutation effects %v", calls)
	}
}

// qaHandleNative supplies only the explicit temporary credential namespace.
// A random descriptor in that directory is deliberately not that namespace.
func qaHandleNative(ctx context.Context, r NativeRequest, lock *os.File, d HandlerDependencies) (NativeReply, error) {
	if lock != nil {
		d.LockPath = filepath.Join(filepath.Dir(lock.Name()), "owner.lock")
	}
	return HandleNative(ctx, r, lock, d)
}
