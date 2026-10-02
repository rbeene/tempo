package auth

import (
	"context"
	"errors"
	"github.com/rbeene/tempo/internal/harvest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type qaProvider struct {
	harvest.Provider
	accounts func(context.Context) ([]harvest.Object, error)
}

func (p qaProvider) Accounts(c context.Context) ([]harvest.Object, error) { return p.accounts(c) }
func qaService(t *testing.T, runner Runner) *Service {
	t.Helper()
	return NewService(Options{ConfigPath: filepath.Join(t.TempDir(), "cfg"), LockPath: filepath.Join(t.TempDir(), "owner.lock"), Getenv: func(string) string { return "" }, Runner: runner, PersistentAvailable: func() bool { return true }, NewProvider: func(string, string) harvest.Provider {
		return qaProvider{accounts: func(context.Context) ([]harvest.Object, error) {
			return []harvest.Object{{"id": "11", "product": "harvest"}}, nil
		}}
	}})
}
func TestQAEnvironmentTokenNeverStartsHelper(t *testing.T) {
	s := NewService(Options{Getenv: func(k string) string {
		if k == "HARVEST_TOKEN" {
			return " synthetic-qa-secret "
		}
		return ""
	}, Runner: RunnerFunc(func(context.Context, NativeRequest, *os.File) (NativeReply, error) {
		t.Fatal("environment auth invoked native helper")
		return NativeReply{}, nil
	})})
	token, source, err := s.ResolveToken(context.Background())
	if err != nil || token != "synthetic-qa-secret" || source != "environment" {
		t.Fatalf("environment resolution source=%q err=%v", source, err)
	}
}
func TestQALoginAttemptSingleConsumptionAndSelection(t *testing.T) {
	calls := 0
	s := qaService(t, RunnerFunc(func(_ context.Context, r NativeRequest, l *os.File) (NativeReply, error) {
		calls++
		if l == nil || r.Operation != "login" || r.AccountID != "11" {
			t.Fatalf("wrong dispatch %+v lock=%v", r, l)
		}
		return NativeReply{Effects: Effects{Credential: "applied", Config: "saved"}}, nil
	}))
	a, err := s.PrepareLogin(context.Background(), []byte("synthetic-qa-secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if calls != 0 {
		t.Fatal("prepare wrote credentials")
	}
	if _, err = s.CommitLogin(context.Background(), a, "99"); err == nil {
		t.Fatal("inaccessible account accepted")
	}
	if calls != 0 {
		t.Fatal("invalid account dispatched")
	}
	a, err = s.PrepareLogin(context.Background(), []byte("synthetic-qa-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitLogin(context.Background(), a, "11"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitLogin(context.Background(), a, "11"); err == nil {
		t.Fatal("attempt replayed")
	}
	if calls != 1 {
		t.Fatalf("dispatches %d", calls)
	}
}
func TestQALogoutRequiresConfirmationBeforeDispatch(t *testing.T) {
	s := qaService(t, RunnerFunc(func(context.Context, NativeRequest, *os.File) (NativeReply, error) {
		t.Fatal("unconfirmed logout dispatched")
		return NativeReply{}, nil
	}))
	_, err := s.Logout(context.Background(), false)
	var ae *Error
	if !errors.As(err, &ae) || ae.Code != "confirmation_required" {
		t.Fatalf("confirmation error %v", err)
	}
}
func TestQAValidatedMutationReplyWinsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := qaService(t, RunnerFunc(func(context.Context, NativeRequest, *os.File) (NativeReply, error) {
		cancel()
		return NativeReply{Effects: Effects{Credential: "applied", Config: "cleared"}}, nil
	}))
	r, e := s.Logout(ctx, true)
	if e != nil || !r.LoggedOut || r.Effects.Credential != "applied" {
		t.Fatalf("conclusive reply lost to cancellation %+v %v", r, e)
	}
}
func TestQAAccountValidationOwnsLockThroughConfigCommit(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "owner.lock")
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	s := NewService(Options{ConfigPath: filepath.Join(dir, "cfg"), LockPath: lockPath, Getenv: func(k string) string {
		if k == "HARVEST_TOKEN" {
			return "synthetic-qa-secret"
		}
		return ""
	}, PersistentAvailable: func() bool { return false }, NewProvider: func(string, string) harvest.Provider {
		return qaProvider{accounts: func(context.Context) ([]harvest.Object, error) {
			close(entered)
			<-release
			return []harvest.Object{{"id": "11", "product": "harvest"}}, nil
		}}
	}, Runner: RunnerFunc(func(_ context.Context, r NativeRequest, l *os.File) (NativeReply, error) {
		if r.Operation != "account" || l == nil {
			t.Errorf("account mutation bypassed protected config helper: %+v", r)
		}
		return NativeReply{Effects: Effects{Credential: "unchanged", Config: "saved"}}, nil
	})})
	go func() { _, e := s.UseAccount(context.Background(), "11"); done <- e }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("account validation not entered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	f, e := AcquireMutationLock(ctx, lockPath)
	if f != nil {
		f.Close()
	}
	close(release)
	opErr := <-done
	if e == nil || f != nil {
		t.Fatal("credential mutation could race account preflight")
	}
	if opErr != nil {
		t.Fatal(opErr)
	}
}
func TestQALoginRollbackUsesFreshLockedBaseline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg")
	if e := Save(path, Config{Account: "22"}); e != nil {
		t.Fatal(e)
	}
	calls := []string{}
	store := &qaStore{calls: &calls, setErr: &Error{Code: "keychain", Message: "rejected", Effects: unchanged()}}
	s := NewService(Options{ConfigPath: path, LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, NewProvider: func(string, string) harvest.Provider {
		return qaProvider{accounts: func(context.Context) ([]harvest.Object, error) {
			return []harvest.Object{{"id": "11", "product": "harvest"}}, nil
		}}
	}, Runner: RunnerFunc(func(ctx context.Context, r NativeRequest, l *os.File) (NativeReply, error) {
		return qaHandleNative(ctx, r, l, HandlerDependencies{Store: store})
	})})
	attempt, e := s.PrepareLogin(context.Background(), []byte("synthetic-qa-secret"))
	if e != nil {
		t.Fatal(e)
	}
	defer attempt.Close()
	owner, e := AcquireMutationLock(context.Background(), filepath.Join(dir, "owner.lock"))
	if e != nil {
		t.Fatal(e)
	}
	if e = Save(path, Config{Account: "33"}); e != nil {
		t.Fatal(e)
	}
	owner.Close()
	_, e = s.CommitLogin(context.Background(), attempt, "11")
	if e == nil {
		t.Fatal("rejected credential reported success")
	}
	cfg, e := Load(path)
	if e != nil || cfg.Account != "33" {
		t.Fatalf("rollback erased later account selection %+v %v", cfg, e)
	}
}
