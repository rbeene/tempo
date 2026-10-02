//go:build darwin || linux

package auth

import (
	"context"
	"errors"
	"github.com/rbeene/tempo/internal/harvest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestQAMutationLockWaitCancellationPreservesOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock")
	owner, e := AcquireMutationLock(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	defer owner.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	f, e := AcquireMutationLock(ctx, path)
	if f != nil {
		f.Close()
		t.Fatal("waiter acquired live owner's lock")
	}
	if e == nil || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("wait not bounded: %v %s", e, time.Since(start))
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel2()
	f, e = AcquireMutationLock(ctx2, path)
	if f != nil {
		f.Close()
		t.Fatal("canceled waiter released another owner's lock")
	}
	if e == nil {
		t.Fatal("owner no longer exclusive")
	}
	owner.Close()
	f, e = AcquireMutationLock(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	f.Close()
}
func TestQAMutationLockRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if e := os.WriteFile(target, []byte("untouched"), 0600); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "lock")
	if e := os.Symlink(target, path); e != nil {
		t.Fatal(e)
	}
	f, e := AcquireMutationLock(context.Background(), path)
	if f != nil {
		f.Close()
		t.Fatal("symlink lock accepted")
	}
	if e == nil {
		t.Fatal("symlink lock missing failure")
	}
	data, _ := os.ReadFile(target)
	if string(data) != "untouched" {
		t.Fatal("symlink target changed")
	}
}
func TestQALockInheritedChild(t *testing.T) {
	if os.Getenv("TEMPO_QA_LOCK_CHILD") != "1" {
		return
	}
	f := os.NewFile(3, "inherited-test-lock")
	if f == nil {
		os.Exit(10)
	}
	defer f.Close()
	dir := os.Getenv("TEMPO_QA_BARRIER_DIR")
	if os.WriteFile(filepath.Join(dir, "ready"), []byte("ready"), 0600) != nil {
		os.Exit(11)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, e := os.Stat(filepath.Join(dir, "release")); e == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	os.Exit(12)
}
func qaAwaitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, e := os.Stat(path); e == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("barrier did not arrive %s", filepath.Base(path))
}
func TestQAMutationLockLastInheritedDescriptorOwnsLock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lock")
	owner, e := AcquireMutationLock(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	defer owner.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestQALockInheritedChild$")
	cmd.Env = append(os.Environ(), "TEMPO_QA_LOCK_CHILD=1", "TEMPO_QA_BARRIER_DIR="+dir)
	cmd.ExtraFiles = []*os.File{owner}
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	qaAwaitFile(t, filepath.Join(dir, "ready"))
	owner.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	next, e := AcquireMutationLock(ctx, path)
	if next != nil {
		next.Close()
		t.Fatal("closing parent descriptor unlocked surviving child")
	}
	if e == nil {
		t.Fatal("inherited lock not held")
	}
	if e = os.WriteFile(filepath.Join(dir, "release"), []byte("go"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = cmd.Wait(); e != nil {
		t.Fatal(e)
	}
	next, e = AcquireMutationLock(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	next.Close()
}
func TestQAMutationLockBusyHasFiniteSafeEffects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock")
	owner, e := AcquireMutationLock(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	defer owner.Close()
	start := time.Now()
	f, e := AcquireMutationLock(context.Background(), path)
	if f != nil {
		f.Close()
		t.Fatal("concurrent lock acquired")
	}
	var ae *Error
	if !errors.As(e, &ae) || ae.Code != "state_busy" || !ae.Retryable || ae.Uncertain || ae.Effects != (Effects{Credential: "unchanged", Config: "unchanged"}) {
		t.Fatalf("busy failure %#v", e)
	}
	if time.Since(start) > 1500*time.Millisecond {
		t.Fatal("lock wait exceeded one-second budget")
	}
}

type qaProcessStore struct {
	dir, id string
	paused  bool
}

func (s qaProcessStore) Get() (string, error) { return "synthetic-qa-secret", nil }
func (s qaProcessStore) Delete() error {
	return os.WriteFile(filepath.Join(s.dir, "native-account"), []byte("cleared"), 0600)
}
func (s qaProcessStore) Set(string) error {
	if s.paused {
		if e := os.WriteFile(filepath.Join(s.dir, "inside-commit"), []byte("ready"), 0600); e != nil {
			return e
		}
		deadline := time.Now().Add(4 * time.Second)
		for {
			if _, e := os.Stat(filepath.Join(s.dir, "release-commit")); e == nil {
				break
			}
			if time.Now().After(deadline) {
				return errors.New("synthetic fixture deadline")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	return os.WriteFile(filepath.Join(s.dir, "native-account"), []byte(s.id), 0600)
}
func TestQASerializedMutationProcess(t *testing.T) {
	if os.Getenv("TEMPO_QA_MUTATOR") != "1" {
		return
	}
	dir := os.Getenv("TEMPO_QA_MUTATOR_DIR")
	id := os.Getenv("TEMPO_QA_MUTATOR_ACCOUNT")
	s := NewService(Options{ConfigPath: filepath.Join(dir, "config-"+id), LockPath: filepath.Join(dir, "owner.lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, NewProvider: func(string, string) harvest.Provider {
		return qaProvider{accounts: func(context.Context) ([]harvest.Object, error) {
			return []harvest.Object{{"id": id, "product": "harvest"}}, nil
		}}
	}, Runner: RunnerFunc(func(ctx context.Context, r NativeRequest, l *os.File) (NativeReply, error) {
		return qaHandleNative(ctx, r, l, HandlerDependencies{Store: qaProcessStore{dir: dir, id: id, paused: id == "11"}})
	})})
	a, e := s.PrepareLogin(context.Background(), []byte("synthetic-qa-secret"))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	if _, e = s.CommitLogin(context.Background(), a, id); e != nil {
		t.Fatal(e)
	}
}
func TestQAMutationsDifferentConfigsSerializeOneCredentialNamespace(t *testing.T) {
	dir := t.TempDir()
	start := func(id string) *exec.Cmd {
		c := exec.Command(os.Args[0], "-test.run=^TestQASerializedMutationProcess$")
		c.Env = append(os.Environ(), "GORACE=atexit_sleep_ms=0", "TEMPO_QA_MUTATOR=1", "TEMPO_QA_MUTATOR_DIR="+dir, "TEMPO_QA_MUTATOR_ACCOUNT="+id)
		if e := c.Start(); e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { c.Process.Kill(); c.Wait() })
		return c
	}
	first := start("11")
	qaAwaitFile(t, filepath.Join(dir, "inside-commit"))
	second := start("22")
	time.Sleep(80 * time.Millisecond)
	if _, e := os.Stat(filepath.Join(dir, "config-22")); !os.IsNotExist(e) {
		t.Fatal("second configuration crossed active credential commit")
	}
	if e := os.WriteFile(filepath.Join(dir, "release-commit"), []byte("go"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := first.Wait(); e != nil {
		t.Fatal(e)
	}
	if e := second.Wait(); e != nil {
		t.Fatal(e)
	}
	native, e := os.ReadFile(filepath.Join(dir, "native-account"))
	if e != nil || string(native) != "22" {
		t.Fatalf("last credential mismatch %q %v", native, e)
	}
	for _, id := range []string{"11", "22"} {
		cfg, e := Load(filepath.Join(dir, "config-"+id))
		if e != nil || cfg.Account != id {
			t.Fatalf("other invoking config overwritten %+v %v", cfg, e)
		}
	}
}
func TestQALockParentProcess(t *testing.T) {
	if os.Getenv("TEMPO_QA_LOCK_PARENT") != "1" {
		return
	}
	dir := os.Getenv("TEMPO_QA_BARRIER_DIR")
	f, e := AcquireMutationLock(context.Background(), filepath.Join(dir, "owner.lock"))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	child := exec.Command(os.Args[0], "-test.run=^TestQALockInheritedChild$")
	child.Env = append(os.Environ(), "TEMPO_QA_LOCK_CHILD=1", "GORACE=atexit_sleep_ms=0")
	child.ExtraFiles = []*os.File{f}
	if e = child.Start(); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "child.pid"), []byte(strconv.Itoa(child.Process.Pid)), 0600); e != nil {
		t.Fatal(e)
	}
	child.Wait()
}
func TestQAKilledParentDoesNotReleaseLiveHelperLock(t *testing.T) {
	dir := t.TempDir()
	parent := exec.Command(os.Args[0], "-test.run=^TestQALockParentProcess$")
	parent.Env = append(os.Environ(), "TEMPO_QA_LOCK_PARENT=1", "TEMPO_QA_BARRIER_DIR="+dir)
	if e := parent.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		parent.Process.Kill()
		parent.Wait()
		os.WriteFile(filepath.Join(dir, "release"), []byte("go"), 0600)
	})
	qaAwaitFile(t, filepath.Join(dir, "ready"))
	if e := parent.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	parent.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	f, e := AcquireMutationLock(ctx, filepath.Join(dir, "owner.lock"))
	if f != nil {
		f.Close()
		t.Fatal("killed parent released surviving helper ownership")
	}
	if e == nil {
		t.Fatal("live helper no longer owned lock")
	}
	if e = os.WriteFile(filepath.Join(dir, "release"), []byte("go"), 0600); e != nil {
		t.Fatal(e)
	}
	f, e = AcquireMutationLock(context.Background(), filepath.Join(dir, "owner.lock"))
	if e != nil {
		t.Fatal(e)
	}
	f.Close()
}
