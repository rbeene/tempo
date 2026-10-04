package activity

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestQASyncProcessHelper(t *testing.T) {
	mode := os.Getenv("TEMPO_QA_SYNC_HELPER")
	if mode == "" {
		return
	}
	s := qaLegacyNew(Options{Path: os.Getenv("TEMPO_QA_SYNC_PATH")})
	if mode == "lock" {
		lock, err := s.acquireSyncLock(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer lock.close()
		ready := os.NewFile(3, "qa-ready")
		if _, err = ready.Write([]byte("owned")); err != nil {
			t.Fatal(err)
		}
		ready.Close()
		time.Sleep(time.Minute)
		t.Fatal("parent did not terminate bounded fixture")
	}
	if mode == "claim-crash" {
		s.store.fail = func(stage string) error {
			if stage == "sync_after_claim" {
				os.Exit(47)
			}
			return nil
		}
		p := qaNewSyncProvider(t)
		p.beforeCreate = func(_ map[string]any) { t.Fatal("child passed crash boundary into POST") }
		_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(90)}, qaSyncDeps(t, p))
		t.Fatalf("child did not reach durable claim crash: %v", err)
	}
	t.Fatalf("unknown fixture mode %s", mode)
}
func qaSyncChild(t *testing.T, path, mode string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestQASyncProcessHelper$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "TEMPO_QA_SYNC_HELPER="+mode, "TEMPO_QA_SYNC_PATH="+path, "GORACE=atexit_sleep_ms=0")
	return cmd
}
func TestQASyncProcessLiveLockIsBusyAndDeathReleases(t *testing.T) {
	_, path, _ := qaSyncFixture(t, time.Minute)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	cmd := qaSyncChild(t, path, "lock")
	cmd.ExtraFiles = []*os.File{w}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	w.Close()
	if err = r.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	ready, err := io.ReadAll(r)
	if err != nil || string(ready) != "owned" {
		t.Fatalf("child ownership barrier=%q err=%v", ready, err)
	}
	contender := qaLegacyNew(Options{Path: path, LockTimeout: 30 * time.Millisecond})
	_, err = contender.acquireSyncLock(context.Background())
	qaCode(t, err, "state_busy")
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	acquired, err := contender.acquireSyncLock(context.Background())
	if err != nil {
		t.Fatalf("dead holder left lock owned: %v", err)
	}
	acquired.close()
}
func TestQASyncProcessDeathAfterClaimPausedRecoveryMakesNoRequest(t *testing.T) {
	s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	cmd := qaSyncChild(t, path, "claim-crash")
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 47 {
		t.Fatalf("child crash exit=%v output=%s", err, output)
	}
	item := qaSyncOnlyItem(t, s)
	if item.State != "submitting" {
		t.Fatalf("child did not persist submitting: %+v", item)
	}
	if _, err = s.SyncPause(context.Background(), qaSyncID(91)); err != nil {
		t.Fatal(err)
	}
	// A NEW startup recovery request must recover orphans before paused return.
	_, err = qaLegacyNew(Options{Path: path}).SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(92)}, qaSyncNoProvider(t))
	if err != nil {
		t.Fatal(err)
	}
	item = qaSyncOnlyItem(t, s)
	if item.State != "unknown" || item.RunRequestID != nil || item.Plan.Parts[0].State != "unknown" {
		t.Fatalf("paused startup skipped crash recovery: %+v", item)
	}
	if len(p.posts) != 0 {
		t.Fatal("parent mock saw unrequested POST")
	}
}
