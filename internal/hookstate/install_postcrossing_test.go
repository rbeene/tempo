package hookstate

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
)

func assertPostCrossingPending(t *testing.T, f qaInstallFixture, in ApplyInstallInput) {
	t.Helper()
	before := qaInstallTree(t, f.root)
	status, err := New(f.options).Status(context.Background(), HookSelector{Host: "claude", Scope: "project", Path: f.project})
	if err != nil || len(status.Hooks) != 1 {
		t.Fatal("pending installation status unavailable")
	}
	p := status.Hooks[0].Pending
	if status.Hooks[0].State != "needs_repair" || p == nil || p.RequestID != in.RequestID || p.Fingerprint != in.Fingerprint || p.Intent != in.Intent {
		t.Fatal("partial installation lost original pending request")
	}
	qaInstallCheckNoPromotion(t, status)
	qaInstallAssertUnchanged(t, f, before)
}

func TestInstallPostCrossingErrorsRetainUncertainRecovery(t *testing.T) {
	for _, phase := range []string{"cancel", "intervening_edit"} {
		t.Run(phase, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			originals := qaInstallOriginals(t, f)
			s := New(f.options)
			in := qaInstallInput(t, s, f.intent("claude", "project"), 981)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hits := 0
			edit := []byte("synthetic intervening skill edit")
			s.fail = func(stage string) error {
				if stage == "install_target_sync" {
					hits++
					if hits == 1 {
						if phase == "cancel" {
							cancel()
						} else {
							qaInstallWrite(t, f.skill("claude", "project"), edit)
						}
					}
				}
				return nil
			}
			result, err := s.ApplyInstall(ctx, in)
			if hits != 1 || bytes.Equal(qaInstallRead(t, f.target("claude", "project")), originals[f.target("claude", "project")]) {
				t.Fatal("fixture did not publish the first destination")
			}
			var safe *Error
			if !errors.As(err, &safe) || safe.Code != "local_write_unknown" || !safe.Uncertain || safe.Retryable || safe.RequestID != in.RequestID || result.RequestID != in.RequestID {
				t.Fatalf("post-crossing failure lost uncertain original request: result=%+v error=%+v", result, safe)
			}
			assertPostCrossingPending(t, f, in)
			backups := qaInstallBackups(t, f)
			if phase == "intervening_edit" {
				if !bytes.Equal(qaInstallRead(t, f.skill("claude", "project")), edit) {
					t.Fatal("first attempt overwrote intervening edit")
				}
				before := qaInstallTree(t, f.root)
				_, err := New(f.options).ApplyInstall(context.Background(), in)
				qaInstallError(t, err, "revision_conflict")
				qaInstallAssertUnchanged(t, f, before)
				// Restore only the known synthetic fixture's reviewed absent state.
				// The prior replay must reject, never silently absorb this edit.
				if err := os.Remove(f.skill("claude", "project")); err != nil {
					t.Fatal(err)
				}
			} else if _, err := os.Stat(f.skill("claude", "project")); !os.IsNotExist(err) {
				t.Fatal("cancelled attempt published the next destination")
			}
			completed, err := New(f.options).ApplyInstall(context.Background(), in)
			if err != nil || completed.RequestID != in.RequestID {
				t.Fatalf("same-ID recovery failed: %v", err)
			}
			qaInstallCheckNoPromotion(t, completed)
			if !reflect.DeepEqual(backups, qaInstallBackups(t, f)) {
				t.Fatal("same-ID recovery duplicated original backups")
			}
			status, err := New(f.options).Status(context.Background(), HookSelector{Host: "claude", Scope: "project", Path: f.project})
			if err != nil || len(status.Hooks) != 1 || status.Hooks[0].Pending != nil {
				t.Fatal("completed recovery retained a pending request")
			}
		})
	}
}

func TestInstallBeforeFirstPublicationFailureRemainsDefinite(t *testing.T) {
	f := qaNewInstallFixture(t)
	originals := qaInstallOriginals(t, f)
	s := New(f.options)
	in := qaInstallInput(t, s, f.intent("claude", "project"), 982)
	hits := 0
	s.fail = func(stage string) error {
		if stage == "install_target_rename" {
			hits++
			return errors.New("synthetic pre-publication failure")
		}
		return nil
	}
	_, err := s.ApplyInstall(context.Background(), in)
	var safe *Error
	if hits != 1 || !errors.As(err, &safe) || safe.Code != "state_corrupt" || safe.Uncertain || safe.RequestID != in.RequestID {
		t.Fatalf("pre-publication failure classification changed: %+v", safe)
	}
	if changed := qaInstallChangedTarget(t, f, originals); changed != "" {
		t.Fatal("definite failure changed a destination")
	}
	assertPostCrossingPending(t, f, in)
}
