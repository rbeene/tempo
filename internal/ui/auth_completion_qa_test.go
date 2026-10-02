package ui

import (
	"context"
	"errors"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/terminal"
	"reflect"
	"testing"
	"time"
)

func TestQAAuthCompletedReplyRetainedWhenCancellationRacesIt(t *testing.T) {
	for _, mode := range []string{"success", "known-failure", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			result := auth.Result{LoggedOut: true, AccountID: "22", EnvironmentTokenPresent: true, Effects: auth.Effects{Credential: "applied", Config: "cleared"}}
			var sharedErr error
			if mode == "known-failure" {
				sharedErr = &auth.Error{Code: "config", Effects: auth.Effects{Credential: "unchanged", Config: "saved"}}
			}
			if mode == "unknown" {
				sharedErr = &auth.Error{Code: "credential_write_unknown", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}
			}
			entered := make(chan struct{})
			a := qaAuthBase()
			a.Logout = func(ctx context.Context, _ bool) (auth.Result, error) {
				close(entered)
				<-ctx.Done()
				return result, sharedErr
			}
			c := &authController{}
			f := qaAuthStart(t, c, a)
			qaAuthPick(t, f.next(t, "choose"), "logout")
			f.next(t, "confirm").reply <- promptReply{confirmed: true}
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("not dispatched")
			}
			cause := &terminal.ExitError{Code: 143}
			f.cancel(cause)
			returned := f.finish(t)
			if mode == "unknown" {
				if returned != sharedErr || c.pending != sharedErr {
					t.Error("unknown demoted")
				}
				if c.completed != nil {
					t.Error("uncertainty became known completion")
				}
				return
			}
			if !errors.Is(returned, cause) {
				t.Errorf("ordinary session cause changed: %v", returned)
			}
			if c.completed == nil {
				t.Fatal("valid shared reply lost before canceled result View")
			}
			if c.completed.operation != "logout" || !reflect.DeepEqual(c.completed.result, result) || c.completed.err != sharedErr {
				t.Errorf("shared operation/result/error changed: %#v", c.completed)
			}
			if c.pending != nil {
				t.Error("known reply invented uncertainty")
			}
		})
	}
}
func TestQAAuthKnownEffectsRetainedBeforeResultViewCancel(t *testing.T) {
	for _, mode := range []string{"success", "known-failure"} {
		t.Run(mode, func(t *testing.T) {
			result := auth.Result{AccountID: "11", Effects: auth.Effects{Credential: "unchanged", Config: "saved"}}
			var sharedErr error
			if mode == "known-failure" {
				sharedErr = &auth.Error{Code: "config", Effects: result.Effects}
			}
			a := qaAuthBase()
			a.Accounts = qaAuthProvider{}.Accounts
			a.UseAccount = func(context.Context, string) (auth.Result, error) { return result, sharedErr }
			c := &authController{}
			f := qaAuthStart(t, c, a)
			qaAuthPick(t, f.next(t, "choose"), "accounts")
			qaAuthPick(t, f.next(t, "choose"), "11")
			f.next(t, "confirm").reply <- promptReply{confirmed: true}
			f.next(t, "view")
			if c.completed == nil || c.completed.operation != "account selection" || c.completed.err != sharedErr || !reflect.DeepEqual(c.completed.result, result) {
				t.Error("known reply not retained before view")
			}
			cause := &terminal.ExitError{Code: 130}
			f.cancel(cause)
			f.finish(t)
			if c.completed == nil || c.completed.err != sharedErr || !reflect.DeepEqual(c.completed.result, result) {
				t.Error("view cancellation erased known effects")
			}
		})
	}
}
