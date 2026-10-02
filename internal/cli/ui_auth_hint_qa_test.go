//go:build darwin || linux

package cli

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
)

type qaUIAuthHintProvider struct{ harvest.Provider }

func (qaUIAuthHintProvider) Accounts(context.Context) ([]harvest.Object, error) {
	return []harvest.Object{{"id": "11", "product": "harvest", "name": "Synthetic account"}}, nil
}

func TestQAUIAuthSharedWrappersNotifyOnlySuccessfulExplicitState(t *testing.T) {
	for _, operation := range []string{"login", "account"} {
		for _, mode := range []string{"success", "failure", "unknown", "canceled-before", "known-cancel", "no-state", "missing-socket"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				root, err := os.MkdirTemp("/tmp", "tempo-auth-hint-")
				if err != nil {
					t.Fatal(err)
				}
				defer os.RemoveAll(root)
				path := filepath.Join(root, "state")
				config := filepath.Join(root, "config")
				if err = auth.Save(config, auth.Config{Account: "22"}); err != nil {
					t.Fatal(err)
				}
				var socket *net.UnixConn
				if mode != "missing-socket" {
					socket, err = net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path + ".worker.sock", Net: "unixgram"})
					if err != nil {
						t.Fatal(err)
					}
					defer socket.Close()
					if err = os.Chmod(path+".worker.sock", 0600); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				mutations, stateReads := 0, 0
				effects := auth.Effects{Credential: "applied", Config: "saved"}
				if operation == "account" {
					effects.Credential = "unchanged"
				}
				service := auth.NewService(auth.Options{ConfigPath: config, LockPath: filepath.Join(root, "lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, NewProvider: func(token, account string) harvest.Provider {
					if token != "synthetic-auth-hint-token" {
						t.Error("wrapper changed private token source")
					}
					return qaUIAuthHintProvider{}
				}, Runner: auth.RunnerFunc(func(_ context.Context, in auth.NativeRequest, lock *os.File) (auth.NativeReply, error) {
					if in.Operation == "read" {
						return auth.NativeReply{Token: []byte("synthetic-auth-hint-token")}, nil
					}
					mutations++
					if in.Operation != operation || in.AccountID != "11" || lock == nil {
						t.Error("wrapper changed exact shared operation or lock ownership")
					}
					if mode == "failure" {
						return auth.NativeReply{}, &auth.Error{Code: "keychain", Effects: auth.Effects{Credential: "unchanged", Config: "unchanged"}}
					}
					if mode == "unknown" {
						return auth.NativeReply{}, &auth.Error{Code: "credential_write_unknown", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}
					}
					if e := auth.Save(config, auth.Config{Account: in.AccountID}); e != nil {
						t.Fatal(e)
					}
					if mode == "known-cancel" {
						cancel()
					}
					return auth.NativeReply{Effects: effects}, nil
				})})
				d := Dependencies{Auth: service, Getenv: func(key string) string {
					if key != "TEMPO_STATE" {
						t.Errorf("UI auth hint used undeclared default dependency %s", key)
						return ""
					}
					stateReads++
					if mode == "no-state" {
						return ""
					}
					return path
				}}
				actions := uiAuthActions(d)
				if stateReads != 0 || mutations != 0 {
					t.Fatal("opening UI auth actions performed a service action")
				}
				var attempt *auth.LoginAttempt
				if operation == "login" {
					attempt, err = actions.PrepareLogin(ctx, []byte("synthetic-auth-hint-token"))
					if err != nil {
						t.Fatal(err)
					}
					defer attempt.Close()
				}
				if mode == "canceled-before" {
					cancel()
				}
				start := time.Now()
				var result auth.Result
				if operation == "login" {
					result, err = actions.CommitLogin(ctx, attempt, "11")
				} else {
					result, err = actions.UseAccount(ctx, "11")
				}
				if time.Since(start) > 150*time.Millisecond {
					t.Error("best-effort worker hint extended successful shared reply beyond bounded budget")
				}
				switch mode {
				case "failure", "unknown":
					var typed *auth.Error
					wantCode, wantEffects := "keychain", auth.Effects{Credential: "unchanged", Config: "unchanged"}
					if mode == "unknown" {
						wantCode, wantEffects = "credential_write_unknown", auth.Effects{Credential: "unknown", Config: "unknown"}
					}
					if !errors.As(err, &typed) || typed.Code != wantCode || typed.Effects != wantEffects || typed.Uncertain != (mode == "unknown") || !reflect.DeepEqual(result, auth.Result{}) {
						t.Fatalf("hint changed shared failure/result: %+v %v", result, err)
					}
				case "canceled-before":
					if err != context.Canceled || !reflect.DeepEqual(result, auth.Result{}) || mutations != 0 {
						t.Fatalf("hint erased pre-admission cancellation: %+v %v calls%d", result, err, mutations)
					}
				default:
					want := auth.Result{AccountID: "11", Effects: effects}
					if operation == "login" {
						want.Authenticated, want.Source = true, "keychain"
					}
					if err != nil || !reflect.DeepEqual(result, want) || mutations != 1 {
						t.Fatalf("hint changed exact successful shared reply: %+v %v calls%d", result, err, mutations)
					}
					cfg, e := auth.Load(config)
					if e != nil || cfg.Account != "11" {
						t.Fatal("successful reply lost committed synthetic config")
					}
				}
				wantReads := 1
				if mode == "failure" || mode == "unknown" || mode == "canceled-before" {
					wantReads = 0
				}
				if mode == "known-cancel" && stateReads <= 1 {
					// A canceled success retains its reply; the best-effort hint may
					// skip immediately or reach its canceled 25ms sender.
				} else if stateReads != wantReads {
					t.Errorf("notification readiness path looked up %d times want%d", stateReads, wantReads)
				}
				if socket != nil {
					socket.SetReadDeadline(time.Now().Add(35 * time.Millisecond))
					buf := make([]byte, 32)
					n, _, e := socket.ReadFromUnix(buf)
					if mode == "success" {
						if e != nil || string(buf[:n]) != "recheck" {
							t.Errorf("successful %s did not send bounded recheck: %q %v", operation, buf[:n], e)
						}
						socket.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
						if _, _, e = socket.ReadFromUnix(buf); e == nil {
							t.Error("duplicate auth hint")
						}
					} else if e == nil {
						t.Errorf("non-success/nonready %s sent worker hint %q", mode, buf[:n])
					}
				}
			})
		}
	}
}

func TestQAUIAuthLogoutNeverSendsReadinessHint(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failed], func(t *testing.T) {
			root := t.TempDir()
			calls := 0
			effects := auth.Effects{Credential: "applied", Config: "cleared"}
			service := auth.NewService(auth.Options{ConfigPath: filepath.Join(root, "config"), LockPath: filepath.Join(root, "lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, Runner: auth.RunnerFunc(func(_ context.Context, in auth.NativeRequest, lock *os.File) (auth.NativeReply, error) {
				calls++
				if in.Operation != "logout" || lock == nil {
					t.Fatal("logout wrapper changed shared request")
				}
				if failed {
					return auth.NativeReply{}, &auth.Error{Code: "keychain", Effects: auth.Effects{Credential: "unchanged", Config: "unchanged"}}
				}
				return auth.NativeReply{Effects: effects}, nil
			})})
			actions := uiAuthActions(Dependencies{Auth: service, Getenv: func(key string) string {
				t.Errorf("logout performed unrequested notification lookup %s", key)
				return ""
			}})
			result, err := actions.Logout(context.Background(), true)
			if calls != 1 {
				t.Error("logout retried shared mutation")
			}
			if failed {
				var typed *auth.Error
				if !errors.As(err, &typed) || typed.Code != "keychain" || result.LoggedOut {
					t.Fatalf("logout failure changed: %+v %v", result, err)
				}
			} else if err != nil || !result.LoggedOut || result.Effects != effects {
				t.Fatalf("logout success changed: %+v %v", result, err)
			}
		})
	}
}
