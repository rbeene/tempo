//go:build darwin || linux

package cli

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
)

type qaUISetupHintProvider struct {
	harvest.Provider
	fail bool
}

func (qaUISetupHintProvider) Accounts(context.Context) ([]harvest.Object, error) {
	return []harvest.Object{{"id": "11", "product": "harvest", "name": "Synthetic"}}, nil
}
func (qaUISetupHintProvider) Get(context.Context, string) (harvest.Object, error) {
	return harvest.Object{"id": "2", "is_active": true, "timezone": "UTC"}, nil
}
func (p qaUISetupHintProvider) List(context.Context, string, url.Values) ([]harvest.Object, error) {
	if p.fail {
		return nil, &harvest.Error{Code: "network", Message: "RAW-SYNTHETIC-SETUP-CANARY"}
	}
	return []harvest.Object{{"is_active": true, "project": harvest.Object{"id": "100", "name": "Synthetic"}, "task_assignments": []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": "200", "name": "Work"}}}}}, nil
}

type qaUISetupHintPrompt struct {
	t             *testing.T
	path, mode    string
	secret        []byte
	confirmations int
}

func (p *qaUISetupHintPrompt) Choose(_ context.Context, title string, choices []terminal.Choice) (string, error) {
	for _, choice := range choices {
		if choice.ID == "100" {
			return choice.ID, nil
		}
	}
	p.t.Fatalf("unexpected shared picker %s", title)
	return "", nil
}
func (p *qaUISetupHintPrompt) Text(_ context.Context, title, _ string) (string, error) {
	if title == "Directory to link (Enter uses current directory)" {
		return p.path, nil
	}
	return "UTC", nil
}
func (p *qaUISetupHintPrompt) Secret(context.Context, string) ([]byte, error) {
	p.secret = []byte("synthetic-setup-hint-token")
	return p.secret, nil
}
func (p *qaUISetupHintPrompt) Confirm(context.Context, string) (bool, error) {
	p.confirmations++
	return p.confirmations == 1 || p.mode != "link-declined", nil
}

func TestQAUISetupActualSharedPartialAuthStepNotifiesWithoutInventingResult(t *testing.T) {
	for _, mode := range []string{"later-network-failure", "link-declined", "auth-unknown", "no-state"} {
		t.Run(mode, func(t *testing.T) {
			root, e := os.MkdirTemp("/tmp", "tempo-setup-hint-")
			if e != nil {
				t.Fatal(e)
			}
			defer os.RemoveAll(root)
			project := filepath.Join(root, "project")
			if e = os.Mkdir(project, 0700); e != nil {
				t.Fatal(e)
			}
			state := filepath.Join(root, "state", "activity.json")
			socketPath := filepath.Join(root, "notification-state")
			socket, e := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: socketPath + ".worker.sock", Net: "unixgram"})
			if e != nil {
				t.Fatal(e)
			}
			defer socket.Close()
			if e = os.Chmod(socketPath+".worker.sock", 0600); e != nil {
				t.Fatal(e)
			}
			config := filepath.Join(root, "config")
			stored := false
			logins := 0
			lookups := 0
			credentials := auth.NewService(auth.Options{ConfigPath: config, LockPath: filepath.Join(root, "lock"), Getenv: func(string) string { return "" }, PersistentAvailable: func() bool { return true }, NewProvider: func(string, string) harvest.Provider { return qaUISetupHintProvider{fail: mode != "link-declined"} }, Runner: auth.RunnerFunc(func(_ context.Context, in auth.NativeRequest, lock *os.File) (auth.NativeReply, error) {
				if in.Operation == "read" {
					if stored {
						return auth.NativeReply{Token: []byte("synthetic-setup-hint-token")}, nil
					}
					return auth.NativeReply{Code: "not_found"}, nil
				}
				logins++
				if in.Operation != "login" || in.AccountID != "11" || lock == nil {
					t.Fatal("guided wrapper changed native operation/lock")
				}
				if mode == "auth-unknown" {
					return auth.NativeReply{}, &auth.Error{Code: "credential_write_unknown", Uncertain: true, Effects: auth.Effects{Credential: "unknown", Config: "unknown"}}
				}
				if e := auth.Save(config, auth.Config{Account: "11"}); e != nil {
					t.Fatal(e)
				}
				stored = true
				return auth.NativeReply{Effects: auth.Effects{Credential: "applied", Config: "saved"}}, nil
			})})
			epoch, elapsed := "synthetic-setup-hint", "0"
			local := activity.New(activity.Options{Path: state, Clock: activity.ClockFunc(func() (activity.ClockSample, error) {
				return activity.ClockSample{Capability: "available", WallUTC: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC), Epoch: &epoch, ElapsedNS: &elapsed, AwakeNS: &elapsed}, nil
			})})
			d := Dependencies{Activity: local, Auth: credentials, ConfigPath: config, Getenv: func(key string) string {
				if key != "TEMPO_STATE" {
					t.Errorf("wizard initialized undeclared dependency %s", key)
					return ""
				}
				lookups++
				if mode == "no-state" {
					return ""
				}
				return socketPath
			}}
			actions := uiSetupActions(d)
			if logins != 0 || lookups != 0 {
				t.Fatal("constructing guided adapter performed effects")
			}
			prompt := &qaUISetupHintPrompt{t: t, path: project, mode: mode}
			result, outcome := actions.Run(context.Background(), setup.Input{}, prompt)
			if result.ContractVersion != 1 || result.Complete || len(result.Steps) < 4 || logins != 1 {
				t.Fatalf("wrapper fabricated shared completion or repeated login: %+v calls%d", result, logins)
			}
			unknown := mode == "auth-unknown"
			if unknown {
				var failure *auth.Error
				if !errors.As(outcome, &failure) || failure.Code != "credential_write_unknown" || failure.Effects != (auth.Effects{Credential: "unknown", Config: "unknown"}) || result.Steps[0].Action != "auth.status" || result.Steps[0].State != "input_required" {
					t.Fatal("unknown native outcome changed or inferred login success")
				}
			} else {
				if result.Steps[0].Action != "auth.login" || result.Steps[0].State != "complete" || result.Steps[1].State != "input_required" {
					t.Fatal("shared partial steps lost")
				}
				if mode == "link-declined" {
					if outcome != nil {
						t.Fatal("shared declined Link outcome changed")
					}
				} else {
					var failure *harvest.Error
					if !errors.As(outcome, &failure) || failure.Code != "network" {
						t.Fatal("later shared network failure changed")
					}
				}
			}
			for _, b := range prompt.secret {
				if b != 0 {
					t.Error("shared wizard retained prompt secret")
				}
			}
			if _, e = os.Stat(filepath.Dir(state)); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("failed/declined guided Link initialized activity state")
			}
			socket.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
			buf := make([]byte, 32)
			n, _, e := socket.ReadFromUnix(buf)
			if !unknown && mode != "no-state" {
				if e != nil || string(buf[:n]) != "recheck" {
					t.Errorf("known completed auth.login step lacked bounded recheck despite later partial outcome: %q %v", buf[:n], e)
				}
				socket.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
				if _, _, e = socket.ReadFromUnix(buf); e == nil {
					t.Error("guided partial sent duplicate hint")
				}
			} else if e == nil {
				t.Error("unknown or undeclared destination sent worker hint")
			}
		})
	}
}
