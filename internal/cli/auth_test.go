package cli_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
)

type observedPipe struct {
	*io.PipeReader
	entered chan struct{}
	once    sync.Once
}

func (p *observedPipe) Read(b []byte) (int, error) {
	p.once.Do(func() { close(p.entered) })
	return p.PipeReader.Read(b)
}

func TestQALoginCancellationUnblocksTokenInput(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	input := &observedPipe{PipeReader: reader, entered: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeStore{}
	a := &fakeAPI{}
	var out, stderr bytes.Buffer
	done := make(chan int, 1)
	path := filepath.Join(t.TempDir(), "config.json")
	go func() {
		done <- qaLegacyRun(t, ctx, []string{"auth", "login", "--token-stdin", "--json"}, input, &out, &stderr, cli.Dependencies{Store: s, ConfigPath: path, Getenv: func(string) string { return "" }, NewProvider: func(string, string) harvest.Provider { return a }})
	}()
	select {
	case <-input.entered:
	case <-time.After(time.Second):
		cancel()
		reader.Close()
		<-done
		t.Fatal("login never began reading stdin")
	}
	cancel()
	select {
	case code := <-done:
		if code == 0 {
			t.Fatal("cancelled token read reported success")
		}
	case <-time.After(time.Second):
		reader.Close()
		<-done
		t.Fatal("cancelled login stayed blocked on empty open stdin")
	}
	if s.sets != 0 || s.gets != 0 || len(a.calls) != 0 {
		t.Fatalf("cancelled token input touched credentials/API: store=%+v calls=%v", s, a.calls)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("cancelled login persisted configuration")
	}
}

func TestQALogoutRecoversFromInvalidConfigAndEnvironment(t *testing.T) {
	for _, tc := range []struct{ name, config, account string }{{"malformed config", "{broken", ""}, {"invalid environment", "{\"account_id\":\"11\"}", "not-an-id"}, {"both invalid", "{broken", "not-an-id"}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(tc.config), 0600); err != nil {
				t.Fatal(err)
			}
			s := &fakeStore{}
			a := &fakeAPI{}
			var out, stderr bytes.Buffer
			code := qaLegacyRun(t, context.Background(), []string{"auth", "logout", "--yes", "--json"}, strings.NewReader(""), &out, &stderr, cli.Dependencies{Store: s, ConfigPath: path, Getenv: func(k string) string {
				if k == "HARVEST_ACCOUNT_ID" {
					return tc.account
				}
				return ""
			}, NewProvider: func(string, string) harvest.Provider { return a }})
			envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 0, "")
			if s.deletes != 1 || s.gets != 0 || s.sets != 0 || len(a.calls) != 0 {
				t.Fatalf("logout credential/API effects: store=%+v calls=%v", s, a.calls)
			}
			cfg, err := auth.Load(path)
			if err != nil || cfg.Account != "" {
				t.Fatalf("logout did not clear invalid configuration: %+v %v", cfg, err)
			}
		})
	}
}

func TestQALoginPostRenameSaveFailureRestoresBeforeChangingToken(t *testing.T) {
	for _, restoreFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "restored", true: "restore failed"}[restoreFails], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := auth.Save(path, auth.Config{Account: "22"}); err != nil {
				t.Fatal(err)
			}
			s := &fakeStore{}
			a := &fakeAPI{}
			var out, stderr bytes.Buffer
			saves := []string{}
			code := qaLegacyRun(t, context.Background(), []string{"auth", "login", "--token-stdin", "--account", "11", "--json"}, strings.NewReader("synthetic-secret"), &out, &stderr, cli.Dependencies{Store: s, ConfigPath: path, Getenv: func(string) string { return "" }, NewProvider: func(string, string) harvest.Provider { return a }, SaveConfig: func(p string, cfg auth.Config) error {
				saves = append(saves, cfg.Account)
				if len(saves) == 1 {
					if err := auth.Save(p, cfg); err != nil {
						t.Fatal(err)
					}
					return &auth.SaveError{Replaced: true}
				}
				if restoreFails {
					return errors.New("private-file-path synthetic-secret storage detail")
				}
				return auth.Save(p, cfg)
			}})
			envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 1, "config")
			if s.sets != 0 {
				t.Fatal("configuration persistence failure replaced saved token")
			}
			if len(saves) != 2 || saves[0] != "11" || saves[1] != "22" {
				t.Fatalf("wrong save/restore order: %v", saves)
			}
			cfg, err := auth.Load(path)
			if err != nil {
				t.Fatal(err)
			}
			want := "22"
			if restoreFails {
				want = "11"
			}
			if cfg.Account != want {
				t.Fatalf("account %s want %s", cfg.Account, want)
			}
			if strings.Contains(stderr.String(), "private-file-path") || strings.Contains(stderr.String(), "storage detail") {
				t.Fatal("raw storage failure leaked")
			}
			if restoreFails {
				if !strings.Contains(stderr.String(), "could not be durably saved or restored") || !strings.Contains(stderr.String(), "saved token was not changed") {
					t.Fatalf("missing partial-state guidance: %s", stderr.String())
				}
			} else if !strings.Contains(stderr.String(), "previous account restored") {
				t.Fatalf("missing restored-state guidance: %s", stderr.String())
			}
		})
	}
}
