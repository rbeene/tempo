package auth

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeStore struct {
	token string
	err   error
	reads int
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("sensitive-reader-detail") }

func TestCredentialErrorsAreRedacted(t *testing.T) {
	if _, err := ReadToken(errorReader{}); err == nil || strings.Contains(err.Error(), "sensitive-reader-detail") {
		t.Fatal("reader error must be redacted")
	}
	s := &fakeStore{err: errors.New("sensitive-store-detail")}
	if _, _, err := ResolveToken(s, nil); err == nil || strings.Contains(err.Error(), "sensitive-store-detail") {
		t.Fatal("store error must be redacted")
	}
}

func TestConfigReplacementTightensFileModeAndCleansTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := Save(path, Config{Account: "123"}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("replacement must be private", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatal("temporary config file left behind", err)
	}
}

func (s *fakeStore) Get() (string, error) { s.reads++; return s.token, s.err }
func (s *fakeStore) Set(v string) error   { s.token = v; return s.err }
func (s *fakeStore) Delete() error        { s.token = ""; return s.err }

func TestResolveToken(t *testing.T) {
	s := &fakeStore{token: "stored"}
	token, source, err := ResolveToken(s, func(k string) string {
		if k == "HARVEST_TOKEN" {
			return "environment"
		}
		return ""
	})
	if err != nil || token != "environment" || source != "environment" || s.reads != 0 {
		t.Fatalf("environment precedence failed: %q %q %v reads=%d", token, source, err, s.reads)
	}
	token, source, err = ResolveToken(s, func(string) string { return "" })
	if err != nil || token != "stored" || source != "keychain" || s.reads != 1 {
		t.Fatal("store fallback failed")
	}
	s.err = ErrNotFound
	if _, _, err = ResolveToken(s, func(string) string { return "" }); !errors.Is(err, ErrNotFound) {
		t.Fatal("missing credential not preserved")
	}
	s.reads = 0
	if _, _, err = ResolveToken(s, func(string) string { return "bad\ntoken" }); err == nil || s.reads != 0 {
		t.Fatal("invalid env must fail without store fallback")
	}
}

func TestReadToken(t *testing.T) {
	for _, v := range []string{"", " \n", "two tokens", "bad\ntoken", "bad\x00token", strings.Repeat("x", 16385)} {
		if _, err := ReadToken(strings.NewReader(v)); err == nil {
			t.Errorf("accepted invalid token length %d", len(v))
		}
	}
	for _, v := range []string{" token\r\n", strings.Repeat("x", 16384)} {
		got, err := ReadToken(strings.NewReader(v))
		if err != nil || got != strings.TrimSpace(v) {
			t.Fatal("valid token rejected", err)
		}
	}
}

func TestConfigRoundTripAndPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "config.json")
	got, err := Load(path)
	if err != nil || got.Account != "" {
		t.Fatal("missing config must be empty", err)
	}
	if err = Save(path, Config{Account: "123"}); err != nil {
		t.Fatal(err)
	}
	got, err = Load(path)
	if err != nil || got.Account != "123" {
		t.Fatal("roundtrip", err)
	}
	for _, p := range []string{path, filepath.Dir(path)} {
		info, e := os.Stat(p)
		if e != nil || info.Mode().Perm()&0077 != 0 {
			t.Fatalf("insecure permissions %s: %v", p, e)
		}
	}
	if err = Save(path, Config{Account: "456"}); err != nil {
		t.Fatal(err)
	}
	got, err = Load(path)
	if err != nil || got.Account != "456" {
		t.Fatal("replacement failed", err)
	}
	if err = Save(path, Config{Account: "bad"}); err == nil {
		t.Fatal("invalid account accepted")
	}
	got, _ = Load(path)
	if got.Account != "456" {
		t.Fatal("invalid write damaged previous config")
	}
}

func TestConfigRejectsMalformedAndSymlinks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	for _, v := range []string{``, `null`, `[]`, `{"account_id":null}`, `{"account_id":123}`, `{"account_id":"abc"}`, `{"account_id":"0"}`, `{"account_id":"-1"}`, `{"account_id":"1","token":"secret"}`, `{"account_id":"1"} {}`, `{"account_id":"1","account_id":"2"}`} {
		if err := os.WriteFile(path, []byte(v), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Errorf("accepted malformed config %s", v)
		}
	}
	os.Remove(path)
	target := filepath.Join(dir, "target")
	os.WriteFile(target, []byte(`{"account_id":"1"}`), 0600)
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("loaded symlink")
	}
	if err := Save(path, Config{Account: "2"}); err == nil {
		t.Fatal("saved through symlink")
	}
}

func TestConfigPathOverride(t *testing.T) {
	want := filepath.Join(t.TempDir(), "custom.json")
	t.Setenv("TEMPO_CONFIG", want)
	got, err := ConfigPath()
	if err != nil || got != want {
		t.Fatalf("path %q: %v", got, err)
	}
}

func TestConfigSaveReportsReplacementAfterDirectorySyncFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := Save(path, Config{Account: "11"}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	err := save(path, Config{Account: "22"}, func(dir string) error {
		calls++
		if dir != filepath.Dir(path) {
			t.Fatalf("synced directory %q", dir)
		}
		cfg, err := Load(path)
		if err != nil || cfg.Account != "22" {
			t.Fatal("config not replaced before directory sync", err)
		}
		return errors.New("sensitive filesystem details")
	})
	var saveErr *SaveError
	if !errors.As(err, &saveErr) || !saveErr.Replaced || calls != 1 {
		t.Fatalf("missing replacement error: %v", err)
	}
	if strings.Contains(err.Error(), "sensitive filesystem details") {
		t.Fatal("filesystem detail leaked")
	}
	cfg, err := Load(path)
	if err != nil || cfg.Account != "22" {
		t.Fatal("replacement not visible after failed sync", err)
	}
	err = save(path, Config{Account: "invalid"}, func(string) error { t.Fatal("invalid config reached sync"); return nil })
	if err == nil || errors.As(err, &saveErr) {
		t.Fatalf("validation falsely reports replacement: %v", err)
	}
	cfg, err = Load(path)
	if err != nil || cfg.Account != "22" {
		t.Fatal("validation changed config", err)
	}
}
