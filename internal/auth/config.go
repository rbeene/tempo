package auth

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/rbeene/tempo/internal/identity"
)

type Config struct {
	Account string `json:"account_id,omitempty"`
}

// SaveError reports a failure after the config has been atomically replaced.
// Replaced means the new config is visible, although its durability is uncertain.
// Callers must account for that state before attempting credential replacement.
type SaveError struct {
	Replaced bool
}

func (*SaveError) Error() string { return "cannot finish saving account config" }

var errConfig = errors.New("invalid account config; expected an object containing only a positive account_id string")

func ConfigPath() (string, error) {
	if p := os.Getenv("TEMPO_CONFIG"); p != "" {
		return p, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", errors.New("cannot determine config directory; set TEMPO_CONFIG")
	}
	return filepath.Join(dir, "tempo", "config.json"), nil
}

func validAccount(v string) bool {
	if v == "" {
		return true
	}
	return identity.Valid(v)
}

// Load rejects unknown fields and duplicate keys so config cannot hide tokens or
// depend on ambiguous JSON interpretations. Missing config is an empty selection.
func Load(path string) (Config, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return Config{}, nil
	}
	if err != nil {
		return Config{}, errors.New("cannot inspect account config")
	}
	if !info.Mode().IsRegular() || info.Size() > 4096 {
		return Config{}, errConfig
	}
	f, err := os.Open(path)
	if err != nil {
		return Config{}, errors.New("cannot read account config")
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return Config{}, errConfig
	}
	dec := json.NewDecoder(io.LimitReader(f, 4097))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return Config{}, errConfig
	}
	var cfg Config
	seen := false
	for dec.More() {
		k, e := dec.Token()
		if e != nil || k != "account_id" || seen {
			return Config{}, errConfig
		}
		seen = true
		value, e := dec.Token()
		account, ok := value.(string)
		if e != nil || !ok {
			return Config{}, errConfig
		}
		cfg.Account = account
	}
	if tok, err = dec.Token(); err != nil || tok != json.Delim('}') {
		return Config{}, errConfig
	}
	var extra any
	if dec.Decode(&extra) != io.EOF || !validAccount(cfg.Account) {
		return Config{}, errConfig
	}
	return cfg, nil
}

// Save uses a private temporary file and atomic rename; failed validation never
// changes the previous config. Existing parent directories retain their modes.
func Save(path string, cfg Config) error {
	return save(path, cfg, syncDirectory)
}

func save(path string, cfg Config, syncDir func(string) error) error {
	if !validAccount(cfg.Account) {
		return errConfig
	}
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return errConfig
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("cannot inspect account config")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return errors.New("cannot create account config directory")
	}
	f, err := os.CreateTemp(dir, ".tempo-config-*")
	if err != nil {
		return errors.New("cannot create account config")
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	fail := func() error { return errors.New("cannot save account config") }
	if err = f.Chmod(0600); err != nil {
		return fail()
	}
	if err = json.NewEncoder(f).Encode(cfg); err != nil {
		return fail()
	}
	if err = f.Sync(); err != nil {
		return fail()
	}
	if err = f.Close(); err != nil {
		return fail()
	}
	if err = os.Rename(name, path); err != nil {
		return fail()
	}
	if err = syncDir(dir); err != nil {
		return &SaveError{Replaced: true}
	}
	return nil
}

func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
