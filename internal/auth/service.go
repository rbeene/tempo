package auth

import (
	"context"
	"github.com/rbeene/tempo/internal/harvest"
	"os"
	"sync"
	"time"
)

// Effects contains nonsecret observations, not a rollback guarantee.
type Effects struct {
	Credential string `json:"credential"`
	Config     string `json:"config"`
}
type Error struct {
	Code           string
	Message        string
	Retryable      bool
	Uncertain      bool
	Effects        Effects
	RequiredFields []string
}

func (e *Error) Error() string { return e.Message }

type Result struct {
	Authenticated           bool             `json:"authenticated,omitempty"`
	LoggedOut               bool             `json:"logged_out,omitempty"`
	Verified                bool             `json:"verified,omitempty"`
	AccountID               string           `json:"account_id"`
	Source                  string           `json:"source,omitempty"`
	EnvironmentOverride     bool             `json:"environment_override,omitempty"`
	EnvironmentTokenPresent bool             `json:"environment_token_present,omitempty"`
	Note                    string           `json:"note,omitempty"`
	Accounts                []harvest.Object `json:"accounts,omitempty"`
	Effects                 Effects          `json:"effects"`
}

// NativeRequest/Reply travel only over private pipes, never user output.
type NativeRequest struct {
	Operation  string `json:"operation"`
	ConfigPath string `json:"config_path,omitempty"`
	AccountID  string `json:"account_id,omitempty"`
	Token      []byte `json:"token,omitempty"`
}
type NativeReply struct {
	Token   []byte  `json:"token,omitempty"`
	Code    string  `json:"code,omitempty"`
	Effects Effects `json:"effects"`
}
type Runner interface {
	Run(context.Context, NativeRequest, *os.File) (NativeReply, error)
}
type RunnerFunc func(context.Context, NativeRequest, *os.File) (NativeReply, error)

func (f RunnerFunc) Run(c context.Context, r NativeRequest, l *os.File) (NativeReply, error) {
	return f(c, r, l)
}

type ProcessRunner struct {
	Executable string
	Args       []string
	Timeout    time.Duration
}

type HandlerDependencies struct {
	LockPath   string
	Store      Store
	SaveConfig func(string, Config) error
	LoadConfig func(string) (Config, error)
}

// HandleNative requires a held mutation lock for all operations except read.
// LockPath is injectable for tests only; default ownership is per OS user and
// independent of TEMPO_CONFIG/TEMPO_STATE. Closing the last fd releases ownership.
type Options struct {
	ConfigPath          string
	LockPath            string
	Getenv              func(string) string
	Runner              Runner
	NewProvider         func(string, string) harvest.Provider
	PersistentAvailable func() bool
}
type Service struct{ options Options }
type LoginAttempt struct {
	mu       sync.Mutex
	Accounts []harvest.Object
	token    []byte
	consumed bool
}
