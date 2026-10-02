package worker

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity"
)

type Service struct {
	options Options
	fail    func(stage string) error
}

func New(o Options) (*Service, error) {
	if o.Sync == nil || o.UID < 0 {
		return nil, issue("validation")
	}
	for _, p := range []string{o.StatePath, o.ConfigPath, o.Executable, o.ServiceDir} {
		if !validPath(p) {
			return nil, issue("validation")
		}
	}
	var err error
	o.StatePath, err = activity.ResolveStatePath(o.StatePath)
	if err != nil {
		return nil, issue("validation")
	}
	for _, p := range []*string{&o.ConfigPath, &o.Executable, &o.ServiceDir} {
		*p, err = activity.ResolveStatePath(*p)
		if err != nil {
			return nil, issue("validation")
		}
	}
	if o.InstanceMode == "" {
		o.InstanceMode = "foreground"
	}
	if o.InstanceMode != "foreground" && o.InstanceMode != "managed" {
		return nil, issue("validation")
	}
	if o.Platform == "" {
		o.Platform = runtime.GOOS
	}
	if o.Clock == nil {
		o.Clock = wallClock{}
	}
	if o.NewRequestID == nil {
		o.NewRequestID = randomID
	}
	return &Service{options: o}, nil
}
func validPath(p string) bool {
	return p != "" && filepath.IsAbs(p) && filepath.Clean(p) == p && utf8.ValidString(p) && strings.IndexFunc(p, unicode.IsControl) < 0
}
func issue(code string) *Error {
	messages := map[string]string{"validation": "invalid worker input", "confirmation_required": "worker operation requires confirmation", "request_conflict": "worker request identity was used with different input", "revision_conflict": "worker service definition changed; preserve it for review", "state_corrupt": "worker state is unsafe or corrupt; preserve it for review", "state_busy": "worker state is busy", "local_write_unknown": "worker control durability is unknown; retry the same request identity", "control_history_full": "worker control history is full; preserve it for review", "unsupported": "worker service manager is unsupported", "manager": "worker service manager requires attention"}
	return &Error{Code: code, Message: messages[code], Retryable: code == "state_busy", Uncertain: code == "local_write_unknown"}
}
func (s *Service) fault(stage string) error {
	if s.fail != nil && s.fail(stage) != nil {
		return issue("local_write_unknown")
	}
	return nil
}
func (s *Service) Install(ctx context.Context, r ControlRequest) (Result, error) {
	return s.control(ctx, "install", r)
}
func (s *Service) Start(ctx context.Context, r ControlRequest) (Result, error) {
	return s.control(ctx, "start", r)
}
func (s *Service) Stop(ctx context.Context, r ControlRequest) (Result, error) {
	return s.control(ctx, "stop", r)
}
func (s *Service) Uninstall(ctx context.Context, r ControlRequest) (Result, error) {
	return s.control(ctx, "uninstall", r)
}
func (s *Service) Status(ctx context.Context) (Result, error) { return s.statusWithControl(ctx, nil) }
func (s *Service) statusWithControl(ctx context.Context, control *controlRecord) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	st, err := s.options.Sync.SyncStatus(ctx)
	if err != nil {
		return Result{}, err
	}
	return Result{ContractVersion: 1, Status: observe(ctx, s.options.StatePath, st.Worker, control)}, nil
}
