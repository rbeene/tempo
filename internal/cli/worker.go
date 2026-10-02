package cli

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/worker"
)

func workerCommand(name string) bool { return strings.HasPrefix(name, "worker ") }

func validateWorkerCLI(p *parsed) error {
	if _, ok := p.flags["account"]; ok {
		return problem("usage", "worker uses saved sync accounts; --account is not valid")
	}
	confirmed := p.flags["yes"] == "true"
	if p.command.Name == "worker install" || p.command.Name == "worker uninstall" {
		if !confirmed {
			return problem("confirmation_required", "this command requires explicit --yes")
		}
	} else if confirmed {
		return problem("usage", "--yes is only valid for worker install or uninstall")
	}
	return nil
}

// WorkerRunInvocation selects only a parsed and validated explicit lifetime.
// Output flags never convert worker run into a finite command.
func WorkerRunInvocation(args []string) bool {
	p, err := parse(args)
	return err == nil && p.command.Name == "worker run" && validateWorkerCLI(&p) == nil
}

// activityService composes read-only worker evidence onto the same activity
// snapshot. Injected services keep their own observer and storage policy.
func activityService(d Dependencies) *activity.Service {
	if d.Activity != nil {
		return d.Activity
	}
	path := d.Getenv("TEMPO_STATE")
	return activity.New(activity.Options{Path: path, ObserveWorker: func(ctx context.Context, base activity.WorkerStatus) activity.WorkerStatus {
		return worker.Observe(ctx, path, base)
	}})
}

// notifyWorker is a best-effort post-commit hint. Injected services must declare
// TEMPO_STATE to select a notification destination; their private store paths
// cannot be inferred, and must never fall back to the user's default store.
func notifyWorker(ctx context.Context, d Dependencies, kind worker.NotifyKind) {
	path := d.Getenv("TEMPO_STATE")
	if path == "" && (d.Activity != nil || d.Auth != nil) {
		return
	}
	_ = worker.Notify(ctx, path, kind)
}

func workerService(d Dependencies) (*worker.Service, error) {
	if d.Worker != nil {
		return d.Worker, nil
	}
	state, err := activity.ResolveStatePath(d.Getenv("TEMPO_STATE"))
	if err != nil {
		return nil, err
	}
	config := d.ConfigPath
	if config == "" {
		config = d.Getenv("TEMPO_CONFIG")
	}
	if config == "" {
		dir, err := os.UserConfigDir()
		if err != nil {
			return nil, problem("config", "cannot locate configuration")
		}
		config = filepath.Join(dir, "tempo", "config.json")
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, problem("config", "cannot locate worker executable")
	}
	var serviceDir string
	if runtime.GOOS == "darwin" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, problem("config", "cannot locate user service directory")
		}
		serviceDir = filepath.Join(home, "Library", "LaunchAgents")
	} else {
		dir, err := os.UserConfigDir()
		if err != nil {
			return nil, problem("config", "cannot locate user service directory")
		}
		serviceDir = filepath.Join(dir, "systemd", "user")
	}
	mode := "foreground"
	if d.Getenv("TEMPO_WORKER_MODE") == "managed" {
		mode = "managed"
	}
	sync := d.Activity
	if sync == nil {
		// Worker.Status adds its own observation. Keep runtime snapshot reads
		// undecorated while using the same canonical activity store and engine.
		sync = activity.New(activity.Options{Path: state})
	}
	return worker.New(worker.Options{
		StatePath: state, ConfigPath: config, Executable: executable, ServiceDir: serviceDir,
		UID: os.Getuid(), InstanceMode: mode, Runner: worker.ProcessRunner{},
		Sync: sync, SyncDependencies: activity.SyncDependencies{NewProvider: authService(d).Provider},
	})
}

func executeWorker(ctx context.Context, p parsed, d Dependencies) (any, error) {
	s, err := workerService(d)
	if err != nil {
		return nil, err
	}
	if p.command.Name == "worker status" {
		return s.Status(ctx)
	}
	if p.command.Name == "worker run" {
		return nil, s.Run(ctx)
	}
	id := p.flags["request-id"]
	if id == "" {
		id = linkRequestID()
	}
	r := worker.ControlRequest{RequestID: id, Confirmed: p.flags["yes"] == "true"}
	var result worker.Result
	switch p.command.Name {
	case "worker install":
		result, err = s.Install(ctx, r)
	case "worker start":
		result, err = s.Start(ctx, r)
	case "worker stop":
		result, err = s.Stop(ctx, r)
	case "worker uninstall":
		result, err = s.Uninstall(ctx, r)
	default:
		return nil, problem("usage", "unknown worker command")
	}
	if err != nil {
		// Preserve the generated identity for exact replay after an uncertain save.
		e := *safeError(err)
		e.Details = map[string]any{"request_id": id}
		return nil, &e
	}
	return result, nil
}
