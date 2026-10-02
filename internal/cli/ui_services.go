package cli

import (
	"context"

	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/ui"
	"github.com/rbeene/tempo/internal/worker"
)

// Factories run only after an explicit action, never while opening or ticking
// the dashboard. All operations delegate to the same finite-command services.
func uiHookActions(d Dependencies) *ui.HookActions {
	return &ui.HookActions{
		Status: func(ctx context.Context, in hookstate.HookSelector) (hookstate.HookList, error) {
			return hooksService(d).Status(ctx, in)
		},
		Verify: func(ctx context.Context, in hookstate.HookSelector) (hookstate.HookList, error) {
			return hooksService(d).Verify(ctx, in)
		},
		PreviewInstall: func(ctx context.Context, in hookstate.InstallIntent) (hookstate.HookPreview, error) {
			return hooksService(d).PreviewInstall(ctx, in)
		},
		ApplyInstall: func(ctx context.Context, in hookstate.ApplyInstallInput) (hookstate.HookList, error) {
			return hooksService(d).ApplyInstall(ctx, in)
		},
		ConfirmInstalled: func(ctx context.Context, in hookstate.InstalledConfirmInput) (hookstate.HookList, error) {
			return hooksService(d).ConfirmInstalled(ctx, in)
		},
		Revoke: func(ctx context.Context, in hookstate.RevokeInput) (hookstate.Profile, error) {
			return hooksService(d).Revoke(ctx, in)
		},
	}
}

func uiWorkerActions(d Dependencies) *ui.WorkerActions {
	action := func(call func(*worker.Service, context.Context, worker.ControlRequest) (worker.Result, error)) func(context.Context, worker.ControlRequest) (worker.Result, error) {
		return func(ctx context.Context, in worker.ControlRequest) (worker.Result, error) {
			service, err := workerService(d)
			if err != nil {
				return worker.Result{}, err
			}
			return call(service, ctx, in)
		}
	}
	return &ui.WorkerActions{
		Status: func(ctx context.Context) (worker.Result, error) {
			service, err := workerService(d)
			if err != nil {
				return worker.Result{}, err
			}
			return service.Status(ctx)
		},
		Install: action((*worker.Service).Install), Start: action((*worker.Service).Start),
		Stop: action((*worker.Service).Stop), Uninstall: action((*worker.Service).Uninstall),
	}
}
