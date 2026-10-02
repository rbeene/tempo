package cli

import (
	"context"

	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
	"github.com/rbeene/tempo/internal/worker"
)

// Constructing adapters performs no I/O. Shared services and provider reads run
// only for an explicit action; the dashboard does not perform them on a tick.
func uiAuthActions(d Dependencies) *ui.AuthActions {
	credentials := authService(d)
	return &ui.AuthActions{CanPersist: credentials.CanPersist, Status: credentials.Status, Accounts: credentials.Accounts, PrepareLogin: credentials.PrepareLogin, Logout: credentials.Logout, ConfigShow: credentials.ConfigShow,
		CommitLogin: func(ctx context.Context, attempt *auth.LoginAttempt, account string) (auth.Result, error) {
			result, err := credentials.CommitLogin(ctx, attempt, account)
			if err == nil {
				notifyWorker(ctx, d, worker.Recheck)
			}
			return result, err
		},
		UseAccount: func(ctx context.Context, account string) (auth.Result, error) {
			result, err := credentials.UseAccount(ctx, account)
			if err == nil {
				notifyWorker(ctx, d, worker.Recheck)
			}
			return result, err
		},
	}
}

func uiSetupActions(d Dependencies) *ui.SetupActions {
	return &ui.SetupActions{Run: func(ctx context.Context, input setup.Input, prompt terminal.Prompter) (setup.Status, error) {
		service, err := setupService(d)
		if err != nil {
			return setup.Status{}, err
		}
		return service.Run(ctx, input, prompt)
	}}
}

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
