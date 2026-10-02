package ui

import (
	"context"

	"github.com/rbeene/tempo/internal/hookstate"
)

// HookActions delegates installation and retained capture policy to the shared
// installer. Observed delivery and operator declarations remain distinct.
type HookActions struct {
	Status           func(context.Context, hookstate.HookSelector) (hookstate.HookList, error)
	Verify           func(context.Context, hookstate.HookSelector) (hookstate.HookList, error)
	PreviewInstall   func(context.Context, hookstate.InstallIntent) (hookstate.HookPreview, error)
	ApplyInstall     func(context.Context, hookstate.ApplyInstallInput) (hookstate.HookList, error)
	ConfirmInstalled func(context.Context, hookstate.InstalledConfirmInput) (hookstate.HookList, error)
	Revoke           func(context.Context, hookstate.RevokeInput) (hookstate.Profile, error)
}

type hookPending struct {
	operation string
	apply     hookstate.ApplyInstallInput
	confirm   hookstate.InstalledConfirmInput
	revoke    hookstate.RevokeInput
	err       error
}

type hookController struct{ pending *hookPending }

func (c *hookController) run(ctx context.Context, p *promptBridge, actions *HookActions) error {
	return nil
}
