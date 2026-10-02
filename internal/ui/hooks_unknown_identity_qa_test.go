package ui

import (
	"context"
	"errors"
	"testing"

	"github.com/rbeene/tempo/internal/hookstate"
)

// A changed read-only observation must not replace the original recovery input
// in the unknown-outcome guidance presented to the operator.
func TestQAHookControllerUnknownDetailsPreserveCompleteRecoveryIdentity(t *testing.T) {
	for _, op := range []string{"install", "confirm-profile", "revoke-profile"} {
		t.Run(op, func(t *testing.T) {
			a := qaHookActions(t)
			uncertain := &hookstate.Error{Code: "local_write_unknown", Uncertain: true}
			var frozenID string
			a.PreviewInstall = func(context.Context, hookstate.InstallIntent) (hookstate.HookPreview, error) {
				return qaHookPreview("install"), nil
			}
			a.ApplyInstall = func(ctx context.Context, in hookstate.ApplyInstallInput) (hookstate.HookList, error) {
				frozenID = in.RequestID
				return hookstate.HookList{}, uncertain
			}
			a.ConfirmInstalled = func(ctx context.Context, in hookstate.InstalledConfirmInput) (hookstate.HookList, error) {
				frozenID = in.RequestID
				return hookstate.HookList{}, uncertain
			}
			a.Revoke = func(ctx context.Context, in hookstate.RevokeInput) (hookstate.Profile, error) {
				frozenID = in.RequestID
				return hookstate.Profile{}, uncertain
			}
			f := qaHookStart(t, &hookController{}, a)
			qaHookSelect(t, f, op, "codex", "project", qaHWPath)
			f.next(t, "confirm").reply <- promptReply{confirmed: true}
			r := f.next(t, "view")
			qaHWContains(t, r.body, op, "codex", "project", qaHWPath, frozenID)
			switch op {
			case "install":
				qaHWContains(t, r.body, qaHWFingerprint)
			case "confirm-profile":
				qaHWContains(t, r.body, qaHWFingerprint, hookstate.DeclarationVersion)
			case "revoke-profile":
				qaHWContains(t, r.body, "revision 8")
			}
			r.reply <- promptReply{}
			qaHWPick(t, f.next(t, "choose"), "back")
			if !errors.Is(f.finish(t), uncertain) {
				t.Fatal("unknown guidance acknowledged uncertainty as completion")
			}
		})
	}
}
