package ui

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/worker"
)

func TestQAHookControllerReturnedCanonicalPreviewIsAuthoritative(t *testing.T) {
	a := qaHookActions(t)
	preview := qaHookPreview("repair")
	preview.Intent.Path = "/synthetic/canonical/checkout"
	var got hookstate.ApplyInstallInput
	a.PreviewInstall = func(context.Context, hookstate.InstallIntent) (hookstate.HookPreview, error) { return preview, nil }
	a.ApplyInstall = func(ctx context.Context, in hookstate.ApplyInstallInput) (hookstate.HookList, error) {
		got = in
		out := qaHookList("codex")
		out.RequestID = in.RequestID
		return out, nil
	}
	f := qaHookStart(t, &hookController{}, a)
	qaHookSelect(t, f, "repair", "codex", "project", qaHWPath)
	r := f.next(t, "confirm")
	qaHWContains(t, r.title, preview.Intent.Path, preview.Fingerprint)
	r.reply <- promptReply{confirmed: true}
	r = f.next(t, "view")
	qaHWContains(t, r.body, got.RequestID)
	r.reply <- promptReply{}
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
	if got.Intent != preview.Intent || got.Fingerprint != preview.Fingerprint {
		t.Errorf("reconstructed query rather than returned normalized intent: %+v", got)
	}
}
func TestQAHookControllerProfileObservedCanonicalContextIsAuthoritative(t *testing.T) {
	for _, op := range []string{"confirm-profile", "revoke-profile"} {
		t.Run(op, func(t *testing.T) {
			a := qaHookActions(t)
			canonical := "/synthetic/canonical/context"
			list := qaHookList("codex")
			list.Hooks[0].Path = canonical
			list.Hooks[0].Profile.Context.Path = canonical
			a.Status = func(context.Context, hookstate.HookSelector) (hookstate.HookList, error) { return list, nil }
			a.ConfirmInstalled = func(ctx context.Context, in hookstate.InstalledConfirmInput) (hookstate.HookList, error) {
				if in.Selector.Path != canonical {
					t.Errorf("confirmation ignored observed context: %+v", in)
				}
				out := list
				out.RequestID = in.RequestID
				return out, nil
			}
			a.Revoke = func(ctx context.Context, in hookstate.RevokeInput) (hookstate.Profile, error) {
				if in.Path != canonical || in.IfRevision != "8" {
					t.Errorf("revocation ignored observed context: %+v", in)
				}
				return list.Hooks[0].Profile, nil
			}
			f := qaHookStart(t, &hookController{}, a)
			qaHookSelect(t, f, op, "codex", "project", qaHWPath)
			r := f.next(t, "confirm")
			qaHWContains(t, r.title, canonical)
			r.reply <- promptReply{confirmed: true}
			qaHWView(t, f, "request")
			if e := f.finish(t); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestQAHookControllerProfileUnknownReplaysExactObservedIntent(t *testing.T) {
	for _, op := range []string{"confirm-profile", "revoke-profile"} {
		t.Run(op, func(t *testing.T) {
			a := qaHookActions(t)
			var confirmed []hookstate.InstalledConfirmInput
			var revoked []hookstate.RevokeInput
			uncertain := &hookstate.Error{Code: "local_write_unknown", Uncertain: true}
			a.ConfirmInstalled = func(ctx context.Context, in hookstate.InstalledConfirmInput) (hookstate.HookList, error) {
				qaHWBound(t, ctx, 2*time.Minute)
				confirmed = append(confirmed, in)
				if len(confirmed) == 1 {
					return hookstate.HookList{}, uncertain
				}
				out := qaHookList("codex")
				out.RequestID = in.RequestID
				return out, nil
			}
			a.Revoke = func(ctx context.Context, in hookstate.RevokeInput) (hookstate.Profile, error) {
				qaHWBound(t, ctx, 2*time.Minute)
				revoked = append(revoked, in)
				if len(revoked) == 1 {
					return hookstate.Profile{}, uncertain
				}
				return qaHookList("codex").Hooks[0].Profile, nil
			}
			c := &hookController{}
			f := qaHookStart(t, c, a)
			qaHookSelect(t, f, op, "codex", "project", qaHWPath)
			f.next(t, "confirm").reply <- promptReply{confirmed: true}
			qaHWView(t, f, "local_write_unknown")
			qaHWPick(t, f.next(t, "choose"), "back")
			if !errors.Is(f.finish(t), uncertain) {
				t.Fatal("profile uncertainty disappeared")
			}
			// Reopen's read observation may drift; the replay must retain the old values.
			a.Status = func(context.Context, hookstate.HookSelector) (hookstate.HookList, error) {
				list := qaHookList("codex")
				list.Hooks[0].Profile.Revision = "99"
				list.Hooks[0].Profile.Fingerprint = strings.Repeat("c", 64)
				return list, nil
			}
			g := qaHookStart(t, c, a)
			qaHWPick(t, g.next(t, "choose"), "status")
			qaHWView(t, g, "request")
			qaHWPick(t, g.next(t, "choose"), "replay")
			qaHWView(t, g, "request")
			if e := g.finish(t); e != nil {
				t.Fatal(e)
			}
			if op == "confirm-profile" && (len(confirmed) != 2 || !reflect.DeepEqual(confirmed[0], confirmed[1])) {
				t.Errorf("profile replay changed frozen confirmation: %+v", confirmed)
			}
			if op == "revoke-profile" && (len(revoked) != 2 || !reflect.DeepEqual(revoked[0], revoked[1])) {
				t.Errorf("profile replay changed frozen revoke: %+v", revoked)
			}
		})
	}
}
func TestQAHookControllerMultiplePendingRequiresExplicitRequestChoice(t *testing.T) {
	a := qaHookActions(t)
	secondID := "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	list := qaHookList("both")
	list.Hooks[0].Pending = &hookstate.PendingInstall{RequestID: qaHWPendingID, Fingerprint: qaHWFingerprint, Intent: qaHookPreview("repair").Intent}
	intent := hookstate.InstallIntent{Host: "claude", Scope: "project", Path: qaHWPath, Operation: "uninstall"}
	list.Hooks[1].Pending = &hookstate.PendingInstall{RequestID: secondID, Fingerprint: strings.Repeat("d", 64), Intent: intent}
	a.Status = func(context.Context, hookstate.HookSelector) (hookstate.HookList, error) { return list, nil }
	calls := 0
	a.ApplyInstall = func(ctx context.Context, in hookstate.ApplyInstallInput) (hookstate.HookList, error) {
		calls++
		if in.RequestID != secondID || in.Intent != intent || in.Fingerprint != strings.Repeat("d", 64) || !in.Confirmed {
			t.Errorf("selected pending identity changed: %+v", in)
		}
		out := qaHookList("claude")
		out.RequestID = in.RequestID
		return out, nil
	}
	a.PreviewInstall = func(context.Context, hookstate.InstallIntent) (hookstate.HookPreview, error) {
		t.Error("multiple pending bypassed by fresh preview")
		return hookstate.HookPreview{}, nil
	}
	f := qaHookStart(t, &hookController{}, a)
	qaHookSelect(t, f, "install", "both", "project", qaHWPath)
	qaHWPick(t, f.next(t, "choose"), secondID)
	if calls != 0 {
		t.Error("choosing pending auto-applied without replay selection")
	}
	qaHWPick(t, f.next(t, "choose"), "replay")
	qaHWView(t, f, "request")
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
	if calls != 1 {
		t.Errorf("replayed %d pending requests", calls)
	}
}
func TestQAWorkerControllerUnknownMissingActionCannotPanicOrLoseIdentity(t *testing.T) {
	original := &worker.Error{Code: "local_write_unknown", Message: "RAW-HIDDEN", Uncertain: true}
	c := &workerController{pending: &workerPending{operation: "start", request: worker.ControlRequest{RequestID: qaHWPendingID}, err: original}}
	f := qaWorkerStart(t, c, &WorkerActions{})
	qaHWPick(t, f.next(t, "choose"), "replay")
	r := f.next(t, "view")
	qaHWContains(t, r.body, qaHWPendingID)
	if strings.Contains(r.body, "RAW-HIDDEN") {
		t.Error("raw pending error exposed")
	}
	r.reply <- promptReply{}
	qaHWPick(t, f.next(t, "choose"), "back")
	if !errors.Is(f.finish(t), original) || c.pending == nil || c.pending.request.RequestID != qaHWPendingID {
		t.Fatal("missing replay callback erased uncertain identity")
	}
}
