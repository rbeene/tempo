package ui

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/hookstate"
)

// These tests exercise the private presentation controller through its public
// prompt/service boundary. Every path is synthetic; no host is discovered.
type qaHWFlow struct {
	p      *promptBridge
	done   chan error
	cancel context.CancelFunc
}

func qaHWStart(t *testing.T, run func(context.Context, *promptBridge) error) *qaHWFlow {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f := &qaHWFlow{newPromptBridge(ctx), make(chan error, 1), cancel}
	go func() { f.done <- run(ctx, f.p); close(f.done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-f.done:
		case <-time.After(time.Second):
			t.Error("controller failed to join cancellation")
		}
	})
	return f
}
func (f *qaHWFlow) next(t *testing.T, kind string) promptRequest {
	t.Helper()
	select {
	case r := <-f.p.requests:
		if r.kind != kind {
			t.Fatalf("prompt %s wanted %s (%s)", r.kind, kind, r.title)
		}
		return r
	case e := <-f.done:
		t.Fatalf("controller skipped %s: %v", kind, e)
	case <-time.After(time.Second):
		t.Fatalf("controller stalled before %s", kind)
	}
	return promptRequest{}
}
func (f *qaHWFlow) finish(t *testing.T) error {
	t.Helper()
	select {
	case e := <-f.done:
		return e
	case <-time.After(time.Second):
		t.Fatal("controller did not finish")
	}
	return nil
}
func qaHWPick(t *testing.T, r promptRequest, id string) {
	t.Helper()
	for _, v := range r.choices {
		if v.ID == id {
			r.reply <- promptReply{choiceID: id}
			return
		}
	}
	t.Fatalf("missing choice %q: %+v", id, r.choices)
}
func qaHWContains(t *testing.T, s string, words ...string) {
	t.Helper()
	for _, w := range words {
		if !strings.Contains(strings.ToLower(s), strings.ToLower(w)) {
			t.Errorf("review omitted %q: %s", w, s)
		}
	}
}
func qaHWView(t *testing.T, f *qaHWFlow, words ...string) {
	t.Helper()
	r := f.next(t, "view")
	qaHWContains(t, r.title+" "+r.body, words...)
	r.reply <- promptReply{}
}
func qaHWBound(t *testing.T, ctx context.Context, max time.Duration) {
	t.Helper()
	d, ok := ctx.Deadline()
	if !ok || time.Until(d) > max+100*time.Millisecond {
		t.Error("callback lacks bounded action deadline")
	}
}

var qaHWUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

const qaHWPath = "/synthetic/tempo/project"
const qaHWFingerprint = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
const qaHWPendingID = "11111111-2222-4333-8444-555555555555"

func qaHookList(host string) hookstate.HookList {
	hosts := []string{host}
	if host == "both" {
		hosts = []string{"codex", "claude"}
	}
	out := hookstate.HookList{ContractVersion: 1}
	for _, h := range hosts {
		out.Hooks = append(out.Hooks, hookstate.HookStatus{Host: h, Scope: "project", Path: qaHWPath, State: "awaiting_real_event", Ordering: "supported", RuntimeVersion: "0.159.3", Profile: hookstate.Profile{Basis: "operator_declared", State: "eligible", Revision: "8", Fingerprint: qaHWFingerprint, DeclarationVersion: hookstate.DeclarationVersion, CaptureEligible: true, Context: hookstate.Context{Host: h, Scope: "project", Path: qaHWPath, RuntimeVersion: "0.159.3", Surface: "local", Artifacts: []hookstate.Artifact{{Role: "definitions", Path: qaHWPath + "/.codex/hooks.json", SHA256: qaHWFingerprint}}}}})
	}
	return out
}
func qaHookActions(t *testing.T) *HookActions {
	return &HookActions{Status: func(ctx context.Context, s hookstate.HookSelector) (hookstate.HookList, error) {
		qaHWBound(t, ctx, 250*time.Millisecond)
		return qaHookList(s.Host), nil
	}}
}
func qaHookStart(t *testing.T, c *hookController, a *HookActions) *qaHWFlow {
	return qaHWStart(t, func(ctx context.Context, p *promptBridge) error { return c.run(ctx, p, a) })
}
func qaHookSelect(t *testing.T, f *qaHWFlow, op, host, scope, path string) {
	t.Helper()
	qaHWPick(t, f.next(t, "choose"), op)
	qaHWPick(t, f.next(t, "choose"), host)
	qaHWPick(t, f.next(t, "choose"), scope)
	f.next(t, "text").reply <- promptReply{text: path}
}
func qaHookPreview(op string) hookstate.HookPreview {
	return hookstate.HookPreview{ContractVersion: 1, Intent: hookstate.InstallIntent{Host: "codex", Scope: "project", Path: qaHWPath, Operation: op}, Fingerprint: qaHWFingerprint, Changes: []hookstate.HookChange{{Host: "codex", Path: qaHWPath + "/.codex/hooks.json", Operation: "replace_owned", SafeSummary: "tempo hook codex --input-stdin; preserve unrelated content"}, {Host: "codex", Path: "/synthetic/main-checkout/.codex/hooks.json", Operation: "replace_owned", SafeSummary: "shared linked-worktree destination"}}, ApprovalSteps: []string{"Review workspace trust and /hooks; reload host; no real delivery is verified."}}
}

func TestQAHookControllerLazyBack(t *testing.T) {
	a := &HookActions{Status: func(context.Context, hookstate.HookSelector) (hookstate.HookList, error) {
		t.Error("menu accessed service")
		return hookstate.HookList{}, nil
	}}
	f := qaHookStart(t, &hookController{}, a)
	r := f.next(t, "choose")
	for _, id := range []string{"status", "verify", "preview", "install", "repair", "uninstall", "confirm-profile", "revoke-profile", "back"} {
		found := false
		for _, v := range r.choices {
			found = found || v.ID == id
		}
		if !found {
			t.Errorf("missing menu %s", id)
		}
	}
	qaHWPick(t, r, "back")
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
}
func TestQAHookControllerReadsPreserveEvidence(t *testing.T) {
	for _, op := range []string{"status", "verify"} {
		t.Run(op, func(t *testing.T) {
			a := qaHookActions(t)
			calls := 0
			read := func(ctx context.Context, s hookstate.HookSelector) (hookstate.HookList, error) {
				calls++
				qaHWBound(t, ctx, 250*time.Millisecond)
				if s != (hookstate.HookSelector{Host: "codex", Scope: "project", Path: qaHWPath}) {
					t.Errorf("selector changed: %+v", s)
				}
				return qaHookList("codex"), nil
			}
			a.Status = read
			a.Verify = read
			f := qaHookStart(t, &hookController{}, a)
			qaHookSelect(t, f, op, "codex", "project", qaHWPath+"/child/..")
			qaHWView(t, f, "awaiting_real_event", "supported", "operator_declared", qaHWPath, "delivery")
			if e := f.finish(t); e != nil {
				t.Fatal(e)
			}
			if calls < 1 || calls > 2 {
				t.Errorf("read calls=%d", calls)
			}
		})
	}
}
func TestQAHookControllerApplyFreezesPreviewAndRequiresConsent(t *testing.T) {
	for _, op := range []string{"install", "repair", "uninstall"} {
		for _, yes := range []bool{false, true} {
			t.Run(op+"/"+map[bool]string{false: "decline", true: "accept"}[yes], func(t *testing.T) {
				a := qaHookActions(t)
				preview := qaHookPreview(op)
				previews, applies := 0, 0
				a.PreviewInstall = func(ctx context.Context, in hookstate.InstallIntent) (hookstate.HookPreview, error) {
					previews++
					qaHWBound(t, ctx, 2*time.Minute)
					if in != (hookstate.InstallIntent{Host: "codex", Scope: "project", Path: qaHWPath, Operation: op}) {
						t.Errorf("wrong preview: %+v", in)
					}
					return preview, nil
				}
				a.ApplyInstall = func(ctx context.Context, in hookstate.ApplyInstallInput) (hookstate.HookList, error) {
					applies++
					qaHWBound(t, ctx, 2*time.Minute)
					if in.Intent != preview.Intent || in.Fingerprint != preview.Fingerprint || !in.Confirmed || !qaHWUUID.MatchString(in.RequestID) {
						t.Errorf("apply not exact reviewed intent: %+v", in)
					}
					out := qaHookList("codex")
					out.RequestID = in.RequestID
					return out, nil
				}
				f := qaHookStart(t, &hookController{}, a)
				qaHookSelect(t, f, op, "codex", "project", qaHWPath+"/other/..")
				r := f.next(t, "confirm")
				qaHWContains(t, r.title, op, qaHWPath, qaHWFingerprint, "/synthetic/main-checkout/.codex/hooks.json", "--input-stdin", "workspace", "/hooks", "capture", "upload")
				if applies != 0 || previews != 1 {
					t.Error("mutation preceded explicit review")
				}
				r.reply <- promptReply{confirmed: yes}
				if yes {
					qaHWView(t, f, "request")
				}
				_ = f.finish(t)
				if applies != map[bool]int{false: 0, true: 1}[yes] {
					t.Errorf("apply calls=%d", applies)
				}
			})
		}
	}
}
func TestQAHookControllerProfileUsesObservedSingleHost(t *testing.T) {
	for _, op := range []string{"confirm-profile", "revoke-profile"} {
		t.Run(op, func(t *testing.T) {
			a := qaHookActions(t)
			calls := 0
			a.ConfirmInstalled = func(ctx context.Context, in hookstate.InstalledConfirmInput) (hookstate.HookList, error) {
				calls++
				qaHWBound(t, ctx, 2*time.Minute)
				if in.Selector != (hookstate.HookSelector{Host: "claude", Scope: "project", Path: qaHWPath}) || in.Fingerprint != qaHWFingerprint || in.DeclarationVersion != hookstate.DeclarationVersion || !in.Confirmed || !qaHWUUID.MatchString(in.RequestID) {
					t.Errorf("profile consent changed: %+v", in)
				}
				out := qaHookList("claude")
				out.RequestID = in.RequestID
				return out, nil
			}
			a.Revoke = func(ctx context.Context, in hookstate.RevokeInput) (hookstate.Profile, error) {
				calls++
				qaHWBound(t, ctx, 2*time.Minute)
				if in.Host != "claude" || in.Scope != "project" || in.Path != qaHWPath || in.IfRevision != "8" || !in.Confirmed || !qaHWUUID.MatchString(in.RequestID) {
					t.Errorf("revocation lost observed context: %+v", in)
				}
				return qaHookList("claude").Hooks[0].Profile, nil
			}
			f := qaHookStart(t, &hookController{}, a)
			qaHookSelect(t, f, op, "both", "project", qaHWPath)
			qaHWPick(t, f.next(t, "choose"), "claude")
			r := f.next(t, "confirm")
			qaHWContains(t, r.title, "claude", qaHWPath, "8", "history")
			if op == "confirm-profile" {
				qaHWContains(t, r.title, qaHWFingerprint, hookstate.DeclarationVersion, "operator", "dynamic", "plugin", "trust", "delivery")
			}
			r.reply <- promptReply{confirmed: true}
			qaHWView(t, f, "request")
			if e := f.finish(t); e != nil {
				t.Fatal(e)
			}
			if calls != 1 {
				t.Errorf("mutations=%d", calls)
			}
		})
	}
}
func TestQAHookControllerUnknownRetainsExactReplayAcrossReopen(t *testing.T) {
	a := qaHookActions(t)
	previews := 0
	a.PreviewInstall = func(context.Context, hookstate.InstallIntent) (hookstate.HookPreview, error) {
		previews++
		return qaHookPreview("install"), nil
	}
	var submitted []hookstate.ApplyInstallInput
	uncertain := &hookstate.Error{Code: "local_write_unknown", Uncertain: true}
	a.ApplyInstall = func(ctx context.Context, in hookstate.ApplyInstallInput) (hookstate.HookList, error) {
		qaHWBound(t, ctx, 2*time.Minute)
		submitted = append(submitted, in)
		if len(submitted) == 1 {
			return hookstate.HookList{}, uncertain
		}
		out := qaHookList("codex")
		out.RequestID = in.RequestID
		return out, nil
	}
	c := &hookController{}
	f := qaHookStart(t, c, a)
	qaHookSelect(t, f, "install", "codex", "project", qaHWPath)
	f.next(t, "confirm").reply <- promptReply{confirmed: true}
	qaHWView(t, f, "local_write_unknown", "request", "same", "nonapplication")
	qaHWPick(t, f.next(t, "choose"), "back")
	if !errors.Is(f.finish(t), uncertain) || c.pending == nil {
		t.Fatal("unknown outcome was discarded")
	}
	g := qaHookStart(t, c, a)
	qaHWPick(t, g.next(t, "choose"), "status")
	qaHWView(t, g, "request", "read")
	qaHWPick(t, g.next(t, "choose"), "replay")
	qaHWView(t, g, "request")
	if e := g.finish(t); e != nil {
		t.Fatal(e)
	}
	if c.pending != nil || previews != 1 || len(submitted) != 2 || !reflect.DeepEqual(submitted[0], submitted[1]) {
		t.Fatalf("unknown replaced frozen request: previews=%d submitted=%+v pending=%+v", previews, submitted, c.pending)
	}
}
func TestQAHookControllerPersistedPendingBlocksFreshPreview(t *testing.T) {
	a := qaHookActions(t)
	saved := hookstate.ApplyInstallInput{Intent: qaHookPreview("repair").Intent, Fingerprint: qaHWFingerprint, RequestID: qaHWPendingID, Confirmed: true}
	a.Status = func(context.Context, hookstate.HookSelector) (hookstate.HookList, error) {
		out := qaHookList("codex")
		out.Hooks[0].Pending = &hookstate.PendingInstall{RequestID: saved.RequestID, Fingerprint: saved.Fingerprint, Intent: saved.Intent}
		return out, nil
	}
	a.PreviewInstall = func(context.Context, hookstate.InstallIntent) (hookstate.HookPreview, error) {
		t.Error("fresh preview bypassed unresolved transaction")
		return hookstate.HookPreview{}, nil
	}
	a.ApplyInstall = func(ctx context.Context, in hookstate.ApplyInstallInput) (hookstate.HookList, error) {
		if in != saved {
			t.Errorf("persisted replay changed: %+v", in)
		}
		out := qaHookList("codex")
		out.RequestID = in.RequestID
		return out, nil
	}
	f := qaHookStart(t, &hookController{}, a)
	qaHookSelect(t, f, "install", "codex", "project", qaHWPath)
	r := f.next(t, "choose")
	qaHWContains(t, r.title+" "+r.body+" "+r.choices[0].Label, qaHWPendingID)
	qaHWPick(t, r, "replay")
	qaHWView(t, f, "request")
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
}
func TestQAHookControllerDefiniteConflictAndRawErrorsSafe(t *testing.T) {
	for _, raw := range []error{&hookstate.Error{Code: "revision_conflict"}, errors.New("RAW-HOOK-CANARY")} {
		t.Run(reflect.TypeOf(raw).String(), func(t *testing.T) {
			a := qaHookActions(t)
			a.PreviewInstall = func(context.Context, hookstate.InstallIntent) (hookstate.HookPreview, error) {
				return qaHookPreview("install"), nil
			}
			calls := 0
			a.ApplyInstall = func(context.Context, hookstate.ApplyInstallInput) (hookstate.HookList, error) {
				calls++
				return hookstate.HookList{}, raw
			}
			c := &hookController{}
			f := qaHookStart(t, c, a)
			qaHookSelect(t, f, "install", "codex", "project", qaHWPath)
			f.next(t, "confirm").reply <- promptReply{confirmed: true}
			r := f.next(t, "view")
			if strings.Contains(r.title+r.body, "RAW-HOOK-CANARY") {
				t.Error("raw error disclosed")
			}
			qaHWContains(t, r.body, "review")
			r.reply <- promptReply{}
			_ = f.finish(t)
			if calls != 1 || c.pending != nil {
				t.Error("definite failure retried or retained uncertain pending")
			}
		})
	}
}

func TestQAHookControllerInvalidOrCanceledDraftHasNoReads(t *testing.T) {
	for _, path := range []string{"", "relative/project"} {
		t.Run(path, func(t *testing.T) {
			a := qaHookActions(t)
			a.Status = func(context.Context, hookstate.HookSelector) (hookstate.HookList, error) {
				t.Error("invalid path accessed service")
				return hookstate.HookList{}, nil
			}
			f := qaHookStart(t, &hookController{}, a)
			qaHookSelect(t, f, "install", "codex", "project", path)
			qaHWView(t, f, "validation")
			_ = f.finish(t)
		})
	}
	a := qaHookActions(t)
	a.Status = func(context.Context, hookstate.HookSelector) (hookstate.HookList, error) {
		t.Error("cancel accessed service")
		return hookstate.HookList{}, nil
	}
	f := qaHookStart(t, &hookController{}, a)
	qaHWPick(t, f.next(t, "choose"), "install")
	_ = f.next(t, "choose")
	f.cancel()
	if e := f.finish(t); !errors.Is(e, context.Canceled) {
		t.Errorf("cancel result=%v", e)
	}
}
func TestQAHookControllerPreviewHasNoApply(t *testing.T) {
	a := qaHookActions(t)
	a.PreviewInstall = func(ctx context.Context, in hookstate.InstallIntent) (hookstate.HookPreview, error) {
		qaHWBound(t, ctx, 2*time.Minute)
		if in.Operation != "repair" {
			t.Errorf("preview operation=%s", in.Operation)
		}
		return qaHookPreview("repair"), nil
	}
	a.ApplyInstall = func(context.Context, hookstate.ApplyInstallInput) (hookstate.HookList, error) {
		t.Error("read-only preview applied hooks")
		return hookstate.HookList{}, nil
	}
	f := qaHookStart(t, &hookController{}, a)
	qaHookSelect(t, f, "preview", "codex", "project", qaHWPath)
	qaHWPick(t, f.next(t, "choose"), "repair")
	qaHWView(t, f, qaHWPath, qaHWFingerprint, "/synthetic/main-checkout/.codex/hooks.json", "/hooks")
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
}
