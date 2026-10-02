package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rbeene/tempo/internal/hookstate"
)

func TestQAHostAbsentReadsAndUnlinkedIngressDoNotInitialize(t *testing.T) {
	h := qaNew(t)
	policyPath := filepath.Join(t.TempDir(), "absent-policy", "hooks.json")
	h.service = New(Options{Path: h.path, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, nil }), HookPolicies: hookstate.New(hookstate.Options{Path: policyPath})})
	list, err := h.service.HostReceipts(context.Background(), HostReceiptFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if list.ContractVersion != 1 || list.SnapshotRevision != "0" || list.Receipts == nil || len(list.Receipts) != 0 {
		t.Fatalf("invalid absent receipt list: %+v", list)
	}
	r, err := h.service.IngestHost(context.Background(), HostEvent{Source: "codex", Kind: "UserPromptSubmit", SessionID: "s", TurnID: "t", CWD: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if r.Disposition != "untracked" || r.Actor != nil || r.Durability != "not_committed" || r.Origin != "unverified" {
		t.Fatalf("absent ingress fabricated capture: %+v", r)
	}
	for _, path := range []string{filepath.Dir(h.path), filepath.Dir(policyPath)} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("absent ingress/read created %s: %v", path, err)
		}
	}
	s := h.snapshot()
	if s.ComputerID != nil || len(s.Actors) != 0 {
		t.Fatalf("ingress bootstrapped identity: %+v", s)
	}
}

type qaHostHarness struct {
	*qaHarness
	policies *hookstate.Service
	cwd      string
}

func qaNewHost(t *testing.T) *qaHostHarness {
	t.Helper()
	return qaNewHostAt(t, t.TempDir())
}

func qaNewHostAt(t *testing.T, cwd string) *qaHostHarness {
	t.Helper()
	h := &qaHostHarness{qaHarness: qaNew(t), cwd: cwd}
	h.policies = hookstate.New(hookstate.Options{Path: filepath.Join(t.TempDir(), "hooks", "state.json")})
	c := hookstate.Context{Host: "codex", Scope: "project", Path: h.cwd, RuntimeVersion: "0.159.3", Surface: "local", Conflicts: []string{}}
	for _, role := range []string{"runtime", "executable", "definitions"} {
		path := filepath.Join(h.cwd, role)
		if err := os.WriteFile(path, []byte("synthetic fixture "+role), 0600); err != nil {
			t.Fatal(err)
		}
		c.Artifacts = append(c.Artifacts, hookstate.Artifact{Role: role, Path: path})
	}
	p, err := h.policies.Preview(context.Background(), c)
	if err != nil {
		t.Fatalf("synthetic policy preview: %v", err)
	}
	p, err = h.policies.Confirm(context.Background(), hookstate.ConfirmInput{Context: p.Context, Fingerprint: p.Fingerprint, DeclarationVersion: hookstate.DeclarationVersion, RequestID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", Confirmed: true})
	if err != nil {
		t.Fatalf("synthetic operator declaration: %v", err)
	}
	if !p.CaptureEligible || p.Basis != "operator_declared" {
		t.Fatalf("bad synthetic policy: %+v", p)
	}
	h.restartHost()
	in := qaLinkInput(t)
	in.Path = h.cwd
	if _, err := h.service.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t))); err != nil {
		t.Fatalf("synthetic public link: %v", err)
	}
	return h
}

func (h *qaHostHarness) restartHost() {
	h.service = New(Options{Path: h.path, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, h.clockErr }), HookPolicies: h.policies})
}
func (h *qaHostHarness) event(kind, turn, agent string) HostEvent {
	return HostEvent{Source: "codex", SessionID: "host-session", TurnID: turn, AgentID: agent, Kind: kind, CWD: h.cwd}
}
func (h *qaHostHarness) send(at int64, e HostEvent) HostReceipt {
	h.t.Helper()
	h.at(at)
	r, err := h.service.IngestHost(context.Background(), e)
	if err != nil {
		h.t.Fatalf("%s at %d: %#v", e.Kind, at, err)
	}
	if r.Origin != "unverified" {
		h.t.Fatalf("synthetic callback claimed native delivery: %+v", r)
	}
	return r
}
func (h *qaHostHarness) startSession() {
	e := h.event("SessionStart", "", "")
	e.SessionSource = "startup"
	h.send(0, e)
}

func TestQAHostQuietThirtyMinuteTurnAndExactReplay(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	first := h.send(0, h.event("UserPromptSubmit", "opaque-turn", ""))
	if first.Actor == nil || first.Disposition != "applied" || first.Durability != "committed" || first.ProfileBasis != "operator_declared" {
		t.Fatalf("start not durably admitted: %+v", first)
	}
	replay := h.send(20, h.event("UserPromptSubmit", "opaque-turn", ""))
	if replay.Disposition != "duplicate" || replay.Actor == nil || *replay.Actor != *first.Actor {
		t.Fatalf("replay allocated another actor: first=%+v replay=%+v", first, replay)
	}
	h.restartHost()
	stop := h.send(1800, h.event("Stop", "opaque-turn", ""))
	if stop.Durability != "committed" {
		t.Fatalf("stop not committed: %+v", stop)
	}
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 1800}})
	if len(s.Actors) != 1 || s.Actors[0].State != "wait_user" || len(s.Uncertainties) != 0 {
		t.Fatalf("quiet normal turn lost continuity: %+v", s)
	}
	before, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	receipts, err := h.service.HostReceipts(context.Background(), HostReceiptFilter{Source: "codex", SessionID: "host-session"})
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts.Receipts) < 3 {
		t.Fatalf("missing accepted host receipts: %+v", receipts)
	}
	after, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("HostReceipts read wrote activity state")
	}
	h.send(1810, h.event("Stop", "opaque-turn", ""))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 1800}})
}

func TestQAHostChildNamedRootRemainsIndependent(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	root := h.send(0, h.event("UserPromptSubmit", "turn", ""))
	child := h.send(10, h.event("SubagentStart", "turn", "root"))
	if root.Actor == nil || child.Actor == nil || *root.Actor == *child.Actor {
		t.Fatalf("root/child namespace collision: %+v %+v", root, child)
	}
	h.send(20, h.event("Stop", "turn", ""))
	s := h.snapshot()
	qaIntervals(t, s, nil)
	working := 0
	for _, a := range s.Actors {
		if a.State == "working" {
			working++
		}
	}
	if working != 1 {
		t.Fatalf("parent stop cascaded to child: %+v", s.Actors)
	}
	h.send(30, h.event("SubagentStop", "turn", "root"))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 30}})
}

func TestQAHostFollowupCWDDoesNotChangeAttribution(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	h.send(0, h.event("UserPromptSubmit", "turn", ""))
	e := h.event("Stop", "turn", "")
	e.CWD = filepath.Join(t.TempDir(), "nonexistent")
	h.send(60, e)
	s := h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 60}})
	if len(s.ClosedIntervals) != 1 || s.ClosedIntervals[0].Attribution.ProjectID != "3" {
		t.Fatalf("followup cwd lost persisted context: %+v", s)
	}
}

func TestQAHostLinkedWithoutPolicyNeverAdmitsWork(t *testing.T) {
	h := qaNew(t)
	in := qaLinkInput(t)
	h.service = New(Options{Path: h.path, Clock: ClockFunc(func() (ClockSample, error) { return h.sample, nil }), HookPolicies: hookstate.New(hookstate.Options{Path: filepath.Join(t.TempDir(), "missing", "hooks.json")})})
	if _, err := h.service.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t))); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"SessionStart", "UserPromptSubmit", "SessionEnd"} {
		e := HostEvent{Source: "codex", Kind: kind, SessionID: "unattested-session", CWD: in.Path}
		if kind == "SessionStart" {
			e.SessionSource = "startup"
		}
		if kind == "UserPromptSubmit" {
			e.TurnID = "turn"
		}
		r, err := h.service.IngestHost(context.Background(), e)
		if err != nil {
			t.Fatal(err)
		}
		if r.Actor != nil || r.ProfileBasis == "host_observed" {
			t.Fatalf("missing policy admitted timing/delivery: %+v", r)
		}
	}
	s := h.snapshot()
	if len(s.Actors) != 0 || len(s.ClosedIntervals) != 0 {
		t.Fatalf("linked context silently became capture eligible: %+v", s)
	}
}

func TestQAHostUnlinkedCallbackDoesNotInvalidateDriftedPolicy(t *testing.T) {
	h := qaNewHost(t)
	list, err := h.service.ListBindings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Bindings) != 1 {
		t.Fatal("fixture expected one binding")
	}
	// Use a distinct unlinked directory beneath the same policy? A directory link
	// inherits descendants, so instead initialize policy only and use an absent
	// activity store at that exact policy context.
	path := filepath.Join(t.TempDir(), "absent", "activity.json")
	if err := os.WriteFile(filepath.Join(h.cwd, "runtime"), []byte("drift"), 0600); err != nil {
		t.Fatal(err)
	}
	s := New(Options{Path: path, HookPolicies: h.policies})
	r, err := s.IngestHost(context.Background(), h.event("UserPromptSubmit", "unlinked", ""))
	if err != nil {
		t.Fatal(err)
	}
	if r.Disposition != "untracked" {
		t.Fatalf("unlinked disposition %+v", r)
	}
	// Restore before explicit eligibility check. If the unlinked callback sampled
	// and durably invalidated the policy, restoration cannot make this eligible.
	if err := os.WriteFile(filepath.Join(h.cwd, "runtime"), []byte("synthetic fixture runtime"), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := h.policies.Eligibility(context.Background(), "codex", h.cwd)
	if err != nil {
		t.Fatal(err)
	}
	if !p.CaptureEligible {
		t.Fatalf("unlinked callback mutated policy: %+v", p)
	}
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unlinked callback initialized activity: %v", err)
	}
}

func TestQAHostSiblingWorktreeSharesLinkButNotProjectPolicy(t *testing.T) {
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0700); err != nil {
		t.Fatal(err)
	}
	qaGit(t, repo, "init")
	qaGit(t, repo, "-c", "core.hooksPath=/dev/null", "commit", "--allow-empty", "-m", "fixture")
	sibling := filepath.Join(root, "sibling")
	qaGit(t, repo, "-c", "core.hooksPath=/dev/null", "worktree", "add", "-b", "qa-host-sibling", sibling)
	h := qaNewHostAt(t, repo)
	a, err := h.service.ShowBinding(context.Background(), ShowBindingInput{Path: repo})
	if err != nil {
		t.Fatal(err)
	}
	b, err := h.service.ShowBinding(context.Background(), ShowBindingInput{Path: sibling})
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Bindings) != 1 || len(b.Bindings) != 1 || a.Bindings[0].ID != b.Bindings[0].ID {
		t.Fatalf("worktree unexpectedly needs relink: %+v %+v", a, b)
	}
	p, err := h.policies.Eligibility(context.Background(), "codex", sibling)
	if err != nil {
		t.Fatal(err)
	}
	if p.CaptureEligible {
		t.Fatalf("checkout A declaration silently attests B configuration: %+v", p)
	}
	e := h.event("SessionStart", "", "")
	e.SessionID = "sibling-session"
	e.SessionSource = "startup"
	e.CWD = sibling
	h.send(0, e)
	e.Kind = "UserPromptSubmit"
	e.SessionSource = ""
	e.TurnID = "sibling-turn"
	r := h.send(0, e)
	if r.Actor != nil {
		t.Fatalf("sibling lacked policy but admitted timed actor: %+v", r)
	}
	after, err := h.service.ShowBinding(context.Background(), ShowBindingInput{Path: sibling})
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Bindings) != 1 || after.Bindings[0].ID != b.Bindings[0].ID || after.Bindings[0].Revision != b.Bindings[0].Revision {
		t.Fatalf("policy admission rewrote repository binding: %+v", after)
	}
}

func TestQAHostDistinctChildTurnsPreserveParentAndSibling(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	parent := h.send(0, h.event("UserPromptSubmit", "root-turn", ""))
	first := h.send(5, h.event("SubagentStart", "opaque-child-z", "child-one"))
	second := h.send(10, h.event("SubagentStart", "opaque-child-a", "child-two"))
	if parent.Actor == nil || first.Actor == nil || second.Actor == nil || *parent.Actor == *first.Actor || *first.Actor == *second.Actor {
		t.Fatalf("distinct native child turns not independently mapped: %+v %+v %+v", parent, first, second)
	}
	h.send(15, h.event("SubagentStop", "opaque-child-z", "child-one"))
	s := h.snapshot()
	qaIntervals(t, s, nil)
	working := map[ActorRef]bool{}
	for _, a := range s.Actors {
		if a.State == "working" {
			working[a.Ref] = true
		}
	}
	if len(working) != 2 || !working[*parent.Actor] || !working[*second.Actor] || working[*first.Actor] {
		t.Fatalf("exact child stop closed wrong actors: %+v", s.Actors)
	}
	h.send(20, h.event("Stop", "root-turn", ""))
	s = h.snapshot()
	qaIntervals(t, s, nil)
	working = map[ActorRef]bool{}
	for _, a := range s.Actors {
		if a.State == "working" {
			working[a.Ref] = true
		}
	}
	if len(working) != 1 || !working[*second.Actor] {
		t.Fatalf("parent stop cascaded to different-turn child: %+v", s.Actors)
	}
	h.send(25, h.event("SubagentStop", "opaque-child-a", "child-two"))
	qaIntervals(t, h.snapshot(), [][2]int64{{0, 25}})
}

func TestQAHostDetachedUnlinkedActorRetainsTerminalIdentity(t *testing.T) {
	h := qaNewHost(t)
	h.startSession()
	first := h.send(0, h.event("UserPromptSubmit", "turn", ""))
	h.send(10, h.event("Stop", "turn", ""))
	s := h.snapshot()
	if len(s.Actors) != 1 || first.Actor == nil {
		t.Fatalf("fixture actor missing: %+v", s)
	}
	a := s.Actors[0]
	if _, err := h.service.Interrupt(context.Background(), InterruptInput{ActorID: a.ID, Generation: a.Ref.Generation, IfRevision: a.Revision, RequestID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", Confirmed: true}); err != nil {
		t.Fatal(err)
	}
	bindings, err := h.service.ListBindings(context.Background())
	if err != nil || len(bindings.Bindings) != 1 {
		t.Fatalf("fixture binding: %+v %v", bindings, err)
	}
	b := bindings.Bindings[0]
	if _, err := h.service.Unlink(context.Background(), UnlinkInput{BindingID: b.ID, IfRevision: b.Revision, RequestID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Confirmed: true}); err != nil {
		t.Fatal(err)
	}
	h.restartHost()
	r := h.send(20, h.event("SessionEnd", "", ""))
	if r.Disposition != "stale" || r.Actor == nil || *r.Actor != *first.Actor {
		t.Fatalf("unlinked terminal forgot exact registered actor: %+v", r)
	}
	s = h.snapshot()
	qaIntervals(t, s, [][2]int64{{0, 10}})
	if len(s.Actors) != 1 || s.Actors[0].Attribution != a.Attribution || s.Actors[0].BindingID != a.BindingID {
		t.Fatalf("terminal rebounded immutable attribution: %+v", s.Actors)
	}
}
