package cli_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/hooks"
)

func TestQACodexNativeChildPromptDecodeAndIngress(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "tempo-child-prompt-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path, err := activity.ResolveStatePath(filepath.Join(dir, "activity.json"))
	if err != nil {
		t.Fatal(err)
	}
	d, cwd, seconds, _ := qaHostWakeFixture(t, path)
	ctx := context.Background()
	send := func(at int64, kind, turn, agent string) activity.HostReceipt {
		t.Helper()
		*seconds = at
		raw := map[string]any{"hook_event_name": kind, "session_id": "s", "turn_id": turn, "cwd": cwd}
		if agent != "" {
			raw["agent_id"] = agent
		}
		if kind == "Stop" || kind == "SubagentStop" {
			raw["stop_hook_active"] = false
		}
		b, err := json.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		e, err := hooks.DecodeCodex(strings.NewReader(string(b)))
		if err != nil {
			t.Fatal(err)
		}
		r, err := d.Activity.IngestHost(ctx, e)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	initial, err := d.Activity.Status(ctx)
	if err != nil || len(initial.Actors) != 1 {
		t.Fatalf("root fixture: %+v %v", initial, err)
	}
	root := initial.Actors[0].Ref
	child := send(10, "SubagentStart", "child-turn", "child-agent")
	send(20, "Stop", "t", "")
	r := send(30, "UserPromptSubmit", "child-turn", "child-agent")
	if r.AgentID != "child-agent" || r.Actor == nil || child.Actor == nil || *r.Actor != *child.Actor {
		t.Fatalf("native child identity lost during decode/ingress: child=%+v receipt=%+v", child.Actor, r)
	}
	s, err := d.Activity.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Actors) != 2 || len(s.Uncertainties) != 0 {
		t.Fatalf("child prompt fabricated actor/uncertainty: %+v", s)
	}
	rootFound, childFound := false, false
	for _, a := range s.Actors {
		if a.Ref == root {
			rootFound = true
			if a.State != "wait_user" || a.Health != "continuous" {
				t.Fatalf("parent resumed by child prompt: %+v", a)
			}
		}
		if a.Ref == *child.Actor {
			childFound = true
			if a.State != "working" || a.Health != "continuous" {
				t.Fatalf("child lost work: %+v", a)
			}
		}
	}
	if !rootFound || !childFound {
		t.Fatal("child prompt replaced registered actor")
	}
	send(40, "SubagentStop", "child-turn", "child-agent")
	s, err = d.Activity.Status(ctx)
	if err != nil || len(s.ClosedIntervals) != 1 || s.ClosedIntervals[0].DurationNS != "40000000000" {
		t.Fatalf("parent/child union drift: %+v %v", s.ClosedIntervals, err)
	}
	next := send(50, "UserPromptSubmit", "next-root", "")
	if next.Actor == nil || next.Actor.Key != root.Key || next.Actor.Generation != "2" {
		t.Fatalf("child consumed root generation: %+v", next)
	}
}
